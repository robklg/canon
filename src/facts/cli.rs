use anyhow::{bail, Result};
use unicode_width::UnicodeWidthStr;

pub(super) mod convert;
pub(super) mod import;

use crate::core::domain::config::{LedgerConfig, RecordingMode};
use crate::core::domain::decision::DecisionCommand;
use crate::core::domain::fact::{FactType, KeyTypes};
use crate::core::domain::format_count;
use crate::core::domain::scope::DecisionScope;
use crate::core::domain::time::zone::ReadTimesOn;
use crate::core::domain::IncludeSet;
use crate::core::ops::decision::DecisionParams;
use crate::core::ops::scope::{classify_all, ResolvedScope};
use crate::core::repo::Db;
use crate::expr::{pictured, Key, KeyKind, Listing};
use crate::expr::{read_filters, read_report, ExpressionText};
use crate::expr::{select_sources, RolePolicy, SelectionNotes, SelectionParams};
use crate::facts::domain::delete_key;
use crate::facts::ops::maintain::{
    execute_delete, execute_prune_excluded, execute_prune_orphaned, execute_prune_stale,
    plan_delete, plan_prune_excluded, plan_prune_orphaned, plan_prune_stale,
    validate_prune_excluded_scope,
};
use crate::facts::ops::report::{
    compute_all_keys, compute_distribution, compute_grouped_distribution,
    compute_root_distribution, report_keys, AllKeysResult, Annotation, Distribution, Group,
    Grouped, KeyInfo, Line, Mark, Part, RootTable, Row,
};

/// What kind of key the listing and a report's header say a key is: nothing for a content
/// fact.
fn kind_word(kind: KeyKind) -> Option<&'static str> {
    match kind {
        KeyKind::SourceFact => Some("source fact"),
        KeyKind::BuiltinKey => Some("built-in key"),
        KeyKind::ContentFact => None,
    }
}

/// The key list's kind suffix: the kind in parentheses.
fn kind_label(kind: KeyKind) -> Option<String> {
    kind_word(kind).map(|word| format!("({word})"))
}

#[allow(clippy::too_many_arguments)]
pub fn run(
    db: &mut Db,
    key_arg: Option<&str>,
    scope_prefixes: &[String],
    filter_text: &ExpressionText,
    limit: usize,
    show_all: bool,
    include: &IncludeSet,
    by_root: bool,
    group_by: &[String],
    scope: &ResolvedScope,
) -> Result<()> {
    // Validate --group-by requires --key (--by-root works standalone)
    if !group_by.is_empty() && key_arg.is_none() {
        bail!("--group-by requires --key to be specified");
    }

    let conn = db.conn_mut();

    // Read every key named and every filter before anything is selected, so every cause —
    // a wrong-type modifier, a value a key cannot read — is refused up front, together; then
    // give every key its default, once.
    let reading = read_report(conn, key_arg, group_by, filter_text)?;
    let filters = reading.filters;
    let keys = report_keys(reading.key, reading.grouping, by_root)?;

    let scopes = classify_all(scope_prefixes);
    let params = SelectionParams {
        scopes,
        include: include.clone(),
        filters,
        role_policy: RolePolicy::SourceUnlessIncluded,
        clock: crate::user_zone().clone(),
        notes: false,
    };
    let sel = select_sources(conn, &params)?;
    let source_ids = sel.source_ids();
    let total_sources = source_ids.len();

    // The scope line comes before the report, including the report that says
    // there is nothing to report: "no matches" is only meaningful once the
    // reader knows where Canon looked.
    {
        use std::io::Write as _;
        let stdout = std::io::stdout();
        let mut handle = stdout.lock();
        crate::scope::print_report_scope(&mut handle, "Facts", scope);
        let _ = handle.flush();
    }

    if total_sources == 0 {
        println!("No sources match the given filters.");
        if !include.includes_excluded() && sel.excluded_count > 0 {
            // Visibility hints go to stderr — they are about the view, not part
            // of the report a caller may be piping.
            eprintln!(
                "\n({} excluded sources hidden, use --include excluded to show)",
                sel.excluded_count
            );
        }
        return Ok(());
    }

    {
        use std::io::Write;
        let stdout = std::io::stdout();
        let mut handle = stdout.lock();
        if include.is_expanded()
            && (sel.included_excluded_count > 0 || sel.included_archived_count > 0)
        {
            let mut parts = Vec::new();
            if sel.included_excluded_count > 0 {
                parts.push(format!("{} excluded", sel.included_excluded_count));
            }
            if sel.included_archived_count > 0 {
                parts.push(format!("{} from archive", sel.included_archived_count));
            }
            let _ = writeln!(
                handle,
                "Sources matching filters: {total_sources} (incl. {})\n",
                parts.join(", ")
            );
        } else {
            let _ = writeln!(handle, "Sources matching filters: {total_sources}\n");
        }
    }

    if let Some(main_key) = keys.main {
        if !keys.grouping.is_empty() {
            let result = compute_grouped_distribution(
                conn,
                &source_ids,
                &main_key,
                &keys.grouping,
                limit,
                crate::user_zone(),
            )?;
            display_grouped_distribution(&result);
        } else {
            let result =
                compute_distribution(conn, &source_ids, &main_key, limit, crate::user_zone())?;
            display_distribution(&result);
        }
    } else if by_root {
        let result = compute_root_distribution(&sel.sources);
        display_root_distribution(&result);
    } else {
        let result = compute_all_keys(conn, &source_ids, show_all)?;
        display_all_keys(&result, show_all, filter_text.typed().is_empty());
    }

    if !include.includes_excluded() && sel.excluded_count > 0 {
        eprintln!(
            "\n({} excluded sources hidden, use --include excluded to show)",
            sel.excluded_count
        );
    }

    Ok(())
}

// ============================================================================
// Display functions (format typed results from ops layer)
// ============================================================================

/// The narrowest a distribution's value column is, so a short report keeps the layout every
/// report has; a wider cell widens it.
const MIN_COLUMN: usize = 40;

/// A cell's width on a terminal, in display columns — the one measure every column is sized by.
fn width(text: &str) -> usize {
    UnicodeWidthStr::width(text)
}

/// `text` followed by spaces to `columns` display columns.
fn pad(text: &str, columns: usize) -> String {
    format!("{text}{}", " ".repeat(columns.saturating_sub(width(text))))
}

/// A mark as the mark column prints it, after Coverage; nothing on an unmarked row.
fn mark_cell(mark: Option<Mark>) -> &'static str {
    match mark {
        Some(Mark::Coarse) => "  +",
        Some(Mark::Unshowable) => "  !",
        None => "",
    }
}

/// A line under a table: what is on the row, then its filter after `: `, a control character
/// shown as its picture so the line stays one line.
fn line_text(line: &Line) -> String {
    pictured(&match &line.filter {
        Some(filter) => format!("{}: {filter}", line.label),
        None => line.label.clone(),
    })
}

/// A grouped table's part as printed: its cell, and a mixed value's type after it,
/// `2019 (number)`.
fn part_text(part: &Part) -> String {
    match part.type_word {
        Some(word) => format!("{} ({word})", part.cell),
        None => part.cell.clone(),
    }
}

/// A grouped row's parts as printed, joined by ` / `.
fn parts_text(parts: &[Part]) -> String {
    parts.iter().map(part_text).collect::<Vec<_>>().join(" / ")
}

/// The lines under a table, after a blank line; nothing where there are none.
fn write_lines(out: &mut impl std::io::Write, lines: &[Line]) {
    if lines.is_empty() {
        return;
    }
    let _ = writeln!(out);
    for line in lines {
        let _ = writeln!(out, "{}", line_text(line));
    }
}

/// The statement that a key is of mixed type and its way out, above a table.
fn write_mixed(out: &mut impl std::io::Write, mixed: &[String]) {
    if mixed.is_empty() {
        return;
    }
    for line in mixed {
        let _ = writeln!(out, "{line}");
    }
    let _ = writeln!(out);
}

fn display_root_distribution(result: &RootTable) {
    use std::io::Write;
    let stdout = std::io::stdout();
    let mut handle = stdout.lock();

    let column = result
        .rows
        .iter()
        .map(|row| width(&row.cell))
        .chain([width("Root")])
        .max()
        .unwrap_or_default();

    let _ = writeln!(handle, "{} {:>10}", pad("Root", column), "Sources");
    let _ = writeln!(handle, "{}", "─".repeat(column + 18));

    for row in &result.rows {
        let pct = if result.total_sources > 0 {
            (row.count as f64 / result.total_sources as f64) * 100.0
        } else {
            0.0
        };
        if writeln!(
            handle,
            "{} {:>10} ({:>5.1}%)",
            pad(&row.cell, column),
            format_count(row.count),
            pct,
        )
        .is_err()
        {
            return;
        }
    }
    write_lines(&mut handle, &result.lines);
}

fn display_all_keys(result: &AllKeysResult, show_all: bool, show_status_predicates: bool) {
    use std::io::Write;
    let stdout = std::io::stdout();
    let mut handle = stdout.lock();
    let column = key_column(result);
    let _ = writeln!(
        handle,
        "{} {:>6} {:>10} {:>10}",
        pad("Key", column),
        "Type",
        "Count",
        "Coverage"
    );
    let _ = writeln!(handle, "{}", "─".repeat(column + 30));

    for key_info in &result.keys {
        if writeln!(
            handle,
            "{}",
            key_row(key_info, result.total_sources, column)
        )
        .is_err()
        {
            break;
        }
    }
    write_lines(&mut handle, &result.lines);

    if !show_all {
        let hidden_count = Key::all()
            .filter(|k| k.listing() == Listing::Hidden)
            .count();
        let _ = writeln!(
            handle,
            "\n({hidden_count} source facts hidden, use --all to show)"
        );
    }

    let mixed = mixed_key_lines(result);
    if !mixed.is_empty() {
        let _ = writeln!(handle);
        for line in mixed {
            let _ = writeln!(handle, "{line}");
        }
    }

    if show_status_predicates {
        let _ = writeln!(handle, "\nStatus Predicates (use in --where):");
        let _ = writeln!(handle, "  {:<14} content exists in an archive", "archived?");
        let _ = writeln!(handle, "  {:<14} content hash has been computed", "hashed?");
        let _ = writeln!(handle, "  {:<14} source or object is excluded", "excluded?");
        let _ = writeln!(
            handle,
            "  {:<14} has metadata beyond content hash",
            "enriched?"
        );
        let _ = writeln!(handle);
        for line in built_in_key_lines() {
            let _ = writeln!(handle, "{line}");
        }
    }
}

/// The built-in keys, beside the status words and in their form: each key's name and its
/// meaning, read off the key vocabulary — never a list of its own, so a key added there is
/// listed here.
fn built_in_key_lines() -> Vec<String> {
    let mut lines =
        vec!["Built-in keys (use in --where, --key, --group-by, --emit and patterns):".to_string()];
    lines.extend(
        Key::all_typed()
            .filter(|(key, _, _)| key.kind() == KeyKind::BuiltinKey)
            .map(|(key, _, meaning)| format!("  {:<14} {meaning}", key.name())),
    );
    lines
}

/// The narrowest the key list's key column is, as it has always been printed.
const MIN_KEY_COLUMN: usize = 30;

/// The key list's key column: as wide as its widest printed key.
fn key_column(result: &AllKeysResult) -> usize {
    result
        .keys
        .iter()
        .map(|key_info| width(&key_info.cell))
        .chain([MIN_KEY_COLUMN])
        .max()
        .unwrap_or(MIN_KEY_COLUMN)
}

/// One key's row in the listing: its printed name, the key's type word, the scope's count and
/// coverage, its mark, and what kind of key it is.
fn key_row(key_info: &KeyInfo, total_sources: usize, column: usize) -> String {
    let coverage = (key_info.count as f64 / total_sources as f64) * 100.0;
    let mark = if key_info.mark { "  !" } else { "" };
    let suffix = kind_label(key_info.kind)
        .map(|label| format!("  {label}"))
        .unwrap_or_default();
    format!(
        "{} {:>6} {:>10} {:>9.1}%{mark}{suffix}",
        pad(&key_info.cell, column),
        key_info.types.word(),
        key_info.count,
        coverage,
    )
}

/// The lines under the listing that name its keys of mixed type: the statement, then the
/// way out, one command per key; none when no listed key is mixed.
fn mixed_key_lines(result: &AllKeysResult) -> Vec<String> {
    use crate::expr::{mixed_type_statement, mixed_type_way_out};

    let mixed: Vec<(&str, &KeyTypes)> = result
        .keys
        .iter()
        .filter(|key_info| key_info.types.is_mixed())
        .map(|key_info| (key_info.key.as_str(), &key_info.types))
        .collect();
    let mut lines = mixed_type_statement(&mixed);
    lines.extend(mixed_type_way_out(&mixed));
    lines
}

/// What a header says its rows are, at the end of the header line: the rows' type, and the
/// key's kind where it has one.
fn annotation_text(annotation: &Annotation) -> String {
    match annotation.kind.and_then(kind_word) {
        Some(kind) => format!("({}, {kind})", annotation.rows),
        None => format!("({})", annotation.rows),
    }
}

fn display_distribution(result: &Distribution) {
    write_distribution(&mut std::io::stdout().lock(), result);
    if result.shown < result.total_values {
        eprintln!(
            "Showing {} of {} values (use --limit 0 for all)",
            result.shown, result.total_values
        );
    }
}

/// One key's table: the mixed-type statement, the header — its control characters shown as
/// pictures — and its rule, the rows, and the lines under it.
fn write_distribution(handle: &mut impl std::io::Write, result: &Distribution) {
    write_mixed(handle, &result.mixed);

    let rows: Vec<&Row> = result.rows.iter().chain(&result.no_value).collect();
    let shown_header = pictured(&result.header);
    let column = rows
        .iter()
        .map(|row| width(&row.cell))
        .chain([width(&shown_header), MIN_COLUMN])
        .max()
        .unwrap_or(MIN_COLUMN);
    let typed = rows.iter().any(|row| row.type_word.is_some());
    let type_column = |word: &str| {
        if typed {
            format!(" {word:>6}")
        } else {
            String::new()
        }
    };

    let header = format!(
        "{}{} {:>10} {:>10}   {}",
        pad(&shown_header, column),
        type_column("Type"),
        "Count",
        "Coverage",
        annotation_text(&result.annotation)
    );
    let _ = writeln!(handle, "{header}");
    let _ = writeln!(handle, "{}", "─".repeat(width(&header)));

    for row in rows {
        let coverage = (row.count as f64 / result.total_sources as f64) * 100.0;
        if writeln!(
            handle,
            "{}{} {:>10} {:>9.1}%{}",
            pad(&row.cell, column),
            type_column(row.type_word.unwrap_or_default()),
            row.count,
            coverage,
            mark_cell(row.mark)
        )
        .is_err()
        {
            return;
        }
    }
    write_lines(handle, &result.lines);
}

fn display_grouped_distribution(result: &Grouped) {
    write_grouped(&mut std::io::stdout().lock(), result);
    if result.shown < result.total_groups {
        eprintln!(
            "Showing {} of {} groups (use --limit 0 for all)",
            result.shown, result.total_groups
        );
    }
}

/// A grouped table: the mixed-type statement, the header — its control characters shown as
/// pictures — each group and its rows, and the lines under it.
fn write_grouped(handle: &mut impl std::io::Write, result: &Grouped) {
    write_mixed(handle, &result.mixed);
    let _ = writeln!(handle, "{}\n", pictured(&result.header));

    let groups: Vec<&Group> = result.groups.iter().chain(&result.no_value_group).collect();
    let column = groups
        .iter()
        .flat_map(|group| &group.rows)
        .map(|row| width(&parts_text(&row.parts)))
        .chain([MIN_COLUMN])
        .max()
        .unwrap_or(MIN_COLUMN);

    for group in groups {
        let coverage = (group.total as f64 / result.total_sources as f64) * 100.0;
        if writeln!(
            handle,
            "{} (total: {:>6}, {:>5.1}%){}",
            part_text(&group.part),
            format_count(group.total),
            coverage,
            mark_cell(group.mark)
        )
        .is_err()
        {
            return;
        }
        for row in &group.rows {
            let coverage = (row.count as f64 / group.total as f64) * 100.0;
            if writeln!(
                handle,
                "  {} {:>8} {:>6.1}%{}",
                pad(&parts_text(&row.parts), column),
                format_count(row.count),
                coverage,
                mark_cell(row.mark)
            )
            .is_err()
            {
                return;
            }
        }
        if writeln!(handle).is_err() {
            return;
        }
    }
    for line in &result.lines {
        let _ = writeln!(handle, "{}", line_text(line));
    }
}

// ============================================================================
// Delete Facts
// ============================================================================

pub struct DeleteOptions {
    pub entity_type: String, // "source" or "object"
    pub value_type: Option<FactType>,
    pub dry_run: bool,
    /// The visibility the equivalent `ls --where` would have used at this
    /// scope. Deletion must act on the population the user could preview.
    pub include: IncludeSet,
}

#[allow(clippy::too_many_arguments)]
pub fn delete_facts(
    db: &mut Db,
    key: &str,
    scope_prefixes: &[String],
    filter_text: &ExpressionText,
    options: &DeleteOptions,
    command_line: &str,
    config: &LedgerConfig,
    no_receipt: bool,
) -> Result<()> {
    // The key acted on, read first: nothing below may hide its refusal.
    let key = delete_key(key)?;
    if options.entity_type != "source" && options.entity_type != "object" {
        bail!(
            "Invalid entity type '{}'. Must be 'source' or 'object'.",
            options.entity_type
        );
    }

    let conn = db.conn_mut();

    // Parse filters
    let filters = read_filters(conn, filter_text)?;

    let scopes = classify_all(scope_prefixes);

    // The population is the one the user can look at first: `facts delete
    // --where X` deletes over exactly what `ls --where X` at this scope
    // lists. Substituting a wider visibility here made a destructive command
    // reach sources — dismissed ones, archive copies — that no preview of it
    // ever showed.
    let params = SelectionParams {
        scopes,
        include: options.include.clone(),
        filters,
        role_policy: RolePolicy::SourceUnlessIncluded,
        clock: crate::user_zone().clone(),
        notes: true,
    };
    let plan = plan_delete(
        conn,
        &params,
        &key,
        &options.entity_type,
        options.value_type,
    )?;

    if plan.source_ids.is_empty() {
        println!("No sources match the given filters.");
        print_notes(&plan.notes);
        return Ok(());
    }

    let entity_label = if options.entity_type == "source" {
        "sources"
    } else {
        "objects"
    };

    if plan.fact_count == 0 {
        println!("No '{key}' facts found on matching {entity_label}.");
        print_notes(&plan.notes);
    } else if options.dry_run {
        println!(
            "Would delete {} '{}' fact rows across {} {}",
            format_count(plan.fact_count),
            plan.key,
            format_count(plan.entity_count),
            entity_label
        );
        print_notes(&plan.notes);
    } else {
        // Execute
        let roots = crate::core::repo::root::fetch_all(conn)?;
        let decision = DecisionParams {
            command: DecisionCommand::FactsDelete,
            scope: DecisionScope::decompose(scope_prefixes, &roots),
            command_line: command_line.to_string(),
            reason: None,
            record_enabled: config.recording != RecordingMode::Off && !options.dry_run,
            receipt_enabled: config.recording == RecordingMode::Full
                && !no_receipt
                && !options.dry_run,
            ledger_config: config.clone(),
            zone: ReadTimesOn::of_user(crate::user_zone()),
        };
        let result = execute_delete(
            conn,
            &options.entity_type,
            options.value_type,
            &plan,
            Some(&decision),
        )?;
        println!("{}", result.summary);
    }

    Ok(())
}

/// The plan's line about its selection, directly under the count it qualifies.
fn print_notes(notes: &SelectionNotes) {
    for line in notes.lines() {
        println!("{line}");
    }
}

// ============================================================================
// Prune Stale Facts
// ============================================================================

pub fn prune_stale(
    db: &Db,
    dry_run: bool,
    command_line: &str,
    config: &LedgerConfig,
    no_receipt: bool,
) -> Result<()> {
    let conn = db.conn();

    let plan = plan_prune_stale(conn)?;

    if plan.stale_count == 0 {
        println!("No stale facts found.");
        return Ok(());
    }

    if dry_run {
        println!(
            "Would delete {} stale fact rows (observed_basis_rev mismatch)",
            format_count(plan.stale_count)
        );
    } else {
        let decision = DecisionParams {
            command: DecisionCommand::Prune,
            scope: Vec::new(),
            command_line: command_line.to_string(),
            reason: None,
            record_enabled: config.recording != RecordingMode::Off && !dry_run,
            receipt_enabled: config.recording == RecordingMode::Full && !no_receipt && !dry_run,
            ledger_config: config.clone(),
            zone: ReadTimesOn::of_user(crate::user_zone()),
        };
        let result = execute_prune_stale(conn, Some(&decision))?;
        println!("{}", result.summary);
    }

    Ok(())
}

// ============================================================================
// Prune Orphaned Objects
// ============================================================================

pub fn prune_orphaned_objects(
    db: &mut Db,
    dry_run: bool,
    command_line: &str,
    config: &LedgerConfig,
    no_receipt: bool,
) -> Result<()> {
    let conn = db.conn_mut();

    let stats = plan_prune_orphaned(conn)?;

    if stats.object_count == 0 {
        println!("No orphaned objects found.");
        return Ok(());
    }

    if dry_run {
        println!(
            "Would delete {} orphaned objects, {} non-present sources, and {} facts",
            format_count(stats.object_count),
            format_count(stats.source_count),
            format_count(stats.total_fact_count())
        );
        println!();
        println!("Note: Orphaned objects represent content you've seen but no longer have.");
        println!("They may be useful if the content reappears (backup restore, found elsewhere).");
        println!(
            "Object-level exclusions will also be deleted (use `exclude list-objects` to review)."
        );
        println!("Use --yes to proceed with deletion.");
    } else {
        let decision = DecisionParams {
            command: DecisionCommand::Prune,
            scope: Vec::new(),
            command_line: command_line.to_string(),
            reason: None,
            record_enabled: config.recording != RecordingMode::Off && !dry_run,
            receipt_enabled: config.recording == RecordingMode::Full && !no_receipt && !dry_run,
            ledger_config: config.clone(),
            zone: ReadTimesOn::of_user(crate::user_zone()),
        };
        let result = execute_prune_orphaned(db, Some(&decision))?;
        println!("{}", result.summary);
    }

    Ok(())
}

// ============================================================================
// Prune Excluded Facts
// ============================================================================

pub fn prune_excluded_facts(
    db: &Db,
    scope: &str,
    dry_run: bool,
    command_line: &str,
    config: &LedgerConfig,
    no_receipt: bool,
) -> Result<()> {
    validate_prune_excluded_scope(scope)?;
    let conn = db.conn();

    let plan = plan_prune_excluded(conn, scope)?;

    let prune_sources = scope == "all" || scope == "source";
    let prune_objects = scope == "all" || scope == "object";

    if plan.total_count() == 0 {
        // The scope is validated above, so at least one side is asked.
        println!(
            "{}",
            match (prune_sources, prune_objects) {
                (true, true) => "No facts stored on excluded sources or content.",
                (true, false) => "No facts stored on excluded sources.",
                (false, _) => "No facts stored on excluded content.",
            }
        );
        return Ok(());
    }

    if dry_run {
        println!("Facts that would be deleted:");
        if prune_sources {
            println!(
                "  Stored on excluded sources: {}",
                format_count(plan.stored_on_source_count)
            );
        }
        if prune_objects {
            println!(
                "  Stored on excluded content: {}",
                format_count(plan.stored_on_content_count)
            );
        }
        println!(
            "  Total: {} facts would be deleted",
            format_count(plan.total_count())
        );
        println!();
        if scope == "all" {
            println!(
                "Tip: Use --excluded-facts=source or --excluded-facts=object to narrow scope."
            );
        }
        println!("Use --yes to proceed with deletion.");
    } else {
        let decision = DecisionParams {
            command: DecisionCommand::Prune,
            scope: Vec::new(),
            command_line: command_line.to_string(),
            reason: None,
            record_enabled: config.recording != RecordingMode::Off && !dry_run,
            receipt_enabled: config.recording == RecordingMode::Full && !no_receipt && !dry_run,
            ledger_config: config.clone(),
            zone: ReadTimesOn::of_user(crate::user_zone()),
        };
        let result = execute_prune_excluded(conn, scope, Some(&decision))?;
        if !result.summary.is_empty() {
            println!("{}", result.summary);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// **One width function sizes every column**, in display columns: a cell of wide or
    /// accented characters pads to the same column as plain text, and nothing is cut.
    #[test]
    fn a_cell_is_padded_by_its_display_width_and_never_cut() {
        let accented = "é".repeat(50);
        assert_eq!(width(&accented), 50);
        assert_eq!(pad(&accented, 40), accented);
        assert_eq!(width("東京"), 4);
        assert_eq!(pad("東京", 6), "東京  ");
        assert_eq!(pad("ab", 4), "ab  ");
    }

    /// **A line under a table prints on one line**: a control character in its label or its
    /// filter is shown as its picture, and a line holding none prints as composed.
    #[test]
    fn a_line_holding_a_control_character_prints_on_one_line() {
        let line = Line {
            label: "Row ! \"a\tb\"".to_string(),
            filter: Some("NOT \"content.NL\nKey\"?".to_string()),
        };
        assert_eq!(
            line_text(&line),
            "Row ! \"a\u{2409}b\": NOT \"content.NL\u{240A}Key\"?"
        );
        let plain = Line {
            label: "(no value)".to_string(),
            filter: Some("NOT content.Make?".to_string()),
        };
        assert_eq!(line_text(&plain), "(no value): NOT content.Make?");
    }

    /// **A header whose key holds a control character prints on one line**, in both tables:
    /// ops hands the key as `--where` reads it, and the screen shows the line feed as `␊`.
    #[test]
    fn a_header_holding_a_control_character_prints_on_one_line() {
        let key = "\"content.NL\nKey\"";
        let plain = Distribution {
            header: key.to_string(),
            annotation: Annotation {
                rows: "text",
                kind: None,
            },
            mixed: Vec::new(),
            rows: Vec::new(),
            no_value: None,
            lines: Vec::new(),
            shown: 0,
            total_values: 0,
            total_sources: 1,
        };
        let mut out = Vec::new();
        write_distribution(&mut out, &plain);
        let out = String::from_utf8(out).unwrap();
        assert!(
            out.lines()
                .next()
                .unwrap()
                .starts_with("\"content.NL\u{240A}Key\""),
            "{out}"
        );

        let grouped = Grouped {
            header: format!("{key} (grouped by source.ext)"),
            mixed: Vec::new(),
            groups: Vec::new(),
            no_value_group: None,
            lines: Vec::new(),
            shown: 0,
            total_groups: 0,
            total_sources: 1,
        };
        let mut out = Vec::new();
        write_grouped(&mut out, &grouped);
        let out = String::from_utf8(out).unwrap();
        assert_eq!(
            out.lines().next().unwrap(),
            "\"content.NL\u{240A}Key\" (grouped by source.ext)"
        );
    }

    /// **A grouped part prints its value's type after it** where it has one, `2019 (number)`,
    /// and the parts of a row join with ` / `.
    #[test]
    fn a_typed_part_prints_its_type_after_its_cell() {
        let parts = [
            Part {
                cell: "2019".to_string(),
                type_word: Some("number"),
            },
            Part {
                cell: "jpg".to_string(),
                type_word: None,
            },
        ];
        assert_eq!(parts_text(&parts), "2019 (number) / jpg");
    }

    // =========================================================================
    // The key listing
    // =========================================================================

    fn stored(key: &str, types: &[(FactType, u64)]) -> KeyInfo {
        KeyInfo {
            key: key.to_string(),
            cell: crate::expr::printed_key(key),
            mark: false,
            count: 1,
            kind: KeyKind::ContentFact,
            types: crate::core::domain::fact::KeyTypes::from_counts(types.iter().copied()).unwrap(),
        }
    }

    fn listing(keys: Vec<KeyInfo>) -> AllKeysResult {
        AllKeysResult {
            keys,
            lines: Vec::new(),
            total_sources: 1,
        }
    }

    #[test]
    fn the_listing_prints_the_type_words() {
        for (types, word) in [
            (vec![(FactType::Text, 3)], "text"),
            (vec![(FactType::Num, 3)], "number"),
            (vec![(FactType::Time, 3)], "time"),
            (vec![(FactType::Text, 2), (FactType::Num, 1)], "mixed"),
        ] {
            let row = key_row(&stored("content.K", &types), 1, MIN_KEY_COLUMN);
            assert_eq!(
                row,
                format!("{:<30} {word:>6} {:>10} {:>9.1}%", "content.K", 1, 100.0)
            );
        }
        let row = key_row(
            &stored("content.K", &[(FactType::Num, 1)]),
            1,
            MIN_KEY_COLUMN,
        );
        assert!(!row.contains("num "));
    }

    /// **The key column fits its widest key**: a 60-character key prints whole, and every row
    /// of the listing pads to it, so the Type column stands in one place.
    #[test]
    fn the_key_column_fits_a_long_key() {
        let long = format!("content.{}", "k".repeat(52));
        assert_eq!(long.len(), 60);
        let result = listing(vec![
            stored(&long, &[(FactType::Text, 1)]),
            stored("content.Make", &[(FactType::Text, 1)]),
        ]);
        let column = key_column(&result);
        assert_eq!(column, 60);
        let rows: Vec<String> = result
            .keys
            .iter()
            .map(|key| key_row(key, 1, column))
            .collect();
        assert!(rows[0].starts_with(&long));
        assert_eq!(rows[0].find(" text"), rows[1].find(" text"));
    }

    /// A key holding a control character prints its pictures and its mark in the mark column,
    /// before the kind suffix.
    #[test]
    fn a_marked_key_prints_its_mark_after_coverage() {
        let mut key = stored("content.a\tb", &[(FactType::Text, 1)]);
        key.cell = crate::expr::pictured(&crate::expr::printed_key(&key.key));
        key.mark = true;
        let row = key_row(&key, 1, MIN_KEY_COLUMN);
        assert!(row.starts_with("\"content.a\u{2409}b\""), "{row}");
        assert!(row.ends_with("100.0%  !"), "{row}");
    }

    #[test]
    fn one_mixed_key_is_named_with_its_command() {
        let result = listing(vec![
            stored("content.Make", &[(FactType::Text, 4)]),
            stored(
                "content.File Size",
                &[(FactType::Text, 2), (FactType::Num, 5)],
            ),
        ]);
        assert_eq!(
            mixed_key_lines(&result),
            vec![
                r#""content.File Size" is of mixed type (number and text); filters and patterns support one type only."#,
                "To see its values and convert it to one type:",
                "  `canon facts convert 'content.File Size'`",
            ]
        );
    }

    #[test]
    fn several_mixed_keys_are_each_named() {
        let result = listing(vec![
            stored(
                "content.CreateDate",
                &[(FactType::Text, 1), (FactType::Time, 9)],
            ),
            stored("content.Make", &[(FactType::Text, 4)]),
            stored(
                "content.File Size",
                &[(FactType::Text, 2), (FactType::Num, 5)],
            ),
        ]);
        assert_eq!(
            mixed_key_lines(&result),
            vec![
                "2 keys are of mixed type; filters and patterns support one type only:",
                "  content.CreateDate (time and text)",
                r#"  "content.File Size" (number and text)"#,
                "To see their values and convert each to one type:",
                "  `canon facts convert content.CreateDate`",
                "  `canon facts convert 'content.File Size'`",
            ]
        );
    }

    #[test]
    fn no_mixed_key_prints_no_lines() {
        let result = listing(vec![
            stored("content.Make", &[(FactType::Text, 4)]),
            stored("content.Width", &[(FactType::Num, 4)]),
        ]);
        assert!(mixed_key_lines(&result).is_empty());
    }

    // =========================================================================
    // delete_facts — the population it acts on
    // =========================================================================

    /// A destructive command must not reach past what a look would show. The
    /// selection here is the one `ls --where` uses at the same scope, so the
    /// dismissed source and the archive copy — neither of which any preview
    /// lists — keep their facts.
    #[test]
    fn delete_acts_on_the_population_a_preview_would_have_listed() {
        crate::core::domain::time::zone::with_test_zone(
            crate::core::domain::time::zone::UserZone::utc(),
            || {
                use crate::core::repo::{open_in_memory_for_test, Db};
                use crate::core::testing::{insert_object, insert_root, insert_source};

                let conn = open_in_memory_for_test();
                let source_root = insert_root(&conn, "/photos", "source", false);
                let archive_root = insert_root(&conn, "/archive", "archive", false);
                let obj = insert_object(&conn, "h1", false);

                let visible = insert_source(&conn, source_root, "visible.jpg", Some(obj));
                let dismissed = crate::core::testing::insert_source_excluded(
                    &conn,
                    source_root,
                    "dismissed.jpg",
                    None,
                );
                let archived = insert_source(&conn, archive_root, "kept.jpg", Some(obj));
                for id in [visible, dismissed, archived] {
                    crate::core::testing::insert_fact(&conn, id, "content.Make", "Canon");
                }

                let mut db = Db::from_connection(conn);
                let options = DeleteOptions {
                    entity_type: "source".to_string(),
                    value_type: None,
                    dry_run: false,
                    include: IncludeSet::default(),
                };
                delete_facts(
                    &mut db,
                    "content.Make",
                    &[],
                    &ExpressionText::plain(&[]),
                    &options,
                    "canon facts delete content.Make --on source --yes",
                    &LedgerConfig::default(),
                    true,
                )
                .unwrap();

                let surviving = |id: i64| -> i64 {
                    db.conn()
                .query_row(
                    "SELECT COUNT(*) FROM facts WHERE entity_type = 'source' AND entity_id = ?",
                    rusqlite::params![id],
                    |row| row.get(0),
                )
                .unwrap()
                };
                assert_eq!(surviving(visible), 0, "the previewable fact is deleted");
                assert_eq!(surviving(dismissed), 1, "a dismissed source is not listed");
                assert_eq!(surviving(archived), 1, "nor is an archive copy");
            },
        )
    }

    /// The key is read before anything else the command does, so a name starting with
    /// `source.` is refused even where the filter selects nothing — an empty selection
    /// never answers "No sources match" in its place.
    #[test]
    fn a_source_name_is_refused_before_the_selection() {
        crate::core::domain::time::zone::with_test_zone(
            crate::core::domain::time::zone::UserZone::utc(),
            || {
                use crate::core::repo::{open_in_memory_for_test, Db};
                use crate::core::testing::{insert_root, insert_source};

                let conn = open_in_memory_for_test();
                let root = insert_root(&conn, "/photos", "source", false);
                insert_source(&conn, root, "a.jpg", None);
                let mut db = Db::from_connection(conn);
                let options = DeleteOptions {
                    entity_type: "source".to_string(),
                    value_type: None,
                    dry_run: true,
                    include: IncludeSet::default(),
                };
                let refused = delete_facts(
                    &mut db,
                    "source.size",
                    &[],
                    &ExpressionText::plain(&["source.size = 987654321"]),
                    &options,
                    "canon facts delete source.size",
                    &LedgerConfig::default(),
                    true,
                )
                .expect_err("refused");
                assert_eq!(
                    refused.to_string(),
                    "Cannot delete 'source.size': names starting with 'source.' cannot be deleted."
                );
            },
        )
    }

    /// **The built-in keys footer lists every built-in key, each with its meaning**: the
    /// header, then the nine keys of `reference/builtins.md` in the status words' form. Asserted
    /// against the vocabulary's own built-in keys too, so a key added there and not here, or
    /// dropped here, fails.
    #[test]
    fn the_built_in_keys_footer_lists_every_built_in_key() {
        let lines = built_in_key_lines();
        assert_eq!(
            lines,
            [
                "Built-in keys (use in --where, --key, --group-by, --emit and patterns):",
                "  filename       last path component",
                "  stem           file name without its extension",
                "  ext            same as source.ext",
                "  hash           content hash, content.hash.sha256",
                "  hash_short     first 8 characters of the content hash",
                "  id             same as source.id",
                "  size           same as source.size",
                "  mtime          same as source.mtime",
                "  root_id        ID of the source's root",
            ]
        );
        let listed: Vec<&str> = lines[1..]
            .iter()
            .map(|line| line.split_whitespace().next().unwrap())
            .collect();
        let vocabulary: Vec<String> = Key::all()
            .filter(|key| key.kind() == KeyKind::BuiltinKey)
            .map(|key| key.name().to_string())
            .collect();
        assert_eq!(listed, vocabulary);
    }
}
