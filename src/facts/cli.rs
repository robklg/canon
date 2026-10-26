use anyhow::{bail, Result};

pub(super) mod convert;
pub(super) mod import;

use crate::core::domain::config::{LedgerConfig, RecordingMode};
use crate::core::domain::decision::DecisionCommand;
use crate::core::domain::fact::{FactType, KeyTypes};
use crate::core::domain::format::first_chars;
use crate::core::domain::format_count;
use crate::core::domain::scope::DecisionScope;
use crate::core::domain::time::zone::ReadTimesOn;
use crate::core::domain::IncludeSet;
use crate::core::ops::decision::DecisionParams;
use crate::core::ops::scope::{classify_all, ResolvedScope};
use crate::core::repo::Db;
use crate::expr::{read_filters, read_report, ExpressionText};
use crate::expr::{select_sources, RolePolicy, SelectionNotes, SelectionParams};
use crate::expr::{Key, KeyKind, Listing, ParsedFactKey};
use crate::facts::domain::{delete_key, names_the_root};
use crate::facts::ops::maintain::{
    execute_delete, execute_prune_excluded, execute_prune_orphaned, execute_prune_stale,
    plan_delete, plan_prune_excluded, plan_prune_orphaned, plan_prune_stale,
    validate_prune_excluded_scope,
};
use crate::facts::ops::report::{
    compute_all_keys, compute_distribution, compute_grouped_distribution,
    compute_root_distribution, AllKeysResult, DistributionResult, GroupedDistributionResult,
    KeyInfo, RootDistributionResult,
};

/// What kind of key the listing and a report's header say a key is: nothing after a content
/// fact.
fn kind_label(kind: KeyKind) -> Option<&'static str> {
    match kind {
        KeyKind::SourceFact => Some("(source fact)"),
        KeyKind::BuiltinKey => Some("(built-in key)"),
        KeyKind::ContentFact => None,
    }
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
    // a wrong-type modifier, a value a key cannot read — is refused up front, together.
    let grouping: Vec<String> = by_root
        .then(|| "source.root".to_string())
        .into_iter()
        .chain(group_by.iter().cloned())
        .collect();
    let reading = read_report(conn, key_arg, &grouping, filter_text)?;
    let (main_key, grouping_keys, filters) = (reading.key, reading.grouping, reading.filters);

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

    if let Some(main_key) = main_key {
        if !grouping_keys.is_empty() {
            let result = compute_grouped_distribution(
                conn,
                &source_ids,
                &main_key,
                &grouping_keys,
                limit,
                crate::user_zone(),
            )?;
            display_grouped_distribution(&result, &main_key, &grouping_keys, total_sources);
        } else {
            let result =
                compute_distribution(conn, &source_ids, &main_key, limit, crate::user_zone())?;
            display_distribution(&result, &main_key);
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

fn display_root_distribution(result: &RootDistributionResult) {
    use std::io::Write;
    let stdout = std::io::stdout();
    let mut handle = stdout.lock();

    // Adaptive path width: compute max needed, cap at 60
    let max_path = result
        .entries
        .iter()
        .map(|e| format!("id:{:<2} {}", e.root_id, e.root_path).len())
        .max()
        .unwrap_or(30);
    let col_width = max_path.min(60);

    let _ = writeln!(
        handle,
        "{:<width$} {:>10}",
        "Root",
        "Sources",
        width = col_width,
    );
    let _ = writeln!(handle, "{}", "─".repeat(col_width + 18));

    for entry in &result.entries {
        let pct = if result.total_sources > 0 {
            (entry.count as f64 / result.total_sources as f64) * 100.0
        } else {
            0.0
        };
        let label = format_root_display_adaptive(entry.root_id, &entry.root_path, col_width);
        if writeln!(
            handle,
            "{:<width$} {:>10} ({:>5.1}%)",
            label,
            format_count(entry.count),
            pct,
            width = col_width,
        )
        .is_err()
        {
            return;
        }
    }
}

fn display_all_keys(result: &AllKeysResult, show_all: bool, show_status_predicates: bool) {
    use std::io::Write;
    let stdout = std::io::stdout();
    let mut handle = stdout.lock();
    let _ = writeln!(
        handle,
        "{:<30} {:>6} {:>10} {:>10}",
        "Key", "Type", "Count", "Coverage"
    );
    let _ = writeln!(handle, "{}", "─".repeat(60));

    for key_info in &result.keys {
        if writeln!(handle, "{}", key_row(key_info, result.total_sources)).is_err() {
            break;
        }
    }

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

/// One key's row in the listing: its name, the key's type word, the scope's count and
/// coverage, and what kind of key it is.
fn key_row(key_info: &KeyInfo, total_sources: usize) -> String {
    let coverage = (key_info.count as f64 / total_sources as f64) * 100.0;
    let suffix = kind_label(key_info.kind)
        .map(|label| format!("  {label}"))
        .unwrap_or_default();
    format!(
        "{:<30} {:>6} {:>10} {:>9.1}%{}",
        key_info.key,
        key_info.types.word(),
        key_info.count,
        coverage,
        suffix
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

/// A value as the distribution table's first column shows it: labelled when
/// empty, cut to fit the 40-character column when long. The cut counts
/// characters, as the column's padding does, never bytes.
fn value_cell(value: &str) -> String {
    if value.is_empty() {
        EMPTY.to_string()
    } else if value.chars().count() > 38 {
        format!("{}...", first_chars(value, 35))
    } else {
        value.to_string()
    }
}

/// What an empty value is shown as in a report, for every key: one label for every table a
/// report prints, its main values and its groups alike. A file with no extension has no
/// value for it, so it is counted with the sources that have none.
const EMPTY: &str = "(empty)";

/// A grouping value as a report's group line shows it: labelled when empty.
fn group_cell(value: &str) -> String {
    if value.is_empty() {
        EMPTY.to_string()
    } else {
        value.to_string()
    }
}

fn display_distribution(result: &DistributionResult, key: &ParsedFactKey) {
    use std::io::Write;
    // The header names the key as it was read, and what kind of key it is, as the listing does
    let label = match kind_label(key.key.as_ref().map_or(KeyKind::ContentFact, Key::kind)) {
        Some(kind) => format!("{} {kind}", key.raw),
        None => key.raw.clone(),
    };

    let stdout = std::io::stdout();
    let mut handle = stdout.lock();
    let _ = writeln!(handle, "{:<40} {:>10} {:>10}", label, "Count", "Coverage");
    let _ = writeln!(handle, "{}", "─".repeat(62));

    for entry in &result.entries {
        let display_val = value_cell(&entry.value);
        let coverage = (entry.count as f64 / result.total_sources as f64) * 100.0;
        if writeln!(
            handle,
            "{display_val:<40} {:>10} {:>9.1}%",
            entry.count, coverage
        )
        .is_err()
        {
            break;
        }
    }

    // Show "(no value)" count
    let without_value = result.total_sources as i64 - result.sources_with_value;
    if without_value > 0 {
        let coverage = (without_value as f64 / result.total_sources as f64) * 100.0;
        let _ = writeln!(
            handle,
            "{:<40} {:>10} {:>9.1}%",
            "(no value)", without_value, coverage
        );
    }

    // Warn about truncation if limit was applied
    if result.entries.len() < result.total_distinct_values {
        eprintln!(
            "Showing {} of {} values (use --limit 0 for all)",
            result.entries.len(),
            result.total_distinct_values
        );
    }
}

/// Format root for display: id:N path (with fixed max path length for grouped output)
fn format_root_display(root_id: i64, root_path: &str) -> String {
    format_root_display_adaptive(root_id, root_path, 40)
}

/// Format root for display with adaptive width: id:N path, truncating path if needed.
fn format_root_display_adaptive(root_id: i64, root_path: &str, max_total: usize) -> String {
    let id_prefix = format!("id:{root_id:<2}");
    let prefix_len = id_prefix.len() + 1; // +1 for space
    let max_path = if max_total > prefix_len + 3 {
        max_total - prefix_len
    } else {
        10 // minimum
    };
    format!(
        "{id_prefix} {}",
        crate::core::domain::format::cap_path(root_path, max_path)
    )
}

fn display_grouped_distribution(
    result: &GroupedDistributionResult,
    main_key: &ParsedFactKey,
    grouping_keys: &[ParsedFactKey],
    total_sources: usize,
) {
    use std::io::Write;
    // Build grouping label
    let grouping_label = if grouping_keys.len() == 1 && names_the_root(&grouping_keys[0]) {
        "by root".to_string()
    } else {
        let labels: Vec<&str> = grouping_keys.iter().map(|gk| gk.raw.as_str()).collect();
        format!("grouped by {}", labels.join(", "))
    };

    let stdout = std::io::stdout();
    let mut handle = stdout.lock();
    let _ = writeln!(handle, "{} ({})\n", main_key.raw, grouping_label);

    for group in &result.groups {
        let coverage = (group.total_count as f64 / total_sources as f64) * 100.0;
        let main_display = group_cell(&group.main_value);
        if writeln!(
            handle,
            "{} (total: {:>6}, {:>5.1}%)",
            main_display,
            format_count(group.total_count),
            coverage
        )
        .is_err()
        {
            return;
        }

        for sub in &group.sub_groups {
            let sub_coverage = (sub.count as f64 / group.total_count as f64) * 100.0;

            // Format group display
            let group_display = if grouping_keys.len() == 1 && names_the_root(&grouping_keys[0]) {
                if let (Some(rid), Some(rpath)) = (sub.root_id, sub.root_path.as_ref()) {
                    format_root_display(rid, rpath)
                } else {
                    group_cell(&sub.group_values[0])
                }
            } else {
                let parts: Vec<String> = grouping_keys
                    .iter()
                    .enumerate()
                    .map(|(i, gk)| {
                        if names_the_root(gk) {
                            if let (Some(rid), Some(rpath)) = (sub.root_id, sub.root_path.as_ref())
                            {
                                format_root_display(rid, rpath)
                            } else {
                                group_cell(&sub.group_values[i])
                            }
                        } else {
                            group_cell(&sub.group_values[i])
                        }
                    })
                    .collect();
                parts.join(" / ")
            };

            if writeln!(
                handle,
                "  {:<40} {:>8} {:>6.1}%",
                group_display,
                format_count(sub.count),
                sub_coverage
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

    // Show "(no value)" count for main key
    let without_main_value = total_sources as i64 - result.sources_with_value;
    if without_main_value > 0 {
        let coverage = (without_main_value as f64 / total_sources as f64) * 100.0;
        let _ = writeln!(
            handle,
            "(no value) (total: {:>6}, {:>5.1}%)",
            format_count(without_main_value),
            coverage
        );
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

    /// Both of a report's cells read one empty label, `(empty)`, for every key —
    /// `value_cell` for the plain table's values, `group_cell` for the grouped table's main and
    /// grouping values. That the grouped display calls `group_cell` is interface text this pin
    /// cannot see; it was read on the binary.
    #[test]
    fn an_empty_value_is_labelled_alike_in_every_table() {
        assert_eq!(value_cell(""), "(empty)");
        assert_eq!(group_cell(""), "(empty)");
        assert_eq!(group_cell("jpg"), "jpg");
    }

    // =========================================================================
    // The key listing
    // =========================================================================

    fn stored(key: &str, types: &[(FactType, u64)]) -> KeyInfo {
        KeyInfo {
            key: key.to_string(),
            count: 1,
            kind: KeyKind::ContentFact,
            types: crate::core::domain::fact::KeyTypes::from_counts(types.iter().copied()).unwrap(),
        }
    }

    fn listing(keys: Vec<KeyInfo>) -> AllKeysResult {
        AllKeysResult {
            keys,
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
            let row = key_row(&stored("content.K", &types), 1);
            assert_eq!(
                row,
                format!("{:<30} {word:>6} {:>10} {:>9.1}%", "content.K", 1, 100.0)
            );
        }
        assert!(!key_row(&stored("content.K", &[(FactType::Num, 1)]), 1).contains("num "));
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
    // value_cell tests
    // =========================================================================

    /// A long value whose 35th byte falls inside an accented letter is cut
    /// between letters, never inside one.
    #[test]
    fn a_long_accented_value_is_cut_between_characters() {
        let cell = value_cell(&"é".repeat(40));
        assert_eq!(cell, format!("{}...", "é".repeat(35)));
    }

    /// The cut is measured in characters, as the column's padding is: a value
    /// that fits the column in characters prints whole, however many bytes.
    #[test]
    fn a_value_that_fits_in_characters_prints_whole() {
        let value = "é".repeat(38);
        assert_eq!(value_cell(&value), value);
        assert_eq!(value_cell(&"a".repeat(39)).chars().count(), 38);
    }

    // =========================================================================
    // format_root_display tests
    // =========================================================================

    #[test]
    fn format_root_display_short_path() {
        // Path that fits within MAX_PATH_LEN (30 chars)
        let result = format_root_display(1, "/home/user");
        assert_eq!(result, "id:1  /home/user");
    }

    #[test]
    fn format_root_display_exact_length() {
        // Path exactly at MAX_PATH_LEN (30 chars)
        let path = "/home/user/exactly30charslongg";
        assert_eq!(path.len(), 30);
        let result = format_root_display(1, path);
        assert_eq!(result, "id:1  /home/user/exactly30charslongg");
    }

    #[test]
    fn format_root_display_long_path_truncated() {
        // Path longer than MAX_PATH_LEN - should be truncated with ...
        let long_path = "/home/user/very/long/path/that/exceeds/the/limit";
        let result = format_root_display(42, long_path);
        // Should start with "id:42 ..."
        assert!(result.starts_with("id:42 ..."));
        // Should end with last portion of path
        assert!(result.ends_with("exceeds/the/limit"));
    }

    #[test]
    fn format_root_display_id_formatting() {
        // ID should be left-aligned in 2-char field
        assert!(format_root_display(1, "/tmp").starts_with("id:1 "));
        assert!(format_root_display(99, "/tmp").starts_with("id:99"));
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
