use anyhow::{bail, Result};

pub(super) mod import;

use crate::core::domain::config::{LedgerConfig, RecordingMode};
use crate::core::domain::decision::DecisionCommand;
use crate::core::domain::format_count;
use crate::core::domain::scope::DecisionScope;
use crate::core::domain::IncludeSet;
use crate::core::ops::decision::DecisionParams;
use crate::core::ops::scope::{classify_all, ResolvedScope};
use crate::core::repo::Db;
use crate::expr::Filter;
use crate::expr::{select_sources, RolePolicy, SelectionParams};
use crate::expr::{BuiltinKey, BuiltinKeyCategory, BuiltinKeyVisibility, ParsedFactKey};
use crate::facts::ops::maintain::{
    execute_delete, execute_prune_excluded, execute_prune_orphaned, execute_prune_stale,
    plan_delete, plan_prune_excluded, plan_prune_orphaned, plan_prune_stale, validate_delete_key,
    validate_prune_excluded_scope,
};
use crate::facts::ops::report::{
    compute_all_keys, compute_distribution, compute_grouped_distribution,
    compute_root_distribution, AllKeysResult, DistributionResult, GroupedDistributionResult,
    RootDistributionResult,
};

/// Check if a parsed key represents source.root (for special display formatting)
fn is_root_key(key: &ParsedFactKey) -> bool {
    key.raw == "source.root" || key.raw == "root_id"
}

fn get_fact_category(key: &str) -> BuiltinKeyCategory {
    BuiltinKey::from_str(key)
        .map(|k| k.category())
        .unwrap_or(BuiltinKeyCategory::Stored)
}

#[allow(clippy::too_many_arguments)]
pub fn run(
    db: &mut Db,
    key_arg: Option<&str>,
    scope_prefixes: &[String],
    filter_strs: &[String],
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

    // Build grouping keys list
    let mut grouping_keys: Vec<ParsedFactKey> = Vec::new();
    if by_root {
        grouping_keys.push(ParsedFactKey::parse("source.root")?);
    }
    for key in group_by {
        grouping_keys.push(ParsedFactKey::parse(key)?);
    }

    let conn = db.conn_mut();

    // Parse filters
    let filters: Vec<Filter> = filter_strs
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    let scopes = classify_all(scope_prefixes);
    let params = SelectionParams {
        scopes,
        include: include.clone(),
        filters,
        role_policy: RolePolicy::SourceUnlessIncluded,
    };
    let sel = select_sources(conn, &params)?;
    let source_ids = sel.source_ids();
    let total_sources = source_ids.len();

    if total_sources == 0 {
        println!("No sources match the given filters.");
        if !include.includes_excluded() && sel.excluded_count > 0 {
            println!(
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
        crate::scope::print_report_scope(&mut handle, "Facts", scope);
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

    if let Some(fact_key) = key_arg {
        let main_key = ParsedFactKey::parse(fact_key)?;

        if !grouping_keys.is_empty() {
            let result =
                compute_grouped_distribution(conn, &source_ids, &main_key, &grouping_keys, limit)?;
            display_grouped_distribution(&result, &main_key, &grouping_keys, total_sources);
        } else {
            let result = compute_distribution(conn, &source_ids, &main_key, limit)?;
            display_distribution(&result, fact_key, &main_key);
        }
    } else if by_root {
        let result = compute_root_distribution(&sel.sources);
        display_root_distribution(&result);
    } else {
        let result = compute_all_keys(conn, &source_ids, show_all)?;
        display_all_keys(&result, show_all, filter_strs.is_empty());
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
        "Fact", "Type", "Count", "Coverage"
    );
    let _ = writeln!(handle, "{}", "─".repeat(60));

    for key_info in &result.keys {
        let coverage = (key_info.count as f64 / result.total_sources as f64) * 100.0;
        let suffix = match key_info.category {
            BuiltinKeyCategory::BuiltIn => "  (built-in)",
            BuiltinKeyCategory::Derived => "  (derived)",
            BuiltinKeyCategory::Stored => "",
        };
        if writeln!(
            handle,
            "{:<30} {:>6} {:>10} {:>9.1}%{}",
            key_info.key,
            key_info.fact_type.as_str(),
            key_info.count,
            coverage,
            suffix
        )
        .is_err()
        {
            break;
        }
    }

    if !show_all {
        use strum::IntoEnumIterator;
        let hidden_count = BuiltinKey::iter()
            .filter(|k| k.visibility() == BuiltinKeyVisibility::Hidden)
            .count();
        let _ = writeln!(
            handle,
            "\n({hidden_count} built-in/derived facts hidden, use --all to show)"
        );
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
    }
}

fn display_distribution(result: &DistributionResult, display_key: &str, key: &ParsedFactKey) {
    use std::io::Write;
    // Build label with category for builtin keys
    let category = get_fact_category(&key.base_key);
    let label = match category {
        BuiltinKeyCategory::BuiltIn => format!("{display_key} (built-in)"),
        BuiltinKeyCategory::Derived => format!("{display_key} (derived)"),
        BuiltinKeyCategory::Stored => display_key.to_string(),
    };

    let stdout = std::io::stdout();
    let mut handle = stdout.lock();
    let _ = writeln!(handle, "{:<40} {:>10} {:>10}", label, "Count", "Coverage");
    let _ = writeln!(handle, "{}", "─".repeat(62));

    for entry in &result.entries {
        let display_val = if entry.value.is_empty() {
            if key.base_key == "source.ext" {
                "(no extension)".to_string()
            } else {
                "(empty)".to_string()
            }
        } else if entry.value.len() > 38 {
            format!("{}...", &entry.value[..35])
        } else {
            entry.value.clone()
        };
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

    // Warn about skipped values due to type mismatch
    if result.skipped_type_mismatch > 0 {
        eprintln!(
            "Warning: skipped {} values with incompatible type for transform",
            result.skipped_type_mismatch
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
    let grouping_label = if grouping_keys.len() == 1 && is_root_key(&grouping_keys[0]) {
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
        let main_display = if group.main_value.is_empty() {
            "(empty)"
        } else {
            &group.main_value
        };
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
            let group_display = if grouping_keys.len() == 1 && is_root_key(&grouping_keys[0]) {
                if let (Some(rid), Some(rpath)) = (sub.root_id, sub.root_path.as_ref()) {
                    format_root_display(rid, rpath)
                } else {
                    sub.group_values[0].clone()
                }
            } else {
                let parts: Vec<String> = grouping_keys
                    .iter()
                    .enumerate()
                    .map(|(i, gk)| {
                        if is_root_key(gk) {
                            if let (Some(rid), Some(rpath)) = (sub.root_id, sub.root_path.as_ref())
                            {
                                format_root_display(rid, rpath)
                            } else {
                                sub.group_values[i].clone()
                            }
                        } else {
                            sub.group_values[i].clone()
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
    pub entity_type: String,        // "source" or "object"
    pub value_type: Option<String>, // "text", "num", or "time"
    pub dry_run: bool,
}

#[allow(clippy::too_many_arguments)]
pub fn delete_facts(
    db: &mut Db,
    key: &str,
    scope_prefixes: &[String],
    filter_strs: &[String],
    options: &DeleteOptions,
    command_line: &str,
    config: &LedgerConfig,
    no_receipt: bool,
) -> Result<()> {
    // Validate key and entity type
    validate_delete_key(key)?;
    if options.entity_type != "source" && options.entity_type != "object" {
        bail!(
            "Invalid entity type '{}'. Must be 'source' or 'object'.",
            options.entity_type
        );
    }

    let conn = db.conn_mut();

    // Parse filters
    let filters: Vec<Filter> = filter_strs
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    let scopes = classify_all(scope_prefixes);

    // Select all sources (include all for delete operations)
    let include_all = IncludeSet {
        excluded: true,
        archived: true,
    };
    let params = SelectionParams {
        scopes,
        include: include_all,
        filters,
        role_policy: RolePolicy::SourceUnlessIncluded,
    };
    let source_ids = select_sources(conn, &params)?.source_ids();

    if source_ids.is_empty() {
        println!("No sources match the given filters.");
        return Ok(());
    }

    // Plan
    let plan = plan_delete(
        conn,
        &source_ids,
        key,
        &options.entity_type,
        options.value_type.as_deref(),
    )?;

    let entity_label = if options.entity_type == "source" {
        "sources"
    } else {
        "objects"
    };

    if plan.fact_count == 0 {
        println!("No '{key}' facts found on matching {entity_label}.");
    } else if options.dry_run {
        println!(
            "Would delete {} fact rows across {} {}",
            format_count(plan.fact_count),
            format_count(plan.entity_count),
            entity_label
        );
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
        };
        let result = execute_delete(
            conn,
            &source_ids,
            key,
            &options.entity_type,
            options.value_type.as_deref(),
            &plan,
            Some(&decision),
        )?;
        println!("{}", result.summary);
    }

    Ok(())
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

    if plan.total_count() == 0 {
        println!("No facts found for excluded entities.");
        return Ok(());
    }

    let prune_sources = scope == "all" || scope == "source";
    let prune_objects = scope == "all" || scope == "object";

    if dry_run {
        println!("Facts for excluded entities:");
        if prune_sources {
            println!(
                "  Source facts (excluded sources): {}",
                format_count(plan.source_fact_count)
            );
        }
        if prune_objects {
            println!(
                "  Object facts (excluded objects): {}",
                format_count(plan.object_fact_count)
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
        };
        let result = execute_prune_excluded(conn, scope, Some(&decision))?;
        if !result.summary.is_empty() {
            println!("{}", result.summary);
        }
    }

    Ok(())
}

// ============================================================================
// Show Aliases
// ============================================================================

pub fn show_aliases() {
    use crate::expr::BuiltinKey;
    use strum::IntoEnumIterator;

    println!("Pattern Aliases:");
    println!();

    for key in BuiltinKey::iter() {
        if let Some(expansion) = key.expansion() {
            let name: &'static str = key.into();
            println!("  {name:<15} \u{2192} {expansion}");
        }
    }

    println!();
    println!("Note: 'filename' and 'ext' also work in --where filters.");
    println!("Other aliases (stem, hash, hash_short, id) only work in manifest patterns.");
}

#[cfg(test)]
mod tests {
    use super::*;

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

    // is_protected_fact tests moved to facts::ops::maintain::tests::test_validate_delete_key_protected
}
