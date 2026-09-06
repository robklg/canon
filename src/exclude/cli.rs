use anyhow::Result;
use std::path::Path;

use crate::ceremony;
use crate::core::domain::config::{LedgerConfig, RecordingMode};
use crate::core::domain::decision::DecisionCommand;
use crate::core::domain::path::validate_paths_in_roots;
use crate::core::domain::root::find_containing_root;
use crate::core::domain::scope::DecisionScope;
use crate::core::ops::decision::DecisionParams;
use crate::core::ops::receipt::{resolve_ledger_root, LedgerRootOutcome, ReceiptPlacement};
use crate::core::ops::scope::{classify_all_indexed, resolve_path};
use crate::core::repo::{Connection, Db};
use crate::exclude::ops::execute::{
    execute_clear, execute_duplicates, execute_set, execute_set_objects,
};
use crate::exclude::ops::plan::{plan_clear, plan_duplicates, plan_set, plan_set_objects};
use crate::exclude::ops::single::{
    check_clear_object, check_set_object_by_file, check_set_object_by_hash, check_set_source_by_id,
    check_set_source_by_path, execute_clear_object, execute_set_object, execute_set_source,
    list_excluded_objects, ObjectClearCheck, ObjectExclusionCheck, SourceExclusionCheck,
};
use crate::exclude::ops::types::{
    ExcludeClearParams, ExcludeDuplicatesParams, ExcludeSetObjectsParams, ExcludeSetParams,
    ObjectSourceInfo, ReceiptDestination,
};
use crate::expr::Filter;

/// Build the decision params, decomposing the given canonical scope prefixes to
/// their roots (the one funnel). Pass an empty slice for a global decision. A
/// prefix under no known root is dropped — a stray non-canonical scope is
/// unrecordable by construction.
#[allow(clippy::too_many_arguments)]
fn make_decision(
    conn: &Connection,
    command: DecisionCommand,
    scope_prefixes: &[String],
    command_line: &str,
    config: &LedgerConfig,
    no_receipt: bool,
    reason: Option<&str>,
    dry_run: bool,
) -> Result<DecisionParams> {
    let roots = crate::core::repo::root::fetch_all(conn)?;
    Ok(DecisionParams {
        command,
        scope: DecisionScope::decompose(scope_prefixes, &roots),
        command_line: command_line.to_string(),
        reason: reason
            .map(|r| r.to_string())
            .filter(|r| !r.trim().is_empty()),
        record_enabled: config.recording != RecordingMode::Off && !dry_run,
        receipt_enabled: config.recording == RecordingMode::Full && !no_receipt && !dry_run,
        ledger_config: config.clone(),
    })
}

/// Resolve where exclusion receipts land (flat at the ledger root) — and,
/// when they land nowhere, the reason, which travels into the decision row's
/// summary. The dismissal judgment is recorded either way; what a reader of
/// the row must not have to guess is *why* it carries no receipt, the
/// consumption-readiness ADR's self-explaining gap. A receipt that was never
/// owed — `recording = Records`, `--no-receipt`, a dry run — is no gap and
/// carries no reason.
///
/// No way back is named here, deliberately: unlike the retirement doors,
/// nothing is blocked, and unsuspending afterwards does not write the receipt
/// this decision did not write — a remedy that does not remedy is worse than
/// none (the sweep's own footer reasoning, applied to a surface it does not
/// cover).
fn resolve_placement(
    conn: &Connection,
    config: &LedgerConfig,
    decision: &DecisionParams,
) -> Result<ReceiptDestination> {
    let roots = crate::core::repo::root::fetch_all(conn)?;
    let outcome = resolve_ledger_root(&roots, config);
    let placement = match &outcome {
        LedgerRootOutcome::Found { root_id, root_path } => Some(ReceiptPlacement::LedgerRoot {
            root_id: *root_id,
            root_path: root_path.clone(),
        }),
        LedgerRootOutcome::NoArchiveRoot | LedgerRootOutcome::AllArchiveRootsSuspended { .. } => {
            None
        }
    };
    let gap = if decision.receipt_enabled {
        outcome.unplaceable_reason()
    } else {
        None
    };
    Ok(ReceiptDestination { placement, gap })
}

/// Print receipt-write warnings (one per line) to stderr.
fn print_warnings(warnings: &[String]) {
    for w in warnings {
        eprintln!("{w}");
    }
}

// ============================================================================
// Options
// ============================================================================

pub struct SetOptions {
    pub dry_run: bool,
    pub verbose: bool,
    pub yes: bool,
}

pub struct ClearOptions {
    pub dry_run: bool,
    pub yes: bool,
}

// ============================================================================
// Set Command
// ============================================================================

#[allow(clippy::too_many_arguments)]
pub fn set(
    db: &mut Db,
    scope_prefixes: &[String],
    filter_strs: &[String],
    options: &SetOptions,
    command_line: &str,
    config: &LedgerConfig,
    no_receipt: bool,
    reason: Option<&str>,
) -> Result<()> {
    let conn = db.conn_mut();

    // Parse filters
    let filters: Vec<Filter> = filter_strs
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    let scopes = classify_all_indexed(conn, scope_prefixes)?;
    let plan = plan_set(conn, &ExcludeSetParams { scopes, filters })?;

    if plan.source_ids().is_empty() {
        println!("No sources to exclude (0 matching non-excluded sources)");
        return Ok(());
    }

    if options.dry_run {
        println!("Would exclude {} sources:", plan.source_ids().len());
        for path in &plan.paths() {
            println!("  {path}");
        }
        return Ok(());
    }

    // Confirmation when affecting > 1 source
    if plan.source_ids().len() > 1 {
        if !options.yes {
            eprintln!("Will exclude {} sources", plan.source_ids().len());
            eprintln!("  Across {} roots", plan.root_count);
            eprintln!("  {} have no archived copy", plan.not_archived_count);
        }

        if !ceremony::confirm(options.yes)? {
            return Ok(());
        }
    }

    let decision = make_decision(
        conn,
        DecisionCommand::ExcludeSet,
        scope_prefixes,
        command_line,
        config,
        no_receipt,
        reason,
        options.dry_run,
    )?;
    let destination = resolve_placement(conn, config, &decision)?;
    let result = execute_set(conn, &plan, &destination, Some(&decision))?;
    println!("{}", result.summary);
    print_warnings(&result.warnings);
    Ok(())
}

// ============================================================================
// Clear Command
// ============================================================================

#[allow(clippy::too_many_arguments)]
pub fn clear(
    db: &mut Db,
    scope_prefixes: &[String],
    filter_strs: &[String],
    options: &ClearOptions,
    command_line: &str,
    config: &LedgerConfig,
    no_receipt: bool,
    reason: Option<&str>,
) -> Result<()> {
    let conn = db.conn_mut();

    // Parse filters
    let filters: Vec<Filter> = filter_strs
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    let scopes = classify_all_indexed(conn, scope_prefixes)?;
    let plan = plan_clear(conn, &ExcludeClearParams { scopes, filters })?;

    if plan.source_ids().is_empty() {
        println!("No excluded sources match the given filters");
        return Ok(());
    }

    if options.dry_run {
        println!(
            "Would clear exclusions for {} sources:",
            plan.source_ids().len()
        );
        for path in &plan.paths() {
            println!("  {path}");
        }
        return Ok(());
    }

    // Confirmation when affecting > 1 source
    if plan.source_ids().len() > 1 {
        if !options.yes {
            eprintln!(
                "Will clear exclusions for {} sources",
                plan.source_ids().len()
            );
            eprintln!("  Across {} roots", plan.root_count);
        }

        if !ceremony::confirm(options.yes)? {
            return Ok(());
        }
    }

    let decision = make_decision(
        conn,
        DecisionCommand::ExcludeClear,
        scope_prefixes,
        command_line,
        config,
        no_receipt,
        reason,
        options.dry_run,
    )?;
    let destination = resolve_placement(conn, config, &decision)?;
    let result = execute_clear(conn, &plan, &destination, Some(&decision))?;
    println!("{}", result.summary);
    print_warnings(&result.warnings);
    Ok(())
}

/// Exclude a specific source by ID
pub fn set_by_id(
    db: &mut Db,
    source_id: i64,
    options: &SetOptions,
    command_line: &str,
    config: &LedgerConfig,
    no_receipt: bool,
    reason: Option<&str>,
) -> Result<()> {
    let conn = db.conn_mut();

    match check_set_source_by_id(conn, source_id)? {
        SourceExclusionCheck::AlreadyExcluded { path } => {
            println!("Source already excluded: {path}");
        }
        SourceExclusionCheck::Ready { item } => {
            if options.dry_run {
                println!("Would exclude source (id: {}):", item.source_id);
                println!("  {}", item.path());
            } else {
                let decision = make_decision(
                    conn,
                    DecisionCommand::ExcludeSet,
                    &[item.path()],
                    command_line,
                    config,
                    no_receipt,
                    reason,
                    options.dry_run,
                )?;
                let destination = resolve_placement(conn, config, &decision)?;
                let result = execute_set_source(conn, &item, &destination, Some(&decision))?;
                println!("{}", result.summary);
                print_warnings(&result.warnings);
            }
        }
    }
    Ok(())
}

/// Exclude a specific source by exact file path
pub fn set_by_path(
    db: &mut Db,
    file_path: &Path,
    options: &SetOptions,
    command_line: &str,
    config: &LedgerConfig,
    no_receipt: bool,
    reason: Option<&str>,
) -> Result<()> {
    let conn = db.conn_mut();

    // Resolve path (soft resolution: matches known roots, falls back to fs)
    let roots = crate::core::repo::root::fetch_all(conn)?;
    let cwd = std::env::current_dir()?;
    let path_str = resolve_path(file_path, &roots, &cwd)?;
    // Dismissing one named file is an act aimed at a place, and behind a
    // closed door it is refused by name.
    crate::core::ops::scope::refuse_parked_locations(std::slice::from_ref(&path_str), &roots)?;

    // Find which root contains this path (domain layer)
    let Some((root_id, _root_path, _role, rel_path)) = find_containing_root(&path_str, &roots)
    else {
        anyhow::bail!("No source found for path: {}", file_path.display());
    };

    let display_path = format!("{}", file_path.display());
    match check_set_source_by_path(conn, root_id, &rel_path, &display_path)? {
        SourceExclusionCheck::AlreadyExcluded { path } => {
            println!("Source already excluded: {path}");
        }
        SourceExclusionCheck::Ready { item } => {
            if options.dry_run {
                println!("Would exclude:");
                println!("  {}", item.path());
            } else {
                let decision = make_decision(
                    conn,
                    DecisionCommand::ExcludeSet,
                    &[item.path()],
                    command_line,
                    config,
                    no_receipt,
                    reason,
                    options.dry_run,
                )?;
                let destination = resolve_placement(conn, config, &decision)?;
                let result = execute_set_source(conn, &item, &destination, Some(&decision))?;
                println!("{}", result.summary);
                print_warnings(&result.warnings);
            }
        }
    }
    Ok(())
}

// ============================================================================
// Duplicates Command
// ============================================================================

/// Exclude duplicate sources, keeping copies in the preferred path
///
/// Logic:
/// - scope (path) = which sources are candidates for exclusion
/// - prefer = where the surviving copies live — the user's election of the
///   preferred side
///
/// For each source in scope, we check if there's a duplicate in the prefer path.
/// If exactly one duplicate exists in prefer, we exclude the scoped source.
#[allow(clippy::too_many_arguments)]
pub fn exclude_duplicates(
    db: &mut Db,
    prefer_path: &Path,
    scope_path: Option<&Path>,
    filter_strs: &[String],
    dry_run: bool,
    yes: bool,
    command_line: &str,
    config: &LedgerConfig,
    no_receipt: bool,
    reason: Option<&str>,
) -> Result<()> {
    let conn = db.conn_mut();

    // Parse filters
    let filters: Vec<Filter> = filter_strs
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    // Resolve paths (soft resolution: matches known roots, falls back to fs)
    let all_roots = crate::core::repo::root::fetch_all(conn)?;
    let cwd = std::env::current_dir()?;
    let scope_prefixes: Vec<String> = if let Some(p) = scope_path {
        vec![resolve_path(p, &all_roots, &cwd)?]
    } else {
        vec![]
    };
    validate_paths_in_roots(&scope_prefixes, &all_roots)?;
    // Both locations are load-bearing here — which copies are preferred is
    // the whole question — so a closed door on either is refused by name
    // rather than narrowed past. The door precedes the existence gate: a
    // parked place is stated as parked, never as empty.
    crate::core::ops::scope::refuse_parked_locations(&scope_prefixes, &all_roots)?;
    let scope_prefixes =
        crate::core::ops::scope::validate_sources_exist(conn, &scope_prefixes, &all_roots)?;
    let prefer_prefix = resolve_path(prefer_path, &all_roots, &cwd)?;
    validate_paths_in_roots(std::slice::from_ref(&prefer_prefix), &all_roots)?;
    crate::core::ops::scope::refuse_parked_locations(
        std::slice::from_ref(&prefer_prefix),
        &all_roots,
    )?;
    let prefer_prefix = crate::core::ops::scope::validate_sources_exist(
        conn,
        std::slice::from_ref(&prefer_prefix),
        &all_roots,
    )?
    .pop()
    .expect("the gate returns one path per input");

    // Plan
    let scopes = classify_all_indexed(conn, &scope_prefixes)?;
    let params = ExcludeDuplicatesParams {
        scopes,
        filters,
        prefer_prefix,
    };
    let plan = plan_duplicates(conn, &params)?;

    if plan.source_ids().is_empty() {
        if plan.scope_count == 0 {
            println!("No sources match the given filters.");
        } else {
            println!("Nothing to exclude.");
        }
        return Ok(());
    }

    if dry_run {
        eprintln!(
            "Sources in scope: {} ({} unhashed skipped)",
            plan.scope_count, plan.skipped_no_hash
        );
        eprintln!("  Will exclude: {}", plan.source_ids().len());
        eprintln!(
            "  Skipped (no copy in --prefer): {}",
            plan.skipped_not_covered
        );
        eprintln!(
            "  Skipped (multiple copies in --prefer): {}",
            plan.skipped_multiple
        );
        if plan.skipped_in_prefer > 0 {
            eprintln!(
                "  Skipped (already in --prefer): {}",
                plan.skipped_in_prefer
            );
        }
        eprintln!();
        println!("Would exclude {} sources:", plan.source_ids().len());
        for path in &plan.paths() {
            println!("  {path}");
        }
        return Ok(());
    }

    // Interactive confirmation for > 1 source
    if plan.source_ids().len() > 1 {
        if !yes {
            eprintln!(
                "Will exclude {} sources ({} duplicate groups)",
                plan.source_ids().len(),
                plan.group_count
            );
            eprintln!("  Keeping copies in: {}", plan.prefer_prefix);
            if plan.skipped_not_covered > 0 {
                eprintln!(
                    "  Skipped {} (no copy in --prefer)",
                    plan.skipped_not_covered
                );
            }
            if plan.skipped_multiple > 0 {
                eprintln!(
                    "  Skipped {} (multiple copies in --prefer)",
                    plan.skipped_multiple
                );
            }
        }

        if !ceremony::confirm(yes)? {
            return Ok(());
        }
    }

    // Execute
    let decision = make_decision(
        conn,
        DecisionCommand::ExcludeDuplicates,
        &scope_prefixes,
        command_line,
        config,
        no_receipt,
        reason,
        dry_run,
    )?;
    let destination = resolve_placement(conn, config, &decision)?;
    let result = execute_duplicates(conn, &plan, &destination, Some(&decision))?;
    println!("{}", result.summary);
    print_warnings(&result.warnings);
    println!();
    println!("Use `canon ls --duplicates` to see remaining duplicates.");

    Ok(())
}

// ============================================================================
// Object Exclusion Commands
// ============================================================================

/// Exclude an object by its hash. All sources with this content will be excluded.
/// This is the only way to exclude empty files (size = 0).
pub fn set_object_by_hash(
    db: &mut Db,
    hash: &str,
    options: &SetOptions,
    command_line: &str,
    config: &LedgerConfig,
    no_receipt: bool,
    reason: Option<&str>,
) -> Result<()> {
    let conn = db.conn_mut();

    match check_set_object_by_hash(conn, hash)? {
        ObjectExclusionCheck::AlreadyExcluded { hash_prefix } => {
            println!("Object already excluded: {hash_prefix}...");
        }
        ObjectExclusionCheck::Ready {
            object_id,
            hash_prefix,
            hash,
            sources,
        } => {
            // Object exclusion has no confirmation prompt: the command has no
            // --dry-run flag, so a plain run is always this preview and --yes
            // is what executes. Keep both halves — dropping either one turns a
            // preview into a universe-wide dismissal.
            if options.dry_run {
                println!("Would exclude object: {hash_prefix}...");
                print_source_locations(&sources, options.verbose);
                println!("\nUse --yes to execute.");
            } else {
                let decision = make_decision(
                    conn,
                    DecisionCommand::ExcludeSetObject,
                    &[],
                    command_line,
                    config,
                    no_receipt,
                    reason,
                    options.dry_run,
                )?;
                let destination = resolve_placement(conn, config, &decision)?;
                let result = execute_set_object(
                    conn,
                    object_id,
                    &hash_prefix,
                    &hash,
                    &sources,
                    &destination,
                    Some(&decision),
                )?;
                println!("{}", result.summary);
                print_source_locations(&sources, options.verbose);
                print_warnings(&result.warnings);
            }
        }
    }
    Ok(())
}

/// Exclude an object by file path. Looks up the source, gets its object, and excludes it.
pub fn set_object_by_file(
    db: &mut Db,
    file_path: &Path,
    options: &SetOptions,
    command_line: &str,
    config: &LedgerConfig,
    no_receipt: bool,
    reason: Option<&str>,
) -> Result<()> {
    let conn = db.conn_mut();

    // Resolve path (soft resolution: matches known roots, falls back to fs)
    let roots = crate::core::repo::root::fetch_all(conn)?;
    let cwd = std::env::current_dir()?;
    let path_str = resolve_path(file_path, &roots, &cwd)?;
    // Naming a file to reach its object is still an act aimed at a place, and
    // behind a closed door it is refused by name.
    crate::core::ops::scope::refuse_parked_locations(std::slice::from_ref(&path_str), &roots)?;

    // Find which root contains this path (domain layer)
    let Some((root_id, _root_path, _role, rel_path)) = find_containing_root(&path_str, &roots)
    else {
        anyhow::bail!(
            "No hashed source found for path: {}\n  (File must be scanned and hashed first)",
            file_path.display()
        );
    };

    let display_path = format!("{}", file_path.display());
    match check_set_object_by_file(conn, root_id, &rel_path, &display_path)? {
        ObjectExclusionCheck::AlreadyExcluded { hash_prefix } => {
            println!("Object already excluded: {hash_prefix}...");
        }
        ObjectExclusionCheck::Ready {
            object_id,
            hash_prefix,
            hash,
            sources,
        } => {
            // Object exclusion has no confirmation prompt: the command has no
            // --dry-run flag, so a plain run is always this preview and --yes
            // is what executes. Keep both halves — dropping either one turns a
            // preview into a universe-wide dismissal.
            if options.dry_run {
                println!("Would exclude object: {hash_prefix}...");
                print_source_locations(&sources, options.verbose);
                println!("\nUse --yes to execute.");
            } else {
                let decision = make_decision(
                    conn,
                    DecisionCommand::ExcludeSetObject,
                    &[path_str],
                    command_line,
                    config,
                    no_receipt,
                    reason,
                    options.dry_run,
                )?;
                let destination = resolve_placement(conn, config, &decision)?;
                let result = execute_set_object(
                    conn,
                    object_id,
                    &hash_prefix,
                    &hash,
                    &sources,
                    &destination,
                    Some(&decision),
                )?;
                println!("{}", result.summary);
                print_source_locations(&sources, options.verbose);
                print_warnings(&result.warnings);
            }
        }
    }
    Ok(())
}

/// Exclude objects matching the given scope and filters.
#[allow(clippy::too_many_arguments)]
pub fn set_objects_by_filter(
    db: &mut Db,
    scope_prefixes: &[String],
    filter_strs: &[String],
    options: &SetOptions,
    command_line: &str,
    config: &LedgerConfig,
    no_receipt: bool,
    reason: Option<&str>,
) -> Result<()> {
    let conn = db.conn_mut();

    // Parse filters
    let filters: Vec<Filter> = filter_strs
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    let scopes = classify_all_indexed(conn, scope_prefixes)?;
    let plan = plan_set_objects(conn, &ExcludeSetObjectsParams { scopes, filters })?;

    if plan.objects.is_empty() {
        println!("No objects to exclude.");
        if plan.skipped_no_hash > 0 {
            println!("  {} sources have no hash yet", plan.skipped_no_hash);
        }
        if plan.skipped_empty > 0 {
            println!(
                "  {} empty files skipped (use --hash to exclude explicitly)",
                plan.skipped_empty
            );
        }
        if plan.skipped_already_excluded > 0 {
            println!(
                "  {} objects already excluded",
                plan.skipped_already_excluded
            );
        }
        return Ok(());
    }

    let total_in_source_roots = plan.total_source_count - plan.total_archive_count;

    // Object exclusion has no confirmation prompt: the command has no
    // --dry-run flag, so a plain run is always this preview and --yes is what
    // executes. Keep both halves — dropping either one turns a preview into a
    // universe-wide dismissal.
    if options.dry_run {
        println!(
            "Would exclude {} objects affecting {} sources ({} in source roots, {} in archives):",
            plan.objects.len(),
            plan.total_source_count,
            total_in_source_roots,
            plan.total_archive_count
        );
        for entry in &plan.objects {
            let archive_count = entry.sources.iter().filter(|s| s.is_archive).count();
            let src_count = entry.sources.len() - archive_count;
            println!(
                "  {}... ({} source, {} archive)",
                entry.hash_prefix, src_count, archive_count
            );
            if options.verbose {
                for source in &entry.sources {
                    let marker = if source.is_archive { " (archive)" } else { "" };
                    println!("      {}{}", source.path, marker);
                }
            }
        }
        if plan.skipped_no_hash > 0 {
            println!("\n  {} sources skipped (no hash)", plan.skipped_no_hash);
        }
        if plan.skipped_empty > 0 {
            println!(
                "  {} empty files skipped (use --hash to exclude explicitly)",
                plan.skipped_empty
            );
        }
        if plan.skipped_already_excluded > 0 {
            println!(
                "  {} objects already excluded",
                plan.skipped_already_excluded
            );
        }
        println!("\nUse --yes to execute.");
        return Ok(());
    }

    // Execute
    let decision = make_decision(
        conn,
        DecisionCommand::ExcludeSetObject,
        scope_prefixes,
        command_line,
        config,
        no_receipt,
        reason,
        options.dry_run,
    )?;
    let destination = resolve_placement(conn, config, &decision)?;
    let result = execute_set_objects(conn, &plan, &destination, Some(&decision))?;
    println!("{}", result.summary);
    print_warnings(&result.warnings);
    Ok(())
}

/// Display source locations for an object
fn print_source_locations(sources: &[ObjectSourceInfo], verbose: bool) {
    let archive_count = sources.iter().filter(|s| s.is_archive).count();
    let source_count = sources.len() - archive_count;

    println!("  Sources: {source_count} in source roots, {archive_count} in archive roots");

    // Show paths (limited unless verbose)
    const DEFAULT_LIMIT: usize = 3;
    let show_count = if verbose {
        sources.len()
    } else {
        DEFAULT_LIMIT
    };
    let truncated = sources.len() > show_count && !verbose;

    for source in sources.iter().take(show_count) {
        let marker = if source.is_archive { " (archive)" } else { "" };
        println!("    {}{}", source.path, marker);
    }

    if truncated {
        println!(
            "    ... and {} more (use --verbose to show all)",
            sources.len() - show_count
        );
    }
}

/// Clear exclusion from an object by its hash
pub fn clear_object(
    db: &mut Db,
    hash: &str,
    options: &ClearOptions,
    command_line: &str,
    config: &LedgerConfig,
    no_receipt: bool,
) -> Result<()> {
    let conn = db.conn_mut();

    match check_clear_object(conn, hash)? {
        ObjectClearCheck::NotExcluded { hash_prefix } => {
            println!("Object is not excluded: {hash_prefix}...");
        }
        ObjectClearCheck::Ready {
            object_id,
            hash_prefix,
            hash,
        } => {
            if options.dry_run {
                println!("Would clear exclusion from object: {hash_prefix}...");
            } else {
                let decision = make_decision(
                    conn,
                    DecisionCommand::ExcludeClearObject,
                    &[],
                    command_line,
                    config,
                    no_receipt,
                    None,
                    options.dry_run,
                )?;
                let destination = resolve_placement(conn, config, &decision)?;
                let result = execute_clear_object(
                    conn,
                    object_id,
                    &hash_prefix,
                    &hash,
                    &destination,
                    Some(&decision),
                )?;
                println!("{}", result.summary);
                print_warnings(&result.warnings);
            }
        }
    }
    Ok(())
}

/// List all excluded objects
pub fn list_objects(db: &Db) -> Result<()> {
    let conn = db.conn();

    let entries = list_excluded_objects(conn)?;

    if entries.is_empty() {
        println!("No excluded objects");
        return Ok(());
    }

    println!("Excluded objects ({}):", entries.len());
    for entry in &entries {
        println!(
            "  {}... (id: {}, {} sources)",
            entry.hash_prefix, entry.object_id, entry.source_count
        );
    }

    Ok(())
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::repo::open_in_memory_for_test;
    use rusqlite::Connection as RusqliteConnection;

    fn setup_test_db() -> RusqliteConnection {
        open_in_memory_for_test()
    }

    /// `exclude duplicates` names two load-bearing locations: the scope it
    /// judges and the prefer path holding the copies that survive. Neither
    /// can be set aside — a dismissal decided against a location Canon knows
    /// nothing about would be a dismissal decided against nothing. The
    /// carve-out from the scope boundary's proceed-and-state policy.
    #[test]
    fn exclude_duplicates_still_aborts_on_a_sourceless_prefer_path() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        insert_root(&conn, "/archive", "archive", false);
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, size, mtime, partial_hash,
                                  scanned_at, last_seen_at, device, inode, present)
             VALUES (?, 'a.jpg', 10, 0, '', 0, 0, 1, 1, 1)",
            rusqlite::params![root_id],
        )
        .unwrap();
        let mut db = crate::core::repo::Db::from_connection(conn);

        let result = exclude_duplicates(
            &mut db,
            Path::new("/archive/nothing-here"),
            Some(Path::new("/photos")),
            &[],
            true,
            true,
            "test",
            &LedgerConfig::default(),
            true,
            None,
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("no sources known at /archive/nothing-here"),
            "{err}"
        );
    }

    /// Insert a test root and return its ID
    fn insert_root(conn: &RusqliteConnection, path: &str, role: &str, suspended: bool) -> i64 {
        conn.execute(
            "INSERT INTO roots (path, role, suspended) VALUES (?, ?, ?)",
            rusqlite::params![path, role, suspended as i64],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    /// Insert a test object and return its ID
    fn insert_object(conn: &RusqliteConnection, hash: &str, excluded: bool) -> i64 {
        conn.execute(
            "INSERT INTO objects (hash_type, hash_value, excluded) VALUES ('sha256', ?, ?)",
            rusqlite::params![hash, excluded as i64],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    /// Insert a test source and return its ID
    fn insert_source(
        conn: &RusqliteConnection,
        root_id: i64,
        rel_path: &str,
        object_id: Option<i64>,
        present: bool,
        excluded: bool,
    ) -> i64 {
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, object_id, size, mtime, partial_hash, scanned_at, last_seen_at, device, inode, present, excluded)
             VALUES (?, ?, ?, 1000, 1704067200, '', 0, 0, 0, 0, ?, ?)",
            rusqlite::params![
                root_id,
                rel_path,
                object_id,
                present as i64,
                excluded as i64
            ],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    // =========================================================================
    // exclude_duplicates integration tests
    // =========================================================================

    /// Create a Db wrapper for testing exclude_duplicates
    fn make_test_db() -> Db {
        let conn = setup_test_db();
        crate::core::repo::Db::from_connection(conn)
    }

    /// Check if a source is excluded in the database
    fn is_source_excluded(conn: &RusqliteConnection, source_id: i64) -> bool {
        conn.query_row(
            "SELECT excluded FROM sources WHERE id = ?",
            [source_id],
            |row| row.get::<_, i64>(0),
        )
        .map(|v| v == 1)
        .unwrap_or(false)
    }

    // =========================================================================
    // set_by_id tests
    // =========================================================================

    #[test]
    fn test_set_by_id_excludes_source() {
        let mut db = make_test_db();
        let conn = db.conn();

        let root = insert_root(conn, "/photos", "source", false);
        let source_id = insert_source(conn, root, "photo.jpg", None, true, false);

        let options = SetOptions {
            dry_run: false,
            verbose: false,
            yes: true,
        };

        let result = set_by_id(
            &mut db,
            source_id,
            &options,
            "test",
            &LedgerConfig::default(),
            false,
            None,
        );
        assert!(result.is_ok());

        assert!(
            is_source_excluded(db.conn(), source_id),
            "Source should be excluded after set_by_id"
        );
    }

    // =========================================================================
    // set_by_path tests
    // =========================================================================

    #[test]
    fn test_set_by_path_nonexistent_file_fails() {
        let mut db = make_test_db();

        let options = SetOptions {
            dry_run: false,
            verbose: false,
            yes: true,
        };

        // Path that definitely doesn't exist
        let result = set_by_path(
            &mut db,
            Path::new("/nonexistent/path/to/file.jpg"),
            &options,
            "test",
            &LedgerConfig::default(),
            false,
            None,
        );
        assert!(result.is_err());

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Failed to resolve path"),
            "Error should mention path resolution failure, got: {err_msg}"
        );
    }

    #[test]
    fn test_set_by_path_not_in_db_fails() {
        let mut db = make_test_db();

        let options = SetOptions {
            dry_run: false,
            verbose: false,
            yes: true,
        };

        // Use a path that exists on disk but isn't in the database
        // /tmp should exist on most Unix systems
        let result = set_by_path(
            &mut db,
            Path::new("/tmp"),
            &options,
            "test",
            &LedgerConfig::default(),
            false,
            None,
        );
        assert!(result.is_err());

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("No source found"),
            "Error should mention no source found, got: {err_msg}"
        );
    }

    // =========================================================================
    // set_objects_by_filter tests
    // =========================================================================

    /// Check if an object is excluded in the database
    fn is_object_excluded_in_db(conn: &RusqliteConnection, object_id: i64) -> bool {
        conn.query_row(
            "SELECT excluded FROM objects WHERE id = ?",
            [object_id],
            |row| row.get::<_, i64>(0),
        )
        .map(|v| v == 1)
        .unwrap_or(false)
    }

    #[test]
    fn test_set_objects_by_filter_excludes_objects() {
        let mut db = make_test_db();
        let conn = db.conn_mut();

        let root = insert_root(conn, "/photos", "source", false);
        let obj = insert_object(conn, "abc123hash", false);
        insert_source(conn, root, "photo.jpg", Some(obj), true, false);

        let options = SetOptions {
            dry_run: false,
            verbose: false,
            yes: true,
        };

        let result = set_objects_by_filter(
            &mut db,
            &[], // no scope restriction
            &[], // no filters
            &options,
            "test",
            &LedgerConfig::default(),
            false,
            None,
        );

        assert!(result.is_ok());
        assert!(
            is_object_excluded_in_db(db.conn(), obj),
            "Object should be excluded after set_objects_by_filter"
        );
    }

    #[test]
    fn test_set_objects_by_filter_dry_run() {
        let mut db = make_test_db();
        let conn = db.conn_mut();

        let root = insert_root(conn, "/photos", "source", false);
        let obj = insert_object(conn, "dry_run_hash", false);
        insert_source(conn, root, "photo.jpg", Some(obj), true, false);

        let options = SetOptions {
            dry_run: true, // DRY RUN
            verbose: false,
            yes: true,
        };

        let result = set_objects_by_filter(
            &mut db,
            &[],
            &[],
            &options,
            "test",
            &LedgerConfig::default(),
            false,
            None,
        );

        assert!(result.is_ok());
        // Object should NOT be excluded (dry run)
        assert!(
            !is_object_excluded_in_db(db.conn(), obj),
            "Dry run should NOT actually exclude objects"
        );
    }

    #[test]
    fn test_set_single_source_no_confirmation() {
        // When count = 1, set() should execute directly without confirmation.
        // We verify by running set() with yes=false — if it tried to confirm,
        // it would block on stdin. Since the test doesn't hang, confirmation was skipped.
        let mut db = make_test_db();
        let conn = db.conn_mut();

        let root = insert_root(conn, "/source", "source", false);
        let source_id = insert_source(conn, root, "only_file.jpg", None, true, false);

        let options = SetOptions {
            dry_run: false,
            verbose: false,
            yes: false, // Would block on stdin if confirmation triggered
        };

        // Use empty scopes so we don't try to canonicalize non-existent paths
        let result = set(
            &mut db,
            &[],
            &[],
            &options,
            "test",
            &LedgerConfig::default(),
            false,
            None,
        );
        assert!(result.is_ok());

        // Verify the source was excluded
        assert!(is_source_excluded(db.conn(), source_id));
    }

    // =========================================================================
    // exclude_duplicates single-source tests
    // =========================================================================

    #[test]
    fn test_duplicates_single_source_no_confirmation() {
        // Count = 1: execution directly, no confirmation prompt.
        // Verify by running with yes=false — if it tried to confirm,
        // it would block on stdin.
        let mut db = make_test_db();
        let conn = db.conn_mut();

        let source_root = insert_root(conn, "/source", "source", false);
        let archive_root = insert_root(conn, "/archive", "archive", false);

        let obj = insert_object(conn, "single_dup_hash", false);
        let source_id = insert_source(conn, source_root, "photo.jpg", Some(obj), true, false);
        insert_source(conn, archive_root, "photo.jpg", Some(obj), true, false);

        // yes=false — would block if confirmation triggered for count=1
        let result = exclude_duplicates(
            &mut db,
            Path::new("/archive"),
            Some(Path::new("/source")),
            &[],
            false,
            false, // yes=false, but count=1 so no prompt
            "test",
            &LedgerConfig::default(),
            false,
            None,
        );

        assert!(result.is_ok());
        assert!(
            is_source_excluded(db.conn(), source_id),
            "Single source should be excluded without confirmation"
        );
    }
}
