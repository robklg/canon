use anyhow::Result;
use std::path::Path;

use crate::ceremony;
use crate::domain::config::{LedgerConfig, RecordingMode};
use crate::domain::decision::DecisionCommand;
use crate::domain::path::{resolve_path, validate_paths_in_roots};
use crate::domain::root::find_containing_root;
use crate::domain::scope::ScopeMatch;
use crate::expr::filter::Filter;
use crate::ops::decision::DecisionParams;
use crate::ops::exclude::{
    self, check_clear_object, check_set_object_by_file, check_set_object_by_hash,
    check_set_source_by_id, check_set_source_by_path, execute_clear, execute_clear_object,
    execute_duplicates, execute_set, execute_set_object, execute_set_objects, execute_set_source,
    plan_clear, plan_duplicates, plan_set, plan_set_objects, ExcludeClearParams,
    ExcludeDuplicatesParams, ExcludeSetObjectsParams, ExcludeSetParams, ObjectClearCheck,
    ObjectExclusionCheck, ObjectSourceInfo, SourceExclusionCheck,
};
use crate::repo::{self, Db};

fn make_decision(
    command: DecisionCommand,
    scope: Option<Vec<String>>,
    command_line: &str,
    config: &LedgerConfig,
    no_receipt: bool,
    reason: Option<&str>,
    dry_run: bool,
) -> DecisionParams {
    DecisionParams {
        command,
        scope,
        command_line: command_line.to_string(),
        reason: reason
            .map(|r| r.to_string())
            .filter(|r| !r.trim().is_empty()),
        record_enabled: config.recording != RecordingMode::Off && !dry_run,
        receipt_enabled: config.recording == RecordingMode::Full && !no_receipt && !dry_run,
        ledger_config: config.clone(),
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

    let scopes = ScopeMatch::classify_all(scope_prefixes);
    let plan = plan_set(conn, &ExcludeSetParams { scopes, filters })?;

    if plan.source_ids.is_empty() {
        println!("No sources to exclude (0 matching non-excluded sources)");
        return Ok(());
    }

    if options.dry_run {
        println!("Would exclude {} sources:", plan.source_ids.len());
        for path in &plan.paths {
            println!("  {path}");
        }
        return Ok(());
    }

    // Confirmation when affecting > 1 source
    if plan.source_ids.len() > 1 {
        if !options.yes {
            eprintln!("Will exclude {} sources", plan.source_ids.len());
            eprintln!("  Across {} roots", plan.root_count);
            eprintln!("  {} have no archived copy", plan.not_archived_count);
        }

        if !ceremony::confirm(options.yes)? {
            return Ok(());
        }
    }

    let decision = make_decision(
        DecisionCommand::ExcludeSet,
        Some(scope_prefixes.to_vec()),
        command_line,
        config,
        no_receipt,
        reason,
        options.dry_run,
    );
    let result = execute_set(conn, &plan, Some(&decision))?;
    println!("{}", result.summary);
    Ok(())
}

// ============================================================================
// Clear Command
// ============================================================================

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

    let scopes = ScopeMatch::classify_all(scope_prefixes);
    let plan = plan_clear(conn, &ExcludeClearParams { scopes, filters })?;

    if plan.source_ids.is_empty() {
        println!("No excluded sources match the given filters");
        return Ok(());
    }

    if options.dry_run {
        println!(
            "Would clear exclusions for {} sources:",
            plan.source_ids.len()
        );
        for path in &plan.paths {
            println!("  {path}");
        }
        return Ok(());
    }

    // Confirmation when affecting > 1 source
    if plan.source_ids.len() > 1 {
        if !options.yes {
            eprintln!(
                "Will clear exclusions for {} sources",
                plan.source_ids.len()
            );
            eprintln!("  Across {} roots", plan.root_count);
        }

        if !ceremony::confirm(options.yes)? {
            return Ok(());
        }
    }

    let decision = make_decision(
        DecisionCommand::ExcludeClear,
        Some(scope_prefixes.to_vec()),
        command_line,
        config,
        no_receipt,
        reason,
        options.dry_run,
    );
    let result = execute_clear(conn, &plan, Some(&decision))?;
    println!("{}", result.summary);
    Ok(())
}

/// Exclude a specific source by ID
pub fn set_by_id(
    db: &Db,
    source_id: i64,
    options: &SetOptions,
    command_line: &str,
    config: &LedgerConfig,
    no_receipt: bool,
    reason: Option<&str>,
) -> Result<()> {
    let conn = db.conn();

    match check_set_source_by_id(conn, source_id)? {
        SourceExclusionCheck::AlreadyExcluded { path } => {
            println!("Source already excluded: {path}");
        }
        SourceExclusionCheck::Ready { source_id, path } => {
            if options.dry_run {
                println!("Would exclude source (id: {source_id}):");
                println!("  {path}");
            } else {
                let decision = make_decision(
                    DecisionCommand::ExcludeSet,
                    Some(vec![path.clone()]),
                    command_line,
                    config,
                    no_receipt,
                    reason,
                    options.dry_run,
                );
                let result = execute_set_source(conn, source_id, &path, Some(&decision))?;
                println!("{}", result.summary);
            }
        }
    }
    Ok(())
}

/// Exclude a specific source by exact file path
pub fn set_by_path(
    db: &Db,
    file_path: &Path,
    options: &SetOptions,
    command_line: &str,
    config: &LedgerConfig,
    no_receipt: bool,
    reason: Option<&str>,
) -> Result<()> {
    let conn = db.conn();

    // Resolve path (soft resolution: matches known roots, falls back to fs)
    let roots = repo::root::fetch_all(conn)?;
    let cwd = std::env::current_dir()?;
    let path_str = resolve_path(file_path, &roots, &cwd)?;

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
        SourceExclusionCheck::Ready { source_id, path } => {
            if options.dry_run {
                println!("Would exclude:");
                println!("  {path}");
            } else {
                let decision = make_decision(
                    DecisionCommand::ExcludeSet,
                    Some(vec![path.clone()]),
                    command_line,
                    config,
                    no_receipt,
                    reason,
                    options.dry_run,
                );
                let result = execute_set_source(conn, source_id, &path, Some(&decision))?;
                println!("{}", result.summary);
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
/// - prefer = where the "keeper" copies should be
///
/// For each source in scope, we check if there's a duplicate in the prefer path.
/// If exactly one duplicate exists in prefer, we exclude the scoped source.
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
    let all_roots = repo::root::fetch_all(conn)?;
    let cwd = std::env::current_dir()?;
    let scope_prefixes: Vec<String> = if let Some(p) = scope_path {
        vec![resolve_path(p, &all_roots, &cwd)?]
    } else {
        vec![]
    };
    validate_paths_in_roots(&scope_prefixes, &all_roots)?;
    crate::ops::scope::validate_sources_exist(conn, &scope_prefixes, &all_roots)?;
    let prefer_prefix = resolve_path(prefer_path, &all_roots, &cwd)?;
    validate_paths_in_roots(&[prefer_prefix.clone()], &all_roots)?;
    crate::ops::scope::validate_sources_exist(conn, &[prefer_prefix.clone()], &all_roots)?;

    // Plan
    let scopes = ScopeMatch::classify_all(&scope_prefixes);
    let params = ExcludeDuplicatesParams {
        scopes,
        filters,
        prefer_prefix,
    };
    let plan = plan_duplicates(conn, &params)?;

    if plan.source_ids.is_empty() {
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
        eprintln!("  Will exclude: {}", plan.source_ids.len());
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
        println!("Would exclude {} sources:", plan.source_ids.len());
        for path in &plan.paths {
            println!("  {path}");
        }
        return Ok(());
    }

    // Interactive confirmation for > 1 source
    if plan.source_ids.len() > 1 {
        if !yes {
            eprintln!(
                "Will exclude {} sources ({} duplicate groups)",
                plan.source_ids.len(),
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
        DecisionCommand::ExcludeDuplicates,
        Some(scope_prefixes.clone()),
        command_line,
        config,
        no_receipt,
        reason,
        dry_run,
    );
    let result = execute_duplicates(conn, &plan, Some(&decision))?;
    println!("{}", result.summary);
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
    db: &Db,
    hash: &str,
    options: &SetOptions,
    command_line: &str,
    config: &LedgerConfig,
    no_receipt: bool,
    reason: Option<&str>,
) -> Result<()> {
    let conn = db.conn();

    match check_set_object_by_hash(conn, hash)? {
        ObjectExclusionCheck::AlreadyExcluded { hash_prefix } => {
            println!("Object already excluded: {hash_prefix}...");
        }
        ObjectExclusionCheck::Ready {
            object_id,
            hash_prefix,
            sources,
        } => {
            if options.dry_run {
                println!("Would exclude object: {hash_prefix}...");
                print_source_locations(&sources, options.verbose);
                println!("\nUse --yes to execute.");
            } else {
                let decision = make_decision(
                    DecisionCommand::ExcludeSetObject,
                    None,
                    command_line,
                    config,
                    no_receipt,
                    reason,
                    options.dry_run,
                );
                let result =
                    execute_set_object(conn, object_id, &hash_prefix, &sources, Some(&decision))?;
                println!("{}", result.summary);
                print_source_locations(&sources, options.verbose);
            }
        }
    }
    Ok(())
}

/// Exclude an object by file path. Looks up the source, gets its object, and excludes it.
pub fn set_object_by_file(
    db: &Db,
    file_path: &Path,
    options: &SetOptions,
    command_line: &str,
    config: &LedgerConfig,
    no_receipt: bool,
    reason: Option<&str>,
) -> Result<()> {
    let conn = db.conn();

    // Resolve path (soft resolution: matches known roots, falls back to fs)
    let roots = repo::root::fetch_all(conn)?;
    let cwd = std::env::current_dir()?;
    let path_str = resolve_path(file_path, &roots, &cwd)?;

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
            sources,
        } => {
            if options.dry_run {
                println!("Would exclude object: {hash_prefix}...");
                print_source_locations(&sources, options.verbose);
                println!("\nUse --yes to execute.");
            } else {
                let decision = make_decision(
                    DecisionCommand::ExcludeSetObject,
                    Some(vec![path_str]),
                    command_line,
                    config,
                    no_receipt,
                    reason,
                    options.dry_run,
                );
                let result =
                    execute_set_object(conn, object_id, &hash_prefix, &sources, Some(&decision))?;
                println!("{}", result.summary);
                print_source_locations(&sources, options.verbose);
            }
        }
    }
    Ok(())
}

/// Exclude objects matching the given scope and filters.
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

    let scopes = ScopeMatch::classify_all(scope_prefixes);
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
        DecisionCommand::ExcludeSetObject,
        Some(scope_prefixes.to_vec()),
        command_line,
        config,
        no_receipt,
        reason,
        options.dry_run,
    );
    let result = execute_set_objects(conn, &plan, Some(&decision))?;
    println!("{}", result.summary);
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
    db: &Db,
    hash: &str,
    options: &ClearOptions,
    command_line: &str,
    config: &LedgerConfig,
    no_receipt: bool,
) -> Result<()> {
    let conn = db.conn();

    match check_clear_object(conn, hash)? {
        ObjectClearCheck::NotExcluded { hash_prefix } => {
            println!("Object is not excluded: {hash_prefix}...");
        }
        ObjectClearCheck::Ready {
            object_id,
            hash_prefix,
        } => {
            if options.dry_run {
                println!("Would clear exclusion from object: {hash_prefix}...");
            } else {
                let decision = make_decision(
                    DecisionCommand::ExcludeClearObject,
                    None,
                    command_line,
                    config,
                    no_receipt,
                    None,
                    options.dry_run,
                );
                let result = execute_clear_object(conn, object_id, &hash_prefix, Some(&decision))?;
                println!("{}", result.summary);
            }
        }
    }
    Ok(())
}

/// List all excluded objects
pub fn list_objects(db: &Db) -> Result<()> {
    let conn = db.conn();

    let entries = exclude::list_excluded_objects(conn)?;

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
    use crate::repo::open_in_memory_for_test;
    use rusqlite::Connection as RusqliteConnection;

    fn setup_test_db() -> RusqliteConnection {
        open_in_memory_for_test()
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
    fn make_test_db() -> repo::Db {
        let conn = setup_test_db();
        repo::Db::from_connection(conn)
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
    // set_by_id tests (Phase 1: path pattern completion)
    // =========================================================================

    #[test]
    fn test_set_by_id_excludes_source() {
        let db = make_test_db();
        let conn = db.conn();

        let root = insert_root(conn, "/photos", "source", false);
        let source_id = insert_source(conn, root, "photo.jpg", None, true, false);

        let options = SetOptions {
            dry_run: false,
            verbose: false,
            yes: true,
        };

        let result = set_by_id(
            &db,
            source_id,
            &options,
            "test",
            &LedgerConfig::default(),
            false,
            None,
        );
        assert!(result.is_ok());

        assert!(
            is_source_excluded(conn, source_id),
            "Source should be excluded after set_by_id"
        );
    }

    // =========================================================================
    // set_by_path tests
    // =========================================================================

    #[test]
    fn test_set_by_path_nonexistent_file_fails() {
        let db = make_test_db();

        let options = SetOptions {
            dry_run: false,
            verbose: false,
            yes: true,
        };

        // Path that definitely doesn't exist
        let result = set_by_path(
            &db,
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
        let db = make_test_db();

        let options = SetOptions {
            dry_run: false,
            verbose: false,
            yes: true,
        };

        // Use a path that exists on disk but isn't in the database
        // /tmp should exist on most Unix systems
        let result = set_by_path(
            &db,
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
    // exclude_duplicates integration tests (Phase 3)
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
