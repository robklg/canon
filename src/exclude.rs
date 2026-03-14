use anyhow::Result;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::ceremony;
use crate::domain::include::IncludeSet;
use crate::domain::path::{resolve_path, resolve_paths};
use crate::domain::root::find_containing_root;
use crate::domain::scope::ScopeMatch;
use crate::expr::filter::Filter;
use crate::ops::exclude::{
    execute_clear, execute_duplicates, execute_set, plan_clear, plan_duplicates, plan_set,
    ExcludeClearParams, ExcludeDuplicatesParams, ExcludeSetParams,
};
use crate::ops::selection::{self, RolePolicy, SelectionParams};
use crate::repo::{self, Connection, Db};

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
    scope_paths: &[PathBuf],
    filter_strs: &[String],
    options: &SetOptions,
) -> Result<()> {
    let conn = db.conn_mut();

    // Parse filters
    let filters: Vec<Filter> = filter_strs
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    // Resolve scope paths (soft resolution: matches known roots, falls back to fs)
    let all_roots = repo::root::fetch_all(conn)?;
    let scope_prefixes = resolve_paths(scope_paths, &all_roots)?;

    let scopes = ScopeMatch::classify_all(&scope_prefixes);
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

    execute_set(conn, &plan)?;

    let noun = if plan.source_ids.len() == 1 {
        "source"
    } else {
        "sources"
    };
    println!(
        "Excluded {} {noun}",
        ceremony::format_count(plan.source_ids.len())
    );
    Ok(())
}

// ============================================================================
// Clear Command
// ============================================================================

pub fn clear(
    db: &mut Db,
    scope_paths: &[PathBuf],
    filter_strs: &[String],
    options: &ClearOptions,
) -> Result<()> {
    let conn = db.conn_mut();

    // Parse filters
    let filters: Vec<Filter> = filter_strs
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    // Resolve scope paths (soft resolution: matches known roots, falls back to fs)
    let all_roots = repo::root::fetch_all(conn)?;
    let scope_prefixes = resolve_paths(scope_paths, &all_roots)?;

    let scopes = ScopeMatch::classify_all(&scope_prefixes);
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

    execute_clear(conn, &plan)?;

    let noun = if plan.source_ids.len() == 1 {
        "source"
    } else {
        "sources"
    };
    println!(
        "Cleared exclusions for {} {noun}",
        ceremony::format_count(plan.source_ids.len())
    );
    Ok(())
}

/// Exclude a specific source by ID
pub fn set_by_id(db: &Db, source_id: i64, options: &SetOptions) -> Result<()> {
    let conn = db.conn();

    // Fetch source using repo layer
    let sources = repo::source::batch_fetch_by_ids(conn, &[source_id])?;
    let Some(source) = sources.get(&source_id) else {
        anyhow::bail!("Source with id {source_id} not found or not present");
    };

    // Use Source::path() for display
    let path = source.path();

    // Check if already excluded using domain predicate
    if source.is_excluded() {
        println!("Source already excluded: {path}");
        return Ok(());
    }

    if options.dry_run {
        println!("Would exclude source (id: {source_id}):");
        println!("  {path}");
        return Ok(());
    }

    repo::source::set_excluded(conn, source_id, true)?;

    println!("Excluded source (id: {source_id}): {path}");
    Ok(())
}

/// Exclude a specific source by exact file path
pub fn set_by_path(db: &Db, file_path: &Path, options: &SetOptions) -> Result<()> {
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

    // Fetch the source using repo layer
    let Some(source) = repo::source::fetch_by_path(conn, root_id, &rel_path)? else {
        anyhow::bail!("No source found for path: {}", file_path.display());
    };

    // Use Source::path() for display (consistent path formatting)
    let display_path = source.path();

    // Check if already excluded using domain predicate
    if source.is_excluded() {
        println!("Source already excluded: {display_path}");
        return Ok(());
    }

    if options.dry_run {
        println!("Would exclude:");
        println!("  {display_path}");
        return Ok(());
    }

    repo::source::set_excluded(conn, source.id, true)?;

    println!("Excluded: {display_path}");
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
    let prefer_prefix = resolve_path(prefer_path, &all_roots, &cwd)?;

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
    let excluded_count = execute_duplicates(conn, &plan)?;

    let noun = if excluded_count == 1 {
        "source"
    } else {
        "sources"
    };
    println!("Excluded {} {noun}", ceremony::format_count(excluded_count));
    println!();
    println!("Use `canon ls --duplicates` to see remaining duplicates.");

    Ok(())
}

// ============================================================================
// Object Exclusion Commands
// ============================================================================

/// Exclude an object by its hash. All sources with this content will be excluded.
/// This is the only way to exclude empty files (size = 0).
pub fn set_object_by_hash(db: &Db, hash: &str, options: &SetOptions) -> Result<()> {
    let conn = db.conn();

    // Find the object by hash
    let Some(object) = repo::object::fetch_by_hash(conn, hash)? else {
        anyhow::bail!("No object found with hash: {hash}");
    };

    exclude_object_by_id(conn, object.id, &object.hash_value, options)
}

/// Exclude an object by file path. Looks up the source, gets its object, and excludes it.
pub fn set_object_by_file(db: &Db, file_path: &Path, options: &SetOptions) -> Result<()> {
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

    // Fetch the source using repo layer
    let Some(source) = repo::source::fetch_by_path(conn, root_id, &rel_path)? else {
        anyhow::bail!(
            "No hashed source found for path: {}\n  (File must be scanned and hashed first)",
            file_path.display()
        );
    };

    // Verify source has an object (is hashed)
    let Some(object_id) = source.object_id else {
        anyhow::bail!(
            "No hashed source found for path: {}\n  (File must be scanned and hashed first)",
            file_path.display()
        );
    };

    // Get the object to access hash_value
    let objects = repo::object::batch_fetch_by_ids(conn, &[object_id])?;
    let Some(object) = objects.get(&object_id) else {
        anyhow::bail!(
            "No hashed source found for path: {}\n  (File must be scanned and hashed first)",
            file_path.display()
        );
    };

    // Safety check: refuse to exclude empty files via path lookup
    if source.size == 0 {
        anyhow::bail!(
            "Cannot exclude empty file via path (all empty files share the same hash).\n  \
             Use --hash {} to explicitly exclude all empty files.",
            object.hash_value
        );
    }

    exclude_object_by_id(conn, object_id, &object.hash_value, options)
}

/// Exclude objects matching the given scope and filters.
pub fn set_objects_by_filter(
    db: &mut Db,
    scope_paths: &[PathBuf],
    filter_strs: &[String],
    options: &SetOptions,
) -> Result<()> {
    let conn = db.conn_mut();

    // Parse filters
    let filters: Vec<Filter> = filter_strs
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    // Resolve scope paths (soft resolution: matches known roots, falls back to fs)
    let all_roots = repo::root::fetch_all(conn)?;
    let scope_prefixes = resolve_paths(scope_paths, &all_roots)?;

    // Get matching sources (include already-excluded to find their objects)
    let scopes = ScopeMatch::classify_all(&scope_prefixes);
    let params = SelectionParams {
        scopes,
        include: IncludeSet {
            excluded: true,
            archived: false,
        },
        filters,
        role_policy: RolePolicy::SourceOnly,
    };
    let source_ids = selection::select_sources(conn, &params)?.source_ids();

    if source_ids.is_empty() {
        println!("No sources match the given filters.");
        return Ok(());
    }

    // Batch fetch all sources
    let sources_map = repo::source::batch_fetch_by_ids(conn, &source_ids)?;

    // Collect unique object_ids and track stats
    let mut object_ids_to_check: Vec<i64> = Vec::new();
    let mut seen_objects: HashSet<i64> = HashSet::new();
    let mut no_hash = 0;
    let mut empty_skipped = 0;

    for source_id in &source_ids {
        let Some(source) = sources_map.get(source_id) else {
            continue;
        };

        let Some(object_id) = source.object_id else {
            no_hash += 1;
            continue;
        };

        if seen_objects.contains(&object_id) {
            continue;
        }
        seen_objects.insert(object_id);

        // Skip empty files
        if source.size == 0 {
            empty_skipped += 1;
            continue;
        }

        object_ids_to_check.push(object_id);
    }

    if object_ids_to_check.is_empty() {
        println!("No objects to exclude.");
        if no_hash > 0 {
            println!("  {no_hash} sources have no hash yet");
        }
        if empty_skipped > 0 {
            println!("  {empty_skipped} empty files skipped (use --hash to exclude explicitly)");
        }
        return Ok(());
    }

    // Batch fetch objects to check exclusion status and get hash values
    let objects_map = repo::object::batch_fetch_by_ids(conn, &object_ids_to_check)?;

    // Batch fetch sources per object for counting
    let sources_by_object = repo::source::fetch_sources_by_object_ids(conn, &object_ids_to_check)?;

    // Filter to non-excluded objects and build final list
    let mut objects_to_exclude: Vec<(i64, String, i64)> = Vec::new(); // (object_id, hash, source_count)
    let mut already_excluded = 0;

    for object_id in &object_ids_to_check {
        let Some(object) = objects_map.get(object_id) else {
            continue;
        };

        // Skip already excluded (using domain predicate)
        if object.is_excluded() {
            already_excluded += 1;
            continue;
        }

        // Count present sources for this object (from batch-fetched data)
        let source_count = sources_by_object
            .get(object_id)
            .map(|sources| sources.len() as i64)
            .unwrap_or(0);

        objects_to_exclude.push((*object_id, object.hash_value.clone(), source_count));
    }

    if objects_to_exclude.is_empty() {
        println!("No objects to exclude.");
        if no_hash > 0 {
            println!("  {no_hash} sources have no hash yet");
        }
        if empty_skipped > 0 {
            println!("  {empty_skipped} empty files skipped (use --hash to exclude explicitly)");
        }
        if already_excluded > 0 {
            println!("  {already_excluded} objects already excluded");
        }
        return Ok(());
    }

    // Gather source details for each object
    let mut all_sources: Vec<(i64, String, Vec<SourceInfo>)> = Vec::new(); // (object_id, hash, sources)
    let mut total_source_count = 0;
    let mut total_archive_count = 0;

    for (object_id, hash, _) in &objects_to_exclude {
        let sources = get_object_sources(conn, *object_id)?;
        let archive_count = sources.iter().filter(|s| s.is_archive).count();
        total_archive_count += archive_count;
        total_source_count += sources.len();
        all_sources.push((*object_id, hash.clone(), sources));
    }

    let total_in_source_roots = total_source_count - total_archive_count;

    // Summary
    if options.dry_run {
        println!(
            "Would exclude {} objects affecting {} sources ({} in source roots, {} in archives):",
            objects_to_exclude.len(),
            total_source_count,
            total_in_source_roots,
            total_archive_count
        );
        for (_, hash, sources) in &all_sources {
            let archive_count = sources.iter().filter(|s| s.is_archive).count();
            let src_count = sources.len() - archive_count;
            println!(
                "  {}... ({} source, {} archive)",
                &hash[..16.min(hash.len())],
                src_count,
                archive_count
            );
            if options.verbose {
                for source in sources {
                    let marker = if source.is_archive { " (archive)" } else { "" };
                    println!("      {}{}", source.path, marker);
                }
            }
        }
        if no_hash > 0 {
            println!("\n  {no_hash} sources skipped (no hash)");
        }
        if empty_skipped > 0 {
            println!("  {empty_skipped} empty files skipped (use --hash to exclude explicitly)");
        }
        if already_excluded > 0 {
            println!("  {already_excluded} objects already excluded");
        }
        println!("\nUse --yes to execute.");
        return Ok(());
    }

    // Execute exclusions
    for (object_id, _, _) in &all_sources {
        repo::object::set_excluded(conn, *object_id, true)?;
    }

    println!(
        "Excluded {} objects affecting {} sources ({} in source roots, {} in archives)",
        all_sources.len(),
        total_source_count,
        total_in_source_roots,
        total_archive_count
    );
    Ok(())
}

/// Source info for display
struct SourceInfo {
    path: String,
    is_archive: bool,
}

/// Fetch source details for an object
fn get_object_sources(conn: &Connection, object_id: i64) -> Result<Vec<SourceInfo>> {
    let sources_map = repo::source::fetch_sources_by_object_ids(conn, &[object_id])?;
    let mut sources: Vec<_> = sources_map.get(&object_id).cloned().unwrap_or_default();

    // Sort: same as previous SQL ORDER BY r.role DESC, r.path, s.rel_path
    // Note: role DESC puts 'source' before 'archive' (s > a alphabetically)
    sources.sort_by(|a, b| {
        b.root_role
            .cmp(&a.root_role) // DESC
            .then_with(|| a.root_path.cmp(&b.root_path))
            .then_with(|| a.rel_path.cmp(&b.rel_path))
    });

    Ok(sources
        .into_iter()
        .map(|s| SourceInfo {
            path: s.path(),
            is_archive: s.is_from_role("archive"),
        })
        .collect())
}

/// Display source locations for an object
fn print_source_locations(sources: &[SourceInfo], verbose: bool) {
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

/// Internal helper to exclude an object by its ID
fn exclude_object_by_id(
    conn: &Connection,
    object_id: i64,
    hash_value: &str,
    options: &SetOptions,
) -> Result<()> {
    // Check if already excluded using domain predicate
    let objects = repo::object::batch_fetch_by_ids(conn, &[object_id])?;
    if let Some(object) = objects.get(&object_id) {
        if object.is_excluded() {
            println!(
                "Object already excluded: {}...",
                &hash_value[..16.min(hash_value.len())]
            );
            return Ok(());
        }
    }

    // Get source details
    let sources = get_object_sources(conn, object_id)?;

    if options.dry_run {
        println!(
            "Would exclude object: {}...",
            &hash_value[..16.min(hash_value.len())]
        );
        print_source_locations(&sources, options.verbose);
        println!("\nUse --yes to execute.");
        return Ok(());
    }

    repo::object::set_excluded(conn, object_id, true)?;

    println!(
        "Excluded object: {}...",
        &hash_value[..16.min(hash_value.len())]
    );
    print_source_locations(&sources, options.verbose);
    Ok(())
}

/// Clear exclusion from an object by its hash
pub fn clear_object(db: &Db, hash: &str, options: &ClearOptions) -> Result<()> {
    let conn = db.conn();

    // Find the object by hash
    let Some(object) = repo::object::fetch_by_hash(conn, hash)? else {
        anyhow::bail!("No object found with hash: {hash}");
    };

    // Check if excluded (use domain predicate)
    if !object.is_excluded() {
        println!(
            "Object is not excluded: {}...",
            &object.hash_value[..16.min(object.hash_value.len())]
        );
        return Ok(());
    }

    if options.dry_run {
        println!(
            "Would clear exclusion from object: {}...",
            &object.hash_value[..16.min(object.hash_value.len())]
        );
        return Ok(());
    }

    repo::object::set_excluded(conn, object.id, false)?;

    println!(
        "Cleared exclusion from object: {}...",
        &object.hash_value[..16.min(object.hash_value.len())]
    );
    Ok(())
}

/// List all excluded objects
pub fn list_objects(db: &Db) -> Result<()> {
    let conn = db.conn();

    // Fetch excluded objects via repo layer
    let excluded = repo::object::fetch_excluded(conn)?;

    if excluded.is_empty() {
        println!("No excluded objects");
        return Ok(());
    }

    // Get source counts for display
    let object_ids: Vec<i64> = excluded.iter().map(|o| o.id).collect();
    let sources_by_object = repo::source::fetch_sources_by_object_ids(conn, &object_ids)?;

    println!("Excluded objects ({}):", excluded.len());
    for object in &excluded {
        let hash_short = &object.hash_value[..16.min(object.hash_value.len())];
        let source_count = sources_by_object
            .get(&object.id)
            .map(|sources| sources.len())
            .unwrap_or(0);
        println!(
            "  {}... (id: {}, {} sources)",
            hash_short, object.id, source_count
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

        let result = set_by_id(&db, source_id, &options);
        assert!(result.is_ok());

        assert!(
            is_source_excluded(conn, source_id),
            "Source should be excluded after set_by_id"
        );
    }

    #[test]
    fn test_set_by_id_nonexistent_fails() {
        let db = make_test_db();

        let options = SetOptions {
            dry_run: false,
            verbose: false,
            yes: true,
        };

        let result = set_by_id(&db, 99999, &options);
        assert!(result.is_err());

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("not found"),
            "Error should mention 'not found', got: {err_msg}"
        );
    }

    #[test]
    fn test_set_by_id_already_excluded_skips() {
        let db = make_test_db();
        let conn = db.conn();

        let root = insert_root(conn, "/photos", "source", false);
        // Create source that's already excluded
        let source_id = insert_source(conn, root, "photo.jpg", None, true, true);

        let options = SetOptions {
            dry_run: false,
            verbose: false,
            yes: true,
        };

        // Should succeed (not error) even though already excluded
        let result = set_by_id(&db, source_id, &options);
        assert!(result.is_ok());

        // Should still be excluded
        assert!(
            is_source_excluded(conn, source_id),
            "Source should remain excluded"
        );
    }

    #[test]
    fn test_set_by_id_not_present_fails() {
        let db = make_test_db();
        let conn = db.conn();

        let root = insert_root(conn, "/photos", "source", false);
        // Create source that's not present (present=false)
        let source_id = insert_source(conn, root, "deleted.jpg", None, false, false);

        let options = SetOptions {
            dry_run: false,
            verbose: false,
            yes: true,
        };

        let result = set_by_id(&db, source_id, &options);
        assert!(result.is_err());

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("not found") || err_msg.contains("not present"),
            "Error should mention source not found/present, got: {err_msg}"
        );
    }

    // =========================================================================
    // set_by_path tests (Phase 1: path pattern completion)
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
        let result = set_by_path(&db, Path::new("/nonexistent/path/to/file.jpg"), &options);
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
        let result = set_by_path(&db, Path::new("/tmp"), &options);
        assert!(result.is_err());

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("No source found"),
            "Error should mention no source found, got: {err_msg}"
        );
    }

    // =========================================================================
    // get_object_sources tests (Phase 1: path pattern completion)
    // =========================================================================

    #[test]
    fn test_get_object_sources_returns_paths() {
        let conn = setup_test_db();

        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "abc123hash", false);
        insert_source(&conn, root, "2024/photo.jpg", Some(obj), true, false);

        let sources = get_object_sources(&conn, obj).unwrap();

        assert_eq!(sources.len(), 1);
        assert_eq!(
            sources[0].path, "/photos/2024/photo.jpg",
            "Path should be correctly constructed from root + rel_path"
        );
    }

    #[test]
    fn test_get_object_sources_includes_role() {
        let conn = setup_test_db();

        let source_root = insert_root(&conn, "/source", "source", false);
        let archive_root = insert_root(&conn, "/archive", "archive", false);
        let obj = insert_object(&conn, "abc123hash", false);

        insert_source(&conn, source_root, "photo.jpg", Some(obj), true, false);
        insert_source(&conn, archive_root, "photo.jpg", Some(obj), true, false);

        let sources = get_object_sources(&conn, obj).unwrap();

        assert_eq!(sources.len(), 2);

        // Archives come first (ORDER BY r.role DESC)
        let archive_sources: Vec<_> = sources.iter().filter(|s| s.is_archive).collect();
        let source_sources: Vec<_> = sources.iter().filter(|s| !s.is_archive).collect();

        assert_eq!(archive_sources.len(), 1, "Should have one archive source");
        assert_eq!(source_sources.len(), 1, "Should have one source source");

        assert_eq!(archive_sources[0].path, "/archive/photo.jpg");
        assert_eq!(source_sources[0].path, "/source/photo.jpg");
    }

    #[test]
    fn test_get_object_sources_empty_rel_path() {
        let conn = setup_test_db();

        // Root IS the file (empty rel_path)
        let root = insert_root(&conn, "/archive/photo.jpg", "archive", false);
        let obj = insert_object(&conn, "abc123hash", false);
        insert_source(&conn, root, "", Some(obj), true, false);

        let sources = get_object_sources(&conn, obj).unwrap();

        assert_eq!(sources.len(), 1);
        // Source::path() correctly handles empty rel_path (no trailing slash)
        // This fixes the R1 inconsistency that existed with inline SQL
        assert_eq!(
            sources[0].path, "/archive/photo.jpg",
            "Empty rel_path should NOT produce trailing slash"
        );
    }

    #[test]
    fn test_get_object_sources_excludes_not_present() {
        let conn = setup_test_db();

        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "abc123hash", false);

        // One present, one not present
        insert_source(&conn, root, "present.jpg", Some(obj), true, false);
        insert_source(&conn, root, "deleted.jpg", Some(obj), false, false);

        let sources = get_object_sources(&conn, obj).unwrap();

        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].path, "/photos/present.jpg");
    }

    // =========================================================================
    // set_objects_by_filter tests (Phase 2: final extraction)
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
        );

        assert!(result.is_ok());
        assert!(
            is_object_excluded_in_db(db.conn(), obj),
            "Object should be excluded after set_objects_by_filter"
        );
    }

    #[test]
    fn test_set_objects_by_filter_skips_empty_files() {
        let mut db = make_test_db();
        let conn = db.conn_mut();

        let root = insert_root(conn, "/photos", "source", false);
        let obj = insert_object(conn, "empty_file_hash", false);
        // Size = 0 (empty file)
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, object_id, size, mtime, partial_hash, scanned_at, last_seen_at, device, inode, present, excluded)
             VALUES (?, ?, ?, 0, 1704067200, '', 0, 0, 0, 0, 1, 0)",
            rusqlite::params![root, "empty.txt", obj],
        )
        .unwrap();

        let options = SetOptions {
            dry_run: false,
            verbose: false,
            yes: true,
        };

        let result = set_objects_by_filter(&mut db, &[], &[], &options);

        assert!(result.is_ok());
        assert!(
            !is_object_excluded_in_db(db.conn(), obj),
            "Empty file objects should NOT be excluded"
        );
    }

    #[test]
    fn test_set_objects_by_filter_skips_already_excluded() {
        let mut db = make_test_db();
        let conn = db.conn_mut();

        let root = insert_root(conn, "/photos", "source", false);
        // Object is already excluded
        let obj = insert_object(conn, "already_excluded_hash", true);
        insert_source(conn, root, "photo.jpg", Some(obj), true, false);

        let options = SetOptions {
            dry_run: false,
            verbose: false,
            yes: true,
        };

        // Should succeed without error (skips already excluded)
        let result = set_objects_by_filter(&mut db, &[], &[], &options);

        assert!(result.is_ok());
        // Should still be excluded (unchanged)
        assert!(is_object_excluded_in_db(db.conn(), obj));
    }

    #[test]
    fn test_set_objects_by_filter_skips_unhashed() {
        let mut db = make_test_db();
        let conn = db.conn_mut();

        let root = insert_root(conn, "/photos", "source", false);
        // Source without object_id (unhashed)
        insert_source(conn, root, "unhashed.jpg", None, true, false);

        let options = SetOptions {
            dry_run: false,
            verbose: false,
            yes: true,
        };

        // Should succeed (just reports nothing to exclude)
        let result = set_objects_by_filter(&mut db, &[], &[], &options);

        assert!(result.is_ok());
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

        let result = set_objects_by_filter(&mut db, &[], &[], &options);

        assert!(result.is_ok());
        // Object should NOT be excluded (dry run)
        assert!(
            !is_object_excluded_in_db(db.conn(), obj),
            "Dry run should NOT actually exclude objects"
        );
    }

    // =========================================================================
    // list_objects tests (Phase 3: final extraction)
    // =========================================================================

    #[test]
    fn test_list_objects_shows_excluded() {
        let db = make_test_db();
        let conn = db.conn();

        let root = insert_root(conn, "/photos", "source", false);

        // Create excluded object with a source
        let obj = insert_object(conn, "excluded_object_hash", true);
        insert_source(conn, root, "photo.jpg", Some(obj), true, false);

        // list_objects prints to stdout, just verify it doesn't error
        let result = list_objects(&db);
        assert!(result.is_ok());
    }

    #[test]
    fn test_list_objects_shows_source_count() {
        let db = make_test_db();
        let conn = db.conn();

        let root = insert_root(conn, "/photos", "source", false);

        // Create excluded object with multiple sources
        let obj = insert_object(conn, "multi_source_hash", true);
        insert_source(conn, root, "photo1.jpg", Some(obj), true, false);
        insert_source(conn, root, "photo2.jpg", Some(obj), true, false);
        insert_source(conn, root, "deleted.jpg", Some(obj), false, false); // not present, shouldn't count

        // Verify the function runs without error
        // (the actual source count of 2 is displayed to stdout)
        let result = list_objects(&db);
        assert!(result.is_ok());
    }

    #[test]
    fn test_list_objects_empty() {
        let db = make_test_db();
        let conn = db.conn();

        // Create non-excluded object
        let root = insert_root(conn, "/photos", "source", false);
        let obj = insert_object(conn, "not_excluded_hash", false);
        insert_source(conn, root, "photo.jpg", Some(obj), true, false);

        // Should handle no excluded objects gracefully
        let result = list_objects(&db);
        assert!(result.is_ok());
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
        let result = set(&mut db, &[], &[], &options);
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
        );

        assert!(result.is_ok());
        assert!(
            is_source_excluded(db.conn(), source_id),
            "Single source should be excluded without confirmation"
        );
    }
}
