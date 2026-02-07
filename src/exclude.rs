use anyhow::{Context, Result};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::domain::exclusion::find_excludable_duplicates;
use crate::domain::path::canonicalize_scopes;
use crate::domain::scope::ScopeMatch;
use crate::expr::filter::{self, Filter};
use crate::repo::{self, Connection, Db};

// ============================================================================
// Options
// ============================================================================

pub struct SetOptions {
    pub dry_run: bool,
    pub verbose: bool,
}

pub struct ClearOptions {
    pub dry_run: bool,
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

    // Resolve scope paths
    let scope_prefixes = canonicalize_scopes(scope_paths)?;

    // Get matching sources (only from source roots, exclude already-excluded)
    let source_ids = get_matching_sources(conn, &scope_prefixes, &filters, false)?;

    // Filter out already excluded sources
    let to_exclude: Vec<i64> = source_ids
        .into_iter()
        .filter(|id| !is_excluded(&conn, *id).unwrap_or(true))
        .collect();

    if to_exclude.is_empty() {
        println!("No sources to exclude (0 matching non-excluded sources)");
        return Ok(());
    }

    if options.dry_run {
        println!("Would exclude {} sources:", to_exclude.len());
        for &id in &to_exclude {
            if let Some(path) = get_source_path(&conn, id)? {
                println!("  {}", path);
            }
        }
        return Ok(());
    }

    // Mark sources as excluded
    for source_id in &to_exclude {
        repo::source::set_excluded(conn, *source_id, true)?;
    }

    println!("Excluded {} sources", to_exclude.len());
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

    // Resolve scope paths
    let scope_prefixes = canonicalize_scopes(scope_paths)?;

    // Get excluded sources matching filters
    let excluded_sources = get_excluded_sources(conn, &scope_prefixes, &filters)?;

    if excluded_sources.is_empty() {
        println!("No excluded sources match the given filters");
        return Ok(());
    }

    if options.dry_run {
        println!("Would clear exclusions for {} sources:", excluded_sources.len());
        for (_, path) in &excluded_sources {
            println!("  {}", path);
        }
        return Ok(());
    }

    // Clear exclusions
    for (source_id, _) in &excluded_sources {
        repo::source::set_excluded(conn, *source_id, false)?;
    }

    println!("Cleared exclusions for {} sources", excluded_sources.len());
    Ok(())
}

// ============================================================================
// List Command
// ============================================================================

pub fn list(
    db: &mut Db,
    scope_paths: &[PathBuf],
    filter_strs: &[String],
) -> Result<()> {
    let conn = db.conn_mut();

    // Parse filters
    let filters: Vec<Filter> = filter_strs
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    // Resolve scope paths
    let scope_prefixes = canonicalize_scopes(scope_paths)?;

    // Get directly excluded sources
    let direct_excluded = get_excluded_sources(conn, &scope_prefixes, &filters)?;

    // Get sources excluded via their object
    let object_excluded = get_object_excluded_sources(conn, &scope_prefixes, &filters)?;

    if direct_excluded.is_empty() && object_excluded.is_empty() {
        println!("No excluded sources match the given filters");
        return Ok(());
    }

    if !direct_excluded.is_empty() {
        println!("Directly excluded ({}):", direct_excluded.len());
        for (id, path) in &direct_excluded {
            println!("  {} (id: {})", path, id);
        }
    }

    if !object_excluded.is_empty() {
        if !direct_excluded.is_empty() {
            println!();
        }
        println!("Excluded via object ({}):", object_excluded.len());
        for (id, path, hash_short) in &object_excluded {
            println!("  {} (id: {}, object: {}...)", path, id, hash_short);
        }
    }

    Ok(())
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Check if a source is excluded (either directly or via its object)
/// Uses denormalized columns for fast lookup
pub fn is_excluded(conn: &Connection, source_id: i64) -> Result<bool> {
    let excluded: bool = conn
        .query_row(
            "SELECT s.excluded = 1 OR (o.excluded IS NOT NULL AND o.excluded = 1)
             FROM sources s
             LEFT JOIN objects o ON s.object_id = o.id
             WHERE s.id = ?",
            [source_id],
            |row| row.get(0),
        )
        .unwrap_or(false);

    Ok(excluded)
}

/// Check if an object is excluded
/// Uses denormalized column for fast lookup
pub fn is_object_excluded(conn: &Connection, object_id: i64) -> Result<bool> {
    let excluded: bool = conn
        .query_row(
            "SELECT excluded = 1 FROM objects WHERE id = ?",
            [object_id],
            |row| row.get(0),
        )
        .unwrap_or(false);
    Ok(excluded)
}

fn get_matching_sources(
    conn: &mut Connection,
    scope_prefixes: &[String],
    filters: &[Filter],
    include_excluded: bool,
) -> Result<Vec<i64>> {
    // Get all source root IDs (active, source role only)
    let roots = repo::root::fetch_all(conn)?;
    let source_root_ids: Vec<i64> = roots
        .iter()
        .filter(|r| r.is_active() && r.is_source())
        .map(|r| r.id)
        .collect();

    if source_root_ids.is_empty() {
        return Ok(Vec::new());
    }

    // Batch fetch all present sources from source roots
    let sources = repo::source::batch_fetch_by_roots(conn, &source_root_ids)?;

    // Classify scopes for matching
    let scopes = ScopeMatch::classify_all(scope_prefixes);

    // Apply domain predicates
    let filtered: Vec<i64> = sources
        .into_iter()
        .filter(|s| scopes.is_empty() || s.matches_scope(&scopes))
        .filter(|s| include_excluded || !s.is_excluded())
        .map(|s| s.id)
        .collect();

    // Apply --where filters if present
    if filters.is_empty() {
        return Ok(filtered);
    }
    filter::apply_filters(conn, &filtered, filters)
}

fn get_excluded_sources(
    conn: &mut Connection,
    scope_prefixes: &[String],
    filters: &[Filter],
) -> Result<Vec<(i64, String)>> {
    // Get all source root IDs (active, source role only)
    let roots = repo::root::fetch_all(conn)?;
    let source_root_ids: Vec<i64> = roots
        .iter()
        .filter(|r| r.is_active() && r.is_source())
        .map(|r| r.id)
        .collect();

    if source_root_ids.is_empty() {
        return Ok(Vec::new());
    }

    // Batch fetch all present sources from source roots
    let sources = repo::source::batch_fetch_by_roots(conn, &source_root_ids)?;

    // Classify scopes for matching
    let scopes = ScopeMatch::classify_all(scope_prefixes);

    // Filter for DIRECTLY excluded sources only (s.excluded = true)
    // NOT s.is_excluded() which would include object-level exclusions
    let filtered: Vec<(i64, String)> = sources
        .into_iter()
        .filter(|s| scopes.is_empty() || s.matches_scope(&scopes))
        .filter(|s| s.excluded) // Source-level exclusion only
        .map(|s| (s.id, s.path()))
        .collect();

    // Apply --where filters if present
    if filters.is_empty() {
        return Ok(filtered);
    }

    // Apply filters and preserve paths
    let ids: Vec<i64> = filtered.iter().map(|(id, _)| *id).collect();
    let filtered_ids: std::collections::HashSet<i64> =
        filter::apply_filters(conn, &ids, filters)?.into_iter().collect();

    Ok(filtered
        .into_iter()
        .filter(|(id, _)| filtered_ids.contains(id))
        .collect())
}

fn get_source_path(conn: &Connection, source_id: i64) -> Result<Option<String>> {
    let result: Option<String> = conn
        .query_row(
            "SELECT r.path || '/' || s.rel_path
             FROM sources s JOIN roots r ON s.root_id = r.id
             WHERE s.id = ?",
            [source_id],
            |row| row.get(0),
        )
        .ok();
    Ok(result)
}

/// Exclude a specific source by ID
pub fn set_by_id(db: &Db, source_id: i64, options: &SetOptions) -> Result<()> {
    let conn = db.conn();

    // Verify source exists and get its path
    let path: Option<String> = conn
        .query_row(
            "SELECT r.path || '/' || s.rel_path
             FROM sources s
             JOIN roots r ON s.root_id = r.id
             WHERE s.id = ? AND s.present = 1",
            [source_id],
            |row| row.get(0),
        )
        .ok();

    let Some(path) = path else {
        anyhow::bail!("Source with id {} not found or not present", source_id);
    };

    // Check if already excluded
    if is_excluded(conn, source_id)? {
        println!("Source already excluded: {}", path);
        return Ok(());
    }

    if options.dry_run {
        println!("Would exclude source (id: {}):", source_id);
        println!("  {}", path);
        return Ok(());
    }

    repo::source::set_excluded(db.conn(), source_id, true)?;

    println!("Excluded source (id: {}): {}", source_id, path);
    Ok(())
}

/// Exclude a specific source by exact file path
pub fn set_by_path(db: &Db, file_path: &Path, options: &SetOptions) -> Result<()> {
    let conn = db.conn();

    // Canonicalize the path
    let canonical = std::fs::canonicalize(file_path)
        .with_context(|| format!("Failed to resolve path: {}", file_path.display()))?;
    let path_str = canonical
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("Path contains invalid UTF-8"))?;

    // Look up source by exact path match
    let source_id: Option<i64> = conn
        .query_row(
            "SELECT s.id
             FROM sources s
             JOIN roots r ON s.root_id = r.id
             WHERE r.path || '/' || s.rel_path = ? AND s.present = 1",
            [path_str],
            |row| row.get(0),
        )
        .ok();

    let Some(source_id) = source_id else {
        anyhow::bail!("No source found for path: {}", file_path.display());
    };

    // Check if already excluded
    if is_excluded(conn, source_id)? {
        println!("Source already excluded: {}", path_str);
        return Ok(());
    }

    if options.dry_run {
        println!("Would exclude:");
        println!("  {}", path_str);
        return Ok(());
    }

    repo::source::set_excluded(db.conn(), source_id, true)?;

    println!("Excluded: {}", path_str);
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
) -> Result<()> {
    let conn = db.conn_mut();

    // Parse filters
    let filters: Vec<Filter> = filter_strs
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    // Resolve paths (canonicalization happens at command boundary)
    let scope_prefixes: Vec<String> = if let Some(p) = scope_path {
        vec![std::fs::canonicalize(p)
            .map(|cp| cp.to_string_lossy().to_string())
            .unwrap_or_else(|_| p.to_string_lossy().to_string())]
    } else {
        vec![]
    };
    let prefer_prefix = std::fs::canonicalize(prefer_path)
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|_| prefer_path.to_string_lossy().to_string());

    // Get matching source IDs in scope (candidates for exclusion)
    let source_ids = get_matching_sources(conn, &scope_prefixes, &filters, false)?;

    if source_ids.is_empty() {
        println!("No sources match the given filters.");
        return Ok(());
    }

    // Fetch full Source objects for the scope
    let scope_sources_map = repo::source::batch_fetch_by_ids(conn, &source_ids)?;
    let scope_sources: Vec<_> = source_ids
        .iter()
        .filter_map(|id| scope_sources_map.get(id).cloned())
        .collect();

    // Collect object_ids from scope sources (for duplicate lookup)
    let object_ids: Vec<i64> = scope_sources
        .iter()
        .filter_map(|s| s.object_id)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    // Fetch all sources that share these objects (potential duplicates)
    let sources_by_object = repo::source::fetch_sources_by_object_ids(conn, &object_ids)?;

    // Use pure domain function to determine what to exclude
    let result = find_excludable_duplicates(&scope_sources, &sources_by_object, &prefer_prefix);

    // Build path lookup for display
    let to_exclude_with_paths: Vec<(i64, String)> = result
        .to_exclude
        .iter()
        .filter_map(|id| {
            scope_sources_map.get(id).map(|s| (*id, s.path()))
        })
        .collect();

    // Summary header
    println!(
        "Sources in scope: {} ({} unhashed skipped)",
        source_ids.len(),
        result.skipped_no_hash
    );
    println!("  Will exclude: {}", to_exclude_with_paths.len());
    println!("  Skipped (no copy in --prefer): {}", result.skipped_not_covered);
    println!("  Skipped (multiple copies in --prefer): {}", result.skipped_multiple);
    if result.skipped_in_prefer > 0 {
        println!("  Skipped (already in --prefer): {}", result.skipped_in_prefer);
    }
    println!();

    if to_exclude_with_paths.is_empty() {
        println!("Nothing to exclude.");
        return Ok(());
    }

    if dry_run {
        println!("Would exclude {} sources:", to_exclude_with_paths.len());
        for (_, path) in &to_exclude_with_paths {
            println!("  {}", path);
        }
        println!();
        println!("Use `canon ls --duplicates` to see remaining duplicates.");
        return Ok(());
    }

    // Execute exclusions
    let mut excluded_count = 0;

    for (source_id, _) in &to_exclude_with_paths {
        // Skip if already excluded
        if is_excluded(conn, *source_id)? {
            continue;
        }

        repo::source::set_excluded(conn, *source_id, true)?;
        excluded_count += 1;
    }

    println!("Excluded {} sources", excluded_count);
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
        anyhow::bail!("No object found with hash: {}", hash);
    };

    exclude_object_by_id(conn, object.id, &object.hash_value, options)
}

/// Exclude an object by file path. Looks up the source, gets its object, and excludes it.
pub fn set_object_by_file(db: &Db, file_path: &Path, options: &SetOptions) -> Result<()> {
    let conn = db.conn();

    // Canonicalize the path
    let canonical = std::fs::canonicalize(file_path)
        .with_context(|| format!("Failed to resolve path: {}", file_path.display()))?;
    let path_str = canonical
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("Path contains invalid UTF-8"))?;

    // Look up source by exact path match
    let source_info: Option<(i64, i64, String, i64)> = conn
        .query_row(
            "SELECT s.object_id, o.id, o.hash_value, s.size
             FROM sources s
             JOIN roots r ON s.root_id = r.id
             JOIN objects o ON s.object_id = o.id
             WHERE r.path || '/' || s.rel_path = ? AND s.present = 1",
            [path_str],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .ok();

    let Some((_object_id_check, object_id, hash_value, size)) = source_info else {
        anyhow::bail!("No hashed source found for path: {}\n  (File must be scanned and hashed first)", file_path.display());
    };

    // Safety check: refuse to exclude empty files via path lookup
    if size == 0 {
        anyhow::bail!(
            "Cannot exclude empty file via path (all empty files share the same hash).\n  \
             Use --hash {} to explicitly exclude all empty files.",
            hash_value
        );
    }

    exclude_object_by_id(conn, object_id, &hash_value, options)
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

    // Resolve scope paths
    let scope_prefixes = canonicalize_scopes(scope_paths)?;

    // Get matching sources (only from source roots, include already-excluded to find their objects)
    let source_ids = get_matching_sources(conn, &scope_prefixes, &filters, true)?;

    if source_ids.is_empty() {
        println!("No sources match the given filters.");
        return Ok(());
    }

    // Get unique objects from these sources, excluding empty files and already-excluded objects
    let mut objects_to_exclude: Vec<(i64, String, i64)> = Vec::new(); // (object_id, hash, source_count)
    let mut empty_skipped = 0;
    let mut already_excluded = 0;
    let mut no_hash = 0;

    // Get unique object IDs
    let mut seen_objects: std::collections::HashSet<i64> = std::collections::HashSet::new();

    for source_id in &source_ids {
        let obj_info: Option<(i64, String, i64)> = conn
            .query_row(
                "SELECT o.id, o.hash_value, s.size
                 FROM sources s
                 JOIN objects o ON s.object_id = o.id
                 WHERE s.id = ?",
                [source_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .ok();

        let Some((object_id, hash_value, size)) = obj_info else {
            no_hash += 1;
            continue;
        };

        if seen_objects.contains(&object_id) {
            continue;
        }
        seen_objects.insert(object_id);

        // Skip empty files
        if size == 0 {
            empty_skipped += 1;
            continue;
        }

        // Skip already excluded
        if is_object_excluded(conn, object_id)? {
            already_excluded += 1;
            continue;
        }

        // Count affected sources for this object
        let source_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM sources WHERE object_id = ? AND present = 1",
            [object_id],
            |row| row.get(0),
        )?;

        objects_to_exclude.push((object_id, hash_value, source_count));
    }

    if objects_to_exclude.is_empty() {
        println!("No objects to exclude.");
        if no_hash > 0 {
            println!("  {} sources have no hash yet", no_hash);
        }
        if empty_skipped > 0 {
            println!("  {} empty files skipped (use --hash to exclude explicitly)", empty_skipped);
        }
        if already_excluded > 0 {
            println!("  {} objects already excluded", already_excluded);
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
        println!("Would exclude {} objects affecting {} sources ({} in source roots, {} in archives):",
            objects_to_exclude.len(), total_source_count, total_in_source_roots, total_archive_count);
        for (_, hash, sources) in &all_sources {
            let archive_count = sources.iter().filter(|s| s.is_archive).count();
            let src_count = sources.len() - archive_count;
            println!("  {}... ({} source, {} archive)", &hash[..16.min(hash.len())], src_count, archive_count);
            if options.verbose {
                for source in sources {
                    let marker = if source.is_archive { " (archive)" } else { "" };
                    println!("      {}{}", source.path, marker);
                }
            }
        }
        if no_hash > 0 {
            println!("\n  {} sources skipped (no hash)", no_hash);
        }
        if empty_skipped > 0 {
            println!("  {} empty files skipped (use --hash to exclude explicitly)", empty_skipped);
        }
        if already_excluded > 0 {
            println!("  {} objects already excluded", already_excluded);
        }
        println!("\nUse --yes to execute.");
        return Ok(());
    }

    // Execute exclusions
    for (object_id, _, _) in &all_sources {
        repo::object::set_excluded(conn, *object_id, true)?;
    }

    println!("Excluded {} objects affecting {} sources ({} in source roots, {} in archives)",
        all_sources.len(), total_source_count, total_in_source_roots, total_archive_count);
    Ok(())
}

/// Source info for display
struct SourceInfo {
    path: String,
    is_archive: bool,
}

/// Fetch source details for an object
fn get_object_sources(conn: &Connection, object_id: i64) -> Result<Vec<SourceInfo>> {
    let sources: Vec<SourceInfo> = conn
        .prepare(
            "SELECT r.path || '/' || s.rel_path, r.role
             FROM sources s
             JOIN roots r ON s.root_id = r.id
             WHERE s.object_id = ? AND s.present = 1
             ORDER BY r.role DESC, r.path, s.rel_path"  // archives first
        )?
        .query_map([object_id], |row| {
            let path: String = row.get(0)?;
            let role: String = row.get(1)?;
            Ok(SourceInfo {
                path,
                is_archive: role == "archive",
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(sources)
}

/// Display source locations for an object
fn print_source_locations(sources: &[SourceInfo], verbose: bool) {
    let archive_count = sources.iter().filter(|s| s.is_archive).count();
    let source_count = sources.len() - archive_count;

    println!("  Sources: {} in source roots, {} in archive roots", source_count, archive_count);

    // Show paths (limited unless verbose)
    const DEFAULT_LIMIT: usize = 3;
    let show_count = if verbose { sources.len() } else { DEFAULT_LIMIT };
    let truncated = sources.len() > show_count && !verbose;

    for source in sources.iter().take(show_count) {
        let marker = if source.is_archive { " (archive)" } else { "" };
        println!("    {}{}", source.path, marker);
    }

    if truncated {
        println!("    ... and {} more (use --verbose to show all)", sources.len() - show_count);
    }
}

/// Internal helper to exclude an object by its ID
fn exclude_object_by_id(conn: &Connection, object_id: i64, hash_value: &str, options: &SetOptions) -> Result<()> {
    // Check if already excluded
    if is_object_excluded(conn, object_id)? {
        println!("Object already excluded: {}...", &hash_value[..16.min(hash_value.len())]);
        return Ok(());
    }

    // Get source details
    let sources = get_object_sources(conn, object_id)?;

    if options.dry_run {
        println!("Would exclude object: {}...", &hash_value[..16.min(hash_value.len())]);
        print_source_locations(&sources, options.verbose);
        println!("\nUse --yes to execute.");
        return Ok(());
    }

    repo::object::set_excluded(conn, object_id, true)?;

    println!("Excluded object: {}...", &hash_value[..16.min(hash_value.len())]);
    print_source_locations(&sources, options.verbose);
    Ok(())
}

/// Clear exclusion from an object by its hash
pub fn clear_object(db: &Db, hash: &str, options: &ClearOptions) -> Result<()> {
    let conn = db.conn();

    // Find the object by hash
    let Some(object) = repo::object::fetch_by_hash(conn, hash)? else {
        anyhow::bail!("No object found with hash: {}", hash);
    };

    // Check if excluded (use domain predicate)
    if !object.is_excluded() {
        println!("Object is not excluded: {}...", &object.hash_value[..16.min(object.hash_value.len())]);
        return Ok(());
    }

    if options.dry_run {
        println!("Would clear exclusion from object: {}...", &object.hash_value[..16.min(object.hash_value.len())]);
        return Ok(());
    }

    repo::object::set_excluded(conn, object.id, false)?;

    println!("Cleared exclusion from object: {}...", &object.hash_value[..16.min(object.hash_value.len())]);
    Ok(())
}

/// List all excluded objects
pub fn list_objects(db: &Db) -> Result<()> {
    let conn = db.conn();

    let excluded: Vec<(i64, String, i64)> = conn
        .prepare(
            "SELECT o.id, o.hash_value, (
                 SELECT COUNT(*) FROM sources s WHERE s.object_id = o.id AND s.present = 1
             ) as source_count
             FROM objects o
             WHERE o.excluded = 1
             ORDER BY o.id"
        )?
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;

    if excluded.is_empty() {
        println!("No excluded objects");
        return Ok(());
    }

    println!("Excluded objects ({}):", excluded.len());
    for (id, hash, source_count) in &excluded {
        let hash_short = &hash[..16.min(hash.len())];
        println!("  {}... (id: {}, {} sources)", hash_short, id, source_count);
    }

    Ok(())
}

/// Get sources excluded via their object (not directly excluded)
fn get_object_excluded_sources(
    conn: &mut Connection,
    scope_prefixes: &[String],
    filters: &[Filter],
) -> Result<Vec<(i64, String, String)>> {
    // Returns (source_id, path, hash_short)

    // Get all source root IDs (active, source role only)
    let roots = repo::root::fetch_all(conn)?;
    let source_root_ids: Vec<i64> = roots
        .iter()
        .filter(|r| r.is_active() && r.is_source())
        .map(|r| r.id)
        .collect();

    if source_root_ids.is_empty() {
        return Ok(Vec::new());
    }

    // Batch fetch all present sources from source roots
    let sources = repo::source::batch_fetch_by_roots(conn, &source_root_ids)?;

    // Classify scopes for matching
    let scopes = ScopeMatch::classify_all(scope_prefixes);

    // First pass: filter sources that are NOT directly excluded and have an object
    let candidates: Vec<_> = sources
        .into_iter()
        .filter(|s| scopes.is_empty() || s.matches_scope(&scopes))
        .filter(|s| !s.excluded) // NOT directly excluded
        .filter(|s| s.object_id.is_some()) // Must have an object
        .collect();

    if candidates.is_empty() {
        return Ok(Vec::new());
    }

    // Fetch objects to check exclusion status and get hash
    let object_ids: Vec<i64> = candidates.iter().filter_map(|s| s.object_id).collect();
    let objects = repo::object::batch_fetch_by_ids(conn, &object_ids)?;

    // Filter for sources where object IS excluded
    let filtered: Vec<(i64, String, String)> = candidates
        .into_iter()
        .filter_map(|s| {
            let object_id = s.object_id?;
            let obj = objects.get(&object_id)?;
            if obj.excluded {
                let hash_short = obj.hash_value[..16.min(obj.hash_value.len())].to_string();
                Some((s.id, s.path(), hash_short))
            } else {
                None
            }
        })
        .collect();

    // Apply --where filters if present
    if filters.is_empty() {
        return Ok(filtered);
    }

    // Apply filters and preserve paths/hashes
    let ids: Vec<i64> = filtered.iter().map(|(id, _, _)| *id).collect();
    let filtered_ids: std::collections::HashSet<i64> =
        filter::apply_filters(conn, &ids, filters)?.into_iter().collect();

    Ok(filtered
        .into_iter()
        .filter(|(id, _, _)| filtered_ids.contains(id))
        .collect())
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection as RusqliteConnection;

    /// Create an in-memory database with the canon schema and test data.
    fn setup_test_db() -> RusqliteConnection {
        let conn = RusqliteConnection::open_in_memory().unwrap();

        // Create minimal schema needed for tests
        conn.execute_batch(
            r#"
            CREATE TABLE roots (
                id INTEGER PRIMARY KEY,
                path TEXT NOT NULL UNIQUE,
                role TEXT NOT NULL DEFAULT 'source',
                comment TEXT,
                last_scanned_at INTEGER,
                suspended INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE objects (
                id INTEGER PRIMARY KEY,
                hash_type TEXT NOT NULL,
                hash_value TEXT NOT NULL,
                excluded INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE sources (
                id INTEGER PRIMARY KEY,
                root_id INTEGER NOT NULL REFERENCES roots(id),
                rel_path TEXT NOT NULL,
                object_id INTEGER REFERENCES objects(id),
                size INTEGER NOT NULL,
                mtime INTEGER NOT NULL,
                device INTEGER NOT NULL DEFAULT 0,
                inode INTEGER NOT NULL DEFAULT 0,
                partial_hash TEXT NOT NULL DEFAULT '',
                basis_rev INTEGER NOT NULL DEFAULT 0,
                present INTEGER NOT NULL DEFAULT 1,
                excluded INTEGER NOT NULL DEFAULT 0
            );

            -- Needed for filter::apply_filters (facts table)
            CREATE TABLE facts (
                id INTEGER PRIMARY KEY,
                entity_type TEXT NOT NULL,
                entity_id INTEGER NOT NULL,
                key TEXT NOT NULL,
                value_type TEXT NOT NULL,
                value_text TEXT,
                value_num REAL,
                value_time INTEGER
            );
            "#,
        )
        .unwrap();

        conn
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
            "INSERT INTO sources (root_id, rel_path, object_id, size, mtime, present, excluded)
             VALUES (?, ?, ?, 1000, 1704067200, ?, ?)",
            rusqlite::params![root_id, rel_path, object_id, present as i64, excluded as i64],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    // =========================================================================
    // get_matching_sources tests
    // =========================================================================

    #[test]
    fn test_get_matching_sources_excludes_suspended_roots() {
        let mut conn = setup_test_db();

        // Active source root
        let active_root = insert_root(&conn, "/active", "source", false);
        let active_id = insert_source(&conn, active_root, "file.txt", None, true, false);

        // Suspended source root
        let suspended_root = insert_root(&conn, "/suspended", "source", true);
        let _suspended_id = insert_source(&conn, suspended_root, "file.txt", None, true, false);

        let result = get_matching_sources(&mut conn, &[], &[], false).unwrap();

        assert_eq!(result.len(), 1);
        assert!(result.contains(&active_id));
    }

    #[test]
    fn test_get_matching_sources_excludes_archive_roots() {
        let mut conn = setup_test_db();

        // Source root
        let source_root = insert_root(&conn, "/source", "source", false);
        let source_id = insert_source(&conn, source_root, "file.txt", None, true, false);

        // Archive root
        let archive_root = insert_root(&conn, "/archive", "archive", false);
        let _archive_id = insert_source(&conn, archive_root, "file.txt", None, true, false);

        let result = get_matching_sources(&mut conn, &[], &[], false).unwrap();

        assert_eq!(result.len(), 1);
        assert!(result.contains(&source_id));
    }

    #[test]
    fn test_get_matching_sources_respects_scope() {
        let mut conn = setup_test_db();

        let root = insert_root(&conn, "/photos", "source", false);
        let in_scope_id = insert_source(&conn, root, "2024/photo.jpg", None, true, false);
        let _out_of_scope_id = insert_source(&conn, root, "2023/photo.jpg", None, true, false);

        // Scope to /photos/2024
        let scopes = vec!["/photos/2024".to_string()];
        let result = get_matching_sources(&mut conn, &scopes, &[], false).unwrap();

        assert_eq!(result.len(), 1);
        assert!(result.contains(&in_scope_id));
    }

    #[test]
    fn test_get_matching_sources_excludes_source_level_excluded() {
        let mut conn = setup_test_db();

        let root = insert_root(&conn, "/photos", "source", false);
        let normal_id = insert_source(&conn, root, "normal.jpg", None, true, false);
        let _excluded_id = insert_source(&conn, root, "excluded.jpg", None, true, true); // source-level excluded

        let result = get_matching_sources(&mut conn, &[], &[], false).unwrap();

        assert_eq!(result.len(), 1);
        assert!(result.contains(&normal_id));
    }

    #[test]
    fn test_get_matching_sources_excludes_object_level_excluded() {
        let mut conn = setup_test_db();

        let root = insert_root(&conn, "/photos", "source", false);
        let normal_id = insert_source(&conn, root, "normal.jpg", None, true, false);

        // Source not excluded, but linked to excluded object
        let excluded_obj = insert_object(&conn, "abc123excluded", true);
        let _obj_excluded_id = insert_source(&conn, root, "obj_excluded.jpg", Some(excluded_obj), true, false);

        let result = get_matching_sources(&mut conn, &[], &[], false).unwrap();

        assert_eq!(result.len(), 1);
        assert!(result.contains(&normal_id));
    }

    #[test]
    fn test_get_matching_sources_includes_excluded_when_flag_set() {
        let mut conn = setup_test_db();

        let root = insert_root(&conn, "/photos", "source", false);
        let normal_id = insert_source(&conn, root, "normal.jpg", None, true, false);
        let source_excluded_id = insert_source(&conn, root, "source_excluded.jpg", None, true, true);

        let excluded_obj = insert_object(&conn, "abc123excluded", true);
        let obj_excluded_id = insert_source(&conn, root, "obj_excluded.jpg", Some(excluded_obj), true, false);

        // With include_excluded = true
        let result = get_matching_sources(&mut conn, &[], &[], true).unwrap();

        assert_eq!(result.len(), 3);
        assert!(result.contains(&normal_id));
        assert!(result.contains(&source_excluded_id));
        assert!(result.contains(&obj_excluded_id));
    }

    // =========================================================================
    // get_excluded_sources tests
    // =========================================================================

    #[test]
    fn test_get_excluded_sources_returns_source_level_only() {
        let mut conn = setup_test_db();

        let root = insert_root(&conn, "/photos", "source", false);
        let excluded_id = insert_source(&conn, root, "excluded.jpg", None, true, true); // source-level excluded

        let result = get_excluded_sources(&mut conn, &[], &[]).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, excluded_id);
    }

    #[test]
    fn test_get_excluded_sources_ignores_object_level_excluded() {
        let mut conn = setup_test_db();

        let root = insert_root(&conn, "/photos", "source", false);

        // Source NOT excluded, but object IS excluded
        let excluded_obj = insert_object(&conn, "abc123excluded", true);
        let _obj_excluded_id = insert_source(&conn, root, "obj_excluded.jpg", Some(excluded_obj), true, false);

        // This is the critical distinction: get_excluded_sources should NOT return this
        let result = get_excluded_sources(&mut conn, &[], &[]).unwrap();

        assert!(result.is_empty(), "Object-level excluded sources should NOT appear in get_excluded_sources");
    }

    #[test]
    fn test_get_excluded_sources_respects_scope() {
        let mut conn = setup_test_db();

        let root = insert_root(&conn, "/photos", "source", false);
        let in_scope_id = insert_source(&conn, root, "2024/excluded.jpg", None, true, true);
        let _out_of_scope_id = insert_source(&conn, root, "2023/excluded.jpg", None, true, true);

        // Scope to /photos/2024
        let scopes = vec!["/photos/2024".to_string()];
        let result = get_excluded_sources(&mut conn, &scopes, &[]).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, in_scope_id);
    }

    #[test]
    fn test_get_excluded_sources_returns_correct_path() {
        let mut conn = setup_test_db();

        let root = insert_root(&conn, "/photos", "source", false);
        let excluded_id = insert_source(&conn, root, "subdir/excluded.jpg", None, true, true);

        let result = get_excluded_sources(&mut conn, &[], &[]).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, excluded_id);
        assert_eq!(result[0].1, "/photos/subdir/excluded.jpg");
    }

    // =========================================================================
    // get_object_excluded_sources tests
    // =========================================================================

    #[test]
    fn test_get_object_excluded_sources_returns_object_level_only() {
        let mut conn = setup_test_db();

        let root = insert_root(&conn, "/photos", "source", false);

        // Source NOT excluded, but object IS excluded
        let excluded_obj = insert_object(&conn, "abc123excluded", true);
        let obj_excluded_id = insert_source(&conn, root, "obj_excluded.jpg", Some(excluded_obj), true, false);

        let result = get_object_excluded_sources(&mut conn, &[], &[]).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, obj_excluded_id);
    }

    #[test]
    fn test_get_object_excluded_sources_ignores_source_level_excluded() {
        let mut conn = setup_test_db();

        let root = insert_root(&conn, "/photos", "source", false);

        // Source IS excluded AND object IS excluded
        let excluded_obj = insert_object(&conn, "abc123excluded", true);
        let _both_excluded_id = insert_source(&conn, root, "both_excluded.jpg", Some(excluded_obj), true, true);

        // This is the critical distinction: when BOTH are excluded, it should NOT appear
        // (because the source is directly excluded)
        let result = get_object_excluded_sources(&mut conn, &[], &[]).unwrap();

        assert!(result.is_empty(), "Sources with source-level exclusion should NOT appear in get_object_excluded_sources");
    }

    #[test]
    fn test_get_object_excluded_sources_returns_hash_prefix() {
        let mut conn = setup_test_db();

        let root = insert_root(&conn, "/photos", "source", false);
        let hash = "abcdef1234567890abcdef1234567890abcdef1234567890";
        let excluded_obj = insert_object(&conn, hash, true);
        let _id = insert_source(&conn, root, "file.jpg", Some(excluded_obj), true, false);

        let result = get_object_excluded_sources(&mut conn, &[], &[]).unwrap();

        assert_eq!(result.len(), 1);
        // Hash prefix should be first 16 characters
        assert_eq!(result[0].2, "abcdef1234567890");
    }

    #[test]
    fn test_get_object_excluded_sources_respects_scope() {
        let mut conn = setup_test_db();

        let root = insert_root(&conn, "/photos", "source", false);
        let excluded_obj = insert_object(&conn, "abc123excluded", true);

        let in_scope_id = insert_source(&conn, root, "2024/file.jpg", Some(excluded_obj), true, false);
        let _out_of_scope_id = insert_source(&conn, root, "2023/file.jpg", Some(excluded_obj), true, false);

        // Scope to /photos/2024
        let scopes = vec!["/photos/2024".to_string()];
        let result = get_object_excluded_sources(&mut conn, &scopes, &[]).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, in_scope_id);
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

    #[test]
    fn test_exclude_duplicates_excludes_when_one_copy_in_prefer() {
        let mut db = make_test_db();
        let conn = db.conn_mut();

        // Setup: source root with a file, archive root with the same file (duplicate)
        let source_root = insert_root(conn, "/source", "source", false);
        let archive_root = insert_root(conn, "/archive", "archive", false);

        // Same object (same content)
        let obj = insert_object(conn, "same_content_hash", false);

        // Source file (candidate for exclusion)
        let source_id = insert_source(conn, source_root, "photo.jpg", Some(obj), true, false);

        // Archive copy (the preferred copy)
        let _archive_id = insert_source(conn, archive_root, "photo.jpg", Some(obj), true, false);

        // Run exclude_duplicates with prefer=/archive, scope=/source
        let result = exclude_duplicates(
            &mut db,
            Path::new("/archive"),
            Some(Path::new("/source")),
            &[],
            false, // not dry run
        );

        assert!(result.is_ok());

        // The source file should now be excluded
        assert!(
            is_source_excluded(db.conn(), source_id),
            "Source should be excluded when exactly one copy exists in prefer path"
        );
    }

    #[test]
    fn test_exclude_duplicates_skips_when_no_copy_in_prefer() {
        let mut db = make_test_db();
        let conn = db.conn_mut();

        // Setup: source root with a file, archive is empty (no duplicate there)
        let source_root = insert_root(conn, "/source", "source", false);
        let _archive_root = insert_root(conn, "/archive", "archive", false);

        let obj = insert_object(conn, "unique_content_hash", false);
        let source_id = insert_source(conn, source_root, "unique.jpg", Some(obj), true, false);

        // Run exclude_duplicates - no copy in /archive
        let result = exclude_duplicates(
            &mut db,
            Path::new("/archive"),
            Some(Path::new("/source")),
            &[],
            false,
        );

        assert!(result.is_ok());

        // Source should NOT be excluded (no backup exists)
        assert!(
            !is_source_excluded(db.conn(), source_id),
            "Source should NOT be excluded when no copy exists in prefer path"
        );
    }

    #[test]
    fn test_exclude_duplicates_skips_when_multiple_copies_in_prefer() {
        let mut db = make_test_db();
        let conn = db.conn_mut();

        // Setup: source with file, archive has TWO copies (ambiguous)
        let source_root = insert_root(conn, "/source", "source", false);
        let archive_root = insert_root(conn, "/archive", "archive", false);

        let obj = insert_object(conn, "duplicated_content", false);

        // Source file
        let source_id = insert_source(conn, source_root, "photo.jpg", Some(obj), true, false);

        // Two copies in archive (ambiguous - which is the canonical one?)
        let _archive_copy1 = insert_source(conn, archive_root, "copy1.jpg", Some(obj), true, false);
        let _archive_copy2 = insert_source(conn, archive_root, "copy2.jpg", Some(obj), true, false);

        // Run exclude_duplicates
        let result = exclude_duplicates(
            &mut db,
            Path::new("/archive"),
            Some(Path::new("/source")),
            &[],
            false,
        );

        assert!(result.is_ok());

        // Source should NOT be excluded (ambiguous - multiple copies)
        assert!(
            !is_source_excluded(db.conn(), source_id),
            "Source should NOT be excluded when multiple copies exist in prefer path"
        );
    }

    #[test]
    fn test_exclude_duplicates_skips_source_already_in_prefer() {
        let mut db = make_test_db();
        let conn = db.conn_mut();

        // Setup: file is in the archive (prefer path) itself
        let archive_root = insert_root(conn, "/archive", "archive", false);

        let obj = insert_object(conn, "archive_file_hash", false);

        // This file IS in the prefer path - should never be excluded
        let archive_file_id = insert_source(conn, archive_root, "keeper.jpg", Some(obj), true, false);

        // Run exclude_duplicates with scope=/archive (the file is in the prefer path)
        // Note: This tests the case where scope overlaps with prefer
        let result = exclude_duplicates(
            &mut db,
            Path::new("/archive"),
            Some(Path::new("/archive")),
            &[],
            false,
        );

        assert!(result.is_ok());

        // File in prefer path should NOT be excluded
        assert!(
            !is_source_excluded(db.conn(), archive_file_id),
            "Source in prefer path should never be excluded"
        );
    }

    #[test]
    fn test_exclude_duplicates_path_prefix_no_false_positive() {
        let mut db = make_test_db();
        let conn = db.conn_mut();

        // Setup: Test that /a/bc is NOT under /a/b (different directory names)
        // This tests the path-prefix matching logic for false positives
        let source_root = insert_root(conn, "/source", "source", false);
        let _archive_root = insert_root(conn, "/archive/photos", "archive", false);
        let other_root = insert_root(conn, "/archive/photos-old", "archive", false);

        let obj = insert_object(conn, "test_content", false);

        // Source file to potentially exclude
        let source_id = insert_source(conn, source_root, "file.jpg", Some(obj), true, false);

        // Copy in /archive/photos-old (NOT under /archive/photos)
        let _other_copy = insert_source(conn, other_root, "file.jpg", Some(obj), true, false);

        // Run exclude_duplicates with prefer=/archive/photos
        // The copy is in /archive/photos-old which should NOT match
        let result = exclude_duplicates(
            &mut db,
            Path::new("/archive/photos"),
            Some(Path::new("/source")),
            &[],
            false,
        );

        assert!(result.is_ok());

        // Source should NOT be excluded (/archive/photos-old is not under /archive/photos)
        assert!(
            !is_source_excluded(db.conn(), source_id),
            "Path prefix matching should not have false positives: /archive/photos-old is NOT under /archive/photos"
        );
    }

    #[test]
    fn test_exclude_duplicates_empty_rel_path() {
        // Test that duplicates are found correctly when a source has empty rel_path.
        // Empty rel_path means the root path IS the file (e.g., someone registered
        // "/archive/photo.jpg" as a root rather than "/archive" with rel_path "photo.jpg").
        let mut db = make_test_db();
        let conn = db.conn_mut();

        // Source file to potentially exclude (normal path)
        let source_root = insert_root(conn, "/source", "source", false);
        let obj = insert_object(conn, "duplicate_content", false);
        let source_id = insert_source(conn, source_root, "photo.jpg", Some(obj), true, false);

        // Archive "file" where the root IS the file (empty rel_path)
        // This simulates registering a single file as a root
        let archive_file_root = insert_root(conn, "/archive/photo.jpg", "archive", false);
        let _archive_id = insert_source(conn, archive_file_root, "", Some(obj), true, false);

        // Run exclude_duplicates with prefer=/archive/photo.jpg (the exact file path)
        let result = exclude_duplicates(
            &mut db,
            Path::new("/archive/photo.jpg"),
            Some(Path::new("/source")),
            &[],
            false,
        );

        assert!(result.is_ok());

        // The source file should be excluded - there's a copy at the prefer path
        // (even though the archive source has empty rel_path)
        assert!(
            is_source_excluded(db.conn(), source_id),
            "Source should be excluded when duplicate exists at prefer path with empty rel_path"
        );
    }
}
