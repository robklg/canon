use anyhow::{Context, Result};
use rusqlite::params;
use rusqlite::types::Value;
use std::path::{Path, PathBuf};

use crate::db::{Connection, Db};
use crate::scope::{build_scope_clause, ScopeMatch};
use crate::path::{canonicalize_scopes, path_is_under};
use crate::filter::{self, Filter};

const BATCH_SIZE: i64 = 1000;

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
        conn.execute("UPDATE sources SET excluded = 1 WHERE id = ?", [source_id])?;
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
        conn.execute("UPDATE sources SET excluded = 0 WHERE id = ?", [source_id])?;
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

/// SQL clause for excluding excluded sources (checks both source and object exclusions)
pub fn exclude_clause(include_excluded: bool) -> &'static str {
    if include_excluded {
        "1=1"
    } else {
        // Check both source-level and object-level exclusions
        // NOTE: Queries using this clause must LEFT JOIN objects o ON s.object_id = o.id
        "s.excluded = 0 AND (s.object_id IS NULL OR o.excluded = 0)"
    }
}

fn get_matching_sources(
    conn: &mut Connection,
    scope_prefixes: &[String],
    filters: &[Filter],
    include_excluded: bool,
) -> Result<Vec<i64>> {
    let mut all_sources = Vec::new();
    let mut last_id: i64 = 0;

    let exclude_sql = exclude_clause(include_excluded);
    let scopes = ScopeMatch::classify_all(scope_prefixes);
    let (scope_clause, scope_params) = build_scope_clause(&scopes);

    loop {
        // Build params: scope params + last_id + batch_size
        let mut params: Vec<Value> = scope_params.iter().map(|s| Value::from(s.clone())).collect();
        params.push(Value::from(last_id));
        params.push(Value::from(BATCH_SIZE));

        let source_ids: Vec<i64> = conn
            .prepare(&format!(
                "SELECT s.id FROM sources s
                 JOIN roots r ON s.root_id = r.id
                 LEFT JOIN objects o ON s.object_id = o.id
                 WHERE s.present = 1 AND r.role = 'source' AND r.suspended = 0 AND {} AND {} AND s.id > ?
                 ORDER BY s.id LIMIT ?",
                exclude_sql, scope_clause
            ))?
            .query_map(rusqlite::params_from_iter(params), |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()?;

        if source_ids.is_empty() {
            break;
        }

        last_id = *source_ids.last().unwrap();

        // Apply filters
        let filtered_ids = filter::apply_filters(conn, &source_ids, filters)?;
        all_sources.extend(filtered_ids);
    }

    Ok(all_sources)
}

fn get_excluded_sources(
    conn: &mut Connection,
    scope_prefixes: &[String],
    filters: &[Filter],
) -> Result<Vec<(i64, String)>> {
    let mut all_excluded = Vec::new();
    let mut last_id: i64 = 0;

    let scopes = ScopeMatch::classify_all(scope_prefixes);
    let (scope_clause, scope_params) = build_scope_clause(&scopes);

    loop {
        // Build params: last_id + scope params + batch_size
        let mut params: Vec<Value> = Vec::new();
        params.push(Value::from(last_id));
        for s in &scope_params {
            params.push(Value::from(s.clone()));
        }
        params.push(Value::from(BATCH_SIZE));

        let batch: Vec<(i64, String)> = conn
            .prepare(&format!(
                "SELECT s.id, r.path || '/' || s.rel_path as full_path
                 FROM sources s
                 JOIN roots r ON s.root_id = r.id
                 WHERE s.present = 1 AND r.role = 'source' AND r.suspended = 0 AND s.id > ?
                   AND {}
                   AND s.excluded = 1
                 ORDER BY s.id LIMIT ?",
                scope_clause
            ))?
            .query_map(rusqlite::params_from_iter(params), |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;

        if batch.is_empty() {
            break;
        }

        last_id = batch.last().map(|(id, _)| *id).unwrap();

        // Apply additional filters
        let ids: Vec<i64> = batch.iter().map(|(id, _)| *id).collect();
        let filtered_ids = filter::apply_filters(conn, &ids, filters)?;

        // Keep only filtered results
        for (id, path) in batch {
            if filtered_ids.contains(&id) {
                all_excluded.push((id, path));
            }
        }
    }

    Ok(all_excluded)
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

    conn.execute("UPDATE sources SET excluded = 1 WHERE id = ?", [source_id])?;

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

    conn.execute("UPDATE sources SET excluded = 1 WHERE id = ?", [source_id])?;

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

    // Resolve paths
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

    // Get matching sources in scope (candidates for exclusion)
    let source_ids = get_matching_sources(conn, &scope_prefixes, &filters, false)?;

    if source_ids.is_empty() {
        println!("No sources match the given filters.");
        return Ok(());
    }

    // For each scoped source, check if it has duplicates in the prefer path
    let mut to_exclude: Vec<(i64, String)> = Vec::new();
    let mut skipped_not_covered = 0usize;
    let mut skipped_multiple = 0usize;
    let mut skipped_no_hash = 0usize;

    for &source_id in &source_ids {
        // Get source info
        let source_info: Option<(i64, String, String)> = conn
            .query_row(
                "SELECT s.object_id, r.path, s.rel_path
                 FROM sources s
                 JOIN roots r ON s.root_id = r.id
                 WHERE s.id = ? AND s.object_id IS NOT NULL",
                [source_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .ok();

        let Some((object_id, root_path, rel_path)) = source_info else {
            skipped_no_hash += 1;
            continue;
        };

        let source_path = if rel_path.is_empty() {
            root_path
        } else {
            format!("{}/{}", root_path, rel_path)
        };

        // Skip if this source is already in the prefer path
        if path_is_under(&source_path, &prefer_prefix) {
            continue;
        }

        // Find duplicates of this source in the prefer path (excluding already-excluded sources)
        let prefer_copies: Vec<String> = conn
            .prepare(
                "SELECT r.path || '/' || s.rel_path
                 FROM sources s
                 JOIN roots r ON s.root_id = r.id
                 WHERE s.object_id = ? AND s.present = 1 AND s.id != ?
                   AND (r.path || '/' || s.rel_path LIKE ? || '/%' OR r.path || '/' || s.rel_path = ?)
                   AND s.excluded = 0"
            )?
            .query_map(params![object_id, source_id, prefer_prefix, prefer_prefix], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()?;

        match prefer_copies.len() {
            0 => {
                // No copy in prefer path
                skipped_not_covered += 1;
            }
            1 => {
                // Exactly one copy in prefer path - exclude this source
                to_exclude.push((source_id, source_path));
            }
            _ => {
                // Multiple copies in prefer path - ambiguous
                skipped_multiple += 1;
            }
        }
    }

    // Summary header
    let total_candidates = source_ids.len() - skipped_no_hash;
    println!("Sources in scope: {} ({} unhashed skipped)", source_ids.len(), skipped_no_hash);
    println!("  Will exclude: {}", to_exclude.len());
    println!("  Skipped (no copy in --prefer): {}", skipped_not_covered);
    println!("  Skipped (multiple copies in --prefer): {}", skipped_multiple);
    if total_candidates > 0 {
        let in_prefer_count = total_candidates - to_exclude.len() - skipped_not_covered - skipped_multiple;
        if in_prefer_count > 0 {
            println!("  Skipped (already in --prefer): {}", in_prefer_count);
        }
    }
    println!();

    if to_exclude.is_empty() {
        println!("Nothing to exclude.");
        return Ok(());
    }

    if dry_run {
        println!("Would exclude {} sources:", to_exclude.len());
        for (_, path) in &to_exclude {
            println!("  {}", path);
        }
        println!();
        println!("Use `canon ls --duplicates` to see remaining duplicates.");
        return Ok(());
    }

    // Execute exclusions
    let mut excluded_count = 0;

    for (source_id, _) in &to_exclude {
        // Skip if already excluded
        if is_excluded(conn, *source_id)? {
            continue;
        }

        conn.execute("UPDATE sources SET excluded = 1 WHERE id = ?", [source_id])?;
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
    let object_info: Option<(i64, String)> = conn
        .query_row(
            "SELECT id, hash_value FROM objects WHERE hash_value = ?",
            [hash],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .ok();

    let Some((object_id, hash_value)) = object_info else {
        anyhow::bail!("No object found with hash: {}", hash);
    };

    exclude_object_by_id(conn, object_id, &hash_value, options)
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
        conn.execute("UPDATE objects SET excluded = 1 WHERE id = ?", [object_id])?;
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

    conn.execute("UPDATE objects SET excluded = 1 WHERE id = ?", [object_id])?;

    println!("Excluded object: {}...", &hash_value[..16.min(hash_value.len())]);
    print_source_locations(&sources, options.verbose);
    Ok(())
}

/// Clear exclusion from an object by its hash
pub fn clear_object(db: &Db, hash: &str, options: &ClearOptions) -> Result<()> {
    let conn = db.conn();

    // Find the object by hash
    let object_info: Option<(i64, String)> = conn
        .query_row(
            "SELECT id, hash_value FROM objects WHERE hash_value = ?",
            [hash],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .ok();

    let Some((object_id, hash_value)) = object_info else {
        anyhow::bail!("No object found with hash: {}", hash);
    };

    // Check if excluded
    if !is_object_excluded(conn, object_id)? {
        println!("Object is not excluded: {}...", &hash_value[..16.min(hash_value.len())]);
        return Ok(());
    }

    if options.dry_run {
        println!("Would clear exclusion from object: {}...", &hash_value[..16.min(hash_value.len())]);
        return Ok(());
    }

    conn.execute("UPDATE objects SET excluded = 0 WHERE id = ?", [object_id])?;

    println!("Cleared exclusion from object: {}...", &hash_value[..16.min(hash_value.len())]);
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
    let mut all_excluded = Vec::new();
    let mut last_id: i64 = 0;

    let scopes = ScopeMatch::classify_all(scope_prefixes);
    let (scope_clause, scope_params) = build_scope_clause(&scopes);

    loop {
        let mut params: Vec<Value> = Vec::new();
        params.push(Value::from(last_id));
        for s in &scope_params {
            params.push(Value::from(s.clone()));
        }
        params.push(Value::from(BATCH_SIZE));

        let batch: Vec<(i64, String, String)> = conn
            .prepare(&format!(
                "SELECT s.id, r.path || '/' || s.rel_path as full_path, o.hash_value
                 FROM sources s
                 JOIN roots r ON s.root_id = r.id
                 JOIN objects o ON s.object_id = o.id
                 WHERE s.present = 1 AND r.role = 'source' AND r.suspended = 0 AND s.id > ?
                   AND {}
                   AND o.excluded = 1
                   AND s.excluded = 0
                 ORDER BY s.id LIMIT ?",
                scope_clause
            ))?
            .query_map(rusqlite::params_from_iter(params), |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;

        if batch.is_empty() {
            break;
        }

        last_id = batch.last().map(|(id, _, _)| *id).unwrap();

        // Apply additional filters
        let ids: Vec<i64> = batch.iter().map(|(id, _, _)| *id).collect();
        let filtered_ids = filter::apply_filters(conn, &ids, filters)?;

        for (id, path, hash) in batch {
            if filtered_ids.contains(&id) {
                let hash_short = hash[..16.min(hash.len())].to_string();
                all_excluded.push((id, path, hash_short));
            }
        }
    }

    Ok(all_excluded)
}
