use anyhow::Result;
use rusqlite::params;
use rusqlite::types::Value;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::db::{build_scope_clause, canonicalize_scopes, Connection, Db};
use crate::filter::{self, Filter};

const BATCH_SIZE: i64 = 1000;
const POLICY_EXCLUDE_KEY: &str = "policy.exclude";

// ============================================================================
// Options
// ============================================================================

pub struct SetOptions {
    pub dry_run: bool,
}

pub struct ClearOptions {
    pub dry_run: bool,
}

// ============================================================================
// Set Command
// ============================================================================

pub fn set(
    db: &Db,
    scope_paths: &[PathBuf],
    filter_strs: &[String],
    options: &SetOptions,
) -> Result<()> {
    let conn = db.conn();

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

    // Insert exclusion facts
    let now = current_timestamp();
    let mut excluded_count = 0;

    for source_id in &to_exclude {
        let basis_rev: i64 = conn.query_row(
            "SELECT basis_rev FROM sources WHERE id = ?",
            [source_id],
            |row| row.get(0),
        )?;

        conn.execute(
            "INSERT INTO facts (entity_type, entity_id, key, value_text, observed_at, observed_basis_rev)
             VALUES ('source', ?, ?, 'true', ?, ?)",
            params![source_id, POLICY_EXCLUDE_KEY, now, basis_rev],
        )?;
        excluded_count += 1;
    }

    println!("Excluded {} sources", excluded_count);
    Ok(())
}

// ============================================================================
// Clear Command
// ============================================================================

pub fn clear(
    db: &Db,
    scope_paths: &[PathBuf],
    filter_strs: &[String],
    options: &ClearOptions,
) -> Result<()> {
    let conn = db.conn();

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

    // Delete exclusion facts
    let mut cleared_count = 0;
    for (source_id, _) in &excluded_sources {
        let rows = conn.execute(
            "DELETE FROM facts
             WHERE entity_type = 'source' AND entity_id = ? AND key = ?",
            params![source_id, POLICY_EXCLUDE_KEY],
        )?;
        cleared_count += rows;
    }

    println!("Cleared exclusions for {} sources", cleared_count);
    Ok(())
}

// ============================================================================
// List Command
// ============================================================================

pub fn list(
    db: &Db,
    scope_paths: &[PathBuf],
    filter_strs: &[String],
) -> Result<()> {
    let conn = db.conn();

    // Parse filters
    let filters: Vec<Filter> = filter_strs
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    // Resolve scope paths
    let scope_prefixes = canonicalize_scopes(scope_paths)?;

    // Get excluded sources matching filters
    let excluded = get_excluded_sources(conn, &scope_prefixes, &filters)?;

    if excluded.is_empty() {
        println!("No excluded sources match the given filters");
        return Ok(());
    }

    println!("Excluded sources ({}):", excluded.len());
    for (id, path) in &excluded {
        println!("  {} (id: {})", path, id);
    }

    Ok(())
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Check if a source is excluded
pub fn is_excluded(conn: &Connection, source_id: i64) -> Result<bool> {
    let exists: bool = conn
        .query_row(
            "SELECT 1 FROM facts
             WHERE entity_type = 'source' AND entity_id = ? AND key = ?",
            params![source_id, POLICY_EXCLUDE_KEY],
            |_| Ok(true),
        )
        .unwrap_or(false);
    Ok(exists)
}

/// SQL clause for excluding excluded sources
pub fn exclude_clause(include_excluded: bool) -> &'static str {
    if include_excluded {
        "1=1"
    } else {
        "NOT EXISTS (SELECT 1 FROM facts WHERE entity_type = 'source' AND entity_id = s.id AND key = 'policy.exclude')"
    }
}

/// Count excluded sources in scope
pub fn count_excluded(conn: &Connection, scope_prefix: Option<&str>, include_archived: bool) -> Result<i64> {
    let role_clause = if include_archived { "1=1" } else { "r.role = 'source'" };

    let count: i64 = if let Some(prefix) = scope_prefix {
        conn.query_row(
            &format!(
                "SELECT COUNT(*) FROM sources s
                 JOIN roots r ON s.root_id = r.id
                 WHERE s.present = 1 AND {}
                   AND (r.path || '/' || s.rel_path) LIKE ? || '/%'
                   AND EXISTS (SELECT 1 FROM facts WHERE entity_type = 'source' AND entity_id = s.id AND key = ?)",
                role_clause
            ),
            params![prefix, POLICY_EXCLUDE_KEY],
            |row| row.get(0),
        )?
    } else {
        conn.query_row(
            &format!(
                "SELECT COUNT(*) FROM sources s
                 JOIN roots r ON s.root_id = r.id
                 WHERE s.present = 1 AND {}
                   AND EXISTS (SELECT 1 FROM facts WHERE entity_type = 'source' AND entity_id = s.id AND key = ?)",
                role_clause
            ),
            params![POLICY_EXCLUDE_KEY],
            |row| row.get(0),
        )?
    };
    Ok(count)
}

fn get_matching_sources(
    conn: &Connection,
    scope_prefixes: &[String],
    filters: &[Filter],
    include_excluded: bool,
) -> Result<Vec<i64>> {
    let mut all_sources = Vec::new();
    let mut last_id: i64 = 0;

    let exclude_sql = exclude_clause(include_excluded);
    let (scope_clause, scope_params) = build_scope_clause(scope_prefixes);

    loop {
        // Build params: scope params + last_id + batch_size
        let mut params: Vec<Value> = scope_params.iter().map(|s| Value::from(s.clone())).collect();
        params.push(Value::from(last_id));
        params.push(Value::from(BATCH_SIZE));

        let source_ids: Vec<i64> = conn
            .prepare(&format!(
                "SELECT s.id FROM sources s
                 JOIN roots r ON s.root_id = r.id
                 WHERE s.present = 1 AND r.role = 'source' AND {} AND {} AND s.id > ?
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
    conn: &Connection,
    scope_prefixes: &[String],
    filters: &[Filter],
) -> Result<Vec<(i64, String)>> {
    let mut all_excluded = Vec::new();
    let mut last_id: i64 = 0;

    let (scope_clause, scope_params) = build_scope_clause(scope_prefixes);

    loop {
        // Build params: last_id + scope params + POLICY_EXCLUDE_KEY + batch_size
        let mut params: Vec<Value> = Vec::new();
        params.push(Value::from(last_id));
        for s in &scope_params {
            params.push(Value::from(s.clone()));
        }
        params.push(Value::from(POLICY_EXCLUDE_KEY.to_string()));
        params.push(Value::from(BATCH_SIZE));

        let batch: Vec<(i64, String)> = conn
            .prepare(&format!(
                "SELECT s.id, r.path || '/' || s.rel_path as full_path
                 FROM sources s
                 JOIN roots r ON s.root_id = r.id
                 WHERE s.present = 1 AND r.role = 'source' AND s.id > ?
                   AND {}
                   AND EXISTS (
                       SELECT 1 FROM facts
                       WHERE entity_type = 'source' AND entity_id = s.id AND key = ?
                   )
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

fn current_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs() as i64
}

/// Exclude a specific source by ID
pub fn set_by_id(db: &Db, source_id: i64, options: &SetOptions) -> Result<()> {
    let conn = db.conn();

    // Verify source exists and get its path
    let source_info: Option<(String, i64)> = conn
        .query_row(
            "SELECT r.path || '/' || s.rel_path, s.basis_rev
             FROM sources s
             JOIN roots r ON s.root_id = r.id
             WHERE s.id = ? AND s.present = 1",
            [source_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .ok();

    let Some((path, basis_rev)) = source_info else {
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

    // Insert exclusion fact
    let now = current_timestamp();
    conn.execute(
        "INSERT INTO facts (entity_type, entity_id, key, value_text, observed_at, observed_basis_rev)
         VALUES ('source', ?, ?, 'true', ?, ?)",
        params![source_id, POLICY_EXCLUDE_KEY, now, basis_rev],
    )?;

    println!("Excluded source (id: {}): {}", source_id, path);
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
    db: &Db,
    prefer_path: &Path,
    scope_path: Option<&Path>,
    filter_strs: &[String],
    dry_run: bool,
) -> Result<()> {
    let conn = db.conn();

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
        if source_path.starts_with(&prefer_prefix) || source_path.starts_with(&format!("{}/", prefer_prefix)) {
            continue;
        }

        // Find duplicates of this source in the prefer path
        let prefer_copies: Vec<String> = conn
            .prepare(
                "SELECT r.path || '/' || s.rel_path
                 FROM sources s
                 JOIN roots r ON s.root_id = r.id
                 WHERE s.object_id = ? AND s.present = 1 AND s.id != ?
                   AND (r.path || '/' || s.rel_path LIKE ? || '/%' OR r.path || '/' || s.rel_path = ?)"
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
    let now = current_timestamp();
    let mut excluded_count = 0;

    for (source_id, _) in &to_exclude {
        // Skip if already excluded
        if is_excluded(conn, *source_id)? {
            continue;
        }

        let basis_rev: i64 = conn.query_row(
            "SELECT basis_rev FROM sources WHERE id = ?",
            [source_id],
            |row| row.get(0),
        )?;

        conn.execute(
            "INSERT INTO facts (entity_type, entity_id, key, value_text, observed_at, observed_basis_rev)
             VALUES ('source', ?, ?, 'true', ?, ?)",
            params![source_id, POLICY_EXCLUDE_KEY, now, basis_rev],
        )?;
        excluded_count += 1;
    }

    println!("Excluded {} sources", excluded_count);
    println!();
    println!("Use `canon ls --duplicates` to see remaining duplicates.");

    Ok(())
}
