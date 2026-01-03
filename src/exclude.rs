use anyhow::Result;
use rusqlite::params;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::db::{Connection, Db};
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
    scope_path: Option<&Path>,
    filter_strs: &[String],
    options: &SetOptions,
) -> Result<()> {
    let conn = db.conn();

    // Parse filters
    let filters: Vec<Filter> = filter_strs
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    // Resolve scope path
    let scope_prefix = if let Some(p) = scope_path {
        Some(std::fs::canonicalize(p)?.to_string_lossy().to_string())
    } else {
        None
    };

    // Get matching sources (only from source roots, exclude already-excluded)
    let source_ids = get_matching_sources(&conn, scope_prefix.as_deref(), &filters, false)?;

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
    scope_path: Option<&Path>,
    filter_strs: &[String],
    options: &ClearOptions,
) -> Result<()> {
    let conn = db.conn();

    // Parse filters
    let filters: Vec<Filter> = filter_strs
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    // Resolve scope path
    let scope_prefix = if let Some(p) = scope_path {
        Some(std::fs::canonicalize(p)?.to_string_lossy().to_string())
    } else {
        None
    };

    // Get excluded sources matching filters
    let excluded_sources = get_excluded_sources(&conn, scope_prefix.as_deref(), &filters)?;

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
    scope_path: Option<&Path>,
    filter_strs: &[String],
) -> Result<()> {
    let conn = db.conn();

    // Parse filters
    let filters: Vec<Filter> = filter_strs
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    // Resolve scope path
    let scope_prefix = if let Some(p) = scope_path {
        Some(std::fs::canonicalize(p)?.to_string_lossy().to_string())
    } else {
        None
    };

    // Get excluded sources matching filters
    let excluded = get_excluded_sources(&conn, scope_prefix.as_deref(), &filters)?;

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
                   AND (r.path || '/' || s.rel_path) LIKE ? || '%'
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
    scope_prefix: Option<&str>,
    filters: &[Filter],
    include_excluded: bool,
) -> Result<Vec<i64>> {
    let mut all_sources = Vec::new();
    let mut last_id: i64 = 0;

    let exclude_clause = exclude_clause(include_excluded);

    loop {
        let source_ids: Vec<i64> = if let Some(prefix) = scope_prefix {
            conn.prepare(&format!(
                "SELECT s.id FROM sources s
                 JOIN roots r ON s.root_id = r.id
                 WHERE s.present = 1 AND r.role = 'source' AND {} AND s.id > ?
                   AND (r.path || '/' || s.rel_path) LIKE ? || '%'
                 ORDER BY s.id LIMIT ?",
                exclude_clause
            ))?
            .query_map(params![last_id, prefix, BATCH_SIZE], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()?
        } else {
            conn.prepare(&format!(
                "SELECT s.id FROM sources s
                 JOIN roots r ON s.root_id = r.id
                 WHERE s.present = 1 AND r.role = 'source' AND {} AND s.id > ?
                 ORDER BY s.id LIMIT ?",
                exclude_clause
            ))?
            .query_map(params![last_id, BATCH_SIZE], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()?
        };

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
    scope_prefix: Option<&str>,
    filters: &[Filter],
) -> Result<Vec<(i64, String)>> {
    let mut all_excluded = Vec::new();
    let mut last_id: i64 = 0;

    loop {
        let batch: Vec<(i64, String)> = if let Some(prefix) = scope_prefix {
            conn.prepare(
                "SELECT s.id, r.path || '/' || s.rel_path as full_path
                 FROM sources s
                 JOIN roots r ON s.root_id = r.id
                 WHERE s.present = 1 AND r.role = 'source' AND s.id > ?
                   AND (r.path || '/' || s.rel_path) LIKE ? || '%'
                   AND EXISTS (
                       SELECT 1 FROM facts
                       WHERE entity_type = 'source' AND entity_id = s.id AND key = ?
                   )
                 ORDER BY s.id LIMIT ?"
            )?
            .query_map(params![last_id, prefix, POLICY_EXCLUDE_KEY, BATCH_SIZE], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?
        } else {
            conn.prepare(
                "SELECT s.id, r.path || '/' || s.rel_path as full_path
                 FROM sources s
                 JOIN roots r ON s.root_id = r.id
                 WHERE s.present = 1 AND r.role = 'source' AND s.id > ?
                   AND EXISTS (
                       SELECT 1 FROM facts
                       WHERE entity_type = 'source' AND entity_id = s.id AND key = ?
                   )
                 ORDER BY s.id LIMIT ?"
            )?
            .query_map(params![last_id, POLICY_EXCLUDE_KEY, BATCH_SIZE], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?
        };

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
