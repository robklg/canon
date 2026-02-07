//! Source repository — infrastructure layer for fetching sources.
//!
//! This module provides batch fetch functions that return `Source` structs
//! from the database. It is intentionally "dumb" — no domain logic here,
//! just data access.
//!
//! ## Design Principles
//!
//! 1. **Simple SQL**: Queries do data access only, no business logic in WHERE clauses
//! 2. **Batch fetching**: Avoid N+1 queries by fetching in chunks of BATCH_SIZE
//! 3. **Returns domain types**: Functions return `Source` structs, not raw rows
//! 4. **present=1 baked in**: Only fetches present (non-deleted) sources
//!
//! ## Usage
//!
//! ```ignore
//! use canon::source_repo;
//!
//! // Fetch all sources for specific roots
//! let sources = source_repo::batch_fetch_by_roots(conn, &[1, 2, 3])?;
//! ```

use std::collections::HashMap;

use anyhow::Result;
use rusqlite::types::Value;
use rusqlite::OptionalExtension;

use super::db::Connection;
use crate::domain::scan::{FileObservation, Reconciliation};
use crate::domain::source::{NewSource, Source};

/// Batch size for SQL IN clauses. Consistent across all repositories.
pub const BATCH_SIZE: usize = 1000;

/// The columns we SELECT for Source construction.
/// Kept as a constant to ensure consistency across fetch functions.
const SOURCE_COLUMNS: &str = r#"
    s.id,
    s.root_id,
    r.path as root_path,
    s.rel_path,
    s.object_id,
    s.size,
    s.mtime,
    s.excluded,
    o.excluded as object_excluded,
    s.device,
    s.inode,
    s.partial_hash,
    s.basis_rev,
    r.role as root_role,
    r.suspended as root_suspended
"#;

/// The base FROM/JOIN clause for Source queries.
const SOURCE_FROM: &str = r#"
    FROM sources s
    JOIN roots r ON s.root_id = r.id
    LEFT JOIN objects o ON s.object_id = o.id
"#;

/// Construct a Source from a row. Column order must match SOURCE_COLUMNS.
fn source_from_row(row: &rusqlite::Row) -> rusqlite::Result<Source> {
    Ok(Source {
        id: row.get(0)?,
        root_id: row.get(1)?,
        root_path: row.get(2)?,
        rel_path: row.get(3)?,
        object_id: row.get(4)?,
        size: row.get(5)?,
        mtime: row.get(6)?,
        excluded: row.get(7)?,
        object_excluded: row.get(8)?,
        device: row.get(9)?,
        inode: row.get(10)?,
        partial_hash: row.get(11)?,
        basis_rev: row.get(12)?,
        root_role: row.get(13)?,
        root_suspended: row.get(14)?,
    })
}

/// Fetch all present sources for the given root IDs.
///
/// Returns sources in no particular order. Callers should sort if needed.
///
/// This is a simple fetch with no filtering beyond `present = 1`.
/// Domain filtering (scope, exclusion, role) should be done in Rust
/// using the Source predicates.
pub fn batch_fetch_by_roots(conn: &Connection, root_ids: &[i64]) -> Result<Vec<Source>> {
    if root_ids.is_empty() {
        return Ok(Vec::new());
    }

    let mut sources = Vec::new();

    // Process root_ids in batches
    for chunk in root_ids.chunks(BATCH_SIZE) {
        let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
        let sql = format!(
            "SELECT {} {} WHERE s.present = 1 AND s.root_id IN ({})",
            SOURCE_COLUMNS,
            SOURCE_FROM,
            placeholders.join(",")
        );

        let params: Vec<Value> = chunk.iter().map(|&id| Value::from(id)).collect();
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(params), source_from_row)?;

        for row in rows {
            sources.push(row?);
        }
    }

    Ok(sources)
}

/// Fetch sources by their IDs, returning a HashMap for O(1) lookup.
///
/// This is useful when you have a list of source IDs (e.g., from filter results)
/// and need to fetch the full Source data for each.
///
/// Only present sources are returned. If an ID doesn't exist or the source
/// is not present, it won't appear in the result map.
pub fn batch_fetch_by_ids(conn: &Connection, source_ids: &[i64]) -> Result<HashMap<i64, Source>> {
    if source_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let mut sources = HashMap::with_capacity(source_ids.len());

    // Process source_ids in batches
    for chunk in source_ids.chunks(BATCH_SIZE) {
        let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
        let sql = format!(
            "SELECT {} {} WHERE s.present = 1 AND s.id IN ({})",
            SOURCE_COLUMNS,
            SOURCE_FROM,
            placeholders.join(",")
        );

        let params: Vec<Value> = chunk.iter().map(|&id| Value::from(id)).collect();
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(params), source_from_row)?;

        for row in rows {
            let source = row?;
            sources.insert(source.id, source);
        }
    }

    Ok(sources)
}

/// Fetch all sources that share the given object IDs, grouped by object_id.
///
/// Used for finding duplicates — given content hashes (via object_id), find all
/// file locations that contain that content.
///
/// # Returns
/// HashMap where key is object_id and value is Vec of all present Sources with
/// that object. Sources include full root_path for path computation via `Source::path()`.
///
/// # Example
/// ```ignore
/// let sources_by_object = fetch_sources_by_object_ids(conn, &object_ids)?;
/// for (object_id, sources) in sources_by_object {
///     // sources contains all files with this content
///     for source in sources {
///         println!("{}", source.path());
///     }
/// }
/// ```
pub fn fetch_sources_by_object_ids(
    conn: &Connection,
    object_ids: &[i64],
) -> Result<HashMap<i64, Vec<Source>>> {
    if object_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let mut result: HashMap<i64, Vec<Source>> = HashMap::new();

    // Process object_ids in batches
    for chunk in object_ids.chunks(BATCH_SIZE) {
        let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
        let sql = format!(
            "SELECT {} {} WHERE s.present = 1 AND s.object_id IN ({})",
            SOURCE_COLUMNS,
            SOURCE_FROM,
            placeholders.join(",")
        );

        let params: Vec<Value> = chunk.iter().map(|&id| Value::from(id)).collect();
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(params), source_from_row)?;

        for row in rows {
            let source = row?;
            if let Some(object_id) = source.object_id {
                result.entry(object_id).or_default().push(source);
            }
        }
    }

    Ok(result)
}

/// Fetch a single source by root_id and rel_path.
///
/// Returns None if no present source exists at that path.
/// Used during scan reconciliation to find existing source at the observed path.
pub fn fetch_by_path(conn: &Connection, root_id: i64, rel_path: &str) -> Result<Option<Source>> {
    let sql = format!(
        "SELECT {} {} WHERE s.present = 1 AND s.root_id = ? AND s.rel_path = ?",
        SOURCE_COLUMNS,
        SOURCE_FROM,
    );

    let result = conn
        .query_row(&sql, rusqlite::params![root_id, rel_path], source_from_row)
        .optional()?;

    Ok(result)
}

/// Fetch a source by its device and inode.
///
/// Searches across ALL roots to detect file moves (including cross-root moves).
/// Returns None if no present source exists with matching device+inode.
///
/// # Note
/// This search is global across all roots because files can be moved between roots.
/// The caller should use the returned source's root_id to detect cross-root moves.
pub fn fetch_by_inode(conn: &Connection, device: u64, inode: u64) -> Result<Option<Source>> {
    let sql = format!(
        "SELECT {} {} WHERE s.present = 1 AND s.device = ? AND s.inode = ?",
        SOURCE_COLUMNS,
        SOURCE_FROM,
    );

    let result = conn
        .query_row(
            &sql,
            rusqlite::params![device as i64, inode as i64],
            source_from_row,
        )
        .optional()?;

    Ok(result)
}

/// Check which destination paths are already registered in an archive.
///
/// This is used by apply's preflight check to detect destination conflicts
/// before any file operations begin. In regular mode, any existing paths
/// are an error. In --resume mode, existing paths are classified for skip/transfer.
///
/// # Arguments
/// * `conn` - Database connection
/// * `archive_root_id` - The archive root to check within
/// * `rel_paths` - Relative paths to check (within the archive)
///
/// # Returns
/// Set of rel_paths that exist in the archive with present=1.
/// Paths not in the result set are available for writing.
///
/// # Example
/// ```ignore
/// let existing = batch_check_paths_exist(conn, archive_id, &["2024/a.jpg", "2024/b.jpg"])?;
/// if existing.contains("2024/a.jpg") {
///     // This path is already occupied
/// }
/// ```
pub fn batch_check_paths_exist(
    conn: &Connection,
    archive_root_id: i64,
    rel_paths: &[&str],
) -> Result<std::collections::HashSet<String>> {
    use std::collections::HashSet;

    if rel_paths.is_empty() {
        return Ok(HashSet::new());
    }

    let mut result = HashSet::new();

    // Process rel_paths in batches to avoid SQLite variable limit
    for chunk in rel_paths.chunks(BATCH_SIZE) {
        let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
        let sql = format!(
            "SELECT rel_path FROM sources WHERE root_id = ? AND present = 1 AND rel_path IN ({})",
            placeholders.join(", ")
        );

        // Build params: archive_root_id first, then all rel_paths
        let mut params: Vec<&dyn rusqlite::ToSql> = Vec::with_capacity(chunk.len() + 1);
        params.push(&archive_root_id);
        for path in chunk {
            params.push(path);
        }

        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params.as_slice(), |row| {
            row.get::<_, String>(0)
        })?;

        for row in rows {
            result.insert(row?);
        }
    }

    Ok(result)
}

/// Insert a new source record for a destination file in an archive.
///
/// This function registers a file that has been copied or moved to an archive root.
/// It handles both fresh inserts and updates to stale records.
///
/// # Behavior
///
/// - **Fresh insert**: If no record exists for (root_id, rel_path), creates a new
///   record with basis_rev=0.
/// - **Stale record revival**: If a stale record exists (present=0), updates it
///   with the new metadata, increments basis_rev, and sets present=1. This preserves
///   the row history and correctly reflects that new content now exists at this path.
/// - **Active record conflict**: If an active record exists (present=1), returns an
///   error. The caller's pre-flight check should have prevented this.
///
/// # Returns
///
/// The complete Source record as it exists in the database after the operation,
/// including joined fields (root_path, root_role, root_suspended, object_excluded).
/// This is fetched via SELECT after the write to ensure accuracy.
///
/// # Caller Responsibilities
///
/// - Ensure the file has been successfully written to disk before calling
/// - Manage transaction boundaries (this function does not BEGIN/COMMIT)
/// - Run pre-flight checks to detect active record conflicts before file operations
///
/// # Example
///
/// ```ignore
/// let new_source = NewSource {
///     root_id: archive_root_id,
///     rel_path: "2024/photo.jpg".to_string(),
///     size: 1024,
///     mtime: 1704067200,
///     partial_hash: "abc123".to_string(),
///     object_id: Some(42),
///     device: Some(65024),
///     inode: Some(12345),
/// };
///
/// let created = repo::source::insert_destination(conn, &new_source)?;
/// println!("Created source {} at {}", created.id, created.path());
/// ```
pub fn insert_destination(conn: &Connection, new: &NewSource) -> Result<Source> {
    use std::time::{SystemTime, UNIX_EPOCH};

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs() as i64;

    // First try to update an existing stale record (present=0).
    // This preserves the row and increments basis_rev to reflect new content at this path.
    let updated = conn.execute(
        "UPDATE sources SET
            device = COALESCE(?, device),
            inode = COALESCE(?, inode),
            size = ?,
            mtime = ?,
            partial_hash = ?,
            object_id = ?,
            basis_rev = basis_rev + 1,
            scanned_at = ?,
            last_seen_at = ?,
            present = 1,
            excluded = 0
         WHERE root_id = ? AND rel_path = ? AND present = 0",
        rusqlite::params![
            new.device,
            new.inode,
            new.size,
            new.mtime,
            new.partial_hash,
            new.object_id,
            now,
            now,
            new.root_id,
            new.rel_path,
        ],
    )?;

    if updated == 0 {
        // No stale record exists. Insert new record.
        // Use COALESCE for device/inode to handle platforms without these values.
        conn.execute(
            "INSERT INTO sources (
                root_id, rel_path, device, inode, size, mtime, partial_hash,
                object_id, basis_rev, scanned_at, last_seen_at, present, excluded
             ) VALUES (?, ?, COALESCE(?, 0), COALESCE(?, 0), ?, ?, ?, ?, 0, ?, ?, 1, 0)",
            rusqlite::params![
                new.root_id,
                new.rel_path,
                new.device,
                new.inode,
                new.size,
                new.mtime,
                new.partial_hash,
                new.object_id,
                now,
                now,
            ],
        )?;
    }

    // Fetch the complete Source record with all joined fields.
    // This ensures the returned Source accurately reflects database state.
    fetch_by_path(conn, new.root_id, &new.rel_path)?
        .ok_or_else(|| anyhow::anyhow!(
            "Failed to fetch source after insert: root_id={}, rel_path={}",
            new.root_id,
            new.rel_path
        ))
}

/// Apply a reconciliation outcome to the database.
///
/// Translates the domain `Reconciliation` into the appropriate SQL operation.
/// This function does NOT manage transactions — the caller should wrap the call
/// in a transaction if atomicity with other operations is needed.
///
/// # Behavior by Reconciliation variant
///
/// - **New**: INSERT source with basis_rev=0, scanned_at=now, present=1
/// - **Unchanged**: UPDATE last_seen_at=now only
/// - **Modified**: UPDATE size, mtime, partial_hash, device, inode, basis_rev+1, last_seen_at=now
/// - **Moved**: UPDATE root_id, rel_path, device, inode, size, mtime, last_seen_at=now
/// - **Disconnected**: No database operation; returns the existing Source unchanged
///
/// # Returns
///
/// The complete Source record after the operation (via SELECT).
/// This ensures the returned Source accurately reflects database state,
/// including all joined fields (root_path, root_role, object_excluded).
///
/// # Caller Responsibilities
///
/// - Ensure `observation.partial_hash` is set for New and Modified reconciliations
/// - Manage transaction boundaries
/// - Handle Disconnected appropriately (log warning, track in stats)
pub fn apply_reconciliation(
    conn: &Connection,
    observation: &FileObservation,
    reconciliation: &Reconciliation,
    now: i64,
) -> Result<Source> {
    match reconciliation {
        Reconciliation::New => {
            // INSERT new source with basis_rev=0, or revive stale record at same path.
            //
            // Two cases lead here:
            // 1. Truly new file: no record exists at this path
            // 2. Replaced file: old file was deleted/marked-missing, new file created at same path
            //
            // We use the same two-step pattern as insert_destination():
            // - First try UPDATE WHERE present=0 (revive stale record)
            // - If no rows updated, INSERT new record
            let partial_hash = observation
                .partial_hash
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("partial_hash required for New reconciliation"))?;

            // Step 1: Try to update any existing record at this path (stale or replaced)
            // - Stale (present=0): file reappeared at previously-used path
            // - Replaced (present=1, different inode): old file deleted, new file at same path
            let updated = conn.execute(
                "UPDATE sources SET
                    device = ?, inode = ?, size = ?, mtime = ?, partial_hash = ?,
                    basis_rev = 0, scanned_at = ?, last_seen_at = ?,
                    present = 1, excluded = 0, object_id = NULL
                 WHERE root_id = ? AND rel_path = ?",
                rusqlite::params![
                    observation.device as i64,
                    observation.inode as i64,
                    observation.size,
                    observation.mtime,
                    partial_hash,
                    now,
                    now,
                    observation.root_id,
                    observation.rel_path,
                ],
            )?;

            if updated == 0 {
                // Step 2: No stale record exists, insert new
                conn.execute(
                    "INSERT INTO sources (
                        root_id, rel_path, device, inode, size, mtime, partial_hash,
                        basis_rev, scanned_at, last_seen_at, present, excluded
                     ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, 1, 0)",
                    rusqlite::params![
                        observation.root_id,
                        observation.rel_path,
                        observation.device as i64,
                        observation.inode as i64,
                        observation.size,
                        observation.mtime,
                        partial_hash,
                        now,
                        now,
                    ],
                )?;
            }

            fetch_by_path(conn, observation.root_id, &observation.rel_path)?
                .ok_or_else(|| anyhow::anyhow!("Failed to fetch source after insert"))
        }

        Reconciliation::Unchanged { source_id } => {
            // UPDATE last_seen_at and device/inode metadata
            // Device/inode may change legitimately (e.g., NAS remount, drive replacement)
            // Even though content is unchanged, we update current location metadata
            conn.execute(
                "UPDATE sources SET device = ?, inode = ?, last_seen_at = ? WHERE id = ?",
                rusqlite::params![
                    observation.device as i64,
                    observation.inode as i64,
                    now,
                    source_id
                ],
            )?;

            fetch_by_id(conn, *source_id)?
                .ok_or_else(|| anyhow::anyhow!("Failed to fetch source after update"))
        }

        Reconciliation::Modified { source_id, .. } => {
            // UPDATE with new metadata, increment basis_rev
            let partial_hash = observation
                .partial_hash
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("partial_hash required for Modified reconciliation"))?;

            conn.execute(
                "UPDATE sources SET
                    device = ?, inode = ?, size = ?, mtime = ?,
                    partial_hash = ?, basis_rev = basis_rev + 1,
                    last_seen_at = ?, present = 1
                 WHERE id = ?",
                rusqlite::params![
                    observation.device as i64,
                    observation.inode as i64,
                    observation.size,
                    observation.mtime,
                    partial_hash,
                    now,
                    source_id,
                ],
            )?;

            fetch_by_id(conn, *source_id)?
                .ok_or_else(|| anyhow::anyhow!("Failed to fetch source after update"))
        }

        Reconciliation::Moved { source_id, .. } => {
            // UPDATE path and location metadata
            conn.execute(
                "UPDATE sources SET
                    root_id = ?, rel_path = ?,
                    device = ?, inode = ?, size = ?, mtime = ?,
                    last_seen_at = ?, present = 1
                 WHERE id = ?",
                rusqlite::params![
                    observation.root_id,
                    observation.rel_path,
                    observation.device as i64,
                    observation.inode as i64,
                    observation.size,
                    observation.mtime,
                    now,
                    source_id,
                ],
            )?;

            fetch_by_id(conn, *source_id)?
                .ok_or_else(|| anyhow::anyhow!("Failed to fetch source after update"))
        }
    }
}

/// Fetch a single source by ID (internal helper).
fn fetch_by_id(conn: &Connection, source_id: i64) -> Result<Option<Source>> {
    let sql = format!(
        "SELECT {} {} WHERE s.id = ?",
        SOURCE_COLUMNS,
        SOURCE_FROM,
    );

    let result = conn
        .query_row(&sql, rusqlite::params![source_id], source_from_row)
        .optional()?;

    Ok(result)
}

/// Mark sources as no longer present (missing from filesystem).
///
/// Sets `present=0` for all specified source IDs. This does NOT delete records —
/// the history is preserved for tracking and potential revival if the file reappears.
///
/// # Arguments
///
/// - `source_ids`: IDs of sources to mark as missing
/// - `now`: Timestamp to record as last_seen_at
///
/// # Returns
///
/// Count of sources that were marked as missing.
///
/// # Note
///
/// Sources already marked as not present (present=0) are not counted in the return value.
/// This function handles empty input gracefully (returns 0).
pub fn mark_missing(conn: &Connection, source_ids: &[i64], now: i64) -> Result<u64> {
    if source_ids.is_empty() {
        return Ok(0);
    }

    let mut total_updated = 0u64;

    for chunk in source_ids.chunks(BATCH_SIZE) {
        let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
        let sql = format!(
            "UPDATE sources SET present = 0, last_seen_at = ? WHERE present = 1 AND id IN ({})",
            placeholders.join(",")
        );

        // Build params: now first, then all the IDs
        let mut params: Vec<rusqlite::types::Value> = Vec::with_capacity(chunk.len() + 1);
        params.push(rusqlite::types::Value::from(now));
        for &id in chunk {
            params.push(rusqlite::types::Value::from(id));
        }

        let updated = conn.execute(&sql, rusqlite::params_from_iter(params))?;
        total_updated += updated as u64;
    }

    Ok(total_updated)
}

/// Fetch source IDs for a given root (for missing detection).
///
/// Returns the set of present source IDs for the specified root.
/// Used at the start of a scan to track which sources should be seen.
///
/// # Arguments
/// - `conn`: Database connection
/// - `root_id`: The root to fetch sources for
/// - `scan_prefix`: Optional path prefix to filter sources (e.g., "photos/" only returns
///   sources whose rel_path starts with "photos/")
pub fn fetch_source_ids_for_root(
    conn: &Connection,
    root_id: i64,
    scan_prefix: Option<&str>,
) -> Result<Vec<i64>> {
    let ids: Vec<i64> = match scan_prefix {
        Some(prefix) => {
            let pattern = format!("{}%", prefix);
            conn.prepare(
                "SELECT id FROM sources WHERE root_id = ? AND present = 1 AND rel_path LIKE ?"
            )?
            .query_map(rusqlite::params![root_id, pattern], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()?
        }
        None => {
            conn.prepare(
                "SELECT id FROM sources WHERE root_id = ? AND present = 1"
            )?
            .query_map(rusqlite::params![root_id], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()?
        }
    };

    Ok(ids)
}

/// Set the exclusion flag for a single source.
///
/// # Behavior
/// - Updates `excluded` column to the specified value
/// - No error if source doesn't exist (0 rows affected)
/// - Does NOT affect object-level exclusion
///
/// # Returns
/// Ok(()) on success. To verify the source existed, use batch variant which returns count.
pub fn set_excluded(conn: &Connection, source_id: i64, excluded: bool) -> Result<()> {
    conn.execute(
        "UPDATE sources SET excluded = ? WHERE id = ?",
        rusqlite::params![excluded as i64, source_id],
    )?;
    Ok(())
}

/// Set the exclusion flag for multiple sources.
///
/// # Behavior
/// - Updates `excluded` column for all specified sources
/// - Handles large inputs via chunking (BATCH_SIZE = 1000)
/// - Sources that don't exist are silently skipped
///
/// # Returns
/// Count of rows actually updated (may be less than input if some sources don't exist).
pub fn batch_set_excluded(conn: &Connection, source_ids: &[i64], excluded: bool) -> Result<u64> {
    if source_ids.is_empty() {
        return Ok(0);
    }

    let mut total_updated = 0u64;

    for chunk in source_ids.chunks(BATCH_SIZE) {
        let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
        let sql = format!(
            "UPDATE sources SET excluded = ? WHERE id IN ({})",
            placeholders.join(",")
        );

        // Build params: excluded flag first, then all the IDs
        let mut params: Vec<rusqlite::types::Value> = Vec::with_capacity(chunk.len() + 1);
        params.push(rusqlite::types::Value::from(excluded as i64));
        for &id in chunk {
            params.push(rusqlite::types::Value::from(id));
        }

        let updated = conn.execute(&sql, rusqlite::params_from_iter(params))?;
        total_updated += updated as u64;
    }

    Ok(total_updated)
}

/// Insert a source for testing purposes.
///
/// This function is only available in test builds. It provides a simple way
/// to set up test data with specific device/inode values for move detection tests.
#[cfg(test)]
pub fn insert_test_source(
    conn: &Connection,
    root_id: i64,
    rel_path: &str,
    device: i64,
    inode: i64,
    size: i64,
    mtime: i64,
) -> i64 {
    conn.execute(
        "INSERT INTO sources (root_id, rel_path, device, inode, size, mtime, partial_hash, scanned_at, last_seen_at)
         VALUES (?, ?, ?, ?, ?, ?, 'testhash', 0, 0)",
        rusqlite::params![root_id, rel_path, device, inode, size, mtime],
    )
    .unwrap();
    conn.last_insert_rowid()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::open_in_memory_for_test;
    use rusqlite::Connection as RusqliteConnection;

    /// Create an in-memory database with the full schema.
    fn setup_test_db() -> RusqliteConnection {
        open_in_memory_for_test()
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
            "INSERT INTO sources (root_id, rel_path, object_id, device, inode, size, mtime, partial_hash, scanned_at, last_seen_at, present, excluded)
             VALUES (?, ?, ?, 0, 0, 1000, 1704067200, 'hash', 0, 0, ?, ?)",
            rusqlite::params![root_id, rel_path, object_id, present as i64, excluded as i64],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    // =========================================================================
    // batch_fetch_by_roots tests
    // =========================================================================

    #[test]
    fn batch_fetch_by_roots_empty_ids() {
        let conn = setup_test_db();
        let result = batch_fetch_by_roots(&conn, &[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn batch_fetch_by_roots_no_matching_roots() {
        let conn = setup_test_db();
        // Query for non-existent root IDs
        let result = batch_fetch_by_roots(&conn, &[999, 1000]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn batch_fetch_by_roots_single_root() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, "a.jpg", None, true, false);
        insert_source(&conn, root_id, "b.jpg", None, true, false);

        let sources = batch_fetch_by_roots(&conn, &[root_id]).unwrap();
        assert_eq!(sources.len(), 2);

        // Verify source data is populated correctly
        let source = sources.iter().find(|s| s.rel_path == "a.jpg").unwrap();
        assert_eq!(source.root_path, "/photos");
        assert_eq!(source.root_role, "source");
        assert!(!source.root_suspended);
    }

    #[test]
    fn batch_fetch_by_roots_multiple_roots() {
        let conn = setup_test_db();

        let root1 = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        let root2 = crate::repo::insert_test_root(&conn, "/archive", "archive", false);

        insert_source(&conn, root1, "photo.jpg", None, true, false);
        insert_source(&conn, root2, "backup.jpg", None, true, false);

        let sources = batch_fetch_by_roots(&conn, &[root1, root2]).unwrap();
        assert_eq!(sources.len(), 2);

        // Verify roles are correct
        let photo = sources.iter().find(|s| s.rel_path == "photo.jpg").unwrap();
        assert_eq!(photo.root_role, "source");

        let backup = sources.iter().find(|s| s.rel_path == "backup.jpg").unwrap();
        assert_eq!(backup.root_role, "archive");
    }

    #[test]
    fn batch_fetch_by_roots_excludes_non_present() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, "present.jpg", None, true, false);
        insert_source(&conn, root_id, "deleted.jpg", None, false, false); // present=false

        let sources = batch_fetch_by_roots(&conn, &[root_id]).unwrap();
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].rel_path, "present.jpg");
    }

    #[test]
    fn batch_fetch_by_roots_includes_excluded_sources() {
        // Repository layer fetches ALL present sources, including excluded ones.
        // Filtering by exclusion is done in the domain layer.
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, "normal.jpg", None, true, false);
        insert_source(&conn, root_id, "excluded.jpg", None, true, true); // excluded=true

        let sources = batch_fetch_by_roots(&conn, &[root_id]).unwrap();
        assert_eq!(sources.len(), 2);

        let excluded = sources.iter().find(|s| s.rel_path == "excluded.jpg").unwrap();
        assert!(excluded.excluded);
    }

    #[test]
    fn batch_fetch_by_roots_includes_object_excluded() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        let obj_id = insert_object(&conn, "abc123", true); // object excluded
        insert_source(&conn, root_id, "file.jpg", Some(obj_id), true, false);

        let sources = batch_fetch_by_roots(&conn, &[root_id]).unwrap();
        assert_eq!(sources.len(), 1);

        let source = &sources[0];
        assert!(!source.excluded); // source not excluded
        assert_eq!(source.object_excluded, Some(true)); // but object is
        assert!(source.is_excluded()); // domain predicate catches both
    }

    #[test]
    fn batch_fetch_by_roots_suspended_root() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", true); // suspended
        insert_source(&conn, root_id, "file.jpg", None, true, false);

        let sources = batch_fetch_by_roots(&conn, &[root_id]).unwrap();
        assert_eq!(sources.len(), 1);
        assert!(sources[0].root_suspended);
        assert!(!sources[0].is_active()); // domain predicate
    }

    // =========================================================================
    // batch_fetch_by_ids tests
    // =========================================================================

    #[test]
    fn batch_fetch_by_ids_empty_ids() {
        let conn = setup_test_db();
        let result = batch_fetch_by_ids(&conn, &[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn batch_fetch_by_ids_no_matching_ids() {
        let conn = setup_test_db();
        let result = batch_fetch_by_ids(&conn, &[999, 1000]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn batch_fetch_by_ids_returns_hashmap() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        let id1 = insert_source(&conn, root_id, "a.jpg", None, true, false);
        let id2 = insert_source(&conn, root_id, "b.jpg", None, true, false);

        let sources = batch_fetch_by_ids(&conn, &[id1, id2]).unwrap();
        assert_eq!(sources.len(), 2);

        // Verify O(1) lookup works
        assert_eq!(sources.get(&id1).unwrap().rel_path, "a.jpg");
        assert_eq!(sources.get(&id2).unwrap().rel_path, "b.jpg");
    }

    #[test]
    fn batch_fetch_by_ids_excludes_non_present() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        let present_id = insert_source(&conn, root_id, "present.jpg", None, true, false);
        let deleted_id = insert_source(&conn, root_id, "deleted.jpg", None, false, false);

        let sources = batch_fetch_by_ids(&conn, &[present_id, deleted_id]).unwrap();
        assert_eq!(sources.len(), 1);
        assert!(sources.contains_key(&present_id));
        assert!(!sources.contains_key(&deleted_id));
    }

    #[test]
    fn batch_fetch_by_ids_partial_match() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        let id1 = insert_source(&conn, root_id, "exists.jpg", None, true, false);

        // Query for mix of existing and non-existing IDs
        let sources = batch_fetch_by_ids(&conn, &[id1, 999, 1000]).unwrap();
        assert_eq!(sources.len(), 1);
        assert!(sources.contains_key(&id1));
    }

    // =========================================================================
    // fetch_sources_by_object_ids tests
    // =========================================================================

    #[test]
    fn fetch_sources_by_object_ids_empty_input() {
        let conn = setup_test_db();
        let result = fetch_sources_by_object_ids(&conn, &[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn fetch_sources_by_object_ids_returns_grouped() {
        let conn = setup_test_db();

        let root1 = crate::repo::insert_test_root(&conn, "/source", "source", false);
        let root2 = crate::repo::insert_test_root(&conn, "/archive", "archive", false);

        // Two objects (different content)
        let obj1 = insert_object(&conn, "content_hash_1", false);
        let obj2 = insert_object(&conn, "content_hash_2", false);

        // obj1 has 2 sources (duplicates)
        let _src1a = insert_source(&conn, root1, "photo.jpg", Some(obj1), true, false);
        let _src1b = insert_source(&conn, root2, "photo.jpg", Some(obj1), true, false);

        // obj2 has 1 source
        let _src2 = insert_source(&conn, root1, "unique.jpg", Some(obj2), true, false);

        let result = fetch_sources_by_object_ids(&conn, &[obj1, obj2]).unwrap();

        // Should have 2 keys
        assert_eq!(result.len(), 2);

        // obj1 should have 2 sources
        assert_eq!(result.get(&obj1).map(|v| v.len()), Some(2));

        // obj2 should have 1 source
        assert_eq!(result.get(&obj2).map(|v| v.len()), Some(1));
    }

    #[test]
    fn fetch_sources_by_object_ids_includes_root_path() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/my/archive", "archive", false);
        let obj = insert_object(&conn, "test_hash", false);
        let _src = insert_source(&conn, root_id, "subdir/file.txt", Some(obj), true, false);

        let result = fetch_sources_by_object_ids(&conn, &[obj]).unwrap();
        let sources = result.get(&obj).unwrap();

        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].root_path, "/my/archive");
        assert_eq!(sources[0].rel_path, "subdir/file.txt");
        // Verify Source::path() works correctly
        assert_eq!(sources[0].path(), "/my/archive/subdir/file.txt");
    }

    #[test]
    fn fetch_sources_by_object_ids_excludes_non_present() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/source", "source", false);
        let obj = insert_object(&conn, "test_hash", false);

        // One present, one deleted
        let _present = insert_source(&conn, root_id, "present.jpg", Some(obj), true, false);
        let _deleted = insert_source(&conn, root_id, "deleted.jpg", Some(obj), false, false);

        let result = fetch_sources_by_object_ids(&conn, &[obj]).unwrap();
        let sources = result.get(&obj).unwrap();

        // Only the present source should be returned
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].rel_path, "present.jpg");
    }

    #[test]
    fn fetch_sources_by_object_ids_handles_large_batch() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/source", "source", false);

        // Create more than BATCH_SIZE objects (1000+)
        let mut object_ids = Vec::new();
        for i in 0..1050 {
            let obj = insert_object(&conn, &format!("hash_{}", i), false);
            insert_source(&conn, root_id, &format!("file_{}.jpg", i), Some(obj), true, false);
            object_ids.push(obj);
        }

        let result = fetch_sources_by_object_ids(&conn, &object_ids).unwrap();

        // Should have all 1050 objects
        assert_eq!(result.len(), 1050);

        // Verify samples from different batch chunks
        assert!(result.contains_key(&object_ids[0]));
        assert!(result.contains_key(&object_ids[500]));
        assert!(result.contains_key(&object_ids[1049]));
    }

    // =========================================================================
    // insert_destination tests
    // =========================================================================

    #[test]
    fn insert_destination_fresh_insert() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "abc123hash", false);

        let new = NewSource {
            root_id,
            rel_path: "2024/photo.jpg".to_string(),
            size: 1024,
            mtime: 1704067200,
            partial_hash: "partial123".to_string(),
            object_id: Some(obj_id),
            device: Some(65024),
            inode: Some(12345),
        };

        let source = insert_destination(&conn, &new).unwrap();

        // Verify returned Source has correct values
        assert_eq!(source.root_id, root_id);
        assert_eq!(source.rel_path, "2024/photo.jpg");
        assert_eq!(source.size, 1024);
        assert_eq!(source.mtime, 1704067200);
        assert_eq!(source.partial_hash, "partial123");
        assert_eq!(source.object_id, Some(obj_id));
        assert_eq!(source.device, 65024);
        assert_eq!(source.inode, 12345);
        // Fresh insert should have basis_rev = 0
        assert_eq!(source.basis_rev, 0);
        // Should not be excluded
        assert!(!source.excluded);
    }

    #[test]
    fn insert_destination_stale_record_update() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "abc123hash", false);

        // Insert a stale record (present=0) with basis_rev=5
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, object_id, size, mtime, partial_hash,
             basis_rev, scanned_at, last_seen_at, present, excluded, device, inode)
             VALUES (?, ?, ?, 500, 1700000000, 'oldhash', 5, 0, 0, 0, 1, 100, 200)",
            rusqlite::params![root_id, "revived.jpg", obj_id],
        ).unwrap();

        let new = NewSource {
            root_id,
            rel_path: "revived.jpg".to_string(),
            size: 2048,
            mtime: 1704067200,
            partial_hash: "newhash".to_string(),
            object_id: Some(obj_id),
            device: Some(65024),
            inode: Some(99999),
        };

        let source = insert_destination(&conn, &new).unwrap();

        // Verify stale record was updated, not inserted
        assert_eq!(source.rel_path, "revived.jpg");
        assert_eq!(source.size, 2048);
        assert_eq!(source.mtime, 1704067200);
        assert_eq!(source.partial_hash, "newhash");
        // basis_rev should be incremented from 5 to 6
        assert_eq!(source.basis_rev, 6);
        // device/inode should be updated
        assert_eq!(source.device, 65024);
        assert_eq!(source.inode, 99999);
        // excluded should be reset to false
        assert!(!source.excluded);

        // Verify only one record exists
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sources WHERE root_id = ? AND rel_path = ?",
                rusqlite::params![root_id, "revived.jpg"],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn insert_destination_null_device_inode() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "abc123hash", false);

        // Simulate non-Unix platform where device/inode are not available
        let new = NewSource {
            root_id,
            rel_path: "nonunix.jpg".to_string(),
            size: 1024,
            mtime: 1704067200,
            partial_hash: "partial123".to_string(),
            object_id: Some(obj_id),
            device: None,  // Not available
            inode: None,   // Not available
        };

        let source = insert_destination(&conn, &new).unwrap();

        // Should succeed with device/inode defaulting to 0
        assert_eq!(source.rel_path, "nonunix.jpg");
        assert_eq!(source.device, 0);
        assert_eq!(source.inode, 0);
        assert_eq!(source.size, 1024);
    }

    #[test]
    fn insert_destination_already_present_fails() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "abc123hash", false);

        // Insert an active record (present=1)
        insert_source(&conn, root_id, "existing.jpg", Some(obj_id), true, false);

        let new = NewSource {
            root_id,
            rel_path: "existing.jpg".to_string(),
            size: 2048,
            mtime: 1704067200,
            partial_hash: "newhash".to_string(),
            object_id: Some(obj_id),
            device: Some(65024),
            inode: Some(12345),
        };

        // Should fail due to UNIQUE constraint on (root_id, rel_path)
        let result = insert_destination(&conn, &new);
        assert!(result.is_err());

        // Verify the error mentions constraint violation
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("UNIQUE") || err_msg.contains("constraint"));
    }

    #[test]
    fn insert_destination_returns_complete_source() {
        // Verify the returned Source has all joined fields populated
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "abc123hash", true); // object is excluded

        let new = NewSource {
            root_id,
            rel_path: "complete.jpg".to_string(),
            size: 1024,
            mtime: 1704067200,
            partial_hash: "partial123".to_string(),
            object_id: Some(obj_id),
            device: Some(65024),
            inode: Some(12345),
        };

        let source = insert_destination(&conn, &new).unwrap();

        // Verify joined fields from roots table
        assert_eq!(source.root_path, "/archive");
        assert_eq!(source.root_role, "archive");
        assert!(!source.root_suspended);

        // Verify joined fields from objects table
        assert_eq!(source.object_id, Some(obj_id));
        assert_eq!(source.object_excluded, Some(true));

        // Verify domain predicate works with joined data
        assert!(source.is_excluded()); // object is excluded
        assert!(source.is_active());   // root is not suspended
        assert!(source.is_from_role("archive"));

        // Verify path() works
        assert_eq!(source.path(), "/archive/complete.jpg");
    }

    // =========================================================================
    // fetch_by_path tests
    // =========================================================================

    #[test]
    fn fetch_by_path_exists() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, "found.jpg", None, true, false);

        let result = fetch_by_path(&conn, root_id, "found.jpg").unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().rel_path, "found.jpg");
    }

    #[test]
    fn fetch_by_path_not_present() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, "deleted.jpg", None, false, false); // present=0

        let result = fetch_by_path(&conn, root_id, "deleted.jpg").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn fetch_by_path_not_found() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);

        let result = fetch_by_path(&conn, root_id, "nonexistent.jpg").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn fetch_by_path_wrong_root() {
        let conn = setup_test_db();

        let root1 = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        let root2 = crate::repo::insert_test_root(&conn, "/archive", "archive", false);
        insert_source(&conn, root1, "file.jpg", None, true, false);

        // File exists in root1, but we query root2
        let result = fetch_by_path(&conn, root2, "file.jpg").unwrap();
        assert!(result.is_none());
    }

    // =========================================================================
    // fetch_by_inode tests
    // =========================================================================

    #[test]
    fn fetch_by_inode_exists() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);

        // Insert source with specific device/inode
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, device, inode, size, mtime, partial_hash, scanned_at, last_seen_at, present)
             VALUES (?, 'file.jpg', 100, 12345, 1000, 1700000000, 'hash', 0, 0, 1)",
            rusqlite::params![root_id],
        ).unwrap();

        let result = fetch_by_inode(&conn, 100, 12345).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().rel_path, "file.jpg");
    }

    #[test]
    fn fetch_by_inode_cross_root() {
        let conn = setup_test_db();

        let root1 = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        let _root2 = crate::repo::insert_test_root(&conn, "/archive", "archive", false);

        // Insert source in root1 with specific device/inode
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, device, inode, size, mtime, partial_hash, scanned_at, last_seen_at, present)
             VALUES (?, 'original.jpg', 100, 12345, 1000, 1700000000, 'hash', 0, 0, 1)",
            rusqlite::params![root1],
        ).unwrap();

        // Should find it even though we're not specifying root
        let result = fetch_by_inode(&conn, 100, 12345).unwrap();
        assert!(result.is_some());
        let source = result.unwrap();
        assert_eq!(source.rel_path, "original.jpg");
        assert_eq!(source.root_id, root1);
    }

    #[test]
    fn fetch_by_inode_not_found() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, "file.jpg", None, true, false);

        // Query for non-existent device/inode
        let result = fetch_by_inode(&conn, 999, 999).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn fetch_by_inode_not_present() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);

        // Insert non-present source with specific device/inode
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, device, inode, size, mtime, partial_hash, scanned_at, last_seen_at, present)
             VALUES (?, 'deleted.jpg', 100, 12345, 1000, 1700000000, 'hash', 0, 0, 0)",
            rusqlite::params![root_id],
        ).unwrap();

        // Should not find it (present=0)
        let result = fetch_by_inode(&conn, 100, 12345).unwrap();
        assert!(result.is_none());
    }

    // =========================================================================
    // apply_reconciliation tests
    // =========================================================================

    #[test]
    fn apply_reconciliation_new() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);

        let observation = FileObservation {
            root_id,
            rel_path: "new_file.jpg".to_string(),
            device: 100,
            inode: 12345,
            size: 2048,
            mtime: 1700000000,
            partial_hash: Some("abc123".to_string()),
        };

        let reconciliation = Reconciliation::New;
        let now = 1700000001;

        let source = apply_reconciliation(&conn, &observation, &reconciliation, now).unwrap();

        assert_eq!(source.rel_path, "new_file.jpg");
        assert_eq!(source.size, 2048);
        assert_eq!(source.mtime, 1700000000);
        assert_eq!(source.device, 100);
        assert_eq!(source.inode, 12345);
        assert_eq!(source.partial_hash, "abc123");
        assert_eq!(source.basis_rev, 0);
    }

    #[test]
    fn apply_reconciliation_new_revives_stale_record() {
        // Test: New reconciliation at path where a stale (present=0) record exists
        // The stale record should be revived with new attributes
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);

        // Create a stale source at this path (present=0)
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, device, inode, size, mtime, partial_hash, basis_rev, scanned_at, last_seen_at, present, excluded)
             VALUES (?, 'revived.jpg', 1, 1, 500, 1600000000, 'oldhash', 5, 0, 0, 0, 0)",
            rusqlite::params![root_id],
        ).unwrap();
        let old_id = conn.last_insert_rowid();

        let observation = FileObservation {
            root_id,
            rel_path: "revived.jpg".to_string(),
            device: 100,
            inode: 12345,
            size: 2048,
            mtime: 1700000000,
            partial_hash: Some("newhash".to_string()),
        };

        let reconciliation = Reconciliation::New;
        let now = 1700000001;

        let source = apply_reconciliation(&conn, &observation, &reconciliation, now).unwrap();

        // Should revive the same record
        assert_eq!(source.id, old_id);
        assert_eq!(source.rel_path, "revived.jpg");
        // Should have new file's attributes
        assert_eq!(source.device, 100);
        assert_eq!(source.inode, 12345);
        assert_eq!(source.size, 2048);
        assert_eq!(source.mtime, 1700000000);
        assert_eq!(source.partial_hash, "newhash");
        // basis_rev should be reset to 0 (new file)
        assert_eq!(source.basis_rev, 0);
        // object_id should be cleared
        assert_eq!(source.object_id, None);

        // Verify only one record exists
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM sources WHERE root_id = ? AND rel_path = ?",
            rusqlite::params![root_id, "revived.jpg"],
            |r| r.get(0)
        ).unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn apply_reconciliation_unchanged() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        let source_id = insert_source(&conn, root_id, "existing.jpg", None, true, false);

        let observation = FileObservation {
            root_id,
            rel_path: "existing.jpg".to_string(),
            device: 0,
            inode: 0,
            size: 1000,
            mtime: 1704067200,
            partial_hash: None,
        };

        let reconciliation = Reconciliation::Unchanged { source_id };
        let now = 1700000001;

        let source = apply_reconciliation(&conn, &observation, &reconciliation, now).unwrap();

        assert_eq!(source.id, source_id);
        assert_eq!(source.rel_path, "existing.jpg");

        // Verify last_seen_at was updated
        let last_seen: i64 = conn.query_row(
            "SELECT last_seen_at FROM sources WHERE id = ?",
            rusqlite::params![source_id],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(last_seen, now);
    }

    #[test]
    fn apply_reconciliation_modified() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);

        // Insert existing source with basis_rev=2
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, device, inode, size, mtime, partial_hash, basis_rev, scanned_at, last_seen_at, present)
             VALUES (?, 'modified.jpg', 100, 12345, 1000, 1700000000, 'oldhash', 2, 0, 0, 1)",
            rusqlite::params![root_id],
        ).unwrap();
        let source_id = conn.last_insert_rowid();

        let observation = FileObservation {
            root_id,
            rel_path: "modified.jpg".to_string(),
            device: 100,
            inode: 12345,
            size: 2048,  // Changed
            mtime: 1700000100,  // Changed
            partial_hash: Some("newhash".to_string()),
        };

        let reconciliation = Reconciliation::Modified {
            source_id,
            old_object_id: None,
        };
        let now = 1700000101;

        let source = apply_reconciliation(&conn, &observation, &reconciliation, now).unwrap();

        assert_eq!(source.id, source_id);
        assert_eq!(source.size, 2048);
        assert_eq!(source.mtime, 1700000100);
        assert_eq!(source.partial_hash, "newhash");
        assert_eq!(source.basis_rev, 3);  // Incremented from 2
    }

    #[test]
    fn apply_reconciliation_moved() {
        let conn = setup_test_db();

        let root1 = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        let root2 = crate::repo::insert_test_root(&conn, "/archive", "archive", false);

        // Insert existing source in root1
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, device, inode, size, mtime, partial_hash, basis_rev, scanned_at, last_seen_at, present)
             VALUES (?, 'old_location.jpg', 100, 12345, 1000, 1700000000, 'hash123', 1, 0, 0, 1)",
            rusqlite::params![root1],
        ).unwrap();
        let source_id = conn.last_insert_rowid();

        // Observation at new location in root2
        let observation = FileObservation {
            root_id: root2,
            rel_path: "new_location.jpg".to_string(),
            device: 100,
            inode: 12345,
            size: 1000,
            mtime: 1700000000,
            partial_hash: None,
        };

        let reconciliation = Reconciliation::Moved {
            source_id,
            from_root_id: root1,
            from_path: "old_location.jpg".to_string(),
            old_object_id: None,
        };
        let now = 1700000001;

        let source = apply_reconciliation(&conn, &observation, &reconciliation, now).unwrap();

        assert_eq!(source.id, source_id);
        assert_eq!(source.root_id, root2);  // Moved to new root
        assert_eq!(source.rel_path, "new_location.jpg");  // New path
        assert_eq!(source.root_path, "/archive");  // Joined field updated
    }

    #[test]
    fn apply_reconciliation_new_requires_partial_hash() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);

        let observation = FileObservation {
            root_id,
            rel_path: "new_file.jpg".to_string(),
            device: 100,
            inode: 12345,
            size: 2048,
            mtime: 1700000000,
            partial_hash: None,  // Missing!
        };

        let reconciliation = Reconciliation::New;
        let now = 1700000001;

        let result = apply_reconciliation(&conn, &observation, &reconciliation, now);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("partial_hash"));
    }

    // =========================================================================
    // mark_missing tests
    // =========================================================================

    #[test]
    fn mark_missing_sets_present_zero() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        let id1 = insert_source(&conn, root_id, "missing1.jpg", None, true, false);
        let id2 = insert_source(&conn, root_id, "missing2.jpg", None, true, false);
        let _id3 = insert_source(&conn, root_id, "present.jpg", None, true, false);

        let now = 1700000001;
        let count = mark_missing(&conn, &[id1, id2], now).unwrap();

        assert_eq!(count, 2);

        // Verify they are now present=0
        let present1: i64 = conn.query_row(
            "SELECT present FROM sources WHERE id = ?",
            rusqlite::params![id1],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(present1, 0);

        // Verify present.jpg is still present=1
        let present3: i64 = conn.query_row(
            "SELECT present FROM sources WHERE rel_path = 'present.jpg'",
            [],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(present3, 1);
    }

    #[test]
    fn mark_missing_empty_list() {
        let conn = setup_test_db();
        let count = mark_missing(&conn, &[], 1700000001).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn mark_missing_returns_count() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        let id1 = insert_source(&conn, root_id, "file1.jpg", None, true, false);
        let id2 = insert_source(&conn, root_id, "file2.jpg", None, false, false);  // already not present

        // Only id1 should be updated (id2 is already present=0)
        let count = mark_missing(&conn, &[id1, id2], 1700000001).unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn mark_missing_updates_last_seen_at() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        let id1 = insert_source(&conn, root_id, "file.jpg", None, true, false);

        let now = 1700000001;
        mark_missing(&conn, &[id1], now).unwrap();

        let last_seen: i64 = conn.query_row(
            "SELECT last_seen_at FROM sources WHERE id = ?",
            rusqlite::params![id1],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(last_seen, now);
    }

    // =========================================================================
    // fetch_source_ids_for_root tests
    // =========================================================================

    #[test]
    fn fetch_source_ids_for_root_returns_present_only() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        let id1 = insert_source(&conn, root_id, "present1.jpg", None, true, false);
        let id2 = insert_source(&conn, root_id, "present2.jpg", None, true, false);
        let _id3 = insert_source(&conn, root_id, "deleted.jpg", None, false, false);

        let ids = fetch_source_ids_for_root(&conn, root_id, None).unwrap();

        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&id1));
        assert!(ids.contains(&id2));
    }

    #[test]
    fn fetch_source_ids_for_root_empty() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);

        let ids = fetch_source_ids_for_root(&conn, root_id, None).unwrap();
        assert!(ids.is_empty());
    }

    #[test]
    fn fetch_source_ids_for_root_with_prefix() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        let id1 = insert_source(&conn, root_id, "2024/photo1.jpg", None, true, false);
        let id2 = insert_source(&conn, root_id, "2024/photo2.jpg", None, true, false);
        let id3 = insert_source(&conn, root_id, "2023/old.jpg", None, true, false);
        let _id4 = insert_source(&conn, root_id, "2024/deleted.jpg", None, false, false);

        // With prefix, only 2024/* present sources
        let ids = fetch_source_ids_for_root(&conn, root_id, Some("2024/")).unwrap();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&id1));
        assert!(ids.contains(&id2));

        // Without prefix, all present sources
        let all_ids = fetch_source_ids_for_root(&conn, root_id, None).unwrap();
        assert_eq!(all_ids.len(), 3);
        assert!(all_ids.contains(&id3));
    }

    // =========================================================================
    // set_excluded tests
    // =========================================================================

    #[test]
    fn set_excluded_marks_source() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        let source_id = insert_source(&conn, root_id, "file.jpg", None, true, false);

        // Verify initially not excluded
        let excluded: i64 = conn.query_row(
            "SELECT excluded FROM sources WHERE id = ?",
            rusqlite::params![source_id],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(excluded, 0);

        // Set excluded
        set_excluded(&conn, source_id, true).unwrap();

        // Verify now excluded
        let excluded: i64 = conn.query_row(
            "SELECT excluded FROM sources WHERE id = ?",
            rusqlite::params![source_id],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(excluded, 1);
    }

    #[test]
    fn set_excluded_clears_source() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        let source_id = insert_source(&conn, root_id, "file.jpg", None, true, true); // starts excluded

        // Verify initially excluded
        let excluded: i64 = conn.query_row(
            "SELECT excluded FROM sources WHERE id = ?",
            rusqlite::params![source_id],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(excluded, 1);

        // Clear excluded
        set_excluded(&conn, source_id, false).unwrap();

        // Verify now not excluded
        let excluded: i64 = conn.query_row(
            "SELECT excluded FROM sources WHERE id = ?",
            rusqlite::params![source_id],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(excluded, 0);
    }

    #[test]
    fn set_excluded_nonexistent_source() {
        let conn = setup_test_db();

        // Should not error when source doesn't exist
        let result = set_excluded(&conn, 99999, true);
        assert!(result.is_ok());
    }

    // =========================================================================
    // batch_set_excluded tests
    // =========================================================================

    #[test]
    fn batch_set_excluded_empty_list() {
        let conn = setup_test_db();
        let count = batch_set_excluded(&conn, &[], true).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn batch_set_excluded_multiple() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        let id1 = insert_source(&conn, root_id, "file1.jpg", None, true, false);
        let id2 = insert_source(&conn, root_id, "file2.jpg", None, true, false);
        let id3 = insert_source(&conn, root_id, "file3.jpg", None, true, false);

        // Exclude id1 and id2, leave id3
        let count = batch_set_excluded(&conn, &[id1, id2], true).unwrap();
        assert_eq!(count, 2);

        // Verify exclusion state
        let excluded1: i64 = conn.query_row(
            "SELECT excluded FROM sources WHERE id = ?",
            rusqlite::params![id1],
            |row| row.get(0),
        ).unwrap();
        let excluded2: i64 = conn.query_row(
            "SELECT excluded FROM sources WHERE id = ?",
            rusqlite::params![id2],
            |row| row.get(0),
        ).unwrap();
        let excluded3: i64 = conn.query_row(
            "SELECT excluded FROM sources WHERE id = ?",
            rusqlite::params![id3],
            |row| row.get(0),
        ).unwrap();

        assert_eq!(excluded1, 1);
        assert_eq!(excluded2, 1);
        assert_eq!(excluded3, 0); // Not in the batch
    }

    #[test]
    fn batch_set_excluded_returns_count() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        let id1 = insert_source(&conn, root_id, "file1.jpg", None, true, false);
        let _id2 = insert_source(&conn, root_id, "file2.jpg", None, true, false);

        // Request update for id1 and a nonexistent id
        let count = batch_set_excluded(&conn, &[id1, 99999], true).unwrap();

        // Only id1 should be updated
        assert_eq!(count, 1);
    }

    #[test]
    fn batch_set_excluded_skips_nonexistent() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        let id1 = insert_source(&conn, root_id, "file.jpg", None, true, false);

        // Mix of existing and nonexistent IDs
        let count = batch_set_excluded(&conn, &[id1, 99998, 99999], true).unwrap();

        // Only the existing source should be updated
        assert_eq!(count, 1);

        // Verify it was actually updated
        let excluded: i64 = conn.query_row(
            "SELECT excluded FROM sources WHERE id = ?",
            rusqlite::params![id1],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(excluded, 1);
    }

    #[test]
    fn batch_set_excluded_handles_large_batch() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);

        // Create more than BATCH_SIZE sources (1000+)
        let mut source_ids = Vec::new();
        for i in 0..1050 {
            let id = insert_source(&conn, root_id, &format!("file_{}.jpg", i), None, true, false);
            source_ids.push(id);
        }

        // Exclude all of them
        let count = batch_set_excluded(&conn, &source_ids, true).unwrap();
        assert_eq!(count, 1050);

        // Verify a sample from each batch chunk
        let excluded_first: i64 = conn.query_row(
            "SELECT excluded FROM sources WHERE id = ?",
            rusqlite::params![source_ids[0]],
            |row| row.get(0),
        ).unwrap();
        let excluded_mid: i64 = conn.query_row(
            "SELECT excluded FROM sources WHERE id = ?",
            rusqlite::params![source_ids[500]],
            |row| row.get(0),
        ).unwrap();
        let excluded_last: i64 = conn.query_row(
            "SELECT excluded FROM sources WHERE id = ?",
            rusqlite::params![source_ids[1049]],
            |row| row.get(0),
        ).unwrap();

        assert_eq!(excluded_first, 1);
        assert_eq!(excluded_mid, 1);
        assert_eq!(excluded_last, 1);
    }

    // =========================================================================
    // batch_check_paths_exist tests
    // =========================================================================

    #[test]
    fn batch_check_paths_exist_empty_input() {
        let conn = setup_test_db();
        let _root_id = crate::repo::insert_test_root(&conn, "/archive", "archive", false);
        let result = batch_check_paths_exist(&conn, 1, &[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn batch_check_paths_exist_none_found() {
        let conn = setup_test_db();
        let root_id = crate::repo::insert_test_root(&conn, "/archive", "archive", false);

        // No sources exist, query for paths that don't exist
        let result = batch_check_paths_exist(&conn, root_id, &["a.jpg", "b.jpg"]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn batch_check_paths_exist_all_found() {
        let conn = setup_test_db();
        let root_id = crate::repo::insert_test_root(&conn, "/archive", "archive", false);

        insert_source(&conn, root_id, "a.jpg", None, true, false);
        insert_source(&conn, root_id, "b.jpg", None, true, false);

        let result = batch_check_paths_exist(&conn, root_id, &["a.jpg", "b.jpg"]).unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.contains("a.jpg"));
        assert!(result.contains("b.jpg"));
    }

    #[test]
    fn batch_check_paths_exist_mixed() {
        let conn = setup_test_db();
        let root_id = crate::repo::insert_test_root(&conn, "/archive", "archive", false);

        insert_source(&conn, root_id, "exists.jpg", None, true, false);
        // "missing.jpg" is not inserted

        let result = batch_check_paths_exist(&conn, root_id, &["exists.jpg", "missing.jpg"]).unwrap();
        assert_eq!(result.len(), 1);
        assert!(result.contains("exists.jpg"));
        assert!(!result.contains("missing.jpg"));
    }

    #[test]
    fn batch_check_paths_exist_ignores_not_present() {
        let conn = setup_test_db();
        let root_id = crate::repo::insert_test_root(&conn, "/archive", "archive", false);

        insert_source(&conn, root_id, "present.jpg", None, true, false);
        insert_source(&conn, root_id, "deleted.jpg", None, false, false); // present=0

        let result = batch_check_paths_exist(&conn, root_id, &["present.jpg", "deleted.jpg"]).unwrap();
        assert_eq!(result.len(), 1);
        assert!(result.contains("present.jpg"));
        assert!(!result.contains("deleted.jpg"));
    }

    #[test]
    fn batch_check_paths_exist_different_root() {
        let conn = setup_test_db();
        let root1 = crate::repo::insert_test_root(&conn, "/archive1", "archive", false);
        let root2 = crate::repo::insert_test_root(&conn, "/archive2", "archive", false);

        // Insert in root1
        insert_source(&conn, root1, "file.jpg", None, true, false);

        // Query against root2 - should not find it
        let result = batch_check_paths_exist(&conn, root2, &["file.jpg"]).unwrap();
        assert!(result.is_empty());

        // Query against root1 - should find it
        let result = batch_check_paths_exist(&conn, root1, &["file.jpg"]).unwrap();
        assert_eq!(result.len(), 1);
        assert!(result.contains("file.jpg"));
    }

    #[test]
    fn batch_check_paths_exist_handles_999_paths() {
        let conn = setup_test_db();
        let root_id = crate::repo::insert_test_root(&conn, "/archive", "archive", false);

        // Create 999 sources (just under BATCH_SIZE)
        let mut paths = Vec::new();
        for i in 0..999 {
            let path = format!("file_{}.jpg", i);
            insert_source(&conn, root_id, &path, None, true, false);
            paths.push(path);
        }

        let path_refs: Vec<&str> = paths.iter().map(|s| s.as_str()).collect();
        let result = batch_check_paths_exist(&conn, root_id, &path_refs).unwrap();

        assert_eq!(result.len(), 999);
    }

    #[test]
    fn batch_check_paths_exist_handles_1000_paths() {
        let conn = setup_test_db();
        let root_id = crate::repo::insert_test_root(&conn, "/archive", "archive", false);

        // Create exactly BATCH_SIZE sources
        let mut paths = Vec::new();
        for i in 0..1000 {
            let path = format!("file_{}.jpg", i);
            insert_source(&conn, root_id, &path, None, true, false);
            paths.push(path);
        }

        let path_refs: Vec<&str> = paths.iter().map(|s| s.as_str()).collect();
        let result = batch_check_paths_exist(&conn, root_id, &path_refs).unwrap();

        assert_eq!(result.len(), 1000);
    }

    #[test]
    fn batch_check_paths_exist_handles_1001_paths() {
        let conn = setup_test_db();
        let root_id = crate::repo::insert_test_root(&conn, "/archive", "archive", false);

        // Create more than BATCH_SIZE sources (requires 2 batches)
        let mut paths = Vec::new();
        for i in 0..1001 {
            let path = format!("file_{}.jpg", i);
            insert_source(&conn, root_id, &path, None, true, false);
            paths.push(path);
        }

        let path_refs: Vec<&str> = paths.iter().map(|s| s.as_str()).collect();
        let result = batch_check_paths_exist(&conn, root_id, &path_refs).unwrap();

        assert_eq!(result.len(), 1001);

        // Verify samples from both batches
        assert!(result.contains("file_0.jpg"));
        assert!(result.contains("file_999.jpg"));
        assert!(result.contains("file_1000.jpg"));
    }

}
