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
use crate::domain::source::{NewSource, Source};

/// Batch size for SQL IN clauses. Consistent across all repositories.
pub const BATCH_SIZE: usize = 1000;

/// The columns we SELECT for Source construction.
/// Kept as a constant to ensure consistency across fetch functions.
pub(crate) const SOURCE_COLUMNS: &str = r#"
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
    r.suspended as root_suspended,
    s.decision_id
"#;

/// The base FROM/JOIN clause for Source queries.
pub(crate) const SOURCE_FROM: &str = r#"
    FROM sources s
    JOIN roots r ON s.root_id = r.id
    LEFT JOIN objects o ON s.object_id = o.id
"#;

/// Construct a Source from a row. Column order must match SOURCE_COLUMNS.
pub(crate) fn source_from_row(row: &rusqlite::Row) -> rusqlite::Result<Source> {
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
        decision_id: row.get(15)?,
    })
}

/// Fetch all absent sources (tombstones, `present = 0`) for the given root
/// IDs — the mirror of [`batch_fetch_by_roots`]. The retirement account
/// partitions a root by presence: present rows through the existing fetch,
/// absent rows through this one.
pub fn fetch_absent_by_roots(conn: &Connection, root_ids: &[i64]) -> Result<Vec<Source>> {
    if root_ids.is_empty() {
        return Ok(Vec::new());
    }

    let mut sources = Vec::new();
    for chunk in root_ids.chunks(BATCH_SIZE) {
        let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
        let sql = format!(
            "SELECT {} {} WHERE s.present = 0 AND s.root_id IN ({})",
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

/// The earliest `scanned_at` across every row of a root, present and absent
/// alike — evidence of when the earliest surviving row was first indexed.
/// Data-level, so it reaches back before decision recording existed (a
/// scan *decision* date would claim only what the trail records).
/// `scanned_at` is set on `New` and preserved by every other reconciliation,
/// so this is an observation, never a guess — though a lower bound: a
/// replaced file resets its row's clock.
pub fn min_scanned_at_by_root(conn: &Connection, root_id: i64) -> Result<Option<i64>> {
    let min = conn.query_row(
        "SELECT MIN(scanned_at) FROM sources WHERE root_id = ?1",
        [root_id],
        |row| row.get(0),
    )?;
    Ok(min)
}

/// Count every source row for a root, present and absent alike. One half of
/// the retirement ceremony's world-moved re-check: computes over SQL exactly
/// what `readiness_lens` derived from the fetched rows, so equality with the
/// review-time snapshot means "same world".
pub fn count_all_by_root(conn: &Connection, root_id: i64) -> Result<i64> {
    let count = conn.query_row(
        "SELECT COUNT(*) FROM sources WHERE root_id = ?1",
        [root_id],
        |row| row.get(0),
    )?;
    Ok(count)
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
        "SELECT {SOURCE_COLUMNS} {SOURCE_FROM} WHERE s.present = 1 AND s.root_id = ? AND s.rel_path = ?",
    );

    let result = conn
        .query_row(&sql, rusqlite::params![root_id, rel_path], source_from_row)
        .optional()?;

    Ok(result)
}

/// Fetch a single source by its ID.
///
/// Returns the complete Source with all joined fields (root_path, root_role, etc.).
/// Returns None if the source doesn't exist or is not present.
///
/// This is useful for operations that have a source_id and need the full
/// Source data (e.g., import processing where source_id comes from worklist).
pub fn fetch_by_id(conn: &Connection, source_id: i64) -> Result<Option<Source>> {
    let sql = format!("SELECT {SOURCE_COLUMNS} {SOURCE_FROM} WHERE s.present = 1 AND s.id = ?",);

    let result = conn
        .query_row(&sql, rusqlite::params![source_id], source_from_row)
        .optional()?;

    Ok(result)
}

/// Fetch the current decision_id for a source at the given path.
///
/// Returns None if no present source exists at this path, or if its decision_id is NULL.
/// Used by apply before overwriting a destination to capture the provenance chain.
pub fn fetch_decision_id_at_path(
    conn: &Connection,
    root_id: i64,
    rel_path: &str,
) -> Result<Option<i64>> {
    conn.prepare_cached(
        "SELECT decision_id FROM sources WHERE root_id = ? AND rel_path = ? AND present = 1",
    )?
    .query_row(rusqlite::params![root_id, rel_path], |row| {
        row.get::<_, Option<i64>>(0)
    })
    .optional()
    .map(|opt| opt.flatten())
    .map_err(Into::into)
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
/// Check if any sources (current or historical) exist at or under a scope path.
/// Includes present=0 records — Canon once knew this place.
/// Returns true if at least one source record exists.
pub fn sources_exist_at_scope(conn: &Connection, root_id: i64, rel_path: &str) -> Result<bool> {
    let exists: bool = if rel_path.is_empty() {
        conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sources WHERE root_id = ?)",
            rusqlite::params![root_id],
            |row| row.get(0),
        )?
    } else {
        conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sources WHERE root_id = ? \
             AND (rel_path = ? OR rel_path LIKE ? || '/%'))",
            rusqlite::params![root_id, rel_path, rel_path],
            |row| row.get(0),
        )?
    };
    Ok(exists)
}

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
        let rows = stmt.query_map(params.as_slice(), |row| row.get::<_, String>(0))?;

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

    // First try to update any existing record at this path (present=0 or present=1).
    // This preserves the row and increments basis_rev to reflect new content at this path.
    // Handles both stale records (present=0) and active records from a scan (present=1).
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
            excluded = 0,
            decision_id = ?
         WHERE root_id = ? AND rel_path = ?",
        rusqlite::params![
            new.device,
            new.inode,
            new.size,
            new.mtime,
            new.partial_hash,
            new.object_id,
            now,
            now,
            new.decision_id,
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
                object_id, basis_rev, scanned_at, last_seen_at, present, excluded,
                decision_id
             ) VALUES (?, ?, COALESCE(?, 0), COALESCE(?, 0), ?, ?, ?, ?, 0, ?, ?, 1, 0, ?)",
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
                new.decision_id,
            ],
        )?;
    }

    // Fetch the complete Source record with all joined fields.
    // This ensures the returned Source accurately reflects database state.
    fetch_by_path(conn, new.root_id, &new.rel_path)?.ok_or_else(|| {
        anyhow::anyhow!(
            "Failed to fetch source after insert: root_id={}, rel_path={}",
            new.root_id,
            new.rel_path
        )
    })
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
/// - `decision_id`: The decision behind this `present → absent` (deletion) transition.
///   `Some` **sets** the column (the deletion is decision-linked); `None` **omits** the
///   column, preserving the existing value — mirroring the scan set/preserve rule
///   (`apply_reconciliation`). Callers that can't attribute the transition to a decision
///   (recording disabled, or a manual marking) pass `None` so an existing provenance link
///   is never clobbered to NULL.
///
/// # Returns
///
/// Count of sources that were marked as missing.
///
/// # Note
///
/// Sources already marked as not present (present=0) are not counted in the return value.
/// This function handles empty input gracefully (returns 0).
pub fn mark_missing(
    conn: &Connection,
    source_ids: &[i64],
    now: i64,
    decision_id: Option<i64>,
) -> Result<u64> {
    if source_ids.is_empty() {
        return Ok(0);
    }

    let mut total_updated = 0u64;

    for chunk in source_ids.chunks(BATCH_SIZE) {
        let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
        // Set decision_id only when Some; omit the column when None to preserve the
        // existing value (set/preserve, per the decision_id set/preserve rule).
        let sql = match decision_id {
            Some(_) => format!(
                "UPDATE sources SET present = 0, last_seen_at = ?, decision_id = ? \
                 WHERE present = 1 AND id IN ({})",
                placeholders.join(",")
            ),
            None => format!(
                "UPDATE sources SET present = 0, last_seen_at = ? \
                 WHERE present = 1 AND id IN ({})",
                placeholders.join(",")
            ),
        };

        // Build params: now (and decision_id when set) first, then all the IDs.
        let mut params: Vec<rusqlite::types::Value> = Vec::with_capacity(chunk.len() + 2);
        params.push(rusqlite::types::Value::from(now));
        if let Some(id) = decision_id {
            params.push(rusqlite::types::Value::from(id));
        }
        for &id in chunk {
            params.push(rusqlite::types::Value::from(id));
        }

        let updated = conn.execute(&sql, rusqlite::params_from_iter(params))?;
        total_updated += updated as u64;
    }

    Ok(total_updated)
}

/// Update a source's location (root and path) after a rename/move operation.
///
/// Used when a source file is relocated to an archive. Updates the root_id,
/// rel_path, and timestamps to reflect the new location.
///
/// # Arguments
/// * `conn` - Database connection
/// * `source_id` - ID of the source to update
/// * `new_root_id` - The new root (typically the archive root)
/// * `new_rel_path` - The new relative path within the root
/// * `now` - Timestamp to record
pub fn update_location(
    conn: &Connection,
    source_id: i64,
    new_root_id: i64,
    new_rel_path: &str,
    now: i64,
    decision_id: Option<i64>,
) -> Result<()> {
    conn.execute(
        "UPDATE sources SET root_id = ?, rel_path = ?, scanned_at = ?, last_seen_at = ?, decision_id = ?
         WHERE id = ?",
        rusqlite::params![new_root_id, new_rel_path, now, now, decision_id, source_id],
    )?;
    Ok(())
}

/// Set the object_id for a source after hashing.
///
/// Links a source to its content object after the file has been hashed.
pub fn set_object_id(conn: &Connection, source_id: i64, object_id: i64) -> Result<()> {
    conn.execute(
        "UPDATE sources SET object_id = ? WHERE id = ?",
        rusqlite::params![object_id, source_id],
    )?;
    Ok(())
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
    fn count_all_by_root_spans_both_presence_classes() {
        // The world-moved re-check counts what fetch_root_story fetched:
        // present + absent, this root only.
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        let other = crate::repo::insert_test_root(&conn, "/other", "source", false);
        insert_source(&conn, root_id, "present.jpg", None, true, false);
        insert_source(&conn, root_id, "deleted.jpg", None, false, false);
        insert_source(&conn, other, "elsewhere.jpg", None, true, false);

        assert_eq!(count_all_by_root(&conn, root_id).unwrap(), 2);
        assert_eq!(count_all_by_root(&conn, other).unwrap(), 1);
        assert_eq!(count_all_by_root(&conn, 999).unwrap(), 0);
    }

    #[test]
    fn min_scanned_at_by_root_ignores_other_roots() {
        let conn = setup_test_db();
        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        let other = crate::repo::insert_test_root(&conn, "/other", "source", false);
        let a = insert_source(&conn, root_id, "a.jpg", None, true, false);
        let b = insert_source(&conn, other, "b.jpg", None, true, false);
        conn.execute("UPDATE sources SET scanned_at = 200 WHERE id = ?", [a])
            .unwrap();
        conn.execute("UPDATE sources SET scanned_at = 50 WHERE id = ?", [b])
            .unwrap();

        assert_eq!(min_scanned_at_by_root(&conn, root_id).unwrap(), Some(200));
        assert_eq!(min_scanned_at_by_root(&conn, 999).unwrap(), None);
    }

    #[test]
    fn fetch_absent_by_roots_is_the_presence_mirror() {
        // The exact mirror of batch_fetch_by_roots_excludes_non_present:
        // same rows, the other presence class.
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, "present.jpg", None, true, false);
        insert_source(&conn, root_id, "deleted.jpg", None, false, false);

        let sources = fetch_absent_by_roots(&conn, &[root_id]).unwrap();
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].rel_path, "deleted.jpg");
        assert_eq!(sources[0].root_path, "/photos");
    }

    #[test]
    fn fetch_absent_by_roots_carries_stamp_and_exclusion_columns() {
        // The account classifies tombstones by their decision_id stamp; the
        // mapper must round-trip it (and empty ids stay cheap).
        let conn = setup_test_db();
        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        let id = insert_source(&conn, root_id, "gone.jpg", None, false, false);
        conn.execute("UPDATE sources SET decision_id = 42 WHERE id = ?", [id])
            .unwrap();

        assert!(fetch_absent_by_roots(&conn, &[]).unwrap().is_empty());
        let sources = fetch_absent_by_roots(&conn, &[root_id]).unwrap();
        assert_eq!(sources[0].decision_id, Some(42));
        assert!(!sources[0].excluded);
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

        let excluded = sources
            .iter()
            .find(|s| s.rel_path == "excluded.jpg")
            .unwrap();
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
    // fetch_by_id tests
    // =========================================================================

    #[test]
    fn fetch_by_id_returns_source() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        let obj_id = insert_object(&conn, "abc123", false);
        let source_id = insert_source(&conn, root_id, "photo.jpg", Some(obj_id), true, false);

        let result = fetch_by_id(&conn, source_id).unwrap();

        assert!(result.is_some());
        let source = result.unwrap();
        assert_eq!(source.id, source_id);
        assert_eq!(source.root_id, root_id);
        assert_eq!(source.root_path, "/photos");
        assert_eq!(source.rel_path, "photo.jpg");
        assert_eq!(source.object_id, Some(obj_id));
        assert_eq!(source.root_role, "source");
    }

    #[test]
    fn fetch_by_id_not_found() {
        let conn = setup_test_db();

        let result = fetch_by_id(&conn, 99999).unwrap();

        assert!(result.is_none());
    }

    #[test]
    fn fetch_by_id_excludes_non_present() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        let source_id = insert_source(&conn, root_id, "deleted.jpg", None, false, false); // present=false

        let result = fetch_by_id(&conn, source_id).unwrap();

        assert!(result.is_none());
    }

    #[test]
    fn fetch_by_id_includes_excluded_source() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        let source_id = insert_source(&conn, root_id, "excluded.jpg", None, true, true); // excluded=true

        let result = fetch_by_id(&conn, source_id).unwrap();

        assert!(result.is_some());
        let source = result.unwrap();
        assert!(source.excluded);
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
            let obj = insert_object(&conn, &format!("hash_{i}"), false);
            insert_source(
                &conn,
                root_id,
                &format!("file_{i}.jpg"),
                Some(obj),
                true,
                false,
            );
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
            decision_id: None,
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
        )
        .unwrap();

        let new = NewSource {
            root_id,
            rel_path: "revived.jpg".to_string(),
            size: 2048,
            mtime: 1704067200,
            partial_hash: "newhash".to_string(),
            object_id: Some(obj_id),
            device: Some(65024),
            inode: Some(99999),
            decision_id: None,
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
            device: None, // Not available
            inode: None,  // Not available
            decision_id: None,
        };

        let source = insert_destination(&conn, &new).unwrap();

        // Should succeed with device/inode defaulting to 0
        assert_eq!(source.rel_path, "nonunix.jpg");
        assert_eq!(source.device, 0);
        assert_eq!(source.inode, 0);
        assert_eq!(source.size, 1024);
    }

    #[test]
    fn insert_destination_update_active_record() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "abc123hash", false);

        // Insert an active record (present=1) — simulates a scan that ran between apply runs
        let existing_id = insert_source(&conn, root_id, "existing.jpg", Some(obj_id), true, false);

        let new = NewSource {
            root_id,
            rel_path: "existing.jpg".to_string(),
            size: 2048,
            mtime: 1704067200,
            partial_hash: "newhash".to_string(),
            object_id: Some(obj_id),
            device: Some(65024),
            inode: Some(12345),
            decision_id: None,
        };

        // Should succeed — UPDATE fires on the active record, no UNIQUE error
        let source = insert_destination(&conn, &new).unwrap();

        // Verify the active record was updated with new metadata
        assert_eq!(source.id, existing_id);
        assert_eq!(source.rel_path, "existing.jpg");
        assert_eq!(source.size, 2048);
        assert_eq!(source.mtime, 1704067200);
        assert_eq!(source.partial_hash, "newhash");
        assert_eq!(source.device, 65024);
        assert_eq!(source.inode, 12345);
        // basis_rev should be incremented
        assert!(source.basis_rev > 0);
        // excluded should be reset
        assert!(!source.excluded);

        // Verify only one record exists (no duplicate)
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sources WHERE root_id = ? AND rel_path = ?",
                rusqlite::params![root_id, "existing.jpg"],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn insert_destination_idempotent() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "abc123hash", false);

        let new = NewSource {
            root_id,
            rel_path: "idempotent.jpg".to_string(),
            size: 1024,
            mtime: 1704067200,
            partial_hash: "partial123".to_string(),
            object_id: Some(obj_id),
            device: Some(65024),
            inode: Some(12345),
            decision_id: None,
        };

        // First call — INSERT path
        let source1 = insert_destination(&conn, &new).unwrap();
        assert_eq!(source1.size, 1024);
        assert_eq!(source1.basis_rev, 0);

        // Second call — UPDATE path (same data)
        let source2 = insert_destination(&conn, &new).unwrap();
        assert_eq!(source2.size, 1024);
        // basis_rev increments because UPDATE always increments
        assert_eq!(source2.basis_rev, 1);
        assert_eq!(source2.id, source1.id);

        // Verify only one record exists
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sources WHERE root_id = ? AND rel_path = ?",
                rusqlite::params![root_id, "idempotent.jpg"],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
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
            decision_id: None,
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
        assert!(source.is_active()); // root is not suspended
        assert!(source.is_from_role("archive"));

        // Verify path() works
        assert_eq!(source.path(), "/archive/complete.jpg");
    }

    // =========================================================================
    // Phase 4: insert_destination decision_id tests
    // =========================================================================

    #[test]
    fn test_insert_destination_sets_decision_id() {
        let conn = setup_test_db();
        let root_id = crate::repo::insert_test_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "hash1", false);

        let new = NewSource {
            root_id,
            rel_path: "photo.jpg".to_string(),
            size: 1024,
            mtime: 1704067200,
            partial_hash: "partial".to_string(),
            object_id: Some(obj_id),
            device: Some(1),
            inode: Some(100),
            decision_id: Some(42),
        };

        let source = insert_destination(&conn, &new).unwrap();
        assert_eq!(source.decision_id, Some(42));

        // Verify it's in the DB
        let db_val: Option<i64> = conn
            .query_row(
                "SELECT decision_id FROM sources WHERE id = ?",
                [source.id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(db_val, Some(42));
    }

    #[test]
    fn test_insert_destination_updates_decision_id() {
        // Re-inserting the same path with a new decision_id overwrites it
        let conn = setup_test_db();
        let root_id = crate::repo::insert_test_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "hash2", false);

        let new = NewSource {
            root_id,
            rel_path: "photo.jpg".to_string(),
            size: 1024,
            mtime: 1704067200,
            partial_hash: "partial".to_string(),
            object_id: Some(obj_id),
            device: Some(1),
            inode: Some(100),
            decision_id: Some(10),
        };
        insert_destination(&conn, &new).unwrap();

        // Re-insert with a new decision_id
        let new2 = NewSource {
            decision_id: Some(20),
            ..new
        };
        let source = insert_destination(&conn, &new2).unwrap();
        assert_eq!(source.decision_id, Some(20));
    }

    #[test]
    fn test_insert_destination_null_decision_id() {
        let conn = setup_test_db();
        let root_id = crate::repo::insert_test_root(&conn, "/archive", "archive", false);
        let obj_id = insert_object(&conn, "hash3", false);

        let new = NewSource {
            root_id,
            rel_path: "photo.jpg".to_string(),
            size: 1024,
            mtime: 1704067200,
            partial_hash: "partial".to_string(),
            object_id: Some(obj_id),
            device: Some(1),
            inode: Some(100),
            decision_id: None,
        };

        let source = insert_destination(&conn, &new).unwrap();
        assert_eq!(source.decision_id, None);
    }

    // =========================================================================
    // decision_id schema tests
    // =========================================================================

    #[test]
    fn test_sources_decision_id_exists() {
        let conn = setup_test_db();
        let root_id = crate::repo::insert_test_root(&conn, "/archive", "archive", false);
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, device, inode, size, mtime, partial_hash,
             basis_rev, scanned_at, last_seen_at, present, excluded, decision_id)
             VALUES (?, 'photo.jpg', 0, 0, 1024, 0, 'hash', 0, 0, 0, 1, 0, 42)",
            rusqlite::params![root_id],
        )
        .unwrap();
        let decision_id: Option<i64> = conn
            .query_row(
                "SELECT decision_id FROM sources WHERE root_id = ? AND rel_path = 'photo.jpg'",
                [root_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(decision_id, Some(42));
    }

    #[test]
    fn test_sources_decision_id_nullable() {
        let conn = setup_test_db();
        let root_id = crate::repo::insert_test_root(&conn, "/archive", "archive", false);
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, device, inode, size, mtime, partial_hash,
             basis_rev, scanned_at, last_seen_at, present, excluded)
             VALUES (?, 'photo.jpg', 0, 0, 1024, 0, 'hash', 0, 0, 0, 1, 0)",
            rusqlite::params![root_id],
        )
        .unwrap();
        let decision_id: Option<i64> = conn
            .query_row(
                "SELECT decision_id FROM sources WHERE root_id = ? AND rel_path = 'photo.jpg'",
                [root_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(decision_id, None);
    }

    // =========================================================================
    // fetch_decision_id_at_path tests
    // =========================================================================

    #[test]
    fn fetch_decision_id_at_path_returns_value() {
        let conn = setup_test_db();
        let root_id = crate::repo::insert_test_root(&conn, "/archive", "archive", false);
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, device, inode, size, mtime, partial_hash,
             basis_rev, scanned_at, last_seen_at, present, excluded, decision_id)
             VALUES (?, 'photo.jpg', 0, 0, 1024, 0, 'hash', 0, 0, 0, 1, 0, 42)",
            rusqlite::params![root_id],
        )
        .unwrap();
        let result = fetch_decision_id_at_path(&conn, root_id, "photo.jpg").unwrap();
        assert_eq!(result, Some(42));
    }

    #[test]
    fn fetch_decision_id_at_path_null_returns_none() {
        let conn = setup_test_db();
        let root_id = crate::repo::insert_test_root(&conn, "/archive", "archive", false);
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, device, inode, size, mtime, partial_hash,
             basis_rev, scanned_at, last_seen_at, present, excluded)
             VALUES (?, 'photo.jpg', 0, 0, 1024, 0, 'hash', 0, 0, 0, 1, 0)",
            rusqlite::params![root_id],
        )
        .unwrap();
        let result = fetch_decision_id_at_path(&conn, root_id, "photo.jpg").unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn fetch_decision_id_at_path_missing_returns_none() {
        let conn = setup_test_db();
        let root_id = crate::repo::insert_test_root(&conn, "/archive", "archive", false);
        let result = fetch_decision_id_at_path(&conn, root_id, "nonexistent.jpg").unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn fetch_decision_id_at_path_not_present_returns_none() {
        let conn = setup_test_db();
        let root_id = crate::repo::insert_test_root(&conn, "/archive", "archive", false);
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, device, inode, size, mtime, partial_hash,
             basis_rev, scanned_at, last_seen_at, present, excluded, decision_id)
             VALUES (?, 'photo.jpg', 0, 0, 1024, 0, 'hash', 0, 0, 0, 0, 0, 99)",
            rusqlite::params![root_id],
        )
        .unwrap();
        // present = 0, should not be returned
        let result = fetch_decision_id_at_path(&conn, root_id, "photo.jpg").unwrap();
        assert_eq!(result, None);
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
        let count = mark_missing(&conn, &[id1, id2], now, None).unwrap();

        assert_eq!(count, 2);

        // Verify they are now present=0
        let present1: i64 = conn
            .query_row(
                "SELECT present FROM sources WHERE id = ?",
                rusqlite::params![id1],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(present1, 0);

        // Verify present.jpg is still present=1
        let present3: i64 = conn
            .query_row(
                "SELECT present FROM sources WHERE rel_path = 'present.jpg'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(present3, 1);
    }

    #[test]
    fn mark_missing_empty_list() {
        let conn = setup_test_db();
        let count = mark_missing(&conn, &[], 1700000001, None).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn mark_missing_returns_count() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        let id1 = insert_source(&conn, root_id, "file1.jpg", None, true, false);
        let id2 = insert_source(&conn, root_id, "file2.jpg", None, false, false); // already not present

        // Only id1 should be updated (id2 is already present=0)
        let count = mark_missing(&conn, &[id1, id2], 1700000001, None).unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn mark_missing_updates_last_seen_at() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        let id1 = insert_source(&conn, root_id, "file.jpg", None, true, false);

        let now = 1700000001;
        mark_missing(&conn, &[id1], now, None).unwrap();

        let last_seen: i64 = conn
            .query_row(
                "SELECT last_seen_at FROM sources WHERE id = ?",
                rusqlite::params![id1],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(last_seen, now);
    }

    #[test]
    fn mark_missing_sets_decision_id_when_some() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        let id1 = insert_source(&conn, root_id, "gone.jpg", None, true, false);

        // A decision row must exist (decision_id has no FK, but keep the test realistic).
        let count = mark_missing(&conn, &[id1], 1700000001, Some(77)).unwrap();
        assert_eq!(count, 1);

        let decision_id: Option<i64> = conn
            .query_row(
                "SELECT decision_id FROM sources WHERE id = ?",
                rusqlite::params![id1],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(decision_id, Some(77));
    }

    #[test]
    fn mark_missing_preserves_decision_id_when_none() {
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        let id1 = insert_source(&conn, root_id, "gone.jpg", None, true, false);

        // Seed an existing provenance link (e.g. a prior apply/exclude decision).
        conn.execute(
            "UPDATE sources SET decision_id = 42 WHERE id = ?",
            rusqlite::params![id1],
        )
        .unwrap();

        // None must OMIT the column, preserving the existing value (set/preserve rule).
        let count = mark_missing(&conn, &[id1], 1700000001, None).unwrap();
        assert_eq!(count, 1);

        let decision_id: Option<i64> = conn
            .query_row(
                "SELECT decision_id FROM sources WHERE id = ?",
                rusqlite::params![id1],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(decision_id, Some(42));
    }

    #[test]
    fn mark_missing_batches_beyond_variable_limit() {
        // Exercise the chunking path: more IDs than the SQLite variable limit (~32k).
        let conn = setup_test_db();

        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        let ids: Vec<i64> = (0..35_000)
            .map(|i| insert_source(&conn, root_id, &format!("f{i}.jpg"), None, true, false))
            .collect();

        let count = mark_missing(&conn, &ids, 1700000001, Some(9)).unwrap();
        assert_eq!(count, 35_000);

        // Spot-check a source in a later chunk got both present=0 and the decision_id.
        let (present, decision_id): (i64, Option<i64>) = conn
            .query_row(
                "SELECT present, decision_id FROM sources WHERE id = ?",
                rusqlite::params![ids[34_999]],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(present, 0);
        assert_eq!(decision_id, Some(9));
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

        let result =
            batch_check_paths_exist(&conn, root_id, &["exists.jpg", "missing.jpg"]).unwrap();
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

        let result =
            batch_check_paths_exist(&conn, root_id, &["present.jpg", "deleted.jpg"]).unwrap();
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
            let path = format!("file_{i}.jpg");
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
            let path = format!("file_{i}.jpg");
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
            let path = format!("file_{i}.jpg");
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

    // =========================================================================
    // update_location tests
    // =========================================================================

    #[test]
    fn update_location_updates_fields() {
        let conn = setup_test_db();

        let source_root = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        let archive_root = crate::repo::insert_test_root(&conn, "/archive", "archive", false);
        let source_id = insert_source(&conn, source_root, "original.jpg", None, true, false);

        let now = 1700000001i64;
        update_location(
            &conn,
            source_id,
            archive_root,
            "new/path.jpg",
            now,
            Some(55),
        )
        .unwrap();

        // Verify fields updated
        let (root_id, rel_path, scanned_at, last_seen_at, decision_id): (i64, String, i64, i64, Option<i64>) = conn
            .query_row(
                "SELECT root_id, rel_path, scanned_at, last_seen_at, decision_id FROM sources WHERE id = ?",
                rusqlite::params![source_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
            )
            .unwrap();

        assert_eq!(root_id, archive_root);
        assert_eq!(rel_path, "new/path.jpg");
        assert_eq!(scanned_at, now);
        assert_eq!(last_seen_at, now);
        assert_eq!(decision_id, Some(55));
    }

    #[test]
    fn update_location_nonexistent_source() {
        let conn = setup_test_db();
        let root_id = crate::repo::insert_test_root(&conn, "/archive", "archive", false);

        // Should not error when source doesn't exist (0 rows affected)
        let result = update_location(&conn, 99999, root_id, "path.jpg", 1700000001, None);
        assert!(result.is_ok());
    }

    // =========================================================================
    // set_object_id tests
    // =========================================================================

    #[test]
    fn set_object_id_links_source() {
        let conn = setup_test_db();
        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        let source_id = insert_source(&conn, root_id, "photo.jpg", None, true, false);
        let object_id = insert_object(&conn, "abc123", false);

        set_object_id(&conn, source_id, object_id).unwrap();

        // Verify source is linked to object
        let stored: i64 = conn
            .query_row(
                "SELECT object_id FROM sources WHERE id = ?",
                [source_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored, object_id);
    }

    #[test]
    fn set_object_id_nonexistent_source() {
        let conn = setup_test_db();
        let object_id = insert_object(&conn, "abc123", false);

        // Should not error when source doesn't exist
        let result = set_object_id(&conn, 99999, object_id);
        assert!(result.is_ok());
    }

    // =========================================================================
    // sources_exist_at_scope tests
    // =========================================================================

    #[test]
    fn sources_exist_at_scope_with_present() {
        let conn = setup_test_db();
        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        insert_test_source(&conn, root_id, "a/1.jpg", 1, 1, 1000, 100);

        assert!(sources_exist_at_scope(&conn, root_id, "a").unwrap());
    }

    #[test]
    fn sources_exist_at_scope_with_non_present() {
        let conn = setup_test_db();
        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        let source_id = insert_test_source(&conn, root_id, "a/1.jpg", 1, 1, 1000, 100);
        // Mark as not present
        conn.execute(
            "UPDATE sources SET present = 0 WHERE id = ?",
            rusqlite::params![source_id],
        )
        .unwrap();

        // Should still return true — Canon knew this place
        assert!(sources_exist_at_scope(&conn, root_id, "a").unwrap());
    }

    #[test]
    fn sources_exist_at_scope_no_sources() {
        let conn = setup_test_db();
        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);

        assert!(!sources_exist_at_scope(&conn, root_id, "nonexistent").unwrap());
    }

    #[test]
    fn sources_exist_at_scope_descendant() {
        let conn = setup_test_db();
        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        insert_test_source(&conn, root_id, "a/b/c/1.jpg", 1, 1, 1000, 100);

        // Scope "a" should find descendant at "a/b/c/1.jpg"
        assert!(sources_exist_at_scope(&conn, root_id, "a").unwrap());
    }

    #[test]
    fn sources_exist_at_scope_no_false_prefix() {
        let conn = setup_test_db();
        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        insert_test_source(&conn, root_id, "ab/1.jpg", 1, 1, 1000, 100);

        // Scope "a" should NOT match "ab/1.jpg"
        assert!(!sources_exist_at_scope(&conn, root_id, "a").unwrap());
    }

    #[test]
    fn sources_exist_at_scope_root_level() {
        let conn = setup_test_db();
        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        insert_test_source(&conn, root_id, "1.jpg", 1, 1, 1000, 100);

        // Empty rel_path = root level
        assert!(sources_exist_at_scope(&conn, root_id, "").unwrap());
    }

    #[test]
    fn sources_exist_at_scope_root_level_empty() {
        let conn = setup_test_db();
        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);

        // Root with no sources
        assert!(!sources_exist_at_scope(&conn, root_id, "").unwrap());
    }
}
