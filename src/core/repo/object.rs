//! Object repository — infrastructure layer for fetching objects.
//!
//! This module provides batch fetch functions for objects and archive detection.
//! Archive detection answers: "Is this content in any archive root?"
//!
//! ## Design Principles
//!
//! 1. **Simple SQL**: Queries do data access only, no business logic in WHERE clauses
//! 2. **Returns domain types**: Functions return `Object` structs, not raw rows
//! 3. **Batch operations**: All functions handle large ID sets via chunking
//!
//! ## Usage
//!
//! ```ignore
//! use crate::core::repo;
//!
//! // Fetch objects by ID
//! let objects = repo::object::batch_fetch_by_ids(conn, &object_ids)?;
//!
//! // Check which objects are in any archive
//! let archived = repo::object::batch_check_archived(conn, &object_ids, None)?;
//!
//! // Check which objects are in a specific archive
//! let in_archive = repo::object::batch_check_archived(conn, &object_ids, Some(archive_root_id))?;
//!
//! // Get archive paths for objects (by object_id)
//! let paths = repo::object::batch_find_archive_paths(conn, &object_ids)?;
//!
//! // Get archive info by content hash (for manifest workflows)
//! let info = repo::object::batch_find_archive_info_by_hash(conn, &["abc123", "def456"])?;
//! ```

use std::collections::{HashMap, HashSet};

use anyhow::Result;
use rusqlite::OptionalExtension;

use super::db::Connection;
use crate::core::domain::object::{ArchiveCopy, ArchiveLocation, Object};

/// Batch size for SQL IN clauses (consistent with other repos).
const BATCH_SIZE: usize = 1000;

/// The archived-ness WHERE fragment: a source counts as an archive-side copy
/// of its object when its root is archive-role, the source is present, and
/// it carries content. Bound-value precedent applied from the path law's
/// [`super::db::path_at_or_under_sql`] — one string spoken once and bound at
/// every site that asks "is this an archive copy", rather than four
/// hand-synchronized spellings held together by cross-referencing comments.
/// Assumes `sources` aliased `s` and `roots` aliased `r`, joined on
/// `s.root_id = r.id`.
///
/// `s.size > 0` is the contentless law (`Source::is_contentless`): an empty
/// archive copy is no coverage evidence — the universal empty object would
/// hollowly "cover" every empty file in the universe, so identity claims
/// about empty content are vacuous and never count as coverage.
fn archive_copy_sql() -> &'static str {
    "r.role = 'archive' AND s.present = 1 AND s.size > 0"
}

/// The columns we SELECT for Object construction.
///
/// `pub(crate)`, not private: `exclude::repo::object::fetch_excluded` reuses
/// it rather than duplicating the column list.
pub(crate) const OBJECT_COLUMNS: &str = "id, hash_type, hash_value, excluded";

/// Construct an Object from a row. Column order must match OBJECT_COLUMNS.
///
/// `pub(crate)` for the same reason as `OBJECT_COLUMNS` above.
pub(crate) fn object_from_row(row: &rusqlite::Row) -> rusqlite::Result<Object> {
    Ok(Object {
        id: row.get(0)?,
        hash_type: row.get(1)?,
        hash_value: row.get(2)?,
        excluded: row.get(3)?,
    })
}

/// Fetch objects by their IDs.
///
/// Returns HashMap for O(1) lookup. Missing IDs are not included in the result.
pub fn batch_fetch_by_ids(conn: &Connection, object_ids: &[i64]) -> Result<HashMap<i64, Object>> {
    if object_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let mut result = HashMap::with_capacity(object_ids.len());

    for chunk in object_ids.chunks(BATCH_SIZE) {
        let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
        let sql = format!(
            "SELECT {} FROM objects WHERE id IN ({})",
            OBJECT_COLUMNS,
            placeholders.join(",")
        );

        let params: Vec<rusqlite::types::Value> = chunk
            .iter()
            .map(|&id| rusqlite::types::Value::from(id))
            .collect();

        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(params), object_from_row)?;

        for row in rows {
            let obj = row?;
            result.insert(obj.id, obj);
        }
    }

    Ok(result)
}

/// Fetch an object by its hash value.
///
/// # Returns
/// - `Ok(Some(Object))` if found
/// - `Ok(None)` if no object with that hash exists
///
/// This is a single-row lookup, not a batch operation.
pub fn fetch_by_hash(conn: &Connection, hash: &str) -> Result<Option<Object>> {
    let sql = format!("SELECT {OBJECT_COLUMNS} FROM objects WHERE hash_value = ?");

    let result = conn.query_row(&sql, [hash], object_from_row).optional()?;

    Ok(result)
}

/// Every archive-side copy of these objects, with the standing of the root
/// each stands on.
///
/// An object is "archived" if a source holding it stands under a root with
/// `role='archive'` and `present=1` and carries content — [`archive_copy_sql`]
/// is the predicate, here and at every other archived-ness query in this
/// module. The suspension of that root is **returned, never filtered**: a
/// closed door is the user's own act of setting a place aside from their
/// attention, and content does not stop standing where it stands because
/// attention moved. Filtering here would make every coverage claim in Canon
/// conditional on which roots happen to be parked today — which is the
/// inversion this projection exists to make unwritable. Surfaces state the
/// standing; nothing here decides it.
///
/// If `archive_root_id` is Some, only that archive is read.
///
/// **Important**: Callers must filter out sources with object_id=None before
/// calling this function. Only valid object IDs should be passed.
pub fn batch_archive_copies(
    conn: &Connection,
    object_ids: &[i64],
    archive_root_id: Option<i64>,
) -> Result<HashMap<i64, Vec<ArchiveCopy>>> {
    if object_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let mut result: HashMap<i64, Vec<ArchiveCopy>> = HashMap::new();

    for chunk in object_ids.chunks(BATCH_SIZE) {
        let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();

        let (sql, params): (String, Vec<rusqlite::types::Value>) =
            if let Some(root_id) = archive_root_id {
                // Specific archive root. The role is not re-checked: the id
                // came from a spec the caller already resolved against it.
                let sql = format!(
                    "SELECT DISTINCT s.object_id, r.id, r.suspended
                 FROM sources s
                 JOIN roots r ON s.root_id = r.id
                 WHERE s.root_id = ? AND s.present = 1 AND s.size > 0
                   AND s.object_id IN ({})",
                    placeholders.join(",")
                );
                let mut params = vec![rusqlite::types::Value::from(root_id)];
                params.extend(chunk.iter().map(|&id| rusqlite::types::Value::from(id)));
                (sql, params)
            } else {
                let sql = format!(
                    "SELECT DISTINCT s.object_id, r.id, r.suspended
                 FROM sources s
                 JOIN roots r ON s.root_id = r.id
                 WHERE {} AND s.object_id IN ({})",
                    archive_copy_sql(),
                    placeholders.join(",")
                );
                let params: Vec<rusqlite::types::Value> = chunk
                    .iter()
                    .map(|&id| rusqlite::types::Value::from(id))
                    .collect();
                (sql, params)
            };

        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(params), |row| {
            Ok((
                row.get::<_, i64>(0)?,
                ArchiveCopy {
                    root_id: row.get(1)?,
                    root_suspended: row.get(2)?,
                },
            ))
        })?;

        for row in rows {
            let (object_id, copy) = row?;
            result.entry(object_id).or_default().push(copy);
        }
    }

    Ok(result)
}

/// Check which objects have copies in archive root(s).
///
/// The key set of [`batch_archive_copies`] — one spelling of the
/// archived-ness WHERE, so a caller that only asks *whether* and a caller
/// that asks *where, and on what standing* can never come apart.
/// Pinned by `batch_check_archived_is_the_key_set_of_the_copies`.
///
/// **Important**: Callers must filter out sources with object_id=None before
/// calling this function. Only valid object IDs should be passed.
pub fn batch_check_archived(
    conn: &Connection,
    object_ids: &[i64],
    archive_root_id: Option<i64>,
) -> Result<HashSet<i64>> {
    Ok(batch_archive_copies(conn, object_ids, archive_root_id)?
        .into_keys()
        .collect())
}

/// Which of these objects were archived *from* the given root: an archive
/// copy of the object stands ([`archive_copy_sql`]) stamped by a decision
/// whose extraction rows draw from `origin_root_id`. Object-grain,
/// like coverage itself — the standing split's evidence. DB projections
/// only: apply stamps the destination rows (never the origin root's), so the
/// archive side carries the apply id and the extraction ledger names its
/// origin — no receipt is read.
///
/// **Important**: Callers must filter out sources with object_id=None before
/// calling this function. Only valid object IDs should be passed.
pub fn batch_check_archived_from_root(
    conn: &Connection,
    object_ids: &[i64],
    origin_root_id: i64,
) -> Result<HashSet<i64>> {
    if object_ids.is_empty() {
        return Ok(HashSet::new());
    }

    let mut result = HashSet::new();

    for chunk in object_ids.chunks(BATCH_SIZE) {
        let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
        let sql = format!(
            "SELECT DISTINCT s.object_id
             FROM sources s
             JOIN roots r ON s.root_id = r.id
             JOIN decision_extractions de ON de.decision_id = s.decision_id
             WHERE {} AND de.root_id = ?
               AND s.object_id IN ({})",
            archive_copy_sql(),
            placeholders.join(",")
        );
        let mut params = vec![rusqlite::types::Value::from(origin_root_id)];
        params.extend(chunk.iter().map(|&id| rusqlite::types::Value::from(id)));

        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(params), |row| {
            row.get::<_, i64>(0)
        })?;

        for row in rows {
            result.insert(row?);
        }
    }

    Ok(result)
}

/// Find every archive-side location of these objects.
///
/// Returns map from object_id to the archive locations holding that content,
/// each carrying the standing of the root it stands on ([`ArchiveLocation`]).
/// Objects with no archive copies are absent from the map. A caller wanting
/// bare paths maps over `.path`; a caller that marks its citations reads
/// `.root.root_suspended` beside it — which is the whole reason the standing
/// travels here rather than being looked up again per surface.
///
/// Suspension is **returned, never filtered**, for the reason
/// [`batch_archive_copies`] states. Archived-ness is [`archive_copy_sql`],
/// so the universal empty object claims no locations, and coverage-location
/// answers (cluster's skip set, the story's covered-where, the compile's
/// locations) never see it.
///
/// **Important**: Callers must filter out sources with object_id=None before
/// calling this function. Only valid object IDs should be passed.
pub fn batch_find_archive_paths(
    conn: &Connection,
    object_ids: &[i64],
) -> Result<HashMap<i64, Vec<ArchiveLocation>>> {
    if object_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let mut result: HashMap<i64, Vec<ArchiveLocation>> = HashMap::new();

    for chunk in object_ids.chunks(BATCH_SIZE) {
        let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
        let sql = format!(
            "SELECT s.object_id, r.path, s.rel_path, r.id, r.suspended
             FROM sources s
             JOIN roots r ON s.root_id = r.id
             WHERE {} AND s.object_id IN ({})
             ORDER BY s.object_id, r.path, s.rel_path",
            archive_copy_sql(),
            placeholders.join(",")
        );

        let params: Vec<rusqlite::types::Value> = chunk
            .iter()
            .map(|&id| rusqlite::types::Value::from(id))
            .collect();

        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(params), |row| {
            let object_id: i64 = row.get(0)?;
            let root_path: String = row.get(1)?;
            let rel_path: String = row.get(2)?;
            let root = ArchiveCopy {
                root_id: row.get(3)?,
                root_suspended: row.get(4)?,
            };
            Ok((object_id, root_path, rel_path, root))
        })?;

        for row in rows {
            let (object_id, root_path, rel_path, root) = row?;
            let path = if rel_path.is_empty() {
                root_path
            } else {
                format!("{root_path}/{rel_path}")
            };
            result
                .entry(object_id)
                .or_default()
                .push(ArchiveLocation { path, root });
        }
    }

    Ok(result)
}

/// Find archive info for objects identified by content hash.
///
/// This function is designed for manifest workflows where `hash_value` is the
/// content identifier (see D3 in write-infrastructure spec). It returns archive
/// location information needed for conflict detection.
///
/// # Behavior
/// - Looks up objects by hash_value (sha256), finds all archive copies
/// - Returns the archive root_id along with the full path
/// - Archived-ness is [`archive_copy_sql`] — present, archive-role roots,
///   content-bearing (the contentless law: an empty file "conflicting" with
///   every empty file already in the archive would be a vacuous claim)
/// - Handles large inputs via chunking (BATCH_SIZE)
///
/// # Returns
/// Map from hash_value to list of (archive_root_id, full_path) tuples.
/// Hashes not found in any archive are not included in the result.
/// Results are ordered by archive root_id, then rel_path within each hash.
///
/// # Caller Responsibilities
/// - Filter out sources without hash values before calling
/// - Use the archive_root_id to distinguish destination archive from others
pub fn batch_find_archive_info_by_hash(
    conn: &Connection,
    hash_values: &[&str],
) -> Result<HashMap<String, Vec<(i64, String)>>> {
    if hash_values.is_empty() {
        return Ok(HashMap::new());
    }

    let mut result: HashMap<String, Vec<(i64, String)>> = HashMap::new();

    for chunk in hash_values.chunks(BATCH_SIZE) {
        let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
        let sql = format!(
            "SELECT o.hash_value, r.id, r.path, s.rel_path
             FROM sources s
             JOIN roots r ON s.root_id = r.id
             JOIN objects o ON s.object_id = o.id
             WHERE {} AND o.hash_value IN ({})
             ORDER BY o.hash_value, r.id, s.rel_path",
            archive_copy_sql(),
            placeholders.join(",")
        );

        let params: Vec<rusqlite::types::Value> = chunk
            .iter()
            .map(|&h| rusqlite::types::Value::from(h.to_string()))
            .collect();

        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(params), |row| {
            let hash_value: String = row.get(0)?;
            let archive_root_id: i64 = row.get(1)?;
            let root_path: String = row.get(2)?;
            let rel_path: String = row.get(3)?;
            Ok((hash_value, archive_root_id, root_path, rel_path))
        })?;

        for row in rows {
            let (hash_value, archive_root_id, root_path, rel_path) = row?;
            let full_path = if rel_path.is_empty() {
                root_path
            } else {
                format!("{root_path}/{rel_path}")
            };
            result
                .entry(hash_value)
                .or_default()
                .push((archive_root_id, full_path));
        }
    }

    Ok(result)
}

// ============================================================================
// Orphaned object management
// ============================================================================

/// Statistics about orphaned objects and their associated data.
///
/// An object is considered orphaned when no source with `present = 1` references it.
#[derive(Debug, Clone, Default)]
pub struct OrphanedStats {
    /// Number of orphaned objects
    pub object_count: i64,
    /// Number of non-present sources referencing orphaned objects
    pub source_count: i64,
    /// Number of facts stored on those sources
    pub stored_on_source_count: i64,
    /// Number of facts stored on orphaned objects
    pub stored_on_content_count: i64,
}

impl OrphanedStats {
    /// Total number of facts (source + object)
    pub fn total_fact_count(&self) -> i64 {
        self.stored_on_source_count + self.stored_on_content_count
    }
}

/// Find statistics about orphaned objects (objects with no present sources).
///
/// Returns counts of orphaned objects, their non-present sources, and associated facts.
/// Use this for dry-run reporting before calling `delete_orphaned()`.
pub fn find_orphaned_stats(conn: &Connection) -> Result<OrphanedStats> {
    // Count orphaned objects
    let object_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM objects o
         WHERE NOT EXISTS (
             SELECT 1 FROM sources s
             WHERE s.object_id = o.id AND s.present = 1
         )",
        [],
        |row| row.get(0),
    )?;

    if object_count == 0 {
        return Ok(OrphanedStats::default());
    }

    // Count non-present sources referencing orphaned objects
    let source_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM sources s
         WHERE s.present = 0
           AND s.object_id IN (
               SELECT o.id FROM objects o
               WHERE NOT EXISTS (
                   SELECT 1 FROM sources s2
                   WHERE s2.object_id = o.id AND s2.present = 1
               )
           )",
        [],
        |row| row.get(0),
    )?;

    // Count facts stored on those sources
    let stored_on_source_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM facts f
         WHERE f.entity_type = 'source'
           AND f.entity_id IN (
               SELECT s.id FROM sources s
               WHERE s.present = 0
                 AND s.object_id IN (
                     SELECT o.id FROM objects o
                     WHERE NOT EXISTS (
                         SELECT 1 FROM sources s2
                         WHERE s2.object_id = o.id AND s2.present = 1
                     )
                 )
           )",
        [],
        |row| row.get(0),
    )?;

    // Count facts stored on orphaned objects
    let stored_on_content_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM facts f
         WHERE f.entity_type = 'object'
           AND f.entity_id IN (
               SELECT o.id FROM objects o
               WHERE NOT EXISTS (
                   SELECT 1 FROM sources s
                   WHERE s.object_id = o.id AND s.present = 1
               )
           )",
        [],
        |row| row.get(0),
    )?;

    Ok(OrphanedStats {
        object_count,
        source_count,
        stored_on_source_count,
        stored_on_content_count,
    })
}

/// Delete orphaned objects and all associated data.
///
/// Deletes in cascade order:
/// 1. Facts stored on non-present sources of orphaned objects
/// 2. Non-present sources referencing orphaned objects
/// 3. Facts stored on orphaned objects
/// 4. Orphaned objects
///
/// Returns actual counts deleted.
///
/// **IMPORTANT**: This function should be called within a transaction for atomicity.
/// The caller is responsible for transaction management:
///
/// ```ignore
/// let tx = conn.transaction()?;
/// let stats = core::repo::object::delete_orphaned(&tx)?;
/// tx.commit()?;
/// ```
pub fn delete_orphaned(conn: &Connection) -> Result<OrphanedStats> {
    // Delete facts stored on those sources first
    let stored_on_source_count = conn.execute(
        "DELETE FROM facts
         WHERE entity_type = 'source'
           AND entity_id IN (
               SELECT s.id FROM sources s
               WHERE s.present = 0
                 AND s.object_id IN (
                     SELECT o.id FROM objects o
                     WHERE NOT EXISTS (
                         SELECT 1 FROM sources s2
                         WHERE s2.object_id = o.id AND s2.present = 1
                     )
                 )
           )",
        [],
    )?;

    // Delete non-present sources referencing orphaned objects
    let source_count = conn.execute(
        "DELETE FROM sources
         WHERE present = 0
           AND object_id IN (
               SELECT o.id FROM objects o
               WHERE NOT EXISTS (
                   SELECT 1 FROM sources s
                   WHERE s.object_id = o.id AND s.present = 1
               )
           )",
        [],
    )?;

    // Delete facts stored on orphaned objects
    let stored_on_content_count = conn.execute(
        "DELETE FROM facts
         WHERE entity_type = 'object'
           AND entity_id IN (
               SELECT o.id FROM objects o
               WHERE NOT EXISTS (
                   SELECT 1 FROM sources s
                   WHERE s.object_id = o.id AND s.present = 1
               )
           )",
        [],
    )?;

    // Delete orphaned objects
    let object_count = conn.execute(
        "DELETE FROM objects
         WHERE NOT EXISTS (
             SELECT 1 FROM sources s
             WHERE s.object_id = objects.id AND s.present = 1
         )",
        [],
    )?;

    Ok(OrphanedStats {
        object_count: object_count as i64,
        source_count: source_count as i64,
        stored_on_source_count: stored_on_source_count as i64,
        stored_on_content_count: stored_on_content_count as i64,
    })
}

/// Get or create an object by hash, returning the complete Object.
///
/// This is an idempotent, concurrent-safe operation: if an object with the
/// given hash exists, it is returned; otherwise a new object is created and
/// returned. Uses `INSERT ON CONFLICT DO NOTHING` to handle race conditions
/// where two processes try to create the same hash simultaneously.
///
/// # Arguments
/// * `conn` - Database connection
/// * `hash_type` - Type of hash (e.g., "sha256")
/// * `hash_value` - The hash value
///
/// # Returns
/// The existing or newly created Object with all fields populated.
pub fn get_or_create(conn: &Connection, hash_type: &str, hash_value: &str) -> Result<Object> {
    Ok(get_or_create_reporting_creation(conn, hash_type, hash_value)?.0)
}

/// [`get_or_create`], also saying whether this call is the one that created the
/// row: `true` means content new to the universe, `false` that the hash was
/// already known and this caller merely found it. A caller counting creations
/// must read this rather than its own subject's change of link — many sources
/// share one object, and each of them linking is one creation, not several.
pub fn get_or_create_reporting_creation(
    conn: &Connection,
    hash_type: &str,
    hash_value: &str,
) -> Result<(Object, bool)> {
    // Atomic upsert: INSERT if not exists, do nothing on conflict.
    // This handles race conditions where two processes try to create the same hash.
    let inserted = conn.execute(
        "INSERT INTO objects (hash_type, hash_value) VALUES (?, ?)
         ON CONFLICT(hash_type, hash_value) DO NOTHING",
        rusqlite::params![hash_type, hash_value],
    )?;

    // Now fetch the object (whether we just created it or it already existed)
    let sql =
        format!("SELECT {OBJECT_COLUMNS} FROM objects WHERE hash_type = ? AND hash_value = ?");
    let obj = conn.query_row(
        &sql,
        rusqlite::params![hash_type, hash_value],
        object_from_row,
    )?;
    Ok((obj, inserted == 1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::repo::open_in_memory_for_test;
    use rusqlite::Connection as RusqliteConnection;

    fn setup_test_db() -> RusqliteConnection {
        open_in_memory_for_test()
    }

    /// Insert a test root and return its ID.
    fn insert_root(conn: &RusqliteConnection, path: &str, role: &str) -> i64 {
        conn.execute(
            "INSERT INTO roots (path, role) VALUES (?, ?)",
            rusqlite::params![path, role],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    /// Insert a test object and return its ID.
    fn insert_object(conn: &RusqliteConnection, hash_value: &str, excluded: bool) -> i64 {
        conn.execute(
            "INSERT INTO objects (hash_type, hash_value, excluded) VALUES ('sha256', ?, ?)",
            rusqlite::params![hash_value, excluded as i64],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    /// Insert a test root whose door the user has closed.
    fn insert_parked_root(conn: &RusqliteConnection, path: &str, role: &str) -> i64 {
        let id = insert_root(conn, path, role);
        conn.execute("UPDATE roots SET suspended = 1 WHERE id = ?", [id])
            .unwrap();
        id
    }

    /// Insert a test source and return its ID.
    fn insert_source(
        conn: &RusqliteConnection,
        root_id: i64,
        rel_path: &str,
        object_id: Option<i64>,
        present: bool,
    ) -> i64 {
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, object_id, present, size, mtime, partial_hash, scanned_at, last_seen_at, device, inode)
             VALUES (?, ?, ?, ?, 100, 0, '', 0, 0, 0, 0)",
            rusqlite::params![root_id, rel_path, object_id, present as i64],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    // =========================================================================
    // batch_fetch_by_ids tests
    // =========================================================================

    #[test]
    fn batch_fetch_by_ids_empty_returns_empty() {
        let conn = setup_test_db();
        let result = batch_fetch_by_ids(&conn, &[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn batch_fetch_by_ids_found() {
        let conn = setup_test_db();
        let obj_id = insert_object(&conn, "abc123", false);

        let result = batch_fetch_by_ids(&conn, &[obj_id]).unwrap();

        assert_eq!(result.len(), 1);
        let obj = result.get(&obj_id).unwrap();
        assert_eq!(obj.id, obj_id);
        assert_eq!(obj.hash_type, "sha256");
        assert_eq!(obj.hash_value, "abc123");
        assert!(!obj.excluded);
    }

    #[test]
    fn batch_fetch_by_ids_partial_missing_ids_ignored() {
        let conn = setup_test_db();
        let obj_id = insert_object(&conn, "abc123", false);

        // Query for existing and non-existing IDs
        let result = batch_fetch_by_ids(&conn, &[obj_id, 999, 1000]).unwrap();

        assert_eq!(result.len(), 1);
        assert!(result.contains_key(&obj_id));
        assert!(!result.contains_key(&999));
    }

    #[test]
    fn batch_fetch_by_ids_includes_excluded_objects() {
        let conn = setup_test_db();
        let obj_id = insert_object(&conn, "abc123", true);

        let result = batch_fetch_by_ids(&conn, &[obj_id]).unwrap();

        assert_eq!(result.len(), 1);
        let obj = result.get(&obj_id).unwrap();
        assert!(obj.is_excluded());
    }

    // =========================================================================
    // batch_check_archived_from_root tests
    // =========================================================================

    fn link_extraction(conn: &RusqliteConnection, decision_id: i64, origin_root_id: i64) {
        conn.execute(
            "INSERT INTO decision_extractions
             (decision_id, root_id, root_path, rel_prefix, files, bytes,
              destination_root_id, destination_path, disposition)
             VALUES (?, ?, '/origin', '', 1, 0, NULL, '/archive/x', 'retained')",
            rusqlite::params![decision_id, origin_root_id],
        )
        .unwrap();
    }

    fn stamp_source(conn: &RusqliteConnection, source_id: i64, decision_id: i64) {
        conn.execute(
            "UPDATE sources SET decision_id = ? WHERE id = ?",
            rusqlite::params![decision_id, source_id],
        )
        .unwrap();
    }

    #[test]
    fn archived_from_root_links_archive_stamp_to_origin_extraction() {
        // The standing split's evidence: the archive copy carries the apply
        // id, and the extraction ledger names that decision's origin root —
        // the join is DB projections only, no receipt read.
        let conn = setup_test_db();
        let origin_id = insert_root(&conn, "/origin", "source");
        let other_origin = insert_root(&conn, "/other", "source");
        let archive_id = insert_root(&conn, "/archive", "archive");

        // Object archived from /origin by decision 41.
        let from_here = insert_object(&conn, "aaa", false);
        let s1 = insert_source(&conn, archive_id, "a.jpg", Some(from_here), true);
        stamp_source(&conn, s1, 41);
        link_extraction(&conn, 41, origin_id);

        // Object archived from the OTHER root by decision 42.
        let from_elsewhere = insert_object(&conn, "bbb", false);
        let s2 = insert_source(&conn, archive_id, "b.jpg", Some(from_elsewhere), true);
        stamp_source(&conn, s2, 42);
        link_extraction(&conn, 42, other_origin);

        // Object in the archive with no extraction-linked stamp at all.
        let merely_covered = insert_object(&conn, "ccc", false);
        insert_source(&conn, archive_id, "c.jpg", Some(merely_covered), true);

        let result = batch_check_archived_from_root(
            &conn,
            &[from_here, from_elsewhere, merely_covered],
            origin_id,
        )
        .unwrap();
        assert!(result.contains(&from_here));
        assert!(
            !result.contains(&from_elsewhere),
            "another root's apply is not this root's deliberate act"
        );
        assert!(!result.contains(&merely_covered));
    }

    #[test]
    fn archived_from_root_requires_present_archive_copy() {
        // Live evidence only: an absent archive copy supports no standing.
        let conn = setup_test_db();
        let origin_id = insert_root(&conn, "/origin", "source");
        let archive_id = insert_root(&conn, "/archive", "archive");
        let obj = insert_object(&conn, "aaa", false);
        let s = insert_source(&conn, archive_id, "a.jpg", Some(obj), false);
        stamp_source(&conn, s, 41);
        link_extraction(&conn, 41, origin_id);

        let result = batch_check_archived_from_root(&conn, &[obj], origin_id).unwrap();
        assert!(result.is_empty());
    }

    // =========================================================================
    // batch_check_archived tests
    // =========================================================================

    #[test]
    fn batch_check_archived_empty_returns_empty() {
        let conn = setup_test_db();
        let result = batch_check_archived(&conn, &[], None).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn batch_check_archived_finds_archived_objects() {
        let conn = setup_test_db();

        // Setup: archive root with a source
        let archive_id = insert_root(&conn, "/archive", "archive");
        let obj_id = insert_object(&conn, "abc123", false);
        insert_source(&conn, archive_id, "file.jpg", Some(obj_id), true);

        let result = batch_check_archived(&conn, &[obj_id], None).unwrap();

        assert_eq!(result.len(), 1);
        assert!(result.contains(&obj_id));
    }

    #[test]
    fn batch_check_archived_excludes_non_archive_roots() {
        let conn = setup_test_db();

        // Setup: source root (not archive) with a source
        let source_root_id = insert_root(&conn, "/photos", "source");
        let obj_id = insert_object(&conn, "abc123", false);
        insert_source(&conn, source_root_id, "file.jpg", Some(obj_id), true);

        let result = batch_check_archived(&conn, &[obj_id], None).unwrap();

        // Should not find it because root is not an archive
        assert!(result.is_empty());
    }

    #[test]
    fn batch_check_archived_requires_present_source() {
        let conn = setup_test_db();

        // Setup: archive root with a non-present source
        let archive_id = insert_root(&conn, "/archive", "archive");
        let obj_id = insert_object(&conn, "abc123", false);
        insert_source(&conn, archive_id, "file.jpg", Some(obj_id), false); // present=false

        let result = batch_check_archived(&conn, &[obj_id], None).unwrap();

        // Should not find it because source is not present
        assert!(result.is_empty());
    }

    #[test]
    fn batch_check_archived_deduplicates_multiple_archive_sources() {
        let conn = setup_test_db();

        // Setup: same object in two different archives
        let archive1_id = insert_root(&conn, "/archive1", "archive");
        let archive2_id = insert_root(&conn, "/archive2", "archive");
        let obj_id = insert_object(&conn, "abc123", false);
        insert_source(&conn, archive1_id, "file1.jpg", Some(obj_id), true);
        insert_source(&conn, archive2_id, "file2.jpg", Some(obj_id), true);

        let result = batch_check_archived(&conn, &[obj_id], None).unwrap();

        // Should only contain the object_id once
        assert_eq!(result.len(), 1);
        assert!(result.contains(&obj_id));
    }

    #[test]
    fn batch_check_archived_specific_root_filters_correctly() {
        let conn = setup_test_db();

        // Setup: object in specific archive
        let archive1_id = insert_root(&conn, "/archive1", "archive");
        let _archive2_id = insert_root(&conn, "/archive2", "archive");
        let obj_id = insert_object(&conn, "abc123", false);
        insert_source(&conn, archive1_id, "file.jpg", Some(obj_id), true);

        // Check with specific archive root
        let result = batch_check_archived(&conn, &[obj_id], Some(archive1_id)).unwrap();

        assert_eq!(result.len(), 1);
        assert!(result.contains(&obj_id));
    }

    #[test]
    fn batch_check_archived_specific_root_ignores_other_archives() {
        let conn = setup_test_db();

        // Setup: object in archive1, but we query archive2
        let archive1_id = insert_root(&conn, "/archive1", "archive");
        let archive2_id = insert_root(&conn, "/archive2", "archive");
        let obj_id = insert_object(&conn, "abc123", false);
        insert_source(&conn, archive1_id, "file.jpg", Some(obj_id), true);

        // Check with different archive root
        let result = batch_check_archived(&conn, &[obj_id], Some(archive2_id)).unwrap();

        // Should not find it because object is in archive1, not archive2
        assert!(result.is_empty());
    }

    #[test]
    fn batch_check_archived_handles_large_id_sets() {
        let conn = setup_test_db();

        // Setup: one archive with many objects
        let archive_id = insert_root(&conn, "/archive", "archive");

        // Create more than BATCH_SIZE objects (1000+)
        let mut object_ids = Vec::new();
        for i in 0..1050 {
            let obj_id = insert_object(&conn, &format!("hash_{i}"), false);
            object_ids.push(obj_id);
            // Put every 10th object in archive
            if i % 10 == 0 {
                insert_source(
                    &conn,
                    archive_id,
                    &format!("file_{i}.jpg"),
                    Some(obj_id),
                    true,
                );
            }
        }

        let result = batch_check_archived(&conn, &object_ids, None).unwrap();

        // Should find 105 objects (every 10th from 0 to 1040)
        assert_eq!(result.len(), 105);
    }

    // =========================================================================
    // batch_find_archive_paths tests
    // =========================================================================

    #[test]
    fn batch_find_archive_paths_empty_returns_empty() {
        let conn = setup_test_db();
        let result = batch_find_archive_paths(&conn, &[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn batch_find_archive_paths_returns_correct_path_format() {
        let conn = setup_test_db();

        // Setup: archive with source
        let archive_id = insert_root(&conn, "/archive", "archive");
        let obj_id = insert_object(&conn, "abc123", false);
        insert_source(&conn, archive_id, "subdir/file.jpg", Some(obj_id), true);

        let result = batch_find_archive_paths(&conn, &[obj_id]).unwrap();

        assert_eq!(result.len(), 1);
        let paths = result.get(&obj_id).unwrap();
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].path, "/archive/subdir/file.jpg");
    }

    #[test]
    fn batch_find_archive_paths_empty_rel_path() {
        let conn = setup_test_db();

        // Setup: source at root of archive (empty rel_path)
        let archive_id = insert_root(&conn, "/archive", "archive");
        let obj_id = insert_object(&conn, "abc123", false);
        insert_source(&conn, archive_id, "", Some(obj_id), true);

        let result = batch_find_archive_paths(&conn, &[obj_id]).unwrap();

        let paths = result.get(&obj_id).unwrap();
        assert_eq!(paths[0].path, "/archive"); // No trailing slash
    }

    #[test]
    fn batch_find_archive_paths_multiple_paths_per_object() {
        let conn = setup_test_db();

        // Setup: same object in two archives
        let archive1_id = insert_root(&conn, "/archive1", "archive");
        let archive2_id = insert_root(&conn, "/archive2", "archive");
        let obj_id = insert_object(&conn, "abc123", false);
        insert_source(&conn, archive1_id, "file.jpg", Some(obj_id), true);
        insert_source(&conn, archive2_id, "copy.jpg", Some(obj_id), true);

        let result = batch_find_archive_paths(&conn, &[obj_id]).unwrap();

        assert_eq!(result.len(), 1);
        let paths = result.get(&obj_id).unwrap();
        assert_eq!(paths.len(), 2);
        // Ordered by root path, then rel_path
        assert_eq!(paths[0].path, "/archive1/file.jpg");
        assert_eq!(paths[1].path, "/archive2/copy.jpg");
    }

    #[test]
    fn batch_find_archive_paths_excludes_non_archive_roots() {
        let conn = setup_test_db();

        // Setup: source in non-archive root
        let source_root_id = insert_root(&conn, "/photos", "source");
        let obj_id = insert_object(&conn, "abc123", false);
        insert_source(&conn, source_root_id, "file.jpg", Some(obj_id), true);

        let result = batch_find_archive_paths(&conn, &[obj_id]).unwrap();

        // Should not include the source root path
        assert!(result.is_empty());
    }

    #[test]
    fn batch_find_archive_paths_excludes_non_present() {
        let conn = setup_test_db();

        // Setup: non-present source in archive
        let archive_id = insert_root(&conn, "/archive", "archive");
        let obj_id = insert_object(&conn, "abc123", false);
        insert_source(&conn, archive_id, "file.jpg", Some(obj_id), false); // present=false

        let result = batch_find_archive_paths(&conn, &[obj_id]).unwrap();

        assert!(result.is_empty());
    }

    // =========================================================================
    // batch_find_archive_info_by_hash tests
    // =========================================================================

    #[test]
    fn batch_find_archive_info_by_hash_empty_returns_empty() {
        let conn = setup_test_db();
        let result = batch_find_archive_info_by_hash(&conn, &[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn batch_find_archive_info_by_hash_single_hash_single_archive() {
        let conn = setup_test_db();

        // Setup: archive with source
        let archive_id = insert_root(&conn, "/archive", "archive");
        let obj_id = insert_object(&conn, "abc123", false);
        insert_source(&conn, archive_id, "subdir/file.jpg", Some(obj_id), true);

        let result = batch_find_archive_info_by_hash(&conn, &["abc123"]).unwrap();

        assert_eq!(result.len(), 1);
        let info = result.get("abc123").unwrap();
        assert_eq!(info.len(), 1);
        assert_eq!(info[0].0, archive_id); // archive_root_id
        assert_eq!(info[0].1, "/archive/subdir/file.jpg"); // full_path
    }

    #[test]
    fn batch_find_archive_info_by_hash_returns_archive_root_id() {
        let conn = setup_test_db();

        // Setup: two archives with different content
        let archive1_id = insert_root(&conn, "/archive1", "archive");
        let archive2_id = insert_root(&conn, "/archive2", "archive");
        let obj1_id = insert_object(&conn, "hash1", false);
        let obj2_id = insert_object(&conn, "hash2", false);
        insert_source(&conn, archive1_id, "file1.jpg", Some(obj1_id), true);
        insert_source(&conn, archive2_id, "file2.jpg", Some(obj2_id), true);

        let result = batch_find_archive_info_by_hash(&conn, &["hash1", "hash2"]).unwrap();

        // Verify we can distinguish which archive each hash is in
        let info1 = result.get("hash1").unwrap();
        assert_eq!(info1[0].0, archive1_id);

        let info2 = result.get("hash2").unwrap();
        assert_eq!(info2[0].0, archive2_id);
    }

    #[test]
    fn batch_find_archive_info_by_hash_multiple_archives_per_hash() {
        let conn = setup_test_db();

        // Setup: same hash in two archives
        let archive1_id = insert_root(&conn, "/archive1", "archive");
        let archive2_id = insert_root(&conn, "/archive2", "archive");
        let obj_id = insert_object(&conn, "abc123", false);
        insert_source(&conn, archive1_id, "file.jpg", Some(obj_id), true);
        insert_source(&conn, archive2_id, "copy.jpg", Some(obj_id), true);

        let result = batch_find_archive_info_by_hash(&conn, &["abc123"]).unwrap();

        assert_eq!(result.len(), 1);
        let info = result.get("abc123").unwrap();
        assert_eq!(info.len(), 2);
        // Ordered by archive root_id
        assert_eq!(info[0].0, archive1_id);
        assert_eq!(info[0].1, "/archive1/file.jpg");
        assert_eq!(info[1].0, archive2_id);
        assert_eq!(info[1].1, "/archive2/copy.jpg");
    }

    #[test]
    fn batch_find_archive_info_by_hash_empty_rel_path() {
        let conn = setup_test_db();

        // Setup: source at root of archive (empty rel_path)
        let archive_id = insert_root(&conn, "/archive", "archive");
        let obj_id = insert_object(&conn, "abc123", false);
        insert_source(&conn, archive_id, "", Some(obj_id), true);

        let result = batch_find_archive_info_by_hash(&conn, &["abc123"]).unwrap();

        let info = result.get("abc123").unwrap();
        assert_eq!(info[0].1, "/archive"); // No trailing slash
    }

    #[test]
    fn batch_find_archive_info_by_hash_excludes_non_archive_roots() {
        let conn = setup_test_db();

        // Setup: source in non-archive root
        let source_root_id = insert_root(&conn, "/photos", "source");
        let obj_id = insert_object(&conn, "abc123", false);
        insert_source(&conn, source_root_id, "file.jpg", Some(obj_id), true);

        let result = batch_find_archive_info_by_hash(&conn, &["abc123"]).unwrap();

        // Should not include the source root
        assert!(result.is_empty());
    }

    #[test]
    fn batch_find_archive_info_by_hash_excludes_non_present() {
        let conn = setup_test_db();

        // Setup: non-present source in archive
        let archive_id = insert_root(&conn, "/archive", "archive");
        let obj_id = insert_object(&conn, "abc123", false);
        insert_source(&conn, archive_id, "file.jpg", Some(obj_id), false); // present=false

        let result = batch_find_archive_info_by_hash(&conn, &["abc123"]).unwrap();

        assert!(result.is_empty());
    }

    #[test]
    fn batch_find_archive_info_by_hash_not_found_hashes_excluded() {
        let conn = setup_test_db();

        // Setup: one hash exists, one doesn't
        let archive_id = insert_root(&conn, "/archive", "archive");
        let obj_id = insert_object(&conn, "exists", false);
        insert_source(&conn, archive_id, "file.jpg", Some(obj_id), true);

        let result = batch_find_archive_info_by_hash(&conn, &["exists", "missing"]).unwrap();

        // Only the existing hash should be in results
        assert_eq!(result.len(), 1);
        assert!(result.contains_key("exists"));
        assert!(!result.contains_key("missing"));
    }

    #[test]
    fn batch_find_archive_info_by_hash_handles_large_hash_sets() {
        let conn = setup_test_db();

        // Setup: one archive with many objects
        let archive_id = insert_root(&conn, "/archive", "archive");

        // Create more than BATCH_SIZE hashes (1000+)
        let mut hashes: Vec<String> = Vec::new();
        for i in 0..1050 {
            let hash = format!("hash_{i}");
            let obj_id = insert_object(&conn, &hash, false);
            hashes.push(hash);
            // Put every 10th object in archive
            if i % 10 == 0 {
                insert_source(
                    &conn,
                    archive_id,
                    &format!("file_{i}.jpg"),
                    Some(obj_id),
                    true,
                );
            }
        }

        let hash_refs: Vec<&str> = hashes.iter().map(|s| s.as_str()).collect();
        let result = batch_find_archive_info_by_hash(&conn, &hash_refs).unwrap();

        // Should find 105 hashes (every 10th from 0 to 1040)
        assert_eq!(result.len(), 105);
    }

    // =========================================================================
    // fetch_by_hash tests
    // =========================================================================

    #[test]
    fn fetch_by_hash_returns_object() {
        let conn = setup_test_db();
        let obj_id = insert_object(&conn, "abc123def456", false);

        let result = fetch_by_hash(&conn, "abc123def456").unwrap();

        assert!(result.is_some());
        let obj = result.unwrap();
        assert_eq!(obj.id, obj_id);
        assert_eq!(obj.hash_type, "sha256");
        assert_eq!(obj.hash_value, "abc123def456");
        assert!(!obj.excluded);
    }

    #[test]
    fn fetch_by_hash_not_found() {
        let conn = setup_test_db();

        let result = fetch_by_hash(&conn, "nonexistent_hash").unwrap();

        assert!(result.is_none());
    }

    #[test]
    fn fetch_by_hash_returns_excluded_flag() {
        let conn = setup_test_db();
        insert_object(&conn, "excluded_hash", true);

        let result = fetch_by_hash(&conn, "excluded_hash").unwrap();

        assert!(result.is_some());
        let obj = result.unwrap();
        assert!(obj.excluded);
    }

    // =========================================================================
    // get_or_create tests
    // =========================================================================

    #[test]
    fn get_or_create_creates_new_returns_complete_object() {
        let conn = setup_test_db();

        let obj = get_or_create(&conn, "sha256", "abc123").unwrap();

        // Verify returned Object has all fields populated correctly
        assert!(obj.id > 0);
        assert_eq!(obj.hash_type, "sha256");
        assert_eq!(obj.hash_value, "abc123");
        assert!(!obj.excluded); // New objects are not excluded

        // Verify object was created in database
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM objects WHERE hash_type = 'sha256' AND hash_value = 'abc123'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn get_or_create_returns_existing_with_excluded_flag() {
        let conn = setup_test_db();

        // Create an excluded object first
        let id1 = insert_object(&conn, "existing_hash", true);

        // get_or_create should return the same object with excluded=true
        let obj = get_or_create(&conn, "sha256", "existing_hash").unwrap();
        assert_eq!(obj.id, id1);
        assert_eq!(obj.hash_value, "existing_hash");
        assert!(obj.excluded); // Preserved from existing object

        // Verify only one object exists
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM objects WHERE hash_value = 'existing_hash'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn get_or_create_is_idempotent() {
        let conn = setup_test_db();

        // Call multiple times with same hash
        let obj1 = get_or_create(&conn, "sha256", "same_hash").unwrap();
        let obj2 = get_or_create(&conn, "sha256", "same_hash").unwrap();
        let obj3 = get_or_create(&conn, "sha256", "same_hash").unwrap();

        // All should return same Object
        assert_eq!(obj1.id, obj2.id);
        assert_eq!(obj2.id, obj3.id);
        assert_eq!(obj1.hash_value, "same_hash");

        // Verify only one object exists
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM objects WHERE hash_value = 'same_hash'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn get_or_create_different_hashes() {
        let conn = setup_test_db();

        let obj1 = get_or_create(&conn, "sha256", "hash1").unwrap();
        let obj2 = get_or_create(&conn, "sha256", "hash2").unwrap();

        // Different IDs and hash values
        assert_ne!(obj1.id, obj2.id);
        assert_eq!(obj1.hash_value, "hash1");
        assert_eq!(obj2.hash_value, "hash2");

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM objects", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 2);
    }

    // =========================================================================
    // find_orphaned_stats / delete_orphaned tests
    // =========================================================================

    fn insert_fact(
        conn: &RusqliteConnection,
        entity_type: &str,
        entity_id: i64,
        key: &str,
        value: &str,
    ) {
        conn.execute(
            "INSERT INTO facts (entity_type, entity_id, key, value_text, observed_at, observed_basis_rev)
             VALUES (?, ?, ?, ?, 0, CASE WHEN ? = 'source' THEN 0 ELSE NULL END)",
            rusqlite::params![entity_type, entity_id, key, value, entity_type],
        )
        .unwrap();
    }

    #[test]
    fn find_orphaned_stats_no_orphans() {
        let conn = setup_test_db();

        // Object with present source - not orphaned
        let root_id = insert_root(&conn, "/root", "source");
        let obj_id = insert_object(&conn, "abc123", false);
        insert_source(&conn, root_id, "file.jpg", Some(obj_id), true); // present=true

        let stats = find_orphaned_stats(&conn).unwrap();

        assert_eq!(stats.object_count, 0);
        assert_eq!(stats.source_count, 0);
        assert_eq!(stats.stored_on_source_count, 0);
        assert_eq!(stats.stored_on_content_count, 0);
    }

    #[test]
    fn find_orphaned_stats_object_with_no_sources() {
        let conn = setup_test_db();

        // Object with no sources at all - orphaned
        insert_object(&conn, "abc123", false);

        let stats = find_orphaned_stats(&conn).unwrap();

        assert_eq!(stats.object_count, 1);
        assert_eq!(stats.source_count, 0);
    }

    #[test]
    fn find_orphaned_stats_object_with_only_non_present_sources() {
        let conn = setup_test_db();

        // Object with only non-present sources - orphaned
        let root_id = insert_root(&conn, "/root", "source");
        let obj_id = insert_object(&conn, "abc123", false);
        insert_source(&conn, root_id, "file1.jpg", Some(obj_id), false); // present=false
        insert_source(&conn, root_id, "file2.jpg", Some(obj_id), false); // present=false

        let stats = find_orphaned_stats(&conn).unwrap();

        assert_eq!(stats.object_count, 1);
        assert_eq!(stats.source_count, 2);
    }

    #[test]
    fn find_orphaned_stats_counts_facts() {
        let conn = setup_test_db();

        // Orphaned object with facts
        let root_id = insert_root(&conn, "/root", "source");
        let obj_id = insert_object(&conn, "abc123", false);
        let source_id = insert_source(&conn, root_id, "file.jpg", Some(obj_id), false);

        // Add facts stored on the source
        insert_fact(&conn, "source", source_id, "content.Make", "Canon");
        insert_fact(&conn, "source", source_id, "content.Model", "EOS");

        // Add a fact stored on the object
        insert_fact(&conn, "object", obj_id, "content.hash.sha256", "abc123");

        let stats = find_orphaned_stats(&conn).unwrap();

        assert_eq!(stats.object_count, 1);
        assert_eq!(stats.source_count, 1);
        assert_eq!(stats.stored_on_source_count, 2);
        assert_eq!(stats.stored_on_content_count, 1);
        assert_eq!(stats.total_fact_count(), 3);
    }

    #[test]
    fn find_orphaned_stats_mixed_orphaned_and_active() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/root", "source");

        // Active object (not orphaned)
        let active_obj_id = insert_object(&conn, "active_hash", false);
        insert_source(&conn, root_id, "active.jpg", Some(active_obj_id), true);
        insert_fact(&conn, "object", active_obj_id, "content.Make", "Canon");

        // Orphaned object
        let orphaned_obj_id = insert_object(&conn, "orphaned_hash", false);
        insert_source(&conn, root_id, "orphaned.jpg", Some(orphaned_obj_id), false);
        insert_fact(&conn, "object", orphaned_obj_id, "content.Make", "Nikon");

        let stats = find_orphaned_stats(&conn).unwrap();

        // Only counts orphaned
        assert_eq!(stats.object_count, 1);
        assert_eq!(stats.source_count, 1);
        assert_eq!(stats.stored_on_content_count, 1);
    }

    #[test]
    fn delete_orphaned_no_orphans() {
        let conn = setup_test_db();

        // Object with present source - not orphaned
        let root_id = insert_root(&conn, "/root", "source");
        let obj_id = insert_object(&conn, "abc123", false);
        insert_source(&conn, root_id, "file.jpg", Some(obj_id), true);

        let stats = delete_orphaned(&conn).unwrap();

        assert_eq!(stats.object_count, 0);

        // Verify nothing was deleted
        let obj_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM objects", [], |row| row.get(0))
            .unwrap();
        assert_eq!(obj_count, 1);
    }

    #[test]
    fn delete_orphaned_removes_orphaned_object() {
        let conn = setup_test_db();

        // Orphaned object (no sources)
        insert_object(&conn, "abc123", false);

        let stats = delete_orphaned(&conn).unwrap();

        assert_eq!(stats.object_count, 1);

        // Verify object was deleted
        let obj_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM objects", [], |row| row.get(0))
            .unwrap();
        assert_eq!(obj_count, 0);
    }

    #[test]
    fn delete_orphaned_cascade_deletes_all() {
        let conn = setup_test_db();

        // Orphaned object with sources and facts
        let root_id = insert_root(&conn, "/root", "source");
        let obj_id = insert_object(&conn, "abc123", false);
        let source_id = insert_source(&conn, root_id, "file.jpg", Some(obj_id), false);

        insert_fact(&conn, "source", source_id, "content.Make", "Canon");
        insert_fact(&conn, "object", obj_id, "content.hash.sha256", "abc123");

        let stats = delete_orphaned(&conn).unwrap();

        assert_eq!(stats.object_count, 1);
        assert_eq!(stats.source_count, 1);
        assert_eq!(stats.stored_on_source_count, 1);
        assert_eq!(stats.stored_on_content_count, 1);

        // Verify all deleted
        let obj_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM objects", [], |row| row.get(0))
            .unwrap();
        let src_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM sources", [], |row| row.get(0))
            .unwrap();
        let fact_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM facts", [], |row| row.get(0))
            .unwrap();

        assert_eq!(obj_count, 0);
        assert_eq!(src_count, 0);
        assert_eq!(fact_count, 0);
    }

    #[test]
    fn delete_orphaned_preserves_active_objects() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/root", "source");

        // Active object (should be preserved)
        let active_obj_id = insert_object(&conn, "active_hash", false);
        let active_source_id =
            insert_source(&conn, root_id, "active.jpg", Some(active_obj_id), true);
        insert_fact(&conn, "object", active_obj_id, "content.Make", "Canon");
        insert_fact(&conn, "source", active_source_id, "source.policy", "keep");

        // Orphaned object (should be deleted)
        let orphaned_obj_id = insert_object(&conn, "orphaned_hash", false);
        let orphaned_source_id =
            insert_source(&conn, root_id, "orphaned.jpg", Some(orphaned_obj_id), false);
        insert_fact(&conn, "object", orphaned_obj_id, "content.Make", "Nikon");
        insert_fact(
            &conn,
            "source",
            orphaned_source_id,
            "source.policy",
            "delete",
        );

        let stats = delete_orphaned(&conn).unwrap();

        // Only orphaned deleted
        assert_eq!(stats.object_count, 1);
        assert_eq!(stats.source_count, 1);
        assert_eq!(stats.stored_on_source_count, 1);
        assert_eq!(stats.stored_on_content_count, 1);

        // Active preserved
        let obj_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM objects", [], |row| row.get(0))
            .unwrap();
        let src_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM sources", [], |row| row.get(0))
            .unwrap();
        let fact_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM facts", [], |row| row.get(0))
            .unwrap();

        assert_eq!(obj_count, 1);
        assert_eq!(src_count, 1);
        assert_eq!(fact_count, 2); // Both facts for active object/source
    }

    #[test]
    fn delete_orphaned_handles_object_with_mixed_present_sources() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/root", "source");

        // Object with both present and non-present sources - NOT orphaned
        let obj_id = insert_object(&conn, "abc123", false);
        insert_source(&conn, root_id, "present.jpg", Some(obj_id), true);
        insert_source(&conn, root_id, "not_present.jpg", Some(obj_id), false);

        let stats = delete_orphaned(&conn).unwrap();

        // Object is not orphaned because it has a present source
        assert_eq!(stats.object_count, 0);

        // Both sources still exist (non-present source is preserved because object is not orphaned)
        let src_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM sources", [], |row| row.get(0))
            .unwrap();
        assert_eq!(src_count, 2);
    }

    // ========================================================================
    // The archived-ness WHERE, spoken once
    // ========================================================================

    #[test]
    fn batch_archive_copies_returns_parked_roots_rather_than_hiding_them() {
        // Testifying is what a closed door permits: content does not stop
        // standing where it stands because the user parked the root.
        let conn = setup_test_db();
        let live = insert_root(&conn, "/arc-live", "archive");
        let parked = insert_parked_root(&conn, "/arc-parked", "archive");
        let both = insert_object(&conn, "both", false);
        let only_parked = insert_object(&conn, "onlyparked", false);
        insert_source(&conn, live, "a.jpg", Some(both), true);
        insert_source(&conn, parked, "a.jpg", Some(both), true);
        insert_source(&conn, parked, "b.jpg", Some(only_parked), true);

        let copies = batch_archive_copies(&conn, &[both, only_parked], None).unwrap();

        let mut both_copies = copies.get(&both).unwrap().clone();
        both_copies.sort_by_key(|c| c.root_id);
        assert_eq!(
            both_copies,
            vec![
                ArchiveCopy {
                    root_id: live,
                    root_suspended: false
                },
                ArchiveCopy {
                    root_id: parked,
                    root_suspended: true
                },
            ]
        );
        assert_eq!(
            copies.get(&only_parked).unwrap(),
            &vec![ArchiveCopy {
                root_id: parked,
                root_suspended: true
            }],
            "an object standing only behind a door is still archived"
        );
    }

    #[test]
    fn batch_check_archived_is_the_key_set_of_the_copies() {
        // One spelling of the archived-ness WHERE: the whether-question and
        // the where-and-on-what-standing question can never come apart.
        let conn = setup_test_db();
        let live = insert_root(&conn, "/arc-live", "archive");
        let parked = insert_parked_root(&conn, "/arc-parked", "archive");
        let source_root = insert_root(&conn, "/src", "source");

        let on_live = insert_object(&conn, "onlive", false);
        let on_parked = insert_object(&conn, "onparked", false);
        let on_both = insert_object(&conn, "onboth", false);
        let empty = insert_object(&conn, "empty", false);
        let source_side_only = insert_object(&conn, "srconly", false);
        let absent = insert_object(&conn, "absent", false);

        insert_source(&conn, live, "1.jpg", Some(on_live), true);
        insert_source(&conn, parked, "2.jpg", Some(on_parked), true);
        insert_source(&conn, live, "3.jpg", Some(on_both), true);
        insert_source(&conn, parked, "3.jpg", Some(on_both), true);
        insert_source(&conn, source_root, "4.jpg", Some(source_side_only), true);
        insert_source(&conn, parked, "5.jpg", Some(absent), false);
        let empty_id = insert_source(&conn, live, "6.jpg", Some(empty), true);
        conn.execute("UPDATE sources SET size = 0 WHERE id = ?", [empty_id])
            .unwrap();

        let ids = vec![on_live, on_parked, on_both, empty, source_side_only, absent];
        for archive in [None, Some(live), Some(parked)] {
            let copies = batch_archive_copies(&conn, &ids, archive).unwrap();
            let archived = batch_check_archived(&conn, &ids, archive).unwrap();
            let keys: HashSet<i64> = copies.keys().copied().collect();
            assert_eq!(archived, keys, "restricted to {archive:?}");
        }

        // The parity would hold vacuously if both were empty, so pin what
        // the fixture is actually saying.
        let archived = batch_check_archived(&conn, &ids, None).unwrap();
        assert_eq!(archived, HashSet::from([on_live, on_parked, on_both]));
    }

    #[test]
    fn the_archive_locations_carry_the_standing_of_their_root() {
        let conn = setup_test_db();
        let live = insert_root(&conn, "/arc-live", "archive");
        let parked = insert_parked_root(&conn, "/arc-parked", "archive");
        let obj = insert_object(&conn, "abc", false);
        insert_source(&conn, live, "keep/a.jpg", Some(obj), true);
        insert_source(&conn, parked, "keep/a.jpg", Some(obj), true);

        let found = batch_find_archive_paths(&conn, &[obj]).unwrap();
        let locations = found.get(&obj).unwrap();

        // Ordered by root path, so the live archive comes first here.
        assert_eq!(locations[0].path, "/arc-live/keep/a.jpg");
        assert!(!locations[0].root.root_suspended);
        assert_eq!(locations[1].path, "/arc-parked/keep/a.jpg");
        assert!(locations[1].root.root_suspended);
    }

    #[test]
    fn archive_copies_survive_the_sql_chunking_boundary() {
        // `BATCH_SIZE` chunks the IN-list, and the map is accumulated across
        // chunks. An object cannot straddle a chunk — the chunking is over
        // object ids — so what this guards is the accumulator itself: a map
        // rebuilt per chunk, or a `collect` that kept only the last one,
        // would answer for a suffix of the ids and every claim about what a
        // count rests on would be short by a chunk. Silent, and only at
        // scale.
        let conn = setup_test_db();
        let live = insert_root(&conn, "/arc-live", "archive");
        let parked = insert_parked_root(&conn, "/arc-parked", "archive");

        let mut ids = Vec::new();
        for i in 0..1_100 {
            let obj = insert_object(&conn, &format!("hash{i}"), false);
            // Every object stands in the parked archive; every third also in
            // the live one, so the all-parked answer varies across chunks.
            insert_source(&conn, parked, &format!("p{i}.jpg"), Some(obj), true);
            if i % 3 == 0 {
                insert_source(&conn, live, &format!("l{i}.jpg"), Some(obj), true);
            }
            ids.push(obj);
        }
        assert!(ids.len() > BATCH_SIZE, "the fixture must cross the seam");

        let copies = batch_archive_copies(&conn, &ids, None).unwrap();
        assert_eq!(copies.len(), ids.len());
        let only_parked = ids
            .iter()
            .filter(|id| {
                crate::core::domain::resolution::only_on_suspended(copies.get(id).unwrap())
            })
            .count();
        assert_eq!(
            only_parked,
            1_100 - 1_100_usize.div_ceil(3),
            "every object with a live copy is excluded, across the seam"
        );
        assert_eq!(
            batch_check_archived(&conn, &ids, None).unwrap().len(),
            ids.len()
        );
    }
}
