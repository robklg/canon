//! The exclude subsystem's repo stratum: the six exclusion-transition SQL
//! functions and the receipt-capture row type, carved from `repo::source`/
//! `repo::object` unchanged in SQL and signature. Kept as two inner
//! `source`/`object` modules mirroring their origin, since both define a
//! `set_excluded` with a different signature — flattening them into one
//! namespace would force a rename, which the carve does not do.

use crate::repo::Connection;

pub(crate) mod source {
    use std::collections::HashMap;

    use anyhow::Result;
    use rusqlite::types::Value;

    use super::Connection;
    use crate::repo::source::BATCH_SIZE;

    /// A source sharing an object's content, captured for an object-exclusion
    /// receipt. Unlike [`crate::scan::repo::source::ReceiptSource`] (and every
    /// [`crate::core::domain::source::Source`] fetch in `repo::source`), this
    /// includes non-present tombstone rows: the object-level stamp
    /// (`set_decision_id_by_object`) touches every sharer, present or not, and
    /// the receipt must list exactly that stamp-set so the stamp is
    /// reconstructable from disk (stamp-set = receipt-set).
    pub struct ObjectReceiptSource {
        pub object_id: i64,
        pub root_path: String,
        /// Role of the root ("source"/"archive") — for receipt-entry ordering.
        pub root_role: String,
        pub rel_path: String,
        pub size: i64,
        pub mtime: i64,
        /// `false` for a tombstone row (`present = 0`).
        pub present: bool,
        /// The source's current `decision_id` — the pre-stamp provenance link,
        /// which becomes the receipt item's `previous_decision_id`.
        pub previous_decision_id: Option<i64>,
    }

    /// Fetch receipt snapshots for every source sharing the given objects —
    /// present **and** tombstone rows, grouped by object. Call before
    /// `set_decision_id_by_object` in the same transaction, so
    /// `previous_decision_id` is the pre-stamp value.
    ///
    /// Chunks the ID list to stay under the SQLite variable limit. Rows within a
    /// group are unordered; the caller sorts for a stable receipt.
    pub fn fetch_object_sharers_for_receipt(
        conn: &Connection,
        object_ids: &[i64],
    ) -> Result<HashMap<i64, Vec<ObjectReceiptSource>>> {
        if object_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let mut result: HashMap<i64, Vec<ObjectReceiptSource>> = HashMap::new();

        for chunk in object_ids.chunks(BATCH_SIZE) {
            let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
            let sql = format!(
                "SELECT s.object_id, r.path, r.role, s.rel_path, s.size, s.mtime, s.present, s.decision_id
                 FROM sources s
                 JOIN roots r ON s.root_id = r.id
                 WHERE s.object_id IN ({})",
                placeholders.join(",")
            );

            let params: Vec<Value> = chunk.iter().map(|&id| Value::from(id)).collect();
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map(rusqlite::params_from_iter(params), |row| {
                Ok(ObjectReceiptSource {
                    object_id: row.get(0)?,
                    root_path: row.get(1)?,
                    root_role: row.get(2)?,
                    rel_path: row.get(3)?,
                    size: row.get(4)?,
                    mtime: row.get(5)?,
                    present: row.get(6)?,
                    previous_decision_id: row.get(7)?,
                })
            })?;

            for row in rows {
                let row = row?;
                result.entry(row.object_id).or_default().push(row);
            }
        }

        Ok(result)
    }

    /// Set the exclusion flag for a single source, recording the deciding decision.
    ///
    /// # Behavior
    /// - Updates `excluded` and `decision_id` columns to the specified values
    /// - `decision_id` is the decision that caused this exclusion state transition;
    ///   `None` writes NULL (e.g. when recording is disabled or in tests)
    /// - No error if source doesn't exist (0 rows affected)
    /// - Does NOT affect object-level exclusion
    ///
    /// # Returns
    /// Ok(()) on success. To verify the source existed, use batch variant which returns count.
    pub fn set_excluded(
        conn: &Connection,
        source_id: i64,
        excluded: bool,
        decision_id: Option<i64>,
    ) -> Result<()> {
        conn.execute(
            "UPDATE sources SET excluded = ?, decision_id = ? WHERE id = ?",
            rusqlite::params![excluded as i64, decision_id, source_id],
        )?;
        Ok(())
    }

    /// Set the exclusion flag for multiple sources, recording the deciding decision.
    ///
    /// # Behavior
    /// - Updates `excluded` and `decision_id` columns for all specified sources
    /// - `decision_id` is the decision that caused this exclusion state transition;
    ///   `None` writes NULL
    /// - Handles large inputs via chunking (BATCH_SIZE = 1000)
    /// - Sources that don't exist are silently skipped
    ///
    /// # Returns
    /// Count of rows actually updated (may be less than input if some sources don't exist).
    pub fn batch_set_excluded(
        conn: &Connection,
        source_ids: &[i64],
        excluded: bool,
        decision_id: Option<i64>,
    ) -> Result<u64> {
        if source_ids.is_empty() {
            return Ok(0);
        }

        let mut total_updated = 0u64;

        for chunk in source_ids.chunks(BATCH_SIZE) {
            let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
            let sql = format!(
                "UPDATE sources SET excluded = ?, decision_id = ? WHERE id IN ({})",
                placeholders.join(",")
            );

            // Build params: excluded flag, decision_id, then all the IDs
            let mut params: Vec<rusqlite::types::Value> = Vec::with_capacity(chunk.len() + 2);
            params.push(rusqlite::types::Value::from(excluded as i64));
            params.push(match decision_id {
                Some(id) => rusqlite::types::Value::from(id),
                None => rusqlite::types::Value::Null,
            });
            for &id in chunk {
                params.push(rusqlite::types::Value::from(id));
            }

            let updated = conn.execute(&sql, rusqlite::params_from_iter(params))?;
            total_updated += updated as u64;
        }

        Ok(total_updated)
    }

    /// Set `decision_id` on all sources sharing the given object.
    ///
    /// Object-level exclusion flips `objects.excluded`, but the provenance link
    /// (which decision caused the transition) lives on `sources`. This records that
    /// link on every present-or-not source pointing at the object.
    ///
    /// `None` writes NULL. Returns the number of source rows updated.
    pub fn set_decision_id_by_object(
        conn: &Connection,
        object_id: i64,
        decision_id: Option<i64>,
    ) -> Result<u64> {
        let updated = conn.execute(
            "UPDATE sources SET decision_id = ? WHERE object_id = ?",
            rusqlite::params![decision_id, object_id],
        )?;
        Ok(updated as u64)
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::exclude::repo::tests::{insert_object, insert_source, setup_test_db};
        use rusqlite::Connection as RusqliteConnection;

        // =========================================================================
        // fetch_object_sharers_for_receipt tests
        // =========================================================================

        #[test]
        fn fetch_object_sharers_for_receipt_handles_large_batch() {
            let conn = setup_test_db();

            let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);

            // More than BATCH_SIZE objects, so the chunking loop runs more than once.
            let mut object_ids = Vec::new();
            for i in 0..1050 {
                let object_id = insert_object(&conn, &format!("hash_{i}"), false);
                insert_source(
                    &conn,
                    root_id,
                    &format!("file_{i}.jpg"),
                    Some(object_id),
                    true,
                    false,
                );
                object_ids.push(object_id);
            }

            let sharers = fetch_object_sharers_for_receipt(&conn, &object_ids).unwrap();

            assert_eq!(sharers.len(), 1050);

            // Sample from each chunk — a chunk that never ran leaves a hole here.
            for idx in [0usize, 500, 1049] {
                let group = sharers
                    .get(&object_ids[idx])
                    .unwrap_or_else(|| panic!("object at index {idx} missing from the result"));
                assert_eq!(group.len(), 1);
                assert_eq!(group[0].rel_path, format!("file_{idx}.jpg"));
            }
        }

        #[test]
        fn fetch_object_sharers_for_receipt_groups_by_object() {
            let conn = setup_test_db();

            let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
            let obj_a = insert_object(&conn, "hash_a", false);
            let obj_b = insert_object(&conn, "hash_b", false);

            insert_source(&conn, root_id, "a1.jpg", Some(obj_a), true, false);
            // A tombstone sharer: the object-level stamp touches it, so it belongs
            // in its object's group like any present row.
            insert_source(&conn, root_id, "a2.jpg", Some(obj_a), false, false);
            insert_source(&conn, root_id, "b1.jpg", Some(obj_b), true, false);
            insert_source(&conn, root_id, "b2.jpg", Some(obj_b), true, false);

            let sharers = fetch_object_sharers_for_receipt(&conn, &[obj_a, obj_b]).unwrap();

            assert_eq!(sharers.len(), 2);

            // Each group holds its own object's sharers and no other's.
            let mut group_a: Vec<&str> = sharers[&obj_a]
                .iter()
                .map(|s| s.rel_path.as_str())
                .collect();
            group_a.sort_unstable();
            assert_eq!(group_a, ["a1.jpg", "a2.jpg"]);

            let mut group_b: Vec<&str> = sharers[&obj_b]
                .iter()
                .map(|s| s.rel_path.as_str())
                .collect();
            group_b.sort_unstable();
            assert_eq!(group_b, ["b1.jpg", "b2.jpg"]);

            let tombstone = sharers[&obj_a]
                .iter()
                .find(|s| s.rel_path == "a2.jpg")
                .unwrap();
            assert!(!tombstone.present);
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
            let excluded: i64 = conn
                .query_row(
                    "SELECT excluded FROM sources WHERE id = ?",
                    rusqlite::params![source_id],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(excluded, 0);

            // Set excluded
            set_excluded(&conn, source_id, true, None).unwrap();

            // Verify now excluded
            let excluded: i64 = conn
                .query_row(
                    "SELECT excluded FROM sources WHERE id = ?",
                    rusqlite::params![source_id],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(excluded, 1);
        }

        #[test]
        fn set_excluded_clears_source() {
            let conn = setup_test_db();

            let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
            let source_id = insert_source(&conn, root_id, "file.jpg", None, true, true); // starts excluded

            // Verify initially excluded
            let excluded: i64 = conn
                .query_row(
                    "SELECT excluded FROM sources WHERE id = ?",
                    rusqlite::params![source_id],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(excluded, 1);

            // Clear excluded
            set_excluded(&conn, source_id, false, None).unwrap();

            // Verify now not excluded
            let excluded: i64 = conn
                .query_row(
                    "SELECT excluded FROM sources WHERE id = ?",
                    rusqlite::params![source_id],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(excluded, 0);
        }

        #[test]
        fn set_excluded_nonexistent_source() {
            let conn = setup_test_db();

            // Should not error when source doesn't exist
            let result = set_excluded(&conn, 99999, true, None);
            assert!(result.is_ok());
        }

        // =========================================================================
        // batch_set_excluded tests
        // =========================================================================

        #[test]
        fn batch_set_excluded_empty_list() {
            let conn = setup_test_db();
            let count = batch_set_excluded(&conn, &[], true, None).unwrap();
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
            let count = batch_set_excluded(&conn, &[id1, id2], true, None).unwrap();
            assert_eq!(count, 2);

            // Verify exclusion state
            let excluded1: i64 = conn
                .query_row(
                    "SELECT excluded FROM sources WHERE id = ?",
                    rusqlite::params![id1],
                    |row| row.get(0),
                )
                .unwrap();
            let excluded2: i64 = conn
                .query_row(
                    "SELECT excluded FROM sources WHERE id = ?",
                    rusqlite::params![id2],
                    |row| row.get(0),
                )
                .unwrap();
            let excluded3: i64 = conn
                .query_row(
                    "SELECT excluded FROM sources WHERE id = ?",
                    rusqlite::params![id3],
                    |row| row.get(0),
                )
                .unwrap();

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
            let count = batch_set_excluded(&conn, &[id1, 99999], true, None).unwrap();

            // Only id1 should be updated
            assert_eq!(count, 1);
        }

        #[test]
        fn batch_set_excluded_skips_nonexistent() {
            let conn = setup_test_db();

            let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
            let id1 = insert_source(&conn, root_id, "file.jpg", None, true, false);

            // Mix of existing and nonexistent IDs
            let count = batch_set_excluded(&conn, &[id1, 99998, 99999], true, None).unwrap();

            // Only the existing source should be updated
            assert_eq!(count, 1);

            // Verify it was actually updated
            let excluded: i64 = conn
                .query_row(
                    "SELECT excluded FROM sources WHERE id = ?",
                    rusqlite::params![id1],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(excluded, 1);
        }

        #[test]
        fn batch_set_excluded_handles_large_batch() {
            let conn = setup_test_db();

            let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);

            // Create more than BATCH_SIZE sources (1000+)
            let mut source_ids = Vec::new();
            for i in 0..1050 {
                let id = insert_source(&conn, root_id, &format!("file_{i}.jpg"), None, true, false);
                source_ids.push(id);
            }

            // Exclude all of them
            let count = batch_set_excluded(&conn, &source_ids, true, None).unwrap();
            assert_eq!(count, 1050);

            // Verify a sample from each batch chunk
            let excluded_first: i64 = conn
                .query_row(
                    "SELECT excluded FROM sources WHERE id = ?",
                    rusqlite::params![source_ids[0]],
                    |row| row.get(0),
                )
                .unwrap();
            let excluded_mid: i64 = conn
                .query_row(
                    "SELECT excluded FROM sources WHERE id = ?",
                    rusqlite::params![source_ids[500]],
                    |row| row.get(0),
                )
                .unwrap();
            let excluded_last: i64 = conn
                .query_row(
                    "SELECT excluded FROM sources WHERE id = ?",
                    rusqlite::params![source_ids[1049]],
                    |row| row.get(0),
                )
                .unwrap();

            assert_eq!(excluded_first, 1);
            assert_eq!(excluded_mid, 1);
            assert_eq!(excluded_last, 1);
        }

        // =========================================================================
        // decision_id threading tests
        // =========================================================================

        fn fetch_decision_id(conn: &RusqliteConnection, source_id: i64) -> Option<i64> {
            conn.query_row(
                "SELECT decision_id FROM sources WHERE id = ?",
                rusqlite::params![source_id],
                |row| row.get(0),
            )
            .unwrap()
        }

        #[test]
        fn set_excluded_writes_decision_id() {
            let conn = setup_test_db();
            let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
            let source_id = insert_source(&conn, root_id, "file.jpg", None, true, false);

            // Some(id) records the deciding decision
            set_excluded(&conn, source_id, true, Some(42)).unwrap();
            assert_eq!(fetch_decision_id(&conn, source_id), Some(42));

            // None writes NULL
            set_excluded(&conn, source_id, false, None).unwrap();
            assert_eq!(fetch_decision_id(&conn, source_id), None);
        }

        #[test]
        fn batch_set_excluded_writes_decision_id() {
            let conn = setup_test_db();
            let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
            let id1 = insert_source(&conn, root_id, "a.jpg", None, true, false);
            let id2 = insert_source(&conn, root_id, "b.jpg", None, true, false);

            set_excluded(&conn, id1, true, Some(7)).unwrap();
            set_excluded(&conn, id2, true, Some(7)).unwrap();

            // Re-exclude in one batch with a new decision
            let count = batch_set_excluded(&conn, &[id1, id2], true, Some(99)).unwrap();
            assert_eq!(count, 2);
            assert_eq!(fetch_decision_id(&conn, id1), Some(99));
            assert_eq!(fetch_decision_id(&conn, id2), Some(99));

            // None writes NULL across the batch
            batch_set_excluded(&conn, &[id1, id2], false, None).unwrap();
            assert_eq!(fetch_decision_id(&conn, id1), None);
            assert_eq!(fetch_decision_id(&conn, id2), None);
        }

        #[test]
        fn set_decision_id_by_object_updates_all_sources() {
            let conn = setup_test_db();
            let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
            let obj_id = crate::repo::object::get_or_create(&conn, "sha256", "deadbeef")
                .unwrap()
                .id;
            let id1 = insert_source(&conn, root_id, "copy1.jpg", Some(obj_id), true, false);
            let id2 = insert_source(&conn, root_id, "copy2.jpg", Some(obj_id), true, false);
            // A source on a different object must NOT be touched.
            let other_obj = crate::repo::object::get_or_create(&conn, "sha256", "cafef00d")
                .unwrap()
                .id;
            let id3 = insert_source(&conn, root_id, "other.jpg", Some(other_obj), true, false);

            let count = set_decision_id_by_object(&conn, obj_id, Some(5)).unwrap();
            assert_eq!(count, 2);
            assert_eq!(fetch_decision_id(&conn, id1), Some(5));
            assert_eq!(fetch_decision_id(&conn, id2), Some(5));
            assert_eq!(
                fetch_decision_id(&conn, id3),
                None,
                "other object untouched"
            );

            // None writes NULL
            set_decision_id_by_object(&conn, obj_id, None).unwrap();
            assert_eq!(fetch_decision_id(&conn, id1), None);
            assert_eq!(fetch_decision_id(&conn, id2), None);
        }
    }
}

pub(crate) mod object {
    use anyhow::Result;

    use super::Connection;
    use crate::core::domain::object::Object;
    use crate::repo::object::{object_from_row, OBJECT_COLUMNS};

    /// Set the exclusion flag for an object.
    ///
    /// # Behavior
    /// - Updates `excluded` column to the specified value
    /// - No error if object doesn't exist (0 rows affected)
    /// - Affects all sources linked to this object (via Source::is_excluded() predicate)
    ///
    /// # Returns
    /// Ok(()) on success.
    pub fn set_excluded(conn: &Connection, object_id: i64, excluded: bool) -> Result<()> {
        conn.execute(
            "UPDATE objects SET excluded = ? WHERE id = ?",
            rusqlite::params![excluded as i64, object_id],
        )?;
        Ok(())
    }

    /// Fetch all excluded objects.
    ///
    /// Returns a Vec of Object structs where excluded = 1, ordered by id.
    /// Used by `exclude list --objects` to show all excluded objects.
    pub fn fetch_excluded(conn: &Connection) -> Result<Vec<Object>> {
        let sql = format!("SELECT {OBJECT_COLUMNS} FROM objects WHERE excluded = 1 ORDER BY id");

        let mut stmt = conn.prepare(&sql)?;
        let objects = stmt
            .query_map([], object_from_row)?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(objects)
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::exclude::repo::tests::{insert_object, setup_test_db};

        // =========================================================================
        // set_excluded tests
        // =========================================================================

        #[test]
        fn set_excluded_marks_object() {
            let conn = setup_test_db();
            let obj_id = insert_object(&conn, "abc123", false);

            // Verify initially not excluded
            let excluded: i64 = conn
                .query_row(
                    "SELECT excluded FROM objects WHERE id = ?",
                    rusqlite::params![obj_id],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(excluded, 0);

            // Set excluded
            set_excluded(&conn, obj_id, true).unwrap();

            // Verify now excluded
            let excluded: i64 = conn
                .query_row(
                    "SELECT excluded FROM objects WHERE id = ?",
                    rusqlite::params![obj_id],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(excluded, 1);
        }

        #[test]
        fn set_excluded_clears_object() {
            let conn = setup_test_db();
            let obj_id = insert_object(&conn, "abc123", true); // starts excluded

            // Verify initially excluded
            let excluded: i64 = conn
                .query_row(
                    "SELECT excluded FROM objects WHERE id = ?",
                    rusqlite::params![obj_id],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(excluded, 1);

            // Clear excluded
            set_excluded(&conn, obj_id, false).unwrap();

            // Verify now not excluded
            let excluded: i64 = conn
                .query_row(
                    "SELECT excluded FROM objects WHERE id = ?",
                    rusqlite::params![obj_id],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(excluded, 0);
        }

        #[test]
        fn set_excluded_nonexistent_object() {
            let conn = setup_test_db();

            // Should not error when object doesn't exist
            let result = set_excluded(&conn, 99999, true);
            assert!(result.is_ok());
        }

        // =========================================================================
        // fetch_excluded tests
        // =========================================================================

        #[test]
        fn fetch_excluded_returns_only_excluded() {
            let conn = setup_test_db();

            // Insert mix of excluded and non-excluded
            insert_object(&conn, "excluded1", true);
            insert_object(&conn, "not_excluded", false);
            insert_object(&conn, "excluded2", true);

            let result = fetch_excluded(&conn).unwrap();

            assert_eq!(result.len(), 2);
            assert!(result.iter().all(|o| o.excluded));
            // Ordered by id
            assert_eq!(result[0].hash_value, "excluded1");
            assert_eq!(result[1].hash_value, "excluded2");
        }

        #[test]
        fn fetch_excluded_empty_when_none_excluded() {
            let conn = setup_test_db();

            insert_object(&conn, "not_excluded1", false);
            insert_object(&conn, "not_excluded2", false);

            let result = fetch_excluded(&conn).unwrap();

            assert!(result.is_empty());
        }
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection as RusqliteConnection;

    /// Create an in-memory database with the full schema.
    pub(super) fn setup_test_db() -> RusqliteConnection {
        crate::repo::open_in_memory_for_test()
    }

    /// Insert a test object and return its ID.
    pub(super) fn insert_object(conn: &RusqliteConnection, hash: &str, excluded: bool) -> i64 {
        conn.execute(
            "INSERT INTO objects (hash_type, hash_value, excluded) VALUES ('sha256', ?, ?)",
            rusqlite::params![hash, excluded as i64],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    /// Insert a test source and return its ID.
    pub(super) fn insert_source(
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
}
