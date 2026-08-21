//! The scan subsystem's repo stratum: the nine scan-transition SQL functions
//! and the receipt-capture row type, carved from `core::repo::source`/
//! `core::repo::root`/`core::repo::fact` unchanged in SQL and signature. Kept as three inner
//! `source`/`root`/`fact` modules mirroring their origin — `insert_object`
//! exists in two incompatible signatures between the source and fact test
//! flavors, so flattening into one namespace would force a rename the carve
//! does not do. Opens no transaction: every function here takes `&Connection`.

use crate::core::repo::Connection;

pub(crate) mod source {
    use anyhow::Result;
    use rusqlite::types::Value;
    use rusqlite::OptionalExtension;

    use super::Connection;
    use crate::core::domain::source::Source;
    use crate::core::repo::source::{
        fetch_by_id, fetch_by_path, source_from_row, BATCH_SIZE, SOURCE_COLUMNS, SOURCE_FROM,
    };
    use crate::scan::domain::{FileObservation, Reconciliation};

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
            "SELECT {SOURCE_COLUMNS} {SOURCE_FROM} WHERE s.present = 1 AND s.device = ? AND s.inode = ?",
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

    /// Apply a reconciliation outcome to the database.
    ///
    /// Translates the domain `Reconciliation` into the appropriate SQL operation.
    /// This function does NOT manage transactions — the caller should wrap the call
    /// in a transaction if atomicity with other operations is needed.
    ///
    /// # Behavior by Reconciliation variant
    ///
    /// - **New**: INSERT source with basis_rev=0, scanned_at=now, present=1 (or
    ///   revive a stale row at the path, incrementing its basis_rev)
    /// - **Unchanged**: UPDATE last_seen_at=now plus device+inode — the file is
    ///   the same one, but where it sits may have been renumbered (the silent refresh)
    /// - **Modified**: UPDATE size, mtime, partial_hash, device, inode, basis_rev+1,
    ///   last_seen_at=now, and clear object_id (content changed, identity unknown)
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
        decision_id: Option<i64>,
    ) -> Result<Source> {
        match reconciliation {
            Reconciliation::New => {
                // INSERT new source with basis_rev=0, or revive stale record at same path.
                //
                // A revived row's basis_rev is incremented, never reset: the
                // number's whole job is to differ from any value a manifest or
                // a fact could have recorded earlier, and a reset can land back
                // on one of them — staleness is compared by inequality, so an
                // alias reads as fresh. A fresh INSERT starts at 0 safely: no
                // record can predate the row's first indexing.
                //
                // Two cases lead here:
                // 1. Truly new file: no record exists at this path
                // 2. Replaced file: old file was deleted/marked-missing, new file created at same path
                //
                // We use the same two-step pattern as insert_destination():
                // - First try UPDATE WHERE present=0 (revive stale record)
                // - If no rows updated, INSERT new record
                //
                // decision_id is set on New only — scan UPDATEs (Modified, Moved, Unchanged)
                // preserve the existing value to maintain provenance. One carve-out:
                // a revive of an excluded row preserves it too (see below).
                let partial_hash = observation.partial_hash.as_ref().ok_or_else(|| {
                    anyhow::anyhow!("partial_hash required for New reconciliation")
                })?;

                // Step 1: Try to update any existing record at this path (stale or replaced)
                // - Stale (present=0): file reappeared at previously-used path
                // - Replaced (present=1, different inode): old file deleted, new file at same path
                //
                // A source exclusion survives both: the dismissal is the user's
                // judgment about this path, and it holds through replacement and
                // reappearance just as it holds through in-place modification —
                // whether an edit lands as Modified (same inode) or as a replace
                // (atomic-save apps rename a temp file over the path) is invisible
                // to the user. Undoing a dismissal is exclude clear's recorded act,
                // never a scan's side effect. An excluded row also keeps its
                // decision_id: the row must keep pointing at the judgment that
                // governs it, not at the scan that re-observed the path.
                let updated = conn.execute(
                    "UPDATE sources SET
                        device = ?, inode = ?, size = ?, mtime = ?, partial_hash = ?,
                        basis_rev = basis_rev + 1, scanned_at = ?, last_seen_at = ?,
                        present = 1, object_id = NULL,
                        decision_id = CASE WHEN excluded = 1 THEN decision_id ELSE ? END
                     WHERE root_id = ? AND rel_path = ?",
                    rusqlite::params![
                        observation.device as i64,
                        observation.inode as i64,
                        observation.size,
                        observation.mtime,
                        partial_hash,
                        now,
                        now,
                        decision_id,
                        observation.root_id,
                        observation.rel_path,
                    ],
                )?;

                if updated == 0 {
                    // Step 2: No stale record exists, insert new
                    conn.execute(
                        "INSERT INTO sources (
                            root_id, rel_path, device, inode, size, mtime, partial_hash,
                            basis_rev, scanned_at, last_seen_at, present, excluded, decision_id
                         ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, 1, 0, ?)",
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
                            decision_id,
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
                // UPDATE with new metadata, increment basis_rev, and drop the
                // object link: the content at this path changed, so the identity
                // Canon recorded for it is no longer a claim it can make. The
                // hash pass re-establishes it; until then the source reads as
                // unhashed, which is the truth and is what a later scan's hash
                // queue looks for. Uniform across the two shapes an edit takes —
                // written in place, or renamed over the path — so the row lands
                // in the same state either way.
                //
                // decision_id and excluded are absent from the SET list, and so
                // preserved: an observation never overwrites the judgment that
                // governs a standing path.
                let partial_hash = observation.partial_hash.as_ref().ok_or_else(|| {
                    anyhow::anyhow!("partial_hash required for Modified reconciliation")
                })?;

                conn.execute(
                    "UPDATE sources SET
                        device = ?, inode = ?, size = ?, mtime = ?,
                        partial_hash = ?, basis_rev = basis_rev + 1,
                        last_seen_at = ?, present = 1, object_id = NULL
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
                // Clear any stale record at the destination path before moving.
                // A present=0 record can legitimately exist at the target (root_id, rel_path)
                // from a previous scan where the file was missing. Since fetch_by_path filters
                // by present=1, the reconciliation logic never sees it, but the UNIQUE constraint
                // on (root_id, rel_path) still blocks the UPDATE. Delete the stale record first.
                conn.execute(
                    "DELETE FROM sources WHERE root_id = ? AND rel_path = ? AND present = 0",
                    rusqlite::params![observation.root_id, observation.rel_path],
                )?;

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

    /// Batch update last_seen_at and device/inode for unchanged sources.
    ///
    /// For unchanged files during scan, we only need to update location metadata
    /// (device/inode may change on remount) and the last_seen_at timestamp.
    /// Batching these updates avoids per-file transactions.
    ///
    /// Each entry is (source_id, device, inode).
    pub fn batch_update_unchanged(
        conn: &Connection,
        entries: &[(i64, i64, i64)],
        now: i64,
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut stmt = conn.prepare_cached(
            "UPDATE sources SET device = ?, inode = ?, last_seen_at = ? WHERE id = ?",
        )?;

        for &(source_id, device, inode) in entries {
            stmt.execute(rusqlite::params![device, inode, now, source_id])?;
        }

        Ok(())
    }

    /// Fetch source IDs for a given root (for missing detection).
    ///
    /// Returns the set of present source IDs for the specified root.
    /// Used at the start of a scan to track which sources should be seen.
    ///
    /// # Arguments
    /// - `conn`: Database connection
    /// - `root_id`: The root to fetch sources for
    /// - `scan_prefix`: Optional path prefix bounding the scan ("photos" returns only
    ///   sources at or under "photos/"; a trailing slash is accepted and means the same)
    pub fn fetch_source_ids_for_root(
        conn: &Connection,
        root_id: i64,
        scan_prefix: Option<&str>,
    ) -> Result<Vec<i64>> {
        let ids: Vec<i64> = match scan_prefix {
            Some(prefix) => {
                // The expected set feeds missing detection, so it must stop at the
                // path separator: a scan scoped to "vacation" must never sweep a
                // sibling like "vacation-2023" into deletion. The shared boundary
                // predicate spells that law once — the path itself, or anything
                // under "{prefix}/", with '_'/'%' in the path matched literally.
                let prefix = prefix.trim_end_matches('/');
                let sql = format!(
                    "SELECT id FROM sources WHERE root_id = ? AND present = 1 AND {}",
                    crate::core::repo::db::path_at_or_under_sql("rel_path")
                );
                conn.prepare(&sql)?
                    .query_map(rusqlite::params![root_id, prefix, prefix, prefix], |row| {
                        row.get(0)
                    })?
                    .collect::<Result<Vec<_>, _>>()?
            }
            None => conn
                .prepare("SELECT id FROM sources WHERE root_id = ? AND present = 1")?
                .query_map(rusqlite::params![root_id], |row| row.get(0))?
                .collect::<Result<Vec<_>, _>>()?,
        };

        Ok(ids)
    }

    /// A source snapshot captured for a receipt, resolved while the source is still
    /// present. `fetch_for_receipt` returns these; the caller maps them into receipt
    /// items. The content hash is resolved from the linked object (`None` if unhashed).
    pub struct ReceiptSource {
        pub rel_path: String,
        /// Content hash formatted `{hash_type}:{hash_value}`; `None` if the source
        /// has no linked object (never hashed).
        pub hash: Option<String>,
        pub size: i64,
        pub mtime: i64,
        /// The source's current `decision_id` — its provenance link at capture time,
        /// which becomes the receipt item's `previous_decision_id`.
        pub previous_decision_id: Option<i64>,
    }

    /// Fetch receipt snapshots for the given source IDs, still-present rows only.
    ///
    /// Used for deletion receipts, which must capture each source's identity, content
    /// hash, and provenance link **before** the `present → absent` flip stamps a new
    /// `decision_id` — so the returned `previous_decision_id` is the pre-flip value.
    /// Restricting to `present = 1` keeps the receipt's set equal to the set the same
    /// transition stamps (stamp-set = receipt-set): already-absent rows are excluded.
    ///
    /// Chunks the ID list to stay under the SQLite variable limit. Returns rows in no
    /// particular order; the caller sorts if a stable receipt is wanted.
    pub fn fetch_for_receipt(conn: &Connection, source_ids: &[i64]) -> Result<Vec<ReceiptSource>> {
        if source_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut result = Vec::with_capacity(source_ids.len());

        for chunk in source_ids.chunks(BATCH_SIZE) {
            let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
            let sql = format!(
                "SELECT s.rel_path, o.hash_type, o.hash_value, s.size, s.mtime, s.decision_id
                 FROM sources s
                 LEFT JOIN objects o ON s.object_id = o.id
                 WHERE s.present = 1 AND s.id IN ({})",
                placeholders.join(",")
            );

            let params: Vec<Value> = chunk.iter().map(|&id| Value::from(id)).collect();
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map(rusqlite::params_from_iter(params), |row| {
                let hash_type: Option<String> = row.get(1)?;
                let hash_value: Option<String> = row.get(2)?;
                let hash = match (hash_type, hash_value) {
                    (Some(t), Some(v)) => Some(format!("{t}:{v}")),
                    _ => None,
                };
                Ok(ReceiptSource {
                    rel_path: row.get(0)?,
                    hash,
                    size: row.get(3)?,
                    mtime: row.get(4)?,
                    previous_decision_id: row.get(5)?,
                })
            })?;

            for row in rows {
                result.push(row?);
            }
        }

        Ok(result)
    }

    /// Fetch source IDs and device info for sources matching a path prefix.
    ///
    /// Used by scan to detect disconnected sources (sources on a different device
    /// than the current scan). Returns `(source_id, device)` pairs for mount
    /// protection logic.
    ///
    /// # Arguments
    /// * `conn` - Database connection
    /// * `root_id` - The root to search within
    /// * `rel_prefix` - Relative path prefix (empty string matches all)
    ///
    /// # Returns
    /// Vector of (source_id, device) tuples for present sources matching the prefix.
    pub fn fetch_device_info_by_prefix(
        conn: &Connection,
        root_id: i64,
        rel_prefix: &str,
    ) -> Result<Vec<(i64, Option<i64>)>> {
        // Empty prefix means the whole root; otherwise the shared boundary
        // predicate (strictly under "{prefix}/", wildcard bytes literal).
        let sql = if rel_prefix.is_empty() {
            "SELECT id, device FROM sources WHERE root_id = ? AND present = 1".to_string()
        } else {
            format!(
                "SELECT id, device FROM sources WHERE root_id = ? AND present = 1 AND {}",
                crate::core::repo::db::path_strictly_under_sql("rel_path")
            )
        };
        let mut stmt = conn.prepare(&sql)?;
        let params: Vec<rusqlite::types::Value> = if rel_prefix.is_empty() {
            vec![root_id.into()]
        } else {
            vec![
                root_id.into(),
                rel_prefix.to_string().into(),
                rel_prefix.to_string().into(),
            ]
        };
        let rows = stmt.query_map(rusqlite::params_from_iter(params), |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::core::repo::insert_test_source;
        use crate::scan::repo::tests::setup_test_db;
        use rusqlite::Connection as RusqliteConnection;

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
        // fetch_by_inode tests
        // =========================================================================

        #[test]
        fn fetch_by_inode_exists() {
            let conn = setup_test_db();

            let root_id = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);

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

            let root1 = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);
            let _root2 = crate::core::repo::insert_test_root(&conn, "/archive", "archive", false);

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

            let root_id = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);
            insert_source(&conn, root_id, "file.jpg", None, true, false);

            // Query for non-existent device/inode
            let result = fetch_by_inode(&conn, 999, 999).unwrap();
            assert!(result.is_none());
        }

        #[test]
        fn fetch_by_inode_not_present() {
            let conn = setup_test_db();

            let root_id = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);

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

            let root_id = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);

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

            let source =
                apply_reconciliation(&conn, &observation, &reconciliation, now, None).unwrap();

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

            let root_id = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);

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

            let source =
                apply_reconciliation(&conn, &observation, &reconciliation, now, None).unwrap();

            // Should revive the same record
            assert_eq!(source.id, old_id);
            assert_eq!(source.rel_path, "revived.jpg");
            // Should have new file's attributes
            assert_eq!(source.device, 100);
            assert_eq!(source.inode, 12345);
            assert_eq!(source.size, 2048);
            assert_eq!(source.mtime, 1700000000);
            assert_eq!(source.partial_hash, "newhash");
            // basis_rev moves forward, never back: the stale row stood at 5, so
            // a reader holding any earlier value still sees a difference.
            assert_eq!(source.basis_rev, 6);
            // object_id should be cleared
            assert_eq!(source.object_id, None);

            // Verify only one record exists
            let count: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM sources WHERE root_id = ? AND rel_path = ?",
                    rusqlite::params![root_id, "revived.jpg"],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(count, 1);
        }

        #[test]
        fn apply_reconciliation_unchanged() {
            let conn = setup_test_db();

            let root_id = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);
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

            let source =
                apply_reconciliation(&conn, &observation, &reconciliation, now, None).unwrap();

            assert_eq!(source.id, source_id);
            assert_eq!(source.rel_path, "existing.jpg");

            // Verify last_seen_at was updated
            let last_seen: i64 = conn
                .query_row(
                    "SELECT last_seen_at FROM sources WHERE id = ?",
                    rusqlite::params![source_id],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(last_seen, now);
        }

        #[test]
        fn apply_reconciliation_modified() {
            let conn = setup_test_db();

            let root_id = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);

            // Insert existing source with basis_rev=2, linked to an object
            let old_object = insert_object(&conn, "oldcontent", false);
            conn.execute(
                "INSERT INTO sources (root_id, rel_path, object_id, device, inode, size, mtime, partial_hash, basis_rev, scanned_at, last_seen_at, present)
                 VALUES (?, 'modified.jpg', ?, 100, 12345, 1000, 1700000000, 'oldhash', 2, 0, 0, 1)",
                rusqlite::params![root_id, old_object],
            ).unwrap();
            let source_id = conn.last_insert_rowid();

            let observation = FileObservation {
                root_id,
                rel_path: "modified.jpg".to_string(),
                device: 100,
                inode: 12345,
                size: 2048,        // Changed
                mtime: 1700000100, // Changed
                partial_hash: Some("newhash".to_string()),
            };

            let reconciliation = Reconciliation::Modified {
                source_id,
                old_object_id: None,
            };
            let now = 1700000101;

            let source =
                apply_reconciliation(&conn, &observation, &reconciliation, now, None).unwrap();

            assert_eq!(source.id, source_id);
            assert_eq!(source.size, 2048);
            assert_eq!(source.mtime, 1700000100);
            assert_eq!(source.partial_hash, "newhash");
            assert_eq!(source.basis_rev, 3); // Incremented from 2
                                             // The content changed, so the recorded identity no longer describes
                                             // what is at this path — the link is dropped until the hash pass
                                             // establishes a new one.
            assert_eq!(source.object_id, None);
        }

        #[test]
        fn apply_reconciliation_moved() {
            let conn = setup_test_db();

            let root1 = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);
            let root2 = crate::core::repo::insert_test_root(&conn, "/archive", "archive", false);

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

            let source =
                apply_reconciliation(&conn, &observation, &reconciliation, now, None).unwrap();

            assert_eq!(source.id, source_id);
            assert_eq!(source.root_id, root2); // Moved to new root
            assert_eq!(source.rel_path, "new_location.jpg"); // New path
            assert_eq!(source.root_path, "/archive"); // Joined field updated
        }

        #[test]
        fn apply_reconciliation_moved_clears_stale_record() {
            // Regression test: Moved reconciliation must handle a stale (present=0) record
            // at the destination path. Without this, the UNIQUE(root_id, rel_path) constraint
            // fails because fetch_by_path filters by present=1 (so reconciliation never sees
            // the stale record) but the constraint covers all records regardless of present.
            let conn = setup_test_db();

            let root_id = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);

            // Create a stale record at the destination path (present=0)
            conn.execute(
                "INSERT INTO sources (root_id, rel_path, device, inode, size, mtime, partial_hash, basis_rev, scanned_at, last_seen_at, present)
                 VALUES (?, 'destination.jpg', 50, 999, 500, 1600000000, 'oldhash', 0, 0, 0, 0)",
                rusqlite::params![root_id],
            ).unwrap();

            // Create the source that will be "moved" to the destination path
            conn.execute(
                "INSERT INTO sources (root_id, rel_path, device, inode, size, mtime, partial_hash, basis_rev, scanned_at, last_seen_at, present)
                 VALUES (?, 'origin.jpg', 100, 12345, 1000, 1700000000, 'hash123', 1, 0, 0, 1)",
                rusqlite::params![root_id],
            ).unwrap();
            let source_id = conn.last_insert_rowid();

            let observation = FileObservation {
                root_id,
                rel_path: "destination.jpg".to_string(),
                device: 100,
                inode: 12345,
                size: 1000,
                mtime: 1700000000,
                partial_hash: None,
            };

            let reconciliation = Reconciliation::Moved {
                source_id,
                from_root_id: root_id,
                from_path: "origin.jpg".to_string(),
                old_object_id: None,
            };
            let now = 1700000001;

            // This would fail with UNIQUE constraint before the fix
            let source =
                apply_reconciliation(&conn, &observation, &reconciliation, now, None).unwrap();

            assert_eq!(source.id, source_id);
            assert_eq!(source.rel_path, "destination.jpg");

            // Verify only one record exists at the destination path
            let count: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM sources WHERE root_id = ? AND rel_path = 'destination.jpg'",
                    rusqlite::params![root_id],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(count, 1);
        }

        #[test]
        fn apply_reconciliation_moved_no_stale_record() {
            // Verify the stale-record cleanup is harmless when no stale record exists
            let conn = setup_test_db();

            let root_id = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);

            conn.execute(
                "INSERT INTO sources (root_id, rel_path, device, inode, size, mtime, partial_hash, basis_rev, scanned_at, last_seen_at, present)
                 VALUES (?, 'origin.jpg', 100, 12345, 1000, 1700000000, 'hash123', 1, 0, 0, 1)",
                rusqlite::params![root_id],
            ).unwrap();
            let source_id = conn.last_insert_rowid();

            let observation = FileObservation {
                root_id,
                rel_path: "clean_destination.jpg".to_string(),
                device: 100,
                inode: 12345,
                size: 1000,
                mtime: 1700000000,
                partial_hash: None,
            };

            let reconciliation = Reconciliation::Moved {
                source_id,
                from_root_id: root_id,
                from_path: "origin.jpg".to_string(),
                old_object_id: None,
            };

            let source =
                apply_reconciliation(&conn, &observation, &reconciliation, 1700000001, None)
                    .unwrap();
            assert_eq!(source.id, source_id);
            assert_eq!(source.rel_path, "clean_destination.jpg");
        }

        #[test]
        fn apply_reconciliation_new_requires_partial_hash() {
            let conn = setup_test_db();

            let root_id = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);

            let observation = FileObservation {
                root_id,
                rel_path: "new_file.jpg".to_string(),
                device: 100,
                inode: 12345,
                size: 2048,
                mtime: 1700000000,
                partial_hash: None, // Missing!
            };

            let reconciliation = Reconciliation::New;
            let now = 1700000001;

            let result = apply_reconciliation(&conn, &observation, &reconciliation, now, None);
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("partial_hash"));
        }

        #[test]
        fn test_scan_new_sets_decision_id() {
            // New reconciliation with a decision_id sets it on the source record
            let conn = setup_test_db();
            let root_id = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);

            let observation = FileObservation {
                root_id,
                rel_path: "new.jpg".to_string(),
                device: 1,
                inode: 100,
                size: 1024,
                mtime: 1700000000,
                partial_hash: Some("hash".to_string()),
            };

            let source = apply_reconciliation(
                &conn,
                &observation,
                &Reconciliation::New,
                1700000001,
                Some(99),
            )
            .unwrap();
            assert_eq!(source.decision_id, Some(99));

            let db_val: Option<i64> = conn
                .query_row(
                    "SELECT decision_id FROM sources WHERE id = ?",
                    [source.id],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(db_val, Some(99));
        }

        #[test]
        fn test_scan_new_null_when_disabled() {
            // New reconciliation with decision_id=None leaves decision_id NULL
            let conn = setup_test_db();
            let root_id = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);

            let observation = FileObservation {
                root_id,
                rel_path: "new.jpg".to_string(),
                device: 1,
                inode: 100,
                size: 1024,
                mtime: 1700000000,
                partial_hash: Some("hash".to_string()),
            };

            let source =
                apply_reconciliation(&conn, &observation, &Reconciliation::New, 1700000001, None)
                    .unwrap();
            assert_eq!(source.decision_id, None);
        }

        /// Insert a row at `rel_path` with the given present/excluded state and
        /// decision_id, for the revive-path tests. Inode is fixed at 100.
        fn insert_row_for_revive(
            conn: &RusqliteConnection,
            root_id: i64,
            rel_path: &str,
            present: bool,
            excluded: bool,
            decision_id: Option<i64>,
        ) -> i64 {
            conn.execute(
                "INSERT INTO sources (root_id, rel_path, device, inode, size, mtime, partial_hash,
                 basis_rev, scanned_at, last_seen_at, present, excluded, decision_id)
                 VALUES (?, ?, 1, 100, 500, 1600000000, 'oldhash', 5, 0, 0, ?, ?, ?)",
                rusqlite::params![
                    root_id,
                    rel_path,
                    present as i64,
                    excluded as i64,
                    decision_id
                ],
            )
            .unwrap();
            conn.last_insert_rowid()
        }

        /// A New observation at `rel_path` with a fresh inode (a replacement or
        /// a reappeared file, never the tracked inode 100).
        fn revive_observation(root_id: i64, rel_path: &str) -> FileObservation {
            FileObservation {
                root_id,
                rel_path: rel_path.to_string(),
                device: 1,
                inode: 200,
                size: 1024,
                mtime: 1700000000,
                partial_hash: Some("newhash".to_string()),
            }
        }

        #[test]
        fn test_scan_revive_preserves_source_exclusion() {
            // A file reappearing at an excluded path stays excluded, and the row
            // keeps pointing at the excluding decision — undoing a dismissal is
            // exclude clear's recorded act, never a scan's side effect.
            let conn = setup_test_db();
            let root_id = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);
            let row_id =
                insert_row_for_revive(&conn, root_id, "dismissed.jpg", false, true, Some(42));

            let observation = revive_observation(root_id, "dismissed.jpg");
            let source = apply_reconciliation(
                &conn,
                &observation,
                &Reconciliation::New,
                1700000001,
                Some(99),
            )
            .unwrap();

            assert_eq!(source.id, row_id);
            assert!(source.excluded);
            assert_eq!(source.decision_id, Some(42));
            let present: i64 = conn
                .query_row("SELECT present FROM sources WHERE id = ?", [row_id], |r| {
                    r.get(0)
                })
                .unwrap();
            assert_eq!(present, 1);
        }

        #[test]
        fn test_scan_replacement_preserves_source_exclusion() {
            // Replacement in place (same path, new inode — the shape every
            // atomic-save edit takes) keeps the exclusion and its decision_id,
            // exactly as an in-place edit does. It reconciles through the
            // Modified arm: a standing path is never new, whichever way the
            // editor wrote the bytes, and the arm preserves both by omission.
            let conn = setup_test_db();
            let root_id = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);
            let row_id =
                insert_row_for_revive(&conn, root_id, "dismissed.jpg", true, true, Some(42));
            let old_object = insert_object(&conn, "oldcontent", false);
            conn.execute(
                "UPDATE sources SET object_id = ? WHERE id = ?",
                rusqlite::params![old_object, row_id],
            )
            .unwrap();

            let observation = revive_observation(root_id, "dismissed.jpg");
            let source = apply_reconciliation(
                &conn,
                &observation,
                &Reconciliation::Modified {
                    source_id: row_id,
                    old_object_id: None,
                },
                1700000001,
                Some(99),
            )
            .unwrap();

            assert_eq!(source.id, row_id);
            assert!(source.excluded);
            assert_eq!(source.decision_id, Some(42));
            // Content identity is unknown until the hash pass re-establishes it.
            assert_eq!(source.object_id, None);
        }

        #[test]
        fn a_revive_never_reuses_a_basis_rev_it_already_held() {
            // Staleness is read as a difference, so a basis_rev that returns to
            // a value some fact or manifest already recorded reads as fresh
            // when it is not. A row that goes missing and comes back moves
            // forward instead — here from 5 to 6, past every value it held.
            let conn = setup_test_db();
            let root_id = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);
            let row_id =
                insert_row_for_revive(&conn, root_id, "gone-and-back.jpg", false, false, None);

            let observation = revive_observation(root_id, "gone-and-back.jpg");
            let source =
                apply_reconciliation(&conn, &observation, &Reconciliation::New, 1700000001, None)
                    .unwrap();

            assert_eq!(source.id, row_id);
            assert_eq!(source.basis_rev, 6);
        }

        #[test]
        fn a_fresh_insert_starts_at_basis_rev_zero() {
            // Nothing can have recorded a basis for a path Canon has never
            // indexed, so the first revision is free to be 0.
            let conn = setup_test_db();
            let root_id = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);

            let observation = revive_observation(root_id, "first-sighting.jpg");
            let source =
                apply_reconciliation(&conn, &observation, &Reconciliation::New, 1700000001, None)
                    .unwrap();

            assert_eq!(source.basis_rev, 0);
        }

        #[test]
        fn test_scan_revive_excluded_preserves_decision_id_when_disabled() {
            // With recording off, an excluded row's decision_id is preserved,
            // not NULLed — same direction as the deletion path's convention.
            let conn = setup_test_db();
            let root_id = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);
            insert_row_for_revive(&conn, root_id, "dismissed.jpg", false, true, Some(42));

            let observation = revive_observation(root_id, "dismissed.jpg");
            let source =
                apply_reconciliation(&conn, &observation, &Reconciliation::New, 1700000001, None)
                    .unwrap();

            assert!(source.excluded);
            assert_eq!(source.decision_id, Some(42));
        }

        #[test]
        fn test_scan_revive_unexcluded_stamps_decision_id() {
            // The carve-out is the exclusion's alone: an unexcluded revive is a
            // fresh state transition and takes the scan's decision_id as before.
            let conn = setup_test_db();
            let root_id = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);
            insert_row_for_revive(&conn, root_id, "plain.jpg", false, false, Some(42));

            let observation = revive_observation(root_id, "plain.jpg");
            let source = apply_reconciliation(
                &conn,
                &observation,
                &Reconciliation::New,
                1700000001,
                Some(99),
            )
            .unwrap();

            assert!(!source.excluded);
            assert_eq!(source.decision_id, Some(99));
        }

        #[test]
        fn test_scan_unchanged_preserves_decision_id() {
            // Unchanged reconciliation must not overwrite an existing decision_id
            let conn = setup_test_db();
            let root_id = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);

            // Insert with a decision_id
            conn.execute(
                "INSERT INTO sources (root_id, rel_path, device, inode, size, mtime, partial_hash,
                 basis_rev, scanned_at, last_seen_at, present, excluded, decision_id)
                 VALUES (?, 'existing.jpg', 1, 100, 1000, 1704067200, 'hash', 0, 0, 0, 1, 0, 55)",
                rusqlite::params![root_id],
            )
            .unwrap();
            let source_id = conn.last_insert_rowid();

            let observation = FileObservation {
                root_id,
                rel_path: "existing.jpg".to_string(),
                device: 1,
                inode: 100,
                size: 1000,
                mtime: 1704067200,
                partial_hash: None,
            };

            // Pass a different decision_id — Unchanged must ignore it
            let source = apply_reconciliation(
                &conn,
                &observation,
                &Reconciliation::Unchanged { source_id },
                1700000001,
                Some(77),
            )
            .unwrap();

            // Should still be 55 (the original), not 77
            assert_eq!(source.decision_id, Some(55));
        }

        #[test]
        fn test_scan_modified_preserves_decision_id() {
            // Modified reconciliation must not overwrite an existing decision_id
            let conn = setup_test_db();
            let root_id = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);

            conn.execute(
                "INSERT INTO sources (root_id, rel_path, device, inode, size, mtime, partial_hash,
                 basis_rev, scanned_at, last_seen_at, present, excluded, decision_id)
                 VALUES (?, 'file.jpg', 1, 100, 1000, 1700000000, 'oldhash', 2, 0, 0, 1, 0, 33)",
                rusqlite::params![root_id],
            )
            .unwrap();
            let source_id = conn.last_insert_rowid();

            let observation = FileObservation {
                root_id,
                rel_path: "file.jpg".to_string(),
                device: 1,
                inode: 100,
                size: 2048,
                mtime: 1700000100,
                partial_hash: Some("newhash".to_string()),
            };

            let source = apply_reconciliation(
                &conn,
                &observation,
                &Reconciliation::Modified {
                    source_id,
                    old_object_id: None,
                },
                1700000101,
                Some(88),
            )
            .unwrap();

            // decision_id should still be 33 (unchanged by Modified)
            assert_eq!(source.decision_id, Some(33));
        }

        #[test]
        fn test_scan_moved_preserves_decision_id() {
            // Moved reconciliation must not overwrite an existing decision_id
            let conn = setup_test_db();
            let root_id = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);

            conn.execute(
                "INSERT INTO sources (root_id, rel_path, device, inode, size, mtime, partial_hash,
                 basis_rev, scanned_at, last_seen_at, present, excluded, decision_id)
                 VALUES (?, 'origin.jpg', 1, 100, 1000, 1700000000, 'hash', 1, 0, 0, 1, 0, 11)",
                rusqlite::params![root_id],
            )
            .unwrap();
            let source_id = conn.last_insert_rowid();

            let observation = FileObservation {
                root_id,
                rel_path: "destination.jpg".to_string(),
                device: 1,
                inode: 100,
                size: 1000,
                mtime: 1700000000,
                partial_hash: None,
            };

            let source = apply_reconciliation(
                &conn,
                &observation,
                &Reconciliation::Moved {
                    source_id,
                    from_root_id: root_id,
                    from_path: "origin.jpg".to_string(),
                    old_object_id: None,
                },
                1700000001,
                Some(22),
            )
            .unwrap();

            // decision_id should still be 11 (unchanged by Moved)
            assert_eq!(source.decision_id, Some(11));
        }

        // =========================================================================
        // fetch_for_receipt tests
        // =========================================================================

        #[test]
        fn fetch_for_receipt_resolves_hash_and_provenance() {
            let conn = setup_test_db();

            let root_id = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);
            let obj = insert_object(&conn, "abc123", false);
            let id1 = insert_source(&conn, root_id, "vacation/img.jpg", Some(obj), true, false);
            // Seed the pre-flip provenance link the receipt must capture.
            conn.execute(
                "UPDATE sources SET decision_id = 42 WHERE id = ?",
                rusqlite::params![id1],
            )
            .unwrap();

            let rows = fetch_for_receipt(&conn, &[id1]).unwrap();

            assert_eq!(rows.len(), 1);
            let r = &rows[0];
            assert_eq!(r.rel_path, "vacation/img.jpg");
            assert_eq!(r.hash.as_deref(), Some("sha256:abc123"));
            assert_eq!(r.size, 1000);
            assert_eq!(r.mtime, 1704067200);
            assert_eq!(r.previous_decision_id, Some(42));
        }

        #[test]
        fn fetch_for_receipt_unhashed_source_has_no_hash() {
            let conn = setup_test_db();

            let root_id = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);
            let id1 = insert_source(&conn, root_id, "raw.dat", None, true, false);

            let rows = fetch_for_receipt(&conn, &[id1]).unwrap();

            assert_eq!(rows.len(), 1);
            assert!(rows[0].hash.is_none());
            assert!(rows[0].previous_decision_id.is_none());
        }

        #[test]
        fn fetch_for_receipt_returns_present_only() {
            // Protects stamp-set = receipt-set: already-absent rows are never listed,
            // so the receipt matches exactly the sources this transition stamps.
            let conn = setup_test_db();

            let root_id = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);
            let present = insert_source(&conn, root_id, "here.jpg", None, true, false);
            let absent = insert_source(&conn, root_id, "gone.jpg", None, false, false);

            let rows = fetch_for_receipt(&conn, &[present, absent]).unwrap();

            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].rel_path, "here.jpg");
        }

        #[test]
        fn fetch_for_receipt_empty_ids() {
            let conn = setup_test_db();
            let rows = fetch_for_receipt(&conn, &[]).unwrap();
            assert!(rows.is_empty());
        }

        #[test]
        fn fetch_for_receipt_handles_large_batch() {
            // More ids than BATCH_SIZE, so the chunking loop runs more than once.
            // Every call site so far passes a short list, leaving the loop
            // unexercised — but a deleted folder easily exceeds the limit, and all
            // of its sources flow through here to build the deletion receipt.
            let conn = setup_test_db();

            let root_id = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);
            let ids: Vec<i64> = (0..2500)
                .map(|i| insert_source(&conn, root_id, &format!("f{i}.jpg"), None, true, false))
                .collect();

            let rows = fetch_for_receipt(&conn, &ids).unwrap();

            assert_eq!(rows.len(), 2500);

            // Rows come back in no particular order, so index by path and sample
            // from each chunk — a chunk that never ran leaves a hole here.
            let paths: std::collections::HashSet<String> =
                rows.into_iter().map(|r| r.rel_path).collect();
            for idx in [0usize, 1500, 2499] {
                assert!(
                    paths.contains(&format!("f{idx}.jpg")),
                    "source at index {idx} missing from the result"
                );
            }
        }

        // =========================================================================
        // fetch_source_ids_for_root tests
        // =========================================================================

        #[test]
        fn fetch_source_ids_for_root_returns_present_only() {
            let conn = setup_test_db();

            let root_id = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);
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

            let root_id = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);

            let ids = fetch_source_ids_for_root(&conn, root_id, None).unwrap();
            assert!(ids.is_empty());
        }

        #[test]
        fn fetch_source_ids_for_root_with_prefix() {
            let conn = setup_test_db();

            let root_id = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);
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

        #[test]
        fn fetch_source_ids_for_root_prefix_stops_at_the_path_boundary() {
            let conn = setup_test_db();

            let root_id = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);
            let inside = insert_source(&conn, root_id, "alpha/a.jpg", None, true, false);
            let dash_sibling = insert_source(&conn, root_id, "alpha-beta/b.jpg", None, true, false);
            let run_on_sibling = insert_source(&conn, root_id, "alphabet/c.jpg", None, true, false);

            // "alpha" bounds at the separator: neither string-prefix sibling matches.
            let ids = fetch_source_ids_for_root(&conn, root_id, Some("alpha")).unwrap();
            assert_eq!(ids, vec![inside]);
            assert!(!ids.contains(&dash_sibling));
            assert!(!ids.contains(&run_on_sibling));

            // The trailing-slash spelling names the same scope.
            let ids_slash = fetch_source_ids_for_root(&conn, root_id, Some("alpha/")).unwrap();
            assert_eq!(ids_slash, vec![inside]);
        }

        #[test]
        fn fetch_source_ids_for_root_prefix_wildcard_bytes_are_literal() {
            let conn = setup_test_db();

            let root_id = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);
            let under_underscore =
                insert_source(&conn, root_id, "alpha_beta/a.jpg", None, true, false);
            let trap_sibling = insert_source(&conn, root_id, "alphaXbeta/b.jpg", None, true, false);
            let under_percent = insert_source(&conn, root_id, "pct%/c.jpg", None, true, false);
            let percent_trap = insert_source(&conn, root_id, "pct100/d.jpg", None, true, false);

            // '_' and '%' in a scope path are path bytes, not wildcards. The
            // expected set feeds missing detection: a trap sibling swept in
            // here would be flipped absent and receipted as deleted.
            let ids = fetch_source_ids_for_root(&conn, root_id, Some("alpha_beta")).unwrap();
            assert_eq!(ids, vec![under_underscore]);
            assert!(!ids.contains(&trap_sibling));

            let ids = fetch_source_ids_for_root(&conn, root_id, Some("pct%")).unwrap();
            assert_eq!(ids, vec![under_percent]);
            assert!(!ids.contains(&percent_trap));
        }

        // =========================================================================
        // fetch_device_info_by_prefix tests
        // =========================================================================

        #[test]
        fn fetch_device_info_by_prefix_empty_root() {
            let conn = setup_test_db();
            let root_id = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);

            let results = fetch_device_info_by_prefix(&conn, root_id, "").unwrap();
            assert!(results.is_empty());
        }

        #[test]
        fn fetch_device_info_by_prefix_matches_all() {
            let conn = setup_test_db();
            let root_id = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);

            // Insert sources with different devices
            insert_test_source(&conn, root_id, "a/1.jpg", 100, 1, 1000, 1700000000);
            insert_test_source(&conn, root_id, "a/2.jpg", 100, 2, 1000, 1700000000);
            insert_test_source(&conn, root_id, "b/3.jpg", 200, 3, 1000, 1700000000);

            // Empty prefix matches all
            let results = fetch_device_info_by_prefix(&conn, root_id, "").unwrap();
            assert_eq!(results.len(), 3);
        }

        #[test]
        fn fetch_device_info_by_prefix_matches_prefix() {
            let conn = setup_test_db();
            let root_id = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);

            insert_test_source(&conn, root_id, "a/1.jpg", 100, 1, 1000, 1700000000);
            insert_test_source(&conn, root_id, "a/2.jpg", 100, 2, 1000, 1700000000);
            insert_test_source(&conn, root_id, "b/3.jpg", 200, 3, 1000, 1700000000);

            // Prefix "a" matches only files under "a/"
            let results = fetch_device_info_by_prefix(&conn, root_id, "a").unwrap();
            assert_eq!(results.len(), 2);
        }

        #[test]
        fn fetch_device_info_by_prefix_wildcard_bytes_are_literal() {
            let conn = setup_test_db();
            let root_id = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);

            insert_test_source(&conn, root_id, "alpha_beta/1.jpg", 100, 1, 1000, 1700000000);
            insert_test_source(&conn, root_id, "alphaXbeta/2.jpg", 200, 2, 1000, 1700000000);

            // '_' is a path byte, not a wildcard: the trap sibling's device
            // must not leak into the mount-stability read for "alpha_beta".
            let results = fetch_device_info_by_prefix(&conn, root_id, "alpha_beta").unwrap();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].1, Some(100));
        }

        #[test]
        fn fetch_device_info_by_prefix_excludes_not_present() {
            let conn = setup_test_db();
            let root_id = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);

            insert_test_source(&conn, root_id, "a/1.jpg", 100, 1, 1000, 1700000000);
            // Mark as not present
            conn.execute(
                "UPDATE sources SET present = 0 WHERE rel_path = 'a/1.jpg'",
                [],
            )
            .unwrap();

            let results = fetch_device_info_by_prefix(&conn, root_id, "").unwrap();
            assert!(results.is_empty());
        }

        #[test]
        fn fetch_device_info_by_prefix_returns_device() {
            let conn = setup_test_db();
            let root_id = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);

            insert_test_source(&conn, root_id, "a/1.jpg", 12345, 1, 1000, 1700000000);

            let results = fetch_device_info_by_prefix(&conn, root_id, "").unwrap();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].1, Some(12345));
        }
    }
}

pub(crate) mod root {
    use anyhow::Result;

    use super::Connection;
    use crate::core::domain::root::Root;
    use crate::core::repo::root::{root_from_row, ROOT_COLUMNS};

    /// Create a new root in the database.
    ///
    /// # Arguments
    /// * `conn` - Database connection
    /// * `path` - Canonical path of the root directory
    /// * `role` - Role of the root ("source" or "archive")
    /// * `comment` - Optional comment/description
    ///
    /// # Returns
    /// The newly created Root with all fields populated.
    pub fn create(
        conn: &Connection,
        path: &str,
        role: &str,
        comment: Option<&str>,
    ) -> Result<Root> {
        conn.execute(
            "INSERT INTO roots (path, role, comment) VALUES (?, ?, ?)",
            rusqlite::params![path, role, comment],
        )?;
        let id = conn.last_insert_rowid();

        // Fetch the complete Root to ensure consistency with database state.
        // This follows the archive repository's insert_destination() pattern.
        let sql = format!("SELECT {ROOT_COLUMNS} FROM roots WHERE id = ?");
        let root = conn.query_row(&sql, [id], root_from_row)?;
        Ok(root)
    }

    /// Update the last_scanned_at timestamp for a root.
    ///
    /// Called after a full root scan completes (not for subdirectory scans).
    pub fn update_last_scanned_at(conn: &Connection, root_id: i64, timestamp: i64) -> Result<()> {
        conn.execute(
            "UPDATE roots SET last_scanned_at = ? WHERE id = ?",
            rusqlite::params![timestamp, root_id],
        )?;
        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::core::repo::root::fetch_all;
        use crate::scan::repo::tests::setup_test_db;
        use rusqlite::Connection as RusqliteConnection;

        /// Insert a test root and return its ID.
        fn insert_root(
            conn: &RusqliteConnection,
            path: &str,
            role: &str,
            comment: Option<&str>,
            last_scanned_at: Option<i64>,
            suspended: bool,
        ) -> i64 {
            conn.execute(
                "INSERT INTO roots (path, role, comment, last_scanned_at, suspended) VALUES (?, ?, ?, ?, ?)",
                rusqlite::params![path, role, comment, last_scanned_at, suspended as i64],
            )
            .unwrap();
            conn.last_insert_rowid()
        }

        // =========================================================================
        // create tests
        // =========================================================================

        #[test]
        fn create_returns_complete_root() {
            let conn = setup_test_db();

            let root = create(&conn, "/photos", "source", None).unwrap();

            // Verify returned Root has all fields populated correctly
            assert!(root.id > 0);
            assert_eq!(root.path, "/photos");
            assert_eq!(root.role, "source");
            assert_eq!(root.comment, None);
            assert_eq!(root.last_scanned_at, None);
            assert!(!root.suspended);

            // Verify it matches what's in the database
            let roots = fetch_all(&conn).unwrap();
            assert_eq!(roots.len(), 1);
            assert_eq!(roots[0].id, root.id);
        }

        #[test]
        fn create_with_comment() {
            let conn = setup_test_db();

            let root = create(&conn, "/archive", "archive", Some("My archive")).unwrap();

            // Verify returned Root includes comment
            assert_eq!(root.path, "/archive");
            assert_eq!(root.role, "archive");
            assert_eq!(root.comment, Some("My archive".to_string()));

            // Verify it matches what's in the database
            let roots = fetch_all(&conn).unwrap();
            assert_eq!(roots.len(), 1);
            assert_eq!(roots[0].id, root.id);
            assert_eq!(roots[0].comment, Some("My archive".to_string()));
        }

        #[test]
        fn create_multiple_roots() {
            let conn = setup_test_db();

            let root1 = create(&conn, "/photos", "source", None).unwrap();
            let root2 = create(&conn, "/archive", "archive", None).unwrap();

            // Verify different IDs
            assert_ne!(root1.id, root2.id);

            // Verify returned objects have correct data
            assert_eq!(root1.path, "/photos");
            assert_eq!(root2.path, "/archive");

            let roots = fetch_all(&conn).unwrap();
            assert_eq!(roots.len(), 2);
        }

        // =========================================================================
        // update_last_scanned_at tests
        // =========================================================================

        #[test]
        fn update_last_scanned_at_sets_timestamp() {
            let conn = setup_test_db();
            let id = insert_root(&conn, "/photos", "source", None, None, false);

            // Initially None
            let roots = fetch_all(&conn).unwrap();
            assert!(roots[0].last_scanned_at.is_none());

            // Update timestamp
            update_last_scanned_at(&conn, id, 1700000001).unwrap();

            // Verify updated
            let roots = fetch_all(&conn).unwrap();
            assert_eq!(roots[0].last_scanned_at, Some(1700000001));
        }

        #[test]
        fn update_last_scanned_at_overwrites() {
            let conn = setup_test_db();
            let id = insert_root(&conn, "/photos", "source", None, Some(1700000000), false);

            update_last_scanned_at(&conn, id, 1700000001).unwrap();

            let roots = fetch_all(&conn).unwrap();
            assert_eq!(roots[0].last_scanned_at, Some(1700000001));
        }

        #[test]
        fn update_last_scanned_at_nonexistent_root() {
            let conn = setup_test_db();

            // Should not error when root doesn't exist
            let result = update_last_scanned_at(&conn, 99999, 1700000001);
            assert!(result.is_ok());
        }
    }
}

pub(crate) mod fact {
    use anyhow::Result;

    use super::Connection;

    /// Store a fact for an object (upsert).
    ///
    /// Used to record content-derived facts like `content.hash.sha256`.
    /// If the fact already exists, it is updated with the new value and timestamp.
    ///
    /// # Arguments
    /// * `conn` - Database connection
    /// * `object_id` - ID of the object
    /// * `key` - Fact key (e.g., "content.hash.sha256")
    /// * `value` - Fact value (text)
    /// * `timestamp` - When the fact was observed
    pub fn store_object_fact(
        conn: &Connection,
        object_id: i64,
        key: &str,
        value: &str,
        timestamp: i64,
    ) -> Result<()> {
        conn.execute(
            "INSERT INTO facts (entity_type, entity_id, key, value_text, observed_at)
             VALUES ('object', ?, ?, ?, ?)
             ON CONFLICT(entity_type, entity_id, key) DO UPDATE SET
               value_text = excluded.value_text,
               observed_at = excluded.observed_at",
            rusqlite::params![object_id, key, value, timestamp],
        )?;
        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::scan::repo::tests::setup_test_db;

        /// Insert a test object and return its ID.
        fn insert_object(conn: &Connection, id: i64, hash: &str) {
            conn.execute(
                "INSERT INTO objects (id, hash_type, hash_value) VALUES (?1, 'sha256', ?2)",
                [&id as &dyn rusqlite::ToSql, &hash],
            )
            .unwrap();
        }

        // =========================================================================
        // store_object_fact tests
        // =========================================================================

        #[test]
        fn store_object_fact_inserts_new() {
            let conn = setup_test_db();
            insert_object(&conn, 100, "abc123");

            store_object_fact(&conn, 100, "content.hash.sha256", "abc123", 1700000000).unwrap();

            // Verify fact was created
            let value: String = conn.query_row(
                "SELECT value_text FROM facts WHERE entity_type = 'object' AND entity_id = 100 AND key = 'content.hash.sha256'",
                [],
                |row| row.get(0),
            ).unwrap();
            assert_eq!(value, "abc123");
        }

        #[test]
        fn store_object_fact_upserts_existing() {
            let conn = setup_test_db();
            insert_object(&conn, 100, "abc123");

            // Insert initial fact
            store_object_fact(&conn, 100, "content.hash.sha256", "old_hash", 1700000000).unwrap();

            // Upsert with new value
            store_object_fact(&conn, 100, "content.hash.sha256", "new_hash", 1700000001).unwrap();

            // Verify fact was updated
            let (value, timestamp): (String, i64) = conn.query_row(
                "SELECT value_text, observed_at FROM facts WHERE entity_type = 'object' AND entity_id = 100 AND key = 'content.hash.sha256'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            ).unwrap();
            assert_eq!(value, "new_hash");
            assert_eq!(timestamp, 1700000001);

            // Verify only one fact exists
            let count: i64 = conn.query_row(
                "SELECT COUNT(*) FROM facts WHERE entity_type = 'object' AND entity_id = 100 AND key = 'content.hash.sha256'",
                [],
                |row| row.get(0),
            ).unwrap();
            assert_eq!(count, 1);
        }

        #[test]
        fn store_object_fact_different_keys() {
            let conn = setup_test_db();
            insert_object(&conn, 100, "abc123");

            store_object_fact(&conn, 100, "content.hash.sha256", "hash1", 1700000000).unwrap();
            store_object_fact(&conn, 100, "content.Make", "Canon", 1700000000).unwrap();

            // Verify both facts exist
            let count: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM facts WHERE entity_type = 'object' AND entity_id = 100",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(count, 2);
        }
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection as RusqliteConnection;

    /// Create an in-memory database with the full schema.
    pub(super) fn setup_test_db() -> RusqliteConnection {
        crate::core::repo::open_in_memory_for_test()
    }
}
