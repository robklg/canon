//! The roots subsystem's repo stratum: the roots-exclusive SQL carved from
//! `core::repo::root`/`notes::repo` unchanged in SQL and signature. Kept as two
//! inner `root`/`note` modules mirroring their origin table.

use crate::core::repo::Connection;

pub(crate) mod root {
    use std::collections::HashMap;

    use anyhow::Result;

    use super::Connection;

    /// Fetch file counts (present sources) for a set of root IDs.
    ///
    /// Returns a HashMap from root_id to count of present sources.
    /// Roots with no present sources will not appear in the result.
    pub fn fetch_file_counts(conn: &Connection, root_ids: &[i64]) -> Result<HashMap<i64, i64>> {
        if root_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let placeholders: Vec<&str> = root_ids.iter().map(|_| "?").collect();
        let sql = format!(
            "SELECT root_id, COUNT(*) FROM sources WHERE present = 1 AND root_id IN ({}) GROUP BY root_id",
            placeholders.join(",")
        );

        let params: Vec<rusqlite::types::Value> = root_ids
            .iter()
            .map(|&id| rusqlite::types::Value::from(id))
            .collect();

        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(params), |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
        })?;

        let mut counts = HashMap::new();
        for row in rows {
            let (root_id, count) = row?;
            counts.insert(root_id, count);
        }

        Ok(counts)
    }

    /// Set the suspended state of a root.
    pub fn set_suspended(conn: &Connection, root_id: i64, suspended: bool) -> Result<()> {
        conn.execute(
            "UPDATE roots SET suspended = ? WHERE id = ?",
            rusqlite::params![suspended as i64, root_id],
        )?;
        Ok(())
    }

    /// Set or clear the comment on a root.
    pub fn set_comment(conn: &Connection, root_id: i64, comment: Option<&str>) -> Result<()> {
        conn.execute(
            "UPDATE roots SET comment = ? WHERE id = ?",
            rusqlite::params![comment, root_id],
        )?;
        Ok(())
    }

    /// Remove a root and all its sources and facts.
    ///
    /// Deletes in order: facts for sources → sources → root.
    /// Returns the number of sources deleted.
    pub fn remove(conn: &Connection, root_id: i64) -> Result<i64> {
        // Delete facts for sources in this root
        conn.execute(
            "DELETE FROM facts WHERE entity_type = 'source' AND entity_id IN (
                SELECT id FROM sources WHERE root_id = ?
            )",
            [root_id],
        )?;

        // Delete sources
        let deleted_sources = conn.execute("DELETE FROM sources WHERE root_id = ?", [root_id])?;

        // Delete the root
        conn.execute("DELETE FROM roots WHERE id = ?", [root_id])?;

        Ok(deleted_sources as i64)
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::core::repo::open_in_memory_for_test;
        use crate::core::repo::root::fetch_all;
        use rusqlite::Connection as RusqliteConnection;

        /// Create an in-memory database with the full schema.
        fn setup_test_db() -> RusqliteConnection {
            open_in_memory_for_test()
        }

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
        // fetch_file_counts tests
        // =========================================================================

        /// Insert a test source and return its ID.
        fn insert_source(
            conn: &RusqliteConnection,
            root_id: i64,
            rel_path: &str,
            present: bool,
        ) -> i64 {
            conn.execute(
                "INSERT INTO sources (root_id, rel_path, present, device, inode, size, mtime, partial_hash, scanned_at, last_seen_at)
                 VALUES (?, ?, ?, 1, 1, 100, 1700000000, 'testhash', 0, 0)",
                rusqlite::params![root_id, rel_path, present as i64],
            )
            .unwrap();
            conn.last_insert_rowid()
        }

        #[test]
        fn fetch_file_counts_empty_ids() {
            let conn = setup_test_db();
            let counts = fetch_file_counts(&conn, &[]).unwrap();
            assert!(counts.is_empty());
        }

        #[test]
        fn fetch_file_counts_root_with_sources() {
            let conn = setup_test_db();
            let root_id = insert_root(&conn, "/photos", "source", None, None, false);

            insert_source(&conn, root_id, "a.jpg", true);
            insert_source(&conn, root_id, "b.jpg", true);
            insert_source(&conn, root_id, "c.jpg", true);

            let counts = fetch_file_counts(&conn, &[root_id]).unwrap();
            assert_eq!(counts.get(&root_id), Some(&3));
        }

        #[test]
        fn fetch_file_counts_root_no_sources() {
            let conn = setup_test_db();
            let root_id = insert_root(&conn, "/photos", "source", None, None, false);

            let counts = fetch_file_counts(&conn, &[root_id]).unwrap();
            // Root with no sources is not in the result
            assert!(!counts.contains_key(&root_id));
        }

        #[test]
        fn fetch_file_counts_multiple_roots() {
            let conn = setup_test_db();
            let root1 = insert_root(&conn, "/photos", "source", None, None, false);
            let root2 = insert_root(&conn, "/archive", "archive", None, None, false);

            insert_source(&conn, root1, "a.jpg", true);
            insert_source(&conn, root1, "b.jpg", true);
            insert_source(&conn, root2, "c.jpg", true);

            let counts = fetch_file_counts(&conn, &[root1, root2]).unwrap();
            assert_eq!(counts.get(&root1), Some(&2));
            assert_eq!(counts.get(&root2), Some(&1));
        }

        #[test]
        fn fetch_file_counts_only_present() {
            let conn = setup_test_db();
            let root_id = insert_root(&conn, "/photos", "source", None, None, false);

            insert_source(&conn, root_id, "a.jpg", true);
            insert_source(&conn, root_id, "b.jpg", true);
            insert_source(&conn, root_id, "missing.jpg", false); // Not present

            let counts = fetch_file_counts(&conn, &[root_id]).unwrap();
            assert_eq!(counts.get(&root_id), Some(&2)); // Only counts present=1
        }

        // =========================================================================
        // set_suspended tests
        // =========================================================================

        #[test]
        fn set_suspended_activates() {
            let conn = setup_test_db();
            let root_id = insert_root(&conn, "/photos", "source", None, None, false);

            set_suspended(&conn, root_id, true).unwrap();

            let roots = fetch_all(&conn).unwrap();
            assert!(roots[0].is_suspended());
        }

        #[test]
        fn set_suspended_deactivates() {
            let conn = setup_test_db();
            let root_id = insert_root(&conn, "/photos", "source", None, None, true);

            set_suspended(&conn, root_id, false).unwrap();

            let roots = fetch_all(&conn).unwrap();
            assert!(!roots[0].is_suspended());
        }

        #[test]
        fn set_suspended_idempotent() {
            let conn = setup_test_db();
            let root_id = insert_root(&conn, "/photos", "source", None, None, true);

            // Setting same value should not error
            set_suspended(&conn, root_id, true).unwrap();

            let roots = fetch_all(&conn).unwrap();
            assert!(roots[0].is_suspended());
        }

        // =========================================================================
        // set_comment tests
        // =========================================================================

        #[test]
        fn set_comment_adds() {
            let conn = setup_test_db();
            let root_id = insert_root(&conn, "/photos", "source", None, None, false);

            set_comment(&conn, root_id, Some("My photos")).unwrap();

            let roots = fetch_all(&conn).unwrap();
            assert_eq!(roots[0].comment, Some("My photos".to_string()));
        }

        #[test]
        fn set_comment_updates() {
            let conn = setup_test_db();
            let root_id = insert_root(&conn, "/photos", "source", Some("Old comment"), None, false);

            set_comment(&conn, root_id, Some("New comment")).unwrap();

            let roots = fetch_all(&conn).unwrap();
            assert_eq!(roots[0].comment, Some("New comment".to_string()));
        }

        #[test]
        fn set_comment_clears() {
            let conn = setup_test_db();
            let root_id = insert_root(&conn, "/photos", "source", Some("Comment"), None, false);

            set_comment(&conn, root_id, None).unwrap();

            let roots = fetch_all(&conn).unwrap();
            assert_eq!(roots[0].comment, None);
        }

        // =========================================================================
        // remove tests
        // =========================================================================

        #[test]
        fn remove_empty_root() {
            let conn = setup_test_db();
            let root_id = insert_root(&conn, "/photos", "source", None, None, false);

            let deleted = remove(&conn, root_id).unwrap();

            assert_eq!(deleted, 0);
            let roots = fetch_all(&conn).unwrap();
            assert!(roots.is_empty());
        }

        #[test]
        fn remove_with_sources() {
            let conn = setup_test_db();
            let root_id = insert_root(&conn, "/photos", "source", None, None, false);

            insert_source(&conn, root_id, "a.jpg", true);
            insert_source(&conn, root_id, "b.jpg", true);
            insert_source(&conn, root_id, "c.jpg", true);

            let deleted = remove(&conn, root_id).unwrap();

            assert_eq!(deleted, 3);
            let roots = fetch_all(&conn).unwrap();
            assert!(roots.is_empty());
        }

        #[test]
        fn remove_deletes_facts() {
            let conn = setup_test_db();
            let root_id = insert_root(&conn, "/photos", "source", None, None, false);
            let source_id = insert_source(&conn, root_id, "a.jpg", true);

            // Insert a fact for this source
            conn.execute(
                "INSERT INTO facts (entity_type, entity_id, key, value_text, observed_at, observed_basis_rev)
                 VALUES ('source', ?, 'test.key', 'test value', 0, 1)",
                [source_id],
            )
            .unwrap();

            // Verify fact exists
            let fact_count: i64 = conn
                .query_row("SELECT COUNT(*) FROM facts", [], |row| row.get(0))
                .unwrap();
            assert_eq!(fact_count, 1);

            remove(&conn, root_id).unwrap();

            // Verify fact was deleted
            let fact_count: i64 = conn
                .query_row("SELECT COUNT(*) FROM facts", [], |row| row.get(0))
                .unwrap();
            assert_eq!(fact_count, 0);
        }

        #[test]
        fn remove_deletes_root() {
            let conn = setup_test_db();
            let root_id = insert_root(&conn, "/photos", "source", None, None, false);

            remove(&conn, root_id).unwrap();

            let roots = fetch_all(&conn).unwrap();
            assert!(roots.is_empty());
        }
    }
}

pub(crate) mod note {
    use anyhow::Result;

    use super::Connection;

    /// Delete all notes for a root. For use in roots rm.
    pub fn delete_by_root(conn: &Connection, root_id: i64) -> Result<usize> {
        let deleted = conn.execute(
            "DELETE FROM notes WHERE root_id = ?",
            rusqlite::params![root_id],
        )?;
        Ok(deleted)
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::notes::fetch_all;
        use crate::ops::test_helpers::{insert_note, insert_root, setup_test_db};

        #[test]
        fn delete_by_root_removes_all_notes() {
            let conn = setup_test_db();
            let root1 = insert_root(&conn, "/photos", "source", false);
            let root2 = insert_root(&conn, "/archive", "archive", false);

            insert_note(&conn, root1, "a", "root1 note", 100);
            insert_note(&conn, root1, "b", "root1 note2", 200);
            insert_note(&conn, root2, "c", "root2 note", 300);

            let count = delete_by_root(&conn, root1).unwrap();
            assert_eq!(count, 2);

            let remaining = fetch_all(&conn).unwrap();
            assert_eq!(remaining.len(), 1);
            assert_eq!(remaining[0].root_id, root2);
        }
    }
}
