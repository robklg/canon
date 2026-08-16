//! Retirement's own SQL: the reads no other subsystem performs.
//!
//! Two questions live here. What has already been bound — the decisions
//! carrying an artifact reference, read by the shelf listing and by the
//! lookup that answers whether a given path lies inside a root already
//! retired. And whether the world moved between the review and the release —
//! the two aggregates the ceremony compares against its snapshot.

use anyhow::Result;

use crate::core::repo::Connection;

/// One bound retirement: a decision holding an artifact reference, joined to
/// a scope-row path snapshot of the root it retired. Newest first. Serves the
/// shelf listing and the trail's retired-scope statement; path matching
/// happens in the caller (repo never compares paths).
pub struct BoundRetirementRow {
    pub decision_id: i64,
    pub created_at: i64,
    pub reason: Option<String>,
    /// The retired root's write-time path snapshot.
    pub root_path: String,
    pub receipt_root_id: i64,
    pub receipt_rel_path: String,
}

/// Every decision with the given command that recorded an artifact reference
/// and a scope-row path snapshot — the bound retirements, newest first.
pub fn fetch_bound_retirements(
    conn: &Connection,
    command: &str,
) -> Result<Vec<BoundRetirementRow>> {
    let mut stmt = conn.prepare(
        "SELECT d.id, d.created_at, d.reason, s.root_path,
                d.receipt_root_id, d.receipt_rel_path
         FROM decisions d
         JOIN decision_scopes s ON s.decision_id = d.id
         WHERE d.command = ?1
           AND d.receipt_root_id IS NOT NULL AND d.receipt_rel_path IS NOT NULL
           AND s.root_path IS NOT NULL
         ORDER BY d.created_at DESC, d.id DESC",
    )?;
    let rows = stmt.query_map([command], |row| {
        Ok(BoundRetirementRow {
            decision_id: row.get(0)?,
            created_at: row.get(1)?,
            reason: row.get(2)?,
            root_path: row.get(3)?,
            receipt_root_id: row.get(4)?,
            receipt_rel_path: row.get(5)?,
        })
    })?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
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

/// Highest decision id referenced by anything touching a root — source
/// stamps, scope-index rows, and extraction rows (by origin), the same three
/// tables `fetch_root_story` draws referenced ids from. The other half of the
/// retirement ceremony's world-moved re-check.
///
/// `exclude` drops exactly one id: the ceremony's own decision, which `begin`
/// inserted with a scope row for this root — without the exclusion the check
/// would always trip over itself. A concurrent process's decision has a
/// different id and correctly trips it.
pub fn max_decision_id_touching_root(
    conn: &Connection,
    root_id: i64,
    exclude: Option<i64>,
) -> Result<Option<i64>> {
    // In SQLite `x != NULL` is NULL (filtering everything), so a no-exclusion
    // call passes a sentinel no real rowid can carry.
    let exclude = exclude.unwrap_or(-1);
    let max = conn.query_row(
        "SELECT MAX(m) FROM (
            SELECT MAX(decision_id) AS m FROM sources
             WHERE root_id = ?1 AND decision_id IS NOT NULL AND decision_id != ?2
            UNION ALL
            SELECT MAX(decision_id) FROM decision_scopes
             WHERE root_id = ?1 AND decision_id != ?2
            UNION ALL
            SELECT MAX(decision_id) FROM decision_extractions
             WHERE root_id = ?1 AND decision_id != ?2
         )",
        [root_id, exclude],
        |row| row.get(0),
    )?;
    Ok(max)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::repo::open_in_memory_for_test;

    fn setup_test_db() -> Connection {
        open_in_memory_for_test()
    }

    /// A source row with caller-chosen presence — the counts here span both
    /// presence classes, so tests must be able to make either.
    fn insert_source(conn: &Connection, root_id: i64, rel_path: &str, present: bool) {
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, device, inode, size, mtime,
                                  partial_hash, scanned_at, last_seen_at, present)
             VALUES (?1, ?2, 0, 0, 1000, 1704067200, 'hash', 0, 0, ?3)",
            rusqlite::params![root_id, rel_path, present as i64],
        )
        .unwrap();
    }

    // -----------------------------------------------------------------------
    // count_all_by_root
    // -----------------------------------------------------------------------

    #[test]
    fn count_all_by_root_spans_both_presence_classes() {
        // The world-moved re-check counts what fetch_root_story fetched:
        // present + absent, this root only.
        let conn = setup_test_db();

        let root_id = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);
        let other = crate::core::repo::insert_test_root(&conn, "/other", "source", false);
        insert_source(&conn, root_id, "present.jpg", true);
        insert_source(&conn, root_id, "deleted.jpg", false);
        insert_source(&conn, other, "elsewhere.jpg", true);

        assert_eq!(count_all_by_root(&conn, root_id).unwrap(), 2);
        assert_eq!(count_all_by_root(&conn, other).unwrap(), 1);
        assert_eq!(count_all_by_root(&conn, 999).unwrap(), 0);
    }

    // -----------------------------------------------------------------------
    // max_decision_id_touching_root
    // -----------------------------------------------------------------------

    /// A decision row with a caller-chosen id (the max queries compare raw
    /// ids, so tests pick meaningful ones).
    fn decision_with_id(conn: &Connection, id: i64) {
        conn.execute(
            "INSERT INTO decisions (id, command, command_line, status, canon_version, created_at)
             VALUES (?1, 'scan', 'canon scan', 'completed', '0', 0)",
            [id],
        )
        .unwrap();
    }

    /// A source row on `root_id` stamped with `decision_id` (absent, like the
    /// tombstones a scan stamps — presence is irrelevant to the max).
    fn stamp_source(conn: &Connection, root_id: i64, rel_path: &str, decision_id: i64) {
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, device, inode, size, mtime,
                                  partial_hash, scanned_at, last_seen_at, present, decision_id)
             VALUES (?1, ?2, 0, 0, 100, 0, 'h', 0, 0, 0, ?3)",
            rusqlite::params![root_id, rel_path, decision_id],
        )
        .unwrap();
    }

    fn scope_row(conn: &Connection, decision_id: i64, root_id: i64) {
        conn.execute(
            "INSERT INTO decision_scopes (decision_id, root_id, rel_prefix) VALUES (?1, ?2, '')",
            rusqlite::params![decision_id, root_id],
        )
        .unwrap();
    }

    fn extraction_row(conn: &Connection, decision_id: i64, root_id: i64) {
        conn.execute(
            "INSERT INTO decision_extractions
                (decision_id, root_id, root_path, rel_prefix, files, bytes,
                 destination_root_id, destination_path)
             VALUES (?1, ?2, '/photos', '', 1, 10, 99, '/archive/x')",
            rusqlite::params![decision_id, root_id],
        )
        .unwrap();
    }

    #[test]
    fn max_decision_id_takes_the_max_across_all_three_tables() {
        let conn = setup_test_db();
        let root = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);
        let other = crate::core::repo::insert_test_root(&conn, "/other", "source", false);
        for id in [5, 7, 9, 50] {
            decision_with_id(&conn, id);
        }

        // Each table alone can supply the max.
        stamp_source(&conn, root, "a.jpg", 5);
        assert_eq!(
            max_decision_id_touching_root(&conn, root, None).unwrap(),
            Some(5)
        );
        scope_row(&conn, 7, root);
        assert_eq!(
            max_decision_id_touching_root(&conn, root, None).unwrap(),
            Some(7)
        );
        extraction_row(&conn, 9, root);
        assert_eq!(
            max_decision_id_touching_root(&conn, root, None).unwrap(),
            Some(9)
        );
        // Another root's references never count.
        scope_row(&conn, 50, other);
        assert_eq!(
            max_decision_id_touching_root(&conn, root, None).unwrap(),
            Some(9)
        );
    }

    #[test]
    fn max_decision_id_excludes_exactly_the_given_id() {
        let conn = setup_test_db();
        let root = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);
        for id in [5, 8, 9] {
            decision_with_id(&conn, id);
        }
        stamp_source(&conn, root, "a.jpg", 5);
        scope_row(&conn, 8, root);

        // The excluded id (the ceremony's own decision) disappears...
        assert_eq!(
            max_decision_id_touching_root(&conn, root, Some(8)).unwrap(),
            Some(5)
        );
        // ...but a higher foreign id still wins over the exclusion.
        scope_row(&conn, 9, root);
        assert_eq!(
            max_decision_id_touching_root(&conn, root, Some(8)).unwrap(),
            Some(9)
        );
    }

    #[test]
    fn max_decision_id_empty_root_is_none() {
        let conn = setup_test_db();
        let root = crate::core::repo::insert_test_root(&conn, "/photos", "source", false);
        assert_eq!(
            max_decision_id_touching_root(&conn, root, None).unwrap(),
            None
        );
        // Unstamped sources contribute nothing.
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, device, inode, size, mtime,
                                  partial_hash, scanned_at, last_seen_at)
             VALUES (?1, 'a.jpg', 0, 0, 100, 0, 'h', 0, 0)",
            [root],
        )
        .unwrap();
        assert_eq!(
            max_decision_id_touching_root(&conn, root, None).unwrap(),
            None
        );
    }
}
