use anyhow::Result;
use std::time::{SystemTime, UNIX_EPOCH};

use super::db::Connection;

/// Insert the initial "started" decision record. Returns the row ID.
pub fn insert_started(
    conn: &Connection,
    command: &str,
    scope: Option<&[String]>,
    command_line: &str,
    reason: Option<&str>,
    canon_version: &str,
    receipt_root_id: Option<i64>,
    receipt_rel_path: Option<&str>,
) -> Result<i64> {
    let scope_json = scope.map(|s| serde_json::to_string(s).unwrap());
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs() as i64;

    conn.execute(
        "INSERT INTO decisions (command, scope, command_line, reason, status, canon_version, created_at,
                                receipt_root_id, receipt_rel_path)
         VALUES (?1, ?2, ?3, ?4, 'started', ?5, ?6, ?7, ?8)",
        rusqlite::params![command, scope_json, command_line, reason, canon_version, now,
                          receipt_root_id, receipt_rel_path],
    )?;

    Ok(conn.last_insert_rowid())
}

/// Update a started record with completion data.
pub fn update_completed(
    conn: &Connection,
    id: i64,
    status: &str,
    count_attempted: Option<i64>,
    count_completed: Option<i64>,
    count_failed: Option<i64>,
    count_skipped: Option<i64>,
    summary: Option<&str>,
) -> Result<()> {
    conn.execute(
        "UPDATE decisions SET status = ?1, count_attempted = ?2, count_completed = ?3,
         count_failed = ?4, count_skipped = ?5, summary = ?6 WHERE id = ?7",
        rusqlite::params![
            status,
            count_attempted,
            count_completed,
            count_failed,
            count_skipped,
            summary,
            id,
        ],
    )?;
    Ok(())
}

/// Update the receipt location on an existing decision record.
///
/// Called after `insert_started` once the decision_id is known and the
/// receipt path has been computed and the directory confirmed writable.
pub fn update_receipt_path(
    conn: &Connection,
    id: i64,
    receipt_root_id: Option<i64>,
    receipt_rel_path: Option<&str>,
) -> Result<()> {
    conn.execute(
        "UPDATE decisions SET receipt_root_id = ?1, receipt_rel_path = ?2 WHERE id = ?3",
        rusqlite::params![receipt_root_id, receipt_rel_path, id],
    )?;
    Ok(())
}

/// Insert durable scope-index rows for a decision — one row per `(root_id, rel_prefix)`.
///
/// Powers future subtree-scoped queries ("what decisions touched this path?"). The
/// pairs come from decomposing the decision's resolved scope. Pair count is small
/// (one per scoped root/prefix), so no chunking is needed.
pub fn insert_scopes(conn: &Connection, decision_id: i64, pairs: &[(i64, String)]) -> Result<()> {
    for (root_id, rel_prefix) in pairs {
        conn.execute(
            "INSERT INTO decision_scopes (decision_id, root_id, rel_prefix) VALUES (?1, ?2, ?3)",
            rusqlite::params![decision_id, root_id, rel_prefix],
        )?;
    }
    Ok(())
}

/// Record the per-root receipt a decision emitted, in the scope index.
///
/// A single decision can write several source-local receipts (one per root that
/// lost files in a scan). Each is linked to its root here so a by-root query
/// finds the decision and its receipt. Upserts: sets `receipt_rel_path` on the
/// existing `(decision_id, root_id)` scope row(s), or inserts a fresh
/// whole-root row when the decision recorded no scope for that root (e.g. a
/// global scan carries no scope entry until this point).
pub fn set_scope_receipt(
    conn: &Connection,
    decision_id: i64,
    root_id: i64,
    rel_path: &str,
) -> Result<()> {
    let updated = conn.execute(
        "UPDATE decision_scopes SET receipt_rel_path = ?3
         WHERE decision_id = ?1 AND root_id = ?2",
        rusqlite::params![decision_id, root_id, rel_path],
    )?;
    if updated == 0 {
        conn.execute(
            "INSERT INTO decision_scopes (decision_id, root_id, rel_prefix, receipt_rel_path)
             VALUES (?1, ?2, '', ?3)",
            rusqlite::params![decision_id, root_id, rel_path],
        )?;
    }
    Ok(())
}

/// Fetch a decision by ID. For testing.
#[cfg(test)]
pub fn fetch_by_id(
    conn: &Connection,
    id: i64,
) -> Result<Option<crate::domain::decision::Decision>> {
    let mut stmt = conn.prepare(
        "SELECT id, command, scope, command_line, reason, status,
                count_attempted, count_completed, count_failed, count_skipped,
                summary, canon_version, created_at,
                receipt_root_id, receipt_rel_path
         FROM decisions WHERE id = ?",
    )?;

    let result = stmt
        .query_row([id], |row| {
            let scope_json: Option<String> = row.get(2)?;
            let scope = scope_json.map(|s| serde_json::from_str(&s).unwrap());
            Ok(crate::domain::decision::Decision {
                id: row.get(0)?,
                command: row.get(1)?,
                scope,
                command_line: row.get(3)?,
                reason: row.get(4)?,
                status: row.get(5)?,
                count_attempted: row.get(6)?,
                count_completed: row.get(7)?,
                count_failed: row.get(8)?,
                count_skipped: row.get(9)?,
                summary: row.get(10)?,
                canon_version: row.get(11)?,
                created_at: row.get(12)?,
                receipt_root_id: row.get(13)?,
                receipt_rel_path: row.get(14)?,
            })
        })
        .optional()?;

    Ok(result)
}

#[cfg(test)]
use rusqlite::OptionalExtension;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::db::open_in_memory_for_test;

    fn setup_test_db() -> Connection {
        open_in_memory_for_test()
    }

    #[test]
    fn insert_started_returns_id() {
        let conn = setup_test_db();
        let id = insert_started(
            &conn,
            "scan",
            None,
            "canon scan /photos",
            None,
            "0.4.0",
            None,
            None,
        )
        .unwrap();
        assert!(id > 0);
    }

    #[test]
    fn test_decisions_receipt_columns_exist() {
        let conn = setup_test_db();
        let id = insert_started(
            &conn,
            "apply",
            None,
            "canon apply m.lock",
            None,
            "0.4.0",
            Some(3),
            Some("000001-apply.toml"),
        )
        .unwrap();
        let d = fetch_by_id(&conn, id).unwrap().unwrap();
        assert_eq!(d.receipt_root_id, Some(3));
        assert_eq!(d.receipt_rel_path, Some("000001-apply.toml".to_string()));
    }

    #[test]
    fn test_decisions_receipt_columns_nullable() {
        let conn = setup_test_db();
        let id =
            insert_started(&conn, "scan", None, "canon scan", None, "0.4.0", None, None).unwrap();
        let d = fetch_by_id(&conn, id).unwrap().unwrap();
        assert_eq!(d.receipt_root_id, None);
        assert_eq!(d.receipt_rel_path, None);
    }

    #[test]
    fn test_decision_scopes_insert_and_select() {
        let conn = setup_test_db();
        let decision_id =
            insert_started(&conn, "scan", None, "canon scan", None, "0.4.0", None, None).unwrap();
        conn.execute(
            "INSERT INTO decision_scopes (decision_id, root_id, rel_prefix) VALUES (?, ?, ?)",
            rusqlite::params![decision_id, 1, "photos"],
        )
        .unwrap();
        let (did, rid, prefix): (i64, i64, String) = conn.query_row(
            "SELECT decision_id, root_id, rel_prefix FROM decision_scopes WHERE decision_id = ?",
            [decision_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        ).unwrap();
        assert_eq!(did, decision_id);
        assert_eq!(rid, 1);
        assert_eq!(prefix, "photos");
    }

    #[test]
    fn test_decision_scopes_multiple_per_decision() {
        let conn = setup_test_db();
        let decision_id = insert_started(
            &conn,
            "exclude_set",
            None,
            "canon exclude set",
            None,
            "0.4.0",
            None,
            None,
        )
        .unwrap();
        conn.execute(
            "INSERT INTO decision_scopes (decision_id, root_id, rel_prefix) VALUES (?, ?, ?)",
            rusqlite::params![decision_id, 1, ""],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO decision_scopes (decision_id, root_id, rel_prefix) VALUES (?, ?, ?)",
            rusqlite::params![decision_id, 2, "photos"],
        )
        .unwrap();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM decision_scopes WHERE decision_id = ?",
                [decision_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn insert_scopes_writes_rows() {
        let conn = setup_test_db();
        let decision_id = insert_started(
            &conn,
            "exclude_set",
            None,
            "canon exclude set",
            None,
            "0.4.0",
            None,
            None,
        )
        .unwrap();

        insert_scopes(
            &conn,
            decision_id,
            &[(1, "photos".to_string()), (2, String::new())],
        )
        .unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM decision_scopes WHERE decision_id = ?",
                [decision_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn set_scope_receipt_updates_existing_scope_row() {
        let conn = setup_test_db();
        let decision_id = insert_started(
            &conn,
            "scan",
            None,
            "canon scan /photos",
            None,
            "0.4.0",
            None,
            None,
        )
        .unwrap();
        insert_scopes(&conn, decision_id, &[(7, "photos".to_string())]).unwrap();

        set_scope_receipt(&conn, decision_id, 7, "000042-scan.toml").unwrap();

        let (prefix, receipt): (String, Option<String>) = conn
            .query_row(
                "SELECT rel_prefix, receipt_rel_path FROM decision_scopes
                 WHERE decision_id = ? AND root_id = ?",
                rusqlite::params![decision_id, 7],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        // Existing scope row is reused (prefix preserved), receipt attached.
        assert_eq!(prefix, "photos");
        assert_eq!(receipt, Some("000042-scan.toml".to_string()));
    }

    #[test]
    fn set_scope_receipt_inserts_when_no_scope_row() {
        // Global scans carry no decision_scopes row for the root until now.
        let conn = setup_test_db();
        let decision_id = insert_started(
            &conn,
            "scan",
            None,
            "canon scan --all",
            None,
            "0.4.0",
            None,
            None,
        )
        .unwrap();

        set_scope_receipt(&conn, decision_id, 3, "000009-scan.toml").unwrap();

        let (prefix, receipt): (String, Option<String>) = conn
            .query_row(
                "SELECT rel_prefix, receipt_rel_path FROM decision_scopes
                 WHERE decision_id = ? AND root_id = ?",
                rusqlite::params![decision_id, 3],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(prefix, "");
        assert_eq!(receipt, Some("000009-scan.toml".to_string()));
    }

    #[test]
    fn set_scope_receipt_is_idempotent() {
        let conn = setup_test_db();
        let decision_id = insert_started(
            &conn,
            "scan",
            None,
            "canon scan --all",
            None,
            "0.4.0",
            None,
            None,
        )
        .unwrap();

        set_scope_receipt(&conn, decision_id, 5, "000001-scan.toml").unwrap();
        set_scope_receipt(&conn, decision_id, 5, "000001-scan.toml").unwrap();

        // Second call updates the row inserted by the first — no duplicate.
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM decision_scopes WHERE decision_id = ? AND root_id = ?",
                rusqlite::params![decision_id, 5],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn set_scope_receipt_by_root_query_finds_decision() {
        // The retirement-shaped query: given a root, find deletion decisions and receipts.
        let conn = setup_test_db();
        let d1 = insert_started(
            &conn,
            "scan",
            None,
            "canon scan --all",
            None,
            "0.4.0",
            None,
            None,
        )
        .unwrap();
        let d2 = insert_started(
            &conn,
            "scan",
            None,
            "canon scan --all",
            None,
            "0.4.0",
            None,
            None,
        )
        .unwrap();
        set_scope_receipt(&conn, d1, 2, "000001-scan.toml").unwrap();
        set_scope_receipt(&conn, d2, 9, "000002-scan.toml").unwrap();

        let (did, receipt): (i64, String) = conn
            .query_row(
                "SELECT decision_id, receipt_rel_path FROM decision_scopes
                 WHERE root_id = ? AND receipt_rel_path IS NOT NULL",
                [2],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(did, d1);
        assert_eq!(receipt, "000001-scan.toml");
    }

    #[test]
    fn insert_started_with_scope() {
        let conn = setup_test_db();
        let scope = vec!["/photos".to_string(), "/videos".to_string()];
        let id = insert_started(
            &conn,
            "exclude_set",
            Some(&scope),
            "canon exclude set --where 'ext=dll'",
            None,
            "0.4.0",
            None,
            None,
        )
        .unwrap();

        let decision = fetch_by_id(&conn, id).unwrap().unwrap();
        assert_eq!(
            decision.scope,
            Some(vec!["/photos".to_string(), "/videos".to_string()])
        );
    }

    #[test]
    fn insert_started_null_scope() {
        let conn = setup_test_db();
        let id = insert_started(
            &conn,
            "import_facts",
            None,
            "canon import-facts",
            None,
            "0.4.0",
            None,
            None,
        )
        .unwrap();

        let decision = fetch_by_id(&conn, id).unwrap().unwrap();
        assert!(decision.scope.is_none());
    }

    #[test]
    fn insert_started_with_reason() {
        let conn = setup_test_db();
        let id = insert_started(
            &conn,
            "exclude_set",
            None,
            "canon exclude set --reason 'OS files'",
            Some("OS files"),
            "0.4.0",
            None,
            None,
        )
        .unwrap();

        let decision = fetch_by_id(&conn, id).unwrap().unwrap();
        assert_eq!(decision.reason, Some("OS files".to_string()));
    }

    #[test]
    fn insert_started_null_reason() {
        let conn = setup_test_db();
        let id =
            insert_started(&conn, "scan", None, "canon scan", None, "0.4.0", None, None).unwrap();

        let decision = fetch_by_id(&conn, id).unwrap().unwrap();
        assert!(decision.reason.is_none());
    }

    #[test]
    fn insert_started_status_is_started() {
        let conn = setup_test_db();
        let id =
            insert_started(&conn, "scan", None, "canon scan", None, "0.4.0", None, None).unwrap();

        let decision = fetch_by_id(&conn, id).unwrap().unwrap();
        assert_eq!(decision.status, "started");
    }

    #[test]
    fn update_completed_changes_status() {
        let conn = setup_test_db();
        let id =
            insert_started(&conn, "scan", None, "canon scan", None, "0.4.0", None, None).unwrap();

        update_completed(
            &conn,
            id,
            "completed",
            Some(100),
            Some(100),
            Some(0),
            None,
            Some("Scanned 100 files"),
        )
        .unwrap();

        let decision = fetch_by_id(&conn, id).unwrap().unwrap();
        assert_eq!(decision.status, "completed");
    }

    #[test]
    fn update_completed_with_counts() {
        let conn = setup_test_db();
        let id = insert_started(
            &conn,
            "apply",
            None,
            "canon apply m.lock",
            None,
            "0.4.0",
            None,
            None,
        )
        .unwrap();

        update_completed(
            &conn,
            id,
            "completed",
            Some(50),
            Some(48),
            Some(2),
            None,
            Some("Applied"),
        )
        .unwrap();

        let decision = fetch_by_id(&conn, id).unwrap().unwrap();
        assert_eq!(decision.count_attempted, Some(50));
        assert_eq!(decision.count_completed, Some(48));
        assert_eq!(decision.count_failed, Some(2));
        assert!(decision.count_skipped.is_none());
    }

    #[test]
    fn update_completed_null_counts() {
        let conn = setup_test_db();
        let id = insert_started(
            &conn,
            "roots_rm",
            None,
            "canon roots rm id:1",
            None,
            "0.4.0",
            None,
            None,
        )
        .unwrap();

        update_completed(
            &conn,
            id,
            "completed",
            None,
            None,
            None,
            None,
            Some("Removed root 1"),
        )
        .unwrap();

        let decision = fetch_by_id(&conn, id).unwrap().unwrap();
        assert!(decision.count_attempted.is_none());
        assert!(decision.count_completed.is_none());
    }

    #[test]
    fn update_completed_with_summary() {
        let conn = setup_test_db();
        let id =
            insert_started(&conn, "scan", None, "canon scan", None, "0.4.0", None, None).unwrap();

        update_completed(
            &conn,
            id,
            "completed",
            None,
            None,
            None,
            None,
            Some("Scanned 50 files: 10 new"),
        )
        .unwrap();

        let decision = fetch_by_id(&conn, id).unwrap().unwrap();
        assert_eq!(
            decision.summary,
            Some("Scanned 50 files: 10 new".to_string())
        );
    }

    #[test]
    fn update_completed_partial_status() {
        let conn = setup_test_db();
        let id = insert_started(
            &conn,
            "apply",
            None,
            "canon apply m.lock",
            None,
            "0.4.0",
            None,
            None,
        )
        .unwrap();

        update_completed(
            &conn,
            id,
            "partial",
            Some(47),
            Some(45),
            Some(2),
            None,
            Some("Applied 45 of 47"),
        )
        .unwrap();

        let decision = fetch_by_id(&conn, id).unwrap().unwrap();
        assert_eq!(decision.status, "partial");
    }
}
