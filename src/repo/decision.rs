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
