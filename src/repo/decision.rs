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
) -> Result<i64> {
    let scope_json = scope.map(|s| serde_json::to_string(s).unwrap());
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs() as i64;

    conn.execute(
        "INSERT INTO decisions (command, scope, command_line, reason, status, canon_version, created_at)
         VALUES (?1, ?2, ?3, ?4, 'started', ?5, ?6)",
        rusqlite::params![command, scope_json, command_line, reason, canon_version, now],
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

/// Fetch a decision by ID. For testing.
#[cfg(test)]
pub fn fetch_by_id(conn: &Connection, id: i64) -> Result<Option<crate::domain::decision::Decision>> {
    let mut stmt = conn.prepare(
        "SELECT id, command, scope, command_line, reason, status,
                count_attempted, count_completed, count_failed, count_skipped,
                summary, canon_version, created_at
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
        let id = insert_started(&conn, "scan", None, "canon scan /photos", None, "0.4.0").unwrap();
        assert!(id > 0);
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
        )
        .unwrap();

        let decision = fetch_by_id(&conn, id).unwrap().unwrap();
        assert_eq!(decision.scope, Some(vec!["/photos".to_string(), "/videos".to_string()]));
    }

    #[test]
    fn insert_started_null_scope() {
        let conn = setup_test_db();
        let id = insert_started(&conn, "import_facts", None, "canon import-facts", None, "0.4.0").unwrap();

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
        )
        .unwrap();

        let decision = fetch_by_id(&conn, id).unwrap().unwrap();
        assert_eq!(decision.reason, Some("OS files".to_string()));
    }

    #[test]
    fn insert_started_null_reason() {
        let conn = setup_test_db();
        let id = insert_started(&conn, "scan", None, "canon scan", None, "0.4.0").unwrap();

        let decision = fetch_by_id(&conn, id).unwrap().unwrap();
        assert!(decision.reason.is_none());
    }

    #[test]
    fn insert_started_status_is_started() {
        let conn = setup_test_db();
        let id = insert_started(&conn, "scan", None, "canon scan", None, "0.4.0").unwrap();

        let decision = fetch_by_id(&conn, id).unwrap().unwrap();
        assert_eq!(decision.status, "started");
    }

    #[test]
    fn update_completed_changes_status() {
        let conn = setup_test_db();
        let id = insert_started(&conn, "scan", None, "canon scan", None, "0.4.0").unwrap();

        update_completed(&conn, id, "completed", Some(100), Some(100), Some(0), None, Some("Scanned 100 files")).unwrap();

        let decision = fetch_by_id(&conn, id).unwrap().unwrap();
        assert_eq!(decision.status, "completed");
    }

    #[test]
    fn update_completed_with_counts() {
        let conn = setup_test_db();
        let id = insert_started(&conn, "apply", None, "canon apply m.lock", None, "0.4.0").unwrap();

        update_completed(&conn, id, "completed", Some(50), Some(48), Some(2), None, Some("Applied")).unwrap();

        let decision = fetch_by_id(&conn, id).unwrap().unwrap();
        assert_eq!(decision.count_attempted, Some(50));
        assert_eq!(decision.count_completed, Some(48));
        assert_eq!(decision.count_failed, Some(2));
        assert!(decision.count_skipped.is_none());
    }

    #[test]
    fn update_completed_null_counts() {
        let conn = setup_test_db();
        let id = insert_started(&conn, "roots_rm", None, "canon roots rm id:1", None, "0.4.0").unwrap();

        update_completed(&conn, id, "completed", None, None, None, None, Some("Removed root 1")).unwrap();

        let decision = fetch_by_id(&conn, id).unwrap().unwrap();
        assert!(decision.count_attempted.is_none());
        assert!(decision.count_completed.is_none());
    }

    #[test]
    fn update_completed_with_summary() {
        let conn = setup_test_db();
        let id = insert_started(&conn, "scan", None, "canon scan", None, "0.4.0").unwrap();

        update_completed(&conn, id, "completed", None, None, None, None, Some("Scanned 50 files: 10 new")).unwrap();

        let decision = fetch_by_id(&conn, id).unwrap().unwrap();
        assert_eq!(decision.summary, Some("Scanned 50 files: 10 new".to_string()));
    }

    #[test]
    fn update_completed_partial_status() {
        let conn = setup_test_db();
        let id = insert_started(&conn, "apply", None, "canon apply m.lock", None, "0.4.0").unwrap();

        update_completed(&conn, id, "partial", Some(47), Some(45), Some(2), None, Some("Applied 45 of 47")).unwrap();

        let decision = fetch_by_id(&conn, id).unwrap().unwrap();
        assert_eq!(decision.status, "partial");
    }
}
