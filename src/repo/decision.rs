use anyhow::Result;
use rusqlite::OptionalExtension;
use std::time::{SystemTime, UNIX_EPOCH};

use super::db::Connection;
use super::source::BATCH_SIZE;
use crate::domain::decision::Decision;
use crate::domain::extraction::{DecisionExtraction, OriginDisposition};

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

/// Overwrite the `decisions.scope` display column (JSON array of canonical
/// paths). Used to backfill scopes discovered after `start()` — a `scan --add`
/// root that did not exist when the row was inserted.
pub fn update_scope_display(conn: &Connection, id: i64, scope: &[String]) -> Result<()> {
    let scope_json = serde_json::to_string(scope)?;
    conn.execute(
        "UPDATE decisions SET scope = ?1 WHERE id = ?2",
        rusqlite::params![scope_json, id],
    )?;
    Ok(())
}

/// Ensure durable scope-index rows exist for a decision — one row per `(root_id, rel_prefix)`.
///
/// Powers future subtree-scoped queries ("what decisions touched this path?"). The
/// pairs come from decomposing the decision's resolved scope. Pair count is small
/// (one per scoped root/prefix), so no chunking is needed.
///
/// Idempotent: a `(decision_id, root_id, rel_prefix)` row that already exists is
/// left untouched (its `receipt_rel_path`, if any, is preserved). This lets scan
/// record newly-created roots at completion without duplicating the rows written
/// at `start()`.
pub fn insert_scopes(conn: &Connection, decision_id: i64, pairs: &[(i64, String)]) -> Result<()> {
    for (root_id, rel_prefix) in pairs {
        conn.execute(
            "INSERT INTO decision_scopes (decision_id, root_id, rel_prefix)
             SELECT ?1, ?2, ?3
             WHERE NOT EXISTS (
                 SELECT 1 FROM decision_scopes
                 WHERE decision_id = ?1 AND root_id = ?2 AND rel_prefix = ?3
             )",
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

// ============================================================================
// Read layer (trail consumption)
// ============================================================================

const DECISION_COLUMNS: &str = "id, command, scope, command_line, reason, status,
                count_attempted, count_completed, count_failed, count_skipped,
                summary, canon_version, created_at,
                receipt_root_id, receipt_rel_path";

fn decision_from_row(row: &rusqlite::Row) -> rusqlite::Result<Decision> {
    let scope_json: Option<String> = row.get(2)?;
    let scope = scope_json.map(|s| serde_json::from_str(&s).unwrap());
    Ok(Decision {
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
}

/// Fetch a decision by ID.
pub fn fetch_by_id(conn: &Connection, id: i64) -> Result<Option<Decision>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {DECISION_COLUMNS} FROM decisions WHERE id = ?"
    ))?;
    let result = stmt.query_row([id], decision_from_row).optional()?;
    Ok(result)
}

/// Fetch decisions by IDs (chunked). Order is not guaranteed.
pub fn fetch_by_ids(conn: &Connection, ids: &[i64]) -> Result<Vec<Decision>> {
    let mut decisions = Vec::with_capacity(ids.len());
    for chunk in ids.chunks(BATCH_SIZE) {
        let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
        let sql = format!(
            "SELECT {DECISION_COLUMNS} FROM decisions WHERE id IN ({})",
            placeholders.join(",")
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(chunk.iter()), decision_from_row)?;
        for row in rows {
            decisions.push(row?);
        }
    }
    Ok(decisions)
}

/// Fetch the most recent decisions, newest first. `limit: None` fetches all.
pub fn fetch_recent(conn: &Connection, limit: Option<usize>) -> Result<Vec<Decision>> {
    let sql = match limit {
        Some(n) => format!(
            "SELECT {DECISION_COLUMNS} FROM decisions
             ORDER BY created_at DESC, id DESC LIMIT {n}"
        ),
        None => {
            format!("SELECT {DECISION_COLUMNS} FROM decisions ORDER BY created_at DESC, id DESC")
        }
    };
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], decision_from_row)?;
    let mut decisions = Vec::new();
    for row in rows {
        decisions.push(row?);
    }
    Ok(decisions)
}

/// Fetch every decision with the given command identifier, oldest first.
/// Used by `ledger reindex` to walk all `apply` decisions for backfill.
pub fn fetch_by_command(conn: &Connection, command: &str) -> Result<Vec<Decision>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {DECISION_COLUMNS} FROM decisions WHERE command = ? ORDER BY id ASC"
    ))?;
    let rows = stmt.query_map([command], decision_from_row)?;
    let mut decisions = Vec::new();
    for row in rows {
        decisions.push(row?);
    }
    Ok(decisions)
}

/// Fetch decisions with `start <= created_at < end`, oldest first.
pub fn fetch_in_range(conn: &Connection, start: i64, end: i64) -> Result<Vec<Decision>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {DECISION_COLUMNS} FROM decisions
         WHERE created_at >= ? AND created_at < ?
         ORDER BY created_at ASC, id ASC"
    ))?;
    let rows = stmt.query_map([start, end], decision_from_row)?;
    let mut decisions = Vec::new();
    for row in rows {
        decisions.push(row?);
    }
    Ok(decisions)
}

/// One row of the durable scope index.
#[derive(Debug, Clone)]
pub struct DecisionScopeRow {
    pub decision_id: i64,
    pub root_id: i64,
    pub rel_prefix: String,
    pub receipt_rel_path: Option<String>,
}

fn scope_row_from_row(row: &rusqlite::Row) -> rusqlite::Result<DecisionScopeRow> {
    Ok(DecisionScopeRow {
        decision_id: row.get(0)?,
        root_id: row.get(1)?,
        rel_prefix: row.get(2)?,
        receipt_rel_path: row.get(3)?,
    })
}

/// Fetch all scope-index rows for the given roots (chunked). Prefix matching
/// against a viewed scope is domain logic — SQL never compares paths.
pub fn fetch_scope_rows_by_roots(
    conn: &Connection,
    root_ids: &[i64],
) -> Result<Vec<DecisionScopeRow>> {
    let mut rows_out = Vec::new();
    for chunk in root_ids.chunks(BATCH_SIZE) {
        let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
        let sql = format!(
            "SELECT decision_id, root_id, rel_prefix, receipt_rel_path
             FROM decision_scopes WHERE root_id IN ({})",
            placeholders.join(",")
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(chunk.iter()), scope_row_from_row)?;
        for row in rows {
            rows_out.push(row?);
        }
    }
    Ok(rows_out)
}

/// Fetch the scope-index rows of one decision (per-root receipt pointers).
pub fn fetch_scope_rows(conn: &Connection, decision_id: i64) -> Result<Vec<DecisionScopeRow>> {
    let mut stmt = conn.prepare(
        "SELECT decision_id, root_id, rel_prefix, receipt_rel_path
         FROM decision_scopes WHERE decision_id = ? ORDER BY root_id, rel_prefix",
    )?;
    let rows = stmt.query_map([decision_id], scope_row_from_row)?;
    let mut rows_out = Vec::new();
    for row in rows {
        rows_out.push(row?);
    }
    Ok(rows_out)
}

/// Count all decisions.
pub fn count_all(conn: &Connection) -> Result<i64> {
    let count = conn.query_row("SELECT COUNT(*) FROM decisions", [], |row| row.get(0))?;
    Ok(count)
}

/// Count decisions with no scope-index rows (global operations), optionally
/// restricted to `start <= created_at < end`.
pub fn count_unscoped(conn: &Connection, range: Option<(i64, i64)>) -> Result<i64> {
    let base = "SELECT COUNT(*) FROM decisions d
                WHERE NOT EXISTS (SELECT 1 FROM decision_scopes s WHERE s.decision_id = d.id)";
    let count = match range {
        Some((start, end)) => conn.query_row(
            &format!("{base} AND d.created_at >= ? AND d.created_at < ?"),
            [start, end],
            |row| row.get(0),
        )?,
        None => conn.query_row(base, [], |row| row.get(0))?,
    };
    Ok(count)
}

// ============================================================================
// Extraction ledger (the trail's outbound direction)
// ============================================================================

const EXTRACTION_COLUMNS: &str = "decision_id, root_id, root_path, rel_prefix, files, bytes,
                destination_root_id, destination_path, disposition";

fn extraction_from_row(row: &rusqlite::Row) -> rusqlite::Result<DecisionExtraction> {
    let disposition: Option<String> = row.get(8)?;
    Ok(DecisionExtraction {
        decision_id: row.get(0)?,
        root_id: row.get(1)?,
        root_path: row.get(2)?,
        rel_prefix: row.get(3)?,
        files: row.get(4)?,
        bytes: row.get(5)?,
        destination_root_id: row.get(6)?,
        destination_path: row.get(7)?,
        disposition: disposition.and_then(|s| OriginDisposition::from_str(&s)),
    })
}

/// Replace the extraction rows of every decision the batch covers:
/// delete-then-insert, converging on the batch regardless of the prior row
/// shape. Row-level upsert on the PK would leave a legacy coarse row
/// standing beside freshly precise rows — a silent double count. Each row
/// carries its own `decision_id`, so a batch spanning decisions is
/// representable and no caller-supplied id can disagree with the rows it
/// writes. No transaction here (repo convention) — callers wrap the pair so
/// a concurrent reader never sees a half-replaced decision.
pub fn replace_extractions(conn: &Connection, rows: &[DecisionExtraction]) -> Result<()> {
    let mut ids: Vec<i64> = rows.iter().map(|r| r.decision_id).collect();
    ids.sort_unstable();
    ids.dedup();
    for id in ids {
        conn.execute(
            "DELETE FROM decision_extractions WHERE decision_id = ?1",
            [id],
        )?;
    }
    for row in rows {
        conn.execute(
            "INSERT INTO decision_extractions
                (decision_id, root_id, root_path, rel_prefix, files, bytes,
                 destination_root_id, destination_path, disposition)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            rusqlite::params![
                row.decision_id,
                row.root_id,
                row.root_path,
                row.rel_prefix,
                row.files,
                row.bytes,
                row.destination_root_id,
                row.destination_path,
                row.disposition.map(|d| d.as_str()),
            ],
        )?;
    }
    Ok(())
}

/// Fetch every extraction row. The table is aggregate-only (one row per apply
/// x source root) — tiny by construction, so a full scan keeps path
/// comparison out of SQL (the path-handling law): the caller classifies each
/// row's recorded locations against the viewed scope in domain code. Ordered
/// by `(decision_id, root_id)` so a multi-root decision's lines render in a
/// stable order run to run.
pub fn fetch_all_extractions(conn: &Connection) -> Result<Vec<DecisionExtraction>> {
    let sql = format!(
        "SELECT {EXTRACTION_COLUMNS} FROM decision_extractions ORDER BY decision_id, root_id"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], extraction_from_row)?;
    let mut rows_out = Vec::new();
    for row in rows {
        rows_out.push(row?);
    }
    Ok(rows_out)
}

/// Fetch all extraction rows for the given decisions (chunked). Used to make
/// JSONL output view-independent (extraction fields present regardless of
/// which lens surfaced the decision).
pub fn fetch_extractions_by_decisions(
    conn: &Connection,
    ids: &[i64],
) -> Result<Vec<DecisionExtraction>> {
    let mut rows_out = Vec::new();
    for chunk in ids.chunks(BATCH_SIZE) {
        let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
        let sql = format!(
            "SELECT {EXTRACTION_COLUMNS} FROM decision_extractions WHERE decision_id IN ({})
             ORDER BY decision_id, root_id",
            placeholders.join(",")
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(
            rusqlite::params_from_iter(chunk.iter()),
            extraction_from_row,
        )?;
        for row in rows {
            rows_out.push(row?);
        }
    }
    Ok(rows_out)
}

/// Of the given decision ids, return those with no `decision_scopes` row
/// (chunked). Serves the scoped-trail footer adjustment: decisions surfaced
/// via an extraction row must not also be double-counted as "not shown".
pub fn filter_unscoped_ids(conn: &Connection, ids: &[i64]) -> Result<Vec<i64>> {
    let mut out = Vec::new();
    for chunk in ids.chunks(BATCH_SIZE) {
        let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
        let sql = format!(
            "SELECT id FROM decisions WHERE id IN ({}) AND NOT EXISTS
             (SELECT 1 FROM decision_scopes s WHERE s.decision_id = decisions.id)",
            placeholders.join(",")
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(chunk.iter()), |row| {
            row.get::<_, i64>(0)
        })?;
        for row in rows {
            out.push(row?);
        }
    }
    Ok(out)
}

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
    fn insert_scopes_is_idempotent() {
        // Re-inserting the same pair (e.g. scan re-recording a root's scope at
        // completion that was already written at start) does not duplicate the row.
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

        insert_scopes(&conn, decision_id, &[(3, String::new())]).unwrap();
        insert_scopes(&conn, decision_id, &[(3, String::new())]).unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM decision_scopes WHERE decision_id = ? AND root_id = ?",
                rusqlite::params![decision_id, 3],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn insert_scopes_preserves_existing_receipt_link() {
        // A scope row that already carries a receipt link (set by set_scope_receipt
        // before completion) must survive a later idempotent re-insert of the same pair.
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

        set_scope_receipt(&conn, decision_id, 4, ".canon-ledger/000001-scan.toml").unwrap();
        insert_scopes(&conn, decision_id, &[(4, String::new())]).unwrap();

        let (count, receipt): (i64, Option<String>) = conn
            .query_row(
                "SELECT COUNT(*), MAX(receipt_rel_path) FROM decision_scopes
                 WHERE decision_id = ? AND root_id = ?",
                rusqlite::params![decision_id, 4],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(count, 1);
        assert_eq!(receipt, Some(".canon-ledger/000001-scan.toml".to_string()));
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

    // ------------------------------------------------------------------
    // Read layer
    // ------------------------------------------------------------------

    /// Insert a decision row with a controlled timestamp (read-layer tests
    /// need deterministic created_at; insert_started stamps now()).
    fn insert_decision_at(conn: &Connection, command: &str, created_at: i64) -> i64 {
        conn.execute(
            "INSERT INTO decisions (command, scope, command_line, status, canon_version, created_at)
             VALUES (?1, NULL, ?2, 'completed', 'test', ?3)",
            rusqlite::params![command, format!("canon {command}"), created_at],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    #[test]
    fn fetch_recent_orders_and_limits() {
        let conn = setup_test_db();
        let a = insert_decision_at(&conn, "scan", 100);
        let b = insert_decision_at(&conn, "apply", 300);
        let c = insert_decision_at(&conn, "scan", 200);

        let all = fetch_recent(&conn, None).unwrap();
        assert_eq!(all.iter().map(|d| d.id).collect::<Vec<_>>(), vec![b, c, a]);

        let top = fetch_recent(&conn, Some(2)).unwrap();
        assert_eq!(top.iter().map(|d| d.id).collect::<Vec<_>>(), vec![b, c]);
    }

    #[test]
    fn fetch_by_command_filters_and_orders() {
        let conn = setup_test_db();
        let a = insert_decision_at(&conn, "apply", 100);
        insert_decision_at(&conn, "scan", 150);
        let b = insert_decision_at(&conn, "apply", 200);

        let hits = fetch_by_command(&conn, "apply").unwrap();
        assert_eq!(hits.iter().map(|d| d.id).collect::<Vec<_>>(), vec![a, b]);
    }

    #[test]
    fn fetch_by_command_no_matches_is_empty() {
        let conn = setup_test_db();
        insert_decision_at(&conn, "scan", 100);
        assert!(fetch_by_command(&conn, "apply").unwrap().is_empty());
    }

    #[test]
    fn fetch_in_range_boundaries() {
        let conn = setup_test_db();
        insert_decision_at(&conn, "scan", 99);
        let b = insert_decision_at(&conn, "scan", 100);
        let c = insert_decision_at(&conn, "scan", 150);
        insert_decision_at(&conn, "scan", 200); // end is exclusive

        let hits = fetch_in_range(&conn, 100, 200).unwrap();
        assert_eq!(hits.iter().map(|d| d.id).collect::<Vec<_>>(), vec![b, c]);
    }

    #[test]
    fn fetch_by_ids_chunks_over_batch_size() {
        let conn = setup_test_db();
        let ids: Vec<i64> = (0..1100)
            .map(|i| insert_decision_at(&conn, "scan", i))
            .collect();
        let fetched = fetch_by_ids(&conn, &ids).unwrap();
        assert_eq!(fetched.len(), 1100);
    }

    #[test]
    fn fetch_by_ids_empty() {
        let conn = setup_test_db();
        assert!(fetch_by_ids(&conn, &[]).unwrap().is_empty());
    }

    #[test]
    fn count_unscoped_ignores_scoped_decisions() {
        let conn = setup_test_db();
        let scoped = insert_decision_at(&conn, "scan", 100);
        insert_scopes(&conn, scoped, &[(1, String::new())]).unwrap();
        insert_decision_at(&conn, "import_facts", 150);
        insert_decision_at(&conn, "import_facts", 250);

        assert_eq!(count_unscoped(&conn, None).unwrap(), 2);
        assert_eq!(count_unscoped(&conn, Some((100, 200))).unwrap(), 1);
        assert_eq!(count_all(&conn).unwrap(), 3);
    }

    #[test]
    fn scope_rows_round_trip_with_receipt_path() {
        let conn = setup_test_db();
        let d = insert_decision_at(&conn, "scan", 100);
        insert_scopes(&conn, d, &[(1, "a/b".to_string()), (2, String::new())]).unwrap();
        set_scope_receipt(&conn, d, 2, ".canon-ledger/000001-scan.toml").unwrap();

        let by_root = fetch_scope_rows_by_roots(&conn, &[2]).unwrap();
        assert_eq!(by_root.len(), 1);
        assert_eq!(by_root[0].decision_id, d);
        assert_eq!(by_root[0].rel_prefix, "");
        assert_eq!(
            by_root[0].receipt_rel_path.as_deref(),
            Some(".canon-ledger/000001-scan.toml")
        );

        let for_decision = fetch_scope_rows(&conn, d).unwrap();
        assert_eq!(for_decision.len(), 2);
        assert_eq!(for_decision[0].root_id, 1);
        assert_eq!(for_decision[0].rel_prefix, "a/b");
        assert!(for_decision[0].receipt_rel_path.is_none());
    }

    // ------------------------------------------------------------------
    // Extraction ledger
    // ------------------------------------------------------------------

    fn mk_extraction(decision_id: i64, root_id: i64) -> DecisionExtraction {
        DecisionExtraction {
            decision_id,
            root_id,
            root_path: format!("/root{root_id}"),
            rel_prefix: "2016/italy".to_string(),
            files: 47,
            bytes: Some(3_900_000),
            destination_root_id: Some(9),
            destination_path: "/archive/2016/Italy".to_string(),
            disposition: Some(OriginDisposition::Retained),
        }
    }

    #[test]
    fn replace_extractions_converges_on_repeat() {
        let conn = setup_test_db();
        let d = insert_decision_at(&conn, "apply", 100);
        replace_extractions(&conn, &[mk_extraction(d, 1)]).unwrap();
        // Re-run (e.g. reindex) with a slightly different aggregate.
        let mut updated = mk_extraction(d, 1);
        updated.files = 50;
        replace_extractions(&conn, &[updated]).unwrap();

        let rows = fetch_extractions_by_decisions(&conn, &[d]).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].files, 50);
    }

    #[test]
    fn replace_extractions_removes_rows_the_new_set_does_not_carry() {
        // The precision-upgrade guard: a legacy coarse row replaced by
        // precise rows with *different* PK values must not survive beside
        // them — row-level upsert would keep it and double-count the
        // decision.
        let conn = setup_test_db();
        let d = insert_decision_at(&conn, "apply", 100);
        replace_extractions(&conn, &[mk_extraction(d, 1)]).unwrap();

        let mut p1 = mk_extraction(d, 1);
        p1.rel_prefix = "2016/italy/01".to_string();
        p1.files = 1;
        let mut p2 = mk_extraction(d, 1);
        p2.rel_prefix = "2016/italy/02".to_string();
        p2.files = 2;
        replace_extractions(&conn, &[p1, p2]).unwrap();

        let rows = fetch_extractions_by_decisions(&conn, &[d]).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows.iter().map(|r| r.files).sum::<i64>(), 3);
        assert!(
            rows.iter().all(|r| r.rel_prefix.starts_with("2016/italy/")),
            "the coarse 2016/italy row must be gone"
        );
        // Another decision's rows are untouched by the replacement.
        let other = insert_decision_at(&conn, "apply", 200);
        replace_extractions(&conn, &[mk_extraction(other, 1)]).unwrap();
        assert_eq!(
            fetch_extractions_by_decisions(&conn, &[d]).unwrap().len(),
            2
        );
    }

    #[test]
    fn extraction_pk_permits_multiple_rows_per_decision_and_root() {
        // Directory precision means several rows for one (decision, source
        // root), keyed apart by their placement pair — every fetch path
        // must round-trip them all.
        let conn = setup_test_db();
        let d = insert_decision_at(&conn, "apply", 100);
        let mut a = mk_extraction(d, 1);
        a.destination_path = "/archive/m/01".to_string();
        let mut b = mk_extraction(d, 1);
        b.destination_path = "/archive/m/02".to_string();
        replace_extractions(&conn, &[a, b]).unwrap();

        assert_eq!(
            fetch_extractions_by_decisions(&conn, &[d]).unwrap().len(),
            2
        );
        assert_eq!(fetch_all_extractions(&conn).unwrap().len(), 2);
    }

    #[test]
    fn fetch_extractions_by_decisions_round_trip() {
        let conn = setup_test_db();
        let d1 = insert_decision_at(&conn, "apply", 100);
        let d2 = insert_decision_at(&conn, "apply", 200);
        replace_extractions(&conn, &[mk_extraction(d1, 1), mk_extraction(d1, 2)]).unwrap();
        replace_extractions(&conn, &[mk_extraction(d2, 1)]).unwrap();

        let by_decision = fetch_extractions_by_decisions(&conn, &[d1]).unwrap();
        assert_eq!(by_decision.len(), 2);
        assert!(by_decision.iter().all(|r| r.decision_id == d1));

        let row = &by_decision[0];
        assert_eq!(row.rel_prefix, "2016/italy");
        assert_eq!(row.files, 47);
        assert_eq!(row.bytes, Some(3_900_000));
        assert_eq!(row.destination_root_id, Some(9));
        assert_eq!(row.destination_path, "/archive/2016/Italy");
        assert_eq!(row.disposition, Some(OriginDisposition::Retained));
    }

    #[test]
    fn fetch_extractions_chunks_over_batch_size() {
        let conn = setup_test_db();
        let d = insert_decision_at(&conn, "apply", 100);
        let rows: Vec<DecisionExtraction> = (1..=1100)
            .map(|root_id| mk_extraction(d, root_id))
            .collect();
        replace_extractions(&conn, &rows).unwrap();

        assert_eq!(
            fetch_extractions_by_decisions(&conn, &[d]).unwrap().len(),
            1100
        );
    }

    #[test]
    fn fetch_all_extractions_returns_every_row_ordered() {
        let conn = setup_test_db();
        let d1 = insert_decision_at(&conn, "apply", 100);
        let d2 = insert_decision_at(&conn, "apply", 200);
        replace_extractions(&conn, &[mk_extraction(d1, 2), mk_extraction(d1, 1)]).unwrap();
        replace_extractions(&conn, &[mk_extraction(d2, 1)]).unwrap();

        let rows = fetch_all_extractions(&conn).unwrap();
        let pairs: Vec<(i64, i64)> = rows.iter().map(|r| (r.decision_id, r.root_id)).collect();
        assert_eq!(pairs, vec![(d1, 1), (d1, 2), (d2, 1)]);
    }

    #[test]
    fn fetch_all_extractions_empty_table() {
        let conn = setup_test_db();
        assert!(fetch_all_extractions(&conn).unwrap().is_empty());
    }

    #[test]
    fn filter_unscoped_ids_against_mixed_decisions() {
        let conn = setup_test_db();
        let scoped = insert_decision_at(&conn, "exclude_set", 100);
        insert_scopes(&conn, scoped, &[(1, String::new())]).unwrap();
        let unscoped = insert_decision_at(&conn, "apply", 200);

        let result = filter_unscoped_ids(&conn, &[scoped, unscoped]).unwrap();
        assert_eq!(result, vec![unscoped]);
    }

    #[test]
    fn extraction_disposition_none_round_trips() {
        let conn = setup_test_db();
        let d = insert_decision_at(&conn, "apply", 100);
        let mut row = mk_extraction(d, 1);
        row.disposition = None;
        row.bytes = None;
        replace_extractions(&conn, &[row]).unwrap();

        let fetched = fetch_extractions_by_decisions(&conn, &[d]).unwrap();
        assert_eq!(fetched[0].disposition, None);
        assert_eq!(fetched[0].bytes, None);
    }
}
