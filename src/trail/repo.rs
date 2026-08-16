//! Trail's own SQL — the repo stratum. Read-only queries backing the
//! timeline merge, the extraction ledger's whole-table reads, and the
//! scoped-footer adjustments; `decisions`/`decision_scopes`/
//! `decision_extractions` access shared with other subsystems (root_story,
//! ledger reindex, apply) stays in `repo::decision`/`repo::source`.

use std::collections::HashMap;

use anyhow::Result;

use crate::core::domain::decision::Decision;
use crate::core::domain::extraction::{DecisionExtraction, OriginDisposition};
use crate::repo::db::Connection;
use crate::repo::decision::DecisionScopeRow;
use crate::repo::source::BATCH_SIZE;
use crate::trail::domain::timeline::StampAgg;

const DECISION_COLUMNS: &str = "id, command, scope, command_line, reason, status,
                count_attempted, count_completed, count_failed, count_skipped,
                summary, canon_version, created_at,
                receipt_root_id, receipt_rel_path";

fn decision_from_row(row: &rusqlite::Row) -> rusqlite::Result<Decision> {
    let scope_json: Option<String> = row.get(2)?;
    // A scope that doesn't parse reads as no scope rather than failing the
    // whole query — one corrupt row must not take the trail down with it.
    let scope = scope_json.and_then(|s| serde_json::from_str(&s).ok());
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

fn scope_row_from_row(row: &rusqlite::Row) -> rusqlite::Result<DecisionScopeRow> {
    Ok(DecisionScopeRow {
        decision_id: row.get(0)?,
        root_id: row.get(1)?,
        root_path: row.get(2)?,
        rel_prefix: row.get(3)?,
        receipt_rel_path: row.get(4)?,
    })
}

/// Fetch the scope-index rows of one decision (per-root receipt pointers).
pub fn fetch_scope_rows(conn: &Connection, decision_id: i64) -> Result<Vec<DecisionScopeRow>> {
    let mut stmt = conn.prepare(
        "SELECT decision_id, root_id, root_path, rel_prefix, receipt_rel_path
         FROM decision_scopes WHERE decision_id = ? ORDER BY root_id, rel_prefix",
    )?;
    let rows = stmt.query_map([decision_id], scope_row_from_row)?;
    let mut rows_out = Vec::new();
    for row in rows {
        rows_out.push(row?);
    }
    Ok(rows_out)
}

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

/// Aggregate stamped sources per decision, split by the presence axis
/// (chunked). A decision's stamp-set can mix transitions — scan stamps both
/// newly indexed (present) and deleted (absent) sources — so the split is
/// what lets the trail's rollups read each bucket for the right fate.
pub fn aggregate_stamped_by_decisions(
    conn: &Connection,
    decision_ids: &[i64],
) -> Result<HashMap<i64, StampAgg>> {
    let mut aggs: HashMap<i64, StampAgg> = HashMap::new();
    for chunk in decision_ids.chunks(BATCH_SIZE) {
        let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
        let sql = format!(
            "SELECT decision_id, present, COUNT(*), COALESCE(SUM(size), 0)
             FROM sources WHERE decision_id IN ({})
             GROUP BY decision_id, present",
            placeholders.join(",")
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(chunk.iter()), |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })?;
        for row in rows {
            let (decision_id, present, count, bytes) = row?;
            let agg = aggs.entry(decision_id).or_default();
            if present == 1 {
                agg.present_count += count;
                agg.present_bytes += bytes;
            } else {
                agg.absent_count += count;
                agg.absent_bytes += bytes;
            }
        }
    }
    Ok(aggs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::db::open_in_memory_for_test;

    fn setup_test_db() -> Connection {
        open_in_memory_for_test()
    }

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
    fn count_unscoped_ignores_scoped_decisions() {
        let conn = setup_test_db();
        let scoped = insert_decision_at(&conn, "scan", 100);
        crate::repo::decision::insert_scopes(
            &conn,
            scoped,
            &[(1, "/vol/j".to_string(), String::new())],
        )
        .unwrap();
        insert_decision_at(&conn, "import_facts", 150);
        insert_decision_at(&conn, "import_facts", 250);

        assert_eq!(count_unscoped(&conn, None).unwrap(), 2);
        assert_eq!(count_unscoped(&conn, Some((100, 200))).unwrap(), 1);
        // count_all has no production caller — sanity-checked here alongside
        // count_unscoped, per its own doc comment.
        assert_eq!(crate::repo::decision::count_all(&conn).unwrap(), 3);
    }

    #[test]
    fn filter_unscoped_ids_against_mixed_decisions() {
        let conn = setup_test_db();
        let scoped = insert_decision_at(&conn, "exclude_set", 100);
        crate::repo::decision::insert_scopes(
            &conn,
            scoped,
            &[(1, "/vol/m".to_string(), String::new())],
        )
        .unwrap();
        let unscoped = insert_decision_at(&conn, "apply", 200);

        let result = filter_unscoped_ids(&conn, &[scoped, unscoped]).unwrap();
        assert_eq!(result, vec![unscoped]);
    }

    #[test]
    fn scope_rows_round_trip_with_receipt_path() {
        let conn = setup_test_db();
        let d = insert_decision_at(&conn, "scan", 100);
        crate::repo::decision::insert_scopes(
            &conn,
            d,
            &[
                (1, "/vol/k".to_string(), "a/b".to_string()),
                (2, "/vol/l".to_string(), String::new()),
            ],
        )
        .unwrap();
        crate::repo::decision::set_scope_receipt(
            &conn,
            d,
            2,
            "/vol/l",
            ".canon-ledger/000001-scan.toml",
        )
        .unwrap();

        let by_root = crate::repo::decision::fetch_scope_rows_by_roots(&conn, &[2]).unwrap();
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
    fn fetch_all_extractions_returns_every_row_ordered() {
        let conn = setup_test_db();
        let d1 = insert_decision_at(&conn, "apply", 100);
        let d2 = insert_decision_at(&conn, "apply", 200);
        crate::repo::decision::replace_extractions(
            &conn,
            &[mk_extraction(d1, 2), mk_extraction(d1, 1)],
        )
        .unwrap();
        crate::repo::decision::replace_extractions(&conn, &[mk_extraction(d2, 1)]).unwrap();

        let rows = fetch_all_extractions(&conn).unwrap();
        let pairs: Vec<(i64, i64)> = rows.iter().map(|r| (r.decision_id, r.root_id)).collect();
        assert_eq!(pairs, vec![(d1, 1), (d1, 2), (d2, 1)]);
    }

    #[test]
    fn fetch_all_extractions_empty_table() {
        let conn = setup_test_db();
        assert!(fetch_all_extractions(&conn).unwrap().is_empty());
    }

    fn stamp_source(conn: &Connection, source_id: i64, decision_id: i64, present: i64) {
        conn.execute(
            "UPDATE sources SET decision_id = ?, present = ? WHERE id = ?",
            rusqlite::params![decision_id, present, source_id],
        )
        .unwrap();
    }

    #[test]
    fn aggregate_stamped_splits_by_presence() {
        let conn = setup_test_db();
        let root_id = crate::repo::insert_test_root(&conn, "/photos", "source", false);
        // Decision 7: one scan stamping a new file (present) and two deletions.
        let a = crate::repo::source::insert_test_source(&conn, root_id, "new.jpg", 1, 1, 100, 0);
        let b = crate::repo::source::insert_test_source(&conn, root_id, "gone1.jpg", 1, 2, 200, 0);
        let c = crate::repo::source::insert_test_source(&conn, root_id, "gone2.jpg", 1, 3, 300, 0);
        stamp_source(&conn, a, 7, 1);
        stamp_source(&conn, b, 7, 0);
        stamp_source(&conn, c, 7, 0);
        // Decision 8: an apply stamping one destination.
        let d = crate::repo::source::insert_test_source(&conn, root_id, "dest.jpg", 1, 4, 400, 0);
        stamp_source(&conn, d, 8, 1);

        let aggs = aggregate_stamped_by_decisions(&conn, &[7, 8, 9]).unwrap();
        let seven = &aggs[&7];
        assert_eq!(seven.present_count, 1);
        assert_eq!(seven.present_bytes, 100);
        assert_eq!(seven.absent_count, 2);
        assert_eq!(seven.absent_bytes, 500);
        let eight = &aggs[&8];
        assert_eq!(eight.present_count, 1);
        assert_eq!(eight.present_bytes, 400);
        // Decision 9 stamped nothing — absent from the map, not zeroed.
        assert!(!aggs.contains_key(&9));
    }

    #[test]
    fn aggregate_stamped_empty_ids() {
        let conn = setup_test_db();
        assert!(aggregate_stamped_by_decisions(&conn, &[])
            .unwrap()
            .is_empty());
    }
}
