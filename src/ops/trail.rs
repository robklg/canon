//! Trail operations — the read side of the decision trail.
//!
//! This is the reader; the recorder lives in `ops/decision.rs` and the two
//! stay separate (the reader never records, the recorder never reads for
//! display). Everything here is served from DB projections — receipt files
//! are never read; `show` returns their locations as pointers only.
//!
//! Read operations: no transactions, no stdio.

use anyhow::Result;
use chrono::{Duration, Local, NaiveDate, TimeZone};

use crate::domain::decision::Decision;
use crate::domain::root::find_containing_root;
use crate::domain::trail::{
    group_by_day, merge_events, scopes_touch, DayGroup, TimelineEvent, WhenValue,
};
use crate::repo::{self, Connection};

/// Default decision-window cap for timeline views.
pub const DEFAULT_LIMIT: usize = 20;

pub struct TrailParams {
    /// Pre-resolved scope prefixes (empty = global). The interface resolves
    /// scope via `ops::scope::resolve_scope` before building params.
    pub prefixes: Vec<String>,
    /// `Some` activates the time lens (day-grouped story view).
    pub timeframe: Option<WhenValue>,
    pub include_notes: bool,
    /// Decision-window cap; `None` = `--all`.
    pub limit: Option<usize>,
}

pub enum TrailView {
    /// Scope lens: chronological (oldest→newest); the cap keeps the most
    /// recent N, so the window ends at now.
    Recent(Vec<TimelineEvent>),
    /// Time lens: days oldest→newest, chronological within each day.
    Days(Vec<DayGroup>),
}

pub struct TrailResult {
    pub view: TrailView,
    /// Decisions beyond the cap (older than the shown window).
    pub earlier_decisions: usize,
    /// Global decisions invisible to this scoped view (0 for global views).
    pub unscoped_decisions: i64,
    /// Matching decisions before capping.
    pub total_decisions: usize,
}

pub fn compute_trail(conn: &Connection, params: &TrailParams) -> Result<TrailResult> {
    let range = params.timeframe.map(when_range);

    let (mut decisions, unscoped, notes) = if params.prefixes.is_empty() {
        let decisions = match range {
            Some((start, end)) => repo::decision::fetch_in_range(conn, start, end)?,
            None => repo::decision::fetch_recent(conn, None)?,
        };
        let notes = if params.include_notes {
            repo::note::fetch_all(conn)?
        } else {
            Vec::new()
        };
        (decisions, 0, notes)
    } else {
        let roots = repo::root::fetch_all(conn)?;
        // The same decomposition the recorder used to populate the index.
        let pairs: Vec<(i64, String)> = params
            .prefixes
            .iter()
            .filter_map(|p| {
                find_containing_root(p, &roots).map(|(root_id, _, _, rel)| (root_id, rel))
            })
            .collect();
        let mut root_ids: Vec<i64> = pairs.iter().map(|(id, _)| *id).collect();
        root_ids.sort_unstable();
        root_ids.dedup();

        let rows = repo::decision::fetch_scope_rows_by_roots(conn, &root_ids)?;
        let mut ids: Vec<i64> = rows
            .iter()
            .filter(|row| {
                pairs
                    .iter()
                    .any(|(rid, rel)| *rid == row.root_id && scopes_touch(rel, &row.rel_prefix))
            })
            .map(|row| row.decision_id)
            .collect();
        ids.sort_unstable();
        ids.dedup();

        let mut decisions = repo::decision::fetch_by_ids(conn, &ids)?;
        if let Some((start, end)) = range {
            decisions.retain(|d| d.created_at >= start && d.created_at < end);
        }
        let unscoped = repo::decision::count_unscoped(conn, range)?;
        let notes = if params.include_notes {
            repo::note::fetch_by_roots(conn, &root_ids)?
                .into_iter()
                .filter(|n| {
                    pairs
                        .iter()
                        .any(|(rid, rel)| *rid == n.root_id && scopes_touch(rel, &n.rel_path))
                })
                .collect()
        } else {
            Vec::new()
        };
        (decisions, unscoped, notes)
    };

    let total_decisions = decisions.len();

    // The cap applies to the decision window and keeps the most recent N.
    decisions.sort_by_key(|d| (d.created_at, d.id));
    let cap = params.limit.unwrap_or(usize::MAX);
    let earlier_decisions = total_decisions.saturating_sub(cap);
    if earlier_decisions > 0 {
        decisions.drain(..earlier_decisions);
    }

    // Notes render within the covered span only — they can never evict
    // decisions from the timeline. A scope with no decisions still shows its
    // notes (the timeline's thought voice survives an actionless place).
    let mut notes = notes;
    let span_start = match (range, decisions.first()) {
        (Some((start, _)), _) => Some(start),
        (None, Some(oldest)) => Some(oldest.created_at),
        (None, None) => None,
    };
    if let Some(start) = span_start {
        notes.retain(|n| n.created_at >= start);
    }
    if let Some((_, end)) = range {
        notes.retain(|n| n.created_at < end);
    }

    let events = merge_events(decisions, notes); // ascending, stable tie-break

    let view = if params.timeframe.is_some() {
        let ids: Vec<i64> = events
            .iter()
            .filter_map(|e| match e {
                TimelineEvent::Decision(d) => Some(d.id),
                TimelineEvent::Note(_) => None,
            })
            .collect();
        let stamps = repo::source::aggregate_stamped_by_decisions(conn, &ids)?;
        let dated: Vec<(NaiveDate, TimelineEvent)> = events
            .into_iter()
            .map(|e| (local_date(e.created_at()), e))
            .collect();
        TrailView::Days(group_by_day(dated, &stamps))
    } else {
        TrailView::Recent(events)
    };

    Ok(TrailResult {
        view,
        earlier_decisions,
        unscoped_decisions: unscoped,
        total_decisions,
    })
}

/// A receipt's on-disk location, as a pointer (contents are never read here).
pub struct ReceiptPointer {
    pub root_display: String,
    pub rel_path: String,
}

pub struct ShowResult {
    pub decision: Decision,
    pub receipts: Vec<ReceiptPointer>,
    /// Why there is no receipt, when there is none — absence is never mute.
    pub receipt_absence: Option<String>,
}

pub fn compute_show(conn: &Connection, id: i64) -> Result<Option<ShowResult>> {
    let Some(decision) = repo::decision::fetch_by_id(conn, id)? else {
        return Ok(None);
    };
    let roots = repo::root::fetch_all(conn)?;
    let root_display = |root_id: i64| {
        roots
            .iter()
            .find(|r| r.id == root_id)
            .map(|r| r.path.clone())
            .unwrap_or_else(|| format!("root #{root_id} (removed)"))
    };

    let mut receipts = Vec::new();
    if let (Some(root_id), Some(rel)) = (decision.receipt_root_id, &decision.receipt_rel_path) {
        receipts.push(ReceiptPointer {
            root_display: root_display(root_id),
            rel_path: rel.clone(),
        });
    }
    // Per-root receipts (e.g. one deletion receipt per source root).
    for row in repo::decision::fetch_scope_rows(conn, id)? {
        if let Some(rel) = row.receipt_rel_path {
            let dup = receipts
                .iter()
                .any(|p| p.rel_path == rel && p.root_display == root_display(row.root_id));
            if !dup {
                receipts.push(ReceiptPointer {
                    root_display: root_display(row.root_id),
                    rel_path: rel,
                });
            }
        }
    }

    let receipt_absence = if receipts.is_empty() {
        // The opt-out is recorded in the command line itself; beyond that the
        // row can't say whether recording mode or placement suppressed it.
        Some(if decision.command_line.contains("--no-receipt") {
            "no receipt (--no-receipt)".to_string()
        } else {
            "no receipt recorded".to_string()
        })
    } else {
        None
    };

    Ok(Some(ShowResult {
        decision,
        receipts,
        receipt_absence,
    }))
}

/// Epoch range [start, end) for a time-lens value, in local time.
fn when_range(when: WhenValue) -> (i64, i64) {
    match when {
        WhenValue::Since(date) => (local_midnight(date), i64::MAX),
        WhenValue::On(date) => (
            local_midnight(date),
            local_midnight(date + Duration::days(1)),
        ),
    }
}

fn local_midnight(date: NaiveDate) -> i64 {
    let naive = date.and_hms_opt(0, 0, 0).unwrap();
    match Local.from_local_datetime(&naive) {
        chrono::LocalResult::Single(dt) => dt.timestamp(),
        // DST gap/fold at midnight: take the earliest valid instant.
        chrono::LocalResult::Ambiguous(dt, _) => dt.timestamp(),
        chrono::LocalResult::None => Local
            .from_local_datetime(&date.and_hms_opt(1, 0, 0).unwrap())
            .earliest()
            .map(|dt| dt.timestamp())
            .unwrap_or_default(),
    }
}

fn local_date(ts: i64) -> NaiveDate {
    Local
        .timestamp_opt(ts, 0)
        .single()
        .map(|dt| dt.date_naive())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::db::open_in_memory_for_test;
    use crate::repo::insert_test_root;

    fn insert_decision_at(conn: &Connection, command: &str, created_at: i64) -> i64 {
        insert_decision_full(conn, command, created_at, &format!("canon {command}"))
    }

    fn insert_decision_full(
        conn: &Connection,
        command: &str,
        created_at: i64,
        command_line: &str,
    ) -> i64 {
        conn.execute(
            "INSERT INTO decisions (command, command_line, status, canon_version, created_at)
             VALUES (?1, ?2, 'completed', 'test', ?3)",
            rusqlite::params![command, command_line, created_at],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn insert_note_at(conn: &Connection, root_id: i64, rel_path: &str, created_at: i64) {
        conn.execute(
            "INSERT INTO notes (root_id, rel_path, text, created_at) VALUES (?1, ?2, 'thought', ?3)",
            rusqlite::params![root_id, rel_path, created_at],
        )
        .unwrap();
    }

    fn scope(conn: &Connection, decision_id: i64, root_id: i64, rel_prefix: &str) {
        repo::decision::insert_scopes(conn, decision_id, &[(root_id, rel_prefix.to_string())])
            .unwrap();
    }

    fn params(prefixes: Vec<String>) -> TrailParams {
        TrailParams {
            prefixes,
            timeframe: None,
            include_notes: true,
            limit: Some(DEFAULT_LIMIT),
        }
    }

    fn decision_ids(view: &TrailView) -> Vec<i64> {
        match view {
            TrailView::Recent(events) => events
                .iter()
                .filter_map(|e| match e {
                    TimelineEvent::Decision(d) => Some(d.id),
                    TimelineEvent::Note(_) => None,
                })
                .collect(),
            TrailView::Days(days) => days
                .iter()
                .flat_map(|g| &g.events)
                .filter_map(|e| match e {
                    TimelineEvent::Decision(d) => Some(d.id),
                    TimelineEvent::Note(_) => None,
                })
                .collect(),
        }
    }

    #[test]
    fn scoped_visibility_is_bidirectional() {
        let conn = open_in_memory_for_test();
        let root = insert_test_root(&conn, "/a", "source", false);
        let on_ancestor = insert_decision_at(&conn, "exclude_set", 100);
        scope(&conn, on_ancestor, root, "x");
        let on_descendant = insert_decision_at(&conn, "exclude_set", 200);
        scope(&conn, on_descendant, root, "x/y/z");
        let sibling = insert_decision_at(&conn, "exclude_set", 300);
        scope(&conn, sibling, root, "w");

        let result = compute_trail(&conn, &params(vec!["/a/x/y".to_string()])).unwrap();
        let ids = decision_ids(&result.view);
        assert!(ids.contains(&on_ancestor)); // decision on ancestor touched here
        assert!(ids.contains(&on_descendant)); // decision below is activity here
        assert!(!ids.contains(&sibling));
    }

    #[test]
    fn global_decisions_counted_not_listed_in_scoped_view() {
        let conn = open_in_memory_for_test();
        let root = insert_test_root(&conn, "/a", "source", false);
        let scoped = insert_decision_at(&conn, "scan", 100);
        scope(&conn, scoped, root, "");
        insert_decision_at(&conn, "import_facts", 200); // no scope rows

        let result = compute_trail(&conn, &params(vec!["/a".to_string()])).unwrap();
        assert_eq!(decision_ids(&result.view), vec![scoped]);
        assert_eq!(result.unscoped_decisions, 1);

        // The global view lists it and reports no unscoped footer.
        let global = compute_trail(&conn, &params(Vec::new())).unwrap();
        assert_eq!(decision_ids(&global.view).len(), 2);
        assert_eq!(global.unscoped_decisions, 0);
    }

    #[test]
    fn cap_keeps_most_recent_and_counts_earlier() {
        let conn = open_in_memory_for_test();
        for ts in [100, 200, 300] {
            insert_decision_at(&conn, "scan", ts);
        }
        let mut p = params(Vec::new());
        p.limit = Some(2);
        let result = compute_trail(&conn, &p).unwrap();
        let ids = decision_ids(&result.view);
        assert_eq!(ids.len(), 2);
        assert_eq!(result.earlier_decisions, 1);
        assert_eq!(result.total_decisions, 3);
        // A timeline reads forward: oldest of the kept window first.
        match &result.view {
            TrailView::Recent(events) => {
                assert!(events[0].created_at() < events[1].created_at());
            }
            TrailView::Days(_) => panic!("scope lens must be Recent"),
        }
    }

    #[test]
    fn notes_interleave_and_can_be_suppressed() {
        let conn = open_in_memory_for_test();
        let root = insert_test_root(&conn, "/a", "source", false);
        let d = insert_decision_at(&conn, "scan", 100);
        scope(&conn, d, root, "");
        insert_note_at(&conn, root, "x", 150);

        let with_notes = compute_trail(&conn, &params(vec!["/a".to_string()])).unwrap();
        match &with_notes.view {
            TrailView::Recent(events) => {
                assert_eq!(events.len(), 2);
                // Chronological: the decision (100) precedes the note (150).
                assert!(matches!(events[0], TimelineEvent::Decision(_)));
                assert!(matches!(events[1], TimelineEvent::Note(_)));
            }
            TrailView::Days(_) => panic!(),
        }

        let mut p = params(vec!["/a".to_string()]);
        p.include_notes = false;
        let without = compute_trail(&conn, &p).unwrap();
        match &without.view {
            TrailView::Recent(events) => assert_eq!(events.len(), 1),
            TrailView::Days(_) => panic!(),
        }
    }

    #[test]
    fn scope_with_only_notes_still_shows_them() {
        let conn = open_in_memory_for_test();
        let root = insert_test_root(&conn, "/a", "source", false);
        insert_note_at(&conn, root, "x", 150);

        let result = compute_trail(&conn, &params(vec!["/a".to_string()])).unwrap();
        match &result.view {
            TrailView::Recent(events) => {
                assert_eq!(events.len(), 1);
                assert!(matches!(events[0], TimelineEvent::Note(_)));
            }
            TrailView::Days(_) => panic!(),
        }
        assert_eq!(result.total_decisions, 0);
    }

    #[test]
    fn time_lens_groups_days_and_rolls_up() {
        let conn = open_in_memory_for_test();
        let root = insert_test_root(&conn, "/a", "source", false);
        // Two dates, epochs built through the same local mapping the
        // implementation uses, so the test is timezone-independent.
        let day1 = NaiveDate::from_ymd_opt(2026, 7, 10).unwrap();
        let day2 = NaiveDate::from_ymd_opt(2026, 7, 11).unwrap();
        let ts1 = local_midnight(day1) + 3600;
        let ts2 = local_midnight(day2) + 3600;

        let scan = insert_decision_at(&conn, "scan", ts1);
        scope(&conn, scan, root, "");
        let apply = insert_decision_at(&conn, "apply", ts2);
        scope(&conn, apply, root, "");

        // Stamp: the scan observed two deletions and one new file.
        let new = crate::repo::source::insert_test_source(&conn, root, "n.jpg", 1, 1, 10, 0);
        let g1 = crate::repo::source::insert_test_source(&conn, root, "g1.jpg", 1, 2, 100, 0);
        let g2 = crate::repo::source::insert_test_source(&conn, root, "g2.jpg", 1, 3, 200, 0);
        for (id, present) in [(new, 1), (g1, 0), (g2, 0)] {
            conn.execute(
                "UPDATE sources SET decision_id = ?, present = ? WHERE id = ?",
                rusqlite::params![scan, present, id],
            )
            .unwrap();
        }

        let mut p = params(vec!["/a".to_string()]);
        p.timeframe = Some(WhenValue::Since(day1));
        let result = compute_trail(&conn, &p).unwrap();
        match &result.view {
            TrailView::Days(days) => {
                assert_eq!(days.len(), 2);
                assert_eq!(days[0].date, day1); // oldest first
                assert_eq!(days[0].rollup.removed.files, 2);
                assert_eq!(days[0].rollup.removed.bytes, Some(300));
                assert_eq!(days[1].date, day2);
            }
            TrailView::Recent(_) => panic!("time lens must be Days"),
        }

        // --on day2 excludes day1.
        p.timeframe = Some(WhenValue::On(day2));
        let result = compute_trail(&conn, &p).unwrap();
        assert_eq!(decision_ids(&result.view), vec![apply]);
    }

    #[test]
    fn show_lists_receipt_pointers_per_root() {
        let conn = open_in_memory_for_test();
        let root = insert_test_root(&conn, "/a", "source", false);
        let d = insert_decision_at(&conn, "scan", 100);
        scope(&conn, d, root, "");
        repo::decision::set_scope_receipt(&conn, d, root, ".canon-ledger/000001-scan.toml")
            .unwrap();
        // A second, since-removed root also wrote a receipt.
        repo::decision::set_scope_receipt(&conn, d, 999, ".canon-ledger/000001-scan.toml").unwrap();

        let show = compute_show(&conn, d).unwrap().unwrap();
        assert_eq!(show.receipts.len(), 2);
        assert_eq!(show.receipts[0].root_display, "/a");
        assert_eq!(show.receipts[1].root_display, "root #999 (removed)");
        assert!(show.receipt_absence.is_none());
    }

    #[test]
    fn show_explains_receipt_absence() {
        let conn = open_in_memory_for_test();
        let quiet = insert_decision_full(
            &conn,
            "exclude_set",
            100,
            "canon exclude set --no-receipt x",
        );
        let plain = insert_decision_at(&conn, "exclude_set", 200);

        let show = compute_show(&conn, quiet).unwrap().unwrap();
        assert_eq!(
            show.receipt_absence.as_deref(),
            Some("no receipt (--no-receipt)")
        );
        let show = compute_show(&conn, plain).unwrap().unwrap();
        assert_eq!(show.receipt_absence.as_deref(), Some("no receipt recorded"));
    }

    #[test]
    fn show_unknown_id_is_none() {
        let conn = open_in_memory_for_test();
        assert!(compute_show(&conn, 12345).unwrap().is_none());
    }

    #[test]
    fn started_decision_appears_in_timeline() {
        let conn = open_in_memory_for_test();
        conn.execute(
            "INSERT INTO decisions (command, command_line, status, canon_version, created_at)
             VALUES ('apply', 'canon apply m.lock', 'started', 'test', 100)",
            [],
        )
        .unwrap();
        let result = compute_trail(&conn, &params(Vec::new())).unwrap();
        match &result.view {
            TrailView::Recent(events) => match &events[0] {
                TimelineEvent::Decision(d) => assert_eq!(d.status, "started"),
                TimelineEvent::Note(_) => panic!(),
            },
            TrailView::Days(_) => panic!(),
        }
    }
}
