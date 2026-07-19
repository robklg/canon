//! Trail operations — the read side of the decision trail.
//!
//! This is the reader; the recorder lives in `ops/decision.rs` and the two
//! stay separate (the reader never records, the recorder never reads for
//! display). Everything here is served from DB projections — receipt files
//! are never read; `show` returns their locations as pointers only.
//!
//! Read operations: no transactions, no stdio.

use std::collections::{HashMap, HashSet};

use anyhow::Result;
use chrono::{Duration, Local, NaiveDate, TimeZone};

use crate::domain::decision::Decision;
use crate::domain::extraction::DecisionExtraction;
use crate::domain::root::find_containing_root;
use crate::domain::trail::{
    group_by_day, merge_events, row_aspect, scopes_touch, DayGroup, RowAspect, TimelineEvent,
    WhenValue,
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

/// Whole-history rollup over a scoped view's extraction-touching rows, before
/// the decision-window cap — "Archived from here" answers "where am I with
/// this drive?", not "what did the last N decisions do?".
pub struct ExtractionRollup {
    pub files: i64,
    /// `None` if any contributing row lacks a size — never a partial sum.
    pub bytes: Option<i64>,
    pub destinations: usize,
}

/// Whole-history rollup over a scoped view's arrival-touching rows, before the
/// decision-window cap — the mirror of `ExtractionRollup` for the inbound
/// direction: "Arrived here" answers "where am I with this archive?".
pub struct ArrivalRollup {
    pub files: i64,
    /// `None` if any contributing row lacks a size — never a partial sum.
    pub bytes: Option<i64>,
    /// Distinct origins by `root_path` (the drawn-from side).
    pub origins: usize,
}

/// Whole-history rollup over rows that crossed no boundary — content this
/// view's own roots moved within it, an intra-archive curation pass.
///
/// Deliberately has no counterparty count: "Archived from here" names where
/// content went and "Arrived here" where it came from, but a rearrangement's
/// counterparty *is* this place, so counting it would say nothing.
pub struct RearrangementRollup {
    pub files: i64,
    /// `None` if any contributing row lacks a size — never a partial sum.
    /// Evaluated over this rollup's own rows: an unknown-size crossing must
    /// not suppress a fully known rearrangement total.
    pub bytes: Option<i64>,
}

pub struct TrailResult {
    pub view: TrailView,
    /// Decisions beyond the cap (older than the shown window).
    pub earlier_decisions: usize,
    /// Global decisions invisible to this scoped view (0 for global views).
    pub unscoped_decisions: i64,
    /// Matching decisions before capping.
    pub total_decisions: usize,
    /// Extraction rows touching this view, by decision id — content this
    /// scope's roots drew content into an archive from. Powers the
    /// extraction-aspect line and the rollup below. Empty for the global
    /// view (nothing "touches" a scope that doesn't exist).
    pub extractions: HashMap<i64, Vec<DecisionExtraction>>,
    /// `None` when there are no touching rows, or the view is global or a
    /// time-lens view (the rollup is a scope-lens-only footer).
    pub extraction_rollup: Option<ExtractionRollup>,
    /// Extraction rows whose recorded *destination* touches this view, by
    /// decision id — content this scope's roots received from an apply, even
    /// when the apply's own selection scope was elsewhere. Matched on
    /// absolute snapshot paths, not root ids, so a removed or re-added
    /// destination root doesn't break the link. Empty for the global view.
    pub arrivals: HashMap<i64, Vec<DecisionExtraction>>,
    /// `None` when there are no touching rows, or the view is global or a
    /// time-lens view (the rollup is a scope-lens-only footer).
    pub arrival_rollup: Option<ArrivalRollup>,
    /// `None` when no row has both endpoints inside this view, or the view is
    /// global or a time-lens view.
    pub rearrangement_rollup: Option<RearrangementRollup>,
    /// The *full* (not touching-filtered) extraction rows for every decision
    /// in the final listed view, across every lens and scope — a decision's
    /// JSONL extraction data must read the same regardless of which view
    /// surfaced it.
    pub extractions_all: HashMap<i64, Vec<DecisionExtraction>>,
}

pub fn compute_trail(conn: &Connection, params: &TrailParams) -> Result<TrailResult> {
    let range = params.timeframe.map(when_range);

    let (mut decisions, unscoped, notes, extractions, arrivals, rollups) =
        if params.prefixes.is_empty() {
            let decisions = match range {
                Some((start, end)) => repo::decision::fetch_in_range(conn, start, end)?,
                None => repo::decision::fetch_recent(conn, None)?,
            };
            let notes = if params.include_notes {
                repo::note::fetch_all(conn)?
            } else {
                Vec::new()
            };
            (
                decisions,
                0,
                notes,
                HashMap::new(),
                HashMap::new(),
                Rollups::default(),
            )
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

            let touches = |root_id: i64, rel_prefix: &str| {
                pairs
                    .iter()
                    .any(|(rid, rel)| *rid == root_id && scopes_touch(rel, rel_prefix))
            };

            let rows = repo::decision::fetch_scope_rows_by_roots(conn, &root_ids)?;
            let mut ids: Vec<i64> = rows
                .iter()
                .filter(|row| touches(row.root_id, &row.rel_prefix))
                .map(|row| row.decision_id)
                .collect();

            // Extraction rows touching this view: apply decisions that drew
            // content from here into an archive, even when the decision's
            // *selection* scope was global or elsewhere entirely.
            let ext_rows = repo::decision::fetch_extractions_by_roots(conn, &root_ids)?;
            let mut extractions: HashMap<i64, Vec<DecisionExtraction>> = HashMap::new();
            for row in ext_rows {
                if touches(row.root_id, &row.rel_prefix) {
                    extractions.entry(row.decision_id).or_default().push(row);
                }
            }

            // Arrival rows touching this view: apply decisions whose recorded
            // *destination* lands here, even when the decision's source root
            // is unrelated to this scope's roots. Matched on absolute
            // snapshot paths (params.prefixes are already absolute), not root
            // ids — a removed or re-added destination root can't break this.
            let all_ext_rows = repo::decision::fetch_all_extractions(conn)?;
            let mut arrivals: HashMap<i64, Vec<DecisionExtraction>> = HashMap::new();
            for row in all_ext_rows {
                if params
                    .prefixes
                    .iter()
                    .any(|prefix| scopes_touch(prefix, &row.destination_path))
                {
                    arrivals.entry(row.decision_id).or_default().push(row);
                }
            }

            ids.extend(extractions.keys().copied());
            ids.extend(arrivals.keys().copied());
            ids.sort_unstable();
            ids.dedup();

            let mut decisions = repo::decision::fetch_by_ids(conn, &ids)?;
            if let Some((start, end)) = range {
                decisions.retain(|d| d.created_at >= start && d.created_at < end);
            }

            let unscoped_raw = repo::decision::count_unscoped(conn, range)?;
            // Footer honesty: a decision surfaced here only via an extraction
            // or arrival row (no decision_scopes row of its own) must not
            // also be counted as "not shown" — restricted to ids that
            // actually survived the time-range filter above, since a
            // touching id outside --since/--on was never part of
            // unscoped_raw's count either.
            let shown_ids: HashSet<i64> = decisions.iter().map(|d| d.id).collect();
            let shown_extraction_ids: Vec<i64> = extractions
                .keys()
                .chain(arrivals.keys())
                .filter(|id| shown_ids.contains(id))
                .copied()
                .collect::<HashSet<i64>>()
                .into_iter()
                .collect();
            let unscoped_adjustment =
                repo::decision::filter_unscoped_ids(conn, &shown_extraction_ids)?.len() as i64;
            let unscoped = unscoped_raw - unscoped_adjustment;

            let notes = if params.include_notes {
                repo::note::fetch_by_roots(conn, &root_ids)?
                    .into_iter()
                    .filter(|n| touches(n.root_id, &n.rel_path))
                    .collect()
            } else {
                Vec::new()
            };

            // Whole-history rollups: every touching row, never capped by the
            // decision-window limit. Scope-lens only — never a time-lens view.
            let rollups = if range.is_none() {
                build_rollups(&extractions, &arrivals)
            } else {
                Rollups::default()
            };

            (decisions, unscoped, notes, extractions, arrivals, rollups)
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

    // JSONL completeness: the full extraction rows (not touching-filtered)
    // for every decision that ends up listed, across every lens and scope —
    // all views, global included — so machine output never varies by view.
    let listed_ids: Vec<i64> = decisions.iter().map(|d| d.id).collect();
    let extractions_all = group_extractions_by_decision(
        repo::decision::fetch_extractions_by_decisions(conn, &listed_ids)?,
    );

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
        extractions,
        extraction_rollup: rollups.extraction,
        arrivals,
        arrival_rollup: rollups.arrival,
        rearrangement_rollup: rollups.rearrangement,
        extractions_all,
    })
}

fn group_extractions_by_decision(
    rows: Vec<DecisionExtraction>,
) -> HashMap<i64, Vec<DecisionExtraction>> {
    let mut map: HashMap<i64, Vec<DecisionExtraction>> = HashMap::new();
    for row in rows {
        map.entry(row.decision_id).or_default().push(row);
    }
    map
}

/// Every extraction row a decision renders in this view, each tagged with the
/// direction it reads from here.
///
/// Classification is **per row, not per decision**: one apply can draw from a
/// root inside the view and from another outside it in the same breath, so a
/// decision appearing in both maps is not necessarily a pure rearrangement —
/// classifying it as one would drop the outside-origin rows entirely.
///
/// This lives in ops because ops owns the two maps and the membership they
/// encode: a row in `extractions` was fetched by root id, so its origin is in
/// the view by construction, and likewise a row in `arrivals` for its
/// destination. Rows are matched between the maps by `root_id` (the extraction
/// ledger's `(decision_id, root_id)` key); the rule itself is
/// [`domain::trail::row_aspect`]. `Outside` is unreachable here — a row absent
/// from both maps was never fetched.
pub fn classify_decision_rows<'a>(
    id: i64,
    extractions: &'a HashMap<i64, Vec<DecisionExtraction>>,
    arrivals: &'a HashMap<i64, Vec<DecisionExtraction>>,
) -> Vec<(&'a DecisionExtraction, RowAspect)> {
    let outbound = extractions.get(&id).map(Vec::as_slice).unwrap_or_default();
    let inbound = arrivals.get(&id).map(Vec::as_slice).unwrap_or_default();
    let arrived_here: HashSet<i64> = inbound.iter().map(|row| row.root_id).collect();
    let drawn_from_here: HashSet<i64> = outbound.iter().map(|row| row.root_id).collect();

    outbound
        .iter()
        .map(|row| (row, row_aspect(true, arrived_here.contains(&row.root_id))))
        .chain(
            inbound
                .iter()
                .filter(|row| !drawn_from_here.contains(&row.root_id))
                .map(|row| (row, row_aspect(false, true))),
        )
        .collect()
}

/// Sum files/bytes and count distinct counterparties over a set of rows.
/// `None` if the set is empty; bytes `None` if any row lacks a size.
///
/// One builder for all three rollups: the all-or-omitted bytes rule is the
/// same rule in each, and three copies of it would be three places to fix.
/// They differ only in which end of the row is the counterparty — and the
/// rearrangement rollup, whose counterparty is this place, discards it.
fn rollup_parts<'a>(
    rows: impl Iterator<Item = &'a DecisionExtraction>,
    counterparty: fn(&DecisionExtraction) -> &str,
) -> Option<(i64, Option<i64>, usize)> {
    let rows: Vec<&DecisionExtraction> = rows.collect();
    if rows.is_empty() {
        return None;
    }
    let files: i64 = rows.iter().map(|r| r.files).sum();
    let bytes = if rows.iter().all(|r| r.bytes.is_some()) {
        Some(rows.iter().filter_map(|r| r.bytes).sum())
    } else {
        None
    };
    let counterparties = rows
        .iter()
        .map(|r| counterparty(r))
        .collect::<HashSet<_>>()
        .len();
    Some((files, bytes, counterparties))
}

/// "Archived from here": content that left, by where it went.
fn build_extraction_rollup<'a>(
    rows: impl Iterator<Item = &'a DecisionExtraction>,
) -> Option<ExtractionRollup> {
    rollup_parts(rows, |r| &r.destination_path).map(|(files, bytes, destinations)| {
        ExtractionRollup {
            files,
            bytes,
            destinations,
        }
    })
}

/// "Arrived here": content that entered, by where it came from.
fn build_arrival_rollup<'a>(
    rows: impl Iterator<Item = &'a DecisionExtraction>,
) -> Option<ArrivalRollup> {
    rollup_parts(rows, |r| &r.root_path).map(|(files, bytes, origins)| ArrivalRollup {
        files,
        bytes,
        origins,
    })
}

/// "Rearranged here": content that crossed nothing. The counterparty count is
/// computed and dropped — the counterparty is this place, so counting it would
/// be self-referential.
fn build_rearrangement_rollup<'a>(
    rows: impl Iterator<Item = &'a DecisionExtraction>,
) -> Option<RearrangementRollup> {
    rollup_parts(rows, |r| &r.destination_path)
        .map(|(files, bytes, _)| RearrangementRollup { files, bytes })
}

/// The three whole-history rollups of a scoped scope-lens view, over three
/// disjoint row sets. Every field is `None` for a global or time-lens view.
#[derive(Default)]
struct Rollups {
    extraction: Option<ExtractionRollup>,
    arrival: Option<ArrivalRollup>,
    rearrangement: Option<RearrangementRollup>,
}

/// Partition every row the view can see by which boundary it crossed, then
/// build each rollup from its own set.
///
/// Partitioning is **per row**: one apply drawing from inside the view and
/// from outside it contributes to the arrival rollup *and* the rearrangement
/// rollup at once. The sets are disjoint, so no row is counted twice — which
/// is the whole point, since a rearrangement used to be claimed by both
/// crossing rollups and read as double the activity.
fn build_rollups(
    extractions: &HashMap<i64, Vec<DecisionExtraction>>,
    arrivals: &HashMap<i64, Vec<DecisionExtraction>>,
) -> Rollups {
    let mut ids: Vec<i64> = extractions.keys().chain(arrivals.keys()).copied().collect();
    ids.sort_unstable();
    ids.dedup();

    let (mut left, mut entered, mut stayed) = (Vec::new(), Vec::new(), Vec::new());
    for id in ids {
        for (row, aspect) in classify_decision_rows(id, extractions, arrivals) {
            match aspect {
                RowAspect::Extraction => left.push(row),
                RowAspect::Arrival => entered.push(row),
                RowAspect::Rearrangement => stayed.push(row),
                // Unreachable: every row here came from one of the two maps.
                RowAspect::Outside => {}
            }
        }
    }

    Rollups {
        extraction: build_extraction_rollup(left.into_iter()),
        arrival: build_arrival_rollup(entered.into_iter()),
        rearrangement: build_rearrangement_rollup(stayed.into_iter()),
    }
}

/// A receipt's on-disk location, as a pointer (contents are never read here).
pub struct ReceiptPointer {
    pub root_display: String,
    pub rel_path: String,
}

/// One `drew from:` line: the extraction row plus whether its source root is
/// still known to the index. Liveness is derived at read time from the live
/// roots list — never stored in the row (the snapshot records what happened;
/// the marker says what the index knows now).
pub struct ShowExtraction {
    pub row: DecisionExtraction,
    pub root_removed: bool,
}

pub struct ShowResult {
    pub decision: Decision,
    pub receipts: Vec<ReceiptPointer>,
    /// Why there is no receipt, when there is none — absence is never mute.
    pub receipt_absence: Option<String>,
    /// What this decision drew from each source root, if any (the extraction
    /// ledger's per-decision view — the source side of an apply).
    pub extractions: Vec<ShowExtraction>,
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

    let extractions = repo::decision::fetch_extractions_by_decisions(conn, &[id])?
        .into_iter()
        .map(|row| ShowExtraction {
            root_removed: row.origin_root_removed(roots.iter().map(|r| r.path.as_str())),
            row,
        })
        .collect();

    Ok(Some(ShowResult {
        decision,
        receipts,
        receipt_absence,
        extractions,
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

    // ------------------------------------------------------------------
    // Extraction ledger visibility (Story 1/2)
    // ------------------------------------------------------------------

    fn extraction_row(
        decision_id: i64,
        root_id: i64,
        root_path: &str,
        rel_prefix: &str,
        files: i64,
        bytes: Option<i64>,
        destination_path: &str,
    ) -> crate::domain::extraction::DecisionExtraction {
        crate::domain::extraction::DecisionExtraction {
            decision_id,
            root_id,
            root_path: root_path.to_string(),
            rel_prefix: rel_prefix.to_string(),
            files,
            bytes,
            destination_root_id: Some(999),
            destination_path: destination_path.to_string(),
            disposition: Some(crate::domain::extraction::OriginDisposition::Retained),
        }
    }

    #[test]
    fn classify_decision_rows_judges_each_row_by_its_own_endpoints() {
        // Per row, not per decision. One apply reaches three ways at once, and
        // each row is judged by its own two endpoints:
        //   - `inside`  drawn from the view, landing in it  -> rearrangement
        //   - `left`    drawn from the view, landing outside -> extraction
        //   - `outside` drawn from elsewhere, landing here   -> arrival
        // Any classification that asks "is this decision in both maps?"
        // instead of asking it of each row gets `left` wrong and drops
        // `outside` entirely.
        let inside = extraction_row(42, 1, "/archive", "2016", 47, Some(10), "/archive/2020");
        let left = extraction_row(42, 3, "/archive", "raw", 5, Some(1), "/elsewhere");
        let outside = extraction_row(42, 2, "/Volumes/sd", "dcim", 8, Some(2), "/archive/2020");

        let extractions = HashMap::from([(42, vec![inside.clone(), left.clone()])]);
        let arrivals = HashMap::from([(42, vec![inside.clone(), outside.clone()])]);

        assert_eq!(
            classify_decision_rows(42, &extractions, &arrivals),
            vec![
                (&inside, RowAspect::Rearrangement),
                (&left, RowAspect::Extraction),
                (&outside, RowAspect::Arrival)
            ]
        );
    }

    #[test]
    fn classify_decision_rows_classifies_one_directional_decisions() {
        let row = extraction_row(42, 1, "/src", "photos", 3, Some(9), "/archive/2020");
        let only_out = HashMap::from([(42, vec![row.clone()])]);
        let only_in = HashMap::from([(42, vec![row.clone()])]);

        assert_eq!(
            classify_decision_rows(42, &only_out, &HashMap::new()),
            vec![(&row, RowAspect::Extraction)]
        );
        assert_eq!(
            classify_decision_rows(42, &HashMap::new(), &only_in),
            vec![(&row, RowAspect::Arrival)]
        );
        // A decision touching neither map contributes no row at all — the
        // caller falls back to the selection-scope headline.
        assert!(classify_decision_rows(42, &HashMap::new(), &HashMap::new()).is_empty());
    }

    #[test]
    fn extraction_row_surfaces_scoped_view_despite_global_selection_scope() {
        let conn = open_in_memory_for_test();
        let root_a = insert_test_root(&conn, "/a", "source", false);
        insert_test_root(&conn, "/b", "source", false);
        // The apply's own selection scope is global (no decision_scopes row).
        let decision_id = insert_decision_at(&conn, "apply", 100);
        repo::decision::upsert_extractions(
            &conn,
            &[extraction_row(
                decision_id,
                root_a,
                "/a",
                "",
                47,
                Some(3_900_000),
                "/archive/x",
            )],
        )
        .unwrap();

        // Surfaces in a view of the drawn-from root...
        let view_a = compute_trail(&conn, &params(vec!["/a".to_string()])).unwrap();
        assert_eq!(decision_ids(&view_a.view), vec![decision_id]);
        assert!(view_a.extractions.contains_key(&decision_id));
        // ...and being shown here means it must not double as "not shown".
        assert_eq!(view_a.unscoped_decisions, 0);

        // A sibling root never touched by the extraction doesn't see it...
        let view_b = compute_trail(&conn, &params(vec!["/b".to_string()])).unwrap();
        assert!(decision_ids(&view_b.view).is_empty());
        // ...and its footer still counts the untouched global decision.
        assert_eq!(view_b.unscoped_decisions, 1);
    }

    #[test]
    fn decision_with_scope_row_and_extraction_row_appears_exactly_once() {
        let conn = open_in_memory_for_test();
        let root = insert_test_root(&conn, "/a", "source", false);
        let decision_id = insert_decision_at(&conn, "apply", 100);
        scope(&conn, decision_id, root, "");
        repo::decision::upsert_extractions(
            &conn,
            &[extraction_row(
                decision_id,
                root,
                "/a",
                "",
                5,
                Some(500),
                "/archive",
            )],
        )
        .unwrap();

        let result = compute_trail(&conn, &params(vec!["/a".to_string()])).unwrap();
        // Union+dedup: one id, not two — never both a selection line and an
        // extraction line (the id-set union collapses to one appearance).
        assert_eq!(decision_ids(&result.view), vec![decision_id]);
        assert!(result.extractions.contains_key(&decision_id));
    }

    #[test]
    fn extraction_rollup_reports_whole_history_even_when_capped() {
        let conn = open_in_memory_for_test();
        let root = insert_test_root(&conn, "/a", "source", false);
        let d1 = insert_decision_at(&conn, "apply", 100);
        let d2 = insert_decision_at(&conn, "apply", 200);
        repo::decision::upsert_extractions(
            &conn,
            &[extraction_row(
                d1,
                root,
                "/a",
                "",
                10,
                Some(1_000),
                "/archive/x",
            )],
        )
        .unwrap();
        repo::decision::upsert_extractions(
            &conn,
            &[extraction_row(
                d2,
                root,
                "/a",
                "",
                20,
                Some(2_000),
                "/archive/y",
            )],
        )
        .unwrap();

        let mut p = params(vec!["/a".to_string()]);
        p.limit = Some(1);
        let result = compute_trail(&conn, &p).unwrap();
        assert_eq!(result.earlier_decisions, 1); // the window is capped...
        let rollup = result.extraction_rollup.unwrap();
        assert_eq!(rollup.files, 30); // ...but the rollup is whole-history
        assert_eq!(rollup.bytes, Some(3_000));
        assert_eq!(rollup.destinations, 2);
    }

    #[test]
    fn extraction_rollup_none_when_no_touching_rows() {
        let conn = open_in_memory_for_test();
        insert_test_root(&conn, "/a", "source", false);
        let result = compute_trail(&conn, &params(vec!["/a".to_string()])).unwrap();
        assert!(result.extraction_rollup.is_none());
    }

    #[test]
    fn extraction_rollup_bytes_omitted_when_any_row_lacks_them() {
        let conn = open_in_memory_for_test();
        let root = insert_test_root(&conn, "/a", "source", false);
        let d1 = insert_decision_at(&conn, "apply", 100);
        let d2 = insert_decision_at(&conn, "apply", 200);
        repo::decision::upsert_extractions(
            &conn,
            &[extraction_row(
                d1,
                root,
                "/a",
                "",
                10,
                Some(1_000),
                "/archive/x",
            )],
        )
        .unwrap();
        repo::decision::upsert_extractions(
            &conn,
            &[extraction_row(d2, root, "/a", "", 20, None, "/archive/y")],
        )
        .unwrap();

        let result = compute_trail(&conn, &params(vec!["/a".to_string()])).unwrap();
        let rollup = result.extraction_rollup.unwrap();
        assert_eq!(rollup.files, 30);
        assert_eq!(rollup.bytes, None);
    }

    #[test]
    fn extraction_rollup_none_for_global_view() {
        let conn = open_in_memory_for_test();
        let root = insert_test_root(&conn, "/a", "source", false);
        let d = insert_decision_at(&conn, "apply", 100);
        repo::decision::upsert_extractions(
            &conn,
            &[extraction_row(d, root, "/a", "", 1, Some(10), "/archive")],
        )
        .unwrap();

        let result = compute_trail(&conn, &params(Vec::new())).unwrap();
        assert!(result.extraction_rollup.is_none());
        assert!(result.extractions.is_empty());
    }

    #[test]
    fn extraction_rollup_none_for_time_lens_view() {
        let conn = open_in_memory_for_test();
        let root = insert_test_root(&conn, "/a", "source", false);
        let d = insert_decision_at(&conn, "apply", 100);
        scope(&conn, d, root, "");
        repo::decision::upsert_extractions(
            &conn,
            &[extraction_row(d, root, "/a", "", 1, Some(10), "/archive")],
        )
        .unwrap();

        let mut p = params(vec!["/a".to_string()]);
        p.timeframe = Some(WhenValue::Since(
            NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(),
        ));
        let result = compute_trail(&conn, &p).unwrap();
        assert!(result.extraction_rollup.is_none());
    }

    // ------------------------------------------------------------------
    // Arrival ledger visibility — the extraction ledger's inbound direction
    // ------------------------------------------------------------------

    #[test]
    fn arrival_row_surfaces_scoped_view_despite_unrelated_source_root() {
        let conn = open_in_memory_for_test();
        let source_root = insert_test_root(&conn, "/a", "source", false);
        insert_test_root(&conn, "/archive", "archive", false);
        // The apply's own selection scope is global (no decision_scopes row).
        let decision_id = insert_decision_at(&conn, "apply", 100);
        repo::decision::upsert_extractions(
            &conn,
            &[extraction_row(
                decision_id,
                source_root,
                "/a",
                "",
                47,
                Some(3_900_000),
                "/archive/x",
            )],
        )
        .unwrap();

        // Surfaces in a view of the destination it landed in...
        let view = compute_trail(&conn, &params(vec!["/archive".to_string()])).unwrap();
        assert_eq!(decision_ids(&view.view), vec![decision_id]);
        assert!(view.arrivals.contains_key(&decision_id));
        assert!(!view.extractions.contains_key(&decision_id));
        // ...and being shown here means it must not double as "not shown".
        assert_eq!(view.unscoped_decisions, 0);

        // A view of the source root sees the outbound extraction line, not
        // an arrival line — the two directions are distinct maps.
        let source_view = compute_trail(&conn, &params(vec!["/a".to_string()])).unwrap();
        assert!(source_view.extractions.contains_key(&decision_id));
        assert!(!source_view.arrivals.contains_key(&decision_id));
    }

    #[test]
    fn arrival_matching_is_bidirectional_and_segment_aware() {
        let conn = open_in_memory_for_test();
        let source_root = insert_test_root(&conn, "/a", "source", false);
        insert_test_root(&conn, "/archive", "archive", false);
        let deeper = insert_decision_at(&conn, "apply", 100); // destination deeper than the view
        repo::decision::upsert_extractions(
            &conn,
            &[extraction_row(
                deeper,
                source_root,
                "/a",
                "",
                1,
                Some(10),
                "/archive/x/y",
            )],
        )
        .unwrap();
        let shallower = insert_decision_at(&conn, "apply", 200); // destination is an ancestor of the view
        repo::decision::upsert_extractions(
            &conn,
            &[extraction_row(
                shallower,
                source_root,
                "/a",
                "",
                1,
                Some(10),
                "/archive",
            )],
        )
        .unwrap();
        let sibling = insert_decision_at(&conn, "apply", 300); // similar prefix, not a real ancestor/descendant
        repo::decision::upsert_extractions(
            &conn,
            &[extraction_row(
                sibling,
                source_root,
                "/a",
                "",
                1,
                Some(10),
                "/archive/xc",
            )],
        )
        .unwrap();

        let result = compute_trail(&conn, &params(vec!["/archive/x".to_string()])).unwrap();
        let ids = decision_ids(&result.view);
        assert!(ids.contains(&deeper));
        assert!(ids.contains(&shallower));
        assert!(!ids.contains(&sibling)); // /archive/xc is not under /archive/x
    }

    #[test]
    fn arrival_surfaces_when_the_destination_root_was_removed() {
        // The archive root that received this apply is gone; the folder is
        // still viewable because a surviving ancestor root covers it. The
        // extraction row's `destination_root_id` now points at nothing, so
        // only snapshot-path matching can still surface the arrival.
        let conn = open_in_memory_for_test();
        let source_root = insert_test_root(&conn, "/a", "source", false);
        insert_test_root(&conn, "/archive", "archive", false);
        let removed_root = insert_test_root(&conn, "/archive/media", "archive", false);
        let decision_id = insert_decision_at(&conn, "apply", 100);
        let mut row = extraction_row(
            decision_id,
            source_root,
            "/a",
            "",
            5,
            Some(500),
            "/archive/media",
        );
        row.destination_root_id = Some(removed_root);
        repo::decision::upsert_extractions(&conn, &[row]).unwrap();

        conn.execute("DELETE FROM roots WHERE id = ?", [removed_root])
            .unwrap();

        let result = compute_trail(&conn, &params(vec!["/archive/media".to_string()])).unwrap();
        assert!(result.arrivals.contains_key(&decision_id));
        // Shown here means it must not also count as "not shown".
        assert_eq!(result.unscoped_decisions, 0);
    }

    #[test]
    fn arrival_surfaces_when_the_destination_root_was_removed_and_re_added() {
        // The harder half: the destination path is registered again, but as a
        // *new* root with a new id. The recorded `destination_root_id` is
        // stale, so a root-id-keyed join would silently lose this arrival
        // while the snapshot path still matches exactly.
        let conn = open_in_memory_for_test();
        let old_destination = insert_test_root(&conn, "/archive/media", "archive", false);
        // Inserted after, so re-adding below can't reuse the freed rowid.
        let source_root = insert_test_root(&conn, "/a", "source", false);
        let decision_id = insert_decision_at(&conn, "apply", 100);
        let mut row = extraction_row(
            decision_id,
            source_root,
            "/a",
            "",
            5,
            Some(500),
            "/archive/media",
        );
        row.destination_root_id = Some(old_destination);
        repo::decision::upsert_extractions(&conn, &[row]).unwrap();

        conn.execute("DELETE FROM roots WHERE id = ?", [old_destination])
            .unwrap();
        let re_added = insert_test_root(&conn, "/archive/media", "archive", false);
        assert_ne!(
            re_added, old_destination,
            "the point of this test is a changed id"
        );

        let result = compute_trail(&conn, &params(vec!["/archive/media".to_string()])).unwrap();
        assert!(result.arrivals.contains_key(&decision_id));
        assert_eq!(result.unscoped_decisions, 0);
    }

    #[test]
    fn decision_with_extraction_and_arrival_row_touching_same_view_appears_once() {
        // Intra-view relocation: a decision whose source and destination are
        // both inside the viewed scope. It must list once, with both maps
        // populated — rendering is responsible for collapsing that into one
        // line rather than two.
        let conn = open_in_memory_for_test();
        let root = insert_test_root(&conn, "/a", "source", false);
        let decision_id = insert_decision_at(&conn, "apply", 100);
        repo::decision::upsert_extractions(
            &conn,
            &[extraction_row(
                decision_id,
                root,
                "/a",
                "photos",
                5,
                Some(500),
                "/a/archive",
            )],
        )
        .unwrap();

        let result = compute_trail(&conn, &params(vec!["/a".to_string()])).unwrap();
        assert_eq!(decision_ids(&result.view), vec![decision_id]);
        assert!(result.extractions.contains_key(&decision_id));
        assert!(result.arrivals.contains_key(&decision_id));
    }

    #[test]
    fn arrival_rollup_reports_whole_history_even_when_capped() {
        let conn = open_in_memory_for_test();
        let source_root = insert_test_root(&conn, "/a", "source", false);
        insert_test_root(&conn, "/archive", "archive", false);
        let d1 = insert_decision_at(&conn, "apply", 100);
        let d2 = insert_decision_at(&conn, "apply", 200);
        repo::decision::upsert_extractions(
            &conn,
            &[extraction_row(
                d1,
                source_root,
                "/a",
                "",
                10,
                Some(1_000),
                "/archive/x",
            )],
        )
        .unwrap();
        repo::decision::upsert_extractions(
            &conn,
            &[extraction_row(
                d2,
                source_root,
                "/a",
                "",
                20,
                Some(2_000),
                "/archive/y",
            )],
        )
        .unwrap();

        let mut p = params(vec!["/archive".to_string()]);
        p.limit = Some(1);
        let result = compute_trail(&conn, &p).unwrap();
        assert_eq!(result.earlier_decisions, 1); // the window is capped...
        let rollup = result.arrival_rollup.unwrap();
        assert_eq!(rollup.files, 30); // ...but the rollup is whole-history
        assert_eq!(rollup.bytes, Some(3_000));
        assert_eq!(rollup.origins, 1); // both rows drawn from the same root
    }

    #[test]
    fn arrival_rollup_none_when_no_touching_rows() {
        let conn = open_in_memory_for_test();
        insert_test_root(&conn, "/archive", "archive", false);
        let result = compute_trail(&conn, &params(vec!["/archive".to_string()])).unwrap();
        assert!(result.arrival_rollup.is_none());
    }

    #[test]
    fn arrival_rollup_bytes_omitted_when_any_row_lacks_them() {
        let conn = open_in_memory_for_test();
        let source_root = insert_test_root(&conn, "/a", "source", false);
        insert_test_root(&conn, "/archive", "archive", false);
        let d1 = insert_decision_at(&conn, "apply", 100);
        let d2 = insert_decision_at(&conn, "apply", 200);
        repo::decision::upsert_extractions(
            &conn,
            &[extraction_row(
                d1,
                source_root,
                "/a",
                "",
                10,
                Some(1_000),
                "/archive/x",
            )],
        )
        .unwrap();
        repo::decision::upsert_extractions(
            &conn,
            &[extraction_row(
                d2,
                source_root,
                "/a",
                "",
                20,
                None,
                "/archive/y",
            )],
        )
        .unwrap();

        let result = compute_trail(&conn, &params(vec!["/archive".to_string()])).unwrap();
        let rollup = result.arrival_rollup.unwrap();
        assert_eq!(rollup.files, 30);
        assert_eq!(rollup.bytes, None);
    }

    #[test]
    fn arrival_rollup_distinguishes_origins_by_root_path() {
        let conn = open_in_memory_for_test();
        let root_a = insert_test_root(&conn, "/a", "source", false);
        let root_b = insert_test_root(&conn, "/b", "source", false);
        insert_test_root(&conn, "/archive", "archive", false);
        let d = insert_decision_at(&conn, "apply", 100);
        repo::decision::upsert_extractions(
            &conn,
            &[
                extraction_row(d, root_a, "/a", "", 1, Some(10), "/archive/x"),
                extraction_row(d, root_b, "/b", "", 1, Some(10), "/archive/x"),
            ],
        )
        .unwrap();

        let result = compute_trail(&conn, &params(vec!["/archive".to_string()])).unwrap();
        let rollup = result.arrival_rollup.unwrap();
        assert_eq!(rollup.files, 2);
        assert_eq!(rollup.origins, 2);
    }

    #[test]
    fn arrival_rollup_and_arrivals_empty_for_global_view() {
        let conn = open_in_memory_for_test();
        let source_root = insert_test_root(&conn, "/a", "source", false);
        insert_test_root(&conn, "/archive", "archive", false);
        let d = insert_decision_at(&conn, "apply", 100);
        repo::decision::upsert_extractions(
            &conn,
            &[extraction_row(
                d,
                source_root,
                "/a",
                "",
                1,
                Some(10),
                "/archive/x",
            )],
        )
        .unwrap();

        let result = compute_trail(&conn, &params(Vec::new())).unwrap();
        assert!(result.arrivals.is_empty());
        assert!(result.arrival_rollup.is_none());
    }

    #[test]
    fn arrival_rollup_none_for_time_lens_view_but_decision_still_listed() {
        let conn = open_in_memory_for_test();
        let source_root = insert_test_root(&conn, "/a", "source", false);
        insert_test_root(&conn, "/archive", "archive", false);
        let day1 = NaiveDate::from_ymd_opt(2026, 7, 10).unwrap();
        let ts1 = local_midnight(day1) + 3600;
        let apply = insert_decision_at(&conn, "apply", ts1);
        repo::decision::upsert_extractions(
            &conn,
            &[extraction_row(
                apply,
                source_root,
                "/a",
                "",
                5,
                Some(500),
                "/archive/x",
            )],
        )
        .unwrap();

        let mut p = params(vec!["/archive".to_string()]);
        p.timeframe = Some(WhenValue::Since(day1));
        let result = compute_trail(&conn, &p).unwrap();
        assert!(result.arrival_rollup.is_none());
        match &result.view {
            TrailView::Days(days) => {
                assert_eq!(days.len(), 1);
                let ids: Vec<i64> = days[0]
                    .events
                    .iter()
                    .filter_map(|e| match e {
                        TimelineEvent::Decision(d) => Some(d.id),
                        TimelineEvent::Note(_) => None,
                    })
                    .collect();
                assert_eq!(ids, vec![apply]);
            }
            TrailView::Recent(_) => panic!("time lens must be Days"),
        }
    }

    // ------------------------------------------------------------------
    // Rearrangement: rows that crossed no boundary
    //
    // An intra-archive apply cannot currently be produced through the CLI
    // (`apply` aborts with "files already in destination archive" whatever
    // --allow is given) — that is the one-way-relocation machinery the vision
    // open question names. These fixtures write the extraction rows such an
    // apply *would* record, which is what the read layer sees either way.
    // ------------------------------------------------------------------

    #[test]
    fn rearrangement_row_leaves_both_crossing_rollups() {
        let conn = open_in_memory_for_test();
        let archive = insert_test_root(&conn, "/archive", "archive", false);
        let d = insert_decision_at(&conn, "apply", 100);
        // Drawn from /archive/2016, landing in /archive/2020: viewed at the
        // archive root, both endpoints are inside, so nothing crossed.
        repo::decision::upsert_extractions(
            &conn,
            &[extraction_row(
                d,
                archive,
                "/archive",
                "2016",
                47,
                Some(3_900),
                "/archive/2020",
            )],
        )
        .unwrap();

        let result = compute_trail(&conn, &params(vec!["/archive".to_string()])).unwrap();
        assert!(
            result.extraction_rollup.is_none(),
            "nothing left this place"
        );
        assert!(result.arrival_rollup.is_none(), "nothing entered it");
        let rollup = result.rearrangement_rollup.unwrap();
        assert_eq!(rollup.files, 47);
        assert_eq!(rollup.bytes, Some(3_900));
    }

    #[test]
    fn narrower_view_reads_the_same_decision_as_an_arrival() {
        // The scope-dependence is the rule working: the boundary moved, so
        // the same row now crosses it.
        let conn = open_in_memory_for_test();
        let archive = insert_test_root(&conn, "/archive", "archive", false);
        let d = insert_decision_at(&conn, "apply", 100);
        repo::decision::upsert_extractions(
            &conn,
            &[extraction_row(
                d,
                archive,
                "/archive",
                "2016",
                47,
                Some(3_900),
                "/archive/2020",
            )],
        )
        .unwrap();

        let result = compute_trail(&conn, &params(vec!["/archive/2020".to_string()])).unwrap();
        assert!(result.rearrangement_rollup.is_none());
        let rollup = result.arrival_rollup.unwrap();
        assert_eq!(rollup.files, 47);
    }

    #[test]
    fn one_decision_can_feed_two_rollups_at_once() {
        // The footer-level form of the mixed-origin bug: an apply drawing
        // from inside the view and from outside it rearranged some content
        // and received the rest. Filtering decisions rather than rows would
        // put all 55 files in one rollup.
        let conn = open_in_memory_for_test();
        let archive = insert_test_root(&conn, "/archive", "archive", false);
        let sd = insert_test_root(&conn, "/Volumes/sd", "source", false);
        let d = insert_decision_at(&conn, "apply", 100);
        repo::decision::upsert_extractions(
            &conn,
            &[
                extraction_row(
                    d,
                    archive,
                    "/archive",
                    "2016",
                    47,
                    Some(3_900),
                    "/archive/2020",
                ),
                extraction_row(d, sd, "/Volumes/sd", "dcim", 8, Some(800), "/archive/2020"),
            ],
        )
        .unwrap();

        let result = compute_trail(&conn, &params(vec!["/archive".to_string()])).unwrap();
        let rearranged = result.rearrangement_rollup.unwrap();
        assert_eq!(rearranged.files, 47);
        assert_eq!(rearranged.bytes, Some(3_900));
        let arrived = result.arrival_rollup.unwrap();
        assert_eq!(arrived.files, 8);
        assert_eq!(arrived.bytes, Some(800));
        assert_eq!(arrived.origins, 1, "only the outside root is an origin");
        assert!(result.extraction_rollup.is_none());
    }

    #[test]
    fn bytes_are_all_or_omitted_per_rollup_not_across_them() {
        // An unknown-size crossing must not suppress a fully known
        // rearrangement total — each rollup judges its own rows.
        let conn = open_in_memory_for_test();
        let archive = insert_test_root(&conn, "/archive", "archive", false);
        let sd = insert_test_root(&conn, "/Volumes/sd", "source", false);
        let d = insert_decision_at(&conn, "apply", 100);
        repo::decision::upsert_extractions(
            &conn,
            &[
                extraction_row(
                    d,
                    archive,
                    "/archive",
                    "2016",
                    47,
                    Some(3_900),
                    "/archive/2020",
                ),
                extraction_row(d, sd, "/Volumes/sd", "dcim", 8, None, "/archive/2020"),
            ],
        )
        .unwrap();

        let result = compute_trail(&conn, &params(vec!["/archive".to_string()])).unwrap();
        assert_eq!(result.arrival_rollup.unwrap().bytes, None);
        assert_eq!(result.rearrangement_rollup.unwrap().bytes, Some(3_900));
    }

    #[test]
    fn rearrangement_rollup_is_whole_history_despite_the_cap() {
        let conn = open_in_memory_for_test();
        let archive = insert_test_root(&conn, "/archive", "archive", false);
        let d1 = insert_decision_at(&conn, "apply", 100);
        let d2 = insert_decision_at(&conn, "apply", 200);
        for (d, files, bytes) in [(d1, 10, 1_000), (d2, 20, 2_000)] {
            repo::decision::upsert_extractions(
                &conn,
                &[extraction_row(
                    d,
                    archive,
                    "/archive",
                    "2016",
                    files,
                    Some(bytes),
                    "/archive/2020",
                )],
            )
            .unwrap();
        }

        let mut p = params(vec!["/archive".to_string()]);
        p.limit = Some(1);
        let result = compute_trail(&conn, &p).unwrap();
        assert_eq!(result.earlier_decisions, 1);
        let rollup = result.rearrangement_rollup.unwrap();
        assert_eq!(rollup.files, 30);
        assert_eq!(rollup.bytes, Some(3_000));
    }

    #[test]
    fn rearrangement_rollup_none_for_global_and_time_lens_views() {
        let conn = open_in_memory_for_test();
        let archive = insert_test_root(&conn, "/archive", "archive", false);
        let day = NaiveDate::from_ymd_opt(2026, 7, 10).unwrap();
        let d = insert_decision_at(&conn, "apply", local_midnight(day) + 3600);
        repo::decision::upsert_extractions(
            &conn,
            &[extraction_row(
                d,
                archive,
                "/archive",
                "2016",
                47,
                Some(3_900),
                "/archive/2020",
            )],
        )
        .unwrap();

        let global = compute_trail(&conn, &params(Vec::new())).unwrap();
        assert!(global.rearrangement_rollup.is_none());

        let mut p = params(vec!["/archive".to_string()]);
        p.timeframe = Some(WhenValue::Since(day));
        let timed = compute_trail(&conn, &p).unwrap();
        assert!(timed.rearrangement_rollup.is_none());
    }

    #[test]
    fn extractions_all_populated_for_global_view_jsonl_completeness() {
        let conn = open_in_memory_for_test();
        let root = insert_test_root(&conn, "/a", "source", false);
        let d = insert_decision_at(&conn, "apply", 100);
        repo::decision::upsert_extractions(
            &conn,
            &[extraction_row(d, root, "/a", "", 1, Some(10), "/archive")],
        )
        .unwrap();

        let result = compute_trail(&conn, &params(Vec::new())).unwrap();
        // The touching map is empty at global scope (nothing to touch)...
        assert!(result.extractions.is_empty());
        // ...but the full-per-decision map used for JSONL still has it.
        assert!(result.extractions_all.contains_key(&d));
        assert_eq!(result.extractions_all[&d].len(), 1);
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
                assert_eq!(days[0].rollup.deleted.files, 2);
                assert_eq!(days[0].rollup.deleted.bytes, Some(300));
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
    fn show_lists_extractions_including_removed_root_snapshot() {
        let conn = open_in_memory_for_test();
        let root = insert_test_root(&conn, "/a", "source", false);
        let d = insert_decision_at(&conn, "apply", 100);
        repo::decision::upsert_extractions(
            &conn,
            &[
                extraction_row(
                    d,
                    root,
                    "/a",
                    "photos/2016/italy",
                    47,
                    Some(3_900_000),
                    "/archive/x",
                ),
                // A second root already removed from the DB — the row's
                // root_path snapshot must still render.
                extraction_row(
                    d,
                    999,
                    "/Volumes/gone",
                    "dcim",
                    12,
                    Some(401_000),
                    "/archive/y",
                ),
            ],
        )
        .unwrap();

        let show = compute_show(&conn, d).unwrap().unwrap();
        assert_eq!(show.extractions.len(), 2);
        let a = show
            .extractions
            .iter()
            .find(|e| e.row.root_id == root)
            .unwrap();
        assert_eq!(a.row.root_path, "/a");
        assert_eq!(a.row.rel_prefix, "photos/2016/italy");
        assert!(!a.root_removed);
        let gone = show
            .extractions
            .iter()
            .find(|e| e.row.root_id == 999)
            .unwrap();
        assert_eq!(gone.row.root_path, "/Volumes/gone");
        assert!(gone.root_removed);
    }

    #[test]
    fn show_does_not_mark_a_re_added_root_as_removed() {
        // The row's snapshot id is stale because the root was removed and
        // re-added, but the location is registered and visitable — matching
        // on the path is what keeps `drew from:` honest.
        let conn = open_in_memory_for_test();
        let re_added = insert_test_root(&conn, "/a", "source", false);
        let d = insert_decision_at(&conn, "apply", 100);
        let mut row = extraction_row(d, re_added, "/a", "photos", 3, Some(30), "/archive/x");
        row.root_id = 999; // the id the root carried before it was re-added
        repo::decision::upsert_extractions(&conn, &[row]).unwrap();

        let show = compute_show(&conn, d).unwrap().unwrap();
        assert_eq!(show.extractions.len(), 1);
        assert_ne!(show.extractions[0].row.root_id, re_added);
        assert!(
            !show.extractions[0].root_removed,
            "a live location must not read as removed"
        );
    }

    #[test]
    fn show_no_extractions_is_empty_not_absent() {
        let conn = open_in_memory_for_test();
        let d = insert_decision_at(&conn, "scan", 100);
        let show = compute_show(&conn, d).unwrap().unwrap();
        assert!(show.extractions.is_empty());
    }

    // ------------------------------------------------------------------
    // Time lens pinning (Story 4): extraction-touching decisions join
    // --since/--on views through the same shared scoped id-union as the
    // scope lens; day rollups need no new mechanics.
    // ------------------------------------------------------------------

    #[test]
    fn time_lens_includes_extraction_touching_decision_in_right_day() {
        let conn = open_in_memory_for_test();
        let root = insert_test_root(&conn, "/a", "source", false);
        let day1 = NaiveDate::from_ymd_opt(2026, 7, 10).unwrap();
        let ts1 = local_midnight(day1) + 3600;

        // Global selection scope (no decision_scopes row) — only the
        // extraction row ties it to this view.
        let apply = insert_decision_at(&conn, "apply", ts1);
        repo::decision::upsert_extractions(
            &conn,
            &[extraction_row(
                apply,
                root,
                "/a",
                "",
                5,
                Some(500),
                "/archive",
            )],
        )
        .unwrap();

        let mut p = params(vec!["/a".to_string()]);
        p.timeframe = Some(WhenValue::Since(day1));
        let result = compute_trail(&conn, &p).unwrap();
        match &result.view {
            TrailView::Days(days) => {
                assert_eq!(days.len(), 1);
                assert_eq!(days[0].date, day1);
                let ids: Vec<i64> = days[0]
                    .events
                    .iter()
                    .filter_map(|e| match e {
                        TimelineEvent::Decision(d) => Some(d.id),
                        TimelineEvent::Note(_) => None,
                    })
                    .collect();
                assert_eq!(ids, vec![apply]);
            }
            TrailView::Recent(_) => panic!("time lens must be Days"),
        }
    }

    #[test]
    fn time_lens_day_archived_rollup_reflects_apply_stamps_regardless_of_extraction_rows() {
        // Day rollups already aggregate apply's destination-row stamps
        // (present bucket => archived); extraction rows are a separate
        // projection and need no new rollup mechanics.
        let conn = open_in_memory_for_test();
        let root = insert_test_root(&conn, "/a", "source", false);
        let archive_root = insert_test_root(&conn, "/archive", "archive", false);
        let day1 = NaiveDate::from_ymd_opt(2026, 7, 10).unwrap();
        let ts1 = local_midnight(day1) + 3600;

        let apply = insert_decision_at(&conn, "apply", ts1);
        scope(&conn, apply, root, "");
        repo::decision::upsert_extractions(
            &conn,
            &[extraction_row(
                apply,
                root,
                "/a",
                "",
                3,
                Some(300),
                "/archive",
            )],
        )
        .unwrap();
        // Three destination sources stamped by this decision — the rollup's
        // "archived" line comes from *these* DB stamps, an independent
        // mechanism from the extraction row's own `files` count above (which
        // happens to agree here, but is not where the rollup reads from).
        for (i, name) in ["a.jpg", "b.jpg", "c.jpg"].iter().enumerate() {
            let dest = crate::repo::source::insert_test_source(
                &conn,
                archive_root,
                name,
                1,
                i as i64 + 1,
                100,
                0,
            );
            conn.execute(
                "UPDATE sources SET decision_id = ?, present = 1 WHERE id = ?",
                rusqlite::params![apply, dest],
            )
            .unwrap();
        }

        let mut p = params(vec!["/a".to_string()]);
        p.timeframe = Some(WhenValue::Since(day1));
        let result = compute_trail(&conn, &p).unwrap();
        match &result.view {
            TrailView::Days(days) => {
                assert_eq!(days[0].rollup.archived.files, 3);
            }
            TrailView::Recent(_) => panic!("time lens must be Days"),
        }
    }

    #[test]
    fn time_lens_global_view_unchanged_by_extraction_rows() {
        let conn = open_in_memory_for_test();
        let root = insert_test_root(&conn, "/a", "source", false);
        let day1 = NaiveDate::from_ymd_opt(2026, 7, 10).unwrap();
        let ts1 = local_midnight(day1) + 3600;
        let apply = insert_decision_at(&conn, "apply", ts1);
        repo::decision::upsert_extractions(
            &conn,
            &[extraction_row(
                apply,
                root,
                "/a",
                "",
                5,
                Some(500),
                "/archive",
            )],
        )
        .unwrap();

        let mut p = params(Vec::new());
        p.timeframe = Some(WhenValue::Since(day1));
        let result = compute_trail(&conn, &p).unwrap();
        assert_eq!(result.unscoped_decisions, 0); // global view: never counted
        match &result.view {
            TrailView::Days(days) => assert_eq!(days.len(), 1),
            TrailView::Recent(_) => panic!("time lens must be Days"),
        }
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
