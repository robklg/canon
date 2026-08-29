//! The trail's compute path: `compute_trail`, the timeline/rollup query
//! that answers "what happened here?" over a scope or time lens.
//!
//! Read-only: no transactions, no stdio.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use anyhow::Result;
use chrono::{Duration, Local, NaiveDate, TimeZone};

use crate::core::domain::extraction::DecisionExtraction;
use crate::core::domain::root::{find_containing_root, Root};
use crate::core::repo::{self, Connection};
use crate::notes::{fetch_all, fetch_by_roots};
use crate::trail::domain::grouping::group_destinations;
use crate::trail::domain::placement::{
    placement_in_view, row_aspect, scopes_touch, RowAspect, ScopeMatch,
};
use crate::trail::domain::timeline::{
    group_by_day, merge_events, DayGroup, TimelineEvent, WhenValue,
};

/// Default decision-window cap for timeline views.
pub const DEFAULT_LIMIT: usize = 20;

pub struct TrailParams {
    /// Pre-resolved scope prefixes (empty = global). The interface resolves
    /// scope via `core::ops::scope::resolve_scope` before building params.
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
    /// Destinations at the **derived grain**, not at the ledger's leaf: the
    /// count the counterpart door itemizes, through the same
    /// `domain::grouping` derivation over the same rows.
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
    /// Extraction rows this view reads, by decision id, each tagged with the
    /// direction it reads from here — drawn out of the view (`Extraction`),
    /// delivered into it (`Arrival`), or moved within it (`Rearrangement`).
    /// Membership is per row and per placement: a row is inside the view only
    /// where the view *contains* its recorded location (`placement_in_view`,
    /// never the bidirectional scope rule). Origin membership is root-id-
    /// keyed; destination membership runs on absolute snapshot paths, so a
    /// removed or re-added destination root doesn't break the link. Powers
    /// the timeline lines and the rollups below. Empty for the global view.
    pub placements: HashMap<i64, Vec<(DecisionExtraction, RowAspect)>>,
    /// `None` when there are no touching rows, or the view is global or a
    /// time-lens view (the rollup is a scope-lens-only footer).
    pub extraction_rollup: Option<ExtractionRollup>,
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
    /// Which recorded scope pulled each decision into this view. Empty for
    /// global views (no boundary, so nothing matched anything).
    ///
    /// A **side map**, never a mutation of `Decision.scope`: the durable
    /// display column is never reordered or rewritten, so `--jsonl` — which
    /// serialises `d.scope` directly — is unaffected by construction rather
    /// than by care. Same shape as [`placements`](Self::placements), for the
    /// same reason.
    pub scope_matches: HashMap<i64, ScopeMatch>,
    /// The single root containing every prefix of this view, when there is
    /// one — what root-relative rendering measures from. `None` for global
    /// and multi-root views, which render absolute.
    ///
    /// Derived here rather than in the interface: `ResolvedScope` carries
    /// prefixes but no root, so answering "which root is this view in?" is a
    /// business question, and `core::domain::root::find_containing_root`
    /// already owns it.
    pub view_root: Option<String>,
}

pub fn compute_trail(conn: &Connection, params: &TrailParams) -> Result<TrailResult> {
    let range = params.timeframe.map(when_range);

    let (mut decisions, unscoped, notes, placements, rollups, scope_matches, view_root) =
        if params.prefixes.is_empty() {
            let decisions = match range {
                Some((start, end)) => crate::trail::repo::fetch_in_range(conn, start, end)?,
                None => crate::trail::repo::fetch_recent(conn, None)?,
            };
            let notes = if params.include_notes {
                fetch_all(conn)?
            } else {
                Vec::new()
            };
            (
                decisions,
                0,
                notes,
                HashMap::new(),
                Rollups::default(),
                HashMap::new(),
                None,
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
            let matching: Vec<&repo::decision::DecisionScopeRow> = rows
                .iter()
                .filter(|row| touches(row.root_id, &row.rel_prefix))
                .collect();
            let mut ids: Vec<i64> = matching.iter().map(|row| row.decision_id).collect();

            // The single root every prefix of this view sits in, if there is
            // one — what root-relative rendering measures from.
            let view_root = single_view_root(&pairs, &roots);

            // Extraction rows this view reads — apply decisions that drew content
            // out of here, delivered content into here, or moved content within
            // here, even when the decision's *selection* scope was global or
            // elsewhere entirely. Each row is classified by its own two recorded
            // locations against the view boundary.
            let all_ext_rows = crate::trail::repo::fetch_all_extractions(conn)?;
            let placements = classify_extraction_rows(all_ext_rows, &pairs, &params.prefixes);

            ids.extend(placements.keys().copied());
            ids.sort_unstable();
            ids.dedup();

            let mut decisions = repo::decision::fetch_by_ids(conn, &ids)?;
            if let Some((start, end)) = range {
                decisions.retain(|d| d.created_at >= start && d.created_at < end);
            }

            let unscoped_raw = crate::trail::repo::count_unscoped(conn, range)?;
            // Footer honesty: a decision surfaced here only via an extraction
            // or arrival row (no decision_scopes row of its own) must not
            // also be counted as "not shown" — restricted to ids that
            // actually survived the time-range filter above, since a
            // touching id outside --since/--on was never part of
            // unscoped_raw's count either.
            let shown_ids: HashSet<i64> = decisions.iter().map(|d| d.id).collect();
            let shown_extraction_ids: Vec<i64> = placements
                .keys()
                .filter(|id| shown_ids.contains(id))
                .copied()
                .collect();
            let unscoped_adjustment =
                crate::trail::repo::filter_unscoped_ids(conn, &shown_extraction_ids)?.len() as i64;
            let unscoped = unscoped_raw - unscoped_adjustment;

            let notes = if params.include_notes {
                fetch_by_roots(conn, &root_ids)?
                    .into_iter()
                    .filter(|n| touches(n.root_id, &n.rel_path))
                    .collect()
            } else {
                Vec::new()
            };

            // The join the filter above already computed, kept instead of
            // discarded: which of a decision's recorded places is the one that
            // brought it into this view. No new query — the rows are the ones
            // just fetched, and `d.scope` is already loaded on the decision.
            let scope_matches = build_scope_matches(&matching, &decisions);

            // Whole-history rollups: every touching row, never capped by the
            // decision-window limit. Scope-lens only — never a time-lens view.
            let rollups = if range.is_none() {
                let floors: Vec<&str> = roots.iter().map(|r| r.path.as_str()).collect();
                build_rollups(&placements, &floors)
            } else {
                Rollups::default()
            };

            (
                decisions,
                unscoped,
                notes,
                placements,
                rollups,
                scope_matches,
                view_root,
            )
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
        let stamps = crate::trail::repo::aggregate_stamped_by_decisions(conn, &ids)?;
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
        placements,
        extraction_rollup: rollups.extraction,
        arrival_rollup: rollups.arrival,
        rearrangement_rollup: rollups.rearrangement,
        extractions_all,
        scope_matches,
        view_root,
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

/// The single root containing every prefix of this view, when there is one.
///
/// `None` for a global view (no prefixes), a view spanning several roots, and
/// a view whose prefixes resolve to no root at all — each renders absolute,
/// because there is no one root a reader could be told about once in the
/// header.
///
/// `pub(super)`, not private: the externalized test module
/// (`trail::ops::tests::compute`) exercises the multi-root and no-root arms
/// directly.
pub(super) fn single_view_root(pairs: &[(i64, String)], roots: &[Root]) -> Option<String> {
    let mut ids = pairs.iter().map(|(id, _)| *id);
    let first = ids.next()?;
    if ids.any(|id| id != first) {
        return None;
    }
    roots.iter().find(|r| r.id == first).map(|r| r.path.clone())
}

/// Which of each decision's recorded scopes brought it into this view.
///
/// One entry per decision that a `decision_scopes` row matched. Where several
/// of a decision's rows match, the **deepest** wins: a scope inside the view
/// is a more precise statement of where the act was than an ancestor of it,
/// and an ancestor is what a 31-prefix scan used to be labelled by. Depth is
/// measured in path segments, ties broken lexicographically so repeated runs
/// render identically.
///
/// The display path is composed snapshot-first (`root_path` as written, then
/// the live-root join) — the established order everywhere a scope row is
/// rendered, so a removed root's scope still names a path rather than an id.
///
/// `other_count` is read off the decision's own `scope` display column rather
/// than off the rows, so `+N` keeps meaning exactly what it means today and
/// stays consistent with `trail show` and `--jsonl`, which both render that
/// column. A decision whose display column never got the matched path
/// backfilled counts the column whole — one more place, honestly, rather than
/// a silent subtraction.
///
/// `pub(super)`, not private: the externalized test module
/// (`trail::ops::tests::compute`) exercises the match rule directly, without
/// building a database for every case.
pub(super) fn build_scope_matches(
    matching: &[&repo::decision::DecisionScopeRow],
    decisions: &[crate::core::domain::decision::Decision],
) -> HashMap<i64, ScopeMatch> {
    let scopes_by_id: HashMap<i64, &Option<Vec<String>>> =
        decisions.iter().map(|d| (d.id, &d.scope)).collect();

    let mut best: HashMap<i64, String> = HashMap::new();
    for row in matching {
        let Some(display) = scope_row_display_path(row) else {
            continue;
        };
        match best.get(&row.decision_id) {
            Some(current) if !is_deeper(&display, current) => {}
            _ => {
                best.insert(row.decision_id, display);
            }
        }
    }

    best.into_iter()
        .map(|(decision_id, matched)| {
            let paths = scopes_by_id
                .get(&decision_id)
                .and_then(|s| s.as_ref())
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let other_count = if paths.iter().any(|p| p == &matched) {
                paths.len() - 1
            } else {
                paths.len()
            };
            (
                decision_id,
                ScopeMatch {
                    matched,
                    other_count,
                },
            )
        })
        .collect()
}

/// A scope row's absolute display path, snapshot-first. `None` when the row
/// carries no snapshot (a pre-migration row the recovery hook could not
/// resolve) — such a row cannot name a place, and inventing one from its root
/// id is exactly the guess the snapshot convention exists to avoid.
fn scope_row_display_path(row: &repo::decision::DecisionScopeRow) -> Option<String> {
    let root_path = row.root_path.as_ref()?;
    if row.rel_prefix.is_empty() {
        Some(root_path.clone())
    } else {
        Some(format!("{root_path}/{}", row.rel_prefix))
    }
}

/// Whether `candidate` is a more precise statement of place than `current`:
/// more path segments, or equal depth and lexicographically first.
fn is_deeper(candidate: &str, current: &str) -> bool {
    let depth = |p: &str| Path::new(p).components().count();
    match depth(candidate).cmp(&depth(current)) {
        std::cmp::Ordering::Greater => true,
        std::cmp::Ordering::Less => false,
        std::cmp::Ordering::Equal => candidate < current,
    }
}

/// Every extraction row a decision renders in this view, each tagged with the
/// direction it reads from here.
///
/// Classification is **per row, not per decision**: one apply can draw from a
/// root inside the view and from another outside it in the same breath, so
/// each row is judged by its own two endpoints — classifying per decision
/// would drop the outside-origin rows entirely.
///
/// Membership is per placement, never bidirectional
/// ([`trail::domain::placement::placement_in_view`]): origin membership is
/// root-id-keyed over the view's own decomposed roots (`pairs`), destination
/// membership runs on absolute snapshot paths (`prefixes`) so a removed or
/// re-added destination root can't break the link. The rule itself is
/// [`trail::domain::placement::row_aspect`]; `Outside` rows are dropped
/// here, so the tagged map holds exactly what the view renders.
///
/// `pub(super)`, not private: the externalized test module
/// (`trail::ops::tests::compute`) calls this directly to test row-level
/// classification without going through the full `compute_trail` query.
pub(super) fn classify_extraction_rows(
    rows: Vec<DecisionExtraction>,
    pairs: &[(i64, String)],
    prefixes: &[String],
) -> HashMap<i64, Vec<(DecisionExtraction, RowAspect)>> {
    let mut placements: HashMap<i64, Vec<(DecisionExtraction, RowAspect)>> = HashMap::new();
    for row in rows {
        let origin_in_view = pairs
            .iter()
            .any(|(rid, rel)| *rid == row.root_id && placement_in_view(rel, &row.rel_prefix));
        let destination_in_view = prefixes
            .iter()
            .any(|prefix| placement_in_view(prefix, &row.destination_path));
        let aspect = row_aspect(origin_in_view, destination_in_view);
        if aspect != RowAspect::Outside {
            placements
                .entry(row.decision_id)
                .or_default()
                .push((row, aspect));
        }
    }
    placements
}

/// Sum files/bytes and count distinct counterparties over a set of rows.
/// `None` if the set is empty; bytes `None` if any row lacks a size.
///
/// One builder for all three rollups: the all-or-omitted bytes rule is the
/// same rule in each, and three copies of it would be three places to fix.
/// They differ only in which end of the row is the counterparty — and the
/// rearrangement rollup, whose counterparty is this place, discards it.
///
/// `pub(super)`, not private: `ops::crossings` builds its section totals
/// through this same function. The crossings door is the rollup lines made
/// expandable, so its totals must *be* the rollup totals rather than agree
/// with them — same rows, same keys, same summation, one spelling.
pub(super) fn rollup_parts<'a>(
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
///
/// The counterparty count `rollup_parts` computes is **discarded**: it counts
/// ledger leaves, and this line's number is the one the door itemizes, which
/// is the derived destination grain. Sharing that derivation — not merely
/// matching its result — is what keeps a rollup saying `→ 3 destinations`
/// from standing above a door listing forty-seven of them, one line apart,
/// with the teaching hint between them inviting the comparison.
fn build_extraction_rollup<'a>(
    rows: impl Iterator<Item = &'a DecisionExtraction>,
    floors: &[&str],
) -> Option<ExtractionRollup> {
    let rows: Vec<&DecisionExtraction> = rows.collect();
    let (files, bytes, _) = rollup_parts(rows.iter().copied(), |r| &r.destination_path)?;
    let leaves: Vec<&str> = rows.iter().map(|r| r.destination_path.as_str()).collect();
    Some(ExtractionRollup {
        files,
        bytes,
        destinations: group_destinations(&leaves, floors).len(),
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
///
/// `floors` are the live root paths the outbound grouping stays below; they
/// are threaded from the roots this query already loaded rather than fetched,
/// so the derivation itself stays I/O-free.
fn build_rollups(
    placements: &HashMap<i64, Vec<(DecisionExtraction, RowAspect)>>,
    floors: &[&str],
) -> Rollups {
    let (mut left, mut entered, mut stayed) = (Vec::new(), Vec::new(), Vec::new());
    for (row, aspect) in placements.values().flatten() {
        match aspect {
            RowAspect::Extraction => left.push(row),
            RowAspect::Arrival => entered.push(row),
            RowAspect::Rearrangement => stayed.push(row),
            // Unreachable: `Outside` rows were dropped at classification.
            RowAspect::Outside => {}
        }
    }

    Rollups {
        extraction: build_extraction_rollup(left.into_iter(), floors),
        arrival: build_arrival_rollup(entered.into_iter()),
        rearrangement: build_rearrangement_rollup(stayed.into_iter()),
    }
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

/// `pub(super)`, not private: several tests in the externalized test module
/// build epoch timestamps directly (`local_midnight(day) + 3600`) to stay
/// timezone-independent, rather than hardcoding an epoch.
pub(super) fn local_midnight(date: NaiveDate) -> i64 {
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
