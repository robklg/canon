//! The trail's counterpart door: `compute_crossings`, which answers "what
//! moved between here and there?" — the relation axis, beside the place, time
//! and single-decision ones the trail already had.
//!
//! The bare view is the three whole-history rollups made expandable: same
//! counterparty keys and the same summation, shared rather than matched, so a
//! listing's line count is the rollup's counterparty count and its sums are
//! the rollup's sums wherever both surfaces speak. They select by different
//! evidence — root id there, snapshot path here — and part where an id has
//! gone stale; see the crossings conventions in `src/trail/CLAUDE.md`.
//! Naming a counterpart narrows to one relation and drops to row grain,
//! because once a counterpart is named its internal structure *is* the
//! subject.
//!
//! Read-only: no transactions, no stdio. Served entirely from DB projections
//! — no receipt file is opened here, or anywhere on a query path.

use std::collections::HashMap;

use anyhow::Result;

use crate::core::domain::decision::Decision;
use crate::core::domain::extraction::{DecisionExtraction, OriginDisposition};
use crate::core::domain::root::{find_containing_root, Root};
use crate::core::repo::{self, Connection};
use crate::trail::domain::composition::OriginLine;
use crate::trail::domain::crossings::{
    counterpart_of, crossing_verdict, CrossingFilter, CrossingVerdict,
};
use crate::trail::domain::placement::{placement_in_view, RowAspect};
use crate::trail::ops::compute::rollup_parts;
use crate::trail::ops::place::{place_knowledge, PlaceKnowledge};

pub struct CrossingsParams {
    /// Pre-resolved scope prefixes (empty = global).
    pub prefixes: Vec<String>,
    /// Counterpart paths, cleaned lexically and never canonicalized — an old
    /// mount path need not exist to be asked about.
    pub origin: Option<String>,
    pub destination: Option<String>,
    /// Listing cap; `None` = `--all`. Applies **per section**.
    pub limit: Option<usize>,
    /// Machine output. Gates the reconciliation line only — the same input
    /// `composition::ViewShape` takes, and for the same reason: a
    /// present-tense number has no place in a stream whose contract is
    /// view-independent.
    pub machine_output: bool,
}

/// How a crossings query ended.
pub enum Crossings {
    /// A named counterpart Canon has no record of. The question was
    /// well-formed and it is answered — the answer is that this place is
    /// unknown — so the interface states it and exits non-zero without an
    /// `Error:` prefix, exactly as a scope-lens miss does.
    UnknownCounterpart(Vec<String>),
    Reported(Box<CrossingsResult>),
}

pub struct CrossingsResult {
    /// Archived-from-here first, then Arrived-here — the trail's own footer
    /// order. Empty sections are omitted, never printed empty.
    pub sections: Vec<CrossingSection>,
    /// What to say when nothing crossed. `None` when something did.
    pub nothing_crossed: Option<NothingCrossed>,
    /// The one deliberate register crossing: two observations, no cause.
    pub reconciliation: Option<Reconciliation>,
    /// The decisions carrying a crossing in this view, chronological — the
    /// filtered set machine output serialises.
    pub decisions: Vec<Decision>,
    /// Those decisions' **full** extraction rows, not the crossing-matched
    /// subset: a decision serialises identically wherever it was surfaced
    /// from, and this door must not become the one view that varies.
    pub extractions_all: HashMap<i64, Vec<DecisionExtraction>>,
}

pub struct CrossingSection {
    /// `Arrival` or `Extraction` — never a non-crossing.
    pub aspect: RowAspect,
    pub files: i64,
    /// `None` if any contributing row lacks a size — never a partial sum.
    pub bytes: Option<i64>,
    pub counterparty_count: usize,
    /// Set when this section's outside end was named by a flag.
    pub named: Option<Counterpart>,
    pub body: CrossingBody,
}

pub enum CrossingBody {
    /// Unnamed: one entry per counterpart, capped with an explicit remainder.
    Counterparts {
        lines: Vec<CounterpartLine>,
        more: usize,
    },
    /// Named: per-decision deliveries across that counterpart, at row grain,
    /// capped likewise.
    Deliveries {
        lines: Vec<CrossingDelivery>,
        more: usize,
    },
}

/// A place bound to this view by one relation claim: the where-else of the
/// matched content.
pub struct Counterpart {
    pub path: String,
    /// No live root contains this location.
    pub root_removed: bool,
    /// Where its story lives now, when the root was retired. The recorded
    /// path — nothing on disk is observed.
    pub retired_book: Option<String>,
}

pub struct CounterpartLine {
    pub counterpart: Counterpart,
    pub files: i64,
    pub bytes: Option<i64>,
    pub decisions: usize,
    pub first_at: i64,
    pub last_at: i64,
}

/// One decision's deliveries across the named counterpart, at **row grain**.
///
/// The timeline collapses a decision's rows to one line per (origin root,
/// aspect) taking the common prefix. Here that would be fatal: an apply
/// drawing from `Photos/2016` and `Video/raw` collapses to the root, which is
/// exactly the aboutness the reader reached for when they named the
/// counterpart. Once a counterpart is named, its internal structure is the
/// subject.
pub struct CrossingDelivery {
    pub decision_id: i64,
    pub at: i64,
    pub files: i64,
    pub bytes: Option<i64>,
    /// Decision-wide by construction — one apply, one mode. The same
    /// assumption `aggregate_placement_lines` already records.
    pub disposition: Option<OriginDisposition>,
    /// Absent where none was recorded. Reasons are optional and never
    /// prompted, so absence is a real absence, not a gap in the record.
    pub reason: Option<String>,
    /// Every (origin, destination) pair this decision recorded across the
    /// counterpart, uncapped — the display cap is the interface's, beside
    /// the identical one `drew from:` already applies to the same shape.
    pub places: Vec<CrossingPlace>,
}

/// One (origin, destination) pair a delivery recorded.
///
/// The two locations and nothing else: the delivery's own line above already
/// carries the counts, and a per-pair count nothing renders would be a field
/// recording demand that does not exist.
pub struct CrossingPlace {
    pub origin: String,
    pub destination: String,
}

pub enum NothingCrossed {
    /// Rows exist, but every one of them crossed nothing. Name the
    /// rearrangement, or "nothing crossed" reads as "nothing ever happened
    /// here" — the opposite of the truth about a curated archive.
    Rearranged { files: i64, bytes: Option<i64> },
    /// No recorded crossing at all between the view and what was asked about.
    Nothing,
}

/// Two observations about one origin, side by side: how much it delivered
/// (the ledger's count) and how much of that still stands here (the card's).
///
/// The gap is **never decomposed**. Its causes — deleted since, moved on
/// again, or later re-transitioned in place — are indistinguishable from
/// these rows, so naming any subset of them would state a cause Canon cannot
/// know.
pub struct Reconciliation {
    pub standing: i64,
    pub delivered: i64,
}

pub fn compute_crossings(conn: &Connection, params: &CrossingsParams) -> Result<Crossings> {
    let roots = repo::root::fetch_all(conn)?;
    // The trail's established idiom for this table: one scan of an
    // aggregate-only index, with path matching done in domain code. A
    // root-id-keyed query would go silent on exactly the counterparts this
    // door exists for — the removed and the retired.
    let rows = crate::trail::repo::fetch_all_extractions(conn)?;

    let named: Vec<&String> = [params.origin.as_ref(), params.destination.as_ref()]
        .into_iter()
        .flatten()
        .collect();
    let mut unknown = Vec::new();
    for path in &named {
        if !counterpart_is_known(&rows, path, conn, &roots)? {
            unknown.push((*path).clone());
        }
    }
    if !unknown.is_empty() {
        return Ok(Crossings::UnknownCounterpart(unknown));
    }

    let filter = CrossingFilter {
        view: &params.prefixes,
        origin: params.origin.as_ref(),
        destination: params.destination.as_ref(),
    };

    let mut crossed: Vec<(&DecisionExtraction, RowAspect)> = Vec::new();
    let mut rearranged: Vec<&DecisionExtraction> = Vec::new();
    for row in &rows {
        match crossing_verdict(row, &filter) {
            CrossingVerdict::Crossed(aspect) => crossed.push((row, aspect)),
            CrossingVerdict::Rearranged => rearranged.push(row),
            CrossingVerdict::NotOurs => {}
        }
    }

    let mut ids: Vec<i64> = crossed.iter().map(|(row, _)| row.decision_id).collect();
    ids.sort_unstable();
    ids.dedup();
    let decisions_by_id: HashMap<i64, Decision> = repo::decision::fetch_by_ids(conn, &ids)?
        .into_iter()
        .map(|d| (d.id, d))
        .collect();

    let mut sections = Vec::new();
    for aspect in [RowAspect::Extraction, RowAspect::Arrival] {
        if let Some(section) =
            build_section(aspect, &crossed, &decisions_by_id, &roots, conn, params)?
        {
            sections.push(section);
        }
    }

    let nothing_crossed = if !sections.is_empty() {
        None
    } else if rearranged.is_empty() {
        Some(NothingCrossed::Nothing)
    } else {
        let (files, bytes, _) =
            rollup_parts(rearranged.into_iter(), |r| &r.destination_path).unwrap_or((0, None, 0));
        Some(NothingCrossed::Rearranged { files, bytes })
    };

    let reconciliation = reconcile(conn, params, &sections)?;

    let mut decisions: Vec<Decision> = decisions_by_id.into_values().collect();
    decisions.sort_by_key(|d| (d.created_at, d.id));
    let listed: Vec<i64> = decisions.iter().map(|d| d.id).collect();
    let mut extractions_all: HashMap<i64, Vec<DecisionExtraction>> = HashMap::new();
    for row in &rows {
        if listed.contains(&row.decision_id) {
            extractions_all
                .entry(row.decision_id)
                .or_default()
                .push(row.clone());
        }
    }

    Ok(Crossings::Reported(Box::new(CrossingsResult {
        sections,
        nothing_crossed,
        reconciliation,
        decisions,
        extractions_all,
    })))
}

/// Whether Canon records this place as a counterpart at all.
///
/// The **ledger arm runs first**, and answers from rows already in hand: a
/// counterpart is by definition a place the ledger names, and it is the one
/// evidence class that survives its root being removed. That is the whole
/// motivating case here, and the one `history_evidence_at` cannot reach —
/// that function opens on `find_containing_root` and returns false for a path
/// under no live root, so a plain-`rm`'d origin root would be reported
/// unknown and this door would refuse to open on a line the composition card
/// printed a second earlier.
///
/// Then the existing chain: a retirement covering the path answers, then the
/// four evidence classes. `ops::place` is untouched — the gate is conjugated
/// for this door, not widened for every caller of that one.
///
/// Matching in the ledger arm is `placement_in_view` on either endpoint —
/// descendant-or-equal, the same direction as everything else here.
fn counterpart_is_known(
    rows: &[DecisionExtraction],
    path: &str,
    conn: &Connection,
    roots: &[Root],
) -> Result<bool> {
    if rows.iter().any(|row| {
        placement_in_view(path, &row.drawn_from()) || placement_in_view(path, &row.destination_path)
    }) {
        return Ok(true);
    }
    Ok(match place_knowledge(conn, path, roots)? {
        PlaceKnowledge::Retired(_) | PlaceKnowledge::Evidence => true,
        PlaceKnowledge::Unknown => false,
    })
}

/// The counterpart a flag named for this aspect, if it named one.
///
/// A section's counterpart is its **outside** end — origin for an arrival,
/// destination for an extraction — which is the rollup key rule already, and
/// which is what lets both flags compose with no special case: naming an
/// origin opens the arrival section and leaves the extraction section listing
/// its destinations as usual.
fn named_for(aspect: RowAspect, params: &CrossingsParams) -> Option<&String> {
    match aspect {
        RowAspect::Arrival => params.origin.as_ref(),
        RowAspect::Extraction => params.destination.as_ref(),
        RowAspect::Rearrangement | RowAspect::Outside => None,
    }
}

fn build_section(
    aspect: RowAspect,
    crossed: &[(&DecisionExtraction, RowAspect)],
    decisions: &HashMap<i64, Decision>,
    roots: &[Root],
    conn: &Connection,
    params: &CrossingsParams,
) -> Result<Option<CrossingSection>> {
    let rows: Vec<&DecisionExtraction> = crossed
        .iter()
        .filter(|(_, a)| *a == aspect)
        .map(|(row, _)| *row)
        .collect();
    // The same builder the whole-history rollups use, over the same rows and
    // the same counterparty key: the section header is the rollup line, so it
    // must be the rollup's arithmetic rather than a second copy of it.
    let Some((files, bytes, counterparty_count)) =
        rollup_parts(rows.iter().copied(), counterparty_fn(aspect))
    else {
        return Ok(None);
    };

    let named = match named_for(aspect, params) {
        Some(path) => Some(counterpart_at(path, roots, conn)?),
        None => None,
    };
    let body = match &named {
        Some(_) => build_deliveries(&rows, decisions, params.limit),
        None => build_counterparts(&rows, decisions, aspect, params.limit, roots, conn)?,
    };

    Ok(Some(CrossingSection {
        aspect,
        files,
        bytes,
        counterparty_count,
        named,
        body,
    }))
}

/// Which end of a row this aspect counts as its counterparty — the rollups'
/// own choice, reached through the domain's one spelling of it.
fn counterparty_fn(aspect: RowAspect) -> fn(&DecisionExtraction) -> &str {
    match aspect {
        RowAspect::Extraction => |row| counterpart_of(row, RowAspect::Extraction),
        RowAspect::Arrival => |row| counterpart_of(row, RowAspect::Arrival),
        RowAspect::Rearrangement | RowAspect::Outside => {
            |row| counterpart_of(row, RowAspect::Extraction)
        }
    }
}

/// A counterpart with what the index knows about the root behind it.
///
/// Removal is decided by containment among live roots rather than by a stored
/// id: a root removed and re-added carries a new id but the same location,
/// and a location that exists today is visitable today. The book lookup runs
/// only for a removed location, and its own liveness gate is what keeps a
/// bound-but-unreleased ceremony from marking a live place as bound history.
fn counterpart_at(path: &str, roots: &[Root], conn: &Connection) -> Result<Counterpart> {
    let root_removed = find_containing_root(path, roots).is_none();
    let retired_book = if root_removed {
        crate::retire::find_retirement_covering_path(conn, path)?.map(|r| r.book_display)
    } else {
        None
    };
    Ok(Counterpart {
        path: path.to_string(),
        root_removed,
        retired_book,
    })
}

/// The bare listing: one entry per counterpart, the rollup line itemized.
fn build_counterparts(
    rows: &[&DecisionExtraction],
    decisions: &HashMap<i64, Decision>,
    aspect: RowAspect,
    limit: Option<usize>,
    roots: &[Root],
    conn: &Connection,
) -> Result<CrossingBody> {
    let key = counterparty_fn(aspect);
    let mut groups: Vec<(&str, Vec<&DecisionExtraction>)> = Vec::new();
    for row in rows {
        match groups.iter_mut().find(|(k, _)| *k == key(row)) {
            Some((_, members)) => members.push(row),
            None => groups.push((key(row), vec![row])),
        }
    }

    let mut lines: Vec<CounterpartLine> = Vec::new();
    for (path, members) in groups {
        let (files, bytes, _) =
            rollup_parts(members.iter().copied(), key).expect("a group is never empty");
        let mut ids: Vec<i64> = members.iter().map(|r| r.decision_id).collect();
        ids.sort_unstable();
        ids.dedup();
        let stamps: Vec<i64> = ids
            .iter()
            .filter_map(|id| decisions.get(id).map(|d| d.created_at))
            .collect();
        lines.push(CounterpartLine {
            counterpart: counterpart_at(path, roots, conn)?,
            files,
            bytes,
            decisions: ids.len(),
            first_at: stamps.iter().copied().min().unwrap_or_default(),
            last_at: stamps.iter().copied().max().unwrap_or_default(),
        });
    }
    // Heaviest first — the reading order the card already uses — with a
    // lexicographic tie-break so repeated runs render identically.
    lines.sort_by(|a, b| {
        b.files
            .cmp(&a.files)
            .then(a.counterpart.path.cmp(&b.counterpart.path))
    });

    let more = cap(&mut lines, limit);
    Ok(CrossingBody::Counterparts { lines, more })
}

/// The named listing: one entry per decision, at row grain.
fn build_deliveries(
    rows: &[&DecisionExtraction],
    decisions: &HashMap<i64, Decision>,
    limit: Option<usize>,
) -> CrossingBody {
    let mut groups: Vec<(i64, Vec<&DecisionExtraction>)> = Vec::new();
    for row in rows {
        match groups.iter_mut().find(|(id, _)| *id == row.decision_id) {
            Some((_, members)) => members.push(row),
            None => groups.push((row.decision_id, vec![row])),
        }
    }

    let mut lines: Vec<CrossingDelivery> = groups
        .into_iter()
        .map(|(decision_id, members)| {
            let (files, bytes, _) = rollup_parts(members.iter().copied(), |r| &r.destination_path)
                .expect("a group is never empty");
            let decision = decisions.get(&decision_id);
            CrossingDelivery {
                decision_id,
                at: decision.map(|d| d.created_at).unwrap_or_default(),
                files,
                bytes,
                disposition: members[0].disposition,
                reason: decision.and_then(|d| d.reason.clone()),
                places: members
                    .iter()
                    .map(|row| CrossingPlace {
                        origin: row.drawn_from(),
                        destination: row.destination_path.clone(),
                    })
                    .collect(),
            }
        })
        .collect();
    // Chronological: a named counterpart is one relation read as a story, and
    // a story is read from its beginning. The remainder falls at the end,
    // where the reader is when they run out.
    lines.sort_by_key(|line| (line.at, line.decision_id));

    let more = cap(&mut lines, limit);
    CrossingBody::Deliveries { lines, more }
}

/// Truncate to the cap and return what was left out — never a silent
/// truncation; the caller states the remainder.
fn cap<T>(lines: &mut Vec<T>, limit: Option<usize>) -> usize {
    match limit {
        Some(limit) if lines.len() > limit => {
            let more = lines.len() - limit;
            lines.truncate(limit);
            more
        }
        _ => 0,
    }
}

/// The reconciliation line, or nothing.
///
/// Renders **only** when the view is scoped and human-readable, an origin was
/// named, and the composition card carries a `FromRoot` line whose root path
/// equals that origin **exactly**. Every other grain is absent rather than
/// guessed: a sub-root origin has no card number at that grain — the card
/// attributes at root level by design — and a `MultiOrigin` line cannot
/// attribute at all. Inventing a number for either is precisely what this
/// line exists to avoid.
///
/// **A named destination suppresses it too**, and this one is not a matter of
/// grain: `--destination` narrows the delivered count and narrows nothing
/// about the card, which answers for the whole view. Comparing the two then
/// puts a narrowed number beside an unnarrowed one and can state that more
/// files stand here than were ever delivered — arithmetically impossible, and
/// the one thing a line designed never to guess must never do. The two counts
/// are only comparable when both range over the same content.
///
/// The standing count comes from `compute_composition`, never a
/// re-derivation: one meaning, one spelling.
fn reconcile(
    conn: &Connection,
    params: &CrossingsParams,
    sections: &[CrossingSection],
) -> Result<Option<Reconciliation>> {
    if params.machine_output || params.prefixes.is_empty() || params.destination.is_some() {
        return Ok(None);
    }
    let Some(origin) = params.origin.as_ref() else {
        return Ok(None);
    };
    let Some(delivered) = sections
        .iter()
        .find(|s| s.aspect == RowAspect::Arrival)
        .map(|s| s.files)
    else {
        return Ok(None);
    };
    let Some(card) = crate::trail::ops::composition::compute_composition(conn, &params.prefixes)?
    else {
        return Ok(None);
    };
    let standing = card.origins.iter().find_map(|line| match line {
        OriginLine::FromRoot {
            root_path, files, ..
        } if root_path == origin => Some(*files),
        OriginLine::FromRoot { .. } | OriginLine::MultiOrigin { .. } => None,
    });
    Ok(standing.map(|standing| Reconciliation {
        standing,
        delivered,
    }))
}
