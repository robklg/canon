// ---------------------------------------------------------------------------
// Reduction lens — the v1 ranking derivation over the structural computation.
// A future lens is another function over the same `StructuralSweep`.
// ---------------------------------------------------------------------------

use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};

use super::structural::{
    FindingNature, FindingTier, Location, RelationClass, RelationShape, StructuralFinding,
    StructuralSweep, SweepStats,
};
use crate::core::domain::path::path_is_under;
use crate::core::domain::resolution::unresolved_remainder;
use crate::core::domain::root::Root;
use crate::core::domain::source::Source;

/// The leaderboard as the reduction lens ranks it: findings sharing a
/// counterpart grouped into hubs, everything ordered by the opinionated
/// default — tier, then archive standing, then root nearness, then weight,
/// then counterpart standing, then residual burden —
/// with a path tie-break so identical input always ranks identically.
/// Places behind a closed door are not in `entries` at all; they are
/// counted in `suspended`.
#[derive(Debug, PartialEq)]
pub struct RankedSweep {
    pub entries: Vec<LeaderboardEntry>,
    /// One entry per suspended root whose door kept places off the board,
    /// ordered by root path.
    pub suspended: Vec<SuspendedRootTally>,
    /// Roots whose remainder the board states beside an entry, by root id —
    /// exactly those inside the lens's regime. The board carries no composite
    /// score, so the order explains itself by stating its factors, and this
    /// line's presence is the explanation for a place that outranked a
    /// heavier one.
    ///
    /// **The set is the regime, and so is the ordering term.** Every ordering
    /// key reads `ranking_bucket`, which ties every root above the regime, so
    /// "the board states this root" and "nearness could have separated this
    /// entry" are one condition rather than two rules that could drift.
    ///
    /// It is regime membership, **not** a per-board counterfactual: two
    /// entries inside the regime and on the same bucket both state their
    /// remainder though nearness separated neither of them. That reading was
    /// weighed and declined — computing it would mean sorting the board twice
    /// to see what moved.
    pub stated_remainders: HashMap<i64, i64>,
    /// Places that mirror each other, by the surviving place's subject: two
    /// entries stating one overlap from opposite ends took one slot, and the
    /// fact the collapse would otherwise have deleted is stated on the one
    /// that stayed.
    pub reciprocal_places: HashMap<Location, Location>,
    pub stats: SweepStats,
}

/// Per-root remainder, projected from the resolution account: the retirement
/// readiness review's own measure of what is left on a root, never a second
/// count of the same thing.
///
/// **Source roots only.** An archive root is never retired, so nearness says
/// nothing about one; it carries no projection and buckets at the far end of
/// the scale, where every archive subject ties and the order falls through to
/// gain.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct RootNearness {
    remaining: HashMap<i64, i64>,
}

impl RootNearness {
    /// Project every source root's remainder from rows already fetched.
    ///
    /// `sources` is the sweep's **whole** fetch, not its kept slice: the two
    /// give the same count today (excluded and contentless rows classify as
    /// neither unresolved nor anything this reads), but the whole fetch is
    /// the identical *input* the readiness review itself passes, so the two
    /// cannot diverge if `classify_present` ever changes. The kept slice is
    /// right there and looks like the obvious saving; this is why it is not
    /// taken.
    pub fn project(roots: &[Root], sources: &[Source], archived: &HashSet<i64>) -> Self {
        let mut by_root: HashMap<i64, Vec<&Source>> = HashMap::new();
        for source in sources {
            by_root.entry(source.root_id).or_default().push(source);
        }
        let remaining = roots
            .iter()
            .filter(|root| root.role != "archive")
            .map(|root| {
                let rows = by_root.remove(&root.id).unwrap_or_default();
                (root.id, unresolved_remainder(&rows, archived))
            })
            .collect();
        Self { remaining }
    }

    pub fn remaining(&self, root_id: i64) -> Option<i64> {
        self.remaining.get(&root_id).copied()
    }

    /// Order of magnitude, ascending: 0 → 0, 1..9 → 1, 10..99 → 2,
    /// 100..999 → 3, 1000 and up → 4; the `u8` maximum — the far end of the
    /// scale — where there is no projection.
    ///
    /// This is the **only** place the cut points are written. They are coarse
    /// on purpose: a leaderboard that reshuffles when nothing the user did
    /// changed reads as broken, so a root's remainder moving by one never
    /// moves the board unless it crosses a decade. Bucket 0 is exactly
    /// `Readiness::NoBlockersFound` — a join with the review's own verdict,
    /// not a taste.
    ///
    /// The raw measure. Nothing **orders** on it: every ordering key reads
    /// `ranking_bucket` instead.
    pub fn bucket(&self, root_id: i64) -> u8 {
        match self.remaining(root_id) {
            None => OUT_OF_REGIME,
            Some(n) if n <= 0 => 0,
            Some(n) if n < 10 => 1,
            Some(n) if n < 100 => 2,
            Some(n) if n < 1_000 => 3,
            Some(_) => 4,
        }
    }

    /// The bucket **as the ordering key reads it**: itself inside the lens's
    /// regime, and a single tie value above it.
    ///
    /// This is what makes the board's promise hold in both directions. The
    /// term exists for roots *near retirement*; letting it separate two roots
    /// that are both far from done bought nothing the reader could act on and
    /// demoted heavier findings for a reason no line could explain, because
    /// the line only ever speaks inside the regime. Tying above it makes
    /// ranking and statement coextensive by construction rather than by a
    /// second rule that could drift from the first.
    pub fn ranking_bucket(&self, root_id: i64, params: &LensParams) -> u8 {
        let bucket = self.bucket(root_id);
        if bucket <= params.nearness_render_bucket {
            bucket
        } else {
            OUT_OF_REGIME
        }
    }
}

/// Where every root that nearness does not separate ties: roots above the
/// lens's regime, and roots with no projection at all (archive roots). One
/// value, so "nearness decided nothing here" is a single fact.
const OUT_OF_REGIME: u8 = u8::MAX;

/// How many sources stand under each place, projected from rows the sweep
/// already fetched — the one thing the sibling-parent axis needs that a
/// finding does not carry.
///
/// **The population is the comparison-participating slice**, the same one
/// `compute_structural` is given: present, non-excluded, non-contentless.
/// Excluded content is resolution rather than overlap, so an excluded row puts
/// nothing further at stake under a parent; a contentless one is carried with
/// its place and resolved with the place's own fate, so it is not at risk
/// either. Counting either would make a parent's coverage read lower than the
/// risk warrants, and would measure the numerator and the denominator over two
/// different populations.
///
/// Built in ops from rows already in hand and handed to the lens, the way
/// `RootNearness` is: a lens input, never an engine output. Returning
/// per-folder subtree sums from the structural computation would put a ranking
/// input inside a computation the separation law calls lens-free.
#[derive(Debug, Default)]
pub struct PlaceCensus<'a> {
    /// Per root, every source's path, sorted. The sources under a folder are
    /// then one contiguous block, so a count is two binary searches rather
    /// than a scan — the axis asks this once per candidate parent and once per
    /// member, and a board may hold hundreds.
    by_root: HashMap<i64, Vec<&'a str>>,
}

impl<'a> PlaceCensus<'a> {
    pub fn project(sources: impl IntoIterator<Item = &'a Source>) -> Self {
        let mut by_root: HashMap<i64, Vec<&'a str>> = HashMap::new();
        for source in sources {
            by_root
                .entry(source.root_id)
                .or_default()
                .push(source.rel_path.as_str());
        }
        for paths in by_root.values_mut() {
            paths.sort_unstable();
        }
        Self { by_root }
    }

    /// Sources standing under `rel_prefix` on `root_id`; an empty prefix is
    /// the root's own top and counts every source on it.
    ///
    /// The block is bounded by the separator's own successor: `/` is `0x2F`,
    /// so every path under `p` sorts at or after `p/` and before `p0`, and
    /// nothing else can fall between the two — a path in that range must
    /// continue `p` with a byte in `['/', '0')`, and `/` is the only one. A
    /// source cannot sit *at* `p`, since `p` is a folder in the same tree.
    fn count_under(&self, root_id: i64, rel_prefix: &str) -> usize {
        let Some(paths) = self.by_root.get(&root_id) else {
            return 0;
        };
        if rel_prefix.is_empty() {
            return paths.len();
        }
        let mut lower = String::with_capacity(rel_prefix.len() + 1);
        lower.push_str(rel_prefix);
        lower.push('/');
        let mut upper = lower.clone();
        upper.pop();
        upper.push('0');
        let lo = paths.partition_point(|p| *p < lower.as_str());
        let hi = paths.partition_point(|p| *p < upper.as_str());
        hi - lo
    }
}

/// The lens's own calibratable constants, kept apart from the structural
/// `SweepParams` so the separation law reads in the types: recalibrating what
/// the board *shows* is a different act from recalibrating what the engine
/// *finds*.
/// `Eq` is deliberately absent: `sibling_parent_coverage` is a fraction, and a
/// total equality over one would be a claim about floats this type has no need
/// to make.
#[derive(Debug, Clone, PartialEq)]
pub struct LensParams {
    /// At or below this bucket, a source root claims its places into one root
    /// entry. Default 1 — fewer than ten unresolved sources remain.
    pub root_entry_bucket: u8,
    /// The lens's **regime**: at or below this bucket nearness both separates
    /// entries in the ordering and is stated on them; above it every root ties
    /// on the term and gain leads. Default 2.
    pub nearness_render_bucket: u8,
    /// At least this many sibling subjects before a parent claims a slot.
    /// Default 2 — grouping at two is rare rather than trigger-happy: on the
    /// board this axis was calibrated against, only four runs existed across
    /// seventy-two subject places.
    pub sibling_run_min: usize,
    /// The grouped members must account for at least this fraction of the
    /// parent's own sources, or the parent is not where the decision is and no
    /// entry forms. Default 0.60 — the calibration corpus has a clean gap
    /// between a 70%-and-up cluster and a 16%-and-below one, and the
    /// low-coverage cases are exactly where grouping buys least. At 14%,
    /// dismissing the parent would reach seven times further than the
    /// situation, so the parent must not be the headline.
    ///
    /// **Below the gate nothing is hidden**: the places compete individually,
    /// exactly as they did before this axis existed.
    pub sibling_parent_coverage: f64,
}

/// **`root_entry_bucket` must not exceed `nearness_render_bucket`.**
///
/// A root entry **always** states its remainder — that is the entry kind's own
/// criterion, not a consequence of the regime. So a root qualifying for one
/// from outside the regime would state a term that did **not** order it: its
/// key ties like every other out-of-regime root, and the line would appear
/// with nothing behind it. This invariant is what keeps that unreachable, and
/// it is why `stated_root`'s root-entry arm may answer unconditionally where
/// the single-finding arm must test. The two constants are free to move, but
/// not past each other.
pub const fn lens_params_invariant_holds(params: &LensParams) -> bool {
    params.root_entry_bucket <= params.nearness_render_bucket
}

impl Default for LensParams {
    fn default() -> Self {
        Self {
            root_entry_bucket: 1,
            nearness_render_bucket: 2,
            sibling_run_min: 2,
            sibling_parent_coverage: 0.60,
        }
    }
}

/// One suspended root's effect on the board: places on it, and places whose
/// copies are on it. Two causes, one root, one line.
///
/// **The two masses are never added together.** The axes are content-
/// correlated by construction — axis 1 counts places whose copies are *on*
/// this root, axis 0 counts the places *on* it, so the copies behind axis 1
/// generally sit inside the places behind axis 0 — and one merged figure
/// therefore states the same bytes twice. `HubEntry.total_gain_bytes` is not
/// a precedent for merging them: a hub's members all point *into* a
/// counterpart that is never itself a member, so its summands are separated
/// **by role** — the members are co-dismissable, the counterpart survives all
/// of them — and that is exactly the property these two axes lack, since the
/// root sits on both sides of the ledger. (Separated by role, not perfectly
/// disjoint: gain is union-outside containment while `concentrated()` only
/// requires the cited counterpart to hold `concentration_threshold` of it, so
/// a member's gain may rest up to that slack on another member. Bounded, and
/// a different order of thing from restating the same bytes.) Place-
/// disjointness — which the partition below does guarantee — licenses
/// counting places, never adding bytes.
///
/// Each figure is a sum of per-place resolution gain within its own cause: an
/// answer to "what were the places this door kept off the board worth", not a
/// count of distinct recoverable bytes.
#[derive(Clone, Debug, PartialEq)]
pub struct SuspendedRootTally {
    pub root_path: String,
    /// Places standing on this root.
    pub places_on_it: usize,
    /// Places elsewhere whose evidence stands on this root.
    pub places_with_copies_on_it: usize,
    /// Resolution gain of the places standing on this root.
    pub gain_bytes_on_it: u64,
    /// Resolution gain of the places whose evidence stands on this root.
    pub gain_bytes_with_copies_on_it: u64,
}

/// One leaderboard slot: a single finding, a near-retirable root claiming
/// every place on it, a run of siblings claimed by the parent they share, or a
/// hub of findings that share one counterpart.
///
/// **A slot is claimed by exactly one axis**, and the axes apply in a fixed
/// precedence: set-aside and sink first (the closed door, above), then
/// `Root`, then `Parent`, then `Hub`, then `Single` for everything left. A
/// place claimed by an earlier axis is unavailable to a later one — which is
/// why a root entry can leave a hub with one member, degrading it to singles
/// by the hub's own existing rule.
#[derive(Debug, PartialEq)]
pub enum LeaderboardEntry {
    Single(StructuralFinding),
    Root(RootEntry),
    Parent(ParentEntry),
    Hub(HubEntry),
}

/// Every place on one near-retirable source root, as a single slot: finishing
/// a root resolves more than any one place on it, and the board must say so
/// once rather than once per place left. Without this, nearness would rebuild
/// the flooding it was added to fix, on exactly the roots the work means to
/// celebrate.
///
/// The entry **states a remainder and never claims readiness**. Canon can
/// prove NOT READY and never proves the other side; the review the handoff
/// points at is what is entitled to judge.
#[derive(Debug, PartialEq)]
pub struct RootEntry {
    /// The root's own top — `rel_prefix` is empty by construction.
    pub root: Location,
    /// The retirement readiness review's own remainder measure, projected
    /// (`core::domain::resolution::unresolved_remainder`). A fact about the
    /// root; never a verdict about it.
    pub unresolved_remaining: i64,
    /// Ranked within by the same key that orders the leaderboard.
    pub members: Vec<StructuralFinding>,
    /// **An upper bound, and never a total.** `HubEntry.total_gain_bytes` is
    /// not a precedent: a hub's members all point *into* a counterpart that is
    /// never itself a member, so its summands are separated **by role** — the
    /// members are co-dismissable — and that is exactly the property a root's
    /// own places lack. Intra-root duplication is subtracted from the copies'
    /// common ancestor **upward** (`weights.rs`), so two sibling places below
    /// that ancestor each legitimately count the shared bytes as "exists
    /// outside me", and at most one of the two can ever be let go. Both
    /// numbers are true; their sum is not a statement about content.
    ///
    /// What is exactly true is the inequality, so this is rendered `up to`
    /// and **must never be named `gain`** — the same discipline the suspended
    /// footer's figures carry, for the same reason.
    pub gain_bytes_upper: u64,
    pub gain_files_upper: u32,
}

/// Findings whose subjects are siblings under one parent, as a single slot:
/// ten month folders each mirroring their own counterpart are one situation
/// told ten ways, and the board owes it one line.
///
/// The parent is an **entry**, never a **finding** — nothing is emitted for
/// it, no containment is computed for it, and no ancestor-descendant pair of
/// findings comes into existence. That is why the disjointness invariant is
/// untouched here by construction rather than by care.
///
/// **The claim is that this parent is where the decision is**, which is what
/// the coverage figure below both states and gates on. A run whose members
/// cover little of their parent is not one decision, and forms no entry: the
/// places compete individually, exactly as they did before this axis existed.
#[derive(Debug, PartialEq)]
pub struct ParentEntry {
    /// The shared parent. `rel_prefix` is the parent path — empty where the
    /// members sit directly on the root's own top — and the root is the
    /// members' root, which they share by construction: the grouping key is
    /// the root **and** the path, so a run cannot span two roots.
    pub parent: Location,
    /// Fraction of the parent's own sources that lie under its grouped
    /// members — stated on the entry, and the gate the entry had to clear.
    /// Counted over the comparison-participating slice (see `PlaceCensus`).
    pub coverage: f64,
    /// Ranked within by the same key that orders the board.
    pub members: Vec<StructuralFinding>,
    /// **An upper bound, and never a total.** `HubEntry.total_gain_bytes` is
    /// not a precedent, and its justification must not be carried across: a
    /// hub's members all point *into* a counterpart that is never itself a
    /// member, so its summands are separated **by role** and are
    /// co-dismissable. A sibling run has no such structure, and is in fact the
    /// exact shape the exposure was described from — intra-root duplication is
    /// subtracted from the copies' common ancestor **upward**
    /// (`domain/structural/weights.rs`), so two siblings sitting *below* that
    /// ancestor each legitimately count the other's copies as "exists outside
    /// me", and at most one of the two can ever be let go.
    ///
    /// Both members' numbers are true; their sum is not a statement about
    /// content. What is exactly true is the inequality, so this is rendered
    /// `up to` and **must never be named `gain`** — the same discipline the
    /// root entry's figure and the suspended footer's figures carry, for the
    /// same reason at a third site.
    pub gain_bytes_upper: u64,
    pub gain_files_upper: u32,
}

/// Findings sharing one counterpart, presented as one entry: a star reads
/// as one constellation, not as its points flooding the board.
#[derive(Debug, PartialEq)]
pub struct HubEntry {
    pub counterpart: Location,
    pub counterpart_is_archive: bool,
    pub counterpart_last_scanned_at: Option<i64>,
    /// Ranked within by the same key that orders the leaderboard.
    pub members: Vec<StructuralFinding>,
    /// The member root whose nearness set this hub's ordering term, when that
    /// term is inside the regime; `None` when nearness ties for this hub.
    /// Its `rel_prefix` is empty — a root as a place, the same shape
    /// `RootEntry.root` uses.
    ///
    /// Chosen deterministically (lowest bucket, then root path) and **carried
    /// rather than re-derived**: the interface must not pick which member
    /// explains the hub's position, and the ordering key reads this same field,
    /// so the rank and the line it prints cannot disagree.
    pub nearness_root: Option<Location>,
    /// Gain sums attribute each subject once — members are distinct
    /// subjects, deduped by the structural computation.
    pub total_gain_bytes: u64,
    pub total_gain_files: u32,
}

/// Rank the structural findings under the reduction lens.
pub fn reduction_lens(
    sweep: StructuralSweep,
    nearness: &RootNearness,
    census: &PlaceCensus<'_>,
    params: &LensParams,
) -> RankedSweep {
    debug_assert!(
        lens_params_invariant_holds(params),
        "a root entry must not be able to form outside the regime that states it"
    );
    // Places behind a closed door are computed always and ranked never: the
    // registry permits a suspended root exactly four things, and reading a
    // parked place for resolution is none of them — the default for a view
    // is to set the root aside. Nothing leaves the universe; only the board
    // declines to offer the place a slot.
    //
    // Partition before grouping, in this order — subject first, so a place
    // parked on both sides is counted once, on the root it stands on. Each
    // place therefore lands in exactly one root's tally and one cause within
    // it, which is what licenses the *counts*; the two masses stay apart
    // because place-disjointness says nothing about bytes (see the tally
    // type). Partitioning first also
    // means a hub that would have formed and then lost members never forms:
    // its aggregates are born correct rather than recomputed, and the board
    // is identical to one computed with the parked findings absent.
    let mut tallies: HashMap<String, SuspendedRootTally> = HashMap::new();
    let mut claimable: Vec<StructuralFinding> = Vec::new();
    for finding in sweep.findings {
        if finding.subject_suspended {
            let t = tally(&mut tallies, &finding.subject.root_path);
            t.places_on_it += 1;
            t.gain_bytes_on_it += finding.gain_bytes;
        } else if let Some(root) = parked_evidence_root(&finding).map(str::to_string) {
            let t = tally(&mut tallies, &root);
            t.places_with_copies_on_it += 1;
            t.gain_bytes_with_copies_on_it += finding.gain_bytes;
        } else {
            claimable.push(finding);
        }
    }
    debug_assert!(
        claimable.iter().all(|f| f.nature != FindingNature::Verify),
        "no ranked place waits on a closed door"
    );
    let mut suspended: Vec<SuspendedRootTally> = tallies.into_values().collect();
    suspended.sort_by(|a, b| a.root_path.cmp(&b.root_path));

    let mut entries: Vec<LeaderboardEntry> = Vec::new();

    // Axis 2 — the root, claiming ahead of the hub. Finishing a root resolves
    // more than any one place on it, so the root's claim on a place outranks a
    // shared counterpart's. Archive roots are excluded here by the projection
    // itself and not by a second test: `RootNearness` holds source roots only
    // (an archive root is never retired), so an archive subject buckets past
    // any threshold and never qualifies — one rule, spoken where nearness is
    // defined.
    let mut by_root: HashMap<i64, Vec<StructuralFinding>> = HashMap::new();
    let mut unclaimed: Vec<StructuralFinding> = Vec::new();
    for finding in claimable {
        if nearness.bucket(finding.subject.root_id) <= params.root_entry_bucket {
            by_root
                .entry(finding.subject.root_id)
                .or_default()
                .push(finding);
        } else {
            unclaimed.push(finding);
        }
    }
    for (root_id, mut members) in by_root {
        // One place is already one slot: a root entry there would be
        // furniture, not information — and the place carries the remainder
        // fact and the retirement handoff itself.
        if members.len() < 2 {
            unclaimed.extend(members);
            continue;
        }
        members.sort_by(|a, b| {
            rank_key(a, nearness, params)
                .cmp(&rank_key(b, nearness, params))
                .then_with(|| subject_path(a).cmp(&subject_path(b)))
        });
        let root = Location {
            root_id,
            root_path: members[0].subject.root_path.clone(),
            rel_prefix: String::new(),
        };
        entries.push(LeaderboardEntry::Root(RootEntry {
            root,
            unresolved_remaining: nearness.remaining(root_id).unwrap_or(0),
            gain_bytes_upper: members.iter().map(|m| m.gain_bytes).sum(),
            gain_files_upper: members.iter().map(|m| m.gain_files).sum(),
            members,
        }));
    }

    // Axis 3 — the sibling run, claiming ahead of the hub. The two group by
    // different things: a **hub groups by evidence** (many places, one
    // counterpart), a **sibling run groups by decision** (many places, one act
    // that covers them). This is a decision board and the headline is where
    // the decision is, so decision-grouping precedes evidence-grouping.
    //
    // Putting the hub first splits one situation: a folder whose places are
    // already partly hubbed would leave a hub *and* a parent entry for the one
    // folder. Putting the run first splits a cross-root hub instead, and that
    // split is correct — places under one root's folder and places under
    // another's are two decisions, not one. The hub is thereby the residual
    // grouping for places with no common decision unit, which is a coherent
    // role rather than a demotion.
    //
    // The parent is an entry and never a finding: nothing is emitted for it and
    // no containment is computed for it, so no ancestor-descendant pair of
    // findings can come into existence and the disjointness invariant is
    // untouched by construction.
    let mut by_parent: HashMap<(i64, String), Vec<StructuralFinding>> = HashMap::new();
    let mut ungrouped: Vec<StructuralFinding> = Vec::new();
    for finding in unclaimed {
        match parent_prefix(&finding.subject.rel_prefix) {
            Some(parent) => by_parent
                .entry((finding.subject.root_id, parent))
                .or_default()
                .push(finding),
            // A place that *is* its root's top has no parent to lift to.
            None => ungrouped.push(finding),
        }
    }
    for ((root_id, prefix), mut members) in by_parent {
        if members.len() < params.sibling_run_min {
            ungrouped.extend(members);
            continue;
        }
        let parent = Location {
            root_id,
            root_path: members[0].subject.root_path.clone(),
            rel_prefix: prefix,
        };
        // An entry that cannot state its coverage does not form. The figure is
        // the entry's own claim — that this parent is where the decision is —
        // and a census with nothing to say about the place cannot support it.
        // Unreachable in production, where every finding is built from the same
        // rows the census counts.
        let Some(coverage) = parent_coverage(census, &parent, &members) else {
            ungrouped.extend(members);
            continue;
        };
        // The gate, and the same figure the entry states. A run accounting for
        // little of its parent is not one decision: the headline would claim a
        // decision unit far larger than the situation, so the parent is not
        // where the decision is and no entry forms. Nothing is hidden by
        // declining — the places fall through and compete individually.
        if coverage < params.sibling_parent_coverage {
            ungrouped.extend(members);
            continue;
        }
        members.sort_by(|a, b| {
            rank_key(a, nearness, params)
                .cmp(&rank_key(b, nearness, params))
                .then_with(|| subject_path(a).cmp(&subject_path(b)))
        });
        entries.push(LeaderboardEntry::Parent(ParentEntry {
            parent,
            coverage,
            gain_bytes_upper: members.iter().map(|m| m.gain_bytes).sum(),
            gain_files_upper: members.iter().map(|m| m.gain_files).sum(),
            members,
        }));
    }

    let mut by_counterpart: HashMap<Location, Vec<StructuralFinding>> = HashMap::new();
    for finding in ungrouped {
        match &finding.shape {
            RelationShape::Pair { counterpart, .. } => by_counterpart
                .entry(counterpart.clone())
                .or_default()
                .push(finding),
            RelationShape::Coverage { .. } => entries.push(LeaderboardEntry::Single(finding)),
        }
    }
    for (counterpart, mut members) in by_counterpart {
        if members.len() < 2 {
            entries.extend(members.into_iter().map(LeaderboardEntry::Single));
            continue;
        }
        members.sort_by(|a, b| {
            rank_key(a, nearness, params)
                .cmp(&rank_key(b, nearness, params))
                .then_with(|| subject_path(a).cmp(&subject_path(b)))
        });
        let status = pair_counterpart_status(&members[0]);
        debug_assert!(
            members.iter().all(|m| pair_counterpart_status(m) == status),
            "hub members share one counterpart, so its status fields agree"
        );
        let (counterpart_is_archive, counterpart_last_scanned_at) = status;
        entries.push(LeaderboardEntry::Hub(HubEntry {
            counterpart,
            counterpart_is_archive,
            counterpart_last_scanned_at,
            nearness_root: hub_nearness_root(&members, nearness, params),
            total_gain_bytes: members.iter().map(|m| m.gain_bytes).sum(),
            total_gain_files: members.iter().map(|m| m.gain_files).sum(),
            members,
        }));
    }
    let reciprocal_places = collapse_reciprocal_places(&mut entries);
    entries.sort_by(|a, b| {
        entry_key(a, nearness, params)
            .cmp(&entry_key(b, nearness, params))
            .then_with(|| entry_path(a).cmp(&entry_path(b)))
    });
    // The remainder is stated exactly where nearness is in play — inside the
    // regime — and is silent everywhere else, which is the same set the
    // ordering term can separate on, because the key ties outside it. So the
    // line's absence now carries meaning too: nearness could not have moved
    // this entry. Its presence means nearness was in play here, which is
    // weaker than "nearness moved this one" and is the claim the board makes.
    //
    // Ranked, not *shown*: the cap runs after the lens, so this can carry a
    // root whose entry the cap later trims. Harmless — nothing renders a
    // remainder for an entry that is off the screen.
    let mut stated_remainders: HashMap<i64, i64> = HashMap::new();
    for entry in &entries {
        if let Some(root_id) = stated_root(entry, nearness, params) {
            if let Some(remaining) = nearness.remaining(root_id) {
                stated_remainders.insert(root_id, remaining);
            }
        }
    }
    RankedSweep {
        entries,
        suspended,
        stated_remainders,
        reciprocal_places,
        stats: sweep.stats,
    }
}

/// Two places that mirror **each other** are one overlap told twice, and one
/// decision resolves both.
///
/// Reciprocal-mirror dedup already runs in the engine, but it matches on the
/// cited counterpart being the *same place*, so it misses the pair that sits at
/// different depths: one place mirroring a **child** of the other, and the
/// other mirroring a child of the first. That takes two slots for one overlap —
/// the shape the requester met as `#1` and `#2` — and it recurs on live roots.
///
/// **Reciprocity is the whole criterion, and it is not weakened to one
/// direction.** A one-directional test would fold a chain: `A` inside `B` and
/// `B` inside `C` share the place `B` in opposite roles and are two genuine
/// situations. Only entries that point at each other are one.
///
/// Collapse keeps the entry whose subject sorts first, so an unchanged database
/// always keeps the same side, and **states the reciprocity on the survivor
/// rather than deleting a fact**: that the other place mirrors back is true, and
/// suppressing the duplicate slot must not suppress it.
///
/// **Entries headlined by a counterpart do not take part.** The test reads a
/// subject side, and a hub is headlined by the place its members point *into*,
/// with no single subject of its own; a root or parent entry is headlined by a
/// place that is no relation's counterpart at all. See the story record for the
/// question this leaves open.
///
/// `O(n²)` over entries is fine here: the board holds hundreds at most, and
/// `--all` is bounded by the same findings the lens already sorts.
fn collapse_reciprocal_places(entries: &mut Vec<LeaderboardEntry>) -> HashMap<Location, Location> {
    let mut sided: Vec<(usize, Location, Location)> = entries
        .iter()
        .enumerate()
        .filter_map(|(idx, entry)| match entry {
            LeaderboardEntry::Single(f) => match &f.shape {
                // **Mirrors only.** The claim the collapse makes is that one
                // decision resolves both places, and only a mutual mirror
                // carries it: each side is essentially the match, so the two
                // statements are about the same content seen from opposite
                // ends. Two reciprocal *subsets* are topologically identical
                // here and are not the same situation at all — each may hold
                // a majority of content the other never mentions, so folding
                // them would delete a real opportunity and print a false
                // claim over the survivor.
                RelationShape::Pair {
                    counterpart,
                    class: RelationClass::Mirror,
                    ..
                } => Some((idx, f.subject.clone(), counterpart.clone())),
                RelationShape::Pair {
                    class: RelationClass::Subset,
                    ..
                }
                | RelationShape::Coverage { .. } => None,
            },
            LeaderboardEntry::Root(_) | LeaderboardEntry::Parent(_) | LeaderboardEntry::Hub(_) => {
                None
            }
        })
        .collect();
    sided.sort_by(|a, b| location_key(&a.1).cmp(&location_key(&b.1)));

    let mut collapsed: HashSet<usize> = HashSet::new();
    let mut reciprocal_places: HashMap<Location, Location> = HashMap::new();
    for a in 0..sided.len() {
        for b in (a + 1)..sided.len() {
            let (idx_a, subject_a, counterpart_a) = &sided[a];
            let (idx_b, subject_b, counterpart_b) = &sided[b];
            if collapsed.contains(idx_a) || collapsed.contains(idx_b) {
                continue;
            }
            if contains(subject_a, counterpart_b) && contains(subject_b, counterpart_a) {
                // `sided` is ordered by subject, so `a` is the survivor.
                collapsed.insert(*idx_b);
                let previous = reciprocal_places.insert(subject_a.clone(), subject_b.clone());
                // One survivor states one mirror, and a second would be
                // overwritten in silence. It is unreachable rather than
                // handled: `A` mirroring both `B` and `C` needs `A`'s
                // counterpart under both `B` and `C`, which forces those two
                // to be ancestor-related on one root — a pair of findings the
                // disjointness invariant makes impossible. Pinned rather than
                // written in prose, so the day it stops being true is a
                // failure and not a deletion.
                debug_assert!(
                    previous.is_none(),
                    "a survivor states one mirror; a second would be lost"
                );
            }
        }
    }
    let mut idx = 0;
    entries.retain(|_| {
        let keep = !collapsed.contains(&idx);
        idx += 1;
        keep
    });
    reciprocal_places
}

/// Whether `place` is an ancestor of `other` or is `other` — within one root,
/// since containment across roots is not containment at all.
fn contains(place: &Location, other: &Location) -> bool {
    place.root_id == other.root_id && path_is_under(&other.rel_prefix, &place.rel_prefix)
}

fn location_key(loc: &Location) -> (&str, &str) {
    (&loc.root_path, &loc.rel_prefix)
}

/// The root an entry states a remainder for, or `None` where nearness
/// separated nothing. Read the *subject* side only: nearness is a fact about
/// the place being read for resolution, never about the counterpart it is
/// measured against.
///
/// This is the one place the board's rule lives — **a line appears exactly
/// where the entry's nearness term is inside the regime** — and every arm
/// answers from the same field the ordering key reads, so the two cannot
/// disagree. A root entry always states, unconditionally: its root cleared
/// `root_entry_bucket`, which the invariant keeps inside the regime.
fn stated_root(
    entry: &LeaderboardEntry,
    nearness: &RootNearness,
    params: &LensParams,
) -> Option<i64> {
    match entry {
        LeaderboardEntry::Single(f) => {
            let root_id = f.subject.root_id;
            (nearness.ranking_bucket(root_id, params) <= params.nearness_render_bucket)
                .then_some(root_id)
        }
        LeaderboardEntry::Root(r) => Some(r.root.root_id),
        // A run's members share one root by construction, so the entry's
        // nearness term is that root's and the same regime test applies as for
        // a single place standing on it.
        LeaderboardEntry::Parent(p) => {
            let root_id = p.parent.root_id;
            (nearness.ranking_bucket(root_id, params) <= params.nearness_render_bucket)
                .then_some(root_id)
        }
        LeaderboardEntry::Hub(h) => h.nearness_root.as_ref().map(|l| l.root_id),
    }
}

/// The member root that set a hub's nearness term: lowest bucket, root path
/// as the tie-break so an unchanged database always names the same member.
/// `None` when no member is inside the regime, which is exactly when the hub
/// ties on the term.
fn hub_nearness_root(
    members: &[StructuralFinding],
    nearness: &RootNearness,
    params: &LensParams,
) -> Option<Location> {
    members
        .iter()
        .map(|m| &m.subject)
        .filter(|s| nearness.ranking_bucket(s.root_id, params) <= params.nearness_render_bucket)
        .min_by(|a, b| {
            nearness
                .ranking_bucket(a.root_id, params)
                .cmp(&nearness.ranking_bucket(b.root_id, params))
                .then_with(|| a.root_path.cmp(&b.root_path))
        })
        .map(|s| Location {
            root_id: s.root_id,
            root_path: s.root_path.clone(),
            rel_prefix: String::new(),
        })
}

/// The immediate parent of a place within its root: `None` for the root's own
/// top, which has none, and `Some("")` for a place sitting directly on it.
///
/// **Grouping is at the immediate parent and never recurses.** The corpus
/// holds one nested case and it is decisive: a folder and its own child were
/// both run parents, and grouping at each gives two entries at 88% and 16%
/// coverage where recursing gives one at 16%. Lifting trades slots for
/// honesty, and coverage collapses fast — one level is where the trade still
/// pays. This is deliberately not a constant: a configurable depth would
/// invite a value the evidence says is always wrong.
fn parent_prefix(rel_prefix: &str) -> Option<String> {
    if rel_prefix.is_empty() {
        return None;
    }
    Some(match rel_prefix.rfind('/') {
        Some(cut) => rel_prefix[..cut].to_string(),
        None => String::new(),
    })
}

/// What fraction of a parent's own sources lie under its grouped members.
///
/// The members are distinct siblings — the discovery walk emits maximal
/// subjects and never a parent beside its child — so the numerator adds
/// disjoint sets and cannot exceed the denominator. `None` where the census
/// knows nothing of the parent, which is the one case an entry may not form
/// on: the figure is the entry's own claim, and an entry that cannot state it
/// has not earned a slot.
fn parent_coverage(
    census: &PlaceCensus<'_>,
    parent: &Location,
    members: &[StructuralFinding],
) -> Option<f64> {
    let own = census.count_under(parent.root_id, &parent.rel_prefix);
    if own == 0 {
        return None;
    }
    let under: usize = members
        .iter()
        .map(|m| census.count_under(m.subject.root_id, &m.subject.rel_prefix))
        .sum();
    Some(under as f64 / own as f64)
}

fn tally<'a>(
    tallies: &'a mut HashMap<String, SuspendedRootTally>,
    root_path: &str,
) -> &'a mut SuspendedRootTally {
    tallies
        .entry(root_path.to_string())
        .or_insert_with(|| SuspendedRootTally {
            root_path: root_path.to_string(),
            places_on_it: 0,
            places_with_copies_on_it: 0,
            gain_bytes_on_it: 0,
            gain_bytes_with_copies_on_it: 0,
        })
}

/// The suspended root a place's resolution waits on. `Verify` is derived in
/// assembly from `counterpart_suspended`, which only a pair shape carries,
/// so the coverage arm is unreachable by construction — it yields `None`
/// rather than panicking, and the assertion pins the implication.
///
/// Returning the root rather than a bool is deliberate: one function answers
/// both "does this place leave the board?" and "which root explains it?", so
/// the two can never disagree.
fn parked_evidence_root(f: &StructuralFinding) -> Option<&str> {
    match (f.nature, &f.shape) {
        (FindingNature::Verify, RelationShape::Pair { counterpart, .. }) => {
            Some(&counterpart.root_path)
        }
        (FindingNature::Verify, RelationShape::Coverage { .. }) => {
            debug_assert!(false, "Verify implies a pair shape");
            None
        }
        (FindingNature::Dismiss, _) | (FindingNature::Consolidate, _) => None,
    }
}

fn tier_rank(tier: FindingTier) -> u8 {
    match tier {
        FindingTier::Clean => 0,
        FindingTier::Candidate => 1,
    }
}

/// The counterpart's standing — how safely the finding's redundancy claim
/// can be acted on: an archived counterpart outranks a merely-present one;
/// scattered coverage with nothing archived ranks last. A lens constant,
/// derived from lens-free facts — never stored on the finding.
///
/// The `Verify` arm no longer orders anything: a place whose evidence stands
/// behind a closed door is set aside before ranking, and the partition's
/// `debug_assert` pins that. The arm stays because deleting it would force a
/// `_` rest onto an exhaustive match over `nature`, and `unreachable!()`
/// would trade a wrong order for a crash.
///
/// `pub` (not private): the domain test tree exercises this directly
/// (`counterpart_standing_orders_equal_weights`,
/// `scattered_with_archived_locations_ranks_as_present`) from its own
/// sibling module — the private `mod` chain up to the sealed barrel is what
/// keeps this out of reach from other subsystems.
pub fn counterpart_standing(finding: &StructuralFinding) -> u8 {
    match (finding.nature, &finding.shape) {
        (FindingNature::Dismiss, _) => 0,
        (FindingNature::Verify, _) => 2,
        (FindingNature::Consolidate, RelationShape::Pair { .. }) => 1,
        (
            FindingNature::Consolidate,
            RelationShape::Coverage {
                archived_locations, ..
            },
        ) => {
            if *archived_locations > 0 {
                1
            } else {
                3
            }
        }
    }
}

/// The reduction lens's ordering: tier, archive standing, root nearness,
/// weight (size-led), counterpart standing, residual burden. Lower sorts
/// first.
///
/// Archive standing sits directly after tier, ahead of weight: this is a
/// *reduction* board, and a place already standing in the archive holds no
/// unresolved content, so it cannot compete for triage attention on mass
/// alone. Demoted, never removed — it keeps its `(in the archive)` marker and
/// its claiming value. Tier still leads: whether the statement is trustworthy
/// enough to act on is a prior question to what acting would resolve.
///
/// Root nearness sits directly after archive standing and **ahead of weight**,
/// which is the whole point of the term: the places left on a root near the
/// end of its story are small by definition, so behind a size-led key they are
/// invisible exactly when they matter most. Finishing a root resolves more
/// than reclaiming bytes off one barely started. It sits *behind* archive
/// standing deliberately — nearness says nothing about an archive root, so
/// archive subjects tie there and fall through to gain.
fn rank_key(
    finding: &StructuralFinding,
    nearness: &RootNearness,
    params: &LensParams,
) -> (u8, u8, u8, Reverse<u64>, u8, u64) {
    (
        tier_rank(finding.tier),
        u8::from(finding.subject_is_archive),
        nearness.ranking_bucket(finding.subject.root_id, params),
        Reverse(finding.gain_bytes),
        counterpart_standing(finding),
        finding.residual_bytes,
    )
}

/// A hub competes as its aggregate: best member on each ordinal term (tier,
/// archive standing, nearness, counterpart safety), summed on each
/// quantitative one. The archive term reads the *members'* subjects, so a hub
/// of live source places under an archive counterpart still competes as source
/// and may top the board — the rule is about the subject side only, and
/// nearness reads the same side for the same reason.
fn entry_key(
    entry: &LeaderboardEntry,
    nearness: &RootNearness,
    params: &LensParams,
) -> (u8, u8, u8, Reverse<u64>, u8, u64) {
    match entry {
        LeaderboardEntry::Single(f) => rank_key(f, nearness, params),
        // A root entry competes the same way a hub does — best member on each
        // ordinal term, summed on each quantitative one — except on nearness,
        // which is the root's own and is what put the entry here.
        LeaderboardEntry::Root(r) => (
            r.members
                .iter()
                .map(|m| tier_rank(m.tier))
                .min()
                .unwrap_or(u8::MAX),
            r.members
                .iter()
                .map(|m| u8::from(m.subject_is_archive))
                .min()
                .unwrap_or(u8::MAX),
            nearness.ranking_bucket(r.root.root_id, params),
            Reverse(r.gain_bytes_upper),
            r.members
                .iter()
                .map(counterpart_standing)
                .min()
                .unwrap_or(u8::MAX),
            r.members.iter().map(|m| m.residual_bytes).sum(),
        ),
        // Same aggregation, and nearness read from the run's own root: its
        // members share one, so there is no minimum to take.
        LeaderboardEntry::Parent(p) => (
            p.members
                .iter()
                .map(|m| tier_rank(m.tier))
                .min()
                .unwrap_or(u8::MAX),
            p.members
                .iter()
                .map(|m| u8::from(m.subject_is_archive))
                .min()
                .unwrap_or(u8::MAX),
            nearness.ranking_bucket(p.parent.root_id, params),
            Reverse(p.gain_bytes_upper),
            p.members
                .iter()
                .map(counterpart_standing)
                .min()
                .unwrap_or(u8::MAX),
            p.members.iter().map(|m| m.residual_bytes).sum(),
        ),
        LeaderboardEntry::Hub(h) => (
            h.members
                .iter()
                .map(|m| tier_rank(m.tier))
                .min()
                .unwrap_or(u8::MAX),
            h.members
                .iter()
                .map(|m| u8::from(m.subject_is_archive))
                .min()
                .unwrap_or(u8::MAX),
            // The carried field, not a fresh minimum over the members: the
            // line the interface prints reads the same value, so the hub's
            // position and its explanation are one derivation.
            h.nearness_root
                .as_ref()
                .map(|l| nearness.ranking_bucket(l.root_id, params))
                .unwrap_or(OUT_OF_REGIME),
            Reverse(h.total_gain_bytes),
            h.members
                .iter()
                .map(counterpart_standing)
                .min()
                .unwrap_or(u8::MAX),
            h.members.iter().map(|m| m.residual_bytes).sum(),
        ),
    }
}

fn subject_path(finding: &StructuralFinding) -> (&str, &str) {
    (&finding.subject.root_path, &finding.subject.rel_prefix)
}

fn entry_path(entry: &LeaderboardEntry) -> (&str, &str) {
    match entry {
        LeaderboardEntry::Single(f) => subject_path(f),
        LeaderboardEntry::Root(r) => (&r.root.root_path, &r.root.rel_prefix),
        LeaderboardEntry::Parent(p) => (&p.parent.root_path, &p.parent.rel_prefix),
        LeaderboardEntry::Hub(h) => (&h.counterpart.root_path, &h.counterpart.rel_prefix),
    }
}

/// A hub never carries a suspension flag: every place sharing a parked
/// counterpart sinks in the partition above, so a hub with one cannot form.
fn pair_counterpart_status(finding: &StructuralFinding) -> (bool, Option<i64>) {
    match &finding.shape {
        RelationShape::Pair {
            counterpart_is_archive,
            counterpart_last_scanned_at,
            ..
        } => (*counterpart_is_archive, *counterpart_last_scanned_at),
        RelationShape::Coverage { .. } => {
            unreachable!("hub members are grouped from pair-shaped findings")
        }
    }
}
