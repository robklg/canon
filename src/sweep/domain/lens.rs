// ---------------------------------------------------------------------------
// Reduction lens — the v1 ranking derivation over the structural computation.
// A future lens is another function over the same `StructuralSweep`.
// ---------------------------------------------------------------------------

use std::cmp::Reverse;
use std::collections::HashMap;

use super::structural::{
    FindingNature, FindingTier, Location, RelationShape, StructuralFinding, StructuralSweep,
    SweepStats,
};

/// The leaderboard as the reduction lens ranks it: findings sharing a
/// counterpart grouped into hubs, everything ordered by the opinionated
/// default — tier, then archive standing, then weight, then counterpart
/// standing, then residual burden —
/// with a path tie-break so identical input always ranks identically.
/// Places behind a closed door are not in `entries` at all; they are
/// counted in `suspended`.
#[derive(Debug, PartialEq)]
pub struct RankedSweep {
    pub entries: Vec<LeaderboardEntry>,
    /// One entry per suspended root whose door kept places off the board,
    /// ordered by root path.
    pub suspended: Vec<SuspendedRootTally>,
    pub stats: SweepStats,
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

/// One leaderboard slot: a single finding, or a hub of findings that share
/// one counterpart.
#[derive(Debug, PartialEq)]
pub enum LeaderboardEntry {
    Single(StructuralFinding),
    Hub(HubEntry),
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
    /// Gain sums attribute each subject once — members are distinct
    /// subjects, deduped by the structural computation.
    pub total_gain_bytes: u64,
    pub total_gain_files: u32,
}

/// Rank the structural findings under the reduction lens.
pub fn reduction_lens(sweep: StructuralSweep) -> RankedSweep {
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
    let mut by_counterpart: HashMap<Location, Vec<StructuralFinding>> = HashMap::new();
    for finding in claimable {
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
            rank_key(a)
                .cmp(&rank_key(b))
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
            total_gain_bytes: members.iter().map(|m| m.gain_bytes).sum(),
            total_gain_files: members.iter().map(|m| m.gain_files).sum(),
            members,
        }));
    }
    entries.sort_by(|a, b| {
        entry_key(a)
            .cmp(&entry_key(b))
            .then_with(|| entry_path(a).cmp(&entry_path(b)))
    });
    RankedSweep {
        entries,
        suspended,
        stats: sweep.stats,
    }
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

/// The reduction lens's ordering: tier, archive standing, weight (size-led),
/// counterpart standing, residual burden. Lower sorts first.
///
/// Archive standing sits directly after tier, ahead of weight: this is a
/// *reduction* board, and a place already standing in the archive holds no
/// unresolved content, so it cannot compete for triage attention on mass
/// alone. Demoted, never removed — it keeps its `(in the archive)` marker and
/// its claiming value. Tier still leads: whether the statement is trustworthy
/// enough to act on is a prior question to what acting would resolve.
fn rank_key(finding: &StructuralFinding) -> (u8, u8, Reverse<u64>, u8, u64) {
    (
        tier_rank(finding.tier),
        u8::from(finding.subject_is_archive),
        Reverse(finding.gain_bytes),
        counterpart_standing(finding),
        finding.residual_bytes,
    )
}

/// A hub competes as its aggregate: best member tier, best member archive
/// standing, summed gain, best member safety, summed residual. The archive
/// term reads the *members'* subjects, so a hub of live source places under
/// an archive counterpart still competes as source and may top the board —
/// the rule is about the subject side only.
fn entry_key(entry: &LeaderboardEntry) -> (u8, u8, Reverse<u64>, u8, u64) {
    match entry {
        LeaderboardEntry::Single(f) => rank_key(f),
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
