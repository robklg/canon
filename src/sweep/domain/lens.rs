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
/// default — tier, then weight, then counterpart standing, then residual
/// burden —
/// with a path tie-break so identical input always ranks identically.
#[derive(Debug, PartialEq)]
pub struct RankedSweep {
    pub entries: Vec<LeaderboardEntry>,
    pub stats: SweepStats,
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
    pub counterpart_suspended: bool,
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
    let mut entries: Vec<LeaderboardEntry> = Vec::new();
    let mut by_counterpart: HashMap<Location, Vec<StructuralFinding>> = HashMap::new();
    for finding in sweep.findings {
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
        let (counterpart_is_archive, counterpart_suspended, counterpart_last_scanned_at) = status;
        entries.push(LeaderboardEntry::Hub(HubEntry {
            counterpart,
            counterpart_is_archive,
            counterpart_suspended,
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
        stats: sweep.stats,
    }
}

fn tier_rank(tier: FindingTier) -> u8 {
    match tier {
        FindingTier::Clean => 0,
        FindingTier::Candidate => 1,
    }
}

/// The counterpart's standing — how safely the finding's redundancy claim
/// can be acted on: an archived counterpart outranks a merely-present one,
/// which outranks a suspended one; scattered coverage with nothing archived
/// ranks last. A lens constant, derived from lens-free facts — never stored
/// on the finding.
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

/// The reduction lens's ordering: tier, weight (size-led), counterpart
/// standing, residual burden. Lower sorts first.
fn rank_key(finding: &StructuralFinding) -> (u8, Reverse<u64>, u8, u64) {
    (
        tier_rank(finding.tier),
        Reverse(finding.gain_bytes),
        counterpart_standing(finding),
        finding.residual_bytes,
    )
}

/// A hub competes as its aggregate: best member tier, summed gain, best
/// member safety, summed residual.
fn entry_key(entry: &LeaderboardEntry) -> (u8, Reverse<u64>, u8, u64) {
    match entry {
        LeaderboardEntry::Single(f) => rank_key(f),
        LeaderboardEntry::Hub(h) => (
            h.members
                .iter()
                .map(|m| tier_rank(m.tier))
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

fn pair_counterpart_status(finding: &StructuralFinding) -> (bool, bool, Option<i64>) {
    match &finding.shape {
        RelationShape::Pair {
            counterpart_is_archive,
            counterpart_suspended,
            counterpart_last_scanned_at,
            ..
        } => (
            *counterpart_is_archive,
            *counterpart_suspended,
            *counterpart_last_scanned_at,
        ),
        RelationShape::Coverage { .. } => {
            unreachable!("hub members are grouped from pair-shaped findings")
        }
    }
}
