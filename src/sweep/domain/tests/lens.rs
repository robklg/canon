//! Reduction lens tests — the v1 ranking derivation over the structural
//! computation.

use crate::sweep::domain::lens::{
    counterpart_standing, reduction_lens, LeaderboardEntry, RankedSweep,
};
use crate::sweep::domain::structural::{
    FindingNature, FindingTier, Location, RelationClass, RelationShape, StructuralFinding,
    StructuralSweep, SweepStats,
};

use super::fixtures::{lens_finding, lens_loc, low_floors, run_structural, scale_fixture};

fn lens_pair(counterpart: Location) -> RelationShape {
    RelationShape::Pair {
        counterpart,
        class: RelationClass::Subset,
        pair_size_pct: 0.96,
        pair_count_pct: 0.9,
        counterpart_share_pct: 0.5,
        counterpart_suspended: false,
        counterpart_is_archive: false,
        counterpart_last_scanned_at: None,
    }
}

fn lens_pair_suspended(counterpart: Location) -> RelationShape {
    RelationShape::Pair {
        counterpart,
        class: RelationClass::Subset,
        pair_size_pct: 0.96,
        pair_count_pct: 0.9,
        counterpart_share_pct: 0.5,
        counterpart_suspended: true,
        counterpart_is_archive: false,
        counterpart_last_scanned_at: None,
    }
}

fn lens(findings: Vec<StructuralFinding>) -> RankedSweep {
    reduction_lens(StructuralSweep {
        findings,
        stats: SweepStats {
            ubiquitous_objects_dropped: 0,
            ubiquitous_bytes_dropped: 0,
            below_floor_subjects: 0,
        },
    })
}

fn entry_labels(ranked: &RankedSweep) -> Vec<String> {
    ranked
        .entries
        .iter()
        .map(|e| match e {
            LeaderboardEntry::Single(f) => f.subject.rel_prefix.clone(),
            LeaderboardEntry::Hub(h) => format!("hub:{}", h.counterpart.rel_prefix),
        })
        .collect()
}

#[test]
fn tier_outranks_weight() {
    use FindingNature::Consolidate;
    let ranked = lens(vec![
        lens_finding(
            "big-candidate",
            FindingTier::Candidate,
            1_000_000,
            0,
            Consolidate,
            lens_pair(lens_loc("/r2", "a")),
        ),
        lens_finding(
            "small-clean",
            FindingTier::Clean,
            1_000,
            0,
            Consolidate,
            lens_pair(lens_loc("/r2", "b")),
        ),
    ]);
    assert_eq!(entry_labels(&ranked), ["small-clean", "big-candidate"]);
}

#[test]
fn weight_orders_within_tier() {
    use FindingNature::Consolidate;
    let ranked = lens(vec![
        lens_finding(
            "lighter",
            FindingTier::Clean,
            5_000,
            0,
            Consolidate,
            lens_pair(lens_loc("/r2", "a")),
        ),
        lens_finding(
            "heavier",
            FindingTier::Clean,
            10_000,
            0,
            Consolidate,
            lens_pair(lens_loc("/r2", "b")),
        ),
    ]);
    assert_eq!(entry_labels(&ranked), ["heavier", "lighter"]);
}

#[test]
fn counterpart_standing_orders_equal_weights() {
    // Names are reverse-alphabetical so a path sort would invert the
    // expected order — only counterpart standing can produce it.
    let ranked = lens(vec![
        lens_finding(
            "w-scattered",
            FindingTier::Clean,
            1_000,
            0,
            FindingNature::Consolidate,
            RelationShape::Coverage {
                locations: 4,
                archived_locations: 0,
            },
        ),
        lens_finding(
            "x-verify",
            FindingTier::Clean,
            1_000,
            0,
            FindingNature::Verify,
            lens_pair_suspended(lens_loc("/r2", "b")),
        ),
        lens_finding(
            "y-present",
            FindingTier::Clean,
            1_000,
            0,
            FindingNature::Consolidate,
            lens_pair(lens_loc("/r2", "c")),
        ),
        lens_finding(
            "z-dismiss",
            FindingTier::Clean,
            1_000,
            0,
            FindingNature::Dismiss,
            lens_pair(lens_loc("/r2", "d")),
        ),
    ]);
    assert_eq!(
        entry_labels(&ranked),
        ["z-dismiss", "y-present", "x-verify", "w-scattered"]
    );
}

#[test]
fn scattered_with_archived_locations_ranks_as_present() {
    let covered = lens_finding(
        "s",
        FindingTier::Clean,
        1_000,
        0,
        FindingNature::Consolidate,
        RelationShape::Coverage {
            locations: 4,
            archived_locations: 2,
        },
    );
    let bare = lens_finding(
        "s",
        FindingTier::Clean,
        1_000,
        0,
        FindingNature::Consolidate,
        RelationShape::Coverage {
            locations: 4,
            archived_locations: 0,
        },
    );
    assert_eq!(counterpart_standing(&covered), 1);
    assert_eq!(counterpart_standing(&bare), 3);
}

#[test]
fn residual_burden_breaks_safety_ties() {
    use FindingNature::Consolidate;
    let ranked = lens(vec![
        lens_finding(
            "z-clean-dismissal",
            FindingTier::Clean,
            1_000,
            0,
            Consolidate,
            lens_pair(lens_loc("/r2", "a")),
        ),
        lens_finding(
            "a-burdened",
            FindingTier::Clean,
            1_000,
            500,
            Consolidate,
            lens_pair(lens_loc("/r2", "b")),
        ),
    ]);
    assert_eq!(entry_labels(&ranked), ["z-clean-dismissal", "a-burdened"]);
}

#[test]
fn path_breaks_full_ties_regardless_of_input_order() {
    use FindingNature::Consolidate;
    let build = |order_flipped: bool| {
        let mut findings = vec![
            lens_finding(
                "alpha",
                FindingTier::Clean,
                1_000,
                0,
                Consolidate,
                lens_pair(lens_loc("/r2", "a")),
            ),
            lens_finding(
                "beta",
                FindingTier::Clean,
                1_000,
                0,
                Consolidate,
                lens_pair(lens_loc("/r2", "b")),
            ),
        ];
        if order_flipped {
            findings.reverse();
        }
        lens(findings)
    };
    assert_eq!(entry_labels(&build(false)), ["alpha", "beta"]);
    assert_eq!(build(false), build(true));
}

#[test]
fn shared_counterpart_forms_hub_ranked_as_aggregate() {
    use FindingNature::Consolidate;
    let hub_cp = lens_loc("/r2", "hub");
    let ranked = lens(vec![
        lens_finding(
            "a",
            FindingTier::Clean,
            10,
            0,
            Consolidate,
            lens_pair(hub_cp.clone()),
        ),
        lens_finding(
            "b",
            FindingTier::Clean,
            20,
            0,
            Consolidate,
            lens_pair(hub_cp.clone()),
        ),
        lens_finding(
            "solo-under",
            FindingTier::Clean,
            25,
            0,
            Consolidate,
            lens_pair(lens_loc("/r2", "other")),
        ),
        lens_finding(
            "solo-over",
            FindingTier::Clean,
            40,
            0,
            Consolidate,
            lens_pair(lens_loc("/r2", "third")),
        ),
    ]);
    // The hub competes with its 30-byte total: above solo-under (25),
    // below solo-over (40).
    assert_eq!(
        entry_labels(&ranked),
        ["solo-over", "hub:hub", "solo-under"]
    );
    let LeaderboardEntry::Hub(hub) = &ranked.entries[1] else {
        panic!("expected hub entry");
    };
    assert_eq!(hub.total_gain_bytes, 30);
    assert_eq!(hub.total_gain_files, 20);
    // Members ranked within by the same key: heavier first.
    let member_paths: Vec<&str> = hub
        .members
        .iter()
        .map(|m| m.subject.rel_prefix.as_str())
        .collect();
    assert_eq!(member_paths, ["b", "a"]);
    assert!(!hub.counterpart_is_archive);
    assert!(!hub.counterpart_suspended);
}

#[test]
fn coverage_findings_never_group() {
    use FindingNature::Consolidate;
    let shape = || RelationShape::Coverage {
        locations: 3,
        archived_locations: 0,
    };
    let ranked = lens(vec![
        lens_finding("one", FindingTier::Clean, 10, 0, Consolidate, shape()),
        lens_finding("two", FindingTier::Clean, 10, 0, Consolidate, shape()),
    ]);
    assert!(ranked
        .entries
        .iter()
        .all(|e| matches!(e, LeaderboardEntry::Single(_))));
    assert_eq!(ranked.entries.len(), 2);
}

#[test]
fn lens_groups_scale_star_into_one_hub() {
    let (sources, roots) = scale_fixture();
    let sweep = run_structural(&sources, &roots, &low_floors());
    let ranked = reduction_lens(sweep);
    let star = ranked
        .entries
        .iter()
        .find_map(|e| match e {
            LeaderboardEntry::Hub(h)
                if h.counterpart.root_path == "/r2" && h.counterpart.rel_prefix == "hub" =>
            {
                Some(h)
            }
            _ => None,
        })
        .expect("the star renders as one hub entry");
    assert_eq!(star.members.len(), 30);
}
