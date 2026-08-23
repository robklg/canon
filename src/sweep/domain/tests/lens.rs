//! Reduction lens tests — the v1 ranking derivation over the structural
//! computation.

use crate::sweep::domain::lens::{
    counterpart_standing, reduction_lens, LeaderboardEntry, RankedSweep, SuspendedRootTally,
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

/// A place standing on a suspended root — the first sink axis.
fn on_suspended_root(mut f: StructuralFinding, root_path: &str) -> StructuralFinding {
    f.subject.root_path = root_path.to_string();
    f.subject_suspended = true;
    f
}

/// A place whose evidence stands on a suspended root — the second sink axis.
/// The engine derives `Verify` from `counterpart_suspended`; both are set
/// here because the fixture skips the engine.
fn evidence_on_suspended_root(rel: &str, gain_bytes: u64, root_path: &str) -> StructuralFinding {
    lens_finding(
        rel,
        FindingTier::Clean,
        gain_bytes,
        0,
        FindingNature::Verify,
        lens_pair_suspended(Location {
            root_id: 9,
            root_path: root_path.to_string(),
            rel_prefix: "parked".to_string(),
        }),
    )
}

fn tally<'a>(ranked: &'a RankedSweep, root_path: &str) -> &'a SuspendedRootTally {
    ranked
        .suspended
        .iter()
        .find(|t| t.root_path == root_path)
        .unwrap_or_else(|| panic!("no tally for {root_path}"))
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
                suspended_locations: 0,
            },
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
        ["z-dismiss", "y-present", "w-scattered"]
    );
    // `Verify` sits between present and scattered and orders nothing, because
    // a place whose evidence is behind a closed door never reaches the sort.
    // Pinned here directly rather than through a ranked list, which is what
    // it looked like before the sink.
    let verify = lens_finding(
        "x-verify",
        FindingTier::Clean,
        1_000,
        0,
        FindingNature::Verify,
        lens_pair_suspended(lens_loc("/r2", "b")),
    );
    assert_eq!(counterpart_standing(&verify), 2);
    assert!(lens(vec![verify]).entries.is_empty());
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
            suspended_locations: 0,
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
            suspended_locations: 0,
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
}

#[test]
fn coverage_findings_never_group() {
    use FindingNature::Consolidate;
    let shape = || RelationShape::Coverage {
        locations: 3,
        archived_locations: 0,
        suspended_locations: 0,
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

// ---------------------------------------------------------------------------
// The closed door: computed always, ranked never.
// ---------------------------------------------------------------------------

#[test]
fn a_place_on_a_suspended_root_does_not_rank() {
    let ranked = lens(vec![
        on_suspended_root(
            lens_finding(
                "parked",
                FindingTier::Clean,
                9_000,
                0,
                FindingNature::Consolidate,
                lens_pair(lens_loc("/r2", "a")),
            ),
            "/rs",
        ),
        lens_finding(
            "live",
            FindingTier::Clean,
            1_000,
            0,
            FindingNature::Consolidate,
            lens_pair(lens_loc("/r2", "b")),
        ),
    ]);
    // The heavier place would have topped the board; the door outranks gain.
    assert_eq!(entry_labels(&ranked), ["live"]);
    let t = tally(&ranked, "/rs");
    assert_eq!(t.places_on_it, 1);
    assert_eq!(t.places_with_copies_on_it, 0);
    assert_eq!(t.gain_bytes_on_it, 9_000);
    assert_eq!(t.gain_bytes_with_copies_on_it, 0);
}

#[test]
fn a_place_whose_counterpart_is_suspended_does_not_rank() {
    let ranked = lens(vec![
        evidence_on_suspended_root("waiting", 9_000, "/rs"),
        lens_finding(
            "live",
            FindingTier::Clean,
            1_000,
            0,
            FindingNature::Consolidate,
            lens_pair(lens_loc("/r2", "b")),
        ),
    ]);
    assert_eq!(entry_labels(&ranked), ["live"]);
    let t = tally(&ranked, "/rs");
    assert_eq!(t.places_on_it, 0);
    assert_eq!(t.places_with_copies_on_it, 1);
    assert_eq!(t.gain_bytes_on_it, 0);
    assert_eq!(t.gain_bytes_with_copies_on_it, 9_000);
}

#[test]
fn a_hub_with_a_suspended_counterpart_leaves_the_board_whole() {
    // Three places sharing one parked counterpart: the hub never forms, and
    // its members are never redistributed into slots of their own.
    let ranked = lens(vec![
        evidence_on_suspended_root("a", 3_000, "/rs"),
        evidence_on_suspended_root("b", 2_000, "/rs"),
        evidence_on_suspended_root("c", 1_000, "/rs"),
    ]);
    assert!(ranked.entries.is_empty());
    let t = tally(&ranked, "/rs");
    assert_eq!(t.places_with_copies_on_it, 3);
    assert_eq!(t.gain_bytes_with_copies_on_it, 6_000);
}

#[test]
fn a_place_suspended_on_both_sides_is_counted_once_on_the_root_it_stands_on() {
    let ranked = lens(vec![on_suspended_root(
        evidence_on_suspended_root("both", 5_000, "/rs-counterpart"),
        "/rs-subject",
    )]);
    assert!(ranked.entries.is_empty());
    assert_eq!(ranked.suspended.len(), 1);
    let t = tally(&ranked, "/rs-subject");
    assert_eq!(t.places_on_it, 1);
    assert_eq!(t.places_with_copies_on_it, 0);
    assert_eq!(t.gain_bytes_on_it, 5_000);
    assert_eq!(t.gain_bytes_with_copies_on_it, 0);
}

#[test]
fn a_mixed_hub_keeps_its_live_members_and_recomputes_its_totals() {
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
        on_suspended_root(
            lens_finding(
                "c",
                FindingTier::Clean,
                40,
                0,
                Consolidate,
                lens_pair(hub_cp.clone()),
            ),
            "/rs",
        ),
    ]);
    assert_eq!(entry_labels(&ranked), ["hub:hub"]);
    let LeaderboardEntry::Hub(hub) = &ranked.entries[0] else {
        panic!("expected hub entry");
    };
    assert_eq!(hub.members.len(), 2);
    // Born correct: 30, not 70 with a member struck out afterwards.
    assert_eq!(hub.total_gain_bytes, 30);
    assert_eq!(hub.total_gain_files, 20);
    assert_eq!(tally(&ranked, "/rs").places_on_it, 1);
}

#[test]
fn a_hub_falling_below_two_members_degrades_to_singles() {
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
        on_suspended_root(
            lens_finding(
                "b",
                FindingTier::Clean,
                20,
                0,
                Consolidate,
                lens_pair(hub_cp.clone()),
            ),
            "/rs",
        ),
    ]);
    assert_eq!(entry_labels(&ranked), ["a"]);
    assert!(matches!(ranked.entries[0], LeaderboardEntry::Single(_)));
}

#[test]
fn a_partitioned_board_matches_one_the_parked_places_never_reached() {
    // The equivalence the partition rests on, pinned rather than argued: the
    // board is identical to one computed with the parked places absent — hub
    // membership, hub totals, ordering and all. This is the stronger of the
    // two readings of "partition before grouping"; that a hub's totals are
    // born correct rather than struck out afterwards is pinned separately by
    // `a_mixed_hub_keeps_its_live_members_and_recomputes_its_totals`.
    use FindingNature::Consolidate;
    let hub_cp = lens_loc("/r2", "hub");
    let live = || {
        vec![
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
                "solo",
                FindingTier::Clean,
                25,
                0,
                Consolidate,
                lens_pair(lens_loc("/r2", "other")),
            ),
        ]
    };
    let mut mixed = live();
    mixed.push(on_suspended_root(
        lens_finding(
            "parked-member",
            FindingTier::Clean,
            999,
            0,
            Consolidate,
            lens_pair(hub_cp.clone()),
        ),
        "/rs",
    ));
    mixed.push(evidence_on_suspended_root("sunk", 999, "/rs2"));
    mixed.push(on_suspended_root(
        lens_finding(
            "parked-solo",
            FindingTier::Clean,
            999,
            0,
            Consolidate,
            lens_pair(lens_loc("/r2", "elsewhere")),
        ),
        "/rs",
    ));

    assert_eq!(lens(mixed).entries, lens(live()).entries);
}

#[test]
fn the_tally_sums_gain_within_each_cause_and_never_across_them() {
    use FindingNature::Consolidate;
    let ranked = lens(vec![
        on_suspended_root(
            lens_finding(
                "p1",
                FindingTier::Clean,
                1_000,
                0,
                Consolidate,
                lens_pair(lens_loc("/r2", "a")),
            ),
            "/rs",
        ),
        on_suspended_root(
            lens_finding(
                "p2",
                FindingTier::Clean,
                2_000,
                0,
                Consolidate,
                lens_pair(lens_loc("/r2", "b")),
            ),
            "/rs",
        ),
        evidence_on_suspended_root("p3", 500, "/rs"),
    ]);
    let t = tally(&ranked, "/rs");
    assert_eq!(t.places_on_it, 2);
    assert_eq!(t.places_with_copies_on_it, 1);
    // Within a cause the gains add; across the two causes they never do —
    // the axes are content-correlated, so one merged figure would state the
    // same bytes twice.
    assert_eq!(t.gain_bytes_on_it, 3_000);
    assert_eq!(t.gain_bytes_with_copies_on_it, 500);
    assert_eq!(ranked.suspended.len(), 1);
}

#[test]
fn suspended_tallies_are_ordered_by_root_path() {
    let ranked = lens(vec![
        on_suspended_root(
            lens_finding(
                "z",
                FindingTier::Clean,
                10,
                0,
                FindingNature::Consolidate,
                lens_pair(lens_loc("/r2", "a")),
            ),
            "/rs-z",
        ),
        on_suspended_root(
            lens_finding(
                "a",
                FindingTier::Clean,
                10,
                0,
                FindingNature::Consolidate,
                lens_pair(lens_loc("/r2", "b")),
            ),
            "/rs-a",
        ),
    ]);
    let paths: Vec<&str> = ranked
        .suspended
        .iter()
        .map(|t| t.root_path.as_str())
        .collect();
    assert_eq!(paths, ["/rs-a", "/rs-z"]);
}

// ---------------------------------------------------------------------------
// A reduction board ranks decisions that shrink the universe first.
// ---------------------------------------------------------------------------

fn on_archive_root(mut f: StructuralFinding) -> StructuralFinding {
    f.subject_is_archive = true;
    f
}

#[test]
fn an_archive_subject_ranks_below_an_equivalent_source_subject() {
    use FindingNature::Consolidate;
    // The archive subject is heavier and sorts first alphabetically, so only
    // the archive term can produce this order.
    let ranked = lens(vec![
        on_archive_root(lens_finding(
            "a-archived",
            FindingTier::Clean,
            9_000,
            0,
            Consolidate,
            lens_pair(lens_loc("/r2", "a")),
        )),
        lens_finding(
            "z-source",
            FindingTier::Clean,
            1_000,
            0,
            Consolidate,
            lens_pair(lens_loc("/r2", "b")),
        ),
    ]);
    assert_eq!(entry_labels(&ranked), ["z-source", "a-archived"]);
    // Demoted, never removed.
    assert_eq!(ranked.entries.len(), 2);
}

#[test]
fn tier_still_outranks_archive_standing() {
    use FindingNature::Consolidate;
    let ranked = lens(vec![
        lens_finding(
            "a-candidate-source",
            FindingTier::Candidate,
            1_000,
            0,
            Consolidate,
            lens_pair(lens_loc("/r2", "a")),
        ),
        on_archive_root(lens_finding(
            "z-clean-archived",
            FindingTier::Clean,
            1_000,
            0,
            Consolidate,
            lens_pair(lens_loc("/r2", "b")),
        )),
    ]);
    assert_eq!(
        entry_labels(&ranked),
        ["z-clean-archived", "a-candidate-source"]
    );
}

#[test]
fn a_hub_of_source_subjects_under_an_archive_counterpart_still_tops_the_board() {
    use FindingNature::Consolidate;
    // The rule is about the subject side only: the hub's counterpart is an
    // archive place, but its members are live source places, so the hub
    // competes as source and outranks a heavier archive-standing single.
    let hub_cp = lens_loc("/archive", "media");
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
        on_archive_root(lens_finding(
            "heavy-archived",
            FindingTier::Clean,
            9_000,
            0,
            Consolidate,
            lens_pair(lens_loc("/r2", "x")),
        )),
    ]);
    assert_eq!(entry_labels(&ranked), ["hub:media", "heavy-archived"]);
}
