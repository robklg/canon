//! Reduction lens tests — the v1 ranking derivation over the structural
//! computation.

use std::collections::HashSet;

use crate::core::domain::path::path_is_under;
use crate::core::domain::source::Source;
use crate::sweep::domain::lens::{
    counterpart_standing, entry_order, lens_params_invariant_holds, reduction_lens, HubEntry,
    LeaderboardEntry, LensParams, ParentEntry, PlaceCensus, RankedSweep, RootEntry, RootNearness,
    SuspendedRootTally,
};
use crate::sweep::domain::structural::{
    FindingNature, FindingTier, Location, RelationClass, RelationShape, StructuralFinding,
    StructuralSweep, SweepParams, SweepStats,
};

use super::fixtures::{
    lens_finding, lens_loc, low_floors, make_archive_root, make_root, make_source, nearness,
    run_structural, scale_fixture,
};

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

fn lens_with(
    findings: Vec<StructuralFinding>,
    nearness: &RootNearness,
    params: &LensParams,
) -> RankedSweep {
    lens_with_census(findings, nearness, &PlaceCensus::default(), params)
}

/// The lens with a census projected from real rows — what the sibling-parent
/// axis needs, since an entry that cannot state its coverage does not form.
/// `lens_with` passes an empty one deliberately: a fixture that builds
/// findings by hand and no sources has no population to measure, and the axis
/// correctly declines rather than inventing a figure.
fn lens_with_census(
    findings: Vec<StructuralFinding>,
    nearness: &RootNearness,
    census: &PlaceCensus<'_>,
    params: &LensParams,
) -> RankedSweep {
    reduction_lens(
        StructuralSweep {
            findings,
            stats: SweepStats {
                ubiquitous_objects_dropped: 0,
                ubiquitous_bytes_dropped: 0,
                below_floor_subjects: 0,
            },
        },
        nearness,
        census,
        params,
    )
}

/// The lens with nothing projected: every root buckets at the far end of the
/// scale, so the nearness term ties everywhere and the order is the one the
/// board had before nearness existed.
fn lens(findings: Vec<StructuralFinding>) -> RankedSweep {
    lens_with(findings, &RootNearness::default(), &LensParams::default())
}

/// Move a finding onto a named root. Nearness is keyed by root **id** and the
/// board's last tie-break reads the root **path**, so a fixture that moves one
/// without the other tests neither.
fn on_root(mut f: StructuralFinding, root_id: i64, root_path: &str) -> StructuralFinding {
    f.subject.root_id = root_id;
    f.subject.root_path = root_path.to_string();
    f
}

fn entry_labels(ranked: &RankedSweep) -> Vec<String> {
    ranked
        .entries
        .iter()
        .map(|e| match e {
            LeaderboardEntry::Single(f) => f.subject.rel_prefix.clone(),
            LeaderboardEntry::Root(r) => format!("root:{}", r.root.root_path),
            LeaderboardEntry::Parent(p) => {
                format!("parent:{}:{}", p.parent.root_path, p.parent.rel_prefix)
            }
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

// Root nearness — closing a root outranks reclaiming bytes.

#[test]
fn a_small_place_on_a_nearly_done_root_outranks_a_large_one_on_a_fresh_root() {
    // The whole point of the term. Under a size-led key the small place is
    // invisible exactly when it matters most: what is left on a root near the
    // end of its story is small *by definition*.
    use FindingNature::Consolidate;
    let ranked = lens_with(
        vec![
            on_root(
                lens_finding(
                    "heavy",
                    FindingTier::Clean,
                    50_000_000_000,
                    0,
                    Consolidate,
                    lens_pair(lens_loc("/rx", "a")),
                ),
                7,
                "/r7",
            ),
            on_root(
                lens_finding(
                    "light",
                    FindingTier::Clean,
                    1_000,
                    0,
                    Consolidate,
                    lens_pair(lens_loc("/rx", "b")),
                ),
                8,
                "/r8",
            ),
        ],
        // Root 8 has three sources left; root 7 has five hundred.
        &nearness(&[(7, 500), (8, 3)]),
        &LensParams::default(),
    );
    assert_eq!(entry_labels(&ranked), ["light", "heavy"]);
}

#[test]
fn archive_subjects_tie_on_nearness_and_fall_through_to_gain() {
    // Nearness is meaningless for an archive root — one is never retired — so
    // archive subjects carry no projection, tie on the term, and are ordered
    // by gain among themselves. Archive standing sorting *ahead* of nearness
    // is what makes that tie reachable.
    use FindingNature::Consolidate;
    let archive = |mut f: StructuralFinding| {
        f.subject_is_archive = true;
        f
    };
    let ranked = lens_with(
        vec![
            archive(on_root(
                lens_finding(
                    "arch-light",
                    FindingTier::Clean,
                    1_000,
                    0,
                    Consolidate,
                    lens_pair(lens_loc("/rx", "a")),
                ),
                20,
                "/a20",
            )),
            archive(on_root(
                lens_finding(
                    "arch-heavy",
                    FindingTier::Clean,
                    9_000,
                    0,
                    Consolidate,
                    lens_pair(lens_loc("/rx", "b")),
                ),
                21,
                "/a21",
            )),
        ],
        // Neither archive root is in the projection at all; the two source
        // roots present here are irrelevant to both subjects.
        &nearness(&[(7, 0), (8, 900)]),
        &LensParams::default(),
    );
    assert_eq!(entry_labels(&ranked), ["arch-heavy", "arch-light"]);
}

#[test]
fn a_remainder_changing_by_one_does_not_reshuffle_the_board() {
    // Stability is part of the requirement: a leaderboard that reshuffles when
    // nothing the user did changed reads as broken. The buckets are
    // order-of-magnitude, so a remainder must cross a decade to move anything.
    use FindingNature::Consolidate;
    let board = |near| {
        entry_labels(&lens_with(
            vec![
                on_root(
                    lens_finding(
                        "heavy",
                        FindingTier::Clean,
                        50_000,
                        0,
                        Consolidate,
                        lens_pair(lens_loc("/rx", "a")),
                    ),
                    7,
                    "/r7",
                ),
                on_root(
                    lens_finding(
                        "light",
                        FindingTier::Clean,
                        1_000,
                        0,
                        Consolidate,
                        lens_pair(lens_loc("/rx", "b")),
                    ),
                    8,
                    "/r8",
                ),
            ],
            &near,
            &LensParams::default(),
        ))
    };
    let settled = board(nearness(&[(7, 40), (8, 4)]));
    assert_eq!(settled, ["light", "heavy"]);
    // One more source left on each root, inside the same decade: unmoved.
    assert_eq!(board(nearness(&[(7, 41), (8, 5)])), settled);
    // Crossing the decade is what moves it — both roots now read as tens.
    assert_eq!(board(nearness(&[(7, 40), (8, 10)])), ["heavy", "light"]);
}

#[test]
fn bucket_zero_agrees_with_no_blockers_found() {
    // Bucket 0 is the retirement review's `NoBlockersFound`, not an arbitrary
    // cut — the join is what licenses reading "close to done" off the bucket
    // at all. It is pinned in two halves because `Readiness` lives behind
    // `retire`'s barrel and a sweep test may not name it: this half fixes
    // bucket 0 to a zero remainder, and
    // `a_zero_remainder_is_exactly_no_blockers_found` in
    // `retire/domain/readiness.rs` fixes a zero remainder to the verdict.
    let near = nearness(&[(1, 0), (2, 1), (3, 9), (4, 10), (5, 99), (6, 100)]);
    assert_eq!(near.bucket(1), 0);
    assert_eq!(near.remaining(1), Some(0));
    for root_id in [2, 3] {
        assert_eq!(near.bucket(root_id), 1, "one to nine is the next bucket up");
    }
    assert_eq!(near.bucket(4), 2);
    assert_eq!(near.bucket(5), 2);
    assert_eq!(near.bucket(6), 3);
    // A root with no projection sorts last, never first.
    assert!(near.bucket(99) > near.bucket(6));
    assert_eq!(near.remaining(99), None);
}

#[test]
fn identical_state_yields_identical_ordering_with_nearness() {
    // Determinism survives the new term: two runs over one unchanged state
    // give one order, including the roots whose remainder the board states.
    let (sources, roots) = scale_fixture();
    let near = nearness(&[(1, 3), (2, 40), (3, 0), (4, 700)]);
    let run = || {
        lens_with(
            run_structural(&sources, &roots, &low_floors()).findings,
            &near,
            &LensParams::default(),
        )
    };
    let a = run();
    let b = run();
    assert_eq!(entry_labels(&a), entry_labels(&b));
    assert_eq!(a.stated_remainders, b.stated_remainders);
    assert!(
        !a.stated_remainders.is_empty(),
        "the fixture must state something, or determinism here is vacuous"
    );
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
    let ranked = reduction_lens(
        sweep,
        &RootNearness::default(),
        &PlaceCensus::default(),
        &LensParams::default(),
    );
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

// The regime — nearness separates only where the board can say so.

#[test]
fn nearness_ties_above_the_regime_and_gain_leads() {
    // The reproduced case that drove the rule: 400 sources left against 4,000
    // is not a difference this board ranks on. Neither root is anywhere near
    // done, so letting the term separate them demoted a finding fifty times
    // heavier for a reason no line could ever explain — the line only speaks
    // inside the regime. Above it every root ties and weight leads again.
    use FindingNature::Consolidate;
    let ranked = lens_with(
        vec![
            on_root(
                lens_finding(
                    "heavy-50gb",
                    FindingTier::Clean,
                    50_000_000_000,
                    0,
                    Consolidate,
                    lens_pair(lens_loc("/rx", "a")),
                ),
                8,
                "/r8",
            ),
            on_root(
                lens_finding(
                    "light-1gb",
                    FindingTier::Clean,
                    1_000_000_000,
                    0,
                    Consolidate,
                    lens_pair(lens_loc("/rx", "b")),
                ),
                7,
                "/r7",
            ),
        ],
        &nearness(&[(7, 400), (8, 4_000)]),
        &LensParams::default(),
    );
    assert_eq!(entry_labels(&ranked), ["heavy-50gb", "light-1gb"]);
    assert!(
        ranked.stated_remainders.is_empty(),
        "nothing separated, so nothing is stated"
    );
}

#[test]
fn a_hub_lifted_by_an_in_regime_member_names_that_members_root() {
    // "Option A covers hubs automatically" holds only at root-entry grade: a
    // member inside the regime but above `root_entry_bucket` is not claimed
    // away, so it lifts the hub — and the hub must say which root did it.
    let shared = lens_loc("/rx", "shared");
    let member = |rel: &str, root_id: i64, root_path: &str| {
        on_root(
            lens_finding(
                rel,
                FindingTier::Clean,
                10_000,
                0,
                FindingNature::Consolidate,
                lens_pair(shared.clone()),
            ),
            root_id,
            root_path,
        )
    };
    // Root 7 is at bucket 2 — inside the regime, above the root-entry bucket,
    // so it stays a hub member. The others are far from done.
    let ranked = lens_with(
        vec![
            member("a", 7, "/r7"),
            member("b", 8, "/r8"),
            member("c", 9, "/r9"),
        ],
        &nearness(&[(7, 40), (8, 4_000), (9, 4_000)]),
        &LensParams::default(),
    );
    let hub = match &ranked.entries[0] {
        LeaderboardEntry::Hub(h) => h,
        other => panic!("expected a hub, got {other:?}"),
    };
    let named = hub
        .nearness_root
        .as_ref()
        .expect("the hub names the root that lifted it");
    assert_eq!(named.root_id, 7);
    assert_eq!(named.root_path, "/r7");
    assert_eq!(named.rel_prefix, "", "a root as a place");
    assert_eq!(ranked.stated_remainders.get(&7), Some(&40));
    // Only the root that set the term is stated — the other members are
    // outside the regime and contributed nothing to explain.
    assert_eq!(ranked.stated_remainders.len(), 1);
}

#[test]
fn a_hub_outside_the_regime_names_no_root() {
    // The other direction of the same rule: nearness tied for every member, so
    // it moved the hub nowhere and the hub states nothing.
    let shared = lens_loc("/rx", "shared");
    let member = |rel: &str, root_id: i64, root_path: &str| {
        on_root(
            lens_finding(
                rel,
                FindingTier::Clean,
                10_000,
                0,
                FindingNature::Consolidate,
                lens_pair(shared.clone()),
            ),
            root_id,
            root_path,
        )
    };
    let ranked = lens_with(
        vec![member("a", 8, "/r8"), member("b", 9, "/r9")],
        &nearness(&[(8, 4_000), (9, 900)]),
        &LensParams::default(),
    );
    let hub = match &ranked.entries[0] {
        LeaderboardEntry::Hub(h) => h,
        other => panic!("expected a hub, got {other:?}"),
    };
    assert!(hub.nearness_root.is_none());
    assert!(ranked.stated_remainders.is_empty());
}

#[test]
fn a_root_entry_can_never_form_outside_the_regime_that_states_it() {
    // The invariant `root_entry_bucket <= nearness_render_bucket`, and why it
    // is load-bearing rather than tidy. A root entry states its remainder
    // *unconditionally* — that is the entry kind's own criterion — so one
    // qualifying from outside the regime would state a term that did not
    // order it: its key would tie like every other out-of-regime root, and
    // the line would appear with nothing behind it.
    let params = LensParams::default();
    assert!(lens_params_invariant_holds(&params));

    // The property the invariant buys, over every bucket a root can hold:
    // whatever qualifies for a root entry is inside the regime, so the
    // unconditional statement on that entry always has an ordering term
    // behind it.
    let near = nearness(&[(1, 0), (2, 5), (3, 40), (4, 400), (5, 4_000)]);
    for root_id in 1..=5 {
        if near.bucket(root_id) <= params.root_entry_bucket {
            assert_eq!(
                near.ranking_bucket(root_id, &params),
                near.bucket(root_id),
                "a root that can form an entry must not be tied out of the regime"
            );
        }
    }
    // And the predicate refuses the arrangement that would break it.
    assert!(!lens_params_invariant_holds(&LensParams {
        root_entry_bucket: 3,
        nearness_render_bucket: 2,
        ..LensParams::default()
    }));
}

// Axis 2 — one root, one slot.

/// A place on the given root pointing at its own counterpart, so nothing here
/// forms a hub by accident.
fn place_on(rel: &str, root_id: i64, root_path: &str, gain_bytes: u64) -> StructuralFinding {
    on_root(
        lens_finding(
            rel,
            FindingTier::Clean,
            gain_bytes,
            0,
            FindingNature::Consolidate,
            lens_pair(lens_loc("/rx", rel)),
        ),
        root_id,
        root_path,
    )
}

fn root_entry<'a>(ranked: &'a RankedSweep, root_path: &str) -> &'a RootEntry {
    ranked
        .entries
        .iter()
        .find_map(|e| match e {
            LeaderboardEntry::Root(r) if r.root.root_path == root_path => Some(r),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no root entry for {root_path}"))
}

#[test]
fn a_near_retirable_root_takes_one_slot_carrying_its_places() {
    // Nearness alone would make the board worse: every place on a nearly-done
    // root inherits the boost, so three places left becomes three top slots.
    // The root takes one slot and carries them.
    let ranked = lens_with(
        vec![
            place_on("pictures", 7, "/r7", 30_000),
            place_on("music", 7, "/r7", 20_000),
            place_on("docs", 7, "/r7", 10_000),
        ],
        &nearness(&[(7, 3)]),
        &LensParams::default(),
    );
    assert_eq!(entry_labels(&ranked), ["root:/r7"]);
    let entry = root_entry(&ranked, "/r7");
    assert_eq!(entry.root.rel_prefix, "", "the headline is the root's top");
    assert_eq!(entry.unresolved_remaining, 3);
    assert_eq!(entry.members.len(), 3);
    // Members are ranked within by the board's own key, heaviest first here.
    assert_eq!(
        entry
            .members
            .iter()
            .map(|m| m.subject.rel_prefix.as_str())
            .collect::<Vec<_>>(),
        ["pictures", "music", "docs"]
    );
    assert_eq!(entry.gain_files_upper, 30);
    // And the board states the root's remainder beside it.
    assert_eq!(ranked.stated_remainders.get(&7), Some(&3));
}

#[test]
fn a_root_far_from_done_forms_no_entry_and_its_places_compete_individually() {
    // The axis is about roots near the end of their story. A root barely
    // started has nothing to celebrate and its places compete on their own
    // merits, exactly as before.
    let ranked = lens_with(
        vec![
            place_on("pictures", 7, "/r7", 30_000),
            place_on("music", 7, "/r7", 20_000),
            place_on("docs", 7, "/r7", 10_000),
        ],
        &nearness(&[(7, 500)]),
        &LensParams::default(),
    );
    assert_eq!(entry_labels(&ranked), ["pictures", "music", "docs"]);
    assert!(ranked.stated_remainders.is_empty());
}

#[test]
fn a_qualifying_root_with_one_place_forms_no_entry() {
    // One place is already one slot: a root entry there is furniture, not
    // information — and the place carries the remainder fact and the
    // retirement handoff itself.
    let ranked = lens_with(
        vec![place_on("pictures", 7, "/r7", 30_000)],
        &nearness(&[(7, 2)]),
        &LensParams::default(),
    );
    assert_eq!(entry_labels(&ranked), ["pictures"]);
    // The fact still reaches the board — it is why this place ranks where it
    // does, and the entry that would have carried it does not exist.
    assert_eq!(ranked.stated_remainders.get(&7), Some(&2));
}

#[test]
fn a_root_entry_claims_places_away_from_a_hub_which_degrades() {
    // A slot is claimed by exactly one axis, and the root claims ahead of the
    // hub: finishing a root resolves more than any one place on it. The hub
    // that loses two of its three members falls below two and degrades to
    // singles, which is its own existing rule and not a new one.
    let shared = lens_loc("/rx", "shared");
    let member = |rel: &str, root_id: i64, root_path: &str| {
        on_root(
            lens_finding(
                rel,
                FindingTier::Clean,
                10_000,
                0,
                FindingNature::Consolidate,
                lens_pair(shared.clone()),
            ),
            root_id,
            root_path,
        )
    };
    // Without the root axis all three form one hub.
    let hub_only = lens_with(
        vec![
            member("a", 7, "/r7"),
            member("b", 7, "/r7"),
            member("c", 8, "/r8"),
        ],
        &nearness(&[(7, 500), (8, 500)]),
        &LensParams::default(),
    );
    assert_eq!(entry_labels(&hub_only), ["hub:shared"]);

    // With root 7 near done, its two places leave for the root entry and the
    // hub's one survivor degrades.
    let ranked = lens_with(
        vec![
            member("a", 7, "/r7"),
            member("b", 7, "/r7"),
            member("c", 8, "/r8"),
        ],
        &nearness(&[(7, 3), (8, 500)]),
        &LensParams::default(),
    );
    let mut labels = entry_labels(&ranked);
    labels.sort();
    assert_eq!(labels, ["c", "root:/r7"]);
    assert_eq!(root_entry(&ranked, "/r7").members.len(), 2);
}

#[test]
fn an_archive_root_never_forms_a_root_entry() {
    // An archive root is never retired, so nearness says nothing about one.
    // That is spoken once, in the projection: `RootNearness` holds source
    // roots only, so an archive subject buckets past every threshold and can
    // never qualify. No second test guards it here, deliberately — a second
    // spelling of one rule is what lets the two disagree later.
    let archive = make_archive_root(9, "/a9");
    let rows = [
        make_source(1, 9, "x/f", 100, None),
        make_source(2, 9, "y/f", 100, None),
    ];
    let near = RootNearness::project(&[archive], &rows, &HashSet::new());
    assert_eq!(near.remaining(9), None, "archive roots carry no nearness");

    let on_archive = |rel: &str| {
        let mut f = place_on(rel, 9, "/a9", 10_000);
        f.subject_is_archive = true;
        f
    };
    let ranked = lens_with(
        vec![on_archive("x"), on_archive("y")],
        &near,
        &LensParams::default(),
    );
    let mut labels = entry_labels(&ranked);
    labels.sort();
    assert_eq!(labels, ["x", "y"]);
    assert!(ranked.stated_remainders.is_empty());
}

#[test]
fn a_root_entrys_gain_does_not_double_count_content_shared_between_its_members() {
    // The summands are **not** byte-disjoint, and this is the corpus where the
    // exposure is live. `/r1/A/x` and `/r1/A/y` each hold a 10 MB object that
    // exists nowhere else, plus 30 MB of their own that is copied on `/r2`.
    // LCA subtraction removes the intra-root duplication from `A` **upward**,
    // so both siblings — sitting below `A` — legitimately count the shared
    // 10 MB as "exists outside me". Both numbers are true.
    //
    // Their sum is not a statement about content: letting both places go would
    // destroy the shared object outright, so what is actually recoverable
    // while keeping one copy of everything is 70 MB, not 80. The entry's
    // figure is therefore an upper bound, is named `gain_bytes_upper`, and is
    // rendered `up to` — pinned on the surface by
    // `a_root_entry_states_a_bound_and_never_calls_it_gain`.
    let roots = vec![make_root(1, "/r1"), make_root(2, "/r2")];
    let mut sources = Vec::new();
    let mut id = 0i64;
    let mut next = |root_id: i64, rel: String, size: i64, oid: Option<i64>| {
        id += 1;
        make_source(id, root_id, &rel, size, oid)
    };
    for i in 0..10 {
        // The shared object, in both siblings and nowhere else.
        sources.push(next(1, format!("A/x/o{i}"), 1_000_000, Some(100 + i)));
        sources.push(next(1, format!("A/y/o{i}"), 1_000_000, Some(100 + i)));
        // Each sibling's own content, copied on the other root.
        sources.push(next(1, format!("A/x/p{i}"), 3_000_000, Some(200 + i)));
        sources.push(next(2, format!("q/p{i}"), 3_000_000, Some(200 + i)));
        sources.push(next(1, format!("A/y/q{i}"), 3_000_000, Some(300 + i)));
        sources.push(next(2, format!("q/q{i}"), 3_000_000, Some(300 + i)));
        // Noise keeping `/r2` from lifting whole.
        sources.push(next(2, format!("noise/n{i}"), 5_000_000, Some(400 + i)));
    }
    let findings = run_structural(&sources, &roots, &low_floors()).findings;
    let ranked = lens_with(
        findings,
        &nearness(&[(1, 3), (2, 900)]),
        &LensParams::default(),
    );
    let entry = root_entry(&ranked, "/r1");
    assert_eq!(entry.members.len(), 2);
    for member in &entry.members {
        assert_eq!(
            member.gain_bytes, 40_000_000,
            "each sibling truthfully counts the shared object as existing outside it"
        );
    }
    assert_eq!(entry.gain_bytes_upper, 80_000_000);
    // 30 MB + 30 MB copied on `/r2`, plus one copy of the shared 10 MB: the
    // bound overstates by exactly the object counted twice.
    let actually_recoverable = 70_000_000u64;
    assert!(
        entry.gain_bytes_upper > actually_recoverable,
        "if this ever holds with equality the corpus stopped exercising the exposure"
    );
}

// Axis 3 — one situation, one slot.

/// Rows standing at the given paths on one root. The sibling-parent axis asks
/// the census exactly one question — how many sources stand under a place — so
/// the rows need nothing but their paths, and projecting a census the way
/// production does keeps a fixture from asserting a coverage the projection
/// could not produce.
fn rows_on(root_id: i64, paths: &[&str]) -> Vec<Source> {
    paths
        .iter()
        .enumerate()
        .map(|(i, p)| make_source(root_id * 1_000 + i as i64, root_id, p, 100, None))
        .collect()
}

fn parent_entry<'a>(ranked: &'a RankedSweep, root_path: &str, rel: &str) -> &'a ParentEntry {
    ranked
        .entries
        .iter()
        .find_map(|e| match e {
            LeaderboardEntry::Parent(p)
                if p.parent.root_path == root_path && p.parent.rel_prefix == rel =>
            {
                Some(p)
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("no parent entry for {root_path} :: {rel}"))
}

/// Every finding the board carries, members included — the set the
/// disjointness invariant is a claim about.
fn board_findings(ranked: &RankedSweep) -> Vec<&StructuralFinding> {
    ranked
        .entries
        .iter()
        .flat_map(|e| match e {
            LeaderboardEntry::Single(f) => vec![f],
            LeaderboardEntry::Root(r) => r.members.iter().collect(),
            LeaderboardEntry::Parent(p) => p.members.iter().collect(),
            LeaderboardEntry::Hub(h) => h.members.iter().collect(),
        })
        .collect()
}

/// Two places sharing one counterpart, so the hub axis would take them if the
/// sibling run did not claim first.
fn sibling(root_id: i64, root_path: &str, rel: &str, shared: &Location) -> StructuralFinding {
    on_root(
        lens_finding(
            rel,
            FindingTier::Clean,
            10_000,
            0,
            FindingNature::Consolidate,
            lens_pair(shared.clone()),
        ),
        root_id,
        root_path,
    )
}

#[test]
fn a_sibling_run_claims_ahead_of_a_hub() {
    // Decision-grouping precedes evidence-grouping. These two places share one
    // counterpart, so the hub axis would take them — but they are siblings
    // under one parent, and one act at that parent covers both. This is a
    // decision board and the headline is where the decision is, so the run
    // claims first and the hub is the residual grouping for places with no
    // common decision unit.
    let shared = lens_loc("/rx", "store");
    let findings = || {
        vec![
            sibling(1, "/r1", "photos/2020", &shared),
            sibling(1, "/r1", "photos/2021", &shared),
        ]
    };
    let sources = rows_on(
        1,
        &[
            "photos/2020/a",
            "photos/2020/b",
            "photos/2021/a",
            "photos/2021/b",
        ],
    );
    let census = PlaceCensus::project(sources.iter());

    let ranked = lens_with_census(
        findings(),
        &RootNearness::default(),
        &census,
        &LensParams::default(),
    );
    assert_eq!(entry_labels(&ranked), ["parent:/r1:photos"]);
    let entry = parent_entry(&ranked, "/r1", "photos");
    assert_eq!(entry.members.len(), 2);
    assert_eq!(entry.coverage, 1.0, "the run accounts for the whole parent");

    // The same two findings with nothing to measure them against: the axis
    // declines rather than inventing a figure, and the hub takes them — which
    // is exactly what the run claimed ahead of.
    let hubbed = lens_with(findings(), &RootNearness::default(), &LensParams::default());
    assert_eq!(entry_labels(&hubbed), ["hub:store"]);
}

#[test]
fn sibling_runs_on_two_roots_form_two_entries() {
    // The grouping key is the root **and** the path, so a run cannot span two
    // roots. That split is *correct* rather than a limitation: places under one
    // root's folder and places under another's are two decisions, not one,
    // however much they hold in common — and it is the price of claiming ahead
    // of the hub, paid knowingly.
    let shared = lens_loc("/rx", "store");
    let mut sources = rows_on(1, &["photos/2020/a", "photos/2021/a"]);
    sources.extend(rows_on(2, &["photos/2020/a", "photos/2021/a"]));
    let census = PlaceCensus::project(sources.iter());
    let ranked = lens_with_census(
        vec![
            sibling(1, "/r1", "photos/2020", &shared),
            sibling(1, "/r1", "photos/2021", &shared),
            sibling(2, "/r2", "photos/2020", &shared),
            sibling(2, "/r2", "photos/2021", &shared),
        ],
        &RootNearness::default(),
        &census,
        &LensParams::default(),
    );
    let mut labels = entry_labels(&ranked);
    labels.sort();
    assert_eq!(labels, ["parent:/r1:photos", "parent:/r2:photos"]);
    // One counterpart, four places, and still two entries: the hub would have
    // merged them into one slot for two separate decisions.
    assert_eq!(parent_entry(&ranked, "/r1", "photos").members.len(), 2);
    assert_eq!(parent_entry(&ranked, "/r2", "photos").members.len(), 2);
}

#[test]
fn a_parent_entry_creates_no_finding_for_the_parent() {
    // The parent is an **entry**, never a **finding**. Nothing is emitted for
    // it, no containment is computed for it, and the findings the board carries
    // are exactly the ones the lens was handed. This is the property that
    // leaves the disjointness invariant untouched by construction rather than
    // by care: there is no parent finding for a child finding to sit under.
    let shared = lens_loc("/rx", "store");
    let sources = rows_on(1, &["photos/2020/a", "photos/2021/a"]);
    let census = PlaceCensus::project(sources.iter());
    let ranked = lens_with_census(
        vec![
            sibling(1, "/r1", "photos/2020", &shared),
            sibling(1, "/r1", "photos/2021", &shared),
        ],
        &RootNearness::default(),
        &census,
        &LensParams::default(),
    );
    let entry = parent_entry(&ranked, "/r1", "photos");
    assert_eq!(entry.parent.rel_prefix, "photos");
    assert_eq!(
        board_findings(&ranked)
            .iter()
            .map(|f| f.subject.rel_prefix.as_str())
            .collect::<Vec<_>>(),
        ["photos/2020", "photos/2021"],
    );
    assert!(
        board_findings(&ranked)
            .iter()
            .all(|f| f.subject.rel_prefix != "photos"),
        "the headline place is on no finding anywhere"
    );
}

#[test]
fn the_disjointness_invariant_survives_parent_grouping() {
    // Emission at the maximal subject is the correctness heart, and grouping
    // must not reach around it. Driven through the real engine so the pairs
    // checked here are the ones the descent actually produced: `photos` carries
    // enough unmatched content of its own to fail the lifting tolerance, so the
    // descent continues past it and both month folders emit.
    let roots = vec![make_root(1, "/r1"), make_root(2, "/r2")];
    let mut sources = Vec::new();
    let mut id = 0i64;
    let mut next = |root_id: i64, rel: String, oid: Option<i64>| {
        id += 1;
        make_source(id, root_id, &rel, 1_000_000, oid)
    };
    for i in 0..30i64 {
        sources.push(next(1, format!("photos/2020/f{i}"), Some(100 + i)));
        sources.push(next(2, format!("one/f{i}"), Some(100 + i)));
        sources.push(next(1, format!("photos/2021/f{i}"), Some(200 + i)));
        sources.push(next(2, format!("two/f{i}"), Some(200 + i)));
    }
    // Unmatched content directly under `photos`, keeping it below the lifting
    // tolerance so the descent reaches the months.
    for i in 0..10i64 {
        sources.push(next(1, format!("photos/misc/u{i}"), Some(300 + i)));
    }
    let findings = run_structural(&sources, &roots, &SweepParams::default()).findings;
    let census = PlaceCensus::project(sources.iter());
    let ranked = lens_with_census(
        findings,
        &RootNearness::default(),
        &census,
        &LensParams::default(),
    );

    let entry = parent_entry(&ranked, "/r1", "photos");
    assert_eq!(
        entry
            .members
            .iter()
            .map(|m| m.subject.rel_prefix.as_str())
            .collect::<Vec<_>>(),
        ["photos/2020", "photos/2021"],
    );
    // 60 of the parent's 70 sources lie under the run; the 10 under `misc` are
    // what the entry does not account for, and the figure says so.
    assert!((entry.coverage - 60.0 / 70.0).abs() < 1e-9);

    // No finding on the board sits under another. The invariant is a claim
    // about findings, and grouping added none.
    let places: Vec<(i64, &str)> = board_findings(&ranked)
        .iter()
        .map(|f| (f.subject.root_id, f.subject.rel_prefix.as_str()))
        .collect();
    for (i, a) in places.iter().enumerate() {
        for (j, b) in places.iter().enumerate() {
            assert!(
                i == j || a.0 != b.0 || !path_is_under(b.1, a.1),
                "{b:?} sits under {a:?}"
            );
        }
    }
}

// Axis 3 — the two constants.

#[test]
fn two_siblings_are_enough_to_group() {
    // Grouping at two is rare rather than trigger-happy: on the board this was
    // calibrated against, only four runs existed across seventy-two subject
    // places. One place is already one slot and never groups; two is a run.
    let shared = lens_loc("/rx", "store");
    let sources = rows_on(1, &["photos/2020/a", "photos/2021/a"]);
    let census = PlaceCensus::project(sources.iter());
    let run = |rels: &[&str]| {
        lens_with_census(
            rels.iter()
                .map(|rel| sibling(1, "/r1", rel, &shared))
                .collect(),
            &RootNearness::default(),
            &census,
            &LensParams::default(),
        )
    };
    assert_eq!(
        entry_labels(&run(&["photos/2020", "photos/2021"])),
        ["parent:/r1:photos"]
    );
    // One place under the parent: no run, and the place keeps its own slot.
    assert_eq!(entry_labels(&run(&["photos/2020"])), ["photos/2020"]);
}

#[test]
fn a_parent_below_the_coverage_gate_does_not_group_and_its_places_compete_individually() {
    // At low coverage the parent is not where the decision is: dismissing it
    // would reach far beyond the situation, so it must not be the headline.
    // **Nothing is hidden below the gate** — the places fall through and
    // compete exactly as they did before this axis existed.
    let shared = lens_loc("/rx", "store");
    let mut paths = vec!["backups/phone/a", "backups/tablet/a"];
    // Eight more sources under the parent that no member accounts for: the run
    // covers two of ten.
    let rest: Vec<String> = (0..8).map(|i| format!("backups/other/f{i}")).collect();
    paths.extend(rest.iter().map(String::as_str));
    let sources = rows_on(1, &paths);
    let census = PlaceCensus::project(sources.iter());
    let findings = vec![
        sibling(1, "/r1", "backups/phone", &shared),
        sibling(1, "/r1", "backups/tablet", &shared),
    ];
    let ranked = lens_with_census(
        findings,
        &RootNearness::default(),
        &census,
        &LensParams::default(),
    );
    // Not a parent entry — and the two places are still both on the board,
    // grouped by the evidence they share, which is what they were before.
    assert_eq!(entry_labels(&ranked), ["hub:store"]);
    assert_eq!(
        board_findings(&ranked)
            .iter()
            .map(|f| f.subject.rel_prefix.as_str())
            .collect::<Vec<_>>(),
        ["backups/phone", "backups/tablet"],
    );
}

#[test]
fn grouping_never_recurses_past_the_immediate_parent() {
    // The nested case, and it is decisive. A folder and its own child can both
    // be run parents — from different sets of findings, since a parent is
    // never itself a finding. Grouping at each keeps two honest entries;
    // recursing would merge them into one headlined further up, and lifting
    // trades slots for honesty while coverage collapses fast. One level is
    // where the trade still pays, which is why the depth is not a constant: a
    // configurable one would invite a value the evidence says is always wrong.
    let shared = lens_loc("/rx", "store");
    let mut paths = vec![
        "phone/2020/01/a",
        "phone/2020/01/b",
        "phone/2020/02/a",
        "phone/2020/02/b",
    ];
    let notes: Vec<String> = (0..5).map(|i| format!("phone/notes/f{i}")).collect();
    let misc: Vec<String> = (0..5).map(|i| format!("phone/misc/f{i}")).collect();
    paths.extend(notes.iter().chain(misc.iter()).map(String::as_str));
    let sources = rows_on(1, &paths);
    let census = PlaceCensus::project(sources.iter());
    let ranked = lens_with_census(
        vec![
            sibling(1, "/r1", "phone/2020/01", &shared),
            sibling(1, "/r1", "phone/2020/02", &shared),
            sibling(1, "/r1", "phone/notes", &shared),
            sibling(1, "/r1", "phone/misc", &shared),
        ],
        &RootNearness::default(),
        &census,
        &LensParams::default(),
    );
    let mut labels = entry_labels(&ranked);
    labels.sort();
    assert_eq!(labels, ["parent:/r1:phone", "parent:/r1:phone/2020"]);
    // Each entry carries its own two members, and the months never lift into
    // the grandparent's entry.
    assert_eq!(
        parent_entry(&ranked, "/r1", "phone/2020")
            .members
            .iter()
            .map(|m| m.subject.rel_prefix.as_str())
            .collect::<Vec<_>>(),
        ["phone/2020/01", "phone/2020/02"],
    );
    assert_eq!(
        parent_entry(&ranked, "/r1", "phone")
            .members
            .iter()
            .map(|m| m.subject.rel_prefix.as_str())
            .collect::<Vec<_>>(),
        ["phone/misc", "phone/notes"],
    );
}

#[test]
fn the_coverage_figure_is_the_parents_own_sources_not_its_members() {
    // The denominator is the parent's own population, not the members'. A
    // figure over the members would be 100% by construction and would say
    // nothing — the question the entry answers is how much of the parent this
    // run accounts for, which is what makes it the place the decision is.
    let shared = lens_loc("/rx", "store");
    let mut paths = vec![
        "photos/2020/a",
        "photos/2020/b",
        "photos/2020/c",
        "photos/2021/a",
        "photos/2021/b",
        "photos/2021/c",
    ];
    let rest: Vec<String> = (0..4).map(|i| format!("photos/misc/f{i}")).collect();
    paths.extend(rest.iter().map(String::as_str));
    let sources = rows_on(1, &paths);
    let census = PlaceCensus::project(sources.iter());
    let ranked = lens_with_census(
        vec![
            sibling(1, "/r1", "photos/2020", &shared),
            sibling(1, "/r1", "photos/2021", &shared),
        ],
        &RootNearness::default(),
        &census,
        &LensParams::default(),
    );
    // Six of the parent's ten sources lie under the run — exactly the default
    // gate, which admits at its own value rather than above it.
    let entry = parent_entry(&ranked, "/r1", "photos");
    assert_eq!(entry.coverage, 0.6);
    assert_eq!(
        entry.coverage,
        LensParams::default().sibling_parent_coverage
    );
    assert_eq!(entry.members.len(), 2);
}

#[test]
fn a_parent_entrys_gain_does_not_double_count_content_shared_between_its_members() {
    // A sibling run is the exact shape the exposure was described from, and
    // this is the corpus where it is live. `/r1/A/x` and `/r1/A/y` each hold a
    // 10 MB object that exists nowhere else, plus 30 MB of their own copied on
    // `/r2`. LCA subtraction removes the intra-root duplication from `A`
    // **upward**, so both siblings — sitting below `A` — legitimately count the
    // shared 10 MB as "exists outside me". Both numbers are true.
    //
    // Their sum is not a statement about content: letting both go would
    // destroy the shared object outright, so what is recoverable while keeping
    // one copy of everything is 70 MB, not 80. `HubEntry.total_gain_bytes` is
    // no precedent — a hub's members point into a counterpart that is never
    // itself a member, so its summands are separated by role, and a run has no
    // such structure. Hence `gain_bytes_upper`, rendered `up to`, pinned on the
    // surface by `a_parent_entry_states_a_bound_and_never_calls_it_gain`.
    let roots = vec![make_root(1, "/r1"), make_root(2, "/r2")];
    let mut sources = Vec::new();
    let mut id = 0i64;
    let mut next = |root_id: i64, rel: String, size: i64, oid: Option<i64>| {
        id += 1;
        make_source(id, root_id, &rel, size, oid)
    };
    for i in 0..10 {
        // The shared object, in both siblings and nowhere else.
        sources.push(next(1, format!("A/x/o{i}"), 1_000_000, Some(100 + i)));
        sources.push(next(1, format!("A/y/o{i}"), 1_000_000, Some(100 + i)));
        // Each sibling's own content, copied on the other root.
        sources.push(next(1, format!("A/x/p{i}"), 3_000_000, Some(200 + i)));
        sources.push(next(2, format!("q/p{i}"), 3_000_000, Some(200 + i)));
        sources.push(next(1, format!("A/y/q{i}"), 3_000_000, Some(300 + i)));
        sources.push(next(2, format!("q/q{i}"), 3_000_000, Some(300 + i)));
        // Noise keeping `/r2` from lifting whole.
        sources.push(next(2, format!("noise/n{i}"), 5_000_000, Some(400 + i)));
    }
    let findings = run_structural(&sources, &roots, &low_floors()).findings;
    let census = PlaceCensus::project(sources.iter());
    // Both roots far from done, so the root axis claims nothing and the run
    // takes the two siblings — ahead of the hub they would otherwise form
    // around the counterpart they share.
    let ranked = lens_with_census(
        findings,
        &nearness(&[(1, 900), (2, 900)]),
        &census,
        &LensParams::default(),
    );
    let entry = parent_entry(&ranked, "/r1", "A");
    assert_eq!(entry.members.len(), 2);
    assert_eq!(entry.coverage, 1.0);
    for member in &entry.members {
        assert_eq!(
            member.gain_bytes, 40_000_000,
            "each sibling truthfully counts the shared object as existing outside it"
        );
    }
    assert_eq!(entry.gain_bytes_upper, 80_000_000);
    let actually_recoverable = 70_000_000u64;
    assert!(
        entry.gain_bytes_upper > actually_recoverable,
        "if this ever holds with equality the corpus stopped exercising the exposure"
    );
}

// Shape (b) — one overlap told from both ends.

/// A place mirroring a **child** of another place: the shape the engine's own
/// reciprocal-mirror dedup cannot see, because it matches on the cited
/// counterpart being the same place and these two sit at different depths.
fn mirrors(rel: &str, counterpart: Location, gain_bytes: u64) -> StructuralFinding {
    lens_finding(
        rel,
        FindingTier::Clean,
        gain_bytes,
        0,
        FindingNature::Consolidate,
        RelationShape::Pair {
            counterpart,
            class: RelationClass::Mirror,
            pair_size_pct: 1.0,
            pair_count_pct: 1.0,
            counterpart_share_pct: 1.0,
            counterpart_suspended: false,
            counterpart_is_archive: false,
            counterpart_last_scanned_at: None,
        },
    )
}

/// The observed live pair: each place mirrors a child of the other, so one
/// overlap takes two slots.
fn reciprocal_pair() -> Vec<StructuralFinding> {
    vec![
        mirrors(
            "downloads/tools",
            lens_loc("/r1", "tools/vendor/app"),
            30_000,
        ),
        mirrors(
            "tools",
            lens_loc("/r1", "downloads/tools/vendor/app"),
            20_000,
        ),
    ]
}

#[test]
fn a_reciprocal_pair_at_different_depths_collapses_to_one_entry() {
    // The requester's original `#1`/`#2` shape, recurring on a live root. The
    // engine's dedup misses it because the two subjects sit at different
    // paths; each mirrors a child of the other, so it is one overlap stated
    // from both ends and owes the board one slot.
    let ranked = lens(reciprocal_pair());
    assert_eq!(entry_labels(&ranked), ["downloads/tools"]);
}

#[test]
fn the_survivor_states_the_reciprocity() {
    // Collapsing the duplicate slot must not suppress the fact: the other
    // place really does mirror this one, and one decision resolves both.
    let ranked = lens(reciprocal_pair());
    assert_eq!(
        ranked
            .reciprocal_places
            .get(&lens_loc("/r1", "downloads/tools")),
        Some(&lens_loc("/r1", "tools")),
    );
}

#[test]
fn a_chain_of_places_is_not_reciprocal_and_does_not_collapse() {
    // Reciprocity is the whole criterion and is never weakened to one
    // direction. `a` inside `b` and `b` inside `c` share the place `b` in
    // opposite roles, but they are two genuine situations and both keep their
    // slots — only entries pointing at *each other* are one.
    let ranked = lens(vec![
        mirrors("a", lens_loc("/r1", "b"), 30_000),
        mirrors("b", lens_loc("/r1", "c"), 20_000),
    ]);
    let mut labels = entry_labels(&ranked);
    labels.sort();
    assert_eq!(labels, ["a", "b"]);
    assert!(ranked.reciprocal_places.is_empty());
}

#[test]
fn collapse_is_deterministic_across_runs() {
    // Which side survives is decided by subject path and by nothing else, so
    // an unchanged database always keeps the same place — input order, which
    // the engine's own sort already fixes, must not be able to change it
    // either.
    let forward = lens(reciprocal_pair());
    let mut reversed_input = reciprocal_pair();
    reversed_input.reverse();
    let reversed = lens(reversed_input);
    assert_eq!(entry_labels(&forward), entry_labels(&reversed));
    assert_eq!(forward.reciprocal_places, reversed.reciprocal_places);
    // The heavier place is not the one kept: the tie-break is the path, so the
    // rule cannot drift into a weight judgment.
    assert_eq!(entry_labels(&forward), ["downloads/tools"]);
}

#[test]
fn two_reciprocal_subsets_are_not_one_situation_and_both_keep_their_slots() {
    // The collapse is a claim about **mirrors**, and the topology alone does
    // not carry it. These two places contain each other's counterpart exactly
    // as a reciprocal mirror pair does, but each is only a *subset* of what it
    // points at: most of each place is content the other never mentions. They
    // are two overlaps, not one told twice, and folding them would delete a
    // real opportunity while printing "one decision resolves both" over what
    // survived.
    let subset = |rel: &str, counterpart: Location, gain_bytes: u64| {
        lens_finding(
            rel,
            FindingTier::Candidate,
            gain_bytes,
            0,
            FindingNature::Consolidate,
            RelationShape::Pair {
                counterpart,
                class: RelationClass::Subset,
                pair_size_pct: 0.6,
                pair_count_pct: 0.6,
                counterpart_share_pct: 0.95,
                counterpart_suspended: false,
                counterpart_is_archive: false,
                counterpart_last_scanned_at: None,
            },
        )
    };
    let ranked = lens(vec![
        subset("photos", lens_loc("/r1", "docs/photo-backup"), 60_000),
        subset("docs", lens_loc("/r1", "photos/doc-scans"), 30_000),
    ]);
    let mut labels = entry_labels(&ranked);
    labels.sort();
    assert_eq!(labels, ["docs", "photos"]);
    assert!(ranked.reciprocal_places.is_empty());
}

#[test]
fn the_board_order_is_total_so_a_full_tie_cannot_rest_on_construction_order() {
    // Two entries can reach the same ranking key **and** the same place: a run
    // headlined at `/r1/photos` and a hub whose members all point into
    // `/r1/photos` sit at one path, and the aggregates here coincide term for
    // term. Without a last discriminator the comparator is not total, and
    // `sort_by` is stable, so such a pair keeps whatever order it was built in.
    //
    // Through `reduction_lens` that order is fixed today — the kinds are pushed
    // by sequential loops — so driving the whole lens cannot tell a total
    // comparator from a lucky one. This sorts the pair **already reversed**,
    // which is exactly the arrangement a future reordering of those loops would
    // produce, and asserts the comparator puts it back.
    let shared = lens_loc("/r1", "photos");
    let member = |rel: &str| {
        on_root(
            lens_finding(
                rel,
                FindingTier::Clean,
                10_000,
                0,
                FindingNature::Consolidate,
                lens_pair(shared.clone()),
            ),
            2,
            "/r2",
        )
    };
    let hub = LeaderboardEntry::Hub(HubEntry {
        counterpart: shared.clone(),
        counterpart_is_archive: false,
        counterpart_last_scanned_at: None,
        nearness_root: None,
        members: vec![member("a"), member("b")],
        total_gain_bytes: 20_000,
        total_gain_files: 20,
    });
    let run = LeaderboardEntry::Parent(ParentEntry {
        parent: shared.clone(),
        coverage: 1.0,
        members: vec![
            sibling(1, "/r1", "photos/x", &lens_loc("/rx", "x")),
            sibling(1, "/r1", "photos/y", &lens_loc("/rx", "y")),
        ],
        gain_bytes_upper: 20_000,
        gain_files_upper: 20,
    });
    let nearness = RootNearness::default();
    let params = LensParams::default();

    // They really do tie: neither the key nor the place separates them.
    assert_eq!(
        entry_order(&run, &hub, &nearness, &params),
        std::cmp::Ordering::Less,
        "the kind is the only thing left to separate them"
    );
    assert_eq!(
        entry_order(&hub, &run, &nearness, &params),
        std::cmp::Ordering::Greater,
        "and it must separate them the same way from either side"
    );

    // Built hub-first, the sort must still put the run first.
    let mut entries = vec![hub, run];
    entries.sort_by(|a, b| entry_order(a, b, &nearness, &params));
    assert_eq!(
        entry_labels(&RankedSweep {
            entries,
            suspended: Vec::new(),
            stated_remainders: std::collections::HashMap::new(),
            reciprocal_places: std::collections::HashMap::new(),
            stats: SweepStats {
                ubiquitous_objects_dropped: 0,
                ubiquitous_bytes_dropped: 0,
                below_floor_subjects: 0,
            },
        }),
        ["parent:/r1:photos", "hub:photos"],
    );
}
