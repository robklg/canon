//! Stage D tests — relation shape resolution: pair vs. coverage, the
//! concentration walk, and reciprocal-mirror dedup.

use crate::core::domain::root::Root;
use crate::sweep::domain::lens::{reduction_lens, LeaderboardEntry, LensParams, RootNearness};
use crate::sweep::domain::structural::{
    FindingNature, FindingTier, LocalizedSubject, RelationClass, RelationShape, StructuralFinding,
    Universe,
};

use super::fixtures::{
    low_floors, make_archive_root, make_root, make_source, run_localize, run_structural,
};

fn find_subject<'a>(
    u: &Universe,
    localized: &'a [LocalizedSubject],
    root_idx: usize,
    path: &str,
) -> &'a LocalizedSubject {
    localized
        .iter()
        .find(|ls| {
            ls.raw.root_idx == root_idx && u.roots_data[root_idx].tree.path(ls.raw.fid) == path
        })
        .unwrap_or_else(|| panic!("no subject at root {root_idx} path {path:?}"))
}

#[test]
fn tight_counterpart_found_through_nesting() {
    let roots = vec![make_root(1, "/r1"), make_root(2, "/r2")];
    let sources = vec![
        make_source(1, 1, "dup/f1", 100, Some(10)),
        make_source(2, 1, "dup/f2", 100, Some(20)),
        make_source(3, 1, "noise/u", 100, Some(90)), // unique
        make_source(4, 2, "archive/2020/photos/f1", 100, Some(10)),
        make_source(5, 2, "archive/2020/photos/f2", 100, Some(20)),
    ];
    let (u, localized) = run_localize(&sources, &roots, &low_floors());
    let ls = find_subject(&u, &localized, 0, "dup");
    match &ls.shape {
        RelationShape::Pair {
            counterpart,
            class,
            pair_size_pct,
            counterpart_share_pct,
            ..
        } => {
            assert_eq!(counterpart.rel_prefix, "archive/2020/photos");
            assert_eq!(counterpart.root_path, "/r2");
            assert_eq!(*class, RelationClass::Mirror);
            assert!((pair_size_pct - 1.0).abs() < 1e-9);
            assert!((counterpart_share_pct - 1.0).abs() < 1e-9);
        }
        RelationShape::Coverage { .. } => panic!("expected a pair statement"),
    }
}

#[test]
fn scattered_counterpart_degrades_to_coverage() {
    let mut roots = vec![make_root(1, "/r1"), make_archive_root(2, "/r2")];
    for id in 3..=6 {
        roots.push(make_root(id, &format!("/r{id}")));
    }
    let mut sources = vec![make_source(100, 1, "noise/u", 100, Some(90))];
    for i in 0..5i64 {
        sources.push(make_source(
            i + 1,
            1,
            &format!("scatter/f{i}"),
            100,
            Some(10 + i),
        ));
        sources.push(make_source(i + 50, i + 2, "k/f", 100, Some(10 + i)));
    }
    let (u, localized) = run_localize(&sources, &roots, &low_floors());
    let ls = find_subject(&u, &localized, 0, "scatter");
    match &ls.shape {
        RelationShape::Coverage {
            locations,
            archived_locations,
            suspended_locations,
        } => {
            assert_eq!(*locations, 5);
            assert_eq!(*archived_locations, 1);
            assert_eq!(*suspended_locations, 0);
        }
        RelationShape::Pair { .. } => panic!("expected a coverage statement"),
    }
    // Every counterpart scope remains visible as context.
    assert_eq!(ls.context.len(), 5);
}

#[test]
fn ancestor_settle_degrades_to_coverage() {
    let roots = vec![make_root(1, "/r1")];
    // Subject a/sub's outside copies scatter over two ancestor branches:
    // the concentration walk settles on the root folder, an ancestor of
    // the subject — no counterpart, coverage instead.
    let sources = vec![
        make_source(1, 1, "a/sub/f1", 100, Some(10)),
        make_source(2, 1, "a/x/f1c", 100, Some(10)),
        make_source(3, 1, "a/sub/f2", 100, Some(20)),
        make_source(4, 1, "b/f2c", 100, Some(20)),
    ];
    let (u, localized) = run_localize(&sources, &roots, &low_floors());
    let ls = find_subject(&u, &localized, 0, "a/sub");
    match &ls.shape {
        RelationShape::Coverage {
            locations,
            archived_locations,
            suspended_locations,
        } => {
            assert_eq!(*locations, 1);
            assert_eq!(*archived_locations, 0);
            assert_eq!(*suspended_locations, 0);
        }
        RelationShape::Pair { .. } => panic!("an ancestor must not become a counterpart"),
    }
}

#[test]
fn intra_root_sibling_pair_dedups_to_canonical_mirror() {
    let roots = vec![make_root(1, "/r1")];
    let sources = vec![
        make_source(1, 1, "docs/f1", 100, Some(10)),
        make_source(2, 1, "docs kopie/f1", 100, Some(10)),
    ];
    let (u, localized) = run_localize(&sources, &roots, &low_floors());
    assert_eq!(localized.len(), 1);
    let ls = &localized[0];
    assert_eq!(u.roots_data[0].tree.path(ls.raw.fid), "docs");
    match &ls.shape {
        RelationShape::Pair {
            counterpart, class, ..
        } => {
            assert_eq!(counterpart.rel_prefix, "docs kopie");
            assert_eq!(*class, RelationClass::Mirror);
        }
        RelationShape::Coverage { .. } => panic!("expected an intra-root pair"),
    }
}

#[test]
fn subset_when_counterpart_larger() {
    let roots = vec![make_root(1, "/r1"), make_root(2, "/r2")];
    let sources = vec![
        make_source(1, 1, "a/f1", 100, Some(10)),
        make_source(2, 1, "n/u", 50, Some(90)), // unique
        make_source(3, 2, "b/f1", 100, Some(10)),
        make_source(4, 2, "b/extra", 100, Some(80)), // unique to b
    ];
    let (u, localized) = run_localize(&sources, &roots, &low_floors());
    let ls = find_subject(&u, &localized, 0, "a");
    match &ls.shape {
        RelationShape::Pair {
            counterpart,
            class,
            pair_size_pct,
            counterpart_share_pct,
            ..
        } => {
            assert_eq!(counterpart.rel_prefix, "b");
            assert_eq!(*class, RelationClass::Subset);
            assert!((pair_size_pct - 1.0).abs() < 1e-9);
            assert!((counterpart_share_pct - 0.5).abs() < 1e-9);
        }
        RelationShape::Coverage { .. } => panic!("expected a pair statement"),
    }
}

#[test]
fn mirror_reciprocal_dedups_canonical() {
    let roots = vec![make_root(1, "/r1"), make_root(2, "/r2")];
    let sources = vec![
        make_source(1, 1, "m/f1", 100, Some(10)),
        make_source(2, 1, "m/f2", 100, Some(20)),
        make_source(3, 1, "u1/x", 50, Some(91)),
        make_source(4, 2, "n/f1", 100, Some(10)),
        make_source(5, 2, "n/f2", 100, Some(20)),
        make_source(6, 2, "u2/y", 50, Some(92)),
    ];
    let (u, localized) = run_localize(&sources, &roots, &low_floors());
    let mirrors: Vec<_> = localized
        .iter()
        .filter(|ls| {
            matches!(
                ls.shape,
                RelationShape::Pair {
                    class: RelationClass::Mirror,
                    ..
                }
            )
        })
        .collect();
    assert_eq!(mirrors.len(), 1);
    assert_eq!(mirrors[0].raw.root_idx, 0);
    assert_eq!(u.roots_data[0].tree.path(mirrors[0].raw.fid), "m");
}

#[test]
fn subset_reciprocal_not_deduped() {
    let roots = vec![make_root(1, "/r1"), make_root(2, "/r2")];
    let sources = vec![
        make_source(1, 1, "a/f1", 100, Some(10)),
        make_source(2, 1, "z1/u", 100, Some(91)),
        make_source(3, 2, "b/f1", 100, Some(10)),
        make_source(4, 2, "b/u", 50, Some(93)), // unique inside b
        make_source(5, 2, "z2/u", 100, Some(92)),
    ];
    let (u, localized) = run_localize(&sources, &roots, &low_floors());
    // Subject a (clean, subset of b) and subject b (candidate, partially
    // inside a) are distinct honest statements — both survive.
    let a = find_subject(&u, &localized, 0, "a");
    let b = find_subject(&u, &localized, 1, "b");
    assert!(matches!(
        a.shape,
        RelationShape::Pair {
            class: RelationClass::Subset,
            ..
        }
    ));
    assert!(matches!(
        b.shape,
        RelationShape::Pair {
            class: RelationClass::Subset,
            ..
        }
    ));
    assert!(matches!(b.raw.tier, FindingTier::Candidate));
}

#[test]
fn context_lines_carry_secondary_counterparts() {
    let roots = vec![
        make_root(1, "/r1"),
        make_root(2, "/r2"),
        make_root(3, "/r3"),
    ];
    let mut sources = vec![make_source(200, 1, "noise/u", 200, Some(90))];
    for i in 0..19i64 {
        sources.push(make_source(i + 1, 1, &format!("s/f{i}"), 100, Some(10 + i)));
        sources.push(make_source(
            i + 100,
            2,
            &format!("main/f{i}"),
            100,
            Some(10 + i),
        ));
    }
    sources.push(make_source(20, 1, "s/f19", 100, Some(50)));
    sources.push(make_source(150, 3, "side/f19", 100, Some(50)));
    let (u, localized) = run_localize(&sources, &roots, &low_floors());
    let ls = find_subject(&u, &localized, 0, "s");
    match &ls.shape {
        RelationShape::Pair {
            counterpart,
            pair_size_pct,
            ..
        } => {
            assert_eq!(counterpart.root_path, "/r2");
            assert_eq!(counterpart.rel_prefix, "main");
            assert!((pair_size_pct - 0.95).abs() < 1e-9);
        }
        RelationShape::Coverage { .. } => panic!("expected a pair statement"),
    }
    assert_eq!(ls.context.len(), 1);
    assert_eq!(ls.context[0].location.root_path, "/r3");
    assert_eq!(ls.context[0].location.rel_prefix, "side");
    assert!((ls.context[0].size_pct - 0.05).abs() < 1e-9);
}

// ---------------------------------------------------------------------------
// Which place is cited as evidence: prefer a witness the user can look at.
// ---------------------------------------------------------------------------

fn parked(mut r: Root) -> Root {
    r.suspended = true;
    r
}

/// Subject `a` on `/r1`, fully copied into two counterparts that each hold
/// more besides: a parked source root and a live archive root. The parked
/// root's path sorts first, so before the live preference it won the citation
/// on alphabetical accident alone — both scopes concentrate identically.
fn two_witnesses(park_the_first: bool) -> (Vec<crate::core::domain::source::Source>, Vec<Root>) {
    let first = make_root(2, "/r0-other");
    let roots = vec![
        make_root(1, "/r1"),
        if park_the_first { parked(first) } else { first },
        make_archive_root(3, "/r2-live"),
    ];
    let sources = vec![
        make_source(1, 1, "a/f1", 100, Some(10)),
        make_source(2, 1, "a/f2", 100, Some(20)),
        make_source(3, 1, "noise/u", 100, Some(90)),
        make_source(4, 2, "k/f1", 100, Some(10)),
        make_source(5, 2, "k/f2", 100, Some(20)),
        make_source(6, 2, "k/extra", 500, Some(91)),
        make_source(7, 3, "arch/f1", 100, Some(10)),
        make_source(8, 3, "arch/f2", 100, Some(20)),
        make_source(9, 3, "arch/extra", 500, Some(92)),
    ];
    (sources, roots)
}

fn subject_a(sweep: &crate::sweep::domain::structural::StructuralSweep) -> &StructuralFinding {
    sweep
        .findings
        .iter()
        .find(|f| f.subject.root_path == "/r1" && f.subject.rel_prefix == "a")
        .expect("subject a")
}

#[test]
fn a_live_scope_wins_the_counterpart_choice_over_a_suspended_one() {
    let (sources, roots) = two_witnesses(true);
    let (u, localized) = run_localize(&sources, &roots, &low_floors());
    let ls = find_subject(&u, &localized, 0, "a");
    match &ls.shape {
        RelationShape::Pair {
            counterpart,
            counterpart_suspended,
            ..
        } => {
            assert_eq!(counterpart.root_path, "/r2-live");
            assert!(!counterpart_suspended);
        }
        RelationShape::Coverage { .. } => panic!("expected a pair statement"),
    }
    // The parked scope loses the citation, not its testimony: it stays on a
    // context line. Suspension decides which place is cited, never what the
    // evidence says.
    assert_eq!(ls.context.len(), 1);
    assert_eq!(ls.context[0].location.root_path, "/r0-other");
    assert_eq!(ls.context[0].location.rel_prefix, "k");
}

#[test]
fn a_place_covered_by_a_live_archive_does_not_sink_when_a_suspended_root_sorts_first() {
    // The alphabetical-accident regression: before the live preference this
    // place was billed to the parked root, took `Verify`, and left the board.
    let (sources, roots) = two_witnesses(true);
    let sweep = run_structural(&sources, &roots, &low_floors());
    assert_ne!(subject_a(&sweep).nature, FindingNature::Verify);
    let ranked = reduction_lens(sweep, &RootNearness::default(), &LensParams::default());
    assert!(ranked.entries.iter().any(|e| match e {
        LeaderboardEntry::Single(f) => f.subject.root_path == "/r1" && f.subject.rel_prefix == "a",
        LeaderboardEntry::Root(_) | LeaderboardEntry::Hub(_) => false,
    }));
    // Nothing on the parked root's line on the subject's account.
    assert!(ranked
        .suspended
        .iter()
        .all(|t| t.places_with_copies_on_it == 0));
}

#[test]
fn the_sink_still_fires_when_no_live_scope_qualifies() {
    // The same shape with the live archive removed: the parked scope is the
    // only witness, so it is cited exactly as before and the place sinks.
    let roots = vec![make_root(1, "/r1"), parked(make_root(2, "/r0-other"))];
    let sources = vec![
        make_source(1, 1, "a/f1", 100, Some(10)),
        make_source(2, 1, "a/f2", 100, Some(20)),
        make_source(3, 1, "noise/u", 100, Some(90)),
        make_source(4, 2, "k/f1", 100, Some(10)),
        make_source(5, 2, "k/f2", 100, Some(20)),
        make_source(6, 2, "k/extra", 500, Some(91)),
    ];
    let sweep = run_structural(&sources, &roots, &low_floors());
    assert_eq!(subject_a(&sweep).nature, FindingNature::Verify);
    let ranked = reduction_lens(sweep, &RootNearness::default(), &LensParams::default());
    assert!(ranked.entries.is_empty());
    assert_eq!(ranked.suspended.len(), 1);
    assert_eq!(ranked.suspended[0].root_path, "/r0-other");
    assert_eq!(ranked.suspended[0].places_with_copies_on_it, 1);
}

#[test]
fn preferring_a_live_counterpart_leaves_gain_and_residual_untouched() {
    // The same universe twice, differing only in whether the first-sorting
    // root's door is closed. The citation moves; the evidence does not.
    let (parked_sources, parked_roots) = two_witnesses(true);
    let (open_sources, open_roots) = two_witnesses(false);
    let with_door_closed = run_structural(&parked_sources, &parked_roots, &low_floors());
    let with_door_open = run_structural(&open_sources, &open_roots, &low_floors());
    let closed = subject_a(&with_door_closed);
    let open = subject_a(&with_door_open);

    let cited = |f: &StructuralFinding| match &f.shape {
        RelationShape::Pair { counterpart, .. } => counterpart.root_path.clone(),
        RelationShape::Coverage { .. } => panic!("expected a pair statement"),
    };
    assert_eq!(cited(closed), "/r2-live");
    assert_eq!(cited(open), "/r0-other");

    assert_eq!(closed.gain_bytes, open.gain_bytes);
    assert_eq!(closed.gain_files, open.gain_files);
    assert_eq!(closed.residual_bytes, open.residual_bytes);
    assert_eq!(closed.residual_files, open.residual_files);
    assert!((closed.containment_size_pct - open.containment_size_pct).abs() < 1e-12);
    assert!((closed.containment_count_pct - open.containment_count_pct).abs() < 1e-12);
}

// ---------------------------------------------------------------------------
// Scattered evidence says how much of it is behind a closed door.
// ---------------------------------------------------------------------------

/// Subject `scatter` on `/r1`, its five matched objects one apiece across
/// five other roots — too spread for any one to concentrate. `park` names how
/// many of those five roots the user suspended.
fn scattered_over_five(park: usize) -> (Vec<crate::core::domain::source::Source>, Vec<Root>) {
    let mut roots = vec![make_root(1, "/r1"), make_archive_root(2, "/r2")];
    for id in 3..=6 {
        roots.push(make_root(id, &format!("/r{id}")));
    }
    for r in roots.iter_mut().skip(1).take(park) {
        r.suspended = true;
    }
    let mut sources = vec![make_source(100, 1, "noise/u", 100, Some(90))];
    for i in 0..5i64 {
        sources.push(make_source(
            i + 1,
            1,
            &format!("scatter/f{i}"),
            100,
            Some(10 + i),
        ));
        sources.push(make_source(i + 50, i + 2, "k/f", 100, Some(10 + i)));
    }
    (sources, roots)
}

#[test]
fn a_coverage_shape_counts_its_suspended_locations() {
    let (sources, roots) = scattered_over_five(2);
    let (u, localized) = run_localize(&sources, &roots, &low_floors());
    let ls = find_subject(&u, &localized, 0, "scatter");
    match &ls.shape {
        RelationShape::Coverage {
            locations,
            archived_locations,
            suspended_locations,
        } => {
            assert_eq!(*locations, 5);
            assert_eq!(*archived_locations, 1);
            assert_eq!(*suspended_locations, 2);
        }
        RelationShape::Pair { .. } => panic!("expected a coverage statement"),
    }
}

#[test]
fn a_suspended_location_changes_no_count_and_no_percentage() {
    // The same universe with and without two of the five doors closed. A
    // location on a suspended root is still a location, and the content is
    // still there: recomputing containment without it would make real copies
    // read as residual.
    let (open_sources, open_roots) = scattered_over_five(0);
    let (parked_sources, parked_roots) = scattered_over_five(2);
    let open = run_structural(&open_sources, &open_roots, &low_floors());
    let parked = run_structural(&parked_sources, &parked_roots, &low_floors());
    let find = |sweep: &crate::sweep::domain::structural::StructuralSweep| {
        sweep
            .findings
            .iter()
            .find(|f| f.subject.root_path == "/r1" && f.subject.rel_prefix == "scatter")
            .expect("subject scatter")
            .gain_bytes
    };
    let counts = |sweep: &crate::sweep::domain::structural::StructuralSweep| {
        let f = sweep
            .findings
            .iter()
            .find(|f| f.subject.root_path == "/r1" && f.subject.rel_prefix == "scatter")
            .expect("subject scatter");
        match &f.shape {
            RelationShape::Coverage {
                locations,
                archived_locations,
                ..
            } => (
                *locations,
                *archived_locations,
                f.containment_size_pct,
                f.residual_bytes,
            ),
            RelationShape::Pair { .. } => panic!("expected a coverage statement"),
        }
    };
    assert_eq!(find(&open), find(&parked));
    let (lo, ao, co, ro) = counts(&open);
    let (lp, ap, cp, rp) = counts(&parked);
    assert_eq!((lo, ao, ro), (lp, ap, rp));
    assert!((co - cp).abs() < 1e-12);
    // ...and the entry is neither sunk nor demoted for it.
    let ranked = reduction_lens(parked, &RootNearness::default(), &LensParams::default());
    assert!(ranked.entries.iter().any(|e| match e {
        LeaderboardEntry::Single(f) =>
            f.subject.root_path == "/r1" && f.subject.rel_prefix == "scatter",
        LeaderboardEntry::Root(_) | LeaderboardEntry::Hub(_) => false,
    }));
}
