//! Stage D tests — relation shape resolution: pair vs. coverage, the
//! concentration walk, and reciprocal-mirror dedup.

use crate::sweep::domain::structural::{
    FindingTier, LocalizedSubject, RelationClass, RelationShape, Universe,
};

use super::fixtures::{low_floors, make_archive_root, make_root, make_source, run_localize};

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
        } => {
            assert_eq!(*locations, 5);
            assert_eq!(*archived_locations, 1);
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
        } => {
            assert_eq!(*locations, 1);
            assert_eq!(*archived_locations, 0);
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
