//! Stage B tests — LCA-subtraction matched weights.

use crate::sweep::domain::structural::{compute_matched, MatchedWeights, Universe};

use super::fixtures::{low_floors, make_root, make_source, universe};

fn matched_at(u: &Universe, weights: &MatchedWeights, ri: usize, path: &str) -> (u64, u32) {
    let fid = u.roots_data[ri].tree.id(path).unwrap() as usize;
    weights.matched[ri][fid]
}

#[test]
fn lca_subtraction_parent_unmatched() {
    let roots = vec![make_root(1, "/r1")];
    // The same object in two sibling folders of one root: each folder's
    // copy exists outside that folder, but nothing exists outside their
    // common parent.
    let sources = vec![
        make_source(1, 1, "docs/f1", 100, Some(10)),
        make_source(2, 1, "docs kopie/f1", 100, Some(10)),
    ];
    let u = universe(&sources, &roots, &low_floors());
    let matched = compute_matched(&u);
    assert_eq!(matched_at(&u, &matched, 0, "docs"), (100, 1));
    assert_eq!(matched_at(&u, &matched, 0, "docs kopie"), (100, 1));
    assert_eq!(matched_at(&u, &matched, 0, ""), (0, 0));
}

#[test]
fn three_copy_intra_root_subtracts_at_common_ancestor() {
    let roots = vec![make_root(1, "/r1")];
    let sources = vec![
        make_source(1, 1, "a/f", 100, Some(10)),
        make_source(2, 1, "b/f", 100, Some(10)),
        make_source(3, 1, "c/f", 100, Some(10)),
    ];
    let u = universe(&sources, &roots, &low_floors());
    let matched = compute_matched(&u);
    for path in ["a", "b", "c"] {
        assert_eq!(matched_at(&u, &matched, 0, path), (100, 1));
    }
    assert_eq!(matched_at(&u, &matched, 0, ""), (0, 0));
}

#[test]
fn intra_and_cross_copies_match_everywhere() {
    let roots = vec![make_root(1, "/r1"), make_root(2, "/r2")];
    let sources = vec![
        make_source(1, 1, "a/f1", 100, Some(10)),
        make_source(2, 1, "b/f2", 100, Some(10)),
        make_source(3, 2, "x/f", 100, Some(10)),
    ];
    let u = universe(&sources, &roots, &low_floors());
    let matched = compute_matched(&u);
    assert_eq!(matched_at(&u, &matched, 0, "a"), (100, 1));
    assert_eq!(matched_at(&u, &matched, 0, "b"), (100, 1));
    assert_eq!(matched_at(&u, &matched, 0, ""), (200, 2));
}

#[test]
fn same_folder_duplicates_match_nowhere() {
    let roots = vec![make_root(1, "/r1")];
    let sources = vec![
        make_source(1, 1, "a/f", 100, Some(10)),
        make_source(2, 1, "a/f copy", 100, Some(10)),
    ];
    let u = universe(&sources, &roots, &low_floors());
    let matched = compute_matched(&u);
    assert_eq!(matched_at(&u, &matched, 0, "a"), (0, 0));
    assert_eq!(matched_at(&u, &matched, 0, ""), (0, 0));
}
