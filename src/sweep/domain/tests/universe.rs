//! Stage A tests — the working set: folder interning and subtree sums.

use crate::sweep::domain::structural::SweepParams;

use super::fixtures::{make_root, make_source, universe};

#[test]
fn top_level_file_lands_in_root_folder() {
    let roots = vec![make_root(1, "/r1")];
    let sources = vec![make_source(1, 1, "file.jpg", 100, Some(10))];
    let u = universe(&sources, &roots, &SweepParams::default());
    let rd = &u.roots_data[0];
    let root_fid = 0u32;
    assert_eq!(rd.tree.path(root_fid), "");
    assert_eq!(rd.leaf_all[root_fid as usize], (100, 1));
    assert_eq!(rd.leaf_hashed[root_fid as usize], (100, 1));
}

#[test]
fn subtree_sums_aggregate_bottom_up() {
    let roots = vec![make_root(1, "/r1")];
    let sources = vec![
        make_source(1, 1, "a/one.bin", 10, Some(10)),
        make_source(2, 1, "a/b/two.bin", 20, Some(20)),
        make_source(3, 1, "a/c/three.bin", 30, None), // unhashed
        make_source(4, 1, "top.bin", 5, Some(40)),
    ];
    let u = universe(&sources, &roots, &SweepParams::default());
    let rd = &u.roots_data[0];
    let fid = |p: &str| rd.tree.id(p).unwrap() as usize;
    assert_eq!(rd.sub_all[fid("")], (65, 4));
    assert_eq!(rd.sub_all[fid("a")], (60, 3));
    assert_eq!(rd.sub_all[fid("a/b")], (20, 1));
    assert_eq!(rd.sub_hashed[fid("")], (35, 3));
    assert_eq!(rd.sub_hashed[fid("a")], (30, 2));
    assert_eq!(rd.sub_hashed[fid("a/c")], (0, 0));
}

#[test]
fn ubiquity_cap_drops_and_counts() {
    let params = SweepParams {
        ubiquity_cap: 2,
        ..SweepParams::default()
    };
    let roots = vec![make_root(1, "/r1"), make_root(2, "/r2")];
    // Object 10: three distinct (root, folder) locations — over the cap.
    // Object 20: two distinct locations — at the cap, stays.
    let sources = vec![
        make_source(1, 1, "a/f1", 100, Some(10)),
        make_source(2, 1, "b/f2", 100, Some(10)),
        make_source(3, 2, "c/f3", 100, Some(10)),
        make_source(4, 1, "a/g1", 40, Some(20)),
        make_source(5, 2, "c/g2", 40, Some(20)),
    ];
    let u = universe(&sources, &roots, &params);
    assert!(!u.obj_locs.contains_key(&10));
    assert!(!u.obj_size.contains_key(&10));
    assert!(u.obj_locs.contains_key(&20));
    assert_eq!(u.stats.ubiquitous_objects_dropped, 1);
    assert_eq!(u.stats.ubiquitous_bytes_dropped, 100);
    // Dropped object leaves hashed totals but stays in all-content totals.
    let rd = &u.roots_data[0];
    let fid_a = rd.tree.id("a").unwrap() as usize;
    assert_eq!(rd.leaf_all[fid_a], (140, 2));
    assert_eq!(rd.leaf_hashed[fid_a], (40, 1));
    assert!(rd.files[fid_a].iter().all(|&(oid, _)| oid != 10));
}

#[test]
fn multiple_copies_in_one_folder_count_as_one_location() {
    let params = SweepParams {
        ubiquity_cap: 2,
        ..SweepParams::default()
    };
    let roots = vec![make_root(1, "/r1"), make_root(2, "/r2")];
    // Object 10: two copies in the same folder plus one elsewhere —
    // two distinct locations, at the cap, stays.
    let sources = vec![
        make_source(1, 1, "a/f1", 100, Some(10)),
        make_source(2, 1, "a/f1-copy", 100, Some(10)),
        make_source(3, 2, "c/f3", 100, Some(10)),
    ];
    let u = universe(&sources, &roots, &params);
    assert!(u.obj_locs.contains_key(&10));
    assert_eq!(u.stats.ubiquitous_objects_dropped, 0);
}
