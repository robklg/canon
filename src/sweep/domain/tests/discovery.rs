//! Stage C tests — the descent that emits maximal subjects.

use crate::sweep::domain::structural::{
    compute_matched, discover_subjects, FindingTier, RawSubject, SweepParams, Universe,
};

use super::fixtures::{low_floors, make_root, make_source, universe};

fn subject_paths(u: &Universe, subjects: &[RawSubject]) -> Vec<(usize, String)> {
    let mut out: Vec<(usize, String)> = subjects
        .iter()
        .map(|s| {
            (
                s.root_idx,
                u.roots_data[s.root_idx].tree.path(s.fid).to_string(),
            )
        })
        .collect();
    out.sort();
    out
}

#[test]
fn maximality_emits_once_at_the_top() {
    let roots = vec![make_root(1, "/r1"), make_root(2, "/r2")];
    let sources = vec![
        make_source(1, 1, "backup/x/f1", 100, Some(10)),
        make_source(2, 1, "backup/y/f2", 100, Some(20)),
        make_source(3, 1, "other/f3", 100, Some(30)), // unique
        make_source(4, 2, "orig/x/c1", 100, Some(10)),
        make_source(5, 2, "orig/y/c2", 100, Some(20)),
    ];
    let u = universe(&sources, &roots, &low_floors());
    let matched = compute_matched(&u);
    let subjects = discover_subjects(&u, &matched, &low_floors());
    let root1: Vec<_> = subjects.iter().filter(|s| s.root_idx == 0).collect();
    assert_eq!(root1.len(), 1);
    assert_eq!(u.roots_data[0].tree.path(root1[0].fid), "backup");
    assert!(matches!(root1[0].tier, FindingTier::Clean));
    assert_eq!(root1[0].matched_bytes, 200);
    assert_eq!(root1[0].matched_files, 2);
}

#[test]
fn ancestor_descendant_pairs_unreachable() {
    // A varied fixture: mirrored subtree, partial subtree, unique noise.
    let roots = vec![make_root(1, "/r1"), make_root(2, "/r2")];
    let sources = vec![
        make_source(1, 1, "backup/x/f1", 100, Some(10)),
        make_source(2, 1, "backup/y/f2", 100, Some(20)),
        make_source(3, 1, "backup/z/f3", 100, Some(30)),
        make_source(4, 1, "mixed/in/f4", 100, Some(40)),
        make_source(5, 1, "mixed/out/f5", 100, Some(50)), // unique
        make_source(6, 1, "noise/f6", 100, Some(60)),     // unique
        make_source(7, 2, "keep/f1", 100, Some(10)),
        make_source(8, 2, "keep/f2", 100, Some(20)),
        make_source(9, 2, "keep/f3", 100, Some(30)),
        make_source(10, 2, "keep/f4", 100, Some(40)),
    ];
    let u = universe(&sources, &roots, &low_floors());
    let matched = compute_matched(&u);
    let subjects = discover_subjects(&u, &matched, &low_floors());
    assert!(!subjects.is_empty());
    for a in &subjects {
        for b in &subjects {
            if std::ptr::eq(a, b) || a.root_idx != b.root_idx {
                continue;
            }
            let tree = &u.roots_data[a.root_idx].tree;
            assert!(
                !tree.is_ancestor_or_equal(a.fid, b.fid),
                "subject {} contains subject {}",
                tree.path(a.fid),
                tree.path(b.fid),
            );
        }
    }
}

#[test]
fn mixed_parent_emits_clean_children() {
    let roots = vec![make_root(1, "/r1"), make_root(2, "/r2")];
    let sources = vec![
        make_source(1, 1, "p/a/f1", 100, Some(10)),
        make_source(2, 1, "p/b/f2", 100, Some(20)), // unique
        make_source(3, 2, "q/f1", 100, Some(10)),
    ];
    let u = universe(&sources, &roots, &low_floors());
    let matched = compute_matched(&u);
    let subjects = discover_subjects(&u, &matched, &low_floors());
    let root1 = subject_paths(&u, &subjects)
        .into_iter()
        .filter(|(ri, _)| *ri == 0)
        .map(|(_, p)| p)
        .collect::<Vec<_>>();
    assert_eq!(root1, vec!["p/a".to_string()]);
}

#[test]
fn parent_candidate_when_children_below_floors() {
    // Five children each 3 MB matched (below the 10 MB / 25 file floors);
    // the parent aggregates 15 MB matched of 25 MB total: 60%, candidate.
    let roots = vec![make_root(1, "/r1"), make_root(2, "/r2")];
    let mut sources = vec![make_source(100, 1, "p/unique.bin", 10_000_000, Some(999))];
    for i in 0..5i64 {
        sources.push(make_source(
            i + 1,
            1,
            &format!("p/c{i}/f{i}"),
            3_000_000,
            Some(10 + i),
        ));
        sources.push(make_source(
            i + 50,
            2,
            &format!("q/f{i}"),
            3_000_000,
            Some(10 + i),
        ));
    }
    let u = universe(&sources, &roots, &SweepParams::default());
    let matched = compute_matched(&u);
    let subjects = discover_subjects(&u, &matched, &SweepParams::default());
    let root1: Vec<_> = subjects.iter().filter(|s| s.root_idx == 0).collect();
    assert_eq!(root1.len(), 1);
    assert_eq!(u.roots_data[0].tree.path(root1[0].fid), "p");
    assert!(matches!(root1[0].tier, FindingTier::Candidate));
}

#[test]
fn tight_tolerance_fragments_a_finding() {
    // One logical finding (parent ~97% contained) with children at 100%,
    // 100%, and 90%: the default tolerance emits the parent once; a
    // too-tight tolerance fragments it into the children. Unique content
    // outside `p` keeps the subject from lifting to the whole root.
    let roots = vec![make_root(1, "/r1"), make_root(2, "/r2")];
    let sources = vec![
        make_source(1, 1, "p/c1/f1", 100, Some(10)),
        make_source(2, 1, "p/c2/f2", 100, Some(20)),
        make_source(3, 1, "p/c3/f3", 90, Some(30)),
        make_source(4, 1, "p/c3/u", 10, Some(40)), // unique
        make_source(5, 1, "z/u2", 100, Some(50)),  // unique, outside p
        make_source(6, 2, "q/f1", 100, Some(10)),
        make_source(7, 2, "q/f2", 100, Some(20)),
        make_source(8, 2, "q/f3", 90, Some(30)),
    ];
    let u = universe(&sources, &roots, &low_floors());
    let matched = compute_matched(&u);

    let at_default = discover_subjects(&u, &matched, &low_floors());
    let root1_default: Vec<_> = at_default.iter().filter(|s| s.root_idx == 0).collect();
    assert_eq!(root1_default.len(), 1);
    assert_eq!(u.roots_data[0].tree.path(root1_default[0].fid), "p");

    let tight = SweepParams {
        lifting_tolerance: 0.99,
        ..low_floors()
    };
    let at_tight = discover_subjects(&u, &matched, &tight);
    let root1_tight = subject_paths(&u, &at_tight)
        .into_iter()
        .filter(|(ri, _)| *ri == 0)
        .map(|(_, p)| p)
        .collect::<Vec<_>>();
    assert_eq!(
        root1_tight,
        vec!["p/c1".to_string(), "p/c2".to_string(), "p/c3".to_string()]
    );
}

#[test]
fn loose_tolerance_lifts_into_diluted_parent() {
    // backup fully mirrored, sibling only 70% covered: the default
    // tolerance keeps the clean finding at backup; a too-loose tolerance
    // lifts the subject into the diluted parent.
    let roots = vec![make_root(1, "/r1"), make_root(2, "/r2")];
    let sources = vec![
        make_source(1, 1, "top/backup/f1", 200, Some(10)),
        make_source(2, 1, "top/other/f2", 70, Some(20)),
        make_source(3, 1, "top/other/u", 30, Some(30)), // unique
        make_source(4, 1, "elsewhere/u2", 100, Some(40)), // unique, outside top
        make_source(5, 2, "q/f1", 200, Some(10)),
        make_source(6, 2, "q/f2", 70, Some(20)),
    ];
    let u = universe(&sources, &roots, &low_floors());
    let matched = compute_matched(&u);

    let at_default = discover_subjects(&u, &matched, &low_floors());
    let root1_default = subject_paths(&u, &at_default)
        .into_iter()
        .filter(|(ri, _)| *ri == 0)
        .map(|(_, p)| p)
        .collect::<Vec<_>>();
    assert!(root1_default.contains(&"top/backup".to_string()));

    let loose = SweepParams {
        lifting_tolerance: 0.80,
        ..low_floors()
    };
    let at_loose = discover_subjects(&u, &matched, &loose);
    let root1_loose: Vec<_> = at_loose.iter().filter(|s| s.root_idx == 0).collect();
    assert_eq!(root1_loose.len(), 1);
    assert_eq!(u.roots_data[0].tree.path(root1_loose[0].fid), "top");
}

#[test]
fn floors_trim_small_subjects() {
    let roots = vec![make_root(1, "/r1"), make_root(2, "/r2")];
    let sources = vec![
        make_source(1, 1, "small/f", 5_000_000, Some(10)),
        make_source(2, 2, "q/f", 5_000_000, Some(10)),
    ];
    let u = universe(&sources, &roots, &SweepParams::default());
    let matched = compute_matched(&u);
    let subjects = discover_subjects(&u, &matched, &SweepParams::default());
    assert!(subjects.is_empty());
}

#[test]
fn file_floor_clears_when_byte_floor_does_not() {
    let roots = vec![make_root(1, "/r1"), make_root(2, "/r2")];
    let mut sources = vec![
        // Unique bulk outside `many` keeps the subject from lifting to
        // the whole root.
        make_source(500, 1, "other/u", 1_000_000, Some(999)),
    ];
    for i in 0..30i64 {
        sources.push(make_source(
            i + 1,
            1,
            &format!("many/f{i}"),
            1_000,
            Some(10 + i),
        ));
        sources.push(make_source(
            i + 100,
            2,
            &format!("q/f{i}"),
            1_000,
            Some(10 + i),
        ));
    }
    let u = universe(&sources, &roots, &SweepParams::default());
    let matched = compute_matched(&u);
    let subjects = discover_subjects(&u, &matched, &SweepParams::default());
    let root1: Vec<_> = subjects.iter().filter(|s| s.root_idx == 0).collect();
    assert_eq!(root1.len(), 1);
    assert_eq!(u.roots_data[0].tree.path(root1[0].fid), "many");
}
