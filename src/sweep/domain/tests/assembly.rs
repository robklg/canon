//! Stage E tests — the orchestrator, finding assembly, and the
//! floor-released second walk.

use crate::core::domain::root::Root;
use crate::core::domain::source::Source;
use crate::sweep::domain::structural::{
    FindingNature, FindingTier, RelationShape, StructuralFinding, StructuralSweep, SweepParams,
};

use super::fixtures::{
    low_floors, make_archive_root, make_root, make_source, run_structural, scale_fixture,
};

fn find_finding<'a>(
    sweep: &'a StructuralSweep,
    root_path: &str,
    rel_prefix: &str,
) -> &'a StructuralFinding {
    sweep
        .findings
        .iter()
        .find(|f| f.subject.root_path == root_path && f.subject.rel_prefix == rel_prefix)
        .unwrap_or_else(|| panic!("no finding at {root_path} {rel_prefix:?}"))
}

/// Fixture with one above-floor subject (`big`, 20 MB) and one disjoint
/// below-floor subject (`small`, 2 MB / 1 file); unique noise keeps the
/// subjects from lifting to the whole root.
fn floor_split_fixture() -> (Vec<Source>, Vec<Root>) {
    let roots = vec![make_root(1, "/r1"), make_root(2, "/r2")];
    let sources = vec![
        make_source(1, 1, "big/f", 20_000_000, Some(10)),
        make_source(2, 1, "small/f", 2_000_000, Some(20)),
        make_source(3, 1, "noise/u", 5_000_000, Some(90)),
        make_source(4, 2, "q/f", 20_000_000, Some(10)),
        make_source(5, 2, "q2/f", 2_000_000, Some(20)),
    ];
    (sources, roots)
}

#[test]
fn residual_arithmetic_excludes_unhashed() {
    let roots = vec![make_root(1, "/r1"), make_root(2, "/r2")];
    let sources = vec![
        make_source(1, 1, "s/f1", 100, Some(10)),
        make_source(2, 1, "s/f2", 50, Some(20)), // unique: the residual
        make_source(3, 1, "s/u", 30, None),      // unhashed: qualifier, not residual
        make_source(4, 2, "q/f1", 100, Some(10)),
    ];
    let sweep = run_structural(&sources, &roots, &low_floors());
    let f = find_finding(&sweep, "/r1", "s");
    assert_eq!(f.gain_bytes, 100);
    assert_eq!(f.gain_files, 1);
    assert_eq!(f.residual_bytes, 50);
    assert_eq!(f.residual_files, 1);
    assert!((f.containment_size_pct - 100.0 / 150.0).abs() < 1e-9);
    assert!((f.hash_coverage_pct - 150.0 / 180.0).abs() < 1e-9);
    assert!(matches!(f.tier, FindingTier::Candidate));
}

#[test]
fn below_floor_subjects_counted_but_not_assembled_by_default() {
    let (sources, roots) = floor_split_fixture();
    let sweep = run_structural(&sources, &roots, &SweepParams::default());
    assert_eq!(sweep.stats.below_floor_subjects, 1);
    let f = find_finding(&sweep, "/r1", "big");
    assert!(!f.below_floors);
    assert!(!sweep
        .findings
        .iter()
        .any(|f| f.subject.root_path == "/r1" && f.subject.rel_prefix == "small"));
}

#[test]
fn below_floor_subjects_assembled_and_tagged_on_request() {
    let (sources, roots) = floor_split_fixture();
    let params = SweepParams {
        assemble_below_floors: true,
        ..SweepParams::default()
    };
    let sweep = run_structural(&sources, &roots, &params);
    assert_eq!(sweep.stats.below_floor_subjects, 1);
    let small = find_finding(&sweep, "/r1", "small");
    assert!(small.below_floors);
    // A below-floor finding is a full finding: localized like any other.
    match &small.shape {
        RelationShape::Pair { counterpart, .. } => {
            assert_eq!(counterpart.root_path, "/r2");
        }
        other => panic!("expected pair shape, got {other:?}"),
    }
    assert!(!find_finding(&sweep, "/r1", "big").below_floors);
}

#[test]
fn parent_candidate_view_unchanged_with_zero_below_floor_count() {
    // The parent-aggregation fixture: five children each below floors
    // lift into one candidate parent. The floor-released walk finds the
    // children instead, but they fall inside the parent subject — the
    // default view is unchanged and nothing counts as below-floor.
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
    let sweep = run_structural(&sources, &roots, &SweepParams::default());
    assert_eq!(sweep.stats.below_floor_subjects, 0);
    let p = find_finding(&sweep, "/r1", "p");
    assert!(matches!(p.tier, FindingTier::Candidate));
    assert!(!p.below_floors);
    let r1_subjects: Vec<_> = sweep
        .findings
        .iter()
        .filter(|f| f.subject.root_path == "/r1")
        .collect();
    assert_eq!(r1_subjects.len(), 1);
}

#[test]
fn released_floors_leave_nothing_below() {
    // With the floors already at 1/1 the released walk rediscovers the
    // same subjects, every extra filters as equal, and the small subject
    // is an ordinary (untagged) finding.
    let (sources, roots) = floor_split_fixture();
    let sweep = run_structural(&sources, &roots, &low_floors());
    assert_eq!(sweep.stats.below_floor_subjects, 0);
    assert!(!find_finding(&sweep, "/r1", "small").below_floors);
}

#[test]
fn archive_coverage_unions_across_archive_roots() {
    let roots = vec![
        make_root(1, "/r1"),
        make_archive_root(2, "/a1"),
        make_archive_root(3, "/a2"),
    ];
    let sources = vec![
        make_source(1, 1, "s/f1", 100, Some(10)),
        make_source(2, 1, "s/f2", 100, Some(20)),
        make_source(3, 1, "noise/u", 100, Some(90)),
        make_source(4, 2, "kept/f1", 100, Some(10)),
        make_source(5, 3, "kept/f2", 100, Some(20)),
    ];
    let sweep = run_structural(&sources, &roots, &low_floors());
    let f = find_finding(&sweep, "/r1", "s");
    assert!((f.archive_cover_pct - 1.0).abs() < 1e-9);
    assert_eq!(f.nature, FindingNature::Dismiss);
}

#[test]
fn nature_consolidate_when_unarchived() {
    let roots = vec![make_root(1, "/r1"), make_root(2, "/r2")];
    let sources = vec![
        make_source(1, 1, "s/f1", 100, Some(10)),
        make_source(2, 1, "noise/u", 100, Some(90)),
        make_source(3, 2, "elsewhere/f1", 100, Some(10)),
    ];
    let sweep = run_structural(&sources, &roots, &low_floors());
    let f = find_finding(&sweep, "/r1", "s");
    assert!((f.archive_cover_pct).abs() < 1e-9);
    assert_eq!(f.nature, FindingNature::Consolidate);
}

#[test]
fn nature_verify_outranks_dismiss() {
    let mut archive = make_archive_root(2, "/a1");
    archive.suspended = true;
    archive.last_scanned_at = Some(1_000);
    let roots = vec![make_root(1, "/r1"), archive];
    let sources = vec![
        make_source(1, 1, "s/f1", 100, Some(10)),
        make_source(2, 1, "noise/u", 100, Some(90)),
        make_source(3, 2, "kept/f1", 100, Some(10)),
    ];
    let sweep = run_structural(&sources, &roots, &low_floors());
    let f = find_finding(&sweep, "/r1", "s");
    assert!((f.archive_cover_pct - 1.0).abs() < 1e-9);
    assert_eq!(f.nature, FindingNature::Verify);
    match &f.shape {
        RelationShape::Pair {
            counterpart_suspended,
            counterpart_is_archive,
            counterpart_last_scanned_at,
            ..
        } => {
            assert!(counterpart_suspended);
            assert!(counterpart_is_archive);
            assert_eq!(*counterpart_last_scanned_at, Some(1_000));
        }
        RelationShape::Coverage { .. } => panic!("expected a pair statement"),
    }
}

#[test]
fn suspension_flags_on_both_sides() {
    let mut r1 = make_root(1, "/r1");
    r1.suspended = true;
    r1.last_scanned_at = Some(500);
    let mut r2 = make_root(2, "/r2");
    r2.suspended = true;
    let roots = vec![r1, r2];
    let sources = vec![
        make_source(1, 1, "m/f1", 100, Some(10)),
        make_source(2, 1, "u1/x", 50, Some(91)),
        make_source(3, 2, "n/f1", 100, Some(10)),
        make_source(4, 2, "u2/y", 50, Some(92)),
    ];
    let sweep = run_structural(&sources, &roots, &low_floors());
    let f = find_finding(&sweep, "/r1", "m");
    assert!(f.subject_suspended);
    assert_eq!(f.subject_last_scanned_at, Some(500));
    assert!(matches!(
        f.shape,
        RelationShape::Pair {
            counterpart_suspended: true,
            ..
        }
    ));
}

#[test]
fn scale_synthetic_star_scatter_and_siblings() {
    let (sources, roots) = scale_fixture();
    assert!(sources.len() > 7_000);
    let sweep = run_structural(&sources, &roots, &low_floors());

    // The star: exactly 30 subjects under mb/, each a pair into r2's hub.
    let star: Vec<_> = sweep
        .findings
        .iter()
        .filter(|f| f.subject.root_path == "/r1" && f.subject.rel_prefix.starts_with("mb/"))
        .collect();
    assert_eq!(star.len(), 30);
    for f in &star {
        match &f.shape {
            RelationShape::Pair { counterpart, .. } => {
                assert_eq!(counterpart.root_path, "/r2");
                assert_eq!(counterpart.rel_prefix, "hub");
            }
            RelationShape::Coverage { .. } => panic!("star member lost its hub"),
        }
    }

    // The scattered subject degrades to coverage over two roots.
    let scatter = find_finding(&sweep, "/r1", "scatter");
    assert_eq!(
        scatter.shape,
        RelationShape::Coverage {
            locations: 2,
            archived_locations: 0
        }
    );

    // The intra-root mirror dedups to the canonical sibling.
    assert!(sweep
        .findings
        .iter()
        .any(|f| f.subject.root_path == "/r3" && f.subject.rel_prefix == "Documents"));
    assert!(!sweep
        .findings
        .iter()
        .any(|f| f.subject.root_path == "/r3" && f.subject.rel_prefix == "Documents kopie"));
}

#[test]
fn determinism_run_twice_identical() {
    let (sources, roots) = scale_fixture();
    let a = run_structural(&sources, &roots, &low_floors());
    let b = run_structural(&sources, &roots, &low_floors());
    assert_eq!(a, b);
}
