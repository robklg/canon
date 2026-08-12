//! Fixtures shared across the sweep domain test modules — genuinely
//! cross-cutting helpers only; a helper used by a single test file lives
//! there instead.

use crate::domain::root::Root;
use crate::domain::source::Source;
use crate::sweep::domain::structural::{
    build_universe, compute_structural, FindingNature, FindingTier, LocalizedSubject, Location,
    RelationShape, StructuralFinding, StructuralSweep, SweepParams, Universe,
};
use crate::sweep::domain::structural::{compute_matched, discover_subjects, localize_subjects};

pub(super) fn make_root(id: i64, path: &str) -> Root {
    Root {
        id,
        path: path.to_string(),
        role: "source".to_string(),
        comment: None,
        last_scanned_at: None,
        suspended: false,
    }
}

pub(super) fn make_archive_root(id: i64, path: &str) -> Root {
    Root {
        role: "archive".to_string(),
        ..make_root(id, path)
    }
}

pub(super) fn make_source(
    id: i64,
    root_id: i64,
    rel_path: &str,
    size: i64,
    object_id: Option<i64>,
) -> Source {
    Source {
        id,
        root_id,
        root_path: format!("/root{root_id}"),
        rel_path: rel_path.to_string(),
        object_id,
        size,
        mtime: 0,
        excluded: false,
        object_excluded: None,
        device: 0,
        inode: 0,
        partial_hash: String::new(),
        basis_rev: 0,
        root_role: "source".to_string(),
        root_suspended: false,
        decision_id: None,
    }
}

pub(super) fn universe(sources: &[Source], roots: &[Root], params: &SweepParams) -> Universe {
    let refs: Vec<&Source> = sources.iter().collect();
    build_universe(&refs, roots, params)
}

pub(super) fn low_floors() -> SweepParams {
    SweepParams {
        emit_floor_bytes: 1,
        emit_floor_files: 1,
        ..SweepParams::default()
    }
}

pub(super) fn run_localize(
    sources: &[Source],
    roots: &[Root],
    params: &SweepParams,
) -> (Universe, Vec<LocalizedSubject>) {
    let u = universe(sources, roots, params);
    let matched = compute_matched(&u);
    let subjects = discover_subjects(&u, &matched, params);
    let localized = localize_subjects(&u, roots, subjects, params);
    (u, localized)
}

pub(super) fn run_structural(
    sources: &[Source],
    roots: &[Root],
    params: &SweepParams,
) -> StructuralSweep {
    let refs: Vec<&Source> = sources.iter().collect();
    compute_structural(&refs, roots, params)
}

/// A moderate-scale synthetic universe: a 30-subject star, a scattered
/// subject, intra-root siblings, and unique noise.
pub(super) fn scale_fixture() -> (Vec<Source>, Vec<Root>) {
    let roots = vec![
        make_root(1, "/r1"),
        make_root(2, "/r2"),
        make_root(3, "/r3"),
        make_root(4, "/r4"),
    ];
    let mut sources = Vec::new();
    let mut id = 0i64;
    let mut next = |root_id: i64, rel: String, size: i64, oid: Option<i64>| {
        id += 1;
        Source {
            id,
            root_id,
            root_path: format!("/root{root_id}"),
            rel_path: rel,
            object_id: oid,
            size,
            mtime: 0,
            excluded: false,
            object_excluded: None,
            device: 0,
            inode: 0,
            partial_hash: String::new(),
            basis_rev: 0,
            root_role: "source".to_string(),
            root_suspended: false,
            decision_id: None,
        }
    };
    // The star: 30 subjects in r1, all pointing into r2's hub.
    for i in 0..30i64 {
        for j in 0..100i64 {
            let oid = 100_000 + i * 100 + j;
            sources.push(next(1, format!("mb/{i:02}/f{j}"), 10, Some(oid)));
            sources.push(next(2, format!("hub/f{i}_{j}"), 10, Some(oid)));
        }
    }
    // Unique noise inside mb keeps the star from lifting to one subject.
    for k in 0..50i64 {
        sources.push(next(1, format!("mb/u{k}"), 100, Some(200_000 + k)));
    }
    // The scattered subject: half its copies in r3, half in r4.
    for i in 0..10i64 {
        let oid = 300_000 + i;
        sources.push(next(1, format!("scatter/f{i}"), 10, Some(oid)));
        let other = if i % 2 == 0 { 3 } else { 4 };
        sources.push(next(other, format!("k/f{i}"), 10, Some(oid)));
    }
    // Intra-root siblings in r3.
    for i in 0..500i64 {
        let oid = 400_000 + i;
        sources.push(next(3, format!("Documents/g{i}"), 10, Some(oid)));
        sources.push(next(3, format!("Documents kopie/g{i}"), 10, Some(oid)));
    }
    (sources, roots)
}

pub(super) fn lens_loc(root_path: &str, rel: &str) -> Location {
    Location {
        root_id: 1,
        root_path: root_path.to_string(),
        rel_prefix: rel.to_string(),
    }
}

pub(super) fn lens_finding(
    rel: &str,
    tier: FindingTier,
    gain_bytes: u64,
    residual_bytes: u64,
    nature: FindingNature,
    shape: RelationShape,
) -> StructuralFinding {
    StructuralFinding {
        subject: lens_loc("/r1", rel),
        subject_suspended: false,
        subject_is_archive: false,
        subject_last_scanned_at: None,
        tier,
        below_floors: false,
        shape,
        context: Vec::new(),
        containment_size_pct: 0.96,
        containment_count_pct: 0.9,
        gain_bytes,
        gain_files: 10,
        residual_bytes,
        residual_files: 1,
        archive_cover_pct: 0.0,
        hash_coverage_pct: 1.0,
        nature,
    }
}
