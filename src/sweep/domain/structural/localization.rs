//! Stage D — resolving each raw subject's relation shape: a pair story when
//! one counterpart scope concentrates the match, an honest coverage
//! statement when it is scattered. The concentration walk and its reciprocal-
//! mirror dedup are the one genuinely dense stage in the pipeline.

use std::collections::HashMap;

use crate::core::domain::folder_tree::FolderTree;
use crate::domain::root::Root;

use super::discovery::RawSubject;
use super::universe::{subtree_sums, SweepParams, Universe};

/// A place in the universe: a root and a folder within it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Location {
    pub root_id: i64,
    pub root_path: String,
    /// Folder within the root; `""` is the root itself.
    pub rel_prefix: String,
}

/// How a pair statement reads from the subject's side. The subject is always
/// the contained side; the percentages carry the strength.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RelationClass {
    /// Both sides are essentially the match (each at or above the lifting
    /// tolerance).
    Mirror,
    /// The subject sits (partially) inside a counterpart that holds more.
    Subset,
}

/// The finding's relation: a pair story when one counterpart scope
/// concentrates the match, an honest coverage statement when it is scattered.
#[derive(Debug, PartialEq)]
pub enum RelationShape {
    Pair {
        counterpart: Location,
        class: RelationClass,
        /// Fraction of the subject's comparison-participating weight with a
        /// copy under the counterpart scope.
        pair_size_pct: f64,
        pair_count_pct: f64,
        /// Fraction of the counterpart scope that is the match — how much of
        /// that place this relation accounts for.
        counterpart_share_pct: f64,
        counterpart_suspended: bool,
        counterpart_is_archive: bool,
        counterpart_last_scanned_at: Option<i64>,
    },
    Coverage {
        /// Distinct roots holding copies of the subject's matched content.
        locations: usize,
        /// How many of those roots are archive roots.
        archived_locations: usize,
    },
}

/// A secondary counterpart scope for a subject that relates to several
/// places: "also N% inside <location>".
#[derive(Debug, PartialEq)]
pub struct ContextRelation {
    pub location: Location,
    pub size_pct: f64,
}

/// A subject with its relation shape resolved.
pub struct LocalizedSubject {
    pub raw: RawSubject,
    pub shape: RelationShape,
    pub context: Vec<ContextRelation>,
    /// Subject-side weight of matched content that has an outside copy on an
    /// archive root.
    pub archive_matched_bytes: u64,
}

/// Every folder id in the subtree under `fid`, including `fid` itself.
fn subtree_fids(tree: &FolderTree, fid: u32) -> Vec<u32> {
    let mut out = Vec::new();
    let mut stack = vec![fid];
    while let Some(f) = stack.pop() {
        out.push(f);
        stack.extend_from_slice(tree.children(f));
    }
    out
}

/// A counterpart scope candidate produced by one root's concentration walk.
struct ScopeCandidate {
    root_idx: usize,
    fid: u32,
    /// Subject-side weight with a copy under this scope.
    subject_bytes: u64,
    subject_files: u32,
    /// Counterpart-side share: matched weight under the scope over the
    /// scope's own comparison-participating weight.
    counterpart_share: f64,
}

/// Resolve each raw subject's relation shape and context.
///
/// `roots` must be the same slice the universe was built from.
pub fn localize_subjects(
    universe: &Universe,
    roots: &[Root],
    subjects: Vec<RawSubject>,
    params: &SweepParams,
) -> Vec<LocalizedSubject> {
    let mut localized: Vec<LocalizedSubject> = subjects
        .into_iter()
        .map(|raw| localize_one(universe, roots, raw, params))
        .collect();
    dedup_reciprocal_mirrors(universe, roots, &mut localized);
    localized
}

fn localize_one(
    universe: &Universe,
    roots: &[Root],
    raw: RawSubject,
    params: &SweepParams,
) -> LocalizedSubject {
    let rd = &universe.roots_data[raw.root_idx];

    // Subject-side weight per object under the subject.
    let mut subject_objects: HashMap<i64, (u64, u32)> = HashMap::new();
    for fid in subtree_fids(&rd.tree, raw.fid) {
        for &(oid, size) in &rd.files[fid as usize] {
            let cell = subject_objects.entry(oid).or_insert((0, 0));
            cell.0 += size;
            cell.1 += 1;
        }
    }

    // Outside copies of the subject's matched objects, per counterpart root.
    let mut per_root: HashMap<usize, Vec<(u32, u64)>> = HashMap::new();
    let mut matched_objects: Vec<i64> = Vec::new();
    let mut matched_bytes: u64 = 0;
    let mut matched_files: u32 = 0;
    let mut archive_matched_bytes: u64 = 0;
    for (&oid, &(bytes, files)) in &subject_objects {
        let locs = &universe.obj_locs[&oid];
        let mut outside_any = false;
        let mut outside_archived = false;
        for &(ri, fid) in locs {
            let outside = ri != raw.root_idx || !rd.tree.is_ancestor_or_equal(raw.fid, fid);
            if outside {
                per_root
                    .entry(ri)
                    .or_default()
                    .push((fid, universe.obj_size[&oid]));
                outside_any = true;
                if roots[ri].role == "archive" {
                    outside_archived = true;
                }
            }
        }
        if outside_any {
            matched_objects.push(oid);
            matched_bytes += bytes;
            matched_files += files;
            if outside_archived {
                archive_matched_bytes += bytes;
            }
        }
    }
    // The per-object recomputation must agree with the descent's arrays —
    // one union semantics, two derivations.
    debug_assert_eq!(matched_bytes, raw.matched_bytes);
    debug_assert_eq!(matched_files, raw.matched_files);

    // One concentration walk per counterpart root, in deterministic order.
    let mut involved: Vec<usize> = per_root.keys().copied().collect();
    involved.sort_unstable();
    let mut scopes: Vec<ScopeCandidate> = Vec::new();
    for ri in involved.iter().copied() {
        let cd = &universe.roots_data[ri];
        let mut leaf: Vec<(u64, u32)> = vec![(0, 0); cd.tree.len()];
        for &(fid, size) in &per_root[&ri] {
            let cell = &mut leaf[fid as usize];
            cell.0 += size;
            cell.1 += 1;
        }
        let m = subtree_sums(&cd.tree, &leaf);
        // Folder id 0 is the root folder: interning always creates it first.
        let total = m[0].0;
        if total == 0 {
            continue;
        }
        let mut cur: u32 = 0;
        loop {
            let next = cd.tree.children(cur).iter().copied().find(|&ch| {
                m[ch as usize].0 as f64 >= params.concentration_threshold * total as f64
            });
            match next {
                Some(ch) => cur = ch,
                None => break,
            }
        }
        // A counterpart must be disjoint from the subject: a walk that
        // settles on the subject's own ancestor found no localized scope in
        // this root — the root still counts as coverage.
        if ri == raw.root_idx && cd.tree.is_ancestor_or_equal(cur, raw.fid) {
            continue;
        }
        // Subject-side weight with a copy under the settled scope.
        let mut subject_bytes: u64 = 0;
        let mut subject_files: u32 = 0;
        for &oid in &matched_objects {
            let under = universe.obj_locs[&oid]
                .iter()
                .any(|&(lri, lfid)| lri == ri && cd.tree.is_ancestor_or_equal(cur, lfid));
            if under {
                let (bytes, files) = subject_objects[&oid];
                subject_bytes += bytes;
                subject_files += files;
            }
        }
        let scope_hashed = cd.sub_hashed[cur as usize].0;
        scopes.push(ScopeCandidate {
            root_idx: ri,
            fid: cur,
            subject_bytes,
            subject_files,
            counterpart_share: if scope_hashed > 0 {
                m[cur as usize].0 as f64 / scope_hashed as f64
            } else {
                0.0
            },
        });
    }

    // The best scope carries the pair statement iff it concentrates the
    // match; otherwise the honest statement is coverage-shaped.
    scopes.sort_by(|a, b| {
        b.subject_bytes.cmp(&a.subject_bytes).then_with(|| {
            let pa = (&roots[a.root_idx].path, a.fid);
            let pb = (&roots[b.root_idx].path, b.fid);
            pa.cmp(&pb)
        })
    });
    let location = |sc: &ScopeCandidate| {
        let root = &roots[sc.root_idx];
        Location {
            root_id: root.id,
            root_path: root.path.clone(),
            rel_prefix: universe.roots_data[sc.root_idx]
                .tree
                .path(sc.fid)
                .to_string(),
        }
    };
    let concentrated = |sc: &ScopeCandidate| {
        matched_bytes > 0
            && sc.subject_bytes as f64 >= params.concentration_threshold * matched_bytes as f64
    };
    let (shape, context_from) = match scopes.first() {
        Some(best) if concentrated(best) => {
            let root = &roots[best.root_idx];
            let pair_size_pct = if raw.total_bytes > 0 {
                best.subject_bytes as f64 / raw.total_bytes as f64
            } else {
                0.0
            };
            let pair_count_pct = if raw.total_files > 0 {
                f64::from(best.subject_files) / f64::from(raw.total_files)
            } else {
                0.0
            };
            let class = if pair_size_pct >= params.lifting_tolerance
                && best.counterpart_share >= params.lifting_tolerance
            {
                RelationClass::Mirror
            } else {
                RelationClass::Subset
            };
            (
                RelationShape::Pair {
                    counterpart: location(best),
                    class,
                    pair_size_pct,
                    pair_count_pct,
                    counterpart_share_pct: best.counterpart_share,
                    counterpart_suspended: root.suspended,
                    counterpart_is_archive: root.role == "archive",
                    counterpart_last_scanned_at: root.last_scanned_at,
                },
                1,
            )
        }
        _ => (
            RelationShape::Coverage {
                locations: involved.len(),
                archived_locations: involved
                    .iter()
                    .filter(|&&ri| roots[ri].role == "archive")
                    .count(),
            },
            0,
        ),
    };
    let context: Vec<ContextRelation> = scopes
        .iter()
        .skip(context_from)
        .map(|sc| ContextRelation {
            location: location(sc),
            size_pct: if raw.total_bytes > 0 {
                sc.subject_bytes as f64 / raw.total_bytes as f64
            } else {
                0.0
            },
        })
        .collect();

    LocalizedSubject {
        raw,
        shape,
        context,
        archive_matched_bytes,
    }
}

/// Two mirror findings that are each other's reciprocal are one statement:
/// keep the canonical side (root path, then subject path). Reciprocal
/// subset findings are deliberately kept — each is an honest, distinct
/// statement about its own subject.
fn dedup_reciprocal_mirrors(
    universe: &Universe,
    roots: &[Root],
    localized: &mut Vec<LocalizedSubject>,
) {
    let subject_loc = |ls: &LocalizedSubject| {
        (
            roots[ls.raw.root_idx].id,
            universe.roots_data[ls.raw.root_idx]
                .tree
                .path(ls.raw.fid)
                .to_string(),
        )
    };
    let mirror_counterpart = |ls: &LocalizedSubject| match &ls.shape {
        RelationShape::Pair {
            counterpart,
            class: RelationClass::Mirror,
            ..
        } => Some((counterpart.root_id, counterpart.rel_prefix.clone())),
        _ => None,
    };
    let by_subject: HashMap<(i64, String), (i64, String)> = localized
        .iter()
        .filter_map(|ls| mirror_counterpart(ls).map(|cp| (subject_loc(ls), cp)))
        .collect();
    localized.retain(|ls| {
        let Some(cp) = mirror_counterpart(ls) else {
            return true;
        };
        let subject = subject_loc(ls);
        // Reciprocal iff the counterpart is itself a mirror subject pointing
        // back; the canonically smaller subject carries the statement.
        let reciprocal = by_subject.get(&cp).is_some_and(|back| *back == subject);
        !reciprocal || subject_key(roots, &subject) <= subject_key(roots, &cp)
    });
}

/// Canonical ordering key for a (root id, rel prefix) subject location.
fn subject_key<'a>(roots: &'a [Root], loc: &'a (i64, String)) -> (&'a str, &'a str) {
    let root_path = roots
        .iter()
        .find(|r| r.id == loc.0)
        .map(|r| r.path.as_str())
        .unwrap_or("");
    (root_path, loc.1.as_str())
}
