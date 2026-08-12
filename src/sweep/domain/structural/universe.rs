//! Stage A — the sweep's working set: per-root folder trees, content totals,
//! and the object-location maps that ground overlap reasoning.

use std::collections::{HashMap, HashSet};

use crate::core::domain::folder_tree::FolderTree;
use crate::domain::root::Root;
use crate::domain::source::Source;

/// The named calibratable parameters of the sweep computation.
///
/// Defaults carry the analysis prototype's constants; calibration against the
/// real archive may adjust them.
#[derive(Clone)]
pub struct SweepParams {
    /// Containment at or above this emits a clean finding and stops the
    /// descent (maximal-subject emission).
    pub lifting_tolerance: f64,
    /// A mixed folder with no emitting children still emits, candidate-tier,
    /// at or above this containment.
    pub candidate_threshold: f64,
    /// A counterpart scope must concentrate at least this fraction of the
    /// matched weight to yield a pair statement; below it, the relation is
    /// coverage-shaped.
    pub concentration_threshold: f64,
    /// Objects present in more than this many distinct (root, folder)
    /// locations are dropped from comparison as ubiquitous.
    pub ubiquity_cap: usize,
    /// A finding must carry at least this many matched bytes...
    pub emit_floor_bytes: u64,
    /// ...or at least this many matched files.
    pub emit_floor_files: u32,
    /// Also localize and assemble the below-floor subjects. They are always
    /// counted into the stats; assembling them is only worth the work when
    /// the caller will show them.
    pub assemble_below_floors: bool,
}

impl Default for SweepParams {
    fn default() -> Self {
        Self {
            lifting_tolerance: 0.95,
            candidate_threshold: 0.60,
            concentration_threshold: 0.90,
            ubiquity_cap: 50,
            emit_floor_bytes: 10_000_000,
            emit_floor_files: 25,
            assemble_below_floors: false,
        }
    }
}

/// What the computation dropped or set aside, for the honesty header.
#[derive(Debug, PartialEq)]
pub struct SweepStats {
    /// Objects dropped as ubiquitous (over the location cap).
    pub ubiquitous_objects_dropped: usize,
    /// Total content weight of the dropped objects (counted once per object).
    pub ubiquitous_bytes_dropped: u64,
    /// Maximal subjects below the emit floors, disjoint from every floored
    /// subject — what the floors keep off the board.
    pub below_floor_subjects: usize,
}

/// One root's folders and content, indexed by folder id.
pub struct RootData {
    /// Index into the roots slice the universe was built from.
    pub root_idx: usize,
    pub tree: FolderTree,
    /// Direct (non-recursive) comparison-participating files per folder:
    /// (object id, size).
    pub files: Vec<Vec<(i64, u64)>>,
    /// Direct totals per folder over all content, hashed or not. The
    /// subtree sums derive from these; kept as the observable intermediate
    /// the accumulation tests assert on.
    #[allow(dead_code)]
    pub leaf_all: Vec<(u64, u32)>,
    /// Direct totals per folder over comparison-participating content.
    #[allow(dead_code)]
    pub leaf_hashed: Vec<(u64, u32)>,
    /// Subtree-aggregated totals over all content.
    pub sub_all: Vec<(u64, u32)>,
    /// Subtree-aggregated totals over comparison-participating content.
    pub sub_hashed: Vec<(u64, u32)>,
}

/// The engine's working set: every root's tree and content, plus the
/// object-location maps that ground overlap reasoning.
///
/// Ubiquity-dropped objects do not participate in comparison at all: they are
/// absent from the object maps and the hashed totals, but remain in the
/// all-content totals — so the comparison-coverage qualifier (hashed vs. all)
/// honestly reports them as unverified rather than silently omitting them.
pub struct Universe {
    pub roots_data: Vec<RootData>,
    /// Content size per participating object.
    pub obj_size: HashMap<i64, u64>,
    /// Every copy of each participating object as (root index, folder id).
    pub obj_locs: HashMap<i64, Vec<(usize, u32)>>,
    pub stats: SweepStats,
}

/// The folder holding a source, relative to its root (`""` = the root itself).
fn parent_folder(rel_path: &str) -> &str {
    match rel_path.rsplit_once('/') {
        Some((head, _)) => head,
        None => "",
    }
}

pub(super) fn subtree_sums(tree: &FolderTree, leaf: &[(u64, u32)]) -> Vec<(u64, u32)> {
    let mut sums = leaf.to_vec();
    // Ids are created parents-first, so a reverse walk folds every folder
    // into its parent after its own children have been folded into it.
    for fid in (0..sums.len()).rev() {
        if let Some(p) = tree.parent(fid as u32) {
            let (bytes, files) = sums[fid];
            let parent = &mut sums[p as usize];
            parent.0 += bytes;
            parent.1 += files;
        }
    }
    sums
}

/// Build the working set from a policy-filtered source slice.
///
/// Every source must belong to one of `roots`; sources are attributed to the
/// folder containing them.
pub fn build_universe(sources: &[&Source], roots: &[Root], params: &SweepParams) -> Universe {
    let root_index: HashMap<i64, usize> =
        roots.iter().enumerate().map(|(i, r)| (r.id, i)).collect();

    // Intern folders and collect per-source entries; fill object maps.
    let mut trees: Vec<FolderTree> = (0..roots.len()).map(|_| FolderTree::new()).collect();
    let mut entries: Vec<Vec<(u32, u64, Option<i64>)>> = vec![Vec::new(); roots.len()];
    let mut obj_size: HashMap<i64, u64> = HashMap::new();
    let mut obj_locs: HashMap<i64, Vec<(usize, u32)>> = HashMap::new();
    for s in sources {
        let Some(&ri) = root_index.get(&s.root_id) else {
            debug_assert!(false, "source {} belongs to no supplied root", s.id);
            continue;
        };
        let fid = trees[ri].intern(parent_folder(&s.rel_path));
        let size = s.size.max(0) as u64;
        entries[ri].push((fid, size, s.object_id));
        if let Some(oid) = s.object_id {
            obj_size.insert(oid, size);
            obj_locs.entry(oid).or_default().push((ri, fid));
        }
    }

    // Ubiquity cap: drop objects present in too many distinct locations.
    let mut dropped: HashSet<i64> = HashSet::new();
    let mut ubiquitous_bytes_dropped: u64 = 0;
    for (&oid, locs) in &obj_locs {
        let distinct: HashSet<&(usize, u32)> = locs.iter().collect();
        if distinct.len() > params.ubiquity_cap {
            dropped.insert(oid);
            ubiquitous_bytes_dropped += obj_size[&oid];
        }
    }
    for oid in &dropped {
        obj_locs.remove(oid);
        obj_size.remove(oid);
    }

    // Materialize per-root data; dropped objects stay in the all-content
    // totals only.
    let mut roots_data: Vec<RootData> = Vec::with_capacity(roots.len());
    for (ri, tree) in trees.into_iter().enumerate() {
        let n = tree.len();
        let mut files: Vec<Vec<(i64, u64)>> = vec![Vec::new(); n];
        let mut leaf_all: Vec<(u64, u32)> = vec![(0, 0); n];
        let mut leaf_hashed: Vec<(u64, u32)> = vec![(0, 0); n];
        for &(fid, size, obj) in &entries[ri] {
            let f = fid as usize;
            leaf_all[f].0 += size;
            leaf_all[f].1 += 1;
            if let Some(oid) = obj {
                if !dropped.contains(&oid) {
                    leaf_hashed[f].0 += size;
                    leaf_hashed[f].1 += 1;
                    files[f].push((oid, size));
                }
            }
        }
        let sub_all = subtree_sums(&tree, &leaf_all);
        let sub_hashed = subtree_sums(&tree, &leaf_hashed);
        roots_data.push(RootData {
            root_idx: ri,
            tree,
            files,
            leaf_all,
            leaf_hashed,
            sub_all,
            sub_hashed,
        });
    }

    Universe {
        roots_data,
        obj_size,
        obj_locs,
        stats: SweepStats {
            ubiquitous_objects_dropped: dropped.len(),
            ubiquitous_bytes_dropped,
            below_floor_subjects: 0,
        },
    }
}
