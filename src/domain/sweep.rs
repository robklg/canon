//! Sweep structural computation — lens-free reduction-opportunity findings.
//!
//! The engine behind `canon sweep`: from a policy-filtered source slice it
//! computes, per folder, how much of that folder's content exists outside it —
//! anywhere in the universe, another root or elsewhere in the same root — and
//! emits maximal subjects with their relations. Everything here is pure
//! aggregation over interned per-root folder trees; ranking and presentation
//! belong to the lens derivation, not this module.
//!
//! Callers apply inclusion policy before building (presence, exclusion,
//! zero-byte, suspension participation) — the engine indexes what it is given.

use std::collections::{HashMap, HashSet};

use crate::domain::root::Root;
use crate::domain::source::Source;

/// The named calibratable parameters of the sweep computation.
///
/// Defaults carry the analysis prototype's constants; calibration against the
/// real archive may adjust them.
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
}

/// Per-root folder tree with interned folder ids. Folder `""` is the root
/// folder itself. Ids are created parents-first, so `parent(fid) < fid`
/// always holds — bottom-up aggregation is a reverse walk over ids.
pub struct FolderTree {
    ids: HashMap<String, u32>,
    paths: Vec<String>,
    parent: Vec<Option<u32>>,
    depth: Vec<u32>,
    children: Vec<Vec<u32>>,
}

impl FolderTree {
    pub fn new() -> Self {
        Self {
            ids: HashMap::new(),
            paths: Vec::new(),
            parent: Vec::new(),
            depth: Vec::new(),
            children: Vec::new(),
        }
    }

    /// Intern a folder path (and any missing ancestors), returning its id.
    pub fn intern(&mut self, folder: &str) -> u32 {
        if let Some(&fid) = self.ids.get(folder) {
            return fid;
        }
        // Walk up to the nearest interned ancestor, collecting the missing
        // chain, then create it shallowest-first so parents precede children.
        let mut chain: Vec<String> = Vec::new();
        let mut cur = folder.to_string();
        let mut anchor: Option<u32> = None;
        loop {
            chain.push(cur.clone());
            if cur.is_empty() {
                break;
            }
            let head = match cur.rsplit_once('/') {
                Some((head, _)) => head.to_string(),
                None => String::new(),
            };
            if let Some(&fid) = self.ids.get(&head) {
                anchor = Some(fid);
                break;
            }
            cur = head;
        }
        let mut parent = anchor;
        for path in chain.into_iter().rev() {
            let fid = self.paths.len() as u32;
            self.ids.insert(path.clone(), fid);
            self.paths.push(path);
            self.parent.push(parent);
            self.depth.push(match parent {
                Some(p) => self.depth[p as usize] + 1,
                None => 0,
            });
            self.children.push(Vec::new());
            if let Some(p) = parent {
                self.children[p as usize].push(fid);
            }
            parent = Some(fid);
        }
        parent.expect("intern creates at least one folder")
    }

    pub fn len(&self) -> usize {
        self.paths.len()
    }

    pub fn is_empty(&self) -> bool {
        self.paths.is_empty()
    }

    pub fn path(&self, fid: u32) -> &str {
        &self.paths[fid as usize]
    }

    pub fn parent(&self, fid: u32) -> Option<u32> {
        self.parent[fid as usize]
    }

    pub fn children(&self, fid: u32) -> &[u32] {
        &self.children[fid as usize]
    }

    /// Whether `anc` is `desc` or one of its ancestors.
    pub fn is_ancestor_or_equal(&self, anc: u32, desc: u32) -> bool {
        let mut cur = Some(desc);
        while let Some(f) = cur {
            if f == anc {
                return true;
            }
            cur = self.parent[f as usize];
        }
        false
    }

    /// Lowest common ancestor of two folders.
    pub fn lca(&self, a: u32, b: u32) -> u32 {
        let (mut a, mut b) = (a, b);
        while self.depth[a as usize] > self.depth[b as usize] {
            a = self.parent[a as usize].expect("deeper folder has a parent");
        }
        while self.depth[b as usize] > self.depth[a as usize] {
            b = self.parent[b as usize].expect("deeper folder has a parent");
        }
        while a != b {
            a = self.parent[a as usize].expect("distinct folders share a root ancestor");
            b = self.parent[b as usize].expect("distinct folders share a root ancestor");
        }
        a
    }
}

impl Default for FolderTree {
    fn default() -> Self {
        Self::new()
    }
}

/// One root's folders and content, indexed by folder id.
pub struct RootData {
    /// Index into the roots slice the universe was built from.
    pub root_idx: usize,
    pub tree: FolderTree,
    /// Direct (non-recursive) comparison-participating files per folder:
    /// (object id, size).
    pub files: Vec<Vec<(i64, u64)>>,
    /// Direct totals per folder over all content, hashed or not.
    pub leaf_all: Vec<(u64, u32)>,
    /// Direct totals per folder over comparison-participating content.
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

fn subtree_sums(tree: &FolderTree, leaf: &[(u64, u32)]) -> Vec<(u64, u32)> {
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
        },
    }
}

/// How settled a finding is: at or above the lifting tolerance, or the
/// consolidation-candidate tier below it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FindingTier {
    Clean,
    Candidate,
}

/// A maximal subject emitted by the descent, before localization.
pub struct RawSubject {
    pub root_idx: usize,
    pub fid: u32,
    pub tier: FindingTier,
    /// Weight of the subject's content that exists outside the subject.
    pub matched_bytes: u64,
    pub matched_files: u32,
    /// The subject's comparison-participating totals.
    pub total_bytes: u64,
    pub total_files: u32,
}

/// Per-root, per-folder matched weight, in two forms.
struct MatchedWeights {
    /// Subtree weight of content that exists outside that folder — anywhere:
    /// another root, or the same root outside the subtree. NOT monotonic down
    /// the tree: the intra-root subtraction can leave a parent with less
    /// matched weight than its children.
    matched: Vec<Vec<(u64, u32)>>,
    /// Pre-subtraction subtree weight: a monotonic upper bound on `matched`,
    /// safe for trimming whole subtrees during the descent.
    upper: Vec<Vec<(u64, u32)>>,
}

/// Compute matched weights per root.
///
/// An object duplicated only within one root is matched everywhere except on
/// the path from its copies' common ancestor up to the root: those are
/// exactly the subtrees containing every copy, where nothing exists outside.
/// The same subtraction makes an object with all copies in one folder match
/// nowhere. Cross-root objects match at every local node unconditionally.
fn compute_matched(universe: &Universe) -> MatchedWeights {
    let mut leaf: Vec<Vec<(u64, u32)>> = universe
        .roots_data
        .iter()
        .map(|rd| vec![(0, 0); rd.tree.len()])
        .collect();
    // (lca fid, bytes, files) to remove along the lca→root path, per root.
    let mut subtractions: Vec<Vec<(u32, u64, u32)>> = vec![Vec::new(); universe.roots_data.len()];

    for (oid, locs) in &universe.obj_locs {
        let size = universe.obj_size[oid];
        let cross_root = locs.iter().any(|&(ri, _)| ri != locs[0].0);
        for &(ri, fid) in locs {
            let cell = &mut leaf[ri][fid as usize];
            cell.0 += size;
            cell.1 += 1;
        }
        if !cross_root {
            let ri = locs[0].0;
            let tree = &universe.roots_data[ri].tree;
            let lca = locs
                .iter()
                .map(|&(_, fid)| fid)
                .reduce(|a, b| tree.lca(a, b))
                .expect("object has at least one copy");
            let copies = locs.len() as u32;
            subtractions[ri].push((lca, size * u64::from(copies), copies));
        }
    }

    let upper: Vec<Vec<(u64, u32)>> = universe
        .roots_data
        .iter()
        .zip(&leaf)
        .map(|(rd, l)| subtree_sums(&rd.tree, l))
        .collect();
    let mut matched = upper.clone();
    for (ri, subs) in subtractions.iter().enumerate() {
        let tree = &universe.roots_data[ri].tree;
        for &(lca, bytes, files) in subs {
            let mut cur = Some(lca);
            while let Some(fid) = cur {
                let cell = &mut matched[ri][fid as usize];
                cell.0 -= bytes;
                cell.1 -= files;
                cur = tree.parent(fid);
            }
        }
    }
    MatchedWeights { matched, upper }
}

/// Walk each root's tree and emit maximal subjects.
///
/// Emission at or above the lifting tolerance stops the descent, so a clean
/// parent dominates its children by construction; a mixed folder whose
/// children emit nothing still emits candidate-tier at or above the
/// candidate threshold.
fn discover_subjects(
    universe: &Universe,
    weights: &MatchedWeights,
    params: &SweepParams,
) -> Vec<RawSubject> {
    let mut subjects: Vec<RawSubject> = Vec::new();
    for rd in &universe.roots_data {
        if rd.tree.is_empty() {
            continue;
        }
        walk(
            rd,
            &weights.matched[rd.root_idx],
            &weights.upper[rd.root_idx],
            params,
            0,
            &mut subjects,
        );
    }
    subjects
}

fn walk(
    rd: &RootData,
    matched: &[(u64, u32)],
    upper: &[(u64, u32)],
    params: &SweepParams,
    fid: u32,
    subjects: &mut Vec<RawSubject>,
) -> bool {
    // Trim on the monotonic upper bound: if even the pre-subtraction weight
    // is below the floors, no descendant can clear them. The true matched
    // weight cannot gate the descent — a parent can hold less than its
    // children after the intra-root subtraction.
    let (u_bytes, u_files) = upper[fid as usize];
    if u_bytes < params.emit_floor_bytes && u_files < params.emit_floor_files {
        return false;
    }
    let (m_bytes, m_files) = matched[fid as usize];
    let floors_met = m_bytes >= params.emit_floor_bytes || m_files >= params.emit_floor_files;
    let (t_bytes, t_files) = rd.sub_hashed[fid as usize];
    let containment = if t_bytes > 0 {
        m_bytes as f64 / t_bytes as f64
    } else {
        0.0
    };
    let emit = |tier: FindingTier, subjects: &mut Vec<RawSubject>| {
        subjects.push(RawSubject {
            root_idx: rd.root_idx,
            fid,
            tier,
            matched_bytes: m_bytes,
            matched_files: m_files,
            total_bytes: t_bytes,
            total_files: t_files,
        });
    };
    if floors_met && containment >= params.lifting_tolerance {
        emit(FindingTier::Clean, subjects);
        return true;
    }
    let mut emitted = false;
    for &child in rd.tree.children(fid) {
        if walk(rd, matched, upper, params, child, subjects) {
            emitted = true;
        }
    }
    if !emitted && floors_met && containment >= params.candidate_threshold {
        emit(FindingTier::Candidate, subjects);
        return true;
    }
    emitted
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_root(id: i64, path: &str) -> Root {
        Root {
            id,
            path: path.to_string(),
            role: "source".to_string(),
            comment: None,
            last_scanned_at: None,
            suspended: false,
        }
    }

    fn make_source(
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

    fn universe(sources: &[Source], roots: &[Root], params: &SweepParams) -> Universe {
        let refs: Vec<&Source> = sources.iter().collect();
        build_universe(&refs, roots, params)
    }

    #[test]
    fn intern_is_idempotent_with_correct_parent_chain() {
        let mut tree = FolderTree::new();
        let c = tree.intern("a/b/c");
        assert_eq!(tree.intern("a/b/c"), c);
        assert_eq!(tree.path(c), "a/b/c");
        let b = tree.parent(c).unwrap();
        assert_eq!(tree.path(b), "a/b");
        let a = tree.parent(b).unwrap();
        assert_eq!(tree.path(a), "a");
        let root = tree.parent(a).unwrap();
        assert_eq!(tree.path(root), "");
        assert_eq!(tree.parent(root), None);
        assert_eq!(tree.len(), 4);
    }

    #[test]
    fn parent_id_always_precedes_child_id() {
        let mut tree = FolderTree::new();
        tree.intern("x/y/z");
        tree.intern("a");
        tree.intern("x/w");
        for fid in 0..tree.len() as u32 {
            if let Some(p) = tree.parent(fid) {
                assert!(p < fid, "parent {p} must precede child {fid}");
            }
        }
    }

    #[test]
    fn deep_path_interns_iteratively() {
        let deep = (0..60)
            .map(|i| format!("d{i}"))
            .collect::<Vec<_>>()
            .join("/");
        let mut tree = FolderTree::new();
        let fid = tree.intern(&deep);
        assert_eq!(tree.len(), 61); // 60 segments + the root folder
        assert_eq!(tree.path(fid), deep);
    }

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
        let fid = |p: &str| *rd.tree.ids.get(p).unwrap() as usize;
        assert_eq!(rd.sub_all[fid("")], (65, 4));
        assert_eq!(rd.sub_all[fid("a")], (60, 3));
        assert_eq!(rd.sub_all[fid("a/b")], (20, 1));
        assert_eq!(rd.sub_hashed[fid("")], (35, 3));
        assert_eq!(rd.sub_hashed[fid("a")], (30, 2));
        assert_eq!(rd.sub_hashed[fid("a/c")], (0, 0));
    }

    #[test]
    fn lca_covers_all_relations() {
        let mut tree = FolderTree::new();
        let root = tree.intern("");
        let ab = tree.intern("a/b");
        let ac = tree.intern("a/c");
        let a = tree.intern("a");
        let x = tree.intern("x");
        assert_eq!(tree.lca(ab, ab), ab);
        assert_eq!(tree.lca(a, ab), a);
        assert_eq!(tree.lca(ab, ac), a);
        assert_eq!(tree.lca(ab, x), root);
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
        let fid_a = *rd.tree.ids.get("a").unwrap() as usize;
        assert_eq!(rd.leaf_all[fid_a], (140, 2));
        assert_eq!(rd.leaf_hashed[fid_a], (40, 1));
        assert!(rd.files[fid_a].iter().all(|&(oid, _)| oid != 10));
    }

    fn low_floors() -> SweepParams {
        SweepParams {
            emit_floor_bytes: 1,
            emit_floor_files: 1,
            ..SweepParams::default()
        }
    }

    fn matched_at(u: &Universe, weights: &MatchedWeights, ri: usize, path: &str) -> (u64, u32) {
        let fid = *u.roots_data[ri].tree.ids.get(path).unwrap() as usize;
        weights.matched[ri][fid]
    }

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

}
