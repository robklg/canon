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

use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};

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
            below_floor_subjects: 0,
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
    /// Discovered only by the floor-released walk (below the emit floors).
    pub below_floors: bool,
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
            below_floors: false,
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
fn localize_subjects(
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

/// The finding's natural next move, derived from structural facts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FindingNature {
    /// The counterpart lives on a suspended root: nothing reads as safe
    /// until that root is reconnected and re-verified.
    Verify,
    /// The subject's content is essentially archive-covered.
    Dismiss,
    /// Copies exist, but the archive doesn't hold them yet.
    Consolidate,
}

/// One reduction opportunity, as lens-free data. Ranking and presentation
/// derive from these fields; nothing here presumes the triage lens.
#[derive(Debug, PartialEq)]
pub struct StructuralFinding {
    pub subject: Location,
    pub subject_suspended: bool,
    /// The subject stands on an archive root — under the triage lens its
    /// content is already resolved; surfaces honestly marked.
    pub subject_is_archive: bool,
    pub subject_last_scanned_at: Option<i64>,
    pub tier: FindingTier,
    /// True when this finding sits below the emit floors — discovered by the
    /// floor-released walk, shown only when the caller asks for everything.
    pub below_floors: bool,
    pub shape: RelationShape,
    pub context: Vec<ContextRelation>,
    /// Fraction of the subject's comparison-participating weight existing
    /// outside the subject (the union containment).
    pub containment_size_pct: f64,
    pub containment_count_pct: f64,
    /// Resolution gain: what acting on this finding resolves.
    pub gain_bytes: u64,
    pub gain_files: u32,
    /// Content existing nowhere else (over comparison-participating weight).
    pub residual_bytes: u64,
    pub residual_files: u32,
    /// Fraction of the subject's comparison-participating weight with an
    /// outside copy on an archive root.
    pub archive_cover_pct: f64,
    /// Comparison-participating weight over all content in the subject —
    /// the honesty qualifier ("compared on N% by size").
    pub hash_coverage_pct: f64,
    pub nature: FindingNature,
}

/// The structural computation's result: every finding, plus the honesty
/// stats for the header.
#[derive(Debug, PartialEq)]
pub struct StructuralSweep {
    pub findings: Vec<StructuralFinding>,
    pub stats: SweepStats,
}

/// Compute every reduction-opportunity finding in the universe.
///
/// `sources` is the policy-filtered slice (presence, exclusion, zero-byte —
/// the caller's rules); `roots` must contain every root the sources belong
/// to. Output order is deterministic: subject root path, then subject path.
pub fn compute_structural(
    sources: &[&Source],
    roots: &[Root],
    params: &SweepParams,
) -> StructuralSweep {
    let mut universe = build_universe(sources, roots, params);
    let weights = compute_matched(&universe);
    let subjects = discover_subjects(&universe, &weights, params);
    // The floors gate discovery (they lift below-floor fragments into an
    // aggregated parent subject), but they must trim output, never
    // existence: a second discovery with the floors released finds the
    // subjects the floors kept off the board. An extra counts only where it
    // is disjoint from every floored subject on its root — anything at,
    // under, or over a floored subject is already that finding's territory.
    let released = SweepParams {
        emit_floor_bytes: 1,
        emit_floor_files: 1,
        ..params.clone()
    };
    let mut extras = discover_subjects(&universe, &weights, &released);
    extras.retain(|e| {
        let tree = &universe.roots_data[e.root_idx].tree;
        !subjects.iter().any(|s| {
            s.root_idx == e.root_idx
                && (tree.is_ancestor_or_equal(s.fid, e.fid)
                    || tree.is_ancestor_or_equal(e.fid, s.fid))
        })
    });
    for e in &mut extras {
        e.below_floors = true;
    }
    universe.stats.below_floor_subjects = extras.len();
    // Reciprocal-mirror dedup runs within each set, so a below-floors
    // finding never displaces an above-floors one. A reciprocal mirror pair
    // spanning the floor boundary can therefore state itself from both ends
    // when everything is assembled — honest, and constructible only in a
    // thin band near the floors.
    let mut localized = localize_subjects(&universe, roots, subjects, params);
    if params.assemble_below_floors {
        localized.extend(localize_subjects(&universe, roots, extras, params));
    }
    let mut findings: Vec<StructuralFinding> = localized
        .into_iter()
        .map(|ls| assemble_finding(&universe, roots, ls, params))
        .collect();
    findings.sort_by(|a, b| {
        (&a.subject.root_path, &a.subject.rel_prefix)
            .cmp(&(&b.subject.root_path, &b.subject.rel_prefix))
    });
    StructuralSweep {
        findings,
        stats: universe.stats,
    }
}

fn assemble_finding(
    universe: &Universe,
    roots: &[Root],
    ls: LocalizedSubject,
    params: &SweepParams,
) -> StructuralFinding {
    let raw = &ls.raw;
    let rd = &universe.roots_data[raw.root_idx];
    let root = &roots[raw.root_idx];
    let (all_bytes, _) = rd.sub_all[raw.fid as usize];
    let archive_cover_pct = if raw.total_bytes > 0 {
        ls.archive_matched_bytes as f64 / raw.total_bytes as f64
    } else {
        0.0
    };
    let counterpart_suspended = matches!(
        ls.shape,
        RelationShape::Pair {
            counterpart_suspended: true,
            ..
        }
    );
    // Verify outranks Dismiss: an archive-covered claim whose keeper sits on
    // a disconnected drive is not actionable until re-verified.
    let nature = if counterpart_suspended {
        FindingNature::Verify
    } else if archive_cover_pct >= params.lifting_tolerance {
        FindingNature::Dismiss
    } else {
        FindingNature::Consolidate
    };
    StructuralFinding {
        subject: Location {
            root_id: root.id,
            root_path: root.path.clone(),
            rel_prefix: rd.tree.path(raw.fid).to_string(),
        },
        subject_suspended: root.suspended,
        subject_is_archive: root.role == "archive",
        subject_last_scanned_at: root.last_scanned_at,
        tier: raw.tier,
        below_floors: raw.below_floors,
        containment_size_pct: if raw.total_bytes > 0 {
            raw.matched_bytes as f64 / raw.total_bytes as f64
        } else {
            0.0
        },
        containment_count_pct: if raw.total_files > 0 {
            f64::from(raw.matched_files) / f64::from(raw.total_files)
        } else {
            0.0
        },
        gain_bytes: raw.matched_bytes,
        gain_files: raw.matched_files,
        residual_bytes: raw.total_bytes - raw.matched_bytes,
        residual_files: raw.total_files - raw.matched_files,
        archive_cover_pct,
        hash_coverage_pct: if all_bytes > 0 {
            raw.total_bytes as f64 / all_bytes as f64
        } else {
            1.0
        },
        nature,
        shape: ls.shape,
        context: ls.context,
    }
}

// ---------------------------------------------------------------------------
// Reduction lens — the v1 ranking derivation over the structural computation.
// A future lens is another function over the same `StructuralSweep`.
// ---------------------------------------------------------------------------

/// The leaderboard as the reduction lens ranks it: findings sharing a
/// counterpart grouped into hubs, everything ordered by the opinionated
/// default — tier, then weight, then keeper safety, then residual burden —
/// with a path tie-break so identical input always ranks identically.
#[derive(Debug, PartialEq)]
pub struct RankedSweep {
    pub entries: Vec<LeaderboardEntry>,
    pub stats: SweepStats,
}

/// One leaderboard slot: a single finding, or a hub of findings that share
/// one counterpart.
#[derive(Debug, PartialEq)]
pub enum LeaderboardEntry {
    Single(StructuralFinding),
    Hub(HubEntry),
}

/// Findings sharing one counterpart, presented as one entry: a star reads
/// as one constellation, not as its points flooding the board.
#[derive(Debug, PartialEq)]
pub struct HubEntry {
    pub counterpart: Location,
    pub counterpart_is_archive: bool,
    pub counterpart_suspended: bool,
    pub counterpart_last_scanned_at: Option<i64>,
    /// Ranked within by the same key that orders the leaderboard.
    pub members: Vec<StructuralFinding>,
    /// Gain sums attribute each subject once — members are distinct
    /// subjects, deduped by the structural computation.
    pub total_gain_bytes: u64,
    pub total_gain_files: u32,
}

/// Rank the structural findings under the reduction lens.
pub fn reduction_lens(sweep: StructuralSweep) -> RankedSweep {
    let mut entries: Vec<LeaderboardEntry> = Vec::new();
    let mut by_counterpart: HashMap<Location, Vec<StructuralFinding>> = HashMap::new();
    for finding in sweep.findings {
        match &finding.shape {
            RelationShape::Pair { counterpart, .. } => by_counterpart
                .entry(counterpart.clone())
                .or_default()
                .push(finding),
            RelationShape::Coverage { .. } => entries.push(LeaderboardEntry::Single(finding)),
        }
    }
    for (counterpart, mut members) in by_counterpart {
        if members.len() < 2 {
            entries.extend(members.into_iter().map(LeaderboardEntry::Single));
            continue;
        }
        members.sort_by(|a, b| {
            rank_key(a)
                .cmp(&rank_key(b))
                .then_with(|| subject_path(a).cmp(&subject_path(b)))
        });
        let status = pair_counterpart_status(&members[0]);
        debug_assert!(
            members.iter().all(|m| pair_counterpart_status(m) == status),
            "hub members share one counterpart, so its status fields agree"
        );
        let (counterpart_is_archive, counterpart_suspended, counterpart_last_scanned_at) = status;
        entries.push(LeaderboardEntry::Hub(HubEntry {
            counterpart,
            counterpart_is_archive,
            counterpart_suspended,
            counterpart_last_scanned_at,
            total_gain_bytes: members.iter().map(|m| m.gain_bytes).sum(),
            total_gain_files: members.iter().map(|m| m.gain_files).sum(),
            members,
        }));
    }
    entries.sort_by(|a, b| {
        entry_key(a)
            .cmp(&entry_key(b))
            .then_with(|| entry_path(a).cmp(&entry_path(b)))
    });
    RankedSweep {
        entries,
        stats: sweep.stats,
    }
}

fn tier_rank(tier: FindingTier) -> u8 {
    match tier {
        FindingTier::Clean => 0,
        FindingTier::Candidate => 1,
    }
}

/// How safely the finding's redundancy claim can be acted on: an archived
/// keeper outranks a merely-present one, which outranks a suspended one;
/// scattered coverage with nothing archived ranks last. A lens constant,
/// derived from lens-free facts — never stored on the finding.
fn keeper_safety(finding: &StructuralFinding) -> u8 {
    match (finding.nature, &finding.shape) {
        (FindingNature::Dismiss, _) => 0,
        (FindingNature::Verify, _) => 2,
        (FindingNature::Consolidate, RelationShape::Pair { .. }) => 1,
        (
            FindingNature::Consolidate,
            RelationShape::Coverage {
                archived_locations, ..
            },
        ) => {
            if *archived_locations > 0 {
                1
            } else {
                3
            }
        }
    }
}

/// The reduction lens's ordering: tier, weight (size-led), keeper safety,
/// residual burden. Lower sorts first.
fn rank_key(finding: &StructuralFinding) -> (u8, Reverse<u64>, u8, u64) {
    (
        tier_rank(finding.tier),
        Reverse(finding.gain_bytes),
        keeper_safety(finding),
        finding.residual_bytes,
    )
}

/// A hub competes as its aggregate: best member tier, summed gain, best
/// member safety, summed residual.
fn entry_key(entry: &LeaderboardEntry) -> (u8, Reverse<u64>, u8, u64) {
    match entry {
        LeaderboardEntry::Single(f) => rank_key(f),
        LeaderboardEntry::Hub(h) => (
            h.members
                .iter()
                .map(|m| tier_rank(m.tier))
                .min()
                .unwrap_or(u8::MAX),
            Reverse(h.total_gain_bytes),
            h.members.iter().map(keeper_safety).min().unwrap_or(u8::MAX),
            h.members.iter().map(|m| m.residual_bytes).sum(),
        ),
    }
}

fn subject_path(finding: &StructuralFinding) -> (&str, &str) {
    (&finding.subject.root_path, &finding.subject.rel_prefix)
}

fn entry_path(entry: &LeaderboardEntry) -> (&str, &str) {
    match entry {
        LeaderboardEntry::Single(f) => subject_path(f),
        LeaderboardEntry::Hub(h) => (&h.counterpart.root_path, &h.counterpart.rel_prefix),
    }
}

fn pair_counterpart_status(finding: &StructuralFinding) -> (bool, bool, Option<i64>) {
    match &finding.shape {
        RelationShape::Pair {
            counterpart_is_archive,
            counterpart_suspended,
            counterpart_last_scanned_at,
            ..
        } => (
            *counterpart_is_archive,
            *counterpart_suspended,
            *counterpart_last_scanned_at,
        ),
        RelationShape::Coverage { .. } => {
            unreachable!("hub members are grouped from pair-shaped findings")
        }
    }
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

    fn make_archive_root(id: i64, path: &str) -> Root {
        Root {
            role: "archive".to_string(),
            ..make_root(id, path)
        }
    }

    fn run_localize(
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

    fn run_structural(sources: &[Source], roots: &[Root], params: &SweepParams) -> StructuralSweep {
        let refs: Vec<&Source> = sources.iter().collect();
        compute_structural(&refs, roots, params)
    }

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

    /// A moderate-scale synthetic universe: a 30-subject star, a scattered
    /// subject, intra-root siblings, and unique noise.
    fn scale_fixture() -> (Vec<Source>, Vec<Root>) {
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

    // --- Reduction lens ---

    fn lens_loc(root_path: &str, rel: &str) -> Location {
        Location {
            root_id: 1,
            root_path: root_path.to_string(),
            rel_prefix: rel.to_string(),
        }
    }

    fn lens_pair(counterpart: Location) -> RelationShape {
        RelationShape::Pair {
            counterpart,
            class: RelationClass::Subset,
            pair_size_pct: 0.96,
            pair_count_pct: 0.9,
            counterpart_share_pct: 0.5,
            counterpart_suspended: false,
            counterpart_is_archive: false,
            counterpart_last_scanned_at: None,
        }
    }

    fn lens_pair_suspended(counterpart: Location) -> RelationShape {
        RelationShape::Pair {
            counterpart,
            class: RelationClass::Subset,
            pair_size_pct: 0.96,
            pair_count_pct: 0.9,
            counterpart_share_pct: 0.5,
            counterpart_suspended: true,
            counterpart_is_archive: false,
            counterpart_last_scanned_at: None,
        }
    }

    fn lens_finding(
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

    fn lens(findings: Vec<StructuralFinding>) -> RankedSweep {
        reduction_lens(StructuralSweep {
            findings,
            stats: SweepStats {
                ubiquitous_objects_dropped: 0,
                ubiquitous_bytes_dropped: 0,
                below_floor_subjects: 0,
            },
        })
    }

    fn entry_labels(ranked: &RankedSweep) -> Vec<String> {
        ranked
            .entries
            .iter()
            .map(|e| match e {
                LeaderboardEntry::Single(f) => f.subject.rel_prefix.clone(),
                LeaderboardEntry::Hub(h) => format!("hub:{}", h.counterpart.rel_prefix),
            })
            .collect()
    }

    #[test]
    fn tier_outranks_weight() {
        use FindingNature::Consolidate;
        let ranked = lens(vec![
            lens_finding(
                "big-candidate",
                FindingTier::Candidate,
                1_000_000,
                0,
                Consolidate,
                lens_pair(lens_loc("/r2", "a")),
            ),
            lens_finding(
                "small-clean",
                FindingTier::Clean,
                1_000,
                0,
                Consolidate,
                lens_pair(lens_loc("/r2", "b")),
            ),
        ]);
        assert_eq!(entry_labels(&ranked), ["small-clean", "big-candidate"]);
    }

    #[test]
    fn weight_orders_within_tier() {
        use FindingNature::Consolidate;
        let ranked = lens(vec![
            lens_finding(
                "lighter",
                FindingTier::Clean,
                5_000,
                0,
                Consolidate,
                lens_pair(lens_loc("/r2", "a")),
            ),
            lens_finding(
                "heavier",
                FindingTier::Clean,
                10_000,
                0,
                Consolidate,
                lens_pair(lens_loc("/r2", "b")),
            ),
        ]);
        assert_eq!(entry_labels(&ranked), ["heavier", "lighter"]);
    }

    #[test]
    fn keeper_safety_orders_equal_weights() {
        // Names are reverse-alphabetical so a path sort would invert the
        // expected order — only keeper safety can produce it.
        let ranked = lens(vec![
            lens_finding(
                "w-scattered",
                FindingTier::Clean,
                1_000,
                0,
                FindingNature::Consolidate,
                RelationShape::Coverage {
                    locations: 4,
                    archived_locations: 0,
                },
            ),
            lens_finding(
                "x-verify",
                FindingTier::Clean,
                1_000,
                0,
                FindingNature::Verify,
                lens_pair_suspended(lens_loc("/r2", "b")),
            ),
            lens_finding(
                "y-present",
                FindingTier::Clean,
                1_000,
                0,
                FindingNature::Consolidate,
                lens_pair(lens_loc("/r2", "c")),
            ),
            lens_finding(
                "z-dismiss",
                FindingTier::Clean,
                1_000,
                0,
                FindingNature::Dismiss,
                lens_pair(lens_loc("/r2", "d")),
            ),
        ]);
        assert_eq!(
            entry_labels(&ranked),
            ["z-dismiss", "y-present", "x-verify", "w-scattered"]
        );
    }

    #[test]
    fn scattered_with_archived_locations_ranks_as_present() {
        let covered = lens_finding(
            "s",
            FindingTier::Clean,
            1_000,
            0,
            FindingNature::Consolidate,
            RelationShape::Coverage {
                locations: 4,
                archived_locations: 2,
            },
        );
        let bare = lens_finding(
            "s",
            FindingTier::Clean,
            1_000,
            0,
            FindingNature::Consolidate,
            RelationShape::Coverage {
                locations: 4,
                archived_locations: 0,
            },
        );
        assert_eq!(keeper_safety(&covered), 1);
        assert_eq!(keeper_safety(&bare), 3);
    }

    #[test]
    fn residual_burden_breaks_safety_ties() {
        use FindingNature::Consolidate;
        let ranked = lens(vec![
            lens_finding(
                "z-clean-dismissal",
                FindingTier::Clean,
                1_000,
                0,
                Consolidate,
                lens_pair(lens_loc("/r2", "a")),
            ),
            lens_finding(
                "a-burdened",
                FindingTier::Clean,
                1_000,
                500,
                Consolidate,
                lens_pair(lens_loc("/r2", "b")),
            ),
        ]);
        assert_eq!(entry_labels(&ranked), ["z-clean-dismissal", "a-burdened"]);
    }

    #[test]
    fn path_breaks_full_ties_regardless_of_input_order() {
        use FindingNature::Consolidate;
        let build = |order_flipped: bool| {
            let mut findings = vec![
                lens_finding(
                    "alpha",
                    FindingTier::Clean,
                    1_000,
                    0,
                    Consolidate,
                    lens_pair(lens_loc("/r2", "a")),
                ),
                lens_finding(
                    "beta",
                    FindingTier::Clean,
                    1_000,
                    0,
                    Consolidate,
                    lens_pair(lens_loc("/r2", "b")),
                ),
            ];
            if order_flipped {
                findings.reverse();
            }
            lens(findings)
        };
        assert_eq!(entry_labels(&build(false)), ["alpha", "beta"]);
        assert_eq!(build(false), build(true));
    }

    #[test]
    fn shared_counterpart_forms_hub_ranked_as_aggregate() {
        use FindingNature::Consolidate;
        let hub_cp = lens_loc("/r2", "hub");
        let ranked = lens(vec![
            lens_finding(
                "a",
                FindingTier::Clean,
                10,
                0,
                Consolidate,
                lens_pair(hub_cp.clone()),
            ),
            lens_finding(
                "b",
                FindingTier::Clean,
                20,
                0,
                Consolidate,
                lens_pair(hub_cp.clone()),
            ),
            lens_finding(
                "solo-under",
                FindingTier::Clean,
                25,
                0,
                Consolidate,
                lens_pair(lens_loc("/r2", "other")),
            ),
            lens_finding(
                "solo-over",
                FindingTier::Clean,
                40,
                0,
                Consolidate,
                lens_pair(lens_loc("/r2", "third")),
            ),
        ]);
        // The hub competes with its 30-byte total: above solo-under (25),
        // below solo-over (40).
        assert_eq!(
            entry_labels(&ranked),
            ["solo-over", "hub:hub", "solo-under"]
        );
        let LeaderboardEntry::Hub(hub) = &ranked.entries[1] else {
            panic!("expected hub entry");
        };
        assert_eq!(hub.total_gain_bytes, 30);
        assert_eq!(hub.total_gain_files, 20);
        // Members ranked within by the same key: heavier first.
        let member_paths: Vec<&str> = hub
            .members
            .iter()
            .map(|m| m.subject.rel_prefix.as_str())
            .collect();
        assert_eq!(member_paths, ["b", "a"]);
        assert!(!hub.counterpart_is_archive);
        assert!(!hub.counterpart_suspended);
    }

    #[test]
    fn coverage_findings_never_group() {
        use FindingNature::Consolidate;
        let shape = || RelationShape::Coverage {
            locations: 3,
            archived_locations: 0,
        };
        let ranked = lens(vec![
            lens_finding("one", FindingTier::Clean, 10, 0, Consolidate, shape()),
            lens_finding("two", FindingTier::Clean, 10, 0, Consolidate, shape()),
        ]);
        assert!(ranked
            .entries
            .iter()
            .all(|e| matches!(e, LeaderboardEntry::Single(_))));
        assert_eq!(ranked.entries.len(), 2);
    }

    #[test]
    fn lens_groups_scale_star_into_one_hub() {
        let (sources, roots) = scale_fixture();
        let sweep = run_structural(&sources, &roots, &low_floors());
        let ranked = reduction_lens(sweep);
        let star = ranked
            .entries
            .iter()
            .find_map(|e| match e {
                LeaderboardEntry::Hub(h)
                    if h.counterpart.root_path == "/r2" && h.counterpart.rel_prefix == "hub" =>
                {
                    Some(h)
                }
                _ => None,
            })
            .expect("the star renders as one hub entry");
        assert_eq!(star.members.len(), 30);
    }
}
