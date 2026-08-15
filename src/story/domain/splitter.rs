//! The place splitter — builds on both location aggregation and act
//! grouping to walk a root's fetched world into the containment tree of
//! emitted places.
//!
//! No I/O anywhere here; callers supply everything fetched.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use super::acts::{group_acts, ActAtom};
use super::locations::{aggregate_locations, aggregate_locations_expanded};
use super::place::{PlaceStanding, StoryInputs, StoryParams, StoryPlace};
use crate::core::domain::extraction::OriginDisposition;
use crate::core::domain::fate::{
    fate_posture, fate_transition, DecisionFamily, FateAspect, Posture,
};
use crate::core::domain::folder_tree::FolderTree;
use crate::core::domain::resolution::{
    classify_absent, classify_present, AbsentBucket, StandingBucket,
};
use crate::domain::path::path_is_under;
use crate::notes::Note;

/// Per-node accumulation for the walk: direct, then subtree by a reverse
/// pass (ids are parents-first).
#[derive(Debug, Clone, Copy, Default)]
struct Counts {
    /// Archived-from-here standing — kept beside `covered` for rendering,
    /// merged with it in the divergence signature (one axis: the split is a
    /// reading distinction, not a boundary the splitter should cut on).
    archived: i64,
    covered: i64,
    /// Contentless standing — outside the question population, like
    /// excluded: nothing to second-guess about no-content (the contentless
    /// law; this is also what keeps empty-file walls from splitting on
    /// hollow coverage).
    contentless: i64,
    excluded: i64,
    /// Subset of `excluded`: no decision stamp (see `PlaceStanding`).
    excluded_stampless: i64,
    unresolved: i64,
    unhashed: i64,
    missing: i64,
    /// Absent with an Observe-family stamp — a recorded loss. Part of the
    /// story population so a deleted-away dir can diverge from its context
    /// (it has no present rows to diverge with).
    deleted: i64,
    files_present: i64,
    bytes_present: i64,
}

impl Counts {
    fn add(&mut self, other: &Counts) {
        self.archived += other.archived;
        self.covered += other.covered;
        self.contentless += other.contentless;
        self.excluded += other.excluded;
        self.excluded_stampless += other.excluded_stampless;
        self.unresolved += other.unresolved;
        self.unhashed += other.unhashed;
        self.missing += other.missing;
        self.deleted += other.deleted;
        self.files_present += other.files_present;
        self.bytes_present += other.bytes_present;
    }

    /// The whole story population: present rows, unexplained-missing rows,
    /// and recorded deletions.
    fn population(&self) -> i64 {
        self.files_present + self.missing + self.deleted
    }

    /// The question population — the second-guessable and the narratable:
    /// covered (evidence without an act), unresolved, unexplained-missing,
    /// and observed deletions. Excluded content is deliberately absent:
    /// exclusion is resolution, the fold target ("fold the
    /// deliberate-uniform, split on the second-guessable") — a uniformly
    /// excluded child has nothing to ask and never splits on standing.
    fn question(&self) -> i64 {
        self.archived + self.covered + self.unresolved + self.missing + self.deleted
    }

    /// Proportions within the question population, plus question density
    /// over the whole population — a child far more question-dense than its
    /// context diverges even when the question mix matches. Archived and
    /// covered share one axis (the split is a reading distinction, not a
    /// boundary); contentless is outside the question entirely.
    fn question_proportions(&self) -> [f64; 5] {
        let q = self.question() as f64;
        [
            (self.archived + self.covered) as f64 / q,
            self.unresolved as f64 / q,
            self.missing as f64 / q,
            self.deleted as f64 / q,
            q / self.population() as f64,
        ]
    }
}

fn dir_of(rel: &str) -> &str {
    match rel.rsplit_once('/') {
        Some((dir, _)) => dir,
        None => "",
    }
}

/// Merge one place's attributed atoms so each decision contributes one
/// slice: several of a decision's dirs can resolve to the same emitted
/// boundary, and `group_acts` expects one entry per decision per group. The
/// merged atom's `files` is the decision's slice weight at this place.
fn merge_slices(atoms: Vec<ActAtom<'_>>) -> Vec<ActAtom<'_>> {
    use std::collections::hash_map::Entry;
    let mut order: Vec<(i64, &'static str, bool)> = Vec::new();
    let mut merged: HashMap<(i64, &'static str, bool), ActAtom<'_>> = HashMap::new();
    for atom in atoms {
        let key = (atom.decision_id, atom.transition, atom.observed);
        match merged.entry(key) {
            Entry::Occupied(mut entry) => {
                let acc = entry.get_mut();
                acc.files += atom.files;
                acc.present_files += atom.present_files;
                acc.bytes = match (acc.bytes, atom.bytes) {
                    (Some(a), Some(b)) => Some(a + b),
                    _ => None,
                };
                acc.moved = match (acc.moved, atom.moved) {
                    (Some(a), Some(b)) => Some(a + b),
                    _ => None,
                };
                acc.copied = match (acc.copied, atom.copied) {
                    (Some(a), Some(b)) => Some(a + b),
                    _ => None,
                };
                acc.destination_dirs.extend(atom.destination_dirs);
            }
            Entry::Vacant(entry) => {
                order.push(key);
                entry.insert(atom);
            }
        }
    }
    order
        .into_iter()
        .map(|key| merged.remove(&key).expect("merged key"))
        .collect()
}

/// Build the place map: the containment tree of emitted places, everything
/// attributed by deepest match.
///
/// Acts land where they touched — per-(decision, directory) slices from
/// extraction origins and stamped dirs, never scope claims — so a decision
/// spanning places renders as slices (partial counts, same id) and
/// `no decision here` is true wherever it renders (the place-grain ruling of
/// the 2026-08-03 tuning spec, superseding the earlier decision-grain
/// anchoring).
///
/// The walk is the sweep's emission discipline transplanted — emit at the
/// widest boundary where the line stays honest, descend only while the story
/// changes: noted nodes and care anchors (a reasoned decision's dir-LCA)
/// always emit; a node emits when its judgment signature (standing
/// proportions over the whole story population, act-signature proportions,
/// or which of the context's covered-where groups it touches) diverges from
/// its nearest emitted ancestor's; dust-sized subtrees — present weight and
/// act weight both under the floors — lift into their parent. Pockets
/// surface: a merged node's descendants are still walked against the same
/// context, so a divergent grandchild splits out even when its parent
/// blended in.
pub fn build_places(inputs: &StoryInputs<'_>, params: &StoryParams) -> StoryPlace {
    let bases: Vec<&str> = inputs.bases.iter().map(String::as_str).collect();

    // ---- tree: intern every location anything refers to ----
    // A source normally lives at its directory's node — but a source whose
    // path is itself a noted place gets its own node, so a note on a file
    // gathers that file's standing and act slices beside the testimony
    // (a noted file whose fate renders elsewhere reads as a mute place —
    // the noted-script finding, first review).
    let noted_paths: HashSet<&str> = inputs.notes.iter().map(|n| n.rel_path.as_str()).collect();
    let mut tree = FolderTree::new();
    tree.intern("");
    let node_of = |tree: &mut FolderTree, rel_path: &str| -> u32 {
        if noted_paths.contains(rel_path) {
            tree.intern(rel_path)
        } else {
            tree.intern(dir_of(rel_path))
        }
    };
    let present_fids: Vec<u32> = inputs
        .present
        .iter()
        .map(|s| node_of(&mut tree, &s.rel_path))
        .collect();
    let absent_fids: Vec<u32> = inputs
        .absent
        .iter()
        .map(|s| node_of(&mut tree, &s.rel_path))
        .collect();
    let note_fids: Vec<u32> = inputs
        .notes
        .iter()
        .map(|n| tree.intern(&n.rel_path))
        .collect();
    for row in inputs.extractions {
        tree.intern(&row.rel_prefix);
    }

    let lca_all = |tree: &FolderTree, fids: &[u32]| -> u32 {
        fids.iter()
            .copied()
            .reduce(|a, b| tree.lca(a, b))
            .unwrap_or(0)
    };

    // ---- act atoms, one per (decision, directory) — the slice grain ----
    // Acts land where they touched: archived acts at the extraction rows'
    // origin dirs, stamp acts at the stamped sources' dirs — never scope
    // claims. A decision spanning places renders as slices; `no decision
    // here` is true wherever it renders. (Archived acts come from extraction
    // rows, never receipts, never Archive stamps — apply stamps the
    // destination rows, not this root's.)
    struct ArchAccum<'a> {
        files: i64,
        bytes: i64,
        bytes_known: bool,
        moved: i64,
        copied: i64,
        disposition_known: bool,
        destination_dirs: Vec<(&'a str, i64)>,
    }
    let mut arch: BTreeMap<(i64, u32), ArchAccum> = BTreeMap::new();
    // Decision-level destination pool: the act signature's destination
    // answer is computed once per decision over all its rows, so
    // same-decision slices share one signature and a mirrored-destination
    // apply cannot fragment into per-dir signatures.
    let mut dest_pool: HashMap<i64, HashMap<&str, i64>> = HashMap::new();
    for row in inputs.extractions {
        let fid = tree.id(&row.rel_prefix).expect("interned above");
        let acc = arch
            .entry((row.decision_id, fid))
            .or_insert_with(|| ArchAccum {
                files: 0,
                bytes: 0,
                bytes_known: true,
                moved: 0,
                copied: 0,
                disposition_known: true,
                destination_dirs: Vec::new(),
            });
        acc.files += row.files;
        match row.bytes {
            Some(b) => acc.bytes += b,
            None => acc.bytes_known = false,
        }
        match row.disposition {
            Some(OriginDisposition::Relocated) => acc.moved += row.files,
            Some(OriginDisposition::Retained) => acc.copied += row.files,
            None => acc.disposition_known = false,
        }
        acc.destination_dirs
            .push((row.destination_path.as_str(), row.files));
        *dest_pool
            .entry(row.decision_id)
            .or_default()
            .entry(row.destination_path.as_str())
            .or_insert(0) += row.files;
    }

    let archived_word = fate_transition(DecisionFamily::Archive, FateAspect::Present)
        .expect("archive/present is a registered transition")
        .as_str();
    let mut atoms: Vec<(u32, ActAtom)> = Vec::new();
    for ((id, fid), acc) in &arch {
        let info = inputs.decisions.get(id);
        atoms.push((
            *fid,
            ActAtom {
                decision_id: *id,
                created_at: info.map(|i| i.created_at).unwrap_or(0),
                reason: info.and_then(|i| i.reason.as_deref()),
                transition: archived_word,
                observed: false,
                files: acc.files,
                present_files: 0,
                bytes: acc.bytes_known.then_some(acc.bytes),
                moved: acc.disposition_known.then_some(acc.moved),
                copied: acc.disposition_known.then_some(acc.copied),
                destination_dirs: acc.destination_dirs.clone(),
            },
        ));
    }

    // Stamp acts per (decision, dir): exclusions, restores, scan-observed
    // deletions. Absent stamped rows (an object exclusion's tombstones)
    // carry slices too — the act register is whole-history, so a dir whose
    // stamped files are all gone still narrates the act while the standing
    // registers stay present-tense. Do not "fix" tombstone slices away.
    #[derive(Default)]
    struct StampAccum {
        present: i64,
        present_bytes: i64,
        absent: i64,
        absent_bytes: i64,
    }
    let mut stamps: BTreeMap<(i64, u32), StampAccum> = BTreeMap::new();
    for (source, fid) in inputs.present.iter().zip(&present_fids) {
        if let Some(id) = source.decision_id {
            let acc = stamps.entry((id, *fid)).or_default();
            acc.present += 1;
            acc.present_bytes += source.size;
        }
    }
    for (source, fid) in inputs.absent.iter().zip(&absent_fids) {
        if let Some(id) = source.decision_id {
            let acc = stamps.entry((id, *fid)).or_default();
            acc.absent += 1;
            acc.absent_bytes += source.size;
        }
    }
    for ((id, fid), acc) in &stamps {
        let Some(info) = inputs.decisions.get(id) else {
            continue; // unknown stamp: no act to narrate (classify_absent
                      // already reads it as unexplained)
        };
        match info.family {
            // Apply stamps its own rows; the extraction row already narrates
            // this archival — narrating the stamp too would count the same
            // files twice.
            DecisionFamily::Archive => continue,
            DecisionFamily::Observe => {
                // A deletion is narrated where it happened — the absent
                // rows' own dirs; the scan merely observed.
                if acc.absent > 0 {
                    let transition = fate_transition(DecisionFamily::Observe, FateAspect::Absent)
                        .expect("observe/absent is a registered transition")
                        .as_str();
                    let observed = fate_posture(DecisionFamily::Observe, FateAspect::Absent)
                        == Posture::Observed;
                    atoms.push((
                        *fid,
                        ActAtom {
                            decision_id: *id,
                            created_at: info.created_at,
                            reason: info.reason.as_deref(),
                            transition,
                            observed,
                            files: acc.absent,
                            present_files: 0,
                            bytes: Some(acc.absent_bytes),
                            moved: None,
                            copied: None,
                            destination_dirs: Vec::new(),
                        },
                    ));
                }
            }
            family => {
                let Some(transition) = fate_transition(family, FateAspect::Present) else {
                    continue;
                };
                let files = acc.present + acc.absent;
                if files == 0 {
                    continue;
                }
                atoms.push((
                    *fid,
                    ActAtom {
                        decision_id: *id,
                        created_at: info.created_at,
                        reason: info.reason.as_deref(),
                        transition: transition.as_str(),
                        observed: false,
                        files,
                        present_files: acc.present,
                        bytes: Some(acc.present_bytes + acc.absent_bytes),
                        moved: None,
                        copied: None,
                        destination_dirs: Vec::new(),
                    },
                ));
            }
        }
    }

    // ---- the care anchor: a reasoned decision forces exactly one node —
    // the LCA of the dirs it touched. Care was expressed at the decision's
    // grain, never per-dir (per-dir forcing would explode a reasoned
    // root-level apply into dozens of forced places); reasonless decisions
    // force nothing — their slices fold wherever the story allows.
    let mut decision_dirs: HashMap<i64, Vec<u32>> = HashMap::new();
    for (fid, atom) in &atoms {
        decision_dirs
            .entry(atom.decision_id)
            .or_default()
            .push(*fid);
    }
    let mut care_anchors: Vec<u32> = Vec::new();
    for (id, fids) in &decision_dirs {
        let reasoned = inputs
            .decisions
            .get(id)
            .and_then(|i| i.reason.as_deref())
            .map(|r| !r.trim().is_empty())
            .unwrap_or(false);
        if reasoned {
            care_anchors.push(lca_all(&tree, fids));
        }
    }

    // ---- act signatures: (transition, posture, decision-level destination
    // answer), interned. Signatures key on what/where, never decision
    // identity: exclusions across many decisions share one signature and
    // fold together, ids and reasons enumerated in the shared register. The
    // why is deliberately NOT a divergence axis — a reasoned act surfaces
    // through its care anchor; putting reasons in the proportional
    // comparison fragments any region where differently-reasoned
    // same-transition acts interleave (the data/usr sgml wall, first
    // review).
    let mut dest_keys: HashMap<i64, String> = HashMap::new();
    for (id, pool) in &dest_pool {
        let mut dirs: Vec<(&str, i64)> = pool.iter().map(|(d, f)| (*d, *f)).collect();
        dirs.sort();
        // Uncapped on purpose: this aggregate is a comparison key, not a
        // line. The display cap may drop groups; a key that drops groups
        // makes different stories compare equal.
        let agg = aggregate_locations(&dirs, &bases, usize::MAX);
        let key = agg
            .locations
            .iter()
            .map(|l| l.path.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        dest_keys.insert(*id, key);
    }
    let mut sig_ids: HashMap<(&'static str, bool, String), usize> = HashMap::new();
    let mut atom_sigs: Vec<usize> = Vec::with_capacity(atoms.len());
    for (_, atom) in &atoms {
        let key = (
            atom.transition,
            atom.observed,
            dest_keys
                .get(&atom.decision_id)
                .cloned()
                .unwrap_or_default(),
        );
        let next = sig_ids.len();
        atom_sigs.push(*sig_ids.entry(key).or_insert(next));
    }

    // ---- per-node payloads: direct, then subtree by reverse pass ----
    let n = tree.len();
    let mut direct = vec![Counts::default(); n];
    let mut direct_locs: Vec<HashMap<&str, i64>> = vec![HashMap::new(); n];
    for (source, &fid) in inputs.present.iter().zip(&present_fids) {
        let counts = &mut direct[fid as usize];
        counts.files_present += 1;
        counts.bytes_present += source.size;
        let bucket = classify_present(source, inputs.archived, inputs.archived_from_here);
        match bucket {
            StandingBucket::Archived | StandingBucket::Covered => {
                match bucket {
                    StandingBucket::Archived => counts.archived += 1,
                    _ => counts.covered += 1,
                }
                // Both standings have copies in the archive; both feed the
                // covered-where answer.
                if let Some(paths) = source
                    .object_id
                    .and_then(|obj| inputs.archive_locations.get(&obj))
                {
                    let mut seen: HashSet<&str> = HashSet::new();
                    for path in paths {
                        let dir = dir_of(path);
                        if seen.insert(dir) {
                            *direct_locs[fid as usize].entry(dir).or_insert(0) += 1;
                        }
                    }
                }
            }
            StandingBucket::Contentless => counts.contentless += 1,
            StandingBucket::Excluded => {
                counts.excluded += 1;
                if source.decision_id.is_none() {
                    counts.excluded_stampless += 1;
                }
            }
            StandingBucket::Unresolved { unhashed } => {
                counts.unresolved += 1;
                if unhashed {
                    counts.unhashed += 1;
                }
            }
        }
    }
    for (source, &fid) in inputs.absent.iter().zip(&absent_fids) {
        let family = source
            .decision_id
            .and_then(|id| inputs.decisions.get(&id))
            .map(|i| i.family);
        match classify_absent(family) {
            AbsentBucket::Unexplained => direct[fid as usize].missing += 1,
            AbsentBucket::Deleted => direct[fid as usize].deleted += 1,
        }
    }

    // Act weight per node: signature files for the divergence test, plain
    // files/bytes for the dust test (a fully-moved-away place has zero
    // present files but a real story).
    let mut direct_acts: Vec<HashMap<usize, i64>> = vec![HashMap::new(); n];
    let mut direct_act_files = vec![0i64; n];
    let mut direct_act_bytes = vec![0i64; n];
    for ((fid, atom), sig) in atoms.iter().zip(&atom_sigs) {
        *direct_acts[*fid as usize].entry(*sig).or_insert(0) += atom.files;
        direct_act_files[*fid as usize] += atom.files;
        direct_act_bytes[*fid as usize] += atom.bytes.unwrap_or(0);
    }

    let mut sub = direct.clone();
    let mut sub_locs = direct_locs.clone();
    for fid in (1..n as u32).rev() {
        let parent = tree.parent(fid).expect("non-root has a parent") as usize;
        let child_counts = sub[fid as usize];
        sub[parent].add(&child_counts);
        let child_locs = sub_locs[fid as usize].clone();
        for (dir, files) in child_locs {
            *sub_locs[parent].entry(dir).or_insert(0) += files;
        }
    }

    // Forced places: the root, noted nodes, and care anchors — never bare
    // operated scopes. Boundaries fall where the what/why changes, not
    // wherever a command happened to be typed; recorded care (a note, a
    // reasoned act) earns a line, floors notwithstanding.
    let mut forced: HashSet<u32> = HashSet::new();
    forced.insert(0);
    forced.extend(note_fids.iter().copied());
    forced.extend(care_anchors.iter().copied());

    // Residual act weight per node: the subtree's acts MINUS forced
    // descendants' (the cut happens at each forced node, so a forced
    // node's own residual stays its own). A folding child merges into its
    // context's register, and by deepest-match that register holds only
    // what isn't claimed deeper — so the divergence test must compare
    // against what the child would actually fold into, or forced-out
    // siblings dilute the context and same-story children split (the
    // home-dir dotfolder over-emission caught on first smoke test).
    // `res_narratable` counts non-exclusion act files — the act-side
    // question gate (excluded is never a question, in standing or in
    // acts: a child whose acts are exclusion-only has nothing narratable
    // to split on).
    let excluded_word = fate_transition(DecisionFamily::Exclude, FateAspect::Present)
        .expect("exclude/present is a registered transition")
        .as_str();
    let narratable_sigs: HashSet<usize> = sig_ids
        .iter()
        .filter(|((transition, _, _), _)| *transition != excluded_word)
        .map(|(_, sig)| *sig)
        .collect();
    let mut res_narratable = vec![0i64; n];
    for ((fid, atom), sig) in atoms.iter().zip(&atom_sigs) {
        if narratable_sigs.contains(sig) {
            res_narratable[*fid as usize] += atom.files;
        }
    }
    let mut res_acts = direct_acts;
    let mut res_act_files = direct_act_files;
    let mut res_act_bytes = direct_act_bytes;
    for fid in (1..n as u32).rev() {
        if forced.contains(&fid) {
            continue;
        }
        let parent = tree.parent(fid).expect("non-root has a parent") as usize;
        let child_acts = res_acts[fid as usize].clone();
        for (sig, files) in child_acts {
            *res_acts[parent].entry(sig).or_insert(0) += files;
        }
        res_act_files[parent] += res_act_files[fid as usize];
        res_act_bytes[parent] += res_act_bytes[fid as usize];
        res_narratable[parent] += res_narratable[fid as usize];
    }

    // ---- the walk ----

    // The context's covered-where groups, computed once per emitted place:
    // which archive prefixes the context's line answers with, and which of
    // them the context itself touches.
    struct CtxSig {
        groups: Vec<String>,
        multi: bool,
        full_set: BTreeSet<usize>,
    }
    let group_set = |dirs: &HashMap<&str, i64>, groups: &[String]| -> BTreeSet<usize> {
        let mut set = BTreeSet::new();
        for dir in dirs.keys() {
            let idx = groups
                .iter()
                .position(|g| path_is_under(dir, g))
                .unwrap_or(usize::MAX);
            set.insert(idx);
        }
        set
    };
    let mut ctx_sigs: HashMap<u32, CtxSig> = HashMap::new();

    let mut emitted: HashSet<u32> = HashSet::new();
    emitted.insert(0);
    let mut stack: Vec<(u32, u32)> = tree.children(0).iter().map(|&c| (c, 0)).collect();
    while let Some((fid, ctx)) = stack.pop() {
        let force = forced.contains(&fid);
        let counts = &sub[fid as usize];
        // Dusty only when present weight AND act weight both sit under the
        // floors.
        let dusty = counts.files_present < params.dust_floor_files
            && counts.bytes_present < params.dust_floor_bytes
            && res_act_files[fid as usize] < params.dust_floor_files
            && res_act_bytes[fid as usize] < params.dust_floor_bytes;
        let mut split = force;
        if !split && !dusty {
            let ctx_counts = &sub[ctx as usize];
            // Standing divergence over the question axes only: a child with
            // no question population has nothing second-guessable and never
            // splits on standing (its resolved content folds into the
            // context's register). The context's question population is at
            // least the child's (subtree sums), so both sides are positive.
            if counts.question() > 0 && ctx_counts.question() > 0 {
                let a = counts.question_proportions();
                let b = ctx_counts.question_proportions();
                split = a
                    .iter()
                    .zip(b.iter())
                    .any(|(x, y)| (x - y).abs() > params.signature_tolerance);
            }
            // Act-signature proportions over the residuals, same tolerance:
            // a node whose act mix diverges from what it would fold into
            // earns a line. Gated on narratable act weight — a child whose
            // acts are exclusion-only has nothing to split on (resolution,
            // the fold target). A residual child's signatures are a subset
            // of its context's residual (both cut at the same forced
            // nodes), so iterating the context covers the union.
            if !split && res_narratable[fid as usize] > 0 {
                let child_total = res_act_files[fid as usize];
                let ctx_total = res_act_files[ctx as usize];
                if child_total > 0 && ctx_total > 0 {
                    let child_map = &res_acts[fid as usize];
                    for (sig, ctx_files) in &res_acts[ctx as usize] {
                        let c = *child_map.get(sig).unwrap_or(&0) as f64 / child_total as f64;
                        let x = *ctx_files as f64 / ctx_total as f64;
                        if (c - x).abs() > params.signature_tolerance {
                            split = true;
                            break;
                        }
                    }
                }
            }
            if !split && !sub_locs[fid as usize].is_empty() {
                let sig = ctx_sigs.entry(ctx).or_insert_with(|| {
                    let dirs: Vec<(&str, i64)> = sub_locs[ctx as usize]
                        .iter()
                        .map(|(d, f)| (*d, *f))
                        .collect();
                    // Uncapped on purpose: this aggregate is a comparison key,
                    // not a line. The display cap may drop groups; a key that
                    // drops groups makes different stories compare equal.
                    let agg = aggregate_locations(&dirs, &bases, usize::MAX);
                    let groups: Vec<String> = agg.locations.into_iter().map(|l| l.path).collect();
                    let multi = groups.len() > 1;
                    let full_set = group_set(&sub_locs[ctx as usize], &groups);
                    CtxSig {
                        groups,
                        multi,
                        full_set,
                    }
                });
                if sig.multi {
                    let child_set = group_set(&sub_locs[fid as usize], &sig.groups);
                    split = child_set != sig.full_set;
                }
            }
        }
        let next_ctx = if split {
            emitted.insert(fid);
            fid
        } else {
            ctx
        };
        for &child in tree.children(fid) {
            stack.push((child, next_ctx));
        }
    }

    // ---- attribution by deepest emitted ancestor ----
    let resolve = |fid: u32| -> u32 {
        let mut cur = fid;
        loop {
            if emitted.contains(&cur) {
                return cur;
            }
            cur = tree.parent(cur).expect("root is emitted");
        }
    };

    struct PlaceAccum<'a> {
        standing: PlaceStanding,
        locs: HashMap<&'a str, i64>,
        notes: Vec<Note>,
        atoms: Vec<ActAtom<'a>>,
        content_dirs: HashSet<u32>,
    }
    let mut accums: HashMap<u32, PlaceAccum> = emitted
        .iter()
        .map(|&fid| {
            (
                fid,
                PlaceAccum {
                    standing: PlaceStanding::default(),
                    locs: HashMap::new(),
                    notes: Vec::new(),
                    atoms: Vec::new(),
                    content_dirs: HashSet::new(),
                },
            )
        })
        .collect();

    for fid in 0..n as u32 {
        let counts = &direct[fid as usize];
        let has_content = counts.files_present > 0 || counts.missing > 0;
        if !has_content && direct_locs[fid as usize].is_empty() {
            continue;
        }
        let place = resolve(fid);
        let accum = accums.get_mut(&place).expect("emitted place");
        accum.standing.archived += counts.archived;
        accum.standing.covered += counts.covered;
        accum.standing.contentless += counts.contentless;
        accum.standing.excluded += counts.excluded;
        accum.standing.excluded_stampless += counts.excluded_stampless;
        accum.standing.unresolved += counts.unresolved;
        accum.standing.unhashed_unresolved += counts.unhashed;
        accum.standing.missing_unexplained += counts.missing;
        if has_content {
            accum.content_dirs.insert(fid);
        }
        for (dir, files) in &direct_locs[fid as usize] {
            *accum.locs.entry(dir).or_insert(0) += files;
        }
    }
    for (note, fid) in inputs.notes.iter().zip(&note_fids) {
        accums
            .get_mut(&resolve(*fid))
            .expect("emitted place")
            .notes
            .push(note.clone());
    }
    for (anchor, atom) in atoms {
        accums
            .get_mut(&resolve(anchor))
            .expect("emitted place")
            .atoms
            .push(atom);
    }

    // ---- assemble the containment tree (ids are parents-first, so a
    // descending pass sees every child before its parent) ----
    let mut emitted_sorted: Vec<u32> = emitted.iter().copied().collect();
    emitted_sorted.sort_unstable();
    let mut built: HashMap<u32, StoryPlace> = HashMap::new();
    let mut children_of: HashMap<u32, Vec<StoryPlace>> = HashMap::new();
    for &fid in emitted_sorted.iter().rev() {
        let accum = accums.remove(&fid).expect("accumulated place");
        let mut notes = accum.notes;
        notes.sort_by_key(|note| (note.created_at, note.id));
        let loc_dirs: Vec<(&str, i64)> = {
            let mut dirs: Vec<(&str, i64)> = accum.locs.iter().map(|(d, f)| (*d, *f)).collect();
            dirs.sort();
            dirs
        };
        let mut children = children_of.remove(&fid).unwrap_or_default();
        children.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
        let place = StoryPlace {
            rel_path: tree.path(fid).to_string(),
            acts: group_acts(&merge_slices(accum.atoms), &bases, params.where_cap),
            standing: accum.standing,
            covered_where: aggregate_locations_expanded(&loc_dirs, &bases, params.where_cap),
            notes,
            folder_breadth: accum.content_dirs.len() as u32,
            children,
        };
        if fid == 0 {
            built.insert(0, place);
        } else {
            let parent_place = {
                let mut cur = tree.parent(fid).expect("non-root has a parent");
                loop {
                    if emitted.contains(&cur) {
                        break cur;
                    }
                    cur = tree.parent(cur).expect("root is emitted");
                }
            };
            children_of.entry(parent_place).or_default().push(place);
        }
    }
    built.remove(&0).expect("root place")
}
