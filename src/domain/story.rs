//! Pure logic for the story review — the judgment instrument that renders a
//! root's resolution story as a map of places (`canon roots story`).
//!
//! The organizing axis is deliberate versus never-explicitly-decided: acts
//! (decisions with their reasons) versus standings that carry no trace of
//! judgment. This module holds the leaf helpers: location aggregation (the
//! one "where" derivation, shared by act destinations and covered-copy
//! locations) and act grouping (the what/why register). The place splitter
//! builds on both.
//!
//! No I/O anywhere here; callers supply everything fetched.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use super::extraction::{DecisionExtraction, OriginDisposition};
use super::folder_tree::FolderTree;
use super::note::Note;
use super::path::path_is_under;
use super::retire::{classify_absent, classify_present, AbsentBucket, StandingBucket};
use super::source::Source;
use super::trail::{fate_posture, fate_transition, DecisionFamily, FateAspect, Posture};

/// Named calibratable constants (the sweep discipline). Defaults are initial
/// guesses until the first calibration pass against the real archive locks
/// them; recalibration changes constants, never code paths.
pub struct StoryParams {
    /// Children whose standing proportions differ by no more than this tell
    /// the same line and merge into their parent place.
    pub signature_tolerance: f64,
    /// A child below both dust floors lifts into its parent rather than
    /// earning a place line of its own.
    pub dust_floor_files: i64,
    pub dust_floor_bytes: i64,
    /// Location entries per "where" line before the counted remainder.
    pub where_cap: usize,
}

impl Default for StoryParams {
    fn default() -> Self {
        Self {
            signature_tolerance: 0.10,
            dust_floor_files: 10,
            dust_floor_bytes: 1_000_000,
            where_cap: 3,
        }
    }
}

/// One location entry on a "where" line: a directory prefix and how many of
/// the line's files it accounts for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocationCount {
    pub path: String,
    pub files: i64,
}

/// A set of locations aggregated to the fewest legible prefixes, capped with
/// a counted remainder — never a silent truncation.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LocationAggregate {
    pub locations: Vec<LocationCount>,
    /// Location groups beyond the cap ("… and N more locations").
    pub omitted_locations: usize,
}

impl LocationAggregate {
    pub fn is_empty(&self) -> bool {
        self.locations.is_empty()
    }

    /// The location paths alone — the merge identity for act grouping.
    fn paths(&self) -> Vec<String> {
        self.locations.iter().map(|l| l.path.clone()).collect()
    }
}

/// Aggregate a set of directories (with per-directory file counts) to the
/// fewest legible prefixes.
///
/// `bases` are the path prefixes below which an answer starts to say
/// something (archive root paths for covered locations, known root paths for
/// act destinations): a prefix at or above every base is vacuous — "in the
/// archive" answers nothing — so the walk splits below it instead.
///
/// The rule: collapse single-branch chains from the top to the deepest
/// common node. If that node is strictly below a base, it alone is the
/// answer (the widest honest one-line where — fan-out beneath it is shape,
/// not information). Otherwise emit its branch groups, each chain-collapsed
/// to where it first branches or holds files, ordered by weight then path,
/// capped with a counted remainder.
pub fn aggregate_locations(dirs: &[(&str, i64)], bases: &[&str], cap: usize) -> LocationAggregate {
    if dirs.is_empty() {
        return LocationAggregate::default();
    }

    let mut tree = FolderTree::new();
    let mut direct: Vec<i64> = Vec::new();
    for (dir, files) in dirs {
        let fid = tree.intern(dir) as usize;
        if direct.len() < tree.len() {
            direct.resize(tree.len(), 0);
        }
        direct[fid] += files;
    }
    direct.resize(tree.len(), 0);

    // Subtree sums: ids are parents-first, so one reverse pass suffices.
    let mut sub = direct.clone();
    for fid in (0..tree.len() as u32).rev() {
        if let Some(parent) = tree.parent(fid) {
            sub[parent as usize] += sub[fid as usize];
        }
    }

    let collapse = |mut fid: u32| -> u32 {
        while direct[fid as usize] == 0 && tree.children(fid).len() == 1 {
            fid = tree.children(fid)[0];
        }
        fid
    };

    let top = collapse(0);
    let top_path = tree.path(top);
    let legible = bases
        .iter()
        .any(|base| top_path != *base && path_is_under(top_path, base));
    if legible {
        return LocationAggregate {
            locations: vec![LocationCount {
                path: top_path.to_string(),
                files: sub[top as usize],
            }],
            omitted_locations: 0,
        };
    }

    // Vacuous common node: emit its branch groups instead.
    let mut groups: Vec<LocationCount> = Vec::new();
    if direct[top as usize] > 0 {
        groups.push(LocationCount {
            path: tree.path(top).to_string(),
            files: direct[top as usize],
        });
    }
    for &child in tree.children(top) {
        let group = collapse(child);
        groups.push(LocationCount {
            path: tree.path(group).to_string(),
            files: sub[child as usize],
        });
    }
    groups.sort_by(|a, b| b.files.cmp(&a.files).then_with(|| a.path.cmp(&b.path)));
    let omitted_locations = groups.len().saturating_sub(cap);
    groups.truncate(cap);

    LocationAggregate {
        locations: groups,
        omitted_locations,
    }
}

/// One decision's contribution to a place, before grouping — the atom the
/// splitter derives from stamps and extraction rows.
#[derive(Debug, Clone)]
pub struct ActAtom<'a> {
    pub decision_id: i64,
    pub created_at: i64,
    pub reason: Option<&'a str>,
    /// Registered transition word, derived via `fate_transition` — never a
    /// literal at the call site.
    pub transition: &'static str,
    /// Scan-observed (a deletion the world made) as opposed to performed.
    pub observed: bool,
    pub files: i64,
    /// `None` when the record cannot say — all-or-omitted at group level.
    pub bytes: Option<i64>,
    /// Disposition split for archivals; `None` when any contributing row
    /// predates the vocabulary — omitted, never guessed.
    pub moved: Option<i64>,
    pub copied: Option<i64>,
    /// Destination directories with per-directory file counts (archivals
    /// only; empty for exclusions and deletions — nothing went anywhere).
    pub destination_dirs: Vec<(&'a str, i64)>,
}

/// One decision inside an act group, oldest-first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActDecision {
    pub id: i64,
    pub created_at: i64,
    pub reason: Option<String>,
}

/// Acts aggregated for one place line: same transition, same destination
/// aggregate. The what compresses; the whys (reasons per decision) never
/// disappear; the where never blurs — acts that went to different
/// destinations stay separate lines.
#[derive(Debug, Clone, PartialEq)]
pub struct ActGroup {
    pub transition: &'static str,
    pub observed: bool,
    pub destination: LocationAggregate,
    pub files: i64,
    /// All-or-omitted: `Some` only when every grouped decision knew.
    pub bytes: Option<i64>,
    pub moved: Option<i64>,
    pub copied: Option<i64>,
    pub decisions: Vec<ActDecision>,
}

/// The whys of an act group, ready to render: distinct reasons in
/// first-seen order with the decisions that gave them, and the count that
/// recorded none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReasonSummary {
    pub reasons: Vec<(String, Vec<i64>)>,
    pub without_reason: usize,
}

impl ActGroup {
    pub fn reason_summary(&self) -> ReasonSummary {
        let mut reasons: Vec<(String, Vec<i64>)> = Vec::new();
        let mut without_reason = 0;
        for decision in &self.decisions {
            match &decision.reason {
                Some(reason) => match reasons.iter_mut().find(|(r, _)| r == reason) {
                    Some((_, ids)) => ids.push(decision.id),
                    None => reasons.push((reason.clone(), vec![decision.id])),
                },
                None => without_reason += 1,
            }
        }
        ReasonSummary {
            reasons,
            without_reason,
        }
    }
}

/// Group a place's act atoms by (transition, posture, destination
/// aggregate). Groups order by their earliest decision; decisions within a
/// group are oldest-first.
pub fn group_acts(atoms: &[ActAtom], bases: &[&str], cap: usize) -> Vec<ActGroup> {
    struct Accum<'a> {
        transition: &'static str,
        observed: bool,
        pooled_dirs: HashMap<&'a str, i64>,
        files: i64,
        bytes: Option<i64>,
        bytes_complete: bool,
        moved: Option<i64>,
        moved_complete: bool,
        copied: Option<i64>,
        copied_complete: bool,
        decisions: Vec<ActDecision>,
    }

    let mut order: Vec<(String, usize)> = Vec::new();
    let mut accums: HashMap<String, Accum> = HashMap::new();

    for atom in atoms {
        let dest = aggregate_locations(&atom.destination_dirs, bases, cap);
        let key = format!(
            "{}|{}|{}",
            atom.transition,
            atom.observed,
            dest.paths().join("\n")
        );
        let accum = accums.entry(key.clone()).or_insert_with(|| {
            order.push((key, order.len()));
            Accum {
                transition: atom.transition,
                observed: atom.observed,
                pooled_dirs: HashMap::new(),
                files: 0,
                bytes: Some(0),
                bytes_complete: true,
                moved: Some(0),
                moved_complete: true,
                copied: Some(0),
                copied_complete: true,
                decisions: Vec::new(),
            }
        });
        for (dir, files) in &atom.destination_dirs {
            *accum.pooled_dirs.entry(dir).or_insert(0) += files;
        }
        accum.files += atom.files;
        match atom.bytes {
            Some(b) => accum.bytes = accum.bytes.map(|acc| acc + b),
            None => accum.bytes_complete = false,
        }
        match atom.moved {
            Some(m) => accum.moved = accum.moved.map(|acc| acc + m),
            None => accum.moved_complete = false,
        }
        match atom.copied {
            Some(c) => accum.copied = accum.copied.map(|acc| acc + c),
            None => accum.copied_complete = false,
        }
        accum.decisions.push(ActDecision {
            id: atom.decision_id,
            created_at: atom.created_at,
            reason: atom.reason.map(str::to_string),
        });
    }

    let mut groups: Vec<ActGroup> = order
        .into_iter()
        .map(|(key, _)| {
            let mut accum = accums.remove(&key).expect("accumulated key");
            accum
                .decisions
                .sort_by(|a, b| a.created_at.cmp(&b.created_at).then(a.id.cmp(&b.id)));
            let pooled: Vec<(&str, i64)> = {
                let mut dirs: Vec<(&str, i64)> =
                    accum.pooled_dirs.iter().map(|(d, f)| (*d, *f)).collect();
                dirs.sort();
                dirs
            };
            ActGroup {
                transition: accum.transition,
                observed: accum.observed,
                destination: aggregate_locations(&pooled, bases, cap),
                files: accum.files,
                bytes: if accum.bytes_complete {
                    accum.bytes
                } else {
                    None
                },
                moved: if accum.moved_complete {
                    accum.moved
                } else {
                    None
                },
                copied: if accum.copied_complete {
                    accum.copied
                } else {
                    None
                },
                decisions: accum.decisions,
            }
        })
        .collect();

    groups.sort_by_key(|g| {
        g.decisions
            .first()
            .map(|d| (d.created_at, d.id))
            .unwrap_or((i64::MAX, i64::MAX))
    });
    groups
}

/// What the splitter needs to know about one decision.
#[derive(Debug, Clone)]
pub struct DecisionInfo {
    pub family: DecisionFamily,
    pub created_at: i64,
    pub reason: Option<String>,
}

/// Everything the place walk consumes, decomposed from the fetched story.
/// The ops layer supplies borrowed slices; no repo types cross this boundary.
pub struct StoryInputs<'a> {
    pub present: &'a [Source],
    pub absent: &'a [Source],
    /// Object ids among the present rows verified present in the archive.
    pub archived: &'a HashSet<i64>,
    /// Extraction rows whose origin is this root — the archived acts.
    pub extractions: &'a [DecisionExtraction],
    /// (decision id, rel prefix), one entry per decision-scope row on this
    /// root: where each decision declared it operated.
    pub operated_scopes: &'a [(i64, String)],
    pub decisions: &'a HashMap<i64, DecisionInfo>,
    pub notes: &'a [Note],
    /// Object id → full paths of the archive copies. Zero-byte objects are
    /// excluded upstream (the book's contentless gate, reused), so a covered
    /// empty file counts in the standing but claims no locations.
    pub archive_locations: &'a HashMap<i64, Vec<String>>,
    /// Known root paths — the legibility bases for every "where" answer.
    pub bases: &'a [String],
}

/// Present standings and record-quality facts attributed to one place.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PlaceStanding {
    pub covered: i64,
    pub excluded: i64,
    pub unresolved: i64,
    /// Subset of `unresolved`: never hashed — cannot be content-verified.
    pub unhashed_unresolved: i64,
    /// Absent without a recorded deletion — a record-quality fact.
    pub missing_unexplained: i64,
}

impl PlaceStanding {
    pub fn is_empty(&self) -> bool {
        self.covered == 0
            && self.excluded == 0
            && self.unresolved == 0
            && self.missing_unexplained == 0
    }
}

/// One place on the map: a node the walk emitted, with everything under it
/// attributed by deepest match.
#[derive(Debug)]
pub struct StoryPlace {
    /// Path relative to the root; `""` is the root itself.
    pub rel_path: String,
    /// The acts, grouped for the what/why register; empty = no decision here.
    pub acts: Vec<ActGroup>,
    pub standing: PlaceStanding,
    /// Where this place's covered copies stand — observed, nobody chose it.
    pub covered_where: LocationAggregate,
    /// The user's own testimony at this place, oldest first.
    pub notes: Vec<Note>,
    /// Distinct folders holding content merged into this line.
    pub folder_breadth: u32,
    pub children: Vec<StoryPlace>,
}

impl StoryPlace {
    /// No decision here: nothing was ever acted on at or under this place
    /// (its standing is evidence without an act).
    pub fn undecided(&self) -> bool {
        self.acts.is_empty()
    }
}

/// Per-node accumulation for the walk: direct, then subtree by a reverse
/// pass (ids are parents-first).
#[derive(Debug, Clone, Copy, Default)]
struct Counts {
    covered: i64,
    excluded: i64,
    unresolved: i64,
    unhashed: i64,
    missing: i64,
    files_present: i64,
    bytes_present: i64,
}

impl Counts {
    fn add(&mut self, other: &Counts) {
        self.covered += other.covered;
        self.excluded += other.excluded;
        self.unresolved += other.unresolved;
        self.unhashed += other.unhashed;
        self.missing += other.missing;
        self.files_present += other.files_present;
        self.bytes_present += other.bytes_present;
    }

    /// The judgment population: present rows plus unexplained-missing rows.
    fn population(&self) -> i64 {
        self.files_present + self.missing
    }

    fn proportions(&self) -> [f64; 4] {
        let pop = self.population() as f64;
        [
            self.covered as f64 / pop,
            self.excluded as f64 / pop,
            self.unresolved as f64 / pop,
            self.missing as f64 / pop,
        ]
    }
}

fn dir_of(rel: &str) -> &str {
    match rel.rsplit_once('/') {
        Some((dir, _)) => dir,
        None => "",
    }
}

/// Build the place map: the containment tree of emitted places, everything
/// attributed by deepest match.
///
/// The walk is the sweep's emission discipline transplanted — emit at the
/// widest boundary where the line stays honest, descend only while the story
/// changes: operated nodes (act anchors) and noted nodes always emit; an
/// undecided node emits when its judgment signature (standing proportions +
/// which of the context's covered-where groups it touches) diverges from its
/// nearest emitted ancestor's; dust-sized subtrees lift into their parent.
/// Pockets surface: a merged node's descendants are still walked against the
/// same context, so a divergent grandchild splits out even when its parent
/// blended in.
pub fn build_places(inputs: &StoryInputs<'_>, params: &StoryParams) -> StoryPlace {
    let bases: Vec<&str> = inputs.bases.iter().map(String::as_str).collect();

    // ---- tree: intern every location anything refers to ----
    let mut tree = FolderTree::new();
    tree.intern("");
    let present_fids: Vec<u32> = inputs
        .present
        .iter()
        .map(|s| tree.intern(dir_of(&s.rel_path)))
        .collect();
    let absent_fids: Vec<u32> = inputs
        .absent
        .iter()
        .map(|s| tree.intern(dir_of(&s.rel_path)))
        .collect();
    let note_fids: Vec<u32> = inputs
        .notes
        .iter()
        .map(|n| tree.intern(&n.rel_path))
        .collect();
    for (_, prefix) in inputs.operated_scopes {
        tree.intern(prefix);
    }
    for row in inputs.extractions {
        tree.intern(&row.rel_prefix);
    }

    let lca_all = |tree: &FolderTree, fids: &[u32]| -> u32 {
        fids.iter()
            .copied()
            .reduce(|a, b| tree.lca(a, b))
            .unwrap_or(0)
    };

    let mut scope_fids: HashMap<i64, Vec<u32>> = HashMap::new();
    for (id, prefix) in inputs.operated_scopes {
        let fid = tree.id(prefix).expect("interned above");
        scope_fids.entry(*id).or_default().push(fid);
    }

    // ---- act atoms, each anchored at the node where the user acted ----
    // Archived acts come from extraction rows (never receipts, never Archive
    // stamps — apply stamps the destination rows, not this root's).
    struct ArchAccum<'a> {
        files: i64,
        bytes: i64,
        bytes_known: bool,
        moved: i64,
        copied: i64,
        disposition_known: bool,
        destination_dirs: Vec<(&'a str, i64)>,
        origin_fids: Vec<u32>,
    }
    let mut arch: BTreeMap<i64, ArchAccum> = BTreeMap::new();
    for row in inputs.extractions {
        let acc = arch.entry(row.decision_id).or_insert_with(|| ArchAccum {
            files: 0,
            bytes: 0,
            bytes_known: true,
            moved: 0,
            copied: 0,
            disposition_known: true,
            destination_dirs: Vec::new(),
            origin_fids: Vec::new(),
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
        acc.origin_fids
            .push(tree.id(&row.rel_prefix).expect("interned above"));
    }

    let archived_word = fate_transition(DecisionFamily::Archive, FateAspect::Present)
        .expect("archive/present is a registered transition")
        .as_str();
    let mut atoms: Vec<(u32, ActAtom)> = Vec::new();
    for (id, acc) in &arch {
        let info = inputs.decisions.get(id);
        let anchor = match scope_fids.get(id) {
            Some(fids) => lca_all(&tree, fids),
            None => lca_all(&tree, &acc.origin_fids),
        };
        atoms.push((
            anchor,
            ActAtom {
                decision_id: *id,
                created_at: info.map(|i| i.created_at).unwrap_or(0),
                reason: info.and_then(|i| i.reason.as_deref()),
                transition: archived_word,
                observed: false,
                files: acc.files,
                bytes: acc.bytes_known.then_some(acc.bytes),
                moved: acc.disposition_known.then_some(acc.moved),
                copied: acc.disposition_known.then_some(acc.copied),
                destination_dirs: acc.destination_dirs.clone(),
            },
        ));
    }

    // Stamp acts: exclusions, restores, scan-observed deletions.
    #[derive(Default)]
    struct StampAccum {
        present: i64,
        present_bytes: i64,
        absent: i64,
        absent_bytes: i64,
        stamped_fids: Vec<u32>,
        absent_fids: Vec<u32>,
    }
    let mut stamps: BTreeMap<i64, StampAccum> = BTreeMap::new();
    for (source, fid) in inputs.present.iter().zip(&present_fids) {
        if let Some(id) = source.decision_id {
            let acc = stamps.entry(id).or_default();
            acc.present += 1;
            acc.present_bytes += source.size;
            acc.stamped_fids.push(*fid);
        }
    }
    for (source, fid) in inputs.absent.iter().zip(&absent_fids) {
        if let Some(id) = source.decision_id {
            let acc = stamps.entry(id).or_default();
            acc.absent += 1;
            acc.absent_bytes += source.size;
            acc.stamped_fids.push(*fid);
            acc.absent_fids.push(*fid);
        }
    }
    for (id, acc) in &stamps {
        let Some(info) = inputs.decisions.get(id) else {
            continue; // unknown stamp: no act to narrate (classify_absent
                      // already reads it as unexplained)
        };
        match info.family {
            DecisionFamily::Archive => continue,
            DecisionFamily::Observe => {
                // A deletion is placed where it happened (the absent rows),
                // not at the scan's scope — the scan observed, the user
                // didn't act there.
                if acc.absent > 0 {
                    let transition = fate_transition(DecisionFamily::Observe, FateAspect::Absent)
                        .expect("observe/absent is a registered transition")
                        .as_str();
                    let observed = fate_posture(DecisionFamily::Observe, FateAspect::Absent)
                        == Posture::Observed;
                    atoms.push((
                        lca_all(&tree, &acc.absent_fids),
                        ActAtom {
                            decision_id: *id,
                            created_at: info.created_at,
                            reason: info.reason.as_deref(),
                            transition,
                            observed,
                            files: acc.absent,
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
                let anchor = match scope_fids.get(id) {
                    Some(fids) => lca_all(&tree, fids),
                    None => lca_all(&tree, &acc.stamped_fids),
                };
                atoms.push((
                    anchor,
                    ActAtom {
                        decision_id: *id,
                        created_at: info.created_at,
                        reason: info.reason.as_deref(),
                        transition: transition.as_str(),
                        observed: false,
                        files,
                        bytes: Some(acc.present_bytes + acc.absent_bytes),
                        moved: None,
                        copied: None,
                        destination_dirs: Vec::new(),
                    },
                ));
            }
        }
    }

    // ---- per-node payloads: direct, then subtree by reverse pass ----
    let n = tree.len();
    let mut direct = vec![Counts::default(); n];
    let mut direct_locs: Vec<HashMap<&str, i64>> = vec![HashMap::new(); n];
    for (source, &fid) in inputs.present.iter().zip(&present_fids) {
        let counts = &mut direct[fid as usize];
        counts.files_present += 1;
        counts.bytes_present += source.size;
        match classify_present(source, inputs.archived) {
            StandingBucket::Covered => {
                counts.covered += 1;
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
            StandingBucket::Excluded => counts.excluded += 1,
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
        if classify_absent(family) == AbsentBucket::Unexplained {
            direct[fid as usize].missing += 1;
        }
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

    // ---- the walk ----
    let mut forced: HashSet<u32> = HashSet::new();
    forced.insert(0);
    forced.extend(note_fids.iter().copied());
    forced.extend(atoms.iter().map(|(anchor, _)| *anchor));

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
        let dusty = counts.files_present < params.dust_floor_files
            && counts.bytes_present < params.dust_floor_bytes;
        let mut split = force;
        if !split && !dusty {
            let ctx_counts = &sub[ctx as usize];
            if counts.population() > 0 && ctx_counts.population() > 0 {
                let a = counts.proportions();
                let b = ctx_counts.proportions();
                split = a
                    .iter()
                    .zip(b.iter())
                    .any(|(x, y)| (x - y).abs() > params.signature_tolerance);
            }
            if !split && !sub_locs[fid as usize].is_empty() {
                let sig = ctx_sigs.entry(ctx).or_insert_with(|| {
                    let dirs: Vec<(&str, i64)> = sub_locs[ctx as usize]
                        .iter()
                        .map(|(d, f)| (*d, *f))
                        .collect();
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
        accum.standing.covered += counts.covered;
        accum.standing.excluded += counts.excluded;
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
            acts: group_acts(&accum.atoms, &bases, params.where_cap),
            standing: accum.standing,
            covered_where: aggregate_locations(&loc_dirs, &bases, params.where_cap),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn agg(dirs: &[(&str, i64)], bases: &[&str]) -> LocationAggregate {
        aggregate_locations(dirs, bases, 3)
    }

    #[test]
    fn empty_dirs_aggregate_to_nothing() {
        let out = agg(&[], &["/archive"]);
        assert!(out.is_empty());
        assert_eq!(out.omitted_locations, 0);
    }

    #[test]
    fn single_directory_is_its_own_answer() {
        let out = agg(&[("/archive/media/2016", 640)], &["/archive"]);
        assert_eq!(
            out.locations,
            vec![LocationCount {
                path: "/archive/media/2016".into(),
                files: 640
            }]
        );
    }

    #[test]
    fn coherent_fanout_collapses_to_the_common_prefix() {
        // Twelve month dirs under one year: the year is the answer; the
        // fan-out beneath it is shape, not information.
        let dirs: Vec<(String, i64)> = (1..=12)
            .map(|m| (format!("/archive/media/2016/{m:02}"), 10))
            .collect();
        let refs: Vec<(&str, i64)> = dirs.iter().map(|(d, f)| (d.as_str(), *f)).collect();
        let out = agg(&refs, &["/archive"]);
        assert_eq!(
            out.locations,
            vec![LocationCount {
                path: "/archive/media/2016".into(),
                files: 120
            }]
        );
        assert_eq!(out.omitted_locations, 0);
    }

    #[test]
    fn divergence_at_the_base_lists_the_groups() {
        // The common node is the archive root itself — vacuous ("in the
        // archive" answers nothing) — so the branch groups are the answer.
        let out = agg(
            &[
                ("/archive/staging-2019/worlds", 3401),
                ("/archive/games", 11),
            ],
            &["/archive"],
        );
        assert_eq!(
            out.locations,
            vec![
                LocationCount {
                    path: "/archive/staging-2019/worlds".into(),
                    files: 3401
                },
                LocationCount {
                    path: "/archive/games".into(),
                    files: 11
                },
            ]
        );
    }

    #[test]
    fn groups_beyond_the_cap_are_counted_never_dropped_silently() {
        let dirs: Vec<(String, i64)> = (0..6)
            .map(|i| (format!("/archive/spot-{i}"), 10 + i as i64))
            .collect();
        let refs: Vec<(&str, i64)> = dirs.iter().map(|(d, f)| (d.as_str(), *f)).collect();
        let out = agg(&refs, &["/archive"]);
        assert_eq!(out.locations.len(), 3);
        assert_eq!(out.omitted_locations, 3);
        // Heaviest first.
        assert_eq!(out.locations[0].path, "/archive/spot-5");
    }

    #[test]
    fn ordering_is_weight_then_path() {
        let out = agg(
            &[("/archive/b", 10), ("/archive/a", 10), ("/archive/c", 20)],
            &["/archive"],
        );
        let paths: Vec<&str> = out.locations.iter().map(|l| l.path.as_str()).collect();
        assert_eq!(paths, vec!["/archive/c", "/archive/a", "/archive/b"]);
    }

    #[test]
    fn no_matching_base_splits_at_the_first_branching() {
        // A destination whose root is gone from the fleet: nothing is
        // legible, so the top-level branch groups are the answer.
        let out = agg(
            &[("/old-archive/media", 5), ("/old-archive/docs", 3)],
            &["/archive"],
        );
        let paths: Vec<&str> = out.locations.iter().map(|l| l.path.as_str()).collect();
        assert_eq!(paths, vec!["/old-archive/media", "/old-archive/docs"]);
    }

    #[test]
    fn base_itself_carrying_files_is_its_own_group() {
        let out = agg(&[("/archive", 7), ("/archive/media", 5)], &["/archive"]);
        let paths: Vec<&str> = out.locations.iter().map(|l| l.path.as_str()).collect();
        assert_eq!(paths, vec!["/archive", "/archive/media"]);
    }

    fn atom<'a>(
        id: i64,
        at: i64,
        reason: Option<&'a str>,
        transition: &'static str,
        files: i64,
    ) -> ActAtom<'a> {
        ActAtom {
            decision_id: id,
            created_at: at,
            reason,
            transition,
            observed: false,
            files,
            bytes: None,
            moved: None,
            copied: None,
            destination_dirs: vec![],
        }
    }

    #[test]
    fn iterative_exclusions_merge_into_one_group_with_reasons_enumerated() {
        let atoms = vec![
            atom(57, 100, Some("installer junk"), "excluded", 200),
            atom(61, 200, Some("installer junk"), "excluded", 300),
            atom(63, 300, Some("old exports"), "excluded", 90),
            atom(64, 400, None, "excluded", 4300),
        ];
        let groups = group_acts(&atoms, &["/archive"], 3);
        assert_eq!(groups.len(), 1);
        let g = &groups[0];
        assert_eq!(g.files, 4890);
        assert_eq!(g.decisions.len(), 4);
        let summary = g.reason_summary();
        assert_eq!(
            summary.reasons,
            vec![
                ("installer junk".to_string(), vec![57, 61]),
                ("old exports".to_string(), vec![63]),
            ]
        );
        assert_eq!(summary.without_reason, 1);
    }

    #[test]
    fn archivals_to_different_destinations_never_merge() {
        let mut a = atom(42, 100, Some("the Italy trip"), "archived", 640);
        a.destination_dirs = vec![("/archive/media/2016-italy", 640)];
        a.bytes = Some(1000);
        let mut b = atom(51, 200, None, "archived", 4102);
        b.destination_dirs = vec![("/archive/media/2017", 4102)];
        b.bytes = Some(2000);
        let groups = group_acts(&[a, b], &["/archive"], 3);
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].decisions[0].id, 42);
        assert_eq!(groups[1].decisions[0].id, 51);
    }

    #[test]
    fn archivals_to_the_same_destination_merge_and_sum() {
        let mut a = atom(42, 100, None, "archived", 100);
        a.destination_dirs = vec![("/archive/media/2016", 100)];
        a.bytes = Some(1_000);
        a.moved = Some(100);
        a.copied = Some(0);
        let mut b = atom(48, 200, None, "archived", 50);
        b.destination_dirs = vec![("/archive/media/2016", 50)];
        b.bytes = Some(500);
        b.moved = Some(20);
        b.copied = Some(30);
        let groups = group_acts(&[a, b], &["/archive"], 3);
        assert_eq!(groups.len(), 1);
        let g = &groups[0];
        assert_eq!(g.files, 150);
        assert_eq!(g.bytes, Some(1_500));
        assert_eq!(g.moved, Some(120));
        assert_eq!(g.copied, Some(30));
        assert_eq!(g.destination.locations[0].path, "/archive/media/2016");
        assert_eq!(g.destination.locations[0].files, 150);
    }

    #[test]
    fn bytes_and_disposition_are_all_or_omitted() {
        let mut a = atom(42, 100, None, "archived", 100);
        a.destination_dirs = vec![("/archive/media", 100)];
        a.bytes = Some(1_000);
        a.moved = Some(100);
        a.copied = Some(0);
        let mut b = atom(48, 200, None, "archived", 50);
        b.destination_dirs = vec![("/archive/media", 50)];
        // Pre-vocabulary rows: bytes and disposition unknown.
        let groups = group_acts(&[a, b], &["/archive"], 3);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].bytes, None);
        assert_eq!(groups[0].moved, None);
        assert_eq!(groups[0].copied, None);
    }

    #[test]
    fn observed_deletions_stay_apart_from_performed_acts() {
        let mut observed = atom(70, 100, None, "deleted", 1204);
        observed.observed = true;
        let performed = atom(71, 200, Some("dupes"), "excluded", 10);
        let groups = group_acts(&[observed, performed], &[], 3);
        assert_eq!(groups.len(), 2);
        assert!(groups[0].observed);
        assert!(!groups[1].observed);
    }

    #[test]
    fn generic_transitions_flow_through_untouched() {
        let atoms = vec![atom(80, 100, Some("changed my mind"), "restored", 12)];
        let groups = group_acts(&atoms, &[], 3);
        assert_eq!(groups[0].transition, "restored");
        assert_eq!(groups[0].files, 12);
    }

    #[test]
    fn groups_order_by_earliest_decision() {
        let mut late = atom(90, 900, None, "excluded", 1);
        late.observed = false;
        let mut early = atom(10, 100, None, "deleted", 2);
        early.observed = true;
        let groups = group_acts(&[late, early], &[], 3);
        assert_eq!(groups[0].transition, "deleted");
        assert_eq!(groups[1].transition, "excluded");
    }

    #[test]
    fn decisions_within_a_group_are_oldest_first() {
        let atoms = vec![
            atom(63, 300, None, "excluded", 1),
            atom(57, 100, None, "excluded", 1),
            atom(61, 200, None, "excluded", 1),
        ];
        let groups = group_acts(&atoms, &[], 3);
        let ids: Vec<i64> = groups[0].decisions.iter().map(|d| d.id).collect();
        assert_eq!(ids, vec![57, 61, 63]);
    }

    // ---- the splitter ----

    use crate::domain::retire::build_account;

    fn src(id: i64, rel: &str, object_id: Option<i64>) -> Source {
        Source {
            id,
            root_id: 1,
            root_path: "/root".to_string(),
            rel_path: rel.to_string(),
            object_id,
            size: 100,
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

    fn stamped(id: i64, rel: &str, object_id: Option<i64>, decision: i64) -> Source {
        Source {
            decision_id: Some(decision),
            ..src(id, rel, object_id)
        }
    }

    fn excluded_src(id: i64, rel: &str, object_id: Option<i64>, decision: i64) -> Source {
        Source {
            excluded: true,
            ..stamped(id, rel, object_id, decision)
        }
    }

    fn note_at(id: i64, rel: &str, text: &str) -> Note {
        Note {
            id,
            root_id: 1,
            rel_path: rel.to_string(),
            text: text.to_string(),
            created_at: id * 100,
        }
    }

    fn extraction(
        decision_id: i64,
        rel_prefix: &str,
        files: i64,
        destination: &str,
    ) -> DecisionExtraction {
        DecisionExtraction {
            decision_id,
            root_id: 1,
            root_path: "/root".to_string(),
            rel_prefix: rel_prefix.to_string(),
            files,
            bytes: Some(files * 100),
            destination_root_id: Some(2),
            destination_path: destination.to_string(),
            disposition: Some(OriginDisposition::Relocated),
        }
    }

    fn dinfo(family: DecisionFamily, created_at: i64, reason: Option<&str>) -> DecisionInfo {
        DecisionInfo {
            family,
            created_at,
            reason: reason.map(str::to_string),
        }
    }

    fn no_dust() -> StoryParams {
        StoryParams {
            dust_floor_files: 0,
            dust_floor_bytes: 0,
            ..StoryParams::default()
        }
    }

    struct Fixture {
        present: Vec<Source>,
        absent: Vec<Source>,
        archived: HashSet<i64>,
        extractions: Vec<DecisionExtraction>,
        operated_scopes: Vec<(i64, String)>,
        decisions: HashMap<i64, DecisionInfo>,
        notes: Vec<Note>,
        archive_locations: HashMap<i64, Vec<String>>,
        bases: Vec<String>,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                present: vec![],
                absent: vec![],
                archived: HashSet::new(),
                extractions: vec![],
                operated_scopes: vec![],
                decisions: HashMap::new(),
                notes: vec![],
                archive_locations: HashMap::new(),
                bases: vec!["/root".to_string(), "/archive".to_string()],
            }
        }

        /// A covered present source: hashed, archived, standing at one or
        /// more archive locations.
        fn covered(&mut self, id: i64, rel: &str, locations: &[&str]) {
            let obj = 1000 + id;
            self.present.push(src(id, rel, Some(obj)));
            self.archived.insert(obj);
            self.archive_locations
                .insert(obj, locations.iter().map(|l| l.to_string()).collect());
        }

        fn build(&self, params: &StoryParams) -> StoryPlace {
            build_places(
                &StoryInputs {
                    present: &self.present,
                    absent: &self.absent,
                    archived: &self.archived,
                    extractions: &self.extractions,
                    operated_scopes: &self.operated_scopes,
                    decisions: &self.decisions,
                    notes: &self.notes,
                    archive_locations: &self.archive_locations,
                    bases: &self.bases,
                },
                params,
            )
        }
    }

    fn child_paths(place: &StoryPlace) -> Vec<&str> {
        place.children.iter().map(|c| c.rel_path.as_str()).collect()
    }

    #[test]
    fn empty_root_is_a_bare_root_place() {
        let fx = Fixture::new();
        let root = fx.build(&no_dust());
        assert_eq!(root.rel_path, "");
        assert!(root.undecided());
        assert!(root.standing.is_empty());
        assert!(root.children.is_empty());
        assert_eq!(root.folder_breadth, 0);
    }

    #[test]
    fn uniform_undecided_tree_merges_to_one_line() {
        let mut fx = Fixture::new();
        fx.covered(1, "a/x.jpg", &["/archive/media/x.jpg"]);
        fx.covered(2, "a/y.jpg", &["/archive/media/y.jpg"]);
        fx.covered(3, "b/z.jpg", &["/archive/media/z.jpg"]);
        let root = fx.build(&no_dust());
        assert!(root.children.is_empty());
        assert_eq!(root.standing.covered, 3);
        assert!(root.undecided());
        assert_eq!(root.folder_breadth, 2, "a and b merged into the root line");
        assert_eq!(
            root.covered_where.locations,
            vec![LocationCount {
                path: "/archive/media".into(),
                files: 3
            }]
        );
    }

    #[test]
    fn covered_where_divergence_splits_the_minecraft_case() {
        let mut fx = Fixture::new();
        fx.covered(
            1,
            "home/minecraft/world1/level.dat",
            &["/archive/staging-2019/worlds/level.dat"],
        );
        fx.covered(
            2,
            "home/minecraft/world1/region.mca",
            &["/archive/staging-2019/worlds/region.mca"],
        );
        fx.covered(3, "home/photos/a.jpg", &["/archive/media/a.jpg"]);
        fx.covered(4, "home/photos/b.jpg", &["/archive/media/b.jpg"]);
        let root = fx.build(&no_dust());
        // `home` blends both stories; its children diverge and surface as
        // pockets — children of the root place, not of an unemitted `home`.
        assert_eq!(child_paths(&root), vec!["home/minecraft", "home/photos"]);
        let minecraft = &root.children[0];
        assert!(minecraft.undecided());
        assert_eq!(minecraft.standing.covered, 2);
        assert_eq!(
            minecraft.covered_where.locations[0].path,
            "/archive/staging-2019/worlds"
        );
        assert_eq!(root.standing.covered, 0, "everything attributed deeper");
    }

    #[test]
    fn standing_divergence_splits() {
        let mut fx = Fixture::new();
        fx.covered(1, "a/x.jpg", &["/archive/media/x.jpg"]);
        fx.covered(2, "a/y.jpg", &["/archive/media/y.jpg"]);
        fx.covered(3, "a/z.jpg", &["/archive/media/z.jpg"]);
        // Hashed but not archived: unresolved.
        for (i, rel) in ["b/p.raw", "b/q.raw", "b/r.raw"].iter().enumerate() {
            fx.present
                .push(src(10 + i as i64, rel, Some(500 + i as i64)));
        }
        let root = fx.build(&no_dust());
        assert_eq!(child_paths(&root), vec!["a", "b"]);
        assert_eq!(root.children[0].standing.covered, 3);
        assert_eq!(root.children[1].standing.unresolved, 3);
        assert!(root.children[1].covered_where.is_empty());
    }

    #[test]
    fn within_tolerance_children_merge() {
        let mut fx = Fixture::new();
        fx.covered(1, "a/x.jpg", &["/archive/media/x.jpg"]);
        fx.present.push(src(2, "a/y.raw", Some(501)));
        fx.covered(3, "b/z.jpg", &["/archive/media/z.jpg"]);
        fx.present.push(src(4, "b/w.raw", Some(502)));
        let root = fx.build(&no_dust());
        assert!(root.children.is_empty());
        assert_eq!(root.standing.covered, 2);
        assert_eq!(root.standing.unresolved, 2);
    }

    #[test]
    fn dust_floor_lifts_fragments() {
        let mut fx = Fixture::new();
        for i in 0..100 {
            fx.covered(i, &format!("big/f{i}.jpg"), &["/archive/media/f.jpg"]);
        }
        // Strongly divergent but tiny: five unresolved files under the
        // default floors lift into the root line instead of splitting.
        for i in 0..5 {
            fx.present
                .push(src(200 + i, &format!("tiny/t{i}.raw"), Some(600 + i)));
        }
        let root = fx.build(&StoryParams::default());
        assert!(root.children.is_empty());
        assert_eq!(root.standing.covered, 100);
        assert_eq!(root.standing.unresolved, 5);
    }

    #[test]
    fn a_note_forces_a_place() {
        let mut fx = Fixture::new();
        fx.covered(1, "a/x.jpg", &["/archive/media/x.jpg"]);
        fx.covered(2, "b/y.jpg", &["/archive/media/y.jpg"]);
        fx.notes
            .push(note_at(7, "b", "beautiful pictures, still need a home"));
        let root = fx.build(&no_dust());
        assert_eq!(child_paths(&root), vec!["b"]);
        let b = &root.children[0];
        assert_eq!(b.notes.len(), 1);
        assert_eq!(b.notes[0].text, "beautiful pictures, still need a home");
        assert_eq!(b.standing.covered, 1);
        assert!(b.undecided(), "a note is testimony, not a decision stamp");
    }

    #[test]
    fn an_operated_scope_is_an_unconditional_place() {
        let mut fx = Fixture::new();
        fx.present.push(excluded_src(1, "old/setup1.exe", None, 57));
        fx.present.push(excluded_src(2, "old/setup2.exe", None, 57));
        fx.operated_scopes.push((57, "old".to_string()));
        fx.decisions.insert(
            57,
            dinfo(DecisionFamily::Exclude, 100, Some("installer junk")),
        );
        let root = fx.build(&no_dust());
        assert_eq!(child_paths(&root), vec!["old"]);
        let old = &root.children[0];
        assert!(!old.undecided());
        assert_eq!(old.acts.len(), 1);
        assert_eq!(old.acts[0].transition, "excluded");
        assert_eq!(old.acts[0].files, 2);
        assert_eq!(old.standing.excluded, 2);
        let reasons = old.acts[0].reason_summary();
        assert_eq!(reasons.reasons[0].0, "installer junk");
    }

    #[test]
    fn nested_operated_scopes_render_as_containment() {
        let mut fx = Fixture::new();
        fx.extractions.push(extraction(
            42,
            "pictures/italy",
            640,
            "/archive/media/2016-italy",
        ));
        fx.extractions
            .push(extraction(51, "pictures", 4102, "/archive/media/rest"));
        fx.operated_scopes.push((42, "pictures/italy".to_string()));
        fx.operated_scopes.push((51, "pictures".to_string()));
        fx.decisions.insert(
            42,
            dinfo(DecisionFamily::Archive, 100, Some("the Italy trip")),
        );
        fx.decisions
            .insert(51, dinfo(DecisionFamily::Archive, 200, None));
        fx.covered(
            9,
            "pictures/italy/leftover.jpg",
            &["/archive/media/2016-italy/leftover.jpg"],
        );
        let root = fx.build(&no_dust());
        assert_eq!(child_paths(&root), vec!["pictures"]);
        let pictures = &root.children[0];
        assert_eq!(pictures.acts.len(), 1);
        assert_eq!(pictures.acts[0].files, 4102);
        assert_eq!(
            pictures.acts[0].destination.locations[0].path,
            "/archive/media/rest"
        );
        assert_eq!(child_paths(pictures), vec!["pictures/italy"]);
        let italy = &pictures.children[0];
        assert_eq!(italy.acts[0].files, 640);
        assert_eq!(italy.acts[0].moved, Some(640));
        assert_eq!(
            italy.standing.covered, 1,
            "deepest-match: the leftover belongs to italy, not pictures"
        );
        let reasons = italy.acts[0].reason_summary();
        assert_eq!(reasons.reasons[0].0, "the Italy trip");
    }

    #[test]
    fn a_multi_scope_decision_anchors_at_the_lca() {
        let mut fx = Fixture::new();
        fx.present.push(excluded_src(1, "a/b/x.tmp", None, 60));
        fx.present.push(excluded_src(2, "a/c/y.tmp", None, 60));
        fx.operated_scopes.push((60, "a/b".to_string()));
        fx.operated_scopes.push((60, "a/c".to_string()));
        fx.decisions
            .insert(60, dinfo(DecisionFamily::Exclude, 100, None));
        let root = fx.build(&no_dust());
        assert_eq!(child_paths(&root), vec!["a"]);
        assert_eq!(root.children[0].acts[0].files, 2);
    }

    #[test]
    fn a_global_decision_anchors_at_the_stamped_lca() {
        let mut fx = Fixture::new();
        fx.present.push(excluded_src(1, "d/x.tmp", None, 61));
        fx.present.push(excluded_src(2, "d/y.tmp", None, 61));
        fx.decisions
            .insert(61, dinfo(DecisionFamily::Exclude, 100, None));
        let root = fx.build(&no_dust());
        assert_eq!(child_paths(&root), vec!["d"]);
        assert_eq!(root.children[0].acts[0].transition, "excluded");
    }

    #[test]
    fn an_observed_deletion_lands_where_it_happened() {
        let mut fx = Fixture::new();
        fx.absent.push(stamped(1, "gone/a.jpg", None, 70));
        fx.absent.push(stamped(2, "gone/b.jpg", None, 70));
        // The scan operated at the root; the loss happened at `gone`.
        fx.operated_scopes.push((70, String::new()));
        fx.decisions
            .insert(70, dinfo(DecisionFamily::Observe, 100, None));
        let root = fx.build(&no_dust());
        assert_eq!(child_paths(&root), vec!["gone"]);
        let gone = &root.children[0];
        assert_eq!(gone.acts.len(), 1);
        assert_eq!(gone.acts[0].transition, "deleted");
        assert!(gone.acts[0].observed);
        assert_eq!(gone.acts[0].files, 2);
        assert_eq!(gone.standing.missing_unexplained, 0);
    }

    #[test]
    fn missing_without_a_stamp_is_standing_not_an_act() {
        let mut fx = Fixture::new();
        fx.absent.push(src(1, "lost/x.jpg", None));
        let root = fx.build(&no_dust());
        assert!(root.children.is_empty());
        assert!(root.undecided());
        assert_eq!(root.standing.missing_unexplained, 1);
    }

    #[test]
    fn copies_count_per_location_never_per_item() {
        let mut fx = Fixture::new();
        // Two copies in one archive dir count that dir once for this file.
        fx.covered(
            1,
            "m/x.jpg",
            &[
                "/archive/a/x.jpg",
                "/archive/a/x-copy.jpg",
                "/archive/b/x.jpg",
            ],
        );
        let root = fx.build(&no_dust());
        let paths: Vec<(&str, i64)> = root
            .covered_where
            .locations
            .iter()
            .map(|l| (l.path.as_str(), l.files))
            .collect();
        assert_eq!(paths, vec![("/archive/a", 1), ("/archive/b", 1)]);
    }

    #[test]
    fn agreement_law_place_sums_fold_to_the_account() {
        let mut fx = Fixture::new();
        fx.covered(1, "a/x.jpg", &["/archive/media/x.jpg"]);
        fx.covered(2, "a/y.jpg", &["/archive/media/y.jpg"]);
        fx.covered(3, "a/z.jpg", &["/archive/media/z.jpg"]);
        fx.present.push(excluded_src(10, "b/i.exe", None, 57));
        fx.present.push(excluded_src(11, "b/j.exe", None, 57));
        fx.present.push(src(12, "c/u.raw", Some(700)));
        fx.present.push(src(13, "c/v.raw", None));
        fx.absent.push(stamped(20, "gone/a.jpg", None, 70));
        fx.absent.push(stamped(21, "gone/b.jpg", None, 70));
        fx.absent.push(src(22, "lost/w.jpg", None));
        fx.extractions
            .push(extraction(42, "a", 5, "/archive/media"));
        fx.operated_scopes.push((57, "b".to_string()));
        fx.operated_scopes.push((42, "a".to_string()));
        fx.decisions
            .insert(57, dinfo(DecisionFamily::Exclude, 100, None));
        fx.decisions
            .insert(70, dinfo(DecisionFamily::Observe, 200, None));
        fx.decisions
            .insert(42, dinfo(DecisionFamily::Archive, 300, None));
        let root = fx.build(&no_dust());

        fn fold(place: &StoryPlace, sum: &mut PlaceStanding) {
            sum.covered += place.standing.covered;
            sum.excluded += place.standing.excluded;
            sum.unresolved += place.standing.unresolved;
            sum.unhashed_unresolved += place.standing.unhashed_unresolved;
            sum.missing_unexplained += place.standing.missing_unexplained;
            for child in &place.children {
                fold(child, sum);
            }
        }
        let mut sum = PlaceStanding::default();
        fold(&root, &mut sum);

        let stamp_families: HashMap<i64, DecisionFamily> = fx
            .decisions
            .iter()
            .map(|(id, info)| (*id, info.family))
            .collect();
        let account = build_account(
            &fx.present,
            &fx.absent,
            &fx.archived,
            &fx.extractions,
            &stamp_families,
        );
        assert_eq!(sum.covered, account.covered);
        assert_eq!(sum.excluded, account.excluded);
        assert_eq!(sum.unresolved, account.unresolved);
        assert_eq!(sum.unhashed_unresolved, account.unhashed_unresolved);
        assert_eq!(sum.missing_unexplained, account.unexplained_missing);
    }
}
