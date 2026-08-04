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

/// Named calibratable constants (the sweep discipline). Defaults were
/// calibrated against the real archive 2026-08-04 across three root shapes
/// (session journal `.claude/specs/2026-08-04-story-calibration-journal.md`);
/// recalibration changes constants, never code paths.
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
            signature_tolerance: 0.15,
            dust_floor_files: 20,
            dust_floor_bytes: 5_000_000,
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
    aggregate_impl(dirs, bases, cap, false)
}

/// The observed-scatter variant ("copies stand in"): a legible one-line
/// answer descends one level when its branch groups fit the cap — a huge
/// hub node ("Archive/Media") can hide a short, far more informative list.
/// One step, never recursion. Chosen destinations keep the one-line answer
/// (the arrow states a choice; the scatter is what nobody chose).
pub fn aggregate_locations_expanded(
    dirs: &[(&str, i64)],
    bases: &[&str],
    cap: usize,
) -> LocationAggregate {
    aggregate_impl(dirs, bases, cap, true)
}

fn aggregate_impl(
    dirs: &[(&str, i64)],
    bases: &[&str],
    cap: usize,
    expand: bool,
) -> LocationAggregate {
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
    let branch_groups = |top: u32| -> Vec<LocationCount> {
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
        groups
    };

    let legible = bases
        .iter()
        .any(|base| top_path != *base && path_is_under(top_path, base));
    if legible {
        // The widest honest one-line where — unless the caller asked for
        // the expanded form and the hub's branch groups fit the cap whole,
        // in which case the short list is the more informative answer.
        if expand {
            let groups = branch_groups(top);
            if groups.len() > 1 && groups.len() <= cap {
                return LocationAggregate {
                    locations: groups,
                    omitted_locations: 0,
                };
            }
        }
        return LocationAggregate {
            locations: vec![LocationCount {
                path: top_path.to_string(),
                files: sub[top as usize],
            }],
            omitted_locations: 0,
        };
    }

    // Vacuous common node: emit its branch groups instead.
    let mut groups = branch_groups(top);
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
    /// How many of this slice's files still stand (present rows) — the
    /// stamp accumulator's present split. Extraction and deletion atoms
    /// contribute 0: nothing they narrate stands here. Feeds the
    /// coincidence predicate, never a rendered count.
    pub present_files: i64,
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
    /// Whether the full reason renders here. Defaults `true` (every slice
    /// states its reason); `assign_reason_sites` narrows to the decision's
    /// first emitted slice in reading order — the other slices cite the
    /// bare id.
    pub reason_here: bool,
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
    /// Present-file share of the group's slices (see `ActAtom`); the
    /// coincidence predicate's evidence, never a rendered count.
    pub present_files: i64,
    /// All-or-omitted: `Some` only when every grouped decision knew.
    pub bytes: Option<i64>,
    pub moved: Option<i64>,
    pub copied: Option<i64>,
    pub decisions: Vec<ActDecision>,
}

/// The whys of an act group, ready to render: distinct reasons in
/// first-seen order with the decisions that gave them, the reasoned
/// decisions whose full text renders elsewhere (cited by bare id), and the
/// count that recorded none. `cited` and `without_reason` never conflate —
/// `without_reason` stays an exact truth-claim about decisions with no
/// reason anywhere.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReasonSummary {
    pub reasons: Vec<(String, Vec<i64>)>,
    pub cited: Vec<i64>,
    pub without_reason: usize,
}

impl ActGroup {
    pub fn reason_summary(&self) -> ReasonSummary {
        let mut reasons: Vec<(String, Vec<i64>)> = Vec::new();
        let mut cited = Vec::new();
        let mut without_reason = 0;
        for decision in &self.decisions {
            match &decision.reason {
                Some(_) if !decision.reason_here => cited.push(decision.id),
                Some(reason) => match reasons.iter_mut().find(|(r, _)| r == reason) {
                    Some((_, ids)) => ids.push(decision.id),
                    None => reasons.push((reason.clone(), vec![decision.id])),
                },
                None => without_reason += 1,
            }
        }
        ReasonSummary {
            reasons,
            cited,
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
        present_files: i64,
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
                present_files: 0,
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
        accum.present_files += atom.present_files;
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
            reason_here: true,
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
                present_files: accum.present_files,
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
    /// Subset of `covered`: zero-byte files — contentless, so they claim no
    /// locations (the book's `locations_for` gate). Lets the rendering say
    /// why a covered line names no where instead of staying silently bare.
    pub covered_empty: i64,
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

    /// The standing register says nothing the act register hasn't: every
    /// other bucket is zero and the excluded standing is exactly what the
    /// excluded-transition performed acts narrate — same count, all still
    /// standing. Only the excluded line is ever coincidence-omittable —
    /// covered/unresolved/missing are spec-protected — and the match is
    /// exact both ways: unaccounted standing (a stampless exclusion) fails
    /// the sum, and a tombstone-carrying slice fails it too (the act's
    /// whole-history count exceeds what stands, so omitting the standing
    /// line would misread as all of it still standing).
    pub fn standing_coincides(&self) -> bool {
        if self.standing.covered != 0
            || self.standing.unresolved != 0
            || self.standing.missing_unexplained != 0
        {
            return false;
        }
        let excluded_word = fate_transition(DecisionFamily::Exclude, FateAspect::Present)
            .expect("exclude/present is a registered transition")
            .as_str();
        let mut acted_files = 0i64;
        let mut acted_present = 0i64;
        for group in &self.acts {
            if group.transition == excluded_word && !group.observed {
                acted_files += group.files;
                acted_present += group.present_files;
            }
        }
        self.standing.excluded == acted_present && acted_present == acted_files
    }
}

/// Site each reasoned decision's full reason at its first emitted slice in
/// pre-order (= render order; children are `rel_path`-sorted) and mark
/// every other slice as a bare-id citation — the reader meets the full
/// reason the first time they meet the id, and every later cite is a
/// backward reference (a widest-slice site was tried and read wrong: the
/// reason landed deep in the tree while the top-of-story slices cited
/// forward to it). A post-pass over the built tree, deliberately: the fold
/// composes first, so the site is an *emitted* slice, and the once-rule is
/// precomputed — never render-order-coupled.
pub fn assign_reason_sites(root: &mut StoryPlace) {
    // Pass 1: per reasoned decision, the pre-order index of its first slice.
    fn collect(place: &StoryPlace, index: &mut usize, sites: &mut HashMap<i64, usize>) {
        let my_index = *index;
        *index += 1;
        for group in &place.acts {
            for decision in &group.decisions {
                if decision.reason.is_some() {
                    sites.entry(decision.id).or_insert(my_index);
                }
            }
        }
        for child in &place.children {
            collect(child, index, sites);
        }
    }
    // Pass 2: mark each slice — the site renders the reason, the rest cite.
    fn apply(place: &mut StoryPlace, index: &mut usize, sites: &HashMap<i64, usize>) {
        let my_index = *index;
        *index += 1;
        for group in &mut place.acts {
            for decision in &mut group.decisions {
                if let Some(site) = sites.get(&decision.id) {
                    decision.reason_here = *site == my_index;
                }
            }
        }
        for child in &mut place.children {
            apply(child, index, sites);
        }
    }
    let mut sites = HashMap::new();
    collect(root, &mut 0, &mut sites);
    apply(root, &mut 0, &sites);
}

/// Per-node accumulation for the walk: direct, then subtree by a reverse
/// pass (ids are parents-first).
#[derive(Debug, Clone, Copy, Default)]
struct Counts {
    covered: i64,
    covered_empty: i64,
    excluded: i64,
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
        self.covered += other.covered;
        self.covered_empty += other.covered_empty;
        self.excluded += other.excluded;
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
        self.covered + self.unresolved + self.missing + self.deleted
    }

    /// Proportions within the question population, plus question density
    /// over the whole population — a child far more question-dense than its
    /// context diverges even when the question mix matches.
    fn question_proportions(&self) -> [f64; 5] {
        let q = self.question() as f64;
        [
            self.covered as f64 / q,
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
        match classify_present(source, inputs.archived) {
            StandingBucket::Covered => {
                counts.covered += 1;
                if source.size == 0 {
                    counts.covered_empty += 1;
                }
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
    // old-home-dotfolder over-emission caught on first smoke test).
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
        accum.standing.covered_empty += counts.covered_empty;
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
    fn expanded_form_descends_into_a_hub_when_the_list_is_short() {
        // "copies stand in Archive/Media" hides a two-entry answer; the
        // expanded form lists the branches when they fit the cap whole.
        let dirs = [
            ("/archive/media/2010/june", 280),
            ("/archive/media/2011/spain", 300),
            ("/archive/media/2011/home", 60),
        ];
        let plain = aggregate_locations(&dirs, &["/archive"], 3);
        assert_eq!(plain.locations[0].path, "/archive/media");
        assert_eq!(plain.locations.len(), 1);

        let expanded = aggregate_locations_expanded(&dirs, &["/archive"], 3);
        assert_eq!(
            expanded.locations,
            vec![
                LocationCount {
                    path: "/archive/media/2011".into(),
                    files: 360
                },
                LocationCount {
                    path: "/archive/media/2010/june".into(),
                    files: 280
                },
            ]
        );
        assert_eq!(expanded.omitted_locations, 0);
    }

    #[test]
    fn expanded_form_keeps_the_hub_when_branches_exceed_the_cap() {
        let dirs: Vec<(String, i64)> = (0..5)
            .map(|i| (format!("/archive/media/{}", 2010 + i), 10))
            .collect();
        let refs: Vec<(&str, i64)> = dirs.iter().map(|(d, f)| (d.as_str(), *f)).collect();
        let expanded = aggregate_locations_expanded(&refs, &["/archive"], 3);
        assert_eq!(
            expanded.locations,
            vec![LocationCount {
                path: "/archive/media".into(),
                files: 50
            }],
            "five branches never truncate to three — the hub stays the one honest line"
        );
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
            present_files: 0,
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
    fn a_reasoned_exclusion_forces_its_place() {
        let mut fx = Fixture::new();
        fx.present.push(excluded_src(1, "old/setup1.exe", None, 57));
        fx.present.push(excluded_src(2, "old/setup2.exe", None, 57));
        fx.decisions.insert(
            57,
            dinfo(DecisionFamily::Exclude, 100, Some("installer junk")),
        );
        let root = fx.build(&no_dust());
        // The care anchor: the reasoned decision's dirs LCA is `old`.
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
    fn a_reasonless_uniform_exclusion_folds_to_the_root() {
        let mut fx = Fixture::new();
        fx.present.push(excluded_src(1, "old/setup1.exe", None, 57));
        fx.present.push(excluded_src(2, "old/setup2.exe", None, 57));
        fx.decisions
            .insert(57, dinfo(DecisionFamily::Exclude, 100, None));
        let root = fx.build(&no_dust());
        // No care, no divergence: the slice folds to the root line.
        assert!(root.children.is_empty());
        assert_eq!(root.acts.len(), 1);
        assert_eq!(root.acts[0].transition, "excluded");
        assert_eq!(root.acts[0].files, 2);
        assert_eq!(root.standing.excluded, 2);
    }

    #[test]
    fn nested_reasoned_acts_render_as_containment() {
        let mut fx = Fixture::new();
        fx.extractions.push(extraction(
            42,
            "pictures/italy",
            640,
            "/archive/media/2016-italy",
        ));
        fx.extractions
            .push(extraction(51, "pictures", 4102, "/archive/media/rest"));
        fx.decisions.insert(
            42,
            dinfo(DecisionFamily::Archive, 100, Some("the Italy trip")),
        );
        fx.decisions.insert(
            51,
            dinfo(
                DecisionFamily::Archive,
                200,
                Some("rest of the pictures, mechanical"),
            ),
        );
        fx.covered(
            9,
            "pictures/italy/leftover.jpg",
            &["/archive/media/2016-italy/leftover.jpg"],
        );
        let root = fx.build(&no_dust());
        // The onion: both reasoned acts force their care anchors, which are
        // their origin dirs — the layered passes read as layers.
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
    fn a_reasoned_decision_forces_the_lca_of_its_dirs() {
        let mut fx = Fixture::new();
        fx.present.push(excluded_src(1, "a/b/x.tmp", None, 60));
        fx.present.push(excluded_src(2, "a/c/y.tmp", None, 60));
        fx.decisions
            .insert(60, dinfo(DecisionFamily::Exclude, 100, Some("temp litter")));
        let root = fx.build(&no_dust());
        // Care was expressed at the decision's grain: one forced place at
        // the dirs' LCA, the per-dir slices merged into one line there.
        assert_eq!(child_paths(&root), vec!["a"]);
        let a = &root.children[0];
        assert!(a.children.is_empty());
        assert_eq!(a.acts.len(), 1);
        assert_eq!(a.acts[0].files, 2);
        assert_eq!(
            a.acts[0].decisions.len(),
            1,
            "slices merged, not duplicated"
        );
    }

    #[test]
    fn an_observed_deletion_lands_where_it_happened() {
        let mut fx = Fixture::new();
        fx.absent.push(stamped(1, "gone/a.jpg", None, 70));
        fx.absent.push(stamped(2, "gone/b.jpg", None, 70));
        fx.decisions
            .insert(70, dinfo(DecisionFamily::Observe, 100, None));
        // Divergent surroundings: the loss reads as `gone`'s story, not the
        // root's (deleted rows join the story population).
        fx.covered(10, "kept/a.jpg", &["/archive/media/a.jpg"]);
        fx.covered(11, "kept/b.jpg", &["/archive/media/b.jpg"]);
        let root = fx.build(&no_dust());
        assert_eq!(child_paths(&root), vec!["gone", "kept"]);
        let gone = &root.children[0];
        assert_eq!(gone.acts.len(), 1);
        assert_eq!(gone.acts[0].transition, "deleted");
        assert!(gone.acts[0].observed);
        assert_eq!(gone.acts[0].files, 2);
        assert_eq!(gone.standing.missing_unexplained, 0);
    }

    #[test]
    fn slice_sum_law_reconciles_through_any_fold() {
        // One apply drew from three dirs; the emission shape must never
        // change the totals. Distinct reasons keep groups one-decision so
        // per-decision files are readable from the built tree.
        let mut fx = Fixture::new();
        fx.extractions
            .push(extraction(42, "a", 5, "/archive/media/set"));
        fx.extractions
            .push(extraction(42, "a/b", 3, "/archive/media/set"));
        fx.extractions
            .push(extraction(42, "c", 2, "/archive/media/set"));
        fx.decisions.insert(
            42,
            dinfo(DecisionFamily::Archive, 100, Some("the whole set")),
        );
        // Divergent standings force `a` out while `c` folds to the root.
        for i in 0..20 {
            fx.covered(100 + i, &format!("a/k{i}.jpg"), &["/archive/media/k.jpg"]);
        }
        for i in 0..20 {
            fx.present
                .push(src(200 + i, &format!("z/u{i}.raw"), Some(900 + i)));
        }
        let root = fx.build(&no_dust());

        fn slice_files(place: &StoryPlace, id: i64, sum: &mut i64) {
            for group in &place.acts {
                if group.decisions.iter().any(|d| d.id == id) {
                    *sum += group.files;
                }
            }
            for child in &place.children {
                slice_files(child, id, sum);
            }
        }
        let mut total = 0;
        slice_files(&root, 42, &mut total);
        assert_eq!(total, 10, "slices reconcile exactly to the row totals");

        // And the shape: `a` split out (care anchor at `a` — the LCA of the
        // decision's dirs — plus covered divergence), holding its merged
        // slice; the `c` slice folded to the root line.
        let a = root
            .children
            .iter()
            .find(|p| p.rel_path == "a")
            .expect("a splits");
        let a_slice: i64 = a.acts.iter().map(|g| g.files).sum();
        assert_eq!(a_slice, 8, "a's dirs merged into one slice");
        let root_slice: i64 = root.acts.iter().map(|g| g.files).sum();
        assert_eq!(root_slice, 2, "c's slice folded to the root");
    }

    #[test]
    fn no_decision_here_never_lies() {
        // The `.cache` contradiction: a child uniformly excluded by a
        // decision stamped in its own dirs must either fold into a register
        // that shows the act, or show its slice — never render undecided
        // beside excluded standing.
        let mut fx = Fixture::new();
        for i in 0..20 {
            fx.present
                .push(excluded_src(i, &format!("home/.cache/c{i}.tmp"), None, 114));
        }
        fx.decisions
            .insert(114, dinfo(DecisionFamily::Exclude, 100, None));
        for i in 0..20 {
            fx.covered(
                100 + i,
                &format!("home/pics/p{i}.jpg"),
                &["/archive/m/p.jpg"],
            );
        }
        let root = fx.build(&no_dust());

        fn assert_honest(place: &StoryPlace) {
            if place.undecided() {
                assert_eq!(
                    place.standing.excluded, 0,
                    "{}: no decision here beside excluded standing",
                    place.rel_path
                );
            }
            for child in &place.children {
                assert_honest(child);
            }
        }
        assert_honest(&root);
    }

    #[test]
    fn reasonless_mechanical_exclusions_fold_ids_enumerated() {
        // The dotfolder wall: three sibling folders excluded by three
        // different reasonless decisions tell the same story — one register,
        // every id enumerated.
        let mut fx = Fixture::new();
        for i in 0..4 {
            fx.present
                .push(excluded_src(i, &format!("home/.cache/c{i}.tmp"), None, 114));
        }
        for i in 0..3 {
            fx.present.push(excluded_src(
                10 + i,
                &format!("home/.opera/o{i}.dat"),
                None,
                115,
            ));
        }
        for i in 0..2 {
            fx.present.push(excluded_src(
                20 + i,
                &format!("home/.purple/p{i}.xml"),
                None,
                116,
            ));
        }
        for id in [114, 115, 116] {
            fx.decisions
                .insert(id, dinfo(DecisionFamily::Exclude, id * 10, None));
        }
        // Divergent context so `home` earns its own line.
        for i in 0..10 {
            fx.covered(100 + i, &format!("pics/p{i}.jpg"), &["/archive/m/p.jpg"]);
        }
        let root = fx.build(&no_dust());
        // Nothing about `home` is second-guessable (uniformly excluded, one
        // shared signature), so the wall folds all the way into the root's
        // register; only the covered context splits out.
        assert_eq!(child_paths(&root), vec!["pics"]);
        assert_eq!(root.acts.len(), 1, "one shared register");
        let group = &root.acts[0];
        assert_eq!(group.files, 9);
        let ids: Vec<i64> = group.decisions.iter().map(|d| d.id).collect();
        assert_eq!(ids, vec![114, 115, 116], "every id enumerated");
        assert_eq!(group.reason_summary().without_reason, 3);
    }

    #[test]
    fn same_story_children_fold_against_the_residual_context() {
        // A reasoned sibling forced out of the fold (.tvtime) must not
        // dilute the context: the dotfolders' story is compared against
        // what they would fold into — the parent's residual register —
        // not the whole subtree (the old-home over-emission).
        let mut fx = Fixture::new();
        for i in 0..5 {
            fx.present
                .push(excluded_src(i, &format!("old-home/k{i}.dat"), None, 98));
        }
        for i in 0..20 {
            fx.present.push(excluded_src(
                10 + i,
                &format!("old-home/.cache/c{i}"),
                None,
                98,
            ));
        }
        for i in 0..15 {
            fx.present.push(excluded_src(
                40 + i,
                &format!("old-home/.compiz/z{i}"),
                None,
                98,
            ));
        }
        fx.decisions.insert(
            98,
            dinfo(
                DecisionFamily::Exclude,
                100,
                Some("nothing important remains here"),
            ),
        );
        for i in 0..6 {
            fx.present.push(excluded_src(
                60 + i,
                &format!("old-home/.tvtime/t{i}"),
                None,
                97,
            ));
        }
        fx.decisions.insert(
            97,
            dinfo(
                DecisionFamily::Exclude,
                90,
                Some("channels worth remembering"),
            ),
        );
        let root = fx.build(&no_dust());
        let old-home = root
            .children
            .iter()
            .find(|p| p.rel_path == "old-home")
            .expect("the care anchor forces old-home");
        assert_eq!(
            child_paths(old-home),
            vec!["old-home/.tvtime"],
            "only the differently-reasoned act splits; same-story dotfolders fold"
        );
        let register: i64 = old-home.acts.iter().map(|g| g.files).sum();
        assert_eq!(register, 40, "the folded slices land in old-home's register");
    }

    #[test]
    fn a_note_on_a_file_gathers_that_files_fate() {
        // The noted-script finding: a noted file is its own place, and
        // that place carries the file's standing and act slice — intent and
        // fate side by side, discrepancies visible without the trail.
        let mut fx = Fixture::new();
        fx.present
            .push(excluded_src(1, "usr/local/bin/keepme.sh", None, 153));
        for i in 0..10 {
            fx.present.push(excluded_src(
                10 + i,
                &format!("usr/local/bin/b{i}"),
                None,
                153,
            ));
        }
        fx.decisions.insert(
            153,
            dinfo(
                DecisionFamily::Exclude,
                100,
                Some("non-package files are not important"),
            ),
        );
        fx.notes
            .push(note_at(7, "usr/local/bin/keepme.sh", "important script"));
        let root = fx.build(&no_dust());

        fn find<'a>(place: &'a StoryPlace, rel: &str) -> Option<&'a StoryPlace> {
            if place.rel_path == rel {
                return Some(place);
            }
            place.children.iter().find_map(|c| find(c, rel))
        }
        let script =
            find(&root, "usr/local/bin/keepme.sh").expect("the noted file is its own place");
        assert_eq!(script.standing.excluded, 1, "the file's standing sits here");
        assert_eq!(script.acts.len(), 1, "and its act slice");
        assert_eq!(script.acts[0].transition, "excluded");
        assert_eq!(script.acts[0].files, 1);
        assert_eq!(script.notes[0].text, "important script");
    }

    #[test]
    fn a_note_on_a_dir_gathers_the_subtree_it_alone_claims() {
        // With no deeper place forced, a noted dir is the deepest emitted
        // ancestor of everything beneath it — standings and act slices from
        // the whole subtree render at the noted place.
        let mut fx = Fixture::new();
        fx.present
            .push(excluded_src(1, "keep/sub/tool.sh", None, 153));
        fx.decisions
            .insert(153, dinfo(DecisionFamily::Exclude, 100, None));
        fx.notes.push(note_at(7, "keep", "the tool lives here"));
        // Divergent surroundings so the root context differs.
        for i in 0..10 {
            fx.covered(100 + i, &format!("pics/p{i}.jpg"), &["/archive/m/p.jpg"]);
        }
        let root = fx.build(&no_dust());
        let keep = root
            .children
            .iter()
            .find(|p| p.rel_path == "keep")
            .expect("the note forces keep");
        assert!(keep.children.is_empty());
        assert_eq!(
            keep.standing.excluded, 1,
            "the subtree's standing gathers here"
        );
        assert_eq!(keep.acts.len(), 1, "and its act slice");
        assert_eq!(keep.acts[0].files, 1);
    }

    #[test]
    fn disjoint_reasoned_footprints_surface_as_crossing_boundaries() {
        // Two reasoned exclusions with coherent, disjoint footprints: the
        // care anchors land at each decision's own dirs-LCA — the map shows
        // exactly the boundary where the decisions cross, each place with
        // its reason.
        let mut fx = Fixture::new();
        for i in 0..20 {
            fx.present
                .push(excluded_src(i, &format!("usr/sgml/4.0/f{i}"), None, 131));
        }
        for i in 0..20 {
            fx.present.push(excluded_src(
                30 + i,
                &format!("usr/sgml/4.1/g{i}"),
                None,
                153,
            ));
        }
        fx.decisions.insert(
            131,
            dinfo(
                DecisionFamily::Exclude,
                100,
                Some("only the script matters"),
            ),
        );
        fx.decisions.insert(
            153,
            dinfo(
                DecisionFamily::Exclude,
                200,
                Some("non-package files checked"),
            ),
        );
        let root = fx.build(&no_dust());
        assert_eq!(
            child_paths(&root),
            vec!["usr/sgml/4.0", "usr/sgml/4.1"],
            "each decision's footprint is its own boundary"
        );
        let r40 = root.children[0].acts[0].reason_summary();
        assert_eq!(r40.reasons[0].0, "only the script matters");
        let r41 = root.children[1].acts[0].reason_summary();
        assert_eq!(r41.reasons[0].0, "non-package files checked");
    }

    #[test]
    fn interleaved_scattered_acts_share_one_register() {
        // The same two decisions with scattered, interleaved footprints
        // (the data/usr sgml wall): there is no clean crossing line — both
        // care anchors land on the shared region, dirs holding a slice of
        // only one decision tell the same what/where story and fold, and
        // both reasons render in the one register. The why is deliberately
        // not a divergence axis; it surfaces at each decision's own grain
        // via the care anchor.
        let mut fx2 = Fixture::new();
        for (i, dir) in ["usr/a", "usr/b", "usr/c"].iter().enumerate() {
            for j in 0..10 {
                fx2.present.push(excluded_src(
                    (i * 10 + j) as i64,
                    &format!("{dir}/f{j}"),
                    None,
                    131,
                ));
            }
            for j in 0..10 {
                fx2.present.push(excluded_src(
                    (100 + i * 10 + j) as i64,
                    &format!("{dir}/sub/g{j}"),
                    None,
                    153,
                ));
            }
        }
        fx2.decisions.insert(
            131,
            dinfo(
                DecisionFamily::Exclude,
                100,
                Some("only the script matters"),
            ),
        );
        fx2.decisions.insert(
            153,
            dinfo(
                DecisionFamily::Exclude,
                200,
                Some("non-package files checked"),
            ),
        );
        let root2 = fx2.build(&no_dust());
        // Both decisions' dirs LCA to `usr` — one forced place, no
        // fragmentation beneath, both reasons in its register.
        assert_eq!(child_paths(&root2), vec!["usr"]);
        let usr = &root2.children[0];
        assert!(usr.children.is_empty(), "interleaved slices fold");
        assert_eq!(usr.acts.len(), 1, "one shared what/where register");
        let summary = usr.acts[0].reason_summary();
        assert_eq!(summary.reasons.len(), 2, "both whys enumerated");
    }

    #[test]
    fn reasoned_act_forces_below_dust() {
        // `data/.deb`: one reasoned file far under the floors still
        // surfaces — recorded care earns a line, floors notwithstanding.
        let mut fx = Fixture::new();
        fx.present
            .push(excluded_src(1, "data/.deb/stray.deb", None, 161));
        fx.decisions
            .insert(161, dinfo(DecisionFamily::Exclude, 100, Some("stray file")));
        for i in 0..100 {
            fx.present
                .push(excluded_src(10 + i, &format!("data/bin/b{i}"), None, 157));
        }
        fx.decisions
            .insert(157, dinfo(DecisionFamily::Exclude, 90, None));
        let root = fx.build(&StoryParams::default());
        let deb = root
            .children
            .iter()
            .find(|p| p.rel_path == "data/.deb")
            .expect("the reasoned act surfaces despite the dust floors");
        assert_eq!(deb.acts[0].files, 1);
        assert_eq!(deb.acts[0].reason_summary().reasons[0].0, "stray file");
    }

    #[test]
    fn mirror_destination_apply_does_not_fragment() {
        // A reasonless export whose per-dir destinations mirror the origins:
        // the signature's destination answer is decision-level, so the
        // slices share one signature and fold to one line.
        let mut fx = Fixture::new();
        fx.extractions
            .push(extraction(174, "home/a", 2, "/archive/export/home/a"));
        fx.extractions
            .push(extraction(174, "home/b", 3, "/archive/export/home/b"));
        fx.extractions
            .push(extraction(174, "etc/x", 1, "/archive/export/etc/x"));
        fx.decisions
            .insert(174, dinfo(DecisionFamily::Archive, 100, None));
        let root = fx.build(&no_dust());
        assert!(root.children.is_empty(), "uniform story, no fragmentation");
        assert_eq!(root.acts.len(), 1);
        assert_eq!(root.acts[0].files, 6);
        assert_eq!(
            root.acts[0].destination.locations[0].path, "/archive/export",
            "the pooled destination collapses to the common answer"
        );
    }

    #[test]
    fn tombstone_dirs_carry_slices() {
        // An object exclusion stamps an absent sharer in another dir: the
        // act register is whole-history, so that dir narrates the act while
        // its standing stays a missing fact.
        let mut fx = Fixture::new();
        fx.present.push(excluded_src(1, "keep/x.jpg", None, 120));
        fx.absent.push(stamped(2, "gone2/x.jpg", None, 120));
        fx.decisions
            .insert(120, dinfo(DecisionFamily::Exclude, 100, None));
        let root = fx.build(&no_dust());
        let gone2 = root
            .children
            .iter()
            .find(|p| p.rel_path == "gone2")
            .expect("the tombstone dir diverges");
        assert_eq!(gone2.acts.len(), 1, "the tombstone carries the slice");
        assert_eq!(gone2.acts[0].transition, "excluded");
        assert_eq!(gone2.acts[0].files, 1);
        assert_eq!(gone2.standing.excluded, 0, "standing stays present-tense");
        assert_eq!(gone2.standing.missing_unexplained, 1);
    }

    #[test]
    fn emptied_place_dust_uses_act_weight() {
        // The emptied-place shape: a move-mode apply left zero present
        // files, but the act weight carries the place past the floors; a
        // sibling one-file slice under both floors folds instead.
        let mut fx = Fixture::new();
        fx.extractions.push(DecisionExtraction {
            decision_id: 174,
            root_id: 1,
            root_path: "/root".to_string(),
            rel_prefix: "some/important/dir".to_string(),
            files: 2,
            bytes: Some(3_100_000_000),
            destination_root_id: Some(2),
            destination_path: "/archive/export/some/important/dir".to_string(),
            disposition: Some(OriginDisposition::Relocated),
        });
        fx.extractions.push(DecisionExtraction {
            decision_id: 174,
            root_id: 1,
            root_path: "/root".to_string(),
            rel_prefix: "tiny".to_string(),
            files: 1,
            bytes: Some(100),
            destination_root_id: Some(2),
            destination_path: "/archive/export/tiny".to_string(),
            disposition: Some(OriginDisposition::Relocated),
        });
        fx.decisions
            .insert(174, dinfo(DecisionFamily::Archive, 100, None));
        for i in 0..50 {
            fx.present
                .push(excluded_src(10 + i, &format!("data/d{i}"), None, 100));
        }
        fx.decisions
            .insert(100, dinfo(DecisionFamily::Exclude, 50, None));
        let root = fx.build(&StoryParams::default());
        // The boundary settles at the widest honest node of the emptied
        // chain (`some`); in real data a note is what would force the deep
        // dir itself.
        let widest = root
            .children
            .iter()
            .find(|p| p.rel_path == "some")
            .expect("act weight carries the emptied place past the floors");
        assert_eq!(widest.acts[0].files, 2);
        assert!(
            !root.children.iter().any(|p| p.rel_path == "tiny"),
            "a dust-sized slice folds"
        );
        let root_archived: i64 = root
            .acts
            .iter()
            .filter(|g| g.transition == "archived")
            .map(|g| g.files)
            .sum();
        assert_eq!(
            root_archived, 1,
            "the folded slice lands in the root register"
        );
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

    fn child<'a>(root: &'a StoryPlace, rel: &str) -> &'a StoryPlace {
        root.children
            .iter()
            .find(|p| p.rel_path == rel)
            .unwrap_or_else(|| panic!("expected place {rel:?}"))
    }

    #[test]
    fn the_first_slice_in_reading_order_carries_the_reason() {
        // #50's slices weigh 2 at `a` and 3 at `b` — the reason still
        // sites at `a`, the first place in pre-order (= render order): the
        // reader meets the full reason the first time they meet the id,
        // and the wider later slice cites backward.
        let mut fx = Fixture::new();
        for i in 0..2 {
            fx.present
                .push(excluded_src(1 + i, &format!("a/f{i}"), None, 50));
        }
        for i in 0..3 {
            fx.present
                .push(excluded_src(10 + i, &format!("b/f{i}"), None, 50));
        }
        fx.decisions
            .insert(50, dinfo(DecisionFamily::Exclude, 100, Some("junk")));
        fx.notes.push(note_at(1, "a", "watching"));
        fx.notes.push(note_at(2, "b", "watching"));
        let mut root = fx.build(&no_dust());
        assign_reason_sites(&mut root);

        let a = child(&root, "a");
        let b = child(&root, "b");
        assert!(a.acts[0].decisions[0].reason_here, "first in reading order");
        assert!(!b.acts[0].decisions[0].reason_here, "wider, but later");
        let summary = b.acts[0].reason_summary();
        assert!(summary.reasons.is_empty());
        assert_eq!(summary.cited, vec![50]);
        assert_eq!(summary.without_reason, 0, "cited is never without-reason");
    }

    #[test]
    fn reason_site_is_an_emitted_slice() {
        // #70's raw dirs a/x and a/y fold into `a`: the site is the first
        // EMITTED slice in reading order — the post-pass runs over the
        // built tree, never over raw atoms.
        let mut fx = Fixture::new();
        for i in 0..3 {
            fx.present
                .push(excluded_src(1 + i, &format!("a/x/f{i}"), None, 70));
        }
        for i in 0..2 {
            fx.present
                .push(excluded_src(10 + i, &format!("a/y/f{i}"), None, 70));
        }
        for i in 0..4 {
            fx.present
                .push(excluded_src(20 + i, &format!("b/f{i}"), None, 70));
        }
        fx.decisions
            .insert(70, dinfo(DecisionFamily::Exclude, 100, Some("old builds")));
        fx.notes.push(note_at(1, "a", "watching"));
        fx.notes.push(note_at(2, "b", "watching"));
        let mut root = fx.build(&no_dust());
        assign_reason_sites(&mut root);

        let a = child(&root, "a");
        let b = child(&root, "b");
        assert_eq!(a.acts[0].files, 5, "a/x and a/y folded into one slice");
        assert!(a.acts[0].decisions[0].reason_here);
        assert!(!b.acts[0].decisions[0].reason_here);
    }

    #[test]
    fn standing_coincidence_is_exact_both_ways() {
        let excluded_group = |files: i64, present_files: i64| ActGroup {
            transition: fate_transition(DecisionFamily::Exclude, FateAspect::Present)
                .expect("registered")
                .as_str(),
            observed: false,
            destination: LocationAggregate::default(),
            files,
            present_files,
            bytes: None,
            moved: None,
            copied: None,
            decisions: vec![],
        };
        let place = |standing: PlaceStanding, acts: Vec<ActGroup>| StoryPlace {
            rel_path: "old".to_string(),
            acts,
            standing,
            covered_where: LocationAggregate::default(),
            notes: vec![],
            folder_breadth: 1,
            children: vec![],
        };
        let excluded_only = |n: i64| PlaceStanding {
            excluded: n,
            ..PlaceStanding::default()
        };

        // The stutter: standing exactly what the act narrates, all standing.
        assert!(place(excluded_only(2), vec![excluded_group(2, 2)]).standing_coincides());
        // Unaccounted standing (a stampless exclusion) fails the sum.
        assert!(!place(excluded_only(3), vec![excluded_group(2, 2)]).standing_coincides());
        // A tombstone-carrying slice fails: the act's whole-history count
        // exceeds what stands — omitting the standing line would misread.
        assert!(!place(excluded_only(2), vec![excluded_group(3, 2)]).standing_coincides());
        // Any question bucket present renders everything.
        assert!(!place(
            PlaceStanding {
                covered: 1,
                excluded: 2,
                ..PlaceStanding::default()
            },
            vec![excluded_group(2, 2)]
        )
        .standing_coincides());
        // A non-exclusion act never accounts for excluded standing.
        let mut archived = excluded_group(2, 2);
        archived.transition = fate_transition(DecisionFamily::Archive, FateAspect::Present)
            .expect("registered")
            .as_str();
        assert!(!place(excluded_only(2), vec![archived]).standing_coincides());
    }

    #[test]
    fn coincidence_holds_through_the_real_splitter() {
        // Two stamped exclusions and nothing else: the built place's
        // excluded standing is exactly the act's present share.
        let mut fx = Fixture::new();
        fx.present.push(excluded_src(1, "old/a.exe", None, 57));
        fx.present.push(excluded_src(2, "old/b.exe", None, 57));
        fx.decisions
            .insert(57, dinfo(DecisionFamily::Exclude, 100, None));
        let root = fx.build(&no_dust());
        assert_eq!(root.standing.excluded, 2);
        assert_eq!(root.acts[0].present_files, 2);
        assert!(root.standing_coincides());
    }
}
