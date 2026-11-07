//! The two-register resolution account — shared substrate independently
//! computed over by `retire`'s readiness gate and `story`'s place lens.
//! Neither subsystem owns it; both interpret it for their own purposes,
//! which is exactly why it lives here rather than in either.
//!
//! - **The story so far** counts whole-history events: what was archived from
//!   here (extraction-recorded, copies and moves alike — the trail rollup's
//!   established meaning of "archived from here"), what was deleted
//!   (scan-observed), and what is missing without a recorded deletion.
//! - **Standing here now** partitions the present rows: archived from here
//!   (the surviving originals of copy-mode applies — the file the run chose,
//!   by the evidence `FromHereEvidence` carries), covered (copies elsewhere,
//!   nobody chose it), excluded,
//!   contentless (empty files — all shape, no content; stated, never
//!   blocking), unresolved.
//!
//! Copies overlap the two registers deliberately (a file copied to the
//! archive is both "archived from here" and typically standing `covered`);
//! the moved/copied split on the archived line is what makes that overlap
//! readable rather than confusing.
//!
//! `retire`'s own verdict (`Readiness`, derived from this account) and its
//! book-compile fate model stay in `retire/domain.rs` — only one subsystem
//! consumes those.

use std::collections::{BTreeMap, HashMap, HashSet};

use super::extraction::{DecisionExtraction, OriginDisposition};
use super::fate::DecisionFamily;
use crate::core::domain::object::ArchiveCopy;
use crate::core::domain::root::{only_on_suspended_segment, Root};
use crate::core::domain::source::Source;

/// The two-register resolution account of a root.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ResolutionAccount {
    // The story so far (event register, whole-history).
    /// Files archived from this root, extraction-recorded, all dispositions.
    pub archived_files: i64,
    /// All-or-omitted: `None` if any contributing row lacks a size.
    pub archived_bytes: Option<i64>,
    pub archived_moved: i64,
    pub archived_copied: i64,
    /// Files whose extraction rows carry no disposition (pre-vocabulary
    /// records) — the moved/copied split degrades explicitly, never guessed.
    pub archived_unrecorded: i64,
    /// Absent rows whose stamp is the Observe-family decision that witnessed
    /// the loss.
    pub deleted: i64,
    /// Absent rows whose stamp no longer supports a deletion explanation
    /// (recording was off, or a later transition overwrote it) — a
    /// record-quality fact, not a distinct physical state.
    pub unexplained_missing: i64,
    // Standing here now (state register, present rows).
    /// Present rows whose content was archived *from here* and still stands
    /// in the archive — the deliberate act, told apart from mere coverage.
    pub archived_standing: i64,
    pub covered: i64,
    pub excluded: i64,
    /// Empty files — contentless: nothing to cover, nothing to verify;
    /// stated, never silent, never blocking.
    pub contentless: i64,
    pub unresolved: i64,
    /// Subset of `unresolved`: present, non-excluded, never hashed — they
    /// cannot be verified covered, which is exactly why they block.
    pub unhashed_unresolved: i64,
    /// What the `covered` claim rests on, where it rests solely behind
    /// closed doors.
    ///
    /// Not a sixth standing and not a doubt about the evidence — parking
    /// never un-archives, and these rows are covered. It is what the claim
    /// **rests on**, said out loud, because the reader of this account is
    /// usually deciding whether a drive can go.
    pub covered_only_parked: ParkedEvidence,
    /// The same question asked of `archived_standing`, where the claim is
    /// custody rather than coverage: this root's own act put the content in
    /// the archive, and every surviving copy of it is behind a door.
    ///
    /// Kept apart from the covered evidence rather than pooled, because each
    /// line names the roots **its own** claim rests on — a union would let
    /// the covered line name a root only the custody claim stands behind.
    pub archived_standing_only_parked: ParkedEvidence,
    /// Of `archived_standing`, the files held only through the directory
    /// fallback, per decision: a decision whose receipt was not read, the
    /// file named by no receipt (`FromHereEvidence::directory_only_decisions`).
    /// Empty wherever the evidence is the directory itself (the screens). A
    /// file two such decisions hold counts under each, so each decision's
    /// count is true of that decision.
    pub archived_standing_by_directory: BTreeMap<i64, i64>,
}

impl ResolutionAccount {
    /// Present rows, partitioned exactly: archived + covered + excluded +
    /// contentless + unresolved.
    pub fn standing(&self) -> i64 {
        self.archived_standing + self.covered + self.excluded + self.contentless + self.unresolved
    }

    /// Every source ever indexed here: standing + absent + moved away.
    /// `None` when any extraction row lacks a disposition — without it the
    /// moved count is unsupported by the record (omitted, never guessed).
    pub fn ever_indexed(&self) -> Option<i64> {
        if self.archived_unrecorded > 0 {
            return None;
        }
        Some(self.standing() + self.deleted + self.unexplained_missing + self.archived_moved)
    }
}

/// Where a present row stands, by priority excluded > contentless >
/// archived > covered > unresolved. Exclusion first: dismissal is a
/// judgment, and judgment covers shape too. Contentless second, before any
/// identity test (including the hash test — an unhashed empty source is
/// Contentless, never unresolved): identity evidence about no-content is
/// vacuous (the contentless law). Archived over covered: the
/// deliberate act wins mixed evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StandingBucket {
    /// Archived from here and the copy still stands in the archive — the
    /// file the run chose, as `FromHereEvidence` reads it.
    Archived,
    Covered,
    Excluded,
    /// Empty — all shape, no content; carried with its place.
    Contentless,
    Unresolved {
        unhashed: bool,
    },
}

/// What an absent row's stamp still explains.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbsentBucket {
    Deleted,
    Unexplained,
}

/// One archive copy of an object that an archiving run drawing from this
/// root stamped, beside one extraction row of that run — the raw evidence
/// the from-here query returns (`repo::object::batch_check_archived_from_root`).
/// Matching rows to copies is [`place_from_here`]'s, never the SQL's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FromHereCandidate {
    pub object_id: i64,
    pub decision_id: i64,
    /// The row's origin directory under this root.
    pub rel_prefix: String,
    /// The row's recorded destination directory (absolute).
    pub destination_path: String,
    /// Where the stamped copy stands: its root's path and its own `rel_path`.
    pub copy_root_path: String,
    pub copy_rel_path: String,
}

/// A directory an archiving run chose a file's content from, as the index
/// records it — `precise` when the row's destination is the very directory
/// the archive copy stands in, coarse when it only contains it (a row
/// recorded before directory precision).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FromHereDirectory {
    pub decision_id: i64,
    pub rel_prefix: String,
    pub precise: bool,
}

/// Which recorded rows placed each archive copy, per object.
///
/// For each copy, the rows of its stamping run whose destination **is** the
/// copy's directory are precise placements of it; failing those, the rows
/// whose destination **contains** the copy's directory are coarse ones. A
/// copy no row places — moved within the archive after the apply — adds
/// nothing, which reads covered: the safe direction. The copy's directory is
/// spelled the way the row's destination was built (`join_under_root` over
/// `parent_dir`), so a precise match is equality of two values one rule made.
pub fn place_from_here(candidates: &[FromHereCandidate]) -> HashMap<i64, Vec<FromHereDirectory>> {
    use super::extraction::parent_dir;
    use super::path::{join_under_root, path_is_under};

    // One copy stamped by one run: (object, copy root, copy path, decision).
    let mut copies: HashMap<(i64, &str, &str, i64), Vec<&FromHereCandidate>> = HashMap::new();
    for c in candidates {
        copies
            .entry((
                c.object_id,
                c.copy_root_path.as_str(),
                c.copy_rel_path.as_str(),
                c.decision_id,
            ))
            .or_default()
            .push(c);
    }

    let mut placed: HashMap<i64, Vec<FromHereDirectory>> = HashMap::new();
    for ((object_id, root_path, rel_path, decision_id), rows) in copies {
        let copy_dir = join_under_root(root_path, parent_dir(rel_path));
        let precise: Vec<&&FromHereCandidate> = rows
            .iter()
            .filter(|row| row.destination_path == copy_dir)
            .collect();
        let (matching, is_precise) = if precise.is_empty() {
            (
                rows.iter()
                    .filter(|row| path_is_under(&copy_dir, &row.destination_path))
                    .collect(),
                false,
            )
        } else {
            (precise, true)
        };
        if matching.is_empty() {
            continue;
        }
        let entries = placed.entry(object_id).or_default();
        for row in matching {
            let entry = FromHereDirectory {
                decision_id,
                rel_prefix: row.rel_prefix.clone(),
                precise: is_precise,
            };
            if !entries.contains(&entry) {
                entries.push(entry);
            }
        }
    }
    for entries in placed.values_mut() {
        entries.sort_by(|a, b| {
            (a.decision_id, &a.rel_prefix, a.precise).cmp(&(
                b.decision_id,
                &b.rel_prefix,
                b.precise,
            ))
        });
    }
    placed
}

/// What the archiving receipts say, file by file: the paths they name as
/// chosen from this root, and the decisions whose receipt was read.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ReceiptOrigins {
    pub paths: HashSet<String>,
    pub read_decisions: HashSet<i64>,
}

/// What a standing file's "archived from here" rests on: an archiving run
/// that drew from this root and stamped a present archive copy of the file's
/// content — narrowed to the file the run chose.
///
/// One classifier, two evidence constructors: the screens read the index
/// (`by_directory` — the directory the index records for the run), the book
/// reads the receipts (`by_receipt` — the receipt where one was read, the
/// directory where none was). They differ only in evidence, so they cannot
/// drift. Whatever the evidence, a file whose content has no from-here entry
/// is never held, which keeps `Archived ⊆ archived`.
#[derive(Clone, Copy)]
pub struct FromHereEvidence<'a> {
    directories: &'a HashMap<i64, Vec<FromHereDirectory>>,
    receipts: Option<&'a ReceiptOrigins>,
}

impl<'a> FromHereEvidence<'a> {
    /// The screens: the directory the index records for the run.
    pub fn by_directory(directories: &'a HashMap<i64, Vec<FromHereDirectory>>) -> Self {
        Self {
            directories,
            receipts: None,
        }
    }

    /// The book: the receipt where one was read, the directory where none was.
    pub fn by_receipt(
        directories: &'a HashMap<i64, Vec<FromHereDirectory>>,
        receipts: &'a ReceiptOrigins,
    ) -> Self {
        Self {
            directories,
            receipts: Some(receipts),
        }
    }

    /// Whether this standing file is the one an archiving run chose.
    pub fn holds(&self, source: &Source) -> bool {
        let Some(entries) = self.entries(source) else {
            return false;
        };
        match self.receipts {
            None => entries.iter().any(|entry| matches_directory(entry, source)),
            Some(receipts) => {
                receipts.paths.contains(&source.rel_path)
                    || entries.iter().any(|entry| {
                        !receipts.read_decisions.contains(&entry.decision_id)
                            && matches_directory(entry, source)
                    })
            }
        }
    }

    /// The decisions that hold this file **only** through the directory
    /// fallback — no receipt of theirs was read and no receipt names the
    /// file — ascending. Empty on the screens, where the directory is the
    /// evidence rather than a fallback. What the book's gap for each such
    /// decision counts.
    pub fn directory_only_decisions(&self, source: &Source) -> Vec<i64> {
        let (Some(receipts), Some(entries)) = (self.receipts, self.entries(source)) else {
            return Vec::new();
        };
        if receipts.paths.contains(&source.rel_path) {
            return Vec::new();
        }
        let mut ids: Vec<i64> = entries
            .iter()
            .filter(|entry| {
                !receipts.read_decisions.contains(&entry.decision_id)
                    && matches_directory(entry, source)
            })
            .map(|entry| entry.decision_id)
            .collect();
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    fn entries(&self, source: &Source) -> Option<&'a Vec<FromHereDirectory>> {
        source
            .object_id
            .and_then(|id| self.directories.get(&id))
            .filter(|entries| !entries.is_empty())
    }
}

/// A precise row names the file's own directory; a coarse one names a
/// directory the file is at or under.
fn matches_directory(entry: &FromHereDirectory, source: &Source) -> bool {
    if entry.precise {
        super::extraction::parent_dir(&source.rel_path) == entry.rel_prefix
    } else {
        super::path::path_is_under(&source.rel_path, &entry.rel_prefix)
    }
}

pub fn classify_present(
    source: &Source,
    archived: &HashSet<i64>,
    from_here: FromHereEvidence,
) -> StandingBucket {
    if source.is_excluded() {
        return StandingBucket::Excluded;
    }
    if source.is_contentless() {
        return StandingBucket::Contentless;
    }
    match source.object_id {
        Some(_) if from_here.holds(source) => StandingBucket::Archived,
        Some(object_id) if archived.contains(&object_id) => StandingBucket::Covered,
        Some(_) => StandingBucket::Unresolved { unhashed: false },
        None => StandingBucket::Unresolved { unhashed: true },
    }
}

/// The readiness review's own remainder measure, computed where the
/// archived-from-here evidence is not on hand: how many of these present
/// rows are neither archived, nor covered, nor excluded, nor empty.
///
/// Routing through `classify_present` is what makes this **one law with two
/// call shapes**, not a second spelling of "unresolved". It is sound because
/// the from-here evidence only ever splits `Archived` from `Covered`, and
/// neither of those is unresolved — so any value of it leaves this count
/// alone. The evidence holds only for objects with a from-here entry, and
/// those are a subset of `archived` (both SQL projections demand a present,
/// non-empty archive-role copy; the from-here half only adds the extraction
/// join), which is the premise that argument rests on. Pinned by
/// `archived_from_here_never_moves_the_unresolved_count`.
///
/// The contentless law is inherited rather than restated: `classify_present`
/// buckets an empty source as `Contentless` before any identity test, so a
/// root holding only empty files reads zero here.
pub fn unresolved_remainder(present: &[&Source], archived: &HashSet<i64>) -> i64 {
    // No archived-from-here evidence in hand. Passing empty evidence reads
    // every such row as `Covered` instead of `Archived` — a distinction this
    // count cannot see, which is exactly the pinned fact above.
    let none: HashMap<i64, Vec<FromHereDirectory>> = HashMap::new();
    present
        .iter()
        .filter(|source| {
            matches!(
                classify_present(source, archived, FromHereEvidence::by_directory(&none)),
                StandingBucket::Unresolved { .. }
            )
        })
        .count() as i64
}

/// A claim's dependence on places behind closed doors: how many rows rest
/// solely there, and which parked roots hold the copies.
///
/// The count and the roots travel together because they are one statement —
/// separate fields drift, and a line naming roots its own count does not
/// rest on is exactly the false testimony this carries evidence to prevent.
/// Roots are ids: the account knows no paths, and every surface that names
/// them already holds the fleet.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ParkedEvidence {
    pub count: i64,
    /// Ascending and deduped once [`ParkedEvidence::settle`] has run.
    pub roots: Vec<i64>,
}

impl ParkedEvidence {
    /// Record one claim grounded on these copies, if it rests solely behind
    /// doors; returns whether it did.
    ///
    /// The one accumulation site, so `only_on_suspended` is consulted here
    /// and not re-spelled beside every counter.
    pub fn observe(&mut self, copies: &[ArchiveCopy]) -> bool {
        if !only_on_suspended(copies) {
            return false;
        }
        self.count += 1;
        self.roots.extend(copies.iter().map(|copy| copy.root_id));
        true
    }

    /// Order and dedupe the roots — called once the last claim is in.
    pub fn settle(&mut self) {
        self.roots.sort_unstable();
        self.roots.dedup();
    }

    /// The trailing qualifier this evidence earns, naming its roots against
    /// the fleet the caller holds — `None` when the claim rests on nothing
    /// closed.
    pub fn segment(&self, roots: &[Root]) -> Option<String> {
        let paths: Vec<String> = self
            .roots
            .iter()
            .filter_map(|id| roots.iter().find(|root| root.id == *id))
            .map(|root| root.path.clone())
            .collect();
        only_on_suspended_segment(self.count, &paths)
    }

    /// The claim's segment as a trailing parenthetical: `" (N only on
    /// suspended <roots>)"`, or empty when nothing rests behind a door.
    ///
    /// One of three attachment shapes a caller wants: a claim sentence
    /// already carries its own words, and this qualifier rides at the end of
    /// it in parentheses. The others are [`ParkedEvidence::semicolon_clause`]
    /// and [`ParkedEvidence::bulleted_clause`].
    pub fn parenthetical(&self, roots: &[Root]) -> String {
        self.segment(roots)
            .map(|segment| format!(" ({segment})"))
            .unwrap_or_default()
    }

    /// The claim's segment as a trailing semicolon-joined clause:
    /// `"; N only on suspended <roots>"`, or empty when nothing rests behind
    /// a door. The second of three attachment shapes —
    /// [`ParkedEvidence::parenthetical`] and [`ParkedEvidence::bulleted_clause`]
    /// are the others.
    pub fn semicolon_clause(&self, roots: &[Root]) -> String {
        self.segment(roots)
            .map(|segment| format!("; {segment}"))
            .unwrap_or_default()
    }

    /// The claim's segment as a trailing bulleted clause: `" · N only on
    /// suspended <roots>"`, or empty when nothing rests behind a door.
    ///
    /// The third attachment shape: coverage's own reading, a middle-dot
    /// separator distinct from both the parenthetical and the semicolon
    /// clause. Named rather than left as a call-site `format!` for the same
    /// reason those two were minted — a caller composing the separator by
    /// hand can mistype it or forget to gate on `None`.
    pub fn bulleted_clause(&self, roots: &[Root]) -> String {
        self.segment(roots)
            .map(|segment| format!(" · {segment}"))
            .unwrap_or_default()
    }
}

/// Whether a claim grounded on these archive copies rests **solely** behind
/// closed doors: there is at least one copy, and the user has closed the
/// door on every root holding one.
///
/// The one owner of the all-parked test (the account's two counts and
/// coverage's segment). Both halves are load-bearing. **Non-empty**: no
/// copies is no claim at all, and "only on suspended roots" said of nothing
/// is a claim about an absence. **Every**: one copy on an open root means
/// the claim stands without opening anything, so saying so would raise a
/// question the evidence does not pose — the registry's "stated, never
/// headlined" cuts both ways, and a marker that fires on mixed evidence is
/// a headline.
pub fn only_on_suspended(copies: &[ArchiveCopy]) -> bool {
    !copies.is_empty() && copies.iter().all(|copy| copy.root_suspended)
}

/// The **stated-never-headlined** law: a place behind a door the user closed
/// never leads a list of places to go.
///
/// Ordering, not filtering — *this function* never drops a place, it only
/// says a closed one never sorts ahead of an open one on weight alone,
/// always the **head** of a comparison so everything else a list orders by
/// (weight, path, ids) applies *within* standing, never across it, because a
/// list's first line is its headline. **This function alone does not make
/// "still appears" true**: a consumer that also caps a list owes that half
/// itself — selecting which entries earn a cap slot by weight (never
/// standing, so a closed door can never decide membership) before calling
/// here to order only the survivors, or stating the omission as a count
/// where a cap could otherwise make a closed-door entry silently vanish
/// (`story::domain::locations::aggregate_locations`'s selection, capped by
/// weight alone and left in that order, paired with
/// `story::ops::render::fmt_locations`'s render-time call here — gated on
/// `Voicing::Judgment` — is the worked example of the split living in two
/// files, one voicing-blind and one voicing-aware, exactly because the same
/// selection also feeds a voicing that must never call here at all; a
/// present-tense list that merely counts its remainder, like the trail
/// composition card, satisfies "stated" through the count itself, the same
/// disposition the closed-door registry already grants elsewhere).
///
/// Canon-reach: several subsystems independently rank lists of cited
/// locations and each must answer this the same way, or the same place reads
/// as led in one view and trailing in its neighbour for no reason a user
/// caused. Consumers that call this function: survey's four location lists,
/// story's covered-where line, the trail composition card's origin lines.
/// The sweep's live-preferred counterpart choice answers the same law by a
/// **different, filtering** mechanism — a losing parked scope forfeits the
/// citation outright rather than sorting behind — so it shares the law, not
/// this function; see `sweep/CLAUDE.md`'s own "computed always, ranked
/// never" section for why ordering was not the right shape there.
pub fn standing_first(a_suspended: bool, b_suspended: bool) -> std::cmp::Ordering {
    a_suspended.cmp(&b_suspended)
}

/// Deleted iff the stamp is Observe-family — the trail's presence-axis rule:
/// "deleted reads only the absent bucket of Observe-family decisions".
pub fn classify_absent(family: Option<DecisionFamily>) -> AbsentBucket {
    match family {
        Some(DecisionFamily::Observe) => AbsentBucket::Deleted,
        _ => AbsentBucket::Unexplained,
    }
}

/// Build the account from the two presence classes, the archive copies, the
/// evidence a standing file's "archived from here" rests on, the root's
/// origin extraction rows, and the absent rows' stamp families
/// (`decision_id` → family).
pub fn build_account(
    present: &[Source],
    absent: &[Source],
    archive_copies: &HashMap<i64, Vec<ArchiveCopy>>,
    from_here: FromHereEvidence,
    extractions: &[DecisionExtraction],
    stamp_families: &HashMap<i64, DecisionFamily>,
) -> ResolutionAccount {
    let mut account = ResolutionAccount {
        archived_bytes: Some(0),
        ..Default::default()
    };

    // The archive-membership question and the where-does-it-stand question
    // are answered from **one** input. A set of ids cannot say whether the
    // evidence is all behind doors, so passing one alongside the copies
    // would be passing a pre-computed answer that could disagree with them.
    let archived: HashSet<i64> = archive_copies.keys().copied().collect();
    let copies_of = |source: &Source| {
        source
            .object_id
            .and_then(|id| archive_copies.get(&id))
            .map(Vec::as_slice)
            .unwrap_or(&[])
    };

    for source in present {
        match classify_present(source, &archived, from_here) {
            StandingBucket::Archived => {
                account.archived_standing += 1;
                for decision_id in from_here.directory_only_decisions(source) {
                    *account
                        .archived_standing_by_directory
                        .entry(decision_id)
                        .or_insert(0) += 1;
                }
                account
                    .archived_standing_only_parked
                    .observe(copies_of(source));
            }
            StandingBucket::Covered => {
                account.covered += 1;
                account.covered_only_parked.observe(copies_of(source));
            }
            StandingBucket::Excluded => account.excluded += 1,
            StandingBucket::Contentless => account.contentless += 1,
            StandingBucket::Unresolved { unhashed } => {
                account.unresolved += 1;
                if unhashed {
                    account.unhashed_unresolved += 1;
                }
            }
        }
    }

    for source in absent {
        let family = source
            .decision_id
            .and_then(|id| stamp_families.get(&id).copied());
        match classify_absent(family) {
            AbsentBucket::Deleted => account.deleted += 1,
            AbsentBucket::Unexplained => account.unexplained_missing += 1,
        }
    }

    for row in extractions {
        account.archived_files += row.files;
        account.archived_bytes = match (account.archived_bytes, row.bytes) {
            (Some(total), Some(bytes)) => Some(total + bytes),
            _ => None,
        };
        match row.disposition {
            Some(OriginDisposition::Relocated) => account.archived_moved += row.files,
            Some(OriginDisposition::Retained) => account.archived_copied += row.files,
            None => account.archived_unrecorded += row.files,
        }
    }
    if account.archived_files == 0 {
        account.archived_bytes = Some(0);
    }

    account.covered_only_parked.settle();
    account.archived_standing_only_parked.settle();

    account
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source(id: i64, object_id: Option<i64>, excluded: bool, object_excluded: bool) -> Source {
        Source {
            id,
            root_id: 1,
            root_path: "/r".to_string(),
            rel_path: format!("f{id}.jpg"),
            object_id,
            size: 100,
            mtime: 0,
            excluded,
            object_excluded: object_id.map(|_| object_excluded),
            device: 1,
            inode: id,
            partial_hash: String::new(),
            basis_rev: 0,
            root_role: "source".to_string(),
            root_suspended: false,
            decision_id: None,
        }
    }

    fn stamped(mut s: Source, decision_id: i64) -> Source {
        s.decision_id = Some(decision_id);
        s
    }

    fn empty(mut s: Source) -> Source {
        s.size = 0;
        s
    }

    /// One corpus exercising every arm of `classify_present`: two rows whose
    /// objects stand in the archive, two dismissed (source- and
    /// object-excluded), two genuinely unresolved (hashed and never hashed),
    /// and two empty ones — the contentless arm, including the unhashed
    /// empty that must not read as a blocker.
    fn remainder_corpus() -> Vec<Source> {
        vec![
            source(1, Some(10), false, false),
            source(2, Some(11), false, false),
            source(3, Some(12), true, false),
            source(4, Some(13), false, true),
            source(5, Some(20), false, false),
            source(6, None, false, false),
            empty(source(7, Some(10), false, false)),
            empty(source(8, None, false, false)),
        ]
    }

    fn extraction(
        files: i64,
        bytes: Option<i64>,
        disposition: Option<OriginDisposition>,
    ) -> DecisionExtraction {
        // Rows shaped directly rather than via build_extraction_rows: the
        // legacy no-disposition case the builder never emits is under test.
        DecisionExtraction {
            decision_id: 1,
            root_id: 1,
            root_path: "/r".to_string(),
            rel_prefix: String::new(),
            files,
            bytes,
            destination_root_id: Some(2),
            destination_path: "/archive".to_string(),
            disposition,
        }
    }

    fn archived_set(ids: &[i64]) -> HashSet<i64> {
        ids.iter().copied().collect()
    }

    use crate::core::testing::from_here_anywhere;

    fn by_dir(map: &HashMap<i64, Vec<FromHereDirectory>>) -> FromHereEvidence<'_> {
        FromHereEvidence::by_directory(map)
    }

    /// Archive copies of these objects, every one on a root whose door
    /// stands open — the ordinary shape, and the one that leaves both
    /// only-parked counts at zero.
    fn copies_of(ids: &[i64]) -> HashMap<i64, Vec<ArchiveCopy>> {
        copies_behind(ids, false)
    }

    /// The same, with every copy behind a closed door.
    fn copies_behind(ids: &[i64], suspended: bool) -> HashMap<i64, Vec<ArchiveCopy>> {
        ids.iter()
            .map(|id| {
                (
                    *id,
                    vec![ArchiveCopy {
                        root_id: 9,
                        root_suspended: suspended,
                    }],
                )
            })
            .collect()
    }

    fn mixed_account() -> ResolutionAccount {
        let present = vec![
            source(1, Some(10), false, false), // covered
            source(2, Some(11), false, false), // covered
            source(3, Some(12), true, false),  // excluded
            source(4, Some(13), false, false), // unresolved, hashed
            source(5, None, false, false),     // unresolved, unhashed
        ];
        let absent = vec![
            stamped(source(6, Some(14), false, false), 100), // Observe → deleted
            stamped(source(7, None, false, false), 200),     // Exclude → unexplained
            source(8, None, false, false),                   // no stamp → unexplained
        ];
        let families = HashMap::from([
            (100, DecisionFamily::Observe),
            (200, DecisionFamily::Exclude),
        ]);
        let extractions = vec![
            extraction(3, Some(300), Some(OriginDisposition::Relocated)),
            extraction(2, Some(200), Some(OriginDisposition::Retained)),
        ];
        build_account(
            &present,
            &absent,
            &copies_of(&[10, 11]),
            by_dir(&from_here_anywhere(&[])),
            &extractions,
            &families,
        )
    }

    // classify_present — priority excluded > covered > unresolved

    #[test]
    fn present_source_excluded_beats_archived_object() {
        let s = source(1, Some(10), true, false);
        assert_eq!(
            classify_present(&s, &archived_set(&[10]), by_dir(&from_here_anywhere(&[]))),
            StandingBucket::Excluded
        );
    }

    #[test]
    fn present_object_excluded_beats_archived_object() {
        let s = source(1, Some(10), false, true);
        assert_eq!(
            classify_present(&s, &archived_set(&[10]), by_dir(&from_here_anywhere(&[]))),
            StandingBucket::Excluded
        );
    }

    #[test]
    fn present_hashed_in_archive_is_covered() {
        let s = source(1, Some(10), false, false);
        assert_eq!(
            classify_present(&s, &archived_set(&[10]), by_dir(&from_here_anywhere(&[]))),
            StandingBucket::Covered
        );
    }

    #[test]
    fn present_hashed_uncovered_is_unresolved() {
        let s = source(1, Some(10), false, false);
        assert_eq!(
            classify_present(&s, &archived_set(&[]), by_dir(&from_here_anywhere(&[]))),
            StandingBucket::Unresolved { unhashed: false }
        );
    }

    #[test]
    fn present_unhashed_is_unresolved_unhashed() {
        let s = source(1, None, false, false);
        assert_eq!(
            classify_present(&s, &archived_set(&[]), by_dir(&from_here_anywhere(&[]))),
            StandingBucket::Unresolved { unhashed: true }
        );
    }

    #[test]
    fn present_unhashed_but_excluded_is_excluded() {
        let s = source(1, None, true, false);
        assert_eq!(
            classify_present(&s, &archived_set(&[]), by_dir(&from_here_anywhere(&[]))),
            StandingBucket::Excluded
        );
    }

    // The contentless law + the standing split: precedence
    // excluded > contentless > archived > covered > unresolved.

    #[test]
    fn empty_source_is_contentless_even_when_its_object_is_archived() {
        let mut s = source(1, Some(10), false, false);
        s.size = 0;
        assert_eq!(
            classify_present(&s, &archived_set(&[10]), by_dir(&from_here_anywhere(&[10]))),
            StandingBucket::Contentless,
            "identity evidence about no-content is vacuous"
        );
    }

    #[test]
    fn unhashed_empty_source_is_contentless_not_unresolved() {
        // Contentless precedes the hash test — empty files on a
        // never-enriched root must not block retirement.
        let mut s = source(1, None, false, false);
        s.size = 0;
        assert_eq!(
            classify_present(&s, &archived_set(&[]), by_dir(&from_here_anywhere(&[]))),
            StandingBucket::Contentless
        );
    }

    #[test]
    fn excluded_empty_source_is_excluded() {
        // Judgment covers shape: exclusion outranks contentless.
        let mut s = source(1, Some(10), true, false);
        s.size = 0;
        assert_eq!(
            classify_present(&s, &archived_set(&[10]), by_dir(&from_here_anywhere(&[]))),
            StandingBucket::Excluded
        );
    }

    #[test]
    fn archived_from_here_beats_covered() {
        // The deliberate act wins mixed evidence: an object both archived
        // from here and standing in the archive reads Archived, not Covered.
        let s = source(1, Some(10), false, false);
        assert_eq!(
            classify_present(&s, &archived_set(&[10]), by_dir(&from_here_anywhere(&[10]))),
            StandingBucket::Archived
        );
    }

    // unresolved_remainder — the readiness review's measure, projected

    #[test]
    fn archived_from_here_never_moves_the_unresolved_count() {
        // The whole soundness argument for `unresolved_remainder`, pinned: if
        // this fails the projection is unsound and the sweep's nearness term
        // is wrong, not merely buggy.
        //
        // `archived_from_here` only ever splits `Archived` from `Covered`,
        // and neither of those is unresolved — so no value of it can move
        // this count. Checked over every subset of the archived set, which is
        // "arbitrary archived_from_here" in the only form the two SQL
        // projections can produce: both demand a present, non-empty
        // archive-role copy, and the from-here half only adds the extraction
        // join, so it is always a subset.
        let archived_ids = [10i64, 11, 12, 13];
        let corpus = remainder_corpus();
        let refs: Vec<&Source> = corpus.iter().collect();
        let archived = archived_set(&archived_ids);
        let baseline = unresolved_remainder(&refs, &archived);
        assert_eq!(baseline, 2, "the corpus must have something left to lose");

        for mask in 0..(1u32 << archived_ids.len()) {
            let chosen: Vec<i64> = archived_ids
                .iter()
                .enumerate()
                .filter(|(i, _)| mask & (1 << i) != 0)
                .map(|(_, &id)| id)
                .collect();
            let from_here = from_here_anywhere(&chosen);
            let counted = refs
                .iter()
                .filter(|s| {
                    matches!(
                        classify_present(s, &archived, by_dir(&from_here)),
                        StandingBucket::Unresolved { .. }
                    )
                })
                .count() as i64;
            assert_eq!(
                counted, baseline,
                "archived from here {chosen:?} moved the unresolved count"
            );
        }
    }

    #[test]
    fn a_root_holding_only_empty_files_reads_zero_remaining() {
        // The contentless law, load-bearing here: empty files never block a
        // retirement and must not read as a remainder either — including the
        // unhashed ones on a root nobody ever enriched.
        let corpus = [
            empty(source(1, Some(10), false, false)),
            empty(source(2, None, false, false)),
            empty(source(3, Some(11), false, false)),
        ];
        let refs: Vec<&Source> = corpus.iter().collect();
        assert_eq!(unresolved_remainder(&refs, &archived_set(&[])), 0);
        assert_eq!(unresolved_remainder(&refs, &archived_set(&[10, 11])), 0);
    }

    #[test]
    fn the_remainder_matches_the_readiness_account_on_the_same_root() {
        // Not two measures that happen to agree: the same rows and the same
        // archived set give the same number, with the account exercising the
        // one arm the remainder deliberately cannot see.
        let present = remainder_corpus();
        let refs: Vec<&Source> = present.iter().collect();
        let archived = archived_set(&[10, 11, 12, 13]);
        let account = build_account(
            &present,
            &[],
            &copies_of(&[10, 11, 12, 13]),
            by_dir(&from_here_anywhere(&[11])),
            &[],
            &HashMap::new(),
        );
        assert!(
            account.archived_standing > 0,
            "the account must exercise the Archived arm the remainder cannot see"
        );
        assert_eq!(unresolved_remainder(&refs, &archived), account.unresolved);
    }

    // FromHereEvidence — "archived from here" is the file the run chose

    /// A standing file at `rel` holding object `object_id`.
    fn file(id: i64, rel: &str, object_id: i64) -> Source {
        let mut s = source(id, Some(object_id), false, false);
        s.rel_path = rel.to_string();
        s
    }

    /// The from-here query's row: one copy of `object_id` at
    /// `/archive/<copy_rel>`, stamped by `decision`, beside one of that run's
    /// rows (`origin` → `destination`).
    fn candidate(
        object_id: i64,
        decision: i64,
        origin: &str,
        destination: &str,
        copy_rel: &str,
    ) -> FromHereCandidate {
        FromHereCandidate {
            object_id,
            decision_id: decision,
            rel_prefix: origin.to_string(),
            destination_path: destination.to_string(),
            copy_root_path: "/archive".to_string(),
            copy_rel_path: copy_rel.to_string(),
        }
    }

    #[test]
    fn the_chosen_file_is_archived_from_here_and_its_copy_elsewhere_is_covered() {
        // `photos/a.jpg` was chosen into `/archive/photos`; its copy in
        // `backup/` holds the same content but nobody chose it.
        let placed = place_from_here(&[candidate(
            10,
            5,
            "photos",
            "/archive/photos",
            "photos/a.jpg",
        )]);
        let evidence = FromHereEvidence::by_directory(&placed);
        let archived = archived_set(&[10]);
        assert_eq!(
            classify_present(&file(1, "photos/a.jpg", 10), &archived, evidence),
            StandingBucket::Archived
        );
        assert_eq!(
            classify_present(&file(2, "backup/a.jpg", 10), &archived, evidence),
            StandingBucket::Covered
        );
    }

    #[test]
    fn a_copy_in_a_subdirectory_of_a_chosen_directory_is_covered() {
        // A precise row names the chosen file's own directory, exactly.
        let placed = place_from_here(&[candidate(
            10,
            5,
            "photos",
            "/archive/photos",
            "photos/a.jpg",
        )]);
        let evidence = FromHereEvidence::by_directory(&placed);
        assert_eq!(
            classify_present(
                &file(2, "photos/old/a.jpg", 10),
                &archived_set(&[10]),
                evidence
            ),
            StandingBucket::Covered
        );
    }

    #[test]
    fn a_copy_in_a_sibling_directory_of_the_same_run_is_covered() {
        // One run chose `photos/2019/a.jpg` and `photos/2020/b.jpg`. The copy
        // of `a` in `photos/2020` is matched against the row that placed
        // `a`'s archive copy, not against every directory the run drew from.
        let placed = place_from_here(&[
            candidate(10, 5, "photos/2019", "/archive/2019", "2019/a.jpg"),
            candidate(10, 5, "photos/2020", "/archive/2020", "2019/a.jpg"),
            candidate(11, 5, "photos/2019", "/archive/2019", "2020/b.jpg"),
            candidate(11, 5, "photos/2020", "/archive/2020", "2020/b.jpg"),
        ]);
        let evidence = FromHereEvidence::by_directory(&placed);
        let archived = archived_set(&[10, 11]);
        assert_eq!(
            classify_present(&file(1, "photos/2019/a.jpg", 10), &archived, evidence),
            StandingBucket::Archived
        );
        assert_eq!(
            classify_present(&file(2, "photos/2020/b.jpg", 11), &archived, evidence),
            StandingBucket::Archived
        );
        assert_eq!(
            classify_present(&file(3, "photos/2020/a.jpg", 10), &archived, evidence),
            StandingBucket::Covered
        );
    }

    #[test]
    fn a_coarse_row_counts_its_subdirectories_as_chosen() {
        // A row recorded before directory precision holds a common prefix on
        // both ends. Its files in subdirectories read archived from here, so
        // *standing here now* agrees with *the story so far*.
        let placed = place_from_here(&[
            candidate(10, 5, "photos", "/archive", "2019/a.jpg"),
            candidate(11, 5, "photos", "/archive", "2020/b.jpg"),
        ]);
        assert!(placed[&10].iter().all(|entry| !entry.precise));
        let present = [
            file(1, "photos/2019/a.jpg", 10),
            file(2, "photos/2020/b.jpg", 11),
        ];
        let mut row = extraction(2, Some(200), Some(OriginDisposition::Retained));
        row.rel_prefix = "photos".to_string();
        let account = build_account(
            &present,
            &[],
            &copies_of(&[10, 11]),
            FromHereEvidence::by_directory(&placed),
            &[row],
            &HashMap::new(),
        );
        assert_eq!(account.archived_standing, account.archived_files);
        assert_eq!(account.covered, 0);
    }

    #[test]
    fn a_copy_no_row_places_adds_nothing() {
        // Moved within the archive after the apply: no row's destination is
        // or contains its directory — covered, the safe direction.
        let placed = place_from_here(&[candidate(
            10,
            5,
            "photos",
            "/archive/photos",
            "elsewhere/a.jpg",
        )]);
        assert!(placed.is_empty());
    }

    #[test]
    fn the_book_reads_the_receipt_where_one_was_read_and_the_directory_where_none_was() {
        // Decision 5 left a receipt naming `photos/a.jpg`; decision 6 left
        // none and drew from `docs`. A same-directory copy of `a` is told
        // apart by the receipt; decision 6's files are held by directory and
        // counted as its fallback.
        let placed = place_from_here(&[
            candidate(10, 5, "photos", "/archive/photos", "photos/a.jpg"),
            candidate(11, 6, "docs", "/archive/docs", "docs/d.txt"),
        ]);
        let receipts = ReceiptOrigins {
            paths: HashSet::from(["photos/a.jpg".to_string()]),
            read_decisions: HashSet::from([5]),
        };
        let book = FromHereEvidence::by_receipt(&placed, &receipts);
        let screens = FromHereEvidence::by_directory(&placed);
        let chosen = file(1, "photos/a.jpg", 10);
        let beside = file(2, "photos/a (1).jpg", 10);
        let by_dir = file(3, "docs/d.txt", 11);

        assert!(book.holds(&chosen) && screens.holds(&chosen));
        assert!(
            !book.holds(&beside),
            "the receipt names the chosen file only"
        );
        assert!(
            screens.holds(&beside),
            "the screens' named gap: one directory"
        );
        assert!(book.holds(&by_dir));
        assert_eq!(book.directory_only_decisions(&by_dir), vec![6]);
        assert!(book.directory_only_decisions(&chosen).is_empty());
        assert!(screens.directory_only_decisions(&by_dir).is_empty());

        let account = build_account(
            &[chosen, beside, by_dir],
            &[],
            &copies_of(&[10, 11]),
            book,
            &[],
            &HashMap::new(),
        );
        assert_eq!(account.archived_standing, 2);
        assert_eq!(account.covered, 1);
        assert_eq!(
            account.archived_standing_by_directory,
            BTreeMap::from([(6, 1)])
        );
    }

    #[test]
    fn a_path_the_receipt_names_holds_only_where_its_content_was_archived_from_here() {
        // `Archived ⊆ archived`: a receipt naming the path is not enough when
        // the file's content now has no from-here entry (it changed since).
        let placed: HashMap<i64, Vec<FromHereDirectory>> = HashMap::new();
        let receipts = ReceiptOrigins {
            paths: HashSet::from(["photos/a.jpg".to_string()]),
            read_decisions: HashSet::from([5]),
        };
        let book = FromHereEvidence::by_receipt(&placed, &receipts);
        assert!(!book.holds(&file(1, "photos/a.jpg", 99)));
    }

    // classify_absent — Deleted iff Observe

    #[test]
    fn absent_observe_stamp_is_deleted() {
        assert_eq!(
            classify_absent(Some(DecisionFamily::Observe)),
            AbsentBucket::Deleted
        );
    }

    #[test]
    fn absent_archive_stamp_is_unexplained() {
        assert_eq!(
            classify_absent(Some(DecisionFamily::Archive)),
            AbsentBucket::Unexplained
        );
    }

    #[test]
    fn absent_exclude_stamp_is_unexplained() {
        assert_eq!(
            classify_absent(Some(DecisionFamily::Exclude)),
            AbsentBucket::Unexplained
        );
    }

    #[test]
    fn absent_without_stamp_is_unexplained() {
        assert_eq!(classify_absent(None), AbsentBucket::Unexplained);
    }

    // build_account

    #[test]
    fn account_buckets_every_class() {
        let a = mixed_account();
        assert_eq!(a.covered, 2);
        assert_eq!(a.excluded, 1);
        assert_eq!(a.unresolved, 2);
        assert_eq!(a.unhashed_unresolved, 1);
        assert_eq!(a.deleted, 1);
        assert_eq!(a.unexplained_missing, 2);
        assert_eq!(a.archived_files, 5);
        assert_eq!(a.archived_bytes, Some(500));
        assert_eq!(a.archived_moved, 3);
        assert_eq!(a.archived_copied, 2);
        assert_eq!(a.archived_unrecorded, 0);
    }

    #[test]
    fn sum_invariants_hold() {
        let a = mixed_account();
        assert_eq!(a.standing(), a.covered + a.excluded + a.unresolved);
        assert_eq!(
            a.ever_indexed(),
            Some(a.standing() + a.deleted + a.unexplained_missing + a.archived_moved)
        );
    }

    #[test]
    fn unrecorded_disposition_degrades_split_and_ever_indexed() {
        let extractions = vec![
            extraction(3, Some(300), Some(OriginDisposition::Relocated)),
            extraction(4, Some(400), None), // legacy row
        ];
        let a = build_account(
            &[],
            &[],
            &copies_of(&[]),
            by_dir(&from_here_anywhere(&[])),
            &extractions,
            &HashMap::new(),
        );
        assert_eq!(a.archived_files, 7);
        assert_eq!(a.archived_unrecorded, 4);
        assert_eq!(a.ever_indexed(), None, "moved count unsupported → omitted");
    }

    #[test]
    fn archived_bytes_are_all_or_omitted() {
        let extractions = vec![
            extraction(3, Some(300), Some(OriginDisposition::Relocated)),
            extraction(2, None, Some(OriginDisposition::Retained)),
        ];
        let a = build_account(
            &[],
            &[],
            &copies_of(&[]),
            by_dir(&from_here_anywhere(&[])),
            &extractions,
            &HashMap::new(),
        );
        assert_eq!(a.archived_bytes, None);
    }

    // ========================================================================
    // only_on_suspended — the all-parked test, spoken once
    // ========================================================================

    fn copy(root_id: i64, suspended: bool) -> ArchiveCopy {
        ArchiveCopy {
            root_id,
            root_suspended: suspended,
        }
    }

    #[test]
    fn only_on_suspended_needs_every_copy_parked() {
        assert!(only_on_suspended(&[copy(1, true)]));
        assert!(only_on_suspended(&[copy(1, true), copy(2, true)]));
        assert!(
            !only_on_suspended(&[copy(1, true), copy(2, false)]),
            "one open door means nothing needs opening"
        );
        assert!(!only_on_suspended(&[copy(1, false)]));
    }

    #[test]
    fn only_on_suspended_says_nothing_about_no_copies() {
        // No copies is no claim at all; "only on suspended roots" said of
        // nothing would be a claim about an absence.
        assert!(!only_on_suspended(&[]));
    }

    // ========================================================================
    // The three attachment shapes — each pinned at its own separator, not
    // only at a consuming call site. A call site's own test can pin a shape
    // by accident and lose that coverage the moment it is rewritten to
    // assert on `segment()` instead of the composed string; owning the pin
    // here keeps it independent of which call site a future edit touches.
    // ========================================================================

    fn evidence_root(id: i64, path: &str) -> Root {
        Root {
            id,
            path: path.to_string(),
            role: "archive".to_string(),
            comment: None,
            last_scanned_at: None,
            suspended: true,
        }
    }

    fn one_parked_claim() -> (ParkedEvidence, Vec<Root>) {
        let evidence = ParkedEvidence {
            count: 1,
            roots: vec![7],
        };
        (evidence, vec![evidence_root(7, "/arc-parked")])
    }

    #[test]
    fn parenthetical_wraps_the_segment_in_parens() {
        let (evidence, roots) = one_parked_claim();
        assert_eq!(
            evidence.parenthetical(&roots),
            " (1 only on suspended /arc-parked)"
        );
        assert_eq!(ParkedEvidence::default().parenthetical(&roots), "");
    }

    #[test]
    fn semicolon_clause_joins_the_segment_with_a_semicolon() {
        let (evidence, roots) = one_parked_claim();
        assert_eq!(
            evidence.semicolon_clause(&roots),
            "; 1 only on suspended /arc-parked"
        );
        assert_eq!(ParkedEvidence::default().semicolon_clause(&roots), "");
    }

    #[test]
    fn bulleted_clause_joins_the_segment_with_a_middle_dot() {
        let (evidence, roots) = one_parked_claim();
        assert_eq!(
            evidence.bulleted_clause(&roots),
            " · 1 only on suspended /arc-parked"
        );
        assert_eq!(ParkedEvidence::default().bulleted_clause(&roots), "");
    }

    #[test]
    fn the_account_counts_what_each_claim_rests_on() {
        // Two rows covered and one archived from here. Of them, one covered
        // row and the archived row stand only behind closed doors.
        let present = vec![
            source(1, Some(10), false, false), // covered, evidence live
            source(2, Some(11), false, false), // covered, evidence all parked
            source(3, Some(12), false, false), // archived from here, parked
        ];
        let mut copies = HashMap::new();
        copies.insert(10, vec![copy(1, false), copy(2, true)]);
        copies.insert(11, vec![copy(2, true)]);
        copies.insert(12, vec![copy(2, true)]);

        let account = build_account(
            &present,
            &[],
            &copies,
            by_dir(&from_here_anywhere(&[12])),
            &[],
            &HashMap::new(),
        );

        assert_eq!(account.covered, 2);
        assert_eq!(account.covered_only_parked.count, 1);
        assert_eq!(account.archived_standing, 1);
        assert_eq!(account.archived_standing_only_parked.count, 1);
        assert_eq!(
            account.standing(),
            3,
            "the counts qualify the buckets, they do not add a sixth"
        );
    }

    #[test]
    fn an_excluded_or_empty_row_never_reaches_the_only_parked_counts() {
        // Precedence is unchanged: exclusion and contentless are decided
        // before any identity test, so a row that never lands in covered or
        // archived cannot be counted as resting on parked evidence.
        let mut empty = source(2, Some(11), false, false);
        empty.size = 0;
        let present = vec![source(1, Some(10), true, false), empty];
        let mut copies = HashMap::new();
        copies.insert(10, vec![copy(2, true)]);
        copies.insert(11, vec![copy(2, true)]);

        let account = build_account(
            &present,
            &[],
            &copies,
            by_dir(&from_here_anywhere(&[])),
            &[],
            &HashMap::new(),
        );

        assert_eq!(account.excluded, 1);
        assert_eq!(account.contentless, 1);
        assert_eq!(account.covered_only_parked.count, 0);
        assert_eq!(account.archived_standing_only_parked.count, 0);
    }

    #[test]
    fn standing_first_orders_the_open_place_before_the_parked_one() {
        use std::cmp::Ordering;
        // Open before parked, whatever the weight beside it — the ordering
        // a caller composes this ahead of every other sort key with.
        assert_eq!(standing_first(false, true), Ordering::Less);
        assert_eq!(standing_first(true, false), Ordering::Greater);
        assert_eq!(standing_first(false, false), Ordering::Equal);
        assert_eq!(standing_first(true, true), Ordering::Equal);
    }
}
