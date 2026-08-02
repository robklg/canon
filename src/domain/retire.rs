//! Retirement readiness: the resolution account and the asymmetric verdict.
//!
//! The account has two registers, never reconciled — the same discipline as
//! the trail's "Arrived here" (event total) beside the card's "Standing here"
//! (state total):
//!
//! - **The story so far** counts whole-history events: what was archived from
//!   here (extraction-recorded, copies and moves alike — the trail rollup's
//!   established meaning of "archived from here"), what was deleted
//!   (scan-observed), and what is missing without a recorded deletion.
//! - **Standing here now** partitions the present rows: covered, excluded,
//!   unresolved.
//!
//! Copies overlap the two registers deliberately (a file copied to the
//! archive is both "archived from here" and typically standing `covered`);
//! the moved/copied split on the archived line is what makes that overlap
//! readable rather than confusing.

use std::collections::{HashMap, HashSet};

use super::extraction::{DecisionExtraction, OriginDisposition};
use super::source::Source;
use super::trail::{fate_transition, DecisionFamily, FateAspect};

/// The asymmetric verdict. There is deliberately no `Ready` variant: Canon
/// can know NOT READY — present sources neither archived nor excluded — but
/// whether a story is complete is the user's judgment, never certified.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Readiness {
    NotReady { unresolved: i64, unhashed: i64 },
    NoBlockersFound,
}

impl Readiness {
    /// The gate: does the ceremony refuse to proceed? Takes only the
    /// `--allow unresolved` acknowledgment — never `--yes`: the prompt-skip
    /// and the acknowledgment are orthogonal, and this signature is what
    /// enforces it.
    pub fn blocks(&self, allow_unresolved: bool) -> bool {
        match self {
            Readiness::NotReady { .. } => !allow_unresolved,
            Readiness::NoBlockersFound => false,
        }
    }
}

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
    pub covered: i64,
    pub excluded: i64,
    pub unresolved: i64,
    /// Subset of `unresolved`: present, non-excluded, never hashed — they
    /// cannot be verified covered, which is exactly why they block.
    pub unhashed_unresolved: i64,
}

impl ResolutionAccount {
    /// Present rows, partitioned exactly: covered + excluded + unresolved.
    pub fn standing(&self) -> i64 {
        self.covered + self.excluded + self.unresolved
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

/// Where a present row stands, by priority excluded > covered > unresolved:
/// an excluded-and-covered source counts excluded — its resolution is
/// already recorded, which is the fact the account reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StandingBucket {
    Covered,
    Excluded,
    Unresolved { unhashed: bool },
}

/// What an absent row's stamp still explains.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbsentBucket {
    Deleted,
    Unexplained,
}

pub fn classify_present(source: &Source, archived: &HashSet<i64>) -> StandingBucket {
    if source.is_excluded() {
        return StandingBucket::Excluded;
    }
    match source.object_id {
        Some(object_id) if archived.contains(&object_id) => StandingBucket::Covered,
        Some(_) => StandingBucket::Unresolved { unhashed: false },
        None => StandingBucket::Unresolved { unhashed: true },
    }
}

/// Deleted iff the stamp is Observe-family — the trail's presence-axis rule:
/// "deleted reads only the absent bucket of Observe-family decisions".
pub fn classify_absent(family: Option<DecisionFamily>) -> AbsentBucket {
    match family {
        Some(DecisionFamily::Observe) => AbsentBucket::Deleted,
        _ => AbsentBucket::Unexplained,
    }
}

/// Build the account from the two presence classes, the archive membership
/// set, the root's origin extraction rows, and the absent rows' stamp
/// families (`decision_id` → family).
pub fn build_account(
    present: &[Source],
    absent: &[Source],
    archived: &HashSet<i64>,
    extractions: &[DecisionExtraction],
    stamp_families: &HashMap<i64, DecisionFamily>,
) -> ResolutionAccount {
    let mut account = ResolutionAccount {
        archived_bytes: Some(0),
        ..Default::default()
    };

    for source in present {
        match classify_present(source, archived) {
            StandingBucket::Covered => account.covered += 1,
            StandingBucket::Excluded => account.excluded += 1,
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

    account
}

pub fn derive_readiness(account: &ResolutionAccount) -> Readiness {
    if account.unresolved > 0 {
        Readiness::NotReady {
            unresolved: account.unresolved,
            unhashed: account.unhashed_unresolved,
        }
    } else {
        Readiness::NoBlockersFound
    }
}

// ---------------------------------------------------------------------------
// The book's per-entry model
// ---------------------------------------------------------------------------

/// Standing words for book entries whose state is not a transition. The
/// never-literal law covers *transition* words (always derived through
/// `fate_transition`); covered / present / missing-unexplained are standings
/// — present-tense facts, not state changes — and are named here, their one
/// home, rather than forced through a derivation that rightly doesn't know
/// them.
/// The book's disposition words, aligned with the review's moved/copied
/// register: receipt vocabulary says retained/relocated (the origin's
/// perspective); the reader-facing account says moved/copied.
pub fn disposition_word(disposition: OriginDisposition) -> &'static str {
    match disposition {
        OriginDisposition::Relocated => "moved",
        OriginDisposition::Retained => "copied",
    }
}

pub const STANDING_COVERED: &str = "covered";
pub const STANDING_PRESENT: &str = "present";
pub const STANDING_MISSING_UNEXPLAINED: &str = "missing_unexplained";

/// The fate of one book entry — what happened to (or presently holds for)
/// this path on the retired root. The bucket decision underneath is
/// `classify_present`/`classify_absent` — the same derivation the account
/// folds over, so the inventory and the account cannot drift.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceFate {
    /// Extraction-recorded as an apply origin: a receipt item names this
    /// path. `destination` is the recorded destination (the
    /// Canon-independent fallback tier); `current_locations` are the archive
    /// paths holding the content at compile time (the live tier).
    /// `disposition` is `None` for pre-vocabulary receipts — moved vs.
    /// copied is omitted, never guessed.
    ArchivedFromHere {
        disposition: Option<OriginDisposition>,
        destination: String,
        current_locations: Vec<String>,
    },
    /// Content verified present in the archive, archived from elsewhere —
    /// or from here in Records mode (no receipt to say so; the compile
    /// records that gap).
    Covered { locations: Vec<String> },
    /// Dismissed — with the recorded reason, and the archive locations as
    /// context when the content is also archived (both truths carried).
    Excluded {
        reason: Option<String>,
        archive_locations: Vec<String>,
    },
    /// Absent, and the stamp is the Observe-family decision that witnessed
    /// the loss.
    Deleted { reason: Option<String> },
    /// Present at retirement, nothing above applies — listed honestly.
    PresentAtRetirement,
    /// Absent without a recorded deletion — the account's unexplained
    /// tombstones. The inventory must list them: "every source the root
    /// ever had" includes the ones whose loss the record cannot explain.
    MissingUnexplained,
}

impl SourceFate {
    /// The wire word for this fate. Terminal fates derive through
    /// `fate_transition` (the never-literal law); standings come from the
    /// named constants above.
    pub fn word(&self) -> &'static str {
        match self {
            SourceFate::ArchivedFromHere { .. } => {
                fate_transition(DecisionFamily::Archive, FateAspect::Present)
                    .expect("Archive+Present is a registered transition")
                    .as_str()
            }
            SourceFate::Excluded { .. } => {
                fate_transition(DecisionFamily::Exclude, FateAspect::Present)
                    .expect("Exclude is a registered transition")
                    .as_str()
            }
            SourceFate::Deleted { .. } => {
                fate_transition(DecisionFamily::Observe, FateAspect::Absent)
                    .expect("Observe+Absent is a registered transition")
                    .as_str()
            }
            SourceFate::Covered { .. } => STANDING_COVERED,
            SourceFate::PresentAtRetirement => STANDING_PRESENT,
            SourceFate::MissingUnexplained => STANDING_MISSING_UNEXPLAINED,
        }
    }
}

/// Hash honesty as a property of the entry, never the formatter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryVerification {
    ContentVerified,
    NameOnly,
}

impl EntryVerification {
    pub fn word(&self) -> &'static str {
        match self {
            Self::ContentVerified => "content_verified",
            Self::NameOnly => "name_only",
        }
    }
}

/// One inventory entry of the book.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookEntry {
    pub rel_path: String,
    pub size: i64,
    /// `None` only on recovered entries whose receipt predates per-item
    /// mtimes.
    pub mtime: Option<i64>,
    /// `"sha256:<hex>"`, where known.
    pub hash: Option<String>,
    pub fate: SourceFate,
    /// The decision behind this entry's fate — `archived` entries carry the
    /// apply; `excluded`/`deleted` the stamping decision; the standings
    /// carry the source's most recent recorded transition (often the
    /// indexing scan). The id cross-references the timeline (`#N`) and the
    /// `{id:06}-{command}.toml` receipt-filename convention, in the gathered
    /// ledger and the archive's live ledger alike. `None` = no recorded
    /// decision — omitted, never guessed.
    pub decision: Option<i64>,
}

impl BookEntry {
    /// Derived from hash presence — structurally incapable of disagreeing
    /// with the data: an unhashed entry can never render content-verified.
    pub fn verification(&self) -> EntryVerification {
        if self.hash.is_some() {
            EntryVerification::ContentVerified
        } else {
            EntryVerification::NameOnly
        }
    }
}

/// One apply-receipt item naming this root as origin — prepared by ops
/// (destination resolved absolute, current locations resolved by hash,
/// latest decision winning when several receipts name one path).
///
/// An origin *enriches* a matching present row into `ArchivedFromHere`; when
/// no row exists at the path it *recovers* the entry outright — move-mode
/// applies relocate the origin row to the destination root, so nothing
/// remains to fetch. Receipt reading is entry recovery, not enrichment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyOrigin {
    pub rel_path: String,
    /// The apply decision whose receipt recorded this origin claim.
    pub decision_id: i64,
    /// `None` for pre-vocabulary receipts — omitted, never guessed.
    pub disposition: Option<OriginDisposition>,
    /// Absolute recorded destination path.
    pub destination: String,
    pub size: i64,
    pub mtime: Option<i64>,
    pub hash: Option<String>,
    pub current_locations: Vec<String>,
}

/// Prepared lookup context for fate derivation — ops fetches, domain
/// derives.
pub struct FateContext<'a> {
    /// Object ids verified present in the archive.
    pub archived: &'a HashSet<i64>,
    /// `decision_id` → family, for the absent rows' stamps.
    pub stamp_families: &'a HashMap<i64, DecisionFamily>,
    /// `decision_id` → recorded reason (entries only where a reason exists).
    pub stamp_reasons: &'a HashMap<i64, String>,
    /// `object_id` → `"sha256:<hex>"`.
    pub object_hashes: &'a HashMap<i64, String>,
    /// `object_id` → current archive locations (absolute paths).
    pub archive_locations: &'a HashMap<i64, Vec<String>>,
    /// `rel_path` → the apply-origin claim for that path.
    pub origins: &'a HashMap<String, ApplyOrigin>,
}

/// Build the full inventory: every source the root ever had. Present and
/// absent rows classify through the account's own bucket derivations;
/// origins without a matching row are recovered as entries of their own.
/// Rows win over origins on a shared path — the row's current story
/// supersedes the receipt's older claim, and the inventory stays
/// one-entry-per-path. Sorted by `rel_path`: that order is the book's tree
/// structure.
pub fn build_book_entries(
    present: &[Source],
    absent: &[Source],
    ctx: &FateContext,
) -> Vec<BookEntry> {
    let mut entries: Vec<BookEntry> = Vec::with_capacity(present.len() + absent.len());
    let mut row_paths: HashSet<&str> = HashSet::with_capacity(present.len() + absent.len());

    for source in present {
        row_paths.insert(source.rel_path.as_str());
        // The line's decision pointer: the fate-determining decision — the
        // apply for archived-from-here (the row's own stamp would name the
        // indexing scan, not the archiving) — else the source's most recent
        // recorded transition.
        let mut decision = source.decision_id;
        let fate = match classify_present(source, ctx.archived) {
            StandingBucket::Excluded => SourceFate::Excluded {
                reason: stamp_reason(source, ctx),
                archive_locations: locations_for(source, ctx),
            },
            StandingBucket::Covered => match ctx.origins.get(source.rel_path.as_str()) {
                Some(origin) => {
                    decision = Some(origin.decision_id);
                    SourceFate::ArchivedFromHere {
                        disposition: origin.disposition,
                        destination: origin.destination.clone(),
                        current_locations: locations_for(source, ctx),
                    }
                }
                None => SourceFate::Covered {
                    locations: locations_for(source, ctx),
                },
            },
            StandingBucket::Unresolved { .. } => SourceFate::PresentAtRetirement,
        };
        entries.push(BookEntry {
            rel_path: source.rel_path.clone(),
            size: source.size,
            mtime: Some(source.mtime),
            hash: hash_for(source, ctx),
            fate,
            decision,
        });
    }

    for source in absent {
        row_paths.insert(source.rel_path.as_str());
        let family = source
            .decision_id
            .and_then(|id| ctx.stamp_families.get(&id).copied());
        let fate = match classify_absent(family) {
            AbsentBucket::Deleted => SourceFate::Deleted {
                reason: stamp_reason(source, ctx),
            },
            AbsentBucket::Unexplained => SourceFate::MissingUnexplained,
        };
        entries.push(BookEntry {
            rel_path: source.rel_path.clone(),
            size: source.size,
            mtime: Some(source.mtime),
            hash: hash_for(source, ctx),
            fate,
            decision: source.decision_id,
        });
    }

    for (rel_path, origin) in ctx.origins {
        if row_paths.contains(rel_path.as_str()) {
            continue;
        }
        entries.push(BookEntry {
            rel_path: rel_path.clone(),
            size: origin.size,
            mtime: origin.mtime,
            hash: origin.hash.clone(),
            fate: SourceFate::ArchivedFromHere {
                disposition: origin.disposition,
                destination: origin.destination.clone(),
                current_locations: origin.current_locations.clone(),
            },
            decision: Some(origin.decision_id),
        });
    }

    entries.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
    entries
}

fn stamp_reason(source: &Source, ctx: &FateContext) -> Option<String> {
    source
        .decision_id
        .and_then(|id| ctx.stamp_reasons.get(&id).cloned())
}

fn hash_for(source: &Source, ctx: &FateContext) -> Option<String> {
    source
        .object_id
        .and_then(|id| ctx.object_hashes.get(&id).cloned())
}

fn locations_for(source: &Source, ctx: &FateContext) -> Vec<String> {
    // Zero-byte objects are contentless (the sweep's rule, applied here the
    // same locally-scoped way; the suite-wide question stays a /vision
    // item): every empty file shares the one empty-content object, so a
    // location list would name every zero-byte file in the archive while
    // answering nothing about *this* file. Fate and account are untouched —
    // only the where-does-this-content-live claim is withheld.
    if source.size == 0 {
        return Vec::new();
    }
    source
        .object_id
        .and_then(|id| ctx.archive_locations.get(&id).cloned())
        .unwrap_or_default()
}

/// The book's verification posture: whether the bound state was
/// scan-verified, or bound on faith — and when the last verification
/// happened. An observation must never read as a certainty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerificationPosture {
    ScanVerified {
        last_scan: i64,
    },
    OnFaith {
        last_scan: Option<i64>,
        reason: &'static str,
    },
}

/// A root that was never scanned, is suspended, or whose path is
/// unreachable binds on faith — the story as last observed.
pub fn derive_posture(
    suspended: bool,
    reachable: bool,
    last_scanned_at: Option<i64>,
) -> VerificationPosture {
    match last_scanned_at {
        None => VerificationPosture::OnFaith {
            last_scan: None,
            reason: "never scanned",
        },
        Some(last_scan) if suspended => VerificationPosture::OnFaith {
            last_scan: Some(last_scan),
            reason: "root suspended",
        },
        Some(last_scan) if !reachable => VerificationPosture::OnFaith {
            last_scan: Some(last_scan),
            reason: "path unreachable",
        },
        Some(last_scan) => VerificationPosture::ScanVerified { last_scan },
    }
}

/// The book directory's name: the root's last path component slugified
/// (lowercased; runs of anything non-alphanumeric collapse to a single `-`),
/// then the retirement date — `/mnt/photos-backup` on 2026-08-02 becomes
/// `photos-backup-2026-08-02`. A bare `/` falls back to `root`. Collision
/// handling (same-name siblings on the shelf) is the caller's concern; this
/// is only the base name.
pub fn book_dir_name(root_path: &str, date: &str) -> String {
    let last = root_path
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or("");
    let mut slug = String::with_capacity(last.len());
    let mut pending_dash = false;
    for c in last.chars() {
        if c.is_alphanumeric() {
            if pending_dash && !slug.is_empty() {
                slug.push('-');
            }
            pending_dash = false;
            slug.extend(c.to_lowercase());
        } else {
            pending_dash = true;
        }
    }
    if slug.is_empty() {
        slug.push_str("root");
    }
    format!("{slug}-{date}")
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

    // classify_present — priority excluded > covered > unresolved

    #[test]
    fn present_source_excluded_beats_archived_object() {
        let s = source(1, Some(10), true, false);
        assert_eq!(
            classify_present(&s, &archived_set(&[10])),
            StandingBucket::Excluded
        );
    }

    #[test]
    fn present_object_excluded_beats_archived_object() {
        let s = source(1, Some(10), false, true);
        assert_eq!(
            classify_present(&s, &archived_set(&[10])),
            StandingBucket::Excluded
        );
    }

    #[test]
    fn present_hashed_in_archive_is_covered() {
        let s = source(1, Some(10), false, false);
        assert_eq!(
            classify_present(&s, &archived_set(&[10])),
            StandingBucket::Covered
        );
    }

    #[test]
    fn present_hashed_uncovered_is_unresolved() {
        let s = source(1, Some(10), false, false);
        assert_eq!(
            classify_present(&s, &archived_set(&[])),
            StandingBucket::Unresolved { unhashed: false }
        );
    }

    #[test]
    fn present_unhashed_is_unresolved_unhashed() {
        let s = source(1, None, false, false);
        assert_eq!(
            classify_present(&s, &archived_set(&[])),
            StandingBucket::Unresolved { unhashed: true }
        );
    }

    #[test]
    fn present_unhashed_but_excluded_is_excluded() {
        let s = source(1, None, true, false);
        assert_eq!(
            classify_present(&s, &archived_set(&[])),
            StandingBucket::Excluded
        );
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
            &archived_set(&[10, 11]),
            &extractions,
            &families,
        )
    }

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
        let a = build_account(&[], &[], &archived_set(&[]), &extractions, &HashMap::new());
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
        let a = build_account(&[], &[], &archived_set(&[]), &extractions, &HashMap::new());
        assert_eq!(a.archived_bytes, None);
    }

    #[test]
    fn empty_account_is_all_zero_with_zero_bytes() {
        let a = build_account(&[], &[], &archived_set(&[]), &[], &HashMap::new());
        assert_eq!(a.archived_files, 0);
        assert_eq!(a.archived_bytes, Some(0), "zero known bytes, not omitted");
        assert_eq!(a.standing(), 0);
        assert_eq!(a.ever_indexed(), Some(0));
        assert_eq!(derive_readiness(&a), Readiness::NoBlockersFound);
    }

    // derive_readiness + blocks

    #[test]
    fn unresolved_sources_make_not_ready_with_both_counts() {
        let a = mixed_account();
        assert_eq!(
            derive_readiness(&a),
            Readiness::NotReady {
                unresolved: 2,
                unhashed: 1
            }
        );
    }

    #[test]
    fn no_unresolved_is_no_blockers_found_never_ready() {
        let present = vec![source(1, Some(10), false, false)];
        let a = build_account(&present, &[], &archived_set(&[10]), &[], &HashMap::new());
        assert_eq!(derive_readiness(&a), Readiness::NoBlockersFound);
    }

    #[test]
    fn unhashed_only_root_still_blocks() {
        // The "user forgot to hash" case: unhashed ⊆ unresolved, blocks.
        let present = vec![source(1, None, false, false)];
        let a = build_account(&present, &[], &archived_set(&[]), &[], &HashMap::new());
        assert_eq!(
            derive_readiness(&a),
            Readiness::NotReady {
                unresolved: 1,
                unhashed: 1
            }
        );
    }

    #[test]
    fn gate_blocks_without_allow_and_passes_with() {
        let not_ready = Readiness::NotReady {
            unresolved: 5,
            unhashed: 0,
        };
        assert!(not_ready.blocks(false));
        assert!(!not_ready.blocks(true));
        assert!(!Readiness::NoBlockersFound.blocks(false));
        assert!(!Readiness::NoBlockersFound.blocks(true));
    }

    // build_book_entries — the fate truth table

    #[derive(Default)]
    struct Ctx {
        archived: HashSet<i64>,
        families: HashMap<i64, DecisionFamily>,
        reasons: HashMap<i64, String>,
        hashes: HashMap<i64, String>,
        locations: HashMap<i64, Vec<String>>,
        origins: HashMap<String, ApplyOrigin>,
    }

    impl Ctx {
        fn context(&self) -> FateContext<'_> {
            FateContext {
                archived: &self.archived,
                stamp_families: &self.families,
                stamp_reasons: &self.reasons,
                object_hashes: &self.hashes,
                archive_locations: &self.locations,
                origins: &self.origins,
            }
        }
    }

    fn at(mut s: Source, rel_path: &str) -> Source {
        s.rel_path = rel_path.to_string();
        s
    }

    fn origin(rel_path: &str, disposition: Option<OriginDisposition>) -> ApplyOrigin {
        ApplyOrigin {
            rel_path: rel_path.to_string(),
            decision_id: 41,
            disposition,
            destination: format!("/archive/{rel_path}"),
            size: 77,
            mtime: Some(1_704_067_200),
            hash: Some("sha256:origin".to_string()),
            current_locations: vec![format!("/archive/{rel_path}")],
        }
    }

    fn add_origin(cx: &mut Ctx, o: ApplyOrigin) {
        cx.origins.insert(o.rel_path.clone(), o);
    }

    #[test]
    fn excluded_present_entry_carries_both_truths() {
        let mut cx = Ctx::default();
        cx.archived.insert(10);
        cx.reasons.insert(5, "duplicate of archive".to_string());
        cx.hashes.insert(10, "sha256:abc".to_string());
        cx.locations.insert(10, vec!["/archive/a.jpg".to_string()]);
        let present = vec![stamped(source(1, Some(10), true, false), 5)];

        let entries = build_book_entries(&present, &[], &cx.context());
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].fate,
            SourceFate::Excluded {
                reason: Some("duplicate of archive".to_string()),
                archive_locations: vec!["/archive/a.jpg".to_string()],
            }
        );
        assert_eq!(entries[0].hash.as_deref(), Some("sha256:abc"));
    }

    #[test]
    fn entry_decision_points_at_the_fate_determining_decision() {
        let mut cx = Ctx::default();
        cx.archived.insert(10);
        cx.hashes.insert(10, "sha256:abc".to_string());
        // A present row whose stamp is its indexing scan (#5) but whose
        // origin claim comes from apply #41: the line points at the apply —
        // the stamp would name the indexing, not the archiving.
        add_origin(&mut cx, origin("a.jpg", Some(OriginDisposition::Retained)));
        let archived_here = stamped(at(source(1, Some(10), false, false), "a.jpg"), 5);
        // A covered row with no origin keeps its own stamp as the pointer.
        let covered = stamped(at(source(2, Some(10), false, false), "b.jpg"), 5);
        // A recovered entry (no row) carries its receipt's decision.
        add_origin(
            &mut cx,
            origin("gone.jpg", Some(OriginDisposition::Relocated)),
        );

        let entries = build_book_entries(&[archived_here, covered], &[], &cx.context());
        let by_path: HashMap<&str, &BookEntry> =
            entries.iter().map(|e| (e.rel_path.as_str(), e)).collect();
        assert_eq!(by_path["a.jpg"].decision, Some(41));
        assert_eq!(by_path["b.jpg"].decision, Some(5));
        assert_eq!(by_path["gone.jpg"].decision, Some(41));
    }

    #[test]
    fn zero_byte_entry_carries_its_fate_but_no_locations() {
        // Every empty file shares the one empty-content object — a location
        // list would name every zero-byte file in the archive while saying
        // nothing about this one. Fate and verification stay honest: size 0
        // is on the line, the hash genuinely proves the (empty) content.
        let mut cx = Ctx::default();
        cx.archived.insert(10);
        cx.hashes.insert(10, "sha256:empty".to_string());
        cx.locations
            .insert(10, vec!["/archive/a/.DS_Store".to_string()]);
        let mut empty = source(1, Some(10), false, false);
        empty.size = 0;

        let entries = build_book_entries(&[empty], &[], &cx.context());
        assert_eq!(entries[0].fate, SourceFate::Covered { locations: vec![] });
        assert_eq!(
            entries[0].verification(),
            EntryVerification::ContentVerified
        );
    }

    #[test]
    fn object_excluded_present_entry_is_excluded() {
        let cx = Ctx::default();
        let present = vec![source(1, Some(10), false, true)];
        let entries = build_book_entries(&present, &[], &cx.context());
        assert!(matches!(entries[0].fate, SourceFate::Excluded { .. }));
    }

    #[test]
    fn covered_present_with_origin_is_archived_from_here() {
        let mut cx = Ctx::default();
        cx.archived.insert(10);
        // Live locations come from the archive-location map, not the origin:
        // the map is compile-time truth, the origin the recorded claim.
        cx.locations
            .insert(10, vec!["/archive/live.jpg".to_string()]);
        add_origin(&mut cx, origin("f1.jpg", Some(OriginDisposition::Retained)));
        let present = vec![source(1, Some(10), false, false)];

        let entries = build_book_entries(&present, &[], &cx.context());
        assert_eq!(
            entries.len(),
            1,
            "origin enriches the row, never duplicates"
        );
        assert_eq!(
            entries[0].fate,
            SourceFate::ArchivedFromHere {
                disposition: Some(OriginDisposition::Retained),
                destination: "/archive/f1.jpg".to_string(),
                current_locations: vec!["/archive/live.jpg".to_string()],
            }
        );
        // Row metadata wins over the receipt item's.
        assert_eq!(entries[0].size, 100);
        assert_eq!(entries[0].mtime, Some(0));
    }

    #[test]
    fn covered_present_without_origin_is_covered() {
        let mut cx = Ctx::default();
        cx.archived.insert(10);
        cx.locations.insert(10, vec!["/archive/a.jpg".to_string()]);
        let present = vec![source(1, Some(10), false, false)];

        let entries = build_book_entries(&present, &[], &cx.context());
        assert_eq!(
            entries[0].fate,
            SourceFate::Covered {
                locations: vec!["/archive/a.jpg".to_string()],
            }
        );
    }

    #[test]
    fn unresolved_present_is_present_at_retirement() {
        let cx = Ctx::default();
        let present = vec![
            source(1, Some(10), false, false), // hashed, uncovered
            source(2, None, false, false),     // unhashed
        ];
        let entries = build_book_entries(&present, &[], &cx.context());
        assert!(entries
            .iter()
            .all(|e| e.fate == SourceFate::PresentAtRetirement));
    }

    #[test]
    fn absent_observe_stamp_entry_is_deleted_with_reason() {
        let mut cx = Ctx::default();
        cx.families.insert(100, DecisionFamily::Observe);
        cx.reasons.insert(100, "spring cleaning".to_string());
        let absent = vec![stamped(source(6, Some(14), false, false), 100)];

        let entries = build_book_entries(&[], &absent, &cx.context());
        assert_eq!(
            entries[0].fate,
            SourceFate::Deleted {
                reason: Some("spring cleaning".to_string()),
            }
        );
    }

    #[test]
    fn absent_other_or_no_stamp_is_missing_unexplained() {
        let mut cx = Ctx::default();
        cx.families.insert(200, DecisionFamily::Exclude);
        let absent = vec![
            stamped(source(7, None, false, false), 200),
            source(8, None, false, false),
        ];
        let entries = build_book_entries(&[], &absent, &cx.context());
        assert!(entries
            .iter()
            .all(|e| e.fate == SourceFate::MissingUnexplained));
    }

    #[test]
    fn origin_without_row_recovers_the_entry_with_item_metadata() {
        let mut cx = Ctx::default();
        add_origin(
            &mut cx,
            origin("moved/away.jpg", Some(OriginDisposition::Relocated)),
        );

        let entries = build_book_entries(&[], &[], &cx.context());
        assert_eq!(entries.len(), 1);
        let e = &entries[0];
        assert_eq!(e.rel_path, "moved/away.jpg");
        assert_eq!(e.size, 77);
        assert_eq!(e.mtime, Some(1_704_067_200));
        assert_eq!(e.hash.as_deref(), Some("sha256:origin"));
        assert_eq!(
            e.fate,
            SourceFate::ArchivedFromHere {
                disposition: Some(OriginDisposition::Relocated),
                destination: "/archive/moved/away.jpg".to_string(),
                current_locations: vec!["/archive/moved/away.jpg".to_string()],
            }
        );
    }

    #[test]
    fn legacy_origin_without_disposition_recovers_with_disposition_omitted() {
        let mut cx = Ctx::default();
        add_origin(&mut cx, origin("old/receipt.jpg", None));

        let entries = build_book_entries(&[], &[], &cx.context());
        assert!(matches!(
            &entries[0].fate,
            SourceFate::ArchivedFromHere {
                disposition: None,
                ..
            }
        ));
    }

    #[test]
    fn unhashed_entries_can_never_be_content_verified() {
        let mut cx = Ctx::default();
        let mut recovered = origin("gone.jpg", Some(OriginDisposition::Relocated));
        recovered.hash = None;
        add_origin(&mut cx, recovered);
        let present = vec![source(1, None, false, false)];
        let absent = vec![source(2, None, false, false)];

        let entries = build_book_entries(&present, &absent, &cx.context());
        assert_eq!(entries.len(), 3);
        assert!(entries
            .iter()
            .all(|e| e.verification() == EntryVerification::NameOnly));
    }

    #[test]
    fn hashed_entry_is_content_verified() {
        let mut cx = Ctx::default();
        cx.hashes.insert(10, "sha256:abc".to_string());
        let present = vec![source(1, Some(10), false, false)];
        let entries = build_book_entries(&present, &[], &cx.context());
        assert_eq!(
            entries[0].verification(),
            EntryVerification::ContentVerified
        );
    }

    #[test]
    fn entries_sort_by_rel_path_tree_order() {
        let mut cx = Ctx::default();
        add_origin(
            &mut cx,
            origin("b/moved.jpg", Some(OriginDisposition::Relocated)),
        );
        let present = vec![
            at(source(1, None, false, false), "c/last.jpg"),
            at(source(2, None, false, false), "a/first.jpg"),
        ];
        let entries = build_book_entries(&present, &[], &cx.context());
        let paths: Vec<&str> = entries.iter().map(|e| e.rel_path.as_str()).collect();
        assert_eq!(paths, vec!["a/first.jpg", "b/moved.jpg", "c/last.jpg"]);
    }

    // The agreement law: folding the entries' fates reproduces the account.

    #[test]
    fn entries_fold_to_the_account_buckets() {
        let present = vec![
            source(1, Some(10), false, false), // covered, origin → archived from here
            source(2, Some(11), false, false), // covered, no origin
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
        let archived = archived_set(&[10, 11]);
        let account = build_account(&present, &absent, &archived, &[], &families);

        let mut cx = Ctx {
            archived,
            families,
            ..Ctx::default()
        };
        add_origin(&mut cx, origin("f1.jpg", Some(OriginDisposition::Retained))); // enriches present row 1
        add_origin(
            &mut cx,
            origin("recovered.jpg", Some(OriginDisposition::Relocated)),
        ); // no row — recovered

        let entries = build_book_entries(&present, &absent, &cx.context());
        let recovered = entries.len() as i64 - (present.len() + absent.len()) as i64;
        let count =
            |f: fn(&SourceFate) -> bool| entries.iter().filter(|e| f(&e.fate)).count() as i64;

        assert_eq!(recovered, 1);
        assert_eq!(
            count(|f| matches!(f, SourceFate::Excluded { .. })),
            account.excluded
        );
        assert_eq!(
            count(|f| matches!(f, SourceFate::Deleted { .. })),
            account.deleted
        );
        assert_eq!(
            count(|f| matches!(f, SourceFate::MissingUnexplained)),
            account.unexplained_missing
        );
        assert_eq!(
            count(|f| matches!(f, SourceFate::PresentAtRetirement)),
            account.unresolved
        );
        assert_eq!(
            count(|f| matches!(
                f,
                SourceFate::Covered { .. } | SourceFate::ArchivedFromHere { .. }
            )) - recovered,
            account.covered
        );
        assert_eq!(
            entries.len() as i64,
            account.standing() + account.deleted + account.unexplained_missing + recovered
        );
    }

    // Fate words: terminal fates derive, standings are the constants.

    #[test]
    fn terminal_fate_words_derive_through_fate_transition() {
        let archived = SourceFate::ArchivedFromHere {
            disposition: None,
            destination: String::new(),
            current_locations: vec![],
        };
        assert_eq!(
            archived.word(),
            fate_transition(DecisionFamily::Archive, FateAspect::Present)
                .unwrap()
                .as_str()
        );
        let excluded = SourceFate::Excluded {
            reason: None,
            archive_locations: vec![],
        };
        assert_eq!(
            excluded.word(),
            fate_transition(DecisionFamily::Exclude, FateAspect::Present)
                .unwrap()
                .as_str()
        );
        let deleted = SourceFate::Deleted { reason: None };
        assert_eq!(
            deleted.word(),
            fate_transition(DecisionFamily::Observe, FateAspect::Absent)
                .unwrap()
                .as_str()
        );
    }

    // derive_posture

    #[test]
    fn reachable_scanned_root_is_scan_verified() {
        assert_eq!(
            derive_posture(false, true, Some(42)),
            VerificationPosture::ScanVerified { last_scan: 42 }
        );
    }

    #[test]
    fn suspended_unreachable_and_unscanned_bind_on_faith() {
        assert_eq!(
            derive_posture(true, true, Some(42)),
            VerificationPosture::OnFaith {
                last_scan: Some(42),
                reason: "root suspended",
            }
        );
        assert_eq!(
            derive_posture(false, false, Some(42)),
            VerificationPosture::OnFaith {
                last_scan: Some(42),
                reason: "path unreachable",
            }
        );
        assert_eq!(
            derive_posture(false, true, None),
            VerificationPosture::OnFaith {
                last_scan: None,
                reason: "never scanned",
            }
        );
    }

    #[test]
    fn standing_words_are_the_named_constants() {
        assert_eq!(
            SourceFate::Covered { locations: vec![] }.word(),
            STANDING_COVERED
        );
        assert_eq!(SourceFate::PresentAtRetirement.word(), STANDING_PRESENT);
        assert_eq!(
            SourceFate::MissingUnexplained.word(),
            STANDING_MISSING_UNEXPLAINED
        );
    }

    // -----------------------------------------------------------------------
    // book_dir_name
    // -----------------------------------------------------------------------

    #[test]
    fn book_dir_name_slugs_the_last_component() {
        assert_eq!(
            book_dir_name("/mnt/photos-backup", "2026-08-02"),
            "photos-backup-2026-08-02"
        );
        assert_eq!(
            book_dir_name("/mnt/drives/Old Laptop", "2026-08-02"),
            "old-laptop-2026-08-02"
        );
        // A trailing slash never changes the name.
        assert_eq!(
            book_dir_name("/mnt/photos-backup/", "2026-08-02"),
            "photos-backup-2026-08-02"
        );
    }

    #[test]
    fn book_dir_name_collapses_punctuation_without_edge_dashes() {
        // Runs of non-alphanumerics collapse to one dash; none leads or trails.
        assert_eq!(
            book_dir_name("/mnt/__weird!!  (copy) __", "2026-08-02"),
            "weird-copy-2026-08-02"
        );
        // Unicode letters survive, lowercased.
        assert_eq!(
            book_dir_name("/mnt/Fotoś Über", "2026-08-02"),
            "fotoś-über-2026-08-02"
        );
    }

    #[test]
    fn book_dir_name_bare_root_falls_back() {
        assert_eq!(book_dir_name("/", "2026-08-02"), "root-2026-08-02");
        assert_eq!(book_dir_name("", "2026-08-02"), "root-2026-08-02");
    }
}
