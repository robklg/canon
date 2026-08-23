//! Scan's shared result/parameter types: the pipeline's per-file outcome and
//! stats types — the walk's and the hash pass's alike — options, the
//! walk-observability trait, the deletion receipt's document shape, and the
//! shared timestamp helper the other four stratum files call.
//!
//! The hash pass's `HashStats` lives here rather than beside the pass, because
//! its whole field list is handed to `ScanStats` through one guarded carry: the
//! two structs are read together, so they are declared together.
//!
//! The receipt body lives here rather than beside the writer in `receipt.rs`
//! because the pipeline's result types carry it as a field; this is the one
//! stratum both can depend on without an edge against the grain. The document
//! shape is scan's own — the shared machinery serializes any body and never
//! inspects one, so the only part of a receipt that is common across commands
//! is its `[meta]` table.

use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

use crate::core::ops::receipt::ReceiptMeta;
use crate::scan::domain::StandingDebt;

/// Deletion receipt — written when `canon scan` observes sources gone from disk
/// (`present → absent`). Source-root-local: it lists exactly the sources deleted
/// from one root and lives at that root's `.canon-ledger/`, because the loss is
/// part of that drive's story. A single scan may emit several — one per affected
/// root.
#[derive(Serialize)]
pub(super) struct DeletionReceipt {
    pub meta: ReceiptMeta,
    pub items: Vec<DeletionReceiptItem>,
}

/// One item in a deletion receipt — a single source that went missing. The root
/// is shared across the receipt (placement is per-root), so only the rel_path is
/// recorded per item.
#[derive(Debug, Serialize)]
pub struct DeletionReceiptItem {
    pub rel_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
    pub size: i64,
    pub mtime: i64,
    /// The source's `decision_id` before the deletion — the predecessor in the
    /// provenance chain. Captured before the `present → absent` flip so a later
    /// revival resetting `sources.decision_id` cannot erase it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_decision_id: Option<i64>,
}

/// Classification of a source's fate during scan.
pub enum SourceOutcome {
    Seen,
    Missing,
    Disconnected,
}

/// Action taken for a processed file.
pub enum FileAction {
    New,
    Modified,
    Moved,
    Unchanged,
}

/// Accumulated scan statistics.
#[derive(Default)]
pub struct ScanStats {
    pub scanned: u64,
    pub new: u64,
    pub updated: u64,
    pub moved: u64,
    pub unchanged: u64,
    pub missing: u64,
    pub disconnected: u64,
    pub skipped: u64,
    pub hashed: u64,
    pub unexpected_hash_changes: u64,
    /// Present sources in the scanned scope that still hold no content
    /// identity when the scan is over. Set once after the hash pass, like
    /// `hashed` — it is a fact about what stands at the end, not a per-root
    /// tally, so it is deliberately outside the fold.
    pub unhashed: u64,
    /// How many of `unhashed` this scan tried to read and could not. Rides the
    /// debt line as its qualifier, the way the backlog count rides `hashed`,
    /// so a scan that failed on forty files does not report the same standing
    /// debt as one that was told not to hash. Never exceeds `unhashed`: both
    /// come from one query over one row set.
    pub unhashed_unreadable: u64,
    /// Number of walk roots where missing detection was skipped (mount guard).
    /// Counted in the stats — and thus the durable decision summary — so a scan
    /// that *couldn't verify* absence is distinguishable from one that verified
    /// nothing was missing.
    pub missing_detection_skipped: u64,
    /// New paths that share content with a file already indexed at another
    /// path, all of which are still standing on disk — hardlink companions.
    /// Counted per path, and only where the content agrees: this annotates the
    /// `new` count, so it must never exceed it, and it makes a claim about
    /// shared content that a bare inode-number collision does not support.
    /// Its job is the one-time convergence flood, when every twin path gains a
    /// row at once, which must read as bookkeeping catching up rather than as
    /// a library that suddenly grew.
    pub hardlink_companions: u64,
    /// Files whose possible move could not be checked, because a root holding
    /// a nominated row did not answer. Never claimed as moves; counted per
    /// file — a file can make at most one move — and the root is named, so
    /// ignorance is stated rather than passing for "not moved".
    pub moves_unverified: u64,
    /// Number of walk entries that could not be read (permissions, I/O). A
    /// non-zero count gates missing detection for the affected root — part of
    /// the tree went unseen, and unseen must never read as deleted — and lands
    /// in the durable decision summary like the mount-guard skip.
    pub walk_errors: u64,
    /// Symlinks the walk saw and did not index. Canon indexes what a path
    /// holds, and a symlink holds a reference rather than content: following
    /// one would index the target twice, once under each path, and claim
    /// content for a path that has none of its own. Counted rather than
    /// silently dropped, because a path visible on disk and absent from the
    /// index is otherwise a mystery — the user sees a file, `canon ls` does
    /// not, and nothing says why.
    pub symlinks_skipped: u64,
    /// Everything else the walk saw that is not a regular file — fifos,
    /// sockets, block and character devices. Not content, and nothing a hash
    /// or a copy could meaningfully take, so they are skipped for the same
    /// reason and stated for the same reason.
    pub special_skipped: u64,
    /// Files the hash pass read because the row carried no content identity,
    /// not because anything about them changed — standing debt this scan paid
    /// off. A root indexed once with `--no-hash` pays its whole backlog on the
    /// next plain scan, which can be the entire root at once; the count
    /// annotates the hashed line so a large pay-down reads as bookkeeping
    /// catching up rather than as a library that suddenly needs rehashing.
    ///
    /// Counted where the hash succeeds, never where it is queued: it qualifies
    /// the `hashed` count, so it must never exceed it. A backlog file that
    /// could not be read was not paid off — it is still debt, and the line
    /// below says so.
    pub hash_backlog: u64,
}

impl ScanStats {
    /// Fold one root's counts into a running total.
    ///
    /// Every counter a walk produces is added here, and the fields are
    /// enumerated beside the struct that declares them — a scan of several
    /// roots reports one summary, and a counter that reaches the per-root
    /// result but not this fold is computed correctly and then dropped on the
    /// floor. The hash pass's five counters are deliberately absent: the pass
    /// runs once, after every root, so they are carried rather than folded —
    /// by `carry_hash_pass` below, which is guarded the same way for the same
    /// reason.
    pub fn absorb(&mut self, root: &ScanStats) {
        self.scanned += root.scanned;
        self.new += root.new;
        self.updated += root.updated;
        self.moved += root.moved;
        self.unchanged += root.unchanged;
        self.missing += root.missing;
        self.disconnected += root.disconnected;
        self.skipped += root.skipped;
        self.missing_detection_skipped += root.missing_detection_skipped;
        self.hardlink_companions += root.hardlink_companions;
        self.moves_unverified += root.moves_unverified;
        self.walk_errors += root.walk_errors;
        self.symlinks_skipped += root.symlinks_skipped;
        self.special_skipped += root.special_skipped;
    }

    /// Carry the hash pass's answer into the totals.
    ///
    /// The twin of `absorb`, and rest-less for the same reason: the pass runs
    /// once after every root, so its counters are set here rather than folded,
    /// and a field added to `HashStats` must fail the build until someone
    /// decides where it lands. It went unguarded for the project's whole life,
    /// and the cost was exactly the failure the shape predicts — the count of
    /// files the pass could not read was computed correctly, dropped at this
    /// hand-off, and absent from every durable record of every scan.
    pub fn carry_hash_pass(&mut self, pass: &HashPassResult) {
        let HashPassResult {
            stats:
                HashStats {
                    hashed,
                    backlog_hashed,
                    unexpected_hash_changes,
                    // Spent, not carried: the ids are what the debt query below
                    // joins against, and the number the reader is told is that
                    // query's answer, never this list's length.
                    errored_ids: _spent,
                },
            standing_debt: StandingDebt { total, unreadable },
        } = pass;

        self.hashed = *hashed;
        self.hash_backlog = *backlog_hashed;
        self.unexpected_hash_changes = *unexpected_hash_changes;
        self.unhashed = *total;
        self.unhashed_unreadable = *unreadable;
    }

    /// Compose the scan summary message.
    pub fn compose_summary(&self) -> String {
        // The companions annotation rides the "new" count rather than trailing
        // the line, because it explains that number and nothing else. The first
        // scan after per-path grain arrives reports every hardlink twin at once
        // — tens of thousands of them on a deduplicated library — and a flood of
        // unexplained "new" is indistinguishable from something having gone
        // wrong. Stated whenever there are any, never only when the number is
        // large: the user decides what is surprising.
        let new_clause = if self.hardlink_companions > 0 {
            format!(
                "{} new ({} hardlink companions of already-indexed files)",
                self.new, self.hardlink_companions
            )
        } else {
            format!("{} new", self.new)
        };
        let mut summary = format!(
            "Scanned {} files: {}, {} updated, {} moved, {} unchanged, {} missing",
            self.scanned, new_clause, self.updated, self.moved, self.unchanged, self.missing
        );
        if self.moves_unverified > 0 {
            summary.push_str(&format!(
                ", {} possible moves could not be verified",
                self.moves_unverified
            ));
        }
        if self.missing_detection_skipped == 1 {
            summary.push_str(", missing detection skipped (mount unstable)");
        } else if self.missing_detection_skipped > 1 {
            summary.push_str(&format!(
                ", missing detection skipped on {} roots (mount unstable)",
                self.missing_detection_skipped
            ));
        }
        if self.walk_errors > 0 {
            summary.push_str(&format!(
                ", {} walk errors (missing detection skipped)",
                self.walk_errors
            ));
        }
        if self.skipped > 0 {
            summary.push_str(&format!(", {} skipped (read errors)", self.skipped));
        }
        if self.disconnected > 0 {
            summary.push_str(&format!(", {} skipped (disconnected)", self.disconnected));
        }
        // Entries the walk saw and deliberately did not index. Stated for the
        // same reason every other skip is: a path visible on disk and absent
        // from the index must never be a mystery. "skipped" leads the pair so
        // the special-file count is not left standing as a bare number with no
        // verb when it is the only one there.
        match (self.symlinks_skipped, self.special_skipped) {
            (0, 0) => {}
            (links, 0) => summary.push_str(&format!(", skipped {links} symlinks")),
            (0, special) => summary.push_str(&format!(", skipped {special} special files")),
            (links, special) => summary.push_str(&format!(
                ", skipped {links} symlinks, {special} special files"
            )),
        }
        if self.hashed > 0 {
            // The backlog annotation rides the hashed count for the same reason
            // the companions annotation rides "new": it explains that number and
            // nothing else. A root indexed once without hashing pays its whole
            // backlog on the next plain scan, which can be every file it holds
            // — a pay-down of that size has to read as bookkeeping catching up,
            // not as a library that suddenly needs re-reading.
            if self.hash_backlog > 0 {
                summary.push_str(&format!(
                    "\nHashed {} files ({} from backlog)",
                    self.hashed, self.hash_backlog
                ));
            } else {
                summary.push_str(&format!("\nHashed {} files", self.hashed));
            }
        }
        if self.unhashed > 0 {
            // Stated in every mode, because the omission is the same one either
            // way: content Canon holds no identity for is invisible to coverage
            // and to cluster selection, and silence there is what let debt sit
            // unnoticed from one January to the following August. Under
            // --no-hash this is the debt the user declined to pay; after a
            // hashing scan it is what a failed pay-down left standing.
            //
            // When some of it could not be read, that rides the line as its
            // qualifier — the same shape the backlog count takes on "hashed" —
            // so the reader can tell debt Canon was told to leave from debt it
            // tried to clear and could not. The per-file reasons stay on
            // stderr; this is the standing fact, and the durable record needs
            // it more than the terminal does.
            if self.unhashed_unreadable > 0 {
                summary.push_str(&format!(
                    "\n{} sources remain unhashed ({} could not be read)",
                    self.unhashed, self.unhashed_unreadable
                ));
            } else {
                summary.push_str(&format!("\n{} sources remain unhashed", self.unhashed));
            }
        }
        summary
    }
}

/// Result of the hash pipeline.
///
/// Lives here beside `ScanStats` rather than with the pass that produces it,
/// because the hand-off between the two is guarded by `carry_hash_pass` above:
/// a guard that forces a decision about every field reads as one piece only
/// when the two field lists sit together. `hash.rs` already depends on this
/// stratum, so the pair costs no edge against the grain.
#[derive(Default)]
pub struct HashStats {
    pub hashed: u64,
    /// How many of `hashed` were standing debt rather than work this scan's
    /// walk caused. Counted here, beside the success it reports, because it
    /// qualifies `hashed` and a qualifier that outruns what it qualifies is a
    /// number no reader can make sense of: a file queued from the backlog and
    /// then found unreadable was not paid off.
    pub backlog_hashed: u64,
    pub unexpected_hash_changes: u64,
    /// The sources this pass tried to read and could not — kept as ids, not a
    /// tally, because the number that reaches the summary is not this one. A
    /// file that failed here may have been hashed a moment later by a
    /// concurrent scan, or may sit outside the scopes this scan speaks about;
    /// what the reader is told is how much of the *standing debt* is
    /// unreadable, which only a query over the same rows as the debt itself
    /// can answer. These ids are that query's input.
    pub errored_ids: Vec<i64>,
}

/// What the hash pass did, and what it left standing.
pub struct HashPassResult {
    pub stats: HashStats,
    /// Present sources in the walked scopes that still hold no identity, and
    /// how much of that this scan could not read.
    pub standing_debt: StandingDebt,
}

/// Why the hash pass must visit a file. Decided once by the walk's gate and
/// carried here, because two of the pass's own answers are readings of it: a
/// file whose basis changed is exactly one admitted for `Basis`, and a file
/// paying off debt is exactly one admitted for `Backlog`.
#[derive(Debug, PartialEq)]
pub enum HashNeed {
    /// The file is newly indexed, or its content evidence changed — whatever
    /// identity Canon held for it is stale.
    Basis,
    /// The row stood still and carries no object: content Canon has never
    /// identified. This is standing debt, and a plain scan pays it.
    Backlog,
    /// `--verify` — re-read the content whatever is already known about it.
    Reverify,
}

/// A file that needs full hashing after the walk completes.
pub struct FileToHash {
    pub source_id: i64,
    pub full_path: PathBuf,
    pub old_object_id: Option<i64>,
    pub need: HashNeed,
}

/// Result of scanning a single root.
pub struct ScanRootResult {
    pub stats: ScanStats,
    pub files_to_hash: Vec<FileToHash>,
    /// Sources that went missing during this scan, captured before the
    /// `present → absent` flip for the deletion receipt. Empty when receipt
    /// capture is off (`capture_deletions = false`) or nothing was deleted.
    pub deleted_items: Vec<DeletionReceiptItem>,
    /// Warnings collected during scan (disconnected storage, errors).
    pub warnings: Vec<String>,
}

/// Where `--missing` would act: the root that holds the path, and the path's
/// own remainder below it. Produced by `resolve_missing_target`, which is the
/// one place the act's preconditions are asked — by the act, and by the
/// interface deciding whether to suggest it.
#[derive(Debug)]
pub struct MissingTarget {
    pub root_id: i64,
    /// Absolute path of the containing root.
    pub root_path: String,
    /// The path's remainder below its root. Empty when the root itself was
    /// named.
    pub rel_prefix: String,
}

/// Result of marking a path's sources deleted via `--missing`.
#[derive(Debug)]
pub struct MarkMissingPathResult {
    /// The root that contained the deleted sources.
    pub root_id: i64,
    /// Absolute path of that root (for source-local receipt placement).
    pub root_path: String,
    /// The path's remainder below its root — the folder the deletion was
    /// aimed at. Empty when the whole root was named. The caller records it
    /// as the decision's scope, so the trail says where this happened rather
    /// than falling back to `global`.
    pub rel_prefix: String,
    /// How many present sources were flipped to absent.
    pub missing_count: u64,
    /// Deletion-receipt items captured before the flip. Empty when receipt
    /// capture is off (`capture_deletions = false`) or nothing was present.
    pub deleted_items: Vec<DeletionReceiptItem>,
}

/// Parameters controlling scan behavior.
pub struct ScanOptions {
    /// Whether to compute partial hashes during the walk.
    pub hash: bool,
    /// Whether to re-hash files that already have a hash.
    pub hash_all: bool,
    /// Whether to treat device ID mismatches as missing (--ignore-device-id).
    pub ignore_device_id: bool,
}

/// Observability for the scan pipeline. The interface implements this
/// to update progress bars, emit warnings, etc.
pub trait ScanProgress {
    /// Called after each file is processed.
    fn on_file(&self, path: &str, action: &FileAction);
    /// Called when a walk error is encountered (e.g., permission denied).
    fn on_walk_error(&self, error: &str);
    /// Called when process_file fails for a specific file.
    fn on_process_error(&self, path: &str, error: &str);
}

pub fn current_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::ops::receipt::{write_receipt, ReceiptLocus};
    use tempfile::tempdir;

    fn sample_meta(command: &str) -> ReceiptMeta {
        ReceiptMeta {
            receipt_version: 1,
            decision_id: 42,
            command: command.to_string(),
            transition: "deleted".to_string(),
            posture: "observed".to_string(),
            status: "completed".to_string(),
            timestamp: 1700000000,
            scope: None,
            reason: None,
            summary: "test".to_string(),
            canon_version: "0.0.0".to_string(),
            command_line: "canon test".to_string(),
            manifest: None,
            origin_disposition: None,
            locus: ReceiptLocus {
                path: "/vol".to_string(),
                id: 42,
            },
        }
    }

    #[test]
    fn test_deletion_receipt_serializes() {
        let receipt = DeletionReceipt {
            meta: sample_meta("scan"),
            items: vec![
                DeletionReceiptItem {
                    rel_path: "vacation/IMG_001.jpg".to_string(),
                    hash: Some("sha256:gone".to_string()),
                    size: 2048,
                    mtime: 1700000000,
                    previous_decision_id: Some(7),
                },
                DeletionReceiptItem {
                    rel_path: "notes.txt".to_string(),
                    hash: None,
                    size: 12,
                    mtime: 1700000100,
                    previous_decision_id: None,
                },
            ],
        };
        let out = toml::to_string_pretty(&receipt).unwrap();
        assert!(out.contains("[[items]]"));
        assert!(out.contains("rel_path = \"vacation/IMG_001.jpg\""));
        // Unhashed item omits hash; only the Some item carries it.
        assert_eq!(out.matches("hash =").count(), 1);
        assert!(out.contains("hash = \"sha256:gone\""));
        // Only the item with a prior decision carries previous_decision_id.
        assert_eq!(out.matches("previous_decision_id").count(), 1);
        assert!(out.contains("previous_decision_id = 7"));
    }

    /// A fully-populated deletion receipt survives the real write path and
    /// reads back as TOML. The test above serializes the body; this one pins
    /// the *file* — the writer prepends a comment header and writes to an
    /// `.incomplete` path, and neither is exercised anywhere else.
    #[test]
    fn deletion_receipt_round_trips_through_the_writer() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("000042-scan.toml");
        let receipt = DeletionReceipt {
            meta: sample_meta("scan"),
            items: vec![DeletionReceiptItem {
                rel_path: "vacation/IMG_001.jpg".to_string(),
                hash: Some("sha256:gone".to_string()),
                size: 2048,
                mtime: 1700000000,
                previous_decision_id: Some(7),
            }],
        };

        write_receipt(&path, &receipt, "2 sources gone").unwrap();

        let written = std::fs::read_to_string(dir.path().join("000042-scan.incomplete")).unwrap();
        let parsed: toml::Table =
            toml::from_str(&written).expect("receipt must parse back as TOML");
        assert_eq!(parsed["meta"]["transition"].as_str(), Some("deleted"));
        assert_eq!(parsed["items"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn absorb_carries_every_counter_a_walk_produces() {
        // The destructuring below has no `..` rest, so adding a field to
        // ScanStats fails to compile here until someone decides whether the
        // fold carries it. That is the point: the previous spelling enumerated
        // these fields far from the struct, where a new counter could be
        // computed per root, never summed, and silently absent from the summary
        // and from the durable decision record.
        let root = ScanStats {
            scanned: 1,
            new: 2,
            updated: 3,
            moved: 4,
            unchanged: 5,
            missing: 6,
            disconnected: 7,
            skipped: 8,
            hashed: 100,
            unexpected_hash_changes: 200,
            unhashed: 300,
            missing_detection_skipped: 9,
            hardlink_companions: 11,
            moves_unverified: 12,
            walk_errors: 10,
            hash_backlog: 13,
            symlinks_skipped: 14,
            special_skipped: 15,
            unhashed_unreadable: 400,
        };

        let mut total = ScanStats::default();
        total.absorb(&root);
        total.absorb(&root);

        let ScanStats {
            scanned,
            new,
            updated,
            moved,
            unchanged,
            missing,
            disconnected,
            skipped,
            hashed,
            unexpected_hash_changes,
            unhashed,
            missing_detection_skipped,
            hardlink_companions,
            moves_unverified,
            walk_errors,
            hash_backlog,
            symlinks_skipped,
            special_skipped,
            unhashed_unreadable,
        } = total;

        // An array rather than a tuple: the counters outgrew the arity Rust's
        // tuple trait impls reach, and the next counter must not have to
        // reshape this assertion to be added.
        assert_eq!(
            [
                scanned,
                new,
                updated,
                moved,
                unchanged,
                missing,
                disconnected,
                skipped,
                missing_detection_skipped,
                hardlink_companions,
                moves_unverified,
                walk_errors,
                symlinks_skipped,
                special_skipped
            ],
            [2, 4, 6, 8, 10, 12, 14, 16, 18, 22, 24, 20, 28, 30]
        );
        // Set once from the hash pass after every root — what it did, how much
        // of that was debt, and what it left standing — never folded per root.
        assert_eq!(
            (
                hashed,
                hash_backlog,
                unexpected_hash_changes,
                unhashed,
                unhashed_unreadable
            ),
            (0, 0, 0, 0, 0)
        );
    }

    #[test]
    fn carry_hash_pass_places_every_counter_the_pass_produces() {
        // The twin of the fold guard above. `carry_hash_pass` destructures
        // `HashStats` with no `..` rest, so a counter added to the pass fails
        // to compile until someone decides where it lands. The hole that closed
        // was exactly this shape: the count of files the pass could not read
        // was produced correctly for the project's whole life and dropped at
        // this hand-off, so no durable record of any scan ever held it.
        let pass = HashPassResult {
            stats: HashStats {
                hashed: 7,
                backlog_hashed: 3,
                unexpected_hash_changes: 1,
                errored_ids: vec![41, 42],
            },
            standing_debt: StandingDebt {
                total: 9,
                unreadable: 2,
            },
        };

        let mut total = ScanStats {
            new: 5,
            ..Default::default()
        };
        total.carry_hash_pass(&pass);

        assert_eq!(
            (
                total.hashed,
                total.hash_backlog,
                total.unexpected_hash_changes,
                total.unhashed,
                total.unhashed_unreadable
            ),
            (7, 3, 1, 9, 2)
        );

        // What the reader is told is the debt query's answer, not the length of
        // the id list: two ids failed here and two are still standing debt only
        // because this fixture made them agree. They are separate numbers, and
        // the summary must speak the one bounded to what the scan walked.
        assert_eq!(pass.stats.errored_ids.len(), 2);

        // The walk's own counters are untouched: carrying is not folding.
        assert_eq!(total.new, 5);
    }

    #[test]
    fn compose_summary_records_missing_detection_skip() {
        // The skip must reach the durable decision summary — a scan that
        // couldn't verify absence must not read like one that verified
        // nothing was missing.
        let stats = ScanStats {
            scanned: 10,
            missing_detection_skipped: 1,
            ..Default::default()
        };
        assert!(stats
            .compose_summary()
            .contains("missing detection skipped (mount unstable)"));

        let stats = ScanStats {
            missing_detection_skipped: 2,
            ..Default::default()
        };
        assert!(stats
            .compose_summary()
            .contains("missing detection skipped on 2 roots (mount unstable)"));

        assert!(!ScanStats::default()
            .compose_summary()
            .contains("missing detection"));
    }

    #[test]
    fn compose_summary_annotates_the_companion_flood() {
        // The convergence line, in the durable decision summary: the trail must
        // be able to say later *why* one scan indexed thirty thousand files.
        let stats = ScanStats {
            scanned: 40,
            new: 30,
            hardlink_companions: 28,
            ..Default::default()
        };
        assert!(stats
            .compose_summary()
            .contains("30 new (28 hardlink companions of already-indexed files)"));

        // With no companions the line reads exactly as it always has — the
        // annotation is additive, never a permanent change of shape.
        let plain = ScanStats {
            scanned: 40,
            new: 30,
            ..Default::default()
        };
        assert!(plain.compose_summary().contains("30 new,"));
        assert!(!plain.compose_summary().contains("companions"));
    }

    #[test]
    fn compose_summary_records_unverified_moves() {
        // Ignorance reaches the durable record too: a scan that could not check
        // whether files moved must not read like one that checked and found
        // none.
        let stats = ScanStats {
            scanned: 5,
            new: 2,
            moves_unverified: 2,
            ..Default::default()
        };
        assert!(stats
            .compose_summary()
            .contains("2 possible moves could not be verified"));
        assert!(!ScanStats::default()
            .compose_summary()
            .contains("could not be verified"));
    }

    #[test]
    fn compose_summary_annotates_a_backlog_pay_down() {
        // The durable summary must be able to answer, months later, why one
        // scan hashed a whole root: not because the files changed, but because
        // they had never been read.
        let stats = ScanStats {
            scanned: 500,
            unchanged: 500,
            hashed: 500,
            hash_backlog: 480,
            ..Default::default()
        };
        assert!(stats
            .compose_summary()
            .contains("Hashed 500 files (480 from backlog)"));

        // With no debt paid the line reads exactly as it always has — the
        // annotation is additive, never a permanent change of shape.
        let plain = ScanStats {
            scanned: 500,
            hashed: 20,
            ..Default::default()
        };
        assert!(plain.compose_summary().contains("Hashed 20 files"));
        assert!(!plain.compose_summary().contains("backlog"));
    }

    #[test]
    fn compose_summary_states_debt_left_standing() {
        // Stated in whichever mode leaves it: --no-hash declines to pay, a
        // failed pay-down cannot. Either way the omission is durably visible,
        // because content with no identity is invisible everywhere else.
        let declined = ScanStats {
            scanned: 40,
            unhashed: 40,
            ..Default::default()
        };
        assert!(declined
            .compose_summary()
            .contains("40 sources remain unhashed"));

        // A pay-down of 40 where 2 files could not be read: 38 hashed, all 38
        // of them backlog, and the 2 still owed. The backlog number is what was
        // *paid*, never what was attempted — a parenthetical larger than the
        // count it qualifies is a number no reader can act on.
        let residue = ScanStats {
            scanned: 40,
            hashed: 38,
            hash_backlog: 38,
            unhashed: 2,
            ..Default::default()
        };
        let summary = residue.compose_summary();
        assert!(summary.contains("Hashed 38 files (38 from backlog)"));
        assert!(summary.contains("2 sources remain unhashed"));

        // Nothing standing, nothing said.
        assert!(!ScanStats::default()
            .compose_summary()
            .contains("remain unhashed"));
    }

    #[test]
    fn compose_summary_qualifies_debt_the_scan_could_not_read() {
        // Debt Canon was told to leave and debt it tried to clear and could not
        // are different facts, and only one of them means something is wrong
        // with the storage. The per-file reasons go to stderr and are gone; the
        // durable decision record is where the standing fact has to live.
        let residue = ScanStats {
            scanned: 40,
            hashed: 37,
            hash_backlog: 37,
            unhashed: 3,
            unhashed_unreadable: 3,
            ..Default::default()
        };
        assert!(residue
            .compose_summary()
            .contains("3 sources remain unhashed (3 could not be read)"));

        // A part, not the whole: forty standing, three of them unreadable.
        let mixed = ScanStats {
            scanned: 40,
            unhashed: 40,
            unhashed_unreadable: 3,
            ..Default::default()
        };
        assert!(mixed
            .compose_summary()
            .contains("40 sources remain unhashed (3 could not be read)"));

        // Nothing failed: the line reads exactly as it does without the
        // qualifier, which is additive and never a permanent change of shape.
        let declined = ScanStats {
            scanned: 40,
            unhashed: 40,
            ..Default::default()
        };
        let summary = declined.compose_summary();
        assert!(summary.contains("40 sources remain unhashed"));
        assert!(!summary.contains("could not be read"));
    }

    #[test]
    fn compose_summary_states_the_entries_the_walk_would_not_index() {
        // A path the user can see on disk and cannot find in the index must
        // never be silent. Both counts reach the durable decision summary, and
        // "skipped" leads the pair so neither reads as a bare number.
        let both = ScanStats {
            scanned: 10,
            symlinks_skipped: 4,
            special_skipped: 2,
            ..Default::default()
        };
        assert!(both
            .compose_summary()
            .contains(", skipped 4 symlinks, 2 special files"));

        let links_only = ScanStats {
            scanned: 10,
            symlinks_skipped: 4,
            ..Default::default()
        };
        let summary = links_only.compose_summary();
        assert!(summary.contains(", skipped 4 symlinks"));
        assert!(!summary.contains("special"));

        // A special file alone still says what happened to it.
        let special_only = ScanStats {
            scanned: 10,
            special_skipped: 2,
            ..Default::default()
        };
        let summary = special_only.compose_summary();
        assert!(summary.contains(", skipped 2 special files"));
        assert!(!summary.contains("symlink"));

        // Nothing skipped, nothing said — the annotation is additive, never a
        // permanent change of shape.
        let quiet = ScanStats::default().compose_summary();
        assert!(!quiet.contains("symlink"));
        assert!(!quiet.contains("special"));
    }

    #[test]
    fn compose_summary_records_walk_errors() {
        // Walk errors reach the durable summary with the skip stated — an
        // incomplete walk must not read like a complete one.
        let stats = ScanStats {
            scanned: 5,
            walk_errors: 3,
            ..Default::default()
        };
        assert!(stats
            .compose_summary()
            .contains("3 walk errors (missing detection skipped)"));
        assert!(!ScanStats::default()
            .compose_summary()
            .contains("walk errors"));
    }
}
