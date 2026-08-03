//! Retirement operations: the readiness review.
//!
//! `compute_readiness` is the one structural fetch of the retirement
//! ceremony — the readiness review is its first lens, and the book compile
//! (the second lens) builds on the same substrate. One fetch, two
//! renderings: the gate and the book can never drift apart.

use std::collections::{HashMap, HashSet};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use rusqlite::{Connection, Transaction, TransactionBehavior};
use serde::{Deserialize, Serialize};

use crate::domain::config::{LedgerConfig, RecordingMode};
use crate::domain::decision::{Decision, DecisionCommand, DecisionStatus};
use crate::domain::extraction::{DecisionExtraction, OriginDisposition};
use crate::domain::format::format_size;
use crate::domain::note::note_display_path;
use crate::domain::retire::{
    book_dir_name, build_account, build_book_entries, derive_posture, derive_readiness,
    disposition_word, ApplyOrigin, BookEntry, FateContext, Readiness, ResolutionAccount,
    SourceFate, VerificationPosture, STANDING_COVERED, STANDING_MISSING_UNEXPLAINED,
    STANDING_PRESENT,
};
use crate::domain::scope::DecisionScope;
use crate::domain::trail::{
    decision_family, fate_transition, DecisionFamily, FateAspect, TimelineEvent,
};
use crate::domain::{format_count, Note, Root, Source};
use crate::ops;
use crate::ops::decision::{DecisionCounts, DecisionParams, DecisionRecorder};
use crate::ops::ledger::{read_apply_receipt, ReceiptRead};
use crate::ops::trail::{TrailParams, TrailResult, TrailView};
use crate::repo;
use crate::repo::decision::DecisionScopeRow;

/// Facts the review states beside the account — facts, never warnings, and
/// none of them block. Unexplained-missing and unhashed counts render from
/// the account itself (single source of truth).
pub struct GapFacts {
    pub last_scanned_at: Option<i64>,
    /// Whether the root's path is a reachable directory right now. An
    /// unreachable root retires on faith — surfaced, never refused.
    pub reachable: bool,
    /// Cluster-generate decisions on this root newer than the last apply
    /// that drew from it — possible open intentions, nothing stronger.
    pub open_cluster_intentions: i64,
}

/// The readiness review: identity, account, gap facts, verdict — plus the
/// review-time basis the release movement will re-check before removal
/// (the world can move between review and removal; the ceremony must
/// notice).
pub struct ReadinessReview {
    pub root: Root,
    /// When the earliest surviving row was first indexed (min `scanned_at`
    /// over present + absent rows) — data-level evidence that predates
    /// decision recording. `None` = no rows. The first *recorded* scan is
    /// the timeline's opening line, not an identity claim.
    pub first_indexed: Option<i64>,
    pub account: ResolutionAccount,
    pub gaps: GapFacts,
    pub readiness: Readiness,
    /// Review-time basis: total source rows (present + absent). Consumed by
    /// the release movement's world-moved re-check.
    pub snapshot_source_count: i64,
    /// Review-time basis: highest decision id seen touching this root.
    pub snapshot_max_decision_id: Option<i64>,
}

/// Ceremony-entry policy: what may be retired at all. An archive root is
/// refused — the archive is where books live, not what gets retired. And
/// with no archive root registered the book has no shelf: a bookless
/// retirement is `roots rm`, which remains available.
pub fn validate_retire_target(roots: &[Root], root_id: i64, config: &LedgerConfig) -> Result<()> {
    let root = roots
        .iter()
        .find(|r| r.id == root_id)
        .ok_or_else(|| anyhow::anyhow!("Root {root_id} not found"))?;
    if root.role == "archive" {
        bail!(
            "Cannot retire {}: an archive root is not retired — the archive is where the books live",
            root.path
        );
    }
    if ops::receipt::resolve_ledger_root(roots, config).is_none() {
        bail!("Retirement needs an archive root to hold the record — no archive root is registered. To remove the root without binding its story: canon roots rm");
    }
    Ok(())
}

/// The root's complete story, fetched once — the retirement ceremony's one
/// structural substrate. The readiness review and the book compile are both
/// lenses over this, so the ceremony fetches once and the gate and the book
/// read the same world by construction.
pub struct RootStory {
    pub root: Root,
    /// The full fleet — receipt-locus resolution (the compile) and shelf
    /// placement (the ceremony) both need roots beyond the retiring one.
    pub roots: Vec<Root>,
    pub present: Vec<Source>,
    pub absent: Vec<Source>,
    /// Object ids among the present rows verified present in the archive.
    pub archived: HashSet<i64>,
    /// Extraction rows whose origin is this root.
    pub extractions: Vec<DecisionExtraction>,
    pub scope_rows: Vec<DecisionScopeRow>,
    /// Every decision touching the root (stamps, scopes, extractions),
    /// deduped, ascending by id.
    pub decisions: Vec<Decision>,
    pub stamp_families: HashMap<i64, DecisionFamily>,
    /// Fetch-time observation: whether the root's path is a reachable
    /// directory. An unreachable root retires on faith — surfaced, never
    /// refused.
    pub reachable: bool,
    /// When the earliest surviving row was first indexed (min `scanned_at`,
    /// present + absent) — identity evidence that predates decision
    /// recording. A scan-decision date would overclaim on any root older
    /// than the trail.
    pub first_indexed: Option<i64>,
    /// Highest decision id seen touching the root — computed over the raw
    /// referenced ids at fetch time, not over `decisions` (a stamped id may
    /// no longer resolve to a row and must still count as world state).
    pub max_decision_id: Option<i64>,
}

pub fn fetch_root_story(conn: &Connection, root_id: i64) -> Result<RootStory> {
    let roots = repo::root::fetch_all(conn)?;
    let root = roots
        .iter()
        .find(|r| r.id == root_id)
        .ok_or_else(|| anyhow::anyhow!("Root {root_id} not found"))?
        .clone();

    let present = repo::source::batch_fetch_by_roots(conn, &[root_id])?;
    let absent = repo::source::fetch_absent_by_roots(conn, &[root_id])?;

    let present_object_ids: Vec<i64> = present.iter().filter_map(|s| s.object_id).collect();
    let archived = repo::object::batch_check_archived(conn, &present_object_ids, None)?;

    let extractions = repo::decision::fetch_extractions_by_origin_root(conn, root_id)?;
    let scope_rows = repo::decision::fetch_scope_rows_by_roots(conn, &[root_id])?;

    // One decision fetch serves every consumer: the absent rows' stamp
    // families, first-scan, the open-intentions comparison, and (for the
    // compile) the per-decision reasons.
    let mut decision_ids: Vec<i64> = absent.iter().filter_map(|s| s.decision_id).collect();
    decision_ids.extend(present.iter().filter_map(|s| s.decision_id));
    decision_ids.extend(scope_rows.iter().map(|r| r.decision_id));
    decision_ids.extend(extractions.iter().map(|r| r.decision_id));
    decision_ids.sort_unstable();
    decision_ids.dedup();
    let decisions = repo::decision::fetch_by_ids(conn, &decision_ids)?;

    let stamp_families: HashMap<i64, DecisionFamily> = decisions
        .iter()
        .map(|d| (d.id, decision_family(&d.command)))
        .collect();

    let reachable = ops::fs::dir_exists(Path::new(&root.path));
    let max_decision_id = decision_ids.last().copied();
    let first_indexed = repo::source::min_scanned_at_by_root(conn, root_id)?;

    Ok(RootStory {
        root,
        roots,
        present,
        absent,
        archived,
        extractions,
        scope_rows,
        decisions,
        stamp_families,
        reachable,
        first_indexed,
        max_decision_id,
    })
}

/// The readiness review as a pure lens over the fetched story.
pub fn readiness_lens(story: &RootStory) -> ReadinessReview {
    let by_id: HashMap<i64, &Decision> = story.decisions.iter().map(|d| (d.id, d)).collect();

    let scope_decision_ids: HashSet<i64> = story.scope_rows.iter().map(|r| r.decision_id).collect();

    let last_apply_from_here = story
        .extractions
        .iter()
        .filter_map(|r| by_id.get(&r.decision_id))
        .map(|d| d.created_at)
        .max();
    let open_cluster_intentions = scope_decision_ids
        .iter()
        .filter_map(|id| by_id.get(id))
        .filter(|d| d.command == DecisionCommand::ClusterGenerate.as_str())
        .filter(|d| match last_apply_from_here {
            Some(last) => d.created_at > last,
            None => true,
        })
        .count() as i64;

    let account = build_account(
        &story.present,
        &story.absent,
        &story.archived,
        &story.extractions,
        &story.stamp_families,
    );
    let readiness = derive_readiness(&account);

    let gaps = GapFacts {
        last_scanned_at: story.root.last_scanned_at,
        reachable: story.reachable,
        open_cluster_intentions,
    };

    ReadinessReview {
        root: story.root.clone(),
        first_indexed: story.first_indexed,
        account,
        gaps,
        readiness,
        snapshot_source_count: (story.present.len() + story.absent.len()) as i64,
        snapshot_max_decision_id: story.max_decision_id,
    }
}

/// Fetch + lens in one call, for callers that need only the review. The
/// ceremony itself fetches once and runs both lenses over the same story.
#[allow(dead_code)]
pub fn compute_readiness(conn: &Connection, root_id: i64) -> Result<ReadinessReview> {
    let story = fetch_root_story(conn, root_id)?;
    Ok(readiness_lens(&story))
}

// ---------------------------------------------------------------------------
// The book compile — the second lens over the story
// ---------------------------------------------------------------------------

pub struct CompileParams {
    /// The user's `--reason`, bound onto the identity page.
    pub reason: Option<String>,
    /// Compile timestamp (unix seconds), stamped as `compiled_at`.
    pub now: i64,
    /// Target directory. Must not exist — a leftover directory is an
    /// explicit collision, never silently overwritten. The ceremony passes
    /// a temp name beside the shelf; the final rename is the placement step.
    pub dest_dir: PathBuf,
    /// The ceremony's own in-flight decision. Two consumers: kept out of
    /// the timeline — compiled before release completes, it cannot narrate
    /// itself; the identity page carries the retirement — and stamped as
    /// `identity.decision_id`, the id the index references this retirement
    /// by, readable from the book alone.
    pub ceremony_decision_id: Option<i64>,
}

#[derive(Debug)]
pub struct CompiledBook {
    /// The compile target. The ceremony reports the *placed* path from its
    /// plan; this one names where the compile itself wrote.
    #[allow(dead_code)]
    pub dir: PathBuf,
    pub entry_count: i64,
    /// Self-explaining gaps, also recorded in `meta.toml` and the README.
    pub gaps: Vec<String>,
    /// Receipt files gathered into `ledger/`; `None` when ungatherable.
    pub ledger_files: Option<usize>,
}

/// Compile the book into `params.dest_dir`. Program errors abort (nothing
/// is destroyed — the compile only reads the DB and writes the target
/// directory); gaps are recorded inside the book, never silent. The README
/// is written last, so a partial compile is self-evident: no README, no
/// complete book.
pub fn compile_book(
    conn: &Connection,
    story: &RootStory,
    params: &CompileParams,
) -> Result<CompiledBook> {
    std::fs::create_dir(&params.dest_dir).with_context(|| {
        format!(
            "Compile target already exists or cannot be created: {}",
            params.dest_dir.display()
        )
    })?;

    let mut gaps: Vec<String> = Vec::new();

    // Compile-only fetches on top of the substrate.
    let notes = repo::note::fetch_by_roots(conn, &[story.root.id])?;
    let trail = ops::trail::compute_trail(
        conn,
        &TrailParams {
            prefixes: vec![story.root.path.clone()],
            timeframe: None,
            include_notes: false,
            limit: None,
        },
    )?;

    let mut object_ids: Vec<i64> = story
        .present
        .iter()
        .chain(story.absent.iter())
        .filter_map(|s| s.object_id)
        .collect();
    object_ids.sort_unstable();
    object_ids.dedup();
    let objects = repo::object::batch_fetch_by_ids(conn, &object_ids)?;
    let object_hashes: HashMap<i64, String> = objects
        .iter()
        .map(|(id, o)| (*id, format!("{}:{}", o.hash_type, o.hash_value)))
        .collect();
    let archive_locations = repo::object::batch_find_archive_paths(conn, &object_ids)?;

    let origins = collect_origins(conn, story, &mut gaps)?;

    let stamp_reasons: HashMap<i64, String> = story
        .decisions
        .iter()
        .filter_map(|d| d.reason.clone().map(|r| (d.id, r)))
        .collect();

    let ctx = FateContext {
        archived: &story.archived,
        stamp_families: &story.stamp_families,
        stamp_reasons: &stamp_reasons,
        object_hashes: &object_hashes,
        archive_locations: &archive_locations,
        origins: &origins,
    };
    let entries = build_book_entries(&story.present, &story.absent, &ctx);

    let account = build_account(
        &story.present,
        &story.absent,
        &story.archived,
        &story.extractions,
        &story.stamp_families,
    );
    let posture = derive_posture(
        story.root.suspended,
        story.reachable,
        story.root.last_scanned_at,
    );

    write_inventory(&params.dest_dir, &entries)?;
    write_timeline(&params.dest_dir, &trail, params.ceremony_decision_id)?;
    write_notes(&params.dest_dir, &notes, &story.roots)?;
    let ledger_files = gather_ledger(&params.dest_dir, story, &mut gaps)?;
    let counts = fate_counts(&entries);
    write_meta(
        &params.dest_dir,
        story,
        params,
        &account,
        &posture,
        &counts,
        ledger_files,
        &gaps,
    )?;
    write_readme(
        &params.dest_dir,
        story,
        params,
        &account,
        &posture,
        ledger_files,
        &gaps,
    )?;

    Ok(CompiledBook {
        dir: params.dest_dir.clone(),
        entry_count: entries.len() as i64,
        gaps,
        ledger_files,
    })
}

/// Read the apply receipts of every decision that drew from this root and
/// collect their per-item origin claims. Ascending decision order, so a
/// later apply's claim on a path wins. Items naming another source root are
/// the other side of a multi-origin apply — skipped, not a gap. Unreadable
/// receipts are gaps: per-item origin degrades (covered + directory-level
/// extraction context), recorded, never guessed.
fn collect_origins(
    conn: &Connection,
    story: &RootStory,
    gaps: &mut Vec<String>,
) -> Result<HashMap<String, ApplyOrigin>> {
    let apply_ids: HashSet<i64> = story.extractions.iter().map(|r| r.decision_id).collect();
    let mut applies: Vec<&Decision> = story
        .decisions
        .iter()
        .filter(|d| apply_ids.contains(&d.id))
        .collect();
    applies.sort_by_key(|d| d.id);

    let mut origins: HashMap<String, ApplyOrigin> = HashMap::new();
    for decision in applies {
        let read = match read_apply_receipt(decision, &story.roots) {
            ReceiptRead::Ok(read) => read,
            ReceiptRead::NoReceipt { reason } => {
                gaps.push(format!(
                    "decision #{}: per-item origin unavailable — {reason}",
                    decision.id
                ));
                continue;
            }
            ReceiptRead::Unreachable { reason } => {
                gaps.push(format!(
                    "decision #{}: per-item origin unavailable — receipt unreachable: {reason}",
                    decision.id
                ));
                continue;
            }
            ReceiptRead::Malformed { reason } => {
                gaps.push(format!(
                    "decision #{}: per-item origin unavailable — receipt malformed: {reason}",
                    decision.id
                ));
                continue;
            }
        };

        let base = match &read.doc.meta.locus {
            Some(locus) => locus.path.clone(),
            None => read.receipt_root_path.clone(),
        };
        let disposition = read
            .doc
            .meta
            .origin_disposition
            .as_deref()
            .and_then(OriginDisposition::from_str);
        for item in &read.doc.items {
            if item.source_root != story.root.path {
                continue;
            }
            let destination = if item.destination_rel_path.is_empty() {
                base.clone()
            } else {
                format!("{base}/{}", item.destination_rel_path)
            };
            origins.insert(
                item.source_rel_path.clone(),
                ApplyOrigin {
                    rel_path: item.source_rel_path.clone(),
                    decision_id: decision.id,
                    disposition,
                    destination,
                    size: item.size,
                    mtime: item.mtime,
                    hash: item.hash.clone(),
                    current_locations: Vec::new(),
                },
            );
        }
    }

    // Resolve current archive locations by content hash — the recovered
    // entries' live tier (row-matched entries resolve via object id in the
    // fate context instead).
    let hash_values: Vec<&str> = origins
        .values()
        .filter_map(|o| o.hash.as_deref())
        .map(strip_hash_prefix)
        .collect();
    let by_hash = repo::object::batch_find_archive_info_by_hash(conn, &hash_values)?;
    for origin in origins.values_mut() {
        if let Some(hash) = origin.hash.as_deref() {
            if let Some(locations) = by_hash.get(strip_hash_prefix(hash)) {
                origin.current_locations = locations.iter().map(|(_, p)| p.clone()).collect();
            }
        }
    }

    Ok(origins)
}

/// Receipt hashes are `"sha256:<hex>"`; the object index keys on the bare
/// value.
fn strip_hash_prefix(hash: &str) -> &str {
    hash.split_once(':').map(|(_, v)| v).unwrap_or(hash)
}

fn iso_utc(ts: i64) -> String {
    chrono::DateTime::from_timestamp(ts, 0)
        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%SZ").to_string())
        .unwrap_or_else(|| ts.to_string())
}

fn iso_date(ts: i64) -> String {
    chrono::DateTime::from_timestamp(ts, 0)
        .map(|dt| dt.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| ts.to_string())
}

/// One `inventory.jsonl` line. Dates are ISO-8601 UTC and hashes carry
/// their algorithm prefix — the Canon-independent reader needs no epoch
/// math and no schema knowledge.
#[derive(Serialize)]
struct InventoryLine<'a> {
    path: &'a str,
    size: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    mtime: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    hash: Option<&'a str>,
    fate: &'a str,
    /// The fate-determining decision — cross-references the timeline's `#N`
    /// and the `{id:06}-{command}.toml` receipt-filename convention.
    #[serde(skip_serializing_if = "Option::is_none")]
    decision: Option<i64>,
    verification: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    disposition: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    destination: Option<&'a str>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    locations: Vec<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'a str>,
}

fn write_inventory(dir: &Path, entries: &[BookEntry]) -> Result<()> {
    let file = std::fs::File::create(dir.join("inventory.jsonl"))?;
    let mut w = BufWriter::new(file);
    for entry in entries {
        let (disposition, destination, locations, reason) = match &entry.fate {
            SourceFate::ArchivedFromHere {
                disposition,
                destination,
                current_locations,
            } => (
                disposition.map(disposition_word),
                Some(destination.as_str()),
                current_locations.iter().map(String::as_str).collect(),
                None,
            ),
            SourceFate::Covered { locations } => (
                None,
                None,
                locations.iter().map(String::as_str).collect(),
                None,
            ),
            SourceFate::Excluded {
                reason,
                archive_locations,
            } => (
                None,
                None,
                archive_locations.iter().map(String::as_str).collect(),
                reason.as_deref(),
            ),
            SourceFate::Deleted { reason } => (None, None, Vec::new(), reason.as_deref()),
            SourceFate::PresentAtRetirement | SourceFate::MissingUnexplained => {
                (None, None, Vec::new(), None)
            }
        };
        let line = InventoryLine {
            path: &entry.rel_path,
            size: entry.size,
            mtime: entry.mtime.map(iso_utc),
            hash: entry.hash.as_deref(),
            fate: entry.fate.word(),
            decision: entry.decision,
            verification: entry.verification().word(),
            disposition,
            destination,
            locations,
            reason,
        };
        serde_json::to_writer(&mut w, &line)?;
        w.write_all(b"\n")?;
    }
    w.flush()?;
    Ok(())
}

fn write_timeline(dir: &Path, trail: &TrailResult, exclude_decision_id: Option<i64>) -> Result<()> {
    let mut out = String::from(
        "# Decision timeline\n\nEvery decision that touched this root, oldest first.\n\n",
    );
    let mut any = false;
    if let TrailView::Recent(events) = &trail.view {
        for event in events {
            if let TimelineEvent::Decision(decision) = event {
                // The ceremony's own in-flight decision cannot narrate
                // itself — the book is compiled before release completes,
                // so its row holds no summary yet; the identity page
                // carries the retirement. A *prior* attempt's decision
                // (release declined, interrupted) renders like any other.
                if Some(decision.id) == exclude_decision_id {
                    continue;
                }
                any = true;
                let text = decision
                    .summary
                    .clone()
                    .unwrap_or_else(|| decision.command.clone());
                let mut lines = text.lines();
                out.push_str(&format!(
                    "- {}  #{} {}: {}\n",
                    iso_date(decision.created_at),
                    decision.id,
                    decision.command,
                    lines.next().unwrap_or("")
                ));
                // Continuation lines of a multi-line summary sit indented
                // under their entry, keeping their own relative indent.
                for continuation in lines {
                    out.push_str(&format!("  {continuation}\n"));
                }
                if let Some(reason) = &decision.reason {
                    let mut reason_lines = reason.lines();
                    if let Some(first) = reason_lines.next() {
                        out.push_str(&format!("  reason: {first}\n"));
                    }
                    for continuation in reason_lines {
                        out.push_str(&format!("  {continuation}\n"));
                    }
                }
            }
        }
    }
    if !any {
        out.push_str("No decisions were recorded on this root.\n");
    }
    if trail.unscoped_decisions > 0 {
        out.push_str(&format!(
            "\n{} global decisions fall outside this root's scoped story.\n",
            trail.unscoped_decisions
        ));
    }
    std::fs::write(dir.join("timeline.md"), out)?;
    Ok(())
}

fn write_notes(dir: &Path, notes: &[Note], roots: &[Root]) -> Result<()> {
    let mut out = String::from(
        "# Notes\n\nEvery note on this root, oldest first — the thinking between the actions.\n\n",
    );
    if notes.is_empty() {
        out.push_str("No notes were recorded on this root.\n");
    } else {
        // The one shared note-identity rendering (note list, the trail's
        // mixed timeline, and the book): root-relative, `(root)` for the
        // root itself — never a view-relative `.` a future reader can't
        // anchor.
        let by_id: HashMap<i64, Root> = roots.iter().map(|r| (r.id, r.clone())).collect();
        let mut sorted: Vec<&Note> = notes.iter().collect();
        sorted.sort_by_key(|n| (n.created_at, n.id));
        for note in sorted {
            out.push_str(&format!(
                "- {}  {}: {}\n",
                iso_date(note.created_at),
                note_display_path(note, &by_id, false),
                note.text
            ));
        }
    }
    std::fs::write(dir.join("notes.md"), out)?;
    Ok(())
}

/// Gather the drive-local ledger verbatim — filenames preserved, so
/// `previous_decision_id` chains stay walkable from disk into the book.
/// An unreachable root is a gap (the gather is skipped, the book says so);
/// a reachable root without a ledger simply has nothing to gather; an I/O
/// error mid-copy is an error — the compile stops.
fn gather_ledger(dir: &Path, story: &RootStory, gaps: &mut Vec<String>) -> Result<Option<usize>> {
    let src = Path::new(&story.root.path).join(".canon-ledger");
    if !story.reachable {
        gaps.push(format!(
            "drive-local ledger not gathered — root path unreachable: {}",
            story.root.path
        ));
        return Ok(None);
    }
    if !src.is_dir() {
        return Ok(Some(0));
    }
    let count = ops::fs::copy_tree(&src, &dir.join("ledger"))?;
    Ok(Some(count))
}

fn fate_counts(entries: &[BookEntry]) -> MetaCounts {
    let mut counts = MetaCounts {
        entries: entries.len() as i64,
        archived_from_here: 0,
        covered: 0,
        excluded: 0,
        deleted: 0,
        present: 0,
        missing_unexplained: 0,
    };
    for entry in entries {
        match &entry.fate {
            SourceFate::ArchivedFromHere { .. } => counts.archived_from_here += 1,
            SourceFate::Covered { .. } => counts.covered += 1,
            SourceFate::Excluded { .. } => counts.excluded += 1,
            SourceFate::Deleted { .. } => counts.deleted += 1,
            SourceFate::PresentAtRetirement => counts.present += 1,
            SourceFate::MissingUnexplained => counts.missing_unexplained += 1,
        }
    }
    counts
}

// TOML field order law: values (version, gaps) must precede the sub-tables,
// or serialization fails — same constraint receipt meta lives under.
#[derive(Serialize)]
struct BookMeta<'a> {
    version: u32,
    gaps: &'a [String],
    identity: MetaIdentity<'a>,
    account: MetaAccount,
    posture: MetaPosture,
    counts: &'a MetaCounts,
    ledger: MetaLedger,
}

#[derive(Serialize)]
struct MetaIdentity<'a> {
    path: &'a str,
    role: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    comment: Option<&'a str>,
    suspended: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    first_indexed: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_scan: Option<String>,
    compiled_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    decision_id: Option<i64>,
    canon_version: &'a str,
}

#[derive(Serialize)]
struct MetaAccount {
    archived_files: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    archived_bytes: Option<i64>,
    archived_moved: i64,
    archived_copied: i64,
    archived_unrecorded: i64,
    deleted: i64,
    unexplained_missing: i64,
    covered: i64,
    excluded: i64,
    unresolved: i64,
    unhashed_unresolved: i64,
    standing: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    ever_indexed: Option<i64>,
}

#[derive(Serialize)]
struct MetaPosture {
    posture: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_scan: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'static str>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct MetaCounts {
    entries: i64,
    archived_from_here: i64,
    covered: i64,
    excluded: i64,
    deleted: i64,
    present: i64,
    missing_unexplained: i64,
}

#[derive(Serialize, Deserialize)]
struct MetaLedger {
    gathered: bool,
    files: usize,
}

#[allow(clippy::too_many_arguments)]
fn write_meta(
    dir: &Path,
    story: &RootStory,
    params: &CompileParams,
    account: &ResolutionAccount,
    posture: &VerificationPosture,
    counts: &MetaCounts,
    ledger_files: Option<usize>,
    gaps: &[String],
) -> Result<()> {
    let (posture_word, posture_last_scan, posture_reason) = match posture {
        VerificationPosture::ScanVerified { last_scan } => {
            ("scan_verified", Some(iso_utc(*last_scan)), None)
        }
        VerificationPosture::OnFaith { last_scan, reason } => {
            ("on_faith", last_scan.map(iso_utc), Some(*reason))
        }
    };
    let meta = BookMeta {
        version: 1,
        gaps,
        identity: MetaIdentity {
            path: &story.root.path,
            role: &story.root.role,
            comment: story.root.comment.as_deref(),
            suspended: story.root.suspended,
            first_indexed: story.first_indexed.map(iso_utc),
            last_scan: story.root.last_scanned_at.map(iso_utc),
            compiled_at: iso_utc(params.now),
            reason: params.reason.as_deref(),
            decision_id: params.ceremony_decision_id,
            canon_version: env!("CARGO_PKG_VERSION"),
        },
        account: MetaAccount {
            archived_files: account.archived_files,
            archived_bytes: account.archived_bytes,
            archived_moved: account.archived_moved,
            archived_copied: account.archived_copied,
            archived_unrecorded: account.archived_unrecorded,
            deleted: account.deleted,
            unexplained_missing: account.unexplained_missing,
            covered: account.covered,
            excluded: account.excluded,
            unresolved: account.unresolved,
            unhashed_unresolved: account.unhashed_unresolved,
            standing: account.standing(),
            ever_indexed: account.ever_indexed(),
        },
        posture: MetaPosture {
            posture: posture_word,
            last_scan: posture_last_scan,
            reason: posture_reason,
        },
        counts,
        ledger: MetaLedger {
            gathered: ledger_files.is_some(),
            files: ledger_files.unwrap_or(0),
        },
    };
    let body = toml::to_string_pretty(&meta).context("Failed to serialize meta.toml")?;
    std::fs::write(dir.join("meta.toml"), body)?;
    Ok(())
}

fn write_readme(
    dir: &Path,
    story: &RootStory,
    params: &CompileParams,
    account: &ResolutionAccount,
    posture: &VerificationPosture,
    ledger_files: Option<usize>,
    gaps: &[String],
) -> Result<()> {
    let mut out = String::new();
    out.push_str(&format!("# The book of {}\n\n", story.root.path));
    out.push_str(&format!(
        "The bound story of a retired root — complete, self-contained, and readable\n\
         without Canon. Compiled {}.\n\n",
        iso_date(params.now)
    ));

    out.push_str("## Identity\n\n");
    out.push_str(&format!("- path: {}\n", story.root.path));
    out.push_str(&format!("- role: {}\n", story.root.role));
    if let Some(comment) = &story.root.comment {
        out.push_str(&format!("- comment: {comment}\n"));
    }
    if let Some(first) = story.first_indexed {
        out.push_str(&format!("- first indexed: {}\n", iso_date(first)));
    }
    if let Some(last) = story.root.last_scanned_at {
        out.push_str(&format!("- last scan: {}\n", iso_date(last)));
    }
    if let Some(reason) = &params.reason {
        out.push_str(&format!("- reason: {reason}\n"));
    }
    out.push('\n');

    out.push_str("## Resolution account\n\n");
    if let Some(ever) = account.ever_indexed() {
        out.push_str(&format!(
            "Ever indexed here: {} sources.\n\n",
            format_count(ever)
        ));
    }
    out.push_str("The story so far:\n");
    let bytes = account
        .archived_bytes
        .map(|b| format!(", {}", format_size(b)))
        .unwrap_or_default();
    let mut split = vec![];
    if account.archived_moved > 0 {
        split.push(format!("{} moved", format_count(account.archived_moved)));
    }
    if account.archived_copied > 0 {
        split.push(format!("{} copied", format_count(account.archived_copied)));
    }
    if account.archived_unrecorded > 0 {
        split.push(format!(
            "{} unrecorded",
            format_count(account.archived_unrecorded)
        ));
    }
    let split = if split.is_empty() {
        String::new()
    } else {
        format!(" ({})", split.join(", "))
    };
    out.push_str(&format!(
        "- archived from here: {} files{bytes}{split}\n",
        format_count(account.archived_files)
    ));
    out.push_str(&format!(
        "- deleted: {} (scan-observed)\n",
        format_count(account.deleted)
    ));
    out.push_str(&format!(
        "- missing, unexplained: {}\n",
        format_count(account.unexplained_missing)
    ));
    out.push_str(&format!(
        "\nStanding at binding: {} sources\n",
        format_count(account.standing())
    ));
    out.push_str(&format!("- covered: {}\n", format_count(account.covered)));
    out.push_str(&format!("- excluded: {}\n", format_count(account.excluded)));
    let unhashed = if account.unhashed_unresolved > 0 {
        format!(" ({} unhashed)", format_count(account.unhashed_unresolved))
    } else {
        String::new()
    };
    out.push_str(&format!(
        "- unresolved: {}{unhashed}\n\n",
        format_count(account.unresolved)
    ));

    out.push_str("## Verification posture\n\n");
    match posture {
        VerificationPosture::ScanVerified { last_scan } => {
            out.push_str(&format!(
                "Scan-verified; last scan {}.\n\n",
                iso_date(*last_scan)
            ));
        }
        VerificationPosture::OnFaith { last_scan, reason } => {
            let when = last_scan
                .map(|ts| format!("; last scan {}", iso_date(ts)))
                .unwrap_or_default();
            out.push_str(&format!(
                "Bound on faith ({reason}){when} — the story as last observed.\n\n"
            ));
        }
    }

    out.push_str("## Contents\n\n");
    out.push_str(
        "- inventory.jsonl — every source this root ever had, one JSON line each,\n\
         \x20 sorted by path: size, dates, hash where known, and fate.\n",
    );
    out.push_str("- timeline.md — every decision that touched this root, with reasons.\n");
    out.push_str("- notes.md — the notes bound beside the timeline.\n");
    match ledger_files {
        Some(count) => out.push_str(&format!(
            "- ledger/ — the drive-local receipts, gathered verbatim ({} files).\n",
            format_count(count as i64)
        )),
        None => out.push_str("- ledger/ — not gathered (see gaps below).\n"),
    }
    out.push_str("- meta.toml — identity, account, counts, and gaps, machine-readable.\n\n");

    out.push_str("## Gaps\n\n");
    if gaps.is_empty() {
        out.push_str("None: nothing this book should hold is missing from it.\n");
    } else {
        for gap in gaps {
            out.push_str(&format!("- {gap}\n"));
        }
    }

    std::fs::write(dir.join("README.md"), out)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Book verification — the hinge the release movement's safety hangs on
// ---------------------------------------------------------------------------

/// Lenient read side of `meta.toml` for verification. Identity and account
/// are prose for the future reader; verification needs only the claims it
/// can check against the directory.
#[derive(Deserialize)]
struct MetaDoc {
    version: u32,
    #[serde(default)]
    gaps: Vec<String>,
    counts: MetaCounts,
    ledger: MetaLedger,
}

#[derive(Deserialize)]
struct InventoryLineDoc {
    fate: String,
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct BookVerification {
    pub entries: i64,
}

/// Structural verification of a compiled book: parse `meta.toml` back,
/// stream-count the inventory per fate, and require every artifact the meta
/// claims. Deliberately not an existence test — a book that fails here is
/// partial or tampered, and the removal movement must not proceed on it.
pub fn verify_book(dir: &Path) -> Result<BookVerification> {
    let meta_path = dir.join("meta.toml");
    let meta_raw = std::fs::read_to_string(&meta_path)
        .with_context(|| format!("Book meta missing or unreadable: {}", meta_path.display()))?;
    let meta: MetaDoc = toml::from_str(&meta_raw).context("Book meta failed to parse")?;
    if meta.version != 1 {
        bail!(
            "Book meta version {} is not supported by this canon (expected 1)",
            meta.version
        );
    }

    for file in ["README.md", "timeline.md", "notes.md"] {
        if !dir.join(file).is_file() {
            bail!("Book is incomplete: {file} is missing");
        }
    }

    let inventory = std::fs::File::open(dir.join("inventory.jsonl"))
        .context("Book is incomplete: inventory.jsonl is missing")?;
    let reader = std::io::BufReader::new(inventory);
    let mut counted = MetaCounts {
        entries: 0,
        archived_from_here: 0,
        covered: 0,
        excluded: 0,
        deleted: 0,
        present: 0,
        missing_unexplained: 0,
    };
    // The same word derivations the writer used — the never-literal law
    // holds on the read side too.
    let archived = fate_transition(DecisionFamily::Archive, FateAspect::Present)
        .expect("Archive+Present is a registered transition")
        .as_str();
    let excluded = fate_transition(DecisionFamily::Exclude, FateAspect::Present)
        .expect("Exclude is a registered transition")
        .as_str();
    let deleted = fate_transition(DecisionFamily::Observe, FateAspect::Absent)
        .expect("Observe+Absent is a registered transition")
        .as_str();
    for (index, line) in std::io::BufRead::lines(reader).enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let doc: InventoryLineDoc = serde_json::from_str(&line)
            .with_context(|| format!("inventory.jsonl line {} failed to parse", index + 1))?;
        counted.entries += 1;
        match doc.fate.as_str() {
            w if w == archived => counted.archived_from_here += 1,
            w if w == excluded => counted.excluded += 1,
            w if w == deleted => counted.deleted += 1,
            w if w == STANDING_COVERED => counted.covered += 1,
            w if w == STANDING_PRESENT => counted.present += 1,
            w if w == STANDING_MISSING_UNEXPLAINED => counted.missing_unexplained += 1,
            other => bail!(
                "inventory.jsonl line {}: unknown fate word {other:?}",
                index + 1
            ),
        }
    }

    if counted != meta.counts {
        bail!(
            "Book counts disagree with the inventory — meta claims {:?}, the inventory holds {:?}",
            meta.counts,
            counted
        );
    }

    if meta.ledger.gathered {
        let found = count_files(&dir.join("ledger"))?;
        if found != meta.ledger.files {
            bail!(
                "Book ledger disagrees — meta claims {} gathered files, ledger/ holds {}",
                meta.ledger.files,
                found
            );
        }
    } else if meta.gaps.is_empty() {
        bail!("Book says the ledger was not gathered but records no gap explaining it");
    }

    Ok(BookVerification {
        entries: counted.entries,
    })
}

fn count_files(dir: &Path) -> Result<usize> {
    if !dir.is_dir() {
        return Ok(0);
    }
    let mut count = 0;
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            count += count_files(&entry.path())?;
        } else if file_type.is_file() {
            count += 1;
        }
    }
    Ok(count)
}

// ---------------------------------------------------------------------------
// The ceremony — plan, begin, bind
// ---------------------------------------------------------------------------

/// The shelf's directory name at the archive ledger root: a visible place,
/// deliberately not under `.canon-ledger/` — the books are for human eyes.
pub const SHELF_DIR: &str = "retired";

/// Where the book will land — computed before any confirmation, so the
/// interface can state a replacement as awareness, never surprise.
#[derive(Debug)]
pub struct BindPlan {
    pub shelf: PathBuf,
    /// `retired/<name>` — the book's final home.
    pub final_dir: PathBuf,
    /// `retired/.compiling-<name>` — the compile target. Inside the shelf so
    /// the placement rename never crosses a filesystem boundary (atomic).
    pub temp_dir: PathBuf,
    pub ledger_root_id: i64,
    /// A same-root book already stands at `final_dir` (a prior
    /// aborted-after-bind run) — this run replaces it with a fresh compile.
    pub replaces_existing: bool,
    /// `false` → first use: the shelf directory and its README are generated.
    pub shelf_exists: bool,
}

/// Resolve the shelf and the book's name. Collision handling is keyed on the
/// standing book's own `meta.toml` identity — deliberately disk-keyed, never
/// a decision-row lookup, so convergence survives any recording-mode history:
/// the same root's book is replaced (a re-run converges); a different root's
/// book pushes this one to a `-2`/`-3`… sibling; a directory that cannot be
/// identified refuses the ceremony.
pub fn plan_bind(story: &RootStory, config: &LedgerConfig, now: i64) -> Result<BindPlan> {
    let (ledger_root_id, ledger_root_path) =
        ops::receipt::resolve_ledger_root(&story.roots, config).ok_or_else(|| {
            anyhow::anyhow!(
            "Retirement needs an archive root to hold the record — no archive root is registered"
        )
        })?;
    let shelf = PathBuf::from(&ledger_root_path).join(SHELF_DIR);
    let base = book_dir_name(&story.root.path, &iso_date(now));

    let mut chosen: Option<(String, bool)> = None;
    for attempt in 1u32..=99 {
        let name = if attempt == 1 {
            base.clone()
        } else {
            format!("{base}-{attempt}")
        };
        let candidate = shelf.join(&name);
        if !candidate.exists() {
            chosen = Some((name, false));
            break;
        }
        let standing_root = read_book_root_path(&candidate)?;
        if standing_root == story.root.path {
            chosen = Some((name, true));
            break;
        }
    }
    let Some((name, replaces_existing)) = chosen else {
        bail!(
            "No free book name beside {} after 99 attempts — the shelf needs attention",
            shelf.join(&base).display()
        );
    };

    Ok(BindPlan {
        final_dir: shelf.join(&name),
        temp_dir: shelf.join(format!(".compiling-{name}")),
        shelf_exists: shelf.is_dir(),
        shelf,
        ledger_root_id,
        replaces_existing,
    })
}

/// The identity half of a standing book's `meta.toml` — read only to answer
/// "whose book is this?" during collision planning. Deliberately separate
/// from `verify_book`'s `MetaDoc`: different question, different tolerance.
#[derive(Deserialize)]
struct IdentityProbe {
    identity: IdentityProbePath,
}

#[derive(Deserialize)]
struct IdentityProbePath {
    path: String,
}

fn read_book_root_path(dir: &Path) -> Result<String> {
    let refusal = || {
        format!(
            "A directory stands at {} but cannot be identified as a book — refusing to replace what cannot be identified; move it aside and re-run",
            dir.display()
        )
    };
    let raw = std::fs::read_to_string(dir.join("meta.toml")).with_context(refusal)?;
    let probe: IdentityProbe = toml::from_str(&raw).with_context(refusal)?;
    Ok(probe.identity.path)
}

/// The ceremony's parameters, held for the life of one invocation — the
/// config is read once, so recording modes cannot change mid-ceremony.
pub struct CeremonyParams {
    /// The user's `--reason`: bound onto the book and the decision row.
    pub reason: Option<String>,
    pub now: i64,
    pub command_line: String,
    pub config: LedgerConfig,
}

/// The ceremony across its movements: one decision spanning bind and
/// release, with the interface's confirmations between them. Ops owns the
/// ceremony policy (ordering, recording, verification gating); the interface
/// owns the prompts and the printing.
pub struct RetireCeremony {
    story: RootStory,
    plan: BindPlan,
    recorder: DecisionRecorder,
    reason: Option<String>,
    now: i64,
    /// Review-time basis for the release movement's world-moved re-check.
    snapshot_source_count: i64,
    snapshot_max_decision_id: Option<i64>,
}

/// Start the ceremony's two-phase decision (status: `started` — an
/// interrupted ceremony is findable from here on). Called after the first
/// confirmation; a declined prompt records nothing.
pub fn begin_ceremony(
    conn: &Connection,
    story: RootStory,
    review: &ReadinessReview,
    plan: BindPlan,
    params: CeremonyParams,
) -> RetireCeremony {
    let decision = DecisionParams {
        command: DecisionCommand::RootsRetire,
        scope: vec![DecisionScope::new(
            story.root.id,
            story.root.path.clone(),
            String::new(),
        )],
        command_line: params.command_line,
        reason: params.reason.clone().filter(|r| !r.trim().is_empty()),
        record_enabled: params.config.recording != RecordingMode::Off,
        // The book is the decision's artifact — retire never writes a receipt
        // file. The pointer columns are recorded separately at bind, gated on
        // recording alone.
        receipt_enabled: false,
        ledger_config: params.config,
    };
    let recorder = DecisionRecorder::start(conn, &decision, None);

    RetireCeremony {
        story,
        plan,
        recorder,
        reason: params.reason,
        now: params.now,
        snapshot_source_count: review.snapshot_source_count,
        snapshot_max_decision_id: review.snapshot_max_decision_id,
    }
}

/// The placed, verified book — what the bind movement hands the interface
/// for the inspection window.
#[derive(Debug)]
pub struct BoundBook {
    pub dir: PathBuf,
    pub entry_count: i64,
    pub gaps: Vec<String>,
    pub ledger_files: Option<usize>,
    pub replaced_previous: bool,
    pub warnings: Vec<String>,
}

impl RetireCeremony {
    /// The bind movement: shelf, compile to temp, verify, place, pointer.
    ///
    /// Verify-before-touch is the load-bearing order: the book is verified at
    /// its temp name, so a standing book is never touched until the fresh one
    /// is proven whole — and the placement rename is what commits a book, so
    /// anything still at a temp name was structurally never placed.
    pub fn bind(&mut self, conn: &Connection) -> Result<BoundBook> {
        if !self.plan.shelf_exists {
            std::fs::create_dir_all(&self.plan.shelf).with_context(|| {
                format!(
                    "Could not create the shelf at {}",
                    self.plan.shelf.display()
                )
            })?;
        }
        write_shelf_readme(&self.plan.shelf)?;

        // A leftover temp dir is a compile that was never placed — removable
        // without ceremony.
        if self.plan.temp_dir.exists() {
            std::fs::remove_dir_all(&self.plan.temp_dir).with_context(|| {
                format!(
                    "Could not clear the leftover compile at {}",
                    self.plan.temp_dir.display()
                )
            })?;
        }

        let compiled = compile_book(
            conn,
            &self.story,
            &CompileParams {
                reason: self.reason.clone(),
                now: self.now,
                dest_dir: self.plan.temp_dir.clone(),
                ceremony_decision_id: self.recorder.decision_id(),
            },
        )?;
        verify_book(&self.plan.temp_dir).with_context(|| {
            format!(
                "The compiled book failed verification — nothing was placed; the compile is kept for inspection at {}",
                self.plan.temp_dir.display()
            )
        })?;

        place_book(&self.plan)?;

        let name = self
            .plan
            .final_dir
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        self.recorder.record_artifact_pointer(
            conn,
            self.plan.ledger_root_id,
            &format!("{SHELF_DIR}/{name}"),
        );

        Ok(BoundBook {
            dir: self.plan.final_dir.clone(),
            entry_count: compiled.entry_count,
            gaps: compiled.gaps,
            ledger_files: compiled.ledger_files,
            replaced_previous: self.plan.replaces_existing,
            warnings: self.recorder.take_warnings(),
        })
    }

    /// The release movement: one `BEGIN IMMEDIATE` transaction holding the
    /// world-moved re-check, the removal, and the decision's completion — the
    /// re-check's reads must be authoritative against concurrent writers, and
    /// the transaction is short.
    pub fn release(&mut self, conn: &Connection) -> Result<ReleaseOutcome> {
        let book_display = self.plan.final_dir.display().to_string();
        let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)?;

        if let Some(detail) = self.world_moved(&tx)? {
            // Nothing was written — dropping the transaction rolls back.
            drop(tx);
            let summary = format!(
                "The book is bound at {book_display}; the world moved before release ({detail}) — the root remains in the index"
            );
            self.recorder.complete_db(
                conn,
                DecisionStatus::Partial,
                DecisionCounts {
                    attempted: Some(self.snapshot_source_count),
                    completed: None,
                    failed: None,
                    skipped: None,
                },
                &summary,
            );
            return Ok(ReleaseOutcome::WorldMoved {
                detail,
                warnings: self.recorder.take_warnings(),
            });
        }

        let removed = ops::roots::remove_root_data(&tx, self.story.root.id)?;
        let summary = format!(
            "Retired {}: {} sources released; the story is bound at {book_display}",
            self.story.root.path,
            format_count(removed.deleted_sources),
        );
        self.recorder.complete_db(
            &tx,
            DecisionStatus::Completed,
            DecisionCounts {
                attempted: Some(self.snapshot_source_count),
                completed: Some(removed.deleted_sources),
                failed: None,
                skipped: None,
            },
            &summary,
        );
        tx.commit()?;

        Ok(ReleaseOutcome::Released {
            deleted_sources: removed.deleted_sources,
            deleted_notes: removed.deleted_notes,
            summary,
            warnings: self.recorder.take_warnings(),
        })
    }

    /// Has the world moved since the review? Two cheap aggregates against the
    /// review-time snapshots — computed over SQL exactly as `readiness_lens`
    /// derived them from the fetched rows, so equality means "same world".
    /// The ceremony's own decision is excluded: its recording must not read
    /// as the world moving (a concurrent process's decision has a different
    /// id and correctly trips the check).
    fn world_moved(&self, conn: &Connection) -> Result<Option<String>> {
        let count = repo::source::count_all_by_root(conn, self.story.root.id)?;
        if count != self.snapshot_source_count {
            return Ok(Some(format!(
                "the root held {} source rows at review, {} now",
                format_count(self.snapshot_source_count),
                format_count(count)
            )));
        }
        let max = repo::decision::max_decision_id_touching_root(
            conn,
            self.story.root.id,
            self.recorder.decision_id(),
        )?;
        if max != self.snapshot_max_decision_id {
            return Ok(Some(
                "another decision touched this root since the review".to_string(),
            ));
        }
        Ok(None)
    }

    /// The declined second confirmation: the book stands, the root stays.
    /// The decision completes `partial` — a findable state the rm guard and
    /// a re-run both read (the re-run converges by replacing the book).
    pub fn abandon(&mut self, conn: &Connection) -> AbandonResult {
        let summary = format!(
            "The book is bound at {}; the root remains in the index (release declined)",
            self.plan.final_dir.display()
        );
        self.recorder.complete_db(
            conn,
            DecisionStatus::Partial,
            DecisionCounts {
                attempted: Some(self.snapshot_source_count),
                completed: None,
                failed: None,
                skipped: None,
            },
            &summary,
        );
        AbandonResult {
            summary,
            warnings: self.recorder.take_warnings(),
        }
    }

    /// Record a failed bind as `interrupted` — fix-forward: the failure is
    /// findable in the trail, the root untouched. The error itself is the
    /// caller's to propagate; this only records. Returns drained warnings.
    pub fn interrupt(&mut self, conn: &Connection, error: &str) -> Vec<String> {
        let summary = format!("Retirement interrupted during bind: {error}");
        self.recorder.complete_db(
            conn,
            DecisionStatus::Interrupted,
            DecisionCounts {
                attempted: Some(self.snapshot_source_count),
                completed: None,
                failed: None,
                skipped: None,
            },
            &summary,
        );
        self.recorder.take_warnings()
    }
}

/// The release movement's outcome. `WorldMoved` is a ceremony outcome, not a
/// program error: the ceremony stops (root intact, book standing, decision
/// `partial`) and asks to be re-run.
pub enum ReleaseOutcome {
    Released {
        /// Counts carried per the typed-result convention; the summary is
        /// the composed narration the interface prints.
        #[allow(dead_code)]
        deleted_sources: i64,
        #[allow(dead_code)]
        deleted_notes: usize,
        summary: String,
        warnings: Vec<String>,
    },
    WorldMoved {
        detail: String,
        warnings: Vec<String>,
    },
}

pub struct AbandonResult {
    pub summary: String,
    pub warnings: Vec<String>,
}

/// Commit the verified temp compile to its final name. In the replacement
/// case the standing book steps aside first, so there is never a moment with
/// a partial book at the final name — the only non-atomic instant has both
/// the old book (aside) and the new one (placed) present.
fn place_book(plan: &BindPlan) -> Result<()> {
    if !plan.replaces_existing {
        return std::fs::rename(&plan.temp_dir, &plan.final_dir)
            .with_context(|| format!("Could not place the book at {}", plan.final_dir.display()));
    }

    let name = plan
        .final_dir
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let aside = plan.shelf.join(format!(".replaced-{name}"));
    // A leftover aside dir is a book a prior swap already replaced —
    // removable, same standing as a leftover temp.
    if aside.exists() {
        std::fs::remove_dir_all(&aside)?;
    }
    std::fs::rename(&plan.final_dir, &aside).with_context(|| {
        format!(
            "Could not move the standing book aside at {}",
            plan.final_dir.display()
        )
    })?;
    std::fs::rename(&plan.temp_dir, &plan.final_dir).with_context(|| {
        format!(
            "Could not place the book at {} (the previous book is at {})",
            plan.final_dir.display(),
            aside.display()
        )
    })?;
    std::fs::remove_dir_all(&aside)
        .with_context(|| format!("Could not remove the replaced book at {}", aside.display()))?;
    Ok(())
}

/// The shelf explains itself once; the README is never rewritten.
fn write_shelf_readme(shelf: &Path) -> Result<()> {
    let path = shelf.join("README.md");
    if path.exists() {
        return Ok(());
    }
    let text = "\
# The Shelf

This directory holds the books of retired roots — drives and folders whose
complete story was compiled by `canon roots retire` before their index was
removed.

Each book is a plain directory: open its `README.md` and start reading. Books
are self-contained — inventory, decisions, notes, and receipts in plain,
stable formats — and are meant to outlive Canon itself. No database and no
tool is needed to read them.

Keep this directory with your archive. Deleting a book deletes the only
reviewable story of a root that is already gone.
";
    std::fs::write(&path, text)
        .with_context(|| format!("Could not write the shelf README at {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::db::open_in_memory_for_test;
    use crate::repo::insert_test_root;

    fn insert_source(
        conn: &Connection,
        root_id: i64,
        rel_path: &str,
        object_id: Option<i64>,
        present: bool,
        excluded: bool,
        decision_id: Option<i64>,
    ) -> i64 {
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, object_id, device, inode, size, mtime,
                                  partial_hash, scanned_at, last_seen_at, present, excluded, decision_id)
             VALUES (?, ?, ?, 0, 0, 1000, 0, 'hash', 0, 0, ?, ?, ?)",
            rusqlite::params![
                root_id,
                rel_path,
                object_id,
                present as i64,
                excluded as i64,
                decision_id
            ],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn insert_object(conn: &Connection, hash: &str) -> i64 {
        conn.execute(
            "INSERT INTO objects (hash_type, hash_value) VALUES ('sha256', ?)",
            [hash],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn insert_decision(conn: &Connection, command: &str, created_at: i64) -> i64 {
        conn.execute(
            "INSERT INTO decisions (command, command_line, status, canon_version, created_at)
             VALUES (?1, 'test', 'completed', '0', ?2)",
            rusqlite::params![command, created_at],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn scope(conn: &Connection, decision_id: i64, root_id: i64) {
        conn.execute(
            "INSERT INTO decision_scopes (decision_id, root_id, root_path, rel_prefix)
             VALUES (?1, ?2, '/r', '')",
            rusqlite::params![decision_id, root_id],
        )
        .unwrap();
    }

    fn extraction_from(conn: &Connection, decision_id: i64, root_id: i64, files: i64) {
        crate::repo::decision::replace_extractions(
            conn,
            &[crate::domain::extraction::DecisionExtraction {
                decision_id,
                root_id,
                root_path: "/r".to_string(),
                rel_prefix: String::new(),
                files,
                bytes: Some(files * 100),
                destination_root_id: Some(999),
                destination_path: "/archive/dest".to_string(),
                disposition: Some(crate::domain::extraction::OriginDisposition::Relocated),
            }],
        )
        .unwrap();
    }

    fn ledger_config() -> LedgerConfig {
        LedgerConfig::default()
    }

    // validate_retire_target

    #[test]
    fn validate_refuses_an_archive_root() {
        let conn = open_in_memory_for_test();
        insert_test_root(&conn, "/archive", "archive", false);
        let roots = repo::root::fetch_all(&conn).unwrap();
        let err = validate_retire_target(&roots, roots[0].id, &ledger_config()).unwrap_err();
        assert!(err.to_string().contains("archive root is not retired"));
    }

    #[test]
    fn validate_requires_an_archive_root_to_exist() {
        let conn = open_in_memory_for_test();
        let root_id = insert_test_root(&conn, "/source", "source", false);
        let roots = repo::root::fetch_all(&conn).unwrap();
        let err = validate_retire_target(&roots, root_id, &ledger_config()).unwrap_err();
        assert!(err.to_string().contains("needs an archive root"));
        assert!(err.to_string().contains("canon roots rm"));
    }

    #[test]
    fn validate_passes_a_source_root_with_an_archive_registered() {
        let conn = open_in_memory_for_test();
        let root_id = insert_test_root(&conn, "/source", "source", false);
        insert_test_root(&conn, "/archive", "archive", false);
        let roots = repo::root::fetch_all(&conn).unwrap();
        validate_retire_target(&roots, root_id, &ledger_config()).unwrap();
    }

    // compute_readiness — the account, end to end

    #[test]
    fn readiness_accounts_every_bucket_from_real_rows() {
        let conn = open_in_memory_for_test();
        let root = insert_test_root(&conn, "/r", "source", false);
        let archive = insert_test_root(&conn, "/archive", "archive", false);

        // Covered: object also present at the archive.
        let covered_obj = insert_object(&conn, "aa");
        insert_source(
            &conn,
            root,
            "covered.jpg",
            Some(covered_obj),
            true,
            false,
            None,
        );
        insert_source(
            &conn,
            archive,
            "kept.jpg",
            Some(covered_obj),
            true,
            false,
            None,
        );
        // Excluded, unresolved-hashed, unresolved-unhashed.
        let lone_obj = insert_object(&conn, "bb");
        insert_source(&conn, root, "excluded.jpg", None, true, true, None);
        insert_source(
            &conn,
            root,
            "unresolved.jpg",
            Some(lone_obj),
            true,
            false,
            None,
        );
        insert_source(&conn, root, "unhashed.jpg", None, true, false, None);
        // Absent: scan-stamped (deleted) and unstamped (unexplained).
        let scan = insert_decision(&conn, "scan", 100);
        scope(&conn, scan, root);
        insert_source(&conn, root, "deleted.jpg", None, false, false, Some(scan));
        insert_source(&conn, root, "vanished.jpg", None, false, false, None);
        // Archived from here: one apply extraction.
        let apply = insert_decision(&conn, "apply", 200);
        extraction_from(&conn, apply, root, 3);

        let review = compute_readiness(&conn, root).unwrap();
        let a = &review.account;
        assert_eq!(a.covered, 1);
        assert_eq!(a.excluded, 1);
        assert_eq!(a.unresolved, 2);
        assert_eq!(a.unhashed_unresolved, 1);
        assert_eq!(a.deleted, 1);
        assert_eq!(a.unexplained_missing, 1);
        assert_eq!(a.archived_files, 3);
        assert_eq!(a.archived_moved, 3);
        assert_eq!(
            review.readiness,
            Readiness::NotReady {
                unresolved: 2,
                unhashed: 1
            }
        );
        assert_eq!(review.snapshot_source_count, 6);
        assert!(review.snapshot_max_decision_id >= Some(apply));
    }

    #[test]
    fn empty_root_has_zero_account_and_no_blockers() {
        let conn = open_in_memory_for_test();
        let root = insert_test_root(&conn, "/r", "source", false);
        let review = compute_readiness(&conn, root).unwrap();
        assert_eq!(review.account.standing(), 0);
        assert_eq!(review.account.ever_indexed(), Some(0));
        assert_eq!(review.readiness, Readiness::NoBlockersFound);
        assert_eq!(review.snapshot_source_count, 0);
        assert_eq!(review.snapshot_max_decision_id, None);
    }

    #[test]
    fn suspended_root_computes_and_carries_suspension() {
        let conn = open_in_memory_for_test();
        let root = insert_test_root(&conn, "/r", "source", true);
        let review = compute_readiness(&conn, root).unwrap();
        assert!(review.root.is_suspended());
        assert_eq!(review.readiness, Readiness::NoBlockersFound);
    }

    #[test]
    fn unknown_root_errors() {
        let conn = open_in_memory_for_test();
        assert!(compute_readiness(&conn, 999).is_err());
    }

    // first_indexed

    #[test]
    fn first_indexed_is_the_earliest_row_evidence_not_a_decision_date() {
        // A root scanned long before decision recording existed: the only
        // scan *decision* is recent, but the rows carry the older truth.
        let conn = open_in_memory_for_test();
        let root = insert_test_root(&conn, "/r", "source", false);
        let recent_scan = insert_decision(&conn, "scan", 9_000);
        scope(&conn, recent_scan, root);
        let old = insert_source(&conn, root, "old.jpg", None, true, false, None);
        let tombstone = insert_source(&conn, root, "gone.jpg", None, false, false, None);
        let newer = insert_source(&conn, root, "new.jpg", None, true, false, None);
        conn.execute("UPDATE sources SET scanned_at = 500 WHERE id = ?", [old])
            .unwrap();
        // A tombstone's evidence counts — the absent rows are part of identity.
        conn.execute(
            "UPDATE sources SET scanned_at = 100 WHERE id = ?",
            [tombstone],
        )
        .unwrap();
        conn.execute("UPDATE sources SET scanned_at = 700 WHERE id = ?", [newer])
            .unwrap();

        let review = compute_readiness(&conn, root).unwrap();
        assert_eq!(review.first_indexed, Some(100));
    }

    #[test]
    fn first_indexed_unknown_without_rows() {
        let conn = open_in_memory_for_test();
        let root = insert_test_root(&conn, "/r", "source", false);
        let review = compute_readiness(&conn, root).unwrap();
        assert_eq!(review.first_indexed, None);
    }

    // open cluster intentions

    #[test]
    fn cluster_generate_after_last_apply_counts_as_open() {
        let conn = open_in_memory_for_test();
        let root = insert_test_root(&conn, "/r", "source", false);
        let apply = insert_decision(&conn, "apply", 100);
        extraction_from(&conn, apply, root, 1);
        let cg = insert_decision(&conn, "cluster_generate", 200);
        scope(&conn, cg, root);

        let review = compute_readiness(&conn, root).unwrap();
        assert_eq!(review.gaps.open_cluster_intentions, 1);
    }

    #[test]
    fn cluster_generate_before_a_later_apply_is_settled() {
        let conn = open_in_memory_for_test();
        let root = insert_test_root(&conn, "/r", "source", false);
        let cg = insert_decision(&conn, "cluster_generate", 100);
        scope(&conn, cg, root);
        let apply = insert_decision(&conn, "apply", 200);
        extraction_from(&conn, apply, root, 1);

        let review = compute_readiness(&conn, root).unwrap();
        assert_eq!(review.gaps.open_cluster_intentions, 0);
    }

    #[test]
    fn cluster_generate_with_no_apply_ever_counts_as_open() {
        let conn = open_in_memory_for_test();
        let root = insert_test_root(&conn, "/r", "source", false);
        let cg = insert_decision(&conn, "cluster_generate", 100);
        scope(&conn, cg, root);

        let review = compute_readiness(&conn, root).unwrap();
        assert_eq!(review.gaps.open_cluster_intentions, 1);
    }

    #[test]
    fn an_apply_drawing_from_another_root_settles_nothing() {
        let conn = open_in_memory_for_test();
        let root = insert_test_root(&conn, "/r", "source", false);
        let other = insert_test_root(&conn, "/other", "source", false);
        let cg = insert_decision(&conn, "cluster_generate", 100);
        scope(&conn, cg, root);
        let apply = insert_decision(&conn, "apply", 200);
        extraction_from(&conn, apply, other, 1);

        let review = compute_readiness(&conn, root).unwrap();
        assert_eq!(review.gaps.open_cluster_intentions, 1);
    }

    // reachability

    #[test]
    fn unreachable_path_reads_as_disconnected() {
        let conn = open_in_memory_for_test();
        let root = insert_test_root(&conn, "/definitely/not/a/real/path", "source", false);
        let review = compute_readiness(&conn, root).unwrap();
        assert!(!review.gaps.reachable);
    }

    #[test]
    fn reachable_path_reads_as_connected() {
        let conn = open_in_memory_for_test();
        let dir = tempfile::tempdir().unwrap();
        let root = insert_test_root(&conn, dir.path().to_str().unwrap(), "source", false);
        let review = compute_readiness(&conn, root).unwrap();
        assert!(review.gaps.reachable);
    }

    // compile_book

    fn set_decision_extras(
        conn: &Connection,
        decision_id: i64,
        summary: Option<&str>,
        reason: Option<&str>,
    ) {
        conn.execute(
            "UPDATE decisions SET summary = ?2, reason = ?3 WHERE id = ?1",
            rusqlite::params![decision_id, summary, reason],
        )
        .unwrap();
    }

    fn set_decision_receipt(conn: &Connection, decision_id: i64, root_id: i64, rel_path: &str) {
        conn.execute(
            "UPDATE decisions SET receipt_root_id = ?2, receipt_rel_path = ?3 WHERE id = ?1",
            rusqlite::params![decision_id, root_id, rel_path],
        )
        .unwrap();
    }

    fn compile_to(conn: &Connection, root_id: i64, dest: &std::path::Path) -> CompiledBook {
        let story = fetch_root_story(conn, root_id).unwrap();
        compile_book(
            conn,
            &story,
            &CompileParams {
                reason: Some("story complete".to_string()),
                now: 1_753_000_000,
                dest_dir: dest.to_path_buf(),
                ceremony_decision_id: None,
            },
        )
        .unwrap()
    }

    fn inventory_lines(dir: &std::path::Path) -> Vec<serde_json::Value> {
        std::fs::read_to_string(dir.join("inventory.jsonl"))
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    #[test]
    fn compile_writes_all_artifacts_with_sorted_iso_inventory() {
        let conn = open_in_memory_for_test();
        let src_dir = tempfile::tempdir().unwrap();
        let book_dir = tempfile::tempdir().unwrap();
        let root_id = insert_test_root(&conn, src_dir.path().to_str().unwrap(), "source", false);
        insert_test_root(&conn, "/archive", "archive", false);

        let scan = insert_decision(&conn, "scan", 100);
        scope(&conn, scan, root_id);
        set_decision_extras(&conn, scan, Some("Indexed 2 sources"), Some("first gather"));
        insert_source(&conn, root_id, "z/last.jpg", None, true, false, None);
        insert_source(&conn, root_id, "a/first.jpg", None, true, false, None);
        repo::note::insert(&conn, root_id, "a", "looks like the 2015 batch").unwrap();
        repo::note::insert(&conn, root_id, "", "ready to retire").unwrap();

        let dest = book_dir.path().join("book");
        let book = compile_to(&conn, root_id, &dest);

        assert_eq!(book.entry_count, 2);
        for file in [
            "inventory.jsonl",
            "timeline.md",
            "notes.md",
            "meta.toml",
            "README.md",
        ] {
            assert!(dest.join(file).is_file(), "{file} missing");
        }

        let lines = inventory_lines(&dest);
        let paths: Vec<&str> = lines.iter().map(|l| l["path"].as_str().unwrap()).collect();
        assert_eq!(paths, vec!["a/first.jpg", "z/last.jpg"]);
        assert_eq!(lines[0]["mtime"], "1970-01-01T00:00:00Z");
        assert_eq!(lines[0]["fate"], "present");
        assert_eq!(lines[0]["verification"], "name_only");

        let timeline = std::fs::read_to_string(dest.join("timeline.md")).unwrap();
        assert!(timeline.contains("Indexed 2 sources"));
        assert!(timeline.contains("reason: first gather"));
        let notes = std::fs::read_to_string(dest.join("notes.md")).unwrap();
        // Note identity through the one shared rendering: root-relative,
        // `(root)` for the root itself — never a view-relative `.`.
        assert!(notes.contains("a: looks like the 2015 batch"), "{notes}");
        assert!(notes.contains("(root): ready to retire"), "{notes}");

        let meta: toml::Value =
            toml::from_str(&std::fs::read_to_string(dest.join("meta.toml")).unwrap()).unwrap();
        assert_eq!(meta["version"].as_integer(), Some(1));
        assert_eq!(meta["counts"]["entries"].as_integer(), Some(2));
        assert_eq!(meta["counts"]["present"].as_integer(), Some(2));
        assert_eq!(meta["identity"]["reason"].as_str(), Some("story complete"));
        // A compile outside a ceremony has no retirement decision to name —
        // the field is absent, never guessed.
        assert!(meta["identity"].get("decision_id").is_none());

        let readme = std::fs::read_to_string(dest.join("README.md")).unwrap();
        assert!(readme.contains("The book of"));
        assert!(readme.contains("unresolved: 2 (2 unhashed)"));
    }

    #[test]
    fn records_mode_apply_degrades_to_covered_and_records_the_gap() {
        let conn = open_in_memory_for_test();
        let src_dir = tempfile::tempdir().unwrap();
        let book_dir = tempfile::tempdir().unwrap();
        let root_id = insert_test_root(&conn, src_dir.path().to_str().unwrap(), "source", false);
        let archive_id = insert_test_root(&conn, "/archive", "archive", false);

        let object = insert_object(&conn, "h1");
        insert_source(&conn, root_id, "kept.jpg", Some(object), true, false, None);
        insert_source(
            &conn,
            archive_id,
            "2015/kept.jpg",
            Some(object),
            true,
            false,
            None,
        );
        let apply = insert_decision(&conn, "apply", 200);
        extraction_from(&conn, apply, root_id, 1); // no receipt columns: Records mode

        let dest = book_dir.path().join("book");
        let book = compile_to(&conn, root_id, &dest);

        assert!(book
            .gaps
            .iter()
            .any(|g| g.contains("per-item origin unavailable")));
        let lines = inventory_lines(&dest);
        assert_eq!(lines[0]["fate"], "covered", "degrades, never guesses");
        assert_eq!(lines[0]["locations"][0], "/archive/2015/kept.jpg");
        let readme = std::fs::read_to_string(dest.join("README.md")).unwrap();
        assert!(readme.contains("per-item origin unavailable"));
    }

    #[test]
    fn receipt_recovers_the_moved_entry_with_live_locations() {
        let conn = open_in_memory_for_test();
        let src_dir = tempfile::tempdir().unwrap();
        let arch_dir = tempfile::tempdir().unwrap();
        let book_dir = tempfile::tempdir().unwrap();
        let src_path = src_dir.path().to_str().unwrap().to_string();
        let arch_path = arch_dir.path().to_str().unwrap().to_string();
        let root_id = insert_test_root(&conn, &src_path, "source", false);
        let archive_id = insert_test_root(&conn, &arch_path, "archive", false);

        // The moved content now lives in the archive under this object.
        let object = insert_object(&conn, "movedhash");
        insert_source(
            &conn,
            archive_id,
            "2015/gone.jpg",
            Some(object),
            true,
            false,
            None,
        );

        let apply = insert_decision(&conn, "apply", 300);
        extraction_from(&conn, apply, root_id, 1);
        let receipt_rel = ".canon-ledger/000001-apply.toml";
        std::fs::create_dir_all(arch_dir.path().join(".canon-ledger")).unwrap();
        std::fs::write(
            arch_dir.path().join(receipt_rel),
            format!(
                r#"
[meta]
decision_id = {apply}
origin_disposition = "relocated"

[meta.locus]
path = "{arch_path}"
id = {archive_id}

[[items]]
source_root = "{src_path}"
source_rel_path = "moved/gone.jpg"
destination_rel_path = "2015/gone.jpg"
size = 555
hash = "sha256:movedhash"
mtime = 1700000000

[[items]]
source_root = "/some/other/root"
source_rel_path = "not/ours.jpg"
destination_rel_path = "2016/ours.jpg"
size = 1
"#
            ),
        )
        .unwrap();
        set_decision_receipt(&conn, apply, archive_id, receipt_rel);

        let dest = book_dir.path().join("book");
        let book = compile_to(&conn, root_id, &dest);

        assert_eq!(book.entry_count, 1, "foreign item skipped, ours recovered");
        assert!(book.gaps.is_empty());
        let lines = inventory_lines(&dest);
        assert_eq!(lines[0]["path"], "moved/gone.jpg");
        assert_eq!(lines[0]["fate"], "archived");
        assert_eq!(lines[0]["disposition"], "moved");
        assert_eq!(lines[0]["size"], 555);
        assert_eq!(lines[0]["hash"], "sha256:movedhash");
        assert_eq!(lines[0]["verification"], "content_verified");
        assert_eq!(
            lines[0]["destination"],
            format!("{arch_path}/2015/gone.jpg")
        );
        assert_eq!(
            lines[0]["locations"][0],
            format!("{arch_path}/2015/gone.jpg")
        );
    }

    #[test]
    fn gather_copies_the_ledger_verbatim() {
        let conn = open_in_memory_for_test();
        let src_dir = tempfile::tempdir().unwrap();
        let book_dir = tempfile::tempdir().unwrap();
        let root_id = insert_test_root(&conn, src_dir.path().to_str().unwrap(), "source", false);
        insert_test_root(&conn, "/archive", "archive", false);
        std::fs::create_dir_all(src_dir.path().join(".canon-ledger")).unwrap();
        std::fs::write(
            src_dir.path().join(".canon-ledger/000009-scan.toml"),
            b"receipt body",
        )
        .unwrap();

        let dest = book_dir.path().join("book");
        let book = compile_to(&conn, root_id, &dest);

        assert_eq!(book.ledger_files, Some(1));
        assert_eq!(
            std::fs::read(dest.join("ledger/000009-scan.toml")).unwrap(),
            b"receipt body".to_vec()
        );
        let meta: toml::Value =
            toml::from_str(&std::fs::read_to_string(dest.join("meta.toml")).unwrap()).unwrap();
        assert_eq!(meta["ledger"]["gathered"].as_bool(), Some(true));
        assert_eq!(meta["ledger"]["files"].as_integer(), Some(1));
    }

    #[test]
    fn unreachable_root_records_the_gather_gap_and_still_compiles() {
        let conn = open_in_memory_for_test();
        let book_dir = tempfile::tempdir().unwrap();
        let root_id = insert_test_root(&conn, "/definitely/not/a/real/path", "source", false);
        insert_test_root(&conn, "/archive", "archive", false);
        insert_source(&conn, root_id, "a.jpg", None, true, false, None);

        let dest = book_dir.path().join("book");
        let book = compile_to(&conn, root_id, &dest);

        assert_eq!(book.ledger_files, None);
        assert!(book.gaps.iter().any(|g| g.contains("unreachable")));
        assert!(!dest.join("ledger").exists());
        let meta: toml::Value =
            toml::from_str(&std::fs::read_to_string(dest.join("meta.toml")).unwrap()).unwrap();
        assert_eq!(meta["ledger"]["gathered"].as_bool(), Some(false));
        assert_eq!(meta["posture"]["posture"].as_str(), Some("on_faith"));
    }

    #[test]
    fn empty_and_all_excluded_roots_bind_complete_books() {
        let conn = open_in_memory_for_test();
        let src_dir = tempfile::tempdir().unwrap();
        let book_dir = tempfile::tempdir().unwrap();
        let root_id = insert_test_root(&conn, src_dir.path().to_str().unwrap(), "source", false);
        insert_test_root(&conn, "/archive", "archive", false);

        let empty_dest = book_dir.path().join("empty");
        let book = compile_to(&conn, root_id, &empty_dest);
        assert_eq!(book.entry_count, 0);
        assert!(empty_dest.join("README.md").is_file());

        let exclude = insert_decision(&conn, "exclude_set", 400);
        set_decision_extras(&conn, exclude, None, Some("not worth keeping"));
        insert_source(&conn, root_id, "junk.jpg", None, true, true, Some(exclude));

        let excluded_dest = book_dir.path().join("excluded");
        let book = compile_to(&conn, root_id, &excluded_dest);
        assert_eq!(book.entry_count, 1);
        let lines = inventory_lines(&excluded_dest);
        assert_eq!(lines[0]["fate"], "excluded");
        assert_eq!(lines[0]["reason"], "not worth keeping");
    }

    // verify_book — the round-trip law and tamper detection

    /// A root exercising every fate at once: covered-enriched (archived from
    /// here), covered plain, excluded, unresolved hashed + unhashed, deleted,
    /// unexplained, and a receipt-recovered moved entry.
    fn every_fate_fixture() -> (Connection, tempfile::TempDir, tempfile::TempDir, i64) {
        let conn = open_in_memory_for_test();
        let src_dir = tempfile::tempdir().unwrap();
        let arch_dir = tempfile::tempdir().unwrap();
        let src_path = src_dir.path().to_str().unwrap().to_string();
        let arch_path = arch_dir.path().to_str().unwrap().to_string();
        let root_id = insert_test_root(&conn, &src_path, "source", false);
        let archive_id = insert_test_root(&conn, &arch_path, "archive", false);

        let copied = insert_object(&conn, "copiedhash");
        let plain = insert_object(&conn, "plainhash");
        let uncovered = insert_object(&conn, "uncoveredhash");
        let moved = insert_object(&conn, "movedhash");
        insert_source(
            &conn,
            archive_id,
            "a/copied.jpg",
            Some(copied),
            true,
            false,
            None,
        );
        insert_source(
            &conn,
            archive_id,
            "a/plain.jpg",
            Some(plain),
            true,
            false,
            None,
        );
        insert_source(
            &conn,
            archive_id,
            "a/moved.jpg",
            Some(moved),
            true,
            false,
            None,
        );

        insert_source(
            &conn,
            root_id,
            "copied.jpg",
            Some(copied),
            true,
            false,
            None,
        );
        insert_source(&conn, root_id, "plain.jpg", Some(plain), true, false, None);
        let exclude = insert_decision(&conn, "exclude_set", 400);
        set_decision_extras(&conn, exclude, None, Some("duplicate"));
        insert_source(&conn, root_id, "junk.jpg", None, true, true, Some(exclude));
        insert_source(
            &conn,
            root_id,
            "loose.jpg",
            Some(uncovered),
            true,
            false,
            None,
        );
        insert_source(&conn, root_id, "unhashed.jpg", None, true, false, None);
        let scan = insert_decision(&conn, "scan", 500);
        insert_source(
            &conn,
            root_id,
            "deleted.jpg",
            None,
            false,
            false,
            Some(scan),
        );
        insert_source(&conn, root_id, "vanished.jpg", None, false, false, None);

        let apply = insert_decision(&conn, "apply", 600);
        extraction_from(&conn, apply, root_id, 2);
        let receipt_rel = ".canon-ledger/000001-apply.toml";
        std::fs::create_dir_all(arch_dir.path().join(".canon-ledger")).unwrap();
        std::fs::write(
            arch_dir.path().join(receipt_rel),
            format!(
                r#"
[meta]
decision_id = {apply}
origin_disposition = "relocated"

[meta.locus]
path = "{arch_path}"
id = {archive_id}

[[items]]
source_root = "{src_path}"
source_rel_path = "copied.jpg"
destination_rel_path = "a/copied.jpg"
size = 1000
hash = "sha256:copiedhash"
mtime = 0

[[items]]
source_root = "{src_path}"
source_rel_path = "gone/moved.jpg"
destination_rel_path = "a/moved.jpg"
size = 555
hash = "sha256:movedhash"
mtime = 1700000000
"#
            ),
        )
        .unwrap();
        set_decision_receipt(&conn, apply, archive_id, receipt_rel);

        (conn, src_dir, arch_dir, root_id)
    }

    #[test]
    fn round_trip_law_verify_matches_the_compiled_db_state() {
        let (conn, _src, _arch, root_id) = every_fate_fixture();
        let book_dir = tempfile::tempdir().unwrap();
        let dest = book_dir.path().join("book");
        let book = compile_to(&conn, root_id, &dest);

        let verified = verify_book(&dest).unwrap();
        assert_eq!(verified.entries, book.entry_count);
        assert_eq!(verified.entries, 8);

        let meta: toml::Value =
            toml::from_str(&std::fs::read_to_string(dest.join("meta.toml")).unwrap()).unwrap();
        let counts = &meta["counts"];
        assert_eq!(counts["archived_from_here"].as_integer(), Some(2));
        assert_eq!(counts["covered"].as_integer(), Some(1));
        assert_eq!(counts["excluded"].as_integer(), Some(1));
        assert_eq!(counts["deleted"].as_integer(), Some(1));
        assert_eq!(counts["present"].as_integer(), Some(2));
        assert_eq!(counts["missing_unexplained"].as_integer(), Some(1));

        // The account and the book tell one story: covered bucket = the two
        // covered rows (one enriched into archived-from-here), unresolved =
        // the present entries, absent buckets match one-to-one.
        let story = fetch_root_story(&conn, root_id).unwrap();
        let review = readiness_lens(&story);
        assert_eq!(review.account.covered, 2);
        assert_eq!(review.account.excluded, 1);
        assert_eq!(review.account.unresolved, 2);
        assert_eq!(review.account.deleted, 1);
        assert_eq!(review.account.unexplained_missing, 1);
    }

    #[test]
    fn inventory_lines_carry_the_fate_determining_decision() {
        let (conn, _src, _arch, root_id) = every_fate_fixture();
        let book_dir = tempfile::tempdir().unwrap();
        let dest = book_dir.path().join("book");
        compile_to(&conn, root_id, &dest);

        let id_of = |command: &str| -> i64 {
            conn.query_row(
                "SELECT id FROM decisions WHERE command = ?1",
                [command],
                |r| r.get(0),
            )
            .unwrap()
        };
        let lines = inventory_lines(&dest);
        let by_path = |p: &str| {
            lines
                .iter()
                .find(|l| l["path"] == p)
                .unwrap_or_else(|| panic!("no line for {p}"))
                .clone()
        };

        // Archived-from-here points at the apply — row-backed and recovered
        // alike — not at the row's indexing stamp.
        let apply = id_of("apply");
        assert_eq!(by_path("copied.jpg")["decision"].as_i64(), Some(apply));
        assert_eq!(by_path("gone/moved.jpg")["decision"].as_i64(), Some(apply));
        // Excluded points at the stamping exclusion.
        assert_eq!(
            by_path("junk.jpg")["decision"].as_i64(),
            Some(id_of("exclude_set"))
        );
        // No recorded decision → the key is absent, never guessed.
        assert!(by_path("loose.jpg")["decision"].is_null());
    }

    #[test]
    fn verify_book_catches_a_tampered_inventory() {
        let (conn, _src, _arch, root_id) = every_fate_fixture();
        let book_dir = tempfile::tempdir().unwrap();
        let dest = book_dir.path().join("book");
        compile_to(&conn, root_id, &dest);

        let inventory = std::fs::read_to_string(dest.join("inventory.jsonl")).unwrap();
        let truncated: Vec<&str> = inventory.lines().skip(1).collect();
        std::fs::write(dest.join("inventory.jsonl"), truncated.join("\n")).unwrap();

        let err = verify_book(&dest).unwrap_err();
        assert!(err.to_string().contains("disagree"));
    }

    #[test]
    fn verify_book_requires_the_readme() {
        let (conn, _src, _arch, root_id) = every_fate_fixture();
        let book_dir = tempfile::tempdir().unwrap();
        let dest = book_dir.path().join("book");
        compile_to(&conn, root_id, &dest);

        std::fs::remove_file(dest.join("README.md")).unwrap();
        let err = verify_book(&dest).unwrap_err();
        assert!(err.to_string().contains("README.md is missing"));
    }

    #[test]
    fn verify_book_catches_a_missing_gathered_ledger() {
        let (conn, _src, _arch, root_id) = every_fate_fixture();
        let book_dir = tempfile::tempdir().unwrap();
        let dest = book_dir.path().join("book");
        let book = compile_to(&conn, root_id, &dest);
        assert!(book.ledger_files.is_none() || book.ledger_files == Some(0));

        // Claim a gathered ledger the directory doesn't hold.
        let meta = std::fs::read_to_string(dest.join("meta.toml")).unwrap();
        let tampered = meta.replace("files = 0", "files = 3");
        std::fs::write(dest.join("meta.toml"), tampered).unwrap();

        let err = verify_book(&dest).unwrap_err();
        assert!(err.to_string().contains("ledger disagrees"));
    }

    #[test]
    fn scale_ceremony_round_trips_past_the_chunking_boundary() {
        // The whole ceremony — bind (compile + verify + place) and release —
        // over a root past the SQL chunking boundary.
        let conn = open_in_memory_for_test();
        let src_dir = tempfile::tempdir().unwrap();
        let arch_dir = tempfile::tempdir().unwrap();
        let root_id = insert_test_root(&conn, src_dir.path().to_str().unwrap(), "source", false);
        let archive_id =
            insert_test_root(&conn, arch_dir.path().to_str().unwrap(), "archive", false);

        conn.execute_batch("BEGIN").unwrap();
        for i in 0..2000 {
            let object = insert_object(&conn, &format!("hash{i:05}"));
            insert_source(
                &conn,
                root_id,
                &format!("d{}/f{i:05}.jpg", i % 7),
                Some(object),
                true,
                false,
                None,
            );
            insert_source(
                &conn,
                archive_id,
                &format!("a/f{i:05}.jpg"),
                Some(object),
                true,
                false,
                None,
            );
        }
        conn.execute_batch("COMMIT").unwrap();

        let mut ceremony = begin_with(&conn, root_id, RecordingMode::Full);
        let bound = ceremony.bind(&conn).unwrap();
        assert_eq!(bound.entry_count, 2000);
        let meta: toml::Value =
            toml::from_str(&std::fs::read_to_string(bound.dir.join("meta.toml")).unwrap()).unwrap();
        assert_eq!(meta["counts"]["covered"].as_integer(), Some(2000));

        match ceremony.release(&conn).unwrap() {
            ReleaseOutcome::Released {
                deleted_sources, ..
            } => assert_eq!(deleted_sources, 2000),
            ReleaseOutcome::WorldMoved { detail, .. } => panic!("world moved: {detail}"),
        }
        assert!(!repo::root::fetch_all(&conn)
            .unwrap()
            .iter()
            .any(|r| r.id == root_id));
    }

    #[test]
    fn existing_compile_target_is_an_explicit_collision() {
        let conn = open_in_memory_for_test();
        let src_dir = tempfile::tempdir().unwrap();
        let book_dir = tempfile::tempdir().unwrap();
        let root_id = insert_test_root(&conn, src_dir.path().to_str().unwrap(), "source", false);
        insert_test_root(&conn, "/archive", "archive", false);

        let dest = book_dir.path().join("book");
        std::fs::create_dir(&dest).unwrap();
        let story = fetch_root_story(&conn, root_id).unwrap();
        let err = compile_book(
            &conn,
            &story,
            &CompileParams {
                reason: None,
                now: 0,
                dest_dir: dest,
                ceremony_decision_id: None,
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }

    // The ceremony: plan_bind + begin + bind

    const CEREMONY_NOW: i64 = 1_753_000_000;

    fn config_with(recording: RecordingMode) -> LedgerConfig {
        LedgerConfig {
            recording,
            ..LedgerConfig::default()
        }
    }

    fn plan_for(conn: &Connection, root_id: i64) -> BindPlan {
        let story = fetch_root_story(conn, root_id).unwrap();
        plan_bind(&story, &ledger_config(), CEREMONY_NOW).unwrap()
    }

    fn begin_with(conn: &Connection, root_id: i64, recording: RecordingMode) -> RetireCeremony {
        let story = fetch_root_story(conn, root_id).unwrap();
        let review = readiness_lens(&story);
        let config = config_with(recording);
        let plan = plan_bind(&story, &config, CEREMONY_NOW).unwrap();
        begin_ceremony(
            conn,
            story,
            &review,
            plan,
            CeremonyParams {
                reason: Some("story complete".to_string()),
                now: CEREMONY_NOW,
                command_line: "canon roots retire".to_string(),
                config,
            },
        )
    }

    fn count_retire_decisions(conn: &Connection) -> i64 {
        conn.query_row(
            "SELECT COUNT(*) FROM decisions WHERE command = 'roots_retire'",
            [],
            |r| r.get(0),
        )
        .unwrap()
    }

    /// A directory that identifies itself as some root's book — just enough
    /// meta.toml for the collision probe.
    fn fake_book(shelf: &std::path::Path, name: &str, root_path: &str) {
        let dir = shelf.join(name);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("meta.toml"),
            format!("version = 1\n\n[identity]\npath = \"{root_path}\"\n"),
        )
        .unwrap();
    }

    #[test]
    fn plan_bind_fresh_shelf_takes_the_plain_name() {
        let (conn, _src, arch, root_id) = every_fate_fixture();
        let story = fetch_root_story(&conn, root_id).unwrap();

        let plan = plan_for(&conn, root_id);

        let shelf = arch.path().join(SHELF_DIR);
        assert_eq!(plan.shelf, shelf);
        assert!(!plan.shelf_exists);
        assert!(!plan.replaces_existing);
        let name = book_dir_name(&story.root.path, &iso_date(CEREMONY_NOW));
        assert_eq!(plan.final_dir, shelf.join(&name));
        assert_eq!(plan.temp_dir, shelf.join(format!(".compiling-{name}")));
    }

    #[test]
    fn plan_bind_same_root_book_is_a_replacement() {
        let (conn, _src, arch, root_id) = every_fate_fixture();
        let story = fetch_root_story(&conn, root_id).unwrap();
        let shelf = arch.path().join(SHELF_DIR);
        let name = book_dir_name(&story.root.path, &iso_date(CEREMONY_NOW));
        fake_book(&shelf, &name, &story.root.path);

        let plan = plan_for(&conn, root_id);

        assert!(plan.replaces_existing);
        assert_eq!(plan.final_dir, shelf.join(&name));
    }

    #[test]
    fn plan_bind_different_root_book_probes_to_a_sibling_name() {
        let (conn, _src, arch, root_id) = every_fate_fixture();
        let story = fetch_root_story(&conn, root_id).unwrap();
        let shelf = arch.path().join(SHELF_DIR);
        let name = book_dir_name(&story.root.path, &iso_date(CEREMONY_NOW));
        fake_book(&shelf, &name, "/somebody/else");

        let plan = plan_for(&conn, root_id);
        assert!(!plan.replaces_existing);
        assert_eq!(plan.final_dir, shelf.join(format!("{name}-2")));

        // A second stranger pushes to -3.
        fake_book(&shelf, &format!("{name}-2"), "/a/third/party");
        let plan = plan_for(&conn, root_id);
        assert_eq!(plan.final_dir, shelf.join(format!("{name}-3")));
    }

    #[test]
    fn plan_bind_refuses_an_unidentifiable_directory() {
        let (conn, _src, arch, root_id) = every_fate_fixture();
        let story = fetch_root_story(&conn, root_id).unwrap();
        let shelf = arch.path().join(SHELF_DIR);
        let name = book_dir_name(&story.root.path, &iso_date(CEREMONY_NOW));
        // A directory at the book's name with no meta.toml at all.
        std::fs::create_dir_all(shelf.join(&name)).unwrap();

        let err = plan_bind(
            &fetch_root_story(&conn, root_id).unwrap(),
            &ledger_config(),
            CEREMONY_NOW,
        )
        .unwrap_err();
        assert!(err.to_string().contains("refusing to replace"), "{err:#}");
    }

    #[test]
    fn bind_places_a_verified_book_and_records_the_pointer() {
        let (conn, _src, arch, root_id) = every_fate_fixture();
        let mut ceremony = begin_with(&conn, root_id, RecordingMode::Full);

        let bound = ceremony.bind(&conn).unwrap();

        assert!(bound.dir.is_dir());
        assert_eq!(bound.entry_count, 8);
        assert!(!bound.replaced_previous);
        assert!(bound.warnings.is_empty(), "{:?}", bound.warnings);
        verify_book(&bound.dir).unwrap();
        assert!(arch.path().join(SHELF_DIR).join("README.md").is_file());
        // No compile residue on the shelf.
        assert!(!ceremony.plan.temp_dir.exists());

        // The decision is still open (release hasn't run) but the pointer is
        // already recorded — abort-after-bind stays findable.
        let decision_id = ceremony.recorder.decision_id().unwrap();
        let decision = repo::decision::fetch_by_id(&conn, decision_id)
            .unwrap()
            .unwrap();
        assert_eq!(decision.status, "started");
        let name = bound.dir.file_name().unwrap().to_string_lossy();
        assert_eq!(
            decision.receipt_rel_path.as_deref(),
            Some(format!("{SHELF_DIR}/{name}").as_str())
        );
        let (arch_id, _) = ops::receipt::resolve_ledger_root(
            &fetch_root_story(&conn, root_id).unwrap().roots,
            &ledger_config(),
        )
        .unwrap();
        assert_eq!(decision.receipt_root_id, Some(arch_id));

        // The book names the decision that bound it — the index reference is
        // readable from the shelf alone.
        let meta: toml::Value =
            toml::from_str(&std::fs::read_to_string(bound.dir.join("meta.toml")).unwrap()).unwrap();
        assert_eq!(
            meta["identity"]["decision_id"].as_integer(),
            Some(decision_id)
        );
    }

    #[test]
    fn timeline_indents_multi_line_summaries_and_reasons() {
        let conn = open_in_memory_for_test();
        let src_dir = tempfile::tempdir().unwrap();
        let book_dir = tempfile::tempdir().unwrap();
        let root_id = insert_test_root(&conn, src_dir.path().to_str().unwrap(), "source", false);
        insert_test_root(&conn, "/archive", "archive", false);
        insert_source(&conn, root_id, "a.jpg", None, true, false, None);
        let scan = insert_decision(&conn, "scan", 100);
        scope(&conn, scan, root_id);
        set_decision_extras(
            &conn,
            scan,
            Some("Scanned 4 files: 4 new\nHashed 7 files"),
            Some("first pass\nsecond thoughts, still open"),
        );

        let dest = book_dir.path().join("book");
        compile_to(&conn, root_id, &dest);

        let timeline = std::fs::read_to_string(dest.join("timeline.md")).unwrap();
        assert!(
            timeline.contains(": Scanned 4 files: 4 new\n  Hashed 7 files\n"),
            "{timeline}"
        );
        assert!(
            timeline.contains("  reason: first pass\n  second thoughts, still open\n"),
            "{timeline}"
        );
    }

    #[test]
    fn bind_keeps_its_own_in_flight_decision_out_of_the_timeline() {
        let (conn, _src, _arch, root_id) = every_fate_fixture();
        let mut ceremony = begin_with(&conn, root_id, RecordingMode::Full);

        let bound = ceremony.bind(&conn).unwrap();

        let timeline = std::fs::read_to_string(bound.dir.join("timeline.md")).unwrap();
        assert!(!timeline.contains("roots_retire"), "{timeline}");
    }

    #[test]
    fn a_prior_attempts_decision_renders_in_the_next_books_timeline() {
        let (conn, _src, _arch, root_id) = every_fate_fixture();
        let mut first = begin_with(&conn, root_id, RecordingMode::Full);
        first.bind(&conn).unwrap();
        first.abandon(&conn);

        let mut second = begin_with(&conn, root_id, RecordingMode::Full);
        let bound = second.bind(&conn).unwrap();

        // The abandoned attempt is history and narrates itself; only the
        // current in-flight decision stays out.
        let timeline = std::fs::read_to_string(bound.dir.join("timeline.md")).unwrap();
        assert!(timeline.contains("release declined"), "{timeline}");
        let own_id = second.recorder.decision_id().unwrap();
        assert!(!timeline.contains(&format!("#{own_id} ")), "{timeline}");
    }

    #[test]
    fn bind_clears_a_leftover_temp_compile() {
        let (conn, _src, _arch, root_id) = every_fate_fixture();
        let mut ceremony = begin_with(&conn, root_id, RecordingMode::Full);
        std::fs::create_dir_all(&ceremony.plan.temp_dir).unwrap();
        std::fs::write(ceremony.plan.temp_dir.join("junk.txt"), "leftover").unwrap();

        let bound = ceremony.bind(&conn).unwrap();

        verify_book(&bound.dir).unwrap();
        assert!(!ceremony.plan.temp_dir.exists());
    }

    #[test]
    fn shelf_readme_is_written_once_and_kept() {
        let (conn, _src, arch, root_id) = every_fate_fixture();
        let shelf = arch.path().join(SHELF_DIR);
        std::fs::create_dir_all(&shelf).unwrap();
        std::fs::write(shelf.join("README.md"), "my own shelf notes\n").unwrap();

        let mut ceremony = begin_with(&conn, root_id, RecordingMode::Full);
        ceremony.bind(&conn).unwrap();

        let readme = std::fs::read_to_string(shelf.join("README.md")).unwrap();
        assert_eq!(readme, "my own shelf notes\n");
    }

    #[test]
    fn bind_replaces_a_standing_same_root_book_without_residue() {
        let (conn, _src, arch, root_id) = every_fate_fixture();

        // First ceremony binds, then is abandoned before release.
        let mut first = begin_with(&conn, root_id, RecordingMode::Full);
        let first_book = first.bind(&conn).unwrap();
        let sentinel = first_book.dir.join("meta.toml");
        let first_meta = std::fs::read_to_string(&sentinel).unwrap();

        // The re-run converges: same name, fresh compile, old book gone.
        let mut second = begin_with(&conn, root_id, RecordingMode::Full);
        assert!(second.plan.replaces_existing);
        let second_book = second.bind(&conn).unwrap();

        assert!(second_book.replaced_previous);
        assert_eq!(second_book.dir, first_book.dir);
        verify_book(&second_book.dir).unwrap();
        // Freshly written: the standing book names the *second* binding's
        // decision — the old id is exactly the residue that must be gone —
        // and everything else is identical (the compile stamps the same now).
        let second_meta = std::fs::read_to_string(&sentinel).unwrap();
        let first_id = first.recorder.decision_id().unwrap();
        let second_id = second.recorder.decision_id().unwrap();
        assert!(second_meta.contains(&format!("decision_id = {second_id}")));
        assert!(!second_meta.contains(&format!("decision_id = {first_id}")));
        assert_eq!(
            second_meta.replace(
                &format!("decision_id = {second_id}"),
                &format!("decision_id = {first_id}")
            ),
            first_meta
        );

        // No swap residue on the shelf: only the book and the README.
        let shelf = arch.path().join(SHELF_DIR);
        let mut names: Vec<String> = std::fs::read_dir(&shelf)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        let book_name = second_book.dir.file_name().unwrap().to_string_lossy();
        assert_eq!(names, vec!["README.md".to_string(), book_name.to_string()]);
    }

    #[test]
    fn bind_failure_at_placement_leaves_the_verified_temp_standing() {
        let (conn, _src, _arch, root_id) = every_fate_fixture();
        let mut ceremony = begin_with(&conn, root_id, RecordingMode::Full);

        // The plan saw a free final name; an interloper appears before the
        // rename (a concurrent process racing the shelf). Placement must
        // fail, and fail without touching anything.
        std::fs::create_dir_all(&ceremony.plan.final_dir).unwrap();
        std::fs::write(ceremony.plan.final_dir.join("interloper.txt"), "mine").unwrap();

        let err = ceremony.bind(&conn).unwrap_err();
        assert!(err.to_string().contains("Could not place"), "{err:#}");

        // The verified compile still stands at its temp name for inspection…
        verify_book(&ceremony.plan.temp_dir).unwrap();
        // …and the interloper is untouched.
        let interloper = ceremony.plan.final_dir.join("interloper.txt");
        assert_eq!(std::fs::read_to_string(interloper).unwrap(), "mine");
    }

    #[test]
    fn bind_under_recording_off_places_the_book_and_indexes_nothing() {
        let (conn, _src, _arch, root_id) = every_fate_fixture();
        let mut ceremony = begin_with(&conn, root_id, RecordingMode::Off);

        let bound = ceremony.bind(&conn).unwrap();

        verify_book(&bound.dir).unwrap();
        assert!(bound.warnings.is_empty(), "{:?}", bound.warnings);
        assert_eq!(count_retire_decisions(&conn), 0);
    }

    // The ceremony: release, abandon, world-moved

    fn retire_decision_row(conn: &Connection, ceremony: &RetireCeremony) -> Decision {
        repo::decision::fetch_by_id(conn, ceremony.recorder.decision_id().unwrap())
            .unwrap()
            .unwrap()
    }

    #[test]
    fn full_ceremony_releases_the_root_and_completes_the_decision() {
        let (conn, _src, _arch, root_id) = every_fate_fixture();
        let mut ceremony = begin_with(&conn, root_id, RecordingMode::Full);
        let bound = ceremony.bind(&conn).unwrap();
        let rows_before = repo::source::count_all_by_root(&conn, root_id).unwrap();

        // Between review and release, the only new world state is the
        // ceremony's own decision and its scope row — releasing cleanly here
        // is the self-exclusion regression test.
        let outcome = ceremony.release(&conn).unwrap();

        let ReleaseOutcome::Released {
            deleted_sources,
            summary,
            warnings,
            ..
        } = outcome
        else {
            panic!("expected Released");
        };
        assert_eq!(deleted_sources, rows_before);
        assert!(warnings.is_empty(), "{warnings:?}");
        let root_path = &ceremony.story.root.path;
        assert!(summary.contains(&format!("Retired {root_path}")));
        assert!(summary.contains("the story is bound at"));
        assert!(summary.contains(bound.dir.to_str().unwrap()));

        // The root and its rows are gone; the book stands.
        assert!(!repo::root::fetch_all(&conn)
            .unwrap()
            .iter()
            .any(|r| r.id == root_id));
        assert_eq!(repo::source::count_all_by_root(&conn, root_id).unwrap(), 0);
        verify_book(&bound.dir).unwrap();

        let decision = retire_decision_row(&conn, &ceremony);
        assert_eq!(decision.status, "completed");
        assert_eq!(decision.count_attempted, Some(rows_before));
        assert_eq!(decision.count_completed, Some(rows_before));
        assert_eq!(decision.summary.as_deref(), Some(summary.as_str()));
    }

    #[test]
    fn abandon_after_bind_leaves_root_and_book_standing() {
        let (conn, _src, _arch, root_id) = every_fate_fixture();
        let mut ceremony = begin_with(&conn, root_id, RecordingMode::Full);
        let bound = ceremony.bind(&conn).unwrap();

        let abandoned = ceremony.abandon(&conn);

        assert!(abandoned.summary.contains("release declined"));
        assert!(abandoned.summary.contains(bound.dir.to_str().unwrap()));
        // Root intact, book standing — the safety invariant's abort arm.
        assert!(repo::root::fetch_all(&conn)
            .unwrap()
            .iter()
            .any(|r| r.id == root_id));
        verify_book(&bound.dir).unwrap();

        let decision = retire_decision_row(&conn, &ceremony);
        assert_eq!(decision.status, "partial");

        // The rm guard now reads the pointer: the story is already bound.
        let plan = ops::roots::plan_remove(&conn, root_id).unwrap();
        let pointer = plan.retirement.expect("retirement pointer");
        assert!(pointer.artifact_display.contains("retired/"));
    }

    #[test]
    fn release_stops_when_a_source_row_appeared_since_review() {
        let (conn, _src, _arch, root_id) = every_fate_fixture();
        let mut ceremony = begin_with(&conn, root_id, RecordingMode::Full);
        let bound = ceremony.bind(&conn).unwrap();

        // A concurrent scan indexed a new file between review and release.
        insert_source(&conn, root_id, "new/arrival.jpg", None, true, false, None);

        let outcome = ceremony.release(&conn).unwrap();
        let ReleaseOutcome::WorldMoved { detail, .. } = outcome else {
            panic!("expected WorldMoved");
        };
        assert!(detail.contains("source rows"), "{detail}");

        // Root intact (the transaction wrote nothing), book standing,
        // decision partial — never both partial.
        assert!(repo::root::fetch_all(&conn)
            .unwrap()
            .iter()
            .any(|r| r.id == root_id));
        verify_book(&bound.dir).unwrap();
        assert_eq!(retire_decision_row(&conn, &ceremony).status, "partial");
    }

    #[test]
    fn release_stops_when_a_foreign_decision_touched_the_root() {
        let (conn, _src, _arch, root_id) = every_fate_fixture();
        let mut ceremony = begin_with(&conn, root_id, RecordingMode::Full);
        let bound = ceremony.bind(&conn).unwrap();

        // Another process's decision landed a scope row on this root.
        let foreign = insert_decision(&conn, "exclude_set", 9_999);
        scope(&conn, foreign, root_id);

        let outcome = ceremony.release(&conn).unwrap();
        let ReleaseOutcome::WorldMoved { detail, .. } = outcome else {
            panic!("expected WorldMoved");
        };
        assert!(detail.contains("another decision"), "{detail}");
        assert!(repo::root::fetch_all(&conn)
            .unwrap()
            .iter()
            .any(|r| r.id == root_id));
        verify_book(&bound.dir).unwrap();
    }

    #[test]
    fn off_then_full_rerun_converges_from_disk() {
        let (conn, _src, _arch, root_id) = every_fate_fixture();

        // First ceremony under Off: the book binds, nothing is indexed.
        let mut first = begin_with(&conn, root_id, RecordingMode::Off);
        let first_book = first.bind(&conn).unwrap();
        first.abandon(&conn);
        assert_eq!(count_retire_decisions(&conn), 0);

        // The re-run under Full finds the standing book on disk — collision
        // detection is meta.toml-keyed, so no decision row is needed.
        let mut second = begin_with(&conn, root_id, RecordingMode::Full);
        assert!(second.plan.replaces_existing);
        let second_book = second.bind(&conn).unwrap();
        assert_eq!(second_book.dir, first_book.dir);

        let outcome = second.release(&conn).unwrap();
        assert!(matches!(outcome, ReleaseOutcome::Released { .. }));
        let decision = retire_decision_row(&conn, &second);
        assert_eq!(decision.status, "completed");
        assert!(decision.receipt_rel_path.unwrap().starts_with("retired/"));
    }

    #[test]
    fn interrupt_records_a_findable_interrupted_decision() {
        let (conn, _src, _arch, root_id) = every_fate_fixture();
        let mut ceremony = begin_with(&conn, root_id, RecordingMode::Full);
        // Force a placement failure: an interloper at the final name.
        std::fs::create_dir_all(&ceremony.plan.final_dir).unwrap();
        std::fs::write(ceremony.plan.final_dir.join("interloper.txt"), "mine").unwrap();
        let err = ceremony.bind(&conn).unwrap_err();

        ceremony.interrupt(&conn, &format!("{err:#}"));

        let decision = retire_decision_row(&conn, &ceremony);
        assert_eq!(decision.status, "interrupted");
        assert!(decision
            .summary
            .unwrap()
            .contains("Retirement interrupted during bind"));
        // The root is untouched.
        assert!(repo::root::fetch_all(&conn)
            .unwrap()
            .iter()
            .any(|r| r.id == root_id));
    }
}
