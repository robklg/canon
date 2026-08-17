//! The book compile: the second lens over `RootStory`.

use std::collections::{HashMap, HashSet};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};

use crate::core::domain::decision::Decision;
use crate::core::domain::extraction::OriginDisposition;
use crate::core::domain::format::format_size;
use crate::core::domain::resolution::{build_account, ResolutionAccount};
use crate::core::domain::{format_count, Root};
use crate::core::ops::ledger::{read_apply_receipt, ReceiptRead};
use crate::core::ops::root_story::RootStory;
use crate::core::repo;
use crate::notes::{fetch_by_roots, note_display_path, Note};
use crate::retire::domain::{
    build_book_entries, derive_posture, disposition_word, ApplyOrigin, BookEntry, FateContext,
    SourceFate, VerificationPosture,
};
use crate::trail::TimelineEvent;
use crate::trail::{TrailParams, TrailResult, TrailView};

use super::{iso_date, iso_utc, strip_hash_prefix};

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
    /// The story as told — written into the book as `story.md`, claimed in
    /// `meta.toml`, required by verification. The ceremony always passes
    /// one (its `bind` takes the artifact as a required parameter); `None`
    /// keeps the direct compile testable and matches pre-telling books.
    pub telling: Option<super::frame::TellingArtifact>,
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
    let notes = fetch_by_roots(conn, &[story.root.id])?;
    let trail = crate::trail::compute_trail(
        conn,
        &TrailParams {
            prefixes: vec![story.root.path.clone()],
            timeframe: None,
            include_notes: false,
            // Uncapped on purpose: the book's timeline claims every decision
            // this root ever saw, oldest first.
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
        archived_from_here: &story.archived_from_here,
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
        &story.archived_from_here,
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
    // The story before the meta that claims it, and both before the README
    // (written last — a partial compile stays self-evident).
    if let Some(telling) = &params.telling {
        std::fs::write(params.dest_dir.join("story.md"), &telling.text)
            .context("Failed to write the book's story")?;
    }
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
            SourceFate::PresentAtRetirement
            | SourceFate::Contentless
            | SourceFate::MissingUnexplained => (None, None, Vec::new(), None),
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
    let count = crate::core::ops::fs::copy_tree(&src, &dir.join("ledger"))?;
    if count == 0 {
        // An empty gather leaves no directory: the book never lists (or
        // holds) a `ledger/` with nothing in it — README and story say the
        // drive kept no receipts of its own.
        let _ = std::fs::remove_dir(dir.join("ledger"));
    }
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
        contentless: 0,
        missing_unexplained: 0,
    };
    for entry in entries {
        match &entry.fate {
            SourceFate::ArchivedFromHere { .. } => counts.archived_from_here += 1,
            SourceFate::Covered { .. } => counts.covered += 1,
            SourceFate::Excluded { .. } => counts.excluded += 1,
            SourceFate::Deleted { .. } => counts.deleted += 1,
            SourceFate::PresentAtRetirement => counts.present += 1,
            SourceFate::Contentless => counts.contentless += 1,
            SourceFate::MissingUnexplained => counts.missing_unexplained += 1,
        }
    }
    counts
}

// Values (version, gaps) are declared before the sub-tables so the struct reads
// in the order meta.toml is written: TOML requires a table to follow every
// scalar key of its parent, and the serializer arranges that on its own.
#[derive(Serialize)]
struct BookMeta<'a> {
    version: u32,
    gaps: &'a [String],
    identity: MetaIdentity<'a>,
    account: MetaAccount,
    posture: MetaPosture,
    counts: &'a MetaCounts,
    ledger: MetaLedger,
    #[serde(skip_serializing_if = "Option::is_none")]
    story: Option<MetaStory>,
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
    /// Archived from here and still standing — the standing split's
    /// deliberate side (additive within format v1).
    archived_standing: i64,
    covered: i64,
    excluded: i64,
    /// Empty files — contentless (additive within format v1).
    contentless: i64,
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

/// Shared with `verify.rs`: the writer builds it (`fate_counts`), the reader
/// recounts the inventory into a fresh one and compares — the round-trip
/// law's subject.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct MetaCounts {
    pub(super) entries: i64,
    pub(super) archived_from_here: i64,
    pub(super) covered: i64,
    pub(super) excluded: i64,
    pub(super) deleted: i64,
    pub(super) present: i64,
    /// Empty at retirement — additive within format v1; `default` so books
    /// bound before the contentless standing still parse (their inventories
    /// hold no contentless entries, so the recount agrees at 0).
    #[serde(default)]
    pub(super) contentless: i64,
    pub(super) missing_unexplained: i64,
}

/// Shared with `verify.rs`: the writer states what it gathered, the reader
/// cross-checks it against the standing `ledger/` directory.
#[derive(Serialize, Deserialize)]
pub(super) struct MetaLedger {
    pub(super) gathered: bool,
    pub(super) files: usize,
}

/// The telling's claim: the artifact verification requires, plus the
/// reading settings that shaped it (additive within format v1). The prose
/// itself is never recounted — the dossier is the verified record; a
/// hand-refined telling is marked, never passed off as pure derivation.
#[derive(Serialize)]
struct MetaStory {
    file: &'static str,
    hand_edited: bool,
    signature_tolerance: f64,
    dust_floor_files: i64,
    dust_floor_bytes: i64,
    where_cap: usize,
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
            archived_standing: account.archived_standing,
            covered: account.covered,
            excluded: account.excluded,
            contentless: account.contentless,
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
        story: params.telling.as_ref().map(|t| MetaStory {
            file: "story.md",
            hand_edited: t.hand_edited,
            signature_tolerance: t.params.signature_tolerance,
            dust_floor_files: t.params.dust_floor_files,
            dust_floor_bytes: t.params.dust_floor_bytes,
            where_cap: t.params.where_cap,
        }),
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
    if params.telling.is_some() {
        // The front door names the way in: the book opens as a story, not
        // a dossier.
        out.push_str(
            "Start with story.md — the story as told at the retirement. The files\n\
             listed below are the record beneath it.\n\n",
        );
    }

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
    if account.archived_standing > 0 {
        out.push_str(&format!(
            "- archived from here, still standing: {}\n",
            format_count(account.archived_standing)
        ));
    }
    out.push_str(&format!("- covered: {}\n", format_count(account.covered)));
    out.push_str(&format!("- excluded: {}\n", format_count(account.excluded)));
    if account.contentless > 0 {
        out.push_str(&format!(
            "- empty files (contentless — nothing to cover, nothing to verify): {}\n",
            format_count(account.contentless)
        ));
    }
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
    if params.telling.is_some() {
        out.push_str("- story.md — the story as told, written at the retirement.\n");
    }
    out.push_str(
        "- inventory.jsonl — every source this root ever had, one JSON line each,\n\
         \x20 sorted by path: size, dates, hash where known, and fate.\n",
    );
    out.push_str("- timeline.md — every decision that touched this root, with reasons.\n");
    out.push_str("- notes.md — the notes bound beside the timeline.\n");
    match ledger_files {
        Some(0) => out.push_str(
            "- (no ledger/ — the drive kept no receipts of its own; archiving and\n\
             \x20 letting-go receipts live in the archive's own ledger.)\n",
        ),
        Some(count) => out.push_str(&format!(
            "- ledger/ — the receipts that lived on the drive itself (what was lost\n\
             \x20 here), gathered verbatim ({} files).\n",
            format_count(count as i64)
        )),
        None => out.push_str("- ledger/ — not gathered (see gaps below).\n"),
    }
    out.push_str("- meta.toml — identity, account, counts, and gaps, machine-readable.\n\n");

    if params.telling.is_some() {
        // The Canon-word mapping lives here, beside the machine-facing
        // material — the story defines its terms once, where first used,
        // and never carries a glossary.
        out.push_str("## The story's words\n\n");
        out.push_str(
            "story.md speaks plainly; the records beneath it use Canon's own words.\n\
             The mapping:\n\n",
        );
        out.push_str("- chosen for the archive = archived\n");
        out.push_str("- let go = excluded\n");
        out.push_str("- preserved by copies in the archive = covered\n");
        out.push_str("- no known copy in the archive = unresolved\n");
        out.push_str("- empty file = contentless\n");
        out.push_str("- returned to consideration = restored\n\n");
    }

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
