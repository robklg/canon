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
    SourceFate, VerificationPosture, STANDING_CONTENTLESS, STANDING_COVERED,
    STANDING_MISSING_UNEXPLAINED, STANDING_PRESENT,
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

/// A bound retirement covering a path — the retired-scope statement's data.
pub struct RetiredScope {
    /// The retired root's path, from the scope-row snapshot.
    pub root_path: String,
    /// When the retirement decision was made (epoch seconds).
    pub retired_at: i64,
    pub reason: Option<String>,
    /// Where the story is bound: the book's location, marked fallback when
    /// the shelf's own root has left the index.
    pub book_display: String,
    pub decision_id: i64,
}

/// The newest bound retirement whose scope snapshot contains `path`
/// (descendant-or-equal — a view merely containing a retired root is not
/// "this place is retired"). Serves the trail's retired-scope statement on
/// a scope miss; a live root at the path never reaches this (resolution
/// succeeds). Off-mode retirements left no decision row and cannot match —
/// the caller's existing miss behavior stands.
pub fn find_retirement_covering_path(
    conn: &Connection,
    path: &str,
) -> Result<Option<RetiredScope>> {
    let rows =
        repo::decision::fetch_bound_retirements(conn, DecisionCommand::RootsRetire.as_str())?;
    // Newest first — the first hit is the latest retirement of the place
    // (a re-retired path resolves to its newest telling).
    let Some(hit) = rows
        .into_iter()
        .find(|r| crate::domain::path::path_is_under(path, &r.root_path))
    else {
        return Ok(None);
    };
    let roots = repo::root::fetch_all(conn)?;
    let book_display = match roots.iter().find(|r| r.id == hit.receipt_root_id) {
        Some(root) => format!("{}/{}", root.path, hit.receipt_rel_path),
        None => format!(
            "root #{} (removed)/{}",
            hit.receipt_root_id, hit.receipt_rel_path
        ),
    };
    Ok(Some(RetiredScope {
        root_path: hit.root_path,
        retired_at: hit.created_at,
        reason: hit.reason,
        book_display,
        decision_id: hit.decision_id,
    }))
}

/// One line of the retired fleet.

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
    /// Subset archived *from this root*: an archive copy stands stamped by a
    /// decision whose extraction rows draw from here (object-grain, DB
    /// projections only — apply stamps the destination rows, so the archive
    /// side carries the apply id and the ledger names its origin).
    pub archived_from_here: HashSet<i64>,
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
    let archived_from_here =
        repo::object::batch_check_archived_from_root(conn, &present_object_ids, root_id)?;

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
        archived_from_here,
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
        &story.archived_from_here,
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

/// The shelf's directory name at the archive ledger root: a visible place,
/// deliberately not under `.canon-ledger/` — the books are for human eyes.
pub const SHELF_DIR: &str = "retired";

mod ceremony;
mod compile;
mod shelf;
#[cfg(test)]
mod tests;

// Glob re-exports: the split is internal — `ops::retire::*` keeps exactly
// the surface the single file had.
pub use ceremony::*;
pub use compile::*;
pub use shelf::*;
