//! Retirement operations: the readiness review.
//!
//! `compute_readiness` is the one structural fetch of the retirement
//! ceremony — the readiness review is its first lens, and the book compile
//! (the second lens) builds on the same substrate. One fetch, two
//! renderings: the gate and the book can never drift apart.

use std::collections::{HashMap, HashSet};

use anyhow::{bail, Result};
use rusqlite::Connection;

use crate::core::domain::config::LedgerConfig;
use crate::core::domain::decision::{Decision, DecisionCommand};
use crate::core::domain::resolution::{build_account, ResolutionAccount};
use crate::core::domain::Root;
use crate::core::ops::root_story::{fetch_root_story, RootStory};
use crate::ops;
use crate::repo;
use crate::retire::domain::{derive_readiness, Readiness};

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
    // The liveness gate: a retirement answers only when no indexed root
    // contains the asked path — the statement must never call a live place
    // retired. A bound-not-released ceremony (declined release, crash
    // during inspection) leaves its artifact reference recorded while the
    // root stays fully indexed, and a released root can be re-added at the
    // same path. A status filter would be wrong the other way: an
    // abandoned-bind root later removed by plain `rm` must keep matching
    // (disk truth — its book stands). Suspended roots count as live: their
    // index is intact, merely awaiting reconnection.
    let roots = repo::root::fetch_all(conn)?;
    if roots
        .iter()
        .any(|r| crate::core::domain::path::path_is_under(path, &r.path))
    {
        return Ok(None);
    }
    let rows =
        repo::decision::fetch_bound_retirements(conn, DecisionCommand::RootsRetire.as_str())?;
    // Newest first — the first hit is the latest retirement of the place
    // (a re-retired path resolves to its newest telling).
    let Some(hit) = rows
        .into_iter()
        .find(|r| crate::core::domain::path::path_is_under(path, &r.root_path))
    else {
        return Ok(None);
    };
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
