//! `RootStory`: the one structural fetch of a root's complete world-state —
//! shared substrate for `retire`'s readiness gate and book compile, and
//! `story`'s place lens. Each subsystem interprets the same fetch for its
//! own purpose; neither owns it, which is why it lives here rather than in
//! either. "One fetch, [multiple] lenses": the gate, the book, and the
//! story review can never drift apart because they all read this.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use anyhow::Result;
use rusqlite::Connection;

use crate::core::domain::decision::Decision;
use crate::core::domain::extraction::DecisionExtraction;
use crate::core::domain::fate::{decision_family, DecisionFamily};
use crate::core::domain::object::ArchiveCopy;
use crate::core::domain::resolution::{place_from_here, FromHereDirectory};
use crate::core::domain::root::Root;
use crate::core::domain::source::Source;
use crate::core::repo;
use crate::core::repo::decision::DecisionScopeRow;

use super::fs;

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
    /// Every archive-side copy of the present rows' objects, each with the
    /// standing of the root it stands on. The account's evidence: what a
    /// coverage or custody claim about this root actually rests on.
    pub archive_copies: HashMap<i64, Vec<ArchiveCopy>>,
    /// Object ids among the present rows verified present in the archive —
    /// the key set of `archive_copies`, derived here rather than asked for
    /// again, for the membership questions that need no standing.
    pub archived: HashSet<i64>,
    /// Per object among `archived`, the directories the index records an
    /// archiving run drawing from this root as choosing it from: an archive
    /// copy stands stamped by a decision whose extraction rows draw from here
    /// (DB projections only — apply stamps the destination rows, so the
    /// archive side carries the apply id and the ledger names its origin),
    /// placed by that run's row for the copy's own directory. What
    /// `FromHereEvidence` reads: which *file* was chosen is the directories'
    /// to say, never the object's alone.
    pub from_here_directories: HashMap<i64, Vec<FromHereDirectory>>,
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
    let archive_copies = repo::object::batch_archive_copies(conn, &present_object_ids, None)?;
    let archived: HashSet<i64> = archive_copies.keys().copied().collect();
    let from_here_directories = place_from_here(&repo::object::batch_check_archived_from_root(
        conn,
        &present_object_ids,
        root_id,
    )?);

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

    let reachable = fs::dir_exists(Path::new(&root.path));
    let max_decision_id = decision_ids.last().copied();
    let first_indexed = repo::source::min_scanned_at_by_root(conn, root_id)?;

    Ok(RootStory {
        root,
        roots,
        present,
        absent,
        archive_copies,
        archived,
        from_here_directories,
        extractions,
        scope_rows,
        decisions,
        stamp_families,
        reachable,
        first_indexed,
        max_decision_id,
    })
}
