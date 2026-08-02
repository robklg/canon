//! Retirement operations: the readiness review.
//!
//! `compute_readiness` is the one structural fetch of the retirement
//! ceremony — the readiness review is its first lens, and the book compile
//! (the second lens) builds on the same substrate. One fetch, two
//! renderings: the gate and the book can never drift apart.

#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::path::Path;

use anyhow::{bail, Result};
use rusqlite::Connection;

use crate::domain::config::LedgerConfig;
use crate::domain::decision::DecisionCommand;
use crate::domain::retire::{build_account, derive_readiness, Readiness, ResolutionAccount};
use crate::domain::trail::{decision_family, DecisionFamily};
use crate::domain::Root;
use crate::ops;
use crate::repo;

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
    /// Earliest scan decision touching this root; `None` = unknown (the
    /// root may predate decision recording).
    pub first_scan: Option<i64>,
    pub account: ResolutionAccount,
    pub gaps: GapFacts,
    pub readiness: Readiness,
    /// Review-time basis: total source rows (present + absent).
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

pub fn compute_readiness(conn: &Connection, root_id: i64) -> Result<ReadinessReview> {
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

    // One decision fetch serves three needs: the absent rows' stamp
    // families, first-scan, and the open-intentions comparison.
    let mut decision_ids: Vec<i64> = absent.iter().filter_map(|s| s.decision_id).collect();
    decision_ids.extend(present.iter().filter_map(|s| s.decision_id));
    decision_ids.extend(scope_rows.iter().map(|r| r.decision_id));
    decision_ids.extend(extractions.iter().map(|r| r.decision_id));
    decision_ids.sort_unstable();
    decision_ids.dedup();
    let decisions = repo::decision::fetch_by_ids(conn, &decision_ids)?;
    let by_id: HashMap<i64, &crate::domain::decision::Decision> =
        decisions.iter().map(|d| (d.id, d)).collect();

    let stamp_families: HashMap<i64, DecisionFamily> = decisions
        .iter()
        .map(|d| (d.id, decision_family(&d.command)))
        .collect();

    let scope_decision_ids: HashSet<i64> = scope_rows.iter().map(|r| r.decision_id).collect();
    let first_scan = scope_decision_ids
        .iter()
        .filter_map(|id| by_id.get(id))
        .filter(|d| d.command == DecisionCommand::Scan.as_str())
        .map(|d| d.created_at)
        .min();

    let last_apply_from_here = extractions
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

    let account = build_account(&present, &absent, &archived, &extractions, &stamp_families);
    let readiness = derive_readiness(&account);

    let gaps = GapFacts {
        last_scanned_at: root.last_scanned_at,
        reachable: ops::fs::dir_exists(Path::new(&root.path)),
        open_cluster_intentions,
    };

    let snapshot_source_count = (present.len() + absent.len()) as i64;
    let snapshot_max_decision_id = decision_ids.last().copied();

    Ok(ReadinessReview {
        root,
        first_scan,
        account,
        gaps,
        readiness,
        snapshot_source_count,
        snapshot_max_decision_id,
    })
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

    // first_scan

    #[test]
    fn first_scan_is_the_earliest_scan_decision() {
        let conn = open_in_memory_for_test();
        let root = insert_test_root(&conn, "/r", "source", false);
        let s1 = insert_decision(&conn, "scan", 300);
        let s2 = insert_decision(&conn, "scan", 100);
        let other = insert_decision(&conn, "exclude_set", 50);
        for d in [s1, s2, other] {
            scope(&conn, d, root);
        }
        let review = compute_readiness(&conn, root).unwrap();
        assert_eq!(review.first_scan, Some(100), "earliest scan, not exclude");
    }

    #[test]
    fn first_scan_unknown_without_scan_decisions() {
        let conn = open_in_memory_for_test();
        let root = insert_test_root(&conn, "/r", "source", false);
        let review = compute_readiness(&conn, root).unwrap();
        assert_eq!(review.first_scan, None);
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
}
