use anyhow::{bail, Result};
use rusqlite::Connection;

use crate::core::domain::decision::{DecisionCommand, DecisionStatus};
use crate::core::domain::format_count;
use crate::core::repo;
use crate::notes::count_subtree_notes;
use crate::ops::decision::{DecisionCounts, DecisionParams, DecisionRecorder};
use crate::roots::repo as roots_repo;

// =============================================================================
// roots rm: plan/execute
// =============================================================================

pub struct RemoveRootPlan {
    pub root_id: i64,
    pub root_path: String,
    pub role: String,
    pub source_count: i64,
    pub in_archive_count: i64,
    pub not_in_archive: i64,
    pub note_count: usize,
    /// The root's bound story, when one exists: the receipt of the latest
    /// `roots retire` decision touching this root. `None` means removal
    /// destroys the only reviewable record.
    pub retirement: Option<RetirementPointer>,
}

/// Pointer to a root's retirement artifact (the book).
pub struct RetirementPointer {
    pub artifact_display: String,
}

/// What `remove_root_data` deleted — the removal mechanics rm and retire
/// share, under their different decisions.
pub struct RemovedRootData {
    pub deleted_sources: i64,
    pub deleted_notes: usize,
}

/// The removal's typed account. Only `summary` is printed today; the counts
/// are the operation's result, kept for callers that report rather than echo.
#[allow(dead_code)]
pub struct RemoveRootResult {
    pub deleted_sources: i64,
    pub deleted_notes: usize,
    pub summary: String,
}

/// Compute what removing a root would affect.
pub fn plan_remove(conn: &Connection, root_id: i64) -> Result<RemoveRootPlan> {
    let roots = repo::root::fetch_all(conn)?;
    let root = roots
        .iter()
        .find(|r| r.id == root_id)
        .ok_or_else(|| anyhow::anyhow!("Root {} not found", root_id))?;

    let sources = repo::source::batch_fetch_by_roots(conn, &[root_id])?;
    let source_count = sources.len() as i64;

    let object_ids: Vec<i64> = sources.iter().filter_map(|s| s.object_id).collect();
    let archived_objects = repo::object::batch_check_archived(conn, &object_ids, None)?;
    let in_archive_count = sources
        .iter()
        .filter(|s| {
            s.object_id
                .map(|id| archived_objects.contains(&id))
                .unwrap_or(false)
        })
        .count() as i64;
    let not_in_archive = source_count - in_archive_count;

    let note_count = count_subtree_notes(conn, root_id, "")?;

    let retirement = repo::decision::fetch_latest_receipt_for_root(
        conn,
        DecisionCommand::RootsRetire.as_str(),
        root_id,
    )?
    .map(|(receipt_root_id, rel_path)| {
        let receipt_root = roots
            .iter()
            .find(|r| r.id == receipt_root_id)
            .map(|r| r.path.clone())
            .unwrap_or_else(|| format!("root #{receipt_root_id} (removed)"));
        RetirementPointer {
            artifact_display: format!("{receipt_root}/{rel_path}"),
        }
    });

    Ok(RemoveRootPlan {
        root_id,
        root_path: root.path.clone(),
        role: root.role.clone(),
        source_count,
        in_archive_count,
        not_in_archive,
        note_count,
        retirement,
    })
}

/// Delete a root's notes, facts, sources, and root row — the shared removal
/// mechanics under rm's and retire's different decisions. No transaction
/// management here; callers establish scope.
pub fn remove_root_data(conn: &Connection, root_id: i64) -> Result<RemovedRootData> {
    let deleted_notes = roots_repo::note::delete_by_root(conn, root_id)?;
    let deleted_sources = roots_repo::root::remove(conn, root_id)?;
    Ok(RemovedRootData {
        deleted_sources,
        deleted_notes,
    })
}

/// Execute the removal. Deletes notes, facts, sources, and the root.
///
/// Runs outside a transaction: the four deletes are separate statements, so an
/// interruption partway can leave a root with some of its rows already gone.
/// Retirement's release performs the same deletion inside one transaction —
/// this path does not, and closing that gap is its own change.
pub fn execute_remove(
    conn: &Connection,
    plan: &RemoveRootPlan,
    decision: Option<&DecisionParams>,
) -> Result<RemoveRootResult> {
    let mut recorder = decision.map(|d| DecisionRecorder::start(conn, d, None));

    let removed = remove_root_data(conn, plan.root_id)?;
    let (deleted_sources, deleted_notes) = (removed.deleted_sources, removed.deleted_notes);

    let summary = format!(
        "Removed root {} and {} sources",
        plan.root_id, deleted_sources
    );

    if let Some(recorder) = recorder.as_mut() {
        recorder.complete(
            conn,
            DecisionStatus::Completed,
            DecisionCounts {
                attempted: Some(plan.source_count),
                completed: Some(deleted_sources),
                failed: None,
                skipped: None,
            },
            &summary,
        );
    }

    Ok(RemoveRootResult {
        deleted_sources,
        deleted_notes,
        summary,
    })
}

// =============================================================================
// suspend/unsuspend: simple operations
// =============================================================================

/// The flip's typed account. Only `summary` is printed today; the identity
/// and count are the operation's result, kept for callers that report rather
/// than echo.
#[allow(dead_code)]
#[derive(Debug)]
pub struct SuspendResult {
    pub root_id: i64,
    pub root_path: String,
    pub source_count: i64,
    pub summary: String,
}

/// Suspend a root. Returns info message if already suspended.
pub fn execute_suspend(
    conn: &Connection,
    root_id: i64,
    decision: Option<&DecisionParams>,
) -> Result<SuspendResult> {
    let roots = repo::root::fetch_all(conn)?;
    let root = roots
        .iter()
        .find(|r| r.id == root_id)
        .ok_or_else(|| anyhow::anyhow!("Root {} not found", root_id))?;

    // "already suspended" is load-bearing text, not just a message: the caller
    // matches on it to report a no-op as information rather than a failure.
    if root.is_suspended() {
        bail!("Root {} is already suspended: {}", root_id, root.path);
    }

    // The recorder starts only past that check, so a no-op records nothing.

    let mut recorder = decision.map(|d| DecisionRecorder::start(conn, d, None));

    roots_repo::root::set_suspended(conn, root_id, true)?;
    let counts = roots_repo::root::fetch_file_counts(conn, &[root_id])?;
    let source_count = counts.get(&root_id).copied().unwrap_or(0);

    let summary = format!(
        "Suspended root {}: {} ({} sources)",
        root_id,
        root.path,
        format_count(source_count)
    );

    if let Some(recorder) = recorder.as_mut() {
        recorder.complete(
            conn,
            DecisionStatus::Completed,
            DecisionCounts {
                attempted: None,
                completed: None,
                failed: None,
                skipped: None,
            },
            &summary,
        );
    }

    Ok(SuspendResult {
        root_id,
        root_path: root.path.clone(),
        source_count,
        summary,
    })
}

/// Unsuspend a root. Returns info message if not suspended.
pub fn execute_unsuspend(
    conn: &Connection,
    root_id: i64,
    decision: Option<&DecisionParams>,
) -> Result<SuspendResult> {
    let roots = repo::root::fetch_all(conn)?;
    let root = roots
        .iter()
        .find(|r| r.id == root_id)
        .ok_or_else(|| anyhow::anyhow!("Root {} not found", root_id))?;

    // "not suspended" is load-bearing text, matched by the caller the same way
    // the suspend message is.
    if !root.is_suspended() {
        bail!("Root {} is not suspended: {}", root_id, root.path);
    }

    // The recorder starts only past that check, so a no-op records nothing.

    let mut recorder = decision.map(|d| DecisionRecorder::start(conn, d, None));

    roots_repo::root::set_suspended(conn, root_id, false)?;
    let counts = roots_repo::root::fetch_file_counts(conn, &[root_id])?;
    let source_count = counts.get(&root_id).copied().unwrap_or(0);

    let summary = format!(
        "Unsuspended root {}: {} ({} sources)",
        root_id,
        root.path,
        format_count(source_count)
    );

    if let Some(recorder) = recorder.as_mut() {
        recorder.complete(
            conn,
            DecisionStatus::Completed,
            DecisionCounts {
                attempted: None,
                completed: None,
                failed: None,
                skipped: None,
            },
            &summary,
        );
    }

    Ok(SuspendResult {
        root_id,
        root_path: root.path.clone(),
        source_count,
        summary,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::repo::db::open_in_memory_for_test;

    fn setup_test_db() -> Connection {
        open_in_memory_for_test()
    }

    fn insert_root(conn: &Connection, path: &str, role: &str, suspended: bool) -> i64 {
        conn.execute(
            "INSERT INTO roots (path, role, suspended) VALUES (?, ?, ?)",
            rusqlite::params![path, role, suspended as i64],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn insert_source(conn: &Connection, root_id: i64, rel_path: &str) -> i64 {
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, device, inode, size, mtime, partial_hash, scanned_at, last_seen_at)
             VALUES (?, ?, 1, 1, 100, 1700000000, 'testhash', 0, 0)",
            rusqlite::params![root_id, rel_path],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn insert_note(conn: &Connection, root_id: i64, rel_path: &str, text: &str) {
        conn.execute(
            "INSERT INTO notes (root_id, rel_path, text, created_at) VALUES (?, ?, ?, 0)",
            rusqlite::params![root_id, rel_path, text],
        )
        .unwrap();
    }

    // =========================================================================
    // plan_remove tests
    // =========================================================================

    #[test]
    fn plan_remove_returns_counts() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, "a.jpg");
        insert_source(&conn, root_id, "b.jpg");
        insert_source(&conn, root_id, "c.jpg");
        insert_note(&conn, root_id, "", "root note");
        insert_note(&conn, root_id, "subdir", "subdir note");

        let plan = plan_remove(&conn, root_id).unwrap();
        assert_eq!(plan.root_id, root_id);
        assert_eq!(plan.root_path, "/photos");
        assert_eq!(plan.role, "source");
        assert_eq!(plan.source_count, 3);
        assert_eq!(plan.note_count, 2);
    }

    #[test]
    fn plan_remove_empty_root() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/empty", "source", false);

        let plan = plan_remove(&conn, root_id).unwrap();
        assert_eq!(plan.source_count, 0);
        assert_eq!(plan.note_count, 0);
    }

    #[test]
    fn plan_remove_nonexistent_root() {
        let conn = setup_test_db();
        let result = plan_remove(&conn, 999);
        assert!(result.is_err());
    }

    fn insert_retire_decision(
        conn: &Connection,
        root_id: i64,
        created_at: i64,
        receipt_root_id: Option<i64>,
        receipt_rel_path: Option<&str>,
    ) -> i64 {
        conn.execute(
            "INSERT INTO decisions
             (command, command_line, status, canon_version, created_at,
              receipt_root_id, receipt_rel_path)
             VALUES ('roots_retire', 'canon roots retire', 'completed', '0', ?1, ?2, ?3)",
            rusqlite::params![created_at, receipt_root_id, receipt_rel_path],
        )
        .unwrap();
        let decision_id = conn.last_insert_rowid();
        conn.execute(
            "INSERT INTO decision_scopes (decision_id, root_id, root_path, rel_prefix)
             VALUES (?1, ?2, '/photos', '')",
            rusqlite::params![decision_id, root_id],
        )
        .unwrap();
        decision_id
    }

    #[test]
    fn plan_remove_retirement_is_none_on_plain_root() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let plan = plan_remove(&conn, root_id).unwrap();
        assert!(plan.retirement.is_none());
    }

    #[test]
    fn plan_remove_retirement_points_at_the_bound_story() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let archive_id = insert_root(&conn, "/archive", "archive", false);
        insert_retire_decision(
            &conn,
            root_id,
            100,
            Some(archive_id),
            Some(".canon-ledger/retired/000007-roots_retire.toml"),
        );

        let plan = plan_remove(&conn, root_id).unwrap();
        let pointer = plan.retirement.expect("retirement pointer");
        assert_eq!(
            pointer.artifact_display,
            "/archive/.canon-ledger/retired/000007-roots_retire.toml"
        );
    }

    #[test]
    fn plan_remove_retirement_uses_the_latest_receipted_decision() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let archive_id = insert_root(&conn, "/archive", "archive", false);
        insert_retire_decision(&conn, root_id, 100, Some(archive_id), Some("old.toml"));
        insert_retire_decision(&conn, root_id, 200, Some(archive_id), Some("new.toml"));
        // Newer still, but no receipt recorded — must not shadow the bound one.
        insert_retire_decision(&conn, root_id, 300, None, None);
        // Another root's retirement is not this root's story.
        let other = insert_root(&conn, "/other", "source", false);
        insert_retire_decision(&conn, other, 400, Some(archive_id), Some("other.toml"));

        let plan = plan_remove(&conn, root_id).unwrap();
        assert_eq!(
            plan.retirement.unwrap().artifact_display,
            "/archive/new.toml"
        );
    }

    #[test]
    fn plan_remove_retirement_marks_a_removed_receipt_root() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        insert_retire_decision(&conn, root_id, 100, Some(999), Some("book.toml"));

        let plan = plan_remove(&conn, root_id).unwrap();
        assert_eq!(
            plan.retirement.unwrap().artifact_display,
            "root #999 (removed)/book.toml"
        );
    }

    // =========================================================================
    // remove_root_data / execute_remove tests
    // =========================================================================

    #[test]
    fn remove_root_data_deletes_notes_facts_sources_and_root_row() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let keep = insert_root(&conn, "/archive", "archive", false);
        let source_id = insert_source(&conn, root_id, "a.jpg");
        insert_source(&conn, root_id, "b.jpg");
        insert_note(&conn, root_id, "", "a note");
        conn.execute(
            "INSERT INTO facts (entity_type, entity_id, key, value_text, observed_at, observed_basis_rev)
             VALUES ('source', ?1, 'content.Make', 'X', 0, 0)",
            [source_id],
        )
        .unwrap();

        let removed = remove_root_data(&conn, root_id).unwrap();
        assert_eq!(removed.deleted_sources, 2);
        assert_eq!(removed.deleted_notes, 1);

        let count = |sql: &str| -> i64 { conn.query_row(sql, [], |r| r.get(0)).unwrap() };
        assert_eq!(count("SELECT COUNT(*) FROM sources"), 0);
        assert_eq!(count("SELECT COUNT(*) FROM notes"), 0);
        assert_eq!(count("SELECT COUNT(*) FROM facts"), 0);
        assert_eq!(count("SELECT COUNT(*) FROM roots"), 1);
        let survivor: i64 = conn
            .query_row("SELECT id FROM roots", [], |r| r.get(0))
            .unwrap();
        assert_eq!(survivor, keep);
    }

    #[test]
    fn execute_remove_deletes_all() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, "a.jpg");
        insert_source(&conn, root_id, "b.jpg");
        insert_note(&conn, root_id, "", "a note");

        let plan = plan_remove(&conn, root_id).unwrap();
        let result = execute_remove(&conn, &plan, None).unwrap();

        assert_eq!(result.deleted_sources, 2);
        assert_eq!(result.deleted_notes, 1);

        // Verify root is gone
        let roots = repo::root::fetch_all(&conn).unwrap();
        assert!(roots.is_empty());
    }

    #[test]
    fn execute_remove_summary_format() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, "a.jpg");

        let plan = plan_remove(&conn, root_id).unwrap();
        let result = execute_remove(&conn, &plan, None).unwrap();

        assert_eq!(
            result.summary,
            format!("Removed root {} and 1 sources", root_id)
        );
    }

    // =========================================================================
    // execute_suspend tests
    // =========================================================================

    #[test]
    fn suspend_active_root() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, "a.jpg");
        insert_source(&conn, root_id, "b.jpg");

        let result = execute_suspend(&conn, root_id, None).unwrap();

        assert_eq!(result.root_id, root_id);
        assert_eq!(result.root_path, "/photos");
        assert_eq!(result.source_count, 2);
        assert!(result.summary.contains("Suspended root"));
        assert!(result.summary.contains("/photos"));
        assert!(result.summary.contains("2 sources"));

        // Verify actually suspended in DB
        let roots = repo::root::fetch_all(&conn).unwrap();
        assert!(roots[0].is_suspended());
    }

    #[test]
    fn suspend_already_suspended_errors() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", true);

        let result = execute_suspend(&conn, root_id, None);
        assert!(result.is_err());
        // The substring, not just the failure: the command surface matches on
        // it to report the no-op as information instead of an error.
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("already suspended"));
    }

    #[test]
    fn suspend_summary_includes_path_and_count() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/mnt/drive", "source", false);
        // No sources — count should be 0
        let result = execute_suspend(&conn, root_id, None).unwrap();
        assert_eq!(
            result.summary,
            format!("Suspended root {}: /mnt/drive (0 sources)", root_id)
        );
    }

    // =========================================================================
    // execute_unsuspend tests
    // =========================================================================

    #[test]
    fn unsuspend_suspended_root() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", true);
        insert_source(&conn, root_id, "a.jpg");

        let result = execute_unsuspend(&conn, root_id, None).unwrap();

        assert_eq!(result.root_id, root_id);
        assert!(result.summary.contains("Unsuspended root"));
        assert!(result.summary.contains("/photos"));

        // Verify actually unsuspended in DB
        let roots = repo::root::fetch_all(&conn).unwrap();
        assert!(roots[0].is_active());
    }

    #[test]
    fn unsuspend_not_suspended_errors() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);

        let result = execute_unsuspend(&conn, root_id, None);
        assert!(result.is_err());
        // The substring is matched by the command surface, same as suspend's.
        assert!(result.unwrap_err().to_string().contains("not suspended"));
    }

    #[test]
    fn unsuspend_summary_includes_path_and_count() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/mnt/drive", "source", true);
        insert_source(&conn, root_id, "a.jpg");
        insert_source(&conn, root_id, "b.jpg");
        insert_source(&conn, root_id, "c.jpg");

        let result = execute_unsuspend(&conn, root_id, None).unwrap();
        assert_eq!(
            result.summary,
            format!("Unsuspended root {}: /mnt/drive (3 sources)", root_id)
        );
    }

    // =========================================================================
    // decision recording
    // =========================================================================

    /// DB-only recording: `roots` writes no receipt file, so no ledger root
    /// needs to exist and nothing touches the filesystem.
    fn recording_params(command: DecisionCommand, root_id: i64, root_path: &str) -> DecisionParams {
        DecisionParams {
            command,
            scope: vec![crate::core::domain::scope::DecisionScope::new(
                root_id,
                root_path.to_string(),
                String::new(),
            )],
            command_line: "canon roots".to_string(),
            reason: None,
            record_enabled: true,
            receipt_enabled: false,
            ledger_config: crate::core::domain::config::LedgerConfig::default(),
        }
    }

    fn decision_count(conn: &Connection) -> i64 {
        conn.query_row("SELECT COUNT(*) FROM decisions", [], |r| r.get(0))
            .unwrap()
    }

    #[test]
    fn execute_remove_records_a_completed_decision() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, "a.jpg");
        insert_source(&conn, root_id, "b.jpg");

        let plan = plan_remove(&conn, root_id).unwrap();
        let params = recording_params(DecisionCommand::RootsRm, root_id, "/photos");
        execute_remove(&conn, &plan, Some(&params)).unwrap();

        assert_eq!(decision_count(&conn), 1);
        let (command, status, attempted, completed): (String, String, i64, i64) = conn
            .query_row(
                "SELECT command, status, count_attempted, count_completed FROM decisions",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();
        assert_eq!(command, "roots_rm");
        assert_eq!(status, "completed");
        assert_eq!(attempted, 2);
        assert_eq!(completed, 2);
    }

    #[test]
    fn execute_suspend_records_a_completed_decision() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);

        let params = recording_params(DecisionCommand::RootsSuspend, root_id, "/photos");
        execute_suspend(&conn, root_id, Some(&params)).unwrap();

        assert_eq!(decision_count(&conn), 1);
        let (command, status): (String, String) = conn
            .query_row("SELECT command, status FROM decisions", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        assert_eq!(command, "roots_suspend");
        assert_eq!(status, "completed");
    }

    #[test]
    fn suspend_of_a_suspended_root_records_nothing() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", true);

        let params = recording_params(DecisionCommand::RootsSuspend, root_id, "/photos");
        assert!(execute_suspend(&conn, root_id, Some(&params)).is_err());

        assert_eq!(
            decision_count(&conn),
            0,
            "a root already in the asked-for state changes nothing, so there is nothing to record"
        );
    }

    #[test]
    fn execute_unsuspend_records_a_completed_decision() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", true);

        let params = recording_params(DecisionCommand::RootsUnsuspend, root_id, "/photos");
        execute_unsuspend(&conn, root_id, Some(&params)).unwrap();

        assert_eq!(decision_count(&conn), 1);
        let (command, status): (String, String) = conn
            .query_row("SELECT command, status FROM decisions", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        assert_eq!(command, "roots_unsuspend");
        assert_eq!(status, "completed");
    }

    #[test]
    fn unsuspend_of_an_active_root_records_nothing() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);

        let params = recording_params(DecisionCommand::RootsUnsuspend, root_id, "/photos");
        assert!(execute_unsuspend(&conn, root_id, Some(&params)).is_err());

        assert_eq!(
            decision_count(&conn),
            0,
            "a root already in the asked-for state changes nothing, so there is nothing to record"
        );
    }
}
