use anyhow::{bail, Result};
use rusqlite::Connection;

use crate::domain::format_count;
use crate::repo;

// =============================================================================
// roots rm: plan/execute
// =============================================================================

#[allow(dead_code)]
pub struct RemoveRootPlan {
    pub root_id: i64,
    pub root_path: String,
    pub role: String,
    pub source_count: i64,
    pub in_archive_count: i64,
    pub not_in_archive: i64,
    pub note_count: usize,
}

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

    let note_count = repo::note::count_subtree_notes(conn, root_id, "")?;

    Ok(RemoveRootPlan {
        root_id,
        root_path: root.path.clone(),
        role: root.role.clone(),
        source_count,
        in_archive_count,
        not_in_archive,
        note_count,
    })
}

/// Execute the removal. Deletes notes, facts, sources, and the root.
pub fn execute_remove(conn: &Connection, plan: &RemoveRootPlan) -> Result<RemoveRootResult> {
    let deleted_notes = repo::note::delete_by_root(conn, plan.root_id)?;
    let deleted_sources = repo::root::remove(conn, plan.root_id)?;

    let summary = format!(
        "Removed root {} and {} sources",
        plan.root_id, deleted_sources
    );

    Ok(RemoveRootResult {
        deleted_sources,
        deleted_notes,
        summary,
    })
}

// =============================================================================
// suspend/unsuspend: simple operations
// =============================================================================

#[allow(dead_code)]
#[derive(Debug)]
pub struct SuspendResult {
    pub root_id: i64,
    pub root_path: String,
    pub source_count: i64,
    pub summary: String,
}

/// Suspend a root. Returns info message if already suspended.
pub fn execute_suspend(conn: &Connection, root_id: i64) -> Result<SuspendResult> {
    let roots = repo::root::fetch_all(conn)?;
    let root = roots
        .iter()
        .find(|r| r.id == root_id)
        .ok_or_else(|| anyhow::anyhow!("Root {} not found", root_id))?;

    if root.is_suspended() {
        bail!("Root {} is already suspended: {}", root_id, root.path);
    }

    repo::root::set_suspended(conn, root_id, true)?;
    let counts = repo::root::fetch_file_counts(conn, &[root_id])?;
    let source_count = counts.get(&root_id).copied().unwrap_or(0);

    let summary = format!(
        "Suspended root {}: {} ({} sources)",
        root_id,
        root.path,
        format_count(source_count)
    );

    Ok(SuspendResult {
        root_id,
        root_path: root.path.clone(),
        source_count,
        summary,
    })
}

/// Unsuspend a root. Returns info message if not suspended.
pub fn execute_unsuspend(conn: &Connection, root_id: i64) -> Result<SuspendResult> {
    let roots = repo::root::fetch_all(conn)?;
    let root = roots
        .iter()
        .find(|r| r.id == root_id)
        .ok_or_else(|| anyhow::anyhow!("Root {} not found", root_id))?;

    if !root.is_suspended() {
        bail!("Root {} is not suspended: {}", root_id, root.path);
    }

    repo::root::set_suspended(conn, root_id, false)?;
    let counts = repo::root::fetch_file_counts(conn, &[root_id])?;
    let source_count = counts.get(&root_id).copied().unwrap_or(0);

    let summary = format!(
        "Unsuspended root {}: {} ({} sources)",
        root_id,
        root.path,
        format_count(source_count)
    );

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
    use crate::repo::db::open_in_memory_for_test;

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

    // =========================================================================
    // execute_remove tests
    // =========================================================================

    #[test]
    fn execute_remove_deletes_all() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, "a.jpg");
        insert_source(&conn, root_id, "b.jpg");
        insert_note(&conn, root_id, "", "a note");

        let plan = plan_remove(&conn, root_id).unwrap();
        let result = execute_remove(&conn, &plan).unwrap();

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
        let result = execute_remove(&conn, &plan).unwrap();

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

        let result = execute_suspend(&conn, root_id).unwrap();

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

        let result = execute_suspend(&conn, root_id);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already suspended"));
    }

    #[test]
    fn suspend_summary_includes_path_and_count() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/mnt/drive", "source", false);
        // No sources — count should be 0
        let result = execute_suspend(&conn, root_id).unwrap();
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

        let result = execute_unsuspend(&conn, root_id).unwrap();

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

        let result = execute_unsuspend(&conn, root_id);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not suspended"));
    }

    #[test]
    fn unsuspend_summary_includes_path_and_count() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/mnt/drive", "source", true);
        insert_source(&conn, root_id, "a.jpg");
        insert_source(&conn, root_id, "b.jpg");
        insert_source(&conn, root_id, "c.jpg");

        let result = execute_unsuspend(&conn, root_id).unwrap();
        assert_eq!(
            result.summary,
            format!("Unsuspended root {}: /mnt/drive (3 sources)", root_id)
        );
    }
}
