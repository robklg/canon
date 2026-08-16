//! Note operations — view, list, plan/execute clear, survey context.
//!
//! Composes repo and domain functions into note behaviors.
//! No SQL in this layer — only calls to repo and domain functions.

use std::collections::HashMap;

use anyhow::{bail, Result};

use crate::core::domain::decision::DecisionStatus;
use crate::core::domain::root::Root;
use crate::notes::domain::{ancestor_paths, LocationEntry, Note};
use crate::notes::repo as notes_repo;
use crate::ops::decision::{DecisionCounts, DecisionParams, DecisionRecorder};
use crate::repo::{self, Connection};

// ============================================================================
// Types
// ============================================================================

/// Resolved note scope — a single (root_id, rel_path) pair.
#[derive(Clone)]
pub struct NoteScope {
    pub root_id: i64,
    pub root_path: String,
    pub rel_path: String,
}

impl NoteScope {
    /// Display path: rel_path if non-empty, otherwise root_path.
    pub fn display(&self) -> String {
        if self.rel_path.is_empty() {
            self.root_path.clone()
        } else {
            self.rel_path.clone()
        }
    }
}

/// Result of viewing notes at a scope.
pub struct NoteViewResult {
    pub scope: NoteScope,
    pub notes: Vec<Note>,
    pub ancestor_count: usize,
    pub descendant_location_count: usize,
}

/// Result of listing notes (temporal mode).
pub struct NoteListResult {
    pub notes: Vec<Note>,
    pub roots: HashMap<i64, Root>,
    pub total_note_count: usize,
    pub total_location_count: usize,
}

/// Result of listing locations (spatial mode).
pub struct NoteSpatialResult {
    pub locations: Vec<LocationEntry>,
    pub roots: HashMap<i64, Root>,
    pub total_location_count: usize,
}

/// Plan for recursive clear.
pub struct ClearPlan {
    pub scope: NoteScope,
    pub note_count: usize,
    pub location_count: usize,
}

/// Info needed by survey for note display.
pub struct SurveyNoteContext {
    pub subtree_notes: Vec<Note>,
    pub ancestor_count: usize,
}

// ============================================================================
// Scope resolution
// ============================================================================

/// Resolve a canonical path to a NoteScope.
pub fn resolve_note_scope(path: &str, roots: &[Root]) -> Result<NoteScope> {
    match crate::core::domain::root::find_containing_root(path, roots) {
        Some((root_id, root_path, _role, rel_path)) => Ok(NoteScope {
            root_id,
            root_path,
            rel_path,
        }),
        None => bail!("{path} is not under any known root"),
    }
}

// ============================================================================
// Operations
// ============================================================================

/// View notes at an exact scope with spatial context indicators.
pub fn view_notes(conn: &Connection, scope: &NoteScope) -> Result<NoteViewResult> {
    let notes = notes_repo::fetch_by_scope(conn, scope.root_id, &scope.rel_path)?;

    let ancestors = ancestor_paths(&scope.rel_path);
    let ancestor_count = notes_repo::count_ancestor_notes(conn, scope.root_id, &ancestors)?;

    let descendant_location_count =
        notes_repo::count_descendant_locations(conn, scope.root_id, &scope.rel_path)?;

    Ok(NoteViewResult {
        scope: scope.clone(),
        notes,
        ancestor_count,
        descendant_location_count,
    })
}

/// Default listing cap.
const DEFAULT_LIMIT: usize = 10;

/// List notes globally, temporal mode.
/// limit: None = default (10), Some(0) = unlimited, Some(n) = n.
pub fn list_notes_global(conn: &Connection, limit: Option<usize>) -> Result<NoteListResult> {
    let effective = limit.unwrap_or(DEFAULT_LIMIT);
    let (mut notes, total_note_count, total_location_count) =
        notes_repo::fetch_recent(conn, effective)?;
    notes.reverse(); // oldest-first for display
    let all_roots = repo::root::fetch_all(conn)?;
    let roots: HashMap<i64, Root> = all_roots.into_iter().map(|r| (r.id, r)).collect();
    Ok(NoteListResult {
        notes,
        roots,
        total_note_count,
        total_location_count,
    })
}

/// List notes recursively, temporal mode.
pub fn list_notes_recursive(
    conn: &Connection,
    scope: &NoteScope,
    limit: Option<usize>,
) -> Result<NoteListResult> {
    let effective = limit.unwrap_or(DEFAULT_LIMIT);
    let (mut notes, total_note_count, total_location_count) =
        notes_repo::fetch_recent_subtree(conn, scope.root_id, &scope.rel_path, effective)?;
    notes.reverse(); // oldest-first for display
    let all_roots = repo::root::fetch_all(conn)?;
    let roots: HashMap<i64, Root> = all_roots.into_iter().map(|r| (r.id, r)).collect();
    Ok(NoteListResult {
        notes,
        roots,
        total_note_count,
        total_location_count,
    })
}

/// List locations globally, spatial mode.
pub fn list_locations_global(conn: &Connection, limit: Option<usize>) -> Result<NoteSpatialResult> {
    let effective = limit.unwrap_or(DEFAULT_LIMIT);
    let (mut locations, total_location_count) = notes_repo::fetch_locations(conn, effective)?;
    locations.reverse(); // oldest-first for display
    let all_roots = repo::root::fetch_all(conn)?;
    let roots: HashMap<i64, Root> = all_roots.into_iter().map(|r| (r.id, r)).collect();
    Ok(NoteSpatialResult {
        locations,
        roots,
        total_location_count,
    })
}

/// List locations recursively, spatial mode.
pub fn list_locations_recursive(
    conn: &Connection,
    scope: &NoteScope,
    limit: Option<usize>,
) -> Result<NoteSpatialResult> {
    let effective = limit.unwrap_or(DEFAULT_LIMIT);
    let (mut locations, total_location_count) =
        notes_repo::fetch_locations_subtree(conn, scope.root_id, &scope.rel_path, effective)?;
    locations.reverse(); // oldest-first for display
    let all_roots = repo::root::fetch_all(conn)?;
    let roots: HashMap<i64, Root> = all_roots.into_iter().map(|r| (r.id, r)).collect();
    Ok(NoteSpatialResult {
        locations,
        roots,
        total_location_count,
    })
}

/// Plan a recursive clear — compute counts without deleting.
pub fn plan_clear_recursive(conn: &Connection, scope: &NoteScope) -> Result<ClearPlan> {
    let note_count = notes_repo::count_subtree_notes(conn, scope.root_id, &scope.rel_path)?;
    let location_count = notes_repo::count_subtree_locations(conn, scope.root_id, &scope.rel_path)?;
    Ok(ClearPlan {
        scope: scope.clone(),
        note_count,
        location_count,
    })
}

/// Result of a recursive note clear.
#[allow(dead_code)]
pub struct ClearRecursiveResult {
    pub deleted: usize,
    pub summary: String,
    pub warnings: Vec<String>,
}

/// Shared clear execution: delete → record, atomically.
///
/// Decision row only, never a receipt: receipts record *content fates* (the
/// receipt gate is the per-item source state transition), and notes are the
/// user's own annotations, not assets — clearing them is housekeeping of the
/// scaffolding. The decision row keeps the disappearance non-mute. A clear
/// that touches nothing records nothing (0-item convention).
///
/// Pure-DB op: the transaction covers the deletion and the decision record
/// together (write-path-atomicity ADR — pure-DB ops are atomic).
fn run_clear(
    conn: &mut Connection,
    scope: &NoteScope,
    decision: Option<&DecisionParams>,
    delete: fn(&Connection, i64, &str) -> Result<usize>,
    compose_summary: impl FnOnce(usize) -> String,
) -> Result<(usize, String, Vec<String>)> {
    let tx = conn.transaction()?;

    let deleted = delete(&tx, scope.root_id, &scope.rel_path)?;
    let summary = compose_summary(deleted);

    let mut recorder = if deleted > 0 {
        decision.map(|d| DecisionRecorder::start(&tx, d, None))
    } else {
        None
    };
    if let Some(r) = recorder.as_mut() {
        r.complete_db(
            &tx,
            DecisionStatus::Completed,
            DecisionCounts {
                attempted: Some(deleted as i64),
                completed: Some(deleted as i64),
                failed: None,
                skipped: None,
            },
            &summary,
        );
    }
    tx.commit()?;

    let warnings = recorder.map(|mut r| r.take_warnings()).unwrap_or_default();

    Ok((deleted, summary, warnings))
}

/// Execute a recursive clear — delete all notes in scope + descendants.
pub fn execute_clear_recursive(
    conn: &mut Connection,
    scope: &NoteScope,
    decision: Option<&DecisionParams>,
) -> Result<ClearRecursiveResult> {
    let (deleted, summary, warnings) = run_clear(
        conn,
        scope,
        decision,
        notes_repo::clear_subtree,
        |deleted| format!("Cleared {} notes", deleted),
    )?;
    Ok(ClearRecursiveResult {
        deleted,
        summary,
        warnings,
    })
}

/// Result of clearing notes at an exact scope.
#[allow(dead_code)]
pub struct ClearExactResult {
    pub deleted: usize,
    pub summary: String,
    pub warnings: Vec<String>,
}

/// Clear notes at an exact scope (not recursive).
pub fn execute_clear_exact(
    conn: &mut Connection,
    scope: &NoteScope,
    decision: Option<&DecisionParams>,
) -> Result<ClearExactResult> {
    let display = scope.display();
    let (deleted, summary, warnings) = run_clear(
        conn,
        scope,
        decision,
        notes_repo::clear_by_scope,
        |deleted| {
            if deleted == 0 {
                format!("No notes at {display}")
            } else {
                format!("Cleared {deleted} notes at {display}")
            }
        },
    )?;
    Ok(ClearExactResult {
        deleted,
        summary,
        warnings,
    })
}

/// Fetch note context for survey display.
pub fn survey_note_context(
    conn: &Connection,
    root_id: i64,
    rel_path: &str,
) -> Result<SurveyNoteContext> {
    let subtree_notes = notes_repo::fetch_subtree(conn, root_id, rel_path)?;
    let ancestors = ancestor_paths(rel_path);
    let ancestor_count = notes_repo::count_ancestor_notes(conn, root_id, &ancestors)?;
    Ok(SurveyNoteContext {
        subtree_notes,
        ancestor_count,
    })
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::test_helpers::{insert_note, insert_root, setup_test_db};

    #[test]
    fn view_notes_with_spatial_indicators() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);

        // Ancestor notes
        insert_note(&conn, root_id, "", "root note", 100);
        insert_note(&conn, root_id, "a", "parent note", 200);

        // Exact scope notes
        insert_note(&conn, root_id, "a/b", "note at scope", 300);
        insert_note(&conn, root_id, "a/b", "another at scope", 400);

        // Descendant notes at different locations
        insert_note(&conn, root_id, "a/b/c", "child note", 500);
        insert_note(&conn, root_id, "a/b/d", "another child", 600);
        insert_note(&conn, root_id, "a/b/d", "second at d", 700);

        let scope = NoteScope {
            root_id,
            root_path: "/photos".to_string(),
            rel_path: "a/b".to_string(),
        };

        let result = view_notes(&conn, &scope).unwrap();

        // Exact scope notes
        assert_eq!(result.notes.len(), 2);
        assert_eq!(result.notes[0].text, "note at scope");
        assert_eq!(result.notes[1].text, "another at scope");

        // Ancestor count: root note + parent note = 2
        assert_eq!(result.ancestor_count, 2);

        // Descendant locations: a/b/c and a/b/d = 2
        assert_eq!(result.descendant_location_count, 2);
    }

    #[test]
    fn view_notes_empty_scope() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);

        let scope = NoteScope {
            root_id,
            root_path: "/photos".to_string(),
            rel_path: "empty".to_string(),
        };

        let result = view_notes(&conn, &scope).unwrap();

        assert!(result.notes.is_empty());
        assert_eq!(result.ancestor_count, 0);
        assert_eq!(result.descendant_location_count, 0);
    }

    #[test]
    fn list_global_includes_all_roots() {
        let conn = setup_test_db();
        let root1 = insert_root(&conn, "/photos", "source", false);
        let root2 = insert_root(&conn, "/archive", "archive", false);

        insert_note(&conn, root1, "a", "root1 note", 100);
        insert_note(&conn, root2, "b", "root2 note", 200);

        let result = list_notes_global(&conn, Some(0)).unwrap();

        assert_eq!(result.notes.len(), 2);
        assert!(result.roots.contains_key(&root1));
        assert!(result.roots.contains_key(&root2));
    }

    #[test]
    fn list_recursive_scoped() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);

        insert_note(&conn, root_id, "a", "scope", 100);
        insert_note(&conn, root_id, "a/b", "child", 200);
        insert_note(&conn, root_id, "a/b/c", "grandchild", 300);
        insert_note(&conn, root_id, "other", "unrelated", 400);

        let scope = NoteScope {
            root_id,
            root_path: "/photos".to_string(),
            rel_path: "a".to_string(),
        };

        let result = list_notes_recursive(&conn, &scope, Some(0)).unwrap();

        assert_eq!(result.notes.len(), 3);
        // All notes should be from scope "a" or descendants
        for note in &result.notes {
            assert!(
                note.rel_path == "a" || note.rel_path.starts_with("a/"),
                "unexpected rel_path: {}",
                note.rel_path
            );
        }
    }

    #[test]
    fn plan_clear_recursive_correct_counts() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);

        insert_note(&conn, root_id, "a", "scope note 1", 100);
        insert_note(&conn, root_id, "a", "scope note 2", 200);
        insert_note(&conn, root_id, "a/b", "child", 300);
        insert_note(&conn, root_id, "a/c", "another child", 400);
        insert_note(&conn, root_id, "other", "unrelated", 500);

        let scope = NoteScope {
            root_id,
            root_path: "/photos".to_string(),
            rel_path: "a".to_string(),
        };

        let plan = plan_clear_recursive(&conn, &scope).unwrap();

        // 4 notes total in subtree (2 at "a", 1 at "a/b", 1 at "a/c")
        assert_eq!(plan.note_count, 4);
        // 3 distinct locations: "a", "a/b", "a/c"
        assert_eq!(plan.location_count, 3);

        // Verify nothing was actually deleted
        let all = notes_repo::fetch_all(&conn).unwrap();
        assert_eq!(all.len(), 5);
    }

    #[test]
    fn survey_note_context_subtree_and_ancestors() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);

        // Ancestors
        insert_note(&conn, root_id, "", "root note", 100);
        insert_note(&conn, root_id, "a", "parent note", 200);

        // Subtree (scope + descendants), inserted out of order
        insert_note(&conn, root_id, "a/b", "scope note", 300);
        insert_note(&conn, root_id, "a/b/c", "child", 500);
        insert_note(&conn, root_id, "a/b/d", "another child", 400);

        let result = survey_note_context(&conn, root_id, "a/b").unwrap();

        // Subtree notes: 3 (scope + 2 descendants), ordered by recency (DESC)
        assert_eq!(result.subtree_notes.len(), 3);
        assert_eq!(result.subtree_notes[0].text, "child"); // created_at=500
        assert_eq!(result.subtree_notes[1].text, "another child"); // created_at=400
        assert_eq!(result.subtree_notes[2].text, "scope note"); // created_at=300

        // Ancestor count: root note + parent note = 2
        assert_eq!(result.ancestor_count, 2);
    }

    /// Survey renders each of these notes relative to the scope, and that
    /// rendering panics on anything outside it. The guarantee it relies on is
    /// this one: the context holds descendants only, even when the scope's
    /// name carries characters a pattern match would treat as wildcards.
    #[test]
    fn survey_note_context_returns_only_descendants() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);

        insert_note(&conn, root_id, "alpha_beta", "in scope", 100);
        insert_note(&conn, root_id, "alpha_beta/inner", "descendant", 200);
        insert_note(&conn, root_id, "alphaXbeta", "one wildcard away", 300);
        insert_note(&conn, root_id, "alphaXbeta/inner", "under it", 350);
        insert_note(&conn, root_id, "alpha_betaX", "no separator", 400);

        let result = survey_note_context(&conn, root_id, "alpha_beta").unwrap();

        assert_eq!(result.subtree_notes.len(), 2);
        for note in &result.subtree_notes {
            assert!(
                note.rel_path == "alpha_beta" || note.rel_path.starts_with("alpha_beta/"),
                "context returned a non-descendant: {}",
                note.rel_path
            );
            // Would panic if the note were outside the scope.
            crate::notes::domain::relative_to_scope(&note.rel_path, "alpha_beta");
        }
    }

    #[test]
    fn list_global_temporal_reverses_to_oldest_first() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);

        insert_note(&conn, root_id, "a", "oldest", 100);
        insert_note(&conn, root_id, "b", "middle", 200);
        insert_note(&conn, root_id, "c", "newest", 300);

        let result = list_notes_global(&conn, Some(0)).unwrap();

        assert_eq!(result.notes.len(), 3);
        // Oldest first after reversal
        assert_eq!(result.notes[0].text, "oldest");
        assert_eq!(result.notes[1].text, "middle");
        assert_eq!(result.notes[2].text, "newest");
    }

    #[test]
    fn list_global_temporal_default_limit() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);

        for i in 1..=15 {
            insert_note(
                &conn,
                root_id,
                &format!("loc{}", i % 5),
                &format!("note {i}"),
                i * 100,
            );
        }

        let result = list_notes_global(&conn, None).unwrap(); // default limit = 10

        assert_eq!(result.notes.len(), 10);
        assert_eq!(result.total_note_count, 15);
        // Oldest first within the capped set
        assert!(result.notes[0].created_at < result.notes[9].created_at);
    }

    #[test]
    fn list_global_spatial_returns_location_entries() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);

        insert_note(&conn, root_id, "a", "first at a", 100);
        insert_note(&conn, root_id, "a", "second at a", 200);
        insert_note(&conn, root_id, "b", "at b", 150);

        let result = list_locations_global(&conn, Some(0)).unwrap();

        assert_eq!(result.locations.len(), 2);
        assert_eq!(result.total_location_count, 2);
        // Oldest first after reversal — b (150) before a (200)
        assert_eq!(result.locations[0].rel_path, "b");
        assert_eq!(result.locations[1].rel_path, "a");
        assert_eq!(result.locations[1].note_count, 2);
        assert_eq!(result.locations[1].latest_text, "second at a");
    }

    #[test]
    fn list_recursive_temporal_scoped() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);

        insert_note(&conn, root_id, "a", "in scope", 100);
        insert_note(&conn, root_id, "a/b", "child", 200);
        insert_note(&conn, root_id, "other", "outside", 300);

        let scope = NoteScope {
            root_id,
            root_path: "/photos".to_string(),
            rel_path: "a".to_string(),
        };

        let result = list_notes_recursive(&conn, &scope, Some(0)).unwrap();

        assert_eq!(result.notes.len(), 2);
        assert_eq!(result.total_note_count, 2);
        assert_eq!(result.total_location_count, 2);
        // Oldest first
        assert_eq!(result.notes[0].text, "in scope");
        assert_eq!(result.notes[1].text, "child");
    }

    // =========================================================================
    // execute_clear_exact tests
    // =========================================================================

    #[test]
    fn test_execute_clear_exact_returns_summary() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        insert_note(&conn, root_id, "a", "note 1", 100);
        insert_note(&conn, root_id, "a", "note 2", 200);

        let scope = NoteScope {
            root_id,
            root_path: "/photos".to_string(),
            rel_path: "a".to_string(),
        };

        let result = execute_clear_exact(&mut conn, &scope, None).unwrap();
        assert_eq!(result.deleted, 2);
        assert!(result.summary.contains("Cleared 2 notes"));
        assert!(result.summary.contains(" a")); // scope.display() returns rel_path
    }

    #[test]
    fn test_execute_clear_exact_zero_deleted() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);

        let scope = NoteScope {
            root_id,
            root_path: "/photos".to_string(),
            rel_path: "empty".to_string(),
        };

        let result = execute_clear_exact(&mut conn, &scope, None).unwrap();
        assert_eq!(result.deleted, 0);
        assert!(result.summary.contains("No notes at"));
    }

    #[test]
    fn test_clear_records_decision_row_without_receipt() {
        // Notes are annotations, not assets: clearing them is a decision-row
        // event only — receipts record content fates (per-item source state
        // transitions), and note clear is neither.
        let mut conn = setup_test_db();
        let dir = tempfile::tempdir().unwrap();
        let root_path = dir.path().to_str().unwrap().to_string();
        let root_id = insert_root(&conn, &root_path, "source", false);
        insert_note(&conn, root_id, "a", "looks like school photos 2008", 100);
        insert_note(&conn, root_id, "a/b", "come back for the Nikon set", 200);

        let scope = NoteScope {
            root_id,
            root_path: root_path.clone(),
            rel_path: "a".to_string(),
        };
        let decision = DecisionParams {
            command: crate::core::domain::decision::DecisionCommand::NoteClear,
            scope: vec![crate::core::domain::scope::DecisionScope::new(
                scope.root_id,
                scope.root_path.clone(),
                scope.rel_path.clone(),
            )],
            command_line: "canon note a --clear -r".to_string(),
            reason: None,
            record_enabled: true,
            receipt_enabled: true,
            ledger_config: crate::core::domain::config::LedgerConfig::default(),
        };

        let result = execute_clear_recursive(&mut conn, &scope, Some(&decision)).unwrap();
        assert_eq!(result.deleted, 2);
        assert!(
            result.warnings.is_empty(),
            "warnings: {:?}",
            result.warnings
        );

        // The disappearance is non-mute: a completed decision row with counts.
        let (command, status, completed, receipt_rel_path) = conn
            .query_row(
                "SELECT command, status, count_completed, receipt_rel_path FROM decisions",
                [],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, i64>(2)?,
                        r.get::<_, Option<String>>(3)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(command, "note_clear");
        assert_eq!(status, "completed");
        assert_eq!(completed, 2);
        // ...but no receipt: neither a DB pointer nor a ledger file.
        assert_eq!(receipt_rel_path, None);
        assert!(!dir.path().join(".canon-ledger").exists());
    }

    #[test]
    fn test_clear_zero_notes_records_nothing() {
        // A clear that touches nothing must not leave a decision row or a
        // dangling receipt.
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);

        let scope = NoteScope {
            root_id,
            root_path: "/photos".to_string(),
            rel_path: "empty".to_string(),
        };
        let decision = DecisionParams {
            command: crate::core::domain::decision::DecisionCommand::NoteClear,
            scope: vec![crate::core::domain::scope::DecisionScope::new(
                scope.root_id,
                scope.root_path.clone(),
                scope.rel_path.clone(),
            )],
            command_line: "canon note empty --clear".to_string(),
            reason: None,
            record_enabled: true,
            receipt_enabled: true,
            ledger_config: crate::core::domain::config::LedgerConfig::default(),
        };

        let result = execute_clear_exact(&mut conn, &scope, Some(&decision)).unwrap();
        assert_eq!(result.deleted, 0);
        let decisions: i64 = conn
            .query_row("SELECT COUNT(*) FROM decisions", [], |r| r.get(0))
            .unwrap();
        assert_eq!(decisions, 0, "empty clear must not record a decision");
    }

    #[test]
    fn note_scope_display() {
        let root = NoteScope {
            root_id: 1,
            root_path: "/photos".to_string(),
            rel_path: String::new(),
        };
        // Whole-root scope displays as the root path.
        assert_eq!(root.display(), "/photos");
        let sub = NoteScope {
            root_id: 1,
            root_path: "/photos".to_string(),
            rel_path: "italy/2016".to_string(),
        };
        // A sub-scope displays as the short rel path.
        assert_eq!(sub.display(), "italy/2016");
    }
}
