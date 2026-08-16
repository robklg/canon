// ============================================================================
// Write operations (plan/execute)
// ============================================================================

use anyhow::Result;

use crate::core::domain::decision::DecisionStatus;
use crate::core::domain::format_count;
use crate::facts::repo as facts_repo;
use crate::ops::decision::{DecisionCounts, DecisionParams, DecisionRecorder};
use crate::repo::object::OrphanedStats;
use crate::repo::{Connection, Db};

/// Plan result for fact deletion.
pub struct DeletePlan {
    /// Kept for plan self-containedness — the interface currently uses its own
    /// copies of key/entity_type for display and execute.
    #[allow(dead_code)]
    pub key: String,
    #[allow(dead_code)]
    pub entity_type: String,
    pub fact_count: i64,
    pub entity_count: i64,
}

/// Plan result for stale fact pruning.
pub struct PruneStalePlan {
    pub stale_count: i64,
}

/// Plan result for excluded fact pruning.
pub struct PruneExcludedPlan {
    #[allow(dead_code)]
    pub scope: String,
    pub source_fact_count: i64,
    pub object_fact_count: i64,
}

impl PruneExcludedPlan {
    pub fn total_count(&self) -> i64 {
        self.source_fact_count + self.object_fact_count
    }
}

/// Validate that a fact key is not protected from deletion.
/// Protected namespaces: `source.*`, `policy.*`.
pub fn validate_delete_key(key: &str) -> Result<()> {
    if key.starts_with("source.") || key.starts_with("policy.") {
        anyhow::bail!(
            "Cannot delete protected fact '{key}'. Facts in source.* and policy.* namespaces cannot be deleted."
        );
    }
    Ok(())
}

/// Plan fact deletion: count matching facts without performing the delete.
pub fn plan_delete(
    conn: &mut Connection,
    source_ids: &[i64],
    key: &str,
    entity_type: &str,
    value_type: Option<&str>,
) -> Result<DeletePlan> {
    let (fact_count, entity_count) =
        facts_repo::count_by_criteria(conn, source_ids, key, entity_type, value_type)?;

    Ok(DeletePlan {
        key: key.to_string(),
        entity_type: entity_type.to_string(),
        fact_count,
        entity_count,
    })
}

/// Result of fact deletion.
pub struct DeleteResult {
    pub summary: String,
}

/// Execute fact deletion.
///
/// Records unconditionally: every caller checks the plan first and returns
/// early on an empty one, so a decision row here always describes a real
/// deletion. A caller that skipped that check would record an empty act.
///
/// The summary and counts come from the plan, not from the delete's own
/// return — they describe what was counted, which is the same thing only as
/// long as nothing changed the facts in between.
pub fn execute_delete(
    conn: &mut Connection,
    source_ids: &[i64],
    key: &str,
    entity_type: &str,
    value_type: Option<&str>,
    plan: &DeletePlan,
    decision: Option<&DecisionParams>,
) -> Result<DeleteResult> {
    let mut recorder = decision.map(|d| DecisionRecorder::start(conn, d, None));

    facts_repo::delete_by_criteria(conn, source_ids, key, entity_type, value_type)?;
    let entity_label = if entity_type == "source" {
        "sources"
    } else {
        "objects"
    };
    let summary = format!(
        "Deleted {} fact rows across {} {}",
        format_count(plan.fact_count),
        format_count(plan.entity_count),
        entity_label
    );

    if let Some(recorder) = recorder.as_mut() {
        recorder.complete(
            conn,
            DecisionStatus::Completed,
            DecisionCounts {
                attempted: Some(plan.fact_count),
                completed: Some(plan.fact_count),
                failed: None,
                skipped: None,
            },
            &summary,
        );
    }

    Ok(DeleteResult { summary })
}

/// Plan stale fact pruning: count facts with mismatched basis_rev.
pub fn plan_prune_stale(conn: &Connection) -> Result<PruneStalePlan> {
    let stale_count = facts_repo::count_stale(conn)?;
    Ok(PruneStalePlan { stale_count })
}

/// Result of stale fact pruning.
#[allow(dead_code)]
pub struct PruneStaleResult {
    pub deleted: usize,
    pub summary: String,
}

/// Execute stale fact pruning.
///
/// Records unconditionally — the caller's empty-plan check is what keeps a
/// decision row from describing nothing.
pub fn execute_prune_stale(
    conn: &Connection,
    decision: Option<&DecisionParams>,
) -> Result<PruneStaleResult> {
    let mut recorder = decision.map(|d| DecisionRecorder::start(conn, d, None));

    let deleted = facts_repo::delete_stale(conn)?;
    let summary = format!(
        "Deleted {} stale fact rows (observed_basis_rev mismatch)",
        format_count(deleted as i64)
    );

    if let Some(recorder) = recorder.as_mut() {
        recorder.complete(
            conn,
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

    Ok(PruneStaleResult { deleted, summary })
}

/// Plan orphaned object pruning: count orphaned objects, sources, and facts.
pub fn plan_prune_orphaned(conn: &mut Connection) -> Result<OrphanedStats> {
    crate::repo::object::find_orphaned_stats(conn)
}

/// Result of orphaned object pruning.
#[allow(dead_code)]
pub struct PruneOrphanedResult {
    pub stats: OrphanedStats,
    pub summary: String,
}

/// Execute orphaned object pruning. Owns the transaction for atomicity.
///
/// Records unconditionally — the caller's empty-plan check is what keeps a
/// decision row from describing nothing.
pub fn execute_prune_orphaned(
    db: &mut Db,
    decision: Option<&DecisionParams>,
) -> Result<PruneOrphanedResult> {
    let mut recorder = decision.map(|d| DecisionRecorder::start(db.conn(), d, None));

    let conn = db.conn_mut();
    let tx = conn.transaction()?;
    let deleted = crate::repo::object::delete_orphaned(&tx)?;
    tx.commit()?;
    let summary = format!(
        "Deleted {} orphaned objects, {} non-present sources, and {} facts",
        format_count(deleted.object_count),
        format_count(deleted.source_count),
        format_count(deleted.total_fact_count())
    );

    if let Some(recorder) = recorder.as_mut() {
        let total_deleted =
            deleted.object_count + deleted.source_count + deleted.total_fact_count();
        recorder.complete(
            db.conn(),
            DecisionStatus::Completed,
            DecisionCounts {
                attempted: Some(total_deleted),
                completed: Some(total_deleted),
                failed: None,
                skipped: None,
            },
            &summary,
        );
    }

    Ok(PruneOrphanedResult {
        stats: deleted,
        summary,
    })
}

/// Validate the scope parameter for excluded fact pruning.
pub fn validate_prune_excluded_scope(scope: &str) -> Result<()> {
    if scope != "all" && scope != "source" && scope != "object" {
        anyhow::bail!("Invalid scope '{scope}'. Use 'source', 'object', or omit for both.");
    }
    Ok(())
}

/// Plan excluded fact pruning: count facts for excluded entities.
pub fn plan_prune_excluded(conn: &Connection, scope: &str) -> Result<PruneExcludedPlan> {
    let (source_fact_count, object_fact_count) = facts_repo::count_excluded(conn, scope)?;
    Ok(PruneExcludedPlan {
        scope: scope.to_string(),
        source_fact_count,
        object_fact_count,
    })
}

/// Result of excluded fact pruning.
#[allow(dead_code)]
pub struct PruneExcludedResult {
    pub source_deleted: usize,
    pub object_deleted: usize,
    pub summary: String,
}

/// Execute excluded fact pruning.
///
/// Records unconditionally — the caller's empty-plan check is what keeps a
/// decision row from describing nothing. Note that a delete finding nothing
/// composes an empty summary here, which is why the caller checks before
/// printing it.
pub fn execute_prune_excluded(
    conn: &Connection,
    scope: &str,
    decision: Option<&DecisionParams>,
) -> Result<PruneExcludedResult> {
    let mut recorder = decision.map(|d| DecisionRecorder::start(conn, d, None));

    let (source_deleted, object_deleted) = facts_repo::delete_excluded(conn, scope)?;
    let total_deleted = source_deleted + object_deleted;
    let mut parts = Vec::new();
    if source_deleted > 0 {
        parts.push(format!(
            "Deleted {} source facts (from excluded sources)",
            format_count(source_deleted as i64)
        ));
    }
    if object_deleted > 0 {
        parts.push(format!(
            "Deleted {} object facts (from excluded objects)",
            format_count(object_deleted as i64)
        ));
    }
    if total_deleted > 0 {
        parts.push(format!(
            "Total: {} facts deleted",
            format_count(total_deleted as i64)
        ));
    }
    let summary = parts.join("\n");

    if let Some(recorder) = recorder.as_mut() {
        recorder.complete(
            conn,
            DecisionStatus::Completed,
            DecisionCounts {
                attempted: Some(total_deleted as i64),
                completed: Some(total_deleted as i64),
                failed: None,
                skipped: None,
            },
            &summary,
        );
    }

    Ok(PruneExcludedResult {
        source_deleted,
        object_deleted,
        summary,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::test_helpers::{insert_object, insert_root, insert_source, setup_test_db};

    #[test]
    fn test_validate_delete_key_protected() {
        assert!(validate_delete_key("source.policy").is_err());
        assert!(validate_delete_key("source.anything").is_err());
        assert!(validate_delete_key("policy.reviewed").is_err());
        assert!(validate_delete_key("policy.archive").is_err());

        assert!(validate_delete_key("content.Make").is_ok());
        assert!(validate_delete_key("custom.field").is_ok());
        assert!(validate_delete_key("Make").is_ok());
    }

    #[test]
    fn test_plan_delete_counts() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let obj1 = insert_object(&conn, "h1", false);
        let obj2 = insert_object(&conn, "h2", false);
        let s1 = insert_source(&conn, root_id, "a.jpg", Some(obj1));
        let s2 = insert_source(&conn, root_id, "b.jpg", Some(obj2));

        crate::ops::test_helpers::insert_fact(&conn, s1, "content.Make", "Canon");
        crate::ops::test_helpers::insert_fact(&conn, s2, "content.Make", "Nikon");

        let plan = plan_delete(&mut conn, &[s1, s2], "content.Make", "source", None).unwrap();

        assert_eq!(plan.fact_count, 2);
        assert_eq!(plan.entity_count, 2);
        assert_eq!(plan.key, "content.Make");
        assert_eq!(plan.entity_type, "source");
    }

    #[test]
    fn test_plan_prune_stale_counts() {
        let conn = setup_test_db();
        // With no stale facts, count should be 0
        let plan = plan_prune_stale(&conn).unwrap();
        assert_eq!(plan.stale_count, 0);
    }

    #[test]
    fn test_validate_prune_excluded_scope() {
        assert!(validate_prune_excluded_scope("all").is_ok());
        assert!(validate_prune_excluded_scope("source").is_ok());
        assert!(validate_prune_excluded_scope("object").is_ok());
        assert!(validate_prune_excluded_scope("invalid").is_err());
        assert!(validate_prune_excluded_scope("").is_err());
    }

    // =========================================================================
    // Decision recording on the execute paths
    // =========================================================================

    /// A decision with recording on and receipts off — fact maintenance leaves
    /// a decision row and never a receipt (facts are the user's scaffolding,
    /// not content, so no fate is being recorded).
    fn recording_decision(
        command: crate::core::domain::decision::DecisionCommand,
    ) -> DecisionParams {
        DecisionParams {
            command,
            scope: Vec::new(),
            command_line: "canon facts".to_string(),
            reason: None,
            record_enabled: true,
            receipt_enabled: false,
            ledger_config: crate::core::domain::config::LedgerConfig::default(),
        }
    }

    /// The one decision row: command, status, attempted/completed, summary.
    fn only_decision(conn: &Connection) -> (String, String, i64, i64, String) {
        conn.query_row(
            "SELECT command, status, count_attempted, count_completed, summary FROM decisions",
            [],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, i64>(2)?,
                    r.get::<_, i64>(3)?,
                    r.get::<_, String>(4)?,
                ))
            },
        )
        .unwrap()
    }

    #[test]
    fn execute_delete_records_the_decision() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "h1", false);
        let s1 = insert_source(&conn, root_id, "a.jpg", Some(obj));
        crate::ops::test_helpers::insert_fact(&conn, s1, "content.Make", "Canon");

        let plan = plan_delete(&mut conn, &[s1], "content.Make", "source", None).unwrap();
        let decision =
            recording_decision(crate::core::domain::decision::DecisionCommand::FactsDelete);
        let result = execute_delete(
            &mut conn,
            &[s1],
            "content.Make",
            "source",
            None,
            &plan,
            Some(&decision),
        )
        .unwrap();

        let (command, status, attempted, completed, summary) = only_decision(&conn);
        assert_eq!(command, "facts_delete");
        assert_eq!(status, "completed");
        assert_eq!(attempted, 1);
        assert_eq!(completed, 1);
        assert_eq!(summary, result.summary);
        assert!(!summary.is_empty());
    }

    #[test]
    fn execute_prune_stale_records_the_decision() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "h1", false);
        let s1 = insert_source(&conn, root_id, "a.jpg", Some(obj));
        // A fact observed against an older basis than the source now carries.
        conn.execute(
            "INSERT INTO facts (entity_type, entity_id, key, value_text, observed_at,
                                observed_basis_rev)
             VALUES ('source', ?1, 'content.Make', 'Canon', 0, 1)",
            rusqlite::params![s1],
        )
        .unwrap();
        conn.execute("UPDATE sources SET basis_rev = 2 WHERE id = ?1", [s1])
            .unwrap();

        assert_eq!(plan_prune_stale(&conn).unwrap().stale_count, 1);

        let decision = recording_decision(crate::core::domain::decision::DecisionCommand::Prune);
        let result = execute_prune_stale(&conn, Some(&decision)).unwrap();

        let (command, status, attempted, completed, summary) = only_decision(&conn);
        assert_eq!(command, "prune");
        assert_eq!(status, "completed");
        assert_eq!(attempted, 1);
        assert_eq!(completed, 1);
        assert_eq!(summary, result.summary);
    }

    #[test]
    fn execute_prune_orphaned_records_the_decision() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "gone", false);
        let s1 = insert_source(&conn, root_id, "a.jpg", Some(obj));
        // The source is no longer present, so its object is orphaned.
        conn.execute("UPDATE sources SET present = 0 WHERE id = ?1", [s1])
            .unwrap();

        let mut db = crate::repo::Db::from_connection(conn);
        let decision = recording_decision(crate::core::domain::decision::DecisionCommand::Prune);
        let result = execute_prune_orphaned(&mut db, Some(&decision)).unwrap();
        assert_eq!(result.stats.object_count, 1);

        let (command, status, attempted, completed, summary) = only_decision(db.conn());
        assert_eq!(command, "prune");
        assert_eq!(status, "completed");
        assert_eq!(attempted, completed);
        assert_eq!(summary, result.summary);
    }

    #[test]
    fn execute_prune_excluded_records_the_decision() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "h1", false);
        let s1 = insert_source(&conn, root_id, "a.jpg", Some(obj));
        conn.execute("UPDATE sources SET excluded = 1 WHERE id = ?1", [s1])
            .unwrap();
        crate::ops::test_helpers::insert_fact(&conn, s1, "content.Make", "Canon");

        assert_eq!(plan_prune_excluded(&conn, "all").unwrap().total_count(), 1);

        let decision = recording_decision(crate::core::domain::decision::DecisionCommand::Prune);
        let result = execute_prune_excluded(&conn, "all", Some(&decision)).unwrap();

        let (command, status, attempted, completed, summary) = only_decision(&conn);
        assert_eq!(command, "prune");
        assert_eq!(status, "completed");
        assert_eq!(attempted, 1);
        assert_eq!(completed, 1);
        assert_eq!(summary, result.summary);
        assert!(!summary.is_empty());
    }
}
