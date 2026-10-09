// ============================================================================
// Write operations (plan/execute)
// ============================================================================

use anyhow::Result;

use crate::core::domain::decision::DecisionStatus;
use crate::core::domain::format_count;
use crate::core::ops::decision::{
    DecisionCounts, DecisionParams, DecisionRecorder, UnwindTerminal,
};
use crate::core::repo::object::OrphanedStats;
use crate::core::repo::{Connection, Db};
use crate::facts::repo as facts_repo;

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

/// The sentence an excluded-fact prune that ended early records.
///
/// Held as a const so the pin asserts the same bytes the command writes, and
/// so no formatter can reflow it: at the default scope the repo runs two
/// DELETEs, sources first, and a failure between them is what the hedge and
/// the converge pointer are about.
const EXCLUDED_PRUNE_UNWIND: &str = "Excluded-fact pruning ended early; excluded-source facts may be gone while excluded-object facts stand — re-run to converge";

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
///
/// **The one site of the ten that settles through its own wrapper rather than
/// an armed recorder**, and the reason is a borrow, not a preference: the
/// delete needs `&mut Connection` (`delete_by_criteria` fills a temp table
/// through `populate_temp_sources`, which opens a transaction of its own),
/// while the guard would have to hold a shared one for the whole run. Narrowing
/// the helper instead would trade the compile-time refusal that keeps those
/// fills out of an outer transaction for a runtime error, across the tree.
///
/// The wrapper reaches every `?` in `delete_body` structurally, the same way
/// retirement's release and scan's closure do, and `a_failed_fact_delete_settles_interrupted`
/// is this file's declaring test. What is convention rather than construction
/// is the boundary: a statement added *here* rather than inside the body would
/// sit outside the settlement.
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

    match delete_body(conn, source_ids, key, entity_type, value_type, plan) {
        Ok(summary) => {
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
        Err(e) => {
            // One fact-changing statement in the window — the rest is
            // temp-table housekeeping — so the run either did the deletion or
            // did not, and `interrupted` is the word. The sentence says the
            // run ended early, never that the act failed.
            if let Some(recorder) = recorder.as_mut() {
                recorder.complete(
                    conn,
                    DecisionStatus::Interrupted,
                    DecisionCounts::none(),
                    &format!("Deletion of {key} facts ended early"),
                );
            }
            Err(e)
        }
    }
}

/// Everything `execute_delete`'s settlement brackets. Separate so that every
/// `?` it holds arrives at one `Err` arm rather than at a list someone keeps
/// current.
fn delete_body(
    conn: &mut Connection,
    source_ids: &[i64],
    key: &str,
    entity_type: &str,
    value_type: Option<&str>,
    plan: &DeletePlan,
) -> Result<String> {
    facts_repo::delete_by_criteria(conn, source_ids, key, entity_type, value_type)?;
    let entity_label = if entity_type == "source" {
        "sources"
    } else {
        "objects"
    };
    Ok(format!(
        "Deleted {} fact rows across {} {}",
        format_count(plan.fact_count),
        format_count(plan.entity_count),
        entity_label
    ))
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
    // Armed: one fact-changing statement, so the run either did it or did not.
    let mut recorder = decision.map(|d| {
        DecisionRecorder::start(conn, d, None).armed(
            conn,
            UnwindTerminal {
                status: DecisionStatus::Interrupted,
                summary: "Stale-fact pruning ended early".to_string(),
            },
        )
    });

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
    crate::core::repo::object::find_orphaned_stats(conn)
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
    // Armed on the shared connection, and **before** the transaction below:
    // locals drop in reverse order, so on a `?` the transaction rolls back
    // first and this settlement lands in autocommit. Armed the other way
    // round it would roll back with the transaction and the row would read
    // `started` over a run that reached a last act. This is the ordering rule
    // at its one production site that opens a transaction on the same
    // connection — `unchecked_transaction` is what lets the two share it.
    let conn = db.conn();
    let mut recorder = decision.map(|d| {
        DecisionRecorder::start(conn, d, None).armed(
            conn,
            UnwindTerminal {
                status: DecisionStatus::Interrupted,
                summary: "Orphan pruning ended early; nothing was removed".to_string(),
            },
        )
    });

    let tx = conn.unchecked_transaction()?;
    let deleted = crate::core::repo::object::delete_orphaned(&tx)?;
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
            conn,
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
    // Armed `partial`, not `interrupted`: at the default scope (`all`) the
    // repo runs two DELETEs, sources first, so a failure between them leaves
    // source facts gone and object facts standing.
    let mut recorder = decision.map(|d| {
        DecisionRecorder::start(conn, d, None).armed(
            conn,
            UnwindTerminal {
                status: DecisionStatus::Partial,
                // One line on purpose: rustfmt collapses a `\`-continued
                // literal and keeps its indentation, which silently changed
                // this sentence once already.
                summary: EXCLUDED_PRUNE_UNWIND.to_string(),
            },
        )
    });

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
    use crate::core::domain::time::zone::{ReadTimesOn, UserZone};
    use crate::core::testing::{insert_object, insert_root, insert_source, setup_test_db};

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

        crate::core::testing::insert_fact(&conn, s1, "content.Make", "Canon");
        crate::core::testing::insert_fact(&conn, s2, "content.Make", "Nikon");

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
            zone: ReadTimesOn::of_user(&UserZone::utc()),
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
        crate::core::testing::insert_fact(&conn, s1, "content.Make", "Canon");

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

        let mut db = crate::core::repo::Db::from_connection(conn);
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
        crate::core::testing::insert_fact(&conn, s1, "content.Make", "Canon");

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

    // =========================================================================
    // The drop settlement at this file's four sites (the subsystem's fifth
    // is `cli/import.rs`, which carries its own)
    //
    // Each plants a failure inside the window and reads the row afterwards.
    // Red-smoked by removing `.armed(..)` at the site — or, at `facts delete`,
    // by deleting the wrapper's `Err` arm: the row then reads `started`.
    // `summary` is read nullable so that smoke fails on the status assertion
    // rather than on a NULL column, where the message would say nothing.
    // =========================================================================

    fn settled_row(conn: &Connection) -> (String, Option<String>) {
        conn.query_row("SELECT status, summary FROM decisions", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap()
    }

    /// One fact-changing statement in the window, so the run either did the
    /// deletion or did not: `interrupted`, and the key names what was asked.
    #[test]
    fn a_failed_fact_delete_settles_interrupted() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "h1", false);
        let s1 = insert_source(&conn, root_id, "a.jpg", Some(obj));
        crate::core::testing::insert_fact(&conn, s1, "content.Make", "Canon");
        let plan = plan_delete(&mut conn, &[s1], "content.Make", "source", None).unwrap();
        // A `BEFORE DELETE` trigger over an empty result set never fires, so
        // the row the delete must match is asserted rather than assumed.
        assert_eq!(plan.fact_count, 1);
        let decision =
            recording_decision(crate::core::domain::decision::DecisionCommand::FactsDelete);
        crate::core::testing::plant_failure(&conn, "DELETE", "facts");

        assert!(execute_delete(
            &mut conn,
            &[s1],
            "content.Make",
            "source",
            None,
            &plan,
            Some(&decision)
        )
        .is_err());

        let (status, summary) = settled_row(&conn);
        assert_eq!(status, "interrupted");
        assert_eq!(
            summary.unwrap(),
            "Deletion of content.Make facts ended early"
        );
    }

    /// One statement again, so the same word.
    #[test]
    fn a_failed_stale_prune_settles_interrupted() {
        let conn = setup_test_db();
        // A stale fact must exist for the prune's DELETE to match a row: a
        // `BEFORE DELETE` trigger over an empty result set never fires, and
        // the statement would then succeed.
        let root_id = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "h1", false);
        let s1 = insert_source(&conn, root_id, "a.jpg", Some(obj));
        crate::core::testing::insert_fact(&conn, s1, "content.Make", "Canon");
        conn.execute("UPDATE sources SET basis_rev = 7 WHERE id = ?1", [s1])
            .unwrap();
        assert_eq!(plan_prune_stale(&conn).unwrap().stale_count, 1);

        let decision = recording_decision(crate::core::domain::decision::DecisionCommand::Prune);
        crate::core::testing::plant_failure(&conn, "DELETE", "facts");

        assert!(execute_prune_stale(&conn, Some(&decision)).is_err());

        let (status, summary) = settled_row(&conn);
        assert_eq!(status, "interrupted");
        assert_eq!(summary.unwrap(), "Stale-fact pruning ended early");
    }

    /// Two DELETEs at the default scope, sources first, so a failure between
    /// them leaves half the prune standing: `partial`, with a sentence that
    /// hedges and says how to converge. Both branches delete from `facts`, so
    /// the pin asserts the word and the sentence, never which half stood.
    #[test]
    fn a_failed_excluded_prune_settles_partial() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "h1", false);
        let s1 = insert_source(&conn, root_id, "a.jpg", Some(obj));
        conn.execute("UPDATE sources SET excluded = 1 WHERE id = ?1", [s1])
            .unwrap();
        crate::core::testing::insert_fact(&conn, s1, "content.Make", "Canon");
        // A `BEFORE DELETE` trigger over an empty result set never fires, so
        // the row the prune must match is asserted rather than assumed.
        assert_eq!(plan_prune_excluded(&conn, "all").unwrap().total_count(), 1);
        let decision = recording_decision(crate::core::domain::decision::DecisionCommand::Prune);
        crate::core::testing::plant_failure(&conn, "DELETE", "facts");

        assert!(execute_prune_excluded(&conn, "all", Some(&decision)).is_err());

        let (status, summary) = settled_row(&conn);
        assert_eq!(status, "partial");
        assert_eq!(
            summary.unwrap(),
            "Excluded-fact pruning ended early; excluded-source facts may be gone while \
             excluded-object facts stand — re-run to converge",
            "the chartered sentence, spelled here rather than read from the const, \
             so the pin cannot follow a change to it"
        );
    }

    /// **The ordering rule at its one production site.** The failure happens
    /// inside the prune's own transaction, so the transaction rolls back and
    /// the guard's settlement lands after it, in autocommit. Armed after the
    /// transaction instead, the settlement would roll back with it and the row
    /// would read `started` over a run that reached a last act.
    #[test]
    fn a_failed_orphan_prune_settles_after_its_rollback() {
        let conn = setup_test_db();
        insert_object(&conn, "h1", false);
        let mut db = crate::core::repo::Db::from_connection(conn);
        let decision = recording_decision(crate::core::domain::decision::DecisionCommand::Prune);
        crate::core::testing::plant_failure(db.conn(), "DELETE", "objects");

        assert!(execute_prune_orphaned(&mut db, Some(&decision)).is_err());

        let (status, summary) = settled_row(db.conn());
        assert_eq!(status, "interrupted");
        assert_eq!(
            summary.unwrap(),
            "Orphan pruning ended early; nothing was removed"
        );
        let objects: i64 = db
            .conn()
            .query_row("SELECT COUNT(*) FROM objects", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            objects, 1,
            "the transaction's own work must have rolled back"
        );
    }
}
