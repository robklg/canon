// ============================================================================
// Write operations (plan/execute)
// ============================================================================

use anyhow::Result;

use crate::core::domain::decision::DecisionStatus;
use crate::core::domain::fact::FactType;
use crate::core::domain::format_count;
use crate::core::domain::object::Holders;
use crate::core::ops::decision::{
    DecisionCounts, DecisionParams, DecisionRecorder, UnwindTerminal,
};
use crate::core::repo::decision::set_fact_key;
use crate::core::repo::fact::key_types;
use crate::core::repo::object::OrphanedStats;
use crate::core::repo::{Connection, Db};
use crate::expr::printed_key;
use crate::expr::{select_sources, NoteUnit, SelectionNotes, SelectionParams};
use crate::facts::domain::{counted, plural, FactSide};
use crate::facts::repo as facts_repo;

/// Plan result for fact deletion: what the delete acts on, and everything its lines say.
pub struct DeletePlan {
    /// The key counted, and the one `execute_delete` deletes: one key for both,
    /// so the count and the delete cannot diverge.
    pub key: String,
    pub side: FactSide,
    pub value_type: Option<FactType>,
    pub fact_count: i64,
    pub entity_count: i64,
    /// The matching sources: what the selection chose, and what the delete runs over.
    pub source_ids: Vec<i64>,
    /// Facts of the key on the other side, over the same matching sources and type.
    pub other_side_count: i64,
    /// `--on object` with facts to delete: the present files holding the content whose fact is
    /// deleted, every copy, archive copies and copies on suspended roots included.
    pub reach: Option<Holders>,
    /// The plan's line about its selection: of the matching sources, how many have no value
    /// for a negated key; and which stored times a comparison could not place.
    pub notes: SelectionNotes,
}

impl DeletePlan {
    /// The line for a selection that matched no source.
    pub fn no_sources_line(&self) -> &'static str {
        "No sources match the given filters."
    }

    /// The line for matching sources holding no fact of the key on this side, with the facts of
    /// the key on the other side where it holds any.
    pub fn none_line(&self) -> String {
        let mut line = format!(
            "No {} facts{} on matching {}",
            printed_key(&self.key),
            self.type_words(),
            self.side.noun(2)
        );
        if self.other_side_count > 0 {
            line.push_str(&format!(
                "; {} on {} ({})",
                format_count(self.other_side_count),
                self.side.other().noun(2),
                self.side.other().flag()
            ));
        }
        line
    }

    /// The preview's line: what a deletion would remove, and how far it reaches.
    pub fn preview_line(&self) -> String {
        format!("Would delete {}", self.tail())
    }

    /// The summary's first line — the decision record's text, printed after `--yes`.
    pub fn summary_head(&self) -> String {
        format!("Deleted {}", self.tail())
    }

    /// The summary a deletion that ended early records.
    pub fn ended_early(&self) -> String {
        format!("Deletion of {} facts ended early", printed_key(&self.key))
    }

    /// ` of type number` where `--value-type` named a type, in the words a type is named by
    /// everywhere. A phrase inside a sentence about an act, so not a statement of the grammar.
    fn type_words(&self) -> String {
        self.value_type
            .map(|t| format!(" of type {t}"))
            .unwrap_or_default()
    }

    /// What the preview and the summary both say after their first word: spoken once, so the
    /// summary states the reach the preview showed.
    fn tail(&self) -> String {
        let facts = self.fact_count as u64;
        let mut line = format!(
            "{} {} {}{} across {} {}",
            format_count(self.fact_count),
            printed_key(&self.key),
            plural(facts, "fact", "facts"),
            self.type_words(),
            format_count(self.entity_count),
            self.side.noun(self.entity_count as u64)
        );
        if let Some(reach) = &self.reach {
            line.push_str(&reach_clauses(reach));
        }
        if self.other_side_count > 0 {
            line.push_str(&format!(
                "; {} on {} not deleted ({})",
                format_count(self.other_side_count),
                self.side.other().noun(2),
                self.side.other().flag()
            ));
        }
        line
    }
}

/// How far a deletion of facts stored on content reaches: `, held by 4 sources (3 in source
/// roots, 1 in archives; 1 of them on suspended roots)`, a zero part left out; then the fact on
/// empty files' content, shown by every empty file holding it.
fn reach_clauses(reach: &Holders) -> String {
    let mut line = String::new();
    let held = reach.total() as u64;
    if held > 0 {
        let parts: Vec<String> = [
            (reach.in_source_roots, "in source roots"),
            (reach.in_archives, "in archives"),
        ]
        .into_iter()
        .filter(|&(n, _)| n > 0)
        .map(|(n, words)| format!("{} {words}", format_count(n as i64)))
        .collect();
        let suspended = if reach.on_suspended_roots > 0 {
            format!(
                "; {} of them on suspended roots",
                format_count(reach.on_suspended_roots as i64)
            )
        } else {
            String::new()
        };
        line.push_str(&format!(
            ", held by {} ({}{suspended})",
            counted(held, "source", "sources"),
            parts.join(", ")
        ));
    }
    if reach.empty_files > 0 {
        let lead = if held > 0 { "; also" } else { "," };
        line.push_str(&format!(
            "{lead} the fact on empty files' content, shown by {}",
            counted(reach.empty_files as u64, "empty file", "empty files")
        ));
    }
    line
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
const EXCLUDED_PRUNE_UNWIND: &str = "Excluded-fact pruning ended early; facts stored on excluded sources may be gone while facts stored on excluded content remain — re-run to converge";

/// Plan result for excluded fact pruning.
pub struct PruneExcludedPlan {
    #[allow(dead_code)]
    pub scope: String,
    pub stored_on_source_count: i64,
    pub stored_on_content_count: i64,
}

impl PruneExcludedPlan {
    pub fn total_count(&self) -> i64 {
        self.stored_on_source_count + self.stored_on_content_count
    }
}

/// Plan fact deletion: select the matching sources and count their facts, without
/// performing the delete. `key` is the one `facts::domain::delete_key` read. The selection is
/// the one `ls --where` at this scope would list, so a delete never reaches a source no
/// preview of it showed.
pub fn plan_delete(
    conn: &mut Connection,
    selection: &SelectionParams,
    key: &str,
    side: FactSide,
    value_type: Option<FactType>,
) -> Result<DeletePlan> {
    let selection = select_sources(conn, selection)?;
    let source_ids = selection.source_ids();
    let mut plan = plan_delete_over(conn, source_ids, key, side, value_type)?;
    // A plan that deletes nothing acts on no source, so it counts none as acted on.
    let acted: &[i64] = if plan.fact_count == 0 {
        &[]
    } else {
        &plan.source_ids
    };
    plan.notes = SelectionNotes::of(
        selection.marks.as_ref(),
        acted,
        NoteUnit::OfMatching(plan.source_ids.len()),
    );
    Ok(plan)
}

/// The counting half of [`plan_delete`], over sources already chosen.
fn plan_delete_over(
    conn: &mut Connection,
    source_ids: Vec<i64>,
    key: &str,
    side: FactSide,
    value_type: Option<FactType>,
) -> Result<DeletePlan> {
    let count = |conn: &mut Connection, side: FactSide| {
        facts_repo::count_by_criteria(conn, &source_ids, key, side.as_stored(), value_type)
    };
    let (other_side_count, _) = count(conn, side.other())?;
    // On the object side the facts counted, the objects counted and the objects whose holders
    // are counted are one read, so no two numbers on the line can disagree.
    let (fact_count, entity_count, reach) = match side {
        FactSide::Object => {
            let objects = facts_repo::object_ids_with_fact(conn, &source_ids, key, value_type)?;
            let n = objects.len() as i64;
            let reach = if objects.is_empty() {
                None
            } else {
                let held = crate::core::repo::source::fetch_sources_by_object_ids(conn, &objects)?;
                Some(Holders::of(held.values().flatten()))
            };
            (n, n, reach)
        }
        FactSide::Source => {
            let (facts, sources) = count(conn, side)?;
            (facts, sources, None)
        }
    };

    Ok(DeletePlan {
        key: key.to_string(),
        side,
        value_type,
        fact_count,
        entity_count,
        source_ids,
        other_side_count,
        reach,
        notes: SelectionNotes::none(NoteUnit::OfMatching(0)),
    })
}

/// Result of fact deletion.
pub struct DeleteResult {
    pub summary: String,
}

/// Execute fact deletion of the plan's key.
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
    plan: &DeletePlan,
    decision: Option<&DecisionParams>,
) -> Result<DeleteResult> {
    let mut recorder = decision.map(|d| DecisionRecorder::start(conn, d, None));

    let deleted = delete_body(conn, plan);
    // The row names the key and what it holds now, whichever way the delete went, so a key a
    // conversion emptied earlier is never answered as that conversion's once this ran.
    if let Some(recorder) = recorder.as_mut() {
        record_fact_key(conn, recorder, &plan.key);
    }
    match deleted {
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
                    &plan.ended_early(),
                );
            }
            Err(e)
        }
    }
}

/// Record on the decision row the key it acted on and the facts that key holds now — read from
/// the key's row set. A failure is a warning on the recorder, never the act's.
pub(super) fn record_fact_key(conn: &Connection, recorder: &mut DecisionRecorder, key: &str) {
    let Some(id) = recorder.decision_id() else {
        return;
    };
    let recorded = key_types(conn, &[key]).and_then(|types| {
        let remaining = types
            .get(key)
            .map_or(0, |t| t.counts().iter().map(|&(_, n)| n).sum());
        set_fact_key(conn, id, key, remaining)
    });
    if let Err(e) = recorded {
        recorder.push_warning(format!(
            "Warning: failed to record the key on the decision: {e}"
        ));
    }
}

/// Everything `execute_delete`'s settlement brackets. Separate so that every
/// `?` it holds arrives at one `Err` arm rather than at a list someone keeps
/// current.
fn delete_body(conn: &mut Connection, plan: &DeletePlan) -> Result<String> {
    facts_repo::delete_by_criteria(
        conn,
        &plan.source_ids,
        &plan.key,
        plan.side.as_stored(),
        plan.value_type,
    )?;
    Ok(plan.notes.under(&plan.summary_head()))
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
        "Deleted {} (observed_basis_rev mismatch)",
        counted(deleted as u64, "stale fact", "stale facts")
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
    let (stored_on_source_count, stored_on_content_count) =
        facts_repo::count_excluded(conn, scope)?;
    Ok(PruneExcludedPlan {
        scope: scope.to_string(),
        stored_on_source_count,
        stored_on_content_count,
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
    // the facts stored on sources gone and those stored on content in place.
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
            "Deleted {} facts stored on excluded sources",
            format_count(source_deleted as i64)
        ));
    }
    if object_deleted > 0 {
        parts.push(format!(
            "Deleted {} facts stored on excluded content",
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
    use crate::expr::{ExpressionText, RolePolicy};

    /// The plan selects the population `ls --where` would list, runs the delete over it, and says
    /// how many of the matching sources have no value for a negated key — including the key
    /// deleted, whose sources with no value lose nothing.
    #[test]
    fn plan_delete_selects_and_states_its_matching_count() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let canon = insert_source(&conn, root_id, "a.jpg", None);
        let nikon = insert_source(&conn, root_id, "b.jpg", None);
        let bare = insert_source(&conn, root_id, "c.jpg", None);
        crate::core::testing::insert_fact(&conn, canon, "content.Make", "Canon");
        crate::core::testing::insert_fact(&conn, nikon, "content.Make", "Nikon");
        let filters =
            crate::expr::read_filters(&conn, &ExpressionText::plain(&["Make != Canon"])).unwrap();
        let params = SelectionParams {
            scopes: vec![],
            include: crate::core::domain::include::IncludeSet::default(),
            filters,
            role_policy: RolePolicy::SourceUnlessIncluded,
            clock: UserZone::utc(),
            notes: true,
        };
        let plan = plan_delete(&mut conn, &params, "content.Make", FactSide::Source, None).unwrap();
        assert_eq!(plan.source_ids, vec![nikon, bare]);
        assert_eq!(plan.fact_count, 1);
        assert_eq!(
            plan.notes.lines(),
            ["  from 2 matching sources, 1 of which has no Make"]
        );

        let decision =
            recording_decision(crate::core::domain::decision::DecisionCommand::FactsDelete);
        let result = execute_delete(&mut conn, &plan, Some(&decision)).unwrap();
        assert_eq!(
            result.summary,
            "Deleted 1 content.Make fact across 1 source\n  from 2 matching sources, 1 of which has no Make"
        );
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

        let plan = plan_delete_over(
            &mut conn,
            vec![s1, s2],
            "content.Make",
            FactSide::Source,
            None,
        )
        .unwrap();

        assert_eq!(plan.fact_count, 2);
        assert_eq!(plan.entity_count, 2);
        assert_eq!(plan.key, "content.Make");
        assert_eq!(plan.side, FactSide::Source);
    }

    /// A key copied from Canon's output, in its quotes, previews the same facts as the bare
    /// name, on sources and on content alike.
    #[test]
    fn a_key_in_canons_quotes_previews_the_facts_of_the_bare_name() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let obj1 = insert_object(&conn, "h1", false);
        let obj2 = insert_object(&conn, "h2", false);
        let s1 = insert_source(&conn, root_id, "a.jpg", Some(obj1));
        let s2 = insert_source(&conn, root_id, "b.jpg", Some(obj2));
        let s3 = insert_source(&conn, root_id, "c.jpg", None);
        crate::core::testing::insert_fact(&conn, s3, "content.File Size", "10");
        crate::core::testing::insert_object_fact(&conn, obj1, "content.File Size", "20");
        crate::core::testing::insert_object_fact(&conn, obj2, "content.File Size", "30");
        for (entity, facts) in [(FactSide::Source, 1), (FactSide::Object, 2)] {
            let counts: Vec<(i64, i64)> = ["content.File Size", r#""content.File Size""#]
                .into_iter()
                .map(|name| {
                    let key = crate::facts::domain::delete_key(name).unwrap();
                    let plan =
                        plan_delete_over(&mut conn, vec![s1, s2, s3], &key, entity, None).unwrap();
                    (plan.fact_count, plan.entity_count)
                })
                .collect();
            assert_eq!(counts, [(facts, facts), (facts, facts)], "{entity:?}");
        }
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
    // The lines a deletion prints, composed on the plan
    // =========================================================================

    /// A library for the lines: `content.Make` on two objects — one held by a source-root copy,
    /// an archive copy and a copy on a suspended source root, one by a source-root copy alone —
    /// and on one source of its own. Returns the matching sources: the two source-root copies
    /// and the source with its own fact.
    fn held_library(conn: &Connection) -> Vec<i64> {
        let open = insert_root(conn, "/photos", "source", false);
        let parked = insert_root(conn, "/old", "source", true);
        let archive = insert_root(conn, "/archive", "archive", false);
        let wide = insert_object(conn, "h1", false);
        let one = insert_object(conn, "h2", false);
        let a = insert_source(conn, open, "a.jpg", Some(wide));
        insert_source(conn, archive, "a.jpg", Some(wide));
        insert_source(conn, parked, "a.jpg", Some(wide));
        let b = insert_source(conn, open, "b.jpg", Some(one));
        let c = insert_source(conn, open, "c.jpg", None);
        crate::core::testing::insert_object_fact(conn, wide, "content.Make", "Canon");
        crate::core::testing::insert_object_fact(conn, one, "content.Make", "Nikon");
        crate::core::testing::insert_fact(conn, c, "content.Make", "Sony");
        vec![a, b, c]
    }

    /// **The preview and the summary share one tail**: the line after its first word is the
    /// same, reach, empty files and other side included, so the record states what the preview
    /// showed.
    #[test]
    fn the_preview_and_the_summary_share_one_tail() {
        let mut conn = setup_test_db();
        let mut matching = held_library(&conn);
        let root = insert_root(&conn, "/empty", "source", false);
        let empty = insert_object(&conn, "e", false);
        matching.push(crate::core::testing::insert_source_with_size(
            &conn,
            root,
            "e.txt",
            Some(empty),
            0,
        ));
        crate::core::testing::insert_object_fact(&conn, empty, "content.Make", "none");
        let plan =
            plan_delete_over(&mut conn, matching, "content.Make", FactSide::Object, None).unwrap();
        let preview = plan.preview_line();
        let summary = plan.summary_head();
        assert_eq!(
            preview.strip_prefix("Would delete "),
            summary.strip_prefix("Deleted ")
        );
        assert_eq!(
            summary,
            "Deleted 3 content.Make facts across 3 objects, held by 4 sources (3 in source \
             roots, 1 in archives; 1 of them on suspended roots); also the fact on empty files' \
             content, shown by 1 empty file; 1 on sources not deleted (--on source)"
        );
        // The decision row keeps the same line: what `canon trail` shows after `--yes`.
        let decision =
            recording_decision(crate::core::domain::decision::DecisionCommand::FactsDelete);
        let result = execute_delete(&mut conn, &plan, Some(&decision)).unwrap();
        assert_eq!(result.summary, summary);
        assert_eq!(only_decision(&conn).4, summary);
    }

    /// **Every line names the key as Canon prints it**, so it reads back as the key: a key with
    /// a space stands in Canon's quotes in the none line, the preview, the summary and the
    /// sentence of a deletion that ended early.
    #[test]
    fn every_line_names_the_key_as_printed() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let s = insert_source(&conn, root, "a.jpg", None);
        crate::core::testing::insert_fact(&conn, s, "content.File Size", "10");
        let key = "content.File Size";
        let printed = r#""content.File Size""#;
        let some = plan_delete_over(&mut conn, vec![s], key, FactSide::Source, None).unwrap();
        let none = plan_delete_over(&mut conn, vec![s], key, FactSide::Object, None).unwrap();
        assert_eq!(
            none.none_line(),
            format!("No {printed} facts on matching objects; 1 on sources (--on source)")
        );
        assert_eq!(
            some.preview_line(),
            format!("Would delete 1 {printed} fact across 1 source")
        );
        assert_eq!(
            some.summary_head(),
            format!("Deleted 1 {printed} fact across 1 source")
        );
        assert_eq!(
            some.ended_early(),
            format!("Deletion of {printed} facts ended early")
        );
    }

    /// **The reach states every copy** of the content whose fact is deleted: by role, with the
    /// suspended share only where there is one, and a role holding none left out.
    #[test]
    fn the_reach_states_every_copy() {
        let mut conn = setup_test_db();
        let matching = held_library(&conn);
        let plan = plan_delete_over(
            &mut conn,
            matching[..1].to_vec(),
            "content.Make",
            FactSide::Object,
            None,
        )
        .unwrap();
        assert_eq!(
            plan.preview_line(),
            "Would delete 1 content.Make fact across 1 object, held by 3 sources (2 in source \
             roots, 1 in archives; 1 of them on suspended roots)"
        );

        let no_parked = setup_test_db();
        let open = insert_root(&no_parked, "/photos", "source", false);
        let archive = insert_root(&no_parked, "/archive", "archive", false);
        let obj = insert_object(&no_parked, "h1", false);
        let a = insert_source(&no_parked, open, "a.jpg", Some(obj));
        insert_source(&no_parked, archive, "a.jpg", Some(obj));
        crate::core::testing::insert_object_fact(&no_parked, obj, "content.Make", "Canon");
        let mut conn = no_parked;
        let plan =
            plan_delete_over(&mut conn, vec![a], "content.Make", FactSide::Object, None).unwrap();
        assert!(
            plan.preview_line()
                .ends_with("held by 2 sources (1 in source roots, 1 in archives)"),
            "{}",
            plan.preview_line()
        );

        let only_sources = setup_test_db();
        let open = insert_root(&only_sources, "/photos", "source", false);
        let obj = insert_object(&only_sources, "h1", false);
        let a = insert_source(&only_sources, open, "a.jpg", Some(obj));
        insert_source(&only_sources, open, "copy.jpg", Some(obj));
        crate::core::testing::insert_object_fact(&only_sources, obj, "content.Make", "Canon");
        let mut conn = only_sources;
        let plan =
            plan_delete_over(&mut conn, vec![a], "content.Make", FactSide::Object, None).unwrap();
        assert!(
            plan.preview_line()
                .ends_with("held by 2 sources (2 in source roots)"),
            "{}",
            plan.preview_line()
        );
        // A deletion of facts stored on sources states no reach.
        let plan =
            plan_delete_over(&mut conn, vec![a], "content.Make", FactSide::Source, None).unwrap();
        assert_eq!(plan.reach, None);
    }

    /// **A deletion through empty files' content states the empty files**: they hold no content
    /// to be counted as holders, but every one of them shows the fact stored on the empty content.
    #[test]
    fn a_deletion_through_empty_files_content_states_the_empty_files() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let empty = insert_object(&conn, "e", false);
        let full = insert_object(&conn, "h1", false);
        let empties: Vec<i64> = ["a.txt", "b.txt", "c.txt"]
            .into_iter()
            .map(|name| {
                crate::core::testing::insert_source_with_size(&conn, root, name, Some(empty), 0)
            })
            .collect();
        let held = insert_source(&conn, root, "d.jpg", Some(full));
        crate::core::testing::insert_object_fact(&conn, empty, "content.Make", "none");
        crate::core::testing::insert_object_fact(&conn, full, "content.Make", "Canon");

        let plan = plan_delete_over(
            &mut conn,
            empties.clone(),
            "content.Make",
            FactSide::Object,
            None,
        )
        .unwrap();
        assert_eq!(
            plan.preview_line(),
            "Would delete 1 content.Make fact across 1 object, the fact on empty files' content, \
             shown by 3 empty files"
        );
        let beside: Vec<i64> = empties.into_iter().chain([held]).collect();
        let plan =
            plan_delete_over(&mut conn, beside, "content.Make", FactSide::Object, None).unwrap();
        assert_eq!(
            plan.preview_line(),
            "Would delete 2 content.Make facts across 2 objects, held by 1 source (1 in source \
             roots); also the fact on empty files' content, shown by 3 empty files"
        );
    }

    /// **The other side is named**: matching sources with no fact on the side asked about are
    /// told how many the other side holds and the flag that reaches them; a deletion leaving
    /// facts of the key on the other side says so, in the preview and the summary alike.
    #[test]
    fn the_other_side_is_named() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let mut on_objects = Vec::new();
        for n in 0..4 {
            let obj = insert_object(&conn, &format!("h{n}"), false);
            on_objects.push(insert_source(&conn, root, &format!("{n}.jpg"), Some(obj)));
            crate::core::testing::insert_object_fact(&conn, obj, "content.Make", "Canon");
        }
        let plan = plan_delete_over(
            &mut conn,
            on_objects.clone(),
            "content.Make",
            FactSide::Source,
            None,
        )
        .unwrap();
        assert_eq!(
            plan.none_line(),
            "No content.Make facts on matching sources; 4 on objects (--on object)"
        );

        let mut both = on_objects;
        for n in 0..3 {
            let s = insert_source(&conn, root, &format!("own{n}.jpg"), None);
            crate::core::testing::insert_fact(&conn, s, "content.Make", "Nikon");
            both.push(s);
        }
        let plan =
            plan_delete_over(&mut conn, both, "content.Make", FactSide::Object, None).unwrap();
        for line in [plan.preview_line(), plan.summary_head()] {
            assert!(
                line.ends_with("; 3 on sources not deleted (--on source)"),
                "{line}"
            );
        }
    }

    /// **The type is named beside the key** where `--value-type` narrowed the deletion.
    #[test]
    fn the_type_is_named_beside_the_key() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let s = insert_source(&conn, root, "a.jpg", None);
        crate::core::testing::insert_number_fact(&conn, s, "content.Make", 5.0);
        let plan = plan_delete_over(
            &mut conn,
            vec![s],
            "content.Make",
            FactSide::Source,
            Some(FactType::Num),
        )
        .unwrap();
        assert_eq!(
            plan.preview_line(),
            "Would delete 1 content.Make fact of type number across 1 source"
        );
        let plan = plan_delete_over(
            &mut conn,
            vec![s],
            "content.Make",
            FactSide::Source,
            Some(FactType::Text),
        )
        .unwrap();
        assert_eq!(
            plan.none_line(),
            "No content.Make facts of type text on matching sources"
        );
    }

    /// **A key from the key list reaches what the list counted**: pasted as printed, bare or in
    /// Canon's quotes, the sources a deletion reaches across `--on source` and `--on object` —
    /// counted once — are the sources the key list counts for it.
    #[test]
    fn a_key_from_the_key_list_reaches_what_the_list_counted() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "h1", false);
        let both = insert_source(&conn, root, "both.jpg", Some(obj));
        let on_content = insert_source(&conn, root, "content.jpg", Some(obj));
        let own = insert_source(&conn, root, "own.jpg", None);
        let bare = insert_source(&conn, root, "bare.jpg", None);
        crate::core::testing::insert_object_fact(&conn, obj, "content.File Size", "20");
        crate::core::testing::insert_fact(&conn, both, "content.File Size", "10");
        crate::core::testing::insert_fact(&conn, own, "content.File Size", "30");
        let matching = vec![both, on_content, own, bare];

        let listed = crate::facts::ops::report::compute_all_keys(&mut conn, &matching, false)
            .unwrap()
            .keys
            .into_iter()
            .find(|k| k.key == "content.File Size")
            .expect("listed")
            .count;
        for given in [r#""content.File Size""#, "'content.File Size'"] {
            let key = crate::facts::domain::delete_key(given).unwrap();
            let on_sources: Vec<i64> = matching
                .iter()
                .copied()
                .filter(|&s| {
                    plan_delete_over(&mut conn, vec![s], &key, FactSide::Source, None)
                        .unwrap()
                        .fact_count
                        > 0
                })
                .collect();
            let objects =
                facts_repo::object_ids_with_fact(&mut conn, &matching, &key, None).unwrap();
            let reached: std::collections::BTreeSet<i64> =
                crate::core::repo::source::batch_fetch_by_ids(&conn, &matching)
                    .unwrap()
                    .into_values()
                    .filter(|s| s.object_id.is_some_and(|o| objects.contains(&o)))
                    .map(|s| s.id)
                    .chain(on_sources)
                    .collect();
            assert_eq!(reached.len() as i64, listed, "{given}");
        }
    }

    /// **The object read holds over many objects**: past the batching size, every object with
    /// the fact is returned, and the holders are every copy.
    #[test]
    fn object_ids_with_fact_reads_every_object_past_the_batch_size() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let archive = insert_root(&conn, "/archive", "archive", false);
        let mut matching = Vec::new();
        for n in 0..1_500 {
            let obj = insert_object(&conn, &format!("h{n}"), false);
            matching.push(insert_source(&conn, root, &format!("{n}.jpg"), Some(obj)));
            insert_source(&conn, archive, &format!("{n}.jpg"), Some(obj));
            crate::core::testing::insert_object_fact(&conn, obj, "content.Make", "Canon");
        }
        let objects =
            facts_repo::object_ids_with_fact(&mut conn, &matching, "content.Make", None).unwrap();
        assert_eq!(objects.len(), 1_500);
        let plan =
            plan_delete_over(&mut conn, matching, "content.Make", FactSide::Object, None).unwrap();
        assert_eq!(plan.entity_count, 1_500);
        assert_eq!(
            plan.reach,
            Some(Holders {
                in_source_roots: 1_500,
                in_archives: 1_500,
                on_suspended_roots: 0,
                empty_files: 0,
            })
        );
        assert!(plan.preview_line().contains("held by 3,000 sources"));
    }

    /// **An object-side deletion removes exactly what its plan counted**, narrowed by type or
    /// not: the facts counted, the objects counted and the holders come from the read the
    /// delete's own rows answer to.
    #[test]
    fn an_object_side_deletion_removes_exactly_what_its_plan_counted() {
        let object_facts = |conn: &Connection, key: &str| -> i64 {
            conn.query_row(
                "SELECT COUNT(*) FROM facts WHERE entity_type = 'object' AND key = ?1",
                [key],
                |row| row.get(0),
            )
            .unwrap()
        };
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/media", "source", false);
        let mut matching = Vec::new();
        for (n, number) in [(0, Some(5.0)), (1, None), (2, Some(7.0))] {
            let obj = insert_object(&conn, &format!("h{n}"), false);
            matching.push(insert_source(&conn, root, &format!("{n}.mp4"), Some(obj)));
            match number {
                Some(v) => {
                    conn.execute(
                        "INSERT INTO facts (entity_type, entity_id, key, value_num, observed_at)
                         VALUES ('object', ?1, 'content.Len', ?2, 0)",
                        rusqlite::params![obj, v],
                    )
                    .unwrap();
                }
                None => crate::core::testing::insert_object_fact(&conn, obj, "content.Len", "long"),
            }
        }

        let all = plan_delete_over(
            &mut conn,
            matching.clone(),
            "content.Len",
            FactSide::Object,
            None,
        )
        .unwrap();
        assert_eq!((all.fact_count, all.entity_count), (3, 3));

        let numbers = plan_delete_over(
            &mut conn,
            matching,
            "content.Len",
            FactSide::Object,
            Some(FactType::Num),
        )
        .unwrap();
        assert_eq!((numbers.fact_count, numbers.entity_count), (2, 2));
        assert_eq!(numbers.reach.map(|r| r.total()), Some(2));
        let before = object_facts(&conn, "content.Len");
        execute_delete(&mut conn, &numbers, None).unwrap();
        assert_eq!(
            before - object_facts(&conn, "content.Len"),
            numbers.fact_count,
            "the delete removes the facts the plan counted"
        );
        assert_eq!(object_facts(&conn, "content.Len"), 1, "the text fact stays");
    }

    /// **The stale prune says facts**, in its summary as in its preview.
    #[test]
    fn prune_stale_says_facts() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let s1 = insert_source(&conn, root_id, "a.jpg", None);
        crate::core::testing::insert_fact(&conn, s1, "content.Make", "Canon");
        conn.execute("UPDATE sources SET basis_rev = 7 WHERE id = ?1", [s1])
            .unwrap();
        let result = execute_prune_stale(&conn, None).unwrap();
        assert_eq!(
            result.summary,
            "Deleted 1 stale fact (observed_basis_rev mismatch)"
        );
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
        let s2 = insert_source(&conn, root_id, "b.jpg", None);
        crate::core::testing::insert_fact(&conn, s1, "content.Make", "Canon");
        crate::core::testing::insert_fact(&conn, s2, "content.Make", "Nikon");

        let plan =
            plan_delete_over(&mut conn, vec![s1], "content.Make", FactSide::Source, None).unwrap();
        let decision =
            recording_decision(crate::core::domain::decision::DecisionCommand::FactsDelete);
        let result = execute_delete(&mut conn, &plan, Some(&decision)).unwrap();

        let (command, status, attempted, completed, summary) = only_decision(&conn);
        assert_eq!(command, "facts_delete");
        assert_eq!(status, "completed");
        assert_eq!(attempted, 1);
        assert_eq!(completed, 1);
        assert_eq!(summary, result.summary);
        assert!(!summary.is_empty());
        // The row names the key and the facts it still holds: the one the delete left.
        let (key, remaining): (String, i64) = conn
            .query_row(
                "SELECT fact_key, fact_key_remaining FROM decisions",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!((key.as_str(), remaining), ("content.Make", 1));
    }

    /// **The delete acts on the key the plan counted.** `Make` is read as
    /// `content.Make` (`facts::domain::delete_key`), the plan counts that key,
    /// and the delete removes exactly the rows counted: a row stored under the
    /// name as given (`Make`, planted by hand) and every other key stay. The
    /// result names the key acted on, and is also the decision's summary.
    #[test]
    fn the_delete_removes_exactly_the_key_the_plan_counted() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let s1 = insert_source(&conn, root_id, "a.jpg", None);
        let s2 = insert_source(&conn, root_id, "b.jpg", None);
        crate::core::testing::insert_fact(&conn, s1, "content.Make", "Canon");
        crate::core::testing::insert_fact(&conn, s2, "content.Make", "Nikon");
        crate::core::testing::insert_fact(&conn, s1, "Make", "as given");
        crate::core::testing::insert_fact(&conn, s1, "content.Model", "EOS");

        let key = crate::facts::domain::delete_key("Make").unwrap();
        let plan = plan_delete_over(&mut conn, vec![s1, s2], &key, FactSide::Source, None).unwrap();
        assert_eq!(plan.fact_count, 2);
        let decision =
            recording_decision(crate::core::domain::decision::DecisionCommand::FactsDelete);
        let result = execute_delete(&mut conn, &plan, Some(&decision)).unwrap();

        assert_eq!(
            result.summary,
            "Deleted 2 content.Make facts across 2 sources"
        );
        assert_eq!(only_decision(&conn).4, result.summary);
        let mut left: Vec<String> = conn
            .prepare("SELECT key FROM facts")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        left.sort();
        assert_eq!(left, ["Make", "content.Model"]);
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
        assert_eq!(
            summary,
            "Deleted 1 facts stored on excluded sources\nTotal: 1 facts deleted"
        );
    }

    // =========================================================================
    // The drop settlement at this file's four sites (the subsystem's fifth
    // is `cli/import.rs`, which carries its own)
    //
    // Each plants a failure inside the window and reads the row afterwards.
    // Without `.armed(..)` at the site — or, at `facts delete`, without the
    // wrapper's `Err` arm — the row reads `started`. `summary` is read
    // nullable so that case fails on the status assertion rather than on a
    // NULL column, where the message would say nothing.
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
        // A key with a space, so the sentence shows it as Canon prints it.
        crate::core::testing::insert_fact(&conn, s1, "content.File Size", "10");
        let plan = plan_delete_over(
            &mut conn,
            vec![s1],
            "content.File Size",
            FactSide::Source,
            None,
        )
        .unwrap();
        // A `BEFORE DELETE` trigger over an empty result set never fires, so
        // the row the delete must match is asserted rather than assumed.
        assert_eq!(plan.fact_count, 1);
        let decision =
            recording_decision(crate::core::domain::decision::DecisionCommand::FactsDelete);
        crate::core::testing::plant_failure(&conn, "DELETE", "facts");

        assert!(execute_delete(&mut conn, &plan, Some(&decision)).is_err());

        let (status, summary) = settled_row(&conn);
        assert_eq!(status, "interrupted");
        assert_eq!(
            summary.unwrap(),
            r#"Deletion of "content.File Size" facts ended early"#
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
            "Excluded-fact pruning ended early; facts stored on excluded sources may be gone \
             while facts stored on excluded content remain — re-run to converge",
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
