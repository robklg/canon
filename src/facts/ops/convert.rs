//! `canon facts convert`'s operations: the read, the plan and the act over a key's census.
//!
//! Every invocation begins with the same key reading ([`read_census`]): the name's own rules,
//! then the census, and for a key with no facts the fixed-type answer, else the emptied-key
//! answer, else an unknown key. The read and the plan each run in one read transaction, so
//! every count comes from one view of the index; the act runs in one write transaction, the
//! reading and the plan made again inside it.

use std::path::Path;

use anyhow::Result;
use rusqlite::TransactionBehavior;

use crate::core::domain::decision::{DecisionCommand, DecisionStatus};
use crate::core::domain::fact::{FactType, FactValue, KeyCensus};
use crate::core::domain::format_count;
use crate::core::domain::time::zone::UserZone;
use crate::core::ops::decision::{DecisionCounts, DecisionParams, DecisionRecorder};
use crate::core::repo::decision::{import_recorded_after, latest_naming_key, set_fact_key};
use crate::core::repo::fact::key_census;
use crate::core::repo::Connection;
use crate::expr::{aliases_naming_key, printed_key, AliasesNaming};
use crate::facts::domain::convert::{
    bridge_clause, census_types, check_acknowledgments, class_summary, convert_key,
    fixed_type_answer, holdings, list_lines, next_import, plan, printed_stored, targets,
    Acknowledgments, ConversionPlan, ConvertRefused, Holdings, ListLine, NextImport, Target,
    TargetLine,
};
use crate::facts::domain::{counted, plural};
use crate::facts::repo::{delete_facts_by_id, update_fact_values};

/// The read: what a key holds and every target it can reach or not.
#[derive(Debug, Clone)]
pub struct KeyRead {
    pub key: String,
    pub holdings: Holdings,
    pub targets: Vec<TargetLine>,
}

/// The plan for one target, with what comes after it and the census it was made from.
#[derive(Debug, Clone)]
pub struct PlanRead {
    pub key: String,
    pub plan: ConversionPlan,
    pub next: Vec<NextImport>,
    pub aliases: AliasesNaming,
    census: KeyCensus,
}

impl PlanRead {
    /// Every lossy and removed fact, one line per source holding it.
    pub fn list(&self) -> Vec<ListLine> {
        list_lines(&self.census, &self.plan)
    }
}

/// **The key reading every invocation begins with**: the stored key and its census — or the
/// refusal for a `source.` name and the content hash; and for a key with no facts, the
/// fixed-type answer of a built-in name, else the emptied-key answer, else an unknown key.
fn read_census(conn: &Connection, name: &str) -> Result<(String, KeyCensus)> {
    let key = convert_key(name).map_err(ConvertRefused::Key)?;
    let census = key_census(conn, &key)?;
    if !census.facts.is_empty() {
        return Ok((key, census));
    }
    if let Some(refusal) = fixed_type_answer(name) {
        return Err(ConvertRefused::Key(refusal).into());
    }
    if let Some(decision_id) = emptied_by(conn, &key)? {
        return Err(ConvertRefused::Emptied { key, decision_id }.into());
    }
    Err(ConvertRefused::UnknownKey {
        name: name.to_string(),
    }
    .into())
}

/// The conversion that removed the last facts of a key that has none: the latest decision
/// naming the key is a conversion that left it no facts, and no import is recorded after it.
fn emptied_by(conn: &Connection, key: &str) -> Result<Option<i64>> {
    let Some(latest) = latest_naming_key(conn, key)? else {
        return Ok(None);
    };
    let emptying = DecisionCommand::from_recorded(&latest.command)
        == Some(DecisionCommand::FactsConvert)
        && latest.remaining == Some(0);
    if emptying && !import_recorded_after(conn, latest.id)? {
        Ok(Some(latest.id))
    } else {
        Ok(None)
    }
}

/// **The read**: what the key named holds, and every target with its plan or why it is not a
/// conversion — read-only.
pub fn read_key(conn: &mut Connection, name: &str, zone: &UserZone) -> Result<KeyRead> {
    let tx = conn.transaction()?;
    let (key, census) = read_census(&tx, name)?;
    let holdings = holdings(&census, zone).ok_or_else(|| ConvertRefused::UnknownKey {
        name: name.to_string(),
    })?;
    let targets = targets(&census);
    tx.commit()?;
    Ok(KeyRead {
        key,
        holdings,
        targets,
    })
}

/// **The plan** for converting the key named to `target`, with the next import for the target
/// type and the aliases naming the key — or the refusal for a target no conversion reaches
/// and for `duration`, each with its routes.
pub fn plan_convert(
    conn: &mut Connection,
    name: &str,
    target: Target,
    canon_home: &Path,
) -> Result<PlanRead> {
    let tx = conn.transaction()?;
    let (key, census) = read_census(&tx, name)?;
    tx.commit()?;
    let plan = reach(&key, name, &census, target)?;
    let aliases = aliases_naming_key(canon_home, &key);
    Ok(PlanRead {
        next: next_import(plan.to),
        aliases,
        key,
        plan,
        census,
    })
}

/// The plan for `target` over the census — or `duration`'s routes, or why the target is not a
/// conversion: the same answer for the plan and the act.
fn reach(key: &str, name: &str, census: &KeyCensus, target: Target) -> Result<ConversionPlan> {
    let types = census_types(census).ok_or_else(|| ConvertRefused::UnknownKey {
        name: name.to_string(),
    })?;
    let to = match target {
        Target::Type(to) => to,
        Target::Duration => {
            let removal = plan(census, FactType::Num)
                .ok()
                .and_then(|plan| plan.duration_removal());
            return Err(ConvertRefused::Duration {
                numbers_only: types.single() == Some(FactType::Num),
                key: key.to_string(),
                removal,
            }
            .into());
        }
    };
    Ok(
        plan(census, to).map_err(|why| ConvertRefused::NotAConversion {
            key: key.to_string(),
            types,
            to,
            why,
        })?,
    )
}

/// What the act did.
#[derive(Debug)]
pub enum ConvertOutcome {
    /// Every fact is already of the target type: nothing changed, nothing recorded.
    NothingToConvert {
        key: String,
        to: FactType,
    },
    Converted(Box<ConvertResult>),
}

/// A conversion performed: the plan it carried out, its summary — the printed result, the
/// decision record's summary and the trail's narration — and its record.
#[derive(Debug)]
pub struct ConvertResult {
    pub key: String,
    pub summary: String,
    pub decision_id: Option<i64>,
    pub performed: ConversionPlan,
    pub warnings: Vec<String>,
}

/// **The act**: every fact of the key converted to `target`, all or nothing.
///
/// One write transaction, taken before anything is read, so no import can change the key's
/// type between the plan and the writes: the key reading, the census and the plan are made
/// inside it, and the acknowledgments are checked against that plan before anything is
/// recorded or written. A plan that changes nothing ends first, since an acknowledgment
/// protects nothing there. The decision row is recorded on the transaction and rolls back
/// with the writes; no receipt is written, since a change of type changes no source's standing.
pub fn execute_convert(
    conn: &mut Connection,
    name: &str,
    target: Target,
    given: &Acknowledgments,
    decision: Option<&DecisionParams>,
) -> Result<ConvertOutcome> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let (key, census) = read_census(&tx, name)?;
    let plan = reach(&key, name, &census, target)?;
    if plan.counts.total() == 0 {
        return Ok(ConvertOutcome::NothingToConvert { key, to: plan.to });
    }
    if let Err(problems) = check_acknowledgments(&plan, given) {
        return Err(ConvertRefused::Acknowledgment {
            key,
            to: plan.to,
            problems,
            counts: plan.counts,
            reason: decision.and_then(|d| d.reason.clone()),
        }
        .into());
    }

    let mut recorder = decision.map(|d| DecisionRecorder::start(&tx, d, None));
    let changed: Vec<(i64, &FactValue)> = plan
        .changes
        .iter()
        .filter_map(|change| Some((change.fact_id, change.becomes.as_ref()?)))
        .collect();
    let removed: Vec<i64> = plan
        .changes
        .iter()
        .filter(|change| change.becomes.is_none())
        .map(|change| change.fact_id)
        .collect();
    update_fact_values(&tx, &changed)?;
    delete_facts_by_id(&tx, &removed)?;

    let summary = summary_of(&key, &plan);
    let decision_id = recorder.as_ref().and_then(|r| r.decision_id());
    if let Some(recorder) = recorder.as_mut() {
        if let Some(id) = decision_id {
            if let Err(e) = set_fact_key(&tx, id, &key, plan.remaining()) {
                recorder.push_warning(format!(
                    "Warning: failed to record the key on the decision: {e}"
                ));
            }
        }
        let count = |n: u64| Some(n as i64);
        recorder.complete_db(
            &tx,
            DecisionStatus::Completed,
            DecisionCounts {
                attempted: count(plan.facts()),
                completed: count(plan.counts.total()),
                failed: None,
                skipped: count(plan.already),
            },
            &summary,
        );
    }
    tx.commit()?;
    let warnings = recorder.map(|mut r| r.take_warnings()).unwrap_or_default();
    Ok(ConvertOutcome::Converted(Box::new(ConvertResult {
        key,
        summary,
        decision_id,
        performed: plan,
        warnings,
    })))
}

/// How many values per class the summary names.
const SUMMARY_VALUES: usize = 3;

/// **The summary, composed once**: its first line is what a trail line carries — the key, the
/// target and the count in each class; then the bridge clause; then the most frequent lossy
/// and removed values, each class on its own line.
fn summary_of(key: &str, plan: &ConversionPlan) -> String {
    let mut lines = vec![
        format!(
            "{} → {}: {}",
            printed_key(key),
            plan.to,
            class_summary(&plan.counts)
        ),
        format!("  whole index: {}", bridge_clause(&plan.bridge)),
    ];
    for (class, groups) in [("lossy", &plan.lossy), ("removed", &plan.removed)] {
        if groups.shown.is_empty() {
            continue;
        }
        let mut values: Vec<String> = groups
            .shown
            .iter()
            .take(SUMMARY_VALUES)
            .map(|group| {
                format!(
                    "{} ×{}",
                    printed_stored(&group.value),
                    counted(group.facts, "fact", "facts")
                )
            })
            .collect();
        let more = (groups.shown.len() as u64 + groups.more).saturating_sub(SUMMARY_VALUES as u64);
        if more > 0 {
            values.push(format!(
                "and {} more distinct {}",
                format_count(more),
                plural(more, "value", "values")
            ));
        }
        lines.push(format!("  {class}: {}", values.join(", ")));
    }
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::{plan_convert, read_key};
    use crate::core::domain::fact::FactType;
    use crate::core::domain::time::zone::UserZone;
    use crate::core::domain::IncludeSet;
    use crate::core::ops::scope::classify_all;
    use crate::core::repo::decision::{insert_started, set_fact_key};
    use crate::core::repo::fact::key_census;
    use crate::core::testing::*;
    use crate::expr::{select_sources, BoundFilters, RolePolicy, SelectionParams};
    use crate::facts::domain::convert::{valued_places, Place};
    use crate::facts::domain::convert::{ConvertRefused, Target};

    /// **The counted place is what `select_sources` selects**: the census's sources counted by
    /// `canon facts --global` are exactly the sources `canon facts --global`'s selection keeps,
    /// among those whose value is a fact of the key — on a database holding every place.
    #[test]
    fn the_counted_place_is_what_select_sources_selects() {
        let mut conn = setup_test_db();
        let open = insert_root(&conn, "/open", "source", false);
        let parked = insert_root(&conn, "/parked", "source", true);
        let archive = insert_root(&conn, "/archive", "archive", false);
        let excluded_obj = insert_object(&conn, "x", true);
        let shared = insert_object(&conn, "s", false);
        let ids = [
            insert_source(&conn, open, "counted", None),
            insert_source_excluded(&conn, open, "excluded", None),
            insert_source(&conn, open, "excluded-content", Some(excluded_obj)),
            insert_source(&conn, open, "gone", None),
            insert_source(&conn, parked, "parked", None),
            insert_source(&conn, archive, "copy", Some(shared)),
            insert_source(&conn, open, "shared", Some(shared)),
        ];
        conn.execute("UPDATE sources SET present = 0 WHERE id = ?", [ids[3]])
            .unwrap();
        for &id in &ids[..5] {
            insert_fact(&conn, id, "content.K", "v");
        }
        insert_object_fact(&conn, excluded_obj, "content.K", "v");
        insert_object_fact(&conn, shared, "content.K", "w");
        insert_source(&conn, open, "no-fact", None);

        let valued = valued_places(&key_census(&conn, "content.K").unwrap());
        let counted: Vec<i64> = valued
            .iter()
            .filter(|(_, place)| *place == Place::Counted)
            .map(|(id, _)| *id)
            .collect();

        let selection = select_sources(
            &mut conn,
            &SelectionParams {
                scopes: classify_all(&[]),
                include: IncludeSet::default(),
                filters: BoundFilters::none(),
                role_policy: RolePolicy::SourceUnlessIncluded,
                clock: UserZone::utc(),
                notes: false,
            },
        )
        .unwrap();
        let mut selected: Vec<i64> = selection
            .source_ids()
            .into_iter()
            .filter(|id| valued.iter().any(|(valued, _)| valued == id))
            .collect();
        selected.sort_unstable();
        assert_eq!(counted, selected);
        assert_eq!(counted, [ids[0], ids[6]]);
        assert_eq!(
            valued.len(),
            7,
            "every place holds a source whose value is a fact"
        );
    }

    fn decision(conn: &crate::core::repo::Connection, command: &str) -> i64 {
        insert_started(
            conn, command, None, "canon", None, "0.7.0", "UTC", None, None,
        )
        .unwrap()
    }

    /// The refusal the read gives for `name`.
    fn refused(conn: &mut crate::core::repo::Connection, name: &str) -> ConvertRefused {
        let err = read_key(conn, name, &UserZone::utc()).expect_err("refused");
        err.downcast_ref::<ConvertRefused>()
            .unwrap_or_else(|| panic!("{err:?}"))
            .clone()
    }

    fn is_emptied_by(refusal: &ConvertRefused, id: i64) -> bool {
        matches!(refusal, ConvertRefused::Emptied { decision_id, .. } if *decision_id == id)
    }

    /// **The emptied-key answer is given exactly where a conversion removed the key's last facts
    /// and nothing recorded since could have brought them back**: a conversion leaving none →
    /// the answer, naming it; an import recorded after it → an unknown key; a `facts delete`
    /// naming the key after it → an unknown key; a conversion that kept facts, gone since by a
    /// prune that names no key → an unknown key; nothing recorded (recording off) → an unknown key.
    #[test]
    fn the_emptied_key_answer_names_the_conversion_that_emptied_it() {
        let mut conn = setup_test_db();
        let emptying = decision(&conn, "facts_convert");
        set_fact_key(&conn, emptying, "content.Gone", 0).unwrap();
        let answer = refused(&mut conn, "Gone");
        assert!(is_emptied_by(&answer, emptying), "{answer:?}");
        assert!(answer.is_answer());
        assert_eq!(
            answer.to_string(),
            format!(
                "content.Gone has no facts: conversion {emptying} removed every fact the key \
                 then held\n`canon trail show {emptying}` to read it"
            )
        );

        decision(&conn, "import_facts");
        let after_import = refused(&mut conn, "Gone");
        assert!(matches!(after_import, ConvertRefused::UnknownKey { .. }));
        assert!(!after_import.is_answer());

        let emptying = decision(&conn, "facts_convert");
        set_fact_key(&conn, emptying, "content.Deleted", 0).unwrap();
        let deleting = decision(&conn, "facts_delete");
        set_fact_key(&conn, deleting, "content.Deleted", 0).unwrap();
        assert!(matches!(
            refused(&mut conn, "Deleted"),
            ConvertRefused::UnknownKey { .. }
        ));

        let keeping = decision(&conn, "facts_convert");
        set_fact_key(&conn, keeping, "content.Pruned", 3).unwrap();
        decision(&conn, "prune_stale");
        assert!(matches!(
            refused(&mut conn, "Pruned"),
            ConvertRefused::UnknownKey { .. }
        ));

        assert!(matches!(
            refused(&mut conn, "Unrecorded"),
            ConvertRefused::UnknownKey { .. }
        ));
    }

    /// The plan reads the same key reading: an emptied key, the content hash and a built-in
    /// name are answered before any target is asked about; `duration` is answered with its
    /// routes and `--to number` on a time key with the reason it is not a conversion.
    #[test]
    fn the_plan_begins_with_the_same_key_reading() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/r", "source", false);
        let a = insert_source(&conn, root, "a", None);
        let b = insert_source(&conn, root, "b", None);
        insert_fact(&conn, a, "content.Len", "1:23:45");
        insert_fact(&conn, b, "content.Len", "12:05");
        insert_fact(&conn, a, "content.Mix", "1:23:45");
        insert_fact(&conn, b, "content.Mix", "misc");
        insert_fact(&conn, a, "content.Lossy", "1:23:45");
        insert_fact(&conn, b, "content.Lossy", "01234");
        insert_time_fact(&conn, a, "content.Taken", "2021-03-02T18:44:10");
        let emptying = decision(&conn, "facts_convert");
        set_fact_key(&conn, emptying, "content.Gone", 0).unwrap();
        let home = tempfile::tempdir().unwrap();
        let plan_refused = |conn: &mut crate::core::repo::Connection, name: &str, to| {
            let err = plan_convert(conn, name, to, home.path()).expect_err("refused");
            err.downcast_ref::<ConvertRefused>()
                .unwrap_or_else(|| panic!("{err:?}"))
                .clone()
        };
        let time = Target::Type(FactType::Time);
        assert!(is_emptied_by(
            &plan_refused(&mut conn, "Gone", time),
            emptying
        ));
        assert!(matches!(
            plan_refused(&mut conn, "hash", time),
            ConvertRefused::Key(crate::facts::domain::convert::KeyRefusal::ContentHash)
        ));
        assert!(matches!(
            plan_refused(&mut conn, "filename", time),
            ConvertRefused::Key(crate::facts::domain::convert::KeyRefusal::Builtin { .. })
        ));
        let duration = plan_refused(&mut conn, "Len", Target::Duration);
        assert!(
            matches!(
                duration,
                ConvertRefused::Duration {
                    numbers_only: false,
                    removal: Some(2),
                    ..
                }
            ),
            "{duration:?}"
        );
        assert!(duration
            .to_string()
            .starts_with("`1:23:45` is not a number\n"));
        // A removal that would take a value the hint cannot bring back offers no removal route.
        let mixed = plan_refused(&mut conn, "Mix", Target::Duration);
        assert!(
            matches!(mixed, ConvertRefused::Duration { removal: None, .. }),
            "{mixed:?}"
        );
        // A removal beside a loss: the removal command alone would not run, so no removal route.
        let lossy = plan_refused(&mut conn, "Lossy", Target::Duration);
        assert!(
            matches!(lossy, ConvertRefused::Duration { removal: None, .. }),
            "{lossy:?}"
        );
        let number = plan_refused(&mut conn, "Taken", Target::Type(FactType::Num));
        assert_eq!(
            number.to_string(),
            "content.Taken is of type time: a time cannot be converted to a number; a time \
             known only to a year can go through text"
        );
    }

    // ========================================================================
    // The act
    // ========================================================================

    use super::{execute_convert, ConvertOutcome, ConvertResult};
    use crate::core::domain::config::LedgerConfig;
    use crate::core::domain::decision::DecisionCommand;
    use crate::core::domain::fact::{FactValue, KeyTypes, Stored};
    use crate::core::domain::time::zone::ReadTimesOn;
    use crate::core::ops::decision::DecisionParams;
    use crate::core::repo::fact::key_types;
    use crate::core::repo::Connection;
    use crate::expr::{read_filters, ExpressionText};
    use crate::facts::domain::convert::{stored_spelling, Ack, Acknowledgments};

    fn recording(reason: Option<&str>) -> DecisionParams {
        DecisionParams {
            command: DecisionCommand::FactsConvert,
            scope: Vec::new(),
            command_line: "canon facts convert".to_string(),
            reason: reason.map(str::to_string),
            record_enabled: true,
            receipt_enabled: false,
            ledger_config: LedgerConfig::default(),
            zone: ReadTimesOn::of_user(&UserZone::utc()),
        }
    }

    fn acks(loss: Option<u64>, removal: Option<u64>) -> Acknowledgments {
        Acknowledgments {
            loss: loss.map(Ack::Count),
            removal: removal.map(Ack::Count),
        }
    }

    /// The act, which must convert.
    fn converted(
        conn: &mut Connection,
        name: &str,
        to: FactType,
        given: Acknowledgments,
    ) -> ConvertResult {
        let outcome =
            execute_convert(conn, name, Target::Type(to), &given, Some(&recording(None))).unwrap();
        match outcome {
            ConvertOutcome::Converted(result) => *result,
            ConvertOutcome::NothingToConvert { .. } => panic!("{outcome:?}"),
        }
    }

    fn types_of(conn: &Connection, key: &str) -> Option<KeyTypes> {
        key_types(conn, &[key]).unwrap().remove(key)
    }

    fn decision_rows(conn: &Connection) -> i64 {
        conn.query_row("SELECT COUNT(*) FROM decisions", [], |r| r.get(0))
            .unwrap()
    }

    /// A stored value as its type and stored spelling, to compare.
    fn spelled(value: &Stored<FactValue>) -> String {
        match value {
            Stored::Readable(v) => format!("{} {}", v.kind(), stored_spelling(v)),
            Stored::Unreadable => "unreadable".to_string(),
        }
    }

    /// Every fact of the key, by id: its value as stored.
    fn values(conn: &Connection, key: &str) -> Vec<(i64, String)> {
        let mut facts: Vec<(i64, String)> = key_census(conn, key)
            .unwrap()
            .facts
            .iter()
            .map(|fact| (fact.id, spelled(&fact.value)))
            .collect();
        facts.sort_by_key(|(id, _)| *id);
        facts
    }

    /// A key holding text and time facts at every place a fact can sit: an open root, a
    /// suspended root, a file no longer present, an archive copy, an excluded source, content no
    /// source holds, and a source's own fact hidden behind its content's. One text is no time.
    fn a_key_of_two_types_everywhere(conn: &Connection) {
        let open = insert_root(conn, "/open", "source", false);
        let parked = insert_root(conn, "/parked", "source", true);
        let archive = insert_root(conn, "/archive", "archive", false);
        let on_open = insert_source(conn, open, "a.jpg", None);
        insert_fact(conn, on_open, "content.K", "2021:03:02 18:44:10");
        let timed = insert_source(conn, open, "t.jpg", None);
        insert_time_fact(conn, timed, "content.K", "2022-01-01T10:00:00");
        let behind = insert_source(conn, parked, "p.jpg", None);
        insert_fact(conn, behind, "content.K", "2020-01-01");
        let not_a_time = insert_source(conn, parked, "q.jpg", None);
        insert_fact(conn, not_a_time, "content.K", "misc");
        let gone = insert_source(conn, open, "gone.jpg", None);
        insert_fact(conn, gone, "content.K", "2019-05-05");
        conn.execute("UPDATE sources SET present = 0 WHERE id = ?", [gone])
            .unwrap();
        let copied = insert_object(conn, "copied", false);
        insert_source(conn, archive, "c.jpg", Some(copied));
        insert_object_fact(conn, copied, "content.K", "2018-01-01");
        let dismissed = insert_source_excluded(conn, open, "x.jpg", None);
        insert_fact(conn, dismissed, "content.K", "2017-01-01");
        let orphan = insert_object(conn, "orphan", false);
        insert_object_fact(conn, orphan, "content.K", "2016-01-01");
        let shown = insert_object(conn, "shown", false);
        let holder = insert_source(conn, open, "h.jpg", Some(shown));
        insert_object_fact(conn, shown, "content.K", "2015-01-01");
        insert_fact(conn, holder, "content.K", "2014-01-01");
    }

    /// **One type per key, wherever the facts sit**: a key of two types across every place,
    /// converted, holds one type — every fact reached, parked and gone ones included.
    #[test]
    fn the_conversion_leaves_one_type_wherever_the_facts_sit() {
        let mut conn = setup_test_db();
        a_key_of_two_types_everywhere(&conn);
        assert_eq!(types_of(&conn, "content.K").unwrap().counts().len(), 2);

        let result = converted(&mut conn, "K", FactType::Time, acks(None, Some(1)));

        assert_eq!(result.performed.counts.removed, 1);
        let types = types_of(&conn, "content.K").unwrap();
        assert_eq!(types.single(), Some(FactType::Time), "{types:?}");
        assert_eq!(types.counts(), [(FactType::Time, 9)]);
    }

    /// **Opening a root changes no key's type and no filter's binding**: a key whose every fact
    /// sits on a suspended root, converted there, is of its new type while the root is parked,
    /// and a filter on it binds — or is refused — the same before and after the root is opened.
    #[test]
    fn unsuspending_a_root_changes_no_key_type_and_no_binding_verdict() {
        let mut conn = setup_test_db();
        let parked = insert_root(&conn, "/parked", "source", true);
        for (name, value) in [("a.jpg", "2021-03-02"), ("b.jpg", "2020:01:01 08:00:00")] {
            let id = insert_source(&conn, parked, name, None);
            insert_fact(&conn, id, "content.When", value);
        }
        converted(&mut conn, "When", FactType::Time, acks(None, None));

        let verdicts = |conn: &Connection| {
            (
                types_of(conn, "content.When").and_then(|types| types.single()),
                read_filters(conn, &ExpressionText::plain(&["content.When > 2020"])).is_ok(),
                read_filters(conn, &ExpressionText::plain(&["content.When = misc"])).is_ok(),
            )
        };
        let while_parked = verdicts(&conn);
        assert_eq!(while_parked, (Some(FactType::Time), true, false));
        conn.execute("UPDATE roots SET suspended = 0 WHERE id = ?", [parked])
            .unwrap();
        assert_eq!(verdicts(&conn), while_parked);
    }

    /// **A key emptied by a conversion takes its type from its next import**: nothing outside
    /// the key's rows keeps the type it had, so a number imported next lands as a number.
    #[test]
    fn a_key_whose_facts_are_all_removed_takes_a_new_type_at_its_next_import() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/r", "source", false);
        let a = insert_source(&conn, root, "a.jpg", None);
        let b = insert_source(&conn, root, "b.jpg", None);
        insert_fact(&conn, a, "content.Serial", "misc");
        insert_fact(&conn, b, "content.Serial", "abc");

        let result = converted(&mut conn, "Serial", FactType::Num, acks(None, Some(2)));
        assert_eq!(result.performed.remaining(), 0);
        assert!(types_of(&conn, "content.Serial").is_none());

        let record: crate::facts::ops::import::ImportRecord = serde_json::from_str(&format!(
            r#"{{"source_id": {a}, "basis_rev": 0, "facts": {{"content.Serial": 5}}}}"#
        ))
        .unwrap();
        crate::facts::ops::import::import_batch(&conn, &[record], false).unwrap();
        assert_eq!(
            types_of(&conn, "content.Serial").and_then(|types| types.single()),
            Some(FactType::Num)
        );
    }

    /// **Value by value, the act is the plan**: every fact the plan left unchanged reads back as
    /// the same value of the target type, every lossy one as its nearest value, and exactly the
    /// facts the plan removed are gone.
    #[test]
    fn value_by_value_after_the_act() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/r", "source", false);
        for (name, value) in [
            ("a", "12"),
            ("b", "01234"),
            ("c", "2.50"),
            ("d", "misc"),
            ("e", "7"),
        ] {
            let id = insert_source(&conn, root, name, None);
            insert_fact(&conn, id, "content.N", value);
        }
        let before = values(&conn, "content.N");

        let result = converted(&mut conn, "N", FactType::Num, acks(Some(2), Some(1)));

        let after: std::collections::HashMap<i64, String> =
            values(&conn, "content.N").into_iter().collect();
        for change in &result.performed.changes {
            let planned = change
                .becomes
                .as_ref()
                .map(|v| spelled(&Stored::Readable(v.clone())));
            assert_eq!(
                after.get(&change.fact_id),
                planned.as_ref(),
                "fact {}",
                change.fact_id
            );
        }
        let id_of = |text: &str| {
            before
                .iter()
                .find(|(_, v)| *v == format!("text {text}"))
                .unwrap()
                .0
        };
        let read = |text: &str| after.get(&id_of(text)).map(String::as_str);
        assert_eq!(read("12"), Some("number 12"));
        assert_eq!(read("01234"), Some("number 1234"));
        assert_eq!(read("2.50"), Some("number 2.5"));
        assert_eq!(read("7"), Some("number 7"));
        assert_eq!(read("misc"), None);
        assert_eq!(after.len(), 4);
    }

    /// **A converted fact keeps when it was observed** and the file revision it was observed
    /// at, so the out-of-date checks see nothing new.
    #[test]
    fn observation_data_is_kept() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/r", "source", false);
        let id = insert_source(&conn, root, "a", None);
        insert_fact(&conn, id, "content.N", "12");
        conn.execute(
            "UPDATE facts SET observed_at = 1600000000, observed_basis_rev = 7",
            [],
        )
        .unwrap();

        converted(&mut conn, "N", FactType::Num, acks(None, None));

        let kept: (Option<f64>, i64, i64) = conn
            .query_row(
                "SELECT value_num, observed_at, observed_basis_rev FROM facts",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(kept, (Some(12.0), 1600000000, 7));
    }

    /// **A refusal records nothing and changes nothing**: an import grows the removed class
    /// between the dry run and the act, and the act given the dry run's count is refused —
    /// leading with the dry run — with no decision row and every fact as it was.
    #[test]
    fn a_refusal_records_nothing_and_changes_nothing() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/r", "source", false);
        for (name, value) in [("a", "12"), ("b", "misc")] {
            let id = insert_source(&conn, root, name, None);
            insert_fact(&conn, id, "content.N", value);
        }
        let home = tempfile::tempdir().unwrap();
        let seen = plan_convert(&mut conn, "N", Target::Type(FactType::Num), home.path())
            .unwrap()
            .plan
            .counts
            .removed;
        assert_eq!(seen, 1);
        let later = insert_source(&conn, root, "c", None);
        insert_fact(&conn, later, "content.N", "other");
        let before = values(&conn, "content.N");

        let err = execute_convert(
            &mut conn,
            "N",
            Target::Type(FactType::Num),
            &acks(None, Some(seen)),
            Some(&recording(None)),
        )
        .unwrap_err();

        let refusal = err.downcast_ref::<ConvertRefused>().expect("an answer");
        assert!(refusal.is_answer());
        assert!(
            matches!(refusal, ConvertRefused::Acknowledgment { problems, .. }
                if problems.len() == 1 && problems[0].now == 2),
            "{refusal:?}"
        );
        assert_eq!(decision_rows(&conn), 0);
        assert_eq!(values(&conn, "content.N"), before);
    }

    /// **A failure inside the act rolls the record back with the writes**: a write that fails
    /// partway leaves no decision row and every fact as it was.
    #[test]
    fn a_failure_inside_the_act_rolls_back_the_record_too() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/r", "source", false);
        for (name, value) in [("a", "12"), ("b", "misc")] {
            let id = insert_source(&conn, root, name, None);
            insert_fact(&conn, id, "content.N", value);
        }
        conn.execute_batch(
            "CREATE TEMP TRIGGER fail_delete BEFORE DELETE ON facts \
             BEGIN SELECT RAISE(ABORT, 'planted'); END",
        )
        .unwrap();
        let before = values(&conn, "content.N");

        let err = execute_convert(
            &mut conn,
            "N",
            Target::Type(FactType::Num),
            &acks(None, Some(1)),
            Some(&recording(None)),
        )
        .unwrap_err();

        assert!(err.to_string().contains("planted"), "{err}");
        assert_eq!(decision_rows(&conn), 0);
        assert_eq!(values(&conn, "content.N"), before);
    }

    /// **An act with nothing to change records nothing**, and acknowledgments given for it are
    /// never checked: the printed acting command run a second time converts nothing.
    #[test]
    fn a_zero_fact_act_records_nothing() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/r", "source", false);
        let id = insert_source(&conn, root, "a", None);
        insert_number_fact(&conn, id, "content.N", 12.0);

        let outcome = execute_convert(
            &mut conn,
            "N",
            Target::Type(FactType::Num),
            &acks(Some(3), Some(4)),
            Some(&recording(None)),
        )
        .unwrap();

        assert!(
            matches!(&outcome, ConvertOutcome::NothingToConvert { key, to: FactType::Num }
                if key == "content.N"),
            "{outcome:?}"
        );
        assert_eq!(decision_rows(&conn), 0);
    }

    /// **The summary is the record**: the result's summary is the decision row's, its first line
    /// the key, the target and each class's count, then where the facts sat and the most
    /// frequent removed values; the row names the key, the facts it holds after, and the reason.
    #[test]
    fn the_summary_is_the_record_and_the_trail_line() {
        let mut conn = setup_test_db();
        let open = insert_root(&conn, "/r", "source", false);
        let parked = insert_root(&conn, "/p", "source", true);
        for (root, name, value) in [
            (open, "a", "12"),
            (open, "b", "misc"),
            (parked, "c", "misc"),
            (open, "d", "x"),
            (open, "e", "y"),
            (open, "f", "z"),
        ] {
            let id = insert_source(&conn, root, name, None);
            insert_fact(&conn, id, "content.N", value);
        }

        let outcome = execute_convert(
            &mut conn,
            "N",
            Target::Type(FactType::Num),
            &acks(None, Some(5)),
            Some(&recording(Some("the tool wrote numbers as text"))),
        )
        .unwrap();
        let ConvertOutcome::Converted(result) = outcome else {
            panic!("{outcome:?}")
        };

        assert_eq!(
            result.summary,
            "content.N → number: 1 unchanged, 5 removed\n  \
             whole index: 6 text facts, the value of 6 sources — 1 of them not counted by \
             `canon facts --global`: 1 on a suspended root\n  \
             removed: misc ×2 facts, x ×1 fact, y ×1 fact, and 1 more distinct value"
        );
        let row: (String, Option<String>, String, i64) = conn
            .query_row(
                "SELECT summary, reason, fact_key, fact_key_remaining FROM decisions WHERE id = ?",
                [result.decision_id.unwrap()],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();
        assert_eq!(
            row,
            (
                result.summary.clone(),
                Some("the tool wrote numbers as text".to_string()),
                "content.N".to_string(),
                1
            )
        );
    }

    /// **The counts say what the act did**: attempted every fact of the key, completed every
    /// planned change — converted or removed — skipped the facts already of the target type,
    /// none failed; attempted = completed + skipped.
    #[test]
    fn the_counts_say_what_the_act_did() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/r", "source", false);
        for value in ["12", "01234", "misc"] {
            let id = insert_source(&conn, root, value, None);
            insert_fact(&conn, id, "content.N", value);
        }
        for n in [1.0, 2.0] {
            let id = insert_source(&conn, root, &format!("n{n}"), None);
            insert_number_fact(&conn, id, "content.N", n);
        }

        let result = converted(&mut conn, "N", FactType::Num, acks(Some(1), Some(1)));

        let counts: (i64, i64, Option<i64>, i64, String) = conn
            .query_row(
                "SELECT count_attempted, count_completed, count_failed, count_skipped, status \
                 FROM decisions WHERE id = ?",
                [result.decision_id.unwrap()],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
            )
            .unwrap();
        assert_eq!(counts, (5, 3, None, 2, "completed".to_string()));
    }

    /// **An import beside the conversion never leaves the key two types**: a writer adds a text
    /// fact while the conversion waits for the write lock; once it has the lock, the conversion
    /// reads the key as it now is and converts that fact too. Two connections to one file.
    #[test]
    fn an_import_and_a_conversion_never_leave_a_key_two_types() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::time::Duration;
        static WAITING: AtomicBool = AtomicBool::new(false);
        fn wait_for_the_lock(_: i32) -> bool {
            WAITING.store(true, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(5));
            true
        }
        let open_file = |path: &std::path::Path| {
            crate::core::repo::db::open_with_options(
                path,
                crate::core::repo::db::DbOptions {
                    debug_sql: false,
                    profile: false,
                },
            )
            .unwrap()
        };

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("canon.db");
        let writer = open_file(&path);
        let root = insert_root(&writer, "/r", "source", false);
        let first = insert_source(&writer, root, "a", None);
        insert_fact(&writer, first, "content.N", "5");
        let arriving = insert_source(&writer, root, "b", None);

        writer.execute_batch("BEGIN IMMEDIATE").unwrap();
        let mut converter = open_file(&path);
        converter.busy_handler(Some(wait_for_the_lock)).unwrap();
        let act = std::thread::spawn(move || {
            execute_convert(
                converter.conn_mut(),
                "N",
                Target::Type(FactType::Num),
                &acks(None, None),
                None,
            )
            .unwrap()
        });
        let started = std::time::Instant::now();
        while !WAITING.load(Ordering::SeqCst) {
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "the conversion never waited for the lock"
            );
            std::thread::sleep(Duration::from_millis(2));
        }
        insert_fact(&writer, arriving, "content.N", "7");
        writer.execute_batch("COMMIT").unwrap();

        let ConvertOutcome::Converted(result) = act.join().unwrap() else {
            panic!("nothing converted")
        };
        assert_eq!(result.performed.counts.unchanged, 2);
        assert_eq!(
            types_of(&writer, "content.N").unwrap().counts(),
            [(FactType::Num, 2)]
        );
    }
}
