//! **Selection notes**: what one selecting run recorded about each candidate source, counted
//! for the plan of a command that acts on its selection, and the plan's lines composed once.
//!
//! Two meanings people rely on without noticing them are made visible here. Every negated
//! operator is `NOT` of its positive form, so "everything else" includes the sources with no
//! value for the key; a line says how many of what the command acts on are there for that
//! reason alone. And a stored time coarser than a typed one is on neither side of it; a line
//! says how many such times the comparison could not place, on each side of the selection.
//!
//! The lines count what the selection's own evaluation decided: the marks are recorded in
//! the selecting loop, from the same cache and source row, never by a second evaluation. A
//! key is negated when its comparison sits under an odd number of negations in the bound
//! tree; the "no value" count is taken over what the command acts on; the spanning count's
//! "selected" direction over what the command acts on, and its "not selected" direction over
//! what the filter did not select. What the filter selected but the command will not act on
//! is in neither.

use std::collections::HashSet;

use anyhow::Result;

use crate::core::domain::fact::{FactType, FactValue};
use crate::core::domain::format::format_count;
use crate::core::domain::time::value::{ClockSpan, PeriodOp, Precision};
use crate::core::domain::time::zone::UserZone;
use crate::expr::domain::bind::{Bound, BoundKey, Test};
use crate::expr::domain::filter::{period_op, CompareOp};
use crate::expr::domain::print::{printed_value, printed_word};
use crate::expr::domain::transform::InstantRecord;
use crate::expr::ops::filter::{check_exists, read_value, OneSource};

/// What one evaluation recorded about each candidate source, for the plan's lines: the
/// negated keys and the time tests, read from the bound tree; per negated key, the selected
/// sources with no value for it; per time test, the candidates whose value spans it; and the
/// filter's own selection.
#[derive(Debug, Clone)]
pub struct NoteMarks {
    negated: Vec<NegatedKey>,
    times: Vec<TimeTest>,
    selected: HashSet<i64>,
}

/// A negated key as the plan names it, and the selected sources with no value for it.
#[derive(Debug, Clone)]
struct NegatedKey {
    key: BoundKey,
    no_value: HashSet<i64>,
}

/// One key against one typed period: every comparison of that pair, and the candidates whose
/// value spans the boundary of any of them.
#[derive(Debug, Clone)]
struct TimeTest {
    key: BoundKey,
    period: ClockSpan,
    time_of_day: bool,
    ops: Vec<PeriodOp>,
    spanning: HashSet<i64>,
}

/// How many of what the command acts on have no value for one negated key.
#[derive(Debug, Clone, PartialEq)]
struct NoValueCount {
    key: String,
    count: usize,
}

/// How many stored times one comparison could not place, on each side of the selection.
#[derive(Debug, Clone, PartialEq)]
struct SpanCount {
    key: String,
    period: String,
    precision: Precision,
    not_selected: usize,
    selected: usize,
}

/// What the "of which" lines count: sources; objects (`exclude set-object`); or sources out of
/// a matching count the line states itself (`facts delete`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum NoteUnit {
    Sources,
    Objects,
    OfMatching(usize),
}

/// The plan's lines, counted and composed once.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectionNotes {
    no_value: Vec<NoValueCount>,
    spans: Vec<SpanCount>,
    unit: NoteUnit,
}

impl NoteMarks {
    /// The negated keys and time tests the bound trees hold, on the user's clock, before any
    /// source is read.
    pub(super) fn of(trees: &[Bound], zone: &UserZone) -> NoteMarks {
        let mut marks = NoteMarks {
            negated: Vec::new(),
            times: Vec::new(),
            selected: HashSet::new(),
        };
        for tree in trees {
            marks.collect(tree, false, zone);
        }
        marks
    }

    fn collect(&mut self, tree: &Bound, negated: bool, zone: &UserZone) {
        match tree {
            Bound::And(all) | Bound::Or(all) => {
                all.iter().for_each(|t| self.collect(t, negated, zone))
            }
            Bound::Not(inner) => self.collect(inner, !negated, zone),
            Bound::Compare { key, op, test } => {
                self.negate(key, negated);
                self.time_test(key, *op, test, zone);
            }
            Bound::In { key, tests } => {
                self.negate(key, negated);
                tests
                    .iter()
                    .for_each(|test| self.time_test(key, CompareOp::Eq, test, zone));
            }
            Bound::Glob { key, .. } => self.negate(key, negated),
            // A bare `NOT key?` asks about having no value itself, so it owes no line.
            Bound::Exists { .. } | Bound::Status(_) => {}
        }
    }

    fn negate(&mut self, key: &BoundKey, negated: bool) {
        if negated && !self.negated.iter().any(|n| same_key(&n.key, key)) {
            self.negated.push(NegatedKey {
                key: key.clone(),
                no_value: HashSet::new(),
            });
        }
    }

    fn time_test(&mut self, key: &BoundKey, op: CompareOp, test: &Test, zone: &UserZone) {
        let (tv, time_of_day) = match test {
            Test::Period(tv) => (tv, false),
            Test::TimeOfDay(tv) => (tv, true),
            Test::Text(_) | Test::Number(_) | Test::Part(_) | Test::Bucket(_) => return,
        };
        let period = tv.on_clock(zone);
        let op = period_op(op);
        match self
            .times
            .iter_mut()
            .find(|t| same_key(&t.key, key) && t.period == period)
        {
            Some(found) if !found.ops.contains(&op) => found.ops.push(op),
            Some(_) => {}
            None => self.times.push(TimeTest {
                key: key.clone(),
                period,
                time_of_day,
                ops: vec![op],
                spanning: HashSet::new(),
            }),
        }
    }

    /// Mark every source selected: a run with no filters, which has nothing else to note.
    pub(super) fn select_all(&mut self, source_ids: &[i64]) {
        self.selected.extend(source_ids);
    }

    /// Record one source's marks, right after its evaluation decided `selected`.
    pub(super) fn record(&mut self, source: &OneSource, selected: bool) -> Result<()> {
        let id = source.id();
        // A value read here for the marks alone is not one the filter read: the run's own
        // answer about reading an instant stays the evaluation's.
        let unread = InstantRecord::default();
        if selected {
            self.selected.insert(id);
            for negated in &mut self.negated {
                if !check_exists(source, &negated.key, &unread)? {
                    negated.no_value.insert(id);
                }
            }
        }
        for time in &mut self.times {
            let Some(FactValue::Time(tv)) = read_value(source, &time.key, &unread)? else {
                continue;
            };
            let span = tv.on_clock(source.zone());
            if time.ops.iter().any(|op| span.spans(*op, &time.period)) {
                time.spanning.insert(id);
            }
        }
        Ok(())
    }

    /// Per negated key, how many of the `acted` sources have no value for it.
    fn no_value(&self, acted: &[i64]) -> Vec<NoValueCount> {
        self.check_acted(acted);
        self.negated
            .iter()
            .map(|n| NoValueCount {
                key: n.key.input.clone(),
                count: acted.iter().filter(|id| n.no_value.contains(id)).count(),
            })
            .collect()
    }

    /// Per negated key, how many objects have no value for it, each object given as its
    /// selected sources: an object counts when none of its selected sources has a value.
    fn no_value_objects(&self, objects: &[Vec<i64>]) -> Vec<NoValueCount> {
        objects.iter().for_each(|sources| self.check_acted(sources));
        self.negated
            .iter()
            .map(|n| NoValueCount {
                key: n.key.input.clone(),
                count: objects
                    .iter()
                    .filter(|sources| {
                        !sources.is_empty() && sources.iter().all(|id| n.no_value.contains(id))
                    })
                    .count(),
            })
            .collect()
    }

    /// Per time test, the spanning values: "selected" over what the command acts on; "not
    /// selected" over what the filter did not select. What the filter selected and the
    /// command will not act on is in neither direction.
    fn spans(&self, acted: &[i64]) -> Vec<SpanCount> {
        self.check_acted(acted);
        self.times
            .iter()
            .map(|t| SpanCount {
                key: t.key.input.clone(),
                period: printed_period(&t.period, t.time_of_day),
                precision: t.period.precision(),
                not_selected: t
                    .spanning
                    .iter()
                    .filter(|id| !self.selected.contains(id))
                    .count(),
                selected: acted.iter().filter(|id| t.spanning.contains(id)).count(),
            })
            .collect()
    }

    /// Every command acts on what the filter selected or less; an acted-on source the filter
    /// did not select is a caller's defect, and its counts would be false.
    fn check_acted(&self, acted: &[i64]) {
        debug_assert!(
            acted.iter().all(|id| self.selected.contains(id)),
            "a command acts only on what its filter selected"
        );
    }
}

/// One key, as the bound tree reads it: its base, accessor and modifiers, after the name
/// reader — so `Make` and `content.Make` are one key, `source.mtime|year` and `source.mtime`
/// two.
fn same_key(a: &BoundKey, b: &BoundKey) -> bool {
    a.base == b.base && a.accessor == b.accessor && a.modifiers == b.modifiers
}

/// A typed period named back as it was asked, in the printing rule's form: a time of day by
/// its time alone.
fn printed_period(period: &ClockSpan, time_of_day: bool) -> String {
    match period.printed_time_as_asked().filter(|_| time_of_day) {
        Some(time) => printed_word(&time),
        None => printed_value(&period.printed_as_asked(), FactType::Time),
    }
}

impl SelectionNotes {
    /// The lines of a plan acting on `acted`, out of a selection's marks: per negated key, how
    /// many of `acted` have no value, in `unit`; per time test, the spanning values on each
    /// side. No marks — no filter, or a selection that did not record — is no lines.
    pub fn of(marks: Option<&NoteMarks>, acted: &[i64], unit: NoteUnit) -> Self {
        match marks {
            Some(marks) => SelectionNotes {
                no_value: marks.no_value(acted),
                spans: marks.spans(acted),
                unit,
            },
            None => SelectionNotes::none(unit),
        }
    }

    /// The lines of a plan acting on objects, each given as its selected sources (`objects`),
    /// over the selected sources it acts through (`acted`): an object has no value where none
    /// of its selected sources has one; the spanning values are counted on those sources.
    pub fn of_objects(marks: Option<&NoteMarks>, objects: &[Vec<i64>], acted: &[i64]) -> Self {
        match marks {
            Some(marks) => SelectionNotes {
                no_value: marks.no_value_objects(objects),
                spans: marks.spans(acted),
                unit: NoteUnit::Objects,
            },
            None => SelectionNotes::none(NoteUnit::Objects),
        }
    }

    /// No lines.
    pub fn none(unit: NoteUnit) -> Self {
        SelectionNotes {
            no_value: Vec::new(),
            spans: Vec::new(),
            unit,
        }
    }

    /// Every line, indented two spaces; empty when every count is zero.
    pub fn lines(&self) -> Vec<String> {
        let no_value = self
            .no_value
            .iter()
            .filter(|n| n.count > 0)
            .map(|n| self.no_value_line(n));
        let spans = self
            .spans
            .iter()
            .filter(|s| s.not_selected + s.selected > 0)
            .map(span_line);
        no_value
            .chain(spans)
            .map(|line| format!("  {line}"))
            .collect()
    }

    /// A summary these lines qualify: `head` alone on the first line — the line the trail
    /// narrates — and each line below it. The one place a plan's notes join its count.
    pub fn under(&self, head: &str) -> String {
        std::iter::once(head.to_string())
            .chain(self.lines())
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn no_value_line(&self, n: &NoValueCount) -> String {
        let (count, key) = (format_count(n.count), &n.key);
        let has = if n.count == 1 { "has" } else { "have" };
        match self.unit {
            NoteUnit::Sources => format!("of which {count} {has} no {key}"),
            NoteUnit::Objects => {
                let objects = if n.count == 1 { "object" } else { "objects" };
                format!("of which {count} {objects} {has} no {key}")
            }
            NoteUnit::OfMatching(matching) => {
                let sources = if matching == 1 { "source" } else { "sources" };
                format!(
                    "from {} matching {sources}, {count} of which {has} no {key}",
                    format_count(matching)
                )
            }
        }
    }
}

/// `12 sources have a time that spans 2024-06 (coarser than a month): 9 not selected, 3
/// selected`, a zero direction and its separator left out.
fn span_line(s: &SpanCount) -> String {
    let total = s.not_selected + s.selected;
    let have = if total == 1 {
        "source has"
    } else {
        "sources have"
    };
    let directions: Vec<String> = [(s.not_selected, "not selected"), (s.selected, "selected")]
        .into_iter()
        .filter(|(n, _)| *n > 0)
        .map(|(n, what)| format!("{} {what}", format_count(n)))
        .collect();
    format!(
        "{} {have} a time that spans {} (coarser than a {}): {}",
        format_count(total),
        s.period,
        s.precision.word(),
        directions.join(", ")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::repo::Connection;
    use crate::core::testing::{insert_fact, insert_root, insert_source, insert_time_fact};
    use crate::expr::ops::bind::bound;
    use crate::expr::ops::filter::{apply_filters, apply_filters_noting};

    fn utc() -> UserZone {
        UserZone::utc()
    }

    /// The filter's marks over `ids`, and what it selected.
    fn marks(conn: &mut Connection, ids: &[i64], filter: &str) -> (Vec<i64>, NoteMarks) {
        let filter = bound(conn, filter);
        let (result, marks) = apply_filters_noting(conn, ids, &filter, &utc()).unwrap();
        (result.source_ids, marks)
    }

    /// The lines of a plan acting on everything the filter selected.
    fn lines(conn: &mut Connection, ids: &[i64], filter: &str) -> Vec<String> {
        let (selected, marks) = marks(conn, ids, filter);
        SelectionNotes::of(Some(&marks), &selected, NoteUnit::Sources).lines()
    }

    /// What `filter` selects among `ids`, by an ordinary selection.
    fn select(conn: &mut Connection, ids: &[i64], filter: &str) -> Vec<i64> {
        let filter = bound(conn, filter);
        apply_filters(conn, ids, &filter, &utc())
            .unwrap()
            .source_ids
    }

    /// Six sources: three with a `content.Make`, three with none; `content.Taken` at a year,
    /// a month, a day, and on none.
    fn library(conn: &Connection) -> Vec<i64> {
        let root = insert_root(conn, "/photos", "source", false);
        [
            (Some("Canon"), Some("2024")),
            (Some("Nikon"), Some("2024-03")),
            (Some("Canon"), None),
            (None, Some("2024")),
            (None, Some("2024-08-02")),
            (None, None),
        ]
        .into_iter()
        .enumerate()
        .map(|(i, (make, taken))| {
            let id = insert_source(conn, root, &format!("{i}.jpg"), None);
            if let Some(make) = make {
                insert_fact(conn, id, "content.Make", make);
            }
            if let Some(taken) = taken {
                insert_time_fact(conn, id, "content.Taken", taken);
            }
            id
        })
        .collect()
    }

    #[test]
    fn a_key_is_negated_under_an_odd_number_of_negations() {
        let mut conn = crate::core::testing::setup_test_db();
        let ids = library(&conn);
        for text in [
            "NOT Make = Canon",
            "Make != Canon",
            "Make !~ Can*",
            "Make NOT IN (Canon)",
        ] {
            assert_eq!(
                lines(&mut conn, &ids, text),
                ["  of which 3 have no Make"],
                "{text}"
            );
        }
        assert!(lines(&mut conn, &ids, "NOT (NOT Make = Canon)").is_empty());
        // `Make` under one negation, `Taken?` (a bare `?`) under two.
        assert_eq!(
            lines(&mut conn, &ids, "NOT (Make = Canon AND NOT Taken?)"),
            ["  of which 3 have no Make"]
        );
        // `Taken|year` under one, `Make` under two: selected are the Canon dated 2024 and
        // the two with no `Taken`, which have no `Taken|year`.
        assert_eq!(
            lines(
                &mut conn,
                &ids,
                "NOT (Taken|year = 2024 AND NOT Make = Canon)"
            ),
            ["  of which 2 have no Taken|year"]
        );
    }

    #[test]
    fn a_bare_not_exists_gives_no_line() {
        let mut conn = crate::core::testing::setup_test_db();
        let ids = library(&conn);
        assert_eq!(select(&mut conn, &ids, "NOT Make?").len(), 3);
        assert!(lines(&mut conn, &ids, "NOT Make?").is_empty());
    }

    /// `Taken|month` has no value where `Taken` is known only to the year, so those sources
    /// count beside the ones with no `Taken` at all.
    #[test]
    fn a_modified_key_is_counted_as_written() {
        let mut conn = crate::core::testing::setup_test_db();
        let ids = library(&conn);
        assert_eq!(
            lines(&mut conn, &ids, "Taken|month != 3"),
            ["  of which 4 have no Taken|month"]
        );
        assert_eq!(
            lines(&mut conn, &ids, "Taken|year != 2023"),
            ["  of which 2 have no Taken|year"]
        );
    }

    #[test]
    fn a_zero_count_prints_no_line() {
        let mut conn = crate::core::testing::setup_test_db();
        let ids = library(&conn);
        // Every source `Make != Nikon` selects without a value is set aside by the AND.
        assert!(lines(&mut conn, &ids, "Make != Nikon AND Make?").is_empty());
        assert!(SelectionNotes::none(NoteUnit::Sources).lines().is_empty());
    }

    /// The check rows: `Taken` known only to 2024 on two sources, compared with 2024-06 every
    /// way a comparison reads it.
    #[test]
    fn the_spanning_check_rows() {
        let mut conn = crate::core::testing::setup_test_db();
        let ids = library(&conn);
        let spans = "2 sources have a time that spans 2024-06 (coarser than a month)";
        for (text, directions) in [
            ("Taken < 2024-06", "2 not selected"),
            ("NOT Taken >= 2024-06", "2 selected"),
            ("Taken != 2024-06", "2 selected"),
            ("Taken IN (2024-06)", "2 not selected"),
        ] {
            let found = lines(&mut conn, &ids, text);
            assert!(
                found.contains(&format!("  {spans}: {directions}")),
                "{text}: {found:?}"
            );
        }
        // A finer value is placed, and a coarser one not holding the period is too.
        assert!(lines(&mut conn, &ids, "Taken < 2025-06").is_empty());
        assert!(lines(&mut conn, &ids, "Taken < 2024-01").is_empty());
    }

    #[test]
    fn an_object_has_no_value_only_when_none_of_its_selected_sources_has_one() {
        let mut conn = crate::core::testing::setup_test_db();
        let ids = library(&conn);
        let (selected, marks) = marks(&mut conn, &ids, "Make != Nikon");
        // Selected: Canon (0), Canon (2), and the three with no Make (3, 4, 5).
        assert_eq!(selected, [ids[0], ids[2], ids[3], ids[4], ids[5]]);
        let objects = vec![
            vec![ids[0], ids[3]], // one selected source has a Make
            vec![ids[3], ids[4]], // none has
            vec![ids[5]],         // none has
            vec![ids[2]],         // has
        ];
        let acted: Vec<i64> = objects.iter().flatten().copied().collect();
        let notes = SelectionNotes::of_objects(Some(&marks), &objects, &acted);
        assert_eq!(notes.lines(), ["  of which 2 objects have no Make"]);
    }

    /// What the filter selected and the command will not act on is in neither direction.
    #[test]
    fn the_spanning_count_reads_selected_as_acted_on() {
        let mut conn = crate::core::testing::setup_test_db();
        let ids = library(&conn);
        let (selected, marks) = marks(&mut conn, &ids, "Taken != 2024-06");
        assert_eq!(selected, ids);
        // The command acts on one of the two spanning sources.
        let acted: Vec<i64> = selected
            .iter()
            .copied()
            .filter(|id| *id != ids[3])
            .collect();
        let notes = SelectionNotes::of(Some(&marks), &acted, NoteUnit::Sources);
        assert_eq!(
            notes.lines(),
            [
                "  of which 2 have no Taken",
                "  1 source has a time that spans 2024-06 (coarser than a month): 1 selected"
            ]
        );
    }

    #[test]
    fn one_key_in_two_spellings_is_one_line() {
        let mut conn = crate::core::testing::setup_test_db();
        let ids = library(&conn);
        assert_eq!(
            lines(
                &mut conn,
                &ids,
                "Make != Canon AND NOT content.Make = Nikon"
            ),
            ["  of which 3 have no Make"]
        );
        // A modifier makes another key.
        assert_eq!(
            lines(&mut conn, &ids, "Taken != 2024-06 AND Taken|year != 2023").len(),
            3
        );
    }

    #[test]
    fn the_lines_read_as_drafted() {
        let no_value = |key: &str, count| NoValueCount {
            key: key.to_string(),
            count,
        };
        let span = |period: &str, precision, not_selected, selected| SpanCount {
            key: "Taken".to_string(),
            period: period.to_string(),
            precision,
            not_selected,
            selected,
        };
        let notes = |no: Vec<NoValueCount>, spans: Vec<SpanCount>, unit| {
            SelectionNotes {
                no_value: no,
                spans,
                unit,
            }
            .lines()
        };
        assert_eq!(
            notes(
                vec![no_value("content.Make", 312), no_value("Make", 1)],
                vec![],
                NoteUnit::Sources
            ),
            [
                "  of which 312 have no content.Make",
                "  of which 1 has no Make"
            ]
        );
        assert_eq!(
            notes(
                vec![no_value("content.Make", 290), no_value("Make", 1)],
                vec![],
                NoteUnit::Objects
            ),
            [
                "  of which 290 objects have no content.Make",
                "  of which 1 object has no Make"
            ]
        );
        assert_eq!(
            notes(
                vec![no_value("content.Make", 312)],
                vec![],
                NoteUnit::OfMatching(2_140)
            ),
            ["  from 2,140 matching sources, 312 of which have no content.Make"]
        );
        assert_eq!(
            notes(
                vec![no_value(r#""content.File Size""#, 1)],
                vec![],
                NoteUnit::OfMatching(1)
            ),
            [r#"  from 1 matching source, 1 of which has no "content.File Size""#]
        );
        assert_eq!(
            notes(
                vec![],
                vec![
                    span("2024-06", Precision::Month, 9, 3),
                    span(r#""2024-06-15 14:30""#, Precision::Minute, 0, 1),
                    span("2024", Precision::Year, 1_200, 0),
                    span("2025", Precision::Year, 0, 0),
                ],
                NoteUnit::Objects
            ),
            [
                "  12 sources have a time that spans 2024-06 (coarser than a month): 9 not selected, 3 selected",
                r#"  1 source has a time that spans "2024-06-15 14:30" (coarser than a minute): 1 selected"#,
                "  1,200 sources have a time that spans 2024 (coarser than a year): 1,200 not selected",
            ]
        );
        // The typed period prints as the printing rule writes it.
        let period = |text: &str, time_of_day| {
            let tv = crate::core::domain::time::reading::read_time(text).unwrap();
            printed_period(&tv.on_clock(&utc()), time_of_day)
        };
        assert_eq!(period("2024-06", false), "2024-06");
        assert_eq!(period("2024-06-15 14:30", false), r#""2024-06-15 14:30""#);
    }

    /// **The law's verifier**: each count, computed a second way from real selections. "Of
    /// which N have no k" is what `NOT k?` selects among the acted-on sources; a spanning
    /// count is an explicit selection of the values on neither side of each comparison, among
    /// the acted-on sources and among what the filter did not select.
    #[test]
    fn the_notes_count_what_the_selection_decided() {
        let mut conn = crate::core::testing::setup_test_db();
        let mut ids = library(&conn);
        // More shapes: a second year-only and month-only value, and a Make on a dated source.
        let root = insert_root(&conn, "/more", "source", false);
        for (i, (make, taken)) in [("Sony", "2024"), ("Canon", "2024-06"), ("Leica", "2023")]
            .into_iter()
            .enumerate()
        {
            let id = insert_source(&conn, root, &format!("{i}.jpg"), None);
            insert_fact(&conn, id, "content.Make", make);
            insert_time_fact(&conn, id, "content.Taken", taken);
            ids.push(id);
        }
        // Each filter, with the negated keys it holds and its time comparisons as the period
        // table's two opposite sides.
        type Case<'a> = (&'a str, &'a [&'a str], &'a [(&'a str, &'a str)]);
        let cases: [Case; 5] = [
            ("Make != Canon", &["Make"], &[]),
            (
                "Taken < 2024-06 OR Make NOT IN (Nikon)",
                &["Make"],
                &[("Taken < 2024-06", "Taken >= 2024-06")],
            ),
            (
                "NOT Taken >= 2024-06 AND NOT Make ~ Ni*",
                &["Taken", "Make"],
                &[("Taken < 2024-06", "Taken >= 2024-06")],
            ),
            (
                "Taken != 2024-03",
                &["Taken"],
                &[("Taken = 2024-03", "(Taken < 2024-03 OR Taken > 2024-03)")],
            ),
            (
                "Taken > 2024-06 AND NOT (Make = Canon AND NOT Taken|year = 2024)",
                &["Make"],
                &[("Taken > 2024-06", "Taken <= 2024-06")],
            ),
        ];
        for (text, negated, comparisons) in cases {
            let (selected, marks) = marks(&mut conn, &ids, text);
            // A command acting on less than the filter selected: every other selected source.
            let acted: Vec<i64> = selected.iter().copied().step_by(2).collect();
            let unselected: Vec<i64> = ids
                .iter()
                .copied()
                .filter(|id| !selected.contains(id))
                .collect();

            let counted = marks.no_value(&acted);
            assert_eq!(counted.len(), negated.len(), "{text}: {counted:?}");
            for (count, key) in counted.iter().zip(negated) {
                assert_eq!(count.key, *key, "{text}");
                let second = select(&mut conn, &acted, &format!("NOT {key}?")).len();
                assert_eq!(count.count, second, "{text}: no {key}");
            }

            let spans = marks.spans(&acted);
            assert_eq!(spans.len(), comparisons.len(), "{text}: {spans:?}");
            for (span, (side, other)) in spans.iter().zip(comparisons) {
                let neither = format!("Taken? AND NOT {side} AND NOT {other}");
                assert_eq!(
                    span.selected,
                    select(&mut conn, &acted, &neither).len(),
                    "{text}: selected"
                );
                assert_eq!(
                    span.not_selected,
                    select(&mut conn, &unselected, &neither).len(),
                    "{text}: not selected"
                );
            }
        }
    }
}
