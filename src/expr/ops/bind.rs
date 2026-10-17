//! **Binding, applied** — the binding law's owner: `read_filters` reads every `--where`
//! string a command was given, reads the types of the stored keys they name once from all of
//! each key's facts, and binds each clause through the pure binder. What comes back is
//! either [`BoundFilters`] — the only thing [`super::filter::apply_filters`] accepts, so no
//! command can run a filter that was not bound — or a [`FilterRefused`] naming every cause,
//! before any source is looked at. `read_report` is the same reading for everything a `facts`
//! report names — its key, its grouping keys and its filters, every cause in one refusal.
//! Reads only: no zone, no scope.

use std::collections::HashMap;

use anyhow::Result;
use rusqlite::Connection;

use crate::core::domain::fact::KeyTypes;
use crate::core::repo::fact::key_types;
use crate::expr::domain::bind::{
    bind_expr, bind_key, keep_ways_that_bind, known_type, stored_keys, BindCause, Bound, BoundKey,
    Entry, FilterRefused, Reads, Refused,
};
use crate::expr::domain::filter::{Expr, UsedStatus, WrittenKey};
use crate::expr::domain::key::ParsedFactKey;

/// Filters read and bound: every clause's key checked and every typed value read as its
/// key's type. Made only by [`read_filters`].
#[derive(Debug, Clone)]
pub struct BoundFilters {
    /// Each `--where` string's bound tree; several are combined with AND.
    pub(super) trees: Vec<Bound>,
    pub(super) used_status: UsedStatus,
}

impl BoundFilters {
    /// No filter: every source in scope — for a test. A command with no `--where` reads its
    /// empty list through [`read_filters`] like any other.
    #[cfg(test)]
    pub fn none() -> Self {
        BoundFilters {
            trees: Vec::new(),
            used_status: UsedStatus::default(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.trees.is_empty()
    }
}

/// **Read and bind a command's `--where` strings**, as given: `--where` arrives alias-expanded,
/// a manifest's recorded query as written. Each string is parsed on
/// its own, so a parse error is one cause and the other strings still bind; every cause in
/// the run is reported together, through a [`FilterRefused`] carried in the error.
pub fn read_filters(conn: &Connection, written: &[String]) -> Result<BoundFilters> {
    bind_filters(conn, written)?.map_err(|causes| {
        FilterRefused {
            refused: Refused::Filter,
            causes,
        }
        .into()
    })
}

/// What a report reads: its key, its grouping keys and its filters.
pub struct ReportReading {
    pub key: Option<ParsedFactKey>,
    pub grouping: Vec<ParsedFactKey>,
    pub filters: BoundFilters,
}

/// **Read everything a report names** — `facts --key`, each `--group-by` and the `--where`
/// strings — through the one key reader, the modifier table and binding, with every cause
/// in one refusal: a wrong `--group-by` never hides a wrong `--where`. A refusal holding a
/// filter's cause carries the filter's header, one of keys alone the key's.
pub fn read_report(
    conn: &Connection,
    key: Option<&str>,
    grouping: &[String],
    filters: &[String],
) -> Result<ReportReading> {
    let mut causes = Vec::new();
    let mut read = |written: &str| -> Result<Option<ParsedFactKey>> {
        Ok(match bind_report_key(conn, written)? {
            Ok(key) => Some(key),
            Err(cause) => {
                causes.push(cause);
                None
            }
        })
    };
    let key = key.map(&mut read).transpose()?.flatten();
    let mut grouping_keys = Vec::new();
    for written in grouping {
        grouping_keys.extend(read(written)?);
    }
    match bind_filters(conn, filters)? {
        Ok(bound) if causes.is_empty() => Ok(ReportReading {
            key,
            grouping: grouping_keys,
            filters: bound,
        }),
        Ok(_) => Err(FilterRefused {
            refused: Refused::Key,
            causes,
        }
        .into()),
        Err(filter_causes) => {
            causes.extend(filter_causes);
            Err(FilterRefused {
                refused: Refused::Filter,
                causes,
            }
            .into())
        }
    }
}

/// Bind `--where` strings: the bound filters, or every cause in the order typed. Every string
/// is parsed first, so the stored keys' types are read once.
fn bind_filters(
    conn: &Connection,
    written: &[String],
) -> Result<std::result::Result<BoundFilters, Vec<BindCause>>> {
    let parsed: Vec<(&String, Result<Expr>)> =
        written.iter().map(|t| (t, Expr::parse(t))).collect();
    let mut keys = Vec::new();
    for (_, expr) in &parsed {
        if let Ok(expr) = expr {
            stored_keys(expr, &mut keys);
        }
    }
    let names: Vec<&str> = keys.iter().map(String::as_str).collect();
    let types = key_types(conn, &names)?;
    let mut causes = Vec::new();
    let mut trees = Vec::new();
    for (text, expr) in &parsed {
        match expr {
            Ok(expr) => trees.push(bind_expr(expr, &types, &mut causes)),
            Err(error) => causes.push(BindCause::Unparsed(Entry::unparsed(text, error))),
        }
    }
    if !causes.is_empty() {
        return Ok(Err(causes));
    }
    let mut used_status = UsedStatus::default();
    for tree in &trees {
        tree.note_status(&mut used_status);
    }
    Ok(Ok(BoundFilters { trees, used_status }))
}

/// **Read and check one key a report names**, through the one key reader and the modifier
/// table: a modifier that cannot apply to the key's type is its cause, and so is any modifier
/// on a key of two types (a bare one reports both types' rows). A key with no facts reads as
/// itself — there is nothing to report.
fn bind_report_key(
    conn: &Connection,
    written: &str,
) -> Result<std::result::Result<ParsedFactKey, BindCause>> {
    let base = match WrittenKey::read(written) {
        Ok(key) => key.base,
        Err(error) => return Ok(Err(BindCause::Unparsed(Entry::unparsed(written, &error)))),
    };
    let types = if known_type(&base).is_none() {
        key_types(conn, &[base.as_str()])?
    } else {
        Default::default()
    };
    Ok(match read_as_report(written, &types) {
        Ok((key, bound)) => Ok(ParsedFactKey {
            raw: written.to_string(),
            base_key: key.base,
            accessor: key.accessor,
            modifiers: key.modifiers,
            key: key.key,
            read_as: bound.read_as,
        }),
        Err(mut cause) => {
            // A report's way out rewrites the key, so it is read the same way.
            keep_ways_that_bind(&mut cause, &|rewrite| {
                read_as_report(rewrite, &types).is_ok()
            });
            Err(cause)
        }
    })
}

/// Read a written key and bind it as a report reads it: the one reading the report's own
/// key and every way out offered for it go through.
fn read_as_report(
    written: &str,
    types: &HashMap<String, KeyTypes>,
) -> std::result::Result<(WrittenKey, BoundKey), BindCause> {
    let key = WrittenKey::read(written)
        .map_err(|error| BindCause::Unparsed(Entry::unparsed(written, &error)))?;
    let (bound, _) = bind_key(&key, written, Reads::Report, types)?;
    Ok((key, bound))
}

/// One `--where` string read and bound, for a test: it must bind.
#[cfg(test)]
pub(crate) fn bound(conn: &Connection, filter: &str) -> BoundFilters {
    read_filters(conn, &[filter.to_string()]).unwrap_or_else(|e| panic!("{filter}: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::domain::time::zone::UserZone;
    use crate::core::testing::{
        insert_fact, insert_root, insert_source, insert_time_fact, setup_test_db,
    };
    use crate::expr::ops::filter::apply_filters;

    /// The content hash's type (text) is known before any fact of it exists, so a comparison
    /// on it binds on a database that holds none — `hash` too, which is read as it — where
    /// any other key with no facts is refused as unknown.
    #[test]
    fn the_content_hash_binds_with_no_stored_rows() {
        let conn = setup_test_db();
        for filter in ["content.hash.sha256 = abc123", "hash = abc123"] {
            bound(&conn, filter);
        }
        let other = refusal(&conn, &["content.Make = abc123"]);
        assert_eq!(kind(&other.causes[0]), "unknown key");
    }

    /// A `source.` name that is no source fact names no key, so it is refused as unknown in
    /// every position a filter or a report reads a key — compared, `?`, `NOT …?`, `--key`,
    /// `--group-by` — with one sentence. The defect it names: read as a key with no facts,
    /// `NOT source.extension?` selected every source. A source fact still binds for existence.
    #[test]
    fn a_source_name_that_is_no_source_fact_is_an_unknown_key_wherever_it_stands() {
        let (conn, _) = world();
        let sentence =
            "Unknown fact key: 'source.extension'. Use 'canon facts' to see available keys.";
        for filter in [
            "source.extension = jpg",
            "source.extension?",
            "NOT source.extension?",
            "source.extension|lowercase?",
        ] {
            let refused = refusal(&conn, &[filter]);
            let kinds: Vec<&str> = refused.causes.iter().map(kind).collect();
            assert_eq!(kinds, ["unknown key"], "{filter}");
            assert!(
                refused.to_string().contains(sentence),
                "{filter}: {refused}"
            );
        }
        let reports: [(Option<&str>, &[String]); 2] = [
            (Some("source.extension"), &[]),
            (None, &["source.extension".to_string()]),
        ];
        for (key, grouping) in reports {
            let Err(error) = read_report(&conn, key, grouping, &[]) else {
                panic!("{key:?} {grouping:?}: binds");
            };
            let refused = error.downcast::<FilterRefused>().expect("a refusal");
            let kinds: Vec<&str> = refused.causes.iter().map(kind).collect();
            assert_eq!(kinds, ["unknown key"], "{key:?} {grouping:?}");
            assert!(refused.to_string().contains(sentence), "{refused}");
        }
        for filter in ["source.size?", "NOT source.size?"] {
            bound(&conn, filter);
        }
    }

    /// One report key, read as `facts --key` reads it.
    fn read_key(conn: &Connection, key: &str) -> Result<ParsedFactKey> {
        Ok(read_report(conn, Some(key), &[], &[])?
            .key
            .expect("a key was named"))
    }

    fn insert_number_fact(conn: &Connection, source_id: i64, key: &str, n: f64) {
        conn.execute(
            "INSERT INTO facts (entity_type, entity_id, key, value_num, observed_at, observed_basis_rev)
             VALUES ('source', ?, ?, ?, 0, 0)",
            rusqlite::params![source_id, key, n],
        )
        .unwrap();
    }

    /// One source per key type a refusal names, and a key of two types.
    fn world() -> (Connection, i64) {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let a = insert_source(&conn, root, "2019/a.jpg", None);
        let b = insert_source(&conn, root, "misc/b.jpg", None);
        insert_time_fact(&conn, a, "content.taken", "2024-06-15T14:30:05.45");
        insert_fact(&conn, a, "content.numtext", "10");
        insert_fact(&conn, a, "content.textdate", "2024:07:23 11:06:32");
        insert_number_fact(&conn, a, "content.File Size", 41_220.0);
        insert_fact(&conn, b, "content.File Size", "380 KB");
        (conn, a)
    }

    fn refusal(conn: &Connection, filters: &[&str]) -> FilterRefused {
        let written: Vec<String> = filters.iter().map(|f| f.to_string()).collect();
        let error = read_filters(conn, &written).expect_err("refused");
        error
            .downcast::<FilterRefused>()
            .unwrap_or_else(|e| panic!("{filters:?}: not a refusal: {e}"))
    }

    fn kind(cause: &BindCause) -> &'static str {
        match cause {
            BindCause::Unparsed(_) => "unparsed",
            BindCause::UnknownKey(_) => "unknown key",
            BindCause::Unreadable(_) => "unreadable",
            BindCause::TypeMismatch(_) => "type mismatch",
            BindCause::OutOfRange(_) => "out of range",
            BindCause::WrongModifier { .. } => "wrong modifier",
            BindCause::GlobNotText(_) => "glob not text",
            BindCause::NotABucketLabel(_) => "not a bucket label",
            BindCause::BucketOrdered(_) => "bucket ordered",
            BindCause::TooFine(_) => "too fine",
            BindCause::MixedType { .. } => "two types",
            BindCause::TypeChanged { .. } => "type changed",
        }
    }

    /// **The binding law's pin**: every kind of value that cannot mean anything for its key,
    /// each refused when the filter is read, before any source is looked at; `NOT` changes
    /// nothing. A typed fraction of a second is not among them — it binds as that point,
    /// asserted below. It fails if a value that cannot be read binds as a test no value meets.
    #[test]
    fn a_value_that_cannot_mean_anything_is_refused_before_selection() {
        let (conn, _) = world();
        let table = [
            ("source.size=garbage", "unreadable"),
            ("NOT source.size=garbage", "unreadable"),
            ("source.size IN (1024, garbage)", "unreadable"),
            ("source.mtime>=yesterday", "unreadable"),
            (r#"source.mtime>="Sep 26 2026""#, "unreadable"),
            ("taken=2024-02-30", "unreadable"),
            ("taken=2024-13", "unreadable"),
            ("source.mtime=1704067200", "unreadable"),
            ("source.mtime=24", "unreadable"),
            ("source.mtime|month=13", "out of range"),
            ("source.mtime|weekday=0", "out of range"),
            ("source.size|year=2024", "wrong modifier"),
            ("source.mtime|lowercase=x", "wrong modifier"),
            ("source.size~1*", "glob not text"),
            ("source.mtime!~2024*", "glob not text"),
            ("source.size|bucket(60,300)=61-299", "not a bucket label"),
            ("numtext<10", "type mismatch"),
            ("source.rel_path[0]=2019", "type mismatch"),
            ("stem=18:00", "type mismatch"),
            ("textdate>=2024-06", "type mismatch"),
            ("numtext=.5", "unreadable"),
            ("numtext=1-2", "unreadable"),
            ("stem=5.", "unreadable"),
            (r#""content.File Size">1000"#, "two types"),
        ];
        for (filter, expected) in table {
            let refused = refusal(&conn, &[filter]);
            let kinds: Vec<&str> = refused.causes.iter().map(kind).collect();
            assert_eq!(kinds, [expected], "{filter}");
            assert_eq!(refused.is_answer(), expected == "two types", "{filter}");
        }
        // The typed fraction binds, and selects that point.
        let (mut conn, a) = world();
        let filters = read_filters(&conn, &[r#"taken="2024-06-15 14:30:05.45""#.to_string()])
            .expect("a typed fraction binds");
        let selected = apply_filters(&mut conn, &[a], &filters, &UserZone::utc()).unwrap();
        assert_eq!(selected.source_ids, vec![a]);
    }

    /// Every cause in one run, several `--where` strings read as one filter: a parse error is
    /// one cause, and the strings beside it still bind.
    #[test]
    fn every_cause_in_one_run_is_reported_together() {
        let (conn, _) = world();
        let refused = refusal(
            &conn,
            &[
                "source.size=1 source.size=2",
                "content.nothing=1 AND source.ext=jpg",
                r#"title="abc"#,
                "source.size=garbage OR taken=yesterday",
                "source.size|nonsense=1",
            ],
        );
        let kinds: Vec<&str> = refused.causes.iter().map(kind).collect();
        assert_eq!(
            kinds,
            [
                "unparsed",
                "unknown key",
                "unparsed",
                "unreadable",
                "unreadable",
                "unparsed"
            ]
        );
        let text = refused.to_string();
        assert!(text.contains("Unterminated string"), "{text}");
        assert!(text.contains("Unknown modifier: 'nonsense'"), "{text}");
        assert!(
            text.contains("content.nothing=1: Unknown fact key: 'content.nothing'"),
            "{text}"
        );

        // A key that cannot be read is one clause's cause; the clauses beside it in the same
        // string are still read, and each entry names its own clause.
        let refused = refusal(
            &conn,
            &["source.size|nonsense=1 AND source.size=garbage AND numtext<10"],
        );
        let kinds: Vec<&str> = refused.causes.iter().map(kind).collect();
        assert_eq!(kinds, ["unparsed", "unreadable", "type mismatch"]);
        // A status word asked with an operator is likewise its own clause's cause.
        let refused = refusal(&conn, &["source.size|nonsense=1 AND archived=1"]);
        let text = refused.to_string();
        assert_eq!(refused.causes.len(), 2, "{text}");
        assert!(
            text.contains("\n  archived=1: 'archived' is a status predicate"),
            "{text}"
        );
        let text = refused.to_string();
        assert!(
            text.contains("\n  source.size|nonsense=1: Unknown modifier: 'nonsense'"),
            "{text}"
        );
        // `?` followed by a value is its clause's cause too, named in words, for a status word
        // and a key alike, and with every comparison form.
        for (clause, said) in [
            (
                "archived?=1",
                "'archived?' is complete on its own; a status word takes no value",
            ),
            (
                "source.ext?=jpg",
                "'source.ext?' is complete on its own; '?' takes no value",
            ),
            (
                "source.ext?!=jpg",
                "'source.ext?' is complete on its own; '?' takes no value",
            ),
            (
                "source.ext? IN (jpg)",
                "'source.ext?' is complete on its own; '?' takes no value",
            ),
            (
                "source.ext? NOT IN (jpg)",
                "'source.ext?' is complete on its own; '?' takes no value",
            ),
            (
                "archived? 1",
                "'archived?' is complete on its own; a status word takes no value",
            ),
            (
                "source.ext? 1",
                "'source.ext?' is complete on its own; '?' takes no value",
            ),
            (
                "source.ext? jpg",
                "'source.ext?' is complete on its own; '?' takes no value",
            ),
            (
                "archived? \"x\"",
                "'archived?' is complete on its own; a status word takes no value",
            ),
        ] {
            let refused = refusal(&conn, &[&format!("{clause} AND numtext<10")]);
            let text = refused.to_string();
            assert_eq!(refused.causes.len(), 2, "{text}");
            assert!(text.contains(&format!("\n  {clause}: {said}")), "{text}");
        }
        // An alias left unexpanded where a key belongs — a manifest's line, or one inside an
        // alias's own value — is named, in words true at either door; an `@` in a value is text.
        let text = refusal(&conn, &["@image OR source.ext = jpg"]).to_string();
        assert!(
            text.contains(
                "'@image' is an alias, and aliases are not expanded here; write the filter it \
                 stands for"
            ),
            "{text}"
        );
        assert!(read_filters(&conn, &["source.rel_path ~ *@2x*".to_string()]).is_ok());
        // A bare `?` followed by another clause is still the whole question.
        assert!(read_filters(&conn, &["archived? AND source.ext?".to_string()]).is_ok());
    }

    /// A key's type is read from all of its facts — parked roots, files no longer present,
    /// content no present file holds — so a filter means the same whatever is in view.
    /// It fails if a key's type is read only from present facts on open roots.
    #[test]
    fn a_key_binds_the_same_whatever_is_parked_or_gone() {
        let mut conn = setup_test_db();
        let open = insert_root(&conn, "/open", "source", false);
        let parked = insert_root(&conn, "/parked", "source", true);
        let in_view = insert_source(&conn, open, "a.jpg", None);
        let behind_the_door = insert_source(&conn, parked, "b.jpg", None);
        let gone = insert_source(&conn, open, "c.jpg", None);
        conn.execute("UPDATE sources SET present = 0 WHERE id = ?", [gone])
            .unwrap();
        insert_time_fact(&conn, behind_the_door, "content.parked", "2023-05-01");
        insert_time_fact(&conn, gone, "content.gone", "2023-05-01");

        for key in ["parked", "gone"] {
            let refused = refusal(&conn, &[&format!("{key}=garbage")]);
            assert_eq!(
                refused.causes.iter().map(kind).collect::<Vec<_>>(),
                ["unreadable"],
                "{key}"
            );
            for (filter, expected) in [
                (format!("{key}=2024"), Vec::<i64>::new()),
                (format!("NOT {key}=2024"), vec![in_view]),
                (format!("{key}|year=2024"), vec![]),
            ] {
                let bound = read_filters(&conn, std::slice::from_ref(&filter))
                    .unwrap_or_else(|e| panic!("{filter}: {e}"));
                let selected =
                    apply_filters(&mut conn, &[in_view], &bound, &UserZone::utc()).unwrap();
                assert_eq!(selected.source_ids, expected, "{filter}");
            }
        }
    }

    /// `facts --key` and `--group-by` read their key through the one key reader and the
    /// modifier table: a wrong-type modifier is refused (no "skipped N values" report), a
    /// modifier on a key of two types is refused as an answer, and the bare key runs.
    #[test]
    fn a_report_key_is_read_and_checked_like_a_filter_key() {
        let (conn, _) = world();
        let refused = |key: &str| {
            read_key(&conn, key)
                .expect_err(key)
                .downcast::<FilterRefused>()
                .unwrap()
        };
        let bucket_on_text = refused("numtext|bucket");
        assert_eq!(
            bucket_on_text.causes.iter().map(kind).collect::<Vec<_>>(),
            ["wrong modifier"]
        );
        assert!(bucket_on_text
            .to_string()
            .starts_with("cannot use the key as written\n"));
        let two_types = refused(r#""content.File Size"|number"#);
        assert!(two_types.is_answer());
        assert!(
            two_types
                .to_string()
                .contains("a modifier works on one type only"),
            "{two_types}"
        );
        assert!(!refused("source.size|nonsense").is_answer());
        // A report's way out is bound as a key: one refused again is not offered, and the
        // modifiers that apply are named instead.
        let tail = refused("source.size|lowercase|bucket").to_string();
        assert!(!tail.contains("|number|lowercase|bucket"), "{tail}");
        assert!(tail.contains("Modifiers for numbers"), "{tail}");
        assert!(refused("source.size|lowercase")
            .to_string()
            .contains("To use its printed form as text: source.size|number|lowercase"));

        let read = read_key(&conn, r#""content.File Size""#).unwrap();
        assert_eq!(read.base_key, "content.File Size");
        let read = read_key(&conn, "source.mtime|weekday|lowercase").unwrap();
        assert_eq!(read.modifiers.len(), 2);
        assert!(read_key(&conn, "numtext|number|bucket(5,10)").is_ok());

        // One run reports every cause across the key, the grouping keys and the filters.
        let all = read_report(
            &conn,
            Some("source.mtime|nonsense"),
            &["source.size|bogus".to_string()],
            &["numtext<10".to_string()],
        )
        .err()
        .unwrap()
        .downcast::<FilterRefused>()
        .unwrap();
        assert_eq!(
            all.causes.iter().map(kind).collect::<Vec<_>>(),
            ["unparsed", "unparsed", "type mismatch"]
        );
        assert!(all
            .to_string()
            .starts_with("cannot run the filter as written\n"));
        assert!(read_key(&conn, "content.nothing|year").is_ok());
    }
}
