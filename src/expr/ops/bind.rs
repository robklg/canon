//! **Binding, applied** — the binding law's owner: `read_filters` reads every `--where`
//! string a command was given ([`ExpressionText`]) through the reader, its aliases from the
//! aliases file the first time a use is met, reads the types of the stored keys they name
//! once from all of each key's facts, and binds each condition through the pure binder. What
//! comes back is either [`BoundFilters`] — the only thing [`super::filter::apply_filters`]
//! accepts, so no command can run a filter that was not bound — or an [`ExpressionRefused`]
//! naming every problem the reader met and every cause binding found, before any source is
//! looked at. `read_report` is the same reading for everything a `facts` report names — its
//! key, its grouping keys and its filters, every cause in one refusal. Reads only — the
//! database, and the aliases file where an alias is used: no zone, no scope.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

use anyhow::Result;
use rusqlite::Connection;

use crate::core::domain::fact::KeyTypes;
use crate::core::repo::fact::key_types;
use crate::expr::domain::alias::{recorded_text, CheckedAliases};
use crate::expr::domain::bind::{
    bind_expr, bind_key, keep_fix_that_binds, keep_quoted_name_with_facts, known_type,
    quoted_name_key, stored_keys, BindCause, Bound, BoundKey, ExpressionRefused, Reads, Refused,
};
use crate::expr::domain::filter::UsedStatus;
use crate::expr::domain::key::{InputKey, ParsedFactKey};
use crate::expr::domain::problem::Problem;
use crate::expr::domain::read::{read_filter, read_keys, AliasMode, AliasUse, Grammar, Reading};
use crate::expr::ops::alias::{aliases_file, load_aliases};

/// **A command's expression texts**: each `--where` string as given on the command line, or a
/// manifest's recorded query as specified, with where an alias in them is read from. The one
/// thing a command hands the facility to read its filters by; a report's `--key` and `--group-by`
/// texts, typed on the same command line, are read with its filters' aliases.
#[derive(Debug, Clone)]
pub struct ExpressionText {
    texts: Vec<String>,
    aliases: AliasSource,
}

/// Where an `@name` in an expression text is read from.
#[derive(Debug, Clone)]
enum AliasSource {
    /// Given on the command line: the aliases file at this path, read the first time a use is
    /// met.
    File(PathBuf),
    /// A manifest's recorded query, which takes no alias.
    NotAllowed,
}

impl ExpressionText {
    /// Texts given on the command line, their aliases in the aliases file of `canon_home`.
    pub fn given(texts: Vec<String>, canon_home: &Path) -> Self {
        ExpressionText {
            texts,
            aliases: AliasSource::File(aliases_file(canon_home)),
        }
    }

    /// A manifest's recorded query, as specified: no alias stands in it.
    pub fn specified(texts: Vec<String>) -> Self {
        ExpressionText {
            texts,
            aliases: AliasSource::NotAllowed,
        }
    }

    /// The texts as typed.
    pub fn typed(&self) -> &[String] {
        &self.texts
    }

    /// Texts given with no aliases file, for a test: an alias in them is unknown.
    #[cfg(test)]
    pub fn plain(texts: &[&str]) -> Self {
        ExpressionText {
            texts: texts.iter().map(|t| t.to_string()).collect(),
            aliases: AliasSource::File(PathBuf::new()),
        }
    }
}

/// Filters read and bound: every condition's key checked and every typed value read as its
/// key's type. Made only by [`read_filters`] and [`read_report`], and only where nothing was
/// refused.
#[derive(Debug, Clone)]
pub struct BoundFilters {
    /// Each `--where` string's bound tree; several are combined with AND.
    pub(super) trees: Vec<Bound>,
    pub(super) used_status: UsedStatus,
    /// Each string as a manifest records it: every alias replaced by what it stands for.
    recorded: Vec<String>,
}

impl BoundFilters {
    /// No filter: every source in scope — for a test. A command with no `--where` reads its
    /// empty list through [`read_filters`] like any other.
    #[cfg(test)]
    pub fn none() -> Self {
        BoundFilters {
            trees: Vec::new(),
            used_status: UsedStatus::default(),
            recorded: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.trees.is_empty()
    }

    /// **The recorded text**: each string as read, every alias use replaced by what it stands
    /// for ([`recorded_text`]) — built in the reading that bound it, so what a
    /// manifest records selects what this run selected.
    pub fn recorded(&self) -> &[String] {
        &self.recorded
    }
}

/// **How one run reads its texts' aliases**: not at all (a manifest's query), or from the
/// aliases file, loaded the first time a text uses an alias, that text then read again with it.
/// It holds the run's typed texts, and each use takes the next of the refusal's texts after
/// them, in the order met: the number a use's reading carries and its text in the refusal are
/// counted here alone.
struct AliasReading {
    state: AliasState,
    typed: Vec<String>,
    uses: Vec<AliasUse>,
}

/// Where a run's aliases stand.
enum AliasState {
    /// A manifest's recorded query, which takes no alias.
    NotAllowed,
    /// Given on the command line: the aliases file at this path, not read yet.
    NotLoaded(PathBuf),
    /// The aliases file, read and checked.
    Loaded(CheckedAliases),
}

impl AliasReading {
    /// The aliases of a run reading `typed`, every text as typed, in the order read.
    fn new(source: &AliasSource, typed: Vec<String>) -> Self {
        let state = match source {
            AliasSource::File(path) => AliasState::NotLoaded(path.clone()),
            AliasSource::NotAllowed => AliasState::NotAllowed,
        };
        AliasReading {
            state,
            typed,
            uses: Vec::new(),
        }
    }

    /// The mode a text, or a fix, is read in now.
    fn mode(&self) -> AliasMode<'_> {
        match &self.state {
            AliasState::NotAllowed => AliasMode::NotAllowed,
            AliasState::NotLoaded(_) => AliasMode::NotLoaded,
            AliasState::Loaded(aliases) => AliasMode::Loaded {
                aliases,
                first_text: self.typed.len() + self.uses.len(),
            },
        }
    }

    /// **Read one typed text** with `read`: where it uses an alias with the file not read yet
    /// (the one mode a reading waits in), the file is loaded and the text read again with it, so
    /// no reading this returns skipped a use. An aliases file that cannot be read, or holds an
    /// entry that does not read, is the error.
    fn read<T>(&mut self, read: impl Fn(AliasMode) -> Reading<T>) -> Result<Reading<T>> {
        let mut reading = read(self.mode());
        if reading.waits_for_aliases {
            if let AliasState::NotLoaded(path) = &self.state {
                self.state = AliasState::Loaded(load_aliases(path)?);
            }
            reading = read(self.mode());
        }
        self.uses.extend_from_slice(&reading.alias_uses);
        Ok(reading)
    }

    /// The refusal of this run's texts: a filter refused in a manifest's query says so, and
    /// every use's alias value follows the typed texts, drawn where the use was typed.
    fn refusal(self, refused: Refused, causes: Vec<BindCause>) -> ExpressionRefused {
        let refused = match (refused, &self.state) {
            (Refused::Filter, AliasState::NotAllowed) => Refused::RecordedQuery,
            (other, _) => other,
        };
        let mut refusal = ExpressionRefused {
            refused,
            texts: self.typed,
            typed_at: BTreeMap::new(),
            causes,
        };
        refusal.push_alias_texts(&self.uses);
        refusal
    }
}

/// **Read and bind a command's `--where` strings**: given on the command line, each alias read
/// from the aliases file, which is loaded only where a use is met; a manifest's recorded query
/// as specified, with no alias. Each string is read on its own, so a problem in one is reported
/// beside the causes of the others; every problem and cause in the run is reported together,
/// through an [`ExpressionRefused`] carried in the error. A string with any problem never runs,
/// though what of it could be read is still bound, so its causes are named too. An aliases file
/// that cannot be read, or holds an entry that does not read, is the error instead.
pub fn read_filters(conn: &Connection, text: &ExpressionText) -> Result<BoundFilters> {
    let mut aliases = AliasReading::new(&text.aliases, text.texts.clone());
    bind_filters(conn, &text.texts, 0, &mut aliases)?
        .map_err(|causes| aliases.refusal(Refused::Filter, causes).into())
}

/// What a report reads: its key, its grouping keys and its filters.
pub struct ReportReading {
    pub key: Option<ParsedFactKey>,
    pub grouping: Vec<ParsedFactKey>,
    pub filters: BoundFilters,
}

/// **Read everything a report names** — `facts --key`, each `--group-by` value (a list of keys)
/// and the `--where` strings — through the reader, the modifier table and binding, with every
/// cause in one refusal: a wrong `--group-by` never hides a wrong `--where`. The texts are
/// numbered in that order, and the key and grouping keys read aliases as the filters do. A
/// refusal holding a filter's cause carries the filter's first line, one of keys alone the
/// key's.
pub fn read_report(
    conn: &Connection,
    key: Option<&str>,
    grouping: &[String],
    filters: &ExpressionText,
) -> Result<ReportReading> {
    let mut typed: Vec<String> = key.iter().map(|text| text.to_string()).collect();
    let first_grouping = typed.len();
    typed.extend(grouping.iter().cloned());
    let first_filter = typed.len();
    typed.extend(filters.texts.iter().cloned());
    let mut aliases = AliasReading::new(&filters.aliases, typed);
    let mut causes = Vec::new();
    let key = match key {
        Some(text) => report_keys(conn, text, 0, Grammar::Key, &mut aliases, &mut causes)?
            .into_iter()
            .next(),
        None => None,
    };
    let mut grouping_keys = Vec::new();
    for (i, text) in grouping.iter().enumerate() {
        grouping_keys.extend(report_keys(
            conn,
            text,
            first_grouping + i,
            Grammar::KeyList,
            &mut aliases,
            &mut causes,
        )?);
    }
    let refused = match bind_filters(conn, &filters.texts, first_filter, &mut aliases)? {
        Ok(bound) if causes.is_empty() => {
            return Ok(ReportReading {
                key,
                grouping: grouping_keys,
                filters: bound,
            })
        }
        Ok(_) => Refused::Key,
        Err(filter_causes) => {
            causes.extend(filter_causes);
            Refused::Filter
        }
    };
    Err(aliases.refusal(refused, causes).into())
}

/// Bind `--where` strings, numbered from `first`, reading aliases with `aliases`: the bound
/// filters, or every problem and cause in the order met. Every string is read first, so the
/// stored keys' types are read once.
fn bind_filters(
    conn: &Connection,
    given: &[String],
    first: usize,
    aliases: &mut AliasReading,
) -> Result<std::result::Result<BoundFilters, Vec<BindCause>>> {
    let mut causes = Vec::new();
    let mut read = Vec::new();
    let mut recorded = Vec::new();
    for (i, text) in given.iter().enumerate() {
        let mut reading = aliases.read(|mode| read_filter(text, first + i, mode))?;
        recorded.push(recorded_text(text, &reading.alias_uses));
        offer_quoted_names(conn, &mut reading.problems)?;
        causes.extend(reading.problems.into_iter().map(BindCause::Read));
        read.extend(reading.value);
    }
    let mut keys = Vec::new();
    for expr in &read {
        stored_keys(expr, &mut keys);
    }
    let names: Vec<&str> = keys.iter().map(String::as_str).collect();
    let types = key_types(conn, &names)?;
    let trees: Vec<Bound> = read
        .iter()
        .map(|expr| bind_expr(expr, &types, aliases.mode(), &mut causes))
        .collect();
    if !causes.is_empty() {
        return Ok(Err(causes));
    }
    let mut used_status = UsedStatus::default();
    for tree in &trees {
        tree.note_status(&mut used_status);
    }
    Ok(Ok(BoundFilters {
        trees,
        used_status,
        recorded,
    }))
}

/// Keep each quoted name the reader offers only where it names a key with facts, the types read
/// from the stored keys it names.
fn offer_quoted_names(conn: &Connection, problems: &mut [Problem]) -> Result<()> {
    let names: Vec<String> = problems
        .iter()
        .filter_map(quoted_name_key)
        .map(|key| key.base)
        .collect();
    let types = if names.is_empty() {
        HashMap::new()
    } else {
        key_types(conn, &names.iter().map(String::as_str).collect::<Vec<_>>())?
    };
    for problem in problems {
        keep_quoted_name_with_facts(problem, &types);
    }
    Ok(())
}

/// Read one report text as `grammar` reads it, its refusal's text number `index`, and bind each
/// key it names: the keys that bind, every problem and cause added to `causes`.
fn report_keys(
    conn: &Connection,
    text: &str,
    index: usize,
    grammar: Grammar,
    aliases: &mut AliasReading,
    causes: &mut Vec<BindCause>,
) -> Result<Vec<ParsedFactKey>> {
    let mut reading = aliases.read(|mode| read_keys(text, index, grammar, mode))?;
    offer_quoted_names(conn, &mut reading.problems)?;
    causes.extend(reading.problems.into_iter().map(BindCause::Read));
    let mut keys = Vec::new();
    for key in reading.value.unwrap_or_default() {
        match bind_report_key(conn, &key, aliases.mode())? {
            Ok(key) => keys.push(key),
            Err(cause) => causes.push(cause),
        }
    }
    Ok(keys)
}

/// **Check one key a report names** against the modifier table: a modifier that cannot apply to
/// the key's type is its cause, and so is any modifier on a key of mixed type (a bare one reports
/// every type's rows). A key with no facts has its chain checked, and reads as itself — there is
/// nothing to report. A fix is read with the report's own aliases.
fn bind_report_key(
    conn: &Connection,
    key: &InputKey,
    aliases: AliasMode,
) -> Result<std::result::Result<ParsedFactKey, BindCause>> {
    let types = if known_type(&key.base).is_none() {
        key_types(conn, &[key.base.as_str()])?
    } else {
        Default::default()
    };
    Ok(match bind_as_report(key, &types) {
        Ok(bound) => Ok(ParsedFactKey {
            raw: bound.input,
            base_key: bound.base,
            accessor: bound.accessor,
            modifiers: bound.modifiers,
            key: key.key.clone(),
            read_as: bound.read_as,
        }),
        Err(mut cause) => {
            // A report's fix rewrites the key, so it is read the same way.
            keep_fix_that_binds(&mut cause, &|rewrite| {
                let reading = read_keys(rewrite, 0, Grammar::Key, aliases);
                reading.problems.is_empty()
                    && matches!(reading.value.as_deref(), Some([key]) if bind_as_report(key, &types).is_ok())
            });
            Err(cause)
        }
    })
}

/// Bind a key as a report reads it: the one reading the report's own key and every fix
/// offered for it go through.
fn bind_as_report(
    key: &InputKey,
    types: &HashMap<String, KeyTypes>,
) -> std::result::Result<BoundKey, BindCause> {
    bind_key(key, &key.input, Reads::Report, types).map(|(bound, _)| bound)
}

/// One `--where` string read and bound, for a test: it must bind.
#[cfg(test)]
pub(crate) fn bound(conn: &Connection, filter: &str) -> BoundFilters {
    read_filters(conn, &ExpressionText::plain(&[filter]))
        .unwrap_or_else(|e| panic!("{filter}: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::domain::time::zone::UserZone;
    use crate::core::testing::{
        insert_fact, insert_number_fact, insert_root, insert_source, insert_time_fact,
        setup_test_db,
    };
    use crate::expr::domain::problem::Fix;
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
        let sentence = "^ `source.extension` is an unknown key\n    `canon facts` lists the keys";
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
            let Err(error) = read_report(&conn, key, grouping, &ExpressionText::plain(&[])) else {
                panic!("{key:?} {grouping:?}: binds");
            };
            let refused = error.downcast::<ExpressionRefused>().expect("a refusal");
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
        Ok(
            read_report(conn, Some(key), &[], &ExpressionText::plain(&[]))?
                .key
                .expect("a key was named"),
        )
    }

    /// One source per key type a refusal names, and a key of mixed type.
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

    fn refusal(conn: &Connection, filters: &[&str]) -> ExpressionRefused {
        let error = read_filters(conn, &ExpressionText::plain(filters)).expect_err("refused");
        error
            .downcast::<ExpressionRefused>()
            .unwrap_or_else(|e| panic!("{filters:?}: not a refusal: {e}"))
    }

    fn kind(cause: &BindCause) -> &'static str {
        match cause {
            BindCause::Read(_) => "read",
            BindCause::UnknownName(_) => "unknown name",
            BindCause::UnknownKey(_) => "unknown key",
            BindCause::Unreadable(_) => "unreadable",
            BindCause::TypeMismatch { .. } => "type mismatch",
            BindCause::OutOfRange(_) => "out of range",
            BindCause::WrongModifier(_) => "wrong modifier",
            BindCause::GlobNotText(_) => "glob not text",
            BindCause::NotABucketLabel(_) => "not a bucket label",
            BindCause::BucketOrdered(_) => "bucket ordered",
            BindCause::TooFine(_) => "too fine",
            BindCause::MixedType(_) => "mixed type",
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
            ("numtext=1e400", "type mismatch"),
            ("source.size=1e-400", "unreadable"),
            ("source.size=1e400", "unreadable"),
            ("source.size=1.0000000000000001", "unreadable"),
            ("source.mtime=14:30", "unreadable"),
            ("content.nothing|year|time?", "wrong modifier"),
            ("NOT content.nothing|bucket|year?", "wrong modifier"),
            ("source.size|bucket|year?", "wrong modifier"),
            (r#""content.File Size">1000"#, "mixed type"),
        ];
        for (filter, expected) in table {
            let refused = refusal(&conn, &[filter]);
            let kinds: Vec<&str> = refused.causes.iter().map(kind).collect();
            assert_eq!(kinds, [expected], "{filter}");
            assert_eq!(refused.is_answer(), expected == "mixed type", "{filter}");
        }
        // A value that reads as no number, time or time of day is text, however it looks.
        for filter in [
            "numtext=.5",
            "numtext=1-2",
            "stem=5.",
            "numtext=2024e",
            "numtext=12:3",
        ] {
            read_filters(&conn, &ExpressionText::plain(&[filter]))
                .unwrap_or_else(|e| panic!("{filter}: {e}"));
        }
        // The typed fraction binds, and selects that point.
        let (mut conn, a) = world();
        let filters = read_filters(
            &conn,
            &ExpressionText::plain(&[r#"taken="2024-06-15 14:30:05.45""#]),
        )
        .expect("a typed fraction binds");
        let selected = apply_filters(&mut conn, &[a], &filters, &UserZone::utc()).unwrap();
        assert_eq!(selected.source_ids, vec![a]);
    }

    /// Each block of a refusal as it is drawn: the typed line, and the sentence under its caret.
    fn blocks(refused: &ExpressionRefused) -> Vec<(String, String)> {
        let lines = refused.render("");
        lines
            .windows(2)
            .filter_map(|pair| {
                let sentence = pair[1].trim_start().strip_prefix("^ ")?;
                Some((pair[0].clone(), sentence.to_string()))
            })
            .collect()
    }

    /// Every cause in one run, several `--where` strings read as one filter: a problem in one
    /// string is reported beside the causes of the others, each under its own typed line, in
    /// order of string and position. The expected sentences are the reader's rows and the
    /// binder's own words.
    #[test]
    fn every_cause_in_one_run_is_reported_together() {
        let (conn, _) = world();
        let texts = [
            "source.size=1 source.size=2",
            "content.nothing=1 AND source.ext=jpg",
            r#"title="abc"#,
            "source.size=garbage OR taken=yesterday",
            "source.size|nonsense=1",
        ];
        let refused = refusal(&conn, &texts);
        let drawn = blocks(&refused);
        let lines: Vec<&str> = drawn.iter().map(|(line, _)| line.as_str()).collect();
        assert_eq!(
            lines,
            [texts[0], texts[1], texts[2], texts[3], texts[3], texts[4]]
        );
        let said: Vec<&str> = drawn.iter().map(|(_, said)| said.as_str()).collect();
        assert_eq!(said[0], "a space separates values");
        assert_eq!(said[1], "`content.nothing` is an unknown key");
        assert_eq!(said[2], "this quote is unclosed");
        assert_eq!(
            said[3],
            "source.size is of type number; `garbage` is not a number"
        );
        assert_eq!(said[4], "taken is of type time; `yesterday` is not a time");
        assert!(
            said[5].starts_with("`|nonsense` is not a modifier — Modifiers: "),
            "{}",
            said[5]
        );

        // A key that cannot be read is one condition's problem; the conditions beside it in the
        // same string are still read and bound, each drawn under the one typed line.
        let refused = refusal(
            &conn,
            &["source.size|nonsense=1 AND source.size=garbage AND numtext<10"],
        );
        let said: Vec<String> = blocks(&refused).into_iter().map(|(_, s)| s).collect();
        assert_eq!(said.len(), 3, "{refused}");
        assert_eq!(
            said[1],
            "source.size is of type number; `garbage` is not a number"
        );
        assert_eq!(said[2], "numtext is of type text, and `10` is a number");
        // A status word asked with an operator is its own condition's problem too.
        let refused = refusal(&conn, &["source.size|nonsense=1 AND archived=1"]);
        let said: Vec<String> = blocks(&refused).into_iter().map(|(_, s)| s).collect();
        assert_eq!(said.len(), 2, "{refused}");
        assert_eq!(said[1], "`archived` can only be used with `?`");
        // After `?` the condition is complete, so what follows it is named by the
        // after-condition row, for a status word and a key alike, and the condition after the
        // next AND is still read.
        for (clause, word) in [
            ("archived?=1", "=1"),
            ("source.ext?=jpg", "=jpg"),
            ("source.ext?!=jpg", "!"),
            ("source.ext? IN (jpg)", "IN"),
            ("source.ext? NOT IN (jpg)", "NOT"),
            ("archived? 1", "1"),
            ("source.ext? 1", "1"),
            ("source.ext? jpg", "jpg"),
            ("archived? \"x\"", "\"x\""),
        ] {
            let refused = refusal(&conn, &[&format!("{clause} AND numtext<10")]);
            let said: Vec<String> = blocks(&refused).into_iter().map(|(_, s)| s).collect();
            assert_eq!(
                said,
                [
                    format!("AND or OR is missing before `{word}`"),
                    "numtext is of type text, and `10` is a number".to_string()
                ],
                "{clause}"
            );
        }
        // After `~` an `@` is part of the glob, and needs quotes there like any character a
        // plain value cannot hold.
        assert!(refusal(&conn, &["source.rel_path ~ *@2x*"])
            .to_string()
            .contains("`@` needs quotes; quote that part, but not * ? [ ]"));
        assert!(read_filters(
            &conn,
            &ExpressionText::plain(&[r#"source.rel_path ~ *"@"2x*"#])
        )
        .is_ok());
        // A bare `?` followed by another condition is still the whole question.
        assert!(read_filters(
            &conn,
            &ExpressionText::plain(&["archived? AND source.ext?"])
        )
        .is_ok());
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
                let bound = read_filters(&conn, &ExpressionText::plain(&[filter.as_str()]))
                    .unwrap_or_else(|e| panic!("{filter}: {e}"));
                let selected =
                    apply_filters(&mut conn, &[in_view], &bound, &UserZone::utc()).unwrap();
                assert_eq!(selected.source_ids, expected, "{filter}");
            }
        }
    }

    /// `facts --key` and `--group-by` read their key through the one key reader and the
    /// modifier table: a wrong-type modifier is refused (no "skipped N values" report), a
    /// modifier on a key of mixed type is refused as an answer, and the bare key runs. A key with
    /// no facts has its chain checked like any other.
    #[test]
    fn a_report_key_is_read_and_checked_like_a_filter_key() {
        let (conn, _) = world();
        let refused = |key: &str| {
            read_key(&conn, key)
                .expect_err(key)
                .downcast::<ExpressionRefused>()
                .unwrap()
        };
        let bucket_on_text = refused("numtext|bucket");
        assert_eq!(
            bucket_on_text.causes.iter().map(kind).collect::<Vec<_>>(),
            ["wrong modifier"]
        );
        assert!(bucket_on_text
            .to_string()
            .starts_with("cannot use the key as given\n"));
        let mixed = refused(r#""content.File Size"|number"#);
        assert!(mixed.is_answer());
        assert!(
            mixed
                .to_string()
                .contains("a modifier supports one type only"),
            "{mixed}"
        );
        assert!(!refused("source.size|nonsense").is_answer());
        // No other key is offered: the modifiers that apply are named.
        let tail = refused("source.size|lowercase|bucket").to_string();
        assert!(!tail.contains("|number|lowercase"), "{tail}");
        assert!(tail.contains("Modifiers for numbers"), "{tail}");

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
            &ExpressionText::plain(&["numtext<10"]),
        )
        .err()
        .unwrap()
        .downcast::<ExpressionRefused>()
        .unwrap();
        assert_eq!(
            all.causes.iter().map(kind).collect::<Vec<_>>(),
            ["read", "read", "type mismatch"]
        );
        assert!(all
            .to_string()
            .starts_with("cannot run the filter as given\n"));
        assert!(read_key(&conn, "content.nothing|year").is_ok());
        assert_eq!(
            refused("content.nothing|year|time")
                .causes
                .iter()
                .map(kind)
                .collect::<Vec<_>>(),
            ["wrong modifier"]
        );
    }

    /// **A text with a problem the reader met never runs**: what of it could be read is still
    /// bound, so its causes are named beside the problem, and the refusal is all that comes back
    /// — never the filters that read, run without the rest. It fails if a partial reading binds
    /// into `BoundFilters`.
    #[test]
    fn a_filter_with_a_reading_problem_never_runs() {
        let (conn, _) = world();
        for filter in [
            "source.size=1 source.size=2",
            "a b AND c?",
            "source.ext=jpg AND",
        ] {
            let Err(error) = read_filters(&conn, &ExpressionText::plain(&[filter])) else {
                panic!("{filter}: ran");
            };
            assert!(error.downcast::<ExpressionRefused>().is_ok(), "{filter}");
            let Err(error) = read_report(&conn, None, &[], &ExpressionText::plain(&[filter]))
            else {
                panic!("{filter}: ran in a report");
            };
            assert!(error.downcast::<ExpressionRefused>().is_ok(), "{filter}");
        }
        // A report key read with a problem never runs either.
        let grouping = ["source.ext,,source.size".to_string()];
        assert!(read_report(
            &conn,
            Some("source.ext"),
            &grouping,
            &ExpressionText::plain(&[])
        )
        .is_err());
    }

    /// Several `--where` strings each echo their own text above their own problems.
    #[test]
    fn several_where_flags_each_echo_their_own_text() {
        let (conn, _) = world();
        let refused = refusal(&conn, &["source.size=1 2", "numtext<10"]);
        let lines: Vec<String> = refused.to_string().lines().map(str::to_string).collect();
        assert_eq!(
            lines[..7],
            [
                "cannot run the filter as given",
                "  source.size=1 2",
                "               ^ a space separates values",
                "    After a value: AND, OR, ) or the end",
                "  numtext<10",
                "          ^ numtext is of type text, and `10` is a number",
                "  Type conversion: in the filter with a modifier such as |number or |year; for \
                 the stored type, `canon facts convert`.",
            ]
        );
    }

    /// The blocks are drawn in order of text, then position in it, whichever of reading or
    /// binding found them. It fails if they are drawn in the order found, where every reading
    /// problem comes before any binding cause.
    #[test]
    fn the_blocks_are_sorted_by_text_then_position() {
        let (conn, _) = world();
        let texts = ["numtext<10 AND source.size=1 2", r#"title="abc"#];
        let drawn = blocks(&refusal(&conn, &texts));
        assert_eq!(
            drawn,
            [
                (
                    texts[0].to_string(),
                    "numtext is of type text, and `10` is a number".to_string()
                ),
                (texts[0].to_string(), "a space separates values".to_string()),
                (texts[1].to_string(), "this quote is unclosed".to_string()),
            ]
        );
    }

    /// One key grammar: a key reads as the same key, accessor and modifiers in a filter and as
    /// a report key.
    #[test]
    fn a_key_reads_the_same_in_a_filter_and_a_report() {
        use crate::expr::domain::filter::Expr;
        for key in [
            "Make",
            "hash",
            "source.rel_path[-1]|stem",
            r#""content.File Size"|bucket(60,300)"#,
            "content.XMP-dc:Subject|lowercase",
            "source.mtime|yearmonth",
        ] {
            let filter = read_filter(&format!("{key}?"), 0, AliasMode::NotLoaded);
            assert!(filter.problems.is_empty(), "{key}");
            let Some(Expr::Exists { key: in_filter, .. }) = filter.value else {
                panic!("{key}: not a condition");
            };
            let report = read_keys(key, 0, Grammar::Key, AliasMode::NotLoaded);
            assert!(report.problems.is_empty(), "{key}");
            let in_report = &report.value.unwrap()[0];
            let shape = |k: &InputKey| {
                (
                    k.base.clone(),
                    k.key.clone(),
                    k.accessor.as_ref().map(|(a, _)| a.clone()),
                    k.modifiers
                        .iter()
                        .map(|(m, _)| m.clone())
                        .collect::<Vec<_>>(),
                )
            };
            assert_eq!(shape(&in_filter), shape(in_report), "{key}");
        }
    }

    /// An unknown key is named as typed, its caret under the name, with the line saying where
    /// the keys are listed.
    #[test]
    fn an_unknown_key_is_named_with_a_caret_under_it() {
        let (conn, _) = world();
        assert_eq!(
            refusal(&conn, &["source.ext=jpg AND Rating=5"]).to_string(),
            [
                "cannot run the filter as given",
                "  source.ext=jpg AND Rating=5",
                "                     ^ `Rating` is an unknown key",
                "    `canon facts` lists the keys",
            ]
            .join("\n")
        );
    }

    /// The first line names what was refused: a filter as given, a key as given — a report
    /// whose filter has a problem takes the filter's line — and a filter a manifest specified.
    #[test]
    fn the_first_line_follows_what_was_refused() {
        let (conn, _) = world();
        let first = |error: anyhow::Error| {
            let refused = error.downcast::<ExpressionRefused>().unwrap();
            refused.to_string().lines().next().unwrap().to_string()
        };
        let filter = read_filters(&conn, &ExpressionText::plain(&["numtext<10"])).unwrap_err();
        assert_eq!(first(filter), "cannot run the filter as given");
        let key = read_report(
            &conn,
            Some("source.size|nonsense"),
            &[],
            &ExpressionText::plain(&[]),
        )
        .err()
        .unwrap();
        assert_eq!(first(key), "cannot use the key as given");
        let list = read_report(
            &conn,
            None,
            &["source.ext,".to_string()],
            &ExpressionText::plain(&[]),
        )
        .err()
        .unwrap();
        assert_eq!(first(list), "cannot use the key as given");
        let both = read_report(
            &conn,
            Some("source.size|nonsense"),
            &[],
            &ExpressionText::plain(&["numtext<10"]),
        )
        .err()
        .unwrap();
        assert_eq!(first(both), "cannot run the filter as given");
        let specified = read_filters(&conn, &ExpressionText::specified(vec!["numtext<10".into()]))
            .expect_err("refused")
            .to_string();
        assert!(
            specified.starts_with("cannot run the filter as specified\n"),
            "{specified}"
        );
    }

    /// `--group-by` is read as a list of keys: a comma inside `|bucket( … )` belongs to the key,
    /// and spaces around a comma are allowed.
    #[test]
    fn group_by_reads_a_two_number_bucket_beside_another_key() {
        let (conn, _) = world();
        let grouping = ["source.ext, source.size|bucket(60,300)".to_string()];
        let read = read_report(
            &conn,
            Some("source.ext"),
            &grouping,
            &ExpressionText::plain(&[]),
        )
        .unwrap();
        let keys: Vec<(&str, Vec<Vec<f64>>)> = read
            .grouping
            .iter()
            .map(|key| {
                let args = key.modifiers.iter().map(|m| m.args.clone()).collect();
                (key.base_key.as_str(), args)
            })
            .collect();
        assert_eq!(
            keys,
            [
                ("source.ext", vec![]),
                ("source.size", vec![vec![60.0, 300.0]])
            ]
        );
    }

    /// Where the key gives text, a character an unquoted value cannot hold is named: it needs
    /// quotes, or — after `=`, `!=` or in a list — it is a wildcard the comparison does not take,
    /// the operator named as typed — each with its fix, none in a list or after `~`. A key of
    /// another type reads the value as that type and is refused by its type sentence; an unknown
    /// key is only unknown. It fails if such a value binds as text.
    #[test]
    fn a_character_a_value_cannot_hold_bare_is_named_where_the_key_gives_text() {
        let (conn, _) = world();
        for (filter, at, said) in [
            (
                "filename=a%b",
                10,
                r#"`%` needs quotes around the value: filename="a%b""#,
            ),
            (
                "filename=IMG*",
                12,
                r#"`*` is a wildcard, but `=` does not take wildcards. As a glob: filename~IMG*   As exact text: filename="IMG*""#,
            ),
            (
                "filename != a?b",
                13,
                r#"`?` is a wildcard, but `!=` does not take wildcards. As a glob: filename !~ a?b   As exact text: filename != "a?b""#,
            ),
            (
                "filename IN (a, [b])",
                16,
                "`[` is a wildcard, but IN does not take wildcards",
            ),
            (
                "filename~a&b",
                10,
                "`&` needs quotes; quote that part, but not * ? [ ]",
            ),
            (
                "numtext=a#b",
                9,
                r#"`#` needs quotes around the value: numtext="a#b""#,
            ),
            (
                "source.size=1%",
                12,
                "source.size is of type number; `1%` is not a number",
            ),
            ("Rating=a%b", 0, "`Rating` is an unknown key"),
        ] {
            let lines = refusal(&conn, &[filter]).render("");
            let caret = format!("{}^ {said}", " ".repeat(at));
            assert_eq!(lines[..2], [filter.to_string(), caret], "{filter}");
        }
    }

    /// A database holding a text fact under each key named, beside `world`'s.
    fn world_with_text(keys: &[&str]) -> Connection {
        let (conn, a) = world();
        for key in keys {
            insert_fact(&conn, a, key, "x");
        }
        conn
    }

    /// The sentences of a refusal's blocks, in order, each with its fix.
    fn said(conn: &Connection, filter: &str) -> Vec<String> {
        blocks(&refusal(conn, &[filter]))
            .into_iter()
            .map(|(_, sentence)| sentence)
            .collect()
    }

    /// **Every fix shown reads back and binds**: over clauses holding every character a value
    /// cannot hold bare, and values that read as a number, a time or a time of day, against
    /// keys of each type and under each operator, every quoted value, every glob and exact pair
    /// and every value quoted for the key's type a refusal shows is read and bound again as a
    /// filter of its own, and binds. It fails if a fix is shown that would be refused again
    /// (`filename=a*'b` gives a glob that does not read; `Title IN (a%b, c*)` a list that still
    /// holds a wildcard; `filename IN (2024, a%b)` a list that still holds a `%`).
    #[test]
    fn every_fix_shown_reads_back_and_binds() {
        let conn = world_with_text(&["content.Title"]);
        let keys = [
            "filename",
            "Title",
            "source.rel_path",
            "source.ext|lowercase",
            "source.size",
            "taken",
        ];
        let values = [
            "a%b",
            "a#b",
            "IMG*",
            "a*'b",
            "=a",
            "!a",
            "(a",
            "O'Brien",
            "O\"Brien",
            "a'b\"c",
            "[x]",
            "a]b",
            "photo(1).jpg",
            "a*%",
            "a%b)",
            "O'Brien*",
            "a@b*",
            "a\\b*",
            "[abc",
            "2024",
            "1e400",
            "2024-06",
            "2024:07:23",
            "12:30",
            "2024-06-15T14:30:05.45+02:00",
        ];
        let mut clauses = Vec::new();
        for key in keys {
            for value in values {
                for op in ["=", " != ", "<", "~"] {
                    clauses.push(format!("{key}{op}{value}"));
                }
                clauses.push(format!("{key} IN ({value}, c*)"));
                clauses.push(format!("{key} NOT IN ({value}, b%c)"));
                clauses.push(format!("{key} IN ({value} was, x)"));
            }
        }
        let (mut quoted, mut pairs, mut for_type) = (0, 0, 0);
        for clause in &clauses {
            let Err(error) = read_filters(&conn, &ExpressionText::plain(&[clause.as_str()])) else {
                continue;
            };
            let refused = error.downcast::<ExpressionRefused>().expect("a refusal");
            for cause in &refused.causes {
                let Some(problem) = cause.problem() else {
                    continue;
                };
                let shown = match &problem.fix {
                    Fix::Quoted { rewrite } => {
                        quoted += 1;
                        vec![rewrite.clone()]
                    }
                    Fix::GlobOrExact { glob, exact } => {
                        pairs += 1;
                        vec![glob.clone(), exact.clone()]
                    }
                    Fix::ForKeyType { rewrite, .. } => {
                        for_type += 1;
                        vec![rewrite.clone()]
                    }
                    Fix::None | Fix::KeyName { .. } => Vec::new(),
                };
                for rewrite in shown {
                    assert!(
                        read_filters(&conn, &ExpressionText::plain(&[rewrite.as_str()])).is_ok(),
                        "{clause}: the fix {rewrite} does not bind"
                    );
                }
            }
        }
        assert!(
            quoted >= 20 && pairs >= 5 && for_type >= 20,
            "{quoted} quoted, {pairs} pairs, {for_type} for the key's type"
        );
        // The glob half is read by the glob reader: where it cannot be read, neither fix shows.
        for clause in [
            "filename=O'Brien*",
            "filename=a@b*",
            "filename=a\\b*",
            "filename=[abc",
        ] {
            assert_eq!(
                said(&conn, clause),
                [format!(
                    "`{}` is a wildcard, but `=` does not take wildcards",
                    if clause.ends_with("[abc") { '[' } else { '*' }
                )],
                "{clause}"
            );
        }
        assert_eq!(
            said(&conn, "filename=IMG*"),
            [
                r#"`*` is a wildcard, but `=` does not take wildcards. As a glob: filename~IMG*   As exact text: filename="IMG*""#
            ]
        );
    }

    /// A value starting with a character an operator can hold — `=`, `!`, `<`, `>`, `~`, `(` —
    /// is quoted whole: the language has no `==`, so the rules read the operator, then the
    /// value, and the fix states that reading for the user to judge.
    #[test]
    fn a_value_starting_with_an_operator_character_is_quoted_whole() {
        let (conn, _) = world();
        for (filter, said_first) in [
            (
                "filename==a",
                r#"`=` needs quotes around the value: filename="=a""#,
            ),
            (
                "filename=!a",
                r#"`!` needs quotes around the value: filename="!a""#,
            ),
            (
                "filename=<a",
                r#"`<` needs quotes around the value: filename="<a""#,
            ),
            (
                "filename=>a",
                r#"`>` needs quotes around the value: filename=">a""#,
            ),
            (
                "filename=~a",
                r#"`~` needs quotes around the value: filename="~a""#,
            ),
            (
                "filename=(a",
                r#"`(` needs quotes around the value: filename="(a""#,
            ),
            (
                "filename!==a",
                r#"`=` needs quotes around the value: filename!="=a""#,
            ),
        ] {
            assert_eq!(said(&conn, filter), [said_first], "{filter}");
        }
    }

    /// In an `IN` list, one fix for the clause, every item that needs quotes quoted and every
    /// other character as typed, shown with the first such item; the others name their
    /// character alone. After `<` the sentence stands alone.
    #[test]
    fn one_fix_quotes_every_item_of_a_list_that_needs_quotes() {
        let conn = world_with_text(&["content.Title"]);
        assert_eq!(
            said(&conn, "Title IN (a%b, c, d#e)"),
            [
                r#"`%` needs quotes around the value: Title IN ("a%b", c, "d#e")"#,
                "`#` needs quotes around the value",
            ]
        );
        assert_eq!(
            said(&conn, "Title NOT IN ( a%b ,c )"),
            [r#"`%` needs quotes around the value: Title NOT IN ( "a%b" ,c )"#]
        );
    }

    /// After an ordering operator a value is quoted as after `=`: a text key compares quoted
    /// text in character order. After `~` a value is a glob, which shows no fix, and where a
    /// quote mark opens quoted text.
    #[test]
    fn a_value_after_an_ordering_operator_is_quoted_too() {
        let conn = world_with_text(&["content.Title"]);
        for op in ["<", ">", "<=", ">="] {
            assert_eq!(
                said(&conn, &format!("Title{op}a%b")),
                [format!(
                    r#"`%` needs quotes around the value: Title{op}"a%b""#
                )]
            );
        }
        assert_eq!(
            said(&conn, "Title~a%b"),
            ["`%` needs quotes; quote that part, but not * ? [ ]"]
        );
        assert_eq!(said(&conn, "Title~a'b"), ["this quote is unclosed"]);
    }

    /// A quoted value uses `"`, or `'` where the text holds a `"` and no `'`, so no mark is
    /// doubled where one need not be; with both marks in it, the `"` is doubled.
    #[test]
    fn a_quoted_value_uses_the_other_mark_where_that_needs_no_doubling() {
        let conn = world_with_text(&["content.Title"]);
        for (filter, said_first) in [
            (
                "Title=O'Brien",
                r#"`'` needs quotes around the value: Title="O'Brien""#,
            ),
            (
                "Title=O\"Brien",
                r#"`"` needs quotes around the value: Title='O"Brien'"#,
            ),
            (
                "Title=a'b\"c",
                r#"`'` needs quotes around the value: Title="a'b""c""#,
            ),
        ] {
            assert_eq!(said(&conn, filter), [said_first], "{filter}");
        }
    }

    /// A wildcard after `=` or `!=` shows the glob and the exact text, and only when both read
    /// back and bind: `a*'b` read as a glob holds a `'`, which needs quotes there too, so
    /// neither is shown. In a list, no fix.
    #[test]
    fn a_wildcard_after_equals_shows_both_fixes_only_when_both_read_back() {
        let (conn, _) = world();
        assert_eq!(
            said(&conn, "filename=IMG*"),
            [
                r#"`*` is a wildcard, but `=` does not take wildcards. As a glob: filename~IMG*   As exact text: filename="IMG*""#
            ]
        );
        assert_eq!(
            said(&conn, "filename != a?b"),
            [
                r#"`?` is a wildcard, but `!=` does not take wildcards. As a glob: filename !~ a?b   As exact text: filename != "a?b""#
            ]
        );
        assert_eq!(
            said(&conn, "filename=a*'b"),
            ["`*` is a wildcard, but `=` does not take wildcards"]
        );
        assert_eq!(
            said(&conn, "filename IN (a, IMG*)"),
            ["`*` is a wildcard, but IN does not take wildcards"]
        );
    }

    /// A fix keeps where the rules end each part and adds only quotes: the value ends at the
    /// space, so the fix quotes what the rules read as the value and the space keeps its own
    /// sentence, which has no fix.
    #[test]
    fn a_fix_keeps_where_the_rules_end_each_part() {
        let conn = world_with_text(&["content.Title", "content.Model"]);
        assert_eq!(
            said(&conn, "Title=O'Brien was here"),
            [
                r#"`'` needs quotes around the value: Title="O'Brien""#,
                "a space separates values",
            ]
        );
        assert_eq!(
            said(&conn, "filename=IMG*01 copy.jpg"),
            [
                r#"`*` is a wildcard, but `=` does not take wildcards. As a glob: filename~IMG*01   As exact text: filename="IMG*01""#,
                "a space separates values",
            ]
        );
        assert_eq!(said(&conn, "Model=EOS 5D"), ["a space separates values"]);
    }

    /// A list where an item is followed by more has no fix: one for the clause would have to
    /// drop the word or add a comma, and a fix adds only quotes.
    #[test]
    fn no_list_fix_where_an_item_is_followed_by_more() {
        let conn = world_with_text(&["content.Title"]);
        assert_eq!(
            said(&conn, "Title IN (O'Brien was, x)"),
            [
                "`'` needs quotes around the value",
                "values in a list are separated by commas",
            ]
        );
    }

    /// A `)` that closes nothing ends the value before it and is named on its own, so a fix for
    /// the value quotes the value alone and the `)` has none.
    #[test]
    fn a_paren_that_closes_nothing_ends_the_value() {
        let (conn, _) = world();
        assert_eq!(
            said(&conn, "filename=photo1.jpg)"),
            ["`)` has no matching `(`"]
        );
        assert_eq!(
            said(&conn, "(filename=a OR filename=b) AND filename=c)"),
            ["`)` has no matching `(`"]
        );
        assert_eq!(
            said(&conn, "filename=a%b)"),
            [
                r#"`%` needs quotes around the value: filename="a%b""#,
                "`)` has no matching `(`",
            ]
        );
    }

    /// The quoted name is offered only where it names a key with facts, so a refusal never
    /// offers an unknown key; the line under the sentence states the key-name rule with it.
    #[test]
    fn a_quoted_name_is_offered_only_for_a_key_with_facts() {
        let conn = world_with_text(&["content.Foo%Bar"]);
        let rule = "A key name contains letters, digits, _ . : / and -; quote a name with any \
                    other character";
        let lines = refusal(&conn, &["content.Foo%Bar=1"]).render("");
        assert_eq!(
            lines.last().map(String::as_str),
            Some(format!(r#"  {rule}: "content.Foo%Bar""#).as_str()),
            "{lines:#?}"
        );
        let lines = refusal(&conn, &["content.Baz%Qux=1"]).render("");
        assert!(!lines.iter().any(|l| l.contains(rule)), "{lines:#?}");
    }

    /// In a report key, a space after a bare name with more after it is where the quoted name
    /// is offered, since nothing may follow the key there; again only for a key with facts.
    #[test]
    fn a_quoted_name_with_a_space_is_offered_for_a_report_key() {
        let (conn, _) = world();
        let lines = |key: &str| {
            read_report(&conn, Some(key), &[], &ExpressionText::plain(&[]))
                .err()
                .expect("refused")
                .downcast::<ExpressionRefused>()
                .expect("a refusal")
                .render("")
        };
        assert_eq!(
            lines("content.File Size"),
            [
                "content.File Size",
                "             ^ `Size` cannot follow `content.File`",
                r#"  A key name contains letters, digits, _ . : / and -; quote a name with any other character: "content.File Size""#,
            ]
        );
        assert_eq!(
            lines("content.No Such"),
            [
                "content.No Such",
                "           ^ `Such` cannot follow `content.No`",
            ]
        );
    }

    // =========================================================================
    // Aliases
    // =========================================================================

    /// An aliases file holding `content` in a fresh directory, and texts given with it.
    fn given(content: Option<&str>, texts: &[&str]) -> (tempfile::TempDir, ExpressionText) {
        let dir = tempfile::tempdir().unwrap();
        if let Some(content) = content {
            std::fs::write(dir.path().join("aliases.toml"), content).unwrap();
        }
        let texts = texts.iter().map(|t| t.to_string()).collect();
        let text = ExpressionText::given(texts, dir.path());
        (dir, text)
    }

    fn refused_with(conn: &Connection, text: &ExpressionText) -> ExpressionRefused {
        read_filters(conn, text)
            .expect_err("refused")
            .downcast::<ExpressionRefused>()
            .unwrap_or_else(|e| panic!("not a refusal: {e:?}"))
    }

    /// No aliases file is no error for a filter with no alias; a use meets the unknown alias
    /// sentence naming the path where the file would be.
    #[test]
    fn a_missing_file_is_no_error_until_an_alias_is_used() {
        let (conn, _) = world();
        let (_dir, text) = given(None, &["numtext? AND filename~*\"@\"*"]);
        read_filters(&conn, &text).expect("no alias, no file read");
        let (dir, text) = given(None, &["numtext? AND @x"]);
        let path = dir.path().join("aliases.toml");
        assert_eq!(
            refused_with(&conn, &text).render(""),
            [
                "numtext? AND @x".to_string(),
                format!(
                    "             ^ `@x` is an unknown alias; there is no aliases file at {}",
                    path.display()
                ),
            ]
        );
    }

    /// A broken file refuses the run on its own: the file's refusal is the error, and nothing
    /// the texts would have been refused for is drawn beside it. A run with no alias never
    /// reads it.
    #[test]
    fn a_broken_file_is_refused_on_its_own() {
        let (conn, _) = world();
        let (_dir, text) = given(Some("x = \"Title=\"\n"), &["Nope=1"]);
        assert_eq!(kind(&refused_with(&conn, &text).causes[0]), "unknown key");
        let (_dir, text) = given(Some("x = \"Title=\"\n"), &["Nope=1", "@x"]);
        let error = read_filters(&conn, &text).expect_err("refused");
        assert!(
            error.downcast_ref::<ExpressionRefused>().is_none(),
            "{error}"
        );
        let text = error.to_string();
        assert!(text.contains("has entries that are not valid"), "{text}");
        assert!(!text.contains("Nope"), "{text}");
    }

    /// A cause binding finds inside a filter alias is drawn under the alias where it was typed,
    /// with the alias named before binding's own sentence and no fix that rewrites the value.
    #[test]
    fn a_problem_inside_an_alias_is_drawn_under_its_use() {
        let (conn, _) = world();
        let (_dir, text) = given(
            Some("x = \"numtext<10\"\n"),
            &["source.size>1", "source.size>1 AND @x"],
        );
        let lines = refused_with(&conn, &text).render("");
        assert_eq!(lines[0], "source.size>1 AND @x", "{lines:?}");
        assert_eq!(
            lines[1],
            "                  ^ in alias `@x`: numtext is of type text, and `10` is a number",
            "{lines:?}"
        );
        assert!(
            !lines.iter().any(|line| line.contains("numtext<")),
            "a fix rewrote the alias's value: {lines:?}"
        );
    }

    /// **A key alias compared wrongly is answered as the key written out is**, every name the
    /// answer and its fix use being the alias as typed. The alias and the key share a length,
    /// so each line compares whole.
    #[test]
    fn a_key_alias_compared_wrongly_echoes_the_alias() {
        let (conn, _) = world();
        let alias = |text: &str| {
            text.replace("numtext", "@numtxt")
                .replace("taken", "@take")
                .replace("textdate", "@txtdate")
        };
        for clause in [
            "numtext<10",
            "numtext=2024-06",
            "taken=garbage",
            "textdate>=2024-06",
        ] {
            let (_dir, text) = given(
                Some("numtxt = \"numtext\"\ntake = \"taken\"\ntxtdate = \"textdate\"\n"),
                &[&alias(clause)],
            );
            let written = refusal(&conn, &[clause]).render("");
            let typed = refused_with(&conn, &text).render("");
            let expected: Vec<String> = written.iter().map(|line| alias(line)).collect();
            assert_eq!(typed, expected, "{clause}");
        }
    }

    /// A fix that rewrites a clause holding an alias is bound with the same aliases, so it is
    /// offered where the clause written out would be offered it — never dropped because the
    /// rewrite, read with no aliases, names an alias it cannot read.
    #[test]
    fn a_fix_with_an_alias_binds_with_the_same_aliases() {
        let (conn, _) = world();
        let (_dir, text) = given(Some("numtxt = \"numtext\"\n"), &["@numtxt=10"]);
        let typed = refused_with(&conn, &text).to_string();
        let written = refusal(&conn, &["numtext=10"]).to_string();
        assert!(
            written.contains(r#"For a text key: numtext="10""#),
            "{written}"
        );
        assert_eq!(typed.replace("@numtxt", "numtext"), written, "{typed}");
    }

    /// `--key` and `--group-by` read a key alias as the key it stands for, shown as typed, and
    /// refuse a filter alias where a key stands.
    #[test]
    fn a_report_reads_key_aliases_in_key_and_group_by() {
        let (conn, _) = world();
        let (_dir, filters) = given(
            Some("t = \"taken|year\"\nsz = \"source.size\"\nimage = \"numtext?\"\n"),
            &["@image"],
        );
        let read = read_report(
            &conn,
            Some("@t"),
            &["@sz, source.ext".to_string()],
            &filters,
        )
        .expect("reads");
        let key = read.key.unwrap();
        assert_eq!(
            (key.raw.as_str(), key.base_key.as_str()),
            ("@t", "content.taken")
        );
        assert_eq!(key.modifiers.len(), 1);
        let grouping: Vec<(&str, &str)> = read
            .grouping
            .iter()
            .map(|k| (k.raw.as_str(), k.base_key.as_str()))
            .collect();
        assert_eq!(
            grouping,
            [("@sz", "source.size"), ("source.ext", "source.ext")]
        );
        let error = read_report(&conn, Some("@image"), &[], &filters)
            .err()
            .unwrap()
            .downcast::<ExpressionRefused>()
            .unwrap();
        assert!(
            error
                .to_string()
                .contains("^ `@image` is a filter, not a key"),
            "{error}"
        );
    }

    /// **The recorded text's pin**: each filter as a manifest records it — a filter alias in
    /// parentheses, a key alias's key as written, without the spaces around either — reads
    /// back with no aliases and selects exactly what the typed text selected.
    #[test]
    fn the_recorded_text_selects_what_the_typed_text_selected() {
        let (mut conn, a) = world();
        let b = a + 1;
        let (_dir, text) = given(
            Some(
                "big = \" source.size>0 OR numtext? \"\n\
                 n = \"numtext\"\n\
                 p = \"source.rel_path\"\n",
            ),
            &["NOT @big OR (@n=\"10\" AND @p[0]=\"2019\")", "@p~*.jpg"],
        );
        let bound = read_filters(&conn, &text).expect("binds");
        assert_eq!(
            bound.recorded(),
            [
                r#"NOT (source.size>0 OR numtext?) OR (numtext="10" AND source.rel_path[0]="2019")"#,
                "source.rel_path~*.jpg",
            ]
        );
        let again = read_filters(&conn, &ExpressionText::specified(bound.recorded().to_vec()))
            .expect("the recorded text binds with no aliases");
        let typed = apply_filters(&mut conn, &[a, b], &bound, &UserZone::utc()).unwrap();
        let recorded = apply_filters(&mut conn, &[a, b], &again, &UserZone::utc()).unwrap();
        assert_eq!(typed.source_ids, recorded.source_ids);
        assert!(typed.source_ids.contains(&a), "{:?}", typed.source_ids);
    }

    /// **A use is never dropped from a tree that binds**: a text met after others with no alias
    /// loads the file there and is read again with it, its condition kept and recorded.
    #[test]
    fn a_use_met_after_a_text_with_none_is_read_with_the_file() {
        let (conn, _) = world();
        let (_dir, text) = given(Some("n = \"numtext\"\n"), &["source.size>0", "@n=\"10\""]);
        let bound = read_filters(&conn, &text).expect("binds");
        assert_eq!(bound.trees.len(), 2);
        assert_eq!(bound.recorded(), ["source.size>0", r#"numtext="10""#]);
    }
}
