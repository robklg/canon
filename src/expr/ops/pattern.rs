//! Reading a pattern against the database: its keys' types, to check it, and
//! the facts it needs, to place by it.
//!
//! The facts' one trip to storage, and the place the context-supplied
//! law is applied rather than merely stated. Three commands used to run this
//! loop themselves — planning an apply, reading a manifest's status, and
//! computing the confirmation's sample destinations — each carrying its own
//! copy of the rule about which keys to leave alone. They now ask here.
//!
//! Moving the fetch rather than exporting the predicate is the point. A
//! predicate three callers must remember to call has the same shape as the
//! defect being repaired; a fetch that applies it makes a fourth wrong call
//! site impossible instead of merely detectable.

use std::collections::{BTreeMap, HashMap};

use anyhow::Result;
use rusqlite::Connection;

use crate::core::domain::fact::FactEntry;
use crate::core::repo::fact::key_types;
use crate::expr::domain::bind::{
    bind_pattern_key, keep_fix_that_binds, keep_quoted_name_that_binds, known_type,
    quoted_name_key, BindCause, ExpressionRefused, Refused,
};
use crate::expr::domain::pattern::{
    is_context_supplied, read_pattern_text, BoundPattern, InputPiece, PatternFacts,
};
use crate::expr::ops::bind::offer_quoted_names;
use crate::expr::ops::value::fetch_source_values;

/// **Read and check a manifest's pattern**, as specified: every `{…}` read by the key grammar
/// and bound against the stored keys' types, read once from all of each key's facts — a key with
/// no facts is an unknown key, a key of mixed type one of the pattern's problems — with every
/// problem the reader met and every cause binding found in one refusal. The outer level is the
/// database; the inner is the pattern's answer, the only way a [`BoundPattern`] is made.
pub fn read_pattern(
    conn: &Connection,
    specified: &str,
) -> Result<std::result::Result<BoundPattern, ExpressionRefused>> {
    let mut reading = read_pattern_text(specified);
    offer_quoted_names(conn, &mut reading.problems)?;
    let offered: Vec<String> = reading
        .problems
        .iter()
        .filter_map(quoted_name_key)
        .map(|key| key.base)
        .collect();
    let mut causes: Vec<BindCause> = reading.problems.into_iter().map(BindCause::Read).collect();
    let pieces = reading.value.unwrap_or_default();
    let mut names: Vec<&str> = Vec::new();
    let read_bases = pieces.iter().filter_map(|piece| match piece {
        InputPiece::Key(input) if !input.scope => Some(input.key.base.as_str()),
        InputPiece::Key(_) | InputPiece::Literal(_) => None,
    });
    for base in read_bases.chain(offered.iter().map(String::as_str)) {
        if known_type(base).is_none() && !names.contains(&base) {
            names.push(base);
        }
    }
    let types = key_types(conn, &names)?;
    // A fix shown reads back and binds. A pattern's quoted name rewrites its whole `{…}`,
    // modifiers included, so it is read back as a pattern and bound like one.
    let binds = |rewrite: &str| {
        let reading = read_pattern_text(rewrite);
        reading.problems.is_empty()
            && matches!(reading.value.as_deref(), Some([InputPiece::Key(input)])
                if bind_pattern_key(&input.key, input.scope, &input.clause, &types).is_ok())
    };
    for cause in &mut causes {
        keep_fix_that_binds(cause, &binds);
        keep_quoted_name_that_binds(cause, &binds);
    }
    let mut bound = Vec::with_capacity(pieces.len());
    for piece in &pieces {
        let binding = match piece {
            InputPiece::Key(input) => {
                match bind_pattern_key(&input.key, input.scope, &input.clause, &types) {
                    Ok(binding) => Some(binding),
                    Err(cause) => {
                        causes.push(cause);
                        None
                    }
                }
            }
            InputPiece::Literal(_) => None,
        };
        bound.push((piece.clone(), binding));
    }
    if !causes.is_empty() {
        return Ok(Err(ExpressionRefused {
            refused: Refused::Pattern,
            texts: vec![specified.to_string()],
            typed_at: BTreeMap::new(),
            causes,
        }));
    }
    Ok(Ok(BoundPattern::new(bound)))
}

/// Fetch the stored facts a pattern needs, for a batch of sources.
///
/// `needed_keys` is what the pattern names — `BoundPattern::stored_keys` — which
/// includes the keys the evaluation context answers for itself. Those are
/// dropped here and never reach the database: fetching them would be pointless,
/// and a stored row landing in the map under one of their names would win over
/// the computed value and move where files land.
///
/// Each source carries its one value per key (`fetch_source_values`), so a
/// pattern places a source by the same value a filter selected it on.
///
/// Keys are deduplicated first. A pattern naming one key twice —
/// `{source.mtime|year}/{source.mtime|month}` — reaches this function with two
/// entries, because `stored_keys` gives one per `{...}` occurrence rather than
/// one per key.
pub fn prefetch_pattern_facts(
    conn: &mut Connection,
    source_ids: &[i64],
    needed_keys: &[String],
) -> Result<PatternFacts> {
    let mut facts: HashMap<i64, Vec<FactEntry>> = HashMap::new();

    let mut wanted: Vec<&str> = Vec::new();
    for key in needed_keys {
        if is_context_supplied(key) {
            continue;
        }
        if !wanted.contains(&key.as_str()) {
            wanted.push(key);
        }
    }

    if source_ids.is_empty() || wanted.is_empty() {
        return Ok(PatternFacts::new(facts));
    }

    for key in wanted {
        for (source_id, entry) in fetch_source_values(conn, source_ids, key)? {
            if let Some(entry) = entry {
                facts.entry(source_id).or_default().push(entry);
            }
        }
    }

    Ok(PatternFacts::new(facts))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::testing::{insert_fact, insert_root, insert_source, setup_test_db};

    /// One source with a stored `content.Make`, plus whatever hostile rows a
    /// test asks for. The planted rows go in through the shared fixture's raw
    /// insert on purpose: no Canon write path can create them — import stores a
    /// key only under its content reading (`expr::read_key_name`), which no
    /// `source.` name has, and scanning writes only `content.hash.sha256` —
    /// so the hostile case the four skiplist comments warned about could not
    /// be reached through any writer, which is why nothing had ever
    /// exercised it.
    fn db(planted: &[(&str, &str)]) -> Connection {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let source = insert_source(&conn, root, "IMG_001.jpg", None);
        assert_eq!(source, SOURCE, "the corpus assumes the first source is 1");
        insert_fact(&conn, source, "content.Make", "Canon");
        for (key, value) in planted {
            insert_fact(&conn, source, key, value);
        }
        conn
    }

    /// The one source every test in this corpus reads back.
    const SOURCE: i64 = 1;

    fn keys(list: &[&str]) -> Vec<String> {
        list.iter().map(|k| k.to_string()).collect()
    }

    fn fetched(facts: &PatternFacts) -> Vec<&str> {
        let mut got: Vec<&str> = facts
            .for_source(SOURCE)
            .iter()
            .map(|e| e.key.as_str())
            .collect();
        got.sort_unstable();
        got
    }

    /// The hostile case, at the fetch. Rows literally named `source.mtime`
    /// and `scope.rel_path` exist; the prefetch is asked for both by name and
    /// must not carry either, because whatever it carries reaches evaluation
    /// under that name.
    #[test]
    fn a_stored_fact_never_shadows_a_computed_key() {
        let mut db = db(&[
            ("source.mtime", "1000000000"),
            ("scope.rel_path", "elsewhere"),
        ]);
        let facts = prefetch_pattern_facts(
            &mut db,
            &[SOURCE],
            &keys(&["source.mtime", "scope.rel_path", "content.Make"]),
        )
        .unwrap();
        assert_eq!(fetched(&facts), vec!["content.Make"]);
    }

    /// The `PatternFacts` invariant, over every context-supplied name at once.
    /// Asking for nothing else means the result must be empty — the type's
    /// claim, stated as a test.
    #[test]
    fn the_prefetch_asks_for_no_key_the_context_supplies() {
        let mut db = db(&[
            ("source.mtime", "1000000000"),
            ("source.ext", "png"),
            ("scope.rel_path", "elsewhere"),
            ("content.hash.sha256", "deadbeef"),
            ("source.rel_path", "elsewhere/x.jpg"),
        ]);
        let facts = prefetch_pattern_facts(
            &mut db,
            &[SOURCE],
            &keys(&[
                "source.mtime",
                "source.ext",
                "scope.rel_path",
                "content.hash.sha256",
                "source.rel_path",
            ]),
        )
        .unwrap();
        assert!(
            facts.for_source(SOURCE).is_empty(),
            "fetched {:?}",
            fetched(&facts)
        );
    }

    /// The content hash is the listed half's other key: the source row cannot
    /// answer it, but the lock entry recorded it, and `{hash}`, `{hash_short}`
    /// and `{content.hash.sha256}` read that. So a stored row under it is never
    /// fetched — the destination is placed by the hash `apply` carries, never a
    /// second answer for one value.
    #[test]
    fn content_hash_sha256_is_answered_by_the_lock_not_fetched() {
        let mut db = db(&[("content.hash.sha256", "abcdef")]);
        let facts =
            prefetch_pattern_facts(&mut db, &[SOURCE], &keys(&["content.hash.sha256"])).unwrap();
        assert_eq!(fetched(&facts), Vec::<&str>::new());
    }

    /// The other side of that coin, and the narrowing this story took
    /// deliberately: the old skiplist dropped two whole namespaces by prefix
    /// — `source.` and `scope.` — while the law drops only what the context
    /// actually supplies. A key under either prefix that the context does not
    /// answer is supplied by nothing, shadows nothing, and is fetched like
    /// any other stored fact.
    ///
    /// **Both prefixes, on purpose.** The `scope.` half is the same widening
    /// as the `source.` half — the law exact-matches `scope.rel_path` where
    /// the skiplist matched the prefix — and pinning only the side the
    /// judgment record happened to argue would leave the other side changing
    /// silently.
    ///
    /// No Canon writer can create either row: import stores a key only under
    /// its content reading (`expr::read_key_name`), which no `source.` name has,
    /// and which reads everything without a `content.` prefix — `scope.curated`
    /// included — as `content.*`. These are planted by hand, so the change is unreachable in
    /// practice. It is pinned because it is a real widening of what can decide
    /// a destination, not because it is likely.
    #[test]
    fn a_key_the_context_does_not_supply_is_fetched() {
        let mut db = db(&[("source.curated_by", "hand"), ("scope.curated_by", "hand")]);
        let facts = prefetch_pattern_facts(
            &mut db,
            &[SOURCE],
            &keys(&["source.curated_by", "scope.curated_by"]),
        )
        .unwrap();
        assert_eq!(
            fetched(&facts),
            vec!["scope.curated_by", "source.curated_by"]
        );
    }

    /// One entry per source, not two. `stored_keys` gives a key once
    /// per `{...}` occurrence, so a pattern naming the same fact twice used to
    /// run the same query twice and push the same row in twice.
    #[test]
    fn a_repeated_pattern_key_is_fetched_once() {
        let mut db = db(&[]);
        let facts =
            prefetch_pattern_facts(&mut db, &[SOURCE], &keys(&["content.Make", "content.Make"]))
                .unwrap();
        assert_eq!(fetched(&facts), vec!["content.Make"]);
    }

    /// Totality, not a guard. Neither this nor `no_sources_fetches_nothing`
    /// can fail against the context-supplied filter — deleting the matching
    /// half of the early return leaves both green, because
    /// `fetch_source_values` already answers an empty id list with an
    /// empty map and an empty key loop already produces nothing. What they
    /// pin is that an empty ask is *answered* rather than refused or panicked,
    /// which is a real claim about a function three commands call with
    /// whatever a pattern happened to name. Labelled rather than dressed up:
    /// a pin that cannot fail against the defect it appears to name is the
    /// shape this story's own rules forbid, so neither claims to be one.
    #[test]
    fn no_keys_fetches_nothing() {
        let mut db = db(&[]);
        let facts = prefetch_pattern_facts(&mut db, &[SOURCE], &[]).unwrap();
        assert!(facts.for_source(SOURCE).is_empty());
    }

    #[test]
    fn no_sources_fetches_nothing() {
        let mut db = db(&[]);
        let facts = prefetch_pattern_facts(&mut db, &[], &keys(&["content.Make"])).unwrap();
        assert!(facts.for_source(SOURCE).is_empty());
    }

    /// A pattern naming only keys the context supplies comes back with
    /// nothing, even though a row exists under one of those exact names.
    ///
    /// This one *is* a guard, unlike its two neighbours above: delete the
    /// `is_context_supplied` filter and the planted `source.mtime` row
    /// surfaces and it fails. The stronger claim next door — that such an ask
    /// reaches the database not at all — is true and is why the law is
    /// cheaper than the loop it replaced, but it is not what this observes,
    /// so it is not what this says.
    #[test]
    fn an_all_context_supplied_ask_reaches_no_source() {
        let mut db = db(&[("source.mtime", "1000000000")]);
        let facts = prefetch_pattern_facts(
            &mut db,
            &[SOURCE],
            &keys(&["source.mtime", "source.path", "content.hash.sha256"]),
        )
        .unwrap();
        assert!(facts.for_source(SOURCE).is_empty());
    }
}

/// Reading, checking and placing a pattern, against a real database: the grammar's sentences,
/// binding against the stored keys' types, and where each value lands.
#[cfg(test)]
mod read_and_place {
    use super::*;
    use crate::core::domain::fact::FactValue;
    use crate::core::domain::path::NotContained;
    use crate::core::domain::time::zone::UserZone;
    use crate::core::testing::{
        insert_fact, insert_number_fact, insert_root, insert_source, setup_test_db,
    };
    use crate::expr::domain::pattern::{
        evaluate, EvalContext, NoPlaceCause, NotPlaced, Unmeasured,
    };
    use crate::expr::domain::problem::Fix;
    use crate::expr::domain::value::SourceAttributes;
    use crate::expr::ops::bind::{read_filters, ExpressionText};

    /// A library whose keys have facts: `content.Make` (text), `content.Model` of mixed type
    /// (two text facts, one number), `content.File Size` (number), `content.Taken` (text).
    fn world() -> Connection {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let a = insert_source(&conn, root, "a.jpg", None);
        let b = insert_source(&conn, root, "b.jpg", None);
        let c = insert_source(&conn, root, "c.jpg", None);
        insert_fact(&conn, a, "content.Make", "Canon");
        insert_fact(&conn, a, "content.Model", "EOS");
        insert_fact(&conn, b, "content.Model", "R5");
        insert_number_fact(&conn, c, "content.Model", 5.0);
        insert_number_fact(&conn, a, "content.File Size", 100.0);
        insert_fact(&conn, a, "content.Taken", "2024");
        conn
    }

    fn bound(conn: &Connection, text: &str) -> BoundPattern {
        read_pattern(conn, text)
            .unwrap()
            .unwrap_or_else(|refused| panic!("{text}: {refused}"))
    }

    fn refused(conn: &Connection, text: &str) -> ExpressionRefused {
        match read_pattern(conn, text).unwrap() {
            Ok(_) => panic!("{text}: bound"),
            Err(refused) => refused,
        }
    }

    /// Each cause's caret byte (or none) and sentence, in the order met.
    fn said(conn: &Connection, text: &str) -> Vec<(Option<usize>, String)> {
        refused(conn, text)
            .causes
            .iter()
            .map(|cause| match cause.problem() {
                Some(problem) => (Some(problem.place.start), problem.sentence.clone()),
                None => (None, format!("{cause:?}")),
            })
            .collect()
    }

    fn at(at: usize, sentence: &str) -> Vec<(Option<usize>, String)> {
        vec![(Some(at), sentence.to_string())]
    }

    /// **A pattern's quoted name is shown only where it binds**: it rewrites the whole `{…}`,
    /// modifiers and accessors included, so each one shown is read again as a pattern and
    /// binds; one that would be refused again is not shown. Walked over a number key and a text
    /// key, each with facts and a space in its name, under modifiers and an accessor each
    /// type does or does not take.
    #[test]
    fn a_pattern_quoted_name_is_shown_only_where_it_binds() {
        let conn = world();
        let source = insert_source(&conn, 1, "d.jpg", None);
        insert_fact(&conn, source, "content.Lens Name", "50mm");
        let mut met = Vec::new();
        for name in ["content.File Size", "content.Lens Name"] {
            for rest in ["", "|year", "|bucket(1,2)", "|lowercase", "|stem", "[0]"] {
                let text = format!("{{{name}{rest}}}");
                for cause in refused(&conn, &text).causes {
                    let Some(problem) = cause.problem() else {
                        continue;
                    };
                    if !problem.sentence.contains("a space cannot stand") {
                        continue;
                    }
                    let shown = match &problem.fix {
                        Fix::KeyName { rewrite } => {
                            assert!(
                                read_pattern(&conn, rewrite).unwrap().is_ok(),
                                "{text}: the fix {rewrite} does not bind"
                            );
                            true
                        }
                        _ => false,
                    };
                    met.push((text.clone(), shown));
                }
            }
        }
        let row = |text: &str, shown: bool| (text.to_string(), shown);
        assert_eq!(
            met,
            [
                // A number key: the name alone and a number modifier.
                row("{content.File Size}", true),
                row("{content.File Size|year}", false),
                row("{content.File Size|bucket(1,2)}", true),
                row("{content.File Size|lowercase}", false),
                row("{content.File Size|stem}", false),
                row("{content.File Size[0]}", false),
                // A text key: everything but a number modifier.
                row("{content.Lens Name}", true),
                row("{content.Lens Name|year}", true),
                row("{content.Lens Name|bucket(1,2)}", false),
                row("{content.Lens Name|lowercase}", true),
                row("{content.Lens Name|stem}", true),
                row("{content.Lens Name[0]}", true),
            ]
        );
    }

    /// **Every sentence of the pattern grammar**: input, the caret's byte, the sentence — the
    /// literal text's rows, then each `{…}` row.
    #[test]
    fn every_sentence_of_the_pattern_grammar() {
        let conn = world();
        let rows: &[(&str, usize, &str)] = &[
            ("{content.Make", 0, "`{` has no matching `}`"),
            ("content.Make}", 12, "`}` has no matching `{`"),
            ("{}/{filename}", 0, "a key is missing between `{` and `}`"),
            ("/{filename}", 0, "a pattern cannot start with `/`"),
            ("{content.Make}/", 14, "a pattern cannot end with `/`"),
            (
                "a//{filename}",
                1,
                "an empty name between `//` cannot be a directory name",
            ),
            ("a/../{filename}", 2, "`..` cannot be a directory name"),
            ("./{filename}", 0, "`.` cannot be a directory name"),
            // Ending the pattern, it stands where the file's own name goes.
            ("photos/..", 7, "`..` cannot be a file or directory name"),
            ("photos/.", 7, "`.` cannot be a file or directory name"),
            (
                "{@taken}",
                1,
                "`@taken` is an alias; a pattern cannot contain aliases",
            ),
            ("{archived}", 1, "`archived` is not a key"),
            (
                "{source.rel_path|stem(5)}",
                21,
                "`(5)` cannot follow `source.rel_path|stem`",
            ),
            ("{source.foo}", 1, "`source.foo` is an unknown key"),
            ("{Rating}", 1, "`Rating` is an unknown key"),
            (
                "{source.size|year}",
                12,
                "|year works on times or text; source.size is of type number",
            ),
            ("{'abc}", 1, "this quote is unclosed"),
            ("{|year}/{filename}", 1, "`|year` is not a key"),
            ("{[0]}/{filename}", 1, "`[0]` is not a key"),
        ];
        for (text, caret, sentence) in rows {
            assert_eq!(said(&conn, text), at(*caret, sentence), "{text}");
        }

        // A `{` inside a key: the text after the key cannot follow it, and the `}` left over has
        // no `{`.
        let mut nested = said(&conn, "{a{b}c}/{filename}");
        nested.sort();
        assert_eq!(
            nested,
            [
                (Some(2), "`{b` cannot follow `a`".to_string()),
                (Some(6), "`}` has no matching `{`".to_string()),
            ]
        );

        // The rule lines.
        let archived = refused(&conn, "{archived}").to_string();
        assert!(
            archived.contains("    a stored fact of that name is `content.archived`"),
            "{archived}"
        );
        let unknown = refused(&conn, "{Rating}").to_string();
        assert!(
            unknown.contains("    `canon facts` lists the keys"),
            "{unknown}"
        );

        // A space inside `{…}`: one problem, a caret under every such space, and the key read
        // on past them, so it binds.
        let spaced = refused(&conn, "{ source.mtime | year }/{filename}");
        let [cause] = spaced.causes.as_slice() else {
            panic!("{spaced}");
        };
        let problem = cause.problem().unwrap();
        assert_eq!(
            (
                problem.place.start,
                problem.also.as_deref(),
                problem.sentence.as_str()
            ),
            (
                1,
                Some(&vec![14, 16, 21]),
                "a space cannot stand inside `{…}`"
            )
        );
        assert_eq!(
            spaced.to_string(),
            [
                "cannot use the pattern as specified",
                "  { source.mtime | year }/{filename}",
                "   ^            ^ ^    ^ a space cannot stand inside `{…}`",
            ]
            .join("\n")
        );

        // A space between two parts of a name: the key-name line and the quoted name, for a key
        // with facts — and no fix for one with none.
        let file_size = refused(&conn, "{content.File Size}");
        assert_eq!(
            said(&conn, "{content.File Size}"),
            at(13, "a space cannot stand inside `{…}`")
        );
        assert_eq!(
            file_size.causes[0].problem().unwrap().fix,
            Fix::KeyName {
                rewrite: "{'content.File Size'}".to_string()
            }
        );
        assert!(file_size.to_string().ends_with(": {'content.File Size'}"));
        let no_facts = refused(&conn, "{content.Lens Model}");
        assert_eq!(no_facts.causes[0].problem().unwrap().fix, Fix::None);

        // A key of mixed type is one of the pattern's problems, under its first line.
        let mixed = refused(&conn, "{Model}/{filename}");
        assert!(!mixed.is_answer());
        let lines = mixed.to_string();
        assert!(
            lines.starts_with(
                "cannot use the pattern as specified\n  {Model}: Model is of mixed type (text and \
                 number); a pattern supports one type only"
            ),
            "{lines}"
        );
    }

    #[test]
    fn every_pattern_problem_is_reported_in_one_run() {
        let conn = world();
        let text = "/{Rating}/{source.size|year}//{archived}/";
        let carets: Vec<Option<usize>> = said(&conn, text).into_iter().map(|(at, _)| at).collect();
        // Leading `/`, `//`, trailing `/`, `{archived}`, then binding's two.
        for expected in [0, 28, 40, 31, 2, 22] {
            assert!(carets.contains(&Some(expected)), "{expected}: {carets:?}");
        }
        assert_eq!(carets.len(), 6, "{carets:?}");
    }

    #[test]
    fn a_pattern_name_takes_the_content_default() {
        let conn = world();
        assert_eq!(
            bound(&conn, "{Make}/{filename}").stored_keys(),
            ["content.Make", "filename"]
        );
        // `mtime` is the built-in key, as in a filter.
        let pattern = bound(&conn, "{mtime|year}/{filename}");
        let zone = UserZone::utc();
        let ctx = context(&zone, "x.jpg");
        assert_eq!(placed(&pattern, &ctx), "2024/x.jpg");
    }

    #[test]
    fn scope_rel_path_reads_as_a_path_in_a_pattern_only() {
        let conn = world();
        let pattern = bound(&conn, "{scope.rel_path}");
        assert!(pattern.names_scope_rel_path());
        assert!(pattern.may_nest());
        assert_eq!(pattern.stored_keys(), Vec::<String>::new());
        assert_eq!(
            said(&conn, "{scope.rel_path|year}"),
            at(
                15,
                "|year works on times or text; scope.rel_path is of type path"
            )
        );
        let report = crate::expr::read_report(
            &conn,
            Some("scope.rel_path"),
            &[],
            &ExpressionText::plain(&[]),
        );
        assert!(report.is_err());
    }

    // ------------------------------------------------------------------------
    // Where a value lands
    // ------------------------------------------------------------------------

    fn source(rel: &str) -> SourceAttributes {
        SourceAttributes {
            id: 7,
            root_id: 1,
            root_path: "/photos".to_string(),
            rel_path: rel.to_string(),
            size: 100,
            mtime: 1718452800,
            device: 1,
            inode: 1,
        }
    }

    fn context<'a>(zone: &'a UserZone, rel: &str) -> EvalContext<'a> {
        let mut ctx = EvalContext::new(zone);
        ctx.set_source(source(rel));
        ctx
    }

    fn placed(pattern: &BoundPattern, ctx: &EvalContext) -> String {
        match evaluate(pattern, ctx) {
            Ok(placement) => placement.path.as_str().to_string(),
            Err(not) => panic!("{pattern:?}: {not:?}"),
        }
    }

    fn causes(pattern: &BoundPattern, ctx: &EvalContext) -> Vec<String> {
        match evaluate(pattern, ctx) {
            Err(NotPlaced::NoPlace(no_place)) => {
                no_place.causes.iter().map(NoPlaceCause::sentence).collect()
            }
            other => panic!("{pattern:?}: {other:?}"),
        }
    }

    /// **Where a value lands**, by its type: a path makes a directory per segment, a leading
    /// `/` none, a path with no segments none in each position; any other value one name, with
    /// no place where it holds `/`, is empty, is `.` or `..`, or holds a null character.
    #[test]
    fn where_a_value_lands() {
        let conn = world();
        let zone = UserZone::utc();
        let deep = context(&zone, "trip/day1/x.jpg");
        let top = context(&zone, "README");

        // A path's segments; a leading `/` makes none.
        assert_eq!(
            placed(&bound(&conn, "{source.rel_path}"), &deep),
            "trip/day1/x.jpg"
        );
        assert_eq!(
            placed(&bound(&conn, "{source.path}"), &deep),
            "photos/trip/day1/x.jpg"
        );
        // A path with no segments, at the start, in the middle, before literal text, last.
        let rows = [
            ("{source.rel_path[:-1]}/{filename}", "README"),
            ("x/{source.rel_path[:-1]}/{filename}", "x/README"),
            ("x/{source.rel_path[:-1]}y/{filename}", "x/y/README"),
            ("a/{source.rel_path[:-1]}.jpg", "a/.jpg"),
            // The `.` it leaves is read as the operating system reads it.
            ("a/{source.rel_path[:-1]}./{filename}", "a/README"),
            // Last, after text that is already the file name.
            ("a/{filename}{source.rel_path[:-1]}", "a/README"),
        ];
        for (text, landed) in rows {
            assert_eq!(placed(&bound(&conn, text), &top), landed, "{text}");
        }
        assert_eq!(
            causes(&bound(&conn, "a/{source.rel_path[:-1]}"), &top),
            ["{source.rel_path[:-1]} has no segments: a file name cannot be empty"]
        );
        assert_eq!(
            causes(&bound(&conn, "{source.rel_path[1:]}"), &top),
            ["{source.rel_path[1:]} has no segments: a file name cannot be empty"]
        );
        // Where another `{…}` in the last name gave no value, or a refused one, the name is
        // short because of that `{…}`, and only its cause is said.
        assert_eq!(
            causes(
                &bound(&conn, "out/{source.rel_path[:-1]}{content.Make}"),
                &top
            ),
            ["{content.Make} has no value"]
        );
        let mut dot = context(&zone, "README");
        dot.set_fact("content.Make", FactValue::Text(".".to_string()));
        for text in [
            "out/{source.rel_path[:-1]}{content.Make}",
            "out/{content.Make}{source.rel_path[:-1]}",
        ] {
            let said = causes(&bound(&conn, text), &dot);
            assert_eq!(said.len(), 1, "{text}: {said:?}");
            assert!(
                said[0].starts_with("{content.Make} is `.`"),
                "{text}: {said:?}"
            );
        }
        assert_eq!(
            causes(
                &bound(&conn, "out/{source.rel_path[:-1]}.{content.Make}"),
                &top
            ),
            ["{content.Make} has no value"]
        );
        // A refused path value is the same: `[1:]` of `a/.` is the path `.`.
        let mut path_dot = context(&zone, "README");
        path_dot.set_fact("content.Make", FactValue::Text("a/.".to_string()));
        let said = causes(
            &bound(&conn, "out/{content.Make[1:]}{source.rel_path[:-1]}"),
            &path_dot,
        );
        assert_eq!(said.len(), 1, "{said:?}");
        assert!(
            said[0].starts_with("{content.Make[1:]} contains"),
            "{said:?}"
        );
        // But a `{…}` in an earlier directory, its `/` dropped beside an empty path, does not
        // hide a cause in the last name.
        assert_eq!(
            causes(
                &bound(
                    &conn,
                    "x/{content.Make}{source.rel_path[:-1]}/{source.rel_path[1:]}"
                ),
                &top
            ),
            [
                "{content.Make} has no value",
                "{source.rel_path[1:]} has no segments: a file name cannot be empty",
            ]
        );
        // A `.` beside it in the last name is that name's directory, not a file name.
        for text in [
            "{filename}/.{source.rel_path[:-1]}",
            "a/.{source.rel_path[:-1]}",
            "a/{source.rel_path[:-1]}.",
        ] {
            assert_eq!(
                causes(&bound(&conn, text), &top),
                ["{source.rel_path[:-1]} has no segments: a file name cannot be empty"],
                "{text}"
            );
        }
        // `..` inside a name stays.
        assert_eq!(
            placed(
                &bound(&conn, "{filename}"),
                &context(&zone, "sub/photo..jpg")
            ),
            "photo..jpg"
        );

        // A text value: "a directory name", or "a file name" where it ends the destination.
        let make = |value: &str| {
            let mut ctx = context(&zone, "x.jpg");
            ctx.set_fact("content.Make", FactValue::Text(value.to_string()));
            ctx
        };
        let dir = bound(&conn, "{content.Make}/{filename}");
        let file = bound(&conn, "{content.Make}");
        let rows: [(&str, &BoundPattern, &str); 7] = [
            (
                "AC/DC",
                &dir,
                "{content.Make} is `AC/DC`: a file or directory name cannot contain `/`",
            ),
            (
                "AC/DC",
                &file,
                "{content.Make} is `AC/DC`: a file or directory name cannot contain `/`",
            ),
            (
                "",
                &dir,
                "{content.Make} is empty: a file or directory name cannot be empty",
            ),
            (
                ".",
                &dir,
                "{content.Make} is `.`: a file or directory name cannot be `.`",
            ),
            (
                "..",
                &dir,
                "{content.Make} is `..`: a file or directory name cannot be `..`",
            ),
            (
                "a\0b",
                &dir,
                "{content.Make} contains a null character: a file or directory name cannot contain one",
            ),
            (
                "a\0b",
                &file,
                "{content.Make} contains a null character: a file or directory name cannot contain one",
            ),
        ];
        for (value, pattern, sentence) in rows {
            assert_eq!(causes(pattern, &make(value)), [sentence], "{value:?}");
        }

        // A path value with a `..` name, or a null character in one. No stored fact is a path,
        // so only a source fact or the lock's measurement can carry one.
        let pattern = bound(&conn, "{scope.rel_path}/{filename}");
        for (value, sentence) in [
            (
                "a/../b",
                "{scope.rel_path} contains `..`: a file or directory name cannot be `..`",
            ),
            (
                "a/b\0c",
                "{scope.rel_path} contains a null character: a file or directory name cannot contain one",
            ),
        ] {
            let mut ctx = context(&zone, "x.jpg");
            ctx.set_scope_rel(Some(value), Unmeasured::NoScopeRecorded);
            assert_eq!(causes(&pattern, &ctx), [sentence], "{value:?}");
        }
    }

    /// A destination that would leave its directory is refused by the type, after every value
    /// had a place.
    #[test]
    fn a_destination_joined_to_leave_its_directory_is_not_contained() {
        let conn = world();
        let zone = UserZone::utc();
        let top = context(&zone, "README");
        let pattern = bound(&conn, "{source.rel_path[:-1]}../{filename}");
        match evaluate(&pattern, &top) {
            Err(NotPlaced::NotContained { destination, why }) => {
                assert_eq!(
                    (destination.as_str(), why),
                    ("../README", NotContained::ParentDirectory)
                )
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn every_cause_of_every_expression_is_collected() {
        let conn = world();
        crate::core::testing::insert_fact(&conn, 2, "content.Lens", "50mm");
        let zone = UserZone::utc();
        let ctx = context(&zone, "README");
        assert_eq!(
            causes(
                &bound(
                    &conn,
                    "{content.Make}/{content.Lens}/{source.ext}/{filename}"
                ),
                &ctx
            ),
            [
                "{content.Make} has no value",
                "{content.Lens} has no value",
                "{source.ext} has no value",
            ]
        );
    }

    /// The sentence names the `{…}` as specified — its accessor and modifiers too, since the key
    /// alone may well have a value.
    #[test]
    fn a_no_place_sentence_names_the_expression_as_specified() {
        let conn = world();
        let zone = UserZone::utc();
        let ctx = context(&zone, "misc/x.jpg");
        assert_eq!(
            causes(&bound(&conn, "{source.rel_path[0]|month}/{filename}"), &ctx),
            ["{source.rel_path[0]|month} has no value: the text is not a time"]
        );
        let mut ctx = context(&zone, "x.jpg");
        ctx.set_fact(
            "content.Taken",
            FactValue::Time(crate::core::domain::time::reading::read_time("2024").unwrap()),
        );
        let taken = world();
        crate::core::testing::insert_time_fact(&taken, 2, "content.When", "2024");
        let pattern = bound(&taken, "{content.When|month}/{filename}");
        ctx.set_fact(
            "content.When",
            FactValue::Time(crate::core::domain::time::reading::read_time("2024").unwrap()),
        );
        assert_eq!(
            causes(&pattern, &ctx),
            ["{content.When|month} has no value: the time is known only to the year"]
        );
    }

    /// Each cause's query entry reads back, through `--where`'s reading and binding, as a filter.
    #[test]
    fn every_remedy_query_entry_binds_as_a_filter() {
        let conn = world();
        crate::core::testing::insert_time_fact(&conn, 2, "content.When", "2024");
        let subjects = [
            NoPlaceCause::NoFact {
                subject: "{Make}".into(),
                key: "content.Make".into(),
            },
            NoPlaceCause::NoFact {
                subject: "{'content.File Size'}".into(),
                key: "content.File Size".into(),
            },
            NoPlaceCause::NoValue {
                subject: "{source.rel_path[0]|month}".into(),
                reason: None,
            },
            NoPlaceCause::NoValue {
                subject: "{source.ext}".into(),
                reason: None,
            },
            NoPlaceCause::TooCoarse {
                subject: "{content.When|month}".into(),
                modifier: crate::expr::domain::transform::Modifier::Month,
                precision: crate::core::domain::time::value::Precision::Year,
            },
            NoPlaceCause::UnreadableSaveTime,
        ];
        for cause in subjects {
            let entry = cause.query_entry().expect("a remedy entry");
            read_filters(&conn, &ExpressionText::plain(&[&entry]))
                .unwrap_or_else(|e| panic!("{entry}: {e}"));
        }
    }

    /// A count line is the sentence with the count in front; where the sentence quotes a value,
    /// which differs per source, it names the character the sources share.
    #[test]
    fn a_count_line_names_what_the_sources_share() {
        let slash = NoPlaceCause::ContainsSlash {
            subject: "{content.Make}".into(),
            value: "AC/DC".into(),
        };
        assert_eq!(
            slash.count_line(12),
            "12 sources: {content.Make} contains `/`: a file or directory name cannot contain `/`"
        );
        let empty = NoPlaceCause::Empty {
            subject: "{content.Make}".into(),
        };
        assert_eq!(
            empty.count_line(1),
            "1 source: {content.Make} is empty: a file or directory name cannot be empty"
        );
        let dots = NoPlaceCause::DotName {
            subject: "{content.Make}".into(),
            value: "..".into(),
            in_path: false,
        };
        assert_eq!(
            dots.count_line(1200),
            "1,200 sources: {content.Make} is `..`: a file or directory name cannot be `..`"
        );
    }

    #[test]
    fn the_hash_keys_read_the_locks_recorded_hash() {
        let conn = world();
        let zone = UserZone::utc();
        let mut ctx = context(&zone, "x.jpg");
        ctx.set_lock_hash(Some("abcdef0123456789".to_string()));
        // A stored hash planted under the content-hash key never reaches a destination.
        ctx.set_fact(
            "content.hash.sha256",
            FactValue::Text("deadbeef99".to_string()),
        );
        for (text, landed) in [
            ("{hash}", "abcdef0123456789"),
            ("{hash_short}", "abcdef01"),
            ("{content.hash.sha256}", "abcdef0123456789"),
            ("{hash|short}", "abcdef01"),
        ] {
            assert_eq!(placed(&bound(&conn, text), &ctx), landed, "{text}");
        }
        assert_eq!(
            said(&conn, "{object.hash}"),
            at(1, "`object.hash` is an unknown key")
        );
    }

    /// A key's type changing between reading the pattern and a source's evaluation stops
    /// the run with the answer, never a source with no place.
    #[test]
    fn a_type_change_during_apply_stops_the_run_as_an_answer() {
        let conn = world();
        let pattern = bound(&conn, "{content.Make}/{filename}");
        let zone = UserZone::utc();
        let mut ctx = context(&zone, "x.jpg");
        ctx.set_fact("content.Make", FactValue::Num(5.0));
        match evaluate(&pattern, &ctx) {
            Err(NotPlaced::TypeChanged(refused)) => {
                assert!(refused.is_answer());
                assert_eq!(
                    refused.to_string(),
                    "content.Make's type changed while this ran; run it again"
                );
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn the_leading_directory_needs_no_clean_up() {
        let conn = world();
        let shape = |text: &str| {
            let pattern = bound(&conn, text);
            (
                pattern.leading_directory().map(str::to_string),
                pattern.may_nest(),
            )
        };
        let rows: [(&str, Option<&str>, bool); 7] = [
            ("photos/2024/{filename}", Some("photos/2024"), false),
            ("{content.Make}/{filename}", None, true),
            ("photos/IMG_{filename}", Some("photos"), false),
            ("photos/{content.Make}/x/{filename}", Some("photos"), true),
            ("photos/{source.rel_path}", Some("photos"), true),
            ("photos/{source.rel_path[-1]}", Some("photos"), false),
            ("photos/x.jpg", Some("photos"), false),
        ];
        for (text, prefix, nests) in rows {
            assert_eq!(shape(text), (prefix.map(str::to_string), nests), "{text}");
        }
    }

    /// The instant rule: a pattern placing by a save time read an instant; by a clock reading,
    /// none.
    #[test]
    fn a_placement_says_whether_it_read_an_instant() {
        let conn = world();
        let zone = UserZone::utc();
        let ctx = context(&zone, "x.jpg");
        let by_mtime = evaluate(&bound(&conn, "{source.mtime|year}/{filename}"), &ctx).unwrap();
        assert!(by_mtime.used_an_instant);
        let ctx = context(&zone, "x.jpg");
        let by_name = evaluate(&bound(&conn, "{filename}"), &ctx).unwrap();
        assert!(!by_name.used_an_instant);
    }
}
