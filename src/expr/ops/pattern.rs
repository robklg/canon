//! Reading the facts a pattern needs out of the database.
//!
//! The shaping half's one trip to storage, and the place the context-supplied
//! law is applied rather than merely stated. Three commands used to run this
//! loop themselves — planning an apply, reading a manifest's status, and
//! computing the confirmation's sample destinations — each carrying its own
//! copy of the rule about which keys to leave alone. They now ask here.
//!
//! Moving the fetch rather than exporting the predicate is the point. A
//! predicate three callers must remember to call has the same shape as the
//! defect being repaired; a fetch that applies it makes a fourth wrong call
//! site impossible instead of merely detectable.

use std::collections::HashMap;

use anyhow::Result;
use rusqlite::Connection;

use crate::core::domain::fact::FactEntry;
use crate::core::repo::fact::batch_fetch_key_for_sources;
use crate::expr::domain::pattern::{is_context_supplied, PatternFacts};

/// Fetch the stored facts a pattern needs, for a batch of sources.
///
/// `needed_keys` is what the pattern names — `expr::extract_fact_keys` — which
/// includes the keys the evaluation context answers for itself. Those are
/// dropped here and never reach the database: fetching them would be pointless,
/// and a stored row landing in the map under one of their names would win over
/// the computed value and move where files land.
///
/// Keys are deduplicated first. A pattern naming one key twice —
/// `{source.mtime|year}/{source.mtime|month}` — reaches this function with two
/// entries, because `extract_fact_keys` reports one per `{...}` occurrence
/// rather than one per key.
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
        for (source_id, entry) in batch_fetch_key_for_sources(conn, source_ids, key)? {
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
    /// insert on purpose: no Canon write path can create them —
    /// `facts::domain::normalize_fact_key` refuses the whole `source.`
    /// namespace on import, and scanning writes only `content.hash.sha256` —
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
            ("object.hash", "deadbeef"),
            ("source.rel_path", "elsewhere/x.jpg"),
        ]);
        let facts = prefetch_pattern_facts(
            &mut db,
            &[SOURCE],
            &keys(&[
                "source.mtime",
                "source.ext",
                "scope.rel_path",
                "object.hash",
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

    /// The case a hand-written list gets wrong. `content.hash.sha256` is a
    /// `BuiltinKey` — so a rule phrased as "skip the built-ins" would drop it
    /// — but the source cannot answer it, so it is a genuinely stored fact
    /// and must be fetched.
    #[test]
    fn content_hash_sha256_is_fetched_not_skipped() {
        let mut db = db(&[("content.hash.sha256", "abcdef")]);
        let facts =
            prefetch_pattern_facts(&mut db, &[SOURCE], &keys(&["content.hash.sha256"])).unwrap();
        assert_eq!(fetched(&facts), vec!["content.hash.sha256"]);
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
    /// No Canon writer can create either row: `facts::domain::normalize_fact_key`
    /// refuses the whole `source.` prefix on import and rewrites everything
    /// without a `content.` prefix — `scope.curated` included — into
    /// `content.*`. These are planted by hand, so the change is unreachable in
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

    /// One entry per source, not two. `extract_fact_keys` reports a key once
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
    /// `batch_fetch_key_for_sources` already answers an empty id list with an
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
            &keys(&["source.mtime", "source.path", "object.hash"]),
        )
        .unwrap();
        assert!(facts.for_source(SOURCE).is_empty());
    }
}
