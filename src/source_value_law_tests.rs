//! The source-value law's verifier: a source holding a key twice — a fact on the source and one
//! on its content — reads one value for it on every surface that takes a source's value for
//! a key. The rule is `source_value` (`expr/domain/value.rs`); this module asks the surfaces,
//! not the rule, so a surface reading the two rows itself fails here by name. An empty file
//! is read like any other: its content's value first.

use std::collections::HashMap;

use crate::core::domain::fact::FactValue;
use crate::core::domain::source::Source;
use crate::core::domain::time::zone::UserZone;
use crate::core::repo::source::fetch_by_id;
use crate::core::repo::Connection;
use crate::core::testing::*;
use crate::expr::{
    apply_filters, evaluate, parse_pattern, prefetch_pattern_facts, read_filters, EvalContext,
    ExpressionText, ParsedFactKey,
};

/// Three sources in one root. `two_rows` has content with `content.Make=Canon` and holds
/// `Nikon` on the source itself; `own_only` has no content and holds `Sony` on the source;
/// `empty` is an empty file holding `Fuji` on the file and `Leica` on the empty object.
struct World {
    conn: Connection,
    two_rows: i64,
    own_only: i64,
    empty: i64,
}

fn world() -> World {
    let conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    let content = insert_object(&conn, "datahash", false);
    let two_rows = insert_source_with_size(&conn, root, "a.jpg", Some(content), 100);
    let own_only = insert_source(&conn, root, "o.jpg", None);
    let empty_object = insert_object(&conn, "emptyhash", false);
    let empty = insert_source_with_size(&conn, root, "e.log", Some(empty_object), 0);
    insert_object_fact(&conn, content, "content.Make", "Canon");
    insert_fact(&conn, two_rows, "content.Make", "Nikon");
    insert_fact(&conn, own_only, "content.Make", "Sony");
    insert_object_fact(&conn, empty_object, "content.Make", "Leica");
    insert_fact(&conn, empty, "content.Make", "Fuji");
    World {
        conn,
        two_rows,
        own_only,
        empty,
    }
}

/// The sources `--where <clause>` selects.
fn filtered(conn: &mut Connection, ids: &[i64], clause: &str) -> Vec<i64> {
    apply_filters(
        conn,
        ids,
        &read_filters(conn, &ExpressionText::plain(&[clause])).unwrap(),
        &UserZone::utc(),
    )
    .unwrap()
    .source_ids
}

/// What `facts --key content.Make` counts, value by value.
fn report(conn: &mut Connection, ids: &[i64]) -> HashMap<String, i64> {
    let key = ParsedFactKey::parse("content.Make").unwrap();
    crate::facts::compute_distribution(conn, ids, &key, 100, &UserZone::utc())
        .unwrap()
        .entries
        .into_iter()
        .map(|entry| (entry.value, entry.count))
        .collect()
}

/// Where `{content.Make}` places one source.
fn placed(conn: &mut Connection, id: i64) -> String {
    let zone = UserZone::utc();
    let facts = prefetch_pattern_facts(conn, &[id], &["content.Make".to_string()]).unwrap();
    let mut ctx = EvalContext::new(&zone);
    for fact in facts.for_source(id) {
        ctx.set_fact(&fact.key, fact.value.clone());
    }
    evaluate(&parse_pattern("{content.Make}").unwrap(), &ctx).unwrap()
}

/// What `worklist --emit content.Make` writes for one source.
fn emitted(conn: &mut Connection, id: i64) -> serde_json::Value {
    let sources: Vec<Source> = vec![fetch_by_id(conn, id).unwrap().unwrap()];
    let key = crate::expr::Key::read("content.Make").unwrap();
    let keys = [("content.Make".to_string(), key)];
    let result =
        crate::worklist::build_entries(conn, &sources, false, &keys, &UserZone::utc()).unwrap();
    let facts = result.entries[0].facts.as_ref().unwrap();
    facts["content.Make"].clone()
}

#[test]
fn one_source_one_value_in_filter_report_pattern_and_worklist() {
    let mut w = world();
    let (two_rows, own_only, empty) = (w.two_rows, w.own_only, w.empty);
    let all = [two_rows, own_only, empty];

    // A filter compares the content's value only: the source's own never matches where the
    // content holds one, an empty file's included.
    assert_eq!(
        filtered(&mut w.conn, &all, "content.Make=Canon"),
        vec![two_rows]
    );
    assert!(
        filtered(&mut w.conn, &all, "content.Make=Nikon").is_empty(),
        "the two-row source's own fact is not its value"
    );
    assert_eq!(
        filtered(&mut w.conn, &all, "NOT content.Make=Nikon"),
        vec![two_rows, own_only, empty]
    );
    assert_eq!(
        filtered(&mut w.conn, &all, "content.Make=Leica"),
        vec![empty]
    );
    assert!(
        filtered(&mut w.conn, &all, "content.Make=Fuji").is_empty(),
        "an empty file's own fact is not its value where the empty object holds one"
    );

    // The report counts each source once, under its content's value.
    assert_eq!(
        report(&mut w.conn, &all),
        HashMap::from([
            ("Canon".to_string(), 1),
            ("Sony".to_string(), 1),
            ("Leica".to_string(), 1),
        ]),
        "facts --key counts each source once, by its one value"
    );

    // A pattern places each by the same value.
    assert_eq!(placed(&mut w.conn, two_rows), "Canon");
    assert_eq!(placed(&mut w.conn, own_only), "Sony");
    assert_eq!(placed(&mut w.conn, empty), "Leica");

    // And the worklist emits it.
    assert_eq!(emitted(&mut w.conn, two_rows), serde_json::json!("Canon"));
    assert_eq!(emitted(&mut w.conn, own_only), serde_json::json!("Sony"));
    assert_eq!(emitted(&mut w.conn, empty), serde_json::json!("Leica"));

    // The value carried is the stored one, untransformed.
    let values =
        crate::expr::fetch_source_values(&mut w.conn, &[two_rows], "content.Make").unwrap();
    assert!(matches!(
        values[&two_rows].as_ref().map(|e| &e.value),
        Some(FactValue::Text(s)) if s == "Canon"
    ));
}
