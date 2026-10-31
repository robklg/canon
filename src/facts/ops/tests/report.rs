//! The report's tables, computed: the key list, one key's distribution, a grouped distribution
//! and the per-root table — their rows, order, marks and the lines under them.

use crate::core::domain::fact::FactType;
use crate::core::domain::time::zone::UserZone;
use crate::core::repo::Connection;
use crate::core::testing::{insert_object, insert_root, insert_source, setup_test_db};
use crate::expr::{ExpressionText, Key, KeyKind, ParsedFactKey};
use crate::facts::ops::report::{
    compute_all_keys, compute_distribution, compute_grouped_distribution,
    compute_root_distribution, AllKeysResult, Group, KeyInfo, Mark, Part, Position,
};

use super::fixtures::{cells, cells_of, joined, line_filter, printed_lines, Library};

/// Insert a fact stored on a source, with a time value.
fn insert_time_fact(conn: &Connection, source_id: i64, key: &str, timestamp: i64) {
    conn.execute(
        "INSERT INTO facts (entity_type, entity_id, key, value_time, time_form, time_precision,
                            observed_at, observed_basis_rev)
         VALUES ('source', ?1, ?2, ?3, 'clock', 'second', 0, 0)",
        rusqlite::params![source_id, key, timestamp],
    )
    .unwrap();
}

/// Store a number fact on a source.
fn insert_num_fact(conn: &Connection, source_id: i64, key: &str, value: f64) {
    conn.execute(
        "INSERT INTO facts (entity_type, entity_id, key, value_num, observed_at, observed_basis_rev)
         VALUES ('source', ?1, ?2, ?3, 0, 0)",
        rusqlite::params![source_id, key, value],
    )
    .unwrap();
}

/// The sources with no value in a distribution.
fn without(distribution: &crate::facts::ops::report::Distribution) -> i64 {
    distribution.no_value.as_ref().map_or(0, |row| row.count)
}

/// A key as `canon facts` reads it, given its default.
fn defaulted(key: &str) -> ParsedFactKey {
    let mut keys = crate::facts::ops::report::report_keys(
        Some(ParsedFactKey::parse(key).unwrap()),
        Vec::new(),
        false,
    )
    .unwrap();
    keys.main.take().unwrap()
}

// =========================================================================
// compute_all_keys tests
// =========================================================================

#[test]
fn test_compute_all_keys_includes_source_facts_and_stored() {
    let mut conn = setup_test_db();
    let root_id = insert_root(&conn, "/photos", "source", false);
    let obj_id = insert_object(&conn, "abc123", false);
    let s1 = insert_source(&conn, root_id, "a.jpg", Some(obj_id));
    let s2 = insert_source(&conn, root_id, "b.jpg", Some(obj_id));

    // Add a stored fact for one source
    crate::core::testing::insert_fact(&conn, s1, "content.Make", "Canon");

    let result = compute_all_keys(&mut conn, &[s1, s2], false).unwrap();

    // Should include the source facts with count == 2 (total_sources)
    let ext_key = result.keys.iter().find(|k| k.key == "source.ext").unwrap();
    assert_eq!(ext_key.count, 2);
    assert_eq!(ext_key.kind, KeyKind::SourceFact);

    // Should include stored fact with count == 1
    let make_key = result
        .keys
        .iter()
        .find(|k| k.key == "content.Make")
        .unwrap();
    assert_eq!(make_key.count, 1);
    assert_eq!(make_key.kind, KeyKind::ContentFact);

    assert_eq!(result.total_sources, 2);
}

fn listed<'a>(result: &'a AllKeysResult, key: &str) -> &'a KeyInfo {
    result.keys.iter().find(|k| k.key == key).unwrap()
}

/// The Type column reads the key, not the scope: a key holding text on a file outside
/// the scope and a number inside it lists as mixed, while its count stays the scope's.
#[test]
fn the_listing_states_the_key_type_not_the_scoped_type() {
    let mut conn = setup_test_db();
    let here = insert_root(&conn, "/photos", "source", false);
    let elsewhere = insert_root(&conn, "/other", "source", false);
    let inside = insert_source(&conn, here, "a.jpg", None);
    let outside = insert_source(&conn, elsewhere, "b.jpg", None);
    insert_num_fact(&conn, inside, "content.Width", 4000.0);
    crate::core::testing::insert_fact(&conn, outside, "content.Width", "wide");
    insert_num_fact(&conn, inside, "content.Height", 3000.0);

    let result = compute_all_keys(&mut conn, &[inside], false).unwrap();
    let width = listed(&result, "content.Width");
    assert_eq!(width.types.word(), "mixed");
    assert_eq!(width.count, 1);
    assert_eq!(listed(&result, "content.Height").types.word(), "number");
}

/// Each stored key lists with the type its facts hold, and a source fact or built-in key with its own.
#[test]
fn the_listing_types_each_key_from_key_types() {
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    let s1 = insert_source(&conn, root, "a.jpg", None);
    crate::core::testing::insert_fact(&conn, s1, "content.Make", "Canon");
    insert_num_fact(&conn, s1, "content.Width", 4000.0);
    insert_time_fact(&conn, s1, "content.DateTimeOriginal", 1704067200);

    let result = compute_all_keys(&mut conn, &[s1], false).unwrap();
    for (key, want) in [
        ("content.Make", FactType::Text),
        ("content.Width", FactType::Num),
        ("content.DateTimeOriginal", FactType::Time),
        ("source.size", FactType::Num),
    ] {
        assert_eq!(listed(&result, key).types.single(), Some(want), "{key}");
    }
}

/// **The key list prints each key by the printing rule**: a key holding a space in Canon's
/// quotes, a key holding a control character with its pictures, marked, and one line naming what
/// stands for it.
#[test]
fn the_key_list_prints_each_key_as_it_reads_back() {
    let mut library = Library::new();
    library.text("content.File Size", "4 MB");
    library.text("content.a\nb", "x");
    library.text("content.c\td", "y");
    let ids = library.all();
    let result = compute_all_keys(&mut library.conn, &ids, false).unwrap();

    let spaced = listed(&result, "content.File Size");
    assert_eq!(spaced.cell, "\"content.File Size\"");
    assert!(!spaced.mark);
    let newline = listed(&result, "content.a\nb");
    assert_eq!(newline.cell, "\"content.a\u{240A}b\"");
    assert!(newline.mark);
    assert!(listed(&result, "content.c\td").mark);
    assert_eq!(
        printed_lines(&result.lines),
        ["Key marked !: a control character stands where \u{2409} or \u{240A} shows, so the key cannot be pasted as printed"]
    );

    let plain = Library::new();
    let result = compute_all_keys(&mut { plain }.conn, &[], false).unwrap();
    assert!(result.lines.is_empty());
}

/// **A key's count is what `<key>?` selects** (its one value by the source-value law): a source
/// whose content holds a row Canon cannot read has no value, though its own row holds one; a key
/// whose every row is unreadable here lists with none.
#[test]
fn the_key_list_counts_what_the_key_with_a_question_mark_selects() {
    let mut library = Library::new();
    library.text("content.T", "own");
    let unreadable = library.text("content.T", "own too");
    library
        .conn
        .execute(
            "INSERT INTO facts (entity_type, entity_id, key, value_time, time_form, time_precision,
                                observed_at, observed_basis_rev)
             SELECT 'object', object_id, 'content.T', 253402315200, 'clock', 'second', 0, NULL
             FROM sources WHERE id = ?1",
            [unreadable],
        )
        .unwrap();
    let only_unreadable = library.file("u.jpg");
    library
        .conn
        .execute(
            "INSERT INTO facts (entity_type, entity_id, key, value_time, time_form, time_precision,
                                observed_at, observed_basis_rev)
             SELECT 'object', object_id, 'content.U', 253402315200, 'clock', 'second', 0, NULL
             FROM sources WHERE id = ?1",
            [only_unreadable],
        )
        .unwrap();
    let ids = library.all();
    let result = compute_all_keys(&mut library.conn, &ids, false).unwrap();
    for key in ["content.T", "content.U"] {
        let asked = library.selected(&[&format!("{key}?")]).len() as i64;
        assert_eq!(listed(&result, key).count, asked, "{key}");
    }
    assert_eq!(listed(&result, "content.T").count, 1);
    assert_eq!(listed(&result, "content.U").count, 0);
}

// =========================================================================
// compute_distribution tests
// =========================================================================

#[test]
fn test_compute_distribution_stored_fact() {
    let mut conn = setup_test_db();
    let root_id = insert_root(&conn, "/photos", "source", false);
    let obj1 = insert_object(&conn, "hash1", false);
    let obj2 = insert_object(&conn, "hash2", false);
    let obj3 = insert_object(&conn, "hash3", false);
    let s1 = insert_source(&conn, root_id, "a.jpg", Some(obj1));
    let s2 = insert_source(&conn, root_id, "b.jpg", Some(obj2));
    let s3 = insert_source(&conn, root_id, "c.jpg", Some(obj3));

    crate::core::testing::insert_fact(&conn, s1, "content.Make", "Canon");
    crate::core::testing::insert_fact(&conn, s2, "content.Make", "Canon");
    crate::core::testing::insert_fact(&conn, s3, "content.Make", "Nikon");

    let key = ParsedFactKey::parse("content.Make").unwrap();
    let result = compute_distribution(&mut conn, &[s1, s2, s3], &key, 0, &UserZone::utc()).unwrap();

    // Sorted by count desc — Canon (2) first
    assert_eq!(
        cells(&result),
        [("Canon".to_string(), 2), ("Nikon".to_string(), 1)]
    );
    assert_eq!(without(&result), 0);
    assert_eq!(result.total_sources, 3);
}

#[test]
fn test_compute_distribution_with_limit() {
    let mut conn = setup_test_db();
    let root_id = insert_root(&conn, "/photos", "source", false);
    let obj1 = insert_object(&conn, "h1", false);
    let obj2 = insert_object(&conn, "h2", false);
    let obj3 = insert_object(&conn, "h3", false);
    let obj4 = insert_object(&conn, "h4", false);
    let s1 = insert_source(&conn, root_id, "a.jpg", Some(obj1));
    let s2 = insert_source(&conn, root_id, "b.jpg", Some(obj2));
    let s3 = insert_source(&conn, root_id, "c.jpg", Some(obj3));
    let s4 = insert_source(&conn, root_id, "d.jpg", Some(obj4));

    crate::core::testing::insert_fact(&conn, s1, "content.Make", "Canon");
    crate::core::testing::insert_fact(&conn, s2, "content.Make", "Nikon");
    crate::core::testing::insert_fact(&conn, s3, "content.Make", "Sony");
    crate::core::testing::insert_fact(&conn, s4, "content.Make", "Canon");

    let key = ParsedFactKey::parse("content.Make").unwrap();
    let result =
        compute_distribution(&mut conn, &[s1, s2, s3, s4], &key, 2, &UserZone::utc()).unwrap();

    // Limited to top 2: the tie between Nikon and Sony goes by text, every run.
    assert_eq!(
        cells(&result),
        [("Canon".to_string(), 2), ("Nikon".to_string(), 1)]
    );
    assert_eq!((result.shown, result.total_values), (2, 3));
}

/// A size counts by `|bucket`'s ranges, the header naming the default.
#[test]
fn test_compute_distribution_source_size_buckets() {
    let mut conn = setup_test_db();
    let root_id = insert_root(&conn, "/photos", "source", false);
    let obj1 = insert_object(&conn, "h1", false);
    let obj2 = insert_object(&conn, "h2", false);
    let obj3 = insert_object(&conn, "h3", false);

    use crate::core::testing::insert_source_with_size;
    let s1 = insert_source_with_size(&conn, root_id, "tiny.txt", Some(obj1), 500);
    let s2 = insert_source_with_size(&conn, root_id, "medium.jpg", Some(obj2), 512_000);
    let s3 = insert_source_with_size(&conn, root_id, "large.raw", Some(obj3), 5_242_880);

    let key = defaulted("source.size");
    let result = compute_distribution(&mut conn, &[s1, s2, s3], &key, 0, &UserZone::utc()).unwrap();

    assert_eq!(result.header, "source.size|bucket");
    assert_eq!(
        cells(&result),
        [
            ("100-to-1K".to_string(), 1),
            ("100K-to-1M".to_string(), 1),
            ("1M-to-10M".to_string(), 1)
        ]
    );
    assert_eq!(without(&result), 0);
}

/// A modification time counts by year, the header naming the default.
#[test]
fn test_compute_distribution_source_mtime_year() {
    let mut conn = setup_test_db();
    let root_id = insert_root(&conn, "/photos", "source", false);
    let obj1 = insert_object(&conn, "h1", false);
    let obj2 = insert_object(&conn, "h2", false);
    let obj3 = insert_object(&conn, "h3", false);

    // 2023-06-15, 2024-01-01, 2024-07-01
    use crate::core::testing::insert_source_with_metadata;
    let s1 = insert_source_with_metadata(&conn, root_id, "a.jpg", Some(obj1), 1000, 1686830400);
    let s2 = insert_source_with_metadata(&conn, root_id, "b.jpg", Some(obj2), 1000, 1704067200);
    let s3 = insert_source_with_metadata(&conn, root_id, "c.jpg", Some(obj3), 1000, 1719792000);

    let key = defaulted("source.mtime");
    let result = compute_distribution(&mut conn, &[s1, s2, s3], &key, 0, &UserZone::utc()).unwrap();

    assert_eq!(result.header, "source.mtime|year");
    assert_eq!(
        cells(&result),
        [("2024".to_string(), 2), ("2023".to_string(), 1)]
    );
}

#[test]
fn test_compute_distribution_with_transforms() {
    let mut conn = setup_test_db();
    let root_id = insert_root(&conn, "/photos", "source", false);
    let obj1 = insert_object(&conn, "h1", false);
    let obj2 = insert_object(&conn, "h2", false);
    let obj3 = insert_object(&conn, "h3", false);
    let s1 = insert_source(&conn, root_id, "a.jpg", Some(obj1));
    let s2 = insert_source(&conn, root_id, "b.jpg", Some(obj2));
    let s3 = insert_source(&conn, root_id, "c.jpg", Some(obj3));

    // Insert time facts for different years
    insert_time_fact(&conn, s1, "content.DateTimeOriginal", 1686830400);
    insert_time_fact(&conn, s2, "content.DateTimeOriginal", 1704067200);
    insert_time_fact(&conn, s3, "content.DateTimeOriginal", 1719792000);

    let key = ParsedFactKey::parse_as("content.DateTimeOriginal|year", FactType::Time).unwrap();
    let result = compute_distribution(&mut conn, &[s1, s2, s3], &key, 0, &UserZone::utc()).unwrap();

    assert_eq!(
        cells(&result),
        [("2024".to_string(), 2), ("2023".to_string(), 1)]
    );
}

/// `facts --key source.mtime` groups a save time by its year on the user's clock:
/// 23:30 UTC on New Year's Eve is half past midnight on 1 January in Amsterdam.
#[test]
fn the_save_time_groups_by_its_year_on_the_users_clock() {
    let mut conn = setup_test_db();
    let root_id = insert_root(&conn, "/photos", "source", false);
    let obj = insert_object(&conn, "h1", false);
    // 2023-12-31T23:30:00Z
    let s1 = crate::core::testing::insert_source_with_metadata(
        &conn,
        root_id,
        "a.jpg",
        Some(obj),
        1000,
        1704065400,
    );

    let key = defaulted("source.mtime");
    let amsterdam = UserZone::for_test("Europe/Amsterdam");
    let result = compute_distribution(&mut conn, &[s1], &key, 0, &amsterdam).unwrap();
    assert_eq!(result.rows[0].cell, "2024");

    let utc = compute_distribution(&mut conn, &[s1], &key, 0, &UserZone::utc()).unwrap();
    assert_eq!(utc.rows[0].cell, "2023");
}

/// A save time Canon cannot read has no value: the report counts it under `(no value)`, and
/// the key listing counts `source.mtime` at one fewer than the sources — the same answer
/// as the key's own `(no value)` line. The sentinel is 9999-12-31 23:59:59 UTC.
#[test]
fn a_save_time_canon_cannot_read_is_not_counted() {
    use crate::core::testing::insert_source_with_metadata;
    let mut conn = setup_test_db();
    let root_id = insert_root(&conn, "/photos", "source", false);
    let obj1 = insert_object(&conn, "h1", false);
    let obj2 = insert_object(&conn, "h2", false);
    let s1 = insert_source_with_metadata(&conn, root_id, "a.jpg", Some(obj1), 1000, 1704065400);
    let s2 = insert_source_with_metadata(&conn, root_id, "b.jpg", Some(obj2), 1000, 253402300799);

    let key = ParsedFactKey::parse("source.mtime").unwrap();
    let result = compute_distribution(&mut conn, &[s1, s2], &key, 0, &UserZone::utc()).unwrap();
    assert_eq!(without(&result), 1);
    assert_eq!(result.total_sources, 2);

    let year = defaulted("source.mtime");
    let result = compute_distribution(&mut conn, &[s1, s2], &year, 0, &UserZone::utc()).unwrap();
    assert_eq!(without(&result), 1);

    let keys = compute_all_keys(&mut conn, &[s1, s2], false).unwrap();
    let count_of = |name: &str| keys.keys.iter().find(|k| k.key == name).unwrap().count;
    assert_eq!(count_of("source.mtime"), 1);
    assert_eq!(count_of("source.ext"), 2);
}

/// A text modifier after a modifier reads that modifier's printed form, in a report as
/// in a filter and a pattern: `|weekday|lowercase` reports `monday`.
#[test]
fn a_text_modifier_after_a_modifier_reports_its_printed_form() {
    use crate::core::testing::insert_source_with_metadata;
    let mut conn = setup_test_db();
    let root_id = insert_root(&conn, "/photos", "source", false);
    // 1718582400 is Monday 17 June 2024, 00:00 UTC.
    let s1 = insert_source_with_metadata(&conn, root_id, "a.jpg", None, 1000, 1718582400);
    let key = ParsedFactKey::parse("source.mtime|weekday|lowercase").unwrap();
    let result = compute_distribution(&mut conn, &[s1], &key, 0, &UserZone::utc()).unwrap();
    assert_eq!(cells(&result), [("monday".to_string(), 1)]);
}

/// A report key's type that changes between reading the key and counting stops the report
/// as an answer, never a modifier failing, on the plain and the grouped report alike — the
/// same answer a filter gives.
#[test]
fn a_type_that_changes_mid_report_refuses_the_report() {
    use crate::core::testing::insert_fact;
    let mut conn = setup_test_db();
    let root_id = insert_root(&conn, "/photos", "source", false);
    let s1 = insert_source(&conn, root_id, "a.jpg", None);
    let s2 = insert_source(&conn, root_id, "b.jpg", None);
    insert_fact(&conn, s1, "content.Make", "Canon");
    insert_fact(&conn, s2, "content.Make", "Nikon");
    let read = crate::expr::read_report(
        &conn,
        Some("Make|lowercase"),
        &["Make|uppercase".into()],
        &ExpressionText::plain(&[]),
    )
    .unwrap();
    let (key, group) = (read.key.unwrap(), read.grouping);
    let group: Vec<Position> = group.into_iter().map(Position::Key).collect();
    // The key's type changes after it was read.
    conn.execute(
        "UPDATE facts SET value_text = NULL, value_num = 5 WHERE entity_id = ?",
        [s2],
    )
    .unwrap();
    let plain = compute_distribution(&mut conn, &[s1, s2], &key, 0, &UserZone::utc());
    let bare = ParsedFactKey::parse("source.ext").unwrap();
    let grouped =
        compute_grouped_distribution(&mut conn, &[s1, s2], &bare, &group, 0, &UserZone::utc());
    // A key with no facts is unknown when a report reads it, so a report key never reads
    // as having none and cannot gain its first facts mid-report; the filter pins that arm.
    for err in [plain.err(), grouped.err()] {
        let err = err.expect("a changed type must stop the report");
        let refused = err
            .downcast_ref::<crate::expr::ExpressionRefused>()
            .unwrap_or_else(|| panic!("{err}"));
        assert!(refused.is_answer());
        assert_eq!(
            refused.to_string(),
            "content.Make's type changed while this ran; run it again"
        );
    }
}

/// A modifier finer than the value was written gives no value, and the report does
/// not count that source as having one.
#[test]
fn a_value_too_coarse_for_its_modifier_is_not_counted() {
    let mut conn = setup_test_db();
    let root_id = insert_root(&conn, "/photos", "source", false);
    let obj1 = insert_object(&conn, "h1", false);
    let obj2 = insert_object(&conn, "h2", false);
    let s1 = insert_source(&conn, root_id, "a.jpg", Some(obj1));
    let s2 = insert_source(&conn, root_id, "b.jpg", Some(obj2));
    insert_time_fact(&conn, s1, "content.DateTimeOriginal", 1719792000);
    // The clock reading "2024", known only to the year.
    conn.execute(
        "INSERT INTO facts (entity_type, entity_id, key, value_time, time_form, time_precision,
                            observed_at, observed_basis_rev)
         VALUES ('source', ?1, 'content.DateTimeOriginal', 1704067200, 'clock', 'year', 0, 0)",
        rusqlite::params![s2],
    )
    .unwrap();

    let month = ParsedFactKey::parse_as("content.DateTimeOriginal|month", FactType::Time).unwrap();
    let result = compute_distribution(&mut conn, &[s1, s2], &month, 0, &UserZone::utc()).unwrap();
    assert_eq!(without(&result), 1);
    assert_eq!(result.rows.len(), 1);

    let year = ParsedFactKey::parse_as("content.DateTimeOriginal|year", FactType::Time).unwrap();
    let result = compute_distribution(&mut conn, &[s1, s2], &year, 0, &UserZone::utc()).unwrap();
    assert_eq!(without(&result), 0);
}

/// A hashed source carrying its content hash, the way scanning stores it.
fn hashed_source(conn: &Connection, root_id: i64, rel_path: &str, hash: &str) -> i64 {
    let obj = insert_object(conn, hash, false);
    conn.execute(
        "INSERT INTO facts (entity_type, entity_id, key, value_text, observed_at, observed_basis_rev)
         VALUES ('object', ?1, 'content.hash.sha256', ?2, 0, NULL)",
        rusqlite::params![obj, hash],
    )
    .unwrap();
    insert_source(conn, root_id, rel_path, Some(obj))
}

/// Every built-in key has a value in `facts --key`, alone and grouped.
/// The defect it names: the report split computed keys from stored facts by
/// listing visibility (and the grouped path by "is a computed key"), so the
/// unlisted aliases (`stem`, `size`, `hash`) and the content hash went
/// to a lookup that could never answer, and printed "(no value)".
#[test]
fn every_built_in_key_has_a_value_in_the_report() {
    let mut conn = setup_test_db();
    let root_id = insert_root(&conn, "/photos", "source", false);
    let full = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";
    let s1 = hashed_source(&conn, root_id, "2024/a.jpg", full);

    for every in Key::all().chain([Key::ContentFact(crate::expr::CONTENT_HASH.to_string())]) {
        let name = every.name();
        let key = defaulted(name);
        let alone = compute_distribution(&mut conn, &[s1], &key, 0, &UserZone::utc()).unwrap();
        assert_eq!(without(&alone), 0, "facts --key {name}");

        let grouped = compute_grouped_distribution(
            &mut conn,
            &[s1],
            &key,
            &[Position::Root],
            0,
            &UserZone::utc(),
        )
        .unwrap();
        assert!(
            grouped.no_value_group.is_none(),
            "facts --key {name} --by-root"
        );
        assert_eq!(grouped.groups.len(), 1, "facts --key {name} --by-root");
    }

    for (name, want) in [
        ("hash", full),
        ("hash_short", "2cf24dba"),
        ("content.hash.sha256", full),
        ("size", "1K-to-10K"),
        ("mtime", "2024"),
        ("stem", "a"),
    ] {
        let key = defaulted(name);
        let result = compute_distribution(&mut conn, &[s1], &key, 0, &UserZone::utc()).unwrap();
        assert_eq!(result.rows[0].cell, want, "facts --key {name}");
    }
}

// =========================================================================
// The plain table: header, rows, order, marks and lines
// =========================================================================

/// **The header names the expression counted**, defaults included, and the annotation names
/// the rows' type and the key's kind: `source.mtime|year … (number, source fact)`,
/// `size|bucket … (range, built-in key)`, `content.Model … (text)`, `mixed` on a bare key of
/// mixed type — one per kind of row a key's chain gives.
#[test]
fn the_header_names_the_expression_and_what_its_rows_are() {
    let mut library = Library::new();
    library.text("content.Model", "EOS");
    library.number("content.Width", 4000.0);
    library.time("content.Taken", "2024-06-15T14:30:05");
    library.text("content.Mixed", "a");
    library.number("content.Mixed", 5.0);
    for (key, header, rows, kind) in [
        (
            "source.mtime",
            "source.mtime|year",
            "number",
            Some(KeyKind::SourceFact),
        ),
        ("size", "size|bucket", "range", Some(KeyKind::BuiltinKey)),
        ("content.Model", "content.Model", "text", None),
        ("filename", "filename", "text", Some(KeyKind::BuiltinKey)),
        (
            "source.rel_path",
            "source.rel_path",
            "path",
            Some(KeyKind::SourceFact),
        ),
        ("Width", "Width", "number", None),
        ("content.Taken", "content.Taken", "time", None),
        ("content.Taken|date", "content.Taken|date", "time", None),
        (
            "content.Taken|time",
            "content.Taken|time",
            "time of day",
            None,
        ),
        (
            "content.Taken|weekday",
            "content.Taken|weekday",
            "number",
            None,
        ),
        ("content.Mixed", "content.Mixed", "mixed", None),
    ] {
        let report = library.distribution(key, 0);
        assert_eq!(report.header, header, "{key}");
        assert_eq!(report.annotation.rows, rows, "{key}");
        assert_eq!(report.annotation.kind, kind, "{key}");
    }
}

/// The empty value is a value, printed `""` — never a label — and the sources with none
/// print last, with their line.
#[test]
fn an_empty_value_prints_as_empty_quotes_and_no_value_has_its_line() {
    let mut library = Library::new();
    library.text("content.Model", "");
    library.text("content.Model", "EOS");
    library.text("content.Model", "EOS");
    library.file("none.jpg");
    let report = library.distribution("content.Model", 0);
    assert_eq!(
        cells(&report),
        [
            ("EOS".to_string(), 2),
            ("\"\"".to_string(), 1),
            ("(no value)".to_string(), 1)
        ]
    );
    assert_eq!(
        printed_lines(&report.lines),
        ["(no value): NOT content.Model?"]
    );
}

/// **Ties print in the same order on every run**: numbers by value (`9` before `10`), times by
/// start then text (`2024` before `2024-01-01`), text by character, buckets by range.
#[test]
fn ties_print_in_the_value_order_on_every_run() {
    let mut library = Library::new();
    for n in [10.0, 9.0, 100.0] {
        library.number("content.N", n);
    }
    for written in ["2024-01-01", "2024", "2023-12-31"] {
        library.time("content.T", written);
    }
    for text in ["b", "B", "a"] {
        library.text("content.S", text);
    }
    for (rel, size) in [("z.jpg", 20_000), ("y.jpg", 0), ("x.jpg", 5)] {
        let root = library.root;
        library.file_in(root, rel, size, 1_704_067_200);
    }
    for (key, want) in [
        ("content.N", vec!["9", "10", "100"]),
        ("content.T", vec!["2023-12-31", "2024", "2024-01-01"]),
        ("content.S", vec!["B", "a", "b"]),
    ] {
        for _ in 0..3 {
            let report = library.distribution(key, 0);
            let printed: Vec<&str> = report.rows.iter().map(|row| row.cell.as_str()).collect();
            assert_eq!(printed, want, "{key}");
        }
    }
    let sizes = library.distribution("size", 0);
    let printed: Vec<&str> = sizes.rows.iter().map(|row| row.cell.as_str()).collect();
    // Each of the 1000-byte files counts in 1K-to-10K; the planted three each in its own.
    assert_eq!(printed, ["1K-to-10K", "0", "1-to-10", "10K-to-100K"]);
}

/// **`--limit` cuts value rows only**: `(no value)` stays, and the cut is counted for the notice.
#[test]
fn the_limit_keeps_no_value_and_counts_the_cut() {
    let mut library = Library::new();
    for text in ["a", "a", "b", "c"] {
        library.text("content.S", text);
    }
    library.file("none.jpg");
    let report = library.distribution("content.S", 1);
    assert_eq!(
        cells(&report),
        [("a".to_string(), 2), ("(no value)".to_string(), 1)]
    );
    assert_eq!((report.shown, report.total_values), (1, 3));
}

/// **A coarse time is marked `+`**, its typed filter the exact one, and one line per grain
/// marked: a year beside a day of it, a month beside a day, a day beside a minute, a minute
/// beside a second — each line naming the next finer part — and a time of day at the minute
/// beside one at the second. The finer row is unmarked.
#[test]
fn a_coarse_time_is_marked_with_its_line_at_each_grain() {
    for (coarse, finer, grain, part) in [
        ("2024", "2024-06-15", "year", "month"),
        ("2024-06", "2024-06-15", "month", "day"),
        ("2024-06-15", "2024-06-15T14:30", "day", "hour"),
        (
            "2024-06-15T14:30",
            "2024-06-15T14:30:05",
            "minute",
            "second",
        ),
    ] {
        let mut library = Library::new();
        library.time("content.T", coarse);
        library.time("content.T", finer);
        let report = library.distribution("content.T", 0);
        let marks: Vec<Option<Mark>> = report.rows.iter().map(|row| row.mark).collect();
        assert_eq!(marks, [Some(Mark::Coarse), None], "{coarse}");
        let value = &report.rows[0].cell;
        assert_eq!(
            report.rows[0].filter.as_ref().unwrap().text(),
            format!("content.T={value} AND NOT content.T|{part}?")
        );
        assert_eq!(
            printed_lines(&report.lines),
            [format!(
                "Each {grain} row marked +: content.T=<row> AND NOT content.T|{part}?"
            )]
        );
    }

    let mut library = Library::new();
    for written in [
        "2024-07-23T18:30",
        "2024-07-24T18:30",
        "2024-07-23T18:30:45",
    ] {
        library.time("content.Taken", written);
    }
    let report = library.distribution("content.Taken|time", 0);
    assert_eq!(
        cells(&report),
        [("18:30".to_string(), 2), ("18:30:45".to_string(), 1)]
    );
    assert_eq!(report.rows[0].mark, Some(Mark::Coarse));
    assert_eq!(
        printed_lines(&report.lines),
        ["Each minute row marked +: content.Taken|time=<row> AND NOT content.Taken|time|second?"]
    );
}

/// A coarse row with no finer value in its period pastes back exactly and is unmarked; so is
/// every row of a key of bare years only, and a year beside a day of another year.
#[test]
fn a_coarse_time_with_nothing_finer_in_its_period_is_not_marked() {
    let mut library = Library::new();
    for written in ["2023", "2024", "2024", "2025-03-01"] {
        library.time("content.T", written);
    }
    let report = library.distribution("content.T", 0);
    assert!(report.rows.iter().all(|row| row.mark.is_none()));
    assert!(report.lines.is_empty());
}

/// **A value holding a control character is marked `!`**: its cell shows the control pictures,
/// its filter is the wider glob, and its line says so — on a text value and on a path.
#[test]
fn a_value_holding_a_control_character_is_marked_with_its_glob() {
    let mut library = Library::new();
    library.text("content.Note", "line1\nline2");
    library.text("content.Note", "plain");
    let report = library.distribution("content.Note", 0);
    let marked = report
        .rows
        .iter()
        .find(|row| row.mark == Some(Mark::Unshowable))
        .unwrap();
    assert_eq!(marked.cell, "\"line1\u{240A}line2\"");
    assert!(!marked.filter.as_ref().unwrap().is_exact());
    assert_eq!(
        printed_lines(&report.lines),
        ["Row ! \"line1\u{240A}line2\" (may select more): content.Note~\"line1\"?\"line2\""]
    );

    let root = library.root;
    library.file_in(root, "dir/a\nb.jpg", 1000, 1_704_067_200);
    let report = library.distribution("source.rel_path", 0);
    assert_eq!(
        line_filter(
            &report.lines,
            "Row ! \"dir/a\u{240A}b.jpg\" (may select more)"
        ),
        "source.rel_path~\"dir/a\"?\"b.jpg\""
    );
}

/// A long value prints whole, however wide.
#[test]
fn a_long_value_prints_whole() {
    let mut library = Library::new();
    let long = "Ä long value written in Ünïcode, longer than any column was: ".repeat(3);
    library.text("content.Long", &long);
    let report = library.distribution("content.Long", 0);
    assert_eq!(report.rows[0].cell, format!("\"{long}\""));
}

/// **A key of mixed type** prints its values, each with its type, carries no filter, and
/// states the type and the way out above the table; its `(no value)` row keeps its line.
#[test]
fn a_mixed_key_states_its_type_above_the_table_and_its_rows_carry_no_filter() {
    let mut library = Library::new();
    library.text("content.Mixed", "a");
    library.number("content.Mixed", 5.0);
    library.file("none.jpg");
    let report = library.distribution("content.Mixed", 0);
    assert!(
        report.mixed[0].contains("is of mixed type"),
        "{:?}",
        report.mixed
    );
    assert!(report
        .mixed
        .iter()
        .any(|line| line.contains("canon facts convert content.Mixed")));
    let typed: Vec<(&str, Option<&str>)> = report
        .rows
        .iter()
        .map(|row| (row.cell.as_str(), row.type_word))
        .collect();
    assert_eq!(typed, [("5", Some("number")), ("a", Some("text"))]);
    assert!(report.rows.iter().all(|row| row.filter.is_none()));
    assert_eq!(
        printed_lines(&report.lines),
        ["(no value): NOT content.Mixed?"]
    );
}

/// **The one default owner**: a modification time counts by year and a size by range, as the
/// main key and as a grouping key, named directly, by its built-in key or through a key alias —
/// and a key written with a modifier, or any other key, counts as written.
#[test]
fn the_default_is_given_once_to_every_key_from_one_place() {
    let mut library = Library::new().with_aliases("taken = \"source.mtime\"\nbig = \"size\"\n");
    library.text("content.Model", "EOS");
    let printed = |keys: crate::facts::ops::report::ReportKeys| -> Vec<String> {
        keys.main
            .iter()
            .map(ParsedFactKey::printed)
            .chain(keys.grouping.iter().map(|position| match position {
                Position::Root => "root".to_string(),
                Position::Key(key) => key.printed(),
            }))
            .collect()
    };
    assert_eq!(
        printed(library.read(
            Some("source.mtime"),
            &["source.size,mtime,size,@taken,@big"],
            true
        )),
        [
            "source.mtime|year",
            "root",
            "source.size|bucket",
            "mtime|year",
            "size|bucket",
            "@taken|year",
            "@big|bucket"
        ]
    );
    assert_eq!(
        printed(library.read(
            Some("source.mtime|datetime"),
            &["source.size|number,content.Model,source.ext"],
            false
        )),
        [
            "source.mtime|datetime",
            "source.size|number",
            "content.Model",
            "source.ext"
        ]
    );
}

/// **The header keeps the user's spelling**: a name that needs Canon's quotes quoted, never
/// swapped for the full stored key; a built-in key short; an alias as typed.
#[test]
fn the_header_keeps_the_name_as_the_user_spelled_it() {
    let mut library = Library::new().with_aliases("model = '\"content.Camera Model\"'\n");
    library.number("content.File Size", 1000.0);
    library.text("content.Camera Model", "EOS");
    for (typed, header) in [
        ("content.File Size", "\"content.File Size\""),
        ("\"content.File Size\"", "\"content.File Size\""),
        ("File Size", "\"File Size\""),
        ("'File Size'", "\"File Size\""),
        ("hash_short", "hash_short"),
        ("\"mtime\"", "mtime|year"),
        ("@model", "@model"),
        ("@model|lowercase", "@model|lowercase"),
        ("source.rel_path[:-1]", "source.rel_path[:-1]"),
        ("size|bucket( 60 , 300 )", "size|bucket(60,300)"),
    ] {
        let report = library.distribution(typed, 0);
        assert_eq!(report.header, header, "--key {typed}");
    }
}

// =========================================================================
// compute_grouped_distribution tests
// =========================================================================

#[test]
fn test_compute_grouped_basic() {
    let mut conn = setup_test_db();
    let root1 = insert_root(&conn, "/photos", "source", false);
    let root2 = insert_root(&conn, "/backup", "source", false);
    let obj1 = insert_object(&conn, "h1", false);
    let obj2 = insert_object(&conn, "h2", false);
    let obj3 = insert_object(&conn, "h3", false);
    let s1 = insert_source(&conn, root1, "a.jpg", Some(obj1));
    let s2 = insert_source(&conn, root1, "b.png", Some(obj2));
    let s3 = insert_source(&conn, root2, "c.jpg", Some(obj3));

    let main_key = ParsedFactKey::parse("source.ext").unwrap();
    let result = compute_grouped_distribution(
        &mut conn,
        &[s1, s2, s3],
        &main_key,
        &[Position::Root],
        0,
        &UserZone::utc(),
    )
    .unwrap();

    // "jpg" should have total 2 (from both roots), "png" 1
    assert_eq!(result.groups.len(), 2);
    assert_eq!(result.groups[0].part.cell, "jpg");
    assert_eq!(result.groups[0].total, 2);
    assert_eq!(result.groups[0].rows.len(), 2); // one per root
    assert_eq!(result.groups[1].part.cell, "png");
    assert_eq!(result.groups[1].total, 1);
    assert!(result.no_value_group.is_none());
}

/// A source holding a key itself and on its content counts once, under the content's
/// value, through a modifier and as either key of a grouping.
#[test]
fn a_key_held_twice_reports_the_one_value_through_a_modifier_and_a_grouping() {
    use crate::core::testing::{insert_fact, insert_object_fact};
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    let obj = insert_object(&conn, "h1", false);
    let s = insert_source(&conn, root, "a.jpg", Some(obj));
    insert_fact(&conn, s, "content.Make", "Nikon");
    insert_object_fact(&conn, obj, "content.Make", "Canon");
    insert_fact(&conn, s, "content.Model", "D850");
    insert_object_fact(&conn, obj, "content.Model", "R5");
    let zone = UserZone::utc();
    let key = |k: &str| ParsedFactKey::parse(k).unwrap();

    let lowered =
        compute_distribution(&mut conn, &[s], &key("content.Make|lowercase"), 100, &zone).unwrap();
    assert_eq!(cells(&lowered), [("canon".to_string(), 1)]);

    let grouped = compute_grouped_distribution(
        &mut conn,
        &[s],
        &key("content.Make"),
        &[Position::Key(key("content.Model"))],
        0,
        &zone,
    )
    .unwrap();
    let rows: Vec<(String, Vec<String>, i64)> = grouped
        .groups
        .iter()
        .flat_map(|g| {
            g.rows
                .iter()
                .map(move |row| (g.part.cell.clone(), cells_of(&row.parts), row.count))
        })
        .collect();
    assert_eq!(rows, [("Canon".to_string(), vec!["R5".to_string()], 1)]);
}

/// The sources with no main value form the `(no value)` group, last, with rows of their own.
#[test]
fn test_compute_grouped_missing_main_value() {
    let mut conn = setup_test_db();
    let root_id = insert_root(&conn, "/photos", "source", false);
    let obj1 = insert_object(&conn, "h1", false);
    let obj2 = insert_object(&conn, "h2", false);
    let s1 = insert_source(&conn, root_id, "a.jpg", Some(obj1));
    let s2 = insert_source(&conn, root_id, "b.jpg", Some(obj2));

    // Only s1 has the main key value
    crate::core::testing::insert_fact(&conn, s1, "content.Make", "Canon");

    let main_key = ParsedFactKey::parse("content.Make").unwrap();
    let result = compute_grouped_distribution(
        &mut conn,
        &[s1, s2],
        &main_key,
        &[Position::Root],
        0,
        &UserZone::utc(),
    )
    .unwrap();

    assert_eq!(result.groups.len(), 1);
    assert_eq!(result.groups[0].part.cell, "Canon");
    assert_eq!(result.groups[0].total, 1);
    let no_value = result.no_value_group.unwrap();
    assert_eq!(
        (no_value.part.cell.as_str(), no_value.total),
        ("(no value)", 1)
    );
    assert_eq!(cells_of(&no_value.rows[0].parts), ["id:1  /photos"]);
}

/// A grouping key takes its default too, and the header names it: `(grouped by
/// source.mtime|year)`; the conjunction lines say how to ask for an indented row, under a value
/// group and under `(no value)`.
#[test]
fn a_grouped_table_names_its_positions_and_writes_its_conjunctions() {
    let mut library = Library::new();
    let root = library.root;
    library.file_in(root, "a.jpg", 1000, 1_293_840_000); // 2011-01-01
    library.file_in(root, "b.jpg", 1000, 1_325_376_000); // 2012-01-01
    library.file_in(root, "c.png", 1000, 1_293_840_000);
    library.file_in(root, "d", 1000, 1_293_840_000);
    let grouped = library.grouped("source.ext", &["source.mtime"], false, 0);
    assert_eq!(grouped.header, "source.ext (grouped by source.mtime|year)");
    let groups: Vec<(&str, i64, Vec<String>)> = grouped
        .groups
        .iter()
        .chain(&grouped.no_value_group)
        .map(|group| {
            (
                group.part.cell.as_str(),
                group.total,
                group.rows.iter().map(|row| joined(&row.parts)).collect(),
            )
        })
        .collect();
    assert_eq!(
        groups,
        [
            ("jpg", 2, vec!["2011".to_string(), "2012".to_string()]),
            ("png", 1, vec!["2011".to_string()]),
            ("(no value)", 1, vec!["2011".to_string()]),
        ]
    );
    assert_eq!(
        printed_lines(&grouped.lines),
        [
            "Each indented row: source.ext=<value> AND source.mtime|year=<row>",
            "Each indented row under (no value): NOT source.ext? AND source.mtime|year=<row>",
            "(no value): NOT source.ext?",
        ]
    );
}

/// **With two positions or more, each placeholder is its part's place on the row**, the root
/// counted, so `<2nd>` is the second part printed; a `(no value)` part sorts after every value,
/// though it counts more, and gets its own conjunction, and the root has its line.
#[test]
fn placeholders_number_the_printed_parts_and_a_no_value_part_sorts_last() {
    let mut library = Library::new();
    library.text("content.Make", "Canon");
    library.file("plain.jpg");
    library.file("other.jpg");
    let grouped = library.grouped("source.ext", &["size,Make"], true, 0);
    assert_eq!(
        grouped.header,
        "source.ext (grouped by root, size|bucket, Make)"
    );
    let rows: Vec<String> = grouped.groups[0]
        .rows
        .iter()
        .map(|row| joined(&row.parts))
        .collect();
    assert_eq!(
        rows,
        [
            "id:1  /photos / 1K-to-10K / Canon",
            "id:1  /photos / 1K-to-10K / (no value)"
        ]
    );
    assert_eq!(
        printed_lines(&grouped.lines),
        [
            "Each indented row: source.ext=<value> AND root_id=<N> AND size|bucket=<2nd> AND Make=<3rd>",
            "Each indented (no value) row: source.ext=<value> AND root_id=<N> AND size|bucket=<2nd> AND NOT Make?",
            "id:<N> (not id=, which names a source): root_id=<N>",
        ]
    );
}

/// **Inside a group, the first part where only one of two rows has a value decides their
/// order**, whatever their counts: a row with a year and no make before every row with no year,
/// in a value group and in the `(no value)` group.
#[test]
fn the_first_part_where_only_one_row_has_a_value_decides_its_place() {
    let mut library = Library::new();
    let both = library.text("content.Season", "spring");
    crate::core::testing::insert_fact(&library.conn, both, "content.Make", "Canon");
    library.text("content.Season", "spring");
    library.text("content.Make", "Canon");
    library.text("content.Make", "Canon");
    library.file("a.jpg");
    library.file("b.jpg");
    library.file("c.jpg");
    let make = library.file("d");
    crate::core::testing::insert_fact(&library.conn, make, "content.Make", "Canon");
    library.file("e");
    library.file("f");
    let grouped = library.grouped("source.ext", &["content.Season,content.Make"], false, 0);
    let rows = |group: &Group| -> Vec<(String, i64)> {
        group
            .rows
            .iter()
            .map(|row| (joined(&row.parts), row.count))
            .collect()
    };
    let row = |parts: &str, count: i64| (parts.to_string(), count);
    assert_eq!(
        rows(&grouped.groups[0]),
        [
            row("spring / Canon", 1),
            row("spring / (no value)", 1),
            row("(no value) / Canon", 2),
            row("(no value) / (no value)", 3),
        ]
    );
    assert_eq!(
        rows(grouped.no_value_group.as_ref().expect("a (no value) group")),
        [
            row("(no value) / Canon", 1),
            row("(no value) / (no value)", 2)
        ]
    );
}

/// A content fact named `root` prints its full name in a grouped header, so it never reads as
/// the `--by-root` position.
#[test]
fn a_content_fact_named_root_prints_its_full_name_in_a_grouped_header() {
    let mut library = Library::new();
    library.text("content.root", "x");
    let grouped = library.grouped("source.ext", &["root"], true, 0);
    assert_eq!(grouped.header, "source.ext (grouped by root, content.root)");
}

/// **A grouped table with a key of mixed type** states it above the table and prints no
/// conjunction holding that key's value; its `(no value)` conjunction stays.
#[test]
fn a_mixed_grouping_key_prints_no_conjunction_holding_its_value() {
    let mut library = Library::new();
    library.text("content.Mixed", "a");
    library.number("content.Mixed", 5.0);
    library.file("none.jpg");
    let grouped = library.grouped("source.ext", &["content.Mixed"], false, 0);
    assert!(grouped.mixed[0].contains("is of mixed type"));
    assert_eq!(
        printed_lines(&grouped.lines),
        ["Each indented (no value) row: source.ext=<value> AND NOT content.Mixed?"]
    );
    let filters: Vec<bool> = grouped.groups[0]
        .rows
        .iter()
        .map(|row| row.filter.is_some())
        .collect();
    assert_eq!(filters, [false, false, true]);
}

/// **A coarse main value marks its group** `+`, with the group's line.
#[test]
fn a_coarse_main_value_marks_its_group() {
    let mut library = Library::new();
    library.time("content.T", "2024");
    library.time("content.T", "2024-06-15");
    let grouped = library.grouped("content.T", &["source.ext"], false, 0);
    let marks: Vec<Option<Mark>> = grouped.groups.iter().map(|group| group.mark).collect();
    assert_eq!(marks, [Some(Mark::Coarse), None]);
    assert!(printed_lines(&grouped.lines).contains(
        &"Each year group marked +: content.T=<value> AND NOT content.T|month?".to_string()
    ));
}

/// **A coarse part marks its row only where a finer value sits in the same group**: under
/// `jpg` the year `2024` beside `2024-06-15` is marked, while under `png` the year whose finer
/// value sits in another group is not.
#[test]
fn a_coarse_part_marks_its_row_only_beside_a_finer_value_in_its_group() {
    let mut library = Library::new();
    let root = library.root;
    for (rel, written) in [
        ("a.jpg", "2024"),
        ("b.jpg", "2024-06-15"),
        ("c.png", "2024"),
        ("d.gif", "2024-06-15"),
    ] {
        let id = library.file_in(root, rel, 1000, 1_704_067_200);
        crate::core::testing::insert_time_fact(&library.conn, id, "content.T", written);
    }
    let grouped = library.grouped("source.ext", &["content.T"], false, 0);
    let marked: Vec<(String, String, Option<Mark>)> = grouped
        .groups
        .iter()
        .flat_map(|group| {
            group
                .rows
                .iter()
                .map(move |row| (group.part.cell.clone(), joined(&row.parts), row.mark))
        })
        .collect();
    assert_eq!(
        marked,
        [
            ("jpg".to_string(), "2024".to_string(), Some(Mark::Coarse)),
            ("jpg".to_string(), "2024-06-15".to_string(), None),
            ("gif".to_string(), "2024-06-15".to_string(), None),
            ("png".to_string(), "2024".to_string(), None),
        ]
    );
    assert!(printed_lines(&grouped.lines).contains(
        &"Each indented year row marked +: source.ext=<value> AND content.T=<row> AND NOT content.T|month?"
            .to_string()
    ));
}

/// **A row coarse at two terms names both grains**, in print order, its line exact at both.
#[test]
fn a_row_coarse_at_two_terms_names_both_grains() {
    let mut library = Library::new();
    for (main, part) in [("2024", "2024-06-15"), ("2024-03-01", "2024-06-15T10:00")] {
        let id = library.time("content.T", main);
        crate::core::testing::insert_time_fact(&library.conn, id, "content.U", part);
    }
    let grouped = library.grouped("content.T", &["content.U"], false, 0);
    assert_eq!(grouped.groups[0].rows[0].mark, Some(Mark::Coarse));
    assert!(printed_lines(&grouped.lines).contains(
        &"Each indented year and day row marked +: content.T=<value> AND NOT content.T|month? AND content.U=<row> AND NOT content.U|hour?"
            .to_string()
    ), "{:?}", printed_lines(&grouped.lines));
}

/// **A part holding a control character marks its row `!`**, the line the conjunction with the
/// glob in that part's place.
#[test]
fn a_part_holding_a_control_character_marks_its_row() {
    let mut library = Library::new();
    library.text("content.Note", "a\nb");
    let grouped = library.grouped("source.ext", &["content.Note"], false, 0);
    let row = &grouped.groups[0].rows[0];
    assert_eq!(cells_of(&row.parts), ["\"a\u{240A}b\""]);
    assert_eq!(row.mark, Some(Mark::Unshowable));
    assert!(printed_lines(&grouped.lines).contains(
        &"Row ! jpg / \"a\u{240A}b\" (may select more): source.ext=jpg AND content.Note~\"a\"?\"b\""
            .to_string()
    ));
}

/// **`--limit` cuts main groups only**, counted for the notice; the `(no value)` group stays.
#[test]
fn the_limit_cuts_groups_and_counts_the_cut() {
    let mut library = Library::new();
    let root = library.root;
    for rel in ["a.jpg", "b.jpg", "c.png", "d.gif", "e"] {
        library.file_in(root, rel, 1000, 1_704_067_200);
    }
    let grouped = library.grouped("source.ext", &["source.mtime"], false, 1);
    assert_eq!(grouped.groups.len(), 1);
    assert_eq!((grouped.shown, grouped.total_groups), (1, 3));
    assert!(grouped.no_value_group.is_some());
}

// =========================================================================
// The per-root table
// =========================================================================

/// **The per-root table** prints each root's full path, control pictures in place, its filter
/// `root_id=<id>` and the root line; a root row is never marked.
#[test]
fn the_per_root_table_prints_full_paths_and_its_line() {
    let mut library = Library::new();
    let long = format!("/Volumes/{}/a\nb", "very-long-folder-name/".repeat(4));
    let second = library.add_root(&long);
    library.file("a.jpg");
    library.file_in(second, "b.jpg", 1000, 1_704_067_200);
    library.file_in(second, "c.jpg", 1000, 1_704_067_200);
    let ids = library.all();
    let fetched = crate::core::repo::source::batch_fetch_by_ids(&library.conn, &ids).unwrap();
    let sources: Vec<_> = fetched.into_values().collect();
    let table = compute_root_distribution(&sources);
    let rows: Vec<(String, usize, String)> = table
        .rows
        .iter()
        .map(|row| (row.cell.clone(), row.count, row.filter.text().to_string()))
        .collect();
    assert_eq!(
        rows,
        [
            (
                format!("id:{second}  {}", long.replace('\n', "\u{240A}")),
                2,
                format!("root_id={second}")
            ),
            ("id:1  /photos".to_string(), 1, "root_id=1".to_string()),
        ]
    );
    assert_eq!(
        printed_lines(&table.lines),
        ["id:<N> (not id=, which names a source): root_id=<N>"]
    );
}

/// **A table whose header key holds a control character** ends its lines with the line saying
/// the key cannot be pasted as printed — plain, grouped with the key in either place — and
/// hands every line and the header as `--where` text, the key as it is; the screen pictures
/// them.
#[test]
fn a_key_holding_a_control_character_closes_its_lines_with_the_key_line() {
    let key_line = "Key in the header: a control character stands where \u{240A} shows, so the key cannot be pasted as printed";
    let mut library = Library::new();
    library.text("content.NL\nKey", "x");
    library.file("none.jpg");
    let report = library.distribution("\"content.NL\nKey\"", 0);
    assert_eq!(report.header, "\"content.NL\nKey\"");
    assert_eq!(
        printed_lines(&report.lines),
        ["(no value): NOT \"content.NL\nKey\"?", key_line]
    );
    assert_eq!(
        report.rows[0].filter.as_ref().unwrap().text(),
        "\"content.NL\nKey\"=x"
    );

    let main = library.grouped("\"content.NL\nKey\"", &["source.ext"], false, 0);
    assert_eq!(printed_lines(&main.lines).last().unwrap(), key_line);
    let part = library.grouped("source.ext", &["\"content.NL\nKey\""], false, 0);
    assert_eq!(printed_lines(&part.lines).last().unwrap(), key_line);
    assert!(printed_lines(&part.lines).contains(
        &"Each indented row: source.ext=<value> AND \"content.NL\nKey\"=<row>".to_string()
    ));

    let plain = library.grouped("source.ext", &["source.mtime"], false, 0);
    assert!(!printed_lines(&plain.lines)
        .iter()
        .any(|line| line.starts_with("Key in the header")));
}

/// **A grouped table over a key of mixed type** carries each value's type beside its cell, in
/// the Type column's words, so values of two types that print alike stay apart; no type on
/// `(no value)` or on a position that is not mixed.
#[test]
fn a_mixed_position_names_each_value_s_type() {
    let mut library = Library::new();
    library.number("content.Mixed", 2019.0);
    library.time("content.Mixed", "2019");
    library.file("none.jpg");
    let typed = |part: &Part| (part.cell.clone(), part.type_word);
    let grouped = library.grouped("content.Mixed", &["source.ext"], false, 0);
    let groups: Vec<_> = grouped
        .groups
        .iter()
        .chain(&grouped.no_value_group)
        .map(|group| {
            (
                typed(&group.part),
                group
                    .rows
                    .iter()
                    .flat_map(|row| row.parts.iter().map(typed))
                    .collect::<Vec<_>>(),
            )
        })
        .collect();
    let jpg = vec![("jpg".to_string(), None)];
    assert_eq!(
        groups,
        [
            (("2019".to_string(), Some("number")), jpg.clone()),
            (("2019".to_string(), Some("time")), jpg.clone()),
            (("(no value)".to_string(), None), jpg),
        ]
    );
    let by_part = library.grouped("source.ext", &["content.Mixed"], false, 0);
    let parts: Vec<_> = by_part.groups[0]
        .rows
        .iter()
        .flat_map(|row| row.parts.iter().map(typed))
        .collect();
    assert_eq!(
        parts,
        [
            ("2019".to_string(), Some("number")),
            ("2019".to_string(), Some("time")),
            ("(no value)".to_string(), None),
        ]
    );
}
