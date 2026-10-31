//! A key's value for a batch of sources: a stored key's read out of the
//! database, any key's as an expression reads it (`fetch_named_values`), a report
//! key's typed (`fetch_row_values`), and how many sources each stored key has a value for
//! (`count_values_by_key`).
//!
//! A source can hold a key twice — a fact on the source and one on its content —
//! and the rule that picks between them (`domain/value.rs`'s `source_value`)
//! is the facility's. Readers outside get the picked value, never the two
//! rows, so none of them can pick differently.

use std::collections::{BTreeMap, HashMap};

use anyhow::Result;
use rusqlite::Connection;

use crate::core::domain::fact::{FactEntry, FactValue};
use crate::core::domain::source::Source;
use crate::core::domain::time::zone::UserZone;
use crate::expr::domain::key::{read_base_key, Key, ParsedFactKey};
use crate::expr::domain::row::RowValue;
use crate::expr::domain::transform::{apply_chain, parse_modifier, Modified};
use crate::expr::domain::value::{row_value, source_row_value, source_value, SourceAttributes};
use crate::expr::repo::{fetch_all_key_rows, fetch_key_rows};

/// **Every source's value for each report key, typed**, in the keys' order: a key the source row
/// answers from that row, any other from its stored rows, each base key's read once and picked
/// by the source-value law; each through the key's accessor and modifiers ([`row_value`]), its
/// type checked against what the key was read as. `None` where a source has no value.
pub fn fetch_row_values(
    conn: &mut Connection,
    sources: &[&Source],
    keys: &[&ParsedFactKey],
    zone: &UserZone,
) -> Result<HashMap<i64, Vec<Option<RowValue>>>> {
    let ids: Vec<i64> = sources.iter().map(|source| source.id).collect();
    let mut stored: HashMap<&str, HashMap<i64, Option<FactEntry>>> = HashMap::new();
    for key in keys {
        let base = key.base_key.as_str();
        let computed = Key::read(base).is_some_and(|read| read.is_computed());
        if !computed && !stored.contains_key(base) {
            stored.insert(base, fetch_source_values(conn, &ids, base)?);
        }
    }
    let mut values = HashMap::with_capacity(sources.len());
    for &source in sources {
        let attributes = SourceAttributes::from(source);
        let mut row = Vec::with_capacity(keys.len());
        for key in keys {
            let value = stored
                .get(key.base_key.as_str())
                .and_then(|of| of.get(&source.id))
                .and_then(Option::as_ref)
                .map(|entry| entry.value.clone());
            row.push(row_value(&attributes, key, value, zone)?);
        }
        values.insert(source.id, row);
    }
    Ok(values)
}

/// **Every stored key the sources hold, and how many of them have a value for it** — exactly what
/// a bare `<key>?` selects: the source's one value by the source-value law, so a source whose
/// content holds a row Canon cannot read has none. A key every row of which is unreadable here
/// is listed with none. In key order.
pub fn count_values_by_key(
    conn: &mut Connection,
    source_ids: &[i64],
) -> Result<Vec<(String, usize)>> {
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    fetch_all_key_rows(conn, source_ids, |key, rows| {
        let has_value = source_value(rows.on_source, rows.on_content).is_some();
        let count = match counts.get_mut(key) {
            Some(count) => count,
            None => counts.entry(key.to_string()).or_default(),
        };
        *count += usize::from(has_value);
    })?;
    Ok(counts.into_iter().collect())
}

/// Every source's one value for a stored key: each id maps to its value, or `None` where
/// it has none. The key is read as given — callers that take a written key parse it first.
pub fn fetch_source_values(
    conn: &mut Connection,
    source_ids: &[i64],
    key: &str,
) -> Result<HashMap<i64, Option<FactEntry>>> {
    let mut rows = fetch_key_rows(conn, source_ids, key)?;
    Ok(source_ids
        .iter()
        .map(|&id| {
            let value = rows
                .remove(&id)
                .and_then(|of| source_value(of.on_source, of.on_content));
            (id, value)
        })
        .collect())
}

/// Every source's value for a key of any kind, as an expression reads it: a key the source
/// row answers from that row (`source_row_value`); any other from the stored facts, read as
/// the filter reads it (`read_base_key`) — `hash` the source's one content hash, `hash_short`
/// that hash through the modifier its reading names (`|short`), a content fact its one value
/// (`fetch_source_values`). Each id maps to its value, or `None` where it has none.
///
/// `zone` is the clock a reading's modifier reads on, as in a filter; `|short`, today's only
/// one, reads none.
pub fn fetch_named_values(
    conn: &mut Connection,
    sources: &[&Source],
    key: &Key,
    zone: &UserZone,
) -> Result<HashMap<i64, Option<FactValue>>> {
    if key.is_computed() {
        return Ok(sources
            .iter()
            .map(|&source| {
                let value = source_row_value(&SourceAttributes::from(source), key);
                (source.id, value)
            })
            .collect());
    }
    let (stored, leading) = read_base_key(key.name());
    let modifiers: Vec<_> = leading
        .map(parse_modifier)
        .transpose()?
        .into_iter()
        .collect();
    let ids: Vec<i64> = sources.iter().map(|source| source.id).collect();
    let mut values = HashMap::with_capacity(ids.len());
    for (id, entry) in fetch_source_values(conn, &ids, &stored)? {
        let value = match entry {
            Some(entry) => {
                match apply_chain(entry.value, None, &modifiers, key.name(), zone)?.value {
                    Modified::Value(value) => Some(value),
                    Modified::NoValue(_) => None,
                }
            }
            None => None,
        };
        values.insert(id, value);
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::domain::time::value::TimeValue;
    use crate::core::repo::source::fetch_by_id;
    use crate::core::testing::{
        insert_fact, insert_object, insert_object_fact, insert_root, insert_source,
        insert_source_with_size, setup_test_db,
    };

    fn text(values: &HashMap<i64, Option<FactEntry>>, id: i64) -> Option<&str> {
        match values[&id].as_ref().map(|e| &e.value) {
            Some(FactValue::Text(s)) => Some(s.as_str()),
            Some(other) => panic!("expected text, got {other:?}"),
            None => None,
        }
    }

    /// The rule applied to stored rows: a source with content reads the content's fact, an
    /// empty file as well, a file with only its own fact that one, and every id asked
    /// about is answered.
    #[test]
    fn each_source_gets_its_one_value() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let content = insert_object(&conn, "datahash", false);
        let empty = insert_object(&conn, "emptyhash", false);
        let two_rows = insert_source_with_size(&conn, root, "a.jpg", Some(content), 100);
        let empty_file = insert_source_with_size(&conn, root, "e.jpg", Some(empty), 0);
        let own_only = insert_source(&conn, root, "o.jpg", None);
        let neither = insert_source(&conn, root, "n.jpg", None);
        insert_object_fact(&conn, content, "content.Make", "Canon");
        insert_fact(&conn, two_rows, "content.Make", "Nikon");
        insert_object_fact(&conn, empty, "content.Make", "Canon");
        insert_fact(&conn, empty_file, "content.Make", "Nikon");
        insert_fact(&conn, own_only, "content.Make", "Sony");

        let ids = [two_rows, empty_file, own_only, neither];
        let values = fetch_source_values(&mut conn, &ids, "content.Make").unwrap();
        assert_eq!(values.len(), ids.len());
        assert_eq!(text(&values, two_rows), Some("Canon"));
        assert_eq!(text(&values, empty_file), Some("Canon"));
        assert_eq!(text(&values, own_only), Some("Sony"));
        assert_eq!(text(&values, neither), None);
    }

    /// Any key's value, as an expression reads it: a source fact and a built-in key from the
    /// source row, `size` the same value as `source.size`; `hash` the content hash in full and
    /// `hash_short` its first eight characters, through `|short`; a plain name the `content.`
    /// fact's one value. A source with no content has no hash.
    #[test]
    fn a_key_of_every_kind_reads_as_an_expression_reads_it() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let content = insert_object(&conn, "0123456789abcdef", false);
        let hashed = insert_source_with_size(&conn, root, "trip/a.jpg", Some(content), 100);
        let unhashed = insert_source(&conn, root, "b.png", None);
        insert_object_fact(&conn, content, "content.hash.sha256", "0123456789abcdef");
        insert_object_fact(&conn, content, "content.Make", "Canon");
        insert_fact(&conn, hashed, "content.Make", "Nikon");
        let a = fetch_by_id(&conn, hashed).unwrap().unwrap();
        let b = fetch_by_id(&conn, unhashed).unwrap().unwrap();

        // `FactValue` has no equality; values are compared by their debug form.
        let zone = UserZone::utc();
        let mut value = |name: &str, id: i64| {
            let key = Key::read(name).unwrap();
            let values = fetch_named_values(&mut conn, &[&a, &b], &key, &zone).unwrap();
            format!("{:?}", values[&id])
        };
        let some = |value: FactValue| format!("{:?}", Some(value));
        let text = |s: &str| some(FactValue::Text(s.to_string()));
        let num = |n: i64| some(FactValue::Num(n as f64));
        let none = format!("{:?}", None::<FactValue>);

        assert_eq!(value("source.size", hashed), num(100));
        assert_eq!(value("size", hashed), num(100));
        assert_eq!(
            value("source.mtime", hashed),
            some(FactValue::Time(
                TimeValue::from_epoch_secs(a.mtime).unwrap()
            ))
        );
        assert_eq!(value("source.ext", hashed), text("jpg"));
        assert_eq!(value("filename", hashed), text("a.jpg"));
        assert_eq!(value("stem", unhashed), text("b"));
        assert_eq!(value("id", hashed), num(hashed));
        assert_eq!(value("root_id", hashed), num(root));
        assert_eq!(value("hash", hashed), text("0123456789abcdef"));
        assert_eq!(value("hash_short", hashed), text("01234567"));
        assert_eq!(value("Make", hashed), text("Canon"));
        assert_eq!(value("hash", unhashed), none);
        assert_eq!(value("hash_short", unhashed), none);
        assert_eq!(value("Make", unhashed), none);
    }

    // =========================================================================
    // count_values_by_key
    // =========================================================================

    /// A stored key's count for each source: no sources, no keys.
    #[test]
    fn count_values_by_key_empty_ids() {
        let mut conn = setup_test_db();
        assert!(count_values_by_key(&mut conn, &[]).unwrap().is_empty());
    }

    /// A fact on a source's content counts the source.
    #[test]
    fn count_values_by_key_basic() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/root", "source", false);
        let object = insert_object(&conn, "abc123", false);
        let source = insert_source(&conn, root, "file.txt", Some(object));
        insert_object_fact(&conn, object, "content.Make", "Canon");
        assert_eq!(
            count_values_by_key(&mut conn, &[source]).unwrap(),
            [("content.Make".to_string(), 1)]
        );
    }

    /// Each source with a value counts once, whichever content holds it.
    #[test]
    fn count_values_by_key_multiple_sources() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/root", "source", false);
        let a = insert_object(&conn, "abc123", false);
        let b = insert_object(&conn, "def456", false);
        let one = insert_source(&conn, root, "file1.txt", Some(a));
        let two = insert_source(&conn, root, "file2.txt", Some(b));
        insert_object_fact(&conn, a, "content.Make", "Canon");
        insert_object_fact(&conn, b, "content.Make", "Nikon");
        assert_eq!(
            count_values_by_key(&mut conn, &[one, two]).unwrap(),
            [("content.Make".to_string(), 2)]
        );
    }

    /// Two sources sharing one content each count, though the fact sits on the content once.
    #[test]
    fn count_values_by_key_shared_object() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/root", "source", false);
        let object = insert_object(&conn, "abc123", false);
        let one = insert_source(&conn, root, "file1.txt", Some(object));
        let two = insert_source(&conn, root, "file2.txt", Some(object));
        insert_object_fact(&conn, object, "content.Make", "Canon");
        assert_eq!(
            count_values_by_key(&mut conn, &[one, two]).unwrap(),
            [("content.Make".to_string(), 2)]
        );
    }

    /// A source holding a key itself and on its content counts once: one source, one value.
    #[test]
    fn count_values_by_key_no_double_count_on_source_and_object() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/root", "source", false);
        let object = insert_object(&conn, "abc123", false);
        let source = insert_source(&conn, root, "file.txt", Some(object));
        insert_fact(&conn, source, "content.mime", "image/jpeg");
        insert_object_fact(&conn, object, "content.mime", "image/jpeg");
        assert_eq!(
            count_values_by_key(&mut conn, &[source]).unwrap(),
            [("content.mime".to_string(), 1)]
        );
    }

    /// **A key's count is what a bare `<key>?` selects**: a source whose content holds a row Canon
    /// cannot read has no value, though its own row holds one, and a key whose only row is
    /// unreadable lists with none.
    #[test]
    fn a_row_canon_cannot_read_is_not_counted() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/root", "source", false);
        let object = insert_object(&conn, "abc123", false);
        let unreadable = insert_source(&conn, root, "a.jpg", Some(object));
        let readable = insert_source(&conn, root, "b.jpg", None);
        let own_unreadable = insert_source(&conn, root, "c.jpg", None);
        insert_fact(&conn, unreadable, "content.T", "own");
        insert_fact(&conn, readable, "content.T", "own");
        // A second in year 10000, which Canon cannot read: on the content, and on a source.
        conn.execute(
            "INSERT INTO facts (entity_type, entity_id, key, value_time, time_form, time_precision,
                                observed_at, observed_basis_rev)
             VALUES ('object', ?1, 'content.T', 253402315200, 'clock', 'second', 0, NULL),
                    ('source', ?2, 'content.U', 253402315200, 'clock', 'second', 0, 0)",
            [object, own_unreadable],
        )
        .unwrap();
        let ids = [unreadable, readable, own_unreadable];
        assert_eq!(
            count_values_by_key(&mut conn, &ids).unwrap(),
            [("content.T".to_string(), 1), ("content.U".to_string(), 0)]
        );
        // The value a bare `content.T?` reads, through the filter's own cache.
        let cache =
            crate::expr::repo::prefetch_facts(&mut conn, &ids, &["content.T".into()]).unwrap();
        let asked = ids
            .iter()
            .filter(|&&id| cache.value(id, "content.T").is_some())
            .count();
        assert_eq!(asked, 1);
    }
}
