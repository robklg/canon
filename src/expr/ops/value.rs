//! A stored key's value for a batch of sources, read out of the database.
//!
//! A source can hold a key twice — a fact on the source and one on its content —
//! and the rule that picks between them (`domain/value.rs`'s `source_value`)
//! is the facility's. Readers outside get the picked value, never the two
//! rows, so none of them can pick differently.

use std::collections::HashMap;

use anyhow::Result;
use rusqlite::Connection;

use crate::core::domain::fact::FactEntry;
use crate::expr::domain::value::source_value;
use crate::expr::repo::fetch_key_rows;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::domain::fact::FactValue;
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
}
