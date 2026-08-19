//! What `canon ls` computes: which sources in a selection are copies of each
//! other.
//!
//! Grouping is by content identity — the object a source resolves to, not the
//! hash string, though an object owns exactly one hash so the two answer the
//! same question. A group of one is not a duplicate and is dropped, leaving
//! only the places where the same content sits more than once. The read
//! itself lives in the repository stratum beside this one; what happens here
//! is the grouping, the ordering, and composing each source's path from the
//! two halves it is stored as.

use anyhow::Result;

use crate::core::repo::Connection;
use crate::ls::repo::fetch_duplicate_rows;

/// A group of sources sharing the same content identity — the object they all
/// resolve to. The hash is carried for display; it is not what grouped them.
pub struct DuplicateGroup {
    pub hash_value: String,
    pub total_size: i64,
    pub sources: Vec<DuplicateSource>,
}

pub struct DuplicateSource {
    pub path: String,
    pub source_id: i64,
}

/// Find groups of sources that share the same object_id.
/// Returns groups sorted by first path, sources sorted within each group.
pub fn find_duplicate_groups(conn: &Connection, source_ids: &[i64]) -> Result<Vec<DuplicateGroup>> {
    if source_ids.is_empty() {
        return Ok(Vec::new());
    }

    // Build a map of object_id -> (hash, size, sources)
    use std::collections::HashMap;
    let mut object_map: HashMap<i64, (String, i64, Vec<DuplicateSource>)> = HashMap::new();

    for row in fetch_duplicate_rows(conn, source_ids)? {
        let full_path = if row.rel_path.is_empty() {
            row.root_path
        } else {
            format!("{}/{}", row.root_path, row.rel_path)
        };

        object_map
            .entry(row.object_id)
            .or_insert_with(|| (row.hash_value, row.size, Vec::new()))
            .2
            .push(DuplicateSource {
                path: full_path,
                source_id: row.source_id,
            });
    }

    // Filter to only groups with 2+ sources
    let mut groups: Vec<DuplicateGroup> = object_map
        .into_values()
        .filter(|(_, _, sources)| sources.len() > 1)
        .map(|(hash, size, mut sources)| {
            sources.sort_by(|a, b| a.path.cmp(&b.path));
            DuplicateGroup {
                hash_value: hash,
                total_size: size,
                sources,
            }
        })
        .collect();

    // Sort groups by first path
    groups.sort_by(|a, b| {
        a.sources
            .first()
            .map(|s| s.path.as_str())
            .cmp(&b.sources.first().map(|s| s.path.as_str()))
    });

    Ok(groups)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::testing::{insert_object, insert_root, insert_source, setup_test_db};

    #[test]
    fn find_duplicates_groups_by_object() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "same_hash", false);
        let id1 = insert_source(&conn, root, "a.jpg", Some(obj));
        let id2 = insert_source(&conn, root, "b.jpg", Some(obj));

        let groups = find_duplicate_groups(&conn, &[id1, id2]).unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].sources.len(), 2);
        assert_eq!(groups[0].hash_value, "same_hash");
    }

    #[test]
    fn find_duplicates_no_duplicates() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj1 = insert_object(&conn, "hash1", false);
        let obj2 = insert_object(&conn, "hash2", false);
        let id1 = insert_source(&conn, root, "a.jpg", Some(obj1));
        let id2 = insert_source(&conn, root, "b.jpg", Some(obj2));

        let groups = find_duplicate_groups(&conn, &[id1, id2]).unwrap();
        assert!(groups.is_empty());
    }

    #[test]
    fn find_duplicates_empty_input() {
        let conn = setup_test_db();
        let groups = find_duplicate_groups(&conn, &[]).unwrap();
        assert!(groups.is_empty());
    }
}
