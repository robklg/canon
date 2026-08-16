//! Ls operations — duplicate detection.
//!
//! `find_duplicate_groups()` groups sources by content hash and returns groups
//! with 2+ sources.

use anyhow::Result;
use rusqlite::types::Value;

use crate::core::repo::source::BATCH_SIZE;
use crate::core::repo::Connection;

/// A group of sources sharing the same content hash.
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

    for chunk in source_ids.chunks(BATCH_SIZE) {
        let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
        let sql = format!(
            "SELECT s.id, s.object_id, o.hash_value, s.size, r.path, s.rel_path
             FROM sources s
             JOIN roots r ON s.root_id = r.id
             JOIN objects o ON s.object_id = o.id
             WHERE s.id IN ({}) AND s.object_id IS NOT NULL",
            placeholders.join(",")
        );

        let params: Vec<Value> = chunk.iter().map(|&id| Value::from(id)).collect();
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(params), |row| {
            Ok((
                row.get::<_, i64>(0)?,    // source_id
                row.get::<_, i64>(1)?,    // object_id
                row.get::<_, String>(2)?, // hash_value
                row.get::<_, i64>(3)?,    // size
                row.get::<_, String>(4)?, // root_path
                row.get::<_, String>(5)?, // rel_path
            ))
        })?;

        for row in rows {
            let (_source_id, object_id, hash, size, root_path, rel_path) = row?;
            let full_path = if rel_path.is_empty() {
                root_path
            } else {
                format!("{root_path}/{rel_path}")
            };

            object_map
                .entry(object_id)
                .or_insert_with(|| (hash, size, Vec::new()))
                .2
                .push(DuplicateSource {
                    path: full_path,
                    source_id: _source_id,
                });
        }
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
    use crate::ops::test_helpers::{insert_object, insert_root, insert_source, setup_test_db};

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
