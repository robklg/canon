//! Ls operations — mode filtering and duplicate detection.
//!
//! `filter_by_mode()` applies post-selection mode filtering (archived, unarchived,
//! unhashed, excluded) with batch archive status lookups.
//!
//! `find_duplicate_groups()` groups sources by content hash and returns groups
//! with 2+ sources.

use std::collections::HashMap;

use anyhow::Result;
use rusqlite::types::Value;

use crate::domain::source::Source;
use crate::repo::{self, Connection};
use crate::repo::source::BATCH_SIZE;

/// The active ls filter mode (mutually exclusive).
pub enum LsMode {
    /// Show all sources (default).
    All,
    /// Show only sources whose content exists in an archive.
    Archived,
    /// Like Archived, but expand each archive location as a separate entry.
    ArchivedShow,
    /// Show only sources whose content does NOT exist in an archive.
    Unarchived,
    /// Show only sources without an object_id.
    Unhashed,
    /// Show only excluded sources (source-level or object-level).
    Excluded,
}

/// A source entry after mode filtering, with optional archive path annotation.
pub struct LsEntry<'a> {
    pub source: &'a Source,
    /// Present only in ArchivedShow mode (one LsEntry per archive location).
    pub archive_path: Option<String>,
}

/// Result of applying ls mode filtering to a selection.
pub struct LsModeResult<'a> {
    /// Filtered entries (may have duplicated sources in ArchivedShow mode).
    pub entries: Vec<LsEntry<'a>>,
    /// Unhashed sources skipped during archived/unarchived filtering.
    pub unhashed_skipped: usize,
}

/// Apply mode filtering to a set of selected sources.
///
/// Performs batch archive status lookups as needed (only for Archived/ArchivedShow/
/// Unarchived modes). Returns filtered entries and metadata.
pub fn filter_by_mode<'a>(
    conn: &Connection,
    sources: &'a [Source],
    mode: &LsMode,
) -> Result<LsModeResult<'a>> {
    match mode {
        LsMode::All => {
            let entries = sources.iter().map(|s| LsEntry { source: s, archive_path: None }).collect();
            Ok(LsModeResult { entries, unhashed_skipped: 0 })
        }
        LsMode::Unhashed => {
            let entries = sources
                .iter()
                .filter(|s| s.object_id.is_none())
                .map(|s| LsEntry { source: s, archive_path: None })
                .collect();
            Ok(LsModeResult { entries, unhashed_skipped: 0 })
        }
        LsMode::Excluded => {
            let entries = sources
                .iter()
                .filter(|s| s.is_excluded())
                .map(|s| LsEntry { source: s, archive_path: None })
                .collect();
            Ok(LsModeResult { entries, unhashed_skipped: 0 })
        }
        LsMode::Archived | LsMode::ArchivedShow | LsMode::Unarchived => {
            let object_ids: Vec<i64> = sources.iter().filter_map(|s| s.object_id).collect();
            let archived_set = repo::object::batch_check_archived(conn, &object_ids, None)?;

            let archive_paths_map: HashMap<i64, Vec<String>> =
                if matches!(mode, LsMode::ArchivedShow) {
                    repo::object::batch_find_archive_paths(conn, &object_ids)?
                } else {
                    HashMap::new()
                };

            let mut entries = Vec::new();
            let mut unhashed_skipped = 0usize;

            for source in sources {
                match source.object_id {
                    None => {
                        unhashed_skipped += 1;
                    }
                    Some(obj_id) => match mode {
                        LsMode::ArchivedShow => {
                            if let Some(paths) = archive_paths_map.get(&obj_id) {
                                for path in paths {
                                    entries.push(LsEntry {
                                        source,
                                        archive_path: Some(path.clone()),
                                    });
                                }
                            }
                        }
                        LsMode::Archived => {
                            if archived_set.contains(&obj_id) {
                                entries.push(LsEntry { source, archive_path: None });
                            }
                        }
                        LsMode::Unarchived => {
                            if !archived_set.contains(&obj_id) {
                                entries.push(LsEntry { source, archive_path: None });
                            }
                        }
                        _ => unreachable!(),
                    },
                }
            }

            Ok(LsModeResult { entries, unhashed_skipped })
        }
    }
}

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
pub fn find_duplicate_groups(
    conn: &Connection,
    source_ids: &[i64],
) -> Result<Vec<DuplicateGroup>> {
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
    use crate::ops::test_helpers::{
        insert_object, insert_root, insert_source, setup_test_db,
    };

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
