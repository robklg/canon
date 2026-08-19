//! The ls instrument's own SQL.
//!
//! One read: the sources in a selection that carry content identity, with the
//! pieces needed to group them by it. Batched, because a selection can hold
//! more ids than a query may bind at once.

use anyhow::Result;
use rusqlite::types::Value;

use crate::core::repo::source::BATCH_SIZE;
use crate::core::repo::Connection;

/// One source that has an object, as the duplicate scan needs it: the identity
/// to group by, the source's own id and the size to report, and the two halves
/// its path is built from.
///
/// The rows come back in no defined order — the query does not ask for one —
/// and the caller takes the first row it sees for an object as the source of
/// that group's hash and size. The hash is safe that way, being fixed by the
/// object itself. The size is safe only while sources sharing an object really
/// do share a size, which nothing here enforces.
pub(super) struct DuplicateRow {
    pub(super) source_id: i64,
    pub(super) object_id: i64,
    pub(super) hash_value: String,
    pub(super) size: i64,
    pub(super) root_path: String,
    pub(super) rel_path: String,
}

/// Fetch every source in the selection that has content identity.
///
/// Sources without an object are skipped: identity is what the caller groups
/// by, so a source that has none cannot join a group.
pub(super) fn fetch_duplicate_rows(
    conn: &Connection,
    source_ids: &[i64],
) -> Result<Vec<DuplicateRow>> {
    let mut rows = Vec::new();

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
        let chunk_rows = stmt.query_map(rusqlite::params_from_iter(params), |row| {
            Ok(DuplicateRow {
                source_id: row.get(0)?,
                object_id: row.get(1)?,
                hash_value: row.get(2)?,
                size: row.get(3)?,
                root_path: row.get(4)?,
                rel_path: row.get(5)?,
            })
        })?;

        for row in chunk_rows {
            rows.push(row?);
        }
    }

    Ok(rows)
}
