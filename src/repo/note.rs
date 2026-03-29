//! Note repository — data access for timestamped location annotations.
//!
//! Notes are append-only entries attached to `(root_id, rel_path)` scopes.
//! Multiple notes can exist per scope. Clearing is explicit.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;

use super::db::Connection;
use crate::domain::note::Note;

/// The columns we SELECT for Note construction.
const NOTE_COLUMNS: &str = "id, root_id, rel_path, text, created_at";

/// Construct a Note from a row. Column order must match NOTE_COLUMNS.
fn note_from_row(row: &rusqlite::Row) -> rusqlite::Result<Note> {
    Ok(Note {
        id: row.get(0)?,
        root_id: row.get(1)?,
        rel_path: row.get(2)?,
        text: row.get(3)?,
        created_at: row.get(4)?,
    })
}

/// Insert a note and return the created Note.
pub fn insert(conn: &Connection, root_id: i64, rel_path: &str, text: &str) -> Result<Note> {
    let created_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs() as i64;

    conn.execute(
        "INSERT INTO notes (root_id, rel_path, text, created_at) VALUES (?, ?, ?, ?)",
        rusqlite::params![root_id, rel_path, text, created_at],
    )?;

    let id = conn.last_insert_rowid();
    let sql = format!("SELECT {NOTE_COLUMNS} FROM notes WHERE id = ?");
    let note = conn.query_row(&sql, [id], note_from_row)?;
    Ok(note)
}

/// Fetch notes at an exact scope, ordered chronologically (ASC).
pub fn fetch_by_scope(conn: &Connection, root_id: i64, rel_path: &str) -> Result<Vec<Note>> {
    let sql = format!(
        "SELECT {NOTE_COLUMNS} FROM notes WHERE root_id = ? AND rel_path = ? ORDER BY created_at ASC"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(rusqlite::params![root_id, rel_path], note_from_row)?;

    let mut notes = Vec::new();
    for row in rows {
        notes.push(row?);
    }
    Ok(notes)
}

/// Fetch notes for scope + descendants, ordered by recency (DESC).
pub fn fetch_subtree(conn: &Connection, root_id: i64, rel_path: &str) -> Result<Vec<Note>> {
    let sql = if rel_path.is_empty() {
        format!("SELECT {NOTE_COLUMNS} FROM notes WHERE root_id = ? ORDER BY created_at DESC")
    } else {
        format!(
            "SELECT {NOTE_COLUMNS} FROM notes WHERE root_id = ? AND (rel_path = ? OR rel_path LIKE ? || '/%') ORDER BY created_at DESC"
        )
    };

    let mut stmt = conn.prepare(&sql)?;
    let rows = if rel_path.is_empty() {
        stmt.query_map(rusqlite::params![root_id], note_from_row)?
    } else {
        stmt.query_map(
            rusqlite::params![root_id, rel_path, rel_path],
            note_from_row,
        )?
    };

    let mut notes = Vec::new();
    for row in rows {
        notes.push(row?);
    }
    Ok(notes)
}

/// Fetch notes for scope + descendants, ordered by path then chronologically (ASC).
/// Used by listing modes where the user reads top-to-bottom.
pub fn fetch_subtree_chronological(
    conn: &Connection,
    root_id: i64,
    rel_path: &str,
) -> Result<Vec<Note>> {
    let sql = if rel_path.is_empty() {
        format!(
            "SELECT {NOTE_COLUMNS} FROM notes WHERE root_id = ? ORDER BY rel_path, created_at"
        )
    } else {
        format!(
            "SELECT {NOTE_COLUMNS} FROM notes WHERE root_id = ? AND (rel_path = ? OR rel_path LIKE ? || '/%') ORDER BY rel_path, created_at"
        )
    };

    let mut stmt = conn.prepare(&sql)?;
    let rows = if rel_path.is_empty() {
        stmt.query_map(rusqlite::params![root_id], note_from_row)?
    } else {
        stmt.query_map(
            rusqlite::params![root_id, rel_path, rel_path],
            note_from_row,
        )?
    };

    let mut notes = Vec::new();
    for row in rows {
        notes.push(row?);
    }
    Ok(notes)
}

/// Fetch all notes across all roots, ordered by root_id, rel_path, created_at.
pub fn fetch_all(conn: &Connection) -> Result<Vec<Note>> {
    let sql =
        format!("SELECT {NOTE_COLUMNS} FROM notes ORDER BY root_id, rel_path, created_at");
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], note_from_row)?;

    let mut notes = Vec::new();
    for row in rows {
        notes.push(row?);
    }
    Ok(notes)
}

/// Count notes on the given ancestor paths within a root.
/// Empty input returns 0 without querying.
pub fn count_ancestor_notes(
    conn: &Connection,
    root_id: i64,
    ancestor_paths: &[String],
) -> Result<usize> {
    if ancestor_paths.is_empty() {
        return Ok(0);
    }

    let placeholders: Vec<&str> = ancestor_paths.iter().map(|_| "?").collect();
    let sql = format!(
        "SELECT COUNT(*) FROM notes WHERE root_id = ? AND rel_path IN ({})",
        placeholders.join(",")
    );

    let mut params: Vec<rusqlite::types::Value> =
        vec![rusqlite::types::Value::from(root_id)];
    for path in ancestor_paths {
        params.push(rusqlite::types::Value::from(path.clone()));
    }

    let count: i64 = conn.query_row(&sql, rusqlite::params_from_iter(params), |row| {
        row.get(0)
    })?;
    Ok(count as usize)
}

/// Count distinct rel_paths with notes that are strict descendants of the scope.
pub fn count_descendant_locations(
    conn: &Connection,
    root_id: i64,
    rel_path: &str,
) -> Result<usize> {
    let count: i64 = if rel_path.is_empty() {
        conn.query_row(
            "SELECT COUNT(DISTINCT rel_path) FROM notes WHERE root_id = ? AND rel_path != ''",
            rusqlite::params![root_id],
            |row| row.get(0),
        )?
    } else {
        conn.query_row(
            "SELECT COUNT(DISTINCT rel_path) FROM notes WHERE root_id = ? AND rel_path LIKE ? || '/%'",
            rusqlite::params![root_id, rel_path],
            |row| row.get(0),
        )?
    };
    Ok(count as usize)
}

/// Count total notes for scope + descendants.
pub fn count_subtree_notes(conn: &Connection, root_id: i64, rel_path: &str) -> Result<usize> {
    let count: i64 = if rel_path.is_empty() {
        conn.query_row(
            "SELECT COUNT(*) FROM notes WHERE root_id = ?",
            rusqlite::params![root_id],
            |row| row.get(0),
        )?
    } else {
        conn.query_row(
            "SELECT COUNT(*) FROM notes WHERE root_id = ? AND (rel_path = ? OR rel_path LIKE ? || '/%')",
            rusqlite::params![root_id, rel_path, rel_path],
            |row| row.get(0),
        )?
    };
    Ok(count as usize)
}

/// Count distinct rel_paths with notes for scope + descendants.
pub fn count_subtree_locations(
    conn: &Connection,
    root_id: i64,
    rel_path: &str,
) -> Result<usize> {
    let count: i64 = if rel_path.is_empty() {
        conn.query_row(
            "SELECT COUNT(DISTINCT rel_path) FROM notes WHERE root_id = ?",
            rusqlite::params![root_id],
            |row| row.get(0),
        )?
    } else {
        conn.query_row(
            "SELECT COUNT(DISTINCT rel_path) FROM notes WHERE root_id = ? AND (rel_path = ? OR rel_path LIKE ? || '/%')",
            rusqlite::params![root_id, rel_path, rel_path],
            |row| row.get(0),
        )?
    };
    Ok(count as usize)
}

/// Delete notes at exact scope. Returns count deleted.
pub fn clear_by_scope(conn: &Connection, root_id: i64, rel_path: &str) -> Result<usize> {
    let deleted = conn.execute(
        "DELETE FROM notes WHERE root_id = ? AND rel_path = ?",
        rusqlite::params![root_id, rel_path],
    )?;
    Ok(deleted)
}

/// Delete notes for scope + descendants. Returns count deleted.
pub fn clear_subtree(conn: &Connection, root_id: i64, rel_path: &str) -> Result<usize> {
    let deleted = if rel_path.is_empty() {
        conn.execute(
            "DELETE FROM notes WHERE root_id = ?",
            rusqlite::params![root_id],
        )?
    } else {
        conn.execute(
            "DELETE FROM notes WHERE root_id = ? AND (rel_path = ? OR rel_path LIKE ? || '/%')",
            rusqlite::params![root_id, rel_path, rel_path],
        )?
    };
    Ok(deleted)
}

/// Delete all notes for a root. For use in roots rm.
pub fn delete_by_root(conn: &Connection, root_id: i64) -> Result<usize> {
    let deleted = conn.execute(
        "DELETE FROM notes WHERE root_id = ?",
        rusqlite::params![root_id],
    )?;
    Ok(deleted)
}

/// Count notes under each scope. Returns only non-zero counts.
pub fn batch_count_subtree(
    conn: &Connection,
    scopes: &[(i64, String)],
) -> Result<HashMap<(i64, String), usize>> {
    let mut result = HashMap::new();

    for (root_id, rel_path) in scopes {
        let count = count_subtree_notes(conn, *root_id, rel_path)?;
        if count > 0 {
            result.insert((*root_id, rel_path.clone()), count);
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::test_helpers::{insert_note, insert_root, setup_test_db};

    // =========================================================================
    // insert tests
    // =========================================================================

    #[test]
    fn insert_creates_note_with_timestamp() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);

        let note = insert(&conn, root_id, "vacation", "check this later").unwrap();

        assert!(note.id > 0);
        assert_eq!(note.root_id, root_id);
        assert_eq!(note.rel_path, "vacation");
        assert_eq!(note.text, "check this later");
        assert!(note.created_at > 0);
    }

    // =========================================================================
    // fetch_by_scope tests
    // =========================================================================

    #[test]
    fn fetch_by_scope_returns_exact_match() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);

        insert_note(&conn, root_id, "a/b", "at scope", 100);
        insert_note(&conn, root_id, "a/b/c", "descendant", 200);
        insert_note(&conn, root_id, "a", "ancestor", 300);

        let notes = fetch_by_scope(&conn, root_id, "a/b").unwrap();
        assert_eq!(notes.len(), 1);
        assert_eq!(notes[0].text, "at scope");
    }

    #[test]
    fn fetch_by_scope_chronological_order() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);

        insert_note(&conn, root_id, "a", "second", 200);
        insert_note(&conn, root_id, "a", "first", 100);
        insert_note(&conn, root_id, "a", "third", 300);

        let notes = fetch_by_scope(&conn, root_id, "a").unwrap();
        assert_eq!(notes.len(), 3);
        assert_eq!(notes[0].text, "first");
        assert_eq!(notes[1].text, "second");
        assert_eq!(notes[2].text, "third");
    }

    #[test]
    fn fetch_by_scope_empty() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);

        let notes = fetch_by_scope(&conn, root_id, "nonexistent").unwrap();
        assert!(notes.is_empty());
    }

    // =========================================================================
    // fetch_subtree tests
    // =========================================================================

    #[test]
    fn fetch_subtree_includes_scope_and_descendants() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);

        insert_note(&conn, root_id, "a", "at scope", 100);
        insert_note(&conn, root_id, "a/b", "child", 200);
        insert_note(&conn, root_id, "a/b/c", "grandchild", 300);
        insert_note(&conn, root_id, "other", "unrelated", 400);

        let notes = fetch_subtree(&conn, root_id, "a").unwrap();
        assert_eq!(notes.len(), 3);
        // All should be from scope "a" or descendants
        for note in &notes {
            assert!(
                note.rel_path == "a" || note.rel_path.starts_with("a/"),
                "unexpected rel_path: {}",
                note.rel_path
            );
        }
    }

    #[test]
    fn fetch_subtree_root_scope_returns_all() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);

        insert_note(&conn, root_id, "", "root note", 100);
        insert_note(&conn, root_id, "a", "child", 200);
        insert_note(&conn, root_id, "a/b/c", "deep", 300);

        let notes = fetch_subtree(&conn, root_id, "").unwrap();
        assert_eq!(notes.len(), 3);
    }

    #[test]
    fn fetch_subtree_ordered_by_recency() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);

        insert_note(&conn, root_id, "a", "oldest", 100);
        insert_note(&conn, root_id, "a/b", "middle", 200);
        insert_note(&conn, root_id, "a/c", "newest", 300);

        let notes = fetch_subtree(&conn, root_id, "a").unwrap();
        assert_eq!(notes.len(), 3);
        assert_eq!(notes[0].text, "newest");
        assert_eq!(notes[1].text, "middle");
        assert_eq!(notes[2].text, "oldest");
    }

    #[test]
    fn fetch_subtree_no_false_prefix_match() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);

        insert_note(&conn, root_id, "a", "scope", 100);
        insert_note(&conn, root_id, "a/child", "descendant", 200);
        insert_note(&conn, root_id, "ab", "false prefix", 300);
        insert_note(&conn, root_id, "ab/child", "also false", 400);

        let notes = fetch_subtree(&conn, root_id, "a").unwrap();
        assert_eq!(notes.len(), 2);
        for note in &notes {
            assert!(
                note.rel_path == "a" || note.rel_path.starts_with("a/"),
                "false prefix match: {}",
                note.rel_path
            );
        }
    }

    // =========================================================================
    // fetch_all tests
    // =========================================================================

    #[test]
    fn fetch_all_across_roots() {
        let conn = setup_test_db();
        let root1 = insert_root(&conn, "/photos", "source", false);
        let root2 = insert_root(&conn, "/archive", "archive", false);

        insert_note(&conn, root1, "a", "root1 note", 100);
        insert_note(&conn, root2, "b", "root2 note", 200);

        let notes = fetch_all(&conn).unwrap();
        assert_eq!(notes.len(), 2);
    }

    #[test]
    fn fetch_all_ordered_by_root_path_then_time() {
        let conn = setup_test_db();
        let root1 = insert_root(&conn, "/photos", "source", false);
        let root2 = insert_root(&conn, "/archive", "archive", false);

        insert_note(&conn, root2, "x", "root2", 100);
        insert_note(&conn, root1, "b", "root1 second", 200);
        insert_note(&conn, root1, "a", "root1 first", 100);

        let notes = fetch_all(&conn).unwrap();
        assert_eq!(notes.len(), 3);
        // Ordered by root_id, then rel_path, then created_at
        assert_eq!(notes[0].root_id, root1);
        assert_eq!(notes[0].rel_path, "a");
        assert_eq!(notes[1].root_id, root1);
        assert_eq!(notes[1].rel_path, "b");
        assert_eq!(notes[2].root_id, root2);
    }

    // =========================================================================
    // count_ancestor_notes tests
    // =========================================================================

    #[test]
    fn count_ancestor_notes_returns_correct_count() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);

        insert_note(&conn, root_id, "", "root note", 100);
        insert_note(&conn, root_id, "a", "parent note", 200);
        insert_note(&conn, root_id, "a", "another parent", 300);
        insert_note(&conn, root_id, "a/b/c", "not ancestor", 400);

        let ancestors = vec!["a".to_string(), "".to_string()];
        let count = count_ancestor_notes(&conn, root_id, &ancestors).unwrap();
        assert_eq!(count, 3); // root note + 2 parent notes
    }

    #[test]
    fn count_ancestor_notes_empty_for_root() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);

        insert_note(&conn, root_id, "", "root note", 100);

        // Root has no ancestors
        let count = count_ancestor_notes(&conn, root_id, &[]).unwrap();
        assert_eq!(count, 0);
    }

    // =========================================================================
    // count_descendant_locations tests
    // =========================================================================

    #[test]
    fn count_descendant_locations_excludes_self() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);

        insert_note(&conn, root_id, "a", "self", 100);
        insert_note(&conn, root_id, "a/b", "child", 200);
        insert_note(&conn, root_id, "a/c", "another child", 300);

        let count = count_descendant_locations(&conn, root_id, "a").unwrap();
        assert_eq!(count, 2); // a/b and a/c, not "a" itself
    }

    #[test]
    fn count_descendant_locations_counts_distinct() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);

        insert_note(&conn, root_id, "a/b", "first", 100);
        insert_note(&conn, root_id, "a/b", "second", 200);
        insert_note(&conn, root_id, "a/c", "other", 300);

        let count = count_descendant_locations(&conn, root_id, "a").unwrap();
        assert_eq!(count, 2); // a/b and a/c (distinct), not 3
    }

    // =========================================================================
    // count_subtree_notes tests
    // =========================================================================

    #[test]
    fn count_subtree_notes_includes_self_and_descendants() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);

        insert_note(&conn, root_id, "a", "self", 100);
        insert_note(&conn, root_id, "a/b", "child", 200);
        insert_note(&conn, root_id, "a/b/c", "grandchild", 300);
        insert_note(&conn, root_id, "other", "unrelated", 400);

        let count = count_subtree_notes(&conn, root_id, "a").unwrap();
        assert_eq!(count, 3);
    }

    // =========================================================================
    // count_subtree_locations tests
    // =========================================================================

    #[test]
    fn count_subtree_locations_includes_self_and_descendants() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);

        insert_note(&conn, root_id, "a", "self", 100);
        insert_note(&conn, root_id, "a", "also self", 150);
        insert_note(&conn, root_id, "a/b", "child", 200);
        insert_note(&conn, root_id, "a/c", "another child", 300);

        let count = count_subtree_locations(&conn, root_id, "a").unwrap();
        assert_eq!(count, 3); // a, a/b, a/c
    }

    // =========================================================================
    // clear_by_scope tests
    // =========================================================================

    #[test]
    fn clear_by_scope_exact_only() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);

        insert_note(&conn, root_id, "a", "scope note", 100);
        insert_note(&conn, root_id, "a/b", "descendant", 200);

        clear_by_scope(&conn, root_id, "a").unwrap();

        let scope_notes = fetch_by_scope(&conn, root_id, "a").unwrap();
        assert!(scope_notes.is_empty());

        let child_notes = fetch_by_scope(&conn, root_id, "a/b").unwrap();
        assert_eq!(child_notes.len(), 1);
    }

    #[test]
    fn clear_by_scope_returns_count() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);

        insert_note(&conn, root_id, "a", "first", 100);
        insert_note(&conn, root_id, "a", "second", 200);

        let count = clear_by_scope(&conn, root_id, "a").unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn clear_by_scope_zero_when_empty() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);

        let count = clear_by_scope(&conn, root_id, "nonexistent").unwrap();
        assert_eq!(count, 0);
    }

    // =========================================================================
    // clear_subtree tests
    // =========================================================================

    #[test]
    fn clear_subtree_includes_descendants() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);

        insert_note(&conn, root_id, "a", "scope", 100);
        insert_note(&conn, root_id, "a/b", "child", 200);
        insert_note(&conn, root_id, "a/b/c", "grandchild", 300);
        insert_note(&conn, root_id, "other", "unrelated", 400);

        let count = clear_subtree(&conn, root_id, "a").unwrap();
        assert_eq!(count, 3);

        // Unrelated note should survive
        let remaining = fetch_all(&conn).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].rel_path, "other");
    }

    #[test]
    fn clear_subtree_no_false_prefix() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);

        insert_note(&conn, root_id, "a", "scope", 100);
        insert_note(&conn, root_id, "ab", "false prefix", 200);
        insert_note(&conn, root_id, "ab/child", "also false", 300);

        let count = clear_subtree(&conn, root_id, "a").unwrap();
        assert_eq!(count, 1); // Only "a", not "ab" or "ab/child"

        let remaining = fetch_all(&conn).unwrap();
        assert_eq!(remaining.len(), 2);
    }

    // =========================================================================
    // delete_by_root tests
    // =========================================================================

    #[test]
    fn delete_by_root_removes_all_notes() {
        let conn = setup_test_db();
        let root1 = insert_root(&conn, "/photos", "source", false);
        let root2 = insert_root(&conn, "/archive", "archive", false);

        insert_note(&conn, root1, "a", "root1 note", 100);
        insert_note(&conn, root1, "b", "root1 note2", 200);
        insert_note(&conn, root2, "c", "root2 note", 300);

        let count = delete_by_root(&conn, root1).unwrap();
        assert_eq!(count, 2);

        let remaining = fetch_all(&conn).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].root_id, root2);
    }

    // =========================================================================
    // batch_count_subtree tests
    // =========================================================================

    #[test]
    fn batch_count_subtree_multiple_scopes() {
        let conn = setup_test_db();
        let root1 = insert_root(&conn, "/photos", "source", false);
        let root2 = insert_root(&conn, "/archive", "archive", false);

        insert_note(&conn, root1, "a", "note1", 100);
        insert_note(&conn, root1, "a/b", "note2", 200);
        insert_note(&conn, root2, "x", "note3", 300);

        let scopes = vec![
            (root1, "a".to_string()),
            (root2, "x".to_string()),
            (root2, "empty".to_string()),
        ];

        let counts = batch_count_subtree(&conn, &scopes).unwrap();

        assert_eq!(counts.get(&(root1, "a".to_string())), Some(&2));
        assert_eq!(counts.get(&(root2, "x".to_string())), Some(&1));
        // Empty scope not in result (zero count omitted)
        assert!(!counts.contains_key(&(root2, "empty".to_string())));
    }
}
