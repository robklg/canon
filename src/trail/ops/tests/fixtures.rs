//! Fixtures shared across the trail ops test modules — genuinely
//! cross-cutting helpers only; a helper used by a single test file lives
//! there instead.

use std::collections::HashMap;

use crate::core::domain::extraction::{DecisionExtraction, OriginDisposition};
use crate::core::repo;
use crate::core::repo::Connection;
use crate::trail::domain::placement::RowAspect;
use crate::trail::domain::timeline::TimelineEvent;
use crate::trail::ops::compute::{TrailParams, TrailView, DEFAULT_LIMIT};

pub(super) fn insert_decision_at(conn: &Connection, command: &str, created_at: i64) -> i64 {
    insert_decision_full(conn, command, created_at, &format!("canon {command}"))
}

pub(super) fn insert_decision_full(
    conn: &Connection,
    command: &str,
    created_at: i64,
    command_line: &str,
) -> i64 {
    conn.execute(
        "INSERT INTO decisions (command, command_line, status, canon_version, created_at)
         VALUES (?1, ?2, 'completed', 'test', ?3)",
        rusqlite::params![command, command_line, created_at],
    )
    .unwrap();
    conn.last_insert_rowid()
}

/// A decision that attempted work and completed none of it — the shape an
/// apply leaves when every transfer failed.
pub(super) fn insert_zero_transfer_decision(
    conn: &Connection,
    command: &str,
    created_at: i64,
    attempted: i64,
) -> i64 {
    conn.execute(
        "INSERT INTO decisions
           (command, command_line, status, canon_version, created_at,
            count_attempted, count_completed, count_failed)
         VALUES (?1, ?2, 'partial', 'test', ?3, ?4, 0, ?4)",
        rusqlite::params![
            command,
            // The command line must match the command: these rows are read by
            // tests whose whole subject is which command a row belongs to.
            format!("canon {}", command.replace('_', " ")),
            created_at,
            attempted
        ],
    )
    .unwrap();
    conn.last_insert_rowid()
}

/// A decision that refused: a pre-flight check said no and the run bailed
/// before touching content. It reaches its last act, so it carries a terminal
/// status — and no counts at all, because it attempted nothing.
pub(super) fn insert_refused_decision(
    conn: &Connection,
    command: &str,
    created_at: i64,
    command_line: &str,
) -> i64 {
    conn.execute(
        "INSERT INTO decisions (command, command_line, status, canon_version, created_at)
         VALUES (?1, ?2, 'refused', 'test', ?3)",
        rusqlite::params![command, command_line, created_at],
    )
    .unwrap();
    conn.last_insert_rowid()
}

pub(super) fn insert_note_at(conn: &Connection, root_id: i64, rel_path: &str, created_at: i64) {
    conn.execute(
        "INSERT INTO notes (root_id, rel_path, text, created_at) VALUES (?1, ?2, 'thought', ?3)",
        rusqlite::params![root_id, rel_path, created_at],
    )
    .unwrap();
}

pub(super) fn scope(conn: &Connection, decision_id: i64, root_id: i64, rel_prefix: &str) {
    // Mirror production: scope rows snapshot the root's path at write time.
    // A root the test never inserted gets a synthetic path, like a legacy row.
    let root_path: String = conn
        .query_row("SELECT path FROM roots WHERE id = ?", [root_id], |r| {
            r.get(0)
        })
        .unwrap_or_else(|_| format!("/removed/{root_id}"));
    repo::decision::insert_scopes(
        conn,
        decision_id,
        &[(root_id, root_path, rel_prefix.to_string())],
    )
    .unwrap();
}

/// Set a decision's `scope` display column — the durable JSON list every
/// surface renders, and what `+N` is counted from. Separate from
/// [`scope`], which writes the index rows: production writes both, and a
/// test that needs only one must be able to say so (a scope row whose
/// display entry was never backfilled is a real shape).
pub(super) fn scope_display(conn: &Connection, decision_id: i64, paths: &[&str]) {
    conn.execute(
        "UPDATE decisions SET scope = ?2 WHERE id = ?1",
        rusqlite::params![decision_id, serde_json::to_string(paths).unwrap()],
    )
    .unwrap();
}

pub(super) fn params(prefixes: Vec<String>) -> TrailParams {
    TrailParams {
        prefixes,
        timeframe: None,
        include_notes: true,
        limit: Some(DEFAULT_LIMIT),
    }
}

pub(super) fn decision_ids(view: &TrailView) -> Vec<i64> {
    match view {
        TrailView::Recent(events) => events
            .iter()
            .filter_map(|e| match e {
                TimelineEvent::Decision(d) => Some(d.id),
                TimelineEvent::Note(_) => None,
            })
            .collect(),
        TrailView::Days(days) => days
            .iter()
            .flat_map(|g| &g.events)
            .filter_map(|e| match e {
                TimelineEvent::Decision(d) => Some(d.id),
                TimelineEvent::Note(_) => None,
            })
            .collect(),
    }
}

pub(super) fn extraction_row(
    decision_id: i64,
    root_id: i64,
    root_path: &str,
    rel_prefix: &str,
    files: i64,
    bytes: Option<i64>,
    destination_path: &str,
) -> DecisionExtraction {
    DecisionExtraction {
        decision_id,
        root_id,
        root_path: root_path.to_string(),
        rel_prefix: rel_prefix.to_string(),
        files,
        bytes,
        destination_root_id: Some(999),
        destination_path: destination_path.to_string(),
        disposition: Some(OriginDisposition::Retained),
    }
}

/// The tagged aspects a classified decision's rows carry, in row order.
pub(super) fn aspects_of(
    placements: &HashMap<i64, Vec<(DecisionExtraction, RowAspect)>>,
    id: i64,
) -> Vec<RowAspect> {
    placements
        .get(&id)
        .map(|rows| rows.iter().map(|(_, aspect)| *aspect).collect())
        .unwrap_or_default()
}
