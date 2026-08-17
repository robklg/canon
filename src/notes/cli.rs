//! Note command — annotate locations with timestamped notes.
//!
//! Modes:
//! - Add: `-m "text"` — insert a note at the resolved scope
//! - View: no flags — show exact-scope notes with spatial indicators
//! - List temporal: `--global` or `-r` — recent notes by date, capped
//! - List spatial: `--global --by-scope` or `-r --by-scope` — one line per location
//! - Clear exact: `--clear` — delete notes at exact scope
//! - Clear recursive: `--clear -r` — delete notes in subtree (with confirmation)

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use anyhow::Result;
use chrono::{TimeZone, Utc};

use crate::ceremony;
use crate::core::domain::config::{LedgerConfig, RecordingMode};
use crate::core::domain::decision::DecisionCommand;
use crate::core::domain::root::Root;
use crate::core::domain::scope::DecisionScope;
use crate::core::ops::decision::DecisionParams;
use crate::core::ops::scope::resolve_scope;
use crate::core::repo::{self, Db};
use crate::notes::domain::{note_display_path, LocationEntry};
use crate::notes::ops as notes_ops;
use crate::notes::ops::{NoteListResult, NoteScope, NoteSpatialResult, NoteViewResult};
use crate::notes::repo as notes_repo;

#[allow(clippy::too_many_arguments)]
pub fn run(
    db: &mut Db,
    path: Option<&Path>,
    message: Option<&str>,
    recursive: bool,
    global: bool,
    clear: bool,
    yes: bool,
    by_scope: bool,
    limit: Option<usize>,
    command_line: &str,
    config: &LedgerConfig,
    no_receipt: bool,
) -> Result<()> {
    let conn = db.conn_mut();

    // --by-scope without --global or -r implies -r
    let recursive = recursive || (by_scope && !global);

    if let Some(text) = message {
        if clear {
            anyhow::bail!("Cannot use -m and --clear together");
        }
        // Add mode
        let scope = resolve_single_scope(conn, path, false)?;
        notes_repo::insert(conn, scope.root_id, &scope.rel_path, text)?;
        eprintln!("Note added: {}", scope.display());
        return Ok(());
    }

    if clear {
        let scope = resolve_single_scope(conn, path, false)?;
        if recursive {
            // Clear recursive with confirmation
            let plan = notes_ops::plan_clear_recursive(conn, &scope)?;
            if plan.note_count == 0 {
                eprintln!("No notes to clear under {}", plan.scope.display());
                return Ok(());
            }
            if !yes {
                eprintln!(
                    "Clear {} notes across {} locations under {}?",
                    plan.note_count,
                    plan.location_count,
                    plan.scope.display(),
                );
            }
            if !ceremony::confirm(yes)? {
                return Ok(());
            }
            let decision = DecisionParams {
                command: DecisionCommand::NoteClear,
                scope: vec![DecisionScope::new(
                    scope.root_id,
                    scope.root_path.clone(),
                    scope.rel_path.clone(),
                )],
                command_line: command_line.to_string(),
                reason: None,
                record_enabled: config.recording != RecordingMode::Off,
                receipt_enabled: config.recording == RecordingMode::Full && !no_receipt,
                ledger_config: config.clone(),
            };
            let result = notes_ops::execute_clear_recursive(conn, &scope, Some(&decision))?;
            eprintln!("{}", result.summary);
            for w in &result.warnings {
                eprintln!("Warning: {w}");
            }
        } else {
            // Clear exact scope
            let decision = DecisionParams {
                command: DecisionCommand::NoteClear,
                scope: vec![DecisionScope::new(
                    scope.root_id,
                    scope.root_path.clone(),
                    scope.rel_path.clone(),
                )],
                command_line: command_line.to_string(),
                reason: None,
                record_enabled: config.recording != RecordingMode::Off,
                receipt_enabled: config.recording == RecordingMode::Full && !no_receipt,
                ledger_config: config.clone(),
            };
            let result = notes_ops::execute_clear_exact(conn, &scope, Some(&decision))?;
            eprintln!("{}", result.summary);
            for w in &result.warnings {
                eprintln!("Warning: {w}");
            }
        }
        return Ok(());
    }

    if global {
        if by_scope {
            let result = notes_ops::list_locations_global(conn, limit)?;
            print_spatial(&result, true);
        } else {
            let result = notes_ops::list_notes_global(conn, limit)?;
            print_temporal(&result, true);
        }
        return Ok(());
    }

    if recursive {
        let scope = resolve_single_scope(conn, path, false)?;
        if by_scope {
            let result = notes_ops::list_locations_recursive(conn, &scope, limit)?;
            print_spatial(&result, false);
        } else {
            let result = notes_ops::list_notes_recursive(conn, &scope, limit)?;
            print_temporal(&result, false);
        }
        return Ok(());
    }

    // View mode — try to resolve scope; fall back to global list if not in a root
    match resolve_single_scope_optional(conn, path)? {
        Some(scope) => {
            let result = notes_ops::view_notes(conn, &scope)?;
            print_view(&result);
        }
        None => {
            // CWD not in any root, fall back to global temporal list
            let result = notes_ops::list_notes_global(conn, limit)?;
            print_temporal(&result, true);
        }
    }

    Ok(())
}

/// Resolve a single scope using the shared resolve_scope() infrastructure.
/// Validates path is under a known root and warns on non-existent paths.
fn resolve_single_scope(
    conn: &repo::Connection,
    path: Option<&Path>,
    global: bool,
) -> Result<NoteScope> {
    let all_roots = repo::root::fetch_all(conn)?;
    let paths: Vec<PathBuf> = path.iter().map(|p| p.to_path_buf()).collect();
    let resolved = resolve_scope(conn, &paths, global, &all_roots)?;

    if resolved.is_global() {
        anyhow::bail!("Not inside a known root. Specify a path or cd into a scanned directory.");
    }

    // Note operates on a single scope
    if resolved.prefixes.len() != 1 {
        anyhow::bail!("Note operates on a single scope, got multiple paths");
    }

    notes_ops::resolve_note_scope(&resolved.prefixes[0], &all_roots)
}

/// Like resolve_single_scope but returns None when CWD is not under a root
/// (instead of erroring). Used by view mode for global fallback.
fn resolve_single_scope_optional(
    conn: &repo::Connection,
    path: Option<&Path>,
) -> Result<Option<NoteScope>> {
    let all_roots = repo::root::fetch_all(conn)?;
    let paths: Vec<PathBuf> = path.iter().map(|p| p.to_path_buf()).collect();
    let resolved = resolve_scope(conn, &paths, false, &all_roots)?;

    if resolved.is_global() {
        return Ok(None);
    }

    if resolved.prefixes.len() != 1 {
        anyhow::bail!("Note operates on a single scope, got multiple paths");
    }

    let scope = notes_ops::resolve_note_scope(&resolved.prefixes[0], &all_roots)?;
    Ok(Some(scope))
}

pub fn format_note_date(timestamp: i64) -> String {
    match Utc.timestamp_opt(timestamp, 0) {
        chrono::LocalResult::Single(dt) => dt.format("%Y-%m-%d").to_string(),
        _ => "????-??-??".to_string(),
    }
}

fn print_view(result: &NoteViewResult) {
    if result.notes.is_empty()
        && result.ancestor_count == 0
        && result.descendant_location_count == 0
    {
        // Completely empty — silent
        return;
    }

    if !result.notes.is_empty() {
        println!("{}:", result.scope.display());
        for note in &result.notes {
            println!("  {}  {}", format_note_date(note.created_at), note.text);
        }
    }

    // Spatial indicators
    let mut parts = Vec::new();
    if result.ancestor_count > 0 {
        let label = if result.ancestor_count == 1 {
            "1 note on parent scopes".to_string()
        } else {
            format!("{} notes on parent scopes", result.ancestor_count)
        };
        parts.push(label);
    }
    if result.descendant_location_count > 0 {
        let label = if result.descendant_location_count == 1 {
            "1 noted location below (-r to show)".to_string()
        } else {
            format!(
                "{} noted locations below (-r to show)",
                result.descendant_location_count
            )
        };
        parts.push(label);
    }

    if !parts.is_empty() {
        if !result.notes.is_empty() {
            // Blank line between notes and indicators
            println!();
        }
        println!("{}", parts.join(" \u{00b7} "));
    }
}

/// Display path for a location in spatial listing.
fn location_display_path(
    loc: &LocationEntry,
    roots: &std::collections::HashMap<i64, Root>,
    use_full_path: bool,
) -> String {
    if use_full_path {
        match roots.get(&loc.root_id) {
            Some(root) => {
                if loc.rel_path.is_empty() {
                    root.path.clone()
                } else {
                    format!("{}/{}", root.path, loc.rel_path)
                }
            }
            None => loc.rel_path.clone(),
        }
    } else if loc.rel_path.is_empty() {
        "(root)".to_string()
    } else {
        loc.rel_path.clone()
    }
}

/// Temporal listing — notes by date, oldest-first, capped with footer.
fn print_temporal(result: &NoteListResult, use_full_path: bool) {
    use std::io::Write;
    let stdout = std::io::stdout();
    let mut handle = stdout.lock();

    if result.notes.is_empty() {
        return;
    }

    // Compute max path width for alignment
    let max_path_len = result
        .notes
        .iter()
        .map(|n| note_display_path(n, &result.roots, use_full_path).len())
        .max()
        .unwrap_or(0);

    for note in &result.notes {
        let path = note_display_path(note, &result.roots, use_full_path);
        if writeln!(
            handle,
            "{:<width$}  {}  {}",
            path,
            format_note_date(note.created_at),
            note.text,
            width = max_path_len
        )
        .is_err()
        {
            break;
        }
    }

    // Footer on stderr
    let displayed = result.notes.len();
    let remaining_notes = result.total_note_count.saturating_sub(displayed);
    if remaining_notes > 0 {
        let displayed_locations: HashSet<_> = result
            .notes
            .iter()
            .map(|n| (n.root_id, &n.rel_path))
            .collect();
        let remaining_locations = result
            .total_location_count
            .saturating_sub(displayed_locations.len());
        eprintln!("({remaining_notes} more notes, {remaining_locations} more locations)");
    }
}

/// Spatial listing — one line per location, oldest-first by most recent note, capped with footer.
fn print_spatial(result: &NoteSpatialResult, use_full_path: bool) {
    use std::io::Write;
    let stdout = std::io::stdout();
    let mut handle = stdout.lock();

    if result.locations.is_empty() {
        return;
    }

    let max_path_len = result
        .locations
        .iter()
        .map(|l| location_display_path(l, &result.roots, use_full_path).len())
        .max()
        .unwrap_or(0);

    let max_count_len = result
        .locations
        .iter()
        .map(|l| format!("({})", l.note_count).len())
        .max()
        .unwrap_or(0);

    for loc in &result.locations {
        let path = location_display_path(loc, &result.roots, use_full_path);
        let count = format!("({})", loc.note_count);
        if writeln!(
            handle,
            "{:<pwidth$}  {:>cwidth$}  {}  {}",
            path,
            count,
            format_note_date(loc.latest_created_at),
            loc.latest_text,
            pwidth = max_path_len,
            cwidth = max_count_len
        )
        .is_err()
        {
            break;
        }
    }

    // Footer on stderr
    let remaining = result
        .total_location_count
        .saturating_sub(result.locations.len());
    if remaining > 0 {
        eprintln!("({remaining} more locations with notes)");
    }
}
