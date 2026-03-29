//! Note command — annotate locations with timestamped notes.
//!
//! Modes:
//! - Add: `-m "text"` — insert a note at the resolved scope
//! - View: no flags — show exact-scope notes with spatial indicators
//! - List global: `--global` — all notes, pretty-printed grouped by location
//! - List recursive: `-r` — subtree notes, pretty-printed grouped by location
//! - Clear exact: `--clear` — delete notes at exact scope
//! - Clear recursive: `--clear -r` — delete notes in subtree (with confirmation)

use std::path::{Path, PathBuf};

use anyhow::Result;
use chrono::{TimeZone, Utc};

use crate::ceremony;
use crate::domain::path::warn_nonexistent_scope_paths;
use crate::ops;
use crate::ops::note::{NoteListResult, NoteScope, NoteViewResult};
use crate::ops::scope::resolve_scope;
use crate::repo::{self, Db};

pub fn run(
    db: &Db,
    path: Option<&Path>,
    message: Option<&str>,
    recursive: bool,
    global: bool,
    clear: bool,
    yes: bool,
) -> Result<()> {
    let conn = db.conn();

    if let Some(text) = message {
        if clear {
            anyhow::bail!("Cannot use -m and --clear together");
        }
        // Add mode
        let scope = resolve_single_scope(conn, path, false)?;
        repo::note::insert(conn, scope.root_id, &scope.rel_path, text)?;
        eprintln!("Note added: {}", scope.display());
        return Ok(());
    }

    if clear {
        let scope = resolve_single_scope(conn, path, false)?;
        if recursive {
            // Clear recursive with confirmation
            let plan = ops::note::plan_clear_recursive(conn, &scope)?;
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
            let deleted = ops::note::execute_clear_recursive(conn, &scope)?;
            eprintln!("Cleared {deleted} notes");
        } else {
            // Clear exact scope
            let deleted = repo::note::clear_by_scope(conn, scope.root_id, &scope.rel_path)?;
            if deleted == 0 {
                eprintln!("No notes at {}", scope.display());
            } else {
                eprintln!("Cleared {deleted} notes at {}", scope.display());
            }
        }
        return Ok(());
    }

    if global {
        // Global list
        let result = ops::note::list_notes_global(conn)?;
        print_list(&result, true);
        return Ok(());
    }

    if recursive {
        // Recursive list
        let scope = resolve_single_scope(conn, path, false)?;
        let result = ops::note::list_notes_recursive(conn, &scope)?;
        print_list(&result, false);
        return Ok(());
    }

    // View mode — try to resolve scope; fall back to global list if not in a root
    match resolve_single_scope_optional(conn, path)? {
        Some(scope) => {
            let result = ops::note::view_notes(conn, &scope)?;
            print_view(&result);
        }
        None => {
            // CWD not in any root, fall back to global list
            let result = ops::note::list_notes_global(conn)?;
            print_list(&result, true);
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
    let resolved = resolve_scope(&paths, global, &all_roots)?;

    if resolved.is_global() {
        anyhow::bail!(
            "Not inside a known root. Specify a path or cd into a scanned directory."
        );
    }

    // Warn on non-existent scope paths (consistent with other commands)
    warn_nonexistent_scope_paths(&resolved.prefixes, &all_roots);

    // Note operates on a single scope
    if resolved.prefixes.len() != 1 {
        anyhow::bail!("Note operates on a single scope, got multiple paths");
    }

    ops::note::resolve_note_scope(&resolved.prefixes[0], &all_roots)
}

/// Like resolve_single_scope but returns None when CWD is not under a root
/// (instead of erroring). Used by view mode for global fallback.
fn resolve_single_scope_optional(
    conn: &repo::Connection,
    path: Option<&Path>,
) -> Result<Option<NoteScope>> {
    let all_roots = repo::root::fetch_all(conn)?;
    let paths: Vec<PathBuf> = path.iter().map(|p| p.to_path_buf()).collect();
    let resolved = resolve_scope(&paths, false, &all_roots)?;

    if resolved.is_global() {
        return Ok(None);
    }

    warn_nonexistent_scope_paths(&resolved.prefixes, &all_roots);

    if resolved.prefixes.len() != 1 {
        anyhow::bail!("Note operates on a single scope, got multiple paths");
    }

    let scope = ops::note::resolve_note_scope(&resolved.prefixes[0], &all_roots)?;
    Ok(Some(scope))
}

pub(crate) fn format_note_date(timestamp: i64) -> String {
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
            "1 noted location below".to_string()
        } else {
            format!(
                "{} noted locations below",
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

/// Pretty-printed grouped listing for --global and -r modes.
fn print_list(result: &NoteListResult, use_full_path: bool) {
    use std::io::Write;
    let stdout = std::io::stdout();
    let mut handle = stdout.lock();

    let mut current_path: Option<String> = None;

    for note in &result.notes {
        let display_path = if use_full_path {
            match result.roots.get(&note.root_id) {
                Some(root) => {
                    if note.rel_path.is_empty() {
                        root.path.clone()
                    } else {
                        format!("{}/{}", root.path, note.rel_path)
                    }
                }
                None => note.rel_path.clone(),
            }
        } else {
            if note.rel_path.is_empty() {
                "(root)".to_string()
            } else {
                note.rel_path.clone()
            }
        };

        // Group by location — print header when location changes
        let new_group = current_path.as_ref() != Some(&display_path);
        if new_group {
            if current_path.is_some() {
                // Blank line between groups
                if writeln!(handle).is_err() {
                    break;
                }
            }
            if writeln!(handle, "{}:", display_path).is_err() {
                break;
            }
            current_path = Some(display_path);
        }

        if writeln!(
            handle,
            "  {}  {}",
            format_note_date(note.created_at),
            note.text
        )
        .is_err()
        {
            break;
        }
    }
}
