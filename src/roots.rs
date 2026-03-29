use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;

use crate::ceremony;
use crate::domain::path::resolve_path;
use crate::domain::{parse_root_spec, parse_root_spec_any, Root};
use crate::repo::{self, Db};

pub fn list(db: &Db, scope: Option<&Path>, suspended_only: bool) -> Result<()> {
    let conn = db.conn();

    // Fetch all roots using repository layer
    let all_roots = repo::root::fetch_all(conn)?;

    // Resolve scope path if provided (soft resolution: matches known roots, falls back to fs)
    let scope_str = match scope {
        Some(p) => {
            let cwd = std::env::current_dir()?;
            Some(resolve_path(p, &all_roots, &cwd)?)
        }
        None => None,
    };

    // Apply domain predicates for filtering
    let filtered_roots: Vec<&Root> = all_roots
        .iter()
        .filter(|r| {
            // Filter by suspended status
            if suspended_only {
                r.is_suspended()
            } else {
                r.is_active()
            }
        })
        .filter(|r| {
            // Filter by scope if provided
            match &scope_str {
                Some(scope) => r.matches_scope(scope),
                None => true,
            }
        })
        .collect();

    if filtered_roots.is_empty() {
        if scope.is_some() {
            println!("No roots at or beneath this path.");
        } else {
            println!("No roots registered. Use `canon scan --add --role <source|archive> <path>` to add one.");
        }
        return Ok(());
    }

    // Fetch file counts for the filtered roots
    let root_ids: Vec<i64> = filtered_roots.iter().map(|r| r.id).collect();
    let file_counts = repo::root::fetch_file_counts(conn, &root_ids)?;

    // Print header
    {
        use std::io::Write;
        let stdout = std::io::stdout();
        let mut handle = stdout.lock();
        let _ = writeln!(
            handle,
            "{:<4} {:<8} {:>8}  {:<16}  PATH",
            "ID", "ROLE", "FILES", "LAST SCAN"
        );

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        for root in filtered_roots {
            let file_count = file_counts.get(&root.id).copied().unwrap_or(0);
            let scan_ago = format_time_ago(root.last_scanned_at, now);
            let suspended_marker = if root.is_suspended() {
                " [suspended]"
            } else {
                ""
            };
            let path_with_info = match &root.comment {
                Some(c) => format!("{}{} ({})", root.path, suspended_marker, c),
                None => format!("{}{}", root.path, suspended_marker),
            };
            if writeln!(
                handle,
                "{:<4} {:<8} {:>8}  {:<16}  {}",
                root.id, root.role, file_count, scan_ago, path_with_info
            )
            .is_err()
            {
                break;
            }
        }
    }

    Ok(())
}

fn format_time_ago(timestamp: Option<i64>, now: i64) -> String {
    match timestamp {
        None => "never".to_string(),
        Some(ts) => {
            let secs = now - ts;
            if secs < 0 {
                "just now".to_string()
            } else if secs < 60 {
                format!("{secs}s ago")
            } else if secs < 3600 {
                format!("{}m ago", secs / 60)
            } else if secs < 86400 {
                format!("{}h ago", secs / 3600)
            } else {
                format!("{}d ago", secs / 86400)
            }
        }
    }
}

pub fn remove(db: &Db, spec: &str, yes: bool) -> Result<()> {
    let conn = db.conn();

    // Fetch all roots for spec resolution
    let roots = repo::root::fetch_all(conn)?;

    // Parse the spec to get root id and validate it exists
    let root_id = parse_root_spec(&roots, spec, None)?;

    // Get root info from already-fetched roots
    let root = roots.iter().find(|r| r.id == root_id).unwrap();

    // Get sources for this root to compute statistics
    let sources = repo::source::batch_fetch_by_roots(conn, &[root_id])?;
    let source_count = sources.len() as i64;

    // Check which sources have their content in an archive
    let object_ids: Vec<i64> = sources.iter().filter_map(|s| s.object_id).collect();
    let archived_objects = repo::object::batch_check_archived(conn, &object_ids, None)?;
    let in_archive_count = sources
        .iter()
        .filter(|s| {
            s.object_id
                .map(|id| archived_objects.contains(&id))
                .unwrap_or(false)
        })
        .count() as i64;
    let not_in_archive = source_count - in_archive_count;

    if !yes {
        eprintln!("About to remove {} root: {}", root.role, root.path);
        eprintln!(
            "This will forget {source_count} sources ({in_archive_count} in archive, {not_in_archive} not in archive)."
        );
        eprintln!("Files on disk will NOT be deleted.");
        eprintln!();
        eprintln!("To see which sources will be forgotten:");
        eprintln!("  canon ls {}", root.path);
        eprintln!();
    }

    if !ceremony::confirm(yes)? {
        return Ok(());
    }

    // Delete notes for this root before removing the root itself
    repo::note::delete_by_root(conn, root_id)?;

    // Delete facts, sources, and root via repo function
    let deleted_sources = repo::root::remove(conn, root_id)?;

    println!("Removed root {root_id} and {deleted_sources} sources");

    Ok(())
}

pub fn set_comment(db: &Db, spec: &str, comment: Option<&str>) -> Result<()> {
    let conn = db.conn();

    // Fetch all roots for spec resolution
    let roots = repo::root::fetch_all(conn)?;

    // Parse the spec to get root id and validate it exists
    let root_id = parse_root_spec(&roots, spec, None)?;

    repo::root::set_comment(conn, root_id, comment)?;

    match comment {
        Some(c) => println!("Set comment on root {root_id}: {c}"),
        None => println!("Cleared comment on root {root_id}"),
    }

    Ok(())
}

pub fn suspend(db: &Db, spec: &str) -> Result<()> {
    let conn = db.conn();

    // Fetch all roots for spec resolution
    let roots = repo::root::fetch_all(conn)?;

    // Use parse_root_spec_any to allow suspending already-suspended roots (no-op)
    let root_id = parse_root_spec_any(&roots, spec)?;

    // Get root info from already-fetched roots
    let root = roots.iter().find(|r| r.id == root_id).unwrap();

    if root.is_suspended() {
        println!("Root {} is already suspended: {}", root_id, root.path);
        return Ok(());
    }

    repo::root::set_suspended(conn, root_id, true)?;
    let counts = repo::root::fetch_file_counts(conn, &[root_id])?;
    let count = counts.get(&root_id).copied().unwrap_or(0) as usize;
    println!(
        "Suspended root {}: {} ({} sources)",
        root_id,
        root.path,
        ceremony::format_count(count)
    );
    Ok(())
}

pub fn unsuspend(db: &Db, spec: &str) -> Result<()> {
    let conn = db.conn();

    // Fetch all roots for spec resolution
    let roots = repo::root::fetch_all(conn)?;

    // Use parse_root_spec_any to find suspended roots
    let root_id = parse_root_spec_any(&roots, spec)?;

    // Get root info from already-fetched roots
    let root = roots.iter().find(|r| r.id == root_id).unwrap();

    if !root.is_suspended() {
        println!("Root {} is not suspended: {}", root_id, root.path);
        return Ok(());
    }

    repo::root::set_suspended(conn, root_id, false)?;
    let counts = repo::root::fetch_file_counts(conn, &[root_id])?;
    let count = counts.get(&root_id).copied().unwrap_or(0) as usize;
    println!(
        "Unsuspended root {}: {} ({} sources)",
        root_id,
        root.path,
        ceremony::format_count(count)
    );
    Ok(())
}
