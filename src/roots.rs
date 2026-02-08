use std::fs;
use std::io::{self, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Result};

use crate::domain::{parse_root_spec, parse_root_spec_any, Root};
use crate::repo::{self, Db};

pub fn list(db: &Db, scope: Option<&Path>, suspended_only: bool) -> Result<()> {
    let conn = db.conn();

    // Canonicalize scope path if provided
    let scope_str = match scope {
        Some(p) => Some(
            fs::canonicalize(p)
                .map_err(|e| anyhow::anyhow!("Failed to resolve path '{}': {}", p.display(), e))?
                .to_string_lossy()
                .to_string(),
        ),
        None => None,
    };

    // Fetch all roots using repository layer
    let all_roots = repo::root::fetch_all(conn)?;

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
    println!(
        "{:<4} {:<8} {:>8}  {:<16}  {}",
        "ID", "ROLE", "FILES", "LAST SCAN", "PATH"
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
        println!(
            "{:<4} {:<8} {:>8}  {:<16}  {}",
            root.id, root.role, file_count, scan_ago, path_with_info
        );
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
                format!("{}s ago", secs)
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
        .filter(|s| s.object_id.map(|id| archived_objects.contains(&id)).unwrap_or(false))
        .count() as i64;
    let not_in_archive = source_count - in_archive_count;

    if !yes {
        eprintln!("About to remove {} root: {}", root.role, root.path);
        eprintln!(
            "This will forget {} sources ({} in archive, {} not in archive).",
            source_count, in_archive_count, not_in_archive
        );
        eprintln!("Files on disk will NOT be deleted.");
        eprintln!();
        eprintln!("To see which sources will be forgotten:");
        eprintln!("  canon ls {}", root.path);
        eprintln!();
        eprint!("Proceed? [y/N] ");
        io::stderr().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        if !input.trim().eq_ignore_ascii_case("y") {
            bail!("Aborted");
        }
    }

    // Delete facts, sources, and root via repo function
    let deleted_sources = repo::root::remove(conn, root_id)?;

    println!("Removed root {} and {} sources", root_id, deleted_sources);

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
        Some(c) => println!("Set comment on root {}: {}", root_id, c),
        None => println!("Cleared comment on root {}", root_id),
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
    println!("Suspended root {}: {}", root_id, root.path);
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
    println!("Unsuspended root {}: {}", root_id, root.path);
    Ok(())
}
