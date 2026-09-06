use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;

use crate::ceremony;
use crate::core::domain::config::{LedgerConfig, RecordingMode};
use crate::core::domain::decision::DecisionCommand;
use crate::core::domain::format::format_time_ago;
use crate::core::domain::format_count;
use crate::core::domain::root::DoorVerb;
use crate::core::domain::scope::DecisionScope;
use crate::core::domain::Root;
use crate::core::ops::decision::DecisionParams;
use crate::core::ops::scope::{parse_root_spec, parse_root_spec_any, resolve_path, RootLookup};
use crate::core::repo::{self, Db};
use crate::roots::repo as roots_repo;

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
    let file_counts = roots_repo::root::fetch_file_counts(conn, &root_ids)?;

    // Writes tolerate a closed pipe: `canon roots | head` must exit quietly
    // rather than error. That is why the header ignores its result and the
    // row loop breaks instead of propagating with `?`.
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

pub fn remove(
    db: &Db,
    spec: &str,
    yes: bool,
    command_line: &str,
    config: &LedgerConfig,
    no_receipt: bool,
    reason: Option<&str>,
) -> Result<()> {
    let conn = db.conn();

    // Fetch all roots for spec resolution
    let roots = repo::root::fetch_all(conn)?;

    // Removing a root is an act, and an act at a closed door is refused by
    // name with the way back stated — the door is exactly what protects what
    // is inside from destruction, so the way out is to open it deliberately
    // rather than to be let through by silence. Until now this said
    // "No root for path" about a root that plainly exists.
    let root_id = match parse_root_spec(&roots, spec, None)? {
        RootLookup::Found(id) => id,
        RootLookup::Parked(parked) => {
            eprintln!("{}", parked.door_line(DoorVerb::Refused, &parked.root_path));
            std::process::exit(1);
        }
    };

    let plan = crate::roots::ops::plan_remove(conn, root_id)?;

    if !yes {
        eprintln!("About to remove {} root: {}", plan.role, plan.root_path);
        eprintln!(
            "This will forget {} sources ({} in archive, {} not in archive).",
            plan.source_count, plan.in_archive_count, plan.not_in_archive
        );
        eprintln!("Files on disk will NOT be deleted.");
        eprintln!();
        match &plan.retirement {
            Some(pointer) => {
                eprintln!(
                    "The story of this root is already bound at {}.",
                    pointer.artifact_display
                );
            }
            None => {
                eprintln!("No retirement artifact exists for this root.");
                eprintln!(
                    "Removing it deletes the inventory of {} sources, {} notes, and the \
                     recorded fates — the story will not be reviewable afterward.",
                    format_count(plan.source_count),
                    format_count(plan.note_count as i64)
                );
                eprintln!(
                    "To bind it first: canon roots retire path:{}",
                    plan.root_path
                );
            }
        }
        eprintln!();
        eprintln!("To see which sources will be forgotten:");
        eprintln!("  canon ls {}", plan.root_path);
        eprintln!();
    }

    if !ceremony::confirm(yes)? {
        return Ok(());
    }

    let decision = DecisionParams {
        command: DecisionCommand::RootsRm,
        scope: vec![DecisionScope::new(
            plan.root_id,
            plan.root_path.clone(),
            String::new(),
        )],
        command_line: command_line.to_string(),
        reason: reason
            .map(|r| r.to_string())
            .filter(|r| !r.trim().is_empty()),
        record_enabled: config.recording != RecordingMode::Off,
        receipt_enabled: config.recording == RecordingMode::Full && !no_receipt,
        ledger_config: config.clone(),
    };
    let result = crate::roots::ops::execute_remove(conn, &plan, Some(&decision))?;
    println!("{}", result.summary);

    Ok(())
}

pub fn set_comment(db: &Db, spec: &str, comment: Option<&str>) -> Result<()> {
    let conn = db.conn();

    // Fetch all roots for spec resolution
    let roots = repo::root::fetch_all(conn)?;

    // A comment is the label on the door, not a hand inside it: root-grain
    // metadata, never content standing. It stays permitted while the door is
    // closed — and the false "No root for path" it used to answer with is
    // retired here too.
    let root_id = parse_root_spec_any(&roots, spec)?;

    roots_repo::root::set_comment(conn, root_id, comment)?;

    match comment {
        Some(c) => println!("Set comment on root {root_id}: {c}"),
        None => println!("Cleared comment on root {root_id}"),
    }

    Ok(())
}

pub fn suspend(
    db: &Db,
    spec: &str,
    command_line: &str,
    config: &LedgerConfig,
    no_receipt: bool,
) -> Result<()> {
    let conn = db.conn();

    // Fetch all roots for spec resolution
    let roots = repo::root::fetch_all(conn)?;

    // Use parse_root_spec_any to allow suspending already-suspended roots (no-op)
    let root_id = parse_root_spec_any(&roots, spec)?;

    // The spec parse above already proved the root exists, so this lookup
    // cannot miss; the fallback yields a global scope rather than panicking if
    // that ever stops being true.
    let root_path = roots
        .iter()
        .find(|r| r.id == root_id)
        .map(|r| r.path.clone());
    let decision = DecisionParams {
        command: DecisionCommand::RootsSuspend,
        scope: root_path
            .map(|p| vec![DecisionScope::new(root_id, p, String::new())])
            .unwrap_or_default(),
        command_line: command_line.to_string(),
        reason: None,
        record_enabled: config.recording != RecordingMode::Off,
        receipt_enabled: config.recording == RecordingMode::Full && !no_receipt,
        ledger_config: config.clone(),
    };

    match crate::roots::ops::execute_suspend(conn, root_id, Some(&decision)) {
        Ok(result) => {
            println!("{}", result.summary);
            Ok(())
        }
        // Asking for a state a root is already in is not a failure, so it
        // prints as information. The substring is the contract with
        // execute_suspend's bail message and is asserted on that side too;
        // rewording either end silently turns this info line into an error
        // exit.
        Err(e) if e.to_string().contains("already suspended") => {
            let root = roots.iter().find(|r| r.id == root_id).unwrap();
            println!("Root {} is already suspended: {}", root_id, root.path);
            Ok(())
        }
        Err(e) => Err(e),
    }
}

pub fn unsuspend(
    db: &Db,
    spec: &str,
    command_line: &str,
    config: &LedgerConfig,
    no_receipt: bool,
) -> Result<()> {
    let conn = db.conn();

    // Fetch all roots for spec resolution
    let roots = repo::root::fetch_all(conn)?;

    // Use parse_root_spec_any to find suspended roots
    let root_id = parse_root_spec_any(&roots, spec)?;

    // The spec parse above already proved the root exists, so this lookup
    // cannot miss; the fallback yields a global scope rather than panicking if
    // that ever stops being true.
    let root_path = roots
        .iter()
        .find(|r| r.id == root_id)
        .map(|r| r.path.clone());
    let decision = DecisionParams {
        command: DecisionCommand::RootsUnsuspend,
        scope: root_path
            .map(|p| vec![DecisionScope::new(root_id, p, String::new())])
            .unwrap_or_default(),
        command_line: command_line.to_string(),
        reason: None,
        record_enabled: config.recording != RecordingMode::Off,
        receipt_enabled: config.recording == RecordingMode::Full && !no_receipt,
        ledger_config: config.clone(),
    };

    match crate::roots::ops::execute_unsuspend(conn, root_id, Some(&decision)) {
        Ok(result) => {
            println!("{}", result.summary);
            Ok(())
        }
        // Same contract as suspend above: the substring is shared with
        // execute_unsuspend's bail message, asserted on that side, and is what
        // keeps an already-active root from reading as a failure.
        Err(e) if e.to_string().contains("not suspended") => {
            let root = roots.iter().find(|r| r.id == root_id).unwrap();
            println!("Root {} is not suspended: {}", root_id, root.path);
            Ok(())
        }
        Err(e) => Err(e),
    }
}
