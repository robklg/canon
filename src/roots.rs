use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;

use crate::ceremony;
use crate::domain::config::{LedgerConfig, RecordingMode};
use crate::domain::decision::DecisionCommand;
use crate::domain::format::format_size;
use crate::domain::format_count;
use crate::domain::path::resolve_path;
use crate::domain::retire::Readiness;
use crate::domain::scope::DecisionScope;
use crate::domain::{parse_root_spec, parse_root_spec_any, Root};
use crate::ops;
use crate::ops::decision::DecisionParams;
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

    // Parse the spec to get root id and validate it exists
    let root_id = parse_root_spec(&roots, spec, None)?;

    let plan = ops::roots::plan_remove(conn, root_id)?;

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
    let result = ops::roots::execute_remove(conn, &plan, Some(&decision))?;
    println!("{}", result.summary);

    Ok(())
}

/// The retirement readiness review — the opening movement of the ceremony.
/// Review on stdout (a ceremony surface, not a list command); `--dry-run`
/// always exits 0 (it is a report); a NOT READY verdict without
/// `--allow unresolved` exits non-zero after the review (compare precedent:
/// the verdict is the message, no `Error:` duplication).
pub fn retire(
    db: &Db,
    spec: &str,
    dry_run: bool,
    allow_unresolved: bool,
    config: &LedgerConfig,
) -> Result<()> {
    let conn = db.conn();
    let roots = repo::root::fetch_all(conn)?;
    // `_any`: a suspended root retires on faith — surfaced, never refused.
    let root_id = parse_root_spec_any(&roots, spec)?;
    ops::retire::validate_retire_target(&roots, root_id, config)?;

    let review = ops::retire::compute_readiness(conn, root_id)?;
    print_review(&review);

    match &review.readiness {
        Readiness::NotReady { unresolved, .. } => {
            println!(
                "NOT READY for retirement — {} sources are neither archived nor excluded.",
                format_count(*unresolved)
            );
            if !allow_unresolved {
                println!(
                    "To retire anyway: canon roots retire path:{} --allow unresolved",
                    review.root.path
                );
            }
        }
        Readiness::NoBlockersFound => {
            println!("No blockers found. Whether this story is complete is yours to judge.");
        }
    }

    if dry_run {
        return Ok(());
    }
    if review.readiness.blocks(allow_unresolved) {
        std::process::exit(1);
    }
    if matches!(review.readiness, Readiness::NotReady { .. }) {
        println!("Retiring with unresolved sources acknowledged (--allow unresolved).");
    }
    println!();
    println!("The review is complete. Binding the book is not yet available in this version.");
    Ok(())
}

fn print_review(review: &ops::retire::ReadinessReview) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let account = &review.account;

    println!("Retirement review: {}", review.root.path);
    println!();
    println!("  role         {}", review.root.role);
    if let Some(comment) = &review.root.comment {
        println!("  comment      {comment}");
    }
    println!(
        "  suspended    {}",
        if review.root.is_suspended() {
            "yes"
        } else {
            "no"
        }
    );
    println!(
        "  first scan   {}",
        match review.first_scan {
            Some(ts) => format_date(ts),
            None => "unknown".to_string(),
        }
    );
    println!(
        "  last scan    {}",
        match review.gaps.last_scanned_at {
            Some(ts) => format!("{} ({})", format_date(ts), format_time_ago(Some(ts), now)),
            None => "never".to_string(),
        }
    );
    println!();

    println!("Resolution account");
    println!(
        "  ever indexed here      {}",
        match account.ever_indexed() {
            Some(n) => format!("{} sources", format_count(n)),
            None => format!(
                "not derivable — {} files have unrecorded disposition",
                format_count(account.archived_unrecorded)
            ),
        }
    );
    println!();
    println!("  the story so far");
    let mut archived = format!("{} files", format_count(account.archived_files));
    if let Some(bytes) = account.archived_bytes {
        if account.archived_files > 0 {
            archived.push_str(&format!(", {}", format_size(bytes)));
        }
    }
    let mut split = Vec::new();
    if account.archived_moved > 0 {
        split.push(format!("{} moved", format_count(account.archived_moved)));
    }
    if account.archived_copied > 0 {
        split.push(format!("{} copied", format_count(account.archived_copied)));
    }
    if account.archived_unrecorded > 0 {
        split.push(format!(
            "disposition unrecorded for {}",
            format_count(account.archived_unrecorded)
        ));
    }
    if !split.is_empty() {
        archived.push_str(&format!("   ({})", split.join(", ")));
    }
    println!("    archived from here   {archived}");
    println!(
        "    deleted              {} sources           (scan-observed)",
        format_count(account.deleted)
    );
    println!(
        "    missing, unexplained {} sources",
        format_count(account.unexplained_missing)
    );
    println!();
    println!(
        "  standing here now      {} sources",
        format_count(account.standing())
    );
    println!(
        "    covered              {}   (content verified present in the archive)",
        format_count(account.covered)
    );
    println!(
        "    excluded             {}",
        format_count(account.excluded)
    );
    let mut unresolved = format_count(account.unresolved);
    if account.unhashed_unresolved > 0 {
        unresolved.push_str(&format!(
            "      ({} unhashed — listed by name only)",
            format_count(account.unhashed_unresolved)
        ));
    }
    println!("    unresolved           {unresolved}");
    println!();

    let mut facts = Vec::new();
    if account.unexplained_missing > 0 {
        facts.push(format!(
            "{} sources are missing without a recorded deletion.",
            format_count(account.unexplained_missing)
        ));
    }
    if account.unhashed_unresolved > 0 {
        facts.push(format!(
            "{} present sources were never hashed — they cannot be content-verified.",
            format_count(account.unhashed_unresolved)
        ));
    }
    if !review.gaps.reachable {
        facts.push(
            "The root's path is unreachable — retirement would bind the story as last observed."
                .to_string(),
        );
    }
    if review.gaps.open_cluster_intentions > 0 {
        facts.push(format!(
            "{} cluster-generate decisions on this root have no subsequent apply — possible open intentions.",
            format_count(review.gaps.open_cluster_intentions)
        ));
    }
    if !facts.is_empty() {
        println!("Facts to weigh");
        for fact in facts {
            println!("  {fact}");
        }
        println!();
    }
}

fn format_date(ts: i64) -> String {
    use chrono::{Local, TimeZone};
    match Local.timestamp_opt(ts, 0) {
        chrono::LocalResult::Single(dt) => dt.format("%Y-%m-%d").to_string(),
        _ => format!("@{ts}"),
    }
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

    match ops::roots::execute_suspend(conn, root_id, Some(&decision)) {
        Ok(result) => {
            println!("{}", result.summary);
            Ok(())
        }
        Err(e) if e.to_string().contains("already suspended") => {
            // Match the existing behavior: print info message, not error
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

    match ops::roots::execute_unsuspend(conn, root_id, Some(&decision)) {
        Ok(result) => {
            println!("{}", result.summary);
            Ok(())
        }
        Err(e) if e.to_string().contains("not suspended") => {
            // Match the existing behavior: print info message, not error
            let root = roots.iter().find(|r| r.id == root_id).unwrap();
            println!("Root {} is not suspended: {}", root_id, root.path);
            Ok(())
        }
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    #[test]
    fn retire_argv_parses_through_the_real_cli() {
        // The handoff-law discipline: CLI drift is a test failure.
        for argv in [
            vec!["canon", "roots", "retire", "/mnt/old-drive"],
            vec!["canon", "roots", "retire", "id:3", "--dry-run"],
            vec![
                "canon",
                "roots",
                "retire",
                "/mnt/old-drive",
                "--allow",
                "unresolved",
                "--reason",
                "resolved this summer",
                "--yes",
            ],
        ] {
            crate::Cli::try_parse_from(&argv)
                .unwrap_or_else(|e| panic!("must parse: {argv:?}\n{e}"));
        }
    }

    #[test]
    fn retire_allow_rejects_unknown_values() {
        let argv = ["canon", "roots", "retire", "/r", "--allow", "everything"];
        assert!(crate::Cli::try_parse_from(argv).is_err());
    }
}
