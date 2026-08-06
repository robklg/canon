use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;

use crate::ceremony;
use crate::domain::config::{LedgerConfig, RecordingMode};
use crate::domain::decision::DecisionCommand;
use crate::domain::format::{format_date, format_size, format_time_ago};
use crate::domain::format_count;
use crate::domain::path::resolve_path;
use crate::domain::retire::Readiness;
use crate::domain::scope::DecisionScope;
use crate::domain::story::StoryParams;
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

/// The retirement ceremony: review → confirm → bind → confirm → release.
/// Review and ceremony on stdout (a ceremony surface, not a list command);
/// `--dry-run` always exits 0 (it is a report); a NOT READY verdict without
/// `--allow unresolved` exits non-zero after the review (compare precedent:
/// the verdict is the message, no `Error:` duplication). A world-moved
/// release exits non-zero and asks to be re-run.
#[allow(clippy::too_many_arguments)]
/// `canon roots retired` — the retired fleet, one line per book.
pub fn retired(db: &Db, config: &LedgerConfig) -> Result<()> {
    let listing = ops::retire::compute_shelf_listing(db.conn(), config)?;
    let books = listing
        .lines
        .iter()
        .filter(|l| matches!(l, ops::retire::ShelfLine::Book { .. }))
        .count();

    // The shelf can be observed (books are the primary lines) or not (the
    // index still answers, hedged) — the header states which reading this is.
    let shelf_observed = match (&listing.shelf, listing.shelf_reachable) {
        (None, _) => {
            if listing.lines.is_empty() {
                println!(
                    "No archive root is registered — there is no shelf, and the index records no retirements."
                );
                return Ok(());
            }
            println!("No archive root is registered — there is no shelf. The index records:");
            false
        }
        (Some(shelf), false) => {
            if listing.lines.is_empty() {
                println!(
                    "The shelf at {shelf} is not reachable right now, and the index records no retirements."
                );
                return Ok(());
            }
            println!("The shelf at {shelf} is not reachable right now — listing from the index:");
            false
        }
        (Some(shelf), true) => {
            if listing.lines.is_empty() {
                println!("The shelf is empty — no roots retired yet. ({shelf})");
                return Ok(());
            }
            let word = if books == 1 { "book" } else { "books" };
            println!(
                "The retired fleet: {} {word} on the shelf ({shelf})",
                format_count(books as i64)
            );
            true
        }
    };
    println!();

    for line in &listing.lines {
        match line {
            ops::retire::ShelfLine::Book {
                root_path,
                retired_on,
                entries,
                book_dir,
                reason,
                indexed,
            } => {
                let date = retired_on.as_deref().unwrap_or("(undated)");
                let mut s = format!("{date}  {root_path}");
                if let Some(entries) = entries {
                    s.push_str(&format!(" — {} entries", format_count(*entries)));
                }
                s.push_str(&format!(" → {book_dir}"));
                if let Some(reason) = reason {
                    s.push_str(&format!(" · \"{reason}\""));
                }
                if !indexed {
                    s.push_str(" (not indexed)");
                }
                println!("{s}");
            }
            ops::retire::ShelfLine::RecordedOnly {
                root_path,
                retired_on,
                book_path,
                reason,
            } => {
                // With the shelf in view, absence is a fact; without it,
                // only the recorded location can be claimed.
                let mut s = if shelf_observed {
                    format!(
                        "{retired_on}  {root_path} — recorded, but no book stands at {book_path}"
                    )
                } else {
                    format!("{retired_on}  {root_path} → {book_path}")
                };
                if let Some(reason) = reason {
                    s.push_str(&format!(" · \"{reason}\""));
                }
                println!("{s}");
            }
            ops::retire::ShelfLine::Unidentified { dir_name } => {
                println!("            {dir_name}/ — on the shelf, but not identifiable as a book");
            }
        }
    }
    Ok(())
}

pub fn retire(
    db: &Db,
    spec: &str,
    dry_run: bool,
    allow_unresolved: bool,
    reason: Option<&str>,
    yes: bool,
    command_line: &str,
    config: &LedgerConfig,
) -> Result<()> {
    let conn = db.conn();
    let roots = repo::root::fetch_all(conn)?;
    // `_any`: a suspended root retires on faith — surfaced, never refused.
    let root_id = parse_root_spec_any(&roots, spec)?;
    ops::retire::validate_retire_target(&roots, root_id, config)?;

    let story = ops::retire::fetch_root_story(conn, root_id)?;
    let review = ops::retire::readiness_lens(&story);
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
    println!("{}", story_pointer(&review.root.path));

    if dry_run {
        return Ok(());
    }
    if review.readiness.blocks(allow_unresolved) {
        std::process::exit(1);
    }
    if matches!(review.readiness, Readiness::NotReady { .. }) {
        println!("Retiring with unresolved sources acknowledged (--allow unresolved).");
    }

    // Movement 1: bind the book.
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let plan = ops::retire::plan_bind(&story, config, now)?;

    println!();
    if !yes {
        if plan.replaces_existing {
            println!(
                "A book for this root already stands at {} and will be replaced by this fresh compile.",
                plan.final_dir.display()
            );
        }
        if config.recording == RecordingMode::Off {
            println!(
                "Recording is off — this retirement will not be indexed. The book still \
                 binds; the shelf listing will not show it."
            );
        }
        println!("Bind the book at {}?", plan.final_dir.display());
    }
    if !ceremony::confirm(yes)? {
        return Ok(());
    }

    let mut ceremony_state = ops::retire::begin_ceremony(
        conn,
        story,
        &review,
        plan,
        ops::retire::CeremonyParams {
            reason: reason.map(|r| r.to_string()),
            now,
            command_line: command_line.to_string(),
            config: config.clone(),
        },
    );

    // The story as it will bind — composed over the ceremony's own fetch,
    // offered once to the user's editor (the manifest precedent: composed
    // declaration, human refinement, then binding).
    let telling = match prepare_telling(&ceremony_state, conn, yes) {
        Ok(telling) => telling,
        Err(e) => {
            for warning in ceremony_state.interrupt(conn, &format!("{e:#}")) {
                eprintln!("{warning}");
            }
            return Err(e);
        }
    };

    let bound = match ceremony_state.bind(conn, telling) {
        Ok(bound) => bound,
        Err(e) => {
            for warning in ceremony_state.interrupt(conn, &format!("{e:#}")) {
                eprintln!("{warning}");
            }
            return Err(e);
        }
    };

    println!();
    println!("The book is at {}", bound.dir.display());
    let ledger_line = match bound.ledger_files {
        Some(n) => format!("{} receipts gathered", format_count(n as i64)),
        None => "ledger not gathered".to_string(),
    };
    println!(
        "  {} entries bound; {ledger_line}",
        format_count(bound.entry_count)
    );
    println!("  story.md — the story as told");
    if bound.replaced_previous {
        println!("  the previous book was replaced");
    }
    for gap in &bound.gaps {
        println!("  gap: {gap}");
    }
    for warning in &bound.warnings {
        eprintln!("{warning}");
    }

    // Movement 2: release the root — after the inspection window.
    println!();
    if !yes {
        println!("Remove the root from the index? Aborting keeps both the root and the book.");
    }
    if !ceremony::confirm(yes)? {
        let abandoned = ceremony_state.abandon(conn);
        println!("{}", abandoned.summary);
        for warning in &abandoned.warnings {
            eprintln!("{warning}");
        }
        return Ok(());
    }

    match ceremony_state.release(conn)? {
        ops::retire::ReleaseOutcome::Released {
            summary, warnings, ..
        } => {
            println!("{summary}");
            println!("The drive is yours to discard.");
            for warning in &warnings {
                eprintln!("{warning}");
            }
        }
        ops::retire::ReleaseOutcome::WorldMoved { detail, warnings } => {
            println!("The world has moved since the review: {detail}.");
            println!("The root remains in the index; the book is bound. Re-run the ceremony.");
            for warning in &warnings {
                eprintln!("{warning}");
            }
            std::process::exit(1);
        }
    }

    Ok(())
}

/// Compose the telling and offer it once to the user's editor before it
/// binds. Under `--yes` the composed story binds silently — no prompt, no
/// editor, ever. An editor failure or an empty edit never aborts the
/// ceremony: the choice re-opens until the user edits successfully or
/// binds the composed draft (Ctrl-C remains the escape — an interrupted
/// ceremony is findable).
fn prepare_telling(
    ceremony_state: &ops::retire::RetireCeremony,
    conn: &rusqlite::Connection,
    yes: bool,
) -> Result<ops::telling::TellingArtifact> {
    let draft = ceremony_state.compose_telling(conn)?;
    let composed = ops::telling::finalize_telling(&draft)?;
    let artifact = |text: String| {
        let hand_edited = text != composed;
        ops::telling::TellingArtifact {
            text,
            hand_edited,
            params: StoryParams::default(),
        }
    };
    if yes {
        return Ok(artifact(composed.clone()));
    }

    println!();
    println!("The book will carry the story as composed — title, foreword slot, and all.");
    // Re-offers resume from the person's last edit, never the pristine
    // draft — a refused finalize (emptied page) must not cost the words
    // that were saved.
    let mut current = draft;
    loop {
        if !ceremony::ask("Edit the story before it is written into the book?")? {
            return Ok(artifact(composed.clone()));
        }
        match ceremony::edit_in_editor(&current, "story.md") {
            Ok(None) => {
                eprintln!("No $VISUAL or $EDITOR is set — the story binds as composed.");
                return Ok(artifact(composed.clone()));
            }
            Ok(Some(edited)) => match ops::telling::finalize_telling(&edited) {
                Ok(text) => return Ok(artifact(text)),
                Err(e) => {
                    current = edited;
                    eprintln!("{e:#}");
                    eprintln!("Nothing was bound — edit again, or answer no to bind the story as composed.");
                }
            },
            Err(e) => {
                eprintln!("{e:#}");
                eprintln!(
                    "Nothing was bound — edit again, or answer no to bind the story as composed."
                );
            }
        }
    }
}

fn print_review(review: &ops::retire::ReadinessReview) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let account = &review.account;

    println!("Retirement review: {}", review.root.path);
    println!();
    println!("  role           {}", review.root.role);
    if let Some(comment) = &review.root.comment {
        println!("  comment        {comment}");
    }
    println!(
        "  suspended      {}",
        if review.root.is_suspended() {
            "yes"
        } else {
            "no"
        }
    );
    println!(
        "  first indexed  {}",
        match review.first_indexed {
            Some(ts) => format_date(ts),
            None => "unknown".to_string(),
        }
    );
    println!(
        "  last scan      {}",
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
    if account.archived_standing > 0 {
        println!(
            "    archived from here   {}   (this copy still stands; the archive holds it)",
            format_count(account.archived_standing)
        );
    }
    println!(
        "    covered              {}   (content verified present in the archive)",
        format_count(account.covered)
    );
    println!(
        "    excluded             {}",
        format_count(account.excluded)
    );
    if account.contentless > 0 {
        println!(
            "    empty files          {}   (contentless — nothing to cover, nothing to verify)",
            format_count(account.contentless)
        );
    }
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

/// The readiness review's pointer to the story review: the review is the
/// gate's counts; the story is the substance behind them.
fn story_pointer(root_path: &str) -> String {
    format!("To read the story behind these counts: canon roots story path:{root_path}")
}

/// `canon roots story` — the judgment instrument: a source root's
/// resolution story as a path-ordered map of places, where you acted and
/// what no decision ever touched. Read-only in the fullest sense: no
/// decision row, no receipt, no cache — fresh per run; exits 0 (a report —
/// the verdict belongs to `retire --dry-run`).
pub fn story(db: &Db, spec: &str, limit: usize, all: bool) -> Result<()> {
    let conn = db.conn();
    let roots = repo::root::fetch_all(conn)?;
    // `_any`: a suspended root's story reads fine — as last observed.
    let root_id = parse_root_spec_any(&roots, spec)?;
    let report = ops::story::compute_story(conn, root_id, &StoryParams::default())?;

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let cap = if all { usize::MAX } else { limit };
    for line in ops::telling::story_lines(&report, cap, now) {
        println!("{line}");
    }
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

    use super::story_pointer;
    use crate::ops::telling::trail_handoff;

    #[test]
    fn story_argv_parses_through_the_real_cli() {
        for argv in [
            vec!["canon", "roots", "story", "path:/mnt/old-disk"],
            vec!["canon", "roots", "story", "id:3", "--limit", "10"],
            vec!["canon", "roots", "story", "id:3", "--all"],
        ] {
            crate::Cli::try_parse_from(&argv)
                .unwrap_or_else(|e| panic!("must parse: {argv:?}\n{e}"));
        }
        // --limit and --all are contradictory intents.
        assert!(crate::Cli::try_parse_from([
            "canon", "roots", "story", "id:3", "--limit", "5", "--all"
        ])
        .is_err());
    }

    #[test]
    fn trail_handoff_round_trips_through_the_real_cli() {
        for path in ["/r/photos", "/r/with space/x"] {
            let (display, argv) = trail_handoff(path);
            assert!(display.starts_with("→ canon trail "), "{display}");
            crate::Cli::try_parse_from(&argv)
                .unwrap_or_else(|e| panic!("handoff must parse: {display}\n{e}"));
        }
        let (display, _) = trail_handoff("/r/with space/x");
        assert!(display.contains("'/r/with space/x'"), "{display}");
    }

    #[test]
    fn story_pointer_names_the_command() {
        let pointer = story_pointer("/mnt/old-disk");
        assert!(
            pointer.contains("canon roots story path:/mnt/old-disk"),
            "{pointer}"
        );
    }

    #[test]
    fn retire_argv_parses_through_the_real_cli() {
        // The handoff-law discipline: CLI drift is a test failure.
        for argv in [
            vec!["canon", "roots", "retire", "/mnt/photos-backup"],
            vec!["canon", "roots", "retire", "id:3", "--dry-run"],
            vec![
                "canon",
                "roots",
                "retire",
                "/mnt/photos-backup",
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

    #[test]
    fn retired_argv_parses_through_the_real_cli() {
        crate::Cli::try_parse_from(["canon", "roots", "retired"])
            .unwrap_or_else(|e| panic!("must parse: {e}"));
        // The listing takes no arguments in v1.
        assert!(crate::Cli::try_parse_from(["canon", "roots", "retired", "/path"]).is_err());
    }
}
