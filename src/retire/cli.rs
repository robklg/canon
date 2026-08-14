use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;

use crate::ceremony;
use crate::domain::config::{LedgerConfig, RecordingMode};
use crate::domain::format::{format_date, format_size, format_time_ago};
use crate::domain::format_count;
use crate::ops::scope::parse_root_spec_any;
use crate::repo::{self, Db};
use crate::retire::domain::Readiness;
use crate::story::StoryParams;

/// `canon roots retired` — the retired fleet, one line per book.
pub fn retired(db: &Db, config: &LedgerConfig) -> Result<()> {
    let listing = super::ops::compute_shelf_listing(db.conn(), config)?;
    let books = listing
        .lines
        .iter()
        .filter(|l| matches!(l, super::ops::ShelfLine::Book { .. }))
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
            super::ops::ShelfLine::Book {
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
            super::ops::ShelfLine::RecordedOnly {
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
            super::ops::ShelfLine::Unidentified { dir_name } => {
                println!("            {dir_name}/ — on the shelf, but not identifiable as a book");
            }
        }
    }
    // Never silent: a stranded aside is a full book copy from an
    // interrupted swap — named, so the reader can compare and clean up.
    for aside in &listing.aside_dirs {
        println!(
            "            {aside}/ — a replaced book, set aside by an interrupted swap; \
             the standing book of the same name is current"
        );
    }
    Ok(())
}

/// The retirement ceremony: review → confirm → bind → confirm → release.
/// Review and ceremony on stdout (a ceremony surface, not a list command);
/// `--dry-run` always exits 0 (it is a report); a NOT READY verdict without
/// `--allow unresolved` exits non-zero after the review (compare precedent:
/// the verdict is the message, no `Error:` duplication). A world-moved
/// release exits non-zero and asks to be re-run.
#[allow(clippy::too_many_arguments)]
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
    super::ops::validate_retire_target(&roots, root_id, config)?;

    let story = crate::core::ops::root_story::fetch_root_story(conn, root_id)?;
    let review = super::ops::readiness_lens(&story);
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

    // The report exits 0 even on a NOT READY verdict; only a real run turns
    // that verdict into the non-zero exit below.
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
    let plan = super::ops::plan_bind(&story, config, now)?;

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

    // The decision row starts here, after the confirmation — a declined
    // prompt must leave no record of a retirement that never happened.
    let mut ceremony_state = super::ops::begin_ceremony(
        conn,
        story,
        &review,
        plan,
        super::ops::CeremonyParams {
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
        super::ops::ReleaseOutcome::Released {
            summary, warnings, ..
        } => {
            println!("{summary}");
            println!("The storage is yours to discard.");
            for warning in &warnings {
                eprintln!("{warning}");
            }
        }
        super::ops::ReleaseOutcome::WorldMoved { detail, warnings } => {
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
    ceremony_state: &super::ops::RetireCeremony,
    conn: &rusqlite::Connection,
    yes: bool,
) -> Result<super::ops::frame::TellingArtifact> {
    let draft = ceremony_state.compose_telling(conn)?;
    let composed = super::ops::frame::finalize_telling(&draft)?;
    let artifact = |text: String| {
        let hand_edited = text != composed;
        super::ops::frame::TellingArtifact {
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
            Ok(Some(edited)) => match super::ops::frame::finalize_telling(&edited) {
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

fn print_review(review: &super::ops::ReadinessReview) {
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

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::story_pointer;

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
