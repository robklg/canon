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
use crate::domain::story::{ActGroup, LocationAggregate, StoryParams, StoryPlace};
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

    let bound = match ceremony_state.bind(conn) {
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
    for line in story_lines(&report, cap, now) {
        println!("{line}");
    }
    Ok(())
}

/// The whole report as lines — pure, so rendering is testable.
fn story_lines(report: &ops::story::StoryReport, cap: usize, now: i64) -> Vec<String> {
    let mut lines = Vec::new();
    lines.push(format!("Story: {}", report.root.path));
    lines.push(String::new());
    lines.push(format!("  role           {}", report.root.role));
    if let Some(comment) = &report.root.comment {
        lines.push(format!("  comment        {comment}"));
    }
    if report.root.is_suspended() {
        lines.push("  suspended      yes".to_string());
    }
    lines.push(format!(
        "  first indexed  {}",
        match report.first_indexed {
            Some(ts) => format_date(ts),
            None => "unknown".to_string(),
        }
    ));
    lines.push(format!(
        "  last scan      {}",
        match report.root.last_scanned_at {
            Some(ts) => format!("{} ({})", format_date(ts), format_time_ago(Some(ts), now)),
            None => "never".to_string(),
        }
    ));
    if !report.reachable {
        lines.push("  unreachable    the story as last observed — reconnect to verify".to_string());
    }
    lines.push(String::new());
    lines.push("The places".to_string());

    let mut shown = 0usize;
    let mut omitted = 0usize;
    render_place(
        &report.places,
        0,
        &report.root.path,
        cap,
        &mut shown,
        &mut omitted,
        &mut lines,
    );
    if omitted > 0 {
        lines.push(String::new());
        lines.push(format!(
            "  … and {omitted} more places (--all shows everything)"
        ));
    }

    let account = &report.account;
    lines.push(String::new());
    let mut unresolved = format_count(account.unresolved);
    if account.unhashed_unresolved > 0 {
        unresolved.push_str(&format!(
            " ({} never hashed)",
            format_count(account.unhashed_unresolved)
        ));
    }
    lines.push(format!(
        "Standing: {} sources — {} covered · {} excluded · {} unresolved",
        format_count(account.standing()),
        format_count(account.covered),
        format_count(account.excluded),
        unresolved,
    ));
    lines.push("Whether this story is complete is yours to judge.".to_string());
    lines.push(format!(
        "For the readiness gate: canon roots retire path:{} --dry-run",
        report.root.path
    ));
    lines
}

/// Whether a place earns its own block (the bare root is forced anyway).
fn place_renderable(place: &StoryPlace) -> bool {
    !place.acts.is_empty()
        || !place.standing.is_empty()
        || !place.covered_where.is_empty()
        || !place.notes.is_empty()
}

fn count_renderable(place: &StoryPlace) -> usize {
    place_renderable(place) as usize + place.children.iter().map(count_renderable).sum::<usize>()
}

fn render_place(
    place: &StoryPlace,
    depth: usize,
    root_path: &str,
    cap: usize,
    shown: &mut usize,
    omitted: &mut usize,
    lines: &mut Vec<String>,
) {
    let forced_root = depth == 0 && place.children.is_empty();
    let renderable = place_renderable(place) || forced_root;
    if renderable {
        if *shown >= cap {
            // The whole subtree drops; the omission line carries the count.
            *omitted += count_renderable(place);
            return;
        }
        *shown += 1;
        let indent = "  ".repeat(depth + 1);
        lines.push(String::new());
        let name = if place.rel_path.is_empty() {
            "(root)".to_string()
        } else {
            place.rel_path.clone()
        };
        let breadth = if place.folder_breadth > 1 {
            format!(
                "   · across {} folders",
                format_count(place.folder_breadth as i64)
            )
        } else {
            String::new()
        };
        lines.push(format!("{indent}{name}{breadth}"));
        for group in &place.acts {
            act_lines(group, &indent, lines);
        }
        if place.undecided() && (!place.standing.is_empty() || !place.covered_where.is_empty()) {
            lines.push(format!("{indent}  no decision here"));
        }
        standing_lines(place, &indent, lines);
        for note in &place.notes {
            lines.push(format!(
                "{indent}  note: {}",
                indent_multiline(&note.text, &format!("{indent}        "))
            ));
        }
        if !place_renderable(place) {
            lines.push(format!("{indent}  nothing indexed here"));
        }
        let abs = if place.rel_path.is_empty() {
            root_path.to_string()
        } else {
            format!("{root_path}/{}", place.rel_path)
        };
        let (display, _argv) = trail_handoff(&abs);
        lines.push(format!("{indent}  {display}"));
    }
    let child_depth = if renderable { depth + 1 } else { depth };
    for child in &place.children {
        render_place(child, child_depth, root_path, cap, shown, omitted, lines);
    }
}

/// One act group in the what/why register. The arrow means *sent there by
/// your act* — observed coverage renders with "copies stand in" instead,
/// never the arrow.
fn act_lines(group: &ActGroup, indent: &str, lines: &mut Vec<String>) {
    let mut line = format!(
        "{indent}  {} {} files",
        group.transition,
        format_count(group.files)
    );
    if let Some(bytes) = group.bytes {
        if bytes > 0 {
            line.push_str(&format!(", {}", format_size(bytes)));
        }
    }
    if group.observed {
        line.push_str(" (scan-observed)");
    }
    if let (Some(moved), Some(copied)) = (group.moved, group.copied) {
        if moved > 0 && copied > 0 {
            line.push_str(&format!(
                " ({} moved, {} copied)",
                format_count(moved),
                format_count(copied)
            ));
        }
    }
    if !group.destination.is_empty() {
        line.push_str(&format!(" → {}", fmt_locations(&group.destination)));
    }
    if group.decisions.len() == 1 {
        let decision = &group.decisions[0];
        line.push_str(&format!("   #{}", decision.id));
        if let Some(reason) = &decision.reason {
            line.push_str(&format!(
                " · \"{}\"",
                indent_multiline(reason, &format!("{indent}      "))
            ));
        }
        lines.push(line);
    } else {
        line.push_str(&format!("   across {} decisions", group.decisions.len()));
        lines.push(line);
        let summary = group.reason_summary();
        for (reason, ids) in &summary.reasons {
            let ids = ids
                .iter()
                .map(|id| format!("#{id}"))
                .collect::<Vec<_>>()
                .join(", ");
            lines.push(format!(
                "{indent}    · \"{}\"   {ids}",
                indent_multiline(reason, &format!("{indent}       "))
            ));
        }
        if summary.without_reason > 0 {
            lines.push(format!(
                "{indent}    · {} without reason",
                summary.without_reason
            ));
        }
    }
}

fn standing_lines(place: &StoryPlace, indent: &str, lines: &mut Vec<String>) {
    let standing = &place.standing;
    if standing.covered > 0 {
        let mut line = format!("{indent}  {} covered", format_count(standing.covered));
        if !place.covered_where.is_empty() {
            line.push_str(&format!(
                " — copies stand in {}",
                fmt_locations(&place.covered_where)
            ));
        }
        lines.push(line);
    }
    if standing.excluded > 0 {
        lines.push(format!(
            "{indent}  {} excluded",
            format_count(standing.excluded)
        ));
    }
    if standing.unresolved > 0 {
        let mut line = format!("{indent}  {} unresolved", format_count(standing.unresolved));
        if standing.unhashed_unresolved > 0 {
            line.push_str(&format!(
                " ({} never hashed — cannot be content-verified)",
                format_count(standing.unhashed_unresolved)
            ));
        }
        lines.push(line);
    }
    if standing.missing_unexplained > 0 {
        lines.push(format!(
            "{indent}  {} missing, unexplained",
            format_count(standing.missing_unexplained)
        ));
    }
}

/// A location aggregate for one line: a single coherent answer renders as
/// the bare path; a genuine divergence lists prefixes with counts; the
/// remainder is counted, never silent.
fn fmt_locations(agg: &LocationAggregate) -> String {
    let mut out = if agg.locations.len() == 1 && agg.omitted_locations == 0 {
        agg.locations[0].path.clone()
    } else {
        agg.locations
            .iter()
            .map(|l| format!("{} ({})", l.path, format_count(l.files)))
            .collect::<Vec<_>>()
            .join(", ")
    };
    if agg.omitted_locations > 0 {
        out.push_str(&format!(" … and {} more locations", agg.omitted_locations));
    }
    out
}

fn indent_multiline(text: &str, indent: &str) -> String {
    text.replace('\n', &format!("\n{indent}"))
}

/// The drill-down handoff: display and argv from one builder, so the
/// round-trip test parses exactly what the user sees (the sweep's law).
pub(crate) fn trail_handoff(abs_path: &str) -> (String, Vec<String>) {
    let argv: Vec<String> = vec!["canon".into(), "trail".into(), abs_path.into()];
    let display = argv
        .iter()
        .map(|a| crate::sweep::shell_quote(a))
        .collect::<Vec<_>>()
        .join(" ");
    (format!("→ {display}"), argv)
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

    use super::{story_lines, story_pointer, trail_handoff};
    use crate::domain::note::Note;
    use crate::domain::retire::ResolutionAccount;
    use crate::domain::root::Root;
    use crate::domain::story::{
        ActDecision, ActGroup, LocationAggregate, LocationCount, PlaceStanding, StoryPlace,
    };
    use crate::ops::story::StoryReport;

    fn place(rel: &str) -> StoryPlace {
        StoryPlace {
            rel_path: rel.to_string(),
            acts: vec![],
            standing: PlaceStanding::default(),
            covered_where: LocationAggregate::default(),
            notes: vec![],
            folder_breadth: 0,
            children: vec![],
        }
    }

    fn locations(entries: &[(&str, i64)]) -> LocationAggregate {
        LocationAggregate {
            locations: entries
                .iter()
                .map(|(path, files)| LocationCount {
                    path: path.to_string(),
                    files: *files,
                })
                .collect(),
            omitted_locations: 0,
        }
    }

    fn act(transition: &'static str, files: i64, decisions: Vec<(i64, Option<&str>)>) -> ActGroup {
        ActGroup {
            transition,
            observed: false,
            destination: LocationAggregate::default(),
            files,
            bytes: None,
            moved: None,
            copied: None,
            decisions: decisions
                .into_iter()
                .enumerate()
                .map(|(i, (id, reason))| ActDecision {
                    id,
                    created_at: (i as i64 + 1) * 100,
                    reason: reason.map(str::to_string),
                })
                .collect(),
        }
    }

    fn zero_account() -> ResolutionAccount {
        ResolutionAccount {
            archived_files: 0,
            archived_bytes: None,
            archived_moved: 0,
            archived_copied: 0,
            archived_unrecorded: 0,
            deleted: 0,
            unexplained_missing: 0,
            covered: 0,
            excluded: 0,
            unresolved: 0,
            unhashed_unresolved: 0,
        }
    }

    fn report(places: StoryPlace) -> StoryReport {
        StoryReport {
            root: Root {
                id: 1,
                path: "/r".to_string(),
                role: "source".to_string(),
                comment: None,
                last_scanned_at: None,
                suspended: false,
            },
            first_indexed: None,
            reachable: true,
            places,
            account: zero_account(),
        }
    }

    fn assert_has_line(lines: &[String], needle: &str) {
        assert!(
            lines.iter().any(|l| l.contains(needle)),
            "missing {needle:?} in:\n{}",
            lines.join("\n")
        );
    }

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
    fn rendering_shows_containment_acts_and_the_undecided() {
        let mut italy = place("pictures/italy");
        italy.standing.covered = 2;
        italy.covered_where = locations(&[("/archive/a", 1), ("/archive/b", 1)]);

        let mut pictures = place("pictures");
        let mut archived = act("archived", 5, vec![(42, Some("the Italy trip"))]);
        archived.destination = locations(&[("/archive/media", 5)]);
        pictures.acts.push(archived);
        pictures.children.push(italy);

        let mut root = place("");
        root.children.push(pictures);

        let lines = story_lines(&report(root), usize::MAX, 0);
        assert_has_line(&lines, "  pictures");
        assert_has_line(
            &lines,
            "archived 5 files → /archive/media   #42 · \"the Italy trip\"",
        );
        assert_has_line(&lines, "    pictures/italy");
        assert_has_line(&lines, "      no decision here");
        assert_has_line(
            &lines,
            "      2 covered — copies stand in /archive/a (1), /archive/b (1)",
        );
        assert_has_line(&lines, "→ canon trail /r/pictures");
        assert_has_line(&lines, "→ canon trail /r/pictures/italy");
        assert_has_line(&lines, "Whether this story is complete is yours to judge.");
        assert_has_line(&lines, "canon roots retire path:/r --dry-run");
    }

    #[test]
    fn multi_decision_acts_enumerate_the_whys() {
        let mut old = place("old");
        old.acts.push(act(
            "excluded",
            4890,
            vec![
                (57, Some("installer junk")),
                (61, Some("installer junk")),
                (63, None),
            ],
        ));
        let mut root = place("");
        root.children.push(old);

        let lines = story_lines(&report(root), usize::MAX, 0);
        assert_has_line(&lines, "excluded 4,890 files   across 3 decisions");
        assert_has_line(&lines, "· \"installer junk\"   #57, #61");
        assert_has_line(&lines, "· 1 without reason");
    }

    #[test]
    fn observed_deletions_read_as_observations() {
        let mut gone = place("gone");
        let mut deleted = act("deleted", 1204, vec![(70, None)]);
        deleted.observed = true;
        gone.acts.push(deleted);
        let mut root = place("");
        root.children.push(gone);

        let lines = story_lines(&report(root), usize::MAX, 0);
        assert_has_line(&lines, "deleted 1,204 files (scan-observed)   #70");
    }

    #[test]
    fn notes_render_verbatim_at_their_place() {
        let mut keep = place("keep");
        keep.notes.push(Note {
            id: 7,
            root_id: 1,
            rel_path: "keep".to_string(),
            text: "beautiful pictures, still need a home".to_string(),
            created_at: 100,
        });
        let mut root = place("");
        root.children.push(keep);

        let lines = story_lines(&report(root), usize::MAX, 0);
        assert_has_line(&lines, "note: beautiful pictures, still need a home");
    }

    #[test]
    fn the_cap_counts_omissions_and_keeps_the_close() {
        let mut root = place("");
        for name in ["a", "b", "c"] {
            let mut child = place(name);
            child.standing.unresolved = 1;
            root.children.push(child);
        }
        let lines = story_lines(&report(root), 1, 0);
        assert_has_line(&lines, "… and 2 more places (--all shows everything)");
        assert_has_line(&lines, "Standing: 0 sources");
    }

    #[test]
    fn a_bare_root_still_tells_its_empty_story() {
        let lines = story_lines(&report(place("")), usize::MAX, 0);
        assert_has_line(&lines, "  (root)");
        assert_has_line(&lines, "nothing indexed here");
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
