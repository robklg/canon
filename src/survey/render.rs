//! Survey's presentation layer: the nine `print_*` functions that render a
//! computed survey outcome to stdout, plus the display-cap constants they share.

use std::collections::HashMap;

use crate::domain::format_count;
use crate::note::format_note_date;
use crate::ops::note::SurveyNoteContext;
use crate::ops::scope::ResolvedScope;
use crate::survey::ops::compute::{ArchivedLocationDetail, LocationResult};

const DETAIL_SAMPLE_SIZE: usize = 5;
const DETAIL_SHOW_ALL_THRESHOLD: usize = 20;
const DEFAULT_LOCATION_CAP: usize = 10;
const NOTE_DISPLAY_CAP: usize = 5;

pub(super) fn print_survey_header(
    scope: &ResolvedScope,
    original_filters: &[String],
    total: usize,
    unhashed: usize,
    hashed: usize,
    contentless: usize,
    unique_count: Option<usize>,
) {
    let mut handle = std::io::stdout().lock();
    crate::scope::print_report_scope(&mut handle, "Survey", scope);
    drop(handle);

    if !original_filters.is_empty() {
        println!("  Filters: {}", original_filters.join(" AND "));
    }

    println!(
        "  {} sources here ({} unhashed, {} hashed)",
        format_count(total),
        format_count(unhashed),
        format_count(hashed),
    );

    // The contentless law's "stated, never silent": empty files vanish
    // from every comparison (the index refuses them), so the summary
    // counts them — rendered only when present, like its siblings on
    // sweep, compare, and coverage.
    if contentless > 0 {
        println!(
            "  {} empty files (no content to compare)",
            format_count(contentless)
        );
    }

    if let Some(unique) = unique_count {
        println!("  {} unique here", format_count(unique));
    }
}

pub(super) fn print_notes_section(ctx: &SurveyNoteContext, scope_rel_path: &str, verbose: bool) {
    let has_subtree = !ctx.subtree_notes.is_empty();
    let has_ancestors = ctx.ancestor_count > 0;

    if !has_subtree && !has_ancestors {
        return;
    }

    if has_subtree {
        let cap = if verbose {
            ctx.subtree_notes.len()
        } else {
            NOTE_DISPLAY_CAP
        };
        let display_notes = &ctx.subtree_notes[..cap.min(ctx.subtree_notes.len())];
        let remaining_notes = ctx.subtree_notes.len().saturating_sub(cap);

        // Compute alignment width for relative paths
        let rel_paths: Vec<String> = display_notes
            .iter()
            .map(|n| crate::domain::note::relative_to_scope(&n.rel_path, scope_rel_path))
            .collect();
        let max_rel_len = rel_paths.iter().map(|p| p.len()).max().unwrap_or(0);

        println!("  Notes:");
        for (note, rel) in display_notes.iter().zip(rel_paths.iter()) {
            println!(
                "    {}  {:rel_w$}  {}",
                format_note_date(note.created_at),
                rel,
                note.text,
                rel_w = max_rel_len,
            );
        }

        // Summary line for hidden notes and/or ancestor notes
        let mut parts = Vec::new();
        if remaining_notes > 0 {
            // Count distinct locations in the hidden notes
            let hidden_notes = &ctx.subtree_notes[cap..];
            let hidden_locations: std::collections::HashSet<&str> =
                hidden_notes.iter().map(|n| n.rel_path.as_str()).collect();
            parts.push(format!(
                "{} earlier notes across {} locations",
                remaining_notes,
                hidden_locations.len()
            ));
        }
        if has_ancestors {
            parts.push(format!("{} ancestral notes", ctx.ancestor_count));
        }
        if !parts.is_empty() {
            println!("    ({})", parts.join(", "));
        }
    } else {
        // No subtree notes but ancestors exist
        println!("  ({} ancestral notes)", ctx.ancestor_count);
    }
}

pub(super) fn print_archive_section(
    archived_count: usize,
    total_hashed: usize,
    archive_scopes: &[(String, usize)],
    archive_label: Option<&str>,
    verbose: bool,
) {
    let header = match archive_label {
        Some(label) => format!("Archived ({label})"),
        None => "Archived".to_string(),
    };

    if archived_count == 0 {
        println!("{}: 0 of {}", header, format_count(total_hashed));
        return;
    }

    let pct = 100.0 * archived_count as f64 / total_hashed as f64;
    println!(
        "{}: {} of {} ({:.1}%)",
        header,
        format_count(archived_count),
        format_count(total_hashed),
        pct,
    );

    // Sort by count descending for display
    let mut sorted: Vec<&(String, usize)> = archive_scopes.iter().collect();
    sorted.sort_by(|a, b| b.1.cmp(&a.1));

    // Adaptive cap: show all when ≤20, top 10 when >20 (unless --verbose)
    let (display, hidden) = if verbose || sorted.len() <= DETAIL_SHOW_ALL_THRESHOLD {
        (sorted.as_slice(), 0)
    } else {
        let cap = DEFAULT_LOCATION_CAP;
        (&sorted[..cap], sorted.len() - cap)
    };

    // Scope-grouped archive paths with right-aligned counts
    let max_path_len = display.iter().map(|(p, _)| p.len()).max().unwrap_or(0);
    let max_count_len = display
        .iter()
        .map(|(_, c)| format_count(*c).len())
        .max()
        .unwrap_or(0);

    for (path, count) in display {
        println!(
            "  {:path_w$}  {:>count_w$}",
            path,
            format_count(*count),
            path_w = max_path_len,
            count_w = max_count_len,
        );
    }

    if hidden > 0 {
        println!("  ... and {hidden} more locations (use --verbose to show all)");
    }
}

pub(super) fn print_archived_detail(
    details: &[ArchivedLocationDetail],
    archived_count: usize,
    cwd: Option<&str>,
    verbose: bool,
    null_delim: bool,
) {
    use std::io::Write;

    if null_delim {
        // Flat output: null-delimited selection-side paths of archived files
        let stdout = std::io::stdout();
        let mut handle = stdout.lock();
        let mut seen = std::collections::HashSet::new();
        for detail in details {
            for pair in &detail.pairs {
                if seen.insert(&pair.selection_path) {
                    let display = crate::domain::path::format_path(&pair.selection_path, cwd);
                    // Ignore write errors: a closed pipe (piped to head, say)
                    // must end the command quietly, not panic.
                    let _ = write!(handle, "{}\0", display);
                }
            }
        }
        return;
    }

    if details.is_empty() {
        println!("No archived files in selection.");
        return;
    }

    println!(
        "Archived files ({} sources across {} locations):\n",
        format_count(archived_count),
        details.len(),
    );

    for detail in details {
        let count = detail.pairs.len();
        println!(
            "  Archived at {} ({} files):",
            detail.path,
            format_count(count),
        );

        let show_all = verbose || count <= DETAIL_SHOW_ALL_THRESHOLD;
        let display_pairs = if show_all {
            &detail.pairs[..]
        } else {
            &detail.pairs[..DETAIL_SAMPLE_SIZE]
        };

        for pair in display_pairs {
            let display = crate::domain::path::format_path(&pair.selection_path, cwd);
            println!("    {display}");
            for cp in &pair.counterpart_paths {
                println!("      → {cp}");
            }
        }

        if !show_all {
            println!(
                "    ... and {} more (use --verbose to show all)",
                count - DETAIL_SAMPLE_SIZE
            );
        }
        println!();
    }
}

pub(super) fn print_related_locations(
    locations: &[LocationResult],
    total_hashed: usize,
    is_other_mode: bool,
    verbose: bool,
    note_counts: &HashMap<String, usize>,
) {
    if locations.is_empty() {
        if is_other_mode {
            println!("No shared content at specified locations.");
        } else {
            println!("No related locations found.");
        }
        return;
    }

    if is_other_mode {
        println!("Comparing with:");
    } else {
        println!("Related locations:");
    }

    let display_locations = if verbose || locations.len() <= DEFAULT_LOCATION_CAP {
        locations
    } else {
        &locations[..DEFAULT_LOCATION_CAP]
    };
    let truncated_count = locations.len() - display_locations.len();

    let max_path_len = display_locations
        .iter()
        .map(|l| l.path.len())
        .max()
        .unwrap_or(0);
    let max_shared_len = display_locations
        .iter()
        .map(|l| format_count(l.shared_count).len())
        .max()
        .unwrap_or(0);
    let m_str = format_count(total_hashed);

    for loc in display_locations {
        // Base: path + shared count + total (always present)
        print!(
            "  {:path_w$}  {:>count_w$} of {} overlap ({} total)",
            loc.path,
            format_count(loc.shared_count),
            m_str,
            format_count(loc.total_count),
            path_w = max_path_len,
            count_w = max_shared_len,
        );

        // Affinity columns (only when present and complementary > 0)
        match (loc.complementary_count, loc.only_here_count) {
            (Some(comp), Some(only)) if comp > 0 => {
                print!("   +{} more", format_count(comp));
                if only > 0 {
                    print!(" ({} unique)", format_count(only));
                }
            }
            _ => {} // Mirror/Subset or no affinity — no affinity columns
        }

        // Note indicator
        if let Some(&count) = note_counts.get(&loc.path) {
            print!("  ({} notes)", count);
        }

        println!();
    }

    if truncated_count > 0 {
        println!(
            "  ... and {} more locations (use --verbose to show all)",
            format_count(truncated_count),
        );
    }
}

pub(super) fn print_complement_detail(
    locations: &[LocationResult],
    total_hashed: usize,
    is_other_mode: bool,
    verbose: bool,
) {
    if is_other_mode {
        println!("Complementary content at specified locations:");
    } else {
        println!("Complementary content at related locations:");
    }

    let mut any_output = false;

    for loc in locations {
        let paths = match &loc.complementary_paths {
            Some(p) => p,
            None => continue,
        };

        if paths.is_empty() {
            if is_other_mode {
                // --other mode: show mirrors with a note
                any_output = true;
                println!();
                println!(
                    "  {} — no complementary content ({} of {} shared)",
                    loc.path,
                    format_count(loc.shared_count),
                    format_count(total_hashed),
                );
            }
            // Default mode: skip mirrors silently
            continue;
        }

        any_output = true;
        println!();

        // Header: path (+N, K unique):
        let comp_count = loc.complementary_count.unwrap_or(0);
        let only_here = loc.only_here_count.unwrap_or(0);
        if only_here > 0 {
            println!(
                "  {} (+{}, {} unique):",
                loc.path,
                format_count(comp_count),
                format_count(only_here),
            );
        } else {
            println!("  {} (+{}):", loc.path, format_count(comp_count));
        }

        // Paths (capped unless --verbose or small enough to show all)
        let show_count = if verbose || paths.len() <= DETAIL_SHOW_ALL_THRESHOLD {
            paths.len()
        } else {
            DETAIL_SAMPLE_SIZE.min(paths.len())
        };
        for path in &paths[..show_count] {
            println!("    {path}");
        }
        if !verbose && paths.len() > DETAIL_SHOW_ALL_THRESHOLD {
            println!(
                "    ... and {} more (use --verbose to show all)",
                format_count(paths.len() - DETAIL_SAMPLE_SIZE),
            );
        }
    }

    if !any_output {
        println!();
        if is_other_mode {
            println!("  No complementary content found at specified locations.");
        } else {
            println!("  No complementary content found at related locations.");
        }
    }
}

pub(super) fn print_overlap_detail(
    locations: &[LocationResult],
    total_hashed: usize,
    is_other_mode: bool,
    verbose: bool,
    cwd: Option<&str>,
    null_delim: bool,
) {
    if null_delim {
        use std::io::Write;
        // -0 mode: flat, deduplicated, selection-side paths only (no counterparts)
        let all_paths: std::collections::BTreeSet<&str> = locations
            .iter()
            .filter_map(|loc| loc.overlap_pairs.as_ref())
            .flat_map(|pairs| pairs.iter().map(|p| p.selection_path.as_str()))
            .collect();
        let stdout = std::io::stdout();
        let mut handle = stdout.lock();
        for path in all_paths {
            // Ignore broken pipe errors when stdout is closed
            if write!(handle, "{path}\0").is_err() {
                break;
            }
        }
        return;
    }

    // Human-readable mode
    if is_other_mode {
        println!("Overlapping with specified locations (overlap):");
    } else {
        println!("Overlapping with related locations (overlap):");
    }

    let display_locations = if verbose || locations.len() <= DEFAULT_LOCATION_CAP {
        locations
    } else {
        &locations[..DEFAULT_LOCATION_CAP]
    };

    let mut any_output = false;

    for loc in display_locations {
        let pairs = match &loc.overlap_pairs {
            Some(p) => p,
            None => continue,
        };

        if pairs.is_empty() {
            continue;
        }

        any_output = true;
        println!();

        // Header: /path/ (N of M overlap):
        println!(
            "  {} ({} of {} overlap):",
            crate::domain::path::format_path(&loc.path, cwd),
            format_count(loc.shared_count),
            format_count(total_hashed),
        );

        // Pairs (capped unless --verbose or small enough to show all)
        let show_count = if verbose || pairs.len() <= DETAIL_SHOW_ALL_THRESHOLD {
            pairs.len()
        } else {
            DETAIL_SAMPLE_SIZE.min(pairs.len())
        };
        for pair in &pairs[..show_count] {
            println!(
                "    {}",
                crate::domain::path::format_path(&pair.selection_path, cwd)
            );
            for cp in &pair.counterpart_paths {
                println!("      \u{2192} {cp}");
            }
        }
        if !verbose && pairs.len() > DETAIL_SHOW_ALL_THRESHOLD {
            println!(
                "    ... and {} more (use --verbose to show all)",
                format_count(pairs.len() - DETAIL_SAMPLE_SIZE),
            );
        }
    }

    if !any_output {
        println!();
        if is_other_mode {
            println!("  No overlapping content at specified locations.");
        } else {
            println!("  No overlapping content found.");
        }
    }

    // Location truncation notice
    if !verbose && locations.len() > DEFAULT_LOCATION_CAP {
        println!();
        println!(
            "  ({} locations not shown, use --verbose to see all)",
            format_count(locations.len() - DEFAULT_LOCATION_CAP),
        );
    }
}

pub(super) fn print_residual_detail(
    locations: &[LocationResult],
    cwd: Option<&str>,
    verbose: bool,
    null_delim: bool,
) {
    if null_delim {
        use std::io::Write;
        // -0 mode: flat, deduplicated, absolute paths
        let all_paths: std::collections::BTreeSet<&str> = locations
            .iter()
            .filter_map(|loc| loc.residual_paths.as_ref())
            .flat_map(|paths| paths.iter().map(|p| p.as_str()))
            .collect();
        let stdout = std::io::stdout();
        let mut handle = stdout.lock();
        for path in all_paths {
            // Ignore broken pipe errors when stdout is closed
            if write!(handle, "{path}\0").is_err() {
                break;
            }
        }
        return;
    }

    // Human-readable mode: per-location grouping
    for loc in locations {
        let paths = match &loc.residual_paths {
            Some(p) => p,
            None => continue,
        };

        println!(
            "Not at {} (residual):",
            crate::domain::path::format_path(&loc.path, cwd),
        );

        if paths.is_empty() {
            println!("  (none)");
        } else {
            // Capped unless --verbose or small enough to show all
            let show_count = if verbose || paths.len() <= DETAIL_SHOW_ALL_THRESHOLD {
                paths.len()
            } else {
                DETAIL_SAMPLE_SIZE.min(paths.len())
            };
            for path in &paths[..show_count] {
                println!("  {}", crate::domain::path::format_path(path, cwd));
            }
            if !verbose && paths.len() > DETAIL_SHOW_ALL_THRESHOLD {
                println!(
                    "  ... and {} more (use --verbose to show all)",
                    format_count(paths.len() - DETAIL_SAMPLE_SIZE),
                );
            }
        }
        println!();
    }
}

pub(super) fn print_unique_detail(paths: &[String], null_delim: bool, cwd: Option<&str>) {
    use std::io::Write;
    let stdout = std::io::stdout();
    let mut handle = stdout.lock();

    for path in paths {
        let display = if null_delim {
            path.clone() // -0: always absolute
        } else {
            crate::domain::path::format_path(path, cwd)
        };
        // Ignore broken pipe errors (EPIPE) when stdout is closed by consumer (e.g., piped to `head`)
        let sep = if null_delim { "\0" } else { "\n" };
        if write!(handle, "{display}{sep}").is_err() {
            // Pipe closed; exit gracefully
            break;
        }
    }
}
