use anyhow::{bail, Result};
use std::path::PathBuf;

use crate::ceremony::format_count;
use crate::domain;
use crate::domain::root::parse_root_spec;
use crate::domain::IncludeSet;
use crate::expr::filter::Filter;
use crate::ops::survey::{LocationResult, SurveyOutcome, SurveyParams};
use crate::repo;
use crate::ops::scope::ResolvedScope;

const DETAIL_SAMPLE_SIZE: usize = 5;
const DETAIL_SHOW_ALL_THRESHOLD: usize = 20;
const DEFAULT_LOCATION_CAP: usize = 10;

/// Detail output mode for `--detail`.
#[derive(Clone, Copy, PartialEq, clap::ValueEnum)]
pub enum DetailMode {
    /// Show which selection files are archived and where
    Archived,
    /// Show complementary content at related locations
    Complement,
    /// Output bare paths of unique-to-selection content
    Unique,
    /// Show which selection files overlap with each related location
    Overlap,
    /// Show selection files NOT shared with reference location(s)
    Residual,
}

/// Options controlling survey behavior.
pub struct SurveyOptions {
    /// Original (pre-expansion) filter strings — for display in selection header.
    pub original_filters: Vec<String>,
    /// Visibility control (--include excluded).
    pub include: IncludeSet,
    /// Compare against specific locations instead of discovering them.
    pub other_paths: Vec<PathBuf>,
    /// Opt into affinity enrichment (requires --where).
    pub affinity: bool,
    /// Skip per-location affinity computation.
    pub brief: bool,
    /// Detail output mode (replaces summary).
    pub detail: Option<DetailMode>,
    /// Null-delimited output for --detail unique/overlap.
    pub null_delim: bool,
    /// Filter archive section to a specific archive root.
    pub archive: Option<String>,
    /// Show all paths per location (complement view) / all locations (summary).
    pub verbose: bool,
    /// Resolved scope information for display.
    pub scope: ResolvedScope,
}

pub fn run(
    db: &mut repo::Db,
    paths: &[PathBuf],
    filter_strs: &[String],
    options: &SurveyOptions,
) -> Result<()> {
    // Validate --detail complement requires --where
    if options.detail == Some(DetailMode::Complement) && filter_strs.is_empty() {
        bail!("`--detail complement` requires `--where` filters to define matching content.");
    }

    // Validate --detail residual requires --other
    if options.detail == Some(DetailMode::Residual) && options.other_paths.is_empty() {
        bail!("`--detail residual` requires `--other` to specify a reference location.");
    }

    // Validate --affinity requires --where
    if options.affinity && filter_strs.is_empty() {
        bail!("`--affinity` requires `--where` filters.");
    }

    // Parse expanded filter strings
    let filters: Vec<Filter> = filter_strs
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    let conn = db.conn_mut();

    // Fetch all roots and sources upfront
    let all_roots = repo::root::fetch_all(conn)?;

    // Scope is now resolved by the caller via scope::resolve_scope() and passed as paths.
    // Resolve scope prefixes for display and validation.
    let scope_prefixes = domain::path::resolve_paths(paths, &all_roots)?;
    domain::path::warn_nonexistent_scope_paths(&scope_prefixes, &all_roots);

    // Resolve --other paths (same soft resolution as scope paths)
    let other_resolved = if !options.other_paths.is_empty() {
        let resolved = domain::path::resolve_paths(&options.other_paths, &all_roots)?;
        domain::path::warn_nonexistent_scope_paths(&resolved, &all_roots);
        resolved
    } else {
        Vec::new()
    };

    // Validate that --other doesn't match the surveyed scope
    for other_path in &other_resolved {
        if scope_prefixes.contains(other_path) {
            bail!("Error: --other location is identical to the surveyed scope. Comparing a location to itself is not meaningful.");
        }
    }

    // Resolve --archive spec (must be archive role)
    let archive_root_id = if let Some(ref spec) = options.archive {
        Some(parse_root_spec(&all_roots, spec, Some("archive"))?)
    } else {
        None
    };
    let archive_label = archive_root_id.map(|id| {
        let root = all_roots.iter().find(|r| r.id == id).unwrap();
        format!("in {}", root.path)
    });

    let root_ids: Vec<i64> = all_roots.iter().map(|r| r.id).collect();
    let all_sources = repo::source::batch_fetch_by_roots(conn, &root_ids)?;

    // Build computation params from options
    let compute_affinity =
        (options.affinity || options.detail == Some(DetailMode::Complement)) && !options.brief;
    let params = SurveyParams {
        include: options.include.clone(),
        compute_affinity,
        compute_overlap_pairs: options.detail == Some(DetailMode::Overlap),
        compute_residual: options.detail == Some(DetailMode::Residual),
        compute_archived_pairs: options.detail == Some(DetailMode::Archived),
    };

    match crate::ops::survey::compute_survey(
        conn,
        paths,
        &filters,
        &params,
        &all_sources,
        &all_roots,
        &other_resolved,
        archive_root_id,
    )? {
        SurveyOutcome::Empty => {
            let suppress = options.null_delim
                && matches!(
                    options.detail,
                    Some(DetailMode::Unique)
                        | Some(DetailMode::Overlap)
                        | Some(DetailMode::Residual)
                );
            if !suppress {
                print_survey_header(&options.scope, &options.original_filters, 0, 0, 0, None);
            }
        }
        SurveyOutcome::AllUnhashed {
            total_count,
        } => {
            let suppress = options.null_delim
                && matches!(
                    options.detail,
                    Some(DetailMode::Unique)
                        | Some(DetailMode::Overlap)
                        | Some(DetailMode::Residual)
                );
            if !suppress {
                print_survey_header(
                    &options.scope,
                    &options.original_filters,
                    total_count,
                    total_count,
                    0,
                    None,
                );
                println!();
                println!("No hashed sources in selection. Content comparison requires hashing.");
                println!("Run `canon scan` to hash these sources.");
            }
        }
        SurveyOutcome::Result(mut result) => {
            result.archive_label = archive_label;
            let display_cwd = std::env::current_dir()
                .ok()
                .and_then(|p| p.to_str().map(|s| s.to_string()));
            match options.detail {
                Some(DetailMode::Archived) => {
                    if !options.null_delim {
                        print_survey_header(
                            &options.scope,
                            &options.original_filters,
                            result.total_count,
                            result.unhashed_count,
                            result.total_hashed,
                            Some(result.unique_count),
                        );
                        println!();
                    }
                    let cwd = if options.null_delim {
                        None
                    } else {
                        display_cwd.as_deref()
                    };
                    print_archived_detail(
                        &result.archived_details,
                        result.archived_source_count,
                        cwd,
                        options.verbose,
                        options.null_delim,
                    );
                }
                Some(DetailMode::Complement) => {
                    print_survey_header(
                        &options.scope,
                        &options.original_filters,
                        result.total_count,
                        result.unhashed_count,
                        result.total_hashed,
                        Some(result.unique_count),
                    );
                    println!();
                    print_complement_detail(
                        &result.location_results,
                        result.total_hashed,
                        result.is_other_mode,
                        options.verbose,
                    );
                }
                Some(DetailMode::Overlap) => {
                    if !options.null_delim {
                        print_survey_header(
                            &options.scope,
                            &options.original_filters,
                            result.total_count,
                            result.unhashed_count,
                            result.total_hashed,
                            Some(result.unique_count),
                        );
                        println!();
                    }
                    let cwd = if options.null_delim {
                        None
                    } else {
                        display_cwd.as_deref()
                    };
                    print_overlap_detail(
                        &result.location_results,
                        result.total_hashed,
                        result.is_other_mode,
                        options.verbose,
                        cwd,
                        options.null_delim,
                    );
                }
                Some(DetailMode::Residual) => {
                    if !options.null_delim {
                        print_survey_header(
                            &options.scope,
                            &options.original_filters,
                            result.total_count,
                            result.unhashed_count,
                            result.total_hashed,
                            Some(result.unique_count),
                        );
                        println!();
                    }
                    let cwd = if options.null_delim {
                        None
                    } else {
                        display_cwd.as_deref()
                    };
                    print_residual_detail(
                        &result.location_results,
                        cwd,
                        options.verbose,
                        options.null_delim,
                    );
                }
                Some(DetailMode::Unique) => {
                    let cwd = if options.null_delim {
                        None
                    } else {
                        display_cwd.as_deref()
                    };
                    print_unique_detail(&result.unique_paths, options.null_delim, cwd);
                }
                None => {
                    print_survey_header(
                        &options.scope,
                        &options.original_filters,
                        result.total_count,
                        result.unhashed_count,
                        result.total_hashed,
                        Some(result.unique_count),
                    );
                    println!();
                    if !result.is_other_mode {
                        print_archive_section(
                            result.archived_source_count,
                            result.total_hashed,
                            &result.archive_scopes,
                            result.archive_label.as_deref(),
                            options.verbose,
                        );
                        println!();
                    }
                    print_related_locations(
                        &result.location_results,
                        result.total_hashed,
                        result.is_other_mode,
                        options.verbose,
                    );
                }
            }

            // Visibility hint for status predicates
            if result.used_status.excluded
                && !options.include.includes_excluded()
                && result.excluded_count > 0
            {
                eprintln!(
                    "({} excluded sources hidden, use --include excluded to show)",
                    result.excluded_count
                );
            }
        }
    }

    Ok(())
}

// =============================================================================
// Output formatting
// =============================================================================

fn print_survey_header(
    scope: &ResolvedScope,
    original_filters: &[String],
    total: usize,
    unhashed: usize,
    hashed: usize,
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

    if let Some(unique) = unique_count {
        println!("  {} unique here", format_count(unique));
    }
}

fn print_archive_section(
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

fn print_archived_detail(
    details: &[crate::ops::survey::ArchivedLocationDetail],
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
                    let display = domain::path::format_path(&pair.selection_path, cwd);
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
            let display = domain::path::format_path(&pair.selection_path, cwd);
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

fn print_related_locations(
    locations: &[LocationResult],
    total_hashed: usize,
    is_other_mode: bool,
    verbose: bool,
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

        println!();
    }

    if truncated_count > 0 {
        println!(
            "  ... and {} more locations (use --verbose to show all)",
            format_count(truncated_count),
        );
    }
}

fn print_complement_detail(
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

fn print_overlap_detail(
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
            domain::path::format_path(&loc.path, cwd),
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
                domain::path::format_path(&pair.selection_path, cwd)
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

fn print_residual_detail(
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
            domain::path::format_path(&loc.path, cwd),
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
                println!("  {}", domain::path::format_path(path, cwd));
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

fn print_unique_detail(paths: &[String], null_delim: bool, cwd: Option<&str>) {
    use std::io::Write;
    let stdout = std::io::stdout();
    let mut handle = stdout.lock();

    for path in paths {
        let display = if null_delim {
            path.clone() // -0: always absolute
        } else {
            domain::path::format_path(path, cwd)
        };
        // Ignore broken pipe errors (EPIPE) when stdout is closed by consumer (e.g., piped to `head`)
        let sep = if null_delim { "\0" } else { "\n" };
        if write!(handle, "{display}{sep}").is_err() {
            // Pipe closed; exit gracefully
            break;
        }
    }
}

// =============================================================================
// Tests — validation logic only (computation tests are in ops/survey.rs)
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::test_helpers::{insert_object, insert_root, insert_source};
    use crate::repo::open_in_memory_for_test;

    fn test_options() -> SurveyOptions {
        SurveyOptions {
            original_filters: Vec::new(),
            include: IncludeSet::default(),
            other_paths: Vec::new(),
            affinity: false,
            brief: false,
            detail: None,
            null_delim: false,
            archive: None,
            verbose: false,
            scope: ResolvedScope {
                prefixes: vec!["/mnt/drive".to_string()],
                from_cwd: false,
                auto_include_archived: false,
            },
        }
    }

    #[test]
    fn test_affinity_requires_where() {
        let conn = open_in_memory_for_test();

        let root = insert_root(&conn, "/mnt/drive", "source", false);
        let obj1 = insert_object(&conn, "hash_001", false);
        insert_source(&conn, root, "a.jpg", Some(obj1));

        let options = SurveyOptions {
            affinity: true,
            ..test_options()
        };
        let paths = vec![PathBuf::from("/mnt/drive")];
        let filter_strs: Vec<String> = vec![];

        let mut db = repo::Db::from_connection(conn);
        let result = run(&mut db, &paths, &filter_strs, &options);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("--affinity"));
        assert!(err.contains("--where"));
    }

    #[test]
    fn test_residual_requires_other() {
        let conn = open_in_memory_for_test();

        let root = insert_root(&conn, "/mnt/drive", "source", false);
        let obj1 = insert_object(&conn, "hash_001", false);
        insert_source(&conn, root, "a.jpg", Some(obj1));

        let options = SurveyOptions {
            detail: Some(DetailMode::Residual),
            ..test_options()
        };
        let paths = vec![PathBuf::from("/mnt/drive")];
        let filter_strs: Vec<String> = vec![];

        let mut db = repo::Db::from_connection(conn);
        let result = run(&mut db, &paths, &filter_strs, &options);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("--detail residual"));
        assert!(err.contains("--other"));
    }
}
