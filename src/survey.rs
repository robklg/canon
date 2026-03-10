use anyhow::{bail, Result};
use rusqlite::Connection;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use crate::ceremony::format_count;
use crate::domain;
use crate::domain::root::parse_root_spec;
use crate::domain::scope::ScopeMatch;
use crate::domain::source::Source;
use crate::domain::IncludeSet;
use crate::expr::filter::{self, Filter};
use crate::repo;

const SUPERSET_THRESHOLD: f64 = 0.8;
const COMPLEMENT_SAMPLE_SIZE: usize = 5;
const DEFAULT_LOCATION_CAP: usize = 10;

/// Detail output mode for `--detail`.
#[derive(Clone, Copy, PartialEq, clap::ValueEnum)]
pub enum DetailMode {
    /// Show complementary content at related locations
    Complement,
    /// Output bare paths of unique-to-selection content
    Unique,
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
    /// Null-delimited output for --detail unique.
    pub null_delim: bool,
    /// Filter archive section to a specific archive root.
    pub archive: Option<String>,
    /// Show all paths per location (complement view) / all locations (summary).
    pub verbose: bool,
}

struct SurveyResult {
    scope_prefixes: Vec<String>,
    total_count: usize,
    unhashed_count: usize,
    total_hashed: usize,
    archived_source_count: usize,
    archive_scopes: Vec<(String, usize)>,
    location_results: Vec<LocationResult>,
    unique_count: usize,
    unique_paths: Vec<String>,
    is_other_mode: bool,
    /// Display label when --archive is specified (e.g., "in /archive/photos").
    archive_label: Option<String>,
}

struct LocationResult {
    path: String,
    shared_count: usize,
    /// Total hashed sources at this location (always computed).
    total_count: usize,
    complementary_count: Option<usize>,
    only_here_count: Option<usize>,
    kind: Option<domain::survey::LocationKind>,
    /// Complementary source paths relative to location, sorted.
    /// None when affinity not computed; Some(vec![]) when computed but empty.
    complementary_paths: Option<Vec<String>>,
    /// Selection source paths that overlap with this location (Phase 2).
    overlap_paths: Option<Vec<String>>,
    /// Selection source paths NOT shared with this location (Phase 3).
    residual_paths: Option<Vec<String>>,
}

/// Outcome of compute_survey: either a result to display or an early exit.
enum SurveyOutcome {
    /// Normal result with all computed data.
    Result(SurveyResult),
    /// Empty selection — display header and stop.
    Empty { scope_prefixes: Vec<String> },
    /// All unhashed — display header and hashing guidance.
    AllUnhashed {
        scope_prefixes: Vec<String>,
        total_count: usize,
    },
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

    // Resolve --other paths (same soft resolution as scope paths)
    let other_resolved = if !options.other_paths.is_empty() {
        domain::path::resolve_paths(&options.other_paths, &all_roots)?
    } else {
        Vec::new()
    };

    let root_ids: Vec<i64> = all_roots.iter().map(|r| r.id).collect();
    let all_sources = repo::source::batch_fetch_by_roots(conn, &root_ids)?;

    match compute_survey(
        conn,
        paths,
        &filters,
        options,
        &all_sources,
        &all_roots,
        &other_resolved,
        archive_root_id,
    )? {
        SurveyOutcome::Empty { scope_prefixes } => {
            if options.detail != Some(DetailMode::Unique) {
                print_survey_header(&scope_prefixes, &options.original_filters, 0, 0, 0, None);
            }
        }
        SurveyOutcome::AllUnhashed {
            scope_prefixes,
            total_count,
        } => {
            if options.detail != Some(DetailMode::Unique) {
                print_survey_header(
                    &scope_prefixes,
                    &options.original_filters,
                    total_count,
                    total_count,
                    0,
                    None,
                );
                println!();
                println!(
                    "No hashed sources in selection. Content comparison requires hashing."
                );
                println!("Run `canon scan` to hash these sources.");
            }
        }
        SurveyOutcome::Result(mut result) => {
            result.archive_label = archive_label;
            match options.detail {
                Some(DetailMode::Complement) => {
                    print_survey_header(
                        &result.scope_prefixes,
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
                Some(DetailMode::Unique) => {
                    print_unique_detail(&result.unique_paths, options.null_delim);
                }
                None => {
                    print_survey_header(
                        &result.scope_prefixes,
                        &options.original_filters,
                        result.total_count,
                        result.unhashed_count,
                        result.total_hashed,
                        Some(result.unique_count),
                    );
                    println!();
                    print_archive_section(
                        result.archived_source_count,
                        result.total_hashed,
                        &result.archive_scopes,
                        result.archive_label.as_deref(),
                    );
                    println!();
                    print_related_locations(
                        &result.location_results,
                        result.total_hashed,
                        result.is_other_mode,
                        options.verbose,
                    );
                }
            }
        }
    }

    Ok(())
}

fn compute_survey(
    conn: &mut Connection,
    paths: &[PathBuf],
    filters: &[Filter],
    options: &SurveyOptions,
    all_sources: &[Source],
    all_roots: &[domain::Root],
    other_paths: &[String],
    archive_root_id: Option<i64>,
) -> Result<SurveyOutcome> {
    // Default to cwd
    let scope_paths = if paths.is_empty() {
        vec![std::env::current_dir()?]
    } else {
        paths.to_vec()
    };

    // Resolve scope paths (soft resolution — offline-capable)
    let scope_prefixes = domain::path::resolve_paths(&scope_paths, all_roots)?;
    let scopes = ScopeMatch::classify_all(&scope_prefixes);

    // Build selection from domain predicates
    let selection: Vec<&Source> = all_sources
        .iter()
        .filter(|s| s.is_active())
        .filter(|s| s.is_from_role("source"))
        .filter(|s| s.matches_scope(&scopes))
        .filter(|s| options.include.includes_excluded() || !s.is_excluded())
        .collect();

    // Apply --where filters to selection
    let selection = if filters.is_empty() {
        selection
    } else {
        let ids: Vec<i64> = selection.iter().map(|s| s.id).collect();
        let passed: HashSet<i64> = filter::apply_filters(conn, &ids, filters)?
            .into_iter()
            .collect();
        selection
            .into_iter()
            .filter(|s| passed.contains(&s.id))
            .collect()
    };

    // Partition: unhashed vs hashed
    let total_count = selection.len();
    let hashed: Vec<&Source> = selection
        .iter()
        .filter(|s| s.object_id.is_some())
        .copied()
        .collect();
    let unhashed_count = total_count - hashed.len();
    let total_hashed = hashed.len();

    // Early exit: empty selection
    if total_count == 0 {
        return Ok(SurveyOutcome::Empty { scope_prefixes });
    }

    // Early exit: all unhashed
    if total_hashed == 0 {
        return Ok(SurveyOutcome::AllUnhashed {
            scope_prefixes,
            total_count,
        });
    }

    // Collect selection identity
    let sel_object_ids: HashSet<i64> = hashed.iter().filter_map(|s| s.object_id).collect();
    let sel_source_ids: HashSet<i64> = selection.iter().map(|s| s.id).collect();

    // Build object index from ALL active, non-excluded, hashed sources
    let mut by_object_id: HashMap<i64, Vec<&Source>> = HashMap::new();
    for s in all_sources {
        if s.is_active() && !s.is_excluded() {
            if let Some(oid) = s.object_id {
                by_object_id.entry(oid).or_default().push(s);
            }
        }
    }

    // Archive status: find selection content that exists on archive roots
    let mut archive_sources: Vec<&Source> = Vec::new();
    let mut archived_object_ids: HashSet<i64> = HashSet::new();

    for &oid in &sel_object_ids {
        if let Some(siblings) = by_object_id.get(&oid) {
            let mut found_archive = false;
            for sib in siblings {
                if sib.is_from_role("archive") {
                    // When --archive is specified, only count that archive
                    if let Some(target_id) = archive_root_id {
                        if sib.root_id != target_id {
                            continue;
                        }
                    }
                    if !found_archive {
                        archived_object_ids.insert(oid);
                        found_archive = true;
                    }
                    archive_sources.push(sib);
                }
            }
        }
    }

    // Count selection sources that are archived (source-based counting)
    let archived_source_count = hashed
        .iter()
        .filter(|s| archived_object_ids.contains(&s.object_id.unwrap()))
        .count();

    // Scope discovery on archive sources for grouped display
    let mut archive_scopes = domain::survey::discover_scopes_by_root(&archive_sources);
    archive_scopes.sort_by(|a, b| a.0.cmp(&b.0));

    // Overlap: find selection content that exists on other source roots
    let mut overlap_sources: Vec<&Source> = Vec::new();
    for &oid in &sel_object_ids {
        if let Some(siblings) = by_object_id.get(&oid) {
            for sib in siblings {
                if !sel_source_ids.contains(&sib.id) && sib.is_from_role("source") {
                    overlap_sources.push(sib);
                }
            }
        }
    }

    // Location discovery
    let is_other_mode = !other_paths.is_empty();
    let location_scopes: Vec<(String, usize)> = if is_other_mode {
        // --other: user paths directly
        other_paths.iter().map(|p| (p.clone(), 0)).collect()
    } else {
        // Default: scope discovery from overlap sources
        domain::survey::discover_scopes_by_root(&overlap_sources)
    };

    // Per-location: shared count + affinity
    let compute_affinity =
        (options.affinity || options.detail == Some(DetailMode::Complement)) && !options.brief;
    let mut location_results: Vec<LocationResult> = Vec::new();

    for (scope_path, _overlap_count) in &location_scopes {
        let loc_scope = vec![ScopeMatch::UnderDirectory(scope_path.clone())];

        // Object IDs at this location — used for shared_count and detail views.
        // --other mode: all roles. Default mode: source role only.
        let loc_oids: HashSet<i64> = if is_other_mode {
            all_sources
                .iter()
                .filter(|s| s.is_active())
                .filter(|s| !s.is_excluded())
                .filter(|s| s.matches_scope(&loc_scope))
                .filter(|s| !sel_source_ids.contains(&s.id))
                .filter_map(|s| s.object_id)
                .collect()
        } else {
            overlap_sources
                .iter()
                .filter(|s| s.matches_scope(&loc_scope))
                .filter_map(|s| s.object_id)
                .collect()
        };

        // Shared count: how many selection sources have content at this location
        let shared_count = hashed
            .iter()
            .filter(|s| loc_oids.contains(&s.object_id.unwrap()))
            .count();

        // Total hashed sources at location (not excluding selection sources).
        // Same visibility rules as shared_count: source-role only in default mode,
        // all roles in --other mode.
        let total_count: usize = all_sources
            .iter()
            .filter(|s| s.is_active())
            .filter(|s| !s.is_excluded())
            .filter(|s| s.object_id.is_some())
            .filter(|s| s.matches_scope(&loc_scope))
            .filter(|s| is_other_mode || s.is_from_role("source"))
            .count();

        // Complementary content and classification (only with --affinity or --detail complement)
        let (complementary_count, only_here_count, kind, complementary_paths) = if compute_affinity {
            // Step 1: Get ALL sources within this location
            // Active, non-excluded, not in selection
            let loc_sources: Vec<&Source> = all_sources
                .iter()
                .filter(|s| s.is_active())
                .filter(|s| !s.is_excluded())
                .filter(|s| s.matches_scope(&loc_scope))
                .filter(|s| !sel_source_ids.contains(&s.id))
                .collect();

            // Step 2: Apply --where filters to location sources
            let loc_ids: Vec<i64> = loc_sources.iter().map(|s| s.id).collect();
            let passed: HashSet<i64> = filter::apply_filters(conn, &loc_ids, filters)?
                .into_iter()
                .collect();

            // Step 3: Partition into overlap vs complementary
            // CRITICAL: filter to hashed-only BEFORE partitioning.
            // Unhashed sources can't participate in content comparison.
            // Without this guard, unhashed sources leak into complementary
            // count (their object_id is None, which is never in sel_object_ids,
            // so they'd always be classified as "complementary").
            let matching_hashed: Vec<&Source> = loc_sources
                .iter()
                .filter(|s| passed.contains(&s.id))
                .filter(|s| s.object_id.is_some())
                .copied()
                .collect();

            let complementary: Vec<&Source> = matching_hashed
                .iter()
                .filter(|s| !sel_object_ids.contains(&s.object_id.unwrap()))
                .copied()
                .collect();

            let comp_count = complementary.len();

            // Step 4: Collect complementary paths relative to location
            let mut comp_paths: Vec<String> = complementary
                .iter()
                .filter_map(|s| {
                    domain::path::path_strip_prefix(&s.path(), scope_path)
                        .map(|p| p.to_string())
                })
                .collect();
            comp_paths.sort_unstable();

            // Step 5: "Only here" — unique object_ids among complementary
            let comp_oids: HashSet<i64> =
                complementary.iter().filter_map(|s| s.object_id).collect();
            let only_here =
                domain::survey::count_only_here(&comp_oids, scope_path, &by_object_id);

            // Step 6: Classify
            let kind = domain::survey::classify_location(
                shared_count,
                total_hashed,
                comp_count,
                SUPERSET_THRESHOLD,
                total_count,
            );

            (Some(comp_count), Some(only_here), Some(kind), Some(comp_paths))
        } else {
            (None, None, None, None)
        };

        location_results.push(LocationResult {
            path: scope_path.clone(),
            shared_count,
            total_count,
            complementary_count,
            only_here_count,
            kind,
            complementary_paths,
            overlap_paths: None,  // populated in Phase 2
            residual_paths: None, // populated in Phase 3
        });
    }

    // Sort locations
    if is_other_mode {
        // --other: preserve user-specified order (no sort)
    } else if compute_affinity {
        // Classification: supersets first, then leads, then mirrors
        // Within each group: complementary desc, then shared desc
        location_results.sort_by(|a, b| {
            let kind_a = a.kind.as_ref().unwrap();
            let kind_b = b.kind.as_ref().unwrap();
            kind_a
                .cmp(kind_b)
                .then(b.complementary_count.cmp(&a.complementary_count))
                .then(b.shared_count.cmp(&a.shared_count))
        });
    } else {
        // No affinity data: sort by shared count descending
        location_results.sort_by(|a, b| b.shared_count.cmp(&a.shared_count));
    }

    // Unique: selection content that exists nowhere else
    let unique_oids = domain::survey::find_unique_object_ids(
        &sel_object_ids,
        &sel_source_ids,
        &by_object_id,
    );
    let unique_count = unique_oids.len();
    let mut unique_paths: Vec<String> = hashed
        .iter()
        .filter(|s| unique_oids.contains(&s.object_id.unwrap()))
        .map(|s| s.path())
        .collect();
    unique_paths.sort_unstable();

    Ok(SurveyOutcome::Result(SurveyResult {
        scope_prefixes,
        total_count,
        unhashed_count,
        total_hashed,
        archived_source_count,
        archive_scopes,
        location_results,
        unique_count,
        unique_paths,
        is_other_mode,
        archive_label: None, // set by run() after return
    }))
}

// =============================================================================
// Output formatting
// =============================================================================

fn print_survey_header(
    scope_prefixes: &[String],
    original_filters: &[String],
    total: usize,
    unhashed: usize,
    hashed: usize,
    unique_count: Option<usize>,
) {
    if scope_prefixes.len() == 1 {
        println!("Survey: {}", scope_prefixes[0]);
    } else {
        println!("Survey:");
        for p in scope_prefixes {
            println!("  {p}");
        }
    }

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

    // Scope-grouped archive paths with right-aligned counts
    let max_path_len = archive_scopes
        .iter()
        .map(|(p, _)| p.len())
        .max()
        .unwrap_or(0);
    let max_count_len = archive_scopes
        .iter()
        .map(|(_, c)| format_count(*c).len())
        .max()
        .unwrap_or(0);

    for (path, count) in archive_scopes {
        println!(
            "  {:path_w$}  {:>count_w$}",
            path,
            format_count(*count),
            path_w = max_path_len,
            count_w = max_count_len,
        );
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

        // Paths (capped unless --verbose)
        let show_count = if verbose {
            paths.len()
        } else {
            COMPLEMENT_SAMPLE_SIZE.min(paths.len())
        };
        for path in &paths[..show_count] {
            println!("    {}", path);
        }
        if !verbose && paths.len() > COMPLEMENT_SAMPLE_SIZE {
            println!(
                "    ... and {} more",
                format_count(paths.len() - COMPLEMENT_SAMPLE_SIZE),
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

fn print_unique_detail(paths: &[String], null_delim: bool) {
    let sep = if null_delim { "\0" } else { "\n" };
    for path in paths {
        print!("{}{}", path, sep);
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::open_in_memory_for_test;

    fn insert_root(conn: &Connection, path: &str, role: &str) -> i64 {
        conn.execute(
            "INSERT INTO roots (path, role, suspended) VALUES (?, ?, 0)",
            rusqlite::params![path, role],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn insert_root_suspended(conn: &Connection, path: &str, role: &str) -> i64 {
        conn.execute(
            "INSERT INTO roots (path, role, suspended) VALUES (?, ?, 1)",
            rusqlite::params![path, role],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn insert_object(conn: &Connection, hash: &str) -> i64 {
        conn.execute(
            "INSERT INTO objects (hash_type, hash_value, excluded) VALUES ('sha256', ?, 0)",
            rusqlite::params![hash],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn insert_source(
        conn: &Connection,
        root_id: i64,
        rel_path: &str,
        object_id: Option<i64>,
    ) -> i64 {
        // Use a static counter via last_insert_rowid + 1000 for unique inode
        let inode = conn.last_insert_rowid() + 1000;
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, object_id, size, mtime, \
             partial_hash, scanned_at, last_seen_at, device, inode) \
             VALUES (?, ?, ?, 1000, 1704067200, 'ph', 0, 0, 1, ?)",
            rusqlite::params![root_id, rel_path, object_id, inode],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn insert_source_excluded(
        conn: &Connection,
        root_id: i64,
        rel_path: &str,
        object_id: Option<i64>,
    ) -> i64 {
        let inode = conn.last_insert_rowid() + 1000;
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, object_id, size, mtime, \
             partial_hash, scanned_at, last_seen_at, device, inode, excluded) \
             VALUES (?, ?, ?, 1000, 1704067200, 'ph', 0, 0, 1, ?, 1)",
            rusqlite::params![root_id, rel_path, object_id, inode],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    /// Helper to run compute_survey with test data.
    fn run_compute(
        conn: &mut Connection,
        scope_paths: &[&str],
        options: &SurveyOptions,
        filters: &[Filter],
        archive_root_id: Option<i64>,
    ) -> SurveyOutcome {
        let all_roots = repo::root::fetch_all(conn).unwrap();
        let root_ids: Vec<i64> = all_roots.iter().map(|r| r.id).collect();
        let all_sources = repo::source::batch_fetch_by_roots(conn, &root_ids).unwrap();

        let paths: Vec<PathBuf> = scope_paths.iter().map(|p| PathBuf::from(p)).collect();
        let other: Vec<String> = options.other_paths.iter().map(|p| p.to_string_lossy().to_string()).collect();
        compute_survey(conn, &paths, filters, options, &all_sources, &all_roots, &other, archive_root_id)
            .unwrap()
    }

    /// Build a SurveyOptions for tests with common defaults.
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
        }
    }

    // =========================================================================
    // Basic summary end-to-end
    // =========================================================================

    #[test]
    fn test_basic_summary() {
        let mut conn = open_in_memory_for_test();

        // Source root A (/mnt/drive-a)
        let root_a = insert_root(&conn, "/mnt/drive-a", "source");
        // Source root B (/mnt/backup)
        let root_b = insert_root(&conn, "/mnt/backup", "source");
        // Archive root
        let archive = insert_root(&conn, "/archive/photos", "archive");

        // Objects
        let obj1 = insert_object(&conn, "hash_001"); // on A, B, and archive
        let obj2 = insert_object(&conn, "hash_002"); // on A and B
        let obj3 = insert_object(&conn, "hash_003"); // on A and archive
        let obj4 = insert_object(&conn, "hash_004"); // unique to A

        // Source root A: 5 sources (1 unhashed)
        insert_source(&conn, root_a, "photos/IMG_001.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/IMG_002.jpg", Some(obj2));
        insert_source(&conn, root_a, "photos/IMG_003.jpg", Some(obj3));
        insert_source(&conn, root_a, "photos/IMG_004.jpg", Some(obj4));
        insert_source(&conn, root_a, "photos/IMG_005.jpg", None);

        // Source root B: overlap
        insert_source(&conn, root_b, "vacation/IMG_001.jpg", Some(obj1));
        insert_source(&conn, root_b, "vacation/IMG_002.jpg", Some(obj2));

        // Archive
        insert_source(&conn, archive, "2024/IMG_001.jpg", Some(obj1));
        insert_source(&conn, archive, "2024/IMG_003.jpg", Some(obj3));

        let options = test_options();
        let outcome = run_compute(&mut conn, &["/mnt/drive-a"], &options, &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.total_count, 5);
                assert_eq!(result.unhashed_count, 1);
                assert_eq!(result.total_hashed, 4);
                assert_eq!(result.archived_source_count, 2); // obj1 and obj3
                assert_eq!(result.archive_scopes.len(), 1);
                assert_eq!(result.archive_scopes[0].0, "/archive/photos/2024");
                assert_eq!(result.archive_scopes[0].1, 2);
                assert_eq!(result.location_results.len(), 1);
                assert_eq!(result.location_results[0].path, "/mnt/backup/vacation");
                assert_eq!(result.location_results[0].shared_count, 2); // obj1, obj2
                assert_eq!(result.unique_count, 1); // obj4
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // Empty selection
    // =========================================================================

    #[test]
    fn test_empty_selection() {
        let mut conn = open_in_memory_for_test();
        let root = insert_root(&conn, "/mnt/drive", "source");
        // Sources exist, but not under the scoped subdirectory
        let obj = insert_object(&conn, "hash_001");
        insert_source(&conn, root, "photos/a.jpg", Some(obj));

        let options = test_options();
        let outcome = run_compute(&mut conn, &["/mnt/drive/other"], &options, &[], None);

        match outcome {
            SurveyOutcome::Empty { scope_prefixes } => {
                assert_eq!(scope_prefixes, vec!["/mnt/drive/other"]);
            }
            _ => panic!("Expected SurveyOutcome::Empty"),
        }
    }

    // =========================================================================
    // All unhashed
    // =========================================================================

    #[test]
    fn test_all_unhashed() {
        let mut conn = open_in_memory_for_test();
        let root = insert_root(&conn, "/mnt/drive", "source");
        insert_source(&conn, root, "a.jpg", None);
        insert_source(&conn, root, "b.jpg", None);

        let options = test_options();
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &options, &[], None);

        match outcome {
            SurveyOutcome::AllUnhashed {
                scope_prefixes,
                total_count,
            } => {
                assert_eq!(scope_prefixes, vec!["/mnt/drive"]);
                assert_eq!(total_count, 2);
            }
            _ => panic!("Expected SurveyOutcome::AllUnhashed"),
        }
    }

    // =========================================================================
    // No related locations
    // =========================================================================

    #[test]
    fn test_no_related_locations() {
        let mut conn = open_in_memory_for_test();
        let root = insert_root(&conn, "/mnt/drive", "source");
        let archive = insert_root(&conn, "/archive", "archive");

        let obj1 = insert_object(&conn, "hash_001");
        let obj2 = insert_object(&conn, "hash_002");

        insert_source(&conn, root, "a.jpg", Some(obj1));
        insert_source(&conn, root, "b.jpg", Some(obj2));

        // Archive copies but no overlap on source roots
        insert_source(&conn, archive, "a.jpg", Some(obj1));

        let options = test_options();
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &options, &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert!(result.location_results.is_empty());
                assert_eq!(result.unique_count, 1); // obj2 — not in archive
                assert_eq!(result.archived_source_count, 1); // obj1
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // No archived sources
    // =========================================================================

    #[test]
    fn test_no_archived() {
        let mut conn = open_in_memory_for_test();
        let root = insert_root(&conn, "/mnt/drive", "source");

        let obj1 = insert_object(&conn, "hash_001");
        insert_source(&conn, root, "a.jpg", Some(obj1));

        let options = test_options();
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &options, &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.archived_source_count, 0);
                assert!(result.archive_scopes.is_empty());
                assert_eq!(result.unique_count, 1);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // Multiple scope paths
    // =========================================================================

    #[test]
    fn test_multiple_scope_paths() {
        let mut conn = open_in_memory_for_test();
        let root_a = insert_root(&conn, "/mnt/drive-a", "source");
        let root_b = insert_root(&conn, "/mnt/drive-b", "source");

        let obj1 = insert_object(&conn, "hash_001");
        let obj2 = insert_object(&conn, "hash_002");

        insert_source(&conn, root_a, "a.jpg", Some(obj1));
        insert_source(&conn, root_b, "b.jpg", Some(obj2));

        let options = test_options();
        let outcome = run_compute(&mut conn, &["/mnt/drive-a", "/mnt/drive-b"], &options, &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.scope_prefixes.len(), 2);
                assert_eq!(result.total_count, 2); // union of both scopes
                assert_eq!(result.total_hashed, 2);
                assert_eq!(result.unique_count, 2); // both unique
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // Suspended root excluded
    // =========================================================================

    #[test]
    fn test_suspended_root_excluded() {
        let mut conn = open_in_memory_for_test();
        let root = insert_root(&conn, "/mnt/drive", "source");
        let suspended = insert_root_suspended(&conn, "/mnt/suspended", "source");

        let obj1 = insert_object(&conn, "hash_001");
        let obj2 = insert_object(&conn, "hash_002");

        insert_source(&conn, root, "a.jpg", Some(obj1));
        // This source is on a suspended root — should not appear anywhere
        insert_source(&conn, suspended, "b.jpg", Some(obj1));
        insert_source(&conn, suspended, "c.jpg", Some(obj2));

        let options = test_options();
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &options, &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.total_count, 1);
                // No overlap — suspended root's sources invisible
                assert!(result.location_results.is_empty());
                // Unique: obj1 has a copy on suspended root, but suspended sources
                // are not in the object index, so obj1 is unique to selection
                assert_eq!(result.unique_count, 1);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // Excluded sources hidden by default
    // =========================================================================

    #[test]
    fn test_excluded_sources_hidden() {
        let mut conn = open_in_memory_for_test();
        let root = insert_root(&conn, "/mnt/drive", "source");
        let other = insert_root(&conn, "/mnt/other", "source");

        let obj1 = insert_object(&conn, "hash_001");
        let obj2 = insert_object(&conn, "hash_002");

        insert_source(&conn, root, "a.jpg", Some(obj1));
        insert_source_excluded(&conn, root, "excluded.jpg", Some(obj2));

        // Overlap source also excluded
        insert_source_excluded(&conn, other, "b.jpg", Some(obj1));

        // Default: excluded hidden
        let options = test_options();
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &options, &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.total_count, 1); // excluded.jpg hidden
                assert_eq!(result.total_hashed, 1);
                // Excluded overlap source is not in object index, so no overlap
                assert!(result.location_results.is_empty());
                assert_eq!(result.unique_count, 1);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }

        // With --include excluded: excluded appears in selection
        let options = SurveyOptions {
            include: IncludeSet { excluded: true, archived: false },
            ..test_options()
        };
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &options, &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.total_count, 2); // excluded.jpg now in selection
                assert_eq!(result.total_hashed, 2);
                // Object index still excludes excluded sources (outward side
                // never shows excluded), so no overlap and both unique
                assert!(result.location_results.is_empty());
                assert_eq!(result.unique_count, 2);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // Archive scope grouping
    // =========================================================================

    #[test]
    fn test_archive_scope_grouping() {
        let mut conn = open_in_memory_for_test();
        let root = insert_root(&conn, "/mnt/drive", "source");
        let archive1 = insert_root(&conn, "/archive/a", "archive");
        let archive2 = insert_root(&conn, "/archive/b", "archive");

        let obj1 = insert_object(&conn, "hash_001");
        let obj2 = insert_object(&conn, "hash_002");
        let obj3 = insert_object(&conn, "hash_003");

        insert_source(&conn, root, "x.jpg", Some(obj1));
        insert_source(&conn, root, "y.jpg", Some(obj2));
        insert_source(&conn, root, "z.jpg", Some(obj3));

        // Archive sources across two roots, multiple directories
        insert_source(&conn, archive1, "2024/x.jpg", Some(obj1));
        insert_source(&conn, archive1, "2024/y.jpg", Some(obj2));
        insert_source(&conn, archive2, "backup/z.jpg", Some(obj3));

        let options = test_options();
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &options, &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.archived_source_count, 3);
                assert_eq!(result.archive_scopes.len(), 2);
                // Sorted alphabetically
                assert_eq!(result.archive_scopes[0].0, "/archive/a/2024");
                assert_eq!(result.archive_scopes[0].1, 2);
                assert_eq!(result.archive_scopes[1].0, "/archive/b/backup");
                assert_eq!(result.archive_scopes[1].1, 1);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // Same root, different scope — overlap from own root
    // =========================================================================

    #[test]
    fn test_same_root_different_scope() {
        let mut conn = open_in_memory_for_test();
        let root = insert_root(&conn, "/mnt/drive", "source");

        let obj1 = insert_object(&conn, "hash_001");
        let obj2 = insert_object(&conn, "hash_002");

        // Selection: scoped to /mnt/drive/photos
        insert_source(&conn, root, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root, "photos/b.jpg", Some(obj2));

        // Same root, but outside scope — overlap
        insert_source(&conn, root, "documents/a_copy.jpg", Some(obj1));

        let options = test_options();
        let outcome = run_compute(&mut conn, &["/mnt/drive/photos"], &options, &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.total_count, 2);
                assert_eq!(result.location_results.len(), 1);
                assert_eq!(result.location_results[0].path, "/mnt/drive/documents");
                assert_eq!(result.location_results[0].shared_count, 1); // obj1
                assert_eq!(result.unique_count, 1); // obj2
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // Affinity and classification
    // =========================================================================

    // =========================================================================
    // Basic affinity correctness
    // =========================================================================

    #[test]
    fn test_affinity_basic() {
        let mut conn = open_in_memory_for_test();

        // Source root A (selection)
        let root_a = insert_root(&conn, "/mnt/drive-a", "source");
        // Source root B (related)
        let root_b = insert_root(&conn, "/mnt/backup", "source");

        let obj1 = insert_object(&conn, "hash_001"); // overlap
        let obj2 = insert_object(&conn, "hash_002"); // overlap
        let obj3 = insert_object(&conn, "hash_003"); // unique to selection
        let obj4 = insert_object(&conn, "hash_004"); // complementary at B
        let obj5 = insert_object(&conn, "hash_005"); // complementary at B
        let obj6 = insert_object(&conn, "hash_006"); // at B but .txt — won't match filter

        // Root A: 3 .jpg sources
        insert_source(&conn, root_a, "photos/IMG_001.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/IMG_002.jpg", Some(obj2));
        insert_source(&conn, root_a, "photos/IMG_003.jpg", Some(obj3));

        // Root B: overlap + complementary + non-matching
        insert_source(&conn, root_b, "trip/IMG_001.jpg", Some(obj1));
        insert_source(&conn, root_b, "trip/IMG_002.jpg", Some(obj2));
        insert_source(&conn, root_b, "trip/IMG_004.jpg", Some(obj4));
        insert_source(&conn, root_b, "trip/IMG_005.jpg", Some(obj5));
        insert_source(&conn, root_b, "trip/notes.txt", Some(obj6));

        let options = SurveyOptions { affinity: true, ..test_options() };
        let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
        let outcome = run_compute(&mut conn, &["/mnt/drive-a"], &options, &filters, None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.total_count, 3);
                assert_eq!(result.total_hashed, 3);
                assert_eq!(result.location_results.len(), 1);
                let loc = &result.location_results[0];
                assert_eq!(loc.shared_count, 2);
                assert_eq!(loc.complementary_count, Some(2)); // obj4, obj5
                assert_eq!(loc.only_here_count, Some(2)); // both only at B
                assert_eq!(loc.kind, Some(domain::survey::LocationKind::Lead));
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // "Only here" with content elsewhere
    // =========================================================================

    #[test]
    fn test_affinity_only_here_reduced() {
        let mut conn = open_in_memory_for_test();

        let root_a = insert_root(&conn, "/mnt/drive-a", "source");
        let root_b = insert_root(&conn, "/mnt/backup", "source");
        let root_c = insert_root(&conn, "/mnt/other", "source");

        let obj1 = insert_object(&conn, "hash_001"); // overlap
        let obj2 = insert_object(&conn, "hash_002"); // overlap
        let obj3 = insert_object(&conn, "hash_003"); // unique to selection
        let obj4 = insert_object(&conn, "hash_004"); // complementary, also on C
        let obj5 = insert_object(&conn, "hash_005"); // complementary, only at B

        insert_source(&conn, root_a, "photos/IMG_001.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/IMG_002.jpg", Some(obj2));
        insert_source(&conn, root_a, "photos/IMG_003.jpg", Some(obj3));

        insert_source(&conn, root_b, "trip/IMG_001.jpg", Some(obj1));
        insert_source(&conn, root_b, "trip/IMG_002.jpg", Some(obj2));
        insert_source(&conn, root_b, "trip/IMG_004.jpg", Some(obj4));
        insert_source(&conn, root_b, "trip/IMG_005.jpg", Some(obj5));

        // Root C has a copy of obj4 — makes obj4 NOT "only here" at B
        insert_source(&conn, root_c, "misc/copy.jpg", Some(obj4));

        let options = SurveyOptions { affinity: true, ..test_options() };
        let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
        let outcome = run_compute(&mut conn, &["/mnt/drive-a"], &options, &filters, None);

        match outcome {
            SurveyOutcome::Result(result) => {
                // Find the B location
                let loc_b = result
                    .location_results
                    .iter()
                    .find(|l| l.path.contains("backup"))
                    .expect("Should find backup location");
                assert_eq!(loc_b.complementary_count, Some(2)); // obj4 + obj5
                assert_eq!(loc_b.only_here_count, Some(1)); // only obj5 (obj4 is on C too)
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // Unhashed sources excluded from complementary count
    // =========================================================================

    #[test]
    fn test_affinity_unhashed_excluded() {
        let mut conn = open_in_memory_for_test();

        let root_a = insert_root(&conn, "/mnt/drive-a", "source");
        let root_b = insert_root(&conn, "/mnt/backup", "source");

        let obj1 = insert_object(&conn, "hash_001");
        let obj2 = insert_object(&conn, "hash_002");
        let obj3 = insert_object(&conn, "hash_003");
        let obj4 = insert_object(&conn, "hash_004");
        let obj5 = insert_object(&conn, "hash_005");

        insert_source(&conn, root_a, "photos/IMG_001.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/IMG_002.jpg", Some(obj2));
        insert_source(&conn, root_a, "photos/IMG_003.jpg", Some(obj3));

        insert_source(&conn, root_b, "trip/IMG_001.jpg", Some(obj1));
        insert_source(&conn, root_b, "trip/IMG_002.jpg", Some(obj2));
        insert_source(&conn, root_b, "trip/IMG_004.jpg", Some(obj4));
        insert_source(&conn, root_b, "trip/IMG_005.jpg", Some(obj5));
        // Unhashed source at B matching the filter extension
        insert_source(&conn, root_b, "trip/IMG_006.jpg", None);

        let options = SurveyOptions { affinity: true, ..test_options() };
        let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
        let outcome = run_compute(&mut conn, &["/mnt/drive-a"], &options, &filters, None);

        match outcome {
            SurveyOutcome::Result(result) => {
                let loc = &result.location_results[0];
                // CRITICAL: complementary must be 2 (obj4, obj5), NOT 3
                // The unhashed source passes the filter but must be excluded
                assert_eq!(loc.complementary_count, Some(2));
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // No filters means no affinity data
    // =========================================================================

    #[test]
    fn test_no_filters_no_affinity() {
        let mut conn = open_in_memory_for_test();

        let root_a = insert_root(&conn, "/mnt/drive-a", "source");
        let root_b = insert_root(&conn, "/mnt/backup", "source");

        let obj1 = insert_object(&conn, "hash_001");
        let obj2 = insert_object(&conn, "hash_002");
        let obj3 = insert_object(&conn, "hash_003");
        let obj4 = insert_object(&conn, "hash_004");

        insert_source(&conn, root_a, "photos/IMG_001.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/IMG_002.jpg", Some(obj2));
        insert_source(&conn, root_a, "photos/IMG_003.jpg", Some(obj3));

        insert_source(&conn, root_b, "trip/IMG_001.jpg", Some(obj1));
        insert_source(&conn, root_b, "trip/IMG_002.jpg", Some(obj2));
        insert_source(&conn, root_b, "trip/IMG_004.jpg", Some(obj4));

        // No filters, no --affinity → orientation mode (no affinity data)
        let options = test_options();
        let outcome = run_compute(&mut conn, &["/mnt/drive-a"], &options, &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.location_results.len(), 1);
                let loc = &result.location_results[0];
                assert_eq!(loc.complementary_count, None);
                assert_eq!(loc.only_here_count, None);
                assert_eq!(loc.kind, None);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // Classification sort order
    // =========================================================================

    #[test]
    fn test_classification_sort() {
        let mut conn = open_in_memory_for_test();

        let root_a = insert_root(&conn, "/mnt/drive-a", "source");
        let root_b = insert_root(&conn, "/mnt/backup-main", "source"); // Superset
        let root_c = insert_root(&conn, "/mnt/partner", "source"); // Lead
        let root_d = insert_root(&conn, "/mnt/old-copy", "source"); // Mirror

        // Selection: 10 objects
        let mut sel_objs = Vec::new();
        for i in 1..=10 {
            let obj = insert_object(&conn, &format!("hash_{:03}", i));
            insert_source(
                &conn,
                root_a,
                &format!("photos/IMG_{:03}.jpg", i),
                Some(obj),
            );
            sel_objs.push(obj);
        }

        // Root B (Superset): 9 overlap + 5 complementary
        for i in 1..=9 {
            insert_source(
                &conn,
                root_b,
                &format!("backup/IMG_{:03}.jpg", i),
                Some(sel_objs[i - 1]),
            );
        }
        for i in 11..=15 {
            let obj = insert_object(&conn, &format!("hash_{:03}", i));
            insert_source(
                &conn,
                root_b,
                &format!("backup/EXTRA_{:03}.jpg", i),
                Some(obj),
            );
        }

        // Root C (Lead): 2 overlap + 20 complementary
        insert_source(&conn, root_c, "photos/IMG_001.jpg", Some(sel_objs[0]));
        insert_source(&conn, root_c, "photos/IMG_002.jpg", Some(sel_objs[1]));
        for i in 16..=35 {
            let obj = insert_object(&conn, &format!("hash_{:03}", i));
            insert_source(
                &conn,
                root_c,
                &format!("photos/COMP_{:03}.jpg", i),
                Some(obj),
            );
        }

        // Root D (Mirror): 3 overlap, no complementary
        insert_source(&conn, root_d, "copy/IMG_001.jpg", Some(sel_objs[0]));
        insert_source(&conn, root_d, "copy/IMG_002.jpg", Some(sel_objs[1]));
        insert_source(&conn, root_d, "copy/IMG_003.jpg", Some(sel_objs[2]));

        let options = SurveyOptions { affinity: true, ..test_options() };
        let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
        let outcome = run_compute(&mut conn, &["/mnt/drive-a"], &options, &filters, None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.location_results.len(), 3);
                // Superset first (B: 9/10 = 90% >= 80%)
                assert!(result.location_results[0].path.contains("backup-main"));
                assert_eq!(
                    result.location_results[0].kind,
                    Some(domain::survey::LocationKind::Superset)
                );
                assert_eq!(result.location_results[0].shared_count, 9);
                assert_eq!(result.location_results[0].complementary_count, Some(5));
                // Lead second (C)
                assert!(result.location_results[1].path.contains("partner"));
                assert_eq!(
                    result.location_results[1].kind,
                    Some(domain::survey::LocationKind::Lead)
                );
                assert_eq!(result.location_results[1].complementary_count, Some(20));
                // Subset last (D): 3/3 = 100% overlap, no complementary → Subset
                assert!(result.location_results[2].path.contains("old-copy"));
                assert_eq!(
                    result.location_results[2].kind,
                    Some(domain::survey::LocationKind::Subset)
                );
                assert_eq!(result.location_results[2].complementary_count, Some(0));
                assert_eq!(result.location_results[2].only_here_count, Some(0));
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // Selection narrowed by filter
    // =========================================================================

    #[test]
    fn test_selection_narrowed_by_filter() {
        let mut conn = open_in_memory_for_test();

        let root = insert_root(&conn, "/mnt/drive", "source");

        let obj1 = insert_object(&conn, "hash_001");
        let obj2 = insert_object(&conn, "hash_002");
        let obj3 = insert_object(&conn, "hash_003");
        let obj4 = insert_object(&conn, "hash_004");
        let obj5 = insert_object(&conn, "hash_005");

        insert_source(&conn, root, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root, "photos/b.jpg", Some(obj2));
        insert_source(&conn, root, "photos/c.txt", Some(obj3)); // won't match
        insert_source(&conn, root, "photos/d.txt", Some(obj4)); // won't match
        insert_source(&conn, root, "photos/e.jpg", Some(obj5));

        let options = test_options();
        let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &options, &filters, None);

        match outcome {
            SurveyOutcome::Result(result) => {
                // Selection narrowed from 5 to 3 by filter
                assert_eq!(result.total_count, 3);
                assert_eq!(result.total_hashed, 3);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // Same root, cross-scope complementary content
    // =========================================================================

    #[test]
    fn test_same_root_complementary() {
        let mut conn = open_in_memory_for_test();

        let root = insert_root(&conn, "/mnt/drive", "source");

        let obj1 = insert_object(&conn, "hash_001");
        let obj2 = insert_object(&conn, "hash_002");
        let obj3 = insert_object(&conn, "hash_003"); // complementary

        insert_source(&conn, root, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root, "photos/b.jpg", Some(obj2));
        // Same content as photos/a.jpg — overlap
        insert_source(&conn, root, "documents/a.jpg", Some(obj1));
        // Different content, matches filter — complementary
        insert_source(&conn, root, "documents/c.jpg", Some(obj3));

        let options = SurveyOptions { affinity: true, ..test_options() };
        let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
        let outcome = run_compute(&mut conn, &["/mnt/drive/photos"], &options, &filters, None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.total_count, 2);
                assert_eq!(result.location_results.len(), 1);
                assert_eq!(result.location_results[0].path, "/mnt/drive/documents");
                assert_eq!(result.location_results[0].shared_count, 1); // obj1
                assert_eq!(result.location_results[0].complementary_count, Some(1)); // obj3
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // Mirror with filters has kind=Mirror and comp=Some(0)
    // =========================================================================

    #[test]
    fn test_mirror_with_filters() {
        let mut conn = open_in_memory_for_test();

        let root_a = insert_root(&conn, "/mnt/drive", "source");
        let root_b = insert_root(&conn, "/mnt/mirror", "source");

        let obj1 = insert_object(&conn, "hash_001");
        let obj2 = insert_object(&conn, "hash_002");
        let obj3 = insert_object(&conn, "hash_003");

        insert_source(&conn, root_a, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/b.jpg", Some(obj2));
        insert_source(&conn, root_a, "photos/c.jpg", Some(obj3));

        // Mirror: same content, nothing complementary
        insert_source(&conn, root_b, "backup/a.jpg", Some(obj1));
        insert_source(&conn, root_b, "backup/b.jpg", Some(obj2));

        let options = SurveyOptions { affinity: true, ..test_options() };
        let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &options, &filters, None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.location_results.len(), 1);
                let loc = &result.location_results[0];
                // 2/2 = 100% overlap, no complementary → Subset
                assert_eq!(loc.kind, Some(domain::survey::LocationKind::Subset));
                // Some(0), not None — affinity WAS computed
                assert_eq!(loc.complementary_count, Some(0));
                assert_eq!(loc.only_here_count, Some(0));
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // --other and --brief
    // =========================================================================

    // =========================================================================
    // --other basic with overlap and complementary
    // =========================================================================

    #[test]
    fn test_other_basic() {
        let mut conn = open_in_memory_for_test();

        let root_a = insert_root(&conn, "/mnt/drive-a", "source");
        let root_b = insert_root(&conn, "/mnt/backup", "source");

        let obj1 = insert_object(&conn, "hash_001");
        let obj2 = insert_object(&conn, "hash_002");
        let obj3 = insert_object(&conn, "hash_003");
        let obj4 = insert_object(&conn, "hash_004");
        let obj5 = insert_object(&conn, "hash_005");
        let obj6 = insert_object(&conn, "hash_006");

        insert_source(&conn, root_a, "photos/IMG_001.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/IMG_002.jpg", Some(obj2));
        insert_source(&conn, root_a, "photos/IMG_003.jpg", Some(obj3));

        insert_source(&conn, root_b, "trip/IMG_001.jpg", Some(obj1));
        insert_source(&conn, root_b, "trip/IMG_002.jpg", Some(obj2));
        insert_source(&conn, root_b, "trip/IMG_004.jpg", Some(obj4));
        insert_source(&conn, root_b, "trip/IMG_005.jpg", Some(obj5));
        insert_source(&conn, root_b, "trip/notes.txt", Some(obj6));

        let options = SurveyOptions {
            affinity: true,
            other_paths: vec![PathBuf::from("/mnt/backup/trip")],
            ..test_options()
        };
        let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
        let outcome = run_compute(&mut conn, &["/mnt/drive-a"], &options, &filters, None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert!(result.is_other_mode);
                assert_eq!(result.location_results.len(), 1);
                let loc = &result.location_results[0];
                assert_eq!(loc.path, "/mnt/backup/trip");
                assert_eq!(loc.shared_count, 2);
                assert_eq!(loc.complementary_count, Some(2)); // obj4, obj5
                assert_eq!(loc.only_here_count, Some(2));
                assert_eq!(loc.kind, Some(domain::survey::LocationKind::Lead));
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // --other with zero overlap
    // =========================================================================

    #[test]
    fn test_other_zero_overlap() {
        let mut conn = open_in_memory_for_test();

        let root_a = insert_root(&conn, "/mnt/drive-a", "source");
        let root_b = insert_root(&conn, "/mnt/backup", "source");

        let obj1 = insert_object(&conn, "hash_001");
        let obj2 = insert_object(&conn, "hash_002");
        let obj3 = insert_object(&conn, "hash_003");
        let obj4 = insert_object(&conn, "hash_004");

        insert_source(&conn, root_a, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/b.jpg", Some(obj2));

        insert_source(&conn, root_b, "docs/c.jpg", Some(obj3));
        insert_source(&conn, root_b, "docs/d.jpg", Some(obj4));

        let options = SurveyOptions {
            affinity: true,
            other_paths: vec![PathBuf::from("/mnt/backup")],
            ..test_options()
        };
        let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
        let outcome = run_compute(&mut conn, &["/mnt/drive-a"], &options, &filters, None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert!(result.is_other_mode);
                assert_eq!(result.location_results.len(), 1);
                let loc = &result.location_results[0];
                assert_eq!(loc.shared_count, 0);
                assert_eq!(loc.complementary_count, Some(2)); // obj3, obj4
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // --other preserves user order
    // =========================================================================

    #[test]
    fn test_other_preserves_order() {
        let mut conn = open_in_memory_for_test();

        let root_a = insert_root(&conn, "/mnt/drive-a", "source");
        let root_b = insert_root(&conn, "/mnt/root-b", "source");
        let root_c = insert_root(&conn, "/mnt/root-c", "source");

        let obj1 = insert_object(&conn, "hash_001");
        let obj2 = insert_object(&conn, "hash_002");
        let obj3 = insert_object(&conn, "hash_003");

        // Selection: 1 source
        insert_source(&conn, root_a, "photos/a.jpg", Some(obj1));

        // Root B (high shared): 3 sources including overlap
        insert_source(&conn, root_b, "trip/a.jpg", Some(obj1));
        insert_source(&conn, root_b, "trip/b.jpg", Some(obj2));
        insert_source(&conn, root_b, "trip/c.jpg", Some(obj3));

        // Root C (low shared): 1 source with overlap
        insert_source(&conn, root_c, "backup/a.jpg", Some(obj1));

        // Specify C before B — should preserve this order
        let options = SurveyOptions {
            other_paths: vec![PathBuf::from("/mnt/root-c"), PathBuf::from("/mnt/root-b")],
            ..test_options()
        };
        let outcome = run_compute(&mut conn, &["/mnt/drive-a"], &options, &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert!(result.is_other_mode);
                assert_eq!(result.location_results.len(), 2);
                // User order preserved: C first, B second
                assert!(result.location_results[0].path.contains("root-c"));
                assert!(result.location_results[1].path.contains("root-b"));
                // C has less shared, but appears first (user order)
                assert_eq!(result.location_results[0].shared_count, 1);
                assert_eq!(result.location_results[1].shared_count, 1);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // --other on archive root
    // =========================================================================

    #[test]
    fn test_other_archive_root() {
        let mut conn = open_in_memory_for_test();

        let root_a = insert_root(&conn, "/mnt/drive-a", "source");
        let archive = insert_root(&conn, "/archive", "archive");

        let obj1 = insert_object(&conn, "hash_001");
        let obj2 = insert_object(&conn, "hash_002");
        let obj3 = insert_object(&conn, "hash_003");

        insert_source(&conn, root_a, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/b.jpg", Some(obj2));

        // Archive root: obj1 (overlap) + obj3 (complementary)
        insert_source(&conn, archive, "2024/a.jpg", Some(obj1));
        insert_source(&conn, archive, "2024/c.jpg", Some(obj3));

        let options = SurveyOptions {
            affinity: true,
            other_paths: vec![PathBuf::from("/archive")],
            ..test_options()
        };
        let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
        let outcome = run_compute(&mut conn, &["/mnt/drive-a"], &options, &filters, None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert!(result.is_other_mode);
                assert_eq!(result.location_results.len(), 1);
                let loc = &result.location_results[0];
                // Archive sources counted because --other overrides role restriction
                assert_eq!(loc.shared_count, 1); // obj1
                assert_eq!(loc.complementary_count, Some(1)); // obj3
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // --brief suppresses affinity
    // =========================================================================

    #[test]
    fn test_brief_suppresses_affinity() {
        let mut conn = open_in_memory_for_test();

        let root_a = insert_root(&conn, "/mnt/drive-a", "source");
        let root_b = insert_root(&conn, "/mnt/backup", "source");

        let obj1 = insert_object(&conn, "hash_001");
        let obj2 = insert_object(&conn, "hash_002");
        let obj3 = insert_object(&conn, "hash_003");
        let obj4 = insert_object(&conn, "hash_004");

        insert_source(&conn, root_a, "photos/IMG_001.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/IMG_002.jpg", Some(obj2));
        insert_source(&conn, root_a, "photos/IMG_003.jpg", Some(obj3));

        insert_source(&conn, root_b, "trip/IMG_001.jpg", Some(obj1));
        insert_source(&conn, root_b, "trip/IMG_002.jpg", Some(obj2));
        insert_source(&conn, root_b, "trip/IMG_004.jpg", Some(obj4));

        let options = SurveyOptions { affinity: true, brief: true, ..test_options() };
        let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
        // With --brief: affinity suppressed even though --affinity present
        let outcome = run_compute(&mut conn, &["/mnt/drive-a"], &options, &filters, None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert!(!result.is_other_mode);
                assert_eq!(result.location_results.len(), 1);
                let loc = &result.location_results[0];
                assert_eq!(loc.shared_count, 2); // still computed
                assert_eq!(loc.complementary_count, None);
                assert_eq!(loc.only_here_count, None);
                assert_eq!(loc.kind, None);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // --brief without --where is no-op
    // =========================================================================

    #[test]
    fn test_brief_without_filters_noop() {
        let mut conn = open_in_memory_for_test();

        let root_a = insert_root(&conn, "/mnt/drive-a", "source");
        let root_b = insert_root(&conn, "/mnt/backup", "source");

        let obj1 = insert_object(&conn, "hash_001");
        let obj2 = insert_object(&conn, "hash_002");
        let obj3 = insert_object(&conn, "hash_003");

        insert_source(&conn, root_a, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/b.jpg", Some(obj2));

        insert_source(&conn, root_b, "trip/a.jpg", Some(obj1));
        insert_source(&conn, root_b, "trip/c.jpg", Some(obj3));

        // Without --brief
        let options_normal = test_options();
        let outcome_normal =
            run_compute(&mut conn, &["/mnt/drive-a"], &options_normal, &[], None);
        // With --brief (should be identical — no affinity means brief is a no-op)
        let options_brief = SurveyOptions { brief: true, ..test_options() };
        let outcome_brief =
            run_compute(&mut conn, &["/mnt/drive-a"], &options_brief, &[], None);

        match (outcome_normal, outcome_brief) {
            (SurveyOutcome::Result(normal), SurveyOutcome::Result(brief)) => {
                assert_eq!(normal.location_results.len(), brief.location_results.len());
                assert_eq!(
                    normal.location_results[0].shared_count,
                    brief.location_results[0].shared_count
                );
                assert_eq!(
                    normal.location_results[0].complementary_count,
                    brief.location_results[0].complementary_count
                );
                assert_eq!(normal.unique_count, brief.unique_count);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // --other with --brief
    // =========================================================================

    #[test]
    fn test_other_with_brief() {
        let mut conn = open_in_memory_for_test();

        let root_a = insert_root(&conn, "/mnt/drive-a", "source");
        let root_b = insert_root(&conn, "/mnt/backup", "source");

        let obj1 = insert_object(&conn, "hash_001");
        let obj2 = insert_object(&conn, "hash_002");
        let obj3 = insert_object(&conn, "hash_003");
        let obj4 = insert_object(&conn, "hash_004");

        insert_source(&conn, root_a, "photos/IMG_001.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/IMG_002.jpg", Some(obj2));
        insert_source(&conn, root_a, "photos/IMG_003.jpg", Some(obj3));

        insert_source(&conn, root_b, "trip/IMG_001.jpg", Some(obj1));
        insert_source(&conn, root_b, "trip/IMG_004.jpg", Some(obj4));

        let options = SurveyOptions {
            affinity: true,
            brief: true,
            other_paths: vec![PathBuf::from("/mnt/backup/trip")],
            ..test_options()
        };
        let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
        let outcome = run_compute(&mut conn, &["/mnt/drive-a"], &options, &filters, None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert!(result.is_other_mode);
                assert_eq!(result.location_results.len(), 1);
                let loc = &result.location_results[0];
                assert_eq!(loc.shared_count, 1); // obj1
                // Affinity suppressed by --brief
                assert_eq!(loc.complementary_count, None);
                assert_eq!(loc.only_here_count, None);
                assert_eq!(loc.kind, None);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // --other same root cross-scope
    // =========================================================================

    #[test]
    fn test_other_same_root_cross_scope() {
        let mut conn = open_in_memory_for_test();

        let root = insert_root(&conn, "/mnt/drive", "source");

        let obj1 = insert_object(&conn, "hash_001");
        let obj2 = insert_object(&conn, "hash_002");
        let obj3 = insert_object(&conn, "hash_003");

        // Selection scope
        insert_source(&conn, root, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root, "photos/b.jpg", Some(obj2));
        // Other scope: overlap + complementary
        insert_source(&conn, root, "documents/a.jpg", Some(obj1));
        insert_source(&conn, root, "documents/c.jpg", Some(obj3));

        let options = SurveyOptions {
            affinity: true,
            other_paths: vec![PathBuf::from("/mnt/drive/documents")],
            ..test_options()
        };
        let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
        let outcome = run_compute(&mut conn, &["/mnt/drive/photos"], &options, &filters, None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert!(result.is_other_mode);
                assert_eq!(result.location_results.len(), 1);
                let loc = &result.location_results[0];
                assert_eq!(loc.path, "/mnt/drive/documents");
                assert_eq!(loc.shared_count, 1); // obj1
                assert_eq!(loc.complementary_count, Some(1)); // obj3
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // Detail views: complement paths and unique paths
    // =========================================================================

    // =========================================================================
    // Complement paths populated with correct relative paths
    // =========================================================================

    #[test]
    fn test_detail_complement_paths() {
        let mut conn = open_in_memory_for_test();

        let root_a = insert_root(&conn, "/mnt/drive-a", "source");
        let root_b = insert_root(&conn, "/mnt/backup", "source");

        let obj1 = insert_object(&conn, "hash_001"); // overlap
        let obj2 = insert_object(&conn, "hash_002"); // overlap
        let obj3 = insert_object(&conn, "hash_003"); // unique to selection
        let obj4 = insert_object(&conn, "hash_004"); // complementary at B
        let obj5 = insert_object(&conn, "hash_005"); // complementary at B

        insert_source(&conn, root_a, "photos/IMG_001.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/IMG_002.jpg", Some(obj2));
        insert_source(&conn, root_a, "photos/IMG_003.jpg", Some(obj3));

        insert_source(&conn, root_b, "trip/IMG_001.jpg", Some(obj1));
        insert_source(&conn, root_b, "trip/IMG_002.jpg", Some(obj2));
        insert_source(&conn, root_b, "trip/IMG_004.jpg", Some(obj4));
        insert_source(&conn, root_b, "trip/IMG_005.jpg", Some(obj5));

        let options = SurveyOptions { affinity: true, ..test_options() };
        let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
        let outcome = run_compute(&mut conn, &["/mnt/drive-a"], &options, &filters, None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.location_results.len(), 1);
                let loc = &result.location_results[0];
                assert_eq!(loc.path, "/mnt/backup/trip");
                let paths = loc.complementary_paths.as_ref().unwrap();
                assert_eq!(paths.len(), 2);
                // Sorted alphabetically, relative to /mnt/backup/trip
                assert_eq!(paths[0], "IMG_004.jpg");
                assert_eq!(paths[1], "IMG_005.jpg");
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // Mirror with filters: complementary_paths is Some(empty), not None
    // =========================================================================

    #[test]
    fn test_detail_complement_mirror_has_empty_paths() {
        let mut conn = open_in_memory_for_test();

        let root_a = insert_root(&conn, "/mnt/drive", "source");
        let root_b = insert_root(&conn, "/mnt/mirror", "source");

        let obj1 = insert_object(&conn, "hash_001");
        let obj2 = insert_object(&conn, "hash_002");

        insert_source(&conn, root_a, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/b.jpg", Some(obj2));

        // Mirror: overlap only, no complementary
        insert_source(&conn, root_b, "backup/a.jpg", Some(obj1));

        let options = SurveyOptions { affinity: true, ..test_options() };
        let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &options, &filters, None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.location_results.len(), 1);
                let loc = &result.location_results[0];
                // Affinity computed → Some(empty), not None
                assert_eq!(loc.complementary_paths, Some(vec![]));
                assert_eq!(loc.complementary_count, Some(0));
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // Without --where: complementary_paths is None (no affinity)
    // =========================================================================

    #[test]
    fn test_detail_complement_no_affinity_has_none_paths() {
        let mut conn = open_in_memory_for_test();

        let root_a = insert_root(&conn, "/mnt/drive-a", "source");
        let root_b = insert_root(&conn, "/mnt/backup", "source");

        let obj1 = insert_object(&conn, "hash_001");

        insert_source(&conn, root_a, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root_b, "trip/a.jpg", Some(obj1));

        let options = test_options();
        let outcome = run_compute(&mut conn, &["/mnt/drive-a"], &options, &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.location_results.len(), 1);
                let loc = &result.location_results[0];
                assert_eq!(loc.complementary_paths, None);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // Unique paths populated correctly
    // =========================================================================

    #[test]
    fn test_unique_paths_populated() {
        let mut conn = open_in_memory_for_test();

        let root_a = insert_root(&conn, "/mnt/drive", "source");
        let root_b = insert_root(&conn, "/mnt/other", "source");
        let archive = insert_root(&conn, "/archive", "archive");

        let obj1 = insert_object(&conn, "hash_001"); // unique to selection
        let obj2 = insert_object(&conn, "hash_002"); // copy on root_b
        let obj3 = insert_object(&conn, "hash_003"); // archived

        insert_source(&conn, root_a, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/b.jpg", Some(obj2));
        insert_source(&conn, root_a, "photos/c.jpg", Some(obj3));

        insert_source(&conn, root_b, "backup/b.jpg", Some(obj2));
        insert_source(&conn, archive, "2024/c.jpg", Some(obj3));

        let options = test_options();
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &options, &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.unique_count, 1);
                assert_eq!(result.unique_paths, vec!["/mnt/drive/photos/a.jpg"]);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // Unique paths empty when nothing is unique
    // =========================================================================

    #[test]
    fn test_unique_paths_empty_when_none_unique() {
        let mut conn = open_in_memory_for_test();

        let root_a = insert_root(&conn, "/mnt/drive", "source");
        let root_b = insert_root(&conn, "/mnt/other", "source");

        let obj1 = insert_object(&conn, "hash_001");
        let obj2 = insert_object(&conn, "hash_002");

        insert_source(&conn, root_a, "a.jpg", Some(obj1));
        insert_source(&conn, root_a, "b.jpg", Some(obj2));

        // Both have copies elsewhere
        insert_source(&conn, root_b, "a.jpg", Some(obj1));
        insert_source(&conn, root_b, "b.jpg", Some(obj2));

        let options = test_options();
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &options, &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.unique_count, 0);
                assert!(result.unique_paths.is_empty());
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // Unique paths: duplicates within selection list both sources
    // =========================================================================

    #[test]
    fn test_unique_paths_duplicates_within_selection() {
        let mut conn = open_in_memory_for_test();

        let root = insert_root(&conn, "/mnt/drive", "source");

        let obj1 = insert_object(&conn, "hash_001");

        // Two sources, same content, both in selection
        insert_source(&conn, root, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root, "photos/a_copy.jpg", Some(obj1));

        let options = test_options();
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &options, &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                // Object-level: 1 unique object
                assert_eq!(result.unique_count, 1);
                // Source-level: both files listed
                assert_eq!(result.unique_paths.len(), 2);
                assert_eq!(result.unique_paths[0], "/mnt/drive/photos/a.jpg");
                assert_eq!(result.unique_paths[1], "/mnt/drive/photos/a_copy.jpg");
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // Complement paths relative to deeper location
    // =========================================================================

    #[test]
    fn test_complement_paths_relative_to_location() {
        let mut conn = open_in_memory_for_test();

        let root_a = insert_root(&conn, "/mnt/drive-a", "source");
        let root_b = insert_root(&conn, "/mnt/backup", "source");

        let obj1 = insert_object(&conn, "hash_001"); // overlap
        let obj2 = insert_object(&conn, "hash_002"); // complementary

        insert_source(&conn, root_a, "photos/a.jpg", Some(obj1));

        // Overlap + complementary at a deeper path within root_b
        insert_source(&conn, root_b, "trip/week1/a.jpg", Some(obj1));
        insert_source(&conn, root_b, "trip/week1/sub/deep.jpg", Some(obj2));

        let options = SurveyOptions { affinity: true, ..test_options() };
        let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
        let outcome = run_compute(&mut conn, &["/mnt/drive-a"], &options, &filters, None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.location_results.len(), 1);
                let loc = &result.location_results[0];
                // Scope discovery collapses to /mnt/backup/trip/week1
                assert_eq!(loc.path, "/mnt/backup/trip/week1");
                let paths = loc.complementary_paths.as_ref().unwrap();
                assert_eq!(paths.len(), 1);
                // Relative to the location path
                assert_eq!(paths[0], "sub/deep.jpg");
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // --other zero overlap: complementary paths still populated
    // =========================================================================

    #[test]
    fn test_complement_other_mode_zero_overlap_has_paths() {
        let mut conn = open_in_memory_for_test();

        let root_a = insert_root(&conn, "/mnt/drive-a", "source");
        let root_b = insert_root(&conn, "/mnt/backup", "source");

        let obj1 = insert_object(&conn, "hash_001"); // selection only
        let obj2 = insert_object(&conn, "hash_002"); // at B only (complementary)
        let obj3 = insert_object(&conn, "hash_003"); // at B only (complementary)

        insert_source(&conn, root_a, "photos/a.jpg", Some(obj1));

        insert_source(&conn, root_b, "docs/x.jpg", Some(obj2));
        insert_source(&conn, root_b, "docs/y.jpg", Some(obj3));

        let options = SurveyOptions {
            affinity: true,
            other_paths: vec![PathBuf::from("/mnt/backup")],
            ..test_options()
        };
        let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
        let outcome = run_compute(&mut conn, &["/mnt/drive-a"], &options, &filters, None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert!(result.is_other_mode);
                assert_eq!(result.location_results.len(), 1);
                let loc = &result.location_results[0];
                assert_eq!(loc.shared_count, 0); // no overlap
                assert_eq!(loc.complementary_count, Some(2));
                let paths = loc.complementary_paths.as_ref().unwrap();
                assert_eq!(paths.len(), 2);
                assert_eq!(paths[0], "docs/x.jpg");
                assert_eq!(paths[1], "docs/y.jpg");
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // --archive filters to specific archive root
    // =========================================================================

    #[test]
    fn test_archive_filter_specific_root() {
        let mut conn = open_in_memory_for_test();

        let root = insert_root(&conn, "/mnt/drive", "source");
        let archive_a = insert_root(&conn, "/archive/a", "archive");
        let archive_b = insert_root(&conn, "/archive/b", "archive");

        let obj1 = insert_object(&conn, "hash_001");
        let obj2 = insert_object(&conn, "hash_002");
        let obj3 = insert_object(&conn, "hash_003");

        insert_source(&conn, root, "x.jpg", Some(obj1));
        insert_source(&conn, root, "y.jpg", Some(obj2));
        insert_source(&conn, root, "z.jpg", Some(obj3));

        // Archive A has obj1 and obj2
        insert_source(&conn, archive_a, "2024/x.jpg", Some(obj1));
        insert_source(&conn, archive_a, "2024/y.jpg", Some(obj2));
        // Archive B has obj3
        insert_source(&conn, archive_b, "backup/z.jpg", Some(obj3));

        // Without --archive: all 3 archived
        let options = test_options();
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &options, &[], None);
        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.archived_source_count, 3);
                assert_eq!(result.archive_scopes.len(), 2);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }

        // With --archive filtering to archive A: only 2 archived
        let options = test_options();
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &options, &[], Some(archive_a));
        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.archived_source_count, 2); // obj1 and obj2 only
                assert_eq!(result.archive_scopes.len(), 1);
                assert_eq!(result.archive_scopes[0].0, "/archive/a/2024");
                assert_eq!(result.archive_scopes[0].1, 2);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // --archive with no matching content in target archive
    // =========================================================================

    #[test]
    fn test_archive_filter_no_matches() {
        let mut conn = open_in_memory_for_test();

        let root = insert_root(&conn, "/mnt/drive", "source");
        let archive_a = insert_root(&conn, "/archive/a", "archive");
        let archive_b = insert_root(&conn, "/archive/b", "archive");

        let obj1 = insert_object(&conn, "hash_001");
        let obj2 = insert_object(&conn, "hash_002");

        insert_source(&conn, root, "x.jpg", Some(obj1));
        insert_source(&conn, root, "y.jpg", Some(obj2));

        // Only archive B has content, but we filter to archive A
        insert_source(&conn, archive_b, "backup/x.jpg", Some(obj1));

        let options = test_options();
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &options, &[], Some(archive_a));

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.archived_source_count, 0);
                assert!(result.archive_scopes.is_empty());
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // --archive does not affect other sections
    // =========================================================================

    #[test]
    fn test_archive_filter_does_not_affect_other_sections() {
        let mut conn = open_in_memory_for_test();

        let root_a = insert_root(&conn, "/mnt/drive", "source");
        let root_b = insert_root(&conn, "/mnt/backup", "source");
        let archive_a = insert_root(&conn, "/archive/a", "archive");
        let archive_b = insert_root(&conn, "/archive/b", "archive");

        let obj1 = insert_object(&conn, "hash_001"); // on drive, backup, archive_a
        let obj2 = insert_object(&conn, "hash_002"); // on drive, archive_b
        let obj3 = insert_object(&conn, "hash_003"); // unique to drive

        insert_source(&conn, root_a, "x.jpg", Some(obj1));
        insert_source(&conn, root_a, "y.jpg", Some(obj2));
        insert_source(&conn, root_a, "z.jpg", Some(obj3));

        insert_source(&conn, root_b, "copy/x.jpg", Some(obj1));

        insert_source(&conn, archive_a, "2024/x.jpg", Some(obj1));
        insert_source(&conn, archive_b, "backup/y.jpg", Some(obj2));

        // Without --archive
        let options = test_options();
        let outcome_all = run_compute(&mut conn, &["/mnt/drive"], &options, &[], None);
        // With --archive filtering to archive_a
        let outcome_filtered = run_compute(&mut conn, &["/mnt/drive"], &options, &[], Some(archive_a));

        match (outcome_all, outcome_filtered) {
            (SurveyOutcome::Result(all), SurveyOutcome::Result(filtered)) => {
                // Archive section differs
                assert_eq!(all.archived_source_count, 2); // obj1 + obj2
                assert_eq!(filtered.archived_source_count, 1); // obj1 only

                // But overlap and unique are unchanged
                assert_eq!(all.location_results.len(), filtered.location_results.len());
                assert_eq!(
                    all.location_results[0].shared_count,
                    filtered.location_results[0].shared_count,
                );
                assert_eq!(all.unique_count, filtered.unique_count);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // Many locations: all computed regardless of cap
    // =========================================================================

    #[test]
    fn test_many_locations_all_computed() {
        let mut conn = open_in_memory_for_test();

        let root_sel = insert_root(&conn, "/mnt/selection", "source");
        let obj_shared = insert_object(&conn, "hash_shared");

        insert_source(&conn, root_sel, "a.jpg", Some(obj_shared));

        // Create 15 source roots, each with one overlapping source
        for i in 0..15 {
            let root = insert_root(&conn, &format!("/mnt/other-{:02}", i), "source");
            insert_source(
                &conn,
                root,
                &format!("dir/copy_{:02}.jpg", i),
                Some(obj_shared),
            );
        }

        let options = test_options();
        let outcome = run_compute(&mut conn, &["/mnt/selection"], &options, &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                // All 15 locations computed — cap is output-only
                assert_eq!(result.location_results.len(), 15);
                for loc in &result.location_results {
                    assert_eq!(loc.shared_count, 1);
                }
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // Phase 1: Orientation default + affinity gate tests
    // =========================================================================

    // =========================================================================
    // Orientation default: no filters, no affinity — affinity columns absent,
    // locations sorted by shared_count desc, unique_count present
    // =========================================================================

    #[test]
    fn test_orientation_default_no_filters() {
        let mut conn = open_in_memory_for_test();

        let root_a = insert_root(&conn, "/mnt/drive", "source");
        let root_b = insert_root(&conn, "/mnt/backup", "source");
        let root_c = insert_root(&conn, "/mnt/other", "source");

        let obj1 = insert_object(&conn, "hash_001");
        let obj2 = insert_object(&conn, "hash_002");
        let obj3 = insert_object(&conn, "hash_003");
        let obj4 = insert_object(&conn, "hash_004"); // unique to drive

        insert_source(&conn, root_a, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/b.jpg", Some(obj2));
        insert_source(&conn, root_a, "photos/c.jpg", Some(obj3));
        insert_source(&conn, root_a, "photos/d.jpg", Some(obj4));

        // backup: 2 overlap
        insert_source(&conn, root_b, "trip/a.jpg", Some(obj1));
        insert_source(&conn, root_b, "trip/b.jpg", Some(obj2));

        // other: 1 overlap
        insert_source(&conn, root_c, "misc/a.jpg", Some(obj1));

        let options = test_options(); // affinity: false, brief: false
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &options, &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.total_hashed, 4);
                assert_eq!(result.unique_count, 2); // obj3 and obj4

                // No affinity columns in orientation mode
                assert_eq!(result.location_results.len(), 2);
                for loc in &result.location_results {
                    assert_eq!(loc.complementary_count, None);
                    assert_eq!(loc.only_here_count, None);
                    assert_eq!(loc.kind, None);
                }

                // Sorted by shared_count desc: backup (2) before other (1)
                assert!(result.location_results[0].path.contains("backup"));
                assert_eq!(result.location_results[0].shared_count, 2);
                assert!(result.location_results[1].path.contains("other"));
                assert_eq!(result.location_results[1].shared_count, 1);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // Orientation with filters: --where present but no --affinity
    // Filters narrow selection, affinity columns still absent
    // =========================================================================

    #[test]
    fn test_orientation_with_filters() {
        let mut conn = open_in_memory_for_test();

        let root_a = insert_root(&conn, "/mnt/drive", "source");
        let root_b = insert_root(&conn, "/mnt/backup", "source");

        let obj1 = insert_object(&conn, "hash_001");
        let obj2 = insert_object(&conn, "hash_002");
        let obj3 = insert_object(&conn, "hash_003");

        insert_source(&conn, root_a, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/b.txt", Some(obj2)); // won't match filter
        insert_source(&conn, root_a, "photos/c.jpg", Some(obj3));

        insert_source(&conn, root_b, "trip/a.jpg", Some(obj1));

        let options = test_options(); // affinity: false
        let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &options, &filters, None);

        match outcome {
            SurveyOutcome::Result(result) => {
                // Selection narrowed by filter
                assert_eq!(result.total_count, 2);
                assert_eq!(result.total_hashed, 2);

                // Affinity columns absent — filters don't auto-enable affinity
                assert_eq!(result.location_results.len(), 1);
                let loc = &result.location_results[0];
                assert_eq!(loc.complementary_count, None);
                assert_eq!(loc.only_here_count, None);
                assert_eq!(loc.kind, None);
                assert_eq!(loc.shared_count, 1);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // Zero unique: all content exists elsewhere
    // =========================================================================

    #[test]
    fn test_zero_unique_shown() {
        let mut conn = open_in_memory_for_test();

        let root_a = insert_root(&conn, "/mnt/drive", "source");
        let root_b = insert_root(&conn, "/mnt/backup", "source");

        let obj1 = insert_object(&conn, "hash_001");
        let obj2 = insert_object(&conn, "hash_002");

        insert_source(&conn, root_a, "a.jpg", Some(obj1));
        insert_source(&conn, root_a, "b.jpg", Some(obj2));

        // Everything has a copy
        insert_source(&conn, root_b, "a.jpg", Some(obj1));
        insert_source(&conn, root_b, "b.jpg", Some(obj2));

        let options = test_options();
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &options, &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.unique_count, 0);
                assert!(result.unique_paths.is_empty());
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // --affinity requires --where: validation in run()
    // =========================================================================

    #[test]
    fn test_affinity_requires_where() {
        let conn = open_in_memory_for_test();

        let root = insert_root(&conn, "/mnt/drive", "source");
        let obj1 = insert_object(&conn, "hash_001");
        insert_source(&conn, root, "a.jpg", Some(obj1));

        let options = SurveyOptions { affinity: true, ..test_options() };
        let paths = vec![PathBuf::from("/mnt/drive")];
        let filter_strs: Vec<String> = vec![];

        let mut db = repo::Db::from_connection(conn);
        let result = run(&mut db, &paths, &filter_strs, &options);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("--affinity"));
        assert!(err.contains("--where"));
    }

    // =========================================================================
    // --affinity + --brief: affinity suppressed, identical to orientation
    // =========================================================================

    #[test]
    fn test_affinity_brief_noop() {
        let mut conn = open_in_memory_for_test();

        let root_a = insert_root(&conn, "/mnt/drive", "source");
        let root_b = insert_root(&conn, "/mnt/backup", "source");

        let obj1 = insert_object(&conn, "hash_001");
        let obj2 = insert_object(&conn, "hash_002");
        let obj3 = insert_object(&conn, "hash_003");

        insert_source(&conn, root_a, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/b.jpg", Some(obj2));

        insert_source(&conn, root_b, "trip/a.jpg", Some(obj1));
        insert_source(&conn, root_b, "trip/c.jpg", Some(obj3));

        // --affinity + --brief: affinity suppressed
        let options = SurveyOptions { affinity: true, brief: true, ..test_options() };
        let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &options, &filters, None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.location_results.len(), 1);
                let loc = &result.location_results[0];
                // Affinity columns absent — suppressed by --brief
                assert_eq!(loc.complementary_count, None);
                assert_eq!(loc.only_here_count, None);
                assert_eq!(loc.kind, None);
                // shared_count still computed
                assert_eq!(loc.shared_count, 1);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // --brief without --affinity: identical to orientation
    // =========================================================================

    #[test]
    fn test_brief_without_affinity_noop() {
        let mut conn = open_in_memory_for_test();

        let root_a = insert_root(&conn, "/mnt/drive", "source");
        let root_b = insert_root(&conn, "/mnt/backup", "source");

        let obj1 = insert_object(&conn, "hash_001");
        let obj2 = insert_object(&conn, "hash_002");

        insert_source(&conn, root_a, "a.jpg", Some(obj1));
        insert_source(&conn, root_a, "b.jpg", Some(obj2));
        insert_source(&conn, root_b, "a.jpg", Some(obj1));

        // --brief without --affinity: same as plain orientation
        let options_plain = test_options();
        let outcome_plain = run_compute(&mut conn, &["/mnt/drive"], &options_plain, &[], None);

        let options_brief = SurveyOptions { brief: true, ..test_options() };
        let outcome_brief = run_compute(&mut conn, &["/mnt/drive"], &options_brief, &[], None);

        match (outcome_plain, outcome_brief) {
            (SurveyOutcome::Result(plain), SurveyOutcome::Result(brief)) => {
                assert_eq!(plain.location_results.len(), brief.location_results.len());
                assert_eq!(
                    plain.location_results[0].shared_count,
                    brief.location_results[0].shared_count
                );
                assert_eq!(plain.unique_count, brief.unique_count);
                // Both lack affinity columns
                assert_eq!(plain.location_results[0].kind, None);
                assert_eq!(brief.location_results[0].kind, None);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // Subset classification: comp==0 and shared/total >= 0.8
    // =========================================================================

    #[test]
    fn test_subset_classification() {
        let mut conn = open_in_memory_for_test();

        let root_a = insert_root(&conn, "/mnt/drive", "source");
        let root_b = insert_root(&conn, "/mnt/backup", "source");

        // Selection: 3 objects
        let obj1 = insert_object(&conn, "hash_001");
        let obj2 = insert_object(&conn, "hash_002");
        let obj3 = insert_object(&conn, "hash_003");

        insert_source(&conn, root_a, "photos/a.jpg", Some(obj1));
        insert_source(&conn, root_a, "photos/b.jpg", Some(obj2));
        insert_source(&conn, root_a, "photos/c.jpg", Some(obj3));

        // Backup has 2 of 3 selection objects + no complementary.
        // total_count at location = 2, shared = 2, ratio = 2/2 = 1.0 >= 0.8
        insert_source(&conn, root_b, "trip/a.jpg", Some(obj1));
        insert_source(&conn, root_b, "trip/b.jpg", Some(obj2));

        let options = SurveyOptions { affinity: true, ..test_options() };
        let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &options, &filters, None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.location_results.len(), 1);
                let loc = &result.location_results[0];
                assert_eq!(loc.kind, Some(domain::survey::LocationKind::Subset));
                assert_eq!(loc.shared_count, 2);
                assert_eq!(loc.total_count, 2);
                assert_eq!(loc.complementary_count, Some(0));
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // Subset vs Mirror: below threshold classified as Mirror
    // =========================================================================

    #[test]
    fn test_subset_vs_mirror() {
        let mut conn = open_in_memory_for_test();

        let root_a = insert_root(&conn, "/mnt/drive", "source");
        let root_b = insert_root(&conn, "/mnt/backup", "source");

        // Selection: 1 object
        let obj1 = insert_object(&conn, "hash_001");

        // Non-overlapping objects at backup
        let obj_other1 = insert_object(&conn, "hash_other_1");
        let obj_other2 = insert_object(&conn, "hash_other_2");
        let obj_other3 = insert_object(&conn, "hash_other_3");
        let obj_other4 = insert_object(&conn, "hash_other_4");

        insert_source(&conn, root_a, "photos/a.jpg", Some(obj1));

        // Backup: 1 overlap + 4 other hashed sources (not in selection, not complementary
        // because they don't match filter). Total hashed at location = 5.
        // shared/total = 1/5 = 0.2 < 0.8 → Mirror, not Subset
        insert_source(&conn, root_b, "trip/a.jpg", Some(obj1));
        insert_source(&conn, root_b, "trip/x1.txt", Some(obj_other1));
        insert_source(&conn, root_b, "trip/x2.txt", Some(obj_other2));
        insert_source(&conn, root_b, "trip/x3.txt", Some(obj_other3));
        insert_source(&conn, root_b, "trip/x4.txt", Some(obj_other4));

        let options = SurveyOptions { affinity: true, ..test_options() };
        let filters = vec![Filter::parse("source.ext=jpg").unwrap()];
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &options, &filters, None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.location_results.len(), 1);
                let loc = &result.location_results[0];
                assert_eq!(loc.kind, Some(domain::survey::LocationKind::Mirror));
                assert_eq!(loc.shared_count, 1);
                assert_eq!(loc.total_count, 5);
                // No complementary (filter=jpg, other sources are .txt → not matching)
                assert_eq!(loc.complementary_count, Some(0));
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }

    // =========================================================================
    // total_count matches total hashed sources at location
    // =========================================================================

    #[test]
    fn test_total_count_in_summary() {
        let mut conn = open_in_memory_for_test();

        let root_a = insert_root(&conn, "/mnt/drive", "source");
        let root_b = insert_root(&conn, "/mnt/backup", "source");

        let obj1 = insert_object(&conn, "hash_001"); // overlap
        let obj2 = insert_object(&conn, "hash_002"); // backup only
        let obj3 = insert_object(&conn, "hash_003"); // backup only
        let obj4 = insert_object(&conn, "hash_004"); // drive only

        insert_source(&conn, root_a, "a.jpg", Some(obj1));
        insert_source(&conn, root_a, "b.jpg", Some(obj4));

        // Backup: 3 hashed sources (1 overlap + 2 other)
        insert_source(&conn, root_b, "copy/a.jpg", Some(obj1));
        insert_source(&conn, root_b, "copy/x.jpg", Some(obj2));
        insert_source(&conn, root_b, "copy/y.jpg", Some(obj3));
        // Plus one unhashed — should NOT count in total_count
        insert_source(&conn, root_b, "copy/pending.raw", None);

        let options = test_options();
        let outcome = run_compute(&mut conn, &["/mnt/drive"], &options, &[], None);

        match outcome {
            SurveyOutcome::Result(result) => {
                assert_eq!(result.location_results.len(), 1);
                let loc = &result.location_results[0];
                // total_count = hashed sources at location (source-role, active, non-excluded)
                assert_eq!(loc.total_count, 3);
                assert_eq!(loc.shared_count, 1);
            }
            _ => panic!("Expected SurveyOutcome::Result"),
        }
    }
}
