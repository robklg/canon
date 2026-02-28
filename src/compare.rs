use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::path::Path;

use crate::domain::path::resolve_path;
use crate::domain::scope::ScopeMatch;
use crate::domain::source::Source;
use crate::domain::IncludeSet;
use crate::expr::filter::{self, Filter};
use crate::repo::{self, Db};

pub struct CompareOptions {
    pub include: IncludeSet,
    pub verbose: bool,
}

pub fn run(
    db: &mut Db,
    path_a: &Path,
    path_b: &Path,
    filter_strs: &[String],
    options: &CompareOptions,
) -> Result<bool> {
    // Parse filters
    let filters: Vec<Filter> = filter_strs
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    let conn = db.conn_mut();

    // Resolve both paths (soft resolution: matches known roots, falls back to fs)
    let all_roots = repo::root::fetch_all(conn)?;
    let cwd = std::env::current_dir()?;
    let prefix_a = resolve_path(path_a, &all_roots, &cwd)?;
    let prefix_b = resolve_path(path_b, &all_roots, &cwd)?;

    // Query sources in each scope
    let (sources_a, unhashed_a) =
        get_sources_in_scope(conn, &prefix_a, &filters, &options.include)?;
    let (sources_b, unhashed_b) =
        get_sources_in_scope(conn, &prefix_b, &filters, &options.include)?;

    // Build object_id sets
    let objects_a: HashSet<i64> = sources_a.keys().copied().collect();
    let objects_b: HashSet<i64> = sources_b.keys().copied().collect();

    // Compute differences
    let in_both: HashSet<i64> = objects_a.intersection(&objects_b).copied().collect();
    let only_in_a: HashSet<i64> = objects_a.difference(&objects_b).copied().collect();
    let only_in_b: HashSet<i64> = objects_b.difference(&objects_a).copied().collect();

    // Print header
    println!("Comparing:");
    println!("  A: {prefix_a}");
    println!("  B: {prefix_b}");
    if options.include.includes_excluded() {
        println!("  [including excluded]");
    }
    println!();

    // Report unhashed files
    let total_unhashed = unhashed_a + unhashed_b;
    if total_unhashed > 0 {
        eprintln!(
            "Skipped {total_unhashed} unhashed files (use `canon worklist` to hash them)"
        );
        eprintln!();
    }

    // Check if identical
    let is_identical = only_in_a.is_empty() && only_in_b.is_empty();

    // Print summary (always show all lines, even if count is 0)
    println!("Files in both (by content): {}", in_both.len());

    // Print only in A
    println!("Only in A: {}", only_in_a.len());
    if options.verbose && !only_in_a.is_empty() {
        let mut paths: Vec<&str> = only_in_a
            .iter()
            .filter_map(|oid| sources_a.get(oid))
            .map(|s| s.as_str())
            .collect();
        paths.sort();
        for path in paths {
            println!("  {path}");
        }
    }

    // Print only in B
    println!("Only in B: {}", only_in_b.len());
    if options.verbose && !only_in_b.is_empty() {
        let mut paths: Vec<&str> = only_in_b
            .iter()
            .filter_map(|oid| sources_b.get(oid))
            .map(|s| s.as_str())
            .collect();
        paths.sort();
        for path in paths {
            println!("  {path}");
        }
    }

    Ok(is_identical)
}

/// Get sources in scope, returns (object_id -> path map, unhashed count)
///
/// Note: compare does NOT filter by role. When comparing folders, all sources
/// are included regardless of whether root is "source" or "archive".
fn get_sources_in_scope(
    conn: &mut crate::repo::Connection,
    scope_prefix: &str,
    filters: &[Filter],
    include: &IncludeSet,
) -> Result<(HashMap<i64, String>, usize)> {
    // Classify the scope
    let scopes = ScopeMatch::classify_all(&[scope_prefix.to_string()]);

    // Get all root IDs
    let root_ids: Vec<i64> = conn
        .prepare("SELECT id FROM roots")?
        .query_map([], |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()?;

    // Fetch all present sources
    let all_sources = repo::source::batch_fetch_by_roots(conn, &root_ids)?;

    // Filter using domain predicates
    // Note: No role filtering - compare works across all root types
    let filtered: Vec<Source> = all_sources
        .into_iter()
        .filter(|s| s.is_active())
        .filter(|s| s.matches_scope(&scopes))
        .filter(|s| include.includes_excluded() || !s.is_excluded())
        .collect();

    // Apply --where filters if present
    let final_sources = if filters.is_empty() {
        filtered
    } else {
        let source_ids: Vec<i64> = filtered.iter().map(|s| s.id).collect();
        let filtered_ids: HashSet<i64> = filter::apply_filters(conn, &source_ids, filters)?
            .into_iter()
            .collect();
        filtered
            .into_iter()
            .filter(|s| filtered_ids.contains(&s.id))
            .collect()
    };

    // Build result map and count unhashed
    let mut result: HashMap<i64, String> = HashMap::new();
    let mut unhashed = 0;

    for source in final_sources {
        match source.object_id {
            Some(oid) => {
                // If multiple sources have same object_id, keep first path
                result.entry(oid).or_insert_with(|| source.path());
            }
            None => {
                unhashed += 1;
            }
        }
    }

    Ok((result, unhashed))
}
