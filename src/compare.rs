use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::path::Path;

use crate::domain::path::resolve_path;
use crate::domain::scope::ScopeMatch;
use crate::domain::IncludeSet;
use crate::expr::filter::Filter;
use crate::ops::selection::{self, RolePolicy, SelectionParams};
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
        select_and_build_map(conn, &prefix_a, &filters, &options.include)?;
    let (sources_b, unhashed_b) =
        select_and_build_map(conn, &prefix_b, &filters, &options.include)?;

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
        eprintln!("Skipped {total_unhashed} unhashed files (use `canon worklist` to hash them)");
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

/// Select sources in scope and build an object_id → path map for content comparison.
///
/// Returns (object_id -> path map, unhashed count). Compare uses AnyRole — all
/// sources are included regardless of whether root is "source" or "archive".
fn select_and_build_map(
    conn: &mut crate::repo::Connection,
    scope_prefix: &str,
    filters: &[Filter],
    include: &IncludeSet,
) -> Result<(HashMap<i64, String>, usize)> {
    let scopes = ScopeMatch::classify_all(&[scope_prefix.to_string()]);
    let params = SelectionParams {
        scopes,
        include: include.clone(),
        filters: filters.to_vec(),
        role_policy: RolePolicy::AnyRole,
    };
    let sel = selection::select_sources(conn, &params)?;

    // Build object_id → path map, counting unhashed sources
    let mut result: HashMap<i64, String> = HashMap::new();
    let mut unhashed = 0;

    for source in &sel.sources {
        match source.object_id {
            Some(oid) => {
                result.entry(oid).or_insert_with(|| source.path());
            }
            None => {
                unhashed += 1;
            }
        }
    }

    Ok((result, unhashed))
}
