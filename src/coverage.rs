use anyhow::Result;
use std::path::PathBuf;

use crate::domain::path::resolve_paths;
use crate::domain::root::parse_root_spec;
use crate::domain::scope::ScopeMatch;
use crate::domain::IncludeSet;
use crate::expr::filter::Filter;
use crate::ops;
use crate::ops::coverage::CoverageStats;
use crate::repo::{self, Db};

pub fn run(
    db: &mut Db,
    scope_paths: &[PathBuf],
    filter_strs: &[String],
    archive_spec: Option<&str>,
    include: &IncludeSet,
    compact: bool,
) -> Result<()> {
    let conn = db.conn();

    // Parse filters
    let filters: Vec<Filter> = filter_strs
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    // Fetch all roots for path and spec resolution
    let roots = repo::root::fetch_all(conn)?;

    // Resolve scope paths (soft resolution: matches known roots, falls back to fs)
    let scope_prefixes = resolve_paths(scope_paths, &roots)?;
    let scopes = ScopeMatch::classify_all(&scope_prefixes);

    // Parse and validate archive spec (must be archive role)
    let archive_root_id = if let Some(spec) = archive_spec {
        Some(parse_root_spec(&roots, spec, Some("archive"))?)
    } else {
        None
    };

    // Get mutable reference for operations
    let conn = db.conn_mut();

    // Show annotation when include changes the source set
    if include.is_expanded() {
        let mut parts = Vec::new();
        if include.includes_excluded() {
            parts.push("excluded");
        }
        if include.includes_archived() {
            parts.push("archived");
        }
        eprintln!("[including {}]", parts.join(", "));
    }

    // Compute and display stats
    if !scope_prefixes.is_empty() {
        // Scoped mode
        let stats = ops::coverage::compute_scoped(conn, &scopes, &filters, archive_root_id, include)?;
        let scope_display = if scope_prefixes.len() == 1 {
            Some(scope_prefixes[0].as_str())
        } else {
            None
        };
        if compact {
            display_compact_scoped(&stats, scope_display);
        } else {
            display_scoped_stats(&stats, scope_display, archive_spec);
        }
    } else {
        // Per-root breakdown mode
        let (per_root_stats, overall) =
            ops::coverage::compute_per_root(conn, &filters, archive_root_id, include)?;
        if compact {
            display_compact_per_root(&per_root_stats, &overall);
        } else {
            display_per_root_stats(&per_root_stats, &overall, archive_spec);
        }
    }

    Ok(())
}

fn display_compact_scoped(stats: &CoverageStats, scope: Option<&str>) {
    let label = scope.unwrap_or("(all)");
    print_compact_line(label, stats, true);
}

fn display_compact_per_root(per_root: &[CoverageStats], overall: &CoverageStats) {
    let mut first = true;
    for stats in per_root {
        if stats.total_sources == 0 {
            continue;
        }
        let id = stats
            .root_id
            .map(|i| i.to_string())
            .unwrap_or_else(|| "?".to_string());
        let path = stats.root_path.as_deref().unwrap_or("unknown");
        let label = format_compact_label(&id, path);
        print_compact_line(&label, stats, first);
        first = false;
    }

    // Overall summary if multiple roots
    if per_root.len() > 1 && overall.total_sources > 0 {
        print_compact_line("(total)", overall, false);
    }
}

fn format_compact_label(id: &str, path: &str) -> String {
    const MAX_PATH_LEN: usize = 35;
    let id_prefix = format!("id:{id:<2}");

    if path.len() <= MAX_PATH_LEN {
        format!("{id_prefix} {path}")
    } else {
        // Show ...last_n_chars
        let truncated = &path[path.len() - MAX_PATH_LEN + 3..];
        format!("{id_prefix} ...{truncated}")
    }
}

fn print_compact_line(label: &str, stats: &CoverageStats, show_legend: bool) {
    let sources = stats.included_sources();
    let hashed_pct = stats.hashed_pct();
    let archived_pct = stats.archived_pct();

    let legend = if show_legend {
        "  (sources/hashed/archived)"
    } else {
        ""
    };

    println!(
        "{:<42} {:>10}/{:>5.1}%/{:>5.1}%{}",
        label,
        format_number(sources),
        hashed_pct,
        archived_pct,
        legend
    );
}

fn display_scoped_stats(stats: &CoverageStats, scope: Option<&str>, archive: Option<&str>) {
    if let Some(arch) = archive {
        println!("Archive Coverage (relative to {arch})");
    } else {
        println!("Archive Coverage");
    }

    if let Some(s) = scope {
        println!("Scope: {s}\n");
    } else {
        println!();
    }

    if stats.total_sources == 0 {
        println!("No sources match the given filters.");
        return;
    }

    // Always show included sources as total (excluded are filtered out conceptually)
    println!(
        "  Total sources:   {:>8}",
        format_number(stats.included_sources())
    );
    println!(
        "  Hashed:          {:>8} ({:.1}%)",
        format_number(stats.hashed_sources),
        stats.hashed_pct()
    );

    if archive.is_some() {
        println!(
            "  In this archive: {:>8} ({:.1}% of hashed)",
            format_number(stats.archived_sources),
            stats.archived_pct()
        );
        println!(
            "  Not in archive:  {:>8}",
            format_number(stats.unarchived())
        );
    } else {
        println!(
            "  Archived:        {:>8} ({:.1}% of hashed)",
            format_number(stats.archived_sources),
            stats.archived_pct()
        );
        println!(
            "  Unarchived:      {:>8}",
            format_number(stats.unarchived())
        );
    }
}

fn display_per_root_stats(
    per_root: &[CoverageStats],
    overall: &CoverageStats,
    archive: Option<&str>,
) {
    if let Some(arch) = archive {
        println!("Archive Coverage Report (relative to {arch})\n");
    } else {
        println!("Archive Coverage Report\n");
    }

    if per_root.is_empty() || overall.total_sources == 0 {
        println!("No sources match the given filters.");
        return;
    }

    for stats in per_root {
        if stats.total_sources == 0 {
            continue;
        }

        let root_id = stats
            .root_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "?".to_string());
        let root_path = stats.root_path.as_deref().unwrap_or("unknown");
        let root_role = stats.root_role.as_deref().unwrap_or("unknown");
        println!("Root {root_id}: {root_path} ({root_role})");

        println!(
            "  Total sources:   {:>8}",
            format_number(stats.included_sources())
        );
        println!(
            "  Hashed:          {:>8} ({:.1}%)",
            format_number(stats.hashed_sources),
            stats.hashed_pct()
        );

        if archive.is_some() {
            println!(
                "  In this archive: {:>8} ({:.1}% of hashed)",
                format_number(stats.archived_sources),
                stats.archived_pct()
            );
            println!(
                "  Not in archive:  {:>8}",
                format_number(stats.unarchived())
            );
        } else {
            println!(
                "  Archived:        {:>8} ({:.1}% of hashed)",
                format_number(stats.archived_sources),
                stats.archived_pct()
            );
            println!(
                "  Unarchived:      {:>8}",
                format_number(stats.unarchived())
            );
        }
        println!();
    }

    // Overall summary
    println!("{}", "─".repeat(40));
    println!("Overall:");

    println!(
        "  Total sources:   {:>8}",
        format_number(overall.included_sources())
    );
    println!(
        "  Hashed:          {:>8} ({:.1}%)",
        format_number(overall.hashed_sources),
        overall.hashed_pct()
    );

    if archive.is_some() {
        println!(
            "  In this archive: {:>8} ({:.1}% of hashed)",
            format_number(overall.archived_sources),
            overall.archived_pct()
        );
        println!(
            "  Not in archive:  {:>8}",
            format_number(overall.unarchived())
        );
    } else {
        println!(
            "  Archived:        {:>8} ({:.1}% of hashed)",
            format_number(overall.archived_sources),
            overall.archived_pct()
        );
        println!(
            "  Unarchived:      {:>8}",
            format_number(overall.unarchived())
        );
    }
}

fn format_number(n: i64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

