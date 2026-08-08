use anyhow::Result;

use crate::domain::format_count;
use crate::domain::scope::ScopeMatch;
use crate::domain::IncludeSet;
use crate::expr::filter::Filter;
use crate::ops;
use crate::ops::coverage::CoverageStats;
use crate::ops::scope::{parse_root_spec, ResolvedScope};
use crate::repo::{self, Db};

pub fn run(
    db: &mut Db,
    scope_prefixes: &[String],
    filter_strs: &[String],
    archive_spec: Option<&str>,
    include: &IncludeSet,
    compact: bool,
    scope: &ResolvedScope,
) -> Result<()> {
    let conn = db.conn();

    // Parse filters
    let filters: Vec<Filter> = filter_strs
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    // Fetch all roots for spec resolution
    let roots = repo::root::fetch_all(conn)?;

    let scopes = ScopeMatch::classify_all(scope_prefixes);

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
    let (used_status, excluded_count) = if !scope_prefixes.is_empty() {
        // Scoped mode
        let (stats, used_status, excluded_count) =
            ops::coverage::compute_scoped(conn, &scopes, &filters, archive_root_id, include)?;
        if compact {
            display_compact_scoped(&stats, scope);
        } else {
            display_scoped_stats(&stats, scope, archive_spec);
        }
        (used_status, excluded_count)
    } else {
        // Per-root breakdown mode (global)
        let (per_root_stats, overall, used_status, excluded_count) =
            ops::coverage::compute_per_root(conn, &filters, archive_root_id, include)?;
        if compact {
            display_compact_per_root(&per_root_stats, &overall, scope);
        } else {
            display_per_root_stats(&per_root_stats, &overall, archive_spec, scope);
        }
        (used_status, excluded_count)
    };

    if used_status.excluded && !include.includes_excluded() && excluded_count > 0 {
        eprintln!(
            "({} excluded sources hidden, use --include excluded to show)",
            excluded_count
        );
    }

    Ok(())
}

fn display_compact_scoped(stats: &CoverageStats, scope: &ResolvedScope) {
    let label = if scope.prefixes.len() == 1 {
        scope.prefixes[0].clone()
    } else {
        "(scoped)".to_string()
    };
    print_compact_line(&label, stats, true);
}

fn display_compact_per_root(
    per_root: &[CoverageStats],
    overall: &CoverageStats,
    scope: &ResolvedScope,
) {
    let stdout = std::io::stdout();
    let mut handle = stdout.lock();
    crate::scope::print_report_scope(&mut handle, "Coverage", scope);
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
        if print_compact_line_handle(&mut handle, &label, stats, first).is_err() {
            return;
        }
        first = false;
    }

    // Overall summary if multiple roots
    if per_root.len() > 1 && overall.total_sources > 0 {
        let _ = print_compact_line_handle(&mut handle, "(total)", overall, false);
    }
}

fn format_compact_label(id: &str, path: &str) -> String {
    const MAX_PATH_LEN: usize = 35;
    let id_prefix = format!("id:{id:<2}");
    format!(
        "{id_prefix} {}",
        crate::domain::format::cap_path(path, MAX_PATH_LEN)
    )
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
        format_count(sources),
        hashed_pct,
        archived_pct,
        legend
    );
}

fn print_compact_line_handle<W: std::io::Write>(
    handle: &mut W,
    label: &str,
    stats: &CoverageStats,
    show_legend: bool,
) -> std::io::Result<()> {
    let sources = stats.included_sources();
    let hashed_pct = stats.hashed_pct();
    let archived_pct = stats.archived_pct();

    let legend = if show_legend {
        "  (sources/hashed/archived)"
    } else {
        ""
    };

    writeln!(
        handle,
        "{:<42} {:>10}/{:>5.1}%/{:>5.1}%{}",
        label,
        format_count(sources),
        hashed_pct,
        archived_pct,
        legend
    )
}

fn display_scoped_stats(stats: &CoverageStats, scope: &ResolvedScope, archive: Option<&str>) {
    {
        let stdout = std::io::stdout();
        let mut handle = stdout.lock();
        crate::scope::print_report_scope(&mut handle, "Coverage", scope);
    }

    if let Some(arch) = archive {
        println!("Archive: {arch}\n");
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
        format_count(stats.included_sources())
    );
    println!(
        "  Hashed:          {:>8} ({:.1}%)",
        format_count(stats.hashed_sources),
        stats.hashed_pct()
    );

    if stats.contentless_sources > 0 {
        // The contentless law: empty files are all shape, no content —
        // they can never be archived-by-identity, so they leave both the
        // percentage's denominator and the unarchived remainder.
        println!(
            "  Empty files:     {:>8} (no content to cover)",
            format_count(stats.contentless_sources)
        );
    }
    if archive.is_some() {
        println!(
            "  In this archive: {:>8} ({:.1}% of {} with content)",
            format_count(stats.archived_sources),
            stats.archived_pct(),
            format_count(stats.coverable_sources())
        );
        println!("  Not in archive:  {:>8}", format_count(stats.unarchived()));
    } else {
        println!(
            "  Archived:        {:>8} ({:.1}% of {} with content)",
            format_count(stats.archived_sources),
            stats.archived_pct(),
            format_count(stats.coverable_sources())
        );
        println!("  Unarchived:      {:>8}", format_count(stats.unarchived()));
    }
}

fn display_per_root_stats(
    per_root: &[CoverageStats],
    overall: &CoverageStats,
    archive: Option<&str>,
    scope: &ResolvedScope,
) {
    use std::io::Write;
    let stdout = std::io::stdout();
    let mut handle = stdout.lock();

    crate::scope::print_report_scope(&mut handle, "Coverage", scope);
    if let Some(arch) = archive {
        let _ = writeln!(handle, "Archive: {arch}\n");
    } else {
        let _ = writeln!(handle);
    }

    if per_root.is_empty() || overall.total_sources == 0 {
        let _ = writeln!(handle, "No sources match the given filters.");
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
        if writeln!(handle, "Root {root_id}: {root_path} ({root_role})").is_err() {
            return;
        }

        if writeln!(
            handle,
            "  Total sources:   {:>8}",
            format_count(stats.included_sources())
        )
        .is_err()
        {
            return;
        }
        if writeln!(
            handle,
            "  Hashed:          {:>8} ({:.1}%)",
            format_count(stats.hashed_sources),
            stats.hashed_pct()
        )
        .is_err()
        {
            return;
        }

        if stats.contentless_sources > 0
            && writeln!(
                handle,
                "  Empty files:     {:>8} (no content to cover)",
                format_count(stats.contentless_sources)
            )
            .is_err()
        {
            return;
        }
        if archive.is_some() {
            if writeln!(
                handle,
                "  In this archive: {:>8} ({:.1}% of {} with content)",
                format_count(stats.archived_sources),
                stats.archived_pct(),
                format_count(stats.coverable_sources())
            )
            .is_err()
            {
                return;
            }
            if writeln!(
                handle,
                "  Not in archive:  {:>8}",
                format_count(stats.unarchived())
            )
            .is_err()
            {
                return;
            }
        } else {
            if writeln!(
                handle,
                "  Archived:        {:>8} ({:.1}% of {} with content)",
                format_count(stats.archived_sources),
                stats.archived_pct(),
                format_count(stats.coverable_sources())
            )
            .is_err()
            {
                return;
            }
            if writeln!(
                handle,
                "  Unarchived:      {:>8}",
                format_count(stats.unarchived())
            )
            .is_err()
            {
                return;
            }
        }
        if writeln!(handle).is_err() {
            return;
        }
    }

    // Overall summary
    if writeln!(handle, "{}", "─".repeat(40)).is_err() {
        return;
    }
    if writeln!(handle, "Overall:").is_err() {
        return;
    }

    let _ = writeln!(
        handle,
        "  Total sources:   {:>8}",
        format_count(overall.included_sources())
    );
    let _ = writeln!(
        handle,
        "  Hashed:          {:>8} ({:.1}%)",
        format_count(overall.hashed_sources),
        overall.hashed_pct()
    );

    if overall.contentless_sources > 0 {
        let _ = writeln!(
            handle,
            "  Empty files:     {:>8} (no content to cover)",
            format_count(overall.contentless_sources)
        );
    }
    if archive.is_some() {
        let _ = writeln!(
            handle,
            "  In this archive: {:>8} ({:.1}% of {} with content)",
            format_count(overall.archived_sources),
            overall.archived_pct(),
            format_count(overall.coverable_sources())
        );
        let _ = writeln!(
            handle,
            "  Not in archive:  {:>8}",
            format_count(overall.unarchived())
        );
    } else {
        let _ = writeln!(
            handle,
            "  Archived:        {:>8} ({:.1}% of {} with content)",
            format_count(overall.archived_sources),
            overall.archived_pct(),
            format_count(overall.coverable_sources())
        );
        let _ = writeln!(
            handle,
            "  Unarchived:      {:>8}",
            format_count(overall.unarchived())
        );
    }
}
