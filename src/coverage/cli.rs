use anyhow::Result;

use crate::core::domain::format_count;
use crate::core::domain::root::DoorVerb;
use crate::core::domain::IncludeSet;
use crate::core::ops::scope::{classify_all, ResolvedScope};
use crate::core::repo::Db;
use crate::coverage::ops::{self, CoverageStats};
use crate::expr::{read_filters, ExpressionText};

pub fn run(
    db: &mut Db,
    scope_prefixes: &[String],
    filter_text: &ExpressionText,
    archive_spec: Option<&str>,
    include: &IncludeSet,
    compact: bool,
    scope: &ResolvedScope,
) -> Result<()> {
    let conn = db.conn();

    // Parse filters
    let filters = read_filters(conn, filter_text)?;

    let scopes = classify_all(scope_prefixes);

    let archive_target = ops::resolve_archive_root(conn, archive_spec)?;
    let archive_root_id = archive_target.as_ref().map(|t| t.root_id);
    // A parked archive is measured against and **marked**, never refused:
    // its copies still testify. The marker rides the header label so every
    // line beneath it is read in the right light.
    let archive_label: Option<String> = archive_target.as_ref().map(|target| {
        let spec = archive_spec.unwrap_or_default();
        match &target.parked {
            Some(_) => format!("{spec}{}", crate::core::domain::root::ROOT_SUSPENDED_MARKER),
            None => spec.to_string(),
        }
    });
    let archive_label: Option<&str> = archive_label.as_deref();

    // The compact renderers carry no header, so the marker riding the label
    // would go unsaid on the one output shape whose stdout must stay exactly
    // what was asked for. The channel bends, the statement never does: the
    // pause goes to stderr, in the verbless form a reading states.
    if compact {
        if let Some(parked) = archive_target.as_ref().and_then(|t| t.parked.as_ref()) {
            eprintln!("{}", parked.pause_line());
        }
    }

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
        let (stats, used_status, excluded_count) = ops::compute_scoped(
            conn,
            &scopes,
            &filters,
            archive_root_id,
            include,
            crate::user_zone(),
        )?;
        if compact {
            display_compact_scoped(&stats, scope);
        } else {
            display_scoped_stats(&stats, scope, archive_label);
        }
        (used_status, excluded_count)
    } else {
        // Per-root breakdown mode (global)
        let (per_root_stats, overall, used_status, excluded_count) =
            ops::compute_per_root(conn, &filters, archive_root_id, include, crate::user_zone())?;
        if compact {
            display_compact_per_root(&per_root_stats, &overall, scope);
        } else {
            display_per_root_stats(&per_root_stats, &overall, archive_label, scope);
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
    // Compact is one line on stdout by design and carries no scope header,
    // so the boundary's set-asides go to stderr rather than going unsaid.
    crate::scope::eprint_scope_set_asides(scope, DoorVerb::SetAside);
    let stdout = std::io::stdout();
    let mut handle = stdout.lock();
    let _ = print_compact_line(&mut handle, &label, stats, true);
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
        if print_compact_line(&mut handle, &label, stats, first).is_err() {
            return;
        }
        first = false;
    }

    // Overall summary if multiple roots
    if per_root.len() > 1 && overall.total_sources > 0 {
        let _ = print_compact_line(&mut handle, "(total)", overall, false);
    }
}

fn format_compact_label(id: &str, path: &str) -> String {
    const MAX_PATH_LEN: usize = 35;
    let id_prefix = format!("id:{id:<2}");
    format!(
        "{id_prefix} {}",
        crate::core::domain::format::cap_path(path, MAX_PATH_LEN)
    )
}

fn print_compact_line<W: std::io::Write>(
    handle: &mut W,
    label: &str,
    stats: &CoverageStats,
    show_legend: bool,
) -> std::io::Result<()> {
    let sources = stats.total_sources;
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

/// What the archived line adds when part of the count rests solely behind a
/// closed door — nothing at all when it does not.
///
/// The registry's testifying clause, at the surface a user reads before
/// deciding a drive can go: parking never un-archives, so the number is
/// unchanged and the qualifier trails it rather than replacing it. A
/// one-consumer, screen-only composition, so it stays here rather than
/// behind an ops method — `stats.archived_only_parked`/`stats.archive_roots`
/// are kept beside each other for exactly this call.
fn only_parked_suffix(stats: &CoverageStats) -> String {
    stats
        .archived_only_parked
        .bulleted_clause(&stats.archive_roots)
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

    // Every source the selection put in view: excluded ones only under
    // `--include excluded`.
    println!(
        "  Total sources:   {:>8}",
        format_count(stats.total_sources)
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
    let label = if archive.is_some() {
        "In this archive:"
    } else {
        "Archived:"
    };
    println!(
        "  {label:<16} {:>8} ({:.1}% of {} with content){}",
        format_count(stats.archived_sources),
        stats.archived_pct(),
        format_count(stats.coverable_sources()),
        only_parked_suffix(stats),
    );
    if archive.is_some() {
        println!("  Not in archive:  {:>8}", format_count(stats.unarchived()));
    } else {
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
            format_count(stats.total_sources)
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
        let label = if archive.is_some() {
            "In this archive:"
        } else {
            "Archived:"
        };
        {
            if writeln!(
                handle,
                "  {label:<16} {:>8} ({:.1}% of {} with content){}",
                format_count(stats.archived_sources),
                stats.archived_pct(),
                format_count(stats.coverable_sources()),
                only_parked_suffix(stats),
            )
            .is_err()
            {
                return;
            }
            if writeln!(
                handle,
                "  {:<16} {:>8}",
                if archive.is_some() {
                    "Not in archive:"
                } else {
                    "Unarchived:"
                },
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
        format_count(overall.total_sources)
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
    let label = if archive.is_some() {
        "In this archive:"
    } else {
        "Archived:"
    };
    let _ = writeln!(
        handle,
        "  {label:<16} {:>8} ({:.1}% of {} with content){}",
        format_count(overall.archived_sources),
        overall.archived_pct(),
        format_count(overall.coverable_sources()),
        only_parked_suffix(overall),
    );
    let _ = writeln!(
        handle,
        "  {:<16} {:>8}",
        if archive.is_some() {
            "Not in archive:"
        } else {
            "Unarchived:"
        },
        format_count(overall.unarchived())
    );
}
