use anyhow::Result;
use chrono::{TimeZone, Utc};

use crate::core::domain::source::Source;
use crate::core::domain::IncludeSet;
use crate::core::ops::scope::classify_all;
use crate::core::repo::Db;
use crate::expr::Filter;
use crate::expr::{select_sources, RolePolicy, SelectionParams};

#[allow(clippy::too_many_arguments)]
pub fn run(
    db: &mut Db,
    scope_prefixes: &[String],
    filter_strs: &[String],
    include: &IncludeSet,
    use_relative_paths: bool,
    long_format: bool,
    sort_by: &str,
    reverse: bool,
    null_delim: bool,
) -> Result<()> {
    let conn = db.conn_mut();

    // Validate sort option
    if !matches!(sort_by, "path" | "size" | "mtime" | "name") {
        anyhow::bail!("Invalid sort option '{sort_by}'. Valid options: path, size, mtime, name");
    }

    // Parse filters
    let filters: Vec<Filter> = filter_strs
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    let scopes = classify_all(scope_prefixes);

    // Get cwd for relative path display (must be canonicalized to match DB paths)
    let cwd = if use_relative_paths {
        std::env::current_dir()
            .ok()
            .and_then(|p| std::fs::canonicalize(p).ok())
            .and_then(|p| p.to_str().map(String::from))
    } else {
        None
    };

    let params = SelectionParams {
        scopes,
        include: include.clone(),
        filters,
        role_policy: RolePolicy::SourceUnlessIncluded,
    };
    let sel = select_sources(conn, &params)?;

    if sel.sources.is_empty() {
        eprintln!("No sources match the given filters.");
        if sel.excluded_count > 0 {
            eprintln!(
                "({} excluded sources hidden, use --include excluded to show)",
                sel.excluded_count
            );
        }
        return Ok(());
    }

    // Determine whether to show status column
    let show_status = include.is_expanded();

    // Build output lines (formatting stays here)
    let mut output_lines: Vec<(String, i64, i64, String)> = sel
        .sources
        .iter()
        .map(|s| {
            let formatted = format_path(&s.path(), cwd.as_deref());
            let status = status_indicator(s);
            (formatted, s.size, s.mtime, status.to_string())
        })
        .collect();

    // Sort output (all ascending by default, -r reverses)
    match sort_by {
        "path" => output_lines.sort_by(|a, b| a.0.cmp(&b.0)),
        "size" => output_lines.sort_by(|a, b| a.1.cmp(&b.1)),
        "mtime" => output_lines.sort_by(|a, b| a.2.cmp(&b.2)),
        "name" => output_lines.sort_by(|a, b| {
            let name_a = a.0.rsplit('/').next().unwrap_or(&a.0);
            let name_b = b.0.rsplit('/').next().unwrap_or(&b.0);
            name_a.cmp(name_b)
        }),
        _ => {} // Already validated above
    }
    if reverse {
        output_lines.reverse();
    }

    // Print output (to stdout for pipe-friendliness)
    use std::io::Write;
    let stdout = std::io::stdout();
    let mut handle = stdout.lock();
    let line_end = if null_delim { "\0" } else { "\n" };
    for (source_path, size, mtime, status) in &output_lines {
        let line = if long_format {
            let size_str = format_size(*size);
            let date_str = format_date(*mtime);
            if show_status {
                format!("{status}{size_str:>8}  {date_str}  {source_path}{line_end}")
            } else {
                format!("{size_str:>8}  {date_str}  {source_path}{line_end}")
            }
        } else {
            format!("{source_path}{line_end}")
        };
        // Ignore broken pipe errors when stdout is closed
        if write!(handle, "{}", line).is_err() {
            break;
        }
    }

    // Print footer to stderr
    let source_count = output_lines.len();
    let mut footer_parts = vec![format!("{} sources", source_count)];
    if sel.excluded_count > 0 {
        footer_parts.push(format!("{} excluded hidden", sel.excluded_count));
    }

    if footer_parts.len() > 1 {
        eprintln!("{} ({})", footer_parts[0], footer_parts[1..].join(", "));
    } else {
        eprintln!("{}", footer_parts[0]);
    }

    Ok(())
}

fn format_path(full_path: &str, cwd: Option<&str>) -> String {
    crate::core::domain::path::format_path(full_path, cwd)
}

fn format_size(bytes: i64) -> String {
    const KB: i64 = 1024;
    const MB: i64 = 1024 * KB;
    const GB: i64 = 1024 * MB;

    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}

fn format_date(unix_timestamp: i64) -> String {
    Utc.timestamp_opt(unix_timestamp, 0)
        .single()
        .map(|dt| dt.format("%b %e %Y").to_string())
        .unwrap_or_else(|| "???".to_string())
}

/// Status indicator for long format output.
/// E = source-level excluded, X = object-level excluded, A = archive source.
fn status_indicator(source: &Source) -> &'static str {
    if source.excluded {
        " E"
    } else if source.object_excluded == Some(true) {
        " X"
    } else if source.is_from_role("archive") {
        " A"
    } else {
        "  "
    }
}

/// Show sources with duplicate content, grouped by hash
pub fn show_duplicates(
    db: &mut Db,
    scope_prefixes: &[String],
    filter_strs: &[String],
    include: &IncludeSet,
    use_relative_paths: bool,
) -> Result<()> {
    let conn = db.conn_mut();

    // Parse filters
    let filters: Vec<Filter> = filter_strs
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    let scopes = classify_all(scope_prefixes);

    // Get cwd for relative path display (must be canonicalized to match DB paths)
    let cwd = if use_relative_paths {
        std::env::current_dir()
            .ok()
            .and_then(|p| std::fs::canonicalize(p).ok())
            .and_then(|p| p.to_str().map(String::from))
    } else {
        None
    };

    let params = SelectionParams {
        scopes,
        include: include.clone(),
        filters,
        role_policy: RolePolicy::SourceUnlessIncluded,
    };
    let sel = select_sources(conn, &params)?;

    if sel.sources.is_empty() {
        eprintln!("No sources match the given filters.");
        if sel.excluded_count > 0 {
            eprintln!(
                "({} excluded sources hidden, use --include excluded to show)",
                sel.excluded_count
            );
        }
        return Ok(());
    }

    // Extract source IDs for duplicate finding
    let source_ids = sel.source_ids();

    // Find duplicate groups: object_ids that appear more than once
    let duplicate_groups = crate::ls::ops::find_duplicate_groups(conn, &source_ids)?;

    if duplicate_groups.is_empty() {
        println!("No duplicates found.");
        if sel.excluded_count > 0 {
            eprintln!(
                "({} excluded sources hidden, use --include excluded to show)",
                sel.excluded_count
            );
        }
        return Ok(());
    }

    // Print each duplicate group
    use std::io::Write;
    let stdout = std::io::stdout();
    let mut handle = stdout.lock();
    let mut total_sources = 0usize;
    for group in &duplicate_groups {
        let short_hash = if group.hash_value.len() > 12 {
            &group.hash_value[..12]
        } else {
            &group.hash_value
        };
        let size_str = format_size(group.total_size);
        if writeln!(
            handle,
            "[{}...] {} sources, {}:",
            short_hash,
            group.sources.len(),
            size_str
        )
        .is_err()
        {
            break;
        }
        for src in &group.sources {
            let display_path = format_path(&src.path, cwd.as_deref());
            if writeln!(handle, "  {display_path} (id: {})", src.source_id).is_err() {
                break;
            }
        }
        if writeln!(handle).is_err() {
            break;
        }
        total_sources += group.sources.len();
    }

    // Summary
    let _ = writeln!(
        handle,
        "Found {} duplicate groups ({} sources)",
        duplicate_groups.len(),
        total_sources
    );
    if sel.excluded_count > 0 {
        eprintln!(
            "({} excluded sources hidden, use --include excluded to show)",
            sel.excluded_count
        );
    }

    Ok(())
}
