use anyhow::{bail, Context, Result};
use rusqlite::OptionalExtension;
use std::collections::{HashMap, HashSet};
use std::fs::{self, Metadata};
use std::path::{Path, PathBuf};

use crate::cluster::{Manifest, ManifestSource};
use crate::db::{parse_root_spec, Connection, Db};
use crate::exclude;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferMode {
    Copy,   // Default: copy only, source remains
    Rename, // Unix only, error if cross-device
    Move,   // Try rename, fallback to copy+delete on EXDEV (requires --yes)
}

#[derive(Default)]
struct ApplyStats {
    copied: u64,
    renamed: u64,
    moved: u64,
    skipped_missing: u64,
    skipped_filtered: u64,
    errors: u64,
}

pub struct ApplyOptions {
    pub dry_run: bool,
    pub allow_cross_archive_duplicates: bool,
    pub roots: Vec<String>,
    pub transfer_mode: TransferMode,
}

pub fn run(db: &Db, manifest_path: &Path, options: &ApplyOptions) -> Result<()> {
    // Platform checks: --rename and --move are Unix-only
    #[cfg(not(unix))]
    if options.transfer_mode == TransferMode::Rename || options.transfer_mode == TransferMode::Move {
        bail!("--rename and --move are only supported on Unix platforms");
    }

    // Metadata preservation warning for Copy mode on non-Unix
    #[cfg(not(unix))]
    if options.transfer_mode == TransferMode::Copy {
        eprintln!("Note: mtime/permissions preservation not available on this platform");
    }

    let content = fs::read_to_string(manifest_path)
        .with_context(|| format!("Failed to read manifest: {}", manifest_path.display()))?;

    let manifest: Manifest = toml::from_str(&content)
        .with_context(|| format!("Failed to parse manifest: {}", manifest_path.display()))?;

    let base_dir = fs::canonicalize(&manifest.output.base_dir).unwrap_or_else(|_| {
        PathBuf::from(&manifest.output.base_dir)
    });

    let conn = db.conn();

    // Filter sources by root if specified
    let filtered_sources = filter_by_roots(&manifest, &options.roots, conn)?;
    let skipped_by_filter = manifest.sources.len() - filtered_sources.len();

    // Pre-flight checks (mandatory, always run)
    // Check destination uniqueness first
    let collisions = check_destination_collisions_filtered(&filtered_sources, &manifest.output.pattern, &base_dir)?;
    if !collisions.is_empty() {
        eprintln!(
            "Error: {} destination paths have multiple sources:",
            collisions.len()
        );
        for (dest, sources) in &collisions {
            eprintln!("  {} <- {} files:", dest.display(), sources.len());
            for src in sources {
                eprintln!("    {}", src);
            }
        }
        bail!("Aborting due to destination collisions");
    }

    // Check archive conflicts
    let conflicts = check_archive_conflicts_filtered(conn, &filtered_sources, &base_dir)?;

    if !conflicts.in_dest_archive.is_empty() {
        eprintln!(
            "Error: {} files already exist in destination archive:",
            conflicts.in_dest_archive.len()
        );
        for (src, dst) in &conflicts.in_dest_archive {
            eprintln!("  {} -> {}", src, dst);
        }
        bail!("Aborting due to files already in destination archive");
    }

    if !conflicts.in_other_archives.is_empty() && !options.allow_cross_archive_duplicates {
        eprintln!(
            "Error: {} files already exist in other archive(s):",
            conflicts.in_other_archives.len()
        );
        for (src, dst) in &conflicts.in_other_archives {
            eprintln!("  {} -> {}", src, dst);
        }
        eprintln!("\nUse --allow-cross-archive-duplicates to copy anyway");
        bail!("Aborting due to files already in other archives");
    }

    // Defense-in-depth: Check for excluded sources in manifest (hard gate, no override)
    // This should never happen if the manifest was generated correctly,
    // but we check anyway to prevent accidentally copying excluded files
    {
        let excluded_sources = check_excluded_sources_filtered(conn, &filtered_sources)?;
        if !excluded_sources.is_empty() {
            eprintln!(
                "Error: {} sources in manifest are marked as excluded:",
                excluded_sources.len()
            );
            for (id, path) in &excluded_sources {
                eprintln!("  {} (id: {})", path, id);
            }
            eprintln!("\nExcluded sources cannot be applied. Regenerate the manifest after clearing exclusions.");
            bail!("Aborting due to excluded sources in manifest");
        }
    }

    let mut stats = ApplyStats {
        skipped_filtered: skipped_by_filter as u64,
        ..Default::default()
    };

    for source in &filtered_sources {
        match process_source(source, &manifest.output.pattern, &base_dir, options) {
            Ok(action) => match action {
                ApplyAction::Copied => stats.copied += 1,
                ApplyAction::Renamed => stats.renamed += 1,
                ApplyAction::Moved => stats.moved += 1,
                ApplyAction::SkippedMissing => stats.skipped_missing += 1,
            },
            Err(e) => {
                eprintln!("Error processing {}: {}", source.path, e);
                stats.errors += 1;
            }
        }
    }

    let mode = if options.dry_run { " (dry-run)" } else { "" };
    println!(
        "Applied{}: {} copied, {} renamed, {} moved, {} skipped (missing), {} skipped (filtered), {} errors",
        mode, stats.copied, stats.renamed, stats.moved, stats.skipped_missing, stats.skipped_filtered, stats.errors
    );

    Ok(())
}

struct ArchiveConflicts {
    in_dest_archive: Vec<(String, String)>,   // (source_path, archive_path)
    in_other_archives: Vec<(String, String)>, // (source_path, archive_path)
}

fn find_archive_for_path(conn: &Connection, path: &Path) -> Result<Option<i64>> {
    let path_str = path.to_str().unwrap_or("");

    // Find archive roots and check if path is inside any of them
    let mut stmt = conn.prepare("SELECT id, path FROM roots WHERE role = 'archive'")?;
    let archives: Vec<(i64, String)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<Vec<_>, _>>()?;

    for (id, archive_path) in archives {
        if path_str.starts_with(&archive_path) || path_str == archive_path {
            return Ok(Some(id));
        }
    }

    Ok(None)
}

// ============================================================================
// Helper functions for pre-flight checks (work with filtered source list)
// ============================================================================

fn filter_by_roots<'a>(
    manifest: &'a Manifest,
    roots: &[String],
    conn: &Connection,
) -> Result<Vec<&'a ManifestSource>> {
    if roots.is_empty() {
        return Ok(manifest.sources.iter().collect());
    }

    let mut root_ids = HashSet::new();
    for spec in roots {
        let id = parse_root_spec(conn, spec, None)?;
        root_ids.insert(id);
    }

    Ok(manifest.sources.iter().filter(|s| root_ids.contains(&s.root_id)).collect())
}

fn check_destination_collisions_filtered(
    sources: &[&ManifestSource],
    pattern: &str,
    base_dir: &Path,
) -> Result<Vec<(PathBuf, Vec<String>)>> {
    let mut dest_to_sources: HashMap<PathBuf, Vec<String>> = HashMap::new();

    for source in sources {
        let src_path = Path::new(&source.path);

        // Skip sources that don't exist (they'll be skipped during copy anyway)
        if !src_path.exists() {
            continue;
        }

        // Expand pattern to get destination path
        let dest_rel = expand_pattern(pattern, source, src_path)?;
        let dest_path = base_dir.join(&dest_rel);

        dest_to_sources
            .entry(dest_path)
            .or_default()
            .push(source.path.clone());
    }

    // Filter to only collisions (more than one source per destination)
    let mut collisions: Vec<(PathBuf, Vec<String>)> = dest_to_sources
        .into_iter()
        .filter(|(_, sources)| sources.len() > 1)
        .collect();

    // Sort for consistent output
    collisions.sort_by(|a, b| a.0.cmp(&b.0));

    Ok(collisions)
}

fn check_archive_conflicts_filtered(
    conn: &Connection,
    sources: &[&ManifestSource],
    base_dir: &Path,
) -> Result<ArchiveConflicts> {
    let mut conflicts = ArchiveConflicts {
        in_dest_archive: Vec::new(),
        in_other_archives: Vec::new(),
    };

    // Find if base_dir is inside an archive root
    let dest_archive_id: Option<i64> = find_archive_for_path(conn, base_dir)?;

    for source in sources {
        if let Some(ref hash) = source.hash_value {
            // Check if this hash exists in any archive
            let archive_match: Option<(i64, String, String)> = conn
                .query_row(
                    "SELECT r.id, r.path, s.rel_path
                     FROM sources s
                     JOIN roots r ON s.root_id = r.id
                     JOIN objects o ON s.object_id = o.id
                     WHERE r.role = 'archive' AND o.hash_value = ? AND s.present = 1
                     LIMIT 1",
                    [hash],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()?;

            if let Some((archive_id, root_path, rel_path)) = archive_match {
                let archive_path = if rel_path.is_empty() {
                    root_path
                } else {
                    format!("{}/{}", root_path, rel_path)
                };

                if Some(archive_id) == dest_archive_id {
                    conflicts.in_dest_archive.push((source.path.clone(), archive_path));
                } else {
                    conflicts.in_other_archives.push((source.path.clone(), archive_path));
                }
            }
        }
    }

    Ok(conflicts)
}

fn check_excluded_sources_filtered(
    conn: &Connection,
    sources: &[&ManifestSource],
) -> Result<Vec<(i64, String)>> {
    let mut excluded = Vec::new();

    for source in sources {
        if exclude::is_excluded(conn, source.id)? {
            excluded.push((source.id, source.path.clone()));
        }
    }

    Ok(excluded)
}

enum ApplyAction {
    Copied,
    Renamed,
    Moved,
    SkippedMissing,
}

fn process_source(
    source: &ManifestSource,
    pattern: &str,
    base_dir: &Path,
    options: &ApplyOptions,
) -> Result<ApplyAction> {
    let src_path = Path::new(&source.path);

    // Check if source exists
    if !src_path.exists() {
        if options.dry_run {
            println!("SKIP (missing): {}", source.path);
        }
        return Ok(ApplyAction::SkippedMissing);
    }

    // Expand pattern to get destination path
    let dest_rel = expand_pattern(pattern, source, src_path)?;
    let dest_path = base_dir.join(&dest_rel);

    if options.dry_run {
        match options.transfer_mode {
            TransferMode::Copy => {
                println!("COPY: {} -> {}", source.path, dest_path.display());
                return Ok(ApplyAction::Copied);
            }
            TransferMode::Rename => {
                println!("RENAME: {} -> {}", source.path, dest_path.display());
                return Ok(ApplyAction::Renamed);
            }
            TransferMode::Move => {
                println!("MOVE: {} -> {} (will delete source; may copy if cross-device)", source.path, dest_path.display());
                return Ok(ApplyAction::Moved);
            }
        }
    }

    // Create parent directories
    if let Some(parent) = dest_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create directory: {}", parent.display()))?;
    }

    match options.transfer_mode {
        TransferMode::Copy => {
            // Check exists right before copy (noclobber)
            if dest_path.exists() {
                bail!("Destination already exists: {}", dest_path.display());
            }
            let src_meta = fs::metadata(src_path)
                .with_context(|| format!("Failed to read metadata: {}", source.path))?;
            fs::copy(src_path, &dest_path)
                .with_context(|| format!("Failed to copy {} to {}", source.path, dest_path.display()))?;
            preserve_metadata(&dest_path, &src_meta)?;
            println!("Copied: {} -> {}", source.path, dest_path.display());
            Ok(ApplyAction::Copied)
        }
        TransferMode::Rename => {
            // Check exists right before rename (noclobber)
            if dest_path.exists() {
                bail!("Destination already exists: {}", dest_path.display());
            }
            // No metadata read needed - rename preserves all attributes
            fs::rename(src_path, &dest_path)
                .with_context(|| format!("Failed to rename {} to {}", source.path, dest_path.display()))?;
            println!("Renamed: {} -> {}", source.path, dest_path.display());
            Ok(ApplyAction::Renamed)
        }
        TransferMode::Move => {
            // Check exists right before rename attempt (noclobber)
            if dest_path.exists() {
                bail!("Destination already exists: {}", dest_path.display());
            }
            // Try rename first (mv semantics)
            match fs::rename(src_path, &dest_path) {
                Ok(()) => {
                    println!("Renamed: {} -> {}", source.path, dest_path.display());
                    Ok(ApplyAction::Renamed)
                }
                #[cfg(unix)]
                Err(e) if e.raw_os_error() == Some(libc::EXDEV) => {
                    // Cross-device only: fallback to copy + delete
                    // Re-check dest doesn't exist (race condition guard)
                    if dest_path.exists() {
                        bail!("Destination already exists: {}", dest_path.display());
                    }
                    let src_meta = fs::metadata(src_path)
                        .with_context(|| format!("Failed to read metadata: {}", source.path))?;
                    fs::copy(src_path, &dest_path)
                        .with_context(|| format!("Failed to copy {} to {}", source.path, dest_path.display()))?;
                    preserve_metadata(&dest_path, &src_meta)?;
                    fs::remove_file(src_path)
                        .with_context(|| format!("Failed to delete source: {}", source.path))?;
                    println!("Moved: {} -> {}", source.path, dest_path.display());
                    Ok(ApplyAction::Moved)
                }
                Err(e) => Err(e).with_context(|| {
                    format!("Failed to rename {} to {}", source.path, dest_path.display())
                }),
            }
        }
    }
}

#[cfg(unix)]
fn preserve_metadata(dest: &Path, src_meta: &Metadata) -> Result<()> {
    use filetime::FileTime;

    let mtime = FileTime::from_last_modification_time(src_meta);
    filetime::set_file_mtime(dest, mtime)
        .with_context(|| format!("Failed to set mtime on {}", dest.display()))?;
    fs::set_permissions(dest, src_meta.permissions())
        .with_context(|| format!("Failed to set permissions on {}", dest.display()))?;
    Ok(())
}

#[cfg(not(unix))]
fn preserve_metadata(_dest: &Path, _src_meta: &Metadata) -> Result<()> {
    // No-op on non-Unix
    Ok(())
}

fn expand_pattern(pattern: &str, source: &ManifestSource, src_path: &Path) -> Result<String> {
    let mut result = pattern.to_string();

    // Build substitution map
    let mut vars: HashMap<&str, String> = HashMap::new();

    // Built-in variables from source path
    if let Some(filename) = src_path.file_name().and_then(|s| s.to_str()) {
        vars.insert("filename", filename.to_string());
    }
    if let Some(stem) = src_path.file_stem().and_then(|s| s.to_str()) {
        vars.insert("stem", stem.to_string());
    }
    if let Some(ext) = src_path.extension().and_then(|s| s.to_str()) {
        vars.insert("ext", ext.to_string());
    }

    // Source ID and hash
    vars.insert("id", source.id.to_string());
    if let Some(ref hash) = source.hash_value {
        vars.insert("hash", hash.clone());
        vars.insert("hash_short", hash.chars().take(8).collect());
    }

    // Date/time from facts (if available)
    if let Some(dt) = source.facts.get("exif.datetime_original") {
        if let Some(ts) = dt.as_i64() {
            let dt = chrono::DateTime::from_timestamp(ts, 0);
            if let Some(dt) = dt {
                vars.insert("year", dt.format("%Y").to_string());
                vars.insert("month", dt.format("%m").to_string());
                vars.insert("day", dt.format("%d").to_string());
                vars.insert("date", dt.format("%Y-%m-%d").to_string());
            }
        }
    }

    // Add all facts as variables
    for (key, value) in &source.facts {
        let str_value = match value {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::Bool(b) => b.to_string(),
            _ => continue,
        };
        // Replace dots with underscores for fact keys to make them valid in patterns
        let safe_key = key.replace('.', "_");
        vars.insert(Box::leak(safe_key.into_boxed_str()), str_value);
    }

    // Perform substitutions
    for (key, value) in &vars {
        let placeholder = format!("{{{}}}", key);
        result = result.replace(&placeholder, value);
    }

    // Check for unresolved placeholders
    if result.contains('{') && result.contains('}') {
        // Extract unresolved placeholder for error message
        if let Some(start) = result.find('{') {
            if let Some(end) = result[start..].find('}') {
                let unresolved = &result[start..start + end + 1];
                bail!(
                    "Unresolved placeholder {} in pattern. Available: {:?}",
                    unresolved,
                    vars.keys().collect::<Vec<_>>()
                );
            }
        }
    }

    // Sanitize path (remove potentially dangerous characters)
    let result = result
        .replace("..", "_")
        .replace('\0', "_");

    Ok(result)
}
