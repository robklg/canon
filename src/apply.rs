use anyhow::{bail, Context, Result};
use rusqlite::{params, OptionalExtension};
use std::collections::{HashMap, HashSet};
use std::fs::{self, Metadata};
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::cluster::{Manifest, ManifestSource};
use crate::db::{parse_root_spec, path_strip_prefix, Connection, Db};
use crate::exclude;
use crate::expr::{self, EvalContext, FactValue, Pattern};

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
    pub verbose: bool,
    pub allow_cross_archive_duplicates: bool,
    pub allow_duplicates: bool,
    pub roots: Vec<String>,
    pub transfer_mode: TransferMode,
}

/// Fetch a fact value with its proper type from the database
fn fetch_typed_fact(conn: &Connection, source_id: i64, object_id: Option<i64>, key: &str) -> Result<Option<FactValue>> {
    // Check source facts first
    let row: Option<(Option<String>, Option<f64>, Option<i64>, Option<String>)> = conn
        .query_row(
            "SELECT value_text, value_num, value_time, value_json
             FROM facts WHERE entity_type = 'source' AND entity_id = ? AND key = ?",
            params![source_id, key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;

    if let Some((text, num, time, _json)) = row {
        if let Some(t) = text {
            return Ok(Some(FactValue::Text(t)));
        }
        if let Some(n) = num {
            return Ok(Some(FactValue::Num(n)));
        }
        if let Some(ts) = time {
            return Ok(Some(FactValue::Time(ts)));
        }
        // JSON not yet supported as FactValue
    }

    // Check object facts if source has object_id
    if let Some(obj_id) = object_id {
        let row: Option<(Option<String>, Option<f64>, Option<i64>, Option<String>)> = conn
            .query_row(
                "SELECT value_text, value_num, value_time, value_json
                 FROM facts WHERE entity_type = 'object' AND entity_id = ? AND key = ?",
                params![obj_id, key],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;

        if let Some((text, num, time, _json)) = row {
            if let Some(t) = text {
                return Ok(Some(FactValue::Text(t)));
            }
            if let Some(n) = num {
                return Ok(Some(FactValue::Num(n)));
            }
            if let Some(ts) = time {
                return Ok(Some(FactValue::Time(ts)));
            }
        }
    }

    Ok(None)
}

/// Build an EvalContext for a source using cached root paths
fn build_eval_context(
    conn: &Connection,
    source: &ManifestSource,
    needed_keys: &[String],
    scope_prefix: Option<&str>,
    root_paths: &HashMap<i64, String>,
) -> Result<EvalContext> {
    let mut ctx = EvalContext::new();

    // Get root path from cache (looked up once at apply start)
    let root_path = root_paths.get(&source.root_id)
        .ok_or_else(|| anyhow::anyhow!("Root {} not found in cache", source.root_id))?;

    // Derive rel_path from full path - root_path
    let rel_path = if source.path == *root_path {
        String::new()
    } else if let Some(rel) = path_strip_prefix(&source.path, root_path) {
        rel.to_string()
    } else {
        // Fallback: the path doesn't match the root, use full path as rel_path
        source.path.clone()
    };

    ctx.set_source_root(root_path.clone());
    ctx.set_source_rel_path(rel_path);

    // Set scope prefix if provided
    ctx.set_scope_prefix(scope_prefix.map(|s| s.to_string()));

    // Fetch needed facts from database with proper types
    for key in needed_keys {
        // Skip derived facts (handled by EvalContext)
        if key.starts_with("source.") || key.starts_with("scope.") || key == "object.hash" {
            continue;
        }

        if let Some(value) = fetch_typed_fact(conn, source.id, source.object_id, key)? {
            ctx.set_fact(key, value);
        }
    }

    // Set object.hash from manifest if available
    if let Some(ref hash) = source.hash_value {
        ctx.set_fact("object.hash", FactValue::Text(hash.clone()));
    }

    Ok(ctx)
}

/// Evaluate a pattern for a source, returning the destination relative path
fn evaluate_pattern(
    pattern: &Pattern,
    source: &ManifestSource,
    needed_keys: &[String],
    scope_prefix: Option<&str>,
    conn: &Connection,
    root_paths: &HashMap<i64, String>,
) -> Result<String> {
    let ctx = build_eval_context(conn, source, needed_keys, scope_prefix, root_paths)?;
    expr::evaluate(pattern, &ctx)
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

    let conn = db.conn();

    // Parse the pattern once upfront
    let pattern = expr::parse_pattern(&manifest.output.pattern)
        .with_context(|| format!("Failed to parse output pattern: {}", manifest.output.pattern))?;
    let needed_keys = expr::extract_fact_keys(&pattern);

    // Get scope prefix from manifest if available
    let scope_prefix = manifest.meta.scope.as_deref();

    // Cache all root paths (single query, avoids per-source lookups)
    let root_paths: HashMap<i64, String> = conn
        .prepare("SELECT id, path FROM roots")?
        .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)))?
        .collect::<Result<HashMap<_, _>, _>>()?;

    // Look up archive root path from manifest's archive_root_id
    let archive_root_path: String = conn
        .query_row(
            "SELECT path FROM roots WHERE id = ? AND role = 'archive'",
            [manifest.output.archive_root_id],
            |row| row.get(0),
        )
        .with_context(|| format!("Archive root id {} not found", manifest.output.archive_root_id))?;

    // Construct full base_dir from archive root + relative subdir
    let base_dir = if manifest.output.base_dir.is_empty() {
        PathBuf::from(&archive_root_path)
    } else {
        PathBuf::from(&archive_root_path).join(&manifest.output.base_dir)
    };

    // Filter sources by root if specified
    let filtered_sources = filter_by_roots(&manifest, &options.roots, conn)?;
    let skipped_by_filter = manifest.sources.len() - filtered_sources.len();

    // Pre-flight checks (mandatory, always run)
    eprintln!("Checking {} sources for destination collisions...", filtered_sources.len());
    let collisions = check_destination_collisions_filtered(&filtered_sources, &pattern, &needed_keys, scope_prefix, &base_dir, conn, &root_paths)?;
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
    eprintln!("Checking archive conflicts...");
    let conflicts = check_archive_conflicts_filtered(conn, &filtered_sources, manifest.output.archive_root_id)?;

    if !conflicts.in_dest_archive.is_empty() && !options.allow_duplicates {
        eprintln!(
            "Error: {} files already exist in destination archive:",
            conflicts.in_dest_archive.len()
        );
        for (src, dst) in &conflicts.in_dest_archive {
            eprintln!("  {} -> {}", src, dst);
        }
        eprintln!("\nUse --allow-duplicates to copy anyway (to different paths)");
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
    eprintln!("Checking for excluded sources...");
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
        match process_source(
            source,
            &pattern,
            &needed_keys,
            scope_prefix,
            &base_dir,
            &manifest.output.base_dir,
            options,
            conn,
            manifest.output.archive_root_id,
            &root_paths,
        ) {
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
    pattern: &Pattern,
    needed_keys: &[String],
    scope_prefix: Option<&str>,
    base_dir: &Path,
    conn: &Connection,
    root_paths: &HashMap<i64, String>,
) -> Result<Vec<(PathBuf, Vec<String>)>> {
    let mut dest_to_sources: HashMap<PathBuf, Vec<String>> = HashMap::new();

    for source in sources {
        let src_path = Path::new(&source.path);

        // Skip sources that don't exist (they'll be skipped during copy anyway)
        if !src_path.exists() {
            continue;
        }

        // Evaluate pattern to get destination path
        let dest_rel = evaluate_pattern(pattern, source, needed_keys, scope_prefix, conn, root_paths)?;
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
    dest_archive_id: i64,
) -> Result<ArchiveConflicts> {
    let mut conflicts = ArchiveConflicts {
        in_dest_archive: Vec::new(),
        in_other_archives: Vec::new(),
    };

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

                if archive_id == dest_archive_id {
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
    pattern: &Pattern,
    needed_keys: &[String],
    scope_prefix: Option<&str>,
    base_dir: &Path,
    base_dir_rel: &str,
    options: &ApplyOptions,
    conn: &Connection,
    archive_root_id: i64,
    root_paths: &HashMap<i64, String>,
) -> Result<ApplyAction> {
    let src_path = Path::new(&source.path);

    // Check if source exists
    if !src_path.exists() {
        if options.dry_run {
            println!("SKIP (missing): {}", source.path);
        }
        return Ok(ApplyAction::SkippedMissing);
    }

    // Evaluate pattern to get destination path
    let dest_rel = evaluate_pattern(pattern, source, needed_keys, scope_prefix, conn, root_paths)?;
    let dest_path = base_dir.join(&dest_rel);

    // Compute relative path within archive root for registration
    let archive_rel_path = if base_dir_rel.is_empty() {
        dest_rel.clone()
    } else {
        format!("{}/{}", base_dir_rel, dest_rel)
    };

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
            register_destination(conn, archive_root_id, &dest_path, &archive_rel_path, source.object_id)?;
            if options.verbose {
                println!("Copied: {} -> {}", source.path, dest_path.display());
            }
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
            register_destination(conn, archive_root_id, &dest_path, &archive_rel_path, source.object_id)?;
            if options.verbose {
                println!("Renamed: {} -> {}", source.path, dest_path.display());
            }
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
                    register_destination(conn, archive_root_id, &dest_path, &archive_rel_path, source.object_id)?;
                    if options.verbose {
                        println!("Renamed: {} -> {}", source.path, dest_path.display());
                    }
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
                    register_destination(conn, archive_root_id, &dest_path, &archive_rel_path, source.object_id)?;
                    if options.verbose {
                        println!("Moved: {} -> {}", source.path, dest_path.display());
                    }
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

#[cfg(unix)]
fn register_destination(
    conn: &Connection,
    archive_root_id: i64,
    dest_path: &Path,
    rel_path: &str,
    object_id: Option<i64>,
) -> Result<()> {
    let meta = fs::metadata(dest_path)
        .with_context(|| format!("Failed to read metadata for registration: {}", dest_path.display()))?;
    let device = meta.dev() as i64;
    let inode = meta.ino() as i64;
    let size = meta.size() as i64;
    let mtime = meta.mtime();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs() as i64;

    conn.execute(
        "INSERT INTO sources (root_id, rel_path, device, inode, size, mtime,
         object_id, basis_rev, scanned_at, last_seen_at, present)
         VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, 1)",
        params![archive_root_id, rel_path, device, inode, size, mtime, object_id, now, now],
    )?;
    Ok(())
}

#[cfg(not(unix))]
fn register_destination(
    conn: &Connection,
    archive_root_id: i64,
    dest_path: &Path,
    rel_path: &str,
    object_id: Option<i64>,
) -> Result<()> {
    let meta = fs::metadata(dest_path)
        .with_context(|| format!("Failed to read metadata for registration: {}", dest_path.display()))?;
    let size = meta.len() as i64;
    let mtime = meta.modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs() as i64;

    // No device/inode on non-Unix
    conn.execute(
        "INSERT INTO sources (root_id, rel_path, size, mtime,
         object_id, basis_rev, scanned_at, last_seen_at, present)
         VALUES (?, ?, ?, ?, ?, 0, ?, ?, 1)",
        params![archive_root_id, rel_path, size, mtime, object_id, now, now],
    )?;
    Ok(())
}

