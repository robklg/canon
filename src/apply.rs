use anyhow::{bail, Context, Result};
use rusqlite::{params, OptionalExtension};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File, Metadata};
use std::io::{BufRead, BufReader, ErrorKind};
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::cluster::{LockEntry, ManifestConfig};
use crate::db::{parse_root_spec, path_strip_prefix, Connection, Db};
use crate::exclude;
use crate::expr::{self, EvalContext, FactValue, Pattern};
use crate::scan::compute_partial_hash;

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
    skipped_stale: u64,
    skipped_filtered: u64,
    errors: u64,
}

/// Tracks sources that were skipped due to state changes
struct SkippedStaleSource {
    path: String,
    reason: String,
}

pub struct ApplyOptions {
    pub dry_run: bool,
    pub verbose: bool,
    pub allow_cross_archive_duplicates: bool,
    pub allow_duplicates: bool,
    pub roots: Vec<String>,
    pub transfer_mode: TransferMode,
    pub yes: bool,
}

/// Fetch a fact value with its proper type from the database
fn fetch_typed_fact(conn: &Connection, source_id: i64, object_id: Option<i64>, key: &str) -> Result<Option<FactValue>> {
    // Check source facts first
    let row: Option<(Option<String>, Option<f64>, Option<i64>)> = conn
        .query_row(
            "SELECT value_text, value_num, value_time
             FROM facts WHERE entity_type = 'source' AND entity_id = ? AND key = ?",
            params![source_id, key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;

    if let Some((text, num, time)) = row {
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

    // Check object facts if source has object_id
    if let Some(obj_id) = object_id {
        let row: Option<(Option<String>, Option<f64>, Option<i64>)> = conn
            .query_row(
                "SELECT value_text, value_num, value_time
                 FROM facts WHERE entity_type = 'object' AND entity_id = ? AND key = ?",
                params![obj_id, key],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;

        if let Some((text, num, time)) = row {
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
    source: &LockEntry,
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
    source: &LockEntry,
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

    // Determine config path and lock path
    let (config_path, lock_path) = if manifest_path.extension().and_then(|e| e.to_str()) == Some("lock") {
        (manifest_path.with_extension("toml"), manifest_path.to_path_buf())
    } else {
        (manifest_path.to_path_buf(), manifest_path.with_extension("lock"))
    };

    // Read TOML config
    let config_content = fs::read_to_string(&config_path)
        .with_context(|| format!("Failed to read manifest config: {}", config_path.display()))?;
    let config: ManifestConfig = toml::from_str(&config_content)
        .with_context(|| format!("Failed to parse manifest config: {}", config_path.display()))?;

    // Read JSONL lock file
    let lock_file = File::open(&lock_path)
        .with_context(|| format!("Failed to open lock file: {}", lock_path.display()))?;
    let sources: Vec<LockEntry> = BufReader::new(lock_file)
        .lines()
        .enumerate()
        .map(|(i, line)| {
            let line = line.with_context(|| format!("Failed to read line {} of lock file", i + 1))?;
            serde_json::from_str(&line)
                .with_context(|| format!("Failed to parse line {} of lock file", i + 1))
        })
        .collect::<Result<Vec<_>>>()?;

    // Validate lock file hash matches config
    let actual_hash = crate::cluster::hash_file(&lock_path)?;
    if actual_hash != config.meta.lock_hash {
        bail!(
            "Lock file hash mismatch: expected {}, got {}\n\
             The lock file may have been modified or does not belong to this config.\n\
             Run `cluster refresh` to regenerate the lock file.",
            &config.meta.lock_hash[..16.min(config.meta.lock_hash.len())],
            &actual_hash[..16]
        );
    }

    let conn = db.conn();

    // Early preflight: Check for unhashed sources in manifest
    check_unhashed_sources(&sources)?;

    // Early preflight: Check archive has complete hash coverage
    check_archive_hash_coverage(conn, config.output.archive_root_id)?;

    // Parse the pattern once upfront
    let pattern = expr::parse_pattern(&config.output.pattern)
        .with_context(|| format!("Failed to parse output pattern: {}", config.output.pattern))?;
    let needed_keys = expr::extract_fact_keys(&pattern);

    // Get scope prefix from config if available
    let scope_prefix = config.meta.scope.as_deref();

    // Cache all root paths (single query, avoids per-source lookups)
    let root_paths: HashMap<i64, String> = conn
        .prepare("SELECT id, path FROM roots")?
        .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)))?
        .collect::<Result<HashMap<_, _>, _>>()?;

    // Look up archive root path from manifest's archive_root_id
    let archive_root_path: String = conn
        .query_row(
            "SELECT path FROM roots WHERE id = ? AND role = 'archive'",
            [config.output.archive_root_id],
            |row| row.get(0),
        )
        .with_context(|| format!("Archive root id {} not found", config.output.archive_root_id))?;

    // Construct full base_dir from archive root + relative subdir
    let base_dir = if config.output.base_dir.is_empty() {
        PathBuf::from(&archive_root_path)
    } else {
        PathBuf::from(&archive_root_path).join(&config.output.base_dir)
    };

    // Filter sources by root if specified
    let filtered_sources = filter_by_roots(&sources, &options.roots, conn)?;
    let skipped_by_filter = sources.len() - filtered_sources.len();

    // Show summary and confirm (unless --yes)
    print_apply_summary(&config_path, &base_dir, &filtered_sources, options);

    if !options.yes && !confirm_proceed(options.dry_run)? {
        println!("Aborted.");
        return Ok(());
    }

    // Pre-flight checks (mandatory, always run)
    eprintln!("Checking destination write permissions...");
    check_destination_writable(&base_dir)?;

    eprint!("Checking {} sources for collisions and accessibility...", filtered_sources.len());
    if options.dry_run {
        eprintln!(" (skipping source checks for speed in dry-run mode)");
    } else {
        eprintln!();
    }
    let access_check = check_destination_collisions_filtered(&filtered_sources, &pattern, &needed_keys, scope_prefix, &base_dir, conn, &root_paths, options.dry_run)?;

    if !access_check.unreadable.is_empty() {
        eprintln!(
            "Error: {} sources are not readable:",
            access_check.unreadable.len()
        );
        for (path, reason) in &access_check.unreadable {
            eprintln!("  {} ({})", path, reason);
        }
        bail!("Aborting due to unreadable sources");
    }

    if !access_check.collisions.is_empty() {
        eprintln!(
            "Error: {} destination paths have multiple sources:",
            access_check.collisions.len()
        );
        for (dest, sources) in &access_check.collisions {
            eprintln!("  {} <- {} files:", dest.display(), sources.len());
            for src in sources {
                eprintln!("    {}", src);
            }
        }
        bail!("Aborting due to destination collisions");
    }

    // Check archive conflicts
    eprintln!("Checking archive conflicts...");
    let conflicts = check_archive_conflicts_filtered(conn, &filtered_sources, config.output.archive_root_id)?;

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

    // Preflight: validate pattern-relevant facts haven't changed
    eprintln!("Validating snapshot facts...");
    let fact_mismatches = validate_snapshot_facts(conn, &filtered_sources, &needed_keys)?;
    if !fact_mismatches.is_empty() {
        eprintln!(
            "Error: {} pattern-relevant facts have changed since manifest was generated:",
            fact_mismatches.len()
        );
        for (path, key, old, new) in fact_mismatches.iter().take(5) {
            eprintln!("  {}: {} was {:?}, now {:?}", path, key, old, new);
        }
        if fact_mismatches.len() > 5 {
            eprintln!("  ... and {} more", fact_mismatches.len() - 5);
        }
        eprintln!("\nRun `cluster refresh` to regenerate the lock file.");
        bail!("Aborting due to fact validation failure");
    }

    // Preflight: validate source file states
    // dry-run: fast DB check; real apply: thorough disk check
    eprintln!("Validating source file states...");
    let stale = if options.dry_run {
        check_source_states_db(conn, &filtered_sources)?
    } else {
        check_source_states_disk(&filtered_sources)
    };
    if !stale.is_empty() {
        eprintln!(
            "Error: {} sources have changed since manifest was generated:",
            stale.len()
        );
        for s in stale.iter().take(10) {
            eprintln!("  {}: {}", s.path, s.reason);
        }
        if stale.len() > 10 {
            eprintln!("  ... and {} more", stale.len() - 10);
        }
        eprintln!("\nRun `canon scan` then `cluster refresh` to regenerate the lock file.");
        bail!("Aborting due to stale sources in manifest");
    }

    let mut stats = ApplyStats {
        skipped_filtered: skipped_by_filter as u64,
        ..Default::default()
    };

    // Track stale sources found during transfers (race condition detection)
    let mut stale_during_transfer: Vec<SkippedStaleSource> = Vec::new();

    let total = filtered_sources.len();
    let progress_interval = std::cmp::max(total / 20, 1); // Update every 5%
    eprintln!("Processing {} sources...", total);

    for (i, source) in filtered_sources.iter().enumerate() {
        // Progress indicator
        if i > 0 && i % progress_interval == 0 {
            let pct = (i * 100) / total;
            eprint!("\r  {}% ({}/{})", pct, i, total);
        }

        match process_source(
            source,
            &pattern,
            &needed_keys,
            scope_prefix,
            &base_dir,
            &config.output.base_dir,
            options,
            conn,
            config.output.archive_root_id,
            &root_paths,
        ) {
            Ok(action) => match action {
                ApplyAction::Copied => stats.copied += 1,
                ApplyAction::Renamed => stats.renamed += 1,
                ApplyAction::Moved => stats.moved += 1,
                ApplyAction::SkippedMissing => stats.skipped_missing += 1,
                ApplyAction::SkippedStale(reason) => {
                    stats.skipped_stale += 1;
                    stale_during_transfer.push(SkippedStaleSource {
                        path: source.path.clone(),
                        reason,
                    });
                }
            },
            Err(e) => {
                eprintln!("Error processing {}: {}", source.path, e);
                stats.errors += 1;
            }
        }
    }

    // Clear progress line
    if total > progress_interval {
        eprint!("\r  100% ({}/{})\n", total, total);
    }

    // Summary of files that became stale during transfer (race conditions)
    if !stale_during_transfer.is_empty() {
        eprintln!("\nSkipped {} files that changed during apply:", stale_during_transfer.len());
        for s in stale_during_transfer.iter().take(10) {
            eprintln!("  {}: {}", s.path, s.reason);
        }
        if stale_during_transfer.len() > 10 {
            eprintln!("  ... and {} more", stale_during_transfer.len() - 10);
        }
        eprintln!("Run `canon scan` then `cluster refresh` to regenerate the lock file.");
    }

    let mode = if options.dry_run { " (dry-run)" } else { "" };
    println!(
        "Applied{}: {} copied, {} renamed, {} moved, {} skipped (missing), {} skipped (stale), {} skipped (filtered), {} errors",
        mode, stats.copied, stats.renamed, stats.moved, stats.skipped_missing, stats.skipped_stale, stats.skipped_filtered, stats.errors
    );

    // Update query planner statistics after bulk changes (skip for dry-run)
    if !options.dry_run {
        conn.execute("ANALYZE", [])?;
    }

    Ok(())
}

struct ArchiveConflicts {
    in_dest_archive: Vec<(String, String)>,   // (source_path, archive_path)
    in_other_archives: Vec<(String, String)>, // (source_path, archive_path)
}

struct SourceAccessCheck {
    collisions: Vec<(PathBuf, Vec<String>)>,  // (dest_path, source_paths)
    unreadable: Vec<(String, String)>,        // (source_path, error_message)
}

// ============================================================================
// Summary and confirmation helpers
// ============================================================================

fn print_apply_summary(
    config_path: &Path,
    base_dir: &Path,
    sources: &[&LockEntry],
    options: &ApplyOptions,
) {
    eprintln!();
    eprintln!("=== Apply Summary ===");
    eprintln!("Manifest: {}", config_path.display());
    eprintln!("Destination: {}", base_dir.display());

    let mode_name = match options.transfer_mode {
        TransferMode::Copy => "copy",
        TransferMode::Rename => "rename",
        TransferMode::Move => "move",
    };
    eprintln!("Mode: {}", mode_name);
    eprintln!("Files: {}", sources.len());

    // Show destination preview if exists
    if base_dir.exists() {
        eprintln!();
        eprintln!("Destination current contents:");
        show_directory_preview(base_dir, 5);
    } else {
        eprintln!();
        eprintln!("Destination: (will be created)");
    }

    eprintln!();
}

fn show_directory_preview(dir: &Path, max_items: usize) {
    let entries: Vec<_> = match fs::read_dir(dir) {
        Ok(rd) => rd.filter_map(|e| e.ok()).take(max_items + 1).collect(),
        Err(_) => return,
    };

    if entries.is_empty() {
        eprintln!("  (empty)");
        return;
    }

    let mut count = 0;
    for entry in entries.iter().take(max_items) {
        let name = entry.file_name();
        let suffix = entry.file_type().map(|ft| if ft.is_dir() { "/" } else { "" }).unwrap_or("");
        eprintln!("  {}{}", name.to_string_lossy(), suffix);
        count += 1;
    }

    // Count remaining entries
    if entries.len() > max_items {
        // We took max_items + 1, so there's at least 1 more
        // Count actual total to report accurately
        let total = match fs::read_dir(dir) {
            Ok(rd) => rd.count(),
            Err(_) => count + 1,
        };
        let remaining = total.saturating_sub(count);
        if remaining > 0 {
            eprintln!("  ... and {} more", remaining);
        }
    }
}

fn confirm_proceed(dry_run: bool) -> Result<bool> {
    use std::io::{self, Write};

    if dry_run {
        eprint!("Proceed with dry-run? [y/N] ");
    } else {
        eprint!("Proceed? [y/N] ");
    }
    io::stderr().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;

    Ok(input.trim().eq_ignore_ascii_case("y"))
}

// ============================================================================
// Helper functions for pre-flight checks (work with filtered source list)
// ============================================================================

/// Check if destination directory is writable by creating and removing a test file.
fn check_destination_writable(base_dir: &Path) -> Result<()> {
    // Find the nearest existing directory
    let mut check_dir = base_dir.to_path_buf();
    while !check_dir.exists() {
        if let Some(parent) = check_dir.parent() {
            check_dir = parent.to_path_buf();
        } else {
            bail!("Cannot find existing parent directory for {}", base_dir.display());
        }
    }

    // Try to create a temp file to verify write permissions
    let test_file = check_dir.join(".canon_write_test");
    match File::create(&test_file) {
        Ok(_) => {
            // Successfully created, now remove it
            let _ = fs::remove_file(&test_file);
            Ok(())
        }
        Err(e) if e.kind() == ErrorKind::PermissionDenied => {
            bail!("No write permission for destination directory: {}", check_dir.display());
        }
        Err(e) => {
            bail!("Cannot write to destination directory {}: {}", check_dir.display(), e);
        }
    }
}

fn filter_by_roots<'a>(
    sources: &'a [LockEntry],
    roots: &[String],
    conn: &Connection,
) -> Result<Vec<&'a LockEntry>> {
    if roots.is_empty() {
        return Ok(sources.iter().collect());
    }

    let mut root_ids = HashSet::new();
    for spec in roots {
        let id = parse_root_spec(conn, spec, None)?;
        root_ids.insert(id);
    }

    Ok(sources.iter().filter(|s| root_ids.contains(&s.root_id)).collect())
}

fn check_destination_collisions_filtered(
    sources: &[&LockEntry],
    pattern: &Pattern,
    needed_keys: &[String],
    scope_prefix: Option<&str>,
    base_dir: &Path,
    conn: &Connection,
    root_paths: &HashMap<i64, String>,
    dry_run: bool,
) -> Result<SourceAccessCheck> {
    let mut dest_to_sources: HashMap<PathBuf, Vec<String>> = HashMap::new();
    let mut unreadable: Vec<(String, String)> = Vec::new();
    let total = sources.len();
    let progress_interval = std::cmp::max(total / 20, 1); // Update every 5%

    for (i, source) in sources.iter().enumerate() {
        // Progress indicator
        if i > 0 && i % progress_interval == 0 {
            let pct = (i * 100) / total;
            eprint!("\r  {}% ({}/{})", pct, i, total);
        }

        // Check source accessibility (existence + read permission in one syscall)
        // In dry-run mode, skip this check for speed
        if !dry_run {
            match File::open(&source.path) {
                Ok(_) => { /* readable, continue */ }
                Err(e) if e.kind() == ErrorKind::NotFound => {
                    // File doesn't exist, skip (will be skipped during copy anyway)
                    continue;
                }
                Err(e) if e.kind() == ErrorKind::PermissionDenied => {
                    unreadable.push((source.path.clone(), "permission denied".to_string()));
                    continue;
                }
                Err(e) => {
                    unreadable.push((source.path.clone(), e.to_string()));
                    continue;
                }
            }
        }

        // Evaluate pattern to get destination path
        let dest_rel = evaluate_pattern(pattern, source, needed_keys, scope_prefix, conn, root_paths)?;
        let dest_path = base_dir.join(&dest_rel);

        dest_to_sources
            .entry(dest_path)
            .or_default()
            .push(source.path.clone());
    }

    // Clear progress line
    if total > progress_interval {
        eprint!("\r  100% ({}/{})\n", total, total);
    }

    // Filter to only collisions (more than one source per destination)
    let mut collisions: Vec<(PathBuf, Vec<String>)> = dest_to_sources
        .into_iter()
        .filter(|(_, sources)| sources.len() > 1)
        .collect();

    // Sort for consistent output
    collisions.sort_by(|a, b| a.0.cmp(&b.0));

    Ok(SourceAccessCheck { collisions, unreadable })
}

fn check_archive_conflicts_filtered(
    conn: &Connection,
    sources: &[&LockEntry],
    dest_archive_id: i64,
) -> Result<ArchiveConflicts> {
    let mut conflicts = ArchiveConflicts {
        in_dest_archive: Vec::new(),
        in_other_archives: Vec::new(),
    };

    let total = sources.len();
    let progress_interval = std::cmp::max(total / 20, 1); // Update every 5%

    for (i, source) in sources.iter().enumerate() {
        // Progress indicator
        if i > 0 && i % progress_interval == 0 {
            let pct = (i * 100) / total;
            eprint!("\r  {}% ({}/{})", pct, i, total);
        }

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

    // Clear progress line
    if total > progress_interval {
        eprint!("\r  100% ({}/{})\n", total, total);
    }

    Ok(conflicts)
}

/// Check that all sources in manifest have content hashes.
/// Unhashed sources cannot be applied - deduplication requires content hashes.
fn check_unhashed_sources(sources: &[LockEntry]) -> Result<()> {
    let unhashed: Vec<_> = sources
        .iter()
        .filter(|s| s.object_id.is_none())
        .collect();

    if !unhashed.is_empty() {
        eprintln!(
            "Error: Manifest contains {} sources without content hash",
            unhashed.len()
        );
        for source in unhashed.iter().take(10) {
            eprintln!("  {}", source.path);
        }
        if unhashed.len() > 10 {
            eprintln!("  ... and {} more", unhashed.len() - 10);
        }
        bail!(
            "Cannot apply unhashed sources - deduplication and integrity checks require content hashes.\n\
             Import hashes via worklist pipeline, then run 'canon cluster refresh <manifest>'."
        );
    }
    Ok(())
}

/// Check that destination archive has complete hash coverage.
/// Without complete coverage, we can't reliably detect duplicates.
fn check_archive_hash_coverage(conn: &Connection, archive_root_id: i64) -> Result<()> {
    let (total, unhashed): (i64, i64) = conn
        .query_row(
            "SELECT COUNT(*), SUM(CASE WHEN object_id IS NULL THEN 1 ELSE 0 END)
             FROM sources WHERE root_id = ? AND present = 1",
            [archive_root_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;

    if unhashed > 0 {
        bail!(
            "Destination archive has {} files without content hash (out of {})\n\
             Cannot reliably detect duplicates without complete hash coverage.\n\
             Run 'canon scan <archive-path>' to index and hash the archive.",
            unhashed, total
        );
    }
    Ok(())
}

fn check_excluded_sources_filtered(
    conn: &Connection,
    sources: &[&LockEntry],
) -> Result<Vec<(i64, String)>> {
    let mut excluded = Vec::new();
    let total = sources.len();
    let progress_interval = std::cmp::max(total / 20, 1); // Update every 5%

    for (i, source) in sources.iter().enumerate() {
        // Progress indicator
        if i > 0 && i % progress_interval == 0 {
            let pct = (i * 100) / total;
            eprint!("\r  {}% ({}/{})", pct, i, total);
        }

        if exclude::is_excluded(conn, source.id)? {
            excluded.push((source.id, source.path.clone()));
        }
    }

    // Clear progress line
    if total > progress_interval {
        eprint!("\r  100% ({}/{})\n", total, total);
    }

    Ok(excluded)
}

/// Validate that a source file on disk matches the state recorded in the lock file.
/// Returns Ok(()) if valid, Err with reason if changed.
fn validate_source_state(source: &LockEntry) -> std::result::Result<(), String> {
    let meta = match fs::metadata(&source.path) {
        Ok(m) => m,
        Err(e) if e.kind() == ErrorKind::NotFound => {
            return Err("file not found".to_string());
        }
        Err(e) => {
            return Err(format!("cannot stat: {}", e));
        }
    };

    let mut mismatches = Vec::new();

    #[cfg(unix)]
    {
        // Validate size+mtime only; device/inode changes don't indicate content changes
        // (e.g., NAS remounts). Staleness is determined by size+mtime+partial_hash.
        let current_size = meta.size() as i64;
        let current_mtime = meta.mtime();

        if current_size != source.size {
            mismatches.push(format!("size: {} → {}", source.size, current_size));
        }
        if current_mtime != source.mtime {
            mismatches.push(format!("mtime: {} → {}", source.mtime, current_mtime));
        }
    }

    #[cfg(not(unix))]
    {
        let current_size = meta.len() as i64;
        let current_mtime = meta.modified()
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        if current_size != source.size {
            mismatches.push(format!("size: {} → {}", source.size, current_size));
        }
        if current_mtime != source.mtime {
            mismatches.push(format!("mtime: {} → {}", source.mtime, current_mtime));
        }
    }

    // Partial hash check - recompute from disk and compare to lock
    let current_hash = compute_partial_hash(Path::new(&source.path), source.size as u64)
        .map_err(|e| format!("failed to compute partial hash: {}", e))?;
    if current_hash != source.partial_hash {
        mismatches.push(format!(
            "partial hash mismatch: {}... → {}...",
            &source.partial_hash[..16.min(source.partial_hash.len())],
            &current_hash[..16]
        ));
    }

    if !mismatches.is_empty() {
        Err(mismatches.join(", "))
    } else {
        Ok(())
    }
}

/// Batch validate all source file states against disk. Returns list of stale sources.
fn check_source_states_disk(sources: &[&LockEntry]) -> Vec<SkippedStaleSource> {
    let mut stale = Vec::new();
    let total = sources.len();
    let progress_interval = std::cmp::max(total / 20, 1);

    for (i, source) in sources.iter().enumerate() {
        if i > 0 && i % progress_interval == 0 {
            let pct = (i * 100) / total;
            eprint!("\r  {}% ({}/{})", pct, i, total);
        }

        if let Err(reason) = validate_source_state(source) {
            stale.push(SkippedStaleSource {
                path: source.path.clone(),
                reason,
            });
        }
    }

    if total > progress_interval {
        eprint!("\r  100% ({}/{})\n", total, total);
    }

    stale
}

/// Convert a FactValue to serde_json::Value for comparison with lock file
fn fact_value_to_json(value: &FactValue) -> serde_json::Value {
    match value {
        FactValue::Text(t) => serde_json::Value::String(t.clone()),
        FactValue::Num(n) => serde_json::json!(*n),
        FactValue::Time(t) => serde_json::json!(*t),
        FactValue::Path(p) => serde_json::Value::String(p.clone()),
    }
}

/// Validate that pattern-relevant facts haven't changed since manifest was generated.
/// Only checks stored facts (not built-in source.*/scope.*/object.hash).
/// Returns list of mismatches: (path, key, old_value, new_value)
fn validate_snapshot_facts(
    conn: &Connection,
    sources: &[&LockEntry],
    needed_keys: &[String],
) -> Result<Vec<(String, String, Option<serde_json::Value>, Option<serde_json::Value>)>> {
    // Filter to only stored facts (not built-in facts)
    let stored_keys: Vec<&String> = needed_keys
        .iter()
        .filter(|k| !k.starts_with("source.") && !k.starts_with("scope.") && *k != "object.hash")
        .collect();

    if stored_keys.is_empty() {
        return Ok(Vec::new()); // Pattern only uses built-in facts
    }

    let mut mismatches = Vec::new();
    let total = sources.len();
    let progress_interval = std::cmp::max(total / 20, 1);

    for (i, source) in sources.iter().enumerate() {
        if i > 0 && i % progress_interval == 0 {
            let pct = (i * 100) / total;
            eprint!("\r  {}% ({}/{})", pct, i, total);
        }

        for key in &stored_keys {
            let snapshot_value = source.facts.get(*key);
            let current = fetch_typed_fact(conn, source.id, source.object_id, key)?;
            let current_json = current.as_ref().map(fact_value_to_json);

            // Compare: snapshot should match current DB value
            let mismatch = match (&snapshot_value, &current_json) {
                (None, None) => false,
                (Some(a), Some(b)) => *a != b,
                _ => true,
            };

            if mismatch {
                mismatches.push((
                    source.path.clone(),
                    (*key).clone(),
                    snapshot_value.cloned(),
                    current_json,
                ));
            }
        }
    }

    if total > progress_interval {
        eprint!("\r  100% ({}/{})\n", total, total);
    }

    Ok(mismatches)
}

/// Batch validate source file states against DB values. Returns list of stale sources.
/// Used in dry-run mode for faster validation without disk access.
fn check_source_states_db(conn: &Connection, sources: &[&LockEntry]) -> Result<Vec<SkippedStaleSource>> {
    let mut stale = Vec::new();
    let total = sources.len();
    let progress_interval = std::cmp::max(total / 20, 1);

    for (i, source) in sources.iter().enumerate() {
        if i > 0 && i % progress_interval == 0 {
            let pct = (i * 100) / total;
            eprint!("\r  {}% ({}/{})", pct, i, total);
        }

        // Get current DB values for this source
        let db_state: Option<(i64, i64, Option<String>, bool)> = conn
            .query_row(
                // Validate size+mtime+partial_hash only; device/inode not used for staleness
                "SELECT size, mtime, partial_hash, present FROM sources WHERE id = ?",
                [source.id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;

        match db_state {
            None => {
                stale.push(SkippedStaleSource {
                    path: source.path.clone(),
                    reason: "source not found in DB".to_string(),
                });
            }
            Some((_, _, _, false)) => {
                stale.push(SkippedStaleSource {
                    path: source.path.clone(),
                    reason: "source marked not present in DB".to_string(),
                });
            }
            Some((db_size, db_mtime, db_partial_hash, true)) => {
                let mut mismatches = Vec::new();

                if db_size != source.size {
                    mismatches.push(format!("size: {} → {}", source.size, db_size));
                }
                if db_mtime != source.mtime {
                    mismatches.push(format!("mtime: {} → {}", source.mtime, db_mtime));
                }
                // Compare partial_hash from lock vs DB
                if let Some(ref db_hash) = db_partial_hash {
                    if db_hash != &source.partial_hash {
                        mismatches.push(format!(
                            "partial hash: {}... → {}...",
                            &source.partial_hash[..16.min(source.partial_hash.len())],
                            &db_hash[..16.min(db_hash.len())]
                        ));
                    }
                } else {
                    mismatches.push("partial hash: missing in DB".to_string());
                }

                if !mismatches.is_empty() {
                    stale.push(SkippedStaleSource {
                        path: source.path.clone(),
                        reason: mismatches.join(", "),
                    });
                }
            }
        }
    }

    if total > progress_interval {
        eprint!("\r  100% ({}/{})\n", total, total);
    }

    Ok(stale)
}

enum ApplyAction {
    Copied,
    Renamed,
    Moved,
    SkippedMissing,
    SkippedStale(String),  // reason
}

fn process_source(
    source: &LockEntry,
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
            println!("[dry-run] SKIP (missing): {}", source.path);
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
                println!("[dry-run] COPY: {} -> {}", source.path, dest_path.display());
                return Ok(ApplyAction::Copied);
            }
            TransferMode::Rename => {
                println!("[dry-run] RENAME: {} -> {}", source.path, dest_path.display());
                return Ok(ApplyAction::Renamed);
            }
            TransferMode::Move => {
                println!("[dry-run] MOVE: {} -> {} (would delete source; may copy if cross-device)", source.path, dest_path.display());
                return Ok(ApplyAction::Moved);
            }
        }
    }

    // Per-transfer validation: check source hasn't changed since preflight
    // (catches race conditions where file changes between preflight and transfer)
    if let Err(reason) = validate_source_state(source) {
        return Ok(ApplyAction::SkippedStale(reason));
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
            // Update existing source row (inode unchanged on same device)
            relocate_source(conn, source.id, archive_root_id, &archive_rel_path)?;
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
                    // Update existing source row (inode unchanged on same device)
                    relocate_source(conn, source.id, archive_root_id, &archive_rel_path)?;
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
                    // Mark old source as not present (file was deleted)
                    mark_source_not_present(conn, source.id)?;
                    // Register new destination (new inode on different device)
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

/// Relocate an existing source to a new location (for rename/move on same device).
/// Updates the source row in-place since the inode remains the same.
fn relocate_source(
    conn: &Connection,
    source_id: i64,
    archive_root_id: i64,
    rel_path: &str,
) -> Result<()> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs() as i64;

    conn.execute(
        "UPDATE sources SET root_id = ?, rel_path = ?, scanned_at = ?, last_seen_at = ?
         WHERE id = ?",
        params![archive_root_id, rel_path, now, now, source_id],
    )?;
    Ok(())
}

/// Mark a source as no longer present (for cross-device move after deletion).
fn mark_source_not_present(conn: &Connection, source_id: i64) -> Result<()> {
    conn.execute(
        "UPDATE sources SET present = 0 WHERE id = ?",
        params![source_id],
    )?;
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

