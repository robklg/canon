use anyhow::{bail, Context, Result};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File, Metadata};
use std::io::{BufRead, BufReader, ErrorKind};
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::ceremony;
use crate::cluster::{self, LockEntry, ManifestConfig};
use crate::domain::apply::{classify_destination, DestinationState};
use crate::domain::fact::FactEntry;
use crate::domain::path::path_strip_prefix;
use crate::domain::root::parse_root_spec;
use crate::domain::source::NewSource;
use crate::expr::{self, EvalContext, FactValue, Pattern};
use crate::progress::Progress;
use crate::repo::{self, Connection, Db};
use crate::scan::compute_partial_hash;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferMode {
    Copy,   // Default: copy only, source remains
    Rename, // Unix only, error if cross-device
    Move,   // Try rename, fallback to copy+delete on EXDEV
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
    // Resume mode counts (from work planning phase)
    already_archived: u64,
    resumed: u64,
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
    pub resume: bool,
}

/// Result of work planning phase in resume mode.
struct WorkPlan<'a> {
    /// Sources that need to be transferred (not in DB, not on disk)
    to_transfer: Vec<&'a LockEntry>,
    /// Count of sources already registered in DB (skipped)
    already_archived: usize,
    /// Count of sources on disk but not in DB (skipped, need scan)
    resumed: usize,
}

/// A size mismatch found during work planning.
struct SizeMismatchError {
    dest_path: String,
    expected: u64,
    actual: u64,
}

/// Build an EvalContext for a source using pre-fetched facts and cached root paths.
///
/// Uses the all_facts map (built via repo::fact::batch_fetch_for_sources) instead of
/// per-source queries. This follows the same pattern as cluster.rs.
fn build_eval_context(
    source: &LockEntry,
    needed_keys: &[String],
    scope_prefix: Option<&str>,
    root_paths: &HashMap<i64, String>,
    all_facts: &HashMap<i64, Vec<FactEntry>>,
) -> Result<EvalContext> {
    let mut ctx = EvalContext::new();

    // Get root path from cache (looked up once at apply start)
    let root_path = root_paths
        .get(&source.root_id)
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

    // Look up facts from pre-fetched map (no per-source queries)
    if let Some(source_facts) = all_facts.get(&source.id) {
        for key in needed_keys {
            // Skip derived facts (handled by EvalContext)
            if key.starts_with("source.") || key.starts_with("scope.") || key == "object.hash" {
                continue;
            }

            // Find the fact with matching key in this source's facts
            if let Some(entry) = source_facts.iter().find(|f| f.key == *key) {
                ctx.set_fact(key, entry.value.clone());
            }
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
    root_paths: &HashMap<i64, String>,
    all_facts: &HashMap<i64, Vec<FactEntry>>,
) -> Result<String> {
    let ctx = build_eval_context(source, needed_keys, scope_prefix, root_paths, all_facts)?;
    expr::evaluate(pattern, &ctx)
}

pub fn run(db: &mut Db, manifest_path: &Path, options: &ApplyOptions) -> Result<()> {
    // Platform checks: --rename and --move are Unix-only
    #[cfg(not(unix))]
    if options.transfer_mode == TransferMode::Rename || options.transfer_mode == TransferMode::Move
    {
        bail!("--rename and --move are only supported on Unix platforms");
    }

    // Metadata preservation warning for Copy mode on non-Unix
    #[cfg(not(unix))]
    if options.transfer_mode == TransferMode::Copy {
        eprintln!("Note: mtime/permissions preservation not available on this platform");
    }

    // Determine config path and lock path
    let (config_path, lock_path) =
        if manifest_path.extension().and_then(|e| e.to_str()) == Some("lock") {
            (
                manifest_path.with_extension("toml"),
                manifest_path.to_path_buf(),
            )
        } else {
            (
                manifest_path.to_path_buf(),
                manifest_path.with_extension("lock"),
            )
        };

    // Read TOML config
    let config_content = fs::read_to_string(&config_path)
        .with_context(|| format!("Failed to read manifest config: {}", config_path.display()))?;
    let config: ManifestConfig = toml::from_str(&config_content)
        .with_context(|| format!("Failed to parse manifest config: {}", config_path.display()))?;

    // Validate manifest version
    cluster::validate_manifest_version(config.meta.version)?;

    // Read JSONL lock file
    let lock_file = File::open(&lock_path)
        .with_context(|| format!("Failed to open lock file: {}", lock_path.display()))?;
    let sources: Vec<LockEntry> = BufReader::new(lock_file)
        .lines()
        .enumerate()
        .map(|(i, line)| {
            let line =
                line.with_context(|| format!("Failed to read line {} of lock file", i + 1))?;
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

    let conn = db.conn_mut();

    // Early preflight: Check for unhashed sources in manifest
    check_unhashed_sources(&sources)?;

    // Early preflight: Check archive has complete hash coverage
    check_archive_hash_coverage(&*conn, config.output.archive_root_id)?;

    // Parse the pattern once upfront
    let pattern = expr::parse_pattern(&config.output.pattern)
        .with_context(|| format!("Failed to parse output pattern: {}", config.output.pattern))?;
    let needed_keys = expr::extract_fact_keys(&pattern);

    // Get scope prefix from config if available
    let scope_prefix = config.meta.scope.as_deref();

    // Cache all root paths (single query via repo layer)
    let roots = repo::root::fetch_all(conn)?;
    let root_paths: HashMap<i64, String> = roots.iter().map(|r| (r.id, r.path.clone())).collect();

    // Look up archive root from cached roots, verify it's an archive
    let archive_root = roots
        .iter()
        .find(|r| r.id == config.output.archive_root_id)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Archive root id {} not found",
                config.output.archive_root_id
            )
        })?;
    if !archive_root.is_archive() {
        bail!(
            "Root id {} has role '{}', expected 'archive'",
            config.output.archive_root_id,
            archive_root.role
        );
    }
    let archive_root_path = &archive_root.path;

    // Construct full base_dir from archive root + relative subdir
    let base_dir = if config.output.base_dir.is_empty() {
        PathBuf::from(&archive_root_path)
    } else {
        PathBuf::from(&archive_root_path).join(&config.output.base_dir)
    };

    // Filter sources by root if specified
    let filtered_sources = filter_by_roots(&sources, &options.roots, &roots)?;
    let skipped_by_filter = sources.len() - filtered_sources.len();

    // Show summary and confirm (unless --yes)
    print_apply_summary(&config_path, &base_dir, &filtered_sources, options, &root_paths);

    if !ceremony::confirm(options.yes)? {
        return Ok(());
    }

    // Batch fetch facts for pattern evaluation (eliminates N+1 queries)
    // Fetch only the keys needed by the pattern, one batch query per key
    let source_ids: Vec<i64> = filtered_sources.iter().map(|s| s.id).collect();
    let mut all_facts: HashMap<i64, Vec<FactEntry>> = HashMap::new();
    for key in &needed_keys {
        // Skip derived keys (handled by EvalContext, not from facts table)
        if key.starts_with("source.") || key.starts_with("scope.") || key == "object.hash" {
            continue;
        }
        let key_facts = repo::fact::batch_fetch_key_for_sources(conn, &source_ids, key)?;
        for (source_id, entry_opt) in key_facts {
            if let Some(entry) = entry_opt {
                all_facts.entry(source_id).or_default().push(entry);
            }
        }
    }

    // Pre-flight checks (mandatory, always run)
    eprintln!("Checking destination write permissions...");
    check_destination_writable(&base_dir)?;

    // Validate all pattern expansions succeed before any file operations
    eprint!(
        "Validating pattern expansions for {} sources...",
        filtered_sources.len()
    );
    let expansion_failures = validate_pattern_expansions(
        &filtered_sources,
        &pattern,
        &needed_keys,
        scope_prefix,
        &root_paths,
        &all_facts,
    );
    if !expansion_failures.is_empty() {
        eprintln!();
        eprintln!(
            "Error: {} sources failed pattern expansion:",
            expansion_failures.len()
        );
        for (path, error) in expansion_failures.iter().take(10) {
            eprintln!("  {path}: {error}");
        }
        if expansion_failures.len() > 10 {
            eprintln!("  ... and {} more", expansion_failures.len() - 10);
        }
        eprintln!("\nPattern requires facts that are missing for these sources.");
        eprintln!("Use 'canon facts' to check fact coverage, or adjust the pattern.");
        bail!("Aborting due to pattern expansion failures");
    }
    eprintln!(" ok");

    eprint!(
        "Checking {} sources for collisions and accessibility...",
        filtered_sources.len()
    );
    if options.dry_run {
        eprintln!(" (skipping source checks for speed in dry-run mode)");
    } else {
        eprintln!();
    }
    let access_check = check_destination_collisions_filtered(
        &filtered_sources,
        &pattern,
        &needed_keys,
        scope_prefix,
        &base_dir,
        &root_paths,
        &all_facts,
        options.dry_run,
    )?;

    if !access_check.unreadable.is_empty() {
        eprintln!(
            "Error: {} sources are not readable:",
            access_check.unreadable.len()
        );
        for (path, reason) in &access_check.unreadable {
            eprintln!("  {path} ({reason})");
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
                eprintln!("    {src}");
            }
        }
        bail!("Aborting due to destination collisions");
    }

    // Check for stale destination records (present=1 but file deleted, archive not rescanned)
    eprint!("Checking for stale destination records...");
    let stale_records = check_stale_destination_records(
        &*conn,
        &filtered_sources,
        &pattern,
        &needed_keys,
        scope_prefix,
        &config.output.base_dir,
        config.output.archive_root_id,
        &root_paths,
        &all_facts,
    )?;

    if !stale_records.is_empty() {
        eprintln!();
        eprintln!(
            "Error: {} destination paths have stale database records:",
            stale_records.len()
        );
        for path in stale_records.iter().take(10) {
            eprintln!("  {path}");
        }
        if stale_records.len() > 10 {
            eprintln!("  ... and {} more", stale_records.len() - 10);
        }
        eprintln!();
        eprintln!("These paths are marked as present in the database but the files are missing.");
        eprintln!("Run 'canon scan <archive-path>' to update the database, then retry.");
        bail!("Aborting due to stale destination records");
    }
    eprintln!(" ok");

    // Check for destination path conflicts (paths already occupied)
    // In resume mode, this check is skipped — work planning handles classification
    if !options.resume {
        eprint!("Checking for destination path conflicts...");
        let dest_conflicts = check_destination_conflicts(
            &*conn,
            &filtered_sources,
            &pattern,
            &needed_keys,
            scope_prefix,
            &base_dir,
            &config.output.base_dir,
            config.output.archive_root_id,
            &root_paths,
            &all_facts,
        )?;

        let total_conflicts = dest_conflicts.in_db.len() + dest_conflicts.on_disk_only.len();
        if total_conflicts > 0 {
            eprintln!();
            eprintln!(
                "Preflight failed: {total_conflicts} destination paths already exist."
            );
            eprintln!();
            if !dest_conflicts.in_db.is_empty() {
                eprintln!(
                    "Already registered in archive ({}):",
                    dest_conflicts.in_db.len()
                );
                for path in dest_conflicts.in_db.iter().take(5) {
                    eprintln!("  {path}");
                }
                if dest_conflicts.in_db.len() > 5 {
                    eprintln!("  ... and {} more", dest_conflicts.in_db.len() - 5);
                }
            }
            if !dest_conflicts.on_disk_only.is_empty() {
                eprintln!();
                eprintln!(
                    "Exist on disk but not in database ({}):",
                    dest_conflicts.on_disk_only.len()
                );
                for path in dest_conflicts.on_disk_only.iter().take(5) {
                    eprintln!("  {path}");
                }
                if dest_conflicts.on_disk_only.len() > 5 {
                    eprintln!("  ... and {} more", dest_conflicts.on_disk_only.len() - 5);
                }
            }
            eprintln!();
            eprintln!("This may be from a previously interrupted apply. To resume:");
            eprintln!("  canon apply --resume <manifest>");
            eprintln!();
            eprintln!("Or to see what would be skipped:");
            eprintln!("  canon apply --resume --dry-run <manifest>");
            eprintln!();
            eprintln!("If these are unexpected conflicts, run `canon scan <archive>` to update the database.");
            bail!("Aborting due to destination path conflicts");
        }
        eprintln!(" ok");
    }

    // Check archive conflicts
    eprintln!("Checking archive conflicts...");
    let conflicts =
        check_archive_conflicts_filtered(conn, &filtered_sources, config.output.archive_root_id)?;

    if !conflicts.in_dest_archive.is_empty() && !options.allow_duplicates {
        eprintln!(
            "Error: {} files already exist in destination archive:",
            conflicts.in_dest_archive.len()
        );
        for (src, dst) in &conflicts.in_dest_archive {
            eprintln!("  {src} -> {dst}");
        }
        eprintln!("\nUse --allow duplicates to proceed with duplicates included");
        bail!("Aborting due to files already in destination archive");
    }

    if !conflicts.in_other_archives.is_empty() && !options.allow_cross_archive_duplicates {
        eprintln!(
            "Error: {} files already exist in other archive(s):",
            conflicts.in_other_archives.len()
        );
        for (src, dst) in &conflicts.in_other_archives {
            eprintln!("  {src} -> {dst}");
        }
        eprintln!("\nUse --allow cross-archive-duplicates to proceed");
        bail!("Aborting due to files already in other archives");
    }

    // Check for excluded sources in manifest (hard gate, no override)
    // Sources may have been excluded after manifest generation — refuse to apply stale manifests
    eprintln!("Checking for excluded sources...");
    {
        let excluded_sources = check_excluded_sources_filtered(conn, &filtered_sources)?;
        if !excluded_sources.is_empty() {
            eprintln!(
                "Error: {} sources in manifest are marked as excluded:",
                excluded_sources.len()
            );
            for (id, path) in &excluded_sources {
                eprintln!("  {path} (id: {id})");
            }
            eprintln!("\nExcluded sources cannot be applied. Regenerate the manifest after clearing exclusions.");
            bail!("Aborting due to excluded sources in manifest");
        }
    }

    // Defense-in-depth: Check for sources from suspended roots
    // This catches the case where a root was suspended after a manifest was generated
    eprintln!("Checking for suspended roots...");
    {
        let suspended_sources = check_suspended_sources_filtered(conn, &filtered_sources)?;
        if !suspended_sources.is_empty() {
            eprintln!(
                "Error: {} sources in manifest are from suspended roots:",
                suspended_sources.len()
            );
            for (id, path) in &suspended_sources {
                eprintln!("  {path} (id: {id})");
            }
            eprintln!("\nSources from suspended roots cannot be applied.");
            eprintln!(
                "Use 'canon roots unsuspend' to reactivate the root, or regenerate the manifest."
            );
            bail!("Aborting due to sources from suspended roots");
        }
    }

    // Note: Snapshot fact validation removed. DB is source of truth for facts.
    // Apply looks up current fact values at runtime. If a fact changed, the new value is used.
    // Pattern expansion validation happens earlier (after "Checking destination write permissions").

    let mut stats = ApplyStats {
        skipped_filtered: skipped_by_filter as u64,
        ..Default::default()
    };

    // Phase 2: Work Planning
    // In resume mode, classify sources and determine what needs transfer
    // In regular mode, all filtered sources go to transfer
    // Work planning runs BEFORE source validation so that --resume with --rename/--move
    // can skip sources whose destinations already exist (source may be gone after rename).
    let sources_to_transfer: Vec<&LockEntry> = if options.resume {
        eprint!("Planning transfers (--resume mode)...");
        let work_plan = plan_transfers(
            &*conn,
            &filtered_sources,
            &pattern,
            &needed_keys,
            scope_prefix,
            &base_dir,
            &config.output.base_dir,
            config.output.archive_root_id,
            &root_paths,
            &all_facts,
        )?;
        eprintln!(" ok");

        // Report work plan
        eprintln!();
        eprintln!("Resume plan:");
        eprintln!("  Already archived: {}", work_plan.already_archived);
        eprintln!("  Resumed (need scan): {}", work_plan.resumed);
        eprintln!("  To transfer: {}", work_plan.to_transfer.len());

        // Record counts in stats
        stats.already_archived = work_plan.already_archived as u64;
        stats.resumed = work_plan.resumed as u64;

        work_plan.to_transfer
    } else {
        // Regular mode: all sources need transfer
        filtered_sources.clone()
    };

    // Validate source file states (only for sources that need transfer)
    // dry-run: fast DB check; real apply: thorough disk check
    // This runs after work planning so --resume can skip sources whose destinations
    // already exist — important for --rename/--move where source may be gone.
    if !sources_to_transfer.is_empty() {
        eprintln!("Validating source file states...");
        let stale = if options.dry_run {
            check_source_states_db(conn, &sources_to_transfer)?
        } else {
            check_source_states_disk(&sources_to_transfer)
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
    }

    // Track stale sources found during transfers (race condition detection)
    let mut stale_during_transfer: Vec<SkippedStaleSource> = Vec::new();

    // Phase 3: Transfer
    let total = sources_to_transfer.len();
    if total > 0 {
        let progress = Progress::new(total);
        eprintln!();
        eprintln!("Processing {total} sources...");

        for (i, source) in sources_to_transfer.iter().enumerate() {
            progress.update(i);

            match process_source(
                source,
                &pattern,
                &needed_keys,
                scope_prefix,
                &base_dir,
                &config.output.base_dir,
                options,
                &*conn,
                config.output.archive_root_id,
                &root_paths,
                &all_facts,
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

        progress.finish();
    } else if options.resume {
        eprintln!();
        eprintln!("No sources need transfer.");
    }

    // Summary of files that became stale during transfer (race conditions)
    if !stale_during_transfer.is_empty() {
        eprintln!(
            "\nSkipped {} files that changed during apply:",
            stale_during_transfer.len()
        );
        for s in stale_during_transfer.iter().take(10) {
            eprintln!("  {}: {}", s.path, s.reason);
        }
        if stale_during_transfer.len() > 10 {
            eprintln!("  ... and {} more", stale_during_transfer.len() - 10);
        }
        eprintln!("Run `canon scan` then `cluster refresh` to regenerate the lock file.");
    }

    // Summary output
    let mode = if options.dry_run { " (dry-run)" } else { "" };
    if options.resume {
        println!(
            "Applied{} (--resume): {} copied, {} renamed, {} moved, {} already archived, {} resumed, {} errors",
            mode, stats.copied, stats.renamed, stats.moved, stats.already_archived, stats.resumed, stats.errors
        );

        // Advisory when resumed files need scan
        if stats.resumed > 0 {
            eprintln!();
            eprintln!(
                "Note: {} resumed files are not yet registered. Run `canon scan <archive>` to complete.",
                stats.resumed
            );
        }
    } else {
        println!(
            "Applied{}: {} copied, {} renamed, {} moved, {} skipped (missing), {} skipped (stale), {} skipped (filtered), {} errors",
            mode, stats.copied, stats.renamed, stats.moved, stats.skipped_missing, stats.skipped_stale, stats.skipped_filtered, stats.errors
        );
    }

    // Recovery guidance when errors occurred
    if stats.errors > 0 && !options.dry_run {
        eprintln!();
        eprintln!("Some files failed to transfer. To recover:");
        eprintln!("  1. Fix any reported errors (permissions, disk space, etc.)");
        eprintln!("  2. Delete any partial files left in the archive");
        eprintln!("     (--resume will detect and report size mismatches)");
        eprintln!("  3. Re-run with --resume: canon apply --resume <manifest>");
        eprintln!();
        eprintln!("If source files changed during apply:");
        eprintln!("  1. Scan the sources: canon scan <source-paths>");
        eprintln!("  2. Refresh manifest: canon cluster refresh <manifest.toml>");
        eprintln!("  3. Re-apply: canon apply <manifest.lock>");
    }

    // Update query planner statistics after bulk changes (skip for dry-run)
    if !options.dry_run {
        db.run_analyze()?;
    }

    Ok(())
}

struct ArchiveConflicts {
    in_dest_archive: Vec<(String, String)>, // (source_path, archive_path)
    in_other_archives: Vec<(String, String)>, // (source_path, archive_path)
}

struct SourceAccessCheck {
    collisions: Vec<(PathBuf, Vec<String>)>, // (dest_path, source_paths)
    unreadable: Vec<(String, String)>,       // (source_path, error_message)
}

// ============================================================================
// Summary and confirmation helpers
// ============================================================================

fn print_apply_summary(
    config_path: &Path,
    base_dir: &Path,
    sources: &[&LockEntry],
    options: &ApplyOptions,
    root_paths: &HashMap<i64, String>,
) {
    eprintln!();
    eprintln!("=== Apply Summary ===");
    eprintln!("Manifest: {}", config_path.display());
    eprintln!("Destination: {}", base_dir.display());

    let mode_name = match options.transfer_mode {
        TransferMode::Copy => "copy",
        TransferMode::Rename => "rename (sources will be relocated)",
        TransferMode::Move => "move (sources will be deleted after copy)",
    };
    eprintln!("Mode: {mode_name}");
    eprintln!("Files: {}", sources.len());

    // "Sources from:" section for rename/move (not copy)
    if options.transfer_mode != TransferMode::Copy {
        let mut by_root: HashMap<i64, usize> = HashMap::new();
        for source in sources {
            *by_root.entry(source.root_id).or_insert(0) += 1;
        }
        let mut root_entries: Vec<(&str, usize)> = by_root
            .iter()
            .filter_map(|(root_id, count)| {
                root_paths.get(root_id).map(|p| (p.as_str(), *count))
            })
            .collect();
        root_entries.sort_by_key(|(path, _)| *path);

        eprintln!("Sources from:");
        for (path, count) in &root_entries {
            eprintln!("  {path}  ({count} files)");
        }
    }

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
        let suffix = entry
            .file_type()
            .map(|ft| if ft.is_dir() { "/" } else { "" })
            .unwrap_or("");
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
            eprintln!("  ... and {remaining} more");
        }
    }
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
            bail!(
                "Cannot find existing parent directory for {}",
                base_dir.display()
            );
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
            bail!(
                "No write permission for destination directory: {}",
                check_dir.display()
            );
        }
        Err(e) => {
            bail!(
                "Cannot write to destination directory {}: {}",
                check_dir.display(),
                e
            );
        }
    }
}

fn filter_by_roots<'a>(
    sources: &'a [LockEntry],
    root_specs: &[String],
    all_roots: &[crate::domain::root::Root],
) -> Result<Vec<&'a LockEntry>> {
    if root_specs.is_empty() {
        return Ok(sources.iter().collect());
    }

    let mut root_ids = HashSet::new();
    for spec in root_specs {
        let id = parse_root_spec(all_roots, spec, None)?;
        root_ids.insert(id);
    }

    Ok(sources
        .iter()
        .filter(|s| root_ids.contains(&s.root_id))
        .collect())
}

/// Validate that all sources can successfully expand the output pattern.
/// Returns list of (source_path, error_message) for sources that fail expansion.
fn validate_pattern_expansions(
    sources: &[&LockEntry],
    pattern: &Pattern,
    needed_keys: &[String],
    scope_prefix: Option<&str>,
    root_paths: &HashMap<i64, String>,
    all_facts: &HashMap<i64, Vec<FactEntry>>,
) -> Vec<(String, String)> {
    let mut failures = Vec::new();
    let total = sources.len();
    let progress = Progress::new(total);

    for (i, source) in sources.iter().enumerate() {
        progress.update(i);

        if let Err(e) = evaluate_pattern(
            pattern,
            source,
            needed_keys,
            scope_prefix,
            root_paths,
            all_facts,
        ) {
            failures.push((source.path.clone(), e.to_string()));
        }
    }

    progress.finish();

    failures
}

fn check_destination_collisions_filtered(
    sources: &[&LockEntry],
    pattern: &Pattern,
    needed_keys: &[String],
    scope_prefix: Option<&str>,
    base_dir: &Path,
    root_paths: &HashMap<i64, String>,
    all_facts: &HashMap<i64, Vec<FactEntry>>,
    dry_run: bool,
) -> Result<SourceAccessCheck> {
    let mut dest_to_sources: HashMap<PathBuf, Vec<String>> = HashMap::new();
    let mut unreadable: Vec<(String, String)> = Vec::new();
    let total = sources.len();
    let progress = Progress::new(total);

    for (i, source) in sources.iter().enumerate() {
        progress.update(i);

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
        let dest_rel = evaluate_pattern(
            pattern,
            source,
            needed_keys,
            scope_prefix,
            root_paths,
            all_facts,
        )?;
        let dest_path = base_dir.join(&dest_rel);

        dest_to_sources
            .entry(dest_path)
            .or_default()
            .push(source.path.clone());
    }

    progress.finish();

    // Filter to only collisions (more than one source per destination)
    let mut collisions: Vec<(PathBuf, Vec<String>)> = dest_to_sources
        .into_iter()
        .filter(|(_, sources)| sources.len() > 1)
        .collect();

    // Sort for consistent output
    collisions.sort_by(|a, b| a.0.cmp(&b.0));

    Ok(SourceAccessCheck {
        collisions,
        unreadable,
    })
}

/// Check for stale destination records in the database.
/// These are records with present=1 for paths we're about to copy to, but the file
/// doesn't exist on disk. This usually means the file was deleted but the archive
/// wasn't rescanned.
fn check_stale_destination_records(
    conn: &Connection,
    sources: &[&LockEntry],
    pattern: &Pattern,
    needed_keys: &[String],
    scope_prefix: Option<&str>,
    base_dir_rel: &str,
    archive_root_id: i64,
    root_paths: &HashMap<i64, String>,
    all_facts: &HashMap<i64, Vec<FactEntry>>,
) -> Result<Vec<String>> {
    let total = sources.len();
    let progress = Progress::new(total);

    // Collect destination rel_paths
    let mut dest_rel_paths: Vec<String> = Vec::with_capacity(sources.len());
    for (i, source) in sources.iter().enumerate() {
        progress.update(i);

        let dest_rel = evaluate_pattern(
            pattern,
            source,
            needed_keys,
            scope_prefix,
            root_paths,
            all_facts,
        )?;
        let archive_rel_path = if base_dir_rel.is_empty() {
            dest_rel
        } else {
            format!("{base_dir_rel}/{dest_rel}")
        };
        dest_rel_paths.push(archive_rel_path);
    }
    progress.finish();

    // Check which destination paths already exist in archive
    let path_refs: Vec<&str> = dest_rel_paths.iter().map(|s| s.as_str()).collect();
    let existing = repo::source::batch_check_paths_exist(conn, archive_root_id, &path_refs)?;
    let mut stale_paths: Vec<String> = existing.into_iter().collect();
    stale_paths.sort();
    Ok(stale_paths)
}

/// Result of destination conflict check.
struct DestinationConflicts {
    /// Paths that exist in DB with present=1
    in_db: Vec<String>,
    /// Paths that exist on disk but not in DB (orphan files)
    on_disk_only: Vec<String>,
}

/// Check for destination path conflicts.
///
/// In regular mode (no --resume), any destination path that is already occupied
/// is an error. This includes:
/// - Paths registered in DB with present=1 (already archived)
/// - Paths that exist on disk but not in DB (orphan files from interrupted apply)
///
/// This check runs in preflight to catch all conflicts before any transfers start,
/// giving the user a complete picture of what would fail.
fn check_destination_conflicts(
    conn: &Connection,
    sources: &[&LockEntry],
    pattern: &Pattern,
    needed_keys: &[String],
    scope_prefix: Option<&str>,
    base_dir: &Path,
    base_dir_rel: &str,
    archive_root_id: i64,
    root_paths: &HashMap<i64, String>,
    all_facts: &HashMap<i64, Vec<FactEntry>>,
) -> Result<DestinationConflicts> {
    let total = sources.len();
    let progress = Progress::new(total);

    // Collect destination rel_paths and full paths
    let mut dest_rel_paths: Vec<String> = Vec::with_capacity(sources.len());
    let mut dest_full_paths: Vec<PathBuf> = Vec::with_capacity(sources.len());
    for (i, source) in sources.iter().enumerate() {
        progress.update(i);

        let dest_rel = evaluate_pattern(
            pattern,
            source,
            needed_keys,
            scope_prefix,
            root_paths,
            all_facts,
        )?;
        let archive_rel_path = if base_dir_rel.is_empty() {
            dest_rel.clone()
        } else {
            format!("{base_dir_rel}/{dest_rel}")
        };
        dest_rel_paths.push(archive_rel_path);
        dest_full_paths.push(base_dir.join(&dest_rel));
    }
    progress.finish();

    // Check which paths exist in DB
    let path_refs: Vec<&str> = dest_rel_paths.iter().map(|s| s.as_str()).collect();
    let in_db_set = repo::source::batch_check_paths_exist(conn, archive_root_id, &path_refs)?;

    let in_db: Vec<String> = in_db_set.into_iter().collect();

    // For paths NOT in DB, check if file exists on disk
    let mut on_disk_only = Vec::new();
    for (i, rel_path) in dest_rel_paths.iter().enumerate() {
        // Skip if already in DB (will be reported separately)
        if in_db.iter().any(|p| p == rel_path) {
            continue;
        }
        // Check if file exists on disk
        if dest_full_paths[i].exists() {
            on_disk_only.push(rel_path.clone());
        }
    }

    Ok(DestinationConflicts {
        in_db,
        on_disk_only,
    })
}

/// Plan which transfers need to be executed in resume mode.
///
/// Classifies each source into one of:
/// - `to_transfer`: Not in DB, not on disk — needs copying
/// - `already_archived`: In DB with present=1 — skip (fully complete)
/// - `resumed`: On disk but not in DB, size matches — skip (needs scan to register)
///
/// If any size mismatches are found (partial/corrupted files), collects all of them
/// and returns an error after checking all sources.
fn plan_transfers<'a>(
    conn: &Connection,
    sources: &[&'a LockEntry],
    pattern: &Pattern,
    needed_keys: &[String],
    scope_prefix: Option<&str>,
    base_dir: &Path,
    base_dir_rel: &str,
    archive_root_id: i64,
    root_paths: &HashMap<i64, String>,
    all_facts: &HashMap<i64, Vec<FactEntry>>,
) -> Result<WorkPlan<'a>> {
    let total = sources.len();
    let progress = Progress::new(total);

    // Collect destination paths for all sources
    let mut dest_info: Vec<(String, PathBuf, u64)> = Vec::with_capacity(sources.len());
    for (i, source) in sources.iter().enumerate() {
        progress.update(i);

        let dest_rel = evaluate_pattern(
            pattern,
            source,
            needed_keys,
            scope_prefix,
            root_paths,
            all_facts,
        )?;
        let archive_rel_path = if base_dir_rel.is_empty() {
            dest_rel.clone()
        } else {
            format!("{base_dir_rel}/{dest_rel}")
        };
        let full_path = base_dir.join(&dest_rel);
        dest_info.push((archive_rel_path, full_path, source.size as u64));
    }
    progress.finish();

    // Check which paths exist in DB
    let path_refs: Vec<&str> = dest_info.iter().map(|(p, _, _)| p.as_str()).collect();
    let in_db_set = repo::source::batch_check_paths_exist(conn, archive_root_id, &path_refs)?;

    // Classify each source
    let mut to_transfer = Vec::new();
    let mut already_archived = 0usize;
    let mut resumed = 0usize;
    let mut size_mismatches = Vec::new();

    for (i, source) in sources.iter().enumerate() {
        let (ref rel_path, ref full_path, expected_size) = dest_info[i];

        let in_db = in_db_set.contains(rel_path);

        // Only check disk if not in DB (D4: trust DB without disk verification)
        let on_disk = if in_db {
            None
        } else if full_path.exists() {
            // Get file size
            match fs::metadata(full_path) {
                Ok(meta) => Some(meta.len()),
                Err(_) => None, // Treat read errors as "not on disk"
            }
        } else {
            None
        };

        let state = classify_destination(in_db, on_disk, expected_size);

        match state {
            DestinationState::Available => {
                to_transfer.push(*source);
            }
            DestinationState::Archived => {
                already_archived += 1;
            }
            DestinationState::Resumed => {
                resumed += 1;
            }
            DestinationState::SizeMismatch { expected, actual } => {
                size_mismatches.push(SizeMismatchError {
                    dest_path: full_path.display().to_string(),
                    expected,
                    actual,
                });
            }
        }
    }

    // If any size mismatches, fail with collected errors
    if !size_mismatches.is_empty() {
        eprintln!();
        eprintln!(
            "Work planning found {} partial/mismatched files:",
            size_mismatches.len()
        );
        for err in size_mismatches.iter().take(10) {
            eprintln!(
                "  {} (expected {} bytes, found {})",
                err.dest_path, err.expected, err.actual
            );
        }
        if size_mismatches.len() > 10 {
            eprintln!("  ... and {} more", size_mismatches.len() - 10);
        }
        eprintln!();
        eprintln!("These may be from an interrupted transfer. To resolve:");
        eprintln!("  1. Delete the partial files");
        eprintln!("  2. Re-run: canon apply --resume <manifest>");
        bail!("Aborting due to size mismatches in destination files");
    }

    Ok(WorkPlan {
        to_transfer,
        already_archived,
        resumed,
    })
}

/// Check for duplicate content that already exists in archives.
///
/// Returns two categories of conflicts:
/// - `in_dest_archive`: Sources whose content is already in the destination archive
/// - `in_other_archives`: Sources whose content is in a different archive
///
/// Uses batch fetching to check all hashes in a single query (eliminates N+1).
fn check_archive_conflicts_filtered(
    conn: &Connection,
    sources: &[&LockEntry],
    dest_archive_id: i64,
) -> Result<ArchiveConflicts> {
    let mut conflicts = ArchiveConflicts {
        in_dest_archive: Vec::new(),
        in_other_archives: Vec::new(),
    };

    // Collect all hash values that need checking
    let hash_values: Vec<&str> = sources
        .iter()
        .filter_map(|s| s.hash_value.as_deref())
        .collect();

    if hash_values.is_empty() {
        return Ok(conflicts);
    }

    // Batch fetch archive info for all hashes (eliminates N+1 queries)
    let archive_info = repo::object::batch_find_archive_info_by_hash(conn, &hash_values)?;

    // Check each source against the batch results
    for source in sources {
        if let Some(ref hash) = source.hash_value {
            if let Some(info_list) = archive_info.get(hash) {
                // Use first match (consistent with previous LIMIT 1 behavior)
                if let Some(&(archive_id, ref archive_path)) = info_list.first() {
                    if archive_id == dest_archive_id {
                        conflicts
                            .in_dest_archive
                            .push((source.path.clone(), archive_path.clone()));
                    } else {
                        conflicts
                            .in_other_archives
                            .push((source.path.clone(), archive_path.clone()));
                    }
                }
            }
        }
    }

    Ok(conflicts)
}

/// Check that all sources in manifest have content hashes.
/// Unhashed sources cannot be applied - deduplication requires content hashes.
fn check_unhashed_sources(sources: &[LockEntry]) -> Result<()> {
    let unhashed: Vec<_> = sources.iter().filter(|s| s.object_id.is_none()).collect();

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
    let (total, unhashed) = repo::source::count_unhashed_for_root(conn, archive_root_id)?;

    if unhashed > 0 {
        bail!(
            "Destination archive has {unhashed} files without content hash (out of {total})\n\
             Cannot reliably detect duplicates without complete hash coverage.\n\
             Run 'canon scan <archive-path>' to index and hash the archive."
        );
    }
    Ok(())
}

fn check_excluded_sources_filtered(
    conn: &Connection,
    sources: &[&LockEntry],
) -> Result<Vec<(i64, String)>> {
    // Batch fetch sources and check exclusion via domain predicate
    let source_ids: Vec<i64> = sources.iter().map(|s| s.id).collect();
    let sources_map = repo::source::batch_fetch_by_ids(conn, &source_ids)?;

    let excluded: Vec<(i64, String)> = sources
        .iter()
        .filter(|lock_entry| {
            sources_map
                .get(&lock_entry.id)
                .map(|s| s.is_excluded())
                .unwrap_or(false)
        })
        .map(|lock_entry| (lock_entry.id, lock_entry.path.clone()))
        .collect();

    Ok(excluded)
}

fn check_suspended_sources_filtered(
    conn: &Connection,
    sources: &[&LockEntry],
) -> Result<Vec<(i64, String)>> {
    // Batch fetch sources and check suspension via domain predicate
    let source_ids: Vec<i64> = sources.iter().map(|s| s.id).collect();
    let sources_map = repo::source::batch_fetch_by_ids(conn, &source_ids)?;

    let suspended: Vec<(i64, String)> = sources
        .iter()
        .filter(|lock_entry| {
            sources_map
                .get(&lock_entry.id)
                .map(|s| !s.is_active()) // is_active() checks root_suspended
                .unwrap_or(false) // Treat missing source as not suspended (will fail elsewhere)
        })
        .map(|lock_entry| (lock_entry.id, lock_entry.path.clone()))
        .collect();

    Ok(suspended)
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
            return Err(format!("cannot stat: {e}"));
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
        let current_mtime = meta
            .modified()
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
        .map_err(|e| format!("failed to compute partial hash: {e}"))?;
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
    let progress = Progress::new(total);

    for (i, source) in sources.iter().enumerate() {
        progress.update(i);

        if let Err(reason) = validate_source_state(source) {
            stale.push(SkippedStaleSource {
                path: source.path.clone(),
                reason,
            });
        }
    }

    progress.finish();

    stale
}

/// Batch validate source file states against DB values. Returns list of stale sources.
/// Used in dry-run mode for faster validation without disk access.
fn check_source_states_db(
    conn: &Connection,
    sources: &[&LockEntry],
) -> Result<Vec<SkippedStaleSource>> {
    // Batch fetch all sources from DB
    let source_ids: Vec<i64> = sources.iter().map(|s| s.id).collect();
    let sources_map = repo::source::batch_fetch_by_ids(conn, &source_ids)?;

    let mut stale = Vec::new();

    for lock_entry in sources {
        match sources_map.get(&lock_entry.id) {
            None => {
                // batch_fetch_by_ids excludes non-present sources, so this means
                // either not found OR not present
                stale.push(SkippedStaleSource {
                    path: lock_entry.path.clone(),
                    reason: "source not found or not present in DB".to_string(),
                });
            }
            Some(db_source) => {
                // Compare lock entry values against Source struct fields
                let mut mismatches = Vec::new();

                if db_source.size != lock_entry.size {
                    mismatches.push(format!("size: {} → {}", lock_entry.size, db_source.size));
                }
                if db_source.mtime != lock_entry.mtime {
                    mismatches.push(format!("mtime: {} → {}", lock_entry.mtime, db_source.mtime));
                }
                // Compare partial_hash from lock vs DB
                if db_source.partial_hash.is_empty() {
                    mismatches.push("partial hash: missing in DB".to_string());
                } else if db_source.partial_hash != lock_entry.partial_hash {
                    mismatches.push(format!(
                        "partial hash: {}... → {}...",
                        &lock_entry.partial_hash[..16.min(lock_entry.partial_hash.len())],
                        &db_source.partial_hash[..16.min(db_source.partial_hash.len())]
                    ));
                }

                if !mismatches.is_empty() {
                    stale.push(SkippedStaleSource {
                        path: lock_entry.path.clone(),
                        reason: mismatches.join(", "),
                    });
                }
            }
        }
    }

    Ok(stale)
}

enum ApplyAction {
    Copied,
    Renamed,
    Moved,
    SkippedMissing,
    SkippedStale(String), // reason
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
    all_facts: &HashMap<i64, Vec<FactEntry>>,
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
    let dest_rel = evaluate_pattern(
        pattern,
        source,
        needed_keys,
        scope_prefix,
        root_paths,
        all_facts,
    )?;
    let dest_path = base_dir.join(&dest_rel);

    // Compute relative path within archive root for registration
    let archive_rel_path = if base_dir_rel.is_empty() {
        dest_rel.clone()
    } else {
        format!("{base_dir_rel}/{dest_rel}")
    };

    if options.dry_run {
        match options.transfer_mode {
            TransferMode::Copy => {
                println!("[dry-run] COPY: {} -> {}", source.path, dest_path.display());
                return Ok(ApplyAction::Copied);
            }
            TransferMode::Rename => {
                println!(
                    "[dry-run] RENAME: {} -> {}",
                    source.path,
                    dest_path.display()
                );
                return Ok(ApplyAction::Renamed);
            }
            TransferMode::Move => {
                println!(
                    "[dry-run] MOVE: {} -> {} (would delete source; may copy if cross-device)",
                    source.path,
                    dest_path.display()
                );
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
            fs::copy(src_path, &dest_path).with_context(|| {
                format!("Failed to copy {} to {}", source.path, dest_path.display())
            })?;
            preserve_metadata(&dest_path, &src_meta)?;
            let new_source = build_new_source(
                &dest_path,
                archive_root_id,
                &archive_rel_path,
                source.object_id,
                &source.partial_hash,
            )?;
            repo::source::insert_destination(conn, &new_source)?;
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
            fs::rename(src_path, &dest_path).with_context(|| {
                format!(
                    "Failed to rename {} to {}",
                    source.path,
                    dest_path.display()
                )
            })?;
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
                    fs::copy(src_path, &dest_path).with_context(|| {
                        format!("Failed to copy {} to {}", source.path, dest_path.display())
                    })?;
                    preserve_metadata(&dest_path, &src_meta)?;
                    fs::remove_file(src_path)
                        .with_context(|| format!("Failed to delete source: {}", source.path))?;
                    // Mark old source as not present (file was deleted)
                    mark_source_not_present(conn, source.id)?;
                    // Register new destination (new inode on different device)
                    let new_source = build_new_source(
                        &dest_path,
                        archive_root_id,
                        &archive_rel_path,
                        source.object_id,
                        &source.partial_hash,
                    )?;
                    repo::source::insert_destination(conn, &new_source)?;
                    if options.verbose {
                        println!("Moved: {} -> {}", source.path, dest_path.display());
                    }
                    Ok(ApplyAction::Moved)
                }
                Err(e) => Err(e).with_context(|| {
                    format!(
                        "Failed to rename {} to {}",
                        source.path,
                        dest_path.display()
                    )
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

    repo::source::update_location(conn, source_id, archive_root_id, rel_path, now)
}

/// Mark a source as no longer present (for cross-device move after deletion).
fn mark_source_not_present(conn: &Connection, source_id: i64) -> Result<()> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs() as i64;

    repo::source::mark_missing(conn, &[source_id], now)?;
    Ok(())
}

/// Build a NewSource from destination file metadata for registration.
///
/// Reads metadata from the destination file and constructs a NewSource
/// struct suitable for passing to repo::source::insert_destination().
#[cfg(unix)]
fn build_new_source(
    dest_path: &Path,
    archive_root_id: i64,
    rel_path: &str,
    object_id: Option<i64>,
    partial_hash: &str,
) -> Result<NewSource> {
    let meta = fs::metadata(dest_path).with_context(|| {
        format!(
            "Failed to read metadata for registration: {}",
            dest_path.display()
        )
    })?;

    Ok(NewSource {
        root_id: archive_root_id,
        rel_path: rel_path.to_string(),
        size: meta.size() as i64,
        mtime: meta.mtime(),
        partial_hash: partial_hash.to_string(),
        object_id,
        device: Some(meta.dev() as i64),
        inode: Some(meta.ino() as i64),
    })
}

/// Build a NewSource from destination file metadata for registration.
///
/// Non-Unix version: device and inode are not available.
#[cfg(not(unix))]
fn build_new_source(
    dest_path: &Path,
    archive_root_id: i64,
    rel_path: &str,
    object_id: Option<i64>,
    partial_hash: &str,
) -> Result<NewSource> {
    let meta = fs::metadata(dest_path).with_context(|| {
        format!(
            "Failed to read metadata for registration: {}",
            dest_path.display()
        )
    })?;

    let mtime = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    Ok(NewSource {
        root_id: archive_root_id,
        rel_path: rel_path.to_string(),
        size: meta.len() as i64,
        mtime,
        partial_hash: partial_hash.to_string(),
        object_id,
        device: None,
        inode: None,
    })
}
