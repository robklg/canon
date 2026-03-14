use anyhow::{bail, Context, Result};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, ErrorKind};
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::ceremony;
use crate::cluster::{self, ManifestConfig};
use crate::domain::apply::{classify_destination, DestinationState};
use crate::domain::root::parse_root_spec;
use crate::domain::source::NewSource;
use crate::expr;
use crate::ops;
use crate::ops::apply::TransferMode;
use crate::ops::cluster::LockEntry;
use crate::ops::fs::{compute_partial_hash, preserve_metadata};
use crate::progress::Progress;
use crate::repo::{self, Connection, Db};

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

/// Result of disk classification in resume mode.
/// DB classification is done by plan_apply(); this handles the disk part.
struct DiskWorkPlan<'a> {
    /// Transfers that need to be executed (not on disk)
    to_transfer: Vec<&'a ops::apply::ApplyTransfer>,
    /// Count of files on disk but not in DB (skipped, need scan)
    resumed: usize,
}

/// A size mismatch found during work planning.
struct SizeMismatchError {
    dest_path: String,
    expected: u64,
    actual: u64,
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

    // Merge manifest [options] with CLI options
    let (_, manifest_duplicates) = cluster::parse_manifest_allow(&config.options.allow)?;
    let allow_duplicates = options.allow_duplicates || manifest_duplicates;
    if manifest_duplicates && !options.allow_duplicates {
        eprintln!(
            "Options from manifest: allow {}",
            config.options.allow.join(", ")
        );
    }

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
    print_apply_summary(
        &config_path,
        &base_dir,
        &filtered_sources,
        options,
        &root_paths,
    );

    if !ceremony::confirm(options.yes)? {
        return Ok(());
    }

    // --- Plan: compute all DB-based preflight checks and destination paths ---

    eprintln!("Running preflight checks...");
    let plan = ops::apply::plan_apply(
        conn,
        &ops::apply::ApplyPlanParams {
            sources: &filtered_sources,
            pattern: &pattern,
            needed_keys: &needed_keys,
            scope_prefix,
            root_paths: &root_paths,
            archive_root_id: config.output.archive_root_id,
            base_dir_rel: &config.output.base_dir,
            resume: options.resume,
        },
    )?;

    // --- Inspect violations (preserving error messages and bail behavior) ---

    let v = &plan.violations;

    if !v.expansion_failures.is_empty() {
        eprintln!(
            "Error: {} sources failed pattern expansion:",
            v.expansion_failures.len()
        );
        for (path, error) in v.expansion_failures.iter().take(10) {
            eprintln!("  {path}: {error}");
        }
        if v.expansion_failures.len() > 10 {
            eprintln!("  ... and {} more", v.expansion_failures.len() - 10);
        }
        eprintln!("\nPattern requires facts that are missing for these sources.");
        eprintln!("Use 'canon facts' to check fact coverage, or adjust the pattern.");
        bail!("Aborting due to pattern expansion failures");
    }

    if !v.collisions.is_empty() {
        eprintln!(
            "Error: {} destination paths have multiple sources:",
            v.collisions.len()
        );
        for (dest, sources) in &v.collisions {
            eprintln!("  {} <- {} files:", dest, sources.len());
            for src in sources {
                eprintln!("    {src}");
            }
        }
        bail!("Aborting due to destination collisions");
    }

    if !v.stale_records.is_empty() {
        eprintln!(
            "Error: {} destination paths have stale database records:",
            v.stale_records.len()
        );
        for path in v.stale_records.iter().take(10) {
            eprintln!("  {path}");
        }
        if v.stale_records.len() > 10 {
            eprintln!("  ... and {} more", v.stale_records.len() - 10);
        }
        eprintln!();
        eprintln!("These paths are marked as present in the database but the files are missing.");
        eprintln!("Run 'canon scan <archive-path>' to update the database, then retry.");
        bail!("Aborting due to stale destination records");
    }

    // Destination path conflicts (non-resume only): DB part from plan + on-disk check
    // Destination path conflicts (non-resume only):
    // DB conflicts come from plan, disk-only conflicts checked here
    if !options.resume {
        let mut on_disk_only = Vec::new();
        for transfer in &plan.transfers {
            let dest_path = base_dir.join(&transfer.dest_rel_path);
            if !v.dest_conflicts_in_db.contains(&transfer.archive_rel_path) && dest_path.exists() {
                on_disk_only.push(transfer.archive_rel_path.clone());
            }
        }

        let total_conflicts = v.dest_conflicts_in_db.len() + on_disk_only.len();
        if total_conflicts > 0 {
            eprintln!("Preflight failed: {total_conflicts} destination paths already exist.");
            eprintln!();
            if !v.dest_conflicts_in_db.is_empty() {
                eprintln!(
                    "Already registered in archive ({}):",
                    v.dest_conflicts_in_db.len()
                );
                for path in v.dest_conflicts_in_db.iter().take(5) {
                    eprintln!("  {path}");
                }
                if v.dest_conflicts_in_db.len() > 5 {
                    eprintln!("  ... and {} more", v.dest_conflicts_in_db.len() - 5);
                }
            }
            if !on_disk_only.is_empty() {
                eprintln!();
                eprintln!(
                    "Exist on disk but not in database ({}):",
                    on_disk_only.len()
                );
                for path in on_disk_only.iter().take(5) {
                    eprintln!("  {path}");
                }
                if on_disk_only.len() > 5 {
                    eprintln!("  ... and {} more", on_disk_only.len() - 5);
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
    }

    if !v.archive_conflicts_dest.is_empty() && !allow_duplicates {
        eprintln!(
            "Error: {} files already exist in destination archive:",
            v.archive_conflicts_dest.len()
        );
        for (src, dst) in &v.archive_conflicts_dest {
            eprintln!("  {src} -> {dst}");
        }
        eprintln!("\nUse --allow duplicates to proceed with duplicates included");
        bail!("Aborting due to files already in destination archive");
    }

    if !v.archive_conflicts_other.is_empty() && !options.allow_cross_archive_duplicates {
        eprintln!(
            "Error: {} files already exist in other archive(s):",
            v.archive_conflicts_other.len()
        );
        for (src, dst) in &v.archive_conflicts_other {
            eprintln!("  {src} -> {dst}");
        }
        eprintln!("\nUse --allow cross-archive-duplicates to proceed");
        bail!("Aborting due to files already in other archives");
    }

    if !v.excluded_sources.is_empty() {
        eprintln!(
            "Error: {} sources in manifest are marked as excluded:",
            v.excluded_sources.len()
        );
        for (id, path) in &v.excluded_sources {
            eprintln!("  {path} (id: {id})");
        }
        eprintln!("\nExcluded sources cannot be applied. Regenerate the manifest after clearing exclusions.");
        bail!("Aborting due to excluded sources in manifest");
    }

    if !v.suspended_sources.is_empty() {
        eprintln!(
            "Error: {} sources in manifest are from suspended roots:",
            v.suspended_sources.len()
        );
        for (id, path) in &v.suspended_sources {
            eprintln!("  {path} (id: {id})");
        }
        eprintln!("\nSources from suspended roots cannot be applied.");
        eprintln!(
            "Use 'canon roots unsuspend' to reactivate the root, or regenerate the manifest."
        );
        bail!("Aborting due to sources from suspended roots");
    }

    // --- Filesystem checks (stay in interface) ---

    eprintln!("Checking destination write permissions...");
    check_destination_writable(&base_dir)?;

    // Check source readability (filesystem — skip in dry-run)
    if !options.dry_run {
        let mut unreadable: Vec<(String, String)> = Vec::new();
        for transfer in &plan.transfers {
            match File::open(&transfer.source_path) {
                Ok(_) => {}
                Err(e) if e.kind() == ErrorKind::NotFound => continue,
                Err(e) if e.kind() == ErrorKind::PermissionDenied => {
                    unreadable.push((transfer.source_path.clone(), "permission denied".to_string()));
                }
                Err(e) => {
                    unreadable.push((transfer.source_path.clone(), e.to_string()));
                }
            }
        }
        if !unreadable.is_empty() {
            eprintln!(
                "Error: {} sources are not readable:",
                unreadable.len()
            );
            for (path, reason) in &unreadable {
                eprintln!("  {path} ({reason})");
            }
            bail!("Aborting due to unreadable sources");
        }
    }

    let mut stats = ApplyStats {
        skipped_filtered: skipped_by_filter as u64,
        ..Default::default()
    };

    // --- Resume mode: disk classification ---
    // plan.transfers already excludes sources in DB. Now classify on disk.
    // In non-resume mode, all plan.transfers go to execution.
    let transfers_to_execute: Vec<&ops::apply::ApplyTransfer> = if options.resume {
        eprint!("Planning transfers (--resume mode)...");
        let work_plan = plan_transfers_disk(
            &plan.transfers,
            &base_dir,
        )?;
        eprintln!(" ok");

        eprintln!();
        eprintln!("Resume plan:");
        eprintln!("  Already archived: {}", plan.already_archived_count);
        eprintln!("  Resumed (need scan): {}", work_plan.resumed);
        eprintln!("  To transfer: {}", work_plan.to_transfer.len());

        stats.already_archived = plan.already_archived_count as u64;
        stats.resumed = work_plan.resumed as u64;

        work_plan.to_transfer
    } else {
        plan.transfers.iter().collect()
    };

    // Validate source file states (only for sources that need transfer)
    // dry-run: use plan.stale_sources (DB check already done in plan)
    // real apply: thorough disk check
    if !transfers_to_execute.is_empty() {
        eprintln!("Validating source file states...");
        if options.dry_run {
            // Use pre-computed DB-based staleness from plan
            if !plan.stale_sources.is_empty() {
                eprintln!(
                    "Error: {} sources have changed since manifest was generated:",
                    plan.stale_sources.len()
                );
                for s in plan.stale_sources.iter().take(10) {
                    eprintln!("  {}: {}", s.path, s.reason);
                }
                if plan.stale_sources.len() > 10 {
                    eprintln!("  ... and {} more", plan.stale_sources.len() - 10);
                }
                eprintln!("\nRun `canon scan` then `cluster refresh` to regenerate the lock file.");
                bail!("Aborting due to stale sources in manifest");
            }
        } else {
            let stale = check_source_states_disk_from_transfers(&transfers_to_execute);
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
    }

    // Track stale sources found during transfers (race condition detection)
    let mut stale_during_transfer: Vec<SkippedStaleSource> = Vec::new();

    // Phase 3: Transfer
    let total = transfers_to_execute.len();
    if total > 0 {
        let progress = Progress::new(total);
        eprintln!();
        eprintln!("Processing {total} sources...");

        for (i, transfer) in transfers_to_execute.iter().enumerate() {
            progress.update(i);

            match process_source(
                transfer,
                &base_dir,
                options,
                conn,
                config.output.archive_root_id,
            ) {
                Ok(action) => match action {
                    ApplyAction::Copied => stats.copied += 1,
                    ApplyAction::Renamed => stats.renamed += 1,
                    ApplyAction::Moved => stats.moved += 1,
                    ApplyAction::SkippedMissing => stats.skipped_missing += 1,
                    ApplyAction::SkippedStale(reason) => {
                        stats.skipped_stale += 1;
                        stale_during_transfer.push(SkippedStaleSource {
                            path: transfer.source_path.clone(),
                            reason,
                        });
                    }
                },
                Err(e) => {
                    eprintln!("Error processing {}: {}", transfer.source_path, e);
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
            .filter_map(|(root_id, count)| root_paths.get(root_id).map(|p| (p.as_str(), *count)))
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

/// Classify plan transfers on disk for resume mode.
///
/// plan.transfers already excludes sources in DB (done by plan_apply).
/// This function checks each remaining transfer on disk:
/// - `to_transfer`: Not on disk — needs copying
/// - `resumed`: On disk, size matches — skip (needs scan to register)
/// - Size mismatch → error
fn plan_transfers_disk<'a>(
    transfers: &'a [ops::apply::ApplyTransfer],
    base_dir: &Path,
) -> Result<DiskWorkPlan<'a>> {
    let mut to_transfer = Vec::new();
    let mut resumed = 0usize;
    let mut size_mismatches = Vec::new();

    for transfer in transfers {
        let full_path = base_dir.join(&transfer.dest_rel_path);
        let expected_size = transfer.size as u64;

        // Check disk state (DB filtering already done by plan)
        let on_disk = if full_path.exists() {
            match fs::metadata(&full_path) {
                Ok(meta) => Some(meta.len()),
                Err(_) => None,
            }
        } else {
            None
        };

        let state = classify_destination(false, on_disk, expected_size);

        match state {
            DestinationState::Available => {
                to_transfer.push(transfer);
            }
            DestinationState::Archived => {
                // Should not happen (in_db=false always), but handle gracefully
                to_transfer.push(transfer);
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

    Ok(DiskWorkPlan {
        to_transfer,
        resumed,
    })
}

/// Validate that a source file on disk matches the state recorded in the transfer.
/// Returns Ok(()) if valid, Err with reason if changed.
fn validate_source_state_from_transfer(
    transfer: &ops::apply::ApplyTransfer,
) -> std::result::Result<(), String> {
    let meta = match fs::metadata(&transfer.source_path) {
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
        let current_size = meta.size() as i64;
        let current_mtime = meta.mtime();

        if current_size != transfer.size {
            mismatches.push(format!("size: {} → {}", transfer.size, current_size));
        }
        if current_mtime != transfer.mtime {
            mismatches.push(format!("mtime: {} → {}", transfer.mtime, current_mtime));
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

        if current_size != transfer.size {
            mismatches.push(format!("size: {} → {}", transfer.size, current_size));
        }
        if current_mtime != transfer.mtime {
            mismatches.push(format!("mtime: {} → {}", transfer.mtime, current_mtime));
        }
    }

    // Partial hash check - recompute from disk and compare to lock
    let current_hash =
        compute_partial_hash(Path::new(&transfer.source_path), transfer.size as u64)
            .map_err(|e| format!("failed to compute partial hash: {e}"))?;
    if current_hash != transfer.partial_hash {
        mismatches.push(format!(
            "partial hash mismatch: {}... → {}...",
            &transfer.partial_hash[..16.min(transfer.partial_hash.len())],
            &current_hash[..16]
        ));
    }

    if !mismatches.is_empty() {
        Err(mismatches.join(", "))
    } else {
        Ok(())
    }
}

/// Batch validate source file states against disk using transfers.
fn check_source_states_disk_from_transfers(
    transfers: &[&ops::apply::ApplyTransfer],
) -> Vec<SkippedStaleSource> {
    let mut stale = Vec::new();
    let total = transfers.len();
    let progress = Progress::new(total);

    for (i, transfer) in transfers.iter().enumerate() {
        progress.update(i);

        if let Err(reason) = validate_source_state_from_transfer(transfer) {
            stale.push(SkippedStaleSource {
                path: transfer.source_path.clone(),
                reason,
            });
        }
    }

    progress.finish();

    stale
}

enum ApplyAction {
    Copied,
    Renamed,
    Moved,
    SkippedMissing,
    SkippedStale(String), // reason
}

fn process_source(
    transfer: &ops::apply::ApplyTransfer,
    base_dir: &Path,
    options: &ApplyOptions,
    conn: &Connection,
    archive_root_id: i64,
) -> Result<ApplyAction> {
    let src_path = Path::new(&transfer.source_path);
    let dest_path = base_dir.join(&transfer.dest_rel_path);

    // Check if source exists
    if !src_path.exists() {
        if options.dry_run {
            println!("[dry-run] SKIP (missing): {}", transfer.source_path);
        }
        return Ok(ApplyAction::SkippedMissing);
    }

    if options.dry_run {
        match options.transfer_mode {
            TransferMode::Copy => {
                println!("[dry-run] COPY: {} -> {}", transfer.source_path, dest_path.display());
                return Ok(ApplyAction::Copied);
            }
            TransferMode::Rename => {
                println!(
                    "[dry-run] RENAME: {} -> {}",
                    transfer.source_path,
                    dest_path.display()
                );
                return Ok(ApplyAction::Renamed);
            }
            TransferMode::Move => {
                println!(
                    "[dry-run] MOVE: {} -> {} (would delete source; may copy if cross-device)",
                    transfer.source_path,
                    dest_path.display()
                );
                return Ok(ApplyAction::Moved);
            }
        }
    }

    // Per-transfer validation: check source hasn't changed since preflight
    // (catches race conditions where file changes between preflight and transfer)
    if let Err(reason) = validate_source_state_from_transfer(transfer) {
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
                .with_context(|| format!("Failed to read metadata: {}", transfer.source_path))?;
            fs::copy(src_path, &dest_path).with_context(|| {
                format!("Failed to copy {} to {}", transfer.source_path, dest_path.display())
            })?;
            preserve_metadata(&dest_path, &src_meta)?;
            let new_source = build_new_source(
                &dest_path,
                archive_root_id,
                &transfer.archive_rel_path,
                transfer.object_id,
                &transfer.partial_hash,
            )?;
            repo::source::insert_destination(conn, &new_source)?;
            if options.verbose {
                println!("Copied: {} -> {}", transfer.source_path, dest_path.display());
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
                    transfer.source_path,
                    dest_path.display()
                )
            })?;
            // Update existing source row (inode unchanged on same device)
            relocate_source(conn, transfer.source_id, archive_root_id, &transfer.archive_rel_path)?;
            if options.verbose {
                println!("Renamed: {} -> {}", transfer.source_path, dest_path.display());
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
                    relocate_source(conn, transfer.source_id, archive_root_id, &transfer.archive_rel_path)?;
                    if options.verbose {
                        println!("Renamed: {} -> {}", transfer.source_path, dest_path.display());
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
                        .with_context(|| format!("Failed to read metadata: {}", transfer.source_path))?;
                    fs::copy(src_path, &dest_path).with_context(|| {
                        format!("Failed to copy {} to {}", transfer.source_path, dest_path.display())
                    })?;
                    preserve_metadata(&dest_path, &src_meta)?;
                    fs::remove_file(src_path)
                        .with_context(|| format!("Failed to delete source: {}", transfer.source_path))?;
                    // Mark old source as not present (file was deleted)
                    mark_source_not_present(conn, transfer.source_id)?;
                    // Register new destination (new inode on different device)
                    let new_source = build_new_source(
                        &dest_path,
                        archive_root_id,
                        &transfer.archive_rel_path,
                        transfer.object_id,
                        &transfer.partial_hash,
                    )?;
                    repo::source::insert_destination(conn, &new_source)?;
                    if options.verbose {
                        println!("Moved: {} -> {}", transfer.source_path, dest_path.display());
                    }
                    Ok(ApplyAction::Moved)
                }
                Err(e) => Err(e).with_context(|| {
                    format!(
                        "Failed to rename {} to {}",
                        transfer.source_path,
                        dest_path.display()
                    )
                }),
            }
        }
    }
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
