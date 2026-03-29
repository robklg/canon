use anyhow::{bail, Context, Result};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use crate::ceremony;
use crate::ops::cluster::{ManifestConfig, parse_manifest_allow, validate_manifest_version};
use crate::expr;
use crate::ops;
use crate::ops::apply::TransferMode;
use crate::ops::cluster::LockEntry;
use crate::repo::{self, Db};

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
    validate_manifest_version(config.meta.version)?;

    // Merge manifest [options] with CLI options
    let (_, manifest_duplicates) = parse_manifest_allow(&config.options.allow)?;
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
    let actual_hash = ops::fs::compute_full_hash(&lock_path)?;
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
    let filtered_sources = ops::apply::filter_by_roots(&sources, &options.roots, &roots)?;
    let skipped_by_filter = sources.len() - filtered_sources.len();

    // Show summary and confirm (unless --yes)
    let sample_dests = if !options.yes {
        compute_sample_destinations(
            conn, &filtered_sources, &pattern, &needed_keys,
            scope_prefix, &root_paths, &base_dir,
        )
    } else {
        vec![]
    };

    print_apply_summary(
        &config_path,
        &base_dir,
        &config.output.pattern,
        &filtered_sources,
        options,
        &root_paths,
        &sample_dests,
    );

    if !options.dry_run && !ceremony::confirm(options.yes)? {
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

    // --- Resume mode: display classification and check for fatal errors ---

    if options.resume {
        let total = plan.transfers.len() + plan.already_archived_count;
        eprintln!(
            "Files: {} ({} pending, {} already at destination)",
            total, plan.transfers.len(), plan.already_archived_count
        );
    }

    if options.resume {
        if !plan.resume_source_lost.is_empty() {
            eprintln!(
                "Resume failed: {} source files are missing and not at the destination.",
                plan.resume_source_lost.len()
            );
            eprintln!();
            for (_, path) in plan.resume_source_lost.iter().take(10) {
                eprintln!("  {path}");
            }
            if plan.resume_source_lost.len() > 10 {
                eprintln!("  ... and {} more", plan.resume_source_lost.len() - 10);
            }
            eprintln!();
            eprintln!("Check if the source volume is connected. If files are truly lost,");
            eprintln!("refresh the manifest: canon cluster refresh {}", config_path.display());
            bail!("Aborting due to missing source files in resume mode");
        }

        if !plan.resume_size_mismatches.is_empty() {
            eprintln!(
                "Resume failed: {} destination files have wrong size.",
                plan.resume_size_mismatches.len()
            );
            eprintln!();
            for (path, expected, actual) in plan.resume_size_mismatches.iter().take(10) {
                eprintln!("  {path} (expected {expected} bytes, found {actual} bytes)");
            }
            if plan.resume_size_mismatches.len() > 10 {
                eprintln!("  ... and {} more", plan.resume_size_mismatches.len() - 10);
            }
            eprintln!();
            eprintln!("Delete the corrupt file and retry: canon apply --resume {}", config_path.display());
            bail!("Aborting due to size mismatches in resume mode");
        }
    }

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

    if !v.escaped_paths.is_empty() {
        eprintln!(
            "Preflight failed: {} destination paths resolve outside the archive root.",
            v.escaped_paths.len()
        );
        eprintln!();
        for (source_path, resolved_dest) in v.escaped_paths.iter().take(10) {
            eprintln!("  {resolved_dest} (from source: {source_path})");
        }
        if v.escaped_paths.len() > 10 {
            eprintln!("  ... and {} more", v.escaped_paths.len() - 10);
        }
        eprintln!();
        eprintln!("Check the pattern in your manifest.");
        bail!("Aborting due to escaped destination paths");
    }

    if !v.missing_sources.is_empty() {
        let manifest_display = config_path.display();
        eprintln!(
            "Preflight failed: {} source files are missing.",
            v.missing_sources.len()
        );
        eprintln!();
        for (_, path) in v.missing_sources.iter().take(10) {
            eprintln!("  Missing: {path}");
        }
        if v.missing_sources.len() > 10 {
            eprintln!("  ... and {} more", v.missing_sources.len() - 10);
        }
        eprintln!();
        eprintln!("Source files have changed since the manifest was generated.");
        eprintln!("Refresh the manifest: canon cluster refresh {manifest_display}");
        bail!("Aborting due to missing source files");
    }

    if !v.unreadable_sources.is_empty() {
        eprintln!(
            "Preflight failed: {} source files are not readable.",
            v.unreadable_sources.len()
        );
        eprintln!();
        for (_, path) in v.unreadable_sources.iter().take(10) {
            eprintln!("  Permission denied: {path}");
        }
        if v.unreadable_sources.len() > 10 {
            eprintln!("  ... and {} more", v.unreadable_sources.len() - 10);
        }
        eprintln!();
        eprintln!("Fix file permissions, then retry.");
        bail!("Aborting due to unreadable source files");
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

    // Destination path conflicts (non-resume only):
    // DB conflicts come from plan, disk-only conflicts checked via ops layer
    if !options.resume {
        let on_disk_only = ops::apply::check_disk_conflicts(&plan, &base_dir);

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

    // --- Dry-run: display plan and return ---

    if options.dry_run {
        // DB-based staleness check for dry-run
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

        display_dry_run_plan(&plan, &base_dir, options.transfer_mode, options.resume);

        if options.resume {
            println!(
                "Dry run complete: {} would be {}, {} already at destination.",
                plan.transfers.len(),
                match options.transfer_mode {
                    TransferMode::Copy => "copied",
                    TransferMode::Rename => "renamed",
                    TransferMode::Move => "moved",
                },
                plan.already_archived_count,
            );
        } else {
            println!(
                "Dry run complete: {} would be {}, 0 would fail.",
                plan.transfers.len(),
                match options.transfer_mode {
                    TransferMode::Copy => "copied",
                    TransferMode::Rename => "renamed",
                    TransferMode::Move => "moved",
                },
            );
        }
        return Ok(());
    }

    // --- Execute transfers ---

    eprintln!("Checking destination write permissions...");
    ops::fs::check_destination_writable(&base_dir)?;

    let progress_impl = CliTransferProgress::new(options.verbose);
    let result = ops::apply::execute_apply(
        conn,
        &plan,
        &ops::apply::ApplyExecuteParams {
            base_dir: base_dir.clone(),
            archive_root_id: config.output.archive_root_id,
            transfer_mode: options.transfer_mode,
            resume: options.resume,
            interrupt_flag: None,
        },
        &progress_impl,
    )?;

    // Display stale sources found during transfer (race conditions)
    if !result.skipped_stale.is_empty() {
        eprintln!(
            "\nSkipped {} files that changed during apply:",
            result.skipped_stale.len()
        );
        for s in result.skipped_stale.iter().take(10) {
            eprintln!("  {}: {}", s.path, s.reason);
        }
        if result.skipped_stale.len() > 10 {
            eprintln!("  ... and {} more", result.skipped_stale.len() - 10);
        }
        eprintln!("Run `canon scan` then `cluster refresh` to regenerate the lock file.");
    }

    // Summary output
    if result.interrupted {
        let manifest_display = config_path.display();
        println!(
            "Applied: {} copied, {} renamed, {} moved, {} errors. Interrupted — {} files remaining.",
            result.copied, result.renamed, result.moved, result.errors.len(), result.remaining
        );
        eprintln!("Resume with: canon apply --resume {manifest_display}");
        // Update query planner statistics after bulk changes
        eprintln!("Updating query statistics...");
        db.run_analyze()?;
        return Ok(());
    } else if options.resume {
        println!(
            "Applied (--resume): {} copied, {} renamed, {} moved, {} already at destination, {} errors",
            result.copied, result.renamed, result.moved, result.already_there, result.errors.len()
        );
        if result.already_there_source_present > 0 {
            eprintln!();
            eprintln!(
                "Note: {} source files from a previous operation may still exist at the original location.",
                result.already_there_source_present
            );
        }
    } else {
        println!(
            "Applied: {} copied, {} renamed, {} moved, {} skipped (missing), {} skipped (stale), {} skipped (filtered), {} errors",
            result.copied, result.renamed, result.moved, result.skipped_missing, result.skipped_stale.len(), skipped_by_filter, result.errors.len()
        );
    }

    // Recovery guidance when errors occurred
    if !result.errors.is_empty() {
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

    // Update query planner statistics after bulk changes
    eprintln!("Updating query statistics...");
    db.run_analyze()?;

    Ok(())
}

// ============================================================================
// Sample destination computation
// ============================================================================

struct SampleDestination {
    dest_path: String,
    error: Option<String>,
}

const SAMPLE_COUNT: usize = 5;

/// Compute sample destination paths for the apply confirmation summary.
/// Resolves the output pattern for up to SAMPLE_COUNT sources, fetching
/// only the facts needed for those sources.
fn compute_sample_destinations(
    conn: &mut repo::Connection,
    sources: &[&LockEntry],
    pattern: &expr::Pattern,
    needed_keys: &[String],
    scope_prefix: Option<&str>,
    root_paths: &HashMap<i64, String>,
    base_dir: &Path,
) -> Vec<SampleDestination> {
    let sample_sources: Vec<&&LockEntry> = sources.iter().take(SAMPLE_COUNT).collect();
    if sample_sources.is_empty() {
        return vec![];
    }

    // Fetch facts for sample sources only (same per-key pattern as plan_apply)
    let sample_ids: Vec<i64> = sample_sources.iter().map(|s| s.id).collect();
    let mut all_facts: HashMap<i64, Vec<crate::domain::fact::FactEntry>> = HashMap::new();
    for key in needed_keys {
        if key.starts_with("source.") || key.starts_with("scope.") || key == "object.hash" {
            continue;
        }
        match repo::fact::batch_fetch_key_for_sources(conn, &sample_ids, key) {
            Ok(key_facts) => {
                for (source_id, entry_opt) in key_facts {
                    if let Some(entry) = entry_opt {
                        all_facts.entry(source_id).or_default().push(entry);
                    }
                }
            }
            Err(_) => return vec![], // Graceful degradation: skip samples on DB error
        }
    }

    sample_sources
        .iter()
        .map(|source| {
            match ops::apply::evaluate_pattern(
                pattern, source, needed_keys, scope_prefix, root_paths, &all_facts,
            ) {
                Ok(dest_rel) => {
                    let full_path = base_dir.join(&dest_rel);
                    SampleDestination {
                        dest_path: full_path.display().to_string(),
                        error: None,
                    }
                }
                Err(e) => SampleDestination {
                    dest_path: String::new(),
                    error: Some(format!("pattern error: {e}")),
                },
            }
        })
        .collect()
}

// Summary and confirmation helpers
// ============================================================================

fn print_apply_summary(
    config_path: &Path,
    base_dir: &Path,
    pattern: &str,
    sources: &[&LockEntry],
    options: &ApplyOptions,
    root_paths: &HashMap<i64, String>,
    sample_dests: &[SampleDestination],
) {
    eprintln!();
    eprintln!("=== Apply Summary ===");
    eprintln!("Manifest: {}", config_path.display());
    eprintln!("Destination: {}", base_dir.display());
    eprintln!("Pattern: {pattern}");

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

    // Show sample destinations
    if !sample_dests.is_empty() {
        eprintln!();
        eprintln!("Sample destinations:");
        for sample in sample_dests {
            match &sample.error {
                Some(err) => eprintln!("  ({err})"),
                None => eprintln!("  {}", sample.dest_path),
            }
        }
        let remaining = sources.len().saturating_sub(sample_dests.len());
        if remaining > 0 {
            eprintln!("  ... and {remaining} more");
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

/// Display dry-run transfer plan.
fn display_dry_run_plan(plan: &ops::apply::ApplyPlan, base_dir: &Path, mode: TransferMode, resume: bool) {
    if resume {
        eprintln!("=== DRY RUN (resume) ===");
    } else {
        eprintln!("=== DRY RUN ===");
    }
    let label = match mode {
        TransferMode::Copy => "COPY",
        TransferMode::Rename => "RENAME",
        TransferMode::Move => "MOVE",
    };
    for transfer in &plan.transfers {
        let dest_path = base_dir.join(&transfer.dest_rel_path);
        if !Path::new(&transfer.source_path).exists() {
            println!("[dry-run] SKIP (missing): {}", transfer.source_path);
        } else {
            println!("[dry-run] {}: {} -> {}", label, transfer.source_path, dest_path.display());
        }
    }
    if resume {
        for transfer in &plan.resume_already_there {
            let dest_path = base_dir.join(&transfer.dest_rel_path);
            println!("[dry-run] Already there: {}", dest_path.display());
        }
    }
}

/// CLI implementation of TransferProgress using the Progress spinner.
struct CliTransferProgress {
    verbose: bool,
    progress: std::cell::RefCell<Option<crate::progress::Progress>>,
}

impl CliTransferProgress {
    fn new(verbose: bool) -> Self {
        Self {
            verbose,
            progress: std::cell::RefCell::new(None),
        }
    }
}

impl ops::apply::TransferProgress for CliTransferProgress {
    fn on_start(&self, total: usize) {
        if total > 0 {
            eprintln!();
            eprintln!("Processing {total} sources...");
            *self.progress.borrow_mut() = Some(crate::progress::Progress::new(total));
        }
    }

    fn on_transfer(&self, index: usize, _total: usize, source_path: &str, dest_path: &str, outcome: &ops::apply::TransferOutcome) {
        if let Some(ref p) = *self.progress.borrow() {
            let filename = source_path.rsplit('/').next().unwrap_or(source_path);
            p.update_with_name(index, filename);
        }
        if self.verbose {
            match outcome {
                ops::apply::TransferOutcome::Copied => {
                    println!("Copied: {source_path} -> {dest_path}");
                }
                ops::apply::TransferOutcome::Renamed => {
                    println!("Renamed: {source_path} -> {dest_path}");
                }
                ops::apply::TransferOutcome::Moved => {
                    println!("Moved: {source_path} -> {dest_path}");
                }
                ops::apply::TransferOutcome::Error(msg) => {
                    eprintln!("Error processing {source_path}: {msg}");
                }
                _ => {}
            }
        } else if let ops::apply::TransferOutcome::Error(msg) = outcome {
            eprintln!("Error processing {source_path}: {msg}");
        }
    }

    fn on_interrupt(&self) {
        if let Some(ref p) = *self.progress.borrow() {
            p.finish();
        }
        eprintln!();
        eprintln!("Interrupt received, stopping after current file.");
    }

    fn on_finish(&self) {
        if let Some(ref p) = *self.progress.borrow() {
            p.finish();
        }
    }
}
