use anyhow::{bail, Context, Result};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::archive::domain::{
    extract_notes, parse_manifest_allow, validate_manifest_version, LockEntry, ManifestConfig,
};
use crate::archive::ops::execute::{self, TransferMode};
use crate::archive::ops::{manifest, pattern, plan};
use crate::ceremony;
use crate::core::domain::config::{LedgerConfig, RecordingMode};
use crate::core::domain::decision::DecisionCommand;
use crate::core::domain::format::first_chars;
use crate::core::domain::format_count;
use crate::core::domain::scope::DecisionScope;
use crate::core::ops::decision::DecisionParams;
use crate::core::ops::receipt::ReceiptPlacement;
use crate::core::repo::{self, Db};
use crate::expr::{
    extract_fact_keys, parse_pattern, placement_shape, prefetch_pattern_facts, Pattern, Unmeasured,
};

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

pub fn run(
    db: &mut Db,
    manifest_path: &Path,
    options: &ApplyOptions,
    command_line: &str,
    ledger: &LedgerConfig,
    no_receipt: bool,
    reason: Option<&str>,
) -> Result<()> {
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

    // Merge manifest [options] with CLI options.
    //
    // Only the duplicates flag is merged, and that is settled rather than an
    // oversight: an acknowledgment is per-command and belongs to the
    // invocation that needs it. `archived` acknowledges a *selection*, so it
    // is read by the one command that re-selects from a manifest — `cluster
    // refresh`. Apply selects nothing; it carries out a lock file already
    // written, and the conflicts it is the one to find carry their own
    // acknowledgments on its own flags.
    let (_, manifest_duplicates) = parse_manifest_allow(&config.options.allow)?;
    let allow_duplicates = options.allow_duplicates || manifest_duplicates;
    if manifest_duplicates && !options.allow_duplicates {
        eprintln!(
            "Options from manifest: allow {}",
            config.options.allow.join(", ")
        );
    }

    // Read the JSONL lock file: the header it settled, and its entries.
    let lock = manifest::read_lock_file(&lock_path)?;
    // Why an entry might carry no measurement — asked of the file, which can
    // tell the two causes apart, rather than left to a refusal to guess.
    let unmeasured = lock.unmeasured_reason();
    let sources = lock.entries;

    // Validate lock file hash matches config
    let actual_hash = crate::core::ops::fs::compute_full_hash(&lock_path)?;
    if actual_hash != config.meta.lock_hash {
        bail!(
            "Lock file hash mismatch: expected {}, got {}\n\
             The lock file may have been modified or does not belong to this config.\n\
             Run `cluster refresh` to regenerate the lock file.",
            first_chars(&config.meta.lock_hash, 16),
            first_chars(&actual_hash, 16)
        );
    }

    let conn = db.conn_mut();

    // Parse the pattern once upfront
    let pattern = parse_pattern(&config.output.pattern)
        .with_context(|| format!("Failed to parse output pattern: {}", config.output.pattern))?;
    let needed_keys = extract_fact_keys(&pattern);

    // Cache all root paths (single query via repo layer)
    let roots = repo::root::fetch_all(conn)?;
    let root_paths: HashMap<i64, String> = roots.iter().map(|r| (r.id, r.path.clone())).collect();

    // The scope this run acts under, taken from the lock rather than resolved
    // from `meta.scope`. Apply selects nothing and now measures nothing: the
    // lock settled both when the selection was made, and the header is what
    // the decision record names — so a scope line edited after the refresh
    // cannot move a file or falsify a record.
    //
    // A lock with no header is refused here, before the plan and long before
    // the decision row exists, so a refusal leaves nothing behind to explain.
    // Unconditional, not gated on whether the pattern reads the scope: the
    // record's claim is at stake on every apply whatever the pattern says, and
    // a run that recorded a scoped act as a global one would be exactly the
    // silence this whole mechanism exists to close.
    //
    // Placement is relied on, not designed on: if this check ever moves below
    // `DecisionRecorder::start`, `DecisionRecorder::refuse` is what settles the
    // row it would leave.
    //
    // The whole message is the refusal, on the pattern the lock-hash mismatch
    // above already uses: both say this manifest cannot be applied as it
    // stands and name the way back.
    let Some(header) = lock.header else {
        bail!(
            "This lock file was written before Canon recorded where each file goes.\n\
             Nothing was moved. Rebuild it: canon cluster refresh {}",
            config_path.display()
        );
    };
    // Nothing checks that the header's scopes actually contain the entries,
    // and nothing needs to *while* the writers keep selecting from the same
    // register they measure from (`ScopeResolution::selection`). They do, and
    // `a_line_that_measures_nothing_selects_nothing` is what holds it: select
    // from the recorded list instead and a refresh produces a lock whose two
    // halves genuinely disagree. A check here would only add a second place to
    // notice that; the `lock_hash` above already refuses a hand-edited lock.
    let decision_scope: Vec<DecisionScope> = header
        .scope
        .into_iter()
        .map(|s| DecisionScope::new(s.root_id, s.root_path, s.rel_prefix))
        .collect();

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
    let filtered_sources = plan::filter_by_roots(&sources, &options.roots, &roots)?;
    let skipped_by_filter = sources.len() - filtered_sources.len();

    // Show summary and confirm (unless --yes)
    let sample_dests = if !options.yes {
        compute_sample_destinations(
            conn,
            &filtered_sources,
            &pattern,
            &needed_keys,
            unmeasured,
            &root_paths,
            &base_dir,
        )
    } else {
        vec![]
    };

    let destination = Destination::compute(&base_dir, &pattern);
    print_apply_summary(
        &config_path,
        &destination,
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
    let progress_impl = CliTransferProgress::new(options.verbose);
    let plan = plan::plan_apply(
        conn,
        &plan::ApplyPlanParams {
            sources: &filtered_sources,
            pattern: &pattern,
            needed_keys: &needed_keys,
            unmeasured,
            root_paths: &root_paths,
            archive_root_id: config.output.archive_root_id,
            base_dir_rel: &config.output.base_dir,
            resume: options.resume,
            progress: Some(&progress_impl),
        },
    )?;

    // --- Resume mode: display classification and check for fatal errors ---

    if options.resume {
        let total = plan.transfers.len() + plan.already_archived_count;
        eprintln!(
            "Files: {} ({} pending, {} already at destination)",
            total,
            plan.transfers.len(),
            plan.already_archived_count
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
            eprintln!(
                "refresh the manifest: canon cluster refresh {}",
                config_path.display()
            );
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
            eprintln!(
                "Delete the corrupt file and retry: canon apply --resume {}",
                config_path.display()
            );
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
        // No cause is asserted: a pattern fails to expand for a missing fact,
        // and equally for a scope the manifest does not record, and the lines
        // above already carry each source's own reason. Naming one cause here
        // sends half the readers to a command that cannot help them.
        eprintln!("\nAdjust the pattern in your manifest, or supply what it names.");
        eprintln!("Use 'canon facts' to check coverage for a pattern that names facts.");
        bail!("Aborting due to pattern expansion failures");
    }

    // A file where a directory has to go blocks every destination beneath it,
    // and no retry changes that. Refusing here — in resume mode too — keeps the
    // answer to one message instead of one per transfer, with nothing moved.
    if !v.ancestor_collisions.is_empty() {
        let blocked: usize = v.ancestor_collisions.iter().map(|c| c.blocked_count).sum();
        eprintln!(
            "Preflight failed: {} destination paths are blocked by {} files standing where a directory must go.",
            format_count(blocked),
            format_count(v.ancestor_collisions.len())
        );
        eprintln!();
        for collision in v.ancestor_collisions.iter().take(10) {
            eprintln!(
                "  {} blocks {} destinations:",
                collision.blocking_path,
                format_count(collision.blocked_count)
            );
            for dest in &collision.sample_dests {
                eprintln!("    {dest}");
            }
            if collision.blocked_count > collision.sample_dests.len() {
                eprintln!(
                    "    ... and {} more",
                    format_count(collision.blocked_count - collision.sample_dests.len())
                );
            }
        }
        if v.ancestor_collisions.len() > 10 {
            eprintln!(
                "  ... and {} more",
                format_count(v.ancestor_collisions.len() - 10)
            );
        }
        eprintln!();
        eprintln!("Move or rename the file in the way, or edit the pattern in your manifest.");
        bail!("Aborting due to files blocking destination directories");
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

    // Destination path conflicts (non-resume only):
    // DB conflicts come from plan, disk-only conflicts checked via ops layer
    if !options.resume {
        let on_disk_only = plan::check_disk_conflicts(&plan, &base_dir);

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

    // A source that changed since the manifest was generated invalidates the
    // plan itself, so this gate belongs beside the other preflight refusals —
    // never inside the dry-run arm, where a rehearsal would fail and the run
    // it rehearsed proceed.
    if !plan.stale_sources.is_empty() {
        eprintln!(
            "Error: {} sources have changed since manifest was generated:",
            plan.stale_sources.len()
        );
        for line in plan::staleness_lines(&plan.stale_sources, &config_path.display().to_string()) {
            eprintln!("{line}");
        }
        bail!("Aborting due to stale sources in manifest");
    }

    // --- Dry-run: display plan and return ---

    if options.dry_run {
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
    plan::check_destination_writable(&base_dir)?;

    // Construct decision: reason falls back to manifest notes
    let effective_reason = if let Some(r) = reason.filter(|r| !r.trim().is_empty()) {
        Some(r.to_string())
    } else {
        extract_notes(&config_content).filter(|n| !n.trim().is_empty())
    };
    let decision = DecisionParams {
        command: DecisionCommand::Apply,
        scope: decision_scope,
        command_line: command_line.to_string(),
        reason: effective_reason,
        record_enabled: ledger.recording != RecordingMode::Off && !options.dry_run,
        receipt_enabled: ledger.recording == RecordingMode::Full && !no_receipt && !options.dry_run,
        ledger_config: ledger.clone(),
    };

    let receipt_ctx = if decision.receipt_enabled {
        Some(ReceiptPlacement::Targeted {
            archive_root_id: config.output.archive_root_id,
            archive_root_path: archive_root_path.clone(),
            base_dir_rel: config.output.base_dir.clone(),
        })
    } else {
        None
    };

    let result = execute::execute_apply(
        conn,
        &plan,
        &execute::ApplyExecuteParams {
            base_dir: base_dir.clone(),
            archive_root_id: config.output.archive_root_id,
            transfer_mode: options.transfer_mode,
            resume: options.resume,
            interrupt_flag: None,
            skipped_by_filter,
            manifest_display: format!("{}", config_path.display()),
            receipt_ctx,
        },
        &progress_impl,
        Some(&decision),
    )?;

    // Display stale sources found during transfer (race conditions)
    if !result.skipped_stale.is_empty() {
        eprintln!(
            "\nSkipped {} files that changed during apply:",
            result.skipped_stale.len()
        );
        for line in plan::staleness_lines(&result.skipped_stale, &config_path.display().to_string())
        {
            eprintln!("{line}");
        }
        // This site reports on a run that already moved files, so its retry is
        // not the other two sites' retry: a plain re-apply would collide with
        // everything this run placed and be sent here anyway.
        eprintln!("  canon apply --resume {}", lock_path.display());
    }

    // Summary output
    println!("{}", result.summary);

    // Surface any warnings collected during execution (e.g. receipt-write failures).
    for w in &result.warnings {
        eprintln!("{w}");
    }

    if result.interrupted {
        let manifest_display = config_path.display();
        eprintln!("Resume with: canon apply --resume {manifest_display}");
        // Update query planner statistics after bulk changes
        eprintln!("Updating query statistics...");
        db.run_analyze()?;
        return Ok(());
    } else if options.resume && result.already_there_source_present > 0 {
        eprintln!();
        eprintln!(
            "Note: {} source files from a previous operation may still exist at the original location.",
            result.already_there_source_present
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
    pattern: &Pattern,
    needed_keys: &[String],
    unmeasured: Unmeasured,
    root_paths: &HashMap<i64, String>,
    base_dir: &Path,
) -> Vec<SampleDestination> {
    let sample_sources: Vec<&&LockEntry> = sources.iter().take(SAMPLE_COUNT).collect();
    if sample_sources.is_empty() {
        return vec![];
    }

    // Fetch facts for sample sources only. The samples are decoration on a
    // confirmation prompt, not the act, so a database error drops them rather
    // than failing the command — the same graceful degradation as before,
    // moved out to the one call that can now fail.
    let sample_ids: Vec<i64> = sample_sources.iter().map(|s| s.id).collect();
    let Ok(facts) = prefetch_pattern_facts(conn, &sample_ids, needed_keys) else {
        return vec![];
    };

    sample_sources
        .iter()
        .map(|source| {
            match pattern::evaluate_pattern(pattern, source, unmeasured, root_paths, &facts) {
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
    destination: &Destination,
    pattern: &str,
    sources: &[&LockEntry],
    options: &ApplyOptions,
    root_paths: &HashMap<i64, String>,
    sample_dests: &[SampleDestination],
) {
    eprintln!();
    eprintln!("=== Apply Summary ===");
    eprintln!("Manifest: {}", config_path.display());
    eprintln!("Destination: {}", destination.base_dir.display());
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

    // Show the contents of the directory files actually enter.
    eprintln!();
    eprintln!("{}", destination.placement_label());
    if destination.fans_out {
        eprintln!("  (placements fan out under this directory by pattern)");
    }
    if destination.placement_dir.exists() {
        show_directory_preview(&destination.placement_dir, 5);
    } else {
        eprintln!("  (will be created)");
    }

    eprintln!();
}

/// Where an apply puts files: the manifest's base directory, the directory the
/// pattern actually places into, and whether it spreads them across directories
/// below that one.
///
/// The two directories differ because `base_dir` is where the pattern starts,
/// not where files land: a pattern opening with literal directories places
/// everything below them, so previewing `base_dir` itself would show a folder
/// the run may never write a file into. The preview label always names the
/// directory it is showing, so what is on screen and what it is are never two
/// separate readings.
struct Destination {
    base_dir: PathBuf,
    placement_dir: PathBuf,
    fans_out: bool,
}

impl Destination {
    fn compute(base_dir: &Path, pattern: &Pattern) -> Self {
        let (static_prefix, fans_out) = placement_shape(pattern);
        Self {
            base_dir: base_dir.to_path_buf(),
            placement_dir: match static_prefix {
                Some(prefix) => base_dir.join(prefix),
                None => base_dir.to_path_buf(),
            },
            fans_out,
        }
    }

    fn placement_label(&self) -> String {
        format!(
            "Destination current contents ({}):",
            self.placement_dir.display()
        )
    }
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
fn display_dry_run_plan(plan: &plan::ApplyPlan, base_dir: &Path, mode: TransferMode, resume: bool) {
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
            println!(
                "[dry-run] {}: {} -> {}",
                label,
                transfer.source_path,
                dest_path.display()
            );
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
    validation: std::cell::RefCell<Option<crate::progress::Progress>>,
}

impl CliTransferProgress {
    fn new(verbose: bool) -> Self {
        Self {
            verbose,
            progress: std::cell::RefCell::new(None),
            validation: std::cell::RefCell::new(None),
        }
    }
}

impl execute::TransferProgress for CliTransferProgress {
    fn on_start(&self, total: usize) {
        if total > 0 {
            eprintln!();
            eprintln!("Processing {total} sources...");
            *self.progress.borrow_mut() = Some(crate::progress::Progress::new(total));
        }
    }

    fn on_transfer(
        &self,
        index: usize,
        _total: usize,
        source_path: &str,
        dest_path: &str,
        outcome: &execute::TransferOutcome,
    ) {
        if let Some(ref p) = *self.progress.borrow() {
            let filename = source_path.rsplit('/').next().unwrap_or(source_path);
            p.update_with_name(index, filename);
        }
        if self.verbose {
            match outcome {
                execute::TransferOutcome::Copied => {
                    println!("Copied: {source_path} -> {dest_path}");
                }
                execute::TransferOutcome::Renamed => {
                    println!("Renamed: {source_path} -> {dest_path}");
                }
                execute::TransferOutcome::Moved => {
                    println!("Moved: {source_path} -> {dest_path}");
                }
                execute::TransferOutcome::Error(msg) => {
                    eprintln!("Error processing {source_path}: {msg}");
                }
                _ => {}
            }
        } else if let execute::TransferOutcome::Error(msg) = outcome {
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

    fn on_validation_start(&self, sweep: execute::ValidationSweep, total: usize) {
        if total == 0 {
            return;
        }
        // Each sweep says what it is about to do to every source, so a wait is
        // legible as the work it is rather than as a stall.
        match sweep {
            execute::ValidationSweep::Readability => eprintln!(
                "Checking {} sources can be read...",
                format_count(total as u64)
            ),
            execute::ValidationSweep::LockAgreement => eprintln!(
                "Verifying {} sources against the lock file (reading file heads)...",
                format_count(total as u64)
            ),
        }
        *self.validation.borrow_mut() = Some(crate::progress::Progress::new(total));
    }

    fn on_validation_progress(&self, done: usize) {
        if let Some(ref p) = *self.validation.borrow() {
            p.update(done);
        }
    }

    fn on_validation_finish(&self) {
        if let Some(p) = self.validation.borrow_mut().take() {
            p.finish();
        }
    }
}

/// Planning's sweep shares the command's one progress voice. It runs under the
/// `Running preflight checks...` line, which already names the work, so it
/// shows a bar and adds no second heading.
impl plan::PlanProgress for CliTransferProgress {
    fn on_preflight_start(&self, total: usize) {
        if total > 0 {
            *self.validation.borrow_mut() = Some(crate::progress::Progress::new(total));
        }
    }

    fn on_preflight_progress(&self, done: usize) {
        if let Some(ref p) = *self.validation.borrow() {
            p.update(done);
        }
    }

    fn on_preflight_finish(&self) {
        if let Some(p) = self.validation.borrow_mut().take() {
            p.finish();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A pattern that opens with literal directories places every file below
    /// them, so the previewed directory is the one files enter — not the
    /// manifest's base_dir, which the run may never write a file into.
    #[test]
    fn the_preview_names_the_effective_placement_directory() {
        let pattern = parse_pattern("2024/{filename}").unwrap();
        let destination = Destination::compute(Path::new("/Volumes/Archive"), &pattern);

        assert_eq!(
            destination.placement_dir,
            PathBuf::from("/Volumes/Archive/2024")
        );
        assert!(!destination.fans_out);
        assert_eq!(
            destination.placement_label(),
            "Destination current contents (/Volumes/Archive/2024):"
        );
    }

    /// A pattern whose directories come from the content has no single
    /// placement directory. The preview falls back to the common one it does
    /// know, and says so rather than presenting it as the whole answer.
    #[test]
    fn the_preview_states_fan_out_and_falls_back_labeled() {
        let pattern = parse_pattern("{source.rel_path}").unwrap();
        let destination = Destination::compute(Path::new("/Volumes/Archive"), &pattern);

        assert_eq!(destination.placement_dir, PathBuf::from("/Volumes/Archive"));
        assert!(destination.fans_out);
        assert_eq!(
            destination.placement_label(),
            "Destination current contents (/Volumes/Archive):"
        );

        // A literal head still narrows the fallback: the fan-out happens under
        // the directory the pattern names, not at the top.
        let nested = parse_pattern("photos/{source.rel_path}").unwrap();
        let destination = Destination::compute(Path::new("/Volumes/Archive"), &nested);

        assert_eq!(
            destination.placement_dir,
            PathBuf::from("/Volumes/Archive/photos")
        );
        assert!(destination.fans_out);
    }

    // ------------------------------------------------------------------
    // The recorded scope, against the real command
    // ------------------------------------------------------------------

    use crate::archive::ops::generate::{
        execute_generate, plan_generate, ClusterGenerateParams, ExecuteGenerateParams,
    };
    use crate::core::domain::scope::ScopeMatch;
    use crate::core::testing::{insert_object, insert_root, setup_test_db};
    use std::os::unix::fs::MetadataExt;

    /// A source tree, an archive root, and a manifest + lock written by the
    /// real generator, ready for `run` to be called on.
    ///
    /// Built rather than typed out because a manifest is a pair: the lock
    /// file's hash is named in the config, and apply refuses the pair when
    /// they disagree. Letting generation write both means every case here
    /// reads a document Canon actually produces. Only `meta.scope` is
    /// dictated — that is the field a user edits by hand, and it is what all
    /// three cases are about.
    struct Fixture {
        db: Db,
        manifest: PathBuf,
        source_root: PathBuf,
        archive_root: PathBuf,
        _tree: tempfile::TempDir,
        _archive: tempfile::TempDir,
        _out: tempfile::TempDir,
    }

    impl Fixture {
        /// What the run's decision row recorded as its scope, as
        /// `(root_path, rel_prefix)` pairs — the durable half of every claim
        /// in this module.
        fn recorded_scope(&mut self) -> Vec<(String, String)> {
            self.db
                .conn_mut()
                .prepare(
                    "SELECT root_path, rel_prefix FROM decision_scopes ORDER BY root_path, rel_prefix",
                )
                .unwrap()
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
                .unwrap()
                .collect::<std::result::Result<_, _>>()
                .unwrap()
        }

        /// Where this fixture's one selected source lands: the manifest is
        /// scoped to `proj-v1` and the default pattern is `{scope.rel_path}`,
        /// so a single scope is its own vantage and `proj-v1/day1/a.jpg`
        /// measures to `day1/a.jpg`. Distinct from the filename and from the
        /// root-relative path, which is what makes the cases below able to
        /// fail. Named once because three of them compare against it, and what
        /// each is about is that something did *not* move it.
        fn expected_placement(&self) -> Vec<String> {
            vec!["day1/a.jpg".to_string()]
        }

        /// Rewrite `output.pattern` in the manifest, the way a user would.
        /// The lock is untouched, so its hash still matches.
        fn set_pattern(&self, pattern: &str) {
            let text = fs::read_to_string(&self.manifest).unwrap();
            let rewritten: String = text
                .lines()
                .map(|line| {
                    if line.starts_with("pattern = ") {
                        format!("pattern = \"{pattern}\"\n")
                    } else {
                        format!("{line}\n")
                    }
                })
                .collect();
            fs::write(&self.manifest, rewritten).unwrap();
        }

        /// Turn the lock back into one written before headers existed, and
        /// re-point the manifest at its new hash — otherwise the run would
        /// stop on the hash mismatch instead of on the missing header.
        fn strip_lock_header(&self) {
            let lock_path = self.manifest.with_extension("lock");
            let content = fs::read_to_string(&lock_path).unwrap();
            let without: String = content
                .lines()
                .skip(1)
                .map(|l| {
                    // An old lock also carried no per-entry measurement.
                    let mut v: serde_json::Value = serde_json::from_str(l).unwrap();
                    v.as_object_mut().unwrap().remove("scope_rel_path");
                    format!("{v}\n")
                })
                .collect();
            fs::write(&lock_path, without).unwrap();

            let hash = crate::core::ops::fs::compute_full_hash(&lock_path).unwrap();
            let text = fs::read_to_string(&self.manifest).unwrap();
            let rewritten: String = text
                .lines()
                .map(|line| {
                    if line.starts_with("lock_hash = ") {
                        format!("lock_hash = \"{hash}\"\n")
                    } else {
                        format!("{line}\n")
                    }
                })
                .collect();
            fs::write(&self.manifest, rewritten).unwrap();
        }

        /// What `plan_apply` reported as pattern-expansion failures, with
        /// each source's own reason — the same strings apply prints to
        /// stderr, read where they are data rather than output.
        fn expansion_failures(&mut self) -> Vec<(String, String)> {
            use crate::archive::ops::plan::{plan_apply, ApplyPlanParams};
            use crate::expr::{extract_fact_keys, parse_pattern};

            let text = fs::read_to_string(&self.manifest).unwrap();
            let config: crate::archive::domain::ManifestConfig = toml::from_str(&text).unwrap();
            let lock = manifest::read_lock_file(&self.manifest.with_extension("lock")).unwrap();
            let pattern = parse_pattern(&config.output.pattern).unwrap();
            let needed_keys = extract_fact_keys(&pattern);
            let unmeasured = lock.unmeasured_reason();
            let entries: Vec<&LockEntry> = lock.entries.iter().collect();

            let conn = self.db.conn_mut();
            let root_paths: HashMap<i64, String> = repo::root::fetch_all(conn)
                .unwrap()
                .iter()
                .map(|r| (r.id, r.path.clone()))
                .collect();
            let plan = plan_apply(
                conn,
                &ApplyPlanParams {
                    sources: &entries,
                    pattern: &pattern,
                    needed_keys: &needed_keys,
                    unmeasured,
                    root_paths: &root_paths,
                    archive_root_id: config.output.archive_root_id,
                    base_dir_rel: &config.output.base_dir,
                    resume: false,
                    progress: None,
                },
            )
            .unwrap();
            assert!(
                !plan.violations.expansion_failures.is_empty(),
                "the fixture must actually fail to expand, or this proves nothing"
            );
            plan.violations.expansion_failures.clone()
        }

        fn decision_count(&mut self) -> i64 {
            self.db
                .conn_mut()
                .query_row("SELECT COUNT(*) FROM decisions", [], |r| r.get(0))
                .unwrap()
        }

        /// Every file under the archive root, root-relative and sorted: the
        /// destination surface a run produced. The ledger directory is left
        /// out — it is provenance, not where the content went.
        fn placed(&self) -> Vec<String> {
            fn walk(dir: &Path, base: &Path, out: &mut Vec<String>) {
                let mut entries: Vec<_> = fs::read_dir(dir).unwrap().map(|e| e.unwrap()).collect();
                entries.sort_by_key(|e| e.path());
                for entry in entries {
                    let path = entry.path();
                    if path.file_name().and_then(|n| n.to_str()) == Some(".canon-ledger") {
                        continue;
                    }
                    if path.is_dir() {
                        walk(&path, base, out);
                    } else {
                        out.push(
                            path.strip_prefix(base)
                                .unwrap()
                                .to_string_lossy()
                                .to_string(),
                        );
                    }
                }
            }
            let mut out = Vec::new();
            walk(&self.archive_root, &self.archive_root, &mut out);
            out.sort();
            out
        }
    }

    /// `root_dir_name` is the source root's own last component, so a case can
    /// give it a name with two normalizations. `selected` names the
    /// directories generation queries over — what lands in the lock.
    /// `recorded` is handed the stored root and returns what goes into
    /// `meta.scope`, which is where the divergence under test is introduced;
    /// it takes the root because only the filesystem can say which byte-form
    /// that root will be stored in.
    fn fixture(
        root_dir_name: &str,
        selected: &[&str],
        recorded: impl Fn(&str) -> Vec<String>,
    ) -> Fixture {
        fixture_under("", root_dir_name, selected, recorded)
    }

    /// The same, with the two projects sitting under `under` inside the root —
    /// which is what lets a case put the divergence *below* an ASCII root —
    /// the shape an ASCII volume or home path with accented folders under it
    /// always has.
    fn fixture_under(
        under: &str,
        root_dir_name: &str,
        selected: &[&str],
        recorded: impl Fn(&str) -> Vec<String>,
    ) -> Fixture {
        let tree = tempfile::tempdir().unwrap();
        let archive = tempfile::tempdir().unwrap();
        let out = tempfile::tempdir().unwrap();

        // The stored root is whatever the filesystem hands back for the
        // directory just created, never the string typed here: on a
        // filesystem that normalizes names those differ, and the stored form
        // is the one every comparison downstream has to meet.
        let created = tree.path().join(root_dir_name);
        fs::create_dir_all(&created).unwrap();
        let source_root = fs::canonicalize(&created).unwrap();
        let archive_root = fs::canonicalize(archive.path()).unwrap();

        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, &source_root.to_string_lossy(), "source", false);
        let archive_id = insert_root(&conn, &archive_root.to_string_lossy(), "archive", false);

        // Nested one level below the scope on purpose. The measurement for a
        // manifest scoped to `proj-v1` is then `day1/a.jpg`, which is neither
        // the filename nor the root-relative path — so a reader that quietly
        // substituted either would be visible here rather than coincidentally
        // right.
        let rels: Vec<String> = ["proj-v1/day1/a.jpg", "proj-v2/day1/b.jpg"]
            .iter()
            .map(|r| {
                if under.is_empty() {
                    r.to_string()
                } else {
                    format!("{under}/{r}")
                }
            })
            .collect();
        for (i, rel) in rels.iter().enumerate() {
            let file = source_root.join(rel);
            fs::create_dir_all(file.parent().unwrap()).unwrap();
            fs::write(&file, format!("content-{i}")).unwrap();
            let meta = fs::metadata(&file).unwrap();
            let object = insert_object(&conn, &format!("hash-{i}"), false);
            // Size, mtime and partial hash are the file's own: apply
            // re-observes all three against disk before it transfers anything,
            // so placeholder values would make every case here fail as stale
            // rather than on what it is about.
            let partial = crate::core::ops::fs::compute_partial_hash(&file, meta.len()).unwrap();
            conn.execute(
                "INSERT INTO sources (root_id, rel_path, object_id, size, mtime, partial_hash, \
                 scanned_at, last_seen_at, device, inode, excluded) \
                 VALUES (?, ?, ?, ?, ?, ?, 0, 0, 0, 0, 0)",
                rusqlite::params![
                    root_id,
                    rel,
                    object,
                    meta.len() as i64,
                    meta.mtime(),
                    partial
                ],
            )
            .unwrap();
        }

        let scopes: Vec<ScopeMatch> = selected
            .iter()
            .map(|s| ScopeMatch::UnderDirectory(source_root.join(s).to_string_lossy().to_string()))
            .collect();
        let plan = plan_generate(
            &mut conn,
            &ClusterGenerateParams {
                scopes,
                filters: vec![],
                allow_archived: false,
                allow_duplicates: false,
            },
        )
        .unwrap();
        assert!(!plan.lock_entries.is_empty(), "the fixture locked nothing");

        let manifest = out.path().join("cluster.toml");
        // Through the same resolution `cluster generate` uses, so the lock the
        // fixture writes is the lock the real command writes — measurement,
        // header and healed `meta.scope` alike.
        let all_roots = crate::core::repo::root::fetch_all(&conn).unwrap();
        let scope = crate::core::ops::scope::resolve_recorded_scope(
            &conn,
            &recorded(&source_root.to_string_lossy()),
            &all_roots,
        )
        .unwrap();
        let mut plan = plan;
        execute_generate(
            &mut plan,
            &ExecuteGenerateParams {
                lock_path: out.path().join("cluster.lock"),
                manifest_path: manifest.clone(),
                expanded_filters: vec![],
                original_filters: vec![],
                scope,
                roots: all_roots,
                archive_root_id: archive_id,
                base_dir: String::new(),
                allow: vec![],
            },
        )
        .unwrap();

        Fixture {
            db: Db::from_connection(conn),
            manifest,
            source_root,
            archive_root,
            _tree: tree,
            _archive: archive,
            _out: out,
        }
    }

    fn apply(fixture: &mut Fixture) -> Result<()> {
        let options = ApplyOptions {
            dry_run: false,
            verbose: false,
            allow_cross_archive_duplicates: false,
            allow_duplicates: false,
            roots: vec![],
            transfer_mode: TransferMode::Copy,
            yes: true,
            resume: false,
        };
        let manifest = fixture.manifest.clone();
        run(
            &mut fixture.db,
            &manifest,
            &options,
            "canon apply",
            &LedgerConfig::default(),
            false,
            None,
        )
    }

    /// C1 — the unforgivable half, now closed by construction rather than by
    /// a gate.
    ///
    /// The shape is the friction's own: two sibling scopes recorded, one
    /// naming no known root, and every locked source under the sibling that
    /// survives. Both halves of the old damage were silent. Placement: the
    /// vantage the survivor alone yields is *deeper*, so every source is still
    /// under it and nothing refuses — `a.jpg` landed at the archive top where
    /// `proj-v1/a.jpg` belongs, at exit 0. And the record: a scoped act
    /// written down as a global one.
    ///
    /// Neither can happen now, and apply is not what stops it. The unrooted
    /// prefix never enters the lock: the measurement was taken from the
    /// confirmed scope alone and the header carries only that, so apply reads
    /// a settled answer and has nothing to get wrong. Apply's own unrooted
    /// refusal is gone with the reading that needed it.
    ///
    /// What the confirmed scope alone yields is `a.jpg` — a single scope is
    /// its own vantage — and that is the honest answer rather than a
    /// consolation: the line naming a place Canon has never heard of cannot
    /// contribute a vantage, so it does not drag one. The user hears about it
    /// at the refresh that wrote the lock, which is where the manifest text
    /// is still the live question.
    #[test]
    fn an_unrooted_scope_moves_neither_the_files_nor_the_record() {
        let mut f = fixture("tree", &["proj-v1"], |root| {
            vec![
                format!("{root}/proj-v1"),
                "/canon-test/no-such-root/proj-v2".to_string(),
            ]
        });

        apply(&mut f).unwrap();

        assert_eq!(
            f.placed(),
            f.expected_placement(),
            "the confirmed scope measures from itself"
        );
        assert_eq!(
            f.recorded_scope(),
            [(
                f.source_root.to_string_lossy().to_string(),
                "proj-v1".to_string()
            )],
            "the record names the confirmed scope, and only it"
        );
    }

    /// C2 — the same, with the unrooted lines on both sides of the confirmed
    /// one and two of them, because the register they used to be read out of
    /// was order-sensitive and read back against a user's own file. None of
    /// them may reach the record, whatever order they were written in.
    #[test]
    fn no_unrooted_prefix_reaches_the_record_whatever_its_position() {
        let mut f = fixture("tree", &["proj-v1"], |root| {
            vec![
                "/canon-test/gone-one".to_string(),
                format!("{root}/proj-v1"),
                "/canon-test/gone-two".to_string(),
            ]
        });

        apply(&mut f).unwrap();

        assert_eq!(f.placed(), f.expected_placement());
        assert_eq!(
            f.recorded_scope(),
            [(
                f.source_root.to_string_lossy().to_string(),
                "proj-v1".to_string()
            )]
        );
    }

    /// C3b — the below-root form mismatch, at the destination.
    ///
    /// An **ASCII root** with an accented folder under it, and a manifest
    /// naming two siblings below that folder in two normalizations — the
    /// ordinary Mac shape, and the one the whole-prefix bend cannot reach: the
    /// root matches as typed, so nothing was ever bent and each line kept
    /// whatever form it was written in. The two lines then diverged at the
    /// accented component, the vantage climbed **above** it, and every file
    /// landed a directory level out, at exit 0, with a decision row naming a
    /// path no folder on disk has.
    ///
    /// A climbing vantage is *more* permissive, so nothing downstream objected:
    /// this is not a case the existing refusals could ever have caught.
    ///
    /// Asserted as an equality between two derived surfaces rather than
    /// against a literal, so the case cannot pass by agreeing with a wrong
    /// answer — and the second assertion is what makes it non-vacuous: with
    /// the accented level dragged in, `placed()` would carry it.
    #[test]
    fn a_below_root_form_mismatch_places_where_the_stored_form_places() {
        const DECOMPOSED: &str = "cafe\u{301}";
        const PRECOMPOSED: &str = "caf\u{e9}";
        assert_ne!(DECOMPOSED, PRECOMPOSED);

        let stored = |root: &str| -> Vec<String> {
            vec![
                format!("{root}/{DECOMPOSED}/proj-v1"),
                format!("{root}/{DECOMPOSED}/proj-v2"),
            ]
        };
        // What a user retyping one of the two lines produces: the same place,
        // in the other normalization, beside a sibling still in the stored one.
        let mixed = |root: &str| -> Vec<String> {
            vec![
                format!("{root}/{PRECOMPOSED}/proj-v1"),
                format!("{root}/{DECOMPOSED}/proj-v2"),
            ]
        };

        let selected = [
            &format!("{DECOMPOSED}/proj-v1")[..],
            &format!("{DECOMPOSED}/proj-v2")[..],
        ];
        let mut baseline = fixture_under(DECOMPOSED, "tree", &selected, stored);
        let mut mismatched = fixture_under(DECOMPOSED, "tree", &selected, mixed);

        apply(&mut baseline).unwrap();
        apply(&mut mismatched).unwrap();

        assert_eq!(
            mismatched.placed(),
            baseline.placed(),
            "a line retyped below an ASCII root must place where the stored form places"
        );
        assert_eq!(
            baseline.placed(),
            ["proj-v1/day1/a.jpg", "proj-v2/day1/b.jpg"],
            "the sibling scopes' own names survive and the accented level above them does not"
        );
        assert!(
            !mismatched
                .placed()
                .iter()
                .any(|p| p.contains(DECOMPOSED) || p.contains(PRECOMPOSED)),
            "the level the vantage used to climb above must not reach the archive: {:?}",
            mismatched.placed()
        );
    }

    /// C4 — the old-lock refusal, and it is **unconditional**.
    ///
    /// A lock with no header carries no settled scope, so an apply that went
    /// ahead would write `decisions.scope` empty — a global row for a scoped
    /// act, with no `decision_scopes` rows behind it. That is a silent
    /// provenance gap whatever the pattern says, which is why the pattern is
    /// not consulted: the second half of this test uses `{filename}`, which
    /// needs no measurement at all, and is refused just the same.
    #[test]
    fn a_lock_with_no_header_is_refused_whatever_the_pattern() {
        for pattern in ["{scope.rel_path}", "{filename}"] {
            let mut f = fixture("tree", &["proj-v1"], |root| vec![format!("{root}/proj-v1")]);
            f.set_pattern(pattern);
            f.strip_lock_header();

            let message = format!("{:#}", apply(&mut f).unwrap_err());
            assert!(
                message.contains("cluster refresh"),
                "the refusal must name the way back, for {pattern}: {message}"
            );
            assert!(
                f.placed().is_empty(),
                "nothing may move for {pattern}: {:?}",
                f.placed()
            );
            assert_eq!(
                f.decision_count(),
                0,
                "a run refused before planning leaves no decision row"
            );
        }
    }

    /// C4b — an absent measurement has **two** causes and they take different
    /// answers. A manifest that records no scope measures nothing — correctly,
    /// there is nothing to measure from — so its entries carry no
    /// `scope_rel_path` in a perfectly current lock. Refusing that with the
    /// old-lock message would assert a cause Canon cannot know and prescribe a
    /// refresh that rebuilds the same lock, leaving the user in a loop.
    ///
    /// Reached by two ordinary actions: generating unscoped (`--global`, or
    /// filters alone) and then editing the pattern, or emptying `meta.scope`
    /// and refreshing — which is exactly what the new rule tells a user to do.
    #[test]
    fn an_unscoped_manifest_says_so_rather_than_blaming_the_lock() {
        let mut f = fixture("tree", &["proj-v1"], |_| vec![]);
        f.set_pattern("{scope.rel_path}");

        // The wrapper apply exits with is the same either way, and the cause
        // it prints goes to stderr — so asserting on the error alone cannot
        // tell fixed from unfixed. The cause is read back through
        // `plan_apply`'s own violations, where it is data.
        let message = format!("{:#}", apply(&mut f).unwrap_err());
        assert!(
            message.contains("pattern expansion"),
            "the run must refuse on the expansion, not on the lock's age: {message}"
        );
        assert!(f.placed().is_empty(), "nothing may move: {:?}", f.placed());

        for (_, reason) in f.expansion_failures() {
            assert!(
                reason.contains("records no scope"),
                "the cause must be the manifest, not the lock's age: {reason}"
            );
            assert!(
                !reason.contains("cluster refresh"),
                "a refresh cannot give an unscoped manifest a scope: {reason}"
            );
        }
    }

    /// C5 — the incoherence the whole story exists to close, from the user's
    /// side. Editing `meta.scope` after the lock was written used to change
    /// **where files land** while leaving **which files** alone — half an
    /// edit landing, silently. Now neither half lands until a refresh, and the
    /// record says what the lock settled rather than what the text says.
    #[test]
    fn editing_the_scope_after_the_lock_changes_neither_placement_nor_record() {
        let mut edited = fixture("tree", &["proj-v1"], |root| vec![format!("{root}/proj-v1")]);
        let untouched = fixture("tree", &["proj-v1"], |root| vec![format!("{root}/proj-v1")]);

        // A user hand-edits the scope up one level. The lock is untouched, so
        // its hash still matches and apply proceeds.
        let root = edited.source_root.to_string_lossy().to_string();
        let text = fs::read_to_string(&edited.manifest).unwrap();
        fs::write(
            &edited.manifest,
            text.replace(&format!("{root}/proj-v1"), &root),
        )
        .unwrap();

        apply(&mut edited).unwrap();

        assert_eq!(
            edited.placed(),
            untouched.expected_placement(),
            "an edited meta.scope must not move a destination"
        );
        assert_eq!(
            edited.recorded_scope(),
            [(root, "proj-v1".to_string())],
            "an edited meta.scope must not change what the record claims"
        );
    }

    /// C6 — the workflow that must not break in closing C5. `output.pattern`
    /// is the line of a manifest a user edits most, and it is a property of
    /// how they want things *named* rather than of the selection, so it stays
    /// read at apply time and takes effect without a refresh.
    #[test]
    fn editing_the_pattern_after_the_lock_still_takes_effect() {
        let mut f = fixture("tree", &["proj-v1"], |root| vec![format!("{root}/proj-v1")]);
        f.set_pattern("keep/{filename}");

        apply(&mut f).unwrap();

        assert_eq!(f.placed(), ["keep/a.jpg"]);
    }

    /// C3 — the form half's positive side, checked as an equality between two
    /// derived surfaces rather than against a literal destination: a manifest
    /// naming its scope in one normalization must place exactly where one
    /// naming the same places in the stored form places. Both runs classify
    /// the same rows, so any difference between them is the mismatch itself.
    #[test]
    fn a_form_mismatched_scope_places_where_the_generated_one_would() {
        // A component with two spellings. They must actually differ, or the
        // case is vacuous and would pass against the defect it names.
        const DECOMPOSED: &str = "cafe\u{301}";
        const PRECOMPOSED: &str = "caf\u{e9}";
        assert_ne!(DECOMPOSED, PRECOMPOSED);

        // `shift` is what a user retyping the path produces: the same place,
        // spelled in the other normalization.
        let shift = |s: &str| -> String {
            if s.contains(DECOMPOSED) {
                s.replace(DECOMPOSED, PRECOMPOSED)
            } else {
                s.replace(PRECOMPOSED, DECOMPOSED)
            }
        };
        let stored = |root: &str| -> Vec<String> {
            vec![format!("{root}/proj-v1"), format!("{root}/proj-v2")]
        };

        let mut baseline = fixture(DECOMPOSED, &["proj-v1", "proj-v2"], stored);
        let mut mismatched = fixture(DECOMPOSED, &["proj-v1", "proj-v2"], |root| {
            stored(root).iter().map(|s| shift(s)).collect()
        });
        let root = baseline.source_root.to_string_lossy().to_string();
        assert_ne!(
            shift(&root),
            root,
            "the stored root carries neither spelling — the fixture proves nothing"
        );

        apply(&mut baseline).unwrap();
        apply(&mut mismatched).unwrap();

        assert_eq!(
            mismatched.placed(),
            baseline.placed(),
            "a scope typed in the other normalization must place where the stored form places"
        );
        assert!(
            baseline.placed().iter().any(|p| p.contains('/')),
            "each scope's own name must survive, or this proves nothing: {:?}",
            baseline.placed()
        );
    }
}
