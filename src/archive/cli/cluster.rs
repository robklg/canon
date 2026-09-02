use anyhow::{bail, Context, Result};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::archive::domain::{parse_manifest_allow, validate_manifest_version, ManifestConfig};
use crate::archive::ops::generate::{
    ClusterGenerateParams, ClusterGeneratePlan, ExecuteGenerateParams, ExecuteRefreshParams,
};
use crate::archive::ops::{generate as generate_ops, status as status_ops};
use crate::core::domain::config::{LedgerConfig, RecordingMode};
use crate::core::domain::decision::{DecisionCommand, DecisionStatus};
use crate::core::domain::format::first_chars;
use crate::core::domain::format_count;
use crate::core::domain::scope::ScopeResolution;
use crate::core::ops::decision::{DecisionCounts, DecisionParams, DecisionRecorder};
use crate::core::ops::scope::{
    classify_all, no_sources_known, resolve_archive_path, resolve_recorded_scope,
};
use crate::core::repo::{self, Connection, Db};
use crate::expr::Filter;

pub struct GenerateOptions {
    pub force: bool,
    pub allow_archived: bool,
    pub allow_duplicates: bool,
    pub show_archived: bool,
    pub no_edit: bool,
}

#[allow(clippy::too_many_arguments)]
pub fn generate(
    db: &mut Db,
    scope_prefixes: &[String],
    original_filters: &[String],
    expanded_filters: &[String],
    dest: &Path,
    output_path: &Path,
    options: &GenerateOptions,
    command_line: &str,
    ledger: &LedgerConfig,
    no_receipt: bool,
) -> Result<()> {
    // Prevent overwriting existing TOML config (unless --force)
    if output_path.exists() && !options.force {
        bail!(
            "Output file '{}' already exists.\n\
             Use `cluster refresh` to update the lock file, or -f/--force to overwrite.",
            output_path.display()
        );
    }

    // Require at least one of path scope or filters
    if scope_prefixes.is_empty() && expanded_filters.is_empty() {
        bail!("At least one of path or --where filter is required");
    }

    let conn = db.conn_mut();

    // Fetch all roots for archive path resolution
    let all_roots = repo::root::fetch_all(conn)?;

    // Resolve destination to archive root + relative subdir
    let (archive_root_id, _archive_root_path, base_dir) = resolve_archive_path(&all_roots, dest)?;

    let parsed_filters: Vec<Filter> = expanded_filters
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    // The scope this run settles on, through the very code a manifest's own
    // scope goes through — the requester's "exactly the same code" ruling.
    // Asking again is what makes generate and refresh incapable of disagreeing;
    // it is not a formality over prefixes already known to confirm, as the
    // paragraph below says.
    let generate_scope = resolve_recorded_scope(conn, scope_prefixes, &all_roots)?;

    // A prefix reaching here can still be sourceless, and assuming otherwise
    // is a panic in the commonest invocation there is. `resolve_scope` gates
    // *explicit* paths through the source-existence policy, but its **CWD**
    // branch does not: it resolves the current directory and hands it over
    // unasked, because CWD defaulting is a context switch rather than an
    // assertion about content. So `canon cluster generate --where …` run from
    // a folder created since the last scan arrives here with one prefix the
    // index knows nothing under.
    //
    // Stated, then treated exactly as a refresh treats it — same registers,
    // same answer — which is the "exactly the same code" ruling applied to the
    // arm that is easiest to believe cannot happen.
    crate::scope::write_set_asides(&mut std::io::stdout().lock(), generate_scope.set_aside());

    // Plan. Selection reads the same healed register the measurement is taken
    // from — never the raw argument list beside it — and a scope that
    // confirmed nothing selects nothing rather than everything. See the
    // refresh below, which does the identical thing for the identical reason.
    let mut plan = match generate_scope.selection() {
        Some(prefixes) => {
            let params = ClusterGenerateParams {
                scopes: classify_all(&prefixes),
                filters: parsed_filters,
                allow_archived: options.allow_archived,
                allow_duplicates: options.allow_duplicates,
            };
            generate_ops::plan_generate(conn, &params)?
        }
        None => ClusterGeneratePlan::empty(),
    };

    // Display warnings
    display_plan_warnings(&plan, options);

    if plan.lock_entries.is_empty() {
        println!("No sources matched the query");
        return Ok(());
    }

    // Execute
    let lock_path = output_path.with_extension("lock");
    let exec_params = ExecuteGenerateParams {
        lock_path: lock_path.clone(),
        manifest_path: output_path.to_path_buf(),
        expanded_filters: expanded_filters.to_vec(),
        original_filters: original_filters.to_vec(),
        scope: generate_scope.clone(),
        roots: all_roots.clone(),
        archive_root_id,
        base_dir,
        allow: allow_values_to_strings(options),
    };

    let decision = DecisionParams {
        command: DecisionCommand::ClusterGenerate,
        // The same register the header, the measurement and the selection read.
        // `DecisionScope::decompose` would answer the same for these prefixes
        // today and match roots byte-exactly rather than form-tolerantly, which
        // is a fourth spelling of one question.
        scope: generate_scope.scopes().to_vec(),
        command_line: command_line.to_string(),
        reason: None,
        record_enabled: ledger.recording != RecordingMode::Off,
        receipt_enabled: ledger.recording == RecordingMode::Full && !no_receipt,
        ledger_config: ledger.clone(),
    };
    // The receipt context is None on purpose, and that — not the flag
    // computed above — is what stops a receipt file being written. Generating
    // a manifest performs no transition on any source; the manifest itself is
    // the artifact the run leaves behind.
    let mut recorder = DecisionRecorder::start(conn, &decision, None);

    let result = generate_ops::execute_generate(&mut plan, &exec_params)?;

    let gen_summary = format!(
        "Generated manifest: {} ({} sources in {})",
        output_path.display(),
        result.source_count,
        lock_path.display()
    );
    let full_summary = result.compose_summary(&gen_summary);

    recorder.complete(
        conn,
        DecisionStatus::Completed,
        DecisionCounts {
            attempted: Some(result.source_count as i64),
            completed: Some(result.source_count as i64),
            failed: None,
            skipped: None,
        },
        &full_summary,
    );
    for w in recorder.take_warnings() {
        eprintln!("{w}");
    }

    println!("{}", full_summary);

    warn_if_written_file_blocks_placement(&result.pattern, dest, &[output_path, &lock_path]);

    if !options.no_edit {
        if let Err(e) = open_editor(output_path) {
            eprintln!("Warning: {e}");
        }
    }

    // Only spaces are quoted here. A path carrying a quote or a shell
    // metacharacter produces a suggestion that will not run as printed.
    let path_str = output_path.display().to_string();
    let escaped = if path_str.contains(' ') {
        format!("'{path_str}'")
    } else {
        path_str
    };
    eprintln!("\nTo apply: canon apply {escaped}");

    Ok(())
}

/// Open a file in the user's preferred editor ($VISUAL, $EDITOR, or vi).
///
/// A failed launch or a non-zero exit comes back as an error; what that means
/// is the caller's to decide. After a file is written it is a warning — the
/// work is already on disk. Before a re-query it is a refusal, because the
/// editor's exit is the user's answer about what to query.
fn open_editor(path: &Path) -> Result<()> {
    let editor = std::env::var("VISUAL")
        .or_else(|_| std::env::var("EDITOR"))
        .unwrap_or_else(|_| "vi".to_string());
    match std::process::Command::new(&editor).arg(path).status() {
        Ok(status) if !status.success() => match status.code() {
            Some(code) => bail!("editor '{editor}' exited with status {code}"),
            None => bail!("editor '{editor}' was terminated before it exited"),
        },
        Err(e) => bail!("failed to launch editor '{editor}': {e}"),
        _ => Ok(()),
    }
}

/// Read a manifest from disk. Split from parsing so a `--edit` refresh can
/// read the file twice and parse only what the user saved.
fn read_manifest_text(config_path: &Path) -> Result<String> {
    fs::read_to_string(config_path)
        .with_context(|| format!("Failed to read config: {}", config_path.display()))
}

/// Parse a manifest: TOML body, version gate, `[options] allow` vocabulary.
/// One spelling, so the content a `--edit` refresh re-queries from passes the
/// same gates the plain refresh applies.
fn parse_manifest(content: &str, config_path: &Path) -> Result<(ManifestConfig, bool, bool)> {
    let config: ManifestConfig = toml::from_str(content)
        .with_context(|| format!("Failed to parse config: {}", config_path.display()))?;
    validate_manifest_version(config.meta.version)?;
    let (allow_archived, allow_duplicates) = parse_manifest_allow(&config.options.allow)?;
    Ok((config, allow_archived, allow_duplicates))
}

pub fn refresh(
    db: &mut Db,
    config_path: &Path,
    show_archived: bool,
    edit: bool,
    command_line: &str,
    ledger: &LedgerConfig,
    no_receipt: bool,
) -> Result<()> {
    let launch = |path: &Path| open_editor(path);
    let editor: Option<EditorPass> = if edit { Some(&launch) } else { None };
    refresh_with_editor(
        db,
        config_path,
        show_archived,
        editor,
        command_line,
        ledger,
        no_receipt,
    )
}

/// The user's editor session, as the refresh calls it: hand it the manifest,
/// hear back whether the pass succeeded.
type EditorPass<'a> = &'a dyn Fn(&Path) -> Result<()>;

/// The refresh, with the editor pass as a parameter.
///
/// `Some(_)` is `--edit`: the pass runs on the manifest before anything is
/// read for the re-query. Tests stand in for the user's editor session here,
/// rather than through the process environment the whole test binary shares.
#[allow(clippy::too_many_arguments)]
fn refresh_with_editor(
    db: &mut Db,
    config_path: &Path,
    show_archived: bool,
    editor: Option<EditorPass>,
    command_line: &str,
    ledger: &LedgerConfig,
    no_receipt: bool,
) -> Result<()> {
    let conn = db.conn_mut();

    // Read existing manifest content (for notes preservation)
    let mut old_content = read_manifest_text(config_path)?;

    // The editor comes first, and the refresh re-queries from what was saved.
    // Editing a query after the query has run leaves the manifest describing
    // one thing and its lock file holding another. The read above is only to
    // establish the file is there — nothing is parsed until the user has had
    // their pass, so a manifest that currently fails to parse can still be
    // fixed this way. The manifest is edited in place, so an abort below
    // leaves the user's words in their own file — which is why this does not
    // route through the ceremony's editor helper: that one edits a temp draft
    // and hands the text back, and an abort would strand the words there.
    if let Some(edit) = editor {
        edit(config_path).with_context(|| {
            format!(
                "Refresh aborted — {} and its lock file are unchanged",
                config_path.display()
            )
        })?;
        old_content = read_manifest_text(config_path)?;
    }

    let (config, allow_archived, allow_duplicates) = parse_manifest(&old_content, config_path)?;

    // Report which options are in effect
    if !config.options.allow.is_empty() {
        eprintln!(
            "Options from manifest: allow {}",
            config.options.allow.join(", ")
        );
    }

    // The roots are fetched here rather than beside the placement warning
    // below, because the recorded scope has to be resolved against them before
    // anything reads it: the re-query, the write-back and the decision record
    // all take that one answer.
    let refresh_roots = repo::root::fetch_all(conn)?;

    // Taken as recorded, then resolved — no split, nothing to reconstruct.
    // Through the ops-layer resolution, so a scope line reaching Canon in a
    // manifest is healed by the same lookup as one typed on the command line
    // and partitioned by the same policy.
    let scope = resolve_recorded_scope(conn, &config.meta.scope, &refresh_roots)?;

    // Both statements go out on the honesty policy's own position for an
    // effectful command: before any plan display and before any confirmation.
    // On **stdout**, which is that policy's channel for an effectful command
    // (`scope::print_scope_set_asides`) and the channel `cluster generate` and
    // `cluster status` already use for the same sentences — the skip and the
    // outcome it qualifies belong on one stream. What is said, and in which
    // order, is `scope_statements`' decision; this only prints it.
    let refusing = refresh_must_refuse(&scope);
    {
        use std::io::Write as _;
        let _ = std::io::stdout()
            .lock()
            .write_all(&scope_statements(&scope, refusing));
    }

    if refusing {
        bail!(
            "{}\n\
             Refresh aborted — {} and its lock file are unchanged.\n\
             {}",
            no_sources_known(scope.set_aside()),
            config_path.display(),
            super::edit_then_refresh(config_path)
        );
    }

    // Parse filters from config
    let parsed_filters: Vec<Filter> = config
        .meta
        .query
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    // Plan, from the **confirmed** register — the one the measurement and the
    // lock header are built from. A rooted prefix is re-queried in the
    // byte-form the index stores, so a manifest naming its scope in another
    // normalization matches the sources it names; and a line that measures
    // nothing selects nothing, so the run cannot gather content it has just
    // told the user it has no destination for.
    //
    // A scope that resolved to nothing selects nothing rather than everything:
    // an empty scope list means global to the planner, and a manifest naming a
    // place Canon cannot find must not become a whole-universe archive.
    let mut plan = match scope.selection() {
        Some(prefixes) => {
            let plan_params = ClusterGenerateParams {
                scopes: classify_all(&prefixes),
                filters: parsed_filters,
                allow_archived,
                allow_duplicates,
            };
            generate_ops::plan_generate(conn, &plan_params)?
        }
        None => ClusterGeneratePlan::empty(),
    };

    // Display warnings
    let display_options = GenerateOptions {
        force: false,
        allow_archived,
        allow_duplicates,
        show_archived,
        no_edit: true,
    };
    display_plan_warnings(&plan, &display_options);

    // Execute
    let lock_path = config_path.with_extension("lock");

    // Where this manifest's files would land, for the write-time collision
    // warning below. The loop that produces the collision is generate (no
    // prefix) → edit adds one → refresh, so the rewrite is usually the moment
    // it becomes knowable. A manifest naming a root that is gone gets no
    // warning rather than an error: the refresh itself is still legitimate.
    let placement_root = refresh_roots
        .iter()
        .find(|r| r.id == config.output.archive_root_id)
        .map(|r| Path::new(&r.path).join(&config.output.base_dir));
    let written_pattern = config.output.pattern.clone();

    let exec_params = ExecuteRefreshParams {
        lock_path: lock_path.clone(),
        manifest_path: config_path.to_path_buf(),
        old_manifest_content: old_content,
        // What goes back into meta.scope: a rooted prefix healed to the
        // byte-form the index stores, so a refresh converges on what
        // generation records for the same input; an unrooted one verbatim,
        // because there is nothing to heal it to and rewriting it would be
        // inventing a place.
        scope: scope.clone(),
        roots: refresh_roots,
        config,
    };

    // A refresh whose query now matches nothing records nothing — the 0-item
    // convention, the same answer `cluster generate` gives. The lock is still
    // cleared and the manifest rewritten, because the declaration has to keep
    // telling the truth about its query; but no source was touched, and a 0/0
    // row in the trail would claim an act where there was none.
    if plan.lock_entries.is_empty() {
        generate_ops::execute_refresh(&mut plan, &exec_params)?;
        println!("No sources matched the query");
        // The manifest only: this arm removed the lock file, and a warning
        // about a file standing in the way would be naming one that no longer
        // stands anywhere.
        if let Some(root) = &placement_root {
            warn_if_written_file_blocks_placement(&written_pattern, root, &[config_path]);
        }
        return Ok(());
    }

    let decision = DecisionParams {
        command: DecisionCommand::ClusterRefresh,
        scope: scope.scopes().to_vec(),
        command_line: command_line.to_string(),
        reason: None,
        record_enabled: ledger.recording != RecordingMode::Off,
        receipt_enabled: ledger.recording == RecordingMode::Full && !no_receipt,
        ledger_config: ledger.clone(),
    };
    // The receipt context is None on purpose, and that — not the flag
    // computed above — is what stops a receipt file being written. Generating
    // a manifest performs no transition on any source; the manifest itself is
    // the artifact the run leaves behind.
    let mut recorder = DecisionRecorder::start(conn, &decision, None);

    let result = generate_ops::execute_refresh(&mut plan, &exec_params)?;

    // Only an empty plan leaves no outcome, and that path returned above; a
    // missing one here means the lock was never written, and this `?` leaves
    // the decision row unsettled — one of the sites where an error between
    // start() and completion still exits without naming an outcome.
    let r = result
        .outcome
        .context("refresh wrote no lock file for a non-empty plan")?;
    let refresh_summary = format!(
        "Refreshed lock file: {} ({} sources)",
        lock_path.display(),
        r.source_count
    );
    let full_summary = r.compose_summary(&refresh_summary);
    recorder.complete(
        conn,
        DecisionStatus::Completed,
        DecisionCounts {
            attempted: Some(r.source_count as i64),
            completed: Some(r.source_count as i64),
            failed: None,
            skipped: None,
        },
        &full_summary,
    );
    println!("{}", full_summary);
    for w in recorder.take_warnings() {
        eprintln!("{w}");
    }

    if let Some(root) = &placement_root {
        warn_if_written_file_blocks_placement(&r.pattern, root, &[config_path, &lock_path]);
    }

    Ok(())
}

// ============================================================================
// Display helpers
// ============================================================================

/// Say so when the file just written stands where the pattern's placements
/// need a directory.
///
/// The apply would fail on it later, one transfer at a time; here it is one
/// line at the moment the arrangement was made, naming both paths.
///
/// Both sides are resolved to the same form first, because they arrive in
/// different ones: the manifest path as the user typed it on the command line,
/// the destination from the archive root's stored canonical path. On a machine
/// where a mount or a temp directory is reached through a symlink, the two
/// spellings of one place never compare equal, and the warning would simply
/// never fire. A file that does not exist yet resolves through its parent,
/// which does.
fn placement_warning(
    pattern: &str,
    placement_root: &Path,
    written_paths: &[&Path],
) -> Option<String> {
    let resolve = |p: &Path| -> PathBuf {
        if let Ok(real) = fs::canonicalize(p) {
            return real;
        }
        let through_parent = match (p.parent(), p.file_name()) {
            (Some(parent), Some(name)) => fs::canonicalize(parent).ok().map(|real| real.join(name)),
            _ => None,
        };
        through_parent.unwrap_or_else(|| std::path::absolute(p).unwrap_or_else(|_| p.to_path_buf()))
    };
    let root = resolve(placement_root);
    let written: Vec<PathBuf> = written_paths.iter().map(|p| resolve(p)).collect();
    let refs: Vec<&Path> = written.iter().map(|p| p.as_path()).collect();

    let (blocker, blocked) =
        generate_ops::placement_blocked_by_written_file(pattern, &root, &refs)?;
    let situation = if blocker == blocked {
        format!(
            "Warning: {} is also the directory this manifest's pattern places files into.",
            blocker.display()
        )
    } else {
        format!(
            "Warning: {} stands in the way of {}, where this manifest's pattern places files.",
            blocker.display(),
            blocked.display()
        )
    };
    Some(format!(
        "{situation}\n  Apply will refuse until one of them moves: write the manifest \
         elsewhere (-o/-O), or edit the pattern."
    ))
}

fn warn_if_written_file_blocks_placement(
    pattern: &str,
    placement_root: &Path,
    written_paths: &[&Path],
) {
    if let Some(warning) = placement_warning(pattern, placement_root, written_paths) {
        eprintln!("{warning}");
    }
}

/// Display plan warnings (archived files, mixed types) to stderr.
fn display_plan_warnings(plan: &ClusterGeneratePlan, options: &GenerateOptions) {
    if !plan.archived.is_empty() {
        eprintln!(
            "Excluded {} sources already in archive(s)",
            plan.archived.len()
        );
        if options.show_archived {
            eprintln!("Archived files:");
            for (source_path, archive_path) in &plan.archived {
                eprintln!("  {source_path} -> {archive_path}");
            }
        } else {
            eprintln!("Use --show-archived to list them");
        }
        eprintln!("Use --allow archived to include them");
    }

    if !plan.mixed_type_warnings.is_empty() {
        eprintln!("Warning: some facts have inconsistent types across sources:");
        for (key, breakdown) in &plan.mixed_type_warnings {
            eprintln!("  {key}: {breakdown}");
        }
        eprintln!("  Type-specific modifiers (|year, |month, etc.) may fail on mismatched values.");
        eprintln!("  To fix: delete outliers with 'canon facts delete <key> --on object --value-type <minority-type>'");
    }
}

/// The step that moves this manifest forward, or nothing when there is none.
///
/// **Whether** there is a step does not depend on the recorded scope: it is
/// files still to transfer, and nothing lost or wrong. That gate is outermost
/// on purpose. A manifest with nothing pending has no next step to take, and
/// one with content missing has just been told to check the volume before
/// refreshing — advising a refresh in either state clears the lock and gains
/// nothing, and in the second it contradicts the line above it.
///
/// **Which** step it is depends on one thing only: whether `apply` can run at
/// all. A report must never name the one command that cannot — and naming a
/// remedy that will not work is the same fault wearing a different word. Two
/// states answer it, and they take different answers:
///
/// - a lock written **before the measurement was recorded in it**, which
///   `apply` refuses outright whatever the pattern says. A refresh rebuilds
///   the lock and the run proceeds, so that is the step.
/// - a **pattern that does not expand**, which `apply` collects and aborts on
///   before it transfers anything. Here there is **no step**: what a pattern
///   needs — a fact nothing supplies, a scope the manifest does not record —
///   is not something any one command gives it, and naming a refresh would be
///   the same fault in a different word. Silence is the honest answer, the
///   same one a manifest with content missing gets — and it is only honest
///   because the report says what failed a few lines above, in its own block.
///   Do not make this branch silent without that block: the per-entry table
///   shows a fixed status word and never the reason.
///
/// A scope line that resolves to nothing is deliberately **not** such a state.
/// It once was, because `apply` used to refuse an unrooted prefix; now the
/// measurement and the recorded scope are both settled in the lock without it,
/// so applying is exactly the right next step — and sending the user to a
/// refresh that would preserve the line verbatim only loops them. The line is
/// still stated above; it is not a blocker.
fn write_next_step(
    handle: &mut impl Write,
    status: &status_ops::ManifestStatus,
    manifest_path: &Path,
) {
    if status.pending == 0 || !status.all_accounted_for() {
        return;
    }
    if status.pattern_unexpandable && !status.lock_predates_measurement {
        return;
    }
    let _ = writeln!(handle);
    if status.lock_predates_measurement {
        let _ = writeln!(
            handle,
            "To rebuild the lock: canon cluster refresh {}",
            manifest_path.display()
        );
    } else if status.at_destination > 0 {
        let _ = writeln!(
            handle,
            "To complete: canon apply --resume {}",
            manifest_path.display()
        );
    } else {
        let _ = writeln!(handle, "To apply: canon apply {}", manifest_path.display());
    }
}

/// What a refresh says about its recorded scope, in order.
///
/// Composed rather than printed so the **ordering** is a value a test can
/// read. It is a real decision, not a formatting detail: the unrooted lines
/// are stated whether or not the run is about to refuse, because the refusal
/// names only the set-asides and gating them on it would leave a prefix under
/// no known root unsaid in exactly the run that had least to work with. The
/// set-asides are stated only when the run continues, because the refusal
/// names every one of them itself.
fn scope_statements(scope: &ScopeResolution, refusing: bool) -> Vec<u8> {
    let mut out = Vec::new();
    write_unrooted_scope(&mut out, scope.unrooted());
    if !refusing {
        crate::scope::write_set_asides(&mut out, scope.set_aside());
    }
    out
}

/// Whether a refresh must refuse: the source-existence policy's terminal rule
/// at the manifest door — a scope that kept nothing must never look like a
/// narrowing.
///
/// An all-unrooted scope is deliberately not this case. It kept nothing
/// either, but a refresh is the way back from a manifest naming a root that is
/// gone, and it selects nothing rather than everything, so continuing costs
/// the user only the lock their own manifest no longer describes.
fn refresh_must_refuse(scope: &ScopeResolution) -> bool {
    scope.scopes().is_empty() && !scope.set_aside().is_empty()
}

/// The one spelling of an unrooted scope prefix, shared by the two commands
/// that read a recorded scope and report rather than refuse.
///
/// A prefix the manifest records that names no known root is named, with what
/// follows from it — never dropped. Dropping one silently narrows whatever the
/// reader does next, which is the class this closes; the twin of the scope
/// boundary's own `no sources known at <p> — skipped`, at the manifest door.
fn write_unrooted_scope(handle: &mut impl Write, unrooted: &[String]) {
    for path in unrooted {
        let _ = writeln!(
            handle,
            "no known root at {path} — kept in the manifest, no destination measures from it"
        );
    }
}

fn allow_values_to_strings(options: &GenerateOptions) -> Vec<String> {
    let mut v = Vec::new();
    if options.allow_archived {
        v.push("archived".to_string());
    }
    if options.allow_duplicates {
        v.push("duplicates".to_string());
    }
    v
}

// ============================================================================
// Status
// ============================================================================

pub fn status(conn: &mut Connection, manifest_path: &Path, verbose: bool) -> Result<()> {
    let status = status_ops::compute_manifest_status(conn, manifest_path)?;

    // Header
    println!("Manifest: {}", status.manifest_path);
    println!("Destination: {}", status.dest_display);
    println!("Pattern: {}", status.pattern);
    if !status.lock_hash_valid {
        eprintln!("Warning: lock file hash mismatch — manifest may be out of sync.");
    }
    println!("Lock: {} entries", format_count(status.lock_entry_count));
    // Stated on stdout, this command's own scope channel — a report's scope
    // belongs beside its header, not on a side channel.
    {
        let mut out = std::io::stdout().lock();
        write_unrooted_scope(&mut out, &status.unrooted_scope);
        crate::scope::write_set_asides(&mut out, &status.set_aside_scope);
    }
    println!();

    // Per-entry table: concerning entries (or all if verbose)
    let concerning: Vec<&status_ops::StatusEntry> = status
        .entries
        .iter()
        .filter(|e| {
            matches!(
                e.status,
                status_ops::EntryStatus::SourceLost | status_ops::EntryStatus::SizeMismatch
            )
        })
        .collect();

    let show_entries: Vec<&status_ops::StatusEntry> = if verbose {
        status.entries.iter().collect()
    } else {
        concerning.clone()
    };

    if !show_entries.is_empty() {
        // Column headers
        let hdr_db = "DB registered";
        let sep_db = "-------------";
        println!(
            "  {:<30}  {:<12}  {:<12}  {}",
            "Source", "Source file", "Dest file", hdr_db
        );
        println!(
            "  {:<30}  {:<12}  {:<12}  {}",
            "------", "-----------", "---------", sep_db
        );
        for entry in &show_entries {
            let source_status = if entry.source_exists {
                "present"
            } else {
                "MISSING"
            };
            let dest_status = match entry.status {
                status_ops::EntryStatus::AtDestination => "at dest",
                status_ops::EntryStatus::SizeMismatch => "WRONG SIZE",
                status_ops::EntryStatus::Pending | status_ops::EntryStatus::SourceLost => {
                    "not at dest"
                }
            };
            let db_status = if entry.db_registered {
                "yes"
            } else {
                "\u{2014}"
            };
            // Truncate filename for display. Counted in characters, not bytes,
            // so the width matches the column below and a name that is not
            // plain ASCII cannot be cut inside a character.
            let display_name = if entry.source_filename.chars().count() > 30 {
                format!("{}...", first_chars(&entry.source_filename, 27))
            } else {
                entry.source_filename.clone()
            };
            println!(
                "  {:<30}  {:<12}  {:<12}  {}",
                display_name, source_status, dest_status, db_status
            );
        }
        if !verbose && !concerning.is_empty() {
            println!();
            println!(
                "  (showing {} concerning entries; use --verbose for all)",
                concerning.len()
            );
        }
        println!();
    }

    // A manifest whose pattern will not expand cannot be applied, and the
    // per-entry table above does not carry the reason — it shows a fixed
    // status word. Said here, once per distinct reason rather than once per
    // source, because a pattern fails the same way for every entry and N
    // copies of one sentence is not a report.
    if !status.expansion_failures.is_empty() {
        println!(
            "{} of {} entries have no destination:",
            format_count(status.expansion_failure_count),
            format_count(status.lock_entry_count),
        );
        for reason in &status.expansion_failures {
            println!("  {reason}");
        }
        // Which ones, not only how many: the per-entry table below shows a
        // fixed status word, so without this the report names a count and
        // never a file. Capped like every other listing here.
        const SHOWN: usize = 5;
        let failed: Vec<&status_ops::StatusEntry> = status
            .entries
            .iter()
            .filter(|e| e.dest_path.starts_with(status_ops::EXPANSION_FAILED))
            .collect();
        for entry in failed.iter().take(SHOWN) {
            println!("  {}", entry.source_path);
        }
        if failed.len() > SHOWN {
            println!("  ... and {} more", format_count(failed.len() - SHOWN));
        }
        println!();
    }

    // Summary
    println!(
        "Summary: {} at destination, {} pending, {} source files missing.",
        format_count(status.at_destination),
        format_count(status.pending),
        format_count(status.source_lost),
    );
    if status.source_still_present > 0 {
        println!(
            "         {} at destination with source still present.",
            format_count(status.source_still_present),
        );
    }
    if status.size_mismatch > 0 {
        println!(
            "         {} with size mismatch at destination.",
            format_count(status.size_mismatch),
        );
    }

    // Safety assessment
    if status.all_accounted_for() {
        println!("All source files accounted for.");
    } else {
        if status.source_lost > 0 {
            println!();
            println!(
                "WARNING: {} source files are missing and not at the destination.",
                format_count(status.source_lost),
            );
            let lost_entries: Vec<&status_ops::StatusEntry> = status
                .entries
                .iter()
                .filter(|e| e.status == status_ops::EntryStatus::SourceLost)
                .collect();
            for entry in &lost_entries {
                println!(
                    "  {} (source: {}, dest: {})",
                    entry.source_filename, entry.source_path, entry.dest_path
                );
            }
            println!();
            println!("Check if the source volume is connected. If files are truly lost,");
            println!(
                "refresh the manifest: canon cluster refresh {}",
                manifest_path.display()
            );
        }
        if status.size_mismatch > 0 {
            println!();
            println!(
                "WARNING: {} files at destination have unexpected size.",
                format_count(status.size_mismatch),
            );
        }
    }

    write_next_step(&mut std::io::stdout().lock(), &status, manifest_path);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::testing::{insert_object, insert_root, insert_source, setup_test_db};
    use std::path::PathBuf;

    /// A manifest as generation leaves it: commented shape, Notes, TOML body.
    fn manifest_text(query: &str) -> String {
        format!(
            "# Canon manifest — edit pattern and Notes freely.\n\
             #\n\
             # === Notes ===\n\
             # keep these words\n\
             [meta]\n\
             version = 1\n\
             query = [\"{query}\"]\n\
             generated_at = \"2026-01-01T00:00:00Z\"\n\
             lock_hash = \"\"\n\
             \n\
             [options]\n\
             allow = []\n\
             \n\
             [output]\n\
             pattern = \"{{filename}}\"\n\
             archive_root_id = 1\n\
             base_dir = \"out\"\n"
        )
    }

    /// One `.jpg` and one `.png` under a source root, and a manifest asking
    /// for the jpg — so an edited query is visible in the lock file.
    fn setup(dir: &Path) -> (Db, PathBuf, PathBuf) {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let jpg = insert_object(&conn, "hash-jpg", false);
        let png = insert_object(&conn, "hash-png", false);
        insert_source(&conn, root, "a.jpg", Some(jpg));
        insert_source(&conn, root, "b.png", Some(png));

        let manifest_path = dir.join("cluster.toml");
        let lock_path = dir.join("cluster.lock");
        fs::write(&manifest_path, manifest_text("source.ext=jpg")).unwrap();
        (Db::from_connection(conn), manifest_path, lock_path)
    }

    fn run_refresh(db: &mut Db, manifest_path: &Path, editor: Option<EditorPass>) -> Result<()> {
        refresh_with_editor(
            db,
            manifest_path,
            false,
            editor,
            "canon cluster refresh",
            &LedgerConfig::default(),
            true,
        )
    }

    /// The manifest text a refresh reads, with `meta.scope` written in. The
    /// generated shape carries no scope key when there is none, so this is the
    /// same document `manifest_text` produces with the field a user would have
    /// added by hand.
    ///
    /// Quoted as TOML rather than through Rust's `Debug`, which escapes a
    /// combining accent to `\u{301}` — a six-character sequence TOML rejects,
    /// and one no editor would ever produce. The paths here carry no quote or
    /// backslash, so plain quoting is the faithful spelling.
    fn manifest_text_scoped(query: &str, scope: &[String]) -> String {
        let quoted: Vec<String> = scope.iter().map(|p| format!("\"{p}\"")).collect();
        manifest_text(query).replace(
            "generated_at = ",
            &format!("scope = [{}]\ngenerated_at = ", quoted.join(", ")),
        )
    }

    /// A `ManifestStatus` carrying `pending` entries still to transfer and
    /// `at_destination` already there, plus whatever scope prefixes failed to
    /// resolve — and `source_lost` entries when a case needs the accounting
    /// claim to be false.
    ///
    /// The entry list is built to match the counts rather than left empty:
    /// in production every count is derived from `entries`, so a fixture whose
    /// two halves disagree is a state that cannot occur, and it would go on
    /// passing silently the day this writer starts reading entries.
    fn status_with(
        pending: usize,
        at_destination: usize,
        source_lost: usize,
        cannot_apply: bool,
    ) -> status_ops::ManifestStatus {
        let entry = |i: usize, status: status_ops::EntryStatus| status_ops::StatusEntry {
            source_path: format!("/photos/f{i}.jpg"),
            source_filename: format!("f{i}.jpg"),
            source_exists: !matches!(status, status_ops::EntryStatus::SourceLost),
            dest_path: format!("/archive/f{i}.jpg"),
            db_registered: matches!(status, status_ops::EntryStatus::AtDestination),
            status,
        };
        let entries: Vec<status_ops::StatusEntry> =
            std::iter::repeat_n(status_ops::EntryStatus::Pending, pending)
                .chain(std::iter::repeat_n(
                    status_ops::EntryStatus::AtDestination,
                    at_destination,
                ))
                .chain(std::iter::repeat_n(
                    status_ops::EntryStatus::SourceLost,
                    source_lost,
                ))
                .enumerate()
                .map(|(i, st)| entry(i, st))
                .collect();

        status_ops::ManifestStatus {
            manifest_path: "m.toml".to_string(),
            dest_display: "/archive".to_string(),
            pattern: "{filename}".to_string(),
            lock_entry_count: entries.len(),
            lock_hash_valid: true,
            entries,
            at_destination,
            pending,
            source_lost,
            size_mismatch: 0,
            source_still_present: 0,
            unrooted_scope: Vec::new(),
            set_aside_scope: Vec::new(),
            lock_predates_measurement: cannot_apply,
            pattern_unexpandable: false,
            expansion_failures: Vec::new(),
            expansion_failure_count: 0,
        }
    }

    fn next_step(status: &status_ops::ManifestStatus) -> String {
        let mut out = Vec::new();
        write_next_step(&mut out, status, Path::new("m.toml"));
        String::from_utf8(out).unwrap()
    }

    /// F2 — a report must not name the one command that cannot run. The state
    /// that blocks `apply` moved with this story: it is no longer an
    /// unresolvable scope line but a lock written before the measurement was
    /// recorded in it, which `apply` refuses plain and `--resume` alike. The
    /// finding is unchanged; only the blocking state is.
    ///
    /// The counts are deliberately the ones that *do* earn an apply hint,
    /// because that is the whole finding: the accounting claim is about source
    /// files and stays true, so it cannot be what decides whether applying is
    /// possible.
    #[test]
    fn a_lock_apply_would_refuse_never_points_at_apply() {
        assert_eq!(
            next_step(&status_with(2, 0, 0, true)),
            "\nTo rebuild the lock: canon cluster refresh m.toml\n"
        );
        // The same, where the run would otherwise be told to resume.
        assert_eq!(
            next_step(&status_with(1, 1, 0, true)),
            "\nTo rebuild the lock: canon cluster refresh m.toml\n"
        );
    }

    /// The other side of the same branch: a lock `apply` can actually run
    /// against still gets the hint it always got, in both its arms.
    #[test]
    fn a_lock_apply_can_run_still_points_at_apply() {
        assert_eq!(
            next_step(&status_with(2, 0, 0, false)),
            "\nTo apply: canon apply m.toml\n"
        );
        assert_eq!(
            next_step(&status_with(1, 1, 0, false)),
            "\nTo complete: canon apply --resume m.toml\n"
        );
    }

    /// F2c — an unscoped manifest whose pattern was edited to name
    /// `{scope.rel_path}` cannot expand, so `apply` aborts on it — and the
    /// lock is a current one, so the lock's own age does not see this. A
    /// report that named the apply here would be naming a command that cannot
    /// run, which is the standing finding this branch exists for.
    ///
    /// And there is no *other* command to name in its place: a refresh does
    /// not give a pattern the fact or the scope it is missing, so offering one
    /// would be the same fault in a different word. The rows above already say
    /// what failed for each source.
    #[test]
    fn a_pattern_that_cannot_expand_offers_no_step_at_all() {
        for (pending, at_destination) in [(2, 0), (1, 1)] {
            let mut status = status_with(pending, at_destination, 0, false);
            status.pattern_unexpandable = true;
            assert_eq!(next_step(&status), "", "pending={pending}");
        }

        // But a lock that predates the measurement still gets its rebuild,
        // even though its entries also fail to expand — there the refresh is
        // exactly the fix.
        let mut status = status_with(2, 0, 0, true);
        status.pattern_unexpandable = true;
        assert_eq!(
            next_step(&status),
            "\nTo rebuild the lock: canon cluster refresh m.toml\n"
        );
    }

    /// F2b — and the state that no longer blocks must no longer divert. A
    /// scope line resolving to nothing leaves the lock correct: it was
    /// measured and recorded without that line, so applying is exactly right,
    /// and sending the user to a refresh that preserves the line verbatim
    /// would only loop them. The line is stated above the hint, not instead
    /// of it.
    #[test]
    fn a_scope_line_that_resolves_to_nothing_still_points_at_apply() {
        let mut status = status_with(2, 0, 0, false);
        status.unrooted_scope = vec!["/canon-test/no-such-root".to_string()];
        status.set_aside_scope = vec!["/photos/nothing-here".to_string()];
        assert_eq!(next_step(&status), "\nTo apply: canon apply m.toml\n");
    }

    /// **Whether** there is a next step is in no case the lock's business.
    /// A manifest with nothing pending has no step to take, and one with
    /// content missing has just been told to check the volume before
    /// refreshing — so a lock `apply` would refuse must not conjure advice
    /// where there is none, least of all advice that clears the lock.
    ///
    /// The six states that must stay silent, each reachable: the blocking
    /// state is the easy thing to key this decision on, and keying it there is
    /// wrong in every one of them.
    #[test]
    fn a_lock_apply_would_refuse_conjures_no_step_where_there_was_none() {
        for status in [
            status_with(0, 2, 0, true), // all applied; nothing to do
            status_with(0, 0, 0, true), // an empty lock
            status_with(0, 0, 2, true), // sources lost, no rebuild needed
            status_with(2, 0, 1, true), // pending, but content is missing
            status_with(2, 1, 1, true), // the same, part-applied
            status_with(0, 1, 1, true), // part-applied and part-lost
        ] {
            assert_eq!(
                next_step(&status),
                "",
                "pending={} at_destination={} source_lost={}",
                status.pending,
                status.at_destination,
                status.source_lost
            );
        }
    }

    /// C4 — a refresh is not the place to refuse: it narrows a lock, and it is
    /// the way back from apply's refusal, so a manifest naming a place under no
    /// known root must still be refreshable. It says so and keeps going —
    /// manifest rewritten, lock rewritten.
    #[test]
    fn a_refresh_states_an_unrooted_scope_and_keeps_going() {
        let dir = tempfile::tempdir().unwrap();
        let (mut db, manifest_path, lock_path) = setup(dir.path());
        fs::write(
            &manifest_path,
            manifest_text_scoped(
                "source.ext=jpg",
                &[
                    "/photos".to_string(),
                    "/canon-test/no-such-root".to_string(),
                ],
            ),
        )
        .unwrap();

        run_refresh(&mut db, &manifest_path, None).unwrap();

        let lock = fs::read_to_string(&lock_path).unwrap();
        assert!(
            lock.contains("a.jpg"),
            "the lock was still rewritten: {lock}"
        );
        let manifest = fs::read_to_string(&manifest_path).unwrap();
        assert!(
            manifest.contains("/canon-test/no-such-root"),
            "the prefix is preserved rather than dropped: {manifest}"
        );

        // The statement's own spelling, pinned where it is composed: a run
        // writes it to stderr, which an in-process test cannot read back.
        // Only the wording is pinned here — that the line carries the path it
        // was handed is a property of `writeln!`, not of this code.
        let mut said = Vec::new();
        write_unrooted_scope(&mut said, &["/canon-test/no-such-root".to_string()]);
        let said = String::from_utf8(said).unwrap();
        assert_eq!(
            said,
            "no known root at /canon-test/no-such-root — kept in the manifest, \
             no destination measures from it\n"
        );
    }

    /// C5 — the healing half, and the reason nothing in the system used to fix
    /// a mismatched prefix on its own: a refresh preserved whatever the file
    /// held, indefinitely. A rooted prefix is now written back in the byte-form
    /// the index stores — converging on what generation records for the same
    /// input — and an unrooted one verbatim, because there is nothing to heal
    /// it to and rewriting it would be inventing a place.
    #[test]
    fn a_refresh_writes_the_resolved_form_back() {
        // A root whose stored form is decomposed, named in a manifest the
        // precomposed way. The two must differ or this proves nothing.
        const DECOMPOSED: &str = "/photos/cafe\u{301}";
        const PRECOMPOSED: &str = "/photos/caf\u{e9}";
        assert_ne!(DECOMPOSED, PRECOMPOSED);

        let dir = tempfile::tempdir().unwrap();
        let conn = setup_test_db();
        let root = insert_root(&conn, DECOMPOSED, "source", false);
        let jpg = insert_object(&conn, "hash-jpg", false);
        insert_source(&conn, root, "trip/a.jpg", Some(jpg));

        let manifest_path = dir.path().join("cluster.toml");
        fs::write(
            &manifest_path,
            manifest_text_scoped(
                "source.ext=jpg",
                &[
                    format!("{PRECOMPOSED}/trip"),
                    "/canon-test/no-such-root".to_string(),
                ],
            ),
        )
        .unwrap();

        let mut db = Db::from_connection(conn);
        run_refresh(&mut db, &manifest_path, None).unwrap();

        let manifest = fs::read_to_string(&manifest_path).unwrap();
        assert!(
            manifest.contains(&format!("{DECOMPOSED}/trip")),
            "the rooted prefix must come back in the stored form: {manifest:?}"
        );
        assert!(
            !manifest.contains(&format!("{PRECOMPOSED}/trip")),
            "the typed form must not survive the rewrite: {manifest:?}"
        );
        assert!(
            manifest.contains("/canon-test/no-such-root"),
            "an unrooted prefix has nothing to heal to and stays verbatim: {manifest:?}"
        );
    }

    /// C6 — the defect's own shape, end to end at the refresh: an **ASCII**
    /// root with an accented folder under it, and a manifest naming two
    /// siblings below that folder in two normalizations. Root attribution
    /// alone cannot see this — the root matches as typed, so nothing is ever
    /// bent — and the below-root retry is what heals both lines onto the
    /// stored spelling.
    ///
    /// Both consequences are pinned: the lock keeps **both** sources (before,
    /// the mismatched line matched nothing and the lock silently narrowed),
    /// and the manifest text comes back healed, so the file repairs itself.
    #[test]
    fn a_refresh_heals_a_below_root_form_mismatch() {
        const NFD: &str = "cafe\u{301}";
        const NFC: &str = "caf\u{e9}";
        assert_ne!(NFD, NFC);

        let dir = tempfile::tempdir().unwrap();
        let conn = setup_test_db();
        // An ASCII root: the exposure this closes, and the shape a Mac home
        // path or volume always has.
        let root = insert_root(&conn, "/photos", "source", false);
        let a = insert_object(&conn, "hash-a", false);
        let b = insert_object(&conn, "hash-b", false);
        insert_source(&conn, root, &format!("{NFD}/sub1/a.jpg"), Some(a));
        insert_source(&conn, root, &format!("{NFD}/sub2/b.jpg"), Some(b));

        let manifest_path = dir.path().join("cluster.toml");
        fs::write(
            &manifest_path,
            manifest_text_scoped(
                "source.ext=jpg",
                &[format!("/photos/{NFC}/sub1"), format!("/photos/{NFD}/sub2")],
            ),
        )
        .unwrap();

        let mut db = Db::from_connection(conn);
        run_refresh(&mut db, &manifest_path, None).unwrap();

        let lock = fs::read_to_string(dir.path().join("cluster.lock")).unwrap();
        assert!(
            lock.contains("a.jpg"),
            "the retyped line still selects: {lock}"
        );
        assert!(lock.contains("b.jpg"), "its sibling still selects: {lock}");

        let manifest = fs::read_to_string(&manifest_path).unwrap();
        assert!(
            manifest.contains(&format!("/photos/{NFD}/sub1")),
            "the retyped line must be written back healed: {manifest:?}"
        );
        assert!(
            !manifest.contains(&format!("/photos/{NFC}/sub1")),
            "the typed form must not survive the rewrite: {manifest:?}"
        );
    }

    /// C7 — the source-existence policy at the second door. A recorded prefix
    /// the index knows nothing under is set aside and stated; the sibling that
    /// *is* known proceeds, and the set-aside line is still in the file
    /// afterwards, because a refresh must not narrow the user's own manifest
    /// on their behalf.
    ///
    /// The load-bearing assertion is the **decision record**: a set-aside line
    /// never becomes a recorded scope — the manifest door's twin of
    /// `a_set_aside_never_becomes_a_decision_scope` at the argument door. The
    /// lock and the file are the visible halves; this is the durable one.
    #[test]
    fn a_refresh_sets_a_sourceless_scope_line_aside_and_keeps_going() {
        let dir = tempfile::tempdir().unwrap();
        let (mut db, manifest_path, lock_path) = setup(dir.path());
        fs::write(
            &manifest_path,
            manifest_text_scoped(
                "source.ext=jpg",
                &["/photos".to_string(), "/photos/nothing-here".to_string()],
            ),
        )
        .unwrap();

        run_refresh(&mut db, &manifest_path, None).unwrap();

        let recorded: Vec<String> = db
            .conn_mut()
            .prepare("SELECT rel_prefix FROM decision_scopes ORDER BY rel_prefix")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap();
        assert_eq!(
            recorded,
            [""],
            "only the confirmed line becomes a recorded scope"
        );

        let lock = fs::read_to_string(&lock_path).unwrap();
        assert!(lock.contains("a.jpg"), "the known line still ran: {lock}");
        let manifest = fs::read_to_string(&manifest_path).unwrap();
        assert!(
            manifest.contains("/photos/nothing-here"),
            "the set-aside line stays in the user's file: {manifest:?}"
        );

        // The statement's own spelling, pinned where it is composed — the one
        // the scope boundary already uses, reached rather than re-spelled.
        // The channel is stdout, the same one `cluster generate` and
        // `cluster status` state it on; an in-process test cannot read either
        // stream back, so only the wording is pinned here.
        let mut said = Vec::new();
        crate::scope::write_set_asides(&mut said, &["/photos/nothing-here".to_string()]);
        assert_eq!(
            String::from_utf8(said).unwrap(),
            "no sources known at /photos/nothing-here — skipped\n"
        );
    }

    /// C6a — a scope prefix the index knows nothing under reaches
    /// `cluster generate`, and must be stated and set aside rather than
    /// assumed away.
    ///
    /// It arrives by the commonest route there is: `resolve_scope`'s **CWD**
    /// branch resolves the current directory and hands it over **without**
    /// the source-existence gate, because CWD defaulting is a context switch
    /// rather than an assertion about content. So `canon cluster generate
    /// --where …` run from a folder created since the last scan arrives here
    /// with exactly this. Assuming otherwise cost a panic in that invocation.
    ///
    /// This calls `generate` with the prefix directly, which is the same state
    /// that branch produces — and worth saying plainly: **no test in this
    /// module goes through `resolve_scope`**, so the argument door's own
    /// behaviour is not what is pinned here. What is pinned is that this
    /// command survives what that door can hand it.
    #[test]
    fn a_sourceless_scope_prefix_is_set_aside_rather_than_assumed_away() {
        let dir = tempfile::tempdir().unwrap();
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let jpg = insert_object(&conn, "hash-jpg", false);
        insert_source(&conn, root, "known/a.jpg", Some(jpg));
        insert_root(&conn, "/archive", "archive", false);
        let mut db = Db::from_connection(conn);

        let out = dir.path().join("cluster.toml");
        generate(
            &mut db,
            // Under a known root, and the index knows nothing beneath it.
            &["/photos/new-folder".to_string()],
            &[],
            &[],
            Path::new("/archive"),
            &out,
            &GenerateOptions {
                force: false,
                allow_archived: false,
                allow_duplicates: false,
                show_archived: false,
                no_edit: true,
            },
            "canon cluster generate",
            &LedgerConfig::default(),
            true,
        )
        .expect("a sourceless scope is a nothing-matched run, never a panic");

        assert!(
            !out.exists(),
            "nothing matched, so no manifest is written: {out:?}"
        );

        // And the skip is said, in the one spelling the scope boundary uses.
        let mut said = Vec::new();
        crate::scope::write_set_asides(&mut said, &["/photos/new-folder".to_string()]);
        assert_eq!(
            String::from_utf8(said).unwrap(),
            "no sources known at /photos/new-folder — skipped\n"
        );
    }

    /// C6b — selection and measurement read the same register, or they
    /// disagree about the same run.
    ///
    /// An unrooted prefix can be an **ancestor** of a known root:
    /// `path_is_under` matches it where `find_containing_root` does not. Select
    /// from the recorded list and such a line gathers sources no vantage can
    /// measure — a lock whose header is not empty and whose entries are not all
    /// measured, which is a state every reader downstream is entitled to assume
    /// away. The run would then refuse those files at apply, one by one, for a
    /// line it had already told the user measures nothing.
    #[test]
    fn a_line_that_measures_nothing_selects_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let conn = setup_test_db();
        // The root sits *below* the prefix the manifest also names, which is
        // what makes the unrooted line select rather than merely fail.
        let root = insert_root(&conn, "/vol/work", "source", false);
        let a = insert_object(&conn, "hash-a", false);
        let b = insert_object(&conn, "hash-b", false);
        insert_source(&conn, root, "proj/a.jpg", Some(a));
        insert_source(&conn, root, "other/b.jpg", Some(b));

        let manifest_path = dir.path().join("cluster.toml");
        fs::write(
            &manifest_path,
            manifest_text_scoped(
                "source.ext=jpg",
                &["/vol/work/proj".to_string(), "/vol".to_string()],
            ),
        )
        .unwrap();

        let mut db = Db::from_connection(conn);
        run_refresh(&mut db, &manifest_path, None).unwrap();

        let lock = fs::read_to_string(dir.path().join("cluster.lock")).unwrap();
        assert!(
            lock.contains("proj/a.jpg"),
            "the confirmed line selects: {lock}"
        );
        assert!(
            !lock.contains("other/b.jpg"),
            "the unrooted ancestor must select nothing: {lock}"
        );
        // The claim every reader rests on: no entry is left unmeasured beneath
        // a header that records a scope.
        for line in lock.lines().skip(1) {
            assert!(
                line.contains("\"scope_rel_path\""),
                "a header with a scope leaves no unmeasured entry: {line}"
            );
        }
        // And the line is still in the user's own file.
        let manifest = fs::read_to_string(&manifest_path).unwrap();
        assert!(manifest.contains("\"/vol\""), "{manifest:?}");
    }

    /// C6c — and a scope that confirmed **nothing** selects nothing rather
    /// than everything. An empty scope list means *global* to the planner, so
    /// the register that carries "no confirmed place" must not be spelled the
    /// same way as "no scope was ever recorded" — a manifest naming a drive
    /// that is not plugged in would otherwise archive the whole universe.
    #[test]
    fn a_scope_that_confirmed_nothing_selects_nothing_rather_than_everything() {
        let dir = tempfile::tempdir().unwrap();
        let (mut db, manifest_path, lock_path) = setup(dir.path());
        fs::write(
            &manifest_path,
            manifest_text_scoped("source.ext=jpg", &["/canon-test/gone".to_string()]),
        )
        .unwrap();

        run_refresh(&mut db, &manifest_path, None).unwrap();

        assert!(
            !lock_path.exists(),
            "the query matched nothing, so the lock goes — never a global sweep"
        );
    }

    /// C8 — the policy's terminal rule at the second door: a scope that kept
    /// nothing must never look like a narrowing, so a refresh whose every
    /// rooted line was set aside refuses, naming every one of them, with
    /// neither file touched.
    #[test]
    fn a_refresh_whose_scope_kept_nothing_refuses_naming_every_line() {
        let dir = tempfile::tempdir().unwrap();
        let (mut db, manifest_path, lock_path) = setup(dir.path());
        let before = manifest_text_scoped(
            "source.ext=jpg",
            &[
                "/photos/nothing-here".to_string(),
                "/photos/nor-here".to_string(),
            ],
        );
        fs::write(&manifest_path, &before).unwrap();

        let err = run_refresh(&mut db, &manifest_path, None)
            .unwrap_err()
            .to_string();
        // The refusal names every skipped path itself. That it is not *also*
        // stated a line above is a property of ordering, not of this string —
        // the per-line statement goes to stdout and never into the error — so
        // what is pinned here is the refusal's own content, and the ordering
        // is pinned by `an_unrooted_line_is_stated_even_when_the_refresh_refuses`
        // below, which is the case the ordering can actually get wrong.
        assert!(err.contains("/photos/nothing-here"), "{err}");
        assert!(
            err.contains("/photos/nor-here"),
            "every line is named: {err}"
        );
        assert!(err.contains("unchanged"), "{err}");

        assert_eq!(
            fs::read_to_string(&manifest_path).unwrap(),
            before,
            "the manifest is untouched"
        );
        assert!(!lock_path.exists(), "no lock was written");
    }

    /// C8b — an unrooted line is a fact about the manifest, and the run that
    /// had least to work with must not be the one that goes unsaid.
    ///
    /// The terminal refusal names only the **set-asides**, so gating the
    /// statements on the run continuing would leave a manifest holding both an
    /// unrooted line and a sourceless one refusing while never mentioning the
    /// unrooted one at all — silence at exactly the boundary the honesty
    /// policy exists for.
    ///
    /// Asserted over `scope_statements`' own output rather than over the
    /// stream: `println!` cannot be captured in-process, so a test reading the
    /// error string would be asserting something it cannot see. The ordering
    /// is the decision, so the ordering is the value.
    #[test]
    fn an_unrooted_line_is_stated_even_when_the_refresh_refuses() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root, "a.jpg", None);
        let roots = crate::core::repo::root::fetch_all(&conn).unwrap();
        let scope = resolve_recorded_scope(
            &conn,
            &[
                "/canon-test/no-such-root".to_string(),
                "/photos/nothing-here".to_string(),
            ],
            &roots,
        )
        .unwrap();

        assert!(
            refresh_must_refuse(&scope),
            "the fixture must reach the refusing branch, or this proves nothing"
        );
        let said = String::from_utf8(scope_statements(&scope, true)).unwrap();
        assert_eq!(
            said,
            "no known root at /canon-test/no-such-root — kept in the manifest, \
             no destination measures from it\n",
            "the unrooted line is stated; the set-aside is left to the refusal"
        );

        // And when the run continues, both are said.
        let scope = resolve_recorded_scope(
            &conn,
            &[
                "/canon-test/no-such-root".to_string(),
                "/photos/nothing-here".to_string(),
                "/photos".to_string(),
            ],
            &roots,
        )
        .unwrap();
        assert!(!refresh_must_refuse(&scope));
        let said = String::from_utf8(scope_statements(&scope, false)).unwrap();
        assert!(
            said.contains("no known root at /canon-test/no-such-root"),
            "{said}"
        );
        assert!(
            said.contains("no sources known at /photos/nothing-here — skipped"),
            "{said}"
        );
    }

    /// C9 — and the case that is *not* the terminal rule: a scope naming only
    /// places under no known root keeps nothing either, and still proceeds.
    /// The two are different answers — one is a place Canon cannot confirm,
    /// the other a place Canon has never heard of — and a refresh is the way
    /// back from the second, so refusing it would strand the user.
    #[test]
    fn an_all_unrooted_scope_is_not_the_terminal_rule() {
        let dir = tempfile::tempdir().unwrap();
        let (mut db, manifest_path, _lock_path) = setup(dir.path());
        fs::write(
            &manifest_path,
            manifest_text_scoped("source.ext=jpg", &["/canon-test/no-such-root".to_string()]),
        )
        .unwrap();

        run_refresh(&mut db, &manifest_path, None).expect("a refresh must stay reachable");
    }

    #[test]
    fn refresh_edit_consumes_the_saved_query() {
        let dir = tempfile::tempdir().unwrap();
        let (mut db, manifest_path, lock_path) = setup(dir.path());

        // The user's pass runs first, so the re-query is the edited one —
        // that is the whole reason to edit a query before refreshing.
        let edit = |p: &Path| -> Result<()> {
            fs::write(p, manifest_text("source.ext=png"))?;
            Ok(())
        };
        run_refresh(&mut db, &manifest_path, Some(&edit)).unwrap();

        let lock = fs::read_to_string(&lock_path).unwrap();
        assert!(lock.contains("b.png"), "got: {lock}");
        assert!(
            !lock.contains("a.jpg"),
            "the pre-edit query was queried: {lock}"
        );

        let manifest = fs::read_to_string(&manifest_path).unwrap();
        assert!(manifest.contains("source.ext=png"), "got: {manifest}");
        assert!(manifest.contains("# keep these words"), "got: {manifest}");
    }

    #[test]
    fn refresh_abort_on_parse_failure_touches_neither_manifest_nor_lock() {
        let dir = tempfile::tempdir().unwrap();
        let (mut db, manifest_path, lock_path) = setup(dir.path());
        fs::write(&lock_path, "the lock from last time\n").unwrap();

        let edit = |p: &Path| -> Result<()> {
            fs::write(p, "[meta\nversion = 1\n")?;
            Ok(())
        };
        let err = run_refresh(&mut db, &manifest_path, Some(&edit)).unwrap_err();
        assert!(
            format!("{err:#}").contains("Failed to parse config"),
            "{err:#}"
        );

        // The user's file holds exactly what they saved — Canon wrote nothing
        // over it — and the lock still describes the last good query.
        assert_eq!(
            fs::read_to_string(&manifest_path).unwrap(),
            "[meta\nversion = 1\n"
        );
        assert_eq!(
            fs::read_to_string(&lock_path).unwrap(),
            "the lock from last time\n"
        );
    }

    #[test]
    fn refresh_abort_on_editor_failure_touches_neither_file() {
        let dir = tempfile::tempdir().unwrap();
        let (mut db, manifest_path, lock_path) = setup(dir.path());
        fs::write(&lock_path, "the lock from last time\n").unwrap();

        let edit = |_: &Path| -> Result<()> { bail!("editor 'false' exited with status 1") };
        let err = run_refresh(&mut db, &manifest_path, Some(&edit)).unwrap_err();
        let rendered = format!("{err:#}");
        assert!(rendered.contains("Refresh aborted"), "{rendered}");
        assert!(rendered.contains("editor 'false'"), "{rendered}");

        assert_eq!(
            fs::read_to_string(&manifest_path).unwrap(),
            manifest_text("source.ext=jpg")
        );
        assert_eq!(
            fs::read_to_string(&lock_path).unwrap(),
            "the lock from last time\n"
        );
    }

    // =========================================================================
    // The write-time collision warning
    // =========================================================================

    /// Driven with exactly the arguments `generate` passes: the destination
    /// the user named as the placement root, the manifest and its lock as the
    /// files being written.
    ///
    /// Generation cannot reach this state today: it writes the default
    /// pattern, and both defaults commit to no directory below the
    /// destination, so nothing the manifest could be named would stand in the
    /// way. The call site is wired all the same, and this holds it to its
    /// behaviour for the day a chosen pattern reaches generation.
    #[test]
    fn generate_warns_when_the_manifest_stands_in_the_placement_path() {
        let dest = Path::new("/archive/photos");
        let manifest = Path::new("/archive/photos/2024");
        let lock = Path::new("/archive/photos/2024.lock");

        let warning = placement_warning("2024/{filename}", dest, &[manifest, lock])
            .expect("the manifest occupies the directory the pattern needs");
        assert!(warning.contains("/archive/photos/2024"), "got: {warning}");
        assert!(warning.contains("edit the pattern"), "got: {warning}");

        // The lock file blocks just as well as the manifest does.
        let lock_blocker = Path::new("/archive/photos/2024");
        assert!(
            placement_warning("2024/{filename}", dest, &[lock_blocker]).is_some(),
            "a lock file standing in the path is the same collision"
        );
    }

    /// The refresh twin: the placement root is the manifest's own archive root
    /// joined with its recorded base_dir, and the pattern is the one the
    /// manifest carries — the prefix an edit added is exactly what makes the
    /// collision knowable at this moment.
    #[test]
    fn refresh_warns_when_the_manifest_stands_in_the_placement_path() {
        let placement_root = Path::new("/archive").join("photos");
        let manifest = Path::new("/archive/photos/2024");

        assert!(
            placement_warning("2024/{filename}", &placement_root, &[manifest]).is_some(),
            "the edited prefix put the manifest in the placement path"
        );
        // Before the edit added the prefix there was nothing to warn about.
        assert!(
            placement_warning("{filename}", &placement_root, &[manifest]).is_none(),
            "a flat pattern needs no directory below the destination"
        );
    }

    /// The shape this warning was written for: a manifest named after a folder
    /// whose name carries a dot, written into the archive with `-O`. The dot
    /// reads as an extension so no `.toml` is appended, the bare name lands in
    /// the destination, and the pattern the user then edits in names that same
    /// directory. Dotted folder names are exactly what people name a manifest
    /// after, which is why this coincidence is not the freak it looks like.
    #[test]
    fn the_dotted_manifest_name_that_collided_with_its_own_pattern_is_named() {
        let placement_root = Path::new("/Volumes/Archive").join("Projects");
        let manifest = Path::new("/Volumes/Archive/Projects/example.org");

        let warning =
            placement_warning("example.org/{scope.rel_path}", &placement_root, &[manifest])
                .expect("the manifest is the directory its own pattern needs");
        assert!(warning.contains("example.org"), "got: {warning}");
        assert!(
            warning.contains("Apply will refuse"),
            "the warning says what happens next: {warning}"
        );
    }

    /// The two sides arrive spelled differently — the manifest as typed, the
    /// destination from the archive root's stored canonical path. Comparing
    /// the two spellings without resolving them finds nothing, and finds it
    /// silently: a warning that cannot see the collision is indistinguishable
    /// from a warning with nothing to say. The symlink is built here rather
    /// than borrowed from the platform's temp path, so the guard holds
    /// wherever it runs.
    #[cfg(unix)]
    #[test]
    fn the_warning_sees_through_a_symlinked_path() {
        let dir = tempfile::tempdir().unwrap();
        let real = fs::canonicalize(dir.path()).unwrap().join("archive");
        fs::create_dir(&real).unwrap();
        let linked = fs::canonicalize(dir.path()).unwrap().join("link");
        std::os::unix::fs::symlink(&real, &linked).unwrap();

        // The manifest as the user typed it, through the link; the
        // destination as the database stored it, resolved.
        let manifest = linked.join("2024.toml");
        fs::write(&manifest, "").unwrap();
        assert_ne!(manifest, real.join("2024.toml"), "the two spellings differ");
        assert!(
            placement_warning("2024.toml/{filename}", &real, &[&manifest]).is_some(),
            "the same place spelled two ways must still compare equal"
        );
    }

    #[test]
    fn a_manifest_beside_the_placement_path_draws_no_warning() {
        let dest = Path::new("/archive/photos");
        assert!(
            placement_warning(
                "2024/{filename}",
                dest,
                &[Path::new("/archive/photos/manifest.toml")]
            )
            .is_none(),
            "an ordinary manifest beside the destination blocks nothing"
        );
    }

    #[test]
    fn a_file_standing_above_the_placement_path_is_caught_too() {
        // The chain is checked, not just its last component: a file at
        // /archive/photos blocks /archive/photos/2024/raw just as thoroughly.
        let dest = Path::new("/archive");
        assert!(
            placement_warning(
                "photos/2024/raw/{filename}",
                dest,
                &[Path::new("/archive/photos")]
            )
            .is_some(),
            "a blocker anywhere on the chain stops the directories below it"
        );
    }

    #[test]
    fn refresh_without_edit_never_opens_an_editor() {
        let dir = tempfile::tempdir().unwrap();
        let (mut db, manifest_path, lock_path) = setup(dir.path());

        run_refresh(&mut db, &manifest_path, None).unwrap();

        let lock = fs::read_to_string(&lock_path).unwrap();
        assert!(lock.contains("a.jpg"), "got: {lock}");
    }
}
