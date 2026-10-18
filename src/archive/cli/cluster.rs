use anyhow::{bail, Context, Result};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::archive::domain::{
    parse_manifest_allow, parse_manifest_config, settle_manifest_zone, LockHeaderStanding,
    LockStanding, ManifestConfig, ManifestZone, NextStep, OwedWayBack, RefreshRefusal,
};
use crate::archive::ops::generate::{
    ClusterGenerateParams, ClusterGeneratePlan, ExecuteGenerateParams, ExecuteRefreshParams,
};
use crate::archive::ops::manifest::read_lock_zone_tolerantly;
use crate::archive::ops::manifest::{parse_recorded_query, LockZone};
use crate::archive::ops::pattern::PatternGaps;
use crate::archive::ops::{generate as generate_ops, status as status_ops};
use crate::core::domain::config::{LedgerConfig, RecordingMode};
use crate::core::domain::decision::{DecisionCommand, DecisionStatus};
use crate::core::domain::format::first_chars;
use crate::core::domain::format_count;
use crate::core::domain::root::{DoorVerb, ParkedRoot, RefusedAct, RefusedActs};
use crate::core::domain::scope::ScopeResolution;
use crate::core::domain::time::zone::ReadTimesOn;
use crate::core::domain::time::zone::{parse_zone_name, RecordedZone, UserZone};
use crate::core::ops::decision::{
    DecisionCounts, DecisionParams, DecisionRecorder, UnwindTerminal,
};
use crate::core::ops::scope::{
    classify_all, no_sources_known, resolve_archive_path, resolve_recorded_scope,
};
use crate::core::repo::{self, Connection, Db};
use crate::expr::read_filters;

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
    machine: &UserZone,
) -> Result<()> {
    // Prevent overwriting existing TOML config (unless --force)
    if output_path.exists() && !options.force {
        bail!(
            "Output file '{}' already exists.\n\
             Use `cluster refresh` to update the lock file, or -f/--force to overwrite.",
            output_path.display()
        );
    }

    // The zone the manifest will record, and the clock the query reads on: this machine's,
    // where it can be written down. Refused before anything is selected or written — a
    // manifest that could not say which clock it was made on would read times in a different
    // one at its next refresh.
    let zone = machine
        .recordable()
        .map_err(|u| anyhow::anyhow!(super::generate_machine_zone_refusal(&u)))?;

    // Require at least one of path scope or filters
    if scope_prefixes.is_empty() && expanded_filters.is_empty() {
        bail!("At least one of path or --where filter is required");
    }

    let conn = db.conn_mut();

    // Fetch all roots for archive path resolution
    let all_roots = repo::root::fetch_all(conn)?;

    // Resolve destination to archive root + relative subdir
    let (archive_root_id, _archive_root_path, base_dir) = resolve_archive_path(&all_roots, dest)?;

    let parsed_filters = read_filters(conn, expanded_filters)?;

    // The scope this run settles on, through the very code a manifest's own
    // scope goes through: generate, refresh and apply resolve scope through
    // exactly the same code.
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
    // same answer — which is the same-code rule reaching the arm that is
    // easiest to believe cannot happen.
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
                clock: zone.clock().clone(),
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
        zone: zone.clone(),
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
        zone: ReadTimesOn::of_manifest(&zone),
    };
    // The receipt context is None on purpose, and that — not the flag
    // computed above — is what stops a receipt file being written. Generating
    // a manifest performs no transition on any source; the manifest itself is
    // the artifact the run leaves behind.
    //
    // Armed: the window is `execute_generate`, which writes exactly these two
    // files, and "what is on disk now?" is the user's next question. Both
    // paths are in hand here, so the sentence carries them.
    let mut recorder = DecisionRecorder::start(conn, &decision, None).armed(
        conn,
        UnwindTerminal {
            status: DecisionStatus::Interrupted,
            summary: format!(
                "Manifest generation ended early; check {} and {} on disk",
                output_path.display(),
                lock_path.display()
            ),
        },
    );

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
    println!("{}", super::zone_recorded_statement(&zone));

    warn_if_written_file_blocks_placement(&result.pattern, dest, &[output_path, &lock_path]);

    if !options.no_edit {
        if let Err(e) = open_editor(output_path) {
            eprintln!("Warning: {e}");
        }
    }

    eprintln!("\nTo apply: {}", super::apply_command(output_path, false));

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
    let config = parse_manifest_config(content, config_path)?;
    let (allow_archived, allow_duplicates) = parse_manifest_allow(&config.options.allow)?;
    Ok((config, allow_archived, allow_duplicates))
}

#[allow(clippy::too_many_arguments)]
pub fn refresh(
    db: &mut Db,
    config_path: &Path,
    show_archived: bool,
    edit: bool,
    command_line: &str,
    ledger: &LedgerConfig,
    no_receipt: bool,
    machine: &UserZone,
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
        machine,
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
    machine: &UserZone,
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

    let lock_path = config_path.with_extension("lock");

    // The door, asked before the plan and before anything is written — a
    // refresh rewrites the manifest and clears the lock, so a check that ran
    // after the selection would be reading a world it had already changed.
    let doors = parked_doors(&scope);

    // Both statements go out on the honesty policy's own position for an
    // effectful command: before any plan display and before any confirmation.
    // On **stdout**, which is that policy's channel for an effectful command
    // (`scope::print_scope_set_asides`) and the channel `cluster generate` and
    // `cluster status` already use for the same sentences — the skip and the
    // outcome it qualifies belong on one stream. What is said, and in which
    // order, is `scope_statements`' decision; this only prints it.
    let refusing = doors.is_empty() && scope.kept_nothing();
    {
        use std::io::Write as _;
        let _ = std::io::stdout()
            .lock()
            .write_all(&scope_statements(&scope, &doors, refusing));
    }

    // Every other cause a refresh refuses on is collected before any is
    // answered, so one run reports them all: the manifest's zone, settled from
    // its line or this machine's, and every cause reading and binding refuse the query for.
    // Nothing reads the zone or the query before this point, and nothing is
    // written until every cause is clear.
    let settled_zone = settle_manifest_zone(config.meta.zone.as_deref(), machine);
    let recorded_query = parse_recorded_query(conn, &config.meta.query)?;
    let mut causes: Vec<String> = Vec::new();
    if let Err(refusal) = &settled_zone {
        causes.push(super::zone_refusal(refusal));
    }
    if let Err(refusal) = &recorded_query {
        causes.push(super::query_refusal_block(refusal, config_path));
    }
    let aborted = format!(
        "Refresh aborted — {} and its lock file are unchanged.",
        config_path.display()
    );

    // A parked scope line refuses the **whole** refresh, and the lock is
    // exactly where it was — including a line standing beside live ones. The
    // argument door's set-aside does not conjugate here: an argument is a
    // one-off invocation, while a manifest's scope is a durable declaration of
    // what collection is being assembled, so a lock rebuilt from half of one
    // is a wrong artifact that reads as whole, and the apply after it would
    // archive a partial collection under a manifest claiming the full one.
    // Exit 1 with no `Error:` prefix — nothing went wrong; the user closed the
    // door — and no decision row, because nothing was done.
    //
    // The door's own lines went out above; the other causes follow them on
    // the same stream, then the abort once.
    if !doors.is_empty() {
        for cause in &causes {
            println!("{cause}");
        }
        println!("{aborted}");
        std::process::exit(1);
    }

    // This refusal is itself a legitimate answer — the recorded scope named
    // places Canon knows no sources under, and saying so *is* the answer — so
    // by the root register's rule it should exit 1 without the `Error:` prefix
    // a raised error wears, exactly as the door arm above it does. It does not
    // yet. The door arm gets there by calling `std::process::exit` from inside
    // this function, which no test can reach without dying with it; the shape
    // that works is a typed refusal returned to `main`, as `survey` and
    // `trail` do. That mints a type and changes a pinned barrel, so it is
    // recorded rather than done here.
    if refusing {
        let mut lines = vec![no_sources_known(scope.set_aside())];
        lines.extend(causes);
        lines.push(aborted);
        lines.extend(super::scan_or_edit_scope(config_path, scope.set_aside()));
        bail!("{}", lines.join("\n"));
    }
    let manifest_zone = match settled_zone {
        Ok(zone) if causes.is_empty() => zone,
        _ => {
            causes.push(aborted);
            bail!("{}", causes.join("\n"));
        }
    };
    let zone = manifest_zone.zone().clone();
    // A query that cannot run was refused above with every cause; it is never selected on.
    let Ok(parsed_filters) = recorded_query else {
        bail!("the recorded query cannot run");
    };

    // The old lock's zone, read before anything is written — tolerantly: a lock
    // that is gone, headerless, zoneless or unreadable gives no comparison and
    // never a refusal.
    let old_lock_zone =
        read_lock_zone_tolerantly(&lock_path).and_then(|text| parse_zone_name(&text).ok());

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
                clock: zone.clock().clone(),
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

    // Execute (the lock's path was settled above, before the door was asked)

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
        zone: zone.clone(),
    };

    // A refresh whose query now matches nothing records nothing — the 0-item
    // convention, the same answer `cluster generate` gives. The lock is still
    // cleared and the manifest rewritten, because the declaration has to keep
    // telling the truth about its query; but no source was touched, and a 0/0
    // row in the trail would claim an act where there was none.
    if plan.lock_entries.is_empty() {
        generate_ops::execute_refresh(&mut plan, &exec_params)?;
        println!("No sources matched the query");
        let statements = refresh_zone_statements(
            &manifest_zone,
            old_lock_zone.as_ref(),
            machine,
            plan.query_read_an_instant,
            Some(&PatternGaps::default()),
        );
        for line in statements {
            println!("{line}");
        }
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
        zone: ReadTimesOn::of_manifest(&zone),
    };
    // The receipt context is None on purpose, and that — not the flag
    // computed above — is what stops a receipt file being written. Generating
    // a manifest performs no transition on any source; the manifest itself is
    // the artifact the run leaves behind.
    //
    // Armed: generation's shape mirrored — the window writes these two files,
    // and the sentence names them.
    let mut recorder = DecisionRecorder::start(conn, &decision, None).armed(
        conn,
        UnwindTerminal {
            status: DecisionStatus::Interrupted,
            summary: format!(
                "Refresh ended early; check {} and {} on disk",
                config_path.display(),
                lock_path.display()
            ),
        },
    );

    let result = generate_ops::execute_refresh(&mut plan, &exec_params)?;

    // Only an empty plan leaves no outcome, and that path returned above; a
    // missing one here means the lock was never written.
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

    // The decision is settled; the recorder's hold on the connection ends here.
    drop(recorder);

    // What the pattern meets over the lock just written, on the zone just
    // settled — read only for what the statements below say about it.
    // The refresh has run and is recorded; this pass only composes sentences, so a failure
    // here is said as a warning and never turns the finished refresh into an error.
    let statements = match generate_ops::pattern_gaps_for_written_lock(
        conn,
        &lock_path,
        &r.pattern,
        zone.clock(),
    ) {
        Ok(gaps) => refresh_zone_statements(
            &manifest_zone,
            old_lock_zone.as_ref(),
            machine,
            plan.query_read_an_instant,
            Some(&gaps),
        ),
        Err(e) => {
            eprintln!("{}", super::statement_pass_failed(&e));
            refresh_zone_statements(
                &manifest_zone,
                old_lock_zone.as_ref(),
                machine,
                plan.query_read_an_instant,
                None,
            )
        }
    };
    for line in statements {
        println!("{line}");
    }

    if let Some(root) = &placement_root {
        warn_if_written_file_blocks_placement(&r.pattern, root, &[config_path, &lock_path]);
    }

    Ok(())
}

/// What a refresh says about its zone once it has run, in order: a zone line it had to
/// record, a zone that differs from the one the old lock was built in, the clock it read
/// on where that is not this machine's, and the times too coarse for the pattern.
///
/// The clock is worth a sentence only where the run read an instant on it — the query's
/// filters or the pattern's placements; a clock reading reads the same on any clock. Every
/// comparison is between parsed zones, never text (`RecordedZone::same_zone`).
///
/// `gaps` is `None` where reading the written lock back failed: what the pattern placed is
/// then unknown, so no sentence that turns on it is said — the coarse values, and the clock
/// and the zone change's full form unless the query alone read an instant. A zone change is
/// still said, in its quiet clause, since it is read from the two zones and not from the
/// lock.
fn refresh_zone_statements(
    manifest_zone: &ManifestZone,
    old_lock_zone: Option<&RecordedZone>,
    machine: &UserZone,
    query_read_an_instant: bool,
    gaps: Option<&PatternGaps>,
) -> Vec<String> {
    let zone = manifest_zone.zone();
    let instant_read = query_read_an_instant || gaps.is_some_and(|g| g.used_an_instant);
    let mut lines = Vec::new();
    if let ManifestZone::FromMachine(recorded) = manifest_zone {
        lines.push(super::zone_line_recorded_statement(recorded));
    }
    if let Some(old) = old_lock_zone.filter(|old| !old.same_zone(zone)) {
        lines.push(super::zone_change_statement(old, zone, instant_read));
    }
    if instant_read && !zone.clock().reads_like(machine) {
        lines.push(super::manifest_clock_statement(zone, machine));
    }
    if let Some(gaps) = gaps {
        lines.extend(super::coarse_value_statements(gaps));
    }
    lines
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
        "{situation}\n  Apply will stop with an error until one of them moves: write the manifest \
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

/// Display plan warnings (archived files, keys of mixed type) to stderr.
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

    for line in mixed_key_lines(plan) {
        eprintln!("{line}");
    }
}

/// The statement naming the full-coverage keys that hold two types, and the way out — empty
/// when there are none.
fn mixed_key_lines(plan: &ClusterGeneratePlan) -> Vec<String> {
    use crate::expr::{mixed_type_statement, mixed_type_way_out};

    let mixed = plan.mixed_keys();
    let mut lines = mixed_type_statement(&mixed);
    lines.extend(mixed_type_way_out(&mixed));
    lines
}

/// The step that moves this manifest forward, or nothing when there is none.
///
/// **Whether** there is anything to say at all is this function's own
/// question, and the only one it is: files still to transfer, and nothing lost
/// or wrong. That gate is outermost on purpose. A manifest with nothing
/// pending has no next step to take, and one with content missing has just
/// been told to check the volume before refreshing — advising a refresh in
/// either state clears the lock and gains nothing, and in the second it
/// contradicts the line above it.
///
/// The gate's third arm — a destination of the wrong size — has no such
/// justification and is older than the table below. A report stopped there
/// names no act, and nothing above it has said one either; `apply --resume`
/// on the same manifest holds the look-before-deleting guidance. A standing
/// gap of the gate, stated here rather than closed, so that the table's claim
/// of always answering is read at its true reach: the table is never asked.
///
/// **Which** step it is is not decided here at all. It is
/// `archive::domain::ManifestStanding::next_step`'s, over the table of what
/// stops each act — which is where the rows are enumerated, where the
/// unreachable cells are named, and where the rule that a step is offered only
/// where it brings an apply within reach is stated. This function turns that
/// answer into the two words a user reads, and the one thing it still decides
/// is the apply's own wording: plain, or a resume where part of the lock has
/// already landed. That is a question about counts, not about acts.
///
/// **`NextStep::None` is not silence**, and which sentence it is instead is
/// the table's too. Where no act brings the apply within reach, the cause that
/// withheld it still owes the reader something, and only a closed door
/// discharges that by itself — its line carries `canon roots unsuspend`. What
/// the others owe is `owed_way_back`'s answer, read here and printed: this
/// function neither re-tests the causes nor ranks them.
fn write_next_step(
    handle: &mut impl Write,
    status: &status_ops::ManifestStatus,
    manifest_path: &Path,
) {
    if (status.pending == 0 && status.unplaced == 0) || !status.found_nothing_lost_or_wrong() {
        return;
    }
    match status.next_step() {
        NextStep::None => {
            let lines = match status.owed_way_back() {
                OwedWayBack::Stated => Vec::new(),
                OwedWayBack::ScanOrEditScope => {
                    super::scope_kept_nothing_way_back(manifest_path, &status.set_aside_scope)
                }
                OwedWayBack::AdjustPattern => super::adjust_pattern_remedy(&status.pattern_gaps),
            };
            if !lines.is_empty() {
                let _ = writeln!(handle);
                for line in lines {
                    let _ = writeln!(handle, "{line}");
                }
            }
        }
        NextStep::Rebuild => {
            let _ = writeln!(handle);
            let _ = writeln!(
                handle,
                "To rebuild the lock: {}",
                super::refresh_command(manifest_path, false)
            );
        }
        NextStep::Apply => {
            let _ = writeln!(handle);
            if status.at_destination > 0 {
                let _ = writeln!(
                    handle,
                    "To complete: {}",
                    super::apply_command(manifest_path, true)
                );
            } else {
                let _ = writeln!(
                    handle,
                    "To apply: {}",
                    super::apply_command(manifest_path, false)
                );
            }
        }
    }
}

/// The report's `Lock:` line, from the one shape that carries what the read
/// found.
///
/// **An absence is stated rather than failed on** — the report's whole job is
/// to say what state a manifest is in, and "there is no lock" is a state Canon
/// itself writes: a refresh whose query matched nothing takes the lock away
/// and records the empty hash. Which of the two absences it is comes from the
/// manifest's own record, so the sentence names a cause it can actually know,
/// and the two causes take opposite remedies at `apply`
/// (`cli.rs`’s `no_lock_remedy`) — a report naming the wrong one would send the
/// reader to the act that cannot help.
///
/// **A lock's age is stated on this line too.** `apply` refuses a lock
/// written before the measurement whatever the pattern says, and the report
/// used to say so only through the pattern — a per-entry reason that appears
/// only where the pattern reads the scope. Where it does not, the reader met
/// the refusal at `apply` for a cause the report never mentioned. The age is a
/// standing of the lock, so it belongs beside its count, in the words the
/// refusal will use (`a_locks_age_is_stated_on_its_own_line`).
fn lock_line(lock: LockStanding, entry_count: usize) -> String {
    match lock {
        LockStanding::Absent {
            manifest_names_a_hash: false,
        } => "Lock: none — the last refresh matched nothing.".to_string(),
        LockStanding::Absent {
            manifest_names_a_hash: true,
        } => "Lock: none — the manifest names a lock file that is not there.".to_string(),
        LockStanding::Read {
            header: LockHeaderStanding::Absent,
            ..
        } => format!(
            "Lock: {} entries — {}",
            format_count(entry_count),
            super::LOCK_PREDATES_MEASUREMENT
        ),
        LockStanding::Read {
            header: LockHeaderStanding::PredatesZone,
            ..
        } => format!(
            "Lock: {} entries — {}",
            format_count(entry_count),
            super::LOCK_PREDATES_ZONE
        ),
        // A zone this machine does not know is stated on the zone line, with its ways out.
        LockStanding::Read {
            header: LockHeaderStanding::Sound | LockHeaderStanding::ZoneNotKnownHere,
            ..
        } => format!("Lock: {} entries", format_count(entry_count)),
    }
}

/// The report's zone line and its clauses: the zone placement reads — the lock's — and what
/// the manifest's own line says a refresh would do about it.
///
/// Composed rather than printed so which sentence each pairing gets is a value a test can
/// read. The lock's zone leads, because it is what the destinations above were worked out in;
/// where the lock records none this machine can read, the manifest's leads and the lock's
/// state stands on the `Lock:` line or, for a zone this machine does not know, here with its
/// ways out. Every comparison is between parsed zones (`RecordedZone::same_zone`).
fn zone_lines(status: &status_ops::ManifestStatus, manifest: &Path) -> Vec<String> {
    let mut lines = Vec::new();
    let lock_zone = match &status.lock_zone {
        Some(LockZone::Recorded(zone)) => {
            lines.push(super::status_lock_zone_line(zone));
            Some(zone)
        }
        Some(LockZone::NotKnownHere(name)) => {
            lines.push(super::status_lock_zone_name_line(name));
            // The cause under the zone, and its ways out under the cause.
            for line in super::lock_zone_not_known_here(name, manifest, "") {
                lines.push(format!("  {line}"));
            }
            None
        }
        Some(LockZone::PredatesZone) | None => {
            // No zone in the lock to lead with: the manifest's own line leads, as it stands.
            let lock_present = matches!(status.lock, LockStanding::Read { .. });
            match &status.manifest_zone {
                Ok(ManifestZone::Recorded(zone)) => {
                    lines.push(super::status_manifest_zone_line(zone, lock_present));
                }
                Ok(ManifestZone::FromMachine(zone)) => {
                    lines.push(super::status_no_zone_line_lead(zone, manifest));
                }
                Err(refusal) => lines.push(super::status_zone_refusal_lead(refusal)),
            }
            return lines;
        }
    };
    match &status.manifest_zone {
        Ok(ManifestZone::Recorded(zone)) => {
            if lock_zone.is_none_or(|lock| !lock.same_zone(zone)) {
                lines.push(format!(
                    "  {}",
                    super::manifest_zone_differs(zone, manifest)
                ));
            }
        }
        Ok(ManifestZone::FromMachine(zone)) => {
            lines.push(format!(
                "  {}",
                super::manifest_zone_line_deleted(zone, manifest)
            ));
        }
        Err(refusal) => lines.push(format!(
            "  {}",
            super::refresh_would_refuse(&super::zone_refusal(refusal))
        )),
    }
    lines
}

/// The lines under the report's `Lock:` line, in the order `cluster refresh` states its own
/// causes: the doors first, then the recorded scope's skipped and unrooted lines, then the
/// zone, then each query line the refresh would refuse on.
///
/// Stated on stdout, this command's own scope channel — a report's scope belongs beside its
/// header, not on a side channel. Status is a **report about acts it does not perform**: it
/// states every cause this manifest's acts would be turned away by and refuses nothing of its
/// own. The causes can hold at once — a scope line naming a closed root, entries standing behind
/// one, the destination, the zone, each query line — and each names the act it turns away, in
/// the conditional; composed in one place so a root reached more than one way is stated once.
fn state_lines(status: &status_ops::ManifestStatus, manifest: &Path) -> Vec<u8> {
    let mut out = Vec::new();
    for line in manifest_door_lines(status) {
        let _ = writeln!(out, "{line}");
    }
    write_unrooted_scope(&mut out, &status.unrooted_scope);
    crate::scope::write_set_asides(&mut out, &status.set_aside_scope);
    for line in zone_lines(status, manifest) {
        let _ = writeln!(out, "{line}");
    }
    if let Some(refusal) = &status.query_refusal {
        for line in super::query_status_lines(refusal, manifest) {
            let _ = writeln!(out, "{line}");
        }
    }
    out
}

/// The block for entries the pattern gave no destination: how many, each distinct reason, the
/// first few sources, and — where a time was too coarse for the pattern — the counted lines
/// `cluster refresh` states, per key, modifier and grain. The ways out follow with the next
/// step. Composed rather than printed so the block is a value a test can read.
fn expansion_failure_lines(status: &status_ops::ManifestStatus) -> Vec<String> {
    if status.expansion_failures.is_empty() {
        return Vec::new();
    }
    let mut lines = vec![format!(
        "{} of {} entries have no destination:",
        format_count(status.expansion_failure_count),
        format_count(status.lock_entry_count),
    )];
    for reason in &status.expansion_failures {
        lines.push(format!("  {reason}"));
    }
    // Which ones, not only how many: the per-entry table shows a fixed status word, so
    // without this the report names a count and never a file. Capped like every other
    // listing here.
    const SHOWN: usize = 5;
    let failed: Vec<&status_ops::StatusEntry> = status
        .entries
        .iter()
        .filter(|e| e.dest_path.starts_with(status_ops::EXPANSION_FAILED))
        .collect();
    for entry in failed.iter().take(SHOWN) {
        lines.push(format!("  {}", entry.source_path));
    }
    if failed.len() > SHOWN {
        lines.push(format!(
            "  ... and {} more",
            format_count(failed.len() - SHOWN)
        ));
    }
    lines.extend(super::coarse_value_counts(&status.pattern_gaps));
    lines
}

/// The report's summary line. A lock placed nowhere gets no counts: every one would be a zero
/// nobody measured, the lost-source count among them.
fn summary_line(status: &status_ops::ManifestStatus) -> String {
    if status.unplaced > 0 {
        format!("Summary: {}", super::unplaced_statement(status.unplaced))
    } else {
        format!(
            "Summary: {} at destination, {} pending, {} source files missing.",
            format_count(status.at_destination),
            format_count(status.pending),
            format_count(status.source_lost),
        )
    }
}

/// What a refresh says about its recorded scope, in order.
///
/// Composed rather than printed so the **ordering** is a value a test can
/// read. It is a real decision, not a formatting detail: the door lines come
/// first, because a refusal states its own cause before anything qualifying
/// it; the unrooted lines are stated whether or not the run is about to
/// refuse, because neither refusal names them and gating them would leave a
/// prefix under no known root unsaid in exactly the run that had least to work
/// with. The set-asides are stated except when the **sourceless** refusal is
/// firing, which names every one of them itself — a door refusal names none of
/// them, so they are still owed there.
fn scope_statements(scope: &ScopeResolution, doors: &[String], refusing: bool) -> Vec<u8> {
    let mut out = Vec::new();
    for line in doors {
        let _ = writeln!(out, "{line}");
    }
    write_unrooted_scope(&mut out, scope.unrooted());
    if !refusing {
        crate::scope::write_set_asides(&mut out, scope.set_aside());
    }
    out
}

/// The closed doors a refresh meets: the ones its **recorded scope** names,
/// and nothing else.
///
/// A refresh re-selects from the open world and writes the lock whole, so a
/// lock holding entries on a since-closed root is not a door it meets — the
/// rebuild simply does not gather them, which is what "acts ignore parked
/// content" means at this surface. **A record is not a naming**: the lock is
/// Canon's snapshot of the world when the selection was last made, and a
/// refresh is asking again. The scope is the naming — a durable declaration
/// of what collection is being assembled — and a line of it standing on a
/// closed root is a user mistake worth refusing over.
///
/// The lock's own entries are refused where they would be **carried across**,
/// which is `apply`; `cluster status` states them in `apply`'s conditional.
fn parked_doors(scope: &ScopeResolution) -> Vec<String> {
    crate::scope::closed_door_lines(scope.parked(), DoorVerb::Refused, false)
}

/// What `cluster status` states about every closed door this manifest reaches
/// behind — **in the conditional, naming the act each door turns away**.
///
/// A report performs nothing, so `set aside` and `refused` are both false of
/// it: what a reader needs, having just had a next step withheld, is *which
/// act* is blocked. The verb slot carries that (`apply would be rejected`,
/// `cluster refresh would be rejected`), and the two acts meet different doors —
/// the recorded scope is refresh's, the lock's origins and the destination are
/// apply's.
///
/// **One door, one line**, still: a root the manifest reaches through its
/// scope *and* its lock is stated once, with both acts named in the verb.
/// That is what makes the dedupe honest now that the two causes no longer say
/// the same thing — before the conditional they both read `set aside`, and
/// dropping one dropped nothing.
///
/// `<what>` is the reach, by precedence **place > destination > origin**: the
/// place is what the user wrote, the destination is where writes would land,
/// the origin is the residue. Above the roots cap the statement collapses like
/// every other, with the union of the acts and a `<what>` naming the manifest
/// itself — at that grain the *how* is the manifest.
fn manifest_door_lines(status: &status_ops::ManifestStatus) -> Vec<String> {
    /// One root, and every way this manifest reaches behind its door.
    struct Door {
        root: ParkedRoot,
        places: Vec<String>,
        destination: bool,
        origin: Option<usize>,
    }

    /// This root's door, opened on first mention — by index, so no claim is
    /// made about a `Vec` this function has just written to.
    fn slot(doors: &mut Vec<Door>, root: &ParkedRoot) -> usize {
        if let Some(i) = doors.iter().position(|d| d.root.root_id == root.root_id) {
            return i;
        }
        doors.push(Door {
            root: root.clone(),
            places: Vec::new(),
            destination: false,
            origin: None,
        });
        doors.len() - 1
    }

    // First-appearance order across the three causes, which keeps the scope
    // lines first and the destination beside them, as the report reads today.
    let mut doors: Vec<Door> = Vec::new();
    for place in &status.parked_scope {
        let i = slot(&mut doors, &place.root);
        doors[i].places.push(place.path.clone());
    }
    if let Some(dest) = &status.parked_destination {
        let i = slot(&mut doors, dest);
        doors[i].destination = true;
    }
    for (root, count) in &status.parked_entries {
        let i = slot(&mut doors, root);
        doors[i].origin = Some(*count);
    }

    let stated: Vec<(ParkedRoot, DoorVerb, String)> = doors
        .iter()
        .filter_map(|door| {
            // Every door here was created by a cause, so the `None` arm is
            // unreachable — and stated as a filter rather than an unwrap, so
            // an impossible door is silently not stated rather than a panic in
            // a read-only report.
            let act = RefusedAct::of(RefusedActs {
                refresh: !door.places.is_empty(),
                apply: door.destination || door.origin.is_some(),
            })?;
            let what = if !door.places.is_empty() {
                let paths: Vec<&str> = door.places.iter().map(String::as_str).collect();
                crate::scope::place_reach(&paths)
            } else if door.destination {
                crate::scope::DESTINATION_REACH.to_string()
            } else {
                crate::scope::origin_reach(door.origin.unwrap_or(0))
            };
            Some((door.root.clone(), DoorVerb::WouldRefuse(act), what))
        })
        .collect();

    // The collapsed line's verb is the union of the acts the doors above name,
    // read off the doors themselves rather than re-tested against the status.
    // A door is the only thing this line may speak of: a refresh turned away
    // by the sourceless terminal rule is turned away by no door, and naming it
    // here would put a false cause in the sentence.
    let union = RefusedAct::of(RefusedActs {
        refresh: doors.iter().any(|d| !d.places.is_empty()),
        apply: doors.iter().any(|d| d.destination || d.origin.is_some()),
    })
    .unwrap_or(RefusedAct::Apply);
    crate::scope::closed_door_reaches(
        &stated,
        (DoorVerb::WouldRefuse(union), "this manifest reaches them"),
    )
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
            "no known root at {path} — kept in the manifest; no files come from it, and it does not shape where other files land"
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

pub fn status(
    conn: &mut Connection,
    manifest_path: &Path,
    verbose: bool,
    machine: &UserZone,
) -> Result<()> {
    let status = status_ops::compute_manifest_status(conn, manifest_path, machine)?;

    // Header
    println!("Manifest: {}", status.manifest_path);
    println!("Destination: {}", status.dest_display);
    println!("Pattern: {}", status.pattern);
    // The lock's own line, composed rather than printed so which sentence
    // each standing gets is a value a test can read — the rest of this
    // function speaks straight to the terminal and is unpinned, and a
    // discrimination the user reads must not inherit that.
    if let LockStanding::Read {
        hash_matches: false,
        ..
    } = status.lock
    {
        eprintln!("Warning: lock file hash mismatch — manifest may be out of sync.");
    }
    println!("{}", lock_line(status.lock, status.lock_entry_count));
    {
        use std::io::Write as _;
        let _ = std::io::stdout()
            .lock()
            .write_all(&state_lines(&status, manifest_path));
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
    let failures = expansion_failure_lines(&status);
    if !failures.is_empty() {
        for line in failures {
            println!("{line}");
        }
        println!();
    }

    println!("{}", summary_line(&status));
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

    // Safety assessment. Over an empty lock the claim is vacuous, so it is
    // not made: a line saying every file is accounted for, above nothing,
    // reads as a claim about files that do not exist. Over entries with no
    // destination computed the predicate itself answers no, and the warnings
    // below each need a count that was measured.
    if status.all_accounted_for() {
        if status.lock_entry_count > 0 {
            println!("All source files accounted for.");
        }
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
            // Composed where its twin in `apply`'s resume path is composed,
            // because it is one sentence — the two once stood apart, one of
            // them consulting the refresh's standing and one not.
            //
            // The standing asked for is the whole of it, not the door alone:
            // a recorded scope that kept nothing turns the refresh away with
            // no door in sight. And it is this remedy's *own* act that is
            // asked about, no other — the next step's destination narrowing
            // does not reach here, because this is about a lock naming files
            // the world has lost, and bringing it in line is worth doing
            // whether or not the archive is open.
            let refresh = match status.refresh_refused_by() {
                None => super::RefreshStanding::Runs,
                Some(RefreshRefusal::ScopeKeptNothing) => super::RefreshStanding::ScopeKeptNothing,
                Some(
                    RefreshRefusal::Door
                    | RefreshRefusal::ZoneLine
                    | RefreshRefusal::NoMachineZone
                    | RefreshRefusal::Query,
                ) => super::RefreshStanding::WayBackStated,
            };
            for line in super::source_lost_remedy(manifest_path, refresh, &status.set_aside_scope) {
                println!("{line}");
            }
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

    use crate::archive::ops::generate::FullCoverageFact;
    use crate::core::domain::fact::{FactType, KeyTypes};

    /// A plan whose full-coverage keys hold the given types, counted.
    fn plan_with_keys(keys: &[(&str, &[(FactType, u64)])]) -> ClusterGeneratePlan {
        let mut plan = ClusterGeneratePlan::empty();
        plan.full_coverage_facts = keys
            .iter()
            .map(|(key, counts)| FullCoverageFact {
                key: key.to_string(),
                types: KeyTypes::from_counts(counts.iter().copied()).unwrap(),
                description: String::new(),
                coarsest: None,
            })
            .collect();
        plan
    }

    /// Generate and refresh name a full-coverage key of mixed type through the two-type
    /// statement and its way out — one key named, several counted, a key with a space quoted
    /// so the command runs as printed — and say nothing when every key holds one type. No
    /// remedy deletes facts, no type is called a minority, and no type word is printed.
    #[test]
    fn cluster_generate_names_a_mixed_key_through_the_statement() {
        use FactType::{Num, Text};
        let one = plan_with_keys(&[
            ("content.Height", &[(Num, 4)]),
            ("content.Width", &[(Num, 3), (Text, 1)]),
        ]);
        assert_eq!(
            mixed_key_lines(&one),
            [
                "content.Width is of mixed type, so filters and patterns cannot use its values:",
                "only values of one type can be compared or formatted correctly.",
                "To inspect it and learn how to convert it to one type:",
                "  `canon facts convert content.Width`",
            ]
        );

        let two = plan_with_keys(&[
            ("content.File Size", &[(Num, 2), (Text, 2)]),
            ("content.Width", &[(Num, 3), (Text, 1)]),
        ]);
        assert_eq!(
            mixed_key_lines(&two),
            [
                "2 keys are of mixed type, so filters and patterns cannot use their values:",
                "only values of one type can be compared or formatted correctly.",
                "To inspect each and learn how to convert to one type:",
                r#"  `canon facts convert '"content.File Size"'`"#,
                "  `canon facts convert content.Width`",
            ]
        );
        let all = mixed_key_lines(&two).join("\n");
        for gone in ["facts delete", "minority", "num"] {
            assert!(!all.contains(gone), "{gone:?} in {all}");
        }

        assert!(mixed_key_lines(&plan_with_keys(&[("content.Height", &[(Num, 4)])])).is_empty());
    }

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
        run_refresh_on(db, manifest_path, editor, &UserZone::utc())
    }

    /// A refresh on a machine whose zone is `machine`.
    fn run_refresh_on(
        db: &mut Db,
        manifest_path: &Path,
        editor: Option<EditorPass>,
        machine: &UserZone,
    ) -> Result<()> {
        refresh_with_editor(
            db,
            manifest_path,
            false,
            editor,
            "canon cluster refresh",
            &LedgerConfig::default(),
            true,
            machine,
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
    ///
    /// `lock` is taken whole rather than as a "cannot apply" flag, so every
    /// state a lock can be in is expressible here — a hash that no longer
    /// matches included, which the older writer hard-coded true and so could
    /// not pose the question a report got wrong.
    fn status_with(
        pending: usize,
        at_destination: usize,
        source_lost: usize,
        lock: LockStanding,
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

        let mut status = status_ops::ManifestStatus {
            manifest_path: "m.toml".to_string(),
            dest_display: "/archive".to_string(),
            pattern: "{filename}".to_string(),
            lock_entry_count: entries.len(),
            lock,
            entries,
            at_destination,
            pending,
            source_lost,
            size_mismatch: 0,
            source_still_present: 0,
            // Every one of these five is filled below, through the one
            // funnel, so nothing here is read as the shape they end up in.
            unrooted_scope: Vec::new(),
            set_aside_scope: Vec::new(),
            parked_scope: Vec::new(),
            scope_kept_nothing: false,
            scope_confirms_nothing: false,
            parked_entries: Vec::new(),
            parked_destination: None,
            pattern_unexpandable: false,
            expansion_failures: Vec::new(),
            pattern_gaps: Default::default(),
            expansion_failure_count: 0,
            // The zone the lock's header standing describes, and a manifest line naming the
            // same zone: a row that is not about the zone poses no question about it.
            lock_zone: match lock {
                LockStanding::Absent { .. }
                | LockStanding::Read {
                    header: LockHeaderStanding::Absent,
                    ..
                } => None,
                LockStanding::Read {
                    header: LockHeaderStanding::PredatesZone,
                    ..
                } => Some(LockZone::PredatesZone),
                LockStanding::Read {
                    header: LockHeaderStanding::ZoneNotKnownHere,
                    ..
                } => Some(LockZone::NotKnownHere("Mars/Olympus_Mons".to_string())),
                LockStanding::Read {
                    header: LockHeaderStanding::Sound,
                    ..
                } => Some(LockZone::Recorded(parse_zone_name("UTC").unwrap())),
            },
            manifest_zone: Ok(ManifestZone::Recorded(parse_zone_name("UTC").unwrap())),
            query_refusal: None,
            unplaced: 0,
        };
        // The ordinary manifest, so a row that is not about the scope poses no
        // question about it; a row that is calls `with_scope` itself. Set
        // through the one funnel rather than in the literal above, so the
        // default shape is derived rather than agreed with `with_scope` by
        // hand — which is the drift the funnel exists to close.
        with_scope(&mut status, RecordedScope::AllConfirmed);
        status
    }

    /// What a manifest's recorded scope came to, as the **five** registers a
    /// `ManifestStatus` carries — set together, the way
    /// `compute_manifest_status` derives them from one `ScopeResolution`.
    ///
    /// A writer that set them one at a time could pose states the resolution
    /// cannot produce — a line set aside while the scope is said to have kept
    /// something, or every line unrooted while the scope is said to confirm —
    /// and a pin resting on one of those asserts nothing about production.
    /// This is the same reason `lock` is taken whole rather than as a flag.
    ///
    /// The parked register travels beside the shape rather than inside it:
    /// it is orthogonal to whether the *other* lines confirmed — a closed
    /// root's remainder is never asked about, so a parked line neither
    /// confirms nor is set aside — and every shape below can carry one.
    #[derive(Clone)]
    enum RecordedScope {
        /// Nothing recorded. A rebuild has nothing to measure from.
        Nothing,
        /// Every line confirmed: the ordinary manifest.
        AllConfirmed,
        /// Lines carried, none confirmed, at least one of them set aside —
        /// the source-existence policy's terminal rule's own shape.
        KeptNothing(Vec<String>),
        /// Lines carried, none confirmed, every one of them under no known
        /// root. Deliberately not the terminal rule's case.
        AllUnrooted(Vec<String>),
        /// At least one line confirmed, beside lines that were not.
        SomeConfirmed {
            set_aside: Vec<String>,
            unrooted: Vec<String>,
        },
    }

    fn with_scope(status: &mut status_ops::ManifestStatus, scope: RecordedScope) {
        with_scope_and_doors(status, scope, Vec::new())
    }

    fn with_scope_and_doors(
        status: &mut status_ops::ManifestStatus,
        scope: RecordedScope,
        parked: Vec<crate::core::domain::root::ParkedPath>,
    ) {
        let (set_aside, unrooted, confirms_nothing) = match scope {
            RecordedScope::Nothing => (Vec::new(), Vec::new(), true),
            RecordedScope::AllConfirmed => (Vec::new(), Vec::new(), false),
            RecordedScope::KeptNothing(set_aside) => (set_aside, Vec::new(), true),
            RecordedScope::AllUnrooted(unrooted) => (Vec::new(), unrooted, true),
            RecordedScope::SomeConfirmed {
                set_aside,
                unrooted,
            } => (set_aside, unrooted, false),
        };
        status.scope_kept_nothing = confirms_nothing && !set_aside.is_empty();
        status.scope_confirms_nothing = confirms_nothing;
        status.set_aside_scope = set_aside;
        status.unrooted_scope = unrooted;
        status.parked_scope = parked;
    }

    /// A lock that is there and in order — the state every row that is not
    /// about the lock wants.
    fn sound_lock() -> LockStanding {
        LockStanding::Read {
            hash_matches: true,
            header: LockHeaderStanding::Sound,
        }
    }

    /// A lock written before the measurement was recorded in it.
    fn old_lock() -> LockStanding {
        LockStanding::Read {
            hash_matches: true,
            header: LockHeaderStanding::Absent,
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
            next_step(&status_with(2, 0, 0, old_lock())),
            "\nTo rebuild the lock: canon cluster refresh m.toml\n"
        );
        // The same, where the run would otherwise be told to resume.
        assert_eq!(
            next_step(&status_with(1, 1, 0, old_lock())),
            "\nTo rebuild the lock: canon cluster refresh m.toml\n"
        );
    }

    /// The other side of the same branch: a lock `apply` can actually run
    /// against still gets the hint it always got, in both its arms.
    #[test]
    fn a_lock_apply_can_run_still_points_at_apply() {
        assert_eq!(
            next_step(&status_with(2, 0, 0, sound_lock())),
            "\nTo apply: canon apply m.toml\n"
        );
        assert_eq!(
            next_step(&status_with(1, 1, 0, sound_lock())),
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
    /// would be the same fault in a different word.
    ///
    /// **What is named instead is the edit**, in the words `apply` uses for
    /// the same state. The rows above say what failed for each source, which
    /// is a cause and not an act; leaving it there made the two commands
    /// answer one state two ways, `apply` telling the reader what to change
    /// and the report that sent them to `apply` saying nothing.
    ///
    /// **And which edit depends on which failure**, walked here in both
    /// directions. The scope key is not a fact, so a manifest with no scope
    /// must never be sent to check fact coverage; a pattern naming a fact no
    /// source carries must never be told to scope the manifest. Each answer is
    /// asserted absent from the other's arm, because a remedy that is merely
    /// present is not thereby the right one.
    #[test]
    fn a_pattern_that_cannot_expand_names_the_edit_and_no_command() {
        for (pending, at_destination) in [(2, 0), (1, 1)] {
            for needs_a_scope in [false, true] {
                let mut status = status_with(pending, at_destination, 0, sound_lock());
                status.pattern_unexpandable = true;
                status.pattern_gaps.needs_a_scope_it_lacks = needs_a_scope;
                status.pattern_gaps.fact_not_carried = !needs_a_scope;
                // The scope arm is the manifest that records no scope that
                // confirms. Where one does, the rebuild measures and *is* the
                // step (`a_declared_scope_the_lock_has_not_seen_is_rebuilt_into_it`).
                status.scope_confirms_nothing = needs_a_scope;
                let printed = next_step(&status);
                assert!(
                    !printed.contains("canon cluster refresh") && !printed.contains("canon apply"),
                    "no command may be named here: {printed}"
                );
                if needs_a_scope {
                    assert!(
                        printed.contains("meta.scope") && printed.contains("{source.rel_path}"),
                        "the scope failure takes the scope answer: {printed}"
                    );
                    assert!(
                        !printed.contains("canon facts"),
                        "the scope key is not a fact, so fact coverage is the wrong place to \
                         send the reader: {printed}"
                    );
                } else {
                    assert!(
                        printed.contains("canon facts") && printed.contains("facts it names"),
                        "the fact failure takes the fact answer: {printed}"
                    );
                    assert!(
                        !printed.contains("meta.scope"),
                        "scoping the manifest does nothing for a missing fact: {printed}"
                    );
                }
            }
        }

        // But a lock whose entries lack the scope the pattern needs still
        // gets its rebuild where the scope confirms — there the refresh is
        // exactly the fix, whatever the header says.
        let mut status = status_with(2, 0, 0, old_lock());
        status.pattern_unexpandable = true;
        status.pattern_gaps.needs_a_scope_it_lacks = true;
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
    ///
    /// All three shapes a resolution can actually produce, because the
    /// registers move together and two of them look alike from one field: a
    /// line beside a confirmed one, a lone unrooted line, and a lone
    /// set-aside line — the last of which stops the *refresh* by the terminal
    /// rule while leaving the apply exactly where it was, which is the whole
    /// reason the two acts have their own rows.
    #[test]
    fn a_scope_line_that_resolves_to_nothing_still_points_at_apply() {
        for scope in [
            RecordedScope::SomeConfirmed {
                set_aside: vec!["/photos/nothing-here".to_string()],
                unrooted: vec!["/canon-test/no-such-root".to_string()],
            },
            RecordedScope::AllUnrooted(vec!["/canon-test/no-such-root".to_string()]),
            RecordedScope::KeptNothing(vec!["/photos/nothing-here".to_string()]),
        ] {
            let mut status = status_with(2, 0, 0, sound_lock());
            with_scope(&mut status, scope);
            assert_eq!(next_step(&status), "\nTo apply: canon apply m.toml\n");
        }
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
            status_with(0, 2, 0, old_lock()), // all applied; nothing to do
            status_with(0, 0, 0, old_lock()), // an empty lock
            status_with(0, 0, 2, old_lock()), // sources lost, no rebuild needed
            status_with(2, 0, 1, old_lock()), // pending, but content is missing
            status_with(2, 1, 1, old_lock()), // the same, part-applied
            status_with(0, 1, 1, old_lock()), // part-applied and part-lost
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

    /// **A lock whose hash no longer matches points at the rebuild, never at
    /// the apply.** The mismatch reached the header warning above the report
    /// and stopped there; the step was decided without it, so a user following
    /// the report's own advice met `apply`'s refusal, which points back at the
    /// rebuild the report had declined to print.
    #[test]
    fn a_hash_mismatch_points_at_the_rebuild_not_the_apply() {
        let mismatched = LockStanding::Read {
            hash_matches: false,
            header: LockHeaderStanding::Sound,
        };
        for (pending, at_destination) in [(2, 0), (1, 1)] {
            let status = status_with(pending, at_destination, 0, mismatched);
            assert_eq!(
                next_step(&status),
                "\nTo rebuild the lock: canon cluster refresh m.toml\n",
                "pending={pending}"
            );
        }
    }

    /// **A rebuild that would change only the stated reason is not a step.**
    /// An unmeasured lock has two causes: a lock written before the
    /// measurement, which a rebuild supplies, and a manifest recording no
    /// scope that confirms, where there is nothing to measure from and the
    /// rebuilt lock is unmeasured in exactly the same way.
    ///
    /// The first arm is the discrimination: the same old lock over a scope
    /// that *does* confirm still gets its rebuild, so this is not a blanket
    /// silence over old-and-unexpandable.
    ///
    /// Withholding the rebuild is not falling silent: where no rebuild helps,
    /// the reader is told what to edit instead.
    #[test]
    fn a_rebuild_that_only_changes_the_reason_is_not_offered() {
        let mut confirms = status_with(2, 0, 0, old_lock());
        confirms.pattern_unexpandable = true;
        confirms.pattern_gaps.needs_a_scope_it_lacks = true;
        assert_eq!(
            next_step(&confirms),
            "\nTo rebuild the lock: canon cluster refresh m.toml\n",
            "the scope confirms, so the rebuild measures"
        );

        let mut nothing_to_measure = status_with(2, 0, 0, old_lock());
        nothing_to_measure.pattern_unexpandable = true;
        nothing_to_measure.pattern_gaps.needs_a_scope_it_lacks = true;
        with_scope(&mut nothing_to_measure, RecordedScope::Nothing);
        let printed = next_step(&nothing_to_measure);
        assert!(
            !printed.contains("canon cluster refresh"),
            "the rebuild would change only the reason: {printed}"
        );
        assert!(
            printed.contains("meta.scope"),
            "and the reader is still owed the edit: {printed}"
        );
    }

    /// **The terminal rule is never a bare rebuild, and never silence
    /// either**, though no door is closed anywhere. The report asked only
    /// about doors, so a manifest whose one recorded line is set aside was
    /// first sent to a rebuild that refuses — the pointer's only outcome a
    /// second refusal — and then, when that was withheld, left with no act at
    /// all. Both were wrong, and the second is the one a reader feels: a
    /// set-aside line carries no act of its own, unlike a door line.
    ///
    /// What it gets instead is the pair `cluster refresh` prints when it
    /// refuses for this very reason. The scan leads because Canon cannot tell
    /// an unscanned folder from a wrong one, and only the user can.
    #[test]
    fn a_refresh_the_terminal_rule_turns_away_is_not_named_bare() {
        let mut status = status_with(2, 0, 0, old_lock());
        with_scope(
            &mut status,
            RecordedScope::KeptNothing(vec!["/photos/2011".to_string()]),
        );

        let step = next_step(&status);
        assert_eq!(
            step,
            "\nNo sources are known under the scopes declared in this manifest, so the lock cannot be rebuilt:\n\
             If the files are there, `canon scan /photos/2011` to index them.\n\
             If not, edit meta.scope, then `canon cluster refresh m.toml` to rewrite the lock.\n",
            "the state owes two acts under a lead naming the line they answer"
        );
        assert!(
            !step.contains("To rebuild the lock"),
            "a bare rebuild here refuses: the reader must edit or scan first"
        );
        assert!(
            !status
                .standing()
                .can_run(crate::archive::domain::Act::Apply),
            "the lock is an old one"
        );
        assert_eq!(
            status.refresh_refused_by(),
            Some(RefreshRefusal::ScopeKeptNothing),
            "the terminal rule is not a door, and the report must still see it"
        );
        assert!(
            status.parked_scope.is_empty(),
            "and it sees it with no door in sight, which is the whole finding"
        );
    }

    /// The remedy for a lock naming files the world has lost gives **each**
    /// refusal the way back that refusal leaves the reader needing — one
    /// composition, three answers.
    ///
    /// Renamed from `…_names_the_refresh_only_where_it_would_run`, whose claim
    /// the terminal-rule arm now breaks on purpose: that arm does name a
    /// refresh, after the act that unblocks it. The rule was never "never
    /// mention the refresh" but "never send the reader straight at one that
    /// will turn them away".
    #[test]
    fn the_source_lost_remedy_gives_each_refusal_its_own_way_back() {
        let set_aside = vec!["/photos/2019".to_string()];
        let runs = super::super::source_lost_remedy(
            Path::new("m.toml"),
            super::super::RefreshStanding::Runs,
            &set_aside,
        );
        assert_eq!(
            runs,
            vec![
                "Check if the source volume is connected. If files are truly lost,".to_string(),
                "refresh the manifest: canon cluster refresh m.toml".to_string(),
            ]
        );
        // A door, the zone or a query line states its own way back in the
        // report, so repeating one here would be noise.
        let way_back_stated = super::super::source_lost_remedy(
            Path::new("m.toml"),
            super::super::RefreshStanding::WayBackStated,
            &set_aside,
        );
        assert_eq!(
            way_back_stated,
            vec!["Check if the source volume is connected.".to_string()]
        );
        // The terminal rule does not. `no sources known at <p> — skipped`
        // names no act, so falling silent here would leave the reader with
        // none at all — the defect this arm exists to stop. Both answers are
        // offered because only the user knows which is theirs.
        let kept_nothing = super::super::source_lost_remedy(
            Path::new("m.toml"),
            super::super::RefreshStanding::ScopeKeptNothing,
            &set_aside,
        );
        assert_eq!(
            kept_nothing,
            vec![
                "Check if the source volume is connected.".to_string(),
                "No sources are known under the scopes declared in this manifest, so the lock cannot be rebuilt:"
                    .to_string(),
                "If the files are there, `canon scan /photos/2019` to index them.".to_string(),
                "If not, edit meta.scope, then `canon cluster refresh m.toml` to rewrite the lock."
                    .to_string(),
            ],
            "a scope that kept nothing owes the same pair the refresh itself prints"
        );
        assert!(
            kept_nothing
                .iter()
                .all(|l| !l.contains("If files are truly")),
            "the scope answers are their own thought, not a continuation of the lead"
        );
        assert_eq!(
            super::super::source_lost_remedy(
                Path::new("m.toml"),
                super::super::RefreshStanding::Unasked,
                &[],
            ),
            runs,
            "a caller that has not asked still names the way forward"
        );
    }

    /// **The scan comes first, and it names the paths the report just
    /// skipped.** Canon cannot tell an unscanned folder from a wrong one, so
    /// both answers are offered; the command is there to be pasted, so the
    /// paths are in it and quoted by the tree's own quoter.
    #[test]
    fn a_scope_that_kept_nothing_is_offered_the_scan_before_the_edit() {
        let lines = super::super::scan_or_edit_scope(
            Path::new("m.toml"),
            &["/photos/2019".to_string(), "/tmp/it's dir".to_string()],
        );
        assert_eq!(
            lines,
            vec![
                "If the files are there, `canon scan /photos/2019 '/tmp/it'\\''s dir'` \
                 to index them."
                    .to_string(),
                "If not, edit meta.scope, then `canon cluster refresh m.toml` \
                 to rewrite the lock."
                    .to_string(),
            ],
            "every skipped path is named, and each is quoted to survive a shell"
        );
    }

    /// **The two absences take opposite remedies, and the manifest's own
    /// record tells them apart.** A manifest still naming a lock hash lost a
    /// lock that existed, and a rebuild is the way back. A manifest recording
    /// the empty hash has no lock *because* its last refresh matched nothing,
    /// so naming the rebuild there is a fixed point — the refresh runs,
    /// matches nothing again, and returns the manifest with not even the
    /// stated reason changed. What moves it forward is a different selection.
    #[test]
    fn a_lock_that_matched_nothing_is_not_sent_back_to_the_same_query() {
        let manifest = Path::new("m.toml");

        let matched_nothing = super::super::no_lock_remedy(manifest, "");
        assert!(
            matched_nothing.contains("matched nothing"),
            "{matched_nothing}"
        );
        assert!(
            matched_nothing.contains("Edit the query or scope"),
            "the way out is a different selection: {matched_nothing}"
        );

        let lock_has_gone = super::super::no_lock_remedy(manifest, "abc123");
        assert_eq!(
            lock_has_gone, "Rebuild it: canon cluster refresh m.toml",
            "a lock that existed is rebuilt"
        );
    }

    /// **The two absences read differently, and the sentences are not
    /// interchangeable.** Each names the cause that decides which remedy
    /// `apply` gives, so swapping them would send a reader whose query matched
    /// nothing to a rebuild that matches nothing again — the fixed point this
    /// pair exists to keep the report out of.
    #[test]
    fn each_absent_lock_is_named_by_its_own_cause() {
        assert_eq!(
            super::lock_line(
                LockStanding::Absent {
                    manifest_names_a_hash: false
                },
                0
            ),
            "Lock: none — the last refresh matched nothing.",
            "the empty hash is Canon's own record of a zero-match refresh"
        );
        assert_eq!(
            super::lock_line(
                LockStanding::Absent {
                    manifest_names_a_hash: true
                },
                0
            ),
            "Lock: none — the manifest names a lock file that is not there.",
            "a recorded hash means a lock existed and has since gone"
        );
        assert_eq!(
            super::lock_line(
                LockStanding::Read {
                    hash_matches: true,
                    header: LockHeaderStanding::Sound
                },
                4200
            ),
            "Lock: 4,200 entries",
            "a lock that is there is counted, not explained"
        );
    }

    /// **A lock written before the measurement says so beside its count**, in
    /// the words `apply`'s refusal uses — so a reader who meets that refusal
    /// next has already been told the cause, whatever the pattern reads.
    #[test]
    fn a_locks_age_is_stated_on_its_own_line() {
        for hash_matches in [true, false] {
            let line = super::lock_line(
                LockStanding::Read {
                    hash_matches,
                    header: LockHeaderStanding::Absent,
                },
                2,
            );
            assert_eq!(
                line, "Lock: 2 entries — built before lock files recorded the scopes they were built from",
                "hash_matches={hash_matches}"
            );
        }
    }

    /// **A remedy exists to be typed.** Every sentence naming a command goes
    /// through one spelling, and that spelling defers to the tree's own
    /// `shell_quote` — so all of them run as printed, not just whichever one
    /// the quoting rule was written beside.
    ///
    /// The apostrophe case is the one that matters: a spelling that quoted
    /// only spaces emitted `'/tmp/it's dir/m.toml'`, an unbalanced quote that
    /// will not run, and "a path that needs it" is exactly what that is.
    #[test]
    fn every_remedy_quotes_a_path_that_needs_it() {
        for (dir, want) in [
            ("/tmp/my dir/m.toml", "'/tmp/my dir/m.toml'"),
            ("/tmp/it's dir/m.toml", "'/tmp/it'\\''s dir/m.toml'"),
        ] {
            let path = Path::new(dir);
            let set_aside = vec!["/photos/2019".to_string()];
            let mut sentences = vec![
                super::super::rebuild_remedy(path),
                super::super::no_lock_remedy(path, ""),
                super::super::no_lock_remedy(path, "abc123"),
                super::super::apply_command(path, false),
                super::super::apply_command(path, true),
            ];
            sentences.extend(super::super::scan_or_edit_scope(path, &set_aside));
            sentences.extend(super::super::source_lost_remedy(
                path,
                super::super::RefreshStanding::Runs,
                &set_aside,
            ));
            sentences.extend(super::super::source_lost_remedy(
                path,
                super::super::RefreshStanding::ScopeKeptNothing,
                &set_aside,
            ));

            let mut checked = 0;
            for sentence in &sentences {
                // Longest verb first, and stop at the first match: `canon
                // apply --resume` also contains `canon apply`, and testing the
                // shorter form against it would demand the path sit where the
                // flag is.
                for verb in [
                    "canon cluster refresh ",
                    "canon apply --resume ",
                    "canon apply ",
                ] {
                    if !sentence.contains(verb) {
                        continue;
                    }
                    assert!(
                        sentence.contains(&format!("{verb}{want}")),
                        "will not run as printed: {sentence}"
                    );
                    checked += 1;
                    break;
                }
            }
            assert!(
                checked >= 8,
                "the sweep must actually reach the sentences: {checked}"
            );
        }

        // A path needing no quoting must not grow any, or every ordinary
        // remedy would read as if something were odd about the manifest.
        let plain = super::super::rebuild_remedy(Path::new("/tmp/m.toml"));
        assert_eq!(plain, "Rebuild it: canon cluster refresh /tmp/m.toml");
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
            "no known root at /canon-test/no-such-root — kept in the manifest; \
             no files come from it, and it does not shape where other files land\n"
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
            &UserZone::utc(),
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
            scope.kept_nothing(),
            "the fixture must reach the refusing branch, or this proves nothing"
        );
        let said = String::from_utf8(scope_statements(&scope, &[], true)).unwrap();
        assert_eq!(
            said,
            "no known root at /canon-test/no-such-root — kept in the manifest; \
             no files come from it, and it does not shape where other files land\n",
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
        assert!(!scope.kept_nothing());
        let said = String::from_utf8(scope_statements(&scope, &[], false)).unwrap();
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

    /// A lock file holding one entry per given source id, written by hand
    /// because the writer is `ops`-private — the shape is JSONL and the reader
    /// under test only ever looks at `id`.
    fn write_lock_with_entries(lock_path: &Path, ids: &[i64]) {
        let mut body = String::new();
        for &id in ids {
            let entry = crate::archive::domain::LockEntry {
                id,
                root_id: 1,
                path: format!("/photos/{id}.jpg"),
                device: 1,
                inode: id,
                size: 10,
                mtime: 0,
                partial_hash: "ph".to_string(),
                object_id: None,
                hash_type: None,
                hash_value: None,
                scope_rel_path: None,
            };
            body.push_str(&serde_json::to_string(&entry).unwrap());
            body.push('\n');
        }
        fs::write(lock_path, body).unwrap();
    }

    /// The manifest door's refusal, in the first of its two causes: the
    /// recorded scope names a root the user closed. Stated with the act's own
    /// verb and the way back, and nothing about sources — the door is the
    /// reason, and "no sources known" there would be the second, false cause.
    #[test]
    fn a_parked_recorded_scope_is_refused_by_name() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", true);
        insert_source(&conn, root, "a.jpg", None);
        let roots = crate::core::repo::root::fetch_all(&conn).unwrap();
        let scope = resolve_recorded_scope(&conn, &["/photos".to_string()], &roots).unwrap();

        let doors = parked_doors(&scope);

        assert_eq!(
            doors,
            vec!["/photos suspended — rejected · canon roots unsuspend path:/photos".to_string()]
        );
        assert!(
            !scope.kept_nothing(),
            "the sourceless refusal must not also fire — one cause, one sentence"
        );
    }

    /// **A lock entry behind a door is not a door a refresh meets.** A record
    /// is not a naming: the lock is Canon's snapshot of where sources stood
    /// when the selection was last made, and a refresh is asking again — it
    /// re-selects from the open world, which already leaves parked roots out,
    /// and writes the new lock without them. The entries are refused where
    /// they would be carried across, at `apply`.
    ///
    /// The lock is written here and stands untouched by the answer: what this
    /// pins is that the refresh's door never consults it at all.
    #[test]
    fn a_lock_entry_behind_a_door_is_not_a_refresh_door() {
        let dir = tempfile::tempdir().unwrap();
        let conn = setup_test_db();
        let live = insert_root(&conn, "/live", "source", false);
        let parked = insert_root(&conn, "/photos", "source", true);
        insert_source(&conn, live, "keeper.jpg", None);
        let a = insert_source(&conn, parked, "a.jpg", None);
        let b = insert_source(&conn, parked, "b.jpg", None);
        let roots = crate::core::repo::root::fetch_all(&conn).unwrap();
        let scope = resolve_recorded_scope(&conn, &["/live".to_string()], &roots).unwrap();

        let lock_path = dir.path().join("cluster.lock");
        write_lock_with_entries(&lock_path, &[a, b]);

        assert!(
            parked_doors(&scope).is_empty(),
            "the refresh's own door is the recorded scope, and this one is clean"
        );
    }

    /// **One door, one line — and the verb carries what the dedupe drops.** A
    /// root the manifest reaches through its recorded scope *and* its lock is
    /// stated once by the report, in the place shape the scope gives it, with
    /// both acts named: `cluster refresh` is turned away by the scope line and
    /// `apply` by the entries, and before the conditional verb those two lines
    /// both read `set aside` and saying one was saying both.
    #[test]
    fn a_root_reached_by_both_acts_is_one_line_naming_both() {
        let parked = ParkedRoot {
            root_id: 1,
            root_path: "/photos".to_string(),
        };
        let mut status = status_with(1, 0, 0, sound_lock());
        with_scope_and_doors(
            &mut status,
            RecordedScope::AllConfirmed,
            vec![crate::core::domain::root::ParkedPath {
                path: "/photos".to_string(),
                root: parked.clone(),
            }],
        );
        status.parked_entries = vec![(parked, 2)];

        assert_eq!(
            manifest_door_lines(&status),
            vec![
                "/photos suspended — cluster refresh and apply would be rejected · \
                  canon roots unsuspend path:/photos"
                    .to_string()
            ]
        );
    }

    /// Each cause states its own act, in that act's own nouns, when they fall
    /// on different roots — the origin end counting the sources a run would
    /// carry, the destination naming its role.
    #[test]
    fn each_cause_names_the_act_it_turns_away() {
        let mut status = status_with(1, 0, 0, sound_lock());
        status.parked_entries = vec![(
            ParkedRoot {
                root_id: 1,
                root_path: "/photos".to_string(),
            },
            12,
        )];
        status.parked_destination = Some(ParkedRoot {
            root_id: 2,
            root_path: "/arc".to_string(),
        });

        assert_eq!(
            manifest_door_lines(&status),
            vec![
                "/arc suspended — apply would be rejected: destination · \
                 canon roots unsuspend path:/arc"
                    .to_string(),
                "/photos suspended — apply would be rejected: origin of 12 sources · \
                 canon roots unsuspend path:/photos"
                    .to_string(),
            ]
        );
    }

    /// The door lines lead the refresh's statements: a refusal states its own
    /// cause before anything qualifying it, and the census asserts the first
    /// line spoken is the door.
    #[test]
    fn the_door_leads_what_a_refresh_states() {
        let conn = setup_test_db();
        let parked = insert_root(&conn, "/photos", "source", true);
        insert_source(&conn, parked, "a.jpg", None);
        let roots = crate::core::repo::root::fetch_all(&conn).unwrap();
        let scope = resolve_recorded_scope(
            &conn,
            &[
                "/canon-test/no-such-root".to_string(),
                "/photos".to_string(),
            ],
            &roots,
        )
        .unwrap();
        let doors = parked_doors(&scope);

        let said = String::from_utf8(scope_statements(&scope, &doors, false)).unwrap();
        let mut lines = said.lines();
        assert_eq!(
            lines.next().unwrap(),
            "/photos suspended — rejected · canon roots unsuspend path:/photos"
        );
        assert!(
            lines.next().unwrap().starts_with("no known root at"),
            "the unrooted line is still owed: {said}"
        );
    }

    /// **A step is withheld exactly where its own act meets the door** — and
    /// the two acts meet different doors, which is why one predicate could
    /// never have answered for both.
    ///
    /// `apply` transfers from the lock's entries into the destination, and
    /// meets a door at each; it reads no recorded scope, so a parked scope
    /// line does not stop it. `cluster refresh` re-queries from that scope and
    /// rewrites the manifest and its lock, so only a parked scope line stops
    /// it — a parked lock entry is a record rather than a naming, and the
    /// rebuild simply leaves it out. Each way round, withholding a step whose
    /// command still runs keeps the one thing that works from the user who
    /// needs it.
    ///
    /// The rebuild pointer is offered by the narrower rule: **where it is the
    /// act that brings the apply within reach**. Parked entries with an open
    /// scope and an open destination are exactly that case; a parked
    /// destination is not, whatever else is closed, because no rebuild changes
    /// where the files are going.
    #[test]
    fn a_step_is_withheld_exactly_where_its_own_act_meets_the_door() {
        let parked = |root_id, path: &str| ParkedRoot {
            root_id,
            root_path: path.to_string(),
        };
        let scope_door = |s: &mut status_ops::ManifestStatus| {
            // Through the funnel, so the parked line arrives beside a scope
            // shape that confirmed — which is what a manifest scoped across
            // two roots with one of them closed really looks like, and the
            // shape this row's claim is about.
            with_scope_and_doors(
                s,
                RecordedScope::AllConfirmed,
                vec![crate::core::domain::root::ParkedPath {
                    path: "/photos/2011".to_string(),
                    root: parked(1, "/photos"),
                }],
            )
        };
        // One of the lock's entries behind a door, never all of them — a
        // wholly parked lock is its own case, below, and a fixture that
        // conflated them would pin two rules with one number.
        let entry_door =
            |s: &mut status_ops::ManifestStatus| s.parked_entries = vec![(parked(1, "/photos"), 1)];
        let dest_door =
            |s: &mut status_ops::ManifestStatus| s.parked_destination = Some(parked(2, "/arc"));

        // The apply invitations — the plain one and the resume one — go behind
        // the two doors an apply meets.
        for door in [
            &entry_door as &dyn Fn(&mut status_ops::ManifestStatus),
            &dest_door,
        ] {
            for (pending, at_destination) in [(2, 0), (2, 1)] {
                let mut status = status_with(pending, at_destination, 0, sound_lock());
                door(&mut status);
                assert!(
                    !next_step(&status).contains("canon apply"),
                    "an apply meets this door"
                );
            }
        }

        // …and **not** behind the one it does not. A manifest scoped across
        // two roots with entries on only one of them applies perfectly well
        // when the other is closed, so the invitation stands.
        for (pending, at_destination, expected) in [
            (2, 0, "\nTo apply: canon apply m.toml\n"),
            (2, 1, "\nTo complete: canon apply --resume m.toml\n"),
        ] {
            let mut status = status_with(pending, at_destination, 0, sound_lock());
            scope_door(&mut status);
            assert_eq!(
                next_step(&status),
                expected,
                "an apply reads no recorded scope, so this door is not its own"
            );
        }

        // Parked entries: the rebuild is the act that clears them, so it is
        // offered in place of the withheld apply.
        let mut status = status_with(2, 0, 0, sound_lock());
        entry_door(&mut status);
        assert_eq!(
            next_step(&status),
            "\nTo rebuild the lock: canon cluster refresh m.toml\n",
            "a rebuild leaves the parked entries out, and the apply then runs"
        );

        // A parked destination owes no step at all — its own way back was
        // stated with the door — and it silences the rebuild pointer even
        // where entries are parked too, since the apply is no nearer for it.
        let mut status = status_with(2, 0, 0, sound_lock());
        dest_door(&mut status);
        assert_eq!(next_step(&status), "", "no act brings this apply nearer");
        let mut status = status_with(2, 0, 0, sound_lock());
        entry_door(&mut status);
        dest_door(&mut status);
        assert_eq!(
            next_step(&status),
            "",
            "a refresh would run, but it addresses nothing the reader is stuck on"
        );

        // A lock the apply refuses outright meets the identical rule — the
        // rebuild is the step where it brings the apply back, and nowhere
        // else. Withheld at a door of the refresh's own…
        let mut status = status_with(2, 0, 0, old_lock());
        scope_door(&mut status);
        assert_eq!(next_step(&status), "", "a refresh meets this door");

        // …and withheld at a parked destination, which no rebuild moves,
        // however stale the lock is.
        let mut status = status_with(2, 0, 0, old_lock());
        dest_door(&mut status);
        assert_eq!(
            next_step(&status),
            "",
            "a rebuilt lock still has nowhere to go"
        );

        // …and offered where it would run and would help.
        for door in [
            &entry_door as &dyn Fn(&mut status_ops::ManifestStatus),
            &(|_: &mut status_ops::ManifestStatus| {}) as &dyn Fn(&mut status_ops::ManifestStatus),
        ] {
            let mut status = status_with(2, 0, 0, old_lock());
            door(&mut status);
            assert_eq!(
                next_step(&status),
                "\nTo rebuild the lock: canon cluster refresh m.toml\n",
                "a stale lock's step is the rebuild"
            );
        }

        // **A lock standing wholly behind doors still gets the pointer**, and
        // the temptation not to give it is the one this arm exists to refuse.
        // A rebuild here may re-query to nothing and take the lock away — but
        // it may equally find content the open world has gained since, which
        // this report cannot know without running the selection it does not
        // run. The narrowing above reaches structural facts (a parked
        // destination is unmoved by any rebuild), never a prediction about a
        // query: a predicate over the lock answers the wrong way in both
        // directions, silencing the first case and inviting the second.
        for lock in [sound_lock(), old_lock()] {
            let mut status = status_with(2, 0, 0, lock);
            status.parked_entries = vec![(parked(1, "/photos"), status.lock_entry_count)];
            assert_eq!(
                next_step(&status),
                "\nTo rebuild the lock: canon cluster refresh m.toml\n",
                "the report names the act that runs where it cannot know the outcome"
            );
        }

        // Without a door the same states still point somewhere: this is the
        // door doing the work, not the counts.
        assert!(next_step(&status_with(2, 0, 0, sound_lock())).contains("To apply:"));
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
            format!("{err:#}").contains("Failed to parse manifest config"),
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
            warning.contains("Apply will stop with an error"),
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

    /// **The zone line and its clauses**, one per pairing of what the lock records and what
    /// the manifest's line says a refresh here would do: the lock's zone leads; a manifest
    /// line naming another zone, a deleted line and an unreadable one each add their clause;
    /// a line naming the same zone adds none.
    #[test]
    fn the_status_zone_line_names_the_locks_zone_and_what_a_refresh_would_change() {
        let manifest = Path::new("/m/c.toml");
        let zone = |n: &str| parse_zone_name(n).unwrap();
        let sound = LockStanding::Read {
            hash_matches: true,
            header: LockHeaderStanding::Sound,
        };
        let mut status = status_with(1, 0, 0, sound);
        status.lock_zone = Some(LockZone::Recorded(zone("Europe/Amsterdam")));

        status.manifest_zone = Ok(ManifestZone::Recorded(zone("europe/amsterdam")));
        assert_eq!(
            zone_lines(&status, manifest),
            ["Time zone: Europe/Amsterdam (the lock's)"]
        );

        status.manifest_zone = Ok(ManifestZone::Recorded(zone("America/New_York")));
        assert_eq!(
            zone_lines(&status, manifest),
            [
                "Time zone: Europe/Amsterdam (the lock's)",
                "  the manifest's zone line says America/New_York; canon cluster refresh /m/c.toml brings the lock up to date",
            ]
        );

        status.manifest_zone = Ok(ManifestZone::FromMachine(zone("America/New_York")));
        assert_eq!(
            zone_lines(&status, manifest)[1],
            "  the manifest has no zone line; a refresh on this machine would record America/New_York: canon cluster refresh /m/c.toml"
        );

        status.manifest_zone = Err(crate::archive::domain::ZoneRefusal::Line(
            crate::core::domain::time::zone::ZoneNameError::NotKnownHere(
                "Europe/Amsterdm".to_string(),
            ),
        ));
        assert!(
            zone_lines(&status, manifest)[1].starts_with(
                "  cluster refresh would be rejected: this machine's time zone data does not know \"Europe/Amsterdm\""
            ),
            "{:?}",
            zone_lines(&status, manifest)
        );
    }

    /// **With no zone in the lock, the manifest's line leads**: as it stands, with no lock
    /// or with a lock that records none — the lock's age stands on the `Lock:` line.
    #[test]
    fn status_with_no_lock_shows_the_manifests_zone_line() {
        let manifest = Path::new("/m/c.toml");
        let amsterdam = parse_zone_name("Europe/Amsterdam").unwrap();
        let mut status = status_with(
            0,
            0,
            0,
            LockStanding::Absent {
                manifest_names_a_hash: false,
            },
        );
        status.manifest_zone = Ok(ManifestZone::Recorded(amsterdam.clone()));
        assert_eq!(
            zone_lines(&status, manifest),
            ["Time zone: Europe/Amsterdam (the manifest's; no lock)"]
        );

        let mut status = status_with(
            1,
            0,
            0,
            LockStanding::Read {
                hash_matches: true,
                header: LockHeaderStanding::PredatesZone,
            },
        );
        status.manifest_zone = Ok(ManifestZone::Recorded(amsterdam.clone()));
        assert_eq!(
            zone_lines(&status, manifest),
            ["Time zone: Europe/Amsterdam (the manifest's; the lock records none)"]
        );
        assert_eq!(
            lock_line(status.lock, 1),
            "Lock: 1 entries — built before lock files recorded the time zone"
        );

        status.manifest_zone = Ok(ManifestZone::FromMachine(amsterdam));
        assert_eq!(
            zone_lines(&status, manifest),
            ["Time zone: none recorded; a refresh on this machine would record Europe/Amsterdam: canon cluster refresh /m/c.toml"]
        );
    }

    /// **The third way out names the same zone**: a lock zone this machine does not know is
    /// stated with its cause and the three ways out, the last asking for a name this machine
    /// knows *for the same zone* — any other would move where files go.
    #[test]
    fn the_third_way_out_names_the_same_zone() {
        let mut status = status_with(
            1,
            0,
            0,
            LockStanding::Read {
                hash_matches: true,
                header: LockHeaderStanding::ZoneNotKnownHere,
            },
        );
        status.lock_zone = Some(LockZone::NotKnownHere("Europe/Kyiv".to_string()));
        status.manifest_zone = Ok(ManifestZone::Recorded(
            parse_zone_name("Europe/Kiev").unwrap(),
        ));
        let lines = zone_lines(&status, Path::new("/m/c.toml"));
        assert_eq!(lines[0], "Time zone: Europe/Kyiv (the lock's)");
        assert_eq!(
            lines[1],
            "  This machine's time zone data does not know Europe/Kyiv, the zone the lock was built in."
        );
        assert_eq!(
            lines[4],
            "    change the manifest's zone line to a name this machine knows for the same zone (for example, an older spelling), then canon cluster refresh /m/c.toml"
        );
        assert_eq!(
            lines[5],
            "  the manifest's zone line says Europe/Kiev; canon cluster refresh /m/c.toml brings the lock up to date"
        );
    }

    // =========================================================================
    // The manifest's zone at generate and refresh
    // =========================================================================

    /// [`manifest_text`] with a `zone` line under the query.
    fn manifest_text_zoned(query: &str, zone: &str) -> String {
        manifest_text(query).replacen(
            "generated_at = ",
            &format!("zone = \"{zone}\"\ngenerated_at = "),
            1,
        )
    }

    /// One source saved at 2023-12-31 23:30 UTC — 2024 in Amsterdam, 2023 in New York and
    /// in UTC — and a manifest asking for `query` on `zone`, if any.
    fn new_year_setup(dir: &Path, query: &str, zone: Option<&str>) -> (Db, PathBuf, PathBuf) {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "hash-ny", false);
        crate::core::testing::insert_source_with_metadata(
            &conn,
            root,
            "a.jpg",
            Some(obj),
            1000,
            1704065400,
        );
        let manifest_path = dir.join("cluster.toml");
        let text = match zone {
            Some(z) => manifest_text_zoned(query, z),
            None => manifest_text(query),
        };
        fs::write(&manifest_path, text).unwrap();
        let lock_path = manifest_path.with_extension("lock");
        (Db::from_connection(conn), manifest_path, lock_path)
    }

    fn lock_header_zone(lock_path: &Path) -> Option<String> {
        let first = fs::read_to_string(lock_path).unwrap();
        let header: serde_json::Value =
            serde_json::from_str(first.lines().next().unwrap()).unwrap();
        header["zone"].as_str().map(str::to_string)
    }

    /// **A refresh selects on the manifest's zone, never the machine's**: the New Year file
    /// is 2024 in Amsterdam, the manifest's zone, and 2023 on this New York machine.
    #[test]
    fn refresh_selects_on_the_manifests_zone_not_the_machines() {
        let dir = tempfile::tempdir().unwrap();
        let (mut db, manifest, lock) = new_year_setup(
            dir.path(),
            "source.mtime|year = 2024",
            Some("Europe/Amsterdam"),
        );
        run_refresh_on(
            &mut db,
            &manifest,
            None,
            &UserZone::for_test("America/New_York"),
        )
        .unwrap();
        let text = fs::read_to_string(&lock).unwrap();
        assert!(text.contains("a.jpg"), "the 2024 file is selected: {text}");
        assert_eq!(lock_header_zone(&lock).as_deref(), Some("Europe/Amsterdam"));
    }

    /// **With no zone line a refresh records this machine's**, in both files, and says so.
    #[test]
    fn refresh_without_a_zone_line_records_the_machines_and_says_so() {
        let dir = tempfile::tempdir().unwrap();
        let (mut db, manifest, lock) = new_year_setup(dir.path(), "source.ext = jpg", None);
        let new_york = UserZone::for_test("America/New_York");
        run_refresh_on(&mut db, &manifest, None, &new_york).unwrap();
        assert!(fs::read_to_string(&manifest)
            .unwrap()
            .contains("zone = \"America/New_York\""));
        assert_eq!(lock_header_zone(&lock).as_deref(), Some("America/New_York"));

        let recorded = new_york.recordable().unwrap();
        let lines = refresh_zone_statements(
            &ManifestZone::FromMachine(recorded),
            None,
            &new_york,
            false,
            Some(&PatternGaps::default()),
        );
        assert_eq!(
            lines,
            ["Recorded this machine's time zone in the manifest: America/New_York"]
        );
    }

    /// **A zone line differing from the canonical name only by case is written back in the
    /// canonical form.**
    #[test]
    fn refresh_writes_the_zone_line_in_its_canonical_form() {
        let dir = tempfile::tempdir().unwrap();
        let (mut db, manifest, lock) =
            new_year_setup(dir.path(), "source.ext = jpg", Some("europe/amsterdam"));
        run_refresh(&mut db, &manifest, None).unwrap();
        assert!(fs::read_to_string(&manifest)
            .unwrap()
            .contains("zone = \"Europe/Amsterdam\""));
        assert_eq!(lock_header_zone(&lock).as_deref(), Some("Europe/Amsterdam"));
    }

    /// **An `@name` where a key belongs is named as an alias**, which a manifest's query takes
    /// none of — never as a stray character. After `~` an `@` is part of the glob, so a
    /// generated line holding one (an alias `retina = "source.rel_path ~ *@2x*"`, recorded
    /// expanded) still runs; after `=` it is a character that needs quotes, and quoted it runs.
    #[test]
    fn refresh_names_an_alias_in_the_recorded_query() {
        for (line, at) in [
            ("@img AND source.ext = jpg", 0),
            ("source.ext = jpg AND @img", 21),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let (mut db, manifest, _lock) =
                new_year_setup(dir.path(), line, Some("Europe/Amsterdam"));
            let before = fs::read_to_string(&manifest).unwrap();
            let err = format!("{:#}", run_refresh(&mut db, &manifest, None).unwrap_err());
            assert!(
                err.starts_with(&format!(
                    "cannot run the filter as specified\n  {line}\n  {}^ '@img' is an alias, and \
                     aliases are not expanded here; write the filter it stands for\n`canon \
                     cluster refresh --edit ",
                    " ".repeat(at)
                )),
                "{err}"
            );
            assert_eq!(fs::read_to_string(&manifest).unwrap(), before, "{line}");
        }

        let dir = tempfile::tempdir().unwrap();
        let (mut db, manifest, _lock) =
            new_year_setup(dir.path(), "source.ext = @img", Some("Europe/Amsterdam"));
        let err = format!("{:#}", run_refresh(&mut db, &manifest, None).unwrap_err());
        assert!(
            err.contains("  source.ext = @img\n               ^ `@` needs quotes around the value"),
            "{err}"
        );
        for line in ["(source.rel_path ~ *@2x*)", "source.ext = \"@img\""] {
            let dir = tempfile::tempdir().unwrap();
            let (mut db, manifest, _lock) =
                new_year_setup(dir.path(), line, Some("Europe/Amsterdam"));
            let err = run_refresh(&mut db, &manifest, None)
                .err()
                .map(|e| format!("{e:#}"))
                .unwrap_or_default();
            assert!(
                !err.contains("cannot run") && !err.contains("is an alias"),
                "{line}: {err}"
            );
        }
    }

    /// **Every refusal cause is reported in one run**: an unreadable zone line, then the query
    /// that cannot run — the order `cluster status` states them in — then the abort
    /// once; and neither file is touched.
    #[test]
    fn refresh_reports_every_refusal_cause_in_one_run() {
        let dir = tempfile::tempdir().unwrap();
        let (mut db, manifest, lock) =
            new_year_setup(dir.path(), "source.ext = (", Some("Europe/Amsterdm"));
        fs::write(&lock, "the lock from last time\n").unwrap();
        let before = fs::read_to_string(&manifest).unwrap();

        let err = format!("{:#}", run_refresh(&mut db, &manifest, None).unwrap_err());
        let zone_at = err
            .find("this machine's time zone data does not know \"Europe/Amsterdm\"")
            .unwrap_or_else(|| panic!("{err}"));
        let query_at = err
            .find(
                "cannot run the filter as specified\n  source.ext = (\n               ^ `(` needs \
                 quotes around the value: source.ext = \"(\"\n`canon cluster refresh --edit ",
            )
            .unwrap_or_else(|| panic!("the query's own cause is reported too: {err}"));
        let aborted_at = err
            .find("Refresh aborted")
            .unwrap_or_else(|| panic!("{err}"));
        assert!(zone_at < query_at && query_at < aborted_at, "{err}");
        assert_eq!(err.matches("Refresh aborted").count(), 1, "{err}");
        assert_eq!(fs::read_to_string(&manifest).unwrap(), before);
        assert_eq!(
            fs::read_to_string(&lock).unwrap(),
            "the lock from last time\n"
        );
    }

    /// **The scope's refusal comes first**: a recorded scope that kept nothing is stated
    /// before the zone's cause, which is still stated, before the abort.
    #[test]
    fn the_scope_refusals_come_first() {
        let dir = tempfile::tempdir().unwrap();
        let (mut db, manifest, _lock) =
            new_year_setup(dir.path(), "source.ext = jpg", Some("Europe/Amsterdm"));
        let text = fs::read_to_string(&manifest).unwrap().replacen(
            "generated_at = ",
            "scope = [\"/photos/nothing-here\"]\ngenerated_at = ",
            1,
        );
        fs::write(&manifest, text).unwrap();

        let err = format!("{:#}", run_refresh(&mut db, &manifest, None).unwrap_err());
        let scope_at = err
            .find("no sources known")
            .unwrap_or_else(|| panic!("{err}"));
        let zone_at = err
            .find("Europe/Amsterdm")
            .unwrap_or_else(|| panic!("{err}"));
        let aborted_at = err
            .find("Refresh aborted")
            .unwrap_or_else(|| panic!("{err}"));
        assert!(scope_at < zone_at && zone_at < aborted_at, "{err}");
    }

    /// **No zone line on a machine whose zone cannot be written down is refused**, naming
    /// both ways out, and nothing is written.
    #[test]
    fn refresh_refuses_no_zone_line_on_a_machine_it_cannot_record() {
        let dir = tempfile::tempdir().unwrap();
        let (mut db, manifest, lock) = new_year_setup(dir.path(), "source.ext = jpg", None);
        let before = fs::read_to_string(&manifest).unwrap();
        let err = format!(
            "{:#}",
            run_refresh_on(&mut db, &manifest, None, &UserZone::for_test_unknown(None))
                .unwrap_err()
        );
        assert!(
            err.contains(
                "cannot tell this machine's time zone; write a zone line (for example zone = \"Europe/Amsterdam\") or set TZ"
            ),
            "{err}"
        );
        assert_eq!(fs::read_to_string(&manifest).unwrap(), before);
        assert!(!lock.exists());
    }

    /// **The clock line is said where the run read an instant and the clock is not this
    /// machine's** — and only there: a clock reading reads the same on any clock, and a
    /// machine on the same rules needs no line.
    #[test]
    fn refresh_states_the_clock_when_it_differs_and_an_instant_is_read() {
        let amsterdam = parse_zone_name("Europe/Amsterdam").unwrap();
        let new_york = UserZone::for_test("America/New_York");
        let recorded = ManifestZone::Recorded(amsterdam.clone());
        let placed = PatternGaps {
            used_an_instant: true,
            ..Default::default()
        };
        let line =
            "times are in Europe/Amsterdam, as the manifest records (this machine: America/New_York)";

        let query = refresh_zone_statements(
            &recorded,
            Some(&amsterdam),
            &new_york,
            true,
            Some(&PatternGaps::default()),
        );
        assert_eq!(query, [line]);
        let pattern =
            refresh_zone_statements(&recorded, Some(&amsterdam), &new_york, false, Some(&placed));
        assert_eq!(pattern, [line]);
        let same_machine = refresh_zone_statements(
            &recorded,
            Some(&amsterdam),
            &UserZone::for_test("Europe/Amsterdam"),
            true,
            Some(&placed),
        );
        assert!(same_machine.is_empty(), "{same_machine:?}");
    }

    #[test]
    fn refresh_is_silent_about_the_clock_when_only_clock_readings_are_read() {
        let amsterdam = parse_zone_name("Europe/Amsterdam").unwrap();
        let lines = refresh_zone_statements(
            &ManifestZone::Recorded(amsterdam.clone()),
            Some(&amsterdam),
            &UserZone::for_test("America/New_York"),
            false,
            Some(&PatternGaps::default()),
        );
        assert!(lines.is_empty(), "{lines:?}");
    }

    /// **Where the written lock could not be read back, what was measured is still said, and
    /// nothing more**: the query's own instant is known from the plan, so the full zone change
    /// and the clock follow from it; a query that read none leaves the pattern's reads unknown,
    /// so only the quiet clause — and never a coarse count.
    #[test]
    fn a_failed_read_back_says_what_was_measured_and_nothing_more() {
        let amsterdam = parse_zone_name("Europe/Amsterdam").unwrap();
        let new_york = parse_zone_name("America/New_York").unwrap();
        let machine = UserZone::for_test("Europe/Amsterdam");
        let recorded = ManifestZone::Recorded(new_york.clone());

        let query_read = refresh_zone_statements(&recorded, Some(&amsterdam), &machine, true, None);
        assert_eq!(
            query_read,
            [
                "Time zone changed from Europe/Amsterdam to America/New_York: times in the query and in folder names are now in America/New_York, which can change which sources a time query selects and which folder a file goes to",
                "times are in America/New_York, as the manifest records (this machine: Europe/Amsterdam)",
            ]
        );
        let unknown = refresh_zone_statements(&recorded, Some(&amsterdam), &machine, false, None);
        assert_eq!(
            unknown,
            ["(time zone changed from Europe/Amsterdam to America/New_York)"]
        );
    }

    /// **A zone different from the old lock's is stated**: in full where an instant was
    /// read, in one quiet clause where none was — and never for a link naming the same zone.
    #[test]
    fn refresh_states_a_zone_change_from_the_old_lock() {
        let amsterdam = parse_zone_name("Europe/Amsterdam").unwrap();
        let new_york = parse_zone_name("America/New_York").unwrap();
        let machine = UserZone::for_test("America/New_York");
        let recorded = ManifestZone::Recorded(new_york.clone());

        let full = refresh_zone_statements(
            &recorded,
            Some(&amsterdam),
            &machine,
            true,
            Some(&PatternGaps::default()),
        );
        assert_eq!(
            full,
            ["Time zone changed from Europe/Amsterdam to America/New_York: times in the query and in folder names are now in America/New_York, which can change which sources a time query selects and which folder a file goes to"]
        );
        let quiet = refresh_zone_statements(
            &recorded,
            Some(&amsterdam),
            &machine,
            false,
            Some(&PatternGaps::default()),
        );
        assert_eq!(
            quiet,
            ["(time zone changed from Europe/Amsterdam to America/New_York)"]
        );

        let link = parse_zone_name("US/Eastern").unwrap();
        let none = refresh_zone_statements(
            &recorded,
            Some(&link),
            &machine,
            true,
            Some(&PatternGaps::default()),
        );
        assert!(none.is_empty(), "a link names the same zone: {none:?}");
    }

    /// **An old lock gives no comparison and no refusal**: a refresh over a lock with no
    /// zone, no header or no parse at all runs, and records its zone.
    #[test]
    fn an_old_lock_gives_no_comparison_and_no_refusal() {
        for old in [
            "{\"lock_version\":2,\"scope\":[]}\n",
            "{\"id\":1}\n",
            "not a lock at all\n",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let (mut db, manifest, lock) =
                new_year_setup(dir.path(), "source.ext = jpg", Some("Europe/Amsterdam"));
            fs::write(&lock, old).unwrap();
            run_refresh(&mut db, &manifest, None).unwrap();
            assert_eq!(lock_header_zone(&lock).as_deref(), Some("Europe/Amsterdam"));
        }
    }

    /// **Times too coarse for the pattern are stated with counts and both ways out**, read
    /// over the lock the refresh just wrote.
    #[test]
    fn refresh_states_coarse_values_with_counts_and_both_ways_out() {
        let dir = tempfile::tempdir().unwrap();
        let (mut db, manifest, lock) =
            new_year_setup(dir.path(), "source.ext = jpg", Some("Europe/Amsterdam"));
        let id: i64 = db
            .conn_mut()
            .query_row("SELECT id FROM sources WHERE rel_path = 'a.jpg'", [], |r| {
                r.get(0)
            })
            .unwrap();
        crate::core::testing::insert_time_fact(db.conn_mut(), id, "content.Taken", "2024");
        run_refresh(&mut db, &manifest, None).unwrap();

        let gaps = generate_ops::pattern_gaps_for_written_lock(
            db.conn_mut(),
            &lock,
            "{content.Taken|month}/{filename}",
            parse_zone_name("Europe/Amsterdam").unwrap().clock(),
        )
        .unwrap();
        assert_eq!(
            super::super::coarse_value_statements(&gaps),
            [
                "1 source holds content.Taken only to the year, so {content.Taken|month} has no value for it",
                "Use a coarser modifier in the pattern, which takes effect at the next apply; or add \"content.Taken|month?\" to the manifest's query to leave such files out, then refresh.",
            ]
        );
    }

    /// **`generate` records this machine's zone in both files.**
    #[test]
    fn generate_records_the_machines_zone_in_both_files() {
        let dir = tempfile::tempdir().unwrap();
        let (mut db, _manifest, _lock) = setup(dir.path());
        let out = dir.path().join("generated.toml");
        generate_into(&mut db, &out, &UserZone::for_test("Europe/Amsterdam")).unwrap();
        assert!(fs::read_to_string(&out)
            .unwrap()
            .contains("zone = \"Europe/Amsterdam\""));
        assert_eq!(
            lock_header_zone(&out.with_extension("lock")).as_deref(),
            Some("Europe/Amsterdam")
        );
    }

    /// **`generate` refuses a machine zone it cannot record**, by name and before the
    /// selection: nothing is written and no decision is recorded.
    #[test]
    fn generate_refuses_a_machine_zone_it_cannot_record() {
        let dir = tempfile::tempdir().unwrap();
        let (mut db, _manifest, _lock) = setup(dir.path());
        let out = dir.path().join("generated.toml");
        for (machine, said) in [
            (
                UserZone::for_test_unknown(None),
                "cannot tell this machine's time zone; run it with TZ set, for example TZ=Europe/Amsterdam canon cluster generate",
            ),
            (
                UserZone::for_test_unknown(Some("Europe/Amsterdm")),
                "TZ=Europe/Amsterdm is not a time zone Canon can read; run it with TZ set to a zone name",
            ),
        ] {
            let err = format!("{:#}", generate_into(&mut db, &out, &machine).unwrap_err());
            assert!(err.contains(said), "{err}");
            assert!(!out.exists());
            assert!(!out.with_extension("lock").exists());
            let decisions: i64 = db
                .conn_mut()
                .query_row("SELECT COUNT(*) FROM decisions", [], |r| r.get(0))
                .unwrap();
            assert_eq!(decisions, 0);
        }
    }

    /// `cluster generate` over the `/photos` root `setup` builds, into `/archive`.
    fn generate_into(db: &mut Db, out: &Path, machine: &UserZone) -> Result<()> {
        if crate::core::repo::root::fetch_all(db.conn_mut())
            .unwrap()
            .iter()
            .all(|r| r.path != "/archive")
        {
            insert_root(db.conn_mut(), "/archive", "archive", false);
        }
        generate(
            db,
            &["/photos".to_string()],
            &[],
            &[],
            Path::new("/archive"),
            out,
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
            machine,
        )
    }

    /// **A lock with no time zone asserts no counts**: its summary names the unplaced entries and
    /// that their source files were not checked, never "0 source files missing" over files
    /// nobody looked at.
    #[test]
    fn a_lock_with_no_time_zone_asserts_no_counts() {
        let mut status = status_with(
            0,
            0,
            0,
            LockStanding::Read {
                hash_matches: true,
                header: LockHeaderStanding::PredatesZone,
            },
        );
        status.lock_entry_count = 2;
        status.unplaced = 2;
        assert_eq!(
            summary_line(&status),
            "Summary: 2 entries with no destination computed, and their source files not checked: the lock records no time zone this machine can read."
        );
        status.unplaced = 0;
        assert!(summary_line(&status).contains("source files missing"));
    }

    /// **A lock with no time zone still gets its next step, the rebuild**: nothing was found
    /// wrong in it, only left unchecked, so the gate that withholds a step over lost or
    /// wrong-sized files does not withhold this one — for either header that leaves no zone.
    #[test]
    fn a_lock_with_no_time_zone_names_the_rebuild() {
        for header in [
            LockHeaderStanding::PredatesZone,
            LockHeaderStanding::ZoneNotKnownHere,
        ] {
            let mut status = status_with(
                0,
                0,
                0,
                LockStanding::Read {
                    hash_matches: true,
                    header,
                },
            );
            status.lock_entry_count = 2;
            status.unplaced = 2;
            assert_eq!(
                next_step(&status),
                "\nTo rebuild the lock: canon cluster refresh m.toml\n",
                "{header:?}"
            );
        }
    }

    /// **The report states coarse values with their counts**, in refresh's words, inside the
    /// block for entries with no destination.
    #[test]
    fn status_states_coarse_values_with_their_counts() {
        use crate::core::domain::time::value::Precision;
        let mut status = status_with(
            0,
            0,
            0,
            LockStanding::Read {
                hash_matches: true,
                header: LockHeaderStanding::Sound,
            },
        );
        status.lock_entry_count = 2;
        status.expansion_failures = vec![
            "content.Taken|month has no value: the time is known only to the year".to_string(),
        ];
        status.expansion_failure_count = 2;
        status.pattern_gaps.too_coarse =
            [(("content.Taken|month".to_string(), Precision::Year), 2)].into();
        let lines = expansion_failure_lines(&status);
        assert!(
            lines.contains(
                &"2 sources hold content.Taken only to the year, so {content.Taken|month} has no value for them"
                    .to_string()
            ),
            "{lines:?}"
        );
    }

    /// **The report states the causes in the order `cluster refresh` does**: a door first, then
    /// the zone, then each query line — each query line naming itself and the way back.
    #[test]
    fn status_states_the_door_then_the_zone_then_the_query() {
        let mut status = status_with(1, 0, 0, sound_lock());
        with_scope_and_doors(
            &mut status,
            RecordedScope::AllConfirmed,
            vec![crate::core::domain::root::ParkedPath {
                path: "/photos/2011".to_string(),
                root: ParkedRoot {
                    root_id: 1,
                    root_path: "/photos".to_string(),
                },
            }],
        );
        status.manifest_zone = Err(crate::archive::domain::ZoneRefusal::Line(
            crate::core::domain::time::zone::ZoneNameError::NotKnownHere(
                "Europe/Amsterdm".to_string(),
            ),
        ));
        let conn = crate::core::repo::open_in_memory_for_test();
        let refused = crate::expr::read_filters(&conn, &["source.ext =".to_string()])
            .expect_err("a value is missing")
            .downcast::<crate::expr::ExpressionRefused>()
            .unwrap()
            .into_specified();
        status.query_refusal = Some(refused);
        let text = String::from_utf8(state_lines(&status, Path::new("/m/c.toml"))).unwrap();
        let door = text.find("suspended").unwrap_or_else(|| panic!("{text}"));
        let zone = text
            .find("Europe/Amsterdm")
            .unwrap_or_else(|| panic!("{text}"));
        let query = text
            .find("source.ext =")
            .unwrap_or_else(|| panic!("{text}"));
        assert!(door < zone && zone < query, "{text}");
        assert!(
            text.ends_with(
                "cluster refresh would be rejected: the recorded query cannot run — `canon cluster \
                 refresh --edit /m/c.toml` to fix the query and select again
    source.ext =
                ^ a value is missing after `=`
"
            ),
            "{text}"
        );
    }
}
