use anyhow::{bail, Context, Result};
use std::fs;
use std::path::Path;

use crate::archive::domain::{parse_manifest_allow, validate_manifest_version, ManifestConfig};
use crate::archive::ops::generate::{
    ClusterGenerateParams, ClusterGeneratePlan, ExecuteGenerateParams, ExecuteRefreshParams,
};
use crate::archive::ops::{generate as generate_ops, status as status_ops};
use crate::core::domain::config::{LedgerConfig, RecordingMode};
use crate::core::domain::decision::{DecisionCommand, DecisionStatus};
use crate::core::domain::format::first_chars;
use crate::core::domain::format_count;
use crate::core::domain::scope::DecisionScope;
use crate::core::ops::decision::{DecisionCounts, DecisionParams, DecisionRecorder};
use crate::core::ops::scope::{classify_all, resolve_archive_path};
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

    // Plan
    let scopes = classify_all(scope_prefixes);
    let params = ClusterGenerateParams {
        scopes,
        filters: parsed_filters,
        allow_archived: options.allow_archived,
        allow_duplicates: options.allow_duplicates,
    };
    let plan = generate_ops::plan_generate(conn, &params)?;

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
        scope_prefixes: scope_prefixes.to_vec(),
        archive_root_id,
        base_dir,
        allow: allow_values_to_strings(options),
    };

    let decision = DecisionParams {
        command: DecisionCommand::ClusterGenerate,
        scope: DecisionScope::decompose(scope_prefixes, &all_roots),
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

    let result = generate_ops::execute_generate(&plan, &exec_params)?;

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

    // Split back apart what generation joined. A directory name containing
    // the separator shreds into prefixes that match nothing, and the refresh
    // then re-queries a different set than the one asked for.
    let scope_prefixes: Vec<String> = match &config.meta.scope {
        Some(s) => s.split(", ").map(|p| p.to_string()).collect(),
        None => vec![],
    };

    // Parse filters from config
    let parsed_filters: Vec<Filter> = config
        .meta
        .query
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    // Plan
    let scopes = classify_all(&scope_prefixes);
    let plan_params = ClusterGenerateParams {
        scopes,
        filters: parsed_filters,
        allow_archived,
        allow_duplicates,
    };
    let plan = generate_ops::plan_generate(conn, &plan_params)?;

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
    let exec_params = ExecuteRefreshParams {
        lock_path: lock_path.clone(),
        manifest_path: config_path.to_path_buf(),
        old_manifest_content: old_content,
        config,
    };

    // A refresh whose query now matches nothing records nothing — the 0-item
    // convention, the same answer `cluster generate` gives. The lock is still
    // cleared and the manifest rewritten, because the declaration has to keep
    // telling the truth about its query; but no source was touched, and a 0/0
    // row in the trail would claim an act where there was none.
    if plan.lock_entries.is_empty() {
        generate_ops::execute_refresh(&plan, &exec_params)?;
        println!("No sources matched the query");
        return Ok(());
    }

    // Decompose the manifest's scope to known roots for the decision record.
    let refresh_roots = repo::root::fetch_all(conn)?;

    let decision = DecisionParams {
        command: DecisionCommand::ClusterRefresh,
        scope: DecisionScope::decompose(&scope_prefixes, &refresh_roots),
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

    let result = generate_ops::execute_refresh(&plan, &exec_params)?;

    // Only an empty plan leaves no outcome, and that path returned above; a
    // missing one here means the lock was never written, which the started
    // row stays open to say.
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

    Ok(())
}

// ============================================================================
// Display helpers
// ============================================================================

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

    // Next-step hint
    if status.pending > 0 && status.all_accounted_for() {
        println!();
        if status.at_destination > 0 {
            println!(
                "To complete: canon apply --resume {}",
                manifest_path.display()
            );
        } else {
            println!("To apply: canon apply {}", manifest_path.display());
        }
    }

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

    #[test]
    fn refresh_without_edit_never_opens_an_editor() {
        let dir = tempfile::tempdir().unwrap();
        let (mut db, manifest_path, lock_path) = setup(dir.path());

        run_refresh(&mut db, &manifest_path, None).unwrap();

        let lock = fs::read_to_string(&lock_path).unwrap();
        assert!(lock.contains("a.jpg"), "got: {lock}");
    }
}
