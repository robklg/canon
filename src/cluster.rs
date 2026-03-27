use anyhow::{bail, Context, Result};
use std::fs;
use std::path::{Path, PathBuf};

use crate::ceremony::format_count;
use crate::domain::path::{resolve_paths, validate_paths_in_roots};
use crate::domain::root::resolve_archive_path;
use crate::domain::scope::ScopeMatch;
use crate::expr::filter::Filter;
use crate::ops;
use crate::ops::cluster::{
    ClusterGenerateParams, ClusterGeneratePlan, ExecuteGenerateParams, ExecuteGenerateResult,
    ExecuteRefreshParams, ManifestConfig,
    parse_manifest_allow, validate_manifest_version,
};
use crate::repo::{self, Db};

pub struct GenerateOptions {
    pub force: bool,
    pub allow_archived: bool,
    pub allow_duplicates: bool,
    pub show_archived: bool,
    pub edit: bool,
}

pub fn generate(
    db: &mut Db,
    scope_paths: &[PathBuf],
    original_filters: &[String],
    expanded_filters: &[String],
    dest: &Path,
    output_path: &Path,
    options: &GenerateOptions,
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
    if scope_paths.is_empty() && expanded_filters.is_empty() {
        bail!("At least one of path or --where filter is required");
    }

    let conn = db.conn_mut();

    // Fetch all roots for path resolution
    let all_roots = repo::root::fetch_all(conn)?;

    // Resolve destination to archive root + relative subdir
    let (archive_root_id, _archive_root_path, base_dir) = resolve_archive_path(&all_roots, dest)?;

    // Resolve scope paths (soft resolution: matches known roots, falls back to fs)
    let scope_prefixes = resolve_paths(scope_paths, &all_roots)?;
    validate_paths_in_roots(&scope_prefixes, &all_roots)?;

    let parsed_filters: Vec<Filter> = expanded_filters
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    // Plan
    let scopes = ScopeMatch::classify_all(&scope_prefixes);
    let params = ClusterGenerateParams {
        scopes,
        filters: parsed_filters,
        allow_archived: options.allow_archived,
        allow_duplicates: options.allow_duplicates,
    };
    let plan = ops::cluster::plan_generate(conn, &params)?;

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
        scope_prefixes,
        archive_root_id,
        base_dir,
        allow: allow_values_to_strings(options),
    };

    let result = ops::cluster::execute_generate(&plan, &exec_params)?;

    print_cluster_stdout(
        &format!(
            "Generated manifest: {} ({} sources in {})",
            output_path.display(),
            result.source_count,
            lock_path.display()
        ),
        &result,
    );

    if options.edit {
        let editor = std::env::var("VISUAL")
            .or_else(|_| std::env::var("EDITOR"))
            .unwrap_or_else(|_| "vi".to_string());
        std::process::Command::new(&editor)
            .arg(output_path)
            .status()
            .with_context(|| format!("Failed to launch editor: {editor}"))?;
    }

    let path_str = output_path.display().to_string();
    let escaped = if path_str.contains(' ') {
        format!("'{path_str}'")
    } else {
        path_str
    };
    eprintln!("\nTo apply: canon apply {escaped}");

    Ok(())
}

pub fn refresh(db: &mut Db, config_path: &Path, show_archived: bool) -> Result<()> {
    let conn = db.conn_mut();

    // Read existing manifest content (for notes preservation)
    let old_content = fs::read_to_string(config_path)
        .with_context(|| format!("Failed to read config: {}", config_path.display()))?;
    let config: ManifestConfig = toml::from_str(&old_content)
        .with_context(|| format!("Failed to parse config: {}", config_path.display()))?;

    // Validate manifest version
    validate_manifest_version(config.meta.version)?;

    // Parse allow options from manifest
    let (allow_archived, allow_duplicates) = parse_manifest_allow(&config.options.allow)?;

    // Report which options are in effect
    if !config.options.allow.is_empty() {
        eprintln!(
            "Options from manifest: allow {}",
            config.options.allow.join(", ")
        );
    }

    // Parse scope from config
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
    let scopes = ScopeMatch::classify_all(&scope_prefixes);
    let plan_params = ClusterGenerateParams {
        scopes,
        filters: parsed_filters,
        allow_archived,
        allow_duplicates,
    };
    let plan = ops::cluster::plan_generate(conn, &plan_params)?;

    // Display warnings
    let display_options = GenerateOptions {
        force: false,
        allow_archived,
        allow_duplicates,
        show_archived,
        edit: false,
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

    let result = ops::cluster::execute_refresh(&plan, &exec_params)?;

    match result.outcome {
        Some(r) => {
            print_cluster_stdout(
                &format!(
                    "Refreshed lock file: {} ({} sources)",
                    lock_path.display(),
                    r.source_count
                ),
                &r,
            );
        }
        None => {
            println!("No sources matched the query");
        }
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

fn print_cluster_stdout(header: &str, result: &ExecuteGenerateResult) {
    println!("{header}");
    let root_word = if result.root_breakdown.len() == 1 {
        "root"
    } else {
        "roots"
    };
    println!("  From {} {}:", result.root_breakdown.len(), root_word);
    for (path, count) in &result.root_breakdown {
        println!("    {}  ({})", path, format_count(*count));
    }
    println!(
        "  {} have no archived copy",
        format_count(result.not_archived_count)
    );
}

