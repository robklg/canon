use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use crate::domain::path::resolve_paths;
use crate::domain::root::resolve_archive_path;
use crate::domain::scope::ScopeMatch;
use crate::domain::source::Source;
use crate::expr::filter::Filter;
use crate::expr::{BuiltinKey, BuiltinKeyVisibility, FactType, Modifier, ModifierCategory};
use crate::ops;
use crate::ops::cluster::ClusterGenerateParams;
use crate::repo::{self, Connection, Db};

/// TOML config file (without sources)
#[derive(Serialize, Deserialize)]
pub struct ManifestConfig {
    pub meta: ManifestMeta,
    #[serde(default)]
    pub options: ManifestOptions,
    pub output: ManifestOutput,
}

#[derive(Serialize, Deserialize, Default)]
pub struct ManifestOptions {
    #[serde(default)]
    pub allow: Vec<String>,
}

#[derive(Serialize, Deserialize)]
pub struct ManifestMeta {
    #[serde(default = "default_version")]
    pub version: u32,
    pub query: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    /// RFC3339 timestamp when manifest was generated/refreshed
    pub generated_at: String,
    /// SHA256 hash of the lock file (for integrity validation)
    pub lock_hash: String,
}

fn default_version() -> u32 {
    1
}

const SUPPORTED_MANIFEST_VERSION: u32 = 1;

pub fn validate_manifest_version(version: u32) -> Result<()> {
    if version > SUPPORTED_MANIFEST_VERSION {
        bail!("Manifest version {version} is not supported by this version of Canon. Please update Canon.");
    }
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct ManifestOutput {
    pub pattern: String,
    pub archive_root_id: i64,
    pub base_dir: String,
}

/// JSONL lock entry (one per line in .lock file)
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LockEntry {
    pub id: i64,
    pub root_id: i64,
    pub path: String,
    // Device and inode are recorded for move detection, not for staleness validation.
    // Staleness is determined by size+mtime+partial_hash only.
    pub device: i64,
    pub inode: i64,
    // File state for pre-transfer staleness validation
    pub size: i64,
    pub mtime: i64,
    pub partial_hash: String, // SHA256 of first 8KB + last 8KB (for integrity validation)
    // Content info
    pub object_id: Option<i64>,
    pub hash_type: Option<String>,
    pub hash_value: Option<String>,
    // Note: `facts` field was removed. Apply looks up facts at runtime from DB.
    // Old lock files with `facts` field are still readable (serde ignores unknown fields).
}

impl LockEntry {
    /// Build a LockEntry from a Source and object hash info.
    pub fn from_source(
        source: &Source,
        hash_type: Option<String>,
        hash_value: Option<String>,
    ) -> Self {
        Self {
            id: source.id,
            root_id: source.root_id,
            path: source.path(),
            device: source.device,
            inode: source.inode,
            size: source.size,
            mtime: source.mtime,
            partial_hash: source.partial_hash.clone(),
            object_id: source.object_id,
            hash_type,
            hash_value,
        }
    }
}

pub struct GenerateOptions {
    pub force: bool,
    pub allow_archived: bool,
    pub allow_duplicates: bool,
    pub show_archived: bool,
    pub edit: bool,
}

/// Result from generating a lock file
struct LockGenerationResult {
    source_count: usize,
    full_coverage_facts: Vec<(String, FactType, String)>,
    root_breakdown: Vec<(String, usize)>, // (root_path, count), sorted by path
    not_archived_count: usize,            // sources with no archived copy
    archived_count: usize,                // skipped already-archived sources
    excluded_count: usize,                // skipped excluded sources
    unhashed_count: usize,                // skipped unhashed sources
}

/// Core logic shared between generate() and refresh().
/// Plans the cluster via ops layer, displays warnings, and writes the lock file.
fn generate_lock(
    conn: &mut Connection,
    scope_prefixes: &[String],
    filters: &[Filter],
    lock_path: &Path,
    options: &GenerateOptions,
) -> Result<Option<LockGenerationResult>> {
    let scopes = ScopeMatch::classify_all(scope_prefixes);
    let params = ClusterGenerateParams {
        scopes,
        filters: filters.to_vec(),
        allow_archived: options.allow_archived,
        allow_duplicates: options.allow_duplicates,
    };
    let plan = ops::cluster::plan_generate(conn, &params)?;

    // Report archived files
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

    if plan.lock_entries.is_empty() {
        return Ok(None);
    }

    // Display mixed-type warnings
    if !plan.mixed_type_warnings.is_empty() {
        eprintln!("Warning: some facts have inconsistent types across sources:");
        for (key, breakdown) in &plan.mixed_type_warnings {
            eprintln!("  {key}: {breakdown}");
        }
        eprintln!("  Type-specific modifiers (|year, |month, etc.) may fail on mismatched values.");
        eprintln!("  To fix: delete outliers with 'canon facts delete <key> --on object --value-type <minority-type>'");
    }

    // Write JSONL lock file
    write_lock_file(lock_path, &plan.lock_entries)?;

    Ok(Some(LockGenerationResult {
        source_count: plan.lock_entries.len(),
        full_coverage_facts: plan.full_coverage_facts,
        root_breakdown: plan.root_breakdown,
        not_archived_count: plan.not_archived_count,
        archived_count: plan.archived.len(),
        excluded_count: plan.excluded_count,
        unhashed_count: plan.unhashed_count,
    }))
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

    let parsed_filters: Vec<Filter> = expanded_filters
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    // Generate lock file
    let lock_path = output_path.with_extension("lock");
    let result = generate_lock(conn, &scope_prefixes, &parsed_filters, &lock_path, options)?;

    let result = match result {
        Some(r) => r,
        None => {
            println!("No sources matched the query");
            return Ok(());
        }
    };

    // Compute hash of lock file for integrity validation
    let lock_hash = hash_file(&lock_path)?;

    // Generate fact help from full coverage facts
    let fact_help = generate_fact_help(result.source_count, &result.full_coverage_facts);

    // Build config (TOML without sources) — store expanded filters as the query
    let config = ManifestConfig {
        meta: ManifestMeta {
            version: 1,
            query: expanded_filters.to_vec(),
            scope: if scope_prefixes.len() == 1 {
                Some(scope_prefixes[0].clone())
            } else if scope_prefixes.is_empty() {
                None
            } else {
                Some(scope_prefixes.join(", "))
            },
            generated_at: current_timestamp(),
            lock_hash,
        },
        options: ManifestOptions {
            allow: allow_values_to_strings(options),
        },
        output: ManifestOutput {
            pattern: "{filename}".to_string(),
            archive_root_id,
            base_dir,
        },
    };

    // Assemble manifest with summary comments, notes, and TOML config
    let summary = generate_summary_comments(&result);
    let notes_block = "# === Notes ===\n#\n";

    let toml_str =
        toml::to_string_pretty(&config).context("Failed to serialize manifest config")?;
    let comment_lines: Vec<String> = original_filters
        .iter()
        .zip(expanded_filters.iter())
        .filter(|(orig, exp)| orig != exp)
        .map(|(orig, _)| format!("# Original: {orig}"))
        .collect();
    let toml_str = if comment_lines.is_empty() {
        toml_str
    } else {
        inject_comments_before_key(&toml_str, "query", &comment_lines)
    };

    let manifest = format!(
        "{}\n{}\n{}\n\n{}",
        summary.trim_end(),
        notes_block.trim_end(),
        toml_str.trim_end(),
        fact_help
    );
    fs::write(output_path, &manifest)
        .with_context(|| format!("Failed to write manifest to {}", output_path.display()))?;

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

    eprintln!("\nTo apply: canon apply {}", output_path.display());

    Ok(())
}

pub fn refresh(db: &mut Db, config_path: &Path, show_archived: bool) -> Result<()> {
    let conn = db.conn_mut();

    // Read existing manifest content (for notes preservation)
    let old_content = fs::read_to_string(config_path)
        .with_context(|| format!("Failed to read config: {}", config_path.display()))?;
    let mut config: ManifestConfig = toml::from_str(&old_content)
        .with_context(|| format!("Failed to parse config: {}", config_path.display()))?;

    // Validate manifest version
    validate_manifest_version(config.meta.version)?;

    // Parse allow options from manifest
    let (allow_archived, allow_duplicates) = parse_manifest_allow(&config.options.allow)?;

    let options = GenerateOptions {
        force: false,
        allow_archived,
        allow_duplicates,
        show_archived,
        edit: false,
    };

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

    // Generate lock file using shared logic
    let lock_path = config_path.with_extension("lock");
    let result = generate_lock(conn, &scope_prefixes, &parsed_filters, &lock_path, &options)?;

    match result {
        Some(r) => {
            // Compute hash of new lock file
            let lock_hash = hash_file(&lock_path)?;

            // Update config with new lock_hash and timestamp
            config.meta.lock_hash = lock_hash;
            config.meta.generated_at = current_timestamp();

            // Assemble manifest with summary comments, preserved notes, and TOML config
            let summary = generate_summary_comments(&r);
            let notes = extract_notes(&old_content).unwrap_or_else(|| "\n#\n".to_string());
            let notes_block = format!("# === Notes ==={notes}");

            let fact_help = generate_fact_help(r.source_count, &r.full_coverage_facts);
            let toml_str =
                toml::to_string_pretty(&config).context("Failed to serialize manifest config")?;

            let manifest = format!(
                "{}\n{}\n{}\n\n{}",
                summary.trim_end(),
                notes_block.trim_end(),
                toml_str.trim_end(),
                fact_help
            );
            fs::write(config_path, &manifest)
                .with_context(|| format!("Failed to write config: {}", config_path.display()))?;

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
            // No sources - remove lock file if it exists
            if lock_path.exists() {
                fs::remove_file(&lock_path)?;
            }
            // Update config with empty lock hash
            config.meta.lock_hash = String::new();
            config.meta.generated_at = current_timestamp();
            let toml_str =
                toml::to_string_pretty(&config).context("Failed to serialize manifest config")?;
            fs::write(config_path, &toml_str)
                .with_context(|| format!("Failed to write config: {}", config_path.display()))?;
            println!("No sources matched the query");
        }
    }

    Ok(())
}

/// Write a JSONL lock file with sources filtered to 100% coverage facts
fn write_lock_file(lock_path: &Path, sources: &[LockEntry]) -> Result<()> {
    let lock_file = File::create(lock_path)
        .with_context(|| format!("Failed to create lock file: {}", lock_path.display()))?;
    let mut writer = BufWriter::new(lock_file);

    for source in sources {
        serde_json::to_writer(&mut writer, source)
            .with_context(|| format!("Failed to write lock entry for {}", source.path))?;
        writeln!(writer)?;
    }

    writer.flush()?;
    Ok(())
}

/// Compute SHA256 hash of a file, returning hex string
pub fn hash_file(path: &Path) -> Result<String> {
    let file = File::open(path)
        .with_context(|| format!("Failed to open file for hashing: {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 8192];

    loop {
        let bytes_read = reader.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }

    Ok(format!("{:x}", hasher.finalize()))
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

pub fn parse_manifest_allow(allow: &[String]) -> Result<(bool, bool)> {
    let mut archived = false;
    let mut duplicates = false;
    for v in allow {
        match v.as_str() {
            "archived" => archived = true,
            "duplicates" => duplicates = true,
            other => bail!(
                "Invalid allow value '{other}' in manifest [options]. Valid: archived, duplicates"
            ),
        }
    }
    Ok((archived, duplicates))
}

fn current_timestamp() -> String {
    chrono::Utc::now().to_rfc3339()
}


/// Generate fact help comments for the manifest
fn generate_fact_help(
    source_count: usize,
    full_coverage_facts: &[(String, FactType, String)],
) -> String {
    use strum::IntoEnumIterator;

    if source_count == 0 {
        return String::new();
    }

    let mut help = String::new();
    help.push_str(&format!(
        "# Available facts for pattern (100% coverage on {source_count} sources in this cluster):\n"
    ));
    help.push_str("#\n");

    // Built-in facts (auto-generated from BuiltinKey enum)
    help.push_str("# Built-in:\n");
    for key in BuiltinKey::iter() {
        // Only show Default visibility keys (skip Hidden and NotListed)
        if key.visibility() != BuiltinKeyVisibility::Default {
            continue;
        }
        let name: &'static str = key.into();
        let desc = key.description().unwrap_or("");
        help.push_str(&format!(
            "#   {:18} {:6} - {}\n",
            name,
            key.fact_type().as_str(),
            desc
        ));
    }
    help.push_str(&format!(
        "#   {:18} {:6} - {}\n",
        "object.hash", "text", "Content hash (if hashed)"
    ));
    help.push_str("#\n");

    // User facts with 100% coverage
    if !full_coverage_facts.is_empty() {
        help.push_str("# Content facts:\n");
        for (key, fact_type, description) in full_coverage_facts {
            let desc_part = if description.is_empty() {
                String::new()
            } else {
                format!(" - {description}")
            };
            help.push_str(&format!(
                "#   {:18} {:6}{}\n",
                key,
                fact_type.as_str(),
                desc_part
            ));
        }
        help.push_str("#\n");
    }

    // Modifiers reference (auto-generated from Modifier enum)
    let time_mods: Vec<_> = Modifier::iter()
        .filter(|m| m.category() == ModifierCategory::Time)
        .map(|m| {
            let name: &'static str = m.into();
            format!("|{name}")
        })
        .collect();
    let string_mods: Vec<_> = Modifier::iter()
        .filter(|m| m.category() == ModifierCategory::String)
        .map(|m| {
            let name: &'static str = m.into();
            format!("|{name}")
        })
        .collect();
    help.push_str("# Modifiers:\n");
    help.push_str(&format!("#   Time: {}\n", time_mods.join(" ")));
    help.push_str(&format!("#   String: {}\n", string_mods.join(" ")));
    help.push_str("#   Path: [0] [-1] [1:3] etc.\n");
    help.push_str("#\n");

    // Aliases (auto-generated from BuiltinKey enum)
    help.push_str("# Aliases:\n");
    for key in BuiltinKey::iter() {
        if let Some(expansion) = key.expansion() {
            let name: &'static str = key.into();
            help.push_str(&format!("#   {{{name}}}  →  {{{expansion}}}\n"));
        }
    }
    help.push('\n');

    help
}

/// Insert comment lines before a key in a TOML string.
/// Finds the first line starting with `key = ` and inserts comments above it.
fn inject_comments_before_key(toml_str: &str, key: &str, comments: &[String]) -> String {
    let prefix = format!("{key} = ");
    let mut result = String::with_capacity(toml_str.len() + comments.len() * 40);
    for line in toml_str.lines() {
        if line.starts_with(&prefix) {
            for comment in comments {
                result.push_str(comment);
                result.push('\n');
            }
        }
        result.push_str(line);
        result.push('\n');
    }
    result
}

// ============================================================================
// Cluster summary, notes, and stdout helpers
// ============================================================================

fn generate_summary_comments(result: &LockGenerationResult) -> String {
    use crate::ceremony::format_count;

    let mut s = String::new();
    s.push_str("# === Cluster Summary ===\n");

    let root_word = if result.root_breakdown.len() == 1 {
        "root"
    } else {
        "roots"
    };
    s.push_str(&format!(
        "# {} sources from {} {}:\n",
        format_count(result.source_count),
        result.root_breakdown.len(),
        root_word
    ));
    for (path, count) in &result.root_breakdown {
        s.push_str(&format!("#   {}  ({})\n", path, format_count(*count)));
    }
    s.push_str(&format!(
        "# {} have no archived copy\n",
        format_count(result.not_archived_count)
    ));

    // Skipped line (only if there are skipped sources)
    if result.archived_count > 0 || result.excluded_count > 0 || result.unhashed_count > 0 {
        s.push_str("#\n");
        let mut parts = Vec::new();
        if result.archived_count > 0 {
            parts.push(format!(
                "{} already archived (--allow archived)",
                result.archived_count
            ));
        }
        if result.excluded_count > 0 {
            parts.push(format!("{} excluded", result.excluded_count));
        }
        if result.unhashed_count > 0 {
            parts.push(format!("{} unhashed", result.unhashed_count));
        }
        s.push_str(&format!("# Skipped: {}\n", parts.join(", ")));
    }

    s
}

fn extract_notes(content: &str) -> Option<String> {
    let marker = "# === Notes ===";
    let start_idx = content.find(marker)?;
    let after_marker = start_idx + marker.len();
    let rest = &content[after_marker..];

    // Find end: next "# === " header or first TOML section "[" at line start
    let end = rest
        .lines()
        .enumerate()
        .skip(1) // skip the marker line itself
        .find(|(_, line)| line.starts_with("# === ") || line.starts_with('['))
        .map(|(i, _)| {
            // Calculate byte offset of this line
            rest.lines().take(i).map(|l| l.len() + 1).sum::<usize>()
        })
        .unwrap_or(rest.len());

    Some(rest[..end].to_string())
}

fn print_cluster_stdout(header: &str, result: &LockGenerationResult) {
    use crate::ceremony::format_count;

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

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manifest_options_round_trip() {
        let config = ManifestConfig {
            meta: ManifestMeta {
                version: 1,
                query: vec!["source.ext=jpg".to_string()],
                scope: Some("/photos".to_string()),
                generated_at: "2026-02-15T12:00:00Z".to_string(),
                lock_hash: "abc123".to_string(),
            },
            options: ManifestOptions {
                allow: vec!["archived".to_string(), "duplicates".to_string()],
            },
            output: ManifestOutput {
                pattern: "{filename}".to_string(),
                archive_root_id: 1,
                base_dir: "photos".to_string(),
            },
        };

        let toml_str = toml::to_string_pretty(&config).unwrap();
        let parsed: ManifestConfig = toml::from_str(&toml_str).unwrap();

        assert_eq!(parsed.options.allow, vec!["archived", "duplicates"]);
        assert_eq!(parsed.meta.query, vec!["source.ext=jpg"]);
        assert_eq!(parsed.output.pattern, "{filename}");
    }

    #[test]
    fn test_manifest_options_backward_compat() {
        // Old manifests without [options] should deserialize with defaults
        let toml_str = r#"
[meta]
query = ["source.ext=jpg"]
scope = "/photos"
generated_at = "2026-02-15T12:00:00Z"
lock_hash = "abc123"

[output]
pattern = "{filename}"
archive_root_id = 1
base_dir = "photos"
"#;
        let config: ManifestConfig = toml::from_str(toml_str).unwrap();
        assert!(
            config.options.allow.is_empty(),
            "Options should default to empty"
        );
    }

    #[test]
    fn test_manifest_options_invalid_allow() {
        let result = parse_manifest_allow(&["bogus".to_string()]);
        assert!(result.is_err(), "Should error on invalid allow value");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("bogus"),
            "Error should mention the invalid value"
        );
        assert!(err.contains("archived"), "Error should list valid values");
    }

    // ======================================================================
    // Phase 5: format_count, extract_notes, generate_summary_comments,
    //          version validation, manifest round-trip
    // ======================================================================

    #[test]
    fn test_format_count() {
        use crate::ceremony::format_count;
        assert_eq!(format_count(0), "0");
        assert_eq!(format_count(999), "999");
        assert_eq!(format_count(1000), "1,000");
        assert_eq!(format_count(1234567), "1,234,567");
    }

    #[test]
    fn test_extract_notes_empty_placeholder() {
        let content = "# === Notes ===\n#\n\n[meta]\nversion = 1\n";
        let notes = extract_notes(content).unwrap();
        assert_eq!(notes, "\n#\n\n");
    }

    #[test]
    fn test_extract_notes_with_content() {
        let content =
            "# === Notes ===\n# This cluster has family photos\n# from 2020-2023\n\n[meta]\n";
        let notes = extract_notes(content).unwrap();
        assert_eq!(
            notes,
            "\n# This cluster has family photos\n# from 2020-2023\n\n"
        );
    }

    #[test]
    fn test_extract_notes_missing() {
        let content = "[meta]\nversion = 1\nquery = []\n";
        assert!(extract_notes(content).is_none());
    }

    #[test]
    fn test_extract_notes_before_meta() {
        let content = "# === Notes ===\n# Some note\n[meta]\nversion = 1\n";
        let notes = extract_notes(content).unwrap();
        assert_eq!(notes, "\n# Some note\n");
    }

    #[test]
    fn test_extract_notes_before_next_section() {
        let content = "# === Notes ===\n# My notes\n# === Cluster Summary ===\n# stuff\n";
        let notes = extract_notes(content).unwrap();
        assert_eq!(notes, "\n# My notes\n");
    }

    #[test]
    fn test_generate_summary_single_root() {
        let result = LockGenerationResult {
            source_count: 42,
            full_coverage_facts: vec![],
            root_breakdown: vec![("/photos".to_string(), 42)],
            not_archived_count: 42,
            archived_count: 0,
            excluded_count: 0,
            unhashed_count: 0,
        };
        let summary = generate_summary_comments(&result);
        assert!(summary.contains("42 sources from 1 root:"));
        assert!(summary.contains("#   /photos  (42)"));
        assert!(summary.contains("# 42 have no archived copy"));
        assert!(!summary.contains("Skipped"));
    }

    #[test]
    fn test_generate_summary_multiple_roots() {
        let result = LockGenerationResult {
            source_count: 150,
            full_coverage_facts: vec![],
            root_breakdown: vec![("/backup".to_string(), 50), ("/photos".to_string(), 100)],
            not_archived_count: 120,
            archived_count: 0,
            excluded_count: 0,
            unhashed_count: 0,
        };
        let summary = generate_summary_comments(&result);
        assert!(summary.contains("150 sources from 2 roots:"));
        // Verify sorted order
        let backup_pos = summary.find("/backup").unwrap();
        let photos_pos = summary.find("/photos").unwrap();
        assert!(backup_pos < photos_pos, "Roots should be sorted by path");
    }

    #[test]
    fn test_generate_summary_no_skipped() {
        let result = LockGenerationResult {
            source_count: 10,
            full_coverage_facts: vec![],
            root_breakdown: vec![("/photos".to_string(), 10)],
            not_archived_count: 10,
            archived_count: 0,
            excluded_count: 0,
            unhashed_count: 0,
        };
        let summary = generate_summary_comments(&result);
        assert!(!summary.contains("Skipped"));
    }

    #[test]
    fn test_generate_summary_with_skipped() {
        let result = LockGenerationResult {
            source_count: 10,
            full_coverage_facts: vec![],
            root_breakdown: vec![("/photos".to_string(), 10)],
            not_archived_count: 10,
            archived_count: 0,
            excluded_count: 3,
            unhashed_count: 5,
        };
        let summary = generate_summary_comments(&result);
        assert!(summary.contains("# Skipped: 3 excluded, 5 unhashed"));
    }

    #[test]
    fn test_generate_summary_with_archived_skipped() {
        let result = LockGenerationResult {
            source_count: 10,
            full_coverage_facts: vec![],
            root_breakdown: vec![("/photos".to_string(), 10)],
            not_archived_count: 10,
            archived_count: 4,
            excluded_count: 2,
            unhashed_count: 0,
        };
        let summary = generate_summary_comments(&result);
        assert!(summary.contains("# Skipped: 4 already archived (--allow archived), 2 excluded"));
    }

    #[test]
    fn test_version_1_accepted() {
        assert!(validate_manifest_version(1).is_ok());
    }

    #[test]
    fn test_version_future_rejected() {
        let result = validate_manifest_version(99);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("99"));
        assert!(err.contains("not supported"));
    }

    #[test]
    fn test_manifest_without_version_defaults_to_1() {
        // Old manifests without version field should deserialize as version 1
        let toml_str = r#"
[meta]
query = ["source.ext=jpg"]
scope = "/photos"
generated_at = "2026-02-15T12:00:00Z"
lock_hash = "abc123"

[output]
pattern = "{filename}"
archive_root_id = 1
base_dir = "photos"
"#;
        let config: ManifestConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.meta.version, 1);
    }

    #[test]
    fn test_manifest_with_version_round_trip() {
        let config = ManifestConfig {
            meta: ManifestMeta {
                version: 1,
                query: vec!["source.ext=jpg".to_string()],
                scope: Some("/photos".to_string()),
                generated_at: "2026-02-15T12:00:00Z".to_string(),
                lock_hash: "abc123".to_string(),
            },
            options: ManifestOptions { allow: vec![] },
            output: ManifestOutput {
                pattern: "{filename}".to_string(),
                archive_root_id: 1,
                base_dir: "photos".to_string(),
            },
        };

        let toml_str = toml::to_string_pretty(&config).unwrap();
        let parsed: ManifestConfig = toml::from_str(&toml_str).unwrap();
        assert_eq!(parsed.meta.version, 1);
    }
}
