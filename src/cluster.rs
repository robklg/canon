use anyhow::{bail, Context, Result};
use rusqlite::types::Value;
use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use crate::db::{build_scope_clause, canonicalize_scopes, resolve_archive_path, Connection, Db};
use crate::exclude;
use crate::expr::{BuiltinKey, BuiltinKeyVisibility, FactType, Modifier, ModifierCategory};
use crate::filter::{self, Filter};

/// TOML config file (without sources)
#[derive(Serialize, Deserialize)]
pub struct ManifestConfig {
    pub meta: ManifestMeta,
    pub output: ManifestOutput,
}

#[derive(Serialize, Deserialize)]
pub struct ManifestMeta {
    pub query: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    /// RFC3339 timestamp when manifest was generated/refreshed
    pub generated_at: String,
    /// SHA256 hash of the lock file (for integrity validation)
    pub lock_hash: String,
}

#[derive(Serialize, Deserialize)]
pub struct ManifestOutput {
    pub pattern: String,
    pub archive_root_id: i64,
    pub base_dir: String,
}

/// JSONL lock entry (one per line in .lock file)
#[derive(Serialize, Deserialize, Clone)]
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
    // Snapshot facts (only 100% coverage facts - eligible for pattern use)
    pub facts: HashMap<String, serde_json::Value>,
}

pub struct GenerateOptions {
    pub force: bool,
    pub include_archived: bool,
    pub show_archived: bool,
    pub allow_duplicates: bool,
}

/// Result from generating a lock file
struct LockGenerationResult {
    source_count: usize,
    full_coverage_facts: Vec<(String, FactType, String)>,
}

/// Core logic shared between generate() and refresh()
/// Queries sources, validates, and writes the lock file
fn generate_lock(
    conn: &Connection,
    scope_prefixes: &[String],
    filters: &[Filter],
    lock_path: &Path,
    options: &GenerateOptions,
) -> Result<Option<LockGenerationResult>> {
    let (sources, archived, excluded_count, unhashed_count) =
        query_sources(conn, scope_prefixes, filters, options.include_archived)?;

    // Report excluded files (hard gate - always skipped)
    if excluded_count > 0 {
        eprintln!("Skipped {} excluded sources", excluded_count);
    }

    // Report unhashed files (hard gate - always skipped)
    if unhashed_count > 0 {
        eprintln!("Skipped {} sources without content hash", unhashed_count);
        eprintln!("  To discover: run 'canon ls --unhashed' with your scope/pattern");
        eprintln!("  To include: import hashes via worklist pipeline, then run 'canon cluster refresh'");
        eprintln!("  To permanently exclude: use 'canon exclude set' with your pattern AND 'NOT content.hash.sha256?'");
    }

    // Report archived files
    if !archived.is_empty() {
        eprintln!("Excluded {} files already in archive(s)", archived.len());
        if options.show_archived {
            eprintln!("Archived files:");
            for (source_path, archive_path) in &archived {
                eprintln!("  {} -> {}", source_path, archive_path);
            }
        } else {
            eprintln!("Use --show-archived to list them");
        }
    }

    if sources.is_empty() {
        return Ok(None);
    }

    // Check for source duplicates (same content hash)
    if !options.allow_duplicates {
        let duplicate_groups = find_source_duplicates(&sources);
        if !duplicate_groups.is_empty() {
            let total_dup_sources: usize = duplicate_groups.iter().map(|(_, v)| v.len()).sum();
            bail!(
                "Found {} duplicate groups ({} sources with identical content)\n\
                 Use `canon ls --duplicates` to see details (supports [path] and --where filters).\n\
                 Use `canon exclude duplicates --prefer <path>` to resolve.\n\
                 Use --allow-duplicates to include them anyway.",
                duplicate_groups.len(),
                total_dup_sources
            );
        }
    }

    // Collect facts with 100% coverage
    let full_coverage_facts = collect_full_coverage_facts(conn, &sources)?;

    // Write JSONL lock file
    write_lock_file(lock_path, &sources, &full_coverage_facts)?;

    Ok(Some(LockGenerationResult {
        source_count: sources.len(),
        full_coverage_facts,
    }))
}

pub fn generate(
    db: &Db,
    scope_paths: &[PathBuf],
    filters: &[String],
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
    if scope_paths.is_empty() && filters.is_empty() {
        bail!("At least one of path or --where filter is required");
    }

    let conn = db.conn();

    // Resolve destination to archive root + relative subdir
    let (archive_root_id, _archive_root_path, base_dir) = resolve_archive_path(conn, dest)?;

    // Resolve scope paths to realpaths
    let scope_prefixes = canonicalize_scopes(scope_paths)?;

    let parsed_filters: Vec<Filter> = filters
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

    // Build config (TOML without sources)
    let config = ManifestConfig {
        meta: ManifestMeta {
            query: filters.to_vec(),
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
        output: ManifestOutput {
            pattern: "{filename}".to_string(),
            archive_root_id,
            base_dir,
        },
    };

    // Write TOML config file
    let toml_str =
        toml::to_string_pretty(&config).context("Failed to serialize manifest config")?;
    let toml_with_help = format!("{}\n\n{}", toml_str.trim_end(), fact_help);
    fs::write(output_path, &toml_with_help)
        .with_context(|| format!("Failed to write manifest to {}", output_path.display()))?;

    println!(
        "Generated manifest: {} ({} sources in {})",
        output_path.display(),
        result.source_count,
        lock_path.display()
    );

    Ok(())
}

pub fn refresh(db: &Db, config_path: &Path, options: &GenerateOptions) -> Result<()> {
    let conn = db.conn();

    // Read existing TOML config
    let config_content = fs::read_to_string(config_path)
        .with_context(|| format!("Failed to read config: {}", config_path.display()))?;
    let mut config: ManifestConfig = toml::from_str(&config_content)
        .with_context(|| format!("Failed to parse config: {}", config_path.display()))?;

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
    let result = generate_lock(conn, &scope_prefixes, &parsed_filters, &lock_path, options)?;

    match result {
        Some(r) => {
            // Compute hash of new lock file
            let lock_hash = hash_file(&lock_path)?;

            // Update config with new lock_hash and timestamp
            config.meta.lock_hash = lock_hash;
            config.meta.generated_at = current_timestamp();

            // Regenerate fact help and rewrite TOML
            let fact_help = generate_fact_help(r.source_count, &r.full_coverage_facts);
            let toml_str =
                toml::to_string_pretty(&config).context("Failed to serialize manifest config")?;
            let toml_with_help = format!("{}\n\n{}", toml_str.trim_end(), fact_help);
            fs::write(config_path, &toml_with_help)
                .with_context(|| format!("Failed to write config: {}", config_path.display()))?;

            println!(
                "Refreshed lock file: {} ({} sources)",
                lock_path.display(),
                r.source_count
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
fn write_lock_file(
    lock_path: &Path,
    sources: &[LockEntry],
    full_coverage_facts: &[(String, FactType, String)],
) -> Result<()> {
    let lock_file = File::create(lock_path)
        .with_context(|| format!("Failed to create lock file: {}", lock_path.display()))?;
    let mut writer = BufWriter::new(lock_file);

    // Get 100% coverage fact keys
    let full_coverage_keys: std::collections::HashSet<&str> = full_coverage_facts
        .iter()
        .map(|(k, _, _)| k.as_str())
        .collect();

    for source in sources {
        // Filter facts to only 100% coverage facts
        let filtered_facts: HashMap<String, serde_json::Value> = source
            .facts
            .iter()
            .filter(|(k, _)| full_coverage_keys.contains(k.as_str()))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        let entry = LockEntry {
            id: source.id,
            root_id: source.root_id,
            path: source.path.clone(),
            device: source.device,
            inode: source.inode,
            size: source.size,
            mtime: source.mtime,
            partial_hash: source.partial_hash.clone(),
            object_id: source.object_id,
            hash_type: source.hash_type.clone(),
            hash_value: source.hash_value.clone(),
            facts: filtered_facts,
        };
        serde_json::to_writer(&mut writer, &entry)
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

/// Returns (included_sources, archived_sources, excluded_count, unhashed_count)
/// archived_sources is a list of (source_path, archive_path) for files already in an archive
/// excluded_count is the number of sources skipped due to policy.exclude (hard gate)
/// unhashed_count is the number of sources skipped due to missing content hash
fn query_sources(
    conn: &Connection,
    scope_prefixes: &[String],
    filters: &[Filter],
    include_archived: bool,
) -> Result<(Vec<LockEntry>, Vec<(String, String)>, usize, usize)> {
    // Build query based on filters
    // By default only source roots, with --include-archived also include archive roots
    let role_clause = if include_archived {
        "1=1" // Include all roles
    } else {
        "r.role = 'source'"
    };

    let (scope_clause, scope_params) = build_scope_clause(scope_prefixes);
    let params: Vec<Value> = scope_params.iter().map(|s| Value::from(s.clone())).collect();

    let mut source_ids: Vec<i64> = conn
        .prepare(&format!(
            "SELECT s.id FROM sources s
             JOIN roots r ON s.root_id = r.id
             WHERE s.present = 1 AND {} AND {}",
            role_clause, scope_clause
        ))?
        .query_map(rusqlite::params_from_iter(params), |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()?;

    // Apply filters
    source_ids = filter::apply_filters(conn, &source_ids, filters)?;

    // Check which sources are already archived (same object_id exists in an archive root)
    // Also apply hard gates for excluded and unhashed sources
    let mut sources = Vec::new();
    let mut archived = Vec::new();
    let mut excluded_count = 0;
    let mut unhashed_count = 0;

    for source_id in source_ids {
        // HARD GATE: Skip excluded sources (no override flag)
        if exclude::is_excluded(conn, source_id)? {
            excluded_count += 1;
            continue;
        }

        if let Some(source) = fetch_source(conn, source_id)? {
            // Skip sources without content hash
            if source.object_id.is_none() {
                unhashed_count += 1;
                continue;
            }

            // Check if this content is already in an archive
            let archive_path = if let Some(ref hash) = source.hash_value {
                find_in_archive(conn, hash)?
            } else {
                None
            };

            if let Some(arch_path) = archive_path {
                if include_archived {
                    sources.push(source);
                } else {
                    archived.push((source.path.clone(), arch_path));
                }
            } else {
                sources.push(source);
            }
        }
    }

    Ok((sources, archived, excluded_count, unhashed_count))
}

/// Find if a hash exists in any archive root, return the path if found
fn find_in_archive(conn: &Connection, hash_value: &str) -> Result<Option<String>> {
    let result: Option<(String, String)> = conn
        .query_row(
            "SELECT r.path, s.rel_path
             FROM sources s
             JOIN roots r ON s.root_id = r.id
             JOIN objects o ON s.object_id = o.id
             WHERE r.role = 'archive' AND o.hash_value = ? AND s.present = 1
             LIMIT 1",
            [hash_value],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;

    Ok(result.map(|(root, rel)| {
        if rel.is_empty() {
            root
        } else {
            format!("{}/{}", root, rel)
        }
    }))
}

fn fetch_source(conn: &Connection, source_id: i64) -> Result<Option<LockEntry>> {
    let row: Option<(i64, i64, String, String, i64, i64, i64, i64, Option<String>, Option<i64>)> = conn
        .query_row(
            "SELECT s.id, s.root_id, r.path, s.rel_path, s.device, s.inode, s.size, s.mtime, s.partial_hash, s.object_id
             FROM sources s
             JOIN roots r ON s.root_id = r.id
             WHERE s.id = ?",
            [source_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?, row.get(7)?, row.get(8)?, row.get(9)?)),
        )
        .ok();

    let (id, root_id, root_path, rel_path, device, inode, size, mtime, partial_hash, object_id) = match row {
        Some(r) => r,
        None => return Ok(None),
    };

    // Require partial_hash - sources without it need to be rescanned
    let partial_hash = partial_hash.ok_or_else(|| {
        anyhow::anyhow!(
            "Source {} has no partial_hash. Run `canon scan <path>` to rescan.",
            source_id
        )
    })?;

    let full_path = if rel_path.is_empty() {
        root_path
    } else {
        format!("{}/{}", root_path, rel_path)
    };

    // Get hash if available
    let (hash_type, hash_value): (Option<String>, Option<String>) = if let Some(obj_id) = object_id {
        conn.query_row(
            "SELECT hash_type, hash_value FROM objects WHERE id = ?",
            [obj_id],
            |row| Ok((Some(row.get(0)?), Some(row.get(1)?))),
        )
        .unwrap_or((None, None))
    } else {
        (None, None)
    };

    // Collect facts
    let mut facts = HashMap::new();

    // Source facts
    let mut stmt = conn.prepare(
        "SELECT key, value_text, value_num, value_time
         FROM facts WHERE entity_type = 'source' AND entity_id = ?"
    )?;
    for row in stmt.query_map([source_id], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<f64>>(2)?,
            row.get::<_, Option<i64>>(3)?,
        ))
    })? {
        let (key, text, num, time) = row?;
        let value = fact_to_json(text, num, time);
        facts.insert(key, value);
    }

    // Object facts
    if let Some(obj_id) = object_id {
        let mut stmt = conn.prepare(
            "SELECT key, value_text, value_num, value_time
             FROM facts WHERE entity_type = 'object' AND entity_id = ?"
        )?;
        for row in stmt.query_map([obj_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<f64>>(2)?,
                row.get::<_, Option<i64>>(3)?,
            ))
        })? {
            let (key, text, num, time) = row?;
            let value = fact_to_json(text, num, time);
            facts.insert(key, value);
        }
    }

    Ok(Some(LockEntry {
        id,
        root_id,
        path: full_path,
        device,
        inode,
        size,
        mtime,
        partial_hash,
        object_id,
        hash_type,
        hash_value,
        facts,
    }))
}

fn fact_to_json(
    text: Option<String>,
    num: Option<f64>,
    time: Option<i64>,
) -> serde_json::Value {
    if let Some(t) = text {
        serde_json::Value::String(t)
    } else if let Some(n) = num {
        serde_json::json!(n)
    } else if let Some(t) = time {
        serde_json::json!(t)
    } else {
        serde_json::Value::Null
    }
}

fn current_timestamp() -> String {
    chrono::Utc::now().to_rfc3339()
}


/// Track types seen for a fact key
#[derive(Default)]
struct FactTypeTracker {
    count: usize,
    text_count: usize,
    num_count: usize,
    time_count: usize,
}

impl FactTypeTracker {
    fn add(&mut self, fact_type: FactType) {
        self.count += 1;
        match fact_type {
            FactType::Text | FactType::Path => self.text_count += 1,
            FactType::Num => self.num_count += 1,
            FactType::Time => self.time_count += 1,
        }
    }

    fn has_mixed_types(&self) -> bool {
        let type_count = (self.text_count > 0) as usize
            + (self.num_count > 0) as usize
            + (self.time_count > 0) as usize;
        type_count > 1
    }

    fn dominant_type(&self) -> FactType {
        if self.time_count >= self.text_count && self.time_count >= self.num_count {
            FactType::Time
        } else if self.num_count >= self.text_count {
            FactType::Num
        } else {
            FactType::Text
        }
    }

    fn type_breakdown(&self) -> String {
        let mut parts = Vec::new();
        if self.time_count > 0 {
            parts.push(format!("{} time", self.time_count));
        }
        if self.text_count > 0 {
            parts.push(format!("{} text", self.text_count));
        }
        if self.num_count > 0 {
            parts.push(format!("{} num", self.num_count));
        }
        parts.join(", ")
    }
}

/// Collect facts with 100% coverage across all sources in the manifest
fn collect_full_coverage_facts(conn: &Connection, sources: &[LockEntry]) -> Result<Vec<(String, FactType, String)>> {
    use std::collections::HashSet;

    if sources.is_empty() {
        return Ok(Vec::new());
    }

    let source_count = sources.len();
    let source_ids: Vec<i64> = sources.iter().map(|s| s.id).collect();

    // Count facts by key across all sources, tracking type consistency
    let mut fact_counts: HashMap<String, FactTypeTracker> = HashMap::new();
    let mut seen_keys: HashSet<String> = HashSet::new();

    // Query source facts
    for source_id in &source_ids {
        let mut stmt = conn.prepare(
            "SELECT key, value_text, value_num, value_time
             FROM facts WHERE entity_type = 'source' AND entity_id = ?"
        )?;

        for row in stmt.query_map([source_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<f64>>(2)?,
                row.get::<_, Option<i64>>(3)?,
            ))
        })? {
            let (key, text, num, time) = row?;
            let fact_type = if text.is_some() {
                FactType::Text
            } else if num.is_some() {
                FactType::Num
            } else if time.is_some() {
                FactType::Time
            } else {
                continue;
            };

            let seen_key = format!("{}:{}", source_id, key);
            if !seen_keys.contains(&seen_key) {
                fact_counts.entry(key.clone()).or_default().add(fact_type);
                seen_keys.insert(seen_key);
            }
        }
    }

    // Query object facts (only for sources that have objects)
    for (source, object_id) in sources.iter().filter_map(|s| s.object_id.map(|oid| (s, oid))) {
        let mut stmt = conn.prepare(
            "SELECT key, value_text, value_num, value_time
             FROM facts WHERE entity_type = 'object' AND entity_id = ?"
        )?;

        for row in stmt.query_map([object_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<f64>>(2)?,
                row.get::<_, Option<i64>>(3)?,
            ))
        })? {
            let (key, text, num, time) = row?;
            let fact_type = if text.is_some() {
                FactType::Text
            } else if num.is_some() {
                FactType::Num
            } else if time.is_some() {
                FactType::Time
            } else {
                continue;
            };

            // Use source.id for uniqueness, not object_id (since we want per-source coverage)
            let seen_key = format!("{}:{}", source.id, key);
            if !seen_keys.contains(&seen_key) {
                fact_counts.entry(key.clone()).or_default().add(fact_type);
                seen_keys.insert(seen_key);
            }
        }
    }

    // Warn about facts with mixed types (only for 100% coverage facts)
    let mut mixed_type_warnings: Vec<(String, String)> = Vec::new();
    for (key, tracker) in &fact_counts {
        if tracker.count == source_count && tracker.has_mixed_types() {
            mixed_type_warnings.push((key.clone(), tracker.type_breakdown()));
        }
    }

    if !mixed_type_warnings.is_empty() {
        mixed_type_warnings.sort_by(|a, b| a.0.cmp(&b.0));
        eprintln!("Warning: some facts have inconsistent types across sources:");
        for (key, breakdown) in &mixed_type_warnings {
            eprintln!("  {}: {}", key, breakdown);
        }
        eprintln!("  Type-specific modifiers (|year, |month, etc.) may fail on mismatched values.");
        eprintln!("  To fix: delete outliers with 'canon facts delete <key> --on object --value-type <minority-type>'");
    }

    // Filter to only 100% coverage facts
    let mut full_coverage: Vec<(String, FactType, String)> = fact_counts
        .into_iter()
        .filter(|(_, tracker)| tracker.count == source_count)
        .map(|(key, tracker)| {
            let description = get_fact_description(&key);
            (key, tracker.dominant_type(), description)
        })
        .collect();

    // Sort by key for consistent output
    full_coverage.sort_by(|a, b| a.0.cmp(&b.0));

    Ok(full_coverage)
}

/// Get a human-readable description for a fact key
fn get_fact_description(key: &str) -> String {
    BuiltinKey::from_str(key)
        .and_then(|k| k.description())
        .map(|s| s.to_string())
        .unwrap_or_default()
}

/// Generate fact help comments for the manifest
fn generate_fact_help(source_count: usize, full_coverage_facts: &[(String, FactType, String)]) -> String {
    use strum::IntoEnumIterator;

    if source_count == 0 {
        return String::new();
    }

    let mut help = String::new();
    help.push_str(&format!("# Available facts for pattern (100% coverage on {} sources in this cluster):\n", source_count));
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
        help.push_str(&format!("#   {:18} {:6} - {}\n", name, key.fact_type().as_str(), desc));
    }
    help.push_str(&format!("#   {:18} {:6} - {}\n", "object.hash", "text", "Content hash (if hashed)"));
    help.push_str("#\n");

    // User facts with 100% coverage
    if !full_coverage_facts.is_empty() {
        help.push_str("# Content facts:\n");
        for (key, fact_type, description) in full_coverage_facts {
            let desc_part = if description.is_empty() {
                String::new()
            } else {
                format!(" - {}", description)
            };
            help.push_str(&format!("#   {:18} {:6}{}\n", key, fact_type.as_str(), desc_part));
        }
        help.push_str("#\n");
    }

    // Modifiers reference (auto-generated from Modifier enum)
    let time_mods: Vec<_> = Modifier::iter()
        .filter(|m| m.category() == ModifierCategory::Time)
        .map(|m| { let name: &'static str = m.into(); format!("|{}", name) })
        .collect();
    let string_mods: Vec<_> = Modifier::iter()
        .filter(|m| m.category() == ModifierCategory::String)
        .map(|m| { let name: &'static str = m.into(); format!("|{}", name) })
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
            help.push_str(&format!("#   {{{}}}  →  {{{}}}\n", name, expansion));
        }
    }
    help.push_str("\n");

    help
}

/// Find duplicate sources (same object_id) within the manifest sources
/// Returns Vec of (object_id, Vec<source_id>)
fn find_source_duplicates(sources: &[LockEntry]) -> Vec<(i64, Vec<i64>)> {
    let mut object_map: HashMap<i64, Vec<i64>> = HashMap::new();

    for source in sources {
        if let Some(object_id) = source.object_id {
            object_map
                .entry(object_id)
                .or_default()
                .push(source.id);
        }
    }

    // Return only groups with 2+ sources
    object_map
        .into_iter()
        .filter(|(_, ids)| ids.len() > 1)
        .collect()
}
