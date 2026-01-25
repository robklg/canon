use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use crate::db::{Connection, Db};
use crate::fact::{FactEntry, FactValue};
use crate::fact_repo;
use crate::object_repo;
use crate::root::resolve_archive_path;
use crate::scope::ScopeMatch;
use crate::path::canonicalize_scopes;
use crate::source::Source;
use crate::source_repo;
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
    // Note: `facts` field was removed. Apply looks up facts at runtime from DB.
    // Old lock files with `facts` field are still readable (serde ignores unknown fields).
}

impl LockEntry {
    /// Build a LockEntry from a Source and object hash info.
    pub fn from_source(source: &Source, hash_type: Option<String>, hash_value: Option<String>) -> Self {
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
    conn: &mut Connection,
    scope_prefixes: &[String],
    filters: &[Filter],
    lock_path: &Path,
    options: &GenerateOptions,
) -> Result<Option<LockGenerationResult>> {
    let (sources, archived, excluded_count, unhashed_count, all_facts) =
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

    // Collect facts with 100% coverage (using typed facts from batch fetch)
    let full_coverage_facts = collect_full_coverage_facts(&sources, &all_facts);

    // Write JSONL lock file (no facts — apply looks them up at runtime)
    write_lock_file(lock_path, &sources)?;

    Ok(Some(LockGenerationResult {
        source_count: sources.len(),
        full_coverage_facts,
    }))
}

pub fn generate(
    db: &mut Db,
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

    let conn = db.conn_mut();

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

pub fn refresh(db: &mut Db, config_path: &Path, options: &GenerateOptions) -> Result<()> {
    let conn = db.conn_mut();

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

/// Returns (included_sources, archived_sources, excluded_count, unhashed_count, all_facts)
/// archived_sources is a list of (source_path, archive_path) for files already in an archive
/// excluded_count is the number of sources skipped due to exclusion (hard gate)
/// unhashed_count is the number of sources skipped due to missing content hash
/// all_facts contains typed FactEntry values keyed by source_id (for 100% coverage computation)
fn query_sources(
    conn: &mut Connection,
    scope_prefixes: &[String],
    filters: &[Filter],
    include_archived: bool,
) -> Result<(Vec<LockEntry>, Vec<(String, String)>, usize, usize, HashMap<i64, Vec<FactEntry>>)> {
    // 1. Get all root IDs
    let root_ids: Vec<i64> = conn
        .prepare("SELECT id FROM roots")?
        .query_map([], |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()?;

    // 2. Batch fetch all present sources for those roots
    let all_sources = source_repo::batch_fetch_by_roots(conn, &root_ids)?;

    // 3. Classify scopes for matching
    let scopes = ScopeMatch::classify_all(scope_prefixes);

    // 4. Filter using domain predicates, tracking excluded count
    let mut excluded_count = 0usize;
    let filtered: Vec<_> = all_sources
        .into_iter()
        .filter(|s| s.is_active())
        .filter(|s| include_archived || s.is_from_role("source"))
        .filter(|s| s.matches_scope(&scopes))
        .filter(|s| {
            if s.is_excluded() {
                excluded_count += 1;
                false
            } else {
                true
            }
        })
        .collect();

    // 5. Apply --where filters if present
    let filtered_sources = if filters.is_empty() {
        filtered
    } else {
        let source_ids: Vec<i64> = filtered.iter().map(|s| s.id).collect();
        let filtered_ids = filter::apply_filters(conn, &source_ids, filters)?;
        let filtered_id_set: HashSet<i64> = filtered_ids.into_iter().collect();
        filtered
            .into_iter()
            .filter(|s| filtered_id_set.contains(&s.id))
            .collect()
    };

    // 6. Separate hashed from unhashed sources
    let mut unhashed_count = 0;
    let hashed_sources: Vec<Source> = filtered_sources
        .into_iter()
        .filter(|s| {
            if s.object_id.is_none() {
                unhashed_count += 1;
                false
            } else {
                true
            }
        })
        .collect();

    // 7. Batch fetch objects for all sources with object_id
    let object_ids: Vec<i64> = hashed_sources
        .iter()
        .filter_map(|s| s.object_id)
        .collect();
    let objects = object_repo::batch_fetch_by_ids(conn, &object_ids)?;

    // 8. Batch fetch archive paths for all objects (eliminates N+1 query)
    let archive_paths = object_repo::batch_find_archive_paths(conn, &object_ids)?;

    // 9. Batch fetch all facts for all sources
    let source_ids: Vec<i64> = hashed_sources.iter().map(|s| s.id).collect();
    let all_facts = fact_repo::batch_fetch_for_sources(conn, &source_ids)?;

    // 10. Build LockEntry for each source, check archive status
    let mut sources = Vec::new();
    let mut archived = Vec::new();

    for source in hashed_sources {
        // Get hash info from batch result
        let (hash_type, hash_value) = source
            .object_id
            .and_then(|oid| objects.get(&oid))
            .map(|obj| (Some(obj.hash_type.clone()), Some(obj.hash_value.clone())))
            .unwrap_or((None, None));

        // Check if this content is already in an archive (from batch result)
        let archive_path = source
            .object_id
            .and_then(|oid| archive_paths.get(&oid))
            .and_then(|paths| paths.first())
            .cloned();

        // Build LockEntry from Source + hash info (no facts — apply looks them up at runtime)
        let lock_entry = LockEntry::from_source(&source, hash_type, hash_value);

        if let Some(arch_path) = archive_path {
            if include_archived {
                sources.push(lock_entry);
            } else {
                archived.push((lock_entry.path.clone(), arch_path));
            }
        } else {
            sources.push(lock_entry);
        }
    }

    Ok((sources, archived, excluded_count, unhashed_count, all_facts))
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

/// Collect facts with 100% coverage across all sources in the manifest.
/// Uses pre-fetched typed facts from `all_facts` (keyed by source_id).
fn collect_full_coverage_facts(
    sources: &[LockEntry],
    all_facts: &HashMap<i64, Vec<FactEntry>>,
) -> Vec<(String, FactType, String)> {
    use std::collections::HashSet;

    if sources.is_empty() {
        return Vec::new();
    }

    let source_count = sources.len();

    // Count facts by key across all sources, tracking type consistency
    let mut fact_counts: HashMap<String, FactTypeTracker> = HashMap::new();
    let mut seen_keys: HashSet<String> = HashSet::new();

    // Iterate over pre-fetched facts (already merged source + object facts by source_id)
    for source in sources {
        if let Some(facts) = all_facts.get(&source.id) {
            for fact in facts {
                // Derive FactType from the typed FactValue (preserves Time vs Num distinction)
                let fact_type = match &fact.value {
                    FactValue::Text(_) => FactType::Text,
                    FactValue::Num(_) => FactType::Num,
                    FactValue::Time(_) => FactType::Time,
                    FactValue::Path(_) => FactType::Path,
                };

                // Track uniqueness per source (a source might have same key from both source and object)
                let seen_key = format!("{}:{}", source.id, fact.key);
                if !seen_keys.contains(&seen_key) {
                    fact_counts.entry(fact.key.clone()).or_default().add(fact_type);
                    seen_keys.insert(seen_key);
                }
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

    full_coverage
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
