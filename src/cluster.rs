use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use crate::domain::path::canonicalize_scopes;
use crate::domain::root::resolve_archive_path;
use crate::domain::scope::ScopeMatch;
use crate::domain::source::Source;
use crate::domain::{FactEntry, FactValue};
use crate::expr::filter::{self, Filter};
use crate::expr::{BuiltinKey, BuiltinKeyVisibility, FactType, Modifier, ModifierCategory};
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
}

/// Result from generating a lock file
struct LockGenerationResult {
    source_count: usize,
    full_coverage_facts: Vec<(String, FactType, String)>,
    root_breakdown: Vec<(String, usize)>,  // (root_path, count), sorted by path
    not_archived_count: usize,             // sources with no archived copy
    excluded_count: usize,                 // skipped excluded sources
    unhashed_count: usize,                 // skipped unhashed sources
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
    let qr = query_sources(conn, scope_prefixes, filters, options.allow_archived)?;

    // Report archived files
    if !qr.archived.is_empty() {
        eprintln!("Excluded {} files already in archive(s)", qr.archived.len());
        if options.show_archived {
            eprintln!("Archived files:");
            for (source_path, archive_path) in &qr.archived {
                eprintln!("  {source_path} -> {archive_path}");
            }
        } else {
            eprintln!("Use --show-archived to list them");
        }
    }

    if qr.sources.is_empty() {
        return Ok(None);
    }

    // Check for source duplicates (same content hash)
    if !options.allow_duplicates {
        let duplicate_groups = find_source_duplicates(&qr.sources);
        if !duplicate_groups.is_empty() {
            let total_dup_sources: usize = duplicate_groups.iter().map(|(_, v)| v.len()).sum();
            bail!(
                "Found {} duplicate groups ({} sources with identical content)\n\
                 Use `canon ls --duplicates` to see details (supports [path] and --where filters).\n\
                 Use `canon exclude duplicates --prefer <path>` to resolve.\n\
                 Use --allow duplicates to include them.",
                duplicate_groups.len(),
                total_dup_sources
            );
        }
    }

    // Collect facts with 100% coverage (using typed facts from batch fetch)
    let full_coverage_facts = collect_full_coverage_facts(&qr.sources, &qr.all_facts);

    // Write JSONL lock file (no facts — apply looks them up at runtime)
    write_lock_file(lock_path, &qr.sources)?;

    Ok(Some(LockGenerationResult {
        source_count: qr.sources.len(),
        full_coverage_facts,
        root_breakdown: qr.root_breakdown,
        not_archived_count: qr.not_archived_count,
        excluded_count: qr.excluded_count,
        unhashed_count: qr.unhashed_count,
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

    // Resolve scope paths to realpaths
    let scope_prefixes = canonicalize_scopes(scope_paths)?;

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

/// Result from querying and filtering sources for a cluster.
struct QuerySourcesResult {
    /// Sources included in the cluster (as lock entries)
    sources: Vec<LockEntry>,
    /// Sources excluded because they're already archived: (source_path, archive_path)
    archived: Vec<(String, String)>,
    /// Number of sources skipped due to exclusion (hard gate)
    excluded_count: usize,
    /// Number of sources skipped due to missing content hash
    unhashed_count: usize,
    /// Typed FactEntry values keyed by source_id (for 100% coverage computation)
    all_facts: HashMap<i64, Vec<FactEntry>>,
    /// Root breakdown: (root_path, count) sorted by path
    root_breakdown: Vec<(String, usize)>,
    /// Number of sources in the cluster with no archived copy
    not_archived_count: usize,
}

fn query_sources(
    conn: &mut Connection,
    scope_prefixes: &[String],
    filters: &[Filter],
    allow_archived: bool,
) -> Result<QuerySourcesResult> {
    // 1. Get all root IDs
    let root_ids: Vec<i64> = conn
        .prepare("SELECT id FROM roots")?
        .query_map([], |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()?;

    // 2. Batch fetch all present sources for those roots
    let all_sources = repo::source::batch_fetch_by_roots(conn, &root_ids)?;

    // 3. Classify scopes for matching
    let scopes = ScopeMatch::classify_all(scope_prefixes);

    // 4. Filter using domain predicates, tracking excluded count
    let mut excluded_count = 0usize;
    let filtered: Vec<_> = all_sources
        .into_iter()
        .filter(|s| s.is_active())
        .filter(|s| allow_archived || s.is_from_role("source"))
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
    let object_ids: Vec<i64> = hashed_sources.iter().filter_map(|s| s.object_id).collect();
    let objects = repo::object::batch_fetch_by_ids(conn, &object_ids)?;

    // 8. Batch fetch archive paths for all objects (eliminates N+1 query)
    let archive_paths = repo::object::batch_find_archive_paths(conn, &object_ids)?;

    // 9. Batch fetch all facts for all sources
    let source_ids: Vec<i64> = hashed_sources.iter().map(|s| s.id).collect();
    let all_facts = repo::fact::batch_fetch_for_sources(conn, &source_ids)?;

    // 10. Collect root paths from sources (before consuming them)
    let mut root_path_map: HashMap<i64, String> = HashMap::new();
    for source in &hashed_sources {
        root_path_map
            .entry(source.root_id)
            .or_insert_with(|| source.root_path.clone());
    }

    // 11. Build LockEntry for each source, check archive status
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
            if allow_archived {
                sources.push(lock_entry);
            } else {
                archived.push((lock_entry.path.clone(), arch_path));
            }
        } else {
            sources.push(lock_entry);
        }
    }

    // 12. Compute root breakdown from final sources
    let mut root_counts: HashMap<i64, usize> = HashMap::new();
    for source in &sources {
        *root_counts.entry(source.root_id).or_insert(0) += 1;
    }
    let mut root_breakdown: Vec<(String, usize)> = root_counts
        .into_iter()
        .filter_map(|(root_id, count)| {
            root_path_map.get(&root_id).map(|path| (path.clone(), count))
        })
        .collect();
    root_breakdown.sort_by(|a, b| a.0.cmp(&b.0));

    // 13. Count sources with no archived copy (from final sources list)
    let not_archived_count = sources
        .iter()
        .filter(|s| {
            s.object_id
                .and_then(|oid| archive_paths.get(&oid))
                .map(|paths| paths.is_empty())
                .unwrap_or(true) // no object_id = not archived
        })
        .count();

    Ok(QuerySourcesResult {
        sources,
        archived,
        excluded_count,
        unhashed_count,
        all_facts,
        root_breakdown,
        not_archived_count,
    })
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

fn parse_manifest_allow(allow: &[String]) -> Result<(bool, bool)> {
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
                    fact_counts
                        .entry(fact.key.clone())
                        .or_default()
                        .add(fact_type);
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
            eprintln!("  {key}: {breakdown}");
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

/// Find duplicate sources (same object_id) within the manifest sources
/// Returns Vec of (object_id, Vec<source_id>)
fn find_source_duplicates(sources: &[LockEntry]) -> Vec<(i64, Vec<i64>)> {
    let mut object_map: HashMap<i64, Vec<i64>> = HashMap::new();

    for source in sources {
        if let Some(object_id) = source.object_id {
            object_map.entry(object_id).or_default().push(source.id);
        }
    }

    // Return only groups with 2+ sources
    object_map
        .into_iter()
        .filter(|(_, ids)| ids.len() > 1)
        .collect()
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
    if result.excluded_count > 0 || result.unhashed_count > 0 {
        s.push_str("#\n");
        let mut parts = Vec::new();
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
    use crate::repo::open_in_memory_for_test;
    use rusqlite::Connection as RusqliteConnection;

    fn setup_test_db() -> RusqliteConnection {
        open_in_memory_for_test()
    }

    fn insert_root(conn: &RusqliteConnection, path: &str, role: &str, suspended: bool) -> i64 {
        conn.execute(
            "INSERT INTO roots (path, role, suspended) VALUES (?, ?, ?)",
            rusqlite::params![path, role, suspended as i64],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn insert_object(conn: &RusqliteConnection, hash: &str, excluded: bool) -> i64 {
        conn.execute(
            "INSERT INTO objects (hash_type, hash_value, excluded) VALUES ('sha256', ?, ?)",
            rusqlite::params![hash, excluded as i64],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn insert_source(
        conn: &RusqliteConnection,
        root_id: i64,
        rel_path: &str,
        object_id: Option<i64>,
        excluded: bool,
    ) -> i64 {
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, object_id, size, mtime, partial_hash, scanned_at, last_seen_at, device, inode, excluded)
             VALUES (?, ?, ?, 1000, 1704067200, '', 0, 0, 0, 0, ?)",
            rusqlite::params![root_id, rel_path, object_id, excluded as i64],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    /// Test that sources from suspended roots are excluded from manifest generation.
    #[test]
    fn test_cluster_excludes_suspended_roots() {
        let mut conn = setup_test_db();

        // Create active and suspended source roots
        let active_root = insert_root(&conn, "/active", "source", false);
        let suspended_root = insert_root(&conn, "/suspended", "source", true);

        // Create objects for hashing
        let obj1 = insert_object(&conn, "hash1", false);
        let obj2 = insert_object(&conn, "hash2", false);

        // Insert sources in both roots
        insert_source(&conn, active_root, "file1.jpg", Some(obj1), false);
        insert_source(&conn, suspended_root, "file2.jpg", Some(obj2), false);

        // Query sources (this is what cluster generate does)
        let qr = query_sources(&mut conn, &[], &[], false).unwrap();

        // Should only include source from active root
        assert_eq!(
            qr.sources.len(),
            1,
            "Should exclude sources from suspended roots"
        );
        assert_eq!(qr.sources[0].path, "/active/file1.jpg");
    }

    /// Test that excluded sources are filtered out at both source and object level.
    #[test]
    fn test_cluster_excludes_excluded_sources() {
        let mut conn = setup_test_db();

        let root = insert_root(&conn, "/photos", "source", false);

        // Normal source
        let normal_obj = insert_object(&conn, "normal_hash", false);
        insert_source(&conn, root, "normal.jpg", Some(normal_obj), false);

        // Source-level excluded
        let source_excl_obj = insert_object(&conn, "source_excl_hash", false);
        insert_source(
            &conn,
            root,
            "source_excluded.jpg",
            Some(source_excl_obj),
            true,
        );

        // Object-level excluded (source not excluded, but object is)
        let object_excl_obj = insert_object(&conn, "object_excl_hash", true);
        insert_source(
            &conn,
            root,
            "object_excluded.jpg",
            Some(object_excl_obj),
            false,
        );

        // Query sources
        let qr = query_sources(&mut conn, &[], &[], false).unwrap();

        // Should only include the normal source
        assert_eq!(
            qr.sources.len(),
            1,
            "Should exclude both source-level and object-level excluded"
        );
        assert_eq!(qr.sources[0].path, "/photos/normal.jpg");
        assert_eq!(qr.excluded_count, 2, "Should count both excluded sources");
    }

    /// Test that archive detection handles multiple sources pointing to same object.
    ///
    /// When 3 sources point to 1 archived object, all 3 should be marked as
    /// "already archived" (not just 1).
    #[test]
    fn test_cluster_archive_detection_counts_sources_not_objects() {
        let mut conn = setup_test_db();

        // Create source root and archive root
        let source_root = insert_root(&conn, "/photos", "source", false);
        let archive_root = insert_root(&conn, "/archive", "archive", false);

        // Create ONE object that will be archived
        let archived_obj = insert_object(&conn, "archived_hash", false);

        // Create 3 source files pointing to the SAME object
        insert_source(&conn, source_root, "photo1.jpg", Some(archived_obj), false);
        insert_source(&conn, source_root, "photo2.jpg", Some(archived_obj), false);
        insert_source(&conn, source_root, "photo3.jpg", Some(archived_obj), false);

        // Create another object that is NOT archived
        let unarchived_obj = insert_object(&conn, "unarchived_hash", false);
        insert_source(
            &conn,
            source_root,
            "photo4.jpg",
            Some(unarchived_obj),
            false,
        );

        // Put the first object in archive
        insert_source(&conn, archive_root, "backup.jpg", Some(archived_obj), false);

        // Query sources WITHOUT include_archived flag (default behavior)
        let qr = query_sources(&mut conn, &[], &[], false).unwrap();

        // The critical assertion: all 3 sources pointing to archived object should be
        // in the "archived" list, not just 1
        assert_eq!(
            qr.archived.len(),
            3,
            "Should detect 3 SOURCES as already archived, not 1 unique object"
        );

        // Only the unarchived source should be in the main sources list
        assert_eq!(
            qr.sources.len(),
            1,
            "Only unarchived source should be in sources"
        );
        assert_eq!(qr.sources[0].path, "/photos/photo4.jpg");
    }

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
        assert!(config.options.allow.is_empty(), "Options should default to empty");
    }

    #[test]
    fn test_manifest_options_invalid_allow() {
        let result = parse_manifest_allow(&["bogus".to_string()]);
        assert!(result.is_err(), "Should error on invalid allow value");
        let err = result.unwrap_err().to_string();
        assert!(err.contains("bogus"), "Error should mention the invalid value");
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
        let content = "# === Notes ===\n# This cluster has family photos\n# from 2020-2023\n\n[meta]\n";
        let notes = extract_notes(content).unwrap();
        assert_eq!(notes, "\n# This cluster has family photos\n# from 2020-2023\n\n");
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
            root_breakdown: vec![
                ("/backup".to_string(), 50),
                ("/photos".to_string(), 100),
            ],
            not_archived_count: 120,
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
            excluded_count: 3,
            unhashed_count: 5,
        };
        let summary = generate_summary_comments(&result);
        assert!(summary.contains("# Skipped: 3 excluded, 5 unhashed"));
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
            options: ManifestOptions {
                allow: vec![],
            },
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
