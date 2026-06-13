//! Cluster operations — plan and execute for cluster generation.
//!
//! `plan_generate()` computes what `cluster generate` would produce — source
//! selection, archive detection, duplicate checking, and fact coverage
//! computation — without any side effects.
//!
//! `execute_generate()` and `execute_refresh()` write the lock file and manifest
//! to disk. Manifests are stored artifacts (not presentation) so this logic
//! belongs in the ops layer, not the interface.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

// ============================================================================
// Manifest data contract (shared between cluster generate and apply)
// ============================================================================

/// TOML manifest config file structure.
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

use crate::domain::include::IncludeSet;
use crate::domain::scope::ScopeMatch;
use crate::domain::source::Source;
use crate::domain::{FactEntry, FactValue};
use crate::expr::filter::Filter;
use crate::expr::{BuiltinKey, FactType};
use crate::ops::selection::{self, RolePolicy, SelectionParams};
use crate::repo::{self, Connection};

// ============================================================================
// Types
// ============================================================================

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

// ============================================================================
// Types
// ============================================================================

/// Parameters for planning a cluster generation.
pub struct ClusterGenerateParams {
    pub scopes: Vec<ScopeMatch>,
    pub filters: Vec<Filter>,
    pub allow_archived: bool,
    pub allow_duplicates: bool,
}

/// Computed plan for cluster generation. Contains all data the interface
/// needs for lock file writing, manifest assembly, and display —
/// no further queries needed.
#[derive(Debug)]
pub struct ClusterGeneratePlan {
    /// Lock entries for the manifest (sources to archive).
    pub lock_entries: Vec<LockEntry>,
    /// Sources skipped because already in archive: (source_path, archive_path).
    pub archived: Vec<(String, String)>,
    /// Facts with 100% coverage across all lock entries: (key, type, description).
    pub full_coverage_facts: Vec<(String, FactType, String)>,
    /// Fact keys with mixed types across sources: (key, type_breakdown_string).
    pub mixed_type_warnings: Vec<(String, String)>,
    /// Root breakdown: (root_path, count) sorted by path.
    pub root_breakdown: Vec<(String, usize)>,
    /// Sources in plan with no archived copy.
    pub not_archived_count: usize,
    /// Number of excluded sources skipped.
    pub excluded_count: usize,
    /// Number of unhashed sources skipped.
    pub unhashed_count: usize,
}

// ============================================================================
// Plan function
// ============================================================================

/// Compute what `cluster generate` would produce — no side effects.
///
/// Selects sources via `select_sources()`, separates hashed/unhashed,
/// detects archive status, checks for duplicates (returns Err if found
/// and not allowed), computes full-coverage facts, and returns a plan
/// with all data needed for lock file writing and manifest assembly.
pub fn plan_generate(
    conn: &mut Connection,
    params: &ClusterGenerateParams,
) -> Result<ClusterGeneratePlan> {
    // 1. Select sources using the standard selection contract
    let selection_params = SelectionParams {
        scopes: params.scopes.clone(),
        include: IncludeSet {
            excluded: false,
            archived: params.allow_archived,
        },
        filters: params.filters.clone(),
        role_policy: RolePolicy::SourceUnlessIncluded,
    };
    let selection = selection::select_sources(conn, &selection_params)?;

    // 2. Separate hashed from unhashed sources
    let mut unhashed_count = 0usize;
    let hashed_sources: Vec<_> = selection
        .sources
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

    // 3. Batch fetch objects for hash info
    let object_ids: Vec<i64> = hashed_sources.iter().filter_map(|s| s.object_id).collect();
    let objects = repo::object::batch_fetch_by_ids(conn, &object_ids)?;

    // 4. Batch fetch archive paths for archive detection
    let archive_paths = repo::object::batch_find_archive_paths(conn, &object_ids)?;

    // 5. Collect root paths from sources (before consuming them)
    let mut root_path_map: HashMap<i64, String> = HashMap::new();
    for source in &hashed_sources {
        root_path_map
            .entry(source.root_id)
            .or_insert_with(|| source.root_path.clone());
    }

    // 6. Build lock entries, separating archived from non-archived
    let mut lock_entries = Vec::new();
    let mut archived = Vec::new();

    for source in &hashed_sources {
        let (hash_type, hash_value) = source
            .object_id
            .and_then(|oid| objects.get(&oid))
            .map(|obj| (Some(obj.hash_type.clone()), Some(obj.hash_value.clone())))
            .unwrap_or((None, None));

        let archive_path = source
            .object_id
            .and_then(|oid| archive_paths.get(&oid))
            .and_then(|paths| paths.first())
            .cloned();

        let lock_entry = LockEntry::from_source(source, hash_type, hash_value);

        if let Some(arch_path) = archive_path {
            if params.allow_archived {
                lock_entries.push(lock_entry);
            } else {
                archived.push((lock_entry.path.clone(), arch_path));
            }
        } else {
            lock_entries.push(lock_entry);
        }
    }

    // 7. Check for duplicates (hard gate)
    if !params.allow_duplicates {
        let duplicate_groups = find_source_duplicates(&lock_entries);
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

    // 8. Batch fetch facts for full-coverage computation
    let source_ids: Vec<i64> = lock_entries.iter().map(|s| s.id).collect();
    let all_facts = repo::fact::batch_fetch_for_sources(conn, &source_ids)?;

    // 9. Compute full coverage facts and mixed-type warnings
    let (full_coverage_facts, mixed_type_warnings) =
        compute_full_coverage_facts(&lock_entries, &all_facts);

    // 10. Compute root breakdown from final lock entries
    let mut root_counts: HashMap<i64, usize> = HashMap::new();
    for entry in &lock_entries {
        *root_counts.entry(entry.root_id).or_insert(0) += 1;
    }
    let mut root_breakdown: Vec<(String, usize)> = root_counts
        .into_iter()
        .filter_map(|(root_id, count)| {
            root_path_map
                .get(&root_id)
                .map(|path| (path.clone(), count))
        })
        .collect();
    root_breakdown.sort_by(|a, b| a.0.cmp(&b.0));

    // 11. Compute not-archived count from final lock entries
    let not_archived_count = lock_entries
        .iter()
        .filter(|s| {
            s.object_id
                .and_then(|oid| archive_paths.get(&oid))
                .map(|paths| paths.is_empty())
                .unwrap_or(true)
        })
        .count();

    Ok(ClusterGeneratePlan {
        lock_entries,
        archived,
        full_coverage_facts,
        mixed_type_warnings,
        root_breakdown,
        not_archived_count,
        excluded_count: selection.excluded_count,
        unhashed_count,
    })
}

// ============================================================================
// Execute types
// ============================================================================

/// Parameters for executing a cluster generation (writing lock file + manifest).
pub struct ExecuteGenerateParams {
    pub lock_path: PathBuf,
    pub manifest_path: PathBuf,
    pub expanded_filters: Vec<String>,
    pub original_filters: Vec<String>,
    pub scope_prefixes: Vec<String>,
    pub archive_root_id: i64,
    pub base_dir: String,
    pub allow: Vec<String>,
}

/// Result from executing a cluster generation — display data for the interface.
pub struct ExecuteGenerateResult {
    pub source_count: usize,
    pub root_breakdown: Vec<(String, usize)>,
    pub not_archived_count: usize,
}

impl ExecuteGenerateResult {
    /// Compose the cluster summary given a header line (provided by interface
    /// since it includes display paths that ops doesn't know).
    pub fn compose_summary(&self, header: &str) -> String {
        use crate::domain::format_count;
        let root_word = if self.root_breakdown.len() == 1 {
            "root"
        } else {
            "roots"
        };
        let mut lines = vec![header.to_string()];
        lines.push(format!(
            "  From {} {}:",
            self.root_breakdown.len(),
            root_word
        ));
        for (path, count) in &self.root_breakdown {
            lines.push(format!("    {}  ({})", path, format_count(*count)));
        }
        lines.push(format!(
            "  {} have no archived copy",
            format_count(self.not_archived_count)
        ));
        lines.join("\n")
    }
}

/// Parameters for executing a cluster refresh.
pub struct ExecuteRefreshParams {
    pub lock_path: PathBuf,
    pub manifest_path: PathBuf,
    pub old_manifest_content: String,
    pub config: ManifestConfig,
}

/// Result from executing a cluster refresh.
pub struct ExecuteRefreshResult {
    /// None if no sources matched (lock file removed, minimal manifest written).
    pub outcome: Option<ExecuteGenerateResult>,
}

// ============================================================================
// Execute functions
// ============================================================================

/// Write lock file + manifest for a fresh cluster generation.
pub fn execute_generate(
    plan: &ClusterGeneratePlan,
    params: &ExecuteGenerateParams,
) -> Result<ExecuteGenerateResult> {
    // Create destination directory if needed (after plan confirmed sources exist)
    if let Some(parent) = params.manifest_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create directory: {}", parent.display()))?;
    }

    // Write JSONL lock file
    write_lock_file(&params.lock_path, &plan.lock_entries)?;

    // Compute lock file hash
    let lock_hash = super::fs::compute_full_hash(&params.lock_path)?;

    // Build ManifestConfig
    let scope = if params.scope_prefixes.len() == 1 {
        Some(params.scope_prefixes[0].clone())
    } else if params.scope_prefixes.is_empty() {
        None
    } else {
        Some(params.scope_prefixes.join(", "))
    };

    let config = ManifestConfig {
        meta: ManifestMeta {
            version: 1,
            query: params.expanded_filters.clone(),
            scope,
            generated_at: current_timestamp(),
            lock_hash,
        },
        options: ManifestOptions {
            allow: params.allow.clone(),
        },
        output: ManifestOutput {
            pattern: "{filename}".to_string(),
            archive_root_id: params.archive_root_id,
            base_dir: params.base_dir.clone(),
        },
    };

    // Assemble manifest
    let header = "# Canon manifest — edit pattern and Notes freely.\n\
                  # To change the query, edit it here then run: canon cluster refresh <manifest>\n\
                  # Other fields are managed by Canon — do not edit.\n\
                  #\n";
    let summary = generate_summary_comments(plan);
    let notes_block = "# === Notes ===\n#\n";
    let fact_help = generate_fact_help(
        plan.lock_entries.len(),
        &plan.full_coverage_facts,
        config.meta.scope.is_some(),
    );

    let toml_str =
        toml::to_string_pretty(&config).context("Failed to serialize manifest config")?;

    // Inject "# Original:" comments when alias expansion changed the filter
    let comment_lines: Vec<String> = params
        .original_filters
        .iter()
        .zip(params.expanded_filters.iter())
        .filter(|(orig, exp)| orig != exp)
        .map(|(orig, _)| format!("# Original: {orig}"))
        .collect();
    let toml_str = if comment_lines.is_empty() {
        toml_str
    } else {
        inject_comments_before_key(&toml_str, "query", &comment_lines)
    };

    let manifest = format!(
        "{}{}\n{}\n{}\n\n{}",
        header,
        summary.trim_end(),
        notes_block.trim_end(),
        toml_str.trim_end(),
        fact_help
    );
    write_and_sync(&params.manifest_path, &manifest).with_context(|| {
        format!(
            "Failed to write manifest to {}",
            params.manifest_path.display()
        )
    })?;

    Ok(ExecuteGenerateResult {
        source_count: plan.lock_entries.len(),
        root_breakdown: plan.root_breakdown.clone(),
        not_archived_count: plan.not_archived_count,
    })
}

/// Rewrite lock file + update existing manifest for a cluster refresh.
pub fn execute_refresh(
    plan: &ClusterGeneratePlan,
    params: &ExecuteRefreshParams,
) -> Result<ExecuteRefreshResult> {
    if plan.lock_entries.is_empty() {
        // No sources — remove lock file if it exists
        if params.lock_path.exists() {
            fs::remove_file(&params.lock_path)?;
        }
        // Update config with empty lock hash
        let mut config = ManifestConfig {
            meta: ManifestMeta {
                version: params.config.meta.version,
                query: params.config.meta.query.clone(),
                scope: params.config.meta.scope.clone(),
                generated_at: current_timestamp(),
                lock_hash: String::new(),
            },
            options: ManifestOptions {
                allow: params.config.options.allow.clone(),
            },
            output: ManifestOutput {
                pattern: params.config.output.pattern.clone(),
                archive_root_id: params.config.output.archive_root_id,
                base_dir: params.config.output.base_dir.clone(),
            },
        };
        // Preserve version default behavior
        config.meta.version = params.config.meta.version;
        let toml_str =
            toml::to_string_pretty(&config).context("Failed to serialize manifest config")?;
        write_and_sync(&params.manifest_path, &toml_str).with_context(|| {
            format!("Failed to write config: {}", params.manifest_path.display())
        })?;
        return Ok(ExecuteRefreshResult { outcome: None });
    }

    // Write JSONL lock file
    write_lock_file(&params.lock_path, &plan.lock_entries)?;

    // Compute lock file hash and update config
    let lock_hash = super::fs::compute_full_hash(&params.lock_path)?;
    let mut config = ManifestConfig {
        meta: ManifestMeta {
            version: params.config.meta.version,
            query: params.config.meta.query.clone(),
            scope: params.config.meta.scope.clone(),
            generated_at: current_timestamp(),
            lock_hash,
        },
        options: ManifestOptions {
            allow: params.config.options.allow.clone(),
        },
        output: ManifestOutput {
            pattern: params.config.output.pattern.clone(),
            archive_root_id: params.config.output.archive_root_id,
            base_dir: params.config.output.base_dir.clone(),
        },
    };
    config.meta.version = params.config.meta.version;

    // Assemble manifest with preserved notes
    let header = "# Canon manifest — edit pattern and Notes freely.\n\
                  # To change the query, edit it here then run: canon cluster refresh <manifest>\n\
                  # Other fields are managed by Canon — do not edit.\n\
                  #\n";
    let summary = generate_summary_comments(plan);
    let notes =
        extract_notes_raw(&params.old_manifest_content).unwrap_or_else(|| "\n#\n".to_string());
    let notes_block = format!("# === Notes ==={notes}");
    let fact_help = generate_fact_help(
        plan.lock_entries.len(),
        &plan.full_coverage_facts,
        config.meta.scope.is_some(),
    );

    let toml_str =
        toml::to_string_pretty(&config).context("Failed to serialize manifest config")?;
    let manifest = format!(
        "{}{}\n{}\n{}\n\n{}",
        header,
        summary.trim_end(),
        notes_block.trim_end(),
        toml_str.trim_end(),
        fact_help
    );
    write_and_sync(&params.manifest_path, &manifest)
        .with_context(|| format!("Failed to write config: {}", params.manifest_path.display()))?;

    Ok(ExecuteRefreshResult {
        outcome: Some(ExecuteGenerateResult {
            source_count: plan.lock_entries.len(),
            root_breakdown: plan.root_breakdown.clone(),
            not_archived_count: plan.not_archived_count,
        }),
    })
}

/// Parse `allow` values from a manifest's `[options]` section.
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

// ============================================================================
// Private helpers
// ============================================================================

/// Find duplicate sources (same object_id) within lock entries.
/// Returns Vec of (object_id, Vec<source_id>).
fn find_source_duplicates(entries: &[LockEntry]) -> Vec<(i64, Vec<i64>)> {
    let mut object_map: HashMap<i64, Vec<i64>> = HashMap::new();

    for entry in entries {
        if let Some(object_id) = entry.object_id {
            object_map.entry(object_id).or_default().push(entry.id);
        }
    }

    object_map
        .into_iter()
        .filter(|(_, ids)| ids.len() > 1)
        .collect()
}

/// Track types seen for a fact key.
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

/// Compute facts with 100% coverage and mixed-type warnings.
///
/// Returns (full_coverage_facts, mixed_type_warnings).
fn compute_full_coverage_facts(
    entries: &[LockEntry],
    all_facts: &HashMap<i64, Vec<FactEntry>>,
) -> (Vec<(String, FactType, String)>, Vec<(String, String)>) {
    if entries.is_empty() {
        return (Vec::new(), Vec::new());
    }

    let source_count = entries.len();

    // Count facts by key across all sources, tracking type consistency
    let mut fact_counts: HashMap<String, FactTypeTracker> = HashMap::new();
    let mut seen_keys: HashSet<String> = HashSet::new();

    for entry in entries {
        if let Some(facts) = all_facts.get(&entry.id) {
            for fact in facts {
                let fact_type = match &fact.value {
                    FactValue::Text(_) => FactType::Text,
                    FactValue::Num(_) => FactType::Num,
                    FactValue::Time(_) => FactType::Time,
                    FactValue::Path(_) => FactType::Path,
                };

                // Track uniqueness per source
                let seen_key = format!("{}:{}", entry.id, fact.key);
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

    // Collect mixed-type warnings for 100% coverage facts
    let mut mixed_type_warnings: Vec<(String, String)> = Vec::new();
    for (key, tracker) in &fact_counts {
        if tracker.count == source_count && tracker.has_mixed_types() {
            mixed_type_warnings.push((key.clone(), tracker.type_breakdown()));
        }
    }
    mixed_type_warnings.sort_by(|a, b| a.0.cmp(&b.0));

    // Filter to only 100% coverage facts
    let mut full_coverage: Vec<(String, FactType, String)> = fact_counts
        .into_iter()
        .filter(|(_, tracker)| tracker.count == source_count)
        .map(|(key, tracker)| {
            let description = get_fact_description(&key);
            (key, tracker.dominant_type(), description)
        })
        .collect();

    full_coverage.sort_by(|a, b| a.0.cmp(&b.0));

    (full_coverage, mixed_type_warnings)
}

/// Get a human-readable description for a fact key.
fn get_fact_description(key: &str) -> String {
    BuiltinKey::from_str(key)
        .and_then(|k| k.description())
        .map(|s| s.to_string())
        .unwrap_or_default()
}

// ============================================================================
// Manifest content helpers (moved from interface layer)
// ============================================================================

/// Write content to a file and fsync to ensure it's flushed to disk.
/// This prevents race conditions when opening the file in an editor
/// immediately after writing, especially on network volumes (NAS/SMB).
fn write_and_sync(path: &Path, content: &str) -> Result<()> {
    use std::io::Write as _;
    let mut file = fs::File::create(path)?;
    file.write_all(content.as_bytes())?;
    file.sync_all()?;
    Ok(())
}

/// Write a JSONL lock file from lock entries.
fn write_lock_file(lock_path: &Path, entries: &[LockEntry]) -> Result<()> {
    let lock_file = std::fs::File::create(lock_path)
        .with_context(|| format!("Failed to create lock file: {}", lock_path.display()))?;
    let mut writer = BufWriter::new(lock_file);

    for entry in entries {
        serde_json::to_writer(&mut writer, entry)
            .with_context(|| format!("Failed to write lock entry for {}", entry.path))?;
        writeln!(writer)?;
    }

    writer.flush()?;
    Ok(())
}

fn current_timestamp() -> String {
    chrono::Utc::now().to_rfc3339()
}

/// Generate cluster summary comment block for the manifest.
fn generate_summary_comments(plan: &ClusterGeneratePlan) -> String {
    use crate::domain::format_count;

    let source_count = plan.lock_entries.len();
    let mut s = String::new();
    s.push_str("# === Cluster Summary ===\n");

    let root_word = if plan.root_breakdown.len() == 1 {
        "root"
    } else {
        "roots"
    };
    s.push_str(&format!(
        "# {} sources from {} {}:\n",
        format_count(source_count),
        plan.root_breakdown.len(),
        root_word
    ));
    for (path, count) in &plan.root_breakdown {
        s.push_str(&format!("#   {}  ({})\n", path, format_count(*count)));
    }
    s.push_str(&format!(
        "# {} have no archived copy\n",
        format_count(plan.not_archived_count)
    ));

    let archived_count = plan.archived.len();
    if archived_count > 0 || plan.excluded_count > 0 || plan.unhashed_count > 0 {
        s.push_str("#\n");
        let mut parts = Vec::new();
        if archived_count > 0 {
            parts.push(format!(
                "{} already archived (--allow archived)",
                archived_count
            ));
        }
        if plan.excluded_count > 0 {
            parts.push(format!("{} excluded", plan.excluded_count));
        }
        if plan.unhashed_count > 0 {
            parts.push(format!("{} unhashed", plan.unhashed_count));
        }
        s.push_str(&format!("# Skipped: {}\n", parts.join(", ")));
    }

    s
}

/// Generate fact help comments for the manifest.
fn generate_fact_help(
    source_count: usize,
    full_coverage_facts: &[(String, FactType, String)],
    has_scope: bool,
) -> String {
    use crate::expr::{BuiltinKeyVisibility, Modifier, ModifierCategory};
    use strum::IntoEnumIterator;

    if source_count == 0 {
        return String::new();
    }

    let mut help = String::new();
    help.push_str(&format!(
        "# Available facts for pattern (100% coverage on {source_count} sources in this cluster):\n"
    ));
    help.push_str("#\n");

    // Built-in facts
    help.push_str("# Built-in:\n");
    for key in BuiltinKey::iter() {
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
    if has_scope {
        help.push_str(&format!(
            "#   {:18} {:6} - {}\n",
            "scope.rel_path", "path", "Path relative to the manifest scope"
        ));
    }
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

    // Modifiers reference
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

    // Aliases
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

/// Extract notes section from an existing manifest.
/// Extract the raw notes section from a manifest (preserves `#` comment markers).
/// Used by manifest refresh to re-emit the notes block verbatim.
pub fn extract_notes_raw(content: &str) -> Option<String> {
    let marker = "# === Notes ===";
    let start_idx = content.find(marker)?;
    let after_marker = start_idx + marker.len();
    let rest = &content[after_marker..];

    let end = rest
        .lines()
        .enumerate()
        .skip(1)
        .find(|(_, line)| line.starts_with("# === ") || line.starts_with('['))
        .map(|(i, _)| rest.lines().take(i).map(|l| l.len() + 1).sum::<usize>())
        .unwrap_or(rest.len());

    Some(rest[..end].to_string())
}

/// Extract notes from a manifest as clean text (strips `#` comment markers).
/// Used for decision reason when manifest notes flow into apply records.
pub fn extract_notes(content: &str) -> Option<String> {
    let raw = extract_notes_raw(content)?;
    let stripped: String = raw
        .lines()
        .map(|line| {
            line.strip_prefix("# ")
                .or_else(|| line.strip_prefix("#"))
                .unwrap_or(line)
        })
        .collect::<Vec<_>>()
        .join("\n");
    let trimmed = stripped.trim().to_string();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

/// Insert comment lines before a key in a TOML string.
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
// Manifest I/O helpers (shared between apply and status)
// ============================================================================

/// Read and parse a manifest TOML config file.
pub fn read_manifest_config(manifest_path: &Path) -> Result<ManifestConfig> {
    let config_content = fs::read_to_string(manifest_path).with_context(|| {
        format!(
            "Failed to read manifest config: {}",
            manifest_path.display()
        )
    })?;
    let config: ManifestConfig = toml::from_str(&config_content).with_context(|| {
        format!(
            "Failed to parse manifest config: {}",
            manifest_path.display()
        )
    })?;
    validate_manifest_version(config.meta.version)?;
    Ok(config)
}

/// Read and parse a lock file (JSONL format, one LockEntry per line).
pub fn read_lock_entries(lock_path: &Path) -> Result<Vec<LockEntry>> {
    use std::io::{BufRead, BufReader};
    let lock_file = std::fs::File::open(lock_path)
        .with_context(|| format!("Failed to open lock file: {}", lock_path.display()))?;
    BufReader::new(lock_file)
        .lines()
        .enumerate()
        .map(|(i, line)| {
            let line =
                line.with_context(|| format!("Failed to read line {} of lock file", i + 1))?;
            serde_json::from_str(&line)
                .with_context(|| format!("Failed to parse line {} of lock file", i + 1))
        })
        .collect::<Result<Vec<_>>>()
}

// ============================================================================
// Manifest status
// ============================================================================

/// State of a single lock entry in status assessment.
pub struct StatusEntry {
    /// Absolute source path.
    pub source_path: String,
    /// Last component of source path, for display.
    pub source_filename: String,
    /// Whether the source file exists on disk.
    pub source_exists: bool,
    /// Full destination path.
    pub dest_path: String,
    /// Whether the destination file exists on disk.
    pub dest_exists: bool,
    /// Whether dest size matches expected size (only meaningful when dest_exists).
    pub dest_size_match: bool,
    /// Whether the destination is registered in the DB (present=1).
    pub db_registered: bool,
}

/// Result of manifest status assessment.
pub struct ManifestStatus {
    /// Path to the manifest file.
    pub manifest_path: String,
    /// Display string for destination (archive root + base_dir).
    pub dest_display: String,
    /// Output pattern from the manifest.
    pub pattern: String,
    /// Number of entries in the lock file.
    pub lock_entry_count: usize,
    /// Whether the lock file hash matches the manifest.
    pub lock_hash_valid: bool,
    /// Per-entry status assessment.
    pub entries: Vec<StatusEntry>,
    /// Count of entries where dest exists with correct size.
    pub at_destination: usize,
    /// Count of entries where source exists but dest does not.
    pub pending: usize,
    /// Count of entries where source is missing and dest is missing.
    pub source_lost: usize,
    /// Count of entries where dest exists but size doesn't match.
    pub size_mismatch: usize,
    /// Count of entries at dest where source file still exists.
    pub source_still_present: usize,
}

impl ManifestStatus {
    /// Are all source files accounted for (either at source or at destination)?
    pub fn all_accounted_for(&self) -> bool {
        self.source_lost == 0 && self.size_mismatch == 0
    }
}

/// Compute the status of a manifest by checking filesystem and DB state.
///
/// This is a read-only diagnostic: no DB writes, no file operations.
/// Needs `&mut Connection` because fact fetching uses temp tables.
pub fn compute_manifest_status(
    conn: &mut Connection,
    manifest_path: &Path,
) -> Result<ManifestStatus> {
    use crate::expr;
    use crate::ops::apply::evaluate_pattern;

    // 1. Read manifest and lock
    let config = read_manifest_config(manifest_path)?;
    let lock_path = manifest_path.with_extension("lock");
    let lock_entries = read_lock_entries(&lock_path)?;
    let lock_entry_count = lock_entries.len();

    // 2. Validate lock hash (non-fatal)
    let lock_hash_valid = match super::fs::compute_full_hash(&lock_path) {
        Ok(actual_hash) => actual_hash == config.meta.lock_hash,
        Err(_) => false,
    };

    // 3. Fetch roots, build root_paths map
    let roots = repo::root::fetch_all(conn)?;
    let root_paths: HashMap<i64, String> = roots.iter().map(|r| (r.id, r.path.clone())).collect();

    // 4. Find archive root and compute base_dir
    let archive_root = roots
        .iter()
        .find(|r| r.id == config.output.archive_root_id)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Archive root id {} not found",
                config.output.archive_root_id
            )
        })?;
    let base_dir = if config.output.base_dir.is_empty() {
        PathBuf::from(&archive_root.path)
    } else {
        PathBuf::from(&archive_root.path).join(&config.output.base_dir)
    };
    let dest_display = base_dir.to_string_lossy().to_string();

    // 5. Parse pattern and fetch needed facts
    let pattern = expr::parse_pattern(&config.output.pattern)
        .with_context(|| format!("Failed to parse output pattern: {}", config.output.pattern))?;
    let needed_keys = expr::extract_fact_keys(&pattern);
    let scope_prefix = config.meta.scope.as_deref();

    // Batch fetch facts for all lock entries if pattern uses content facts
    let source_ids: Vec<i64> = lock_entries.iter().map(|s| s.id).collect();
    let mut all_facts: HashMap<i64, Vec<crate::domain::fact::FactEntry>> = HashMap::new();
    for key in &needed_keys {
        if key.starts_with("source.") || key.starts_with("scope.") || key == "object.hash" {
            continue;
        }
        let key_facts = repo::fact::batch_fetch_key_for_sources(conn, &source_ids, key)?;
        for (source_id, entry_opt) in key_facts {
            if let Some(entry) = entry_opt {
                all_facts.entry(source_id).or_default().push(entry);
            }
        }
    }

    // 6. Evaluate patterns to get dest paths, then check filesystem + DB
    let mut entries = Vec::with_capacity(lock_entry_count);
    let mut dest_rel_paths: Vec<String> = Vec::with_capacity(lock_entry_count);

    for lock_entry in &lock_entries {
        let dest_rel = match evaluate_pattern(
            &pattern,
            lock_entry,
            &needed_keys,
            scope_prefix,
            &root_paths,
            &all_facts,
        ) {
            Ok(rel) => rel,
            Err(e) => {
                // If pattern expansion fails, we can't determine dest path.
                // Use a placeholder and mark as not-at-dest.
                let filename = Path::new(&lock_entry.path)
                    .file_name()
                    .map(|f| f.to_string_lossy().to_string())
                    .unwrap_or_else(|| lock_entry.path.clone());
                entries.push(StatusEntry {
                    source_path: lock_entry.path.clone(),
                    source_filename: filename,
                    source_exists: Path::new(&lock_entry.path).exists(),
                    dest_path: format!("<pattern expansion failed: {}>", e),
                    dest_exists: false,
                    dest_size_match: false,
                    db_registered: false,
                });
                dest_rel_paths.push(String::new());
                continue;
            }
        };

        let archive_rel_path = if config.output.base_dir.is_empty() {
            dest_rel.clone()
        } else {
            format!("{}/{}", config.output.base_dir, &dest_rel)
        };

        let dest_full = base_dir.join(&dest_rel);
        let source_exists = Path::new(&lock_entry.path).exists();
        let dest_stat = fs::metadata(&dest_full).ok();

        let (dest_exists, dest_size_match) = match &dest_stat {
            Some(meta) if meta.is_file() => {
                let actual_size = meta.len();
                let expected_size = lock_entry.size as u64;
                (true, actual_size == expected_size)
            }
            _ => (false, false),
        };

        let filename = Path::new(&lock_entry.path)
            .file_name()
            .map(|f| f.to_string_lossy().to_string())
            .unwrap_or_else(|| lock_entry.path.clone());

        entries.push(StatusEntry {
            source_path: lock_entry.path.clone(),
            source_filename: filename,
            source_exists,
            dest_path: dest_full.to_string_lossy().to_string(),
            dest_exists,
            dest_size_match,
            db_registered: false, // filled in below
        });
        dest_rel_paths.push(archive_rel_path);
    }

    // 7. Batch check DB registration
    let rel_refs: Vec<&str> = dest_rel_paths.iter().map(|s| s.as_str()).collect();
    let registered =
        repo::source::batch_check_paths_exist(conn, config.output.archive_root_id, &rel_refs)?;
    for (entry, rel_path) in entries.iter_mut().zip(dest_rel_paths.iter()) {
        if !rel_path.is_empty() && registered.contains(rel_path.as_str()) {
            entry.db_registered = true;
        }
    }

    // 8. Compute counts
    let mut at_destination = 0usize;
    let mut pending = 0usize;
    let mut source_lost = 0usize;
    let mut size_mismatch = 0usize;
    let mut source_still_present = 0usize;

    for entry in &entries {
        if entry.dest_exists && entry.dest_size_match {
            at_destination += 1;
            if entry.source_exists {
                source_still_present += 1;
            }
        } else if entry.dest_exists && !entry.dest_size_match {
            size_mismatch += 1;
        } else if entry.source_exists {
            pending += 1;
        } else {
            source_lost += 1;
        }
    }

    Ok(ManifestStatus {
        manifest_path: manifest_path.to_string_lossy().to_string(),
        dest_display,
        pattern: config.output.pattern.clone(),
        lock_entry_count,
        lock_hash_valid,
        entries,
        at_destination,
        pending,
        source_lost,
        size_mismatch,
        source_still_present,
    })
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::test_helpers::{
        insert_fact, insert_object, insert_root, insert_source, insert_source_excluded,
        setup_test_db,
    };

    fn default_params() -> ClusterGenerateParams {
        ClusterGenerateParams {
            scopes: vec![],
            filters: vec![],
            allow_archived: false,
            allow_duplicates: false,
        }
    }

    // =========================================================================
    // Selection and filtering
    // =========================================================================

    #[test]
    fn test_plan_generate_empty_no_sources() {
        let mut conn = setup_test_db();
        let plan = plan_generate(&mut conn, &default_params()).unwrap();
        assert!(plan.lock_entries.is_empty());
        assert!(plan.archived.is_empty());
        assert_eq!(plan.excluded_count, 0);
        assert_eq!(plan.unhashed_count, 0);
    }

    #[test]
    fn test_plan_generate_excludes_suspended() {
        let mut conn = setup_test_db();
        let active_root = insert_root(&conn, "/active", "source", false);
        let suspended_root = insert_root(&conn, "/suspended", "source", true);
        let obj1 = insert_object(&conn, "hash1", false);
        let obj2 = insert_object(&conn, "hash2", false);
        insert_source(&conn, active_root, "file1.jpg", Some(obj1));
        insert_source(&conn, suspended_root, "file2.jpg", Some(obj2));

        let plan = plan_generate(&mut conn, &default_params()).unwrap();
        assert_eq!(plan.lock_entries.len(), 1);
        assert_eq!(plan.lock_entries[0].path, "/active/file1.jpg");
    }

    #[test]
    fn test_plan_generate_excludes_excluded() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj_normal = insert_object(&conn, "normal_hash", false);
        let obj_src_excl = insert_object(&conn, "src_excl_hash", false);
        let obj_obj_excl = insert_object(&conn, "obj_excl_hash", true);

        insert_source(&conn, root, "normal.jpg", Some(obj_normal));
        insert_source_excluded(&conn, root, "source_excluded.jpg", Some(obj_src_excl));
        insert_source(&conn, root, "object_excluded.jpg", Some(obj_obj_excl));

        let plan = plan_generate(&mut conn, &default_params()).unwrap();
        assert_eq!(plan.lock_entries.len(), 1);
        assert_eq!(plan.lock_entries[0].path, "/photos/normal.jpg");
        assert_eq!(plan.excluded_count, 2);
    }

    #[test]
    fn test_plan_generate_respects_scope() {
        let mut conn = setup_test_db();
        let photos = insert_root(&conn, "/photos", "source", false);
        let videos = insert_root(&conn, "/videos", "source", false);
        let obj1 = insert_object(&conn, "hash1", false);
        let obj2 = insert_object(&conn, "hash2", false);
        insert_source(&conn, photos, "a.jpg", Some(obj1));
        insert_source(&conn, videos, "b.mp4", Some(obj2));

        let mut params = default_params();
        params.scopes = vec![ScopeMatch::UnderDirectory("/photos".to_string())];
        let plan = plan_generate(&mut conn, &params).unwrap();
        assert_eq!(plan.lock_entries.len(), 1);
        assert_eq!(plan.lock_entries[0].path, "/photos/a.jpg");
    }

    // =========================================================================
    // Archive detection
    // =========================================================================

    #[test]
    fn test_plan_generate_archive_detection_per_source() {
        let mut conn = setup_test_db();
        let source_root = insert_root(&conn, "/photos", "source", false);
        let archive_root = insert_root(&conn, "/archive", "archive", false);

        // One object archived, shared by 3 sources
        let archived_obj = insert_object(&conn, "archived_hash", false);
        insert_source(&conn, source_root, "photo1.jpg", Some(archived_obj));
        insert_source(&conn, source_root, "photo2.jpg", Some(archived_obj));
        insert_source(&conn, source_root, "photo3.jpg", Some(archived_obj));

        // One unarchived object
        let unarchived_obj = insert_object(&conn, "unarchived_hash", false);
        insert_source(&conn, source_root, "photo4.jpg", Some(unarchived_obj));

        // Put the first object in the archive
        insert_source(&conn, archive_root, "backup.jpg", Some(archived_obj));

        let plan = plan_generate(&mut conn, &default_params()).unwrap();
        // All 3 sources of the archived object should be in `archived`
        assert_eq!(plan.archived.len(), 3);
        // Only the unarchived source in lock_entries
        assert_eq!(plan.lock_entries.len(), 1);
        assert_eq!(plan.lock_entries[0].path, "/photos/photo4.jpg");
    }

    #[test]
    fn test_plan_generate_allow_archived() {
        let mut conn = setup_test_db();
        let source_root = insert_root(&conn, "/photos", "source", false);
        let archive_root = insert_root(&conn, "/archive", "archive", false);
        let obj1 = insert_object(&conn, "hash1", false);
        let obj2 = insert_object(&conn, "hash2", false);
        // Two sources with different content, both archived
        insert_source(&conn, source_root, "photo1.jpg", Some(obj1));
        insert_source(&conn, source_root, "photo2.jpg", Some(obj2));
        insert_source(&conn, archive_root, "backup1.jpg", Some(obj1));
        insert_source(&conn, archive_root, "backup2.jpg", Some(obj2));

        // Scope to source root to avoid archive-role sources in selection
        let scope = vec![ScopeMatch::UnderDirectory("/photos".to_string())];

        // Without allow_archived, both go to archived list
        let mut params = default_params();
        params.scopes = scope.clone();
        let plan = plan_generate(&mut conn, &params).unwrap();
        assert!(plan.lock_entries.is_empty());
        assert_eq!(plan.archived.len(), 2);

        // With allow_archived, both stay in lock_entries
        let mut params = default_params();
        params.scopes = scope;
        params.allow_archived = true;
        let plan = plan_generate(&mut conn, &params).unwrap();
        assert_eq!(plan.lock_entries.len(), 2);
        assert!(plan.archived.is_empty());
    }

    #[test]
    fn test_plan_generate_not_archived_count() {
        let mut conn = setup_test_db();
        let source_root = insert_root(&conn, "/photos", "source", false);
        let archive_root = insert_root(&conn, "/archive", "archive", false);

        let obj_archived = insert_object(&conn, "archived_hash", false);
        let obj_unarchived = insert_object(&conn, "unarchived_hash", false);

        // Source whose content IS in archive
        insert_source(&conn, source_root, "has_backup.jpg", Some(obj_archived));
        insert_source(&conn, archive_root, "backup.jpg", Some(obj_archived));

        // Source whose content is NOT in archive
        insert_source(&conn, source_root, "no_backup.jpg", Some(obj_unarchived));

        // Scope to source root + allow_archived so the archived source stays in lock_entries
        let mut params = default_params();
        params.scopes = vec![ScopeMatch::UnderDirectory("/photos".to_string())];
        params.allow_archived = true;
        let plan = plan_generate(&mut conn, &params).unwrap();
        assert_eq!(plan.lock_entries.len(), 2);
        // has_backup.jpg has an archive copy, no_backup.jpg doesn't
        assert_eq!(plan.not_archived_count, 1);
    }

    // =========================================================================
    // Hashing and lock entry
    // =========================================================================

    #[test]
    fn test_plan_generate_skips_unhashed() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "hash1", false);
        insert_source(&conn, root, "hashed.jpg", Some(obj));
        insert_source(&conn, root, "unhashed.jpg", None);

        let plan = plan_generate(&mut conn, &default_params()).unwrap();
        assert_eq!(plan.lock_entries.len(), 1);
        assert_eq!(plan.lock_entries[0].path, "/photos/hashed.jpg");
        assert_eq!(plan.unhashed_count, 1);
    }

    #[test]
    fn test_plan_generate_lock_entry_has_hash() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "abcdef1234567890", false);
        insert_source(&conn, root, "photo.jpg", Some(obj));

        let plan = plan_generate(&mut conn, &default_params()).unwrap();
        assert_eq!(plan.lock_entries.len(), 1);
        assert_eq!(plan.lock_entries[0].hash_type.as_deref(), Some("sha256"));
        assert_eq!(
            plan.lock_entries[0].hash_value.as_deref(),
            Some("abcdef1234567890")
        );
    }

    // =========================================================================
    // Duplicate checking
    // =========================================================================

    #[test]
    fn test_plan_generate_duplicates_rejected() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "same_hash", false);
        insert_source(&conn, root, "copy1.jpg", Some(obj));
        insert_source(&conn, root, "copy2.jpg", Some(obj));

        let result = plan_generate(&mut conn, &default_params());
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("duplicate"));
    }

    #[test]
    fn test_plan_generate_duplicates_allowed() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "same_hash", false);
        insert_source(&conn, root, "copy1.jpg", Some(obj));
        insert_source(&conn, root, "copy2.jpg", Some(obj));

        let mut params = default_params();
        params.allow_duplicates = true;
        let plan = plan_generate(&mut conn, &params).unwrap();
        assert_eq!(plan.lock_entries.len(), 2);
    }

    // =========================================================================
    // Root breakdown
    // =========================================================================

    #[test]
    fn test_plan_generate_root_breakdown() {
        let mut conn = setup_test_db();
        let root_a = insert_root(&conn, "/aaa", "source", false);
        let root_b = insert_root(&conn, "/bbb", "source", false);
        let obj1 = insert_object(&conn, "hash1", false);
        let obj2 = insert_object(&conn, "hash2", false);
        let obj3 = insert_object(&conn, "hash3", false);
        insert_source(&conn, root_a, "a1.jpg", Some(obj1));
        insert_source(&conn, root_b, "b1.jpg", Some(obj2));
        insert_source(&conn, root_b, "b2.jpg", Some(obj3));

        let plan = plan_generate(&mut conn, &default_params()).unwrap();
        assert_eq!(plan.root_breakdown.len(), 2);
        // Sorted by path
        assert_eq!(plan.root_breakdown[0].0, "/aaa");
        assert_eq!(plan.root_breakdown[0].1, 1);
        assert_eq!(plan.root_breakdown[1].0, "/bbb");
        assert_eq!(plan.root_breakdown[1].1, 2);
    }

    // =========================================================================
    // Fact coverage
    // =========================================================================

    #[test]
    fn test_plan_generate_full_coverage_facts() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj1 = insert_object(&conn, "hash1", false);
        let obj2 = insert_object(&conn, "hash2", false);
        let id1 = insert_source(&conn, root, "a.jpg", Some(obj1));
        let id2 = insert_source(&conn, root, "b.jpg", Some(obj2));

        // Both sources have "content.Make"
        insert_fact(&conn, id1, "content.Make", "Canon");
        insert_fact(&conn, id2, "content.Make", "Nikon");

        let plan = plan_generate(&mut conn, &default_params()).unwrap();
        assert_eq!(plan.full_coverage_facts.len(), 1);
        assert_eq!(plan.full_coverage_facts[0].0, "content.Make");
    }

    #[test]
    fn test_plan_generate_partial_coverage_excluded() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj1 = insert_object(&conn, "hash1", false);
        let obj2 = insert_object(&conn, "hash2", false);
        let obj3 = insert_object(&conn, "hash3", false);
        let id1 = insert_source(&conn, root, "a.jpg", Some(obj1));
        let id2 = insert_source(&conn, root, "b.jpg", Some(obj2));
        let _id3 = insert_source(&conn, root, "c.jpg", Some(obj3));

        // Only 2 of 3 sources have the fact
        insert_fact(&conn, id1, "content.Make", "Canon");
        insert_fact(&conn, id2, "content.Make", "Nikon");

        let mut params = default_params();
        params.allow_duplicates = true; // avoid duplicate issues
        let plan = plan_generate(&mut conn, &params).unwrap();
        // 3 sources but only 2 have the fact → not 100% coverage
        assert!(
            plan.full_coverage_facts.is_empty(),
            "Partial coverage facts should not appear"
        );
    }

    // =========================================================================
    // parse_manifest_allow
    // =========================================================================

    #[test]
    fn test_manifest_options_invalid_allow() {
        let result = parse_manifest_allow(&["bogus".to_string()]);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("bogus"));
        assert!(err.contains("archived"));
    }

    #[test]
    fn test_parse_manifest_allow_valid() {
        let (archived, duplicates) =
            parse_manifest_allow(&["archived".to_string(), "duplicates".to_string()]).unwrap();
        assert!(archived);
        assert!(duplicates);
    }

    #[test]
    fn test_parse_manifest_allow_empty() {
        let (archived, duplicates) = parse_manifest_allow(&[]).unwrap();
        assert!(!archived);
        assert!(!duplicates);
    }

    // =========================================================================
    // Version validation
    // =========================================================================

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

    // =========================================================================
    // Manifest serde
    // =========================================================================

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
        assert!(config.options.allow.is_empty());
    }

    #[test]
    fn test_manifest_without_version_defaults_to_1() {
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

    // =========================================================================
    // extract_notes
    // =========================================================================

    // extract_notes_raw — preserves # markers for manifest refresh

    #[test]
    fn test_extract_notes_raw_empty_placeholder() {
        let content = "# === Notes ===\n#\n\n[meta]\nversion = 1\n";
        let notes = extract_notes_raw(content).unwrap();
        assert_eq!(notes, "\n#\n\n");
    }

    #[test]
    fn test_extract_notes_raw_with_content() {
        let content =
            "# === Notes ===\n# This cluster has family photos\n# from 2020-2023\n\n[meta]\n";
        let notes = extract_notes_raw(content).unwrap();
        assert_eq!(
            notes,
            "\n# This cluster has family photos\n# from 2020-2023\n\n"
        );
    }

    #[test]
    fn test_extract_notes_raw_before_meta() {
        let content = "# === Notes ===\n# Some note\n[meta]\nversion = 1\n";
        let notes = extract_notes_raw(content).unwrap();
        assert_eq!(notes, "\n# Some note\n");
    }

    #[test]
    fn test_extract_notes_raw_before_next_section() {
        let content = "# === Notes ===\n# My notes\n# === Cluster Summary ===\n# stuff\n";
        let notes = extract_notes_raw(content).unwrap();
        assert_eq!(notes, "\n# My notes\n");
    }

    // extract_notes — strips # markers for decision reason

    #[test]
    fn test_extract_notes_empty_placeholder() {
        let content = "# === Notes ===\n#\n\n[meta]\nversion = 1\n";
        // Empty placeholder: after stripping # prefix, content is whitespace-only → None
        assert!(extract_notes(content).is_none());
    }

    #[test]
    fn test_extract_notes_with_content() {
        let content =
            "# === Notes ===\n# This cluster has family photos\n# from 2020-2023\n\n[meta]\n";
        let notes = extract_notes(content).unwrap();
        assert_eq!(notes, "This cluster has family photos\nfrom 2020-2023");
    }

    #[test]
    fn test_extract_notes_missing() {
        let content = "[meta]\nversion = 1\nquery = []\n";
        assert!(extract_notes(content).is_none());
    }

    #[test]
    fn test_extract_notes_strips_comment_markers() {
        let content = "# === Notes ===\n# Some note\n[meta]\nversion = 1\n";
        let notes = extract_notes(content).unwrap();
        assert_eq!(notes, "Some note");
    }

    #[test]
    fn test_extract_notes_stops_at_next_section() {
        let content = "# === Notes ===\n# My notes\n# === Cluster Summary ===\n# stuff\n";
        let notes = extract_notes(content).unwrap();
        assert_eq!(notes, "My notes");
    }

    // =========================================================================
    // generate_summary_comments
    // =========================================================================

    fn make_plan_for_summary(
        source_count: usize,
        root_breakdown: Vec<(String, usize)>,
        not_archived_count: usize,
        archived_count: usize,
        excluded_count: usize,
        unhashed_count: usize,
    ) -> ClusterGeneratePlan {
        // Build minimal lock entries to match source_count
        let lock_entries: Vec<LockEntry> = (0..source_count)
            .map(|i| LockEntry {
                id: i as i64,
                root_id: 1,
                path: format!("file{i}.jpg"),
                device: 0,
                inode: i as i64,
                size: 100,
                mtime: 0,
                partial_hash: "hash".to_string(),
                object_id: Some(i as i64),
                hash_type: None,
                hash_value: None,
            })
            .collect();

        let archived: Vec<(String, String)> = (0..archived_count)
            .map(|i| (format!("archived{i}.jpg"), format!("archive{i}.jpg")))
            .collect();

        ClusterGeneratePlan {
            lock_entries,
            archived,
            full_coverage_facts: vec![],
            mixed_type_warnings: vec![],
            root_breakdown,
            not_archived_count,
            excluded_count,
            unhashed_count,
        }
    }

    #[test]
    fn test_generate_summary_single_root() {
        let plan = make_plan_for_summary(42, vec![("/photos".to_string(), 42)], 42, 0, 0, 0);
        let summary = generate_summary_comments(&plan);
        assert!(summary.contains("42 sources from 1 root:"));
        assert!(summary.contains("#   /photos  (42)"));
        assert!(summary.contains("# 42 have no archived copy"));
        assert!(!summary.contains("Skipped"));
    }

    #[test]
    fn test_generate_summary_multiple_roots() {
        let plan = make_plan_for_summary(
            150,
            vec![("/backup".to_string(), 50), ("/photos".to_string(), 100)],
            120,
            0,
            0,
            0,
        );
        let summary = generate_summary_comments(&plan);
        assert!(summary.contains("150 sources from 2 roots:"));
        let backup_pos = summary.find("/backup").unwrap();
        let photos_pos = summary.find("/photos").unwrap();
        assert!(backup_pos < photos_pos);
    }

    #[test]
    fn test_generate_summary_no_skipped() {
        let plan = make_plan_for_summary(10, vec![("/photos".to_string(), 10)], 10, 0, 0, 0);
        let summary = generate_summary_comments(&plan);
        assert!(!summary.contains("Skipped"));
    }

    #[test]
    fn test_generate_summary_with_skipped() {
        let plan = make_plan_for_summary(10, vec![("/photos".to_string(), 10)], 10, 0, 3, 5);
        let summary = generate_summary_comments(&plan);
        assert!(summary.contains("# Skipped: 3 excluded, 5 unhashed"));
    }

    #[test]
    fn test_generate_summary_with_archived_skipped() {
        let plan = make_plan_for_summary(10, vec![("/photos".to_string(), 10)], 10, 4, 2, 0);
        let summary = generate_summary_comments(&plan);
        assert!(summary.contains("# Skipped: 4 already archived (--allow archived), 2 excluded"));
    }

    // =========================================================================
    // write_lock_file
    // =========================================================================

    #[test]
    fn test_write_lock_file_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let lock_path = dir.path().join("test.lock");
        let entries = vec![
            LockEntry {
                id: 1,
                root_id: 10,
                path: "/photos/a.jpg".to_string(),
                device: 100,
                inode: 200,
                size: 5000,
                mtime: 1700000000,
                partial_hash: "abc123".to_string(),
                object_id: Some(42),
                hash_type: Some("sha256".to_string()),
                hash_value: Some("deadbeef".to_string()),
            },
            LockEntry {
                id: 2,
                root_id: 10,
                path: "/photos/b.jpg".to_string(),
                device: 100,
                inode: 201,
                size: 3000,
                mtime: 1700000001,
                partial_hash: "def456".to_string(),
                object_id: None,
                hash_type: None,
                hash_value: None,
            },
        ];

        write_lock_file(&lock_path, &entries).unwrap();

        // Read back and verify
        let content = std::fs::read_to_string(&lock_path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2);
        let entry1: LockEntry = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(entry1.id, 1);
        assert_eq!(entry1.path, "/photos/a.jpg");
        assert_eq!(entry1.hash_value.as_deref(), Some("deadbeef"));
        let entry2: LockEntry = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(entry2.id, 2);
        assert!(entry2.object_id.is_none());
    }

    // =========================================================================
    // execute_generate
    // =========================================================================

    #[test]
    fn test_execute_generate_writes_files() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "hash1", false);
        insert_source(&conn, root, "photo.jpg", Some(obj));

        let plan = plan_generate(&mut conn, &default_params()).unwrap();

        let dir = tempfile::tempdir().unwrap();
        let manifest_path = dir.path().join("cluster.toml");
        let lock_path = dir.path().join("cluster.lock");

        let params = ExecuteGenerateParams {
            lock_path: lock_path.clone(),
            manifest_path: manifest_path.clone(),
            expanded_filters: vec![],
            original_filters: vec![],
            scope_prefixes: vec!["/photos".to_string()],
            archive_root_id: 1,
            base_dir: "output".to_string(),
            allow: vec![],
        };

        let result = execute_generate(&plan, &params).unwrap();
        assert_eq!(result.source_count, 1);

        // Verify files exist
        assert!(lock_path.exists());
        assert!(manifest_path.exists());

        // Verify manifest content sections
        let manifest = std::fs::read_to_string(&manifest_path).unwrap();
        assert!(manifest.contains("# === Cluster Summary ==="));
        assert!(manifest.contains("# === Notes ==="));
        assert!(manifest.contains("[meta]"));
        assert!(manifest.contains("[output]"));
        assert!(manifest.contains("pattern = \"{filename}\""));
    }

    #[test]
    fn test_execute_generate_injects_original_filter_comments() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "hash1", false);
        insert_source(&conn, root, "photo.jpg", Some(obj));

        let plan = plan_generate(&mut conn, &default_params()).unwrap();

        let dir = tempfile::tempdir().unwrap();
        let params = ExecuteGenerateParams {
            lock_path: dir.path().join("cluster.lock"),
            manifest_path: dir.path().join("cluster.toml"),
            expanded_filters: vec!["source.ext=jpg".to_string()],
            original_filters: vec!["@image".to_string()],
            scope_prefixes: vec!["/photos".to_string()],
            archive_root_id: 1,
            base_dir: "output".to_string(),
            allow: vec![],
        };

        execute_generate(&plan, &params).unwrap();

        let manifest = std::fs::read_to_string(dir.path().join("cluster.toml")).unwrap();
        assert!(manifest.contains("# Original: @image"));
    }

    // =========================================================================
    // execute_refresh
    // =========================================================================

    #[test]
    fn test_execute_refresh_preserves_notes() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "hash1", false);
        insert_source(&conn, root, "photo.jpg", Some(obj));

        let plan = plan_generate(&mut conn, &default_params()).unwrap();

        // Simulate an existing manifest with user-edited notes
        let old_content = "\
# === Cluster Summary ===\n\
# 1 sources from 1 root:\n\
# === Notes ===\n\
# These are my important notes\n\
# about this cluster\n\
[meta]\n\
version = 1\n\
query = []\n\
generated_at = \"2026-01-01T00:00:00Z\"\n\
lock_hash = \"old\"\n\
\n\
[output]\n\
pattern = \"{filename}\"\n\
archive_root_id = 1\n\
base_dir = \"output\"\n";

        let config: ManifestConfig = toml::from_str(old_content).unwrap();

        let dir = tempfile::tempdir().unwrap();
        let params = ExecuteRefreshParams {
            lock_path: dir.path().join("cluster.lock"),
            manifest_path: dir.path().join("cluster.toml"),
            old_manifest_content: old_content.to_string(),
            config,
        };

        let result = execute_refresh(&plan, &params).unwrap();
        assert!(result.outcome.is_some());

        let manifest = std::fs::read_to_string(dir.path().join("cluster.toml")).unwrap();
        assert!(manifest.contains("# These are my important notes"));
        assert!(manifest.contains("# about this cluster"));
    }

    #[test]
    fn test_execute_refresh_empty_removes_lock() {
        let dir = tempfile::tempdir().unwrap();
        let lock_path = dir.path().join("cluster.lock");
        let manifest_path = dir.path().join("cluster.toml");

        // Create a lock file that should be removed
        std::fs::write(&lock_path, "old content").unwrap();

        let empty_plan = ClusterGeneratePlan {
            lock_entries: vec![],
            archived: vec![],
            full_coverage_facts: vec![],
            mixed_type_warnings: vec![],
            root_breakdown: vec![],
            not_archived_count: 0,
            excluded_count: 0,
            unhashed_count: 0,
        };

        let config = ManifestConfig {
            meta: ManifestMeta {
                version: 1,
                query: vec![],
                scope: None,
                generated_at: "2026-01-01T00:00:00Z".to_string(),
                lock_hash: "old".to_string(),
            },
            options: ManifestOptions { allow: vec![] },
            output: ManifestOutput {
                pattern: "{filename}".to_string(),
                archive_root_id: 1,
                base_dir: "output".to_string(),
            },
        };

        let params = ExecuteRefreshParams {
            lock_path: lock_path.clone(),
            manifest_path: manifest_path.clone(),
            old_manifest_content: String::new(),
            config,
        };

        let result = execute_refresh(&empty_plan, &params).unwrap();
        assert!(result.outcome.is_none());
        assert!(!lock_path.exists());
        assert!(manifest_path.exists());
    }
}
