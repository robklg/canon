//! Generating and refreshing a cluster manifest.
//!
//! `plan_generate` computes what `cluster generate` would produce — source
//! selection, archive detection, duplicate checking and fact coverage — without
//! side effects. `execute_generate` and `execute_refresh` then write the lock
//! file and the manifest. Manifests are stored artifacts rather than
//! presentation, so composing them belongs here and not in the command.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};

use crate::archive::domain::{
    extract_notes_raw, LockEntry, LockHeader, LockScope, ManifestConfig, ManifestMeta,
    ManifestOptions, ManifestOutput, CURRENT_LOCK_VERSION, CURRENT_MANIFEST_VERSION,
};
use crate::core::domain::include::IncludeSet;
use crate::core::domain::path::path_strip_prefix;
use crate::core::domain::root::Root;
use crate::core::domain::scope::{ScopeMatch, ScopeResolution};
use crate::core::domain::{FactEntry, FactType, FactValue};
use crate::core::repo::{self, Connection};
use crate::expr::Filter;
use crate::expr::{select_sources, RolePolicy, SelectionParams};
use crate::expr::{BuiltinKey, ScopeVantage, OBJECT_HASH, SCOPE_REL_PATH};

use super::manifest::{write_and_sync, write_lock_file};

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
    let selection = select_sources(conn, &selection_params)?;

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

    // 4. Batch fetch archive paths for archive detection. Empty files never
    // come back as archived — the archived-ness SQL carries the contentless
    // law, so archive-everything passes carry them with their folders
    // instead of skipping them as "already archived" (a verbatim folder
    // copy stays faithful).
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

impl ClusterGeneratePlan {
    /// The plan a run that selects nothing produces.
    ///
    /// Not the same as a query that matched nothing, though the outcome is
    /// identical and deliberately so: this is a run whose recorded scope
    /// resolved to no confirmed place at all, so there is nowhere to select
    /// *from*. It must never reach the planner, because an empty scope list
    /// means global there, and a manifest naming a place Canon cannot find
    /// must not become a whole-universe archive.
    pub fn empty() -> Self {
        ClusterGeneratePlan {
            lock_entries: vec![],
            archived: vec![],
            full_coverage_facts: vec![],
            mixed_type_warnings: vec![],
            root_breakdown: vec![],
            not_archived_count: 0,
            excluded_count: 0,
            unhashed_count: 0,
        }
    }
}

// ============================================================================
// The measurement
// ============================================================================

/// Settle where each entry goes, at the moment the selection is settled.
///
/// One function, called by both writers — generation and refresh — which is
/// the whole point: two similar loops is the drift this class of defect comes
/// from, and the two files a user compares must be byte-identical for the same
/// inputs.
///
/// One [`ScopeVantage`] for the run, then one `path_strip_prefix` per entry.
/// Both are borrowed rules, not re-derived ones: what "the scope" means when
/// there is more than one belongs to the vantage, and containment belongs to
/// the path law.
///
/// After a fresh generate or refresh every value is `Some` **wherever the run
/// confirmed a scope at all** — entries are selected from the very register
/// this measures from (`ScopeResolution::selection`), so every entry's root
/// has a vantage and every entry lies under it. A run that confirmed nothing
/// measures nothing, correctly: there is nowhere to measure from. So `None`
/// on a fresh lock means the header's scope is empty, and `None` on a lock
/// with no header at all means the lock predates the field — which is the
/// whole of what `LockFile::unmeasured_reason` has to tell apart.
pub(super) fn measure_entries(entries: &mut [LockEntry], scope: &ScopeResolution, roots: &[Root]) {
    let vantage = ScopeVantage::new(scope);
    let root_paths: HashMap<i64, &str> = roots.iter().map(|r| (r.id, r.path.as_str())).collect();

    for entry in entries {
        entry.scope_rel_path = root_paths
            .get(&entry.root_id)
            .and_then(|root_path| vantage.for_root(root_path))
            .and_then(|measured_from| path_strip_prefix(&entry.path, measured_from))
            .map(str::to_string);
    }
}

/// The header a run writes: what it settled, as against what the manifest
/// declares. Confirmed scopes only — a set-aside or unrooted line contributes
/// nothing to a destination and must contribute nothing to the record either.
fn lock_header(scope: &ScopeResolution) -> LockHeader {
    LockHeader {
        lock_version: CURRENT_LOCK_VERSION,
        scope: scope
            .scopes()
            .iter()
            .map(|s| LockScope {
                root_id: s.root_id,
                root_path: s.root_path.clone(),
                rel_prefix: s.rel_prefix.clone(),
            })
            .collect(),
    }
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
    /// The run's scope, already resolved against the known roots — what goes
    /// into `meta.scope`, what the measurement is taken from, and what the
    /// lock header records. One value, because it is one answer: two fields
    /// meaning "the scope" is how the readers drifted apart before.
    pub scope: ScopeResolution,
    /// The known roots, for turning an entry's `root_id` into the path the
    /// vantage is keyed on.
    pub roots: Vec<Root>,
    pub archive_root_id: i64,
    pub base_dir: String,
    pub allow: Vec<String>,
}

/// Result from executing a cluster generation — display data for the interface.
pub struct ExecuteGenerateResult {
    pub source_count: usize,
    pub root_breakdown: Vec<(String, usize)>,
    pub not_archived_count: usize,
    /// The pattern the manifest was written with — chosen here on generation,
    /// carried over from the manifest on refresh. The interface reads it back
    /// to say what the run committed to.
    pub pattern: String,
}

impl ExecuteGenerateResult {
    /// Compose the cluster summary given a header line (provided by interface
    /// since it includes display paths that ops doesn't know).
    pub fn compose_summary(&self, header: &str) -> String {
        use crate::core::domain::format_count;
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
    /// The manifest's own scope, resolved: what goes back into `meta.scope`,
    /// what the measurement is taken from, and what the lock header records.
    /// The caller's resolution of what the manifest recorded, not the recorded
    /// text itself. Separate from
    /// `config` because the config carries what was *read*, and a refresh
    /// writes the document out healed: a rooted prefix in the byte-form the
    /// index stores, an unrooted one verbatim.
    pub scope: ScopeResolution,
    /// The known roots — see [`ExecuteGenerateParams::roots`].
    pub roots: Vec<Root>,
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
///
/// Both files are created outright, replacing whatever is at those paths.
/// Refusing to overwrite an existing manifest — and letting a flag override
/// that refusal — is the caller's job, and only for this entry point: a
/// refresh is meant to rewrite in place.
pub fn execute_generate(
    plan: &mut ClusterGeneratePlan,
    params: &ExecuteGenerateParams,
) -> Result<ExecuteGenerateResult> {
    // Settled before anything is written, so no writer can forget it.
    measure_entries(&mut plan.lock_entries, &params.scope, &params.roots);

    // Create destination directory if needed (after plan confirmed sources exist)
    if let Some(parent) = params.manifest_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create directory: {}", parent.display()))?;
    }

    // Write JSONL lock file
    // Order matters: the lock file is written, then hashed, and the hash is
    // embedded in the manifest written further below. Any other order records
    // a hash of bytes that were never on disk, and every later apply refuses
    // the pair. Both files are synced, so the durable manifest cannot name a
    // lock that did not survive alongside it.
    write_lock_file(
        &params.lock_path,
        &lock_header(&params.scope),
        &plan.lock_entries,
    )?;

    // Compute lock file hash
    let lock_hash = crate::core::ops::fs::compute_full_hash(&params.lock_path)?;

    let config = ManifestConfig {
        meta: ManifestMeta {
            version: CURRENT_MANIFEST_VERSION,
            query: params.expanded_filters.clone(),
            scope: params.scope.recorded().to_vec(),
            generated_at: current_timestamp(),
            lock_hash,
        },
        options: ManifestOptions {
            allow: params.allow.clone(),
        },
        output: ManifestOutput {
            pattern: default_pattern(params.scope.recorded()).to_string(),
            archive_root_id: params.archive_root_id,
            base_dir: params.base_dir.clone(),
        },
    };

    // Inject "# Original:" comments when alias expansion changed the filter
    let comment_lines: Vec<String> = params
        .original_filters
        .iter()
        .zip(params.expanded_filters.iter())
        .filter(|(orig, exp)| orig != exp)
        .map(|(orig, _)| format!("# Original: {orig}"))
        .collect();

    let manifest = assemble_manifest(plan, &config, EMPTY_NOTES, &comment_lines)?;
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
        pattern: config.output.pattern.clone(),
    })
}

/// Rewrite lock file + update existing manifest for a cluster refresh.
///
/// Both arms — matches and no matches — write the same document through the
/// same assembly. A refresh that found nothing still rewrites a manifest the
/// user can read and edit; it must not hand back a bare TOML body with the
/// user's Notes gone.
pub fn execute_refresh(
    plan: &mut ClusterGeneratePlan,
    params: &ExecuteRefreshParams,
) -> Result<ExecuteRefreshResult> {
    // The same act as generation's, through the same function.
    measure_entries(&mut plan.lock_entries, &params.scope, &params.roots);

    let matched_nothing = plan.lock_entries.is_empty();

    // A lock file states what a query matched. With no matches there is
    // nothing to state, so the file goes and the manifest records the empty
    // hash rather than pointing at a stale lock.
    let lock_hash = if matched_nothing {
        if params.lock_path.exists() {
            fs::remove_file(&params.lock_path)?;
        }
        String::new()
    } else {
        // Order matters exactly as it does for generation: write the lock,
        // then hash what landed on disk, then name that hash in the manifest.
        write_lock_file(
            &params.lock_path,
            &lock_header(&params.scope),
            &plan.lock_entries,
        )?;
        crate::core::ops::fs::compute_full_hash(&params.lock_path)?
    };

    let config = ManifestConfig {
        meta: ManifestMeta {
            // A refresh rewrites the whole document, so what lands on disk is
            // this version's format whatever the old one declared. Carrying
            // the old number forward would put a v1 stamp on a v2 body — the
            // one thing the version field exists to stop.
            version: CURRENT_MANIFEST_VERSION,
            query: params.config.meta.query.clone(),
            scope: params.scope.recorded().to_vec(),
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

    // The user's words come back verbatim. A manifest that never carried a
    // Notes block gets the empty one, so the next refresh has somewhere to
    // read from.
    let notes =
        extract_notes_raw(&params.old_manifest_content).unwrap_or_else(|| EMPTY_NOTES.to_string());

    let manifest = assemble_manifest(plan, &config, &notes, &[])?;
    write_and_sync(&params.manifest_path, &manifest)
        .with_context(|| format!("Failed to write config: {}", params.manifest_path.display()))?;

    Ok(ExecuteRefreshResult {
        outcome: (!matched_nothing).then(|| ExecuteGenerateResult {
            source_count: plan.lock_entries.len(),
            root_breakdown: plan.root_breakdown.clone(),
            not_archived_count: plan.not_archived_count,
            pattern: config.output.pattern.clone(),
        }),
    })
}

// ============================================================================
// Private helpers
// ============================================================================

/// The Notes body a manifest starts life with: one empty comment line under
/// the marker, so the block is there to be written in.
const EMPTY_NOTES: &str = "\n#\n";

/// The pattern a freshly generated manifest starts with.
///
/// Structure, not a flat heap: the folders a file was found in are the user's
/// own arrangement, and a default that discards them makes what landed in the
/// archive unrecoverable by hand. It also spares the run the collisions a flat
/// default guarantees, where every folder contributes its own `IMG_0001.jpg`.
///
/// Any scope at all measures from the scope — the place the user named and
/// the shape they were looking at, however many paths they named. No scope
/// measures from each source's own root, because there is nothing else to
/// measure from.
///
/// It is a default, not a rule: the pattern is the line of the manifest the
/// user is most invited to edit, and `{filename}` remains one edit away.
fn default_pattern(recorded_scope: &[String]) -> &'static str {
    if recorded_scope.is_empty() {
        "{source.rel_path}"
    } else {
        "{scope.rel_path}"
    }
}

/// Whether a file about to be written stands where the pattern's placements
/// need a directory.
///
/// The manifest and its lock usually live in the same tree the files land in,
/// so the two can collide: `-O photos` beside a pattern placing into `photos/`
/// leaves a file standing where a directory has to go, and every transfer then
/// fails on a directory it cannot create. It is knowable at write time — the
/// pattern's static prefix says which directories the run needs — so it is said
/// then, while the manifest is still a text file nobody has acted on.
///
/// Returns the written path that stands in the way and the directory it
/// blocks. Only the directories the pattern *commits* to are checked: what
/// expansion opens below them depends on values this side cannot see.
/// A pattern that does not parse is left to the apply, which reports it
/// properly.
pub fn placement_blocked_by_written_file(
    pattern: &str,
    placement_root: &Path,
    written_paths: &[&Path],
) -> Option<(PathBuf, PathBuf)> {
    let parsed = crate::expr::parse_pattern(pattern).ok()?;
    let (static_prefix, _fans_out) = crate::expr::placement_shape(&parsed);
    let placement_dir = match static_prefix {
        Some(prefix) => placement_root.join(prefix),
        None => placement_root.to_path_buf(),
    };

    // The whole chain, not just the leaf: a file standing anywhere along it
    // blocks the directory below it just as thoroughly.
    for dir in placement_dir.ancestors() {
        for written in written_paths {
            if *written == dir {
                return Some((written.to_path_buf(), placement_dir));
            }
        }
    }
    None
}

/// Assemble the manifest document every write path emits: header, Cluster
/// Summary, Notes block, TOML body, fact help.
///
/// One assembly for generation and both refresh arms. The manifest's commented
/// shape is what the user edits and what the notes parser reads back, so a
/// second writer with its own idea of the layout is how the shape — and the
/// user's words with it — goes missing.
///
/// `notes` is the Notes body as it follows the marker line, `#` markers intact.
/// `original_comments` are the `# Original:` lines alias expansion leaves behind
/// (generation only; a refresh re-emits the query it was given).
fn assemble_manifest(
    plan: &ClusterGeneratePlan,
    config: &ManifestConfig,
    notes: &str,
    original_comments: &[String],
) -> Result<String> {
    let header = "# Canon manifest — edit pattern and Notes freely.\n\
                  # To change the query, edit it here then run: canon cluster refresh <manifest>\n\
                  # Other fields are managed by Canon — do not edit.\n\
                  #\n";
    let summary = generate_summary_comments(plan);
    let notes_block = format!("# === Notes ==={notes}");
    // Empty at zero sources by the fact help's own rule — there is no coverage
    // to enumerate over nothing. The section is absent, not dropped.
    let fact_help = generate_fact_help(
        plan.lock_entries.len(),
        &plan.full_coverage_facts,
        !config.meta.scope.is_empty(),
    );

    let toml_str = toml::to_string_pretty(config).context("Failed to serialize manifest config")?;
    let toml_str = if original_comments.is_empty() {
        toml_str
    } else {
        inject_comments_before_key(&toml_str, "query", original_comments)
    };

    Ok(format!(
        "{}{}\n{}\n{}\n\n{}",
        header,
        summary.trim_end(),
        notes_block.trim_end(),
        toml_str.trim_end(),
        fact_help
    ))
}

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

/// A fully-covered fact: (key, dominant type, description).
type FullCoverageFact = (String, FactType, String);
/// A mixed-type warning: (key, type breakdown).
type MixedTypeWarning = (String, String);

/// Compute facts with 100% coverage and mixed-type warnings.
///
/// Returns (full_coverage_facts, mixed_type_warnings).
fn compute_full_coverage_facts(
    entries: &[LockEntry],
    all_facts: &HashMap<i64, Vec<FactEntry>>,
) -> (Vec<FullCoverageFact>, Vec<MixedTypeWarning>) {
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
    let mut mixed_type_warnings: Vec<MixedTypeWarning> = Vec::new();
    for (key, tracker) in &fact_counts {
        if tracker.count == source_count && tracker.has_mixed_types() {
            mixed_type_warnings.push((key.clone(), tracker.type_breakdown()));
        }
    }
    mixed_type_warnings.sort_by(|a, b| a.0.cmp(&b.0));

    // Filter to only 100% coverage facts
    let mut full_coverage: Vec<FullCoverageFact> = fact_counts
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
// Manifest content helpers
// ============================================================================

fn current_timestamp() -> String {
    chrono::Utc::now().to_rfc3339()
}

/// Generate cluster summary comment block for the manifest.
fn generate_summary_comments(plan: &ClusterGeneratePlan) -> String {
    use crate::core::domain::format_count;

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
        OBJECT_HASH, "text", "Content hash (if hashed)"
    ));
    if has_scope {
        help.push_str(&format!(
            "#   {:18} {:6} - {}\n",
            SCOPE_REL_PATH, "path", "Path relative to the manifest scope"
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::testing::{
        insert_fact, insert_object, insert_root, insert_source, insert_source_excluded,
        setup_test_db,
    };
    use crate::expr::Unmeasured;

    fn default_params() -> ClusterGenerateParams {
        ClusterGenerateParams {
            scopes: vec![],
            filters: vec![],
            allow_archived: false,
            allow_duplicates: false,
        }
    }

    /// A plan that matched nothing — the refresh arm this story routes through
    /// the shared assembly.
    fn empty_plan() -> ClusterGeneratePlan {
        ClusterGeneratePlan::empty()
    }

    fn refresh_config() -> ManifestConfig {
        ManifestConfig {
            meta: ManifestMeta {
                version: 1,
                query: vec![],
                scope: vec![],
                generated_at: "2026-01-01T00:00:00Z".to_string(),
                lock_hash: "old".to_string(),
            },
            options: ManifestOptions { allow: vec![] },
            output: ManifestOutput {
                pattern: "{filename}".to_string(),
                archive_root_id: 1,
                base_dir: "output".to_string(),
            },
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
                scope_rel_path: None,
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
            scope: scope_of(&conn, &["/photos".to_string()]),
            roots: crate::core::repo::root::fetch_all(&conn).unwrap(),
            archive_root_id: 1,
            base_dir: "output".to_string(),
            allow: vec![],
        };

        let mut plan = plan;
        let result = execute_generate(&mut plan, &params).unwrap();
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
        assert!(manifest.contains("pattern = \"{scope.rel_path}\""));
    }

    /// The scope a generate test hands `execute_generate`, resolved through
    /// the very function `cluster generate` resolves through — so a fixture
    /// cannot drift from what a run actually hands these functions.
    fn scope_of(conn: &Connection, prefixes: &[String]) -> ScopeResolution {
        crate::core::ops::scope::resolve_recorded_scope(
            conn,
            prefixes,
            &crate::core::repo::root::fetch_all(conn).unwrap(),
        )
        .unwrap()
    }

    /// The same, without a database: every prefix confirmed on its root match
    /// alone. For the measurement cases, which are about `ScopeVantage` and
    /// `path_strip_prefix` and have no index to consult.
    fn attributed(prefixes: &[&str], roots: &[Root]) -> ScopeResolution {
        use crate::core::domain::scope::{attribute_prefix, PrefixOutcome};
        ScopeResolution::from_outcomes(
            prefixes
                .iter()
                .map(|p| match attribute_prefix(p, roots) {
                    Some(scope) => PrefixOutcome::Confirmed(scope),
                    None => PrefixOutcome::Unrooted(p.to_string()),
                })
                .collect(),
        )
    }

    // =========================================================================
    // The measurement — what the lock settles that nothing can recover later
    // =========================================================================

    /// The test that proves the field earns its place.
    ///
    /// The vantage is *where the user pointed*, which may be shallower than
    /// where the files are. With one scope at `/R/proj` and every file under
    /// `/R/proj/src`, the measurement is `src/main.c` — and the common prefix
    /// of the lock's own contents is `/R/proj/src`, which would hand back
    /// `main.c` and lose the `src/` level. The information is genuinely absent
    /// from the entries, which is why it has to be recorded when the selection
    /// is made rather than derived afterwards.
    #[test]
    fn the_measurement_cannot_be_recovered_from_the_entries() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/R", "source", false);
        let roots = crate::core::repo::root::fetch_all(&conn).unwrap();

        let mut entries: Vec<LockEntry> = ["proj/src/main.c", "proj/src/util.c"]
            .iter()
            .map(|rel| lock_entry_at(root, &format!("/R/{rel}")))
            .collect();

        measure_entries(&mut entries, &attributed(&["/R/proj"], &roots), &roots);

        assert_eq!(
            entries
                .iter()
                .map(|e| e.scope_rel_path.clone().unwrap())
                .collect::<Vec<_>>(),
            ["src/main.c", "src/util.c"],
            "the level the user pointed above must survive"
        );

        // And the counter-derivation, spelled out: the deepest directory
        // containing every entry is one level lower, so anything reading the
        // entries would answer differently.
        assert_eq!(
            crate::core::domain::path::common_path_prefix(entries.iter().map(|e| e.path.as_str())),
            "/R/proj/src",
        );
    }

    /// Sibling scopes: each entry's measurement keeps its own scope's name, so
    /// two siblings cannot collide at the destination. The vantage's rule,
    /// seen through the lock it is now written into.
    #[test]
    fn sibling_scopes_keep_their_own_names_in_the_measurement() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/R", "source", false);
        let roots = crate::core::repo::root::fetch_all(&conn).unwrap();

        let mut entries: Vec<LockEntry> = ["work/proj-v1/main.c", "work/proj-v2/main.c"]
            .iter()
            .map(|rel| lock_entry_at(root, &format!("/R/{rel}")))
            .collect();

        measure_entries(
            &mut entries,
            &attributed(&["/R/work/proj-v1", "/R/work/proj-v2"], &roots),
            &roots,
        );

        assert_eq!(
            entries
                .iter()
                .map(|e| e.scope_rel_path.clone().unwrap())
                .collect::<Vec<_>>(),
            ["proj-v1/main.c", "proj-v2/main.c"],
        );
    }

    /// A lock entry standing at `path` under `root_id`, with the rest of the
    /// snapshot fixed — these cases are about the measurement and nothing
    /// else.
    fn lock_entry_at(root_id: i64, path: &str) -> LockEntry {
        LockEntry {
            id: 1,
            root_id,
            path: path.to_string(),
            device: 0,
            inode: 0,
            size: 1,
            mtime: 0,
            partial_hash: String::new(),
            object_id: None,
            hash_type: None,
            hash_value: None,
            scope_rel_path: None,
        }
    }

    /// The same-code rule, pinned rather than trusted: generation and refresh
    /// resolve scope through one pipeline and must write **byte-identical** locks
    /// from the same inputs. Two similar loops is the drift this whole class
    /// of defect comes from, so what is asserted is the file, not a field.
    #[test]
    fn generate_and_refresh_write_the_same_lock() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "hash1", false);
        insert_source(&conn, root, "trip/day1/photo.jpg", Some(obj));
        let roots = crate::core::repo::root::fetch_all(&conn).unwrap();
        let scope = || attributed(&["/photos/trip"], &roots);

        let dir = tempfile::tempdir().unwrap();
        let gen_lock = dir.path().join("gen.lock");
        let mut plan = plan_generate(&mut conn, &default_params()).unwrap();
        execute_generate(
            &mut plan,
            &ExecuteGenerateParams {
                lock_path: gen_lock.clone(),
                manifest_path: dir.path().join("gen.toml"),
                expanded_filters: vec![],
                original_filters: vec![],
                scope: scope(),
                roots: roots.clone(),
                archive_root_id: 1,
                base_dir: String::new(),
                allow: vec![],
            },
        )
        .unwrap();

        let refresh_lock = dir.path().join("refresh.lock");
        let mut plan = plan_generate(&mut conn, &default_params()).unwrap();
        execute_refresh(
            &mut plan,
            &ExecuteRefreshParams {
                lock_path: refresh_lock.clone(),
                manifest_path: dir.path().join("refresh.toml"),
                old_manifest_content: String::new(),
                scope: scope(),
                roots: roots.clone(),
                config: refresh_config(),
            },
        )
        .unwrap();

        assert_eq!(
            std::fs::read_to_string(&gen_lock).unwrap(),
            std::fs::read_to_string(&refresh_lock).unwrap(),
            "generate and refresh must settle a manifest identically"
        );
        // And it is not vacuously equal to an unmeasured lock.
        assert!(
            std::fs::read_to_string(&gen_lock)
                .unwrap()
                .contains("\"scope_rel_path\":\"day1/photo.jpg\""),
            "the lock carries the measurement"
        );
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
            scope: scope_of(&conn, &["/photos".to_string()]),
            roots: crate::core::repo::root::fetch_all(&conn).unwrap(),
            archive_root_id: 1,
            base_dir: "output".to_string(),
            allow: vec![],
        };

        let mut plan = plan;
        execute_generate(&mut plan, &params).unwrap();

        let manifest = std::fs::read_to_string(dir.path().join("cluster.toml")).unwrap();
        assert!(manifest.contains("# Original: @image"));
    }

    // =========================================================================
    // The default pattern — what a manifest starts with before any edit
    // =========================================================================

    /// Generate with the given scopes and hand back the pattern the manifest
    /// was born with.
    fn generated_pattern(scope_prefixes: Vec<String>) -> String {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "hash1", false);
        insert_source(&conn, root, "trip/day1/photo.jpg", Some(obj));

        let mut plan = plan_generate(&mut conn, &default_params()).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let manifest_path = dir.path().join("cluster.toml");
        let params = ExecuteGenerateParams {
            lock_path: dir.path().join("cluster.lock"),
            manifest_path: manifest_path.clone(),
            expanded_filters: vec![],
            original_filters: vec![],
            scope: scope_of(&conn, &scope_prefixes),
            roots: crate::core::repo::root::fetch_all(&conn).unwrap(),
            archive_root_id: 1,
            base_dir: "output".to_string(),
            allow: vec![],
        };
        execute_generate(&mut plan, &params).unwrap();

        let manifest = std::fs::read_to_string(&manifest_path).unwrap();
        let config: ManifestConfig = toml::from_str(&manifest).unwrap();
        config.output.pattern
    }

    #[test]
    fn default_pattern_is_scope_relative_for_a_single_scope() {
        // The scope is the place the user named, so it is the shape they are
        // looking at and the one to measure from.
        assert_eq!(
            generated_pattern(vec!["/photos/trip".to_string()]),
            "{scope.rel_path}"
        );
    }

    /// E3 — with no scope there is nothing else to measure from, so each
    /// source measures from its own root.
    #[test]
    fn default_pattern_is_root_relative_when_unscoped() {
        assert_eq!(generated_pattern(vec![]), "{source.rel_path}");
    }

    /// E2 — any scope at all measures from the scope, not just one. Several
    /// scopes measure from their shared vantage, which keeps each scope's own
    /// name at the destination.
    #[test]
    fn a_scoped_generation_starts_at_the_scope_relative_pattern() {
        assert_eq!(
            generated_pattern(vec![
                "/photos/trip".to_string(),
                "/photos/scans".to_string()
            ]),
            "{scope.rel_path}"
        );
    }

    /// E4 — the one place the key cannot be written as the const: the default
    /// is a `&'static str` literal. This is what carries the one spelling
    /// across it.
    #[test]
    fn the_default_pattern_names_the_key_by_its_one_spelling() {
        assert_eq!(
            generated_pattern(vec!["/photos/trip".to_string()]),
            format!("{{{SCOPE_REL_PATH}}}")
        );
    }

    /// The default carried through to where it decides file placement: two
    /// files sharing a name in different folders land in different places,
    /// keeping the shape they were found in. Under the old flat default this
    /// pair was a collision, and the whole apply refused.
    #[test]
    fn the_default_pattern_keeps_a_nested_tree_apart_at_apply_time() {
        use crate::archive::ops::plan::{plan_apply, ApplyPlanParams};
        use crate::expr::{extract_fact_keys, parse_pattern};

        let tree = tempfile::tempdir().unwrap();
        let root_path = tree.path().to_str().unwrap().to_string();
        for day in ["day1", "day2"] {
            let dir = tree.path().join("trip").join(day);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("IMG_0001.jpg"), day).unwrap();
        }

        let mut conn = setup_test_db();
        let root = insert_root(&conn, &root_path, "source", false);
        let archive = insert_root(&conn, "/archive", "archive", false);
        for (day, hash) in [("day1", "hash-day1"), ("day2", "hash-day2")] {
            let obj = insert_object(&conn, hash, false);
            insert_source(&conn, root, &format!("trip/{day}/IMG_0001.jpg"), Some(obj));
        }

        // Generate, then read the pattern back off the manifest — what apply
        // plans with is what generation wrote, not a literal repeated here.
        let mut gen_plan = plan_generate(&mut conn, &default_params()).unwrap();
        let out = tempfile::tempdir().unwrap();
        let manifest_path = out.path().join("cluster.toml");
        let scope = format!("{root_path}/trip");
        execute_generate(
            &mut gen_plan,
            &ExecuteGenerateParams {
                lock_path: out.path().join("cluster.lock"),
                manifest_path: manifest_path.clone(),
                expanded_filters: vec![],
                original_filters: vec![],
                scope: scope_of(&conn, std::slice::from_ref(&scope)),
                roots: crate::core::repo::root::fetch_all(&conn).unwrap(),
                archive_root_id: archive,
                base_dir: String::new(),
                allow: vec![],
            },
        )
        .unwrap();
        let config: ManifestConfig =
            toml::from_str(&std::fs::read_to_string(&manifest_path).unwrap()).unwrap();
        let pattern = parse_pattern(&config.output.pattern).unwrap();
        let needed_keys = extract_fact_keys(&pattern);

        let sources: Vec<&LockEntry> = gen_plan.lock_entries.iter().collect();
        let mut root_paths = HashMap::new();
        root_paths.insert(root, root_path.clone());
        root_paths.insert(archive, "/archive".to_string());
        let apply_plan = plan_apply(
            &mut conn,
            &ApplyPlanParams {
                sources: &sources,
                pattern: &pattern,
                needed_keys: &needed_keys,
                // These two cases plan against a lock generation just wrote,
                // so every entry is measured and this arm is never taken.
                unmeasured: Unmeasured::NoScopeRecorded,
                root_paths: &root_paths,
                archive_root_id: archive,
                base_dir_rel: "",
                resume: false,
                progress: None,
            },
        )
        .unwrap();

        assert!(
            apply_plan.violations.collisions.is_empty(),
            "same-named files in different folders collided: {:?}",
            apply_plan.violations.collisions
        );
        let mut dests: Vec<&str> = apply_plan
            .transfers
            .iter()
            .map(|t| t.dest_rel_path.as_str())
            .collect();
        dests.sort();
        assert_eq!(dests, vec!["day1/IMG_0001.jpg", "day2/IMG_0001.jpg"]);
    }

    /// E1 — the test that would have caught this. Generating over two sibling
    /// scopes and planning the apply must keep each scope's own name at the
    /// destination, and must not merge the two trees into one. The pattern is
    /// read back off the written manifest rather than repeated as a literal,
    /// so what apply plans with is what generation wrote.
    ///
    /// Under the rejected per-source-own-scope reading both files would render
    /// as `src/main.c`, so the empty-collisions assertion is the second thing
    /// this test pins.
    #[test]
    fn sibling_scopes_keep_their_own_names_at_the_destination() {
        use crate::archive::ops::plan::{plan_apply, ApplyPlanParams};
        use crate::expr::{extract_fact_keys, parse_pattern};

        let tree = tempfile::tempdir().unwrap();
        let root_path = tree.path().to_str().unwrap().to_string();
        for project in ["proj-v1", "proj-v2"] {
            let dir = tree.path().join("work").join(project).join("src");
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("main.c"), project).unwrap();
        }

        let mut conn = setup_test_db();
        let root = insert_root(&conn, &root_path, "source", false);
        let archive = insert_root(&conn, "/archive", "archive", false);
        for (project, hash) in [("proj-v1", "hash-v1"), ("proj-v2", "hash-v2")] {
            let obj = insert_object(&conn, hash, false);
            insert_source(
                &conn,
                root,
                &format!("work/{project}/src/main.c"),
                Some(obj),
            );
        }

        let scopes = vec![
            format!("{root_path}/work/proj-v1"),
            format!("{root_path}/work/proj-v2"),
        ];
        let mut gen_plan = plan_generate(&mut conn, &default_params()).unwrap();
        let out = tempfile::tempdir().unwrap();
        let manifest_path = out.path().join("cluster.toml");
        execute_generate(
            &mut gen_plan,
            &ExecuteGenerateParams {
                lock_path: out.path().join("cluster.lock"),
                manifest_path: manifest_path.clone(),
                expanded_filters: vec![],
                original_filters: vec![],
                scope: scope_of(&conn, &scopes),
                roots: crate::core::repo::root::fetch_all(&conn).unwrap(),
                archive_root_id: archive,
                base_dir: String::new(),
                allow: vec![],
            },
        )
        .unwrap();

        let config: ManifestConfig =
            toml::from_str(&std::fs::read_to_string(&manifest_path).unwrap()).unwrap();
        assert_eq!(config.output.pattern, "{scope.rel_path}");
        let pattern = parse_pattern(&config.output.pattern).unwrap();
        let needed_keys = extract_fact_keys(&pattern);

        let sources: Vec<&LockEntry> = gen_plan.lock_entries.iter().collect();
        let mut root_paths = HashMap::new();
        root_paths.insert(root, root_path.clone());
        root_paths.insert(archive, "/archive".to_string());
        let apply_plan = plan_apply(
            &mut conn,
            &ApplyPlanParams {
                sources: &sources,
                pattern: &pattern,
                needed_keys: &needed_keys,
                // These two cases plan against a lock generation just wrote,
                // so every entry is measured and this arm is never taken.
                unmeasured: Unmeasured::NoScopeRecorded,
                root_paths: &root_paths,
                archive_root_id: archive,
                base_dir_rel: "",
                resume: false,
                progress: None,
            },
        )
        .unwrap();

        assert!(
            apply_plan.violations.collisions.is_empty(),
            "sibling scopes merged into one tree: {:?}",
            apply_plan.violations.collisions
        );
        let mut dests: Vec<&str> = apply_plan
            .transfers
            .iter()
            .map(|t| t.dest_rel_path.as_str())
            .collect();
        dests.sort();
        assert_eq!(dests, vec!["proj-v1/src/main.c", "proj-v2/src/main.c"]);
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
            scope: ScopeResolution::from_outcomes(vec![]),
            roots: vec![],
            config,
        };

        let mut plan = plan;
        let result = execute_refresh(&mut plan, &params).unwrap();
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

        let params = ExecuteRefreshParams {
            lock_path: lock_path.clone(),
            manifest_path: manifest_path.clone(),
            old_manifest_content: String::new(),
            scope: ScopeResolution::from_outcomes(vec![]),
            roots: vec![],
            config: refresh_config(),
        };

        let result = execute_refresh(&mut empty_plan(), &params).unwrap();
        assert!(result.outcome.is_none());
        assert!(!lock_path.exists());
        assert!(manifest_path.exists());
    }

    #[test]
    fn execute_refresh_empty_arm_preserves_notes() {
        let dir = tempfile::tempdir().unwrap();
        let manifest_path = dir.path().join("cluster.toml");

        let old_content = "\
# === Cluster Summary ===\n\
# 3 sources from 1 root:\n\
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

        let params = ExecuteRefreshParams {
            lock_path: dir.path().join("cluster.lock"),
            manifest_path: manifest_path.clone(),
            old_manifest_content: old_content.to_string(),
            scope: ScopeResolution::from_outcomes(vec![]),
            roots: vec![],
            config: refresh_config(),
        };

        let result = execute_refresh(&mut empty_plan(), &params).unwrap();
        assert!(result.outcome.is_none());

        // A query that now matches nothing is not a reason to lose the words
        // the user wrote about it.
        let manifest = std::fs::read_to_string(&manifest_path).unwrap();
        assert!(
            manifest.contains("# These are my important notes"),
            "got: {manifest}"
        );
        assert!(manifest.contains("# about this cluster"), "got: {manifest}");
        let raw = extract_notes_raw(&manifest).expect("the empty arm still writes a notes block");
        assert!(
            !raw.contains("[meta]"),
            "notes block ran into the TOML body"
        );
    }

    #[test]
    fn execute_refresh_empty_arm_carries_header_and_summary() {
        let dir = tempfile::tempdir().unwrap();
        let manifest_path = dir.path().join("cluster.toml");

        let params = ExecuteRefreshParams {
            lock_path: dir.path().join("cluster.lock"),
            manifest_path: manifest_path.clone(),
            old_manifest_content: String::new(),
            scope: ScopeResolution::from_outcomes(vec![]),
            roots: vec![],
            config: refresh_config(),
        };

        execute_refresh(&mut empty_plan(), &params).unwrap();

        let manifest = std::fs::read_to_string(&manifest_path).unwrap();
        assert!(manifest.starts_with("# Canon manifest"), "got: {manifest}");
        assert!(
            manifest.contains("# === Cluster Summary ==="),
            "got: {manifest}"
        );
        // The 0 match is stated, not left to be inferred from an absent lock.
        assert!(
            manifest.contains("# 0 sources from 0 roots:"),
            "got: {manifest}"
        );
        assert!(manifest.contains("# === Notes ==="), "got: {manifest}");

        // Still a manifest the next refresh can read.
        let reparsed: ManifestConfig = toml::from_str(&manifest).unwrap();
        assert_eq!(reparsed.meta.lock_hash, "");
        assert_eq!(reparsed.output.pattern, "{filename}");
    }

    // =========================================================================
    // compose_summary — the body ops writes under the interface's header
    // =========================================================================

    #[test]
    fn compose_summary_says_root_for_one_and_roots_for_several() {
        let one = ExecuteGenerateResult {
            source_count: 3,
            root_breakdown: vec![("/photos".to_string(), 3)],
            not_archived_count: 1,
            pattern: "{filename}".to_string(),
        };
        let summary = one.compose_summary("Generated manifest: cluster.toml");
        assert!(summary.contains("From 1 root:"), "got: {summary}");
        assert!(summary.contains("/photos  (3)"), "got: {summary}");

        let several = ExecuteGenerateResult {
            source_count: 5,
            root_breakdown: vec![("/photos".to_string(), 3), ("/scans".to_string(), 2)],
            not_archived_count: 4,
            pattern: "{filename}".to_string(),
        };
        let summary = several.compose_summary("Generated manifest: cluster.toml");
        assert!(summary.contains("From 2 roots:"), "got: {summary}");
        assert!(summary.contains("/scans  (2)"), "got: {summary}");
    }

    #[test]
    fn compose_summary_keeps_the_header_first_and_the_unarchived_count_last() {
        let result = ExecuteGenerateResult {
            source_count: 2,
            root_breakdown: vec![("/photos".to_string(), 2)],
            not_archived_count: 2,
            pattern: "{filename}".to_string(),
        };
        let summary = result.compose_summary("Generated manifest: cluster.toml (2 sources)");
        let lines: Vec<&str> = summary.lines().collect();
        assert_eq!(lines[0], "Generated manifest: cluster.toml (2 sources)");
        assert_eq!(lines[lines.len() - 1], "  2 have no archived copy");
    }

    // =========================================================================
    // The generated manifest and the notes parser are one contract
    // =========================================================================

    #[test]
    fn generate_output_round_trips_through_the_notes_parser() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "hash1", false);
        insert_source(&conn, root, "photo.jpg", Some(obj));

        let plan = plan_generate(&mut conn, &default_params()).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let manifest_path = dir.path().join("cluster.toml");
        let params = ExecuteGenerateParams {
            lock_path: dir.path().join("cluster.lock"),
            manifest_path: manifest_path.clone(),
            expanded_filters: vec![],
            original_filters: vec![],
            scope: scope_of(&conn, &[]),
            roots: crate::core::repo::root::fetch_all(&conn).unwrap(),
            archive_root_id: 1,
            base_dir: String::new(),
            allow: vec![],
        };
        let mut plan = plan;
        execute_generate(&mut plan, &params).unwrap();

        // The writer's section order and the parser's idea of where a section
        // ends are a single contract, and the tests on either side of it use
        // hand-written strings. This is the one that reads what was written.
        let content = std::fs::read_to_string(&manifest_path).unwrap();
        let raw = extract_notes_raw(&content).expect("a generated manifest carries a notes block");
        assert!(raw.contains('#'), "notes block should carry its markers");
        assert!(
            !raw.contains("[meta]"),
            "notes block ran into the TOML body"
        );
        assert!(
            !raw.contains("# === "),
            "notes block ran into the next section"
        );

        // The same contract on the arm that used to write a bare TOML body:
        // refresh what was just generated, matching nothing.
        let refresh_params = ExecuteRefreshParams {
            lock_path: dir.path().join("cluster.lock"),
            manifest_path: manifest_path.clone(),
            old_manifest_content: content,
            scope: ScopeResolution::from_outcomes(vec![]),
            roots: vec![],
            config: toml::from_str(&std::fs::read_to_string(&manifest_path).unwrap()).unwrap(),
        };
        execute_refresh(&mut empty_plan(), &refresh_params).unwrap();

        let refreshed = std::fs::read_to_string(&manifest_path).unwrap();
        let raw =
            extract_notes_raw(&refreshed).expect("an empty refresh still carries a notes block");
        assert!(raw.contains('#'), "notes block should carry its markers");
        assert!(
            !raw.contains("[meta]"),
            "notes block ran into the TOML body"
        );
        assert!(
            !raw.contains("# === "),
            "notes block ran into the next section"
        );
    }
}
