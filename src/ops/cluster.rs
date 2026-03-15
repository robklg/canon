//! Cluster operations — plan for cluster generation.
//!
//! Provides `plan_generate()`, which computes what `cluster generate` would
//! produce — source selection, archive detection, duplicate checking, and
//! fact coverage computation — without any side effects. The interface layer
//! uses the plan to write the lock file and manifest.

use std::collections::{HashMap, HashSet};

use anyhow::{bail, Result};
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
        assert_eq!(
            plan.lock_entries[0].hash_type.as_deref(),
            Some("sha256")
        );
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
}
