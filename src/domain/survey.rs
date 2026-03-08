//! Survey domain logic for canon.
//!
//! Pure functions for outward-looking comparison: scope discovery, "only here"
//! counting, uniqueness counting, and location classification. All functions
//! take pre-computed data structures — no I/O, no database access.

use std::collections::{BTreeMap, HashMap, HashSet};

use super::path::path_is_under;
use super::source::Source;

/// Result of scope discovery for a single root.
pub struct DiscoveredScope {
    /// Relative to root (empty = root itself)
    pub prefix: String,
    /// Overlap count at this scope
    pub count: usize,
}

/// Internal tree node for scope discovery.
struct DirNode {
    /// Files directly in this directory
    direct: usize,
    /// Subdirectories (BTreeMap for deterministic iteration)
    children: BTreeMap<String, DirNode>,
}

impl DirNode {
    fn new() -> Self {
        DirNode {
            direct: 0,
            children: BTreeMap::new(),
        }
    }

    fn count(&self) -> usize {
        self.direct + self.children.values().map(|c| c.count()).sum::<usize>()
    }
}

/// Extract the directory portion of a relative path.
/// "a/b/c.jpg" → "a/b", "file.jpg" → ""
fn dir_portion(rel_path: &str) -> &str {
    match rel_path.rsplit_once('/') {
        Some((dir, _)) => dir,
        None => "",
    }
}

/// Insert a directory path into the tree, incrementing the direct count
/// at the leaf node.
fn insert_into_tree(root: &mut DirNode, dir: &str) {
    if dir.is_empty() {
        root.direct += 1;
        return;
    }
    let mut node = root;
    for segment in dir.split('/') {
        node = node
            .children
            .entry(segment.to_string())
            .or_insert_with(DirNode::new);
    }
    node.direct += 1;
}

/// Join path prefix and segment, handling empty prefix.
fn join_prefix(prefix: &str, segment: &str) -> String {
    if prefix.is_empty() {
        segment.to_string()
    } else {
        format!("{prefix}/{segment}")
    }
}

/// Top-down walk with collapsing rule.
///
/// Three cases:
/// - direct == 0, one child → skip node, recurse into child (collapse)
/// - direct == 0, multiple children → recurse into each child separately
/// - direct > 0 or leaf → emit as scope with total count
fn walk(node: &DirNode, prefix: String, results: &mut Vec<DiscoveredScope>) {
    if node.direct == 0 && node.children.len() == 1 {
        // Collapse: drill into the single child
        let (segment, child) = node.children.iter().next().unwrap();
        walk(child, join_prefix(&prefix, segment), results);
    } else if node.direct == 0 && node.children.len() > 1 {
        // Split: recurse into each child separately
        for (segment, child) in &node.children {
            walk(child, join_prefix(&prefix, segment), results);
        }
    } else {
        // Emit: direct > 0 or leaf node
        results.push(DiscoveredScope {
            prefix,
            count: node.count(),
        });
    }
}

/// Core algorithm: find directory prefixes that concentrate overlap.
///
/// Input: rel_paths of overlapping files within a single root.
/// Extracts directory portions, builds tree, walks with collapsing rule.
///
/// Returns `(prefix, count)` pairs where prefix is relative to the root.
/// An empty prefix means the root itself.
pub fn discover_scopes(rel_paths: &[&str]) -> Vec<DiscoveredScope> {
    if rel_paths.is_empty() {
        return Vec::new();
    }

    let mut root = DirNode::new();
    for rel_path in rel_paths {
        let dir = dir_portion(rel_path);
        insert_into_tree(&mut root, dir);
    }

    let mut results = Vec::new();
    walk(&root, String::new(), &mut results);
    results
}

/// Count object_ids that exist only within location_path.
///
/// For each object_id, checks if ANY source in the universe exists outside
/// the location. Internal duplicates at the location do NOT disqualify.
/// Counts unique object_ids, not sources.
pub fn count_only_here(
    object_ids: &HashSet<i64>,
    location_path: &str,
    by_object_id: &HashMap<i64, Vec<&Source>>,
) -> usize {
    object_ids
        .iter()
        .filter(|&&oid| {
            let sources = match by_object_id.get(&oid) {
                Some(s) => s,
                None => return true, // no sources at all — vacuously only here
            };
            // "Only here" if no source exists outside location_path
            !sources
                .iter()
                .any(|s| !path_is_under(&s.path(), location_path))
        })
        .count()
}

/// Find selection object_ids with no source outside the selection.
/// Returns the set of unique object_ids.
///
/// "Outside" means source.id not in selection_source_ids.
/// Archive copies in the index but not in the selection count as outside.
pub fn find_unique_object_ids(
    selection_object_ids: &HashSet<i64>,
    selection_source_ids: &HashSet<i64>,
    by_object_id: &HashMap<i64, Vec<&Source>>,
) -> HashSet<i64> {
    selection_object_ids
        .iter()
        .copied()
        .filter(|&oid| {
            let sources = match by_object_id.get(&oid) {
                Some(s) => s,
                None => return true, // no sources at all — vacuously unique
            };
            // Unique if every source for this object is in the selection
            !sources.iter().any(|s| !selection_source_ids.contains(&s.id))
        })
        .collect()
}

/// Count selection object_ids with no source outside the selection.
pub fn count_unique_to_selection(
    selection_object_ids: &HashSet<i64>,
    selection_source_ids: &HashSet<i64>,
    by_object_id: &HashMap<i64, Vec<&Source>>,
) -> usize {
    find_unique_object_ids(selection_object_ids, selection_source_ids, by_object_id).len()
}

/// Classification of a related location.
/// Variant order defines sort priority (lowest first): Superset → Lead → Mirror.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LocationKind {
    Superset, // shared >= threshold AND has complementary
    Lead,     // has complementary content
    Mirror,   // overlap only, no complementary
}

/// Classify a location based on shared/complementary data.
/// Guards against division by zero when total_hashed == 0.
pub fn classify_location(
    shared_count: usize,
    total_hashed: usize,
    complementary_count: usize,
    superset_threshold: f64,
) -> LocationKind {
    if complementary_count == 0 {
        return LocationKind::Mirror;
    }
    if total_hashed == 0 {
        return LocationKind::Lead;
    }
    let ratio = shared_count as f64 / total_hashed as f64;
    if ratio >= superset_threshold {
        LocationKind::Superset
    } else {
        LocationKind::Lead
    }
}

/// Group sources by root, run discover_scopes per root,
/// return (absolute_scope_path, count) pairs.
pub fn discover_scopes_by_root(sources: &[&Source]) -> Vec<(String, usize)> {
    // Group by root_path (BTreeMap for deterministic iteration)
    let mut by_root: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for source in sources {
        by_root
            .entry(&source.root_path)
            .or_default()
            .push(&source.rel_path);
    }

    let mut results = Vec::new();
    for (root_path, rel_paths) in &by_root {
        let refs: Vec<&str> = rel_paths.to_vec();
        let scopes = discover_scopes(&refs);
        for scope in scopes {
            let absolute = if scope.prefix.is_empty() {
                root_path.to_string()
            } else {
                format!("{}/{}", root_path, scope.prefix)
            };
            results.push((absolute, scope.count));
        }
    }
    results
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to create a Source with specific fields for testing.
    fn make_source(id: i64, root_path: &str, rel_path: &str, object_id: Option<i64>) -> Source {
        Source {
            id,
            root_id: 1,
            root_path: root_path.to_string(),
            rel_path: rel_path.to_string(),
            object_id,
            size: 0,
            mtime: 0,
            excluded: false,
            object_excluded: None,
            device: 0,
            inode: 0,
            partial_hash: String::new(),
            basis_rev: 0,
            root_role: "source".to_string(),
            root_suspended: false,
        }
    }

    // =========================================================================
    // Scope discovery tests
    // =========================================================================

    #[test]
    fn scope_single_directory_drills_through_chain() {
        // Single directory — drills down through single-child chain
        let paths = vec!["a/b/c/x.jpg", "a/b/c/y.jpg"];
        let result = discover_scopes(&paths);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].prefix, "a/b/c");
        assert_eq!(result[0].count, 2);
    }

    #[test]
    fn scope_multiple_siblings_split() {
        // Multiple siblings — splits at branching point
        let paths = vec!["photos/a.jpg", "videos/b.jpg"];
        let result = discover_scopes(&paths);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].prefix, "photos");
        assert_eq!(result[0].count, 1);
        assert_eq!(result[1].prefix, "videos");
        assert_eq!(result[1].count, 1);
    }

    #[test]
    fn scope_files_at_root() {
        // Files at root — empty prefix scope
        let paths = vec!["a.jpg", "b.jpg"];
        let result = discover_scopes(&paths);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].prefix, "");
        assert_eq!(result[0].count, 2);
    }

    #[test]
    fn scope_mixed_depth_direct_prevents_drilldown() {
        // Mixed depth — direct files prevent drill-down
        let paths = vec!["photos/a.jpg", "photos/2016/b.jpg"];
        let result = discover_scopes(&paths);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].prefix, "photos");
        assert_eq!(result[0].count, 2);
    }

    #[test]
    fn scope_deep_single_path_collapses() {
        // Deep single path — collapses long chain
        let paths = vec!["a/b/c/d/e/f.jpg"];
        let result = discover_scopes(&paths);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].prefix, "a/b/c/d/e");
        assert_eq!(result[0].count, 1);
    }

    #[test]
    fn scope_separate_branches_at_root() {
        // Separate branches at root — root splits
        let paths = vec!["a/x.jpg", "b/y.jpg", "c/z.jpg"];
        let result = discover_scopes(&paths);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].prefix, "a");
        assert_eq!(result[1].prefix, "b");
        assert_eq!(result[2].prefix, "c");
    }

    #[test]
    fn scope_single_file() {
        // Single file — degenerate minimal input
        let paths = vec!["only.jpg"];
        let result = discover_scopes(&paths);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].prefix, "");
        assert_eq!(result[0].count, 1);
    }

    #[test]
    fn scope_deep_directory_with_sibling() {
        // Deep directory with sibling at higher level — independent collapsing
        let paths = vec!["a/b/c/x.jpg", "d/y.jpg"];
        let result = discover_scopes(&paths);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].prefix, "a/b/c");
        assert_eq!(result[0].count, 1);
        assert_eq!(result[1].prefix, "d");
        assert_eq!(result[1].count, 1);
    }

    #[test]
    fn scope_uniform_distribution() {
        // Uniform distribution — no false collapsing
        let paths = vec!["a/x.jpg", "a/y.jpg", "b/x.jpg", "b/y.jpg"];
        let result = discover_scopes(&paths);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].prefix, "a");
        assert_eq!(result[0].count, 2);
        assert_eq!(result[1].prefix, "b");
        assert_eq!(result[1].count, 2);
    }

    #[test]
    fn scope_empty_input() {
        // Empty input — no panic
        let paths: Vec<&str> = vec![];
        let result = discover_scopes(&paths);
        assert!(result.is_empty());
    }

    // =========================================================================
    // "Only here" tests
    // =========================================================================

    #[test]
    fn only_here_content_only_at_location() {
        // CRITICAL CORRECTNESS TEST: internal duplicates must NOT disqualify
        // Object 42 has sources at ["/loc/a.jpg", "/loc/b.jpg"], both inside /loc → count 1
        let s1 = make_source(1, "/loc", "a.jpg", Some(42));
        let s2 = make_source(2, "/loc", "b.jpg", Some(42));
        let by_object_id: HashMap<i64, Vec<&Source>> = HashMap::from([(42, vec![&s1, &s2])]);
        let object_ids: HashSet<i64> = HashSet::from([42]);
        assert_eq!(count_only_here(&object_ids, "/loc", &by_object_id), 1);
    }

    #[test]
    fn only_here_content_exists_elsewhere() {
        // Object 42 has sources at ["/loc/a.jpg", "/other/b.jpg"] → count 0
        let s1 = make_source(1, "/loc", "a.jpg", Some(42));
        let s2 = make_source(2, "/other", "b.jpg", Some(42));
        let by_object_id: HashMap<i64, Vec<&Source>> = HashMap::from([(42, vec![&s1, &s2])]);
        let object_ids: HashSet<i64> = HashSet::from([42]);
        assert_eq!(count_only_here(&object_ids, "/loc", &by_object_id), 0);
    }

    #[test]
    fn only_here_internal_duplicates_dont_inflate() {
        // CRITICAL CORRECTNESS TEST: counts object_ids, not sources
        // Object 42 has 3 sources all at /loc/ → count 1, not 3
        let s1 = make_source(1, "/loc", "a.jpg", Some(42));
        let s2 = make_source(2, "/loc", "b.jpg", Some(42));
        let s3 = make_source(3, "/loc", "c.jpg", Some(42));
        let by_object_id: HashMap<i64, Vec<&Source>> =
            HashMap::from([(42, vec![&s1, &s2, &s3])]);
        let object_ids: HashSet<i64> = HashSet::from([42]);
        assert_eq!(count_only_here(&object_ids, "/loc", &by_object_id), 1);
    }

    #[test]
    fn only_here_mixed() {
        // Objects 42, 43, 44: two "only here", one not → count 2
        let s1 = make_source(1, "/loc", "a.jpg", Some(42));
        let s2 = make_source(2, "/loc", "b.jpg", Some(43));
        let s3 = make_source(3, "/loc", "c.jpg", Some(44));
        let s4 = make_source(4, "/other", "d.jpg", Some(44));
        let by_object_id: HashMap<i64, Vec<&Source>> = HashMap::from([
            (42, vec![&s1]),
            (43, vec![&s2]),
            (44, vec![&s3, &s4]),
        ]);
        let object_ids: HashSet<i64> = HashSet::from([42, 43, 44]);
        assert_eq!(count_only_here(&object_ids, "/loc", &by_object_id), 2);
    }

    #[test]
    fn only_here_path_boundary() {
        // Object 42 at ["/photos/a.jpg", "/photography/b.jpg"], location /photos → count 0
        // /photography is not under /photos
        let s1 = make_source(1, "/photos", "a.jpg", Some(42));
        let s2 = make_source(2, "/photography", "b.jpg", Some(42));
        let by_object_id: HashMap<i64, Vec<&Source>> = HashMap::from([(42, vec![&s1, &s2])]);
        let object_ids: HashSet<i64> = HashSet::from([42]);
        assert_eq!(count_only_here(&object_ids, "/photos", &by_object_id), 0);
    }

    // =========================================================================
    // "Unique to selection" tests
    // =========================================================================

    #[test]
    fn unique_truly_unique() {
        // Object in selection, no copies outside → count 1
        let s1 = make_source(1, "/sel", "a.jpg", Some(42));
        let by_object_id: HashMap<i64, Vec<&Source>> = HashMap::from([(42, vec![&s1])]);
        let selection_object_ids: HashSet<i64> = HashSet::from([42]);
        let selection_source_ids: HashSet<i64> = HashSet::from([1]);
        assert_eq!(
            count_unique_to_selection(&selection_object_ids, &selection_source_ids, &by_object_id),
            1
        );
    }

    #[test]
    fn unique_copy_outside_selection() {
        // Object in selection + source outside selection → count 0
        let s1 = make_source(1, "/sel", "a.jpg", Some(42));
        let s2 = make_source(2, "/other", "b.jpg", Some(42));
        let by_object_id: HashMap<i64, Vec<&Source>> = HashMap::from([(42, vec![&s1, &s2])]);
        let selection_object_ids: HashSet<i64> = HashSet::from([42]);
        let selection_source_ids: HashSet<i64> = HashSet::from([1]);
        assert_eq!(
            count_unique_to_selection(&selection_object_ids, &selection_source_ids, &by_object_id),
            0
        );
    }

    #[test]
    fn unique_archived_copy_counts_as_elsewhere() {
        // Object in selection + archive source not in selection → count 0
        let s1 = make_source(1, "/sel", "a.jpg", Some(42));
        let mut s2 = make_source(2, "/archive", "a.jpg", Some(42));
        s2.root_role = "archive".to_string();
        let by_object_id: HashMap<i64, Vec<&Source>> = HashMap::from([(42, vec![&s1, &s2])]);
        let selection_object_ids: HashSet<i64> = HashSet::from([42]);
        let selection_source_ids: HashSet<i64> = HashSet::from([1]);
        assert_eq!(
            count_unique_to_selection(&selection_object_ids, &selection_source_ids, &by_object_id),
            0
        );
    }

    #[test]
    fn unique_duplicate_within_selection() {
        // Two sources for same object, both in selection → count 1
        let s1 = make_source(1, "/sel", "a.jpg", Some(42));
        let s2 = make_source(2, "/sel", "b.jpg", Some(42));
        let by_object_id: HashMap<i64, Vec<&Source>> = HashMap::from([(42, vec![&s1, &s2])]);
        let selection_object_ids: HashSet<i64> = HashSet::from([42]);
        let selection_source_ids: HashSet<i64> = HashSet::from([1, 2]);
        assert_eq!(
            count_unique_to_selection(&selection_object_ids, &selection_source_ids, &by_object_id),
            1
        );
    }

    // =========================================================================
    // Classification tests
    // =========================================================================

    #[test]
    fn classify_superset() {
        // shared=320, total=400, complementary=95, threshold=0.8 → Superset (0.80 >= 0.80)
        assert_eq!(
            classify_location(320, 400, 95, 0.8),
            LocationKind::Superset
        );
    }

    #[test]
    fn classify_lead() {
        // shared=45, total=400, complementary=180, threshold=0.8 → Lead (0.1125 < 0.80)
        assert_eq!(classify_location(45, 400, 180, 0.8), LocationKind::Lead);
    }

    #[test]
    fn classify_mirror() {
        // shared=30, total=400, complementary=0, threshold=0.8 → Mirror
        assert_eq!(classify_location(30, 400, 0, 0.8), LocationKind::Mirror);
    }

    #[test]
    fn classify_edge_of_threshold() {
        // shared=319, total=400, complementary=10, threshold=0.8 → Lead (0.7975 < 0.80)
        assert_eq!(classify_location(319, 400, 10, 0.8), LocationKind::Lead);
    }

    // =========================================================================
    // discover_scopes_by_root wrapper test
    // =========================================================================

    #[test]
    fn discover_scopes_by_root_integration() {
        // Sources across two roots, verify absolute paths with correct counts
        let s1 = make_source(1, "/mnt/disk1", "photos/a.jpg", Some(42));
        let s2 = make_source(2, "/mnt/disk1", "photos/b.jpg", Some(43));
        let s3 = make_source(3, "/mnt/disk2", "backup/2024/c.jpg", Some(44));
        let sources: Vec<&Source> = vec![&s1, &s2, &s3];

        let result = discover_scopes_by_root(&sources);
        assert_eq!(result.len(), 2);
        // BTreeMap orders by root_path: /mnt/disk1 before /mnt/disk2
        assert_eq!(result[0].0, "/mnt/disk1/photos");
        assert_eq!(result[0].1, 2);
        assert_eq!(result[1].0, "/mnt/disk2/backup/2024");
        assert_eq!(result[1].1, 1);
    }

    // =========================================================================
    // find_unique_object_ids tests
    // =========================================================================

    #[test]
    fn find_unique_returns_correct_set() {
        // Object 42: only in selection → unique
        // Object 43: copy outside selection → not unique
        let s1 = make_source(1, "/sel", "a.jpg", Some(42));
        let s2 = make_source(2, "/sel", "b.jpg", Some(43));
        let s3 = make_source(3, "/other", "c.jpg", Some(43));
        let by_object_id: HashMap<i64, Vec<&Source>> =
            HashMap::from([(42, vec![&s1]), (43, vec![&s2, &s3])]);
        let sel_oids: HashSet<i64> = HashSet::from([42, 43]);
        let sel_sids: HashSet<i64> = HashSet::from([1, 2]);

        let result = find_unique_object_ids(&sel_oids, &sel_sids, &by_object_id);
        assert_eq!(result, HashSet::from([42]));
    }

    #[test]
    fn find_unique_delegates_correctly() {
        // Same inputs → count_unique_to_selection == find_unique_object_ids.len()
        let s1 = make_source(1, "/sel", "a.jpg", Some(42));
        let s2 = make_source(2, "/sel", "b.jpg", Some(43));
        let s3 = make_source(3, "/other", "c.jpg", Some(43));
        let by_object_id: HashMap<i64, Vec<&Source>> =
            HashMap::from([(42, vec![&s1]), (43, vec![&s2, &s3])]);
        let sel_oids: HashSet<i64> = HashSet::from([42, 43]);
        let sel_sids: HashSet<i64> = HashSet::from([1, 2]);

        let count = count_unique_to_selection(&sel_oids, &sel_sids, &by_object_id);
        let set = find_unique_object_ids(&sel_oids, &sel_sids, &by_object_id);
        assert_eq!(count, set.len());
    }
}
