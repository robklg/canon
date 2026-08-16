//! Exclusion domain logic for canon.
//!
//! Pure functions for determining which sources should be excluded based on
//! duplicate detection. No I/O or side effects.
//!
//! ## Design Principles
//!
//! 1. **Pure functions**: All logic is deterministic with no I/O
//! 2. **Uses Source::path()**: Path comparison uses the domain's authoritative path computation
//! 3. **Testable in isolation**: Can be unit tested without database
//!
//! ## Usage
//!
//! ```ignore
//! use crate::exclude::domain::find_excludable_duplicates;
//!
//! let result = find_excludable_duplicates(&scope_sources, &sources_by_object, "/archive");
//! for source_id in result.to_exclude {
//!     crate::exclude::repo::source::set_excluded(conn, source_id, true, decision_id)?;
//! }
//! ```

use std::collections::HashMap;

use crate::domain::path::path_is_under;
use crate::domain::source::Source;

/// Result of analyzing sources for excludable duplicates.
///
/// Contains both the list of sources to exclude and statistics about
/// sources that were skipped for various reasons.
#[derive(Debug, Default)]
pub struct ExcludableDuplicatesResult {
    /// Source IDs that should be excluded (have exactly one non-excluded copy in prefer path)
    pub to_exclude: Vec<i64>,
    /// Count of sources skipped because they have no object_id (unhashed)
    pub skipped_no_hash: usize,
    /// Count of sources skipped because they're already in the prefer path
    pub skipped_in_prefer: usize,
    /// Count of sources skipped because no copy exists in prefer path
    pub skipped_not_covered: usize,
    /// Count of sources skipped because multiple copies exist in prefer path (ambiguous)
    pub skipped_multiple: usize,
}

/// Determine which sources should be excluded based on duplicate detection.
///
/// For each source in `scope_sources`, checks if there's exactly one non-excluded
/// duplicate in the prefer path. If so, the source is marked for exclusion.
///
/// # Arguments
///
/// * `scope_sources` - Sources to consider for exclusion (candidates)
/// * `all_sources_by_object` - All sources grouped by object_id (for finding duplicates)
/// * `prefer_prefix` - Path prefix for the preferred location (e.g., "/archive")
///
/// # Returns
///
/// Result containing source IDs to exclude and skip statistics.
///
/// # Algorithm
///
/// For each source in scope:
/// 1. Skip if no object_id (unhashed file)
/// 2. Skip if source is already under prefer_prefix
/// 3. Find all duplicates (sources with same object_id) that are:
///    - Under prefer_prefix
///    - Not the source itself
///    - Not already excluded
/// 4. If exactly 1 such duplicate exists → mark source for exclusion
///    If 0 duplicates → skip (not covered)
///    If 2+ duplicates → skip (ambiguous)
pub fn find_excludable_duplicates(
    scope_sources: &[Source],
    all_sources_by_object: &HashMap<i64, Vec<Source>>,
    prefer_prefix: &str,
) -> ExcludableDuplicatesResult {
    let mut result = ExcludableDuplicatesResult::default();

    for source in scope_sources {
        // Skip sources without object_id (unhashed)
        let Some(object_id) = source.object_id else {
            result.skipped_no_hash += 1;
            continue;
        };

        let source_path = source.path();

        // Skip if source is already in the prefer path
        if path_is_under(&source_path, prefer_prefix) {
            result.skipped_in_prefer += 1;
            continue;
        }

        // Find duplicates in prefer path
        let prefer_copies = all_sources_by_object
            .get(&object_id)
            .map(|sources| {
                sources
                    .iter()
                    .filter(|s| s.id != source.id) // Not self
                    .filter(|s| !s.is_excluded()) // Not already excluded
                    .filter(|s| path_is_under(&s.path(), prefer_prefix)) // In prefer path
                    .count()
            })
            .unwrap_or(0);

        match prefer_copies {
            0 => {
                // No copy in prefer path
                result.skipped_not_covered += 1;
            }
            1 => {
                // Exactly one copy in prefer path - exclude this source
                result.to_exclude.push(source.id);
            }
            _ => {
                // Multiple copies in prefer path - ambiguous
                result.skipped_multiple += 1;
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Create a test Source with minimal required fields
    fn make_source(
        id: i64,
        root_path: &str,
        rel_path: &str,
        object_id: Option<i64>,
        excluded: bool,
        object_excluded: bool,
    ) -> Source {
        Source {
            id,
            root_id: 1,
            root_path: root_path.to_string(),
            rel_path: rel_path.to_string(),
            object_id,
            size: 1000,
            mtime: 1704067200,
            excluded,
            object_excluded: Some(object_excluded),
            device: 0,
            inode: 0,
            partial_hash: "hash".to_string(),
            basis_rev: 1,
            root_role: "source".to_string(),
            root_suspended: false,
            decision_id: None,
        }
    }

    #[test]
    fn find_excludable_empty_scope() {
        let result = find_excludable_duplicates(&[], &HashMap::new(), "/archive");
        assert!(result.to_exclude.is_empty());
        assert_eq!(result.skipped_no_hash, 0);
        assert_eq!(result.skipped_in_prefer, 0);
        assert_eq!(result.skipped_not_covered, 0);
        assert_eq!(result.skipped_multiple, 0);
    }

    #[test]
    fn find_excludable_skips_unhashed() {
        // Source with no object_id
        let scope = vec![make_source(1, "/source", "file.txt", None, false, false)];

        let result = find_excludable_duplicates(&scope, &HashMap::new(), "/archive");

        assert!(result.to_exclude.is_empty());
        assert_eq!(result.skipped_no_hash, 1);
    }

    #[test]
    fn find_excludable_skips_already_in_prefer() {
        // Source is already under prefer path
        let scope = vec![make_source(
            1,
            "/archive",
            "file.txt",
            Some(100),
            false,
            false,
        )];

        let result = find_excludable_duplicates(&scope, &HashMap::new(), "/archive");

        assert!(result.to_exclude.is_empty());
        assert_eq!(result.skipped_in_prefer, 1);
    }

    #[test]
    fn find_excludable_one_copy_excludes() {
        // Source in /source, one copy in /archive
        let source = make_source(1, "/source", "photo.jpg", Some(100), false, false);
        let archive_copy = make_source(2, "/archive", "photo.jpg", Some(100), false, false);

        let scope = vec![source];
        let mut by_object = HashMap::new();
        by_object.insert(
            100,
            vec![
                make_source(1, "/source", "photo.jpg", Some(100), false, false),
                archive_copy,
            ],
        );

        let result = find_excludable_duplicates(&scope, &by_object, "/archive");

        assert_eq!(result.to_exclude, vec![1]);
        assert_eq!(result.skipped_not_covered, 0);
        assert_eq!(result.skipped_multiple, 0);
    }

    #[test]
    fn find_excludable_no_copy_skips() {
        // Source in /source, no copy in /archive
        let source = make_source(1, "/source", "unique.jpg", Some(100), false, false);

        let scope = vec![source.clone()];
        let mut by_object = HashMap::new();
        by_object.insert(100, vec![source]); // Only the source itself

        let result = find_excludable_duplicates(&scope, &by_object, "/archive");

        assert!(result.to_exclude.is_empty());
        assert_eq!(result.skipped_not_covered, 1);
    }

    #[test]
    fn find_excludable_multiple_copies_skips() {
        // Source in /source, TWO copies in /archive (ambiguous)
        let source = make_source(1, "/source", "photo.jpg", Some(100), false, false);
        let archive_copy1 = make_source(2, "/archive", "copy1.jpg", Some(100), false, false);
        let archive_copy2 = make_source(3, "/archive", "copy2.jpg", Some(100), false, false);

        let scope = vec![source];
        let mut by_object = HashMap::new();
        by_object.insert(
            100,
            vec![
                make_source(1, "/source", "photo.jpg", Some(100), false, false),
                archive_copy1,
                archive_copy2,
            ],
        );

        let result = find_excludable_duplicates(&scope, &by_object, "/archive");

        assert!(result.to_exclude.is_empty());
        assert_eq!(result.skipped_multiple, 1);
    }

    #[test]
    fn find_excludable_ignores_excluded_copies() {
        // Source in /source, two copies in /archive but one is excluded
        let source = make_source(1, "/source", "photo.jpg", Some(100), false, false);
        let archive_copy = make_source(2, "/archive", "photo.jpg", Some(100), false, false);
        let excluded_copy = make_source(3, "/archive", "excluded.jpg", Some(100), true, false); // source-level excluded

        let scope = vec![source];
        let mut by_object = HashMap::new();
        by_object.insert(
            100,
            vec![
                make_source(1, "/source", "photo.jpg", Some(100), false, false),
                archive_copy,
                excluded_copy,
            ],
        );

        let result = find_excludable_duplicates(&scope, &by_object, "/archive");

        // Should exclude because only ONE non-excluded copy in prefer
        assert_eq!(result.to_exclude, vec![1]);
    }

    #[test]
    fn find_excludable_ignores_object_excluded_copies() {
        // Source in /source, copy in /archive but object is excluded
        let source = make_source(1, "/source", "photo.jpg", Some(100), false, false);
        let object_excluded_copy = make_source(2, "/archive", "photo.jpg", Some(100), false, true); // object-level excluded

        let scope = vec![source];
        let mut by_object = HashMap::new();
        by_object.insert(
            100,
            vec![
                make_source(1, "/source", "photo.jpg", Some(100), false, false),
                object_excluded_copy,
            ],
        );

        let result = find_excludable_duplicates(&scope, &by_object, "/archive");

        // Should skip - the only copy in prefer is excluded at object level
        assert!(result.to_exclude.is_empty());
        assert_eq!(result.skipped_not_covered, 1);
    }

    #[test]
    fn find_excludable_uses_source_path() {
        // Test that path comparison uses Source::path() correctly
        // Source with empty rel_path (root IS the file)
        let source = make_source(1, "/source", "photo.jpg", Some(100), false, false);
        let archive_file = make_source(2, "/archive/photo.jpg", "", Some(100), false, false);

        let scope = vec![source];
        let mut by_object = HashMap::new();
        by_object.insert(
            100,
            vec![
                make_source(1, "/source", "photo.jpg", Some(100), false, false),
                archive_file,
            ],
        );

        // prefer path is exactly the archive file
        let result = find_excludable_duplicates(&scope, &by_object, "/archive/photo.jpg");

        // Should exclude - archive copy is at exactly the prefer path
        assert_eq!(result.to_exclude, vec![1]);
    }

    #[test]
    fn find_excludable_empty_rel_path_in_scope() {
        // Source with empty rel_path that IS the prefer path
        let source = make_source(1, "/archive/photo.jpg", "", Some(100), false, false);

        let scope = vec![source];

        let result = find_excludable_duplicates(&scope, &HashMap::new(), "/archive/photo.jpg");

        // Should skip - source is already in prefer path
        assert!(result.to_exclude.is_empty());
        assert_eq!(result.skipped_in_prefer, 1);
    }

    #[test]
    fn find_excludable_mixed_scenarios() {
        // Complex scenario with multiple sources and various outcomes
        let src_unhashed = make_source(1, "/source", "unhashed.txt", None, false, false);
        let src_in_prefer = make_source(2, "/archive", "already.txt", Some(200), false, false);
        let src_no_backup = make_source(3, "/source", "no_backup.txt", Some(300), false, false);
        let src_to_exclude = make_source(4, "/source", "has_backup.txt", Some(400), false, false);
        let src_ambiguous = make_source(5, "/source", "ambiguous.txt", Some(500), false, false);

        let scope = vec![
            src_unhashed,
            src_in_prefer,
            src_no_backup.clone(),
            src_to_exclude.clone(),
            src_ambiguous.clone(),
        ];

        let mut by_object = HashMap::new();
        // No backup for 300
        by_object.insert(300, vec![src_no_backup]);
        // One backup for 400
        by_object.insert(
            400,
            vec![
                src_to_exclude,
                make_source(10, "/archive", "backup.txt", Some(400), false, false),
            ],
        );
        // Two backups for 500 (ambiguous)
        by_object.insert(
            500,
            vec![
                src_ambiguous,
                make_source(11, "/archive", "backup1.txt", Some(500), false, false),
                make_source(12, "/archive", "backup2.txt", Some(500), false, false),
            ],
        );

        let result = find_excludable_duplicates(&scope, &by_object, "/archive");

        assert_eq!(result.to_exclude, vec![4]); // Only src_to_exclude
        assert_eq!(result.skipped_no_hash, 1); // src_unhashed
        assert_eq!(result.skipped_in_prefer, 1); // src_in_prefer
        assert_eq!(result.skipped_not_covered, 1); // src_no_backup
        assert_eq!(result.skipped_multiple, 1); // src_ambiguous
    }
}
