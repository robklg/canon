//! The in-memory object index — content identity as a queryable structure.
//!
//! Maps each object id to every source holding that content. Overlap,
//! uniqueness, and archive-status reasoning all start here: objects, not
//! sources, ground content comparisons (duplicates don't make content more
//! irreplaceable).
//!
//! The index applies no inclusion *policy* of its own — callers filter
//! sources (visibility, exclusion, role) before building, so different
//! consumers can reason over different populations while sharing one
//! structure. It does enforce one *law*: contentless sources are refused at
//! build (the contentless law). An empty file's identity is the
//! one universal empty object — letting it in would manufacture overlap,
//! coverage, and uniqueness claims about no-content. The law lives here,
//! at the chokepoint, so "an empty file creates overlap" is structurally
//! unreachable for every index consumer; policy stays above, in ops.

use std::collections::{HashMap, HashSet};

use crate::domain::source::Source;

/// Index of content identity: object id → every source holding that content.
pub struct ObjectIndex<'a> {
    by_object: HashMap<i64, Vec<&'a Source>>,
}

/// Archive-side presence for a queried set of objects.
pub struct ArchivePresence<'a> {
    /// Object ids (from the queried set) that exist on an archive root.
    pub archived_object_ids: HashSet<i64>,
    /// Every archive-side source holding one of the queried objects.
    pub archive_sources: Vec<&'a Source>,
}

impl<'a> ObjectIndex<'a> {
    /// Build from sources. Unhashed sources (no object id) cannot be indexed
    /// and are skipped; contentless sources are refused (the contentless
    /// law — see the module doc); any other inclusion policy is the
    /// caller's.
    pub fn build(sources: impl IntoIterator<Item = &'a Source>) -> Self {
        let mut by_object: HashMap<i64, Vec<&'a Source>> = HashMap::new();
        for s in sources {
            if s.is_contentless() {
                continue;
            }
            if let Some(oid) = s.object_id {
                by_object.entry(oid).or_default().push(s);
            }
        }
        Self { by_object }
    }

    /// Every indexed source holding this object; empty when the object is
    /// unknown to the index.
    pub fn locations_of(&self, object_id: i64) -> &[&'a Source] {
        self.by_object
            .get(&object_id)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    /// Which of the queried objects exist on an archive root, and via which
    /// archive sources. `only_root` restricts to a single archive root.
    pub fn archive_presence(
        &self,
        object_ids: &HashSet<i64>,
        only_root: Option<i64>,
    ) -> ArchivePresence<'a> {
        let mut archived_object_ids: HashSet<i64> = HashSet::new();
        let mut archive_sources: Vec<&'a Source> = Vec::new();
        for &oid in object_ids {
            for sib in self.locations_of(oid) {
                if sib.is_from_role("archive") {
                    if let Some(target_id) = only_root {
                        if sib.root_id != target_id {
                            continue;
                        }
                    }
                    archived_object_ids.insert(oid);
                    archive_sources.push(sib);
                }
            }
        }
        ArchivePresence {
            archived_object_ids,
            archive_sources,
        }
    }

    /// The underlying map, for aggregation helpers that traverse all entries.
    pub fn as_map(&self) -> &HashMap<i64, Vec<&'a Source>> {
        &self.by_object
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_source(
        id: i64,
        root_id: i64,
        root_role: &str,
        object_id: Option<i64>,
        excluded: bool,
    ) -> Source {
        Source {
            id,
            root_id,
            root_path: format!("/root{root_id}"),
            rel_path: format!("file{id}.dat"),
            object_id,
            size: 100,
            mtime: 0,
            excluded,
            object_excluded: None,
            device: 0,
            inode: 0,
            partial_hash: String::new(),
            basis_rev: 0,
            root_role: root_role.to_string(),
            root_suspended: false,
            decision_id: None,
        }
    }

    #[test]
    fn build_groups_by_object_id() {
        let sources = vec![
            make_source(1, 1, "source", Some(10), false),
            make_source(2, 2, "source", Some(10), false),
            make_source(3, 1, "source", Some(20), false),
        ];
        let index = ObjectIndex::build(&sources);
        assert_eq!(index.locations_of(10).len(), 2);
        assert_eq!(index.locations_of(20).len(), 1);
    }

    #[test]
    fn build_skips_unhashed_sources() {
        let sources = vec![
            make_source(1, 1, "source", None, false),
            make_source(2, 1, "source", Some(10), false),
        ];
        let index = ObjectIndex::build(&sources);
        assert_eq!(index.as_map().len(), 1);
        assert_eq!(index.locations_of(10).len(), 1);
    }

    #[test]
    fn build_applies_the_law_but_no_policy() {
        // Two contracts in one: an excluded source fed to build IS indexed
        // (policy is the caller's job), while a contentless source is
        // refused (the law is the index's own — empty content can never
        // create overlap, coverage, or uniqueness).
        let excluded = make_source(1, 1, "source", Some(10), true);
        let mut empty = make_source(2, 1, "source", Some(11), false);
        empty.size = 0;
        let sources = vec![excluded, empty];
        let index = ObjectIndex::build(&sources);
        assert_eq!(index.locations_of(10).len(), 1, "policy stays out");
        assert!(
            index.locations_of(11).is_empty(),
            "the contentless law is enforced at build"
        );
    }

    #[test]
    fn locations_of_unknown_object_is_empty() {
        let sources = vec![make_source(1, 1, "source", Some(10), false)];
        let index = ObjectIndex::build(&sources);
        assert!(index.locations_of(99).is_empty());
    }

    #[test]
    fn empty_build_is_empty() {
        let sources: Vec<Source> = vec![];
        let index = ObjectIndex::build(&sources);
        assert!(index.as_map().is_empty());
        assert!(index.locations_of(1).is_empty());
    }

    #[test]
    fn archive_presence_partitions_by_role() {
        let sources = vec![
            make_source(1, 1, "source", Some(10), false),
            make_source(2, 2, "archive", Some(10), false),
            make_source(3, 2, "archive", Some(10), false),
            make_source(4, 1, "source", Some(20), false),
        ];
        let index = ObjectIndex::build(&sources);
        let ids: HashSet<i64> = [10, 20].into_iter().collect();
        let presence = index.archive_presence(&ids, None);
        assert_eq!(
            presence.archived_object_ids,
            [10].into_iter().collect::<HashSet<i64>>()
        );
        assert_eq!(presence.archive_sources.len(), 2);
    }

    #[test]
    fn archive_presence_considers_only_queried_objects() {
        let sources = vec![
            make_source(1, 2, "archive", Some(10), false),
            make_source(2, 2, "archive", Some(20), false),
        ];
        let index = ObjectIndex::build(&sources);
        let ids: HashSet<i64> = [10].into_iter().collect();
        let presence = index.archive_presence(&ids, None);
        assert_eq!(
            presence.archived_object_ids,
            [10].into_iter().collect::<HashSet<i64>>()
        );
        assert_eq!(presence.archive_sources.len(), 1);
    }

    #[test]
    fn archive_presence_restricts_to_one_root() {
        let sources = vec![
            make_source(1, 2, "archive", Some(10), false),
            make_source(2, 3, "archive", Some(10), false),
            make_source(3, 3, "archive", Some(20), false),
        ];
        let index = ObjectIndex::build(&sources);
        let ids: HashSet<i64> = [10, 20].into_iter().collect();
        let presence = index.archive_presence(&ids, Some(3));
        assert_eq!(
            presence.archived_object_ids,
            [10, 20].into_iter().collect::<HashSet<i64>>()
        );
        assert_eq!(presence.archive_sources.len(), 2);
        assert!(presence.archive_sources.iter().all(|s| s.root_id == 3));
    }
}
