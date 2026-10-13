//! The prefetched fact cache and the value shape the filter language compares.
//!
//! Evaluating a filter over many sources one fact at a time is a query per
//! source per key, so values are fetched in bulk first and read from here.
//! The cache holds source facts, object facts, the source-to-object map, and
//! the sets that answer the status predicates. A source's value for a key is
//! read through `value`, the one-value rule applied.
//! Values are the fact noun's own (`core::domain::fact::FactValue`), read
//! from storage through the one column conversion in `core::repo::fact`.

use std::collections::{HashMap, HashSet};

use crate::core::domain::fact;
use crate::expr::domain::transform::InstantRecord;
use crate::expr::domain::value::source_value;

// ============================================================================
// Fact Cache for Bulk Prefetching
// ============================================================================

/// Cache of prefetched fact values to avoid N+1 queries
pub struct FactCache {
    /// Source facts: (source_id, key) -> FactValue
    pub source_facts: HashMap<(i64, String), fact::FactValue>,
    /// Object facts: (object_id, key) -> FactValue
    pub object_facts: HashMap<(i64, String), fact::FactValue>,
    /// Source to object mapping
    pub source_objects: HashMap<i64, i64>,
    /// Keys that were prefetched (for existence checks)
    pub prefetched_keys: HashSet<String>,
    // Lazily populated status predicate data
    /// Object IDs that exist in archive roots (for `archived?`).
    pub archived_objects: Option<HashSet<i64>>,
    /// Source IDs that are excluded at source or object level (for `excluded?`).
    pub excluded_sources: Option<HashSet<i64>>,
    /// Source IDs with facts beyond content.hash.sha256 (for `enriched?`).
    pub enriched_sources: Option<HashSet<i64>>,
    /// Whether any comparison in this run read an instant on the clock.
    pub(crate) instant_read: InstantRecord,
}

impl FactCache {
    pub fn new() -> Self {
        FactCache {
            source_facts: HashMap::new(),
            object_facts: HashMap::new(),
            source_objects: HashMap::new(),
            prefetched_keys: HashSet::new(),
            archived_objects: None,
            excluded_sources: None,
            enriched_sources: None,
            instant_read: InstantRecord::default(),
        }
    }

    pub fn get_source_fact(&self, source_id: i64, key: &str) -> Option<&fact::FactValue> {
        self.source_facts.get(&(source_id, key.to_string()))
    }

    pub fn get_object_fact(&self, source_id: i64, key: &str) -> Option<&fact::FactValue> {
        self.source_objects
            .get(&source_id)
            .and_then(|obj_id| self.object_facts.get(&(*obj_id, key.to_string())))
    }

    /// The source's one value for a stored key: the content's fact first, the source's own
    /// where the content has none (`source_value`).
    pub fn value(&self, source_id: i64, key: &str) -> Option<&fact::FactValue> {
        source_value(
            self.get_source_fact(source_id, key),
            self.get_object_fact(source_id, key),
        )
    }

    pub fn get_object_id(&self, source_id: i64) -> Option<i64> {
        self.source_objects.get(&source_id).copied()
    }
}
