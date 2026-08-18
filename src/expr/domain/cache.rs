//! The prefetched fact cache and the value shape the filter language compares.
//!
//! Evaluating a filter over many sources one fact at a time is a query per
//! source per key, so values are fetched in bulk first and read from here.
//! The cache holds source facts, object facts, the source-to-object map, and
//! the sets that answer the status predicates; the converters translate
//! between the database's column triple, the fact noun's value, and the
//! narrower value the comparisons work on.

use std::collections::{HashMap, HashSet};

use crate::core::domain::fact;

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

    pub fn get_object_id(&self, source_id: i64) -> Option<i64> {
        self.source_objects.get(&source_id).copied()
    }

    pub fn has_key(&self, key: &str) -> bool {
        self.prefetched_keys.contains(key)
    }
}

/// Convert DB values to FactValue
pub fn to_fact_value(
    text: Option<String>,
    num: Option<f64>,
    time: Option<i64>,
) -> Option<fact::FactValue> {
    if let Some(t) = text {
        Some(fact::FactValue::Text(t))
    } else if let Some(n) = num {
        Some(fact::FactValue::Num(n))
    } else {
        time.map(fact::FactValue::Time)
    }
}

/// Convert fact::FactValue to local FactValue
pub fn to_local_fact_value(fv: &fact::FactValue) -> FactValue {
    match fv {
        fact::FactValue::Text(t) => FactValue::Text(t.clone()),
        fact::FactValue::Num(n) => FactValue::Num(*n),
        fact::FactValue::Time(ts) => FactValue::Time(*ts),
        fact::FactValue::Path(p) => FactValue::Text(p.clone()),
    }
}

// ============================================================================
// Value Handling
// ============================================================================

/// Stored fact value - can be text, number, or timestamp
/// A fact value from the database
#[derive(Clone)]
pub enum FactValue {
    Text(String),
    Num(f64),
    Time(i64),
}

impl From<FactValue> for serde_json::Value {
    fn from(fv: FactValue) -> Self {
        match fv {
            FactValue::Text(s) => serde_json::Value::String(s),
            FactValue::Num(n) => serde_json::Number::from_f64(n)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            FactValue::Time(ts) => serde_json::Value::Number(ts.into()),
        }
    }
}
