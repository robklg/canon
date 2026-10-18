//! Facts distribution computation.
//!
//! Computes value distributions and key enumerations for the `facts` command.
//! All functions take source_ids (already selected by the interface) and return
//! typed results for the interface to format.

use std::collections::HashMap;

use anyhow::Result;

use crate::core::domain::fact::FactEntry;
use crate::core::domain::fact::{FactValue, KeyTypes};
use crate::core::domain::time::zone::UserZone;
use crate::core::repo::{self, Connection};
use crate::expr::{
    fetch_source_values, rendered, resolve_fact_value, source_row_value, Key, KeyKind, Listing,
    Modified, ParsedFactKey, SourceAttributes, SourceFact,
};
use crate::facts::domain::names_the_root;
use crate::facts::repo as facts_repo;

// ============================================================================
// Result types
// ============================================================================

/// A single entry in a value distribution.
pub struct DistributionEntry {
    pub value: String,
    pub count: i64,
}

/// Result of computing a value distribution for a fact key.
pub struct DistributionResult {
    pub entries: Vec<DistributionEntry>,
    pub sources_with_value: i64,
    pub total_sources: usize,
    /// Total distinct values found (before truncation to limit).
    pub total_distinct_values: usize,
}

/// Information about a single fact key (for all-keys enumeration).
pub struct KeyInfo {
    pub key: String,
    pub count: i64,
    pub kind: KeyKind,
    /// The key's type, read from all its facts wherever they sit — never from the sources
    /// counted here. A source fact's or a built-in key's is its own.
    pub types: KeyTypes,
}

/// Result of enumerating all available fact keys.
pub struct AllKeysResult {
    pub keys: Vec<KeyInfo>,
    pub total_sources: usize,
}

/// A sub-group within a grouped distribution (e.g., one root's contribution).
pub struct GroupedEntry {
    pub group_values: Vec<String>,
    pub count: i64,
    pub root_id: Option<i64>,
    pub root_path: Option<String>,
}

/// A main value and its grouped breakdown.
pub struct GroupedValueResult {
    pub main_value: String,
    pub total_count: i64,
    pub sub_groups: Vec<GroupedEntry>,
}

/// Result of a grouped distribution computation.
pub struct GroupedDistributionResult {
    pub groups: Vec<GroupedValueResult>,
    pub sources_with_value: i64,
    /// Kept for consistency with DistributionResult/AllKeysResult — the interface
    /// currently passes its own total_sources to display, but this field ensures
    /// the ops result is self-contained if a future caller needs it.
    #[allow(dead_code)]
    pub total_sources: usize,
}

/// A single root's contribution to the selection.
pub struct RootDistributionEntry {
    pub root_id: i64,
    pub root_path: String,
    pub count: usize,
}

/// Result of computing root distribution for a selection.
pub struct RootDistributionResult {
    pub entries: Vec<RootDistributionEntry>,
    pub total_sources: usize,
}

/// Compute source count per root for the given sources.
/// Sources must have root_id populated (from batch_fetch_by_roots).
pub fn compute_root_distribution(
    sources: &[crate::core::domain::source::Source],
) -> RootDistributionResult {
    use std::collections::BTreeMap;
    let mut counts: BTreeMap<i64, (String, usize)> = BTreeMap::new();
    for s in sources {
        let entry = counts
            .entry(s.root_id)
            .or_insert_with(|| (s.root_path.clone(), 0));
        entry.1 += 1;
    }
    let mut entries: Vec<RootDistributionEntry> = counts
        .into_iter()
        .map(|(root_id, (root_path, count))| RootDistributionEntry {
            root_id,
            root_path,
            count,
        })
        .collect();
    // Sort by count descending
    entries.sort_by(|a, b| b.count.cmp(&a.count));
    let total_sources = sources.len();
    RootDistributionResult {
        entries,
        total_sources,
    }
}

// ============================================================================
// Helper functions
// ============================================================================

/// The key a base key names, when the source row answers it — the owner's
/// partition (`Key::is_computed`), never the listing. The content hash, which
/// the source row cannot answer, is a stored fact here, and a built-in key
/// (`stem`, `size`) is answered as a source fact is, though neither is listed.
fn computed_key(key: &str) -> Option<Key> {
    Key::read(key).filter(Key::is_computed)
}

/// Apply default grouping for computed keys without transforms.
/// Returns a human-readable bucket/group string for the value.
fn default_grouping(key: &Key, value: FactValue, zone: &UserZone) -> String {
    match key.as_source_fact() {
        Some(SourceFact::Size) => {
            if let FactValue::Num(size) = value {
                let size = size as i64;
                if size < 1024 {
                    "< 1 KB"
                } else if size < 1024 * 1024 {
                    "1 KB - 1 MB"
                } else if size < 10 * 1024 * 1024 {
                    "1 MB - 10 MB"
                } else if size < 100 * 1024 * 1024 {
                    "10 MB - 100 MB"
                } else if size < 1024 * 1024 * 1024 {
                    "100 MB - 1 GB"
                } else {
                    "> 1 GB"
                }
                .to_string()
            } else {
                rendered(None, &value, zone)
            }
        }
        Some(SourceFact::Mtime) => {
            if let FactValue::Time(tv) = value {
                format!("{:04}", tv.on_clock(zone).start().year())
            } else {
                rendered(None, &value, zone)
            }
        }
        _ => rendered(None, &value, zone),
    }
}

/// Aggregate values into sorted (value, count) entries, truncated to limit.
/// Returns (entries, total_distinct_values_before_truncation).
fn aggregate_and_sort(
    counts: HashMap<String, i64>,
    limit: usize,
) -> (Vec<DistributionEntry>, usize) {
    let mut entries: Vec<DistributionEntry> = counts
        .into_iter()
        .map(|(value, count)| DistributionEntry { value, count })
        .collect();
    entries.sort_by(|a, b| b.count.cmp(&a.count));
    let total_distinct = entries.len();
    if limit > 0 && entries.len() > limit {
        entries.truncate(limit);
    }
    (entries, total_distinct)
}

// ============================================================================
// Compute functions
// ============================================================================

/// Enumerate all fact keys with counts and coverage.
///
/// Combines the listed source facts and built-in keys (filtered by listing + show_hidden)
/// with stored fact keys from the database. A computed key counts the sources the resolver
/// has a value for — every source, but for a save time Canon cannot read — so the listing agrees
/// with the key's own `(no value)` line and with `K?`. Each key's type is the key's own,
/// from all its facts: a key holding two types lists as mixed, whatever the scope holds.
pub fn compute_all_keys(
    conn: &mut Connection,
    source_ids: &[i64],
    show_hidden: bool,
) -> Result<AllKeysResult> {
    let total_sources = source_ids.len();
    if source_ids.is_empty() {
        return Ok(AllKeysResult {
            keys: Vec::new(),
            total_sources,
        });
    }

    let mut keys: Vec<KeyInfo> = Vec::new();
    let sources: Vec<SourceAttributes> = repo::source::batch_fetch_by_ids(conn, source_ids)?
        .values()
        .map(SourceAttributes::from)
        .collect();

    // Add the listed source facts and built-in keys, counted from the resolver's answers
    for (key, known, _) in Key::all_typed() {
        match key.listing() {
            Listing::NotListed => continue,
            Listing::Hidden if !show_hidden => continue,
            Listing::Hidden | Listing::Shown => {}
        }
        let count = sources
            .iter()
            .filter(|source| source_row_value(source, &key).is_some())
            .count();
        keys.push(KeyInfo {
            key: key.name().to_string(),
            count: count as i64,
            kind: key.kind(),
            types: KeyTypes::of(known),
        });
    }

    // Add stored facts from database: the count is the scope's, the type the key's. A key
    // whose facts were all removed between the two reads has no type, and is not listed.
    let stored = facts_repo::count_fact_keys(conn, source_ids)?;
    let listed: Vec<&str> = stored.iter().map(|(key, _)| key.as_str()).collect();
    let mut types = repo::fact::key_types(conn, &listed)?;
    for (key, count) in stored {
        let Some(types) = types.remove(&key) else {
            continue;
        };
        keys.push(KeyInfo {
            key,
            count: count as i64,
            kind: KeyKind::ContentFact,
            types,
        });
    }

    Ok(AllKeysResult {
        keys,
        total_sources,
    })
}

/// Compute value distribution for a single fact key.
///
/// Internally dispatches on whether the source row answers the key
/// (`computed_key`) — the content hash, which it cannot answer, is a stored
/// key here:
/// - Computed key WITHOUT transforms → default grouping (size buckets, mtime→year, etc.)
/// - Computed key WITH transforms → resolve_fact_value()
/// - Stored key WITHOUT transforms → fetch raw values, group by display string
/// - Stored key WITH transforms → fetch raw values, apply transforms, aggregate
///
/// Results are sorted by count descending and truncated to limit (0 = no limit).
pub fn compute_distribution(
    conn: &mut Connection,
    source_ids: &[i64],
    key: &ParsedFactKey,
    limit: usize,
    zone: &UserZone,
) -> Result<DistributionResult> {
    let total_sources = source_ids.len();
    if source_ids.is_empty() {
        return Ok(DistributionResult {
            entries: Vec::new(),
            sources_with_value: 0,
            total_sources,
            total_distinct_values: 0,
        });
    }

    if let Some(computed) = computed_key(&key.base_key) {
        compute_computed_distribution(conn, source_ids, key, &computed, total_sources, limit, zone)
    } else if key.has_transforms() {
        compute_transformed_distribution(conn, source_ids, key, total_sources, limit, zone)
    } else {
        compute_stored_distribution(conn, source_ids, key, total_sources, limit, zone)
    }
}

/// The distribution of a key the source row answers.
fn compute_computed_distribution(
    conn: &mut Connection,
    source_ids: &[i64],
    key: &ParsedFactKey,
    computed: &Key,
    total_sources: usize,
    limit: usize,
    zone: &UserZone,
) -> Result<DistributionResult> {
    let sources = repo::source::batch_fetch_by_ids(conn, source_ids)?;
    let has_transforms = key.has_transforms();
    let mut counts: HashMap<String, i64> = HashMap::new();
    let mut sources_with_value: i64 = 0;

    for source in sources.values() {
        let val = if has_transforms {
            // Use resolve_fact_value for the full transform pipeline
            match resolve_fact_value(source, key, &HashMap::new(), zone)? {
                Some(v) => v,
                None => continue,
            }
        } else {
            // Use default grouping for the no-transform case
            match source_row_value(&SourceAttributes::from(source), computed) {
                Some(raw) => default_grouping(computed, raw, zone),
                None => continue,
            }
        };
        sources_with_value += 1;
        *counts.entry(val).or_insert(0) += 1;
    }

    let (entries, total_distinct_values) = aggregate_and_sort(counts, limit);
    Ok(DistributionResult {
        entries,
        sources_with_value,
        total_sources,
        total_distinct_values,
    })
}

/// Stored fact distribution without transforms — group by raw display value.
fn compute_stored_distribution(
    conn: &mut Connection,
    source_ids: &[i64],
    key: &ParsedFactKey,
    total_sources: usize,
    limit: usize,
    zone: &UserZone,
) -> Result<DistributionResult> {
    let fact_map = fetch_source_values(conn, source_ids, &key.base_key)?;

    let mut counts: HashMap<String, i64> = HashMap::new();
    let mut sources_with_value: i64 = 0;

    for entry in fact_map.values().flatten() {
        sources_with_value += 1;
        let display_val = rendered(None, &entry.value, zone);
        *counts.entry(display_val).or_insert(0) += 1;
    }

    let (entries, total_distinct_values) = aggregate_and_sort(counts, limit);
    Ok(DistributionResult {
        entries,
        sources_with_value,
        total_sources,
        total_distinct_values,
    })
}

/// Stored fact distribution with transforms — fetch raw, apply transforms, aggregate.
fn compute_transformed_distribution(
    conn: &mut Connection,
    source_ids: &[i64],
    key: &ParsedFactKey,
    total_sources: usize,
    limit: usize,
    zone: &UserZone,
) -> Result<DistributionResult> {
    let fact_map = fetch_source_values(conn, source_ids, &key.base_key)?;

    let mut counts: HashMap<String, i64> = HashMap::new();
    let mut sources_with_value: i64 = 0;

    for entry in fact_map.values().flatten() {
        // Accessor then modifiers through the facility's one chain. A modifier that gives no
        // value leaves the source among those without one. A value of another type than the
        // key was read for stops the report as that answer; any other failing step is a
        // defect, since `read_report` refuses a modifier that cannot apply, and is raised.
        match key.report_value(entry.value.clone(), zone)? {
            Modified::Value(value) => {
                sources_with_value += 1;
                let last = key.modifiers.last().map(|call| call.modifier);
                *counts.entry(rendered(last, &value, zone)).or_insert(0) += 1;
            }
            Modified::NoValue(_) => {}
        }
    }

    let (entries, total_distinct_values) = aggregate_and_sort(counts, limit);
    Ok(DistributionResult {
        entries,
        sources_with_value,
        total_sources,
        total_distinct_values,
    })
}

/// Compute grouped distribution (--by-root, --group-by).
///
/// Fetches sources + stored facts, resolves values via resolve_fact_value(),
/// aggregates into nested groups. No default grouping applied (matches existing
/// behavior of show_grouped_distribution).
pub fn compute_grouped_distribution(
    conn: &mut Connection,
    source_ids: &[i64],
    main_key: &ParsedFactKey,
    grouping_keys: &[ParsedFactKey],
    limit: usize,
    zone: &UserZone,
) -> Result<GroupedDistributionResult> {
    let total_sources = source_ids.len();
    if source_ids.is_empty() {
        return Ok(GroupedDistributionResult {
            groups: Vec::new(),
            sources_with_value: 0,
            total_sources,
        });
    }

    // Fetch all sources by ID
    let sources = repo::source::batch_fetch_by_ids(conn, source_ids)?;

    // Collect stored fact keys we need to fetch
    let mut stored_keys: Vec<&str> = Vec::new();
    if computed_key(&main_key.base_key).is_none() {
        stored_keys.push(&main_key.base_key);
    }
    for gk in grouping_keys {
        if computed_key(&gk.base_key).is_none() && !stored_keys.contains(&gk.base_key.as_str()) {
            stored_keys.push(&gk.base_key);
        }
    }

    // Fetch stored facts for all needed keys
    let all_facts: HashMap<i64, HashMap<String, FactEntry>> = if stored_keys.is_empty() {
        HashMap::new()
    } else {
        let mut merged: HashMap<i64, HashMap<String, FactEntry>> = HashMap::new();
        for key in &stored_keys {
            let key_facts = fetch_source_values(conn, source_ids, key)?;
            for (source_id, entry_opt) in key_facts {
                if let Some(entry) = entry_opt {
                    merged
                        .entry(source_id)
                        .or_default()
                        .insert(entry.key.clone(), entry);
                }
            }
        }
        merged
    };

    // Aggregate by (main_value, group_values) composite key
    #[derive(Hash, Eq, PartialEq, Clone)]
    struct CompositeKey {
        main_value: String,
        group_values: Vec<String>,
    }

    struct EntryInfo {
        count: i64,
        root_id: Option<i64>,
        root_path: Option<String>,
    }

    let mut aggregated: HashMap<CompositeKey, EntryInfo> = HashMap::new();
    let mut sources_with_value: i64 = 0;
    let empty_facts: HashMap<String, FactEntry> = HashMap::new();

    for source_id in source_ids {
        let source = match sources.get(source_id) {
            Some(s) => s,
            None => continue,
        };

        let stored_facts = all_facts.get(source_id).unwrap_or(&empty_facts);

        // Resolve main value
        let main_value = match resolve_fact_value(source, main_key, stored_facts, zone)? {
            Some(v) => {
                sources_with_value += 1;
                v
            }
            None => continue,
        };

        // Resolve grouping values, tracking root info for display
        let mut group_values: Vec<String> = Vec::new();
        let mut root_id_for_display: Option<i64> = None;
        let mut root_path_for_display: Option<String> = None;

        for gk in grouping_keys {
            let gk_value = resolve_fact_value(source, gk, stored_facts, zone)?
                .unwrap_or_else(|| "(no value)".to_string());

            // Track root info when grouping by root key
            if names_the_root(gk) {
                root_id_for_display = Some(source.root_id);
                root_path_for_display = Some(source.root_path.clone());
            }

            group_values.push(gk_value);
        }

        let composite = CompositeKey {
            main_value,
            group_values,
        };
        let entry = aggregated.entry(composite).or_insert(EntryInfo {
            count: 0,
            root_id: root_id_for_display,
            root_path: root_path_for_display,
        });
        entry.count += 1;
    }

    // Build nested result: group by main_value, then sort sub-groups
    let mut by_main_value: HashMap<String, GroupedValueResult> = HashMap::new();

    for (composite, info) in aggregated {
        let entry =
            by_main_value
                .entry(composite.main_value.clone())
                .or_insert(GroupedValueResult {
                    main_value: composite.main_value,
                    total_count: 0,
                    sub_groups: Vec::new(),
                });
        entry.total_count += info.count;
        entry.sub_groups.push(GroupedEntry {
            group_values: composite.group_values,
            count: info.count,
            root_id: info.root_id,
            root_path: info.root_path,
        });
    }

    // Sort main values by total count descending
    let mut groups: Vec<GroupedValueResult> = by_main_value.into_values().collect();
    groups.sort_by(|a, b| b.total_count.cmp(&a.total_count));

    if limit > 0 && groups.len() > limit {
        groups.truncate(limit);
    }

    // Sort sub-groups within each main value by count descending
    for group in &mut groups {
        group.sub_groups.sort_by(|a, b| b.count.cmp(&a.count));
    }

    Ok(GroupedDistributionResult {
        groups,
        sources_with_value,
        total_sources,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::core::domain::fact::FactType;
    use crate::core::testing::{insert_object, insert_root, insert_source, setup_test_db};

    /// Insert a fact stored on a source, with a time value.
    fn insert_time_fact(conn: &Connection, source_id: i64, key: &str, timestamp: i64) {
        conn.execute(
            "INSERT INTO facts (entity_type, entity_id, key, value_time, time_form, time_precision,
                                observed_at, observed_basis_rev)
             VALUES ('source', ?1, ?2, ?3, 'clock', 'second', 0, 0)",
            rusqlite::params![source_id, key, timestamp],
        )
        .unwrap();
    }

    // =========================================================================
    // compute_all_keys tests
    // =========================================================================

    #[test]
    fn test_compute_all_keys_includes_source_facts_and_stored() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let obj_id = insert_object(&conn, "abc123", false);
        let s1 = insert_source(&conn, root_id, "a.jpg", Some(obj_id));
        let s2 = insert_source(&conn, root_id, "b.jpg", Some(obj_id));

        // Add a stored fact for one source
        crate::core::testing::insert_fact(&conn, s1, "content.Make", "Canon");

        let result = compute_all_keys(&mut conn, &[s1, s2], false).unwrap();

        // Should include the source facts with count == 2 (total_sources)
        let ext_key = result.keys.iter().find(|k| k.key == "source.ext").unwrap();
        assert_eq!(ext_key.count, 2);
        assert_eq!(ext_key.kind, KeyKind::SourceFact);

        // Should include stored fact with count == 1
        let make_key = result
            .keys
            .iter()
            .find(|k| k.key == "content.Make")
            .unwrap();
        assert_eq!(make_key.count, 1);
        assert_eq!(make_key.kind, KeyKind::ContentFact);

        assert_eq!(result.total_sources, 2);
    }

    /// Store a number fact on a source.
    fn insert_num_fact(conn: &Connection, source_id: i64, key: &str, value: f64) {
        conn.execute(
            "INSERT INTO facts (entity_type, entity_id, key, value_num, observed_at, observed_basis_rev)
             VALUES ('source', ?1, ?2, ?3, 0, 0)",
            rusqlite::params![source_id, key, value],
        )
        .unwrap();
    }

    fn listed<'a>(result: &'a AllKeysResult, key: &str) -> &'a KeyInfo {
        result.keys.iter().find(|k| k.key == key).unwrap()
    }

    /// The Type column reads the key, not the scope: a key holding text on a file outside
    /// the scope and a number inside it lists as mixed, while its count stays the scope's.
    #[test]
    fn the_listing_states_the_key_type_not_the_scoped_type() {
        let mut conn = setup_test_db();
        let here = insert_root(&conn, "/photos", "source", false);
        let elsewhere = insert_root(&conn, "/other", "source", false);
        let inside = insert_source(&conn, here, "a.jpg", None);
        let outside = insert_source(&conn, elsewhere, "b.jpg", None);
        insert_num_fact(&conn, inside, "content.Width", 4000.0);
        crate::core::testing::insert_fact(&conn, outside, "content.Width", "wide");
        insert_num_fact(&conn, inside, "content.Height", 3000.0);

        let result = compute_all_keys(&mut conn, &[inside], false).unwrap();
        let width = listed(&result, "content.Width");
        assert_eq!(width.types.word(), "mixed");
        assert_eq!(width.count, 1);
        assert_eq!(listed(&result, "content.Height").types.word(), "number");
    }

    /// Each stored key lists with the type its facts hold, and a source fact or built-in key with its own.
    #[test]
    fn the_listing_types_each_key_from_key_types() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let s1 = insert_source(&conn, root, "a.jpg", None);
        crate::core::testing::insert_fact(&conn, s1, "content.Make", "Canon");
        insert_num_fact(&conn, s1, "content.Width", 4000.0);
        insert_time_fact(&conn, s1, "content.DateTimeOriginal", 1704067200);

        let result = compute_all_keys(&mut conn, &[s1], false).unwrap();
        for (key, want) in [
            ("content.Make", FactType::Text),
            ("content.Width", FactType::Num),
            ("content.DateTimeOriginal", FactType::Time),
            ("source.size", FactType::Num),
        ] {
            assert_eq!(listed(&result, key).types.single(), Some(want), "{key}");
        }
    }

    // =========================================================================
    // compute_distribution tests
    // =========================================================================

    #[test]
    fn test_compute_distribution_stored_fact() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let obj1 = insert_object(&conn, "hash1", false);
        let obj2 = insert_object(&conn, "hash2", false);
        let obj3 = insert_object(&conn, "hash3", false);
        let s1 = insert_source(&conn, root_id, "a.jpg", Some(obj1));
        let s2 = insert_source(&conn, root_id, "b.jpg", Some(obj2));
        let s3 = insert_source(&conn, root_id, "c.jpg", Some(obj3));

        crate::core::testing::insert_fact(&conn, s1, "content.Make", "Canon");
        crate::core::testing::insert_fact(&conn, s2, "content.Make", "Canon");
        crate::core::testing::insert_fact(&conn, s3, "content.Make", "Nikon");

        let key = ParsedFactKey::parse("content.Make").unwrap();
        let result =
            compute_distribution(&mut conn, &[s1, s2, s3], &key, 0, &UserZone::utc()).unwrap();

        assert_eq!(result.entries.len(), 2);
        // Sorted by count desc — Canon (2) first
        assert_eq!(result.entries[0].value, "Canon");
        assert_eq!(result.entries[0].count, 2);
        assert_eq!(result.entries[1].value, "Nikon");
        assert_eq!(result.entries[1].count, 1);
        assert_eq!(result.sources_with_value, 3);
        assert_eq!(result.total_sources, 3);
    }

    #[test]
    fn test_compute_distribution_with_limit() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let obj1 = insert_object(&conn, "h1", false);
        let obj2 = insert_object(&conn, "h2", false);
        let obj3 = insert_object(&conn, "h3", false);
        let obj4 = insert_object(&conn, "h4", false);
        let s1 = insert_source(&conn, root_id, "a.jpg", Some(obj1));
        let s2 = insert_source(&conn, root_id, "b.jpg", Some(obj2));
        let s3 = insert_source(&conn, root_id, "c.jpg", Some(obj3));
        let s4 = insert_source(&conn, root_id, "d.jpg", Some(obj4));

        crate::core::testing::insert_fact(&conn, s1, "content.Make", "Canon");
        crate::core::testing::insert_fact(&conn, s2, "content.Make", "Nikon");
        crate::core::testing::insert_fact(&conn, s3, "content.Make", "Sony");
        crate::core::testing::insert_fact(&conn, s4, "content.Make", "Canon");

        let key = ParsedFactKey::parse("content.Make").unwrap();
        let result =
            compute_distribution(&mut conn, &[s1, s2, s3, s4], &key, 2, &UserZone::utc()).unwrap();

        // Limited to top 2
        assert_eq!(result.entries.len(), 2);
        assert_eq!(result.entries[0].value, "Canon");
        assert_eq!(result.entries[0].count, 2);
    }

    #[test]
    fn test_compute_distribution_source_size_buckets() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let obj1 = insert_object(&conn, "h1", false);
        let obj2 = insert_object(&conn, "h2", false);
        let obj3 = insert_object(&conn, "h3", false);

        // Insert sources with different sizes via insert_source_with_size
        // 500 bytes (< 1 KB), 500KB (1 KB - 1 MB), 5MB (1 MB - 10 MB)
        use crate::core::testing::insert_source_with_size;
        let s1 = insert_source_with_size(&conn, root_id, "tiny.txt", Some(obj1), 500);
        let s2 = insert_source_with_size(&conn, root_id, "medium.jpg", Some(obj2), 512_000);
        let s3 = insert_source_with_size(&conn, root_id, "large.raw", Some(obj3), 5_242_880);

        let key = ParsedFactKey::parse("source.size").unwrap();
        let result =
            compute_distribution(&mut conn, &[s1, s2, s3], &key, 0, &UserZone::utc()).unwrap();

        assert_eq!(result.entries.len(), 3);
        let buckets: Vec<&str> = result.entries.iter().map(|e| e.value.as_str()).collect();
        assert!(buckets.contains(&"< 1 KB"));
        assert!(buckets.contains(&"1 KB - 1 MB"));
        assert!(buckets.contains(&"1 MB - 10 MB"));
        assert_eq!(result.sources_with_value, 3);
    }

    #[test]
    fn test_compute_distribution_source_mtime_year() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let obj1 = insert_object(&conn, "h1", false);
        let obj2 = insert_object(&conn, "h2", false);
        let obj3 = insert_object(&conn, "h3", false);

        // 2023-06-15, 2024-01-01, 2024-07-01
        use crate::core::testing::insert_source_with_metadata;
        let s1 = insert_source_with_metadata(&conn, root_id, "a.jpg", Some(obj1), 1000, 1686830400);
        let s2 = insert_source_with_metadata(&conn, root_id, "b.jpg", Some(obj2), 1000, 1704067200);
        let s3 = insert_source_with_metadata(&conn, root_id, "c.jpg", Some(obj3), 1000, 1719792000);

        let key = ParsedFactKey::parse("source.mtime").unwrap();
        let result =
            compute_distribution(&mut conn, &[s1, s2, s3], &key, 0, &UserZone::utc()).unwrap();

        // 2024 should have 2 entries, 2023 should have 1
        let y2024 = result.entries.iter().find(|e| e.value == "2024").unwrap();
        assert_eq!(y2024.count, 2);
        let y2023 = result.entries.iter().find(|e| e.value == "2023").unwrap();
        assert_eq!(y2023.count, 1);
    }

    #[test]
    fn test_compute_distribution_with_transforms() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let obj1 = insert_object(&conn, "h1", false);
        let obj2 = insert_object(&conn, "h2", false);
        let obj3 = insert_object(&conn, "h3", false);
        let s1 = insert_source(&conn, root_id, "a.jpg", Some(obj1));
        let s2 = insert_source(&conn, root_id, "b.jpg", Some(obj2));
        let s3 = insert_source(&conn, root_id, "c.jpg", Some(obj3));

        // Insert time facts for different years
        insert_time_fact(&conn, s1, "content.DateTimeOriginal", 1686830400);
        insert_time_fact(&conn, s2, "content.DateTimeOriginal", 1704067200);
        insert_time_fact(&conn, s3, "content.DateTimeOriginal", 1719792000);

        let key = ParsedFactKey::parse("content.DateTimeOriginal|year").unwrap();
        let result =
            compute_distribution(&mut conn, &[s1, s2, s3], &key, 0, &UserZone::utc()).unwrap();

        let y2024 = result.entries.iter().find(|e| e.value == "2024").unwrap();
        assert_eq!(y2024.count, 2);
        let y2023 = result.entries.iter().find(|e| e.value == "2023").unwrap();
        assert_eq!(y2023.count, 1);
    }

    /// `facts --key source.mtime` groups a save time by its year on the user's clock:
    /// 23:30 UTC on New Year's Eve is half past midnight on 1 January in Amsterdam.
    #[test]
    fn the_save_time_groups_by_its_year_on_the_users_clock() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "h1", false);
        // 2023-12-31T23:30:00Z
        let s1 = crate::core::testing::insert_source_with_metadata(
            &conn,
            root_id,
            "a.jpg",
            Some(obj),
            1000,
            1704065400,
        );

        let key = ParsedFactKey::parse("source.mtime").unwrap();
        let amsterdam = UserZone::for_test("Europe/Amsterdam");
        let result = compute_distribution(&mut conn, &[s1], &key, 0, &amsterdam).unwrap();
        assert_eq!(result.entries[0].value, "2024");

        let utc = compute_distribution(&mut conn, &[s1], &key, 0, &UserZone::utc()).unwrap();
        assert_eq!(utc.entries[0].value, "2023");
    }

    /// A save time Canon cannot read has no value: the report does not count it, and the
    /// key listing counts `source.mtime` at one fewer than the sources — the same answer
    /// as the key's own `(no value)` line. The sentinel is 9999-12-31 23:59:59 UTC.
    #[test]
    fn a_save_time_canon_cannot_read_is_not_counted() {
        use crate::core::testing::insert_source_with_metadata;
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let obj1 = insert_object(&conn, "h1", false);
        let obj2 = insert_object(&conn, "h2", false);
        let s1 = insert_source_with_metadata(&conn, root_id, "a.jpg", Some(obj1), 1000, 1704065400);
        let s2 =
            insert_source_with_metadata(&conn, root_id, "b.jpg", Some(obj2), 1000, 253402300799);

        let key = ParsedFactKey::parse("source.mtime").unwrap();
        let result = compute_distribution(&mut conn, &[s1, s2], &key, 0, &UserZone::utc()).unwrap();
        assert_eq!(result.sources_with_value, 1);
        assert_eq!(result.total_sources, 2);

        let year = ParsedFactKey::parse("source.mtime|year").unwrap();
        let result =
            compute_distribution(&mut conn, &[s1, s2], &year, 0, &UserZone::utc()).unwrap();
        assert_eq!(result.sources_with_value, 1);

        let keys = compute_all_keys(&mut conn, &[s1, s2], false).unwrap();
        let count_of = |name: &str| keys.keys.iter().find(|k| k.key == name).unwrap().count;
        assert_eq!(count_of("source.mtime"), 1);
        assert_eq!(count_of("source.ext"), 2);
    }

    /// A text modifier after a modifier reads that modifier's printed form, in a report as
    /// in a filter and a pattern: `|weekday|lowercase` reports `monday`.
    #[test]
    fn a_text_modifier_after_a_modifier_reports_its_printed_form() {
        use crate::core::testing::insert_source_with_metadata;
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        // 1718582400 is Monday 17 June 2024, 00:00 UTC.
        let s1 = insert_source_with_metadata(&conn, root_id, "a.jpg", None, 1000, 1718582400);
        let key = ParsedFactKey::parse("source.mtime|weekday|lowercase").unwrap();
        let result = compute_distribution(&mut conn, &[s1], &key, 0, &UserZone::utc()).unwrap();
        assert_eq!(result.entries.len(), 1);
        assert_eq!(result.entries[0].value, "monday");
    }

    /// A report key's type that changes between reading the key and counting stops the report
    /// as an answer, never a modifier failing, on the plain and the grouped report alike — the
    /// same answer a filter gives.
    #[test]
    fn a_type_that_changes_mid_report_refuses_the_report() {
        use crate::core::testing::insert_fact;
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let s1 = insert_source(&conn, root_id, "a.jpg", None);
        let s2 = insert_source(&conn, root_id, "b.jpg", None);
        insert_fact(&conn, s1, "content.Make", "Canon");
        insert_fact(&conn, s2, "content.Make", "Nikon");
        let read = crate::expr::read_report(
            &conn,
            Some("Make|lowercase"),
            &["Make|uppercase".into()],
            &[],
        )
        .unwrap();
        let (key, group) = (read.key.unwrap(), read.grouping);
        // The key's type changes after it was read.
        conn.execute(
            "UPDATE facts SET value_text = NULL, value_num = 5 WHERE entity_id = ?",
            [s2],
        )
        .unwrap();
        let plain = compute_distribution(&mut conn, &[s1, s2], &key, 0, &UserZone::utc());
        let bare = ParsedFactKey::parse("source.ext").unwrap();
        let grouped =
            compute_grouped_distribution(&mut conn, &[s1, s2], &bare, &group, 0, &UserZone::utc());
        // A key with no facts when it was read, which gains a time its modifier cannot take.
        let absent = crate::expr::read_report(&conn, Some("New|number"), &[], &[])
            .unwrap()
            .key
            .unwrap();
        crate::core::testing::insert_time_fact(&conn, s1, "content.New", "2024-06-15");
        let gained = compute_distribution(&mut conn, &[s1, s2], &absent, 0, &UserZone::utc());
        for err in [plain.err(), grouped.err()] {
            let err = err.expect("a changed type must stop the report");
            let refused = err
                .downcast_ref::<crate::expr::ExpressionRefused>()
                .unwrap_or_else(|| panic!("{err}"));
            assert!(refused.is_answer());
            assert_eq!(
                refused.to_string(),
                "content.Make's type changed while this ran; run it again."
            );
        }
        let Err(err) = gained else {
            panic!("a key gaining facts mid-report must stop it");
        };
        let refused = err
            .downcast_ref::<crate::expr::ExpressionRefused>()
            .unwrap_or_else(|| panic!("{err}"));
        assert!(refused.is_answer(), "{refused}");
        assert_eq!(
            refused.to_string(),
            "content.New gained facts while this ran; run it again."
        );
    }

    /// A modifier finer than the value was written gives no value, and the report does
    /// not count that source as having one.
    #[test]
    fn a_value_too_coarse_for_its_modifier_is_not_counted() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let obj1 = insert_object(&conn, "h1", false);
        let obj2 = insert_object(&conn, "h2", false);
        let s1 = insert_source(&conn, root_id, "a.jpg", Some(obj1));
        let s2 = insert_source(&conn, root_id, "b.jpg", Some(obj2));
        insert_time_fact(&conn, s1, "content.DateTimeOriginal", 1719792000);
        // The clock reading "2024", known only to the year.
        conn.execute(
            "INSERT INTO facts (entity_type, entity_id, key, value_time, time_form, time_precision,
                                observed_at, observed_basis_rev)
             VALUES ('source', ?1, 'content.DateTimeOriginal', 1704067200, 'clock', 'year', 0, 0)",
            rusqlite::params![s2],
        )
        .unwrap();

        let month = ParsedFactKey::parse("content.DateTimeOriginal|month").unwrap();
        let result =
            compute_distribution(&mut conn, &[s1, s2], &month, 0, &UserZone::utc()).unwrap();
        assert_eq!(result.sources_with_value, 1);
        assert_eq!(result.entries.len(), 1);

        let year = ParsedFactKey::parse("content.DateTimeOriginal|year").unwrap();
        let result =
            compute_distribution(&mut conn, &[s1, s2], &year, 0, &UserZone::utc()).unwrap();
        assert_eq!(result.sources_with_value, 2);
    }

    /// A hashed source carrying its content hash, the way scanning stores it.
    fn hashed_source(conn: &Connection, root_id: i64, rel_path: &str, hash: &str) -> i64 {
        let obj = insert_object(conn, hash, false);
        conn.execute(
            "INSERT INTO facts (entity_type, entity_id, key, value_text, observed_at, observed_basis_rev)
             VALUES ('object', ?1, 'content.hash.sha256', ?2, 0, NULL)",
            rusqlite::params![obj, hash],
        )
        .unwrap();
        insert_source(conn, root_id, rel_path, Some(obj))
    }

    /// Every built-in key has a value in `facts --key`, alone and grouped.
    /// The defect it names: the report split computed keys from stored facts by
    /// listing visibility (and the grouped path by "is a computed key"), so the
    /// unlisted aliases (`stem`, `size`, `hash`) and the content hash went
    /// to a lookup that could never answer, and printed "(no value)".
    #[test]
    fn every_built_in_key_has_a_value_in_the_report() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let full = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";
        let s1 = hashed_source(&conn, root_id, "2024/a.jpg", full);

        for every in Key::all().chain([Key::ContentFact(crate::expr::CONTENT_HASH.to_string())]) {
            let name = every.name();
            let key = ParsedFactKey::parse(name).unwrap();
            let alone = compute_distribution(&mut conn, &[s1], &key, 0, &UserZone::utc()).unwrap();
            assert_eq!(alone.sources_with_value, 1, "facts --key {name}");

            let by_root = ParsedFactKey::parse("source.root").unwrap();
            let grouped = compute_grouped_distribution(
                &mut conn,
                &[s1],
                &key,
                &[by_root],
                0,
                &UserZone::utc(),
            )
            .unwrap();
            assert_eq!(
                grouped.sources_with_value, 1,
                "facts --key {name} --by-root"
            );
        }

        for (name, want) in [
            ("hash", full),
            ("hash_short", "2cf24dba"),
            ("content.hash.sha256", full),
            ("size", "< 1 KB"),
            ("mtime", "2024"),
            ("stem", "a"),
        ] {
            let key = ParsedFactKey::parse(name).unwrap();
            let result = compute_distribution(&mut conn, &[s1], &key, 0, &UserZone::utc()).unwrap();
            assert_eq!(result.entries[0].value, want, "facts --key {name}");
        }
    }

    // =========================================================================
    // compute_grouped_distribution tests
    // =========================================================================

    #[test]
    fn test_compute_grouped_basic() {
        let mut conn = setup_test_db();
        let root1 = insert_root(&conn, "/photos", "source", false);
        let root2 = insert_root(&conn, "/backup", "source", false);
        let obj1 = insert_object(&conn, "h1", false);
        let obj2 = insert_object(&conn, "h2", false);
        let obj3 = insert_object(&conn, "h3", false);
        let s1 = insert_source(&conn, root1, "a.jpg", Some(obj1));
        let s2 = insert_source(&conn, root1, "b.png", Some(obj2));
        let s3 = insert_source(&conn, root2, "c.jpg", Some(obj3));

        let main_key = ParsedFactKey::parse("source.ext").unwrap();
        let group_key = ParsedFactKey::parse("source.root").unwrap();
        let result = compute_grouped_distribution(
            &mut conn,
            &[s1, s2, s3],
            &main_key,
            &[group_key],
            0,
            &UserZone::utc(),
        )
        .unwrap();

        // "jpg" should have total_count=2 (from both roots), "png" should have 1
        assert_eq!(result.groups.len(), 2);
        let jpg_group = result
            .groups
            .iter()
            .find(|g| g.main_value == "jpg")
            .unwrap();
        assert_eq!(jpg_group.total_count, 2);
        assert_eq!(jpg_group.sub_groups.len(), 2); // one per root

        let png_group = result
            .groups
            .iter()
            .find(|g| g.main_value == "png")
            .unwrap();
        assert_eq!(png_group.total_count, 1);

        assert_eq!(result.sources_with_value, 3);
    }

    /// A source holding a key itself and on its content counts once, under the content's
    /// value, through a modifier and as either key of a grouping.
    #[test]
    fn a_key_held_twice_reports_the_one_value_through_a_modifier_and_a_grouping() {
        use crate::core::testing::{insert_fact, insert_object_fact};
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "h1", false);
        let s = insert_source(&conn, root, "a.jpg", Some(obj));
        insert_fact(&conn, s, "content.Make", "Nikon");
        insert_object_fact(&conn, obj, "content.Make", "Canon");
        insert_fact(&conn, s, "content.Model", "D850");
        insert_object_fact(&conn, obj, "content.Model", "R5");
        let zone = UserZone::utc();
        let key = |k: &str| ParsedFactKey::parse(k).unwrap();

        let lowered =
            compute_distribution(&mut conn, &[s], &key("content.Make|lowercase"), 100, &zone)
                .unwrap();
        let values: Vec<(String, i64)> = lowered
            .entries
            .into_iter()
            .map(|e| (e.value, e.count))
            .collect();
        assert_eq!(values, [("canon".to_string(), 1)]);

        let grouped = compute_grouped_distribution(
            &mut conn,
            &[s],
            &key("content.Make"),
            &[key("content.Model")],
            0,
            &zone,
        )
        .unwrap();
        let rows: Vec<(String, Vec<String>, i64)> = grouped
            .groups
            .iter()
            .flat_map(|g| {
                g.sub_groups
                    .iter()
                    .map(move |e| (g.main_value.clone(), e.group_values.clone(), e.count))
            })
            .collect();
        assert_eq!(rows, [("Canon".to_string(), vec!["R5".to_string()], 1)]);
    }

    #[test]
    fn test_compute_grouped_missing_main_value() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let obj1 = insert_object(&conn, "h1", false);
        let obj2 = insert_object(&conn, "h2", false);
        let s1 = insert_source(&conn, root_id, "a.jpg", Some(obj1));
        let s2 = insert_source(&conn, root_id, "b.jpg", Some(obj2));

        // Only s1 has the main key value
        crate::core::testing::insert_fact(&conn, s1, "content.Make", "Canon");

        let main_key = ParsedFactKey::parse("content.Make").unwrap();
        let group_key = ParsedFactKey::parse("source.root").unwrap();
        let result = compute_grouped_distribution(
            &mut conn,
            &[s1, s2],
            &main_key,
            &[group_key],
            0,
            &UserZone::utc(),
        )
        .unwrap();

        // Only 1 source has the main value, s2 is excluded
        assert_eq!(result.sources_with_value, 1);
        assert_eq!(result.groups.len(), 1);
        assert_eq!(result.groups[0].main_value, "Canon");
        assert_eq!(result.groups[0].total_count, 1);
    }
}
