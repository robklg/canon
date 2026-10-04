//! Filtering, applied: the `--where` language run against a real database.
//!
//! `apply_filters` is the whole of it from outside — hand it source ids and
//! parsed filters, get back the ids that match. Inside, the work runs against
//! a prefetched cache rather than a query per source, with a per-source
//! fallback for the built-in keys that are derived from source columns instead
//! of stored as facts. Those values are read through the resolver that owns
//! them (`get_builtin_value`), never derived again here; the source row they
//! are read from comes from the repository stratum beside this one.

use anyhow::{bail, Result};
use rusqlite::Connection;

use crate::core::domain::fact::FactValue;
use crate::core::domain::time::zone::UserZone;
use crate::expr::domain::cache::FactCache;
use crate::expr::domain::filter::{
    compare_fact_value, detect_status_predicates, extract_comparable_keys, extract_keys, CompareOp,
    Expr, Filter, FilterResult, StatusPredicate,
};
use crate::expr::domain::key::{parse_key_with_modifiers, BuiltinKey};
use crate::expr::domain::transform::{apply_chain, Modified, ModifierCall, PathAccessor};
use crate::expr::domain::value::get_builtin_value;
use crate::expr::repo::{
    fetch_source_row, get_fact_value, is_known_key, prefetch_facts, prefetch_status_data, SourceRow,
};

// ============================================================================
// Filter Evaluation
// ============================================================================

/// Apply a list of filters to a set of source IDs (AND logic between filters). Every time
/// is read on `zone`, the user's clock.
pub fn apply_filters(
    conn: &mut Connection,
    source_ids: &[i64],
    filters: &[Filter],
    zone: &UserZone,
) -> Result<FilterResult> {
    let used_status = detect_status_predicates(filters);

    if filters.is_empty() {
        return Ok(FilterResult {
            source_ids: source_ids.to_vec(),
            used_status,
        });
    }

    // Validate that all keys in filters are known
    validate_filter_keys(conn, filters)?;

    // Extract all keys used in filters and prefetch their values
    let mut all_keys = Vec::new();
    for filter in filters {
        extract_keys(&filter.0, &mut all_keys);
    }
    let mut cache = prefetch_facts(conn, source_ids, &all_keys)?;

    // Prefetch status predicate data (only what's needed)
    prefetch_status_data(conn, source_ids, &used_status, &mut cache)?;

    // Combine all filters with AND
    let combined = if filters.len() == 1 {
        filters[0].0.clone()
    } else {
        Expr::And(filters.iter().map(|f| f.0.clone()).collect())
    };

    let mut result = Vec::new();
    for &source_id in source_ids {
        if eval_expr_cached(conn, source_id, &combined, &cache, zone)? {
            result.push(source_id);
        }
    }
    Ok(FilterResult {
        source_ids: result,
        used_status,
    })
}

/// Validate that all keys used in filters are known (built-in or exist in facts table).
///
/// Exists (`?`) expressions are excluded from validation because their purpose
/// is to test whether a fact is present — an unknown key simply means "not present"
/// for every source, which is a valid (false) result, not an error.
fn validate_filter_keys(conn: &Connection, filters: &[Filter]) -> Result<()> {
    let mut all_keys = Vec::new();
    for filter in filters {
        extract_comparable_keys(&filter.0, &mut all_keys);
    }

    for key in all_keys {
        let (base_key, _, _) = parse_key_with_modifiers(&key)?;
        if !is_known_key(conn, &base_key)? {
            bail!("Unknown fact key: '{base_key}'. Use 'canon facts' to see available keys.");
        }
    }
    Ok(())
}

// ============================================================================
// Fact Checking Functions
// ============================================================================

/// Check one comparison for a source: a built-in key is read through the
/// resolver from the source row, anything else is a stored fact. This is used
/// by the cached version for the built-in keys and for nothing it holds.
fn check_fact_compare(
    conn: &Connection,
    source_id: i64,
    key: &str,
    op: CompareOp,
    value: &str,
    zone: &UserZone,
) -> Result<bool> {
    let (base_key, accessor, modifiers) = parse_key_with_modifiers(key)?;
    let Some(row) = fetch_source_row(conn, source_id)? else {
        return Ok(false);
    };
    let matches = |fact_value: FactValue| {
        apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key, zone)
            .is_some_and(|modified| compare_fact_value(&modified, op, value))
    };

    // Built-in keys the source row computes are read through the resolver that
    // owns their meaning, and only through it: where the row cannot answer one
    // (a NULL device or inode, a save time outside the years Canon reads) no
    // comparison matches, and never a stored row of its name. (`?` reads a NULL
    // device or inode as 0, as `facts` reports it.) The hash keys, which no
    // source row computes, fall through to the stored-fact lookup.
    if let Some(builtin) = BuiltinKey::from_str(&base_key) {
        if builtin.is_computed() {
            return Ok(builtin_value(&row, builtin).is_some_and(matches));
        }
    }

    // Check source facts then object facts
    if get_fact_value(conn, "source", source_id, &base_key)?.is_some_and(matches) {
        return Ok(true);
    }
    if let Some(obj_id) = row.object_id {
        if get_fact_value(conn, "object", obj_id, &base_key)?.is_some_and(matches) {
            return Ok(true);
        }
    }

    Ok(false)
}

/// A built-in's value for one source row, through the resolver that owns it — `None` where
/// the row cannot answer it: a hash key, a NULL device or inode, or a save time outside the
/// years Canon reads.
fn builtin_value(row: &SourceRow, builtin: BuiltinKey) -> Option<FactValue> {
    if (builtin == BuiltinKey::SourceDevice && !row.has_device)
        || (builtin == BuiltinKey::SourceInode && !row.has_inode)
    {
        return None;
    }
    get_builtin_value(&row.attributes, builtin)
}

// ============================================================================
// Cached Evaluation Functions (for bulk filtering)
// ============================================================================

/// Evaluate an expression using prefetched fact cache
fn eval_expr_cached(
    conn: &Connection,
    source_id: i64,
    expr: &Expr,
    cache: &FactCache,
    zone: &UserZone,
) -> Result<bool> {
    match expr {
        Expr::And(exprs) => {
            for e in exprs {
                if !eval_expr_cached(conn, source_id, e, cache, zone)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        Expr::Or(exprs) => {
            for e in exprs {
                if eval_expr_cached(conn, source_id, e, cache, zone)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Expr::Not(e) => Ok(!eval_expr_cached(conn, source_id, e, cache, zone)?),
        Expr::Exists { key } => check_fact_exists_cached(conn, source_id, key, cache, zone),
        Expr::Compare { key, op, value } => {
            check_fact_compare_cached(conn, source_id, key, *op, value, cache, zone)
        }
        Expr::In { key, values } => check_fact_in_cached(conn, source_id, key, values, cache, zone),
        Expr::Status(predicate) => match predicate {
            StatusPredicate::Hashed => Ok(cache.get_object_id(source_id).is_some()),
            StatusPredicate::Archived => {
                let archived_set = cache.archived_objects.as_ref().unwrap();
                Ok(cache
                    .get_object_id(source_id)
                    .is_some_and(|oid| archived_set.contains(&oid)))
            }
            StatusPredicate::Excluded => {
                let excluded_set = cache.excluded_sources.as_ref().unwrap();
                Ok(excluded_set.contains(&source_id))
            }
            StatusPredicate::Enriched => {
                let enriched_set = cache.enriched_sources.as_ref().unwrap();
                Ok(enriched_set.contains(&source_id))
            }
        },
    }
}

/// Whether a key exists for a source: exactly where it has a value, so `NOT K?` selects the
/// sources a report shows under `(no value)` for `K`.
///
/// Read through the key's accessor and modifiers — `K|month?` is false for a time known
/// only to the year. A transform that cannot apply at all (a time modifier on text) keeps
/// the base key's answer: refusing the wrong type is not this check's to do.
fn check_fact_exists_cached(
    conn: &Connection,
    source_id: i64,
    key: &str,
    cache: &FactCache,
    zone: &UserZone,
) -> Result<bool> {
    let (base_key, accessor, modifiers) = parse_key_with_modifiers(key)?;
    let exists_through =
        |value: FactValue| match apply_chain(value, accessor.as_ref(), &modifiers, key, true, zone)
        {
            Ok(Modified::Value(_)) | Err(_) => true,
            Ok(Modified::NoValue(_)) => false,
        };

    // Check cache for stored facts
    if cache.has_key(&base_key) {
        for value in [
            cache.get_source_fact(source_id, &base_key),
            cache.get_object_fact(source_id, &base_key),
        ]
        .into_iter()
        .flatten()
        {
            if exists_through(value.clone()) {
                return Ok(true);
            }
        }
    }

    // A built-in the source row answers (`is_computed`) exists where the resolver gives it
    // a value for this source — the same answer its value comes from, so a save time Canon
    // cannot read does not exist. The keys the row cannot answer (the hash keys) exist when
    // the source is linked to an object.
    if let Some(builtin) = BuiltinKey::from_str(&base_key) {
        if builtin.is_computed() {
            return Ok(fetch_source_row(conn, source_id)?
                .and_then(|row| get_builtin_value(&row.attributes, builtin))
                .is_some_and(exists_through));
        }
        return Ok(cache.get_object_id(source_id).is_some());
    }
    Ok(false)
}

fn check_fact_compare_cached(
    conn: &Connection,
    source_id: i64,
    key: &str,
    op: CompareOp,
    value: &str,
    cache: &FactCache,
    zone: &UserZone,
) -> Result<bool> {
    let (base_key, accessor, modifiers) = parse_key_with_modifiers(key)?;

    // Handle built-in keys (still need DB for source columns)
    if BuiltinKey::from_str(&base_key).is_some() {
        // For built-ins, fall back to uncached version (they query source table, not facts)
        return check_fact_compare(conn, source_id, key, op, value, zone);
    }

    // Use cache for stored facts
    if let Some(fact_value) = cache.get_source_fact(source_id, &base_key) {
        if let Some(modified) =
            apply_accessor_and_modifiers(fact_value.clone(), &accessor, &modifiers, key, zone)
        {
            if compare_fact_value(&modified, op, value) {
                return Ok(true);
            }
        }
    }

    if let Some(fact_value) = cache.get_object_fact(source_id, &base_key) {
        if let Some(modified) =
            apply_accessor_and_modifiers(fact_value.clone(), &accessor, &modifiers, key, zone)
        {
            if compare_fact_value(&modified, op, value) {
                return Ok(true);
            }
        }
    }

    Ok(false)
}

fn check_fact_in_cached(
    conn: &Connection,
    source_id: i64,
    key: &str,
    values: &[String],
    cache: &FactCache,
    zone: &UserZone,
) -> Result<bool> {
    for value in values {
        if check_fact_compare_cached(conn, source_id, key, CompareOp::Eq, value, cache, zone)? {
            return Ok(true);
        }
    }
    Ok(false)
}

// ============================================================================
// Modifier and Accessor Application
// ============================================================================

/// The value a comparison reads: the key's value through its accessor and modifiers, or
/// `None` where there is nothing to compare — a modifier that gave no value reads as a
/// missing fact, and one that cannot apply (a time modifier on text, from bad data) as no
/// match rather than an error.
fn apply_accessor_and_modifiers(
    value: FactValue,
    accessor: &Option<PathAccessor>,
    modifiers: &[ModifierCall],
    key: &str,
    zone: &UserZone,
) -> Option<FactValue> {
    // for_display: true since filters are typically for display/comparison
    match apply_chain(value, accessor.as_ref(), modifiers, key, true, zone) {
        Ok(Modified::Value(v)) => Some(v),
        Ok(Modified::NoValue(_)) | Err(_) => None,
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::repo::open_in_memory_for_test;
    use crate::expr::domain::filter::STATUS_KEYWORDS;
    use crate::expr::domain::value::SourceAttributes;
    use rusqlite::Connection as RawConnection;

    fn setup_test_db() -> RawConnection {
        open_in_memory_for_test()
    }

    fn insert_root(conn: &RawConnection, path: &str) -> i64 {
        conn.execute("INSERT INTO roots (path) VALUES (?)", [path])
            .unwrap();
        conn.last_insert_rowid()
    }

    fn insert_source(conn: &RawConnection, root_id: i64, rel_path: &str) -> i64 {
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, size, mtime, partial_hash, scanned_at, last_seen_at, device, inode)
             VALUES (?, ?, 1000, 1704067200, '', 0, 0, 0, 0)",
            rusqlite::params![root_id, rel_path],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    #[test]
    fn filter_out_of_bounds_index_is_non_match() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos");

        // 1-segment path: "image.jpg" (index [1] is out of bounds)
        let s1 = insert_source(&conn, root, "image.jpg");
        // 3-segment path: "2024/vacation/photo.jpg" (index [1] = "vacation")
        let s2 = insert_source(&conn, root, "2024/vacation/photo.jpg");
        // 2-segment path: "2024/doc.txt" (index [1] = "doc.txt")
        let s3 = insert_source(&conn, root, "2024/doc.txt");

        let filter = Filter::parse("source.rel_path[1]~'*tion*'").unwrap();
        let result = apply_filters(&mut conn, &[s1, s2, s3], &[filter], &UserZone::utc()).unwrap();

        // s1 silently skipped (out of bounds), s2 matches, s3 doesn't match glob
        assert_eq!(result.source_ids, vec![s2]);
    }

    #[test]
    fn filter_out_of_bounds_negative_index_is_non_match() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos");

        // 1-segment path: [-3] is out of bounds
        let s1 = insert_source(&conn, root, "image.jpg");
        // 3-segment path: [-3] = "2024"
        let s2 = insert_source(&conn, root, "2024/vacation/photo.jpg");

        let filter = Filter::parse("source.rel_path[-3]=2024").unwrap();
        let result = apply_filters(&mut conn, &[s1, s2], &[filter], &UserZone::utc()).unwrap();

        assert_eq!(result.source_ids, vec![s2]);
    }

    #[test]
    fn filter_out_of_bounds_slice_is_non_match() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos");

        // 1-segment path: [2:4] is out of bounds
        let s1 = insert_source(&conn, root, "image.jpg");
        // 4-segment path: [2:4] = "c/d"
        let s2 = insert_source(&conn, root, "a/b/c/d");

        let filter = Filter::parse("source.rel_path[2:4]~'c*'").unwrap();
        let result = apply_filters(&mut conn, &[s1, s2], &[filter], &UserZone::utc()).unwrap();

        assert_eq!(result.source_ids, vec![s2]);
    }

    #[test]
    fn filter_modifier_failure_on_builtin_is_non_match() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos");

        // source.ext is a text value; |year modifier should fail on it
        let s1 = insert_source(&conn, root, "photo.jpg");

        let filter = Filter::parse("source.ext|year=2024").unwrap();
        let result = apply_filters(&mut conn, &[s1], &[filter], &UserZone::utc()).unwrap();

        // Modifier failure treated as non-match, not error
        assert!(result.source_ids.is_empty());
    }

    #[test]
    fn stem_in_a_filter_is_the_filename_without_its_last_extension() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos");
        let s1 = insert_source(&conn, root, "2024/IMG_001.JPG");
        let s2 = insert_source(&conn, root, "2024/archive.tar.gz");
        let s3 = insert_source(&conn, root, "NoExt");
        let all = [s1, s2, s3];

        let ids = |conn: &mut RawConnection, expr: &str| {
            let filter = Filter::parse(expr).unwrap();
            apply_filters(conn, &all, &[filter], &UserZone::utc())
                .unwrap()
                .source_ids
        };

        assert_eq!(ids(&mut conn, "stem=IMG_001"), vec![s1]);
        assert_eq!(ids(&mut conn, "stem=archive.tar"), vec![s2]);
        assert_eq!(ids(&mut conn, "stem=NoExt"), vec![s3]);
        assert_eq!(ids(&mut conn, "stem=other"), Vec::<i64>::new());
        // A modifier applies to the stem, not to the filename around it.
        assert_eq!(ids(&mut conn, "stem|uppercase=ARCHIVE.TAR"), vec![s2]);
    }

    // ========================================================================
    // Status predicate evaluation
    // ========================================================================

    fn insert_source_with_object(
        conn: &RawConnection,
        root_id: i64,
        rel_path: &str,
        object_id: Option<i64>,
    ) -> i64 {
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, object_id, size, mtime, partial_hash, scanned_at, last_seen_at, device, inode)
             VALUES (?, ?, ?, 1000, 1704067200, '', 0, 0, 0, 0)",
            rusqlite::params![root_id, rel_path, object_id],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn insert_object(conn: &RawConnection, hash: &str) -> i64 {
        conn.execute(
            "INSERT INTO objects (hash_type, hash_value) VALUES ('sha256', ?)",
            [hash],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn insert_object_excluded(conn: &RawConnection, hash: &str) -> i64 {
        conn.execute(
            "INSERT INTO objects (hash_type, hash_value, excluded) VALUES ('sha256', ?, 1)",
            [hash],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn insert_archive_root(conn: &RawConnection, path: &str) -> i64 {
        conn.execute(
            "INSERT INTO roots (path, role) VALUES (?, 'archive')",
            [path],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn insert_fact_entry(
        conn: &RawConnection,
        entity_type: &str,
        entity_id: i64,
        key: &str,
        value: &str,
    ) {
        // observed_basis_rev must be NULL for object-type entities (CHECK constraint)
        let basis_rev: Option<i64> = if entity_type == "source" {
            Some(0)
        } else {
            None
        };
        conn.execute(
            "INSERT INTO facts (entity_type, entity_id, key, value_text, observed_at, observed_basis_rev) VALUES (?, ?, ?, ?, 0, ?)",
            rusqlite::params![entity_type, entity_id, key, value, basis_rev],
        )
        .unwrap();
    }

    #[test]
    fn filter_archived_matches() {
        let mut conn = setup_test_db();
        let src_root = insert_root(&conn, "/src");
        let arc_root = insert_archive_root(&conn, "/archive");
        let obj = insert_object(&conn, "hash_a");

        // Source in source root with content also in archive
        let s1 = insert_source_with_object(&conn, src_root, "a.jpg", Some(obj));
        // Same content in archive
        insert_source_with_object(&conn, arc_root, "a.jpg", Some(obj));

        let filter = Filter::parse("archived?").unwrap();
        let result = apply_filters(&mut conn, &[s1], &[filter], &UserZone::utc()).unwrap();
        assert_eq!(result.source_ids, vec![s1]);
    }

    #[test]
    fn filter_archived_excludes_unhashed() {
        let mut conn = setup_test_db();
        let src_root = insert_root(&conn, "/src");
        // Unhashed source (no object_id)
        let s1 = insert_source_with_object(&conn, src_root, "a.jpg", None);

        let filter = Filter::parse("archived?").unwrap();
        let result = apply_filters(&mut conn, &[s1], &[filter], &UserZone::utc()).unwrap();
        assert!(result.source_ids.is_empty());
    }

    #[test]
    fn filter_archived_excludes_unarchived_hashed() {
        let mut conn = setup_test_db();
        let src_root = insert_root(&conn, "/src");
        let obj = insert_object(&conn, "hash_b");
        // Hashed but not in any archive
        let s1 = insert_source_with_object(&conn, src_root, "a.jpg", Some(obj));

        let filter = Filter::parse("archived?").unwrap();
        let result = apply_filters(&mut conn, &[s1], &[filter], &UserZone::utc()).unwrap();
        assert!(result.source_ids.is_empty());
    }

    #[test]
    fn filter_not_archived_includes_unhashed() {
        let mut conn = setup_test_db();
        let src_root = insert_root(&conn, "/src");
        let s1 = insert_source_with_object(&conn, src_root, "a.jpg", None);

        let filter = Filter::parse("NOT archived?").unwrap();
        let result = apply_filters(&mut conn, &[s1], &[filter], &UserZone::utc()).unwrap();
        assert_eq!(result.source_ids, vec![s1]);
    }

    #[test]
    fn filter_hashed_matches() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/src");
        let obj = insert_object(&conn, "hash_c");
        let s1 = insert_source_with_object(&conn, root, "a.jpg", Some(obj));
        let s2 = insert_source_with_object(&conn, root, "b.jpg", None);

        let filter = Filter::parse("hashed?").unwrap();
        let result = apply_filters(&mut conn, &[s1, s2], &[filter], &UserZone::utc()).unwrap();
        assert_eq!(result.source_ids, vec![s1]);
    }

    #[test]
    fn filter_hashed_equivalence() {
        // hashed? and content.hash.sha256? should produce identical results
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/src");
        let obj = insert_object(&conn, "hash_d");
        let s1 = insert_source_with_object(&conn, root, "a.jpg", Some(obj));
        let s2 = insert_source_with_object(&conn, root, "b.jpg", None);
        let ids = [s1, s2];

        let f1 = Filter::parse("hashed?").unwrap();
        let f2 = Filter::parse("content.hash.sha256?").unwrap();
        let r1 = apply_filters(&mut conn, &ids, &[f1], &UserZone::utc()).unwrap();
        let r2 = apply_filters(&mut conn, &ids, &[f2], &UserZone::utc()).unwrap();
        assert_eq!(r1.source_ids, r2.source_ids);
    }

    #[test]
    fn ext_in_a_filter_keeps_its_case_and_lowercase_folds_it() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/src");
        let upper = insert_source(&conn, root, "2024/IMG_001.JPG");
        let lower = insert_source(&conn, root, "2024/b.jpg");
        let ids = [upper, lower];

        let run = |conn: &mut RawConnection, where_clause: &str| {
            let filter = Filter::parse(where_clause).unwrap();
            apply_filters(conn, &ids, &[filter], &UserZone::utc())
                .unwrap()
                .source_ids
        };
        assert_eq!(run(&mut conn, "source.ext=JPG"), vec![upper]);
        assert_eq!(run(&mut conn, "ext=jpg"), vec![lower]);
        assert_eq!(
            run(&mut conn, "source.ext|lowercase=jpg"),
            vec![upper, lower]
        );
        assert_eq!(run(&mut conn, "ext|uppercase=JPG"), vec![upper, lower]);
    }

    #[test]
    fn the_filter_answers_every_computed_key_as_the_resolver_does() {
        use strum::IntoEnumIterator;

        // Two sources that differ in every attribute a built-in reads, so an
        // equality selects exactly one of them and an inequality the other.
        let mut conn = setup_test_db();
        let root_a = insert_root(&conn, "/src");
        let root_b = insert_root(&conn, "/other");
        let insert = |root: i64, rel_path: &str, size: i64, mtime: i64, device: i64, inode: i64| {
            conn.execute(
                "INSERT INTO sources (root_id, rel_path, size, mtime, partial_hash, scanned_at, last_seen_at, device, inode)
                 VALUES (?, ?, ?, ?, '', 0, 0, ?, ?)",
                rusqlite::params![root, rel_path, size, mtime, device, inode],
            )
            .unwrap();
            conn.last_insert_rowid()
        };
        let subject = insert(root_a, "2024/IMG_001.JPG", 1000, 1704067200, 7, 11);
        let other = insert(root_b, "b.png", 2000, 1710000000, 8, 12);
        let attributes = SourceAttributes {
            id: subject,
            root_id: root_a,
            root_path: "/src".to_string(),
            rel_path: "2024/IMG_001.JPG".to_string(),
            size: 1000,
            mtime: 1704067200,
            device: 7,
            inode: 11,
        };

        for builtin in BuiltinKey::iter().filter(|b| b.is_computed()) {
            let name: &'static str = builtin.into();
            let expected = get_builtin_value(&attributes, builtin)
                .unwrap_or_else(|| panic!("{name} is computed but the resolver has no answer"));
            let text = match expected {
                FactValue::Text(t) | FactValue::Path(t) => t,
                FactValue::Num(n) => format!("{n}"),
                FactValue::Time(t) => t.legacy_epoch_number().to_string(),
            };

            for (op, want) in [("=", subject), ("!=", other)] {
                let filter = Filter::parse(&format!("{name}{op}{text}")).unwrap();
                let matched =
                    apply_filters(&mut conn, &[subject, other], &[filter], &UserZone::utc())
                        .unwrap_or_else(|e| panic!("{name}{op}{text} failed: {e}"))
                        .source_ids;
                assert_eq!(
                    matched,
                    vec![want],
                    "{name}{op}{text} must answer as the resolver's own value does"
                );
            }
        }
    }

    /// The other half of the keeper above: a built-in the source row cannot
    /// answer reads the stored content hash, so every `BuiltinKey` answers in
    /// `--where`. The claim table is written out rather than derived from
    /// `BuiltinKey::reading`, and it must name every non-computed built-in.
    ///
    /// The defect it names: `hash` and `hash_short` reached the stored lookup
    /// under their own names, where no fact is ever kept — every comparison
    /// false, and `NOT` selecting every source.
    #[test]
    fn a_built_in_the_source_row_cannot_answer_reads_the_content_hash() {
        use strum::IntoEnumIterator;

        let full = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";
        let claims = [
            (BuiltinKey::Hash, full),
            (BuiltinKey::HashShort, "2cf24dba"),
            (BuiltinKey::ContentHashSha256, full),
        ];
        for builtin in BuiltinKey::iter().filter(|b| !b.is_computed()) {
            assert!(
                claims.iter().any(|(k, _)| *k == builtin),
                "{} has no claim in this table",
                <&'static str>::from(builtin)
            );
        }

        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/src");
        let subject_obj = insert_object(&conn, "subject");
        let other_obj = insert_object(&conn, "other");
        insert_fact_entry(&conn, "object", subject_obj, "content.hash.sha256", full);
        insert_fact_entry(
            &conn,
            "object",
            other_obj,
            "content.hash.sha256",
            "711e9609339e92b03ddc0a211827dba421f38f9ed8b9d806e1ffdd8c15ffa03d",
        );
        let subject = insert_source_with_object(&conn, root, "a.txt", Some(subject_obj));
        let other = insert_source_with_object(&conn, root, "b.txt", Some(other_obj));

        for (builtin, value) in claims {
            let name: &'static str = builtin.into();
            for (clause, want) in [
                (format!("{name}=\"{value}\""), vec![subject]),
                (format!("NOT {name}=\"{value}\""), vec![other]),
                (format!("{name} IN (\"{value}\")"), vec![subject]),
            ] {
                let filter = Filter::parse(&clause).unwrap();
                let matched =
                    apply_filters(&mut conn, &[subject, other], &[filter], &UserZone::utc())
                        .unwrap_or_else(|e| panic!("{clause} failed: {e}"))
                        .source_ids;
                assert_eq!(matched, want, "{clause} must read the content hash");
            }
        }
    }

    #[test]
    fn a_null_device_or_inode_column_answers_false_for_its_own_key() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/src");
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, size, mtime, partial_hash, scanned_at, last_seen_at, device, inode)
             VALUES (?, 'a.jpg', 1000, 1704067200, '', 0, 0, NULL, NULL)",
            [root],
        )
        .unwrap();
        let s1 = conn.last_insert_rowid();

        for clause in [
            "source.device=0",
            "source.inode=0",
            "source.device!=5",
            "source.inode!=5",
        ] {
            let filter = Filter::parse(clause).unwrap();
            let result = apply_filters(&mut conn, &[s1], &[filter], &UserZone::utc()).unwrap();
            assert!(
                result.source_ids.is_empty(),
                "{clause} must not select a NULL column"
            );
        }
        let filter = Filter::parse("source.size=1000").unwrap();
        let result = apply_filters(&mut conn, &[s1], &[filter], &UserZone::utc()).unwrap();
        assert_eq!(result.source_ids, vec![s1]);
    }

    #[test]
    fn a_built_in_exists_when_the_resolver_answers_it_or_the_source_is_linked_to_an_object() {
        use strum::IntoEnumIterator;

        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/src");
        let obj = insert_object(&conn, "hash_exists");
        let hashed = insert_source_with_object(&conn, root, "a.jpg", Some(obj));
        let unhashed = insert_source_with_object(&conn, root, "b.jpg", None);
        let ids = [hashed, unhashed];

        for builtin in BuiltinKey::iter() {
            let name: &'static str = builtin.into();
            let filter = Filter::parse(&format!("{name}?")).unwrap();
            let matched = apply_filters(&mut conn, &ids, &[filter], &UserZone::utc())
                .unwrap()
                .source_ids;
            let expected = if builtin.is_computed() {
                vec![hashed, unhashed]
            } else {
                vec![hashed]
            };
            assert_eq!(matched, expected, "{name}? disagrees with the resolver");
        }
    }

    #[test]
    fn filter_excluded_source_level() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/src");
        // Source-level excluded
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, size, mtime, partial_hash, scanned_at, last_seen_at, device, inode, excluded)
             VALUES (?, 'excl.jpg', 1000, 1704067200, '', 0, 0, 0, 0, 1)",
            [root],
        )
        .unwrap();
        let s1 = conn.last_insert_rowid();

        let filter = Filter::parse("excluded?").unwrap();
        let result = apply_filters(&mut conn, &[s1], &[filter], &UserZone::utc()).unwrap();
        assert_eq!(result.source_ids, vec![s1]);
    }

    #[test]
    fn filter_excluded_object_level() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/src");
        let obj = insert_object_excluded(&conn, "hash_excl");
        let s1 = insert_source_with_object(&conn, root, "a.jpg", Some(obj));

        let filter = Filter::parse("excluded?").unwrap();
        let result = apply_filters(&mut conn, &[s1], &[filter], &UserZone::utc()).unwrap();
        assert_eq!(result.source_ids, vec![s1]);
    }

    #[test]
    fn filter_excluded_non_excluded_fails() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/src");
        let s1 = insert_source_with_object(&conn, root, "a.jpg", None);

        let filter = Filter::parse("excluded?").unwrap();
        let result = apply_filters(&mut conn, &[s1], &[filter], &UserZone::utc()).unwrap();
        assert!(result.source_ids.is_empty());
    }

    #[test]
    fn filter_enriched_with_object_facts() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/src");
        let obj = insert_object(&conn, "hash_e");
        let s1 = insert_source_with_object(&conn, root, "a.jpg", Some(obj));
        // Object-level fact (not hash)
        insert_fact_entry(&conn, "object", obj, "content.mime", "image/jpeg");

        let filter = Filter::parse("enriched?").unwrap();
        let result = apply_filters(&mut conn, &[s1], &[filter], &UserZone::utc()).unwrap();
        assert_eq!(result.source_ids, vec![s1]);
    }

    #[test]
    fn filter_enriched_with_source_facts() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/src");
        // Unhashed source with source-level fact
        let s1 = insert_source_with_object(&conn, root, "a.jpg", None);
        insert_fact_entry(&conn, "source", s1, "policy.tag", "keep");

        let filter = Filter::parse("enriched?").unwrap();
        let result = apply_filters(&mut conn, &[s1], &[filter], &UserZone::utc()).unwrap();
        assert_eq!(result.source_ids, vec![s1]);
    }

    #[test]
    fn filter_enriched_hash_only_fails() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/src");
        let obj = insert_object(&conn, "hash_f");
        let s1 = insert_source_with_object(&conn, root, "a.jpg", Some(obj));
        // Only content.hash.sha256 fact — should NOT count as enriched
        insert_fact_entry(&conn, "object", obj, "content.hash.sha256", "hash_f");

        let filter = Filter::parse("enriched?").unwrap();
        let result = apply_filters(&mut conn, &[s1], &[filter], &UserZone::utc()).unwrap();
        assert!(result.source_ids.is_empty());
    }

    #[test]
    fn filter_enriched_no_facts_fails() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/src");
        let s1 = insert_source_with_object(&conn, root, "a.jpg", None);

        let filter = Filter::parse("enriched?").unwrap();
        let result = apply_filters(&mut conn, &[s1], &[filter], &UserZone::utc()).unwrap();
        assert!(result.source_ids.is_empty());
    }

    #[test]
    fn filter_composed_not_archived_and_hashed() {
        let mut conn = setup_test_db();
        let src_root = insert_root(&conn, "/src");
        let arc_root = insert_archive_root(&conn, "/archive");
        let obj_a = insert_object(&conn, "hash_archived");
        let obj_b = insert_object(&conn, "hash_not_archived");

        // s1: hashed + archived
        let s1 = insert_source_with_object(&conn, src_root, "a.jpg", Some(obj_a));
        insert_source_with_object(&conn, arc_root, "a.jpg", Some(obj_a));
        // s2: hashed + not archived
        let s2 = insert_source_with_object(&conn, src_root, "b.jpg", Some(obj_b));
        // s3: unhashed
        let s3 = insert_source_with_object(&conn, src_root, "c.jpg", None);

        let filter = Filter::parse("NOT archived? AND hashed?").unwrap();
        let result = apply_filters(&mut conn, &[s1, s2, s3], &[filter], &UserZone::utc()).unwrap();
        assert_eq!(result.source_ids, vec![s2]);
    }

    // ========================================================================
    // Status predicate metadata
    // ========================================================================

    #[test]
    fn filter_result_flags_archived_used() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/src");
        let s1 = insert_source(&conn, root, "a.jpg");

        let filter = Filter::parse("archived?").unwrap();
        let result = apply_filters(&mut conn, &[s1], &[filter], &UserZone::utc()).unwrap();
        assert!(result.used_status.archived);
        assert!(!result.used_status.hashed);
        assert!(!result.used_status.excluded);
        assert!(!result.used_status.enriched);
    }

    #[test]
    fn filter_result_flags_not_set_when_unused() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/src");
        let s1 = insert_source(&conn, root, "a.jpg");

        let filter = Filter::parse("source.ext=jpg").unwrap();
        let result = apply_filters(&mut conn, &[s1], &[filter], &UserZone::utc()).unwrap();
        assert!(!result.used_status.archived);
        assert!(!result.used_status.hashed);
        assert!(!result.used_status.excluded);
        assert!(!result.used_status.enriched);
    }

    #[test]
    fn filter_result_flags_nested_detection() {
        let filter = Filter::parse("NOT (archived? AND excluded?)").unwrap();
        let used = detect_status_predicates(&[filter]);
        assert!(used.archived);
        assert!(used.excluded);
        assert!(!used.hashed);
        assert!(!used.enriched);
    }

    /// Regression: `NOT content.mime?` must not error when no facts are ingested.
    /// Existence checks should return false (not error) for unknown keys.
    #[test]
    fn exists_unknown_key_returns_false_not_error() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos");
        let s1 = insert_source(&conn, root, "a.jpg");
        let s2 = insert_source(&conn, root, "b.jpg");

        // No facts ingested — content.mime doesn't exist in facts table
        let filter = Filter::parse("NOT content.mime?").unwrap();
        let result = apply_filters(&mut conn, &[s1, s2], &[filter], &UserZone::utc()).unwrap();

        // All sources should match (content.mime doesn't exist for any)
        assert_eq!(result.source_ids.len(), 2);
    }

    /// Existence check with unknown key (positive) should match nothing.
    #[test]
    fn exists_unknown_key_positive_matches_nothing() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos");
        let s1 = insert_source(&conn, root, "a.jpg");

        let filter = Filter::parse("content.mime?").unwrap();
        let result = apply_filters(&mut conn, &[s1], &[filter], &UserZone::utc()).unwrap();

        assert!(result.source_ids.is_empty());
    }

    /// Compare with unknown key should still error (typo protection).
    #[test]
    fn compare_unknown_key_still_errors() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos");
        let s1 = insert_source(&conn, root, "a.jpg");

        let filter = Filter::parse("content.mime=image/jpeg").unwrap();
        let result = apply_filters(&mut conn, &[s1], &[filter], &UserZone::utc());

        let err = result
            .err()
            .expect("should error on unknown key in comparison");
        assert!(err.to_string().contains("Unknown fact key"));
    }

    #[test]
    fn every_status_predicate_prefetches_before_evaluation() {
        // Evaluating a status predicate reads its prefetched set without
        // checking the set is there, so a predicate that reaches evaluation
        // unprefetched takes the process down on the user's first query.
        // This walks every predicate the language has rather than the ones
        // that happened to exist when it was written, and asserts each is
        // reachable from a keyword — a predicate nothing can ask for cannot
        // be tested here at all.
        use strum::IntoEnumIterator;

        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos");
        let obj = insert_object(&conn, "hash1");
        let s1 = insert_source_with_object(&conn, root, "a.jpg", Some(obj));

        for predicate in StatusPredicate::iter() {
            let keyword = STATUS_KEYWORDS
                .iter()
                .find(|(_, p)| *p == predicate)
                .map(|(k, _)| *k)
                .unwrap_or_else(|| panic!("{predicate:?} has no keyword to ask for it by"));

            let filter = Filter::parse(&format!("{keyword}?")).unwrap();
            apply_filters(&mut conn, &[s1], &[filter], &UserZone::utc())
                .unwrap_or_else(|e| panic!("{keyword}? failed: {e}"));

            let negated = Filter::parse(&format!("NOT {keyword}?")).unwrap();
            apply_filters(&mut conn, &[s1], &[negated], &UserZone::utc())
                .unwrap_or_else(|e| panic!("NOT {keyword}? failed: {e}"));
        }
    }

    // ------------------------------------------------------------------------
    // Times on the user's clock
    // ------------------------------------------------------------------------

    /// A source with a given save time.
    fn insert_source_saved_at(
        conn: &RawConnection,
        root_id: i64,
        rel_path: &str,
        mtime: i64,
    ) -> i64 {
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, size, mtime, partial_hash, scanned_at, last_seen_at, device, inode)
             VALUES (?, ?, 1000, ?, '', 0, 0, 0, 0)",
            rusqlite::params![root_id, rel_path, mtime],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    /// Store a time fact on a source through the one conversion pair.
    fn insert_time_fact(conn: &RawConnection, source_id: i64, key: &str, written: &str) {
        let value =
            FactValue::Time(crate::core::domain::time::reading::read_time(written).unwrap());
        let p = crate::core::repo::fact::fact_value_params(&value).unwrap();
        conn.execute(
            &format!(
                "INSERT INTO facts (entity_type, entity_id, key, {}, observed_at, observed_basis_rev)
                 VALUES ('source', ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, 0)",
                crate::core::repo::fact::FACT_VALUE_COLUMNS
            ),
            rusqlite::params![source_id, key, p[0], p[1], p[2], p[3], p[4], p[5], p[6]],
        )
        .unwrap();
    }

    fn selected(conn: &mut RawConnection, ids: &[i64], filter: &str, zone: &UserZone) -> Vec<i64> {
        apply_filters(conn, ids, &[Filter::parse(filter).unwrap()], zone)
            .unwrap()
            .source_ids
    }

    /// 2023-12-31T23:30:00Z — 00:30 on New Year's Day in Amsterdam.
    const NEW_YEARS_EVE_2330_UTC: i64 = 1704065400;
    /// Past the last instant Canon reads (jiff's `Timestamp::MAX`).
    const PAST_THE_LINE: i64 = 253402207201;

    #[test]
    fn a_save_time_selects_by_its_year_on_the_users_clock() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos");
        let s = insert_source_saved_at(&conn, root, "a.jpg", NEW_YEARS_EVE_2330_UTC);
        let amsterdam = UserZone::for_test("Europe/Amsterdam");

        assert_eq!(
            selected(&mut conn, &[s], "source.mtime|year=2024", &amsterdam),
            vec![s]
        );
        assert!(selected(&mut conn, &[s], "source.mtime|year=2023", &amsterdam).is_empty());
        assert_eq!(
            selected(&mut conn, &[s], "source.mtime|year=2023", &UserZone::utc()),
            vec![s]
        );
    }

    /// A date typed in `--where` keeps today's reading — a UTC instant — whatever the
    /// user's zone, until typed dates are read on the user's clock.
    #[test]
    fn a_typed_date_selects_what_it_selected_before() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos");
        let eve = insert_source_saved_at(&conn, root, "eve.jpg", NEW_YEARS_EVE_2330_UTC);
        let midnight = insert_source_saved_at(&conn, root, "midnight.jpg", 1704067200);
        let amsterdam = UserZone::for_test("Europe/Amsterdam");

        for zone in [amsterdam, UserZone::utc()] {
            assert_eq!(
                selected(
                    &mut conn,
                    &[eve, midnight],
                    "source.mtime>=2024-01-01",
                    &zone
                ),
                vec![midnight]
            );
        }
    }

    /// A save time Canon cannot read has no value at the existence door: `NOT source.mtime?`
    /// selects exactly it, `source.mtime?` leaves it out — the same answer its value has.
    #[test]
    fn a_save_time_canon_cannot_read_does_not_exist() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos");
        let readable = insert_source_saved_at(&conn, root, "a.jpg", NEW_YEARS_EVE_2330_UTC);
        let past = insert_source_saved_at(&conn, root, "b.jpg", PAST_THE_LINE);
        // 9999-12-31 23:59:59 UTC: a moment some zone already reads in year 10000.
        let sentinel = insert_source_saved_at(&conn, root, "c.jpg", 253402300799);
        let all = [readable, past, sentinel];
        let zone = UserZone::for_test("Europe/Amsterdam");

        assert_eq!(
            selected(&mut conn, &all, "NOT source.mtime?", &zone),
            vec![past, sentinel]
        );
        assert_eq!(
            selected(&mut conn, &all, "source.mtime?", &zone),
            vec![readable]
        );
        // Every other computed built-in still exists for every source.
        assert_eq!(
            selected(&mut conn, &all, "source.size?", &zone),
            all.to_vec()
        );
    }

    /// A computed key the source row cannot answer has no value at the compare, and a stored
    /// row wearing its name is never read in its place. Its twin at the report's resolver is
    /// `a_computed_key_never_falls_through_to_a_stored_row`.
    #[test]
    fn a_computed_key_the_row_cannot_answer_matches_no_stored_row() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos");
        let sentinel = insert_source_saved_at(&conn, root, "c.jpg", 253402300799);
        insert_time_fact(&conn, sentinel, "source.mtime", "2020");
        let zone = UserZone::utc();

        for filter in ["source.mtime|year = 2020", "mtime < 2021-01-01"] {
            assert!(
                selected(&mut conn, &[sentinel], filter, &zone).is_empty(),
                "{filter}"
            );
        }
    }

    /// `?` reads through the key's modifiers: a time known only to the year has no month, so
    /// `NOT K|month?` selects exactly what a report shows under `(no value)` for `K|month`.
    #[test]
    fn existence_honours_the_keys_modifiers() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos");
        let year_only = insert_source_saved_at(&conn, root, "a.jpg", 0);
        let full = insert_source_saved_at(&conn, root, "b.jpg", 0);
        let texted = insert_source_saved_at(&conn, root, "c.jpg", 0);
        insert_time_fact(&conn, year_only, "content.When", "2024");
        insert_time_fact(&conn, full, "content.When", "2024-06-15T14:30:05");
        insert_fact_entry(&conn, "source", texted, "content.Make", "Canon");
        let all = [year_only, full, texted];
        let zone = UserZone::for_test("Europe/Amsterdam");

        assert_eq!(
            selected(&mut conn, &all, "content.When|month?", &zone),
            vec![full]
        );
        assert_eq!(
            selected(&mut conn, &all, "NOT content.When|month?", &zone),
            vec![year_only, texted]
        );
        assert_eq!(
            selected(&mut conn, &all, "content.When|year?", &zone),
            vec![year_only, full]
        );
        // A modifier that applies keeps the base key's answer; one that cannot apply at all
        // (a time modifier on text) keeps it too — refusing the type is not this check's job.
        assert_eq!(
            selected(&mut conn, &all, "content.Make|lowercase?", &zone),
            selected(&mut conn, &all, "content.Make?", &zone)
        );
        assert_eq!(
            selected(&mut conn, &all, "content.Make|year?", &zone),
            vec![texted]
        );
    }
}
