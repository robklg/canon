//! Filtering, applied: the `--where` language run against a real database.
//!
//! `apply_filters` is the whole of it from outside — hand it source ids and
//! parsed filters, get back the ids that match. Inside, the work runs against
//! a prefetched cache rather than a query per source, with a per-source
//! fallback for the built-in keys that are derived from source columns instead
//! of stored as facts.
//!
//! That fallback queries the database directly rather than going through the
//! repository stratum beside it — the one place here that does. It is a known
//! exception, marked and pinned at the function itself.

use anyhow::{bail, Result};
use rusqlite::Connection;

use crate::core::domain::fact;
use crate::expr::domain::cache::{to_local_fact_value, FactCache, FactValue};
use crate::expr::domain::filter::{
    compare_fact_value, detect_status_predicates, extract_comparable_keys, extract_keys, CompareOp,
    Expr, Filter, FilterResult, StatusPredicate,
};
use crate::expr::domain::key::{is_builtin_key, parse_key_with_modifiers, BuiltinKey};
use crate::expr::domain::transform::{apply_accessor, apply_modifier, ModifierCall, PathAccessor};
use crate::expr::repo::{get_fact_value, is_known_key, prefetch_facts, prefetch_status_data};

// ============================================================================
// Filter Evaluation
// ============================================================================

/// Apply a list of filters to a set of source IDs (AND logic between filters)
pub fn apply_filters(
    conn: &mut Connection,
    source_ids: &[i64],
    filters: &[Filter],
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
        if eval_expr_cached(conn, source_id, &combined, &cache)? {
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

/// Check fact comparison for built-in keys (derived from source columns)
/// This is used by the cached version for built-in key fallback
///
/// This is the one function in this stratum that queries the database itself
/// rather than going through the repository beside it, and the exception is
/// pinned by a test so it cannot spread or be forgotten. Moving it whole into
/// the repository would not help — that would put comparison logic there
/// instead, which breaks the same rule from the other side. What removes the
/// exception is deriving built-in values in one place rather than two, here
/// and in the value surface the rest of the engine reads through; there would
/// then be nothing left for this to do.
// AUDIT: queries the database from the operations stratum, accepted until
// built-in value derivation is unified.
fn check_fact_compare(
    conn: &Connection,
    source_id: i64,
    key: &str,
    op: CompareOp,
    value: &str,
) -> Result<bool> {
    // Parse key, accessor, and modifiers
    let (base_key, accessor, modifiers) = parse_key_with_modifiers(key)?;

    // Handle built-in keys via enum
    if let Some(builtin) = BuiltinKey::from_str(&base_key) {
        match builtin {
            // Text fields
            BuiltinKey::SourceExt | BuiltinKey::Ext => {
                let rel_path: String = conn.query_row(
                    "SELECT rel_path FROM sources WHERE id = ?",
                    [source_id],
                    |row| row.get(0),
                )?;
                let ext = std::path::Path::new(&rel_path)
                    .extension()
                    .and_then(|e| e.to_str())
                    .unwrap_or("");
                let fact_value = FactValue::Text(ext.to_string());
                if let Ok(modified) =
                    apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key)
                {
                    return Ok(compare_fact_value(&modified, op, value));
                }
                return Ok(false);
            }
            BuiltinKey::Filename => {
                let rel_path: String = conn.query_row(
                    "SELECT rel_path FROM sources WHERE id = ?",
                    [source_id],
                    |row| row.get(0),
                )?;
                let filename = std::path::Path::new(&rel_path)
                    .file_name()
                    .and_then(|f| f.to_str())
                    .unwrap_or(&rel_path);
                let fact_value = FactValue::Text(filename.to_string());
                if let Ok(modified) =
                    apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key)
                {
                    return Ok(compare_fact_value(&modified, op, value));
                }
                return Ok(false);
            }
            BuiltinKey::SourceRoot => {
                let root_path: String = conn.query_row(
                    "SELECT r.path FROM sources s JOIN roots r ON s.root_id = r.id WHERE s.id = ?",
                    [source_id],
                    |row| row.get(0),
                )?;
                let fact_value = FactValue::Text(root_path);
                if let Ok(modified) =
                    apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key)
                {
                    return Ok(compare_fact_value(&modified, op, value));
                }
                return Ok(false);
            }
            BuiltinKey::SourcePath => {
                let (root_path, rel_path): (String, String) = conn.query_row(
                    "SELECT r.path, s.rel_path FROM sources s JOIN roots r ON s.root_id = r.id WHERE s.id = ?",
                    [source_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?;
                let full_path = if rel_path.is_empty() {
                    root_path
                } else {
                    format!("{root_path}/{rel_path}")
                };
                let fact_value = FactValue::Text(full_path);
                if let Ok(modified) =
                    apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key)
                {
                    return Ok(compare_fact_value(&modified, op, value));
                }
                return Ok(false);
            }
            BuiltinKey::SourceRelPath => {
                let rel_path: String = conn.query_row(
                    "SELECT rel_path FROM sources WHERE id = ?",
                    [source_id],
                    |row| row.get(0),
                )?;
                let fact_value = FactValue::Text(rel_path);
                if let Ok(modified) =
                    apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key)
                {
                    return Ok(compare_fact_value(&modified, op, value));
                }
                return Ok(false);
            }

            // Numeric fields
            BuiltinKey::SourceSize | BuiltinKey::Size => {
                let v: i64 = conn.query_row(
                    "SELECT size FROM sources WHERE id = ?",
                    [source_id],
                    |row| row.get(0),
                )?;
                let fact_value = FactValue::Num(v as f64);
                if let Ok(modified) =
                    apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key)
                {
                    return Ok(compare_fact_value(&modified, op, value));
                }
                return Ok(false);
            }
            BuiltinKey::SourceMtime | BuiltinKey::Mtime => {
                let v: i64 = conn.query_row(
                    "SELECT mtime FROM sources WHERE id = ?",
                    [source_id],
                    |row| row.get(0),
                )?;
                // mtime is a time value, so use Time type for proper modifier support
                let fact_value = FactValue::Time(v);
                if let Ok(modified) =
                    apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key)
                {
                    return Ok(compare_fact_value(&modified, op, value));
                }
                return Ok(false);
            }
            BuiltinKey::SourceDevice => {
                let device: Option<i64> = conn.query_row(
                    "SELECT device FROM sources WHERE id = ?",
                    [source_id],
                    |row| row.get(0),
                )?;
                if let Some(d) = device {
                    let fact_value = FactValue::Num(d as f64);
                    if let Ok(modified) =
                        apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key)
                    {
                        return Ok(compare_fact_value(&modified, op, value));
                    }
                }
                return Ok(false);
            }
            BuiltinKey::SourceInode => {
                let inode: Option<i64> = conn.query_row(
                    "SELECT inode FROM sources WHERE id = ?",
                    [source_id],
                    |row| row.get(0),
                )?;
                if let Some(i) = inode {
                    let fact_value = FactValue::Num(i as f64);
                    if let Ok(modified) =
                        apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key)
                    {
                        return Ok(compare_fact_value(&modified, op, value));
                    }
                }
                return Ok(false);
            }
            BuiltinKey::RootId => {
                let v: i64 = conn.query_row(
                    "SELECT root_id FROM sources WHERE id = ?",
                    [source_id],
                    |row| row.get(0),
                )?;
                let fact_value = FactValue::Num(v as f64);
                if let Ok(modified) =
                    apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key)
                {
                    return Ok(compare_fact_value(&modified, op, value));
                }
                return Ok(false);
            }
            BuiltinKey::SourceId | BuiltinKey::Id => {
                // The source ID is the source_id parameter itself
                let fact_value = FactValue::Num(source_id as f64);
                if let Ok(modified) =
                    apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key)
                {
                    return Ok(compare_fact_value(&modified, op, value));
                }
                return Ok(false);
            }

            // Other builtin keys (aliases, etc.) fall through to fact lookup
            _ => {}
        }
    }

    // Get object_id for checking object facts
    let object_id: Option<i64> = conn
        .query_row(
            "SELECT object_id FROM sources WHERE id = ?",
            [source_id],
            |row| row.get(0),
        )
        .unwrap_or(None);

    // Check source facts then object facts
    // Note: if modifier fails (e.g., time modifier on text value due to bad data),
    // treat as "no match" rather than error
    if let Some(fact_value) = get_fact_value(conn, "source", source_id, &base_key)? {
        if let Ok(modified) = apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key) {
            if compare_fact_value(&modified, op, value) {
                return Ok(true);
            }
        }
    }

    if let Some(obj_id) = object_id {
        if let Some(fact_value) = get_fact_value(conn, "object", obj_id, &base_key)? {
            if let Ok(modified) =
                apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key)
            {
                if compare_fact_value(&modified, op, value) {
                    return Ok(true);
                }
            }
        }
    }

    Ok(false)
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
) -> Result<bool> {
    match expr {
        Expr::And(exprs) => {
            for e in exprs {
                if !eval_expr_cached(conn, source_id, e, cache)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        Expr::Or(exprs) => {
            for e in exprs {
                if eval_expr_cached(conn, source_id, e, cache)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Expr::Not(e) => Ok(!eval_expr_cached(conn, source_id, e, cache)?),
        Expr::Exists { key } => check_fact_exists_cached(conn, source_id, key, cache),
        Expr::Compare { key, op, value } => {
            check_fact_compare_cached(conn, source_id, key, *op, value, cache)
        }
        Expr::In { key, values } => check_fact_in_cached(conn, source_id, key, values, cache),
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

fn check_fact_exists_cached(
    _conn: &Connection,
    source_id: i64,
    key: &str,
    cache: &FactCache,
) -> Result<bool> {
    let (base_key, _accessor, _modifiers) = parse_key_with_modifiers(key)?;

    // Check cache for stored facts
    if cache.has_key(&base_key) {
        if cache.get_source_fact(source_id, &base_key).is_some() {
            return Ok(true);
        }
        if cache.get_object_fact(source_id, &base_key).is_some() {
            return Ok(true);
        }
    }

    // Check for built-in keys
    if base_key == "content.hash.sha256" {
        return Ok(cache.get_object_id(source_id).is_some());
    }
    Ok(is_builtin_key(&base_key))
}

fn check_fact_compare_cached(
    conn: &Connection,
    source_id: i64,
    key: &str,
    op: CompareOp,
    value: &str,
    cache: &FactCache,
) -> Result<bool> {
    let (base_key, accessor, modifiers) = parse_key_with_modifiers(key)?;

    // Handle built-in keys (still need DB for source columns)
    if BuiltinKey::from_str(&base_key).is_some() {
        // For built-ins, fall back to uncached version (they query source table, not facts)
        return check_fact_compare(conn, source_id, key, op, value);
    }

    // Use cache for stored facts
    if let Some(fact_value) = cache.get_source_fact(source_id, &base_key) {
        let local_value = to_local_fact_value(fact_value);
        if let Ok(modified) = apply_accessor_and_modifiers(local_value, &accessor, &modifiers, key)
        {
            if compare_fact_value(&modified, op, value) {
                return Ok(true);
            }
        }
    }

    if let Some(fact_value) = cache.get_object_fact(source_id, &base_key) {
        let local_value = to_local_fact_value(fact_value);
        if let Ok(modified) = apply_accessor_and_modifiers(local_value, &accessor, &modifiers, key)
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
) -> Result<bool> {
    for value in values {
        if check_fact_compare_cached(conn, source_id, key, CompareOp::Eq, value, cache)? {
            return Ok(true);
        }
    }
    Ok(false)
}

// ============================================================================
// Modifier and Accessor Application
// ============================================================================

/// Apply accessor and modifiers to a FactValue using the value transforms
fn apply_accessor_and_modifiers(
    value: FactValue,
    accessor: &Option<PathAccessor>,
    modifiers: &[ModifierCall],
    key: &str,
) -> Result<FactValue> {
    // Convert to fact::FactValue
    let mut expr_value = match value {
        FactValue::Text(t) => fact::FactValue::Text(t),
        FactValue::Num(n) => fact::FactValue::Num(n),
        FactValue::Time(ts) => fact::FactValue::Time(ts),
    };

    // Apply accessor if present
    if let Some(acc) = accessor {
        expr_value = apply_accessor(&expr_value, acc, key)?;
    }

    // Apply modifiers (for_display: true since filters are typically for display/comparison)
    for modifier_call in modifiers {
        expr_value = apply_modifier(&expr_value, modifier_call, key, true)?;
    }

    // Convert back to FactValue
    Ok(match expr_value {
        fact::FactValue::Text(t) => FactValue::Text(t),
        fact::FactValue::Num(n) => FactValue::Num(n),
        fact::FactValue::Time(ts) => FactValue::Time(ts),
        fact::FactValue::Path(p) => FactValue::Text(p),
    })
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::repo::open_in_memory_for_test;
    use crate::expr::domain::filter::STATUS_KEYWORDS;
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
        let result = apply_filters(&mut conn, &[s1, s2, s3], &[filter]).unwrap();

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
        let result = apply_filters(&mut conn, &[s1, s2], &[filter]).unwrap();

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
        let result = apply_filters(&mut conn, &[s1, s2], &[filter]).unwrap();

        assert_eq!(result.source_ids, vec![s2]);
    }

    #[test]
    fn filter_modifier_failure_on_builtin_is_non_match() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos");

        // source.ext is a text value; |year modifier should fail on it
        let s1 = insert_source(&conn, root, "photo.jpg");

        let filter = Filter::parse("source.ext|year=2024").unwrap();
        let result = apply_filters(&mut conn, &[s1], &[filter]).unwrap();

        // Modifier failure treated as non-match, not error
        assert!(result.source_ids.is_empty());
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
        let result = apply_filters(&mut conn, &[s1], &[filter]).unwrap();
        assert_eq!(result.source_ids, vec![s1]);
    }

    #[test]
    fn filter_archived_excludes_unhashed() {
        let mut conn = setup_test_db();
        let src_root = insert_root(&conn, "/src");
        // Unhashed source (no object_id)
        let s1 = insert_source_with_object(&conn, src_root, "a.jpg", None);

        let filter = Filter::parse("archived?").unwrap();
        let result = apply_filters(&mut conn, &[s1], &[filter]).unwrap();
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
        let result = apply_filters(&mut conn, &[s1], &[filter]).unwrap();
        assert!(result.source_ids.is_empty());
    }

    #[test]
    fn filter_not_archived_includes_unhashed() {
        let mut conn = setup_test_db();
        let src_root = insert_root(&conn, "/src");
        let s1 = insert_source_with_object(&conn, src_root, "a.jpg", None);

        let filter = Filter::parse("NOT archived?").unwrap();
        let result = apply_filters(&mut conn, &[s1], &[filter]).unwrap();
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
        let result = apply_filters(&mut conn, &[s1, s2], &[filter]).unwrap();
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
        let r1 = apply_filters(&mut conn, &ids, &[f1]).unwrap();
        let r2 = apply_filters(&mut conn, &ids, &[f2]).unwrap();
        assert_eq!(r1.source_ids, r2.source_ids);
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
        let result = apply_filters(&mut conn, &[s1], &[filter]).unwrap();
        assert_eq!(result.source_ids, vec![s1]);
    }

    #[test]
    fn filter_excluded_object_level() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/src");
        let obj = insert_object_excluded(&conn, "hash_excl");
        let s1 = insert_source_with_object(&conn, root, "a.jpg", Some(obj));

        let filter = Filter::parse("excluded?").unwrap();
        let result = apply_filters(&mut conn, &[s1], &[filter]).unwrap();
        assert_eq!(result.source_ids, vec![s1]);
    }

    #[test]
    fn filter_excluded_non_excluded_fails() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/src");
        let s1 = insert_source_with_object(&conn, root, "a.jpg", None);

        let filter = Filter::parse("excluded?").unwrap();
        let result = apply_filters(&mut conn, &[s1], &[filter]).unwrap();
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
        let result = apply_filters(&mut conn, &[s1], &[filter]).unwrap();
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
        let result = apply_filters(&mut conn, &[s1], &[filter]).unwrap();
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
        let result = apply_filters(&mut conn, &[s1], &[filter]).unwrap();
        assert!(result.source_ids.is_empty());
    }

    #[test]
    fn filter_enriched_no_facts_fails() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/src");
        let s1 = insert_source_with_object(&conn, root, "a.jpg", None);

        let filter = Filter::parse("enriched?").unwrap();
        let result = apply_filters(&mut conn, &[s1], &[filter]).unwrap();
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
        let result = apply_filters(&mut conn, &[s1, s2, s3], &[filter]).unwrap();
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
        let result = apply_filters(&mut conn, &[s1], &[filter]).unwrap();
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
        let result = apply_filters(&mut conn, &[s1], &[filter]).unwrap();
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
        let result = apply_filters(&mut conn, &[s1, s2], &[filter]).unwrap();

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
        let result = apply_filters(&mut conn, &[s1], &[filter]).unwrap();

        assert!(result.source_ids.is_empty());
    }

    /// Compare with unknown key should still error (typo protection).
    #[test]
    fn compare_unknown_key_still_errors() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos");
        let s1 = insert_source(&conn, root, "a.jpg");

        let filter = Filter::parse("content.mime=image/jpeg").unwrap();
        let result = apply_filters(&mut conn, &[s1], &[filter]);

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
            apply_filters(&mut conn, &[s1], &[filter])
                .unwrap_or_else(|e| panic!("{keyword}? failed: {e}"));

            let negated = Filter::parse(&format!("NOT {keyword}?")).unwrap();
            apply_filters(&mut conn, &[s1], &[negated])
                .unwrap_or_else(|e| panic!("NOT {keyword}? failed: {e}"));
        }
    }
}
