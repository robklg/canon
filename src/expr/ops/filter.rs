//! Filtering, applied: the `--where` language run against a real database.
//!
//! `apply_filters` is the whole of it from outside — hand it source ids and
//! bound filters, get back the ids that match. Inside, the work runs against
//! a prefetched cache rather than a query per source, with a per-source
//! read for the keys that are derived from source columns instead of stored
//! as facts. Those values are read through the resolver that owns them
//! (`source_row_value`), never derived again here; the source row they
//! are read from comes from the repository stratum beside this one. A stored
//! key is compared on the source's one value, read through the cache.
//!
//! Every typed value was read when the filter was bound, so a comparison here is typed on
//! both sides: text by code point, numbers with a rounding tolerance, times through the
//! whole-period table on the user's clock, parts and bucket labels as read. A source with no
//! value fails every comparison, and `NOT` is everything else.

use anyhow::{anyhow, Result};
use rusqlite::Connection;

use crate::core::domain::fact::FactValue;
use crate::core::domain::time::value::{ClockSpan, TimeForm};
use crate::core::domain::time::zone::UserZone;
use crate::expr::domain::bind::{check_read_as, Bound, BoundKey, ReadAs, Refused, Test};
use crate::expr::domain::cache::FactCache;
use crate::expr::domain::filter::{
    compare_numbers, compare_text, period_op, CompareOp, FilterResult, StatusPredicate,
};
use crate::expr::domain::glob::Glob;
use crate::expr::domain::key::{Key, SourceFact};
use crate::expr::domain::transform::{apply_chain, InstantRecord, Modified};
use crate::expr::domain::value::source_row_value;
use crate::expr::ops::bind::BoundFilters;
use crate::expr::repo::{fetch_source_row, prefetch_facts, prefetch_status_data, SourceRow};

// ============================================================================
// Filter Evaluation
// ============================================================================

/// Apply bound filters to a set of source IDs (AND between them). Every time is read on
/// `zone`, the user's clock; each typed period is translated to it once, here.
pub fn apply_filters(
    conn: &mut Connection,
    source_ids: &[i64],
    filters: &BoundFilters,
    zone: &UserZone,
) -> Result<FilterResult> {
    let used_status = filters.used_status.clone();

    if filters.trees.is_empty() {
        return Ok(FilterResult {
            source_ids: source_ids.to_vec(),
            used_status,
            read_an_instant: false,
        });
    }

    let mut all_keys = Vec::new();
    for tree in &filters.trees {
        tree.keys(&mut all_keys);
    }
    let mut cache = prefetch_facts(conn, source_ids, &all_keys)?;
    prefetch_status_data(conn, source_ids, &used_status, &mut cache)?;

    let local: Vec<Local> = filters.trees.iter().map(|t| to_local(t, zone)).collect();
    let mut typed = Vec::new();
    for tree in &filters.trees {
        tree.tests(&mut typed);
    }
    let offset_typed = typed
        .iter()
        .any(|test| matches!(test, Test::Period(tv) if tv.form() != TimeForm::Clock));

    let mut result = Vec::new();
    for &source_id in source_ids {
        let mut selected = true;
        for tree in &local {
            if !eval(conn, source_id, tree, &cache, zone)? {
                selected = false;
                break;
            }
        }
        if selected {
            result.push(source_id);
        }
    }
    Ok(FilterResult {
        source_ids: result,
        used_status,
        read_an_instant: offset_typed || cache.instant_read.read(),
    })
}

/// A bound tree with its typed periods translated to the user's local time, once per run.
enum Local<'a> {
    And(Vec<Local<'a>>),
    Or(Vec<Local<'a>>),
    Not(Box<Local<'a>>),
    Exists(&'a BoundKey),
    Compare(&'a BoundKey, CompareOp, Value<'a>),
    In(&'a BoundKey, Vec<Value<'a>>),
    Glob(&'a BoundKey, &'a Glob),
    Status(StatusPredicate),
}

/// A typed value, ready to compare.
enum Value<'a> {
    Text(&'a str),
    Number(f64),
    Span(ClockSpan),
    Part(i64),
    Bucket(&'a str),
}

fn to_local<'a>(tree: &'a Bound, zone: &UserZone) -> Local<'a> {
    let value = |test: &'a Test| match test {
        Test::Text(t) => Value::Text(t),
        Test::Number(n) => Value::Number(*n),
        Test::Period(tv) | Test::TimeOfDay(tv) => Value::Span(tv.on_clock(zone)),
        Test::Part(n) => Value::Part(*n),
        Test::Bucket(label) => Value::Bucket(label),
    };
    match tree {
        Bound::And(all) => Local::And(all.iter().map(|t| to_local(t, zone)).collect()),
        Bound::Or(all) => Local::Or(all.iter().map(|t| to_local(t, zone)).collect()),
        Bound::Not(inner) => Local::Not(Box::new(to_local(inner, zone))),
        Bound::Exists { key } => Local::Exists(key),
        Bound::Compare { key, op, test } => Local::Compare(key, *op, value(test)),
        Bound::In { key, tests } => Local::In(key, tests.iter().map(value).collect()),
        Bound::Glob { key, glob } => Local::Glob(key, glob),
        Bound::Status(predicate) => Local::Status(*predicate),
    }
}

/// Evaluate a tree in local time for one source.
fn eval(
    conn: &Connection,
    source_id: i64,
    tree: &Local,
    cache: &FactCache,
    zone: &UserZone,
) -> Result<bool> {
    match tree {
        Local::And(all) => {
            for t in all {
                if !eval(conn, source_id, t, cache, zone)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        Local::Or(all) => {
            for t in all {
                if eval(conn, source_id, t, cache, zone)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Local::Not(inner) => Ok(!eval(conn, source_id, inner, cache, zone)?),
        Local::Exists(key) => check_exists(conn, source_id, key, cache, zone),
        Local::Compare(key, op, value) => {
            let read = read_value(conn, source_id, key, cache, zone)?;
            match read {
                Some(v) => compare(&v, *op, value, zone),
                None => Ok(false),
            }
        }
        Local::In(key, values) => {
            let Some(v) = read_value(conn, source_id, key, cache, zone)? else {
                return Ok(false);
            };
            for value in values {
                if compare(&v, CompareOp::Eq, value, zone)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Local::Glob(key, glob) => match read_value(conn, source_id, key, cache, zone)? {
            Some(FactValue::Text(s) | FactValue::Path(s)) => Ok(glob.matches(&s)),
            Some(value) => Err(not_bound_for(&value)),
            None => Ok(false),
        },
        Local::Status(predicate) => Ok(match predicate {
            StatusPredicate::Hashed => cache.get_object_id(source_id).is_some(),
            StatusPredicate::Archived => {
                let archived_set = cache.archived_objects.as_ref().unwrap();
                cache
                    .get_object_id(source_id)
                    .is_some_and(|oid| archived_set.contains(&oid))
            }
            StatusPredicate::Excluded => {
                let excluded_set = cache.excluded_sources.as_ref().unwrap();
                excluded_set.contains(&source_id)
            }
            StatusPredicate::Enriched => {
                let enriched_set = cache.enriched_sources.as_ref().unwrap();
                enriched_set.contains(&source_id)
            }
        }),
    }
}

/// Compare a value the key's chain gave with a typed value. Binding matched the two types,
/// so any other pairing is a defect, raised rather than read as false.
fn compare(value: &FactValue, op: CompareOp, typed: &Value, zone: &UserZone) -> Result<bool> {
    Ok(match (typed, value) {
        (Value::Text(t), FactValue::Text(s) | FactValue::Path(s)) => compare_text(s, op, t),
        (Value::Number(n), FactValue::Num(v)) => compare_numbers(*v, op, *n),
        (Value::Part(n), FactValue::Num(v)) => compare_numbers(*v, op, *n as f64),
        (Value::Bucket(label), FactValue::Text(s)) => s == label,
        (Value::Span(period), FactValue::Time(tv)) => {
            tv.on_clock(zone).satisfies(period_op(op), period)
        }
        _ => return Err(not_bound_for(value)),
    })
}

/// The defect of a value meeting a comparison or glob that binding did not bind for its type:
/// raised, never read as false.
fn not_bound_for(value: &FactValue) -> anyhow::Error {
    anyhow!(
        "a bound comparison met a {} value it was not bound for",
        value.kind()
    )
}

// ============================================================================
// Reading a key's value for one source
// ============================================================================

/// The value a comparison reads for one source: the key's value through its accessor and
/// modifiers, or `None` where there is none — no fact, a built-in key the row cannot answer, a
/// modifier that gave no value. A stored value of another type than the key was bound for
/// stops the run: the key's type changed while the filter ran.
fn read_value(
    conn: &Connection,
    source_id: i64,
    key: &BoundKey,
    cache: &FactCache,
    zone: &UserZone,
) -> Result<Option<FactValue>> {
    let Some(base) = base_value(conn, source_id, key, cache)? else {
        return Ok(None);
    };
    through_chain(base, key, zone, &cache.instant_read)
}

/// The key's base value for one source: a key the source row computes, through its
/// resolver; any other key (the content hash included) the source's one value from the cache,
/// checked against the type the key was bound for.
fn base_value(
    conn: &Connection,
    source_id: i64,
    key: &BoundKey,
    cache: &FactCache,
) -> Result<Option<FactValue>> {
    let base = &key.base;
    if let Some(computed) = Key::read(base).filter(Key::is_computed) {
        return Ok(fetch_source_row(conn, source_id)?.and_then(|row| row_value(&row, &computed)));
    }
    let Some(value) = cache.value(source_id, base) else {
        return Ok(None);
    };
    check_read_as(key.read_as, base, value, Refused::Filter)?;
    Ok(Some(value.clone()))
}

/// A value through the key's accessor and modifiers. No value is `None`; any other failure
/// is a defect binding should have refused, raised as one.
fn through_chain(
    value: FactValue,
    key: &BoundKey,
    zone: &UserZone,
    record: &InstantRecord,
) -> Result<Option<FactValue>> {
    let (accessor, modifiers) = (key.accessor.as_ref(), &key.modifiers);
    let chained = apply_chain(value, accessor, modifiers, &key.input, zone)?;
    record.note(chained.read_an_instant);
    Ok(match chained.value {
        Modified::Value(v) => Some(v),
        Modified::NoValue(_) => None,
    })
}

/// A computed key's value for one source row, through the resolver that owns it — `None`
/// where the row cannot answer it: a NULL device or inode, or a save time outside the years
/// Canon reads. Never a stored row of its name.
fn row_value(row: &SourceRow, key: &Key) -> Option<FactValue> {
    if (*key == Key::SourceFact(SourceFact::Device) && !row.has_device)
        || (*key == Key::SourceFact(SourceFact::Inode) && !row.has_inode)
    {
        return None;
    }
    source_row_value(&row.attributes, key)
}

/// Whether a key exists for a source: exactly where it has a value, so `NOT K?` selects the
/// sources a report shows under `(no value)` for `K`. Read through the key's accessor and
/// modifiers — `K|month?` is false for a time known only to the year, and `K|year?` for text
/// that is not a time. A bare `K?` reads no value, so it reads no clock and checks
/// no type. Every key alike — the content hash, which the hash keys are read as, included —
/// exists where it has a value; a computed key where its resolver answers (`?` reads a NULL
/// device or inode as 0, as `facts` reports it).
fn check_exists(
    conn: &Connection,
    source_id: i64,
    key: &BoundKey,
    cache: &FactCache,
    zone: &UserZone,
) -> Result<bool> {
    let base = &key.base;
    let value = match Key::read(base) {
        Some(computed) if computed.is_computed() => fetch_source_row(conn, source_id)?
            .and_then(|row| source_row_value(&row.attributes, &computed)),
        _ => match key.read_as {
            ReadAs::Any => cache.value(source_id, base).cloned(),
            ReadAs::Absent | ReadAs::One(_) => base_value(conn, source_id, key, cache)?,
        },
    };
    let Some(value) = value else {
        return Ok(false);
    };
    if !key.has_transforms() {
        return Ok(true);
    }
    Ok(through_chain(value, key, zone, &cache.instant_read)?.is_some())
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::domain::fact::FactType;
    use crate::core::domain::number::spelled;
    use crate::core::repo::open_in_memory_for_test;
    use crate::core::testing::insert_time_fact;
    use crate::expr::domain::filter::STATUS_KEYWORDS;
    use crate::expr::domain::key::CONTENT_HASH;
    use crate::expr::domain::print::printed_value;
    use crate::expr::domain::read::{AliasMode, Grammar};
    use crate::expr::domain::value::SourceAttributes;
    use crate::expr::ops::bind::bound;
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

        let filter = bound(&conn, "source.rel_path[1]~*tion*");
        let result = apply_filters(&mut conn, &[s1, s2, s3], &filter, &UserZone::utc()).unwrap();

        // s1 silently skipped (out of bounds), s2 matches, s3 doesn't match glob
        assert_eq!(result.source_ids, vec![s2]);
    }

    /// Quoted text in a glob is literal: a quoted glob matches only the name it spells, and
    /// `!~` of it selects everything else.
    #[test]
    fn a_quoted_glob_matches_its_text_exactly() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos");
        let literal = insert_source(&conn, root, "IMG_*");
        let photo = insert_source(&conn, root, "IMG_0001.jpg");
        let bracketed = insert_source(&conn, root, "Holiday [2024].jpg");
        let all = [literal, photo, bracketed];
        for (text, expected) in [
            (r#"filename~"IMG_*""#, vec![literal]),
            (r#"filename!~"IMG_*""#, vec![photo, bracketed]),
            ("filename~IMG_*", vec![literal, photo]),
            (r#"filename~*"[2024]"*"#, vec![bracketed]),
        ] {
            let filter = bound(&conn, text);
            let result = apply_filters(&mut conn, &all, &filter, &UserZone::utc()).unwrap();
            assert_eq!(result.source_ids, expected, "{text}");
        }
    }

    #[test]
    fn filter_out_of_bounds_negative_index_is_non_match() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos");

        // 1-segment path: [-3] is out of bounds
        let s1 = insert_source(&conn, root, "image.jpg");
        // 3-segment path: [-3] = "2024"
        let s2 = insert_source(&conn, root, "2024/vacation/photo.jpg");

        let filter = bound(&conn, "source.rel_path[-3]=\"2024\"");
        let result = apply_filters(&mut conn, &[s1, s2], &filter, &UserZone::utc()).unwrap();

        assert_eq!(result.source_ids, vec![s2]);
    }

    /// A slice past the end is the path with no segments, which is a value: `?` is true for
    /// it, `=""` selects it, and a glob that needs a segment does not match it.
    #[test]
    fn filter_an_out_of_bounds_slice_is_a_path_with_no_segments() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos");
        let s1 = insert_source(&conn, root, "image.jpg");
        let s2 = insert_source(&conn, root, "a/b/c/d");
        let run = |conn: &mut RawConnection, filter: &str| {
            let filter = bound(conn, filter);
            apply_filters(conn, &[s1, s2], &filter, &UserZone::utc())
                .unwrap()
                .source_ids
        };
        assert_eq!(run(&mut conn, "source.rel_path[2:4]~c*"), vec![s2]);
        assert_eq!(run(&mut conn, "source.rel_path[2:4]?"), vec![s1, s2]);
        assert_eq!(run(&mut conn, r#"source.rel_path[2:4]="""#), vec![s1]);
        assert_eq!(run(&mut conn, r#"source.rel_path[:-1]="""#), vec![s1]);
        assert_eq!(run(&mut conn, "source.rel_path[:-1]?"), vec![s1, s2]);
    }

    /// **A file with no extension has no value for it**: `NOT source.ext?` selects such files,
    /// `source.ext=""` selects nothing, and `source.ext!=""` includes them, as `!=` is `NOT =`.
    #[test]
    fn a_file_with_no_extension_is_selected_by_not_ext_exists() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos");
        let none: Vec<i64> = ["README", "dir/.bashrc", "trail."]
            .iter()
            .map(|rel| insert_source(&conn, root, rel))
            .collect();
        let jpg = insert_source(&conn, root, "a.jpg");
        let all: Vec<i64> = none.iter().copied().chain([jpg]).collect();
        let run = |conn: &mut RawConnection, filter: &str| {
            let filter = bound(conn, filter);
            apply_filters(conn, &all, &filter, &UserZone::utc())
                .unwrap()
                .source_ids
        };
        assert_eq!(run(&mut conn, "NOT source.ext?"), none);
        assert_eq!(run(&mut conn, "NOT ext?"), none);
        assert_eq!(run(&mut conn, "NOT filename|ext?"), none);
        assert_eq!(run(&mut conn, r#"source.ext="""#), Vec::<i64>::new());
        assert_eq!(run(&mut conn, r#"source.ext!="""#), all);
        assert_eq!(run(&mut conn, "source.ext?"), vec![jpg]);
    }

    /// **A stored empty text stays a value**: `Make=""` selects it and `Make?` is true for it,
    /// through a text modifier too; `|ext`, a number or a time read from it has no value.
    #[test]
    fn an_empty_stored_text_stays_a_value() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos");
        let empty = insert_source(&conn, root, "a.jpg");
        let other = insert_source(&conn, root, "b.jpg");
        insert_fact_entry(&conn, "source", empty, "content.Make", "");
        insert_fact_entry(&conn, "source", other, "content.Make", "Canon");
        let run = |conn: &mut RawConnection, filter: &str| {
            let filter = bound(conn, filter);
            apply_filters(conn, &[empty, other], &filter, &UserZone::utc())
                .unwrap()
                .source_ids
        };
        assert_eq!(run(&mut conn, r#"Make="""#), vec![empty]);
        assert_eq!(run(&mut conn, "Make?"), vec![empty, other]);
        assert_eq!(run(&mut conn, r#"Make|lowercase="""#), vec![empty]);
        assert_eq!(run(&mut conn, "Make|short?"), vec![empty, other]);
        assert_eq!(run(&mut conn, "Make|ext?"), Vec::<i64>::new());
        assert_eq!(run(&mut conn, "Make|number?"), Vec::<i64>::new());
        assert_eq!(run(&mut conn, "Make|year?"), Vec::<i64>::new());
    }

    #[test]
    fn filter_modifier_failure_on_a_source_fact_is_non_match() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos");

        // source.ext is a text value; |year modifier should fail on it
        let s1 = insert_source(&conn, root, "photo.jpg");

        let filter = bound(&conn, "source.ext|year=2024");
        let result = apply_filters(&mut conn, &[s1], &filter, &UserZone::utc()).unwrap();

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
            let filter = bound(conn, expr);
            apply_filters(conn, &all, &filter, &UserZone::utc())
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

        let filter = bound(&conn, "archived?");
        let result = apply_filters(&mut conn, &[s1], &filter, &UserZone::utc()).unwrap();
        assert_eq!(result.source_ids, vec![s1]);
    }

    #[test]
    fn filter_archived_excludes_unhashed() {
        let mut conn = setup_test_db();
        let src_root = insert_root(&conn, "/src");
        // Unhashed source (no object_id)
        let s1 = insert_source_with_object(&conn, src_root, "a.jpg", None);

        let filter = bound(&conn, "archived?");
        let result = apply_filters(&mut conn, &[s1], &filter, &UserZone::utc()).unwrap();
        assert!(result.source_ids.is_empty());
    }

    #[test]
    fn filter_archived_excludes_unarchived_hashed() {
        let mut conn = setup_test_db();
        let src_root = insert_root(&conn, "/src");
        let obj = insert_object(&conn, "hash_b");
        // Hashed but not in any archive
        let s1 = insert_source_with_object(&conn, src_root, "a.jpg", Some(obj));

        let filter = bound(&conn, "archived?");
        let result = apply_filters(&mut conn, &[s1], &filter, &UserZone::utc()).unwrap();
        assert!(result.source_ids.is_empty());
    }

    #[test]
    fn filter_not_archived_includes_unhashed() {
        let mut conn = setup_test_db();
        let src_root = insert_root(&conn, "/src");
        let s1 = insert_source_with_object(&conn, src_root, "a.jpg", None);

        let filter = bound(&conn, "NOT archived?");
        let result = apply_filters(&mut conn, &[s1], &filter, &UserZone::utc()).unwrap();
        assert_eq!(result.source_ids, vec![s1]);
    }

    #[test]
    fn filter_hashed_matches() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/src");
        let obj = insert_object(&conn, "hash_c");
        let s1 = insert_source_with_object(&conn, root, "a.jpg", Some(obj));
        let s2 = insert_source_with_object(&conn, root, "b.jpg", None);

        let filter = bound(&conn, "hashed?");
        let result = apply_filters(&mut conn, &[s1, s2], &filter, &UserZone::utc()).unwrap();
        assert_eq!(result.source_ids, vec![s1]);
    }

    /// `hashed?` answers whether a source is linked to content; `content.hash.sha256?` whether
    /// the content hash has a value — the same where Canon stored the hash it computed, apart
    /// where a source is linked to content with no stored hash.
    #[test]
    fn filter_hashed_and_a_stored_hash_answer_their_own_questions() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/src");
        let obj = insert_object(&conn, "hash_d");
        let s1 = insert_source_with_object(&conn, root, "a.jpg", Some(obj));
        let s2 = insert_source_with_object(&conn, root, "b.jpg", None);
        let ids = [s1, s2];
        let run = |conn: &mut RawConnection, clause: &str| {
            let filter = bound(conn, clause);
            apply_filters(conn, &ids, &filter, &UserZone::utc())
                .unwrap()
                .source_ids
        };
        assert_eq!(run(&mut conn, "hashed?"), vec![s1]);
        assert_eq!(run(&mut conn, "content.hash.sha256?"), Vec::<i64>::new());
        insert_fact_entry(&conn, "object", obj, CONTENT_HASH, "hash_d");
        assert_eq!(
            run(&mut conn, "content.hash.sha256?"),
            run(&mut conn, "hashed?")
        );
    }

    #[test]
    fn ext_in_a_filter_keeps_its_case_and_lowercase_folds_it() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/src");
        let upper = insert_source(&conn, root, "2024/IMG_001.JPG");
        let lower = insert_source(&conn, root, "2024/b.jpg");
        let ids = [upper, lower];

        let run = |conn: &mut RawConnection, where_clause: &str| {
            let filter = bound(conn, where_clause);
            apply_filters(conn, &ids, &filter, &UserZone::utc())
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
        // Two sources that differ in every attribute a computed key reads, so an
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
        let readme = insert(root_a, "README", 10, 1704067200, 9, 13);
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

        for key in Key::all().filter(Key::is_computed) {
            let name = key.name();
            let expected = source_row_value(&attributes, &key)
                .unwrap_or_else(|| panic!("{name} is computed but the resolver has no answer"));
            // The resolver's value as Canon prints it, which reads back as itself.
            let text = match &expected {
                FactValue::Text(t) | FactValue::Path(t) => printed_value(t, expected.kind()),
                FactValue::Num(n) => printed_value(&spelled(*n), FactType::Num),
                FactValue::Time(t) => printed_value(&t.printed(&UserZone::utc()), FactType::Time),
            };

            for (op, want) in [("=", subject), ("!=", other)] {
                let filter = bound(&conn, &format!("{name}{op}{text}"));
                let matched =
                    apply_filters(&mut conn, &[subject, other], &filter, &UserZone::utc())
                        .unwrap_or_else(|e| panic!("{name}{op}{text} failed: {e}"))
                        .source_ids;
                assert_eq!(
                    matched,
                    vec![want],
                    "{name}{op}{text} must answer as the resolver's own value does"
                );
            }
        }

        // A source a computed key has no value for — a name with no extension — is answered
        // as the resolver answers it: `?` false, `NOT …?` true.
        let readme_attributes = SourceAttributes {
            id: readme,
            rel_path: "README".to_string(),
            size: 10,
            device: 9,
            inode: 13,
            ..attributes
        };
        let mut without = 0;
        for key in Key::all().filter(Key::is_computed) {
            let name = key.name();
            let has = source_row_value(&readme_attributes, &key).is_some();
            without += usize::from(!has);
            let filter = bound(&conn, &format!("{name}?"));
            let matched = apply_filters(&mut conn, &[readme], &filter, &UserZone::utc())
                .unwrap()
                .source_ids;
            assert_eq!(
                matched == vec![readme],
                has,
                "{name}? must answer as the resolver does"
            );
        }
        assert!(
            without >= 2,
            "the extension, by both its names, has no value for README"
        );
    }

    /// The other half of the keeper above: a built-in key the source row cannot
    /// answer reads the stored content hash, so every built-in key answers in
    /// `--where`, and so does the content hash itself. The claim table is
    /// written out rather than derived from `BuiltinKey::reading`, and it must
    /// name every key of the vocabulary the source row does not compute.
    ///
    /// The defect it names: `hash` and `hash_short` reached the stored lookup
    /// under their own names, where no fact is ever kept — every comparison
    /// false, and `NOT` selecting every source.
    #[test]
    fn a_built_in_the_source_row_cannot_answer_reads_the_content_hash() {
        use crate::expr::domain::key::BuiltinKey;

        let full = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";
        let claims = [
            (Key::BuiltinKey(BuiltinKey::Hash), full),
            (Key::BuiltinKey(BuiltinKey::HashShort), "2cf24dba"),
            (Key::ContentFact(CONTENT_HASH.to_string()), full),
        ];
        let not_computed = Key::all()
            .chain([Key::ContentFact(CONTENT_HASH.to_string())])
            .filter(|k| !k.is_computed());
        for key in not_computed {
            assert!(
                claims.iter().any(|(k, _)| *k == key),
                "{} has no claim in this table",
                key.name()
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

        for (key, value) in claims {
            let name = key.name();
            for (clause, want) in [
                (format!("{name}=\"{value}\""), vec![subject]),
                (format!("NOT {name}=\"{value}\""), vec![other]),
                (format!("{name} IN (\"{value}\")"), vec![subject]),
            ] {
                let filter = bound(&conn, &clause);
                let matched =
                    apply_filters(&mut conn, &[subject, other], &filter, &UserZone::utc())
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

        for clause in ["source.device=0", "source.inode=0"] {
            let filter = bound(&conn, clause);
            let result = apply_filters(&mut conn, &[s1], &filter, &UserZone::utc()).unwrap();
            assert!(
                result.source_ids.is_empty(),
                "{clause} must not select a NULL column"
            );
        }
        // `!=` is NOT of `=`: a column with no value is everything else, so it is selected.
        for clause in ["source.device!=5", "source.inode!=5"] {
            let filter = bound(&conn, clause);
            let result = apply_filters(&mut conn, &[s1], &filter, &UserZone::utc()).unwrap();
            assert_eq!(result.source_ids, vec![s1], "{clause}");
        }
        let filter = bound(&conn, "source.size=1000");
        let result = apply_filters(&mut conn, &[s1], &filter, &UserZone::utc()).unwrap();
        assert_eq!(result.source_ids, vec![s1]);
    }

    /// A built-in key exists exactly where it has a value: a computed one where its resolver
    /// answers, the hash keys where the content hash is stored.
    #[test]
    fn a_built_in_exists_exactly_where_it_has_a_value() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/src");
        let stored = insert_object(&conn, "hash_stored");
        let bare = insert_object(&conn, "hash_bare");
        let with_hash = insert_source_with_object(&conn, root, "a.jpg", Some(stored));
        let linked = insert_source_with_object(&conn, root, "b.jpg", Some(bare));
        let unhashed = insert_source_with_object(&conn, root, "c.jpg", None);
        insert_fact_entry(&conn, "object", stored, CONTENT_HASH, "abc");
        let ids = [with_hash, linked, unhashed];

        for key in Key::all().chain([Key::ContentFact(CONTENT_HASH.to_string())]) {
            let name = key.name();
            let filter = bound(&conn, &format!("{name}?"));
            let matched = apply_filters(&mut conn, &ids, &filter, &UserZone::utc())
                .unwrap()
                .source_ids;
            let expected = if key.is_computed() {
                vec![with_hash, linked, unhashed]
            } else {
                vec![with_hash]
            };
            assert_eq!(matched, expected, "{name}? disagrees with the resolver");
        }
    }

    /// **`?` means "has a value" for the hash keys too**: true only where the content hash is
    /// stored, read through the key's modifiers like any key's — never for a source linked to
    /// content with no stored hash.
    #[test]
    fn hash_exists_only_where_there_is_a_value() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/src");
        let stored = insert_object(&conn, "hash_stored");
        let bare = insert_object(&conn, "hash_bare");
        let with_hash = insert_source_with_object(&conn, root, "a.jpg", Some(stored));
        let linked = insert_source_with_object(&conn, root, "b.jpg", Some(bare));
        insert_fact_entry(&conn, "object", stored, CONTENT_HASH, "2cf24dba5fb0");
        let ids = [with_hash, linked];
        let run = |conn: &mut RawConnection, clause: &str| {
            let filter = bound(conn, clause);
            apply_filters(conn, &ids, &filter, &UserZone::utc())
                .unwrap()
                .source_ids
        };
        for clause in [
            "hash?",
            "hash_short?",
            "content.hash.sha256?",
            "hash|uppercase?",
            "hash_short|short?",
        ] {
            assert_eq!(run(&mut conn, clause), vec![with_hash], "{clause}");
            assert_eq!(
                run(&mut conn, &format!("NOT {clause}")),
                vec![linked],
                "{clause}"
            );
        }
        // Its modifiers are read: a hash that is no number has no number.
        assert_eq!(run(&mut conn, "hash|number?"), Vec::<i64>::new());
        assert_eq!(run(&mut conn, "hashed?"), vec![with_hash, linked]);
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

        let filter = bound(&conn, "excluded?");
        let result = apply_filters(&mut conn, &[s1], &filter, &UserZone::utc()).unwrap();
        assert_eq!(result.source_ids, vec![s1]);
    }

    #[test]
    fn filter_excluded_object_level() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/src");
        let obj = insert_object_excluded(&conn, "hash_excl");
        let s1 = insert_source_with_object(&conn, root, "a.jpg", Some(obj));

        let filter = bound(&conn, "excluded?");
        let result = apply_filters(&mut conn, &[s1], &filter, &UserZone::utc()).unwrap();
        assert_eq!(result.source_ids, vec![s1]);
    }

    #[test]
    fn filter_excluded_non_excluded_fails() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/src");
        let s1 = insert_source_with_object(&conn, root, "a.jpg", None);

        let filter = bound(&conn, "excluded?");
        let result = apply_filters(&mut conn, &[s1], &filter, &UserZone::utc()).unwrap();
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

        let filter = bound(&conn, "enriched?");
        let result = apply_filters(&mut conn, &[s1], &filter, &UserZone::utc()).unwrap();
        assert_eq!(result.source_ids, vec![s1]);
    }

    #[test]
    fn filter_enriched_with_facts_on_the_source() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/src");
        // Unhashed source with source-level fact
        let s1 = insert_source_with_object(&conn, root, "a.jpg", None);
        insert_fact_entry(&conn, "source", s1, "policy.tag", "keep");

        let filter = bound(&conn, "enriched?");
        let result = apply_filters(&mut conn, &[s1], &filter, &UserZone::utc()).unwrap();
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

        let filter = bound(&conn, "enriched?");
        let result = apply_filters(&mut conn, &[s1], &filter, &UserZone::utc()).unwrap();
        assert!(result.source_ids.is_empty());
    }

    #[test]
    fn filter_enriched_no_facts_fails() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/src");
        let s1 = insert_source_with_object(&conn, root, "a.jpg", None);

        let filter = bound(&conn, "enriched?");
        let result = apply_filters(&mut conn, &[s1], &filter, &UserZone::utc()).unwrap();
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

        let filter = bound(&conn, "NOT archived? AND hashed?");
        let result = apply_filters(&mut conn, &[s1, s2, s3], &filter, &UserZone::utc()).unwrap();
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

        let filter = bound(&conn, "archived?");
        let result = apply_filters(&mut conn, &[s1], &filter, &UserZone::utc()).unwrap();
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

        let filter = bound(&conn, "source.ext=jpg");
        let result = apply_filters(&mut conn, &[s1], &filter, &UserZone::utc()).unwrap();
        assert!(!result.used_status.archived);
        assert!(!result.used_status.hashed);
        assert!(!result.used_status.excluded);
        assert!(!result.used_status.enriched);
    }

    #[test]
    fn filter_result_flags_nested_detection() {
        let conn = setup_test_db();
        let filter = bound(&conn, "NOT (archived? AND excluded?)");
        let used = filter.used_status.clone();
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
        let filter = bound(&conn, "NOT content.mime?");
        let result = apply_filters(&mut conn, &[s1, s2], &filter, &UserZone::utc()).unwrap();

        // All sources should match (content.mime doesn't exist for any)
        assert_eq!(result.source_ids.len(), 2);
    }

    /// Existence check with unknown key (positive) should match nothing.
    #[test]
    fn exists_unknown_key_positive_matches_nothing() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos");
        let s1 = insert_source(&conn, root, "a.jpg");

        let filter = bound(&conn, "content.mime?");
        let result = apply_filters(&mut conn, &[s1], &filter, &UserZone::utc()).unwrap();

        assert!(result.source_ids.is_empty());
    }

    /// Compare with unknown key should still error (typo protection).
    #[test]
    fn compare_unknown_key_still_errors() {
        let conn = setup_test_db();
        let root = insert_root(&conn, "/photos");
        insert_source(&conn, root, "a.jpg");

        // Refused when the filter is read, before any source is looked at.
        let err = crate::expr::ops::bind::read_filters(
            &conn,
            &crate::expr::ops::bind::ExpressionText::plain(&["content.mime=image/jpeg"]),
        )
        .expect_err("an unknown key in a comparison is refused");
        assert!(
            err.to_string()
                .contains("^ `content.mime` is an unknown key\n    `canon facts` lists the keys"),
            "{err}"
        );
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

            let filter = bound(&conn, &format!("{keyword}?"));
            apply_filters(&mut conn, &[s1], &filter, &UserZone::utc())
                .unwrap_or_else(|e| panic!("{keyword}? failed: {e}"));

            let negated = bound(&conn, &format!("NOT {keyword}?"));
            apply_filters(&mut conn, &[s1], &negated, &UserZone::utc())
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

    fn selected(conn: &mut RawConnection, ids: &[i64], filter: &str, zone: &UserZone) -> Vec<i64> {
        apply_filters(conn, ids, &bound(conn, filter), zone)
            .unwrap()
            .source_ids
    }

    /// `!=`, `!~` and `NOT IN` are `NOT` of their positive forms: each selects exactly what
    /// its `NOT` form selects — everything else, a source with no value included. A pin of
    /// the binding law.
    #[test]
    fn every_negated_operator_selects_what_its_not_form_selects() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos");
        let jpg = insert_source(&conn, root, "a");
        let png = insert_source(&conn, root, "b");
        let none = insert_source(&conn, root, "c");
        insert_fact_entry(&conn, "source", jpg, "content.Kind", "jpg");
        insert_fact_entry(&conn, "source", png, "content.Kind", "png");
        let ids = [jpg, png, none];
        let zone = UserZone::utc();
        for (negated, not_form) in [
            ("Kind!=jpg", "NOT Kind=jpg"),
            ("Kind!~j*", "NOT Kind~j*"),
            ("Kind NOT IN (jpg, gif)", "NOT Kind IN (jpg, gif)"),
        ] {
            let by_operator = selected(&mut conn, &ids, negated, &zone);
            assert_eq!(by_operator, vec![png, none], "{negated}");
            assert_eq!(
                by_operator,
                selected(&mut conn, &ids, not_form, &zone),
                "{negated} and {not_form}"
            );
        }
    }

    /// A value of another type than the key was bound for, met while the filter runs, stops
    /// the run as an answer — never read as no value, which `NOT` would select. A pin of the
    /// binding law.
    #[test]
    fn a_type_that_changes_mid_run_refuses_the_run() {
        for filter in ["NOT Kind=jpg", "Kind|lowercase?", "Kind!=png"] {
            let mut conn = setup_test_db();
            let root = insert_root(&conn, "/photos");
            let jpg = insert_source(&conn, root, "a");
            let png = insert_source(&conn, root, "b");
            let none = insert_source(&conn, root, "c");
            insert_fact_entry(&conn, "source", jpg, "content.Kind", "jpg");
            insert_fact_entry(&conn, "source", png, "content.Kind", "png");
            let bound = bound(&conn, filter);
            // The key's type changes between binding and evaluation.
            conn.execute(
                "UPDATE facts SET value_text = NULL, value_num = 5 WHERE entity_id = ?",
                [png],
            )
            .unwrap();
            let Err(err) = apply_filters(&mut conn, &[jpg, png, none], &bound, &UserZone::utc())
            else {
                panic!("{filter} ran over a changed type");
            };
            let refused = err
                .downcast_ref::<crate::expr::ExpressionRefused>()
                .unwrap_or_else(|| panic!("{filter}: {err}"));
            assert!(refused.is_answer(), "{filter}");
            assert_eq!(
                refused.to_string(),
                "content.Kind's type changed while this ran; run it again"
            );
        }
    }

    /// A key with no facts when the filter was read, which gains its first while it runs,
    /// stops the run as the same answer: the value it meets is new, whatever its type.
    #[test]
    fn a_key_that_gains_its_first_facts_mid_run_refuses_the_run() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos");
        let a = insert_source(&conn, root, "a");
        let bound = bound(&conn, "New|year?");
        insert_fact_entry(&conn, "source", a, "content.New", "x");
        let Err(err) = apply_filters(&mut conn, &[a], &bound, &UserZone::utc()) else {
            panic!("a key gaining facts mid-run must stop the run");
        };
        let refused = err
            .downcast_ref::<crate::expr::ExpressionRefused>()
            .unwrap_or_else(|| panic!("{err}"));
        assert!(refused.is_answer());
        assert_eq!(
            refused.to_string(),
            "content.New gained facts while this ran; run it again"
        );
    }

    /// The key decides what a typed value means, read on a user's clock in `Europe/Amsterdam`.
    #[test]
    fn the_key_decides_what_a_typed_value_means() {
        let mut conn = setup_test_db();
        let zone = UserZone::for_test("Europe/Amsterdam");
        let root = insert_root(&conn, "/photos");
        // Saved 2024-03-04 19:00 Amsterdam (a Monday), 2023-07-01 09:00 (a Saturday).
        let march = insert_source_saved_at(&conn, root, "a", 1709575200);
        let july = insert_source_saved_at(&conn, root, "b", 1688194800);
        conn.execute("UPDATE sources SET size = 1024 WHERE id = ?", [march])
            .unwrap();
        conn.execute("UPDATE sources SET size = 2000000 WHERE id = ?", [july])
            .unwrap();
        insert_fact_entry(
            &conn,
            "source",
            march,
            "content.textdate",
            "2024:07:23 11:06:32",
        );
        insert_fact_entry(
            &conn,
            "source",
            july,
            "content.textdate",
            "2023:01:05 10:00:00",
        );
        let ids = [march, july];
        let mut sel = |f: &str| selected(&mut conn, &ids, f, &zone);
        assert_eq!(sel("source.mtime|month=3"), vec![march]);
        assert_eq!(sel("source.mtime|month=03"), vec![march]);
        assert_eq!(sel("source.mtime|weekday=1"), vec![march]);
        assert_eq!(sel("source.mtime|weekday=Monday"), vec![march]);
        assert_eq!(sel("source.mtime|time>=18:00"), vec![march]);
        assert_eq!(sel("source.size>=1e6"), vec![july]);
        assert_eq!(sel(r#"source.size="1024""#), vec![march]);
        assert_eq!(sel("source.mtime>=2024"), vec![march]);
        assert_eq!(sel("textdate|year=2024"), vec![march]);
        // A part orders as a whole number.
        assert_eq!(sel("source.mtime|year<2024"), vec![july]);
    }

    /// **The frame rule's verifier**: a time Canon prints, typed back into `--where`, selects
    /// exactly the values that print the same — on the user's clock, through both clock-change
    /// hours, in two zones. Each value prints as `facts` and a pattern show it, bare and
    /// through `|date`, `|hour`, `|time` and `|datetime`; the expected set is read off those
    /// printed forms, never off the comparison. The corpus holds two instants an hour apart
    /// that print the same in the hour the clock repeats, a clock reading printing the same
    /// as both, a reading in the hour the clock skips, a fraction inside a printed second, and
    /// an instant with a recorded offset. Every value is written to the second or finer, so no
    /// printed form is coarser than another value's: a coarser value also selects the finer
    /// values inside its period, which `clock_span_comparisons_follow_the_whole_period_table`
    /// pins. It fails if a typed time is compared as one instant rather than by the span it
    /// prints as.
    #[test]
    fn a_printed_value_typed_back_selects_exactly_what_prints_the_same() {
        use crate::core::domain::time::reading::read_time;
        use crate::expr::domain::transform::{apply_chain, parse_modifier, rendered, Modified};

        let written = [
            // Amsterdam: the clock repeats 02:00–03:00 on 27 October 2024 and skips it on
            // 31 March 2024.
            "2024-10-27T00:30:00Z",
            "2024-10-27T01:30:00Z",
            "2024-10-27T01:30:00.250Z",
            "2024-10-27 02:30:00",
            "2024-10-27T02:30:00+05:00",
            "2024-03-31T00:30:00Z",
            "2024-03-31T01:30:00Z",
            "2024-03-31 02:30:15",
            // New York: the clock repeats 01:00–02:00 on 3 November 2024 and skips 02:00–03:00
            // on 10 March 2024.
            "2024-11-03T05:30:00Z",
            "2024-11-03T06:30:00Z",
            "2024-03-10T06:30:00Z",
            "2024-03-10T07:30:00Z",
            "2024-03-10 02:30:00",
        ];
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos");
        let corpus: Vec<(i64, FactValue)> = written
            .iter()
            .enumerate()
            .map(|(n, text)| {
                let id = insert_source(&conn, root, &format!("{n}.jpg"));
                insert_time_fact(&conn, id, "content.taken", text);
                (id, FactValue::Time(read_time(text).unwrap()))
            })
            .collect();
        let ids: Vec<i64> = corpus.iter().map(|(id, _)| *id).collect();

        for zone_name in ["Europe/Amsterdam", "America/New_York"] {
            let zone = UserZone::for_test(zone_name);
            // Some printed time stands for several stored values, or nothing is shared.
            let mut shared = 0;
            for modifier in [
                None,
                Some("date"),
                Some("hour"),
                Some("time"),
                Some("datetime"),
            ] {
                let calls: Vec<_> = modifier
                    .map(|m| parse_modifier(m).unwrap())
                    .into_iter()
                    .collect();
                let last = calls.last().map(|c| c.modifier);
                let printed: Vec<(i64, Option<String>)> = corpus
                    .iter()
                    .map(|(id, value)| {
                        let shown =
                            match apply_chain(value.clone(), None, &calls, "content.taken", &zone)
                                .unwrap()
                                .value
                            {
                                Modified::Value(v) => Some(rendered(last, &v, &zone)),
                                Modified::NoValue(_) => None,
                            };
                        (*id, shown)
                    })
                    .collect();
                let read_as = if modifier == Some("hour") {
                    FactType::Num
                } else {
                    FactType::Time
                };
                for (_, shown) in &printed {
                    let Some(text) = shown else { continue };
                    let key = modifier.map_or("taken".to_string(), |m| format!("taken|{m}"));
                    let filter = format!("{key}={}", printed_value(text, read_as));
                    let expected: Vec<i64> = printed
                        .iter()
                        .filter(|(_, other)| other.as_ref() == Some(text))
                        .map(|(id, _)| *id)
                        .collect();
                    shared += usize::from(modifier.is_none() && expected.len() > 1);
                    assert_eq!(
                        selected(&mut conn, &ids, &filter, &zone),
                        expected,
                        "{zone_name}: {filter}"
                    );
                }
            }
            assert!(shared > 0, "{zone_name}: no printed time is shared");
        }
    }

    /// Text compares as text, in character order, and number
    /// and date questions are asked of it through `|number` and the time modifiers.
    #[test]
    fn text_is_text_and_asked_as_numbers_or_times_by_name() {
        let mut conn = setup_test_db();
        let zone = UserZone::utc();
        let root = insert_root(&conn, "/photos");
        let source = |path: &str, facts: &[(&str, &str)]| {
            let id = insert_source(&conn, root, path);
            for (key, value) in facts {
                insert_fact_entry(&conn, "source", id, key, value);
            }
            id
        };
        let n9 = source(
            "2018/a",
            &[("content.numtext", "9"), ("content.version", "v1.9")],
        );
        let n10 = source(
            "2019/b",
            &[("content.numtext", "10"), ("content.version", "v2.0")],
        );
        let n11 = source("2024/c", &[("content.numtext", "11")]);
        let misc = source("misc/d", &[]);
        let ids = [n9, n10, n11, misc];
        let mut sel = |f: &str| selected(&mut conn, &ids, f, &zone);
        assert_eq!(sel("numtext|number>=10"), vec![n10, n11]);
        assert_eq!(sel(r#"numtext="10""#), vec![n10]);
        assert_eq!(sel(r#"numtext>="10""#), vec![n9, n10, n11]);
        assert_eq!(sel("version>=v2"), vec![n10]);
        assert_eq!(sel(r#"source.rel_path[0]="2019""#), vec![n10]);
        assert_eq!(sel("source.rel_path[0]|number>=2019"), vec![n10, n11]);
        assert_eq!(sel("numtext|number?"), vec![n9, n10, n11]);
        assert_eq!(sel("source.rel_path[0]|number?"), vec![n9, n10, n11]);
    }

    /// `NOT` is everything else; the ordering operators select
    /// only sources with a value.
    #[test]
    fn a_negated_filter_selects_everything_else() {
        let mut conn = setup_test_db();
        let zone = UserZone::utc();
        let root = insert_root(&conn, "/photos");
        let a = insert_source(&conn, root, "a");
        let b = insert_source(&conn, root, "b");
        let c = insert_source(&conn, root, "c");
        insert_time_fact(&conn, a, "content.taken", "2023-05-01");
        insert_time_fact(&conn, b, "content.taken", "2024-05-01");
        let ids = [a, b, c];
        let mut sel = |f: &str| selected(&mut conn, &ids, f, &zone);
        assert_eq!(sel("taken=2024"), vec![b]);
        for f in ["taken!=2024", "NOT taken=2024", "taken NOT IN (2024)"] {
            assert_eq!(sel(f), vec![a, c], "{f}");
        }
        assert_eq!(sel("taken!=2024 AND taken?"), vec![a]);
        assert_eq!(sel("taken<2024"), vec![a]);
        assert_eq!(sel("NOT taken>=2024"), vec![a, c]);
    }

    /// Every text value is selected, alone, by its written
    /// form; a bucket is a key whose values are its labels.
    #[test]
    fn every_value_can_be_written() {
        let mut conn = setup_test_db();
        let zone = UserZone::utc();
        let root = insert_root(&conn, "/photos");
        let titles = ["it's", r#"say "hi""#, r#"a "b" 'c'"#, "", " a "];
        let ids: Vec<i64> = titles
            .iter()
            .enumerate()
            .map(|(i, title)| {
                let id = insert_source(&conn, root, &format!("t{i}"));
                insert_fact_entry(&conn, "source", id, "content.Title", title);
                id
            })
            .collect();
        for (title, id) in titles.iter().zip(&ids) {
            let written = format!("Title={}", printed_value(title, FactType::Text));
            assert_eq!(
                selected(&mut conn, &ids, &written, &zone),
                vec![*id],
                "{written}"
            );
        }
        assert_eq!(
            selected(&mut conn, &ids, "Title='it''s'", &zone),
            vec![ids[0]]
        );
        // `source.size` is 1000 for every source here: a bucket's labels select it.
        for written in [
            "source.size|bucket(60,300)=300-and-up",
            "source.size|bucket=1K-to-10K",
        ] {
            assert_eq!(selected(&mut conn, &ids, written, &zone), ids, "{written}");
        }
        assert_eq!(
            selected(
                &mut conn,
                &ids,
                "source.size|bucket(60,300)=60-to-300",
                &zone
            ),
            Vec::<i64>::new()
        );
    }

    /// In a filter, a text modifier after a modifier reads its printed form,
    /// and an accessor reads stored text as path segments.
    #[test]
    fn a_text_modifier_after_a_modifier_and_an_accessor_on_text_select() {
        let mut conn = setup_test_db();
        let zone = UserZone::utc();
        let root = insert_root(&conn, "/photos");
        // 1718582400 is Monday 17 June 2024, 00:00 UTC.
        let monday = insert_source_saved_at(&conn, root, "a", 1718582400);
        let other = insert_source_saved_at(&conn, root, "b", 1718582400 + 86400);
        insert_fact_entry(
            &conn,
            "source",
            monday,
            "content.SourceFile",
            "/cards/DCIM/x.jpg",
        );
        let ids = [monday, other];
        assert_eq!(
            selected(
                &mut conn,
                &ids,
                "source.mtime|weekday|lowercase=monday",
                &zone
            ),
            vec![monday]
        );
        assert_eq!(
            selected(&mut conn, &ids, "source.mtime|quarter|lowercase=q2", &zone),
            vec![monday, other]
        );
        assert_eq!(
            selected(&mut conn, &ids, "SourceFile[-1]=x.jpg", &zone),
            vec![monday]
        );
        assert_eq!(
            selected(&mut conn, &ids, "SourceFile[9]?", &zone),
            Vec::<i64>::new()
        );
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

    #[test]
    fn a_filter_reads_an_instant_wherever_it_reads_a_save_times_value() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos");
        let s = insert_source_saved_at(&conn, root, "a.jpg", NEW_YEARS_EVE_2330_UTC);
        let read = |conn: &mut RawConnection, filter: &str| {
            apply_filters(conn, &[s], &bound(conn, filter), &UserZone::utc())
                .unwrap()
                .read_an_instant
        };

        assert!(read(&mut conn, "source.mtime|year=2023"));
        assert!(read(&mut conn, "source.mtime|month?"));
        assert!(read(&mut conn, "NOT source.mtime|year IN (2020, 2021)"));
        assert!(read(&mut conn, "source.mtime>=2024-01-01"));
        assert!(!read(&mut conn, "source.mtime?"));
        assert!(!read(&mut conn, "source.ext=jpg"));
        assert!(
            !apply_filters(&mut conn, &[s], &BoundFilters::none(), &UserZone::utc())
                .unwrap()
                .read_an_instant
        );
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
        // Every other computed key still exists for every source.
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
        // A modifier that applies keeps the base key's answer; a time modifier reads text
        // as a time, and text that does not read as one has no value.
        assert_eq!(
            selected(&mut conn, &all, "content.Make|lowercase?", &zone),
            selected(&mut conn, &all, "content.Make?", &zone)
        );
        assert_eq!(
            selected(&mut conn, &all, "content.Make|year?", &zone),
            Vec::<i64>::new()
        );
    }

    // ========================================================================
    // A source's one value
    // ========================================================================

    /// A source whose content holds `content.Make=Canon` and which holds `Nikon` itself: the
    /// content's fact is its one value, so the source's own never matches.
    #[test]
    fn the_filter_compares_the_one_value() {
        use crate::core::testing::{insert_fact, insert_object_fact, insert_source_with_size};
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos");
        let content = insert_object(&conn, "datahash");
        let two_rows = insert_source_with_size(&conn, root, "a.jpg", Some(content), 100);
        insert_object_fact(&conn, content, "content.Make", "Canon");
        insert_fact(&conn, two_rows, "content.Make", "Nikon");
        let zone = UserZone::utc();

        for (clause, want) in [
            ("content.Make!=Nikon", vec![two_rows]),
            ("content.Make=Canon", vec![two_rows]),
            ("content.Make=Nikon", vec![]),
            ("content.Make!=Canon", vec![]),
            ("content.Make IN (Nikon)", vec![]),
            ("NOT content.Make=Nikon", vec![two_rows]),
        ] {
            assert_eq!(
                selected(&mut conn, &[two_rows], clause, &zone),
                want,
                "{clause}"
            );
        }
    }

    /// A key exists for a source exactly where its one value does: a source holding only its own
    /// fact and one whose content holds the only fact both have it, and where the two disagree
    /// through a modifier, the content's answers.
    #[test]
    fn existence_reads_the_one_value() {
        use crate::core::testing::{insert_fact, insert_object_fact, insert_source_with_size};
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos");
        let content = insert_object(&conn, "datahash");
        let dated = insert_object(&conn, "datedhash");
        let own_only = insert_source_with_size(&conn, root, "o.jpg", None, 100);
        let content_only = insert_source_with_size(&conn, root, "c.jpg", Some(content), 100);
        let neither = insert_source_with_size(&conn, root, "n.jpg", None, 100);
        let disagree = insert_source_with_size(&conn, root, "d.jpg", Some(dated), 100);
        insert_fact(&conn, own_only, "content.Make", "Sony");
        insert_object_fact(&conn, content, "content.Make", "Canon");

        // The content knows only the year; the source's own fact knows the month.
        let year = FactValue::Time(crate::core::domain::time::reading::read_time("2024").unwrap());
        let p = crate::core::repo::fact::fact_value_params(&year).unwrap();
        conn.execute(
            &format!(
                "INSERT INTO facts (entity_type, entity_id, key, {}, observed_at)
                 VALUES ('object', ?1, 'content.When', ?2, ?3, ?4, ?5, ?6, ?7, ?8, 0)",
                crate::core::repo::fact::FACT_VALUE_COLUMNS
            ),
            rusqlite::params![dated, p[0], p[1], p[2], p[3], p[4], p[5], p[6]],
        )
        .unwrap();
        insert_time_fact(&conn, disagree, "content.When", "2024-06-15T14:30:05");
        let zone = UserZone::utc();

        let all = [own_only, content_only, neither, disagree];
        assert_eq!(
            selected(&mut conn, &all, "content.Make?", &zone),
            vec![own_only, content_only]
        );
        assert_eq!(
            selected(&mut conn, &all, "content.When?", &zone),
            vec![disagree]
        );
        assert!(
            selected(&mut conn, &all, "content.When|month?", &zone).is_empty(),
            "the content's year-only value has no month, whatever the source's own fact says"
        );
    }

    /// The hash keys are read from the facts table under their stored name, through the
    /// cache like any stored key; `hash` is that key under another name.
    #[test]
    fn a_hash_key_reads_through_the_cache() {
        use crate::core::testing::insert_object_fact;
        let full = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos");
        let content = insert_object(&conn, "datahash");
        let other = insert_object(&conn, "otherhash");
        insert_object_fact(&conn, content, "content.hash.sha256", full);
        insert_object_fact(&conn, other, "content.hash.sha256", "711e96");
        let subject = insert_source_with_object(&conn, root, "a.txt", Some(content));
        let neighbour = insert_source_with_object(&conn, root, "b.txt", Some(other));
        let ids = [subject, neighbour];

        let hash =
            crate::expr::domain::read::read_keys("hash", 0, Grammar::Key, AliasMode::NotLoaded)
                .value
                .unwrap()
                .remove(0)
                .base;
        let cache = prefetch_facts(&mut conn, &ids, &[hash]).unwrap();
        assert!(matches!(
            cache.value(subject, "content.hash.sha256"),
            Some(FactValue::Text(h)) if h == full
        ));
        let zone = UserZone::utc();
        assert_eq!(
            selected(
                &mut conn,
                &ids,
                &format!("content.hash.sha256={full}"),
                &zone
            ),
            vec![subject]
        );
        assert_eq!(
            selected(&mut conn, &ids, &format!("hash={full}"), &zone),
            vec![subject]
        );
    }
}
