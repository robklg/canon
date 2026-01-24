use anyhow::Result;
use serde::Serialize;
use std::collections::HashMap;
use std::io::{self, Write};
use std::path::PathBuf;

use crate::db::{Connection, Db};
use crate::filter::{self, get_fact_value, Filter};
use crate::path::canonicalize_scopes;
use crate::scope::ScopeMatch;
use crate::source::Source;
use crate::source_repo;

#[derive(Serialize)]
struct WorklistEntry {
    source_id: i64,
    path: String,
    root_id: i64,
    size: i64,
    mtime: i64,
    basis_rev: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    facts: Option<HashMap<String, serde_json::Value>>,
    #[serde(skip_serializing)]
    object_id: Option<i64>,
}

impl WorklistEntry {
    /// Create a WorklistEntry from a Source, optionally fetching facts.
    fn from_source(
        source: &Source,
        emit_keys: &[String],
        conn: &Connection,
    ) -> Result<Self> {
        let facts = if emit_keys.is_empty() {
            None
        } else {
            let mut map = HashMap::new();
            for key in emit_keys {
                let (entity_type, entity_id) = if key.starts_with("source.") {
                    ("source", Some(source.id))
                } else {
                    ("object", source.object_id)
                };
                let value = match entity_id {
                    Some(eid) => get_fact_value(conn, entity_type, eid, key)?
                        .map(|v| v.into())
                        .unwrap_or(serde_json::Value::Null),
                    None => serde_json::Value::Null,
                };
                map.insert(key.clone(), value);
            }
            Some(map)
        };

        Ok(WorklistEntry {
            source_id: source.id,
            path: source.path(),
            root_id: source.root_id,
            size: source.size,
            mtime: source.mtime,
            basis_rev: source.basis_rev,
            facts,
            object_id: source.object_id,
        })
    }
}

pub fn run(
    db: &mut Db,
    scope_paths: &[PathBuf],
    filter_strs: &[String],
    include_archived: bool,
    include_excluded: bool,
    unique_content: bool,
    emit_keys: &[String],
) -> Result<()> {
    // Parse filters upfront
    let filters: Vec<Filter> = filter_strs
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    // Resolve scope paths to realpaths and classify
    let scope_prefixes = canonicalize_scopes(scope_paths)?;
    let scopes = ScopeMatch::classify_all(&scope_prefixes);

    let conn = db.conn_mut();

    // Fetch all matching sources using domain predicates
    let (sources, excluded_count) =
        get_matching_sources(conn, &scopes, &filters, include_archived, include_excluded)?;

    let stdout = io::stdout();
    let mut handle = stdout.lock();
    let mut seen_objects: std::collections::HashSet<i64> = std::collections::HashSet::new();
    let mut skipped_unhashed: u64 = 0;
    let mut skipped_duplicate: u64 = 0;

    for source in &sources {
        if unique_content {
            // Skip sources without an object_id
            if source.object_id.is_none() {
                skipped_unhashed += 1;
                continue;
            }
            let object_id = source.object_id.unwrap();
            // Skip if we've already emitted a source for this object
            if seen_objects.contains(&object_id) {
                skipped_duplicate += 1;
                continue;
            }
            seen_objects.insert(object_id);
        }

        let entry = WorklistEntry::from_source(source, emit_keys, conn)?;
        let json = serde_json::to_string(&entry)?;
        writeln!(handle, "{}", json)?;
    }

    // Report stats to stderr
    if include_excluded && excluded_count > 0 {
        eprintln!("Included {} excluded sources", excluded_count);
    } else if !include_excluded && excluded_count > 0 {
        eprintln!("Skipped {} excluded sources", excluded_count);
    }
    if unique_content && (skipped_unhashed > 0 || skipped_duplicate > 0) {
        eprintln!(
            "Skipped {} unhashed, {} duplicate sources",
            skipped_unhashed, skipped_duplicate
        );
    }

    Ok(())
}

/// Fetch sources matching scope/role/exclusion criteria, then apply --where filters.
///
/// Returns (matching_sources, excluded_count) where excluded_count is the number
/// of sources that matched scope/role but were excluded.
fn get_matching_sources(
    conn: &mut Connection,
    scopes: &[ScopeMatch],
    filters: &[Filter],
    include_archived: bool,
    include_excluded: bool,
) -> Result<(Vec<Source>, usize)> {
    // 1. Get all root IDs
    let root_ids: Vec<i64> = conn
        .prepare("SELECT id FROM roots")?
        .query_map([], |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()?;

    // 2. Fetch all present sources for those roots
    let all_sources = source_repo::batch_fetch_by_roots(conn, &root_ids)?;

    // 3. Filter using domain predicates, tracking excluded count
    let mut excluded_count = 0usize;
    let filtered: Vec<Source> = all_sources
        .into_iter()
        .filter(|s| s.is_active())
        .filter(|s| include_archived || s.is_from_role("source"))
        .filter(|s| s.matches_scope(scopes))
        .filter(|s| {
            if s.is_excluded() {
                if !include_excluded {
                    excluded_count += 1;
                    return false;
                }
            }
            true
        })
        .collect();

    // 4. Apply --where filters if present
    if filters.is_empty() {
        return Ok((filtered, excluded_count));
    }

    // Extract IDs, apply filters, then map back to Source objects
    let source_ids: Vec<i64> = filtered.iter().map(|s| s.id).collect();
    let filtered_ids = filter::apply_filters(conn, &source_ids, filters)?;

    // Build a set for O(1) lookup
    let filtered_id_set: std::collections::HashSet<i64> = filtered_ids.into_iter().collect();

    // Keep only sources whose ID passed the filter
    let result: Vec<Source> = filtered
        .into_iter()
        .filter(|s| filtered_id_set.contains(&s.id))
        .collect();

    Ok((result, excluded_count))
}
