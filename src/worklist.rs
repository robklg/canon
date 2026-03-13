use anyhow::Result;
use serde::Serialize;
use std::collections::HashMap;
use std::io::{self, Write};
use std::path::PathBuf;

use crate::domain::path::resolve_paths;
use crate::domain::scope::ScopeMatch;
use crate::domain::source::Source;
use crate::domain::IncludeSet;
use crate::expr::filter::{get_fact_value, Filter};
use crate::ops::selection::{self, RolePolicy, SelectionParams};
use crate::repo::{self, Connection, Db};

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
}

impl WorklistEntry {
    /// Create a WorklistEntry from a Source, optionally fetching facts.
    fn from_source(source: &Source, emit_keys: &[String], conn: &Connection) -> Result<Self> {
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
        })
    }
}

pub fn run(
    db: &mut Db,
    scope_paths: &[PathBuf],
    filter_strs: &[String],
    include: &IncludeSet,
    unique_content: bool,
    emit_keys: &[String],
) -> Result<()> {
    // Parse filters upfront
    let filters: Vec<Filter> = filter_strs
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    let conn = db.conn_mut();

    // Resolve scope paths
    let all_roots = repo::root::fetch_all(conn)?;
    let scope_prefixes = resolve_paths(scope_paths, &all_roots)?;
    let scopes = ScopeMatch::classify_all(&scope_prefixes);

    let params = SelectionParams {
        scopes,
        include: include.clone(),
        filters,
        role_policy: RolePolicy::SourceUnlessIncluded,
    };
    let sel = selection::select_sources(conn, &params)?;

    let stdout = io::stdout();
    let mut handle = stdout.lock();
    let mut seen_objects: std::collections::HashSet<i64> = std::collections::HashSet::new();
    let mut skipped_unhashed: u64 = 0;
    let mut skipped_duplicate: u64 = 0;

    for source in &sel.sources {
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
        writeln!(handle, "{json}")?;
    }

    // Report stats to stderr
    let excluded_count = sel.excluded_count;
    if include.includes_excluded() && sel.included_excluded_count > 0 {
        eprintln!("Included {} excluded sources", sel.included_excluded_count);
    } else if excluded_count > 0 {
        eprintln!("Skipped {excluded_count} excluded sources");
    }
    if unique_content && (skipped_unhashed > 0 || skipped_duplicate > 0) {
        eprintln!("Skipped {skipped_unhashed} unhashed, {skipped_duplicate} duplicate sources");
    }

    Ok(())
}
