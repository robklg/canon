use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::db;
use crate::exclude;
use crate::filter::{self, Filter};

#[derive(Serialize, Deserialize)]
pub struct Manifest {
    pub meta: ManifestMeta,
    pub output: ManifestOutput,
    pub sources: Vec<ManifestSource>,
}

#[derive(Serialize, Deserialize)]
pub struct ManifestMeta {
    pub query: Vec<String>,
    pub generated_at: i64,
}

#[derive(Serialize, Deserialize)]
pub struct ManifestOutput {
    pub pattern: String,
    pub base_dir: String,
}

#[derive(Serialize, Deserialize)]
pub struct ManifestSource {
    pub id: i64,
    pub path: String,
    pub size: i64,
    pub hash_type: Option<String>,
    pub hash_value: Option<String>,
    pub facts: HashMap<String, serde_json::Value>,
}

pub struct GenerateOptions {
    pub include_archived: bool,
    pub show_archived: bool,
}

pub fn generate(
    db_path: &Path,
    filters: &[String],
    output_path: &Path,
    options: &GenerateOptions,
) -> Result<()> {
    let conn = db::open(db_path)?;

    let parsed_filters: Vec<Filter> = filters
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    let (sources, archived, excluded_count) = query_sources(&conn, &parsed_filters, options.include_archived)?;

    // Report excluded files (hard gate - always skipped)
    if excluded_count > 0 {
        eprintln!("Skipped {} excluded sources", excluded_count);
    }

    // Report archived files
    if !archived.is_empty() {
        eprintln!(
            "Excluded {} files already in archive(s)",
            archived.len()
        );
        if options.show_archived {
            eprintln!("Archived files:");
            for (source_path, archive_path) in &archived {
                eprintln!("  {} -> {}", source_path, archive_path);
            }
        } else {
            eprintln!("Use --show-archived to list them");
        }
    }

    if sources.is_empty() {
        println!("No sources matched the query");
        return Ok(());
    }

    let manifest = Manifest {
        meta: ManifestMeta {
            query: filters.to_vec(),
            generated_at: current_timestamp(),
        },
        output: ManifestOutput {
            pattern: "{filename}".to_string(),
            base_dir: ".".to_string(),
        },
        sources,
    };

    let toml_str = toml::to_string_pretty(&manifest)
        .context("Failed to serialize manifest")?;

    fs::write(output_path, &toml_str)
        .with_context(|| format!("Failed to write manifest to {}", output_path.display()))?;

    println!(
        "Generated manifest with {} sources: {}",
        manifest.sources.len(),
        output_path.display()
    );

    Ok(())
}

/// Returns (included_sources, archived_sources, excluded_count)
/// archived_sources is a list of (source_path, archive_path) for files already in an archive
/// excluded_count is the number of sources skipped due to policy.exclude (hard gate)
fn query_sources(
    conn: &Connection,
    filters: &[Filter],
    include_archived: bool,
) -> Result<(Vec<ManifestSource>, Vec<(String, String)>, usize)> {
    // Build query based on filters
    // By default only source roots, with --include-archived also include archive roots
    let role_clause = if include_archived {
        "1=1" // Include all roles
    } else {
        "r.role = 'source'"
    };

    let mut source_ids: Vec<i64> = conn
        .prepare(&format!(
            "SELECT s.id FROM sources s
             JOIN roots r ON s.root_id = r.id
             WHERE s.present = 1 AND {}",
            role_clause
        ))?
        .query_map([], |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()?;

    // Apply filters
    source_ids = filter::apply_filters(conn, &source_ids, filters)?;

    // Check which sources are already archived (same object_id exists in an archive root)
    // Also apply hard gate for excluded sources
    let mut sources = Vec::new();
    let mut archived = Vec::new();
    let mut excluded_count = 0;

    for source_id in source_ids {
        // HARD GATE: Skip excluded sources (no override flag)
        if exclude::is_excluded(conn, source_id)? {
            excluded_count += 1;
            continue;
        }

        if let Some(source) = fetch_source(conn, source_id)? {
            // Check if this content is already in an archive
            let archive_path = if let Some(ref hash) = source.hash_value {
                find_in_archive(conn, hash)?
            } else {
                None
            };

            if let Some(arch_path) = archive_path {
                if include_archived {
                    sources.push(source);
                } else {
                    archived.push((source.path.clone(), arch_path));
                }
            } else {
                sources.push(source);
            }
        }
    }

    Ok((sources, archived, excluded_count))
}

/// Find if a hash exists in any archive root, return the path if found
fn find_in_archive(conn: &Connection, hash_value: &str) -> Result<Option<String>> {
    let result: Option<(String, String)> = conn
        .query_row(
            "SELECT r.path, s.rel_path
             FROM sources s
             JOIN roots r ON s.root_id = r.id
             JOIN objects o ON s.object_id = o.id
             WHERE r.role = 'archive' AND o.hash_value = ? AND s.present = 1
             LIMIT 1",
            [hash_value],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;

    Ok(result.map(|(root, rel)| {
        if rel.is_empty() {
            root
        } else {
            format!("{}/{}", root, rel)
        }
    }))
}

fn fetch_source(conn: &Connection, source_id: i64) -> Result<Option<ManifestSource>> {
    let row: Option<(i64, String, String, i64, Option<i64>)> = conn
        .query_row(
            "SELECT s.id, r.path, s.rel_path, s.size, s.object_id
             FROM sources s
             JOIN roots r ON s.root_id = r.id
             WHERE s.id = ?",
            [source_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
        )
        .ok();

    let (id, root_path, rel_path, size, object_id) = match row {
        Some(r) => r,
        None => return Ok(None),
    };

    let full_path = if rel_path.is_empty() {
        root_path
    } else {
        format!("{}/{}", root_path, rel_path)
    };

    // Get hash if available
    let (hash_type, hash_value): (Option<String>, Option<String>) = if let Some(obj_id) = object_id {
        conn.query_row(
            "SELECT hash_type, hash_value FROM objects WHERE id = ?",
            [obj_id],
            |row| Ok((Some(row.get(0)?), Some(row.get(1)?))),
        )
        .unwrap_or((None, None))
    } else {
        (None, None)
    };

    // Collect facts
    let mut facts = HashMap::new();

    // Source facts
    let mut stmt = conn.prepare(
        "SELECT key, value_text, value_num, value_time, value_json
         FROM facts WHERE entity_type = 'source' AND entity_id = ?"
    )?;
    for row in stmt.query_map([source_id], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<f64>>(2)?,
            row.get::<_, Option<i64>>(3)?,
            row.get::<_, Option<String>>(4)?,
        ))
    })? {
        let (key, text, num, time, json) = row?;
        let value = fact_to_json(text, num, time, json);
        facts.insert(key, value);
    }

    // Object facts
    if let Some(obj_id) = object_id {
        let mut stmt = conn.prepare(
            "SELECT key, value_text, value_num, value_time, value_json
             FROM facts WHERE entity_type = 'object' AND entity_id = ?"
        )?;
        for row in stmt.query_map([obj_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<f64>>(2)?,
                row.get::<_, Option<i64>>(3)?,
                row.get::<_, Option<String>>(4)?,
            ))
        })? {
            let (key, text, num, time, json) = row?;
            let value = fact_to_json(text, num, time, json);
            facts.insert(key, value);
        }
    }

    Ok(Some(ManifestSource {
        id,
        path: full_path,
        size,
        hash_type,
        hash_value,
        facts,
    }))
}

fn fact_to_json(
    text: Option<String>,
    num: Option<f64>,
    time: Option<i64>,
    json: Option<String>,
) -> serde_json::Value {
    if let Some(t) = text {
        serde_json::Value::String(t)
    } else if let Some(n) = num {
        serde_json::json!(n)
    } else if let Some(t) = time {
        serde_json::json!(t)
    } else if let Some(j) = json {
        serde_json::from_str(&j).unwrap_or(serde_json::Value::String(j))
    } else {
        serde_json::Value::Null
    }
}

fn current_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs() as i64
}
