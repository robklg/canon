use anyhow::{bail, Context, Result};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::db;

#[derive(Debug, Clone)]
pub enum Filter {
    Exists { key: String },
    Equals { key: String, value: String },
}

impl Filter {
    pub fn parse(s: &str) -> Result<Self> {
        if let Some(key) = s.strip_suffix('?') {
            Ok(Filter::Exists { key: key.to_string() })
        } else if let Some((key, value)) = s.split_once('=') {
            Ok(Filter::Equals {
                key: key.to_string(),
                value: value.to_string(),
            })
        } else {
            bail!("Invalid filter syntax: {}. Use 'key?' for existence or 'key=value' for equality", s);
        }
    }
}

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

pub fn generate(db_path: &Path, filters: &[String], output_path: &Path) -> Result<()> {
    let conn = db::open(db_path)?;

    let parsed_filters: Vec<Filter> = filters
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    let sources = query_sources(&conn, &parsed_filters)?;

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

fn query_sources(conn: &Connection, filters: &[Filter]) -> Result<Vec<ManifestSource>> {
    // Build query based on filters
    // Start with base query for all present sources
    let mut source_ids: Vec<i64> = conn
        .prepare("SELECT id FROM sources WHERE present = 1")?
        .query_map([], |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()?;

    // Apply filters
    for filter in filters {
        source_ids = apply_filter(conn, &source_ids, filter)?;
    }

    // Fetch full source info
    let mut sources = Vec::new();
    for source_id in source_ids {
        if let Some(source) = fetch_source(conn, source_id)? {
            sources.push(source);
        }
    }

    Ok(sources)
}

fn apply_filter(conn: &Connection, source_ids: &[i64], filter: &Filter) -> Result<Vec<i64>> {
    let mut result = Vec::new();

    for &source_id in source_ids {
        let matches = match filter {
            Filter::Exists { key } => check_fact_exists(conn, source_id, key)?,
            Filter::Equals { key, value } => check_fact_equals(conn, source_id, key, value)?,
        };
        if matches {
            result.push(source_id);
        }
    }

    Ok(result)
}

fn check_fact_exists(conn: &Connection, source_id: i64, key: &str) -> Result<bool> {
    // Check source facts
    let source_exists: bool = conn
        .query_row(
            "SELECT 1 FROM facts WHERE entity_type = 'source' AND entity_id = ? AND key = ?",
            params![source_id, key],
            |_| Ok(true),
        )
        .unwrap_or(false);

    if source_exists {
        return Ok(true);
    }

    // Check object facts if source has an object
    let object_id: Option<i64> = conn
        .query_row(
            "SELECT object_id FROM sources WHERE id = ?",
            [source_id],
            |row| row.get(0),
        )
        .unwrap_or(None);

    if let Some(obj_id) = object_id {
        let object_exists: bool = conn
            .query_row(
                "SELECT 1 FROM facts WHERE entity_type = 'object' AND entity_id = ? AND key = ?",
                params![obj_id, key],
                |_| Ok(true),
            )
            .unwrap_or(false);

        if object_exists {
            return Ok(true);
        }
    }

    // Special case: check for built-in fields
    match key {
        "root_id" | "size" | "mtime" | "basis_rev" | "object_id" => Ok(true),
        "hash" | "content_hash" => Ok(object_id.is_some()),
        _ => Ok(false),
    }
}

fn check_fact_equals(conn: &Connection, source_id: i64, key: &str, value: &str) -> Result<bool> {
    // Handle built-in fields first
    match key {
        "root_id" => {
            let v: i64 = conn.query_row(
                "SELECT root_id FROM sources WHERE id = ?",
                [source_id],
                |row| row.get(0),
            )?;
            return Ok(v.to_string() == value);
        }
        "size" => {
            let v: i64 = conn.query_row(
                "SELECT size FROM sources WHERE id = ?",
                [source_id],
                |row| row.get(0),
            )?;
            return Ok(v.to_string() == value);
        }
        _ => {}
    }

    // Check source facts
    let source_match: bool = conn
        .query_row(
            "SELECT 1 FROM facts WHERE entity_type = 'source' AND entity_id = ? AND key = ? AND value_text = ?",
            params![source_id, key, value],
            |_| Ok(true),
        )
        .unwrap_or(false);

    if source_match {
        return Ok(true);
    }

    // Check object facts
    let object_id: Option<i64> = conn
        .query_row(
            "SELECT object_id FROM sources WHERE id = ?",
            [source_id],
            |row| row.get(0),
        )
        .unwrap_or(None);

    if let Some(obj_id) = object_id {
        let object_match: bool = conn
            .query_row(
                "SELECT 1 FROM facts WHERE entity_type = 'object' AND entity_id = ? AND key = ? AND value_text = ?",
                params![obj_id, key, value],
                |_| Ok(true),
            )
            .unwrap_or(false);

        if object_match {
            return Ok(true);
        }
    }

    Ok(false)
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
