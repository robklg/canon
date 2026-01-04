use anyhow::{bail, Context, Result};
use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::db::{canonicalize_scope, resolve_archive_path, scope_param, Connection, Db, SCOPE_CLAUSE};
use crate::exclude;
use crate::filter::{self, Filter};
use rusqlite::params;

#[derive(Serialize, Deserialize)]
pub struct Manifest {
    pub meta: ManifestMeta,
    pub output: ManifestOutput,
    pub sources: Vec<ManifestSource>,
}

#[derive(Serialize, Deserialize)]
pub struct ManifestMeta {
    pub query: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    pub generated_at: i64,
}

#[derive(Serialize, Deserialize)]
pub struct ManifestOutput {
    pub pattern: String,
    pub archive_root_id: i64,
    pub base_dir: String,
}

#[derive(Serialize, Deserialize)]
pub struct ManifestSource {
    pub id: i64,
    pub root_id: i64,
    pub path: String,
    pub size: i64,
    pub object_id: Option<i64>,
    pub hash_type: Option<String>,
    pub hash_value: Option<String>,
    pub facts: HashMap<String, serde_json::Value>,
}

pub struct GenerateOptions {
    pub include_archived: bool,
    pub show_archived: bool,
}

pub fn generate(
    db: &Db,
    scope_path: Option<&Path>,
    filters: &[String],
    dest: &Path,
    output_path: &Path,
    options: &GenerateOptions,
) -> Result<()> {
    // Require at least one of path scope or filters
    if scope_path.is_none() && filters.is_empty() {
        bail!("At least one of path or --where filter is required");
    }

    let conn = db.conn();

    // Resolve destination to archive root + relative subdir
    let (archive_root_id, _archive_root_path, base_dir) = resolve_archive_path(conn, dest)?;

    // Resolve scope path to realpath if provided
    let scope_prefix = canonicalize_scope(scope_path)?;

    let parsed_filters: Vec<Filter> = filters
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    let (sources, archived, excluded_count) = query_sources(conn, &scope_prefix, &parsed_filters, options.include_archived)?;

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

    // Collect facts with 100% coverage for help comments
    let full_coverage_facts = collect_full_coverage_facts(conn, &sources)?;
    let fact_help = generate_fact_help(&sources, &full_coverage_facts);

    let manifest = Manifest {
        meta: ManifestMeta {
            query: filters.to_vec(),
            scope: scope_prefix.clone(),
            generated_at: current_timestamp(),
        },
        output: ManifestOutput {
            pattern: "{filename}".to_string(),
            archive_root_id,
            base_dir,
        },
        sources,
    };

    let toml_str = toml::to_string_pretty(&manifest)
        .context("Failed to serialize manifest")?;

    // Insert fact help comments before [[sources]] section
    let toml_with_help = if let Some(sources_pos) = toml_str.find("[[sources]]") {
        format!(
            "{}\n{}{}",
            toml_str[..sources_pos].trim_end(),
            fact_help,
            &toml_str[sources_pos..]
        )
    } else {
        // No sources section, just append help at the end
        format!("{}\n{}", toml_str, fact_help)
    };

    fs::write(output_path, &toml_with_help)
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
    scope_prefix: &Option<String>,
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

    let prefix = scope_param(scope_prefix);
    let mut source_ids: Vec<i64> = conn
        .prepare(&format!(
            "SELECT s.id FROM sources s
             JOIN roots r ON s.root_id = r.id
             WHERE s.present = 1 AND {} AND {}",
            role_clause, SCOPE_CLAUSE
        ))?
        .query_map(params![prefix, prefix], |row| row.get(0))?
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
    let row: Option<(i64, i64, String, String, i64, Option<i64>)> = conn
        .query_row(
            "SELECT s.id, s.root_id, r.path, s.rel_path, s.size, s.object_id
             FROM sources s
             JOIN roots r ON s.root_id = r.id
             WHERE s.id = ?",
            [source_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?)),
        )
        .ok();

    let (id, root_id, root_path, rel_path, size, object_id) = match row {
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
        root_id,
        path: full_path,
        size,
        object_id,
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

/// Fact type as stored in the database
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FactType {
    Text,
    Num,
    Time,
    Json,
}

impl FactType {
    fn as_str(&self) -> &'static str {
        match self {
            FactType::Text => "text",
            FactType::Num => "num",
            FactType::Time => "time",
            FactType::Json => "json",
        }
    }
}

/// Collect facts with 100% coverage across all sources in the manifest
fn collect_full_coverage_facts(conn: &Connection, sources: &[ManifestSource]) -> Result<Vec<(String, FactType, String)>> {
    use std::collections::HashSet;

    if sources.is_empty() {
        return Ok(Vec::new());
    }

    let source_count = sources.len();
    let source_ids: Vec<i64> = sources.iter().map(|s| s.id).collect();

    // Count facts by key across all sources
    let mut fact_counts: HashMap<String, (usize, FactType)> = HashMap::new();
    let mut seen_keys: HashSet<String> = HashSet::new();

    // Query source facts
    for source_id in &source_ids {
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
            let fact_type = if text.is_some() {
                FactType::Text
            } else if num.is_some() {
                FactType::Num
            } else if time.is_some() {
                FactType::Time
            } else if json.is_some() {
                FactType::Json
            } else {
                continue;
            };

            let entry = fact_counts.entry(key.clone()).or_insert((0, fact_type));
            if !seen_keys.contains(&format!("{}:{}", source_id, key)) {
                entry.0 += 1;
                seen_keys.insert(format!("{}:{}", source_id, key));
            }
        }
    }

    // Query object facts (only for sources that have objects)
    for (source, object_id) in sources.iter().filter_map(|s| s.object_id.map(|oid| (s, oid))) {
        let mut stmt = conn.prepare(
            "SELECT key, value_text, value_num, value_time, value_json
             FROM facts WHERE entity_type = 'object' AND entity_id = ?"
        )?;

        for row in stmt.query_map([object_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<f64>>(2)?,
                row.get::<_, Option<i64>>(3)?,
                row.get::<_, Option<String>>(4)?,
            ))
        })? {
            let (key, text, num, time, json) = row?;
            let fact_type = if text.is_some() {
                FactType::Text
            } else if num.is_some() {
                FactType::Num
            } else if time.is_some() {
                FactType::Time
            } else if json.is_some() {
                FactType::Json
            } else {
                continue;
            };

            let entry = fact_counts.entry(key.clone()).or_insert((0, fact_type));
            // Use source.id for uniqueness, not object_id (since we want per-source coverage)
            if !seen_keys.contains(&format!("{}:{}", source.id, key)) {
                entry.0 += 1;
                seen_keys.insert(format!("{}:{}", source.id, key));
            }
        }
    }

    // Filter to only 100% coverage facts
    let mut full_coverage: Vec<(String, FactType, String)> = fact_counts
        .into_iter()
        .filter(|(_, (count, _))| *count == source_count)
        .map(|(key, (_, fact_type))| {
            let description = get_fact_description(&key);
            (key, fact_type, description)
        })
        .collect();

    // Sort by key for consistent output
    full_coverage.sort_by(|a, b| a.0.cmp(&b.0));

    Ok(full_coverage)
}

/// Get a human-readable description for a fact key
fn get_fact_description(key: &str) -> String {
    match key {
        "source.mtime" => "File modification time".to_string(),
        "source.size" => "File size in bytes".to_string(),
        "content.DateTimeOriginal" | "exif.DateTimeOriginal" => "EXIF capture date".to_string(),
        "content.Make" | "exif.Make" => "Camera manufacturer".to_string(),
        "content.Model" | "exif.Model" => "Camera model".to_string(),
        "content.mime" => "MIME type".to_string(),
        "content.width" => "Image width in pixels".to_string(),
        "content.height" => "Image height in pixels".to_string(),
        _ => String::new(),
    }
}

/// Generate fact help comments for the manifest
fn generate_fact_help(sources: &[ManifestSource], full_coverage_facts: &[(String, FactType, String)]) -> String {
    if sources.is_empty() {
        return String::new();
    }

    let mut help = String::new();
    help.push_str(&format!("# Available facts for pattern (100% coverage on {} sources in this cluster):\n", sources.len()));
    help.push_str("#\n");

    // Built-in/derived facts
    help.push_str("# Built-in:\n");
    help.push_str("#   source.rel_path    path   - Relative path from root\n");
    help.push_str("#   source.path        path   - Full absolute path (derived)\n");
    help.push_str("#   source.root        path   - Root path\n");
    help.push_str("#   source.id          num    - Source ID\n");
    help.push_str("#   object.hash        text   - Content hash (if hashed)\n");
    help.push_str("#\n");

    // User facts with 100% coverage
    if !full_coverage_facts.is_empty() {
        help.push_str("# Content facts:\n");
        for (key, fact_type, description) in full_coverage_facts {
            let desc_part = if description.is_empty() {
                String::new()
            } else {
                format!(" - {}", description)
            };
            help.push_str(&format!("#   {:30} {:6}{}\n", key, fact_type.as_str(), desc_part));
        }
        help.push_str("#\n");
    }

    // Modifiers reference
    help.push_str("# Modifiers:\n");
    help.push_str("#   Time: |year |month |day |hour |minute |second |date |datetime |yearmonth |week |weekday |quarter\n");
    help.push_str("#   String: |stem |ext |short\n");
    help.push_str("#   Path: [0] [-1] [1:3] etc.\n");
    help.push_str("#\n");

    // Aliases
    help.push_str("# Aliases:\n");
    help.push_str("#   {filename}    → {source.rel_path[-1]}\n");
    help.push_str("#   {stem}        → {source.rel_path[-1]|stem}\n");
    help.push_str("#   {ext}         → {source.rel_path[-1]|ext}\n");
    help.push_str("#   {hash}        → {object.hash}\n");
    help.push_str("#   {hash_short}  → {object.hash|short}\n");
    help.push_str("#   {id}          → {source.id}\n");
    help.push_str("\n");

    help
}
