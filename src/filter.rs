use anyhow::{bail, Result};
use rusqlite::{params, Connection};

#[derive(Debug, Clone)]
pub enum Filter {
    Exists { key: String },
    NotExists { key: String },
    Equals { key: String, value: String },
    NotEquals { key: String, value: String },
}

impl Filter {
    pub fn parse(s: &str) -> Result<Self> {
        // Check for negation prefix
        if let Some(rest) = s.strip_prefix('!') {
            if let Some(key) = rest.strip_suffix('?') {
                return Ok(Filter::NotExists { key: key.to_string() });
            }
            if let Some((key, value)) = rest.split_once('=') {
                return Ok(Filter::NotEquals {
                    key: key.to_string(),
                    value: value.to_string(),
                });
            }
            bail!("Invalid negated filter: !{}. Use '!key?' or '!key=value'", rest);
        }

        // Non-negated filters
        if let Some(key) = s.strip_suffix('?') {
            return Ok(Filter::Exists { key: key.to_string() });
        }

        // Check for != before = to avoid matching the = in !=
        if let Some((key, value)) = s.split_once("!=") {
            return Ok(Filter::NotEquals {
                key: key.to_string(),
                value: value.to_string(),
            });
        }

        if let Some((key, value)) = s.split_once('=') {
            return Ok(Filter::Equals {
                key: key.to_string(),
                value: value.to_string(),
            });
        }

        bail!(
            "Invalid filter syntax: {}. Use 'key?' for existence, '!key?' for non-existence, 'key=value' for equality, or 'key!=value' for inequality",
            s
        );
    }
}

/// Apply a list of filters to a set of source IDs (AND logic)
pub fn apply_filters(conn: &Connection, source_ids: &[i64], filters: &[Filter]) -> Result<Vec<i64>> {
    let mut result = source_ids.to_vec();

    for filter in filters {
        result = apply_filter(conn, &result, filter)?;
    }

    Ok(result)
}

fn apply_filter(conn: &Connection, source_ids: &[i64], filter: &Filter) -> Result<Vec<i64>> {
    let mut result = Vec::new();

    for &source_id in source_ids {
        let matches = match filter {
            Filter::Exists { key } => check_fact_exists(conn, source_id, key)?,
            Filter::NotExists { key } => !check_fact_exists(conn, source_id, key)?,
            Filter::Equals { key, value } => check_fact_equals(conn, source_id, key, value)?,
            Filter::NotEquals { key, value } => !check_fact_equals(conn, source_id, key, value)?,
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

    // Special case: check for built-in source.* fields
    match key {
        // New source.* namespace
        "source.ext" | "source.size" | "source.mtime" | "source.path" |
        "source.root" | "source.rel_path" | "source.device" | "source.inode" => Ok(true),
        // New content.* namespace - hash existence means object exists
        "content.hash.sha256" => Ok(object_id.is_some()),
        // Legacy names (backwards compatibility)
        "ext" | "size" | "mtime" | "root_id" | "basis_rev" | "object_id" => Ok(true),
        "hash" | "content_hash" | "content_hash.sha256" => Ok(object_id.is_some()),
        _ => Ok(false),
    }
}

fn check_fact_equals(conn: &Connection, source_id: i64, key: &str, value: &str) -> Result<bool> {
    // Handle built-in source.* fields first
    match key {
        // New source.* namespace
        "source.ext" | "ext" => {
            let rel_path: String = conn.query_row(
                "SELECT rel_path FROM sources WHERE id = ?",
                [source_id],
                |row| row.get(0),
            )?;
            let ext = std::path::Path::new(&rel_path)
                .extension()
                .and_then(|e| e.to_str())
                .unwrap_or("");
            return Ok(ext.eq_ignore_ascii_case(value));
        }
        "source.size" | "size" => {
            let v: i64 = conn.query_row(
                "SELECT size FROM sources WHERE id = ?",
                [source_id],
                |row| row.get(0),
            )?;
            return Ok(v.to_string() == value);
        }
        "source.mtime" | "mtime" => {
            let v: i64 = conn.query_row(
                "SELECT mtime FROM sources WHERE id = ?",
                [source_id],
                |row| row.get(0),
            )?;
            return Ok(v.to_string() == value);
        }
        "source.root" => {
            let root_path: String = conn.query_row(
                "SELECT r.path FROM sources s JOIN roots r ON s.root_id = r.id WHERE s.id = ?",
                [source_id],
                |row| row.get(0),
            )?;
            return Ok(root_path == value);
        }
        "source.path" => {
            let (root_path, rel_path): (String, String) = conn.query_row(
                "SELECT r.path, s.rel_path FROM sources s JOIN roots r ON s.root_id = r.id WHERE s.id = ?",
                [source_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            let full_path = if rel_path.is_empty() {
                root_path
            } else {
                format!("{}/{}", root_path, rel_path)
            };
            return Ok(full_path == value);
        }
        "source.rel_path" => {
            let rel_path: String = conn.query_row(
                "SELECT rel_path FROM sources WHERE id = ?",
                [source_id],
                |row| row.get(0),
            )?;
            return Ok(rel_path == value);
        }
        "source.device" => {
            let device: Option<i64> = conn.query_row(
                "SELECT device FROM sources WHERE id = ?",
                [source_id],
                |row| row.get(0),
            )?;
            return Ok(device.map(|d| d.to_string()).unwrap_or_default() == value);
        }
        "source.inode" => {
            let inode: Option<i64> = conn.query_row(
                "SELECT inode FROM sources WHERE id = ?",
                [source_id],
                |row| row.get(0),
            )?;
            return Ok(inode.map(|i| i.to_string()).unwrap_or_default() == value);
        }
        // Legacy
        "root_id" => {
            let v: i64 = conn.query_row(
                "SELECT root_id FROM sources WHERE id = ?",
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
