use anyhow::{bail, Result};
use rusqlite::{params, Connection};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CompareOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}

#[derive(Debug, Clone)]
pub enum Filter {
    Exists { key: String },
    NotExists { key: String },
    Compare { key: String, op: CompareOp, value: String },
}

impl Filter {
    pub fn parse(s: &str) -> Result<Self> {
        // Check for negation prefix (only for existence checks)
        if let Some(rest) = s.strip_prefix('!') {
            if let Some(key) = rest.strip_suffix('?') {
                return Ok(Filter::NotExists { key: key.to_string() });
            }
            // !key=value is shorthand for key!=value
            if let Some((key, value)) = rest.split_once('=') {
                return Ok(Filter::Compare {
                    key: key.to_string(),
                    op: CompareOp::Ne,
                    value: value.to_string(),
                });
            }
            bail!("Invalid negated filter: !{}. Use '!key?' or '!key=value'", rest);
        }

        // Existence check
        if let Some(key) = s.strip_suffix('?') {
            return Ok(Filter::Exists { key: key.to_string() });
        }

        // Check multi-char operators first (>=, <=, !=)
        if let Some((key, value)) = s.split_once(">=") {
            return Ok(Filter::Compare {
                key: key.to_string(),
                op: CompareOp::Ge,
                value: value.to_string(),
            });
        }

        if let Some((key, value)) = s.split_once("<=") {
            return Ok(Filter::Compare {
                key: key.to_string(),
                op: CompareOp::Le,
                value: value.to_string(),
            });
        }

        if let Some((key, value)) = s.split_once("!=") {
            return Ok(Filter::Compare {
                key: key.to_string(),
                op: CompareOp::Ne,
                value: value.to_string(),
            });
        }

        // Single-char operators (>, <, =)
        if let Some((key, value)) = s.split_once('>') {
            return Ok(Filter::Compare {
                key: key.to_string(),
                op: CompareOp::Gt,
                value: value.to_string(),
            });
        }

        if let Some((key, value)) = s.split_once('<') {
            return Ok(Filter::Compare {
                key: key.to_string(),
                op: CompareOp::Lt,
                value: value.to_string(),
            });
        }

        if let Some((key, value)) = s.split_once('=') {
            return Ok(Filter::Compare {
                key: key.to_string(),
                op: CompareOp::Eq,
                value: value.to_string(),
            });
        }

        bail!(
            "Invalid filter syntax: {}. Use 'key?', '!key?', 'key=value', 'key!=value', 'key>value', 'key>=value', 'key<value', or 'key<=value'",
            s
        );
    }
}

/// Parse a filter value string into a numeric value for comparison.
/// Supports: integers, floats, and dates (ISO 8601, EXIF format).
fn parse_filter_value(value: &str) -> Option<f64> {
    // Try as number first
    if let Ok(n) = value.parse::<f64>() {
        return Some(n);
    }

    // Try date formats - convert to Unix timestamp
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(value) {
        return Some(dt.timestamp() as f64);
    }
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S") {
        return Some(dt.and_utc().timestamp() as f64);
    }
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(value, "%Y:%m:%d %H:%M:%S") {
        return Some(dt.and_utc().timestamp() as f64);
    }
    if let Ok(date) = chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d") {
        return Some(date.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp() as f64);
    }

    None
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
            Filter::Compare { key, op, value } => check_fact_compare(conn, source_id, key, *op, value)?,
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

fn check_fact_compare(conn: &Connection, source_id: i64, key: &str, op: CompareOp, value: &str) -> Result<bool> {
    // Handle built-in source.* fields first
    match key {
        // Text fields - only support = and !=
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
            return Ok(compare_text(ext, op, value));
        }
        "source.root" => {
            let root_path: String = conn.query_row(
                "SELECT r.path FROM sources s JOIN roots r ON s.root_id = r.id WHERE s.id = ?",
                [source_id],
                |row| row.get(0),
            )?;
            return Ok(compare_text(&root_path, op, value));
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
            return Ok(compare_text(&full_path, op, value));
        }
        "source.rel_path" => {
            let rel_path: String = conn.query_row(
                "SELECT rel_path FROM sources WHERE id = ?",
                [source_id],
                |row| row.get(0),
            )?;
            return Ok(compare_text(&rel_path, op, value));
        }

        // Numeric fields - support all comparison operators
        "source.size" | "size" => {
            let v: i64 = conn.query_row(
                "SELECT size FROM sources WHERE id = ?",
                [source_id],
                |row| row.get(0),
            )?;
            return Ok(compare_numeric(v as f64, op, value));
        }
        "source.mtime" | "mtime" => {
            let v: i64 = conn.query_row(
                "SELECT mtime FROM sources WHERE id = ?",
                [source_id],
                |row| row.get(0),
            )?;
            return Ok(compare_numeric(v as f64, op, value));
        }
        "source.device" => {
            let device: Option<i64> = conn.query_row(
                "SELECT device FROM sources WHERE id = ?",
                [source_id],
                |row| row.get(0),
            )?;
            return Ok(device.map(|d| compare_numeric(d as f64, op, value)).unwrap_or(false));
        }
        "source.inode" => {
            let inode: Option<i64> = conn.query_row(
                "SELECT inode FROM sources WHERE id = ?",
                [source_id],
                |row| row.get(0),
            )?;
            return Ok(inode.map(|i| compare_numeric(i as f64, op, value)).unwrap_or(false));
        }
        // Legacy
        "root_id" => {
            let v: i64 = conn.query_row(
                "SELECT root_id FROM sources WHERE id = ?",
                [source_id],
                |row| row.get(0),
            )?;
            return Ok(compare_numeric(v as f64, op, value));
        }
        _ => {}
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
    // Try to get the fact value (checking value_text, value_num, value_time)
    if let Some(fact_value) = get_fact_value(conn, "source", source_id, key)? {
        if compare_fact_value(&fact_value, op, value) {
            return Ok(true);
        }
    }

    if let Some(obj_id) = object_id {
        if let Some(fact_value) = get_fact_value(conn, "object", obj_id, key)? {
            if compare_fact_value(&fact_value, op, value) {
                return Ok(true);
            }
        }
    }

    Ok(false)
}

/// Stored fact value - can be text, number, or timestamp
enum FactValue {
    Text(String),
    Num(f64),
    Time(i64),
}

fn get_fact_value(conn: &Connection, entity_type: &str, entity_id: i64, key: &str) -> Result<Option<FactValue>> {
    let result: Option<(Option<String>, Option<f64>, Option<i64>)> = conn
        .query_row(
            "SELECT value_text, value_num, value_time FROM facts
             WHERE entity_type = ? AND entity_id = ? AND key = ?",
            params![entity_type, entity_id, key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .ok();

    Ok(result.and_then(|(text, num, time)| {
        if let Some(t) = text {
            Some(FactValue::Text(t))
        } else if let Some(n) = num {
            Some(FactValue::Num(n))
        } else if let Some(ts) = time {
            Some(FactValue::Time(ts))
        } else {
            None
        }
    }))
}

fn compare_fact_value(fact: &FactValue, op: CompareOp, filter_value: &str) -> bool {
    match fact {
        FactValue::Text(t) => compare_text(t, op, filter_value),
        FactValue::Num(n) => compare_numeric(*n, op, filter_value),
        FactValue::Time(ts) => compare_numeric(*ts as f64, op, filter_value),
    }
}

fn compare_text(stored: &str, op: CompareOp, filter_value: &str) -> bool {
    match op {
        CompareOp::Eq => stored.eq_ignore_ascii_case(filter_value),
        CompareOp::Ne => !stored.eq_ignore_ascii_case(filter_value),
        // For text, > < etc do lexicographic comparison
        CompareOp::Gt => stored > filter_value,
        CompareOp::Ge => stored >= filter_value,
        CompareOp::Lt => stored < filter_value,
        CompareOp::Le => stored <= filter_value,
    }
}

fn compare_numeric(stored: f64, op: CompareOp, filter_value: &str) -> bool {
    // Parse filter value - could be number or date
    let filter_num = match parse_filter_value(filter_value) {
        Some(n) => n,
        None => return false, // Can't compare if filter value isn't numeric/date
    };

    match op {
        CompareOp::Eq => (stored - filter_num).abs() < f64::EPSILON,
        CompareOp::Ne => (stored - filter_num).abs() >= f64::EPSILON,
        CompareOp::Gt => stored > filter_num,
        CompareOp::Ge => stored >= filter_num,
        CompareOp::Lt => stored < filter_num,
        CompareOp::Le => stored <= filter_num,
    }
}
