use anyhow::{bail, Context, Result};
pub use rusqlite::Connection;
use std::fs;
use std::ops::Deref;
use std::path::Path;
use std::time::Duration;

/// Database context that wraps a Connection with optional SQL debug logging
pub struct Db {
    conn: Connection,
}

impl Db {
    /// Get a reference to the underlying connection
    pub fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Get a mutable reference to the underlying connection (for transactions)
    pub fn conn_mut(&mut self) -> &mut Connection {
        &mut self.conn
    }
}

impl Deref for Db {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}

const SCHEMA: &str = r#"
-- Roots: scanned folder roots
CREATE TABLE IF NOT EXISTS roots (
    id INTEGER PRIMARY KEY,
    path TEXT NOT NULL UNIQUE,
    role TEXT NOT NULL DEFAULT 'source' CHECK (role IN ('source', 'archive'))
);

-- Sources: files discovered on disk
CREATE TABLE IF NOT EXISTS sources (
    id INTEGER PRIMARY KEY,
    root_id INTEGER NOT NULL REFERENCES roots(id),
    rel_path TEXT NOT NULL,
    device INTEGER,
    inode INTEGER,
    size INTEGER NOT NULL,
    mtime INTEGER NOT NULL,
    basis_rev INTEGER NOT NULL DEFAULT 0,
    scanned_at INTEGER NOT NULL,
    last_seen_at INTEGER NOT NULL,
    present INTEGER NOT NULL DEFAULT 1,
    object_id INTEGER REFERENCES objects(id),
    UNIQUE(root_id, rel_path)
);

-- Objects: unique content by hash
CREATE TABLE IF NOT EXISTS objects (
    id INTEGER PRIMARY KEY,
    hash_type TEXT NOT NULL,
    hash_value TEXT NOT NULL,
    UNIQUE(hash_type, hash_value)
);

-- Facts: EAV table with typed values
CREATE TABLE IF NOT EXISTS facts (
    id INTEGER PRIMARY KEY,
    entity_type TEXT NOT NULL CHECK (entity_type IN ('source', 'object')),
    entity_id INTEGER NOT NULL,
    key TEXT NOT NULL,
    value_text TEXT,
    value_num REAL,
    value_time INTEGER,
    value_json TEXT,
    observed_at INTEGER NOT NULL,
    observed_basis_rev INTEGER,
    CHECK (
        (value_text IS NOT NULL) + (value_num IS NOT NULL) +
        (value_time IS NOT NULL) + (value_json IS NOT NULL) = 1
    ),
    CHECK (entity_type != 'source' OR observed_basis_rev IS NOT NULL),
    CHECK (entity_type != 'object' OR observed_basis_rev IS NULL)
);

-- Indexes
CREATE UNIQUE INDEX IF NOT EXISTS sources_device_inode_uq ON sources(device, inode)
    WHERE device IS NOT NULL AND inode IS NOT NULL;
CREATE INDEX IF NOT EXISTS sources_object_id ON sources(object_id);
CREATE INDEX IF NOT EXISTS facts_entity ON facts(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS facts_key ON facts(key);
CREATE INDEX IF NOT EXISTS facts_key_entity ON facts(key, entity_type, entity_id);
CREATE UNIQUE INDEX IF NOT EXISTS facts_entity_key_uq ON facts(entity_type, entity_id, key);
"#;

/// Profile callback for SQL debug logging
fn sql_profile_callback(sql: &str, duration: Duration) {
    eprintln!("[SQL {:.1}ms] {}", duration.as_secs_f64() * 1000.0, sql);
}

pub fn open(path: &Path, debug_sql: bool) -> Result<Db> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create directory: {}", parent.display()))?;
    }

    let mut conn = Connection::open(path)
        .with_context(|| format!("Failed to open database: {}", path.display()))?;

    // Enable SQL profiling if debug flag is set
    if debug_sql {
        conn.profile(Some(sql_profile_callback));
    }

    // Enable WAL mode for concurrent read/write access
    conn.pragma_update(None, "journal_mode", "WAL")
        .context("Failed to enable WAL mode")?;

    // Set busy timeout to 30 seconds (retry instead of failing immediately)
    conn.busy_timeout(Duration::from_secs(30))
        .context("Failed to set busy timeout")?;

    conn.execute_batch(SCHEMA)
        .context("Failed to initialize database schema")?;

    Ok(Db { conn })
}

/// Populate temp_sources table with source IDs using a transaction for efficiency
pub fn populate_temp_sources(conn: &mut Connection, source_ids: &[i64]) -> Result<()> {
    conn.execute("CREATE TEMP TABLE IF NOT EXISTS temp_sources (id INTEGER PRIMARY KEY)", [])?;

    let tx = conn.transaction()?;
    tx.execute("DELETE FROM temp_sources", [])?;
    {
        let mut stmt = tx.prepare("INSERT INTO temp_sources (id) VALUES (?)")?;
        for id in source_ids {
            stmt.execute([id])?;
        }
    }
    tx.commit()?;
    Ok(())
}

/// Parse root spec (id:N or path:/path) with optional role validation
pub fn parse_root_spec(conn: &Connection, spec: &str, required_role: Option<&str>) -> Result<i64> {
    let (id, role) = if let Some(id_str) = spec.strip_prefix("id:") {
        let id: i64 = id_str.parse().context("Invalid root ID")?;
        let role: String = conn
            .query_row("SELECT role FROM roots WHERE id = ?", [id], |row| row.get(0))
            .with_context(|| format!("No root with id {}", id))?;
        (id, role)
    } else if let Some(path) = spec.strip_prefix("path:") {
        let realpath = fs::canonicalize(path)
            .with_context(|| format!("Failed to resolve path: {}", path))?;
        let realpath_str = realpath
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("Path contains invalid UTF-8"))?;
        let (id, role): (i64, String) = conn
            .query_row(
                "SELECT id, role FROM roots WHERE path = ?",
                [realpath_str],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .with_context(|| format!("No root for path: {}", path))?;
        (id, role)
    } else {
        bail!("Invalid format '{}'. Use id:<N> or path:<path>", spec);
    };

    if let Some(req_role) = required_role {
        if role != req_role {
            bail!("Root {} has role '{}', expected '{}'", id, role, req_role);
        }
    }
    Ok(id)
}

/// Resolve a path to its containing archive root and relative subdir.
/// Unlike parse_root_spec which requires exact root match, this accepts any path
/// inside an archive root and extracts the relative portion.
/// Returns (root_id, root_path, relative_subdir) or error if not in an archive.
pub fn resolve_archive_path(conn: &Connection, path: &Path) -> Result<(i64, String, String)> {
    let canon_path = fs::canonicalize(path)
        .with_context(|| format!("Failed to resolve path: {}", path.display()))?;
    let path_str = canon_path
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("Path contains invalid UTF-8"))?;

    let mut stmt = conn.prepare("SELECT id, path FROM roots WHERE role = 'archive'")?;
    let archives: Vec<(i64, String)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<Vec<_>, _>>()?;

    for (id, archive_path) in archives {
        if path_str == archive_path {
            return Ok((id, archive_path, String::new()));
        }
        if let Some(rel) = path_str.strip_prefix(&format!("{}/", archive_path)) {
            return Ok((id, archive_path, rel.to_string()));
        }
    }

    bail!(
        "Path '{}' is not inside any registered archive root",
        path.display()
    );
}
