use anyhow::{bail, Context, Result};
pub use rusqlite::Connection;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fs;
use std::ops::Deref;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Query record for profiling
#[derive(Clone)]
struct QueryRecord {
    sql: String,
    duration_ms: f64,
}

// Thread-local storage for profiling data
thread_local! {
    static PROFILE_DATA: RefCell<Vec<QueryRecord>> = const { RefCell::new(Vec::new()) };
    static PROFILE_ENABLED: RefCell<bool> = const { RefCell::new(false) };
}

/// Database context that wraps a Connection
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

    /// Check if ANALYZE should be run based on time since last analyze.
    /// If stale (>24 hours), runs ANALYZE and updates metadata.
    pub fn maybe_analyze(&self) -> Result<bool> {
        if self.needs_analyze()? {
            self.run_analyze()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Check if ANALYZE statistics are stale (older than threshold)
    pub fn needs_analyze(&self) -> Result<bool> {
        let last_analyze: Option<i64> = self
            .conn
            .query_row(
                "SELECT value_int FROM _meta WHERE key = 'last_analyze_at'",
                [],
                |row| row.get(0),
            )
            .ok();

        match last_analyze {
            None => Ok(true), // Never analyzed
            Some(timestamp) => {
                let now = current_timestamp();
                Ok(now - timestamp > ANALYZE_TIME_THRESHOLD_SECS)
            }
        }
    }

    /// Run ANALYZE and record the timestamp
    pub fn run_analyze(&self) -> Result<()> {
        self.conn.execute("ANALYZE", [])?;

        let now = current_timestamp();
        self.conn.execute(
            "INSERT OR REPLACE INTO _meta (key, value_int) VALUES ('last_analyze_at', ?)",
            [now],
        )?;

        Ok(())
    }
}

fn current_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs() as i64
}

impl Deref for Db {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}

#[cfg(test)]
impl Db {
    /// Create a Db wrapper around an existing connection.
    /// For testing only - in production use open_with_options().
    pub fn from_connection(conn: Connection) -> Self {
        Db { conn }
    }
}

const SCHEMA: &str = r#"
-- Roots: scanned folder roots
CREATE TABLE IF NOT EXISTS roots (
    id INTEGER PRIMARY KEY,
    path TEXT NOT NULL UNIQUE,
    role TEXT NOT NULL DEFAULT 'source' CHECK (role IN ('source', 'archive')),
    comment TEXT,
    last_scanned_at INTEGER,
    suspended INTEGER NOT NULL DEFAULT 0
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
    partial_hash TEXT NOT NULL,  -- SHA256 of first 8KB + last 8KB (for integrity validation)
    basis_rev INTEGER NOT NULL DEFAULT 0,
    scanned_at INTEGER NOT NULL,
    last_seen_at INTEGER NOT NULL,
    present INTEGER NOT NULL DEFAULT 1,
    object_id INTEGER REFERENCES objects(id),
    excluded INTEGER NOT NULL DEFAULT 0,  -- 1 if source is excluded from processing
    UNIQUE(root_id, rel_path)
);

-- Objects: unique content by hash
CREATE TABLE IF NOT EXISTS objects (
    id INTEGER PRIMARY KEY,
    hash_type TEXT NOT NULL,
    hash_value TEXT NOT NULL,
    excluded INTEGER NOT NULL DEFAULT 0,  -- 1 if object (and all its sources) is excluded
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
    observed_at INTEGER NOT NULL,
    observed_basis_rev INTEGER,
    CHECK (
        (value_text IS NOT NULL) + (value_num IS NOT NULL) +
        (value_time IS NOT NULL) = 1
    ),
    CHECK (entity_type != 'source' OR observed_basis_rev IS NOT NULL),
    CHECK (entity_type != 'object' OR observed_basis_rev IS NULL)
);

-- Indexes
CREATE INDEX IF NOT EXISTS sources_object_id ON sources(object_id);
CREATE INDEX IF NOT EXISTS sources_excluded ON sources(excluded);
CREATE INDEX IF NOT EXISTS sources_root_present ON sources(root_id, present);
CREATE INDEX IF NOT EXISTS facts_entity ON facts(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS facts_key ON facts(key);
CREATE INDEX IF NOT EXISTS facts_key_entity ON facts(key, entity_type, entity_id);
CREATE UNIQUE INDEX IF NOT EXISTS facts_entity_key_uq ON facts(entity_type, entity_id, key);

-- Metadata for internal tracking
CREATE TABLE IF NOT EXISTS _meta (
    key TEXT PRIMARY KEY,
    value_int INTEGER,
    value_text TEXT
);
"#;

/// Threshold for auto-ANALYZE: seconds since last analyze (24 hours)
const ANALYZE_TIME_THRESHOLD_SECS: i64 = 24 * 60 * 60;

/// Profile callback for SQL debug logging
fn sql_debug_callback(sql: &str, duration: Duration) {
    eprintln!("[SQL {:.1}ms] {}", duration.as_secs_f64() * 1000.0, sql);
}

/// Profile callback that collects timing data
fn sql_profile_callback(sql: &str, duration: Duration) {
    PROFILE_DATA.with(|data| {
        data.borrow_mut().push(QueryRecord {
            sql: sql.to_string(),
            duration_ms: duration.as_secs_f64() * 1000.0,
        });
    });
}

/// Options for database opening
pub struct DbOptions {
    pub debug_sql: bool,
    pub profile: bool,
}

pub fn open_with_options(path: &Path, options: DbOptions) -> Result<Db> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create directory: {}", parent.display()))?;
    }

    let mut conn = Connection::open(path)
        .with_context(|| format!("Failed to open database: {}", path.display()))?;

    // Enable SQL debug logging if debug flag is set
    if options.debug_sql {
        conn.profile(Some(sql_debug_callback));
    } else if options.profile {
        // Enable profiling (collect data for summary)
        PROFILE_ENABLED.with(|enabled| *enabled.borrow_mut() = true);
        conn.profile(Some(sql_profile_callback));
    }

    // Set busy timeout FIRST so subsequent operations (including WAL conversion)
    // can retry if another process holds a lock
    conn.busy_timeout(Duration::from_secs(30))
        .context("Failed to set busy timeout")?;

    // Enable WAL mode for concurrent read/write access
    conn.pragma_update(None, "journal_mode", "WAL")
        .context("Failed to enable WAL mode")?;

    // Verify WAL mode is actually active (pragma_update doesn't check the result;
    // if WAL fails silently, we'd fall back to DELETE mode where concurrent
    // writes fail without invoking the busy handler)
    let journal_mode: String = conn
        .pragma_query_value(None, "journal_mode", |row| row.get(0))
        .context("Failed to query journal mode")?;
    if journal_mode != "wal" {
        bail!(
            "Failed to enable WAL mode (got '{journal_mode}'). \
             The database may be on a filesystem that doesn't support WAL."
        );
    }

    conn.execute_batch(SCHEMA)
        .context("Failed to initialize database schema")?;

    Ok(Db { conn })
}

/// Populate temp_sources table with source IDs using a transaction for efficiency
pub fn populate_temp_sources(conn: &mut Connection, source_ids: &[i64]) -> Result<()> {
    conn.execute(
        "CREATE TEMP TABLE IF NOT EXISTS temp_sources (id INTEGER PRIMARY KEY)",
        [],
    )?;

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

/// Threshold in ms for a query to be considered "slow"
const SLOW_QUERY_THRESHOLD_MS: f64 = 100.0;

/// Number of slowest queries to show in summary
const TOP_SLOW_QUERIES: usize = 10;

/// Print profile summary if profiling was enabled
pub fn print_profile_summary(conn: &Connection) {
    let enabled = PROFILE_ENABLED.with(|e| *e.borrow());
    if !enabled {
        return;
    }

    // Clone data and release borrow before running EXPLAIN queries
    // (EXPLAIN triggers the profile callback which would cause a borrow conflict)
    let (total_queries, total_time_ms, sorted) = PROFILE_DATA.with(|data| {
        let records = data.borrow();
        if records.is_empty() {
            return (0, 0.0, Vec::new());
        }

        // Aggregate by normalized SQL (remove parameter values for grouping)
        let mut aggregated: HashMap<String, (usize, f64, f64, String)> = HashMap::new();
        for record in records.iter() {
            let normalized = normalize_sql(&record.sql);
            let entry = aggregated
                .entry(normalized)
                .or_insert((0, 0.0, 0.0, record.sql.clone()));
            entry.0 += 1; // count
            entry.1 += record.duration_ms; // total time
            if record.duration_ms > entry.2 {
                entry.2 = record.duration_ms; // max time
                entry.3 = record.sql.clone(); // example SQL with max time
            }
        }

        // Calculate totals
        let total_queries = records.len();
        let total_time_ms: f64 = records.iter().map(|r| r.duration_ms).sum();

        // Sort by total time descending
        let mut sorted: Vec<_> = aggregated.into_iter().collect();
        sorted.sort_by(|a, b| b.1 .1.partial_cmp(&a.1 .1).unwrap());

        (total_queries, total_time_ms, sorted)
    });

    if total_queries == 0 {
        return;
    }

    eprintln!("\n{}", "=".repeat(70));
    eprintln!("SQL Profile Summary");
    eprintln!("{}", "=".repeat(70));
    eprintln!(
        "Total: {} queries in {:.1}ms ({:.1}s)",
        total_queries,
        total_time_ms,
        total_time_ms / 1000.0
    );
    eprintln!("Unique query patterns: {}", sorted.len());

    // Show top slow queries
    eprintln!("\nTop {TOP_SLOW_QUERIES} slowest query patterns (by total time):");
    eprintln!("{}", "-".repeat(70));

    for (i, (normalized, (count, total_ms, max_ms, example_sql))) in
        sorted.iter().take(TOP_SLOW_QUERIES).enumerate()
    {
        let avg_ms = total_ms / *count as f64;
        eprintln!(
            "\n{}. [{:.1}ms total, {}x, {:.1}ms avg, {:.1}ms max]",
            i + 1,
            total_ms,
            count,
            avg_ms,
            max_ms
        );

        // Show truncated SQL
        let display_sql = if normalized.len() > 200 {
            format!("{}...", &normalized[..200])
        } else {
            normalized.clone()
        };
        eprintln!("   {display_sql}");

        // Show EXPLAIN QUERY PLAN for slow queries
        if *max_ms >= SLOW_QUERY_THRESHOLD_MS {
            if let Some(plan) = get_query_plan(conn, example_sql) {
                eprintln!("   Query plan:");
                for line in plan.lines() {
                    eprintln!("     {line}");
                }
            }
        }
    }

    eprintln!("\n{}", "=".repeat(70));
}

/// Normalize SQL by replacing literal values with placeholders
fn normalize_sql(sql: &str) -> String {
    let mut result = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            // String literals
            '\'' => {
                result.push('?');
                // Skip until closing quote (handling escaped quotes)
                while let Some(&next) = chars.peek() {
                    chars.next();
                    if next == '\'' {
                        if chars.peek() == Some(&'\'') {
                            chars.next(); // escaped quote
                        } else {
                            break;
                        }
                    }
                }
            }
            // Numbers (but not after letters/underscores - those are identifiers)
            '0'..='9' if !result.ends_with(|c: char| c.is_alphanumeric() || c == '_') => {
                result.push('?');
                // Skip rest of number
                while let Some(&next) = chars.peek() {
                    if next.is_ascii_digit() || next == '.' {
                        chars.next();
                    } else {
                        break;
                    }
                }
            }
            _ => result.push(c),
        }
    }

    result
}

/// Create an in-memory database with the full canon schema for testing.
///
/// This uses the same SCHEMA as production, ensuring tests validate against
/// the real schema rather than a potentially stale copy.
#[cfg(test)]
pub fn open_in_memory_for_test() -> Connection {
    let conn = Connection::open_in_memory().expect("Failed to open in-memory database");
    conn.execute_batch(SCHEMA)
        .expect("Failed to initialize test database schema");
    conn
}

/// Get EXPLAIN QUERY PLAN output for a SQL statement
fn get_query_plan(conn: &Connection, sql: &str) -> Option<String> {
    // Skip non-SELECT statements and statements with temp tables
    // (temp tables won't exist when we run EXPLAIN)
    let sql_upper = sql.to_uppercase();
    if !sql_upper.starts_with("SELECT") || sql_upper.contains("TEMP_SOURCES") {
        return None;
    }

    let explain_sql = format!("EXPLAIN QUERY PLAN {sql}");
    let mut stmt = conn.prepare(&explain_sql).ok()?;
    let rows: Vec<String> = stmt
        .query_map([], |row| {
            let detail: String = row.get(3)?;
            Ok(detail)
        })
        .ok()?
        .filter_map(|r| r.ok())
        .collect();

    if rows.is_empty() {
        None
    } else {
        Some(rows.join("\n"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn open_with_options_enables_wal_mode() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let db = open_with_options(
            &db_path,
            DbOptions {
                debug_sql: false,
                profile: false,
            },
        )
        .unwrap();

        let mode: String = db
            .conn()
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .unwrap();
        assert_eq!(mode, "wal");
    }
}
