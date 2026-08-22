use anyhow::{bail, Context, Result};
pub use rusqlite::Connection;
use rusqlite_migration::{HookResult, Migrations, M};
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

/// Baseline schema — migration 1 in [`migrations`]. Frozen at the point schema
/// migrations were adopted; do not edit to change the schema. Add changes as new
/// `M::up` entries in [`migrations`] instead. `IF NOT EXISTS` throughout keeps it
/// a safe no-op on databases created before migrations tracked `user_version`.
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
    decision_id INTEGER,                  -- decision that caused the most recent state transition
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
CREATE INDEX IF NOT EXISTS sources_device_inode ON sources(device, inode) WHERE present = 1;
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

-- Decisions: record of effectful actions taken by the user
CREATE TABLE IF NOT EXISTS decisions (
    id INTEGER PRIMARY KEY,
    command TEXT NOT NULL,
    scope TEXT,                  -- JSON array of path strings, or NULL for global
    command_line TEXT NOT NULL,
    reason TEXT,
    status TEXT NOT NULL DEFAULT 'started',
    count_attempted INTEGER,
    count_completed INTEGER,
    count_failed INTEGER,
    count_skipped INTEGER,
    summary TEXT,
    canon_version TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    receipt_root_id INTEGER,     -- references roots table (receipt storage location)
    receipt_rel_path TEXT        -- relative path within receipt root (always the .toml name)
);
CREATE INDEX IF NOT EXISTS decisions_command ON decisions(command);
CREATE INDEX IF NOT EXISTS decisions_created_at ON decisions(created_at);

-- Decision scopes: durable root-based scope index for future consumption
CREATE TABLE IF NOT EXISTS decision_scopes (
    decision_id INTEGER NOT NULL,
    root_id INTEGER NOT NULL,
    rel_prefix TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS decision_scopes_decision_id ON decision_scopes(decision_id);
CREATE INDEX IF NOT EXISTS decision_scopes_root_id ON decision_scopes(root_id);

-- Notes: timestamped annotations on locations within roots
CREATE TABLE IF NOT EXISTS notes (
    id INTEGER PRIMARY KEY,
    root_id INTEGER NOT NULL REFERENCES roots(id),
    rel_path TEXT NOT NULL DEFAULT '',
    text TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS notes_root_path ON notes(root_id, rel_path);
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

    migrations()
        .to_latest(&mut conn)
        .context("Failed to apply database schema migrations")?;

    Ok(Db { conn })
}

/// Ordered schema migrations, applied by `PRAGMA user_version` (rusqlite_migration).
///
/// Migration 1 is the baseline schema ([`SCHEMA`]); each later entry is a delta,
/// e.g. a column added to an existing table. **Append only — never reorder or edit
/// an already-released migration.** Progress is tracked solely by `user_version`,
/// so rewriting history would desync existing databases. Because the baseline uses
/// `IF NOT EXISTS`, databases created before adoption (which have `user_version = 0`
/// but the tables already present) upgrade cleanly: migration 1 no-ops, later
/// deltas apply.
fn migrations() -> Migrations<'static> {
    Migrations::new(vec![
        // 1: baseline schema.
        M::up(SCHEMA),
        // 2: per-root receipt index — one decision can emit several source-local
        //    receipts (a scan deleting across roots), which the single
        //    decisions.receipt_* columns can't hold.
        M::up("ALTER TABLE decision_scopes ADD COLUMN receipt_rel_path TEXT;"),
        // 3: backfill columns added in a0353f4 (after v0.4.1) that were folded
        //    into the migration-1 baseline before the migration system existed.
        //    Databases created before a0353f4 have sources/decisions tables
        //    already present but missing these columns; migration-1's
        //    CREATE TABLE IF NOT EXISTS no-ops on them. The hook checks existence
        //    before each ALTER so it is safe on fresh databases where migration-1
        //    already created the columns.
        M::up_with_hook("SELECT 1", |tx: &rusqlite::Transaction| -> HookResult {
            for (table, col, def) in [
                ("sources", "decision_id", "INTEGER"),
                ("decisions", "receipt_root_id", "INTEGER"),
                ("decisions", "receipt_rel_path", "TEXT"),
            ] {
                let has_col: bool = {
                    let mut stmt = tx.prepare(&format!("PRAGMA table_info({table})"))?;
                    let cols: Vec<String> = stmt
                        .query_map([], |row| row.get::<_, String>(1))?
                        .filter_map(|r| r.ok())
                        .collect();
                    cols.iter().any(|name| name == col)
                };
                if !has_col {
                    tx.execute_batch(&format!("ALTER TABLE {table} ADD COLUMN {col} {def}"))?;
                }
            }
            Ok(())
        }),
        // 4: extraction ledger — the trail's outbound direction. One row per
        //    (decision, drawn source root); aggregate only, never per-item
        //    (per-item detail stays in the apply receipt on disk). The PK
        //    doubles as the upsert key for both forward recording and
        //    `ledger reindex` backfill.
        M::up(
            "CREATE TABLE IF NOT EXISTS decision_extractions (
                decision_id INTEGER NOT NULL,
                root_id INTEGER NOT NULL,
                root_path TEXT NOT NULL,
                rel_prefix TEXT NOT NULL DEFAULT '',
                files INTEGER NOT NULL,
                bytes INTEGER,
                destination_root_id INTEGER,
                destination_path TEXT NOT NULL,
                disposition TEXT,
                PRIMARY KEY (decision_id, root_id)
             );
             CREATE INDEX IF NOT EXISTS decision_extractions_root_id
                 ON decision_extractions(root_id);",
        ),
        // 5: extraction rows at directory precision — one row per (decision,
        //    source root, origin dir, destination dir), so the PK widens.
        //    Same columns, same meaning per row (all counted files lie under
        //    the recorded locations); existing rows carry over unchanged as
        //    coarse rows and `ledger reindex` upgrades them where receipts
        //    exist. SQLite can't alter a PK in place, so: rebuild + rename.
        M::up(
            "CREATE TABLE decision_extractions_precise (
                decision_id INTEGER NOT NULL,
                root_id INTEGER NOT NULL,
                root_path TEXT NOT NULL,
                rel_prefix TEXT NOT NULL DEFAULT '',
                files INTEGER NOT NULL,
                bytes INTEGER,
                destination_root_id INTEGER,
                destination_path TEXT NOT NULL,
                disposition TEXT,
                PRIMARY KEY (decision_id, root_id, rel_prefix, destination_path)
             );
             INSERT INTO decision_extractions_precise
                 SELECT decision_id, root_id, root_path, rel_prefix, files, bytes,
                        destination_root_id, destination_path, disposition
                 FROM decision_extractions;
             DROP TABLE decision_extractions;
             ALTER TABLE decision_extractions_precise RENAME TO decision_extractions;
             CREATE INDEX decision_extractions_root_id
                 ON decision_extractions(root_id);",
        ),
        // 6: root-path snapshot on the scope index (same precedent as
        //    decision_extractions.root_path) — keeps scope rows renderable
        //    after their root is removed. The SQL backfills rows whose root
        //    still lives; the hook recovers already-dangling rows from the
        //    decision's scope display strings (a JSON array of
        //    root_path/rel_prefix joins — the row's known rel_prefix strips
        //    deterministically). NULL-over-guess: ambiguous or unmatched rows
        //    stay NULL and render as a marked removed-root fallback.
        M::up_with_hook(
            "ALTER TABLE decision_scopes ADD COLUMN root_path TEXT;
             UPDATE decision_scopes
                 SET root_path = (SELECT path FROM roots WHERE roots.id = decision_scopes.root_id)
                 WHERE root_path IS NULL;",
            |tx: &rusqlite::Transaction| -> HookResult {
                let mut stmt = tx.prepare(
                    "SELECT ds.decision_id, ds.root_id, ds.rel_prefix, d.scope
                     FROM decision_scopes ds JOIN decisions d ON d.id = ds.decision_id
                     WHERE ds.root_path IS NULL AND d.scope IS NOT NULL",
                )?;
                let dangling: Vec<(i64, i64, String, String)> = stmt
                    .query_map([], |row| {
                        Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                    })?
                    .filter_map(|r| r.ok())
                    .collect();
                for (decision_id, root_id, rel_prefix, scope_json) in dangling {
                    let candidates: Vec<String> =
                        serde_json::from_str(&scope_json).unwrap_or_default();
                    if let Some(root_path) =
                        crate::core::domain::scope::recover_root_path(&candidates, &rel_prefix)
                    {
                        tx.execute(
                            "UPDATE decision_scopes SET root_path = ?4
                             WHERE decision_id = ?1 AND root_id = ?2 AND rel_prefix = ?3
                               AND root_path IS NULL",
                            rusqlite::params![decision_id, root_id, rel_prefix, root_path],
                        )?;
                    }
                }
                Ok(())
            },
        ),
        // 7: inode-only nomination index. Move detection looks a file up by
        //    inode alone — a remount renumbers the device, so a device-leading
        //    key cannot serve the lookup at all. The old (device, inode) index
        //    stays: released schema is never edited, and it still serves
        //    device-qualified reads.
        M::up(
            "CREATE INDEX IF NOT EXISTS sources_inode
                 ON sources(inode) WHERE present = 1;",
        ),
    ])
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

/// The path-boundary membership predicate: the path itself, or anything under
/// "{path}/". String comparison, never LIKE — '_' and '%' in a real path must
/// match literally, not as wildcards (a scope of "2023_06" must never reach
/// "2023X06"). The returned clause binds the prefix THREE times, in order.
///
/// This is the one place the boundary is spelled; every path-scoped SQL query
/// routes through it or its strictly-under sibling.
pub fn path_at_or_under_sql(column: &str) -> String {
    format!("({column} = ? OR substr({column}, 1, length(?) + 1) = ? || '/')")
}

/// Strictly-under variant of [`path_at_or_under_sql`]: anything under
/// "{path}/", the path itself excluded. Binds the prefix TWICE.
pub fn path_strictly_under_sql(column: &str) -> String {
    format!("substr({column}, 1, length(?) + 1) = ? || '/'")
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
    let mut conn = Connection::open_in_memory().expect("Failed to open in-memory database");
    migrations()
        .to_latest(&mut conn)
        .expect("Failed to apply migrations to test database");
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

    /// Evaluate the at-or-under predicate for one (path, prefix) pair.
    fn at_or_under(conn: &Connection, path: &str, prefix: &str) -> bool {
        // The column slot is itself a placeholder here, so the clause reads
        // (path = prefix OR substr(path, 1, length(prefix) + 1) = prefix || '/').
        let sql = format!("SELECT {}", path_at_or_under_sql("?"));
        conn.query_row(
            &sql,
            rusqlite::params![path, prefix, path, prefix, prefix],
            |row| row.get(0),
        )
        .unwrap()
    }

    /// Evaluate the strictly-under predicate for one (path, prefix) pair.
    fn strictly_under(conn: &Connection, path: &str, prefix: &str) -> bool {
        let sql = format!("SELECT {}", path_strictly_under_sql("?"));
        conn.query_row(&sql, rusqlite::params![path, prefix, prefix], |row| {
            row.get(0)
        })
        .unwrap()
    }

    #[test]
    fn path_at_or_under_matches_self_and_descendants() {
        let conn = Connection::open_in_memory().unwrap();
        assert!(at_or_under(&conn, "alpha", "alpha"));
        assert!(at_or_under(&conn, "alpha/a.jpg", "alpha"));
        assert!(at_or_under(&conn, "alpha/deep/b.jpg", "alpha"));
    }

    #[test]
    fn path_at_or_under_stops_at_the_separator() {
        let conn = Connection::open_in_memory().unwrap();
        assert!(!at_or_under(&conn, "alpha-beta/a.jpg", "alpha"));
        assert!(!at_or_under(&conn, "alphabet", "alpha"));
    }

    #[test]
    fn path_at_or_under_treats_wildcard_bytes_literally() {
        let conn = Connection::open_in_memory().unwrap();
        // '_' and '%' are LIKE wildcards; a path must never match through them.
        assert!(!at_or_under(&conn, "alphaXbeta/a.jpg", "alpha_beta"));
        assert!(at_or_under(&conn, "alpha_beta/a.jpg", "alpha_beta"));
        assert!(!at_or_under(&conn, "pct100/a.jpg", "pct%"));
        assert!(at_or_under(&conn, "pct%/a.jpg", "pct%"));
    }

    #[test]
    fn path_strictly_under_excludes_the_path_itself() {
        let conn = Connection::open_in_memory().unwrap();
        assert!(!strictly_under(&conn, "alpha", "alpha"));
        assert!(strictly_under(&conn, "alpha/a.jpg", "alpha"));
        assert!(!strictly_under(&conn, "alphaXbeta/a.jpg", "alpha_beta"));
    }

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

    fn has_column(conn: &Connection, table: &str, column: &str) -> bool {
        let mut stmt = conn
            .prepare(&format!("PRAGMA table_info({table})"))
            .unwrap();
        let names: Vec<String> = stmt
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        names.iter().any(|n| n == column)
    }

    #[test]
    fn migrations_are_valid() {
        // rusqlite_migration applies the set to a scratch DB and checks it is
        // internally coherent — catches a reordered or malformed migration.
        migrations().validate().unwrap();
    }

    #[test]
    fn fresh_db_has_decision_scopes_receipt_column() {
        let conn = open_in_memory_for_test();
        assert!(has_column(&conn, "decision_scopes", "receipt_rel_path"));
    }

    #[test]
    fn migrating_legacy_db_adds_receipt_column() {
        // Simulate a database created before migrations tracked user_version:
        // baseline decision_scopes present, no receipt column, user_version 0.
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE decision_scopes (
                 decision_id INTEGER NOT NULL,
                 root_id INTEGER NOT NULL,
                 rel_prefix TEXT NOT NULL DEFAULT ''
             )",
        )
        .unwrap();
        assert!(!has_column(&conn, "decision_scopes", "receipt_rel_path"));

        migrations().to_latest(&mut conn).unwrap();

        // Migration 1's IF NOT EXISTS no-ops the existing table; migration 2 adds
        // the column.
        assert!(has_column(&conn, "decision_scopes", "receipt_rel_path"));
    }

    /// Simulate a pre-snapshot (v5) database: decision_scopes without root_path.
    /// Upgrading must add the column, backfill live-root rows via SQL, and
    /// recover dangling rows from the decision's scope JSON — NULL-over-guess
    /// for the unrecoverable rest.
    #[test]
    fn migrating_v5_backfills_and_recovers_scope_root_paths() {
        let mut conn = Connection::open_in_memory().unwrap();
        migrations().to_version(&mut conn, 5).unwrap();
        assert!(!has_column(&conn, "decision_scopes", "root_path"));

        conn.execute_batch(
            r#"INSERT INTO roots (id, path) VALUES (1, '/vol/live');
               INSERT INTO decisions (id, command, scope, command_line, canon_version, created_at)
                   VALUES (10, 'exclude_set', '["/gone/root/sub"]', 'x', '0', 0);
               INSERT INTO decisions (id, command, scope, command_line, canon_version, created_at)
                   VALUES (11, 'scan', '["/a/sub","/b/sub"]', 'x', '0', 0);
               INSERT INTO decision_scopes (decision_id, root_id, rel_prefix) VALUES (10, 1, '');
               INSERT INTO decision_scopes (decision_id, root_id, rel_prefix) VALUES (10, 99, 'sub');
               INSERT INTO decision_scopes (decision_id, root_id, rel_prefix) VALUES (11, 98, 'sub');"#,
        )
        .unwrap();

        migrations().to_latest(&mut conn).unwrap();

        let get = |root_id: i64| -> Option<String> {
            conn.query_row(
                "SELECT root_path FROM decision_scopes WHERE root_id = ?",
                [root_id],
                |r| r.get(0),
            )
            .unwrap()
        };
        // Live root: backfilled from roots by the migration SQL.
        assert_eq!(get(1).as_deref(), Some("/vol/live"));
        // Dangling row with a unique display-path match: recovered by the hook.
        assert_eq!(get(99).as_deref(), Some("/gone/root"));
        // Dangling row with an ambiguous match: stays NULL, never guessed.
        assert_eq!(get(98), None);
    }

    /// Whether an index of this name exists.
    fn has_index(conn: &Connection, index: &str) -> bool {
        conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?",
            [index],
            |r| r.get::<_, i64>(0),
        )
        .unwrap()
            > 0
    }

    #[test]
    fn fresh_db_has_the_inode_nomination_index() {
        let conn = open_in_memory_for_test();
        assert!(has_index(&conn, "sources_inode"));
        // The device-leading index it supplements is untouched — a released
        // migration is never edited, only added to.
        assert!(has_index(&conn, "sources_device_inode"));
    }

    #[test]
    fn migrating_v6_adds_the_inode_nomination_index() {
        let mut conn = Connection::open_in_memory().unwrap();
        migrations().to_version(&mut conn, 6).unwrap();
        assert!(!has_index(&conn, "sources_inode"));

        migrations().to_latest(&mut conn).unwrap();

        assert!(has_index(&conn, "sources_inode"));
        assert!(has_index(&conn, "sources_device_inode"));
    }

    #[test]
    fn to_latest_is_idempotent() {
        let mut conn = Connection::open_in_memory().unwrap();
        migrations().to_latest(&mut conn).unwrap();
        // Re-running with user_version already current is a no-op, never an error.
        migrations().to_latest(&mut conn).unwrap();
        assert!(has_column(&conn, "decision_scopes", "receipt_rel_path"));
    }

    /// Simulate a v0.4.1 database: sources and decisions tables exist but without
    /// the columns added in a0353f4 (decision_id, receipt_root_id, receipt_rel_path).
    /// decision_scopes did not yet exist. Upgrading to latest must add all three columns.
    #[test]
    fn migrating_v041_db_adds_missing_columns() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE roots (
                 id INTEGER PRIMARY KEY,
                 path TEXT NOT NULL UNIQUE,
                 role TEXT NOT NULL DEFAULT 'source',
                 comment TEXT,
                 last_scanned_at INTEGER,
                 suspended INTEGER NOT NULL DEFAULT 0
             );
             CREATE TABLE objects (
                 id INTEGER PRIMARY KEY,
                 hash_type TEXT NOT NULL,
                 hash_value TEXT NOT NULL,
                 excluded INTEGER NOT NULL DEFAULT 0,
                 UNIQUE(hash_type, hash_value)
             );
             CREATE TABLE sources (
                 id INTEGER PRIMARY KEY,
                 root_id INTEGER NOT NULL REFERENCES roots(id),
                 rel_path TEXT NOT NULL,
                 device INTEGER,
                 inode INTEGER,
                 size INTEGER NOT NULL,
                 mtime INTEGER NOT NULL,
                 partial_hash TEXT NOT NULL,
                 basis_rev INTEGER NOT NULL DEFAULT 0,
                 scanned_at INTEGER NOT NULL,
                 last_seen_at INTEGER NOT NULL,
                 present INTEGER NOT NULL DEFAULT 1,
                 object_id INTEGER REFERENCES objects(id),
                 excluded INTEGER NOT NULL DEFAULT 0,
                 UNIQUE(root_id, rel_path)
             );
             CREATE TABLE decisions (
                 id INTEGER PRIMARY KEY,
                 command TEXT NOT NULL,
                 scope TEXT,
                 command_line TEXT NOT NULL,
                 reason TEXT,
                 status TEXT NOT NULL DEFAULT 'started',
                 count_attempted INTEGER,
                 count_completed INTEGER,
                 count_failed INTEGER,
                 count_skipped INTEGER,
                 summary TEXT,
                 canon_version TEXT NOT NULL,
                 created_at INTEGER NOT NULL
             );",
        )
        .unwrap();

        assert!(!has_column(&conn, "sources", "decision_id"));
        assert!(!has_column(&conn, "decisions", "receipt_root_id"));
        assert!(!has_column(&conn, "decisions", "receipt_rel_path"));

        migrations().to_latest(&mut conn).unwrap();

        assert!(has_column(&conn, "sources", "decision_id"));
        assert!(has_column(&conn, "decisions", "receipt_root_id"));
        assert!(has_column(&conn, "decisions", "receipt_rel_path"));
        // decision_scopes was absent in v0.4.1 — migration 1 creates it.
        assert!(has_column(&conn, "decision_scopes", "receipt_rel_path"));
    }

    /// Migration 3 must be a no-op on a fresh database where migration 1 already
    /// created the columns. Verifies IF NOT EXISTS prevents a duplicate-column error.
    #[test]
    fn migration3_is_safe_on_fresh_db() {
        let conn = open_in_memory_for_test();
        // If migration 3 had failed on the already-present columns, open_in_memory_for_test
        // above would have panicked. Verify the columns are present and correct.
        assert!(has_column(&conn, "sources", "decision_id"));
        assert!(has_column(&conn, "decisions", "receipt_root_id"));
        assert!(has_column(&conn, "decisions", "receipt_rel_path"));
    }

    #[test]
    fn fresh_db_has_decision_extractions_table() {
        let conn = open_in_memory_for_test();
        let mut stmt = conn
            .prepare("PRAGMA table_info(decision_extractions)")
            .unwrap();
        let cols: Vec<String> = stmt
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        for expected in [
            "decision_id",
            "root_id",
            "root_path",
            "rel_prefix",
            "files",
            "bytes",
            "destination_root_id",
            "destination_path",
            "disposition",
        ] {
            assert!(cols.iter().any(|c| c == expected), "missing {expected}");
        }
    }

    #[test]
    fn decision_extractions_pk_is_placement_precise() {
        // Migration 5 widens the PK to the placement pair — one row per
        // (decision, source root, origin dir, destination dir).
        let conn = open_in_memory_for_test();
        let mut stmt = conn
            .prepare("PRAGMA table_info(decision_extractions)")
            .unwrap();
        let mut pk_cols: Vec<(i64, String)> = stmt
            .query_map([], |row| {
                Ok((row.get::<_, i64>(5)?, row.get::<_, String>(1)?))
            })
            .unwrap()
            .map(|r| r.unwrap())
            .filter(|(pk, _)| *pk > 0)
            .collect();
        pk_cols.sort();
        let names: Vec<&str> = pk_cols.iter().map(|(_, n)| n.as_str()).collect();
        assert_eq!(
            names,
            vec!["decision_id", "root_id", "rel_prefix", "destination_path"]
        );
    }

    #[test]
    fn migration5_preserves_existing_extraction_rows() {
        // A coarse pre-precision row carries over unchanged through the
        // table rebuild — it stays a valid (if slack) placement claim.
        let mut conn = Connection::open_in_memory().unwrap();
        migrations().to_version(&mut conn, 4).unwrap();
        conn.execute(
            "INSERT INTO decision_extractions
                (decision_id, root_id, root_path, rel_prefix, files, bytes,
                 destination_root_id, destination_path, disposition)
             VALUES (1, 1, '/src', 'm', 245, 2450, 9, '/arch/m', 'retained')",
            [],
        )
        .unwrap();

        migrations().to_latest(&mut conn).unwrap();

        let (files, dest, disposition): (i64, String, String) = conn
            .query_row(
                "SELECT files, destination_path, disposition
                 FROM decision_extractions WHERE decision_id = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(files, 245);
        assert_eq!(dest, "/arch/m");
        assert_eq!(disposition, "retained");
    }
}
