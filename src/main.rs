use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use std::path::{Path, PathBuf};

use domain::IncludeSet;

#[derive(Clone, PartialEq, clap::ValueEnum)]
enum IncludeValue {
    Excluded,
    Archived,
    All,
}

#[derive(Clone, PartialEq, clap::ValueEnum)]
enum ClusterAllow {
    Archived,
    Duplicates,
}

#[derive(Clone, PartialEq, clap::ValueEnum)]
enum ApplyAllow {
    Duplicates,
    #[value(name = "cross-archive-duplicates")]
    CrossArchiveDuplicates,
}

#[derive(Clone, PartialEq, clap::ValueEnum)]
enum ImportFactsAllow {
    Archived,
}

#[derive(Clone, PartialEq, clap::ValueEnum)]
enum RetireAllow {
    Unresolved,
}

fn include_set_from(values: &[IncludeValue]) -> IncludeSet {
    let mut set = IncludeSet::default();
    for v in values {
        match v {
            IncludeValue::Excluded => set.excluded = true,
            IncludeValue::Archived => set.archived = true,
            IncludeValue::All => {
                set.excluded = true;
                set.archived = true;
            }
        }
    }
    set
}

// Infrastructure layers
mod domain;
mod expr;
mod ops;
mod repo;

// The shared spine, and the features built on it. These coexist with the
// layer modules above, which are being emptied into them.
mod core;
mod retire;
mod story;

// Utilities
mod alias;
mod ceremony;
mod progress;
mod scope;

// Command modules
mod archive;
mod compare;
mod coverage;
mod exclude;
mod facts;
mod ledger;
mod ls;
mod notes;
mod roots;
mod scan;
mod survey;
mod sweep;
mod trail;
mod worklist;

#[derive(Parser)]
#[command(name = "canon")]
#[command(about = "Organize large media libraries into a canonical archive")]
struct Cli {
    /// Canon home directory (default: ~/.canon/, env: CANON_HOME)
    #[arg(long, global = true)]
    canon_home: Option<PathBuf>,

    /// Print SQL queries with timing for debugging
    #[arg(long, global = true)]
    debug_sql: bool,

    /// Profile SQL queries and show summary with slow query analysis
    #[arg(long, global = true)]
    profile: bool,

    /// Suppress receipt generation for this invocation
    #[arg(long, global = true)]
    no_receipt: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    // -- Scan --
    /// Scan directories and add files to the index
    Scan {
        /// Paths to scan (not required if --all is used)
        paths: Vec<PathBuf>,
        /// Role for new roots: 'source' or 'archive' (required with --add, optional filter with --all)
        #[arg(long)]
        role: Option<String>,
        /// Add path as a new root (required when path is not inside an existing root)
        #[arg(long)]
        add: bool,
        /// Comment for the new root (only with --add)
        #[arg(short = 'c', long)]
        comment: Option<String>,
        /// Scan all existing roots (optionally filtered by --role)
        #[arg(long)]
        all: bool,
        /// Skip computing content hashes (just index files)
        #[arg(long)]
        no_hash: bool,
        /// Recompute ALL hashes for integrity verification (even unchanged files)
        #[arg(long, conflicts_with = "no_hash")]
        verify: bool,
        /// Find directories with files that aren't under any root
        #[arg(long, conflicts_with_all = ["add", "all", "verify"])]
        candidates: bool,
        /// Disable mount protection that skips files on disconnected storage.
        /// Use when device IDs change between scans (e.g., NAS remounts).
        #[arg(long)]
        ignore_device_id: bool,
        /// Mark all sources under the given path(s) as not present.
        /// Use for deleted folders that no longer exist on disk.
        #[arg(long, conflicts_with_all = ["all", "add"])]
        missing: bool,
        /// Reason for this operation (recorded in decision log)
        #[arg(long)]
        reason: Option<String>,
    },
    /// List and manage roots
    #[command(args_conflicts_with_subcommands = true)]
    Roots {
        #[command(subcommand)]
        action: Option<RootsAction>,

        /// Scope to roots at or beneath this path (for default list action)
        path: Option<PathBuf>,

        /// Only list suspended roots
        #[arg(long)]
        suspended: bool,
    },
    // -- Enrich --
    /// Output sources as JSONL worklist
    Worklist {
        /// Directory paths to scope the query (resolved to realpath)
        paths: Vec<PathBuf>,
        /// Filter expressions (e.g., "NOT content.hash.sha256?" or "source.ext=jpg")
        #[arg(long = "where")]
        filters: Vec<String>,
        /// Include additional sources: excluded, archived, all
        #[arg(long, value_delimiter = ',')]
        include: Vec<IncludeValue>,
        /// Show results across all roots, ignoring current directory scope
        #[arg(long)]
        global: bool,
        /// Emit only one source per unique content hash (sources without a hash are skipped)
        #[arg(long)]
        unique_content: bool,
        /// Include specific facts in the output (e.g., --emit geo.lat --emit geo.lon)
        #[arg(long)]
        emit: Vec<String>,
    },
    /// Import facts from JSONL on stdin
    ImportFacts {
        /// Allow: archived
        #[arg(long, value_delimiter = ',')]
        allow: Vec<ImportFactsAllow>,
        /// Show each fact as it's imported
        #[arg(short, long)]
        verbose: bool,
    },
    // -- Discover --
    /// List sources matching filters
    Ls {
        /// Directory paths to scope the query (resolved to realpath)
        paths: Vec<PathBuf>,
        /// Filter expressions (e.g., "source.ext=jpg" or "archived?")
        #[arg(long = "where")]
        filters: Vec<String>,
        /// Show results across all roots, ignoring current directory scope
        #[arg(long)]
        global: bool,
        /// Show sources with duplicate content (same hash), grouped by hash
        #[arg(long)]
        duplicates: bool,
        /// Include additional sources: excluded, archived, all
        #[arg(long, value_delimiter = ',')]
        include: Vec<IncludeValue>,
        /// Use long listing format (size, date, path)
        #[arg(short = 'l', long)]
        long: bool,
        /// Sort by: path (default), size, mtime, name
        #[arg(short = 's', long, default_value = "path")]
        sort: String,
        /// Reverse sort order
        #[arg(short = 'r', long)]
        reverse: bool,
        /// Output null-delimited paths (for use with xargs -0)
        #[arg(short = '0', long = "null")]
        null_delim: bool,
    },
    /// Show fact coverage and value distribution
    #[command(args_conflicts_with_subcommands = true)]
    Facts {
        #[command(subcommand)]
        action: Option<FactsAction>,

        /// Specific fact key to show value distribution
        #[arg(long)]
        key: Option<String>,
        /// Directory paths to scope the query (resolved to realpath)
        paths: Vec<PathBuf>,
        /// Filter expressions (e.g., "source.ext=jpg" or "content.hash.sha256?")
        #[arg(long = "where")]
        filters: Vec<String>,
        /// Show results across all roots, ignoring current directory scope
        #[arg(long)]
        global: bool,
        /// Maximum number of values to show (0 for unlimited, default 50)
        #[arg(long, default_value = "50")]
        limit: usize,
        /// Show all built-in facts (including hidden ones like source.device, source.inode)
        #[arg(long)]
        all: bool,
        /// Show pattern aliases available for manifest patterns
        #[arg(long)]
        show_aliases: bool,
        /// Include additional sources: excluded, archived, all
        #[arg(long, value_delimiter = ',')]
        include: Vec<IncludeValue>,
        /// Show source count per root, or group fact values by root (with --key)
        #[arg(long)]
        by_root: bool,
        /// Group results by fact key(s), comma-separated (requires --key). Supports modifiers.
        #[arg(long, value_delimiter = ',')]
        group_by: Vec<String>,
    },
    /// Show archive coverage statistics
    Coverage {
        /// Directory paths to scope the query (resolved to realpath)
        paths: Vec<PathBuf>,
        /// Filter expressions (e.g., "source.ext=jpg" or "content.hash.sha256?")
        #[arg(long = "where")]
        filters: Vec<String>,
        /// Show results across all roots, ignoring current directory scope
        #[arg(long)]
        global: bool,
        /// Filter coverage relative to a specific archive (id:N or path:/foo/bar)
        #[arg(long)]
        archive: Option<String>,
        /// Include additional sources: excluded, archived, all
        #[arg(long, value_delimiter = ',')]
        include: Vec<IncludeValue>,
        /// Compact output: one line per root
        #[arg(long)]
        compact: bool,
    },
    /// Survey a selection: archive status, related locations, unique content
    Survey {
        /// Directory paths to scope the query (resolved to realpath)
        paths: Vec<PathBuf>,
        /// Filter expressions (e.g., "source.ext=jpg" or "content.hash.sha256?")
        #[arg(long = "where")]
        filters: Vec<String>,
        /// Show results across all roots, ignoring current directory scope
        #[arg(long)]
        global: bool,
        /// Include additional sources: excluded
        #[arg(long, value_delimiter = ',')]
        include: Vec<IncludeValue>,
        /// Compare against specific locations instead of discovering them
        #[arg(long = "other")]
        other_paths: Vec<PathBuf>,
        /// Filter archive section to a specific archive (id:N or path:/foo/bar)
        #[arg(long)]
        archive: Option<String>,
        /// Opt into affinity enrichment (requires --where)
        #[arg(long)]
        affinity: bool,
        /// Skip per-location affinity computation
        #[arg(long, conflicts_with = "detail")]
        brief: bool,
        /// Show detailed output (complement or unique)
        #[arg(long, value_enum)]
        detail: Option<survey::DetailMode>,
        /// Output null-delimited paths (for --detail unique)
        #[arg(short = '0', long = "null")]
        null_delim: bool,
        /// Show all paths per location
        #[arg(long)]
        verbose: bool,
    },
    /// Sweep the universe for reduction opportunities — ranked places where
    /// one decision resolves the most
    Sweep {
        /// Maximum leaderboard entries (default: 10)
        #[arg(long, conflicts_with = "all")]
        limit: Option<usize>,
        /// Show everything: all entries, all hub members, below-floor findings
        #[arg(long)]
        all: bool,
    },
    /// Compare two folders by content hash
    Compare {
        /// Paths to compare (1 path: CWD vs path, 2 paths: A vs B)
        #[arg(required = true, num_args = 1..=2)]
        paths: Vec<PathBuf>,
        /// Filter expressions (e.g., "source.ext=jpg")
        #[arg(long = "where")]
        filters: Vec<String>,
        /// Include additional sources: excluded
        #[arg(long, value_delimiter = ',')]
        include: Vec<IncludeValue>,
        /// Show file paths for differences
        #[arg(short, long)]
        verbose: bool,
    },
    // -- Organize --
    /// Generate a cluster manifest from matching sources
    Cluster {
        #[command(subcommand)]
        action: ClusterAction,
    },
    /// Apply a manifest to copy/move files
    Apply {
        /// Path to the manifest file
        manifest: PathBuf,
        /// Show what would be done without making changes
        #[arg(long)]
        dry_run: bool,
        /// Show detailed output for each file transfer
        #[arg(long, short = 'v')]
        verbose: bool,
        /// Allow: duplicates, cross-archive-duplicates
        #[arg(long, value_delimiter = ',')]
        allow: Vec<ApplyAllow>,
        /// Only apply sources from these roots (id:N or path:/foo/bar, can repeat)
        #[arg(long)]
        root: Vec<String>,
        /// Use rename instead of copy (Unix only, fails if cross-device, never copies)
        #[arg(long, conflicts_with = "move_files")]
        rename: bool,
        /// Move files: rename, or copy+delete if cross-device
        #[arg(long = "move", conflicts_with = "rename")]
        move_files: bool,
        /// Skip confirmation prompt
        #[arg(long)]
        yes: bool,
        /// Resume a previously interrupted apply (skip already-copied files)
        #[arg(long)]
        resume: bool,
        /// Reason for this operation (recorded in decision log; falls back to manifest notes)
        #[arg(long)]
        reason: Option<String>,
    },
    /// Manage source exclusions
    Exclude {
        #[command(subcommand)]
        action: ExcludeAction,
    },
    /// Annotate locations with notes — your thoughts ("what am I thinking?");
    /// for what happened, see 'trail'
    Note {
        /// Path to annotate, view, or scope
        path: Option<PathBuf>,
        /// Add a note with the given text
        #[arg(short = 'm')]
        message: Option<String>,
        /// List notes for scope and all descendants
        #[arg(short = 'r', long)]
        recursive: bool,
        /// List all notes across all roots
        #[arg(long)]
        global: bool,
        /// Clear notes for the scope
        #[arg(long)]
        clear: bool,
        /// Skip confirmation prompt
        #[arg(long)]
        yes: bool,
        /// Group by location, show most recent note per location
        #[arg(long)]
        by_scope: bool,
        /// Maximum number of entries to display (default: 10, 0 = unlimited)
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Read the decision trail — what happened here, or the day's story;
    /// for your thoughts, see 'note'
    Trail {
        #[command(subcommand)]
        action: Option<TrailAction>,
        /// Directory paths to scope the timeline (resolved to realpath)
        paths: Vec<PathBuf>,
        /// Show the trail across all roots, ignoring current directory scope
        #[arg(long)]
        global: bool,
        /// Today's story (sugar for --since today)
        #[arg(long, conflicts_with_all = ["since", "on"])]
        today: bool,
        /// Story from a day onward: today, yesterday, a weekday, or YYYY-MM-DD
        #[arg(long, conflicts_with = "on")]
        since: Option<String>,
        /// One day's story: today, yesterday, a weekday, or YYYY-MM-DD
        #[arg(long)]
        on: Option<String>,
        /// Maximum decisions to show (default: 20)
        #[arg(long, conflicts_with = "all")]
        limit: Option<usize>,
        /// Show all decisions (no cap)
        #[arg(long)]
        all: bool,
        /// Hide notes from the timeline
        #[arg(long)]
        no_notes: bool,
        /// Emit timeline events as JSONL (machine output)
        #[arg(long)]
        jsonl: bool,
    },
    /// Prune orphaned or stale data from the database
    Prune {
        /// Delete objects that have no present sources, along with their facts and non-present sources
        #[arg(long)]
        orphaned_objects: bool,
        /// Delete source facts where the file changed since the fact was recorded
        #[arg(long)]
        stale_facts: bool,
        /// Delete facts for excluded sources and/or objects (=source, =object, or omit for both)
        #[arg(long, value_name = "SCOPE", default_missing_value = "all", num_args = 0..=1)]
        excluded_facts: Option<String>,
        /// Execute deletion (default is dry-run)
        #[arg(long)]
        yes: bool,
    },
    // -- Maintain --
    /// Maintain the extraction ledger index
    Ledger {
        #[command(subcommand)]
        command: LedgerCommands,
    },
}

#[derive(Subcommand)]
enum LedgerCommands {
    /// Rebuild the extraction index from apply receipts on disk
    Reindex {
        /// Show what would be indexed without writing anything
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(Subcommand)]
enum ExcludeAction {
    /// Mark sources as excluded
    Set {
        /// Directory paths to scope the operation (resolved to realpath)
        paths: Vec<PathBuf>,
        /// Filter expressions (e.g., "source.size<1000" or "source.ext=tmp")
        #[arg(long = "where")]
        filters: Vec<String>,
        /// Exclude specific source by ID (as shown in ls --duplicates)
        #[arg(long)]
        id: Option<i64>,
        /// Show what would be excluded without making changes
        #[arg(long)]
        dry_run: bool,
        /// Skip confirmation prompt
        #[arg(long)]
        yes: bool,
        /// Operate on all roots (bypass CWD scope defaulting)
        #[arg(long)]
        global: bool,
        /// Reason for this operation (recorded in decision log)
        #[arg(long)]
        reason: Option<String>,
    },
    /// Remove exclusions from sources
    Clear {
        /// Directory paths to scope the operation (resolved to realpath)
        paths: Vec<PathBuf>,
        /// Filter expressions to match excluded sources
        #[arg(long = "where")]
        filters: Vec<String>,
        /// Show what would be cleared without making changes
        #[arg(long)]
        dry_run: bool,
        /// Skip confirmation prompt
        #[arg(long)]
        yes: bool,
        /// Operate on all roots (bypass CWD scope defaulting)
        #[arg(long)]
        global: bool,
        /// Reason for this operation (recorded in decision log)
        #[arg(long)]
        reason: Option<String>,
    },
    /// Exclude duplicate sources, keeping copies in preferred path
    Duplicates {
        /// Directory path to scope the operation (resolved to realpath)
        path: PathBuf,
        /// Path prefix to prefer (keep sources here, exclude duplicates elsewhere)
        #[arg(long, required = true)]
        prefer: PathBuf,
        /// Filter expressions (e.g., "source.ext=jpg")
        #[arg(long = "where")]
        filters: Vec<String>,
        /// Show what would be excluded without making changes
        #[arg(long)]
        dry_run: bool,
        /// Skip confirmation prompt
        #[arg(long)]
        yes: bool,
        /// Reason for this operation (recorded in decision log)
        #[arg(long)]
        reason: Option<String>,
    },
    /// Exclude objects by hash, file, or filter (affects all sources with matching content)
    SetObject {
        /// Directory paths to scope the operation, or a single file path
        paths: Vec<PathBuf>,
        /// Filter expressions (e.g., "content.mime=application/octet-stream")
        #[arg(long = "where")]
        filters: Vec<String>,
        /// Exclude specific object by hash (use this for empty files)
        #[arg(long)]
        hash: Option<String>,
        /// Execute the exclusion (default is dry-run for safety)
        #[arg(long)]
        yes: bool,
        /// Show all source locations (default shows up to 3)
        #[arg(long, short)]
        verbose: bool,
        /// Operate on all roots (bypass CWD scope defaulting)
        #[arg(long)]
        global: bool,
        /// Reason for this operation (recorded in decision log)
        #[arg(long)]
        reason: Option<String>,
    },
    /// Clear exclusion from an object by hash
    ClearObject {
        /// Content hash (sha256)
        hash: String,
        /// Show what would be cleared without making changes
        #[arg(long)]
        dry_run: bool,
    },
    /// List excluded objects
    ListObjects,
}

#[derive(Subcommand)]
enum TrailAction {
    /// Show one decision in full, with its receipt locations
    Show {
        /// Decision id (as shown in the timeline)
        id: i64,
    },
}

#[derive(Subcommand)]
enum FactsAction {
    /// Delete facts by key
    Delete {
        /// Fact key to delete (e.g., "content.mime")
        key: String,
        /// Directory paths to scope the operation (resolved to realpath)
        paths: Vec<PathBuf>,
        /// Filter expressions (e.g., "source.ext=jpg")
        #[arg(long = "where")]
        filters: Vec<String>,
        /// Entity type: 'source' or 'object'
        #[arg(long, value_name = "TYPE")]
        on: String,
        /// Filter by value storage type (text, num, or time)
        #[arg(long, value_parser = ["text", "num", "time"])]
        value_type: Option<String>,
        /// Execute deletion (default is dry-run)
        #[arg(long)]
        yes: bool,
    },
}

#[derive(Subcommand)]
enum RootsAction {
    /// List all roots
    List {
        /// Scope to roots at or beneath this path
        path: Option<PathBuf>,
        /// Only list suspended roots
        #[arg(long)]
        suspended: bool,
    },
    /// Remove a root and its sources from the database (files on disk are not deleted)
    Rm {
        /// Root specifier: id:<N> or path:<path>
        spec: String,
        /// Skip confirmation prompt
        #[arg(long)]
        yes: bool,
        /// Reason for this operation (recorded in decision log)
        #[arg(long)]
        reason: Option<String>,
    },
    /// Set or clear a comment on a root
    Comment {
        /// Root specifier: id:<N> or path:<path>
        spec: String,
        /// Comment text (omit to clear)
        comment: Option<String>,
    },
    /// Suspend a root (hide from all operations)
    Suspend {
        /// Root specifier: id:<N> or path:<path>
        spec: String,
    },
    /// Unsuspend a root (make visible again)
    Unsuspend {
        /// Root specifier: id:<N> or path:<path>
        spec: String,
    },
    /// List the retired fleet: the books on the shelf
    Retired,
    /// Read a root's story: the map of places — where you acted, and what
    /// no decision ever touched
    Story {
        /// Root specifier: id:<N> or path:<path>
        spec: String,
        /// Cap the number of place lines (omissions are counted, never silent)
        #[arg(long, default_value_t = 50, conflicts_with = "all")]
        limit: usize,
        /// Show every place line
        #[arg(long)]
        all: bool,
    },
    /// Retire a root: review readiness, bind its story into the book on the
    /// shelf, then remove it from the index
    Retire {
        /// Root specifier: id:<N> or path:<path>
        spec: String,
        /// Readiness review only; exits 0
        #[arg(long)]
        dry_run: bool,
        /// Allow: unresolved (retire despite unresolved sources)
        #[arg(long, value_delimiter = ',')]
        allow: Vec<RetireAllow>,
        /// Reason for this operation (recorded in decision log)
        #[arg(long)]
        reason: Option<String>,
        /// Skip confirmation prompts (never implies --allow)
        #[arg(long)]
        yes: bool,
    },
}

#[derive(Subcommand)]
enum ClusterAction {
    /// Generate a new manifest
    Generate {
        /// Directory paths to scope the query (resolved to realpath)
        paths: Vec<PathBuf>,
        /// Filter expressions (e.g., "content_hash.sha256?" or "exif.model=iPhone")
        #[arg(long = "where")]
        filters: Vec<String>,
        /// Destination path (must be inside an archive root)
        #[arg(long, required = true)]
        dest: PathBuf,
        /// Output manifest file
        #[arg(short, long, default_value = "manifest.toml")]
        output: PathBuf,
        /// Output manifest at dest/<name> (e.g., -O test.toml)
        #[arg(short = 'O', long = "dest-output", conflicts_with = "output")]
        dest_output: Option<String>,
        /// Overwrite existing output file
        #[arg(short, long)]
        force: bool,
        /// Allow: archived, duplicates
        #[arg(long, value_delimiter = ',')]
        allow: Vec<ClusterAllow>,
        /// Show which files were excluded because they're already archived
        #[arg(long)]
        show_archived: bool,
        /// Don't open manifest in $VISUAL/$EDITOR after generation
        #[arg(long)]
        no_edit: bool,
        /// Operate on all roots (bypass CWD scope defaulting)
        #[arg(long)]
        global: bool,
    },
    /// Regenerate lock file from existing manifest config
    Refresh {
        /// Path to manifest TOML file
        manifest: PathBuf,
        /// Show which files were excluded because they're already archived
        #[arg(long)]
        show_archived: bool,
        /// Open manifest in $VISUAL/$EDITOR after refresh
        #[arg(long)]
        edit: bool,
    },
    /// Show the state of a manifest's entries
    Status {
        /// Path to manifest TOML file
        manifest: PathBuf,
        /// Show all entries, not just concerning ones
        #[arg(long)]
        verbose: bool,
    },
}

const DEFAULT_CONFIG_CONTENT: &str = r#"# Canon ledger configuration
# This file controls decision provenance and receipt behavior.

[ledger]

# Recording mode controls how much provenance Canon stores.
# Options:
#   "full"    - Record decisions in the database AND write receipt files (default)
#   "records" - Record decisions in the database only (no receipt files)
#   "off"     - No provenance recording (disables both DB records and receipts)
recording = "full"

# Receipt layout controls where receipt files are placed within an archive root.
# Options:
#   "central"    - Receipts mirror the destination path under .canon-ledger/ (default)
#   "alongside"  - Receipts go in .canon-ledger/ next to the destination directory
layout = "central"

# Ledger root: the archive root ID where non-targeted receipts (e.g., exclusions)
# are stored. Defaults to the lowest-ID archive root when unset.
# Use `canon roots` to find root IDs. Must be an archive root, not a source root.
# root = 1
"#;

fn load_or_create_config(canon_home: &Path) -> (domain::config::LedgerConfig, Vec<String>) {
    let path = canon_home.join("config.toml");
    if !path.exists() {
        let mut warnings = Vec::new();
        match write_default_config(&path) {
            Ok(()) => {
                eprintln!("Created {}", path.display());
            }
            Err(e) => {
                warnings.push(format!("Warning: could not create {}: {e}", path.display()));
            }
        }
        return (domain::config::LedgerConfig::default(), warnings);
    }
    match std::fs::read_to_string(&path) {
        Ok(content) => domain::config::parse_ledger_config(&content),
        Err(e) => {
            let warnings = vec![format!(
                "Warning: could not read {}: {e}, using defaults",
                path.display()
            )];
            (domain::config::LedgerConfig::default(), warnings)
        }
    }
}

fn write_default_config(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, DEFAULT_CONFIG_CONTENT)?;
    Ok(())
}

fn resolve_canon_home(flag: Option<&Path>) -> Result<PathBuf> {
    if let Some(path) = flag {
        return Ok(path.to_path_buf());
    }
    if let Ok(val) = std::env::var("CANON_HOME") {
        return Ok(PathBuf::from(val));
    }
    let mut path = dirs::home_dir().context("Could not determine home directory")?;
    path.push(".canon");
    Ok(path)
}

fn main() -> Result<()> {
    let command_line = std::env::args().collect::<Vec<_>>().join(" ");
    let cli = Cli::parse();

    let canon_home = resolve_canon_home(cli.canon_home.as_deref())?;
    if canon_home.exists() && !canon_home.is_dir() {
        bail!(
            "CANON_HOME path is not a directory: {}",
            canon_home.display()
        );
    }

    let (config, config_warnings) = load_or_create_config(&canon_home);
    for w in &config_warnings {
        eprintln!("{w}");
    }

    let db_path = canon_home.join("canon.db");

    let mut db = repo::open_with_options(
        &db_path,
        repo::DbOptions {
            debug_sql: cli.debug_sql,
            profile: cli.profile,
        },
    )?;

    // Run periodic ANALYZE before command dispatch so read-only commands
    // benefit from fresh query planner statistics without a post-command delay.
    // Write commands (scan, apply, import-facts) run their own ANALYZE after
    // bulk changes — the periodic check here catches the gap when only reads
    // have happened since the last analyze.
    if db.needs_analyze()? {
        eprintln!("Updating query statistics...");
        db.run_analyze()?;
    }

    // Validate config.root after DB open (semantic validation: must be archive, not source).
    if let Some(root_id) = config.root {
        let roots = repo::root::fetch_all(db.conn())?;
        if let Some(root) = roots.iter().find(|r| r.id == root_id) {
            if root.is_source() {
                bail!(
                    "Ledger root (id:{root_id}) is a source root, not an archive. \
                     Update [ledger].root in {}",
                    canon_home.join("config.toml").display()
                );
            }
        }
    }

    match cli.command {
        Commands::Scan {
            paths,
            role,
            add,
            comment,
            all,
            no_hash,
            verify,
            candidates,
            ignore_device_id,
            missing,
            reason,
        } => {
            if candidates {
                if paths.is_empty() {
                    anyhow::bail!("--candidates requires a path");
                }
                for path in &paths {
                    scan::find_candidates(&db, path)?;
                }
                return Ok(());
            }
            if add && role.is_none() {
                anyhow::bail!("--role is required when using --add");
            }
            if comment.is_some() && !add {
                anyhow::bail!("--comment requires --add");
            }
            if all && add {
                anyhow::bail!("--all and --add cannot be used together");
            }
            if !all && paths.is_empty() {
                anyhow::bail!("Provide paths to scan, or use --all to scan all roots");
            }
            #[cfg(not(unix))]
            if all {
                anyhow::bail!("--all is not supported on this platform (no device ID detection for mount safety)");
            }
            scan::run(
                &db,
                &paths,
                role.as_deref(),
                add,
                comment.as_deref(),
                all,
                no_hash,
                verify,
                ignore_device_id,
                missing,
                &command_line,
                &config,
                cli.no_receipt,
                reason.as_deref(),
            )?;
        }
        Commands::Worklist {
            paths,
            filters,
            include,
            global,
            unique_content,
            emit,
        } => {
            let filters = alias::expand_filter_strings(&filters, &canon_home)?;
            let mut include = include_set_from(&include);
            let all_roots = repo::root::fetch_all(db.conn())?;
            let resolved = ops::scope::resolve_scope(db.conn(), &paths, global, &all_roots)?;
            if resolved.auto_include_archived {
                include.archived = true;
            }
            scope::print_list_scope(&resolved);
            worklist::run(
                &mut db,
                &resolved.prefixes,
                &filters,
                &include,
                unique_content,
                &emit,
            )?;
        }
        Commands::ImportFacts { allow, verbose } => {
            let allow_archived = allow.contains(&ImportFactsAllow::Archived);
            facts::import_run(
                &mut db,
                allow_archived,
                verbose,
                &command_line,
                &config,
                cli.no_receipt,
            )?;
        }
        Commands::Ls {
            paths,
            filters,
            global,
            duplicates,
            include,
            long,
            sort,
            reverse,
            null_delim,
        } => {
            let filters = alias::expand_filter_strings(&filters, &canon_home)?;
            let mut include = include_set_from(&include);

            let all_roots = repo::root::fetch_all(db.conn())?;
            let resolved = ops::scope::resolve_scope(db.conn(), &paths, global, &all_roots)?;
            if resolved.auto_include_archived {
                include.archived = true;
            }

            let use_relative = resolved.from_cwd;

            scope::print_list_scope(&resolved);

            if duplicates {
                ls::show_duplicates(
                    &mut db,
                    &resolved.prefixes,
                    &filters,
                    &include,
                    use_relative,
                )?;
            } else {
                ls::run(
                    &mut db,
                    &resolved.prefixes,
                    &filters,
                    &include,
                    use_relative,
                    long,
                    &sort,
                    reverse,
                    null_delim,
                )?;
            }
        }
        Commands::Facts {
            action,
            key,
            paths,
            filters,
            global,
            limit,
            all,
            show_aliases,
            include,
            by_root,
            group_by,
        } => {
            if show_aliases {
                facts::show_aliases();
                return Ok(());
            }
            match action {
                Some(FactsAction::Delete {
                    key,
                    paths,
                    filters,
                    on,
                    value_type,
                    yes,
                }) => {
                    let filters = alias::expand_filter_strings(&filters, &canon_home)?;
                    let all_roots = repo::root::fetch_all(db.conn())?;
                    let resolved = ops::scope::resolve_scope(db.conn(), &paths, false, &all_roots)?;
                    let options = facts::DeleteOptions {
                        entity_type: on,
                        value_type,
                        dry_run: !yes,
                    };
                    facts::delete_facts(
                        &mut db,
                        &key,
                        &resolved.prefixes,
                        &filters,
                        &options,
                        &command_line,
                        &config,
                        cli.no_receipt,
                    )?;
                }
                None => {
                    let filters = alias::expand_filter_strings(&filters, &canon_home)?;
                    let mut include = include_set_from(&include);
                    let all_roots = repo::root::fetch_all(db.conn())?;
                    let resolved =
                        ops::scope::resolve_scope(db.conn(), &paths, global, &all_roots)?;
                    if resolved.auto_include_archived {
                        include.archived = true;
                    }
                    facts::run(
                        &mut db,
                        key.as_deref(),
                        &resolved.prefixes,
                        &filters,
                        limit,
                        all,
                        &include,
                        by_root,
                        &group_by,
                        &resolved,
                    )?;
                }
            }
        }
        Commands::Prune {
            orphaned_objects,
            stale_facts,
            excluded_facts,
            yes,
        } => {
            if !orphaned_objects && !stale_facts && excluded_facts.is_none() {
                anyhow::bail!("At least one of --orphaned-objects, --stale-facts, or --excluded-facts is required");
            }
            if stale_facts {
                facts::prune_stale(&db, !yes, &command_line, &config, cli.no_receipt)?;
            }
            if orphaned_objects {
                facts::prune_orphaned_objects(
                    &mut db,
                    !yes,
                    &command_line,
                    &config,
                    cli.no_receipt,
                )?;
            }
            if let Some(scope) = excluded_facts {
                facts::prune_excluded_facts(
                    &db,
                    &scope,
                    !yes,
                    &command_line,
                    &config,
                    cli.no_receipt,
                )?;
            }
        }
        Commands::Ledger { command } => match command {
            LedgerCommands::Reindex { dry_run } => {
                ledger::run_reindex(&mut db, dry_run)?;
            }
        },
        Commands::Coverage {
            paths,
            filters,
            global,
            archive,
            include,
            compact,
        } => {
            let filters = alias::expand_filter_strings(&filters, &canon_home)?;
            let mut include = include_set_from(&include);
            let all_roots = repo::root::fetch_all(db.conn())?;
            let resolved = ops::scope::resolve_scope(db.conn(), &paths, global, &all_roots)?;
            if resolved.auto_include_archived {
                include.archived = true;
            }
            coverage::run(
                &mut db,
                &resolved.prefixes,
                &filters,
                archive.as_deref(),
                &include,
                compact,
                &resolved,
            )?;
        }
        Commands::Survey {
            paths,
            filters,
            global,
            include,
            other_paths,
            archive,
            affinity,
            brief,
            detail,
            null_delim,
            verbose,
        } => {
            let expanded = alias::expand_filter_strings(&filters, &canon_home)?;
            let mut include = include_set_from(&include);
            if include.includes_archived() {
                bail!("--include archived is not valid for survey");
            }
            let all_roots = repo::root::fetch_all(db.conn())?;
            let resolved = ops::scope::resolve_scope(db.conn(), &paths, global, &all_roots)?;
            if resolved.auto_include_archived {
                include.archived = true;
            }
            let scope_prefixes = resolved.prefixes.clone();
            let options = survey::SurveyOptions {
                original_filters: filters,
                include,
                other_paths,
                affinity,
                archive,
                brief,
                detail,
                null_delim,
                verbose,
                scope: resolved,
            };
            survey::run(&mut db, &scope_prefixes, &expanded, &options)?;
        }
        Commands::Sweep { limit, all } => sweep::run(&mut db, limit, all)?,
        Commands::Compare {
            paths,
            filters,
            include,
            verbose,
        } => {
            let filters = alias::expand_filter_strings(&filters, &canon_home)?;
            let include = include_set_from(&include);
            if include.includes_archived() {
                bail!("--include archived is not valid for compare (valid values: excluded)");
            }
            let all_roots = repo::root::fetch_all(db.conn())?;
            let (path_a, path_b) = if paths.len() == 2 {
                (paths[0].clone(), paths[1].clone())
            } else {
                // One path: CWD as side A
                let cwd = std::env::current_dir()?;
                let cwd_resolved = ops::scope::resolve_path(&cwd, &all_roots, &cwd)?;
                if domain::root::find_containing_root(&cwd_resolved, &all_roots).is_none() {
                    bail!("Current directory is not under any known root");
                }
                (cwd, paths[0].clone())
            };
            let options = compare::CompareOptions { include, verbose };
            let identical = compare::run(&mut db, &path_a, &path_b, &filters, &options)?;
            if !identical {
                std::process::exit(1);
            }
        }
        Commands::Cluster { action } => match action {
            ClusterAction::Generate {
                paths,
                filters,
                dest,
                output,
                dest_output,
                force,
                allow,
                show_archived,
                no_edit,
                global,
            } => {
                let expanded = alias::expand_filter_strings(&filters, &canon_home)?;
                let all_roots = repo::root::fetch_all(db.conn())?;
                let resolved = ops::scope::resolve_scope(db.conn(), &paths, global, &all_roots)?;
                let options = archive::GenerateOptions {
                    force,
                    allow_archived: allow.contains(&ClusterAllow::Archived),
                    allow_duplicates: allow.contains(&ClusterAllow::Duplicates),
                    show_archived,
                    no_edit,
                };
                let output_path = if let Some(name) = dest_output {
                    dest.join(name)
                } else {
                    output
                };
                let output_path = if output_path.extension().is_none() {
                    output_path.with_extension("toml")
                } else {
                    output_path
                };
                archive::generate(
                    &mut db,
                    &resolved.prefixes,
                    &filters,
                    &expanded,
                    &dest,
                    &output_path,
                    &options,
                    &command_line,
                    &config,
                    cli.no_receipt,
                )?;
            }
            ClusterAction::Refresh {
                manifest,
                show_archived,
                edit,
            } => {
                archive::refresh(
                    &mut db,
                    &manifest,
                    show_archived,
                    !edit,
                    &command_line,
                    &config,
                    cli.no_receipt,
                )?;
            }
            ClusterAction::Status { manifest, verbose } => {
                archive::status(db.conn_mut(), &manifest, verbose)?;
            }
        },
        Commands::Apply {
            manifest,
            dry_run,
            verbose,
            allow,
            root,
            rename,
            move_files,
            yes,
            resume,
            reason,
        } => {
            let transfer_mode = if rename {
                archive::TransferMode::Rename
            } else if move_files {
                archive::TransferMode::Move
            } else {
                archive::TransferMode::Copy
            };
            let options = archive::ApplyOptions {
                dry_run,
                verbose,
                allow_cross_archive_duplicates: allow.contains(&ApplyAllow::CrossArchiveDuplicates),
                allow_duplicates: allow.contains(&ApplyAllow::Duplicates),
                roots: root,
                transfer_mode,
                yes,
                resume,
            };
            archive::run(
                &mut db,
                &manifest,
                &options,
                &command_line,
                &config,
                cli.no_receipt,
                reason.as_deref(),
            )?;
        }
        Commands::Exclude { action } => match action {
            ExcludeAction::Set {
                paths,
                filters,
                id,
                dry_run,
                yes,
                global,
                reason,
            } => {
                let filters = alias::expand_filter_strings(&filters, &canon_home)?;
                let options = exclude::SetOptions {
                    dry_run,
                    verbose: false,
                    yes,
                };
                if let Some(source_id) = id {
                    exclude::set_by_id(
                        &mut db,
                        source_id,
                        &options,
                        &command_line,
                        &config,
                        cli.no_receipt,
                        reason.as_deref(),
                    )?;
                } else if paths.len() == 1 && filters.is_empty() && paths[0].is_file() {
                    // Single file path with no filters: exclude exact file
                    exclude::set_by_path(
                        &mut db,
                        &paths[0],
                        &options,
                        &command_line,
                        &config,
                        cli.no_receipt,
                        reason.as_deref(),
                    )?;
                } else {
                    let all_roots = repo::root::fetch_all(db.conn())?;
                    let resolved =
                        ops::scope::resolve_scope(db.conn(), &paths, global, &all_roots)?;
                    exclude::set(
                        &mut db,
                        &resolved.prefixes,
                        &filters,
                        &options,
                        &command_line,
                        &config,
                        cli.no_receipt,
                        reason.as_deref(),
                    )?;
                }
            }
            ExcludeAction::Clear {
                paths,
                filters,
                dry_run,
                yes,
                global,
                reason,
            } => {
                let filters = alias::expand_filter_strings(&filters, &canon_home)?;
                let options = exclude::ClearOptions { dry_run, yes };
                let all_roots = repo::root::fetch_all(db.conn())?;
                let resolved = ops::scope::resolve_scope(db.conn(), &paths, global, &all_roots)?;
                exclude::clear(
                    &mut db,
                    &resolved.prefixes,
                    &filters,
                    &options,
                    &command_line,
                    &config,
                    cli.no_receipt,
                    reason.as_deref(),
                )?;
            }
            ExcludeAction::Duplicates {
                path,
                prefer,
                filters,
                dry_run,
                yes,
                reason,
            } => {
                let filters = alias::expand_filter_strings(&filters, &canon_home)?;
                exclude::exclude_duplicates(
                    &mut db,
                    &prefer,
                    Some(path.as_path()),
                    &filters,
                    dry_run,
                    yes,
                    &command_line,
                    &config,
                    cli.no_receipt,
                    reason.as_deref(),
                )?;
            }
            ExcludeAction::SetObject {
                paths,
                filters,
                hash,
                yes,
                verbose,
                global,
                reason,
            } => {
                let filters = alias::expand_filter_strings(&filters, &canon_home)?;
                let options = exclude::SetOptions {
                    dry_run: !yes,
                    verbose,
                    yes,
                };
                if let Some(h) = hash {
                    exclude::set_object_by_hash(
                        &mut db,
                        &h,
                        &options,
                        &command_line,
                        &config,
                        cli.no_receipt,
                        reason.as_deref(),
                    )?;
                } else if paths.len() == 1 && filters.is_empty() && paths[0].is_file() {
                    // Single file path: exclude that file's object
                    exclude::set_object_by_file(
                        &mut db,
                        &paths[0],
                        &options,
                        &command_line,
                        &config,
                        cli.no_receipt,
                        reason.as_deref(),
                    )?;
                } else {
                    let all_roots = repo::root::fetch_all(db.conn())?;
                    let resolved =
                        ops::scope::resolve_scope(db.conn(), &paths, global, &all_roots)?;
                    if resolved.prefixes.is_empty() && filters.is_empty() {
                        anyhow::bail!("Provide a hash (--hash), file path, or filters (--where)");
                    }
                    exclude::set_objects_by_filter(
                        &mut db,
                        &resolved.prefixes,
                        &filters,
                        &options,
                        &command_line,
                        &config,
                        cli.no_receipt,
                        reason.as_deref(),
                    )?;
                }
            }
            ExcludeAction::ClearObject { hash, dry_run } => {
                let options = exclude::ClearOptions { dry_run, yes: true };
                exclude::clear_object(
                    &mut db,
                    &hash,
                    &options,
                    &command_line,
                    &config,
                    cli.no_receipt,
                )?;
            }
            ExcludeAction::ListObjects => {
                exclude::list_objects(&db)?;
            }
        },
        Commands::Note {
            path,
            message,
            recursive,
            global,
            clear,
            yes,
            by_scope,
            limit,
        } => {
            notes::run(
                &mut db,
                path.as_deref(),
                message.as_deref(),
                recursive,
                global,
                clear,
                yes,
                by_scope,
                limit,
                &command_line,
                &config,
                cli.no_receipt,
            )?;
        }
        Commands::Trail {
            action,
            paths,
            global,
            today,
            since,
            on,
            limit,
            all,
            no_notes,
            jsonl,
        } => match action {
            Some(TrailAction::Show { id }) => trail::run_show(&mut db, id)?,
            None => trail::run(
                &mut db,
                trail::TrailArgs {
                    paths,
                    global,
                    today,
                    since,
                    on,
                    limit,
                    all,
                    no_notes,
                    jsonl,
                },
            )?,
        },
        Commands::Roots {
            action,
            path,
            suspended,
        } => match action {
            Some(RootsAction::List { path, suspended }) => {
                roots::list(&db, path.as_deref(), suspended)?;
            }
            None => {
                roots::list(&db, path.as_deref(), suspended)?;
            }
            Some(RootsAction::Rm { spec, yes, reason }) => {
                roots::remove(
                    &db,
                    &spec,
                    yes,
                    &command_line,
                    &config,
                    cli.no_receipt,
                    reason.as_deref(),
                )?;
            }
            Some(RootsAction::Comment { spec, comment }) => {
                roots::set_comment(&db, &spec, comment.as_deref())?;
            }
            Some(RootsAction::Suspend { spec }) => {
                roots::suspend(&db, &spec, &command_line, &config, cli.no_receipt)?;
            }
            Some(RootsAction::Unsuspend { spec }) => {
                roots::unsuspend(&db, &spec, &command_line, &config, cli.no_receipt)?;
            }
            Some(RootsAction::Retired) => {
                retire::retired(&db, &config)?;
            }
            Some(RootsAction::Story { spec, limit, all }) => {
                story::story(&db, &spec, limit, all)?;
            }
            Some(RootsAction::Retire {
                spec,
                dry_run,
                allow,
                reason,
                yes,
            }) => {
                let allow_unresolved = allow.contains(&RetireAllow::Unresolved);
                retire::retire(
                    &db,
                    &spec,
                    dry_run,
                    allow_unresolved,
                    reason.as_deref(),
                    yes,
                    &command_line,
                    &config,
                )?;
            }
        },
    }

    // Print profile summary if profiling was enabled
    repo::print_profile_summary(db.conn());

    Ok(())
}
