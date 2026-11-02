use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use std::path::{Path, PathBuf};

use crate::core::domain::config::{parse_ledger_config, LedgerConfig};
use crate::core::domain::root::{find_containing_root, DoorVerb};
use crate::core::domain::time::zone::{UserZone, ZoneLabel};
use crate::core::domain::IncludeSet;

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

/// The user's clock, read once per process through the zone's one owner. The first use
/// states on stderr, once, a zone Canon could not read — every time is then read in UTC.
///
/// Every command that selects, evaluates a pattern or prints a time reaches its clock here.
#[cfg(not(test))]
fn user_zone() -> &'static UserZone {
    static ZONE: std::sync::OnceLock<UserZone> = std::sync::OnceLock::new();
    ZONE.get_or_init(|| {
        let zone = UserZone::machine();
        if let Some(notice) = notice_for(&zone) {
            eprintln!("{notice}");
        }
        zone
    })
}

/// Under test the zone is whatever the test pinned (`with_test_zone`), read afresh each call:
/// a process-wide cache would hand one test's pin to every test after it.
#[cfg(test)]
fn user_zone() -> &'static UserZone {
    Box::leak(Box::new(UserZone::machine()))
}

/// This machine's zone for the manifest commands, read through the same owner **without** the
/// unknown-zone notice. A manifest's run reads the zone the manifest or its lock records, so
/// the notice's "times are shown and compared in UTC" would be false there; `cluster generate` and
/// `cluster refresh` name the machine's zone in their own refusals and statements instead,
/// and `cluster status` reads it only for what a refresh here would record.
fn manifest_machine_zone() -> UserZone {
    UserZone::machine()
}

/// What a user is told about their clock: nothing for a zone Canon can read, and for one it
/// cannot, that times are shown and compared in UTC and why.
fn notice_for(zone: &UserZone) -> Option<String> {
    match zone.label() {
        ZoneLabel::Unknown {
            unreadable_tz: None,
        } => Some(
            "this machine's time zone is unknown; times are shown and compared in UTC — set TZ to change it"
                .to_string(),
        ),
        ZoneLabel::Unknown {
            unreadable_tz: Some(tz),
        } => Some(format!(
            "TZ={tz} is not a time zone Canon can read; times are shown and compared in UTC"
        )),
        ZoneLabel::Named(_)
        | ZoneLabel::FixedOffset(_)
        | ZoneLabel::RuleString(_)
        | ZoneLabel::UnnamedRules => None,
    }
}

// The shared spine, which every subsystem is built on, and the expression
// facility — the language's one home, which every subsystem that takes a
// selection speaks through and no other re-derives.
mod core;
mod expr;

// Utilities
mod ceremony;
mod progress;
mod scope;

// The subsystems: each a directory of layer strata sealed behind a barrel,
// declared either in a flat file beside this one or in the subsystem's own
// `mod.rs`. A subsystem owns a coherent area of the tool rather than exactly
// one command — `archive` answers for `apply` and the three `cluster` verbs,
// `facts` for two commands, `roots` and `exclude` for several each.
//
// `ledger` in this list is the exception: a flat command module, never part of
// the restructuring that gave the others their own homes.
mod archive;
mod compare;
mod coverage;
mod exclude;
mod facts;
mod ledger;
mod ls;
mod notes;
mod retire;
mod roots;
mod scan;
mod story;
mod survey;
mod sweep;
mod trail;
mod worklist;

// The contentless law's canary. It sits at the crate root because it reads
// across every surface that consumes content identity, belonging to no one
// of them.
#[cfg(test)]
mod contentless_law_tests;

// The source-value law's verifier, at the crate root for the same reason: it reads a source's
// value through every surface that takes one — filter, report, pattern, worklist.
#[cfg(test)]
mod source_value_law_tests;

#[derive(Parser)]
#[command(name = "canon", version)]
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
    /// Scan directories or single files and add them to the index
    Scan {
        /// Paths to scan — a directory is walked, a single file observed (not required if --all is used)
        paths: Vec<PathBuf>,
        /// Role for new roots: 'source' or 'archive' (required with --add, optional filter with --all)
        #[arg(long)]
        role: Option<String>,
        /// Add path as a new root (required when path is not inside an existing root)
        #[arg(long, requires = "role")]
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
        /// Include a key's value for each source: a source fact, a built-in key, or a content
        /// fact (a name such as Make is read as content.Make)
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
        /// Show all source facts, including hidden ones like source.device and source.inode
        #[arg(long)]
        all: bool,
        /// Include additional sources: excluded, archived, all
        #[arg(long, value_delimiter = ',')]
        include: Vec<IncludeValue>,
        /// Show source count per root, or group fact values by root (with --key)
        #[arg(long)]
        by_root: bool,
        /// Group results by fact key(s) (requires --key). Supports modifiers. A comma separates
        /// keys only outside quotes, parentheses and square brackets.
        #[arg(long)]
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
    // The timeline flags below belong to the scoped/time views only; `trail show`
    // honors none of them, so clap must reject them beside the subcommand rather
    // than accept and drop them (same shape as `roots` and `facts`).
    #[command(args_conflicts_with_subcommands = true)]
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
        /// Story from the start of the given date or time onward: today, yesterday, a weekday, or a date or time (2024-06, 2024-06-15, ...)
        #[arg(long, conflicts_with = "on")]
        since: Option<String>,
        /// Story within the given date or time: today, yesterday, a weekday, or a date or time (2024-06, 2024-06-15, ...)
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
        /// Multi-line entries with each place's full absolute path
        #[arg(short = 'l', long)]
        long: bool,
        /// Emit timeline events as JSONL (machine output)
        #[arg(long)]
        jsonl: bool,
    },
    /// Prune orphaned or stale data from the database
    Prune {
        /// Delete objects that have no present sources, along with their facts and non-present sources
        #[arg(long)]
        orphaned_objects: bool,
        /// Delete facts stored on a source whose file changed since the fact was recorded
        #[arg(long)]
        stale_facts: bool,
        /// Delete facts stored on excluded sources or content: =source for sources only, =object for
        /// content only, omit for both
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
        #[arg(long, conflicts_with = "filters")]
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
        #[arg(long, conflicts_with = "filters")]
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
    /// Expand what moved between here and another place
    Crossings {
        /// Directory paths to scope the view (resolved to realpath)
        paths: Vec<PathBuf>,
        /// Narrow to content drawn from at or under this path
        #[arg(long, value_name = "PATH")]
        origin: Option<String>,
        /// Narrow to content placed at or under this path
        #[arg(long, value_name = "PATH")]
        destination: Option<String>,
        /// Read across all roots — needs --origin or --destination
        #[arg(long)]
        global: bool,
        /// Maximum entries per section (default: 20)
        #[arg(long, conflicts_with = "all")]
        limit: Option<usize>,
        /// Show all entries (no cap)
        #[arg(long)]
        all: bool,
        /// Emit the contributing decisions as JSONL (machine output)
        #[arg(long)]
        jsonl: bool,
    },
}

/// The type words `--to` and `--value-type` take, as printed, and quietly the older spellings,
/// which are accepted and never listed; and `duration`, which the command taking it answers —
/// the conversion with its routes, the deletion with where durations are stored. `--to` lists
/// it, since the conversion offers routes for it; `--value-type` accepts it without listing it,
/// since no deletion is narrowed to it.
fn type_word(list_duration: bool) -> impl clap::builder::TypedValueParser<Value = facts::TypeWord> {
    use clap::builder::{PossibleValue, PossibleValuesParser, TypedValueParser};
    PossibleValuesParser::new([
        PossibleValue::new("text"),
        PossibleValue::new("number").alias("num"),
        PossibleValue::new("time").alias("datetime"),
        PossibleValue::new("duration").hide(!list_duration),
    ])
    .try_map(|word| facts::TypeWord::parse_word(&word).ok_or(format!("not a type word: {word}")))
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
        /// Which facts: 'source' for facts stored on the source, 'object' for facts stored on the content
        #[arg(long, value_name = "TYPE")]
        on: String,
        /// Filter by value storage type (text, number, or time)
        #[arg(long, value_parser = type_word(false))]
        value_type: Option<facts::TypeWord>,
        /// Execute deletion (default is dry-run)
        #[arg(long)]
        yes: bool,
    },
    /// Show what a key holds and what each type would keep, lose and remove; convert it to one type
    Convert {
        /// Fact key to convert (e.g., "content.DateTimeOriginal")
        key: String,
        /// The type to convert to: text, number or time (default: show what the key holds)
        #[arg(long, value_name = "TYPE", value_parser = type_word(true))]
        to: Option<facts::TypeWord>,
        /// List each lossy or removed fact with its source, tab-separated
        #[arg(long, requires = "to", conflicts_with = "yes")]
        list: bool,
        /// With --list: separate fields with NUL and end each record with one
        #[arg(short = '0', long = "null", requires = "list")]
        null: bool,
        /// Acknowledge a cost the plan states: loss=N, removal=N (comma-separated, repeatable)
        #[arg(long, value_delimiter = ',', requires = "to")]
        allow: Vec<String>,
        /// Convert (default is a dry run)
        #[arg(long, requires = "to")]
        yes: bool,
        /// Reason for this operation (recorded in decision log)
        #[arg(long)]
        reason: Option<String>,
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
    /// Close the door on a root: out of view and not acted on, until you unsuspend
    Suspend {
        /// Root specifier: id:<N> or path:<path>
        spec: String,
        /// Reason for this operation (recorded in decision log)
        #[arg(long)]
        reason: Option<String>,
    },
    /// Open the door again on a suspended root
    Unsuspend {
        /// Root specifier: id:<N> or path:<path>
        spec: String,
        /// Reason for this operation (recorded in decision log)
        #[arg(long)]
        reason: Option<String>,
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
        /// Open manifest in $VISUAL/$EDITOR first, then refresh from what you saved
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

fn load_or_create_config(canon_home: &Path) -> (LedgerConfig, Vec<String>) {
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
        return (LedgerConfig::default(), warnings);
    }
    match std::fs::read_to_string(&path) {
        Ok(content) => parse_ledger_config(&content),
        Err(e) => {
            let warnings = vec![format!(
                "Warning: could not read {}: {e}, using defaults",
                path.display()
            )];
            (LedgerConfig::default(), warnings)
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

/// The channel a [`crate::core::domain::root::DoorRefused`] surfacing from
/// deep inside a command's own operations belongs on — the same channel that
/// command's own scope statements already use, so a door met early (at the
/// argument-scope boundary, already routed through `scope::open_door` inside
/// [`run`]) and one met later (inside an operation, carried out of it as a
/// `DoorRefused`) read as the same kind of refusal rather than switching
/// streams partway through one run.
///
/// **Only the commands this story's own charter named are routed here; the
/// wildcard arm is today's status quo (stderr), preserved rather than
/// guessed at for everything else.** This is a deliberate narrowing, not a
/// claim of completeness: several commands not named above *do* reach this
/// same catch with a `DoorRefused` raised from deep inside an op (`compare`,
/// `survey --other`, `exclude duplicates` — all three through
/// `core::ops::scope::refuse_parked_locations`), and today fall through to
/// the wildcard's stderr even though each of those commands' own scope
/// statement already prints on stdout. Wiring the remaining raise sites in
/// is named future work, not silently out of scope.
fn door_channel_for(command: &Commands) -> scope::DoorChannel {
    match command {
        // Effectful — the established channel for every other effectful
        // command's own scope statement (`facts delete`, `exclude
        // set/clear/set-object`, `cluster generate`'s own set-aside line).
        Commands::Scan { .. } | Commands::Apply { .. } => scope::DoorChannel::Stdout,
        Commands::Cluster { action } => match action {
            ClusterAction::Generate { .. } | ClusterAction::Refresh { .. } => {
                scope::DoorChannel::Stdout
            }
            ClusterAction::Status { .. } => scope::DoorChannel::Stderr,
        },
        // The ceremony's own printing is already on stdout throughout
        // (`retire/cli.rs`'s target-door refusal, its readiness report); the
        // shelf-precondition refusal (`validate_retire_target`) joins it.
        Commands::Roots {
            action: Some(RootsAction::Retire { .. }),
            ..
        } => scope::DoorChannel::Stdout,
        // The conversion's and the deletion's answers stand where their plans print.
        Commands::Facts {
            action: Some(FactsAction::Convert { .. } | FactsAction::Delete { .. }),
            ..
        } => scope::DoorChannel::Stdout,
        _ => scope::DoorChannel::Stderr,
    }
}

/// The front door.
///
/// Errors are rendered here rather than by `Result`'s own reporting, for one
/// reason: **a refusal that is itself a legitimate answer must not wear an
/// `Error:` prefix**. A closed door is that case exactly — the user shut it,
/// and being told so is the answer to the question they asked — so a
/// [`DoorRefused`] anywhere in the chain is stated as it stands, **on the
/// dispatched command's own class channel** ([`door_channel_for`]) rather
/// than unconditionally on stderr. A [`crate::expr::ExpressionRefused`] that is
/// only an answer about what a key is (mixed type, a type changed mid-run) is
/// stated the same way; one holding input Canon cannot read prints with
/// `Error:` on stderr. A refused `cluster refresh` ([`archive::RefreshRefused`])
/// carries `Error:` in its own text where it belongs and prints as it stands,
/// on the same rule. Everything else is a real failure and renders exactly as
/// before.
fn main() -> std::process::ExitCode {
    // Rust ignores SIGPIPE at startup, which turns a closed pipe into a panic in
    // `println!`. Restoring the default ends the process quietly, as it ends any
    // other Unix filter. A row left open reads `started`, the same as a panic.
    #[cfg(unix)]
    // SAFETY: called once, before any thread starts or any signal handler is set.
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_DFL);
    }
    let cli = Cli::parse();
    let channel = door_channel_for(&cli.command);
    match run(cli) {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(err) => {
            if let Some(refusal) = err.downcast_ref::<crate::core::domain::root::DoorRefused>() {
                match channel {
                    scope::DoorChannel::Stdout => println!("{refusal}"),
                    scope::DoorChannel::Stderr => eprintln!("{refusal}"),
                }
            } else if let Some(refusal) = err.downcast_ref::<archive::RefreshRefused>() {
                // A refused refresh carries `Error:` in its own text where a cause holds
                // input Canon cannot read, so it prints as it stands: on the command's
                // channel where it is an answer, on stderr otherwise.
                if refusal.is_answer() {
                    match channel {
                        scope::DoorChannel::Stdout => println!("{refusal}"),
                        scope::DoorChannel::Stderr => eprintln!("{refusal}"),
                    }
                } else {
                    eprintln!("{refusal}");
                }
            } else if let Some(refusal) = err.downcast_ref::<facts::ConvertRefused>() {
                // A conversion refused: the key's own answer, a target not reached, an
                // emptied key — answers, stated as they stand; a name Canon knows no facts
                // under is an error.
                if refusal.is_answer() {
                    match channel {
                        scope::DoorChannel::Stdout => println!("{refusal}"),
                        scope::DoorChannel::Stderr => eprintln!("{refusal}"),
                    }
                } else {
                    eprintln!("Error: {refusal}");
                }
            } else if let Some(refusal) = err.downcast_ref::<facts::DeleteRefused>() {
                // A deletion refused before anything is read — the content hash, a duration —
                // is an answer, stated as it stands.
                if refusal.is_answer() {
                    match channel {
                        scope::DoorChannel::Stdout => println!("{refusal}"),
                        scope::DoorChannel::Stderr => eprintln!("{refusal}"),
                    }
                } else {
                    eprintln!("Error: {refusal}");
                }
            } else if let Some(refusal) = err.downcast_ref::<crate::expr::ExpressionRefused>() {
                // A filter Canon will not run: an answer about what a key is prints as it
                // stands on the command's class channel, as a closed door does; anything
                // holding input Canon cannot read is an error.
                if refusal.is_answer() {
                    match channel {
                        scope::DoorChannel::Stdout => println!("{refusal}"),
                        scope::DoorChannel::Stderr => eprintln!("{refusal}"),
                    }
                } else {
                    eprintln!("Error: {refusal}");
                }
            } else {
                eprintln!("Error: {err:?}");
            }
            std::process::ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> Result<()> {
    let command_line = std::env::args().collect::<Vec<_>>().join(" ");

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

    let mut db = core::repo::open_with_options(
        &db_path,
        core::repo::DbOptions {
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
        let roots = core::repo::root::fetch_all(db.conn())?;
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
            // --role's presence with --add is the parser's own invariant now
            // (`#[arg(requires = "role")]` on `add`), never re-checked here.
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
            let filters = expr::ExpressionText::given(filters, &canon_home);
            let mut include = include_set_from(&include);
            let all_roots = core::repo::root::fetch_all(db.conn())?;
            let resolved = scope::open_door(
                core::ops::scope::resolve_scope(db.conn(), &paths, global, &all_roots)?,
                DoorVerb::SetAside,
                scope::DoorChannel::Stderr,
            );
            if resolved.auto_include_archived {
                include.archived = true;
            }
            scope::print_list_scope(&mut std::io::stderr(), &resolved);
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
                std::io::BufReader::new(std::io::stdin()),
                allow_archived,
                verbose,
                &command_line,
                &config,
                cli.no_receipt,
                &mut std::io::stderr(),
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
            let filters = expr::ExpressionText::given(filters, &canon_home);
            let mut include = include_set_from(&include);

            let all_roots = core::repo::root::fetch_all(db.conn())?;
            let resolved = scope::open_door(
                core::ops::scope::resolve_scope(db.conn(), &paths, global, &all_roots)?,
                DoorVerb::SetAside,
                scope::DoorChannel::Stderr,
            );
            if resolved.auto_include_archived {
                include.archived = true;
            }

            let use_relative = resolved.from_cwd;

            scope::print_list_scope(&mut std::io::stderr(), &resolved);

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
            include,
            by_root,
            group_by,
        } => {
            match action {
                Some(FactsAction::Delete {
                    key,
                    paths,
                    filters,
                    on,
                    value_type,
                    yes,
                }) => {
                    let filters = expr::ExpressionText::given(filters, &canon_home);
                    let all_roots = core::repo::root::fetch_all(db.conn())?;
                    let resolved = scope::open_door(
                        core::ops::scope::resolve_scope(db.conn(), &paths, false, &all_roots)?,
                        DoorVerb::Refused,
                        scope::DoorChannel::Stdout,
                    );
                    scope::print_scope_set_asides(&resolved, DoorVerb::Refused);
                    // Same visibility a read at this scope would get: the
                    // archive-CWD auto-enable, and nothing else. Deletion
                    // acts on what the matching `ls` would have listed.
                    let mut include = IncludeSet::default();
                    if resolved.auto_include_archived {
                        include.archived = true;
                    }
                    let options = facts::DeleteOptions {
                        entity_type: on,
                        value_type,
                        dry_run: !yes,
                        include,
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
                Some(FactsAction::Convert {
                    key,
                    to,
                    list,
                    null,
                    allow,
                    yes,
                    reason,
                }) => {
                    let options = facts::ConvertOptions {
                        key,
                        to,
                        list,
                        null,
                        allow,
                        yes,
                        reason,
                    };
                    facts::convert(&mut db, &options, &canon_home, &command_line, &config)?;
                }
                None => {
                    let filters = expr::ExpressionText::given(filters, &canon_home);
                    let mut include = include_set_from(&include);
                    let all_roots = core::repo::root::fetch_all(db.conn())?;
                    let resolved = scope::open_door(
                        core::ops::scope::resolve_scope(db.conn(), &paths, global, &all_roots)?,
                        DoorVerb::SetAside,
                        scope::DoorChannel::Stdout,
                    );
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
            let filters = expr::ExpressionText::given(filters, &canon_home);
            let mut include = include_set_from(&include);
            let all_roots = core::repo::root::fetch_all(db.conn())?;
            let resolved = scope::open_door(
                core::ops::scope::resolve_scope(db.conn(), &paths, global, &all_roots)?,
                DoorVerb::SetAside,
                if compact {
                    scope::DoorChannel::Stderr
                } else {
                    scope::DoorChannel::Stdout
                },
            );
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
            let filters = expr::ExpressionText::given(filters, &canon_home);
            let include = include_set_from(&include);
            // Survey has no archived-visibility axis to widen: its selection
            // side is source roots by construction and its outward side always
            // sees archives (the asymmetric-visibility model). So the flag is
            // refused, and the archive-CWD auto-enable other commands honor
            // does not apply here.
            if include.includes_archived() {
                bail!(
                    "--include archived is not valid for survey.\n\
                     Survey works on source-side selections; archive content is its outward\n\
                     side, always visible.\n\
                     For what stands in an archive location: canon trail\n\
                     For a listing of what's there: canon ls"
                );
            }
            let all_roots = core::repo::root::fetch_all(db.conn())?;
            let resolved = scope::open_door(
                core::ops::scope::resolve_scope(db.conn(), &paths, global, &all_roots)?,
                DoorVerb::SetAside,
                if survey::machine_shaped_stdout(detail, null_delim) {
                    scope::DoorChannel::Stderr
                } else {
                    scope::DoorChannel::Stdout
                },
            );
            let scope_prefixes = resolved.prefixes.clone();
            let options = survey::SurveyOptions {
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
            // A refused frame is a well-formed question survey is the wrong
            // instrument for, not an error: non-zero, no `Error:` prefix —
            // the shape compare uses for a non-identical result.
            if matches!(
                survey::run(&mut db, &scope_prefixes, &filters, &options)?,
                survey::SurveyExit::FrameRefused
            ) {
                std::process::exit(1);
            }
        }
        Commands::Sweep { limit, all } => sweep::run(&mut db, limit, all)?,
        Commands::Compare {
            paths,
            filters,
            include,
            verbose,
        } => {
            let filters = expr::ExpressionText::given(filters, &canon_home);
            let include = include_set_from(&include);
            if include.includes_archived() {
                bail!("--include archived is not valid for compare (valid values: excluded)");
            }
            let all_roots = core::repo::root::fetch_all(db.conn())?;
            let (path_a, path_b) = if paths.len() == 2 {
                (paths[0].clone(), paths[1].clone())
            } else {
                // One path: CWD as side A
                let cwd = std::env::current_dir()?;
                let cwd_resolved = core::ops::scope::resolve_path(&cwd, &all_roots, &cwd)?;
                if find_containing_root(&cwd_resolved, &all_roots).is_none() {
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
                let filters = expr::ExpressionText::given(filters, &canon_home);
                let all_roots = core::repo::root::fetch_all(db.conn())?;
                let resolved = scope::open_door(
                    core::ops::scope::resolve_scope(db.conn(), &paths, global, &all_roots)?,
                    DoorVerb::Refused,
                    scope::DoorChannel::Stdout,
                );
                scope::print_scope_set_asides(&resolved, DoorVerb::Refused);
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
                    &dest,
                    &output_path,
                    &options,
                    &command_line,
                    &config,
                    cli.no_receipt,
                    &manifest_machine_zone(),
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
                    edit,
                    &command_line,
                    &config,
                    cli.no_receipt,
                    &manifest_machine_zone(),
                )?;
            }
            ClusterAction::Status { manifest, verbose } => {
                archive::status(db.conn_mut(), &manifest, verbose, &manifest_machine_zone())?;
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
                handle_interrupts: true,
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
                let filters = expr::ExpressionText::given(filters, &canon_home);
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
                } else if paths.len() == 1 && filters.typed().is_empty() && paths[0].is_file() {
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
                    let all_roots = core::repo::root::fetch_all(db.conn())?;
                    let resolved = scope::open_door(
                        core::ops::scope::resolve_scope(db.conn(), &paths, global, &all_roots)?,
                        DoorVerb::Refused,
                        scope::DoorChannel::Stdout,
                    );
                    scope::print_scope_set_asides(&resolved, DoorVerb::Refused);
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
                let filters = expr::ExpressionText::given(filters, &canon_home);
                let options = exclude::ClearOptions { dry_run, yes };
                let all_roots = core::repo::root::fetch_all(db.conn())?;
                let resolved = scope::open_door(
                    core::ops::scope::resolve_scope(db.conn(), &paths, global, &all_roots)?,
                    DoorVerb::Refused,
                    scope::DoorChannel::Stdout,
                );
                scope::print_scope_set_asides(&resolved, DoorVerb::Refused);
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
                let filters = expr::ExpressionText::given(filters, &canon_home);
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
                let filters = expr::ExpressionText::given(filters, &canon_home);
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
                } else if paths.len() == 1 && filters.typed().is_empty() && paths[0].is_file() {
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
                    let all_roots = core::repo::root::fetch_all(db.conn())?;
                    let resolved = scope::open_door(
                        core::ops::scope::resolve_scope(db.conn(), &paths, global, &all_roots)?,
                        DoorVerb::Refused,
                        scope::DoorChannel::Stdout,
                    );
                    scope::print_scope_set_asides(&resolved, DoorVerb::Refused);
                    if resolved.prefixes.is_empty() && filters.typed().is_empty() {
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
            long,
            jsonl,
        } => match action {
            Some(TrailAction::Show { id }) => trail::run_show(&mut db, id)?,
            Some(TrailAction::Crossings {
                paths,
                origin,
                destination,
                global,
                limit,
                all,
                jsonl,
            }) => {
                // Same exit contract as the timeline: a place Canon has never
                // known is a well-formed question answered, not an error.
                if matches!(
                    trail::run_crossings(
                        &mut db,
                        trail::CrossingsArgs {
                            paths,
                            origin,
                            destination,
                            global,
                            limit,
                            all,
                            jsonl,
                        },
                    )?,
                    trail::TrailExit::PlaceUnknown
                ) {
                    std::process::exit(1);
                }
            }
            None => {
                // A place Canon has never known is a well-formed question
                // answered, not an error: non-zero, no `Error:` prefix.
                if matches!(
                    trail::run(
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
                            long,
                            jsonl,
                        },
                    )?,
                    trail::TrailExit::PlaceUnknown
                ) {
                    std::process::exit(1);
                }
            }
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
            Some(RootsAction::Suspend { spec, reason }) => {
                roots::suspend(
                    &db,
                    &spec,
                    &command_line,
                    &config,
                    cli.no_receipt,
                    reason.as_deref(),
                )?;
            }
            Some(RootsAction::Unsuspend { spec, reason }) => {
                roots::unsuspend(
                    &db,
                    &spec,
                    &command_line,
                    &config,
                    cli.no_receipt,
                    reason.as_deref(),
                )?;
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
    core::repo::print_profile_summary(db.conn());

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::domain::fact::FactType;

    #[test]
    fn a_zone_canon_reads_gets_no_notice() {
        assert_eq!(notice_for(&UserZone::for_test("Europe/Amsterdam")), None);
        assert_eq!(notice_for(&UserZone::utc()), None);
    }

    #[test]
    fn an_unknown_zone_is_stated_with_utc_as_the_reading() {
        let zone = UserZone::for_test_unknown(None);
        assert_eq!(
            notice_for(&zone).as_deref(),
            Some(
                "this machine's time zone is unknown; times are shown and compared in UTC — set TZ to change it"
            )
        );
    }

    #[test]
    fn an_unreadable_tz_is_named_in_the_notice() {
        let zone = UserZone::for_test_unknown(Some("Europe/Amsterdm"));
        assert_eq!(
            notice_for(&zone).as_deref(),
            Some("TZ=Europe/Amsterdm is not a time zone Canon can read; times are shown and compared in UTC")
        );
    }

    fn value_type_of(word: &str) -> Option<facts::TypeWord> {
        let argv = [
            "canon",
            "facts",
            "delete",
            "content.W",
            "--on",
            "object",
            "--value-type",
            word,
        ];
        match Cli::try_parse_from(argv).ok()?.command {
            Commands::Facts {
                action: Some(FactsAction::Delete { value_type, .. }),
                ..
            } => value_type,
            _ => None,
        }
    }

    /// `num` and `number` are one type, and so are `datetime` and `time`; `duration` is read,
    /// for the deletion to answer; `path` is not offered.
    #[test]
    fn value_type_num_and_number_delete_the_same_rows() {
        use facts::TypeWord;
        assert_eq!(value_type_of("number"), Some(TypeWord::Type(FactType::Num)));
        assert_eq!(value_type_of("num"), Some(TypeWord::Type(FactType::Num)));
        assert_eq!(value_type_of("time"), Some(TypeWord::Type(FactType::Time)));
        assert_eq!(
            value_type_of("datetime"),
            Some(TypeWord::Type(FactType::Time))
        );
        assert_eq!(value_type_of("text"), Some(TypeWord::Type(FactType::Text)));
        assert_eq!(value_type_of("duration"), Some(TypeWord::Duration));
        assert_eq!(value_type_of("path"), None);
    }

    /// **A deletion's and a conversion's answers stand on stdout, where their plans print**: a
    /// refused deletion or conversion, and a deletion's answer about a filter's key or a closed
    /// door, print on the command's channel.
    #[test]
    fn facts_delete_answers_on_stdout() {
        for argv in [
            &["canon", "facts", "delete", "hash", "--on", "object"][..],
            &["canon", "facts", "convert", "hash"][..],
        ] {
            let cli = Cli::try_parse_from(argv).expect("parses");
            assert!(
                matches!(door_channel_for(&cli.command), scope::DoorChannel::Stdout),
                "{argv:?}"
            );
        }
    }

    /// The whole help line names `number`, and `num` appears nowhere in it. `duration` is read
    /// (`value_type_num_and_number_delete_the_same_rows`) but not listed, where `facts convert
    /// --to` lists it.
    #[test]
    fn the_value_type_help_line_says_number() {
        use clap::CommandFactory;
        let mut cmd = Cli::command();
        let help = cmd
            .find_subcommand_mut("facts")
            .and_then(|c| c.find_subcommand_mut("delete"))
            .expect("facts delete")
            .render_long_help()
            .to_string();
        let line: String = help
            .lines()
            .skip_while(|l| !l.contains("--value-type"))
            .take_while(|l| !l.trim_start().starts_with("--yes"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(line.contains("text, number, or time"), "{line}");
        assert!(
            line.contains("[possible values: text, number, time]"),
            "{line}"
        );
        let convert = cmd
            .find_subcommand_mut("facts")
            .and_then(|c| c.find_subcommand_mut("convert"))
            .expect("facts convert")
            .render_long_help()
            .to_string();
        assert!(
            convert.contains("[possible values: text, number, time, duration]"),
            "{convert}"
        );
        assert!(!line.contains("num,") && !line.contains("num]"), "{line}");
        assert!(!line.contains("datetime"), "{line}");
    }

    /// A flag the named subcommand cannot honor must be refused, never dropped.
    #[test]
    fn trail_show_refuses_timeline_flags_it_cannot_honor() {
        for flag in [
            vec!["--global"],
            vec!["--since", "today"],
            vec!["--jsonl"],
            vec!["--all"],
        ] {
            let mut argv = vec!["canon", "trail", "show", "7"];
            argv.extend(flag.iter());
            assert!(
                Cli::try_parse_from(&argv).is_err(),
                "expected {argv:?} to be refused"
            );
        }
    }

    #[test]
    fn trail_timeline_flags_still_parse_without_a_subcommand() {
        assert!(Cli::try_parse_from(["canon", "trail", "--global", "--jsonl"]).is_ok());
        assert!(Cli::try_parse_from(["canon", "trail", "show", "7"]).is_ok());
    }

    /// Inside a subcommand every flag carries a value, so the positional /
    /// flag ambiguity that rules out `--destination` on the bare timeline
    /// dissolves: `crossings --destination /vol/x` cannot be misread as
    /// "expand destinations, scoped to /vol/x".
    #[test]
    fn crossings_is_a_subcommand_beside_show() {
        assert!(Cli::try_parse_from(["canon", "trail", "crossings"]).is_ok());
        assert!(Cli::try_parse_from(["canon", "trail", "crossings", "/archive"]).is_ok());
        assert!(Cli::try_parse_from([
            "canon",
            "trail",
            "crossings",
            "--origin",
            "/vol/sd",
            "--destination",
            "/archive/Media"
        ])
        .is_ok());
        assert!(Cli::try_parse_from(["canon", "trail", "crossings", "--global"]).is_ok());
    }

    /// Neither counterpart flag has a bare form: a value is always required,
    /// so the flag can never swallow the positional scope path beside it.
    #[test]
    fn the_counterpart_flags_always_require_a_value() {
        for flag in ["--origin", "--destination"] {
            assert!(
                Cli::try_parse_from(["canon", "trail", "crossings", flag]).is_err(),
                "{flag} must require a value"
            );
        }
    }

    /// The timeline flags belong to the timeline; `crossings` honors none of
    /// them, so clap must refuse rather than accept and drop them.
    #[test]
    fn crossings_refuses_timeline_flags_it_cannot_honor() {
        for flag in [
            vec!["--since", "today"],
            vec!["--today"],
            vec!["--no-notes"],
            vec!["-l"],
        ] {
            let mut argv = vec!["canon", "trail", "crossings"];
            argv.extend(flag.iter());
            assert!(
                Cli::try_parse_from(&argv).is_err(),
                "expected {argv:?} to be refused"
            );
        }
    }

    /// **Every emitted way back parses as a real invocation.** A way back that
    /// does not run is worse than none, and the closed door offers one at
    /// every surface Canon has. The sweep's footer already lives under this
    /// law; the door joins it through the same spelling, so one pin covers
    /// both — `roots unsuspend` takes a root specifier, never a bare path.
    #[test]
    fn every_emitted_way_back_parses() {
        use crate::core::domain::root::WayBack;

        for way_back in [
            WayBack::unsuspend("/mnt/d1"),
            WayBack::unsuspend("/mnt/two words"),
            WayBack::list_suspended(),
        ] {
            Cli::try_parse_from(way_back.argv()).unwrap_or_else(|e| {
                panic!("emitted command must parse: {}\n{e}", way_back.display())
            });
        }

        // The spaced path is quoted for display and raw in the argv — the
        // same discipline the sweep's handoff lines follow.
        let spaced = WayBack::unsuspend("/mnt/two words");
        assert_eq!(
            spaced.display(),
            "canon roots unsuspend 'path:/mnt/two words'"
        );
        assert_eq!(spaced.argv().last().unwrap(), "path:/mnt/two words");
    }

    /// The sweep's footer, the closed door's sentence and the unplaceable
    /// receipt's hint are three surfaces of one command. One spelling, so a
    /// repair to any of them repairs all three.
    #[test]
    fn the_sweep_and_the_door_share_one_way_back() {
        use crate::core::domain::root::{DoorVerb, ParkedRoot, WayBack};

        let expected = WayBack::unsuspend("/mnt/d1").display();

        let parked = ParkedRoot {
            root_id: 1,
            root_path: "/mnt/d1".to_string(),
        };
        assert!(parked
            .door_line(DoorVerb::Refused, "here")
            .ends_with(&expected));
        assert!(parked.pause_line().ends_with(&expected));

        let receipt_hint = core::ops::receipt::LedgerRootOutcome::AllArchiveRootsSuspended {
            roots: vec![parked.clone()],
        }
        .unsuspend_hint();
        assert_eq!(receipt_hint.as_deref(), Some(expected.as_str()));
    }

    /// Closing a door and opening one are recorded acts like any other, so
    /// they take `--reason` under its existing meaning — and the way back
    /// Canon prints everywhere still parses beside it, which is what stops the
    /// flag from being added on one form only.
    #[test]
    fn the_suspend_family_takes_a_reason() {
        for argv in [
            vec!["canon", "roots", "suspend", "path:/mnt/d1"],
            vec![
                "canon",
                "roots",
                "suspend",
                "path:/mnt/d1",
                "--reason",
                "steering the sweep; revisit after the phone import",
            ],
            vec![
                "canon",
                "roots",
                "unsuspend",
                "id:7",
                "--reason",
                "back to work on it",
            ],
        ] {
            Cli::try_parse_from(&argv).unwrap_or_else(|e| panic!("must parse: {argv:?}\n{e}"));
        }
    }

    /// `canon facts` takes no `--show-aliases`: an alias is an `@name` from the aliases
    /// file, and the built-in keys end the plain `canon facts` list.
    #[test]
    fn show_aliases_is_an_unknown_flag() {
        let refused = Cli::try_parse_from(["canon", "facts", "--show-aliases"])
            .err()
            .expect("--show-aliases must be refused");
        assert_eq!(refused.kind(), clap::error::ErrorKind::UnknownArgument);
        assert!(Cli::try_parse_from(["canon", "facts"]).is_ok());
    }

    /// An exclusion by id names one source, so a filter beside it can only be
    /// ignored. Refused while parsing: nothing opens the database and a broken
    /// filter is never read.
    #[test]
    fn exclude_id_with_a_filter_is_refused() {
        let refused = Cli::try_parse_from([
            "canon",
            "exclude",
            "set",
            "--id",
            "5",
            "--where",
            "BROKEN(((",
        ])
        .err()
        .expect("--id with --where must be refused");
        assert_eq!(refused.kind(), clap::error::ErrorKind::ArgumentConflict);
        assert!(Cli::try_parse_from(["canon", "exclude", "set", "--id", "5"]).is_ok());
        assert!(Cli::try_parse_from(["canon", "exclude", "set", "--where", "x=1"]).is_ok());
    }

    /// The same for an object named by its hash.
    #[test]
    fn exclude_object_hash_with_a_filter_is_refused() {
        let refused = Cli::try_parse_from([
            "canon",
            "exclude",
            "set-object",
            "--hash",
            "abc",
            "--where",
            "BROKEN(((",
        ])
        .err()
        .expect("--hash with --where must be refused");
        assert_eq!(refused.kind(), clap::error::ErrorKind::ArgumentConflict);
        assert!(Cli::try_parse_from(["canon", "exclude", "set-object", "--hash", "abc"]).is_ok());
        assert!(Cli::try_parse_from(["canon", "exclude", "set-object", "--where", "x=1"]).is_ok());
    }
}
