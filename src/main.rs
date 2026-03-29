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

// Utilities
mod alias;
mod ceremony;
mod progress;
mod scope;

// Command modules
mod apply;
mod cluster;
mod compare;
mod coverage;
mod exclude;
mod facts;
mod import_facts;
mod ls;
mod note;
mod roots;
mod scan;
mod survey;
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
    },
    /// Manage source exclusions
    Exclude {
        #[command(subcommand)]
        action: ExcludeAction,
    },
    /// Annotate locations with notes
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
        /// Don't open manifest in $VISUAL/$EDITOR after refresh
        #[arg(long)]
        no_edit: bool,
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
    let cli = Cli::parse();

    let canon_home = resolve_canon_home(cli.canon_home.as_deref())?;
    if canon_home.exists() && !canon_home.is_dir() {
        bail!(
            "CANON_HOME path is not a directory: {}",
            canon_home.display()
        );
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
            worklist::run(&mut db, &resolved.prefixes, &filters, &include, unique_content, &emit)?;
        }
        Commands::ImportFacts { allow, verbose } => {
            let allow_archived = allow.contains(&ImportFactsAllow::Archived);
            import_facts::run(&mut db, allow_archived, verbose)?;
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
                    facts::delete_facts(&mut db, &key, &resolved.prefixes, &filters, &options)?;
                }
                None => {
                    let filters = alias::expand_filter_strings(&filters, &canon_home)?;
                    let mut include = include_set_from(&include);
                    let all_roots = repo::root::fetch_all(db.conn())?;
                    let resolved = ops::scope::resolve_scope(db.conn(), &paths, global, &all_roots)?;
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
                facts::prune_stale(&db, !yes)?;
            }
            if orphaned_objects {
                facts::prune_orphaned_objects(&mut db, !yes)?;
            }
            if let Some(scope) = excluded_facts {
                facts::prune_excluded_facts(&db, &scope, !yes)?;
            }
        }
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
                let cwd_resolved = domain::path::resolve_path(&cwd, &all_roots, &cwd)?;
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
                let options = cluster::GenerateOptions {
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
                cluster::generate(
                    &mut db,
                    &resolved.prefixes,
                    &filters,
                    &expanded,
                    &dest,
                    &output_path,
                    &options,
                )?;
            }
            ClusterAction::Refresh {
                manifest,
                show_archived,
                no_edit,
            } => {
                cluster::refresh(&mut db, &manifest, show_archived, no_edit)?;
            }
            ClusterAction::Status { manifest, verbose } => {
                cluster::status(db.conn_mut(), &manifest, verbose)?;
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
        } => {
            let transfer_mode = if rename {
                ops::apply::TransferMode::Rename
            } else if move_files {
                ops::apply::TransferMode::Move
            } else {
                ops::apply::TransferMode::Copy
            };
            let options = apply::ApplyOptions {
                dry_run,
                verbose,
                allow_cross_archive_duplicates: allow.contains(&ApplyAllow::CrossArchiveDuplicates),
                allow_duplicates: allow.contains(&ApplyAllow::Duplicates),
                roots: root,
                transfer_mode,
                yes,
                resume,
            };
            apply::run(&mut db, &manifest, &options)?;
        }
        Commands::Exclude { action } => match action {
            ExcludeAction::Set {
                paths,
                filters,
                id,
                dry_run,
                yes,
                global,
            } => {
                let filters = alias::expand_filter_strings(&filters, &canon_home)?;
                let options = exclude::SetOptions {
                    dry_run,
                    verbose: false,
                    yes,
                };
                if let Some(source_id) = id {
                    exclude::set_by_id(&db, source_id, &options)?;
                } else if paths.len() == 1 && filters.is_empty() && paths[0].is_file() {
                    // Single file path with no filters: exclude exact file
                    exclude::set_by_path(&db, &paths[0], &options)?;
                } else {
                    let all_roots = repo::root::fetch_all(db.conn())?;
                    let resolved = ops::scope::resolve_scope(db.conn(), &paths, global, &all_roots)?;
                    exclude::set(&mut db, &resolved.prefixes, &filters, &options)?;
                }
            }
            ExcludeAction::Clear {
                paths,
                filters,
                dry_run,
                yes,
                global,
            } => {
                let filters = alias::expand_filter_strings(&filters, &canon_home)?;
                let options = exclude::ClearOptions { dry_run, yes };
                let all_roots = repo::root::fetch_all(db.conn())?;
                let resolved = ops::scope::resolve_scope(db.conn(), &paths, global, &all_roots)?;
                exclude::clear(&mut db, &resolved.prefixes, &filters, &options)?;
            }
            ExcludeAction::Duplicates {
                path,
                prefer,
                filters,
                dry_run,
                yes,
            } => {
                let filters = alias::expand_filter_strings(&filters, &canon_home)?;
                exclude::exclude_duplicates(
                    &mut db,
                    &prefer,
                    Some(path.as_path()),
                    &filters,
                    dry_run,
                    yes,
                )?;
            }
            ExcludeAction::SetObject {
                paths,
                filters,
                hash,
                yes,
                verbose,
                global,
            } => {
                let filters = alias::expand_filter_strings(&filters, &canon_home)?;
                let options = exclude::SetOptions {
                    dry_run: !yes,
                    verbose,
                    yes,
                };
                if let Some(h) = hash {
                    exclude::set_object_by_hash(&db, &h, &options)?;
                } else if paths.len() == 1 && filters.is_empty() && paths[0].is_file() {
                    // Single file path: exclude that file's object
                    exclude::set_object_by_file(&db, &paths[0], &options)?;
                } else {
                    let all_roots = repo::root::fetch_all(db.conn())?;
                    let resolved = ops::scope::resolve_scope(db.conn(), &paths, global, &all_roots)?;
                    if resolved.prefixes.is_empty() && filters.is_empty() {
                        anyhow::bail!("Provide a hash (--hash), file path, or filters (--where)");
                    }
                    exclude::set_objects_by_filter(&mut db, &resolved.prefixes, &filters, &options)?;
                }
            }
            ExcludeAction::ClearObject { hash, dry_run } => {
                let options = exclude::ClearOptions { dry_run, yes: true };
                exclude::clear_object(&db, &hash, &options)?;
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
            note::run(
                &db,
                path.as_deref(),
                message.as_deref(),
                recursive,
                global,
                clear,
                yes,
                by_scope,
                limit,
            )?;
        }
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
            Some(RootsAction::Rm { spec, yes }) => {
                roots::remove(&db, &spec, yes)?;
            }
            Some(RootsAction::Comment { spec, comment }) => {
                roots::set_comment(&db, &spec, comment.as_deref())?;
            }
            Some(RootsAction::Suspend { spec }) => {
                roots::suspend(&db, &spec)?;
            }
            Some(RootsAction::Unsuspend { spec }) => {
                roots::unsuspend(&db, &spec)?;
            }
        },
    }


    // Print profile summary if profiling was enabled
    repo::print_profile_summary(db.conn());

    Ok(())
}
