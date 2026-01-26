use clap::{Parser, Subcommand};
use std::path::PathBuf;

// Infrastructure layers
mod domain;
mod expr;
mod repo;

// Utilities
mod progress;

// Command modules
mod apply;
mod cluster;
mod compare;
mod coverage;
mod exclude;
mod facts;
mod import_facts;
mod ls;
mod roots;
mod scan;
mod worklist;

#[derive(Parser)]
#[command(name = "canon")]
#[command(about = "Organize large media libraries into a canonical archive")]
struct Cli {
    /// Path to the database file
    #[arg(long, global = true)]
    db: Option<PathBuf>,

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
        /// Include sources from archive roots (by default only source roots)
        #[arg(long)]
        include_archived: bool,
        /// Include excluded sources (by default they are skipped)
        #[arg(long)]
        include_excluded: bool,
        /// Emit only one source per unique content hash (sources without a hash are skipped)
        #[arg(long)]
        unique_content: bool,
        /// Include specific facts in the output (e.g., --emit geo.lat --emit geo.lon)
        #[arg(long)]
        emit: Vec<String>,
    },
    /// Import facts from JSONL on stdin
    ImportFacts {
        /// Allow importing facts for sources in archive roots
        #[arg(long)]
        allow_archived: bool,
        /// Show each fact as it's imported
        #[arg(short, long)]
        verbose: bool,
    },
    // -- Discover --
    /// List sources matching filters
    Ls {
        /// Directory paths to scope the query (resolved to realpath)
        paths: Vec<PathBuf>,
        /// Filter expressions (e.g., "source.ext=jpg" or "content.hash.sha256?")
        #[arg(long = "where")]
        filters: Vec<String>,
        /// Only show archived sources (use --archived=show to include archive paths)
        #[arg(long, value_name = "MODE", num_args = 0..=1, default_missing_value = "list", conflicts_with_all = ["unarchived", "unhashed", "duplicates"])]
        archived: Option<String>,
        /// Only show unarchived sources (hashed but not in any archive)
        #[arg(long, conflicts_with_all = ["archived", "unhashed", "duplicates"])]
        unarchived: bool,
        /// Only show unhashed sources (no content hash yet)
        #[arg(long, conflicts_with_all = ["archived", "unarchived", "duplicates"])]
        unhashed: bool,
        /// Show sources with duplicate content (same hash), grouped by hash
        #[arg(long, conflicts_with_all = ["archived", "unarchived", "unhashed"])]
        duplicates: bool,
        /// Include sources from archive roots (by default only source roots)
        #[arg(long)]
        include_archived: bool,
        /// Include excluded sources (by default they are skipped)
        #[arg(long)]
        include_excluded: bool,
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
        /// Maximum number of values to show (0 for unlimited, default 50)
        #[arg(long, default_value = "50")]
        limit: usize,
        /// Show all built-in facts (including hidden ones like source.device, source.inode)
        #[arg(long)]
        all: bool,
        /// Show pattern aliases available for manifest patterns
        #[arg(long)]
        show_aliases: bool,
        /// Include sources from archive roots (by default only source roots)
        #[arg(long)]
        include_archived: bool,
        /// Include excluded sources (by default they are skipped)
        #[arg(long)]
        include_excluded: bool,
        /// Group results by root (requires --key)
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
        /// Filter coverage relative to a specific archive (id:N or path:/foo/bar)
        #[arg(long)]
        archive: Option<String>,
        /// Include sources from archive roots (by default only source roots)
        #[arg(long)]
        include_archived: bool,
        /// Include excluded sources (by default they are skipped)
        #[arg(long)]
        include_excluded: bool,
        /// Compact output: one line per root
        #[arg(long)]
        compact: bool,
    },
    /// Compare two folders by content hash
    Compare {
        /// First path to compare
        path_a: PathBuf,
        /// Second path to compare
        path_b: PathBuf,
        /// Filter expressions (e.g., "source.ext=jpg")
        #[arg(long = "where")]
        filters: Vec<String>,
        /// Include excluded sources (by default they are skipped)
        #[arg(long)]
        include_excluded: bool,
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
        /// Allow copying files that exist in other archives (but not destination archive)
        #[arg(long)]
        allow_cross_archive_duplicates: bool,
        /// Allow copying files that already exist in the destination archive (same content, different path)
        #[arg(long)]
        allow_duplicates: bool,
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
    },
    /// Manage source exclusions
    Exclude {
        #[command(subcommand)]
        action: ExcludeAction,
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
    },
    /// List excluded sources
    List {
        /// Directory paths to scope the query (resolved to realpath)
        paths: Vec<PathBuf>,
        /// Filter expressions to match excluded sources
        #[arg(long = "where")]
        filters: Vec<String>,
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
        /// Overwrite existing output file
        #[arg(short, long)]
        force: bool,
        /// Include files already in an archive (by default they are excluded)
        #[arg(long)]
        include_archived: bool,
        /// Show which files were excluded because they're already archived
        #[arg(long)]
        show_archived: bool,
        /// Allow sources with duplicate content (same hash) in the manifest
        #[arg(long)]
        allow_duplicates: bool,
    },
    /// Regenerate lock file from existing manifest config
    Refresh {
        /// Path to manifest TOML file
        manifest: PathBuf,
        /// Include files already in an archive (by default they are excluded)
        #[arg(long)]
        include_archived: bool,
        /// Show which files were excluded because they're already archived
        #[arg(long)]
        show_archived: bool,
        /// Allow sources with duplicate content (same hash) in the manifest
        #[arg(long)]
        allow_duplicates: bool,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let db_path = cli.db.unwrap_or_else(|| {
        let mut path = dirs::home_dir().expect("Could not determine home directory");
        path.push(".canon");
        path.push("canon.db");
        path
    });

    let mut db = repo::open_with_options(
        &db_path,
        repo::DbOptions {
            debug_sql: cli.debug_sql,
            profile: cli.profile,
        },
    )?;

    match cli.command {
        Commands::Scan { paths, role, add, comment, all, no_hash, verify, candidates, ignore_device_id } => {
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
            scan::run(&db, &paths, role.as_deref(), add, comment.as_deref(), all, no_hash, verify, ignore_device_id)?;
        }
        Commands::Worklist { paths, filters, include_archived, include_excluded, unique_content, emit } => {
            worklist::run(&mut db, &paths, &filters, include_archived, include_excluded, unique_content, &emit)?;
        }
        Commands::ImportFacts { allow_archived, verbose } => {
            import_facts::run(&db, allow_archived, verbose)?;
        }
        Commands::Ls { paths, filters, archived, unarchived, unhashed, duplicates, include_archived, include_excluded, long, sort, reverse, null_delim } => {
            // If no paths given, check if cwd is inside a root
            // Also detect if scope is inside an archive root to auto-include archived sources
            let (scope_paths, use_relative, auto_include_archived) = if paths.is_empty() {
                let cwd = std::env::current_dir()?;
                match domain::resolve_root_path(db.conn(), &cwd)? {
                    Some((_, _, role, _)) => (vec![cwd], true, role == "archive"),
                    None => (vec![], false, false),
                }
            } else {
                let use_rel = !paths.first().map(|p| p.starts_with("/")).unwrap_or(false);
                // Check if any explicit path is inside an archive root
                let any_archive = paths.iter().any(|p| {
                    domain::resolve_root_path(db.conn(), p)
                        .ok()
                        .flatten()
                        .map(|(_, _, role, _)| role == "archive")
                        .unwrap_or(false)
                });
                (paths, use_rel, any_archive)
            };
            let include_archived = include_archived || auto_include_archived;
            if duplicates {
                ls::show_duplicates(&mut db, &scope_paths, &filters, include_archived, include_excluded, use_relative)?;
            } else {
                ls::run(&mut db, &scope_paths, &filters, archived.as_deref(), unarchived, unhashed, include_archived, include_excluded, use_relative, long, &sort, reverse, null_delim)?;
            }
        }
        Commands::Facts { action, key, paths, filters, limit, all, show_aliases, include_archived, include_excluded, by_root, group_by } => {
            if show_aliases {
                facts::show_aliases();
                return Ok(());
            }
            match action {
                Some(FactsAction::Delete { key, paths, filters, on, value_type, yes }) => {
                    let options = facts::DeleteOptions {
                        entity_type: on,
                        value_type,
                        dry_run: !yes,
                    };
                    facts::delete_facts(&mut db, &key, &paths, &filters, &options)?;
                }
                None => {
                    facts::run(&mut db, key.as_deref(), &paths, &filters, limit, all, include_archived, include_excluded, by_root, &group_by)?;
                }
            }
        }
        Commands::Prune { orphaned_objects, stale_facts, excluded_facts, yes } => {
            if !orphaned_objects && !stale_facts && excluded_facts.is_none() {
                anyhow::bail!("At least one of --orphaned-objects, --stale-facts, or --excluded-facts is required");
            }
            if stale_facts {
                facts::prune_stale(&db, !yes)?;
            }
            if orphaned_objects {
                facts::prune_orphaned_objects(&db, !yes)?;
            }
            if let Some(scope) = excluded_facts {
                facts::prune_excluded_facts(&db, &scope, !yes)?;
            }
        }
        Commands::Coverage { paths, filters, archive, include_archived, include_excluded, compact } => {
            coverage::run(&mut db, &paths, &filters, archive.as_deref(), include_archived, include_excluded, compact)?;
        }
        Commands::Compare { path_a, path_b, filters, include_excluded, verbose } => {
            let options = compare::CompareOptions {
                include_excluded,
                verbose,
            };
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
                force,
                include_archived,
                show_archived,
                allow_duplicates,
            } => {
                let options = cluster::GenerateOptions {
                    force,
                    include_archived,
                    show_archived,
                    allow_duplicates,
                };
                cluster::generate(&mut db, &paths, &filters, &dest, &output, &options)?;
            }
            ClusterAction::Refresh {
                manifest,
                include_archived,
                show_archived,
                allow_duplicates,
            } => {
                let options = cluster::GenerateOptions {
                    force: false, // refresh by definition updates existing file
                    include_archived,
                    show_archived,
                    allow_duplicates,
                };
                cluster::refresh(&mut db, &manifest, &options)?;
            }
        },
        Commands::Apply {
            manifest,
            dry_run,
            verbose,
            allow_cross_archive_duplicates,
            allow_duplicates,
            root,
            rename,
            move_files,
            yes,
        } => {
            let transfer_mode = if rename {
                apply::TransferMode::Rename
            } else if move_files {
                apply::TransferMode::Move
            } else {
                apply::TransferMode::Copy
            };
            let options = apply::ApplyOptions {
                dry_run,
                verbose,
                allow_cross_archive_duplicates,
                allow_duplicates,
                roots: root,
                transfer_mode,
                yes,
            };
            apply::run(&db, &manifest, &options)?;
        }
        Commands::Exclude { action } => match action {
            ExcludeAction::Set { paths, filters, id, dry_run } => {
                let options = exclude::SetOptions { dry_run, verbose: false };
                if let Some(source_id) = id {
                    exclude::set_by_id(&db, source_id, &options)?;
                } else if paths.len() == 1 && filters.is_empty() && paths[0].is_file() {
                    // Single file path with no filters: exclude exact file
                    exclude::set_by_path(&db, &paths[0], &options)?;
                } else {
                    exclude::set(&mut db, &paths, &filters, &options)?;
                }
            }
            ExcludeAction::Clear { paths, filters, dry_run } => {
                let options = exclude::ClearOptions { dry_run };
                exclude::clear(&mut db, &paths, &filters, &options)?;
            }
            ExcludeAction::List { paths, filters } => {
                exclude::list(&mut db, &paths, &filters)?;
            }
            ExcludeAction::Duplicates { path, prefer, filters, dry_run } => {
                exclude::exclude_duplicates(&mut db, &prefer, Some(path.as_path()), &filters, dry_run)?;
            }
            ExcludeAction::SetObject { paths, filters, hash, yes, verbose } => {
                let options = exclude::SetOptions { dry_run: !yes, verbose };
                if let Some(h) = hash {
                    exclude::set_object_by_hash(&db, &h, &options)?;
                } else if paths.len() == 1 && filters.is_empty() && paths[0].is_file() {
                    // Single file path: exclude that file's object
                    exclude::set_object_by_file(&db, &paths[0], &options)?;
                } else if !paths.is_empty() || !filters.is_empty() {
                    // Paths and/or filters: exclude matching objects
                    exclude::set_objects_by_filter(&mut db, &paths, &filters, &options)?;
                } else {
                    anyhow::bail!("Provide a hash (--hash), file path, or filters (--where)");
                }
            }
            ExcludeAction::ClearObject { hash, dry_run } => {
                let options = exclude::ClearOptions { dry_run };
                exclude::clear_object(&db, &hash, &options)?;
            }
            ExcludeAction::ListObjects => {
                exclude::list_objects(&db)?;
            }
        },
        Commands::Roots { action, path, suspended } => match action {
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

    // Check if ANALYZE should run (based on modifications or time since last analyze)
    let _ = db.maybe_analyze();

    // Print profile summary if profiling was enabled
    repo::print_profile_summary(db.conn());

    Ok(())
}
