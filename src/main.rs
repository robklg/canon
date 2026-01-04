use clap::{Parser, Subcommand};
use std::path::PathBuf;

mod apply;
mod cluster;
mod compare;
mod coverage;
mod db;
mod exclude;
mod expr;
mod facts;
mod filter;
mod import_facts;
mod ls;
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

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Scan directories and add files to the index
    Scan {
        /// Paths to scan
        #[arg(required = true)]
        paths: Vec<PathBuf>,
        /// Role for new roots: 'source' (default) or 'archive'
        #[arg(long, default_value = "source")]
        role: String,
        /// Add path as a new root (required when path is not inside an existing root)
        #[arg(long)]
        add: bool,
    },
    /// Output sources as JSONL worklist
    Worklist {
        /// Directory paths to scope the query (resolved to realpath)
        paths: Vec<PathBuf>,
        /// Filter expressions (e.g., "!content_hash.sha256?" or "ext=jpg")
        #[arg(long = "where")]
        filters: Vec<String>,
        /// Include sources from archive roots (by default only source roots)
        #[arg(long)]
        include_archived: bool,
        /// Include excluded sources (by default they are skipped)
        #[arg(long)]
        include_excluded: bool,
    },
    /// Import facts from JSONL on stdin
    ImportFacts {
        /// Allow importing facts for sources in archive roots
        #[arg(long)]
        allow_archived: bool,
    },
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
        /// Only show summary, not file lists
        #[arg(short, long)]
        quiet: bool,
    },
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
        /// Move files: rename, or copy+delete if cross-device (requires --yes)
        #[arg(long = "move", conflicts_with = "rename", requires = "yes")]
        move_files: bool,
        /// Confirm destructive operations (required for --move)
        #[arg(long)]
        yes: bool,
    },
    /// Manage source exclusions
    Exclude {
        #[command(subcommand)]
        action: ExcludeAction,
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
        /// Execute deletion (default is dry-run)
        #[arg(long)]
        yes: bool,
    },
    /// Prune stale or orphaned facts
    Prune {
        /// Delete facts with mismatched observed_basis_rev
        #[arg(long)]
        stale: bool,
        /// Execute deletion (default is dry-run)
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

    let mut db = db::open(&db_path, cli.debug_sql)?;

    match cli.command {
        Commands::Scan { paths, role, add } => {
            scan::run(&db, &paths, &role, add)?;
        }
        Commands::Worklist { paths, filters, include_archived, include_excluded } => {
            worklist::run(&db, &paths, &filters, include_archived, include_excluded)?;
        }
        Commands::ImportFacts { allow_archived } => {
            import_facts::run(&db, allow_archived)?;
        }
        Commands::Ls { paths, filters, archived, unarchived, unhashed, duplicates, include_archived, include_excluded } => {
            // If no paths given, check if cwd is inside a root
            let (scope_paths, use_relative) = if paths.is_empty() {
                let cwd = std::env::current_dir()?;
                match db::resolve_root_path(db.conn(), &cwd)? {
                    Some(_) => (vec![cwd], true),   // Inside root: scope to cwd, relative
                    None => (vec![], false),        // Outside root: all sources, absolute
                }
            } else {
                let use_rel = !paths.first().map(|p| p.starts_with("/")).unwrap_or(false);
                (paths, use_rel)
            };
            if duplicates {
                ls::show_duplicates(&db, &scope_paths, &filters, include_archived, include_excluded, use_relative)?;
            } else {
                ls::run(&db, &scope_paths, &filters, archived.as_deref(), unarchived, unhashed, include_archived, include_excluded, use_relative)?;
            }
        }
        Commands::Facts { action, key, paths, filters, limit, all, show_aliases, include_archived, include_excluded } => {
            if show_aliases {
                facts::show_aliases();
                return Ok(());
            }
            match action {
                Some(FactsAction::Delete { key, paths, filters, on, yes }) => {
                    let options = facts::DeleteOptions {
                        entity_type: on,
                        dry_run: !yes,
                    };
                    facts::delete_facts(&mut db, &key, &paths, &filters, &options)?;
                }
                Some(FactsAction::Prune { stale, yes }) => {
                    if stale {
                        facts::prune_stale(&db, !yes)?;
                    } else {
                        eprintln!("Error: --stale flag is required for prune command");
                        std::process::exit(1);
                    }
                }
                None => {
                    facts::run(&mut db, key.as_deref(), &paths, &filters, limit, all, include_archived, include_excluded)?;
                }
            }
        }
        Commands::Coverage { paths, filters, archive, include_archived, include_excluded } => {
            coverage::run(&mut db, &paths, &filters, archive.as_deref(), include_archived, include_excluded)?;
        }
        Commands::Compare { path_a, path_b, filters, include_excluded, quiet } => {
            let options = compare::CompareOptions {
                include_excluded,
                quiet,
            };
            let identical = compare::run(&db, &path_a, &path_b, &filters, &options)?;
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
                include_archived,
                show_archived,
                allow_duplicates,
            } => {
                let options = cluster::GenerateOptions {
                    include_archived,
                    show_archived,
                    allow_duplicates,
                };
                cluster::generate(&db, &paths, &filters, &dest, &output, &options)?;
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
            yes: _,
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
            };
            apply::run(&db, &manifest, &options)?;
        }
        Commands::Exclude { action } => match action {
            ExcludeAction::Set { paths, filters, id, dry_run } => {
                let options = exclude::SetOptions { dry_run };
                if let Some(source_id) = id {
                    exclude::set_by_id(&db, source_id, &options)?;
                } else {
                    exclude::set(&db, &paths, &filters, &options)?;
                }
            }
            ExcludeAction::Clear { paths, filters, dry_run } => {
                let options = exclude::ClearOptions { dry_run };
                exclude::clear(&db, &paths, &filters, &options)?;
            }
            ExcludeAction::List { paths, filters } => {
                exclude::list(&db, &paths, &filters)?;
            }
            ExcludeAction::Duplicates { path, prefer, filters, dry_run } => {
                exclude::exclude_duplicates(&db, &prefer, Some(path.as_path()), &filters, dry_run)?;
            }
        },
    }

    Ok(())
}
