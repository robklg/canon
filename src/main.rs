use clap::{Parser, Subcommand};
use std::path::PathBuf;

mod apply;
mod cluster;
mod coverage;
mod db;
mod exclude;
mod facts;
mod filter;
mod import_facts;
mod scan;
mod worklist;

#[derive(Parser)]
#[command(name = "canon")]
#[command(about = "Organize large media libraries into a canonical archive")]
struct Cli {
    /// Path to the database file
    #[arg(long, global = true)]
    db: Option<PathBuf>,

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
    },
    /// Output sources as JSONL worklist
    Worklist {
        /// Directory path to scope the query (resolved to realpath)
        path: Option<PathBuf>,
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
    /// Show fact coverage and value distribution
    #[command(args_conflicts_with_subcommands = true)]
    Facts {
        #[command(subcommand)]
        action: Option<FactsAction>,

        /// Specific fact key to show value distribution
        key: Option<String>,
        /// Directory path to scope the query (resolved to realpath)
        path: Option<PathBuf>,
        /// Filter expressions (e.g., "source.ext=jpg" or "content.hash.sha256?")
        #[arg(long = "where")]
        filters: Vec<String>,
        /// Maximum number of values to show (0 for unlimited, default 50)
        #[arg(long, default_value = "50")]
        limit: usize,
        /// Show all built-in facts (including hidden ones like source.device, source.inode)
        #[arg(long)]
        all: bool,
        /// Include sources from archive roots (by default only source roots)
        #[arg(long)]
        include_archived: bool,
        /// Include excluded sources (by default they are skipped)
        #[arg(long)]
        include_excluded: bool,
    },
    /// Show archive coverage statistics
    Coverage {
        /// Directory path to scope the query (resolved to realpath)
        path: Option<PathBuf>,
        /// Filter expressions (e.g., "source.ext=jpg" or "content.hash.sha256?")
        #[arg(long = "where")]
        filters: Vec<String>,
        /// Filter coverage relative to a specific archive (can be archive root or sub-path)
        #[arg(long)]
        archive: Option<PathBuf>,
        /// Include sources from archive roots (by default only source roots)
        #[arg(long)]
        include_archived: bool,
        /// Include excluded sources (by default they are skipped)
        #[arg(long)]
        include_excluded: bool,
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
        /// Skip all archive conflict checks
        #[arg(long)]
        force: bool,
        /// Allow copying files that exist in other archives (but not destination archive)
        #[arg(long)]
        allow_cross_archive_duplicates: bool,
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
        /// Directory path to scope the operation (resolved to realpath)
        path: Option<PathBuf>,
        /// Filter expressions (e.g., "source.size<1000" or "source.ext=tmp")
        #[arg(long = "where")]
        filters: Vec<String>,
        /// Show what would be excluded without making changes
        #[arg(long)]
        dry_run: bool,
    },
    /// Remove exclusions from sources
    Clear {
        /// Directory path to scope the operation (resolved to realpath)
        path: Option<PathBuf>,
        /// Filter expressions to match excluded sources
        #[arg(long = "where")]
        filters: Vec<String>,
        /// Show what would be cleared without making changes
        #[arg(long)]
        dry_run: bool,
    },
    /// List excluded sources
    List {
        /// Directory path to scope the query (resolved to realpath)
        path: Option<PathBuf>,
        /// Filter expressions to match excluded sources
        #[arg(long = "where")]
        filters: Vec<String>,
    },
}

#[derive(Subcommand)]
enum FactsAction {
    /// Delete facts by key
    Delete {
        /// Fact key to delete (e.g., "content.mime")
        key: String,
        /// Directory path to scope the operation (resolved to realpath)
        path: Option<PathBuf>,
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
        /// Filter expressions (e.g., "content_hash.sha256?" or "exif.model=iPhone")
        #[arg(long = "where", required = true)]
        filters: Vec<String>,
        /// Output manifest file
        #[arg(short, long, default_value = "manifest.toml")]
        output: PathBuf,
        /// Include files already in an archive (by default they are excluded)
        #[arg(long)]
        include_archived: bool,
        /// Show which files were excluded because they're already archived
        #[arg(long)]
        show_archived: bool,
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

    match cli.command {
        Commands::Scan { paths, role } => {
            scan::run(&db_path, &paths, &role)?;
        }
        Commands::Worklist { path, filters, include_archived, include_excluded } => {
            worklist::run(&db_path, path.as_deref(), &filters, include_archived, include_excluded)?;
        }
        Commands::ImportFacts { allow_archived } => {
            import_facts::run(&db_path, allow_archived)?;
        }
        Commands::Facts { action, key, path, filters, limit, all, include_archived, include_excluded } => {
            match action {
                Some(FactsAction::Delete { key, path, filters, on, yes }) => {
                    let options = facts::DeleteOptions {
                        entity_type: on,
                        dry_run: !yes,
                    };
                    facts::delete_facts(&db_path, &key, path.as_deref(), &filters, &options)?;
                }
                Some(FactsAction::Prune { stale, yes }) => {
                    if stale {
                        facts::prune_stale(&db_path, !yes)?;
                    } else {
                        eprintln!("Error: --stale flag is required for prune command");
                        std::process::exit(1);
                    }
                }
                None => {
                    facts::run(&db_path, key.as_deref(), path.as_deref(), &filters, limit, all, include_archived, include_excluded)?;
                }
            }
        }
        Commands::Coverage { path, filters, archive, include_archived, include_excluded } => {
            coverage::run(&db_path, path.as_deref(), &filters, archive.as_deref(), include_archived, include_excluded)?;
        }
        Commands::Cluster { action } => match action {
            ClusterAction::Generate {
                filters,
                output,
                include_archived,
                show_archived,
            } => {
                let options = cluster::GenerateOptions {
                    include_archived,
                    show_archived,
                };
                cluster::generate(&db_path, &filters, &output, &options)?;
            }
        },
        Commands::Apply {
            manifest,
            dry_run,
            force,
            allow_cross_archive_duplicates,
        } => {
            let options = apply::ApplyOptions {
                dry_run,
                force,
                allow_cross_archive_duplicates,
            };
            apply::run(&db_path, &manifest, &options)?;
        }
        Commands::Exclude { action } => match action {
            ExcludeAction::Set { path, filters, dry_run } => {
                let options = exclude::SetOptions { dry_run };
                exclude::set(&db_path, path.as_deref(), &filters, &options)?;
            }
            ExcludeAction::Clear { path, filters, dry_run } => {
                let options = exclude::ClearOptions { dry_run };
                exclude::clear(&db_path, path.as_deref(), &filters, &options)?;
            }
            ExcludeAction::List { path, filters } => {
                exclude::list(&db_path, path.as_deref(), &filters)?;
            }
        },
    }

    Ok(())
}
