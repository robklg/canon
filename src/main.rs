use clap::{Parser, Subcommand};
use std::path::PathBuf;

mod apply;
mod cluster;
mod db;
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
    },
    /// Output all sources as JSONL worklist
    Worklist,
    /// Import facts from JSONL on stdin
    ImportFacts,
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
        Commands::Scan { paths } => {
            scan::run(&db_path, &paths)?;
        }
        Commands::Worklist => {
            worklist::run(&db_path)?;
        }
        Commands::ImportFacts => {
            import_facts::run(&db_path)?;
        }
        Commands::Cluster { action } => match action {
            ClusterAction::Generate { filters, output } => {
                cluster::generate(&db_path, &filters, &output)?;
            }
        },
        Commands::Apply { manifest, dry_run } => {
            apply::run(&manifest, dry_run)?;
        }
    }

    Ok(())
}
