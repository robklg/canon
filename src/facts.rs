//! The facts subsystem: `canon facts`/`import-facts` — fact distribution
//! reporting, maintenance (delete/prune), and JSONL import. A leaf subsystem:
//! no sibling reaches in, and it reaches no sibling.

mod cli;
mod domain;
mod ops;
mod repo;

pub use cli::import::import_run;
pub use cli::{
    delete_facts, prune_excluded_facts, prune_orphaned_objects, prune_stale, run, show_aliases,
    DeleteOptions,
};
