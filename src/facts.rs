//! The facts subsystem: `canon facts`/`import-facts` — fact distribution
//! reporting, maintenance (delete/prune), and JSONL import. A leaf subsystem:
//! no sibling reaches in, and it reaches no sibling.

mod cli;
mod domain;
mod ops;
mod repo;

pub use cli::import::import_run;
pub use cli::{
    delete_facts, prune_excluded_facts, prune_orphaned_objects, prune_stale, run, DeleteOptions,
};

// No production caller: the source-value law's verifier (source_value_law_tests.rs) reads the
// report through it, as it reads every surface that takes a source's value for a key.
#[allow(unused_imports)]
pub use ops::report::compute_distribution;
