//! `canon worklist` — handing a selection to something else.
//!
//! The engine's outward edge for enrichment: it writes one JSON object per
//! source to standard output, so an external tool can compute what Canon does
//! not know how to compute and hand the answers back as facts. Canon provides
//! the infrastructure; the tools bring the knowledge.

mod cli;
mod ops;

pub use cli::run;

// No production caller: the source-value law's verifier (source_value_law_tests.rs) reads the
// entries through it, as it reads every surface that takes a source's value for a key.
#[allow(unused_imports)]
pub use ops::build_entries;
