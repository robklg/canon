//! `canon worklist` — handing a selection to something else.
//!
//! The engine's outward edge for enrichment: it writes one JSON object per
//! source to standard output, so an external tool can compute what Canon does
//! not know how to compute and hand the answers back as facts. Canon provides
//! the infrastructure; the tools bring the knowledge.

mod cli;
mod ops;

pub use cli::run;
