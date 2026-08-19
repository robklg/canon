//! `canon ls` — listing the sources a selection resolves to.
//!
//! The plainest of the query instruments: it shows what is there, one line per
//! source, in whatever order was asked for. Its one other mode groups the
//! selection by content identity instead, which is a different question about
//! the same set — not what is here, but what is here more than once.

mod cli;
mod ops;
mod repo;

pub use cli::{run, show_duplicates};
