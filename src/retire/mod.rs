//! The retire subsystem: closing the book on a fully resolved root.
//!
//! `domain` holds the readiness verdict and book-compile fate model;
//! `ops` composes the readiness review, the ceremony, the book compile,
//! the shelf listing, and the bound telling; `cli` is the interface layer
//! (`canon roots retire`/`retired`). The three strata are sealed behind
//! this barrel — the only names reachable from outside the subsystem are
//! the ones re-exported below.

mod cli;
mod domain;
mod ops;

pub use cli::{retire, retired};
pub use ops::{find_retirement_covering_path, RetiredScope};
