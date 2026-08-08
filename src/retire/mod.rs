//! The retire subsystem: closing the book on a fully resolved root.
//!
//! `domain` holds the readiness verdict and book-compile fate model;
//! `ops` composes the readiness review, the ceremony, the book compile,
//! the shelf listing, and the bound telling; `cli` is the interface layer
//! (`canon roots retire`/`retired`).

pub mod cli;
pub mod domain;
pub mod ops;
