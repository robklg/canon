//! Retirement domain: the readiness verdict and the book's per-entry model.

mod book;
mod readiness;

pub use book::{
    book_dir_name, build_book_entries, derive_posture, disposition_word, ApplyOrigin, BookEntry,
    EntryVerification, FateContext, SourceFate, VerificationPosture, STANDING_CONTENTLESS,
    STANDING_COVERED, STANDING_MISSING_UNEXPLAINED, STANDING_PRESENT,
};
pub use readiness::{derive_readiness, Readiness};
