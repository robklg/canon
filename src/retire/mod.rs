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
// Interim re-export: `story_lines` renders the story reading for
// `canon roots story` — a rendering that belongs to the story subsystem,
// not to retire. It rides this barrel only until the story subsystem
// exists as a module of its own; when it does, the place-walk renderer
// moves there, story renders itself, and this item disappears. Do not
// read it as a deliberate piece of retire's surface.
pub use ops::telling::story_lines;
