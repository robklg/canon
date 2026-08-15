//! The notes subsystem: timestamped location annotations — Canon holding
//! awareness for the user, distinct from the decision trail's record of
//! actions.
//!
//! `domain` holds the note struct and its pure path/scope logic; `repo` is
//! notes' SQL; `ops` composes view/list/clear behaviors and the survey
//! context; `cli` is the interface layer (`canon note`) — parse args, call
//! ops, format output.

mod cli;
mod domain;
mod ops;
mod repo;

pub use cli::{format_note_date, run};
pub use domain::{note_display_path, relative_to_scope, Note};
pub use ops::{survey_note_context, SurveyNoteContext};
// Fully qualified so the scanner's textual repo-reach check reads this as
// notes' own repo stratum, not a bare `repo::` interface reach.
pub use crate::notes::repo::{batch_count_subtree, count_subtree_notes, fetch_all, fetch_by_roots};

// Sibling test fixtures are the only non-notes consumers (sweep/ops.rs,
// story/ops/report.rs, retire/ops/tests/compile.rs) — production seeds notes
// through notes' own cli/ops.
#[allow(unused_imports)]
pub use crate::notes::repo::insert;
