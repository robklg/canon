//! The roots subsystem: `canon roots` list/rm/comment/suspend/unsuspend — the
//! command surface over the shared root and note nouns. No domain stratum:
//! roots' domain logic lives on `Root` itself, shared by every subsystem.
//!
//! `cli` is the whole command surface; `ops` composes selection, scope
//! resolution, and decision recording; `repo` holds the roots-exclusive SQL.

mod cli;
mod ops;
mod repo;

pub use cli::{list, remove, set_comment, suspend, unsuspend};
pub use ops::remove_root_data;

// Retire's ceremony test is the only external consumer — production reaches
// plan_remove through cli::remove, in the same subsystem.
#[allow(unused_imports)]
pub use ops::plan_remove;
