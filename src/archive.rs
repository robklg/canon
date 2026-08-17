//! The archive subsystem: declaring an intended archive operation and
//! performing it — `canon cluster generate/refresh/status` and `canon apply`.
//!
//! Three names meet here. A *cluster* is a selection of sources gathered for
//! archiving; a *manifest* is the human-editable declaration of what that
//! selection should become, paired with a lock file snapshotting the sources
//! at generation time; *apply* is the act that carries the files across and
//! records what it did.

mod cli;
mod domain;
mod ops;
mod repo;

pub use cli::apply::{run, ApplyOptions};
pub use cli::cluster::{generate, refresh, status, GenerateOptions};
pub use ops::execute::TransferMode;

// These two have no production caller — the contentless-law canary
// (contentless_law_tests.rs) is the only consumer, reaching them
// through the barrel like the other subsystems' test-only re-exports.
#[allow(unused_imports)]
pub use ops::generate::{plan_generate, ClusterGenerateParams};
