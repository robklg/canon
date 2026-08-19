//! `canon compare` — what two folders hold that the other does not.
//!
//! A content comparison between two places, answered by identity rather than
//! by name: same bytes counts as the same content wherever it sits and
//! whatever it is called. Empty files take no part — they would match each
//! other everywhere and mean nothing by it — so they are set aside and
//! counted, never silently dropped.

mod cli;
mod ops;

pub use cli::{run, CompareOptions};
// Nothing outside this subsystem calls it in production — `cli.rs` does, but
// the contentless-law canary is the only consumer of this re-export, reaching
// it through the barrel like the other subsystems' test-only riders. It
// reaches the comparison itself because this command builds its own content
// maps rather than sharing the engine's, and so has to do its own accounting
// for empty files.
#[allow(unused_imports)]
pub use ops::run_compare;
