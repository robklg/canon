//! `canon coverage` — how much of a place is accounted for by the archive.
//!
//! Coverage is evidence, not a verdict: it reports what Canon can verify by
//! content identity, and says nothing about whether a place is finished. The
//! denominator is the content that can be covered at all, which is why empty
//! files are set aside rather than counted against a place — identity claims
//! about empty content are vacuous.

mod cli;
mod ops;

pub use cli::run;
// Nothing outside this subsystem calls it in production — `cli.rs` does, but
// the contentless-law canary is the only consumer of this re-export, reaching
// it through the barrel like the other subsystems' test-only riders. It
// reaches the computation rather than the command because the law it guards is
// about what the numbers count, not how they are printed.
#[allow(unused_imports)]
pub use ops::compute_per_root;
