//! Sweep's domain layer: the structural computation (five pipeline stages)
//! and the reduction lens derived from it. No I/O anywhere in this layer.

pub(super) mod lens;
pub(super) mod structural;

#[cfg(test)]
mod tests;

// Re-exported at sweep-level visibility (not crate-level — this subsystem's
// only crate-external surface is `sweep.rs`'s own sealed `pub use cli::run`)
// so `ops.rs`/`cli.rs`, siblings of this module under `sweep`, can reach the
// structural/lens items they compose without piercing the outer barrel.
pub use lens::{reduction_lens, HubEntry, LeaderboardEntry, SuspendedRootTally};
pub use structural::{
    compute_structural, Location, RelationClass, RelationShape, StructuralFinding, SweepParams,
    SweepStats,
};

// Riding the barrel for cli.rs's own test module only (unused in production
// code), same treatment trail.rs gives its test/future-only re-exports.
#[allow(unused_imports)]
pub use structural::{FindingNature, FindingTier};
