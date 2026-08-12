//! The structural computation: five stages producing lens-free
//! reduction-opportunity findings from a policy-filtered source slice.
//! Ranking and presentation belong to the lens derivation, not here.
//!
//! Callers apply inclusion policy before building (presence, exclusion,
//! zero-byte, suspension participation) — the engine indexes what it is
//! given.

mod assembly;
mod discovery;
mod localization;
mod universe;
mod weights;

// Re-exports lens.rs and ops.rs/cli.rs (via domain.rs) actually consume in
// production code.
pub use assembly::{compute_structural, FindingNature, StructuralFinding, StructuralSweep};
pub use discovery::FindingTier;
pub use localization::{Location, RelationClass, RelationShape};
pub use universe::{SweepParams, SweepStats};

// Re-exports riding the barrel for the domain test tree only — it sits as a
// sibling of this module, not nested inside it, so it needs the same
// pipeline-stage surface the original single-file test module reached via
// `use super::*`. Unused by production code, same treatment trail.rs gives
// its own test/future-only re-exports.
#[allow(unused_imports)]
pub use discovery::{discover_subjects, RawSubject};
#[allow(unused_imports)]
pub use localization::{localize_subjects, ContextRelation, LocalizedSubject};
#[allow(unused_imports)]
pub use universe::{build_universe, RootData, Universe};
#[allow(unused_imports)]
pub use weights::{compute_matched, MatchedWeights};
