//! Retirement operations: shared helpers, and the barrel gathering
//! `review`, `ceremony`, `compile`, `shelf`, and `frame` into
//! `retire::ops`.

/// Receipt hashes are `"sha256:<hex>"`; the object index keys on the bare
/// value.
fn strip_hash_prefix(hash: &str) -> &str {
    hash.split_once(':').map(|(_, v)| v).unwrap_or(hash)
}

fn iso_utc(ts: i64) -> String {
    chrono::DateTime::from_timestamp(ts, 0)
        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%SZ").to_string())
        .unwrap_or_else(|| ts.to_string())
}

fn iso_date(ts: i64) -> String {
    chrono::DateTime::from_timestamp(ts, 0)
        .map(|dt| dt.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| ts.to_string())
}

/// The shelf's directory name at the archive ledger root: a visible place,
/// deliberately not under `.canon-ledger/` — the books are for human eyes.
pub const SHELF_DIR: &str = "retired";

mod ceremony;
mod compile;
pub mod frame;
mod review;
mod shelf;
#[cfg(test)]
mod tests;
mod verify;

// Explicit re-exports: the split is internal — within the subsystem,
// `retire::ops::*` keeps exactly the surface the single file had. `frame`
// stays a named submodule (its own diction concern, distinct from the
// review/ceremony/compile/verify/shelf split) — internal callers reach it
// as `ops::frame::*`; it has no external consumer of its own (the walk it
// used to carry now lives in the story subsystem and renders itself).
pub use ceremony::{begin_ceremony, plan_bind, CeremonyParams, ReleaseOutcome, RetireCeremony};
pub use review::{
    find_retirement_covering_path, readiness_lens, validate_retire_target, ReadinessReview,
    RetiredScope,
};
pub use shelf::{compute_shelf_listing, ShelfLine};
