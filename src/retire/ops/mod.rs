//! Retirement operations: shared helpers, and the barrel gathering
//! `review`, `ceremony`, `compile`, `shelf`, and `frame` into
//! `retire::ops`.

/// Receipt hashes are `"sha256:<hex>"`; the object index keys on the bare
/// value.
fn strip_hash_prefix(hash: &str) -> &str {
    hash.split_once(':').map(|(_, v)| v).unwrap_or(hash)
}

/// A machine-readable instant for the book's structured fields: UTC, with the
/// `Z` that says so. The book's prose dates are local (see [`iso_date`]) —
/// these are the fields a program reads, and they say which zone they mean.
fn iso_utc(ts: i64) -> String {
    chrono::DateTime::from_timestamp(ts, 0)
        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%SZ").to_string())
        .unwrap_or_else(|| ts.to_string())
}

/// A calendar date for the book's prose and the book's own directory name:
/// the reader's local day, like every other date Canon shows a person. Rendered
/// in UTC, an evening retirement dated the book a day ahead of the story told
/// inside it.
fn iso_date(ts: i64) -> String {
    use chrono::TimeZone;
    match chrono::Local.timestamp_opt(ts, 0) {
        chrono::LocalResult::Single(dt) => dt.format("%Y-%m-%d").to_string(),
        _ => ts.to_string(),
    }
}

/// The shelf's directory name at the archive ledger root: a visible place,
/// deliberately not under `.canon-ledger/` — the books are for human eyes.
pub const SHELF_DIR: &str = "retired";

/// The refusal both retirement doors give when every archive root is parked:
/// the entry gate (`validate_retire_target`) and the bind's backstop
/// (`plan_bind`) meet one fact and must say one thing about it. Spelled here
/// once, because the requester reads this sentence and a second copy is a
/// second sentence the day either is repaired.
///
/// Nothing is missing — the shelf and its books stand where they stood — so
/// the way back is `canon roots unsuspend` and only that; the destructive door
/// belongs to the other absence, where there is genuinely no shelf to bind to.
fn parked_shelf_refusal(
    outcome: &crate::core::ops::receipt::LedgerRootOutcome,
    parked: &[String],
) -> String {
    format!(
        "Retirement needs an archive root to hold the record — every archive root is suspended ({}). To open one: {}",
        parked.join(", "),
        outcome.unsuspend_hint().unwrap_or_default()
    )
}

mod ceremony;
mod compile;
pub(super) mod frame;
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
