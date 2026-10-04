//! Retirement operations: shared helpers, and the barrel gathering
//! `review`, `ceremony`, `compile`, `shelf`, and `frame` into
//! `retire::ops`.

/// Receipt hashes are `"sha256:<hex>"`; the object index keys on the bare
/// value.
fn strip_hash_prefix(hash: &str) -> &str {
    hash.split_once(':').map(|(_, v)| v).unwrap_or(hash)
}

/// A machine-readable instant for the book's structured fields: UTC, with the
/// `Z` that says so. The book's prose dates read on the user's clock
/// (`core::domain::format::format_date`, which also names the book's
/// directory) — these are the fields a program reads, and they say which zone
/// they mean. A year before 0000 is written in ISO 8601's expanded form; past
/// the instants a time can hold (a save time can be anything), the stored
/// seconds are written as they are.
fn iso_utc(ts: i64) -> String {
    jiff::Timestamp::from_second(ts)
        .map(|at| at.to_string())
        .unwrap_or_else(|_| ts.to_string())
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
    parked: &[crate::core::domain::root::ParkedRoot],
) -> String {
    let paths: Vec<String> = parked.iter().map(|p| p.root_path.clone()).collect();
    format!(
        "Retirement needs an archive root to hold the record — every archive root is suspended ({}). To open one: {}",
        crate::core::domain::root::capped_root_names(&paths).as_list(),
        outcome.unsuspend_hint().unwrap_or_default()
    )
}

/// The entry gate named above: the shelf's own precondition, raised as a
/// refusal rather than merely stated. `shelf_blocker` answers the same
/// question for a *reading*, which must state without stopping — this is the
/// act's own door, and the two causes earn different error classes: no
/// archive root at all is a real misconfiguration and wears `Error:`; every
/// archive root suspended is the user's own closed door and does not, so it
/// carries the same sentence out as a [`crate::core::domain::root::DoorRefused`]
/// instead. `Ok(())` when a shelf stands.
pub fn validate_retire_target(
    roots: &[crate::core::domain::root::Root],
    config: &crate::core::domain::config::LedgerConfig,
) -> anyhow::Result<()> {
    let outcome = crate::core::ops::receipt::resolve_ledger_root(roots, config);
    match &outcome {
        crate::core::ops::receipt::LedgerRootOutcome::Found { .. } => Ok(()),
        crate::core::ops::receipt::LedgerRootOutcome::NoArchiveRoot => {
            anyhow::bail!(
                "Retirement needs an archive root to hold the record — no archive root is registered. To remove the root without binding its story: canon roots rm"
            )
        }
        crate::core::ops::receipt::LedgerRootOutcome::AllArchiveRootsSuspended { roots } => Err(
            crate::core::domain::root::DoorRefused::fleetwide(parked_shelf_refusal(
                &outcome, roots,
            ))
            .into(),
        ),
    }
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
    find_retirement_covering_path, readiness_lens, shelf_blocker, validate_retire_subject,
    ReadinessReview, RetiredScope,
};
pub use shelf::{compute_shelf_listing, ShelfLine};
