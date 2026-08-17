//! Exclude operations — plan/execute for exclusion management.
//!
//! Provides plan/execute functions for `exclude set`, `exclude clear`,
//! `exclude duplicates`, and `exclude set --objects`. Plan functions compute
//! what would happen (no side effects), returning typed plan structs with all
//! data needed for display and confirmation. Execute functions perform the writes.
//!
//! `types` holds the shared plan/result/receipt-item types; `plan` and
//! `execute` are the two sides of each command; `receipt` holds the pure
//! receipt-body mappers; `runner` holds `run_exclusion`, the subsystem's one
//! transaction site; `single` holds the single-target check/execute pairs.
//
// pub(super): `exclude::cli` reaches these strata directly; the
// contentless-law canary (`contentless_law_tests.rs`) reaches the three
// it needs through the barrel's re-exports instead.
pub(super) mod execute;
pub(super) mod plan;
pub(super) mod receipt;
pub(super) mod runner;
pub(super) mod single;
pub(super) mod types;

#[cfg(test)]
mod tests;
