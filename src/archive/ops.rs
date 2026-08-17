//! Archive's operations: what a cluster would produce, what an apply would do,
//! and the acts themselves.
//!
//! The manifest and lock file accessors sit in their own module because both
//! generating a manifest and reading its status need them.

pub(super) mod execute;
pub(super) mod generate;
pub(super) mod manifest;
pub(super) mod pattern;
pub(super) mod plan;
pub(super) mod receipt;
pub(super) mod status;
