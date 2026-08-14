//! Scan's operations stratum: shared types, the walk pipeline that persists
//! what it finds, deletion-receipt writing, root-candidate discovery, and
//! the hash pass.

pub(super) mod candidates;
pub(super) mod hash;
pub(super) mod pipeline;
pub(super) mod receipt;
pub(super) mod types;
