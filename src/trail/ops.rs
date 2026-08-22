//! Trail's operations layer: composed reads over the domain layer and the
//! repo. No stdio, no transactions on this read-only side.

pub(super) mod composition;
pub(super) mod compute;
pub(super) mod place;
pub(super) mod show;

#[cfg(test)]
mod tests;
