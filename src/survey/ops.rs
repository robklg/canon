//! Survey's operations layer: `compute` is the pure computation
//! (`compute_survey` and its params/result types); `orchestrate` owns the
//! fetch — scope roots, note context, `--other`/`--archive` resolution,
//! sources — around it, leaving the interface layer nothing but CLI-shape
//! validation and formatting.

pub(super) mod compute;
pub(super) mod orchestrate;

#[cfg(test)]
mod tests;
