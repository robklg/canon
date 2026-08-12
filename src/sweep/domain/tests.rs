//! Sweep domain tests, split by pipeline stage; `fixtures` holds the
//! machinery genuinely shared across stages — a helper used by a single
//! test file lives there instead.

mod assembly;
mod discovery;
mod fixtures;
mod lens;
mod localization;
mod universe;
mod weights;
