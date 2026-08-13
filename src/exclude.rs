//! The exclude subsystem: conscious dismissal of sources and objects from
//! consideration.
//!
//! `domain` holds duplicate detection; `repo` holds the exclusion-transition
//! SQL; `ops` composes plan/execute pairs around it, owning the subsystem's
//! one transaction site; `cli` is the whole command surface (`canon exclude
//! set/clear/duplicates/set-object/clear-object`).

mod cli;
mod domain;
mod ops;
mod repo;

pub use cli::{
    clear, clear_object, exclude_duplicates, list_objects, set, set_by_id, set_by_path,
    set_object_by_file, set_object_by_hash, set_objects_by_filter, ClearOptions, SetOptions,
};

// These three have no production caller — the contentless-law canary
// (ops/contentless_law_tests.rs) is the only consumer, reaching them
// through the barrel like trail.rs/survey.rs's own test-only re-exports.
#[allow(unused_imports)]
pub use ops::plan::plan_set_objects;
#[allow(unused_imports)]
pub use ops::single::check_set_object_by_file;
#[allow(unused_imports)]
pub use ops::types::ExcludeSetObjectsParams;
