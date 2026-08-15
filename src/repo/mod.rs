//! Repository layer for canon.
//!
//! Database access with batch fetching patterns. This layer provides:
//! - Batch fetch functions: `batch_fetch_by_roots()`, `batch_fetch_by_ids()`
//! - Database connection management: `Db`, `open_with_options()`
//! - No domain logic — just data access

// Re-exports are intentional for public API convenience
#![allow(unused_imports)]

pub mod db;
pub mod decision;
pub mod fact;
pub mod object;
pub mod root;
pub mod source;

// Re-export database utilities at module level
pub use db::{
    open_with_options, populate_temp_sources, print_profile_summary, Connection, Db, DbOptions,
};

// Test utilities
#[cfg(test)]
pub use db::open_in_memory_for_test;
#[cfg(test)]
pub use root::insert_test_root;
#[cfg(test)]
pub use source::insert_test_source;

// Note: Repository functions are accessed via their submodules, e.g.:
// - repo::source::batch_fetch_by_roots()
// - repo::root::fetch_all()
// - repo::object::batch_check_archived()
// - repo::fact::batch_fetch_for_sources()
