//! Shared test scaffolding: the insert/setup helpers every stratum's tests
//! build a database with.
//!
//! It sits in core for the same reason the repository layer does — the
//! fixtures speak the fundamental nouns, and every subsystem's tests need
//! them. Compiled only under `cfg(test)`; nothing here is reachable from a
//! release build.

mod helpers;

pub use helpers::{
    insert_fact, insert_note, insert_object, insert_root, insert_source, insert_source_excluded,
    insert_source_with_metadata, insert_source_with_size, is_object_excluded, is_source_excluded,
    setup_test_db,
};
