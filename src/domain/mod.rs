//! Domain layer for canon.
//!
//! Pure domain concepts with no I/O or side effects. This layer defines:
//! - Structs: `Source`, `Root`, `Object`, `FactEntry`, `FileObservation`, `Reconciliation`
//! - Predicates: `is_excluded()`, `is_active()`, `matches_scope()`, etc.
//! - Pure utilities: path manipulation, scope matching
//! - Pure functions: `reconcile()`, `find_missing()`

// Re-exports are intentional for public API convenience
#![allow(unused_imports)]

pub mod fact;
pub mod object;
pub mod path;
pub mod root;
pub mod scan;
pub mod scope;
pub mod source;

// Re-export primary types for convenient access
pub use fact::{FactEntry, FactType, FactValue};
pub use object::Object;
pub use path::{canonicalize_maybe_missing, canonicalize_scope, canonicalize_scopes, path_is_under, path_strip_prefix};
pub use root::{
    find_containing_root, parse_root_spec, parse_root_spec_any, resolve_archive_path,
    resolve_root_path, resolve_root_path_any, Root, RootSpec,
};
pub use scan::{find_missing, reconcile, FileObservation, Reconciliation};
pub use scope::{build_scope_clause, ScopeMatch};
pub use source::{NewSource, Source};
