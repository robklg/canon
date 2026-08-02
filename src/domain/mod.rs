//! Domain layer for canon.
//!
//! Pure domain concepts with no I/O or side effects. This layer defines:
//! - Structs: `Source`, `Root`, `Object`, `FactEntry`, `FileObservation`, `Reconciliation`
//! - Predicates: `is_excluded()`, `is_active()`, `matches_scope()`, etc.
//! - Pure utilities: path manipulation, scope matching
//! - Pure functions: `reconcile()`, `find_missing()`, `find_excludable_duplicates()`

// Re-exports are intentional for public API convenience
#![allow(unused_imports)]

pub mod composition;
pub mod config;
pub mod decision;
pub mod exclusion;
pub mod extraction;
pub mod fact;
pub mod format;
pub mod include;
pub mod note;
pub mod object;
pub mod object_index;
pub mod path;
pub mod retire;
pub mod root;
pub mod scan;
pub mod scope;
pub mod source;
pub mod survey;
pub mod sweep;
pub mod trail;

// Re-export primary types for convenient access
pub use exclusion::{find_excludable_duplicates, ExcludableDuplicatesResult};
pub use fact::{FactEntry, FactType, FactValue};
pub use format::format_count;
pub use include::IncludeSet;
pub use note::Note;
pub use object::Object;
pub use path::{
    canonicalize_maybe_missing, clean_path, path_is_under, path_strip_prefix, resolve_path,
    resolve_paths,
};
pub use root::{
    find_containing_root, parse_root_spec, parse_root_spec_any, resolve_archive_path,
    resolve_root_path, resolve_root_path_any, Root, RootSpec,
};
pub use scan::{find_missing, reconcile, FileObservation, Reconciliation};
pub use scope::ScopeMatch;
pub use source::{NewSource, Source};
pub use survey::{
    classify_location, count_only_here, discover_scopes, discover_scopes_by_root, DiscoveredScope,
    LocationKind,
};
