//! Core domain: the pure layer — no database, filesystem or network I/O.
//!
//! Two things live here side by side. The fundamental nouns — `Source`,
//! `Root`, `Object`, `FactEntry`, and the pure utilities around them (path
//! manipulation, scope matching, formatting, config parsing) — are the
//! vocabulary every subsystem speaks. Beside them sits substrate more than
//! one subsystem independently computes over: the resolution account, the
//! fate vocabulary, extraction rows, folder-tree topology.
//!
//! Both obey the same purity law: structs, predicates, and pure functions
//! only, so core logic is unit-testable with known inputs and outputs. One
//! sanctioned exception: `time::now_epoch_secs`, the epoch-now law's single
//! owner — a clock read, not the database/filesystem/network I/O the layer
//! table forbids, and the one function in this module whose two calls can
//! disagree; every other consumer of "now" reaches it rather than reading
//! the clock a second way.

pub mod config;
pub mod decision;
pub mod extraction;
pub mod fact;
pub mod fate;
pub mod folder_tree;
pub mod format;
pub mod include;
pub mod number;
pub mod object;
pub mod path;
pub mod resolution;
pub mod root;
pub mod scope;
pub mod source;
pub mod time;

// Re-exported for convenience where a caller wants the name without the
// module. Only what is actually reached this way — the rest is spelled
// through its module.
pub use format::format_count;
pub use include::IncludeSet;
pub use path::path_strip_prefix;
pub use root::Root;
