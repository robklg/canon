//! The shared spine: what the rest of the tree is built on.
//!
//! Two kinds of thing qualify. The fundamental nouns — sources, roots,
//! objects, facts — and the pure utilities around them, which every feature
//! speaks. And substrate that more than one feature independently computes
//! over, where neither of them owns it. A feature's own finished output does
//! not qualify, however many other features consume it.
//!
//! Not everything shared lives here yet: database access and parts of the
//! provenance machinery are still top-level modules of their own.
//!
//! Core must never depend on a feature module, or it stops being trustworthy
//! ground to build on. A feature may depend on core at any depth; on a
//! sibling feature only through that sibling's declared public surface. The
//! architecture test enforces both directions.

pub mod domain;
pub mod ops;
