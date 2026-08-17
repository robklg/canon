//! The shared spine: what the rest of the tree is built on.
//!
//! Two kinds of thing qualify. The fundamental nouns — sources, roots,
//! objects, facts — and the pure utilities around them, which every feature
//! speaks. And substrate that more than one feature independently computes
//! over, where neither of them owns it. A feature's own finished output does
//! not qualify, however many other features consume it.
//!
//! Features tell; core warrants. Nothing here narrates: the outputs are
//! facts and typed results, and the voice belongs to the feature that
//! speaks them. The inverse binds just as hard — a feature that re-derives
//! a claim core already makes has made a second claim that can disagree.
//!
//! One piece of shared ground sits outside: the selector every query
//! command runs through is a top-level module, where the commands it was
//! written for can reach it directly.
//!
//! Core must never depend on a feature module, or it stops being trustworthy
//! ground to build on. A feature may depend on core at any depth; on a
//! sibling feature only through that sibling's declared public surface. The
//! architecture test enforces both directions.

pub mod domain;
pub mod ops;
pub mod repo;
#[cfg(test)]
pub mod testing;
