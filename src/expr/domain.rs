//! The expression facility's domain layer: the language itself, as pure logic.
//!
//! What a key may name and how a written key is read; the transforms a value
//! can be put through; the grammar and syntax tree of a `--where` expression,
//! and the comparisons it comes down to; the patterns that arrange values into
//! a path; the shape a prefetched fact takes on its way to a comparison;
//! resolving a key to a value for one source; and rewriting the `@name`
//! shorthands a filter may be written with. No I/O anywhere in this layer.

pub(super) mod alias;
pub(super) mod cache;
pub(super) mod filter;
pub(super) mod key;
pub(super) mod pattern;
pub(super) mod transform;
pub(super) mod value;
