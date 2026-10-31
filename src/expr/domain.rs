//! The expression facility's domain layer: the language itself, as pure logic.
//!
//! The reader, which reads a typed expression once by one rule per position,
//! and the problems it and binding find, with the caret layout a refusal
//! draws them in. What a key may name and how a typed key is read; the transforms a value
//! can be put through; the grammar and syntax tree of a `--where` expression,
//! and the comparisons it comes down to; a glob's own grammar and its matcher;
//! the patterns that arrange values into
//! a path, and the place a scope-relative one measures from; the shape a
//! prefetched fact takes on its way to a comparison; resolving a key to a
//! value for one source; a report's rows and the filter each stands for; rewriting the `@name` shorthands a filter may be
//! written with; and the grammar a statement about a value and its type is
//! built by. No I/O anywhere in this layer.

pub(super) mod alias;
pub(super) mod bind;
pub(super) mod cache;
pub(super) mod filter;
pub(super) mod glob;
pub(super) mod key;
pub(super) mod pattern;
pub(super) mod print;
pub(super) mod problem;
pub(super) mod read;
pub(super) mod row;
pub(super) mod statement;
pub(super) mod transform;
pub(super) mod value;
pub(super) mod vantage;
