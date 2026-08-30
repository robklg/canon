//! The expression facility's operations layer: the language applied.
//!
//! Where the pure grammar meets a database, a scope, and the aliases file.
//! Filtering is the bulk of it: the parsed language run against real rows, and
//! the one thing here that still speaks SQL itself. Selection — the set of
//! sources an operation reports over or acts on — is the language applied
//! within a scope, and it lives here rather than beside any one command because
//! exploring and acting must resolve to the same set. Alias expansion is the
//! half of that story that needs the outside world: the rewriting rules are a
//! domain concern, reading the file they rewrite from is this one.

pub(super) mod alias;
pub(super) mod filter;
pub(super) mod pattern;
pub(super) mod selection;
