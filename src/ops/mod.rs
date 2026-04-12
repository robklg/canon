//! Operations layer for canon.
//!
//! Typed, interface-independent functions that express Canon's composed behaviors.
//! Each operation accepts typed parameters, composes domain predicates with repo
//! functions, and returns a typed result. The interface layer (CLI, TUI) builds
//! parameters from user input and formats results for display.

pub mod apply;
pub mod cluster;
pub mod decision;
pub mod coverage;
pub mod exclude;
pub mod facts;
pub mod fs;
pub mod import_facts;
pub mod ls;
pub mod note;
pub mod receipt;
pub mod roots;
pub mod scan;
pub mod scope;
pub mod selection;
pub mod survey;
pub mod worklist;

#[cfg(test)]
pub(crate) mod test_helpers;
