//! Operations layer for canon.
//!
//! Typed, interface-independent functions that express Canon's composed behaviors.
//! Each operation accepts typed parameters, composes domain predicates with repo
//! functions, and returns a typed result. The interface layer (CLI, TUI) builds
//! parameters from user input and formats results for display.

pub mod compare;
pub mod coverage;
pub mod ls;
pub mod selection;
pub mod worklist;

#[cfg(test)]
mod contentless_law_tests;
#[cfg(test)]
pub(crate) mod test_helpers;
