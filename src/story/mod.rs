//! The story subsystem: the place walk, its compute op, and the `canon
//! roots story` command. The three strata are sealed behind this barrel —
//! the only names reachable from outside the subsystem are the ones
//! re-exported below, each backing a real consumer: `retire` reaches
//! story's domain and render types this way, `main.rs` reaches the command
//! this way — never a blanket glob.

// ReasonSummary has no consumer yet — it rides in the barrel because it's
// the return type of the public ActGroup::reason_summary(), and a future
// crate can't return a private type from a public method.
#![allow(unused_imports)]

mod cli;
mod domain;
mod ops;

pub use cli::story;
pub use domain::acts::{ActDecision, ActGroup, ReasonSummary};
pub use domain::locations::{aggregate_locations, LocationAggregate, LocationCount};
pub use domain::place::{PlaceStanding, StoryParams, StoryPlace};
pub use ops::render::{file_noun, fmt_locations, reference_place_lines};
pub use ops::report::{report_over, StoryReport};
