//! The trail subsystem: read the decision trail.
//!
//! `domain` holds the timeline merge, the two-claims placement law, the
//! crossings selection rule and the composition card; `ops` composes the
//! scope/time-lens reads, the crossings computation and the composition card
//! fetch; `cli`/`render`/`jsonl` are the interface layer (`canon trail`,
//! `trail show`, `trail crossings`) — `cli` parses args and dispatches,
//! `render` composes the human-readable text, `jsonl` composes the
//! machine-output completeness contract.

mod cli;
mod domain;
mod jsonl;
mod ops;
mod render;
mod repo;

pub use cli::{run, run_crossings, run_show, CrossingsArgs, TrailArgs, TrailExit};

// The pub-field and variant types of TrailParams/TrailResult — a future crate
// boundary can't expose a public field of a private type, so these ride on
// the barrel alongside the items an in-crate consumer names today, even
// though nothing in this crate names them directly yet.
#[allow(unused_imports)]
pub use domain::placement::{RowAspect, ScopeMatch};
#[allow(unused_imports)]
pub use domain::timeline::{DayGroup, DayRollup, FateLine, WhenValue};

pub use domain::timeline::TimelineEvent;
pub use ops::compute::{compute_trail, TrailParams, TrailResult, TrailView};
#[allow(unused_imports)]
pub use ops::compute::{ArrivalRollup, ExtractionRollup, RearrangementRollup};
