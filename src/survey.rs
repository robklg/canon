// `domain`, `ops`, `cli`, and `render` stay private; anything an outside
// caller needs is re-exported below, so this file is the one public surface
// for the logic nested under it.
mod cli;
mod domain;
mod ops;
mod render;

// Riding the barrel for the contentless-law canary only (unused in
// production code), same treatment trail.rs gives its test-only re-exports.
#[allow(unused_imports)]
pub use domain::object_index::ObjectIndex;
#[allow(unused_imports)]
pub use ops::compute::compute_survey;
#[allow(unused_imports)]
pub use ops::compute::{SurveyOutcome, SurveyParams};

pub use cli::{machine_shaped_stdout, run, DetailMode, SurveyExit, SurveyOptions};
