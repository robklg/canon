//! Fixtures shared across the survey ops test modules.

use crate::domain::IncludeSet;
use crate::survey::ops::compute::SurveyParams;

pub(super) fn test_params() -> SurveyParams {
    SurveyParams {
        include: IncludeSet::default(),
        compute_affinity: false,
        compute_overlap_pairs: false,
        compute_residual: false,
        compute_archived_pairs: false,
    }
}
