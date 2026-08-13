//! Survey's domain layer: pure functions for outward-looking comparison
//! (scope discovery, "only here"/uniqueness counting, location
//! classification) and the in-memory object index that grounds content
//! comparisons in objects rather than sources. No I/O anywhere in this layer.

pub(super) mod analysis;
pub(super) mod object_index;
