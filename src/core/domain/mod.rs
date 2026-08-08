//! Core domain: pure substrate shared by more than one subsystem, no I/O.
//! Same purity law as the pre-migration `domain/` layer — this is a
//! narrower, subsystem-crossing slice of it, not a different rule.

pub mod resolution;
