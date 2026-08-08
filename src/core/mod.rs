//! Core: the cross-cutting spine of the feature-first migration
//! (feature-first structure ADR, 2026-08-07).
//!
//! Holds only what genuinely qualifies: the fundamental nouns (not yet
//! moved here — they relocate in dedicated blast-radius-sized stories, per
//! the migration's membership-is-criterial/movement-is-staged principle), the
//! provenance spine (ditto — only the slice a real move has forced so far
//! lives here), and substrate multiple subsystems independently compute
//! over. See `CLAUDE.md` in this directory for the current inventory and
//! why each item is here.
//!
//! Core must never depend on a subsystem (`retire/`, and more as the streak
//! adds them) — the architecture test enforces this
//! (`Rule::CoreReferencesSubsystem`). A subsystem may depend on core at any
//! depth; on a sibling subsystem only through its declared one-segment
//! public surface (`Rule::SubsystemSiblingInternalReach`).

pub mod domain;
pub mod ops;
