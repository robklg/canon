# Refactoring Spec: Operations Layer — Migrate Remaining Query Commands

## Overview

Migrate `ls`, `facts`, and `compare` to use `ops::selection::select_sources()`, completing ADR Step 5. Follows the same mechanical pattern validated by the coverage + worklist migration.

**ADR**: `~/store/canon-architecture/2026-03-13-operations-layer.md`
**ADR Step Covered**: Step 5 (extract remaining query commands).
**Predecessor**: `.claude/specs/2026-03-13-operations-layer-selection.md` (completed)

## Phase 1: Migrate ls.rs, facts.rs, compare.rs

- **Status**: completed
- **Goal**: Replace all private selection functions with `select_sources()` calls.
- **Scope**: Three files, six call sites, three function deletions.
- **Non-goals**: No output format changes, no new operations, no effectful command migration.
- **Dependencies**: `ops::selection::select_sources()` (completed).

### Changes

#### ls.rs
- Replace 2 call sites (lines 64, 365) with `SelectionParams` + `select_sources()`
- Delete `get_matching_sources()` (lines 239-289)
- Role policy: `SourceUnlessIncluded`
- Remove unused imports (`filter::{self}`, `Connection`); add `ops::selection` imports

#### facts.rs
- Replace 2 call sites:
  - Line 116 (main `run()`): uses all counts — map to `sel.excluded_count`, `sel.included_excluded_count`, `sel.included_archived_count`, and `sel.source_ids()`
  - Line 965 (`delete_facts()`): discards counts, just needs IDs — use `sel.source_ids()`
- Delete `get_matching_sources()` (lines 203-256)
- Role policy: `SourceUnlessIncluded`
- Remove unused imports (`filter::{self}`, `Connection`); add `ops::selection` imports

#### compare.rs
- Replace 2 call sites (lines 40, 42) with `select_sources()` + post-processing
- Delete `get_sources_in_scope()` (lines 112-170)
- Role policy: `AnyRole`
- The object_id → path map building and unhashed counting move to the call site as post-selection logic
- Scope classification moves to call site (currently done inside the deleted function)
- Remove unused imports (`filter::{self}`, `Source`, `Connection`); add `ops::selection` imports

### Test Impact
- No tests call the deleted functions directly — zero test rewrites needed
- All 662 existing tests must pass unchanged

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| Single phase for all three | Mechanical, identical pattern — no reason to split |
| compare post-processing stays in command layer | Building object_id→path maps is interface-specific presentation logic |
