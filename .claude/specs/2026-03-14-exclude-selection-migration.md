# Refactoring Spec: Migrate exclude.rs Selection to select_sources()

## Overview

Migrate `exclude.rs`'s private `get_matching_sources()` to use `ops::selection::select_sources()` with `RolePolicy::SourceOnly`. Eliminates the last `get_matching_sources()` in the codebase.

**ADR**: `~/store/canon-architecture/2026-03-13-operations-layer.md`
**ADR Step Covered**: Partial Step 6 (selection migration only, not plan/execute).

## Phase 1: Replace get_matching_sources() in exclude.rs

- **Status**: completed
- **Goal**: Last selection function migrated to the shared operation.
- **Scope**: Three production call sites, six test rewrites, one function deletion.
- **Non-goals**: No changes to `get_excluded_sources()`, no plan/execute extraction, no ceremony changes.

### Call Site Mapping

| Call Site | `include_excluded` | Maps To |
|-----------|-------------------|---------|
| `set()` line 52 | `false` | `IncludeSet::default()` |
| `exclude_duplicates()` line 441 | `false` | `IncludeSet::default()` |
| `set_objects_by_filter()` line 666 | `true` | `IncludeSet { excluded: true, archived: false }` |

All three use `RolePolicy::SourceOnly` (exclude always operates on source roots only).

The original function takes `scope_prefixes: &[String]` (already resolved). Call sites resolve paths before calling. Migration requires building `ScopeMatch` from the prefixes at the call site, matching how coverage/worklist do it.

### Changes

- Replace 3 call sites with `SelectionParams` + `select_sources()` + `sel.source_ids()`
- Delete `get_matching_sources()` (lines 238-275)
- Update imports: remove `filter::{self}`, add `ops::selection` imports
- Rewrite 6 tests to use `select_sources()` directly

### Test Impact

Six tests call `get_matching_sources()` directly. All test behaviors already covered by the 14 tests in `ops/selection.rs`, but rewrite them to use `select_sources()` for consistency rather than deleting.

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| `get_excluded_sources()` stays | Different semantics (finds excluded sources, not filters them out). Belongs to plan/execute extraction. |
| Rewrite tests, don't delete | Consistency with coverage.rs migration pattern. Tests verify the exclude-specific usage of selection. |
