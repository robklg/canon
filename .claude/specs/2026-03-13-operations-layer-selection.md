# Refactoring Spec: Operations Layer — Source Selection

## Overview

Establish Canon's operations layer (`src/ops/`) and implement its first shared sub-operation: `select_sources()`. This replaces the eight private `get_matching_sources()` functions scattered across command modules with one designed, tested implementation. Migrates two commands (`coverage`, `worklist`) to validate the pattern.

**ADR**: `~/store/canon-architecture/2026-03-13-operations-layer.md`
**ADR Steps Covered**: Steps 1, 2, and 3 (shared types, CLAUDE.md update, `select_sources()` implementation), plus partial Step 4 (migrate coverage + worklist).

## Phases

### Phase 1: Establish `src/ops/` module with shared types + update CLAUDE.md
- **Status**: completed
- **Goal**: Create the operations layer module structure, define the shared types, and declare the four-layer architecture in CLAUDE.md.
- **Scope**: `src/ops/mod.rs` with types, `src/main.rs` module declaration, CLAUDE.md edits.
- **Non-goals**: No behavioral code, no tests (types only).
- **Dependencies**: None.

#### Changes

**Create `src/ops/mod.rs`:**

```rust
//! Operations layer for canon.
//!
//! Typed, interface-independent functions that express Canon's composed behaviors.
//! Operations accept typed parameters, perform behavioral logic (selection,
//! computation, ceremony policy), and return typed results. The interface layer
//! (CLI, TUI) parses user input into operation parameters and formats results
//! for display.
//!
//! ## What belongs here
//! - Composed behaviors: source selection, survey computation, exclusion plans
//! - Shared sub-operations consumed by multiple operations
//! - Typed parameter and result structs
//! - Ceremony policy: what data to show, when to confirm
//! - Transaction boundaries for write operations
//!
//! ## What does NOT belong here
//! - stdout/stderr/stdin (interface layer)
//! - CLI argument types, clap structs (interface layer)
//! - Display formatting (interface layer)
//! - Pure domain logic without I/O composition (domain layer)
//! - Direct SQL (repo layer)

pub mod selection;
```

**Create `src/ops/selection.rs`** (types only in Phase 1, implementation in Phase 2):

```rust
//! Source selection — the standard query contract for Canon commands.
//!
//! One implementation replaces eight private `get_matching_sources()` functions
//! across command modules. All commands that need filtered sources call
//! `select_sources()` with a `SelectionParams` and get back a `Selection`.

use crate::domain::include::IncludeSet;
use crate::domain::scope::ScopeMatch;
use crate::domain::source::Source;
use crate::expr::filter::Filter;

/// How to handle role filtering during source selection.
///
/// Canon has three distinct role policies, each used by a clear set of commands.
/// This enum makes the policy explicit rather than embedding it in per-command
/// filtering logic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RolePolicy {
    /// Include archive sources only when `IncludeSet::includes_archived()` is true.
    /// Default for most query commands: ls, facts, coverage, worklist, cluster.
    SourceUnlessIncluded,
    /// Only source roots, regardless of IncludeSet.
    /// Used by: exclude, survey (selection side).
    SourceOnly,
    /// All roles — no role filtering.
    /// Used by: compare.
    AnyRole,
}

/// Parameters for source selection — the standard query contract.
///
/// Built by the interface layer from CLI arguments (or TUI inputs) and passed
/// to `select_sources()`. Encapsulates all the dimensions of source filtering.
pub struct SelectionParams {
    /// Path scopes to restrict results (empty = all sources).
    pub scopes: Vec<ScopeMatch>,
    /// Visibility control: whether to include excluded/archived sources.
    pub include: IncludeSet,
    /// `--where` filter expressions (empty = no additional filtering).
    pub filters: Vec<Filter>,
    /// How to handle source vs archive role filtering.
    pub role_policy: RolePolicy,
}

/// Result of source selection — filtered sources plus visibility metadata.
///
/// All counts are always tracked regardless of which command calls this.
/// Commands that don't need specific counts simply ignore them. This prevents
/// the inconsistency where some commands "forget" to track excluded counts
/// for user hints.
pub struct Selection {
    /// Sources that passed all filters.
    pub sources: Vec<Source>,
    /// Sources that matched scope/role but were excluded (filtered out).
    pub excluded_count: usize,
    /// Excluded sources that were kept because `include.excluded` was set.
    pub included_excluded_count: usize,
    /// Archive sources that were kept because `include.archived` was set.
    pub included_archived_count: usize,
}

impl Selection {
    /// Extract source IDs from the selection.
    ///
    /// Convenience for commands that need IDs rather than full Source objects
    /// (e.g., facts, which passes IDs to batch fact fetching).
    pub fn source_ids(&self) -> Vec<i64> {
        self.sources.iter().map(|s| s.id).collect()
    }
}
```

**Update `src/main.rs`:**

Add `mod ops;` alongside the existing module declarations (between the infrastructure modules and the command modules).

**Update CLAUDE.md:**

1. Replace the "Architecture" intro paragraph (line 28) to mention four namespaces:

   > The codebase is organized into four namespaces (domain/, repo/, ops/, expr/) plus command modules:

2. Add new section after the Expression System block and before Command Modules:

   ```
   **Operations Layer** (`src/ops/`) - Composed behaviors, interface-independent:
   - `selection.rs` - Source selection: `select_sources()`, `RolePolicy`, `SelectionParams`, `Selection`
   ```

3. Replace the "Architectural Direction" section (lines 368-464) with the four-layer architecture:

   Update the diagram to show four layers:
   ```
   ┌─────────────────────────────────────────────────────────────┐
   │ Interface Layer (src/*.rs — CLI commands)                    │
   │ - CLI argument parsing (clap structs)                       │
   │ - Output formatting (terminal, JSONL, null-delimited)       │
   │ - Ceremony presentation (display plan, prompt, report)      │
   │ - The ONLY layer that knows about stdout/stderr/stdin       │
   └─────────────────────────────────────────────────────────────┘
                             │
                             ▼
   ┌─────────────────────────────────────────────────────────────┐
   │ Operations Layer (src/ops/)                                  │
   │ - Typed operation functions with typed results               │
   │ - Shared sub-operations (select_sources, etc.)              │
   │ - Ceremony policy: what to show, when to confirm            │
   │ - Transaction boundaries for write operations               │
   │ - Interface-independent — no stdout, stderr, stdin          │
   └─────────────────────────────────────────────────────────────┘
                             │
             ┌───────────────┴───────────────┐
             ▼                               ▼
   ┌──────────────────────────────┐   ┌──────────────────────────────┐
   │ Repository Layer (repo/)     │   │ Domain Layer (domain/)       │
   │ - ALL database access        │   │ - Pure structs and predicates│
   │ - Returns domain types       │   │ - Business logic functions   │
   │ - Batch operations           │   │ - NO I/O, fully unit-testable│
   │ - SQL lives HERE ONLY        │   │ - Path computation/comparison│
   │ - NO transaction management  │   │                              │
   └──────────────────────────────┘   └──────────────────────────────┘
   ```

   Update the Layer Responsibilities table to include Operations and Interface:

   | Layer | Allowed | NOT Allowed |
   |-------|---------|-------------|
   | **Interface** | CLI parsing, output formatting, ceremony presentation, terminal I/O | Business logic, source selection logic, ceremony policy, direct repo calls (use ops/) |
   | **Operations** | Composing domain + repo into typed behaviors, ceremony policy, transactions (writes) | stdout/stderr/stdin, CLI argument types, display formatting |
   | **Repo** | Database queries, returning domain types, batch operations | Business logic, transaction management, path construction |
   | **Domain** | Pure functions, structs, predicates, business logic | Any I/O (database, filesystem, network) |

   Update the "When Adding New Features" section to include ops:

   1. If you need a predicate or business logic → add to domain layer (pure function)
   2. If you need database access → add to repo layer (returns domain types)
   3. If you need composed behavior (selection, computation, ceremony policy) → add to ops layer
   4. Interface modules should ONLY parse arguments, call operations, and format output
   5. When refactoring existing commands, extract behavioral logic to ops layer

   Add a new subsection "Operations Layer Conventions":

   > **Operations Layer Conventions** (`src/ops/`):
   >
   > Operations are typed, interface-independent functions. They compose domain predicates and repo functions into Canon's behavioral contracts.
   >
   > - **Read operations** take `&mut Connection` (needed for filter temp tables). No transaction management.
   > - **Write operations** take `&mut Db` and own their transactions.
   > - **Result types** are concrete structs per operation — no generic containers or trait hierarchies.
   > - **No stdout/stderr/stdin** — operations return data, the interface formats it.
   >
   > The reference implementation is `ops::selection::select_sources()`:
   > ```rust
   > let selection = ops::selection::select_sources(conn, &params)?;
   > // selection.sources — filtered sources
   > // selection.source_ids() — convenience for ID-based consumers
   > // selection.excluded_count — for "N excluded hidden" hints
   > ```

   Keep the existing Command pattern code example but update the comment to note this is the legacy pattern being replaced by `ops::select_sources()` during migration.

### Phase 2: Implement `select_sources()` with comprehensive tests
- **Status**: completed
- **Goal**: The single implementation that replaces eight private functions.
- **Scope**: `src/ops/selection.rs` implementation and test suite.
- **Non-goals**: No command module changes yet. No test helper sharing beyond what's needed for this module.
- **Dependencies**: Phase 1.

#### Changes

**Implement `select_sources()` in `src/ops/selection.rs`:**

```rust
use crate::repo;
use crate::repo::Connection;
use anyhow::Result;

/// Select sources matching the given parameters.
///
/// This is the single implementation of Canon's standard source selection
/// contract. All commands that need filtered sources call this function.
///
/// The pipeline:
/// 1. Fetch all roots via `repo::root::fetch_all()`
/// 2. Fetch all present sources via `repo::source::batch_fetch_by_roots()`
/// 3. Apply domain predicates: active, role policy, scope, exclusion
/// 4. Track visibility counts (excluded, included_excluded, included_archived)
/// 5. Apply `--where` filters via `filter::apply_filters()`
pub fn select_sources(
    conn: &mut Connection,
    params: &SelectionParams,
) -> Result<Selection> {
    // 1. Fetch all root IDs consistently via repo layer
    let roots = repo::root::fetch_all(conn)?;
    let root_ids: Vec<i64> = roots.iter().map(|r| r.id).collect();

    // 2. Fetch all present sources
    let all_sources = repo::source::batch_fetch_by_roots(conn, &root_ids)?;

    // 3. Filter using domain predicates, tracking counts
    let mut excluded_count = 0usize;
    let mut included_excluded_count = 0usize;
    let mut included_archived_count = 0usize;

    let filtered: Vec<Source> = all_sources
        .into_iter()
        .filter(|s| s.is_active())
        .filter(|s| match params.role_policy {
            RolePolicy::SourceUnlessIncluded => {
                params.include.includes_archived() || s.is_from_role("source")
            }
            RolePolicy::SourceOnly => s.is_from_role("source"),
            RolePolicy::AnyRole => true,
        })
        .filter(|s| s.matches_scope(&params.scopes))
        .filter(|s| {
            if s.is_excluded() && !params.include.includes_excluded() {
                excluded_count += 1;
                return false;
            }
            if s.is_excluded() {
                included_excluded_count += 1;
            }
            if s.is_from_role("archive") {
                included_archived_count += 1;
            }
            true
        })
        .collect();

    // 4. Apply --where filters if present
    if params.filters.is_empty() {
        return Ok(Selection {
            sources: filtered,
            excluded_count,
            included_excluded_count,
            included_archived_count,
        });
    }

    let source_ids: Vec<i64> = filtered.iter().map(|s| s.id).collect();
    let filtered_ids = crate::expr::filter::apply_filters(conn, &source_ids, &params.filters)?;
    let filtered_id_set: std::collections::HashSet<i64> = filtered_ids.into_iter().collect();

    let result: Vec<Source> = filtered
        .into_iter()
        .filter(|s| filtered_id_set.contains(&s.id))
        .collect();

    Ok(Selection {
        sources: result,
        excluded_count,
        included_excluded_count,
        included_archived_count,
    })
}
```

#### Tests

All tests in `#[cfg(test)] mod tests` inside `src/ops/selection.rs`.

**Test helpers:**

```rust
fn setup_test_db() -> Connection {
    crate::repo::db::open_in_memory_for_test()
}

fn insert_root(conn: &Connection, path: &str, role: &str, suspended: bool) -> i64 {
    // Insert root with suspended flag
}

fn insert_object(conn: &Connection, hash: &str, excluded: bool) -> i64 {
    // Insert object with excluded flag
}

fn insert_source(conn: &Connection, root_id: i64, rel_path: &str, object_id: Option<i64>) -> i64 {
    // Insert source, present=1, excluded=0
}

fn insert_source_excluded(conn: &Connection, root_id: i64, rel_path: &str, object_id: Option<i64>) -> i64 {
    // Insert source, present=1, excluded=1 (source-level exclusion)
}

// Convenience: build SelectionParams with defaults
fn make_params(role_policy: RolePolicy) -> SelectionParams {
    SelectionParams {
        scopes: vec![],
        include: IncludeSet::default(),
        filters: vec![],
        role_policy,
    }
}
```

**Test specifications (14 tests):**

1. **`select_sources_returns_active_sources_only`**
   - Setup: Two roots, one suspended. Sources in each.
   - Assert: Only sources from the active root appear in results.

2. **`select_sources_role_source_unless_included_default`**
   - Setup: One source root, one archive root with sources in each.
   - Params: `SourceUnlessIncluded`, `include.archived = false`.
   - Assert: Only source-root sources returned. `included_archived_count == 0`.

3. **`select_sources_role_source_unless_included_with_archived`**
   - Setup: Same as above.
   - Params: `SourceUnlessIncluded`, `include.archived = true`.
   - Assert: Both source and archive sources returned. `included_archived_count` equals archive source count.

4. **`select_sources_role_source_only`**
   - Setup: One source root, one archive root.
   - Params: `SourceOnly`, `include.archived = true`.
   - Assert: Only source-root sources returned (IncludeSet.archived is ignored).

5. **`select_sources_role_any`**
   - Setup: One source root, one archive root.
   - Params: `AnyRole`, default IncludeSet.
   - Assert: Both source and archive sources returned.

6. **`select_sources_scope_filtering`**
   - Setup: Sources under `/photos/` and `/videos/`.
   - Params: `scopes = [UnderDirectory("/photos")]`.
   - Assert: Only `/photos/` sources returned.

7. **`select_sources_empty_scopes_returns_all`**
   - Setup: Sources in multiple directories.
   - Params: `scopes = []`.
   - Assert: All sources returned.

8. **`select_sources_excludes_excluded_by_default`**
   - Setup: Source with source-level exclusion, source with object-level exclusion, normal source.
   - Params: Default IncludeSet (excluded = false).
   - Assert: Only normal source returned. `excluded_count == 2`.

9. **`select_sources_includes_excluded_when_requested`**
   - Setup: Same as above.
   - Params: `include.excluded = true`.
   - Assert: All three sources returned. `excluded_count == 0`. `included_excluded_count == 2`.

10. **`select_sources_counts_included_archived`**
    - Setup: Source root + archive root, each with sources.
    - Params: `SourceUnlessIncluded`, `include.archived = true`.
    - Assert: `included_archived_count` equals number of archive sources.

11. **`select_sources_applies_where_filters`**
    - Setup: Multiple sources with varying sizes.
    - Params: Filter expression for `source.size > 5000`.
    - Assert: Only sources matching the filter returned. Excluded counts unaffected by --where.

12. **`select_sources_empty_database`**
    - Setup: Empty database (no roots, no sources).
    - Assert: Returns empty `Selection`, all counts zero.

13. **`select_sources_no_matching_sources`**
    - Setup: Sources exist but all under different scope.
    - Params: Scope that matches nothing.
    - Assert: Empty sources, counts zero.

14. **`select_sources_source_ids_convenience`**
    - Setup: A few sources.
    - Assert: `selection.source_ids()` returns the correct IDs in the same order as `selection.sources`.

### Phase 3: Migrate `coverage.rs` and `worklist.rs`
- **Status**: completed
- **Goal**: First two commands consume `ops::select_sources()`, validating the pattern.
- **Scope**: Replace private `get_matching_sources()` in both files.
- **Non-goals**: No changes to output formatting, no changes to other commands, no test migration (existing tests stay).
- **Dependencies**: Phase 2.

#### Changes

**`src/coverage.rs`:**

Delete `get_matching_sources()` function (lines 138-174). Replace call sites with:

```rust
use crate::ops::selection::{self, RolePolicy, SelectionParams};

// In compute_scoped_stats() and wherever get_matching_sources was called:
let params = SelectionParams {
    scopes: scopes.to_vec(),
    include: include.clone(),
    filters: filters.to_vec(),
    role_policy: RolePolicy::SourceUnlessIncluded,
};
let selection = selection::select_sources(conn, &params)?;
// Use selection.sources where sources were used before
```

The `run()` function currently shows an `[including ...]` annotation when `include.is_expanded()`. This stays in coverage.rs — it's interface-level output.

**`src/worklist.rs`:**

Delete `get_matching_sources()` function (lines 133-183) and its raw SQL root-ID fetch. Replace the call site in `run()`:

```rust
use crate::ops::selection::{self, RolePolicy, SelectionParams};

// Replace lines 83-85:
let params = SelectionParams {
    scopes: scopes.clone(),
    include: include.clone(),
    filters: filters.clone(),
    role_policy: RolePolicy::SourceUnlessIncluded,
};
let selection = selection::select_sources(conn, &params)?;
let sources = selection.sources;
let excluded_count = selection.excluded_count;
```

The existing stderr reporting logic for excluded counts stays — it just reads from `selection.excluded_count` instead of the tuple.

#### Tests

- **Rewrite the 4 `coverage.rs` selection tests** to call `selection::select_sources()` instead of the deleted `get_matching_sources()`. The tests' assertions stay the same — they're testing the same behavioral contracts — but the call target changes to the canonical implementation.
  - `test_get_matching_sources_respects_scope` → `test_coverage_selection_respects_scope` (uses `select_sources` with `SourceUnlessIncluded`)
  - `test_coverage_excludes_excluded_sources` → update to use `select_sources`
  - `test_coverage_includes_excluded_when_requested` → update to use `select_sources`
  - `test_coverage_archived_counts_sources_not_objects` — this test doesn't test selection; it tests archive counting logic in `compute_stats_from_sources`. Stays unchanged.
- All other 648 tests must pass (`cargo test`).
- `worklist.rs` has no tests — correctness is validated by the `select_sources()` test suite.

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| `select_sources()` always returns `Vec<Source>`, never IDs | Uniform interface. Commands needing IDs use `selection.source_ids()`. The allocation is cheap. |
| All three counts always tracked | Prevents inconsistency where commands "forget" to count excluded sources for hints. |
| `select_sources()` takes `&mut Connection`, not `&mut Db` | Read-only operation — no transaction management needed. `&mut` required by `filter::apply_filters()`. |
| Root IDs fetched via `repo::root::fetch_all()` consistently | Eliminates raw SQL `SELECT id FROM roots` in ls.rs and worklist.rs. |
| `RolePolicy` is an enum, not a boolean | Three distinct policies need distinct names. Future policies (if any) are additive. |
| Scope resolution stays in the interface layer | It involves resolving user-provided paths (filesystem I/O). `select_sources()` receives pre-resolved `ScopeMatch` values. |
| `exclude.rs`'s `get_excluded_sources()` not covered | Unique semantics (source-level exclusion only). Separate concern for a later step. |

## Test Requirements

### Existing Tests (Must Pass)
All 648 tests, especially:
- `coverage::tests::test_get_matching_sources_respects_scope`
- `coverage::tests::test_coverage_archived_counts_sources_not_objects`
- `coverage::tests::test_coverage_excludes_excluded_sources`
- `coverage::tests::test_coverage_includes_excluded_when_requested`

### New Tests to Add
14 tests in `ops::selection::tests` (detailed in Phase 2 above). These cover:
- Suspension filtering (currently only tested in exclude.rs)
- All three role policies (currently only SourceOnly tested in exclude.rs)
- Scope filtering (currently only in coverage.rs and exclude.rs)
- Exclusion filtering with both levels (currently only in exclude.rs)
- IncludeSet behavior (currently only in coverage.rs and exclude.rs)
- `--where` filter integration (currently untested everywhere)
- Count tracking for all three counters (currently no command tests all three)
- Empty/no-match edge cases
- `source_ids()` convenience method
