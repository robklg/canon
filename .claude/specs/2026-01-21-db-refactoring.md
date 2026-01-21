# Refactoring Spec: Separate Path and Database Concerns in db.rs

## Overview

The `db.rs` module currently mixes three distinct responsibilities:
1. Pure database infrastructure (connection, schema, transactions)
2. Path/filesystem business logic (path manipulation, scope building)
3. Domain operations coupling both (root resolution, spec parsing)

This refactoring separates these concerns to improve testability, clarity, and maintainability. It establishes the pattern for moving away from the flat module structure.

## Phases

### Phase 1: Extract Pure Path Utilities
- **Status**: completed
- **Goal**: Move `path_is_under` and `path_strip_prefix` to a new `path.rs` module
- **Scope**:
  - Create `src/path.rs` with the two pure functions
  - Update imports in `ls.rs`, `apply.rs`, `exclude.rs`
  - Update internal usage in `db.rs`
  - Add unit tests for both functions
- **Non-goals**:
  - Not touching filesystem I/O functions (`canonicalize_*`)
  - Not touching database query functions
  - Not changing any function signatures
- **Dependencies**: None

### Phase 2: Extract Path Canonicalization
- **Status**: completed
- **Goal**: Move path canonicalization functions to `path.rs`
- **Scope**:
  - Move `canonicalize_scope`, `canonicalize_scopes` to `path.rs`
  - Update `path.rs` module documentation (no longer "pure" — includes I/O)
  - Update imports in `compare.rs`, `exclude.rs`, `ls.rs`, `cluster.rs`, `coverage.rs`, `worklist.rs`, `facts.rs`
- **Non-goals**:
  - Not moving SQL utilities (`SCOPE_CLAUSE`, `scope_param`, `build_scope_clause`) yet
  - Not touching root resolution functions
- **Dependencies**: Phase 1

### Phase 3: Introduce Scope Domain Concept
- **Status**: completed
- **Goal**: Create `scope.rs` with domain-level scope concepts, separating "what kind of match" from "how to express in SQL"
- **Scope**:
  - Create `src/scope.rs` with:
    - `ScopeMatch` enum (`ExactFile`, `UnderDirectory`)
    - `build_scope_clause()` refactored to take `&[ScopeMatch]` (no filesystem I/O)
  - Move `SCOPE_CLAUSE`, `scope_param` to `scope.rs`
  - Update callers to classify paths (file vs directory) before calling scope functions
  - Remove scope-related code from `db.rs`
- **Non-goals**: Not yet creating `domain/` directory structure
- **Dependencies**: Phase 2
- **Rationale**: Separates domain knowledge ("how do scopes work") from repository concerns ("how to query SQLite"). Enables future support for different storage backends.

### Phase 4: Clean Repository Boundary
- **Status**: completed
- **Goal**: `db.rs` becomes purely about SQLite infrastructure
- **Scope**:
  - Create `src/root.rs` with:
    - `RootSpec` enum (`ById`, `ByPath`) — domain concept for how users identify roots
    - `RootSpec::parse()` — pure parsing of spec strings
    - `find_containing_root()` — pure function to match paths against root candidates
  - Move orchestration functions to `root.rs`:
    - `parse_root_spec()`, `parse_root_spec_any()` — refactored to use `RootSpec::parse()`
    - `resolve_root_path()`, `resolve_root_path_any()` — refactored to use `find_containing_root()`
    - `resolve_archive_path()` — thin wrapper with role validation
  - `db.rs` retains only: `Db` struct, `open()`, `SCHEMA`, `populate_temp_sources()`
  - Update imports in `main.rs`, `roots.rs`, `scan.rs`, `cluster.rs`, `coverage.rs`, `apply.rs`
- **Non-goals**:
  - Not abstracting DB queries behind traits (Phase 5 territory)
  - Not abstracting filesystem operations
  - Not changing public API signatures of orchestration functions
- **Rationale**: Follows `scope.rs` pattern — extract domain concepts (`RootSpec`, `find_containing_root`) as pure functions, keep orchestration in the same module. Enables unit testing of domain logic.
- **Dependencies**: Phase 3

### Phase 5: (Future, Optional) Directory Structure
- **Status**: pending
- **Goal**: Evaluate whether to introduce `domain/` and `repository/` directories
- **Scope**:
  - If complexity warrants, restructure into:
    - `domain/` — `scope.rs`, `root.rs` (domain concepts and rules)
    - `repository/` — `db.rs`, traits (storage abstraction)
  - Define repository traits if multiple backends become likely
- **Non-goals**: Premature abstraction
- **Dependencies**: Phase 4
- **Trigger**: Consider this phase if/when alternative storage backends become a real requirement

## Design Decisions

- **Module naming**: Using `path.rs` (not `paths.rs` or `pathutil.rs`)
- **Testing approach**: Add unit tests alongside extractions to establish testing patterns
- **Preserve documentation**: Keep doc comments explaining semantic differences from naive string operations
- **Architectural direction**: Evolving toward separation of:
  - **Domain layer**: Core concepts and rules (scopes, roots, sources)
  - **Repository layer**: Storage implementation (SQLite, potentially others)
  - **Application layer**: Command modules orchestrating domain + repository
- **Incremental approach**: Flat modules first (`scope.rs`, `root.rs`), directory structure (`domain/`, `repository/`) only if complexity warrants
- **Scope domain concept**: `ScopeMatch` enum separates "what kind of match" (domain) from "how to express in SQL" (repository), enabling future storage backends
- **Root domain concept**: `RootSpec` enum represents how users identify roots (by ID or path), independent of storage. `find_containing_root()` is pure matching logic, enabling unit testing and future storage backends

## Test Requirements

### Existing Tests
- No existing tests for `db.rs` functions
- Must not break any existing functionality (verified via `cargo test` and manual testing)

### New Tests to Add

#### Phase 1 (completed)
- `path_is_under_exact_match`: Path equals prefix ✓
- `path_is_under_child`: Path is properly under prefix ✓
- `path_is_under_deep_child`: Deep nesting ✓
- `path_is_under_false_positive_prevention`: `/a/bc` is NOT under `/a/b` ✓
- `path_is_under_unrelated`: Completely different paths ✓
- `path_is_under_root`: Root path as prefix ✓
- `path_strip_prefix_basic`: Normal stripping case ✓
- `path_strip_prefix_deep`: Multiple segments ✓
- `path_strip_prefix_exact_match`: Path equals prefix (empty result) ✓
- `path_strip_prefix_not_under`: Returns None when not under prefix ✓
- `path_strip_prefix_unrelated`: Unrelated paths ✓

#### Phase 2 (completed)
- No new unit tests added (canonicalize functions are thin wrappers around `fs::canonicalize`)
- Verified all 33 existing tests pass
- Functions require filesystem I/O, better suited for integration tests

#### Phase 3 (completed)
- `build_scope_clause_empty`: Empty input returns "1=1" ✓
- `build_scope_clause_single_file`: ExactFile generates "= ?" clause ✓
- `build_scope_clause_single_directory`: UnderDirectory generates "LIKE ? || '/%'" ✓
- `build_scope_clause_mixed`: Mixed file/directory generates OR clause ✓
- `build_scope_clause_multiple_directories`: Multiple directories joined with OR ✓
- `scope_param_with_value`: Returns path when present ✓
- `scope_param_without_value`: Returns "" when None ✓
- Verified all 40 tests pass

#### Phase 4 (completed)
- `parse_root_spec_by_id`: Parse "id:123" format ✓
- `parse_root_spec_by_id_zero`: Parse "id:0" (edge case) ✓
- `parse_root_spec_by_path`: Parse "path:/foo/bar" format ✓
- `parse_root_spec_by_path_relative`: Relative paths accepted (caller canonicalizes) ✓
- `parse_root_spec_invalid_id`: Non-numeric ID returns error ✓
- `parse_root_spec_invalid_format`: Invalid format returns error ✓
- `find_containing_root_exact_match`: Path exactly matches root ✓
- `find_containing_root_under_root`: Path under root returns relative path ✓
- `find_containing_root_not_found`: Path not under any root returns None ✓
- `find_containing_root_not_under_similar_prefix`: `/a/bc` not under `/a/b` ✓
- `find_containing_root_multiple_roots_first_match`: First matching root wins ✓
- `find_containing_root_empty_roots`: Empty roots list returns None ✓
- Verified all 52 tests pass
