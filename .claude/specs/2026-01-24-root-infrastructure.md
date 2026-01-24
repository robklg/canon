# Refactoring Spec: Root Infrastructure

## Why This Refactoring Matters

### The Problem: Incomplete Domain Model for Roots

Canon's root system is **partially modeled** — `RootSpec` and `find_containing_root()` exist as pure domain concepts, but:

1. **No `Root` struct** — Root data is fetched as tuples `(id, path, role, comment, last_scanned_at, suspended, file_count)` scattered across commands
2. **No domain predicates** — Checks like "is this root suspended?" happen in SQL WHERE clauses or inline in commands
3. **Inconsistent fetching** — `roots.rs` does its own SQL; `source_repo.rs` JOINs to roots for denormalization; `root.rs` orchestration fetches for path resolution
4. **No tests for Root struct** — `root.rs` has tests for `RootSpec` parsing and `find_containing_root()`, but no `Root` domain model exists to test

### Why This Matters for Cluster Migration

The Cluster migration (the next major project after this) needs:

1. **Archive root detection** — To determine if content exists in an archive, Cluster needs to know which roots are archives
2. **Clean root resolution** — Cluster uses scopes that resolve to roots; having a `Root` struct makes this cleaner
3. **Root metadata** — Cluster manifests may need root information (role, path) for display or filtering

By establishing Root Infrastructure first, Cluster can use `Root` domain predicates instead of inline SQL checks.

### The Vision: Complete the Roots Layer

Following the Source Infrastructure template:

| Layer | Source | Root (Current) | Root (Target) |
|-------|--------|----------------|---------------|
| Domain | `source.rs` ✅ | `root.rs` (partial) | `root.rs` (enhanced) |
| Infrastructure | `source_repo.rs` ✅ | — | `root_repo.rs` |
| Command | `ls.rs` ✅ | `roots.rs` | `roots.rs` (migrated) |

---

## Architectural Model

### Current State

```
root.rs (mixed)
├── RootSpec enum (domain) ✅
├── find_containing_root() (domain) ✅
├── parse_root_spec() (orchestration, does SQL)
├── resolve_root_path() (orchestration, does SQL)
└── resolve_archive_path() (orchestration, does SQL)

roots.rs (command)
├── list() — own SQL queries, tuple handling
├── remove() — own SQL queries
├── suspend() / unsuspend() — use parse_root_spec_any()
└── set_comment() — use parse_root_spec()
```

### Target State

```
┌─────────────────────────────────────────────────────────────────┐
│  roots.rs (command layer)                                       │
│  - list(): fetch → filter with predicates → display             │
│  - remove/suspend/unsuspend/comment: keep as-is (write ops)     │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Domain Layer: root.rs                                          │
│                                                                  │
│  Existing (keep):           New:                                │
│  - RootSpec enum            - Root struct                       │
│  - find_containing_root()   - is_suspended()                    │
│  - parse_root_spec*()       - is_source() / is_archive()        │
│  - resolve_root_path*()     - matches_scope()                   │
│  - resolve_archive_path()                                       │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Infrastructure Layer: root_repo.rs                             │
│                                                                  │
│  - fetch_all() → Vec<Root>                                      │
│  - batch_fetch_by_ids() → HashMap<i64, Root>                    │
│                                                                  │
│  SQL does ONE thing: "Give me root data"                        │
└─────────────────────────────────────────────────────────────────┘
```

---

## Scope

### In Scope

1. **Enhance `root.rs`** with `Root` struct and predicates
2. **Create `root_repo.rs`** with batch fetch functions
3. **Migrate `roots.rs` list command** to use new infrastructure
4. **Add comprehensive tests** for domain and infrastructure layers

### Out of Scope

- **Write operations** — `remove()`, `suspend()`, `unsuspend()`, `set_comment()` stay as-is
- **Migrating other commands** — They can use the new infrastructure opportunistically
- **Changing `find_containing_root()`** — It works, no need to change
- **Object Infrastructure** — Separate project
- **Cluster migration** — Depends on this, but is a separate project

### Non-Goals

- Changing the roots table schema
- Changing command output format
- Adding new root functionality
- Performance optimization beyond batch fetching

---

## Data Model

### Root Struct

```rust
/// A root directory registered in canon.
///
/// Roots are the top-level directories that canon manages. Each root has a role
/// (source or archive) that determines how its contents are treated.
#[derive(Debug, Clone)]
pub struct Root {
    /// Database ID
    pub id: i64,
    /// Canonical absolute path
    pub path: String,
    /// Role: "source" or "archive"
    pub role: String,
    /// Optional user comment
    pub comment: Option<String>,
    /// Unix timestamp of last scan (None if never scanned)
    pub last_scanned_at: Option<i64>,
    /// Whether this root is suspended (hidden from most operations)
    pub suspended: bool,
}
```

### Domain Predicates

```rust
impl Root {
    /// Check if this root is suspended.
    pub fn is_suspended(&self) -> bool {
        self.suspended
    }

    /// Check if this root is active (not suspended).
    pub fn is_active(&self) -> bool {
        !self.suspended
    }

    /// Check if this root has the "source" role.
    pub fn is_source(&self) -> bool {
        self.role == "source"
    }

    /// Check if this root has the "archive" role.
    pub fn is_archive(&self) -> bool {
        self.role == "archive"
    }

    /// Check if this root matches a scope (path is at or under the scope,
    /// or scope is at or under this root's path).
    ///
    /// This bidirectional matching is used by `canon roots <scope>`.
    pub fn matches_scope(&self, scope: &str) -> bool {
        self.path.starts_with(scope) || scope.starts_with(&self.path)
    }
}
```

### Field Verification

Checking `roots.rs:list()` requirements:
- `id` — for display ✓
- `role` — for display ✓
- `path` — for display, scope filtering ✓
- `comment` — for display ✓
- `last_scanned_at` — for "time ago" display ✓
- `suspended` — for `[suspended]` marker, filtering ✓
- `file_count` — **derived, not stored** (computed via JOIN)

**Decision**: `file_count` is not part of `Root` struct. It's computed during listing via a separate query or passed alongside. This keeps `Root` pure — it represents stored data, not derived aggregates.

---

## Repository Layer

### root_repo.rs

```rust
//! Root repository — infrastructure layer for fetching roots.
//!
//! This module provides batch fetch functions that return `Root` structs.
//! It is intentionally simple — no domain logic, just data access.

use std::collections::HashMap;
use anyhow::Result;
use crate::db::Connection;
use crate::root::Root;

/// The columns we SELECT for Root construction.
const ROOT_COLUMNS: &str = "id, path, role, comment, last_scanned_at, suspended";

/// Construct a Root from a row. Column order must match ROOT_COLUMNS.
fn root_from_row(row: &rusqlite::Row) -> rusqlite::Result<Root> {
    Ok(Root {
        id: row.get(0)?,
        path: row.get(1)?,
        role: row.get(2)?,
        comment: row.get(3)?,
        last_scanned_at: row.get(4)?,
        suspended: row.get(5)?,
    })
}

/// Fetch all roots.
///
/// Returns roots in ID order. No filtering — caller applies domain predicates.
pub fn fetch_all(conn: &Connection) -> Result<Vec<Root>>

/// Fetch roots by specific IDs.
///
/// Returns HashMap for O(1) lookup. Missing IDs are not included.
pub fn batch_fetch_by_ids(conn: &Connection, root_ids: &[i64]) -> Result<HashMap<i64, Root>>
```

### Design Notes

1. **No `present` filter** — Unlike sources, roots don't have a `present` flag. All roots are always "present".

2. **No batching needed** — Root counts are small (dozens, not millions). No need for `BATCH_SIZE` chunking, though we can add it for consistency.

3. **`file_count` handled separately** — When listing needs file counts, we'll compute them via aggregate query and merge with Root data. This keeps repository simple.

---

## Phases

### Phase 1: Domain Types

- **Status**: ✅ completed
- **Goal**: Add `Root` struct and predicates to `root.rs`
- **Scope**:
  - Add `Root` struct with all fields
  - Add predicates: `is_suspended()`, `is_active()`, `is_source()`, `is_archive()`, `matches_scope()`
  - Keep existing `RootSpec`, `find_containing_root()`, orchestration functions unchanged
- **Tests Added** (12 total):
  - `is_suspended_true`, `is_suspended_false`
  - `is_active_when_not_suspended`, `is_active_when_suspended`
  - `is_source_true`, `is_source_false`
  - `is_archive_true`, `is_archive_false`
  - `matches_scope_root_under_scope`, `matches_scope_scope_under_root`, `matches_scope_exact_match`, `matches_scope_no_match`, `matches_scope_similar_prefix_no_match`
- **Notes**:
  - Dead code warning for `id`, `comment`, `last_scanned_at` — expected until Phase 2/3 uses them
  - `matches_scope()` uses `starts_with()` to match current `roots.rs` behavior
- **Dependencies**: None

### Phase 2: Repository Layer

- **Status**: ✅ completed
- **Goal**: Create `root_repo.rs` with fetch functions
- **Scope**:
  - Create `src/root_repo.rs`
  - Implement `fetch_all()`
  - Implement `batch_fetch_by_ids()`
  - Add to `main.rs` module declarations
- **Tests Added** (8 total):
  - `fetch_all_empty`, `fetch_all_returns_all`, `fetch_all_includes_suspended`, `fetch_all_with_domain_predicates`
  - `batch_fetch_by_ids_empty`, `batch_fetch_by_ids_found`, `batch_fetch_by_ids_partial`, `batch_fetch_by_ids_no_matching`
- **Dependencies**: Phase 1

### Phase 3: Migrate roots.rs List

- **Status**: ✅ completed
- **Goal**: Rewrite `list()` to use new infrastructure
- **Scope**:
  - Use `root_repo::fetch_all()` to get roots
  - Apply domain predicates for filtering (`is_suspended()`, `matches_scope()`)
  - Handle `file_count` via separate aggregate query
  - Preserve display format exactly
- **Validation**: ✅ All outputs identical
  - `canon roots` — IDENTICAL
  - `canon roots --suspended` — IDENTICAL
  - `canon roots /Volumes/share/castor-import` — IDENTICAL
- **Changes**:
  - Replaced inline SQL with `root_repo::fetch_all()` + domain predicates
  - Added `fetch_file_counts()` helper for file count aggregation
  - Uses `Root.is_suspended()`, `Root.is_active()`, `Root.matches_scope()` predicates
- **Dependencies**: Phase 2

### Phase 4: Cleanup and Documentation

- **Status**: ✅ completed
- **Goal**: Update documentation, verify consistency
- **Scope**:
  - Update `CLAUDE.md` with Root domain model documentation
  - Update this spec with learnings
  - Verify no dead code introduced
- **Dependencies**: Phase 3

---

## Test Requirements

### Existing Tests

`root.rs` already has 13 tests:
- 6 for `RootSpec::parse()`
- 7 for `find_containing_root()`

These must continue to pass.

### New Tests to Add

**Phase 1 (root.rs domain)**:
- ~10 tests for `Root` predicates (see Phase 1 scope)

**Phase 2 (root_repo.rs)**:
- ~6 tests using in-memory SQLite (see Phase 2 scope)

**Phase 3 (behavioral)**:
- Before/after output comparison for:
  - `canon roots` (default listing)
  - `canon roots --suspended-only`
  - `canon roots <scope>` (path filtering)

---

## Invariants

### Architectural

1. **Root struct is pure data** — No methods that do I/O
2. **Predicates are pure functions** — No database access, no side effects
3. **Repository does data access only** — No domain logic in SQL
4. **Write operations unchanged** — `suspend`, `unsuspend`, `remove`, `comment` stay as-is

### Implementation

1. **All root fields included** — `id`, `path`, `role`, `comment`, `last_scanned_at`, `suspended`
2. **`file_count` is NOT in Root** — It's derived, computed separately when needed
3. **No suspended filtering in repository** — Fetch all, filter in Rust

### Correctness

1. **Scope matching is bidirectional** — Root under scope OR scope under root
2. **Existing orchestration preserved** — `parse_root_spec*()`, `resolve_root_path*()` unchanged
3. **Output format unchanged** — Same columns, same alignment, same content

---

## Design Decisions

### Decision 1: file_count Not in Root Struct

**Chosen**: Keep `file_count` out of `Root`, compute via separate query

**Why**: `file_count` is a derived aggregate, not stored root data. Including it would:
- Couple `Root` to the sources table
- Require JOIN in basic fetch operations
- Make `Root` conceptually impure

**Trade-off**: Listing needs two queries (roots + count aggregation). Acceptable because:
- Root counts are small (dozens)
- Keeps domain model clean
- Can optimize later if needed

### Decision 2: No Suspended Filter in Repository

**Chosen**: `fetch_all()` returns all roots including suspended

**Why**: Consistent with Source Infrastructure pattern — repository fetches broadly, domain predicates filter. Makes the suspended logic testable and visible in Rust.

**Trade-off**: Fetches roots that might be filtered out. Negligible because root counts are small.

### Decision 3: Keep Orchestration Functions Unchanged

**Chosen**: `parse_root_spec()`, `resolve_root_path()`, etc. stay in `root.rs`

**Why**: These are working, tested orchestration functions. Refactoring them isn't necessary for Root Infrastructure goals. Can be revisited later if they become problematic.

**Trade-off**: Some mixing of domain and infrastructure in `root.rs`. Acceptable because:
- Orchestration functions are clearly marked in the file
- They use domain concepts (`RootSpec`, `find_containing_root()`) correctly
- Refactoring them would expand scope significantly

---

## Future Benefits

Once Root Infrastructure is complete:

1. **Cluster migration** — Can use `Root` predicates for archive detection
2. **Cleaner root resolution** — Other commands can use `root_repo::fetch_all()` + predicates
3. **Potential `find_containing_root()` enhancement** — Could take `&[Root]` instead of tuples
4. **Object Infrastructure** — Can use `Root` for archive root identification

---

## Validation Commands

Before Phase 3, capture baseline output:

```bash
# Default listing
canon roots > /tmp/roots_baseline.txt

# Suspended only
canon roots --suspended-only > /tmp/roots_suspended_baseline.txt 2>&1 || true

# Scoped (use an actual path from your system)
canon roots /path/to/some/root > /tmp/roots_scoped_baseline.txt
```

After Phase 3, compare:

```bash
diff /tmp/roots_baseline.txt <(canon roots)
diff /tmp/roots_suspended_baseline.txt <(canon roots --suspended-only 2>&1) || true
diff /tmp/roots_scoped_baseline.txt <(canon roots /path/to/some/root)
```

---

## Learnings

### Performance Improvement

The migration resulted in a noticeable performance improvement for `canon roots`:

- **Before**: Single query with `LEFT JOIN sources ... GROUP BY` computed file counts for ALL roots, even those that would be filtered out
- **After**: Fetch roots first, filter with predicates, then compute file counts only for filtered roots

For a database with ~50 roots and ~1.6M sources, this eliminates unnecessary aggregation work.

### Simplicity of Root vs Source

Root Infrastructure was simpler than Source Infrastructure:
- ~50 roots vs ~1.6M sources — no batching needed
- No two-level exclusion complexity
- No `present` flag to filter
- Fewer fields, simpler predicates

### Unused Predicates

`is_source()` and `is_archive()` are defined but not yet used by `roots.rs` — they'll be valuable for future work like cluster.rs which needs to identify archive roots.

### Final Stats

| Metric | Value |
|--------|-------|
| New files | `src/root_repo.rs` |
| Tests added | 20 (12 domain + 8 repository) |
| Tests total | 150 |
| Phases completed | 4 |

---

## References

- Template: `.claude/specs/2026-01-24-source-infrastructure.md`
- Prior work: `.claude/specs/2026-01-24-fact-infrastructure.md`
- Prior work: `.claude/specs/2026-01-24-fact-value-resolution.md`
- Current implementation: `src/root.rs`, `src/root_repo.rs`, `src/roots.rs`
