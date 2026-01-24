# Refactoring Spec: Source Infrastructure

## Why This Refactoring Matters

### The Problem: Domain Logic Scattered in SQL

Canon's reliability depends on correctly selecting and filtering sources. Currently, this critical logic is:

1. **Embedded in SQL strings** - Scope matching, exclusion filtering, role checks are all SQL WHERE clauses
2. **Duplicated across commands** - Each command rebuilds similar but subtly different queries
3. **Difficult to verify** - SQL string construction can't be unit tested; bugs manifest at runtime
4. **Hard to reason about** - Understanding "what sources will this select?" requires mentally executing SQL

When a user runs `canon cluster generate`, they trust that the right sources are selected. A bug in source selection could mean:
- Missing files in the archive
- Including files that should be excluded
- Silent data loss

**This is a correctness problem, not just a performance problem.**

### The Vision: A Unified Domain Model

We want Canon to have a **single, authoritative representation** of its core concepts:
- What is a Source? → One `Source` struct
- What does "matches scope" mean? → One `matches_scope()` function
- What does "excluded" mean? → One `is_excluded()` predicate

When domain logic lives in one place:
- **It can be tested thoroughly** - Unit tests prove the predicates work correctly
- **It can be reasoned about** - Read the Rust code, understand the behavior
- **It can't drift** - All commands use the same predicates, so they behave consistently
- **Bugs are caught early** - Type system and tests catch errors before runtime

### Performance as a Positive Side Effect

While we initially discovered this need through N+1 query performance issues, the primary benefit is **reliability through clarity**. The performance improvement is a welcome side effect of:
- Batch fetching (fewer round trips)
- Simpler SQL (optimizer works better)
- In-memory filtering (Rust is fast)

### The End State

Once this pattern is applied across all Canon facilities (sources, facts, roots, objects), the tool achieves a new level of solidity:

- **For users**: Commands behave consistently and predictably
- **For development**: Changes to domain logic are safe because tests verify correctness
- **For the codebase**: Clear separation makes the code easier to understand and modify

This is the foundation for Canon to grow reliably.

---

## Architectural Model

### Current State: Domain Logic in SQL
```
Commands → Complex SQL with embedded domain logic
           (scope clauses, exclusion filters, role checks)

Problem: Logic is untestable, duplicated, and error-prone
```

### Target State: Unified Domain Model
```
┌─────────────────────────────────────────────────────────┐
│  Application Layer (commands)                           │
│  - Compose domain predicates                            │
│  - Orchestrate: fetch → filter → transform → output     │
│  - Commands are thin orchestration layers               │
└─────────────────────────────────────────────────────────┘
                          │
          ┌───────────────┴───────────────┐
          ▼                               ▼
┌──────────────────────┐    ┌──────────────────────────────┐
│  Domain Layer        │    │  Infrastructure Layer        │
│  source.rs:          │    │  source_repo.rs:             │
│  - Source struct     │    │  - batch_fetch_by_roots()    │
│  - matches_scope()   │    │  - batch_fetch_by_ids()      │
│  - is_excluded()     │    │                              │
│  - is_from_role()    │    │  SQL does ONE thing:         │
│                      │    │  "Give me source data"       │
│  This is the TRUTH   │    │  No filtering, no logic      │
│  about sources       │    │                              │
└──────────────────────┘    └──────────────────────────────┘
```

### Why This Structure

**Domain Layer is the Source of Truth**
- The `Source` struct defines what a source IS
- The predicates define what operations MEAN
- This layer has zero dependencies on infrastructure
- It can be tested in complete isolation

**Infrastructure Layer is Dumb Storage**
- SQL becomes trivial: `SELECT * FROM sources WHERE id IN (?)`
- No domain logic leaks into SQL
- Easy to verify SQL is correct (it does almost nothing)
- Could be swapped for different storage (not a goal, but possible)

**Application Layer Composes**
- Commands become orchestration: fetch, filter, format, output
- Filtering uses domain predicates: `sources.iter().filter(|s| s.matches_scope(&scopes))`
- Logic is visible, readable, debuggable

### Design Principles

1. **Domain logic belongs in Rust, not SQL** - If it's a business rule, it's a Rust function
2. **One source of truth** - Each concept has one authoritative definition
3. **Predicates are pure functions** - No side effects, easy to test
4. **Dependencies point inward** - Commands → Domain, Domain → nothing

## Scope

### In Scope (This Project)
- `source.rs` - Domain types and predicates
- `source_repo.rs` - Batch fetch infrastructure
- Migrate read paths in: `ls.rs`, `worklist.rs`, `compare.rs`, `coverage.rs`
- `filter.rs` - Source field access for predicates
- `exclude.rs` - Read paths (exclusion checks)

### Out of Scope (This Project)
- Write operations: `scan.rs`, `apply.rs`, `import_facts.rs`
- `cluster.rs` - Requires fact integration (separate project)
- Fact infrastructure (separate project, see Future Work)
- Root infrastructure (separate project)
- Object infrastructure (separate project)

### Non-Goals
- Abstracting database connection/traits (premature)
- Supporting non-SQLite backends (premature)
- Changing command output or behavior

## Data Model

### Source Struct

```rust
/// Core source data - sufficient for most read operations
pub struct Source {
    pub id: i64,
    pub root_id: i64,
    pub root_path: String,      // From joined roots table
    pub rel_path: String,
    pub object_id: Option<i64>,
    pub size: i64,
    pub mtime: i64,
    // Exclusion: source-level and object-level (both must be checked)
    pub excluded: bool,
    pub object_excluded: Option<bool>,  // None if no object_id
    // Extended fields (needed by worklist, cluster)
    pub device: i64,
    pub inode: i64,
    pub partial_hash: String,
    // Role comes from root, cached here for filtering
    pub root_role: String,
    pub root_suspended: bool,
}
```

**Field verification against command needs:**
- `ls`: path, object_id, size, mtime ✓
- `worklist`: + device, inode, partial_hash, excluded ✓
- `coverage`: id, object_id, root_id, excluded ✓
- `compare`: path, object_id ✓
- `filter.rs`: rel_path, root_path (for path accessors) ✓

### Why include all fields?

Memory estimate with real data:
- 1.59M sources in production database
- ~300 bytes per Source struct
- Total: ~477 MB if loading ALL sources

However:
- Most operations are scoped (subset of sources)
- 65% excluded (can filter early if needed)
- CLI context tolerates brief memory spikes
- Simpler to have one struct than multiple variants

### Domain Predicates

```rust
impl Source {
    /// Computed full path
    pub fn path(&self) -> String {
        if self.rel_path.is_empty() {
            self.root_path.clone()
        } else {
            format!("{}/{}", self.root_path, self.rel_path)
        }
    }

    /// Check if source matches any of the given scopes
    pub fn matches_scope(&self, scopes: &[ScopeMatch]) -> bool {
        if scopes.is_empty() {
            return true;
        }
        let full_path = self.path();
        scopes.iter().any(|scope| match scope {
            ScopeMatch::ExactFile(path) => full_path == *path,
            ScopeMatch::UnderDirectory(dir) => {
                full_path == *dir ||
                (full_path.starts_with(dir) &&
                 full_path.as_bytes().get(dir.len()) == Some(&b'/'))
            }
        })
    }

    /// Check exclusion status (source-level OR object-level)
    pub fn is_excluded(&self) -> bool {
        self.excluded || self.object_excluded.unwrap_or(false)
    }

    /// Check if from a specific root role
    pub fn is_from_role(&self, role: &str) -> bool {
        self.root_role == role
    }

    /// Check if root is active (not suspended)
    pub fn is_active(&self) -> bool {
        !self.root_suspended
    }

    /// Check if source is present on disk
    pub fn is_present(&self) -> bool {
        // Note: We only fetch present sources, so this is always true
        // in current design. Kept for clarity.
        true
    }
}
```

## Repository Layer

### source_repo.rs

```rust
/// Fetch all present sources for given root IDs
/// This is a simple fetch - no filtering beyond present=1
pub fn batch_fetch_by_roots(
    conn: &Connection,
    root_ids: &[i64]
) -> Result<Vec<Source>>

/// Fetch sources by specific IDs
/// Returns HashMap for O(1) lookup
pub fn batch_fetch_by_ids(
    conn: &Connection,
    source_ids: &[i64]
) -> Result<HashMap<i64, Source>>

/// Fetch just source IDs for given roots (for pagination patterns)
pub fn fetch_source_ids_by_roots(
    conn: &Connection,
    root_ids: &[i64]
) -> Result<Vec<i64>>
```

### SQL Simplification

**Before** (in ls.rs):
```sql
SELECT s.id FROM sources s
JOIN roots r ON s.root_id = r.id
LEFT JOIN objects o ON s.object_id = o.id
WHERE s.present = 1
  AND r.role = 'source' AND r.suspended = 0
  AND (complex scope clause)
  AND (exclusion clause)
ORDER BY s.id LIMIT ?
```

**After**:
```sql
SELECT s.id, s.root_id, r.path, s.rel_path, s.object_id,
       s.size, s.mtime, s.excluded, o.excluded as object_excluded,
       s.device, s.inode, s.partial_hash, r.role, r.suspended
FROM sources s
JOIN roots r ON s.root_id = r.id
LEFT JOIN objects o ON s.object_id = o.id
WHERE s.present = 1 AND s.root_id IN (?)
```

Note: LEFT JOIN on objects to get `object_excluded`. Returns NULL if no object_id.

Filtering happens in Rust:
```rust
sources.iter()
    .filter(|s| s.is_active())
    .filter(|s| s.is_from_role("source"))
    .filter(|s| s.matches_scope(&scopes))
    .filter(|s| !s.is_excluded() || include_excluded)
```

## Phases

### Phase 1: Domain Types
- **Status**: ✅ completed
- **Goal**: Create `source.rs` with Source struct and predicates
- **Scope**:
  - Create `src/source.rs`
  - Define `Source` struct with all fields
  - Implement `path()`, `matches_scope()`, `is_excluded()`, `is_from_role()`, `is_active()`
  - Add unit tests for all predicates
- **Tests Added** (22 total):
  - `path_combines_root_and_rel`, `path_handles_empty_rel_path`, `path_handles_single_segment_rel`
  - `matches_scope_empty_scopes_matches_everything`
  - `matches_scope_exact_file_match`, `matches_scope_exact_file_no_match`
  - `matches_scope_under_directory`, `matches_scope_directory_itself_matches`
  - `matches_scope_not_under_similar_prefix`, `matches_scope_not_under_similar_prefix_deeper`
  - `matches_scope_multiple_scopes_any_match`, `matches_scope_multiple_scopes_none_match`
  - `is_excluded_*` (6 tests covering source/object/both/none cases)
  - `is_from_role_source`, `is_from_role_archive`
  - `is_active_when_not_suspended`, `is_active_when_suspended`
- **Dependencies**: None

### Phase 2: Repository Layer
- **Status**: pending
- **Goal**: Create `source_repo.rs` with batch fetch functions
- **Scope**:
  - Create `src/source_repo.rs`
  - Implement `batch_fetch_by_roots()`
  - Implement `batch_fetch_by_ids()`
  - Use BATCH_SIZE chunking for large ID sets
- **Tests to Add**:
  - Integration tests with in-memory SQLite
  - `batch_fetch_by_roots_empty` - returns empty vec
  - `batch_fetch_by_roots_single_root` - fetches all sources for root
  - `batch_fetch_by_roots_multiple_roots` - fetches from all roots
  - `batch_fetch_by_ids_empty` - returns empty hashmap
  - `batch_fetch_by_ids_found` - returns matching sources
  - `batch_fetch_by_ids_partial` - handles missing IDs gracefully
- **Dependencies**: Phase 1

### Phase 3: Migrate ls.rs
- **Status**: pending
- **Goal**: Refactor `ls.rs` to use new infrastructure
- **Scope**:
  - Replace `get_matching_sources()` with new pattern
  - Remove `batch_fetch_sources()` (now in source_repo)
  - Use domain predicates for filtering
  - Preserve all existing behavior
- **Validation**:
  - Manual testing: `canon ls`, `canon ls /path`, `canon ls --where`, etc.
  - Performance comparison with `--profile`
- **Dependencies**: Phase 2

### Phase 4: Migrate worklist.rs
- **Status**: pending
- **Goal**: Refactor `worklist.rs` to use new infrastructure
- **Scope**:
  - Replace `fetch_batch()` / `fetch_entry()` with batch pattern
  - Use domain predicates for filtering
  - Preserve JSONL output format exactly
- **Validation**:
  - Manual testing with real worklist pipelines
  - Compare output before/after
- **Dependencies**: Phase 2

### Phase 5: Migrate compare.rs and coverage.rs
- **Status**: pending
- **Goal**: Complete migration of remaining query commands
- **Scope**:
  - Migrate `compare.rs` (already clean, should be simple)
  - Migrate `coverage.rs`
- **Dependencies**: Phase 4

### Phase 6: Cleanup and Documentation
- **Status**: pending
- **Goal**: Remove dead code, document patterns
- **Scope**:
  - Remove SQL scope clause builders if unused
  - Update CLAUDE.md with new architecture
  - Document the pattern for future facilities

## Future Work: Completing the Unified Domain Model

This spec establishes the **template pattern** for Canon's domain model. Once sources are complete, we apply the same pattern to other facilities. Each follows the same structure:

1. **Domain module** (`<facility>.rs`) - Struct + predicates, the source of truth
2. **Repository module** (`<facility>_repo.rs`) - Simple batch fetch, no logic
3. **Migrate commands** - Replace SQL logic with domain predicates
4. **Tests** - Unit tests for domain, integration tests for commands

### Fact Infrastructure (Future Project)

**Why it matters**: Fact queries power `--where` filters, `cluster` manifests, and coverage reports. Currently, fact access is scattered and inconsistent.

**Scope**:
- `fact.rs` - Domain types (Fact struct, FactValue enum, fact predicates)
- `fact_repo.rs` - Batch fetch facts by entity type/id
- Migrate: `facts.rs` command, `cluster.rs` fact collection, `filter.rs` fact evaluation
- Enables: Full `cluster.rs` migration (currently deferred due to fact dependency)

### Root Infrastructure (Future Project)

**Why it matters**: Root selection affects every command. Suspension, roles, and path matching should be consistent.

**Scope**:
- Already partially done: `root.rs` has `RootSpec`, `find_containing_root()`
- Add: `Root` struct with all fields, `root_repo.rs` for batch fetch
- Migrate: `roots.rs` listing, root resolution in all commands

### Object Infrastructure (Future Project)

**Why it matters**: Archive checking ("is this content in an archive?") happens in multiple places with duplicated logic.

**Scope**:
- `object.rs` - Object struct, archive status predicates
- `object_repo.rs` - Batch fetch by hash or ID
- Migrate: Archive checking in `ls.rs`, `cluster.rs`, `coverage.rs`

### The Complete Picture

When all facilities are migrated:

```
┌─────────────────────────────────────────────────────────┐
│  Commands (ls, worklist, cluster, facts, coverage...)   │
│  - Thin orchestration layers                            │
│  - Compose domain predicates                            │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│  Domain Layer (the unified model)                       │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐   │
│  │ source.rs│ │ fact.rs  │ │ root.rs  │ │ object.rs│   │
│  │ Source   │ │ Fact     │ │ Root     │ │ Object   │   │
│  │ +preds   │ │ +preds   │ │ +preds   │ │ +preds   │   │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘   │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│  Infrastructure Layer                                   │
│  source_repo.rs  fact_repo.rs  root_repo.rs  obj_repo   │
│  (simple fetch)  (simple fetch) (simple fetch) (...)    │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│  SQLite (db.rs)                                         │
│  - Connection management                                │
│  - Schema                                               │
└─────────────────────────────────────────────────────────┘
```

This is Canon's target architecture: a unified domain model where each concept has one authoritative definition, all commands behave consistently, and correctness can be verified through tests.

## Design Decisions

These decisions prioritize **correctness and simplicity** over premature optimization.

### Decision 1: Single Source struct vs. multiple variants
**Chosen**: Single struct with all fields
**Why**: One struct = one source of truth. Multiple variants create ambiguity about "which Source do I have?" and risk inconsistency. The memory overhead (~300 bytes × source count) is acceptable for a CLI tool.
**Trade-off**: Slightly higher memory when only minimal fields needed. Acceptable because clarity trumps micro-optimization.

### Decision 2: Include root_path and root_role in Source
**Chosen**: Yes, denormalize these from the roots table into Source
**Why**: Predicates like `matches_scope()` and `is_from_role()` need this data. If we don't include it, predicates would need database access, breaking the "pure domain layer" principle.
**Trade-off**: Denormalization means updating a root doesn't automatically update cached Sources. Acceptable because Sources are fetched fresh each command run.

### Decision 3: Always fetch present=1 sources in repository
**Chosen**: Yes, hardcode `WHERE present = 1` in fetch functions
**Why**: No query command currently needs non-present (deleted) sources. Baking this in simplifies the API and prevents accidental inclusion of stale data.
**Trade-off**: If a future command needs historical sources, we'd add a separate function. That's fine - explicit is better than a boolean parameter.

### Decision 4: Batch size of 1000
**Chosen**: 1000 IDs per SQL IN clause
**Why**: Proven to work in existing code. Balances between too many queries (small batches) and SQL parsing overhead (huge IN clauses).
**Trade-off**: Not scientifically optimized, but good enough. Can tune later if profiling shows need.

### Decision 5: Filter in Rust, not SQL
**Chosen**: Fetch broadly, filter with Rust predicates
**Why**: This is the core architectural principle. Domain logic in Rust can be unit tested, reasoned about, and shared. Domain logic in SQL cannot.
**Trade-off**: Fetches more data than strictly necessary. Acceptable because:
  - Memory is cheap, correctness is not
  - CLI context (not a server with many concurrent users)
  - Most operations are scoped to subsets anyway

## Invariants (Must Remain True)

### Architectural
1. **Domain predicates must be pure functions** — No database access, no I/O, no side effects
2. **One source of truth per concept** — Exactly ONE `matches_scope()`, ONE `is_excluded()`. No command implements its own
3. **Dependencies point inward** — Commands → Domain → nothing. Never import commands into domain
4. **SQL does data access only** — No business logic in SQL WHERE clauses
5. **Commands are orchestration** — Pattern: fetch → filter → transform → output

### Implementation
1. **Source struct fields match database** — Every field maps to column(s). No invented fields
2. **Batch size is 1000** — Consistent across all repos, defined once
3. **present=1 baked into repository** — Repository only returns present sources
4. **Path computation centralized** — `source.path()` is THE way to get full path
5. **ScopeMatch reused** — Use existing `ScopeMatch` from `scope.rs`, don't duplicate

### Correctness
1. **Scope edge case: /a/b vs /a/bc** — Path `/a/bc` is NOT under `/a/b`. Check for trailing slash
2. **Exclusion is TWO-level** — Source excluded directly OR via object. Check both (see note below)
3. **Command output unchanged** — Byte-identical output after migration
4. **Filter expressions still work** — `--where` produces same results
5. **Role includes suspended check** — "source role" means role=source AND not suspended

### Process
1. **Don't touch write paths** — `scan.rs`, `apply.rs`, `import_facts.rs` are OUT OF SCOPE
2. **cluster.rs deferred** — Needs fact integration, don't fully migrate
3. **One phase at a time** — Complete N before N+1. Compile + tests pass before proceeding
4. **Existing 52 tests must pass** — Always green
5. **Spec is the contract** — Update with learnings, don't deviate without discussion

## Critical Note: Object-Level Exclusion

Current SQL often does:
```sql
LEFT JOIN objects o ON s.object_id = o.id
WHERE ... AND (s.excluded = 0 AND (o.excluded IS NULL OR o.excluded = 0))
```

The Source struct includes `excluded` (source-level), but object-level exclusion requires knowing if the linked object is excluded. Options:

1. **Include `object_excluded: bool` in Source** — Denormalize, fetch via JOIN
2. **Separate object exclusion check** — After filtering, check objects separately
3. **Commands that need it fetch objects** — Only some commands care

**Decision**: Include `object_excluded: Option<bool>` in Source struct. Fetched via LEFT JOIN on objects. `None` if no object_id. Predicate becomes:
```rust
pub fn is_excluded(&self) -> bool {
    self.excluded || self.object_excluded.unwrap_or(false)
}
```

## Known Risks / Failure Modes

### Architectural
- **Scope creep into facts** — Resist urge to "just add fact fetching". Defer to future project
- **Predicate diverges from SQL** — Test edge cases thoroughly, especially scope matching
- **Over-abstraction** — Keep it boring. Functions, not frameworks

### Implementation
- **Missing field in Source** — Verify all commands' needs before finalizing struct
- **Memory blowup** — Monitor with real data. 1.59M sources should stay under 500MB
- **Order not preserved** — HashMap loses order. Document if output order matters per command

### Correctness
- **Exclusion logic incomplete** — Two-level check is critical
- **Filter interaction breaks** — `filter.rs` must see same source sets
- **Suspended roots leak** — Every command path must check `is_active()`

### Process
- **Context loss** — Spec must be comprehensive enough to survive compaction
- **Partial migration** — Each phase must fully complete or fully revert
- **Testing debt** — Add tests per phase, don't defer to end

## Output Order Requirements

| Command | Order Matters? | Current Order |
|---------|---------------|---------------|
| `ls` | Yes (sorted by path/size/mtime/name) | Explicit sort in code |
| `worklist` | No (JSONL, processed independently) | By source ID |
| `compare` | No (grouped by status) | By path within group |
| `coverage` | No (aggregated stats) | N/A |

**Note**: Batch fetch uses HashMap (unordered). Commands that need order must sort after filtering. This matches current behavior where sorting happens after fetch.

## Validation Commands

After each phase, run these commands and verify identical output:

```bash
# Basic listing
canon ls /path/to/test/dir > before.txt
# After migration:
canon ls /path/to/test/dir > after.txt
diff before.txt after.txt

# With filters
canon ls --where 'source.ext=jpg' /path > before_filter.txt

# Long format
canon ls -l /path > before_long.txt

# Worklist
canon worklist /path > before_worklist.jsonl

# Coverage
canon coverage > before_coverage.txt
```

Capture "before" outputs before starting Phase 3. Compare after each migration.

## Test Requirements

### Unit Tests (source.rs)
- All predicate methods tested in isolation
- Edge cases for scope matching (exact match, prefix match, similar prefixes)
- **Critical**: `/a/bc` not under `/a/b` test
- See Phase 1 for specific test list

### Integration Tests (source_repo.rs)
- Use in-memory SQLite with test data
- Verify correct data fetching
- Verify batch chunking works correctly
- See Phase 2 for specific test list

### Behavioral Tests (per-command)
- Capture current command output (see Validation Commands above)
- Verify identical output after migration
- Run before starting Phase 3, compare after each migration

## Learnings Log

*This section is updated as we progress through phases*

### Phase 1 Learnings

**What went well:**
- The `Source` struct design from the spec worked as-is. No fields needed to be added or removed.
- Reusing `ScopeMatch` from `scope.rs` was seamless — the domain layer integrates naturally.
- Tests were straightforward to write because predicates are pure functions with no setup required.

**Key decisions confirmed:**
- Including `root_path`, `root_role`, `root_suspended` in Source (denormalization) enables pure predicates. Without these, `matches_scope()` would need database access.
- The `make_source()` test helper with sensible defaults makes tests readable — each test only sets fields relevant to what it's testing.

**Edge cases documented through tests:**
- `/a/bc` not under `/a/b` — the similar-prefix edge case has explicit tests now
- Empty `rel_path` handling — source.path() correctly returns just root_path
- Two-level exclusion fully tested — 6 tests cover all combinations

**Dead code warning:**
- Some Source fields show as "never read" because they're not used yet. This is expected — Phase 2 (repository) and Phase 3+ (migrations) will use them. Warning will disappear as we progress.

**Test count:**
- Phase 1 added 22 tests
- Total project tests: 52 → 74

### Phase 2 Learnings
- (To be filled in)

## References

- Prior refactoring: `.claude/specs/2026-01-21-db-refactoring.md`
- Existing domain layer: `scope.rs`, `root.rs`
- N+1 fix example: `ls.rs:batch_fetch_sources()`
