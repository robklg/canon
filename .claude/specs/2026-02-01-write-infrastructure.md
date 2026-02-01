# Spec: Write Infrastructure Domain Model

**Date:** 2026-02-01
**Status:** In Progress

## Problem Statement

The unified domain model project successfully established patterns for read operations (domain structs, repo batch fetching, predicate filtering). However, write operations (~40 inline SQL statements) remain embedded in command modules.

Recent bugs in `apply.rs` (`5b9f062`) revealed the fragility of this approach:
- Missing `partial_hash` in INSERT caused NOT NULL constraint failures
- Incorrect UPDATE vs INSERT logic for stale records
- Edge cases in empty table handling (COALESCE fix)

These issues would have been caught by unit tests if the write logic lived in the repo layer.

## Goal

Extend the repository layer to include write operations, starting with `apply.rs` as the proving ground. Establish patterns that subsequent modules (scan.rs, exclude.rs, facts.rs) can follow.

---

## Architectural Decisions

These decisions guide this spec and inform future refactoring work.

### D1: Repository layer owns SQL complexity for writes

**Decision:** Write operations go in `repo/` modules, not command modules.

**Rationale:** Same as for reads — centralizes SQL, enables testing, handles edge cases once.

**Implications for future work:**
- scan.rs INSERT/UPDATE statements should migrate to `repo::source`
- exclude.rs UPDATE statements should migrate to `repo::source` and `repo::object`
- facts.rs INSERT/DELETE should migrate to `repo::fact`

### D2: Repository functions do not own transactions

**Decision:** Repo functions execute single statements or statement groups. Transaction boundaries (BEGIN/COMMIT) are managed by command modules.

**Rationale:**
- Keeps repo functions composable (can combine multiple operations in one transaction)
- Command modules know the full scope of work
- Simpler testing (no transaction mocking)

**Implications for future work:**
- `apply.rs` wraps file copy + `insert_destination()` in transaction
- `scan.rs` can batch multiple inserts in single transaction
- No `repo::with_transaction()` helper needed

### D3: Hash values vs object IDs in manifest workflows

**Decision:** Manifest workflows (cluster, apply) use `hash_value` as the content identifier because manifests are portable snapshots that don't depend on database IDs.

**Rationale:** `LockEntry.hash_value` is the manifest's content key. Converting to `object_id` just for repo function calls adds complexity.

**Implications for future work:**
- Archive conflict checking should support lookup by hash (not just object_id)
- Consider `batch_find_archive_paths_by_hash()` when migrating conflict checks
- Two approaches exist: (A) hash-native function, (B) hash→object_id→paths. Decision deferred to Phase B.

### D4: Domain types for write inputs

**Decision:** Write operations accept domain-defined input types (e.g., `NewSource`) and return domain types (e.g., `Source`). This mirrors how read operations return domain types.

**Rationale:**
- **Interface stability**: If backend changes, repo function signatures stay the same — only implementations change. Command modules are insulated.
- **Symmetry**: Reads return domain types, writes accept and return domain types. Same vocabulary throughout.
- **Domain awareness**: The domain layer defines what valid inputs look like.
- **Validation opportunity**: Domain constructors can validate invariants before data reaches repo.

**Pattern:**
```rust
// domain/source.rs — input type alongside Source
pub struct NewSource {
    pub root_id: i64,
    pub rel_path: String,
    pub size: i64,
    pub mtime: i64,
    pub partial_hash: String,
    pub object_id: i64,
    pub device: Option<u64>,
    pub inode: Option<u64>,
}

// repo/source.rs — accepts domain type, returns domain type
pub fn insert_destination(conn: &Connection, new: &NewSource) -> Result<Source>
```

**Implications for future work:**
- `NewSource` lives in `domain/source.rs` alongside `Source`
- Similar pattern for `NewFact`, `NewObject` when those migrations happen
- Domain layer can add validation methods (e.g., `NewSource::validate()`)

### D5: Upsert logic lives in repo, not command

**Decision:** When a write operation has INSERT vs UPDATE semantics (like destination registration), the repo function handles the distinction internally.

**Rationale:**
- Encapsulates the complexity
- Command module doesn't need to query first to decide
- Single "do the right thing" interface

**Implications for future work:**
- `repo::source::insert_destination()` handles fresh vs stale internally
- Returns the resulting `Source` regardless of which path was taken
- Similar pattern for `repo::fact::upsert_fact()`

### D6: SQLite coupling is acceptable, but design for stability

**Decision:** The repo layer uses rusqlite directly (no backend abstraction trait). However, function signatures should use domain types so that if abstraction is ever needed, command modules don't change.

**Rationale:**
- No concrete need for non-SQLite backend
- Adding traits now would slow down work without clear benefit
- The repo layer is the right place to introduce abstraction later if needed
- Domain-typed signatures (D4) provide interface stability

**What would change for a different backend:**
1. Define traits in `repo/` matching current function signatures
2. Rename current implementations to `SqliteXxxRepo`
3. Command modules unchanged — they work with domain types

### D7: Document non-obvious behavior in repo functions

**Decision:** Repo functions must have clear documentation describing their behavioral contract, especially non-obvious behavior like upsert semantics, edge cases, and return value guarantees.

**Rationale:**
- If backend changes, documentation serves as the specification for reimplementation
- Makes expected behavior explicit for testing
- Reduces "what does this actually do?" questions

**Documentation should cover:**
- What the function does (obvious)
- Upsert/conflict behavior (does it INSERT, UPDATE, or both?)
- Edge cases handled (empty inputs, missing records, constraint violations)
- Return value semantics (what state is the returned domain object in?)
- What is NOT handled (caller responsibilities)

**Example:**
```rust
/// Insert a new source record for a destination file in an archive.
///
/// # Behavior
/// - If no record exists for (root_id, rel_path): INSERT with basis_rev=0
/// - If a stale record exists (present=0): UPDATE, increment basis_rev, set present=1
/// - If an active record exists (present=1): Returns error (pre-flight should prevent this)
///
/// # Returns
/// The complete Source record as it exists in the database after the operation,
/// including joined fields (root_path, root_role, object_excluded).
///
/// # Caller Responsibilities
/// - Ensure the file has been successfully written before calling
/// - Manage transaction boundaries
pub fn insert_destination(conn: &Connection, new: &NewSource) -> Result<Source>
```

### D8: Write functions return complete domain objects via SELECT

**Decision:** After INSERT or UPDATE, repo write functions SELECT the complete record to return a fully-populated domain object.

**Rationale:**
- Guarantees returned object matches database state
- Handles auto-generated fields (id, timestamps) and joined fields (root_path, object_excluded)
- Correctness over micro-optimization — the extra SELECT is negligible for write operations

**Implications:**
- Write functions may reuse existing fetch helpers internally
- Tests can assert on returned object completeness

### D9: Use established repo batch functions — no command-local caching types

**Decision:** Command modules use existing `repo::` batch fetch functions directly and pass their return types through. Do not introduce command-local type aliases or wrapper functions for caching.

**Rationale:**
- The repo layer already provides well-tested batch fetch functions (`batch_fetch_for_sources`, `batch_fetch_by_roots`, etc.)
- Command-local caching types (e.g., `type FactCache = HashMap<...>`) duplicate infrastructure concerns
- Command-local wrapper functions that transform repo results add indirection without value
- The established pattern (see `cluster.rs`) fetches once and threads the result through

**Established pattern:**
```rust
// DO: Use repo function directly, pass result through
let all_facts = repo::fact::batch_fetch_for_sources(conn, &source_ids)?;
// ... pass all_facts to functions that need it

// DO: Combine multiple repo calls into a single map (when fetching per-key)
let mut all_facts: HashMap<i64, Vec<FactEntry>> = HashMap::new();
for key in &needed_keys {
    let key_facts = repo::fact::batch_fetch_key_for_sources(conn, &source_ids, key)?;
    for (source_id, entry_opt) in key_facts {
        if let Some(entry) = entry_opt {
            all_facts.entry(source_id).or_default().push(entry);
        }
    }
}

// DON'T: Create command-local type aliases or wrapper functions
type FactCache = HashMap<i64, HashMap<String, FactValue>>;  // Avoid
fn batch_fetch_facts(...) -> FactCache { ... }              // Avoid
```

**Implications:**
- Functions accept `&HashMap<i64, Vec<FactEntry>>` (matching repo return structure)
- Combining repo results inline is fine; creating abstractions around it is not
- Lookups happen at point of use, not via intermediate cache structure

### D10: Defer structural refactors that eliminate redundant computation

**Decision:** When batch fetching eliminates N+1 queries but redundant in-memory computation remains (e.g., pattern evaluation happening 4x per source), defer structural refactors unless profiling shows a bottleneck.

**Rationale:**
- N+1 database queries are the real performance problem (I/O bound)
- Redundant CPU work (string operations, HashMap lookups) is typically negligible
- Structural refactors (e.g., pre-computing all destination paths upfront) are higher risk
- Can revisit when improving separation of concerns in command modules

**Example deferred refactor:**
`apply.rs` evaluates patterns 4 times per source (validate, collision check, stale check, apply). Pre-computing paths once would be cleaner but requires larger structural change. Acceptable to defer.

---

## Validation Responsibilities

| Layer | Validates |
|-------|-----------|
| **Domain** | Structural validity (required fields, value constraints, invariants) |
| **Repo** | Database constraints (uniqueness, foreign keys, type coercion) |
| **Command** | Business rules (e.g., "don't overwrite present=1 without flag") |

---

## Current Focus: apply.rs Migration

### Phase A: Write Operation Extraction

**Status:** completed

**Goal:** Extract `register_destination()` to repo layer with comprehensive tests.

**Scope:**
- Add `NewSource` struct to `domain/source.rs`
- Create `repo::source::insert_destination(conn, &NewSource) -> Result<Source>`
- Handle fresh INSERT vs stale UPDATE logic internally (per D5)
- Add unit tests covering:
  - Fresh insert (all fields, basis_rev=0, returns valid Source)
  - Stale record update (present=0 → present=1, basis_rev incremented)
  - NULL device/inode handling
  - Duplicate detection (present=1 already exists)
- Update `apply.rs` to construct `NewSource` and call repo function
- Remove old `register_destination()` functions from apply.rs

**Non-goals:**
- Other write operations (`mark_source_not_present`)
- Read operation migration (Phase B)
- Performance optimization

### Phase B: Read Batch Migration

**Status:** in progress

**Goal:** Eliminate N+1 query patterns using existing repo functions.

**Scope:**
- Root path lookups: Replace inline SQL with `repo::root::fetch_all()`
- Batch fact fetching: Replace per-source `fetch_typed_fact()` with per-key batch fetching
  - Use `repo::fact::batch_fetch_key_for_sources()` once per pattern key
  - Fetch only the facts needed by the manifest pattern (data minimization)
  - Combine results into `HashMap<i64, Vec<FactEntry>>` keyed by source_id
  - Thread combined map through functions (per D9 — no command-local wrapper types)
  - K queries for K pattern keys is acceptable (K is typically 2-5)
- Change `apply::run` signature to `&mut Db` (consistent with `cluster::generate`)
- Batch archive conflict checking (approach per D3 — decision deferred)

**Non-goals:**
- Suspended root checks (acceptable N+1 for validation)
- Changing `LockEntry` structure or manifest format
- Eliminating redundant pattern evaluation (per D10 — deferred)

**Dependencies:** Phase A complete (establishes the working pattern)

---

## Inventory: Write Operations Across Codebase

For reference when planning future phases. Not in scope for this spec.

| Module | Write Operations | Count | Priority |
|--------|-----------------|-------|----------|
| `apply.rs` | `register_destination`, `mark_source_not_present` | 6 | **High** — recent bugs |
| `scan.rs` | Source/object/fact inserts and updates | 12 | High — core functionality |
| `exclude.rs` | Source/object exclusion toggles | 11 | Medium |
| `facts.rs` | Fact deletion, prune operations | 10 | Medium |
| `roots.rs` | Root removal with cascading | 6 | Low — infrequent |
| `import_facts.rs` | Fact insertion with type validation | 6 | Low — specialized |

---

## Test Requirements

### Phase A Tests

| Test Case | Description |
|-----------|-------------|
| `insert_destination_fresh` | All fields populated, basis_rev=0, returns valid Source |
| `insert_destination_stale_update` | present=0 exists → UPDATE, basis_rev incremented, returns Source |
| `insert_destination_null_device_inode` | Handles None values correctly |
| `insert_destination_already_present` | present=1 exists → defined error behavior |
| `insert_destination_returns_complete_source` | Returned Source has all fields including joined (root_path, object_excluded) |

### Phase B Tests

| Test Case | Description |
|-----------|-------------|
| `batch_fact_fetch_matches_single` | Batch result equals N single fetches |
| `batch_fact_fetch_large` | >1000 sources (chunking boundary) |
| `archive_conflict_batch` | Multiple sources, dest vs other archives |

---

## Related Documents

- `.refactor-analysis-apply.md` — Initial apply.rs analysis (N+1 patterns)
- `.claude/specs/2026-01-25-object-infrastructure.md` — Prior domain model spec (read side)
- `CLAUDE.md` — Architecture documentation (update when complete)
