# Story: import_facts.rs SQL Extraction

**Epic**: [Command Module Architectural Compliance](epic-command-architecture.md)
**Status**: Completed
**Created**: 2026-02-08
**Completed**: 2026-02-08

## Objective

Extract all inline SQL from `import_facts.rs` into repo layer functions, bringing the module into full architectural compliance. Fix atomicity issues discovered during analysis.

## Current State

**import_facts.rs** has 8 SQL operations across 4 functions:

| Function | SQL Calls | Issue |
|----------|-----------|-------|
| `build_fact_type_map()` | 1 | Inline SELECT for fact types |
| `process_import()` | 2 | Source lookup + UPDATE for object linking |
| `get_or_create_object()` | 2 | SELECT + INSERT with TOCTOU race |
| `insert_fact()` | 1 | Inline INSERT ON CONFLICT |
| `promote_content_facts()` | 4 | SELECT + existence check + INSERT + DELETE |

**What's already good:**
- Pure parsing logic: `TypedValue`, `classify_typed_value()`, `classify_value()`
- Pure key normalization: `normalize_fact_key()`
- Type consistency checking logic is pure
- Clear separation between parsing and persistence

**Correctness issues to fix:**
1. `get_or_create_object()` has TOCTOU race condition
2. Link source to object + promote facts is not atomic

## Design

### New Repo Functions

```rust
// repo/source.rs

/// Fetch a single source by ID with full details including root role.
/// Returns None if source doesn't exist.
pub fn fetch_by_id(conn: &Connection, source_id: i64) -> Result<Option<Source>>;

/// Link a source to an object (set object_id).
pub fn link_to_object(conn: &Connection, source_id: i64, object_id: i64) -> Result<()>;
```

```rust
// repo/object.rs

/// Get or create an object by hash. Uses INSERT ON CONFLICT for atomicity.
/// Returns the Object (existing or newly created).
pub fn get_or_create(conn: &Connection, hash_type: &str, hash_value: &str) -> Result<Object>;
```

```rust
// repo/fact.rs

/// Fetch the type map for all existing facts.
/// Returns HashMap from key to FactValueType (Text, Num, Time).
pub fn fetch_type_map(conn: &Connection) -> Result<HashMap<String, FactValueType>>;

/// Upsert a fact (insert or update on conflict).
/// Works for both source and object facts.
pub fn upsert(
    conn: &Connection,
    entity_type: &str,
    entity_id: i64,
    key: &str,
    value_text: Option<&str>,
    value_num: Option<f64>,
    value_time: Option<i64>,
    observed_at: i64,
    observed_basis_rev: Option<i64>,
) -> Result<()>;

/// Fetch all facts for a source (entity_type = 'source').
/// Returns Vec of fact tuples for promotion processing.
pub fn fetch_source_facts(conn: &Connection, source_id: i64) -> Result<Vec<SourceFact>>;

/// Check if an object has a specific fact key.
pub fn object_has_fact(conn: &Connection, object_id: i64, key: &str) -> Result<bool>;

/// Delete a fact by ID.
pub fn delete_by_id(conn: &Connection, fact_id: i64) -> Result<()>;
```

### Domain Layer Additions

```rust
// domain/fact.rs

/// Fact value type for type consistency checking.
/// Moved from import_facts.rs for reuse.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FactValueType {
    Text,
    Num,
    Time,
}

/// A source fact record for promotion processing.
pub struct SourceFact {
    pub id: i64,
    pub key: String,
    pub value_text: Option<String>,
    pub value_num: Option<f64>,
    pub value_time: Option<i64>,
    pub observed_at: i64,
}

/// Normalize a fact key to use the content.* namespace.
/// - Keys starting with "source." are rejected (reserved namespace)
/// - Keys already starting with "content." are left as-is
/// - All other keys are prefixed with "content."
pub fn normalize_fact_key(key: &str) -> Result<String, &'static str>;

/// Check if a key is a content fact (starts with "content.").
pub fn is_content_fact(key: &str) -> bool;
```

### Atomicity Fixes

**get_or_create_object race condition:**

Before (TOCTOU vulnerable):
```rust
let existing = conn.query_row("SELECT id FROM objects WHERE ...")?;
if existing.is_none() {
    conn.execute("INSERT INTO objects ...")?;
}
```

After (atomic):
```rust
conn.execute(
    "INSERT INTO objects (hash_type, hash_value) VALUES (?, ?)
     ON CONFLICT(hash_type, hash_value) DO UPDATE SET hash_type = hash_type",
    params![hash_type, hash_value],
)?;
conn.query_row(
    "SELECT id, hash_type, hash_value, excluded FROM objects
     WHERE hash_type = ? AND hash_value = ?",
    params![hash_type, hash_value],
    // ... map to Object
)?
```

**Link + Promote atomicity:**

The command layer wraps the link and promote operations in a transaction:
```rust
// In process_import(), when linking source to new object:
if object_id.is_some() && current_object_id.is_none() {
    let tx = conn.transaction()?;
    repo::source::link_to_object(&tx, source_id, object_id.unwrap())?;
    let promoted = promote_content_facts(&tx, source_id, object_id.unwrap())?;
    tx.commit()?;
    stats.facts_promoted += promoted;
}
```

Note: `promote_content_facts` remains a helper function in the command module that orchestrates repo calls. It's not a single repo function because the logic (iterate facts, check existence, copy, delete) is orchestration, not pure database access.

## Decisions

| Decision | Rationale |
|----------|-----------|
| Add `repo::source::fetch_by_id()` | Zero SQL in command layer; `Source` already has all needed fields |
| Use `INSERT ON CONFLICT` for get_or_create | Fixes race condition; atomic pattern |
| Transaction for link + promote | Ensures consistency; fixes atomicity bug |
| Move `FactValueType` to domain | Pure enum, reusable across modules |
| Move `normalize_fact_key` to domain | Pure function, enables unit testing |
| Keep `promote_content_facts` as command helper | It orchestrates repo calls, not a single DB operation |
| Individual fact upserts remain non-transactional | Intentional: one bad fact shouldn't block others |

## Non-Goals

- Changing import behavior or output format
- Adding new CLI flags
- Transaction around entire source processing (intentional per-source isolation)
- Moving datetime/duration parsing to domain (tightly coupled to import)
- Performance optimization
- Refactoring `process_import()` structure (future pass)

## Test Plan

### Existing Tests

**None** — `import_facts.rs` has zero unit tests. Relies on integration testing through CLI.

### New Tests to Add

**domain/fact.rs:**
1. `test_normalize_fact_key_adds_content_prefix` — bare key gets `content.` prefix
2. `test_normalize_fact_key_preserves_content_prefix` — already-prefixed key unchanged
3. `test_normalize_fact_key_rejects_source_namespace` — `source.*` keys rejected
4. `test_is_content_fact` — correctly identifies content.* keys

**repo/source.rs:**
1. `test_fetch_by_id_exists` — returns Source with all fields
2. `test_fetch_by_id_not_found` — returns None
3. `test_link_to_object` — updates source.object_id

**repo/object.rs:**
1. `test_get_or_create_creates_new` — returns new Object when hash not found
2. `test_get_or_create_returns_existing` — returns existing Object when hash exists
3. `test_get_or_create_concurrent_safe` — (optional) verify no duplicate on conflict

**repo/fact.rs:**
1. `test_fetch_type_map_empty` — empty DB returns empty map
2. `test_fetch_type_map_detects_types` — correctly identifies Text/Num/Time
3. `test_upsert_inserts` — new fact is inserted
4. `test_upsert_updates` — existing fact is updated
5. `test_fetch_source_facts` — returns all facts for source
6. `test_object_has_fact_true` — returns true when fact exists
7. `test_object_has_fact_false` — returns false when fact missing
8. `test_delete_by_id` — fact is deleted

## Implementation Checklist

- [x] Add `FactValueType` enum to `domain/fact.rs`
- [x] Add `SourceFact` struct to `domain/fact.rs`
- [x] Move `normalize_fact_key()` to `domain/fact.rs` with tests
- [x] Move `is_content_fact()` to `domain/fact.rs`
- [x] Add `repo::source::fetch_by_id()` with tests
- [x] Use existing `repo::source::set_object_id()` for linking (already existed)
- [x] Fix `repo::object::get_or_create()` with atomic INSERT ON CONFLICT
- [x] Add `repo::fact::fetch_type_map()` with tests
- [x] Add `repo::fact::upsert()` with tests
- [x] Add `repo::fact::fetch_source_facts()` with tests
- [x] Add `repo::fact::object_has_fact()` with tests
- [x] Add `repo::fact::delete_by_id()` with tests
- [x] Add `repo::fact::insert_object_fact()` for promotion
- [x] Refactor `build_fact_type_map()` to use `repo::fact::fetch_type_map()`
- [x] Refactor `process_import()` to use `repo::source::fetch_by_id()`
- [x] Refactor `process_import()` to use `repo::source::set_object_id()`
- [x] Refactor `process_import()` to use `repo::object::get_or_create()`
- [x] Refactor fact insertion to use `repo::fact::upsert()`
- [x] Refactor `promote_content_facts()` to use repo functions
- [x] Add transaction around link + promote in `process_import()`
- [x] Remove inline SQL from import_facts.rs
- [x] Verify all 417 tests pass
- [x] Update epic spec with completion status

## Backward Compatibility

Command output must remain identical:
- Import statistics format unchanged
- Warning messages unchanged
- Type mismatch error handling unchanged
- Staleness detection unchanged

## Dependencies

This story establishes patterns needed by **facts.rs**:
- `repo::fact::upsert()` pattern
- `repo::fact::fetch_type_map()` for type handling
- `repo::object::get_or_create()` for hash-based object lookup
