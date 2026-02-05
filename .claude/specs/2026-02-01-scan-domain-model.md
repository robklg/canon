# Spec: Scan Domain Model

**Date:** 2026-02-01
**Status:** Complete

## Problem Statement

The `scan.rs` module has ~12 inline SQL statements interspersed with business logic. The decision logic (is this file new? modified? moved?) is tangled with persistence code, making it:

- **Untestable**: No unit tests exist for scan.rs (0 tests found)
- **Hard to reason about**: Business rules are implicit in SQL/code structure
- **Fragile**: Similar to bugs found in apply.rs (missing fields, incorrect UPDATE vs INSERT)

The write-infrastructure spec established patterns for apply.rs. This spec extends those patterns to scan.rs with a proper domain model.

## Goal

Extract scan's business logic into pure, testable domain functions. Establish a pipeline architecture where:

1. **Observation** captures what we see on disk (data)
2. **Reconciliation** determines what it means (pure logic)
3. **Persistence** executes the outcome (repo layer)

---

## Architectural Decisions

### D1: Pipeline architecture over batch processing

**Decision:** Process files in a streaming pipeline rather than batching.

**Rationale:**
- File I/O (especially hashing) dominates execution time; DB queries are negligible
- Pipeline model is simpler: observe → reconcile → persist → next
- No buffer management, batch boundaries, or accumulator state
- Each stage is independently testable

**Implications:**
- No accumulation of FileObservations before processing
- Reconciliation happens immediately after observation
- Stats accumulate incrementally during walk

### D2: Per-file transactions for concurrency safety

**Decision:** Each file's observe-reconcile-persist cycle is wrapped in its own transaction. Queries fetch fresh state within the transaction.

**Rationale:**
- Eliminates stale data risk from pre-loaded snapshots
- Crash recovery: completed files are committed, no partial state
- Simpler reasoning: each file operation is atomic and independent
- Enables safe concurrent operations (even if not currently expected)

**Implications:**
- No pre-loading of full Source records for lookup
- Two queries per file (by path, by inode) within transaction
- Query cost is negligible compared to file I/O
- Lookup maps are not needed (no stale map maintenance)

### D3: Pre-fetch IDs only for missing detection

**Decision:** At scan start, fetch only the set of source IDs for the root (not full Source records). Track seen IDs during walk. Diff at end to find missing.

**Rationale:**
- IDs are small (just integers), minimal memory overhead
- Enables pure `find_missing()` function
- Single `mark_missing()` call at end (efficient)
- Fresh queries per-file handle the rest

**Implications:**
- `fetch_source_ids_by_roots()` called once at start
- `seen_ids: HashSet<i64>` accumulated during walk
- `find_missing(&expected, &seen) -> Vec<i64>` is pure

### D4: Pure domain functions encode business rules

**Decision:** The rules for interpreting file observations live in pure domain functions that take data in and return decisions out. No I/O, no side effects.

**Rationale:**
- Pure functions are trivially testable
- Business rules are explicit and documented
- Changes to rules are localized
- Enables property-based testing

**Functions:**
- `reconcile()` - given observation and existing state, determine action
- `find_missing()` - given expected and seen IDs, return missing IDs

### D5: Reconciliation captures what, not how

**Decision:** The `Reconciliation` enum describes what happened (New, Modified, Moved, Unchanged), not how to persist it. The repo layer translates reconciliation into SQL.

**Rationale:**
- Separation of concerns: domain describes meaning, repo handles storage
- Same reconciliation could map to different SQL in different contexts
- Easier to test: verify reconciliation without database

**Implications:**
- `Reconciliation::Modified` doesn't contain UPDATE SQL
- Repo function `apply_reconciliation()` interprets the enum
- Tests verify reconciliation outcomes, not SQL generation

### D6: Computed fields are determined by reconciliation, executed by command

**Decision:** Reconciliation describes what happened (New, Modified, etc.). The command layer decides what computed fields to generate based on:
1. Reconciliation type (what's required for persistence?)
2. User configuration (what optional features are enabled?)

**Rationale:**
- Domain describes "what happened" — pure business logic
- Command layer applies policy (user flags, configuration)
- Keeps domain free of configuration concerns
- Some computed fields are infrastructure requirements (needed for DB)
- Others are user-controlled features

**Current application to hashing:**

| Reconciliation | Partial Hash | Full Hash |
|----------------|--------------|-----------|
| New | Required (for INSERT) | If `--hash` enabled |
| Modified | Required (for UPDATE) | If `--hash` enabled |
| Moved | Not needed | Not needed |
| Unchanged | Not needed | Not needed |

**Domain helper (in `domain/scan.rs`):**
```rust
impl Reconciliation {
    /// Whether this reconciliation requires partial hash computation.
    /// This is an infrastructure requirement, not a policy decision.
    pub fn needs_partial_hash(&self) -> bool {
        matches!(self, Reconciliation::New | Reconciliation::Modified { .. })
    }
}
```

**Command layer applies configuration:**
```rust
if reconciliation.needs_partial_hash() {
    observation.partial_hash = Some(compute_partial_hash(&path)?);
}
if hash_enabled && reconciliation.needs_partial_hash() {
    // Queue for full hashing after scan
}
```

### D7: Repo functions mirror existing patterns

**Decision:** Follow the patterns established in write-infrastructure spec (D4, D5, D7, D8):
- Accept domain input types
- Return complete domain objects via SELECT after write
- Document behavioral contracts
- Handle upsert logic internally

### D8: Disconnected storage detection is for missing files only

**Decision:** Disconnected storage detection applies ONLY to files that are **not found** during the walk (specifically, empty directories where files should exist). It does NOT apply to files that ARE present on disk.

**Rationale:**
- If a file IS present, it should be processed normally — device ID changes are just metadata updates
- Empty directories could be disconnected mountpoints (NAS offline, USB unplugged, etc.)
- Before assuming sources under an empty path are gone, check the directory's device ID against stored source device IDs
- If devices differ, we have reasonable certainty the storage is disconnected — protect user from accidentally marking sources as missing
- Common scenario: `canon scan --all` when not all storage is connected

**Behavior:**
- File IS present on disk → always process it (device ID may change, that's fine)
- File NOT found, directory device matches stored device → mark as missing (truly gone)
- File NOT found, directory device differs from stored device → skip as Disconnected (storage offline)
- `--ignore-device-id` flag → treat all missing files as truly missing (trust the filesystem)

**Implementation:**
- `classify_sources_in_empty_dir()` handles the device check for empty directories
- `SourceOutcome::Disconnected` is set there, handled later in `mark_missing_sources()`
- The `reconcile()` function does NOT check device IDs — it only processes files that exist
- `Reconciliation` enum does NOT need a `Disconnected` variant (removed)

**Command layer handling:**
- `SourceOutcome::Disconnected` → skip marking as missing, log warning
- `--ignore-device-id` → treat Disconnected as Missing
- Stats track disconnected count separately

---

## Domain Model

### FileObservation

What scan observes about a single file on disk.

```rust
// domain/scan.rs

/// What scan observes about a file on disk.
///
/// This is pure data captured from filesystem metadata.
/// The partial_hash field is computed lazily only when needed
/// (new files, or files with changed size/mtime).
pub struct FileObservation {
    pub root_id: i64,
    pub rel_path: String,
    pub device: u64,
    pub inode: u64,
    pub size: i64,
    pub mtime: i64,
    pub partial_hash: Option<String>,
}
```

### Reconciliation

The outcome of comparing an observation to known database state.

```rust
/// The outcome of reconciling a file observation with database state.
///
/// This enum describes what happened semantically, not how to persist it.
/// The repo layer translates these outcomes into appropriate SQL operations.
///
/// NOTE: This enum does NOT include a Disconnected variant. Disconnected storage
/// detection happens at the command layer via `classify_sources_in_empty_dir()`,
/// which checks empty directories where files should exist. The reconcile()
/// function only processes files that ARE present on disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reconciliation {
    /// File is new - no existing source at this path or with this inode.
    /// Requires: partial_hash must be computed.
    /// Action: INSERT new source record.
    New,

    /// File exists and is unchanged - same path, same size+mtime.
    /// Action: UPDATE last_seen_at only.
    Unchanged {
        source_id: i64,
    },

    /// File exists but content may have changed - same path, different size or mtime.
    /// Requires: partial_hash must be computed.
    /// Action: UPDATE with new size, mtime, partial_hash, increment basis_rev.
    Modified {
        source_id: i64,
        old_object_id: Option<i64>,  // For detecting unexpected hash changes later
    },

    /// File was moved - different path, but same device+inode.
    /// Action: UPDATE path (and possibly root_id for cross-root moves).
    Moved {
        source_id: i64,
        from_root_id: i64,
        from_path: String,
        old_object_id: Option<i64>,
    },
}

impl Reconciliation {
    /// Whether this reconciliation requires partial hash computation.
    /// This is an infrastructure requirement for persisting the source record.
    pub fn needs_partial_hash(&self) -> bool {
        matches!(self, Reconciliation::New | Reconciliation::Modified { .. })
    }

    /// The source ID affected by this reconciliation, if any.
    /// New files don't have a source ID yet.
    pub fn source_id(&self) -> Option<i64> {
        match self {
            Reconciliation::New => None,
            Reconciliation::Unchanged { source_id }
            | Reconciliation::Modified { source_id, .. }
            | Reconciliation::Moved { source_id, .. } => Some(*source_id),
        }
    }
}
```

### reconcile() - Pure Reconciliation Function

```rust
/// Determine the reconciliation outcome for a file observation.
///
/// This function ONLY processes files that exist on disk. It does NOT perform
/// device ID checking — that happens at the command layer for empty directories
/// via `classify_sources_in_empty_dir()`.
///
/// # Arguments
/// - `observation`: What we observed on disk (the file EXISTS)
/// - `source_at_path`: Existing source at this (root_id, rel_path), if any
/// - `source_by_inode`: Existing source with this (device, inode), if any
///
/// # Behavior
/// The decision tree:
/// 1. If source exists at path:
///    a. If same inode (or inode not tracked): check size+mtime
///       - Match: Unchanged
///       - Differ: Modified
///    b. If different inode: New (replacement, old file handled by mark_missing)
/// 2. Else if source exists with same inode (different path): Moved
/// 3. Else: New
///
/// # Note on "Replaced" case
/// If a source exists at the path but with different inode, this means:
/// - Old file at this path was deleted
/// - New file was created at same path
/// This is handled as: New (for the new file) + Missing (old file detected at end of scan)
/// We don't need a Replaced variant because mark_missing handles the old file.
///
/// # Note on device ID changes
/// If a file is present on disk, it is always processed — even if its device ID
/// differs from what was stored. Device ID changes are legitimate (e.g., NAS remount,
/// drive replacement). The device ID will be updated in the source record.
pub fn reconcile(
    observation: &FileObservation,
    source_at_path: Option<&Source>,
    source_by_inode: Option<&Source>,
) -> Reconciliation {
    // Source exists at this path?
    if let Some(existing) = source_at_path {
        // Check if this is actually the same file (by inode) or a replacement
        let same_inode = existing.inode == Some(observation.inode as i64);
        let inode_not_tracked = existing.inode.is_none();

        if same_inode || inode_not_tracked {
            // Same file (or inode not tracked) - check for modifications
            let fingerprint_matches = existing.size == observation.size
                && existing.mtime == observation.mtime;

            if fingerprint_matches {
                Reconciliation::Unchanged { source_id: existing.id }
            } else {
                Reconciliation::Modified {
                    source_id: existing.id,
                    old_object_id: existing.object_id,
                }
            }
        } else {
            // Different inode at same path = replacement
            // The old file will be caught by mark_missing
            // This observation is treated as New
            Reconciliation::New
        }
    } else if let Some(existing) = source_by_inode {
        // No source at path, but found one with same inode = moved
        Reconciliation::Moved {
            source_id: existing.id,
            from_root_id: existing.root_id,
            from_path: existing.rel_path.clone(),
            old_object_id: existing.object_id,
        }
    } else {
        // No existing source at path or by inode = new file
        Reconciliation::New
    }
}
```

### find_missing() - Pure Missing Detection

```rust
/// Identify source IDs that were expected but not seen during the walk.
///
/// # Arguments
/// - `expected_ids`: Source IDs that existed at start of scan (present=1)
/// - `seen_ids`: Source IDs we encountered during the walk
///
/// # Returns
/// Source IDs that should be marked as missing (present=0).
///
/// # Note
/// Sources that were moved will be in seen_ids (we saw them at their new path),
/// so they won't appear in the missing list.
pub fn find_missing(expected_ids: &HashSet<i64>, seen_ids: &HashSet<i64>) -> Vec<i64> {
    expected_ids.difference(seen_ids).copied().collect()
}
```

---

## Repository Interface

### Existing Functions (reuse)

- `repo::source::fetch_source_ids_by_roots(conn, root_ids)` - Get IDs for missing detection
- `repo::source::batch_fetch_by_ids(conn, ids)` - Fetch full Source records if needed

### New Functions

#### fetch_by_path

```rust
/// Fetch the source at a specific path within a root.
///
/// # Returns
/// The Source if one exists at (root_id, rel_path) with present=1, None otherwise.
pub fn fetch_by_path(conn: &Connection, root_id: i64, rel_path: &str) -> Result<Option<Source>>
```

#### fetch_by_inode

```rust
/// Fetch a source by its device and inode.
///
/// # Behavior
/// - Searches across ALL roots (moves can cross roots)
/// - Only returns present=1 sources
/// - Returns None if device or inode is not tracked for any source
///
/// # Returns
/// The Source if one exists with matching (device, inode), None otherwise.
pub fn fetch_by_inode(conn: &Connection, device: u64, inode: u64) -> Result<Option<Source>>
```

#### apply_reconciliation

```rust
/// Apply a reconciliation outcome to the database.
///
/// # Behavior by Reconciliation variant:
/// - New: INSERT source with basis_rev=0, scanned_at=now, present=1
///        (or UPDATE if stale record exists at path)
/// - Unchanged: UPDATE device, inode, last_seen_at=now
///              (device/inode may change legitimately, e.g., NAS remount)
/// - Modified: UPDATE size, mtime, partial_hash, device, inode, basis_rev+1, last_seen_at=now
/// - Moved: UPDATE root_id, rel_path, device, inode, size, mtime, last_seen_at=now
///
/// # Returns
/// The complete Source record after the operation (via SELECT).
///
/// # Caller Responsibilities
/// - Ensure partial_hash is computed for New and Modified reconciliations
/// - Wrap in transaction for atomicity
pub fn apply_reconciliation(
    conn: &Connection,
    observation: &FileObservation,
    reconciliation: &Reconciliation,
    now: i64,
) -> Result<Source>
```

#### mark_missing

```rust
/// Mark sources as no longer present.
///
/// # Behavior
/// - Sets present=0 for all specified source IDs
/// - Sets last_seen_at to the provided timestamp
/// - Does NOT delete the records (preserves history)
///
/// # Returns
/// Count of sources marked as missing.
pub fn mark_missing(conn: &Connection, source_ids: &[i64], now: i64) -> Result<u64>
```

---

## Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                           scan_root()                               │
│                                                                     │
│  ┌──────────────┐                                                   │
│  │ fetch IDs    │  (one query - just IDs for missing detection)     │
│  │ for root     │                                                   │
│  └──────┬───────┘                                                   │
│         │                                                           │
│         ▼          expected_ids: HashSet<i64>                       │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │  for each file in walk:                         [per-file tx] │  │
│  │                                                               │  │
│  │    ┌──────────────────────────────────────────────────────┐  │  │
│  │    │ BEGIN TRANSACTION                                    │  │  │
│  │    │                                                      │  │  │
│  │    │  observe(entry) → FileObservation                    │  │  │
│  │    │       │                                              │  │  │
│  │    │       ▼                                              │  │  │
│  │    │  fetch_by_path() → Option<Source>                    │  │  │
│  │    │  fetch_by_inode() → Option<Source>                   │  │  │
│  │    │       │                                              │  │  │
│  │    │       ▼                                              │  │  │
│  │    │  reconcile(obs, by_path, by_inode)                   │  │  │
│  │    │       │                          (pure function)     │  │  │
│  │    │       ▼                                              │  │  │
│  │    │  if reconciliation.needs_partial_hash():             │  │  │
│  │    │      observation.partial_hash = compute(...)         │  │  │
│  │    │       │                                              │  │  │
│  │    │       ▼                                              │  │  │
│  │    │  apply_reconciliation() → Source                     │  │  │
│  │    │                                                      │  │  │
│  │    │ COMMIT                                               │  │  │
│  │    └──────────────────────────────────────────────────────┘  │  │
│  │                           │                                   │  │
│  │                   seen_ids.insert(source.id)                  │  │
│  │                   stats.update(reconciliation)                │  │
│  │                                                               │  │
│  └───────────────────────────────────────────────────────────────┘  │
│         │                                                           │
│         ▼          seen_ids: HashSet<i64>                           │
│  ┌──────────────┐                                                   │
│  │ find_missing │  (pure: expected_ids - seen_ids)                  │
│  │              │                                                   │
│  └──────┬───────┘                                                   │
│         │          missing_ids: Vec<i64>                            │
│         ▼                                                           │
│  ┌──────────────┐                                                   │
│  │ mark_missing │  (repo: UPDATE present=0)                         │
│  │              │                                                   │
│  └──────────────┘                                                   │
│                                                                     │
│  Return stats                                                       │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Validation Responsibilities

| Layer | Validates |
|-------|-----------|
| **Domain** | Reconciliation rules (what does this observation mean?) |
| **Domain** | Missing detection (which IDs weren't seen?) |
| **Repo** | Database constraints (foreign keys, NOT NULL) |
| **Repo** | SQL correctness (right fields for each reconciliation type) |
| **Command** | Filesystem access (permissions, existence) |
| **Command** | Orchestration (transaction boundaries, error handling) |

---

## Implementation Phases

### Phase 1: Domain Functions

**Status:** completed

**Goal:** Create pure domain functions with comprehensive tests.

**Scope:**
- Create `domain/scan.rs` module
- Define `FileObservation` struct
- Define `Reconciliation` enum with variants: New, Unchanged, Modified, Moved
- Implement `Reconciliation::needs_partial_hash()` and `Reconciliation::source_id()` helper methods
- Implement `reconcile()` function (no device ID checking - that's command layer)
- Implement `find_missing()` function
- Unit tests for all reconciliation scenarios
- Unit tests for helper methods
- Unit tests for missing detection

**Non-goals:**
- Modifying scan.rs command
- Creating repo functions
- Integration with existing code

**Files:**
- New: `src/domain/scan.rs`
- Modify: `src/domain/mod.rs` (add `pub mod scan;`)

### Phase 2: Repository Functions

**Status:** completed

**Goal:** Create repo functions that translate reconciliations to SQL.

**Scope:**
- Add `fetch_by_path()` to `repo/source.rs`
- Add `fetch_by_inode()` to `repo/source.rs`
- Add `apply_reconciliation()` to `repo/source.rs`
- Add `mark_missing()` to `repo/source.rs`
- Unit tests with in-memory database
- Verify returned Source is complete (all fields including joins)

**Non-goals:**
- Modifying scan.rs command
- Changing transaction boundaries

**Files:**
- Modify: `src/repo/source.rs`

### Phase 3: Command Integration

**Status:** completed

**Goal:** Refactor scan.rs to use the new domain and repo layers.

**Scope:**
- Replace inline SQL in `process_file()` with domain + repo calls
- Add per-file transaction wrapping
- Replace `mark_missing()` inline SQL with repo function
- Ensure stats collection works with new model
- Integration tests verifying end-to-end behavior
- Preserve all existing behavior (backward compatibility)

**Non-goals:**
- Changing scan's external interface
- Performance optimization
- Adding new features

**Files:**
- Modify: `src/scan.rs`

### Phase 4: Cleanup and Documentation

**Status:** completed

**Goal:** Remove dead code, update documentation.

**Scope:**
- Remove old inline SQL from scan.rs
- Update CLAUDE.md with new architecture
- Add docstrings to public functions
- Consider removing intermediate types if superseded

**Files:**
- Modify: `src/scan.rs`
- Modify: `CLAUDE.md`

---

## Test Requirements

### Phase 1 Tests (Domain Functions)

| Test Case | Description |
|-----------|-------------|
| `reconcile_new_file` | No source at path, no source by inode → New |
| `reconcile_unchanged_file` | Source at path, same size+mtime → Unchanged |
| `reconcile_modified_file_size` | Source at path, different size → Modified |
| `reconcile_modified_file_mtime` | Source at path, different mtime → Modified |
| `reconcile_moved_file` | No source at path, source by inode exists → Moved |
| `reconcile_moved_cross_root` | Source by inode in different root → Moved with from_root_id |
| `reconcile_replaced_file` | Source at path with different inode → New (old handled by missing) |
| `reconcile_inode_not_tracked` | Source at path with device=None, inode=None → uses size+mtime only |
| `reconcile_device_changed` | Source at path, different device, same inode → Unchanged (device updated) |
| `reconcile_device_changed_modified` | Source at path, different device+size → Modified |
| `reconcile_inode_not_tracked_modified` | Source with inode=0, different size → Modified |
| `needs_partial_hash_new` | New.needs_partial_hash() → true |
| `needs_partial_hash_modified` | Modified.needs_partial_hash() → true |
| `needs_partial_hash_moved` | Moved.needs_partial_hash() → false |
| `needs_partial_hash_unchanged` | Unchanged.needs_partial_hash() → false |
| `source_id_new` | New.source_id() → None |
| `source_id_unchanged` | Unchanged.source_id() → Some(id) |
| `source_id_modified` | Modified.source_id() → Some(id) |
| `source_id_moved` | Moved.source_id() → Some(id) |
| `find_missing_empty_sets` | Both sets empty → empty result |
| `find_missing_all_seen` | All expected IDs were seen → empty result |
| `find_missing_none_seen` | No expected IDs were seen → all expected returned |
| `find_missing_partial` | Some IDs seen, some not → correct difference |
| `find_missing_seen_has_extra` | Extra IDs in seen_ids are ignored |

### Phase 2 Tests (Repository Functions)

| Test Case | Description |
|-----------|-------------|
| `fetch_by_path_exists` | Returns Source when present=1 at path |
| `fetch_by_path_not_present` | Returns None when present=0 |
| `fetch_by_path_not_found` | Returns None when no source at path |
| `fetch_by_inode_exists` | Returns Source when present=1 with inode |
| `fetch_by_inode_cross_root` | Returns Source from different root |
| `fetch_by_inode_not_found` | Returns None when no match |
| `apply_reconciliation_new` | INSERT with correct fields, returns complete Source |
| `apply_reconciliation_unchanged` | UPDATE device, inode, last_seen_at |
| `apply_reconciliation_modified` | UPDATE size, mtime, partial_hash, basis_rev+1 |
| `apply_reconciliation_moved` | UPDATE root_id, rel_path (covers cross-root moves) |
| `apply_reconciliation_new_revives_stale_record` | UPDATE existing stale record instead of INSERT |
| `apply_reconciliation_new_requires_partial_hash` | Error if partial_hash missing for New |
| `mark_missing_sets_present_zero` | present=0 for specified IDs |
| `mark_missing_empty_list` | No error on empty input |
| `mark_missing_returns_count` | Returns correct count of updated rows |
| `mark_missing_updates_last_seen_at` | last_seen_at updated when marking missing |

### Phase 3 Tests (Command Layer Unit Tests)

These test `process_file()` and `mark_missing_sources()` functions directly, covering the same scenarios as full integration tests but at the unit level.

| Test Case | Description |
|-----------|-------------|
| `process_file_new` | No existing source → New action, source inserted |
| `process_file_unchanged` | Matching source exists → Unchanged action |
| `process_file_modified_size` | Source exists with different size → Modified action |
| `process_file_moved` | Source with same inode at different path → Moved action |
| `process_file_replaced` | Source at path with different inode → New action (old becomes missing) |
| `process_file_revives_stale_record` | Stale (present=0) record at path → New action, record revived |
| `process_file_device_changed` | File exists but device differs → Unchanged action, device updated |
| `process_file_independent_operations` | Multiple operations don't interfere with each other |
| `mark_missing_sources_counts_correctly` | Correct counts for Seen/Missing/Disconnected outcomes |
| `mark_missing_sources_disconnected_with_ignore_flag` | --ignore-device-id treats Disconnected as Missing |

---

## Related Documents

- `.claude/specs/2026-02-01-write-infrastructure.md` — Established patterns for domain types and repo functions
- `CLAUDE.md` — Architecture documentation (update when complete)

---

## Resolved Questions

1. **Computed field timing (partial hash, full hash)**: Resolved in D6.
   - **Decision**: Reconciliation indicates what's needed via `needs_partial_hash()`. Command layer computes partial hash after reconciliation, before persistence. Full hash is user-controlled (`--hash` flag) and handled separately.
   - **Rationale**: Domain describes "what happened", command applies policy. Partial hash is infrastructure requirement. Full hash is optional feature.

2. **Object creation / full hashing**:
   - **Decision**: Out of scope for this spec. Full hashing happens after scan walk completes, in a separate phase.
   - **Rationale**: Hashing phase is already a distinct operation (enabled by default, `--no-hash` to disable). This spec focuses on reconciliation and source record persistence.

3. **Disconnected sources**: Resolved in D8.
   - **Decision**: Disconnected detection is a **command layer** concern, NOT a domain concern. The `reconcile()` function does not check device IDs — it only processes files that exist on disk.
   - **Rationale**: Disconnected detection is about empty directories where files should exist but don't — this requires filesystem knowledge (can we access the path?) that doesn't belong in pure domain logic. The `classify_sources_in_empty_dir()` function handles this at the command layer, comparing the empty directory's device ID to stored source device IDs.
   - **Key insight**: Empty directories on the filesystem could be disconnected mountpoints. Before assuming the sources under that path are actually gone, we do a device ID check — if the directory's device differs from what we stored for those sources, we have reasonable certainty that the storage is disconnected. This protects users from accidentally marking sources as missing when the storage is simply offline.
