# Refactoring Spec: Operations Layer Foundation (Phase 2 Steps 1-3)

## Overview

Foundation work for ADR Phase 2 (boundary correction). Three structural changes that enable all subsequent extraction work: update CLAUDE.md with the five-layer architecture, create `ops/fs.rs` as the filesystem access layer, and fix cross-layer type ownership. No behavioral changes — all existing tests must continue to pass.

**ADR**: `~/store/canon-architecture/2026-03-13-operations-layer.md` (Phase 2 Steps 1-3)

## Phases

### Phase 1: CLAUDE.md + ops/fs.rs
- **Status**: completed
- **Goal**: Declare the five-layer architecture in CLAUDE.md. Create `ops/fs.rs` with filesystem primitives extracted from scan.rs and apply.rs.
- **Scope**:
  - Update CLAUDE.md layer diagram from 4-layer to 5-layer (add ops/fs)
  - Update layer responsibility table with ops/fs row
  - Add ops/fs module description and conventions
  - Create `src/ops/fs.rs` with:
    - `compute_partial_hash()` — moved from `scan.rs:727-754`
    - `compute_full_hash()` — moved from `scan.rs:757-772`
    - `preserve_metadata()` — moved from `apply.rs:1079-1095` (both Unix and non-Unix variants)
  - Add `pub mod fs;` to `ops/mod.rs`
  - Update callers:
    - `scan.rs`: remove `compute_partial_hash` and `compute_full_hash` definitions, import from `ops::fs`
    - `apply.rs:19`: change `use crate::scan::compute_partial_hash` to `use crate::ops::fs::compute_partial_hash`
  - Add unit tests for all three functions in ops/fs.rs
- **Non-goals**: Creating copy_file/rename_file/move_file/validate_file_state — these come with apply execute extraction (ADR step 4) when they'll have callers. Changing any behavioral logic. Extracting `check_destination_writable`.
- **Dependencies**: None

#### CLAUDE.md Changes

**Layer diagram** — replace the current 4-layer diagram (lines 376-404) with:

```
┌─────────────────────────────────────────────────────────────┐
│ Interface Layer (src/*.rs — CLI today, TUI tomorrow)        │
│ - CLI argument parsing (clap structs)                       │
│ - Output formatting (terminal, JSONL, null-delimited)       │
│ - Ceremony presentation (display plan, prompt, report)      │
│ - Directory walk creation (WalkDir configuration)           │
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
│ - Orchestrates repo + domain + fs                           │
│ - Interface-independent — no stdout, stderr, stdin          │
└─────────────────────────────────────────────────────────────┘
                          │
          ┌───────────────┼───────────────┐
          ▼               ▼               ▼
┌──────────────────┐ ┌──────────────┐ ┌──────────────────────┐
│ Repository (repo/)│ │ Domain       │ │ Filesystem (ops/fs)  │
│ - ALL db access   │ │ (domain/)    │ │ - File copy/rename   │
│ - Returns domain  │ │ - Pure logic │ │ - Hash computation   │
│   types           │ │ - No I/O     │ │ - Metadata ops       │
│ - Batch ops       │ │ - Predicates │ │ - No DB, no terminal │
│ - SQL lives HERE  │ │              │ │                      │
└──────────────────┘ └──────────────┘ └──────────────────────┘
```

**Layer responsibility table** — replace (lines 407-414) with:

| Layer | Allowed | NOT Allowed |
|-------|---------|-------------|
| **Interface** | CLI parsing, output formatting, ceremony presentation, terminal I/O, directory walk creation | Business logic, source selection, ceremony policy, computation, filesystem data operations, direct repo calls |
| **Operations** | Composing domain + repo + fs into behaviors, typed results, ceremony policy, transactions (writes) | stdout/stderr/stdin, CLI argument types, display formatting, direct SQL, direct filesystem data operations (use ops/fs) |
| **ops/fs** | Filesystem data operations: copy, rename, validate, hash, metadata | Database access, terminal I/O, business logic decisions |
| **Repo** | Database queries, returning domain types, batch operations | Business logic, transaction management, filesystem access |
| **Domain** | Pure functions, structs, predicates, business logic | Any I/O (database, filesystem, network) |

**Add ops/fs description** — after the Operations Layer Conventions section, add:

```markdown
**Filesystem Layer** (`src/ops/fs.rs`):

The filesystem layer provides structured access to files on disk, parallel to how the repo layer provides structured access to the database. Canon has two fundamental data planes: Sources (DB-indexed, via repo) and Files (on disk, via ops/fs). The operations layer orchestrates both.

ops/fs functions:
- Take paths and parameters, return typed results
- Do NOT make business decisions (the ops layer decides what to do, ops/fs does it)
- Do NOT access the database or terminal
- Are testable in isolation using temp files

Currently provides: `compute_partial_hash()`, `compute_full_hash()`, `preserve_metadata()`.
Future additions (with apply/scan execute extraction): `copy_file()`, `rename_file()`, `move_file()`, `validate_file_state()`.
```

**Update "When Adding New Features"** — add item between 3 and 4:

```
4. If you need filesystem operations (copy, hash, validate, metadata) → add to ops/fs layer
```

(Renumber existing items 4 and 5 to 5 and 6.)

**Update the note about incremental ops introduction** — update the paragraph at line 416 to reflect the current state, including that `ops::cluster` and `ops::apply` have plan functions but execute functions are planned for a future phase.

#### ops/fs.rs Implementation

```rust
//! Filesystem access layer for canon.
//!
//! Structured access to files on disk — parallel to how `repo` provides
//! structured access to the database. Operations layer orchestrates both.
//!
//! Functions here perform filesystem operations but do not make business
//! decisions. The ops layer decides what to do; this module does it.
//!
//! No database access, no terminal I/O.

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use std::fs::{self, File, Metadata};
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

const PARTIAL_HASH_CHUNK_SIZE: usize = 8192; // 8KB

/// Compute SHA256 hash of first 8KB + last 8KB of a file.
/// For files <= 16KB, hash the entire file.
pub fn compute_partial_hash(path: &Path, size: u64) -> Result<String> {
    // ... exact code from scan.rs:729-753
}

/// Compute full SHA256 hash of a file.
pub fn compute_full_hash(path: &Path) -> Result<String> {
    // ... exact code from scan.rs:758-771
}

/// Preserve source file metadata (mtime, permissions) on a destination file.
#[cfg(unix)]
pub fn preserve_metadata(dest: &Path, src_meta: &Metadata) -> Result<()> {
    // ... exact code from apply.rs:1080-1088
}

#[cfg(not(unix))]
pub fn preserve_metadata(_dest: &Path, _src_meta: &Metadata) -> Result<()> {
    Ok(())
}
```

#### Caller Updates

**scan.rs**:
- Remove `compute_partial_hash()` function definition (lines 725-754)
- Remove `compute_full_hash()` function definition (lines 757-772)
- Remove `const PARTIAL_HASH_CHUNK_SIZE` (line 725)
- Remove unused imports that were only needed by these functions (`sha2::{Digest, Sha256}`, `std::io::{Read, Seek, SeekFrom}`) — verify which imports are still needed by the rest of scan.rs
- Add `use crate::ops::fs::{compute_partial_hash, compute_full_hash};`
- All call sites in scan.rs remain unchanged (function names and signatures identical)

**apply.rs**:
- Remove `preserve_metadata()` function definitions (lines 1079-1095, both Unix and non-Unix)
- Remove `use crate::scan::compute_partial_hash;` (line 19)
- Add `use crate::ops::fs::{compute_partial_hash, preserve_metadata};`
- All call sites in apply.rs remain unchanged

**ops/mod.rs**:
- Add `pub mod fs;`

#### Tests for ops/fs.rs

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn partial_hash_small_file() {
        // File < 16KB: hashes entire content
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(b"hello world").unwrap();
        let hash = compute_partial_hash(f.path(), 11).unwrap();
        // SHA256 of "hello world"
        assert_eq!(hash, "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9");
    }

    #[test]
    fn partial_hash_large_file() {
        // File > 16KB: hashes first 8KB + last 8KB
        let mut f = NamedTempFile::new().unwrap();
        let data = vec![0u8; 32768]; // 32KB of zeros
        f.write_all(&data).unwrap();
        let hash = compute_partial_hash(f.path(), 32768).unwrap();
        assert!(!hash.is_empty());
        // Verify deterministic
        let hash2 = compute_partial_hash(f.path(), 32768).unwrap();
        assert_eq!(hash, hash2);
    }

    #[test]
    fn partial_hash_boundary_16kb() {
        // Exactly 16KB: should hash entire content (size <= 2 * 8KB)
        let mut f = NamedTempFile::new().unwrap();
        let data = vec![42u8; 16384];
        f.write_all(&data).unwrap();
        let hash = compute_partial_hash(f.path(), 16384).unwrap();
        assert!(!hash.is_empty());
    }

    #[test]
    fn full_hash_known_content() {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(b"hello world").unwrap();
        let hash = compute_full_hash(f.path()).unwrap();
        assert_eq!(hash, "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9");
    }

    #[test]
    fn full_hash_empty_file() {
        let f = NamedTempFile::new().unwrap();
        let hash = compute_full_hash(f.path()).unwrap();
        // SHA256 of empty input
        assert_eq!(hash, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    }

    #[test]
    fn partial_and_full_hash_match_for_small_file() {
        // For small files (< 16KB), partial == full
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(b"test content").unwrap();
        let partial = compute_partial_hash(f.path(), 12).unwrap();
        let full = compute_full_hash(f.path()).unwrap();
        assert_eq!(partial, full);
    }

    #[cfg(unix)]
    #[test]
    fn preserve_metadata_mtime() {
        use filetime::FileTime;

        let src = NamedTempFile::new().unwrap();
        let dest = NamedTempFile::new().unwrap();

        // Set a known mtime on source
        let known_mtime = FileTime::from_unix_time(1704067200, 0);
        filetime::set_file_mtime(src.path(), known_mtime).unwrap();

        let src_meta = fs::metadata(src.path()).unwrap();
        preserve_metadata(dest.path(), &src_meta).unwrap();

        let dest_meta = fs::metadata(dest.path()).unwrap();
        let dest_mtime = FileTime::from_last_modification_time(&dest_meta);
        assert_eq!(dest_mtime, known_mtime);
    }
}
```

### Phase 2: Cross-Layer Type Moves
- **Status**: completed
- **Goal**: Move `LockEntry` and `TransferMode` to their correct architectural homes, fixing the cross-layer dependency violation where ops imports from interface.
- **Scope**:
  - Move `LockEntry` struct + impl from `cluster.rs:67-109` to `ops/cluster.rs`
  - Move `TransferMode` enum from `apply.rs:21-26` to `ops/apply.rs`
  - Update all import sites
- **Non-goals**: Moving ManifestConfig/ManifestMeta/ManifestOutput/ManifestOptions (these are interface-specific TOML types, correctly in cluster.rs). Moving ApplyOptions/ApplyStats/ApplyAction (interface-specific types).
- **Dependencies**: Phase 1 (ops/fs.rs must exist for mod.rs to be updated)

#### LockEntry Move

**From**: `src/cluster.rs:67-109` (struct definition + `from_source()` impl)

**To**: `src/ops/cluster.rs` (at the top, before plan types)

The full block to move:
```rust
/// JSONL lock entry (one per line in .lock file)
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LockEntry {
    pub id: i64,
    pub root_id: i64,
    pub path: String,
    pub device: i64,
    pub inode: i64,
    pub size: i64,
    pub mtime: i64,
    pub partial_hash: String,
    pub object_id: Option<i64>,
    pub hash_type: Option<String>,
    pub hash_value: Option<String>,
}

impl LockEntry {
    pub fn from_source(
        source: &Source,
        hash_type: Option<String>,
        hash_value: Option<String>,
    ) -> Self {
        Self {
            id: source.id,
            root_id: source.root_id,
            path: source.path(),
            device: source.device,
            inode: source.inode,
            size: source.size,
            mtime: source.mtime,
            partial_hash: source.partial_hash.clone(),
            object_id: source.object_id,
            hash_type,
            hash_value,
        }
    }
}
```

**Import updates**:
- `ops/cluster.rs`: Remove `use crate::cluster::LockEntry;` (line 12). Add `use serde::{Serialize, Deserialize};` if not already present. Add `use crate::domain::source::Source;` if not already present (needed by `from_source`).
- `ops/apply.rs`: Change `use crate::cluster::LockEntry;` (line 10) to `use super::cluster::LockEntry;` or `use crate::ops::cluster::LockEntry;`
- `cluster.rs` (interface): Remove struct+impl definition. Add `use crate::ops::cluster::LockEntry;`. All existing uses of LockEntry in cluster.rs continue to work.
- `apply.rs` (interface): Change `use crate::cluster::{self, LockEntry, ManifestConfig};` (line 11) to `use crate::cluster::{self, ManifestConfig};` and add `use crate::ops::cluster::LockEntry;`

**Verify**: `LockEntry` uses `Source::path()` in `from_source()`. `Source` is a domain type — ops can import domain types, so this dependency direction is correct.

#### TransferMode Move

**From**: `src/apply.rs:21-26`

**To**: `src/ops/apply.rs` (at the top, before plan types)

The block to move:
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferMode {
    Copy,   // Default: copy only, source remains
    Rename, // Unix only, error if cross-device
    Move,   // Try rename, fallback to copy+delete on EXDEV
}
```

**Import updates**:
- `ops/apply.rs`: No import needed — it's defined here now.
- `apply.rs` (interface): Remove enum definition. Add `use crate::ops::apply::TransferMode;`. The `ApplyOptions` struct in apply.rs references `TransferMode` — this still works via the import.
- `main.rs:903-908`: Change `apply::TransferMode::Rename` etc. to `ops::apply::TransferMode::Rename` (or add a `use crate::ops::apply::TransferMode;` import).

### Phase 3: Test Helper Consolidation
- **Status**: completed
- **Goal**: Eliminate duplicated test helpers across ops modules by creating a shared test utility module.
- **Scope**:
  - Create `src/ops/test_helpers.rs` with shared helpers
  - Update all four ops test modules to use shared helpers
  - Remove duplicated helper definitions
- **Non-goals**: Consolidating test helpers in interface modules (separate effort). Adding new tests beyond what's needed for the consolidation.
- **Dependencies**: Phase 2 (LockEntry must be in ops/cluster.rs for make_lock_entry to import cleanly)

#### Shared Test Module

Create `src/ops/test_helpers.rs`:

```rust
//! Shared test helpers for ops layer tests.
//!
//! Consolidates duplicated insert/setup helpers that were independently
//! maintained in each ops module's test section.

use crate::repo::Connection;

pub fn setup_test_db() -> Connection {
    crate::repo::db::open_in_memory_for_test()
}

pub fn insert_root(conn: &Connection, path: &str, role: &str, suspended: bool) -> i64 {
    conn.execute(
        "INSERT INTO roots (path, role, suspended) VALUES (?, ?, ?)",
        rusqlite::params![path, role, suspended as i64],
    )
    .unwrap();
    conn.last_insert_rowid()
}

pub fn insert_object(conn: &Connection, hash: &str, excluded: bool) -> i64 {
    conn.execute(
        "INSERT INTO objects (hash_type, hash_value, excluded) VALUES ('sha256', ?, ?)",
        rusqlite::params![hash, excluded as i64],
    )
    .unwrap();
    conn.last_insert_rowid()
}

/// Insert a source with default metadata.
/// Uses size=1000, mtime=1704067200, partial_hash="testhash", excluded=false.
pub fn insert_source(
    conn: &Connection,
    root_id: i64,
    rel_path: &str,
    object_id: Option<i64>,
) -> i64 {
    insert_source_full(conn, root_id, rel_path, object_id, false, 1000, 1704067200, "testhash")
}

/// Insert a source marked as excluded.
pub fn insert_source_excluded(
    conn: &Connection,
    root_id: i64,
    rel_path: &str,
    object_id: Option<i64>,
) -> i64 {
    insert_source_full(conn, root_id, rel_path, object_id, true, 1000, 1704067200, "testhash")
}

/// Insert a source with a specific size.
pub fn insert_source_with_size(
    conn: &Connection,
    root_id: i64,
    rel_path: &str,
    object_id: Option<i64>,
    size: i64,
) -> i64 {
    insert_source_full(conn, root_id, rel_path, object_id, false, size, 1704067200, "testhash")
}

/// Insert a source with specific size and mtime (for staleness tests).
pub fn insert_source_with_metadata(
    conn: &Connection,
    root_id: i64,
    rel_path: &str,
    object_id: Option<i64>,
    size: i64,
    mtime: i64,
) -> i64 {
    insert_source_full(conn, root_id, rel_path, object_id, false, size, mtime, "testhash")
}

/// Full-control source insertion with all parameters.
pub fn insert_source_full(
    conn: &Connection,
    root_id: i64,
    rel_path: &str,
    object_id: Option<i64>,
    excluded: bool,
    size: i64,
    mtime: i64,
    partial_hash: &str,
) -> i64 {
    conn.execute(
        "INSERT INTO sources (root_id, rel_path, object_id, size, mtime, partial_hash, scanned_at, last_seen_at, device, inode, excluded)
         VALUES (?, ?, ?, ?, ?, ?, 0, 0, 0, 0, ?)",
        rusqlite::params![root_id, rel_path, object_id, size, mtime, partial_hash, excluded as i64],
    )
    .unwrap();
    conn.last_insert_rowid()
}

/// Insert a fact for a source.
pub fn insert_fact(conn: &Connection, source_id: i64, key: &str, value: &str) {
    conn.execute(
        "INSERT INTO facts (entity_type, entity_id, key, value_text, observed_at, observed_basis_rev) VALUES ('source', ?, ?, ?, 0, 0)",
        rusqlite::params![source_id, key, value],
    )
    .unwrap();
}

/// Check if a source is excluded in the DB.
pub fn is_source_excluded(conn: &Connection, source_id: i64) -> bool {
    conn.query_row(
        "SELECT excluded FROM sources WHERE id = ?",
        [source_id],
        |row| row.get::<_, bool>(0),
    )
    .unwrap()
}

/// Check if an object is excluded in the DB.
pub fn is_object_excluded(conn: &Connection, object_id: i64) -> bool {
    conn.query_row(
        "SELECT excluded FROM objects WHERE id = ?",
        [object_id],
        |row| row.get::<_, bool>(0),
    )
    .unwrap()
}
```

**Register in ops/mod.rs**:
```rust
#[cfg(test)]
pub(crate) mod test_helpers;
```

#### Migration Per Module

**selection.rs** — Replace:
- Remove: `setup_test_db`, `insert_root`, `insert_object`, `insert_source`, `insert_source_excluded`, `insert_source_with_size`
- Add: `use super::test_helpers::{setup_test_db, insert_root, insert_object, insert_source, insert_source_excluded, insert_source_with_size};`
- Keep: `make_params()` — module-specific helper

**exclude.rs** — Replace:
- Remove: `setup_test_db`, `insert_root`, `insert_object`, `insert_source`, `insert_source_excluded`, `insert_source_with_size`, `is_source_excluded`, `is_object_excluded`
- Add: `use super::test_helpers::{setup_test_db, insert_root, insert_object, insert_source, insert_source_excluded, insert_source_with_size, is_source_excluded, is_object_excluded};`
- Keep: `make_set_params()`, `make_clear_params()`, `make_duplicates_params()`, `make_set_objects_params()` — module-specific

**cluster.rs** — Replace:
- Remove: `setup_test_db`, `insert_root`, `insert_object`, `insert_source` (with excluded param), `insert_fact`
- Add: `use super::test_helpers::{setup_test_db, insert_root, insert_object, insert_source, insert_source_excluded, insert_fact};`
- **Signature change**: cluster.rs uses `insert_source(conn, root_id, rel_path, object_id, excluded)`. Replace calls with:
  - `excluded=false` → `insert_source(conn, root_id, rel_path, object_id)`
  - `excluded=true` → `insert_source_excluded(conn, root_id, rel_path, object_id)`
- Keep: `default_params()` — module-specific

**apply.rs** (ops) — Replace:
- Remove: `setup_test_db`, `insert_root`, `insert_object`, `insert_source` (with size/mtime), `insert_source_excluded` (with excluded param), `insert_fact`
- Add: `use super::test_helpers::{setup_test_db, insert_root, insert_object, insert_source_with_metadata, insert_source_excluded, insert_fact};`
- **Signature change**: apply.rs uses `insert_source(conn, root_id, rel_path, object_id, size, mtime)`. Replace with `insert_source_with_metadata(conn, root_id, rel_path, object_id, size, mtime)`.
- **Signature change**: apply.rs uses `insert_source_excluded(conn, root_id, rel_path, object_id, excluded)`. The `excluded` param is always `true` in practice — replace with `insert_source_excluded(conn, ...)` (no param) or use `insert_source_full(...)` for the `excluded=false` case if it exists.
- Keep: `make_lock_entry()`, `default_params()` — module-specific

#### Verification

After each module migration, run `cargo test` to verify all tests pass. Migrate one module at a time: selection → exclude → cluster → apply.

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| Only move standalone functions to ops/fs now | copy/rename/move primitives should be created when they have callers (apply execute, step 4). Avoids dead code. |
| LockEntry → ops/cluster.rs (not domain/) | It's produced by ops::cluster::plan_generate and consumed by ops::apply::plan_apply. It carries Serialize/Deserialize for JSONL. It's semantically an ops pipeline type. |
| TransferMode → ops/apply.rs (not domain/) | It determines operational behavior for file transfers. It'll be needed by execute_apply(). It's not a pure domain concept. |
| ManifestConfig etc. stay in cluster.rs | These are TOML file format types, only used by interface for manifest I/O. They're genuinely interface-specific. |
| Unified insert_source_full with convenience wrappers | Handles all four signature variants cleanly. Consistent defaults (size=1000, mtime=1704067200, partial_hash="testhash"). |
| partial_hash default changed to "testhash" | selection.rs and exclude.rs used '' (empty), cluster.rs used 'ph', apply.rs used 'testhash'. Standardizing on 'testhash' — a non-empty value that's clearly test data and doesn't interfere with hash-based logic. |

## Test Requirements

### Existing Tests
All 724 existing tests must pass after each phase. No behavioral changes.

### New Tests to Add

**Phase 1 — ops/fs.rs unit tests:**
- `partial_hash_small_file` — file < 16KB hashes entire content
- `partial_hash_large_file` — file > 16KB hashes first+last 8KB, deterministic
- `partial_hash_boundary_16kb` — exactly 16KB boundary
- `full_hash_known_content` — known SHA256 for "hello world"
- `full_hash_empty_file` — SHA256 of empty input
- `partial_and_full_hash_match_for_small_file` — equivalence for small files
- `preserve_metadata_mtime` — mtime preserved (Unix only)

**Phase 2 — no new tests** (type moves only, existing tests verify)

**Phase 3 — no new tests** (helper consolidation, existing tests verify)

## Implementation Checklist
- [x] Phase 1: CLAUDE.md update + ops/fs.rs creation
- [x] Phase 2: LockEntry and TransferMode type moves
- [x] Phase 3: Test helper consolidation
- [x] Verify all existing tests pass (731 pass)
- [x] Update CLAUDE.md ops module listing (add `fs.rs` to the `src/ops/` list)
