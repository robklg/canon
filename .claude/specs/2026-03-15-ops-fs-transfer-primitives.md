# Refactoring Spec: ops/fs File Transfer Primitives

## Overview

Extract raw filesystem transfer operations from `ops/apply.rs` into `ops/fs.rs`, completing the two-data-planes architecture where ops/fs is to the filesystem what repo is to the database.

**ADR**: `~/store/canon-architecture/2026-03-13-operations-layer.md` — Phase 3, Step 1

## Scope

### In scope
- Add `copy_file`, `rename_file`, `move_file`, `ensure_parent_dir` to `ops/fs.rs`
- Add `MoveOutcome` enum to `ops/fs.rs`
- Noclobber parameter on all three transfer functions
- `copy_file` bundles metadata preservation internally
- Update `execute_single_transfer()` in `ops/apply.rs` to call the new functions
- Tests for all new functions
- Update CLAUDE.md ops/fs listing

### Non-goals
- Extracting `fs::metadata()` calls from `validate_source_state()`, `classify_transfers_disk()`, or `build_new_source()` — those read metadata for business decisions, not transfers
- New traits or abstractions
- Changes to the apply plan/execute public interface

## Design

### New types in `ops/fs.rs`

```rust
/// Outcome of a move operation — caller needs to know which DB path to take.
pub enum MoveOutcome {
    /// Same-device rename succeeded (source relocated)
    Renamed,
    /// Cross-device: copied to dest, deleted source
    CopiedAndDeleted,
}
```

### New functions in `ops/fs.rs`

```rust
/// Create parent directories for a path.
pub fn ensure_parent_dir(path: &Path) -> Result<()>;

/// Copy a file and preserve its metadata (mtime, permissions).
/// If noclobber is true, errors when dest already exists.
pub fn copy_file(src: &Path, dest: &Path, noclobber: bool) -> Result<()>;

/// Rename a file (same filesystem only).
/// If noclobber is true, errors when dest already exists.
pub fn rename_file(src: &Path, dest: &Path, noclobber: bool) -> Result<()>;

/// Move a file: try rename, fall back to copy+delete on cross-device (EXDEV).
/// If noclobber is true, errors when dest already exists.
/// Returns which strategy was used so the caller can take the appropriate DB path.
pub fn move_file(src: &Path, dest: &Path, noclobber: bool) -> Result<MoveOutcome>;
```

### Implementation details

**`copy_file`**: Reads source metadata before copy. Calls `fs::copy()` then `preserve_metadata()`. Bundles the three steps that always go together in apply.rs.

**`rename_file`**: Thin wrapper around `fs::rename()` with noclobber check and error context.

**`move_file`**: Tries `fs::rename()`. On Unix, catches `EXDEV` error and falls back to `copy_file(src, dest, noclobber)` + `fs::remove_file(src)`. Returns `MoveOutcome::Renamed` or `MoveOutcome::CopiedAndDeleted`. The `#[cfg(unix)]` EXDEV handling matches the current pattern in apply.rs.

**Noclobber**: All three functions check `dest.exists()` before proceeding when `noclobber: true`. This is check-then-act (TOCTOU race exists but is acceptable for Canon's archiving use case). When `noclobber: false`, proceeds without checking.

**Error context**: All functions provide descriptive error messages including both source and destination paths, matching the current quality in apply.rs.

### Changes to `ops/apply.rs`

`execute_single_transfer()` (lines 649-746) changes from:

```rust
// Current: raw fs calls with inline noclobber checks
if dest_path.exists() {
    bail!("Destination already exists: {}", dest_path.display());
}
let src_meta = fs::metadata(src_path)...;
fs::copy(src_path, &dest_path)...;
preserve_metadata(&dest_path, &src_meta)?;
```

To:

```rust
// New: ops/fs handles it
ensure_parent_dir(&dest_path)?;

match transfer_mode {
    TransferMode::Copy => {
        copy_file(src_path, &dest_path, true)?;
        // ... DB registration unchanged
    }
    TransferMode::Rename => {
        rename_file(src_path, &dest_path, true)?;
        // ... DB relocation unchanged
    }
    TransferMode::Move => {
        match move_file(src_path, &dest_path, true)? {
            MoveOutcome::Renamed => {
                // ... relocate_source unchanged
            }
            MoveOutcome::CopiedAndDeleted => {
                // ... mark_source_not_present + insert_destination unchanged
            }
        }
    }
}
```

The `ensure_parent_dir` call moves before the match — it's common to all modes.

Remove `use std::fs` from apply.rs imports (only `preserve_metadata` was the ops/fs import before; now all fs access goes through ops/fs). The remaining `fs::metadata()` calls in `validate_source_state`, `classify_transfers_disk`, and `build_new_source` still need `std::fs` — keep that import but it's now only for metadata reads.

## Test Requirements

### Existing tests (must continue to pass)
- `ops/fs` tests: 8 existing (hashing, metadata, writability)
- `ops/apply` tests: ~30 tests including 3 integration tests for `execute_single_transfer` (copy success, noclobber, missing source)

### New tests to add in `ops/fs`

**ensure_parent_dir:**
1. `ensure_parent_dir_creates_nested` — creates `a/b/c` from a tempdir base
2. `ensure_parent_dir_existing_noop` — succeeds when parent already exists

**copy_file:**
3. `copy_file_success` — copies content and preserves mtime
4. `copy_file_noclobber_rejects_existing` — errors with "already exists" when dest exists and noclobber=true
5. `copy_file_overwrites_without_noclobber` — succeeds when dest exists and noclobber=false
6. `copy_file_missing_source` — returns descriptive error

**rename_file:**
7. `rename_file_success` — source gone, dest has correct content
8. `rename_file_noclobber_rejects_existing` — errors when dest exists and noclobber=true

**move_file:**
9. `move_file_same_device` — returns `MoveOutcome::Renamed`, source gone, dest has content
10. `move_file_noclobber_rejects_existing` — errors when dest exists and noclobber=true

Note: Cross-device `move_file` (EXDEV fallback) is not unit-testable without separate filesystems. The existing `execute_single_transfer` integration tests and the separate `copy_file` tests provide sufficient coverage of the fallback path's components.

## CLAUDE.md update

Update the ops/fs listing to include the new functions:

```
Currently provides: `compute_partial_hash()`, `compute_full_hash()`, `preserve_metadata()`, `check_destination_writable()`, `ensure_parent_dir()`, `copy_file()`, `rename_file()`, `move_file()`, `MoveOutcome`.
```

Remove the "Future additions" line since they're now present.

## Design Decisions
- Noclobber as a bool parameter rather than a separate enum — simple, matches the single business decision (Canon always passes `true` today, but the parameter keeps ops/fs general-purpose)
- `copy_file` bundles metadata preservation — these are never called separately in Canon
- `move_file` returns `MoveOutcome` not `TransferOutcome` — ops/fs has its own vocabulary, not coupled to apply's types
- `fs::metadata()` calls for validation/classification stay in apply.rs — reading metadata for decisions is different from performing transfers
