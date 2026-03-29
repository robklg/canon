# Story: Preflight Hardening

**Design Spec**: [~/store/claude-designs/2026-03-29-apply-safety-and-recovery.md](~/store/claude-designs/2026-03-29-apply-safety-and-recovery.md) (Stories 2 & 3)
**Epic**: [epic-apply-safety-and-recovery.md](epic-apply-safety-and-recovery.md)
**Status**: Complete
**Created**: 2026-03-29

## Objective

Apply currently has no upfront check that source files exist and are readable — missing sources are discovered one-by-one during transfer, leaving partial state. And the noclobber implementation uses a check-then-operate pattern with a TOCTOU gap — a concurrent process or race condition could cause silent overwrites.

This story adds source existence/readability preflight (hard abort before any file ops) and upgrades noclobber to use atomic filesystem primitives (`O_EXCL` for copy, platform-native noclobber rename for rename/move).

## Functional Requirements Summary

**Source Preflight**:
- Before any file operations, stat every source file and open briefly to verify readability
- Missing or unreadable sources = hard abort with all affected files listed
- Distinguish "missing" (suggest `cluster refresh`) from "permission denied" (suggest fix permissions)
- In resume mode, only check sources for pending transfers (dest not already present)
- Runs after pattern validation, before confirmation prompt

**Noclobber**:
- Copy: atomic create-only-if-not-exists (`O_EXCL` / `create_new(true)`)
- Rename: platform-native atomic noclobber (`renameatx_np(RENAME_EXCL)` on macOS, `renameat2(RENAME_NOREPLACE)` on Linux, stat-then-rename fallback)
- Per-file error on noclobber failure (not crash, not silent overwrite)
- Defense-in-depth — preflight catches the common case, noclobber catches race conditions

## Current State

**Source preflight**: Does not exist. Sources are checked per-file during execute via `validate_source_state()` which stats the file and recomputes partial hash. Missing sources produce `TransferOutcome::SkippedMissing` — a runtime skip, not a preflight abort.

**Noclobber in `ops/fs.rs`**:
- `copy_file()`: `if noclobber && dest.exists() { bail!() }` then `fs::copy()` — TOCTOU gap between check and copy. `fs::copy()` overwrites by default.
- `rename_file()`: `if noclobber && dest.exists() { bail!() }` then `fs::rename()` — TOCTOU gap. `fs::rename()` atomically replaces on POSIX.
- `move_file()`: Same check, plus EXDEV fallback calls `copy_file(src, dest, false)` — noclobber intentionally bypassed for the copy-then-delete path (but the initial check caught it). Still has TOCTOU gap on the initial check.

## Design

### Phase 1: Source Existence and Readability Preflight

- **Goal**: Apply verifies all sources exist and are readable before any file operations
- **Scope**: `ops/apply.rs` (new violation type + preflight check in plan_apply), `apply.rs` (display)

#### Changes

**New violation fields in `ApplyViolations`** (ops/apply.rs):

```rust
pub struct ApplyViolations {
    // ... existing fields ...
    /// Source files that are missing (stat failed, not found).
    pub missing_sources: Vec<(i64, String)>,  // (source_id, path)
    /// Source files that exist but are not readable (permission denied).
    pub unreadable_sources: Vec<(i64, String)>,  // (source_id, path)
}
```

**New preflight in `plan_apply()`**, after pattern validation and archive-root check, before staleness/conflict checks:

```rust
// --- Source existence and readability preflight ---
// In resume mode, only check sources with pending transfers (dest not already at destination).
// The `transfers` vec already contains only pending entries (resume filtering happened above).
for transfer in &transfers {
    let path = Path::new(&transfer.source_path);
    match fs::metadata(path) {
        Ok(meta) => {
            if !meta.is_file() {
                violations.missing_sources.push((transfer.source_id, transfer.source_path.clone()));
                continue;
            }
            // Check readability: try to open the file
            match File::open(path) {
                Ok(_) => {} // readable
                Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
                    violations.unreadable_sources.push((transfer.source_id, transfer.source_path.clone()));
                }
                Err(_) => {
                    // Other open errors (e.g., too many open files) — treat as unreadable
                    violations.unreadable_sources.push((transfer.source_id, transfer.source_path.clone()));
                }
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            violations.missing_sources.push((transfer.source_id, transfer.source_path.clone()));
        }
        Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
            violations.unreadable_sources.push((transfer.source_id, transfer.source_path.clone()));
        }
        Err(_) => {
            // Other stat errors — treat as missing
            violations.missing_sources.push((transfer.source_id, transfer.source_path.clone()));
        }
    }
}
```

Note: This uses `std::fs` and `std::fs::File` in the ops layer. This is acceptable because it's a preflight check on source files — the same data plane as `validate_source_state()` which already does filesystem I/O in the ops layer. The ops/fs module provides structured file operations (copy, rename, hash); preflight checks are a different concern that lives in the plan phase.

**Interface layer** (`apply.rs`): Display missing and unreadable sources with appropriate guidance:

```rust
if !v.missing_sources.is_empty() {
    eprintln!("Preflight failed: {} source files are missing.", v.missing_sources.len());
    eprintln!();
    for (_, path) in v.missing_sources.iter().take(10) {
        eprintln!("  Missing: {path}");
    }
    if v.missing_sources.len() > 10 {
        eprintln!("  ... and {} more", v.missing_sources.len() - 10);
    }
    eprintln!();
    eprintln!("Source files have changed since the manifest was generated.");
    eprintln!("Refresh the manifest: canon cluster refresh {}", manifest_display);
    bail!("Aborting due to missing source files");
}

if !v.unreadable_sources.is_empty() {
    eprintln!("Preflight failed: {} source files are not readable.", v.unreadable_sources.len());
    eprintln!();
    for (_, path) in v.unreadable_sources.iter().take(10) {
        eprintln!("  Permission denied: {path}");
    }
    if v.unreadable_sources.len() > 10 {
        eprintln!("  ... and {} more", v.unreadable_sources.len() - 10);
    }
    eprintln!();
    eprintln!("Fix file permissions, then retry.");
    bail!("Aborting due to unreadable source files");
}
```

#### Tests

- `test_plan_detects_missing_source`: Lock entry points to non-existent file → `missing_sources` populated
- `test_plan_detects_unreadable_source` (unix only): Create file, chmod 000, verify `unreadable_sources` populated
- `test_plan_source_preflight_skips_resume_completed`: In resume mode with dest already present, source missing is OK (not in transfers)
- `test_plan_source_is_directory`: Source path is a directory → treated as missing

### Phase 2: Atomic Noclobber Copy

- **Goal**: `copy_file()` uses `create_new(true)` for atomic noclobber — no TOCTOU gap
- **Scope**: `ops/fs.rs`

#### Changes

**Modified `copy_file()`**:

```rust
pub fn copy_file(src: &Path, dest: &Path, noclobber: bool) -> Result<()> {
    let src_meta = fs::metadata(src)
        .with_context(|| format!("Failed to read metadata: {}", src.display()))?;

    if noclobber {
        // Atomic noclobber: create dest with O_EXCL, then copy content manually
        let mut src_file = File::open(src)
            .with_context(|| format!("Failed to open source: {}", src.display()))?;
        let dest_file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)  // O_CREAT | O_EXCL — atomic, fails if exists
            .open(dest)
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::AlreadyExists {
                    anyhow::anyhow!("Destination already exists: {}", dest.display())
                } else {
                    anyhow::anyhow!("Failed to create {}: {}", dest.display(), e)
                }
            })?;
        let mut writer = std::io::BufWriter::new(dest_file);
        std::io::copy(&mut src_file, &mut writer)
            .with_context(|| format!("Failed to copy {} to {}", src.display(), dest.display()))?;
        writer.flush()
            .with_context(|| format!("Failed to flush {}", dest.display()))?;
    } else {
        // Allow overwrite (used internally, e.g., move_file EXDEV fallback)
        fs::copy(src, dest).with_context(|| {
            format!("Failed to copy {} to {}", src.display(), dest.display())
        })?;
    }

    preserve_metadata(dest, &src_meta)?;
    Ok(())
}
```

Key change: when `noclobber=true`, we use `OpenOptions::create_new(true)` which maps to `O_CREAT | O_EXCL` — the kernel atomically rejects creation if the file exists. Then we manually copy content via `io::copy`. When `noclobber=false` (the EXDEV fallback path), we keep using `fs::copy()`.

**Modified `move_file()` EXDEV fallback**: Change `copy_file(src, dest, false)` to `copy_file(src, dest, true)` — the noclobber check already passed at the top of `move_file()`, but by the time we reach the EXDEV fallback, another process could have created the file. Using `noclobber=true` in the fallback closes this gap.

Wait — actually, for the EXDEV fallback: the initial noclobber check at the top of `move_file()` will be replaced by the atomic noclobber in `rename_file()` (Phase 3). But in the EXDEV path, the rename failed with EXDEV, so we know the dest didn't exist at rename time. However, between the failed rename and the copy, another process could create the file. So yes, the EXDEV fallback should use `noclobber=true`.

```rust
// In move_file(), EXDEV fallback:
Err(e) if e.raw_os_error() == Some(libc::EXDEV) => {
    copy_file(src, dest, noclobber)?;  // Was: copy_file(src, dest, false)
    fs::remove_file(src)
        .with_context(|| format!("Failed to delete source: {}", src.display()))?;
    Ok(MoveOutcome::CopiedAndDeleted)
}
```

#### Tests

- `test_copy_file_noclobber_atomic`: Verify that `copy_file(src, dest, true)` when dest already exists returns error containing "already exists" (existing test, behavior preserved but now atomic)
- `test_copy_file_noclobber_creates_file`: Verify content is correctly copied via the new create_new path
- `test_copy_file_noclobber_preserves_metadata`: Verify mtime is preserved with the new code path (existing test should still pass)

### Phase 3: Atomic Noclobber Rename

- **Goal**: `rename_file()` and the initial check in `move_file()` use platform-native atomic noclobber
- **Scope**: `ops/fs.rs`

#### Changes

**New helper function `noclobber_rename()`**:

```rust
/// Rename with atomic noclobber where the platform supports it.
/// Falls back to stat-then-rename on unsupported platforms.
fn noclobber_rename(src: &Path, dest: &Path) -> Result<()> {
    #[cfg(target_os = "macos")]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let src_c = CString::new(src.as_os_str().as_bytes())
            .with_context(|| format!("Invalid source path: {}", src.display()))?;
        let dest_c = CString::new(dest.as_os_str().as_bytes())
            .with_context(|| format!("Invalid dest path: {}", dest.display()))?;

        // renamex_np with RENAME_EXCL — atomic noclobber on macOS
        // RENAME_EXCL = 0x00000004
        let ret = unsafe {
            libc::renamex_np(src_c.as_ptr(), dest_c.as_ptr(), 0x00000004)
        };
        if ret == 0 {
            return Ok(());
        }
        let err = std::io::Error::last_os_error();
        if err.kind() == std::io::ErrorKind::AlreadyExists
            || err.raw_os_error() == Some(libc::EEXIST)
        {
            bail!("Destination already exists: {}", dest.display());
        }
        return Err(err).with_context(|| {
            format!("Failed to rename {} to {}", src.display(), dest.display())
        });
    }

    #[cfg(target_os = "linux")]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let src_c = CString::new(src.as_os_str().as_bytes())
            .with_context(|| format!("Invalid source path: {}", src.display()))?;
        let dest_c = CString::new(dest.as_os_str().as_bytes())
            .with_context(|| format!("Invalid dest path: {}", dest.display()))?;

        // renameat2 with RENAME_NOREPLACE — atomic noclobber on Linux
        // AT_FDCWD = -100, RENAME_NOREPLACE = 1
        let ret = unsafe {
            libc::syscall(
                libc::SYS_renameat2,
                libc::AT_FDCWD,
                src_c.as_ptr(),
                libc::AT_FDCWD,
                dest_c.as_ptr(),
                1u32, // RENAME_NOREPLACE
            )
        };
        if ret == 0 {
            return Ok(());
        }
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::EEXIST)
            || err.raw_os_error() == Some(libc::ENOTEMPTY)
        {
            bail!("Destination already exists: {}", dest.display());
        }
        return Err(err).with_context(|| {
            format!("Failed to rename {} to {}", src.display(), dest.display())
        });
    }

    // Fallback: stat-then-rename (TOCTOU gap, but negligible)
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        if dest.exists() {
            bail!("Destination already exists: {}", dest.display());
        }
        fs::rename(src, dest).with_context(|| {
            format!("Failed to rename {} to {}", src.display(), dest.display())
        })?;
        Ok(())
    }
}
```

**Modified `rename_file()`**:

```rust
pub fn rename_file(src: &Path, dest: &Path, noclobber: bool) -> Result<()> {
    if noclobber {
        noclobber_rename(src, dest)
    } else {
        fs::rename(src, dest).with_context(|| {
            format!("Failed to rename {} to {}", src.display(), dest.display())
        })
    }
}
```

**Modified `move_file()`**:

```rust
pub fn move_file(src: &Path, dest: &Path, noclobber: bool) -> Result<MoveOutcome> {
    let rename_result = if noclobber {
        noclobber_rename(src, dest)
    } else {
        fs::rename(src, dest).with_context(|| {
            format!("Failed to rename {} to {}", src.display(), dest.display())
        })
    };

    match rename_result {
        Ok(()) => Ok(MoveOutcome::Renamed),
        #[cfg(unix)]
        Err(e) => {
            // Check for cross-device error in the chain
            let is_exdev = e.chain().any(|cause| {
                cause
                    .downcast_ref::<std::io::Error>()
                    .and_then(|io_err| io_err.raw_os_error())
                    == Some(libc::EXDEV)
            });
            if is_exdev {
                copy_file(src, dest, noclobber)?;
                fs::remove_file(src)
                    .with_context(|| format!("Failed to delete source: {}", src.display()))?;
                Ok(MoveOutcome::CopiedAndDeleted)
            } else {
                Err(e)
            }
        }
        #[cfg(not(unix))]
        Err(e) => Err(e),
    }
}
```

Note: The EXDEV detection needs care because `noclobber_rename()` wraps the io::Error in anyhow context. We need to check the error chain for the raw EXDEV code, not just the top-level error. The `e.chain().any()` pattern traverses the anyhow error chain.

#### Tests

- `test_rename_file_noclobber_rejects_existing`: Existing test — behavior preserved, now atomic
- `test_rename_file_noclobber_success`: Verify successful rename with new code path
- `test_move_file_noclobber_rejects_existing`: Existing test — behavior preserved
- `test_move_file_exdev_fallback_uses_noclobber`: If possible to trigger EXDEV in test (may need different tempdir mounts), verify the fallback uses noclobber

## Architectural Decisions

| Decision | Rationale |
|----------|-----------|
| Source preflight uses `std::fs` in ops layer | Same precedent as `validate_source_state()` — source file I/O is part of the apply operation, not a repo or domain concern |
| Separate `missing_sources` and `unreadable_sources` violations | Different user actions: missing → refresh manifest, unreadable → fix permissions |
| `noclobber_rename()` as private helper in ops/fs | Platform-specific code encapsulated in one function, three callers (rename_file, move_file, future code) |
| Use `libc` crate directly for platform syscalls | Already a dependency (`move_file` uses `libc::EXDEV`). No need for `nix` crate for two syscall wrappers |
| EXDEV fallback passes `noclobber` through | Closes the TOCTOU gap between failed rename and copy fallback |
| Manual `io::copy` for noclobber copy path | `fs::copy()` doesn't support `O_EXCL`. Manual copy via `create_new(true)` + `io::copy` is the only way to get atomic create |

## Non-Goals

- Changing the `noclobber: bool` parameter interface (callers unchanged)
- Adding noclobber to `compute_partial_hash` or other read-only fs operations
- Making the source preflight configurable (always runs)
- Verifying source content (hash) in preflight — that's `validate_source_state()`'s job during execute

## Test Plan

### Existing Tests (Must Pass)

All existing `ops/fs` tests: `copy_file_success`, `copy_file_preserves_mtime`, `copy_file_noclobber_rejects_existing`, `copy_file_overwrites_without_noclobber`, `copy_file_missing_source`, `rename_file_success`, `rename_file_noclobber_rejects_existing`, `move_file_same_device`, `move_file_noclobber_rejects_existing`.

All existing `ops/apply` plan tests (935 total).

### New Tests

| Test | Type | Phase |
|------|------|-------|
| Plan detects missing source | Integration (ops/apply) | 1 |
| Plan detects unreadable source (unix) | Integration (ops/apply) | 1 |
| Plan source preflight skips resume completed | Integration (ops/apply) | 1 |
| Plan source is directory | Integration (ops/apply) | 1 |
| Copy noclobber creates file correctly | Unit (ops/fs) | 2 |
| Copy noclobber preserves metadata | Unit (ops/fs) | 2 |
| Rename noclobber rejects existing (atomic) | Unit (ops/fs) | 3 |
| Rename noclobber success (atomic) | Unit (ops/fs) | 3 |

## Implementation Checklist

- [ ] Phase 1: Add `missing_sources` and `unreadable_sources` to `ApplyViolations`
- [ ] Phase 1: Add source preflight check in `plan_apply()` after pattern/escape validation
- [ ] Phase 1: Display violations in `apply.rs` with appropriate guidance
- [ ] Phase 1: Tests for missing, unreadable, directory, resume-skip cases
- [ ] Phase 2: Rewrite `copy_file()` noclobber path to use `create_new(true)` + manual copy
- [ ] Phase 2: Update `move_file()` EXDEV fallback to pass `noclobber` through
- [ ] Phase 2: Verify existing copy tests pass with new implementation
- [ ] Phase 3: Add `noclobber_rename()` with macOS/Linux/fallback implementations
- [ ] Phase 3: Update `rename_file()` and `move_file()` to use `noclobber_rename()`
- [ ] Phase 3: Fix EXDEV detection in `move_file()` for anyhow error chain
- [ ] Phase 3: Verify existing rename/move tests pass
- [ ] Verify all 935+ existing tests pass

## Documentation Updates

No user-facing documentation changes needed. Noclobber is invisible in the happy path. The source preflight error messages are self-explanatory. The apply docs page may mention "preflight checks" in a future update as part of the broader epic documentation pass.

## Backward Compatibility

- Source preflight is new behavior: manifests that previously would start transferring (then fail mid-operation on missing sources) will now abort upfront. This is strictly better — no partial state.
- Noclobber error messages may differ slightly ("Destination already exists" stays the same, but the error may come from the kernel instead of Canon's check). Existing error-matching in tests uses `contains("already exists")` which will still match.

## Performance Considerations

- Source preflight: 1 stat + 1 open+close per pending source. On local disk: negligible. On NAS with 1000 files: ~2-5 seconds for the stat+open pass. Acceptable — one-time cost that prevents mid-operation surprises.
- Noclobber copy: Manual `io::copy` via BufWriter is equivalent in performance to `fs::copy()` — both do buffered reads and writes. The `create_new` open adds one syscall (the same `open()` call, just with different flags).
- Noclobber rename: Platform-native syscall is the same performance as `fs::rename()` — one syscall either way.
