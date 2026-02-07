# Spec: Apply Resume Mode and Atomicity Improvements

**Date:** 2026-02-07
**Status:** Draft
**Priority:** Medium
**Predecessor:** `.claude/specs/2026-02-01-write-infrastructure.md`

## Problem Statement

An audit of recent refactorings (since 2026-02-01) revealed gaps between spec and implementation regarding concurrency and atomicity:

### Finding 1: scan.rs — Missing Per-File Transactions

The scan-domain-model spec (D2) stated:
> "Each file's observe-reconcile-persist cycle is wrapped in its own transaction."

However, `process_file()` executes `fetch_by_path → fetch_by_inode → reconcile → apply_reconciliation` as separate statements without transaction wrapping. This creates a theoretical TOCTOU race condition.

**Actual Risk:** Low. Two simultaneous scans of the same root is rare (user error), and both would write valid observed state. The system self-corrects on subsequent scans.

### Finding 2: apply.rs — No Atomicity Between File I/O and DB

The write-infrastructure spec (D2) stated:
> "apply.rs wraps file copy + `insert_destination()` in transaction"

However, `perform_transfer()` does:
```
fs::copy(...) → insert_destination(...)
```

No transaction wrapping. If the DB insert fails after file copy, we have an orphan file on disk with no database record.

**Actual Risk:** Medium. The orphan file will be discovered and registered on next `scan` of the archive, but this is not obvious to users. Re-running `apply` shows "destination already exists" errors for all previously-copied files.

### Finding 3: Poor UX on Interruption or Failure

When `apply` is interrupted (Ctrl+C) or encounters errors:
- No signal handling — process dies without summary
- Re-running shows "destination already exists" errors for completed files
- User sees error count but no clear recovery path
- No way to resume from where we left off

---

## Goals

1. **Make apply resumable** — Add `--resume` flag that gracefully handles partially-completed applies
2. **Clear recovery guidance** — When errors occur, tell users exactly what to do
3. **Close spec gaps** — Add transaction wrapping where it makes sense
4. **Distinguish states clearly** — Summary should show "already done" vs "newly copied" vs "errors"

---

## Apply Phases

The apply command has three distinct phases with clear responsibilities:

```
┌─────────────────────────────────────────────────────────────────┐
│ 1. Preflight         "Can we start?"        → Yes/No + errors  │
├─────────────────────────────────────────────────────────────────┤
│ 2. Work Planning     "What needs doing?"    → Classified work  │
├─────────────────────────────────────────────────────────────────┤
│ 3. Transfer          "Do the work"          → Execute + stats  │
└─────────────────────────────────────────────────────────────────┘
```

### Phase 1: Preflight (validation only)

**Responsibility:** Determine if we can proceed. Returns errors or Ok(()).

**Existing checks (unchanged):**
- Pattern expansion works for all sources
- Sources are accessible
- No stale destination records (DB=present but file missing → error, advise `scan`)
- Archive conflicts (if applicable)

**New check for regular mode:**
- Destination path conflicts: if destination path already exists (in DB or on disk), error

**Destination path conflict checks by mode:**

| Destination State | Regular Mode (Preflight) | `--resume` Mode (Work Planning) |
|-------------------|--------------------------|--------------------------------|
| DB=present, disk=missing | Existing: Error → run scan | Existing: Error → run scan |
| DB=present, disk=exists | **NEW: Error** → path occupied | Skip as `already_archived` |
| DB=missing, disk=exists | **NEW: Error** → orphan file | Skip as `resumed` |
| DB=missing, disk=missing | OK → proceed | OK → `to_transfer` |

**Rationale for checking in preflight (regular mode):**
- Catch all conflicts upfront before any transfers start
- User sees complete picture of problems
- No partial transfers followed by unexpected errors

**Error message suggests `--resume`:**

```
Preflight failed: 47 destination paths already exist.

This may be from a previously interrupted apply. To resume:
  canon apply --resume <manifest>

Or to see what would be skipped:
  canon apply --resume --dry-run <manifest>

If these are unexpected conflicts, run `canon scan <archive>` to update the database.
```

**Does NOT:** Classify work for `--resume` (that's Work Planning's job).

### Phase 2: Work Planning (new)

**Responsibility:** Classify sources into work categories.

| Mode | Behavior |
|------|----------|
| Regular | All sources go into `to_transfer` |
| `--resume` | Check each source's destination status, classify accordingly |

**Resume classification logic:**

| Destination State | Classification |
|-------------------|----------------|
| Registered in DB (present=1) | `already_archived` — fully complete |
| File exists on disk, size matches | `resumed` — needs scan to register |
| File exists on disk, size differs | **Error** — possible partial copy |
| Not in DB, not on disk | `to_transfer` — needs copying |

**Error handling:** If size mismatches are found (partial/corrupted files), collect all of them and fail after scanning all sources. This gives the user a complete picture:

```
Work planning found 2 partial/mismatched files:
  /archive/2024/photo1.jpg (expected 4521234 bytes, found 2048000)
  /archive/2024/video.mp4 (expected 150000000 bytes, found 89432100)

These may be from an interrupted transfer. To resolve:
  1. Delete the partial files
  2. Re-run: canon apply --resume <manifest>
```

**Returns:** `WorkPlan` containing:
- `to_transfer: Vec<&LockEntry>` — sources to process
- `already_archived: usize` — count skipped (in DB)
- `resumed: usize` — count skipped (on disk, not in DB)

### Phase 3: Transfer (execution)

**Responsibility:** Execute transfers from the work plan.

Operates only on `work_plan.to_transfer`. For each source:
- Copy/rename/move file
- Register in DB
- Handle errors

| Scenario | Behavior |
|----------|----------|
| Destination file exists on disk | **Error**: "Destination already exists" |

Note: In `--resume` mode, this error should not occur — such sources were classified in Work Planning. If it does occur, it indicates a race condition (file appeared between planning and transfer).

### Summary Output

| Mode | Example |
|------|---------|
| Regular | `Applied: 65 copied, 0 errors` |
| `--resume` | `Applied (--resume): 65 copied, 423 already archived, 12 resumed, 0 errors` |

When `--resume` reports "resumed" files, an advisory is shown:
```
Note: 12 resumed files are not yet registered. Run `canon scan <archive>` to complete.
```

---

## Architectural Decisions

### D1: Three-Phase Apply Structure

**Applies to:** Both regular and `--resume` modes

**Decision:** Apply command has three distinct phases with clear responsibilities:
1. **Preflight** — validation only, go/no-go decision
2. **Work Planning** — classify sources into work categories
3. **Transfer** — execute on the classified work

**Rationale:**
- Separation of concerns: validation vs planning vs execution
- Preflight stays focused on "can we proceed?"
- Work Planning encapsulates resume logic without polluting other phases
- Each phase is independently testable

### D2: Preflight Checks Destination Conflicts (Regular Mode)

**Applies to:** Regular mode only (in `--resume` mode, Work Planning handles this)

**Decision:** In regular mode, preflight checks if any destination path is already occupied (in DB or on disk) and fails with an error if so.

**Rationale:**
- Catch all conflicts upfront before any transfers start
- User sees complete picture of problems, not one-at-a-time during transfer
- Consistent with other preflight checks (fail fast, fix once, retry)
- Uses same `batch_check_paths_exist()` repo function as `--resume` mode

**Distinction from stale destination records check:**
- Stale check: DB=present but file missing → error (DB out of sync)
- New check: DB=present AND file exists → error (path occupied)
- New check: DB=missing but file exists → error (orphan file, run scan)

### D3: Work Planning Uses DB-First, Then Disk Check (Resume Mode)

**Applies to:** `--resume` mode only (in regular mode, preflight catches conflicts per D2)

**Decision:** When `--resume` is specified, Work Planning checks destinations in this order:
1. **DB check**: Is destination path registered in archive with present=1?
2. **Disk check** (only if not in DB): Does file exist on disk?

**Rationale:**
- DB record means full operation completed (copy + registration)
- File on disk without DB record means copy completed but registration failed
- This distinguishes "fully done" from "needs scan to complete registration"

**State Detection:**

| State | File on Disk | DB Record | Detection | Classification |
|-------|--------------|-----------|-----------|----------------|
| A | No | No | Neither check finds it | `to_transfer` |
| B | Yes | Yes | DB check finds it | `already_archived` |
| C | Yes (correct size) | No | Disk check finds it | `resumed` |
| D | Yes (wrong size) | No | Disk check, size mismatch | Error (collected, fail after all checked) |

### D4: DB Check Does Not Require Disk Verification

**Applies to:** `--resume` mode only

**Decision:** If the DB says a destination exists (present=1), we trust it and skip without checking disk.

**Rationale:**
- DB record with present=1 means the full copy+register operation completed
- If file was deleted after registration, that's a "stale record" problem
- Stale records are already detected by existing `check_stale_destination_records()` in Preflight
- Adding redundant disk checks would slow down resume for large manifests

### D5: Resumed Files Without DB Records Need Scan

**Applies to:** `--resume` mode only

**Decision:** Files found on disk but not in DB (state C) are skipped during apply, but user is advised to run `scan` to complete registration.

**Rationale:**
- The file is there — the copy succeeded
- Forcing re-copy would be wasteful and potentially destructive
- Registration can happen via normal `scan` of the archive
- Clear messaging tells user what action is needed

### D6: Size Check Is Sufficient for Resume Matching

**Applies to:** `--resume` mode only

**Decision:** When checking if an existing file matches the expected source, compare size only.

**Rationale:**
- If file exists with exactly the expected size, it's almost certainly the copied file
- Partial copies will have wrong size (smaller)
- Different files at same path with exact same size is extremely unlikely
- Partial hash comparison would require reading file content, slowing work planning

**Future option:** Could add `--resume --verify` for partial hash comparison if needed.

### D7: Add Transaction Wrapping to scan.rs

**Applies to:** scan.rs (not apply.rs)

**Decision:** Wrap the `process_file` operations in a transaction.

**Rationale:**
- The original scan-domain-model spec intended this
- SQLite transactions are cheap
- Provides theoretical correctness even if practical risk is low
- Closes the gap between spec and implementation

### D8: Do Not Add Transactions Around apply.rs File+DB

**Applies to:** Both regular and `--resume` modes

**Decision:** Keep file I/O and DB operations separate (no transaction), but make the operation resumable via `--resume`.

**Rationale:**
- True atomicity between filesystem and database is impossible
- Transaction around DB ops only doesn't help — the file I/O is the risky part
- Making the operation idempotent/resumable is more valuable than partial atomicity
- Intent-logging (future enhancement) would be the proper solution for true crash recovery

### D9: Work Planning Runs Before Source Validation

**Applies to:** Both regular and `--resume` modes

**Decision:** Work planning (which determines which sources need transfer) runs before source file validation. Source validation only checks sources that will actually be transferred.

**Rationale:**
- Enables `--resume` to work correctly with `--rename`/`--move` modes
- If a rename/move succeeded but DB registration failed, the source file is gone
- By running work planning first, we can skip sources whose destinations already exist
- Source validation would fail on missing sources, blocking resume unnecessarily
- In regular mode, all sources need transfer anyway, so order doesn't matter

**Example scenario:**
1. User runs `apply --rename`, file A is renamed to destination
2. DB registration fails (crash, error, etc.)
3. User runs `apply --resume --rename`
4. Work planning sees destination exists with correct size → classified as "resumed"
5. Source validation skips file A (not in `to_transfer`)
6. Resume succeeds, user advised to run `scan` on archive

---

## Implementation

### Phase 1A: Preflight Destination Conflicts

**Status:** completed

**Goal:** Catch destination conflicts upfront in regular mode before any transfers start. This delivers immediate value and establishes the `batch_check_paths_exist()` foundation used by Phase 1B.

**Scope:**
1. Add `batch_check_paths_exist()` to `repo/source.rs` — batch check for registered destinations
2. Add `check_destination_conflicts()` to preflight in `apply.rs` — error if dest occupied (DB or disk)
3. Error message suggests `--resume` (preparing for Phase 1B)

**New repo function:**

```rust
/// Check which destination paths are already registered in an archive.
///
/// # Returns
/// Set of rel_paths that exist in the archive with present=1.
pub fn batch_check_paths_exist(
    conn: &Connection,
    archive_root_id: i64,
    rel_paths: &[&str],
) -> Result<HashSet<String>>
```

**Preflight error message:**

```
Preflight failed: 47 destination paths already exist.

This may be from a previously interrupted apply. To resume:
  canon apply --resume <manifest>

Or to see what would be skipped:
  canon apply --resume --dry-run <manifest>

If these are unexpected conflicts, run `canon scan <archive>` to update the database.
```

### Phase 1B: Resume Mode Core

**Status:** completed

**Goal:** Add `--resume` flag with full work planning phase and domain layer extraction.

**Scope:**
1. Add `--resume` flag to CLI (`main.rs`)
2. Add `resume: bool` field to `ApplyOptions`
3. Add `domain/apply.rs` with pure `DestinationState` enum and `classify_destination()` function
4. Add `plan_transfers()` orchestration function in `apply.rs`
5. Refactor `run()` to use three-phase structure: preflight → plan → transfer
6. Update stats to track `already_archived` and `resumed` counts
7. Update summary output to show new categories
8. Add advisory message when resumed files need scan

**New domain types (`domain/apply.rs`):**

```rust
/// The state of a destination path for resume classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DestinationState {
    /// Not in DB, not on disk — needs transfer
    Available,
    /// In DB with present=1 — fully archived
    Archived,
    /// On disk but not in DB, size matches — resumed (needs scan)
    Resumed,
    /// On disk but not in DB, size mismatch — partial/corrupted
    SizeMismatch { expected: u64, actual: u64 },
}

/// Classify a destination's state for resume mode.
///
/// This is a pure function — all I/O happens in the caller.
///
/// # Arguments
/// * `in_db` - Whether the destination path exists in DB with present=1
/// * `on_disk` - If file exists on disk: Some(actual_size), else None
/// * `expected_size` - The expected file size from the manifest
pub fn classify_destination(
    in_db: bool,
    on_disk: Option<u64>,
    expected_size: u64,
) -> DestinationState {
    if in_db {
        DestinationState::Archived
    } else if let Some(actual_size) = on_disk {
        if actual_size == expected_size {
            DestinationState::Resumed
        } else {
            DestinationState::SizeMismatch {
                expected: expected_size,
                actual: actual_size,
            }
        }
    } else {
        DestinationState::Available
    }
}

impl DestinationState {
    pub fn needs_transfer(&self) -> bool {
        matches!(self, DestinationState::Available)
    }
}
```

**Work Planning function (`apply.rs`):**

```rust
struct WorkPlan<'a> {
    to_transfer: Vec<&'a LockEntry>,
    already_archived: usize,
    resumed: usize,
}

/// Plan which transfers need to be executed.
///
/// In regular mode: all sources go to `to_transfer`.
/// In resume mode: check DB and disk, classify accordingly using
/// `domain::apply::classify_destination()`.
///
/// # Errors
/// Returns error if size mismatches found (partial files).
fn plan_transfers<'a>(
    conn: &Connection,
    sources: &[&'a LockEntry],
    pattern: &Pattern,
    // ... other params for pattern evaluation
    archive_root_id: i64,
    base_dir: &Path,
    resume: bool,
) -> Result<WorkPlan<'a>>
```

**Refactored run() structure:**

```rust
pub fn run(db: &mut Db, manifest_path: &Path, options: &ApplyOptions) -> Result<()> {
    // Load and parse manifest
    let sources = load_manifest(...)?;

    // 1. Preflight: validation only
    preflight_validate(&sources, ...)?;

    // 2. Work Planning: classify sources
    let work_plan = plan_transfers(&sources, options.resume, ...)?;

    // Report plan
    if options.resume {
        eprintln!("Already archived: {}", work_plan.already_archived);
        eprintln!("Resumed (need scan): {}", work_plan.resumed);
        eprintln!("To transfer: {}", work_plan.to_transfer.len());
    }

    // 3. Transfer: execute
    let stats = execute_transfers(&work_plan.to_transfer, ...)?;

    // 4. Summary
    print_summary(&work_plan, &stats, options.resume);
}
```

**Expected output with `--resume`:**

```
Already archived: 423
Resumed (need scan): 12
To transfer: 65

Processing 65 sources...

Applied (--resume): 65 copied, 423 already archived, 12 resumed, 0 errors
Note: 12 resumed files are not yet registered. Run `canon scan <archive>` to complete.
```

### Phase 2: Recovery Guidance

**Status:** completed

**Scope:**
1. When `stats.errors > 0`, print recovery guidance
2. Mention `--resume` flag in the guidance
3. Explain the scan → refresh → apply cycle if needed

**Recovery message:**

```
Applied: 50 copied, 0 renamed, 0 moved, 5 errors

Some files failed to transfer. To recover:
  1. Fix any reported errors (permissions, disk space, etc.)
  2. Re-run with --resume: canon apply --resume <manifest>
     This will skip files that were already copied.

If source files changed during apply:
  1. Scan the sources: canon scan <source-paths>
  2. Refresh manifest: canon cluster refresh <manifest.toml>
  3. Re-apply: canon apply <manifest.lock>
```

### Phase 3: Scan Transaction Wrapping

**Status:** completed

**Scope:**
1. Modify `process_file()` in `scan.rs` to wrap operations in transaction
2. Change function signature to take `&Connection` that supports transactions (or pass `&mut Connection`)
3. Verify tests still pass

**Implementation:**

```rust
fn process_file(
    conn: &Connection,
    // ... params
) -> Result<ProcessResult> {
    let tx = conn.unchecked_transaction()?;  // Or use savepoint

    let source_at_path = repo::source::fetch_by_path(&tx, root_id, rel_path)?;
    let source_by_inode = repo::source::fetch_by_inode(&tx, device, inode)?;

    let reconciliation = reconcile(&observation, source_at_path.as_ref(), source_by_inode.as_ref());

    if reconciliation.needs_partial_hash() {
        observation.partial_hash = Some(compute_partial_hash(full_path, size as u64)?);
    }

    let source = repo::source::apply_reconciliation(&tx, &observation, &reconciliation, now)?;

    tx.commit()?;

    // ... rest of function
}
```

**Note:** Using `unchecked_transaction()` or savepoints because the outer scan loop may have its own connection handling.

### Phase 4: Documentation

**Status:** completed

**Scope:**
Update `docs/src/commands/archive/apply.md` with:

1. **New `--resume` flag section:**
   ```markdown
   # Resume interrupted transfers
   canon apply manifest.toml --resume
   ```
   Explain what it does, when to use it, and the scan advisory.

2. **Update pre-flight checks section:**
   Add "Destination path conflicts" to the list — paths already occupied in archive.

3. **Add recovery guidance section:**
   ```markdown
   **Recovering from interrupted apply:**

   If apply is interrupted or encounters errors:
   1. Re-run with `--resume`: `canon apply --resume manifest.toml`
      This skips files that were already copied.

   If `--resume` reports "resumed" files:
   - These files exist on disk but aren't registered in the database
   - Run `canon scan <archive>` to complete registration
   ```

4. **Clarify existing stale records behavior:**
   Explain that if apply reports "stale destination records", user should run `scan` on the archive first.

### Future: Signal Handling (Deferred)

**Status:** deferred (not part of current implementation)

**Scope:**
- Add `ctrlc` crate dependency
- Register signal handler at start of apply
- On Ctrl+C: finish current file, print summary, exit gracefully
- Include "interrupted" in summary output

**Rationale for deferral:** With `--resume` working, Ctrl+C is less problematic — user can simply re-run with `--resume`. Signal handling is a nice-to-have improvement.

---

## Pre-Implementation Review Findings

*Added 2026-02-07 after panel review of existing codebase.*

### Current `apply.rs` Structure

The `run()` function already has semi-structured phases, making the three-phase refactor straightforward:

| Current Phase | Lines | Description |
|--------------|-------|-------------|
| Setup | 124-213 | Load manifest, parse config, cache roots |
| Preflight | 241-421 | 8 separate checks, each with `eprint!` |
| Processing | 431-469 | Loop over sources, call `process_source()` |
| Summary | 471-494 | Print stats, run analyze |

**Implication:** Refactoring to formal three-phase structure is minimal — we're adding a new preflight check and inserting work planning, not restructuring from scratch.

### `LockEntry` Has Size Field

Confirmed that `LockEntry` (in `cluster.rs`) includes `size: i64`, which provides the expected size for resume comparison. No manifest format changes needed.

### Test Coverage Baseline

| Area | Current Tests |
|------|---------------|
| `apply.rs` | **None** — no `#[test]`, no test module |
| `repo/source.rs` | Yes — `batch_fetch_by_roots`, `batch_fetch_by_ids`, etc. |
| `domain/scan.rs` | Yes — `reconcile()` unit tests |
| `domain/apply.rs` | Does not exist yet (we create it) |

**Implication:** We are adding test coverage, not risking breaking existing tests. All new functionality will have tests.

### Reuse Opportunity: `check_stale_destination_records`

The existing `check_stale_destination_records()` function (lines 739-795) uses inline SQL to batch-check paths. The new `batch_check_paths_exist()` repo function could potentially simplify this existing code. **Not in scope for this spec**, but noted for future cleanup.

### Tech Debt: Inline SQL in `apply.rs`

Lines 196-202 and 773-794 contain inline SQL, violating our strict layer separation. **Not in scope for this spec**, but should be migrated to repo layer in a future refactoring.

### Optimization Opportunity: Destination Path Caching

Pattern evaluation (to compute destination paths) happens multiple times:
- `validate_pattern_expansions()`
- `check_destination_collisions_filtered()`
- `check_stale_destination_records()`
- `process_source()` (per file)

For `--resume`, work planning will compute destination paths again. Consider caching evaluated paths to avoid redundant computation. **Not blocking for MVP**, but noted for performance optimization.

---

## Test Requirements

### Existing Test Infrastructure

Tests follow the pattern established in `repo/source.rs`:
- Use `open_in_memory_for_test()` from `repo/mod.rs`
- Use `insert_test_root()` helper for creating test roots
- Test functions are in `mod tests` blocks within source files

### Phase 1A Tests

**Repo layer (`batch_check_paths_exist`):**

| Test | Description |
|------|-------------|
| `batch_check_paths_exist_empty_input` | Empty slice returns empty set |
| `batch_check_paths_exist_none_found` | Paths not in DB return empty set |
| `batch_check_paths_exist_all_found` | All paths in DB returns all |
| `batch_check_paths_exist_mixed` | Some present, some not — only present returned |
| `batch_check_paths_exist_ignores_not_present` | Records with present=0 excluded |
| `batch_check_paths_exist_handles_999_paths` | Just under batch size boundary |
| `batch_check_paths_exist_handles_1000_paths` | Exact batch size boundary |
| `batch_check_paths_exist_handles_1001_paths` | Just over batch size, requires 2 batches |

**Preflight destination conflicts:**

| Test | Description |
|------|-------------|
| `preflight_detects_dest_in_db_and_on_disk` | Dest registered + file exists → error |
| `preflight_detects_orphan_on_disk` | File exists but not in DB → error |
| `preflight_collects_multiple_conflicts` | Reports all conflicts, not just first |
| `preflight_error_suggests_resume` | Message includes `--resume` suggestion |
| `preflight_passes_when_no_conflicts` | Clean destinations → success |

### Phase 1B Tests

**Domain layer (`classify_destination`):**

| Test | Description |
|------|-------------|
| `classify_not_in_db_not_on_disk` | Returns `Available` |
| `classify_in_db` | Returns `Archived` regardless of disk state |
| `classify_on_disk_size_matches` | Returns `Resumed` |
| `classify_on_disk_size_smaller` | Returns `SizeMismatch` (partial copy scenario) |
| `classify_on_disk_size_larger` | Returns `SizeMismatch` (different file scenario) |

**Work Planning (`plan_transfers`):**

| Test | Description |
|------|-------------|
| `plan_transfers_regular_mode_all_to_transfer` | Without --resume, all sources in to_transfer |
| `plan_transfers_resume_already_archived` | Dest in DB → classified as already_archived |
| `plan_transfers_resume_file_on_disk` | Dest on disk (not in DB), size matches → classified as resumed |
| `plan_transfers_resume_size_mismatch_errors` | Dest on disk, size differs → collected in errors |
| `plan_transfers_resume_multiple_mismatches` | All mismatches collected before failing |
| `plan_transfers_resume_to_transfer` | Dest not in DB, not on disk → in to_transfer |

**Integration (end-to-end `--resume`):**

*Note: Integration tests require filesystem setup (temp directories, actual files). These are more complex than unit tests but essential for verifying end-to-end behavior.*

| Test | Description |
|------|-------------|
| `apply_resume_skips_already_archived` | Full flow: dest in DB → skipped, counted correctly |
| `apply_resume_skips_file_on_disk` | Full flow: dest on disk with matching size → skipped |
| `apply_resume_errors_on_size_mismatch` | Full flow: size mismatch → fails with guidance |
| `apply_resume_summary_shows_categories` | Output shows already_archived, resumed, copied separately |
| `apply_resume_advises_scan` | When resumed > 0, shows scan advisory |

**Deferred:** Integration tests are deferred to after unit tests are passing. The domain layer (`classify_destination`) and repo layer (`batch_check_paths_exist`) unit tests provide the core correctness guarantees.

### Phase 2 Tests

| Test | Description |
|------|-------------|
| `apply_errors_show_recovery_guidance` | When errors > 0, recovery message printed |
| `apply_no_errors_no_guidance` | When errors = 0, no recovery message |

### Phase 3 Tests

*Note: The transaction wrapping is primarily for theoretical correctness (preventing TOCTOU races). Testing concurrent modification is complex and the practical risk is low. Focus on verifying the code structure is correct.*

| Test | Description |
|------|-------------|
| `process_file_wraps_in_transaction` | Code review / inspection that transaction wrapping is in place |

**Rationale:** Since there's only one write operation (`apply_reconciliation`), there are no "partial writes" to roll back. The transaction ensures reads and write are atomic. Verifying this via automated tests is difficult without simulating concurrent access, which is fragile. A code review confirming the transaction structure is sufficient.

---

## Edge Cases

### Resume with Modified Source

If a source file changed since the manifest was generated:
1. The existing stale check (`check_sources_still_valid`) catches this
2. File is skipped with "stale" status
3. This works the same with or without `--resume`

### Resume with Deleted Destination

If a destination file was copied, registered, then deleted:
1. The existing `check_stale_destination_records` catches this in preflight
2. Error is raised about stale destination records
3. User advised to scan archive and refresh manifest

### Partial Copy Detection

If copy was interrupted mid-write:
1. File exists on disk with smaller size than expected
2. Resume mode detects size mismatch
3. Error: "File exists with size X, expected Y (possible partial copy)"
4. User should delete the partial file and re-run

### Multiple `--resume` Runs

If user runs `--resume` multiple times:
1. First run: copies some files, registers them
2. Second run: those files now in DB, skipped as "already archived"
3. Fully idempotent — safe to run repeatedly

---

## Migration / Compatibility

- No schema changes required
- `--resume` is opt-in, default behavior unchanged
- No changes to manifest format
- Backward compatible with existing manifests

---

## Documentation Updates

Update `docs/src/commands/archive/apply.md` with:

1. **New `--resume` flag section:**
   ```markdown
   # Resume interrupted transfers
   canon apply manifest.toml --resume
   ```
   Explain what it does, when to use it, and the scan advisory.

2. **Update pre-flight checks section:**
   Add "Destination path conflicts" to the list — paths already occupied in archive.

3. **Add recovery guidance section:**
   ```markdown
   **Recovering from interrupted apply:**

   If apply is interrupted or encounters errors:
   1. Re-run with `--resume`: `canon apply --resume manifest.toml`
      This skips files that were already copied.

   If `--resume` reports "resumed" files:
   - These files exist on disk but aren't registered in the database
   - Run `canon scan <archive>` to complete registration
   ```

4. **Clarify existing stale records behavior:**
   Explain that if apply reports "stale destination records", user should run `scan` on the archive first.

---

## Open Questions

1. **Should `--resume` be the default?**
   - Pro: Safer, always resumable
   - Con: Might hide accidental overwrites
   - Current decision: Opt-in via flag

2. **Should we verify partial hash for resumed files?**
   - Pro: Higher confidence file is correct
   - Con: Slower preflight, requires reading file content
   - Current decision: Size check only, could add `--verify` later

3. **What about files in DB but not on disk (state E)?**
   - This is caught by existing stale destination record check
   - No special handling needed for `--resume`

---

## Related Documents

- `.claude/specs/2026-02-01-write-infrastructure.md` — Original write infrastructure spec
- `.claude/specs/2026-02-01-scan-domain-model.md` — Scan refactoring spec (D2: transactions)
- `CLAUDE.md` — Architecture documentation

