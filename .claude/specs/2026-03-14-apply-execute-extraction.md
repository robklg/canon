# Refactoring Spec: Apply Execute Extraction (ADR Phase 2 Step 4)

## Overview

Extract the apply execute path from `apply.rs` (interface) to `ops/apply.rs`. This is ~500 lines of behavioral logic that orchestrates `ops/fs` (copy/rename/move, validate, hash) + `repo` (insert_destination, update_location, mark_missing) for file transfers. After extraction, the interface becomes: parse manifest → build params → plan → present → confirm → execute → format result.

**ADR**: `~/store/canon-architecture/2026-03-13-operations-layer.md` (Phase 2 Step 4)

## Phases

### Phase 1: Execute extraction with progress trait
- **Status**: completed
- **Goal**: Move the entire execute path to ops/apply.rs. Interface calls `execute_apply()` and formats the result.
- **Scope**:
  - Create `TransferProgress` trait in ops/apply.rs
  - Create `ApplyExecuteParams` and `ApplyResult` types
  - Create `execute_apply()` function that encapsulates:
    - Source readability pre-check
    - Resume mode disk classification (`plan_transfers_disk`)
    - Batch pre-transfer staleness validation (`check_source_states_disk`)
    - Per-transfer loop (the current `process_source` logic)
    - Result aggregation
  - Move helper functions to ops:
    - `validate_source_state_from_transfer` → ops/apply.rs (private)
    - `plan_transfers_disk` → ops/apply.rs (private)
    - `build_new_source` → ops/apply.rs (private, both platform variants)
    - `relocate_source` → ops/apply.rs (private)
    - `mark_source_not_present` → ops/apply.rs (private)
  - Move `check_destination_writable` → ops/fs.rs (public)
  - Rewire `apply::run()` to call ops functions
  - Remove moved code from apply.rs
  - Add tests for validate and classify functions
- **Non-goals**: Transaction scope redesign. Manifest I/O changes (step 8). Changing dry-run semantics beyond "don't call execute." Moving `ApplyOptions` (CLI-specific). Moving `filter_by_roots` or confirmation/summary display logic.
- **Dependencies**: Foundation spec complete (ops/fs.rs exists, TransferMode in ops/apply.rs)

#### Types

**TransferProgress trait** — in ops/apply.rs:

```rust
/// Outcome of a single transfer operation.
pub enum TransferOutcome {
    Copied,
    Renamed,
    Moved,
    SkippedMissing,
    SkippedStale(String),  // reason
    Error(String),         // error message
}

/// Progress notification for file transfer operations.
/// The interface implements this to display progress, verbose logging, etc.
/// Fire-and-forget — does not affect the operation's behavior.
pub trait TransferProgress {
    fn on_transfer(&self, index: usize, total: usize, source_path: &str, outcome: &TransferOutcome);
}

/// No-op implementation for tests.
pub struct NoopProgress;
impl TransferProgress for NoopProgress {
    fn on_transfer(&self, _index: usize, _total: usize, _source_path: &str, _outcome: &TransferOutcome) {}
}
```

**ApplyExecuteParams** — in ops/apply.rs:

```rust
pub struct ApplyExecuteParams {
    /// Base directory for destination paths (archive root + base_dir from manifest).
    pub base_dir: PathBuf,
    /// Archive root ID for DB registration.
    pub archive_root_id: i64,
    /// How to transfer files.
    pub transfer_mode: TransferMode,
    /// Whether this is a resume operation.
    pub resume: bool,
}
```

Note: No `dry_run` field. Dry-run is handled by the interface — it calls `plan_apply()`, formats the plan for display, and never calls `execute_apply()`. The current inline dry-run in `process_source` (which prints `[dry-run] COPY: ...`) moves to the interface's dry-run display logic.

Note: No `verbose` field. Verbose per-transfer output routes through the `TransferProgress` trait. The interface implementation decides what to print.

**ApplyResult** — in ops/apply.rs:

```rust
pub struct ApplyResult {
    pub copied: u64,
    pub renamed: u64,
    pub moved: u64,
    pub skipped_missing: u64,
    pub skipped_stale: Vec<StaleSource>,  // reuse existing StaleSource type
    pub errors: Vec<TransferError>,
    /// Resume mode: count of sources already registered in archive DB.
    /// Carried through from ApplyPlan.already_archived_count.
    pub already_archived: u64,
    /// Resume mode: count of files on disk with correct size (need scan, not transfer).
    pub resumed: u64,
}

pub struct TransferError {
    pub path: String,
    pub error: String,
}
```

**ResumeClassification** — private in ops/apply.rs:

```rust
/// Result of disk classification in resume mode.
struct ResumeClassification<'a> {
    /// Transfers that need to be executed (not on disk).
    to_transfer: Vec<&'a ApplyTransfer>,
    /// Count of files on disk with correct size (skipped, need scan).
    resumed: usize,
}

struct SizeMismatchError {
    dest_path: String,
    expected: u64,
    actual: u64,
}
```

#### execute_apply() Function

```rust
/// Execute file transfers from a computed apply plan.
///
/// Performs: source readability checks, resume disk classification (if resume mode),
/// batch staleness validation, and the transfer loop. Each transfer is independent
/// (no transaction wrapping) — the operation is idempotent and resume-safe.
///
/// Does NOT handle: manifest parsing, confirmation prompts, dry-run display,
/// output formatting. These are interface concerns.
pub fn execute_apply(
    conn: &Connection,
    plan: &ApplyPlan,
    params: &ApplyExecuteParams,
    progress: &dyn TransferProgress,
) -> Result<ApplyResult>
```

**Internal flow**:

1. **Source readability pre-check**: Iterate `plan.transfers`, try `File::open()` on each source path. Collect permission-denied/unreadable into error list. Bail if any unreadable (matches current behavior).

2. **Resume classification** (if `params.resume`): Call private `classify_transfers_disk()`. Returns `ResumeClassification` with `to_transfer` and `resumed` count. Bail on size mismatches (with structured error, not eprintln — the interface formats).

3. **Batch staleness validation**: Call private `validate_transfers_disk()` on the transfers to execute. Returns `Vec<StaleSource>`. Bail if any stale (matches current preflight behavior).

4. **Transfer loop**: For each transfer in `to_transfer`:
   a. Notify `progress.on_transfer(i, total, path, &outcome)` after each transfer
   b. Call private `execute_single_transfer()` (refactored from `process_source`)
   c. Accumulate results into `ApplyResult`
   d. On error: collect into `result.errors`, continue (matches current behavior)

5. **Return** `ApplyResult` with all counts and error lists.

**Key behavior changes from current code**:
- No `eprintln!` or `println!` — all output through `TransferProgress` or in the return value
- No dry-run path — the function always executes. Interface handles dry-run.
- Size mismatch errors in resume mode are returned as structured data (Vec<SizeMismatchError>), not printed inline. The function bails with a structured error that the interface can format.

#### execute_single_transfer() — private helper

Refactored from `process_source`. Same logic, minus dry-run path and verbose output:

```rust
fn execute_single_transfer(
    transfer: &ApplyTransfer,
    base_dir: &Path,
    transfer_mode: TransferMode,
    conn: &Connection,
    archive_root_id: i64,
) -> Result<TransferOutcome>
```

**Logic** (unchanged from current process_source):
1. Check source exists → `SkippedMissing`
2. Per-transfer staleness validation → `SkippedStale(reason)`
3. Create parent directories
4. Match on transfer_mode:
   - Copy: noclobber check → `fs::copy` → `preserve_metadata` → `build_new_source` → `insert_destination` → `Copied`
   - Rename: noclobber check → `fs::rename` → `relocate_source` → `Renamed`
   - Move: noclobber check → try `fs::rename`:
     - Success → `relocate_source` → `Renamed`
     - EXDEV → copy+delete → `mark_source_not_present` + `insert_destination` → `Moved`

#### ops/fs.rs Addition

```rust
/// Check if a directory (or its nearest existing ancestor) is writable.
/// Creates and removes a test file to verify write permissions.
pub fn check_destination_writable(base_dir: &Path) -> Result<()> {
    // ... exact code from apply.rs:678-714
}
```

#### Caller Updates (apply.rs interface)

**Before** (current ~510 lines of execute logic in run()):
```rust
// ... confirmation ...
check_destination_writable(&base_dir)?;
// ... readability checks ...
// ... resume classification ...
// ... staleness validation ...
// ... transfer loop with process_source() ...
// ... summary output ...
```

**After** (~60 lines):
```rust
// ... confirmation ...

// Dry-run: display plan and return
if options.dry_run {
    display_dry_run_plan(&plan, &base_dir, options.transfer_mode);
    display_summary_dry_run(&plan, skipped_by_filter);
    return Ok(());
}

// Preflight: check destination writable
ops::fs::check_destination_writable(&base_dir)?;

// Execute
let progress_impl = CliTransferProgress::new(options.verbose);
let result = ops::apply::execute_apply(
    conn,
    &plan,
    &ops::apply::ApplyExecuteParams {
        base_dir: base_dir.clone(),
        archive_root_id: config.output.archive_root_id,
        transfer_mode: options.transfer_mode,
        resume: options.resume,
    },
    &progress_impl,
)?;

// Format result
display_stale_during_transfer(&result.skipped_stale);
display_summary(&result, options.resume, skipped_by_filter);
display_error_recovery(&result, options);

// Update query planner statistics
db.run_analyze()?;
```

The interface creates a `CliTransferProgress` struct that implements `TransferProgress`:

```rust
struct CliTransferProgress {
    verbose: bool,
    progress: Option<Progress>,  // set once total is known from first on_transfer call
}

impl TransferProgress for CliTransferProgress {
    fn on_transfer(&self, index: usize, total: usize, source_path: &str, outcome: &TransferOutcome) {
        // Lazy-init progress spinner on first call
        // Update progress spinner
        // If verbose: print per-transfer action
    }
}
```

Wait — this is awkward. The `Progress` spinner needs `total` at construction time, but `execute_apply` knows the total internally. Two options:

**Option A**: Progress trait gets a `fn on_start(&self, total: usize)` method.
**Option B**: `execute_apply()` returns a `total` count first, or the interface computes it from plan.transfers.len().

**Decision**: Option A — add `on_start` to the trait:

```rust
pub trait TransferProgress {
    /// Called once before the transfer loop begins.
    fn on_start(&self, total: usize);
    /// Called after each transfer completes.
    fn on_transfer(&self, index: usize, total: usize, source_path: &str, outcome: &TransferOutcome);
    /// Called once after the transfer loop ends.
    fn on_finish(&self);
}
```

This cleanly matches the current `Progress::new(total)` → `progress.update(i)` → `progress.finish()` pattern.

Actually — `on_start` and `on_finish` need `&mut self` for the CliTransferProgress to initialize its spinner. But traits with `&mut self` are harder to use (can't share). Let's use interior mutability:

```rust
pub trait TransferProgress {
    fn on_start(&self, total: usize);
    fn on_transfer(&self, index: usize, total: usize, source_path: &str, outcome: &TransferOutcome);
    fn on_finish(&self);
}
```

The CLI implementation uses `RefCell<Option<Progress>>` or `Cell` internally. This keeps the trait simple.

#### Dry-run Changes

Currently, `process_source` handles dry-run inline. After extraction:

- The interface checks `options.dry_run` BEFORE calling `execute_apply()`
- For dry-run, it formats output from `plan.transfers` directly:
  ```rust
  fn display_dry_run_plan(plan: &ApplyPlan, base_dir: &Path, mode: TransferMode) {
      for transfer in &plan.transfers {
          let dest_path = base_dir.join(&transfer.dest_rel_path);
          let label = match mode {
              TransferMode::Copy => "COPY",
              TransferMode::Rename => "RENAME",
              TransferMode::Move => "MOVE",
          };
          // Check if source exists for SKIP display
          if !Path::new(&transfer.source_path).exists() {
              println!("[dry-run] SKIP (missing): {}", transfer.source_path);
          } else {
              println!("[dry-run] {}: {} -> {}", label, transfer.source_path, dest_path.display());
          }
      }
  }
  ```

This is a minor behavior difference: currently dry-run goes through `process_source` which does a per-transfer source-exists check. The new dry-run iterates plan.transfers and does the same check. Output is identical.

#### What Stays in apply.rs

- `ApplyOptions` struct (CLI-specific)
- `run()` function — orchestration, now thinner
- `filter_by_roots()` — CLI-specific root filtering
- Manifest/lock file reading and parsing
- Confirmation prompts and display
- Summary output formatting (`display_summary`, `display_dry_run_plan`, etc.)
- Platform checks (--rename/--move Unix-only warning)
- `CliTransferProgress` implementation

#### What Moves to ops/apply.rs

| Function/Type | From | To | Visibility |
|--------------|------|-----|------------|
| `TransferProgress` trait | new | ops/apply.rs | pub |
| `TransferOutcome` enum | new | ops/apply.rs | pub |
| `NoopProgress` struct | new | ops/apply.rs | pub |
| `ApplyExecuteParams` struct | new | ops/apply.rs | pub |
| `ApplyResult` struct | new | ops/apply.rs | pub |
| `TransferError` struct | new | ops/apply.rs | pub |
| `execute_apply()` | new | ops/apply.rs | pub |
| `execute_single_transfer()` | `process_source` | ops/apply.rs | private |
| `validate_source_state` | `validate_source_state_from_transfer` | ops/apply.rs | private |
| `validate_transfers_disk` | `check_source_states_disk_from_transfers` | ops/apply.rs | private |
| `classify_transfers_disk` | `plan_transfers_disk` | ops/apply.rs | private |
| `relocate_source()` | apply.rs | ops/apply.rs | private |
| `mark_source_not_present()` | apply.rs | ops/apply.rs | private |
| `build_new_source()` | apply.rs (both variants) | ops/apply.rs | private |
| `ResumeClassification` | `DiskWorkPlan` | ops/apply.rs | private |
| `SizeMismatchError` | apply.rs | ops/apply.rs | private |

| Function/Type | From | To | Visibility |
|--------------|------|-----|------------|
| `check_destination_writable()` | apply.rs | ops/fs.rs | pub |

#### Removed from apply.rs

- `ApplyAction` enum (replaced by `TransferOutcome` in ops)
- `ApplyStats` struct (replaced by `ApplyResult` in ops)
- `SkippedStaleSource` struct (uses `StaleSource` from ops)
- `DiskWorkPlan` struct (replaced by `ResumeClassification` in ops)
- `SizeMismatchError` struct (moved to ops)
- `process_source()` function
- `validate_source_state_from_transfer()` function
- `check_source_states_disk_from_transfers()` function
- `plan_transfers_disk()` function
- `relocate_source()` function
- `mark_source_not_present()` function
- `build_new_source()` functions (both platform variants)
- `check_destination_writable()` function

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| Dry-run stays in interface | The interface decides not to call execute. Dry-run output is formatted from the plan. This is cleaner than having execute handle a "don't actually do anything" mode. |
| Progress trait with on_start/on_transfer/on_finish | Matches the current Progress::new/update/finish lifecycle. on_start gives the interface the total count for spinner initialization. |
| NoopProgress for tests | Tests don't need progress output. Concrete type avoids trait object overhead in tests. |
| execute_apply bails on stale sources | Pre-transfer staleness is a hard gate (matches current behavior). Sources that become stale during transfer are collected in the result (also matches current behavior). |
| Size mismatch in resume is a bail | Current behavior: prints error and bails. New behavior: returns structured error that the interface can format, then bails. Same outcome, better separation. |
| No transaction wrapping | Current behavior is per-transfer independent operations. ADR deferred transaction scope redesign. Preserving this. |
| check_destination_writable → ops/fs | Pure filesystem check with no business logic. Called by interface during preflight (before execute). |
| Source readability check inside execute_apply | This is a preflight filesystem check. Moving it inside execute keeps the interface thin — the interface just calls execute, and execute does all necessary validation before transferring. |

## Test Requirements

### Existing Tests
All 731 existing tests must pass. The 24 ops/apply.rs plan tests and 5 domain/apply.rs tests are unaffected.

### New Tests to Add

**ops/fs.rs — check_destination_writable:**
- `check_writable_existing_dir` — writable dir → Ok
- `check_writable_nested_missing` — parent exists and is writable → Ok
- `check_writable_permission_denied` — read-only dir → Err (platform-dependent, may skip on CI)

**ops/apply.rs — validate_source_state:**
- `validate_unchanged_file` — file matches transfer metadata → Ok
- `validate_missing_file` — file doesn't exist → Err("file not found")
- `validate_size_changed` — file size differs → Err containing "size"
- `validate_hash_changed` — file content differs (same size/mtime) → Err containing "partial hash"

**ops/apply.rs — classify_transfers_disk:**
- `classify_available` — dest doesn't exist → in to_transfer
- `classify_resumed` — dest exists, size matches → resumed count
- `classify_size_mismatch` — dest exists, wrong size → error

**ops/apply.rs — execute_single_transfer (integration, uses tempfiles):**
- `execute_copy_creates_file` — source exists, dest doesn't → file copied, metadata preserved
- `execute_copy_noclobber` — dest already exists → error
- `execute_source_missing` — source doesn't exist → SkippedMissing

Defer full end-to-end execute_apply tests (requiring DB + filesystem together) to a follow-up.

## Implementation Checklist
- [x] Add `check_destination_writable` to ops/fs.rs with tests
- [x] Add types to ops/apply.rs (TransferProgress, TransferOutcome, ApplyExecuteParams, ApplyResult, etc.)
- [x] Move helper functions to ops/apply.rs (validate, classify, build_new_source, relocate, mark_missing)
- [x] Implement execute_apply() and execute_single_transfer()
- [x] Rewire apply.rs run() — add dry-run display, CliTransferProgress, call execute_apply
- [x] Remove moved code from apply.rs
- [x] Add tests for validate and classify functions (12 new tests)
- [x] Verify all existing tests pass (743 pass)
- [x] Update CLAUDE.md ops module listing
