# Story: Signal Handling

**Design Spec**: [~/store/claude-designs/2026-03-29-apply-safety-and-recovery.md](~/store/claude-designs/2026-03-29-apply-safety-and-recovery.md) (Story 4)
**Epic**: [epic-apply-safety-and-recovery.md](epic-apply-safety-and-recovery.md)
**Status**: Complete
**Created**: 2026-03-29

## Objective

Ctrl+C during apply currently kills the process with no cleanup, potentially leaving filesystem and database out of sync. The two-tier model: first Ctrl+C finishes the current file + DB write then stops cleanly; second Ctrl+C hard-aborts (OS default), leaving at most an orphan file on disk without a DB record — the safe state.

## Functional Requirements Summary

- First Ctrl+C: display "Interrupt received, finishing current file..." on stderr, complete in-flight file + DB write, stop. Print resume hint.
- Second Ctrl+C: hard abort (OS default termination). No DB record for interrupted file.
- Invariant: DB never claims a file exists when it doesn't. Orphan files on disk (no DB record) are safe — detectable by resume.
- Summary reflects actual completions: "Applied: 3 copied, 0 errors. Interrupted — 7 files remaining."
- Ctrl+C during preflight or confirmation: immediate abort (standard behavior, no special handling needed).

## Current State

**No signal handling exists.** Ctrl+C kills the process immediately via OS default SIGINT. If a file copy is in progress, it may be incomplete. If the DB write hasn't happened yet, no orphan record. If the DB write is in progress, SQLite's transaction handling should roll back (write-ahead log), but this is uncontrolled.

**Transfer loop** (`execute_apply()` in ops/apply.rs): Iterates over transfers, calls `execute_single_transfer()` per file. Errors are caught as `TransferOutcome::Error` and loop continues. DB writes happen strictly after file operations (existing invariant).

**Dependencies**: Neither `signal-hook` nor `ctrlc` is in Cargo.toml. `libc` is already a dependency.

## Design

### Phase 1: Interrupt Flag and Loop Check

- **Goal**: First Ctrl+C stops the transfer loop cleanly after the current file completes
- **Scope**: `Cargo.toml` (new dependency), `ops/apply.rs` (interrupt check in execute loop)

#### Changes

**New dependency in `Cargo.toml`**:

```toml
signal-hook = "0.3"
```

`signal-hook` is chosen because it supports registering a flag on SIGINT and restoring the default handler after the first signal — exactly the two-tier model. The `signal_hook::flag::register_conditional_default()` function does this: first signal sets the flag, subsequent signals get default OS behavior.

Alternatively, `signal_hook::flag::register()` sets the flag on every signal. Combined with `signal_hook::low_level::register()` to restore the default handler after first signal. The simplest approach: use `register()` with an `AtomicBool`, and after detecting the flag in the loop, the process exits normally — no need to explicitly restore the default handler because we exit cleanly. For the second Ctrl+C scenario (hard abort during a long file copy), use `register_conditional_default()` which sends default SIGINT when the flag is already set.

**New function in `ops/apply.rs`**:

```rust
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Set up two-tier Ctrl+C handling for the transfer loop.
/// Returns an Arc<AtomicBool> that becomes true on first SIGINT.
/// Second SIGINT gets default OS termination.
fn setup_interrupt_flag() -> Result<Arc<AtomicBool>> {
    let flag = Arc::new(AtomicBool::new(false));
    // register_conditional_default: sets flag on first SIGINT,
    // restores default handler so second SIGINT kills the process
    signal_hook::flag::register_conditional_default(
        signal_hook::consts::SIGINT,
        Arc::clone(&flag),
    )?;
    Ok(flag)
}
```

**Modified `execute_apply()` signature** — add interrupt flag setup at the top:

```rust
pub fn execute_apply(
    conn: &Connection,
    plan: &ApplyPlan,
    params: &ApplyExecuteParams,
    progress: &dyn TransferProgress,
) -> Result<ApplyResult> {
    let interrupt_flag = setup_interrupt_flag()?;
    // ... existing setup ...
```

**Modified transfer loop** — check flag between iterations:

```rust
for (i, transfer) in transfers_to_execute.iter().enumerate() {
    let outcome = match execute_single_transfer(/* ... */) {
        Ok(outcome) => outcome,
        Err(e) => TransferOutcome::Error(e.to_string()),
    };

    // ... existing count updates and progress callback ...

    // Check interrupt flag AFTER the complete unit of work (file + DB + count)
    if interrupt_flag.load(Ordering::Relaxed) {
        result.interrupted = true;
        result.remaining = total - (i + 1);
        break;
    }
}
```

**New fields in `ApplyResult`**:

```rust
pub struct ApplyResult {
    // ... existing fields ...
    /// Whether the operation was interrupted by Ctrl+C.
    pub interrupted: bool,
    /// Number of files remaining when interrupted.
    pub remaining: usize,
}
```

**`TransferProgress` trait** — new callback for interrupt notification:

```rust
pub trait TransferProgress {
    fn on_start(&self, total: usize);
    fn on_transfer(&self, index: usize, total: usize, source_path: &str, dest_path: &str, outcome: &TransferOutcome);
    fn on_interrupt(&self);  // NEW
    fn on_finish(&self);
}
```

Call `progress.on_interrupt()` when the flag is detected, before breaking the loop. This lets the CLI display "Interrupt received, finishing current file..." — although by this point the file is already finished (we check after completion). The message is more accurately displayed from the signal handler or from the interface layer after execute returns.

Actually, the "Interrupt received, finishing current file..." message should appear immediately when Ctrl+C is pressed, not after the file finishes. The signal handler can't easily print (async-signal-safe constraints), but `signal-hook` supports writing to a pipe/fd. The simpler approach: the interface layer (`apply.rs`) sets up the signal with a message output:

**Revised approach**: The ops layer sets up the flag and checks it. The interface layer handles the user-facing message. After `execute_apply()` returns with `result.interrupted == true`, the interface prints the interrupted summary and resume hint. The "finishing current file..." message is harder — it requires knowing the signal was received while a transfer is in progress. The pragmatic solution: `on_interrupt()` callback is called when the flag is detected, which is right after the current file finishes. At that point the CLI can print the message. It won't appear during the file operation, but the user sees it immediately after.

For the truly immediate message: use `signal_hook::low_level::pipe::register()` to write a byte to a pipe on SIGINT, then have the progress display check the pipe. But this adds significant complexity for a cosmetic improvement. The pragmatic approach (message after current file completes) is sufficient — the user sees the message within seconds (or instantly for renames).

#### Tests

Signal handling is difficult to unit test (requires sending real signals). The testable parts:

- `test_execute_apply_respects_interrupt_flag`: Manually set the `AtomicBool` before calling execute, verify `result.interrupted == true` and `result.remaining` is correct. This requires making the interrupt flag injectable rather than always set up internally.

**Revised approach for testability**: `execute_apply` takes an optional `Arc<AtomicBool>` parameter, or we extract the loop into a testable function. Simpler: add an `interrupt_flag` field to `ApplyExecuteParams`:

```rust
pub struct ApplyExecuteParams {
    // ... existing fields ...
    /// Interrupt flag — set to true to stop after current transfer.
    /// If None, signal handling is set up automatically.
    pub interrupt_flag: Option<Arc<AtomicBool>>,
}
```

In `execute_apply()`: if `params.interrupt_flag` is Some, use it; otherwise call `setup_interrupt_flag()`. Tests pass a pre-set flag; production code passes None.

### Phase 2: Interface Layer — Interrupted Summary and Resume Hint

- **Goal**: Display interrupt message, interrupted summary, and resume hint
- **Scope**: `apply.rs` (interface layer)

#### Changes

**`CliTransferProgress::on_interrupt()`**:

```rust
fn on_interrupt(&self) {
    // Finish the progress spinner cleanly
    if let Some(ref p) = *self.progress.borrow() {
        p.finish();
    }
    eprintln!();
    eprintln!("Interrupt received, stopping after current file.");
}
```

**Modified summary display in `apply.rs`** — after `execute_apply()` returns:

```rust
if result.interrupted {
    // Print interrupted summary
    let completed = result.copied + result.renamed + result.moved;
    eprintln!(
        "Applied: {} copied, {} renamed, {} moved, {} errors. Interrupted — {} files remaining.",
        result.copied, result.renamed, result.moved, result.errors.len(), result.remaining
    );
    eprintln!("Resume with: canon apply --resume {}", manifest_display);
} else {
    // Existing summary display
}
```

**Pass `interrupt_flag: None`** from `apply.rs` when constructing `ApplyExecuteParams` (production code lets execute_apply set up the signal handler).

#### Tests

- Existing summary display tests must continue to pass
- `test_interrupted_summary_format`: Construct an `ApplyResult` with `interrupted: true`, verify the summary output format (if summary display is extracted to a function)

## Architectural Decisions

| Decision | Rationale |
|----------|-----------|
| `signal-hook` crate, not `ctrlc` | Supports `register_conditional_default` for two-tier model — first signal sets flag, second signal gets OS default. `ctrlc` only supports one handler. |
| Interrupt check between iterations, not mid-transfer | Checking after the complete unit of work (file + DB + count) preserves the invariant. Mid-transfer checks would require partial rollback. |
| `on_interrupt()` callback called after file completes | Pragmatic — truly immediate message requires pipe-based signaling. Message after current file is sufficient for UX. |
| Injectable interrupt flag via params | Enables testing without real signals. Production code passes None (auto-setup), tests pass a pre-set flag. |
| No special handling for preflight/confirmation Ctrl+C | Standard terminal behavior already handles this — SIGINT during readline/confirmation aborts the process. Only the transfer loop needs custom handling. |

## Non-Goals

- Truly immediate "Interrupt received..." message during a long file copy (would require pipe-based signal notification — disproportionate complexity)
- Signal handling for other signals (SIGTERM, SIGHUP) — SIGINT (Ctrl+C) only
- Cleanup of partial files on hard abort (second Ctrl+C) — orphan files are the safe state, detectable by resume
- Signal handling during scan or other commands — apply only

## Test Plan

### Existing Tests (Must Pass)

All existing `ops/apply` tests (937+). The `ApplyResult` struct gains new fields with defaults — existing tests that construct it need updating or the fields need `Default` values.

### New Tests

| Test | Type | Phase |
|------|------|-------|
| Execute respects pre-set interrupt flag | Integration (ops/apply) | 1 |
| Interrupt after first transfer, remaining count correct | Integration (ops/apply) | 1 |
| Interrupt with zero transfers (empty plan) | Edge case (ops/apply) | 1 |

## Implementation Checklist

- [ ] Phase 1: Add `signal-hook` dependency to Cargo.toml
- [ ] Phase 1: Add `setup_interrupt_flag()` function in ops/apply.rs
- [ ] Phase 1: Add `interrupted` and `remaining` fields to `ApplyResult`
- [ ] Phase 1: Add `interrupt_flag` to `ApplyExecuteParams`
- [ ] Phase 1: Add `on_interrupt()` to `TransferProgress` trait
- [ ] Phase 1: Modify transfer loop to check flag and break
- [ ] Phase 1: Test with injectable flag
- [ ] Phase 2: Implement `on_interrupt()` in `CliTransferProgress`
- [ ] Phase 2: Add interrupted summary and resume hint display
- [ ] Phase 2: Pass `interrupt_flag: None` from apply.rs
- [ ] Verify all existing tests pass

## Documentation Updates

No user-facing doc changes for this story alone. Ctrl+C behavior will be documented as part of the broader epic documentation pass (apply command page).

## Backward Compatibility

- `ApplyResult` gains two new fields (`interrupted: bool`, `remaining: usize`). These default to `false` and `0` — no impact on existing callers.
- `TransferProgress` trait gains `on_interrupt()`. All implementors must add it. There is one: `CliTransferProgress` in apply.rs. Tests that use mock progress will need the method.
- `ApplyExecuteParams` gains `interrupt_flag`. Must be set to `None` by the caller. Existing test construction needs updating.

## Performance Considerations

Negligible. One `AtomicBool::load(Ordering::Relaxed)` per transfer iteration — a single CPU instruction. Signal handler registration is one-time at the start of execute_apply.
