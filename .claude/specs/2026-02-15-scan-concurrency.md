# Refactoring Spec: Scan Concurrency Fixes

## Overview

Running two `canon scan` processes concurrently on different roots fails with instant "database is locked" errors on every file in the second process. The root cause is that per-file transactions use `BEGIN DEFERRED`, which acquires write locks late (at first write, after reading), a pattern where SQLite's busy handler is unreliable — particularly in non-WAL journal modes. Additionally, WAL mode is set but never verified, so a silent fallback to DELETE mode would make this pattern fail immediately without retries.

A secondary issue: the hash-linking phase (`get_or_create_object` + `set_object_id` + `store_object_fact`) runs three separate write operations without a transaction, which can leave the database in an inconsistent state on crash.

## Phases

### Phase 1: Fix per-file transaction locking
- **Status**: completed
- **Goal**: Make concurrent scans work reliably
- **Scope**:
  1. **`repo/db.rs`**: Reorder startup — set `busy_timeout` BEFORE `journal_mode` pragma (protects WAL conversion from contention on fresh databases)
  2. **`repo/db.rs`**: Verify WAL mode is active after setting it — use `pragma_query_value` to read back the journal mode, warn or bail if it's not "wal"
  3. **`scan.rs:process_file`**: Change from `conn.unchecked_transaction()` (which uses `BEGIN DEFERRED`) to `Transaction::new_unchecked(conn, TransactionBehavior::Immediate)` — this acquires the write lock at `BEGIN` time, where the busy handler is reliably invoked
- **Non-goals**: Changing the per-file transaction architecture, adding async I/O, switching database engines
- **Dependencies**: None

### Phase 2: Fix hash-linking atomicity
- **Status**: completed
- **Goal**: Prevent inconsistent state if scan crashes during hash-linking
- **Scope**:
  1. **`scan.rs` hash loop (lines 274-319)**: Wrap `get_or_create_object` + `set_object_id` + `store_object_fact` in a single `Transaction::new_unchecked(conn, TransactionBehavior::Immediate)` per file
  2. Also benefits concurrency: each hash-link becomes an atomic unit with proper busy-handler support
- **Non-goals**: Batching multiple files into a single transaction (each file should remain independent for crash resilience)
- **Dependencies**: Phase 1 (establishes the Immediate transaction pattern)

## Design Decisions

1. **`BEGIN IMMEDIATE` over `BEGIN DEFERRED`**: Every `process_file` call writes (even Unchanged updates `last_seen_at`), so declaring write intent upfront is always correct. The trade-off (write lock acquired slightly earlier) is negligible — the lock duration is the same, just shifted by a few microseconds.

2. **WAL verification, not just setting**: `pragma_update` fires the PRAGMA but doesn't check the result. If WAL fails (unsupported filesystem, permission issue), the connection silently stays in DELETE mode. Verifying prevents silent degradation.

3. **Per-file transactions in hash phase**: Not batching multiple files into one transaction — each file's hash-link should be independently atomic. This matches the per-file pattern in `process_file` and limits blast radius on crash.

## Test Requirements

### Existing Tests
- `scan.rs` tests (process_file_new, process_file_unchanged, etc.) — must continue to pass
- All `cargo test` — no regressions

### New Tests to Add

**Phase 1:**
- `repo/db.rs`: Test that `open_with_options` produces a connection in WAL mode (query `PRAGMA journal_mode` and assert "wal")

**Phase 2:**
- No new tests needed beyond existing scan tests (hash-linking is tested via integration flow; the atomicity fix is structural)
