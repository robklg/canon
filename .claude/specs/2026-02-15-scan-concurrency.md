# Refactoring Spec: Scan Concurrency Fixes

## Overview

Running two `canon scan` processes concurrently on different roots failed with instant "database is locked" errors. Two root causes:

1. Per-file transactions used `BEGIN DEFERRED`, which acquires write locks late (at first write, after reading) — a pattern where SQLite's busy handler is unreliable.
2. The `compute_partial_hash` filesystem I/O ran inside the write transaction, holding the write lock during potentially slow NAS/network reads. A fast local scan could starve a concurrent NAS scan by re-acquiring the lock faster than the busy handler could catch gaps.

A secondary issue: the hash-linking phase ran three separate writes without a transaction, risking inconsistent state on crash.

## Phases

### Phase 1: Fix per-file transaction locking (v0.2.2)
- **Status**: completed
- **Goal**: Make concurrent scans work reliably
- **Scope**:
  1. **`repo/db.rs`**: Reorder startup — set `busy_timeout` BEFORE `journal_mode` pragma (protects WAL conversion from contention on fresh databases)
  2. **`repo/db.rs`**: Verify WAL mode is active after setting it — use `pragma_query_value` to read back the journal mode, bail if it's not "wal"
  3. **`scan.rs:process_file`**: Change from `conn.unchecked_transaction()` (`BEGIN DEFERRED`) to `Transaction::new_unchecked(conn, TransactionBehavior::Immediate)` — acquires the write lock at `BEGIN` time, where the busy handler is reliably invoked
- **Non-goals**: Changing the per-file transaction architecture, adding async I/O, switching database engines
- **Dependencies**: None

### Phase 2: Fix hash-linking atomicity (v0.2.2)
- **Status**: completed
- **Goal**: Prevent inconsistent state if scan crashes during hash-linking
- **Scope**:
  1. **`scan.rs` hash loop**: Wrap `get_or_create_object` + `set_object_id` + `store_object_fact` in a single `Transaction::new_unchecked(conn, TransactionBehavior::Immediate)` per file
  2. Also benefits concurrency: each hash-link becomes an atomic unit with proper busy-handler support
- **Non-goals**: Batching multiple files into a single transaction (each file should remain independent for crash resilience)
- **Dependencies**: Phase 1 (establishes the Immediate transaction pattern)

### Phase 3: Move filesystem I/O outside write transaction (v0.2.3)
- **Status**: completed
- **Goal**: Eliminate write lock contention during slow filesystem I/O
- **Scope**:
  1. **`scan.rs:process_file`**: Restructure into three phases:
     - **Read phase**: `fetch_by_path` + `fetch_by_inode` + `reconcile()` outside any transaction (no lock held)
     - **Hash phase**: `compute_partial_hash` outside any transaction (slow NAS I/O, no lock held)
     - **Write phase**: `BEGIN IMMEDIATE` transaction containing only `apply_reconciliation` + `COMMIT` (fast, DB-only)
- **Non-goals**: TOCTOU re-read guard (see Design Decision #4)
- **Dependencies**: Phase 1 (Immediate transaction pattern)

## Design Decisions

1. **`BEGIN IMMEDIATE` over `BEGIN DEFERRED`**: Every `process_file` call writes (even Unchanged updates `last_seen_at`), so declaring write intent upfront is always correct. The trade-off (write lock acquired slightly earlier) is negligible.

2. **WAL verification, not just setting**: `pragma_update` fires the PRAGMA but doesn't check the result. If WAL fails (unsupported filesystem, permission issue), the connection silently stays in DELETE mode. Verifying prevents silent degradation.

3. **Per-file transactions in hash phase**: Not batching multiple files into one transaction — each file's hash-link should be independently atomic. Limits blast radius on crash.

4. **No TOCTOU re-read in process_file**: An earlier iteration re-read DB state inside the write transaction to guard against changes between the read phase and write phase. This was rejected because:
   - It introduced a bug: if reconciliation changed from Unchanged→New between phases, the partial hash would be missing (None), causing a NOT NULL constraint violation
   - Concurrent scans on different roots operate on different source records, so TOCTOU between phases is impossible in practice
   - `apply_reconciliation`'s SQL is naturally idempotent (UPDATE by root_id+rel_path or by source_id), so even in the degenerate same-root case, stale reads produce correct outcomes
   - The duplicate reads added unnecessary DB queries (4 per file instead of 2)

## Test Requirements

### Existing Tests
- `scan.rs` tests (process_file_new, process_file_unchanged, etc.) — all pass
- All `cargo test` — 499 tests, no regressions

### New Tests Added
- `repo/db.rs`: `open_with_options_enables_wal_mode` — verifies WAL mode is active on a file-backed database
