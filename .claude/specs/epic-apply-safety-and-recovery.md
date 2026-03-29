# Epic: Apply Safety and Recovery

**Design Spec**: [~/store/claude-designs/2026-03-29-apply-safety-and-recovery.md](~/store/claude-designs/2026-03-29-apply-safety-and-recovery.md)
**Status**: In Progress
**Created**: 2026-03-29

## Objective

Canon's apply command — the culmination of the scan → orient → act workflow — has systematic safety and recovery failures exposed by real archiving sessions. A pattern bug scattered files to the filesystem root. Ctrl+C left irrecoverable state. Resume created deepening error spirals. The tool that promises safe, methodical archiving pushed users toward `rm -rf` on archive paths.

This epic makes apply reliable: prevent dangerous outcomes before they happen (safety gates), recover gracefully when things go wrong (resilient resume), and communicate clearly throughout (UX polish).

### Success Criteria

1. Pattern expansion never produces paths outside the archive root
2. Apply verifies all sources exist and are readable before any file operation
3. File operations use atomic noclobber (no silent overwrites)
4. Ctrl+C stops cleanly with DB consistency preserved
5. Resume works by filesystem reconciliation — no stale-record spirals, no UNIQUE crashes
6. `cluster status` gives a complete safety assessment of any manifest
7. Dry-run is clearly labeled and skips confirmation
8. Manifest header honestly describes editability

## Architectural Design

### Overview

Changes span four layers, following the established architecture:

```
Interface (apply.rs, cluster.rs)
  - Signal handling setup, dry-run presentation, status output, header text

Operations (ops/apply.rs, ops/cluster.rs)
  - Pattern normalization, preflight checks, reconciliation engine, status logic

Operations/FS (ops/fs.rs)
  - Atomic noclobber primitives (O_EXCL, RENAME_NOREPLACE)

Expression (expr/eval.rs)
  - Pattern result normalization after evaluation

Repository (repo/source.rs)
  - Robust upsert for destination registration
```

### Key Architectural Decisions

| Decision | Rationale |
|----------|-----------|
| Normalize in expr/eval.rs, not ops/apply.rs | All pattern consumers get safe paths — confirmation samples, dry-run, verbose, error messages |
| Archive-root validation as a plan violation | Same pattern as existing collision/conflict checks — collected, not fail-fast |
| Reconciliation as shared ops function | Used by both resume (classify then act) and status (classify then report) |
| No journal file — filesystem IS the journal | Simpler, handles mode switching, reflects actual reality |
| Upsert handles present=0 AND present=1 | Eliminates UNIQUE constraint crashes regardless of intervening scans |

## Stories

| # | Story | Scope | Dependencies | Status |
|---|-------|-------|--------------|--------|
| 1 | Pattern Safety | expr/eval.rs, ops/apply.rs | — | Complete |
| 2 | Preflight Hardening | ops/apply.rs, ops/fs.rs | — | Complete |
| 3 | Signal Handling | ops/apply.rs, apply.rs | — | Pending |
| 4 | Resume Reconciliation | ops/apply.rs, repo/source.rs | — | Pending |
| 5 | Manifest Status | ops/cluster.rs, cluster.rs | Story 4 | Pending |
| 6 | UX Polish | apply.rs, cluster.rs | — | Pending |

### Story 1: Pattern Safety

**Objective**: Pattern expansion always produces clean relative paths. Archive-root validation catches anything normalization misses.

**Scope**: `expr/eval.rs` (normalization), `ops/apply.rs` (validation)

**Key decisions**:
- Normalization happens in `evaluate()` after existing `..` sanitization
- Archive-root validation uses `domain::path::path_is_under()` (lexical, no fs access for dest)
- Archive root path fetched from `archive_root_id` in manifest config

**Dependencies**: None

### Story 2: Preflight Hardening

**Objective**: Apply verifies source existence/readability upfront and uses atomic noclobber for all file operations.

**Scope**: `ops/apply.rs` (source preflight in plan_apply), `ops/fs.rs` (atomic noclobber primitives)

**Key decisions**:
- Source check: stat + open/close for each pending source in plan phase
- Noclobber copy: `OpenOptions::create_new(true)` (stdlib `O_EXCL`)
- Noclobber rename: platform-native atomic (`renameat2`/`renameatx_np`) with stat fallback
- Existing per-file `validate_source_state()` in execute phase remains (catches race conditions)

**Dependencies**: None (can be done in parallel with Story 1)

### Story 3: Signal Handling

**Objective**: Ctrl+C stops cleanly — first signal finishes current file + DB write, second signal hard-aborts without DB corruption.

**Scope**: `ops/apply.rs` (interrupt flag check in execute loop), `apply.rs` (signal setup, resume hint)

**Key decisions**:
- Atomic flag set by signal handler, checked between file operations
- First signal: complete current file + DB write, then stop
- Second signal: default OS termination (DB write not yet started for interrupted file = safe)
- DB writes strictly after successful file operations (existing invariant, enforced)

**Dependencies**: None (but understanding the execute loop from Story 4 design helps)

### Story 4: Resume Reconciliation

**Objective**: Resume works by filesystem state reconciliation, not DB inference. Robust upsert eliminates UNIQUE constraint crashes.

**Scope**: `ops/apply.rs` (reconciliation engine, revised resume path in plan_apply), `repo/source.rs` (upsert)

**Key decisions**:
- New ops function: `classify_manifest_entries()` — stat source, stat dest, check DB, classify
- Resume path in `plan_apply()` uses classification instead of DB-only `batch_check_paths_exist()`
- `insert_destination()` handles all DB states (no record, present=0, present=1)
- Resume never deletes source files — "already there" entries are skipped + registered
- Summary note when sources still exist for "already there" entries

**Dependencies**: None (but this is the largest story — should be designed in detail)

### Story 5: Manifest Status

**Objective**: `canon cluster status <manifest>` gives a complete read-only safety assessment.

**Scope**: `ops/cluster.rs` (status logic using shared reconciliation), `cluster.rs` (new subcommand, output formatting)

**Key decisions**:
- Shares `classify_manifest_entries()` from Story 4
- Read-only — no DB writes, no file operations
- Safety headline: "All source files accounted for" vs "WARNING: N missing"
- Default shows only concerning entries; `--verbose` shows all
- Works even with lock hash mismatch (diagnostic tool, not blocked by integrity check)

**Dependencies**: Story 4 (reconciliation engine)

### Story 6: UX Polish

**Objective**: Dry-run clarity, manifest header honesty, refresh editor behavior.

**Scope**: `apply.rs` (dry-run presentation), `ops/cluster.rs` (header text), `cluster.rs` (refresh editor flag)

**Key decisions**:
- Dry-run: skip confirmation, show `[DRY RUN]` banner, show reconciliation with `--resume`
- Header: "edit pattern and Notes freely / to change query, edit then refresh / other fields managed by Canon"
- Refresh: no editor by default, `--edit` flag for opt-in

**Dependencies**: None (but dry-run + resume display benefits from Story 4's reconciliation)

## Cross-Cutting Concerns

- **Lock file format unchanged**: JSONL, immutable, hash-verified. No migration needed.
- **Existing tests must pass**: Normalization must not change behavior for currently-valid patterns.
- **Plan/execute separation preserved**: New checks go in plan phase, new behaviors in execute phase.
- **ops/fs noclobber**: The existing `noclobber: bool` parameter signature stays, but the implementation changes from check-then-operate to atomic. All callers already pass `noclobber: true`.

## Documentation Updates

- `docs/src/commands/apply.md`: Document new preflight behavior, Ctrl+C handling, enhanced resume, dry-run changes
- `docs/src/commands/cluster.md`: Document `cluster status` subcommand, updated manifest header, refresh `--edit` flag
- `docs/src/concepts/cluster-apply.md` (or equivalent workflow page): Update the cluster→apply workflow to include status as the recovery entry point

## Non-Goals

- Changing the lock file format
- Adding a journal file
- Auto-cleaning source files during resume
- Making resume re-query the manifest's scope/filter
- Solving general two-system (fs+db) consistency — we solve it specifically for apply's use case

## Risks

| Risk | Mitigation |
|------|------------|
| Pattern normalization changes behavior for valid patterns | Unit tests for all edge cases; normalization only strips degenerate components (empty, `.`) |
| Atomic noclobber not supported on some NFS/SMB | Use as defense-in-depth, not sole protection; preflight catches common case |
| Resume reconciliation I/O cost on large manifests | Batch DB queries; filesystem stats are fast (metadata only) |
| Signal handling complexity in Rust | Well-trodden ground; use established crate; two-tier model is simple |

## Version History

| Date | Change |
|------|--------|
| 2026-03-29 | Epic created from functional design spec |
