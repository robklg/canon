# Refactoring Spec: Apply Plan Extraction

## Overview

Extract preflight validation and destination path computation from `apply.rs` into `ops/apply.rs` using the plan pattern. The interface layer retains manifest I/O, filesystem checks, file transfer execution, and ceremony.

**ADR**: `~/store/canon-architecture/2026-03-13-operations-layer.md`
**ADR Step Covered**: Partial Step 6 (Story D — apply extraction).

## Phase 1: Plan for `apply`

- **Status**: completed
- **Goal**: Extract DB-based preflight validation, pattern expansion, collision detection, and source state checking from `apply.rs` into `ops::apply::plan_apply()`, leaving the interface responsible for manifest I/O, filesystem checks, file transfer, and ceremony.
- **Scope**: One plan function, params/plan types, private helpers, and tests.
- **Non-goals**: No `execute_apply()` (apply's side effect is file I/O, not DB writes — same rationale as cluster). No changes to file transfer logic (`process_source()`), filesystem checks (`check_destination_writable()`, `check_source_states_disk()`), ceremony (`print_apply_summary()`), or manifest format. No changes to `TransferMode`, `ApplyOptions`, or `domain::apply`.
- **Dependencies**: Steps 1-5 + 6a-d completed (selection + exclude + cluster extraction done).

### Architecture

```
Interface (apply.rs)                 Operations (ops/apply.rs)
─────────────────────                ──────────────────────────
run():                               plan_apply():
  read manifest (file I/O)             batch fetch facts for pattern keys
  validate version, merge options      validate unhashed sources
  read lock file (file I/O)            validate archive hash coverage
  validate lock hash                   expand all patterns → dest paths
  fetch roots, build root_paths        detect expansion failures
  validate archive root                detect destination path collisions
  compute base_dir, filter by root     check stale destination records (DB)
  print summary + confirm              check destination conflicts (DB)
  ─── call plan_apply() ──────────►    check archive conflicts (DB)
  check violations → display + bail    check excluded sources (DB)
  check dest writable (filesystem)     check suspended sources (DB)
  check source access (filesystem)     DB-based source state validation
  check source states (filesystem)     in resume mode: DB classification
  resume: classify on disk             return ApplyPlan
  execute transfers
  print summary
```

### New Types in `ops/apply.rs`

```rust
/// Parameters for planning an apply operation.
pub struct ApplyPlanParams<'a> {
    /// Filtered sources from the lock file (already filtered by --root).
    pub sources: &'a [&'a LockEntry],
    /// Parsed output pattern.
    pub pattern: &'a Pattern,
    /// Fact keys needed by the pattern (from expr::extract_fact_keys).
    pub needed_keys: &'a [String],
    /// Scope prefix from manifest config (meta.scope).
    pub scope_prefix: Option<&'a str>,
    /// Root ID → root path cache (from repo::root::fetch_all).
    pub root_paths: &'a HashMap<i64, String>,
    /// Destination archive root ID.
    pub archive_root_id: i64,
    /// Relative base directory within archive root (config.output.base_dir).
    pub base_dir_rel: &'a str,
    /// Whether this is a resume operation.
    pub resume: bool,
    /// Whether duplicates in destination archive are allowed.
    pub allow_duplicates: bool,
    /// Whether duplicates in other archives are allowed.
    pub allow_cross_archive_duplicates: bool,
}

/// A source validated and ready for transfer, with pre-computed destination.
#[derive(Debug)]
pub struct ApplyTransfer {
    /// Source ID from lock entry.
    pub source_id: i64,
    /// Absolute source path.
    pub source_path: String,
    /// Destination path relative to base_dir (for filesystem operations).
    pub dest_rel_path: String,
    /// Destination path relative to archive root (for DB registration).
    pub archive_rel_path: String,
    /// Content object ID (for DB registration).
    pub object_id: Option<i64>,
    /// Partial hash from lock file (for DB registration and staleness).
    pub partial_hash: String,
    /// File size from lock file (for staleness validation).
    pub size: i64,
    /// File mtime from lock file (for staleness validation).
    pub mtime: i64,
}

/// Computed plan for an apply operation. Contains all data the interface
/// needs for violation display, filesystem checks, and transfer execution.
#[derive(Debug)]
pub struct ApplyPlan {
    /// Sources validated and ready for transfer with pre-computed destinations.
    /// In regular mode: all sources that passed validation.
    /// In resume mode: sources whose destination is NOT already in DB.
    pub transfers: Vec<ApplyTransfer>,
    /// All violations found during planning.
    pub violations: ApplyViolations,
    /// Sources whose DB state has changed since lock file (size/mtime/partial_hash).
    /// Computed via DB check. Interface may also do disk-based validation.
    pub stale_sources: Vec<StaleSource>,
    /// Resume mode: count of sources already registered in archive DB.
    pub already_archived_count: usize,
}

/// Violations found during apply planning. The interface inspects each field
/// and decides whether to bail (hard gate) or proceed (with --allow flags).
#[derive(Debug, Default)]
pub struct ApplyViolations {
    /// Sources that failed pattern expansion: (source_path, error_message).
    pub expansion_failures: Vec<(String, String)>,
    /// Destination paths with multiple sources: (dest_rel_path, source_paths).
    pub collisions: Vec<(String, Vec<String>)>,
    /// Destination paths with stale DB records (present=1, file likely missing).
    pub stale_records: Vec<String>,
    /// Destination paths already occupied in DB (non-resume mode only).
    pub dest_conflicts_in_db: Vec<String>,
    /// Sources whose content already exists in destination archive:
    /// (source_path, archive_path).
    pub archive_conflicts_dest: Vec<(String, String)>,
    /// Sources whose content already exists in other archives:
    /// (source_path, archive_path).
    pub archive_conflicts_other: Vec<(String, String)>,
    /// Sources marked as excluded since manifest generation: (id, path).
    pub excluded_sources: Vec<(i64, String)>,
    /// Sources from suspended roots: (id, path).
    pub suspended_sources: Vec<(i64, String)>,
}

/// A source whose state has changed since the lock file was generated.
#[derive(Debug)]
pub struct StaleSource {
    pub path: String,
    pub reason: String,
}
```

### New Function in `ops/apply.rs`

```rust
/// Compute what `apply` would do — validates constraints and computes
/// destination paths. No filesystem I/O, no file transfers.
///
/// Runs all DB-based preflight checks: unhashed sources, archive hash
/// coverage, pattern expansion, collision detection, stale records,
/// destination conflicts, archive conflicts, excluded/suspended sources,
/// and DB-based source state validation.
///
/// Returns an ApplyPlan with validated transfers and any violations.
/// The interface inspects violations to decide whether to proceed.
///
/// Returns `Err` only for unexpected failures (DB errors, missing roots).
/// Constraint violations are returned as data in `plan.violations`.
pub fn plan_apply(conn: &mut Connection, params: &ApplyPlanParams) -> Result<ApplyPlan>;
```

### `plan_apply()` Implementation Notes

1. **Batch fetch facts** for pattern evaluation. For each key in `needed_keys` that isn't a derived key (`source.*`, `scope.*`, `object.hash`), call `repo::fact::batch_fetch_key_for_sources()`. Build `HashMap<i64, Vec<FactEntry>>`.

2. **Validate unhashed sources** (pure). Filter `params.sources` for `s.object_id.is_none()`. If any, add nothing to violations (this is currently a bail — but it should be a violation that the interface gates on). Wait — actually this should be a precondition `Err` because unhashed sources make pattern expansion meaningless. **Decision**: Return `Err` for unhashed sources (precondition failure, same as current behavior).

3. **Validate archive hash coverage** (DB). Call `repo::source::count_unhashed_for_root(conn, archive_root_id)`. If unhashed > 0, return `Err` (precondition failure).

4. **Expand patterns for all sources** → build `Vec<ApplyTransfer>`. For each source, call `evaluate_pattern()` (private helper, moved from apply.rs). On failure, collect into `violations.expansion_failures`. On success, compute `archive_rel_path` from `base_dir_rel` + `dest_rel`. Build `ApplyTransfer` with source data + computed paths.

5. **Detect destination path collisions** (pure). Group successful transfers by `dest_rel_path`. Any dest with >1 source → `violations.collisions`.

6. **Check stale destination records** (DB). Collect `archive_rel_path` from transfers. Call `repo::source::batch_check_paths_exist(conn, archive_root_id, &path_refs)`. Paths that exist in DB → `violations.stale_records`.

7. **Check destination conflicts** (DB, non-resume only). If `!params.resume`: same batch_check_paths_exist call as step 6 (reuse result). Paths in DB → `violations.dest_conflicts_in_db`. Note: the disk-side check stays in interface.

8. **Check archive conflicts** (DB). Collect hash values from transfers. Call `repo::object::batch_find_archive_info_by_hash(conn, &hash_values)`. Separate into dest archive vs other archives → `violations.archive_conflicts_dest`, `violations.archive_conflicts_other`.

9. **Check excluded sources** (DB). Collect source IDs from transfers. Call `repo::source::batch_fetch_by_ids(conn, &source_ids)`. Filter for `s.is_excluded()` → `violations.excluded_sources`.

10. **Check suspended sources** (DB). Same batch result from step 9. Filter for `!s.is_active()` → `violations.suspended_sources`.

11. **DB-based source state validation**. Same batch result from step 9. Compare each source's DB state (size, mtime, partial_hash) against lock entry. Mismatches → `plan.stale_sources`.

12. **Resume mode DB classification**. If `params.resume`: reuse the batch_check_paths_exist result from step 6. Sources whose dest path IS in DB → counted as `already_archived_count`. Sources NOT in DB → kept in `transfers`. (The interface then disk-checks the remaining transfers to separate Available vs Resumed vs SizeMismatch.)

13. **Return `ApplyPlan`** with transfers, violations, stale_sources, already_archived_count.

This absorbs:
- `check_unhashed_sources()` (precondition → Err)
- `check_archive_hash_coverage()` (precondition → Err)
- `validate_pattern_expansions()` (→ violations.expansion_failures)
- Collision detection part of `check_destination_collisions_filtered()` (→ violations.collisions)
- `check_stale_destination_records()` (→ violations.stale_records)
- DB part of `check_destination_conflicts()` (→ violations.dest_conflicts_in_db)
- `check_archive_conflicts_filtered()` (→ violations.archive_conflicts_dest/other)
- `check_excluded_sources_filtered()` (→ violations.excluded_sources)
- `check_suspended_sources_filtered()` (→ violations.suspended_sources)
- `check_source_states_db()` (→ plan.stale_sources)
- `build_eval_context()` (→ private helper in ops)
- `evaluate_pattern()` (→ private helper in ops)

### Changes to `apply.rs` (Interface Layer)

**`run()` becomes:**
1. Read manifest, validate version, merge options (unchanged).
2. Read lock file, validate hash (unchanged).
3. Parse pattern, extract needed keys (unchanged).
4. Fetch roots, build root_paths cache (unchanged).
5. Validate archive root exists and is archive (unchanged).
6. Compute base_dir, filter by root specs (unchanged).
7. Print summary + confirm (unchanged).
8. Build `ApplyPlanParams`, call `ops::apply::plan_apply()`.
9. Check `plan.violations` → display errors and bail as appropriate:
   - expansion_failures → bail (hard gate)
   - collisions → bail (hard gate)
   - stale_records → bail (hard gate)
   - dest_conflicts_in_db → bail if !resume (hard gate)
   - archive_conflicts_dest → bail if !allow_duplicates (soft gate)
   - archive_conflicts_other → bail if !allow_cross_archive_duplicates (soft gate)
   - excluded_sources → bail (hard gate)
   - suspended_sources → bail (hard gate)
10. Check dest writable (filesystem — unchanged).
11. Check source accessibility (filesystem — simplified from `check_destination_collisions_filtered()`; only the readability part, since collisions are now in the plan).
12. Source state validation:
    - Real apply: `check_source_states_disk()` on plan.transfers (filesystem — unchanged).
    - Dry-run: use plan.stale_sources (already computed by plan).
13. Resume mode: classify destinations on disk for plan.transfers. Use `domain::apply::classify_destination()`. Sources with Available → transfer. Others → count stats.
14. Execute transfers using `plan.transfers` (pre-computed dest paths — no pattern re-evaluation).
15. Print summary (unchanged).

**Functions removed from `apply.rs`:**
- `check_unhashed_sources()` — absorbed into plan (precondition)
- `check_archive_hash_coverage()` — absorbed into plan (precondition)
- `validate_pattern_expansions()` — absorbed into plan
- `check_stale_destination_records()` — absorbed into plan
- `check_archive_conflicts_filtered()` — absorbed into plan
- `check_excluded_sources_filtered()` — absorbed into plan
- `check_suspended_sources_filtered()` — absorbed into plan
- `check_source_states_db()` — absorbed into plan
- `build_eval_context()` — moves to ops (private)
- `evaluate_pattern()` — moves to ops (private)
- `ArchiveConflicts` struct — replaced by violations fields
- `SourceAccessCheck` struct — simplified to readability-only check
- `DestinationConflicts` struct — DB part replaced by violations; disk part stays

**Functions staying in `apply.rs`:**
- `run()` — thinned orchestrator
- `print_apply_summary()`, `show_directory_preview()` — ceremony
- `check_destination_writable()` — filesystem
- Source readability check — simplified from old `check_destination_collisions_filtered()`
- `check_source_states_disk()`, `validate_source_state()` — filesystem
- `plan_transfers()` — **renamed/simplified**: only the disk-classification part for resume mode; DB part absorbed by plan. Uses `domain::apply::classify_destination()`.
- `process_source()` — **simplified**: receives `ApplyTransfer` with pre-computed paths instead of re-evaluating pattern. Still does filesystem + DB writes.
- `preserve_metadata()` — filesystem
- `relocate_source()`, `mark_source_not_present()`, `build_new_source()` — thin wrappers
- `filter_by_roots()` — input resolution (uses `domain::root::parse_root_spec`)
- `TransferMode`, `ApplyOptions`, `ApplyStats`, `ApplyAction` — CLI types
- `SkippedStaleSource`, `SizeMismatchError`, `WorkPlan` — interface types (WorkPlan simplified)

**Key simplification in `process_source()`:**

Currently `process_source()` takes 11 parameters including pattern, needed_keys, scope_prefix, root_paths, all_facts — all for re-evaluating the pattern. After extraction, it takes an `ApplyTransfer` which already has `dest_rel_path` and `archive_rel_path`. The function simplifies significantly.

### Changes to `ops/mod.rs`

Add `pub mod apply;`

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| No `execute_apply()` | Apply's side effect is file I/O (copy/rename/move files), not DB writes. File I/O is interface concern. Same rationale as cluster (no `execute_generate()`). |
| Violations returned as data, not errors | The ops layer doesn't know about ceremony. Some violations are hard gates (excluded sources), some are soft gates (archive conflicts + `--allow`). The interface decides based on options. |
| Preconditions (unhashed, hash coverage) return `Err` | These are true preconditions that make the plan meaningless. Unlike violations which are specific constraint failures, these indicate the manifest or archive is not ready for apply at all. |
| Pre-computed destination paths | Pattern expansion happens once in the plan. The interface never re-evaluates patterns — transfers carry pre-computed `dest_rel_path` and `archive_rel_path`. This simplifies `process_source()` from 11 parameters to a small struct. |
| Resume disk classification stays in interface | Resume mode needs filesystem access (check if dest file exists, get size) to distinguish Available vs Resumed vs SizeMismatch. The plan does the DB part (which are already in DB?), interface does the disk part. `domain::apply::classify_destination()` stays in domain — it's pure. |
| DB source state validation in plan | Computing staleness from DB is cheap and useful for both dry-run (primary use) and as additional data for real apply. One batch_fetch_by_ids serves multiple checks (excluded, suspended, staleness). |
| Confirmation order preserved | Confirmation still happens BEFORE plan_apply(). The interface reads the manifest, shows summary, confirms intent, then calls the plan. Users confirm "apply this manifest" before expensive preflight runs. |
| `LockEntry` stays in `cluster.rs` | Shared serialization type used by both cluster and apply. `plan_apply()` imports it. Same decision as cluster extraction. |

## Test Requirements

### Existing Tests (Must Pass)

All 704 existing tests, plus all 5 tests in `domain/apply.rs`.

### Tests to Add (in `ops/apply.rs`)

**Pattern expansion and destination computation:**

1. **`test_plan_apply_computes_dest_paths`** — Sources with facts → transfers have correct `dest_rel_path` and `archive_rel_path`.
2. **`test_plan_apply_dest_paths_with_base_dir`** — Non-empty `base_dir_rel` → `archive_rel_path` includes prefix.
3. **`test_plan_apply_expansion_failure`** — Source missing required fact → `violations.expansion_failures` populated, source not in transfers.

**Collision detection:**

4. **`test_plan_apply_detects_collisions`** — Two sources expanding to same dest → `violations.collisions` populated.
5. **`test_plan_apply_no_collision_different_paths`** — Two sources with different dest → no collisions.

**Archive conflict detection:**

6. **`test_plan_apply_archive_conflict_dest`** — Source hash already in destination archive → `violations.archive_conflicts_dest` populated.
7. **`test_plan_apply_archive_conflict_other`** — Source hash in other archive → `violations.archive_conflicts_other` populated.
8. **`test_plan_apply_no_archive_conflict`** — Source hash not in any archive → no conflicts.

**Excluded and suspended source detection:**

9. **`test_plan_apply_detects_excluded`** — Source marked excluded since manifest → `violations.excluded_sources` populated.
10. **`test_plan_apply_detects_suspended`** — Source root suspended since manifest → `violations.suspended_sources` populated.

**Stale records and destination conflicts:**

11. **`test_plan_apply_detects_stale_records`** — Dest path has present=1 record in DB → `violations.stale_records` populated.
12. **`test_plan_apply_detects_dest_conflicts`** — Non-resume mode, dest path in DB → `violations.dest_conflicts_in_db` populated.
13. **`test_plan_apply_dest_conflicts_skipped_in_resume`** — Resume mode → `dest_conflicts_in_db` empty (check skipped).

**DB source state validation:**

14. **`test_plan_apply_detects_stale_sources`** — Source size changed in DB since lock → `stale_sources` populated.
15. **`test_plan_apply_fresh_sources_not_stale`** — Source unchanged in DB → `stale_sources` empty.

**Resume mode:**

16. **`test_plan_apply_resume_filters_archived`** — Resume mode, dest path in DB → not in transfers, `already_archived_count` incremented.
17. **`test_plan_apply_resume_keeps_non_archived`** — Resume mode, dest path NOT in DB → in transfers.

**Preconditions:**

18. **`test_plan_apply_err_unhashed`** — Source without object_id → returns Err.
19. **`test_plan_apply_err_archive_hash_gap`** — Archive root has unhashed files → returns Err.

**Empty/edge cases:**

20. **`test_plan_apply_empty_sources`** — No sources → empty plan, no violations.

### Tests to Remove from `apply.rs`

None — `apply.rs` has no tests to remove.

### Expected Test Count

704 current + 20 new = **724 total**

## Implementation Checklist

- [x] Create `src/ops/apply.rs` with plan types, `plan_apply()`, and private helpers
- [x] Add `pub mod apply;` to `src/ops/mod.rs`
- [x] Write 20 tests in `ops/apply.rs`
- [x] Thin `run()` in `apply.rs` to call `plan_apply()` + handle violations + execute transfers
- [x] Simplify `process_source()` to use `ApplyTransfer` instead of re-evaluating patterns
- [x] Remove absorbed functions and types from `apply.rs`
- [x] Update imports in `apply.rs`
- [x] Verify all tests pass (724 total)
- [x] Update Story D status in Story A spec's remaining work table

## ADR Step 6 Status After This Spec

| Story | Scope | Status |
|-------|-------|--------|
| A: set + clear | Source-level plan/execute | **Completed** |
| B: duplicates | `exclude_duplicates()` plan/execute | **Completed** |
| C: object-level + single-target | `set_objects_by_filter()` + thin operations | **Completed** |
| D: cluster/apply | Cluster: plan_generate. Apply: plan_apply. | **Completed** |
| E: scan | Scan pipeline | Pending (architecture board to weigh in) |
