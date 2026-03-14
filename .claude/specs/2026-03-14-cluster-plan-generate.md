# Refactoring Spec: Cluster Plan/Generate Extraction

## Overview

Extract source selection, archive detection, duplicate checking, and fact coverage computation from `cluster.rs` into `ops/cluster.rs` using the plan pattern. The interface layer retains file I/O (lock file + manifest writing), presentation (archived-file warnings, summary comments), and ceremony.

**ADR**: `~/store/canon-architecture/2026-03-13-operations-layer.md`
**ADR Step Covered**: Partial Step 6 (Story D — cluster extraction only).

## Phase 1: Plan for `cluster generate` / `cluster refresh`

- **Status**: completed
- **Goal**: Extract the behavioral logic from `query_sources()`, `collect_full_coverage_facts()`, `find_source_duplicates()`, and related helpers into `ops::cluster::plan_generate()`, leaving the interface responsible only for file I/O, presentation, and ceremony.
- **Scope**: One plan function, params/plan types, private helpers, and tests.
- **Non-goals**: No changes to apply.rs, no `execute_generate()` (cluster's side effect is file I/O, not DB writes), no changes to manifest TOML format, no changes to presentation functions (`generate_summary_comments()`, `generate_fact_help()`, `print_cluster_stdout()`, `extract_notes()`), no changes to `LockEntry` location.
- **Dependencies**: Steps 1-5 + 6a-c completed (selection + exclude extraction done).

### Architecture

```
Interface (cluster.rs)              Operations (ops/cluster.rs)
─────────────────────               ──────────────────────────
generate() / refresh():             plan_generate():
  parse filters                       select_sources (SourceUnlessIncluded)
  resolve paths                       separate hashed / unhashed
  ─── call plan_generate() ────────►  batch fetch objects (hash info)
  plan empty? "No sources", return    batch fetch archive paths
  archived? display warnings          build lock entries
  dry-run? display, return            detect per-source archive status
  write lock file (JSONL)             check duplicates (if !allow)
  write manifest (TOML)               batch fetch facts
  print summary                       compute full coverage facts
                                      compute mixed-type warnings
                                      compute root breakdown + counts
                                      return ClusterGeneratePlan
```

### New Types in `ops/cluster.rs`

```rust
/// Parameters for planning a cluster generation.
pub struct ClusterGenerateParams {
    pub scopes: Vec<ScopeMatch>,
    pub filters: Vec<Filter>,
    pub allow_archived: bool,
    pub allow_duplicates: bool,
}

/// Computed plan for cluster generation. Contains all data the interface
/// needs for lock file writing, manifest assembly, and display —
/// no further queries needed.
pub struct ClusterGeneratePlan {
    /// Lock entries for the manifest (sources to archive).
    pub lock_entries: Vec<LockEntry>,
    /// Sources skipped because already in archive: (source_path, archive_path).
    pub archived: Vec<(String, String)>,
    /// Facts with 100% coverage across all lock entries: (key, type, description).
    pub full_coverage_facts: Vec<(String, FactType, String)>,
    /// Fact keys with mixed types across sources: (key, type_breakdown_string).
    pub mixed_type_warnings: Vec<(String, String)>,
    /// Root breakdown: (root_path, count) sorted by path.
    pub root_breakdown: Vec<(String, usize)>,
    /// Sources in plan with no archived copy.
    pub not_archived_count: usize,
    /// Number of excluded sources skipped.
    pub excluded_count: usize,
    /// Number of unhashed sources skipped.
    pub unhashed_count: usize,
}
```

### New Function in `ops/cluster.rs`

```rust
/// Compute what `cluster generate` would produce — no side effects.
///
/// Selects sources via `select_sources()`, separates hashed/unhashed,
/// detects archive status, checks for duplicates (returns Err if found
/// and not allowed), computes full-coverage facts, and returns a plan
/// with all data needed for lock file writing and manifest assembly.
pub fn plan_generate(conn: &mut Connection, params: &ClusterGenerateParams) -> Result<ClusterGeneratePlan>;
```

### `plan_generate()` Implementation Notes

1. Build `SelectionParams` with `RolePolicy::SourceUnlessIncluded`, `IncludeSet { excluded: false, archived: params.allow_archived }`, call `select_sources()`.
2. From `selection.sources`, separate hashed (has `object_id`) from unhashed. Count `unhashed_count`.
3. Collect unique `object_ids`, call `repo::object::batch_fetch_by_ids()` for hash info.
4. Call `repo::object::batch_find_archive_paths()` for archive detection.
5. Build `LockEntry` per hashed source using `LockEntry::from_source()` + hash info from objects.
6. Separate archived from non-archived: if source's object has archive paths and `!allow_archived`, move to `archived` list. If `allow_archived`, keep in `lock_entries`.
7. If `!allow_duplicates`, call `find_source_duplicates()` on `lock_entries`. If duplicates found → return `Err(...)` with the same error message as current code.
8. Collect source IDs from `lock_entries`, call `repo::fact::batch_fetch_for_sources()`.
9. Compute full coverage facts and mixed-type warnings using extracted fact computation logic (absorbs `collect_full_coverage_facts()` and `FactTypeTracker`).
10. Compute `root_breakdown` from `lock_entries` (sorted by path).
11. Compute `not_archived_count` from `lock_entries` + archive paths.
12. Use `selection.excluded_count` for `excluded_count`.

This eliminates:
- Inline SQL in `query_sources()` (`SELECT id FROM roots`) — replaced by `select_sources()` which uses `repo::root::fetch_all()`.
- The entire `query_sources()` function.
- `QuerySourcesResult` struct.
- `collect_full_coverage_facts()` function (absorbed into plan computation).
- `find_source_duplicates()` function (absorbed — or kept as private helper in ops).
- `FactTypeTracker` struct (moves to ops as private helper).

### Changes to `cluster.rs` (Interface Layer)

**`generate_lock()` becomes:**
1. Call `ops::cluster::plan_generate()`.
2. Display archived-file warnings from `plan.archived` (eprintln, unchanged format).
3. If `plan.lock_entries` is empty → return `Ok(None)`.
4. Display mixed-type warnings from `plan.mixed_type_warnings` (eprintln, unchanged format).
5. Write lock file from `plan.lock_entries`.
6. Build `LockGenerationResult` from plan fields, return `Ok(Some(...))`.

**Functions removed from `cluster.rs`:**
- `query_sources()` — absorbed into `plan_generate()`
- `QuerySourcesResult` — replaced by `ClusterGeneratePlan`
- `collect_full_coverage_facts()` — absorbed into `plan_generate()`
- `find_source_duplicates()` — absorbed into `plan_generate()`
- `FactTypeTracker` — moves to `ops/cluster.rs` as private helper
- `get_fact_description()` — moves to `ops/cluster.rs` as private helper

**Functions staying in `cluster.rs`:**
- `generate()`, `refresh()` — entry points (thinned via `generate_lock()`)
- `generate_lock()` — thinned orchestrator (calls `plan_generate()`, writes lock file)
- `write_lock_file()` — file I/O
- `hash_file()` — file I/O utility
- `generate_summary_comments()` — presentation
- `generate_fact_help()` — presentation
- `print_cluster_stdout()` — presentation
- `extract_notes()`, `inject_comments_before_key()` — presentation utilities
- `LockEntry`, `ManifestConfig`, etc. — serialization types
- `LockGenerationResult` — interface result type (built from plan)
- `GenerateOptions` — CLI options type
- `allow_values_to_strings()`, `parse_manifest_allow()` — option handling
- `validate_manifest_version()`, `current_timestamp()` — utilities

### Changes to `ops/mod.rs`

Add `pub mod cluster;`

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| `plan_generate()` uses `select_sources()` | Reuses established selection contract. Eliminates inline SQL violation in `query_sources()`. |
| No `execute_generate()` | Cluster's side effect is file I/O (write lock file), not DB writes. File I/O is interface concern. The ops layer computes the plan; the interface writes files. |
| Duplicate checking returns `Err` in plan | Current behavior is a hard gate, not informational. The plan fails if duplicates exist and aren't allowed. Preserves existing error message and behavior. |
| Mixed-type warnings in plan struct | Ops computes the warnings, interface displays them. No `eprintln!()` in ops layer. |
| `LockEntry` stays in `cluster.rs` | Shared serialization type used by both cluster and apply. Moving it adds churn without benefit. `plan_generate()` imports it from `cluster.rs`. |
| `FactTypeTracker` moves to ops as private | Implementation detail of fact coverage computation. Not public API. |
| `LockGenerationResult` stays in `cluster.rs` | It's the interface's summary view, built from plan fields. Used by `generate_summary_comments()` and `print_cluster_stdout()`. |
| `find_source_duplicates()` absorbed, not reused | Simple grouping function (~15 lines). Not worth a separate public function in ops — used only by `plan_generate()`. |

## Test Requirements

### Existing Tests (Must Pass)

All 693 existing tests.

### Tests to Remove from `cluster.rs`

These test behavioral logic that moves to ops:
- `test_cluster_excludes_suspended_roots` → replaced by `test_plan_generate_excludes_suspended`
- `test_cluster_excludes_excluded_sources` → replaced by `test_plan_generate_excludes_excluded`
- `test_cluster_archive_detection_counts_sources_not_objects` → replaced by `test_plan_generate_archive_detection_per_source`

### Tests to Keep in `cluster.rs`

All 18 remaining tests — they test serialization, formatting, notes extraction, version validation, and option handling. None test behavioral logic that moves to ops.

### New Tests to Add (in `ops/cluster.rs`)

**Selection and filtering tests:**

1. **`test_plan_generate_empty_no_sources`** — No sources in DB → plan has empty `lock_entries` and empty `archived`.
2. **`test_plan_generate_excludes_suspended`** — Source on suspended root → not in plan.
3. **`test_plan_generate_excludes_excluded`** — Source-level excluded and object-level excluded → filtered out, `excluded_count` is 2.
4. **`test_plan_generate_respects_scope`** — Scoped to `/photos` → only `/photos/...` sources in plan.

**Archive detection tests:**

5. **`test_plan_generate_archive_detection_per_source`** — 3 sources sharing 1 archived object → 3 entries in `archived`, 0 in `lock_entries`.
6. **`test_plan_generate_allow_archived`** — With `allow_archived = true`, archived sources stay in `lock_entries`, `archived` is empty.
7. **`test_plan_generate_not_archived_count`** — Mix of archived/unarchived objects → `not_archived_count` correct for sources in `lock_entries`.

**Hashing and lock entry tests:**

8. **`test_plan_generate_skips_unhashed`** — Source without `object_id` → not in `lock_entries`, counted in `unhashed_count`.
9. **`test_plan_generate_lock_entry_has_hash`** — Lock entry includes `hash_type` and `hash_value` from object.

**Duplicate checking tests:**

10. **`test_plan_generate_duplicates_rejected`** — 2 sources with same `object_id`, `allow_duplicates = false` → `Err` mentioning "duplicate".
11. **`test_plan_generate_duplicates_allowed`** — Same setup, `allow_duplicates = true` → plan succeeds with both sources.

**Root breakdown and counts:**

12. **`test_plan_generate_root_breakdown`** — Sources across 2 roots → `root_breakdown` has 2 entries with correct counts, sorted by path.

**Fact coverage tests:**

13. **`test_plan_generate_full_coverage_facts`** — All sources have fact "content.Make" → appears in `full_coverage_facts`.
14. **`test_plan_generate_partial_coverage_excluded`** — 2 of 3 sources have fact → NOT in `full_coverage_facts`.

### Expected Test Count

693 current − 3 removed + 14 new = **704 total**

## Implementation Checklist

- [x] Create `src/ops/cluster.rs` with plan types, `plan_generate()`, and private helpers
- [x] Add `pub mod cluster;` to `src/ops/mod.rs`
- [x] Write 14 tests in `ops/cluster.rs`
- [x] Thin `generate_lock()` in `cluster.rs` to call `plan_generate()` + write lock file
- [x] Remove `query_sources()`, `QuerySourcesResult`, `collect_full_coverage_facts()`, `find_source_duplicates()`, `FactTypeTracker`, `get_fact_description()` from `cluster.rs`
- [x] Remove 3 tests from `cluster.rs` that tested extracted behavior
- [x] Update imports in `cluster.rs` (remove unused `HashSet`, selection-related imports)
- [x] Verify all tests pass (704 total)
- [x] Update Story D status in Story A spec's remaining work table
