# Refactoring Spec: Survey Computation Extraction (ADR Phase 2 Step 5)

## Overview

Extract `compute_survey()` and its supporting types from `src/survey.rs` (interface) to `src/ops/survey.rs` (operations layer). Survey is the largest single extraction (~370 lines of computation + ~2,800 lines of tests). The function already has a clean boundary: it takes data in and returns a typed `SurveyOutcome`.

**ADR**: `~/store/canon-architecture/2026-03-13-operations-layer.md` (Phase 2 Step 5)

**Key constraint**: Survey intentionally uses custom selection logic (not `ops::selection::select_sources()`). The asymmetric visibility model must be preserved as-is.

## Phase 1: Extract computation and tests

- **Status**: completed
- **Goal**: Move `compute_survey()`, its types, and all 63 computation tests to `ops/survey.rs`.
- **Non-goals**: Extracting display functions. Changing the selection logic. Splitting `SurveyOptions`. Refactoring the computation itself.
- **Dependencies**: Foundation complete (steps 1-3 done)

### New module: `src/ops/survey.rs`

### Types that move (currently private in survey.rs)

All become `pub` in ops/survey.rs:

```rust
/// Outcome of compute_survey: either a result to display or an early exit.
pub enum SurveyOutcome {
    Result(SurveyResult),
    Empty { scope_prefixes: Vec<String> },
    AllUnhashed { scope_prefixes: Vec<String>, total_count: usize },
}

pub struct SurveyResult {
    pub scope_prefixes: Vec<String>,
    pub total_count: usize,
    pub unhashed_count: usize,
    pub total_hashed: usize,
    pub archived_source_count: usize,
    pub archive_scopes: Vec<(String, usize)>,
    pub location_results: Vec<LocationResult>,
    pub unique_count: usize,
    pub unique_paths: Vec<String>,
    pub is_other_mode: bool,
    pub archive_label: Option<String>,
}

pub struct LocationResult {
    pub path: String,
    pub shared_count: usize,
    pub total_count: usize,
    pub complementary_count: Option<usize>,
    pub only_here_count: Option<usize>,
    pub kind: Option<domain::survey::LocationKind>,
    pub complementary_paths: Option<Vec<String>>,
    pub overlap_pairs: Option<Vec<OverlapPair>>,
    pub residual_paths: Option<Vec<String>>,
}

pub struct OverlapPair {
    pub selection_path: String,
    pub counterpart_paths: Vec<String>,
}
```

### Ops-level params type

Instead of passing `SurveyOptions` (which contains interface concerns like `null_delim`, `verbose`, `original_filters`), the ops function takes a params struct with only computation-relevant fields:

```rust
/// Parameters controlling survey computation.
pub struct SurveyParams {
    /// Visibility control (--include excluded).
    pub include: IncludeSet,
    /// Whether to compute affinity data (complementary counts, classification).
    pub compute_affinity: bool,
    /// Whether to compute overlap pairs per location.
    pub compute_overlap_pairs: bool,
    /// Whether to compute residual paths per location.
    pub compute_residual: bool,
}
```

The interface derives these from `SurveyOptions` + `DetailMode`:
- `compute_affinity` = `(options.affinity || options.detail == Some(DetailMode::Complement)) && !options.brief`
- `compute_overlap_pairs` = `options.detail == Some(DetailMode::Overlap)`
- `compute_residual` = `options.detail == Some(DetailMode::Residual)`

### Function signature

```rust
pub fn compute_survey(
    conn: &mut Connection,
    paths: &[PathBuf],
    filters: &[Filter],
    params: &SurveyParams,
    all_sources: &[Source],
    all_roots: &[domain::Root],
    other_paths: &[String],
    archive_root_id: Option<i64>,
) -> Result<SurveyOutcome>;
```

This matches the existing `compute_survey` signature except `SurveyOptions` is replaced by `SurveyParams`. The function body is unchanged — just replace `options.include` with `params.include`, `compute_affinity` local with `params.compute_affinity`, etc.

### Constants that move

```rust
const SUPERSET_THRESHOLD: f64 = 0.8;
```

`DETAIL_SAMPLE_SIZE`, `DETAIL_SHOW_ALL_THRESHOLD`, `DEFAULT_LOCATION_CAP` stay in the interface — they're display concerns.

### Interface changes (src/survey.rs)

The interface keeps:
- `DetailMode` (clap enum)
- `SurveyOptions` struct
- `run()` function — builds `SurveyParams` from options, calls `ops::survey::compute_survey()`, dispatches to display
- All `print_*` functions
- `DETAIL_SAMPLE_SIZE`, `DETAIL_SHOW_ALL_THRESHOLD`, `DEFAULT_LOCATION_CAP` constants

The interface removes:
- `compute_survey()` function
- `SurveyResult`, `LocationResult`, `OverlapPair`, `SurveyOutcome` types (now imported from ops)
- `SUPERSET_THRESHOLD` constant
- All 63 tests (moved to ops)
- Test helpers (`insert_root`, `insert_object`, `insert_source`, `insert_source_excluded`, `run_compute`, `test_options`)

### `archive_label` handling

Currently `compute_survey` returns `archive_label: None` and `run()` sets it after. This is display-only text. Two options:

**(A)** Keep the hack — ops returns `None`, interface sets it. Preserves existing behavior exactly.
**(B)** Pass `archive_label` into `compute_survey` so it can set it. Cleaner but means ops knows about display text.

Decision: **(A)** — keep it as-is. The `archive_label` field remains on `SurveyResult` but ops always returns `None`. The interface sets it. This preserves the existing pattern and avoids ops knowing about display concerns.

### `warn_nonexistent_scope_paths` call

Currently inside `compute_survey()` (line 343). This writes to stderr — a layer violation. Move it to `run()` in the interface, right after calling `compute_survey()`. Actually, looking more carefully, `run()` already calls it for `--other` paths (line 154). The one inside `compute_survey` is for scope paths.

Decision: Move the `warn_nonexistent_scope_paths` call for scope paths to `run()`, before calling `ops::survey::compute_survey()`. The ops function should not write to stderr.

### `std::env::current_dir()` call

`compute_survey()` calls `std::env::current_dir()` at line 336 to default empty paths. This is I/O inside ops. Two options:

**(A)** Move the defaulting to `run()` — pass non-empty paths to ops.
**(B)** Leave it — it's a minor convenience that doesn't affect testability (tests always pass explicit paths).

Decision: **(A)** — move the cwd defaulting to `run()`. The ops function should receive resolved paths. This is consistent with how other ops functions work.

### Test migration

All 63 tests move to `ops/survey.rs`. Changes needed:
- Replace local `insert_root`, `insert_object`, `insert_source`, `insert_source_excluded` with `ops::test_helpers::*`
  - Note: survey's `insert_source` uses a unique inode trick (`conn.last_insert_rowid() + 1000`). The ops `insert_source` helper also handles this, so it should be compatible.
  - survey's `insert_root` doesn't take `suspended` param but has a separate `insert_root_suspended`. The ops helper takes `suspended: bool`, so calls change from `insert_root(&conn, path, role)` to `insert_root(&conn, path, role, false)` and `insert_root_suspended(...)` to `insert_root(&conn, path, role, true)`.
- Replace `test_options()` with a local equivalent that builds `SurveyParams` instead of `SurveyOptions`
- Replace `run_compute()` with a local equivalent that calls `ops::survey::compute_survey()`
- Tests that reference `DetailMode` for setting computation flags will instead set `compute_overlap_pairs: true` etc. on `SurveyParams`
- Tests that reference `SurveyOutcome::Result` etc. now use `ops::survey::SurveyOutcome::Result`

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| Ops function takes `SurveyParams` not `SurveyOptions` | Options contains interface concerns (null_delim, verbose, original_filters) that ops shouldn't know about |
| Computation flags are bools not an enum | The code already derives bools internally; making them explicit avoids ops depending on `DetailMode` (a clap type) |
| `archive_label` stays as interface post-processing | It's display text; ops shouldn't construct human-readable labels |
| `warn_nonexistent_scope_paths` moves to interface | stderr output is an interface concern |
| cwd defaulting moves to interface | I/O for path defaulting is an interface concern |
| `SUPERSET_THRESHOLD` moves to ops | It's a computation constant used in `compute_survey` |
| Display constants stay in interface | `DETAIL_SAMPLE_SIZE` etc. are output formatting concerns |
| Custom selection logic preserved | ADR/CLAUDE.md explicitly documents survey's asymmetric visibility model |

## Test Requirements

### Existing Tests
All 748+ existing tests must pass. The 63 survey computation tests move from `src/survey.rs` to `src/ops/survey.rs` — same tests, new location.

### New Tests to Add

No new tests required — the existing 63 tests provide thorough coverage of the computation. The extraction is mechanical.

## Implementation Checklist
- [x] Create `src/ops/survey.rs` with types + `compute_survey()` + constants
- [x] Register `pub mod survey;` in `ops/mod.rs`
- [x] Move 61 computation tests to `ops/survey.rs`, adapt helpers (2 validation tests stay in interface)
- [x] Update `src/survey.rs`: remove computation, import from ops, build `SurveyParams` in `run()`
- [x] Move `warn_nonexistent_scope_paths` for scope paths to `run()`
- [x] Move cwd defaulting to `run()`
- [x] Verify all tests pass (748 pass)
