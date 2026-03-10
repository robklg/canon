# Story: Survey Modes — Orientation Default, Affinity Opt-In, and New Detail Views

**Design Spec**: ~/store/claude-designs/2026-03-09-survey-modes.md
**Status**: Pending
**Created**: 2026-03-10

## Objective

Survey was implemented as layer 1 of assembly support, but first real use revealed it serves three cognitive modes — orientation, assembly, and triage — while the default output is tuned only for assembly. The current default *is* affinity mode: it computes and displays affinity columns when `--where` is present. This work names what exists (affinity becomes `--affinity`), builds a new orientation mode as the default, and adds two detail views (`overlap`, `residual`) that serve orientation and triage drill-downs.

The guiding principle is "structural tools, purposeful modes": detail views are structural (named by what they show), mode flags are purposeful (named by why you're looking). Orientation is the most common arrival state — users should understand the character of a place before being directed toward tasks.

## Functional Requirements Summary

**Story 1 — Orientation-native default summary**: Header "Survey:" instead of "Selection:", "N sources here", "N unique here" at top (always shown when hashed > 0, even when 0), related locations show "N of M overlap (T total)" with no affinity columns, sort by shared desc, bottom "unique to this scope" line removed.

**Story 2 — `--affinity` flag**: New boolean flag requiring `--where`. Restores current affinity columns (+N more, unique count, classification sort). Adds `Subset` classification (shared/location_total >= 0.8 AND complementary == 0). Sort: Superset → Lead → Subset → Mirror. `--brief` without `--affinity` is silent no-op. `--affinity --brief` is silent no-op. `--detail complement` implies affinity (unchanged). Per-location total "(T total)" in both modes.

**Story 3 — `--detail overlap`**: Shows which selection files have copies at other locations. Per-location grouping with path cap (5, `--verbose` for all). `-0` for flat deduplicated output. Paths relative when scope relative, absolute when scope absolute; `-0` always absolute. Works with and without `--other`.

**Story 4 — `--detail residual`**: Shows selection files NOT shared with `--other` location(s). Requires `--other`. Includes unhashed sources. No path cap. `-0` for flat deduplicated output. Same path display convention.

**Cross-cutting — Path display**: Extract `format_path()` from `ls.rs` to `domain/path.rs`. Survey detail modes and `--detail unique` (retroactively) use it. `-0` always absolute. Human-readable: relative when scope was relative/cwd, absolute when scope was absolute.

## Current State

`survey.rs` (command, ~2500 lines including tests) and `domain/survey.rs` (pure domain, ~560 lines) implement the full survey pipeline. The current affinity gate is `!filters.is_empty() && !brief`. `DetailMode` has `Complement` and `Unique` variants. `LocationResult` has no `total_count` field. `LocationKind` has `Superset`, `Lead`, `Mirror` (no `Subset`). `classify_location()` takes 4 parameters. `compute_survey()` takes 9 individual parameters. `format_path()` is private in `ls.rs`. Tests call `run_compute()` which wraps `compute_survey()`.

Existing epic `epic-survey-command.md` is Done (6 stories completed). This is a new story for refinement.

## Design

### Phase 1: Orientation Default + Affinity Gate

- **Goal**: The existing affinity behavior moves behind `--affinity`. The new orientation output becomes the default. All infrastructure changes land here.
- **Scope**: Domain changes (`Subset`, `classify_location`), `format_path` extraction, `compute_survey` refactoring, affinity gate change, all output formatting changes, all existing test updates, new tests.

#### Changes

**`domain/survey.rs` — `LocationKind` enum:**

Add `Subset` between `Lead` and `Mirror`. Ord is derived — insertion order defines sort priority:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LocationKind {
    Superset,  // 0 — shared >= threshold AND has complementary
    Lead,      // 1 — has complementary content
    Subset,    // 2 — no complementary AND shared/location_total >= threshold
    Mirror,    // 3 — overlap only, no complementary, below subset threshold
}
```

**`domain/survey.rs` — `classify_location()` new signature:**

```rust
pub fn classify_location(
    shared_count: usize,
    total_hashed: usize,        // selection's hashed count
    complementary_count: usize,
    threshold: f64,             // renamed from superset_threshold
    location_total: usize,      // NEW: total hashed sources at location
) -> LocationKind
```

Logic:
```rust
if complementary_count == 0 {
    if location_total > 0 && shared_count as f64 / location_total as f64 >= threshold {
        return LocationKind::Subset;
    }
    return LocationKind::Mirror;
}
if total_hashed == 0 {
    return LocationKind::Lead;
}
let ratio = shared_count as f64 / total_hashed as f64;
if ratio >= threshold {
    LocationKind::Superset
} else {
    LocationKind::Lead
}
```

**`domain/path.rs` — `format_path()` extraction:**

Move from `ls.rs` (private) to `domain/path.rs` (public). Same signature:

```rust
pub fn format_path(full_path: &str, cwd: Option<&str>) -> String
```

Update `ls.rs` to call `domain::path::format_path()`.

**`survey.rs` — `SurveyOptions` gains `affinity` field:**

```rust
pub struct SurveyOptions {
    pub original_filters: Vec<String>,
    pub include: IncludeSet,
    pub other_paths: Vec<PathBuf>,
    pub affinity: bool,              // NEW
    pub brief: bool,
    pub detail: Option<DetailMode>,
    pub null_delim: bool,
    pub archive: Option<String>,
    pub verbose: bool,
}
```

**`survey.rs` — `LocationResult` gains `total_count` and detail path fields:**

```rust
struct LocationResult {
    path: String,
    shared_count: usize,
    total_count: usize,                     // NEW: always computed
    complementary_count: Option<usize>,
    only_here_count: Option<usize>,
    kind: Option<LocationKind>,
    complementary_paths: Option<Vec<String>>,
    overlap_paths: Option<Vec<String>>,     // NEW: populated in Phase 2
    residual_paths: Option<Vec<String>>,    // NEW: populated in Phase 3
}
```

The `overlap_paths` and `residual_paths` fields are added now (as `None`) but populated in their respective phases.

**`survey.rs` — `compute_survey()` signature refactored:**

```rust
fn compute_survey(
    conn: &mut Connection,
    paths: &[PathBuf],
    filters: &[Filter],
    options: &SurveyOptions,
    all_sources: &[Source],
    all_roots: &[domain::Root],
    other_paths: &[String],
    archive_root_id: Option<i64>,
) -> Result<SurveyOutcome>
```

Internal reads: `options.include`, `options.brief`, `options.affinity`, `options.detail`. Down from 9 (current) or 11 (if not refactored) to 8 parameters.

**Affinity gate change:**

```rust
// Current:
let compute_affinity = !filters.is_empty() && !brief;

// New:
let compute_affinity = (options.affinity || options.detail == Some(DetailMode::Complement)) && !options.brief;
```

**Total count computation** — in the per-location loop, alongside shared_count:

```rust
// Count all hashed sources at location (not excluding selection sources)
// Default mode: source-role only. --other mode: all roles.
let total_count: usize = all_sources
    .iter()
    .filter(|s| s.is_active())
    .filter(|s| !s.is_excluded())
    .filter(|s| s.object_id.is_some())
    .filter(|s| s.matches_scope(&loc_scope))
    .filter(|s| is_other_mode || s.is_from_role("source"))
    .count();
```

**Classification call updated** — passes `total_count` and renamed parameter:

```rust
let kind = domain::survey::classify_location(
    shared_count,
    total_hashed,
    comp_count,
    SUPERSET_THRESHOLD,
    total_count,        // NEW
);
```

The constant `SUPERSET_THRESHOLD` keeps its name (avoids touching unrelated test assertions).

**`main.rs` — CLI changes:**

```rust
/// Opt into affinity enrichment (requires --where)
#[arg(long)]
affinity: bool,
```

Wire to `SurveyOptions { affinity, ... }`.

**`run()` — New validation:**

```rust
if options.affinity && filter_strs.is_empty() {
    bail!("`--affinity` requires `--where` filters.");
}
```

**Output formatting changes:**

`print_selection_header()` → renamed to `print_survey_header()`:
- "Selection:" → "Survey:"
- "N sources" → "N sources here"
- New parameter `unique_count: usize`, displayed as "N unique here" after source count line
- Unique line only when `hashed > 0` (not shown for empty/all-unhashed early exits)
- "0 unique here" shown when count is zero (not omitted)

`print_related_locations()`:
- "shared" → "overlap" in per-location line
- Add "(T total)" using `loc.total_count`
- Affinity columns unchanged (gated on `complementary_count` being `Some` — naturally absent without `--affinity`)
- The `+N more` column text changes: "only here" → "unique" to match the spec's wording `(31 unique)`

Summary view in `run()`:
- Remove bottom `println!("{} unique to this scope", ...)` line
- Pass `result.unique_count` to `print_survey_header()`

**`--other` mode header** unchanged: still "Comparing with:" (not "Related locations:").

**Existing test updates:**

All tests calling `run_compute()` need updating because `compute_survey()` now takes `&SurveyOptions`:

- `run_compute()` helper updated to accept `&SurveyOptions` (or construct one internally from parameters — TBD during implementation, but the cleaner path is to accept `&SurveyOptions` since that's what `compute_survey` takes)
- All affinity-testing tests (`test_affinity_basic`, `test_affinity_only_here_reduced`, `test_affinity_unhashed_excluded`, `test_classification_sort`, `test_selection_narrowed_by_filter`, `test_same_root_complementary`, `test_mirror_with_filters`) pass `affinity: true` in options
- `test_no_filters_no_affinity` updated: tests that `--where` without `--affinity` still produces no affinity columns (stronger assertion than before)
- `test_brief_suppresses_affinity` updated: `affinity: true, brief: true` — affinity requested but suppressed
- `test_other_basic`, `test_other_zero_overlap`, `test_other_archive_root`, `test_other_same_root_cross_scope` with filters: pass `affinity: true`
- `test_other_with_brief`: pass `affinity: true, brief: true`
- `test_detail_complement_paths`, `test_detail_complement_mirror_has_empty_paths`, `test_complement_paths_relative_to_location`, `test_complement_other_mode_zero_overlap_has_paths`: complement implies affinity, no `affinity: true` needed in options, but internal gate must still compute it
- `test_detail_complement_no_affinity_has_none_paths`: this test asserts complementary_paths is None when no filters — now it also has no `--affinity`, so the assertion remains correct
- All tests asserting on `LocationResult` fields: add `total_count` assertions where meaningful

#### Tests

**Domain tests** (`domain/survey.rs`):
- `test_classify_subset` — complementary == 0, shared/location_total >= 0.8 → Subset
- `test_classify_subset_below_threshold` — complementary == 0, shared/location_total < 0.8 → Mirror
- `test_classify_subset_with_complementary` — complementary > 0, never Subset
- `test_classify_sort_order_with_subset` — verify Superset < Lead < Subset < Mirror

**`format_path()` unit tests** (`domain/path.rs`):
- `test_format_path_strips_cwd` — relative output when path under cwd
- `test_format_path_absolute_fallback` — absolute when path not under cwd
- `test_format_path_cwd_itself` — returns "." for exact cwd match
- `test_format_path_no_cwd` — returns absolute when cwd is None

**Command tests** (`survey.rs`):
- `test_orientation_default_no_filters` — no `--where`, no `--affinity`: "Survey:" header, "sources here", "unique here", "overlap" wording, "(T total)", no affinity columns, sort by shared desc
- `test_orientation_with_filters` — `--where` present, no `--affinity`: filters echoed, affinity columns still absent
- `test_zero_unique_shown` — all content elsewhere: "0 unique here" displayed
- `test_affinity_requires_where` — `--affinity` without `--where`: error message
- `test_affinity_brief_noop` — `--affinity` + `--brief`: no affinity columns (output identical to orientation)
- `test_brief_without_affinity_noop` — `--brief` without `--affinity`: identical to plain orientation
- `test_subset_classification` — location with complementary == 0 and shared/total >= 0.8 classified as Subset, sorted between leads and mirrors (with `--affinity`)
- `test_subset_vs_mirror` — location with complementary == 0 and shared/total < 0.8 classified as Mirror
- `test_total_count_in_summary` — "(T total)" in location output, value matches total hashed sources at location

### Phase 2: `--detail overlap`

- **Goal**: New detail mode showing which selection files overlap with each related location.
- **Scope**: `Overlap` variant in `DetailMode`, overlap path computation in `compute_survey`, `print_overlap_detail()`, `-0` support.

#### Changes

**`DetailMode` enum gains `Overlap`:**

```rust
pub enum DetailMode {
    Complement,
    Unique,
    Overlap,   // NEW
}
```

**`compute_survey()` — overlap path computation:**

When `options.detail == Some(DetailMode::Overlap)`, compute `overlap_paths` per location. The `loc_oids` set (object_ids at the location) is already computed for `shared_count` — hoist it to a `let` binding above both uses:

```rust
let overlap_paths = if options.detail == Some(DetailMode::Overlap) {
    let mut paths: Vec<String> = hashed
        .iter()
        .filter(|s| loc_oids.contains(&s.object_id.unwrap()))
        .map(|s| s.path())
        .collect();
    paths.sort_unstable();
    Some(paths)
} else {
    None
};
```

**New `print_overlap_detail()` function:**

Parameters: `locations`, `total_hashed`, `is_other_mode`, `verbose`, `cwd: Option<&str>`, `null_delim: bool`.

`-0` mode: collect all overlap paths across locations into `BTreeSet<String>` for sorted dedup, emit with `\0`. Always absolute paths (cwd not applied).

Human-readable mode:
- Header: "Overlapping with related locations (overlap):" or "...specified locations (overlap):"
- Location cap (DEFAULT_LOCATION_CAP unless `--verbose`)
- Per location: `"  /path/ (N of M overlap):"` then indented paths
- Path cap: COMPLEMENT_SAMPLE_SIZE (5) unless `--verbose`, with "... and N more" note
- Paths formatted with `format_path(path, cwd)` — relative when scope relative, absolute when scope absolute
- Empty case: "No overlapping content found." or "No overlapping content at specified locations."
- Location truncation notice when applicable

**`run()` orchestration:**

```rust
Some(DetailMode::Overlap) => {
    if !options.null_delim {
        print_survey_header(...);
        println!();
    }
    let cwd = if options.null_delim { None } else { display_cwd.as_deref() };
    print_overlap_detail(..., cwd, options.null_delim);
}
```

Location sort follows mode: shared desc by default, classification sort with `--affinity`.

#### Tests

- `test_overlap_detail_basic` — per-location grouping, "(overlap)" in header, path cap at 5
- `test_overlap_detail_with_other` — "specified locations" in header, works with `--other`
- `test_overlap_detail_null` — `-0`: flat, deduplicated, absolute paths, no headers
- `test_overlap_detail_no_overlap` — "No overlapping content found." message
- `test_overlap_multi_location_dedup` — source overlapping with 2 locations: under both in human-readable, once in `-0`

### Phase 3: `--detail residual`

- **Goal**: New detail mode showing selection files NOT shared with reference location(s).
- **Scope**: `Residual` variant in `DetailMode`, residual path computation, `print_residual_detail()`, `-0` support, `--other` validation.

#### Changes

**`DetailMode` enum gains `Residual`:**

```rust
pub enum DetailMode {
    Complement,
    Unique,
    Overlap,
    Residual,  // NEW
}
```

**`run()` — New validation:**

```rust
if options.detail == Some(DetailMode::Residual) && options.other_paths.is_empty() {
    bail!("`--detail residual` requires `--other` to specify a reference location.");
}
```

**`compute_survey()` — residual path computation:**

When `options.detail == Some(DetailMode::Residual)`, compute `residual_paths` per location. Uses full `selection` (not just `hashed`) — unhashed sources are always residual:

```rust
let residual_paths = if options.detail == Some(DetailMode::Residual) {
    let mut paths: Vec<String> = selection
        .iter()
        .filter(|s| match s.object_id {
            Some(oid) => !loc_oids.contains(&oid),
            None => true,  // unhashed always residual
        })
        .map(|s| s.path())
        .collect();
    paths.sort_unstable();
    Some(paths)
} else {
    None
};
```

**New `print_residual_detail()` function:**

Parameters: `locations`, `cwd: Option<&str>`, `null_delim: bool`.

`-0` mode: collect all residual paths across locations into `BTreeSet<String>` for sorted dedup, emit with `\0`. Always absolute.

Human-readable mode:
- Per location: `"Not at /path/ (residual):"` then indented paths
- No path cap (spec: "All residual paths shown")
- Paths formatted with `format_path(path, cwd)`
- Zero residual: `"  (none)"`
- No location header above groupings (unlike overlap, which has a collective header) — each location is self-describing via "Not at /path/"

**`run()` orchestration:**

```rust
Some(DetailMode::Residual) => {
    if !options.null_delim {
        print_survey_header(...);
        println!();
    }
    let cwd = if options.null_delim { None } else { display_cwd.as_deref() };
    print_residual_detail(&result.location_results, cwd, options.null_delim);
}
```

#### Tests

- `test_residual_detail_basic` — paths NOT shared with `--other` location, "(residual)" in header
- `test_residual_requires_other` — error without `--other`
- `test_residual_includes_unhashed` — unhashed sources appear in residual
- `test_residual_zero` — all content overlaps: "(none)" displayed
- `test_residual_null` — `-0`: flat, deduplicated, absolute paths
- `test_residual_multiple_other` — separate listing per `--other` location

### Phase 4: Documentation and `--detail unique` Path Display

- **Goal**: Update all documentation, retroactive path display fix for `--detail unique`.
- **Scope**: `print_unique_detail()` update, docs, CLAUDE.md.

#### Changes

**`print_unique_detail()` updated:**

```rust
fn print_unique_detail(paths: &[String], null_delim: bool, cwd: Option<&str>) {
    let sep = if null_delim { "\0" } else { "\n" };
    for path in paths {
        let display = if null_delim {
            path.clone()  // -0: always absolute
        } else {
            domain::path::format_path(path, cwd)
        };
        print!("{}{}", display, sep);
    }
}
```

**`run()` — pass cwd to unique detail:**

```rust
Some(DetailMode::Unique) => {
    let cwd = if options.null_delim { None } else { display_cwd.as_deref() };
    print_unique_detail(&result.unique_paths, options.null_delim, cwd);
}
```

#### Tests

- Update existing `test_unique_paths_populated` and `test_unique_paths_empty_when_none_unique` — paths may now be relative depending on scope framing
- `test_unique_detail_relative_paths` — unique paths relative when scope was relative

#### Documentation

**`docs/src/commands/query/survey.md`** — Significant rewrite:
- New default output format (orientation mode)
- `--affinity` flag documentation
- `--detail overlap` and `--detail residual` documentation
- Updated "Reading the output" section with new wording
- Updated examples throughout
- `--affinity` added to options table
- "The three dispositions" section updated to mention Subset
- Updated "Typical workflow" section

**CLAUDE.md** — Survey section updates:
- Mention orientation/affinity modes
- Add `--affinity` to flag vocabulary
- Document `Subset` in `LocationKind`
- Add `format_path()` to `domain/path.rs` utilities section
- Update `DetailMode` enum documentation

## Architectural Decisions

| Decision | Rationale |
|----------|-----------|
| Single `threshold` parameter for both Superset and Subset | Same value (0.8), same concept ("most content overlaps"). Split if they diverge. |
| `format_path()` in `domain/path.rs` | Pure path utility with no I/O. The `cwd` parameter is resolved in command layer and passed down. |
| `overlap_paths` and `residual_paths` on `LocationResult` | Same pattern as `complementary_paths`. Per-location data travels with the location, preserving sort order. |
| `compute_survey` takes `&SurveyOptions` | Reduces parameter count (8 vs 11). Options struct already exists as the public API. Function reads what it needs, ignores output-only fields. |
| `total_count` always computed | Needed for display in both modes and for Subset classification. Same filter chain as shared_count minus the selection-overlap check. |
| Overlap/residual paths computed inside `compute_survey()` | Consistent with `unique_paths` and `complementary_paths`. Keeps all computation in one place. |
| `SUPERSET_THRESHOLD` constant name unchanged | Avoids churning test assertions. The renamed parameter (`threshold`) is internal to `classify_location()`. |

## Non-Goals

- `--triage` mode flag (deferred per spec)
- Changing `--detail complement` behavior (unchanged)
- Changing archive section formatting (unchanged)
- Interactive or stateful survey features
- Changing how `--other` resolves paths

## Test Plan

### Existing Tests (Must Pass)

All existing tests in `survey.rs` (37 tests) updated for new output format and affinity gate. Tests that were testing affinity behavior pass `affinity: true`. All domain tests in `domain/survey.rs` pass with the new `classify_location()` signature.

### New Tests

**Phase 1** (14 new tests):
- 4 domain classification tests (Subset)
- 4 `format_path()` unit tests
- 3 orientation default tests
- 3 affinity gate tests (requires-where, brief-noop combinations)

**Phase 2** (5 new tests):
- Overlap detail: basic, with-other, null, no-overlap, multi-location-dedup

**Phase 3** (6 new tests):
- Residual detail: basic, requires-other, includes-unhashed, zero, null, multiple-other

**Phase 4** (1 new test):
- Unique detail relative paths

## Implementation Checklist

- [x] Phase 1: Orientation default + affinity gate + infrastructure
- [ ] Phase 2: `--detail overlap`
- [ ] Phase 3: `--detail residual`
- [ ] Phase 4: Documentation + `--detail unique` path display
- [ ] Verify all existing tests pass
- [ ] Update CLAUDE.md
- [ ] Update docs/src/commands/query/survey.md

## Documentation Updates

- `docs/src/commands/query/survey.md` — significant rewrite covering new default output, `--affinity`, `--detail overlap`, `--detail residual`, updated examples and "Reading the output" section
- `CLAUDE.md` — survey section updates for modes, flag vocabulary, Subset classification, `format_path()` utility

## Backward Compatibility

The main behavioral change is the affinity gate: users who ran `canon survey ... --where "..."` and expected affinity columns must now add `--affinity`. This is intentional — the change distinguishes orientation from assembly. All other changes are additive (new flags, new detail modes) or cosmetic (header text, wording).

Output format changes ("Selection:" → "Survey:", "shared" → "overlap", etc.) affect any scripts parsing survey output. This is acceptable for a pre-v1 tool with no established user base beyond the creator.

## Performance Considerations

`total_count` computation adds one pass over `all_sources` per location (same filter chain as shared_count). Bounded by location cap for display purposes, but computed for all discovered locations. In practice, the number of locations is small (typically < 20) and the filter chain is in-memory — negligible overhead.

Overlap and residual path computation is bounded by selection size (already in memory). No new database queries.
