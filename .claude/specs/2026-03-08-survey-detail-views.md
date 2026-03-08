# Story: Survey Detail Views (`--detail complement` and `--detail unique`)

**Status**: Pending
**Created**: 2026-03-08

## Objective

Add two focused output modes to `canon survey` that replace the summary view with actionable detail. `--detail complement` shows the actual complementary files at related locations — the content that belongs with the selection but isn't in the current scope. `--detail unique` outputs bare paths of content that exists nowhere else, suitable for piping. Together they complete the two-step workflow within survey: summary to see the landscape, detail to act on it.

## Functional Requirements Summary

### `--detail complement` (Functional Spec Story 2)
- Requires `--where` (error without: "`--detail complement` requires `--where` filters to define matching content.")
- Light header: selection echo + counts (reuses `print_selection_header`)
- Section header: "Complementary content at related locations:" (default) / "Complementary content at specified locations:" (`--other`)
- Per location: path, complementary count, only-here count, then source paths relative to location
- Paths sorted alphabetically within each location
- Per-location path cap of 5 (`COMPLEMENT_SAMPLE_SIZE`), `--verbose` shows all, "... and N more" note
- Default mode: mirrors omitted (no complementary content = low signal)
- `--other` mode: mirrors shown with "— no complementary content (N of M shared)" note
- Sort: same as summary (classification in default, user order in `--other`)
- Replaces summary (not additive)

### `--detail unique` (Functional Spec Story 3)
- Bare paths, one per line, suitable for piping
- No header, no framing — clean output
- `-0` for null-delimited output (for `xargs -0`)
- Empty output (no unique sources) produces no output, exit 0
- Works with or without `--where`, with or without `--other`
- Replaces summary (not additive)

### Mutual Exclusion
- `--brief` and `--detail` are mutually exclusive (clap `conflicts_with`)

## Current State

`compute_survey` already computes all the necessary data — shared counts, complementary counts, only-here counts, unique count, classification. But it discards source identities:
- Complementary sources are counted (`complementary.len()`) but paths are not retained
- Unique count comes from `count_unique_to_selection()` which returns `usize`, not the set of unique object IDs

The enrichment needed: carry paths alongside counts in the result structs.

## Design

### Phase 1: Domain + Data Plumbing
- **Goal**: Add `find_unique_object_ids` to domain, enrich result structs with paths, all existing tests pass, all new tests pass
- **Scope**: Domain function, enriched compute output, `DetailMode` enum, `SurveyOptions` expansion

#### Changes

**`domain/survey.rs`** — new function + delegation:

```rust
/// Find selection object_ids with no source outside the selection.
/// Returns the set of unique object_ids.
pub fn find_unique_object_ids(
    selection_object_ids: &HashSet<i64>,
    selection_source_ids: &HashSet<i64>,
    by_object_id: &HashMap<i64, Vec<&Source>>,
) -> HashSet<i64>
```

Refactor `count_unique_to_selection` to delegate: `find_unique_object_ids(...).len()`.

**`survey.rs`** — new types:

```rust
#[derive(Clone, Copy, PartialEq, clap::ValueEnum)]
pub enum DetailMode {
    Complement,
    Unique,
}
```

Add to `SurveyOptions`:
```rust
pub detail: Option<DetailMode>,
pub null_delim: bool,
pub verbose: bool,
```

Add to `LocationResult`:
```rust
complementary_paths: Option<Vec<String>>,  // relative to location, sorted; None when no affinity
```

Add to `SurveyResult`:
```rust
unique_paths: Vec<String>,  // absolute paths, sorted; always populated
```

**`compute_survey`** changes:

1. Unique paths — replace `count_unique_to_selection` call:
   ```rust
   let unique_oids = domain::survey::find_unique_object_ids(
       &sel_object_ids, &sel_source_ids, &by_object_id,
   );
   let unique_count = unique_oids.len();
   let mut unique_paths: Vec<String> = hashed.iter()
       .filter(|s| unique_oids.contains(&s.object_id.unwrap()))
       .map(|s| s.path())
       .collect();
   unique_paths.sort_unstable();
   ```

2. Complementary paths — in affinity block, after `complementary` vec:
   ```rust
   let mut comp_paths: Vec<String> = complementary.iter()
       .filter_map(|s| {
           domain::path::path_strip_prefix(&s.path(), scope_path)
               .map(|p| p.to_string())
       })
       .collect();
   comp_paths.sort_unstable();
   // LocationResult gets: complementary_paths: Some(comp_paths)
   ```

3. No-affinity branch: `complementary_paths: None`

#### Tests

**Domain** (`domain/survey.rs`):
1. `test_find_unique_object_ids_returns_set` — Two objects, one unique, one with copy outside. Returns HashSet with only the unique one.
2. `test_find_unique_object_ids_delegates_correctly` — Same inputs to both functions, `count_unique_to_selection` == `find_unique_object_ids().len()`.

**Integration** (`survey.rs`):
3. `test_detail_complement_paths` — Selection + related location with complementary content. Assert `complementary_paths` contains correct relative paths, sorted.
4. `test_detail_complement_mirror_has_empty_paths` — Mirror location with filters: `complementary_paths == Some(vec![])`, not `None`.
5. `test_detail_complement_no_affinity_has_none_paths` — Without `--where`: `complementary_paths == None`.
6. `test_unique_paths_populated` — 3 objects, 1 unique, 1 copied, 1 archived. Assert `unique_paths` contains only the unique source path.
7. `test_unique_paths_empty_when_none_unique` — All objects have copies. `unique_paths` is empty.
8. `test_unique_paths_duplicates_within_selection` — Two sources share same unique object_id. `unique_paths` has both paths, `unique_count` is 1.
9. `test_complement_paths_relative_to_location` — Location `/mnt/backup/trip`, source at `.../trip/sub/IMG.jpg`. Path is `sub/IMG.jpg`.
10. `test_complement_other_mode_zero_overlap_has_paths` — `--other` with zero overlap but matching complementary content: `complementary_paths` populated.

### Phase 2: CLI + Validation + Output
- **Goal**: Full end-to-end `--detail complement` and `--detail unique` working from CLI
- **Scope**: CLI plumbing, validation, output functions, dispatch

#### Changes

**`main.rs`** — Survey variant additions:
```rust
/// Show detailed output (complement or unique)
#[arg(long, value_enum)]
detail: Option<survey::DetailMode>,
/// Output null-delimited paths (for --detail unique)
#[arg(short = '0', long = "null")]
null_delim: bool,
/// Show all paths per location
#[arg(long)]
verbose: bool,
```

Add `conflicts_with = "detail"` to the existing `--brief` flag.

Update dispatch to pass `detail`, `null_delim`, `verbose` into `SurveyOptions`.

**`survey.rs`** — validation in `run()`:
```rust
if options.detail == Some(DetailMode::Complement) && filter_strs.is_empty() {
    bail!("`--detail complement` requires `--where` filters to define matching content.");
}
```

**`survey.rs`** — early exit handling for `--detail unique`:
- `Empty` and `AllUnhashed` outcomes: produce no output when `detail == Some(Unique)`

**`survey.rs`** — output dispatch in `run()`:
```rust
match options.detail {
    Some(DetailMode::Complement) => {
        print_selection_header(...);
        println!();
        print_complement_detail(&result.location_results, result.total_hashed,
                                result.is_other_mode, options.verbose);
    }
    Some(DetailMode::Unique) => {
        print_unique_detail(&result.unique_paths, options.null_delim);
    }
    None => { /* existing summary */ }
}
```

**`survey.rs`** — new output functions:

`print_complement_detail(locations, total_hashed, is_other_mode, verbose)`:
- Section header varies by mode (related/specified)
- Iterates locations; default mode skips mirrors, `--other` mode shows mirrors with note
- Per location with complementary content: header line with `(+N, K only here):`, then indented paths
- Path cap: `COMPLEMENT_SAMPLE_SIZE = 5`, `--verbose` shows all, "... and N more" when capped
- Empty state: "No complementary content found at related/specified locations."

`print_unique_detail(paths, null_delim)`:
- Bare output: `print!("{path}{sep}")` where sep is `"\0"` or `"\n"`
- No header, no framing

#### Tests

No new tests in Phase 2. The data correctness is verified in Phase 1. Phase 2 is mechanical CLI wiring and output formatting.

## Architectural Decisions

| Decision | Rationale |
|----------|-----------|
| Always populate paths in compute, not mode-gated | Keeps compute mode-independent. Cost negligible (in-memory mapping of already-loaded sources). Output layer decides what to show. |
| `find_unique_object_ids` in domain, `count_` delegates | Single source of truth. Existing tests unchanged. New capability cleanly exposed. |
| Validation in `run()`, not compute | `compute_survey` stays mode-unaware. Validation is a command-layer concern. |
| `--detail unique` bare output (no header) | Piping requires clean output. Consistent with `ls -0` philosophy. |
| `complementary_paths` is `Option<Vec>` | `None` = no affinity computed; `Some(vec![])` = affinity computed, no complementary. Preserves the distinction. |
| `unique_paths` lists sources, `unique_count` counts objects | Users see files (each duplicate within selection is a distinct file). The count is object-level (content uniqueness). Both are correct for their purpose. |
| `-0` without `--detail unique` is a no-op | No validation needed. Summary output isn't line-delimited paths, so the flag has no effect. Follows the principle of least surprise. |

## Non-Goals

- Location count cap (`--verbose` for location count) — Story 6
- `--archive` flag — Story 6
- Output column alignment / formatting polish — Story 6
- CLAUDE.md updates — Story 6
- docs/ updates — Story 6

## Test Plan

### Existing Tests (Must Pass)
- All 23 domain/survey.rs tests
- All 27 survey.rs integration tests
- Full `cargo test` suite

### New Tests
- Phase 1: 2 domain tests + 8 integration tests (10 total)

## Implementation Checklist
- [x] Phase 1: Domain function + data plumbing + 10 tests
- [x] Phase 2: CLI + validation + output formatting
- [x] Verify all existing tests pass
- [x] Update epic spec: Story 5 status → Done

## Backward Compatibility

No changes to existing behavior. All new functionality is behind new flags (`--detail`, `-0`, `--verbose`). Summary output unchanged when no `--detail` flag is provided.

## Performance Considerations

- `unique_paths` computation: one pass over `hashed` sources filtered by unique object IDs. Bounded by selection size. Negligible.
- `complementary_paths` computation: `path_strip_prefix` + string allocation per complementary source. Already bounded by existing affinity computation (which does the heavy `apply_filters` work).
- Memory: paths are strings derived from already-loaded Source objects. No additional DB queries.
