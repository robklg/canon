# Story: Survey Polish and Documentation

**Epic**: [epic-survey-command.md](epic-survey-command.md)
**Status**: Done
**Created**: 2026-03-08

## Objective

Complete the survey command with remaining features (`--archive`, location cap, `--verbose` for summary) and documentation (CLAUDE.md, docs/). This is the final story in the survey epic — after this, the command is fully shipped.

## Functional Requirements Summary

**`--archive <spec>`**: Accepts a root spec (`id:N` or `path:/foo/bar`) identifying an archive root. Filters the archive section to show coverage relative to that specific archive only. Error if the specified root is not an archive. Without `--archive`, coverage checks against all archive roots (existing behavior). Header changes to "Archived (in /archive/photos):" when specified.

**Location cap**: Related locations are capped at 10 (after sorting). Shows "... and N more locations (use --verbose to show all)" when truncated. `--verbose` overrides the cap.

**`--verbose` in summary view**: Currently `--verbose` only affects complement detail view (paths-per-location cap). Extend it to summary view to override the location cap. The single flag naturally serves both "show everything" purposes.

**CLAUDE.md**: Add survey command to the documented commands list, document key architectural concepts (asymmetric visibility, "only here" counts objects).

**docs/**: New survey command reference page, update SUMMARY.md and query index.

## Current State

Stories 1-5 are complete. The survey command is fully functional:
- All CLI flags except `--archive` are wired
- Full computation pipeline: selection, object index, archive status, overlap, scope discovery, affinity, classification, unique
- All output modes: summary, `--detail complement`, `--detail unique`
- `--other` and `--brief` modes working
- `--verbose` wired for complement detail view
- Column alignment, thousands separators, percentages all working
- 24 integration tests + 23 domain unit tests passing

## Design

### Phase 1: `--archive` flag and location cap

- **Goal**: All functional features complete and tested.
- **Scope**: `--archive` flag, location cap with `--verbose` override, 4 new tests.

#### Changes

**`src/main.rs`** — Add `--archive` to Survey CLI variant:

```rust
/// Filter archive section to a specific archive (id:N or path:/foo/bar)
#[arg(long)]
archive: Option<String>,
```

Add `archive` to the Survey dispatch destructure and pass to `SurveyOptions`:

```rust
let options = survey::SurveyOptions {
    // ... existing fields ...
    archive,
};
```

**`src/survey.rs`** — Multiple changes:

1. **`SurveyOptions`** — add field:
   ```rust
   pub archive: Option<String>,
   ```

2. **`SurveyResult`** — add field:
   ```rust
   archive_label: Option<String>,
   ```

3. **New constant**:
   ```rust
   const DEFAULT_LOCATION_CAP: usize = 10;
   ```

4. **`run()`** — parse archive spec after fetching roots, before `compute_survey()`:
   ```rust
   let archive_root_id = if let Some(ref spec) = options.archive {
       Some(domain::root::parse_root_spec(&all_roots, spec, Some("archive"))?)
   } else {
       None
   };
   let archive_label = archive_root_id.map(|id| {
       let root = all_roots.iter().find(|r| r.id == id).unwrap();
       format!("in {}", root.path)
   });
   ```
   Pass `archive_root_id` to `compute_survey()`. After it returns, attach `archive_label` to the result:
   ```rust
   SurveyOutcome::Result(mut result) => {
       result.archive_label = archive_label;
       // ... existing match arms ...
   }
   ```
   Pass `options.verbose` to `print_related_locations()` in summary mode. Pass `result.archive_label.as_deref()` to `print_archive_section()`.

5. **`compute_survey()`** — add `archive_root_id: Option<i64>` parameter. In the archive source collection loop, skip archive sources whose `root_id` doesn't match the target:
   ```rust
   if let Some(target_id) = archive_root_id {
       if sib.root_id != target_id {
           continue;
       }
   }
   ```

6. **`print_archive_section()`** — add `archive_label: Option<&str>` parameter. When present, header reads "Archived (in /archive/photos):" instead of "Archived:". Applies to both zero and non-zero cases.

7. **`print_related_locations()`** — add `verbose: bool` parameter. Compute display slice:
   ```rust
   let display_locations = if verbose || locations.len() <= DEFAULT_LOCATION_CAP {
       locations
   } else {
       &locations[..DEFAULT_LOCATION_CAP]
   };
   let truncated_count = locations.len() - display_locations.len();
   ```
   Compute column alignment on `display_locations`. Iterate `display_locations`. After the loop:
   ```rust
   if truncated_count > 0 {
       println!(
           "  ... and {} more locations (use --verbose to show all)",
           format_count(truncated_count),
       );
   }
   ```

**Test helper** — update `run_compute()` signature to add `archive_root_id: Option<i64>`. Update all 24 existing calls to pass `None`.

#### Tests

1. **`test_archive_filter_specific_root`** — Two archive roots, content archived in both. Pass `archive_root_id` for one. Assert `archived_source_count` and `archive_scopes` reflect only that archive.

2. **`test_archive_filter_no_matches`** — Archive root specified that has none of the selection's content. Assert `archived_source_count == 0`, `archive_scopes` empty.

3. **`test_archive_filter_does_not_affect_other_sections`** — Two archive roots, overlap on source roots. Pass `archive_root_id`. Assert `location_results`, `unique_count` unchanged vs. without archive filter.

4. **`test_many_locations_all_computed`** — 15 source roots each with one overlapping source. Assert `location_results.len() == 15` — cap is output-only, doesn't affect computation.

### Phase 2: CLAUDE.md updates

- **Goal**: Internal documentation reflects the survey command.
- **Scope**: CLAUDE.md additions only.

#### Changes

Add to CLAUDE.md:

- In the **Command Modules** list, add `survey.rs` with description: "Survey scope for archive status, related locations, unique content"
- In the **Domain Layer** list, add `domain/survey.rs` with its key types and functions
- In the **CLI Flag Vocabulary** section, add `survey` to the list of commands that accept `--include`
- In the **Commands** list, add `survey` with brief description
- Add a brief note about asymmetric visibility model (selection side: source roots only; outward side: source + archive roots)
- Note that "only here" counts unique object_ids, not sources (exception to source-based counting convention)
- Document survey's relationship to `coverage` (project-level progress vs. selection-level context) and `compare` (asymmetric vs. symmetric)

### Phase 3: User documentation

- **Goal**: Complete user-facing documentation.
- **Scope**: New docs page + two small updates.

#### Changes

**New file: `docs/src/commands/query/survey.md`**

Structure:
- **Purpose**: Workflow position (after `ls`, before `cluster`). What questions it answers.
- **Usage**: `canon survey [paths]... [OPTIONS]`
- **Options table**: All flags with descriptions
- **Reading the output**: Walk through each section (Selection, Archived, Related locations, Unique). Explain the three dispositions (Superset, Lead, Mirror).
- **Detail views**: `--detail complement` (requires `--where`) and `--detail unique` (bare paths, pipeable, `-0`).
- **Directed comparison**: `--other` mode — when and why. Header and sort differences.
- **Fast first pass**: `--brief` — skips affinity, good for initial exploration.
- **Archive filtering**: `--archive` — scoping to a specific archive.
- **Examples**: Adapted from the functional spec — summary with filters, without filters, `--detail complement`, `--detail unique`, `--other`, `--brief`.

**Update `docs/src/SUMMARY.md`** — add under Querying:
```markdown
    - [`survey`](commands/query/survey.md)
```

**Update `docs/src/commands/query/index.md`** — add survey to the command list with brief description.

## Architectural Decisions

| Decision | Rationale |
|----------|-----------|
| Archive filtering at source collection, not in domain functions | Domain functions are pure and take pre-filtered data. Filtering is a selection concern in the command layer. |
| `archive_label` attached to result after `compute_survey()` returns | Display concern stays out of the computation function. Computed in `run()` from root lookup. |
| Location cap is output-only | All locations are computed regardless of cap. Avoids re-computation when `--verbose` is used. Cap of 10 means at most 10 affinity passes — already bounded. |
| `--verbose` serves both location cap and complement path cap | Natural "show everything" semantics. Single flag, two truncation points. |

## Non-Goals

- Changing any domain logic or repo functions
- Adding new computation that isn't in the current pipeline
- Reformatting output that already works (column alignment, spacing are done)

## Test Plan

### Existing Tests (Must Pass)

All 24 integration tests in `survey.rs` (updated with `None` for new `archive_root_id` parameter) and 23 domain unit tests in `domain/survey.rs`.

### New Tests

| Test | Phase | What it verifies |
|------|-------|-----------------|
| `test_archive_filter_specific_root` | 1 | Archive section filtered to one root |
| `test_archive_filter_no_matches` | 1 | Zero archived when target archive has no matches |
| `test_archive_filter_does_not_affect_other_sections` | 1 | Overlap, complementary, unique unchanged by `--archive` |
| `test_many_locations_all_computed` | 1 | Location cap doesn't affect computation |

## Implementation Checklist

- [ ] Phase 1: `--archive` flag, location cap, `--verbose` for summary, 4 new tests
- [ ] Phase 2: CLAUDE.md updates
- [ ] Phase 3: User documentation (survey.md, SUMMARY.md, query index)
- [ ] Verify all existing tests pass
- [ ] Update epic status to Done

## Documentation Updates

- CLAUDE.md: survey command entry, domain/survey.rs entry, asymmetric visibility, "only here" counts objects, relationship to coverage/compare
- New page: `docs/src/commands/query/survey.md`
- Update: `docs/src/SUMMARY.md` (add survey under Querying)
- Update: `docs/src/commands/query/index.md` (add survey to list)

## Backward Compatibility

No changes to user-visible behavior. `--archive` is additive. Location cap adds truncation where previously all locations were shown — this is an improvement, not a breaking change. `--verbose` overrides the cap for users who want the previous behavior.

## Performance Considerations

None. All changes are in the output layer or filter an existing computation. No new database queries, no new domain logic.
