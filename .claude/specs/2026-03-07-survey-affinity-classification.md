# Story: Survey Affinity and Classification

**Epic**: [epic-survey-command.md](epic-survey-command.md)
**Status**: Done
**Created**: 2026-03-07
**Depends on**: Story 2 (Summary View)

## Objective

Add `--where` filter support to `survey`. When filters are present, the command computes per-location affinity: how much complementary content exists at each related location, and how much of that is unique to that location. Locations are classified (superset/lead/mirror) and sorted by classification priority.

This is the story that unlocks the full power of survey — answering "is my selection complete?" with actionable data about where to find complementary content.

## Functional Requirements Summary

With `--where` filters, the output gains two new columns on related locations:

```
$ canon survey /mnt/old-drive/photos --where "@image AND source.mtime|year=2016"

Selection: /mnt/old-drive/photos
  Filters: @image AND source.mtime|year=2016
  400 sources (12 unhashed, 388 hashed)

Archived: 285 of 388 (73.5%)
  /archive/photos/2016/                      285

Related locations:
  /mnt/backup-2022/photos/italy-2016/     380 of 388 shared   +95 more (31 only here)
  /mnt/partner-laptop/DCIM/vacation/       45 of 388 shared  +180 more (42 only here)
  /mnt/backup-2022/photos/misc/            30 of 388 shared

76 unique to this scope
```

**What's new vs Story 2:**
- `--where` flag with alias expansion
- Filter echo in selection header ("Filters: ...")
- Per-location affinity: "+N more" (complementary count) and "(K only here)" columns
- Classification-based sort: supersets → leads → mirrors
- Mirrors show no affinity columns (they have zero complementary content)

**What's NOT included** (later stories):
- `--detail`, `--other`, `--brief`, `--archive`, `--verbose`, `-0`
- Location cap and truncation

## Current State

**Exists after Story 2:**
- `src/survey.rs` — command module with `compute_survey()` (pure — no `conn` parameter), `SurveyResult`/`SurveyOutcome` types, output formatting, 10 integration tests
- `Survey` variant in `Commands` with `paths` and `--include`
- Selection pipeline, object index, archive status, overlap with scope discovery, unique count
- `domain/survey.rs` — all pure domain functions: `discover_scopes`, `count_only_here`, `count_unique_to_selection`, `classify_location`, `LocationKind`

**Exists in the broader codebase:**
- `filter::apply_filters(conn: &mut Connection, &source_ids, filters) -> Result<Vec<i64>>` — evaluates `--where` expressions (note: requires `&mut Connection`)
- `alias::expand_filter_strings(&filters, &canon_home) -> Result<Vec<String>>` — expands `@alias` references
- `Filter::parse(s) -> Result<Filter>` — parses a filter expression string (type alias: `Filter = Expr`)
- The `cluster generate` pattern in main.rs for passing both original and expanded filter strings

## Design

### Phase 1: CLI, Filter Plumbing, and Selection Filtering

- **Goal**: `--where` is accepted, parsed, alias-expanded, passed to `survey::run()`, and applied to narrow the selection.

#### Changes

**`src/main.rs`** — expand the Survey variant:

```rust
Survey {
    paths: Vec<PathBuf>,
    /// Filter expressions (e.g., "source.ext=jpg" or "content.hash.sha256?")
    #[arg(long = "where")]
    filters: Vec<String>,
    #[arg(long, value_delimiter = ',')]
    include: Vec<IncludeValue>,
},
```

Dispatch — follow the `cluster generate` pattern of passing both original and expanded filters:

```rust
Commands::Survey {
    paths,
    filters,
    include,
} => {
    let expanded = alias::expand_filter_strings(&filters, &canon_home)?;
    let include = include_set_from(&include);
    if include.includes_archived() {
        bail!("--include archived is not valid for survey");
    }
    let options = survey::SurveyOptions {
        original_filters: filters,
        include,
    };
    survey::run(&mut db, &paths, &expanded, &options)?;
}
```

**`src/survey.rs`** — introduce `SurveyOptions` and update `run()`:

```rust
use crate::expr::filter::{self, Filter};

/// Options controlling survey behavior. Grows as later stories add flags.
pub struct SurveyOptions {
    /// Original (pre-expansion) filter strings — for display in selection header.
    pub original_filters: Vec<String>,
    /// Visibility control (--include excluded).
    pub include: IncludeSet,
    // Story 4 will add: other_paths, brief
    // Story 5 will add: detail, null_delim
    // Story 6 will add: archive, verbose
}

pub fn run(
    db: &mut Db,
    paths: &[PathBuf],
    filter_strs: &[String],       // expanded, for parsing and evaluation
    options: &SurveyOptions,
) -> Result<()> {
    // Parse expanded filter strings
    let filters: Vec<Filter> = filter_strs
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    let conn = db.conn_mut();  // was db.conn() — changed for apply_filters

    let all_roots = repo::root::fetch_all(conn)?;
    let root_ids: Vec<i64> = all_roots.iter().map(|r| r.id).collect();
    let all_sources = repo::source::batch_fetch_by_roots(conn, &root_ids)?;

    match compute_survey(conn, paths, &options.include, &filters, &all_sources, &all_roots)? {
        // ... output formatting, using options.original_filters for header echo
    }
}
```

The `SurveyOptions` pattern follows `CompareOptions`, `GenerateOptions`, and `ApplyOptions` — behavioral configuration collected into a struct while primary data (`paths`, `filter_strs`) stays as direct parameters. `original_filters` lives in the options because it's display configuration, not computational input. `compute_survey` receives only what it needs (`&options.include`, `&filters`) — it never sees the options struct directly.

**Key change: `db.conn()` → `db.conn_mut()`**. Required because `apply_filters` takes `&mut Connection`. The `&mut` reborrows as `&` for read-only repo calls (`fetch_all`, `batch_fetch_by_roots`), so all existing calls continue to work.

**`compute_survey()` gains `conn` and `filters` parameters:**

```rust
fn compute_survey(
    conn: &mut Connection,        // NEW — for apply_filters calls
    paths: &[PathBuf],
    include: &IncludeSet,
    filters: &[Filter],           // NEW — parsed filter expressions
    all_sources: &[Source],
    all_roots: &[domain::Root],
) -> Result<SurveyOutcome>
```

Story 2's `compute_survey` was purely functional (no `conn`). Adding `conn` is necessary because affinity computation (Phase 2) calls `apply_filters` per location. The function is still testable — tests use `open_in_memory_for_test()`.

**Apply `--where` to selection** — standard pattern from `ls.rs`, inserted after the domain predicate chain:

```rust
    // Build selection from domain predicates (existing Story 2 code)
    let selection: Vec<&Source> = all_sources.iter()
        .filter(|s| s.is_active())
        .filter(|s| s.is_from_role("source"))
        .filter(|s| s.matches_scope(&scopes))
        .filter(|s| include.includes_excluded() || !s.is_excluded())
        .collect();

    // Apply --where filters to selection (NEW)
    let selection = if filters.is_empty() {
        selection
    } else {
        let ids: Vec<i64> = selection.iter().map(|s| s.id).collect();
        let passed: HashSet<i64> = filter::apply_filters(conn, &ids, filters)?
            .into_iter().collect();
        selection.into_iter().filter(|s| passed.contains(&s.id)).collect()
    };
```

The rest of `compute_survey` (partition, object index, archive, overlap, unique) operates on the narrowed selection. No changes needed downstream — the `selection` variable is rebound.

**Why both original and expanded**: The selection header echoes what the user typed (`Filters: @image AND source.mtime|year=2016`), not the expanded form. The expanded form is what gets parsed and evaluated. This matches the `cluster generate` pattern. `original_filters` lives in `SurveyOptions` for `run()` to use in formatting; `compute_survey` never sees them.

#### Tests

None in this phase — wiring only, tested through subsequent phases.

### Phase 2: Per-Location Affinity Computation

- **Goal**: For each related location, compute complementary content count and "only here" count.

This is the core of Story 3 and the most subtle piece.

#### Changes

**Add constant:**

```rust
const SUPERSET_THRESHOLD: f64 = 0.8;
```

**Update `LocationResult` to carry affinity data:**

```rust
struct LocationResult {
    path: String,
    shared_count: usize,
    complementary_count: Option<usize>,  // None when no --where
    only_here_count: Option<usize>,      // None when no --where
    kind: Option<LocationKind>,          // None when no classification
}
```

`Option` fields cleanly encode "not computed" (no filters) vs "computed as zero" (`Some(0)`). This distinction matters for output formatting and will matter again for `--brief` in Story 4.

**The `has_filters` flag:**

```rust
    let has_filters = !filters.is_empty();
```

Set once at the top of `compute_survey`, used to gate affinity computation.

**The affinity loop** — replaces the Story 2 per-location shared-count loop:

```rust
    for (scope_path, _overlap_count) in &location_scopes {
        let loc_scope = vec![ScopeMatch::UnderDirectory(scope_path.clone())];

        // --- Shared count (unchanged from Story 2) ---
        let loc_object_ids: HashSet<i64> = overlap_sources.iter()
            .filter(|s| s.matches_scope(&loc_scope))
            .filter_map(|s| s.object_id)
            .collect();

        let shared_count = hashed.iter()
            .filter(|s| loc_object_ids.contains(&s.object_id.unwrap()))
            .count();

        // --- Affinity (NEW — only when filters present) ---
        let (complementary_count, only_here_count, kind) = if has_filters {
            // Step 1: Get ALL sources within this location
            // Active, non-excluded, not in selection
            let loc_sources: Vec<&Source> = all_sources.iter()
                .filter(|s| s.is_active())
                .filter(|s| !s.is_excluded())
                .filter(|s| s.matches_scope(&loc_scope))
                .filter(|s| !sel_source_ids.contains(&s.id))
                .collect();

            // Step 2: Apply --where filters to location sources
            let loc_ids: Vec<i64> = loc_sources.iter().map(|s| s.id).collect();
            let passed: HashSet<i64> = filter::apply_filters(conn, &loc_ids, filters)?
                .into_iter().collect();

            // Step 3: Partition into overlap vs complementary
            // CRITICAL: filter to hashed-only BEFORE partitioning.
            // Unhashed sources can't participate in content comparison.
            // Without this guard, unhashed sources leak into complementary
            // count (their object_id is None, which is never in sel_object_ids,
            // so they'd always be classified as "complementary").
            let matching_hashed: Vec<&Source> = loc_sources.iter()
                .filter(|s| passed.contains(&s.id))
                .filter(|s| s.object_id.is_some())  // MUST filter to hashed
                .copied()
                .collect();

            let complementary: Vec<&Source> = matching_hashed.iter()
                .filter(|s| !sel_object_ids.contains(&s.object_id.unwrap()))
                .copied()
                .collect();

            let comp_count = complementary.len();

            // Step 4: "Only here" — unique object_ids among complementary
            let comp_oids: HashSet<i64> = complementary.iter()
                .filter_map(|s| s.object_id)
                .collect();
            let only_here = domain::survey::count_only_here(
                &comp_oids,
                scope_path,
                &by_object_id,
            );

            // Step 5: Classify
            let kind = domain::survey::classify_location(
                shared_count,
                total_hashed,
                comp_count,
                SUPERSET_THRESHOLD,
            );

            (Some(comp_count), Some(only_here), Some(kind))
        } else {
            (None, None, None)
        };

        location_results.push(LocationResult {
            path: scope_path.clone(),
            shared_count,
            complementary_count,
            only_here_count,
            kind,
        });
    }
```

**The unhashed-source trap** (Step 3): The most important implementation detail in this story. Without the `s.object_id.is_some()` guard after filter application, unhashed sources at the location that match `--where` leak into the complementary count. Their `object_id` is `None`, which is never in `sel_object_ids`, so the complement check always passes. The user would see inflated "+N more" counts including sources Canon can't compare by content. This must be a code comment — it's a correctness invariant future maintainers need to understand.

**Filter transfer**: The user's `--where` filters are applied as-is to each location's sources. Filters referencing path patterns may match differently on other roots. This is correct — the filters describe user intent, and mismatches produce lower "+N more" counts rather than incorrect results.

**DB access per location**: `apply_filters` is the one DB-touching operation per location (it may need to look up stored facts for non-built-in keys). Without a location cap (Story 6), this is bounded by the number of discovered scopes — typically small.

### Phase 3: Classification Sort and Output Formatting

- **Goal**: Sort locations by classification when filters are active. Update output to show filter echo and affinity columns.

#### Changes

**Classification sort:**

```rust
    // Sort locations
    if has_filters {
        // Classification: supersets first, then leads, then mirrors
        // Within each group: complementary desc, then shared desc
        location_results.sort_by(|a, b| {
            let kind_a = a.kind.as_ref().unwrap();
            let kind_b = b.kind.as_ref().unwrap();
            kind_a.cmp(kind_b)
                .then(b.complementary_count.cmp(&a.complementary_count))
                .then(b.shared_count.cmp(&a.shared_count))
        });
    } else {
        // No filters: sort by shared count descending (unchanged from Story 2)
        location_results.sort_by(|a, b| b.shared_count.cmp(&a.shared_count));
    }
```

`LocationKind`'s derived `Ord` gives `Superset < Lead < Mirror` — supersets sort first.

**Selection header — add filter echo:**

Update `print_selection_header` to accept `original_filters: &[String]`:

```rust
fn print_selection_header(
    scope_prefixes: &[String],
    original_filters: &[String],  // NEW
    total: usize,
    unhashed: usize,
    hashed: usize,
) {
    // ... existing path display ...

    // Filter echo (NEW)
    if !original_filters.is_empty() {
        println!("  Filters: {}", original_filters.join(" AND "));
    }

    // ... existing count line ...
}
```

All callers of `print_selection_header` updated to pass `original_filters` (or `&[]` for early-exit cases). The `run()` function has `options.original_filters` available; `compute_survey` does not need it.

**Related locations — affinity columns:**

Update `print_related_locations` to handle the new `Option` fields:

```rust
fn print_related_locations(locations: &[LocationResult], total_hashed: usize) {
    // ... empty check, header ...

    let max_path_len = locations.iter().map(|l| l.path.len()).max().unwrap_or(0);
    let max_shared_len = locations.iter()
        .map(|l| format_count(l.shared_count).len())
        .max()
        .unwrap_or(0);
    let m_str = format_count(total_hashed);

    for loc in locations {
        // Base: path + shared count (always present)
        print!(
            "  {:path_w$}  {:>count_w$} of {} shared",
            loc.path,
            format_count(loc.shared_count),
            m_str,
            path_w = max_path_len,
            count_w = max_shared_len,
        );

        // Affinity columns (only when present and complementary > 0)
        match (loc.complementary_count, loc.only_here_count) {
            (Some(comp), Some(only)) if comp > 0 => {
                print!("   +{} more", format_count(comp));
                if only > 0 {
                    print!(" ({} only here)", format_count(only));
                }
            }
            _ => {}  // Mirror or no filters — no affinity columns
        }

        println!();
    }
}
```

**Column alignment approach**: Paths left-aligned (padded to max), shared counts right-aligned (padded to max count width). Affinity text appended inline after "shared" with a triple-space separator. The "+N more" values are not right-aligned across lines — this matches the functional spec mockup and keeps the code simple. The path and shared columns provide the visual anchor.

**"(K only here)" suppressed when K = 0**: A location with complementary content but nothing unique still shows "+N more" without "(only here)". The absence is a signal — you can get this content from other places too.

#### Tests

This phase's formatting changes are verified by inspection and by the integration tests below that assert on `SurveyResult` values.

## Architectural Decisions

| Decision | Rationale |
|----------|-----------|
| Add `conn` to `compute_survey` | Per-location `apply_filters` requires DB access. Splitting the affinity loop out of `compute_survey` would scatter related computation logic across two functions for no testability gain — tests use `open_in_memory_for_test()` regardless. Accepting the purity loss is the pragmatic choice. |
| `db.conn()` → `db.conn_mut()` | Required by `apply_filters(conn: &mut Connection, ...)`. Reborrows as `&Connection` for read-only repo calls, so no cascading changes. |
| Filter to hashed-only before overlap/complementary partition | Unhashed sources at a location would leak into complementary count since their None object_id is never in sel_object_ids. This is silent data corruption of the "+N more" metric. |
| Pass both original and expanded filter strings | Display shows what the user typed (`@image`); evaluation uses the expanded form. Matches `cluster generate` pattern. `original_filters` stays in `run()` — `compute_survey` never sees them. |
| `apply_filters` runs per location | Each location's sources may have different stored facts. The filter engine handles fact lookups internally. This is the one DB-touching operation per location. |
| Classification sort uses derived Ord on LocationKind | Simple, deterministic. `Superset < Lead < Mirror` gives the desired display order. |
| LocationResult gains Option fields | Story 2's struct evolves naturally. `None` = "not computed" (no filters). `Some(0)` = "computed as zero" (mirror with filters). The distinction drives output formatting. |
| All-sources scan per location for affinity | The overlap sources only cover content shared with the selection. Complementary sources have *different* object_ids — they can't be found via the overlap set. The full `all_sources` scan is unavoidable. Cost is in-memory predicate checks (microseconds), dwarfed by the `apply_filters` DB call. |
| `SurveyOptions` struct for `run()` interface | Follows `CompareOptions`/`GenerateOptions`/`ApplyOptions` pattern. Survey's parameter list will grow through Stories 4-6 (`brief`, `verbose`, `detail`, `null_delim`, `archive`, `other_paths`). Introducing the struct now prevents the `ls.rs` problem (14 parameters). Primary data (`paths`, `filter_strs`) stays as direct parameters; behavioral config goes in the struct. |

## Non-Goals

- `--detail` views (complement, unique) — Story 5
- `--other` and `--brief` modes — Story 4
- Location cap and `--verbose` — Story 6
- `--archive` filter — Story 6

## Test Plan

### Existing Tests (Must Pass)

- All domain/survey.rs tests from Story 1 (24 tests)
- All survey.rs integration tests from Story 2 (10 tests)
- All existing tests (`cargo test`)

### Story 2 Test Updates

Story 2 tests call `compute_survey` which gains `conn` and `filters` parameters. Mechanical updates required:

- `run_compute` helper: add `conn: &mut Connection` and `filters: &[Filter]` parameters
- Change `let conn = open_in_memory_for_test()` → `let mut conn = open_in_memory_for_test()`
- All existing test calls pass `&[]` for filters

These are signature-only changes — no test logic changes.

### New Tests

Tests assert on `SurveyResult` fields from `compute_survey()`, not on formatted output strings.

**Test 1 — Basic affinity correctness:**

Setup:
```
Source root A (/mnt/drive-a):
  photos/IMG_001.jpg  → object 1   (also on root B)
  photos/IMG_002.jpg  → object 2   (also on root B)
  photos/IMG_003.jpg  → object 3   (unique to selection)

Source root B (/mnt/backup):
  trip/IMG_001.jpg    → object 1   (overlap)
  trip/IMG_002.jpg    → object 2   (overlap)
  trip/IMG_004.jpg    → object 4   (complementary — different content, .jpg)
  trip/IMG_005.jpg    → object 5   (complementary — different content, .jpg)
  trip/notes.txt      → object 6   (doesn't match filter — .txt)
```

Survey `/mnt/drive-a` with `--where "source.ext=jpg"`:
- Selection: 3 sources (objects 1, 2, 3)
- Location `/mnt/backup/trip`: shared = 2 of 3, complementary = 2 (objects 4, 5), only_here = 2

Assert: `complementary_count == Some(2)`, `only_here_count == Some(2)`, `kind == Some(LocationKind::Lead)`

**Test 2 — "Only here" with content elsewhere:**

Extend test 1 setup: add root C with one copy of object 4.
```
Source root C (/mnt/other):
  misc/copy.jpg  → object 4   (makes object 4 NOT "only here" at root B)
```

- only_here = 1 (object 5 only — object 4 now exists outside the location)

Assert: `only_here_count == Some(1)`

**Test 3 — Unhashed sources excluded from complementary count:**

Extend test 1 setup: add unhashed source at root B matching the filter:
```
Source root B (additional):
  trip/IMG_006.jpg  → None   (unhashed, .jpg extension)
```

- complementary count must remain 2, not 3
- The unhashed source passes the `--where` filter but must be excluded before the overlap/complementary partition

Assert: `complementary_count == Some(2)` — **critical correctness test**

**Test 4 — No filters means no affinity data:**

Same setup as test 1, but no `--where`:
- All LocationResult fields: `complementary_count == None`, `only_here_count == None`, `kind == None`
- Sort order: by shared count descending (Story 2 behavior preserved)

Assert: all affinity fields are `None`

**Test 5 — Classification sort order:**

Setup with three locations having distinct classifications:
```
Source root A (/mnt/drive-a):
  10 hashed sources (objects 1-10)

Source root B (/mnt/backup-main) — Superset:
  9 overlap sources (objects 1-9) + 5 complementary .jpg sources

Source root C (/mnt/partner) — Lead:
  2 overlap sources (objects 1-2) + 20 complementary .jpg sources

Source root D (/mnt/old-copy) — Mirror:
  3 overlap sources (objects 1-3), no complementary
```

Survey with `--where "source.ext=jpg"`:
- B: shared=9, comp=5, kind=Superset (9/10 = 90% >= 80% threshold)
- C: shared=2, comp=20, kind=Lead
- D: shared=3, comp=0, kind=Mirror

Assert: location_results order is [B, C, D] (Superset → Lead → Mirror)

**Test 6 — Selection narrowed by filter:**

Setup:
```
Source root A (/mnt/drive):
  photos/a.jpg  → object 1
  photos/b.jpg  → object 2
  photos/c.txt  → object 3   (doesn't match filter)
  photos/d.txt  → object 4   (doesn't match filter)
  photos/e.jpg  → object 5
```

Survey `/mnt/drive` with `--where "source.ext=jpg"`:
- Selection: 3 sources (a.jpg, b.jpg, e.jpg — objects 1, 2, 5)
- `total_count == 3`, `total_hashed == 3`

Assert: `total_count == 3` (not 5). Downstream computations (unique, shared) use the filtered 3, not the pre-filter 5.

**Test 7 — Same root, cross-scope complementary content:**

Setup:
```
Source root (/mnt/drive):
  photos/a.jpg     → object 1
  photos/b.jpg     → object 2
  documents/a.jpg  → object 1   (overlap — same content as photos/a.jpg)
  documents/c.jpg  → object 3   (complementary — matches filter, different content)
```

Survey `/mnt/drive/photos` with `--where "source.ext=jpg"`:
- Selection: 2 (objects 1, 2)
- Location `/mnt/drive/documents`: shared = 1 (object 1), complementary = 1 (object 3)

Assert: `location_results[0].complementary_count == Some(1)`. Verifies affinity computation works across scope boundaries on the same root.

**Test 8 — Mirror with filters has kind=Mirror and comp=Some(0):**

A location discovered via overlap but with zero complementary content when filters are active.

Assert: `kind == Some(LocationKind::Mirror)`, `complementary_count == Some(0)`, `only_here_count == Some(0)`. The `Some(0)` (not `None`) confirms affinity was computed and found nothing — distinct from the no-filter case.

(Can be verified within test 5's setup using root D.)

## Implementation Checklist

- [x] Phase 1: Add `--where` to CLI, alias expansion in dispatch, introduce `SurveyOptions` struct, `db.conn()` → `db.conn_mut()`, update `run()` and `compute_survey()` signatures, apply filters to selection
- [x] Phase 2: Per-location affinity loop — `LocationResult` gains Option fields, filter/partition/count_only_here/classify per location, `SUPERSET_THRESHOLD` constant
- [x] Phase 3: Classification sort, output formatting — filter echo in header, affinity columns in related locations
- [x] Update Story 2 tests for new `compute_survey` signature (mechanical: add `conn` + empty filters)
- [x] New tests 1-8
- [x] Verify all existing tests pass (`cargo test`)
- [x] Verify clippy passes (`cargo clippy`)

## Backward Compatibility

Story 2's behavior is preserved when `--where` is not provided. The `compute_survey` function gates all affinity work behind `has_filters`. All existing output is unchanged. The new columns only appear when filters are active.

## Performance Considerations

The per-location `apply_filters` call is the main cost. Each call may access the facts table for stored fact lookups. Cost per call depends on filter complexity:
- Built-in keys only (`source.ext`, `source.size`, etc.): fast, no fact table access
- Stored facts (`content.Make`, `content.DateTimeOriginal`, etc.): requires fact batch fetch per location

Without a location cap (added in Story 6), the number of `apply_filters` calls equals the number of discovered scopes. In practice, scope discovery concentrates overlap into a small number of locations (typically < 20 even for large libraries). Story 6 adds the cap as a safety bound.

The per-location `all_sources` scan for `loc_sources` is O(locations × sources) in-memory predicate checks. This is negligible compared to the `apply_filters` DB access — each predicate check is a string comparison, while `apply_filters` may issue SQL queries for stored facts.
