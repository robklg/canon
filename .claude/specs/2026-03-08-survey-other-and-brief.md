# Story: Survey `--other` and `--brief`

**Epic**: [epic-survey-command.md](epic-survey-command.md)
**Status**: Done
**Created**: 2026-03-08
**Depends on**: Story 3 (Affinity and Classification)

## Objective

Add two modifiers that alter how survey discovers and evaluates locations. `--other` lets the user direct comparison to specific paths instead of relying on scope discovery, enabling focused investigation of locations the default view may miss (sibling directories, archive roots, paths with no overlap). `--brief` provides a fast first pass by skipping the per-location affinity computation.

Together these support the typical workflow: run `survey --brief` for a quick landscape, read the directory paths, then re-run with `--other` targeting specific locations for deeper investigation.

## Functional Requirements Summary

**`--other <path>` (repeatable):**
- Bypasses scope discovery — each `--other` path is a location directly
- Locations displayed in user-specified order (no classification sort)
- Header: "Comparing with:" instead of "Related locations:"
- Works with any root role (archive roots included — user chose the path)
- Zero-overlap locations shown with "0 of M shared"
- Archive status, unique count, and "only here" counts still computed against full universe
- Affinity computation (when `--where` present and not `--brief`) runs per `--other` path

**`--brief`:**
- Skips per-location affinity computation entirely
- No "+N more" or "(only here)" columns
- Sort by shared count descending (classification requires affinity data)
- `--brief` without `--where` is a no-op (affinity already absent without filters)
- `--brief` is orthogonal to `--other` — works with both default and directed modes

**Behavioral differences:**

| Aspect | Default mode | `--other` mode |
|--------|-------------|----------------|
| Location discovery | Scope discovery via overlap | User-specified paths directly |
| Summary header | "Related locations:" | "Comparing with:" |
| Empty state | "No related locations found." | "No shared content at specified locations." |
| Location ordering | Classification (if filters) or shared desc | User-specified order |
| Zero-overlap locations | Not possible (overlap drives discovery) | Shown with "0 of M shared" |
| Shared count scope | Source role only (via overlap_sources) | Any role (user chose the path) |

## Current State

**After Story 3:**
- `src/survey.rs` — `compute_survey()` with `conn`, `paths`, `include`, `filters`, `all_sources`, `all_roots` params. `SurveyOptions` with `original_filters` and `include`. `SurveyResult` and `SurveyOutcome` types. `LocationResult` with `Option` affinity fields. 18 integration tests.
- `src/main.rs` — `Survey` variant with `paths`, `filters`, `include`.
- `domain/survey.rs` — all pure domain functions (unchanged by this story).

**Key existing patterns:**
- Affinity gated by `has_filters` — the `if has_filters { ... } else { (None, None, None) }` branch
- Sort: classification sort when `has_filters`, shared desc otherwise
- `overlap_sources` filtered to `is_from_role("source")` — archive roots excluded
- `loc_sources` scan in affinity loop doesn't filter by role — already any-role

## Design

### Phase 1: CLI, Plumbing, and Test Helper Update

- **Goal**: `--other` and `--brief` accepted, resolved, and passed through to `compute_survey`.

#### Changes

**`src/main.rs`** — expand Survey variant:

```rust
Survey {
    /// Directory paths to scope the query (resolved to realpath)
    paths: Vec<PathBuf>,
    /// Filter expressions (e.g., "source.ext=jpg" or "content.hash.sha256?")
    #[arg(long = "where")]
    filters: Vec<String>,
    /// Include additional sources: excluded
    #[arg(long, value_delimiter = ',')]
    include: Vec<IncludeValue>,
    /// Compare against specific locations instead of discovering them
    #[arg(long = "other")]
    other_paths: Vec<PathBuf>,
    /// Skip per-location affinity computation
    #[arg(long)]
    brief: bool,
},
```

**Dispatch** — resolve `--other` paths and pass through:

```rust
Commands::Survey {
    paths,
    filters,
    include,
    other_paths,
    brief,
} => {
    let expanded = alias::expand_filter_strings(&filters, &canon_home)?;
    let include = include_set_from(&include);
    if include.includes_archived() {
        bail!("--include archived is not valid for survey");
    }
    let options = survey::SurveyOptions {
        original_filters: filters,
        include,
        other_paths,
        brief,
    };
    survey::run(&mut db, &paths, &expanded, &options)?;
}
```

**`src/survey.rs`** — expand `SurveyOptions`:

```rust
pub struct SurveyOptions {
    pub original_filters: Vec<String>,
    pub include: IncludeSet,
    pub other_paths: Vec<PathBuf>,  // NEW
    pub brief: bool,                // NEW
}
```

**`run()`** — resolve `--other` paths and update `compute_survey` call:

```rust
pub fn run(...) -> Result<()> {
    // ... existing parsing ...
    let conn = db.conn_mut();
    let all_roots = repo::root::fetch_all(conn)?;

    // Resolve --other paths (same soft resolution as scope paths)
    let other_resolved = if !options.other_paths.is_empty() {
        domain::path::resolve_paths(&options.other_paths, &all_roots)?
    } else {
        Vec::new()
    };

    let root_ids: Vec<i64> = all_roots.iter().map(|r| r.id).collect();
    let all_sources = repo::source::batch_fetch_by_roots(conn, &root_ids)?;

    match compute_survey(
        conn, paths, &options.include, &filters,
        &all_sources, &all_roots, &other_resolved, options.brief,
    )? {
        // ... formatting with is_other_mode from result ...
    }
}
```

**`compute_survey` signature update:**

```rust
fn compute_survey(
    conn: &mut Connection,
    paths: &[PathBuf],
    include: &IncludeSet,
    filters: &[Filter],
    all_sources: &[Source],
    all_roots: &[domain::Root],
    other_paths: &[String],  // NEW — resolved, empty = default mode
    brief: bool,             // NEW
) -> Result<SurveyOutcome>
```

**`run_compute` test helper update:**

```rust
fn run_compute(
    conn: &mut Connection,
    scope_paths: &[&str],
    include: &IncludeSet,
    filters: &[Filter],
    other_paths: &[&str],  // NEW
    brief: bool,           // NEW
) -> SurveyOutcome {
    let all_roots = repo::root::fetch_all(conn).unwrap();
    let root_ids: Vec<i64> = all_roots.iter().map(|r| r.id).collect();
    let all_sources = repo::source::batch_fetch_by_roots(conn, &root_ids).unwrap();

    let paths: Vec<PathBuf> = scope_paths.iter().map(|p| PathBuf::from(p)).collect();
    let other: Vec<String> = other_paths.iter().map(|p| p.to_string()).collect();
    compute_survey(
        conn, &paths, include, filters, &all_sources, &all_roots, &other, brief,
    ).unwrap()
}
```

All existing test calls updated to pass `&[], false` as the two new trailing arguments.

#### Tests

None in this phase — wiring only. Existing tests updated for new signature (mechanical).

### Phase 2: `--other` and `--brief` Computation Logic

- **Goal**: Location pipeline branches correctly for both modes.

#### Changes

**Flags at top of `compute_survey`:**

```rust
let is_other_mode = !other_paths.is_empty();
let compute_affinity = !filters.is_empty() && !brief;
```

**Location building branch** — replaces the current `discover_scopes_by_root` call:

```rust
// Location discovery
let location_scopes: Vec<(String, usize)> = if is_other_mode {
    // --other: user paths directly
    other_paths.iter().map(|p| (p.clone(), 0)).collect()
} else {
    // Default: scope discovery from overlap sources
    domain::survey::discover_scopes_by_root(&overlap_sources)
};
```

**Shared count branch** — inside the per-location loop:

```rust
let loc_scope = vec![ScopeMatch::UnderDirectory(scope_path.clone())];

let shared_count = if is_other_mode {
    // --other: scan all sources at location (any role) for overlap with selection
    let loc_oids: HashSet<i64> = all_sources.iter()
        .filter(|s| s.is_active())
        .filter(|s| !s.is_excluded())
        .filter(|s| s.matches_scope(&loc_scope))
        .filter(|s| !sel_source_ids.contains(&s.id))
        .filter_map(|s| s.object_id)
        .collect();
    hashed.iter()
        .filter(|s| loc_oids.contains(&s.object_id.unwrap()))
        .count()
} else {
    // Default: use overlap_sources (source role only, already computed)
    let loc_object_ids: HashSet<i64> = overlap_sources.iter()
        .filter(|s| s.matches_scope(&loc_scope))
        .filter_map(|s| s.object_id)
        .collect();
    hashed.iter()
        .filter(|s| loc_object_ids.contains(&s.object_id.unwrap()))
        .count()
};
```

The key difference: default mode uses `overlap_sources` (pre-filtered to source role), while `--other` mode scans `all_sources` at the location regardless of role. This matters when `--other` points to an archive root.

**Affinity gate** — replaces `if has_filters`:

```rust
let (complementary_count, only_here_count, kind) = if compute_affinity {
    // ... existing affinity computation (unchanged) ...
} else {
    (None, None, None)
};
```

**Sort branch** — three cases:

```rust
if is_other_mode {
    // --other: preserve user-specified order (no sort)
} else if compute_affinity {
    // Classification: supersets → leads → mirrors, then comp desc, shared desc
    location_results.sort_by(|a, b| {
        let kind_a = a.kind.as_ref().unwrap();
        let kind_b = b.kind.as_ref().unwrap();
        kind_a.cmp(kind_b)
            .then(b.complementary_count.cmp(&a.complementary_count))
            .then(b.shared_count.cmp(&a.shared_count))
    });
} else {
    // No affinity data: sort by shared count descending
    location_results.sort_by(|a, b| b.shared_count.cmp(&a.shared_count));
}
```

**`SurveyResult` gains `is_other_mode`:**

```rust
struct SurveyResult {
    // ... existing fields ...
    is_other_mode: bool,  // NEW
}
```

Set from the `is_other_mode` flag at the end of `compute_survey`.

#### Tests

**Test 1 — `--other` basic with overlap and complementary:**

```
Root A (selection): photos/IMG_001.jpg→obj1, photos/IMG_002.jpg→obj2, photos/IMG_003.jpg→obj3
Root B: trip/IMG_001.jpg→obj1, trip/IMG_002.jpg→obj2, trip/IMG_004.jpg→obj4, trip/IMG_005.jpg→obj5, trip/notes.txt→obj6
```

Survey `/mnt/drive-a` with `--where "source.ext=jpg"` and `--other /mnt/backup/trip`:
- Assert: `is_other_mode == true`, shared=2, complementary=Some(2), only_here=Some(2), kind=Some(Lead)

**Test 2 — `--other` with zero overlap:**

```
Root A: photos/a.jpg→obj1, photos/b.jpg→obj2
Root B: docs/c.jpg→obj3, docs/d.jpg→obj4
```

Survey `/mnt/drive-a` with `--where "source.ext=jpg"` and `--other /mnt/backup`:
- Assert: `shared_count == 0`, `complementary_count == Some(2)`, location shown

**Test 3 — `--other` preserves user order:**

```
Root A: photos/a.jpg→obj1
Root B (high shared): trip/a.jpg→obj1, trip/b.jpg→obj2, trip/c.jpg→obj3
Root C (low shared): backup/a.jpg→obj1
```

Survey with `--other /mnt/root-c --other /mnt/root-b`:
- Assert: `location_results[0]` contains "root-c", `location_results[1]` contains "root-b"
- User order preserved, NOT sorted by shared count

**Test 4 — `--other` on archive root:**

```
Root A (source): photos/a.jpg→obj1, photos/b.jpg→obj2
Root B (archive): 2024/a.jpg→obj1, 2024/c.jpg→obj3
```

Survey `/mnt/drive-a` with `--where "source.ext=jpg"` and `--other /archive`:
- Assert: `shared_count == 1` (obj1 — archive sources counted because `--other`)
- Assert: `complementary_count == Some(1)` (obj3)

**Test 5 — `--brief` suppresses affinity:**

Same setup as test_affinity_basic. Survey with `--where "source.ext=jpg"` and `--brief`:
- Assert: `complementary_count == None`, `only_here_count == None`, `kind == None`
- Assert: `shared_count` still computed correctly

**Test 6 — `--brief` without `--where` is no-op:**

Same setup, survey without `--where` and with `--brief`:
- Assert: Results match the no-filter case exactly (affinity was already absent)

**Test 7 — `--other` with `--brief`:**

Survey with `--other /mnt/backup/trip` and `--brief` and `--where "source.ext=jpg"`:
- Assert: `is_other_mode == true`, all affinity fields `None`, user order preserved

**Test 8 — `--other` same root cross-scope:**

```
Root A: photos/a.jpg→obj1, photos/b.jpg→obj2, documents/a.jpg→obj1, documents/c.jpg→obj3
```

Survey `/mnt/drive/photos` with `--where "source.ext=jpg"` and `--other /mnt/drive/documents`:
- Assert: shared=1 (obj1), complementary=Some(1) (obj3)

### Phase 3: Output Formatting

- **Goal**: Headers and empty-state messages reflect the mode.

#### Changes

**`print_related_locations` gains `is_other_mode`:**

```rust
fn print_related_locations(locations: &[LocationResult], total_hashed: usize, is_other_mode: bool) {
    if locations.is_empty() {
        if is_other_mode {
            println!("No shared content at specified locations.");
        } else {
            println!("No related locations found.");
        }
        return;
    }

    if is_other_mode {
        println!("Comparing with:");
    } else {
        println!("Related locations:");
    }

    // ... rest unchanged ...
}
```

**`run()` formatting** — pass `is_other_mode` through:

```rust
SurveyOutcome::Result(result) => {
    // ... header, archive section unchanged ...
    println!();
    print_related_locations(&result.location_results, result.total_hashed, result.is_other_mode);
    println!();
    println!("{} unique to this scope", format_count(result.unique_count));
}
```

#### Tests

Formatting verified by inspection and by the integration tests asserting on `SurveyResult.is_other_mode`.

## Architectural Decisions

| Decision | Rationale |
|----------|-----------|
| `--other` paths resolved in `run()`, passed as `&[String]` to `compute_survey` | Path resolution is orchestration concern. Computation receives clean canonical strings. Matches existing pattern. |
| Shared count branches for `--other` vs default | Default mode uses overlap_sources (source role only). `--other` scans all sources at location regardless of role — user chose the path, respect it. |
| `compute_affinity = !filters.is_empty() && !brief` | `--brief` simply widens the existing "no affinity" gate. No new code path needed, reuses the `(None, None, None)` branch. |
| `is_other_mode` flag in `SurveyResult` | Output formatting needs mode awareness. Cleaner than threading the flag separately. |
| `--other` preserves user order (no sort) | User chose these paths deliberately; reordering would be confusing. Matches the functional spec. |
| No `conflicts_with = "detail"` on `--brief` yet | `--detail` flag doesn't exist until Story 5. clap requires conflict targets to exist. Story 5 adds the conflict. |
| `--other` empty-state text differs from default | "No shared content at specified locations." vs "No related locations found." — the user directed Canon to specific places, the message should reflect that. |

## Non-Goals

- `--detail complement` and `--detail unique` — Story 5
- `--archive` filter — Story 6
- Location cap and `--verbose` — Story 6
- `--brief` / `--detail` mutual exclusion — Story 5 (when `--detail` is defined)

## Test Plan

### Existing Tests (Must Pass)

- 24 domain/survey.rs unit tests (Story 1)
- 18 survey.rs integration tests (Stories 2+3)
- All other tests (`cargo test`)

### Story 2/3 Test Updates

`run_compute` helper gains `other_paths: &[&str]` and `brief: bool` parameters. All 18 existing calls pass `&[], false`. Mechanical signature-only changes — no test logic changes.

### New Tests

8 integration tests (described in Phase 2 above):
1. `--other` basic with overlap and complementary
2. `--other` with zero overlap
3. `--other` preserves user order
4. `--other` on archive root
5. `--brief` suppresses affinity
6. `--brief` without `--where` is no-op
7. `--other` with `--brief` combined
8. `--other` same root cross-scope

## Implementation Checklist

- [x] Phase 1: Add `--other` and `--brief` to CLI, resolve `--other` paths, expand `SurveyOptions`, update `compute_survey` signature, update `run_compute` helper and all existing tests
- [x] Phase 2: Location building branch, shared count branch, affinity gate, sort branch, `is_other_mode` in `SurveyResult`, 8 new tests
- [x] Phase 3: `print_related_locations` gains `is_other_mode`, header/empty-state text, `run()` formatting update
- [x] Verify all existing tests pass (`cargo test`)
- [x] Verify clippy passes (`cargo clippy`)

## Backward Compatibility

Story 3's behavior is preserved when neither `--other` nor `--brief` is provided. The `is_other_mode` and `compute_affinity` flags gate all new behavior. All existing output is unchanged. `--brief` without `--where` produces identical output to the no-`--brief` case.

## Performance Considerations

`--brief` is explicitly a performance feature — it eliminates all `apply_filters` calls per location, making survey output near-instant regardless of location count or filter complexity. This is especially valuable for a quick first pass before using `--other` for focused investigation.

`--other` with `--brief` is the fastest combination: no scope discovery, no affinity computation. Only the shared count scan (in-memory predicate checks) runs per location.

The `--other` shared count scan (any-role) iterates `all_sources` per location instead of the smaller `overlap_sources`. This is negligible — in-memory predicate checks, microseconds per source. The dominant cost remains `apply_filters` when affinity is computed.
