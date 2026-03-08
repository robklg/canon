# Epic: Survey Command — Outward-Looking Comparison

**Status**: Done
**Created**: 2026-03-07

## Objective

Give Canon the ability to answer contextual, outward-looking questions from a shaped selection — the first layer of assembly support. The user has explored a scope with `ls`, crafted `--where` filters that capture what's interesting, and now asks: what's already resolved? Is my selection complete? Am I in the right place?

Today the user resorts to manual workarounds — stripping scope, running broad queries, mentally diffing results — each step eroding the cognitive state needed for assembly decisions. `survey` eliminates this by accepting the same query model as `ls` and answering outward questions from data Canon already has. No new persistence, no new domain concepts — just new ways of querying existing content hashes and metadata.

### Success Criteria

1. `canon survey` accepts scope paths + `--where` filters (same input model as `ls`) and produces a summary showing archive status, related locations with overlap/affinity data, and unique content count
2. Related locations are discovered via scope discovery — actionable directory paths, not broad roots
3. `--detail complement` shows what complementary content exists at related locations (requires `--where`)
4. `--detail unique` outputs bare paths of unique-to-selection content (pipeable)
5. `--other` allows directed comparison against user-specified paths, bypassing scope discovery
6. `--brief` provides a fast first pass by skipping per-location affinity computation
7. All pure domain logic (scope discovery, only-here, uniqueness) is thoroughly unit-tested

## Architectural Design

### Overview

Survey is a read-only command that operates from an in-memory snapshot of all sources. It follows the standard fetch → domain-filter → compute → format pattern, with one key architectural addition: an **in-memory object index** (`HashMap<i64, Vec<&Source>>` keyed by `object_id`) that serves as the single data structure powering overlap detection, archive status, "only here" checks, and uniqueness computation.

```
┌─ Command Layer (survey.rs) ──────────────────────────────────────┐
│ CLI parsing, orchestration, output formatting                    │
│ Builds selection, object index, computes per-location results    │
│ Calls apply_filters() for --where evaluation (only DB-touching   │
│ operation per location)                                          │
└──────────────────────────────────────────────────────────────────┘
          │                                    │
          ▼                                    ▼
┌─ Domain (domain/survey.rs) ──┐   ┌─ Repo (existing) ───────────┐
│ discover_scopes()            │   │ batch_fetch_by_roots()       │
│ count_only_here()            │   │ filter::apply_filters()      │
│ count_unique_to_selection()  │   │ root::fetch_all()            │
│ classify_location()          │   │                              │
│ All pure, no I/O             │   │ No new repo functions needed │
└──────────────────────────────┘   └──────────────────────────────┘
```

### Asymmetric Visibility Model

Survey has two sides with different visibility rules:

| Aspect | Selection side | Outward side |
|--------|---------------|--------------|
| Root roles | Source roots only | Source + archive roots |
| Suspension | Active only | Active only |
| Exclusion | Non-excluded (unless `--include excluded`) | Non-excluded always |
| Purpose | Working set the user shaped | Universe for comparison |

Archive roots are visible on the outward side because "what's already resolved?" is a core question — the user doesn't need `--include archived` to see archive coverage. Related locations (the "is my selection complete?" answer) show source roots only; archive roots appear in the dedicated archive section.

### New Domain Types

**`domain/survey.rs`**:

```rust
/// Result of scope discovery for a single root.
pub struct DiscoveredScope {
    pub prefix: String,   // relative to root (empty = root itself)
    pub count: usize,     // overlap count at this scope
}

/// Classification of a related location.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub enum LocationKind {
    Superset,   // shared >= 80% of selection AND has complementary
    Lead,       // has complementary content
    Mirror,     // overlap only, no complementary
}
```

### Key Domain Functions

```rust
/// Core algorithm: find directory prefixes that concentrate overlap.
/// Pure function operating on relative paths within a single root.
pub fn discover_scopes(rel_paths: &[&str]) -> Vec<DiscoveredScope>

/// Wrapper: group sources by root, run discover_scopes per root,
/// return absolute scope paths.
pub fn discover_scopes_by_root(sources: &[&Source]) -> Vec<(String, usize)>

/// Count object_ids that exist only within location_path.
/// Checks full universe via object index. Returns count of unique
/// object_ids (NOT sources — per design decision).
pub fn count_only_here(
    object_ids: &HashSet<i64>,
    location_path: &str,
    by_object_id: &HashMap<i64, Vec<&Source>>,
) -> usize

/// Count selection object_ids that exist nowhere outside the selection.
pub fn count_unique_to_selection(
    selection_object_ids: &HashSet<i64>,
    selection_source_ids: &HashSet<i64>,
    by_object_id: &HashMap<i64, Vec<&Source>>,
) -> usize

/// Classify a location based on shared/complementary data.
pub fn classify_location(
    shared_count: usize,
    total_hashed: usize,
    complementary_count: usize,
    superset_threshold: f64,
) -> LocationKind
```

### Scope Discovery Algorithm

The algorithm finds actionable directory paths where overlapping content concentrates, rather than reporting at the root level.

**Input**: relative paths of overlapping sources on a single root.

**Algorithm**:
1. Extract the directory portion of each rel_path
2. Build a tree of directory nodes, each tracking `direct` (files directly in this directory) and `children` (subdirectories)
3. Walk top-down applying the collapsing rule:
   - If `direct == 0` AND exactly one child has overlap → recurse into that child (go deeper)
   - Otherwise → this node is a scope (emit it), or if `direct == 0` with multiple children, recurse into each child separately

**The collapsing rule formalized**: a node can be skipped (drill deeper) only when `count(single_child) == count(node)`. This automatically handles all cases:
- Direct files prevent drill-down (parent count > any child count)
- Multiple children force a split (no single child captures everything)
- Single-child chains collapse to the deepest meaningful level
- Leaves are always scopes

**Output**: `(prefix, count)` pairs where prefix is relative to the root.

### "Only Here" — Design Decision

"Only here" counts **unique object_ids**, not sources. This is the one metric where the object-level view is meaningful — duplicates within a location don't make content more irreplaceable.

Critical correctness property: internal duplicates at a location must NOT disqualify content from being "only here." If object 42 has 3 copies at `/mnt/backup/photos/` and nowhere else, it IS "only here" (count = 1). The check is: for each object_id, does any source with that object_id exist at a path NOT under the location? Sources within the location are invisible to this check by construction.

### Data Flow

```
1. Resolve scope paths (resolve_paths — offline-capable)
2. Resolve --other paths if any
3. Resolve --archive spec if any
4. Fetch all roots
5. Fetch ALL sources for all root IDs (batch_fetch_by_roots)
6. Build object index: HashMap<object_id, Vec<&Source>>
   (from active, non-excluded, hashed sources)
7. Build selection: active, source role, in scope, visibility rules
8. Apply --where filters to selection (if any)
9. Partition selection: unhashed vs hashed
   Early exit if empty or all unhashed
10. Archive status: for each selection object_id, check archive
    copies via object index. Scope discovery on archive sources.
11. Overlap: for each selection object_id, find sources NOT in
    selection on source roots via object index
12. Location discovery:
    - Default: scope discovery per root on overlap sources
    - --other: user-specified paths directly
13. Per-location shared count: selection sources whose object_id
    appears at location
14. Per-location affinity (only with --where, skip if --brief):
    a. Get all sources within location (active, non-excluded)
    b. apply_filters(conn, source_ids, filters)
    c. Partition: overlap vs complementary (hashed only)
    d. complementary_count = source count
    e. only_here_count = unique object_ids only at location
15. Unique count: selection object_ids with no source outside
    selection in full universe
16. Classification and sorting (if affinity data available)
17. Output formatting per mode
```

### Command Layer Impact

**New files:**
- `src/survey.rs` — command orchestration and output formatting
- `src/domain/survey.rs` — pure domain functions

**Modified files:**
- `src/main.rs` — `Survey` variant in `Commands`, `mod survey`, dispatch
- `src/domain/mod.rs` — `pub mod survey`

**CLI flags:**
```
canon survey [paths]... [OPTIONS]

--where <EXPR>         Filter expression (repeatable)
--detail <MODE>        complement | unique (replaces summary)
--archive <SPEC>       Filter archive section to specific archive
--include <VALUE>      excluded (expand selection visibility)
--other <PATH>         Compare against specific locations (repeatable)
--brief                Skip affinity computation
--verbose              Show all locations / all paths per location
-0                     Null-delimited output (--detail unique)
```

## Stories

| # | Story | Scope | Dependencies | Status |
|---|-------|-------|--------------|--------|
| 1 | Domain foundations | Scope discovery algorithm, only-here, unique, classification — all pure functions with full test suite | — | Done |
| 2 | Summary view (no filters) | Command module, CLI, selection pipeline, archive status, overlap with scope discovery, unique count, formatted output | Story 1 | Done |
| 3 | Affinity and classification | `--where` enables "+N more" / "(only here)" columns, classification sort, filter echo | Story 2 | Done |
| 4 | `--other` and `--brief` | Directed comparison mode, brief mode, behavioral differences | Story 3 | Done |
| 5 | Detail views | `--detail complement` and `--detail unique` output modes | Story 3 | Done |
| 6 | Polish and documentation | `--archive`, location cap, `--verbose`, formatting, CLAUDE.md, docs/ | Story 5 | Done |

### Story 1: Domain Foundations

**Objective**: Build and thoroughly test all pure domain logic before any command wiring.

**Scope**:
- Create `src/domain/survey.rs` with `DirNode` tree structure
- Implement `discover_scopes()` — tree building and top-down walk with collapsing rule
- Implement `discover_scopes_by_root()` — groups sources by root, calls `discover_scopes`, produces absolute paths
- Implement `count_only_here()` — checks object_ids against full universe via object index
- Implement `count_unique_to_selection()` — checks selection object_ids for uniqueness
- Implement `LocationKind` enum and `classify_location()`
- Wire into `domain/mod.rs`

**Key decisions**: All functions take pre-computed data structures (no DB access). `discover_scopes` operates on `&[&str]` (plain relative paths) for easy testing. The grouping wrapper handles Source → rel_path extraction.

**Tests (23 tests)**:

Scope discovery (10 tests):
1. Single directory — drills down through single-child chain
2. Multiple siblings — splits at branching point
3. Files at root — empty prefix scope
4. Mixed depth — direct files prevent drill-down
5. Deep single path — collapses long chain
6. Separate branches at root — root splits
7. Single file — degenerate minimal input
8. Deep directory with sibling at higher level — mixed depths
9. Uniform distribution — no false collapsing
10. Empty input — no panic

Only here (5 tests):
11. Content only at location (internal duplicates don't disqualify) — **critical correctness test**
12. Content exists elsewhere — not "only here"
13. Internal duplicates don't inflate count (3 sources, same object_id → count = 1) — **critical correctness test**
14. Mixed — some only here, some not
15. Path boundary — `/photos` doesn't match `/photography`

Unique to selection (4 tests):
16. Truly unique — no copies outside selection
17. Not unique — copy outside selection
18. Not unique — archived copy counts as elsewhere
19. Duplicate within selection — counts as 1 unique object

Classification (4 tests):
20. Superset — high shared + complementary
21. Lead — low shared + complementary
22. Mirror — overlap only
23. Edge of superset threshold

### Story 2: Summary View (No Filters)

**Objective**: `canon survey <path>` works end-to-end for the no-filter case.

**Scope**:
- Create `src/survey.rs` with `SurveyOptions` struct and `run()` function
- Add `Survey` variant to `Commands` in `main.rs` (initially: `paths` and `--include` only)
- Full orchestration pipeline: resolve paths → fetch sources → build selection → build object index → archive status with scope grouping → overlap with scope discovery → unique count
- Output formatting: selection echo, archive section, related locations (shared count only — no affinity columns), unique count
- Default-to-cwd behavior (following `ls` pattern in `main.rs`)
- Early exits: empty selection, all unhashed
- Thousands separators via `format_count()`

**What's NOT included**: `--where`, `--detail`, `--other`, `--brief`, `--archive`, affinity computation, classification sort. Related locations sort by shared count descending.

**Tests**:
- Basic summary end-to-end (integration test with test DB)
- Empty selection produces "0 sources selected"
- All unhashed produces hashing guidance message

### Story 3: Affinity and Classification

**Objective**: `--where` filters enable the full summary output with complementary content counts and classification-based sorting.

**Scope**:
- Add `--where` flag with alias expansion (following `cluster generate` pattern — pass both original and expanded filters)
- Per-location affinity computation:
  - Get all sources within location scope (active, non-excluded, any role within the location)
  - Call `apply_filters()` on location source IDs
  - Partition matching sources: overlap (object_id in selection) vs complementary (hashed, object_id not in selection)
  - Compute complementary count and only-here count
- Filter echo in selection header: `Filters: <original filter strings>`
- Classification-based sort: supersets → leads → mirrors, then by complementary desc, shared desc
- Without `--where`: affinity columns suppressed, sort by shared count (unchanged from Story 2)

**Key implementation detail**: Unhashed sources at locations must be excluded from the complementary partition — they can't participate in content comparison.

**Tests**:
- Summary with `--where` produces correct affinity columns
- Complementary counts are correct (integration test)
- Only-here counts match expected values

### Story 4: `--other` and `--brief`

**Objective**: Two modifiers that alter location discovery and affinity computation.

**Scope**:
- Add `--other <path>` flag (repeatable) to CLI
- When `--other` active:
  - Bypass scope discovery — user paths become locations directly
  - Display in user-specified order (no classification sort)
  - Header changes: "Comparing with:" instead of "Related locations:"
  - Works with any root role (user chose the path — respect it)
- Add `--brief` flag (`conflicts_with = "detail"` in clap)
- When `--brief` active:
  - Skip affinity computation entirely
  - Sort by shared count descending
  - No "+N more" or "(only here)" columns
- `--brief` without `--where` is silently a no-op (affinity already absent)

**Tests**:
- `--brief` suppresses affinity columns
- `--other` bypasses scope discovery, shows user-specified paths
- `--other` with zero overlap shows `0 of M shared`
- `--other` with mirror in complement view (Story 5 dependency for full test, but basic behavior testable here)

### Story 5: Detail Views

**Objective**: `--detail complement` and `--detail unique` provide focused output replacing the summary.

**Scope**:
- Add `--detail` flag (value: `complement` or `unique`)
- `--detail complement`:
  - Light header (selection echo, counts)
  - Header text: "Complementary content at related locations:" (default) / "...specified locations:" (`--other`)
  - Per location: path, complementary count, only-here count, then source paths relative to location
  - Per-location path cap (5), `--verbose` shows all, "... and N more" note
  - Default mode: mirrors omitted. `--other` mode: mirrors shown with "no complementary content (N of M shared)" note
  - Sort: default mode uses classification priority; `--other` uses user order
- `--detail unique`:
  - Bare paths, one per line, suitable for piping
  - `-0` for null-delimited output (add flag to CLI)
  - Empty output (no unique sources) produces no output, exit 0
- Validation: `--detail complement` requires `--where` (error without)
- Both modes replace the summary (not additive)

**Tests**:
- `--detail complement` without `--where` produces error
- `--detail unique` outputs bare paths
- `-0` produces null-delimited output
- Complement view shows paths relative to location

### Story 6: Polish and Documentation

**Objective**: Remaining features, edge case handling, documentation.

**Scope**:
- `--archive <spec>` flag:
  - Parse root spec, validate archive role
  - Filter archive section to specific archive root
  - Affects archive scope discovery (only sources from specified archive)
- Location cap (default 10):
  - Applied after sorting
  - "... and N more locations (use --verbose to show all)" note
  - `--verbose` shows all locations
- Output formatting polish:
  - Column alignment for location paths and counts
  - Percentage to one decimal place
  - Consistent spacing
- CLAUDE.md updates:
  - Add `survey.rs` to command modules list
  - Document scope discovery as domain concept
  - Document asymmetric visibility model
  - Note "only here" counts unique objects (exception to source-based counting)
  - Document survey's relationship to `coverage` and `compare`
- `docs/` updates:
  - New page for survey command (usage, options, examples, reading the output)
  - Update `SUMMARY.md`

**Tests**:
- `--archive` filters to specific archive root
- Multiple archive roots with scope grouping
- `--include excluded` affects selection only (outward side unchanged)
- Location cap truncation with `--verbose` override

## Architectural Decisions

| Decision | Rationale |
|----------|-----------|
| In-memory object index | Consistent with codebase pattern (batch_fetch_by_roots). Single data structure powers overlap, archive, only-here, and uniqueness — the "one well-tested query path" principle. Avoids batch-size concerns for the core join. |
| No new repo functions | Everything needed exists. The object index replaces what would otherwise be a `fetch_sources_by_object_ids` call with an in-memory lookup. |
| Scope discovery as pure domain function | The most complex new logic — must be independently testable. Takes plain strings, returns plain strings. No I/O. |
| Related locations exclude archive roots | Archives are reported in the dedicated "Archived" section. Related locations are actionable leads for assembly — archive content is already resolved. |
| "Only here" counts unique objects | Object-level count is the meaningful signal for irreplaceable content. Internal duplicates at a location don't make content more irreplaceable. Exception to the general source-based counting convention. |
| Unique count checks full universe including archives | Content in the archive IS "elsewhere" — unique means truly nowhere else. |
| `--include excluded` affects selection only | Outward side never shows excluded sources — "excluded means dismissed." |
| `--where` filters transfer as-is | Filters describe user intent. Mismatches on other roots produce lower counts, not incorrect results. No selective dropping of filter components. |
| `--other` respects user choice regardless of root role | In default mode, archive roots go to archive section. With `--other`, the user explicitly chose the path — respect it. |
| Per-location affinity bounded by location cap | Maximum 10 `apply_filters` calls prevents runaway computation. `--brief` provides escape hatch. |
| Domain-first implementation (Story 1) | De-risks the most complex logic before command wiring adds integration complexity. |

## Cross-Cutting Concerns

**Existing patterns to follow:**
- Source fetching + domain predicate filtering: `ls.rs`, `coverage.rs`
- Path resolution: `resolve_paths()` for source-querying commands
- Default-to-cwd: `ls` dispatch pattern in `main.rs`
- Filter alias expansion: `cluster generate` pattern (pass both original + expanded)
- Root spec parsing: `parse_root_spec()` for `--archive`
- Thousands separators: `format_count()` from `ceremony.rs`

**Constants (tunable, not architectural):**
- `SUPERSET_THRESHOLD: f64 = 0.8` — shared >= 80% qualifies as superset
- `DEFAULT_LOCATION_CAP: usize = 10` — max locations before truncation
- `COMPLEMENT_SAMPLE_SIZE: usize = 5` — max paths per location in complement view

**Concurrency**: Survey is fully read-only. No transactions needed. Multiple survey processes can run concurrently with no contention.

## Test Strategy

Domain layer tests (Story 1) are the highest priority — 23 pure unit tests covering scope discovery, only-here, uniqueness, and classification. These are fast (no DB) and cover the riskiest new logic.

Integration tests (Stories 2–6) use `open_in_memory_for_test()` with the production schema. Setup creates a realistic scenario with source roots, archive roots, overlapping/unique/complementary sources.

Two tests are flagged as **critical correctness tests** from the design review:
1. Internal duplicates at a location must NOT disqualify content from being "only here"
2. "Only here" must count unique object_ids, not sources (3 copies of same content = 1)

## Non-Goals

- New persistence — survey reads existing data only
- Replacing `coverage` or `compare` — they serve different purposes
- Interactive mode or TUI — survey produces static output
- Automatic focal point recommendation — the data supports the decision, the user makes it
- Cross-command state — survey doesn't save results for other commands to consume

## Documentation Updates

- New page in `docs/`: survey command reference with usage, options, examples, and "reading the output" guide
- Update `docs/SUMMARY.md` to include the survey page
- CLAUDE.md: survey command entry, scope discovery concept, asymmetric visibility model, "only here" counts objects

## Risks

| Risk | Mitigation |
|------|------------|
| Scope discovery produces paths that aren't actionable | Thorough unit tests with diverse path distributions. Algorithm is tunable (constants, not architecture). |
| Per-location filter passes too slow for large libraries | Bounded by location cap (10). `--brief` skips entirely. Can optimize later if real-world profiling shows need. |
| Memory usage with very large libraries | Inherited from existing `batch_fetch_by_roots` pattern. Not a new scaling concern. |
| "Only here" correctness bugs | Two dedicated critical tests. Pure domain function is simple to reason about. |
| Scope discovery collapsing too aggressively or not enough | Experimentation with real data may reveal need for tuning. The algorithm and thresholds are separate concerns. |

## Version History

| Date | Change |
|------|--------|
| 2026-03-07 | Initial epic spec created from /design panel |
