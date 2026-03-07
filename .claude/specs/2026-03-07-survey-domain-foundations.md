# Story: Survey Domain Foundations

**Epic**: [Survey Command — Outward-Looking Comparison](.claude/specs/epic-survey-command.md) (Story 1)
**Status**: Pending
**Created**: 2026-03-07

## Objective

Build and thoroughly test all pure domain logic that the `survey` command will need — scope discovery, "only here" counting, uniqueness counting, and location classification — before any command wiring. This is the foundation for assembly support: giving Canon the ability to answer contextual, outward-looking questions from data it already has.

## Functional Requirements Summary

The `survey` command needs four groups of pure domain logic:

1. **Scope discovery**: Given overlapping file paths on a root, find actionable directory prefixes where overlap concentrates. A collapsing tree algorithm that drills through single-child chains and splits at branching points.
2. **"Only here" counting**: Given a set of object_ids at a location and the full universe, count how many objects exist *only* at that location. Internal duplicates must NOT disqualify content.
3. **"Unique to selection" counting**: Given a selection's object_ids and the full universe, count how many objects have no copy outside the selection (including archives).
4. **Location classification**: Given shared/complementary counts, classify as Superset (high overlap + complementary), Lead (has complementary), or Mirror (overlap only).

All functions take pre-computed data structures. No I/O, no database access. Counts are unique object_ids, not sources.

## Current State

- `domain/path.rs` provides `path_is_under()` — reused by `count_only_here`
- `domain/source.rs` provides the `Source` struct — used as value type in the object index
- `domain/exclusion.rs` provides the pattern: pure functions on pre-built data structures, thorough unit tests with `make_source` helpers
- `domain/mod.rs` needs a new `pub mod survey` entry and re-exports

## Design

### Phase 1: Types and Scope Discovery

- **Goal**: Build and test the most complex new logic — the tree-building scope discovery algorithm

#### Changes

Create `src/domain/survey.rs` with:

**Public type:**
```rust
/// Result of scope discovery for a single root.
pub struct DiscoveredScope {
    pub prefix: String,   // relative to root (empty = root itself)
    pub count: usize,     // overlap count at this scope
}
```

**Internal tree structure:**
```rust
struct DirNode {
    direct: usize,                        // files directly in this directory
    children: BTreeMap<String, DirNode>,   // subdirectories (sorted for determinism)
}

impl DirNode {
    fn new() -> Self { ... }
    fn count(&self) -> usize {
        self.direct + self.children.values().map(|c| c.count()).sum::<usize>()
    }
}
```

**Internal helpers:**
- `dir_portion(rel_path: &str) -> &str` — extract directory portion via `rsplit_once('/')`
- `insert_into_tree(root: &mut DirNode, dir: &str)` — walk segments, create nodes, increment `direct`
- `join_prefix(prefix: &str, segment: &str) -> String` — join path segments, handle empty prefix
- `walk(node: &DirNode, prefix: String, results: &mut Vec<DiscoveredScope>)` — three-case top-down traversal

**Public function:**
```rust
/// Core algorithm: find directory prefixes that concentrate overlap.
/// Input: rel_paths of overlapping files within a single root.
/// Extracts directory portions, builds tree, walks with collapsing rule.
pub fn discover_scopes(rel_paths: &[&str]) -> Vec<DiscoveredScope>
```

**The three walk cases:**

| Condition | Action |
|-----------|--------|
| `direct == 0`, one child | Skip node, recurse into child (collapse single-child chain) |
| `direct == 0`, multiple children | Don't emit, recurse into each child separately |
| `direct > 0` or leaf | Emit as scope with total descendant count |

#### Tests

1. **Single directory** — `["a/b/c/x.jpg", "a/b/c/y.jpg"]` → `[("a/b/c", 2)]`. Drills through single-child chain.
2. **Multiple siblings** — `["photos/a.jpg", "videos/b.jpg"]` → `[("photos", 1), ("videos", 1)]`. Splits at branching point.
3. **Files at root** — `["a.jpg", "b.jpg"]` → `[("", 2)]`. Empty prefix scope.
4. **Mixed depth** — `["photos/a.jpg", "photos/2016/b.jpg"]` → `[("photos", 2)]`. Direct files prevent drill-down.
5. **Deep single path** — `["a/b/c/d/e/f.jpg"]` → `[("a/b/c/d/e", 1)]`. Long chain collapses.
6. **Separate branches at root** — `["a/x.jpg", "b/y.jpg", "c/z.jpg"]` → `[("a", 1), ("b", 1), ("c", 1)]`. Root splits.
7. **Single file** — `["only.jpg"]` → `[("", 1)]`. Degenerate minimal input.
8. **Deep directory with sibling at higher level** — `["a/b/c/x.jpg", "d/y.jpg"]` → `[("a/b/c", 1), ("d", 1)]`. Independent collapsing per branch.
9. **Uniform distribution** — `["a/x.jpg", "a/y.jpg", "b/x.jpg", "b/y.jpg"]` → `[("a", 2), ("b", 2)]`. No false collapsing.
10. **Empty input** — `[]` → `[]`. No panic.

### Phase 2: Object-Level Functions

- **Goal**: Build the two counting functions with their critical correctness tests

#### Changes

Add to `src/domain/survey.rs`:

```rust
/// Count object_ids that exist only within location_path.
/// For each object_id, checks if ANY source in the universe exists outside
/// the location. Internal duplicates at the location do NOT disqualify.
/// Counts unique object_ids, not sources.
pub fn count_only_here(
    object_ids: &HashSet<i64>,
    location_path: &str,
    by_object_id: &HashMap<i64, Vec<&Source>>,
) -> usize

/// Count selection object_ids with no source outside the selection.
/// "Outside" means source.id not in selection_source_ids.
/// Archive copies in the index but not in the selection count as outside.
pub fn count_unique_to_selection(
    selection_object_ids: &HashSet<i64>,
    selection_source_ids: &HashSet<i64>,
    by_object_id: &HashMap<i64, Vec<&Source>>,
) -> usize
```

Uses `path_is_under()` from `domain/path.rs` for the location-based check in `count_only_here`. Uses source ID membership for `count_unique_to_selection`.

#### Tests

11. **Content only at location** — Object 42 has sources at `["/loc/a.jpg", "/loc/b.jpg"]`, both inside `/loc` → count 1. **CRITICAL CORRECTNESS TEST**: internal duplicates must NOT disqualify.
12. **Content exists elsewhere** — Object 42 has sources at `["/loc/a.jpg", "/other/b.jpg"]` → count 0. Copy outside the location.
13. **Internal duplicates don't inflate count** — Object 42 has 3 sources all at `/loc/` → count 1, not 3. **CRITICAL CORRECTNESS TEST**: counts object_ids, not sources.
14. **Mixed** — Objects 42, 43, 44: two "only here", one not → count 2.
15. **Path boundary** — Object 42 at `["/photos/a.jpg", "/photography/b.jpg"]`, location `/photos` → count 0. `/photography` is not under `/photos`.
16. **Truly unique** — Object in selection, no copies outside → count 1.
17. **Not unique, copy outside** — Object in selection + source outside selection → count 0.
18. **Not unique, archived copy** — Object in selection + archive source not in selection → count 0. Archives count as "elsewhere."
19. **Duplicate within selection** — Two sources for same object, both in selection → count 1.

### Phase 3: Classification, Wrapper, and Wiring

- **Goal**: Complete the module with classification, the grouping wrapper, and domain layer wiring

#### Changes

Add to `src/domain/survey.rs`:

```rust
/// Classification of a related location.
/// Variant order defines sort priority (lowest first): Superset → Lead → Mirror.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LocationKind {
    Superset,   // shared >= threshold AND has complementary
    Lead,       // has complementary content
    Mirror,     // overlap only, no complementary
}

/// Classify a location based on shared/complementary data.
/// Guards against division by zero when total_hashed == 0.
pub fn classify_location(
    shared_count: usize,
    total_hashed: usize,
    complementary_count: usize,
    superset_threshold: f64,
) -> LocationKind

/// Group sources by root, run discover_scopes per root,
/// return (absolute_scope_path, count) pairs.
pub fn discover_scopes_by_root(sources: &[&Source]) -> Vec<(String, usize)>
```

`discover_scopes_by_root` groups by `root_path` (BTreeMap for determinism), extracts `rel_path` from each source, calls `discover_scopes` per group, prepends `root_path` to each result prefix (handling empty prefix like `Source::path()`).

Update `src/domain/mod.rs`:
```rust
pub mod survey;
pub use survey::{
    classify_location, count_only_here, count_unique_to_selection,
    discover_scopes, discover_scopes_by_root, DiscoveredScope, LocationKind,
};
```

#### Tests

20. **Superset** — shared=320, total=400, complementary=95, threshold=0.8 → `Superset` (0.80 >= 0.80).
21. **Lead** — shared=45, total=400, complementary=180, threshold=0.8 → `Lead` (0.1125 < 0.80).
22. **Mirror** — shared=30, total=400, complementary=0, threshold=0.8 → `Mirror`.
23. **Edge of threshold** — shared=319, total=400, complementary=10, threshold=0.8 → `Lead` (0.7975 < 0.80).
24. **`discover_scopes_by_root` integration** — Sources across two roots, verify absolute paths come back with root_path prepended and correct counts.

## Architectural Decisions

| Decision | Rationale |
|----------|-----------|
| `BTreeMap` for DirNode children | Deterministic iteration order without explicit sorting |
| `discover_scopes` takes `&[&str]` (full file rel_paths) | Extracts directory portions internally; simplest test interface |
| DirNode is internal (not exported) | Implementation detail of the algorithm; no external consumer |
| `LocationKind` derives `Ord` with deliberate variant order | Enables natural sort: Superset < Lead < Mirror |
| String splitting on `/` instead of `std::path::Path` | Input is database-stored rel_paths, already normalized; no cross-platform concern |
| `count_only_here` uses `path_is_under` from domain/path | Reuses established, tested path logic rather than reimplementing |
| All functions are infallible (no `Result`) | Pure functions on pre-validated data; error handling stays in command layer |

## Non-Goals

- Command wiring, CLI flags, output formatting (Story 2)
- CLAUDE.md or docs/ updates (Story 2+)
- Repo layer changes (none needed for domain foundations)
- Integration tests with database (pure unit tests only)

## Test Plan

### Existing Tests (Must Pass)

- `domain::path::tests` — we depend on `path_is_under`
- `domain::source::tests` — we use the `Source` struct
- All other existing tests (no modifications to existing code)

### New Tests

24 unit tests organized by function group:
- Scope discovery: tests 1–10 (Phase 1)
- Only here: tests 11–15, including 2 critical correctness tests (Phase 2)
- Unique to selection: tests 16–19 (Phase 2)
- Classification: tests 20–23 (Phase 3)
- discover_scopes_by_root wrapper: test 24 (Phase 3)

### Critical Correctness Tests

Two tests flagged in the epic design review as guarding the most dangerous potential bugs:
- **Test 11**: Internal duplicates at a location must NOT disqualify content from being "only here"
- **Test 13**: "Only here" must count unique object_ids, not sources (3 copies of same content = 1)

## Implementation Checklist

- [x] Phase 1: Types and scope discovery (DirNode, discover_scopes, tests 1–10)
- [x] Phase 2: Object-level functions (count_only_here, count_unique_to_selection, tests 11–19)
- [x] Phase 3: Classification, wrapper, wiring (LocationKind, classify_location, discover_scopes_by_root, tests 20–24, domain/mod.rs)
- [x] Verify all existing tests pass
- [x] `cargo clippy` clean (dead_code warnings expected — no command consumer yet)
- [x] `cargo fmt` clean

## Documentation Updates

None for this story. CLAUDE.md and docs/ updates will come in Story 2 when the command module is wired up and the feature becomes user-visible.

## Backward Compatibility

No user-visible changes. New module, no modifications to existing code.

## Performance Considerations

None. All functions operate on in-memory data structures bounded by the number of overlapping sources/objects. DirNode tree depth is bounded by filesystem path depth. `count()` is recursive but trees are small.
