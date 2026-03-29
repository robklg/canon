# Story: Note Listing at Scale

**Design Spec**: `~/store/claude-designs/2026-03-29-note-listing-at-scale.md`
**Status**: Done
**Created**: 2026-03-29

## Objective

Notes accumulate quickly — a sign the breadcrumbs feature is landing right. But the listing modes (`--global` and `-r`) don't scale: they dump all notes grouped by path, which becomes unreadable as volume grows. This work refines the listing to serve two distinct cognitive modes:

- **Temporal** (default): "What's been on my mind recently?" — recent notes by date, capped, most recent at the bottom closest to the prompt
- **Spatial** (opt-in via `--by-scope`): "What's the map of this area?" — one line per location showing the most recent note and note count

Both modes are capped at 10 entries by default with a footer showing overflow counts. `--limit N` overrides the cap (0 = unlimited). View mode (scoped, no flags) is unchanged.

## Functional Requirements Summary

**Story 1 — Temporal (default)**: `--global` and `-r` show the 10 most recent notes, displayed oldest-first. Footer: `(N more notes, M more locations)`. `--limit` overrides.

**Story 2 — Spatial (opt-in)**: `--by-scope` on `--global` or `-r` shows one line per location: path, note count `(N)`, date and text of most recent note. Ordered by most recent note date (oldest location at top). Capped at 10. Footer: `(N more locations with notes)`. `--by-scope` alone inside a root implies `-r`.

**Story 3 — View mode**: Unchanged.

## Current State

### Repo layer (`repo/note.rs`)

| Function | Purpose | Used by |
|----------|---------|---------|
| `fetch_all()` | All notes, ordered by root_id/path/time | `ops::note::list_notes_global()` |
| `fetch_subtree_chronological()` | Subtree notes, ordered by path/time | `ops::note::list_notes_recursive()` |
| `fetch_subtree()` | Subtree notes, ordered by recency DESC | `ops::note::survey_note_context()` |
| `count_subtree_notes()` | Total count for subtree | `plan_clear_recursive()` |
| `count_subtree_locations()` | Distinct location count for subtree | `plan_clear_recursive()` |

None of the existing fetch functions support LIMIT or return total counts alongside results. The spatial "most recent per location" pattern doesn't exist.

### Ops layer (`ops/note.rs`)

```rust
pub struct NoteListResult {
    pub notes: Vec<Note>,
    pub roots: HashMap<i64, Root>,
}
```

`list_notes_global()` and `list_notes_recursive()` fetch all notes and return them. No limit parameter. No total counts.

### Interface layer (`note.rs`)

`print_list()` groups notes by path with headers. Ordered by path. No cap, no footer.

### CLI (`main.rs`)

`Note` command has: `path`, `message`, `recursive`, `global`, `clear`, `yes`. No `--by-scope` or `--limit`.

## Design

### Phase 1: Repo Queries

- **Goal**: Add temporal and spatial fetch functions with limit and count support

#### Changes

**`domain/note.rs`** — New type for spatial results:

```rust
/// Summary of a location's notes — most recent note + count.
/// Used by spatial listing mode.
#[derive(Debug, Clone)]
pub struct LocationEntry {
    pub root_id: i64,
    pub rel_path: String,
    pub note_count: usize,
    pub latest_text: String,
    pub latest_created_at: i64,
}
```

**`repo/note.rs`** — Four new functions:

```rust
/// Fetch the N most recent notes across all roots.
/// Returns (notes in DESC order, total note count, total distinct location count).
/// limit=0 means unlimited.
pub fn fetch_recent(
    conn: &Connection,
    limit: usize,
) -> Result<(Vec<Note>, usize, usize)>
```

SQL:
```sql
-- Notes (limited)
SELECT {NOTE_COLUMNS} FROM notes ORDER BY created_at DESC LIMIT ?
-- Total note count
SELECT COUNT(*) FROM notes
-- Total location count
SELECT COUNT(*) FROM (SELECT DISTINCT root_id, rel_path FROM notes)
```

When limit=0, omit the LIMIT clause.

```rust
/// Fetch the N most recent notes within a subtree.
/// Returns (notes in DESC order, total note count in subtree, total location count in subtree).
pub fn fetch_recent_subtree(
    conn: &Connection,
    root_id: i64,
    rel_path: &str,
    limit: usize,
) -> Result<(Vec<Note>, usize, usize)>
```

Same pattern with the standard subtree WHERE clause (`root_id = ? AND (rel_path = ? OR rel_path LIKE ? || '/%')`). Uses existing `count_subtree_notes()` and `count_subtree_locations()` for the counts.

```rust
/// Fetch the most recent note per location across all roots.
/// Returns (locations in DESC order by most recent note, total location count).
pub fn fetch_locations(
    conn: &Connection,
    limit: usize,
) -> Result<(Vec<LocationEntry>, usize)>
```

SQL using window functions:
```sql
SELECT root_id, rel_path, note_count, text, created_at
FROM (
    SELECT root_id, rel_path, text, created_at,
           COUNT(*) OVER (PARTITION BY root_id, rel_path) as note_count,
           ROW_NUMBER() OVER (PARTITION BY root_id, rel_path ORDER BY created_at DESC) as rn
    FROM notes
) ranked
WHERE rn = 1
ORDER BY created_at DESC
LIMIT ?
```

Total count: `SELECT COUNT(*) FROM (SELECT DISTINCT root_id, rel_path FROM notes)`.

```rust
/// Fetch the most recent note per location within a subtree.
pub fn fetch_locations_subtree(
    conn: &Connection,
    root_id: i64,
    rel_path: &str,
    limit: usize,
) -> Result<(Vec<LocationEntry>, usize)>
```

Same window function query with subtree WHERE on the inner query.

#### Tests

In `repo/note.rs`:

- `fetch_recent_returns_most_recent` — 15 notes across locations, limit 5 → 5 notes returned, most recent first (DESC)
- `fetch_recent_counts_correct` — verify total_notes and total_locations from same data
- `fetch_recent_unlimited` — limit 0 → all notes returned
- `fetch_recent_fewer_than_limit` — 3 notes, limit 10 → 3 returned
- `fetch_recent_subtree_filters` — notes across roots, only subtree returned and counted
- `fetch_recent_subtree_counts_within_scope` — counts reflect subtree, not global
- `fetch_locations_one_per_location` — 3 locations with varying note counts → one entry each with correct latest note and count
- `fetch_locations_ordered_by_recency` — locations ordered by most recent note DESC
- `fetch_locations_count_per_location` — verify note_count matches actual count per location
- `fetch_locations_limited` — 10 locations, limit 5 → 5 returned, total_count=10
- `fetch_locations_subtree_filters` — scoped variant only includes subtree locations
- `fetch_locations_total_count` — total includes all locations, not just displayed

### Phase 2: Ops Layer

- **Goal**: Update ops functions to accept limit and return enriched results; add spatial variants

#### Changes

**`ops/note.rs`** — Updated result types:

```rust
pub struct NoteListResult {
    pub notes: Vec<Note>,              // oldest-first (reversed from repo's DESC)
    pub roots: HashMap<i64, Root>,
    pub total_note_count: usize,       // new
    pub total_location_count: usize,   // new
}

pub struct NoteSpatialResult {
    pub locations: Vec<LocationEntry>, // oldest-first by most recent note
    pub roots: HashMap<i64, Root>,
    pub total_location_count: usize,
}
```

**Updated functions:**

```rust
const DEFAULT_LIMIT: usize = 10;

/// List notes globally, temporal mode.
/// limit: None = default (10), Some(0) = unlimited, Some(n) = n.
pub fn list_notes_global(conn: &Connection, limit: Option<usize>) -> Result<NoteListResult> {
    let effective = limit.unwrap_or(DEFAULT_LIMIT);
    let (mut notes, total_note_count, total_location_count) =
        repo::note::fetch_recent(conn, effective)?;
    notes.reverse(); // oldest-first for display
    let all_roots = repo::root::fetch_all(conn)?;
    let roots = all_roots.into_iter().map(|r| (r.id, r)).collect();
    Ok(NoteListResult { notes, roots, total_note_count, total_location_count })
}

/// List notes recursively, temporal mode.
pub fn list_notes_recursive(
    conn: &Connection,
    scope: &NoteScope,
    limit: Option<usize>,
) -> Result<NoteListResult> {
    let effective = limit.unwrap_or(DEFAULT_LIMIT);
    let (mut notes, total_note_count, total_location_count) =
        repo::note::fetch_recent_subtree(conn, scope.root_id, &scope.rel_path, effective)?;
    notes.reverse();
    let all_roots = repo::root::fetch_all(conn)?;
    let roots = all_roots.into_iter().map(|r| (r.id, r)).collect();
    Ok(NoteListResult { notes, roots, total_note_count, total_location_count })
}

/// List locations globally, spatial mode.
pub fn list_locations_global(conn: &Connection, limit: Option<usize>) -> Result<NoteSpatialResult> {
    let effective = limit.unwrap_or(DEFAULT_LIMIT);
    let (mut locations, total_location_count) =
        repo::note::fetch_locations(conn, effective)?;
    locations.reverse(); // oldest-first for display
    let all_roots = repo::root::fetch_all(conn)?;
    let roots = all_roots.into_iter().map(|r| (r.id, r)).collect();
    Ok(NoteSpatialResult { locations, roots, total_location_count })
}

/// List locations recursively, spatial mode.
pub fn list_locations_recursive(
    conn: &Connection,
    scope: &NoteScope,
    limit: Option<usize>,
) -> Result<NoteSpatialResult> {
    let effective = limit.unwrap_or(DEFAULT_LIMIT);
    let (mut locations, total_location_count) =
        repo::note::fetch_locations_subtree(conn, scope.root_id, &scope.rel_path, effective)?;
    locations.reverse();
    let all_roots = repo::root::fetch_all(conn)?;
    let roots = all_roots.into_iter().map(|r| (r.id, r)).collect();
    Ok(NoteSpatialResult { locations, roots, total_location_count })
}
```

Note: `list_notes_recursive()` signature changes — it now takes `limit`. The existing caller in `note.rs` passes `None`.

The view-mode fallback (`canon note` outside root → global list) also calls `list_notes_global()` — it passes `None` for the default cap.

#### Tests

In `ops/note.rs`:

- `list_global_temporal_reverses_to_oldest_first` — verify order is ASC after reversal
- `list_global_temporal_default_limit` — 15 notes, no limit arg → 10 returned with correct totals
- `list_global_spatial_returns_location_entries` — verify spatial result with counts
- `list_recursive_temporal_scoped` — verify subtree filtering with limit

### Phase 3: Interface + CLI

- **Goal**: New flags, mode resolution, temporal and spatial output formatters, footer

#### Changes

**`main.rs`** — Add flags to `Note` command:

```rust
Note {
    path: Option<PathBuf>,
    #[arg(short = 'm')]
    message: Option<String>,
    #[arg(short = 'r', long)]
    recursive: bool,
    #[arg(long)]
    global: bool,
    #[arg(long)]
    clear: bool,
    #[arg(long)]
    yes: bool,
    /// Group by location, show most recent note per location
    #[arg(long)]
    by_scope: bool,
    /// Maximum number of entries to display (default: 10, 0 = unlimited)
    #[arg(long)]
    limit: Option<usize>,
}
```

Pass `by_scope` and `limit` through to `note::run()`.

**`note.rs`** — Updated `run()`:

Mode resolution at the top of `run()`:
```rust
// --by-scope without --global or -r implies -r
let recursive = recursive || (by_scope && !global);
```

Updated listing dispatch:
```rust
if global {
    if by_scope {
        let result = ops::note::list_locations_global(conn, limit)?;
        print_spatial(&result, true);
    } else {
        let result = ops::note::list_notes_global(conn, limit)?;
        print_temporal(&result, true);
    }
    return Ok(());
}

if recursive {
    let scope = resolve_single_scope(conn, path, false)?;
    if by_scope {
        let result = ops::note::list_locations_recursive(conn, &scope, limit)?;
        print_spatial(&result, false);
    } else {
        let result = ops::note::list_notes_recursive(conn, &scope, limit)?;
        print_temporal(&result, false);
    }
    return Ok(());
}

// View mode — unchanged, except global fallback uses temporal
match resolve_single_scope_optional(conn, path)? {
    Some(scope) => {
        let result = ops::note::view_notes(conn, &scope)?;
        print_view(&result);
    }
    None => {
        let result = ops::note::list_notes_global(conn, limit)?;
        print_temporal(&result, true);
    }
}
```

**New `print_temporal()`**:

```rust
fn print_temporal(result: &NoteListResult, use_full_path: bool) {
    use std::io::Write;
    let stdout = std::io::stdout();
    let mut handle = stdout.lock();

    // Compute max path width for alignment
    let max_path_len = result.notes.iter()
        .map(|n| display_path(n, &result.roots, use_full_path).len())
        .max()
        .unwrap_or(0);

    for note in &result.notes {
        let path = display_path(note, &result.roots, use_full_path);
        if writeln!(handle, "{:<width$}  {}  {}",
            path, format_note_date(note.created_at), note.text,
            width = max_path_len
        ).is_err() {
            break;
        }
    }

    // Footer on stderr
    let displayed = result.notes.len();
    let remaining_notes = result.total_note_count.saturating_sub(displayed);
    if remaining_notes > 0 {
        // Count distinct locations in displayed notes
        let displayed_locations: std::collections::HashSet<_> = result.notes.iter()
            .map(|n| (n.root_id, &n.rel_path))
            .collect();
        let remaining_locations = result.total_location_count.saturating_sub(displayed_locations.len());
        eprintln!("({remaining_notes} more notes, {remaining_locations} more locations)");
    }
}
```

**New `print_spatial()`**:

```rust
fn print_spatial(result: &NoteSpatialResult, use_full_path: bool) {
    use std::io::Write;
    let stdout = std::io::stdout();
    let mut handle = stdout.lock();

    let max_path_len = result.locations.iter()
        .map(|l| location_display_path(l, &result.roots, use_full_path).len())
        .max()
        .unwrap_or(0);

    let max_count_len = result.locations.iter()
        .map(|l| format!("({})", l.note_count).len())
        .max()
        .unwrap_or(0);

    for loc in &result.locations {
        let path = location_display_path(loc, &result.roots, use_full_path);
        let count = format!("({})", loc.note_count);
        if writeln!(handle, "{:<pwidth$}  {:>cwidth$}  {}  {}",
            path, count,
            format_note_date(loc.latest_created_at),
            loc.latest_text,
            pwidth = max_path_len,
            cwidth = max_count_len
        ).is_err() {
            break;
        }
    }

    // Footer on stderr
    let remaining = result.total_location_count.saturating_sub(result.locations.len());
    if remaining > 0 {
        eprintln!("({remaining} more locations with notes)");
    }
}
```

Helper functions `display_path()` and `location_display_path()` extract the path string from a note/location entry using the roots map — same logic as the current `print_list()`.

**Remove `print_list()`** — replaced by the two new functions.

#### Tests

No new automated tests in this phase — the output formatting is best verified manually and the logic is covered by Phase 1 and 2 tests. Column alignment is pure formatting.

### Phase 4: Documentation

- **Goal**: Update user docs and CLAUDE.md

#### Changes

**`docs/src/commands/manage/note.md`**:
- Document `--by-scope` flag and spatial listing mode
- Document `--limit` flag with default 10 and `--limit 0` for unlimited
- Update listing examples to show temporal (default) and spatial output
- Document footer behavior
- Document `--by-scope` implying `-r` when used alone inside a root

**CLAUDE.md**:
- Update the `note` entry in Commands section to mention `--by-scope` and `--limit`
- Add `LocationEntry` to `domain/note.rs` in the Domain Layer section
- Add `NoteSpatialResult` to ops/note.rs types
- Add spatial listing ops functions to ops/note.rs listing

#### Tests

`cargo clippy` — ensure no warnings.

## Architectural Decisions

| Decision | Rationale |
|----------|-----------|
| `LocationEntry` in `domain/note.rs` | Domain-adjacent data type returned by repo, consumed by ops. Can't live in ops (repo can't import ops). Can't live in repo (it's a concept, not SQL). Domain is the right place. |
| Window functions for spatial query | SQLite 3.25+ supports them. Cleaner than self-join. Single pass over the data. Canon already requires SQLite 3.35+ (RETURNING clause used in insert). |
| Reverse in ops, not SQL | Fetching DESC LIMIT N then reversing a tiny Vec is simpler than a SQL subquery with outer ASC. The volume is ≤N entries. |
| `limit: Option<usize>` in ops | `None` = default (10), `Some(0)` = unlimited, `Some(n)` = explicit. Clean interface for the CLI layer. |
| Footer on stderr | Data on stdout, metadata on stderr — consistent with `ls` scope display and excluded-count hints. Enables clean piping. |
| `--by-scope` implies `-r` | Natural meaning: "show me the spatial map of here and below." Avoids requiring two flags for the most common spatial use case. |

## Non-Goals

- No changes to view mode (`canon note` scoped, no flags)
- No changes to add mode (`-m`)
- No changes to clear mode (`--clear`)
- No changes to survey integration
- No search/filter on note text (grep handles this)

## Test Plan

### Existing Tests (Must Pass)

- `repo/note.rs` — 33 tests (all existing fetch, count, clear, delete tests)
- `ops/note.rs` — 8 tests (view, list, clear plan/execute, survey context)
- `domain/note.rs` — 5 tests (ancestor_paths, relative_to_scope)

### New Tests

**Phase 1** (repo): 12 tests covering temporal and spatial fetch with limits, counts, ordering, subtree filtering
**Phase 2** (ops): 4 tests covering reversal, default limit, spatial results, scoped temporal

## Implementation Checklist

- [ ] Phase 1: Repo queries — temporal + spatial fetch with limit and counts
- [ ] Phase 2: Ops layer — updated result types, limit threading, spatial operations
- [ ] Phase 3: Interface + CLI — flags, mode resolution, temporal/spatial formatters, footer
- [ ] Phase 4: Documentation — note.md, CLAUDE.md
- [ ] Verify all existing tests pass
- [ ] `cargo clippy` clean

## Documentation Updates

- **`docs/src/commands/manage/note.md`**: Add `--by-scope` and `--limit` flags, update listing examples, document temporal/spatial modes and footer
- **CLAUDE.md**: Update note command entry, add new domain/ops types

## Backward Compatibility

- `--global` and `-r` output changes from path-grouped to date-ordered, and is now capped at 10. Users who want all notes: `--limit 0`. Users who want path-grouped: `--by-scope --limit 0`.
- View mode and add/clear are completely unchanged.
- No flag removals or renames.

## Performance Considerations

- All limits applied at SQL level — no fetching entire table and truncating
- Count queries are lightweight (no row data, just aggregation)
- Window function query scans notes table once per spatial fetch
- Maximum data transfer is bounded by limit (default 10 rows)
- Reversal is O(N) on ≤limit entries — negligible
