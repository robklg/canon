# Story: Survey Summary View (No Filters)

**Epic**: [epic-survey-command.md](epic-survey-command.md)
**Status**: Done
**Created**: 2026-03-07
**Depends on**: Story 1 (Domain Foundations)

## Objective

Get `canon survey` working end-to-end for the no-filter case. This wires up the command module, CLI entry point, full orchestration pipeline, and formatted output — producing the simplest useful summary: selection echo, archive status with scope grouping, related locations with shared counts, and unique content count.

This is the foundational story that validates the architecture: the in-memory object index, the scope discovery integration, and the asymmetric visibility model all get exercised with real data for the first time.

## Functional Requirements Summary

From the design spec, the no-filter output looks like:

```
$ canon survey /mnt/old-drive/photos

Selection: /mnt/old-drive/photos
  1,247 sources (38 unhashed, 1,209 hashed)

Archived: 890 of 1,209 (73.6%)
  /archive/photos/                           890

Related locations:
  /mnt/backup-2022/photos/italy-2016/     380 of 1,209 shared
  /mnt/partner-laptop/DCIM/vacation/       45 of 1,209 shared
  /mnt/backup-2022/photos/misc/            30 of 1,209 shared

319 unique to this scope
```

Without `--where`, there's no meaningful definition of "matching content" at other locations, so the "+N more" and "(only here)" affinity columns are absent. Related locations sort by shared count descending.

**What's included in this story:**
- `canon survey [paths]...` with `--include excluded`
- Selection echo with total/unhashed/hashed counts
- Archive status with scope-grouped paths
- Related locations with "N of M shared" counts
- Unique-to-scope count
- Default-to-cwd behavior
- Early exits for empty selection and all-unhashed
- Thousands separators on all counts

**What's NOT included** (deferred to later stories):
- `--where`, `--detail`, `--other`, `--brief`, `--archive`, `--verbose`, `-0`
- Affinity computation ("+N more", "(only here)")
- Classification sort (supersets → leads → mirrors)
- Location cap and truncation

## Current State

**Exists:**
- `domain/survey.rs` — pure domain functions from Story 1 (discover_scopes, count_only_here, count_unique_to_selection, classify_location)
- Standard fetch/filter infrastructure in repo and domain layers
- `format_count()` in ceremony.rs for thousands separators
- `resolve_paths()` in domain/path.rs for offline-capable path resolution
- `ls` dispatch pattern in main.rs for default-to-cwd

**Doesn't exist yet:**
- `src/survey.rs` — the command module
- `Survey` variant in `Commands` enum in main.rs
- Any CLI entry point for survey

## Design

### Phase 1: Command Module and CLI Wiring

- **Goal**: `canon survey` is a recognized command that parses arguments and calls into a run function.

#### Changes

**`src/main.rs`** — add the Survey command variant and dispatch:

```rust
// Add to mod declarations
mod survey;

// Add to Commands enum (in the Discover section, after Coverage)
/// Survey a selection: archive status, related locations, unique content
Survey {
    /// Directory paths to scope the query (resolved to realpath)
    paths: Vec<PathBuf>,
    /// Include additional sources: excluded
    #[arg(long, value_delimiter = ',')]
    include: Vec<IncludeValue>,
},

// Add to match dispatch
Commands::Survey {
    paths,
    include,
} => {
    let include = include_set_from(&include);
    // Validate: --include archived is not valid for survey
    if include.includes_archived() {
        bail!("--include archived is not valid for survey");
    }
    survey::run(&mut db, &paths, &include)?;
}
```

Default-to-cwd is handled inside `survey::run()`, not in `main.rs`. Unlike `ls`, survey doesn't need `use_relative` (always shows absolute paths) or `auto_include_archived` (handles archive visibility via its asymmetric model). This is an intentional divergence from the `ls` dispatch pattern — survey's simpler needs don't justify the complexity. If cwd is not inside any root, `resolve_paths` still resolves it, scope matching finds no sources, and the user sees "0 sources" — the correct behavior for survey.

**`src/survey.rs`** — command module skeleton:

```rust
use anyhow::Result;
use std::path::PathBuf;

use crate::domain::IncludeSet;
use crate::repo::Db;

pub fn run(
    db: &mut Db,
    paths: &[PathBuf],
    include: &IncludeSet,
) -> Result<()> {
    // ... orchestration
    Ok(())
}
```

The signature will grow in later stories (adding `filters`, `options` struct). For now, keep it minimal.

#### Tests

None yet — this phase is just wiring.

### Phase 2: Selection Pipeline and Object Index

- **Goal**: Build the selection from scope paths, compute the in-memory object index. Structure as a testable computation function separate from output formatting.

#### Changes

The orchestration splits into two concerns:
1. **`compute_survey()`** — builds the selection, object index, and all computed results, returning a `SurveyResult` struct. Tests assert on this.
2. **`run()`** — calls `compute_survey()`, then formats and prints.

This mirrors the `coverage.rs` pattern where `compute_*` functions are tested independently of `display_*` functions.

**Result types** (internal to survey.rs):

```rust
struct SurveyResult<'a> {
    scope_prefixes: Vec<String>,
    total_count: usize,
    unhashed_count: usize,
    total_hashed: usize,
    archived_source_count: usize,
    archive_scopes: Vec<(String, usize)>,
    location_results: Vec<LocationResult>,
    unique_count: usize,
}

struct LocationResult {
    path: String,
    shared_count: usize,
}

/// Outcome of compute_survey: either a result to display or an early exit.
enum SurveyOutcome<'a> {
    /// Normal result with all computed data.
    Result(SurveyResult<'a>),
    /// Empty selection — display header and stop.
    Empty { scope_prefixes: Vec<String> },
    /// All unhashed — display header and hashing guidance.
    AllUnhashed {
        scope_prefixes: Vec<String>,
        total_count: usize,
    },
}
```

**The compute function** (follows `ls.rs` and `coverage.rs` orchestration pattern):

```rust
fn compute_survey<'a>(
    conn: &mut rusqlite::Connection,
    paths: &[PathBuf],
    include: &IncludeSet,
    all_sources: &'a [Source],
    all_roots: &[Root],
) -> Result<SurveyOutcome<'a>> {
    // Default to cwd
    let scope_paths = if paths.is_empty() {
        vec![std::env::current_dir()?]
    } else {
        paths.to_vec()
    };

    // Resolve scope paths (soft resolution — offline-capable)
    let scope_prefixes = domain::path::resolve_paths(&scope_paths, all_roots)?;
    let scopes = ScopeMatch::classify_all(&scope_prefixes);

    // Build selection: active, source role, in scope, visibility rules
    let selection: Vec<&Source> = all_sources.iter()
        .filter(|s| s.is_active())
        .filter(|s| s.is_from_role("source"))
        .filter(|s| s.matches_scope(&scopes))
        .filter(|s| include.includes_excluded() || !s.is_excluded())
        .collect();

    // Partition: unhashed vs hashed
    let total_count = selection.len();
    let hashed: Vec<&Source> = selection.iter()
        .filter(|s| s.object_id.is_some())
        .copied()
        .collect();
    let unhashed_count = total_count - hashed.len();
    let total_hashed = hashed.len();

    // Early exit: empty selection
    if total_count == 0 {
        return Ok(SurveyOutcome::Empty { scope_prefixes });
    }

    // Early exit: all unhashed
    if total_hashed == 0 {
        return Ok(SurveyOutcome::AllUnhashed {
            scope_prefixes,
            total_count,
        });
    }

    // Collect selection identity
    let sel_object_ids: HashSet<i64> = hashed.iter()
        .filter_map(|s| s.object_id)
        .collect();
    let sel_source_ids: HashSet<i64> = selection.iter()
        .map(|s| s.id)
        .collect();

    // Build object index from ALL active, non-excluded, hashed sources
    let mut by_object_id: HashMap<i64, Vec<&Source>> = HashMap::new();
    for s in all_sources {
        if s.is_active() && !s.is_excluded() {
            if let Some(oid) = s.object_id {
                by_object_id.entry(oid).or_default().push(s);
            }
        }
    }

    // ... archive, overlap, unique computations follow (Phases 3-5)

    Ok(SurveyOutcome::Result(SurveyResult {
        scope_prefixes,
        total_count,
        unhashed_count,
        total_hashed,
        archived_source_count,
        archive_scopes,
        location_results,
        unique_count,
    }))
}
```

**The run() function:**

```rust
pub fn run(
    db: &mut Db,
    paths: &[PathBuf],
    include: &IncludeSet,
) -> Result<()> {
    let conn = db.conn_mut();

    // Fetch all roots and sources upfront
    let all_roots = repo::root::fetch_all(conn)?;
    let root_ids: Vec<i64> = all_roots.iter().map(|r| r.id).collect();
    let all_sources = repo::source::batch_fetch_by_roots(conn, &root_ids)?;

    match compute_survey(conn, paths, include, &all_sources, &all_roots)? {
        SurveyOutcome::Empty { scope_prefixes } => {
            print_selection_header(&scope_prefixes, 0, 0, 0);
        }
        SurveyOutcome::AllUnhashed {
            scope_prefixes,
            total_count,
        } => {
            print_selection_header(&scope_prefixes, total_count, total_count, 0);
            println!();
            println!("No hashed sources in selection. Content comparison requires hashing.");
            println!("Use `canon worklist` to generate a hashing worklist.");
        }
        SurveyOutcome::Result(result) => {
            print_selection_header(
                &result.scope_prefixes,
                result.total_count,
                result.unhashed_count,
                result.total_hashed,
            );
            println!();
            print_archive_section(
                result.archived_source_count,
                result.total_hashed,
                &result.archive_scopes,
            );
            println!();
            print_related_locations(&result.location_results, result.total_hashed);
            println!();
            println!("{} unique to this scope", format_count(result.unique_count));
        }
    }

    Ok(())
}
```

**Key details:**
- `all_sources` is owned by `run()` and passed as `&[Source]` to `compute_survey()`, which borrows from it for `selection`, `hashed`, and `by_object_id`.
- The object index includes ALL active non-excluded hashed sources — not just the selection. This is what makes overlap/archive/unique checks work.
- `sel_source_ids` includes all selection sources (including unhashed). This is technically imprecise — only hashed source IDs are tested in practice — but harmless, since unhashed sources aren't in the object index and are never compared against.
- `sel_object_ids` is the set of content hashes the user cares about.

### Phase 3: Archive Status

- **Goal**: Compute archive coverage and scope-grouped archive paths.

#### Changes

Continuing inside `compute_survey()`, after the object index is built:

```rust
    // Archive status: find selection content that has archive copies
    let mut archive_sources: Vec<&Source> = Vec::new();
    let mut archived_object_ids: HashSet<i64> = HashSet::new();

    for &oid in &sel_object_ids {
        if let Some(siblings) = by_object_id.get(&oid) {
            let mut found_archive = false;
            for sib in siblings {
                if sib.is_from_role("archive") {
                    if !found_archive {
                        archived_object_ids.insert(oid);
                        found_archive = true;
                    }
                    archive_sources.push(sib);
                }
            }
        }
    }

    // Count selection sources that are archived (source-based counting)
    let archived_source_count = hashed.iter()
        .filter(|s| archived_object_ids.contains(&s.object_id.unwrap()))
        .count();

    // Scope discovery on archive sources for grouped display
    // Sort by path for deterministic output
    let mut archive_scopes = domain::survey::discover_scopes_by_root(&archive_sources);
    archive_scopes.sort_by(|a, b| a.0.cmp(&b.0));
```

**Counting subtlety**: `archived_source_count` counts *selection sources* whose content has an archive copy, not archive sources themselves. If 3 selection sources share one object_id and that object is archived, count = 3. This is consistent with `coverage.rs`.

The `archive_scopes` from `discover_scopes_by_root` give us the grouped paths for display (e.g., `/archive/photos/2016/` with count 285, not just the archive root). Sorted alphabetically by path for deterministic output.

**Why no dedup is needed**: `sel_object_ids` is a HashSet — each object_id is iterated exactly once. For a given object_id, each sibling source appears exactly once in the object index (a source has exactly one object_id). So each archive source is encountered exactly once during the iteration. No `seen_archive_ids` guard is necessary.

### Phase 4: Overlap and Related Locations

- **Goal**: Find sources outside the selection that share content, run scope discovery, compute per-location shared counts.

#### Changes

```rust
    // Collect overlap sources on source roots (not in selection)
    let mut overlap_sources: Vec<&Source> = Vec::new();
    for &oid in &sel_object_ids {
        if let Some(siblings) = by_object_id.get(&oid) {
            for sib in siblings {
                if !sel_source_ids.contains(&sib.id)
                    && sib.is_from_role("source")
                {
                    overlap_sources.push(sib);
                }
            }
        }
    }

    // Scope discovery on overlap sources → related locations
    let location_scopes = domain::survey::discover_scopes_by_root(&overlap_sources);
```

**Why no dedup is needed**: Same reasoning as archive sources. Each object_id in `sel_object_ids` is unique; each source appears in exactly one siblings list. The `!sel_source_ids.contains(&sib.id)` guard (necessary here — a selection source on a source root must not appear as an overlap source) is a filter, not dedup.

**Per-location shared count**: For each discovered scope, count how many *selection* sources have content at that location. This is the "N of M shared" number:

```rust
    let mut location_results: Vec<LocationResult> = Vec::new();

    for (scope_path, _overlap_count) in &location_scopes {
        let loc_scope = vec![ScopeMatch::UnderDirectory(scope_path.clone())];

        // Object IDs present at this location (from overlap sources)
        let loc_object_ids: HashSet<i64> = overlap_sources.iter()
            .filter(|s| s.matches_scope(&loc_scope))
            .filter_map(|s| s.object_id)
            .collect();

        // Count selection sources whose content appears at this location
        let shared_count = hashed.iter()
            .filter(|s| loc_object_ids.contains(&s.object_id.unwrap()))
            .count();

        location_results.push(LocationResult {
            path: scope_path.clone(),
            shared_count,
        });
    }

    // Sort by shared count descending (no classification yet — Story 3)
    location_results.sort_by(|a, b| b.shared_count.cmp(&a.shared_count));
```

**Important**: `shared_count` is bounded by `total_hashed` (selection size). It answers "what fraction of my selection is duplicated here?"

The `_overlap_count` from scope discovery is the count of overlap sources at that scope — useful for the algorithm but not displayed directly. What's displayed is `shared_count` which counts from the selection's perspective.

Constructing `ScopeMatch::UnderDirectory` directly (rather than via `ScopeMatch::classify_all`) is correct here — we know discovered scopes are directory paths by construction.

### Phase 5: Unique Count and Output Formatting

- **Goal**: Compute uniqueness, format and print all sections.

#### Changes

```rust
    // Unique count
    let unique_count = domain::survey::count_unique_to_selection(
        &sel_object_ids,
        &sel_source_ids,
        &by_object_id,
    );
```

**Output formatting functions** (private helpers in survey.rs):

```rust
fn print_selection_header(
    scope_prefixes: &[String],
    total: usize,
    unhashed: usize,
    hashed: usize,
) {
    // "Selection: <path>" or "Selection:" with one per line for multiple
    if scope_prefixes.len() == 1 {
        println!("Selection: {}", scope_prefixes[0]);
    } else {
        println!("Selection:");
        for p in scope_prefixes {
            println!("  {}", p);
        }
    }

    // Count line — always show unhashed/hashed breakdown
    println!(
        "  {} sources ({} unhashed, {} hashed)",
        format_count(total),
        format_count(unhashed),
        format_count(hashed),
    );
}
```

**Archive section:**

```rust
fn print_archive_section(
    archived_count: usize,
    total_hashed: usize,
    archive_scopes: &[(String, usize)],
) {
    if archived_count == 0 {
        println!("Archived: 0 of {}", format_count(total_hashed));
        return;
    }

    let pct = 100.0 * archived_count as f64 / total_hashed as f64;
    println!(
        "Archived: {} of {} ({:.1}%)",
        format_count(archived_count),
        format_count(total_hashed),
        pct,
    );

    // Scope-grouped archive paths with right-aligned counts
    let max_path_len = archive_scopes.iter().map(|(p, _)| p.len()).max().unwrap_or(0);
    let max_count_len = archive_scopes.iter()
        .map(|(_, c)| format_count(*c).len())
        .max()
        .unwrap_or(0);

    for (path, count) in archive_scopes {
        println!(
            "  {:path_w$}  {:>count_w$}",
            path,
            format_count(*count),
            path_w = max_path_len,
            count_w = max_count_len,
        );
    }
}
```

**Related locations section:**

```rust
fn print_related_locations(
    locations: &[LocationResult],
    total_hashed: usize,
) {
    if locations.is_empty() {
        println!("No related locations found.");
        return;
    }

    println!("Related locations:");

    let max_path_len = locations.iter().map(|l| l.path.len()).max().unwrap_or(0);
    let max_shared_len = locations.iter()
        .map(|l| format_count(l.shared_count).len())
        .max()
        .unwrap_or(0);
    let m_str = format_count(total_hashed);

    for loc in locations {
        println!(
            "  {:path_w$}  {:>count_w$} of {} shared",
            loc.path,
            format_count(loc.shared_count),
            m_str,
            path_w = max_path_len,
            count_w = max_shared_len,
        );
    }
}
```

## Architectural Decisions

| Decision | Rationale |
|----------|-----------|
| Handle default-to-cwd inside `survey::run()` | Simpler than the `ls` pattern in main.rs which needs `use_relative` and `auto_include_archived`. Survey doesn't need either — always shows absolute paths, handles archives asymmetrically. Intentional divergence: if cwd is not inside any root, survey shows "0 sources" while `ls` shows everything. |
| Separate `compute_survey()` from `run()` | Tests assert on computed values (counts, paths) via `SurveyResult`, not formatted strings. Mirrors the `coverage.rs` pattern where `compute_*` functions are tested independently of `display_*` functions. |
| Minimal CLI initially (paths + --include only) | Other flags (--where, --detail, etc.) are added in later stories. Avoids dead code and keeps the diff focused. |
| `LocationResult` struct without Option fields | In Story 2, complementary/only_here/kind aren't computed. The struct starts simple and gains Optional fields in Story 3. |
| No dedup guards on archive/overlap collection | `sel_object_ids` is a HashSet — each object_id is iterated exactly once. Each source appears in exactly one siblings list (a source has exactly one object_id). So each source is encountered at most once. Dedup HashSets are unnecessary. |
| Sort archive scopes alphabetically by path | Deterministic output. The functional spec doesn't specify archive scope ordering; alphabetical is natural and predictable. |
| Right-align numeric columns in output | Both archive counts and related-location shared counts are right-aligned for readability, matching the functional spec's mockup output. |
| `--include archived` rejected for survey | Archives are always visible on the outward side by design. The flag would be confusing — it would imply archives aren't shown by default, when they are (in the archive section). |

## Non-Goals

- Filter support (`--where`) — Story 3
- Affinity computation — Story 3
- `--other`, `--brief` modes — Story 4
- Detail views — Story 5
- Location cap, `--archive`, `--verbose` — Story 6
- Any writes or mutations

## Test Plan

### Existing Tests (Must Pass)

- All domain/survey.rs tests from Story 1
- All existing tests (cargo test) — survey adds no modifications to existing code

### New Tests

Tests assert on `SurveyResult` / `SurveyOutcome` values from `compute_survey()`, not on formatted output strings. Formatting is simple enough to verify by inspection.

**Integration test setup** (in survey.rs `#[cfg(test)]` module):

```rust
use crate::repo::db::open_in_memory_for_test;

fn insert_root(conn: &Connection, path: &str, role: &str) -> i64 {
    conn.execute(
        "INSERT INTO roots (path, role, suspended) VALUES (?, ?, 0)",
        params![path, role],
    ).unwrap();
    conn.last_insert_rowid()
}

fn insert_object(conn: &Connection, hash: &str) -> i64 {
    conn.execute(
        "INSERT INTO objects (hash_type, hash_value, excluded) VALUES ('sha256', ?, 0)",
        params![hash],
    ).unwrap();
    conn.last_insert_rowid()
}

fn insert_source(
    conn: &Connection,
    root_id: i64,
    rel_path: &str,
    object_id: Option<i64>,
) -> i64 {
    conn.execute(
        "INSERT INTO sources (root_id, rel_path, object_id, size, mtime, \
         partial_hash, scanned_at, last_seen_at, device, inode) \
         VALUES (?, ?, ?, 1000, 1704067200, 'ph', 0, 0, 1, ?)",
        params![root_id, rel_path, object_id,
                conn.last_insert_rowid() + 1000],  // unique inode
    ).unwrap();
    conn.last_insert_rowid()
}
```

**Test scenarios:**

1. **Basic summary end-to-end**: 2 source roots, 1 archive root, mix of shared/archived/unique/unhashed. Assert: total=5, unhashed=1, hashed=4, archived=2, archive scope="/archive/photos/2024" with count 2, one related location "/mnt/backup/vacation" with shared=2, unique=1.

    ```
    Source root A (/mnt/drive-a):
      photos/IMG_001.jpg  → object 1 (also on root B, archived)
      photos/IMG_002.jpg  → object 2 (also on root B)
      photos/IMG_003.jpg  → object 3 (archived only)
      photos/IMG_004.jpg  → object 4 (unique — nowhere else)
      photos/IMG_005.jpg  → None (unhashed)

    Source root B (/mnt/backup):
      vacation/IMG_001.jpg → object 1
      vacation/IMG_002.jpg → object 2

    Archive root (/archive/photos):
      2024/IMG_001.jpg → object 1
      2024/IMG_003.jpg → object 3
    ```

2. **Empty selection**: Scope path matches no sources. Assert `SurveyOutcome::Empty`.

3. **All unhashed**: Sources exist but none hashed. Assert `SurveyOutcome::AllUnhashed` with correct `total_count`.

4. **No related locations**: Selection content exists only in the selection and archives — no overlap on other source roots. Assert `location_results` is empty. Combined with unique count equaling hashed count.

5. **No archived sources**: Selection content has no archive copies. Assert `archived_source_count == 0` and `archive_scopes` is empty.

6. **Multiple scope paths**: Two scope paths, union semantics. Assert selection includes sources from both paths.

7. **Suspended root excluded**: Source root is suspended. Its sources don't appear in selection or outward view (not in overlap, not in object index).

8. **Excluded sources hidden by default**: Excluded sources not in selection, not in object index. With `--include excluded`, they appear in selection but object index still excludes them.

9. **Archive scope grouping**: Archive sources in multiple directories across archive roots. Assert scope discovery groups them correctly and paths are sorted alphabetically.

10. **Same root, different scope**: Selection scoped to `/mnt/drive/photos`, overlap source at `/mnt/drive/documents`. Assert it appears as a related location — the user's own root can contain overlap outside their scope.

## Implementation Checklist

- [x] Phase 1: CLI wiring — `Survey` variant, `mod survey`, dispatch in main.rs
- [x] Phase 2: Selection pipeline — `compute_survey()` with `SurveyResult`/`SurveyOutcome`, object index, `run()` with formatting
- [x] Phase 3: Archive status — archive counting, scope discovery on archives, alphabetical sort
- [x] Phase 4: Overlap — overlap collection, scope discovery, per-location shared counts
- [x] Phase 5: Unique count, output formatting with right-aligned columns, early exits
- [x] Integration tests (tests 1-10)
- [x] Verify all existing tests pass (`cargo test`)
- [x] Verify clippy passes (`cargo clippy`)

## Backward Compatibility

No existing behavior changes. New command only.

## Performance Considerations

Survey loads all sources into memory via `batch_fetch_by_roots` — consistent with `ls`, `coverage`, and `compare`. The object index adds a second in-memory structure (HashMap of references into the same data). For very large libraries, this doubles the indexing overhead but doesn't double memory (references, not copies).

No per-location DB queries in Story 2 (no `--where` filter passes). The only DB access is the initial bulk fetch.
