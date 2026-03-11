# Story: Survey Release Polish

**Design Spec**: Based on real-use session findings (~/store/claude-vision/sessions/2026-03-10-survey-real-use.md)
**Status**: Pending
**Created**: 2026-03-11

## Objective

Remove the rough edges identified during first real use of survey before release. These five fixes address trust-critical issues (silent wrong results from unquoted paths), blocked workflows (can't find matched pairs, can't mark deleted folders as missing), and output polish (adaptive caps, focused `--other` output). Together they make survey reliable enough for daily use — which is the prerequisite for gathering more real-world experience to inform future development.

## Functional Requirements Summary

1. **Path existence warning**: Warn when positional scope arguments resolve to paths that don't exist on disk, preventing silent wrong results from unquoted `--other` paths.
2. **Scan `--missing` flag**: Let users mark sources under a deleted path as not-present without rescanning the entire root.
3. **Adaptive detail view cap**: Show all paths when the count is small; cap at 5 only when output would be overwhelming.
4. **`--other` suppresses archive section**: When the user focuses on a specific location with `--other`, hide the full archive breakdown.
5. **Overlap matched pairs**: Show counterpart paths at the other location alongside selection-side paths in `--detail overlap`.

## Current State

- Survey is fully implemented with orientation default, `--affinity` opt-in, four detail views (complement, unique, overlap, residual), `--other` directed comparison, and `--archive` filtering.
- `scan.rs` skips non-existent paths with a warning — no way to mark sources missing without walking the parent.
- `domain::path::resolve_path()` does soft resolution against known roots (offline-capable).
- `repo::source::fetch_source_ids_for_root()` supports prefix filtering. `repo::source::mark_missing()` handles batch marking.
- The `by_object_id` index in survey already contains all sources by object — counterpart paths are discoverable but not currently extracted.

## Design

### Phase 1: Adaptive Detail View Cap

- **Goal**: Show all paths when the count is small; cap only for large results.

#### Changes

In `survey.rs`:

- Replace `COMPLEMENT_SAMPLE_SIZE = 5` with two constants:
  ```rust
  const DETAIL_SAMPLE_SIZE: usize = 5;
  const DETAIL_SHOW_ALL_THRESHOLD: usize = 20;
  ```
- Update the display logic in `print_overlap_detail`, `print_complement_detail`, and `print_residual_detail` to use adaptive behavior:
  ```rust
  // Before:
  let show_count = if verbose {
      paths.len()
  } else {
      COMPLEMENT_SAMPLE_SIZE.min(paths.len())
  };

  // After:
  let show_count = if verbose || paths.len() <= DETAIL_SHOW_ALL_THRESHOLD {
      paths.len()
  } else {
      DETAIL_SAMPLE_SIZE.min(paths.len())
  };
  ```
- Update the truncation notice similarly:
  ```rust
  // Before:
  if !verbose && paths.len() > COMPLEMENT_SAMPLE_SIZE {

  // After:
  if !verbose && paths.len() > DETAIL_SHOW_ALL_THRESHOLD {
  ```
- `--verbose` still overrides to show all regardless.

#### Tests

- Test with 5 paths: all shown (below threshold)
- Test with 20 paths: all shown (at threshold)
- Test with 21 paths: capped at 5, truncation notice
- Test with `--verbose` and 50 paths: all shown

### Phase 2: `--other` Suppresses Archive Section

- **Goal**: When `--other` is active, skip the archive section in summary output.

#### Changes

In `survey.rs`, in the summary output flow (around line 287), wrap the `print_archive_section` call:

```rust
// Before:
print_archive_section(...);

// After:
if !result.is_other_mode {
    print_archive_section(...);
}
```

No changes to computation — archive data is still computed (it feeds unique count via the object index). Only display is affected.

#### Tests

- Summary without `--other`: archive section present
- Summary with `--other`: archive section absent
- `--other` with `--detail` modes: no effect (detail views don't show archive section anyway)
- Verify unique count is still correct when `--other` is active (computation unchanged)

### Phase 3: Path Existence Warning

- **Goal**: Warn when scope paths don't exist on disk, catching stray words from unquoted paths.

#### Changes

Add a new function in `domain/path.rs`:

```rust
/// Warn on stderr for resolved paths that are under an accessible root
/// but don't exist on disk. This catches stray words from unquoted
/// shell arguments (e.g., `--other /path/with spaces` without quotes).
///
/// Only warns when the root itself is accessible — disconnected storage
/// is handled separately and should not trigger warnings.
pub fn warn_nonexistent_scope_paths(paths: &[String], roots: &[Root]) {
    for path in paths {
        if let Some(root) = find_containing_root(path, roots) {
            // Root exists in DB. Is the root itself accessible?
            if Path::new(&root.path).exists() {
                // Root is mounted. Does the specific subpath exist?
                if !Path::new(path).exists() {
                    eprintln!(
                        "Warning: path does not exist on disk: {}",
                        path
                    );
                }
            }
        }
    }
}
```

Call from `survey.rs` after `resolve_paths()`:

```rust
let scope_prefixes = domain::path::resolve_paths(&scope_paths, all_roots)?;
domain::path::warn_nonexistent_scope_paths(&scope_prefixes, all_roots);
```

This function is intentionally in `domain::path` alongside `resolve_path()` — both deal with path resolution and validation. The I/O is minimal (existence checks only) and matches the existing pattern in that module.

Scope: survey only for this story. Other commands can adopt later.

#### Tests

- Path under accessible root, subpath doesn't exist → warning emitted
- Path under accessible root, subpath exists → no warning
- Path under inaccessible root (root path doesn't exist) → no warning
- Path not under any root → no warning (resolve_path would have errored already)

Note: unit tests use fake root structs with paths that don't exist on disk, so the "root accessible" check will be false in most unit tests. Integration testing of the warning output is best done via a command-level test or manual verification.

### Phase 4: Scan `--missing` Flag

- **Goal**: Let users mark sources under a deleted path as not-present without walking the filesystem.

#### Changes

**CLI** (`main.rs` or `scan.rs` arg parsing):

Add `--missing` flag to the scan command. Mutually exclusive with `--all` and `--add`:

```rust
/// Mark all sources under this path as not present (for deleted folders)
#[arg(long)]
missing: bool,
```

Validation at the start of `run()`:
```rust
if missing && all_roots {
    bail!("--missing cannot be used with --all");
}
if missing && add_root {
    bail!("--missing cannot be used with --add");
}
```

**Command flow** (`scan.rs`):

In the per-path loop, when `fs::canonicalize` fails and `--missing` is set:

```rust
let canonical = match fs::canonicalize(path) {
    Ok(p) => p,
    Err(e) => {
        if missing {
            // User explicitly wants to mark this path's sources as missing
            mark_missing_path(conn, path, &roots, now, &mut total_stats)?;
            continue;
        }
        eprintln!("Warning: skipping {}: {}", path.display(), e);
        continue;
    }
};
```

New function `mark_missing_path`:

```rust
fn mark_missing_path(
    conn: &Connection,
    path: &Path,
    roots: &[domain::Root],
    now: i64,
    stats: &mut ScanStats,
) -> Result<()> {
    // Soft-resolve against known roots
    let cwd = std::env::current_dir()?;
    let cleaned = domain::path::clean_path(path, &cwd);
    let cleaned_str = cleaned.to_string_lossy();

    let (root_id, rel_prefix) = match domain::root::find_containing_root(&cleaned_str, roots) {
        Some(root) => {
            let prefix = domain::path::path_strip_prefix(&cleaned_str, &root.path)
                .unwrap_or("")
                .to_string();
            (root.id, prefix)
        }
        None => {
            bail!(
                "Cannot mark missing: {} is not under any known root",
                path.display()
            );
        }
    };

    // Fetch present source IDs under this prefix
    let prefix_arg = if rel_prefix.is_empty() {
        None
    } else {
        Some(rel_prefix.as_str())
    };
    let source_ids = repo::source::fetch_source_ids_for_root(conn, root_id, prefix_arg)?;

    if source_ids.is_empty() {
        eprintln!("No present sources found under {}", path.display());
        return Ok(());
    }

    // Mark them missing
    let marked = repo::source::mark_missing(conn, &source_ids, now)?;
    stats.missing += marked;

    Ok(())
}
```

When `--missing` is set and the path *does* exist on disk (canonicalize succeeds), the existing scan behavior runs unchanged — `--missing` is only relevant when the path doesn't exist. This avoids any interaction with the normal walk-and-reconcile flow.

#### Tests

All tests use `open_in_memory_for_test()` with the production schema.

1. **Path doesn't exist, `--missing`, resolves to root**: Insert 5 sources under root `/photos` with prefix `vacation/`. Call `mark_missing_path` with path `/photos/vacation`. Verify all 5 sources have `present=0`. Verify stats show 5 missing.

2. **Path doesn't exist, no `--missing`**: Existing behavior — warning printed, sources unchanged. (Existing test coverage, verify not broken.)

3. **Path doesn't exist, `--missing`, not under any root**: Error: "not under any known root."

4. **Prefix matches subset**: Insert sources under `vacation/` and `work/`. Mark missing with prefix `vacation/`. Verify only vacation sources marked, work sources untouched.

5. **Sources already not-present**: Insert sources, mark not-present manually, then call `mark_missing_path`. Verify `marked` count is 0 (idempotent — `mark_missing` only updates `present=1` rows).

6. **Empty prefix (root-level)**: `--missing` with a path that equals the root itself. All sources in root marked missing. (This is the `rel_prefix.is_empty()` → `prefix_arg = None` path.)

7. **`--missing` with `--all`**: Error at validation.

8. **`--missing` with `--add`**: Error at validation.

9. **Path exists on disk, `--missing`**: Normal scan behavior runs (canonicalize succeeds, enters existing walk flow). `--missing` has no effect. Verify sources are reconciled normally.

### Phase 5: Overlap Matched Pairs

- **Goal**: Show counterpart paths at the other location alongside selection-side paths in `--detail overlap`.

#### Changes

**New struct** in `survey.rs`:

```rust
struct OverlapPair {
    /// Path of the selection-side source (absolute).
    selection_path: String,
    /// Paths at the other location with matching content (relative to location).
    counterpart_paths: Vec<String>,
}
```

**Change `LocationResult`**:

```rust
// Before:
overlap_paths: Option<Vec<String>>,

// After:
overlap_pairs: Option<Vec<OverlapPair>>,
```

**Data collection** (lines 561-571 in `compute_survey`):

```rust
// Before:
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

// After:
let overlap_pairs = if options.detail == Some(DetailMode::Overlap) {
    let mut pairs: Vec<OverlapPair> = hashed
        .iter()
        .filter(|s| loc_oids.contains(&s.object_id.unwrap()))
        .map(|s| {
            let oid = s.object_id.unwrap();
            // Find counterpart paths at this location
            let counterpart_paths: Vec<String> = by_object_id
                .get(&oid)
                .map(|sources| {
                    sources
                        .iter()
                        .filter(|cs| cs.matches_scope(&loc_scope))
                        .filter(|cs| !sel_source_ids.contains(&cs.id))
                        .map(|cs| {
                            // Path relative to location
                            domain::path::path_strip_prefix(&cs.path(), scope_path)
                                .unwrap_or_else(|| &cs.path())
                                .to_string()
                        })
                        .collect()
                })
                .unwrap_or_default();
            let mut counterpart_paths = counterpart_paths;
            counterpart_paths.sort_unstable();
            OverlapPair {
                selection_path: s.path(),
                counterpart_paths,
            }
        })
        .collect();
    pairs.sort_by(|a, b| a.selection_path.cmp(&b.selection_path));
    Some(pairs)
} else {
    None
};
```

**Display** (`print_overlap_detail`):

Update the function signature to accept `Vec<OverlapPair>` instead of `Vec<String>`.

Human-readable output:

```rust
for pair in &pairs[..show_count] {
    println!("    {}", domain::path::format_path(&pair.selection_path, cwd));
    for cp in &pair.counterpart_paths {
        println!("      \u{2192} {}", cp);  // → arrow
    }
}
```

Output example:

```
Overlapping with related locations (overlap):

  /mnt/backup/phone-export/ (4 of 135 overlap):
    recordings/morning-walk.m4a
      → audio/2020/morning-walk.m4a
    recordings/evening-notes.m4a
      → audio/misc/recording-001.mp3
    photos/IMG_0042.JPG
      → DCIM/2020-W48/IMG_0042.JPG
      → DCIM/2020-W48/IMG_0042 2.JPG
```

Selection-side paths are relative to cwd (existing behavior via `format_path`). Counterpart paths are relative to the location.

**`-0` mode**: Unchanged — flat deduplicated selection-side paths only. Counterpart paths are not relevant for piping.

```rust
if null_delim {
    let all_paths: std::collections::BTreeSet<&str> = locations
        .iter()
        .filter_map(|loc| loc.overlap_pairs.as_ref())
        .flat_map(|pairs| pairs.iter().map(|p| p.selection_path.as_str()))
        .collect();
    for path in all_paths {
        print!("{}\0", path);
    }
    return;
}
```

#### Tests

1. **Single counterpart**: Selection source overlaps with one source at location → one `→` line shown.

2. **Multiple counterparts**: Same content exists twice at location (e.g., OS duplicate `IMG 2.JPG`) → two `→` lines shown.

3. **Counterpart path relative to location**: Verify paths are stripped of the location prefix.

4. **Counterpart path when stripping fails**: Source path doesn't start with location prefix (edge case) → falls back to full path.

5. **`-0` mode unchanged**: Verify null-delimited output contains only selection-side paths, no counterpart data.

6. **Adaptive cap applies to pairs**: With 21+ overlapping pairs, shows 5 pairs with counterparts, then truncation notice.

7. **Empty counterpart list**: Selection source has object_id matching a location's object set, but no actual sources at the location match scope (edge case from scope discovery precision) → pair shown with no `→` lines.

## Architectural Decisions

| Decision | Rationale |
|----------|-----------|
| Warning, not error, for non-existent scope paths | Path might be valid historical query against DB data. Don't block the command. |
| `--missing` as explicit flag, not automatic detection | Can't reliably distinguish deleted folders from dismounted storage within a root. User is the authority. |
| `--missing` only activates when path doesn't exist | When path exists, normal scan runs. No interaction between `--missing` and the walk-and-reconcile flow. |
| Adaptive cap uses two constants (threshold + sample) | Threshold (20) controls when to cap; sample (5) controls how many to show when capped. Independent tuning. |
| Counterpart paths relative to location | Consistent with complement view. Keeps output compact. Location header provides the absolute context. |
| `-0` mode excludes counterpart paths | Piping is for selection-side paths (e.g., `xargs -0 rm`). Counterpart paths are for human evaluation only. |
| `warn_nonexistent_scope_paths` scoped to survey only | Other commands can adopt later. Avoids cross-cutting changes in this story. |

## Non-Goals

- Archive section roll-up / `--detail archived` (identified in session, deferred)
- Zero-byte / trivial file default filtering (needs policy discussion)
- `ls` scope header on stderr (cross-cutting, not survey-specific)
- Date range filter syntax / BETWEEN operator (expression system change)
- Size information in survey output
- Location notes / journal

## Test Plan

### Existing Tests (Must Pass)

- All existing survey tests (overlap, complement, residual, unique detail views)
- All existing scan tests (reconciliation, mark_missing, device detection)
- All existing `domain::path` tests

### New Tests

See per-phase test sections above. Total: ~25 new tests across 5 phases.

## Implementation Checklist

- [x] Phase 1: Adaptive detail view cap (constants + display logic)
- [x] Phase 2: `--other` suppresses archive section (one conditional)
- [x] Phase 3: Path existence warning (new validation function in domain/path.rs)
- [x] Phase 4: Scan `--missing` flag (CLI flag, mark_missing_path function, validation)
- [x] Phase 5: Overlap matched pairs (OverlapPair struct, data collection, display)
- [x] Verify all existing tests pass (647 total, 10 new)
- [ ] Update CLAUDE.md if new patterns established

## Documentation Updates

- `docs/src/commands/query/survey.md`: Update overlap detail section to show counterpart paths in the example output. Note adaptive cap behavior (no `--verbose` needed for small results). Remove archive section from `--other` example if one is shown.
- `docs/src/commands/roots/scan.md`: Document `--missing` flag — purpose, usage, mutual exclusivity with `--all` and `--add`.

## Backward Compatibility

- **Phase 1**: Output change — users who parsed "... and N more" for small counts will see full output instead. Strictly more information.
- **Phase 2**: Output change — `--other` users lose the archive section. This is the desired behavior; the archive data is still computed and available without `--other`.
- **Phase 3**: New warnings on stderr — won't affect stdout piping. May surprise users who were unknowingly surveying wrong paths (which is the point).
- **Phase 4**: New flag only — no change to existing `scan` behavior.
- **Phase 5**: Output format change in `--detail overlap` — counterpart lines added below each path. `-0` mode unchanged.

## Performance Considerations

- **Phase 5**: Counterpart path collection adds lookups in `by_object_id` per overlapping source. This is a HashMap lookup (O(1)) followed by a small filter. Negligible cost — the index is already in memory and already built for other computations.
- **Phase 4**: `fetch_source_ids_for_root` with prefix is a single SQL query with LIKE. `mark_missing` uses batched updates (BATCH_SIZE=1000). Both are well-established patterns.
