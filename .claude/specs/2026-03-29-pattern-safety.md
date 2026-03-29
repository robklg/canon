# Story: Pattern Safety

**Design Spec**: [~/store/claude-designs/2026-03-29-apply-safety-and-recovery.md](~/store/claude-designs/2026-03-29-apply-safety-and-recovery.md) (Story 1)
**Epic**: [epic-apply-safety-and-recovery.md](epic-apply-safety-and-recovery.md)
**Status**: Complete
**Created**: 2026-03-29

## Objective

Pattern expansion can produce absolute paths that cause `PathBuf::join()` to discard the archive base directory, scattering files to arbitrary filesystem locations. This is a live data-safety bug discovered in real use — the pattern `{source.rel_path[:-1]}/{filename}` on flat files resolves to `/filename`, and `base_dir.join("/filename")` yields `/filename` instead of `base_dir/filename`.

Two layered fixes: normalize pattern results to clean relative paths (primary), and validate that resolved destination paths stay under the archive root (safety net).

## Functional Requirements Summary

- Pattern expansion results are normalized: leading `/` stripped, `//` collapsed, `./` and `.` removed
- Pattern results containing `..` are already sanitized (replaced with `_` by existing code)
- After normalization, destination paths are verified to be under the archive root
- Escaped paths are reported as plan violations (collected, not fail-fast)
- Normalization happens at the expression layer so all consumers get clean paths
- Valid patterns are unaffected — only degenerate cases are cleaned up

## Current State

**`expr/eval.rs::evaluate()`** (line ~766): Builds a result string from literal and expression segments. Existing path sanitization replaces `..` with `_` and `\0` with `_`. Returns the raw concatenated string — no normalization of path separators or leading `/`.

**`expr/eval.rs::apply_accessor()`** (line ~928): For slice accessors like `[:-1]`, returns empty string when the slice produces no segments (e.g., single-component path). This is correct accessor behavior — the empty string then participates in concatenation with literal segments.

**`ops/apply.rs::evaluate_pattern()`** (line ~168): Calls `expr::evaluate()`, returns the raw result.

**`ops/apply.rs::compute_archive_rel_path()`** (line ~181): Simple string concatenation: `format!("{base_dir_rel}/{dest_rel}")`. No normalization.

**`ops/apply.rs::execute_single_transfer()`** (line ~692): Calls `base_dir.join(&transfer.dest_rel_path)`. This is where `PathBuf::join()` discards the base when the argument starts with `/`.

**`ops/apply.rs::plan_apply()`**: Collects violations (expansion failures, collisions, stale records, conflicts). No violation type for escaped paths.

**`domain::path::path_is_under()`**: Lexical prefix check with proper boundary handling. Already exists, well-tested.

## Design

### Phase 1: Pattern Result Normalization

- **Goal**: All pattern expansion results are clean relative paths
- **Scope**: `expr/eval.rs`

#### Changes

**New function in `expr/eval.rs`**:

```rust
/// Normalize a pattern result to a clean relative path.
/// Strips leading '/', collapses '//' to '/', removes '.' components.
/// The existing '..' sanitization (replaced with '_') runs before this.
fn normalize_pattern_result(path: &str) -> String {
    path.split('/')
        .filter(|s| !s.is_empty() && *s != ".")
        .collect::<Vec<_>>()
        .join("/")
}
```

**Modified in `evaluate()`**: Call `normalize_pattern_result()` on the final result, after the existing `..` and `\0` sanitization:

```rust
// Existing:
let sanitized = result.replace("..", "_").replace('\0', "_");
// New — add after sanitization:
let normalized = normalize_pattern_result(&sanitized);
normalized
```

This is the complete fix for the absolute-path bug. After normalization, `/5.avi` becomes `5.avi`, and `base_dir.join("5.avi")` works correctly.

#### Tests

**Unit tests for `normalize_pattern_result()`**:

| Input | Expected | Reason |
|-------|----------|--------|
| `"5.avi"` | `"5.avi"` | Normal — unchanged |
| `"subdir/file.jpg"` | `"subdir/file.jpg"` | Normal — unchanged |
| `"/5.avi"` | `"5.avi"` | Leading `/` stripped |
| `"//5.avi"` | `"5.avi"` | Multiple leading `/` stripped |
| `"subdir//file.jpg"` | `"subdir/file.jpg"` | Interior `//` collapsed |
| `"./subdir/file.jpg"` | `"subdir/file.jpg"` | Leading `./` removed |
| `"subdir/./file.jpg"` | `"subdir/file.jpg"` | Interior `.` removed |
| `""` | `""` | Empty — unchanged |
| `"/"` | `""` | Lone slash → empty |
| `"///"` | `""` | Multiple slashes → empty |
| `"a/b/c"` | `"a/b/c"` | Multi-level — unchanged |

**Integration test — the original bug**:

```rust
#[test]
fn test_pattern_flat_file_no_absolute_path() {
    // Source with single-component rel_path (flat file, no subdirectory)
    // Pattern: {source.rel_path[:-1]}/{filename}
    // rel_path = "5.avi", rel_path[:-1] = "" (empty)
    // Raw: "/5.avi" → Normalized: "5.avi"
    // Verify the evaluated result is "5.avi", not "/5.avi"
}
```

**Regression test — valid patterns unaffected**:

```rust
#[test]
fn test_pattern_subdirectory_file_unchanged() {
    // Source with multi-component rel_path
    // Pattern: {source.rel_path[:-1]}/{filename}
    // rel_path = "subdir/file.jpg", rel_path[:-1] = "subdir"
    // Result: "subdir/file.jpg" — unchanged by normalization
}
```

### Phase 2: Archive Root Validation

- **Goal**: Defense-in-depth — catch any destination path that escapes the archive root
- **Scope**: `ops/apply.rs`

#### Changes

**New violation field in `ApplyViolations`**:

```rust
pub struct ApplyViolations {
    // ... existing fields ...
    /// Destination paths that resolve outside the archive root.
    pub escaped_paths: Vec<(String, String)>,  // (source_path, resolved_dest)
}
```

**New validation in `plan_apply()`**, after building transfers:

```rust
// Fetch archive root path
let archive_root = repo::root::batch_fetch_by_ids(conn, &[config.output.archive_root_id])?;
let archive_root_path = archive_root.get(&config.output.archive_root_id)
    .ok_or_else(|| anyhow!("Archive root {} not found", config.output.archive_root_id))?
    .path.clone();

// Validate each transfer stays under archive root
for transfer in &transfers {
    let full_dest = format!("{}/{}", archive_root_path, transfer.archive_rel_path);
    if !domain::path::path_is_under(&full_dest, &archive_root_path) {
        violations.escaped_paths.push((
            transfer.source_path.clone(),
            full_dest,
        ));
    }
}
```

Note: With Phase 1's normalization in place, `escaped_paths` should never fire for the pattern bug — it's caught upstream. This validation catches edge cases normalization can't handle (e.g., symlink-based escapes if path components in the archive are symlinks, though this requires the archive root itself to contain symlinks — unusual but possible).

**Interface layer** (`apply.rs`): Display `escaped_paths` violations alongside existing violation types. Format matches the functional spec:

```
Preflight failed: 2 destination paths resolve outside the archive root.

  /Volumes/other/escape.jpg (from source: /Volumes/source/tricky_file.jpg)
  ... and 1 more

Check the pattern in your manifest.
```

#### Tests

**Unit test for archive-root validation**:

```rust
#[test]
fn test_plan_rejects_escaped_destination() {
    // Set up a manifest with archive_root_id pointing to /archive
    // Manually construct a transfer with archive_rel_path that would escape
    // (This requires bypassing normalization — e.g., a path with symlink component)
    // Verify violations.escaped_paths is populated
}
```

**Integration test — normalization prevents escape**:

```rust
#[test]
fn test_normalization_prevents_archive_escape() {
    // Use the pattern that caused the original bug
    // Verify no escaped_paths violations (normalization fixed it)
    // Verify the transfers have correct relative dest_rel_path
}
```

## Architectural Decisions

| Decision | Rationale |
|----------|-----------|
| Normalize in `evaluate()`, not in `plan_apply()` | All pattern consumers (apply, dry-run, confirmation samples, cluster summary) get normalized paths. No consumer can accidentally use raw results. |
| `normalize_pattern_result()` is a private function in `expr/eval.rs` | It's an implementation detail of pattern evaluation, not a general-purpose utility. |
| Archive-root check uses lexical `path_is_under()`, not `canonicalize()` | Destination doesn't exist yet — can't canonicalize. Archive root is already a canonical path (stored in DB from `fs::canonicalize` at scan time). |
| `escaped_paths` is a new violation type, not merged with expansion_failures | Different problem, different user action. Expansion failure = missing fact. Escaped path = pattern bug. |

## Non-Goals

- Changing how `apply_accessor()` handles empty slices (returning empty string is correct)
- Adding pattern validation at generate time (the pattern is user-editable in the manifest)
- Handling symlinks within the archive root (path_is_under is lexical — this is a known limitation, acceptable)

## Test Plan

### Existing Tests (Must Pass)

All existing pattern evaluation tests in `expr/eval.rs` test module. Normalization must not change any currently-passing test output — valid patterns produce the same results.

### New Tests

| Test | Type | Phase |
|------|------|-------|
| `normalize_pattern_result` unit tests (11 cases) | Unit | 1 |
| Pattern with flat file produces relative path | Integration | 1 |
| Pattern with subdirectory file unchanged | Regression | 1 |
| Plan rejects escaped destination path | Integration | 2 |
| Normalization prevents archive escape | Integration | 2 |

## Implementation Checklist

- [ ] Phase 1: Add `normalize_pattern_result()` in `expr/eval.rs`, call from `evaluate()`
- [ ] Phase 1: Unit tests for normalization function
- [ ] Phase 1: Integration test for flat-file pattern bug
- [ ] Phase 1: Verify all existing pattern tests pass
- [ ] Phase 2: Add `escaped_paths` field to `ApplyViolations`
- [ ] Phase 2: Add archive-root validation in `plan_apply()`
- [ ] Phase 2: Display escaped_paths violations in `apply.rs`
- [ ] Phase 2: Integration tests for validation
- [ ] Update CLAUDE.md if needed (pattern normalization convention)

## Documentation Updates

No user-facing documentation changes needed for this story. Pattern normalization is invisible to the user (patterns just work better). The archive-root validation error message is self-explanatory.

## Backward Compatibility

Pattern normalization only affects degenerate cases (leading `/`, `//`, `./`). Valid patterns that produce clean relative paths are completely unaffected. No user-visible behavior change for working manifests.

## Performance Considerations

Negligible. `normalize_pattern_result()` is a simple string split+filter+join, called once per source during plan phase. Archive-root validation is a string prefix check per transfer. Both are trivial compared to the existing pattern expansion and fact fetching.
