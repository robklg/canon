# Story: Offline Path Resolution

**Design Spec**: `~/store/claude-designs/2026-02-28-offline-path-resolution.md`
**Status**: Done
**Created**: 2026-02-28

## Objective

Canon's source-querying commands (`ls`, `facts`, `coverage`, `worklist`, `compare`, `cluster generate`, `exclude`, `roots`) currently require referenced storage to be physically attached, because every path argument goes through `fs::canonicalize()` which calls POSIX `realpath(3)`. This fails when any path component doesn't exist on the filesystem.

The information needed to resolve these paths is already in the database. When a user scanned `/Volumes/OldBackup`, that canonical path was stored in the `roots` table. To match a user-provided path against it, we only need to make the path absolute and clean it lexically — no disk access required.

This story introduces **soft path resolution**: clean the path lexically, match against known root paths in the database, and fall back to `fs::canonicalize()` only when no root matches. This enables the natural workflow of scanning storage, detaching it, and exploring sources at your desk.

The key safety invariant: **every resolved path is either validated against a known root in the database OR validated by the OS via `fs::canonicalize()`.** Never unvalidated.

## Functional Requirements Summary

Four user stories from the design spec, all sharing one mechanism:

1. **Soft path resolution for scope paths** — `canon ls /Volumes/share/photos` works when the NAS is offline, if the path matches a known root. Applies to all source-querying commands.
2. **Root spec resolution** — `canon coverage --archive path:/Volumes/archive` works offline. `canon roots suspend path:/Volumes/drive` works offline.
3. **Archive auto-detection for `ls`** — `canon ls /Volumes/archive/photos` auto-includes archived sources even when the archive is offline.
4. **`cluster generate --dest`** — `canon cluster generate --dest /Volumes/archive/organized ...` works when the archive is offline.

**Acceptance criteria** (key items):
- When a cleaned path matches a known root, the command succeeds without filesystem access
- When a path matches no root AND `fs::canonicalize()` fails, the error message hints that storage may not be attached
- When storage is online, behavior is identical to today (soft resolution produces the same result for known roots)
- Soft resolution matches against ALL roots including suspended (path recognition is separate from operational filtering)
- `scan` and `apply` (file-accessing commands) are unchanged — they continue using `canonicalize_scopes()`

## Current State

### Path resolution functions

| Function | Location | Does | Filesystem I/O |
|----------|----------|------|----------------|
| `canonicalize_scopes(paths)` | `domain/path.rs` | Wraps `fs::canonicalize` for multiple paths | Yes — hard fails offline |
| `canonicalize_scope(path)` | `domain/path.rs` | Wraps `fs::canonicalize` for one path | Yes — hard fails offline |
| `canonicalize_maybe_missing(path)` | `domain/path.rs` | Walks up to find existing ancestor | Yes — needs some ancestor to exist |
| `parse_root_spec_impl(roots, spec, ...)` | `domain/root.rs` | Resolves `id:N` or `path:/foo` to root ID | Yes for `ByPath` variant |
| `resolve_root_path_impl(roots, path, ...)` | `domain/root.rs` | Finds which root contains a path | Yes — `fs::canonicalize` |
| `resolve_archive_path(roots, path)` | `domain/root.rs` | Finds archive root for a dest path | Yes — `canonicalize_maybe_missing` |
| `find_containing_root(path, roots)` | `domain/root.rs` | Pure: matches path against root list | **No** — already pure |

### Call sites that break offline

| Command | Function | Location |
|---------|----------|----------|
| `ls` | `canonicalize_scopes` | `ls.rs:47`, `ls.rs:357` |
| `facts` | `canonicalize_scopes` | `facts.rs:110` |
| `facts delete` | `canonicalize_scopes` | `facts.rs:958` |
| `coverage` | `canonicalize_scopes` + `parse_root_spec` | `coverage.rs:80`, `coverage.rs:88` |
| `worklist` | `canonicalize_scopes` | `worklist.rs:77` |
| `compare` | `canonicalize_scope` | `compare.rs:31-32` |
| `exclude set` | `canonicalize_scopes` | `exclude.rs:48` |
| `exclude clear` | `canonicalize_scopes` | `exclude.rs:124` |
| `exclude set_by_path` | `fs::canonicalize` | `exclude.rs:350` |
| `exclude set_object_by_file` | `fs::canonicalize` | `exclude.rs:582` |
| `exclude duplicates` | `canonicalize_scopes` | `exclude.rs:652` |
| `cluster generate` (source scope) | `canonicalize_scopes` | `cluster.rs:220` |
| `cluster generate` (--dest) | `resolve_archive_path` | `cluster.rs:217` |
| `roots list` (scope) | `fs::canonicalize` | `roots.rs:17` |
| `roots suspend/unsuspend/comment/rm` | `parse_root_spec` / `parse_root_spec_any` | `roots.rs` |
| `main.rs` (ls auto-detect) | `resolve_root_path` | `main.rs:629,637` |

### What already works offline

- `id:N` root specs — pure lookup, no filesystem I/O
- `cluster refresh` — reads scope from manifest as pre-canonicalized strings
- `ScopeMatch::classify_all()` — defaults to `UnderDirectory` when `is_file()` returns false; correct for offline

## Design

### Phase 1: Core functions — `clean_path` and `resolve_path`

- **Goal**: Add the new path resolution functions with full test coverage

#### Changes

**`domain/path.rs`** — Three new functions:

```rust
/// Clean a path lexically: make absolute (relative to cwd), resolve `.` and `..`
/// without filesystem access. Does NOT resolve symlinks or normalize case.
pub fn clean_path(path: &Path, cwd: &Path) -> PathBuf {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    };
    let mut components = Vec::new();
    for component in absolute.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                // Don't pop past the root
                if components.last().map_or(false, |c| matches!(c, Component::Normal(_))) {
                    components.pop();
                }
            }
            other => components.push(other),
        }
    }
    components.iter().collect()
}

/// Resolve a single path against known roots, falling back to fs::canonicalize().
/// Use for source-querying commands. Use canonicalize_scope()/canonicalize_scopes()
/// for commands that access files on disk (scan, apply).
pub fn resolve_path(path: &Path, roots: &[Root], cwd: &Path) -> Result<String> {
    let cleaned = clean_path(path, cwd);
    let cleaned_str = cleaned.to_string_lossy();

    // Try matching against known roots first (works offline)
    if find_containing_root(&cleaned_str, roots).is_some() {
        return Ok(cleaned_str.into_owned());
    }

    // Fall back to fs::canonicalize (requires path to exist on disk)
    match fs::canonicalize(path) {
        Ok(canonical) => Ok(canonical.to_string_lossy().into_owned()),
        Err(_) => bail!(
            "Failed to resolve path: {}\n\
             Path does not match any known root and is not accessible \
             on disk (is the storage attached?)",
            path.display()
        ),
    }
}

/// Resolve multiple paths against known roots.
pub fn resolve_paths(paths: &[PathBuf], roots: &[Root]) -> Result<Vec<String>> {
    let cwd = std::env::current_dir()
        .context("Failed to determine current directory")?;
    paths.iter().map(|p| resolve_path(p, roots, &cwd)).collect()
}
```

Note: `resolve_path` takes `cwd` as a parameter for testability. `resolve_paths` gets `cwd` once for efficiency. `find_containing_root` is called with all roots (including suspended) since path recognition is separate from operational filtering.

The `fs::canonicalize` fallback uses the original `path`, not the cleaned path — `fs::canonicalize` handles relative paths itself, and we want its full symlink resolution.

#### Tests

In `domain/path.rs`:

**`clean_path` tests** (all pure, no filesystem):
- `clean_absolute_no_dots` — `/a/b/c` with any cwd → `/a/b/c`
- `clean_relative_joins_cwd` — `b/c` with cwd `/a` → `/a/b/c`
- `clean_dotdot` — `/a/b/../c` → `/a/c`
- `clean_dot` — `/a/./b/c` → `/a/b/c`
- `clean_multiple_dotdot` — `/a/b/c/../../d` → `/a/d`
- `clean_dotdot_past_root` — `/a/../../b` → `/b`
- `clean_relative_with_dotdot` — `../b` with cwd `/a/c` → `/a/b`
- `clean_trailing_slash` — `/a/b/` → `/a/b`
- `clean_just_root` — `/` → `/`
- `clean_empty_relative` — `""` with cwd `/a/b` → `/a/b`

**`resolve_path` tests** (using fake `Root` structs, no DB):
- `resolve_matches_known_root_exact` — path = root path → Ok
- `resolve_matches_under_root` — path under root → Ok
- `resolve_relative_matches_root` — relative path, when joined with cwd, falls under root → Ok
- `resolve_dotdot_matches_root` — path with `..`, when cleaned, falls under root → Ok
- `resolve_no_match_returns_error` — path `/nonexistent/path` with no matching root → Err with "not accessible on disk" message
- `resolve_suspended_root_still_matches` — suspended root at `/a/b`, path `/a/b/c` → Ok (path recognition ignores suspension)

### Phase 2: Root resolution functions

- **Goal**: Make `parse_root_spec`, `resolve_root_path`, and `resolve_archive_path` use soft resolution

#### Changes

**`domain/root.rs`** — `parse_root_spec_impl()`:

Replace the `ByPath` branch. Currently:
```rust
RootSpec::ByPath(ref path) => {
    let canonical = fs::canonicalize(&path)
        .with_context(|| format!("Failed to resolve path: {path}"))?;
    let path_str = canonical.to_string_lossy();
    candidates.iter()
        .find(|r| r.path == *path_str)
        .map(|r| r.id)
        .ok_or_else(|| ...)
}
```

After:
```rust
RootSpec::ByPath(ref path) => {
    let cwd = std::env::current_dir()
        .context("Failed to determine current directory")?;
    // Resolve against ALL roots (including suspended) for path recognition
    let canonical = resolve_path(Path::new(path), roots, &cwd)?;
    // Find among filtered candidates (respects suspension filter)
    candidates.iter()
        .find(|r| r.path == canonical)
        .map(|r| r.id)
        .ok_or_else(|| ...)
}
```

Key subtlety: `resolve_path` receives `roots` (all roots, the unfiltered parameter), not `candidates` (filtered by suspension). This ensures path recognition works for any known root, while the operational filter still applies for the final root lookup.

**`domain/root.rs`** — `resolve_root_path_impl()`:

Replace `fs::canonicalize(path)` with `resolve_path(path, roots, &cwd)`. The full root list is used for resolution; candidates (filtered by suspension) are used for the subsequent `find_containing_root` call.

**`domain/root.rs`** — `resolve_archive_path()`:

Replace `canonicalize_maybe_missing(path)` with soft resolution + `canonicalize_maybe_missing` fallback:
```rust
let cwd = std::env::current_dir()
    .context("Failed to determine current directory")?;
let cleaned = clean_path(path, &cwd);
let cleaned_str = cleaned.to_string_lossy();

let path_str = if find_containing_root(&cleaned_str, roots).is_some() {
    cleaned_str.into_owned()
} else {
    // Fallback: canonicalize (tolerating non-existent subdirectories)
    canonicalize_maybe_missing(path)?
};
```

Note: `resolve_path` is NOT used here because its fallback is `fs::canonicalize` (requires full path to exist), while `resolve_archive_path` needs `canonicalize_maybe_missing` (tolerates non-existent leaf directories).

#### Tests

In `domain/root.rs`:
- `parse_root_spec_by_path_matches_root` — `path:/a/b` with root at `/a/b` → Ok(root_id)
- `parse_root_spec_by_path_no_match` — `path:/nonexistent` with no matching root → Err
- `parse_root_spec_by_path_role_filter` — `path:/a/b` with root at `/a/b` (source role), required_role=archive → Err (wrong role)
- `parse_root_spec_by_path_suspended_excluded` — `path:/a/b` with suspended root → Err (path resolves but root filtered out)
- `parse_root_spec_any_by_path_suspended_included` — `path:/a/b` with suspended root, using `_any` variant → Ok

### Phase 3: Command module migration

- **Goal**: Replace `canonicalize_scopes` with `resolve_paths` in all source-querying commands

#### Changes

Each command needs:
1. Fetch roots with `repo::root::fetch_all(conn)` before scope resolution (if not already available)
2. Replace `canonicalize_scopes(scope_paths)` with `resolve_paths(scope_paths, &all_roots)`

**`ls.rs`**:
- `run()`: Add `roots: &[Root]` parameter. Replace `canonicalize_scopes(scope_paths)` at line 47 with `resolve_paths(scope_paths, roots)`.
- `show_duplicates()`: Same — add `roots` parameter, replace at line 357.
- `main.rs`: Pass `&all_roots` (already fetched at line 623) to both `ls::run()` and `ls::show_duplicates()`.

**`facts.rs`**:
- `run()`: Add `let all_roots = repo::root::fetch_all(conn)?;` before line 110. Replace `canonicalize_scopes(scope_paths)` with `resolve_paths(scope_paths, &all_roots)`.
- `delete_facts()`: Same pattern.

**`coverage.rs`**:
- `run()`: Move `let roots = repo::root::fetch_all(conn)?;` (currently line 84) to before line 80. Replace `canonicalize_scopes(scope_paths)` with `resolve_paths(scope_paths, &roots)`.

**`worklist.rs`**:
- `run()`: Add `let all_roots = repo::root::fetch_all(conn)?;` before line 77. Replace `canonicalize_scopes(scope_paths)` with `resolve_paths(scope_paths, &all_roots)`.

**`compare.rs`**:
- `run()`: Add `let all_roots = repo::root::fetch_all(conn)?;`. Replace both `canonicalize_scope(Some(path_a/b))` calls with `resolve_path(path, &all_roots, &cwd)`. Get cwd once. Adjust the `Option` → direct `Result` handling (no more `Some`/`None` from `canonicalize_scope`).

**`exclude.rs`**:
- `set()`: Add root fetch before line 48, replace `canonicalize_scopes`.
- `clear()`: Add root fetch before line 124, replace `canonicalize_scopes`.
- `set_by_path()`: Reorder — move `repo::root::fetch_all` (currently line 357) before line 350. Replace `fs::canonicalize(file_path)` with `resolve_path(file_path, &roots, &cwd)`.
- `set_object_by_file()`: Same reorder and replacement.
- `exclude_duplicates()`: Add root fetch, replace `canonicalize_scopes`.

**`roots.rs`**:
- `list()`: Reorder — move `repo::root::fetch_all` (currently line 26) before line 17. Replace `fs::canonicalize(p)` with `resolve_path(p, &all_roots, &cwd)`.

**`cluster.rs`**:
- `generate()`: Roots already fetched at line 214. Replace `canonicalize_scopes(scope_paths)` at line 220 with `resolve_paths(scope_paths, &all_roots)`.

**Commands NOT changed**:
- `scan.rs` — file-accessing, needs `fs::canonicalize`
- `apply.rs` — file-accessing, needs `canonicalize_scopes` for source paths. `parse_root_spec` for `--root` gets soft resolution from Phase 2 automatically.

#### Tests

Existing command-level tests must pass unchanged. No new tests needed for this phase — the resolution logic is tested in Phase 1-2, and command tests exercise the full pipeline.

### Phase 4: Documentation and cleanup

- **Goal**: Update CLAUDE.md and user docs

#### Changes

**CLAUDE.md** updates:
- Add `clean_path()` to the "Shared Utilities" `domain/path.rs` section
- Add `resolve_path()` and `resolve_paths()` to the same section, with doc comment explaining when to use vs `canonicalize_scopes()`
- Note in the Path Handling Principle section that source-querying commands use `resolve_paths` (soft, works offline) while file-accessing commands use `canonicalize_scopes` (hard, requires disk)

**`docs/src/concepts/roots.md`** update:
- Add a brief section noting that query commands work with offline roots — Canon resolves paths against the database, so you can explore, check coverage, and generate manifests without the storage being attached.

#### Tests

`cargo clippy` — ensure no warnings from new imports or unused code.

## Architectural Decisions

| Decision | Rationale |
|----------|-----------|
| `clean_path` takes `cwd` as parameter | Keeps the function pure and testable without mocking `current_dir()` |
| `resolve_path` matches against ALL roots (including suspended) | Path recognition is separate from operational filtering. Suspension controls visibility in operations, not whether canon recognizes a path. |
| `resolve_archive_path` uses `clean_path` + `canonicalize_maybe_missing` fallback (not `resolve_path`) | `resolve_path`'s fallback is `fs::canonicalize` which requires the full path to exist. Archive dest paths may have non-existent subdirs. |
| `parse_root_spec` gets soft resolution universally (including for `apply --root`) | Simpler API surface. `apply` will still fail at file operations if root is offline — that's the right place to fail. |
| Each command fetches its own roots (not passed from `main.rs`) | Follows existing patterns, minimizes function signature changes. Exception: `ls` receives roots from `main.rs` since they're already fetched there. |
| New functions live in `domain/path.rs` alongside existing impure functions | `canonicalize_scopes` already lives here despite doing I/O. Pragmatic placement — avoids a new module for two functions. |

## Non-Goals

- **Changing `scan` or `apply`**: File-accessing commands stay with `fs::canonicalize`
- **Case-insensitive matching**: Soft resolution does exact string comparison. Case mismatches fall back to `fs::canonicalize` (which handles them on macOS). If offline, the user gets an error. Acceptable.
- **Symlink-aware soft resolution**: Not needed. Symlinked paths fall back to `fs::canonicalize`.
- **Removing `canonicalize_scopes`**: It's still used by `scan` and `apply`.
- **Windows path handling**: Canon doesn't target Windows currently.

## Test Plan

### Existing Tests (Must Pass)

- `domain/path.rs` — 11 tests (`path_is_under`, `path_strip_prefix`)
- `domain/root.rs` — ~30 tests (`RootSpec::parse`, `find_containing_root`, Root predicates, `parse_root_spec` with `id:N`)
- All command-level test modules (`ls`, `coverage`, `exclude`, `cluster`, `facts`, `scan`)

### New Tests

**Phase 1** (in `domain/path.rs`):
- 10 `clean_path` unit tests (pure, no filesystem)
- 6 `resolve_path` unit tests (fake Root structs, no DB)

**Phase 2** (in `domain/root.rs`):
- 5 `parse_root_spec` ByPath tests (soft resolution against fake roots)

## Implementation Checklist

- [x] Phase 1: Add `clean_path`, `resolve_path`, `resolve_paths` with tests
- [x] Phase 2: Modify `parse_root_spec_impl`, `resolve_root_path_impl`, `resolve_archive_path` to use soft resolution
- [x] Phase 3: Replace `canonicalize_scopes` with `resolve_paths` in all source-querying commands
- [x] Phase 4: Update CLAUDE.md and `docs/src/concepts/roots.md`
- [x] Verify all existing tests pass
- [x] `cargo clippy` clean

## Documentation Updates

- **CLAUDE.md**: New entries in "Shared Utilities" for `clean_path`, `resolve_path`, `resolve_paths`. Updated "Path Handling Principle" noting the two-strategy distinction.
- **`docs/src/concepts/roots.md`**: Brief section on offline query support.

## Backward Compatibility

No breaking changes. All existing behavior is preserved when storage is online. The only change is that commands succeed in strictly more situations than before.

When a path matches no known root AND the filesystem is inaccessible, the error message changes from:
```
Failed to resolve path: /Volumes/share/castor-import
No such file or directory (os error 2)
```
to:
```
Failed to resolve path: /Volumes/share/castor-import
Path does not match any known root and is not accessible on disk (is the storage attached?)
```

## Implementation Remarks

Deviations and observations from implementation:

1. **`canonicalize_scope` and `canonicalize_scopes` are fully unused.** The spec's Non-Goals section states "Removing `canonicalize_scopes`: It's still used by `scan` and `apply`", but neither `scan.rs` nor `apply.rs` actually imports or calls these functions. They were only used by the source-querying commands that were migrated. Both functions are retained with `#[allow(dead_code)]` annotations per the non-goal.

2. **`exclude_duplicates` had additional `fs::canonicalize` calls not in the call site table.** The `exclude_duplicates()` function (line 402) used bare `fs::canonicalize` for both `scope_path` and `prefer_path` (lines 420, 426), in addition to the `canonicalize_scopes` call at line 652 (which is actually in `set_objects_by_filter`). All three were migrated to soft resolution.

3. **Spec line numbers in the call site table were approximate.** The `exclude.rs:652` entry labeled "exclude duplicates" was actually `set_objects_by_filter()`. Line numbers had drifted from the spec's snapshot. All call sites were found and migrated regardless.

4. **`compare.rs` restructured slightly.** The spec said to adjust `Option` → direct `Result` handling. The original code used `canonicalize_scope(Some(path))` returning `Option<String>` with `let Some(ref prefix) = ...` destructuring. The new code calls `resolve_path()` directly, returning `String`, which simplified the control flow (no more `Some`/`None` handling or `bail!` for missing paths — `resolve_path` handles the error itself).

5. **`exclude.rs` `Context` import removed.** After replacing `fs::canonicalize(...).with_context(...)` calls in `set_by_path` and `set_object_by_file`, the `Context` trait import became unused and was removed.

6. **`roots.rs` `std::fs` import removed.** After replacing `fs::canonicalize` with `resolve_path`, the `std::fs` import was no longer needed and was removed.

7. **Clippy fix in `clean_path`.** The spec's code used `map_or(false, |c| ...)` which clippy flagged as `unnecessary_map_or`. Changed to `is_some_and(|c| ...)`.

## Performance Considerations

- `clean_path` is pure string manipulation — negligible cost
- `find_containing_root` iterates the root list (typically <20 roots) — negligible
- `repo::root::fetch_all()` is called once per command, returns a handful of rows — negligible
- Commands that already fetch roots (like `coverage`) may fetch slightly earlier, but same total cost
- When storage is online, the `fs::canonicalize` fallback is avoided for paths that match known roots (micro-optimization — `fs::canonicalize` does multiple `lstat()` syscalls)
