# Story: DB-Only Scope Resolution

**Design Spec**: `~/store/claude-designs/2026-03-29-scope-resolution.md`
**Status**: Done
**Created**: 2026-03-29

## Objective

Complete the Sources-vs-Files principle for scope handling. When a user provides a scope path to any Canon command, Canon should verify it knows that place via its sources database — not the filesystem. Typos, unquoted paths, and unknown subdirectories should error immediately rather than producing confusing empty results.

This removes `warn_nonexistent_scope_paths()` (a filesystem-based warning applied to only 2 commands) and replaces it with a DB-based source-existence check applied structurally to all scope-taking commands via `resolve_scope()`.

## Functional Requirements Summary

**Story 1**: `resolve_scope()` errors when an explicit path is under a root but has no sources (any `present` value) at or under it. CWD defaulting and `--global` are exempt. Error message: "no sources known at \<path\>".

**Story 2**: Compare validates both paths for source existence.

**Story 3**: Effectful commands (cluster generate, exclude) validate source existence after `resolve_paths()`.

**Story 4**: Delete `warn_nonexistent_scope_paths()` and all call sites.

## Current State

### ops/scope.rs

`resolve_scope(explicit_paths, global, roots)` — normalizes paths, validates root membership via `validate_paths_in_roots()`, handles CWD defaulting. No DB access (no `conn` parameter). No source-existence check.

### domain/path.rs

`warn_nonexistent_scope_paths(paths, roots)` — filesystem-based warning. Checks `Path::new(root_path).exists()` and `Path::new(path).exists()`. Called from survey.rs (lines 96, 117) and note.rs (lines 144, 168). Domain layer doing I/O — violates architecture.

### Call patterns

- **Discovery commands** (ls, facts, coverage, worklist, survey): `resolve_scope()` in main.rs
- **Effectful commands** (cluster, exclude): `resolve_paths()` + `validate_paths_in_roots()` directly
- **Compare**: `resolve_path()` per side, no validation
- **Note**: `resolve_scope()` in module + `warn_nonexistent_scope_paths()`
- **Survey**: `resolve_scope()` in main + `warn_nonexistent_scope_paths()` for main scope and `--other`

## Design

### Phase 1: Repo Query

- **Goal**: Add source-existence check function to repo layer

#### Changes

**`repo/source.rs`** — new function:

```rust
/// Check if any sources (current or historical) exist at or under a scope path.
/// Includes present=0 records — Canon once knew this place.
/// Returns true if at least one source record exists.
pub fn sources_exist_at_scope(conn: &Connection, root_id: i64, rel_path: &str) -> Result<bool> {
    let exists: bool = if rel_path.is_empty() {
        conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sources WHERE root_id = ?)",
            rusqlite::params![root_id],
            |row| row.get(0),
        )?
    } else {
        conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sources WHERE root_id = ? \
             AND (rel_path = ? OR rel_path LIKE ? || '/%'))",
            rusqlite::params![root_id, rel_path, rel_path],
            |row| row.get(0),
        )?
    };
    Ok(exists)
}
```

Note: the `rel_path` column stores paths relative to the root. The subtree pattern (`rel_path = ? OR rel_path LIKE ? || '/%'`) matches exact and descendant paths. No `present` filter — we want any record.

#### Tests

In `repo/source.rs`:

- `sources_exist_at_scope_with_present` — insert source with present=1, check returns true
- `sources_exist_at_scope_with_non_present` — insert source with present=0, check returns true
- `sources_exist_at_scope_no_sources` — no sources at path, returns false
- `sources_exist_at_scope_descendant` — source at "a/b/c", scope "a" → true
- `sources_exist_at_scope_no_false_prefix` — source at "ab/c", scope "a" → false
- `sources_exist_at_scope_root_level` — empty rel_path, root has sources → true
- `sources_exist_at_scope_root_level_empty` — empty rel_path, root has no sources → false

### Phase 2: Ops Pipeline + Remove Filesystem Check

- **Goal**: Integrate source-existence into resolve_scope(), add standalone helper, remove warn function

#### Changes

**`ops/scope.rs`** — signature change + source-existence check:

```rust
use crate::repo::{self, Connection};

pub fn resolve_scope(
    conn: &Connection,           // NEW
    explicit_paths: &[PathBuf],
    global: bool,
    roots: &[Root],
) -> Result<ResolvedScope> {
    // Case 1: Explicit paths given
    if !explicit_paths.is_empty() {
        let prefixes = resolve_paths(explicit_paths, roots)?;
        validate_paths_in_roots(&prefixes, roots)?;
        validate_sources_exist(conn, &prefixes, roots)?;  // NEW
        // ... rest unchanged
    }
    // Cases 2 & 3 unchanged — no source-existence check for global or CWD
}
```

**`ops/scope.rs`** — new standalone validation function:

```rust
/// Validate that sources exist at each scope path.
/// Errors on the first path with no known sources.
/// Skips root-level paths (empty rel_path) — roots are always valid.
/// Assumes paths are already validated as under known roots.
pub fn validate_sources_exist(
    conn: &Connection,
    paths: &[String],
    roots: &[Root],
) -> Result<()> {
    for path in paths {
        if let Some((root_id, _root_path, _role, rel_path)) =
            domain::root::find_containing_root(path, roots)
        {
            if !rel_path.is_empty()
                && !repo::source::sources_exist_at_scope(conn, root_id, &rel_path)?
            {
                bail!("no sources known at {path}");
            }
        }
    }
    Ok(())
}
```

**`domain/path.rs`** — delete `warn_nonexistent_scope_paths()` (lines 96-114).

**`survey.rs`** — remove both `warn_nonexistent_scope_paths()` calls:
- Line 96 (main scope): remove — already covered by `resolve_scope()` in main.rs
- Lines 116-117 (`--other` paths): replace with `ops::scope::validate_sources_exist()`

**`note.rs`** — remove both `warn_nonexistent_scope_paths()` calls (lines 144, 168) and the import. The check is now in `resolve_scope()`.

#### Tests

In `ops/scope.rs` (these need a test DB — use `setup_test_db()`):

- `resolve_scope_errors_on_unknown_subpath` — root at "/photos", no sources at "/photos/typo" → error containing "no sources known"
- `resolve_scope_accepts_path_with_sources` — root at "/photos", sources at "/photos/2011" → ok
- `validate_sources_exist_errors_on_unknown` — unknown path → error
- `validate_sources_exist_accepts_root_level` — root-level path (empty rel_path) → ok without needing sources

### Phase 3: Wire All Callers + Remove Double Resolution

- **Goal**: Update all resolve_scope() callers, add checks to compare and effectful outliers, eliminate redundant resolve_paths() + validate_paths_in_roots() calls in command modules

#### The double-resolution problem

Currently, main.rs resolves scope via `resolve_scope()` (which normalizes + validates), converts `resolved.prefixes: Vec<String>` to `Vec<PathBuf>`, passes to command modules, and then command modules call `resolve_paths()` + `validate_paths_in_roots()` again. This is:
- Redundant (paths already resolved and validated)
- Confusing (looks like the command module is responsible for validation, but it's not)
- A `String → PathBuf → String` round-trip for no reason

The fix: command modules accept `&[String]` (pre-resolved prefixes) instead of `&[PathBuf]` and go directly to `ScopeMatch::classify_all()`. Main.rs passes `&resolved.prefixes` directly.

#### Changes

**`main.rs`** — update all `resolve_scope()` calls (9 sites) to pass `db.conn()`, and pass `&resolved.prefixes` directly as `&[String]` instead of converting to `Vec<PathBuf>`:

```rust
// Before:
let resolved = ops::scope::resolve_scope(&paths, global, &all_roots)?;
let scope_paths: Vec<PathBuf> = resolved.prefixes.iter().map(PathBuf::from).collect();
command::run(&scope_paths, ...)?;

// After:
let resolved = ops::scope::resolve_scope(db.conn(), &paths, global, &all_roots)?;
command::run(&resolved.prefixes, ...)?;
```

Remove all `Vec<PathBuf>` conversions of `resolved.prefixes` at call sites: worklist, ls, facts, coverage, survey, cluster generate, exclude set/clear/set-object.

**Command modules** — change scope parameter from `&[PathBuf]` to `&[String]` and remove redundant resolution:

Each of these modules changes:
- `ls.rs` `run()` and `show_duplicates()`: `scope_paths: &[String]`, remove `resolve_paths()` call, remove `roots` parameter (no longer needed for resolution)
- `facts.rs` `run()` and `delete_facts()`: same pattern
- `coverage.rs` `run()`: same pattern
- `worklist.rs` `run()`: same pattern
- `survey.rs` `run()`: same pattern for main scope (note: `--other` paths still need their own resolution — see below)
- `cluster.rs` `generate()`: same pattern, also remove `validate_paths_in_roots()` call
- `exclude.rs` `set()`, `clear()`, `set_objects_by_filter()`: same pattern, also remove `validate_paths_in_roots()` calls

Before (each command):
```rust
pub fn run(db: &mut Db, scope_paths: &[PathBuf], roots: &[Root], ...) -> Result<()> {
    let scope_prefixes = resolve_paths(scope_paths, roots)?;
    // validate_paths_in_roots(&scope_prefixes, roots)?;  // effectful commands only
    let scopes = ScopeMatch::classify_all(&scope_prefixes);
    ...
}
```

After:
```rust
pub fn run(db: &mut Db, scope_prefixes: &[String], ...) -> Result<()> {
    let scopes = ScopeMatch::classify_all(scope_prefixes);
    ...
}
```

Where `roots` was only used for `resolve_paths()`, remove the parameter entirely.

**`note.rs`** — update `resolve_single_scope()` and `resolve_single_scope_optional()` to pass `conn`:

```rust
let resolved = resolve_scope(conn, &paths, global, &all_roots)?;
```

Remove `warn_nonexistent_scope_paths()` calls (already handled in Phase 2).

**`survey.rs`** — main scope now arrives pre-resolved. But `--other` paths are resolved inside survey.rs. These still need `resolve_paths()` + the new `validate_sources_exist()`:

```rust
let resolved = domain::path::resolve_paths(&options.other_paths, &all_roots)?;
ops::scope::validate_sources_exist(conn, &resolved, &all_roots)?;
```

**`compare.rs`** — add source-existence check after its own path resolution:

```rust
let prefix_a = resolve_path(path_a, &all_roots, &cwd)?;
let prefix_b = resolve_path(path_b, &all_roots, &cwd)?;
ops::scope::validate_sources_exist(conn, &[prefix_a.clone(), prefix_b.clone()], &all_roots)?;
```

Compare validates both sides — the CWD-with-zero-sources edge case is theoretical.

**`exclude.rs`** — `exclude_duplicates()` bypasses `resolve_scope()` in main.rs, so add `validate_sources_exist()` after its own `validate_paths_in_roots()`. The other exclude variants (`set`, `clear`, `set_objects_by_filter`) lose their redundant `resolve_paths()` + `validate_paths_in_roots()` since they now receive pre-resolved prefixes.

`set_by_path` and `set_object_by_file` take exact file paths — source existence is validated by the subsequent source lookup.

**`cluster.rs`** — `generate()` loses its redundant `resolve_paths()` + `validate_paths_in_roots()` since it receives pre-resolved prefixes.

#### Tests

No new tests — the behavior change (error on unknown scope) is verified by Phase 1 and 2 tests. The double-resolution removal is a refactor that existing tests verify (all commands still work with valid scopes).

#### Tests

No new tests — the behavior change is verified by Phase 1 and 2 tests. Existing command tests continue to pass (they use valid scopes).

### Phase 4: Documentation

- **Goal**: Update CLAUDE.md and user docs

#### Changes

**CLAUDE.md**:
- Update `ops/scope.rs` entry: `resolve_scope()` now takes `&Connection`, add `validate_sources_exist()`
- Update `repo/source.rs` entry: add `sources_exist_at_scope()`
- Update path handling principle to note that scope resolution now includes source-existence checking (DB-only, no filesystem)
- Remove mention of `warn_nonexistent_scope_paths()` if present

**`docs/`**: No page changes needed — no new commands or flags. The error message change ("no sources known at \<path\>") is a behavioral improvement, not a documented interface change.

#### Tests

`cargo clippy` — ensure no warnings from removed function or changed imports.

## Architectural Decisions

| Decision | Rationale |
|----------|-----------|
| Source-existence check inside `resolve_scope()` | Structural enforcement — all commands using `resolve_scope()` get the check automatically. Prevents the inconsistency that plagued `warn_nonexistent_scope_paths()`. |
| `validate_sources_exist()` as standalone function | Commands that don't use `resolve_scope()` (compare, effectful commands) need the same check. Shared function avoids duplication. |
| `bail!` not typed errors | `validate_paths_in_roots()` uses the same pattern. Distinct error messages achieve the same UX. Typed errors can be added later if programmatic error handling is needed. |
| `conn` parameter on `resolve_scope()` | The function needs DB access for the source-existence query. This is the right thing — scope resolution is a behavioral operation that needs the database. |
| Skip root-level paths | If a root exists in the DB, Canon has scanned it. No need to verify source existence at the root level. |
| Validate both sides in compare | The CWD-in-root-with-zero-sources edge case is theoretical. Simplicity wins. |
| No `present` filter in query | `present=0` means Canon knew this place. The scope is valid even if all files are gone. |
| Eliminate double resolution | Command modules that receive scope through `resolve_scope()` should not re-resolve. Pass pre-resolved `&[String]` prefixes instead of `&[PathBuf]`. Removes confusing redundancy and the `String → PathBuf → String` round-trip. |

## Non-Goals

- Not introducing typed error enums (can be added later)
- Not adding source-existence checks to `scan` or `apply` (file-accessing commands — they need the filesystem)

## Test Plan

### Existing Tests (Must Pass)

- `ops/scope.rs` — 5 tests
- `domain/path.rs` — tests for resolve_path, clean_path, validate_paths_in_roots
- `repo/source.rs` — all source tests
- All command-level tests

### New Tests

**Phase 1** (repo): 7 tests for `sources_exist_at_scope()`
**Phase 2** (ops): 4 tests for `resolve_scope()` with DB + `validate_sources_exist()`

## Implementation Checklist

- [ ] Phase 1: Repo query + 7 tests
- [ ] Phase 2: Ops pipeline change + remove warn function + 4 tests
- [ ] Phase 3: Wire all callers (main.rs, note.rs, compare.rs, exclude.rs, cluster.rs)
- [ ] Phase 4: CLAUDE.md update
- [ ] Verify all existing tests pass
- [ ] `cargo clippy` clean

## Documentation Updates

- **CLAUDE.md**: Updated function signatures and entries for ops/scope.rs and repo/source.rs
- **`docs/`**: No changes needed — behavioral improvement, not interface change

## Backward Compatibility

Commands that previously returned empty results for unknown scopes will now error with "no sources known at \<path\>". This is intentional — the previous behavior was a bug that hid user mistakes behind plausible-looking empty output.

## Performance Considerations

- `SELECT EXISTS(SELECT 1 FROM sources WHERE ... LIMIT 1)` is O(1) with the existing index on `(root_id, rel_path)` — negligible cost
- Called once per explicit scope path at command start — not in any hot path
- No performance regression for valid scopes
