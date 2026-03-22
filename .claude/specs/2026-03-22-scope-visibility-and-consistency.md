# Story: Scope Visibility and CWD Consistency

**Design Spec**: `~/store/claude-designs/2026-03-22-scope-visibility-and-consistency.md`
**Status**: Pending
**Created**: 2026-03-22

## Objective

Canon's scope handling is inconsistent across commands, causing real confusion during archiving sessions. Discovery commands should all follow the same mental model: CWD is an intentional context switch — Canon follows where the user is and confirms it. This work unifies CWD defaulting, adds scope display to all discovery commands, adds a `--global` escape flag, errors on explicit non-root paths, and brings compare into the CWD model.

The product intent: "The interface should create awareness, not anxiety." Silent wrong results undermine trust. The user should always know what scope Canon is operating on.

## Functional Requirements Summary

**Story 1 — CWD defaulting**: All discovery commands (ls, survey, facts, coverage, worklist) follow the same logic when no paths given: CWD in root → scope to CWD; CWD not in root → global silently. When CWD is in an archive root, auto-include archived sources. Compare gains one-path mode: CWD as side A, argument as side B. Effectful commands unchanged.

**Story 2 — `--global` flag**: Available on ls, survey, facts, coverage, worklist. Skips CWD defaulting. Silently ignored when explicit paths present. Not on compare (needs a path) or effectful commands.

**Story 3 — Scope display**: Report commands (survey, facts, coverage, compare) show scope on stdout — natural language (e.g., `Facts: /path` or `Facts: all roots`), always shown including global. List commands (ls, worklist) show `scope: /path` on stderr only when scoped; silent when global.

**Story 4 — Non-root error**: Explicit path not under any known root → error. CWD-not-in-root → silent global fallback (not an error). Applies to all scope-taking commands.

**Story 5 — Relative paths**: List/data commands show relative paths when CWD-defaulted; absolute when global or explicitly scoped.

## Current State

**CWD defaulting** lives in two places:
- `main.rs:661-682` — ls: smart root check via `domain::resolve_root_path()`, sets `use_relative` and `auto_include_archived`
- `survey.rs:103-108` — unconditional `vec![std::env::current_dir()?]`

**Scope display** exists in:
- Survey: `println!("Survey: {}", scope_prefixes[0])` on stdout (always)
- Coverage: conditional `println!("Scope: {s}\n")` on stdout (only when single scope prefix)
- Compare: `writeln!(handle, "  A: {prefix_a}")` on stdout (always, both paths)
- Others: none

**Path resolution**: `domain::path::resolve_paths()` does soft resolution (match known roots first, `fs::canonicalize` fallback). `domain::root::find_containing_root()` checks if a resolved path is under any root (pure, checks all roots including suspended). `domain::path::format_path()` handles relative/absolute display.

**Ops boundary**: `SelectionParams.scopes: Vec<ScopeMatch>` — empty = global. Clean boundary, no changes needed.

## Design

### Phase 1: Shared Infrastructure and First Two Commands (ls, survey)

- **Goal**: Create the shared scope resolution infrastructure, wire ls and survey through it, add `--global` flag
- **Scope**: New `src/scope.rs`, domain validation function, ls and survey converted

#### Changes

**New `domain/path.rs` function — `validate_paths_in_roots()`:**

```rust
/// Verify that all resolved paths are under a known root.
/// Returns error on the first path not under any root.
/// Uses find_containing_root() which checks all roots (including suspended),
/// preserving offline root support — paths matching known roots pass
/// regardless of whether the root's disk is mounted.
pub fn validate_paths_in_roots(paths: &[String], roots: &[Root]) -> Result<()> {
    for path in paths {
        if find_containing_root(path, roots).is_none() {
            bail!("{path} is not under any known root");
        }
    }
    Ok(())
}
```

Note on offline roots: `resolve_paths()` resolves against known roots first (soft resolution, no disk access). Paths matching a known root — even an unmounted/suspended one — resolve successfully and pass `validate_paths_in_roots()`. The validation only catches paths that resolved via `fs::canonicalize()` fallback (exist on disk but aren't under any root).

**New `src/scope.rs` — shared scope resolution:**

```rust
/// Result of resolving scope for a discovery command.
pub struct ResolvedScope {
    /// Resolved scope path strings (empty = global)
    pub prefixes: Vec<String>,
    /// Whether scope came from CWD defaulting (controls relative path display)
    pub from_cwd: bool,
    /// Whether to auto-include archived sources (CWD or explicit path in archive root)
    pub auto_include_archived: bool,
}

impl ResolvedScope {
    /// True when operating globally (no scope restriction)
    pub fn is_global(&self) -> bool {
        self.prefixes.is_empty()
    }
}

/// Resolve scope for a discovery command.
///
/// Resolution order:
/// 1. Explicit paths given → resolve, validate in roots, return
/// 2. --global flag → return empty (global)
/// 3. No paths, no --global → try CWD:
///    - CWD in a known active root → scope to CWD
///    - CWD not in any root → global fallback (silent)
///    - current_dir() fails → global fallback (silent)
///
/// When CWD or explicit path is inside an archive root, sets auto_include_archived.
pub fn resolve_scope(
    explicit_paths: &[PathBuf],
    global: bool,
    roots: &[Root],
) -> Result<ResolvedScope>
```

**Scope display functions in `src/scope.rs`:**

```rust
/// Print scope header for report commands on stdout.
/// Always prints — shows "Label: /path" when scoped, "Label: all roots" when global.
/// For multiple paths: "Label: /path1, /path2" (or one-per-line if > 2).
pub fn print_report_scope(handle: &mut impl Write, label: &str, scope: &ResolvedScope)

/// Print scope header for list/data commands on stderr.
/// Only prints when scoped — "scope: /path". Silent when global.
pub fn print_list_scope(scope: &ResolvedScope)
```

**Clap changes** — add `--global` to ls, survey, facts, coverage, worklist:

```rust
/// Show results across all roots, ignoring current directory scope
#[arg(long)]
global: bool,
```

**main.rs ls dispatch** — replace ad-hoc CWD logic (lines 661-682) with:

```rust
let scope = scope::resolve_scope(&paths, global, &all_roots)?;
if scope.auto_include_archived {
    include.archived = true;
}
// ... pass scope.prefixes to ScopeMatch, scope.from_cwd as use_relative ...
```

**ls.rs** — add `scope::print_list_scope(&scope)` before output.

**survey.rs** — remove CWD defaulting (lines 103-108), receive pre-resolved scope from main.rs. Update header to use `scope::print_report_scope()`, which handles the "all roots" case.

#### Tests

**`domain::path::validate_paths_in_roots()`:**
- Path under an active root → Ok
- Path under a suspended root → Ok (still "known")
- Path not under any root → Error with path in message
- Multiple paths, all valid → Ok
- Multiple paths, second invalid → Error on second path
- Empty paths list → Ok

**`scope::resolve_scope()`:**
- Explicit paths → prefixes populated, `from_cwd: false`
- `--global` with no paths → empty (global), `from_cwd: false`
- `--global` with explicit paths → explicit paths used, `from_cwd: false`
- No paths, CWD in source root → CWD scope, `from_cwd: true`, `auto_include_archived: false`
- No paths, CWD in archive root → CWD scope, `from_cwd: true`, `auto_include_archived: true`
- No paths, CWD not in any root → empty (global), `from_cwd: false`
- Explicit path in archive root → `auto_include_archived: true`, `from_cwd: false`
- Explicit path not in any root → Error

Note: `resolve_scope()` takes `&[Root]` and calls `resolve_root_path()` / `resolve_paths()`, which need CWD access. Tests should use controlled root lists and may need to test the core logic with extracted pure helper functions rather than mocking CWD.

### Phase 2: Remaining Discovery Commands (facts, coverage, worklist)

- **Goal**: Wire remaining commands through shared infrastructure, add scope display
- **Scope**: facts, coverage, worklist gain CWD defaulting, `--global`, scope display

#### Changes

**main.rs** — facts, coverage, worklist dispatch each gains:

```rust
let scope = scope::resolve_scope(&paths, global, &all_roots)?;
if scope.auto_include_archived {
    include.archived = true;
}
```

**facts.rs** — add `scope::print_report_scope(handle, "Facts", &scope)` as first output line, before the "Sources matching filters" line.

**coverage.rs** — replace conditional `Scope: {s}` display with `scope::print_report_scope(handle, "Coverage", &scope)`. Remove the `scope_display` variable and conditional logic in `display_scoped_stats()`. Coverage always shows scope now, including "all roots" for the per-root breakdown mode.

**worklist.rs** — add `scope::print_list_scope(&scope)` on stderr before JSONL output.

#### Tests

- Facts with CWD in root shows `Facts: /path` header
- Facts with `--global` shows `Facts: all roots` header
- Coverage scoped shows `Coverage: /path` header
- Coverage global shows `Coverage: all roots` header
- Worklist scoped shows `scope: /path` on stderr
- Worklist global shows no scope header

### Phase 3: Compare One-Path Mode and Non-Root Errors

- **Goal**: Compare gains CWD as side A, non-root error applied uniformly
- **Scope**: Compare CLI change, compare CWD logic, non-root error on effectful commands

#### Changes

**Clap change for compare** — accept 1 or 2 positional paths:

```rust
/// Paths to compare (1 path: CWD vs path, 2 paths: path A vs path B)
#[arg(required = true, num_args = 1..=2)]
paths: Vec<PathBuf>,
```

**main.rs compare dispatch** — when 1 path given, resolve CWD as side A:

```rust
let (path_a, path_b) = if paths.len() == 2 {
    (paths[0].clone(), paths[1].clone())
} else {
    // One path: CWD as side A
    let cwd = std::env::current_dir()?;
    // For compare, CWD-not-in-root is an ERROR (user intended CWD as comparison side)
    let cwd_str = domain::path::clean_path(&cwd, &cwd).to_string_lossy().into_owned();
    if domain::root::find_containing_root(&cwd_str, &all_roots).is_none() {
        bail!("Current directory is not under any known root");
    }
    (cwd, paths[0].clone())
};
```

**Non-root error on effectful commands** — `exclude` and `cluster generate` already take explicit paths. After their `resolve_paths()` call, add `validate_paths_in_roots()`:

```rust
let scope_prefixes = resolve_paths(scope_paths, &all_roots)?;
domain::path::validate_paths_in_roots(&scope_prefixes, &all_roots)?;
```

This is a small addition to each effectful command's path handling — not via `resolve_scope()` (which is for discovery commands with CWD defaulting) but directly using the domain validation function.

#### Tests

- Compare with 1 path: CWD becomes side A
- Compare with 2 paths: unchanged behavior
- Compare with 0 paths: error
- Compare with 1 path, CWD not in root: error "current directory is not under any known root"
- `ls /tmp/existing-but-not-scanned` → error
- `survey /tmp/existing-but-not-scanned` → error
- `exclude set /tmp/not-a-root` → error
- `cluster generate /tmp/not-a-root` → error

### Phase 4: Documentation and Polish

- **Goal**: Update all documentation, clean up dead code
- **Scope**: CLAUDE.md, user docs, removed dead code

#### Changes

**CLAUDE.md updates:**
- Add `scope.rs` to the Shared Utilities section — `resolve_scope()`, `ResolvedScope`, scope display functions
- Document the discovery-vs-effectful distinction: discovery commands default to CWD, effectful commands require explicit scope
- Document the report-vs-list scope display convention
- Add `--global` to CLI Flag Vocabulary section

**User documentation (`docs/src/`):**
- Update command reference pages for ls, survey, facts, coverage, worklist, compare with `--global` flag and scope display behavior
- Update any scope/path documentation to describe the unified CWD defaulting model

**Dead code removal:**
- Remove ad-hoc CWD logic from `main.rs` ls dispatch (replaced by `resolve_scope()`)
- Remove CWD defaulting from `survey.rs` (moved to `resolve_scope()`)
- Remove conditional `scope_display` logic from `coverage.rs` (replaced by shared display)

#### Tests

- Verify all existing tests pass after cleanup
- No new tests in this phase

## Architectural Decisions

| Decision | Rationale |
|----------|-----------|
| New `src/scope.rs` for shared infrastructure | Parallels `ceremony.rs` — shared interface-layer concerns. Not domain (involves CWD I/O), not ops (it's CLI context resolution). |
| `validate_paths_in_roots()` in domain layer | Pure function: paths + roots → result. No I/O. Reusable by both discovery and effectful commands. |
| Compare has its own CWD logic, not `resolve_scope()` | Compare's CWD-not-in-root is an error (user intended CWD as comparison side), unlike discovery commands where it's a silent global fallback. Different semantics require different handling. |
| Report scope always shows (including global) | Reports are self-contained documents. "Facts: all roots" is meaningful context when saved to a file. |
| List scope silent when global | Global is the unmarked state for pipeline output. No noise when there's nothing specific to report. |
| `--global` silently ignored with explicit paths | Explicit paths are the strongest signal of intent. No warnings for redundant flags. |
| Non-root check uses `find_containing_root()` (all roots including suspended) | Suspended roots are still "known." Offline/unmounted roots are still "known." Only paths truly outside Canon's world trigger the error. |

## Non-Goals

- No changes to effectful commands' scope resolution (exclude, cluster generate) beyond adding the non-root error
- No changes to the operations layer or `SelectionParams`
- No changes to `scan` command (requires path on disk, different model)
- No short flag for `--global` (defer to future if needed)
- No changes to `exclude duplicates` CWD behavior (borderline case, defer)

## Test Plan

### Existing Tests (Must Pass)

- All `domain::path` tests (clean_path, resolve_path, path_is_under, format_path)
- All `domain::root` tests (find_containing_root, resolve_root_path, predicates)
- All existing command tests

### New Tests

**Phase 1:**
- `validate_paths_in_roots`: 6 test cases (active root, suspended root, not in root, multiple valid, multiple with invalid, empty)
- `resolve_scope`: 8 test cases (explicit paths, --global variants, CWD in source/archive/no root, explicit path in archive, explicit not in root)

**Phase 2:**
- Scope display: 6 test cases (facts/coverage/worklist scoped and global headers)

**Phase 3:**
- Compare one-path: 4 test cases (1 path, 2 paths, 0 paths, 1 path CWD not in root)
- Non-root error: 4 test cases (ls, survey, exclude, cluster with non-root path)

## Implementation Checklist

- [x] Phase 1: Shared infrastructure (`ops/scope.rs`, `validate_paths_in_roots`), `--global` flag, ls + survey wired through
- [x] Phase 2: facts, coverage, worklist wired through, scope display on all
- [x] Phase 3: Compare one-path mode, non-root error on all commands (exclude + cluster)
- [x] Phase 4: CLAUDE.md updated, dead code cleanup (survey scope_prefixes removed)
- [x] Verify all existing tests pass (813)
- [x] Update CLAUDE.md with new scope infrastructure patterns
- [ ] User documentation updates in `docs/src/`

## Documentation Updates

- **CLAUDE.md**: Add `scope.rs` to Shared Utilities, document discovery-vs-effectful distinction, add `--global` to CLI Flag Vocabulary
- **`docs/src/commands/`** (or equivalent): Update ls, survey, facts, coverage, worklist, compare pages with `--global` flag and scope display
- **`docs/src/concepts/`** (or equivalent): Update any scope/path documentation with unified CWD model

## Backward Compatibility

- `facts` and `coverage` will now default to CWD scope when run from inside a root, where previously they were global. Users who relied on global behavior need `--global`. This aligns with the behavior already experienced with `ls` and `survey`.
- `survey` outside any root now falls back to global instead of trying to use CWD as scope. This is strictly better.
- `coverage` scope display format changes slightly (consistent header instead of conditional).
- Explicit paths not under any root now error instead of producing empty results. This prevents silent wrong results.
- Compare with one path is net-new behavior (previously required two).

## Performance Considerations

No performance implications. All new code is in the interface layer — CWD resolution, root matching, and string formatting. The operations layer and database access patterns are unchanged.
