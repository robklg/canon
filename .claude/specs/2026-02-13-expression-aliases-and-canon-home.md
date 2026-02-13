# Story: Expression Aliases & Canon Home Directory

**Status**: Complete
**Created**: 2026-02-13

## Objective

Introduce named expression aliases (`@name` syntax in `--where`) and a unified Canon home directory (`CANON_HOME`), reducing filter verbosity and consolidating all Canon state under a single configurable path.

## Functional Requirements Summary

**Story 1 — Canon Home Directory**: Replace `--db` with `--canon-home` flag and `CANON_HOME` env var. Precedence: flag > env > `~/.canon/`. Database lives at `$CANON_HOME/canon.db`, aliases at `$CANON_HOME/aliases.toml`. Remove the `--db` flag entirely.

**Story 2 — Expression Alias File**: Flat TOML key-value file at `$CANON_HOME/aliases.toml`. Alias names match `[a-zA-Z][a-zA-Z0-9_-]*`. Values are any valid `--where` expression (including boolean logic). File is only loaded when `@` references appear in filter strings. Missing file is not an error unless an `@alias` is actually used. TOML parse errors are fatal. No nested aliases.

**Story 3 — Alias Expansion in `--where`**: `@name` is replaced with `(value)` — parentheses always applied. Expansion happens before filter parsing (the filter engine never sees `@`). `@` inside quoted strings is literal. Works identically across all commands accepting `--where`. Error messages distinguish "no aliases file" from "alias not found".

**Story 4 — Alias Expansion in Manifests**: `cluster generate` stores the expanded expression in `meta.query`. The original (pre-expansion) expression is stored as a TOML comment `# Original: ...` above the query field. Manifests are self-contained — they work without the aliases file.

**Story 5 — Alias Management Command**: Explicitly deferred. Not part of this implementation.

## Current State

- `--db` flag is defined in `Cli` struct (`main.rs:30-31`), resolved in `main()` (`main.rs:477-482`)
- DB path defaults to `~/.canon/canon.db` via `dirs::home_dir()`
- No alias concept exists anywhere in the codebase
- Filter strings are parsed via `Filter::parse()` at 14 call sites across 8 modules: `ls.rs` (2), `cluster.rs` (2), `exclude.rs` (5), `worklist.rs` (1), `compare.rs` (1), `facts.rs` (2), `coverage.rs` (1)
- `toml` crate is already a dependency
- `expr/filter.rs` tokenizer handles `'` and `"` quoted strings (lines 416-430)
- Manifest generation in `cluster.rs` stores raw filter strings in `ManifestMeta.query`
- `expr/filter.rs` has no existing tests (pre-existing gap)

## Design

### Architecture

```
┌──────────────────────────────────────────────────────────────┐
│ main.rs                                                      │
│  - resolve_canon_home(flag) → PathBuf                       │
│  - --canon-home global flag                                  │
│  - For each command: expand filters, dispatch                │
├──────────────────────────────────────────────────────────────┤
│ alias.rs (command-layer utility)                             │
│  - load_aliases(canon_home) → Option<HashMap>               │
│  - expand_filter_strings(filters, canon_home) → Vec<String> │
├──────────────────────────────────────────────────────────────┤
│ expr/alias.rs (expression layer — pure)                      │
│  - expand_aliases(input, aliases, aliases_path) → String    │
│  - has_alias_references(input) → bool                       │
└──────────────────────────────────────────────────────────────┘
```

Key invariant: the filter engine (`expr/filter.rs`) is completely untouched. It never sees `@`. Expansion is pure string pre-processing.

### Phase 1: Canon Home Directory

- **Goal**: Replace `--db` with `--canon-home` / `CANON_HOME`.

#### Changes

**`src/main.rs`** — Cli struct:
- Remove `db: Option<PathBuf>` field
- Add `canon_home: Option<PathBuf>` field with `#[arg(long, global = true)]`

**`src/main.rs`** — new function:
```rust
fn resolve_canon_home(flag: Option<&Path>) -> Result<PathBuf> {
    if let Some(path) = flag {
        return Ok(path.to_path_buf());
    }
    if let Ok(val) = std::env::var("CANON_HOME") {
        return Ok(PathBuf::from(val));
    }
    let mut path = dirs::home_dir().context("Could not determine home directory")?;
    path.push(".canon");
    Ok(path)
}
```

**`src/main.rs`** — in `main()`, replace the db path resolution block:
```rust
let canon_home = resolve_canon_home(cli.canon_home.as_deref())?;
if canon_home.exists() && !canon_home.is_dir() {
    bail!("CANON_HOME path is not a directory: {}", canon_home.display());
}
let db_path = canon_home.join("canon.db");
```

The rest of `main()` is unchanged — `db_path` is still passed to `open_with_options()`.

#### Tests

None — mechanical CLI change. Verified by `cargo build` + existing test suite.

### Phase 2: Alias Expansion Engine + Tests

- **Goal**: Implement the pure expansion logic and all tests. Nothing calls it from the CLI yet.

#### Changes

**`src/expr/alias.rs`** — new file with:

`expand_aliases(input: &str, aliases: &HashMap<String, String>, aliases_path: &Path) -> Result<String>`:
- Character-by-character scan with quote tracking
- When `@` encountered outside quotes: read name (`[a-zA-Z][a-zA-Z0-9_-]*`), look up in map, replace with `(value)`
- Quote tracking mirrors `filter.rs` tokenizer: skip content between matching `'`/`"` pairs
- Error on invalid name syntax, unknown alias (with distinct messages for "no file" vs "not found")

`has_alias_references(input: &str) -> bool`:
- Fast-path check: scans for `@` outside quoted strings
- Returns false if no `@` found, avoiding file I/O for non-alias usage

**`src/expr/mod.rs`** — add `pub mod alias;`

**`src/alias.rs`** — new file with:

`load_aliases(canon_home: &Path) -> Result<Option<HashMap<String, String>>>`:
- Reads `$canon_home/aliases.toml`
- Returns `Ok(None)` if file doesn't exist
- Errors on TOML parse failure (with file path in message)
- `toml::from_str` into `HashMap<String, String>` enforces flat string values

`expand_filter_strings(filters: &[String], canon_home: &Path) -> Result<Vec<String>>`:
- Fast path: if no filter has `@` (via `has_alias_references`), return as-is
- Otherwise: load aliases, expand each filter string
- Passes `canon_home.join("aliases.toml")` to expansion function for error messages

**`src/main.rs`** — add `mod alias;`

#### Tests

All in `expr/alias.rs` `#[cfg(test)]` block — 21 pure unit tests:

**Basic expansion (7)**:
- Single alias expansion: `@image` → `(source.ext=jpg)`
- Multiple aliases: `@image OR @video` → `(…) OR (…)`
- Mixed with regular expressions: `@image AND source.size>1000`
- Alias containing boolean logic gets wrapped in parens
- No-alias passthrough: input without `@` returned unchanged
- Empty string passthrough
- Parentheses applied even to simple single-condition aliases

**Quoting behavior (3)**:
- `@` inside single quotes is literal: `source.path ~ '*@2x*'` unchanged
- `@` inside double quotes is literal
- Mixed: first `@` expanded, quoted `@` preserved

**Error cases (4)**:
- Unknown alias with aliases file present → "Check your aliases file at …"
- Unknown alias with empty map → "No aliases file found at …"
- Invalid name starting with digit → "Alias names must start with a letter"
- Bare `@` at end of input → error

**`has_alias_references` (4)**:
- Returns true for `@image`
- Returns false for `source.ext=jpg`
- Returns false for `@` only inside quotes
- Returns true for mixed quoted + unquoted `@`

**Name validation edge cases (3)**:
- Hyphens in name: `@my-alias` works
- Underscores in name: `@my_alias` works
- Name stops at boundary: `@image)` reads "image", leaves `)`

### Phase 3: Wire Expansion into CLI + Manifest Comments

- **Goal**: Connect expansion to all `--where` command paths and add manifest comment support.

#### Changes

**`src/main.rs`** — in each command arm that has `filters: Vec<String>`, add expansion before the command call:

```rust
let filters = alias::expand_filter_strings(&filters, &canon_home)?;
```

Command arms needing this line (~10 sites):
- `Commands::Ls`
- `Commands::Worklist`
- `Commands::Facts` (both `None` and `FactsAction::Delete`)
- `Commands::Coverage`
- `Commands::Compare`
- `Commands::Exclude` — `Set`, `Clear`, `List`, `Duplicates`, `SetObject`

For `ClusterAction::Generate`, keep both original and expanded:
```rust
let expanded = alias::expand_filter_strings(&filters, &canon_home)?;
cluster::generate(&mut db, &paths, &filters, &expanded, &dest, &output, &options)?;
```

`ClusterAction::Refresh` is unchanged — it reads already-expanded queries from the manifest.

**`src/cluster.rs`** — `generate()` signature change:
```rust
pub fn generate(
    db: &mut Db,
    scope_paths: &[PathBuf],
    original_filters: &[String],
    expanded_filters: &[String],
    dest: &Path,
    output_path: &Path,
    options: &GenerateOptions,
) -> Result<()>
```

- Parse `expanded_filters` (not originals) for `Filter::parse()`
- Store `expanded_filters` in `ManifestMeta.query`
- When originals differ from expanded, inject `# Original: <original>` comment lines before the `query` key in the serialized TOML

Comment injection: find the `query = ` line in the serialized TOML string and insert comment lines above it.

#### Tests

Existing `cluster.rs` tests must pass (they don't use aliases, so `original == expanded`). The expansion logic itself is fully tested in Phase 2.

## Architectural Decisions

| Decision | Rationale |
|----------|-----------|
| Pure expansion in `expr/` layer | Expression pre-processing, closely tied to filter system, but no I/O |
| I/O in top-level `alias.rs` | File loading is I/O; domain/expr layers stay pure |
| `--db` removed entirely (not hidden) | User base is one person; no migration path needed |
| Expansion in `main.rs` per-command arm | Keeps command functions unaware of aliases; minimal signature changes |
| Only `cluster::generate` gets both original + expanded | Only manifests need the original; all other commands just need expanded strings |
| `has_alias_references` as separate function | Avoids file I/O overhead for the common case of no aliases |

## Non-Goals

- `canon alias` subcommand (Story 5 — deferred to future work)
- Filter parser tests (pre-existing gap, separate concern)
- Refactoring the 14 `Filter::parse` call sites into a shared function
- Nested alias support (`@` in alias values is literal)

## Test Plan

### Existing Tests (Must Pass)

All 468 existing tests, including:
- `cluster::tests::test_cluster_excludes_suspended_roots`
- `cluster::tests::test_cluster_excludes_excluded_sources`
- `cluster::tests::test_cluster_archive_detection_counts_sources_not_objects`

### New Tests

21 unit tests in `expr/alias.rs` — see Phase 2 for full list.

## Implementation Checklist

- [x] Phase 1: Canon Home Directory — replace `--db` with `--canon-home` / `CANON_HOME`
- [x] Phase 2: Alias Expansion Engine — `expr/alias.rs` + `alias.rs` + 21 tests
- [x] Phase 3: Wire into CLI + manifest comments — expansion in all command arms + cluster.rs
- [x] Verify all existing tests pass
- [x] Update CLAUDE.md with canon home concept and alias module documentation

## Backward Compatibility

- `--db` flag is removed. Using it will produce clap's standard "unexpected argument" error.
- All existing `--where` expressions continue to work unchanged (no `@` = no expansion).
- Existing manifests are unaffected (`refresh` doesn't involve aliases).

## Performance Considerations

- **Zero overhead for non-alias usage**: `has_alias_references` scans for `@` outside quotes — O(n) on the filter string length, no file I/O, no allocation if no `@` found.
- **Aliases file loaded at most once per invocation**: `expand_filter_strings` loads the file only if `@` is detected, and all filter strings are expanded in one pass.
- No database impact whatsoever.
