# Story: Unified Flag Vocabulary — `--include` / `--allow`

**Design Spec**: ~/store/claude-designs/2026-02-15-unified-flag-vocabulary.md
**Status**: Complete
**Created**: 2026-02-15

## Objective

Canon's CLI flags for controlling source visibility and awareness use inconsistent vocabulary that conflates two different user intents. `--include-archived` means "show me more" on query commands but means something different on effectful commands. This directly resolves the "CLI doesn't fully click yet" friction identified in the vision (priority #1: interface coherence).

This story establishes a vocabulary that all future commands follow:
- **`--include`** — query commands only. Expands what you see. Always safe.
- **`--allow`** — effectful commands only. Acknowledges non-default source selection. Canon's defaults surface information so the user can decide consciously; `--allow` is the user saying "I'm aware, proceed."

Plus `--excluded` as a filter mode on `ls` (replacing `exclude list`), status indicators in long format, and manifest `[options]` for reproducible refresh.

## Functional Requirements Summary

**Story 1 — `--include` on query commands**: Single `--include` flag with values `excluded`, `archived`, `all`. Comma-separated and repeatable. Available on `ls`, `facts`, `coverage`, `worklist`, `compare`. Compare only accepts `excluded`. Default behavior unchanged. Auto-include-archived when scope is inside archive root preserved.

**Story 2 — `--allow` on effectful commands**: Single `--allow` flag with command-specific values. `cluster generate`: `archived`, `duplicates`. `apply`: `duplicates`, `cross-archive-duplicates`. `import-facts`: `archived`. No `--allow all`. No `--allow` on `cluster refresh` (reads from manifest).

**Story 3 — `ls --excluded`**: New filter mode in the mutually exclusive group (`--archived`, `--unarchived`, `--unhashed`, `--duplicates`, `--excluded`). Implicitly includes excluded sources. Shows both source-level and object-level excluded. Works with all `ls` features. `exclude list` subcommand removed.

**Story 4 — Output annotations**: `ls -l` gets a status column (E/X/A/blank) when visibility is expanded. `facts` gets a summary line noting what's included. `coverage` gets a note when `--include` changes the source set. `compare` gets a note when `--include excluded` is active.

**Story 5 — Manifest `[options]`**: New `[options]` section stores `allow` values. Always written (even when empty). `cluster refresh` reads options from manifest. `--show-archived` stays CLI-only on both generate and refresh (not stored).

## Current State

**Flags scattered across 8+ commands as separate booleans:**
- `--include-archived` (bool): ls, facts, coverage, worklist, cluster generate, cluster refresh
- `--include-excluded` (bool): ls, facts, coverage, worklist, compare
- `--allow-duplicates` (bool): cluster generate, cluster refresh, apply
- `--allow-cross-archive-duplicates` (bool): apply
- `--allow-archived` (bool): import-facts

**Predicate filtering pattern duplicated 4+ times** — `ls.rs`, `worklist.rs`, `coverage.rs`, `cluster.rs` each have their own `get_matching_sources()` with identical filtering logic using `include_archived: bool, include_excluded: bool`.

**Coverage bug**: `_include_excluded` parameter is unused in `coverage.rs` — excluded sources are never filtered.

**`exclude list`** has its own implementation in `exclude.rs` with separate `get_excluded_sources()` and `get_object_excluded_sources()` helpers and a fixed output format (no long format, sorting, `--where`, `--null`, scope paths).

**Manifest format** has `[meta]` and `[output]` sections only. No `[options]`.

**Auto-include-archived** (`main.rs:601-620`): When cwd or scope path is inside an archive root, `ls` automatically sets `include_archived = true`.

## Design

### Phase 1: `--include` Flag — Types, Clap, All Query Commands, Coverage Fix

- **Goal**: Replace `--include-archived` + `--include-excluded` with unified `--include` on all query commands. Fix coverage exclusion bug.

#### Changes

**New file `domain/include.rs`** — shared type for include semantics:

```rust
/// What additional sources to include in the working set.
/// Default (empty): active, non-excluded sources from source roots.
#[derive(Debug, Clone, Default)]
pub struct IncludeSet {
    pub excluded: bool,
    pub archived: bool,
}

impl IncludeSet {
    pub fn includes_excluded(&self) -> bool { self.excluded }
    pub fn includes_archived(&self) -> bool { self.archived }
    /// True when any non-default visibility is active.
    pub fn is_expanded(&self) -> bool { self.excluded || self.archived }
}
```

Register in `domain/mod.rs`.

**`main.rs`** — clap enum and conversion:

```rust
#[derive(Clone, PartialEq, clap::ValueEnum)]
enum IncludeValue {
    Excluded,
    Archived,
    All,
}

fn include_set_from(values: &[IncludeValue]) -> IncludeSet {
    let mut set = IncludeSet::default();
    for v in values {
        match v {
            IncludeValue::Excluded => set.excluded = true,
            IncludeValue::Archived => set.archived = true,
            IncludeValue::All => { set.excluded = true; set.archived = true; }
        }
    }
    set
}
```

**Clap struct changes** — on `Ls`, `Facts`, `Coverage`, `Worklist`, `Compare`:

Remove `include_archived: bool` and `include_excluded: bool`. Add:
```rust
/// Include additional sources: excluded, archived, all
#[arg(long, value_delimiter = ',')]
include: Vec<IncludeValue>,
```

Compare does not have `include_archived` today, only `include_excluded`. Replace with same `--include` flag.

**`main.rs` dispatch** — for each query command, convert to `IncludeSet`:

```rust
let mut include = include_set_from(&include);
// Preserve auto-include-archived for ls
if auto_include_archived { include.archived = true; }
```

Compare dispatch — validate after conversion:
```rust
if include.includes_archived() {
    bail!("'archived' is not a valid --include value for compare (valid: excluded)");
}
```

**`ls.rs`** — change `run()` and `show_duplicates()` signatures:

```rust
// Before
pub fn run(db, scope_paths, filter_strs, archived_mode, unarchived_only, unhashed_only,
           include_archived: bool, include_excluded: bool, ...) -> Result<()>
// After
pub fn run(db, scope_paths, filter_strs, archived_mode, unarchived_only, unhashed_only,
           include: &IncludeSet, ...) -> Result<()>
```

Change `get_matching_sources()` signature and body:
```rust
fn get_matching_sources(conn, scopes, filters, include: &IncludeSet) -> Result<(Vec<Source>, usize)>

// Inside:
.filter(|s| include.includes_archived() || s.is_from_role("source"))
.filter(|s| {
    if s.is_excluded() && !include.includes_excluded() {
        excluded_count += 1;
        return false;
    }
    true
})
```

Update footer message: `"use --include-excluded to show"` → `"use --include excluded to show"`.

**`facts.rs`** — same signature change pattern as ls.

**`worklist.rs`** — same signature change pattern. No JSONL format changes.

**`coverage.rs`** — signature change plus **bug fix**:

Change `get_matching_sources()` to accept `&IncludeSet` and add the missing exclusion filter:
```rust
// Currently missing — add this:
.filter(|s| {
    if s.is_excluded() && !include.includes_excluded() {
        excluded_count += 1;
        return false;
    }
    true
})
```

Change return type from `Result<Vec<Source>>` to `Result<(Vec<Source>, usize)>` to report excluded count (needed for Phase 4 annotations).

**`compare.rs`** — change `CompareOptions`:
```rust
// Before
pub struct CompareOptions {
    pub include_excluded: bool,
    pub verbose: bool,
}
// After
pub struct CompareOptions {
    pub include: IncludeSet,
    pub verbose: bool,
}
```

Update internal filtering to use `options.include.includes_excluded()`.

#### Tests

- New: `IncludeSet` unit tests in `domain/include.rs` (default, excluded, archived, is_expanded)
- New: `test_coverage_excludes_excluded_sources` — verify excluded sources filtered when `include.excluded` is false
- New: `test_coverage_includes_excluded_when_requested` — verify included when `include.excluded` is true
- Update: `test_ls_archived_flag_counts_sources_not_objects` — new `get_matching_sources` signature
- Update: all `get_matching_sources` tests in `exclude.rs` — signature change from `include_excluded: bool` to `include: &IncludeSet` (the `exclude.rs` version is separate — it only takes `include_excluded` and is used for the exclude subcommands, not query commands. Check if it needs updating or if it stays as-is since it serves a different purpose.)
- Update: `test_get_matching_sources_respects_scope` in `coverage.rs`

### Phase 2: `--allow` Flag — Types, Clap, Effectful Commands, Manifest `[options]`

- **Goal**: Replace `--allow-duplicates`, `--allow-cross-archive-duplicates`, `--include-archived` (cluster), `--allow-archived` (import-facts) with unified `--allow`. Add `[options]` to manifest. Drop `--allow` from refresh.

#### Changes

**`main.rs`** — per-command enums:

```rust
#[derive(Clone, PartialEq, clap::ValueEnum)]
enum ClusterAllow {
    Archived,
    Duplicates,
}

#[derive(Clone, PartialEq, clap::ValueEnum)]
enum ApplyAllow {
    Duplicates,
    #[value(name = "cross-archive-duplicates")]
    CrossArchiveDuplicates,
}

#[derive(Clone, PartialEq, clap::ValueEnum)]
enum ImportFactsAllow {
    Archived,
}
```

**Clap struct changes:**

`ClusterGenerate` — remove `include_archived`, `allow_duplicates`. Add:
```rust
/// Override safety guards: archived, duplicates
#[arg(long, value_delimiter = ',')]
allow: Vec<ClusterAllow>,
```
Keep `--show-archived` and `--force` unchanged.

`ClusterRefresh` — remove `include_archived`, `allow_duplicates`. Keep only `--show-archived`. No `--allow` flag.

`Apply` — remove `allow_duplicates`, `allow_cross_archive_duplicates`. Add:
```rust
/// Override safety guards: duplicates, cross-archive-duplicates
#[arg(long, value_delimiter = ',')]
allow: Vec<ApplyAllow>,
```

`ImportFacts` — remove `allow_archived`. Add:
```rust
/// Override safety guards: archived
#[arg(long, value_delimiter = ',')]
allow: Vec<ImportFactsAllow>,
```

**`main.rs` dispatch:**

Cluster generate:
```rust
let options = cluster::GenerateOptions {
    force,
    allow_archived: allow.contains(&ClusterAllow::Archived),
    allow_duplicates: allow.contains(&ClusterAllow::Duplicates),
    show_archived,
};
```

Cluster refresh:
```rust
cluster::refresh(&mut db, &config_path, show_archived)?;
```

Apply:
```rust
let options = apply::ApplyOptions {
    allow_duplicates: allow.contains(&ApplyAllow::Duplicates),
    allow_cross_archive_duplicates: allow.contains(&ApplyAllow::CrossArchiveDuplicates),
    // ... rest unchanged
};
```

Import-facts:
```rust
let allow_archived = allow.contains(&ImportFactsAllow::Archived);
import_facts::run(&mut db, allow_archived)?;
```

**`cluster.rs`** — manifest format:

```rust
#[derive(Serialize, Deserialize)]
pub struct ManifestConfig {
    pub meta: ManifestMeta,
    #[serde(default)]
    pub options: ManifestOptions,
    pub output: ManifestOutput,
}

#[derive(Serialize, Deserialize, Default)]
pub struct ManifestOptions {
    #[serde(default)]
    pub allow: Vec<String>,
}
```

`GenerateOptions` field rename:
```rust
pub struct GenerateOptions {
    pub force: bool,
    pub allow_archived: bool,     // was: include_archived
    pub allow_duplicates: bool,
    pub show_archived: bool,
}
```

Generate — write options:
```rust
fn allow_values_to_strings(options: &GenerateOptions) -> Vec<String> {
    let mut v = vec![];
    if options.allow_archived { v.push("archived".to_string()); }
    if options.allow_duplicates { v.push("duplicates".to_string()); }
    v
}

// In generate():
let config = ManifestConfig {
    meta: ManifestMeta { ... },
    options: ManifestOptions {
        allow: allow_values_to_strings(&options),
    },
    output: ManifestOutput { ... },
};
```

Refresh — new signature, reads options from manifest:
```rust
pub fn refresh(db: &mut Db, config_path: &Path, show_archived: bool) -> Result<()> {
    let config: ManifestConfig = toml::from_str(&config_content)?;

    let (allow_archived, allow_duplicates) = parse_manifest_allow(&config.options.allow)?;

    let options = GenerateOptions {
        force: false,
        allow_archived,
        allow_duplicates,
        show_archived,
    };

    // Print what's in effect
    if !config.options.allow.is_empty() {
        eprintln!("Options: allow {}", config.options.allow.join(", "));
    }

    // Continue with generate_lock()...
}

fn parse_manifest_allow(allow: &[String]) -> Result<(bool, bool)> {
    let mut archived = false;
    let mut duplicates = false;
    for v in allow {
        match v.as_str() {
            "archived" => archived = true,
            "duplicates" => duplicates = true,
            other => bail!(
                "Invalid allow value '{}' in manifest [options]. Valid: archived, duplicates",
                other
            ),
        }
    }
    Ok((archived, duplicates))
}
```

Update `generate_lock` and `query_sources` to use `allow_archived` instead of `include_archived`.

Update bail message in `generate_lock`: `"Use --allow-duplicates to include them anyway."` → `"Use --allow duplicates to include them anyway."`

#### Tests

- New: `test_manifest_options_round_trip` — serialize ManifestConfig with allow values, deserialize, verify
- New: `test_manifest_options_backward_compat` — parse TOML without [options], verify empty defaults
- New: `test_manifest_options_invalid_allow` — `parse_manifest_allow(["bogus"])` returns error
- Update: `test_cluster_excludes_excluded_sources` — new GenerateOptions field names
- Update: `test_cluster_excludes_suspended_roots` — same
- Update: `test_cluster_archive_detection_counts_sources_not_objects` — same

### Phase 3: `--excluded` Filter Mode, Status Column, Remove `exclude list`

- **Goal**: Add `--excluded` filter mode to `ls`. Add status column to `ls -l`. Remove `exclude list` subcommand.

#### Changes

**`main.rs`** — add `--excluded` to Ls clap struct:
```rust
/// Only show excluded sources (source-level and object-level)
#[arg(long, conflicts_with_all = ["archived", "unarchived", "unhashed", "duplicates"])]
excluded: bool,
```

In ls dispatch — implicit include:
```rust
if excluded {
    include.excluded = true;
}
```

Pass `excluded` as a new parameter to `ls::run()`.

Remove `ExcludeAction::List` variant from exclude subcommand enum. Remove its dispatch arm.

**`ls.rs`** — add `excluded_only: bool` parameter to `run()`.

Add filtering branch alongside existing filter modes:
```rust
} else if excluded_only {
    if source.is_excluded() {
        output_lines.push((...));
    }
}
```

Extend output tuples from `(String, Option<String>, i64, i64)` to `(String, Option<String>, i64, i64, String)` adding status. Compute status for every source:
```rust
fn status_indicator(source: &Source) -> &'static str {
    if source.excluded { " E" }
    else if source.object_excluded == Some(true) { " X" }
    else if source.is_from_role("archive") { " A" }
    else { "  " }
}
```

Determine whether to show status column:
```rust
let show_status = include.is_expanded() || excluded_only;
```

In long format output:
```rust
if show_status {
    print!("{status}{size_str:>8}  {date_str}  {source_path}{line_end}");
} else {
    print!("{size_str:>8}  {date_str}  {source_path}{line_end}");
}
```

**`exclude.rs`** — remove `list()` function. Keep `get_excluded_sources()` and `get_object_excluded_sources()` (used by other exclude subcommands). Keep `list_objects()` (used by `exclude list-objects` if that exists, or remove if orphaned — check).

#### Tests

- All existing exclude.rs tests for `get_excluded_sources()` and `get_object_excluded_sources()` remain (helpers are not removed)
- Existing ls tests updated for new `run()` signature (adding `excluded_only` parameter)

### Phase 4: Output Annotations, Cleanup

- **Goal**: Add visibility annotations to command output when `--include` is active. Update docs and CLAUDE.md.

#### Changes

**`facts.rs`** — summary line when include is expanded:

Currently prints something like `12,345 sources`. When `include.is_expanded()`, enhance to:
```
12,345 sources (incl. 2,100 excluded, 890 archived)
```

This requires counting excluded and archived sources during filtering. Add counters alongside `excluded_count`:
```rust
let mut archived_count = 0usize;
// In the filter chain, count sources from archive roots
```

**`coverage.rs`** — note when include changes the source set:

When `include.is_expanded()`, print a note before stats:
```
[including excluded]
```
or similar, depending on which flags are active.

**`compare.rs`** — note when `--include excluded` is active:

When `include.includes_excluded()`, add a note in summary output.

**CLAUDE.md updates:**
- Document `IncludeSet` in Domain Layer section
- Document `--include` / `--allow` vocabulary split (which commands get which)
- Document `ManifestOptions` in Cluster/Apply Workflow section
- Document `--excluded` filter mode alongside existing filter modes
- Update CLI Conventions if needed

**docs/ updates:**
- Update command reference pages for all changed commands
- Update manifest format documentation with `[options]` section
- Remove `exclude list` from command reference, note `ls --excluded` replacement

## Architectural Decisions

| Decision | Rationale |
|----------|-----------|
| `IncludeSet` as struct with booleans, not `HashSet<enum>` | The filtering code needs `includes_archived()` and `includes_excluded()` — booleans are what it actually checks. Simple, no allocation. |
| `IncludeValue` clap enum shared across commands, per-command validation for compare | One enum definition, one concept. Compare's restriction is a command-layer validation, not a type-system concern. Avoids a second enum for one command. |
| Per-command `Allow` enums rather than shared enum | Values differ per command. Per-command enums give compile-time validation and auto-generated clap error messages listing only valid values. |
| `ManifestOptions.allow` as `Vec<String>` not `Vec<ClusterAllow>` | Manifest is human-editable TOML. Strings keep it plain text. Validation happens on read. |
| No `--allow` on refresh | Manifest is the single source of truth. Avoids clap detection complexity (empty vec vs absent flag). Edit the TOML if you want different options. |
| `--show-archived` not stored in manifest | It's output verbosity, not semantics. Manifest captures what affects the result, not presentation preferences. |
| No worklist JSONL format changes | External tools don't use `excluded` or `root_role`. If users need excluded sources identified, `ls --excluded -0` serves that purpose. YAGNI. |
| Status column in ls -l only when visibility is expanded | No visual noise in default output. Column appears when there's something to distinguish. |
| Clean break, no deprecation | Single user at the moment. Old flags removed entirely. |

## Non-Goals

- No deprecation period or backward-compatible flag aliases
- No `--allow all` shortcut (each guard must be named explicitly)
- No worklist JSONL format changes
- No changes to `--archived=show` display mode on ls (orthogonal)
- No changes to `--show-archived` semantics on cluster (stays CLI-only display flag)
- No shared `get_matching_sources()` extraction across modules (keep per-module, change signature)

## Test Plan

### Existing Tests (Must Pass)

All existing tests continue to pass. Tests requiring mechanical updates:

**ls.rs:**
- `test_ls_archived_flag_counts_sources_not_objects` — new `get_matching_sources` signature

**cluster.rs:**
- `test_cluster_excludes_suspended_roots` — `GenerateOptions` field rename
- `test_cluster_excludes_excluded_sources` — same
- `test_cluster_archive_detection_counts_sources_not_objects` — same

**coverage.rs:**
- `test_get_matching_sources_respects_scope` — new signature + return type
- `test_coverage_archived_counts_sources_not_objects` — if affected by signature change

**exclude.rs:**
- `get_matching_sources` tests — check if exclude.rs's version needs updating (it serves exclude subcommands, may keep its own signature)

### New Tests

**Phase 1:**
- `test_include_set_default_not_expanded` — `IncludeSet::default().is_expanded() == false`
- `test_include_set_excluded_is_expanded` — `excluded = true` → `is_expanded() == true`
- `test_include_set_archived_is_expanded` — `archived = true` → `is_expanded() == true`
- `test_coverage_excludes_excluded_sources` — excluded sources filtered when `include.excluded` is false
- `test_coverage_includes_excluded_when_requested` — excluded sources included when `include.excluded` is true

**Phase 2:**
- `test_manifest_options_round_trip` — serialize with allow values, deserialize, verify
- `test_manifest_options_backward_compat` — TOML without [options] deserializes with empty defaults
- `test_manifest_options_invalid_allow` — `parse_manifest_allow(["bogus"])` returns error

## Implementation Checklist

- [x] Phase 1: `--include` flag on all query commands + coverage exclusion fix
- [x] Phase 2: `--allow` flag on all effectful commands + manifest `[options]`
- [x] Phase 3: `--excluded` filter mode + status column + remove `exclude list`
- [x] Phase 4: Output annotations + CLAUDE.md + docs updates
- [x] Verify all existing tests pass
- [x] Update CLAUDE.md with new patterns

## Documentation Updates

- Command reference for `ls`, `facts`, `coverage`, `worklist`, `compare`: new `--include` flag, remove old flags
- Command reference for `cluster generate`, `apply`, `import-facts`: new `--allow` flag, remove old flags
- Command reference for `cluster refresh`: remove `--allow` flags, document that options come from manifest
- `ls` documentation: add `--excluded` filter mode
- Manifest format documentation: add `[options]` section
- Remove `exclude list` from command reference, note `ls --excluded` as replacement

## Backward Compatibility

**CLI**: Clean break. All old flags removed:

| Removed | Replacement | Commands |
|---------|-------------|----------|
| `--include-archived` | `--include archived` | ls, facts, coverage, worklist |
| `--include-excluded` | `--include excluded` | ls, facts, coverage, worklist, compare |
| `--include-archived` | `--allow archived` | cluster generate |
| `--allow-duplicates` | `--allow duplicates` | cluster generate, apply |
| `--allow-cross-archive-duplicates` | `--allow cross-archive-duplicates` | apply |
| `--allow-archived` | `--allow archived` | import-facts |
| `exclude list` | `ls --excluded` | (subcommand removed) |

**Manifest format**: Backward compatible. Old manifests without `[options]` deserialize with `#[serde(default)]` to empty defaults. New manifests always include `[options]`.

## Performance Considerations

None. All changes are to CLI parsing and in-memory filtering. No new database access patterns, no transaction scope changes.
