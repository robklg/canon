# Refactoring Spec: Cluster Execute Extraction

## Overview

Extract manifest and lock file writing from `src/cluster.rs` (interface) to `src/ops/cluster.rs` (operations layer). Manifests are stored artifacts, not presentation — they belong in the ops layer so any interface (CLI, TUI) can produce identical manifests.

**ADR**: `~/store/canon-architecture/2026-03-13-operations-layer.md` — Phase 3, Step 3

## Scope

### In scope
- Add `execute_generate()` and `execute_refresh()` to ops/cluster
- Move manifest content functions to ops/cluster: `generate_summary_comments`, `generate_fact_help`, `extract_notes`, `inject_comments_before_key`, `current_timestamp`, `allow_values_to_strings`, `parse_manifest_allow`, `write_lock_file`
- Replace `hash_file()` with `ops::fs::compute_full_hash()`
- Remove `LockGenerationResult` and `generate_lock()` from interface
- Move related tests to ops/cluster
- Thin interface to: parse args, call plan, display warnings, call execute, print stdout, launch editor

### Non-goals
- Changing manifest format or structure
- Changing `plan_generate()` or `ClusterGeneratePlan`
- Moving `GenerateOptions` (CLI-specific)
- Moving `print_cluster_stdout()` (terminal presentation)

## Design

### New types in ops/cluster

```rust
/// Parameters for executing a cluster generation (writing lock file + manifest).
pub struct ExecuteGenerateParams {
    pub lock_path: PathBuf,
    pub manifest_path: PathBuf,
    pub expanded_filters: Vec<String>,
    pub original_filters: Vec<String>,
    pub scope_prefixes: Vec<String>,
    pub archive_root_id: i64,
    pub base_dir: String,
    pub allow: Vec<String>,
}

/// Result from executing a cluster generation — display data for the interface.
pub struct ExecuteGenerateResult {
    pub source_count: usize,
    pub root_breakdown: Vec<(String, usize)>,
    pub not_archived_count: usize,
    pub archived_count: usize,
    pub excluded_count: usize,
    pub unhashed_count: usize,
}

/// Parameters for executing a cluster refresh.
pub struct ExecuteRefreshParams {
    pub lock_path: PathBuf,
    pub manifest_path: PathBuf,
    pub old_manifest_content: String,
    pub config: ManifestConfig,
}

/// Result from executing a cluster refresh.
pub struct ExecuteRefreshResult {
    /// None if no sources matched (lock file removed, minimal manifest written).
    pub outcome: Option<ExecuteGenerateResult>,
}
```

### New functions in ops/cluster

```rust
/// Write lock file + manifest for a fresh cluster generation.
///
/// 1. Writes JSONL lock file from plan.lock_entries
/// 2. Computes lock file hash
/// 3. Builds ManifestConfig from params
/// 4. Assembles manifest (summary + notes + TOML + fact help)
/// 5. Writes manifest file
///
/// Returns display data for the interface.
pub fn execute_generate(
    plan: &ClusterGeneratePlan,
    params: &ExecuteGenerateParams,
) -> Result<ExecuteGenerateResult>;

/// Rewrite lock file + update existing manifest for a cluster refresh.
///
/// 1. Writes JSONL lock file from plan.lock_entries
/// 2. Computes lock file hash
/// 3. Updates config with new hash + timestamp
/// 4. Preserves notes from old manifest
/// 5. Assembles and writes updated manifest
///
/// If plan has no lock entries: removes lock file, writes minimal manifest.
pub fn execute_refresh(
    plan: &ClusterGeneratePlan,
    params: &ExecuteRefreshParams,
) -> Result<ExecuteRefreshResult>;
```

### Functions moving to ops/cluster (become private)

These move from `src/cluster.rs` to `src/ops/cluster.rs`:

| Function | Notes |
|----------|-------|
| `write_lock_file(path, entries)` | Private helper for execute functions |
| `generate_summary_comments(plan)` | Signature changes: takes `&ClusterGeneratePlan` directly |
| `generate_fact_help(count, facts)` | Unchanged |
| `extract_notes(content)` | Unchanged |
| `inject_comments_before_key(toml, key, comments)` | Unchanged |
| `current_timestamp()` | Unchanged |
| `allow_values_to_strings(allow)` | Signature changes: takes `&[String]` instead of `&GenerateOptions` |
| `parse_manifest_allow(allow)` | Already `pub`, stays `pub` |

### Functions eliminated

| Function | Replacement |
|----------|-------------|
| `hash_file()` | `ops::fs::compute_full_hash()` |
| `generate_lock()` | Split: plan stays as `plan_generate()`, lock writing goes into `execute_generate()`/`execute_refresh()` |
| `LockGenerationResult` | Replaced by `ExecuteGenerateResult` (returned from execute, not built manually) |

### Interface changes to `src/cluster.rs`

**`generate()`** becomes:
1. Validate args (force, scope+filter required) — stays
2. Resolve paths, parse filters — stays
3. Call `ops::cluster::plan_generate()` — stays
4. Display warnings (archived list, mixed types) to stderr — stays
5. Call `ops::cluster::execute_generate(plan, params)` — **new**
6. Print stdout summary — stays
7. Launch editor — stays

**`refresh()`** becomes:
1. Read and parse old manifest — stays
2. Validate version, parse allow options — stays
3. Report options to stderr — stays
4. Parse scope/filters from config — stays
5. Call `ops::cluster::plan_generate()` — **new** (currently goes through `generate_lock`)
6. Display warnings (archived list, mixed types) to stderr — **new** (currently inside `generate_lock`)
7. Call `ops::cluster::execute_refresh(plan, params)` — **new**
8. Print stdout summary — stays (for non-empty result)

Note: `refresh()` currently calls `generate_lock()` which bundles plan+warnings+lock-write. After extraction, `refresh()` calls `plan_generate()` directly and handles warnings inline, matching the pattern in `generate()`. This eliminates the shared `generate_lock()` helper entirely.

### `generate_summary_comments` signature change

Currently takes `&LockGenerationResult` (interface type). After move, takes `&ClusterGeneratePlan` directly:

```rust
fn generate_summary_comments(plan: &ClusterGeneratePlan) -> String {
    // Uses plan.lock_entries.len(), plan.root_breakdown, plan.not_archived_count,
    // plan.archived.len(), plan.excluded_count, plan.unhashed_count
}
```

All needed fields are already on `ClusterGeneratePlan`.

### `allow_values_to_strings` signature change

Currently takes `&GenerateOptions` (interface type). After move, takes the allow values directly:

```rust
// In ops (takes parsed booleans):
fn allow_to_strings(allow_archived: bool, allow_duplicates: bool) -> Vec<String>

// Or simply constructed inline in execute_generate since it's trivial
```

Actually, `execute_generate` receives `allow: Vec<String>` in its params — the interface already knows the allow values from CLI args. So this function is eliminated; the caller passes the strings directly.

## Test Requirements

### Existing tests that move to ops/cluster
- `test_extract_notes_empty_placeholder`
- `test_extract_notes_with_content`
- `test_extract_notes_missing`
- `test_extract_notes_before_meta`
- `test_extract_notes_before_next_section`
- `test_generate_summary_single_root`
- `test_generate_summary_multiple_roots`
- `test_generate_summary_no_skipped`
- `test_generate_summary_with_skipped`
- `test_generate_summary_with_archived_skipped`

### Tests that stay in interface
- `test_format_count` — tests `ceremony::format_count`, unrelated
- `test_manifest_options_round_trip` — tests TOML serde on ops types, could go either way; leave in interface
- `test_manifest_options_backward_compat` — same
- `test_manifest_options_invalid_allow` — tests `parse_manifest_allow` which moves; move with it
- `test_version_*` and `test_manifest_*_version_*` — test `validate_manifest_version` which is already in ops; move to ops

### New tests to add in ops/cluster
1. `test_write_lock_file_round_trip` — write entries, read back as JSONL, verify fields match
2. `test_execute_generate_writes_files` — integration test: plan with sources, execute, verify lock file and manifest exist with expected content sections (summary, notes, TOML, fact help)
3. `test_execute_generate_injects_original_filter_comments` — verify "# Original:" comments appear when original differs from expanded
4. `test_execute_refresh_preserves_notes` — verify user-edited notes survive a refresh
5. `test_execute_refresh_empty_removes_lock` — verify empty plan removes lock file and writes minimal manifest

## Design Decisions
- `execute_generate` and `execute_refresh` are separate functions (not a single `execute` with a mode flag) because their parameter shapes and behaviors differ significantly
- `ClusterGeneratePlan` is consumed directly by the summary/execute functions — no intermediate result type
- `allow_values_to_strings()` is eliminated; the interface passes `allow: Vec<String>` directly to execute params
- Warning display (stderr output for archived files, mixed types) stays in the interface — it's presentation of the plan, separate from execution
- `generate_lock()` is eliminated entirely — its three responsibilities (plan, warnings, lock-write) are cleanly separated into plan_generate + interface warnings + execute
