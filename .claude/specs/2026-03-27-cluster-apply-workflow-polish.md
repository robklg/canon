# Story: Cluster/Apply Workflow Polish

**Design Spec**: `~/store/claude-designs/2026-03-27-cluster-apply-workflow-polish.md`
**Status**: Pending
**Created**: 2026-03-27

## Objective

The cluster generate → edit → apply loop is Canon's "act" phase — where orientation decisions turn into organized archives. Real archiving sessions revealed that rough edges in this loop force destructive recovery steps (manually deleting archive folders). This story addresses three friction areas: destination directory creation at generate time, editor integration, and apply confirmation visibility. The goal isn't new capability — it's making the existing workflow reliable and trustworthy.

## Functional Requirements Summary

**Story 1 — Dest directory creation**: When `--dest` points to a non-existent path under a registered archive root, Canon creates the directory after the plan confirms sources exist, before writing manifest/lock files. Zero-result queries create nothing.

**Story 2 — Editor by default**: Generate and refresh open `$VISUAL`/`$EDITOR`/`vi` by default (like `git commit`). `--no-edit` suppresses. `-e`/`--edit` removed. Manifest `fsync`'d before editor spawn to prevent NAS race conditions.

**Story 3 — Apply confirmation samples**: Show the output pattern and 5 sample destination paths in the apply confirmation summary. Skipped with `--yes`.

**Story 4 — Verbose destinations**: `--verbose` during apply shows `Copied: source -> dest` instead of just `Copied: source`. The `TransferProgress` trait gains a `dest_path` parameter.

## Current State

**`ops/cluster.rs`**: `execute_generate()` writes lock via `write_lock_file()` (File::create + flush) and manifest via `fs::write()`. No `mkdir`, no `fsync`. `execute_refresh()` follows the same pattern.

**`main.rs` (clap)**: `ClusterAction::Generate` has `edit: bool` (`-e`/`--edit`). `ClusterAction::Refresh` has no editor support.

**`cluster.rs` (interface)**: Editor logic at line 109-117 — checks `options.edit`, reads `$VISUAL`/`$EDITOR`, spawns process. "To apply" hint at line 119.

**`apply.rs` (interface)**: `print_apply_summary()` shows manifest path, destination, mode, file count, source roots (for rename/move), directory preview. No pattern display, no sample paths.

**`ops/apply.rs`**: `TransferProgress::on_transfer()` receives `(index, total, source_path, outcome)`. `ApplyTransfer` has `dest_rel_path` available in the transfer loop. `execute_apply()` already computes `base_dir.join(&transfer.dest_rel_path)` in `execute_single_transfer()`.

**Pattern expansion**: Lives in `ops/apply.rs` as `evaluate_pattern()`. Needs `EvalContext` built from root paths, relative path, scope prefix, and facts. Facts fetched via `batch_fetch_for_sources()`.

## Design

### Phase 1: Destination Directory Creation

- **Goal**: `cluster generate` creates dest directory when needed, after plan confirms sources exist.
- **Scope**: `ops/cluster.rs` — `execute_generate()` only.

#### Changes

**In `ops/cluster.rs::execute_generate()`**, add `fs::create_dir_all()` for the manifest parent directory before writing the lock file:

```rust
// After plan confirms sources exist, before writing files:
if let Some(parent) = params.manifest_path.parent() {
    fs::create_dir_all(parent).with_context(|| {
        format!("Failed to create directory: {}", parent.display())
    })?;
}
```

This goes after the plan phase (which already bails on zero results with "No sources matched the query") and before `write_lock_file()`. The `create_dir_all` is idempotent — no-op when the directory exists.

No change to `execute_refresh()` — refresh operates on an existing manifest, so the directory already exists.

#### Tests

- `generate_creates_dest_directory` — generate with a non-existent dest subdir under a temp archive root, verify directory and files created
- `generate_no_dir_on_zero_results` — generate with a filter matching nothing, verify directory NOT created

### Phase 2: Editor by Default with fsync

- **Goal**: Generate and refresh open the editor by default. Manifest fsync'd before editor spawn.
- **Scope**: `main.rs` (clap), `cluster.rs` (interface), `ops/cluster.rs` (fsync).

#### Changes

**Clap changes in `main.rs`**:

Remove from `ClusterAction::Generate`:
```rust
// REMOVE:
#[arg(short = 'e', long)]
edit: bool,
```

Add to `ClusterAction::Generate`:
```rust
/// Don't open editor after generation
#[arg(long)]
no_edit: bool,
```

Add to `ClusterAction::Refresh`:
```rust
/// Don't open editor after refresh
#[arg(long)]
no_edit: bool,
```

Update dispatch: pass `no_edit` to `cluster::generate()` and `cluster::refresh()`.

**fsync in `ops/cluster.rs`**:

Replace `fs::write(&params.manifest_path, &manifest)` with explicit file operations:

```rust
use std::io::Write;

let mut file = fs::File::create(&params.manifest_path).with_context(|| {
    format!("Failed to write manifest to {}", params.manifest_path.display())
})?;
file.write_all(manifest.as_bytes())?;
file.sync_all()?;
```

Apply this to both `execute_generate()` and `execute_refresh()`.

**Editor logic in `cluster.rs`**:

Move editor-opening into a shared helper and call it from both `generate()` and `refresh()`:

```rust
fn open_editor(path: &Path) -> Result<()> {
    let editor = std::env::var("VISUAL")
        .or_else(|_| std::env::var("EDITOR"))
        .unwrap_or_else(|_| "vi".to_string());
    let status = std::process::Command::new(&editor)
        .arg(path)
        .status()
        .with_context(|| format!("Failed to launch editor: {editor}"))?;
    if !status.success() {
        eprintln!("Warning: editor exited with status {}", status);
    }
    Ok(())
}
```

In `generate()`: replace `if options.edit { ... }` with `if !options.no_edit { open_editor(output_path)?; }`.

In `refresh()`: add `if !no_edit { open_editor(config_path)?; }` after the refresh completes.

**Update `GenerateOptions`**: Replace `edit: bool` with `no_edit: bool`.

#### Tests

No new automated tests — editor spawning is not unit-testable. The fsync change is a reliability improvement verified by manual testing on NAS volumes.

### Phase 3: Apply Confirmation with Pattern and Sample Destinations

- **Goal**: Show output pattern and 5 sample destination paths in the apply confirmation summary.
- **Scope**: `apply.rs` (interface layer).

#### Changes

**Expand `print_apply_summary()` signature** to accept the pattern string and sample computation data:

```rust
fn print_apply_summary(
    config_path: &Path,
    base_dir: &Path,
    pattern: &str,
    sources: &[&LockEntry],
    options: &ApplyOptions,
    root_paths: &HashMap<i64, String>,
    sample_destinations: &[SampleDestination],
)
```

**New helper struct**:

```rust
struct SampleDestination {
    dest_path: String,       // full absolute path
    error: Option<String>,   // pattern expansion error, if any
}
```

**Sample computation** — a new helper in `apply.rs`, called before the confirmation prompt (gated behind `!options.yes`):

```rust
fn compute_sample_destinations(
    conn: &mut Connection,
    sources: &[&LockEntry],
    pattern: &Pattern,
    needed_keys: &[String],
    scope_prefix: Option<&str>,
    root_paths: &HashMap<i64, String>,
    base_dir: &Path,
) -> Vec<SampleDestination>
```

This function:
1. Takes the first 5 sources (or fewer if less exist)
2. Fetches facts for those source IDs only: `repo::fact::batch_fetch_for_sources(conn, &sample_ids)`
3. Calls `evaluate_pattern()` (existing function in `ops/apply.rs`, needs to be made `pub`) for each
4. Returns `SampleDestination` entries with full paths or error messages

**Display in `print_apply_summary()`**:

```rust
eprintln!("Pattern: {pattern}");
// ... after file count ...
if !sample_destinations.is_empty() {
    eprintln!();
    eprintln!("Sample destinations:");
    for sample in sample_destinations {
        match &sample.error {
            Some(err) => eprintln!("  ({err})"),
            None => eprintln!("  {}", sample.dest_path),
        }
    }
    let remaining = sources.len().saturating_sub(sample_destinations.len());
    if remaining > 0 {
        eprintln!("  ... and {remaining} more");
    }
}
```

**Make `evaluate_pattern` public**: In `ops/apply.rs`, change `fn evaluate_pattern(...)` to `pub fn evaluate_pattern(...)` so the interface layer can call it for the sample.

**Gate behind `!yes`**: The sample computation and display are inside the `if !options.yes` block, consistent with existing ceremony convention.

#### Tests

- `sample_destinations_computes_for_five` — unit test for `compute_sample_destinations` with a mock set of sources
- `sample_destinations_handles_expansion_error` — test with a pattern referencing a missing fact key

### Phase 4: Verbose Apply Shows Destinations

- **Goal**: `--verbose` shows `Copied: source -> dest` format.
- **Scope**: `ops/apply.rs` (trait), `apply.rs` (implementor).

#### Changes

**Change `TransferProgress::on_transfer` signature** in `ops/apply.rs`:

```rust
// Before:
fn on_transfer(&self, index: usize, total: usize, source_path: &str, outcome: &TransferOutcome);

// After:
fn on_transfer(&self, index: usize, total: usize, source_path: &str, dest_path: &str, outcome: &TransferOutcome);
```

**Update call site** in `execute_apply()`:

```rust
let dest_full = params.base_dir.join(&transfer.dest_rel_path);
let dest_str = dest_full.display().to_string();
progress.on_transfer(i, total, &transfer.source_path, &dest_str, &outcome);
```

**Update `NoopProgress`**:

```rust
fn on_transfer(&self, _index: usize, _total: usize, _source_path: &str, _dest_path: &str, _outcome: &TransferOutcome) {}
```

**Update `CliTransferProgress`** in `apply.rs`:

```rust
fn on_transfer(&self, index: usize, _total: usize, source_path: &str, dest_path: &str, outcome: &ops::apply::TransferOutcome) {
    if let Some(ref p) = *self.progress.borrow() {
        let filename = source_path.rsplit('/').next().unwrap_or(source_path);
        p.update_with_name(index, filename);
    }
    if self.verbose {
        match outcome {
            ops::apply::TransferOutcome::Copied => {
                println!("Copied: {source_path} -> {dest_path}");
            }
            ops::apply::TransferOutcome::Renamed => {
                println!("Renamed: {source_path} -> {dest_path}");
            }
            ops::apply::TransferOutcome::Moved => {
                println!("Moved: {source_path} -> {dest_path}");
            }
            ops::apply::TransferOutcome::Error(msg) => {
                eprintln!("Error processing {source_path}: {msg}");
            }
            _ => {}
        }
    } else if let ops::apply::TransferOutcome::Error(msg) = outcome {
        eprintln!("Error processing {source_path}: {msg}");
    }
}
```

#### Tests

No new tests — the trait change is mechanical and verified by compilation. The verbose output format is verified by manual testing.

## Architectural Decisions

| Decision | Rationale |
|----------|-----------|
| `create_dir_all` after plan, before write | Zero-result queries create no directories. Benign empty dir if write fails — trivially cleaned. |
| `sync_all()` on manifest file | Best-effort fsync before editor spawn. Addresses NAS race condition observed in real use. |
| Editor default follows `git commit` pattern | `--no-edit` opt-out is familiar to the target audience (developers). |
| Sample computation in interface layer | Pattern expansion for 5 sources is ceremony presentation — the ops layer provides the tools (`evaluate_pattern`), the interface orchestrates the sample. Consistent with ADR: ceremony presentation belongs in interface. |
| `dest_path` added to trait, not computed in implementor | The ops layer has the data (`base_dir` + `dest_rel_path`). Passing it through the trait is cleaner than exposing transfer internals to the interface. |

## Non-Goals

- Changing `--dry-run` behavior (already shows full `source -> dest` plan)
- Adding `--no-edit` to `apply` (apply has no editor integration)
- Refactoring pattern expansion into a separate ops function (current `evaluate_pattern` + interface sample helper is sufficient)
- Coverage redundancy warnings for status predicates (mentioned in follow-up, out of scope)

## Test Plan

### Existing Tests (Must Pass)

- All 855 existing tests
- Cluster generate/refresh tests in `ops/cluster.rs`
- Apply plan/execute tests in `ops/apply.rs`

### New Tests

- Phase 1: `generate_creates_dest_directory`, `generate_no_dir_on_zero_results`
- Phase 3: `sample_destinations_computes_for_five`, `sample_destinations_handles_expansion_error`

## Implementation Checklist

- [ ] Phase 1: Destination directory creation in `execute_generate()`
- [ ] Phase 2: Editor by default (`--no-edit`), fsync manifest writes
- [ ] Phase 3: Apply confirmation with pattern and sample destinations
- [ ] Phase 4: Verbose apply shows `source -> dest`
- [ ] Verify all existing tests pass
- [ ] Update CLAUDE.md if needed

## Documentation Updates

- `docs/src/commands/archive/cluster.md` — Update workflow examples to reflect editor-by-default (remove explicit `-e`), add `--no-edit` flag, update manifest structure to show new header
- `docs/src/commands/archive/apply.md` — Update confirmation summary example to show pattern and sample destinations, update `--verbose` output example to show `source -> dest` format

## Backward Compatibility

**Breaking (pre-1.0, acceptable):**
- `-e`/`--edit` flag removed from `cluster generate` (now default behavior)
- `--verbose` output format changes from `Copied: {source}` to `Copied: {source} -> {dest}`

**Non-breaking:**
- `--no-edit` is new opt-out (no existing scripts use it)
- Destination directory creation is additive (previously failed, now succeeds)
- Pattern and sample display in apply summary is additive (more info shown)

## Performance Considerations

- Directory creation: Single `create_dir_all` call — negligible
- fsync: One `sync_all()` per manifest write — may add latency on network volumes, but correctness matters more than speed here
- Sample destinations: Fact fetch for 5 source IDs + 5 pattern evaluations — negligible
- Verbose dest path: One `base_dir.join()` per transfer — negligible
