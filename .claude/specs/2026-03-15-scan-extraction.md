# Refactoring Spec: Scan Pipeline Extraction (ADR Phase 2 Step 7)

## Overview

Extract the scan observe→reconcile→persist pipeline from `src/scan.rs` (interface) to `src/ops/scan.rs` (operations layer). The interface creates the directory walker and passes entries to the ops layer. A `ScanProgress` trait provides per-file observability.

**ADR**: `~/store/canon-architecture/2026-03-13-operations-layer.md` (Phase 2 Step 7)

**Key constraint**: The interface creates the `WalkDir` iterator (configuring symlink policy, error handling). The ops layer receives an iterator, it doesn't create it.

## Phase 1: Extract pipeline and tests

- **Status**: completed
- **Goal**: Move `scan_root()`, `process_file()`, `mark_missing_sources()`, and their types/tests to `ops/scan.rs`.
- **Non-goals**: Extracting `run()` ceremony. Extracting `find_candidates()`/candidate discovery. Extracting the hashing loop. Changing scan behavior.
- **Dependencies**: Foundation complete (ops/fs has `compute_partial_hash`)

### New module: `src/ops/scan.rs`

### Types that move (currently private in scan.rs)

All become `pub` in ops/scan.rs:

```rust
/// Classification of a source's fate during scan.
pub enum SourceOutcome {
    Seen,
    Missing,
    Disconnected,
}

/// Action taken for a processed file.
pub enum FileAction {
    New,
    Modified,
    Moved,
    Unchanged,
}

/// Per-file processing result.
pub struct ProcessResult {
    pub source_id: i64,
    pub action: FileAction,
    pub old_object_id: Option<i64>,
}

/// Accumulated scan statistics.
#[derive(Default)]
pub struct ScanStats {
    pub scanned: u64,
    pub new: u64,
    pub updated: u64,
    pub moved: u64,
    pub unchanged: u64,
    pub missing: u64,
    pub disconnected: u64,
    pub skipped: u64,
    pub hashed: u64,
    pub unexpected_hash_changes: u64,
}

/// A file that needs full hashing after the walk completes.
pub struct FileToHash {
    pub source_id: i64,
    pub full_path: PathBuf,
    pub old_object_id: Option<i64>,
    pub basis_changed: bool,
}

/// Result of scanning a single root.
pub struct ScanRootResult {
    pub stats: ScanStats,
    pub files_to_hash: Vec<FileToHash>,
    /// Warnings collected during scan (disconnected storage, errors).
    pub warnings: Vec<String>,
}
```

### ScanProgress trait

```rust
/// Observability for the scan pipeline. The interface implements this
/// to update progress bars, emit warnings, etc.
pub trait ScanProgress {
    /// Called after each file is processed.
    fn on_file(&self, path: &str, action: &FileAction);
    /// Called when a walk error is encountered (e.g., permission denied).
    fn on_walk_error(&self, error: &str);
    /// Called when process_file fails for a specific file.
    fn on_process_error(&self, path: &str, error: &str);
}
```

### ScanOptions struct

```rust
/// Parameters controlling scan behavior.
pub struct ScanOptions {
    /// Whether to compute partial hashes during the walk.
    pub hash: bool,
    /// Whether to re-hash files that already have a hash.
    pub hash_all: bool,
    /// Whether to treat device ID mismatches as missing (--ignore-device-id).
    pub ignore_device_id: bool,
}
```

### Function signatures

```rust
/// Scan a root directory, processing each entry through the
/// observe→reconcile→persist pipeline.
///
/// The interface creates the directory walker and passes entries here.
/// This function:
/// 1. Fetches expected source IDs (for missing detection)
/// 2. Processes each entry via process_file()
/// 3. Detects missing sources via domain::scan::find_missing()
/// 4. Marks missing/disconnected via mark_missing_sources()
///
/// Returns accumulated stats, files needing hashing, and warnings.
pub fn scan_root(
    conn: &Connection,
    root_id: i64,
    root_path: &str,
    scan_prefix: Option<&str>,
    entries: impl Iterator<Item = Result<walkdir::DirEntry, walkdir::Error>>,
    options: &ScanOptions,
    progress: &dyn ScanProgress,
) -> Result<ScanRootResult>;

/// Process a single file through observe→reconcile→persist.
/// Called by scan_root for each directory entry.
fn process_file(
    conn: &Connection,
    root_id: i64,
    rel_path: &str,
    full_path: &Path,
    device: i64,
    inode: i64,
    size: i64,
    mtime: i64,
    now: i64,
) -> Result<ProcessResult>;

/// Translate source outcomes to DB mutations.
/// Returns (missing_count, disconnected_count).
fn mark_missing_sources(
    conn: &Connection,
    outcomes: &[(i64, SourceOutcome)],
    now: i64,
    ignore_device_id: bool,
) -> Result<(u64, u64)>;
```

### Stderr removal

The current code has 3 locations that write to stderr:
1. `classify_sources_in_empty_dir()` — disconnected storage warning → collect into `ScanRootResult.warnings`
2. `scan_root()` walk errors → report via `progress.on_walk_error()`
3. `mark_missing_sources()` — disconnected summary → collect into `ScanRootResult.warnings`

The ops layer never writes to stderr. Warnings go into the result or through the progress trait. The interface decides how to display them.

### classify_sources_in_empty_dir

This function (lines 38-74) is called by `scan_root` for mount protection. It reads from the DB and checks device IDs — this is computation + repo, appropriate for ops. It currently writes a warning to stderr. In ops, it returns the warning string and the caller collects it.

### Interface changes (src/scan.rs)

The interface keeps:
- `run()` — CLI ceremony, root creation, WalkDir creation, hashing loop, progress bar, summary output
- `find_candidates()`, `scan_for_untracked()`, `find_common_ancestors()` — candidate discovery
- `mark_missing_path()` — CLI-driven path-based missing marking
- `check_overlapping_roots()`, `create_root()` — root validation
- `get_dir_device()`, `is_empty_dir()` — filesystem helpers used by ceremony

The interface removes:
- `scan_root()` function (now calls `ops::scan::scan_root()`)
- `process_file()` function
- `mark_missing_sources()` function
- `classify_sources_in_empty_dir()` function
- `ScanStats`, `FileToHash`, `ScanRootResult`, `ProcessResult`, `FileAction`, `SourceOutcome` types (imported from ops)
- `get_or_create_object()`, `current_timestamp()` helpers

The `run()` function changes:
```rust
// Before: calls local scan_root()
let result = scan_root(conn, root_id, root_path, prefix, now, should_hash, hash_all, ignore_device_id)?;

// After: creates WalkDir, calls ops::scan::scan_root()
let walker = WalkDir::new(&walk_path).follow_links(false);
let options = ScanOptions { hash: should_hash, hash_all, ignore_device_id };
let result = ops::scan::scan_root(conn, root_id, root_path_str, prefix, walker.into_iter(), &options, &progress_impl)?;
// Display warnings from result
for warning in &result.warnings {
    eprintln!("Warning: {warning}");
}
```

### Test migration

Tests that test `process_file()` (7 tests) and `mark_missing_sources()` (2 tests) move to `ops/scan.rs`. Tests that test `mark_missing_path()` (6 tests) stay in the interface — they test a function that remains there.

Tests that move:
- `process_file_new`
- `process_file_unchanged`
- `process_file_modified_size`
- `process_file_moved`
- `process_file_device_changed`
- `process_file_replaced`
- `process_file_revives_stale_record`
- `process_file_independent_operations`
- `mark_missing_sources_counts_correctly`
- `mark_missing_sources_disconnected_with_ignore_flag`

Test helper `create_temp_file()` moves to ops test module.

Tests that stay:
- `mark_missing_path_marks_sources`
- `mark_missing_path_not_under_any_root`
- `mark_missing_path_prefix_matches_subset`
- `mark_missing_path_already_not_present`
- `mark_missing_path_empty_prefix_marks_all`
- `mark_missing_path_no_sources_found`

### NoopProgress for tests

```rust
/// No-op progress implementation for tests.
struct NoopProgress;
impl ScanProgress for NoopProgress {
    fn on_file(&self, _path: &str, _action: &FileAction) {}
    fn on_walk_error(&self, _error: &str) {}
    fn on_process_error(&self, _path: &str, _error: &str) {}
}
```

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| Interface creates WalkDir, ops receives iterator | ADR requirement — walk configuration is interface concern |
| ScanProgress trait for observability | Replaces stderr writes; enables progress bar in CLI, different UI in TUI |
| Warnings collected in result | Ops layer doesn't write to stderr; interface decides display |
| classify_sources_in_empty_dir moves to ops | It's computation + repo, not display |
| process_file stays private in ops | It's an internal implementation detail of scan_root |
| mark_missing_path stays in interface | It's a CLI-specific operation with its own path resolution |
| Hashing loop stays in interface | It has its own progress bar, retry logic, and stdout output |
| get_or_create_object/current_timestamp move to ops | They're used by the pipeline, not by ceremony |

## Test Requirements

### Existing Tests
All 756 existing tests must pass. 10 pipeline tests move from `src/scan.rs` to `src/ops/scan.rs`. 6 mark_missing_path tests stay.

### New Tests to Add
- `test_scan_progress_notifications` — verify ScanProgress callbacks fire during scan_root

## Implementation Checklist
- [x] Create `src/ops/scan.rs` with types + `scan_root()` + `process_file()` + `mark_missing_sources()`
- [x] Add `ScanProgress` trait and `ScanOptions` struct
- [x] Move `classify_sources_in_empty_dir()` to ops, remove stderr writes
- [x] Register `pub mod scan;` in `ops/mod.rs`
- [x] Move 10 pipeline tests to `ops/scan.rs`, adapt helpers
- [x] Update `src/scan.rs`: remove pipeline, import from ops, create WalkDir in run()
- [x] ScanProgress notifications integrated (StderrProgress in interface)
- [x] Verify all tests pass (756 pass)
