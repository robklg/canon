# Story: Apply Receipts

**Epic**: [Decision Receipts](epic-decision-receipts.md) — Story 2
**Design Spec**: [~/store/claude-designs/2026-04-11-decision-receipts.md](~/store/claude-designs/2026-04-11-decision-receipts.md)
**Source Decision Link Spec**: [~/store/claude-designs/2026-04-12-destination-provenance.md](~/store/claude-designs/2026-04-12-destination-provenance.md)
**Status**: Completed
**Created**: 2026-04-12

## Objective

Build the central receipt writing facility and deliver the first receipt type — apply receipts. Apply is first because it has the richest per-item data already available, exercises targeted placement logic (central/alongside), and is the most valuable receipt: it records which files went where, from which sources, preserving the lineage of every archived file.

Each destination source also gains a direct `decision_id` link to the apply decision that placed it, and receipt items capture `previous_decision_id` to create a walkable provenance chain.

## Functional Requirements Summary

- `execute_apply` generates a receipt after successful (or partial) completion
- Receipt is written to the target archive's `.canon-ledger/`, placed according to the `layout` setting
- Each item records: source root path, source relative path, destination relative path, content hash, file size, mtime, previous_decision_id
- For partial applies (interrupted or with failures), only completed transfers are listed
- The receipt's `[meta]` section includes the manifest path for cross-referencing
- The decision record's `receipt_root_id` and `receipt_rel_path` are populated
- Receipt is written atomically (`.incomplete` file + rename)
- No receipt on dry run, all-failures, or `--no-receipt`
- Every completed transfer sets `decision_id` on the destination source record
- Re-apply to the same destination path updates `decision_id` to the new decision
- Receipt items include `previous_decision_id` — the value on the destination source before this apply overwrote it (NULL for new destinations)

## Current State

Story 1 established all infrastructure this story builds on:

- **`domain/config.rs`**: `LedgerConfig`, `RecordingMode`, `ReceiptLayout` — fully implemented
- **`ops/decision.rs`**: `DecisionParams` with `record_enabled`, `receipt_enabled`, `ledger_config`; `DecisionRecorder` with two-phase start/complete and warning collection
- **`ops/fs.rs`**: `write_file_incomplete()` and `finalize_file()` — atomic write primitives
- **`repo/decision.rs`**: `insert_started()` accepts `receipt_root_id`/`receipt_rel_path` (currently always `None`)
- **Schema**: `receipt_root_id` and `receipt_rel_path` columns on decisions table; `decision_id` column on sources table
- **`repo/source.rs`**: `insert_destination()` accepts `decision_id` in `NewSource`; `build_new_source()` currently hardcodes `decision_id: None`
- **CLI**: `--no-receipt` global flag; `receipt_enabled` computed from config + flag + dry-run
- **Scan**: `.canon-ledger/` excluded from walk

What's missing: `ops/receipt.rs` (doesn't exist), receipt path computation, receipt writing, apply integration (per-item collection, decision_id threading, previous_decision_id capture).

## Design

### Receipt Placement

For apply (targeted receipts), the receipt path uses the manifest's `base_dir_rel` — the configured destination directory within the archive. This is deterministic and known at decision start time.

- **Central layout**: `{archive_root}/.canon-ledger/{base_dir_rel}/{id:06}-apply.toml`
- **Alongside layout**: `{archive_root}/{base_dir_rel}/.canon-ledger/{id:06}-apply.toml`
- **Empty base_dir_rel** (archive root is the base): `{archive_root}/.canon-ledger/{id:06}-apply.toml` for both layouts

The `ReceiptRef` stored in the DB is always relative to the archive root:
- Central: `receipt_rel_path = ".canon-ledger/Media/2016/Italy/000043-apply.toml"`
- Alongside: `receipt_rel_path = "Media/2016/Italy/.canon-ledger/000043-apply.toml"`

### Receipt Format

```toml
# Canon Decision Receipt
# Applied 35 files (127.4 MB) to /Volumes/Archive/Media/2016/Italy

[meta]
receipt_version = 1
decision_id = 43
command = "apply"
timestamp = 1744300800
scope = ["/Volumes/old-laptop/Photos/italy"]
reason = "Italy 2016 — assembled from kali, old-laptop, and iPhone backup"
summary = "Applied 35 files (127.4 MB) to /Volumes/Archive/Media/2016/Italy"
canon_version = "0.4.1"
command_line = "canon apply manifest.toml --reason '...'"
manifest = "/Volumes/Archive/Media/2016/Italy/manifest.toml"

[[items]]
source_root = "/Volumes/old-laptop"
source_rel_path = "Photos/italy/IMG_001.jpg"
destination_rel_path = "Media/2016/Italy/IMG_001.jpg"
hash = "sha256:abc123..."
size = 3456789
mtime = 1700000000
previous_decision_id = 12
```

### Data Flow

```
execute_apply(conn, plan, params, progress, decision)
  │
  ├─ DecisionRecorder::start(conn, params, receipt_ctx)
  │    ├─ compute receipt rel_path from (decision_id, command, layout, base_dir_rel)
  │    ├─ ensure .canon-ledger/ directory exists
  │    ├─ insert_started(..., receipt_root_id, receipt_rel_path)
  │    └─ store ReceiptRef + abs_path internally
  │
  ├─ Transfer loop (per item):
  │    ├─ fetch_decision_id_at_path(conn, root_id, rel_path) → prev_id
  │    ├─ execute file transfer (copy/rename/move)
  │    ├─ insert_destination(conn, &new_source)  [with decision_id]
  │    └─ collect ApplyReceiptItem (on success only)
  │
  ├─ write_receipt(receipt_abs_path, &apply_receipt, &summary)
  │    ├─ serialize TOML
  │    ├─ prepend comment header
  │    └─ write_file_incomplete(path)
  │
  └─ recorder.complete(conn, status, counts, summary)
       └─ finalize_file(receipt_path)  [.incomplete → .toml]
```

### Phase 1: Receipt types and writer

**Goal**: The receipt writing facility, tested in isolation. No integration with apply yet.

**Scope**:
- `ops/receipt.rs` (new file)
- `ops/mod.rs`: add `pub mod receipt;`

#### Changes

**New types**:

```rust
/// Reference to a receipt file on disk, stored in the decision record.
pub struct ReceiptRef {
    pub root_id: i64,
    pub rel_path: String,
}

/// Shared meta section for all receipt types.
/// `manifest` is apply-specific — omitted for other receipt types via skip_serializing_if.
#[derive(Serialize)]
pub struct ReceiptMeta {
    pub receipt_version: u32,     // always 1
    pub decision_id: i64,
    pub command: String,
    pub timestamp: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub summary: String,
    pub canon_version: String,
    pub command_line: String,
    /// Manifest path — apply receipts only. Omitted for other receipt types.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manifest: Option<String>,
}

/// Apply-specific receipt.
#[derive(Serialize)]
pub struct ApplyReceipt {
    pub meta: ReceiptMeta,
    pub items: Vec<ApplyReceiptItem>,
}

#[derive(Serialize)]
pub struct ApplyReceiptItem {
    pub source_root: String,
    pub source_rel_path: String,
    pub destination_rel_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,          // "sha256:abc123..."
    pub size: i64,
    pub mtime: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_decision_id: Option<i64>,
}
```

**Receipt path computation** (pure functions, no I/O):

```rust
/// Format the receipt filename: 6-digit zero-padded decision_id + command.
pub fn receipt_filename(decision_id: i64, command: &str) -> String

/// Compute the receipt rel_path within an archive root for targeted receipts.
/// Central: .canon-ledger/{base_dir_rel}/{filename}
/// Alongside: {base_dir_rel}/.canon-ledger/{filename}
/// Empty base_dir_rel: .canon-ledger/{filename} for both layouts.
pub fn compute_targeted_receipt_rel_path(
    decision_id: i64,
    command: &str,
    base_dir_rel: &str,
    layout: &ReceiptLayout,
) -> String
```

**Writer** (uses `ops::fs::write_file_incomplete`):

```rust
/// Write a receipt to disk as an .incomplete file.
/// Serializes to TOML with a comment header prepended.
/// Returns Err on failure — caller decides whether to warn or propagate.
pub fn write_receipt<T: Serialize>(
    path: &Path,           // the final .toml path (writer computes .incomplete)
    receipt: &T,
    comment_summary: &str, // e.g. "Applied 35 files (127.4 MB) to ..."
) -> Result<()>

/// Finalize a receipt: rename .incomplete → .toml.
/// Wraps ops::fs::finalize_file.
pub fn finalize_receipt(path: &Path) -> Result<()>
```

#### Tests

- Serialize `ApplyReceipt` to TOML: verify `[meta]` section, `[[items]]` entries, field names
- Comment header: output starts with `# Canon Decision Receipt\n# {summary}\n\n`
- Optional fields: `reason = None`, `scope = None`, `hash = None`, `previous_decision_id = None` → fields omitted
- `receipt_version` is always 1
- `previous_decision_id` present when `Some(42)` → `previous_decision_id = 42` in output
- Hash format: `Some("sha256:abc...")` serializes correctly
- `receipt_filename(43, "apply")` → `"000043-apply.toml"`
- `receipt_filename(1000000, "apply")` → `"1000000-apply.toml"` (no truncation)
- Central layout path: `compute_targeted_receipt_rel_path(43, "apply", "Media/2016/Italy", Central)` → `".canon-ledger/Media/2016/Italy/000043-apply.toml"`
- Alongside layout path: same inputs with `Alongside` → `"Media/2016/Italy/.canon-ledger/000043-apply.toml"`
- Empty base_dir_rel: both layouts → `".canon-ledger/000043-apply.toml"`
- `write_receipt` creates `.incomplete` file with correct content (tempdir test)
- `finalize_receipt` renames `.incomplete` → `.toml` (tempdir test)
- `write_receipt` to invalid path returns `Err`

### Phase 2: DecisionRecorder receipt integration

**Goal**: Recorder computes receipt path at start, finalizes at complete. All callers updated for new signature.

**Scope**:
- `ops/decision.rs`: `ReceiptContext` struct, `start()` gains `receipt_ctx` parameter, receipt path computation + directory creation, `receipt_ref()`/`receipt_abs_path()` accessors, `complete()` gains finalize step
- All callers of `DecisionRecorder::start()`: pass `None` for `receipt_ctx`

#### Changes

**New struct**:

```rust
/// Context needed to compute receipt placement for targeted receipts (apply).
pub struct ReceiptContext {
    pub archive_root_id: i64,
    pub archive_root_path: String,
    pub base_dir_rel: String,
}
```

**`DecisionRecorder` changes**:

```rust
pub struct DecisionRecorder {
    id: Option<i64>,
    receipt_ref: Option<ReceiptRef>,       // NEW: for DB linkage
    receipt_abs_path: Option<PathBuf>,      // NEW: for write/finalize
    warnings: Vec<String>,
}

impl DecisionRecorder {
    pub fn start(
        conn: &Connection,
        params: &DecisionParams,
        receipt_ctx: Option<&ReceiptContext>,  // NEW
    ) -> Self {
        // If receipt_enabled && receipt_ctx is Some:
        //   1. compute_targeted_receipt_rel_path(id, command, base_dir, layout)
        //   2. abs_path = archive_root_path / rel_path
        //   3. ensure parent dir of abs_path exists (create_dir_all)
        //   4. store ReceiptRef { root_id: archive_root_id, rel_path }
        //   5. pass receipt_root_id + receipt_rel_path to insert_started()
        // Otherwise: receipt fields stay None, insert_started gets None/None
    }

    pub fn decision_id(&self) -> Option<i64> { self.id }

    pub fn receipt_ref(&self) -> Option<&ReceiptRef> { self.receipt_ref.as_ref() }

    pub fn receipt_abs_path(&self) -> Option<&Path> {
        self.receipt_abs_path.as_deref()
    }

    pub fn complete(&mut self, conn: &Connection, status: DecisionStatus,
                    counts: DecisionCounts, summary: &str) {
        // Existing: update_completed(...)
        // NEW: if receipt_abs_path is Some, call finalize_receipt()
        //      failure → push to warnings, don't halt
    }
}
```

**Caller updates** (mechanical — pass `None` for receipt_ctx):

All callers of `DecisionRecorder::start()` across command/ops modules: `scan.rs`, `exclude.rs`, `roots.rs`, `cluster.rs`, `import_facts.rs`, `note.rs`, and `ops/apply.rs`. Each adds `, None` as the third argument. Apply will get a real `ReceiptContext` in Phase 4.

#### Tests

- Recorder with `receipt_ctx`: `receipt_ref()` returns `Some` with correct root_id and rel_path
- Recorder without `receipt_ctx`: `receipt_ref()` returns `None`
- Recorder with `receipt_enabled = false` + `receipt_ctx`: `receipt_ref()` returns `None`
- `complete()` calls `finalize_receipt` when receipt_abs_path is set: verify `.incomplete` → `.toml` rename (tempdir test)
- `complete()` with no receipt path: no file operations, no errors
- Decision record in DB has `receipt_root_id` and `receipt_rel_path` populated when receipt_ctx provided

### Phase 3: Apply decision_id threading + previous_decision_id

**Goal**: Destination sources get `decision_id`; `previous_decision_id` is captured per transfer.

**Scope**:
- `repo/source.rs`: new `fetch_decision_id_at_path()`
- `ops/apply.rs`: `execute_single_transfer()` gains `decision_id` param + returns `previous_decision_id`; `build_new_source()` and `build_new_source_from_lock()` gain `decision_id` param; `execute_apply()` threads `recorder.decision_id()` through

#### Changes

**New repo function**:

```rust
// repo/source.rs
/// Fetch the current decision_id for a source at the given path.
/// Returns None if no source exists at this path, or if decision_id is NULL.
pub fn fetch_decision_id_at_path(
    conn: &Connection,
    root_id: i64,
    rel_path: &str,
) -> Result<Option<i64>> {
    conn.prepare_cached(
        "SELECT decision_id FROM sources WHERE root_id = ? AND rel_path = ? AND present = 1"
    )?
    .query_row(params![root_id, rel_path], |row| row.get(0))
    .optional()
    .map_err(Into::into)
}
```

**`execute_single_transfer()` changes**:

Before:
```rust
fn execute_single_transfer(
    transfer: &ApplyTransfer,
    base_dir: &Path,
    transfer_mode: TransferMode,
    conn: &Connection,
    archive_root_id: i64,
) -> Result<TransferOutcome>
```

After:
```rust
fn execute_single_transfer(
    transfer: &ApplyTransfer,
    base_dir: &Path,
    transfer_mode: TransferMode,
    conn: &Connection,
    archive_root_id: i64,
    decision_id: Option<i64>,
) -> Result<(TransferOutcome, Option<i64>)>  // (outcome, previous_decision_id)
```

The function:
1. Calls `fetch_decision_id_at_path(conn, archive_root_id, &transfer.archive_rel_path)` before the transfer
2. Passes `decision_id` to `build_new_source()` (copy path) and `build_new_source_from_lock()` (resume path)
3. Returns the fetched previous_decision_id alongside the outcome

**`build_new_source()` and `build_new_source_from_lock()` changes**: Both gain a `decision_id: Option<i64>` parameter, used in the `NewSource` construction (replacing the hardcoded `None`).

**`execute_apply()` changes**: Passes `recorder.as_ref().and_then(|r| r.decision_id())` to the transfer loop. The resume "already-there" registration path also passes the decision_id.

#### Tests

- `fetch_decision_id_at_path`: path exists with decision_id → `Some(id)`
- `fetch_decision_id_at_path`: path exists with NULL decision_id → `None`
- `fetch_decision_id_at_path`: path doesn't exist → `None`
- `fetch_decision_id_at_path`: path exists but `present = 0` → `None`
- Apply sets decision_id on destination source (integration test with in-memory DB)
- Re-apply updates decision_id to new decision
- `previous_decision_id` captured from existing record
- `previous_decision_id` is None for new destinations

### Phase 4: Apply receipt integration + docs

**Goal**: Wire everything together — apply writes receipts, CLAUDE.md and docs updated.

**Scope**:
- `ops/apply.rs`: `execute_apply()` collects `ApplyReceiptItem`s during the transfer loop, constructs `ApplyReceipt`, calls `write_receipt()` after loop
- `apply.rs` (interface): construct `ReceiptContext` from archive root info, pass to `execute_apply()`
- `ops/apply.rs`: `execute_apply()` passes `ReceiptContext` to `DecisionRecorder::start()` (replaces `None`)
- CLAUDE.md updates
- docs/ updates

#### Changes

**`execute_apply()` signature change**: Gains `receipt_ctx: Option<&ReceiptContext>` parameter (or receives it via a field on `ApplyExecuteParams`).

Alternative (cleaner): Add `receipt_ctx` as a field on `ApplyExecuteParams`:

```rust
pub struct ApplyExecuteParams {
    // ... existing fields ...
    /// Receipt context for targeted receipt placement. None if receipts disabled.
    pub receipt_ctx: Option<ReceiptContext>,
}
```

**Transfer loop changes** — collect receipt items:

```rust
let decision_id = recorder.as_ref().and_then(|r| r.decision_id());
let mut receipt_items: Vec<ApplyReceiptItem> = Vec::new();

for (i, transfer) in transfers_to_execute.iter().enumerate() {
    let (outcome, prev_decision_id) = execute_single_transfer(
        transfer, &params.base_dir, params.transfer_mode,
        conn, params.archive_root_id, decision_id,
    )?;

    match &outcome {
        TransferOutcome::Copied | TransferOutcome::Renamed | TransferOutcome::Moved => {
            // Look up root path and compute source rel_path for receipt
            receipt_items.push(ApplyReceiptItem {
                source_root: /* root path from transfer.source_path */,
                source_rel_path: /* rel path */,
                destination_rel_path: transfer.archive_rel_path.clone(),
                hash: /* from lock entry hash_value */,
                size: transfer.size,
                mtime: transfer.mtime,
                previous_decision_id: prev_decision_id,
            });
        }
        _ => {}
    }
    // ... rest of existing loop (count updates, progress, interrupt check)
}
```

**After loop, before `recorder.complete()`**:

```rust
// Write receipt if enabled and there are completed items
if let Some(ref recorder) = recorder {
    if let Some(receipt_path) = recorder.receipt_abs_path() {
        if !receipt_items.is_empty() {
            let receipt = ApplyReceipt {
                meta: ReceiptMeta {
                    receipt_version: 1,
                    decision_id: recorder.decision_id().unwrap_or(0),
                    command: "apply".to_string(),
                    timestamp: now(),
                    scope: decision.and_then(|d| d.scope.clone()),
                    reason: decision.and_then(|d| d.reason.clone()),
                    summary: result.summary.clone(),
                    canon_version: env!("CARGO_PKG_VERSION").to_string(),
                    command_line: decision.map(|d| d.command_line.clone())
                        .unwrap_or_default(),
                },
                manifest: Some(params.manifest_display.clone()),
                items: receipt_items,
            };
            if let Err(e) = ops::receipt::write_receipt(
                receipt_path, &receipt, &result.summary,
            ) {
                if let Some(ref mut rec) = recorder {
                    rec.push_warning(format!("Receipt write failed: {e}"));
                }
            }
        }
    }
}
```

**Interface layer (`apply.rs`)** — construct `ReceiptContext`:

```rust
let receipt_ctx = if decision.receipt_enabled {
    Some(ReceiptContext {
        archive_root_id: config.output.archive_root_id,
        archive_root_path: archive_root_path.clone(),
        base_dir_rel: config.output.base_dir.clone(),
    })
} else {
    None
};
```

Pass via `ApplyExecuteParams { ..., receipt_ctx }`.

**Source root/rel_path for receipt items**: `ApplyTransfer` has `source_path` (absolute). We need `source_root` and `source_rel_path`. The root_paths HashMap (already in scope via the plan) can be used: find the root whose path is a prefix of `transfer.source_path`, then strip to get rel_path. Add a small helper or pass the root_paths map to `execute_apply()` via `ApplyExecuteParams`.

Alternatively, since the LockEntry has `root_id` and the root_paths map is available, we can look up the root path. But `execute_apply` doesn't currently have access to lock entries or root_paths. The simplest approach: add `source_root_path: String` and `source_rel_path: String` fields to `ApplyTransfer` during plan computation (where this data is readily available).

#### Tests

- Apply with `receipt_enabled`: receipt `.toml` file exists at expected path after completion
- Receipt content: `[meta]` fields match decision, `manifest` field present, `[[items]]` count matches completed transfers
- Central vs alongside placement: receipt appears at correct path for each layout
- No receipt on `--no-receipt`: decision record saved, no file on disk
- No receipt on dry run: no decision record, no file on disk
- All transfers fail: no receipt file (empty items)
- Partial apply (interrupted): receipt contains only completed items, file is `.toml` (finalized despite interruption)
- `receipt_root_id` and `receipt_rel_path` in DB match the actual file location

## Architectural Decisions

| Decision | Rationale |
|----------|-----------|
| Receipt path uses manifest's `base_dir_rel` | Deterministic, known at start time, semantically correct — this IS the destination directory. No need for scattered-path computation. |
| Receipt path computed at recorder start | Fail-fast on directory creation errors. DB always knows where receipt will be, even for interrupted operations. |
| `previous_decision_id` via per-transfer SELECT | One lightweight query per transfer. Simpler than batch pre-fetch. `insert_destination()` does UPDATE-then-INSERT without returning previous state. |
| `write_receipt()` generic over `Serialize` | Story 3 reuses the writer for exclusion receipts without modification. |
| `ReceiptContext` separate from `DecisionParams` | Different concerns: params describe what decision is being made; receipt context describes where to put the artifact. Non-apply commands won't have receipt context until Story 3. |
| Receipt items only for completed transfers | Partial applies get honest partial receipts. Skipped/failed items are in the decision record counts, not the receipt. |
| Finalize in `recorder.complete()` | Keeps the two-phase lifecycle (`.incomplete` → `.toml`) in one place. Mirrors the DB two-phase pattern. |
| `source_root` + `source_rel_path` in receipt items | Matches the existing sources table split. Human-readable and machine-parseable. |

## Non-Goals

- Exclusion receipts (Story 3)
- Non-targeted receipt placement / `resolve_ledger_root()` (Story 3)
- `common_ancestor()` path utility (not needed — apply uses `base_dir_rel`)
- Receipt consumption/querying (Phase 4 of vision)
- `decision_scopes` population (Story 4)

## Test Plan

### Existing Tests (Must Pass)
All existing tests in `ops/apply.rs`, `repo/source.rs`, `ops/decision.rs`, `domain/path.rs`, and all command-level tests.

### New Tests (~35 total)
- Phase 1: ~14 tests (receipt serialization, path computation, write/finalize)
- Phase 2: ~6 tests (recorder with/without receipt context, complete finalization, DB fields)
- Phase 3: ~8 tests (fetch_decision_id_at_path, decision_id threading, previous_decision_id capture)
- Phase 4: ~7 tests (end-to-end receipt file, content validation, placement, suppression cases)

## Implementation Checklist
- [x] Phase 1: Receipt types and writer (`ops/receipt.rs`)
- [x] Phase 2: DecisionRecorder receipt integration
- [x] Phase 3: Apply decision_id threading + previous_decision_id
- [x] Phase 4: Apply receipt integration + CLAUDE.md + docs
- [x] Verify all existing tests pass

## Documentation Updates

- **CLAUDE.md**: Add `ops/receipt.rs` module description and key types; receipt placement conventions; `ReceiptRef`/`ReceiptContext` in `ops/decision.rs`; `previous_decision_id` semantics for apply; `fetch_decision_id_at_path` in repo/source.rs
- **docs/**: New page for decision receipts (what they contain, where they're stored, layout options); update apply command reference (receipt generation, `--no-receipt`); update config reference (`layout` setting effect on apply)

## Backward Compatibility

No user-visible behavior changes. Receipts are new artifacts that appear alongside existing output. The `--no-receipt` flag is already in place from Story 1. Decision record fields that were previously NULL now get populated.

## Performance Considerations

- One additional lightweight SELECT per transfer (`fetch_decision_id_at_path`) — negligible compared to file copy I/O
- TOML serialization of receipt happens once after all transfers — not in the hot path
- Receipt file size scales linearly with transfer count (a few hundred KB for 10,000 files)
- Directory creation (`create_dir_all` for `.canon-ledger/`) happens once at recorder start

## Version History

| Date | Change |
|------|--------|
| 2026-04-12 | Initial story design |
