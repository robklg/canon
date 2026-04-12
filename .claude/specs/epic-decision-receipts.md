# Epic: Decision Receipts (Decision Provenance Phase 3)

**Design Spec**: [~/store/claude-designs/2026-04-11-decision-receipts.md](~/store/claude-designs/2026-04-11-decision-receipts.md)
**Status**: Pending
**Created**: 2026-04-12

## Objective

Canon's decision provenance system (Phase 1+2) records every effectful action in the `decisions` table — what happened, when, to what scope, with what outcome, and optionally why. But the records are aggregate: "Excluded 42 sources" without listing which 42. "Applied manifest — 35 completed" without recording which files went where.

Receipts complete the recording. A receipt is a TOML file written alongside the archive content, capturing the per-item detail of each decision. The confidence to act — to exclude, to archive, to let go of a drive — comes from knowing the decisions are fully recorded. Not just the summary, but the concrete details. The decision record gives you the *why*, the receipt gives you the *what specifically*. Together they let you replay a decision and validate whether it still makes sense.

Receipts also preserve metadata that may be lost in transit. When a NAS silently drops mtime during copy, the receipt retains the original timestamp — the only durable record of what that value was.

This builds on the decision provenance idea (`~/store/claude-vision/ideas/2026-03-28-decision-provenance.md`) — specifically Phase 3 (Receipts). The ground layer (Phase 1+2) is the foundation; receipts complete the recording.

### Success Criteria

1. A `config.toml` file with `[ledger]` section controls provenance depth, receipt placement, and ledger root
2. Apply operations generate receipts listing every transferred file with source, destination, hash, size, and mtime
3. Exclusion operations generate receipts listing every affected source (with group structure for duplicates)
4. Every source record knows which decision caused its current state — queryable lineage via `decision_id` on the sources table
5. Receipt items capture `previous_decision_id`, creating a chain that reconstructs the full lifecycle of any file
6. A `decision_scopes` table provides durable root-based scope indexing for future consumption
7. Receipt writing never halts a command — graceful degradation on all failures
8. `.canon-ledger/` directories are excluded from scanning
9. `--no-receipt` provides per-invocation receipt suppression; `--no-record` is removed

## Architectural Design

### Overview

Receipts extend the existing decision provenance infrastructure. The `DecisionRecorder` continues to own DB recording. A new receipt writer handles filesystem artifacts. Both share the decision ID and follow the same graceful degradation pattern.

Config loading happens early in `main.rs` (before DB open). The `LedgerConfig` struct threads through `DecisionParams` to ops execute functions — one struct, one flow path. The interface resolves config + CLI flags into two booleans (`record_enabled`, `receipt_enabled`); the ops layer doesn't need to know about config files or CLI flags.

```
┌─────────────────────────────────────────────────────────────┐
│ Interface Layer                                              │
│ - Loads config.toml, parses --no-receipt                    │
│ - Computes record_enabled + receipt_enabled from config+flags│
│ - Constructs DecisionParams with LedgerConfig               │
│ - .canon-ledger/ WalkDir exclusion in scan                  │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│ Operations Layer                                             │
│ - DecisionRecorder.start(): INSERT DB record with receipt   │
│   path (deterministic from decision_id + command)           │
│ - Execute functions collect per-item data during work       │
│ - write_receipt(): .incomplete file on disk                 │
│ - DecisionRecorder.complete(): UPDATE DB, rename            │
│   .incomplete → .toml                                       │
│ - Graceful degradation at every step                        │
└─────────────────────────────────────────────────────────────┘
                          │
          ┌───────────────┼───────────────┐
          ▼               ▼               ▼
┌──────────────────┐ ┌──────────────┐ ┌──────────────────────┐
│ repo/decision.rs  │ │ domain/      │ │ ops/fs.rs            │
│ - insert_started  │ │ config.rs    │ │ - write_file_atomic  │
│   (+ receipt ref) │ │ decision.rs  │ │   (.incomplete →     │
│ - update_completed│ │ path.rs      │ │    .toml rename)     │
│ - insert_scope_   │ │              │ │                      │
│   entries         │ │              │ │                      │
└──────────────────┘ └──────────────┘ └──────────────────────┘
```

### Receipt Write Lifecycle

Receipt writing mirrors the two-phase decision recording pattern:

1. **`recorder.start()`**: INSERT DB record with `receipt_root_id` and `receipt_rel_path` pointing to the final `.toml` name. The path is deterministic (computed from decision_id + command + placement config), so it's known before the file exists.
2. **Work phase**: Execute function does work, collects per-item data.
3. **`write_receipt()`**: Serialize TOML, write to `.incomplete` file (e.g., `000043-apply.incomplete`).
4. **`recorder.complete()`**: UPDATE DB record with status/counts/summary, rename `.incomplete` → `.toml`.

**Crash recovery semantics**:

| DB status | `.toml` | `.incomplete` | What happened |
|-----------|---------|---------------|---------------|
| "completed" | exists | — | Normal completion |
| "started" | — | exists | Interrupted after receipt write but before DB update or rename |
| "started" | — | — | Interrupted before receipt could be written (or no archive root) |

Phase 4 consumption commands can look up `receipt_rel_path` from the DB (always the `.toml` name), try that path, and fall back to checking for `.incomplete` at the same location. An `.incomplete` file tells the user: "this operation was interrupted — the receipt has content but may be partial."

### New Domain Types

**`domain/config.rs`** (new file):
- `RecordingMode` enum — `Full`, `Records`, `Off`
- `ReceiptLayout` enum — `Central`, `Alongside`
- `LedgerConfig` struct — `recording: RecordingMode`, `layout: ReceiptLayout`, `root: Option<i64>`
- Config parsing with per-field validation and fallback to defaults on invalid values

**`domain/decision.rs`** (changes):
- `Decision` struct gains `receipt_root_id: Option<i64>` and `receipt_rel_path: Option<String>`

### New Repository Functions

**`repo/decision.rs`** (changes):
- `insert_started()` gains `receipt_root_id: Option<i64>` and `receipt_rel_path: Option<&str>` parameters — receipt path is written at start time (deterministic from decision_id + command)
- New `insert_scope_entries(conn, decision_id, entries: &[(i64, String)])` for scope index

### New Operations Functions

**`ops/receipt.rs`** (new file):
- `ReceiptMeta` — shared meta section for all receipt types (with `receipt_version: u32`)
- `ReceiptPlacement` enum — `LedgerRoot { root_path }` or `Targeted { archive info, layout }`
- `ReceiptRef` struct — `root_id: i64`, `rel_path: String`
- `write_receipt<T: Serialize>()` — generic writer: serialize TOML, prepend comment header, write to `.incomplete`, graceful degradation
- `finalize_receipt()` — rename `.incomplete` → `.toml` (called during recorder.complete())
- `resolve_ledger_root()` — find the archive root for non-targeted receipts
- `compute_receipt_path()` — filename formatting, directory computation per placement
- Per-command receipt types: `ApplyReceipt`, `ExcludeReceipt`, `DuplicatesReceipt` (each with typed item structs)
- All receipt item types include `previous_decision_id: Option<i64>` — the decision_id that was on the source record before this decision overwrote it (creates the provenance chain)

**`ops/decision.rs`** (changes):
- `DecisionParams` gains `record_enabled: bool`, `receipt_enabled: bool`, `ledger_config: LedgerConfig` (replaces single `enabled: bool`)
- `DecisionRecorder.start()` computes and stores receipt path, writes `receipt_root_id`/`receipt_rel_path` to DB at INSERT time
- `DecisionRecorder.decision_id()` — exposed for receipt writer
- `DecisionRecorder.complete()` renames `.incomplete` → `.toml` as part of completion
- Scope decomposition logic for `decision_scopes` population

**`ops/fs.rs`** (addition):
- `write_file_atomic(path, content)` — write to `.incomplete` path; caller renames to final name on completion. Deterministic naming preserves interrupted receipts as recoverable evidence. No `tempfile` crate needed.

### Schema Changes

**`sources` table** gains a nullable column (in `CREATE TABLE IF NOT EXISTS`):
- `decision_id INTEGER` — the decision that caused the most recent state transition on this source record. See [Source Decision Link spec](~/store/claude-designs/2026-04-12-destination-provenance.md) for full semantics, design principles, and per-command behavior.

**`decisions` table** gains two nullable columns (in `CREATE TABLE IF NOT EXISTS`):
- `receipt_root_id INTEGER` — references roots table (dangling references acceptable)
- `receipt_rel_path TEXT` — relative path within the receipt root (always the final `.toml` name; written at decision start time since the path is deterministic)

**New `decision_scopes` table**:
```sql
CREATE TABLE IF NOT EXISTS decision_scopes (
    decision_id INTEGER NOT NULL,
    root_id INTEGER NOT NULL,
    rel_prefix TEXT NOT NULL DEFAULT ''
);
```
Indexed on both `decision_id` and `root_id`.

**Migration for existing databases**: Manual one-time SQL (2 installs):
```sql
ALTER TABLE decisions ADD COLUMN receipt_root_id INTEGER;
ALTER TABLE decisions ADD COLUMN receipt_rel_path TEXT;
ALTER TABLE sources ADD COLUMN decision_id INTEGER;
```

### Receipt Format

All receipts share a `[meta]` section and use `receipt_version: 1`. Fields are append-only — never removed, never renamed. Additive changes (new optional fields) don't bump the version. Structural changes bump the version. Same forward-compatibility pattern as manifests.

**Standard receipt** (exclude set/clear, set-object):
```toml
# Canon Decision Receipt
# Excluded 42 sources across 1 root

[meta]
receipt_version = 1
decision_id = 42
command = "exclude-set"
timestamp = 1744300200
scope = ["/Volumes/old-laptop/Users/rob"]
reason = "OS files, no personal value"
summary = "Excluded 42 sources across 1 root"
canon_version = "0.4.1"
command_line = "canon exclude set --where 'source.ext=dll' --reason '...'"

[[items]]
root = "/Volumes/old-laptop"
rel_path = "Users/rob/Windows/System32/ntdll.dll"
hash = "sha256:a1b2c3d4..."
size = 1982464
mtime = 1700000000
previous_decision_id = 12
```

**Apply receipt**:
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

**Duplicates receipt**:
```toml
# Canon Decision Receipt
# Excluded 847 duplicate sources (312 groups) across 2 roots

[meta]
receipt_version = 1
decision_id = 51
command = "exclude-duplicates"
timestamp = 1744387200
scope = ["/Volumes/old-laptop"]
reason = "Keeping highest-quality copies"
summary = "Excluded 847 duplicate sources (312 groups) across 2 roots"
canon_version = "0.4.1"
command_line = "canon exclude duplicates --reason '...'"

[[groups]]
hash = "sha256:abc123..."

[[groups.kept]]
root = "/Volumes/old-laptop"
rel_path = "Photos/original/IMG_001.jpg"
size = 3456789
mtime = 1700000000

[[groups.excluded]]
root = "/Volumes/old-laptop"
rel_path = "Photos/copies/IMG_001_copy.jpg"
size = 3456789
mtime = 1700000000
previous_decision_id = 12
```

### Receipt Placement

**Non-targeted receipts** (exclusions — decisions that don't target a specific archive location):
- Written to the ledger root's `.canon-ledger/` directory
- Ledger root: configured archive root ID, or lowest-ID archive root
- Flat structure — no subdirectories

**Targeted receipts** (apply — decisions that write to a specific archive location):
- Written to the target archive's `.canon-ledger/` directory
- `central` layout: `.canon-ledger/{base_dir_rel}/` at archive root (uses the manifest's configured base directory)
- `alongside` layout: `.canon-ledger/` subdirectory at the destination directory itself

**Filename format**: `{decision_id:06}-{command}.toml` (6-digit zero-padded)

**No archive root**: Receipt not generated. Warning emitted: "No archive root configured — decision details not preserved. Create an archive root to enable receipt generation." Decision record still saved. Command proceeds.

### Config File

`$CANON_HOME/config.toml` — created on first Canon run with defaults and inline documentation. Canon emits a one-time message pointing to the file.

Three settings in `[ledger]`:

| Setting | Values | Default | Controls |
|---------|--------|---------|----------|
| `recording` | `"full"`, `"records"`, `"off"` | `"full"` | Provenance depth: DB + receipts, DB only, or nothing |
| `layout` | `"central"`, `"alongside"` | `"central"` | Where receipt files are placed within archive |
| `root` | archive root ID | lowest-ID archive | Which archive holds non-targeted receipts |

**Recording mode matrix**:

| Config `recording` | `--no-receipt` | DB record | Receipt |
|--------------------|----------------|-----------|---------|
| `"full"` | No | Yes | Yes |
| `"full"` | Yes | Yes | No |
| `"records"` | — | Yes | No |
| `"off"` | — | No | No |

Config loads before DB open (parse-time validation for values). Root validation happens after DB open (semantic validation — must be archive, not source). Invalid/missing config falls back to defaults with warnings. Config errors never prevent Canon from operating.

### CLI Changes

- **Remove `--no-record`**: Clean break (pre-1.0). Config `recording = "off"` replaces it.
- **Add `--no-receipt`**: Global flag (`global = true`). Suppresses receipt generation for one invocation. DB record still written when config allows.

### Scan Exclusion

`.canon-ledger/` directories excluded from scanning. Built into WalkDir `filter_entry()` in `src/scan.rs` (interface layer — consistent with scan architecture that interface owns walk config). Applies to all roots (source and archive). Hardcoded, not user-configurable.

## Stories

| # | Story | Scope | Dependencies | Status |
|---|-------|-------|--------------|--------|
| 1 | Infrastructure Foundation | Config, schema (all 3 tables), scan exclusion, CLI flag migration, recording mode, atomic write, source decision_id column | — | Completed |
| 2 | Apply Receipts | Receipt format, path computation, receipt writer, apply integration, destination decision_id + previous_decision_id chain | Story 1 | Completed |
| 3 | Exclusion Receipts | Plan enrichment, per-command receipt content, duplicates group format, exclusion decision_id + previous_decision_id chain | Stories 1 + 2 | Pending |
| 4 | Durable Scope Index | decision_scopes population, scope decomposition, scan timing | Story 1 (schema) | Pending |

### Story 1: Infrastructure Foundation

**Objective**: Establish everything the receipt stories need — config loading, schema, recording mode, scan exclusion, CLI flags, atomic write primitive, and the source decision_id column — without writing any receipts yet.

**Scope**:
- `domain/config.rs` — `LedgerConfig`, `RecordingMode`, `ReceiptLayout`, parsing with per-field validation and fallback
- `repo/db.rs` — `receipt_root_id` + `receipt_rel_path` columns on decisions, `decision_id` column on sources, `decision_scopes` table (in `CREATE TABLE IF NOT EXISTS`)
- `domain/decision.rs` — `Decision` struct gains receipt fields
- `domain/source.rs` — `Source` struct gains `decision_id: Option<i64>`
- `repo/source.rs` — `NewSource` struct gains `decision_id: Option<i64>`; `insert_destination()` gains `decision_id` parameter; scan INSERT path sets `decision_id`
- `repo/decision.rs` — `insert_started()` gains receipt path params (written at start time)
- `ops/decision.rs` — `DecisionParams` split: `record_enabled` + `receipt_enabled` + `ledger_config`; `DecisionRecorder.start()` computes and stores receipt path; `complete()` renames `.incomplete` → `.toml`; expose `decision_id()`
- `ops/fs.rs` — `write_file_atomic()` (write to `.incomplete`, deterministic naming)
- `main.rs` — config loading/creation before DB open, `--no-receipt` global flag, remove `--no-record`, config root validation after DB open, thread config to all command modules
- `src/scan.rs` — `.canon-ledger/` WalkDir `filter_entry()` exclusion; scan INSERT path passes `decision_id` for new files (conservative: UPDATEs do not touch `decision_id`)
- All command modules — update `DecisionParams` construction for `record_enabled`/`receipt_enabled`/`ledger_config` (mechanical, no behavioral change)
- One-time migration SQL for existing databases

**Key decisions**:
- Config loads before DB, root validates after DB
- `recording = "off"` replaces `--no-record` for DB record suppression
- `--no-receipt` is the only per-invocation override
- `.canon-ledger/` exclusion is in the interface layer (WalkDir config)
- Scan sets `decision_id` on INSERT only (new file discovery); UPDATE paths (Modified, Moved, Unchanged) omit it, preserving existing provenance
- No receipts written in this story — infrastructure only

### Story 2: Apply Receipts

**Objective**: Build the central receipt writing facility and deliver the first receipt type, including the destination-side decision link. Apply is first because it has the richest per-item data already available, exercises targeted placement logic (central/alongside), and is the most valuable receipt. The decision_id on destination sources makes every archived file directly traceable to the decision that placed it.

**Scope**:
- `ops/receipt.rs` (new) — `ReceiptMeta`, `ApplyReceipt`, `ApplyReceiptItem`, `ReceiptPlacement`, `ReceiptRef`
- `ops/receipt.rs` — `write_receipt()` generic writer (serialize, comment header, atomic write, graceful degradation)
- `ops/receipt.rs` — `resolve_ledger_root()`, `compute_receipt_path()`, filename formatting
- `ops/receipt.rs` — Targeted placement: central (using manifest `base_dir`) and alongside
- `ops/apply.rs` — collect per-item data during transfer loop (source root, source rel_path, dest rel_path, hash, size, mtime), call `write_receipt()` after completion, pass `ReceiptRef` to recorder
- `ops/apply.rs` — pass `recorder.decision_id()` to `insert_destination()` for each completed transfer, setting `decision_id` on the destination source record
- `ops/apply.rs` — read existing `decision_id` from destination source (returned by `insert_destination()`) before overwrite, populate `previous_decision_id` on each receipt item
- Receipt format: `receipt_version: 1`, `[meta]` with `manifest` field, `[[items]]` with all fields including `previous_decision_id`

**Key decisions**:
- Receipt writer is generic over `Serialize` — Story 3 reuses it without modification
- Only completed transfers appear in receipts (partial applies get partial receipts)
- No receipt on dry run, all-failures, or `--no-receipt`
- Ledger root resolution happens at receipt write time (deferred validation)
- mtime preserved per item (guards against destination metadata loss)
- `insert_destination()` always sets `decision_id` — apply is intentional placement, re-apply updates provenance
- `previous_decision_id` is read from the existing record via a lightweight SELECT before the transfer overwrites it

### Story 3: Exclusion Receipts

**Objective**: Deliver receipts for all exclusion operations, exercising non-targeted (flat) placement, the special group format for duplicates, and the exclusion-side decision link. Each excluded or un-excluded source becomes directly traceable to the decision that changed its state.

**Scope**:
- Plan struct enrichment — add hash + size + mtime + `decision_id` (current, for `previous_decision_id` in receipts) to `ExcludeSetPlan`, `ExcludeClearPlan`, `ExcludeDuplicatesPlan`, `ExcludeSetObjectsPlan` (one batch object fetch during plan computation)
- `ops/receipt.rs` — `ExcludeReceipt`, `ExcludeReceiptItem`, `DuplicatesReceipt`, `DuplicateGroupEntry`, `DuplicateSourceEntry`
- `ops/receipt.rs` — `ReceiptPlacement::LedgerRoot` (non-targeted, flat)
- `ops/exclude.rs` — receipt writing in `execute_set`, `execute_clear`, `execute_duplicates`, `execute_set_objects`, `execute_set_object`, `execute_clear_object`
- `ops/exclude.rs` — UPDATE `decision_id` on each affected source alongside the `excluded` field change; read current `decision_id` from plan data for `previous_decision_id` in receipt items
- Edge cases: no archive root → warning + no receipt; unhashed sources → hash field reflects this

**Key decisions**:
- Plan enrichment adds one batch query per plan computation (objects by object_id for hash values)
- Plan structs carry each source's current `decision_id` — no extra query for `previous_decision_id`
- Duplicates use `[[groups]]` with `kept`/`excluded` sub-arrays — distinct from `[[items]]`; only excluded sources get `previous_decision_id` (kept sources' state didn't transition)
- Single-target operations (set_by_id, set_by_path, set_object_by_hash, set_object_by_file, clear_object) also generate receipts and update `decision_id`
- `exclude set/clear/duplicates` UPDATE `decision_id` in the same SQL that sets `excluded`; `set-object/clear-object` UPDATE `decision_id` on all sources sharing the object

### Story 4: Durable Scope Index

**Objective**: Populate the `decision_scopes` table from already-resolved scope data, enabling future Phase 4 consumption commands to query decisions by root.

**Scope**:
- `repo/decision.rs` — `insert_scope_entries(conn, decision_id, entries: &[(i64, String)])`
- `ops/decision.rs` — scope decomposition: given scope paths + roots, compute `(root_id, rel_prefix)` pairs using `find_containing_root()`
- `DecisionRecorder` integration — scope entries written at `start()` for most commands, at `complete()` for scan (root may not exist at start)
- Global operations — no entries (deferred to Phase 4 design)
- All command modules — thread `&[Root]` to recorder

**Key decisions**:
- Scope entry failure warns, doesn't halt
- Scan writes entries after root creation (at complete time)
- Empty rel_prefix for root-level scope (consistent with sources table)
- No backfill of existing decisions

## Architectural Decisions

| Decision | Rationale |
|----------|-----------|
| `decision_id` = state transition provenance | Not "who created" (too narrow) or "who last touched" (too broad). The decision that caused the source to be in its current state. See [Source Decision Link spec](~/store/claude-designs/2026-04-12-destination-provenance.md). |
| Receipt recording rule = decision_id update rule | If a state transition appears as a receipt item, it updates `decision_id`. If not, it doesn't. Two views of the same event — receipt is the durable detail, decision_id is the queryable link. |
| `previous_decision_id` in receipt items | Creates a provenance chain. Source record is current state; receipts are history. Walking backward reconstructs full lifecycle. Chain degrades gracefully — missing link loses one hop, not all history. |
| Per-command decision_id semantics | Each command's receipt design determines its decision_id update behavior. Scan deferred until scan receipts are designed. Principles apply uniformly; rules are command-specific. |
| Config threads through `DecisionParams` | Single struct flow from interface to ops. No new parameters on execute functions beyond what already flows. |
| Receipt writer is generic over `Serialize` | One writer, many receipt types. Format consistency by construction. |
| `receipt_version: 1` in meta | Same forward-compatibility pattern as manifests. Additive-only field policy. Structural changes bump version. |
| Fields append-only, never removed/renamed | Same principle as `DecisionCommand` strings. Receipts are permanent artifacts. |
| Receipts written in ops, between work and complete() | Ops owns behavior. Receipt path computation needs config + root info (ops concerns). |
| Plan enrichment for exclude (one batch query) | Keeps receipt writer as pure serializer. No DB access at receipt write time. |
| `.canon-ledger/` exclusion in WalkDir filter | Interface owns walk configuration (established scan architecture). |
| `CREATE TABLE` for schema, manual migration | Two existing installs. No migration system needed. |
| Config validates root after DB open | Parse-time for value validity, use-time for semantic validity (is it an archive?). |
| 6-digit zero-padded decision ID in filenames | Covers 999,999 decisions. Beyond that still works, just wider. |
| mtime preserved in receipt items | Guards against destination metadata loss (observed with NAS devices). |
| Separate receipt writer from DecisionRecorder | Recorder owns DB I/O, writer owns filesystem I/O. Different concerns, same degradation pattern. |
| Receipt path written to DB at start time | Path is deterministic (decision_id + command + placement). Writing early means the DB always knows where the receipt should be, even for interrupted operations. |
| `.incomplete` → `.toml` rename lifecycle | Mirrors two-phase decision recording. `.incomplete` files survive crashes as recoverable evidence. Phase 4 consumption can distinguish complete vs interrupted receipts by checking both extensions. |
| `--no-record` removed (clean break) | Pre-1.0. Config `recording` setting is the proper control for DB recording. Per-invocation override targets receipts only. |

## Cross-Cutting Concerns

- **`DecisionParams` changes**: Every command module that constructs `DecisionParams` needs updating for the new fields. Compiler-enforced — mechanical change, moderate blast radius.
- **Config availability**: Loaded once in `main.rs`, threaded to all command modules. Same flow as `command_line` and `no_record` today.
- **No new crate dependencies**: Atomic writes use deterministic `.incomplete` naming with `std::fs` — no `tempfile` crate needed.
- **Recording mode vs receipt mode**: Two independent booleans. A command can record to DB without writing a receipt (recording=records), or neither (recording=off). The `--no-receipt` flag only suppresses receipts.
- **Graceful degradation everywhere**: Config errors → defaults with warning. No archive root → skip receipt with warning. Receipt write failure → warning, command proceeds. Scope index failure → warning. This pattern is consistent with the existing `DecisionRecorder` behavior.

## Test Strategy

### Story 1 (~25 tests)
- Config parsing: valid, invalid, partial, unknown keys, each value for each field
- Schema: receipt columns round-trip, decision_scopes table existence, sources.decision_id column exists
- DecisionRecorder: record_enabled/receipt_enabled split, complete with ReceiptRef
- Scan exclusion: .canon-ledger at root, nested, not similar names
- Recording mode matrix: off→no record, records→DB only, full→both
- Source decision_id: scan INSERT sets decision_id on new sources
- Source decision_id: scan UPDATE (Modified, Moved, Unchanged) preserves existing decision_id
- Source decision_id: insert_destination sets decision_id
- Source decision_id: NULL when recording disabled

### Story 2 (~25 tests)
- Receipt serialization: meta round-trip, version field, optional fields omitted, comment header
- Path computation: non-targeted flat, targeted central mirrored, targeted alongside, filename padding
- Apply integration: receipt file exists, all items present, partial only completed, no receipt on dry-run/all-failures/--no-receipt, meta has manifest, central vs alongside placement
- Ledger root resolution: no archives, default lowest-ID, configured valid, configured invalid
- Atomic write: creates file, no partial on error
- Decision link: destination sources have decision_id set after apply
- Decision link: re-apply to same path updates decision_id
- Provenance chain: receipt items contain previous_decision_id from existing record
- Provenance chain: previous_decision_id is NULL for new destinations
- Provenance chain: previous_decision_id captures prior apply decision on re-apply

### Story 3 (~25 tests)
- Plan enrichment: hash, size, mtime, and decision_id present in plan items
- Exclude receipt content: items match plan, at ledger root, no archive warning
- Duplicates: group structure, all groups present, hash per group
- Set-object: lists all affected sources, clear-object receipt
- Decision link: excluded sources have decision_id updated after execute_set
- Decision link: un-excluded sources have decision_id updated after execute_clear
- Decision link: exclude duplicates updates decision_id on excluded, not on kept
- Decision link: object-level exclusion updates decision_id on all affected sources
- Provenance chain: receipt items contain previous_decision_id
- Provenance chain: chain walkback across exclude→scan discovery

### Story 4 (~10 tests)
- Scope entries: single root, root-level, multiple roots, global produces none
- Scan timing: entries after root creation, existing root at start
- Resilience: failure warns

### Existing Tests (Must Pass)
All existing tests. Receipt infrastructure is additive — no existing behavior changes except `--no-record` removal.

## Documentation Updates

- **New page**: Decision receipts — what they contain, where they're stored, layout options, config settings
- **Update**: Decision provenance page — recording modes, config reference
- **Update**: Command reference pages — `--no-receipt` flag, `--no-record` removal
- **Update**: Config reference (or new page) — `config.toml` with `[ledger]` section
- **CLAUDE.md**: New domain/config.rs, ops/receipt.rs modules; receipt columns + decision_scopes table; `--no-receipt` in CLI flag vocabulary; receipt placement conventions; config.toml in Canon Home section

## Non-Goals

- Scan receipts (what went missing) — future story
- Roots-rm receipts — future story
- Cluster generate/refresh receipts — not specified
- Receipt consumption/querying (Phase 4 of vision)
- Global operation scope indexing — deferred to Phase 4 design
- Backfilling existing decision records with scope entries
- ctime/birthtime capture (separate scope — scan pipeline change)

## Risks

| Risk | Severity | Mitigation |
|------|----------|------------|
| Story 1 blast radius (every command module) | Medium | Mechanical change, compiler-enforced. One phase for threading. |
| Config file becomes support surface | Low | Self-documenting defaults, fall-back-on-error. Users rarely edit. |
| `--no-record` removal breaks scripts | Low | Pre-1.0, document in changelog. |
| Plan enrichment adds latency | Low | One batch query, same pattern used everywhere. |
| Receipt format is a compatibility surface | Medium | `receipt_version` + additive-only policy. Same as manifests. |
| `.incomplete` files accumulate on repeated crashes | Low | Rare scenario. Phase 4 or a future `canon ledger` command can surface and clean up stale `.incomplete` files. |

## Version History

| Date | Change |
|------|--------|
| 2026-04-12 | Initial epic design |
| 2026-04-12 | Integrated source decision link: decision_id on sources table, previous_decision_id in receipt items, four lineage design principles, per-command behavior for apply/exclude/scan |
