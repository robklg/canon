# Story: Exclusion Receipts

**Design Spec**: [~/store/claude-designs/2026-04-11-decision-receipts.md](~/store/claude-designs/2026-04-11-decision-receipts.md) (Story 3)
**Epic**: [.claude/specs/epic-decision-receipts.md](epic-decision-receipts.md)
**Status**: Pending
**Created**: 2026-06-13

## Objective

Give exclusion operations the same durable provenance that apply already has. Today `apply` writes a receipt recording exactly which files went where; exclusions write nothing per-item — "Excluded 42 sources" with no record of *which* 42, their content, or why.

This asymmetry is not cosmetic. Triage interleaves archiving and excluding — the user holds a location and decides, in the same breath, what belongs (archive) and what doesn't (exclude). When only archiving leaves a trail, every exclusion is an act of faith, so the rational response is to *not* exclude — which means the user never actually triages, just archives and leaves the junk behind feeling unfinished. **Complete provenance across both halves of triage is the precondition for using Canon to resolve a drive at all.** This story closes the exclusion half.

After this story, every exclusion state transition leaves two linked records: a `decision_id` on the affected source (the live "what put this here" pointer) and a receipt item (the durable per-item detail — hash, size, mtime, and `previous_decision_id` for the walkable history chain).

## Functional Requirements Summary

From the functional spec, Story 3:

- `exclude set`, `exclude clear`, `exclude duplicates`, object-level exclusion (`set-object`/`clear-object`), and single-source `set_by_id`/`set_by_path` each generate a receipt.
- Exclusion receipts are **non-targeted**: flat in the ledger root's `.canon-ledger/` (no destination subdirectory).
- Each item records: source root path, source rel_path, content hash, file size (and — per the epic's integration of the source decision link — mtime + `previous_decision_id`).
- `exclude duplicates` uses a group structure recording which copy was **kept** vs **excluded**.
- `exclude clear` documents what was un-excluded (restored visibility).
- Object-level exclusion lists the content hash and all sources sharing it.
- The decision record's `receipt_root_id` / `receipt_rel_path` are populated.
- Edge cases: no archive root → warning + no receipt + decision still recorded; ledger root inaccessible → graceful degradation.

**Decisions made during `/design` (2026-06-13):**

1. **Realign with `ReceiptPlacement`.** Story 2 deferred the epic's planned `ReceiptPlacement` enum and shipped a targeted-only `ReceiptContext` struct. This story builds the enum as the epic intended, bringing the code back in line with the contract.
2. **Uniform coverage.** Per the epic principle *"receipt recording rule = decision_id update rule,"* every exclusion state transition gets **both** a `decision_id` update and a receipt — including single-source ops. A one-item receipt still captures the durable hash/size/mtime and keeps the provenance chain unbroken; the cost is trivial.
3. **Surface receipt-write warnings.** Exclusion (and apply) receipt-write failures must warn the user — a silently-missing receipt undermines the "you can trust it's recorded" premise. This also fixes a latent gap: `execute_apply` currently pushes a `"Receipt write failed"` warning into a recorder that is never drained.

**Two gaps in the epic this story closes** (surfaced in design, not silently absorbed):
- The epic's `LedgerRoot { root_path }` lacks the `root_id` needed for the `receipt_root_id` column → the variant carries both.
- The epic enumerated `ExcludeReceipt` + `DuplicatesReceipt` but no object-level receipt type, while the functional spec requires object exclusions to use a distinct structure → a third type, `ObjectExcludeReceipt`, is added.

## Current State

- **`ops/receipt.rs`** — generic `write_receipt<T: Serialize>()` + `finalize_receipt()` (reused unchanged). `ApplyReceipt`/`ApplyReceiptItem`, `ReceiptMeta` (shared), `receipt_filename()`, `compute_targeted_receipt_rel_path()`. No placement abstraction, no `resolve_ledger_root`.
- **`ops/decision.rs`** — `DecisionRecorder` (two-phase `start`/`complete`, graceful degradation, `take_warnings()`). `ReceiptContext { archive_root_id, archive_root_path, base_dir_rel }` (targeted only). `start(conn, params, Option<&ReceiptContext>)` → `compute_and_register_receipt()` computes targeted path only.
- **`ops/exclude.rs`** — plan/execute for `set`/`clear`/`duplicates`/`set_objects`, plus single-target `execute_set_source`/`execute_set_object`/`execute_clear_object` and their `check_*` validators. Plans carry parallel `source_ids: Vec<i64>` + `paths: Vec<String>`. Execute fns create the recorder internally via `DecisionRecorder::start(conn, d, None)` and call `repo::source::set_excluded(conn, id, excluded)` (no `decision_id`). Result structs (`ExcludeSetResult`, etc.) carry `count` + `summary`, no `warnings`.
- **`ops/apply.rs`** — `execute_apply(...) -> Result<ApplyResult>`; recorder built from `ApplyExecuteParams.receipt_ctx: Option<ReceiptContext>`; pushes receipt-write warnings (apply.rs:970) but `ApplyResult` has no `warnings` field and `src/apply.rs` never drains them — **silent drop**.
- **`repo/source.rs`** — `set_excluded(conn, source_id, excluded)`, `batch_set_excluded(conn, ids, excluded)` (currently `#[allow(dead_code)]`). `sources.decision_id` column exists; `fetch_decision_id_at_path()` exists (used by apply).
- **`repo/object.rs`** — `set_excluded(conn, object_id, excluded)` (flips `objects.excluded`).
- **`domain/exclusion.rs`** — `find_excludable_duplicates() -> ExcludableDuplicatesResult { to_exclude: Vec<i64>, skipped_* }` — **no kept/group structure**.
- **`domain/decision.rs`** — `DecisionCommand` already has `ExcludeSet`/`ExcludeClear`/`ExcludeDuplicates`/`ExcludeSetObject`/`ExcludeClearObject` with `as_str()` → `exclude_set` etc.
- **`src/exclude.rs`** — threads `LedgerConfig`; `make_decision_params()` builds `DecisionParams`; calls execute fns with `Some(&decision)`. Command→`DecisionCommand` mapping confirmed (set/set_by_id/set_by_path → `ExcludeSet`; clear → `ExcludeClear`; duplicates → `ExcludeDuplicates`; set-object variants → `ExcludeSetObject`; clear-object → `ExcludeClearObject`).
- Baseline: branch `master`, clean tree, **1057 tests green**.

## Design

### Receipt placement, filename, and format conventions

- **Filename + `[meta].command`** both use `DecisionCommand::as_str()` (underscore form, e.g. `exclude_set`), matching the `decisions.command` column and shipped apply. Filename: `{decision_id:06}-{command}.toml`, e.g. `000042-exclude_set.toml`. (The epic's hyphenated examples — `0042-exclude-set.toml` — were illustrative; underscores are the real convention.)
- **Placement**: exclusions are non-targeted → flat at the ledger root: `.canon-ledger/{filename}`. No `base_dir` subdirectory, no `layout` dependence (layout only affects targeted/apply receipts).
- **`[meta]`** uses the existing `ReceiptMeta` (`receipt_version = 1`, `decision_id`, `command`, `timestamp`, `scope?`, `reason?`, `summary`, `canon_version`, `command_line`). `manifest` is `None` (omitted) for exclusion receipts.

Three receipt bodies:

```toml
# ExcludeReceipt — exclude_set, exclude_clear, set_by_id, set_by_path
[[items]]
root = "/Volumes/old-laptop"
rel_path = "Users/rob/Windows/System32/ntdll.dll"
hash = "sha256:a1b2c3..."        # omitted if unhashed
size = 1982464
mtime = 1700000000
previous_decision_id = 12         # omitted if None

# DuplicatesReceipt — exclude_duplicates
[[groups]]
hash = "sha256:abc123..."
[[groups.kept]]                   # the prefer-prefix copy/copies; no previous_decision_id (no transition)
root = "/Volumes/old-laptop"
rel_path = "Photos/original/IMG_001.jpg"
size = 3456789
mtime = 1700000000
[[groups.excluded]]
root = "/Volumes/old-laptop"
rel_path = "Photos/copies/IMG_001_copy.jpg"
size = 3456789
mtime = 1700000000
previous_decision_id = 12         # omitted if None

# ObjectExcludeReceipt — exclude_set_object (filter + single), exclude_clear_object
[[objects]]
hash = "sha256:def456..."
[[objects.sources]]               # all present sources sharing the hash
root = "/Volumes/old-laptop"
rel_path = "Downloads/dup.bin"
size = 4096
mtime = 1700000000
previous_decision_id = 12         # omitted if None
```

---

### Phase 1: Placement realignment + apply warning fix

- **Goal**: Build `ReceiptPlacement`, teach the recorder both placement shapes, and surface receipt-write warnings — with apply as the only consumer, behavior unchanged except that failures now warn.
- **Scope**: `ops/receipt.rs`, `ops/decision.rs`, `ops/apply.rs`, `src/apply.rs`, and the ~10 `ReceiptContext` test sites.

#### Changes

`ops/receipt.rs` — new placement abstraction:
```rust
pub enum ReceiptPlacement {
    Targeted { archive_root_id: i64, archive_root_path: String, base_dir_rel: String },
    LedgerRoot { root_id: i64, root_path: String },
}

/// Resolve which archive root holds non-targeted receipts.
/// `config.root` if it names an active archive root, else the lowest-id active
/// archive root, else None (no archive → caller warns, no receipt).
pub fn resolve_ledger_root(roots: &[Root], config: &LedgerConfig) -> Option<(i64, String)>;

/// Flat rel_path for non-targeted receipts: `.canon-ledger/{filename}`.
pub fn compute_ledger_root_receipt_rel_path(decision_id: i64, command: &str) -> String;
```

`ops/decision.rs`:
- Replace `ReceiptContext` with `ReceiptPlacement`. `start()` takes `Option<&ReceiptPlacement>`.
- `compute_and_register_receipt()` branches: `Targeted` → `compute_targeted_receipt_rel_path` + abs base = `archive_root_path`, `receipt_root_id = archive_root_id`; `LedgerRoot` → `compute_ledger_root_receipt_rel_path` + abs base = `root_path`, `receipt_root_id = root_id`. Directory creation, DB registration (`update_receipt_path`), and abs-path stashing are shared across both arms.

`ops/apply.rs`:
- `ApplyExecuteParams.receipt_ctx: Option<ReceiptContext>` → `Option<ReceiptPlacement>` (interface builds `Targeted`).
- `ApplyResult` gains `pub warnings: Vec<String>`. After `recorder.complete()`, `result.warnings = recorder.take_warnings()`.

`src/apply.rs`:
- Build `ReceiptPlacement::Targeted { .. }` instead of `ReceiptContext`.
- After printing the summary, print `result.warnings` to stderr (e.g. each line prefixed, consistent with scan/import-facts).

#### Tests
- Path: `compute_ledger_root_receipt_rel_path` → `.canon-ledger/000042-exclude_set.toml`.
- `resolve_ledger_root`: no archives → None; lowest-id default; configured valid; configured-invalid → fallback.
- Recorder: `LedgerRoot` placement sets `receipt_ref`/abs path/`receipt_root_id` to the ledger root, flat rel_path.
- Apply: forced receipt-write failure surfaces a warning in `ApplyResult.warnings` (closes the latent gap).
- All existing apply + recorder tests migrated to `ReceiptPlacement::Targeted` and green.

---

### Phase 2: Repo `decision_id` threading + plan enrichment

- **Goal**: Make `decision_id` writable alongside exclusion, and carry per-item receipt data through the plans.
- **Scope**: `repo/source.rs`, `ops/exclude.rs` (plans), `domain/exclusion.rs` (group reconstruction happens in the plan, not the domain fn).

#### Changes

`repo/source.rs`:
```rust
pub fn set_excluded(conn, source_id: i64, excluded: bool, decision_id: Option<i64>) -> Result<()>;       // UPDATE excluded, decision_id
pub fn batch_set_excluded(conn, source_ids: &[i64], excluded: bool, decision_id: Option<i64>) -> Result<u64>; // same, chunked; drop #[allow(dead_code)]
pub fn set_decision_id_by_object(conn, object_id: i64, decision_id: Option<i64>) -> Result<u64>;          // UPDATE sources SET decision_id WHERE object_id=? (object-level)
```

`ops/exclude.rs` — plan enrichment via a single item type as source of truth:
```rust
pub struct ExcludeItemData {
    pub source_id: i64,
    pub root: String,                       // source.root_path (absolute)
    pub rel_path: String,
    pub hash: Option<String>,               // formatted as ApplyReceiptItem.hash; None if unhashed
    pub size: i64,
    pub mtime: i64,
    pub previous_decision_id: Option<i64>,  // source.decision_id before this op
}
```
- `ExcludeSetPlan` / `ExcludeClearPlan`: replace `source_ids`/`paths` fields with `items: Vec<ExcludeItemData>`; add derived accessors `source_ids()` / `paths()`. (Interface dry-run/confirmation + execute loop migrate from field to method access.)
- `ExcludeDuplicatesPlan`: add `groups: Vec<DuplicateGroupData { hash: String, kept: Vec<ExcludeItemData>, excluded: Vec<ExcludeItemData> }>`; `source_ids()`/`paths()` derive from `groups[*].excluded`. Reconstruct groups in `plan_duplicates`: group `to_exclude` by `object_id`, pull kept copies (those under `prefer_prefix`) from the already-fetched `sources_by_object`. Existing skip-count/`group_count`/`scope_count` fields unchanged.
- `ExcludeSetObjectsPlan` (`ObjectPlanEntry`): enrich each entry's sources from `ObjectSourceInfo { path, is_archive }` to also carry root/rel_path/size/mtime/`previous_decision_id` (receipt-capable). Existing skip/total fields unchanged.
- One batch `repo::object::batch_fetch_by_ids` per plan resolves hash values; size/mtime/`object_id`/`decision_id` come from the `Source` objects already in the plan. No new per-item queries.

#### Tests
- Each plan's items carry hash/size/mtime and `previous_decision_id` (= prior `decision_id`).
- `source_ids()`/`paths()` accessors return the same data the old fields did (regression guard).
- Duplicates: group reconstruction — kept = prefer-prefix copy, excluded = the rest, grouped by hash.
- `set_excluded`/`batch_set_excluded`/`set_decision_id_by_object` write `decision_id` correctly; `None` leaves it NULL.

---

### Phase 3: Receipt types + exclusion integration

- **Goal**: Define the three receipt bodies and wire every exclusion entry point to set `decision_id`, write a receipt, and surface warnings.
- **Scope**: `ops/receipt.rs` (types), `ops/exclude.rs` (execute + single-target + checks), `src/exclude.rs` (placement + warning printing).

#### Changes

`ops/receipt.rs` — add `ExcludeReceipt`/`ExcludeReceiptItem`, `DuplicatesReceipt`/`DuplicateGroup`/`DuplicateKeptEntry`/`DuplicateExcludedEntry`, `ObjectExcludeReceipt`/`ObjectExcludeEntry`/`ObjectSourceReceiptEntry` (`#[derive(Serialize)]`, optional fields `skip_serializing_if = "Option::is_none"`). Reuse `write_receipt`/`finalize_receipt`.

`ops/exclude.rs` — execute functions:
- Take `placement: Option<&ReceiptPlacement>`; create recorder via `DecisionRecorder::start(conn, d, placement)`.
- Flip `excluded` **and** set `decision_id` in one call using the new repo signatures with `recorder.decision_id()`.
- Build the matching receipt from plan data; if `recorder.receipt_abs_path()` is `Some` and there is ≥1 item, `write_receipt()` before `complete()`.
- Drain `recorder.take_warnings()` into the result struct (each result gains `warnings: Vec<String>`).
- `execute_duplicates` → `DuplicatesReceipt` from `plan.groups`. `execute_set_objects` + single object ops + `execute_clear_object` → `ObjectExcludeReceipt`. `execute_set`/`execute_clear` + single-source `execute_set_source` → `ExcludeReceipt`.
- Single-target: enrich `check_*` results to carry `ExcludeItemData` (source ops) / receipt-capable sources (object ops) so execute builds a 1-item/1-object receipt without re-fetching. `check_clear_object` gains the affected sources for its receipt.

`src/exclude.rs`:
- Fetch roots; `resolve_ledger_root(&roots, config)` → build `ReceiptPlacement::LedgerRoot { root_id, root_path }` or `None` (+ emit the "No archive root configured — decision details not preserved" warning).
- Pass `placement.as_ref()` into every execute call. After execution, print `result.warnings` to stderr.

#### Tests
- Receipt serialization round-trips for all three bodies (kept omits `previous_decision_id`; unhashed omits `hash`).
- `exclude set`/`clear`/`duplicates`/`set-object`/`clear-object`/`set_by_id`/`set_by_path` each write a receipt flat at the ledger root with content matching the plan.
- No archive root → warning, no receipt, decision still recorded.
- Forced receipt-write failure → warning drained into the exclude result.

---

### Phase 4: Tests sweep + documentation

- **Goal**: Complete the provenance-chain coverage and update docs.
- **Scope**: test additions, `CLAUDE.md`, `docs/`.

#### Changes
- Provenance chain tests: `previous_decision_id` captured on re-exclude; walkable across exclude→scan-discovery (scan sets `decision_id` on new sources, exclusion captures it as `previous_decision_id`).
- `decision_id` link tests: excluded sources updated after `execute_set`; un-excluded after `execute_clear`; duplicates update excluded but **not** kept; object-level updates `decision_id` on all sources sharing the object.
- `CLAUDE.md`: `ReceiptPlacement` + `resolve_ledger_root` in `ops/receipt.rs`; the three exclusion receipt shapes + flat ledger-root placement; exclusion `decision_id` semantics; `set_decision_id_by_object` in `repo/source.rs`; note receipt-write warnings now surface for exclude + apply.
- `docs/`: see Documentation Updates below.

## Architectural Decisions

| Decision | Rationale |
|----------|-----------|
| Build `ReceiptPlacement` enum now, promoting `ReceiptContext` into it | Realigns code with the epic's contract (Story 2 deferred it on YAGNI grounds; the second consumer — exclusions — has now arrived, validating both variants). `LedgerRoot` carries `root_id` the epic omitted. |
| Receipt + `decision_id` on **every** exclusion transition (uniform) | Epic principle "receipt recording rule = decision_id update rule." Single-item receipts still preserve durable hash/size/mtime and keep the chain unbroken; cost is trivial; no special-casing. |
| `previous_decision_id` lives in the receipt, not the DB | History is irreplaceable → must live in the durable artifact (epic Principle 2). The `sources` row holds only *current* state. The decision id doubles as the predecessor receipt's filename stem, so the chain walks from disk alone; receipts' self-describing `[meta]` means the DB isn't needed to interpret it. A DB-side history index, if ever needed, is a Phase 4 (consumption) concern. |
| Three receipt bodies (`Exclude`/`Duplicates`/`ObjectExclude`) | Functional spec requires distinct structures (flat items, kept/excluded groups, object→sources). Object type was missing from the epic's enumeration. |
| `ExcludeItemData` as plan source-of-truth, `source_ids()`/`paths()` derived | Avoids 4+ index-aligned parallel vectors; single source of truth for display + receipt. Mechanical accessor migration at call sites. |
| Duplicates groups reconstructed in `plan_duplicates`, not in `find_excludable_duplicates` | The domain fn returns only `to_exclude`; group structure is a receipt-presentation concern; the plan already has `sources_by_object` to reconstruct kept vs excluded. |
| Surface receipt-write warnings (exclude + apply) | A silently-missing receipt breaks the "trust it's recorded" premise. The original drop-warnings tradeoff was scoped to DB-recording failures (which co-surface via the main error path); receipt-write failures don't co-surface. Fixes a latent apply gap. |
| `set_decision_id_by_object` separate from `object::set_excluded` | The `excluded` flag lives on `objects`; the provenance link lives on `sources`. Clean layer/responsibility separation. |
| Filename + `[meta].command` use `DecisionCommand::as_str()` (underscore) | Consistency with the `decisions.command` column and shipped apply; the epic's hyphenated examples were illustrative. |

## Non-Goals

- Scan receipts (what went missing) and roots-rm receipts — separate future stories.
- Story 4 (durable `decision_scopes` population) — independent, not part of this story.
- Phase 4 *consumption* (history commands, scan reconciliation, surfacing) — deferred; this story only writes the records.
- A DB-side decision-history/event-log table — deferred to consumption design if it proves needed.
- Normalizing exclude dry-run output destinations (stderr/stdout) — pre-existing, out of scope (per exclude-ceremony ADR).
- Changing exclude ceremony (confirmation models) — unchanged; receipts slot into the existing operation shapes.

## Test Plan

### Existing Tests (Must Pass)
All 1057. Receipt work is additive; the only migrations are `ReceiptContext` → `ReceiptPlacement` (apply + recorder tests) and exclude plan `source_ids`/`paths` field → accessor.

### New Tests (by phase)
- **Phase 1**: `compute_ledger_root_receipt_rel_path`; `resolve_ledger_root` (none/default/configured-valid/configured-invalid); recorder `LedgerRoot` placement; apply receipt-write-failure warning surfaced.
- **Phase 2**: plan items carry hash/size/mtime/`previous_decision_id`; `source_ids()`/`paths()` regression; duplicates group reconstruction; repo `decision_id` writes (`set_excluded`/`batch_set_excluded`/`set_decision_id_by_object`), NULL when None.
- **Phase 3**: serialization round-trips (3 bodies; kept omits `previous_decision_id`; unhashed omits `hash`); per-command receipt content at flat ledger root; no-archive warning + no receipt; receipt-write-failure warning drained into exclude result.
- **Phase 4**: `previous_decision_id` on re-exclude; chain walkback exclude→scan-discovery; `decision_id` updated on excluded/un-excluded/duplicates-excluded-not-kept/object-level.

## Implementation Checklist
- [x] Phase 1: `ReceiptPlacement` + `resolve_ledger_root` + recorder branch; apply → `Targeted`; apply warnings surfaced
- [x] Phase 2: repo `decision_id` threading; `ExcludeItemData` + plan enrichment + duplicates groups + accessors
- [x] Phase 3: three receipt types; wire all exclusion entry points (decision_id + receipt + warnings); `src/exclude.rs` placement + warning printing
- [ ] Phase 4: chain/decision_id tests; `CLAUDE.md`; `docs/`
- [ ] Verify all existing tests pass
- [ ] Delete this story's design seed (already removed at spec creation)

## Documentation Updates
- **New/updated page** under the decision-receipts docs: exclusion receipts — what they contain (the three shapes), where they're stored (flat at the ledger root's `.canon-ledger/`), and the `decision_id`/`previous_decision_id` chain. Co-locate with the apply-receipts documentation.
- **Update** command reference for `exclude set/clear/duplicates/set-object/clear-object`: note receipt generation and `--reason` flowing into the receipt.
- **Update** the recording-modes / config reference if it enumerates which commands produce receipts.
- `SUMMARY.md`: only if a new page is added rather than extending the existing receipts page.

## Backward Compatibility
- No CLI changes. `--no-receipt` and the `recording`/`layout` config already exist (Story 1) and apply uniformly.
- New behavior is additive: exclusions now write receipts and set `decision_id`; receipt-write failures now warn (previously silent — strictly an improvement).
- Receipt format follows the existing `receipt_version = 1` additive-only policy.

## Performance Considerations
- One extra batch `object::batch_fetch_by_ids` per exclusion plan (hash resolution) — same pattern used throughout, negligible.
- `decision_id` write folds into the existing `excluded` UPDATE (one statement, batched for multi-target).
- Receipt is serialized once and written once per decision; a large bulk exclusion produces one file with many items (hundreds of KB at most), read rarely.
