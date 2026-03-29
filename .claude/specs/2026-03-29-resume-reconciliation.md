# Story: Resume Reconciliation

**Design Spec**: [~/store/claude-designs/2026-03-29-apply-safety-and-recovery.md](~/store/claude-designs/2026-03-29-apply-safety-and-recovery.md) (Stories 5, 6, 7)
**Epic**: [epic-apply-safety-and-recovery.md](epic-apply-safety-and-recovery.md)
**Status**: Complete
**Created**: 2026-03-29

## Objective

Resume after a partial apply currently uses DB-driven inference that breaks in practice: stale-record checks block resume, `insert_destination()` crashes with UNIQUE constraints when the archive was scanned between runs, resumed files aren't registered (requiring manual `canon scan`), and resume has no awareness of source-side state. The filesystem-based reconciliation approach: stat both source and destination for every lock entry, classify based on actual state, act accordingly. Resume never deletes source files and always registers completed entries in the DB.

## Functional Requirements Summary

**Filesystem-based classification** for each lock entry:

| Source | Dest (correct size) | Classification | Action |
|--------|-------------------|----------------|--------|
| exists, readable | missing | **Pending** | Transfer (in current mode) |
| exists | present | **Already there** | Skip, register in DB |
| missing | present | **Already there** | Skip, register in DB |
| missing | missing | **Source lost** | Error |
| any | present, wrong size | **Size mismatch** | Error |

**Resume never deletes sources**: "Already there" entries are skipped regardless of current mode. Sources still present from a previous copy are noted in summary but not touched.

**Robust upsert**: `insert_destination()` handles all DB states (no record, present=0, present=1). No UNIQUE constraint errors.

**Stale-record checks skipped in resume mode**: Destination DB records are evidence of progress, not staleness.

## Current State

**`insert_destination()`** (repo/source.rs): Two-step pattern — UPDATE where `present=0`, then INSERT. Fails with UNIQUE constraint when `present=1` record exists.

**Resume in `plan_apply()`**: Calls `batch_check_paths_exist()` to find destination paths in DB with `present=1`. Removes those from transfers. Count tracked as `already_archived_count`.

**Resume in `execute_apply()`**: `classify_transfers_disk()` stats remaining destinations on disk. Classifications: Available (transfer), Resumed (skip, needs scan), SizeMismatch (error). Resumed files counted but NOT registered in DB.

**`classify_destination()`** (domain/apply.rs): Pure function taking `(in_db, on_disk_size, expected_size)` → `DestinationState`. Currently doesn't consider source state.

**Stale-record check**: `plan_apply()` populates `violations.stale_records` for destination paths in DB. Interface layer blocks resume when these exist — the bug.

## Design

### Phase 1: Robust Upsert in insert_destination

- **Goal**: `insert_destination()` handles all prior DB states without UNIQUE constraint errors
- **Scope**: `repo/source.rs`

#### Changes

**Modified `insert_destination()`**: Change the UPDATE to match `present = 0 OR present = 1` (i.e., remove the `present = 0` condition). This means the UPDATE always fires if a record exists at that path, regardless of present status. The INSERT only fires if no record exists at all.

```rust
// Before (current):
// UPDATE sources SET ... WHERE root_id = ? AND rel_path = ? AND present = 0

// After:
// UPDATE sources SET ... WHERE root_id = ? AND rel_path = ?
```

The UPDATE sets `present = 1`, updates metadata (size, mtime, partial_hash, object_id), increments `basis_rev`, clears `excluded`. This is correct for both cases:
- `present=0` (stale record revived) — existing behavior, preserved
- `present=1` (scan-created record refreshed) — new: updates metadata to match the file apply just placed

The INSERT path only fires when no record exists at all — truly new destination.

This is idempotent: calling it twice with the same data produces the same result (UPDATE matches, sets same values).

#### Tests

- `test_insert_destination_new_record`: No existing record → INSERT (existing behavior, verify preserved)
- `test_insert_destination_revive_stale`: Existing `present=0` record → UPDATE to `present=1` (existing behavior)
- `test_insert_destination_update_active`: Existing `present=1` record (from scan) → UPDATE metadata, no UNIQUE error
- `test_insert_destination_idempotent`: Call twice with same data → same result, no error

### Phase 2: Filesystem-Based Classification

- **Goal**: New classification function that checks source + destination + DB state for each lock entry
- **Scope**: `ops/apply.rs` (new function), update to `domain/apply.rs` types

#### Changes

**New enum in `domain/apply.rs`** (or extend existing):

```rust
/// Classification of a lock entry's state during resume reconciliation.
#[derive(Debug, Clone, PartialEq)]
pub enum ResumeEntryState {
    /// Source exists, destination missing — needs transfer
    Pending,
    /// Destination present with correct size — skip, register in DB.
    /// `source_present` indicates whether the source file still exists (for summary note).
    AlreadyThere { source_present: bool },
    /// Source missing, destination missing — error
    SourceLost,
    /// Destination present with wrong size — error
    SizeMismatch { expected: u64, actual: u64 },
}
```

**New struct for classification results**:

```rust
pub struct ResumeClassification<'a> {
    /// Entries that need transfer (source exists, dest missing)
    pub pending: Vec<&'a ApplyTransfer>,
    /// Entries already at destination (with source_present flag)
    pub already_there: Vec<(&'a ApplyTransfer, bool)>,  // (transfer, source_present)
    /// Entries where source is lost (source missing, dest missing)
    pub source_lost: Vec<&'a ApplyTransfer>,
    /// Entries with size mismatch at destination
    pub size_mismatches: Vec<(&'a ApplyTransfer, u64, u64)>,  // (transfer, expected, actual)
}
```

**New function `classify_resume_entries()`** in `ops/apply.rs`:

```rust
/// Classify lock entries by checking filesystem state of source and destination.
/// Also checks DB for destination records (for registration decisions).
fn classify_resume_entries<'a>(
    conn: &Connection,
    transfers: &'a [ApplyTransfer],
    base_dir: &Path,
    archive_root_id: i64,
) -> Result<ResumeClassification<'a>> {
    // Batch check which dest paths are already in DB
    let dest_rel_paths: Vec<&str> = transfers.iter().map(|t| t.archive_rel_path.as_str()).collect();
    let paths_in_db = repo::source::batch_check_paths_exist(conn, archive_root_id, &dest_rel_paths)?;

    let mut result = ResumeClassification { pending: vec![], already_there: vec![], source_lost: vec![], size_mismatches: vec![] };

    for transfer in transfers {
        let dest_path = base_dir.join(&transfer.dest_rel_path);
        let source_exists = Path::new(&transfer.source_path).exists();
        let dest_stat = fs::metadata(&dest_path).ok();

        match dest_stat {
            Some(meta) if meta.is_file() => {
                let actual_size = meta.len() as i64;
                if actual_size == transfer.size {
                    // Destination present with correct size — already there
                    result.already_there.push((transfer, source_exists));
                } else {
                    // Size mismatch
                    result.size_mismatches.push((transfer, transfer.size as u64, actual_size as u64));
                }
            }
            _ => {
                // Destination missing (or not a file)
                if source_exists {
                    result.pending.push(transfer);
                } else {
                    result.source_lost.push(transfer);
                }
            }
        }
    }

    Ok(result)
}
```

This replaces both `batch_check_paths_exist` filtering in plan AND `classify_transfers_disk` in execute with a single unified classification.

#### Tests

- `test_classify_pending`: Source exists, dest missing → Pending
- `test_classify_already_there_source_present`: Source + dest both exist, correct size → AlreadyThere(source_present: true)
- `test_classify_already_there_source_gone`: Source gone, dest exists, correct size → AlreadyThere(source_present: false)
- `test_classify_source_lost`: Source gone, dest gone → SourceLost
- `test_classify_size_mismatch`: Dest exists with wrong size → SizeMismatch

### Phase 3: Reworked Resume Flow

- **Goal**: Replace DB-driven resume with filesystem-based classification. Register "already there" entries in DB. Skip stale-record checks in resume. Summary note for lingering sources.
- **Scope**: `ops/apply.rs` (plan_apply, execute_apply), `apply.rs` (display)

#### Changes

**Modified `plan_apply()` resume path**:

Currently: calls `batch_check_paths_exist()`, removes already-in-DB transfers, sets `already_archived_count`.

New: calls `classify_resume_entries()` to get the full classification. Returns classification data in the plan so the interface can display it and execute can act on it.

**New field in `ApplyPlan`**:

```rust
pub struct ApplyPlan {
    // ... existing fields ...
    /// Resume classification — only populated in resume mode.
    pub resume_classification: Option<ResumeClassificationSummary>,
}

pub struct ResumeClassificationSummary {
    pub pending_count: usize,
    pub already_there_count: usize,
    pub already_there_source_present_count: usize,  // For summary note
    pub source_lost: Vec<(i64, String)>,  // (source_id, path) — errors
    pub size_mismatches: Vec<(String, u64, u64)>,  // (dest_path, expected, actual)
}
```

The actual `pending` transfers go into `plan.transfers` as before. The "already there" entries go into a new `plan.resume_already_there` vec for DB registration during execute.

**New field in `ApplyPlan`**:

```rust
pub resume_already_there: Vec<ApplyTransfer>,  // Entries to register in DB
```

**Modified stale-record handling**: In resume mode, skip the `stale_records` and `dest_conflicts_in_db` violation collection entirely. These violations are meaningless in resume context.

**Modified `execute_apply()`**: After the transfer loop, register "already there" entries in the DB:

```rust
if params.resume {
    for transfer in &plan.resume_already_there {
        // Register in DB via upsert (insert_destination handles all states)
        let new_source = build_new_source_from_lock(transfer);
        let _ = repo::source::insert_destination(conn, &new_source)?;
        result.already_there += 1;
    }
}
```

Note: `build_new_source_from_lock` creates a `NewSource` from the lock entry data (size, mtime, partial_hash, object_id) without reading the file — the lock entry has the metadata.

**New field in `ApplyResult`**:

```rust
pub already_there: u64,  // Replaces already_archived + resumed
pub already_there_source_present: u64,  // For summary note
```

Remove the old `already_archived` and `resumed` fields (or keep for backward compat and just not use them).

**Modified interface layer** (`apply.rs`):

Summary before confirmation:
```
Files: 17 (10 pending, 7 already at destination)
```

Summary after execute:
```
Applied (--resume): 10 renamed, 7 already at destination, 0 errors
```

Note when sources linger:
```
Note: 7 source files from a previous operation may still exist at the original location.
```

Source lost errors (abort before transfer):
```
Resume failed: 2 source files are missing and not at the destination.

  /Volumes/source/Photos/IMG_042.jpg
  /Volumes/source/Photos/IMG_043.jpg

Check if the source volume is connected. If files are truly lost,
refresh the manifest: canon cluster refresh manifest.toml
```

Size mismatch errors (abort before transfer):
```
Resume failed: 1 destination file has wrong size.

  /Archive/Photos/IMG_001.jpg (expected 1234567 bytes, found 567890 bytes)

Delete the corrupt file and retry: canon apply --resume manifest.toml
```

#### Tests

- `test_resume_registers_already_there_in_db`: File at dest, not in DB → after resume, DB has record
- `test_resume_skips_stale_record_violations`: Dest in DB from scan → no violation, classified as "already there"
- `test_resume_source_lost_aborts`: Source gone, dest gone → error, no transfers
- `test_resume_size_mismatch_aborts`: Dest has wrong size → error, no transfers
- `test_resume_pending_transfers_normally`: Source exists, dest missing → transfers as normal
- `test_resume_summary_notes_lingering_sources`: Already-there entries with source present → count tracked
- `test_resume_no_note_when_sources_gone`: Already-there entries with source gone (rename) → no note

## Architectural Decisions

| Decision | Rationale |
|----------|-----------|
| Classification in plan phase, not execute | Confirmation summary needs the full picture before the user says "yes" |
| Single classification function replaces DB check + disk check | One pass over lock entries, one consistent view of state |
| "Already there" entries registered in execute phase | DB writes belong in execute, not plan (existing pattern) |
| Stale-record checks completely skipped in resume | In resume context, destination records are evidence of progress |
| Remove `already_archived` and `resumed` in favor of `already_there` | Cleaner model — one classification, one count |
| `build_new_source_from_lock` uses lock metadata, not disk | Lock entry has size/mtime/partial_hash — no need to re-read the file |
| Source lost and size mismatch are hard aborts | Same principle as source preflight — errors before any file operations |

## Non-Goals

- Deleting source files during resume (Story 6 principle: resume ensures destination, doesn't clean sources)
- Changing the lock file format
- Changing how non-resume apply works (the regular path is unaffected)
- Refreshing the manifest from resume (user does this manually if needed)

## Test Plan

### Existing Tests (Must Pass)

All existing `ops/apply` and `repo/source` tests. The `insert_destination` change (Phase 1) affects existing tests — they should continue to pass since the behavior for `present=0` and new-record cases is preserved.

### New Tests

| Test | Type | Phase |
|------|------|-------|
| insert_destination with present=1 record | Integration (repo) | 1 |
| insert_destination idempotent | Integration (repo) | 1 |
| classify: pending | Integration (ops/apply) | 2 |
| classify: already there, source present | Integration (ops/apply) | 2 |
| classify: already there, source gone | Integration (ops/apply) | 2 |
| classify: source lost | Integration (ops/apply) | 2 |
| classify: size mismatch | Integration (ops/apply) | 2 |
| resume registers already-there in DB | Integration (ops/apply) | 3 |
| resume skips stale-record violations | Integration (ops/apply) | 3 |
| resume source-lost aborts | Integration (ops/apply) | 3 |
| resume pending transfers normally | Integration (ops/apply) | 3 |
| resume notes lingering sources | Integration (ops/apply) | 3 |

## Implementation Checklist

- [ ] Phase 1: Fix `insert_destination()` UPDATE to remove `present = 0` condition
- [ ] Phase 1: Tests for all DB states (new, present=0, present=1, idempotent)
- [ ] Phase 2: Add `ResumeEntryState` enum and `ResumeClassification` struct
- [ ] Phase 2: Implement `classify_resume_entries()` function
- [ ] Phase 2: Tests for all classification cases
- [ ] Phase 3: Rework plan_apply resume path to use classification
- [ ] Phase 3: Add `resume_already_there` and `resume_classification` to ApplyPlan
- [ ] Phase 3: Rework execute_apply to register already-there entries
- [ ] Phase 3: Update ApplyResult with `already_there` fields
- [ ] Phase 3: Skip stale-record/dest-conflict violations in resume mode
- [ ] Phase 3: Update interface summary display
- [ ] Phase 3: Source-lost and size-mismatch error display
- [ ] Phase 3: Lingering-sources note display
- [ ] Phase 3: Tests for reworked resume flow
- [ ] Verify all existing tests pass

## Documentation Updates

No user-facing doc changes for this story alone. Resume behavior will be documented in the broader epic documentation pass.

## Backward Compatibility

- `insert_destination()` now updates `present=1` records instead of failing — strictly better (no UNIQUE crashes)
- Resume summary format changes: "already archived" + "resumed" → "already at destination". Users who parse apply output may need to adjust. Pre-1.0, acceptable.
- Resume no longer requires `canon scan` after completing — the "Note: N resumed files are not yet registered" message goes away. Strictly better.

## Performance Considerations

- Classification stats every source + destination: 2 stats per lock entry. On NAS with 1000 entries: ~5-10 seconds. Acceptable — one-time cost during plan.
- DB registration of "already there" entries: one upsert per entry. Fast for typical counts (10-100).
- `batch_check_paths_exist()` still called by classification for DB awareness. Same performance as before.
