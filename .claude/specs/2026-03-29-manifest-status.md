# Story: Manifest Status

**Design Spec**: [~/store/claude-designs/2026-03-29-apply-safety-and-recovery.md](~/store/claude-designs/2026-03-29-apply-safety-and-recovery.md) (Story 8)
**Epic**: [epic-apply-safety-and-recovery.md](epic-apply-safety-and-recovery.md)
**Status**: Complete
**Created**: 2026-03-29

## Objective

After a failed apply, the user is in a fog — they don't know what state things are in, whether files are safe, or what to do next. `canon cluster status <manifest>` is the "assess the damage" command: a pure read-only diagnostic that shows the complete state of every file in a manifest with a safety assessment headline. It answers the panicked user's first question: "are my files safe?"

## Functional Requirements Summary

- `canon cluster status <manifest>` reads the lock file and checks state for every entry
- Per-entry: source file (stat), destination file (stat + size), DB registration
- Safety headline: "All source files accounted for" or "WARNING: N source files missing and not at destination"
- Works on any manifest state: fresh, partial, complete, broken
- Lock hash mismatch: report but don't block (diagnostic tool)
- Large manifests: show only concerning entries by default, `--verbose` for all
- Pure read-only — no DB writes, no file operations
- Actionable next-step hints based on state

## Current State

**No status subcommand exists.** `ClusterAction` has Generate and Refresh only.

**Manifest/lock reading**: `apply.rs` reads the manifest TOML and lock JSONL, validates lock hash. This code is inline in `apply::run()` — not easily reusable. The manifest config types (`ManifestConfig`, `ManifestMeta`, `ManifestOutput`) and lock entry type (`LockEntry`) are in `ops/cluster.rs`.

**Classification logic**: `classify_resume_entries()` in `ops/apply.rs` does filesystem classification (Pending/AlreadyThere/SourceLost/SizeMismatch) — exactly what status needs, plus DB registration check.

**DB check**: `batch_check_paths_exist()` in `repo/source.rs` queries which destination paths have `present=1` records.

## Design

### Phase 1: Ops Layer — Status Function

- **Goal**: Compute manifest status with full classification + DB check, return typed result
- **Scope**: `ops/cluster.rs` (new function), minor refactoring to share manifest reading

#### Changes

**New types in `ops/cluster.rs`**:

```rust
/// State of a single lock entry in status assessment.
pub struct StatusEntry {
    pub source_path: String,
    pub source_filename: String,      // Last component, for display
    pub source_exists: bool,
    pub dest_exists: bool,
    pub dest_size_match: bool,        // Only meaningful when dest_exists
    pub dest_size_actual: Option<u64>,
    pub db_registered: bool,
}

/// Result of manifest status assessment.
pub struct ManifestStatus {
    pub manifest_path: String,
    pub dest_path: String,            // Archive root + base_dir
    pub pattern: String,
    pub lock_entries: usize,
    pub lock_hash_valid: bool,
    pub entries: Vec<StatusEntry>,
    // Computed counts
    pub at_destination: usize,        // dest_exists && dest_size_match
    pub pending: usize,               // source_exists && !dest_exists
    pub source_lost: Vec<StatusEntry>,// !source_exists && !dest_exists
    pub size_mismatches: Vec<StatusEntry>, // dest_exists && !dest_size_match
    pub already_there_source_present: usize, // dest ok && source still there
}

impl ManifestStatus {
    /// Are all source files accounted for (either at source or at destination)?
    pub fn all_accounted_for(&self) -> bool {
        self.source_lost.is_empty() && self.size_mismatches.is_empty()
    }
}
```

**New function `compute_manifest_status()`** in `ops/cluster.rs`:

```rust
pub fn compute_manifest_status(
    conn: &Connection,
    manifest_path: &Path,
) -> Result<ManifestStatus>
```

This function:
1. Reads the manifest TOML config (reusing existing parsing)
2. Reads the lock file (JSONL)
3. Validates lock hash (stores result in `lock_hash_valid`, does NOT bail on mismatch)
4. Looks up the archive root path from `archive_root_id`
5. Computes `base_dir` as `archive_root_path/config.output.base_dir`
6. For each lock entry:
   - Stat source file
   - Compute dest path: `base_dir.join(evaluate_dest_rel(lock_entry, config))` — but we don't have the pattern evaluation result in the lock. Actually, we need to compute dest from the lock entry's path and the pattern...

   Wait — the lock file doesn't store the destination path. The destination is computed from the pattern + source facts at plan time. For status, we can't re-evaluate the pattern (no facts available without DB setup).

   BUT: the lock entry's `path` field is the source path. To get the destination, we'd need to re-run pattern evaluation. That's too heavy for a diagnostic command.

   **Alternative approach**: Status doesn't need the exact destination path. It can use `batch_check_paths_exist()` for DB state, but for filesystem state it needs the actual destination path.

   **Better approach**: Read the lock entries, and for each one, re-derive the destination path using the same pattern+facts flow that `plan_apply` uses. This requires a DB connection for fact fetching, which is heavy.

   **Simplest approach**: Have `plan_apply` (or a lighter variant) compute the transfer list (which includes `dest_rel_path`), then use those computed paths for the status check. OR: store the destination relative path alongside source info. But that changes the lock format, which is a non-goal.

   **Practical approach**: The status command calls the same manifest-reading + pattern-expansion pipeline as apply, but in read-only mode. It's essentially `plan_apply` without the violation checks, just classification. The cost is a DB connection for fact fetching — but that's already required for the DB registration check.

Let me reconsider. `plan_apply` already does: read manifest → read lock → expand patterns → build transfers. The transfers have `dest_rel_path`. Status needs exactly this. So:

```rust
pub fn compute_manifest_status(
    conn: &mut Connection,
    manifest_path: &Path,
) -> Result<ManifestStatus>
```

1. Read manifest config and lock entries (same as apply)
2. Validate lock hash (non-fatal)
3. Fetch roots, build root_paths
4. Run pattern expansion for all lock entries → get dest_rel_path per entry
5. For each entry: stat source, stat dest (base_dir + dest_rel_path), check DB
6. Build ManifestStatus

This reuses the manifest reading and pattern expansion from the apply pipeline. The pattern expansion needs fact values — same as `plan_apply`. We can extract a shared helper that reads manifest+lock+facts and produces transfers with dest paths.

Actually, the simplest reuse: call `plan_apply()` in a special mode and use its transfers. But `plan_apply` does violation checks, preflight, etc. that status doesn't need. Better to extract the "read manifest + expand patterns" part.

**Pragmatic approach**: Extract `read_manifest_and_expand()` from the common parts of `plan_apply` into a helper that status and plan_apply both call. OR: status directly replicates the read-manifest + pattern-expansion loop (it's ~30 lines). Given this is a one-off command, direct replication is acceptable — DRY applies when there's maintenance risk, and the manifest reading is stable.

```rust
pub fn compute_manifest_status(
    conn: &mut Connection,
    manifest_path: &Path,
) -> Result<ManifestStatus> {
    // 1. Read manifest
    let config = read_manifest_config(manifest_path)?;
    let lock_path = manifest_path.with_extension("lock");
    let lock_entries = read_lock_entries(&lock_path)?;
    let lock_hash_valid = validate_lock_hash(&lock_path, &config.meta.lock_hash);

    // 2. Fetch roots and archive root path
    let roots = repo::root::fetch_all(conn)?;
    let root_paths = build_root_paths(&roots);
    let archive_root = roots.iter().find(|r| r.id == config.output.archive_root_id)
        .ok_or_else(|| anyhow!("Archive root not found"))?;
    let base_dir = PathBuf::from(&archive_root.path).join(&config.output.base_dir);

    // 3. Expand patterns to get dest paths
    let pattern = expr::parse_pattern(&config.output.pattern)?;
    let needed_keys = expr::extract_fact_keys(&pattern);
    // ... fetch facts, evaluate patterns per entry ...

    // 4. For each entry: stat source, stat dest, check DB
    let dest_rel_paths: Vec<&str> = /* collected */;
    let db_registered = repo::source::batch_check_paths_exist(
        conn, config.output.archive_root_id, &dest_rel_paths
    )?;

    // 5. Build StatusEntry per lock entry + compute counts
    // 6. Return ManifestStatus
}
```

**Shared manifest reading**: Extract `read_manifest_config()` and `read_lock_entries()` as public functions in `ops/cluster.rs` (they're currently inline in `apply.rs`). These are simple: read file, parse TOML/JSONL, return.

#### Tests

- `test_status_fresh_manifest`: All sources present, no dests → pending, all accounted for
- `test_status_partially_applied`: Some dests present → mixed, all accounted for
- `test_status_source_lost`: Source missing, dest missing → source_lost, NOT accounted for

### Phase 2: Interface Layer — Status Command

- **Goal**: New `ClusterAction::Status` variant, output formatting
- **Scope**: `main.rs` (clap), `cluster.rs` (display)

#### Changes

**New variant in `ClusterAction`** (main.rs):

```rust
/// Show the state of a manifest's entries
Status {
    /// Path to manifest TOML file
    manifest: PathBuf,
    /// Show all entries, not just concerning ones
    #[arg(long)]
    verbose: bool,
},
```

**Dispatch in main.rs**:

```rust
ClusterAction::Status { manifest, verbose } => {
    cluster::status(db.connection(), &manifest, verbose)?;
}
```

**New `status()` function in `cluster.rs`**:

```rust
pub fn status(conn: &mut Connection, manifest_path: &Path, verbose: bool) -> Result<()> {
    let status = ops::cluster::compute_manifest_status(conn, manifest_path)?;

    // Header
    println!("Manifest: {}", status.manifest_path);
    println!("Destination: {}", status.dest_path);
    println!("Pattern: {}", status.pattern);
    if !status.lock_hash_valid {
        eprintln!("Warning: lock file hash mismatch — manifest may be out of sync.");
    }
    println!("Lock: {} entries", status.lock_entries);
    println!();

    // Per-entry table (verbose: all entries, default: only concerning)
    if verbose {
        // Show all entries in table format
    } else if !status.source_lost.is_empty() || !status.size_mismatches.is_empty() {
        // Show only concerning entries
    }

    // Summary + safety assessment
    println!("Summary: {} at destination, {} pending, {} source files missing.",
        status.at_destination, status.pending, status.source_lost.len());

    if status.all_accounted_for() {
        println!("All source files accounted for.");
    } else {
        println!("WARNING: {} source files are missing and not at the destination.",
            status.source_lost.len());
        for entry in &status.source_lost {
            println!("  {} (source: {})", entry.source_filename, entry.source_path);
        }
        println!();
        println!("Check if the source volume is connected. If files are truly lost,");
        println!("refresh the manifest: canon cluster refresh {}", manifest_path.display());
    }

    // Next-step hint
    if status.pending > 0 && status.all_accounted_for() {
        println!();
        if status.at_destination > 0 {
            println!("To complete: canon apply --resume {}", manifest_path.display());
        } else {
            println!("To apply: canon apply {}", manifest_path.display());
        }
    }

    Ok(())
}
```

#### Tests

Interface tests are primarily manual (output formatting). The ops-layer tests cover the logic.

## Architectural Decisions

| Decision | Rationale |
|----------|-----------|
| Status uses pattern expansion (needs DB for facts) | Destination paths aren't stored in lock — must be computed. DB connection already needed for registration check. |
| Extract `read_manifest_config()` / `read_lock_entries()` | Shared between apply and status. Currently inline in apply.rs. |
| Lock hash mismatch = warning, not error | Status is a diagnostic tool — blocking on integrity defeats the purpose. |
| Default shows only concerning entries | Large manifests would flood the terminal. Verbose flag for full listing. |
| ManifestStatus computed in ops, formatted in interface | Standard Canon pattern — ops returns typed data, interface formats. |

## Non-Goals

- Modifying the lock file format
- DB writes from status (pure read-only)
- Fixing the manifest from status (that's refresh's job)
- Showing per-file destination paths in non-verbose mode

## Test Plan

### Existing Tests (Must Pass)

All existing tests (942+).

### New Tests

| Test | Type | Phase |
|------|------|-------|
| Status with fresh manifest (all pending) | Integration (ops) | 1 |
| Status with partially applied manifest | Integration (ops) | 1 |
| Status with source lost | Integration (ops) | 1 |
| Status with size mismatch | Integration (ops) | 1 |
| Status all accounted for assessment | Unit (ops) | 1 |

## Implementation Checklist

- [ ] Phase 1: Extract `read_manifest_config()` and `read_lock_entries()` as shared functions
- [ ] Phase 1: Implement `compute_manifest_status()` in ops/cluster.rs
- [ ] Phase 1: Add StatusEntry, ManifestStatus types
- [ ] Phase 1: Tests for status computation
- [ ] Phase 2: Add `Status` variant to `ClusterAction` in main.rs
- [ ] Phase 2: Implement `status()` in cluster.rs with output formatting
- [ ] Phase 2: Wire up dispatch in main.rs
- [ ] Verify all existing tests pass

## Documentation Updates

`docs/src/commands/cluster.md` needs a section on `cluster status`. This is a new subcommand visible to the user.

## Backward Compatibility

New command — no backward compatibility concerns. Existing `cluster generate` and `cluster refresh` are unaffected.

## Performance Considerations

- Pattern expansion for all lock entries requires fact fetching from DB — same cost as `plan_apply`. For large manifests (1000+ entries), this is a few seconds. Acceptable for a diagnostic command.
- Filesystem stats: 2 stats per entry (source + dest). On NAS: ~5-10 seconds for 1000 entries. Acceptable.
- DB batch check: one `batch_check_paths_exist()` call with chunking. Fast.
