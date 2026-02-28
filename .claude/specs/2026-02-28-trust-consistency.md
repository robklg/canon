# Story: Trust Consistency — Proportionate Ceremony for Consequential Operations

**Design Spec**: `~/store/claude-designs/2026-02-26-trust-consistency.md`
**Status**: Pending
**Created**: 2026-02-28

## Objective

Canon has a deliberate trust model: transparency over restriction. Silence means safety, ceremony means consequence. The principle exists, but ceremony levels are scattered — some consequential operations lack confirmation, and the information surfaced at decision moments varies. This story makes ceremony proportionate and consistent: shared infrastructure for confirmations, scope confirmation for exclude commands, richer context for apply and cluster generate, and manifest format evolution to bridge the cognitive gap between generation and application.

## Functional Requirements Summary

Six user stories from the design spec:

1. **`exclude set` confirmation**: When affecting > 1 source, show count, root spread, and archive coverage before executing. `--yes` skips, `--dry-run` shows detail without confirmation.
2. **`exclude clear` confirmation**: When affecting > 1 source, show count and root spread. Lighter context (clearing makes sources more visible, not less).
3. **`exclude duplicates` confirmation**: When excluding > 1 source, fold existing statistics into confirmation prompt. Three presentation modes: confirmation (interactive), pre-listing context (dry-run), omitted (single source).
4. **Apply summary enrichment**: Mode line adds parenthetical for rename/move. "Sources from:" section lists roots losing files (rename/move only, not copy).
5. **`--move`/`--yes` docs fix**: Correct any docs implying `--move` requires `--yes`. Canon's safety is structural, not flag-gated.
6. **Cluster summary & notes**: Manifest gains `# === Cluster Summary ===` and `# === Notes ===` comment sections. `[meta]` gains `version = 1`. Summary regenerated on refresh; notes preserved. Richer stdout for generate/refresh.

**Ceremony vocabulary** (uniform across all commands):
- Confirmation: `Will <verb> <count> <noun>` + indented context + `Proceed? [y/N]`
- Execution summary: `<Past-tense verb> <count> <noun>`
- Cancellation: `Aborted.`
- Threshold: > 1 source triggers confirmation
- `--yes`: Skip prompt. `--dry-run`: Show detail, no prompt.

## Current State

**Existing ceremony patterns:**
- `roots rm` (`roots.rs:142-159`): Has confirmation with `--yes`. Uses `bail!("Aborted")` (error exit). Inline stdin reading.
- `apply` (`apply.rs:725-808`): Has `print_apply_summary()` + `confirm_proceed()` helper. Uses `println!("Aborted.")` (success exit). Has `--yes`.
- `exclude set` (`exclude.rs:29-83`): No confirmation. Has `--dry-run`.
- `exclude clear` (`exclude.rs:89-132`): No confirmation. Has `--dry-run`.
- `exclude duplicates` (`exclude.rs:319-440`): No confirmation. Statistics print unconditionally. Has `--dry-run`.
- `exclude set --objects` (`exclude.rs:519-704`): Dry-run-first with `--yes` to execute. Out of scope for this spec.

**Key existing infrastructure:**
- `repo::object::batch_check_archived(conn, &object_ids, None)` — archive coverage query (used by `roots.rs`, `coverage.rs`)
- `repo::source::batch_fetch_by_ids()` — batch source fetch
- `domain::exclusion::find_excludable_duplicates()` — returns `ExcludableDuplicatesResult` with skip counts
- `cluster::LockGenerationResult` — currently carries `source_count` and `full_coverage_facts`
- `cluster::ManifestMeta` — no `version` field yet

## Design

### Phase 1: Ceremony Helper + Adoption

- **Goal**: Establish shared confirmation infrastructure; normalize existing commands.
- **Scope**: New module, refactor `roots rm` and `apply`.

#### Changes

**New file: `src/ceremony.rs`**

```rust
use anyhow::Result;
use std::io::{self, Write};

/// Display "Proceed? [y/N]" and wait for user input.
/// Returns Ok(true) to proceed, Ok(false) if declined.
/// When `yes` is true, returns Ok(true) without prompting.
pub fn confirm(yes: bool) -> Result<bool> {
    if yes {
        return Ok(true);
    }
    eprint!("Proceed? [y/N] ");
    io::stderr().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;

    if input.trim().eq_ignore_ascii_case("y") {
        Ok(true)
    } else {
        println!("Aborted.");
        Ok(false)
    }
}
```

Register `mod ceremony;` in `main.rs`.

**Refactor `roots.rs:remove()`:**
- Replace inline `eprint!("Proceed? [y/N]")` + stdin reading + `bail!("Aborted")` with `ceremony::confirm(yes)`.
- On decline: `return Ok(())` instead of `bail!("Aborted")`. Aborting a confirmation is a normal user action, not an error.
- The content printed before the prompt (root info, source counts) remains unchanged — output normalization is out of scope.

**Refactor `apply.rs`:**
- Remove `confirm_proceed()` helper function.
- Replace its call site with `ceremony::confirm(options.yes)`.
- On decline: `println!("Aborted."); return Ok(());` (already the current behavior).

#### Tests

- Verify all existing `roots rm` and `apply` tests still pass.
- No unit tests for `ceremony::confirm()` (stdin dependency; logic is trivial).

---

### Phase 2: Exclude Set & Clear Confirmations

- **Goal**: Add confirmation prompts to `exclude set` and `exclude clear`.
- **Scope**: CLI flags, confirmation data computation, `get_excluded_sources` return type.

#### Changes

**CLI (`main.rs`):**
- Add `#[arg(long)] yes: bool` to `ExcludeSet` (filter variant) and `ExcludeClear` subcommands.
- Pass `yes` through to `SetOptions` and `ClearOptions`.

**`exclude.rs` — `SetOptions` and `ClearOptions`:**
```rust
pub struct SetOptions {
    pub dry_run: bool,
    pub verbose: bool,
    pub yes: bool,  // NEW
}

pub struct ClearOptions {
    pub dry_run: bool,
    pub yes: bool,  // NEW
}
```

**`exclude.rs` — `set()` confirmation block:**

After computing `to_exclude` and before executing, if `to_exclude.len() > 1 && !options.dry_run`:

1. Collect distinct `root_id`s from `sources_map` for the `to_exclude` IDs.
2. Collect `object_id`s from the same sources.
3. Call `repo::object::batch_check_archived(conn, &object_ids, None)` to get archived set.
4. Count sources where `object_id` is `None` or not in archived set → `not_archived`.
5. Print confirmation:
   ```
   Will exclude {count} sources
     Across {root_count} roots
     {not_archived} have no archived copy
   ```
6. Call `ceremony::confirm(options.yes)`. Return `Ok(())` if declined.

Single source (count = 1): skip confirmation, execute directly, print summary.

**`exclude.rs` — `get_excluded_sources()` return type change:**

Change from `Result<Vec<(i64, String)>>` to `Result<Vec<Source>>`. This follows the pattern of returning domain types. The function already fetches full `Source` objects internally and discards the extra fields — stop discarding them.

**`exclude.rs` — `clear()` confirmation block:**

After getting excluded sources, if `count > 1 && !options.dry_run`:

1. Collect distinct `root_id`s from the returned `Vec<Source>`.
2. Print confirmation:
   ```
   Will clear exclusions for {count} sources
     Across {root_count} roots
   ```
3. Call `ceremony::confirm(options.yes)`. Return `Ok(())` if declined.

Update dry-run and execution code to work with `Vec<Source>` (use `s.id` and `s.path()`).

#### Tests

- `test_set_confirmation_counts_roots` — Sources across 2 roots, verify distinct root count.
- `test_set_confirmation_archive_coverage` — Mix of archived/unarchived sources, verify not-archived count.
- `test_set_confirmation_unhashed_not_archived` — Sources with `object_id=None` count as "no archived copy".
- `test_set_single_source_no_confirmation` — Count = 1 executes directly.
- Migrate existing `get_excluded_sources` tests to work with `Vec<Source>` return type:
  - `test_get_excluded_sources_returns_source_level_only` — Check `result[0].id` and `result[0].path()`.
  - `test_get_excluded_sources_ignores_object_level_excluded` — Unchanged logic, new field access.
  - `test_get_excluded_sources_respects_scope` — Same.
  - `test_get_excluded_sources_returns_correct_path` — Use `result[0].path()`.
- `test_clear_confirmation_counts_roots` — Multiple excluded sources across roots.

---

### Phase 3: Exclude Duplicates Confirmation

- **Goal**: Add confirmation with integrated statistics to `exclude duplicates`.
- **Scope**: Presentation mode refactoring, `--yes` flag.

#### Changes

**CLI (`main.rs`):**
- Add `#[arg(long)] yes: bool` to `ExcludeDuplicates` subcommand.
- Pass `yes` through to `exclude_duplicates()`.

**`exclude.rs` — `exclude_duplicates()` signature:**

Add `yes: bool` parameter.

**Presentation mode refactoring:**

The existing unconditional statistics block (lines 463–483 in current code) must be **removed entirely** and replaced with mode-appropriate presentation. The same data is presented differently depending on mode — not printed twice.

After `find_excludable_duplicates()` returns and paths are built, choose presentation mode:

1. **`to_exclude.is_empty()`**: Print "Nothing to exclude." (unchanged).

2. **`dry_run`**: Print statistics as pre-listing context, then paths:
   ```
   Sources in scope: {scope_count} ({skipped_no_hash} unhashed skipped)
     Will exclude: {to_exclude_count}
     Skipped (no copy in --prefer): {skipped_not_covered}
     Skipped (multiple copies in --prefer): {skipped_multiple}
     Skipped (already in --prefer): {skipped_in_prefer}  // only if > 0

   Would exclude {to_exclude_count} sources:
     /path/to/dup1.jpg
     ...
   ```

3. **Interactive, count > 1**: Gate confirmation content behind `!yes` (same pattern as Phase 2 — content AND expensive formatting skipped when `--yes` is passed). Then call `ceremony::confirm(yes)`:
   ```
   Will exclude {to_exclude_count} sources ({group_count} duplicate groups)
     Keeping copies in: {prefer_path}
     Skipped {skipped_not_covered} (no copy in --prefer)      // only if > 0
     Skipped {skipped_multiple} (multiple copies in --prefer)  // only if > 0

   Proceed? [y/N]
   ```
   Where `group_count` = number of distinct `object_id`s among `to_exclude` sources, computed from `scope_sources_map` by collecting `object_id`s for the `to_exclude` IDs into a `HashSet`.

4. **Count = 1**: Skip statistics entirely, execute directly.

**Post-execution summary** (all modes except dry-run):
```
Excluded {count} sources

Use `canon ls --duplicates` to see remaining duplicates.
```

The `prefer_path` display in the confirmation uses the canonicalized prefer path already available.

#### Tests

- `test_duplicates_group_count` — 4 sources excluded across 2 object_ids → "2 duplicate groups".
- `test_duplicates_single_source_no_stats` — Count = 1: execution summary only, no statistics.
- Existing `exclude_duplicates` integration tests continue to pass (they use `dry_run=false` and don't check stdout format).

---

### Phase 4: Apply Summary Enrichment + Docs Fix

- **Goal**: Richer `apply` summary for rename/move modes; fix misleading docs.
- **Scope**: Formatting changes, no new queries.

#### Changes

**`apply.rs` — `print_apply_summary()` signature:**

Add `root_paths: &HashMap<i64, String>` parameter. Update call site in `run()` to pass `&root_paths` (already available).

**Mode line enrichment:**
```rust
let mode_name = match options.transfer_mode {
    TransferMode::Copy => "copy",
    TransferMode::Rename => "rename (sources will be relocated)",
    TransferMode::Move => "move (sources will be deleted after copy)",
};
eprintln!("Mode: {mode_name}");
```

**"Sources from:" section** (rename/move only, not copy):

After the `Files:` line, if mode is rename or move:
1. Group sources by `root_id`, count per root.
2. Look up root paths from `root_paths` map.
3. Sort by root path for consistent output.
4. Print:
   ```
   Sources from:
     /path/to/root1  ({count} files)
     /path/to/root2  ({count} files)
   ```

**Docs fix:**
- Search `docs/` and code comments for text implying `--move` requires `--yes`.
- Correct to reflect Canon's actual model: `--yes` is a scripting convenience, safety is structural (integrity validation, noclobber, source deletion only after verified copy).

#### Tests

- Existing apply tests continue to pass (print_apply_summary signature change requires updating the call).
- No new formatting tests (output goes to stderr, not easily captured in unit tests; the grouping logic is straightforward).

---

### Phase 5: Cluster Summary, Notes, and Version

- **Goal**: Manifest format evolution — summary comments, notes section, version field, richer stdout.
- **Scope**: Manifest structure, comment generation, notes preservation, version validation.

#### Changes

**`cluster.rs` — `ManifestMeta` version field:**

```rust
#[derive(Serialize, Deserialize)]
pub struct ManifestMeta {
    #[serde(default = "default_version")]
    pub version: u32,
    pub query: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    pub generated_at: String,
    pub lock_hash: String,
}

fn default_version() -> u32 { 1 }
```

Existing manifests without `version` deserialize as version 1 via `serde(default)`.

**Version validation:**

```rust
const SUPPORTED_MANIFEST_VERSION: u32 = 1;

fn validate_manifest_version(version: u32) -> Result<()> {
    if version > SUPPORTED_MANIFEST_VERSION {
        bail!(
            "Manifest version {} is not supported by this version of Canon. Please update Canon.",
            version
        );
    }
    Ok(())
}
```

Called early in `apply::run()` (after parsing config, before any other processing) and `cluster::refresh()` (after parsing config).

**`ceremony.rs` — `format_count()` utility:**

```rust
/// Format a number with thousands separators (e.g., 3847 → "3,847").
pub fn format_count(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}
```

This lives in `ceremony.rs` alongside the confirmation helper — both are ceremony infrastructure used across commands. Used by `generate_summary_comments()`, `print_cluster_stdout()`, and available for future use in exclude confirmation content.

**`cluster.rs` — `LockGenerationResult` enrichment:**

```rust
struct LockGenerationResult {
    source_count: usize,
    full_coverage_facts: Vec<(String, FactType, String)>,
    root_breakdown: Vec<(String, usize)>,  // (root_path, count), sorted by path
    not_archived_count: usize,              // sources with no archived copy
    excluded_count: usize,                  // skipped excluded sources
    unhashed_count: usize,                  // skipped unhashed sources
}
```

**Computing new fields in `generate_lock()`:**

The `excluded_count` and `unhashed_count` are already tracked by `query_sources()`. Instead of printing them inline in `generate_lock()`, return them in the result.

For `root_breakdown`: After building the final `sources` list, group by `root_id`. Look up root paths from the roots already fetched in `query_sources()` (pass roots through or fetch in `generate_lock()`). Sort by path.

For `not_archived_count`: The `archive_paths` HashMap is already computed in `query_sources()`. Count sources in the final list whose `object_id` has no entry in `archive_paths`. This works correctly regardless of `allow_archived` because `archive_paths` is computed for all objects.

**Propagating data**: `query_sources()` needs to return the additional data. Extend its return type or restructure it to return a result struct. The cleanest approach: add `root_breakdown` and `not_archived_count` to its return tuple (or introduce a struct to replace the growing tuple).

**`generate_summary_comments()` — new function:**

```rust
fn generate_summary_comments(result: &LockGenerationResult) -> String {
    let mut s = String::new();
    s.push_str("# === Cluster Summary ===\n");

    let root_word = if result.root_breakdown.len() == 1 { "root" } else { "roots" };
    s.push_str(&format!(
        "# {} sources from {} {}:\n",
        format_count(result.source_count),
        result.root_breakdown.len(),
        root_word
    ));
    for (path, count) in &result.root_breakdown {
        s.push_str(&format!("#   {}  ({})\n", path, format_count(*count)));
    }
    s.push_str(&format!(
        "# {} have no archived copy\n",
        format_count(result.not_archived_count)
    ));

    // Skipped line (only if there are skipped sources)
    if result.excluded_count > 0 || result.unhashed_count > 0 {
        s.push_str("#\n");
        let mut parts = Vec::new();
        if result.excluded_count > 0 {
            parts.push(format!("{} excluded", result.excluded_count));
        }
        if result.unhashed_count > 0 {
            parts.push(format!("{} unhashed", result.unhashed_count));
        }
        s.push_str(&format!("# Skipped: {}\n", parts.join(", ")));
    }

    s
}
```

Where `format_count()` formats numbers with thousands separators (e.g., `3,847`).

**Notes handling:**

`extract_notes()` — reads existing manifest text, returns notes content:

```rust
fn extract_notes(content: &str) -> Option<String> {
    let marker = "# === Notes ===";
    let start_idx = content.find(marker)?;
    let after_marker = start_idx + marker.len();
    let rest = &content[after_marker..];

    // Find end: next "# === " header or first TOML section "[" at line start
    let end = rest.lines()
        .enumerate()
        .skip(1)  // skip the marker line itself
        .find(|(_, line)| line.starts_with("# === ") || line.starts_with('['))
        .map(|(i, _)| {
            // Calculate byte offset of this line
            rest.lines().take(i).map(|l| l.len() + 1).sum::<usize>()
        })
        .unwrap_or(rest.len());

    Some(rest[..end].to_string())
}
```

Empty notes placeholder: `"# === Notes ===\n#\n"`

**Manifest assembly** (both `generate()` and `refresh()`):

```rust
let summary = generate_summary_comments(&result);

let notes = if is_refresh {
    // Preserve existing notes from the old manifest content
    extract_notes(&old_content).unwrap_or_else(|| "\n#\n".to_string())
} else {
    "\n#\n".to_string()  // empty placeholder
};

let notes_block = format!("# === Notes ==={notes}\n");
let toml_str = toml::to_string_pretty(&config)?;
// ... inject original filter comments if needed ...

let manifest = format!("{summary}\n{notes_block}{toml_str}\n\n{fact_help}");
```

**`print_cluster_stdout()` — new function for stdout summary:**

```rust
fn print_cluster_stdout(
    header: &str,  // "Generated manifest: foo.toml (N sources in foo.lock)" or "Refreshed lock file: ..."
    result: &LockGenerationResult,
) {
    println!("{header}");
    let root_word = if result.root_breakdown.len() == 1 { "root" } else { "roots" };
    println!("  From {} {}:", result.root_breakdown.len(), root_word);
    for (path, count) in &result.root_breakdown {
        println!("    {}  ({})", path, format_count(*count));
    }
    println!("  {} have no archived copy", format_count(result.not_archived_count));
}
```

**`generate()` changes:**
- Call `generate_summary_comments()` and include in manifest output.
- Write notes placeholder.
- Replace current `println!("Generated manifest: ...")` with `print_cluster_stdout()`.
- Remove inline `eprintln!` for excluded/unhashed counts. These were ad-hoc progress messages; with the ceremony vocabulary in place, the skip information is consolidated into the manifest summary comments (`# Skipped:` line) where it serves the user's future self. The stdout execution summary focuses on what matters for the current moment (source count, root breakdown, archive coverage). This is removal, not a channel move — the manifest is the right home for skip details.

**`refresh()` changes:**
- Read old manifest content before parsing.
- After generating new result, call `generate_summary_comments()`.
- Call `extract_notes()` on old content, preserve in new manifest.
- Replace current `println!("Refreshed lock file: ...")` with `print_cluster_stdout()`.
- Add `validate_manifest_version()` call early.

**`apply.rs` changes:**
- Add `validate_manifest_version()` call early in `run()`, after parsing config.

#### Tests

**Notes extraction:**
- `test_extract_notes_empty_placeholder` — `"# === Notes ===\n#\n\n[meta]"` → returns `"\n#\n\n"`.
- `test_extract_notes_with_content` — Notes with user text preserved verbatim.
- `test_extract_notes_missing` — No marker → returns `None`.
- `test_extract_notes_before_meta` — Notes end at `[meta]`.
- `test_extract_notes_before_next_section` — Notes end at next `# === `.

**Summary comment generation:**
- `test_generate_summary_single_root` — Uses "1 root:" (singular).
- `test_generate_summary_multiple_roots` — Root breakdown sorted, uses "roots:" (plural).
- `test_generate_summary_no_skipped` — Skipped line omitted when both counts are 0.
- `test_generate_summary_with_skipped` — Skipped line present with correct format.

**`format_count()`:**
- `test_format_count` — `0` → `"0"`, `999` → `"999"`, `1000` → `"1,000"`, `1234567` → `"1,234,567"`.

**Version validation:**
- `test_version_1_accepted` — Passes.
- `test_version_future_rejected` — Error message names version.
- `test_manifest_without_version_defaults_to_1` — Backward compat: missing field → 1.

**Manifest round-trip:**
- `test_manifest_with_version_round_trip` — Serialize/deserialize preserves version field.

---

## Architectural Decisions

| Decision | Rationale |
|----------|-----------|
| Ceremony helper owns only prompt mechanics, not content formatting | Commands have different content shapes; shared helper prevents mechanical drift (prompt wording, abort message, `--yes` behavior) without constraining content |
| `roots rm` and `apply` adopt ceremony helper | Prevents old commands confirming one way and new commands another — exactly the drift the vocabulary aims to prevent |
| `roots rm` abort changes from `bail!` to `return Ok(())` | Aborting a confirmation is a normal user action, not an error. Matches `apply`'s existing behavior and the spec's "Aborted." convention |
| `get_excluded_sources` returns `Vec<Source>` | Follows established pattern of returning domain types; enables root counting without extra fetch |
| `LockGenerationResult` carries all summary data | Single computation pass; same data drives manifest comments, stdout summary, and skip reporting |
| Notes preserved via string extraction, not TOML parsing | Notes are comments — TOML parsers strip them. String matching on distinctive `# === Notes ===` markers is reliable and simple |
| Version field uses `serde(default)` | Zero-migration: existing manifests without `version` deserialize as v1 |
| `format_count()` in `ceremony.rs` | Ceremony infrastructure — used for manifest comments and stdout summaries. Lives alongside `confirm()` as shared formatting for ceremony output |
| Confirmation content gated behind `!yes` | When `--yes` is passed, both the confirmation content (eprintln lines) AND any expensive queries needed only for that content are skipped. `ceremony::confirm(yes)` is called separately and returns `Ok(true)` immediately. Established in Phase 2, applies uniformly to Phase 3 |
| `exclude duplicates` stderr removal | Old ad-hoc `eprintln!` skip messages removed, not moved. Skip information consolidated into manifest summary comments where it serves the user's future self |

## Non-Goals

- Normalizing `roots rm` content wording (e.g., "About to remove" → "Will remove") — explicitly out of scope per spec's "Texture Observations"
- Modifying `exclude set --objects` ceremony flow — spec explicitly excludes it
- Changing `apply`'s confirmation shape (summary block + prompt) — spec says "the shape stays identical"
- Normalizing "sources" vs "files" terminology across existing output — documented as future opportunity

## Test Plan

### Existing Tests (Must Pass)

All existing tests in:
- `src/exclude.rs` (24 tests) — some assertions updated for `get_excluded_sources` return type change
- `src/cluster.rs` (5 tests) — updated for `ManifestMeta` version field
- `src/apply.rs` — updated for `print_apply_summary` signature change
- `src/roots.rs` — verify remove still works with `yes=true`
- `src/domain/exclusion.rs` (11 tests) — unchanged
- `src/domain/source.rs` (5 tests) — unchanged

### New Tests

**Phase 2:**
- `test_set_confirmation_counts_roots`
- `test_set_confirmation_archive_coverage`
- `test_set_confirmation_unhashed_not_archived`
- `test_set_single_source_no_confirmation`
- `test_clear_confirmation_counts_roots`

**Phase 3:**
- `test_duplicates_group_count`
- `test_duplicates_single_source_no_stats`

**Phase 5:**
- `test_format_count`
- `test_extract_notes_empty_placeholder`
- `test_extract_notes_with_content`
- `test_extract_notes_missing`
- `test_extract_notes_before_meta`
- `test_extract_notes_before_next_section`
- `test_generate_summary_single_root`
- `test_generate_summary_multiple_roots`
- `test_generate_summary_no_skipped`
- `test_generate_summary_with_skipped`
- `test_version_1_accepted`
- `test_version_future_rejected`
- `test_manifest_without_version_defaults_to_1`
- `test_manifest_with_version_round_trip`

## Implementation Checklist

- [x] Phase 1: Ceremony helper (`ceremony.rs`) + adoption in `roots rm` and `apply`
- [x] Phase 2: `exclude set` and `exclude clear` confirmations (`--yes`, archive coverage, root counts)
- [x] Phase 3: `exclude duplicates` confirmation (`--yes`, statistics presentation modes)
- [x] Phase 4: Apply summary enrichment (mode line, "Sources from:") + docs fix
- [ ] Phase 5: Cluster summary comments, notes section, version field, richer stdout
- [ ] Verify all existing tests pass
- [ ] Update CLAUDE.md: document `ceremony.rs`, manifest version field, comment sections
- [ ] Update `docs/` (mdbook): apply command changes, cluster generate/refresh changes, manifest format reference

## Documentation Updates

- **`docs/` apply page**: Document enriched Mode line for rename/move, "Sources from:" section
- **`docs/` cluster page**: Document `# === Cluster Summary ===` and `# === Notes ===` sections, `version` field in `[meta]`, richer stdout output for generate and refresh
- **`docs/` exclude page**: Document `--yes` flag for set/clear/duplicates, confirmation behavior, threshold
- **`docs/` manifest format reference** (if exists): Add version field, comment section conventions
- **Correct any `--move`/`--yes` misleading text** in docs

## Backward Compatibility

- All commands continue to work as before with `--dry-run` or `--yes`.
- Interactive `exclude set/clear/duplicates` now shows confirmation when affecting > 1 source. Scripts should add `--yes` if they don't already use `--dry-run`.
- `cluster generate/refresh` produce manifests with additional comment sections. Existing manifests without these sections work normally — `apply` never reads comments.
- `cluster refresh` on existing manifests adds summary and notes sections.
- Existing manifests without `version` field deserialize as version 1 via `serde(default)`.
- `roots rm` abort exit code changes from error (1) to success (0). This is a behavioral correction — aborting is a normal action.

## Performance Considerations

- `exclude set` confirmation adds one `batch_check_archived()` call. This is the same query `coverage` and `roots rm` already use. Negligible overhead for a confirmation prompt the user will read.
- `cluster generate/refresh` compute `root_breakdown` and `not_archived_count` from data already in memory (grouping the `sources` list, checking the `archive_paths` HashMap). No additional database queries.
- `apply` "Sources from:" groups sources already in the working set. No additional queries.
