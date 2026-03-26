# Story: Status Predicates in the Filter Language

**Design Spec**: `~/store/claude-designs/2026-03-25-status-predicates.md`
**Status**: Complete
**Created**: 2026-03-25

## Objective

Canon's most important filtering question — "what still needs my attention?" — isn't expressible in the query language. Status predicates bring archive status, hash status, exclusion status, and enrichment status into the `--where` filter system as composable, boolean-only predicates. This enables the "treasure hunt" workflow: focusing Canon's analytical tools on unresolved content across all discovery commands.

This also removes the bespoke `ls` filter flags (`--archived`, `--unarchived`, `--unhashed`, `--excluded`) that status predicates supersede, cleaning the CLI surface pre-1.0.

## Functional Requirements Summary

**Four status predicates** recognized as bare keywords in `--where`:
- `archived?` — source has `object_id` AND that object exists in at least one archive root (including suspended — matches existing `batch_check_archived()` semantics)
- `hashed?` — source has `object_id` (content hash computed)
- `excluded?` — source excluded at source-level OR object-level (`Source::is_excluded()` semantics)
- `enriched?` — source has any stored fact (source-level or object-level), excluding `content.hash.sha256`

**Design principles:**
- Status predicates are global facts about a source — true or false regardless of scope
- Syntactically identical to fact-existence checks (`key?`), semantically different (computed state, not stored data)
- Boolean-only: work with `?` and `NOT ... ?`, error on comparison operators
- Bare keywords, no namespace — recognized before `content.` normalization
- Not pattern-expandable (`{archived}` in manifests is an error)

**Visibility mismatch warning:** When `excluded?` is used without `--include excluded` and there are hidden excluded sources, show the existing hint pattern on stderr.

**`ls` flag removal:** Remove `--archived` (including `=show`), `--unarchived`, `--unhashed`, `--excluded`. Keep `--duplicates` (display mode, not filter).

**Discoverability:** `canon facts` (no arguments) shows status predicates in a separate section.

**Documentation:** Filter reference gains a "Status Predicates" section. `ls` docs updated.

## Current State

**Filter engine** (`expr/filter.rs`): Two-phase system — bulk prefetch into `FactCache`, then per-source evaluation. Entry point `apply_filters(conn, source_ids, filters) -> Vec<i64>`. AST has `Expr::Exists { key }` for `?` checks. `content.hash.sha256?` is already special-cased to check `object_id IS NOT NULL`.

**Key normalization** (`expr/eval.rs`): `BuiltinKey::from_str()` checks for built-in keys. Unknown bare keys get `content.` prefix via `normalize_fact_key()`.

**`apply_filters()` call sites** (4 production): `ops/selection.rs:115`, `ops/exclude.rs:219`, `ops/survey.rs:130`, `ops/survey.rs:290`.

**`ls` filter modes**: Clap struct has `archived` (with optional `=show`), `unarchived`, `unhashed`, `excluded` — all mutually exclusive via `conflicts_with_all`. Post-selection `filter_by_mode()` in `ops/ls.rs` applies these. `LsMode` enum, `LsEntry`, `LsModeResult` types support this.

**Archive status lookup**: `repo::object::batch_check_archived(conn, object_ids, None)` returns `HashSet<i64>` of archived object IDs. Already used by `ops/ls.rs::filter_by_mode()` and `ops/survey.rs`.

**Selection result**: `Selection` struct has `excluded_count`, `included_excluded_count`, `included_archived_count`. The "N excluded hidden" hint is currently `ls`-specific.

## Design

### Phase 1: Filter Engine — Status Predicates

- **Goal**: Status predicates parse, evaluate, and return metadata in the filter result.
- **Scope**: New AST variant, parser recognition, cache extension, evaluation paths, return type change, caller updates.

#### Changes

**New types** in `expr/filter.rs`:

```rust
/// Status predicates — computed boolean state, not stored facts.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StatusPredicate {
    /// Content exists in at least one archive root (including suspended).
    Archived,
    /// Content hash has been computed (source has object_id).
    Hashed,
    /// Source or object is excluded.
    Excluded,
    /// Source has any stored fact, excluding content.hash.sha256.
    Enriched,
}

/// Keywords recognized as status predicates before normalization.
const STATUS_KEYWORDS: &[(&str, StatusPredicate)] = &[
    ("archived", StatusPredicate::Archived),
    ("hashed", StatusPredicate::Hashed),
    ("excluded", StatusPredicate::Excluded),
    ("enriched", StatusPredicate::Enriched),
];
```

**New `Expr` variant:**

```rust
pub enum Expr {
    And(Vec<Expr>),
    Or(Vec<Expr>),
    Not(Box<Expr>),
    Exists { key: String },
    Compare { key: String, op: CompareOp, value: String },
    In { key: String, values: Vec<String> },
    Status(StatusPredicate),  // NEW
}
```

**New return type:**

```rust
/// Result of applying filters, including metadata about which status predicates were used.
pub struct FilterResult {
    pub source_ids: Vec<i64>,
    pub used_status: UsedStatus,
}

/// Flags indicating which status predicates appeared in the filter expression.
/// Set by walking the AST before evaluation — no interface-layer interpretation needed.
#[derive(Debug, Default, Clone)]
pub struct UsedStatus {
    pub archived: bool,
    pub hashed: bool,
    pub excluded: bool,
    pub enriched: bool,
}
```

**FactCache extension:**

```rust
struct FactCache {
    // existing fields unchanged
    source_facts: HashMap<(i64, String), FactValue>,
    object_facts: HashMap<(i64, String), FactValue>,
    source_objects: HashMap<i64, i64>,
    prefetched_keys: HashSet<String>,
    // NEW: lazily populated status predicate data
    archived_objects: Option<HashSet<i64>>,   // object_ids in archive roots
    excluded_sources: Option<HashSet<i64>>,   // source_ids that are excluded (either level)
    enriched_sources: Option<HashSet<i64>>,   // source_ids with facts beyond hash
}
```

**Parser changes** in `parse_atom()`:

Current flow: consume identifier → normalize → check for `?`/operator.
New flow: consume identifier → check if status keyword → if yes: expect `?` or error → if no: proceed with normalization.

```rust
// After consuming identifier, before normalization:
if let Some((_, predicate)) = STATUS_KEYWORDS.iter().find(|(kw, _)| *kw == &ident) {
    // Must be followed by '?'
    if matches!(tokens.get(pos), Some(Token::Exists)) {
        pos += 1;
        return Ok(Expr::Status(*predicate));
    } else {
        anyhow::bail!("'{}' is a status predicate and only supports the '?' operator", ident);
    }
}
// Otherwise: proceed with existing normalization and parsing
```

**Prefetch changes** in `prefetch_facts()`:

Add a helper that walks the expression tree to detect which status predicates are present. Only compute what's needed:

```rust
fn detect_status_predicates(expr: &Expr) -> UsedStatus { ... }
```

Then after existing prefetch logic:

- If `Archived` detected: call `repo::object::batch_check_archived(conn, &object_ids, None)?` → store in `cache.archived_objects`
- If `Excluded` detected: query for excluded source IDs (both levels) → store in `cache.excluded_sources`
- If `Enriched` detected: query for source IDs with facts beyond `content.hash.sha256` → store in `cache.enriched_sources`
- If only `Hashed` detected: no extra prefetch — uses existing `source_objects` map

**Excluded prefetch query:**

```sql
SELECT DISTINCT s.id FROM temp_sources ts
JOIN sources s ON s.id = ts.id
WHERE s.excluded = 1
UNION
SELECT DISTINCT s.id FROM temp_sources ts
JOIN sources s ON s.id = ts.id
JOIN objects o ON o.id = s.object_id
WHERE o.excluded = 1
```

**Enriched prefetch query:**

```sql
SELECT DISTINCT ts.id FROM temp_sources ts
JOIN facts f ON f.entity_type = 'source' AND f.entity_id = ts.id
    AND f.key != 'content.hash.sha256'
UNION
SELECT DISTINCT s.id FROM temp_sources ts
JOIN sources s ON s.id = ts.id
JOIN facts f ON f.entity_type = 'object' AND f.entity_id = s.object_id
    AND f.key != 'content.hash.sha256'
```

**Evaluation** — new arm in `eval_expr_cached`:

```rust
Expr::Status(predicate) => match predicate {
    StatusPredicate::Hashed => {
        Ok(cache.get_object_id(source_id).is_some())
    }
    StatusPredicate::Archived => {
        let archived_set = cache.archived_objects.as_ref().unwrap();
        Ok(cache.get_object_id(source_id)
            .is_some_and(|oid| archived_set.contains(&oid)))
    }
    StatusPredicate::Excluded => {
        let excluded_set = cache.excluded_sources.as_ref().unwrap();
        Ok(excluded_set.contains(&source_id))
    }
    StatusPredicate::Enriched => {
        let enriched_set = cache.enriched_sources.as_ref().unwrap();
        Ok(enriched_set.contains(&source_id))
    }
}
```

**Key validation**: `validate_filter_keys()` walks `Expr::Exists` and `Expr::Compare` nodes. The new `Expr::Status` variant has no key to validate — skip it in the walker.

**`used_status` computation**: The `detect_status_predicates()` walker that runs during prefetch also produces the `UsedStatus` flags. Return them as part of `FilterResult`.

**Caller updates** (4 sites):

For `ops/selection.rs` and `ops/exclude.rs` — mechanical destructure:

```rust
// Before:
let passing_ids = filter::apply_filters(conn, &source_ids, &params.filters)?;

// After:
let filter_result = filter::apply_filters(conn, &source_ids, &params.filters)?;
let passing_ids = filter_result.source_ids;
```

For `ops/survey.rs` (2 sites) — destructure plus thread `used_status` into `SurveyResult`:

```rust
// Selection-side filter (line 130): capture used_status for the interface hint
let filter_result = filter::apply_filters(conn, &ids, filters)?;
let passed: HashSet<i64> = filter_result.source_ids.into_iter().collect();
// ... thread filter_result.used_status into SurveyResult

// Location-side filter (line 290): used_status not needed (internal computation)
let filter_result = filter::apply_filters(conn, &loc_ids, filters)?;
let passed: HashSet<i64> = filter_result.source_ids.into_iter().collect();
```

**Selection struct extension** (in `ops/selection.rs`):

```rust
pub struct Selection {
    pub sources: Vec<Source>,
    pub excluded_count: usize,
    pub included_excluded_count: usize,
    pub included_archived_count: usize,
    pub used_status: UsedStatus,  // NEW — propagated from FilterResult
}
```

When `select_sources()` calls `apply_filters()`, propagate the `used_status` from the result. When no filters are applied, use `UsedStatus::default()` (all false).

**SurveyResult extension** (in `ops/survey.rs`):

Survey uses custom selection, not `select_sources()`. Thread filter metadata through its own result type:

```rust
pub struct SurveyResult {
    // existing fields...
    pub used_status: UsedStatus,    // NEW — from selection-side FilterResult
    pub excluded_count: usize,      // NEW — count of excluded sources hidden from selection
}
```

`excluded_count` is computed during survey's custom selection: count sources filtered out by `!s.is_excluded()` when `include.includes_excluded()` is false.

#### Tests

**Parser tests** (in `expr/filter.rs`):
- `parse_status_predicate_archived` — `"archived?"` parses to `Expr::Status(Archived)`
- `parse_status_predicate_hashed` — `"hashed?"` parses to `Expr::Status(Hashed)`
- `parse_status_predicate_excluded` — `"excluded?"` parses to `Expr::Status(Excluded)`
- `parse_status_predicate_enriched` — `"enriched?"` parses to `Expr::Status(Enriched)`
- `parse_status_predicate_in_not` — `"NOT archived?"` parses to `Expr::Not(Status(Archived))`
- `parse_status_predicate_composed` — `"archived? AND mime~image/*"` parses correctly
- `parse_status_predicate_error_on_compare` — `"archived = true"` produces error mentioning status predicate and `?` operator
- `parse_status_predicate_error_on_glob` — `"hashed ~ something"` produces same category of error
- `parse_non_status_keyword_normalizes` — `"archival?"` parses to `Expr::Exists { key: "content.archival" }` (not a status keyword, gets `content.` prefix)

**Evaluation tests** (integration with in-memory DB):
- `filter_archived_matches` — sources with content in archive pass `archived?`
- `filter_archived_excludes_unhashed` — unhashed sources fail `archived?`
- `filter_archived_excludes_unarchived_hashed` — hashed but not archived sources fail `archived?`
- `filter_not_archived_includes_unhashed` — unhashed sources pass `NOT archived?`
- `filter_hashed_matches` — sources with object_id pass `hashed?`
- `filter_hashed_equivalence` — `hashed?` and `content.hash.sha256?` produce identical results
- `filter_excluded_source_level` — source-excluded passes `excluded?`
- `filter_excluded_object_level` — object-excluded passes `excluded?`
- `filter_excluded_non_excluded_fails` — normal sources fail `excluded?`
- `filter_enriched_with_object_facts` — source with object-level facts (beyond hash) passes `enriched?`
- `filter_enriched_with_source_facts` — unhashed source with source-level facts passes `enriched?`
- `filter_enriched_hash_only_fails` — source with only `content.hash.sha256` fact fails `enriched?`
- `filter_enriched_no_facts_fails` — source with no facts at all fails `enriched?`
- `filter_composed_not_archived_and_hashed` — `NOT archived? AND hashed?` matches old `--unarchived` behavior
- `filter_composed_complex` — `NOT archived? AND mime~image/*` works end-to-end

**Metadata tests:**
- `filter_result_flags_archived_used` — expression with `archived?` sets `used_status.archived`
- `filter_result_flags_not_set_when_unused` — expression without status predicates has all flags false
- `filter_result_flags_nested_detection` — `NOT (archived? AND excluded?)` sets both flags
- `select_sources_propagates_used_status` — `Selection.used_status` reflects filter metadata

### Phase 2: Remove `ls` Filter Mode Flags

- **Goal**: Clean `ls` interface — status predicates replace bespoke flags.
- **Scope**: Remove clap fields, delete `LsMode`/`LsEntry`/`LsModeResult`/`filter_by_mode()`, simplify `ls::run()`.
- **Depends on**: Phase 1

#### Changes

**Remove from `Ls` clap struct** in `main.rs`:
- `archived: Option<String>` (and its `conflicts_with_all`)
- `unarchived: bool`
- `unhashed: bool`
- `excluded: bool`

Keep: `duplicates: bool`, `include`, `filters` (`--where`), all display flags (`long`, `sort`, `reverse`, `null_delim`).

`--duplicates` no longer needs `conflicts_with_all` against the removed flags. It may retain conflicts with itself only (or none — it's a standalone display mode).

**Remove from `ops/ls.rs`:**
- `LsMode` enum
- `LsEntry` struct
- `LsModeResult` struct
- `filter_by_mode()` function

Keep: `find_duplicate_groups()`, `DuplicateGroup` (used by `--duplicates`).

**Simplify `ls::run()` signature:**

```rust
// Before:
pub fn run(db, scope_paths, roots, filter_strs, archived_mode, unarchived_only,
           unhashed_only, excluded_only, include, use_relative, long, sort, reverse, null_delim)

// After:
pub fn run(db, scope_paths, roots, filter_strs, include, use_relative,
           long, sort, reverse, null_delim)
```

**Simplify `ls::run()` body:**
- Remove `LsMode` determination logic (`match (archived_mode, unarchived_only, ...)`)
- Remove `filter_by_mode()` call
- Work directly with `sel.sources` instead of `mode_result.entries`
- Remove archive path display logic (was `LsEntry.archive_path` in `ArchivedShow` mode)
- Remove footer hint "N unhashed skipped, use --unhashed to see"
- Keep "N excluded hidden" footer (generalized in Phase 3)
- Keep status column logic (`status_indicator()`) — it's about `--include`, not filter modes

**Update dispatch in `main.rs`:**
- Remove the `if excluded { include.excluded = true; }` special case — no longer needed
- Remove `archived_mode`, `unarchived_only`, `unhashed_only`, `excluded_only` from the `ls::run()` call
- Keep `duplicates` dispatch to `show_duplicates()`

#### Tests

- Remove `test_ls_archived_flag_counts_sources_not_objects` (tests deleted code)
- Phase 1 evaluation tests already cover the replacement behavior (`filter_archived_matches`, `filter_composed_not_archived_and_hashed`, etc.)

### Phase 3: Visibility Mismatch Warning

- **Goal**: Generalize the "N excluded hidden" hint to all `--where`-capable commands using filter metadata.
- **Scope**: Interface-layer hint logic using `Selection.used_status`.
- **Depends on**: Phase 1

#### Changes

Two hint mechanisms, both in the interface layer:

**1. `ls` keeps its existing unconditional hint** — unchanged from today. Fires whenever there are hidden excluded sources, regardless of filters. This is valuable awareness for users who don't yet know about excluded sources:

```rust
// Existing ls behavior — DO NOT change to require used_status.excluded
if !include.includes_excluded() && sel.excluded_count > 0 {
    eprintln!("({} excluded sources hidden, use --include excluded to show)", sel.excluded_count);
}
```

**2. Other commands with `--where` and `--include`** gain a status-predicate-triggered hint. These commands don't currently show excluded hints at all. The hint fires when the user's filter references `excluded?` but visibility is closed:

```rust
// New hint for facts, coverage, survey, worklist, compare:
if sel.used_status.excluded && !include.includes_excluded() && sel.excluded_count > 0 {
    eprintln!("({} excluded sources hidden, use --include excluded to show)", sel.excluded_count);
}
```

Applied in:
- `ls.rs` — keep existing unconditional hint (no change)
- `facts.rs` — add status-predicate-triggered hint after distribution output
- `coverage.rs` — add hint after coverage output
- `survey.rs` — add hint after survey output (requires `used_status` in `SurveyResult`, see below)
- `worklist.rs` — add hint after worklist output
- `compare.rs` — add hint after compare output

Note: `archived?` and `hashed?` don't have visibility mismatch scenarios — they operate on source-root content which is visible by default. `enriched?` similarly has no visibility issue. Only `excluded?` interacts with `--include`.

#### Tests

- `visibility_hint_excluded_without_include` — integration test: apply `--where 'excluded?'` without `--include excluded`, verify stderr contains the hint message. One test is sufficient — the mechanism is shared across commands.

### Phase 4: Documentation & Discoverability

- **Goal**: Users can discover and understand status predicates.
- **Scope**: Filter reference docs, `ls` docs, `canon facts` listing.
- **Depends on**: Phases 1-3

#### Changes

**`docs/src/reference/filter.md`** — new "Status Predicates" section:

- Introduce status predicates as a distinct category: computed state, not stored data
- Document all four: `archived?`, `hashed?`, `excluded?`, `enriched?` with semantics
- Make the distinction explicit: `content.Make?` asks "does this fact exist?" — `archived?` asks "is this source in this state?"
- Document `hashed?` as the idiomatic form; `content.hash.sha256?` is equivalent but not promoted
- Document that `enriched?` checks for any stored fact (source-level or object-level) excluding `content.hash.sha256`
- Document the design principle: status predicates are global facts about a source — true or false regardless of scope
- Document the set as closed — four predicates, not user-extensible
- Show composition examples including the treasure hunt workflow

**`docs/src/commands/query/ls.md`**:
- Remove `--archived`, `--unarchived`, `--unhashed`, `--excluded` documentation
- Add `--where` equivalents as examples
- Note that `--archived=show` is superseded by `canon survey --detail archived`

**`canon facts` listing** (in `facts.rs`):
- When displaying available keys (no `--key` argument), add a "Status Predicates" section after the fact keys listing
- Show all four predicates with one-line descriptions:
  ```
  Status Predicates (use in --where):
    archived?    content exists in an archive
    hashed?      content hash has been computed
    excluded?    source or object is excluded
    enriched?    has metadata beyond content hash
  ```

**Update CLAUDE.md**:
- Document `StatusPredicate` enum and `Expr::Status` variant in the Expression System section
- Document `FilterResult` return type and `UsedStatus` propagation through `Selection`
- Note that status predicates are recognized in the parser before normalization
- Add `enriched?` to the list of status predicates with its semantics

#### Tests

None (documentation and display changes only).

## Architectural Decisions

| Decision | Rationale |
|----------|-----------|
| `Expr::Status` variant rather than overloading `Expr::Exists` | Type-safe separation; no string matching at evaluation time; parser is the single recognition point for status keywords |
| Lazy-populated cache fields (`Option<HashSet>`) | `archived?` and `enriched?` require DB queries; don't pay for them when the expression doesn't use the predicate |
| Status keyword error is predicate-centric, not operator-aware | "'{name}' is a status predicate and only supports the '?' operator" — the error knows what the keyword IS, not what operator was attempted. Clean domain error. |
| `enriched?` excludes `content.hash.sha256` | Hashing is identification, everything else is enrichment. |
| `UsedStatus` computed by tree walker, not during evaluation | Single pass before evaluation, O(n) in expression tree size. Clean separation from evaluation logic. |
| `FilterResult` propagated through `Selection` | Interface layer reads metadata from Selection, never interprets filter AST. Domain tells you what happened, interface decides what to say. |
| Status predicate SQL lives in `expr/filter.rs` alongside existing filter SQL | `expr/filter.rs` already contains all filter-related SQL (prefetch, builtin key lookups, key validation, fact value queries). This is a known layer violation — the entire filter engine mixes expr and repo concerns. Status predicate prefetch queries (archived, excluded, enriched) follow the same pattern for consistency. Importantly, the new code uses batch-computed `HashSet`s (not per-source queries), making it straightforward to extract to repo when the filter engine is refactored. Do not deepen the violation further — no new per-source SQL queries. |

## Non-Goals

- Coverage semantic redundancy warning for `archived?` (coverage already has `--where`; see functional spec for design notes)
- `present?` or other future status predicates (set is closed at four for now)
- Changing how `--duplicates` works (display mode, not a filter)
- Deprecation messages for removed `ls` flags (pre-1.0 clean break)
- Refactoring existing filter engine SQL out of `expr/filter.rs` to repo layer (planned as immediate follow-up story)

## Test Plan

### Existing Tests (Must Pass)
- 20 filter parsing/evaluation tests in `expr/filter.rs`
- 14 selection tests in `ops/selection.rs`
- 3 duplicate detection tests in `ops/ls.rs`
- 28 alias expansion tests in `expr/alias.rs`
- All other existing tests unaffected

### New Tests
- **Phase 1**: 9 parser tests + 15 evaluation tests + 4 metadata tests = 28 tests
- **Phase 2**: Remove 1 obsolete test
- **Phase 3**: 1 visibility hint integration test
- **Phase 4**: None

**Total new tests: 28**

## Implementation Checklist

- [x] Phase 1: Status predicates in filter engine (parser, evaluator, return type, cache, callers)
- [x] Phase 2: Remove `ls` filter mode flags (`--archived`, `--unarchived`, `--unhashed`, `--excluded`)
- [x] Phase 3: Visibility mismatch warning (generalize excluded hint to all `--where` commands)
- [x] Phase 4: Documentation & discoverability (filter docs, ls docs, `canon facts` listing, CLAUDE.md)
- [x] Verify all existing tests pass
- [x] Update CLAUDE.md with new patterns

## Documentation Updates

- `docs/src/reference/filter.md` — new "Status Predicates" section
- `docs/src/commands/query/ls.md` — remove filter flags, add `--where` equivalents
- `docs/src/commands/query/facts.md` — mention status predicates in examples
- Other command docs — update examples where status predicates are natural (survey, worklist)
- CLAUDE.md — document new types and patterns

## Backward Compatibility

**Breaking changes (pre-1.0, acceptable):**
- `ls --archived`, `--unarchived`, `--unhashed`, `--excluded` flags removed
- `ls --archived=show` removed (superseded by `canon survey --detail archived`)
- `filter_by_mode()`, `LsMode`, `LsEntry`, `LsModeResult` deleted from `ops/ls`
- `apply_filters()` return type changes from `Vec<i64>` to `FilterResult`

**Non-breaking:**
- `content.hash.sha256?` continues to work unchanged
- `--duplicates` flag unchanged
- `--include archived`, `--include excluded` unchanged
- All other `--where` expressions unchanged

## Performance Considerations

- **`archived?`**: One `batch_check_archived()` call per filter evaluation. Already chunked for large ID sets. No per-source overhead.
- **`excluded?`**: One query per filter evaluation. Uses temp table join. Same scale characteristics as existing fact prefetch.
- **`enriched?`**: One query per filter evaluation. Uses temp table join with `key != 'content.hash.sha256'` exclusion. Same scale characteristics.
- **`hashed?`**: No extra query — uses existing `source_objects` map in FactCache.
- **Lazy computation**: Status predicate data is only computed when the expression tree contains the corresponding predicate. No cost when unused.
- **No regression for non-status filters**: Existing filter performance unchanged — status predicate detection is a lightweight tree walk added to the prefetch phase.
