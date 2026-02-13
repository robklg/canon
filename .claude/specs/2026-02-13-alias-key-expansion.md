# Story: Auto-Classify Expression vs Key Aliases

**Design Spec**: `~/store/claude-designs/2026-02-13-alias-key-expansion.md`
**Status**: Done
**Created**: 2026-02-13

## Objective

Enable key aliases (shorthand for verbose key paths like `source.rel_path[-1]`) in `--where` expressions by auto-classifying alias values at load time — expression aliases are wrapped in parentheses for safe composition, key aliases are substituted literally.

## Functional Requirements Summary

- A key alias (value that does not parse as a filter expression) is substituted literally, without parentheses
- An expression alias (value that parses as a valid filter expression) is wrapped in parentheses on expansion
- Classification happens at alias load time via `Expr::parse()` — deterministic, not heuristic
- No changes to `aliases.toml` format, CLI flags, or any other code
- Key paths and filter expressions are grammatically disjoint (expressions require operators; key paths cannot contain operators), making classification definitionally correct
- Existing expression aliases continue to work identically

## Current State

**`src/expr/alias.rs`** (pure layer):
- `has_alias_references()` — fast-path check for `@` outside quotes
- `expand_aliases()` — replaces `@name` with `(value)`, unconditionally wrapping in parentheses
- Comprehensive test suite (17 tests) covering expansion, quoting, errors, name validation

**`src/alias.rs`** (command layer):
- `load_aliases()` — reads `$canon_home/aliases.toml` into `HashMap<String, String>`
- `expand_filter_strings()` — orchestrates: check for `@` → load aliases → call `expand_aliases()`
- No tests (pure I/O orchestration)

**`src/expr/filter.rs`**:
- `Expr::parse(s: &str) -> Result<Expr>` — purely syntactic parser, no database context needed. Returns `Ok` for valid filter expressions, `Err` for anything else. This is the classification mechanism.

## Design

### Phase 1: Change `expand_aliases()` to literal substitution

- **Goal**: The pure expansion function does literal substitution only — no wrapping
- **Scope**: `src/expr/alias.rs`

#### Changes

In `expand_aliases()`, replace the wrapping logic:

```rust
// Before (lines 87-89):
Some(value) => {
    result.push('(');
    result.push_str(value);
    result.push(')');
}

// After:
Some(value) => {
    result.push_str(value);
}
```

Update the doc comment: remove "Parenthesis wrapping is unconditional", replace with "Values are substituted literally (caller is responsible for any wrapping)."

#### Tests

Update all existing test assertions that expect parentheses to expect literal substitution:

| Test | Before | After |
|------|--------|-------|
| `test_expand_single_alias` | `"(source.ext=jpg)"` | `"source.ext=jpg"` |
| `test_expand_multiple_aliases` | `"(source.ext=jpg) OR (source.ext=mp4)"` | `"source.ext=jpg OR source.ext=mp4"` |
| `test_expand_mixed_with_regular` | `"(source.ext=jpg) AND source.size>1000"` | `"source.ext=jpg AND source.size>1000"` |
| `test_expand_alias_with_boolean_logic` | `"(source.mtime\|year >= 2010 AND source.mtime\|year < 2020)"` | `"source.mtime\|year >= 2010 AND source.mtime\|year < 2020"` |
| `test_parentheses_always_applied` | `"(source.size > 10000000)"` | `"source.size > 10000000"` |
| `test_mixed_quoted_and_unquoted_at` | `"(source.ext=jpg) AND source.path ~ '*@2x*'"` | `"source.ext=jpg AND source.path ~ '*@2x*'"` |
| `test_alias_name_with_hyphens` | `"(source.ext=png)"` | `"source.ext=png"` |
| `test_alias_name_with_underscores` | `"(source.ext=gif)"` | `"source.ext=gif"` |
| `test_alias_name_stops_at_boundary` | `"((source.ext=jpg))"` | `"(source.ext=jpg)"` |

Rename `test_parentheses_always_applied` to `test_expand_simple_predicate` (it no longer tests wrapping).

Add new test:
- **`test_expand_pre_wrapped_value`**: Verify that a value already wrapped in parens passes through correctly. Input: aliases `{"image": "(source.ext=jpg)"}`, filter `"@image"`, expect `"(source.ext=jpg)"`. This simulates what the command layer will provide for expression aliases.

### Phase 2: Add classification in command layer

- **Goal**: Auto-classify aliases and pre-wrap expression values — feature is complete
- **Scope**: `src/alias.rs`

#### Changes

Add a private classification function:

```rust
use crate::expr::filter::Expr;

/// Classify alias values: expression aliases get wrapped in parentheses,
/// key aliases pass through unchanged. Classification uses Expr::parse() —
/// if the value parses as a valid filter expression, it's an expression alias.
fn classify_aliases(raw: HashMap<String, String>) -> HashMap<String, String> {
    raw.into_iter()
        .map(|(name, value)| {
            let processed = if Expr::parse(&value).is_ok() {
                format!("({})", value)
            } else {
                value
            };
            (name, processed)
        })
        .collect()
}
```

Update `expand_filter_strings()` to call it:

```rust
// Before:
let aliases = load_aliases(canon_home)?.unwrap_or_default();

// After:
let aliases = classify_aliases(load_aliases(canon_home)?.unwrap_or_default());
```

#### Tests

Add a `#[cfg(test)] mod tests` block in `src/alias.rs`:

1. **`test_classify_expression_alias`** — `"source.ext=jpg"` → `"(source.ext=jpg)"`
2. **`test_classify_key_alias`** — `"source.rel_path[-1]"` → `"source.rel_path[-1]"` (unchanged)
3. **`test_classify_compound_expression`** — `"source.mtime|year >= 2010 AND source.mtime|year < 2020"` → wrapped
4. **`test_classify_key_with_modifiers`** — `"source.mtime|year"` → unchanged
5. **`test_classify_key_with_accessors`** — `"source.rel_path[-1]"` → unchanged
6. **`test_classify_existence_check`** — `"content.hash.sha256?"` → wrapped (parses as `Expr::Exists`)
7. **`test_classify_empty_map`** — empty input → empty output
8. **`test_classify_mixed`** — map with both types, verify each is handled correctly

## Architectural Decisions

| Decision | Rationale |
|----------|-----------|
| Pre-wrap in command layer, literal substitution in pure layer | Classification requires `Expr::parse()` (a filter module dependency). The pure expansion function should remain a simple string substitutor with no knowledge of filter grammar. |
| No `AliasType` enum | Pre-wrapping the value is sufficient — the pure function never sees the distinction. Adding an enum would be over-engineering for no benefit. |
| Classification at load time, not per-expansion | Aliases are loaded once per invocation. Classifying in the same pass adds no overhead and keeps the logic in one place. |

## Non-Goals

- No changes to `aliases.toml` format (no sections, no type annotations)
- No changes to manifest pattern aliases (`{filename}` in `expr/eval.rs`) — separate mechanism
- No new CLI flags or commands
- No `--debug` or `--verbose` output showing classification results

## Test Plan

### Existing Tests (Must Pass)

All 17 tests in `src/expr/alias.rs` (updated assertions in Phase 1).

### New Tests

**Phase 1** (`src/expr/alias.rs`):
- `test_expand_pre_wrapped_value`

**Phase 2** (`src/alias.rs`):
- `test_classify_expression_alias`
- `test_classify_key_alias`
- `test_classify_compound_expression`
- `test_classify_key_with_modifiers`
- `test_classify_key_with_accessors`
- `test_classify_existence_check`
- `test_classify_empty_map`
- `test_classify_mixed`

## Implementation Checklist

- [x] Phase 1: Literal substitution in `expand_aliases()`, update tests
- [x] Phase 2: Add `classify_aliases()` in command layer, add tests
- [x] Verify all existing tests pass (`cargo test`)
- [x] Verify `cargo clippy` passes

## Backward Compatibility

Fully backward compatible. Existing expression aliases continue to be wrapped in parentheses (now by the command layer instead of the pure layer). The only user-visible change is that key aliases — which previously caused parse errors — now work correctly. This is purely additive.

## Performance Considerations

None. `Expr::parse()` is called once per alias at load time (typically <10 aliases). The tokenizer and parser are fast and allocation-light. No measurable impact.
