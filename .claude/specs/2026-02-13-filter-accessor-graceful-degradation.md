# Story: Filter Accessor Graceful Degradation

**Design Spec**: ~/store/claude-designs/2026-02-13-filter-accessor-graceful-degradation.md
**Status**: Done
**Created**: 2026-02-13

## Objective

Make built-in key accessor/modifier errors in `--where` filters degrade gracefully to "no match" instead of hard-erroring, matching the behavior already established for stored facts.

## Functional Requirements Summary

When a `--where` filter uses a path accessor that can't resolve for a particular source (e.g., `source.rel_path[1]` on a single-segment path), that source should be silently excluded from results rather than causing the entire operation to fail. This applies to all built-in keys in filter evaluation. Pattern evaluation (manifests) and `facts --key` must continue to hard-error.

## Current State

`check_fact_compare` in `filter.rs` (line 704) handles built-in key evaluation. Each built-in key match arm calls `apply_accessor_and_modifiers(...)?`, propagating errors via `?`. There are 11 such call sites (lines 720-858).

The stored fact evaluation path (lines 877-894) already handles this correctly with `if let Ok(modified) = apply_accessor_and_modifiers(...)`.

The cached evaluation path (`check_fact_compare_cached`, line 962) delegates built-in keys to `check_fact_compare` (line 977), so fixing the uncached path covers both.

No tests exist in `filter.rs`.

## Design

### Phase 1: Graceful error handling for built-in keys in filter evaluation

- **Goal**: All 11 `apply_accessor_and_modifiers(...)?` calls in the built-in key match arms of `check_fact_compare` catch errors and return `Ok(false)` instead of propagating.

#### Changes

In `src/expr/filter.rs`, function `check_fact_compare`, change each built-in key match arm from:

```rust
// Before (all non-nullable built-in keys — 9 sites)
let fact_value = FactValue::Text(ext.to_string());
let modified =
    apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key)?;
return Ok(compare_fact_value(&modified, op, value));
```

To:

```rust
// After
let fact_value = FactValue::Text(ext.to_string());
if let Ok(modified) =
    apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key)
{
    return Ok(compare_fact_value(&modified, op, value));
}
return Ok(false);
```

For the two nullable built-in keys (`SourceDevice`, `SourceInode`), the same change applies inside their existing `if let Some(...)` block:

```rust
// Before
if let Some(d) = device {
    let fact_value = FactValue::Num(d as f64);
    let modified =
        apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key)?;
    return Ok(compare_fact_value(&modified, op, value));
}
return Ok(false);

// After
if let Some(d) = device {
    let fact_value = FactValue::Num(d as f64);
    if let Ok(modified) =
        apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key)
    {
        return Ok(compare_fact_value(&modified, op, value));
    }
}
return Ok(false);
```

**All 11 match arms to change:**

| Line | Builtin Key | Nullable |
|------|------------|----------|
| 731-733 | `SourceExt \| Ext` | No |
| 746-748 | `Filename` | No |
| 757-759 | `SourceRoot` | No |
| 773-775 | `SourcePath` | No |
| 784-786 | `SourceRelPath` | No |
| 797-799 | `SourceSize \| Size` | No |
| 809-811 | `SourceMtime \| Mtime` | No |
| 821-823 | `SourceDevice` | Yes |
| 835-837 | `SourceInode` | Yes |
| 848-850 | `RootId` | No |
| 855-857 | `SourceId \| Id` | No |

#### Tests

Add integration tests that exercise the filter path with out-of-bounds accessors. Since `check_fact_compare` requires a database connection, tests should use `apply_filters` with a populated database:

1. **test_filter_out_of_bounds_index_is_non_match**: Create sources with varying path depths. Filter with `source.rel_path[2]` — sources with 3+ segments match if the value matches, sources with fewer segments are silently skipped. Verify no error and correct results.

2. **test_filter_out_of_bounds_negative_index_is_non_match**: Filter with `source.rel_path[-3]` on sources where some have fewer than 3 segments. Verify graceful degradation.

3. **test_filter_out_of_bounds_slice_is_non_match**: Filter with `source.rel_path[2:4]` on sources with fewer than 3 segments. Verify graceful degradation.

4. **test_filter_modifier_failure_on_builtin_is_non_match**: Apply an incompatible modifier to a built-in key (e.g., `source.ext|year`). Verify graceful degradation rather than error.

## Architectural Decisions

| Decision | Rationale |
|----------|-----------|
| Change only the caller, not `apply_accessor` | `apply_accessor` correctly returns errors — the filter context is responsible for deciding errors mean "no match". Pattern evaluation must still hard-error. |
| Mechanical change, no helper extraction | 11 sites is borderline, but the change is a simple `?` to `if let Ok(...)` substitution. Matches the existing pattern at lines 877-894. |

## Non-Goals

- Changing `apply_accessor` or `apply_modifier` error behavior in `eval.rs`
- Changing pattern evaluation (manifests) error handling
- Changing `facts --key` error handling
- Adding any CLI flags or output format changes

## Test Plan

### Existing Tests (Must Pass)

- `expr::eval::tests::test_out_of_bounds_error` — verifies `apply_accessor` still returns errors (eval.rs:1349)
- All existing tests: `cargo test`

### New Tests

Integration tests as described in Phase 1 above.

## Implementation Checklist

- [ ] Phase 1: Change 11 `apply_accessor_and_modifiers(...)?` calls to `if let Ok(...)` in `check_fact_compare`
- [ ] Add integration tests for out-of-bounds accessor graceful degradation
- [ ] Verify all existing tests pass
- [ ] `cargo clippy` clean

## Backward Compatibility

No breaking changes. Queries that previously errored now return results. Existing successful queries are unaffected.

## Performance Considerations

None. The change replaces error propagation with a boolean return — no additional work.
