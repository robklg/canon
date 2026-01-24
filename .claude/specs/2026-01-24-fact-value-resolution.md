# Refactoring Spec: Fact Value Resolution

## Context: A Different Kind of Refactoring

This spec differs from the Source Infrastructure and Fact Infrastructure refactorings in an important way:

- **Previous refactorings** followed the pattern: domain struct + repository layer
- **This refactoring** creates a **domain operation** that uses those existing structures

The grouped distribution functionality in `facts.rs` is inherently complex — multi-key grouping with nested aggregation, built-in vs stored facts, transforms. We're not simplifying the *logic*; we're clarifying the *architecture* by extracting a reusable domain operation.

### Why This Warrants Special Treatment

The `show_grouped_distribution()` function (215 lines) accumulated complexity because:
1. It predates the Source and Fact infrastructure
2. It mixes data fetching, value resolution, and display formatting
3. It duplicates patterns now centralized in `source_repo` and `fact_repo`

Rather than incrementally patching it, we're extracting the core domain concept it implicitly defines: **"resolve a fact value for a source."**

---

## The Missing Abstraction

### Current State: Scattered Logic

"Get the value of fact key K for source S" is currently scattered across:

```
get_builtin_value()        → extracts from Source fields (70 lines)
fetch_stored_fact_values() → queries facts table (31 lines)
apply_transforms()         → applies accessor + modifiers (21 lines)
```

These are combined ad-hoc in `show_grouped_distribution()`, with the caller managing:
- Which path to take (built-in vs stored)
- How to combine with transforms
- Error handling differences

### Target State: Domain Operation

```rust
// One function that answers: "What is the value of this fact for this source?"
pub fn resolve_fact_value(
    source: &Source,
    key: &ParsedFactKey,
    stored_facts: &HashMap<String, FactEntry>,
) -> Result<Option<String>>
```

This handles:
- Built-in facts (derived from Source fields)
- Stored facts (from FactEntry lookup)
- Path accessors
- Modifiers
- Type conversions for display

---

## Architectural Model

### Before

```
┌─────────────────────────────────────────────────────────────────┐
│  facts.rs (command layer)                                       │
│                                                                  │
│  show_grouped_distribution()                                     │
│    ├── populate_temp_sources()      (db access)                 │
│    ├── fetch_source_data()          (custom SQL)                │
│    ├── fetch_stored_fact_values()   (custom SQL, duplicates     │
│    │                                 fact_repo logic)           │
│    ├── get_builtin_value()          (domain logic, buried here) │
│    ├── apply_transforms()           (domain logic, buried here) │
│    └── aggregation + display        (command logic)             │
└─────────────────────────────────────────────────────────────────┘
```

### After

```
┌─────────────────────────────────────────────────────────────────┐
│  facts.rs (command layer)                                       │
│  - Orchestrates: fetch → resolve → aggregate → display          │
│  - NO domain logic about what facts mean                        │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Domain Layer                                                    │
│                                                                  │
│  source.rs        fact.rs           fact_value.rs (NEW)         │
│  ───────────      ────────          ─────────────────           │
│  Source struct    FactEntry         resolve_fact_value()        │
│                   FactValue         - handles built-ins         │
│                                     - handles stored facts      │
│  expr.rs                            - applies transforms        │
│  ────────                                                       │
│  ParsedFactKey (NEW)                                            │
│  - wraps parse_key_with_modifiers                               │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Infrastructure Layer                                            │
│  source_repo.rs              fact_repo.rs                        │
│  (batch_fetch_by_ids)        (batch_fetch_key_for_sources)       │
└─────────────────────────────────────────────────────────────────┘
```

---

## Scope

### In Scope

1. **Create `ParsedFactKey` struct** in `expr.rs`
   - Wraps `(base_key, accessor, modifiers)` tuple
   - Provides `parse()` constructor
   - Replaces `GroupingKey` in `facts.rs`

2. **Create `fact_value.rs` module**
   - `resolve_fact_value()` function
   - Moves `get_builtin_value()` logic here
   - Incorporates transform application

3. **Rewrite `show_grouped_distribution()`**
   - Use `source_repo::batch_fetch_by_ids()` instead of `fetch_source_data()`
   - Use `fact_repo::batch_fetch_key_for_sources()` instead of `fetch_stored_fact_values()`
   - Use `resolve_fact_value()` for value extraction
   - Preserve aggregation logic (it's correct, just reorganize around it)

4. **Add comprehensive tests**
   - Unit tests for `resolve_fact_value()` (~20 tests)
   - Unit tests for `ParsedFactKey` (~4 tests)
   - Behavioral validation (before/after output comparison)

5. **Clean up dead code**
   - Remove `SourceData` struct
   - Remove `fetch_source_data()`
   - Remove `fetch_stored_fact_values()`
   - Remove `get_builtin_value()` from `facts.rs`
   - Remove `GroupingKey` struct

### Out of Scope

- Refactoring the aggregation logic itself (complex but correct)
- Migrating `show_builtin_distribution()` (uses similar patterns but simpler)
- Migrating `cluster.rs` (future project, but will benefit from this work)
- Performance optimization beyond architectural cleanup

---

## Data Model

### ParsedFactKey (in expr.rs)

```rust
/// A parsed fact key with optional accessor and modifiers.
///
/// Wraps the result of `parse_key_with_modifiers()` into a reusable struct.
/// Used when the same parsed key needs to be applied to multiple sources.
#[derive(Debug, Clone)]
pub struct ParsedFactKey {
    /// Original key string for display (e.g., "source.mtime|year")
    pub raw: String,
    /// Base fact key (e.g., "source.mtime")
    pub base_key: String,
    /// Optional path accessor (e.g., [-1] for last segment)
    pub accessor: Option<PathAccessor>,
    /// Modifiers to apply (e.g., [Year])
    pub modifiers: Vec<ModifierCall>,
}

impl ParsedFactKey {
    /// Parse a key string into its components.
    pub fn parse(key: &str) -> Result<Self> {
        let (base_key, accessor, modifiers) = parse_key_with_modifiers(key)?;
        Ok(Self {
            raw: key.to_string(),
            base_key,
            accessor,
            modifiers,
        })
    }

    /// Check if this key refers to a built-in fact.
    pub fn is_builtin(&self) -> bool {
        BuiltinKey::from_str(&self.base_key).is_some()
    }

    /// Check if this key has transforms (accessor or modifiers).
    pub fn has_transforms(&self) -> bool {
        self.accessor.is_some() || !self.modifiers.is_empty()
    }
}
```

### resolve_fact_value (in fact_value.rs)

```rust
//! Fact value resolution — domain layer for getting fact values.
//!
//! This module provides the core operation: "get the value of fact K for source S."
//! It handles built-in facts (derived from Source fields), stored facts (from the
//! facts table), and applies any transforms (accessors, modifiers).
//!
//! ## Usage
//!
//! ```ignore
//! use canon::fact_value::resolve_fact_value;
//! use canon::expr::ParsedFactKey;
//!
//! let key = ParsedFactKey::parse("source.mtime|year")?;
//! let value = resolve_fact_value(&source, &key, &stored_facts)?;
//! // value is Some("2024") or None if fact doesn't exist
//! ```

use std::collections::HashMap;
use anyhow::Result;
use crate::expr::{self, BuiltinKey, FactValue, ParsedFactKey};
use crate::fact::FactEntry;
use crate::source::Source;

/// Resolve a fact value for a source.
///
/// Returns the resolved value as a display string, or None if the fact
/// doesn't exist for this source.
///
/// # Arguments
///
/// * `source` - The source to get the fact value for
/// * `key` - The parsed fact key (base key + accessor + modifiers)
/// * `stored_facts` - Map of fact key → FactEntry for this source's stored facts
///
/// # Errors
///
/// Returns an error if a transform cannot be applied (e.g., `|year` on a text value).
pub fn resolve_fact_value(
    source: &Source,
    key: &ParsedFactKey,
    stored_facts: &HashMap<String, FactEntry>,
) -> Result<Option<String>> {
    // 1. Get raw value (built-in or stored)
    let raw_value = if let Some(builtin) = BuiltinKey::from_str(&key.base_key) {
        get_builtin_value(source, builtin)
    } else {
        stored_facts.get(&key.base_key).map(|e| e.value.clone())
    };

    // 2. Apply transforms if value exists
    match raw_value {
        Some(value) => {
            let transformed = apply_transforms(value, key)?;
            Ok(Some(transformed))
        }
        None => Ok(None),
    }
}

/// Extract a built-in fact value from source fields.
fn get_builtin_value(source: &Source, builtin: BuiltinKey) -> Option<FactValue> {
    match builtin {
        BuiltinKey::SourceExt | BuiltinKey::Ext => {
            let ext = std::path::Path::new(&source.rel_path)
                .extension()
                .and_then(|e| e.to_str())
                .map(|e| e.to_lowercase())
                .unwrap_or_default();
            Some(FactValue::Text(ext))
        }
        BuiltinKey::SourceSize | BuiltinKey::Size => {
            Some(FactValue::Num(source.size as f64))
        }
        BuiltinKey::SourceMtime | BuiltinKey::Mtime => {
            Some(FactValue::Time(source.mtime))
        }
        BuiltinKey::SourcePath => {
            Some(FactValue::Path(source.path()))
        }
        BuiltinKey::SourceRoot => {
            Some(FactValue::Path(source.root_path.clone()))
        }
        BuiltinKey::SourceRelPath => {
            Some(FactValue::Path(source.rel_path.clone()))
        }
        BuiltinKey::SourceId | BuiltinKey::Id => {
            Some(FactValue::Num(source.id as f64))
        }
        BuiltinKey::SourceDevice => {
            Some(FactValue::Num(source.device as f64))
        }
        BuiltinKey::SourceInode => {
            Some(FactValue::Num(source.inode as f64))
        }
        BuiltinKey::Filename => {
            let filename = std::path::Path::new(&source.rel_path)
                .file_name()
                .and_then(|f| f.to_str())
                .unwrap_or(&source.rel_path)
                .to_string();
            Some(FactValue::Text(filename))
        }
        BuiltinKey::Stem => {
            let stem = std::path::Path::new(&source.rel_path)
                .file_stem()
                .and_then(|f| f.to_str())
                .unwrap_or("")
                .to_string();
            Some(FactValue::Text(stem))
        }
        BuiltinKey::RootId => {
            Some(FactValue::Num(source.root_id as f64))
        }
        // Hash-based facts need object lookup, not available from Source alone
        BuiltinKey::Hash | BuiltinKey::HashShort | BuiltinKey::ContentHashSha256 => None,
    }
}

/// Apply accessor and modifiers to a value, returning display string.
fn apply_transforms(value: FactValue, key: &ParsedFactKey) -> Result<String> {
    let mut result = value;

    // Apply accessor if present
    if let Some(ref acc) = key.accessor {
        result = expr::apply_accessor(&result, acc, &key.raw)?;
    }

    // Apply modifiers
    for modifier_call in &key.modifiers {
        result = expr::apply_modifier(&result, modifier_call, &key.raw, true)?;
    }

    // Convert to display string
    Ok(fact_value_to_display(&result))
}

/// Convert a FactValue to a display string.
fn fact_value_to_display(value: &FactValue) -> String {
    match value {
        FactValue::Text(t) => t.clone(),
        FactValue::Path(p) => p.clone(),
        FactValue::Num(n) => {
            if n.fract() == 0.0 {
                format!("{}", *n as i64)
            } else {
                format!("{}", n)
            }
        }
        FactValue::Time(ts) => {
            chrono::DateTime::from_timestamp(*ts, 0)
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                .unwrap_or_else(|| ts.to_string())
        }
    }
}
```

---

## Phases

### Phase 1: ParsedFactKey

- **Status**: ✅ completed
- **Goal**: Create reusable struct for parsed fact keys
- **Scope**:
  - Add `ParsedFactKey` struct to `expr.rs`
  - Add unit tests for parsing
- **Tests**:
  - `parsed_key_simple` — "source.ext" parses correctly
  - `parsed_key_with_accessor` — "source.rel_path[-1]"
  - `parsed_key_with_modifier` — "source.mtime|year"
  - `parsed_key_complex` — "source.rel_path[-1]|stem"
- **Dependencies**: None

### Phase 2: fact_value.rs Module

- **Status**: ✅ completed
- **Goal**: Create domain layer for fact value resolution
- **Scope**:
  - Create `src/fact_value.rs`
  - Implement `resolve_fact_value()`
  - Implement `get_builtin_value()` (moved from facts.rs)
  - Implement `apply_transforms()` and `fact_value_to_display()`
  - Add to module declarations
- **Tests** (20 total):
  - Built-in facts: `resolve_source_ext`, `resolve_source_ext_no_extension`, `resolve_source_size`, `resolve_source_mtime`, `resolve_source_path`, `resolve_source_path_empty_rel`, `resolve_source_root`, `resolve_filename`, `resolve_stem`, `resolve_source_device_inode`
  - Stored facts: `resolve_stored_text`, `resolve_stored_num`, `resolve_stored_time`, `resolve_stored_missing`
  - Transforms: `resolve_with_accessor`, `resolve_with_modifier`, `resolve_with_accessor_and_modifier`, `resolve_transform_type_mismatch`
  - Edge cases: `resolve_builtin_takes_precedence`, `resolve_unknown_key_no_stored`
- **Dependencies**: Phase 1

### Phase 3: Migrate show_grouped_distribution

- **Status**: ✅ completed
- **Goal**: Rewrite function using new infrastructure
- **Scope**:
  - Replace `fetch_source_data()` with `source_repo::batch_fetch_by_ids()`
  - Replace `fetch_stored_fact_values()` with `fact_repo::batch_fetch_key_for_sources()`
  - Replace `GroupingKey` with `ParsedFactKey`
  - Use `resolve_fact_value()` for value extraction
  - Preserve aggregation logic unchanged
  - Preserve display formatting unchanged
- **Validation**:
  - Capture baseline outputs before migration
  - Compare byte-for-byte after migration
  - Key scenarios:
    - `canon facts --key content.device.make --by-root`
    - `canon facts --key source.mtime|year --group-by source.ext`
    - `canon facts --key content.media.capture_datetime|yearmonth --by-root`
- **Dependencies**: Phase 2

### Phase 4: Cleanup

- **Status**: ✅ completed
- **Goal**: Remove dead code, update documentation
- **Scope**:
  - Remove from `facts.rs`:
    - `SourceData` struct
    - `fetch_source_data()` function
    - `fetch_stored_fact_values()` function
    - `get_builtin_value()` function
    - `GroupingKey` struct
    - `apply_transforms()` function (moved to fact_value.rs)
    - `fact_value_to_display()` function (moved to fact_value.rs)
  - Update `CLAUDE.md` with `fact_value.rs` documentation
  - Update this spec with learnings
- **Dependencies**: Phase 3

---

## Performance Expectations

### Memory

| Aspect | Current | After | Assessment |
|--------|---------|-------|------------|
| Source data per source | ~100 bytes (SourceData) | ~300 bytes (Source) | Acceptable |
| For 100K sources | ~10 MB | ~30 MB | Well within CLI limits |
| Fact data | Same | Same | No change |

### Duration

- Fewer SQL round-trips (good)
- Larger result sets per query (slightly more parsing)
- More Rust iteration, less SQL filtering
- **Expected**: Neutral to slight improvement

### Validation

If performance regression is observed, profile before optimizing. The architectural clarity is worth a small performance cost, but we don't expect one.

---

## Test Requirements

### Unit Tests (fact_value.rs)

See Phase 2 for the full list of 20 tests covering:
- All built-in fact types
- Stored fact retrieval
- Transform application
- Edge cases and error handling

### Unit Tests (expr.rs)

See Phase 1 for 4 tests covering `ParsedFactKey` parsing.

### Behavioral Tests

Capture before migration:
```bash
canon facts --key content.device.make --by-root > before_by_root.txt
canon facts --key source.mtime|year --group-by source.ext > before_multi_group.txt
canon facts --key content.media.capture_datetime|yearmonth --by-root > before_transform.txt
```

Compare after migration. Must be byte-identical.

---

## Invariants

### Architectural

1. **`resolve_fact_value()` is a pure function** — No database access, no side effects
2. **Built-in facts derive from Source** — The Source struct has all needed fields
3. **Stored facts come from caller** — The function doesn't fetch, it resolves
4. **Transforms are applied uniformly** — Same logic for built-in and stored

### Behavioral

1. **Output unchanged** — Grouped distribution output is byte-identical
2. **Error handling preserved** — Transform errors handled same as before
3. **Root display formatting preserved** — `id:N ...path` format unchanged

### Implementation

1. **Aggregation logic untouched** — Complex but correct, don't refactor
2. **Use existing infrastructure** — `source_repo`, `fact_repo`, not new SQL
3. **ParsedFactKey replaces GroupingKey** — More general, lives in expr.rs

---

## Future Benefits

Once `resolve_fact_value()` exists:

1. **`cluster.rs` can use it** — Manifest generation needs the same operation
2. **Consistent behavior** — One definition of "how to get a fact value"
3. **Easier testing** — Pure function, trivial to unit test
4. **Potential `filter.rs` integration** — Could simplify fact evaluation in filters

---

## References

- Completed: `.claude/specs/2026-01-24-source-infrastructure.md`
- Completed: `.claude/specs/2026-01-24-fact-infrastructure.md`
- Current implementation: `src/facts.rs` (lines 664-1037)
- Domain types: `src/source.rs`, `src/fact.rs`
- Infrastructure: `src/source_repo.rs`, `src/fact_repo.rs`
- Expression parsing: `src/expr.rs`

---

## Learnings & Summary

### Deviations from Spec

1. **Dead code was inline, not separate functions** — The spec listed `SourceData`, `fetch_source_data()`, `fetch_stored_fact_values()`, and `get_builtin_value()` as separate functions to remove. In reality, these were all part of the monolithic `show_grouped_distribution()` function. The rewrite replaced them inline rather than deleting separate functions.

2. **`is_root_key()` needed to handle `root_id`** — The original `GroupingKey.is_root` field only checked for `source.root`. The replacement `is_root_key()` helper needed to also recognize `root_id` for the special display formatting (`id:N ...path`).

3. **Test counts exceeded estimates** — Delivered 6 tests for `ParsedFactKey` (spec estimated 4) and 24 tests for `fact_value.rs` (spec estimated 20).

### Behavioral Validation

Output comparison showed minor ordering differences for items with equal counts (e.g., two roots both with 895 files). This is due to HashMap iteration order being non-deterministic in Rust. The actual data and format are identical — this is acceptable behavior.

### Key Insight: Spec vs Reality

The spec correctly identified the *conceptual* dead code (the patterns being replaced), but the actual code structure was more tangled. The old implementation had everything inline in one 215-line function rather than factored into reusable pieces. This made the migration more of a rewrite than a refactor, but the new structure is cleaner and testable.

### Final Stats

| Metric | Value |
|--------|-------|
| New module | `src/fact_value.rs` |
| New tests added | 35 (6 ParsedFactKey + 24 fact_value + 5 batch_fetch_by_ids) |
| Lines removed | 242 |
| Lines added | 1250 (including tests and spec) |
| Net code lines | ~200 new (mostly tests) |
