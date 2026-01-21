# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
cargo build          # Build the project
cargo run -- <cmd>   # Run with subcommand
cargo test           # Run all tests
cargo clippy         # Run linter
cargo fmt            # Format code
```

## Project Overview

Canon is a CLI tool for organizing large media libraries into a "canonical archive". It helps manage files scattered across multiple backups and devices, handling duplicates and providing structured organization.

### Core Concepts

- **Root**: A scanned folder, identified by canonical path. Has a role: `source` or `archive`.
- **Source**: A file on disk (identified by root + relative path). Device+inode provide physical identity for move detection.
- **Object**: Content identified by hash (sha256). Multiple sources can map to one object.
- **Facts**: Arbitrary key-value metadata (EAV model). Source facts are tied to a file path; object facts are tied to content hash.

### Architecture

- `src/main.rs` - CLI entry point using clap
- `src/path.rs` - Path utilities (pure manipulation + canonicalization)
- `src/scope.rs` - Scope domain concepts (ScopeMatch enum, SQL clause building)
- `src/db.rs` - SQLite database infrastructure (connection, schema, transactions)
- `src/scan.rs` - Directory scanning logic
- `src/worklist.rs` - JSONL worklist generation for external processing
- `src/import_facts.rs` - Fact import with staleness validation
- `src/ls.rs` - List and query sources
- `src/facts.rs` - Fact inspection and management
- `src/coverage.rs` - Archive coverage statistics
- `src/compare.rs` - Compare folders by content hash
- `src/cluster.rs` - Manifest generation with query filters
- `src/apply.rs` - File copying/moving based on manifests
- `src/exclude.rs` - Source exclusion management
- `src/roots.rs` - Root management (list, suspend/unsuspend, comment, remove)
- `src/expr.rs` - Pattern expression evaluation for manifest output; defines Modifier enum, FactValue types, and modifier application logic. Supports alias expansion and Python-style path accessors.
- `src/filter.rs` - Filter expression parsing; depends on expr.rs for modifier handling. Has hardcoded built-ins for derived facts. New derived facts require code changes here.

### Commands

- `scan` - Index directories, add files to database
- `roots` - List, suspend/unsuspend, comment, and remove roots
- `worklist` - Output sources as JSONL for external processing
- `import-facts` - Import facts from JSONL on stdin
- `ls` - List sources matching filters (supports `-l` for long format)
- `facts` - Show fact coverage and value distribution (`--key` supports modifiers/accessors)
- `coverage` - Show archive coverage statistics
- `compare` - Compare two folders by content hash
- `cluster generate` - Generate manifest from matching sources
- `apply` - Apply manifest to copy/move/rename files
- `exclude set/clear/list/duplicates` - Manage source exclusions

### Database

Default location: `~/.canon/canon.db` (override with `--db` flag)

Key tables: `roots`, `sources`, `objects`, `facts`

Roots table columns include `suspended` (integer, default 0) for temporarily hiding roots from operations, `comment` for user notes, and `last_scanned_at` timestamp.

### Filter Expressions (filter.rs)

Used with `--where`. Supports full boolean logic:

```bash
--where "source.ext=jpg AND source.size>1000000"
--where "(source.ext=jpg OR source.ext=png) AND content.hash.sha256?"
--where "NOT content.hash.sha256?"
--where "source.mtime|year=2023"
--where "source.rel_path[-1]=photo.jpg"
```

**Operators**: `=`, `!=`, `~` (glob), `!~` (not glob), `>`, `<`, `>=`, `<=`, `IN (a, b, c)`, `NOT IN (...)`, `?` (exists)

Note: `=` and `!=` are case-sensitive. Use `|lowercase` modifier for case-insensitive matching.

**Glob patterns** (`~` operator): `*` (any chars), `?` (one char), `[abc]` (char set), `[a-z]` (range), `[!abc]` (negated set)

**Modifiers**: Apply with `|` syntax (reuses expr.rs): `source.mtime|year`, `content.DateTimeOriginal|month`, `source.ext|lowercase`, `filename|capitalize`

**Path accessors**: Python-style indexing works in filters: `source.rel_path[-1]|stem=photo`

**Built-in derived facts**: filter.rs has hardcoded built-ins (like `filename`, `source.ext`) derived at query time for efficiency. These achieve the same result as the equivalent path accessor expressions (e.g., `filename` vs `source.rel_path[-1]`). Adding new derived facts requires modifying filter.rs.

**Database facts**: Any fact stored via `import-facts` can also be used in filters.

**Content prefix is optional**: The `content.` prefix is optional when specifying fact keys. Keys without a namespace prefix are automatically normalized to `content.*`. For example, `Make` becomes `content.Make`. This applies to `--where`, `--key`, `--group-by`, and manifest patterns. Built-in keys (`source.*`, `filename`, etc.) and keys with explicit prefixes (`policy.*`, `object.*`) are not modified.

### Manifest Patterns (expr.rs)

Output patterns in manifests use `{expr}` syntax:

**Path accessors** (Python-style indexing):
- `{source.rel_path[-1]}` - Last segment (filename)
- `{source.rel_path[0]}` - First segment
- `{source.rel_path[1:3]}` - Slice segments
- `{source.rel_path[:-1]}` - All but last

**Modifiers**: `|year`, `|month`, `|stem`, `|ext`, `|short`, etc.

**Aliases** (only in patterns, see `canon facts --show-aliases`):
- `{filename}` expands to `{source.rel_path[-1]}`
- `{stem}` expands to `{source.rel_path[-1]|stem}`
- etc.

Example: `pattern = "{content.DateTimeOriginal|yearmonth}/{filename}"`

### Root Specs

Used with `--root`, `--archive` flags:
- `id:N` - By database ID
- `path:/foo/bar` - By path

### Shared Utilities

In `path.rs`:
- `path_is_under()`, `path_strip_prefix()` - Pure path manipulation (no I/O)
- `canonicalize_scope()`, `canonicalize_scopes()` - Path canonicalization (filesystem I/O)

In `scope.rs`:
- `ScopeMatch` enum - Domain concept for file vs directory scope matching
- `ScopeMatch::classify_all()` - Classify paths as exact file or directory matches
- `build_scope_clause()` - SQL clause building (takes `&[ScopeMatch]`, no I/O)
- `SCOPE_CLAUSE`, `scope_param()` - Helpers for single optional scope

In `db.rs`:
- `parse_root_spec()` - Parse `id:N` or `path:/foo` format
- `resolve_root_path()`, `resolve_archive_path()` - Find roots containing paths (excludes suspended roots)
- `resolve_root_path_any()` - Find roots including suspended ones (for unsuspend command)
- `populate_temp_sources()` - Batch insert pattern for large ID sets

In other modules:
- `filter::apply_filters()` - Apply filter expressions to source IDs
- `exclude::exclude_clause()` - SQL clause for exclusion filtering

### Design Principles

- External tools for hashing/metadata (via JSONL worklist/import)
- Incremental workflow (scan -> enrich -> cluster -> apply)
- Human-editable manifest files (.toml)
- basis_rev tracks file state changes for staleness detection

### CLI Conventions

- `canon roots` and `canon roots list` must behave identically. When adding flags to `RootsAction::List`, also add them to the top-level `Roots` command so both forms work the same way.

### Type System for Facts (expr.rs)

The fact system uses several key types defined in `expr.rs`:

**BuiltinKey enum** - Represents all built-in fact keys (derived from source columns or well-known facts):

```rust
use crate::expr::BuiltinKey;

// Check if a key is built-in
if let Some(builtin) = BuiltinKey::from_str(key) {
    // Handle built-in key
    match builtin {
        BuiltinKey::SourceExt => { /* ... */ }
        BuiltinKey::SourceSize => { /* ... */ }
        // etc.
    }
} else {
    // Handle stored fact (from facts table)
}
```

Built-in keys have associated metadata:
- `builtin.visibility()` → `Default`, `Hidden`, or `NotListed` (for `canon facts` listing)
- `builtin.category()` → `BuiltIn`, `Derived`, or `Stored`
- `builtin.fact_type()` → `Text`, `Num`, `Time`, or `Path`
- `builtin.expansion()` → Pattern alias expansion (e.g., `filename` → `source.rel_path[-1]`)

**FactValue enum** - Typed fact values for processing:

```rust
use crate::expr::FactValue;

let value = FactValue::Text("hello".to_string());
let value = FactValue::Num(42.0);
let value = FactValue::Time(1704067200);  // Unix timestamp
let value = FactValue::Path("/some/path".to_string());
```

**Modifier enum** - Transformations applied to values:

```rust
use crate::expr::{Modifier, apply_modifier};

// Modifiers are parsed from key strings like "source.mtime|year"
let (base_key, accessor, modifiers) = expr::parse_key_with_modifiers("source.mtime|year")?;

// Apply modifiers to a value
let result = expr::apply_modifier(&value, Modifier::Year, "source.mtime|year")?;
```

**PathAccessor** - Python-style path indexing:

```rust
use crate::expr::{PathAccessor, apply_accessor};

// Parsed from keys like "source.rel_path[-1]"
let result = expr::apply_accessor(&path_value, &accessor, key)?;
```

**Key normalization** - The `content.` prefix is optional for user input:

```rust
use crate::expr::{normalize_fact_key, normalize_key_string};

// For base keys (no accessors/modifiers)
normalize_fact_key("Make")           // → "content.Make"
normalize_fact_key("source.ext")     // → "source.ext" (built-in)
normalize_fact_key("content.Make")   // → "content.Make" (already prefixed)

// For full key strings with accessors/modifiers
normalize_key_string("Make|year")    // → "content.Make|year"
normalize_key_string("path[-1]|stem") // → "content.path[-1]|stem"
```

**Best practices:**

1. Always use `BuiltinKey::from_str()` instead of string matching for built-in keys
2. When fetching fact values, check `BuiltinKey` first; fall back to facts table for stored facts
3. Use `FactValue` for typed value handling; apply transforms via `apply_modifier()` and `apply_accessor()`
4. For new features needing fact values, see `facts.rs:get_builtin_value()` as a reference implementation
5. Use `normalize_key_string()` when accepting user input for fact keys to ensure `content.` prefix is added
