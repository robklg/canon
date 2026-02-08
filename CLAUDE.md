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

The codebase is organized into three namespaces (domain/, repo/, expr/) plus command modules:

**Domain Layer** (`src/domain/`) - Pure concepts, no I/O:
- `source.rs` - Source struct and predicates (`is_excluded()`, `matches_scope()`, etc.)
- `root.rs` - Root struct, RootSpec enum, predicates (`is_suspended()`, `is_source()`)
- `object.rs` - Object struct and `is_excluded()` predicate
- `fact.rs` - FactEntry struct, re-exports FactValue/FactType
- `scope.rs` - ScopeMatch enum for file vs directory scope matching
- `path.rs` - Pure path utilities (`path_is_under()`, `path_strip_prefix()`)
- `scan.rs` - Scan reconciliation logic (`FileObservation`, `Reconciliation`, `reconcile()`, `find_missing()`)
- `exclusion.rs` - Duplicate exclusion logic (`find_excludable_duplicates()`)

**Repository Layer** (`src/repo/`) - Database access:
- `db.rs` - Connection, schema, transactions (`Db`, `open_with_options()`)
- `source.rs` - Source batch fetching and writes (`batch_fetch_by_roots()`, `fetch_sources_by_object_ids()`, `insert_destination()`, `apply_reconciliation()`)
- `root.rs` - Root batch fetching (`fetch_all()`, `batch_fetch_by_ids()`)
- `object.rs` - Object batch fetching, archive detection (`batch_check_archived()`, `batch_find_archive_info_by_hash()`)
- `fact.rs` - Fact batch fetching (`batch_fetch_for_sources()`, `batch_fetch_key_for_sources()`)

**Expression System** (`src/expr/`) - Pattern and filter handling:
- `eval.rs` - Pattern evaluation, modifiers, accessors, FactValue types
- `filter.rs` - Filter expression parsing for `--where` clauses
- `value.rs` - Fact value resolution for sources

**Command Modules** (flat in `src/`):
- `main.rs` - CLI entry point using clap
- `ls.rs` - List and query sources
- `coverage.rs` - Archive coverage statistics
- `cluster.rs` - Manifest generation with query filters
- `apply.rs` - File copying/moving based on manifests
- `exclude.rs` - Source exclusion management
- `facts.rs` - Fact inspection and management
- `roots.rs` - Root management (list, suspend/unsuspend, comment, remove)
- `compare.rs` - Compare folders by content hash
- `scan.rs` - Directory scanning logic
- `worklist.rs` - JSONL worklist generation for external processing
- `import_facts.rs` - Fact import with staleness validation

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

**SQL Batching Requirement:** Any SQL with `WHERE ... IN (...)` clauses MUST handle large ID lists. SQLite has a variable limit (~999-32K depending on version). Use one of these patterns:
- **Chunking:** `for chunk in ids.chunks(BATCH_SIZE)` with `BATCH_SIZE = 1000` (see `repo/source.rs`)
- **Temp table:** `repo::db::populate_temp_sources()` then JOIN (see `repo/fact.rs`)

### Filter Expressions (expr/filter.rs)

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

**Built-in derived facts**: `expr/filter.rs` has hardcoded built-ins (like `filename`, `source.ext`) derived at query time for efficiency. These achieve the same result as the equivalent path accessor expressions (e.g., `filename` vs `source.rel_path[-1]`). Adding new derived facts requires modifying `expr/filter.rs`.

**Database facts**: Any fact stored via `import-facts` can also be used in filters.

**Content prefix is optional**: The `content.` prefix is optional when specifying fact keys. Keys without a namespace prefix are automatically normalized to `content.*`. For example, `Make` becomes `content.Make`. This applies to `--where`, `--key`, `--group-by`, and manifest patterns. Built-in keys (`source.*`, `filename`, etc.) and keys with explicit prefixes (`policy.*`, `object.*`) are not modified.

### Manifest Patterns (expr/eval.rs)

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

In `domain/path.rs`:
- `path_is_under()`, `path_strip_prefix()` - Pure path manipulation (no I/O)
- `canonicalize_scope()`, `canonicalize_scopes()` - Path canonicalization (filesystem I/O)

In `domain/scope.rs`:
- `ScopeMatch` enum - Domain concept for file vs directory scope matching
- `ScopeMatch::classify_all()` - Classify paths as exact file or directory matches
- `build_scope_clause()` - SQL clause building (takes `&[ScopeMatch]`, no I/O)

In `domain/root.rs`:
- `Root` struct - The authoritative definition of a root with all fields
- `root.is_suspended()`, `root.is_active()` - Suspension state predicates
- `root.is_source()`, `root.is_archive()` - Role predicates
- `root.matches_scope(scope)` - Check if root relates to a path (bidirectional)
- `RootSpec` enum - Domain concept for how users identify roots (`id:N` or `path:/foo`)
- `RootSpec::parse()` - Parse root spec string (pure, no I/O)
- `find_containing_root()` - Match a path against candidate roots (pure, no I/O)
- `parse_root_spec()`, `parse_root_spec_any()` - Parse and resolve root specs (orchestration)
- `resolve_root_path()`, `resolve_root_path_any()` - Find roots containing paths (orchestration)
- `resolve_archive_path()` - Find archive root containing a path

In `repo/root.rs`:
- `fetch_all(conn)` - Fetch all roots ordered by ID
- `batch_fetch_by_ids(conn, root_ids)` - Fetch specific roots by ID (HashMap)

In `repo/db.rs`:
- `Db` struct, `open_with_options()` - Database connection and initialization
- `populate_temp_sources()` - Batch insert pattern for large ID sets

In `domain/source.rs`:
- `Source` struct - The authoritative definition of a source with all fields
- `NewSource` struct - Input data for inserting a new source (destination) record
- `source.path()` - Compute full absolute path (handles empty rel_path)
- `source.matches_scope(&scopes)` - Check if source is under given paths
- `source.is_excluded()` - Check exclusion (source-level OR object-level)
- `source.is_from_role(role)` - Check root role ("source" or "archive")
- `source.is_active()` - Check if root is not suspended

In `repo/source.rs`:
- `batch_fetch_by_roots(conn, root_ids)` - Fetch all present sources for roots
- `batch_fetch_by_ids(conn, source_ids)` - Fetch specific sources by ID
- `fetch_source_ids_by_roots(conn, root_ids)` - Get just IDs (for pagination)
- `insert_destination(conn, &NewSource)` - Insert/update destination source, returns complete `Source`
- Uses `BATCH_SIZE = 1000` for SQL IN clause chunking

In `domain/fact.rs`:
- `FactEntry` struct - A fact associated with a source (key, value, entity_type, entity_id)
- Re-exports `FactValue`, `FactType` from `expr/eval.rs` for convenience

In `repo/fact.rs`:
- `batch_fetch_key_for_sources(conn, source_ids, key)` - Fetch specific key (returns `HashMap<i64, Option<FactEntry>>`)
- `count_fact_keys(conn, source_ids)` - Count distinct fact keys with types
- Transparently merges source facts + object facts, keyed by source_id

In `domain/object.rs`:
- `Object` struct - Content identified by hash (id, hash_type, hash_value, excluded)
- `object.is_excluded()` - Check if object is excluded (excludes ALL linked sources)

In `repo/object.rs`:
- `batch_fetch_by_ids(conn, object_ids)` - Fetch objects by ID (returns `HashMap<i64, Object>`)
- `batch_check_archived(conn, object_ids, archive_root_id)` - Check which objects are in archive(s) (returns `HashSet<i64>`)
- `batch_find_archive_paths(conn, object_ids)` - Get archive paths for objects by object_id (returns `HashMap<i64, Vec<String>>`)
- `batch_find_archive_info_by_hash(conn, hash_values)` - Get archive info by content hash for manifest workflows (returns `HashMap<String, Vec<(i64, String)>>` — archive_root_id + path)
- Uses `BATCH_SIZE = 1000` for SQL IN clause chunking

In `expr/filter.rs`:
- `apply_filters()` - Apply filter expressions to source IDs

### Design Principles

- External tools for hashing/metadata (via JSONL worklist/import)
- Incremental workflow (scan -> enrich -> cluster -> apply)
- Human-editable manifest files (.toml)
- basis_rev tracks file state changes for staleness detection

### Cluster/Apply Workflow

The `cluster generate` and `apply` commands work together:

**cluster generate**:
- Uses `repo::source::batch_fetch_by_roots()` + domain predicates for source selection
- Uses `repo::fact::batch_fetch_for_sources()` for batch fact fetching
- Computes 100% coverage facts in-memory from the batch result
- Lock file contains source identity + staleness data only (no fact snapshots)

**apply**:
- Validates all pattern expansions upfront before any file operations
- Looks up facts at runtime from DB (DB is source of truth)
- If a fact changed since manifest generation, the new value is used
- Staleness validation uses size+mtime+partial_hash (not facts)

**Key design decisions**:
- Lock file does NOT store fact snapshots — simplifies format, avoids "refresh required" friction
- Pattern expansion failures are collected and reported together (not fail-fast)
- All validation happens before any file operations begin

### Scan Pipeline Architecture

The `scan` command uses a pipeline architecture with pure domain logic:

**Pipeline stages** (per file):
1. **Observe**: Capture file metadata from filesystem (`FileObservation`)
2. **Reconcile**: Compare observation to database state, determine action (pure `reconcile()` function)
3. **Persist**: Apply the reconciliation to database (`apply_reconciliation()`)

**Domain types** (`domain/scan.rs`):
- `FileObservation` - What scan sees on disk (root_id, rel_path, device, inode, size, mtime, partial_hash)
- `Reconciliation` - What happened (New, Unchanged, Modified, Moved, Disconnected)
- `reconcile()` - Pure function: given observation + existing state, returns Reconciliation
- `find_missing()` - Pure function: given expected IDs and seen IDs, returns missing IDs

**Reconciliation outcomes**:
| Outcome | Condition | Database Action |
|---------|-----------|-----------------|
| `New` | No source at path, no source by inode | INSERT (or UPDATE stale/replaced record) |
| `Unchanged` | Same path, same size+mtime | UPDATE last_seen_at only |
| `Modified` | Same path, different size/mtime | UPDATE metadata, increment basis_rev |
| `Moved` | Different path, same device+inode | UPDATE path (possibly cross-root) |
| `Disconnected` | Same path, different device | Skip file, don't mark missing |

**Repository functions** (`repo/source.rs`):
- `fetch_by_path()` - Find source at (root_id, rel_path)
- `fetch_by_inode()` - Find source by (device, inode) across all roots
- `apply_reconciliation()` - Translate Reconciliation to SQL
- `mark_missing()` - Set present=0 for source IDs
- `fetch_source_ids_for_root()` - Get IDs for missing detection

**Key behaviors**:
- File replacement (same path, different inode): Old record is updated with new file's attributes
- Stale record revival (file reappears at old path): Stale record is updated, present=1
- Device mismatch: Detected as `Disconnected`, file skipped (use `--ignore-device-id` to override)

### Architectural Direction

The codebase follows a **strict layered architecture** prioritizing reliability, testability, and correct concurrent behavior:

```
┌─────────────────────────────────────────────────────────────┐
│ Command Layer (ls.rs, exclude.rs, scan.rs, etc.)            │
│ - CLI argument parsing and validation                       │
│ - Transaction boundaries (commands own transactions)        │
│ - Orchestration: repo fetch → domain logic → repo write     │
│ - User-facing output formatting                             │
│ - Path canonicalization (ONLY filesystem I/O for paths)     │
└─────────────────────────────────────────────────────────────┘
                          │
          ┌───────────────┴───────────────┐
          ▼                               ▼
┌──────────────────────────────┐   ┌──────────────────────────────┐
│ Repository Layer (repo/)     │   │ Domain Layer (domain/)       │
│ - ALL database access        │   │ - Pure structs and predicates│
│ - Returns domain types       │   │ - Business logic functions   │
│ - Batch operations           │   │ - NO I/O, fully unit-testable│
│ - SQL lives HERE ONLY        │   │ - Path computation/comparison│
│ - NO transaction management  │   │                              │
└──────────────────────────────┘   └──────────────────────────────┘
```

**Layer Responsibilities (STRICT)**:

| Layer | Allowed | NOT Allowed |
|-------|---------|-------------|
| **Domain** | Pure functions, structs, predicates, business logic | Any I/O (database, filesystem, network) |
| **Repo** | Database queries, returning domain types | Business logic, path construction, transaction management |
| **Command** | Orchestration, transactions, CLI parsing, formatting | Inline SQL, business logic that belongs in domain |

**Repo Function Return Type Conventions:**

| Operation Type | Input | Returns | Example |
|---------------|-------|---------|---------|
| **Create** | Domain input type or primitives | Domain object (fetch after insert) | `create()` → `Root` |
| **Get-or-Create** | Lookup key(s) | Domain object | `get_or_create()` → `Object` |
| **Fetch/Read** | IDs or query params | Domain object(s) | `fetch_all()` → `Vec<Root>` |
| **Mutation** | IDs + update values | `Result<()>` | `set_excluded()` → `()` |

*Rationale*: Creation functions return domain objects so the command layer immediately has usable data with all computed/joined fields populated. This follows the `insert_destination()` pattern — no follow-up fetch required.

**Concurrency Considerations**:
Users may run multiple canon processes simultaneously (scanning, enriching, applying, excluding). When designing commands, consider:
- **Transaction scope**: What operations need to be atomic? Per-item, per-batch, or per-command?
- **Idempotency**: Can users re-run after partial failure? This often reduces the need for transactions.
- **Contention**: Larger transaction scopes block concurrent processes longer.

Repo functions do NOT manage transactions — commands establish scope when needed.

**The Standard Pattern** (see `domain/source.rs`, `repo/source.rs`, `ls.rs`):
1. **Domain module**: Struct + pure predicate functions (no I/O, unit-testable)
2. **Repository module**: Batch fetch/write, returns domain types, SQL lives here
3. **Command module**: Orchestrates repo → domain → repo, manages transactions, formats output

```rust
// Command pattern: fetch → filter with domain predicates → transform → output
let sources = repo::source::batch_fetch_by_roots(conn, &root_ids)?;
let filtered: Vec<Source> = sources.into_iter()
    .filter(|s| s.is_active())           // domain predicate
    .filter(|s| s.is_from_role("source")) // domain predicate
    .filter(|s| s.matches_scope(&scopes)) // domain predicate
    .filter(|s| !s.is_excluded())         // domain predicate
    .collect();
```

**Why Strict Separation Matters:**
- Pure domain functions can be thoroughly unit-tested with known inputs/outputs
- Bugs in core logic (path matching, scope resolution) are caught by tests, not users
- New commands automatically benefit from battle-tested domain functions
- Consistent patterns reduce cognitive load and prevent architectural drift
- Future flexibility (different storage backends, cloud support) without rewrites
- Clear transaction boundaries ensure correct concurrent behavior

**Key Invariants** (defined in `domain/source.rs`):
- `is_excluded()` checks BOTH source-level AND object-level exclusion
- `matches_scope()` handles edge case: `/a/bc` is NOT under `/a/b`
- `path()` correctly handles empty `rel_path` (returns just root_path)

**Path Handling Principle: SQL NEVER constructs or compares paths.**
- **Repo layer** returns `Source` objects with `root_path` populated (via JOIN)
- **Domain layer** computes paths using `Source::path()` and compares using `path_is_under()`
- **Command layer** canonicalizes CLI arguments — this is the ONLY place filesystem I/O happens for paths
- See `domain/exclusion.rs` for the reference implementation

**When Adding New Features:**
1. If you need a predicate or business logic → add to domain layer (pure function)
2. If you need database access → add to repo layer (returns domain types)
3. Command modules should ONLY orchestrate, never contain inline SQL or business logic
4. When refactoring existing code, migrate inline SQL to repo layer, logic to domain layer

This separation enables future flexibility (e.g., different storage backends, cloud filesystem support) without requiring rewrites.

### CLI Conventions

- `canon roots` and `canon roots list` must behave identically. When adding flags to `RootsAction::List`, also add them to the top-level `Roots` command so both forms work the same way.

### Type System for Facts (expr/eval.rs)

The fact system uses several key types defined in `expr/eval.rs`:

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
