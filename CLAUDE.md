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

The codebase is organized into four namespaces (domain/, repo/, ops/, expr/) plus command modules:

**Domain Layer** (`src/domain/`) - Pure concepts, no I/O:
- `source.rs` - Source struct and predicates (`is_excluded()`, `matches_scope()`, etc.)
- `root.rs` - Root struct, RootSpec enum, predicates (`is_suspended()`, `is_source()`)
- `object.rs` - Object struct and `is_excluded()` predicate
- `fact.rs` - FactEntry struct, re-exports FactValue/FactType
- `scope.rs` - ScopeMatch enum for file vs directory scope matching
- `path.rs` - Pure path utilities (`path_is_under()`, `path_strip_prefix()`, `format_path()`)
- `scan.rs` - Scan reconciliation logic (`FileObservation`, `Reconciliation`, `reconcile()`, `find_missing()`)
- `exclusion.rs` - Duplicate exclusion logic (`find_excludable_duplicates()`)
- `include.rs` - `IncludeSet` struct for controlling source visibility (`includes_excluded()`, `includes_archived()`, `is_expanded()`)
- `survey.rs` - Scope discovery (`discover_scopes()`, `discover_scopes_by_root()`), uniqueness (`count_only_here()`, `find_unique_object_ids()`), `LocationKind` classification (Superset, Lead, Subset, Mirror)

**Repository Layer** (`src/repo/`) - Database access:
- `db.rs` - Connection, schema, transactions (`Db`, `open_with_options()`)
- `source.rs` - Source batch fetching and writes (`batch_fetch_by_roots()`, `fetch_sources_by_object_ids()`, `insert_destination()`, `apply_reconciliation()`)
- `root.rs` - Root batch fetching (`fetch_all()`, `batch_fetch_by_ids()`)
- `object.rs` - Object batch fetching, archive detection (`batch_check_archived()`, `batch_find_archive_info_by_hash()`)
- `fact.rs` - Fact batch fetching (`batch_fetch_for_sources()`, `batch_fetch_key_for_sources()`)

**Expression System** (`src/expr/`) - Pattern and filter handling:
- `alias.rs` - Pure alias expansion logic (`expand_aliases()`, `has_alias_references()`)
- `eval.rs` - Pattern evaluation, modifiers, accessors, FactValue types
- `filter.rs` - Filter expression parsing for `--where` clauses
- `value.rs` - Fact value resolution for sources

**Operations Layer** (`src/ops/`) - Composed behaviors, interface-independent:
- `selection.rs` - Source selection: `select_sources()`, `RolePolicy`, `SelectionParams`, `Selection`

**Command Modules** (flat in `src/`):
- `main.rs` - CLI entry point using clap (canon home resolution, alias expansion dispatch)
- `alias.rs` - Alias file I/O and filter expansion orchestration (`expand_filter_strings()`)
- `ceremony.rs` - Shared confirmation infrastructure (`confirm()`, `format_count()`)
- `ls.rs` - List and query sources
- `coverage.rs` - Archive coverage statistics
- `cluster.rs` - Manifest generation with query filters, summary/notes comment sections
- `apply.rs` - File copying/moving based on manifests
- `exclude.rs` - Source exclusion management
- `facts.rs` - Fact inspection and management
- `roots.rs` - Root management (list, suspend/unsuspend, comment, remove)
- `compare.rs` - Compare folders by content hash
- `survey.rs` - Survey scope for archive status, related locations, unique content
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
- `survey` - Survey a selection for archive status, related locations, and unique content (orientation/affinity modes, detail views)
- `cluster generate` - Generate manifest from matching sources
- `apply` - Apply manifest to copy/move/rename files
- `exclude set/clear/duplicates` - Manage source exclusions

### CLI Flag Vocabulary

Two unified flags control visibility and awareness across all commands:

- **`--include`** (query commands: `ls`, `facts`, `coverage`, `worklist`, `compare`, `survey`): Expands what you see. Values: `excluded`, `archived`, `all`. Comma-separated and repeatable. Always safe — no side effects. Compare and survey only accept `excluded`.
- **`--allow`** (effectful commands: `cluster generate`, `apply`, `import-facts`): Acknowledges non-default source selection. Canon's defaults surface information (e.g., duplicates present); `--allow` is the user saying "I'm aware, proceed." Per-command values. Not available on `cluster refresh` (reads from manifest `[options]`).

**Filter modes on `ls`**: `--archived`, `--unarchived`, `--unhashed`, `--duplicates`, `--excluded` are mutually exclusive filter modes. `--excluded` implicitly includes excluded sources and shows both source-level and object-level excluded.

**Status column in `ls -l`**: When `--include` is used or `--excluded` mode is active, long format shows a status indicator: `E` (source-excluded), `X` (object-excluded), `A` (archive source), or blank.

### Canon Home Directory

All Canon state lives under a single "canon home" directory:
- Default: `~/.canon/`
- Override with `CANON_HOME` env var or `--canon-home` flag
- Precedence: `--canon-home` flag > `CANON_HOME` env var > `~/.canon/`
- Contains: `canon.db` (database), `aliases.toml` (expression aliases)

### Expression Aliases

Named aliases for `--where` filter expressions, defined in `$CANON_HOME/aliases.toml`:

```toml
image = "content.mime IN ('image/jpeg', 'image/png', 'image/gif')"
tens = "source.mtime|year >= 2010 AND source.mtime|year < 2020"
```

Usage: `canon ls --where "@image AND @tens"`. The `@name` syntax expands before filter parsing — the filter engine never sees `@`. Expansion wraps each alias value in parentheses unconditionally. `@` inside quoted strings is treated as literal.

Alias expansion happens in `main.rs` before command dispatch. The pure expansion logic lives in `expr/alias.rs`; file I/O lives in `alias.rs`.

### Database

Default location: `$CANON_HOME/canon.db`

Key tables: `roots`, `sources`, `objects`, `facts`

Roots table columns include `suspended` (integer, default 0) for temporarily hiding roots from operations, `comment` for user notes, and `last_scanned_at` timestamp.

**SQL Batching Requirement:** Any SQL with `WHERE ... IN (...)` clauses MUST handle large ID lists. SQLite has a variable limit (~999-32K depending on version). Use one of these patterns:
- **Chunking:** `for chunk in ids.chunks(BATCH_SIZE)` with `BATCH_SIZE = 1000` (see `repo/source.rs`)
- **Temp table:** `repo::db::populate_temp_sources()` then JOIN (see `repo/fact.rs`)

**Test Database Setup:** Always use `repo::open_in_memory_for_test()` for test databases. This creates an in-memory SQLite database with the production schema, ensuring tests run against the real schema with all constraints, indexes, and CHECK clauses. **Do NOT define custom test schemas** — they drift from production and mask bugs. Test insert helpers must provide all NOT NULL columns that lack defaults (notably `size`, `mtime`, `partial_hash`, `scanned_at`, `last_seen_at`, `device`, `inode` for sources).

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
- `clean_path()` - Pure lexical path cleaning: make absolute, resolve `.`/`..` without filesystem access
- `resolve_path()`, `resolve_paths()` - Soft path resolution: match against known roots (works offline), fall back to `fs::canonicalize`. Use for source-querying commands.
- `canonicalize_maybe_missing()` - Canonicalize a path where the leaf may not exist yet (walks up to find existing ancestor). Used by `resolve_archive_path`.

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

**Manifest format**:
- `ManifestMeta` includes `version: u32` — current version is 1. Old manifests without `version` deserialize as 1 via `serde(default)`. `validate_manifest_version()` rejects future versions early in `apply::run()` and `cluster::refresh()`.
- `ManifestOptions` struct with `allow: Vec<String>` — stores `--allow` values (e.g., `["archived", "duplicates"]`)
- Always written to manifest, even when empty
- `cluster refresh` reads options from the manifest — no `--allow` flag on refresh
- `--show-archived` is CLI-only (not stored — it's output verbosity, not semantics)
- Old manifests without `[options]` work via `#[serde(default)]`

**Manifest comment sections**:
- `# === Cluster Summary ===` — generated on `cluster generate` and regenerated on `cluster refresh`. Shows source count, root breakdown, archive coverage, and skipped counts.
- `# === Notes ===` — empty placeholder on generate. Preserved verbatim on refresh (extracted via string matching since TOML parsers strip comments). Users can add free-form notes here.
- `extract_notes()` finds notes content between `# === Notes ===` and the next `# === ` or `[` section header.

**Ceremony infrastructure** (`ceremony.rs`):
- `confirm(yes: bool)` — shared confirmation prompt ("Proceed? [y/N]"). Returns `Ok(false)` on decline (not an error). Used by `roots rm`, `apply`, `exclude set/clear/duplicates`.
- `format_count(n)` — formats numbers with thousands separators (e.g., 3847 → "3,847"). Used in manifest summary comments and stdout summaries.
- Confirmation content is gated behind `!yes` — when `--yes` is passed, both content and expensive queries are skipped.

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

### Survey Architecture

The `survey` command provides outward-looking comparison from a shaped selection. It answers: what's archived, where are related locations, what's unique to this scope.

**Two cognitive modes**:
- **Orientation** (default): What's here? Archive status, related locations sorted by overlap, unique count. No `--where` or `--affinity` required.
- **Affinity** (`--affinity`, requires `--where`): Adds classification columns (+N more, unique count, disposition symbol) per location. Sorted by classification. `--brief` suppresses affinity computation.

**Detail views** (`--detail`): Replace the summary with specific listings:
- `complement` — files at related locations matching filters but with different content (implies affinity)
- `unique` — bare paths of content existing nowhere else
- `overlap` — selection-side paths whose content exists at a location
- `residual` — selection sources NOT shared with a location (requires `--other`); unhashed sources always residual

**Asymmetric visibility model**: Survey has two sides with different rules:
- **Selection side** (the user's query): Active source roots only, non-excluded (unless `--include excluded`), filtered by scope + `--where`
- **Outward side** (the universe): Active roots of any role (source + archive), non-excluded always. Archive roots are visible because "what's resolved?" is a core question

**In-memory object index**: All computations use a `HashMap<i64, Vec<&Source>>` keyed by `object_id`, built from all active non-excluded hashed sources. This single data structure powers overlap, archive status, "only here" checks, and uniqueness.

**"Only here" counts unique objects, not sources**: Exception to the general source-based counting convention. Duplicates within a location don't make content more irreplaceable. A location with 3 copies of the same file has 1 "only here" object.

**Location classification** (`domain/survey.rs`): Five dispositions — Superset (≥), Lead (>), Subset (⊆), Mirror (=). Subset: high overlap (≥80% of location's content), no complementary. Classification is a pure domain function `classify_location()`.

**Scope discovery** (`domain/survey.rs`): Pure domain function that finds actionable directory paths where overlapping content concentrates, rather than reporting root-level paths. Uses a tree-based collapsing algorithm on relative paths.

**Relationship to other commands**: `survey` subsumes `coverage` for a selection but `coverage` serves project-level progress. `survey` is asymmetric (selection vs universe); `compare` is symmetric (folder A vs folder B). The workflow is: explore with `ls` → assess with `survey` → cluster when ready.

### Architectural Direction

The codebase follows a **strict layered architecture** prioritizing reliability, testability, and correct concurrent behavior:

```
┌─────────────────────────────────────────────────────────────┐
│ Interface Layer (src/*.rs — CLI commands)                    │
│ - CLI argument parsing (clap structs)                       │
│ - Output formatting (terminal, JSONL, null-delimited)       │
│ - Ceremony presentation (display plan, prompt, report)      │
│ - The ONLY layer that knows about stdout/stderr/stdin       │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│ Operations Layer (src/ops/)                                  │
│ - Typed operation functions with typed results               │
│ - Shared sub-operations (select_sources, etc.)              │
│ - Ceremony policy: what to show, when to confirm            │
│ - Transaction boundaries for write operations               │
│ - Interface-independent — no stdout, stderr, stdin          │
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
| **Interface** | CLI parsing, output formatting, ceremony presentation, terminal I/O | Business logic, source selection logic, ceremony policy, direct repo calls (use ops/) |
| **Operations** | Composing domain + repo into typed behaviors, ceremony policy, transactions (writes) | stdout/stderr/stdin, CLI argument types, display formatting |
| **Repo** | Database queries, returning domain types, batch operations | Business logic, transaction management, path construction |
| **Domain** | Pure functions, structs, predicates, business logic | Any I/O (database, filesystem, network) |

**Note**: The operations layer is being introduced incrementally. During migration, some commands still contain inline behavioral logic that will be extracted to `ops/`. New commands should use `ops/` from the start.

**Repo Function Return Type Conventions:**

| Operation Type | Input | Returns | Example |
|---------------|-------|---------|---------|
| **Create** | Domain input type or primitives | Domain object (fetch after insert) | `create()` → `Root` |
| **Get-or-Create** | Lookup key(s) | Domain object | `get_or_create()` → `Object` |
| **Fetch/Read** | IDs or query params | Domain object(s) | `fetch_all()` → `Vec<Root>` |
| **Mutation** | IDs + update values | `Result<()>` | `set_excluded()` → `()` |

*Rationale*: Creation functions return domain objects so the command layer immediately has usable data with all computed/joined fields populated. This follows the `insert_destination()` pattern — no follow-up fetch required.

**Operations Layer Conventions** (`src/ops/`):

Operations are typed, interface-independent functions that compose domain predicates and repo functions into Canon's behavioral contracts.

- **Read operations** take `&mut Connection` (needed for filter temp tables). No transaction management.
- **Write operations** take `&mut Db` and own their transactions.
- **Result types** are concrete structs per operation — no generic containers or trait hierarchies.
- **No stdout/stderr/stdin** — operations return data, the interface formats it.

The reference implementation is `ops::selection::select_sources()`:
```rust
let selection = ops::selection::select_sources(conn, &params)?;
// selection.sources — the filtered sources
// selection.source_ids() — convenience for ID-based consumers
// selection.excluded_count — for "N excluded hidden" hints
```

**Concurrency Considerations**:
Users may run multiple canon processes simultaneously (scanning, enriching, applying, excluding). When designing operations, consider:
- **Transaction scope**: What operations need to be atomic? Per-item, per-batch, or per-command?
- **Idempotency**: Can users re-run after partial failure? This often reduces the need for transactions.
- **Contention**: Larger transaction scopes block concurrent processes longer.

Repo functions do NOT manage transactions — operations or commands establish scope when needed.

**The Standard Pattern** (see `domain/source.rs`, `repo/source.rs`, `ops/selection.rs`):
1. **Domain module**: Struct + pure predicate functions (no I/O, unit-testable)
2. **Repository module**: Batch fetch/write, returns domain types, SQL lives here
3. **Operations module**: Composes repo fetch → domain predicates → typed result
4. **Interface module**: Parses CLI arguments, calls operations, formats output

```rust
// Legacy command pattern (being migrated to ops layer):
let sources = repo::source::batch_fetch_by_roots(conn, &root_ids)?;
let filtered: Vec<Source> = sources.into_iter()
    .filter(|s| s.is_active())           // domain predicate
    .filter(|s| s.is_from_role("source")) // domain predicate
    .filter(|s| s.matches_scope(&scopes)) // domain predicate
    .filter(|s| !s.is_excluded())         // domain predicate
    .collect();

// New pattern via operations layer:
let selection = ops::selection::select_sources(conn, &params)?;
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
- **Interface layer** resolves CLI path arguments — two strategies:
  - **Source-querying commands** (ls, facts, coverage, worklist, compare, exclude, roots, cluster generate): Use `resolve_paths()` — soft resolution that matches against known roots in the DB (works offline), falling back to `fs::canonicalize` only when no root matches.
  - **File-accessing commands** (scan): Use `fs::canonicalize` directly — hard resolution that requires the path to exist on disk.
- See `domain/exclusion.rs` for the reference implementation

**When Adding New Features:**
1. If you need a predicate or business logic → add to domain layer (pure function)
2. If you need database access → add to repo layer (returns domain types)
3. If you need composed behavior (selection, computation, ceremony policy) → add to ops layer
4. Interface modules should ONLY parse arguments, call operations, and format output
5. When refactoring existing commands, extract behavioral logic to ops layer

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
