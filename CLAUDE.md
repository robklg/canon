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
- `src/db.rs` - SQLite database, schema, and shared path/scope utilities
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
- `src/expr.rs` - Pattern expression evaluation for manifest output; defines Modifier enum, FactValue types, and modifier application logic. Supports alias expansion and Python-style path accessors.
- `src/filter.rs` - Filter expression parsing; depends on expr.rs for modifier handling. Has hardcoded built-ins for derived facts. New derived facts require code changes here.

### Commands

- `scan` - Index directories, add files to database
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

In `db.rs`:
- `canonicalize_scopes()`, `build_scope_clause()` - Path scoping for queries
- `path_is_under()`, `path_strip_prefix()` - Path manipulation
- `parse_root_spec()` - Parse `id:N` or `path:/foo` format
- `resolve_root_path()`, `resolve_archive_path()` - Find roots containing paths
- `populate_temp_sources()` - Batch insert pattern for large ID sets

In other modules:
- `filter::apply_filters()` - Apply filter expressions to source IDs
- `exclude::exclude_clause()` - SQL clause for exclusion filtering

### Design Principles

- External tools for hashing/metadata (via JSONL worklist/import)
- Incremental workflow (scan -> enrich -> cluster -> apply)
- Human-editable manifest files (.toml)
- basis_rev tracks file state changes for staleness detection
