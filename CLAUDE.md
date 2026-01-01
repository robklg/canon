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

- **Root**: A scanned folder, identified by canonical path
- **Source**: A file on disk (identified by root + relative path). Device+inode provide physical identity for move detection.
- **Object**: Content identified by hash (sha256). Multiple sources can map to one object.
- **Facts**: Arbitrary key-value metadata (EAV model). Source facts are tied to a file path; content facts are tied to content hash.

### Architecture

- `src/main.rs` - CLI entry point using clap
- `src/db.rs` - SQLite database initialization and schema
- `src/scan.rs` - Directory scanning logic
- `src/worklist.rs` - JSONL worklist generation
- `src/import_facts.rs` - Fact import with staleness validation
- `src/cluster.rs` - Manifest generation with query filters
- `src/apply.rs` - File copying based on manifests

### Database

Default location: `~/.canon/canon.db` (override with `--db` flag)

Key tables: `roots`, `sources`, `objects`, `facts`

### Design Principles

- External tools for hashing/metadata (via JSONL worklist/import)
- Incremental workflow (scan → enrich → cluster → apply)
- Human-editable manifest files (.toml)
- basis_rev tracks file state changes for staleness detection
