# Epic: Command Module Architectural Compliance

## Objective

Bring all command modules into compliance with the strict layered architecture defined in CLAUDE.md:

```
┌─────────────────────────────────────────────────────────────┐
│ Command Layer (ls.rs, exclude.rs, scan.rs, etc.)            │
│ - CLI argument parsing and validation                       │
│ - Transaction boundaries (commands own transactions)        │
│ - Orchestration: repo fetch → domain logic → repo write     │
│ - User-facing output formatting                             │
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
└──────────────────────────────┘   └──────────────────────────────┘
```

### Success Criteria

A command module is "compliant" when:
1. **Zero inline SQL** — All database access goes through `repo/` functions
2. **Uses domain predicates** — Filtering uses `source.is_active()`, `source.is_excluded()`, etc.
3. **Clean orchestration** — Command orchestrates: fetch → domain logic → write
4. **Domain types throughout** — Repo functions return domain structs, not raw rows

---

## Stories

### Completed (Prior Work)

| Module | SQL Before | Status | Notes |
|--------|------------|--------|-------|
| **scan.rs** | Heavy | ✅ Done | Exemplary. Uses `domain/scan.rs` for pure reconciliation logic. |
| **apply.rs** | Heavy | ✅ Done | Exemplary. Phases 1-4 extracted SQL to repo, logic to domain. |
| **exclude.rs** | Moderate | ✅ Done | Uses `domain/exclusion.rs` for duplicate detection. |
| **cluster.rs** | Minimal | ✅ Done | 1 trivial query remains (fetch root IDs). |
| **ls.rs** | Minimal | ✅ Done | 1 trivial query remains (fetch root IDs). |
| **worklist.rs** | Minimal | ✅ Done | 1 trivial query remains (fetch root IDs). |
| **compare.rs** | Minimal | ✅ Done | 1 trivial query remains (fetch root IDs). |

### Completed (This Epic)

| Module | SQL Before | Status | Story Spec |
|--------|------------|--------|------------|
| **roots.rs** | 13 | ✅ Done | [2026-02-08-roots-sql-extraction.md](2026-02-08-roots-sql-extraction.md) |

### Pending

| Module | SQL Calls | Priority | Status | Story Spec |
|--------|-----------|----------|--------|------------|
| **coverage.rs** | 7 | Low | 🔲 Pending | — |
| **import_facts.rs** | 12 | High | 🔲 Pending | — |
| **facts.rs** | 46 | High | 🔲 Pending | — |

### Recommended Order

1. **roots.rs** — Quick win, establishes patterns in `repo/root.rs`
2. **coverage.rs** — Minor cleanup, already uses domain predicates well
3. **import_facts.rs** — Fact insertion/object creation patterns needed before facts.rs
4. **facts.rs** — Largest refactoring, benefits from patterns established in earlier stories

---

## Current Story

**None active** — roots.rs completed. Next: coverage.rs or import_facts.rs

---

## Cross-Cutting Insights

*Insights discovered during story work that should inform future stories. Updated after each story completion.*

### Patterns Established (Prior Work)

- **Batch fetch pattern**: `repo::X::batch_fetch_by_Y()` returns `HashMap<id, DomainType>`
- **Domain predicate pattern**: `struct.is_X()` methods on domain types for filtering
- **Scope matching**: Use `ScopeMatch::classify_all()` + domain `matches_scope()`
- **Write pattern**: Repo functions take domain input types, return complete domain objects after insert
- **Reconciliation pattern**: Pure domain function decides action, repo applies it (see `domain/scan.rs`)

### Lessons Learned

**roots.rs story:**
- When commands re-query the database for info they already have (e.g., suspend() querying path after spec resolution), eliminate the query by using the already-fetched Root object
- For archive count statistics, composing existing repo functions (`batch_fetch_by_roots` + `batch_check_archived`) is cleaner than adding specialized one-off queries
- Test helpers in repo layers need to match the full schema (including NOT NULL constraints like `partial_hash`, `scanned_at`)

---

## Module Analysis Detail

### roots.rs (13 SQL calls)

**Current issues:**
- `fetch_file_counts()` — Inline SQL counting files per root
- `remove()` — Inline SQL for deletion with validation
- Various update queries for suspend/unsuspend/comment

**Extraction targets:**
- `repo::root::fetch_file_counts()` — Return counts alongside roots
- `repo::root::remove()` — Handle deletion logic
- Possibly `repo::root::update_suspended()`, `update_comment()`

### coverage.rs (7 SQL calls)

**Current issues:**
- Per-root source filtering query with role filter
- Archive detection queries

**Extraction targets:**
- Already uses `repo::object::batch_check_archived()` — good
- Minor SQL cleanup needed

### import_facts.rs (12 SQL calls)

**Current issues:**
- `build_fact_type_map()` — Queries existing fact types
- `process_import()` — Queries source state for staleness
- `get_or_create_object()` — Object upsert logic
- `promote_content_facts()` — Complex migration SQL

**Extraction targets:**
- `repo::fact::fetch_type_map()`
- `repo::object::get_or_create()` — Return `Object` domain type
- `repo::fact::promote_to_object_facts()`

### facts.rs (46 SQL calls)

**Current issues:**
- `show_builtin_distribution()` — Hardcoded SQL per built-in key type
- `delete_facts()` — Dynamic SQL with key/scope filtering
- `prune_*()` functions — Complex cleanup queries
- Heavy mixing of domain logic with SQL execution

**Extraction targets:**
- `repo::fact::fetch_distribution()` — Grouped value counts
- `repo::fact::delete_by_criteria()` — Scoped deletion
- `repo::fact::prune_stale()`, `prune_orphaned()`
- Possibly new domain types for fact aggregation

---

## Architecture Reference

From CLAUDE.md — the standard command pattern:

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

**Repo function return conventions:**
| Operation | Returns |
|-----------|---------|
| Create | Domain object (fetch after insert) |
| Get-or-Create | Domain object |
| Fetch/Read | Domain object(s) |
| Mutation | `Result<()>` |

---

## Version History

| Date | Change |
|------|--------|
| 2026-02-08 | Epic created. Initial analysis of module compliance. |
| 2026-02-08 | Started roots.rs story. |
| 2026-02-08 | Completed roots.rs story. 13 SQL calls → 0. Added 4 repo functions + 15 tests. |
