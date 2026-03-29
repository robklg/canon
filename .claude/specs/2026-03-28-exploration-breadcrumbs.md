# Story: Exploration Breadcrumbs (`canon note`)

**Design Spec**: `~/store/claude-designs/2026-03-28-exploration-breadcrumbs.md`
**Status**: Complete
**Created**: 2026-03-28

## Objective

Canon's first location-level annotation system — the first foothold into the assembly gap's persistence layers (vision layers 2-4). During orientation, users hop between locations, spot threads worth pursuing later, and move on. Those threads exist only in memory and are lost by the next session. `canon note` lets users quickly jot down a thought about a location and have it surface automatically when that location becomes relevant again — in survey output, and as a breadcrumb map for reviewing open threads.

This is the pre-decision layer of a broader decision provenance model: a note is the *first* decision about a location ("there's more here"); later decisions (archive, exclude, release) continue the story. The design should not close doors on that future.

## Functional Requirements Summary

Six user stories (Story 6 is deferred):

1. **Add** — `canon note -m "text"` with CWD defaulting or explicit path. Timestamped, append-only. Fast insert, stderr confirmation.
2. **View** — `canon note` shows exact-scope notes + spatial indicators (ancestor note count, descendant noted-location count). Pretty-printed.
3. **List** — `canon note --global` and `canon note -r` — pretty-printed grouped by location, chronological within each.
4. **Clear** — `canon note --clear` (exact scope, no prompt) and `--clear -r` (recursive, with confirmation prompt, `--yes` to skip).
5. **Survey surfacing** — Notes from scope + descendants in survey header, recency-capped at 5 (most recent first), each labeled with relative path (`.` for scope itself). `--verbose` expands to show all. Ancestral-only summary when no subtree notes exist. Related location note indicators `(N notes)`.
6. **Deferred** — Notes in `facts`/`coverage`, note counts in `canon roots`.

**Key design decisions**:
- Notes attach to `(root_id, rel_path)` — root-relative, same coordinate system as sources
- Journal model: entries accumulate, never replaced. Clearing is explicit.
- View mode looks at exact scope only, with counts as spatial indicators in both directions
- Survey treats scope as subtree (notes from scope + descendants), consistent with how survey treats everything else
- `-r` / `--recursive` for descendant listing (unix vocabulary), `--global` for all roots (Canon vocabulary)
- Pretty-printed grouped output for all modes — list modes group by location with headers, naturally greppable
- Scope resolution uses `ops::scope::resolve_scope()` — the shared CWD defaulting infrastructure, not custom reimplementation

## What Was Built

### Phase 1: Foundation — Schema, Domain, Repo

**Schema** (`repo/db.rs`):
```sql
CREATE TABLE IF NOT EXISTS notes (
    id INTEGER PRIMARY KEY,
    root_id INTEGER NOT NULL REFERENCES roots(id),
    rel_path TEXT NOT NULL DEFAULT '',
    text TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS notes_root_path ON notes(root_id, rel_path);
```

**Domain** (`domain/note.rs`): `Note` struct, `ancestor_paths()`, `relative_to_scope()` — 9 pure function tests.

**Repo** (`repo/note.rs`): Full CRUD — `insert`, `fetch_by_scope` (chronological ASC), `fetch_subtree` (recency DESC for survey), `fetch_subtree_chronological` (path + time ASC for listing), `fetch_all`, counts (`count_ancestor_notes`, `count_descendant_locations`, `count_subtree_notes`, `count_subtree_locations`), mutations (`clear_by_scope`, `clear_subtree`, `delete_by_root`), batch (`batch_count_subtree`) — 23 tests.

### Phase 2: Ops Layer + Note Command

**Ops** (`ops/note.rs`): `NoteScope`, `resolve_note_scope()`, `view_notes()`, `list_notes_global()`, `list_notes_recursive()` (uses chronological ordering), `plan_clear_recursive()` / `execute_clear_recursive()` (plan/execute pattern), `survey_note_context()` — 6 tests.

**Interface** (`note.rs`): Mode dispatch via `run()`. Uses `ops::scope::resolve_scope()` for CWD defaulting — wrapped in `resolve_single_scope()` and `resolve_single_scope_optional()` that validate single-path constraint and call `warn_nonexistent_scope_paths()`. Pretty-printed grouped output for all listing modes via `print_list()`. View mode uses `print_view()` with spatial indicators. Clear recursive uses `plan.scope.display()` for display.

**CLI** (`main.rs`): `Note` variant in `Commands` enum with `-m`, `-r`/`--recursive`, `--global`, `--clear`, `--yes`.

**Root removal** (`roots.rs`): `repo::note::delete_by_root()` called before root removal.

### Phase 3: Survey Integration

**Survey** (`survey.rs`): `print_notes_section()` renders notes between header and stats. Fetches `SurveyNoteContext` before `compute_survey()` (independent of survey computation). Related location note indicators via `batch_count_subtree()` in the no-detail summary mode.

### Phase 4: Documentation

- `docs/src/commands/manage/note.md` — full command reference
- `docs/src/SUMMARY.md` — note page added
- `docs/src/commands/query/survey.md` — notes surfacing documented
- `docs/src/commands/manage/index.md` — note mentioned alongside exclude
- `CLAUDE.md` — all new modules documented

## Architectural Decisions

| Decision | Rationale |
|----------|-----------|
| New `notes` table (not reusing `facts` or `roots.comment`) | Notes are a distinct entity: timestamped, accumulating, on arbitrary scopes. Facts are content metadata. Root comments are static identifiers. Different lifecycle, different query patterns. |
| `(root_id, rel_path)` as scope identifier | Consistent with how Canon identifies all locations. Root-relative, resilient to mount point changes. |
| Manual root deletion cleanup (not FK cascade) | Canon doesn't enable `PRAGMA foreign_keys`. Manual cleanup in `roots rm` follows existing pattern for source deletion. |
| Uses `ops::scope::resolve_scope()` for CWD defaulting | The CWD defaulting logic is shared infrastructure — reimplementing it creates drift risk. `auto_include_archived` is unused but harmless. |
| Two subtree fetch orderings | `fetch_subtree` (DESC) for survey recency-capped display. `fetch_subtree_chronological` (ASC) for listing modes where oldest-first is natural reading order. |
| Pretty-printed grouped output for all modes | Tab-separated was initially specified but proved ugly and hard to read. Grouped format with location headers is readable and greps naturally. |
| Survey note cap at 5, `--verbose` expands | Notes deserve real estate in survey (they're context for orientation). 5 is generous enough to be useful, bounded enough to not drown the report. |
| Related location note indicators only in summary mode | Detail modes show specific content, not orientation context. Note indicators serve orientation. |

## Corrections Applied During Acceptance Review

1. **Removed custom scope resolution** — initial implementation reimplemented CWD defaulting logic in `note.rs` instead of using `ops::scope::resolve_scope()`. Corrected to use shared infrastructure.
2. **Removed `validate_path_exists`** — band-aid function that errored on non-existent explicit paths. Replaced with `warn_nonexistent_scope_paths()` consistent with other scope-taking commands.
3. **Fixed listing output format** — changed from tab-separated flat format to pretty-printed grouped format matching view mode.
4. **Fixed listing sort order** — added `fetch_subtree_chronological` for listing modes (path + time ASC). Was incorrectly using survey's recency-DESC ordering.
5. **Used `plan.scope.display()`** — clear recursive flow was computing display string separately from the plan. Corrected to use the plan's scope.

## Non-Goals

- Notes in `facts` or `coverage` output — deferred, expand based on real use
- Note counts in `canon roots` listing — deferred
- Resolved/archived state for notes — deferred to decision provenance design
- Text search on notes — `grep` on grouped output is sufficient
- Recency filtering (`--since`) — deferred
- `--reason` annotations on effectful commands — decision provenance scope, not breadcrumbs

## Test Summary

- 38 new tests (9 domain + 23 repo + 6 ops)
- All 894 tests pass
- No clippy warnings from new code
