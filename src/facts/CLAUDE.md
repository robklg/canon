# facts/ — fact distribution, maintenance, and JSONL import

`canon facts` (distribution and coverage reporting), `canon facts delete`/`prune-*`
(maintenance), and `canon import-facts` (JSONL import). A leaf subsystem: no sibling
reaches in, and it reaches no sibling.

**Strata**: `domain.rs` — the import-vocabulary half of the fact noun (`ContentFactOnSource`,
`is_content_fact`, `import_key`, `delete_key`, `names_the_root`). `repo.rs` — the SQL only facts
performs: the type map, upsert, key counting, the fact-promotion reads and writes,
counting and deleting by criteria, and the stale/excluded prunes, plus the private
`value_type_clause`. `ops/report.rs` — the read-only distribution and enumeration
computations (`compute_all_keys`, `compute_distribution`, `compute_grouped_distribution`,
`compute_root_distribution`). `ops/maintain.rs` — the delete and prune plan/execute
pairs. `ops/import.rs` — `process_record`, per-record import, reading times
through `core::domain::time::reading`, never as epoch seconds; durations through `core::domain::number`. `cli.rs` — the
`canon facts` command surface. `cli/import.rs` — the `canon import-facts` one.

**The fact noun is split, not moved.** `FactEntry`, `FactType` and `FactValue` stay in
`core::domain::fact`, and `batch_fetch_for_sources` stays in `core/repo/fact.rs`, because
`archive` and the expression facility consume the nouns, and `archive` the fetch — only what
facts alone speaks lives here. `ops/report.rs` reads values through `expr::fetch_source_values`,
one per source. The key listing's Type is the key's (`core::repo::fact::key_types`), never the scope's.

**Four of this subsystem's five decision sites arm their recorder; `execute_delete` cannot.**
An armed recorder holds a shared connection for the whole run, and the delete needs a mutable
one — `delete_by_criteria` fills a temp table through `core::repo::db::populate_temp_sources`,
which opens a transaction of its own. Narrowing that helper would trade the compile-time
refusal that keeps those fills out of an outer transaction for a runtime error, tree-wide, so
`execute_delete` settles through its own wrapper instead: one `Err` arm over a private
`delete_body`, which is the shape scan and retirement already hold.
`a_failed_fact_delete_settles_interrupted` is its declaring test, and the architecture test's
census carries the file's two shapes as two rows.

**`import_run` takes its input as `impl BufRead`.** The caller passes `stdin.lock()`; a test
passes a reader that breaks partway, which is the only way to reach the command's own `?` at
all. Both of those arms settle explicitly rather than leaving it to the armed recorder behind
them, because there — and nowhere else in this subsystem — the counts, the touched roots and
the error text are all in hand. The scopes are the load-bearing part: `record_scopes` otherwise
runs only after the loop, so a run that died mid-stream would leave a scope-less row, hidden
from a scoped `trail` view behind its "global decisions not shown" footer — invisible from the
folder being enriched.

**The two commands keep separate cli files.** They share no helper, type or call — "parse,
dispatch, display" is a property every cli file has, not a bond between these two. Their
entry points are `run` and `import_run`, because one barrel cannot carry two `run`s.

**The import takes the closed default per record, not per run.** A record naming a source behind
a door the user closed is set aside and counted (`ImportStats.skipped_suspended`, stated in the
summary), never written and never fatal: refusing the whole batch over one parked root would break
an import that is otherwise entirely legitimate, while writing the facts would be a write inside
the closet. The check stands ahead of the object link and the fact promotion, so the parked root
is not touched at all — and ahead of `touched_roots`, so a set-aside record never puts its root in
the decision's scope.

**Each cli file carries its own drift-baseline entry.** Both call
`core::repo::root::fetch_all`, and the architecture test's baseline keys on the violating
file's own path, so a single entry naming one of them leaves the other unmatched and
fails the build. Repairing one call site deletes that file's entry, never both.

**Barrel** (`facts.rs`), `main.rs`-facing: `run`, `import_run`, `delete_facts`,
`DeleteOptions`, `prune_stale`, `prune_orphaned_objects`, `prune_excluded_facts`,
plus `compute_distribution`, which the source-value law's verifier reads. `cli.rs` declares `pub(super) mod import;` and `ops.rs` declares
`report`/`maintain`/`import` the same way, so nothing below the front door is reachable
from outside it. No repo-tier item is on the barrel: nothing else reaches facts.

**The prunes consult no door.** `prune-stale`, `prune-orphaned-objects` and `prune-excluded-facts`
read no `Door`, `RootLookup` or `ParkedRoot`, by design: pruning is index maintenance over rows
that are already absent or excluded, never an act on content, and a source no longer present is
no longer part of its root whether that root is parked or not. A prune that stopped at a closed
door would keep dead rows alive for a root the user has set aside.

