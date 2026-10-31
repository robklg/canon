# facts/ — fact distribution, maintenance, and JSONL import

`canon facts` (distribution and coverage reporting), `canon facts delete`/`prune-*`
(maintenance), `canon facts convert` (a key's type conversion), and `canon import-facts` (JSONL
import). A leaf subsystem: no sibling
reaches in, and it reaches no sibling.

**Strata**: `domain.rs` — the import-vocabulary half of the fact noun (`ContentFactOnSource`,
`is_content_fact`, `import_key`, `delete_key` — which, like the conversion's `read_convert_name`, reads
a name as typed (`expr::NameReading`), where import reads it as written — and `names_the_root`), and what arrives and what
becomes of it: `JsonFact` (a value as the JSON its tool wrote, a number in its own spelling),
and `decide`, which fits one value to its key's type as a `Disposition` — stored, or skipped
for a `SkipCause`. Text is read through `core::domain::conversion`'s table, times through
`core::domain::time::reading`, never as epoch seconds. `repo.rs` — the SQL only facts
performs: upsert, key counting, the fact-promotion reads and writes,
counting and deleting by criteria, the stale/excluded prunes, and the conversion's writes by
fact id, plus the private `value_type_clause`. `domain/convert.rs`, `ops/convert.rs` and
`cli/convert.rs` — `canon facts convert`, in the same three strata. `ops/report.rs` — the read-only distribution and enumeration
computations (`compute_all_keys`, `compute_distribution`, `compute_grouped_distribution`,
`compute_root_distribution`). `ops/maintain.rs` — the delete and prune plan/execute
pairs. `ops/import.rs` — `import_batch`, one batch of records, and the summary's
composition. `cli.rs` — the `canon facts` command surface. `cli/import.rs` — the
`canon import-facts` one: the reader thread, the batch loop, and the way-out lines.

**The fact noun is split, not moved.** `FactEntry`, `FactType` and `FactValue` stay in
`core::domain::fact`, and `batch_fetch_for_sources` stays in `core/repo/fact.rs`, because
`archive` and the expression facility consume the nouns, and `archive` the fetch — only what
facts alone speaks lives here. `ops/report.rs` reads values through `expr::fetch_source_values`,
one per source. The key listing's Type is the key's (`core::repo::fact::key_types`), never the scope's.

**The conversion writes exactly the key's row set.** It reads every fact of the key through
`core::repo::fact::key_census`, the row set `key_types` reads, and a source's value through
`expr::source_value`. The act takes the write lock (`IMMEDIATE`) first and plans and checks
`--allow` again inside it, so a count that changed since the preview refuses before anything is
written. Its recorder starts on the transaction, so a refusal or a failure rolls the decision row
back with the writes, as `exclude`'s does. `the_conversion_leaves_one_type_wherever_the_facts_sit`
pins the write set.

**Four of this subsystem's six decision sites arm their recorder, the conversion's rolls back
with its transaction, and `execute_delete` can do neither.**
An armed recorder holds a shared connection for the whole run, and the delete needs a mutable
one — `delete_by_criteria` fills a temp table through `core::repo::db::populate_temp_sources`,
which opens a transaction of its own. Narrowing that helper would trade the compile-time
refusal that keeps those fills out of an outer transaction for a runtime error, tree-wide, so
`execute_delete` settles through its own wrapper instead: one `Err` arm over a private
`delete_body`, which is the shape scan and retirement already hold.
`a_failed_fact_delete_settles_interrupted` is its declaring test, and the architecture test's
census carries the file's two shapes as two rows.

**A batch is one write transaction.** `import_batch` takes the write lock (`IMMEDIATE`) before it
reads the batch's key types, so another writer changing a key's type cannot interleave with the
decision, and each record runs in a savepoint: a record that fails is undone whole, its facts,
counts and any first type it gave a key. The batch loop closes a batch at `BATCH_RECORDS` or
`BATCH_WINDOW` after its first record; the input is read on a detached thread so a batch never
holds the lock waiting on a slow pipeline. `an_import_and_a_type_change_never_interleave_on_one_key`
pins the lock.

**The way-out lines live in the interface; the summary does not.** The per-key lines are for the
screen alone, so `cli/import.rs` words them, one exhaustive `match` over `SkipCause`. The summary
is also the decision record's, so `ops/import.rs` composes it.

**`import_run` takes its input as `impl BufRead + Send`, and its stderr as a writer.** The caller
passes stdin and stderr; a test passes a reader that breaks partway or is held open, and a buffer
it reads. A run that stops early, on a broken input or a batch that cannot be written, settles
explicitly rather than leaving it to the armed recorder behind it, because there — and nowhere else in this subsystem — the counts, the touched roots and
the error text are all in hand. The scopes are the load-bearing part: `record_scopes` otherwise
runs only after the loop, so a run that died mid-stream would leave a scope-less row, hidden
from a scoped `trail` view behind its "global decisions not shown" footer — invisible from the
folder being enriched.

**The two commands keep separate cli files.** They share no helper, type or call — "parse,
dispatch, display" is a property every cli file has, not a bond between these two. Their
entry points are `run` and `import_run`, because one barrel cannot carry two `run`s.

**The import takes the closed default per record, not per run.** A record naming a source behind
a door the user closed is set aside and counted (`RecordSkip::Suspended`, stated in the
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
`DeleteOptions`, `prune_stale`, `prune_orphaned_objects`, `prune_excluded_facts`, `convert`,
`ConvertOptions`, `Target` and `ConvertRefused`,
plus `compute_distribution`, which the source-value law's verifier reads, and `plan_convert`,
which the contentless-law canary reads. `cli.rs` declares `pub(super) mod import;` and
`convert` the same way, and `ops.rs` declares `report`/`maintain`/`import`/`convert` the same way, so nothing below the front door is reachable
from outside it. No repo-tier item is on the barrel: nothing else reaches facts.

**The prunes consult no door.** `prune-stale`, `prune-orphaned-objects` and `prune-excluded-facts`
read no `Door`, `RootLookup` or `ParkedRoot`, by design: pruning is index maintenance over rows
that are already absent or excluded, never an act on content, and a source no longer present is
no longer part of its root whether that root is parked or not. A prune that stopped at a closed
door would keep dead rows alive for a root the user has set aside.

