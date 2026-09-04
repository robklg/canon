# facts/ — fact distribution, maintenance, and JSONL import

`canon facts` (distribution and coverage reporting), `canon facts delete`/`prune-*`
(maintenance), and `canon import-facts` (JSONL import). A leaf subsystem: no sibling
reaches in, and it reaches no sibling.

**Strata**: `domain.rs` — the import-vocabulary half of the fact noun (`FactValueType`,
`SourceFact`, `normalize_fact_key`, `is_content_fact`). `repo.rs` — the SQL only facts
performs: the type map, upsert, key counting, the fact-promotion reads and writes,
counting and deleting by criteria, and the stale/excluded prunes, plus the private
`value_type_clause`. `ops/report.rs` — the read-only distribution and enumeration
computations (`compute_all_keys`, `compute_distribution`, `compute_grouped_distribution`,
`compute_root_distribution`). `ops/maintain.rs` — the delete and prune plan/execute
pairs. `ops/import.rs` — `process_record`, the per-record import logic. `cli.rs` — the
`canon facts` command surface. `cli/import.rs` — the `canon import-facts` one.

**The fact noun is split, not moved.** `FactEntry`, `FactType` and `FactValue` stay in
`core::domain::fact`, and `batch_fetch_for_sources`/`batch_fetch_key_for_sources` stay in
`core/repo/fact.rs`, because `archive` and `expr::domain::value` consume them too — only
what facts alone speaks lives here. `ops/report.rs` calls `batch_fetch_key_for_sources`
in core directly: an operations stratum reaching the shared repository is the ordinary
edge, not a barrel item.

**The two commands keep separate cli files.** They share no helper, type or call — "parse,
dispatch, display" is a property every cli file has, not a bond between these two. Their
entry points are `run` and `import_run`, because one barrel cannot carry two `run`s.

**Each cli file carries its own drift-baseline entry.** Both call
`core::repo::root::fetch_all`, and the architecture test's baseline keys on the violating
file's own path, so a single entry naming one of them leaves the other unmatched and
fails the build. Repairing one call site deletes that file's entry, never both.

**Barrel** (`facts.rs`), all `main.rs`-facing: `run`, `import_run`, `delete_facts`,
`DeleteOptions`, `prune_stale`, `prune_orphaned_objects`, `prune_excluded_facts`,
`show_aliases`. `cli.rs` declares `pub(super) mod import;` and `ops.rs` declares
`report`/`maintain`/`import` the same way, so nothing below the front door is reachable
from outside it. No repo-tier item is on the barrel: nothing outside `main.rs` reaches
facts at all.
