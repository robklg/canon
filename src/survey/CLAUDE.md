# survey/ — outward-looking comparison from a shaped selection

Answers three questions about a shaped selection at once: what of it is archived, where else
its content lives, and what exists only here.

`domain/analysis.rs` is the pure lens — scope discovery, only-here and uniqueness counting,
location classification; `domain/object_index.rs` is the in-memory object index, which lives
here rather than in `core/` because survey is its only consumer. `ops/compute.rs` is the
computation and `ops/orchestrate.rs` the fetch and resolution wiring around it. `cli.rs`
validates the CLI shape and orchestrates; `render.rs` prints.

Sealed behind a barrel of `run`, `DetailMode`, `SurveyOptions`, `SurveyExit`, `compute_survey`,
`SurveyParams`, `SurveyOutcome` and `ObjectIndex`. `main.rs` reaches `run`, `DetailMode` and
`SurveyOptions` for `canon survey`, and `SurveyExit` to turn a refused frame into an exit code;
the contentless-law canary (`contentless_law_tests.rs`) reaches the rest. Every other boundary
— `domain`, `ops`, `cli`, `render` — is visible only within `survey/`.

## What survey computes

- **Two modes**: orientation (default — archive status, related locations, unique count; no
  `--where` needed) and affinity (`--affinity`, requires `--where` — adds classification
  columns; `--brief` suppresses).
- **Detail views** (`--detail`): `complement`, `unique`, `overlap`, `residual` (`residual`
  requires `--other`).
- **Asymmetric visibility**: selection side = active source roots, non-excluded (unless
  `--include excluded`), scope + `--where`. Outward side = active roots of any role (source and
  archive), non-excluded always — archives are visible because "what's resolved?" is the
  question being asked.
- **In-memory object index**: `HashMap<i64, Vec<&Source>>` keyed by `object_id`, built from all
  active non-excluded hashed sources; powers overlap, archive status, only-here, uniqueness.
- **"Only here" counts unique objects, not sources** — the one exception to source-based
  counting, because duplicates don't make content more irreplaceable.
- **Classification** (`domain/analysis.rs`, pure `classify_location()`): Superset (≥), Lead (>),
  Subset (⊆, high overlap, no complementary), Mirror (=).
- Subsumes `coverage` for a selection, where `coverage` serves project-level progress instead;
  `compare` is symmetric and `survey` asymmetric. Workflow: explore (`ls`) → assess (`survey`)
  → cluster.

**The asymmetric-visibility designed deviation.** `survey` and `cluster generate` are the two
commands that keep custom selection instead of routing through `expr::select_sources()` —
survey's selection side and outward side apply different visibility rules, which the shared
selector doesn't model. It is a standing exception, not a precedent: a new command still
defaults to `expr::select_sources()` from the start.

## The archive-scope statement

**A scope lying entirely inside archive roots is stated, not surveyed.** Survey reads
source-side selections, so such a scope has no selection at all, and the ordinary empty result
would report an emptiness that is an artifact of the frame rather than a fact about the place.
`run_survey` partitions incoming prefixes by containing-root **role**
(`core::domain::root::partition_prefixes_by_role`) and returns `SurveyOutcome::ArchiveScope`
**before any source fetch**. The invariant is that direction: the statement comes from roles at
resolution, **never from a zero count** — a genuinely empty source-side selection still renders
`Empty` (`the_archive_statement_comes_from_roles_never_from_a_zero_count`).

It names each archive root once and states the frame once for the view, never per root; it
points at `canon trail` and `canon ls`, and never lists archive-side counts, which would answer
the question it just declined. A **mixed** scope narrows to the source side: the scope line
names only what was actually surveyed, the set-asides are stated directly under it, and the
boundary's own set-asides carry across the narrowing untouched (`narrowed_header_scope`) — or a
scope naming an archive place would swallow the sourceless one beside it. `cli::run` returns
`SurveyExit::FrameRefused`, which `main.rs` turns into exit 1 with no `Error:` prefix; where a
machine stream was asked for (`-0` with a machine-rendering detail view) the words go to
**stderr**, leaving stdout empty and the exit code carrying the refusal.

**Role is read regardless of suspension** — a suspended archive root gets the same statement any
archive root gets. Suspension's own law governs what a closed door permits, and this seam does
not consult it. `resolve_scope`'s explicit-path arm re-derives `auto_include_archived` through
the same `partition_prefixes_by_role`, one derivation across the two; its CWD-defaulting arm
reads role from `resolve_root_path` instead, a deliberately different lookup that consults only
*active* roots.

**The outward side reads active roots only.** The `is_active()` filters in `ops/compute.rs`
apply on the outward side as well as the selection side, so a parked archive's copies stop
grounding claims about other places: the same content reads `0 unique` while its root is live
and `3 unique`, `No related locations found.` while it is parked — survey's answer inverted by
an act that changes attention, not truth. It contradicts suspension's testifying clause, and is
recorded rather than quietly narrowed because the same filter is correct on the selection side.

## The contentless law's index site

**`ObjectIndex::build()` (`domain/object_index.rs`) refuses contentless sources** — the law's
enforcement point in the index. A computation reading through the index inherits the refusal;
one reading `all_sources` or `selection` directly past it must re-apply the predicate itself,
and five do (`ops/compute.rs`): selection identity (`sel_object_ids`), the `--other` location
object sets, the per-location totals, the affinity complementary set, and the residual detail
read. Missing it at any of them makes an empty file read as vacuously unique, as shared or
complementary content, or as "not at" a location holding a byte-identical empty file
(`test_residual_never_lists_empty_files`).

**Unhashed and contentless are not symmetric here**: an unhashed source stays residual — nothing
can show it as shared — while a contentless one does not, there being nothing to show.
