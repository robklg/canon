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
  `--include excluded`), scope + `--where`. Outward side = roots of any role (source and
  archive) **whatever their standing**, non-excluded always — archives are visible because
  "what's resolved?" is the question being asked.
- **In-memory object index**: `HashMap<i64, Vec<&Source>>` keyed by `object_id`, built from all
  non-excluded hashed sources; powers overlap, archive status, only-here, uniqueness.
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
archive root gets. What a closed door permits is suspension's own question, and this seam does
not consult it. `resolve_scope`'s explicit-path arm re-derives `auto_include_archived` through
the same `partition_prefixes_by_role`, one derivation across the two; its CWD-defaulting arm
reads role from the containing root it resolved, which now finds parked roots too.

**The door precedes the frame.** A scope wholly behind a closed door is answered by the spine's
statement in `main.rs`, before survey runs — the sentence is universal, while `ArchiveScope`'s
is survey's own and points at `trail`/`ls`. `machine_shaped_stdout` is survey's one derivation of
which channel its scope statement belongs on, asked by the front door rather than re-derived
there.

## The outward side, and the door

**The outward side reads every root, whatever the standing of its door.** `is_active()` survives
on the **selection** side alone (`ops/compute.rs`, the selection and its hidden-excluded count):
what you survey is what you can see. The four outward reads — the object index, the `--other`
location object set, the per-location totals, the affinity complementary set — carry no such
filter, because they answer *where else does this content live?* and a copy behind a door the
user closed is still a copy. Testifying is the one thing a closed door does not stop; custody
stands, and parking never un-archives.

The filter used to apply on both sides, and inverted survey's own answer: the same content read
`0 unique` while a neighbouring root was live and `3 unique`, `No related locations found.` once
it was parked — the world's answer changed by an act that changes only attention. Pinned from
both sides (`survey_counts_parked_evidence_on_the_outward_side`,
`the_selection_side_still_sets_parked_sources_aside`).

**Every cited place says whether reaching it means opening a door**, all-or-nothing:
`LocationResult.suspended` is true when every source *in the population survey already fetched for
the place* (`at_location` — non-excluded, non-contentless, hashed, role-appropriate for the mode in
force; not literally every source Canon knows there) stands on a parked root. One copy on an open
root and nothing needs opening, so a mark would raise a question the place does not pose. Read
**from what stands at the place**, not from a discovered scope's root, which is what lets a
`--other` location answer the same way a discovered one does. A discovered location lies in one
root, so the *every* and *any* readings coincide there; a `--other` location is a path the user
chose and can contain several roots, which is where the two readings part and the rule earns its
keep (`an_other_location_spanning_two_standings_is_unmarked`).
`render::door_marker` is the one spelling **for cited locations** — the summary's two lists and
every `--detail` view, so a place cannot be marked in one and bare in the other; the frame label
(`ops/orchestrate.rs`) composes its own, being a label rather than a location. Testimony either
way, so no verb and no way back.

**Standing is the ordering's primary key**, ahead of the relation itself, at every list that
cites places — related locations, the archive breakdown, and the archived detail view: a parked
place never leads, even carrying the stronger relation
(`survey_never_leads_with_a_parked_location`, and
`a_detail_view_marks_and_orders_the_places_the_summary_named` on the binary). **Standing is the
only key the lists share.** Below it each sorts on its own measure — the breakdown on archive-side
sources at the location, its detail view on selection-side sources archived there — so the two can
legitimately disagree about which live place leads. What they may not disagree about is whether a
place is marked, or whether a parked one leads at all. A list's first line is its headline, and
testimony is *stated, never headlined*. This is a conformance to **stated-never-headlined**, not
a rule of survey's own: that law binds Canon-wide and already bound survey, so realising it here
is delivery of a grant, not a new one. The sweep picks one citation and survey ranks many, so the
two consume the key through different operations and share no code; what is shared is the law,
owned at `core::domain::resolution::standing_first`, and each surface carries its own pin.
**Inside** survey every list reaches it through the one import, the head of all four comparisons —
so a fifth list cannot restate it differently. `--other` keeps the
user's own order; the marker still states each place.

**A parked `--archive` is answered and marked, never refused** — the frame's label carries
`(suspended)`; it was refused only while the index hid those copies. A parked **`--other`** is
still refused by the spine's location gate: `--other` names a place to ask *about*, and a scope
on a closed root is refused, made aware of, and left untouched.

## The contentless law's index site

**`ObjectIndex::build()` (`domain/object_index.rs`) refuses contentless sources** — the law's
enforcement point in the index. Its **population** is the caller's and widened here (every root,
open door or closed); its **predicate** is the law's and unchanged, which is why the canary stays
green. A computation reading through the index inherits the refusal;
one reading `all_sources` or `selection` directly past it must re-apply the predicate itself,
and five do (`ops/compute.rs`): selection identity (`sel_object_ids`), the `--other` location
object sets, the per-location totals, the affinity complementary set, and the residual detail
read. Missing it at any of them makes an empty file read as vacuously unique, as shared or
complementary content, or as "not at" a location holding a byte-identical empty file
(`test_residual_never_lists_empty_files`).

**Unhashed and contentless are not symmetric here**: an unhashed source stays residual — nothing
can show it as shared — while a contentless one does not, there being nothing to show.
