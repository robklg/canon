# sweep/ — the finder seat

The universe-wide reduction-opportunities leaderboard: the one command whose output is *places*,
survey's unanchored counterpart — the sweep finds, survey judges, the user decides, the exclusion
ceremony records. Read-only, fresh per run: no ceremony, no decision row, no receipt. If load cost
grows, the answer is a persisted relation index with basis fingerprints, never quiet caching.

`domain/structural.rs` and its five stage files — `universe.rs` → `weights.rs` → `discovery.rs` →
`localization.rs` → `assembly.rs` — are the structural computation; `domain/lens.rs` is the
reduction lens over it; `ops.rs` owns the fetch and the inclusion policy and calls both; `cli.rs`
parses, calls and formats. Sealed behind a one-item barrel (`sweep.rs`): `pub use cli::run;` —
every other boundary (`domain`, `domain::structural`, `domain::lens`, `ops`) is `pub(super)` at
most. The sweep never calls `survey::domain::object_index`; it groups objects itself, in
`universe.rs`, over rows `ops.rs` has already filtered.

## The engine and the lens

- **The lens separation law**: `compute_structural` produces lens-free findings (relations,
  containment, gain/residual, `nature`); `reduction_lens` is a separate pure derivation (ordering,
  counterpart standing, hub and sibling-parent grouping, the reciprocal collapse, the closed-door
  partition). Never rank inside the engine, never classify in the interface — that is what leaves a
  future lens a second function over the same `StructuralSweep` rather than a rewrite. Code
  vocabulary: "structural computation" and "lens derivation". The half a spelling check can
  settle is the **direction of the edge**, and that is what carries the law: the engine's files
  name no identifier containing `lens`, pinned by `the_structural_engine_never_names_the_lens`.
  Ranking computed in the wrong place still ranks correctly, so no behavioural test would see
  the edge reverse.
- **The engine's two suspension-aware lines are not an exception and must not become one**: the
  live-preferred counterpart choice decides which place a finding *cites as evidence*, and
  `Coverage.suspended_locations` *counts* locations — neither orders anything, which is the property
  the law is about, and no more should be claimed for them. The count is inert; the citation is
  destructive, because the losing scope survives only as a `ContextRelation` carrying `location` and
  `size_pct`, so a later lens cannot recompute the discarded witness's pair statement or its
  `class`.
- **The disjointness invariant** is the correctness heart: emission at the maximal subject
  (emit-and-stop at the lifting tolerance), never ancestor-descendant pairs — "A/photos is 100%
  inside A" must stay structurally unreachable. LCA subtraction makes matched weight
  **non-monotonic** down the tree, so descent trimming must use the pre-subtraction upper bound
  (`MatchedWeights.upper`), never the true matched weight.
- **Floors trim output, never existence** (the two-walk merge): the floors gate discovery, so
  below-floor fragments lift into an aggregated parent candidate, and a second floor-released walk
  over the same universe finds what they kept off the board — counted always
  (`SweepStats.below_floor_subjects`), assembled and tagged only on request
  (`SweepParams.assemble_below_floors`), disjoint-filtered against floored subjects so the default
  view never changes. A below-floor finding never displaces an above-floor one.
- **Inclusion policy lives in ops**, in `compute_sweep`, which owns the fetch: present,
  non-excluded, carrying content (`is_contentless()` — the contentless law; empties counted as
  context in `empty_files_ignored`, never filtered silently). Excluded content is resolution, not
  overlap: it leaves the comparison and returns only as per-subject context counts (substantial =
  ≥ `emit_floor_files`). Suspended roots stay in — **never removed from computation**.
- **`SweepParams` are named calibratable constants** (lifting tolerance, candidate/concentration
  thresholds, ubiquity cap, emit floors); recalibration changes constants, never code paths.
  **`LensParams` are the lens's own**, kept apart from them so the lens separation law reads in the
  types: `root_entry_bucket` (default 1 — fewer than ten unresolved sources remain),
  `nearness_render_bucket` (2), `sibling_run_min` (2), `sibling_parent_coverage` (0.60).
- **The interface classifies nothing**: every ranking and grouping decision arrives in the typed
  result, and the member display cap (`ENTRY_MEMBER_CAP`, shared by both multi-place entry kinds) is
  its only trimming — display furniture over a fully-carried `Vec`, omissions counted, `--all`
  revealing them. Counterpart wording: the status line names the counterpart and its *standing*
  ("counterpart: archived, …"), uniform across subset and mirror; declarative always ("inside X",
  never "keep X"); never imply a preferred side — "keeper" is retired vocabulary, because a
  containment fact is not a worth verdict.
- **Finding paths render full, never capped** — a deliberate divergence from the capped-column
  convention: on this surface, location is the reader's primary context.

## Headline and entries

- **Headline direction is taught, not assumed**: a single finding's headline is its *subject*; a
  hub's is the shared *counterpart*, on its own "shared counterpart — N places hold copies inside
  it" line. A subject on an archive root renders `(in the archive)` from
  `StructuralFinding.subject_is_archive` (derived in domain — the interface never infers
  archive-ness from a path) **and ranks below an equivalent source subject**: this is a *reduction*
  board, and a place already in the archive holds no unresolved content, so it cannot compete on
  mass alone. Demoted, never removed. The term sits directly after tier in `rank_key`, ahead of
  gain, and reads the **subject** side only, so a hub of source places under an archive counterpart
  still competes as source (`an_archive_subject_ranks_below_an_equivalent_source_subject`).
- **One slot, one claimant.** A slot is claimed by exactly one axis, in a fixed precedence, and a
  place an earlier axis claimed is unavailable to a later one: **0 set-aside** (subject on a
  suspended root) → **1 sink** (evidence on one) → **2 root** (places on a near-retirable source
  root) → **3 sibling-parent** (siblings under one parent) → **4 hub** (places sharing one
  counterpart) → **5 single**. Set-aside before sink is what makes "a place suspended on both sides
  is counted once" structural rather than remembered; the root claims ahead of the hub because
  finishing a root resolves more than any one place on it, and a hub that falls below two members
  degrades to singles by its own existing rule
  (`a_root_entry_claims_places_away_from_a_hub_which_degrades`). The list is open at the end.
- **Decision-grouping precedes evidence-grouping**: a **hub groups by evidence** (many places, one
  counterpart), a **sibling run groups by decision** (many places, one act that covers them), and
  this is a decision board, so the decision unit wins. Owner: the axis order in `reduction_lens`
  (`domain/lens.rs`), pinned by `a_sibling_run_claims_ahead_of_a_hub`. Run-first splits a cross-root
  hub, which is correct rather than a cost — places under two roots are two decisions — leaving the
  hub as the residual grouping for places with no common decision unit.
- **A multi-place entry's figure is an upper bound and is never named `gain`.** Three sites carry it
  — the parent entry, the root entry, the suspended footer — for one reason: LCA subtraction removes
  intra-root duplication from the copies' common ancestor **upward**
  (`domain/structural/weights.rs`), so two places *below* that ancestor each legitimately count the
  other's copies while at most one of them can ever be let go. Both numbers are true; only the
  inequality is, which is why the surface says `gain_bytes_upper`/`gain_files_upper` and `up to`
  (`a_parent_entry_states_a_bound_and_never_calls_it_gain`). `HubEntry.total_gain_bytes` is not a
  precedent: a hub's members point *into* a counterpart that is never itself a member, so its
  summands are separated **by role** and are co-dismissable.
- **A multi-place entry speaks about its headline place once.** Notes and excluded context attach to
  the parent's or root's own top, never per member: the note lookup matches a whole subtree, so
  listing the members too would print each note twice, and a per-member excluded count would
  partition the headline's own number twice over.

### The parent entry

- **One situation, one slot.** Findings whose subjects are siblings under one parent take one slot
  headlined by that parent, carrying them as members: ten month folders each mirroring their own
  counterpart are one situation told ten ways. **The parent is an entry, never a finding** — nothing
  is emitted for it and no containment is computed for it, so no ancestor-descendant pair of
  findings comes into existence and the disjointness invariant is untouched by construction rather
  than by care (`a_parent_entry_creates_no_finding_for_the_parent`). Entirely lens-side.
- **No walk configuration produces a parent together with its children**, so do not re-litigate this
  by calibration: a lower lifting tolerance makes the parent emit Clean and return before
  descending, a lower candidate threshold hits an arm gated on `!emitted`, raised floors suppress
  the children rather than carrying them, and the floor-released walk behaves identically. Every
  path produces the parent *instead of* its children — it was never a candidate that lost, it failed
  the tolerance and the descent continued past it.
- **Coverage both states and gates.** `ParentEntry.coverage` is the fraction of the parent's own
  sources lying under its grouped members; below `sibling_parent_coverage` no entry forms, because a
  14% headline would claim a decision unit seven times the situation. **Below the gate nothing is
  hidden** — the places compete individually exactly as before
  (`a_parent_below_the_coverage_gate_does_not_group_and_its_places_compete_individually`) — and an
  entry that cannot state its coverage does not form at all, the figure being the entry's own claim.
- **The population is the comparison-participating slice** (`PlaceCensus`, projected in ops from the
  same rows `compute_structural` is given): present, non-excluded, non-contentless — excluded
  content is resolution rather than overlap and a contentless source resolves with its place, so
  counting either would measure numerator and denominator over two populations. **A lens input,
  never an engine output**: per-folder subtree sums returned from the structural computation would
  put a ranking input inside a computation the lens separation law calls lens-free
  (`the_coverage_denominator_counts_only_comparison_participating_rows`). Neither it nor
  `RootNearness` issues a second query.
- **Members state their own counterpart standing**, for the reason the root entry already
  carries: a parent entry ranks on standing aggregated over its members, every ranking factor is
  a stated fact on the entry, and it is what makes acting on a member safe. A hub's members leave
  it off because they share one counterpart whose standing sits on the hub's own headline
  (`each_member_states_its_own_counterpart_standing`). The entry speaks about its parent **once**:
  notes and excluded context attach to the parent's own top, never per member, because the note
  lookup matches a whole subtree and a per-member count would partition the parent's own number
  twice over.
- **The entry states the nearness term it ranks on**: its members share one root, `entry_key` reads
  that root's `ranking_bucket`, and `parent_entry_lines` carries `nearness_line` above the bound. A
  slot ordered by a factor it states nowhere would break the coextensiveness rule below.
- **Grouping is at the immediate parent and never recurses**, and the depth is deliberately not a
  constant: lifting trades slots for honesty and coverage collapses fast, so one level is where the
  trade still pays and a configurable depth would invite a value the evidence says is always wrong
  (`grouping_never_recurses_past_the_immediate_parent`). A finding that *is* its root's top has no
  parent; a root's own top **is** an eligible parent, and no `(whole root)` marker is manufactured
  for such a headline — that marker is derived from the folder tree, never read off an empty prefix.
- **A parent entry on an archive root emits a handoff that does not run**: sibling findings whose
  subjects stand on an archive root can clear the gate, and the entry's way back,
  `canon survey <archive parent>`, meets survey's refusal of a scope resolving entirely inside
  archive roots (`SurveyExit::FrameRefused`, exit non-zero). The axis deliberately grows no archive
  test of its own — archive-ness is spoken once, in the nearness projection — and parse-only
  round-trip checking cannot see it, so it is stated here rather than papered over.
- **No coined noun on the surface**: "N places under here", a percentage, the members.
  `ParentEntry` names tree structure, not a concept, and the surface must not mint a domain word
  sideways (`a_parent_entry_states_its_coverage` pins the absence).

### Reciprocity

- **Two places that mirror each other are one overlap told twice — and *mirror* is load-bearing, not
  decoration.** `collapse_reciprocal_places` (`domain/lens.rs`) matches `RelationClass::Mirror` on
  both sides. Topology alone does not carry the claim: two reciprocal **subsets** contain each
  other's counterpart in exactly the same shape while each holds a majority of content the other
  never mentions, so folding them would delete a real opportunity. The engine's own dedup matches on
  the cited counterpart being the *same place* and so misses the pair at different depths — each
  mirroring a **child** of the other — which then takes two slots for one overlap
  (`a_reciprocal_pair_at_different_depths_collapses_to_one_entry`). The survivor is the entry whose
  subject sorts first, deterministically rather than by weight, and it **states the reciprocity
  rather than deleting a fact** (`RankedSweep.reciprocal_places` → the surface's `also mirrored by
  …` line). Reciprocity is the whole criterion and is never weakened to one direction: `a` inside
  `b` beside `b` inside `c` share `b` in opposite roles and are two genuine situations. **A survivor
  states one mirror**, and a second is unreachable rather than handled — one place mirroring two
  others forces those two to be ancestor-related on one root, which the disjointness invariant
  forbids; `debug_assert`ed at the insert. **Entries headlined by a counterpart do not take part**:
  a hub has no subject side of its own, and a root or parent entry is headlined by a place that is
  no relation's counterpart. Whether a place that is a subject in one entry and a *counterpart
  headline* in another should also collapse has no decided survivor semantics and is left open
  rather than guessed.

### Root nearness

- **A root close to done outranks a root barely started.** `RootNearness` (`domain/lens.rs`) holds
  the retirement readiness review's own remainder measure per root, projected via
  `core::domain::resolution::unresolved_remainder` — never re-derived, and never a place-grain count
  of its own, since the review measures present *source rows* and where the units differ the review
  wins. Ops projects it from one extra `batch_check_archived` over object ids already in hand: **no
  second full pass and no per-root query**, the naive shape (`fetch_root_story` or `build_account`
  once per root) issuing two queries per root. It reads `all_sources`, never `kept`, so it stays the
  identical *input* the readiness review is passed and the two cannot diverge if `classify_present`
  ever changes.
- **Source roots only.** An archive root is never retired, so nearness says nothing about one: it
  carries no projection, buckets at the far end of the scale, ties on the term and falls through to
  gain. Spoken **once**, in the projection — deliberately no second archive test at the claiming
  site, because a second spelling of one rule is what lets the two disagree later
  (`an_archive_root_never_forms_a_root_entry`).
- **The bucket is the only place the cut points are written**: order of magnitude ascending, 0 → 0,
  1–9 → 1, 10–99 → 2, 100–999 → 3, 1000+ → 4. Coarse deliberately — a board that reshuffles when
  nothing the user did changed reads as broken, so a remainder must cross a decade to move anything
  (`a_remainder_changing_by_one_does_not_reshuffle_the_board`). **Bucket 0 is exactly
  `Readiness::NoBlockersFound`**, a join with the review's own verdict rather than a taste, pinned in
  two halves across the `retire` seal.
- **`ranking_bucket`, not `bucket`, is what every ordering key reads**: inside
  `nearness_render_bucket` it is the bucket itself, above it a single tie value shared with roots
  that have no projection at all. Separating two roots both far from done bought nothing actionable
  and cost an unexplainable demotion; tying above the regime makes ranking and statement coextensive
  by construction, leaving no second rule to drift. Its companion is the invariant stated where
  `LensParams` lives — **`root_entry_bucket` must not exceed `nearness_render_bucket`**,
  `debug_assert`ed at the one site params are consumed
  (`a_root_entry_can_never_form_outside_the_regime_that_states_it`): a root entry states its
  remainder *unconditionally*, so one qualifying from outside the regime would state a term that did
  **not** order it.
- **A hub states it too, naming the member root that set its term.** `HubEntry.nearness_root` carries
  that root (a `Location` with empty `rel_prefix`, the shape `RootEntry.root` uses), chosen
  deterministically — lowest bucket, root path tie-break — and the hub's ordering key reads that same
  field rather than re-taking a minimum, so a hub's position and the line explaining it are one
  derivation. `None` is exactly "nearness tied for this hub". A member inside the regime but *above*
  `root_entry_bucket` stays a hub member and lifts the hub, which is the case that would otherwise
  rank unstated (`a_hub_lifted_by_an_in_regime_member_names_that_members_root`).
- **In the key, never in the engine.** The term sits in `rank_key` directly after archive standing
  and **ahead of gain**: what is left on a root near the end of its story is small *by definition*,
  so behind a size-led key it would be invisible exactly when it matters most. Behind archive
  standing, because nearness is meaningless for an archive root and the tie must be reachable. A hub
  aggregates it as it aggregates every ordinal term: best member.
- **The nearness line's presence is the explanation.** There is no composite score, so the order
  explains itself by stating its factors: a place on a root inside the regime carries `N unresolved
  sources remain on <root>` one line above `gain:`, and the **absence** of the line means nearness
  could not have moved this entry. The claim is regime membership, deliberately not a per-board
  counterfactual — computing that would mean sorting the board twice. It is one rule and not two
  kept in step, because `ranking_bucket` ties every root above the regime, so "nearness could have
  moved this" and "the board states it" are the same condition
  (`the_nearness_line_appears_only_where_nearness_moved_the_order`). Which roots the board states is
  decided **in the lens** (`RankedSweep.stated_remainders`) and merely rendered by the interface.

### The root entry

- **It states a remainder and never claims readiness.** A qualifying source root takes **one slot**,
  headlined by the root with `(whole root)`, carrying its places as members: without the slot every
  place on a nearly-done root inherits the boost and three places left becomes three top slots — the
  flooding the axis exists to prevent, rebuilt on exactly the roots the work means to celebrate. A
  qualifying root with **one** place forms no entry: one place is already one slot, and carries the
  remainder fact and the handoff itself. Canon proves NOT READY and never the other side, so no
  wording here may congratulate; zero renders as `no unresolved sources remain`, which is a fact,
  and the review the handoff names is what is entitled to judge
  (`a_root_entry_states_a_remainder_and_never_claims_readiness`).
- **The handoff is `canon roots retire path:<root> --dry-run`**, a report that **exits 0 on either
  verdict**, so what is printed runs as printed on a root that is not ready. One precondition sits
  ahead of the review and is not a verdict: with **no archive root registered** the ceremony refuses
  (the book would have no shelf) and the handoff exits non-zero. The board can reach that state —
  with no archive root nothing reads as covered, so only a root holding very few sources buckets low
  enough to form an entry — and `every_emitted_argv_parses` checks parsing, never execution, so it
  cannot see it.
- **A root entry's members state their own counterpart standing**; a hub's do not. Every ranking
  factor is a stated fact on the entry, and a root entry ranks on `counterpart_standing` aggregated
  over members that each have their own — a hub's share one counterpart, whose standing sits on the
  hub's own headline. It is also the fact that makes acting on a member safe. The words are the
  surface's existing ones: `counterpart_line`'s `archived`/`present` for a pair, the scattered
  qualifier's archived-location count for coverage (`member_standing`).
- **`StructuralFinding.subject_is_root_top` is derived** in `domain/structural/assembly.rs` from the
  folder tree (the top is the one node with no parent), beside `subject_is_archive` and for the same
  reason: the interface must never infer root-ness from an empty path prefix. `(whole root)`
  composes with `(in the archive)`, and an archive root's top gets both markers and no retirement
  handoff (`the_whole_root_marker_is_derived_not_inferred`).

### The closed door

- **Computed always, ranked never.** A suspended root is never dropped from the universe — a live
  folder duplicated entirely inside a parked one would then read as unique — but reading a parked
  place *for resolution* is none of the four things suspension permits (opening, seeing,
  remembering, testifying), so a view sets the root aside by default. Inclusion is an **ops**
  question and unchanged; which places earn a slot is a **board** question and belongs to the lens.
  Two axes, partitioned in `reduction_lens` before grouping:
  - *Places standing on a parked root* (`StructuralFinding.subject_suspended` — the field's only
    consumer).
  - *Places whose evidence stands on one* — **the parked-evidence sink**, owned by
    `parked_evidence_root` in `domain/lens.rs`, the one function answering both "does this place
    leave the board?" and "which root explains it?" so the two cannot disagree. It reads
    `FindingNature::Verify`, the structural statement *this place's evidence sits behind a closed
    door*. Archive or source behind the door makes no difference — the trigger is the door, not what
    stands behind it. A hub leaves whole: every member sharing a parked counterpart sinks, so the
    hub never forms.
- **Subject first**, so a place parked on both sides is counted once, on the root it stands on, and
  each place lands in exactly one `SuspendedRootTally` and one cause within it — which is what
  licenses the *counts* and licenses nothing about bytes: place-disjointness does not make two
  places' bytes distinct, and the two causes on one root are the same content seen from opposite
  sides, so the tally type keeps the two masses apart and states the surviving imprecision.
  Partitioning *before* grouping means a hub that would have lost members never forms and its
  aggregates are born correct (`a_partitioned_board_matches_one_the_parked_places_never_reached`).
- **Where it does not apply**: the universe and every quantity computed over it (gain, residual,
  containment, archive coverage); scattered (`Coverage`) findings, which are neither sunk nor
  demoted and instead state `suspended_locations` beside the archived count; and context relations,
  which may name a parked place — stated, never headlined
  (`a_suspended_root_stays_in_the_universe_but_leaves_the_board`).
- **Suspension decides which place is cited as evidence, never what the evidence says.** The
  counterpart choice in `domain/structural/localization.rs` prefers a scope the user can look at:
  `find(concentrated && live).or_else(find(concentrated))`. The sort is by `subject_bytes`
  descending and `concentrated()` tests the same quantity, so this is a strictly narrowing selection
  over an unchanged sort, inert wherever no live scope qualifies. Citing a different place moves
  what describes the *cited relation* (`pair_size_pct`, `counterpart_share_pct`, possibly `class`)
  and nothing computed over every location from `raw`; the cited scope is tracked **by index, not by
  position-in-sort**, and context relations are every scope except it
  (`a_live_scope_wins_the_counterpart_choice_over_a_suspended_one`).
- **Suspension is about attention, never about mount state.** A parked root is not a disconnected
  drive, and custody stands — suspending never un-archives. What the closed door withdraws is the
  board's willingness to *invite* an action, not the truth of the claim, and no wording on this
  surface may conflate the two.
- **No in-entry suspension furniture.** With both axes in force no shown entry can have a parked root
  on either side, so `counterpart_line` and `print_hub` carry no suspended branch and `HubEntry` no
  `counterpart_suspended` field. Suspension is stated on the footer's own lines and, for scattered
  evidence, as a location count; nowhere else.
- **The suspended footer**: one line per suspended root, never one per reason — the user thinks in
  the act they performed. Grammar `<root> suspended — not ranked: <causes> · <way back>`, the two
  causes running in parallel and differing by one word (*on it* / *with copies on it*), each
  carrying **its own** figure as an upper bound; never one combined mass. Above three roots
  (`SUSPENDED_ROOTS_NAMED_CAP`) the lines collapse to a count with the suspended-root listing as the
  way back, summing each cause across roots and never merging the two. They print **first**, because
  they explain a board that changed without the user acting on it, and they print under `--all`,
  because `--all` reveals what the floors and the cap hid, never a door the user closed. The way
  back is `canon roots unsuspend` and only that: a sweep-local flag partially undoing
  `roots suspend` would be a second control for one meaning. "Not ranked", never "hidden" or
  "removed" — suspension changes position, never existence.
- **Known imprecision, stated rather than papered over**: `SweepStats.below_floor_subjects` counts
  every below-floor subject including parked ones, so `--all` reveals fewer entries than that count
  offers. The suspended lines cannot close the gap and must not be described as doing so — the two
  numbers never appear on one screen (below-floor findings are unassembled by default and so never
  reach the tally, and the below-floor line is suppressed under `--all`), and reporting them apart
  would need a second stat the default path deliberately does not compute: localizing below-floor
  subjects is the work `assemble_below_floors` exists to skip. Held by
  `below_floor_minus_set_aside_equals_what_rendered_under_all`, over a fixture where every finding is
  below the floors; across a real board only the direction holds, since hubs and reciprocal dedup
  collapse findings into fewer entries.

### Surface

- **The handoff round-trip law**: every handoff line comes from one builder returning
  `(display, argv)`, and a test parses every emittable argv through the real clap definitions, so
  CLI drift is a test failure rather than user-facing rot. It covers the footer's ways back too
  (`suspended_lines`, `every_emitted_argv_parses`): `roots unsuspend` takes a root specifier
  (`path:<path>`), never a bare path, and a way back that does not run is worse than none. Handoffs
  judge (survey), never a ready-made exclusion: the journey is find → judge → decide → record.
- **The anti-creep guard**: no `--where` or population shaping, no per-pair/threshold/scope flags,
  no JSONL. Drill-down belongs to survey and compare — the sweep must not become a worse survey.
- **An empty board is an answer, never a false one** (`empty_board_line`, `cli.rs`): the headline may
  deny that folder-level redundancy exists **only when nothing was withheld from the board**. Where
  suspension set places aside it says "outside the suspended roots"; where the cap emptied it
  (`--limit 0`) it names the limit, and the cap precedes suspension because it is the proximate,
  in-place-reversible cause. The floors are deliberately **not** in that list — "worth attention" is
  scoped to *above the floors* by the sentence itself, so a below-floor-only board may still deny,
  and under `--all` the floors lift unseen, leaving the claim weaker than warranted but never false.
  A footer contradicting the headline one line later is not a repair for a false headline
  (`an_emptied_board_never_claims_there_is_no_redundancy`).
- **The board's order is total, and `entry_order` is where it is spoken**: ranking key, then place,
  then **entry kind** (the axes' own claiming precedence). The last term exists because two entries
  can reach the same key *and* the same place — a run headlined at the folder a hub's members point
  into — while within one kind a path is unique by construction; without it `sort_by`'s stability
  decides, leaving the answer resting on a property of the construction loops that nothing states
  and any reordering would silently break. The comparator is named rather than inlined because the
  tie it closes is unreachable through `reduction_lens`, so a test driving the whole lens cannot
  tell a total comparator from a lucky one
  (`the_board_order_is_total_so_a_full_tie_cannot_rest_on_construction_order`).
