# trail/ — read the decision trail

Realizes the domain-language **Decision trail** and **Provenance card** concepts. `domain/`
holds the timeline merge, the two-claims placement law, the crossings selection rule, the
composition card and the decision-id shape predicate; `ops/` composes the scope- and time-lens
reads, the evidence gate, the crossings computation and the composition-card fetch; `repo.rs`
is trail's own SQL; `cli.rs` parses and dispatches (`canon trail`, `trail show`, `trail
crossings`), and `render.rs`/`jsonl.rs` are the two output voices.

Two laws this unit speaks are owned elsewhere and described in `core/CLAUDE.md`: the
fate-vocabulary law on `core/domain/fate.rs`, and the extraction round-trip law on
`core/domain/extraction.rs` — core consumes both independently of trail. The extraction
ledger's row-claim rules sit in the top-level CLAUDE.md instead, because apply, `ledger
reindex` and this unit all depend on them.

Sealed behind a barrel in three groups. The CLI entry points, their argument types and the
exit enum: `run`, `run_crossings`, `run_show`, `TrailArgs`, `CrossingsArgs`, `TrailExit`. The
finished report an in-crate consumer names: `compute_trail`, `TrailParams`, `TrailResult`,
`TrailView`, `TimelineEvent` — retire's book compile composes over that finished report, never
over trail's internals. And the report's own public field and variant types, riding along
because a future crate boundary cannot expose a public field of a private type: `RowAspect`,
`ScopeMatch`, `DayGroup`, `DayRollup`, `FateLine`, `WhenValue`, `ArrivalRollup`,
`ExtractionRollup`, `RearrangementRollup`. Everything else seals — `compute_show` and the
Show/pointer family, the composition card and its whole vocabulary, the two-claims predicates,
`repo.rs` — with zero external consumers.

**Trail read-layer conventions** (`ops/compute.rs`, `ops/show.rs`, `domain/timeline.rs`, `domain/placement.rs`):

- **Reader/recorder separation**: `ops/compute.rs`/`ops/show.rs` read, `core/ops/decision.rs`
  records, and the reader never depends on `receipt.rs` or on recorder internals. **No receipt-file
  reads on any query path** — DB projections only; receipt paths render as pointers (the Provenance
  Consumption Readiness ADR). One narrow carve-out: **pointer relocation may stat the book, never
  read it** — `relocate_pointer` observes the redirect target's *existence* and nothing else, and
  unreachability degrades to an honest hedge. This is not a crack for receipt-reading to creep
  through.
- **Retired-root rendering, two surfaces** (`PointerRelocation`,
  `retire::ops::find_retirement_covering_path`). A `show` pointer whose locus root is retired
  renders where it leads now — `Gathered` (redirect to `<book>/ledger/<file>`, mechanical because
  the gather preserves `.canon-ledger/` subpaths), `NotGathered` (delegates the why to the book's
  gap record, since a stat can't distinguish on-faith from damage), or `Unreachable`. Detection
  reuses the rm guard's `fetch_latest_receipt_for_root`, so a plain-`rm`'d or Off-mode-retired root
  keeps the plain pointer. A scoped `trail` that misses on a retired root's path prints the
  retired-scope statement instead: descendant-or-equal matching on retire decisions' scope
  snapshots (never ancestor), bound decisions only, newest wins, exit 0, and **gated on liveness**
  — it answers only when no indexed root, suspended included, contains the asked path, a
  bound-but-unreleased ceremony leaving its artifact reference while the root stays live. A status
  filter would be wrong the other way: an abandoned-bind root later plain-`rm`'d must keep matching,
  its book standing. A miss on a **live** root falls through to history-tense resolution
  (`core::ops::scope::resolve_history_scope`, the same pipeline minus the source-existence gate),
  because an emptied place still holds extraction rows and notes and refusing would 404 exactly the
  best-resolved places. The leniency is history-tense only: the source-existence gate stays right
  for the present-tense commands (`ls`, `survey`, `exclude`), which ask what is there now. A miss with no retirement and no live root propagates the original error
  untouched. Paths are cleaned lexically, never canonicalized — an old mount path need not exist to
  be asked about.
- **The evidence gate** (`ops::place`): history-tense leniency has a floor, or a mistyped path
  renders a plausible view borrowed from whatever it happens to sit under. Trail **conjugates** the
  scope boundary's set-asides rather than consuming them: each sourceless path runs
  `place_knowledge` — a retirement covering it answers first, then `history_evidence_at`, and
  otherwise it is stated as unknown. Four independent kinds of evidence, any one sufficient:
  sources ever known at-or-under (presence-inclusive); notes at-or-under; an extraction endpoint
  at-or-under (origin by `root_id` + `rel_prefix`, destination by the absolute write-time snapshot
  path rather than by `destination_root_id`, so a row still answers once that id has gone stale);
  or a decision scope **descendant-or-equal**, never ancestor, which is the two-claims placement
  law's direction. Both queries live in trail's own `repo` — core stays history-agnostic — and route
  through `core::repo::db::path_at_or_under_sql`, so a real path containing `_` or `%` matches
  literally. A path with evidence is re-included in the view. **The gate runs at both doors**: a
  place reached by standing in it is the same place as one named on the command line, or a bare
  `canon trail` in a storyless folder renders its *parent's* decisions under that folder's heading.
  A root's own top is exempt at both doors alike — the boundary policy's "root-level paths are
  always kept" spoken once more rather than re-derived — and disjoint from the retired-root CWD
  block beside it, which answers for a CWD under **no live root**. The `trail show` hint fires only
  on the no-evidence statement, for an all-digits single argument
  (`domain::place::looks_like_decision_id`), a real folder named `191` having had evidence and never
  reached it. Exit non-zero via `TrailExit::PlaceUnknown` when every asked-about place is unknown,
  no `Error:` prefix, stdout left clean. Guard: `an_ancestor_scope_is_never_history_evidence`.
**Crossings conventions** (`domain/crossings.rs`, `ops/crossings.rs`, `render::print_crossings`) — the counterpart door, the trail's fourth axis beside place, time and one decision:

- **The view is the boundary, and a global view borrows the named counterpart.** A crossing is a
  row that crosses the view's boundary; a global view has none of its own, so membership-by-view
  would classify everything as a rearrangement and answer nothing. Borrowing is what makes
  `crossings --global --origin <drive>` the *same* computation as standing at the drive rather than
  a second one to keep in agreement. **When a global view names both counterparts, the deeper one
  takes the boundary where they nest**, the origin being the tie-break where they do not: both
  flags are otherwise pure narrowings that never touch the framing, so exactly one must be promoted.
  Where they nest the row sets genuinely differ — framing on the *outer* path puts both endpoints
  inside it, so a recorded delivery reads as a rearrangement — and the deeper path is the more
  precise statement of the place asked about, which is `build_scope_matches`' *deepest match wins*
  reasoning applied to two named asks. An **unbounded** ask names nothing to borrow and is refused
  at the interface as malformed (an `Error:`, never `PlaceUnknown`, which stays reserved for a
  well-formed question about a place Canon never knew). The refusal is keyed on the **resolved
  scope**, never on the `--global` flag (`cli::is_boundless`), because standing outside every known
  root reaches globality *silently* through the CWD fallback: a flag-keyed guard lets that second
  door state "nothing crossed" at exit 0 about content that had crossed. Owner:
  `domain::crossings::{CrossingFilter::boundary, crossing_verdict}`; guard
  `a_global_view_borrows_the_named_counterpart_as_its_boundary`.
- **Membership is `placement_in_view` on both ends, never `scopes_touch`** — the two-claims placement law, at
  the boundary test (via `classify_row`) and at both narrowing filters alike. A counterpart named at
  a deep path is exactly the ancestor shape that manufactures arrivals at sibling folders. Matching
  is on write-time snapshot paths, so a removed — or removed-and-re-added — counterpart root keeps
  its link; `core::repo::decision::fetch_extractions_by_origin_root` is **not** the reuse here,
  because it keys on `root_id` and goes silent on precisely the removed and retired counterparts
  this door exists for.
- **`crossing_verdict` is total, and the two non-crossings are distinguishable.**
  `Crossed(aspect)` / `Rearranged` / `NotOurs`, exhaustive with no `_` arm over the four-cell table.
  A view whose every row stayed inside it must *name* the rearrangement, or "nothing crossed" reads
  as "nothing ever happened here". A row excluded by a named filter is `NotOurs` and never
  `Rearranged` — it was never judged against the boundary, and counting it as rearranged would
  invent activity inside the view (`a_non_crossing_says_which_kind_it_is`).
- **The outbound counterpart key is derived, not the ledger's leaf**
  (`domain/grouping.rs::group_destinations`, consumed by `ops::crossings::display_keys` and by
  `ops::compute::build_extraction_rollup`). The ledger records a placement at `(origin dir,
  destination dir)`, so a pattern that fans one apply across a folder per day makes the leaf a place
  nobody named and the bare outbound view answers *which places?* with a list of date folders. The
  inbound side keys on the origin **root** path — the grain the card attributes at — and needs no
  such rule. The derivation is budget-bounded refinement over the component tree of the distinct
  leaves in view: start at their deepest common ancestor, **force-refine** past every live root
  floor whatever the budget says (a key at or above a root answers "where did it go?" with the whole
  archive — true and useless), then refine greedily largest-group-first while the group count stays
  within `DESTINATION_GROUP_BUDGET`. **Floors are passed as data** — the domain fetches nothing.
  Two properties hold **by construction** and are read off the rendered line: the keys form an
  **antichain**, so a path never reads as containing rows its own entry excludes; and the groups
  **partition** the leaves, so the sum over entries is the section total. A grouped entry states its
  folder count past one; a single-leaf entry states none, the path above it being the place. Four
  recorded residues, none of them defects: the budget bounds the greedy pass only, so an archive
  whose destinations sit one component below its root has no directory between root and leaf to key
  on; leaves under no live floor may group to their own deepest common ancestor, rather than
  inventing a floor for a place the index no longer knows; a group **holding its own key as a
  member** cannot be split into disjoint sibling subtrees, so the antichain wins over force-refine
  and the group stays whole with its count carrying the honesty; and the greedy pass **stops at the
  first unaffordable refinement rather than skipping to smaller groups**, the ordering being
  mass-priority and the budget a ceiling rather than a target to fill. Pinned by
  `keys_are_an_antichain_and_groups_partition_the_leaves`.
- **The rollup and the door share the derivation, not merely its result.** `rollup_parts`' own
  counterparty count is **discarded** on the extraction side and both surfaces call
  `group_destinations` over their own rows: a door listing three destinations beneath a rollup still
  saying forty-seven reintroduces one line up — with the teaching hint between them inviting the
  comparison — exactly the unexplained-number defect the reconciliation line exists to prevent.
  `--destination <grouped key>` narrows into an entry and renders it at ledger grain, the coarsening
  being a display key and never a loss of reach (`a_destination_flag_narrows_into_a_grouped_entry`).
- **The bare view is the rollups itemized — wherever both surfaces speak.** Section totals go
  through `ops::compute::rollup_parts`, the same builder the three whole-history rollups use, over
  the same rows keyed by `domain::crossings::counterpart_of`, so the *summation* is shared rather
  than merely matched. **The row sets are not**, and the claim stops there: the rollups establish
  origin membership by `(root_id, rel_prefix)` while this door uses the absolute snapshot path,
  which `row_aspect`'s contract sanctions ("centralize the rule, not the evidence"). They part where
  a root's id has gone stale — remove a source root and re-add it at the same path, and the rollup
  falls silent while crossings still answers from the snapshot; do not "fix" that by keying this
  door on root ids. `the_bare_view_itemizes_exactly_what_the_rollups_count` pins the agreeing case,
  which is what would break if the counterparty keys or the summation drifted apart. The teaching
  hint prints only beneath a rendered *crossing* rollup, so the door goes untaught in exactly the
  views where it is the only surface holding the answer.
- **The counterpart evidence gate is conjugated, not widened**
  (`ops::crossings::counterpart_is_known`). The **ledger arm runs first**, from rows already in
  hand: a counterpart is by definition a place the ledger names, and it is the one evidence class
  that survives its root being removed — the one `history_evidence_at` cannot reach, since that
  opens on `find_containing_root` and returns false for a path under no live root. Without it a
  plain-`rm`'d origin root reads as unknown and the door refuses to open on a line the card printed
  a second earlier. Then `place_knowledge` unchanged; `ops/place.rs` is untouched. Guard:
  `a_plain_removed_origin_root_still_opens_its_door`.
- **A counterpart path resolves by the scope pipeline's own rule, plus trail's leniency**:
  soft-match against known roots → `fs::canonicalize` → lexical clean (`cli::resolve_counterpart`).
  The first two arms are `core::ops::scope::resolve_path`'s, because a counterpart is the same kind
  of argument as the positional scope path beside it and must not resolve by a second rule; the
  third replaces that function's bail, since the removed and retired mount paths this door exists
  for are precisely paths that no longer exist. Counterparts are **paths only**: a root spec is
  refused by name (`a_root_spec_argument_is_refused_by_name`), since `id:N` cannot name a sub-root
  counterpart and a removed root's id went with it.
- **The reconciliation line is the card's number, never a second count of it**
  (`ops::crossings::reconcile`; `render::reconciliation_line` composes). It renders only when the
  view is scoped and human-readable, an origin was named, **no destination was named**, and
  `compute_composition` carries a `FromRoot` line whose root path equals that origin **exactly**.
  The destination gate is not a matter of grain like the others: `--destination` narrows the
  delivered count and narrows nothing about the card, so the pair could state that more files stand
  here than were ever delivered — arithmetically impossible, from the one line designed never to
  guess. `--origin` narrows the card's lookup by the same root path, so both sides move together
  and it needs no gate. Over-suppressing where a named destination happens not to narrow anything is
  accepted — absent-over-approximate is this line's doctrine — while suppressing altogether stays
  rejected, an absent line being indistinguishable from "nothing was lost". It states two
  observations and **no cause**, the delivered-vs-standing gap never decomposed because its causes
  are indistinguishable from these rows, and it is silently absent at every other grain: a sub-root
  origin has no card number, a `MultiOrigin` line no attribution at all. **Two axes, files and
  decisions**, whose clauses compose **independently**: the card counts decisions that stamped
  surviving sources, the door counts decisions holding extraction rows, and neither can be made to
  agree without lying about its own register. Each clause states an "of the" construction only where
  its own two numbers arithmetically permit one. **Containment fails in both directions** — a
  decision stamp survives a scan-observed move *into* the view — so only the
  standing-below-delivered branch states a proportion. **Equal counts are not a claim about the same
  files**: deliver files here and one elsewhere, lose one, let a scan observe the stray moving in
  with its stamp, and the totals match while the sets do not, so equality and
  standing-beyond-delivered share one spelling, two counts side by side (`229 files stand; 229 were
  delivered`). The decisions clause carries **no pronoun** — two of the three files-clause forms end
  on the delivered count, so a "them" would sit beside the wrong number — and mirrors the files
  clause's shape (`15 decisions stand; 17 delivered`). It **acquires no gate of its own**, both
  numbers coming off the same card line at the same match
  (`the_decisions_clause_inherits_every_file_clause_gate`, which walks all six with a positive
  control first). Five of the six red-smoke; the **global** one cannot, because
  `compute_composition` refuses a global scope first — the gate is held in two places, and the
  test says so rather than claiming a branch it never reaches.
- **A no-crossing answer names the counterpart it was asked about**
  (`render::{nothing_crossed_line, asked_about}`). A named counterpart lives on a `CrossingSection`
  and empty sections are omitted rather than printed empty, so without this rule a named counterpart
  matching **nothing** cannot reach the output at all — leaving the near miss less informative than
  the total one, since `UnknownCounterpart` already names the paths it refuses. Both variants name
  it (`Rearranged` too, or the fix recreates the same silence one variant over); a **global** view
  keeps its own shape (`No recorded crossing at <path>`), having borrowed the named counterpart as
  its boundary and having no "here" to measure between. Origin and destination read alike — the ask
  is what was named, not which flag named it. The live case is a counterpart that is known but is no
  delivery's endpoint, such as a retired book's own directory inside the archive root. Two
  neighbouring cases this must not swallow: a nested counterpart *with* rows still reaches
  `NothingCrossed::Rearranged` with its counts, and `--origin` at an indexed folder never applied
  from still answers "nothing crossed" — honest and informative there, the same evidence arm, and
  narrowing it is not this rule's business. Guard:
  `a_no_crossing_answer_names_the_counterpart_it_was_asked_about`.
- **The scope door is one spelling, at every door** (`cli::open_scope`). Resolution, the
  retired-place statement, the evidence gate at both entry points, the CWD-global fallback: `run`
  and `run_crossings` reach all of it through the one function, or a `cd` defeats the gate — and so
  does a second subcommand.
- **`--jsonl` is filter-only** — `jsonl::decision_json` is the one serializer both paths share, over
  the decisions carrying a crossing in view, each with its **full** row set. Nothing is added,
  nothing dropped, so the view-independence contract holds by construction
  (`crossings_jsonl_emits_unmodified_decision_events`). The reconciliation line is gated off under
  machine output for the same reason the card is: a present-tense number has no place in a
  view-independent stream.
- **Three invitations, one grammar, each taught once**
  (`render::{CROSSINGS_HINT, CARD_ORIGIN_HINT, CROSSINGS_DRILL_DOWN_PAYOFF}`). `crossings` appears
  on no output line otherwise, so the surfaces that invite it name it at the moment of need. The
  grammar: a backquoted literal command, then `to <plain verb> <payoff>`; "list" over "see"; no
  overexplaining; **fix the referent before adding words**. Every one **names what running it
  yields, never only the gesture**, because nothing in a flag's name says what is behind it
  (`every_invitation_names_its_payoff`). Placement, each for its own reason:
  - The **rollup hint** sits beneath the two *crossing* rollups and **above `Rearranged here`**
    (`rollup_block_lines`), so "these totals" points last at the one total the door has nothing to
    expand. A **rearrangement-only view goes untaught** — the door answers "nothing crossed" there,
    so the invitation would promise places it cannot list — joining the global and time-lens views
    the door already leaves untaught.
  - The **card's hint** prints beneath the origins block whenever a **shown** origin line names a
    path, not only on a capped card and never where there is no handle to take. The gate is
    evaluated over the origins the cap actually renders, because the reader can only substitute what
    they can see; `MultiOrigin` names no root, so a card whose visible origins are all multi-origin
    teaches nothing. **One invitation, never two**: a capped card's remainder absorbs the teacher and
    one branch decides, so stacking is unrepresentable
    (`a_capped_card_renders_one_invitation_not_two`). Its payoff carries the drill-down's referent,
    since the card's origin lines already name their decision and promising "the decisions" would
    offer what is on screen.
  - The **bare view's drill-down** prints once after the sections (`drill_down_hint`, an `Option` so
    "once per view, never per entry" is structural). The test is **per section, not per view**,
    since naming a counterpart opens one direction and leaves the other listing counterparts, so
    only a view whose every section is named goes untaught
    (`a_view_with_one_named_and_one_bare_section_still_teaches_the_bare_one`). **Its flag is derived
    from the sections rendered, never written down** (`entry_flag`, exhaustive over the aspects): an
    extraction section lists destinations so `--destination` opens them, an arrival section lists
    origins so `--origin` does. Naming the other flag invites the reader to substitute a listed path
    into the *inside*-end narrowing, which matches nothing and answers `Nothing has crossed between
    here and <that path>` — a **silent** dead end that reads as a finding.
  - **A hint's promise is a claim about another rendering, and prose claims go stale invisibly** —
    the anti-drift rule, and the reason this file records a mechanism rather than a spelling. So the
    payoff is **one word with a checked referent** (`CROSSINGS_DRILL_DOWN_PAYOFF`), pinned from both
    sides by `the_drill_down_hint_promises_only_what_naming_an_entry_adds`: the promised feature
    must be **absent** from a bare entry and **present** where an entry is named, and the promise
    must name it — which catches both ways a promise goes stale with nothing failing, the feature
    moving onto the listing and the promise being reworded past what the named view delivers. It
    does not make English self-verifying: it gives the sentence a referent a test can hold, and
    whether it reads well stays a human judgment. **Any hint that promises what another view shows
    takes this shape**; a hint carrying only a bare command name has no such claim to pin. The rule
    that the door never teaches **its own name** inside its own output is untouched — this teaches
    the *next step*, and that rule's reason, that a hint for a command you are already running is
    noise, does not reach a step the reader provably cannot find.
- **A decision count of exactly one names its decision** (`render::decision_chip`, one spelling at
  three sites: both bare-view directions, the grouped entries, and the card's origin lines). A list
  of one is not a list, so the count-over-list rule that banished id *lists* from the card has
  nothing to say here, and hiding the single id sends the reader through a door to learn a number
  the line already holds. Counts of two or more are unchanged, and `--jsonl` is unchanged. **The id
  rides the aggregation** — `CounterpartLine.decision_ids` and `OriginLine.decision_ids` carry ids
  where a count would have done, off a dedup already running for the date span, so no renderer looks
  a decision up. The **reconciliation line is deliberately not a fourth site**: its decisions clause
  is a comparison of two counts, not a handle.
- **A keyed entry has one shape, and both surfaces that name a place compose through it**
  (`render::keyed_entry_lines`, with `DOOR_INDENTS`/`CARD_INDENTS`). The path renders **full and
  unelided on its own line**: it is the key the reader copies from one invocation into the next, so
  `cap_path` is wrong here and a column-aligned one-line form cannot survive a long path. Its marker
  takes the next line through `render::origin_marker`, and marker and counts **indent alike**, both
  hanging off the path above them so a one-column difference cannot read as structure that is not
  there — one parameter rather than a discipline kept twice
  (`the_card_and_door_continuations_align_alike`). The **card's origin lines take this same shape**,
  uniformly, short live-root lines included, because a card whose line shape varies with content
  length scans worse than one that does not; the card's lines print under a two-space prefix of
  their own, so both surfaces land their continuations in the same column. **`MultiOrigin` stays one
  line** — it names no root, so there is no path to protect and no marker to hang beneath one — and
  `CARD_ORIGIN_CAP` counts **origins, not physical lines**, so an entry is never truncated
  mid-shape.
- **Every remainder this command prints has an invocation that opens it** (`render::place_cap`).
  `--all` uncaps the places listed beneath one delivery exactly as it uncaps the listing of entries
  above them; `--limit N` sizes that listing and not this one, so the places keep their own constant
  (`DREW_FROM_DIR_CAP`, shared with `drew_from_lines`). An unconditional cap prints a `… and N more
  places` line naming content no invocation can reach — this surface's own motivating defect, one
  level down, inside the surface built to answer it. Pinned by
  `all_uncaps_the_places_beneath_a_delivery`. **`trail show`'s `drew from:` caps identically and is
  not fixed here** — it belongs to its own change.

- **`render::origin_marker` is the one spelling of what an origin's line says about its root** — the
  book wins over `(root removed)`, a live root gets neither. Three surfaces render it:
  `drew_from_lines`, the card's origin lines, and `crossings`' counterparts; without one spelling
  they drift, one door pointing at the book while its neighbour, naming the same root, says only
  `(root removed)`. The card's book pointer is a **DB projection and makes no stat call**,
  deliberately unlike `show`'s `relocate_pointer`, which redirects and therefore must observe — the
  card states the recorded path and claims nothing about what stands at it
  (`the_card_states_a_book_path_it_never_observes`). Gated on removal, so the retirement lookup's
  own liveness gate keeps a bound-but-unreleased ceremony from marking a live root as bound
  history.
- **The trail states the recorded act, never the origin's present state**
  (`render::{OUTBOUND_DISPOSITION, INBOUND_DISPOSITION}`). Two consts, one spelling per disposition
  word per direction, consumed by both timeline voices and by `crossings`' deliveries. A line
  claiming an origin's files still stand there is unverifiable exactly where it is read most — on a
  retired root, a drive Canon can no longer observe — and past tense would have this surface
  narrating what became of the drive, which is the book's job. `None` renders nothing, never a guess
  (`the_disposition_words_have_one_spelling_per_direction`).

- **Per-line narration is the stored `summary`** (one composition, two uses — the same string the
  command printed); structured count columns drive rollups and JSONL.
- **Stamp aggregation splits by the presence axis** (`repo::aggregate_stamped_by_decisions`,
  `GROUP BY decision_id, present`): one scan decision stamps both New (present) and missing (absent)
  sources, and object exclusions stamp tombstones — so "deleted" reads only the absent bucket of
  Observe-family (scan) decisions. Rollup sizes are omitted when the stamp no longer supports them,
  never guessed.
- **`fate_transition(family, aspect)`** (`core::domain::fate`) is the one what-derivation — a
  content transition in registered vocabulary (`archived`/`excluded`/`restored`/`deleted`), keyed on
  `(family, aspect)` not command, because command alone can't yield `deleted` from `scan` (the
  presence aspect is the discriminant). One function, two consumers: the trail rollup labels its
  fate lines through it, and receipt `[meta]` stamps `transition` through it via `ReceiptKind`.
  `fate_posture` is its orthogonal `performed`/`observed` half (Observed iff a scan-observed
  deletion), stamped as receipt `posture`. Never emit a transition or posture word as a literal —
  derive it. The receipt↔trail agreement is enforced by the compiler and by test, the integrity test walking
  every `ReceiptKind`.
- **Two matching modes, two named predicates — the two-claims placement law** (`domain/placement.rs`): a
  **declared scope** (`decision_scopes` rows, note paths — "I acted on this subtree") matches
  **bidirectionally** via `scopes_touch`; an **observed placement** (an extraction row's
  origin/destination locations — "my files lie under here") matches **descendant-or-equal only** via
  `placement_in_view`. Never match a placement with `scopes_touch`: its ancestor branch is how a
  common-prefix destination manufactures arrivals at sibling folders. Prefix matching is domain
  logic — repo fetches scope rows by `root_id` only.
- **The view-match is computed once in the operations layer and carried; no surface re-derives which
  scope matched.** The join that decides *whether* a decision surfaces is the same join that says
  *which of its places* brought it here, and discarding it makes every surface guess again — which
  is how a 31-prefix scan came to be labelled in a leaf folder by its **first recorded prefix**, a
  place with nothing to do with the view. Owner: `ops::compute::build_scope_matches` →
  `domain::placement::ScopeMatch`, carried on `TrailResult.scope_matches` as a **side map**, never a
  mutation of `Decision.scope`, since the durable display column is what `--jsonl` serialises.
  Deepest match wins, ties lexicographic; matching is `scopes_touch`, because a `decision_scopes`
  row is a **declared** scope — never `placement_in_view`. `other_count` is read off the decision's
  own display column, so `+N` stays consistent with `show` and `--jsonl`. Three sites carry the
  discipline: `ops::compute::classify_extraction_rows`, `render::scope_cell` and
  `ops::show::ShowScope`; the cross-surface coherence test is
  `show_and_the_timeline_agree_on_what_matched`. **Where it does not apply**: a global view, and a
  decision surfaced by an *extraction row* alone, which has no `decision_scopes` row to match. Both
  fall back to `paths[0] +N`, and that fallback is behaviour, not a gap.
- **The timeline states an act, derived** (`domain::timeline::decision_act`): the registered
  transition word where `fate_transition(family, Present)` has one, the stored command identifier
  otherwise — the one what-derivation, same as the rollups and receipt `[meta]`, never a coined
  literal. The underscored identifiers the fallback returns (`scan`, `cluster_generate`) are
  **accepted residue**: coining an act word is domain vocabulary, and `DecisionFamily`'s own doc
  comment routes the taxonomy there. **Notes carry no act** — a thought is not an act; the column is
  held open for alignment and the `~` keeps marking the voice.

- **Places render in three descending frames** (`render::relativize`): view-relative when the view
  has one prefix (`.` = the viewed folder), then **root-relative** measured from
  `TrailResult.view_root` — the single root containing every prefix, derived in ops via
  `find_containing_root` because `ResolvedScope` carries prefixes but no root — then absolute, where
  no shared frame exists. A root-relative cell carries a leading `/`, and the legend naming the root
  prints **only when the listing actually contains one**: state the convention where the output uses
  it, say nothing where every place sits at or below the viewed folder. Whether it does is decided
  by rendering the cells a second time with no root and comparing — asked of the one function that
  makes the choice, never sniffed back out of a string that no longer records which arm produced
  it.
- **`-l`/`--long` is multi-line, not a wider column** (`render::long_event_lines`, the pure line
  builder; `print_long_event` prints it). An uncapped scope column pushes the narration off the
  right of the screen; a taller entry survives any path length. Places are **absolute, uncapped and
  unelided** — this mode exists to be copied out of — and structurally so, not by a branch: the
  builder takes neither a `ResolvedScope` nor a `view_root`, so no caller can render it any other
  way. Long mode changes only how an event renders — never which events are shown, their order, the
  rollups, or the card. `--jsonl` returns before the renderer, so `-l` is inert there.
- **`trail show` classifies its scopes against where the reader stands**
  (`ops::show::classify_scopes` → `ShowScope`/`ScopeRelation`; rendered by `cli::show_scope_lines`).
  One place per line, `Here` then `WithinHere` then the rest, **stable** within each group so
  recorded order survives, capped at the value `drew_from_lines` uses in the same file with an
  explicit remainder. **Hoisting is load-bearing, not cosmetic**: with a cap and no hoist the place
  the reader cares about falls into the truncated remainder. Matching is `scopes_touch` on
  **absolute display paths** — the same bidirectional predicate that surfaced the decision in the
  timeline, which is what makes the two surfaces agree
  (`show_and_the_timeline_agree_on_what_matched`); root identity is implied by an absolute path, so
  the root-id equality check `compute_trail` pairs it with is unnecessary here, not omitted. Never
  `placement_in_view`. CWD comes from the interface (`cli::run_show`), cleaned lexically and
  **never canonicalized**, and `None` yields recorded order with no markers.
- **`decision_family()`** (`core::domain::fate`) is a total mapping over the frozen
  `DecisionCommand` identifiers — when adding a decision command, add its family arm too (unknown
  identifiers land in `Unrecognized`, rendered raw, never dropped).
- **Global decisions** (no `decision_scopes` rows) surface in scoped views as a counted footer —
  absence is never silent.
- **Timeline lines carry a scope column** (decisions and notes alike), rendered in the three frames
  above and capped. Capping goes through `core::domain::format::cap_path` — the one shared
  path-capping helper (char-safe; also used by coverage and facts labels — never re-implement
  byte-sliced capping). `note_display_path` lives in `notes::domain` for `note list`.
- **One classification pass, per physical row** (`ops::compute::classify_extraction_rows`):
  `compute_trail` fetches all extraction rows once and tags each with its `RowAspect` from its own
  two endpoints — origin membership root-id-keyed over the view's decomposed roots (plus
  `placement_in_view` on the rel location), destination membership on **absolute snapshot paths**,
  so a removed or removed-and-re-added destination root can't break the link where a root-id join
  would. `TrailResult.placements` carries the tagged rows; the interface renders and never
  classifies. Filtering *decisions* rather than *rows* keeps or drops one decision's rows from
  several origins together; a by-`root_id` map pairing silently breaks once one decision has several
  rows per root.
- **A rollup counts boundary crossings, and the view defines the boundary** — the one rule behind
  aspect selection, the three rollups, and the card's origin lines.
  `domain::placement::row_aspect(origin_in_view, destination_in_view)` is its single derivation, a
  total four-cell truth table (`Extraction` / `Arrival` / `Rearrangement` = both inside, crossed
  nothing / `Outside` = neither, dropped at classification). **Centralize the rule, not the
  evidence**: each consumer supplies membership in the form it already holds —
  `classify_extraction_rows` with root ids + snapshot paths, the card by prefixes via
  `classify_row`. Both use `placement_in_view` for the membership test itself.
- **Timeline lines are display aggregates** (`domain::placement::aggregate_placement_lines`): a
  decision's matched rows collapse to one line per (origin root, aspect) — files summed, bytes
  all-or-omitted per line, locations collapsed to the *matched subset's* common prefix
  (`core::domain::path::common_path_prefix`, which takes directory paths whole — never
  `parent_dir`-style filename dropping). Both render passes (`event_cells`, `print_event`) aggregate
  through this one helper, so measured cells and printed lines can't diverge; because only matched
  rows aggregate, a line's counts are the view's counts and it can never name a location outside the
  view.
- **Three disjoint rollups over three row sets** (`ops::compute::build_rollups`): `Archived from
  here` (left), `Arrived here` (entered), `Rearranged here` (stayed). A rearrangement is in
  **neither** crossing rollup, since counting it in both reads as double the activity with both
  counterparty counts naming this place; cancelling it silently would be a silent gap, so it earns
  its own line, and that line carries **no counterparty count** by design. All three go through one
  `rollup_parts` builder so the all-or-omitted bytes rule has one home, evaluated **per rollup over
  its own rows** — an unknown-size crossing must not suppress a known rearrangement total. Same
  gating as siblings: whole-history, cap-independent, scope-lens only, `None` for global and
  time-lens views.
- **Scope-dependence is the rule working, not an inconsistency**: an apply from `/archive/2016` to
  `/archive/2020` reads as a rearrangement at `/archive` and as an arrival at `/archive/2020`. Never
  "reconcile" the two views.
- **The timeline renders a rearrangement once**, as the extraction-aspect line with its destination
  shown view-relative instead of absolute — never a duplicate line for one decision.
  `RowAspect::Outside` is unreachable in rendering (such rows are dropped at classification); the
  render arms fold it into the plain extraction shape rather than panicking on a line the interface
  cannot repair.

**Composition card conventions** (`domain/composition.rs`, `ops/composition.rs`) — the trail's present-tense complement, not part of the trail itself:

- **Every card line answers one question — *how did what stands here come to stand here* — in one
  grammar.** Origin lines read `arrived from <root>`, parallel with their sibling action labels
  (`excluded`, `rearranged`, `first indexed here`); an origin line carries a **count** of the
  decisions behind it, not their ids — except a count of exactly one, which names its decision (the
  singleton rule above) — with the date range retained. The ids may leave because the acts behind
  them are reachable: the timeline holds them directly above for the transitioned section, the
  origin door for the origins section. `MultiOrigin` is deliberately **not** relabelled — it names
  no root, and making it parallel would mean coining a wording for a line nothing asked about.
- **`arrival unrecorded` names an absence and no cause.** Those sources are tracked — indexed,
  present, counted in the header above; what is missing is the record of how they arrived, and the
  row cannot say why, predating recording being one of several indistinguishable reasons.
  Self-explaining gaps, never a guessed cause (`an_unrecorded_arrival_states_no_cause`).
- **A view-agnostic sibling op, never computed inside trail rendering.**
  `ops::composition::compute_composition()` answers "what is this place made of, right now?" — a
  distinct question from `ops::compute::compute_trail()`'s "what happened here?" — so a future
  surface can reuse it without depending on how `trail` displays it.
- **Bucket classification is via `decision_family()` on each present source's `decision_id` stamp**,
  the same derivation the trail rollups use. Archive → an origin line, with origin multiplicity
  counted over **distinct origin roots, never raw rows**, since directory precision gives one apply
  several rows per root and a single-drive delivery fanning across folders is one origin: a single
  root merges into a `FromRoot` line per origin root across decisions; several roots take their own
  `MultiOrigin` line, one per decision. Observe → the `indexed_here` bucket. Every other family → a
  `TransitionedLine::Standing` labeled with the registered transition word (`fate_transition`) or,
  absent one, the raw command name. `None` (no stamp at all) → `untracked`. An Archive-family stamp
  with no matching extraction row still becomes a `TransitionedLine::Gap` rather than being dropped
  or silently folded into `untracked` — self-explaining gaps, never silent.
- **The transitioned section states standings, not events** (`TransitionedLine`, an enum of two
  shapes). The card is a **state** statement, so a standing is *what stands here*: merged across
  every decision that produced it, keyed on its label, carrying **no decision id** — the acts behind
  it are the timeline's to hold, directly above the card. A **gap** is the exception and keeps its
  id, being about one specific decision, and it is never merged. Only the two named gap labels
  (`transition unrecorded`, `archived (origin unknown)`) take the Gap shape; `rearranged` and the
  raw-command fallback merge like any other standing. Ordering: standings first (files desc, label
  asc — a merged line has no id to tie-break with), then gaps (files desc, decision id asc). **Gaps
  are capped with an explicit remainder** (`render::CARD_GAP_CAP`), being the one part of the
  section whose length tracks the history rather than the vocabulary — per-decision *and* uncapped,
  the section out-shouts the capped timeline it is appended to. Standings need no cap by
  construction. Do not "fix" a gap into a merged standing, or the card stops naming the decision
  whose story is missing.
- **`CompositionCard::has_origin_story()`** is the one omission predicate — true iff there's at
  least one origin or transitioned line — and lives on the domain type so every future card surface
  applies the same rule. `compute_composition` returns `None` for a global scope, an
  empty-of-present-sources scope, or a no-story scope.
- **A place is not its own origin.** Before the origin-count match, an Archive-family stamp whose
  rows **all** classify `Rearrangement` (via `domain::placement::classify_row`, hence `build_card`
  taking `prefixes`) becomes a `TransitionedLine::Standing { label: "rearranged" }` instead of an
  origin line. Otherwise the card answers `from /archive` while standing at `/archive`, a non-answer
  in the line that should hold the answer, because extraction rows group by the *source root of the
  items*, which for an intra-archive apply is the archive root itself.
- **The card classifies per decision; the rollups classify per row.** Forced by stamp granularity,
  not an oversight: `sources.decision_id` names a *decision*, not a row, so for a multi-origin apply
  the card cannot know which surviving files came from which side. Hence **all rows inside →
  rearranged; any row outside → keep the origin line**. Carries a code comment saying so — it reads
  like a bug and will otherwise be "fixed" into incorrectness.
- **`OriginLine::FromRoot.from_within`** marks an origin root that *contains* the viewed scope,
  rendered `from elsewhere in <root>`. Still a genuine arrival — its origin is outside the view, so
  the rearrangement guard correctly doesn't catch it — and only the display would otherwise name the
  place the reader is standing in. Safe to hold on the merge accumulator rather than per row: the
  group key *is* `root_path` and `check_no_overlap` forbids nesting, so at most one root can contain
  a view and no group can be half-within. The root stays named (not "this root") because a view can
  span several roots.
- **Origin attribution is root-level, deliberately** — `FromRoot` keys and displays on `root_path`,
  dropping `rel_prefix`, which is the common prefix of the items *one apply happened to draw*, a
  property of the decision rather than the origin, where `FromRoot` merges across decisions. Keying
  on it would split one drive into several lines or need a cross-decision common ancestor that
  collapses back to the root anyway. Sub-root precision lives in `trail show <id>`'s `drew from:`
  and in receipts. Do not "fix" this into finer granularity without a design pass.
- **Sum invariant**: `files`/`bytes` on the card equal the sum across every bucket (origins +
  transitioned + indexed_here + untracked) — test-enforced, not just convention. Holds with a
  rearranged bucket present.
- **State, not events**: "Arrived here" (the arrival rollup) is an event total that never shrinks;
  "Standing here" (the card) is a state total that can honestly be smaller once some of what arrived
  is later deleted or moved on. Exclusion doesn't remove standing (an excluded-but-present source
  still counts); renaming doesn't erase attribution — the `decision_id` set/preserve rule already
  guarantees this, and the card re-derives nothing about a source's current path.

**Test co-location — the fixture-sharing criterion**: tests live in-file with the code they
exercise; test files externalize into a stratum `tests/` directory with a shared `fixtures.rs`
exactly when multiple files share test machinery — layer is never the criterion. Trail complies on
both sides. `domain/{timeline,placement,composition,place}.rs` and `render.rs`/`jsonl.rs` keep
self-contained in-file corpora: same-named builders across these files are signature-incompatible
variants rather than shareable fixtures, and `render.rs`'s builders are deliberate forks, per its
own comments. The ops split (`compute.rs`/`show.rs`/`place.rs`) externalizes to
`ops/tests/{fixtures,compute,show,place}.rs` because those files genuinely share a fixture family
(`insert_decision_at`, `insert_note_at`, `scope`, `params`, and others). `render.rs`'s test-section
banners are its declared internal seams — keep each section covering exactly what it names.
