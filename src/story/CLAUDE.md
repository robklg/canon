# story/ — the place walk: a root's resolution story as a map of places

Realizes the domain-language **Story** concept for `canon roots story` and, through the same
renderer, the book's `story.md`. The confirming step of the journey: the sweep finds, survey
judges a place, the story review confirms a root, the retirement ceremony binds it.

`domain/` is the pure lens: `splitter.rs` builds the tree of places, `acts.rs` groups what was
done, `locations.rs` answers *where*, `place.rs` holds the node, its standing and the once-rule.
`ops/report.rs` is the compute op; `ops/render.rs` composes the walk's text for both readings;
`cli.rs` parses, calls and prints.

Sealed behind a barrel of `story` (the command, reached by `main.rs`) plus what `retire` composes
the book through: `ActDecision`, `ActGroup`, `ReasonSummary`, `aggregate_locations`,
`LocationAggregate`, `LocationCount`, `PlaceStanding`, `StoryParams`, `StoryPlace`, `file_noun`,
`fmt_locations`, `reference_place_lines`, `report_over`, `StoryReport`.

## The walk

**The third lens over `RootStory`**: `compute_story` = `fetch_root_story` + the compile's two
enrichments (notes; `batch_find_archive_paths` over *non-zero-byte* present objects — the book's
contentless gate reused) + the pure lens (`build_places` + `build_account`). Never a second fetch:
the readiness gate, the book and the story read one world by construction, so no two of them can
disagree about a root (`report_over_is_compute_story_minus_the_fetch`).

**Query path: DB projections only.** `collect_origins` (receipt reads) is the bind-time compile's
sanctioned recovery and stays out of bounds here — the Provenance Consumption Readiness ADR's rule
that a query path reads the index, never the durable records.

**Slices, never scope claims.** Acts attribute per (decision, directory): archived acts at
extraction rows' origin dirs, stamp acts at stamped sources' dirs (tombstone dirs included — the
act register is whole-history; do not "fix" tombstone slices away), observed deletions at the
absent rows' dirs (the scan observed; the user did not act there). `StoryInputs` carries no
operated scopes at all. A decision spanning places renders as slices — partial counts, same id, no
slice claiming the total; `merge_slices` runs before `group_acts` so each decision is one slice per
place. **Slice-sum law**: per decision, distributed slices reconcile exactly to stamp/row totals
(`slice_sum_law_reconciles_through_any_fold`). Apply stamps the *destination* rows, never this
root's — Archive-family stamps are skipped, or the same files count twice; an unknown stamp
narrates no act, its content reads undecided, never guessed.

**The splitter** (`build_places`) is the sweep's emission discipline. The root, noted nodes and
**care anchors** always emit — a *reasoned* decision forces exactly one node, the LCA of the dirs
it touched (care at the decision's grain; reasonless decisions force nothing; recorded care earns
a line, floors notwithstanding). Otherwise a node emits when its judgment signature diverges from
its nearest emitted ancestor. Dust lifts only when present weight AND act weight both sit under
the floors — an emptied place has zero present files but a real story. **Pockets surface**: a
merged node's descendants keep walking against the same context. `StoryParams` names the
constants; recalibration changes constants, never code paths. Two guardrails on the defaults: 0.20
tolerance eats a worked root's honest pockets, and the 5 MB dust-bytes floor is what protects small
photo pockets.

**Excluded is never a question, in standing or in acts** — the emission rule behind every
divergence axis. Standing divergence compares proportions within the question population only
(archived+covered as one merged axis — the split is a reading distinction, not a boundary — plus
unresolved/missing/deleted and a question-density term; contentless is outside the question
population like excluded; a question-empty child never splits on standing). Act divergence compares
proportional act signatures — `(transition, posture, decision-level destination answer)`,
destination computed once per decision over all its rows so a mirrored-destination apply cannot
fragment — over **residuals** (subtree minus forced descendants: what a folding child actually
merges into), gated on narratable (non-exclusion) act weight, all under the one
`signature_tolerance`. The why is deliberately not an axis: a reasoned act surfaces through its
care anchor, and reasons in the comparison fragment interleaved scattered sweeps.

**Acted/undecided is structural**: a deciding stamp exists or it does not; intent is never
inferred. The discriminant is the account's own classifiers (`classify_present`/`classify_absent`),
which makes the **agreement law** — place standing sums fold exactly to `build_account`'s buckets —
hold by construction rather than by care (`agreement_law_place_sums_fold_to_the_account`).

**One where-derivation** (`aggregate_locations`): fewest legible prefixes over the bases (known
root paths), legible = strictly below a base; a vacuous common node ("in the archive" answers
nothing) splits into chain-collapsed branch groups, weight-ordered, capped with a counted
remainder. Covered-where uses `aggregate_locations_expanded` (one-step hub descent when the branch
groups fit the cap whole); destinations keep the coarse answer — the arrow states a choice, the
scatter is what nobody chose. **Two wordings at the interface, never mixed**: `→` = recorded
destination (the user chose it); `copies stand in` = observed coverage (nobody chose it).

**Act aggregation** (`group_acts`): by (transition, posture, destination aggregate) — the what
compresses, the whys never disappear (`reason_summary` enumerates distinct reasons with ids, cited
ids on one bare line, reasonless counted, buckets never conflated), the where never blurs. Bytes
and the moved/copied split are all-or-omitted per group. Groups order by earliest decision;
decisions oldest-first within.

**The once-rules** (`assign_reason_sites`, `standing_coincides`): a reasoned decision's full reason
renders at its **first emitted slice in pre-order** — render order, and any wider site would put
the reason after a slice already citing it, leaving the reader a forward reference. Every other
slice cites the bare id. A post-pass over the built tree (fold composes first; `reason_here`
defaults `true`, so direct `group_acts` consumers keep full reasons and only `compute_story`
narrows). The excluded standing line is omitted only on exact coincidence — every other bucket
zero and the excluded standing equal to the excluded performed acts' present share *and*
whole-history count; a tombstone-carrying slice fails that test, because omitting there would
misread as all still standing. Covered/unresolved/missing lines are never omitted.

**A note forces a place, never classifies** — testimony beside the standings, not a decision stamp.
A source whose path exactly matches a noted place gets its own node (a note on a file gathers that
file's fate); a note-forced leaf with nothing standing says `nothing stands here now`. Honesty
wordings gate on evidence: `no decision here` renders only over question content; stampless
excluded standing renders `(no recorded decision)` — exclusion is always deliberate.

**`FolderTree` is not here**: it lives in `core/domain/folder_tree.rs`, shared substrate with
consumers beyond story (see `src/core/CLAUDE.md`). Topology only; consumers hold payloads in
side-vectors by folder id. The three boundary walks — survey's `discover_scopes`, the sweep's
containment walk, the story's signature walk — stay separate on purpose: the emission rules *are*
the features, and only the skeleton is shared.

## Voicing

**One composer, two voicings**: `ops/render.rs`'s `Voicing { Judgment, Reference }` threads
through the single place-walk (`cli.rs` takes the judgment reading, retire's frame the reference
one) — wordings differ *by voicing* at match sites, never by consumer accident; structure
(places, slices, once-rules, aggregates) is computed once. Judgment output is byte-pinned
(`judgment_golden_fixture_is_line_identical`).

**Reference is the ever-axis**: what was ever here, told by where it went — content-first fate
lines (`N files, S · chosen for the archive → dest`), plain diction (*let go* = excluded,
*preserved by copies in the archive* = covered, *no known copy in the archive* = unresolved), no
bind-time claims, no standing vocabulary, no handoffs, no moved/copied split (the dossier keeps
it). The archived-standing line, `no decision here` and `nothing stands here now` are judgment
furniture — **dropped, never re-worded**: "still standing here" is exactly the bind-time claim the
ever-axis forswears. Always full: the place cap is structurally unreachable in a book.

**The no-record marker is row-grain**, never place-grain. `PlaceStanding.excluded_stampless` counts
excluded rows with no decision stamp, and the marker renders from that count in both voicings — all
stampless keeps the base wordings, partial renders "for N of these", zero renders none. A
place-level gate is wrong in both directions: a place mixing stampless and stamped rows earns no
marker at all, and a place whose rows are stamped but which records no act earns a false one.
Accepted residual: a bare standing line from pure count mismatch (tombstone slices) carries no
invented wording — the inventory resolves it per file.

**The interface classifies nothing**: rendering is `story_lines`, pure and unit-tested —
containment indentation, the two wordings, counted omissions that never drop the standing close.
Handoffs go through `trail_handoff` under the sweep's handoff round-trip law; the readiness review points
at the story on both verdict paths (`story_pointer`, in retire). **Handoffs must answer, not just
parse**: the handoff round-trip law covers parseability, the trail's history-scope fallback (see the trail
conventions) covers answerability — a story handoff points at emptied places by design (the
best-resolved places are the emptiest), so the trail must never 404 them.
