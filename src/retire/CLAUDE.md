# retire/ — closing the book on a fully resolved root

Realizes the domain-language **Retirement** and **Book**: a resolved root's complete story
compiled into the book, placed on the shelf, its index removed. Two movements — the **bind**
(compile, verify, place) and the **release** (index removed) — with the user's inspection of the
standing book between them.

`domain/readiness.rs` holds the verdict and `domain/book.rs` the fate model; in `ops/`,
`review.rs` is the readiness review, `compile.rs` the book compile, `verify.rs` the reader that
proves a book whole, `ceremony.rs` the two-movement state object, `shelf.rs` the fleet listing and
`frame.rs` the bound telling's frame; `repo.rs` is the subsystem's own SQL and `cli.rs` the
interface (`canon roots retire`/`retired`). Sealed behind a barrel of `retire`, `retired`,
`find_retirement_covering_path` and `RetiredScope`.

## The review and the account

- **One fetch, two lenses, structural**: `fetch_root_story` → `RootStory` (present + absent rows,
  archived set, extractions, scope rows, deduped decisions, stamp families, the full fleet,
  fetch-time `reachable`/`max_decision_id`); `readiness_lens` and `compile_book` are both lenses
  over it, so the gate and the book read the same world by construction rather than by two fetches
  that happen to agree. `compute_readiness` = fetch + lens. The review-time basis
  (`snapshot_source_count`, `snapshot_max_decision_id`) feeds the release's world-moved re-check.
- **Retirement's own SQL lives in `repo.rs`** — `fetch_bound_retirements`/`BoundRetirementRow`,
  read by the shelf listing and by `find_retirement_covering_path` (the retired-scope statement the
  trail renders at a retired root's old path), and the world-moved pair
  `count_all_by_root`/`max_decision_id_touching_root`. **Not** the readiness review, which reads
  `fetch_root_story` like every other lens. No other subsystem asks these questions, so they sit
  here rather than in the shared repository layer; the structural fetch stays shared, because
  three lenses read it — the readiness review, the book compile and the story report. Reached as
  `crate::retire::repo::…`, spelled in full so the layer scanner reads it as this subsystem's own
  stratum rather than a bare reach at the shared one.
- **The asymmetric verdict is a type**: `Readiness` has `NotReady`/`NoBlockersFound` and
  deliberately **no `Ready` variant** — Canon can know NOT READY, never certifies ready.
  `Readiness::blocks(allow_unresolved)` takes only the acknowledgment, never `--yes`, so the
  yes/allow orthogonality is enforced by the signature. Only unresolved blocks; gap facts inform, never block.
  Unhashed + present + non-excluded counts as unresolved (it cannot be verified covered) — except
  empty sources, which are contentless, never unresolved.
- **The account has two registers, never reconciled** (the trail's event-vs-state discipline):
  *the story so far* (whole-history events: `archived from here` = origin extraction rows **all
  dispositions**, the trail rollup's established meaning, moved/copied split shown; `deleted` =
  absent + Observe-family stamp, the presence-axis rule; `unexplained missing` = absent + any
  other or no stamp, a record-quality fact) and *standing here now* (present rows partitioned
  **excluded > contentless > archived > covered > unresolved**, the archived/covered split resting
  on extraction-linked evidence). Copies overlap the registers deliberately.
- **Sum invariants are derived methods, test-enforced**: `standing() = covered + excluded +
  unresolved`; `ever_indexed() = standing + deleted + unexplained + moved` — `None` when any
  extraction row lacks a disposition (the moved count is unsupported; omitted, never guessed).
  Bytes are all-or-omitted.
- **A review records no decision** (read-only; recording gates on effectful actions).
  Ceremony-entry policy lives in ops (`validate_retire_target`): an archive-role target is
  refused, and no shelf to bind to is a hard error — the book would have nowhere to stand. The
  refusal states **which** absence it met (`LedgerRootOutcome`): with no archive root registered
  the bookless removal stays `roots rm`; with every archive root **suspended** the shelf and its
  books stand where they stood, so the way back is `canon roots unsuspend` and only that, and the
  destructive door is not offered
  (`validate_names_a_parked_archive_fleet_and_offers_only_unsuspend`). `plan_bind` speaks the
  same two causes as the backstop for a root parked between gate and bind, and the parked
  sentence is spelled once for both doors (`ops::parked_shelf_refusal`). The *target* is the
  other way round: suspended or unreachable, it retires on faith — surfaced, never refused.

## The book

- **Receipt reading is entry recovery, not enrichment**: move-mode applies relocate the origin row
  to the destination root, leaving no tombstone, so "every source the root ever had" is
  unanswerable from `sources` alone. The compile recovers moved-away entries per item through
  `core::ops::ledger::read_apply_receipt`, the one shared reader; an unreadable receipt degrades
  those entries to `covered` with the gap recorded — never guessed. Items naming another
  `source_root` are the other side of a multi-origin apply: skipped, not a gap. The
  **aggregate-only law is scoped to `decision_extractions`** (the index writer) and does not bar a
  sanctioned reader from per-item fields.
- **The fate model consumes the account's classifiers** (`SourceFate` via
  `classify_present`/`classify_absent`) so the inventory and the account cannot drift — the
  agreement law (fates fold to the account's buckets) is a test. Terminal fate words derive
  through `fate_transition()`; *covered*/*present*/*missing_unexplained* are **standings, not
  transitions** — named `STANDING_*` constants, the never-literal law's correct scope. The law
  is carried by `the_book_never_spells_a_fate_word_as_a_literal`, which reads the vocabulary out
  of its owner and refuses those words as quoted literals anywhere in this subsystem's
  production code: a literal that is currently correct reads identically to a derivation at run
  time, so only a spelling check sees it. It does **not** reach the standings beside them, which
  have no derivation to come from.
  `ArchivedFromHere` carries `Option<OriginDisposition>` (pre-vocabulary receipts omit
  moved/copied rather than guess; reader-facing words via `disposition_word`).
  `BookEntry::verification()` is a method from hash presence, so an unhashed entry can
  structurally never render content-verified.
- **The compile writes the README last** — a partial book is then self-evident: no README, no
  complete book. It refuses an existing target directory (collision explicit, never overwritten —
  the ceremony compiles to a temp name and renames at placement) and separates errors from gaps:
  an I/O failure stops the compile, while an unreachable ledger or an unreadable receipt is a
  recorded gap. The gather copies `.canon-ledger/` verbatim via `core::ops::fs::copy_tree` (mtime
  and permissions preserved; filenames preserved so `previous_decision_id` chains stay walkable).
  **Machine-readable** dates are ISO-8601 UTC with the `Z` that says so (`iso_utc` — `meta.toml`'s
  stamps, the inventory's `mtime`); **prose** dates and the book's own directory name are the
  binder's local day (`iso_date`), the day the story inside and the trail's own filing both use —
  without the split, a book bound in the evening dates its README a day after its story. Hashes
  are `sha256:`-prefixed: Canon-independence is the format's point. `BookMeta` declares its values
  before its sub-tables, matching the order TOML writes them in.
- **`verify_book` is production code, not a test helper** — the release movement's hinge: it
  parses `meta.toml` back, stream-recounts the inventory per fate (the writer's own word
  derivations), requires every claimed artifact, and cross-checks the gathered-ledger file count.
  The **round-trip check** — compile → verify → counts equal the DB-derived account — is
  test-enforced, scale-tested past the SQL chunking boundary.
- **The book format is a public contract** (`docs/src/reference/book-format.md`, `version = 1` in
  `meta.toml`): fields may be added within a version, meanings never change; verification refuses
  newer versions, and readability never depends on Canon.
- **What the book's own pages may claim**: identity says **`first indexed`** (min `scanned_at`
  over present + absent rows — row evidence stays honest on roots older than decision recording,
  and the first *recorded* scan is the timeline's opening line, never an identity claim); the
  timeline **excludes the ceremony's own in-flight decision** (`CompileParams.ceremony_decision_id`
  — kept out of the timeline because it cannot narrate itself, prior attempts still rendering, and
  stamped as `meta.toml`'s `identity.decision_id` so the index reference is readable from the book
  alone; absent outside a ceremony or under `Off`, omitted never guessed) and indents multi-line
  summaries and reasons; notes render via `note_display_path` (root-relative, `(root)`, never a
  view-relative `.`); **zero-byte sources are contentless** (fate word `contentless`; they carry no
  location lists, which the archived-ness SQL enforces — the contentless law, suite-wide); inventory lines carry
  **`decision`**, the fate-determining decision (`archived` → the apply via
  `ApplyOrigin.decision_id`, else the stamp; omitted when absent, never guessed).

## The ceremony

- **The ceremony is an ops-owned state object** (`RetireCeremony`: `plan_bind` → `begin_ceremony`
  → `bind` → `release`/`abandon`/`interrupt`): one decision spans both movements with the
  interface's confirmations between, and ops owns ordering, recording and verification gating —
  the interface only prompts and prints. `begin` starts the two-phase decision after confirm #1 (a
  declined prompt records nothing); a failed bind completes `interrupted` via `interrupt()`
  (fix-forward, findable); a declined release goes to `abandon` (`partial`, bound-not-released); a
  moved world gives `partial`, root intact, "re-run" — a ceremony outcome, not an error.
- **Every *returning* exit after the bind settles the row, structurally rather than by a list**:
  `release` is a wrapper whose single `Err` arm settles, so every `?` in its private
  `perform_release` body — the immediate transaction, the world-moved probe, the removal, the
  commit — passes through it, later additions included. (*Returning* is the honest word: Ctrl-C in
  the inspection window lands on findable-`started`, that word's reserved meaning, not a gap.)
  **The reaching is structural; the sentence is positional** — the arm is reached from any body,
  but what it records is true only while every `?` sits before `tx.commit()`: past the commit the
  root is gone, and "the root remains in the index" would be false in the durable record. The note
  at the commit line is that guard, and it is prose, not a build refusal. The settlement is written
  on the **connection**, after the transaction has dropped; written inside it, it would roll back
  with it and look like a fix while being none. On a busy database the settling `UPDATE` can itself
  fail — the recorder then warns and honestly leaves the claim registered.
- **The word is positional, not causal**: `interrupted` before a book exists (`interrupt` speaks
  for a bind that produced none), `partial` once it stands with the root intact — every exit after
  the bind, since they all leave the same standing, which is why the prompt-failure method is named
  for `abandon`. That site is the one ops cannot reach: a second confirmation whose prompt itself
  fails is caught in `cli.rs` and handed straight back (`abandon_on_prompt_failure`), so the
  interface chooses neither the status nor the words.
- **The bound-not-released sentence has one home** — `unreleased_summary`, which every such exit
  routes through (release declined, prompt failed, release failed, world moved), differing only in
  the parenthetical naming what happened. A further exit adds a parenthetical, never another
  spelling — a convention held by review, not by the build:
  `every_unreleased_exit_speaks_one_standing` enumerates today's arms by hand, so it would not see
  a new one that spelled its own.
- **Bind order is load-bearing**: shelf + README (written once) → clear leftover temp (a
  temp-named dir was never placed; the rename is what commits a book) → compile into
  `retired/.compiling-<name>` (same filesystem, so the rename is atomic) → **`verify_book` on the
  temp** (a standing book is never touched until the fresh one is proven whole; a failed verify
  keeps the temp for inspection) → place (plain rename, or the swap: old aside → new in → old
  removed) → **pointer written while the decision is still `started`**, so abort-after-bind and
  crash-during-inspection stay findable. Never move the pointer into release.
- **Collision/convergence is disk-keyed, deliberately**: `plan_bind` reads the standing book's own
  `meta.toml` identity — same root → replace (same-day re-runs converge; a later-date
  re-retirement stands beside the earlier book as its own telling, the fleet listing both
  honestly); different root → numbered sibling; unidentifiable → refuse. Never "simplify" into a
  decision-row lookup: disk-keying is what makes convergence survive any recording-mode history,
  and an Off-mode bind leaves no row.
- **The artifact pointer gates on `record_enabled`, not `receipt_enabled`** (documented
  divergence): retire writes no receipt file — the receipt columns reference the book, which
  exists independent of receipt settings, so `--no-receipt` never suppresses it. Under `Off` the
  ceremony proceeds — book bound, root released, nothing indexed — surfaced as an awareness line
  at confirm #1, never refused.
- **The world-moved re-check excludes the ceremony's own decision id**
  (`max_decision_id_touching_root`'s `exclude`): `begin` inserted it with a scope row for this
  root, so without the exclusion the check always trips over itself, while a concurrent process's
  id differs and correctly trips it. Two aggregates — `count_all_by_root`, and the max over the
  three tables that reference a decision (`sources`, `decision_scopes`, `decision_extractions`) —
  are computed to provably equal what `readiness_lens` derived, so equality means "same world".
  `release` runs `BEGIN IMMEDIATE`: the re-check's reads must be authoritative, and the
  transaction is short. Known blind spot, accepted: under a whole-home `recording = Off` a
  concurrent count-preserving mutation leaves no decision rows for the max-id check to trip on —
  the Off-mode awareness line is the mitigation; do not rediscover this as a bug.
- **The shelf listing is a union, disk-primary** (`compute_shelf_listing`, `ShelfLine`): books on
  the shelf are the primary lines, identified by a lenient meta probe (`ListingProbe` —
  identification, never verification, deliberately no version gate) and enriched from decision rows
  through **one join key**: the decision's recorded artifact reference (`receipt_rel_path` basename
  = book dir name), uniform over every bound book, so the pre-`decision_id` book needs no special
  case. What either side knows alone renders marked, never dropped (`RecordedOnly`,
  `indexed: false`, `Unidentified` — an unidentifiable shelf directory is counted); an unreachable
  shelf falls back to decision rows, hedged. `.compiling-*` temps are not fleet; replaced re-runs
  dedup to the newest row per recorded location.

## The bound telling

`ops/frame.rs` (`TellingFrame`, `compose_reference_telling`, `finalize_telling`), consumed by the
compile and the ceremony, is the book's `story.md`. The place-walk composition itself lives in
`story/ops/render.rs` and is reached here through the reference voicing (`Voicing`, `story_lines`,
`reference_place_lines`).

- **The frame states pastness once** (diction, not frame — the v2 rule: ceremony in the structure,
  sentences plain): beginning → foreword placeholder → entries guide (fate words defined at first
  use, `restored`/"returned to consideration" included; the `#N` → `timeline.md` →
  `inventory.jsonl` trace chain; the traceability claim as its own paragraph) → full map → tally
  (zero buckets omitted; grand total dropped when `ever_indexed()` is unsupported; "It never held
  a file" when empty; the **copied-overlap sentence** when `archived_copied − archived_standing >
  0` — the registers overlap deliberately, so the exact excess over the header is stated, gated on
  `archived_unrecorded == 0`, omitted never guessed) → gaps paragraph (only when real, never a "no
  gaps" ceremony) → last page → closing stamp ("one telling of the record", its facts list
  dropping `ledger/` when nothing was gathered). Composed from records only; the human shape
  (title prose, root shape, foreword) arrives via the edit pass or stays honestly absent.
- **The which-ledger law**: the trace-chain paragraph names where each kind of receipt lives —
  apply and exclusion receipts in **the archive's own ledger** (`.canon-ledger/` at the archive
  root, named explicitly, never a bare "the ledger"), the book's `ledger/` holding only what lived
  on the drive (deletion receipts). The drive-local sentence renders in three states from
  `TellingFrame.drive_ledger` (gathered / empty / unreachable), computed on the *same* reachability
  discriminant the bind's gather reads. An empty gather leaves **no `ledger/` directory** (the
  compile removes the empty dest) and the README states the plain absence — the book never lists
  what doesn't exist. Books point at the archive's ledger and never gather copies (rejected:
  cross-root object-exclusion ownership; the story's own pages carry every decision in full).
- **The edit pass** (manifest precedent: composed declaration → human refinement → binding): after
  confirm #1 the ceremony offers the draft once to `$VISUAL`/`$EDITOR` (`cli.rs::prepare_telling`,
  `ceremony.rs::ask`/`edit_in_editor` — run through `sh`, so editor values with arguments work);
  `--yes` never prompts. `finalize_telling` drops the untouched foreword section by **exact match
  only** (one derivation site: `foreword_placeholder_section()` builds from the one
  `FOREWORD_SENTINEL` literal) and refuses an empty telling. `hand_edited` = the finalized text
  differs from the finalized composed draft — an honesty claim about the words, not a keystroke.
  Editor failure never aborts the ceremony: no new ceremony state, the choice re-opens, and Ctrl-C
  lands on findable-`started`.
- **The telling is the human artifact; the dossier is the machine truth**: `RetireCeremony::bind`
  *requires* a `TellingArtifact`, so a ceremony structurally cannot bind a dossier, and composes
  via `compose_telling` over the ceremony's own `RootStory` (`crate::story::report_over` — the
  one-fetch law; a post-`begin` *source* write cannot leak in, test-pinned). The two compose-time
  live reads, notes and the covered-where location lists, are the deliberate exception, same tier
  as the compile's documented enrichments: counts and classification stay snapshot-derived. The
  compile writes `story.md` before `meta.toml` (README still last); `[story]` claims the file,
  `hand_edited` and the reading settings (the version-stamp; additive within format v1);
  `verify_book` requires the claimed file non-empty and **never recounts prose**. Pre-telling books
  verify unchanged. The README is the front door ("Start with story.md") and carries the Canon-word
  mapping — the telling defines its terms at first use and carries no glossary.
