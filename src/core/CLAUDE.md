# core/ — the cross-cutting spine

The capability inventory of the shared ground. Before writing a helper any subsystem might
need, ask whether core already has something close.

The shared selector is the one thing a reader expects here and will not find: it is the
expression language applied within a scope, so its home is the language's rather than the
spine's, and it lives in the expression facility.

## The identity

**Subsystems tell; the spine warrants.** Core is not a subsystem — it is the ground every
subsystem stands on, and its identity is a small set of verbs: it **names** the world,
**warrants** the claims the user acts on, **records** every effectful act, **remembers
rebuildably and proves it**, **lends the hands** that touch disk, **conjugates** every verb
through one grammar, and **defends its own laws**.

**The bidirectional law.** Core never tells — no narration, no voice; its outputs are facts
and typed results, and the voice belongs to the tellers. And tellers never warrant — a
subsystem that re-derives a claim core already makes has made a second claim, and two
mechanics for one claim are a safety defect waiting to disagree with themselves. Every
boundary question here resolves against this law first.

**The warrant-limit pairing.** Every warrant ships with the law of what it does *not* say; a
warrant without its limit law is a false-claim generator. Wherever the spine states a claim
mechanic, the mechanic's limit is stated and enforced beside it — the pairing, not the
positive half alone, is what gets reviewed when either changes.

| Warrant | Positive mechanic | Limit law, and where it is enforced |
|---|---|---|
| **Identity** | the hash; one content, one object — `ops::fs::compute_partial_hash`/`compute_full_hash`, `domain::object` | **contentless** — identity claims about empty content are vacuous. `domain::source::is_contentless` is the one place `size == 0` is written; the SQL projections in `repo::object` carry it into every archived-ness query |
| **Standing** | the two-register resolution account — `domain::resolution::{classify_present, classify_absent, build_account}` | **the asymmetric verdict** — unresolved is provable, ready never is. Coverage is a containment fact: precise about content, silent about shape and worth |
| **Happening** | decision, receipt, transition — `ops::decision`, `ops::receipt`, `domain::fate` | **the posture law** — an observation must never read as an act. `domain::fate::fate_posture` is the one derivation, keyed on `(family, aspect)` |
| **Place** | path and scope mechanics — `domain::path`, `domain::scope`, `ops::scope` | **exactness at the edge** — a place claim is a boundary claim, and nearly-under is not under. `domain::path::path_is_under` is where that boundary is decided — `/a/bc` is not under `/a/b` — and `Source::matches_scope` asks it rather than deciding again; `repo::db::path_at_or_under_sql`/`path_strictly_under_sql` are its registered SQL projections, the one sanctioned rel_path boundary spelling |

## What's here, by verb

### It names — `domain::{source, root, object, fact}`

`Source`, `Root`, `RootSpec`, `Object`, `FactEntry`/`FactType`/`FactValue`, and the predicates
that read them (`is_excluded`, `is_contentless`, `matches_scope`, `find_containing_root`).
Every subsystem speaks these; nothing here knows about any subsystem. Beside them the pure
shape the nouns need to be handled at all — `domain::{path, scope, format, include, config}`:
path manipulation and offline resolution, `ScopeMatch` and `DecisionScope`, display
formatting, the visibility set, ledger-config parsing. Same purity law throughout: no I/O, so
path matching and scope resolution are testable with known inputs.

These are here because the Feature-First Structure ADR names them as the spine, not because
two consumers were counted. The membership criteria below apply to everything else.

### It warrants — `domain::{resolution, fate, extraction}`

- **`domain::resolution`** — `ResolutionAccount`, `StandingBucket`, `AbsentBucket`,
  `classify_present`, `classify_absent`, `build_account`, `unresolved_remainder`. Shared
  substrate `retire`'s readiness gate, `story`'s place lens and `sweep`'s root-nearness term
  each independently compute over; no subsystem owns it, which is exactly why it is here.
  `retire`'s own verdict (`Readiness`, derived *from* this account) and its book-compile fate
  model stay in `retire/domain.rs`: one consumer, so they do not qualify.
  - **The remainder projection** (`unresolved_remainder`) is the readiness review's own
    measure of what is left on a root, computed where the archived-from-here evidence is
    *not* on hand. It routes through `classify_present`, so this is **one law with two call
    shapes, not two laws**: what "unresolved" means is still spoken exactly once, and the
    contentless law is inherited rather than restated — empties bucket as `Contentless`
    before any identity test, so a root holding only empty files reads zero. Its soundness is
    one statable fact: `archived_from_here` only ever splits `Archived` from `Covered`, and
    neither is unresolved, so no value of it can move the count — resting on
    `archived_from_here ⊆ archived`, which both SQL projections guarantee by construction.
    That fact **is** the pin (`archived_from_here_never_moves_the_unresolved_count`), checked
    over every subset of the archived set. Bucket 0 of the sweep's nearness
    scale is exactly `Readiness::NoBlockersFound`, pinned in two halves across the `retire`
    seal because neither side can see the other's.
- **`domain::fate`** — `DecisionFamily`/`decision_family`/`Transition`/`FateAspect`/
  `fate_transition`/`Posture`/`fate_posture`. The one what/posture derivation, keyed on
  `(family, aspect)`. Shared between the trail rollup (which has the presence axis) and
  receipt writing (which supplies the aspect by receipt kind); neither owns it. The posture
  half is the *happening* warrant's limit law, which is why it sits with the transition
  vocabulary rather than in the recorder.
- **`domain::extraction`** — `OriginDisposition`/`DecisionExtraction`/`ExtractionItem`/
  `build_extraction_rows`. Shared between apply's forward recording and `ledger reindex`'s
  backfill. The round-trip law — a backfilled row structurally indistinguishable from a
  forward-recorded one — holds because both call the same aggregation function, not because
  two implementations agree.

### It records — `ops::{decision, receipt}`

The provenance spine, as a unit: receipt placement, path computation, the derived `[meta]`
table and the generic writer that puts any body on disk; and the two-phase recorder that
brackets every effectful action. Named spine by the Feature-First Structure ADR, not counted
into by consumers.

`decision` depends on `receipt`, never the reverse — `receipt` stays decision-agnostic.
Neither owns a receipt *body*: the writer is generic and never inspects what it serializes,
which is what lets each command define its own document shape in its own subsystem.

**Every claim the recorder registers prospectively is settled at the decision's last act.**
`start()` writes rows and columns describing a run that has not finished yet, so the recorder
— the party that registered them — is the party that settles them. Two conjugations of the
one shape, tracked side by side on the recorder:

- **The receipt claim** (`ReceiptClaim`, `settle_receipt_claim`) — the reference projection,
  recorded in the root file.
- **The status claim** (`StatusClaim`, `DecisionRecorder::refuse`) — `started` is written
  before anything happens. Settling it means writing a terminal: confirmed (`completed`),
  corrected (`partial`/`interrupted`), or **retracted** (`refused` — the run declined to do
  anything, so the counts stay empty and the receipt claim retracts in the same act). What
  survives on a row that reads `started` is therefore exactly one thing: a run that never
  reached a last act at all — killed, crashed, power lost. That is the recovery signal the
  provenance model and the user docs both promise, and it is only true because every other
  exit settles.

**Which word an outcome deserves stays the caller's**, legitimately: the caller is what knows
its own results, and funnelling scan's uncounted interruption, retirement's snapshotted
decline and apply's counted partial through one `interrupt()` would lose what each carries.
What the recorder owns is *that* the claim is settled, plus the one outcome carrying no
caller-specific information — the refusal, whose shape is always the same. The other sites
are **declared projections**, not eliminated: `scan/cli.rs` and `retire/ops/ceremony.rs` each
carry a test declaring itself as one.

**Where it does not apply.** A recorder that never registered claims nothing — recording off,
or the INSERT failed (`Unclaimed`, and `refuse` is a no-op). A row rolled back inside its own
transaction never existed, so `exclude` and `notes` are immune by construction rather than
conforming. And a settlement the database refused is not a settlement: the claim stays
registered, because the row really is still `started` and the warning already pushed is the
record of why.

**Still open — the general `?` between `start()` and `complete()`.** A propagating error at
`cluster generate`/`refresh`, `roots rm`/`suspend`/`unsuspend`, `facts delete` and the prunes,
or `import-facts` still leaves `started`. Closing each window is per-command product judgment
(*which* status each failure deserves), not recorder work.

**Retirement's release is closed**, and is the reference projection for the rest: `release` is
a wrapper whose single `Err` arm settles on the connection, so its four internal `?` — the
immediate transaction, the world-moved probe, the removal, the commit — settle by
construction rather than by a list anyone has to keep current. The *reaching* is structural;
the sentence recorded is positional, true only while every `?` precedes the commit, and a note
at the commit line says so. The word is `partial` at every one of them, because they all leave
the one standing the bind ordering guarantees: book bound, root still in the index. The
guarantee is **best-effort and says so** — a settling `UPDATE` that contends for the same busy
lock can itself fail, and the recorder then warns and leaves the claim registered, which is
true.

**One residual, narrowed**: `complete_db` inside a transaction sets `Settled` before the
commit, so a failed commit rolls the row back to `started` while the recorder still claims
settlement. At retirement's release — the one site — the settled path converges, because the
commit failure leaves through the wrapper, which settles on the connection after the rollback.
What survives is one step further out: if that settling `UPDATE` **also** fails, the claim
reads `Settled` while the row reads `started`. This is not repaired, and must not be — the
repair would be walking a settled claim back to `Registered`, which
`settling_a_settled_status_again_leaves_it_alone` forbids for the stronger reason that it
would point crash recovery at a run that finished. It stays unobservable, since nothing reads
`status_claim` after that point, and the instrument that would catch it — a `Drop` assert on an
unsettled claim — is deliberately absent: it would fire on the open `?` window above. `exclude`
and `notes` are not exposed: their row rolls back
entirely, so there is no row left to mis-describe.

The status claim's battery sits beside the recorder, `every_terminal_status_is_reachable_by_name`
among it — exhaustive over the vocabulary with no `_` arm, so a variant added later cannot slip
in unsettled.

### It remembers, rebuildably — and proves it — `repo::*` and `ops::ledger`

- **`repo::{db, source, root, object, fact, decision}`** — the shared database access layer:
  connection lifecycle and schema migrations, and the SQL every subsystem reads the nouns
  through. Here because the nouns are here and this is how they are loaded; its consumer list
  is effectively the whole tree. A subsystem's *exclusive* SQL is the opposite case and lives
  in that subsystem's own `repo` stratum (`trail/repo.rs`, `scan/repo.rs`, and the rest) — the
  split is by who reads it, not by what table it touches.
- **`ops::ledger`** — the extraction ledger's maintenance path (`ledger reindex`): rebuilds
  the `decision_extractions` projection from apply receipts already on disk. This is the
  *prove* half of the verb, and it lives with the discipline it proves: the claim that the
  database is a rebuildable index over durable receipts is only worth making because this
  rebuilds it. It also inverts `ops::receipt`, the writer it sits beside, and **it is the one
  place in the tree that reads a receipt file back** — every query path reads DB projections
  only. Its lenient read types (`ApplyReceiptDoc` and friends, tolerating fields
  pre-vocabulary receipts lack) live here deliberately and must never migrate into
  `ops::receipt`, which stays writer-only.

  Its apply-driving tests do **not** live here: they prove the round-trip law by running a
  real apply, so they name archive's transfer types, and a core test may not name a subsystem
  any more than core code may. They live in `archive/ops/receipt.rs`. If a future core test
  wants a subsystem, the answer is the same one: it is not a core test.

### It lends the hands — `ops::fs`

Structured access to files on disk: hashing, copy, rename, move, the incomplete-then-finalize
write. Canon's second data plane, parallel to `repo` — the ops layer orchestrates both. No
database access, no terminal I/O, and no business decisions: this module does what it is told.
Hashing belongs here rather than beside the object noun because identification is a physical
act — the identity warrant's positive mechanic is a read of the disk.

Apply's two preflight helpers deliberately do *not* live here: they answer a planning question
only apply asks, and sit with it in `archive/ops/plan.rs`.

### It reads the world for the tellers — `ops::root_story`, `domain::folder_tree`

Substrate that stops before any voice — the bidirectional law's sharpest edge, since
everything here exists to be narrated by someone else.

- **`ops::root_story`** — `RootStory`, `fetch_root_story`. The one structural fetch of a
  root's complete world-state. `retire`'s readiness review and book compile, and `story`'s
  place lens, are all lenses over one fetch — the gate, the book, and the story review can
  never drift apart because they all read this.
- **`domain::folder_tree`** — `FolderTree`, interned folder-tree topology. The entry
  criteria's own seed example: the sweep's containment walk and `story`'s signature walk share
  only this skeleton — consumers hold their own payloads in side-vectors by folder id, and
  each walk's emission rules stay independent. "Looks shared" would have pulled a walk's logic
  in too; only the topology qualifies.

### It conjugates — `ops::scope`, `domain::{scope, decision}`

Every effectful verb passes through the same grammar: **posture** (observed or performed),
**scope** (the typed where), **plan/execute** (intent shown, act done, the ceremony's
awareness moment between), **record** (a decision always; receipts for fates, at the locus of
effect; a verb that did nothing records nothing), and **transition** (the effect on standing,
in registered vocabulary).

`ops::scope` is the one scope-resolution pipeline — CWD defaulting, `--global`, root
membership, source existence. It is here because every scope-taking command resolves through
it, and because it is the fallback half of `domain::path`'s pure resolution: same function
names in both, the pure half returning `None` where this one reaches the filesystem. It owns
the two laws that live at that boundary.

The **scope-boundary honesty policy**: the source-existence gate returns a `ScopePartition`,
not a verdict, so a sourceless path among several is set aside and stated rather than aborting
the invocation; `validate_sources_exist` survives beside it as the abort spelling, and its
caller list *is* the carve-out list. **The gate sits on `resolve_scope`'s explicit-path arm
alone** — `--global` yields nothing to gate, and the CWD-defaulting arm hands the current
directory over ungated, because defaulting to CWD is a context switch rather than a claim
about content. A consumer that reads "came from `resolve_scope`" as "met the gate" is wrong on
that arm, which is the commonest one there is.

The **form-tolerance rule**: the gate retries a path's below-root remainder through its
normalization candidates and hands back the byte-form the index stores, so every downstream
comparison — Rust prefix matching and the SQL boundary spellings alike — sees stored bytes.
That retry is spoken once, in `stored_form_of_rel`, and reached from both doors: the argument
door's gate and the manifest door's `resolve_recorded_scope`.

`ops::scope` also owns `cwd_for`/`needs_cwd`: the current directory's absence is only fatal
when a relative path needs one, so an absolute-path invocation survives a CWD deleted out from
under the process, and a relative one is refused by name. `domain::scope` carries the typed
where-contract (`DecisionScope`) that makes an un-decomposed scope string unrepresentable, and
`domain::decision` the decision vocabulary itself.

Beside `DecisionScope`, `domain::scope` owns the **recorded-scope resolution law**: *a
recorded scope is resolved once against the known roots, and a prefix matching no root is
carried, never dropped* (`ScopeResolution`). It exists because a **recorded** scope is not a
resolved one — a manifest's `meta.scope` is user-editable text that arrives in whatever form
and whatever state the user left it, and it enters through a door `ops::scope` never sees.
Readers answering "which root owns this prefix" for themselves, byte-exactly, each lose a
prefix in their own silence: the vantage measures from somewhere deeper and files land
flattened at exit 0, while the recorder writes a scoped act down as a global one.

It reuses rather than re-spells, and it does so **in two stages** — the same form-tolerance
rule reaching both halves of a path, not two rules. Stage one is `attribute_prefix`
(`domain::path::normalization_candidates` composed with `domain::root::find_containing_root`),
which is pure and answers *which root*. Stage two asks which byte-form of the below-root
remainder the index knows sources under, which only a database can answer, so it lives one
layer up in `ops::scope::resolve_recorded_scope` and goes through `stored_form_of_rel` — the
very function the argument door's source-existence gate uses. That is what makes the two doors
incapable of drifting apart, and it is pinned as such by `the_two_doors_agree_on_the_same_paths`.
That pin says nothing about a *sourceless* path, where the two doors legitimately differ — the
argument door aborts on a lone one and sets aside among several, the manifest door always sets
aside — and widening it to cover that would assert an equality that is false by design.
**Order is not interchangeable**: a prefix whose root portion is written in the other form must
match its root before its remainder can be asked about at all.

The type is **infallible on purpose** and stays so: it classifies into five registers —
`scopes()` / `set_aside()` / `unrooted()` / `recorded()` / `measured_from()`, with
`selection()` a *reading* of the first — and never fails, because every failure mode is a
caller's own disposition.
`resolve_recorded_scope`'s `Result` carries *infrastructure* failure (a SQL error) and nothing
else. Every register is derived from one list of `PrefixOutcome`s through a **single assembly
site**, `from_outcomes`, which is also the type's only constructor: what each register means is
decided there rather than agreed on by two loops. There is no offline constructor,
deliberately — attribution alone answers *which root*, which is not enough to confirm a prefix,
so a resolution assembled without the index would be a second, weaker answer to the one
question this type exists to answer once.

**What `scopes()` excludes is the behavioural half of the law.** A confirmed prefix reaches the
vantage, the lock header, the decision record **and the selection**; a set-aside or unrooted
one reaches none of them. The selection belongs in that list rather than beside it:
`selection()` is the same register, spelled as absolute paths, because a run that selects from
a line it has told the user measures nothing gathers content it must then refuse one file at a
time. An unrooted prefix can be an *ancestor* of a known root — `path_is_under` matches it
where `find_containing_root` does not — which is how a lock comes to hold a non-empty header
beside unmeasured entries. `selection()` answers `None`, never an empty list, where a recorded
scope confirmed nothing: an empty scope list means **global** downstream, and a manifest naming
a place Canon cannot find must not become a whole-universe archive. That is what stops a line
naming a place Canon cannot confirm from dragging the measurement, so the surviving lines place
correctly instead of being pulled up a level. `recorded()` still carries every line — healed as
far as it could be, verbatim past that — because a refresh writes the user's own file back and
must not narrow it on their behalf. That partition is the scope-boundary honesty policy's shape
at a second door, which had neither half; the policy's terminal rule (a scope that kept nothing
must never look like a narrowing) is raised by `cluster refresh` through the same
`no_sources_known` sentence the argument door uses.

**`measured_from()` is the register the vantage folds**, and the one place the grain is read.
Every confirmed prefix contributes exactly one `DirectoryLocation`; a set-aside or unrooted one
contributes nothing, the same absence that keeps it out of `scopes()`. A **directory** scope
contributes itself; an **item** scope contributes the directory containing it, which is what
*the deepest directory containing every scope* already says about a file rather than a second
rule. `containing_location` defaults an absent parent to the root, so a root-level item and a
root itself both land on the root. Neither sorted nor deduplicated: it feeds a fold where order
and repetition are immaterial.

`measured_from()` exists **apart from `scopes()`** because measurement and selection ask
different questions of one confirmed prefix. Selection asks *what did the user name*, and a file
scope must select that file. Measurement asks *what is there to name below*, and a file has only
its own name to give. So the grain moves the measurement and moves nothing else — not
`scopes()`, not the lock header, not the decision record
(`the_grain_moves_the_measurement_and_no_other_register`).

**`DirectoryLocation`** carries the invariant *this location is a directory, by construction*.
Minted only in `from_outcomes`, private field, exposing `root_path()` and `location()`.
`common_path_prefix` states the same thing as a prose precondition that nothing enforced; at the
vantage's boundary it is now the signature, so an item path cannot be folded even by mistake —
`ScopeVantage::new` takes `&[DirectoryLocation]`, and passing a `&[DecisionScope]` is a compile
error rather than a test failure.

**The grain is a fact from the door, never derived here.** `ScopeGrain` is supplied by
`ops::scope::resolve_recorded_scope`, on the **confirmed byte-form** and for every confirmed
prefix uniformly — a conditional there would be a second rule about which prefixes have a grain,
and there is only one. A root short-circuits to `Directory` **without touching the index**,
because a root with nothing scanned into it is still a directory and the index could only call
it an item (`a_root_is_a_directory_even_with_a_row_standing_at_its_own_remainder`, asserted on the grain
itself: both grains measure an empty remainder to the root, so a register-reading test asserts
nothing, and on an ordinary index the fall-through happens to agree — the case that pins the
branch is a row standing at the root's own remainder, which the schema permits).

**One question, and it admits no tie**: does a *present* source stand at this path?
`repo::source::present_source_exists_at_path` answers it. Asking about the path itself rather
than about what lies below it is what closes the shapes a `below`-reading rule cannot answer
without choosing — **a path with a past can hold a row at it and rows beneath it at once, on a
current index, with no staleness at all**. A folder replaced by a file of the same name and
rescanned by name leaves both standing (the file-grain scan infers no absence, `src/scan/CLAUDE.md`).

**That is a structural guarantee, not a better guess** — and the step that carries it is the
fold, so it is written out rather than assumed. Take any entry at path `E`. Entries are present
sources (`batch_fetch_by_roots` filters `present = 1`), so a present source stands at `E`. Some
confirmed scope `S` selected it, and selection is at-or-under, so `E ⊑ S`. Each scope's
measuring point `P` is at or above its own scope — equal when `Directory`, the parent when
`Item` — and the vantage `V` is a **common prefix** of every `P` in the root, so at or above
each. That gives `E ⊑ S ⊑ P ⊑ V`. If `E = V` every link is equality, which makes `S` a
`Directory` *and* puts a present source at `S` — and `Directory` means precisely that none
stands there. Contradiction, so `V` is strictly above `E`, `path_strip_prefix` never returns `""`, and the
blank destination is *unreachable* rather than unlikely. Several scopes in a root only push `V`
further up, which is the safe direction.

The argument has one edge, and it is the root: collapsing the links needs `parent(S) ≠ S`, true
of every non-empty remainder and false at `""`, where `containing_location` returns the root
again. `scope_grain` answers the root before reaching that, which is why its short-circuit is
part of the proof rather than a convenience. **Pinned in two halves, each where it lives**:
`S ⊑ P` at the owner (`a_measuring_point_is_strictly_above_anything_standing_at_its_scope`),
the edge beside it (`a_root_is_a_directory_even_with_a_row_standing_at_its_own_remainder`), and
the whole chain through the real vantage against a real index at
`archive::ops::generate::no_scope_combination_measures_an_entry_to_nothing`. The fold link
(`P ⊑ V`) is a property of `common_path_prefix` and is argued rather than pinned as a property;
what is pinned there are its values.

**Presence, because the question is about now.** Filtering to present rows is the ordinary case
in `repo::source`; the history-inclusive predicates beside this one are the deliberate
exceptions, and each has its reason. The **confirmation gate** above is history-inclusive on
purpose — a manifest naming a place whose files have moved out is confirmed, not set aside —
which is exactly why the grain must not read history: a file that has become a directory leaves
a row standing at the path, and calling that an item would push every file below it down a
level (`a_tombstone_at_the_path_is_not_an_item`).

**Index evidence, never the disk** — a presence bit is a scan-time snapshot, not a live `stat`,
so the measurement is the same whether or not the drive is mounted. Stat-ing here would instead
make it vary with mount state, which is what the rule exists to prevent; no test asserts the
mount-invariance itself.

**Selection asks a different question, and at this door reads a different classifier.**
`cluster generate` classifies its scopes with `ops::scope::classify_all` — disk-only, no index
fallback — so a detached drive makes every scope `UnderDirectory`. That is harmless, because
`ScopeMatch::UnderDirectory` resolves through `path_is_under`, which is at-or-under, so a file
path classified as a directory still selects that file — together with anything the index
records beneath it, which in the both-live shape is real and which the measurement is
indifferent to; and the grain is index-only, so the measurement does not move with the mount
either way. (`classify_all_indexed`, the
history-inclusive disk-first classifier, is exclusion's and never runs beside the grain. It
includes tombstones for its own reason — dismissing content must mean the same thing attached
or detached — which is the same reason the *confirmation* gate is history-inclusive and the
grain is not.)

**Core entry** is registry-measured: `archive` (generate, refresh and status resolve; `apply`
takes the answer the lock recorded rather than resolving one) and `expr` (the vantage) consume
it, and it is substrate they independently computed over rather than any one subsystem's
finished output.

The law's registered verifier is `a_recorded_prefix_under_no_root_is_still_carried`, the one
that exercises the resolution a command runs; the rest of the battery sits beside the owner and
at the manifest door. One rung above the tests: `ScopeVantage::new` takes
`&[DirectoryLocation]`, so neither raw manifest strings nor a resolved item path can be
constructed into a vantage at all.

**Where it does not apply**: the `scope_prefixes` sites fed by `ops::scope::resolve_scope`.
Those prefixes are already root-validated and form-tolerant, so there is no drop for this to
close. This is the manifest door specifically. Two limits are real and stated rather than
latent. It does not lexically clean a prefix (`resolve_path` calls `clean_path` first), so a
hand-written `..` is matched literally. And stage one bends the prefix **as a whole** — first
candidate finding a root wins — so under a root that matches as typed nothing but as-given is
tried at that stage; what would otherwise make files land a directory out below such a root is
closed by stage two, which asks the index about the remainder rather than trusting the form it
was typed in.

### It defends its own laws — `testing/`

`core::testing` holds the shared fixtures every stratum's tests build a database with —
`cfg(test)`-only, nothing reachable from a release build. It sits in core for the same reason
`repo` does: the fixtures speak the fundamental nouns.

This facility carries an engineering name deliberately. The domain language speaks to users,
and users never meet the test rig — but its obligation is domain-derived all the same: the laws
above migrate into types and structure where they can, and where they cannot yet, they live as
tests beside the mechanics they guard. `tests/architecture.rs` classifies this directory as
`Layer::Testing`, a rule-free layer, because a fixture builds a database by writing rows and
the layer rules would otherwise read test setup as production data movement.

## The rule this exists to prove

Core must never depend on a subsystem (enforced: `Rule::CoreReferencesSubsystem` in
`tests/architecture.rs`) — the hub never depends on a spoke, or core stops being trustworthy
shared substrate. A subsystem may depend on core at any depth.

A subsystem may depend on a *sibling* subsystem only through its declared one-segment public
surface (`crate::<sibling>::item`) — never past it into the sibling's internals
(`crate::<sibling>::inner::item`, refused: `Rule::SubsystemSiblingInternalReach`). The surface
is the barrel's *item* re-exports: a stratum front door (`domain`/`repo`/`ops`/`cli`) is never
surface even at one segment, and module-handle imports (the sibling's bare root, or
aliased/glob imports at surface depth) are refused as scanning-defeat — the Feature-First
Structure ADR's sibling-boundary amendment. This is deliberately narrower than "core
membership": a sibling's *finished, computed output* consumed wholesale by another subsystem
(`retire`'s bound telling composing over `story`'s `report_over` result) is an ordinary product
dependency through a public surface, not shared substrate — it stays put, not core. Only
substrate multiple subsystems independently interpret for their own purposes belongs here.

## Adding to core

Three questions, in order:

1. **Does it warrant, or does it tell?** Anything that narrates — composes a sentence, chooses
   a voice, decides what the user reads — is a teller's, however many tellers want it. Core
   hands out facts and typed results.
2. **Is it substrate, or one subsystem's finished output?** A sibling's computed result
   consumed wholesale is an ordinary product dependency, not shared ground.
3. **Is it consumed by ≥2 subsystems, or named as spine by the Feature-First Structure ADR?**
   Sole-consumer code stays with its consumer, and comes here when a second one appears — with
   the trace in hand, not the expectation.

And if it states a claim: **what is its limit law, and where is that enforced?** A warrant
arriving without one is not ready to arrive.
