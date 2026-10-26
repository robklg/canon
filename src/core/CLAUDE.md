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

**The bidirectional rule.** Core never tells — no narration, no voice; its outputs are facts
and typed results, and the voice belongs to the tellers. One sentence is the stated
exception, described below beside the nouns it speaks of, with why it is here: the closed
door's. And tellers never warrant — a
subsystem that re-derives a claim core already makes has made a second claim, and two
mechanics for one claim are a safety defect waiting to disagree with themselves. Every
boundary question here resolves against this law first.

**The warrant-limit pairing.** Every warrant ships with the law of what it does *not* say; a
warrant without its limit is a false-claim generator. Wherever the spine states a claim
mechanic, the mechanic's limit is stated and enforced beside it — the pairing, not the
positive half alone, is what gets reviewed when either changes.

| Warrant | Positive mechanic | Limit, and where it is enforced |
|---|---|---|
| **Identity** | the hash; one content, one object — `ops::fs::compute_partial_hash`/`compute_full_hash`, `domain::object` | **contentless** — identity claims about empty content are vacuous. `domain::source::is_contentless_size` is the one place `size == 0` is written, and `Source::is_contentless` asks it; the SQL projections in `repo::object` carry it into every archived-ness query |
| **Standing** | the two-register resolution account — `domain::resolution::{classify_present, classify_absent, build_account}` | **the asymmetric verdict** — unresolved is provable, ready never is. Coverage is a containment fact: precise about content, silent about shape and worth |
| **Happening** | decision, receipt, transition — `ops::decision`, `ops::receipt`, `domain::fate` | **the posture law** — an observation must never read as an act. `domain::fate::fate_posture` is the one derivation, keyed on `(family, aspect)` |
| **Place** | path and scope mechanics — `domain::path`, `domain::scope`, `ops::scope` | **exactness at the edge** — a place claim is a boundary claim, and nearly-under is not under. `domain::path::path_is_under` is where that boundary is decided — `/a/bc` is not under `/a/b` — and `Source::matches_scope` asks it rather than deciding again; `repo::db::path_at_or_under_sql`/`path_strictly_under_sql` are its registered SQL projections, the one sanctioned rel_path boundary spelling |
| **Containment** | a destination is a `domain::path::ContainedPath`, made of ordinary names only, so joined under a directory its names cannot leave it | **lexical only** — the disk is never read, so a link below the directory can still lead outside it (`no_destination_escapes_the_destination_directory`) |

## What's here, by verb

### It names — `domain::{source, root, object, fact}`

`Source`, `Root`, `RootSpec`, `Object`, `ArchiveCopy`/`ArchiveLocation`,
`FactEntry`/`FactType`/`FactValue`/`Stored`/`SourceFacts`, and the predicates
that read them (`is_excluded`, `is_contentless`/`is_contentless_size`, `matches_scope`,
`find_containing_root`, `FactEntry::is_on_source`), and `KeyTypes`, the types a key's facts hold.
Every subsystem speaks these; nothing here knows about any subsystem. Beside them the pure
shape the nouns need to be handled at all — `domain::{path, scope, format, include, config}`:
path manipulation and offline resolution, `ScopeMatch` and `DecisionScope`, display
formatting, the visibility set, ledger-config parsing. Same purity rule throughout: no I/O, so
path matching and scope resolution are testable with known inputs. Two sanctioned exceptions,
side by side in `domain::time`: `now_epoch_secs`, the owner of "now" — a `SystemTime::now()`
read anywhere else is a duplicate re-derivation; test-only `with_test_now` pins it for a
fixture needing a controlled timestamp — and `zone::UserZone::machine`, the one read of the
machine's time zone.

**Time is one noun, read one way.** `domain::time` holds `TimeValue` — an instant, an instant
with its recorded offset, or a clock reading, each at the precision it was written at, never
padded — and `FactValue::Time` carries it. Its owners:
- **The frame rule** (`TimeValue::on_clock`): every time reads on the user's clock — an instant
  through the zone, a clock reading as written, never a clock reading turned into an instant;
  `printed` is the one form shown, `iso_text` the stored form written out. Verifier
  `a_printed_value_typed_back_selects_exactly_what_prints_the_same` (in the facility, where a
  typed time is compared); the span is pinned here through both clock-change hours. It gives
  a `ClockSpan`, which owns the printing (`printed`; `printed_as_asked` keeps a typed
  fraction) and the comparisons, one whole-period table (`satisfies`), read by `--where` and
  the trail alike: a value is selected when its whole period satisfies the operator, a value
  coarser than the typed period is in none of `<`, `=`, `>`, and a fraction is a point one
  nanosecond long. Beside the table, `spans` is its third side: a value whose period holds a
  comparison's boundary strictly inside it, on neither side — what an acting command counts
  (`spans_is_the_third_side_of_the_period_table`). It reads only spans, so nothing converts a clock reading to an instant. A
  time of day (`reading::read_time_of_day`) is a clock reading on `REFERENCE_DAY`, so the one
  table compares it too. `clock_secs_as_if_utc` counts a reading as if UTC
  for storage's encoding (its inverse is `repo::fact`'s read) and a speed path's bounds,
  never a comparison or a selection.
- **The user's-zone owner** (`zone::UserZone::machine`): reads `TZ` and the system zone once;
  everything else takes a `UserZone` as a parameter, and no test reads the machine's zone
  (`with_test_zone`). Build-refused by `the_machine_zone_is_read_only_by_its_owner`.
- **The list of spellings** (`reading::read_time`): the one place a written time's form and
  precision are decided, pinned per row; recognised here, with no roster row. The forms a
  refusal shows (`EXAMPLE_FORMS`, `EXAMPLE_WITH_OFFSET`) are pinned to read through it.
- **The zone-name parse** (`zone::parse_zone_name`): the one reading of a written zone name — a
  manifest's line, a lock's — into a `RecordedZone`: a known name (canonical case), `UTC`, or an
  offset spelled `±HH:MM`; a rule string is refused, never flattened, and an unknown name is not
  known here. `UserZone::recordable` builds the same type from the machine's rules. Zones are the
  same when names match or rules agree through their whole history (`RecordedZone::same_zone`),
  so a link never reads as a change. Build-refused by `zone_rules_are_looked_up_only_by_their_owner`.
  Not reached: the machine's own zone, read by `UserZone::machine`.
- **The record's clock text** (`zone::ReadTimesOn`): the one composition of the clock a durable
  record says it read times on — `of_user` from the user's zone, `of_manifest` from a
  manifest's. Built once per decision as `DecisionParams.zone` and copied to the row, the
  receipt's `[meta].zone` and a book, so none can name a different clock. Pinned by
  `the_row_and_the_receipt_state_the_same_clock` (the book's pin is retire's); recognised here,
  with no roster row. Not reached: a sentence naming the zone on screen speaks `ZoneLabel`.

**Where they do not apply**: machine fields (receipts, the book's structured fields, a
manifest's stamp) are UTC with `Z`. A manifest's run reads the zone the manifest or its lock
records, never the machine's.

**A stored time is `value_time` plus four `time_*` columns** (form, offset, fraction,
precision), read and written only through `repo::fact`'s conversion pair (`fact_value_params`,
`fact_value_at`): `value_time` alone is no epoch second for a clock reading. An unreadable time,
`Stored::Unreadable`, is kept only on the content.

**A number is one noun, read and written one way** (`domain::number`): `read_number` is the
one reading of text as a number — Canon's grammar, no `inf`, no leading `+`, no hexadecimal,
nothing too large to hold — and `spelled` the one writing, the shortest plain decimal that reads
back as the same number, never an exponent. Pinned beside it, every spelling read back;
recognised here, with no roster row. **Form first, fit second**: `number_form` asks the
grammar, `NumberForm::exact` the fit, which filter values and their printing ask. The form also
answers for its parts as written — `whole`, `fraction`, `written_as_integer` — which
the loss judgment and import's reading of a JSON number ask rather than reading characters.

**What a value becomes as another type is one table** (`domain::conversion::convert`): text
read as a number or a time, a number or a time written as text, and no conversion between a
number and a time or to or from a derived path — every arm named, none by `_`. Its text →
number arm carries **the loss judgment**: text read as a number loses what it carried where the
number held is not exactly the number written, where the whole part has a leading zero
(`01234`), or where the fraction ends in a zero (`2.50`) — judged by what is lost, never by
characters, so an exponent's spelling (`1e5`) and `-0` lose nothing. Import skips a value it
would lose. Pinned by `the_loss_judgment_reads_what_is_lost_never_characters`, which fails
against "lossy when the respelled text differs". **Where it does not apply**: text read as a
time is unchanged or does not read, and every type read as text is unchanged.

**Text is cut by characters, never by byte index**, because a cut inside a multi-byte
character panics. `domain::format::first_chars` owns the cut. `Cargo.toml` denies
`clippy::string_slice`; a slice whose index is a known boundary carries an `#[expect]` on its
own statement saying why, and a stale `#[expect]` is fatal. **Not reached**: anything clippy is not run on (no
build, test, hook or CI step runs it), the `canonargs` crate, non-slicing methods that panic
the same way (`truncate`, `split_at`, `drain`, `insert`, `remove`), and the hand-spelled guard
beside each fit-to-column cut.

`ArchiveCopy`/`ArchiveLocation` are what the archived-ness projections answer with: one
archive-side copy, its path where a path is wanted, and **the standing of its root beside it**.
Carried as data rather than looked up per surface — a surface asking the roots table again is
the re-detection the closed door's rule forbids. `domain::root::only_on_suspended_segment` is
the one spelling of the sentence that says so, capped at three named roots like the door
statement, and carrying **no way back**: it qualifies a claim rather than blocking one.

`domain::root` also carries the **closed door's vocabulary**: `ParkedRoot` (constructible only
from a suspended `Root`, via `Root::parked`, so a live root cannot be spoken of as parked),
`ParkedPath`, the permit class `DoorVerb`, and the sentence itself —
`ParkedRoot::door_line(verb, what)`, `<root> suspended — <verb>: <what> · <way back>`, with
`pause_line` for the verbless form a remembering view states. `DoorVerb`'s third arm,
`WouldRefuse(RefusedAct)`, is for a **report about acts it does not perform**: it names the act
the door turns away, in the conditional, because `set aside` and `rejected` are both false of a
report and a withheld next step owes its reader the act that is blocked. Reach is `cluster
status`. **The sentence is composed here
rather than in the interface because its consumers are not all in one layer**: an operation
cannot print, and three refusals raised from operations must speak the same sentence a command
prints, or the second spelling drifts. Grouping, capping and the channel stay in `src/scope.rs`,
where a screen is. Beside it `WayBack` — the one spelling of `canon roots unsuspend path:<root>`
and of the above-cap `canon roots list --suspended` — carried as the argv it would run, so a
round-trip pin (`every_emitted_way_back_parses`) proves what was printed; and `DoorRefused`, the
error that carries a door out through the ordinary error channel so the front door can state it
without an `Error:` prefix.

These are here because the Feature-First Structure ADR names them as the spine, not because
two consumers were counted. The membership criteria below apply to everything else.

### It warrants — `domain::{resolution, fate, extraction}`

- **`domain::resolution`** — `ResolutionAccount`, `StandingBucket`, `AbsentBucket`,
  `classify_present`, `classify_absent`, `build_account`, `unresolved_remainder`,
  `only_on_suspended`, `ParkedEvidence`, `standing_first`. Shared
  substrate `retire`'s readiness gate, `story`'s place lens and `sweep`'s root-nearness term
  each independently compute over; no subsystem owns it, which is exactly why it is here.
  `retire`'s own verdict (`Readiness`, derived *from* this account) and its book-compile fate
  model stay in `retire/domain.rs`: one consumer, so they do not qualify.
  - **What a claim rests on** (`only_on_suspended`, `ParkedEvidence`) — whether the archive
    copies grounding a claim stand **solely** behind closed doors. Both halves are load-bearing:
    non-empty, since no copies is no claim; and *every*, since one copy on an open root means
    nothing needs opening. Reached through `ParkedEvidence::observe`, the one accumulation site,
    which the account and coverage both call — so the law is consulted once and not re-spelled
    beside each counter. `ParkedEvidence` pairs the count with the roots it rests on **as one
    value**, so a line cannot name roots its own claim does not stand behind — the account
    carries one per claim rather than pooling them, and `build_account` takes the copies map
    rather than a set of ids, a set being unable to say where the evidence stands. A statement,
    never a refusal: no verb, no way back.
  - **The stated-never-headlined law** (`standing_first`) — a place behind a closed door never
    leads a list of places to go. `standing_first` itself only orders. A capped consumer owes the
    "still appears" half itself, starting with membership: selecting which entries earn a cap slot
    by weight, never standing, so a closed door cannot decide membership, is unconditional —
    every named consumer does this regardless of what it does next. What differs is what a
    consumer owes *after* selection: a voicing that may carry no bind-time claim at all never
    calls this function on the selected survivors (story's covered-where line, feeding a book
    that is never rewritten and so cannot tolerate even an order derived from today's doors); a
    voicing that may state order calls this function to reorder only those survivors, gated at
    the render layer rather than inside selection (story's own Judgment voicing, the same
    aggregate's other reader); a live, recomputed view instead sorts standing-first and counts
    every omission unconditionally, so a demoted place is stated rather than silent even though
    it can fall out of a cap (survey's four location lists, the trail composition card's
    origins). See the function's own doc for the worked split and which each named consumer
    chose. The sweep's live-preferred counterpart choice answers the same law through a different,
    filtering mechanism instead — a losing parked scope forfeits its citation rather
    than sorting behind — so it shares the law, not the function.
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
  half is the *happening* warrant's limit, which is why it sits with the transition
  vocabulary rather than in the recorder. `RegisteredFate` names the six pairs the totality
  pin proves `fate_transition` never answers `None` for, so a caller already holding one of
  them asks its `transition()`/`posture()` rather than `expect`-ing past the wider `Option`.
- **`domain::extraction`** — `OriginDisposition`/`DecisionExtraction`/`ExtractionItem`/
  `build_extraction_rows`. Shared between apply's forward recording and `ledger reindex`'s
  backfill. The extraction round-trip law — a backfilled row structurally indistinguishable from a
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
  survives on a row that reads `started` is therefore what the recovery signal names: a run
  whose outcome was never written — killed, crashed, power lost, or a settlement the database
  itself refused. That is only true because every other exit settles, which is what the armed
  recorder below closes; the refused-settlement reading is stated on every surface that carries
  the signal rather than rounded away.

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

**Every other window settles on drop — the armed recorder.** `armed(conn, UnwindTerminal)`
yields an `ArmedRecorder` whose cleanup writes the command's own terminal word and sentence. A
normal early return runs it; a kill does not. So a propagating error at `cluster
generate`/`refresh`, `roots rm`/`suspend`/`unsuspend`, the prunes or `import-facts` leaves a
settled row rather than `started`. (`facts delete` settles too, but not this way — see *Where it
does not apply*.) *Which* word and sentence such an exit deserves stays per-command judgment,
composed beside that site's `DecisionParams`; the recorder owns only *that* it is written. The
guard offers no way out — no `into_inner`, no constructor for the inner recorder — though the
public `DerefMut` means the seal is the absence of a reason to swap the recorder out, not a
refusal the type can make. Arming is opt-in, and the architecture test's
`RECORDER_START_SITES` census refuses a new `start()` caller declaring no settlement shape.

**The ordering rule: arm before any transaction is opened on the same connection.** Locals drop
in reverse declaration order, so on a `?` the transaction rolls back first and the guard's
UPDATE lands in autocommit. Armed the other way round it rolls back with the transaction and
the row reads `started` over a run that did reach a last act — the falsehood the guard exists
to remove, wearing its clothes, and silent when broken. Held in the recorder by
`a_settlement_after_a_rollback_lands`, and at the one production site that opens a transaction
on the same connection by `a_failed_orphan_prune_settles_after_its_rollback`.

**Where it does not apply.** `exclude` and `notes` roll their row back with their own
transaction; `scan`, `apply` and retirement settle on their own declared paths (below);
**`facts delete` cannot arm at all** — its delete needs a mutable connection, which the guard's
shared borrow forbids, so it settles through its own wrapper, and `src/facts/CLAUDE.md` carries
why. None of them arms. A panic leaves `started` deliberately — a panic is a crash, and writing to SQLite
during unwind could turn one panic into an abort. So does a drop-time write the database
refuses: the run reached a last act and could not write it down, most likely because the busy
lock that raised the `?` refused the settlement too. **Stated, not repaired** — it is why a
`started` row means *the outcome was never written* rather than *killed*, and why
`docs/src/concepts/decisions.md` and `trail show` say so instead of promising a kill. One blind
spot, named and unheld: a `std::process::exit` inside an armed window skips the guard. None
sits inside one today, and only the guard's doc comment holds it there — the census cannot see
it.

The guard's own battery sits beside the recorder, named in its doc comments.

**Retirement's release is closed**, and is the reference projection for the rest: `release` is
a wrapper whose single `Err` arm settles on the connection, so its four internal `?` — the
immediate transaction, the world-moved probe, the removal, the commit — settle by
construction rather than by a list anyone has to keep current. The reaching is structural, and
so is the sentence: the failure arm reads a `released` flag set on the line after the commit,
rather than counting on nothing below that line being able to fail. The word is `partial` on
every arm that reaches it, because they all leave the one standing the bind ordering
guarantees: book bound, root still in the index; past the commit the committed row is left
alone. The
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
`status_claim` after that point. `exclude` and `notes` are not exposed: their row rolls back
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
  **A key's type is read from the key's row set**: `repo::fact::key_types` reads every fact of
  the key and joins nothing, so suspension, scope, presence and role can never enter it; a
  key with no facts has no type. `repo::fact::key_census` reads the same row set whole, for the
  conversion.
- **`ops::ledger`** — the extraction ledger's maintenance path (`ledger reindex`): rebuilds
  the `decision_extractions` projection from apply receipts already on disk. This is the
  *prove* half of the verb, and it lives with the discipline it proves: the claim that the
  database is a rebuildable index over durable receipts is only worth making because this
  rebuilds it. It also inverts `ops::receipt`, the writer it sits beside, and **it is the one
  place in the tree that reads a receipt file back** — every query path reads DB projections
  only. Its lenient read types (`ApplyReceiptDoc` and friends, tolerating fields
  pre-vocabulary receipts lack) live here deliberately and must never migrate into
  `ops::receipt`, which stays writer-only.

  Its apply-driving tests do **not** live here: they prove the extraction round-trip law by running a
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

Substrate that stops before any voice — the bidirectional rule's sharpest edge, since
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
membership, source existence, the door. It is here because every scope-taking command resolves
through it, and because it is the fallback half of `domain::path`'s pure resolution: same
function names in both, the pure half returning `None` where this one reaches the filesystem.
It owns the three laws that live at that boundary.

The **closed door**: *whether a place is behind a door the user closed is derived once, here,
as data — never re-detected from `current_dir()` or from an `is_active()` filter applied to a
root the caller was already handed resolved.* Owner `resolve_scope`'s `Door` outcome (`Open` a
`ResolvedScope`, `Closed` a `ClosedDoor` naming the parked places); verifier
`a_parked_cwd_is_never_global`; reach `core`. It is an enum and not a flag because
`ResolvedScope::is_global()` would otherwise keep saying "all roots" about a place behind a
door — `Closed` is unreachable from it by construction, and `tests/architecture.rs`'s
`the_door_types_are_consumed_by_name` refuses a `_` arm, so each consumer names the permit
class it is declaring. Two sibling doors project the same fact from their own inputs:
`RootLookup` at the root-spec door (`Found` / `Parked`; a genuinely unknown root stays `Err`),
and `PrefixOutcome::Parked` at the manifest door, which `resolve_recorded_scope` emits and
`ScopeResolution::parked()` carries. All three carry the same `ParkedRoot` payload rather than
each re-deriving one, which is what lets a consumer state a door without a second lookup.
**Precedence in the partition**: root membership (the harder failure) > the door > source
existence — a parked path is stated as parked whether or not it has sources, because the closed
door is *why* it has nothing to say.
`ClosedDoor::read_here` is the one conversion a **remembering** consumer makes; the pause it
carries (`ResolvedScope::pause`) and the set-aside register beside it (`parked`) are opposite
dispositions of one fact and must not be merged. **Where it does not apply**: a directory
under no known root, which is still the silent global fallback, and `--global`, which asks a
different question and is answered as asked. The sentence itself is not here — the grammar
lives on `domain::root::ParkedRoot::door_line`, because its consumers are not all in one
layer.

The **scope-boundary honesty policy**: the source-existence gate returns a `ScopePartition`,
not a verdict, so a sourceless path among several is set aside and stated rather than aborting
the invocation; `validate_sources_exist` survives beside it as the abort spelling, and its
caller list *is* the carve-out list — `refuse_parked_locations` is its door-shaped companion,
covering those callers and, additionally, `exclude`'s two single-file arms, which resolve their
own path and never reach the partition. Those two are not carve-outs; they are single-target
acts, refused by the closed default rather than by load-bearing-ness. Two roads, one verb, which
is why the verb is not a parameter. The parked partition is the policy's **third
cause**, carried on `ResolvedScope::parked` beside the sourceless `set_aside`: same position,
same channels, same terminal rule, and the same "never becomes a `DecisionScope`" pin
(`a_parked_path_never_becomes_a_decision_scope`) — a claim about **what resolution produces**, and
not about every row in the table: a scope settled into a manifest's lock while its root was open
stays what that act was scoped to, and a root closed afterwards does not rewrite it. **The gate sits on `resolve_scope`'s explicit-path arm
alone** — `--global` yields nothing to gate, and the CWD-defaulting arm hands the current
directory over ungated, because defaulting to CWD is a context switch rather than a claim
about content. A consumer that reads "came from `resolve_scope`" as "met the gate" is wrong on
that arm, which is the commonest one there is.

The **form-tolerance law**: the gate retries a path's below-root remainder through its
normalization candidates and hands back the byte-form the index stores, so every downstream
comparison — Rust prefix matching and the SQL boundary spellings alike — sees stored bytes.
One law, two halves: the **owner** is `domain::path::normalization_candidates`, the root half,
database-free and first at every door; the retry is its **registered projection**,
`known_form_of_rel`, whose sources reading `stored_form_of_rel` is what both doors take — the
argument door's gate and the manifest door's `resolve_recorded_scope`. Canon-wide: wherever a path arrives as text and is matched
byte-exact, this is the law it owes.

`ops::scope` also owns `cwd_for`/`needs_cwd`: the current directory's absence is only fatal
when a relative path needs one, so an absolute-path invocation survives a CWD deleted out from
under the process, and a relative one is refused by name. `domain::scope` carries the typed
where-contract (`DecisionScope`) that makes an un-decomposed scope string unrepresentable, and
`domain::decision` the decision vocabulary itself, including what each command puts in a row's
receipt columns (`DecisionCommand::receipt_columns`).

Beside `DecisionScope`, `domain::scope` owns the **recorded-scope resolution law**: *a
recorded scope is resolved once against the known roots, and a prefix matching no root is
carried, never dropped* (`ScopeResolution`). It exists because a **recorded** scope is not a
resolved one — a manifest's `meta.scope` is user-editable text that arrives in whatever form
and whatever state the user left it, and it enters through a door `ops::scope` never sees.
Readers answering "which root owns this prefix" for themselves, byte-exactly, each lose a
prefix in their own silence: the vantage measures from somewhere deeper and files land
flattened at exit 0, while the recorder writes a scoped act down as a global one.

It reuses rather than re-spells, and it does so **in two stages** — the same form-tolerance
law reaching both halves of a path, not two rules. Stage one is `attribute_prefix`, a reading
of `domain::path::resolve_containing_root`, which is pure and answers *which root*. Stage two
asks which byte-form of the below-root remainder the index knows sources under, which only a
database can answer, so it lives one layer up in `ops::scope::resolve_recorded_scope` and goes
through `stored_form_of_rel` — the very function the argument door's source-existence gate
uses. That is what makes the two doors
incapable of drifting apart, and it is pinned as such by `the_two_doors_agree_on_the_same_paths`.
That pin says nothing about a *sourceless* path, where the two doors legitimately differ — the
argument door aborts on a lone one and sets aside among several, the manifest door always sets
aside — and widening it to cover that would assert an equality that is false by design.
**Order is not interchangeable**: a prefix whose root portion is written in the other form must
match its root before its remainder can be asked about at all.

**The law has one owner, one projection and a named set of readings; a site outside them is a
second spelling.** No count is given, deliberately — a count invites a completeness it cannot
warrant, and the readings are enumerated here instead. The *root* half is `domain::path::resolve_containing_root` — candidates of the whole
path over `find_containing_root`, database-free, first match wins, carrying the matched form out
as `RootedPath.path` so the next comparison cannot undo the match; `domain::path::resolve_path`
and `domain::scope::attribute_prefix` are its readings for text that names something already
recorded, and `domain::path::resolve_containing_root_as_written` its reading for text that names
somewhere content is *going* — the root bends to be found, the folder below it is being created
and keeps the bytes the caller typed. None of the three spells any of it themselves. Carrying a
healed remainder back out as a whole path is the law's last step and has its own owner,
`domain::path::join_under_root`, read by `domain::extraction::join_prefix` and
`domain::path::ContainedPath`. What it owns is the **empty-remainder rule**: a remainder
that is empty gives the root back unchanged. `Path::join` is not a spelling of it — it puts a
separator after an empty remainder and hands back a root with a trailing slash. **The tree still
hand-spells that branch** in many places, among them `domain::source::Source::path` and trail's
scope-row display, each a standing second spelling rather than an exemption; route new ones
through the owner. The
*remainder* half is `ops::scope::known_form_of_rel`: the **loop** is the contract — as-given
first, first-confirmed wins, `None` where no form is — and the question is a parameter, because
two questions exist. **A root-level remainder is asked about like any other**: whether an empty
remainder counts as known belongs to the question and not to the loop, so a reading that answers
yes for it says so itself. `stored_form_of_rel` asks the sources one, which is
what both scope doors and `stored_form_with_sources` need; trail's history gate asks about notes,
extraction endpoints and scoped decisions besides, and asks *through* here, since a place with no
sources is exactly the case that gate exists for. Beside them two **comparison readings**,
`path_is_under_any_form` and `same_path_any_form`, for the sites that have no live root to find
and no remainder to ask about — a retirement's scope snapshot, a receipt's origin root, a book's
recorded root. Those answer yes/no, so the as-given ordering that governs healing is moot there:
*any candidate matching* is the honest question when the recorded side is snapshot text whose
form was fixed whenever it was written. Outside core, one subsystem-local reading:
`trail::domain::crossings::ledger_form_of_counterpart`, which asks the **ledger** rather than
the index and is described in `src/trail/CLAUDE.md`. **The asymmetry is the point**: the text
side bends, the recorded side never does. The **door** is asked between
those two stages, never after: a prefix on a closed root comes back `Parked` whether or not the
index knows sources under it, which is the argument door's own precedence (root membership >
the door > source existence) at the second door, and which is why a closed root's remainder is
never asked about at all (`a_parked_recorded_prefix_is_parked_not_set_aside`). What each
command does about a parked prefix is its permit class: `cluster refresh` refuses on it,
`cluster status` states it, and — like the argument door's own parked register — it never
becomes a `DecisionScope`.

The type is **infallible on purpose** and stays so: it classifies into six registers —
`scopes()` / `set_aside()` / `unrooted()` / `parked()` / `recorded()` / `measured_from()`, with
`selection()` and `kept_nothing()` *readings* over them — and never fails, because every failure
mode is a caller's own disposition.
`resolve_recorded_scope`'s `Result` carries *infrastructure* failure (a SQL error) and nothing
else. Every register is derived from one list of `PrefixOutcome`s through a **single assembly
site**, `from_outcomes`, which is also the type's only constructor: what each register means is
decided there rather than agreed on by two loops. There is no offline constructor,
deliberately — attribution alone answers *which root*, which is not enough to confirm a prefix,
so a resolution assembled without the index would be a second, weaker answer to the one
question this type exists to answer once.

**What `scopes()` excludes is the behavioural half of the law.** A confirmed prefix reaches the
vantage, the lock header, the decision record **and the selection**; a set-aside, unrooted or
parked one reaches none of them. The selection belongs in that list rather than beside it:
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
at a second door, which had neither half; the policy's **terminal rule** — a scope that kept
nothing must never look like a narrowing — is `kept_nothing()`, spoken here beside the registers
it reads rather than at the command that refuses on it, because it has a second reader that
refuses nothing: `cluster refresh` raises it through the same `no_sources_known` sentence the
argument door uses, and `cluster status` consults it before naming a refresh, a report that
named one it turns away being the defect the second reader was found as
(`the_terminal_rule_fires_only_where_something_was_set_aside`). An **all-unrooted** scope is
deliberately not the rule's case, and neither is a **parked** one — the door is its own refusal
with its own way back, so which of the two a caller states where both hold is the caller's
precedence, not the predicate's. Both readers being in one subsystem today does not make this a
subsystem's predicate: it is the argument door's own rule at the second door, and its two
siblings live in `core::ops::scope`.

**`measured_from()` is the register the vantage folds**, and the one place the grain is read.
Every confirmed prefix contributes exactly one `DirectoryLocation`; a set-aside, unrooted or
parked one contributes nothing, the same absence that keeps it out of `scopes()`. A
**directory** scope contributes itself; an **item** scope contributes the directory containing it, which is what
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
   hands out facts and typed results. The closed door's sentence is the exception, here
   because its consumers sit in more than one layer.
2. **Is it substrate, or one subsystem's finished output?** A sibling's computed result
   consumed wholesale is an ordinary product dependency, not shared ground.
3. **Is it consumed by ≥2 subsystems, or named as spine by the Feature-First Structure ADR?**
   Sole-consumer code stays with its consumer, and comes here when a second one appears — with
   the trace in hand, not the expectation.

And if it states a claim: **what is its limit, and where is that enforced?** A warrant
arriving without one is not ready to arrive.
