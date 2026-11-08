# archive/ — declaring an archive operation, and performing it

The two commands that move content into the archive: `canon cluster generate/refresh/status` and
`canon apply`. The manifest is the human-editable declaration of an intended archive operation;
apply is the act that carries it out and records what it did.

Three names meet here and are not synonyms: a **cluster** is a selection of sources gathered for
archiving; a **manifest** is the declaration of what that selection should become, paired with a
lock file snapshotting the sources at generation time; **apply** is the act.

## Shape

```
archive.rs        # the barrel; every stratum mod is private
archive/
  domain.rs       # the manifest format — pure: no I/O, no Connection, no expr
  cli.rs          # mods, plus the sentences both commands print
  cli/
    apply.rs      # canon apply
    cluster.rs    # canon cluster generate/refresh/status
  ops.rs          # declaration-only
  ops/
    pattern.rs    # build_eval_context + evaluate_pattern — used by plan, status, cli/apply
    plan.rs       # what an apply would do, and what stands in the way
    execute.rs    # the transfer loop, receipts, extraction rows
    receipt.rs    # apply's two receipt document shapes
    generate.rs   # plan + execute for cluster generate/refresh, manifest emission
    manifest.rs   # the manifest and lock file readers and writers
    status.rs     # the diagnostic read: what landed, what is still waiting
  repo.rs         # archive's own SQL — what apply writes and reads, and the one read cluster status shares
```

**The internal dependency direction is a DAG, and the split is what keeps it one**:
`execute → plan → pattern`; `execute → receipt`; `generate → manifest`; `status →
manifest`, `status → pattern` (a function-local `use` inside `compute_manifest_status`). `pattern`, `manifest`
and `receipt` are the sinks, pointing at nothing above `domain`. An edge from `plan` to `execute`
would undo the separation the split exists to hold.

**The exceptions are all test-only**: `status.rs`'s test module imports `plan` for the one pin
that asks both readers (`cluster status` and `--resume`) the same question of one lock and one
index, and `generate.rs`'s imports `plan` too. And `receipt.rs`'s test module imports `execute` and `plan`
(the transfer types) for the extraction ledger's round-trip corpus, which runs a real apply. Those
imports sit inside `mod tests`, so production `receipt.rs` is still a true sink — but the edge is
real and no build rule catches it, because reaching within one's own subsystem is legal at any
depth, so prose is the only guard here. `execute.rs` would have been the zero-edge home for that
corpus, and is already the subsystem's largest file — the same reason `receipt.rs` exists at all.

**The barrel** is `run`/`ApplyOptions` and `generate`/`refresh`/`status`/`GenerateOptions` from
`cli`, `TransferMode` from `ops::execute`, `RefreshRefused` from `domain` (which `main.rs` prints), and `plan_generate`/`ClusterGenerateParams`, which have
no production caller and serve the contentless-law canary. Those two carry
`#[allow(unused_imports)]` with a comment naming the consumer; `TransferMode` stays on its own
un-allowed line, because it has a real production caller (`main.rs`'s dispatch) and must not hide
behind an allow that would mask a genuine unused-import signal. **A test-only re-export is a real
cost** — indistinguishable at the barrel from a production surface — so a test that needs the
strata moves inside the subsystem rather than the surface widening to reach the test.

## Cluster/Apply Workflow

`cluster generate` selects sources (`batch_fetch_by_roots` + domain predicates), batch-fetches
facts, computes 100%-coverage facts in-memory; the lock file stores **source identity, staleness
data, and, where the manifest is scoped, each file's path relative to the scope (no fact snapshots,
no destination)** — a fact can be looked up fresh and the
measurement cannot, which is the line the format draws. **The lock file is an immutable snapshot
artifact — never mutated after it is written**: a refresh writes a new one whole.

`apply` validates all pattern expansions upfront (failures collected and reported together, not
fail-fast — all validation before any file op), looks up facts at runtime (the DB is source of
truth, so a changed fact uses the new value), and uses size+mtime+partial_hash, not facts, for
staleness.

**Manifest format**: `ManifestMeta.version: u32` (`CURRENT_MANIFEST_VERSION`, 2; old manifests
`serde(default)` to 1; `validate_manifest_version()` rejects future versions).

**The version is gated before the body's shape is asked for.** `domain.rs::parse_manifest_config`
reads `meta.version` off a `toml::Value` — which needs to know nothing about the body — gates it,
and only then deserializes `ManifestConfig`. That order is what makes the gate reachable in the one
case it exists for: a version bump announces a *format* change, so the manifest that most needs
*"update Canon"* is exactly the one serde cannot parse. **Every production read parses through
it** — `ops/manifest.rs::read_manifest_config` for `cluster status`, `cli/apply.rs` (which needs
the text again for the notes), and `cli/cluster.rs::parse_manifest` (which adds the `[options]
allow` vocabulary for generate and refresh) — none of the three deserializing a `ManifestConfig`
itself. The probe is a **gate, not a filter**: a body that is not TOML, or carries no readable
`meta.version`, falls through to the full parse, so serde still speaks for every malformed file
that is not a version problem, and an absent version stays version 1 — an old manifest is not a
future one. Pinned by `a_future_manifest_is_refused_by_version_before_its_shape`, whose fixture is
asserted undeserializable, so the refusal can only have come from asking the version first. **Where
it does not apply**: the lock file, whose `lock_version` is a discriminator and deliberately not a
gate; and binaries already shipped, which parse before they gate — the ordering buys nothing
backward, only forward.

**`ManifestMeta.scope: Vec<String>`** — every path the generation was scoped to, held as a list
because that is what it is. `de_scope` is the only place the field is read, and it accepts
**one** form: a list. A version 1 manifest wrote its scopes joined into a single string, and that
form is **refused by name**, not reconstructed: splitting it apart is a guess about where files
land, a directory name may carry the separator itself, and a reconstruction turns a working
manifest into a refusing one while letting `cluster refresh` overwrite the only faithful copy on
disk. The refusal is spelled in `de_scope` rather than left to serde, whose bare `invalid type:
string …` is true and useless, and it is delivered by a **visitor** rather than an untagged enum,
so every *other* malformed value still gets serde's own message naming its own type and value
instead of one line naming a private enum. The way back is named in the refusal and nowhere else.
**No special case for a string that looks unambiguous**: a lone comma-free path is refused like any
other, because deciding which strings are obviously safe is the same guess in smaller clothes.

**One register, four uses.** A run selects from, measures from, records, and writes its lock header
from the *same* confirmed scopes — `ScopeResolution::selection`/`scopes`, and `measured_from()` for
the measurement, which is that same confirmed set read as directories — never from `recorded()`,
which also carries the lines that resolved to nothing. An unrooted prefix can be an **ancestor** of
a known root, so selecting through it gathers sources no vantage can measure: a lock with a
non-empty header and unmeasured entries, refused file by file at apply for a line the run had
already said measures nothing. At the other end, an empty scope list would read as *global*. Pinned
by `a_line_that_measures_nothing_selects_nothing`.

**Two writers resolve the recorded scope; two readers consume what they settled.** The config is
editable intent; the lock is the settled decision. `cli/cluster.rs` (generate and refresh) resolves
`meta.scope` once per run and writes the answer into the lock — a header carrying the resolved
scope, and a `scope_rel_path` on every entry. `cli/apply.rs` and `ops/status.rs` read the lock:
neither resolves a scope, builds a vantage, or consults roots for placement, and `ops/pattern.rs`
takes each entry's recorded measurement straight off the entry (`ops/plan.rs` borrows it from
there): apply *selects nothing and carries out a lock already built*, placement included.

**Lock file format**: JSONL, a `LockHeader` line followed by one `LockEntry` per line.
`CURRENT_LOCK_VERSION` is 2. There is no version 1 constant, because a version 1 lock has no header
to read one out of — its *absence* is the detection, and `ops/manifest.rs::read_lock_file` tries the
first line as a header and reads it as an entry when that fails. The two shapes cannot be confused:
a header has no `id`, an entry no `lock_version`. `LockHeader.scope: Vec<LockScope>` is deliberately
not a serialized `DecisionScope` — that type's guarantee is that constructing one requires a
matching root, and deserializing into it would put arbitrary file text through the back door; apply
converts via `DecisionScope::new`, leaving the domain type with one constructor. `LockEntry`'s
`scope_rel_path` is the opposite case to a fact snapshot: facts can be looked up fresh, and this
**cannot** — the vantage is *where the user pointed*, which may be shallower than where the files
are, so with one scope at `/R/proj` and all files under `/R/proj/src` the value is `src/main.c`
while the entries' own common prefix would answer `main.c`
(`the_measurement_cannot_be_recovered_from_the_entries`). `lock_hash` covers the whole file, so the
header is tamper-evident at no extra cost, and the write-then-hash order is unchanged.

**An old lock is refused, not migrated** — no `SUPPORTED_MANIFEST_VERSION` bump and no
compatibility path. Apply refuses a headerless lock before the plan and before its decision row
exists, pointing at `cluster refresh`, and does so **unconditionally rather than gated on whether
the pattern reads `{scope.rel_path}`**: such a lock also carries no scope for the decision record,
so a run that went ahead would write a scoped act down as a global one — the same record falsehood
on every apply, whatever the pattern says. The refusal carries its whole message in the `bail!`, on
the pattern the lock-hash mismatch already uses. Pinned by
`a_lock_with_no_header_is_refused_whatever_the_pattern`, which runs it against `{filename}` too.

**One measure function, both writers.** `ops/generate.rs::measure_entries` builds one
`expr::ScopeVantage` for the run and strips it off each entry's path through the path law's own
`path_strip_prefix`; `execute_generate` and `execute_refresh` each call it as their first act, so no
writer can forget it — two similar loops is the drift this class of defect comes from, and
`generate_and_refresh_write_the_same_lock` asserts the two produce **byte-identical** files from the
same inputs. After a fresh generate or refresh every value is `Some` **wherever the run confirmed a
scope**: entries are selected from the very register the measurement is taken from
(`ScopeResolution::selection`), so every entry's root has a vantage and every entry lies under it.
*Confirmed*, not *recorded* — selecting from what the manifest records would gather sources through
a line that measures nothing, and the claim would be false. An **unscoped** manifest (`--global`, or
filters alone) measures nothing, correctly: there is nothing to measure from, and a fresh v2 lock
carries no measurement.

So an absent measurement has **two** causes, taking different answers — a refresh rebuilds an old
lock, and gives an unscoped manifest nothing. The entry cannot tell them apart (both are an absent
field), so the question is asked of the **file**: `ops/manifest.rs::LockFile::unmeasured_reason`
answers it from the header's presence, and every reader hands that `expr::Unmeasured` to
`evaluate_pattern` the way they hand it the root cache. The two refusals then say their own thing,
and `cluster status` asks the same question before naming a next step — offering the rebuild for an
old lock and **nothing at all** for a pattern that will not expand, because a refresh does not give
a pattern the scope or the fact it is missing. Pinned by
`an_unmeasured_entry_is_refused_by_the_reason_it_is_unmeasured` (`expr/domain/pattern.rs`).

**What each command records**: `generate` and `refresh` from the resolution's **confirmed
scopes**; `apply` from **the lock header**, so editing `meta.scope` after the refresh cannot change what a decision row claims
(`editing_the_scope_after_the_lock_changes_neither_placement_nor_record`). Editing `output.pattern`
still takes effect immediately: a pattern is how you want things *named*, not a property of the
selection.

**And no reader decides for itself which root owns a prefix.** The recorded list is *text a user can
edit*, arriving in whatever form and state they left it — retyped in another normalization, naming a
directory since moved, naming a root since removed. Generate, refresh and status all resolve it through
the one `core::ops::scope::resolve_recorded_scope`, which heals a prefix in both halves (root and
below-root remainder) through the same lookup the command line uses, and partitions what is left. A
reader that matches roots privately and byte-exactly drops an unmatched prefix in its own silence.

The dispositions differ by what each command is about to do, and none of them is silence:

- **`cluster refresh` states and continues.** A prefix the index cannot confirm is set aside and
  named, through the scope boundary's own `no sources known at <p> — skipped` spelling; a prefix
  under no known root is named too, in its own words. Both go out on the honesty policy's position
  for an effectful command — before any plan display and before any confirmation. Refresh then
  writes the resolved form back: a confirmed prefix comes back in the byte-form the index stores,
  so **a refresh repairs an accent mismatch in the manifest text itself**; a set-aside or unrooted
  one is preserved verbatim, because there is nothing to heal it to and dropping it would silently
  narrow the user's own file. A refresh whose scope kept nothing refuses, naming every set-aside
  line — the boundary policy's terminal rule. An all-unrooted scope is deliberately not that case: a
  refresh is the way back from a manifest naming a root that is gone.
- **`cluster generate` has nothing left to state.** Its prefixes arrive from
  `core::ops::scope::resolve_scope`, already healed and already gated, and the argument door stated
  its own set-asides before this command was called. Running them through the same
  `resolve_recorded_scope` is therefore idempotent — and that is the point: asking the same question
  through the same code is what makes generate and refresh incapable of disagreeing about a
  manifest, which `the_two_doors_agree_on_the_same_paths` pins at the unit.
- **`apply` does not read the recorded scope at all**, and carries no unrooted refusal. A line that
  resolved to nothing never entered the lock, so it moved neither the files nor the record; the
  refusal apply *does* carry is the old-lock one above.
- **`cluster status` carries every category out** on `ManifestStatus` and lets the interface print
  them: `unrooted_scope` and `set_aside_scope`, and — because a report must name no act that would
  be turned away — `scope_kept_nothing` and `scope_confirms_nothing`, the two halves of the
  recorded scope the decision table reads. `all_accounted_for()` answers about source files the
  report checked — never over a lock placed nowhere. Whether there is a **next step** is never the
  scope's business: none with nothing pending and no entry lacking a destination over a lock that
  is there, nor where a
  checked entry found something lost or wrong (`found_nothing_lost_or_wrong`); *which* is the table's.

The recorded-scope resolution itself, and its pins, live in `src/core/CLAUDE.md`.

## The manifest door

A manifest can reach behind a door the user closed in **three** ways, each belonging to a
different act: its **recorded scope** can name a closed root (`ScopeResolution::parked`, the
manifest door's own arm), its **lock** can hold entries whose sources stand on one, and its
**destination** can be one. A manifest with no scope at all still holds entries gathered before a
root was suspended.

**Which act each suspension stops turns on one distinction: a naming versus a record.** A recorded
scope is a *naming* — a durable declaration of what collection is being assembled — so a line of
it standing on a suspended root refuses the act that reads it. A lock entry is a *record*: a snapshot
of where a source stood when the selection was last made. A refresh is asking again, so it
re-selects from unsuspended roots (which already leaves suspended roots out) and rebuilds the lock
without them — an act ignoring suspended content, silently. The lock is refused where it would be
**carried across**, which is `apply`.

**One computation, two readers.** `ops::status::parked_lock_entries` answers "how many of this
lock's entries stand behind each closed door", and `apply` and `cluster status` both read it — a
report inviting an apply that apply itself turns away is exactly the drift this closes. It asks
the **index**, never the entry's own recorded `root_id`: suspension is a question about where a
source stands now, and a lock is a snapshot of where it stood.

- **`cluster refresh` refuses on its recorded scope and on nothing else**, before the selection and
  before anything is written, stating that the manifest and its lock are unchanged. **Whole**,
  including a suspended root's line beside live ones: the argument door's set-aside does not
  conjugate here, because an argument is a one-off invocation while a scope is a declaration, so a
  lock rebuilt from half of one is a wrong artifact that reads as whole. The guard stays above the
  selection; the census compares the lock's bytes across a refused refresh
  (`a_refresh_through_a_parked_root_leaves_the_lock_standing`).
- **`cluster status` states** every suspended root it reaches **in the conditional, naming the act each one
  turns away** (`apply would be rejected`, `cluster refresh would be rejected`). A report performs nothing,
  so `set aside` and `rejected` are both false of it, and a reader whose next step has just been
  withheld needs the act that is blocked. A root reached more than one way is stated **once**, with
  both acts in the verb and `<what>` by precedence place > destination > origin — the dedupe is
  honest only because the verb carries what it drops (`cli/cluster.rs::manifest_door_lines`).
- **A step is withheld exactly where that step's own act meets a suspended root**, and **the two acts meet
  different ones**: `apply` transfers from the lock's entries into the destination and reads no
  recorded scope, while `cluster refresh` re-queries from that scope and is stopped by neither a
  suspended entry nor a suspended destination. Both are rows of the decision table below, so the
  interface never re-tests the causes
  (`a_step_is_withheld_exactly_where_its_own_act_meets_the_door`). One qualification: `apply` asks
  about suspended origins on the entries `--root` left it, while the report asks over all of them, so a
  *narrowed* apply can run where the report offered nothing.
- **A next step is offered where it is the act that brings an apply within reach**; otherwise the
  cause that withheld it owes the way back, and only a **suspension** discharges that by itself — its
  line carries `canon roots unsuspend`. Suspended entries with an unsuspended scope and destination are that case
  — a rebuild leaves them out and the apply runs. A suspended destination is not, whatever else is
  suspended: no rebuild changes where the files are going, and printing one under a
  suspended-destination line reads as if a refresh addressed it. Narrower than the bullet above,
  deliberately — **and it reaches only what the report knows, never what a query will match**. A
  suspended destination is a structural fact. What a rebuild would *select* is not: a refresh
  re-queries unsuspended roots, so a lock wholly on suspended roots may rebuild into an applicable
  one or into nothing, and a predicate over the lock answers the wrong way in both directions.
  Where the answer needs the selection this report does not run, it names the act that runs and
  lets the refresh state its own outcome
  (`a_lock_wholly_behind_a_door_is_still_pointed_at_the_rebuild`). A lock is a pure function of the
  query, the scope and the index, so a rebuild rebuilds from the same query and scope — over the
  world as it stands then, which after a partial apply is not the world it described before.
- **`apply` refuses at either end**, before the summary and the confirmation — the position rule,
  which a check inside `plan_apply` would break, since planning runs *after* the prompt. Nothing is
  written and no decision row is opened: the recorder starts inside the transfer op, which a
  refused run never reaches. `--dry-run` and `--resume` inherit both gates by position.
- **The `<what>` slot speaks one vocabulary across the three surfaces**, in two shapes: a **place**
  (the path, `here`, `N paths`) or a **role** in the act (`destination`, `origin of N sources`). A
  scope line is a place; the origin end and the destination are roles. Composed in `scope.rs`
  (`place_reach`, `origin_reach`, `DESTINATION_REACH`), so refusal and report use the same words.
- **A refresh through a suspended *destination* is not refused**, and neither is an **apply** whose
  recorded scope names a suspended root while its entries stand elsewhere — the deliberate edges of
  the rule above, and `cluster status` states every such suspended root whatever it withholds.
- **`cluster generate` reads no `parked()` register**, and needs none: its prefixes arrive from
  `core::ops::scope::resolve_scope`, which refuses a wholly parked scope and sets a parked path
  aside before this command is called, so the register is empty by construction.

A **refresh writes `CURRENT_MANIFEST_VERSION`** rather than carrying the old number forward: it
rewrites the whole document, so a v1 stamp on the v2 body it emits would be false.
`ManifestOptions.allow: Vec<String>` stores `--allow` and is always written; `cluster refresh` reads
it (there is no `--allow` on refresh). `--show-archived` is CLI-only — output verbosity, not stored.

**Comment sections**: `# === Cluster Summary ===` (regenerated on refresh, in the order
`cluster generate` prints: the count, the plan's note lines under it — they describe the run that
wrote the block — then the roots and the unarchived count; `# 0 sources` with no roots line when
nothing matched). `# === Notes ===`
(empty on generate, **preserved verbatim on refresh** — extracted via string matching since TOML
strips comments; `extract_notes_raw()` keeps `#` markers, `extract_notes()` strips them for the
decision reason). **The section markers are a split contract**: the writer lives in
`ops/generate.rs`, the parser in `domain.rs`, and the literal has no single home — so a round-trip
test spans both sides, reading generate's output back through the parser, and a change to one half
the other does not follow is a test failure rather than silent rot. **One assembly, every rewrite**:
`assemble_manifest()` composes the whole document — header, Cluster Summary, Notes, TOML body, fact
help — for generation and for *both* refresh arms, and the round-trip test covers both.

## The status decision table

*Can `apply` run at all, and can `cluster refresh`* is one question with one owner:
`domain.rs`'s `ManifestStanding`, whose rows are the `Obstacle` enum. It was a nest of booleans
inside `cluster status`'s next-step hint, listing four causes where `apply` refuses on more — so
the report invited the one command that refuses, in four separate ways.

- **Each row stops exactly one act**, and which one is the enum's own doc table: the lock's
  states (absent, hash, header and its zone), the pattern's, and the two ends' suspended roots stop `apply`;
  the recorded scope's suspensions, the source-existence policy's terminal rule and the manifest's zone
  (a line this machine cannot read, or no line on a machine whose zone cannot be written down)
  and a query line that cannot run stop `cluster refresh` — read through the refresh's own
  `settle_manifest_zone` and `ops/manifest.rs::parse_recorded_query`, which binds the query as
  `--where` is bound, so every cause reading and binding refuse the recorded text for is
  collected before anything is selected, and the report names no rebuild those turn away.
  A lock's header is one `LockHeaderStanding`, so a lock with no header
  raises its older row and never a zone row; both zone-of-the-lock rows are cleared by a refresh
  that runs, which writes a zone it read on this machine, and both zone-of-the-manifest rows owe
  no way back of their own — the report states them in the refresh's words.
  `ManifestStatus::next_step` and `refresh_refused_by` are **projections** into it, never
  derivations beside it (`the_table_holds_every_row_once`, `each_row_stops_the_act_the_table_names`).
  A projection is carried only where production asks for it: a bare "could the refresh run" was
  minted, lost its last caller to the richer question below, and came out rather than standing as a
  second spelling of it.
- **The refusal answers with its cause, because the causes leave the reader differently
  placed.** `refresh_refused_by` names the suspension, the terminal rule, the manifest's zone or the query, in
  the order `cluster refresh` states them in, and its `None` is exactly `can_run(Act::Refresh)`
  (`the_refresh_refusal_agrees_with_the_predicate`). A suspension, zone or query line announces itself
  with its way back; a scope that kept nothing as `no sources known at <p> — skipped`, naming no
  act — so a remedy falling silent at the first leaves the reader holding something and at the
  second nothing.
- **A query line's way back follows its cause, at refresh and status alike** (`cli.rs`'s
  `query_way_back`): an entry Canon cannot read takes `--edit`; a key holding two types takes the
  conversion its refusal names and then a plain refresh, never `--edit` for a correct query; both
  at once, one line taking both steps. It comes last, after the other causes' own ways back, so
  the lines read as steps — at the terminal rule's exit too, where the scan-or-edit pair stands
  under `no sources known`. Where that rule turns the refresh away first (`refresh_refused_by`; a
  suspension outranks it), status names the query's way back after the scope's two answers
  (`scope_kept_nothing_way_back`), never in the query's block, where the empty scope turns it away. **A refused refresh leaves as `RefreshRefused`** (`domain.rs`, built from its causes so its text
  and its answer cannot disagree), from
  the suspended-scope exit and the zone/query exit: `Error:` stands in its text before the first cause
  Canon cannot read — a zone line always, the query block where a cause in it is not an answer —
  never before an answer, and `main` prints it as it stands, on stdout where it is an answer. Why:
  a joined string cannot tell `main` which. The "no sources known" exit stays a raised error, as at
  the argument door. Pinned by `a_refused_refresh_names_the_way_back_by_cause`
  (`tests/typed_values.rs`).
- **Two cells are unrepresentable; the third only looks like them.** `LockStanding` is an enum
  because two of a lock's claims are about a file somebody *read*: with no lock there is no
  hash to mismatch and no header to be missing, so those pairs cannot be posed at all. "No lock
  *and* a pattern leaving sources with no place" reads like a third and is not — `pattern` sits
  beside the enum, so the pair is constructible and only `compute_manifest_status`'s derivation keeps it from
  arising. Both halves are pinned where they hold: the shape's
  (`a_manifest_with_no_lock_raises_exactly_one_obstacle`, which poses the constructible pair
  rather than assuming it away) and the derivation's
  (`an_absent_lock_is_a_state_the_read_states`, over a real manifest whose pattern places nothing
  and whose lock was removed). A pattern `cluster status` cannot read and a `base_dir` leaving the
  archive root are rows of their own (`PatternUnreadable`, `BaseDirNotContained`): each stops
  `apply`, no rebuild leaves either, and each is stated with its own way back.
- **A rebuild is named where it would run and would clear every obstacle in the apply's way**. One
  row answers conditionally: a pattern that will not expand is cleared only where the lock predates
  the measurement *and* the recorded scope confirms something to measure from — otherwise the
  rebuild changes the stated reason and nothing else
  (`a_rebuild_that_supplies_no_measurement_is_not_a_step`). The expansion failure's own text
  names no remedy at all, by the expression facility's own rule — it sees the lock's reason and
  never the manifest's, so the remedy is this table's to give or withhold and the report does
  not contradict itself one paragraph apart. **Declared open**: the table cannot tell whether
  the pattern *also* names a fact nothing supplies, so where a rebuild would supply the
  measurement it answers "clears" without knowing.
- **What the table does not enforce**, named rather than discovered: `apply`'s further refusals —
  a non-archive destination, unhashed entries, every preflight finding —
  are not raised from it. Each row's doc names the `apply` site that enforces it; a new apply
  refusal with no row is not build-refused.
- **An absent lock is a state, not a failure.** `read_lock_file` returns `None` for a file that is
  not there and an error for every other failure — absence is not unreadability. Canon writes the
  state itself: a zero-match refresh removes the lock and records the empty hash, so the report
  states which of the two absences it is, from the manifest's own record, and reads on
  (`an_absent_lock_is_a_state_the_read_states`, `an_unreadable_lock_is_still_an_error`); the two
  sentences themselves are `each_absent_lock_is_named_by_its_own_cause`'s, since naming the wrong
  one sends the reader to the act that cannot help. Each absence also names its step where the
  refresh would run: the rebuild for a lost lock, and for an empty
  selection the table's `OwedWayBack::ChangeTheSelection` — a refresh over the same query
  selects nothing again
  (`a_lock_that_matched_nothing_is_not_sent_back_to_the_same_query`). `apply`
  refuses by name instead, straight after reading the manifest and in one sentence for both
  absences, pointing at the report: it exists to act and cannot, and infers nothing about why.
- **One spelling for the remedies naming a command.** `cli.rs` owns `refresh_command`,
  `apply_command`, `status_command`, `scan_command`, `rebuild_remedy`, `missing_lock_refusal`,
  `empty_selection_remedy`, `source_lost_remedy`,
  `scan_or_edit_scope`, `scope_kept_nothing_way_back` and `adjust_pattern_remedy`. The **quoting belongs to the spelling**, not
  to each sentence — but the
  quoting itself belongs to neither: `core::domain::format::shell_quote` owns it, and
  `ops/plan.rs` already ran manifest paths through it for this very command. A local spelling
  handling only spaces emitted an unbalanced quote for a path carrying an apostrophe, so the rule
  is *route through the owner*, and the pin walks both shapes
  (`every_remedy_quotes_a_path_that_needs_it`). That pin sees only the sentences it is handed,
  which is a rung too low for a defect that arrived twice: three `canon apply --resume` lines
  pasted a path of their own where no pin reached them. So the bypass is refused structurally as
  well — every spelling of one of these commands with something pasted into it lives in `cli.rs`,
  and there are exactly four
  (`a_command_is_spelled_with_a_path_only_where_it_is_quoted`, matched both directions). `source_lost_remedy` takes a
  `RefreshStanding` and asks *would a refresh run, and if not, why* — the refusals are kept
  apart because only some state their own way back in the line that announces them (a suspension; a zone
  or query line, in the refresh's words); a scope
  that kept nothing is announced with no act at all, so that arm names the same pair `cluster
  refresh` itself prints when it refuses for the same reason
  (`the_source_lost_remedy_gives_each_refusal_its_own_way_back`). `Unasked` is `apply`'s: apply carries out a
  lock already built and checks it still holds, resolving no recorded scope, and a manifest may
  record none — so its remedies speak about the lock, and a refresh named from there that is
  turned away is answered by the refresh, which states its own outcome and the acts for it.
  `rebuild_remedy` asks nothing: all three of its callers are apply's, all `Unasked` by
  construction.
  **`scan_or_edit_scope` offers two acts, not one**: a set-aside path is a directory Canon has no
  sources under, and nothing on disk tells an unscanned directory from a wrong one — so the scan
  leads, naming the very paths the report has just skipped, and the scope edit follows
  (`a_scope_that_kept_nothing_is_offered_the_scan_before_the_edit`). At `cluster status` the pair
  is led by a sentence binding it to the skipped line, and lead and pair together are one answer,
  `scope_kept_nothing_way_back`, read at both of the report's sites — its next step and the
  source-lost remedy; at `cluster refresh` the pair stands alone: the refusal prints it under its
  own `no sources known at …`, where a lead repeats what the reader just read, while the report
  prints it at its foot, below a summary ending `All source files accounted for.` — true of the
  lock's sources, silent about the scope, and read as their subject unless a line says otherwise.
  The spellings that stay outside `refresh_command` each name a placeholder or speak from a
  stratum an interface composition cannot reach, and are listed rather than counted — the list is
  prose-held, since the structural pin above refuses only a spelling that pastes a path, which
  none of these does: `ops/plan.rs`'s staleness remedy and its unhashed-sources precondition (an
  operations stratum); `apply`'s recovery recipes, whose spellings name `<manifest>` and paste
  nothing, so there is nothing to quote; `ops/generate.rs`'s manifest header, which writes the
  command into every manifest as a line to *learn*; `domain.rs`'s version-gate refusal, which
  names `--edit <manifest>` for a manifest that will not parse, from the domain stratum; and
  `cli/cluster.rs`'s generate-collision message, which names the command as the alternative to
  `--force`.
- **A withheld step still answers.** Where no act brings an apply within reach, the reader holds a
  cause and no act — honest only where the cause was announced with its own way back, which a
  suspension is and the others are not. Which sentence each owes is the table's, not the
  report's: `owed_way_back` ranks the causes once, a scope that kept nothing outranking a pattern
  that cannot expand because it is *why* the pattern has nothing to measure from, and the report
  matches its four answers with no rest arm
  (`a_withheld_step_is_never_left_without_an_answer`, walked over every shape the table can take,
  every input varied, the one pair construction rules out skipped by name). **The claim reaches the table's answers and no further**: the
  interface's outermost gate — nothing pending and no entry lacking a destination over a lock that
  is there, or a source
  lost, or a destination of the wrong size, or one of the right size that no row records — returns
  before the table is asked, and a report stopped there names no act of its own accord. For a lost
  source the line above has already said to check the volume; for a wrong-sized destination
  nothing has, and `apply --resume` on the same manifest holds the look-before-deleting guidance
  the report withholds. That gap is the gate's, older than the table, and stands. The unrecorded
  destination's line is the exception: it carries its own act, `apply --resume`, because the gate
  would otherwise leave it unnamed, and names it only where `standing().can_run(Act::Apply)`.
  The pattern arm and `apply`'s pattern refusal are one composition, `adjust_pattern_remedy`, so
  wherever both reach it they say the same words. Both reach it only over a lock carrying a zone
  this machine reads: `apply` refuses any other before it evaluates the pattern, and the report
  places none of its entries (below), so for such a lock both name the rebuild and the pattern
  is met after it. The table's rule for the **pattern row** is a conjunction: a rebuild clears an
  unexpandable pattern only where the pattern needs a scope the lock lacks *and* the recorded
  scope confirms something to measure from; the lock's own rows (no header, no zone, a zone not
  known here, hash mismatch, a lost lock) and suspended origins are cleared by a rebuild as before.
  A pattern failing for a fact is cleared by no rebuild, and a lock's age is stated on the
  `Lock:` line in the words `apply`'s refusal will use (`a_locks_age_is_stated_on_its_own_line`)
  — a raised obstacle the step does not answer is never left unspoken.
  **Each source with no place is answered by its cause.** `ops/pattern.rs`'s `PatternGaps` records
  every cause a run met with how many sources met it, read off the facility's typed
  `expr::NoPlaceCause`, never off a sentence's wording, and each way a destination would leave its
  directory. `adjust_pattern_remedy` matches every cause with no rest arm and composes one line per
  cause met, each naming the refresh as a command for the manifest, so the step withheld above
  always has an answer (`the_pattern_remedy_prints_one_pair_per_cause_met`). A pattern placing files
  relative to a scope the **lock** was built without is repaired by `{source.rel_path}` or by
  declaring a scope and refreshing, and never by checking fact coverage — the scope key is not a
  fact, so no import supplies it; a pattern naming a fact no source carries is repaired only by
  the fact. A value that is absent or too coarse is left out through its own query entry
  (`NoPlaceCause::query_entry`), a save time through `source.mtime?`, or corrected; a coarser
  modifier needs no refresh, being read at the next apply; a value that cannot be a name, and a
  destination leaving its directory, are met by an edit to the pattern. The
  condition is the lock's, never the manifest's: a manifest may declare a scope its lock has not
  seen, and there the rebuild is the step — the table reads the same lock answer to decide it
  (`a_declared_scope_the_lock_has_not_seen_is_rebuilt_into_it`; the table once read the header's
  presence instead and told such a manifest it declared none). Which failure it is is the
  lock's answer, `ops/manifest.rs`'s `LockFile::pattern_needs_a_scope_it_lacks`, asked once of
  the whole file at every reader and never of the run's `--root` subset, which could answer
  differently from the file it was cut from
  (`a_pattern_needing_a_scope_is_asked_of_the_whole_lock`), and carried into the table as its
  own input; its pattern half comes off
  `expr::BoundPattern::names_scope_rel_path`, the facility's own reading of the pattern, never off a
  sentence's wording — a caller matching on either would hold a second copy of a language rule
  (`a_pattern_that_cannot_expand_names_the_edit_and_no_command`, which asserts each answer absent
  from the other's arm). Where both fail at once the scope answer leads, the scope having to be
  settled either way.

## Conventions

- **The zone is manifest intent, and the lock carries it.** `cluster generate` records this
  machine's zone in the manifest's editable `zone` line and the lock's header, refusing a zone
  with no name to record; `cluster refresh` settles the line (`domain.rs::settle_manifest_zone`,
  which `cluster status` reads too), selects on it and writes it into both files; `apply` and
  `cluster status` read times in the lock's zone (`ops/manifest.rs::LockFile::zone`), never the
  machine's. An unreadable line is one refusal among a refresh's collected causes, never a parse
  error. A lock with no zone this machine reads is refused by `apply` before the plan and the
  decision row, and `cluster status` computes no destination and states no counts for it — a lock
  recording no time zone gets no destinations, and a zero nobody measured is not said. Zones compare parsed, never as text (`RecordedZone::same_zone`). Pinned by
  `apply_reads_times_in_the_locks_zone_whatever_the_machines` and its siblings at each reader.
- **Lock-file test machinery shared across strata lives beside the format it writes**:
  `ops/manifest.rs::fixtures`, a `#[cfg(test)]` module, serves the tests of `ops/status.rs` and
  `cli/apply.rs`, which stay in-file with the code they exercise. The root's externalized `tests/`
  directory is per stratum, and two strata share this helper.
- **The manifest's base_dir is read as a destination** (`cli/apply.rs`, via
  `core::domain::path::ContainedPath::new`): absolute, `..` and null-holding values are refused
  before any plan is computed — an absolute value replaces the archive root in the filesystem join, a
  `..` one walks out of it. A `.` is read as the operating system reads it, and what is used
  is the path rebuilt from the parsed names.
- **`cluster status` classifies each entry once, in ops** (`EntryStatus`, `classify_entry`): the
  counts, the concerning-entries table, its destination column, and the missing-source warning all
  read that one field. The interface must never re-spell the classification — a second predicate
  admitting a wrong-size destination whose source was gone makes the header count N and list N+1.
  Guard: `every_entry_lands_in_exactly_one_class`.
- **The archive-conflict check runs last in `plan_apply`, over the trimmed transfer list** — in
  resume mode the list has been narrowed to what is still pending, and the check must not see the
  rest: an interrupted run's own placed files are registered in the destination archive, so checking
  them would abort the resume that is finishing them. Pending transfers are checked in full, so
  `--resume` is not a way past the duplicate gate (which is also why the interface's gate stays
  unconditional). Guard: `resume_never_reads_its_own_progress_as_an_archive_conflict`.
- **A destination copy wins the archive-conflict classification**: the archive-info fetch is ordered
  by root id, so content standing in both the destination and an older archive must be classified by
  the destination copy — otherwise `--allow` for cross-archive duplicates bypasses the destination
  gate.
- **Apply is non-transactional by design** (the Provenance Write-Path Atomicity ADR): the filesystem
  cannot roll back alongside the database, so recovery is fix-forward. A decision is opened before
  the first transfer and closed with the outcome; the receipt names only the transfers that
  completed; an interrupted run leaves both saying so. Do not "fix" this by wrapping the transfer
  loop in a transaction.
- **The lock file is written, then hashed, and the hash is embedded in the manifest.** Any other
  order records a hash of bytes that were never on disk, and every later apply refuses the pair.
  Both files are synced all the way to disk (`write_and_sync` for the manifest, `write_lock_file`
  for the lock), so the durable manifest cannot name a lock that did not survive beside it — don't
  reopen that window by writing the lock through a plain buffered writer.
- **`repo.rs` holds the SQL apply performs** — registering a file that has landed in an
  archive, re-pointing a source that was moved rather than copied, reading the decision that last
  stood at a destination path, and the preflight asking which destinations are already occupied
  and by which content (`batch_objects_at_paths`; the existence check is its key set, so the query
  is written once; `cluster status` makes the same read). Nothing else in Canon does these
  things. `fetch_by_path` and `BATCH_SIZE` are reached from the
  shared source repository; the three test fixtures are **duplicated** rather than shared, because
  the tests that stayed there use them too.
- **The contentless law applies to apply's archive conflicts and to cluster's skip set**, via the
  archived-ness SQL — empty files travel with their directories rather than being skipped as "already
  archived", so a verbatim directory copy stays faithful. It applies to **generate's duplicate gate**
  too (`find_source_duplicates`, through `is_contentless_size`): a duplicate is an identity claim,
  so empty files never form a group and never refuse the run; they stay ordinary lock entries,
  nothing set aside. It does not apply to apply's
  path-collision checks (existence, not identity). See the law's block in the top-level CLAUDE.md.
- **`ops/receipt.rs` exists to hold apply's two receipt document shapes** — a seventh stratum for
  two structs, because neither alternative works: `domain.rs` is illegal for them (`ApplyReceipt`
  holds `ReceiptMeta` from the ops layer, which domain may not reference), and `ops/execute.rs` is
  already the subsystem's largest file. Only `[meta]` is shared with other commands' receipts;
  `write_receipt` is generic and never inspects a body, which is what lets each command own its own.
  It also holds the **extraction ledger's round-trip corpus** — tests that run a real apply and then
  reindex it, proving a backfilled row is indistinguishable from a forward-recorded one. They cannot
  live beside `reindex_extractions` in core, because they name the transfer types and core may not
  name a subsystem; they exercise nothing in this file, and are here because this is the smallest
  file in the subsystem that can hold them.
- **The three destination preflight helpers live in `ops/plan.rs`, not the shared filesystem layer**
  — `check_destination_writable` (is the destination writable, or its nearest existing ancestor),
  `ensure_parent_dir`, and the ancestor-collision family (`check_ancestor_collisions`,
  `ancestor_directories`, `blocking_file`: a file standing where a destination's directory has to
  go). Apply is the only caller of any of them, and all answer the question this module already
  owns: what stands in the way. `ensure_parent_dir` is reached from `ops/execute.rs` along the
  existing `execute → plan` edge, so the DAG is unchanged. Note `ensure_parent_dir_existing_noop`'s
  real guard is its `unwrap`, not its assertion: it pins the `create_dir_all`-over-`create_dir`
  choice, and swapping them fails it — the assertion alone would pass against anything.
- **A staleness site owns its headline and its retry, and nothing else** —
  `ops/plan.rs::staleness_lines` composes the listing, the truncation and the remedy;
  `cli/apply.rs`'s preflight refusal, `cli/apply.rs`'s during-run skip display and
  `ops/execute.rs`'s pre-transfer bail each print their own first line, then that body. Two things
  differ in kind and so stay with the sites. The **headline**: two of these are refusals and one is
  a report of what a run passed over. The **retry**: the two refusals transferred nothing, so
  re-observe-refresh-reapply is their whole story, while the skip display reports a run that already
  moved files — a plain re-apply there would collide with everything it placed, so that site appends
  `canon apply --resume`. Everything between is shared, which is what lets one test pin what all
  three say. The body lives in ops because the third printer *is* ops: it assembles its message
  before `bail!` carries it out of the layer, and cannot reach an interface helper. `STALE_SHOWN`
  keys both the truncation and the remedy arm, because the guarantee is that a list the user can see
  in full is a list the remedy can hand back; past it the whole-root remedy stands alone, since a
  command naming *some* of the stale files fixes nothing and reads as if it fixed everything. Only
  the bail's output is reachable from a test
  (`the_pre_transfer_bail_prints_the_shared_staleness_body`); the two `cli` printers speak through
  `eprintln!` and are unpinned in-process — a stated gap, not a covered one.
- **A resume registers a row only for what the disk is shown to hold.** `classify_resume_entries`
  stays pure filesystem; `plan_apply` then reads each right-sized destination in two steps. A
  present row with the entry's own object settles it with no disk read: counted as at the
  destination, nothing written (`row_vouches_for`, which `cluster status` asks too, so a report
  never says "accounted for" over what `--resume` would turn away). Anything else (no row, a
  tombstone, another object, no object) has its destination hashed against the lock: equal is
  *proven*, and `execute_apply` registers it from the file's own metadata and records it in this
  run's receipt and extraction ledger, since no earlier receipt lists it; different is refused
  before any transfer, beside the size refusal. Verifier
  `a_same_size_destination_with_other_content_is_refused`; the two readers agree in
  `status_and_resume_agree_about_a_same_size_destination`; the record is
  `a_proven_entry_is_in_the_receipt_and_the_ledger`. **Where it does not apply**: a row that vouches
  is believed, so bytes that changed behind a correct row are not seen; an entry with no recorded
  hash reads as different; and a proven entry's receipt item carries the run's origin disposition,
  though a resume never removes an origin that still stands.
- **A relocation re-points the origin's row only where no row holds the destination.**
  `record_relocation` asks `source_exists_at_path`, which counts tombstones: the set
  `UNIQUE(root_id, rel_path)` enforces. Where a row stands it clears the origin and registers the
  destination through `insert_destination`, which settles any row at the path, the route a copy and a
  cross-device move already take. Preflight asks only for present rows, so the write may not assume a
  free path. Every index step after a placement reports through `index_not_updated`, which says where
  the file now is and that a resume registers it. Verifiers `a_move_onto_a_deleted_path_succeeds`, and
  `a_relocation_with_no_row_at_the_destination_keeps_its_one_row` as its allowed case. **Where it does
  not apply**: scan's moved arm deletes a tombstone at the path and re-points the row by its own
  code in `scan/repo.rs`, a separate spelling.
- **Ctrl-C: the interface installs, ops reads, the interface answers.** `cli/apply.rs` calls
  `execute::setup_interrupt_flag` just before `execute_apply` — after the confirmation, so a press at
  the prompt keeps its default — and passes the flag; `None` in `ApplyExecuteParams` means no signal
  handling, and `ApplyOptions::handle_interrupts` is false for the interface tests that call `run`, so
  those tests never register one (the callers are listed exactly by
  `only_the_interface_and_the_signal_test_install_the_handler`). The two registrations in `setup_interrupt_flag` are
  order-bound: the conditional default first, then the flag, so the first press arms and the second
  ends the process (pinned by `a_first_interrupt_sets_the_flag_and_a_process_survives_it`, the one
  test that raises a real SIGINT, once). Both sweeps and the loop read the flag; a press before the
  first transfer ends the run with nothing moved or recorded (the entries a resume proved included;
  the next resume proves them again), settled `interrupted` with no receipt, and a sweep
  that stopped early refuses nothing, its list being partial. Ops cannot print and the loop reads the
  flag only when a file ends, so the first press is answered at once by a thread in the interface
  that also states the cost of a second one: the receipt is written after the loop, so a hard stop
  writes none. `apply_command` takes the run's `TransferMode`: a line that finishes a stopped
  `--move` must say `--move`, or it runs as a copy.
- **Extraction rows are written by `ops/execute.rs` right after the transfer loop**, through the
  same `build_extraction_rows` helper the ledger's backfill path uses — sharing the code, not just
  the shape, is what makes a backfilled row indistinguishable from a forward-recorded one.
- **`cli/apply.rs`'s violation blocks repeat a rendering shape with no shared helper** — most of
  them the full count/first-N/ellipsis/remediation form, the rest count-only or listed in full. The
  *gate* is correctly interface; only the renderer wants extracting. Recorded, not scheduled.
- **An apply reports three per-source disk sweeps, and the boundary between the last two is
  load-bearing** — planning's existence/readability sweep (`ops/plan.rs`, silent in resume mode),
  then execute's readability sweep and its lock-agreement sweep, which stat and read every source's
  head. Each is a round-trip per source, which is what the reporting is for. The last two are not
  to be fused into one pass, however tempting the single count is: the readability sweep exists to
  refuse an unreadable source *before* any content is read, and fusing them would make that refusal
  cost a full head-read pass on a network volume — error precedence would survive, cost precedence
  would not. `ValidationSweep` names which sweep is running; ops never spells the label.
- **Two progress traits with one shape, deliberately not unified** — `TransferProgress`
  (`ops/execute.rs`) and `PlanProgress` (`ops/plan.rs`). Unifying them means a supertrait, and a
  supertrait would break what makes the validation methods safe to have added at all: they carry
  default no-op bodies, so an implementation written before the sweeps had a voice still compiles.
  `NoopProgress` in `ops/execute.rs`'s tests is the standing proof — it implements only the four
  transfer-loop methods, so giving any validation method a body in the trait breaks the build. The
  DAG forbids the other direction anyway (`plan` may not name `execute`). Relocate on a third sweep,
  or when the two CLI implementations diverge in more than their labels.
