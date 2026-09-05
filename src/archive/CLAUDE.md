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
  cli.rs          # mods, plus the one remedy sentence both commands print
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
  repo.rs         # archive's own SQL — the four writes and reads only apply performs
```

**The internal dependency direction is a DAG, and the split is what keeps it one**:
`execute → plan → pattern`; `execute → receipt`; `generate → manifest`; `status → manifest`,
`status → pattern` (a function-local `use` inside `compute_manifest_status`). `pattern`, `manifest`
and `receipt` are the sinks, pointing at nothing above `domain`. An edge from `plan` to `execute`
would undo the separation the split exists to hold.

**One exception, and it is test-only**: `receipt.rs`'s test module imports `execute` and `plan`
(the transfer types) for the extraction ledger's round-trip corpus, which runs a real apply. Those
imports sit inside `mod tests`, so production `receipt.rs` is still a true sink — but the edge is
real and no build rule catches it, because reaching within one's own subsystem is legal at any
depth, so prose is the only guard here. `execute.rs` would have been the zero-edge home for that
corpus, and is already the subsystem's largest file — the same reason `receipt.rs` exists at all.

**The barrel** is `run`/`ApplyOptions` and `generate`/`refresh`/`status`/`GenerateOptions` from
`cli`, `TransferMode` from `ops::execute`, and `plan_generate`/`ClusterGenerateParams`, which have
no production caller and serve the contentless-law canary. Those two carry
`#[allow(unused_imports)]` with a comment naming the consumer; `TransferMode` stays on its own
un-allowed line, because it has a real production caller (`main.rs`'s dispatch) and must not hide
behind an allow that would mask a genuine unused-import signal. **A test-only re-export is a real
cost** — indistinguishable at the barrel from a production surface — so a test that needs the
strata moves inside the subsystem rather than the surface widening to reach the test.

## Cluster/Apply Workflow

`cluster generate` selects sources (`batch_fetch_by_roots` + domain predicates), batch-fetches
facts, computes 100%-coverage facts in-memory; the lock file stores **source identity, staleness
data, and where each file goes (no fact snapshots)** — a fact can be looked up fresh and the
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
because that is what it is. `de_scope` is the **only** place the field is read, and it accepts
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
**not** a serialized `DecisionScope` — that type's guarantee is that constructing one requires a
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
answers it from the header's presence, and the three readers hand that `expr::Unmeasured` to
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
folder since moved, naming a root since removed. Generate, refresh and status all resolve it through
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
  narrow the user's own file. A refresh whose scope kept **nothing** refuses, naming every set-aside
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
- **`cluster status` carries both categories out** on `ManifestStatus.unrooted_scope` and
  `set_aside_scope` and lets the interface print them; `all_accounted_for()` is untouched, since its
  claim is about source files and is still true. Its **next-step hint** names the way forward
  *unless apply cannot run at all* — two states, and never an unresolvable scope line:
  `lock_predates_measurement` gets the rebuild, and `pattern_unexpandable` gets **no step**, because
  no one command supplies what a pattern is missing; the report states the reasons in their own
  block instead, which is what makes the silence honest. Whether there is a step is never the
  scope's business: a manifest with nothing pending, or with content missing, has no step to offer,
  and sending either to a refresh clears the lock for nothing.

The recorded-scope resolution itself, and its pins, live in `src/core/CLAUDE.md`.

A **refresh writes `CURRENT_MANIFEST_VERSION`** rather than carrying the old number forward: it
rewrites the whole document, so a v1 stamp on the v2 body it emits would be false.
`ManifestOptions.allow: Vec<String>` stores `--allow` and is always written; `cluster refresh` reads
it (there is no `--allow` on refresh). `--show-archived` is CLI-only — output verbosity, not stored.

**Comment sections**: `# === Cluster Summary ===` (regenerated on refresh). `# === Notes ===`
(empty on generate, **preserved verbatim on refresh** — extracted via string matching since TOML
strips comments; `extract_notes_raw()` keeps `#` markers, `extract_notes()` strips them for the
decision reason). **The section markers are a split contract**: the writer lives in
`ops/generate.rs`, the parser in `domain.rs`, and the literal has no single home — so a round-trip
test spans both sides, reading generate's output back through the parser, and a change to one half
the other does not follow is a test failure rather than silent rot. **One assembly, every rewrite**:
`assemble_manifest()` composes the whole document — header, Cluster Summary, Notes, TOML body, fact
help — for generation and for *both* refresh arms, and the round-trip test covers both.

## Conventions

- **The manifest's base_dir is gated at plan time** (`plan_apply` precondition, via
  `core::domain::path::rel_dir_escapes`): absolute or parent-traversing values are refused before
  any plan is computed — an absolute value replaces the archive root in the filesystem join, a `..`
  one walks out of it while the recorded paths still claim archive-relative. The per-transfer escape
  check compares components without normalising and is not a second guard for either.
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
- **`repo.rs` holds the SQL only apply performs** — registering a file that has landed in an
  archive, re-pointing a source that was moved rather than copied, reading the decision that last
  stood at a destination path, and the preflight asking which destinations are already occupied.
  Nothing else in Canon does these things. `fetch_by_path` and `BATCH_SIZE` are reached from the
  shared source repository; the three test fixtures are **duplicated** rather than shared, because
  the tests that stayed there use them too.
- **The contentless law applies to apply's archive conflicts and to cluster's skip set**, via the
  archived-ness SQL — empty files travel with their folders rather than being skipped as "already
  archived", so a verbatim folder copy stays faithful. It does **not** apply to apply's
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
- **Extraction rows are written by `ops/execute.rs` right after the transfer loop**, through the
  same `build_extraction_rows` helper the ledger's backfill path uses — sharing the code, not just
  the shape, is what makes a backfilled row indistinguishable from a forward-recorded one.
- **`cli/apply.rs`'s violation blocks repeat a rendering shape with no shared helper** — most of
  them the full count/first-N/ellipsis/remediation form, the rest count-only or listed in full. The
  *gate* is correctly interface; only the renderer wants extracting. Recorded, not scheduled.
- **An apply reports three per-source disk sweeps, and the boundary between the last two is
  load-bearing** — planning's existence/readability sweep (`ops/plan.rs`, silent in resume mode),
  then execute's readability sweep and its lock-agreement sweep, which stat and read every source's
  head. Each is a round-trip per source, which is what the reporting is for. The last two are **not**
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
