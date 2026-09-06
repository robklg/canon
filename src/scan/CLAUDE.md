# scan/ — the observe→reconcile→persist pipeline

Populates Canon's index from disk: each file is observed, reconciled against DB state, and
persisted; deletion is detected by difference across a whole walk rather than per file. Scan
observes rather than performs, so it holds no plan/execute pairs, no render seam, and no
scan-wide transaction (the Provenance Write-Path Atomicity ADR) — recovery is fix-forward.

Sealed behind a barrel of `run` and `find_candidates`, both consumed only by `main.rs`, plus
`same_physical_file` — the physical-identity law — with its parameter types `IdentityClaim` and
`FileObservation`, which only the contentless-law canary reaches: the relocation refusal is the
law's one contentless arm, and no command surface states it. `domain`, `repo` (nested
`source`/`root`/`fact` modules), `ops` and its strata
(`types`/`pipeline`/`receipt`/`candidates`/`hash`) and `cli` are private or `pub(super)` at most,
visible only within `scan/`. Scan has no sibling edge in either direction: no subsystem consumes
scan, scan consumes no subsystem.

## Scan Pipeline

Per file: **Observe** (`FileObservation` from disk) → **Reconcile** (pure — `domain::reconcile_at_path()`
when a row stands at the path, `domain::reconcile_pathless()` when none does) → **Persist**
(`scan::repo::source::apply_reconciliation()`). Domain types in `domain.rs`.

**Identity is spoken once**: `domain::same_physical_file()` — the physical-identity law, graded
by `IdentityClaim`, verified by `domain::the_law_reads_content_evidence_never_device_or_inode`.
It reads content evidence only (size+mtime, and the observed partial hash when the caller holds
one); **device and inode are never read there**. Their job is nomination, and `Relocation`
additionally refuses a contentless candidate — vacuous evidence never moves a row. Both
reconciliation arms consume the predicate; neither restates its rules. The pathless arm is the
only site that asks it at `Relocation` grade, and it asks twice for different purposes: once to
decide whether a vacated candidate may be claimed, and once to decide whether a still-standing
one may be *called* a hardlink companion — a claim about shared content, which a bare inode-number
collision across volumes cannot support.

| Outcome | Condition | Corroboration | DB Action |
|---------|-----------|---------------|-----------|
| `New` | No row at path, and no nomination survived the gates | — | INSERT (or UPDATE a stale row at the path, `basis_rev + 1`) |
| `Unchanged` | Row at path, law satisfied under `SamePath` | fingerprint, plus the partial hash when the inode moved | UPDATE `last_seen_at` + device/inode (the silent refresh) |
| `Modified` | Row at path, law not satisfied | as above | UPDATE metadata, clear `object_id` (content changed, identity unknown until the hash pass), increment `basis_rev` |
| `Moved` | No row at path; a nomination was gone from its own path, corroborated, won the pairing, and was still as nominated at write time | disk truth, then the law under `Relocation`, then the in-transaction re-check | UPDATE path (possibly cross-root), at end of walk |
| `Disconnected` | Empty dir, different device | — | Skip file, don't mark missing (`--ignore-device-id` overrides) |

**A move must clear three independent gates.** Inode nomination only says where to look. **Disk
truth** (`ops::pipeline::check_old_path`) asks whether the nominated file still stands at its own
path — one still standing is a hardlink companion, and the check short-circuits before the law is
consulted at all, so the precedence is a shape rather than a convention held by call order
(`domain::a_present_twin_is_never_a_move_donor`: the twin corroborates perfectly and must still be
refused). It also confirms **which storage answered**: a `NotFound` is absence only when the root
directory's current device equals the row's stored device, because a mountpoint with nothing
behind it is a readable, empty directory under which everything reads as gone
(`pipeline::a_root_whose_storage_is_not_mounted_never_claims_a_move`). **Corroboration** is
`same_physical_file` at `Relocation` grade. The **re-check** re-reads the row inside the write
transaction, because a check outside one answers a question about the past
(`pipeline::a_candidate_mutated_before_resolution_degrades_to_new`). Anything that fails a gate is
a new path, which is the truthful reading.

**Nomination is inode-only, and a suspended root never nominates.** Device is dropped from the
lookup key (a remount renumbers it wholesale, so a device-qualified lookup goes blind exactly when
the whole library looks new); it survives as mount-presence evidence in the disk-truth check and as
a pairing tiebreak it could never have decided alone, and it disqualifies nothing. A suspended root
is the user's own closed door: its rows may testify, but relocating one out is an act the closed
default refuses (`pipeline::a_suspended_roots_row_is_never_relocated`, with an unsuspended control
that does move).

**Pairing is deterministic, never iteration order.** `domain::resolve_moves` sorts observations by
`(root_id, rel_path)` and scores candidates by trailing path components, then stored-device
agreement, then lowest id. The final key component is unique, so no two candidates tie completely —
determinism does not rest on `min_by_key` returning the first minimum
(`domain::resolve_moves_is_identical_under_permuted_input_order`, over the three orders a caller can
perturb). Deliberately nomination-agnostic: a future assisted move-suggestion tool reuses it with
content-nominated candidates.

**Move resolution runs after the walk and before missing detection** — a claimed row must be in the
seen set when `find_missing` computes its difference
(`pipeline::a_claimed_row_is_seen_and_the_unclaimed_one_goes_missing`). A move's write failure
never becomes a deletion either: the donor row joins the seen set rather than falling to missing
detection — structural, stated in the error arm's own comment. Resolution runs deliberately
**above** the mount-stability and
walk-error gates: those gates exist because missing detection *infers* absence, while a move is
positive evidence at every step, so an incomplete walk may still follow a file while refusing to
call anything gone (`pipeline::a_move_still_resolves_when_the_walk_could_not_finish`).

**A path that holds a row is never `New`.** Replacement (same path, new inode — what every
atomic-save application leaves) is a `Modified`, so the report says "updated" for a file the
user edited, and the row keeps its `decision_id` by omission
(`pipeline::a_replacement_preserves_the_standing_decision_id`). `New` reaches a standing path
only through revival, where the row was `present=0`. Both leave a source exclusion standing, and
on an excluded row its `decision_id` with it — a dismissal is undone only by `exclude clear`'s
recorded act, so the row must keep pointing at the judgment that governs it
(`repo::source::test_scan_revive_preserves_source_exclusion`). Not total: a deletion still stamps
an excluded row, so an excluded → deleted → reappeared row stands excluded while citing the scan,
its reason reachable through the receipt chain.

**The head read is conditional**: `ops::pipeline::reconcile_file` computes the observed
partial hash before the at-path decision exactly when the stored inode is tracked (non-zero)
and differs from the observed one. That is the only case where a bit-identical recreation and
a real edit are indistinguishable from metadata. The hash is computed at most once per
observation — one taken for the decision is reused by the write
(`pipeline::a_head_read_is_taken_only_when_the_inode_moved`, which fails in both directions).

**The `Modified` arm clears `object_id`** — a changed path holds no identity Canon can claim until
the hash pass (`repo::source::apply_reconciliation_modified`).

**`basis_rev` only ever increases.** The revive path increments rather than resetting: fact
staleness is read as inequality, so a value returning to one already recorded reads as fresh
when it is not (`repo::source::a_revive_never_reuses_a_basis_rev_it_already_held`). A fresh INSERT
starting at 0 is safe — nothing can predate a row's first indexing.

**Deletion (missing) is detected by difference, not per-file** — a source that was
`present=1` but wasn't seen in the walk (`domain::find_missing(expected, seen)`), gated by
the mount-stability guard (walk-root device unchanged before/after, else missing-detection is
skipped to avoid false deletions on a disconnected drive; the skip is counted in
`ScanStats.missing_detection_skipped` and lands in the durable decision summary). `mark_missing`
sets `present=0` + `last_seen_at`, stamps `decision_id`, and — when receipts are enabled —
captures each deleted source **before the flip** into a source-local deletion receipt at
`<source_root>/.canon-ledger/{id:06}-scan.toml` (`pipeline::scan_root_captures_deletion_before_flip`),
so the stamp-set is the receipt-set. `--missing` (`ops::pipeline::mark_missing_path`) marks a
wholly-deleted folder the same way — same stamp, same receipt path — and refuses a suspended root
(`pipeline::mark_missing_path_refuses_a_suspended_root`): `--missing` is reached through the
canonicalize-failure arm, exactly where a suspended root's path lands, so it does not inherit the
walk path's refusal.

### The hash gate and the debt count

**The hash gate is need-driven, not action-driven, and spoken once**: a present source with no
object is queued by any scan that hashes, whatever its reconciliation outcome. Both queue sites —
the streaming walk and end-of-walk move resolution — ask `ops::pipeline::needs_hash`, and the need
it returns **travels on `FileToHash`** rather than being re-derived, so `basis_changed` and the
backlog count are both the gate's one answer
(`pipeline::the_hash_gate_asks_what_is_needed_not_what_happened`, the whole policy table). Debt
entries carry `old_object_id: None`, so paying a backlog can never trip the unexpected-change
detector; that arm of the `basis_changed` conjugation is therefore unobservable, and the exhaustive
match stands to force a future variant to answer rather than because a test pins it. `--verify`
re-reads regardless and is never counted as backlog. The walk queues a file in exactly one place
(`pipeline::the_walk_has_exactly_one_place_that_queues_a_file_for_hashing` — a property the
behavioral guards cannot pin: it catches a third direct push in the walk, not an `.extend()` nor a
queue built in another file).

**The standing debt is counted after the hash pass, never before** — asked first it would report
the debt the scan was about to clear. The pass and the count are one call
(`ops::hash::run_hash_pass`) so the order cannot drift
(`hash::the_debt_a_scan_reports_is_what_survived_its_pay_down`), and overlapping walked scopes
narrow to the outermost first (`domain::outermost_scopes`): event counters tolerate walking a path
twice, a count of what *stands* does not. The backlog number obeys the same discipline from the
other side — counted where the hash **succeeds**, never where it is queued, because it qualifies
the `hashed` count and must never exceed it.

**The debt is reported as a whole and a part** — how much stands, and how much of that this scan
tried to read and could not. Both numbers come from **one statement over one row set** per walked
scope (`repo::source::count_unhashed`, the failed ids staged into `temp_hash_errors` and
`LEFT JOIN`ed), never from a subset counted alongside its whole: a row that leaves the whole takes
its membership of the part with it, so a file hashed meanwhile by a concurrent scan leaves both,
and one outside the walked scopes enters neither
(`repo::source::the_unreadable_count_is_a_part_of_the_debt_it_qualifies`). The predicate is spelled
**once** into a shared `where_sql`; the two SELECT shapes (join, no join) differ only in their
select list, and the no-join shape exists so the ordinary zero-error path stays the query it was.

## Scan conventions

- **`repo.rs` nests three inner modules, `source`/`root`/`fact`** (not a flat file) — forced
  by two incompatible `insert_object` test-helper signatures across the source and fact
  flavours. It opens no transaction: every fn takes `&Connection`, and the transaction sites all
  live in `ops/`. `SOURCE_COLUMNS`/`SOURCE_FROM`/`source_from_row` and
  `ROOT_COLUMNS`/`root_from_row` are `pub(crate)` in the shared `core/repo/source.rs`
  /`core/repo/root.rs` so `fetch_by_inode`/`create` can map rows without duplicating the mapping
  logic.
- **`observe_file` is the walk's sibling, not a second pipeline.** Every unit that *decides*
  anything is shared with `scan_root` — both reconciliation arms, `persist_file`,
  `flush_unchanged`, `persist_resolution`, `resolve_moves`, `needs_hash`, `action_for` — so a
  file named on its own reaches the verdict a walk of its directory would have reached for it.
  What the two do not share is the dispatch around those helpers: the walk defers moves across a
  whole walk while the file path resolves immediately, and extracting it would restructure
  `scan_root`'s hot loop. What `observe_file` *lacks* is the point: no `expected_ids` fetch, no
  `find_missing`, no mount-stability gate, no `capture_deletions`. The interface picks the sibling
  from one question — is the canonicalized path a directory.
- **The door is asked about once, before the recorder opens a row.** `cli::run` walks the
  resolved scan paths against `core::ops::scope::parked_root_of` ahead of
  `DecisionRecorder::start`, so a scan aimed behind a closed door is refused by name with the
  way back and leaves *nothing* behind — not even the `started` row that would read as a scan
  killed mid-walk. Both arms arrive there: a walk canonicalizes its path, and `--missing` —
  whose whole premise is that the folder is gone — resolves lexically against the known roots,
  which is the same list the decision's scope was built from. The inline check that used to sit
  in the walk loop is gone; a second spelling of one rule is free to drift from the sentence the
  first one speaks (`cli::a_scan_at_a_parked_root_is_refused_before_any_row_is_written`).
  **`--all` never reaches the door**: it selects the active roots up front, so a parked root is
  simply not among what it was asked to scan, and there is nothing to refuse. That is a
  different question from naming a place behind a door, and it is unchanged.
- **A suggestion consults the act's preconditions; it never re-derives them.**
  `resolve_missing_target` is where `--missing`'s two refusals live (under no known root,
  suspended root — the second carried as a typed `DoorRefused` rather than a spelled sentence,
  because the user-facing wording has one owner and the hint is a second consumer that is not a
  screen), and both the act and the interface's hint ask it. Two spellings of one rule
  drift silently, because nothing fails when a suggestion and a refusal disagree — and the
  drift runs toward recommending an act Canon would refuse, or accept and should not. The hint
  adds one question the act deliberately does not ask, whether the root's own path resolves on
  disk: `--missing` takes the user's word by design, but Canon offering the advice is a
  different act from Canon obeying it.
- **`use crate::scan::repo as scan_repo;`** resolves the collision between `crate::core::repo` and
  `crate::scan::repo`, both needed in `ops/pipeline.rs`/`ops/hash.rs`.
- **The deletion receipt's document shape lives in `ops/types.rs`, not `ops/receipt.rs`** —
  the pipeline's result types carry `DeletionReceiptItem` as a field, so `types` is the one
  stratum both `pipeline` and `receipt` reach without an edge against the grain. Only the
  item type is `pub` (`scan/cli.rs` groups deletions by root before handing them to the
  writer); the outer `DeletionReceipt` is `pub(super)`. Only `[meta]` is shared with other
  commands' receipts — `write_receipt` is generic and never inspects a body.
- **The `ops` strata**: `types.rs` (shared result/param types + `current_timestamp`; **both**
  stats types live here, the walk's `ScanStats` and the hash pass's `HashStats`, because the
  pass's whole field list is handed across to the walk's through one guarded carry and the two are
  read together — `hash.rs` already depends on this stratum, so the pairing costs no edge against
  the grain), `pipeline.rs` (the walk: `scan_root`, `reconcile_file`, `persist_file`,
  `flush_unchanged`, `mark_missing_sources`, `capture_deletion_items`, `mark_missing_path`,
  `resolve_missing_target`, empty-dir classification — and beside the walk, `observe_file`, the
  file-grain entry: the same reconcile/persist helpers, no walk and no missing detection),
  `receipt.rs` (`coalesce_by_root` + `write_deletion_receipts`), `candidates.rs` (root-candidate
  discovery, pure filesystem, no DB), `hash.rs` (the hash pass itself, and the scope-bounded count
  of sources still holding no identity that `run_hash_pass` pairs it with). `receipt.rs` stays
  split from `pipeline.rs`: `capture_deletion_items`'s before-the-flip contract is stated in
  comments beside both places `pipeline.rs` performs the flip, so folding the writer in with the
  capturer would separate a law from both places it governs.
- **Three `Transaction::new_unchecked` sites, all in `ops/`, none in `repo.rs`**:
  `persist_file` and `flush_unchanged` (`pipeline.rs`), `hash_files` (`hash.rs`). Each takes
  `&Connection`, not `&mut Db`, and each is per-item `TransactionBehavior::Immediate` — never one
  scan-wide transaction. The debt count's staged temp table adds no fourth: that fill uses **plain
  inserts and no transaction at all** (`repo::source::count_unhashed`'s staging), because this
  stratum opens none and a temp table is private to its connection — there is no observer a
  half-filled one could mislead, and SQLite does not fsync the temp database, so the per-row loop
  costs no durability round-trip. The shared `populate_temp_sources` was deliberately **not**
  widened to `&Connection`: it has one consumer, and core entry wants two.
- **`partial_hash` is computed outside the transaction** (`reconcile_file`, before
  `persist_file`/`flush_unchanged` open theirs) — filesystem I/O on NAS/network storage must
  never hold the write lock.
- **`UNCHANGED_BATCH_SIZE = 500`**, flushed both mid-loop (inside `scan_root`, once the buffer
  fills) and once more after the loop ends — unchanged files never wait for the next batch to
  fill before landing.
- **Decisions in the pipeline that must survive any future change verbatim**, each with the
  test that would catch a violation:

  | Decision | Guard |
  |---|---|
  | Only regular files become sources, and what the walk declines is **counted**, never dropped in silence: symlinks apart from the residue (fifos, sockets, devices). Under `follow_links(false)` a symlink to a directory never reaches the directory branch, so both flavours land at the one skip point. These are walk observations, so they repeat on a quiet rescan while every event counter stays at zero — the distinction, not a stuck counter | `pipeline::a_symlink_is_counted_and_never_becomes_a_source` |
  | `hardlink_companions` counts **files**, not nominations, and only where the content agrees — it qualifies the `new` count, so it must never exceed it. `moves_unverified` likewise counts files: one file can make at most one move | `pipeline::convergence_indexes_every_twin_path_and_then_goes_quiet` |
  | Every per-root counter is folded through `ScanStats::absorb`, never by naming fields at a call site | `types::absorb_carries_every_counter_a_walk_produces` — destructures with **no rest pattern**, so a new counter fails the build until someone decides whether it belongs in the total |
  | Every counter the hash pass produces is carried through `ScanStats::carry_hash_pass`, never by naming fields at a call site — the twin of the fold above, rest-less for the same reason. The pass runs once after every root, so its counters are *carried* rather than *folded*; the difference is a placement decision, never a licence to copy fields by hand | build-refused (a new `HashStats` field fails the production build at the destructure), with `types::carry_hash_pass_places_every_counter_the_pass_produces` for the behavioural half |
  | A scan records **what it was aimed at**, never the whole root by default: the `--missing` arm pushes its own `DecisionScope` from the `rel_prefix` `mark_missing_path` resolved, and a **named file** does the same from its own remainder. Without it the decision reads `global` in the trail — a whole-universe claim for an act on one place. Start-time decomposition falls back to lexical soft-match for a path that no longer canonicalizes, so the scope is on the record from the moment the row exists; the completion path records nothing for a path that produced no observation, so a whole-root claim has one author | `cli::a_scan_scope_survives_a_path_that_no_longer_canonicalizes` |
  | A named file is observed singly and never walked (`ops::pipeline::observe_file`), and the file-grain path **infers no absence at all**: it takes no `capture_deletions` parameter, returns an always-empty `deleted_items`, and has no arm that could mark a row missing. Deletion by difference needs a walk to differ from; recording one stays `--missing`'s explicit assertion | `cli::a_scanned_file_never_infers_deletion` (two-sided: the gone path leaves the row standing, `--missing` on it marks, stamps and receipts it) |
  | A move **into** a named path is followed under the same three gates — pairing runs over a set of one through `resolve_moves`, and `persist_resolution` still re-checks the claim inside its own transaction | `pipeline::a_move_into_a_named_file_path_is_followed` |
  | The hash gate is asked at file grain exactly as it is asked in the walk. Without it a modified file loses its `object_id` to the `Modified` arm and the refreshed lock drops it as unhashed — a re-observation would strip the identity it was run to restore | `cli::a_named_file_is_hashed_like_any_other_observation` (end to end, over the `Modified` arm) |
  | The `--missing` hint is offered **only where the act would be both accepted and sound**: the acceptance half is asked of `ops::pipeline::resolve_missing_target`, the act's own precondition check, never re-derived at the interface; the soundness half asks whether the **root's own path** resolves on disk. `--missing` marks everything at or under what it is given, lexically, without asking whether the storage is there — and canonicalization fails identically at *every* depth under an absent volume, so a hint keyed on the root's top alone still recommends deleting a whole library when the user names the library, and writing that receipt recreates the root's directory into a mountpoint shell | `cli::the_missing_hint_is_withheld_where_asserting_would_be_wrong` (root top, no root, suspended root, absent storage below a live root — each with its permitting control) |
  | A root is a folder: `--add` and `--candidates` refuse a file argument naming both it and the directory to use instead, and a file outside every root is refused the same way. A root whose own path is a file is **skipped with a warning, never fatal, and before its scope is recorded** — a per-path fact must not end a run over several paths, nor leave a claim behind it | `cli::a_file_shaped_root_is_skipped_not_fatal` |
  | **Every error inside the wrapped body** completes its own decision row as `interrupted` with the error as summary — the fallible work between `start()` and the summary runs inside one closure with a single failure exit, because a dangling `started` row misreads as a scan killed mid-walk. What sits **outside** the closure stays out: the `unexpected_hash_changes` bail and `run_analyze()` both fire *after* `complete(Completed)`, and routing either through the interrupted exit would double-complete a finished decision | `cli::a_scan_that_errors_after_start_completes_its_decision_as_interrupted` |
  | Mount-stability skip is **counted**, reaching the durable summary | `pipeline::scan_root_unstable_mount_records_no_deletion` |
  | A walk that could not see everything **never infers deletion**, and its error count is **counted, reaching the durable summary** — same shape as the mount-stability skip, second gate on the same `if` | `pipeline::scan_root_walk_error_skips_missing_detection` |
  | Empty-dir classification is not missing detection (`handled_ids` unioned into `all_accounted`) | `pipeline::mark_missing_sources_disconnected_with_ignore_flag` (partial) |
  | Walk/stat errors are non-fatal (`continue` + `stats.skipped`) | structural |
  | Receipts written once at completion, coalesced per root | `receipt::write_deletion_receipts_coalesces_same_root` |
  | `DecisionScope` recorded twice — `decompose` at `start()`, `record_scopes` at completion for roots created mid-run | comment-documented in `cli.rs` |

  **Two cautions about what these guards prove.**
  `pipeline::twin_sequencing_is_order_independent` is an **invariance** pin, not a correctness
  guard: it compares forward and reversed entry orders, so a break that is uniformly wrong in both
  leaves them equal and it passes. Never count it toward a rung's expected failure set.
  And a change that adds a **new route to an existing verdict** can silently mask any guard that
  asserts only the verdict — the tests to re-read after such a change are exactly the ones whose
  expected outcome did *not* change. Repair by re-seeding the fixture so the test reaches the arm
  it names — never by renaming the test to match the accident.

- **The two-file guard shape is complete only when the path really has two files.** A counter's
  producer test plus its `compose_summary` test is the pattern here, and it is sound for a counter
  that reaches the summary directly. A counter produced **per root** travels a third file: the fold
  into the total. Miss it and the counter is computed correctly per root and then dropped — printed
  nowhere, and absent from the durable decision summary. `ScanStats::absorb`'s rest-less
  destructuring is what makes that third file build-enforced rather than remembered.
- **`#[allow(clippy::too_many_arguments)]` on `scan_root`, `reconcile_file` and the test helper
  `process_file` is deliberate** — read fresh it looks like a lapse, and removing it one argument
  at a time is not the repair; an invocation-context type would absorb all three at once.
- **The contentless law does not gate anything in this subsystem** — empty sources are indexed and
  hashed like any other (identity computed, never load-bearing). Hash debt inherits that: an
  unhashed empty source is debt like any other row, is queued like any other row, and is counted by
  `repo::source::count_unhashed` like any other row
  (`count_unhashed_counts_an_empty_source_like_any_other`). Do not optimize empties out of the
  queue.
