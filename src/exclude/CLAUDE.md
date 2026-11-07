# exclude/ — conscious dismissal, source- and object-level

Realizes the domain-language **Exclusion** concept: triage's letting-go — the safe dismissal that
destroys nothing, only releases a source (or every source sharing an object) from consideration,
restorable at any time.

`domain.rs` is duplicate detection; `repo.rs` is the exclusion-transition SQL; `ops/` composes the
plan/execute pairs over both, across six strata (`types`, `plan`, `receipt`, `runner`, `execute`,
`single`); `cli.rs` is the whole command surface (`canon exclude
set/clear/duplicates/set-object/clear-object`). There is no `render` or `jsonl` — printing is
inline in each command flow.

Sealed behind a barrel of the ten command entry points — `set`, `set_by_id`, `set_by_path`,
`clear`, `exclude_duplicates`, `set_object_by_hash`, `set_object_by_file`, `set_objects_by_filter`,
`clear_object`, `list_objects` — plus `SetOptions` and `ClearOptions`, all consumed by `main.rs`;
and five law-surface riders, `plan_set_objects`, `ExcludeSetObjectsParams`,
`check_set_object_by_file`, `plan_duplicates` and `ExcludeDuplicatesParams`, which no production code calls — the contentless-law canary
(`contentless_law_tests.rs`) reaches them through the barrel, re-exported with
`#[allow(unused_imports)]`. Every other boundary — `domain`, `repo`, `ops`, `cli`, and `ops`'s six
strata — is private or `pub(super)` at most, visible only within `exclude/`.

## Exclude conventions

- **`repo.rs` nests two inner modules, `source` and `object`**, rather than being a flat file:
  `set_excluded` exists in both flavours with different signatures (source takes `source_id`,
  `excluded`, `decision_id`; object takes `object_id`, `excluded`) and would collide if flattened.
  Every call site keeps the plain name, disambiguated by the module prefix.
- **The six exclusion-transition SQL fns live here**, with `ObjectReceiptSource`:
  `fetch_object_sharers_for_receipt`, `set_excluded` in each of the two modules,
  `batch_set_excluded`, `set_decision_id_by_object`, `fetch_excluded`. `fetch_excluded` composes
  its query from `core::repo::object`'s `OBJECT_COLUMNS` and `object_from_row` — `pub(crate)` there
  rather than copied here, because a second spelling of the object row would drift the day either
  is repaired.
- **`ops/receipt.rs` holds exclude's receipt document shapes as well as the mappers** —
  `ExcludeReceipt`/`ExcludeReceiptItem`,
  `DuplicatesReceipt`/`DuplicateGroup`/`DuplicateKeptEntry`/`DuplicateExcludedEntry`,
  `ObjectExcludeReceipt`/`ObjectExcludeEntry`/`ObjectSourceReceiptEntry`. A receipt's body is the
  shape of one command's document; only the `[meta]` table is shared, because `write_receipt` is
  generic and never inspects what it serializes. All `pub(super)` — nothing outside `exclude::ops`
  names them.
- **`run_exclusion` (`ops/runner.rs`) is the subsystem's only transaction site** —
  `conn.transaction()` appears exactly once, and every execute path goes through it. It names zero
  exclude types (generic over `FnOnce(&Connection, Option<i64>) -> Result<Option<T>>`,
  `T: Serialize`) — a `core/` candidate the moment a second consumer exists, but apply and scan run
  their own transaction shapes today, leaving it one short of core entry.
- **`object_source_info` lives in `ops/types.rs`**, not in `single.rs` where its name would
  suggest: both `plan.rs` and `single.rs` call it, and `types` is the one stratum both can depend
  on without a backward edge. Dependencies inside `ops` run one way only — `types` and `runner`
  depend on nothing else here, `plan` and `receipt` on `types`, `execute` and `single` on
  `types`/`receipt`/`runner`, `cli` on all of them.
- **`plan_clear` keeps its own selection**, not `expr::select_sources()` — finding sources *marked
  for clearing* is a different contract from finding *visible* sources, and the shared selector's
  default hides exactly the rows clear must find. It walks every active root, **archive role
  included**: single-target set deliberately accepts an archive-role source, so an exclusion can
  stand there, and whatever set can reach, clear must be able to undo. The scope-driven
  set/duplicates/set-objects plans stay `SourceOnly` — a scope must never offer the archive itself
  for dismissal. Guard: `test_plan_clear_reaches_archive_role_sources`.
- **Scopes are classified against the index, not the disk** (`classify_all_indexed`, all four
  scope-taking arms): dismissal is a judgment about indexed sources and needs no mount, so an
  `exclude set` on a detached drive must mean what it means attached. Disk classification answers
  "prefix" for anything it cannot see, which turns a named file into a subtree the moment its
  storage goes away.
- **The object-exclusion guards are contentless-law sites**: `plan_set_objects`'s counted
  set-aside and `check_set_object_by_file`'s refusal toward `--hash`. Both route through
  `Source::is_contentless()` — identity-keyed exclusion would otherwise dismiss every empty file in
  the universe via the one object they all share. **`exclude duplicates` is one too**
  (`domain.rs::find_excludable_duplicates`): a duplicate is an identity claim, so an empty file is
  set aside before the hash test — counted as `skipped_empty`, or as already in `--prefer` where it
  stands there — and the count is stated on every path the plan ends on, its executed summary
  included, through one sentence (`ExcludeDuplicatesPlan::empty_line`) placed after the plan's
  notes, which qualify the head's count.
- **`check_*` outside the transaction, `execute_*` inside** — the ceremony needs the check's output
  to compose its prompt, so the single-target TOCTOU window is accepted; one short transaction per
  invocation, and no lock held across a prompt.
- **Ops never touches stderr** — warnings accumulate into result structs and
  `cli.rs::print_warnings` prints them; `cli.rs` is the only file in the subsystem where
  `eprintln!` appears.
- **A receipt that could not be placed states its reason *in* the summary, not beside it.**
  `cli.rs::resolve_placement` conjugates `LedgerRootOutcome` into a placement and, when there is
  none, the reason; `run_exclusion` joins it onto the summary — the one place that join is
  spelled (`recorded_summary`) — so the printed line, the decision row and the trail's narration
  stay one composition (self-explaining gaps). The reason qualifies the head and stays on the first
  line, which is all the trail's list shows; the plan's note lines follow it, one per line. A gap means no placement, and no placement means the `mutate` closure
  builds no receipt, so the body's own copy cannot disagree. A receipt never owed (`Records`
  mode, `--no-receipt`, a dry run) is no gap. No way back is named: nothing is blocked, and
  unsuspending later does not write the receipt this decision did not.
- **The `role DESC, root_path, rel_path` sort is spelled twice**, in `plan.rs` and `single.rs`, so
  the two orderings can drift apart silently; `test_plan_set_objects_source_sort_order` pins the
  `plan.rs` spelling, and on the `single.rs` side `test_fetch_object_sources_includes_role` pins
  the role half only — the `root_path, rel_path` tail is what can drift unseen.

**Decisions pinned by a guard test** — each row is a claim this subsystem makes, and the test that
fails if it stops holding (in `exclude/ops/tests/`, `domain.rs`):

**Behind a door: counted where a claim would otherwise be false, and re-checked once more inside
the transaction.** **Object exclusion** reaches every source sharing the object, parked ones
included: the stamp-set is the receipt-set, so filtering a sharer would break that law —
`ObjectSourceInfo.is_suspended` and the plan's `Holders` (core's count) carry the fact out
(`object_exclusion_counts_parked_sharers_without_filtering_them`), and the ceremony states
`of which N on suspended roots` **directly under the sources line it qualifies**,
on the preview path and the executing one alike, with the plan's notes (`expr::SelectionNotes`)
below it. It is a share of that number and reads as one only beside it; a `--yes` user is the one who most needs it, and a preview they may have skipped
is no place to keep it. That count is owed because it is a fact about **what the act does** — the
judgment is about the content everywhere, and it lands on copies standing in a place the user
closed.

**Nothing else here counts** (both halves, and the position above, on the built binary:
`object_exclusion_states_how_far_behind_a_door_it_reaches`, which is a census row rather than a
sibling of the rows above — the claim is about what a user sees). A wide
`set`/`clear`/`set-object` states no population behind the
doors it never entered: suspending is the conscious act of taking content out of scope, an act
ignores parked content silently, and a run whose own statement is already true owes no standing
footer about the rooms it did not search. A scoped run met the door at the boundary and was
refused or told; a wide one narrowed nothing. `set-object` asking about content that stands only
behind a door therefore answers a plain "No objects to exclude." — the act, silent.

Finally, `refuse_if_a_door_closed` is the backstop for a
root closed between the plan and the execute: asked of the plan's own root paths inside the
transaction, so the refusal rolls back the `started` row with it. It does not cover object
exclusion (which proceeds by design) or the single-target arms (whose check-outside window this
subsystem accepts by name).

**The three arms outside `resolve_scope` meet the door themselves.** `set_by_path`,
`set_object_by_file` and `exclude duplicates` resolve their own paths, so the boundary's
partition never sees them; each calls `core::ops::scope::refuse_parked_locations` right after
resolution and before the existence gate. `duplicates`' scope and prefer paths are members of the
same `validate_sources_exist` carve-out list `compare` belongs to — a location the question rests
on — and repairing one member of a closed list while leaving another is exactly the drift that
list exists to prevent. The two single-file arms reach `refuse_parked_locations` for a plainer
reason: they are single-target acts, and the closed default refuses every act.

**Those same two arms then heal their path** through
`core::ops::scope::stored_form_with_sources` — the source-existence gate's own spelling of the
form-tolerance law's remainder half — because a named file is about to be matched against the
index byte-for-byte and root containment alone never sees a remainder stored in the other form.
A path no byte-form has sources under comes back unhealed, so the refusal is still the one the
user earned. The filter-driven arms take the door at the front door instead, with the
`Refused` verb, before the plan display and the confirmation, so `--yes` cannot get past it.


| Decision | Guard test |
|---|---|
| Receipt file written **after** commit; write failure is a warning, never a rollback | `test_execute_set_receipt_failure_surfaces_warning` |
| Empty plan records nothing — the `has_items` gate prevents `DecisionRecorder::start` | `test_execute_set_empty_plan_records_nothing` |
| Stamp-set captured **pre-stamp, in-transaction**; tombstones included | `test_object_exclude_receipt_lists_stamp_set_including_tombstones` |
| A failure inside `mutate` rolls back the `started` row and the flips together | `run_exclusion_rolls_back_on_error` |
| An unplaceable receipt's reason reaches the row; a placed one leaves the summary alone | `an_unplaceable_receipt_records_its_reason_on_the_row` |
| The reason joins the summary's first line, the notes follow it | `a_gap_reason_joins_the_first_line_and_notes_follow` |
| `plan_clear` finds source-level-excluded rows only | `test_plan_clear_returns_source_level_only` |
| Contentless set-aside in `plan_set_objects` | `test_plan_set_objects_skips_empty` |
| Contentless refusal in `check_set_object_by_file` | `test_check_object_by_file_empty` |
| Contentless set-aside in `exclude duplicates`, stated in the summary | `find_excludable_skips_empty_files`, `the_duplicates_summary_states_the_empty_files_it_skipped` |
| Sort order `role DESC, root_path, rel_path` | `test_plan_set_objects_source_sort_order` |
