# roots/ — the command surface over the shared root and note nouns

`canon roots list/rm/comment/suspend/unsuspend`. **Three strata, no domain** — a deliberate
absence: roots' domain logic lives on `Root` itself (`is_suspended`/`is_active`/`matches_scope`),
shared by every subsystem, rather than manufactured into a `roots/domain.rs` to fill a template.

Sealed behind a barrel of `list`/`remove`/`set_comment`/`suspend`/`unsuspend`, consumed by
`main.rs`, plus `plan_remove` and `remove_root_data`, which retirement's ceremony reaches across
the subsystem boundary — `remove_root_data` in production (`retire/ops/ceremony.rs`),
`plan_remove` only from retirement's own test. `plan_remove` carries `#[allow(unused_imports)]`
for that reason, the same treatment `exclude.rs`/`trail.rs` give their test-only re-exports.

## Roots conventions

- **The rm ceremony is awareness, never a gate.** `roots rm` states the root's story standing as
  fact before confirming — no artifact: what removal destroys plus the `canon roots retire`
  pointer; bound: where the book lives (`RemoveRootPlan.retirement`). It never blocks the removal
  on that standing — the user decides, informed.
- **`remove_root_data` is the removal mechanics rm and retire share, under their different
  decisions — and their transaction postures differ.** `execute_remove` passes a bare
  `&Connection`: the four deletes (notes, facts, sources, root) run as separate statements, so an
  interruption partway leaves a root with some of its rows already gone. Retirement's release
  performs the identical mechanics inside one `BEGIN IMMEDIATE` transaction. The gap is real and
  comment-mitigated at the call site (`ops.rs::execute_remove`), not closed — closing it is its
  own change.
- **Three decision-recording shapes, all gated on the same pattern**: the recorder starts only
  *after* the already-in-state check, so a no-op records nothing.
  - `execute_remove` (`RootsRm`) records real counts — `attempted`/`completed` from the plan's
    and the deletion's source counts.
  - `execute_suspend`/`execute_unsuspend` (`RootsSuspend`/`RootsUnsuspend`) record a completed
    decision with no counts (`DecisionCounts` all `None`) — the flip has no per-item outcome to
    report, only a summary.
  - `set_comment` and `list` record no decision at all — annotation and reads aren't content-fate
    transitions.
- **`rm` and `comment` take opposite permits at a closed door.** Removing a root is an act and
  meets the closed default — refused by name with the way back, because the door is exactly what
  protects what is inside from destruction. A comment is the label on the door, not a hand inside
  it (root-grain metadata, never content standing), so it stays permitted and reaches the root
  through `parse_root_spec_any`. Both used to answer `No root for path` about a root that plainly
  exists; that sentence is now reserved for a root that genuinely is not there
  (`parse_root_spec_by_path_names_a_parked_root_not_an_absence`).

- **The suspend family's no-op protocol is a substring contract across the ops boundary.**
  `execute_suspend`/`execute_unsuspend` `bail!` on already-in-state, and the cli string-matches
  the error (`.contains("already suspended")` / `.contains("not suspended")`) to downgrade it to
  an info line. Rewording either end silently turns that info line into an error exit. A
  convention-held protocol that could be a type: the repair is a plan/execute suspend family,
  dissolving the string match and the info-vs-error branching.
- **`canon roots` and `canon roots list` must behave identically** — `main.rs` dispatches both the
  bare command and the explicit `list` subcommand to the same `roots::list(...)` call with the
  same arguments. Any new list flag goes on both forms.
- **`list()` composes the whole operation in the interface** — fetch, suspended filter, scope
  filter, count fetch — with no ops-level list op returning typed rows. A standing altitude
  breach, not a shape to copy.
- **`roots/repo.rs` nests by origin table** — `mod root` + `mod note`, each with its own
  `#[cfg(test)] mod tests`. Not a style choice: `root::tests` defines a 6-argument `insert_root`,
  `note::tests` imports a 4-argument one from `crate::core::testing` — same name, different arity,
  which collides (E0252) if the two test modules are flattened into one. `core/repo/root.rs`'s own
  test module duplicates its own copies of `setup_test_db`/`insert_root` rather than sharing with
  `roots/repo.rs`'s — two independent private helpers, not one repointed.
