# notes/ — timestamped location annotations

Realizes the domain-language **Note**: Canon holding awareness for the user, distinct from
the decision trail's record of actions.

`domain.rs` holds the note struct and its pure path/scope logic; `repo.rs` is notes' SQL;
`ops.rs` composes the view/list/clear behaviors and the survey context; `cli.rs` parses,
calls and formats (`canon note`).

Sealed behind a barrel of `run` (cli.rs, called only by `main.rs`); `Note`,
`note_display_path`, `relative_to_scope` (domain.rs); `survey_note_context`,
`SurveyNoteContext` (ops.rs); and `fetch_by_roots`,
`fetch_all`, `count_subtree_notes`, `batch_count_subtree`, `insert` (repo.rs). The repo
tier is on the barrel because six sibling subsystems — trail, sweep, survey, story, retire,
roots — read notes directly rather than through an ops call of their own.

**The barrel's repo-tier re-exports are spelled `crate::notes::repo::...`, never the bare
relative `repo::...`.** The architecture scanner's interface-repo-reach check is textual,
not path-resolved, and a front-door file classifies as `Interface`; a bare `repo::`
re-export would be read as `core::repo::…` and reported as the barrel moving data through
the *shared* repository rather than its own sibling module.

**`insert` stays on the barrel though its only non-notes consumers are sibling test
fixtures** (`sweep/ops.rs`, `story/ops/report.rs`, `retire/ops/tests/compile.rs`) —
production seeds notes through notes' own cli and ops. It carries
`#[allow(unused_imports)]` for exactly that reason: nothing in this crate's non-test code
calls it through the barrel.

**On a suspended root, notes can be read but not written.** Adding or clearing a note is an act,
and no act is done on a suspended root: `resolve_single_scope` refuses through
`scope::open_door` with the `Refused` verb, on stderr, exit 1. Viewing and the `-r` listing
read: `resolve_single_scope_optional` reads at the place on the suspended root via
`ClosedDoor::read_here` and says once that the root is suspended, never falling through to the
global listing. Held by
`a_note_view_at_a_parked_cwd_reads_here` and, over the source,
`the_note_add_door_is_spelled_as_a_refusal`, each with a sibling for `-r` — the modes differ by
one call, and a refusal exits the process, so no test can call it; the census observes it on the
binary.

**Roots owns its own notes-table SQL.** `roots::repo`'s inner `note` module
(`delete_by_root`) mirrors the `notes` table's shape rather than reaching into this
subsystem, and lives in `roots/repo.rs`, unrelated to `notes/repo.rs` despite the name.
Don't go looking for `delete_by_root` here.

**The descendant-contract guard**: `relative_to_scope` (`domain.rs`) panics via `.expect`
on a `note_rel_path` that is not a descendant of `scope_rel_path` — a broken precondition,
not a graceful path. Its callers are safe only because `fetch_subtree`'s SQL boundary
(`core::repo::db::path_at_or_under_sql`, string comparison, never `LIKE`) guarantees every
note it returns is a genuine descendant of the queried scope; a prefix-matching fetch would
hand the caller a sibling whose path merely shares a prefix, and panic it. Held by
`relative_to_scope_requires_a_descendant` (`#[should_panic]`).

**`cli.rs` calls `crate::notes::repo::insert` directly on the note-add path** — an
interface file reaching its own repo stratum, bypassing ops. Legal to the module system (a
subsystem may reference its own internals at any depth) and carried as a named
write-severity row in the architecture test's drift baseline rather than left silent; the
repair is to move the insert behind ops.
