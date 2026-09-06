# compare/ — two places, answered by identity

Answers what two locations hold that the other does not, by content identity rather than by
name: the same bytes count as the same content wherever they sit and whatever they are called.
Symmetric by construction — neither side is the subject, which is what distinguishes it from
`survey`, where one side is read for resolution against a reference.

`cli.rs` parses and formats; `ops.rs` owns the whole fetch and the comparison. **`run_compare`
owns the root list, both path resolutions, source-existence validation and both content maps**,
so `cli.rs` is left with formatting only. Don't move any of that back up.

Sealed behind a barrel (`compare.rs`) of `run` and `CompareOptions`, both for `main.rs`, and
`run_compare`, which production code reaches directly rather than through the barrel — the
contentless law's canary is the re-export's only consumer, so it rides behind a targeted
`#[allow(unused_imports)]`.

**This subsystem builds its own object maps.** Every other archived-ness consumer routes through
the shared SQL projections or the in-memory object index; compare walks its selections into
`HashMap<object_id, path>` directly, because what it needs is a per-side set to intersect and
difference, not an archived-ness answer. That makes it **a contentless-law site in its own
right**: `select_and_build_map` sets empty sources aside and counts them, the same way the index
refuses them and the SQL requires `size > 0`. The count is reported, never silently dropped —
an empty file would otherwise match every other empty file on both sides and mean nothing by it.

**Step order in `run_compare` is observable.** Which error surfaces first when several inputs are
invalid at once depends on it — filter parsing, then the root fetch, then A's path resolution,
then B's, then existence validation for both. Reordering the steps is a user-visible change, not
a tidy-up; the first step of it is pinned by
`filter_parse_error_surfaces_before_path_resolution_error`.

**The two selection calls must stay sequential.** `select_sources` takes `&mut Connection` and
both sides share one, so A fully resolves before B starts. Parallelizing would need a second
connection, which is a different design.

**Both sides are load-bearing, at the door as at the gate.** `run_compare` calls
`refuse_parked_locations` before the existence gate, so a side standing on a root the user
closed refuses the whole ask by name rather than reporting three false lines about a smaller
world — the same reason `validate_sources_exist` aborts here instead of setting a sourceless
side aside. The one-path form's CWD subject takes the same check at the front door, because
standing in a place is naming it.

**Compare uses `RolePolicy::AnyRole`**: both sides are compared whatever their roots' roles.
Asking whether two places hold the same content is a question about the places, not about
whether either is an archive — so `--include` here accepts only `excluded`, and there is no
`--global`, because comparing everything to everything is not a question.
