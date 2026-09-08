# coverage/ — evidence, not a verdict

Reports **Coverage**: the containment fact Canon can verify — how much of a place is present in
the archive by content identity. It is one layer of **Resolution**, never the judgment that
integrates it. Canon can prove unresolved; it never certifies resolved, and no number this
instrument prints means "done".

`cli.rs` parses and formats; `ops.rs` holds `CoverageStats`, the two computations (scoped and
per-root), and archive-root resolution.

Sealed behind a barrel of `run`, called by `main.rs`, and `compute_per_root`, which production
code reaches directly rather than through the barrel — the contentless law's canary is the
re-export's only consumer, so it rides behind a targeted `#[allow(unused_imports)]`. The canary
reaches the computation rather than the command because the law it guards is about what the
numbers count, not how they are printed.

**The denominator is the law's visible edge.** `coverable_sources()` is hashed minus
contentless, and `unarchived()` is that minus archived — so a fully-covered root that also holds
empty files reads 100%, and the empty files are never counted as an unarchived remainder.
Identity claims about empty content are vacuous, so coverage can say nothing about an empty
source in either direction: it is neither covered nor owed. Every count here answers to the
contentless law, whose cross-tree conventions are in the top-level CLAUDE.md.

**Exclusion is checked through the domain predicate**, `Source::is_excluded()`, which tests both
source-level and object-level exclusion. Never re-derive that test from a column.

**Archived-ness comes from the repository, never from paths.** `batch_archive_copies` is the
documented SQL projection of the law: it requires `size > 0` on the archive side, and it returns
each copy's root standing rather than filtering by it. This instrument counts *sources* whose
object has a copy, not unique objects, because several sources can share one object and the user
is being told about their files.

**One voice, two densities.** `cli.rs` carries both the compact and the full renderer; they
share no private helpers, only `crate::scope::print_report_scope` and `format_count`. Coverage
has one output voice printed at two densities rather than two voices, so the renderers sit
beside the command instead of in a `render.rs` of their own.

**An exclusivity claim needs the whole archive world.** The `only on suspended` segment is
computed only when no `--archive` frame is given (`frame_can_answer_exclusivity`): under a frame
the copies in hand are that one archive's, so "every copy is parked" is true by construction
whenever its door is closed, and the sentence would be false of a world where the content also
stands in a live archive the frame cannot see. Suppressed rather than qualified, because the
honest version of it under a frame is a second question ("of what stands here, how much has no
live copy anywhere") and a different query. The header still marks the archive, so the door is
never unsaid (`a_single_archive_frame_makes_no_exclusivity_claim`).

**A parked `--archive` is answered and marked, not refused.** `resolve_archive_root` returns an
`ArchiveTarget` carrying the `ParkedRoot` when the door is closed, and the header label wears
`(suspended)`. The projection behind the count does not filter those copies out, so there is an
answer to give, and the standing beside the id is what lets the view say what it rests on.

**The archived line says what it rests on.** `CoverageStats.archived_only_parked` is a
`ParkedEvidence`: how many of the archived sources have **every** archive copy behind a closed
door, and which roots those are. It is a qualifier, never a deduction — parking never
un-archives, and the count above it does not move. Suppressed at zero, because "0 only on
suspended roots" is a sentence about a situation that is not happening
(`coverage_counts_only_parked_beside_archived`, `a_wholly_open_archive_says_nothing_about_doors`).
It is a **nudge, not a warning**: no verb, no way back — this is a wide view stating what it
found, not an act meeting a door. `--compact` gains no column: a machine row's columns are a
contract, and a new one is furniture no consumer asked for. It does state a parked `--archive`,
on **stderr** — the boundary policy's channel rule for a machine-shaped stdout, since the compact
renderers carry no header for the marker to ride.
