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

**Archived-ness comes from the repository, never from paths.** `batch_check_archived` is the
documented SQL projection of the law: it requires `size > 0` on the archive side. This
instrument counts *sources* whose object is in the returned set, not unique objects, because
several sources can share one object and the user is being told about their files.

**One voice, two densities.** `cli.rs` carries both the compact and the full renderer; they
share no private helpers, only `crate::scope::print_report_scope` and `format_count`. Coverage
has one output voice printed at two densities rather than two voices, so the renderers sit
beside the command instead of in a `render.rs` of their own.
