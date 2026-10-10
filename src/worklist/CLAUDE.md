# worklist/ — the handoff to external tools

The outbound half of **Enrichment**: Canon provides the infrastructure, the external tools
provide the knowledge. `canon worklist` emits a **Selection** as JSONL on stdout, one object per
source, for a tool to read and answer with facts on stdin (`canon import-facts`, which lives in
`facts/`).

`cli.rs` parses, selects and writes; `ops.rs` builds the entries. Sealed behind a barrel of
`run`, called only by `main.rs`, plus `build_entries`, read only by the source-value law's
verifier at the crate root; no sibling subsystem reaches in, and this subsystem reaches no sibling but the expression
facility.

**The JSONL entry shape is a public contract**, not an internal struct. `WorklistEntry` is what
every external enrichment tool parses, so a field rename or removal is a breaking change to
everyone's scripts, invisible to this crate's tests. `basis_rev` exists for the round trip: it
is what lets an import tell whether the file still stands as it did when the worklist was
emitted.

**`--unique-content` skips, and says what it skipped.** Sources with no object are skipped as
unhashed, and after the first source per object the rest are skipped as duplicates; both are
counted and reported on stderr. Never a silent drop — the count is what tells the user their
tool saw fewer files than they selected, and why. The two-tier stream discipline the whole tree
follows: the data goes to stdout, everything about the run goes to stderr, so piping stays
clean.

**`--emit` writes each source's one value per key** — `emit_keys` drives one
`expr::fetch_source_values` call each, over every emitted source at once: the content's fact,
the source's own where the content has none. Every key reads through
it, `source.` keys included, and no Canon writer stores a `source.` fact (import refuses the
namespace; a built-in's value is already an entry field), so `--emit source.size` is null.

**`--emit` does not normalize its keys.** The key is read as typed, so `--emit geo.lat` finds
nothing where the stored key is `content.geo.lat`. `--where` *does* normalize, so one command
line can filter on a key it then emits as null.
