# worklist/ — the handoff to external tools

The outbound half of **Enrichment**: Canon provides the infrastructure, the external tools
provide the knowledge. `canon worklist` emits a **Selection** as JSONL on stdout, one object per
source, for a tool to read and answer with facts on stdin (`canon import-facts`, which lives in
`facts/`).

`cli.rs` parses, selects and writes; `ops.rs` builds the entries. Sealed behind a barrel of
`run`, called only by `main.rs`; no sibling subsystem reaches in, and this subsystem reaches no
sibling but the expression facility.

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

**Facts are fetched per key, per source** — `emit_keys` drives one `get_fact_value` call each.
A key starting with `source.` reads against the source; anything else reads against its object,
and a source with no object yields null rather than an error. It is a per-source read in a loop
over the whole selection: for a large selection with several keys, the instrument's cost centre.

**`--emit` does not normalize its keys.** `get_fact_value` is a bare read of the facts table by
the key as typed, so `--emit geo.lat` finds nothing where the stored key is `content.geo.lat`,
and `--emit source.size` is null by construction (import refuses that namespace, so no such row
can exist — the value is already an entry field). `--where` *does* normalize, so one command
line can filter on a key it then emits as null.
