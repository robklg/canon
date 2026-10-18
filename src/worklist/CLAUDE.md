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

**`--emit` reads a name as an expression does.** `ops.rs::read_emit_keys` reads every name
through the facility's one reader (`expr::Key`) beside the filters, before the selection, and
refuses every `source.` name that is no source fact; refused with the filters, both are one
refusal, the filter's first (`ExpressionRefused::join`), so nothing is selected or written.
`build_entries` then reads each key for every emitted source at once through
`expr::fetch_named_values`: a key the source row answers from the row, any other key the source's
one value (`hash` and `hash_short` its content hash). Each field is named as given. Pinned in both files; the key-name law's
`--emit` leg is pinned in `ops.rs`, because the verifier's subsystem may not name this one.
