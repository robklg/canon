# ls/ — the plainest instrument

Lists the sources a **Selection** resolves to: one line per source, in the order asked for. Its
one other mode groups the same selection by content identity instead — not *what is here*, but
*what is here more than once*.

`cli.rs` parses, calls and formats (both display modes); `ops.rs` is the duplicate grouping;
`repo.rs` is the one read behind it. `repo.rs` returns rows and `ops.rs` groups over all of
them, composing each path from the two halves it is stored as — composition is not the
database's job.

Sealed behind a barrel of `run` and `show_duplicates`, both called only by `main.rs`; nothing
else in the tree reaches into `ls`.

**Grouping is by object id, not by hash string.** An object owns exactly one hash, so the two
answer the same question — but the code keys on identity, and prose naming the hash describes a
key the code does not use.

**A group's `total_size` is first-row-wins over an unordered read.** The query asks for no
order, and the caller takes the first row it sees for an object as that group's hash and size.
The hash is safe that way, being fixed by the object. The size is safe only while sources
sharing an object really do share a size, which nothing enforces: a sibling of a different size
would give a wrong total, silently. Stated at the read rather than fixed — an `ORDER BY` would
be a behaviour change.

**The multi-chunk batching path is unexercised.** `repo.rs` has no test of its own, and the two
`ops.rs` tests that reach it pass two ids, so `chunks(BATCH_SIZE)` runs exactly once. Nothing has
ever driven the loop past a single chunk, which is where a batching bug would live.
