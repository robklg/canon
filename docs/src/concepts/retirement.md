# Retirement, the Book, and the Shelf

Canon's goal for old storage is getting it out of your life. Working through a root
resolves what's on it; **retirement** closes it out so the storage can go.

Retiring binds the root's story into a **book** kept on the **shelf**: plain
text, readable without Canon, without the database, and without the storage it came
from. An old drive is usually hard to discard because you can't say what's on it any
more. The book answers that after it's gone.

## Retirement

Retiring a root means this root is [resolved](resolution.md) and its index may be removed.

What counts as resolved is your judgment ([the verdict is
asymmetric](resolution.md#the-verdict-is-asymmetric)). `--allow unresolved` retires
despite a NOT READY verdict, on the record.

[`canon roots story`](../commands/roots/story.md) shows the map of a root's places
at any point in the triage, and records nothing.

Discarding the storage is yours to do.

[`canon roots retire`](../commands/roots/retire.md) runs four steps: the readiness
review, the bind, an inspection window, and the release of the index.

If you don't want a book, [`canon roots rm`](../commands/roots/roots.md) deletes the
root's index outright. Retiring needs a registered archive root because the book
needs a shelf.

## The book

The **book** is the bound story of a retired root: what was on it, and the fate of
every file. It is a directory of plain text. Open its `README.md` and start reading.

The book's contents and format are a public, Canon-independent contract. See
[the book format](../reference/book-format.md).

## The shelf

The **shelf** is the `retired/` directory at your archive's ledger root, where the
books are kept.

`canon roots retired` lists what's on it. Keep the shelf with your archive. Deleting
a book deletes the only readable story of a root that is already gone.

## What survives what

- **Retirement**: removes the index only. The root's sources, facts, and notes are
  deleted from the database. The complete per-file story is bound in the book.
- **`roots rm`**: binds nothing. The decisions stay in the trail, and the archive's own
  ledger keeps its per-file apply and exclusion receipts, but the notes are deleted,
  content no action touched leaves no record, and the storage's deletion receipts stay
  on the storage, discarded with it.
- **Receipts**: the root's own receipts are gathered into the book. Receipts at the
  archive (apply, exclusion) stay in the archive's ledger.
