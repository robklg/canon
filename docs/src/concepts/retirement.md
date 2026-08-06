# Retirement, the Book, and the Shelf

Canon's goal for old storage is getting it out of your life. Working through a root
resolves what's on it; **retirement** closes it out so the storage can go.

Retiring binds the root's story into a **book** that stands on the **shelf**: plain
text, readable without Canon, without the database, and without the storage it came
from. An old drive is usually hard to discard because you can't say what's on it any
more. The book answers that after it's gone.

## Retirement

Retiring a root means *this root is resolved and its index may be removed*.

What counts as resolved is your judgment. Canon says **NOT READY** when sources are
neither archived nor excluded. When nothing blocks, it reports that it found no
blockers and leaves the verdict to you. `--allow unresolved` retires past a NOT READY
verdict, on the record. Canon can know a story is unfinished; only you can know it is
finished.

The judgment doesn't wait for the ceremony.
[`canon roots story`](../commands/roots/story.md) renders the same map live, at any
point in the triage, and records nothing.

Discarding the storage is yours to do. The ceremony's guarantee is the record, not
the judgment: whatever you decided, the book holds every file's fate.

Retirement operates on a [root](roots.md), at whatever size you drew it. A whole
drive may be one root, or several that each retire on their own.

[`canon roots retire`](../commands/roots/retire.md) runs the ceremony in four steps:
the readiness review, the bind (compile, place, verify), an inspection window, and
the release of the index. At every failure point, either the root is fully intact or
the book is fully placed. Never both partial.

The foreword is optional: leave that section of the composed story untouched and it
drops out of the bound page. And if you don't want a book at all,
[`canon roots rm`](../commands/roots/roots.md) deletes the root's index outright. The
decisions stay in the [trail](../commands/query/trail.md), and the archive's own
ledger keeps its per-file apply and exclusion receipts, but nothing gathers them into
one story: the notes are deleted, content no decision touched loses its account, and
the storage's deletion receipts stay on the storage, discarded with it. Retiring
needs a registered archive root because the book needs a shelf. Removal doesn't.

## The book

The **book** is the bound story of a retired root: what it held, the fate of every
file, the decisions and their reasons, the notes, and the receipts the storage itself
kept. It is a directory of plain text. Open its `README.md` and start reading.

The book's contents and format are a public, Canon-independent contract. See
[the book format](../reference/book-format.md).

## The shelf

The **shelf** is the `retired/` directory at your archive's ledger root, where the
books stand. On first use Canon writes a README there explaining what the directory
holds.

`canon roots retired` lists what's on it. Keep the shelf with your archive. Deleting
a book deletes the only readable story of a root that is already gone.

## What survives what

- **Removal costs the index only.** The root's sources, facts, and notes are deleted
  from the database. The complete per-file story is bound in the book.
- **The shelf is read from disk.** `canon roots retired` reads the shelf itself, so
  losing the database costs only the enrichment it adds: retirement dates and
  reasons. The books and the count of them survive it.
- **Receipts follow the surviving content.** The root's drive-local receipts are
  gathered into the book verbatim, filenames preserved, so decision chains stay
  walkable from disk into the book. Receipts that live at the archive (apply,
  exclusion) stay in the archive's live ledger, which survives because the archive
  does.
