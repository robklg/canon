# Retirement, the Book, and the Shelf

Canon's purpose ends in letting go: the confidence to discard a drive is the ability
to review it forever. **Retirement** is how a fully resolved root leaves the index —
not by deletion, but by closing the book on it.

## Retirement

A root is retired when its story is done: everything worth keeping is archived,
everything dismissed is excluded, the rest is accounted for. Retiring means *this
root is resolved and its index may be removed*. The physical disposal — unmounting
the drive, binning it, deleting the folder — is entirely yours, outside Canon; the
ceremony's promise is that you can perform it feeling entirely free.

The unit is the **root**, not the drive. A root may be a whole drive, a folder-copy
of a long-dead one, or one of several roots that together tell a single disk's story
— each retires on its own.

[`canon roots retire`](../commands/roots/retire.md) runs the ceremony: an honest
readiness review (Canon can say NOT READY; it never certifies ready — that judgment
is yours), the compile and placement of the book, an inspection window, and only
then the release of the index. At every failure point either the root is fully
intact or the book is fully placed — never both partial.

## The book

The **book** is the bound story of a retired root: what it held, the fate of every
part, the decisions and their reasons, the notes, the gathered receipts — readable
decades later, with no Canon and no database. Just a directory of plain text on the
shelf: open its `README.md` and start reading.

The register is completion, never mourning. A book is not an obituary; it is a
finished story. Everything of value was extracted, every fate is recorded, and the
one thing the medium still held — the ability to answer "what was on it?" — now
lives on the shelf.

The book's contents and format are a public, Canon-independent contract — see
[the book format](../reference/book-format.md).

## The shelf

The **shelf** is the visible `retired/` directory at your archive's ledger root,
where the books stand. It is deliberately *not* under `.canon-ledger/` — the books
are for human eyes, not system record-keeping. On first use it gains a README
explaining itself, because it too is meant to outlive Canon.

Browsing the shelf reads the retired fleet: every book on it is a story finished — a
drive that left with its history intact. Keep the shelf with your archive; deleting
a book deletes the only reviewable story of a root that is already gone.

## What survives what

- **The index dies; the record doesn't.** Removal deletes the root's sources, facts,
  and notes from the database — but the decisions survive in the trail, and the
  complete per-file story is bound in the book.
- **The book is disk truth.** The database is a rebuildable index; the shelf is not.
  The listing reads the shelf itself, so DB loss costs only its enrichment (dates,
  reasons) — never the books, and not even the fleet's count.
- **Receipts follow the surviving content.** The root's drive-local receipts are
  gathered into the book verbatim, filenames preserved, so decision chains stay
  walkable from disk into the book. Receipts that live at the archive (apply,
  exclusion) stay in the archive's live ledger — which survives precisely because
  the archive does.
