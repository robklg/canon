# `canon roots retire`

Retirement compiles a root's whole story into a book on the shelf, then removes the
root from the index. Four steps: the **readiness review**, the **bind** (compile, verify, move to the shelf), the
inspection window, and the **release**. A confirmation comes before anything is
written, and another before anything is removed.

```
canon roots retire <id:N|path:/path> [--dry-run] [--allow unresolved] [--reason <text>] [--yes]
```

**At every failure point, either the root is fully intact or the book is fully on the
shelf. Never both partial.** The root cannot be removed until the book has passed
verification and is on the shelf.

## The readiness review

The root's whole story in counts:

```
Retirement review: /mnt/photos-backup

  role           source
  comment        old laptop backup, 2014–2016
  suspended      no
  first indexed  2026-03-14
  last scan      2026-07-28 (5d ago)

Resolution account
  ever indexed here      14,215 sources

  the story so far
    archived from here   9,847 files, 214.6 GB   (6,102 moved, 3,745 copied)
    deleted              3,891 sources           (scan-observed)
    missing, unexplained 12 sources

  standing here now      4,210 sources
    archived from here   3,745   (this copy still stands; the archive holds it)
    covered              235   (content verified present in the archive)
    excluded             195
    unresolved           35      (19 unhashed — listed by name only)

Facts to weigh
  12 sources are missing without a recorded deletion.
  19 present sources were never hashed — they cannot be content-verified.
  2 cluster-generate decisions on this root have no subsequent apply — possible open intentions.

NOT READY for retirement — 35 sources are neither archived nor excluded.
To retire anyway: canon roots retire path:/mnt/photos-backup --allow unresolved
To read the story behind these counts: canon roots story path:/mnt/photos-backup
```

- **`the story so far`**: whole-history events
- **`standing here now`**: the sources presently there, in their
  [standings](../../concepts/resolution.md#standings)

The two do not reconcile: a file copied to the archive appears in both.

**Facts to weigh** never block retirement.

## The verdict is asymmetric

See [the verdict](../../concepts/resolution.md#the-verdict-is-asymmetric). When nothing
blocks, Canon reports **"No blockers found. Whether this story is complete is yours to
judge."**

## Binding the book

After the review, Canon names where the book will go, `retired/<name>-<date>/` on the
shelf at the archive ledger root, and asks the first confirmation. On yes:

1. The **story is composed** for the book (see [the book
   format](../../reference/book-format.md#storymd)) and offered once to your editor.
2. The book is compiled into a temporary directory, with the story as `story.md`.
3. The compile is **verified**.
4. The verified book is moved to the shelf.

```
The book is at /archive/retired/photos-backup-2026-08-02
  14,215 entries bound; 41 receipts gathered
  story.md — the story as told
```

### The story and your foreword

Before the compile, Canon asks once:

```
Edit the story before it is written into the book? [y/N]
```

Yes opens `$VISUAL`/`$EDITOR` on the composed page, with a **Foreword** section for your
words. The book shows your foreword above Canon's narration. If you leave that section
as it is, it drops out of the book. If you do not edit the page, the story is bound as
Canon composed it.

## The release

Between the two confirmations is an inspection window: the verified book is on the
shelf, and the root is untouched. Then:

```
Remove the root from the index? Aborting keeps both the root and the book.
```

- **Aborting**: the root stays indexed and the book stays on the shelf. A re-run the same
  day replaces that book, and any edits you made to it are lost. A re-run on a later day binds a
  new book beside it; the date in each book's name tells them apart, and the earlier one
  is yours to remove.
- **Confirming**: the root's sources, facts and notes are removed in one transaction.

If anything changed on the root since the review, the release stops with root and book
intact, and asks to be re-run.

```
Retired /mnt/photos-backup: 14,215 sources released; the story is bound at /archive/retired/photos-backup-2026-08-02
The storage is yours to discard.
```

[`canon trail`](../query/trail.md) keeps
showing the retired root's history.

## The shelf listing

`canon roots retired` lists the books on the shelf:

```
The retired fleet: 2 books on the shelf (/Volumes/Archive/retired)

2026-08-02  /Volumes/Backup/icloud-export — 3,980 entries → icloud-export-2026-08-02
2026-09-14  /Volumes/old-laptop — 12,404 entries → old-laptop-2026-09-14 · "sold it"
```

Identification is not verification: the listing reads each book's `meta.toml`
identity and counts, nothing more.

The books of two roots retired from the same path show the same path. To tell them apart,
give each root a comment, or each retirement a `--reason`.

## Flags

- `--dry-run`: the review only; always exits 0.
- `--allow unresolved`: retire despite unresolved sources. Without it, a NOT READY
  verdict ends the run with a non-zero exit. **Never implied by `--yes`.**
- `--reason <text>`: recorded with the decision and on the book's identity page.
- `--yes`: skips both confirmations.

## Requirements

- The target must be a **source** root.
- An **archive root must be registered**. Removing a root without binding its story
  remains available as [`canon roots rm`](roots.md).
- An **unreachable** root can be retired: the story is bound from what Canon last
  observed there.
- A **suspended** root is not retired until you unsuspend it. `--dry-run` still shows the review.
