# `canon roots retire`

Retiring a root compiles its whole story into a book, places the book on the shelf,
and then removes the root from the index. The ceremony has four steps: the
**readiness review**, the **bind** (compile, place, verify), the inspection window,
and the **release**. A confirmation comes before anything is written, and another
before anything is removed.

```
canon roots retire <id:N|path:/path> [--dry-run] [--allow unresolved] [--reason <text>] [--yes]
```

The load-bearing safety invariant: **at every failure point, either the root is fully
intact or the book is fully placed. Never both partial.** The removal step is
structurally unreachable until the placed book has passed verification.

## The readiness review

The review states the root's whole story in counts before anything happens:

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
    covered              3,980   (content verified present in the archive)
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

The review is the gate's counts. The substance behind them, the map of places and the
acts with their reasons, is [`canon roots story`](story.md), and the review points
there on both verdicts.

The account has two registers, deliberately not reconciled. **The story so far**
counts whole-history events: what was archived from here (both moves and copies, as
the extraction ledger recorded them), what a scan observed deleted, and what is
missing without a recorded deletion. **Standing here now** partitions the sources
presently there: archived from here (the still-standing originals of copy-mode
applies), covered (content verified present in the archive), excluded, empty files
([contentless](../../concepts/object.md#empty-files-are-contentless), never
blocking), and unresolved. A file copied to the archive appears in both registers,
and the `(moved, copied)` split is what keeps that readable.

**`first indexed`** is row evidence, the time the earliest surviving source was first
indexed rather than a scan-decision date, so it stays honest on roots older than
decision recording. The first *recorded* scan opens the book's timeline.

**Facts to weigh** never block retirement. They are: sources missing without a
recorded deletion, never-hashed sources, an unreachable path ("retirement would bind
the story as last observed"), and cluster-generate decisions with no subsequent
apply.

## The verdict is asymmetric

When present sources are neither archived nor excluded, Canon states plainly:
**NOT READY for retirement**. Sources that were never hashed count as unresolved:
they cannot be content-verified against the archive, and forgetting to hash is
exactly the mistake this catches.

When nothing blocks, Canon reports **"No blockers found. Whether this story is
complete is yours to judge."** and never claims "ready". Canon can know a story is
incomplete; only you can know it is finished.

## Binding the book

After the review and the verdict gate, Canon names where the book will stand,
`retired/<name>-<date>/` on the shelf at the archive ledger root, and asks the first
confirmation. On yes:

1. The **story is composed**: the same reading [`canon roots story`](story.md)
   renders, written for the book (see [the book
   format](../../reference/book-format.md#storymd)), and offered once to your editor.
2. The book is compiled into a temporary directory beside the shelf, the story bound
   inside as `story.md`.
3. The compile is **verified** (structure, per-fate counts, gathered ledger, the
   claimed story) before anything standing is touched.
4. The verified book is placed by rename (same filesystem, atomic). On first use the
   shelf is created with a README explaining what it holds.

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

Yes opens `$VISUAL`/`$EDITOR` on the composed page. The draft opens with a suggested
title (`<name> — <comment>`) and a **Foreword** section awaiting your words: a
reflection on the whole place, signed however you wish, bound verbatim above Canon's
narration. Left exactly as it is, the foreword section drops out of the bound page.
Everything else you reshape binds as you leave it; the inventory and meta beside it
remain the machine-verified record, and a hand-refined story is marked as such in the
book's meta.

Answering no, or having no editor set, binds the story as composed. An editor failure
or an emptied page never aborts the ceremony: the choice simply re-opens. `--yes`
never asks and binds the composed story.

Any gaps the compile recorded (unreadable receipts, an ungatherable drive-local
ledger) are printed and bound inside the book. See [the book
format](../../reference/book-format.md) for what the book contains.

If a book for the **same root** already stands at that name, from a previous run
aborted after binding, the ceremony says so up front and replaces it with the fresh
compile. A book for a *different* root with the same name is never touched; the new
book takes a numbered sibling name. Nothing on the shelf is ever silently
overwritten.

## The release

Between the two confirmations is an inspection window: the book is placed and
verified, the root is untouched. Open the book, read it, take your time. Then:

```
Remove the root from the index? Aborting keeps both the root and the book.
```

- **Aborting is free.** The root stays indexed, the book stays on the shelf, and the
  retirement decision records that the story is bound but the root remains. A later
  re-run recompiles fresh and converges.
- **Confirming releases the root**: sources, facts, notes, and the root row are
  removed in one transaction, and the retirement decision, with your `--reason` and a
  durable reference to the book, completes the trail.

Before removing, Canon re-checks that **the world hasn't moved** since the review. If
another process scanned, applied, or excluded on this root in the meantime, the
release stops (root intact, book standing) and asks to be re-run.

If the release fails outright — a busy database, a write error — the standing is the
same and the record says so: the book is bound, the root stays indexed, and the
retirement shows in the trail as `partial` with the reason, not as a run that never
finished.

The closing summary states the guarantee:

```
Retired /mnt/photos-backup: 14,215 sources released; the story is bound at /archive/retired/photos-backup-2026-08-02
The storage is yours to discard.
```

The trail keeps rendering the retired root's history afterwards: [`canon
trail`](../query/trail.md#after-retirement-the-trail-stays-whole) at the old path
states the retirement and points at the book, and receipt pointers follow the
gathered ledger into it.

## The shelf listing

`canon roots retired` lists the books on the shelf:

```
The retired fleet: 2 books on the shelf (/Volumes/Archive/retired)

2026-08-02  /Volumes/Backup/icloud-export — 3,980 entries → icloud-export-2026-08-02
2026-09-14  /Volumes/old-laptop — 12,404 entries → old-laptop-2026-09-14 · "sold it"
```

The listing reads the shelf itself, so a book bound under `recording = Off` appears
too, marked `(not indexed)`. Each line is enriched from its decision row with the
retirement date and your reason. Where the two sides disagree the listing says so
rather than dropping the line: a recorded retirement whose book no longer stands
lists as exactly that, and a directory on the shelf that cannot be identified as a
book is counted rather than skipped. When the shelf isn't reachable, with the archive
unmounted, the listing falls back to the index and says so. An empty shelf is stated
plainly.

Identification is not verification: the listing reads each book's `meta.toml`
identity and counts, nothing more. A book of a future format version still lists, and
nothing about its contents is checked or claimed.

## Recording modes

`Full` and `Records` behave identically here: retirement writes no receipt file,
because the book is the decision's durable artifact, referenced from the decision
row. Under `recording = Off` the ceremony still binds the book and releases the root
but leaves no index entry: the trail and the rm-guard won't know of it, and the shelf
listing shows the book from disk alone, marked `(not indexed)`. Canon states this at
the first confirmation. `--no-receipt` never suppresses the book, which is the
command's deliverable rather than a provenance side-channel.

## Flags

- `--dry-run` — the review only; always exits 0 (it is a report).
- `--allow unresolved` — acknowledges retiring despite unresolved sources ("I'm
  aware, proceed"). Without it, a NOT READY verdict ends the ceremony with a non-zero
  exit. **Never implied by `--yes`** — skipping prompts and acknowledging unresolved
  content are different decisions.
- `--reason <text>` — recorded with the retirement decision and on the book's
  identity page.
- `--yes` — skips both confirmations. The ordering and the verification stay
  structural: `--yes` can never place an unverified book or remove a root before its
  book stands.

## Requirements

- The target must be a **source** root — an archive root is not retired; the archive
  is where the books live.
- An **archive root must be registered** — the book needs a shelf. Removing a root
  without binding its story remains available as [`canon roots rm`](roots.md).
- A suspended or unreachable root **can** be retired — surfaced as retiring on faith:
  the story is bound as last observed, and the drive-local ledger's absence is
  recorded in the book as a gap.
