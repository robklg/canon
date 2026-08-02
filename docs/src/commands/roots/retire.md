# `canon roots retire`

Retiring a root closes the book on it: its whole story reviewed, bound, and shelved,
so the medium is free to go while the story stays reviewable forever. The ceremony has
four movements — the **readiness review**, the **bind** (compile, place, verify), the
inspection window, and the **release** (removal from the index) — with a confirmation
before anything is written and another before anything is removed.

```
canon roots retire <id:N|path:/path> [--dry-run] [--allow unresolved] [--reason <text>] [--yes]
```

The load-bearing safety invariant: **at every failure point, either the root is fully
intact or the book is fully placed — never both partial.** The removal step is
structurally unreachable until the placed book has passed verification.

## The readiness review

The review tells the root's whole story in counts before anything happens:

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
```

The account has two registers, deliberately not reconciled. **The story so far** counts
whole-history events: what was archived from here (both moves and copies, as the
extraction ledger recorded them), what a scan observed deleted, and what is missing
without a recorded deletion. **Standing here now** partitions the sources presently
there: covered (content verified present in the archive), excluded, and unresolved.
A file copied to the archive appears in both registers — that's the two registers
telling the truth from two directions, and the `(moved, copied)` split is what keeps
it readable.

**`first indexed`** is row evidence — when the earliest surviving source was first
indexed — not a scan-decision date, so it stays honest on roots older than decision
recording. The first *recorded* scan is the opening line of the book's timeline.

**Facts to weigh** are facts, never warnings, and none of them block: missing-without-
record, never-hashed sources, an unreachable path ("retirement would bind the story as
last observed"), and cluster-generate decisions with no subsequent apply.

## The verdict is asymmetric

When present sources are neither archived nor excluded, Canon states plainly:
**NOT READY for retirement**. Sources that were never hashed count as unresolved —
they cannot be content-verified against the archive, and forgetting to hash is exactly
the mistake this catches.

When nothing blocks, Canon reports **"No blockers found. Whether this story is
complete is yours to judge."** — it never claims "ready". Canon can know a story is
incomplete; only you can know it is finished.

## Binding the book

After the review (and the verdict gate), Canon names where the book will stand —
`retired/<name>-<date>/` on the shelf at the archive ledger root — and asks the first
confirmation. On yes:

1. The book is compiled into a temporary directory beside the shelf.
2. The compile is **verified** — structure, per-fate counts, gathered ledger — before
   anything standing is touched.
3. The verified book is placed by rename (same filesystem, atomic). On first use the
   shelf is created with a README explaining what it holds.

```
The book is at /archive/retired/photos-backup-2026-08-02
  14,215 entries bound; 41 receipts gathered
```

Any gaps the compile recorded (unreadable receipts, an ungatherable drive-local
ledger) are printed and bound inside the book — self-explaining, never silent. See
[the book format](../../reference/book-format.md) for what the book contains.

If a book for the **same root** already stands at that name — a previous run that was
aborted after binding — the ceremony says so up front and replaces it with the fresh
compile. A book for a *different* root with the same name is never touched; the new
book takes a numbered sibling name. Nothing on the shelf is ever silently overwritten.

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
  removed in one transaction, and the retirement decision — with your `--reason` and
  a durable reference to the book — completes the trail.

Before removing, Canon re-checks that **the world hasn't moved** since the review: if
another process scanned, applied, or excluded on this root in the meantime, the
release stops (root intact, book standing) and asks to be re-run.

The closing summary states the guarantee:

```
Retired /mnt/photos-backup: 14,215 sources released; the story is bound at /archive/retired/photos-backup-2026-08-02
The drive is yours to discard.
```

Disposal — unmounting, binning the drive, `rm -rf` on the folder — is yours to
perform, licensed by the book.

## Recording modes

`Full` and `Records` behave identically here: retirement writes no receipt file — the
book *is* the decision's durable artifact, referenced from the decision row. Under
`recording = Off` the ceremony still binds the book and releases the root, but leaves
no index entry: the trail, the rm-guard, and the shelf listing won't know of it. Canon
states this at the first confirmation. The book on disk is the durable truth either
way. `--no-receipt` never suppresses the book — it is the command's deliverable, not
a provenance side-channel.

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
