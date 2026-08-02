# `canon roots retire`

Retiring a root closes the book on it: its whole story reviewed, bound, and shelved,
so the medium is free to go while the story stays reviewable forever. This page covers
the **readiness review** — the opening movement of the ceremony. The binding movements
(compiling the book, placing it on the shelf, releasing the root) arrive in upcoming
versions.

```
canon roots retire <id:N|path:/path> [--dry-run] [--allow unresolved] [--reason <text>] [--yes]
```

## The readiness review

The review tells the root's whole story in counts before anything happens:

```
Retirement review: /mnt/old-drive

  role         source
  comment      old laptop backup, 2014–2016
  suspended    no
  first scan   2026-03-14
  last scan    2026-07-28 (5d ago)

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
To retire anyway: canon roots retire path:/mnt/old-drive --allow unresolved
```

The account has two registers, deliberately not reconciled. **The story so far** counts
whole-history events: what was archived from here (both moves and copies, as the
extraction ledger recorded them), what a scan observed deleted, and what is missing
without a recorded deletion. **Standing here now** partitions the sources presently
there: covered (content verified present in the archive), excluded, and unresolved.
A file copied to the archive appears in both registers — that's the two registers
telling the truth from two directions, and the `(moved, copied)` split is what keeps
it readable.

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

## Flags

- `--dry-run` — the review only; always exits 0 (it is a report).
- `--allow unresolved` — acknowledges retiring despite unresolved sources ("I'm
  aware, proceed"). Without it, a NOT READY verdict ends the ceremony with a non-zero
  exit. **Never implied by `--yes`** — skipping prompts and acknowledging unresolved
  content are different decisions.
- `--reason <text>` — recorded with the retirement decision and on the book's
  identity page (takes effect with the binding movements).
- `--yes` — skips confirmation prompts (takes effect with the binding movements).

## Requirements

- The target must be a **source** root — an archive root is not retired; the archive
  is where the books live.
- An **archive root must be registered** — the book needs a shelf. Removing a root
  without binding its story remains available as [`canon roots rm`](roots.md).
- A suspended or unreachable root **can** be retired — surfaced as retiring on faith:
  the story is bound as last observed.
