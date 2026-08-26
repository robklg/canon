# canon trail

Read the decision trail. Canon records every effectful action ([decision provenance](../../concepts/decisions.md)); `trail` reads that record back as a timeline of what happened, with your notes interleaved.

One command, two lenses:

- **The scope lens** — standing in a folder: *what did I do here?* Decisions touching this place, as a timeline ending at now.
- **The time lens** (`--today`, `--since`, `--on`) — *what did I do today?* The day's decisions as a story, with a rollup of what was deleted, archived, and excluded.

```bash
# What happened here?
canon trail

# What happened in a specific folder?
canon trail /mnt/old-drive/photos

# Today's story, across all roots
canon trail --today --global

# Everything since Saturday
canon trail --since saturday

# One specific day
canon trail --on 2026-05-12

# One decision in full
canon trail show 61

# Full paths, for copying
canon trail -l
```

`trail` is a pure query command; it never changes anything.

## The scope lens

With no time flags, `trail` lists the decisions that touched the current scope, oldest to newest, ending at the most recent:

```
Decision trail: /mnt/old-drive/photos

#42   2026-05-12 14:03  archived  .      Applied italy-2016: 47 copied, 0 errors
#57   2026-07-11 15:10  scan      .      Scanned 4,120 files: 12 new, 1,350 missing · "verified duplicates"
#61   2026-07-11 16:42  excluded  misc   Excluded 210 duplicates (kept 105) · "redundant backup"
      2026-07-11 16:50            italy  ~ unsure about the RAW files — revisit

12 earlier decisions not shown (--limit N or --all; showing 20).
2 global decisions not shown (--global).
```

A decision *touches* the scope in either direction: a decision on a parent folder happened to this folder too, and a decision on a subfolder is activity here. Sibling folders' decisions don't appear. That rule applies to a decision's *acted-on scope*. The extraction and arrival lines below follow recorded *placements* instead, which appear only in views that contain them (see [the extraction ledger](../../concepts/decisions.md#the-extraction-ledger--the-trails-outbound-direction)).

Each line carries the decision id, timestamp, the act, the place, the completion summary, and your `--reason` (quoted). Decisions that did not complete cleanly are marked (`[partial]`, `[interrupted]`, `[started]`).

The **act** is the registered transition word where the decision has one (`archived`, `excluded`, `restored`, `deleted`) and the stored command identifier otherwise (`scan`, `cluster_generate`). Notes carry no act; the `~` marks them.

The **place** is the one of the decision's recorded scopes that brought it into this view, with `+N` for its other places. Where several of its scopes match, the deepest is named: a scope inside the view says more about where the act was than an ancestor of it does. Decisions recorded without a scope show `global`.

The place is rendered relative to what you're viewing (`.` is the viewed folder itself). A place elsewhere in the same root is measured from that root and carries a leading `/`; when the listing contains one, a line under the header names the root:

```
Decision trail: /archive/2016
Places are relative to this folder; a leading / is relative to /archive.

#71   2026-08-02 10:56  scan  /       Scanned 99,801 files: 97,746 new, 2,049 unchanged
#84   2026-08-02 11:31  archived  /2020  Applied curation-2020: 412 copied, 0 errors
```

Views spanning several roots, and global views, render full paths, capped from the left. `-l` renders every place in full, absolute and uncapped (see [Full paths](#full-paths--l)).

The listing is capped at the 20 most recent decisions; the footer tells you what's beyond the cap (`--limit N` or `--all` to widen). Global decisions can't be attributed to any folder, so scoped views count them in a footer rather than hiding them.

The trail is the sequence view: what happened here, in order. Its shape-first counterpart is [`canon roots story`](../roots/story.md), which renders a whole root as a map of places and hands each place back to the trail for its full event story.

### Places with no history

A folder holding no sources still has a story when something records it: a file that once stood there, a note, an `apply` that drew from or placed into it, or a decision scoped there or inside it. Those places render normally, which is what keeps a folder emptied by a move-mode `apply` from disappearing from its own history.

A path that none of those record is stated rather than rendered, and the command exits non-zero:

```
No history known at /mnt/old-drive/191 — no sources, notes, or decisions record this place.
(Did you mean 'canon trail show 191'?)
```

The second line appears only when the argument is all digits, where `trail show <id>` is the likely intent. A decision scoped at a parent folder does not make a path beneath it a known place.

The same answer comes back whether you name the place or stand in it: a bare `canon trail` scoped to the current directory is held to the same test. A root's own top is exempt, and `--global` is unaffected.

## The outbound direction: what left from here

Standing at a source location, an `apply` that drew content out of this scope shows up too, even though the apply's own selection scope may have been global or elsewhere. It renders in the *extraction aspect*, replacing the usual summary line:

```
#42   2026-05-12 14:02  archived  2016/italy   → 47 files (3.9 GB) to /Archive/Media/2016/Italy (copied; originals remain) · "italy assembly"
```

The place cell is the drawn-from location, not the destination; the disposition tells you whether the originals remain (`copied`) or are gone from here (`moved`). A decision appears once per view, never as both a selection line and an extraction line.

The ledger records an apply per directory it drew from, so a view shows only what actually left *it*: an apply that drew from two sibling folders never surfaces at a third, and standing inside one of them you see that folder's share of the draw, not the apply-wide total.

Scoped scope-lens views end with a whole-history rollup, independent of the `--limit` cap. It answers "where do I stand with this place?", not "what happened recently?":

```
Archived from here: 1,251 files (22.1 GB) → 2 destinations.
```

Omitted when nothing has ever been drawn from here. Sizes are omitted, not guessed, if any contributing decision's bytes can't be determined. Global views carry no single "here" to roll up, so neither the rollup nor extraction lines appear there; an apply still counts toward the "not shown" footer at any view it doesn't touch.

## The inbound direction: what arrived here

Standing at a destination, the same apply shows up too: files it placed inside this scope are enough, regardless of where its source root sits. It renders in the *arrival aspect*:

```
#42   2026-05-12 14:03  archived  .   ← 47 files (3.9 GB) from /Volumes/old-laptop/photos/2016/italy (copied in; originals remain) · "italy assembly"
```

The place cell is the destination this time, view-relative (`.` for the viewed folder itself); the wording mirrors the outbound direction (`copied in; originals remain` / `moved in`). A source root the live index no longer knows renders with `(root removed)` appended, matching `trail show`'s `drew from:` lines.

When a decision's origin *and* destination both sit inside the view (content rearranged entirely within one scope), it renders once, not twice: the extraction-aspect line, with the destination shown view-relative instead of absolute. Both endpoints stay visible in that one line.

**A placement matches only where the view contains it, at its recorded precision.** Deliveries are recorded per destination directory, so an apply that delivered to `2016/01` and `2016/02` never appears at `2016/03`, and an arrival line's counts are what landed inside the view you're standing in, never the apply-wide total. Applies recorded before Canon kept directory precision are known only to a coarse common prefix of where their files landed: they surface at that prefix and above, and stay silent below it rather than guessing. [`canon ledger reindex`](../maintain/ledger-reindex.md) rebuilds directory precision from the receipts on disk and closes that gap wherever a receipt exists.

The matching whole-history rollup:

```
Arrived here: 2 files (14 B) from 1 origin.
```

## What a rollup counts

**A rollup counts boundary crossings, and the view defines the boundary.** "Archived from here" is content that *left* this place; "Arrived here" is content that *entered* it. Content that moved *within* the view crossed nothing, so it belongs to neither; it gets a third line:

```
Archived from here: 1,251 files (22.1 GB) → 2 destinations.
Arrived here: 340 files (8.2 GB) from 3 origins.
Rearranged here: 47 files (3.9 GB).
```

Crossings are stated first, then what stayed inside. Any combination of the three can appear: a location can draw content out, receive content in, rearrange content within itself, all of these, or none.

`Rearranged here` carries no counterparty clause, unlike its two siblings: rearranged content stayed here, so there is no other place to name.

The same decision reads differently from different scopes ([Crossing In, Crossing Out, Staying Put](../../concepts/decisions.md#crossing-in-crossing-out-staying-put)). An `apply` that moved content from `/archive/2016` to `/archive/2020`:

- Viewed at `/archive`, both endpoints are inside. Nothing crossed → **Rearranged here**.
- Viewed at `/archive/2020`, the origin is outside. Content crossed in → **Arrived here**.

Each view answers its own question. Classification is per *row*, not per decision, so a single apply that drew from inside the view *and* from outside it contributes to `Rearranged here` and `Arrived here` at once.

Sizes are all-or-omitted per rollup, computed over that rollup's own rows: an unknown-size crossing never suppresses a fully known rearrangement total.

## The composition card: what's standing here

Below the rollups, a scoped scope-lens view ends with a present-tense statement of what the location is made of *right now*, read from its surviving sources' stamps rather than from the trail's events (see [the composition card](../../concepts/decisions.md#the-composition-card--state-not-events)):

```
Standing here: 3 files (21 B)
  from /Volumes/old-laptop/photos/2016/italy: 3 files (21 B) · #42 · 2026-05-12
```

"Arrived here" is an event total and never shrinks; "Standing here" is a state total and can be smaller. `Arrived here: 5 files` next to `Standing here: 3 files` means some of what arrived was later deleted or moved elsewhere; neither number is wrong.

Origin lines come first, busiest first: a single-origin root that fed this location across one or more applies merges into one `from <root>` line (listing every contributing decision id and the date range, if more than one); an apply that drew from several roots in a single decision gets its own `via apply #N from M origins` line, since its content isn't merge-worthy with anything else. After origins come **standings**: what present content here was last touched by, one line per transition (`excluded: 28,412 files (19.2 GB)`), merged across every decision that produced it. A standing is a statement about this place now, so it carries no decision id; the decisions behind it are the timeline above. Then a `first indexed here` bucket for content this location saw first via a scan, and an `untracked (predates recording)` bucket for content whose stamp predates recording entirely.

Where the record has a **gap**, the line names the one decision it is about, after the standings: `archived (origin unknown) here (#88)` for an apply the extraction ledger cannot attribute, `transition unrecorded here (#404)` for a stamp whose decision row no longer exists. A gap must read as a gap, so it is never merged away.

Long lists are capped with an explicit remainder line (`… and 2 more origins.`, `… and 3 more gaps.`), never a silent truncation.

An origin line names a place other than this one. Two cases follow from that, both mirroring the boundary rule above:

```
Standing here: 500 files (12.9 GB)
  from /Volumes/old-laptop: 300 files (7.1 GB) · #12, #18 · 2026-03-02 – 2026-05-01
  from elsewhere in /archive: 47 files (3.9 GB) · #42 · 2026-05-12
  rearranged: 12 files (800.0 MB)
  first indexed here: 141 files (1.1 GB)
```

`from elsewhere in <root>` means the content genuinely arrived (its origin sits outside the viewed scope) while the origin root *contains* where you're standing. Origin lines are anchored on the root, so a bare `from /archive` while standing in `/archive/2020` would name the place you are already in. The root is still named rather than left implicit, because a view can span several roots.

`rearranged` means the content didn't arrive at all: every row of the applies behind it was drawn from inside this view, so there is no elsewhere to name. Unlike the rollups, the card classifies per *decision* rather than per row: a source's stamp records which decision last touched it, not which row of that decision, so for an apply spanning several origins the card cannot tell which surviving files came from which side. Any row from outside keeps the origin line, rather than claiming a rearrangement the index can't substantiate.

Origin attribution is root-level throughout: `from /Volumes/old-laptop`, not the subfolder within it. The card merges applies across time, and the root is the stable unit; for the exact subfolder of any one decision, `canon trail show <id>` lists it under `drew from:`.

Exclusion doesn't remove standing: an excluded-but-present source still counts. Renaming a file later doesn't erase its origin either, since attribution follows the decision that stamped it, not the file's current name. The card only appears when it has something to say: a location whose content is entirely first-indexed-here or untracked renders no card at all. It never appears in global views, the time lens, or `--jsonl` output; it is a scoped, present-tense reading, and JSONL's `extractions` field already covers the machine-readable side of provenance.

## The time lens

`--today`, `--since <when>`, or `--on <when>` switch to the day-grouped story view, chronological, so it reads forward:

```
Decision trail: all roots — today

Saturday 2026-07-12 — deleted 1,350 files (35.0 GB), archived 47 files (3.9 GB), excluded 210 files — and 2 other actions

#63   09:14  scan      /mnt/old-disk         Scanned 4,120 files: 12 new, 1,350 missing · "verified duplicates"
      09:40            /mnt/old-disk/photos  ~ unsure about the RAW files — revisit
#64   11:02  archived  ...ive/photos/italy   Applied italy-2016: 47 copied, 0 errors
#65   11:30  excluded  /mnt/old-disk/misc    Excluded 210 duplicates (kept 105) · "redundant backup"
```

`<when>` accepts `today`, `yesterday`, a weekday name (the most recent one, today included), or a date (`YYYY-MM-DD`). Days follow your local timezone.

Each day opens with a rollup by fate: **deleted** (deletions a scan observed), **archived** (apply), **excluded**, plus a count of other actions (scans that deleted nothing, manifest generation, imports, and so on). Sizes are computed from the index and shown when reliable; for older decisions whose files have since been touched by newer decisions, the size is omitted rather than guessed.

Scope still applies: `canon trail --today` inside a root shows that folder's day; add `--global` for the whole story.

## Notes in the timeline

Notes ([`canon note`](../manage/note.md)) interleave with decisions by default, marked with `~` and carrying no id, act, counts, or status: a thought never reads as an action. The trail holds actions ("what did I do?"); notes hold thoughts ("what did I think?"). Use `--no-notes` for decisions only.

## Full paths: `-l`

The place column is capped, which is the wrong shape when what you want is the path itself. `-l` (or `--long`) renders each entry over several lines instead, with the full absolute path, uncapped:

```
$ canon trail -l

#71   2026-08-02 10:56  scan
      /mnt/old-drive/photo library/imported 2007-2010   (+30 other places)
      Scanned 99,801 files: 97,746 new, 2,049 unchanged

      2026-08-02 15:02
      /mnt/old-drive/photo library/imported 2007-2010/raw
      ~ this should probably just be bulk-transferred
```

Paths are absolute in this mode wherever you run it, scoped views included: relative rendering is a convenience for reading, and this mode exists to be copied from. `-l` changes only how an event renders, never which events appear, and has no effect under `--jsonl`.

## Inspecting one decision: `trail show`

The id on every line drills down:

```
$ canon trail show 61
Decision #61 — exclude_duplicates
  when:     2026-07-11 16:42
  status:   completed
  counts:   attempted 315, completed 210, failed 0, skipped 105
  reason:   "redundant backup"
  command:  canon exclude duplicates /mnt/old-drive/photos --prefer /archive ...
  scope:    /mnt/old-drive/photos  (here)
  version:  0.5.2
  summary:  Excluded 210 duplicates (kept 105)
  receipts:
    /archive/.canon-ledger/000061-exclude_duplicates.toml
```

For an `apply` decision, a `drew from:` section lists what it took from each source root: path, files, and size. The path is a snapshot recorded at apply time, so it renders even after the root itself is gone from Canon. A root Canon no longer indexes ends its line with `(root removed)`: the path stays primary (it is the answer to "where did this come from?"), and the marker states that the path is history, not a place you can visit. For a copy, the originals may still exist at the origin, but they are no longer part of Canon's universe:

```
  drew from:
    /Volumes/old-laptop/photos/2016/italy — 47 files (3.9 GB)
    /Volumes/nikon-sd/dcim — 12 files (401 MB) (root removed)
```

When one root's draw fanned out across directories, they are listed beneath its summary line with their own shares, capped at five, with an explicit `… and N more directories` remainder, never a silent truncation:

```
  drew from:
    /Volumes/nikon-sd/dcim — 245 files (2.4 GB)
      dcim/100nikon — 105 files (1.0 GB)
      dcim/101nikon — 140 files (1.4 GB)
```

The marker follows the recorded root, not the path. If you remove a root and later re-add the same path, old extractions still show `(root removed)`: they belong to the root that was removed; the re-added one is a new root that happens to share its path.

A removed origin that left through [`canon roots retire`](../roots/retire.md) points at its book instead: `(root retired — the book: /archive/retired/old-drive)`. The book is the root's complete story, openable without Canon. Only a plain `roots rm` (no bound story to point at) keeps the bare `(root removed)`.

No section when the decision drew from nowhere (every other decision kind).

`show` lists where the decision's [receipts](../../concepts/decisions.md) live on disk, including one receipt per source root for deletions. It does not print receipt contents; open the file to see the per-item record. When there is no receipt, the reason is stated (`no receipt (--no-receipt)`, `no receipt (nothing transferred)` for a run that completed no transfer, or `no receipt recorded`); absence is never silent. A finished decision's receipt pointer names a file that exists: a run whose receipt was never written carries no pointer rather than a dangling one. A receipt pointer whose root has since been removed renders as `root #N (removed)/…`: the receipt was written, but the file now lives on storage Canon no longer indexes.

### `show`'s scope list

A decision can name many places. `show` lists them one per line, capped at five with an explicit remainder, and puts the ones bearing on where you are standing first:

```
  scope:    /mnt/old-drive/photos  (here)
            /mnt/old-drive/photos/2016  (within here)
            /mnt/old-drive/admin
            /mnt/old-drive/misc
            /mnt/old-drive/scratch
            … and 26 more places
```

`(here)` marks a scope that is the current directory or contains it; `(within here)` marks one the current directory contains. **`trail show <id>` therefore reads differently depending on where you run it** — the same scopes are always listed, only their order and these markers change. Run from outside every scope, or where the working directory cannot be resolved, the list is in recorded order with no markers.

The markers use the same rule that decides whether a decision appears in a scoped `canon trail` at all, so a scope marked `(here)` is the reason that decision surfaces where you are standing.

## After retirement: the trail stays whole

When a root leaves through [`canon roots retire`](../roots/retire.md), its history keeps rendering, in two places.

**Receipt pointers follow the gathered ledger.** A deletion receipt was written at the source root itself; retirement gathered a copy into the book's `ledger/`, filenames preserved. `trail show` on such a decision renders the pointer as a relocation:

```
  receipts:
    /Volumes/old-drive/.canon-ledger/000057-scan.toml
      (root retired — gathered into the book at /archive/retired/old-drive/ledger/000057-scan.toml)
```

The first line is where the receipt was written; the second is where it lives now, a path you can open without Canon. Two other states exist: if the book holds no gathered copy (a root retired on faith, unreachable at binding), the line says so and defers to the book, which records the gap (`not gathered into the book; the book at <path> records why`); if the book's own location isn't reachable right now (the archive is unmounted), the line states where the story is bound without claiming what's inside (`the story is bound at <path>, not reachable now`). Canon checks only that the files exist; it never reads the book to answer a trail query.

**A scoped trail at a retired root's old path states the retirement.** Asking `canon trail /Volumes/old-drive`, or running `canon trail` while standing inside the old mount path, answers with the retirement itself instead of an error or a silently global view:

```
This place is retired: /Volumes/old-drive — retired 2026-08-02, "drive failing".
The story is bound at /archive/retired/old-drive (decision #61).
```

The statement answers the question asked, so the command exits 0. A path that was never retired keeps the normal behavior: an explicit unknown path is still an error, and a working directory outside every root still falls back to the global view. (A root removed with plain `roots rm` has no bound story to point at; its decisions still render with snapshot paths, but there is no retirement to state.) Under `--jsonl` the statement is one JSON object (`"type": "retired_scope"`, with `root_path`, `retired_at`, `reason` when recorded, `book`, `decision_id`); stdout stays machine-clean on this path too.

## Machine output

`--jsonl` emits one JSON object per timeline event, with a `type` field (`"decision"` or `"note"`), the raw command identifier, timestamps, counts, reason, scope, summary, and receipt location. An `apply` event additionally carries `extractions`: one entry per recorded placement, a source root's origin directory paired with the destination directory it fed (`root`, `rel_prefix`, `files`, `bytes`, `destination`, `disposition`). It is populated regardless of view, including `--global`, so machine consumers never have to re-derive it from a scoped run. (Rows recorded before directory precision are one per source root, with common-prefix locations: the same fields, coarser values.) The field is absent (not `[]`) for decisions that drew from nothing. The scope header moves to stderr so stdout stays clean:

```bash
canon trail --today --global --jsonl | jq -r 'select(.type=="decision") | .summary'
```

## Flags

| Flag | Meaning |
|------|---------|
| `--global` | All roots, ignoring current-directory scope |
| `--today` | Time lens: today (sugar for `--since today`) |
| `--since <when>` | Time lens: from a day onward |
| `--on <when>` | Time lens: one day |
| `--limit N` | Show at most N decisions (default 20) |
| `--all` | No cap |
| `--no-notes` | Decisions only |
| `-l`, `--long` | Multi-line entries with each place's full absolute path |
| `--jsonl` | Machine output (JSONL on stdout) |
