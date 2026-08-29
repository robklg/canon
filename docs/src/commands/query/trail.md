# canon trail

Read the decision trail. Canon records every effectful action ([decision provenance](../../concepts/decisions.md)); `trail` reads that record back as a timeline of what happened, with your notes interleaved.

One command, three ways to ask:

- **The scope lens** — standing in a folder: *what did I do here?* Decisions touching this place, as a timeline ending at now.
- **The time lens** (`--today`, `--since`, `--on`) — *what did I do today?* The day's decisions as a story, with a rollup of what was deleted, archived, and excluded.
- **The counterpart door** (`trail crossings`) — *what moved between here and there?* The relation between two places: what this one gave another, or took from it.

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

# Expand the rollups by place
canon trail crossings

# Everything one drive ever delivered here
canon trail crossings --origin /Volumes/old-backup

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
#42   2026-05-12 14:02  archived  2016/italy   → 47 files (3.9 GB) to /Archive/Media/2016/Italy (copied) · "italy assembly"
```

The place cell is the drawn-from location, not the destination; the disposition states the recorded act — `copied` or `moved`. It says what the apply did, not what is at the origin now: that place may be long since cleared, or on a drive Canon can no longer see. A decision appears once per view, never as both a selection line and an extraction line.

The ledger records an apply per directory it drew from, so a view shows only what actually left *it*: an apply that drew from two sibling folders never surfaces at a third, and standing inside one of them you see that folder's share of the draw, not the apply-wide total.

Scoped scope-lens views end with a whole-history rollup, independent of the `--limit` cap. It answers "where do I stand with this place?", not "what happened recently?":

```
Archived from here: 1,251 files (22.1 GB) → 2 destinations.
```

Omitted when nothing has ever been drawn from here. Sizes are omitted, not guessed, if any contributing decision's bytes can't be determined. Global views carry no single "here" to roll up, so neither the rollup nor extraction lines appear there; an apply still counts toward the "not shown" footer at any view it doesn't touch.

## The inbound direction: what arrived here

Standing at a destination, the same apply shows up too: files it placed inside this scope are enough, regardless of where its source root sits. It renders in the *arrival aspect*:

```
#42   2026-05-12 14:03  archived  .   ← 47 files (3.9 GB) from /Volumes/old-laptop/photos/2016/italy (copied in) · "italy assembly"
```

The place cell is the destination this time, view-relative (`.` for the viewed folder itself); the wording mirrors the outbound direction (`copied in` / `moved in`). A source root the live index no longer knows renders with `(root removed)` appended, matching `trail show`'s `drew from:` lines.

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

Crossings are stated first, then what stayed inside. The hint sits between them, because it expands the two crossing lines and not the third:

```
Archived from here: 1,251 files (22.1 GB) → 2 destinations.
Arrived here: 340 files (8.2 GB) from 3 origins.
  `canon trail crossings` to list the places behind these totals
Rearranged here: 47 files (3.9 GB).
```

Any combination of the three can appear: a location can draw content out, receive content in, rearrange content within itself, all of these, or none. A view whose only rollup is `Rearranged here` carries no hint — nothing crossed there, so the door has no places to list.

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
  arrived from /Volumes/old-laptop/photos/2016/italy
      3 files (21 B) · decision #12 · 2026-05-12
  `canon trail crossings --origin <path>` to list the folders behind an origin
```

"Arrived here" is an event total and never shrinks; "Standing here" is a state total and can be smaller. `Arrived here: 5 files` next to `Standing here: 3 files` means some of what arrived was later deleted or moved elsewhere; neither number is wrong.

An origin line takes three lines at most — the path, then its marker where it has one, then its counts — the same shape the counterpart door uses for the same facts. The path ends its own line and is never elided or wrapped, because it is what you copy into the next command. Where a line's counts name exactly **one** decision, they name the decision itself (`decision #12`) rather than telling you there is one; two or more stay a count.

Origin lines come first, busiest first: a single-origin root that fed this location across one or more applies merges into one `arrived from <root>` entry, carrying how many decisions are behind it and the date range they span (open them with [`trail crossings --origin <root>`](#the-counterpart-door-trail-crossings)); an apply that drew from several roots in a single decision gets its own `via apply #N from M origins` line, since its content isn't merge-worthy with anything else. After origins come **standings**: what present content here was last touched by, one line per transition (`excluded: 28,412 files (19.2 GB)`), merged across every decision that produced it. A standing is a statement about this place now, so it carries no decision id; the decisions behind it are the timeline above. Then a `first indexed here` bucket for content this location saw first via a scan, and an `arrival unrecorded` bucket for content carrying no stamp at all. That content *is* tracked — it is indexed, present, and counted in the header above; what is missing is the record of how it arrived, and the row cannot say why, so the line names the absence and stops there.

Where the record has a **gap**, the line names the one decision it is about, after the standings: `archived (origin unknown) here (#88)` for an apply the extraction ledger cannot attribute, `transition unrecorded here (#404)` for a stamp whose decision row no longer exists. A gap must read as a gap, so it is never merged away.

Long lists are capped with an explicit remainder line, never a silent truncation — and a capped card carries one invitation, not two, because the remainder absorbs the hint:

```
… and 2 more origins — `canon trail crossings --origin <path>` to list the folders behind an origin
… and 3 more gaps.
```

An origin line names a place other than this one. Two cases follow from that, both mirroring the boundary rule above:

```
Standing here: 500 files (12.9 GB)
  arrived from /Volumes/old-laptop
      300 files (7.1 GB) · 2 decisions · 2026-03-02 – 2026-05-01
  arrived from elsewhere in /archive
      47 files (3.9 GB) · decision #57 · 2026-05-12
  `canon trail crossings --origin <path>` to list the folders behind an origin
  rearranged: 12 files (800.0 MB)
  first indexed here: 141 files (1.1 GB)
```

`arrived from elsewhere in <root>` means the content genuinely arrived (its origin sits outside the viewed scope) while the origin root *contains* where you're standing. Origin lines are anchored on the root, so a bare `/archive` while standing in `/archive/2020` would name the place you are already in. The root is still named rather than left implicit, because a view can span several roots.

`rearranged` means the content didn't arrive at all: every row of the applies behind it was drawn from inside this view, so there is no elsewhere to name. Unlike the rollups, the card classifies per *decision* rather than per row: a source's stamp records which decision last touched it, not which row of that decision, so for an apply spanning several origins the card cannot tell which surviving files came from which side. Any row from outside keeps the origin line, rather than claiming a rearrangement the index can't substantiate.

Origin attribution is root-level throughout: `arrived from /Volumes/old-laptop`, not the subfolder within it. The card merges applies across time, and the root is the stable unit; for the exact subfolder of any one decision, `canon trail show <id>` lists it under `drew from:`.

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

## The counterpart door: `trail crossings`

`canon trail crossings` reads the relation between two places: what this one gave another, or took from it. It takes the paths the trail's own output prints.

### The bare view

```
$ canon trail crossings

Crossings: /archive

Archived from here: 1,251 files (22.1 GB) → 2 destinations.
  /archive/Media/2016
      904 files (18.2 GB) · 3 decisions · 2026-07-11 · 44 folders
  /archive/Documents/scans
      347 files (3.9 GB) · decision #57 · 2026-08-09

Arrived here: 36,412 files (498.2 GB) from 10 origins.
  /Volumes/old-backup/archived
      (root retired — the book: /archive/books/2026-08-11-backup-archived)
      8,398 files (201.4 GB) · 15 decisions · 2026-08-02 – 2026-08-09
  … and 9 more origins.

`canon trail crossings --origin <path>` or `--destination <path>` to list the folders behind an entry
```

The closing line names the flag that opens the entries above it: an outbound section lists destinations, so `--destination` opens them; an inbound section lists origins, so `--origin` does. A view showing both sections names both.

The section headers are the rollup lines from `canon trail`, in the same order and the same form. The entries beneath itemize them by counterpart: the number of entries is the rollup's counterparty count, and their counts sum to its total.

Where an entry's counts name exactly one decision, they name the decision itself (`decision #57`) — a fully-determined answer is a handle you can pass to `canon trail show`, not a statistic to look up.

### Outbound entries name places, not folders

Deliveries are recorded per destination directory, and a manifest pattern can spread one apply across a directory per day. Listed at that precision the outbound section answers *which places?* with a list of generated date folders, so it groups them instead — at a key derived from the destinations in view, coarser than the recorded folder and, wherever the archive's own arrangement leaves room for one, below its root:

```
Archived from here: 1,582 files (33.7 GB) → 3 destinations.
  /archive/Media/2016/03
      1,383 files (30.1 GB) · 4 decisions · 2026-07-19 · 44 folders
  /archive/Media/2016/an-event
      146 files (918.2 MB) · decision #61 · 2026-07-11 · 3 folders
  /archive/Media/2016/another-event
      53 files (2.7 GB) · decision #61 · 2026-07-11
```

`44 folders` is the coverage count: how many recorded destination folders that entry stands for. An entry standing for exactly one omits it, because the path above it *is* that folder.

The grouping is a display key and never a loss of reach: naming a grouped entry with `--destination` opens it at the recorded precision. Nor is the count a second arithmetic — `canon trail`'s `→ N destinations` runs the same grouping over the same key, so wherever both surfaces speak they count the same way. (They can still fall silent differently, for the reason given above: they select by different evidence.)

The **inbound** section is unchanged: its counterpart is the origin root, which is already a place you would name.

An archive whose destinations sit directly under its root has no directory between the root and the leaf to key on, so every destination stays its own entry. One level of nesting is the whole difference; the grouping cannot invent a place the archive does not have.

The two surfaces select by different evidence, so they can part. A source root removed and added again at the same path carries a new id: `canon trail`'s rollup, which matches on that id, falls silent, while `crossings`, which matches on the path each decision recorded, still answers. Where they both speak they agree; where they differ, the crossings answer is the one read from the record.

Counterpart paths render whole, on their own line, never elided. A counterpart whose root the live index no longer knows is marked `(root removed)`; a retired one points at its book instead, in the wording `trail show`'s `drew from:` lines use.

A section with nothing in it does not print. Standing at a source location, only the outbound section appears:

```
$ canon trail crossings

Crossings: /Volumes/camera-card/2019

Archived from here: 1,551 files (44.2 GB) → 1 destination.
  /archive/Media/2019
      1,551 files (44.2 GB) · 3 decisions · 2026-07-14 – 2026-08-01 · 6 folders

`canon trail crossings --destination <path>` to list the folders behind an entry
```

### Naming a counterpart

`--origin <path>` narrows to content drawn from at or under that place; `--destination <path>` to content placed at or under it. Both take a path at any depth, and both compose. Naming one drops that section to per-decision detail at row precision:

```
$ canon trail crossings --origin /Volumes/old-backup/archived

Crossings: /archive

Arrived here: 8,398 files (201.4 GB) from /Volumes/old-backup/archived
  (root retired — the book: /archive/books/2026-08-11-backup-archived).

  #48   2026-08-02   1,204 files (31.2 GB)   moved in
        Photos/2016  → Media/2016
        Photos/2017  → Media/2017
        "italy trip + the 2017 backlog"
  #50   2026-08-02     847 files (12.8 GB)   copied in
        Photos/2018  → Media/2018
  … and 13 more decisions.

Standing here: 8,151 of the 8,398 files delivered — 15 decisions stand behind them; 17 delivered.
```

Each end of a place line is measured from its own anchor: the named counterpart on one side, the viewed scope on the other. The counterpart path in the header stays whole. A named section carries no drill-down hint — you have already stepped through that door — but naming one counterpart leaves the other section listing counterparts as usual, and that half is still taught.

A flag can also narrow the section on the side you are standing rather than the side you asked about. Standing at a source location, `--origin <subfolder>` narrows what left, while what it left *for* is still unnamed, so that section keeps the counterpart listing and names the place it narrowed to instead of saying "here":

```
Archived from /Volumes/camera-card/2019/raw: 812 files (22.4 GB) → 1 destination.
```

The counts are then smaller than the same sentence in `canon trail`, and the header says why.

The `Standing here` line appears when the view is scoped, `--destination` is not in play, and the [composition card](#the-composition-card-whats-standing-here) carries an origin line for exactly this root. It states two counts on each of two axes: how much that origin delivered and how much of it stands here now, in files and in the decisions behind them. The second clause appears only when the decision counts differ, and they can, without either being wrong: the card counts decisions that stamped surviving sources, this door counts decisions holding delivery records.

Where the two match, they sit side by side and you can see it without doing the subtraction:

```
Standing here: 229 files stand; 229 were delivered.
```

Matching counts are not a statement that these are the same files, and the line does not make one. Content can be moved *into* a place by a later act that keeps its original stamp, so a file can leave and another arrive carrying the same delivery's mark — which is also how more can stand here than this door records as delivered:

```
Standing here: 8,300 files stand; 8,199 were delivered.
```

Only where fewer stand than were delivered do the numbers license a proportion, and only there does the line state one: `8,151 of the 8,199 files delivered`.

A named destination narrows the delivered count while the card still answers for the whole location, so the two would no longer be counts of the same content; the line is omitted rather than shown as a comparison that does not hold. The gap between them is not decomposed, on either axis. Content can leave an archive location by deletion, by a later apply, or by a transition recorded in place, and these records cannot tell those apart.

### What counts as a crossing

A crossing is a movement across the boundary of the place in view. Content that moved *within* the view crossed nothing and appears in neither section. When that is all there is, the command says so:

```
Nothing crossed this boundary. 47 files (3.9 GB) were rearranged within it.
```

Where you named a counterpart, the answer names it back, so it is clear which relation came back empty:

```
$ canon trail crossings --origin /archive/retired/archived-2026-08-08

Crossings: /archive

Nothing has crossed between here and /archive/retired/archived-2026-08-08.
```

A place can be known to Canon — indexed, noted, recorded — and still be no delivery's endpoint. Naming one is not an error and does not read as one; the answer is simply that nothing moved between the two places.

A named counterpart matches at or below the path given, never above it. Asking about `/archive/2016` does not surface a delivery whose recorded destination is `/archive`: a common prefix says nothing about a particular folder beneath it. Matching is on literal bytes, so `_` and `%` in a folder name mean themselves.

Counterparts match on the paths recorded when each decision ran, so a removed root, or one removed and re-added, keeps its link.

### `--global`

`--global` borrows the counterpart named as its boundary:

```
$ canon trail crossings --global --origin /Volumes/old-backup/archived
```

reads everything that drive ever delivered, wherever it went. This is the same computation as standing at the drive. `--global` therefore requires `--origin` or `--destination`; on its own there is nothing to measure against, and the command errors.

The same refusal applies without the flag. Running `canon trail crossings` from a directory no open root contains resolves to every root, which is the same boundless state, and it errors for the same reason: a view with no boundary cannot report a crossing, because every place is inside it. Two such directories answer instead of erroring — a [retired root's old path](#after-retirement-the-trail-stays-whole), which states its retirement, and a directory inside a suspended root, whose message names the suspension and the way back.

When `--global` names both, the boundary is the deeper of the two where one contains the other, and the origin otherwise. Where the two paths do not nest, both choices select the same records and only the section header differs. Where one contains the other they select differently, and the deeper path is the one that reads the movement between them as a crossing rather than as a rearrangement inside the wider place.

### Exits, caps and machine output

A counterpart Canon has no record of is stated, exit non-zero, no `Error:` prefix, stdout clean, matching the [scope-lens miss](#places-with-no-history). A counterpart Canon knows across which nothing crossed is answered, exit 0.

`--limit N` (default 20) caps each section independently, always with an explicit remainder; `--all` uncaps. Place listings inside one delivery cap at 5, the value `drew from:` uses, and `--all` uncaps those too — every remainder this command prints has an invocation that opens it.

`--jsonl` emits the same decision events the timeline emits, over the decisions carrying a crossing in view. No field is added or dropped, and each decision carries its full row set, so a decision serializes identically wherever it was surfaced from.

`crossings` is read-only and records no decision.

| Flag | Meaning |
|------|---------|
| `--origin <path>` | Narrow to content drawn from at or under this path |
| `--destination <path>` | Narrow to content placed at or under this path |
| `--global` | All roots; requires `--origin` or `--destination` |
| `--limit N` | At most N entries per section (default 20) |
| `--all` | No cap |
| `--jsonl` | Machine output (JSONL on stdout) |

Counterparts are named by path. A root spec (`id:N`, `path:...`) is refused: it cannot name a location below a root, and a removed root's id is gone with the root.

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
