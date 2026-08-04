# `canon roots story`

Reading a root's story is the judgment instrument between passes: *is everything
resolved here, and am I happy with how and where it ended up?* The trail answers
that question in events and the retirement review answers it in counts; the story
answers in **places** — where you acted (and why), and what no decision ever
touched.

```
canon roots story <id:N|path:/path> [--limit N | --all]
```

Read-only in the fullest sense: nothing is recorded, nothing is cached — every run
reads fresh. Invoke it whenever you like: mid-triage, between sessions, the day
before retiring.

## The map of places

```
Story: /mnt/old-disk

  role           source
  comment        old laptop backup, 2014–2016
  first indexed  2026-03-14
  last scan      2026-07-28 (5d ago)

The places

  (root)
    archived 5 files, 130.0 KB → /archive/exports/old-disk   #66 · "final export before the disk goes"
    → canon trail /mnt/old-disk

  pictures
    archived 4,102 files, 61.0 GB → /archive/media/rest   #51 · "rest of the pictures, mechanical"
    → canon trail /mnt/old-disk/pictures

    pictures/italy
      archived 640 files, 18.4 GB → /archive/media/2016-italy   #42 · "the Italy trip"
      archived 3 files, 2.1 MB → /archive/exports/old-disk/pictures/italy   #66
      → canon trail /mnt/old-disk/pictures/italy

  minecraft-worlds
    no decision here
    3,412 covered — copies stand in /archive/staging-2019 (3,401), /archive/games (11)
    → canon trail /mnt/old-disk/minecraft-worlds

  system-cache   · across 214 folders
    excluded 12,006 files, 1.2 GB   #58
    → canon trail /mnt/old-disk/system-cache

  downloads   · across 61 folders
    excluded 4,890 files   across 3 decisions
      · "installer junk"   #57, #61
      · #63 — no reason given
    deleted 1,204 files (scan-observed)   #64
    35 unresolved (19 never hashed — cannot be content-verified)
    → canon trail /mnt/old-disk/downloads

Standing: 20,911 sources — 3,980 covered · 16,896 excluded · 35 unresolved (19 never hashed)
Whether this story is complete is yours to judge.
For the readiness gate: canon roots retire path:/mnt/old-disk --dry-run
```

Two more standing lines appear where they apply. `N archived from here` marks
content you deliberately archived *from this root* whose copy you left standing
(a copy-mode apply) — told apart from `covered`, which is content that merely
happens to have identical copies in the archive. And `N empty files (no content
to cover)` states the
[contentless](../../concepts/object.md#empty-files-are-contentless) sources
standing at the place: all shape, no content — outside coverage, never blocking
retirement. The line reports what stands, not what happened: whether a past
archive pass carried them is the trail's and the receipts' story (passes made
before the contentless rule skipped empty files as "already archived").

The map is path-ordered — a map of the root, never a leaderboard. A place earns a
line for one of two reasons: it is **second-guessable** — its standing mix, its act
mix, or where its covered copies stand diverges from its surroundings — or it
carries **recorded care**: a note, or a decision you gave a reason. Everything else
merges into the nearest such place (`· across N folders` hints at the breadth), so
the output scales with how varied the situation is, not with how many files the
root holds. Uniformly resolved territory — however vast — is one line.

## Slices

Acts land where they touched. A decision that spanned places renders as **slices**:
partial counts at each place, all carrying the same `#id`. In the sample, decision
`#66` archived 5 files at the root and 3 more inside `pictures/italy` — two slices
of one act, and neither claims to be the decision's total. The place's trail
handoff tells any decision's whole story.

## Deliberate versus never-decided

Everything on the map is one of two kinds, and the line between them is structural —
a deciding stamp exists, or it doesn't. Canon never infers intent.

- **Acts** render with their what and why: the transition word, the counts, the
  destination you chose (`→`), the decision id, and your recorded reason. Iterative
  decisions at one place aggregate — the what compresses, the whys are enumerated
  per decision, and acts that went to different destinations never merge. A deletion
  a scan observed reads as an observation (`scan-observed`), never as your act.
- **What no decision touched** renders at equal standing, marked `no decision
  here`. **Covered** content states where its copies stand — because nobody ever
  chose that where. Coverage is evidence by content identity — precise about
  content, silent about shape and worth — and whether it amounts to *resolved* is
  yours to judge; "covered in a staging folder you never picked" is exactly what
  this line exists to catch. Unresolved and never-hashed counts are stated plainly.

The arrow and the phrase are deliberate and never mix: `→` always means *sent there
by your act*; `copies stand in` always means *observed there today*.

Two wordings keep the record honest at the seams. `#63 — no reason given` names a
real recorded decision that simply had no `--reason` attached — the id renders
precisely so it never reads as a missing decision. `N excluded (no recorded
decision)` is the opposite gap: excluded content whose deciding record is absent
(excluded before provenance existed, or with recording off) — exclusion is always
deliberate, so this line states an unrecorded decision rather than pretending
there was none.

Notes surface at their places verbatim — your own thinking beside the standings —
and a noted place always earns its own line, however uniform its surroundings. A
note on a single file gathers that file's own fate beside the testimony; a noted
place whose content has all moved on says `nothing stands here now`.

## Each reason renders once

A reason is recorded care, and the map states it in full exactly once — at the
decision's **first slice in reading order**. Every later slice cites the bare
`#id`: in the sample, `#66`'s reason opens the story at the root, and the
`pictures/italy` slice cites it back. In a shared register the cited ids collapse
to one line (`· #155, #131`), separate from the reasons quoted in full and from the
reasonless ids (`— no reason given`) — the three never conflate.

The excluded standing line follows the same discipline: when it would exactly
restate what the excluded acts already narrate — same count, everything still
standing — it is omitted (`system-cache` and `downloads` above; `downloads` keeps
its other standing lines, which say what the acts don't). Any exclusion the acts
don't account for, or acts whose whole-history count exceeds what still stands,
keep both registers. Covered, unresolved, and missing lines are never omitted.

## Drilling down

Every place carries its handoff: `→ canon trail <path>` tells that place's full
event story. The story review shows the shape; [the trail](../query/trail.md) shows
the sequence.

## The gate

The story renders no verdict — no NOT READY, and never "ready". It closes with the
standing totals and hands the gate to [`canon roots retire
--dry-run`](retire.md), whose review states the same totals as counts. Both are
lenses over one fetch of the same world, so they cannot disagree.

## Flags

- `--limit N` — cap the number of place lines (default 50). Omissions are counted,
  never silent.
- `--all` — every place line.

## Requirements

- The target is a **source** root — an archive root's places are served by
  [`canon trail`](../query/trail.md) and its composition card.
- A suspended or unreachable root reads fine — the story as last observed, stated
  in the header.
