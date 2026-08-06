# `canon roots story`

What's resolved on this root, and where did it end up? The trail answers in events
and the retirement review answers in counts. The story answers in **places**: where
you acted and why, and what no decision ever touched.

```
canon roots story <id:N|path:/path> [--limit N | --all]
```

Each run reads the index fresh and prints the map.

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

## Reading a place

Each place shows what you did there, what stands there now, and a handoff to the
trail.

**Act lines** carry the transition word, the counts, the destination you chose (`→`),
the decision id, and your reason. The transitions are `archived`, `excluded`, and
`deleted`; a deletion a scan observed is marked `scan-observed`, so it never reads as
your act. Repeated decisions at one place aggregate, though acts that went to
different destinations never merge.

One decision touching several places renders as **slices**: partial counts at each
place, all carrying the same `#id`, none of them the decision's total. `#66` above is
two slices. A reason is quoted in full at its first slice and cited as a bare `#id`
after that.

**Standing lines** say what is there now, whether or not a decision touched it:

| Line | Meaning |
|---|---|
| `covered` | content verified present in the archive; `copies stand in` says where |
| `archived from here` | archived from this root with the copy left standing (a copy-mode apply) |
| `excluded` | shown when it says something the act lines don't |
| `unresolved` | neither archived nor excluded; any never-hashed count is called out |
| `empty files (no content to cover)` | [contentless](../../concepts/object.md#empty-files-are-contentless), outside coverage, never blocking retirement |

`→` always means *sent there by your act*. `copies stand in` always means *observed
there today*. The two never mix.

A place nobody decided on is marked `no decision here`, and its covered content is
worth a second look: coverage is content identity alone, so "covered in a staging
folder you never picked" is exactly what that line exists to catch.

Notes render verbatim at their place, and a noted place always gets its own line
however uniform its surroundings.

Every place ends with `→ canon trail <path>`, which tells that place's full event
story. The story shows the shape; [the trail](../query/trail.md) shows the sequence.

## Why some folders aren't listed

The map is path-ordered and lists only places worth a look: those whose standing mix,
act mix, or covered-copy locations differ from their surroundings, and those carrying
a note or a decision you gave a reason. Everything else merges into the nearest
listed place, with `· across N folders` showing the breadth. Uniformly resolved
territory is one line however vast it is.

## The gate

The story renders no verdict, neither NOT READY nor ready. It closes with the
standing totals and hands the gate to [`canon roots retire --dry-run`](retire.md),
whose review states the same totals as counts.

At retirement this same map is written again for a future reader and bound into the
book as [`story.md`](../../reference/book-format.md#storymd), with plain fate words,
no handoffs, and a beginning and a last page around it.

## Edge cases

- `#63 — no reason given` is a real recorded decision that had no `--reason`
  attached. The id renders so the line reads as a decision without a reason rather
  than as a missing decision.
- `N excluded (no recorded decision)` is the opposite gap: excluded content whose
  deciding record is absent, either excluded before provenance existed or with
  recording off.
- A noted place whose content has all moved on says `nothing stands here now`.
- The excluded standing line is omitted when it would exactly restate what the act
  lines already say. Covered, unresolved, and missing lines are never omitted.
- `empty files` reports what stands there now. Whether a past archive pass carried
  those files is the trail's and the receipts' story; passes made before the
  contentless rule skipped empty files as "already archived".

## Flags

- `--limit N` — cap the number of place lines (default 50). Omissions are counted,
  never silent.
- `--all` — every place line.

## Requirements

- The target is a **source** root — an archive root's places are served by
  [`canon trail`](../query/trail.md) and its composition card.
- A suspended or unreachable root reads fine — the story as last observed, stated
  in the header.
