# `canon roots story`

A root's resolution story as a map of **places**: where you acted and why, and what no
action ever touched.

```
canon roots story <id:N|path:/path> [--limit N | --all]
```

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

**Lines starting with a [transition](../../concepts/decisions.md#transitions)**: the
actions at this place. A deletion a scan observed is marked `scan-observed`.

**One action at several places**: partial counts at each, none of them the action's
total. `#66` above is two such lines.

**Other lines**: what is there now, whether or not an action touched it:

| Line | Meaning |
|---|---|
| `covered` | content verified present in the archive; `copies stand in` says where |
| `archived from here` | the original, still here, of content copied to the archive |
| `excluded` | shown when the lines above don't already say it |
| `unresolved` | neither archived nor excluded; any never-hashed count is called out |
| `empty files (no content to cover)` | [contentless](../../concepts/source-object.md#empty-files-are-contentless), outside coverage, never blocking retirement |

`→` always means sent there by your action. `copies stand in` always means observed there
today.

## Why some directories aren't listed

Only some directories get a line. The rest merge into the nearest listed place, and
`· across 214 folders` shows how many that covers. A directory gets a line when:

- **It differs from its surroundings**: in standings, actions or covered-copy locations
- **It has a note**
- **It has an action you gave a reason**

## The gate

The story gives no verdict. For that, run
[`canon roots retire --dry-run`](retire.md).

## Flags

- `--limit N`: cap the number of place lines (default 50). Omissions are counted.
- `--all`: every place line.

## Requirements

- The target is a **source** root. An archive root's places are served by
  [`canon trail`](../query/trail.md) and its composition card.
- For a suspended or unreachable root, the story shows what Canon last observed
  there, and the header says so.
