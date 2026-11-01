# canon trail

The [decision](../../concepts/decisions.md) trail: every effectful command in a timeline, interleaved with notes.

- **By place**: which actions touched this place.
- **By time** (`--today`, `--since`, `--on`): an overview of a time window.
- **Between places** ([`trail crossings`](trail-crossings.md)): what was transferred between two places.

```bash
# What happened here?
canon trail

# What happened in a specific directory?
canon trail /mnt/old-drive/photos

# Today, across all roots
canon trail --today --global

# Everything since Saturday
canon trail --since saturday

# One month
canon trail --on 2026-05

# One action in full
canon trail show 61

# Full paths, for copying
canon trail -l
```

## By place

With no time flags, the actions that touched the scope:

```
$ canon trail --limit 3
Decision trail: /mnt/old-drive/photos

#14   2026-11-01 15:15  archived  italy  → 47 files (3.9 MB) to /archive/2016/italy (copied) · "italy assembly"
#15   2026-11-01 15:15  scan      .      Scanned 68 files: 0 new, 0 updated, 0 moved, 68 unchanged, 12 missing · "verified duplicates"
#16   2026-11-01 15:15  excluded  misc   Excluded 21 sources · "redundant backup"
      2026-11-01 15:15            italy  ~ unsure about the RAW files — revisit

Archived from here: 47 files (3.9 MB) → 1 destination.
  `canon trail crossings` to list the places behind these totals
12 earlier decisions not shown (--limit N or --all; showing 3).

Standing here: 68 files (5.6 MB)
  excluded: 21 files (1.7 MB)
  first indexed here: 47 files (3.9 MB)
```

An action touches a scope in either direction: one on a parent directory counts here, and so does one on a subdirectory.

Incomplete actions are marked `[partial]`, `[interrupted]`, `[refused]` or `[started]`.

The third column is the [transition](../../concepts/decisions.md#transitions) that occurred (`archived`, `excluded`, `restored`), otherwise the command name.

The fourth column is the place: the scope the action applied to, relative to this directory.

- **This directory**: `.`
- **Below it**: the path from here. `italy`
- **Elsewhere in the same root**: the path from the root, with a leading `/`. `/` alone is the root.
- **Several scopes**: the deepest one, with `+N` for the others. `b/IMG_0.jpg +1`
- **No scope**: `global`. `canon trail --global` lists these actions.
- **A trail crossing roots**: full paths, capped from the left.

Where a place has a leading `/`, a line under the header names the root:

```
Decision trail: /archive/2016
Places are relative to this folder; a leading / is relative to /archive.

#2    2026-11-01 15:14  scan      /      Scanned 0 files: 0 new, 0 updated, 0 moved, 0 unchanged, 0 missing
#4    2026-11-01 15:14  archived  italy  ← 47 files (3.9 MB) from /mnt/old-drive/photos/italy (copied in) · "italy assembly"
```

The last column is the action's summary and your `--reason` in quotes. `~` marks a note there, and `--no-notes` hides notes.

See also [`canon roots story`](../roots/story.md): a whole root as a map of places.

## What went in and what went out

An `apply` shows which way its content went:

- **Out of here**: `→`. From `/mnt/old-drive/photos`: `→ 6 files (30.0 KB) to /archive/2016 (copied)`
- **Into here**: `←`. From `/archive`: `← 6 files (30.0 KB) from /mnt/old-drive/photos/italy (copied in)`
- **Within here**: `→`, on one line. From `/archive`: `→ 3 files (15.0 KB) to 2020 (moved)`

The fourth column is the end in this place, and the counts are what went out of or into it.

A trail of one place ends with whole-history totals, whatever the `--limit`. `Archived from here` and `Arrived here` count what [crossed the boundary](../../concepts/decisions.md#crossing-in-crossing-out-staying-put) of this place, and `Rearranged here` counts what moved within it:

```
Archived from here: 1,251 files (22.1 GB) → 2 destinations.
Arrived here: 340 files (8.2 GB) from 3 origins.
  `canon trail crossings` to list the places behind these totals
Rearranged here: 47 files (3.9 GB).
```

Size is omitted if unknown.

## The composition card: what's standing here

What exists at the location ([the composition card](../../concepts/decisions.md#the-composition-card--state-not-events)):

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

| Line | Meaning |
|---|---|
| `arrived from <root>` | Came from that root, merged across actions, busiest first |
| `arrived from elsewhere in <root>` | The origin is outside this place but inside the root that contains it |
| `via apply #N from M origins` | One `apply` that took content from several roots |
| `excluded:` and other standings | What present content was last touched by, merged across actions |
| `rearranged` | All of it came from inside this place |
| `first indexed here` | Content this location first saw through a scan |
| `arrival unrecorded` | Content with no record of how it arrived |
| `archived (origin unknown) here (#88)` | A gap: an `apply` the record cannot attribute |
| `transition unrecorded here (#404)` | A gap: an action whose decision record is gone |

Origins are named at the root; `trail show <id>` lists the directories.

## By time

`--today`, `--since <when>` or `--on <when>` group the actions by day:

```
Decision trail: all roots — today

Sunday 2026-11-01 — deleted 12 files (3.6 MB), archived 47 files (3.9 MB), excluded 21 files (1.7 MB) — and 3 other actions

#1    15:16  scan              /mnt/old-drive/photos        Scanned 80 files: 80 new, 0 updated, 0 moved, 0 unchanged, 0 missing
#2    15:16  scan              /archive                     Scanned 0 files: 0 new, 0 updated, 0 moved, 0 unchanged, 0 missing
#3    15:16  cluster_generate  /mnt/old-drive/photos/italy  Generated manifest: italy-2016.toml (47 sources in italy-2016.lock)
#4    15:16  archived          /mnt/old-drive/photos/italy  Applied italy-2016.toml: 47 copied, 0 renamed, 0 moved, 0 skipped (missing), 0 skipped (stale), 0 skipped (filtered), 0 errors · "italy assembly"
#5    15:16  scan              /mnt/old-drive/photos        Scanned 68 files: 0 new, 0 updated, 0 moved, 68 unchanged, 12 missing · "verified duplicates"
#6    15:16  excluded          /mnt/old-drive/photos/misc   Excluded 21 sources · "redundant backup"
      15:16                    /mnt/old-drive/photos/italy  ~ unsure about the RAW files — revisit
```

`<when>` is `today`, `yesterday`, a weekday name (the most recent one, today included), or a [time](../../reference/filter.md#times) such as `2026-07`, `2026-07-12` or `"2026-07-12 14:30"`. Each covers the whole span it gives, in your local time.

## Full paths: `-l`

`-l` shows each entry over several lines, with full absolute paths:

```
$ canon trail -l --limit 1
Decision trail: /mnt/old-drive/photos

#16   2026-11-01 15:15  excluded
      /mnt/old-drive/photos/misc
      Excluded 21 sources · "redundant backup"

      2026-11-01 15:15
      /mnt/old-drive/photos/italy
      ~ unsure about the RAW files — revisit
...
```

## Inspecting a decision: `trail show`

```
$ canon trail show 16
Decision #16 — exclude_duplicates
  when:     2026-11-01 15:15
  status:   completed
  counts:   attempted 21, completed 21, failed 0, skipped 0
  reason:   "redundant backup"
  command:  canon exclude duplicates /mnt/old-drive/photos/misc --prefer /archive --yes --reason redundant backup
  time zone: Europe/Amsterdam
  scope:    /mnt/old-drive/photos/misc  (within here)
  version:  0.6.0
  summary:  Excluded 21 sources
  receipts:
    /archive/.canon-ledger/000016-exclude_duplicates.toml
```

For an `apply`, `drew from:` is what was transferred from each source root, recorded at apply time:

```
  drew from:
    /Volumes/old-laptop/photos/2016/italy — 47 files (3.9 GB)
    /Volumes/nikon-sd/dcim — 12 files (401 MB) (root removed)
```

`(here)` marks a scope that is the current directory or contains it; `(within here)` marks one the current directory contains.

## Machine output

`--jsonl` emits one JSON object per timeline event, with a `type` of `"decision"` or `"note"`. A `summary` can span several lines. The scope header goes to stderr:

```bash
canon trail --today --global --jsonl | jq -r 'select(.type=="decision") | .summary'
# the timeline's line alone
canon trail --today --global --jsonl | jq -r 'select(.type=="decision") | .summary | split("\n")[0]'
```

## Flags

| Flag | Meaning |
|------|---------|
| `--global` | All roots, ignoring current-directory scope |
| `--today` | Today (same as `--since today`) |
| `--since <when>` | From the start of `<when>` onward |
| `--on <when>` | Within `<when>`, the whole span it names |
| `--limit N` | Show at most N actions (default 20) |
| `--all` | No cap |
| `--no-notes` | Actions only |
| `-l`, `--long` | Multi-line entries with each place's full absolute path |
| `--jsonl` | Machine output (JSONL on stdout) |
