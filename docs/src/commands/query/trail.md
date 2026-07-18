# canon trail

Read the decision trail. Canon records every effectful action ([decision provenance](../../concepts/decisions.md)); `trail` is how you read that record back — as a timeline of what happened, with your notes interleaved as the thinking between the actions.

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
```

`trail` is a pure query command — it never changes anything.

## The scope lens

With no time flags, `trail` lists the decisions that touched the current scope — a timeline reading oldest to newest, ending at the most recent:

```
Decision trail: /mnt/old-drive/photos

#42   2026-05-12 14:03  .       Applied italy-2016: 47 copied, 0 errors
#57   2026-07-11 15:10  .       Scanned 4,120 files: 12 new, 1,350 missing · "verified duplicates"
#61   2026-07-11 16:42  misc    Excluded 210 duplicates (kept 105) · "redundant backup"
      2026-07-11 16:50  italy   ~ unsure about the RAW files — revisit

12 earlier decisions not shown (--limit N or --all; showing 20).
2 global decisions not shown (--global).
```

A decision *touches* the scope in either direction: a decision on a parent folder happened to this folder too, and a decision on a subfolder is activity here. Sibling folders' decisions don't appear.

Each line carries the decision id, timestamp, the scope it acted on, the completion summary, and your `--reason` (quoted). The scope column is relative to what you're viewing (`.` is the viewed folder itself); in global views it shows the path, capped from the left. Decisions recorded without a scope show `global`. Decisions that did not complete cleanly are marked (`[partial]`, `[interrupted]`, `[started]`).

The listing is capped at the 20 most recent decisions; the footer tells you what's beyond the cap (`--limit N` or `--all` to widen). Decisions recorded without a scope — global operations — can't be attributed to any folder, so scoped views count them in a footer instead of silently hiding them.

## The outbound direction: what left from here

Standing at a source location, an `apply` that drew content out of this scope shows up too — even though the apply's own selection scope may have been global or elsewhere. It renders in the *extraction aspect*, replacing the usual summary line:

```
#42   2026-05-12 14:02  2016/italy   → 47 files (3.9 GB) to /Archive/Media/2016/Italy (copied; originals remain) · "italy assembly"
```

The scope cell is the drawn-from location, not the destination; the disposition tells you whether the originals remain (`copied`) or are gone from here (`moved`). A decision appears once per view — never both a selection line and an extraction line.

Scoped scope-lens views end with a whole-history rollup, independent of the `--limit` cap — it answers "where am I with this drive?", not "what happened recently":

```
Archived from here: 1,251 files (22.1 GB) → 2 destinations.
```

Omitted when nothing has ever been drawn from here. Sizes are omitted (not guessed) if any contributing decision's bytes can't be determined. Global views carry no single "here" to roll up, so neither the rollup nor extraction lines appear there — an apply still counts toward the "not shown" footer at any view it doesn't touch.

## The time lens

`--today`, `--since <when>`, or `--on <when>` switch to the day-grouped story view — chronological, so it reads forward:

```
Decision trail: all roots — today

Saturday 2026-07-12 — deleted 1,350 files (35.0 GB), archived 47 files (3.9 GB), excluded 210 files — and 2 other actions

#63   09:14  /mnt/old-disk             Scanned 4,120 files: 12 new, 1,350 missing · "verified duplicates"
      09:40  /mnt/old-disk/photos      ~ unsure about the RAW files — revisit
#64   11:02  ...ive/photos/italy   Applied italy-2016: 47 copied, 0 errors
#65   11:30  /mnt/old-disk/misc        Excluded 210 duplicates (kept 105) · "redundant backup"
```

`<when>` accepts `today`, `yesterday`, a weekday name (the most recent one, today included), or a date (`YYYY-MM-DD`). Days follow your local timezone.

Each day opens with a rollup by fate: **deleted** (deletions a scan observed), **archived** (apply), **excluded**, plus a count of other actions (scans that deleted nothing, manifest generation, imports, and so on). Sizes are computed from the index and shown when reliable — for older decisions whose files have since been touched by newer decisions, the size is omitted rather than guessed.

Scope still applies: `canon trail --today` inside a root shows that folder's day; add `--global` for the whole story.

## Notes in the timeline

Notes ([`canon note`](../manage/note.md)) interleave with decisions by default, marked with `~` and carrying no id, counts, or status — a thought never reads as an action. The two commands split one story: **the trail holds actions** ("what did I do?"), **notes hold thoughts** ("what did I think?"). Use `--no-notes` for decisions only.

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
  scope:    /mnt/old-drive/photos
  version:  0.5.2
  summary:  Excluded 210 duplicates (kept 105)
  receipts:
    /archive/.canon-ledger/000061-exclude_duplicates.toml
```

For an `apply` decision, a `drew from:` section lists what it took from each source root — path (or `root #N (removed)` if the root is gone since), files, and size:

```
  drew from:
    /Volumes/old-laptop/photos/2016/italy — 47 files (3.9 GB)
    /Volumes/nikon-sd/dcim — 12 files (401 MB)
```

No section when the decision drew from nowhere (every other decision kind).

`show` lists where the decision's [receipts](../../concepts/decisions.md) live on disk — including one receipt per source root for deletions. It does not print receipt contents; open the file to see the per-item record. When there is no receipt, the reason is stated (`no receipt (--no-receipt)` or `no receipt recorded`) — absence is never silent.

## Machine output

`--jsonl` emits one JSON object per timeline event, with a `type` field (`"decision"` or `"note"`), the raw command identifier, timestamps, counts, reason, scope, summary, and receipt location. An `apply` event additionally carries `extractions` — one entry per drawn-from root (`root`, `rel_prefix`, `files`, `bytes`, `destination`, `disposition`) — populated regardless of view, including `--global`, so machine consumers never have to re-derive it from a scoped run. The field is absent (not `[]`) for decisions that drew from nothing. The scope header moves to stderr so stdout stays clean:

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
| `--jsonl` | Machine output (JSONL on stdout) |
