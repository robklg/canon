# canon note

Annotate locations with timestamped notes. A note is a quick observation about a directory scope: "interesting photos from 2016 trip", "possible duplicates of archive set", "needs review". Notes surface automatically in [`survey`](../query/survey.md) output.

```bash
# Add a note to the current directory
canon note -m "lots of unsorted vacation photos here"

# Add a note to a specific path
canon note /mnt/old-drive/exports -m "overlaps with 2019 backup, check survey"

# View notes at the current scope
canon note

# View notes at a specific path
canon note /mnt/old-drive/exports

# List recent notes across all roots (temporal, capped at 10)
canon note --global

# List recent notes recursively under a scope
canon note -r
canon note -r /mnt/old-drive

# Show the spatial map — one line per noted location
canon note --global --by-scope
canon note -r --by-scope

# Show more entries (or all)
canon note --global --limit 20
canon note --global --limit 0

# Clear notes at the current scope
canon note --clear

# Clear all notes under a scope (with confirmation)
canon note --clear -r /mnt/old-drive

# Skip confirmation prompt for recursive clear
canon note --clear -r --yes
```

## Options

| Flag | Description |
|------|-------------|
| `-m <TEXT>` | Add a note with the given text. |
| `-r`, `--recursive` | List or clear notes for scope and all descendants. |
| `--global` | List all notes across all roots. |
| `--by-scope` | Group by location, show most recent note per location (spatial view). |
| `--limit <N>` | Maximum entries to display (default: 10, 0 = unlimited). |
| `--clear` | Clear notes at the scope (or subtree with `-r`). |
| `--yes` | Skip confirmation prompt (recursive clear only). |

## The journal model

Notes use an append-only journal model. Each `-m` call adds a new timestamped entry; notes are never replaced or edited in place. Multiple notes can exist at the same scope, forming a chronological log of observations. Clearing is the only way to remove notes.

This is deliberate: notes capture evolving understanding. "Check for duplicates" and "confirmed: 80% overlap with backup" are two entries in the same log.

## Modes

### Add (`-m`)

Adds a note at the resolved scope. Prints confirmation to stderr.

```bash
$ canon note -m "phone backup from 2019, mostly photos"
Note added: /mnt/old-drive/phone-export
```

### View (default)

Shows notes at the exact scope, with spatial context indicators showing how many notes exist above (on parent scopes) and below (on descendant scopes).

```bash
$ canon note
/mnt/old-drive/phone-export:
  2026-03-15  phone backup from 2019, mostly photos
  2026-03-20  confirmed: 95% archived, 12 unique files remain

2 noted locations below
```

When there are no notes at the scope but notes exist nearby, the spatial indicators appear alone:

```
1 note on parent scopes · 3 noted locations below
```

When CWD is not under any known root, view mode falls back to the global temporal list.

### Temporal listing (`--global`, `-r`)

Shows the most recent notes ordered by date: oldest at top, most recent at the bottom (closest to the prompt). Capped at 10 entries by default.

```bash
$ canon note --global
Photos/2011            2026-03-10  mixed bag, vacation + school stuff, worth sorting
old-laptop/Desktop     2026-03-10  raket project — Daniel's, check with him
old-laptop/Music       2026-03-12  check for unique .flac files
Photos/2011            2026-03-15  tagged vacation photos, school stuff still needs triage
Photos/2011/vacation   2026-03-20  subset tagged and clustered
(14 more notes, 6 more locations)
```

The footer (on stderr) shows how many more notes and locations exist beyond the cap. Use `--limit` to see more:

```bash
$ canon note --global --limit 20    # show 20 entries
$ canon note --global --limit 0     # show all entries
```

Recursive listing (`-r`) is the same but scoped to a subtree:

```bash
$ cd /mnt/old-drive
$ canon note -r
phone-export           2026-03-15  phone backup from 2019, mostly photos
phone-export           2026-03-20  confirmed: 95% archived, 12 unique files remain
phone-export/vacation  2026-03-22  unique sunset photos here
```

### Spatial listing (`--by-scope`)

Shows one line per location: the most recent note and the total note count for that location. Locations ordered by their most recent note date, capped at 10.

```bash
$ canon note --global --by-scope
old-laptop/Music       (1)  2026-03-12  check for unique .flac files
Photos/2012/vacation   (2)  2026-03-14  need to check overlap with phone backup
old-laptop/Desktop     (2)  2026-03-25  raket — checked with Daniel, archive
Photos/2011/vacation   (4)  2026-03-28  beach photos assembled, ready to cluster
(6 more locations with notes)
```

The note count shows which locations have longer histories; view one in full with `canon note <path>`.

Inside a root, `--by-scope` without `--global` or `-r` implies `-r`, a spatial map of the subtree:

```bash
$ cd /mnt/old-drive
$ canon note --by-scope
phone-export           (3)  2026-03-20  confirmed: 95% archived, 12 unique files remain
phone-export/vacation  (1)  2026-03-22  unique sunset photos here
```

### Clear (`--clear`)

Without `-r`, clears notes at the exact scope only; no confirmation needed.

```bash
$ canon note --clear
Cleared 2 notes at /mnt/old-drive/phone-export
```

With `-r`, clears all notes in the subtree. Shows a plan and prompts for confirmation:

```bash
$ canon note --clear -r /mnt/old-drive
Clear 5 notes across 3 locations under /mnt/old-drive?
Proceed? [y/N] y
Cleared 5 notes
```

## CWD defaulting

When no path argument is given, `canon note` uses the current working directory, the same pattern as other Canon commands.

- **CWD inside a root**: scope resolves to `(root_id, rel_path)` for that location
- **CWD not in any root**: view mode falls back to global temporal list; add and clear modes error

## Notes at a suspended location

Reading is unaffected: the view, the listings and the [trail](../query/trail.md) read at a [suspended](../roots/roots.md#suspending-roots) location, with the pause stated once. Adding or clearing a note there is refused by name, with the way back.

## The directional model

The listing modes look in different directions:

- **View** (default): looks at *this level* — notes attached to the exact scope, with counts pointing up and down
- **Recursive** (`-r`): looks *down* — notes at this scope and everything below it
- **Global** (`--global`): looks at *everything* — all notes across all roots

Both temporal and spatial modes work with either `--global` or `-r`. Temporal (by date) is the default; `--by-scope` switches to spatial (by location).

## Notes in survey

Notes surface automatically in [`survey`](../query/survey.md) output, appearing after the scope header. Survey shows notes from the scope and its descendants (the subtree), capped at 5 most recent entries. See the [survey documentation](../query/survey.md#notes) for details.

## Notes and the decision trail

Notes hold thoughts ("what did I think about this?"); the [decision trail](../query/trail.md) holds actions ("what did I do?"). Don't write notes to record actions: effectful commands record themselves, and `--reason` attaches your why. [`canon trail`](../query/trail.md) shows both as one timeline, with notes visually distinct.

## Distinction from other annotations

Canon has several annotation mechanisms, each serving a different purpose:

- **Notes** (`canon note`): Location-level observations during exploration. Timestamped journal. Surface in survey.
- **Root comments** (`canon roots comment`): A single descriptive label on a root. Shown in `canon roots` listings.
- **Manifest notes** (`# === Notes ===` in manifest files): Free-form text in a specific manifest. Preserved across `cluster refresh`.
- **Facts** (`canon import-facts`): Structured key-value metadata on files or content. Used in filters and patterns.
