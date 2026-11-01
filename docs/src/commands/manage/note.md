# canon note

Timestamped notes on locations: quick observations about a directory scope. Notes also show in [`survey`](../query/survey.md#notes).

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
| `--by-scope` | Group by location, show most recent note per location. |
| `--limit <N>` | Maximum entries to display (default: 10, 0 = unlimited). |
| `--clear` | Clear notes at the scope (or subtree with `-r`). |
| `--yes` | Skip confirmation prompt (recursive clear only). |

Notes are a journal: each `-m` adds a timestamped entry, and notes are never edited in place.

## Modes

### Add (`-m`)

```bash
$ canon note phone-export -m "phone backup from 2019, mostly photos"
Note added: phone-export
```

### View (default)

`canon note` shows the notes at the exact scope, with counts of the notes above and below it:

```bash
$ canon note
phone-export:
  2026-03-15  phone backup from 2019, mostly photos
  2026-03-20  confirmed: 95% archived, 12 unique files remain

1 noted location below (-r to show)
```

### Temporal listing (`--global`, `-r`)

The most recent notes, by date:

```bash
$ canon note --global --limit 5
/mnt/photos/2011                      2026-03-15  tagged vacation photos, school stuff still needs triage
/mnt/old-drive/phone-export           2026-03-20  confirmed: 95% archived, 12 unique files remain
/mnt/photos/2011/vacation             2026-03-20  subset tagged and clustered
/mnt/old-drive/phone-export/vacation  2026-03-22  unique sunset photos here
/mnt/old-drive/other                  2026-03-23  not sure what this is
(4 more notes, 2 more locations)
```

`-r` lists a subtree the same way:

```bash
$ cd /mnt/old-drive
$ canon note -r
phone-export           2026-03-15  phone backup from 2019, mostly photos
phone-export           2026-03-20  confirmed: 95% archived, 12 unique files remain
phone-export/vacation  2026-03-22  unique sunset photos here
other                  2026-03-23  not sure what this is
```

### Spatial listing (`--by-scope`)

One line per location: its most recent note and its note count, capped at 10.

```bash
$ canon note --global --by-scope --limit 4
/mnt/old-drive/phone-export           (2)  2026-03-20  confirmed: 95% archived, 12 unique files remain
/mnt/photos/2011/vacation             (1)  2026-03-20  subset tagged and clustered
/mnt/old-drive/phone-export/vacation  (1)  2026-03-22  unique sunset photos here
/mnt/old-drive/other                  (1)  2026-03-23  not sure what this is
(3 more locations with notes)
```

### Clear (`--clear`)

Without `-r`, `--clear` clears the notes at the exact scope, with no confirmation.

```bash
$ canon note --clear
Cleared 2 notes at phone-export
```

With `-r`, it clears all notes in the subtree, after confirmation:

```bash
$ canon note --clear -r /mnt/old-drive
Clear 3 notes across 3 locations under /mnt/old-drive?
Proceed? [y/N] y
Cleared 3 notes
```

## Notes and the decision trail

Notes hold thoughts; the [decision trail](../query/trail.md) holds actions. Effectful commands record themselves, and `--reason` attaches your reason.
