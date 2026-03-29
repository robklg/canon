# canon note

Annotate locations with timestamped notes. Notes are quick observations about a directory scope — "interesting photos from 2016 trip", "possible duplicates of archive set", "needs review". They surface automatically in [`survey`](../query/survey.md) output, serving as breadcrumbs during exploration.

```bash
# Add a note to the current directory
canon note -m "lots of unsorted vacation photos here"

# Add a note to a specific path
canon note /mnt/old-drive/exports -m "overlaps with 2019 backup, check survey"

# View notes at the current scope
canon note

# View notes at a specific path
canon note /mnt/old-drive/exports

# List all notes across all roots
canon note --global

# List notes recursively under a scope
canon note -r
canon note -r /mnt/old-drive

# Clear notes at the current scope
canon note --clear

# Clear notes at a specific path
canon note --clear /mnt/old-drive/exports

# Clear all notes under a scope (with confirmation)
canon note --clear -r
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
| `--clear` | Clear notes at the scope (or subtree with `-r`). |
| `--yes` | Skip confirmation prompt (recursive clear only). |

## The journal model

Notes use an append-only journal model. Each `-m` call adds a new timestamped entry — notes are never replaced or edited in place. Multiple notes can exist at the same scope, forming a chronological log of observations. Clearing is the only way to remove notes.

This is deliberate: notes capture evolving understanding. "Check for duplicates" and "confirmed: 80% overlap with backup" are two entries that tell a story.

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

When CWD is not under any known root, view mode falls back to the global list.

### List global (`--global`)

Flat, tab-separated output of all notes across all roots. Designed for grep and scripting.

```bash
$ canon note --global
/mnt/old-drive/phone-export	2026-03-15	phone backup from 2019, mostly photos
/mnt/old-drive/phone-export	2026-03-20	confirmed: 95% archived, 12 unique files remain
/mnt/backup/photos/italy	2026-03-18	best collection of italy trip
```

Output format: `path\tdate\ttext`, one line per note.

### List recursive (`-r`)

Flat, tab-separated output of all notes at the scope and below. Same format as global, but scoped.

```bash
$ canon note -r /mnt/old-drive
phone-export	2026-03-15	phone backup from 2019, mostly photos
phone-export	2026-03-20	confirmed: 95% archived, 12 unique files remain
phone-export/vacation	2026-03-22	unique sunset photos here
```

### Clear (`--clear`)

Without `-r`, clears notes at the exact scope only — no confirmation needed.

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

When no path argument is given, `canon note` uses the current working directory. This follows the same pattern as other Canon commands — you `cd` into a location and work from there.

- **CWD inside a root**: scope resolves to `(root_id, rel_path)` for that location
- **CWD not in any root**: view mode falls back to global list; add and clear modes error

## The directional model

The three listing modes look in different directions:

- **View** (default): looks at *this level* — notes attached to the exact scope, with counts pointing up and down
- **Recursive** (`-r`): looks *down* — notes at this scope and everything below it
- **Global** (`--global`): looks at *everything* — all notes across all roots

## Notes in survey

Notes surface automatically in [`survey`](../query/survey.md) output, appearing after the scope header. Survey shows notes from the scope and its descendants (the subtree), capped at 5 most recent entries. See the [survey documentation](../query/survey.md#notes) for details.

## Distinction from other annotations

Canon has several annotation mechanisms, each serving a different purpose:

- **Notes** (`canon note`): Location-level observations during exploration. Timestamped journal. Surface in survey.
- **Root comments** (`canon roots comment`): A single descriptive label on a root. Shown in `canon roots` listings.
- **Manifest notes** (`# === Notes ===` in manifest files): Free-form text in a specific manifest. Preserved across `cluster refresh`.
- **Facts** (`canon import-facts`): Structured key-value metadata on files or content. Used in filters and patterns.
