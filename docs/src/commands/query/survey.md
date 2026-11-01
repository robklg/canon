# canon survey

An orientation map of a location: what's archived, which other locations share content, and what exists only here.

Survey works on source-side selections: a scope inside archive roots is refused.

```bash
# Survey current directory
canon survey

# Survey a specific path
canon survey /mnt/old-drive/photos

# Survey with filters
canon survey /mnt/old-drive/photos --where "@image AND source.mtime|year=2016"

# See which of your files overlap with related locations
canon survey /mnt/old-drive/photos --detail overlap

# See content that exists nowhere else
canon survey /mnt/old-drive/photos --detail unique

# Pipe unique paths for further processing
canon survey /mnt/old-drive/photos --detail unique -0 | xargs -0 open

# See what's NOT at a reference location
canon survey /mnt/old-drive/photos --detail residual --other /mnt/backup/vacation/

# Add affinity columns to understand related locations deeper (requires --where)
canon survey /mnt/old-drive/photos --where "@image" --affinity

# See complementary content at related locations (requires --where)
canon survey /mnt/old-drive/photos --where "@image" --detail complement

# Compare against specific locations instead of discovering them
canon survey /mnt/old-drive/photos --other /mnt/backup/vacation/
```

## Options

| Flag | Description |
|------|-------------|
| `--where <EXPR>` | Filter expression (repeatable). Narrows the selection. |
| `--affinity` | Enable affinity columns (+N more, unique count, classification). Requires `--where`. |
| `--detail <MODE>` | `archived`, `complement`, `unique`, `overlap`, or `residual`. Replaces the summary view. |
| `--archive <SPEC>` | Filter archive section to a specific archive root (`id:N` or `path:/...`). |
| `--include <VALUE>` | Include additional sources: `excluded`. |
| `--global` | Survey all roots, ignoring current directory scope. |
| `--other <PATH>` | Compare against specific locations (repeatable). Bypasses scope discovery. |
| `--brief` | Skip per-location affinity computation when `--affinity` is active. |
| `--verbose` | Show all locations (summary) or all paths per location (detail views). |
| `-0` | Null-delimited output for `--detail archived`, `unique`, `overlap`, or `residual`. |

## Reading the output

### Summary view (default)

```
Survey: /mnt/old-drive/exports
  517 sources here (0 unhashed, 517 hashed)
  264 unique here

Archived: 201 of 517 (38.9%)
  /archive/media/2019/holiday          41
  /archive/media/2019/kids             35
  /archive/media/2019/home             43
  /archive/media/2020/kids             22
  ...

Related locations:
  /mnt/backup/pictures/phone/                161 of 517 overlap (18,057 total)
  /mnt/sandisk-export/camera-roll/2019/dec    82 of 517 overlap (370 total)
  /mnt/sandisk-export/camera-roll/2020/jan    40 of 517 overlap (211 total)
  /mnt/backup/phone/2019-W48                  37 of 517 overlap (115 total)
  ... and 6 more locations (use --verbose to show all)
```

**Survey header**: unhashed files can't be matched. "Unique here" is content that exists nowhere else in Canon's universe.

**Archived**: your files with a copy in an archive. `--detail overlap --other <archive-path>` shows which files are at a given location.

**Related locations**: other places that share content with your selection:
- **N of M overlap**: your files that also exist at this location
- **(T total)**: all files at this location

### Notes

Survey shows the 5 most recent [notes](../manage/note.md) on the scope and the directories below it, or all of them with `--verbose`:

```
Survey: /mnt/old-drive/exports
  12 sources here (0 unhashed, 12 hashed)
  12 unique here
  Notes:
    2026-03-20  .          confirmed: 95% archived, 12 unique remain
    2026-03-19  .          looks clean
    2026-03-18  vacation/  interesting sunset photos, check originals
    2026-03-16  .          check dates
    2026-03-15  .          phone backup from 2019, mostly photos
    (2 earlier notes across 2 locations)
```

### Affinity mode (`--affinity`)

With a `--where` filter, `--affinity` shows what related locations have beyond the overlap:

```
Related locations:
  /mnt/backup-2022/photos/italy/    ≥  380 of 388 overlap (420 total)  +95 more (31 unique)
  /mnt/partner-laptop/DCIM/vacation >   45 of 388 overlap (225 total) +180 more (42 unique)
  /mnt/backup-2022/photos/misc/     ⊆   30 of 388 overlap (30 total)
```

- **+N more**: files here that match your filters but have different content from your selection
- **(K unique)**: of those, how many exist nowhere else
- **Classification symbol**: how this location relates to your selection (see below)

### The four dispositions

- **Superset** (≥): nearly everything you have, plus more matching content
- **Lead** (>): complementary content with partial overlap
- **Subset** (⊆): high overlap, no complementary content, and most of the location's own content overlaps with yours
- **Mirror** (=): overlap, no complementary content, and significant other content outside your filter

## Detail views

A detail view shows file listings in place of the summary's sections:

| Summary signal | Question | Detail view |
|---------------|----------|-------------|
| "201 of 517 archived (38.9%)" | Which files are archived, and where? | `--detail archived` |
| "264 unique here" | What content exists only here? | `--detail unique` |
| "161 of 517 overlap" | Which of my files are at that location? | `--detail overlap` |
| "+95 more" (affinity) | What matching content is over there? | `--detail complement` |
| — | What's here that's NOT at a specific location? | `--detail residual` |

`-0` gives flat, deduplicated, absolute paths from your selection.

### Archived (`--detail archived`)

```
Archived files (201 sources across 6 locations):

  Archived at /archive/media/2019/home (43 files):
    exports/photos/IMG_0001.jpg
      → media/2019/home/IMG_0001.jpg
    exports/photos/IMG_0002.jpg
      → media/2019/home/IMG_0002.jpg
    ... and 38 more

  Archived at /archive/media/2019/holiday (41 files):
    exports/vacation/DSC_0100.jpg
      → media/2019/holiday/DSC_0100.jpg
    ...
```

### Unique (`--detail unique`)

Paths of files whose content exists nowhere else:

```
photos/2016-07-14/IMG_4201.jpg
photos/2016-07-14/IMG_4202.jpg
photos/2016-07-18/DSC_0891.jpg
```

### Overlap (`--detail overlap`)

```
Overlapping with related locations (overlap):

  /mnt/backup/phone-export/ (4 of 135 overlap):
    recordings/morning-walk.m4a
      → audio/2020/morning-walk.m4a
    recordings/evening-notes.m4a
      → audio/misc/recording-001.mp3
    photos/IMG_0042.JPG
      → DCIM/2020-W48/IMG_0042.JPG
      → DCIM/2020-W48/IMG_0042 2.JPG
```

### Complement (`--detail complement`)

Requires `--where`. Files at related locations that match your filters but have different content from your selection:

```
Complementary content at related locations:

  /mnt/backup-2022/photos/italy/ (+95, 31 unique):
    week3/IMG_4501.jpg
    week3/IMG_4502.jpg
    week3/IMG_4503.jpg
    week4/IMG_4601.jpg
    week4/IMG_4602.jpg
    ... and 90 more
```

### Residual (`--detail residual`)

Requires `--other`. Your files not shared with the reference location:

```
Not at /mnt/backup/vacation/ (residual):
  photos/IMG_4201.jpg
  photos/IMG_4202.jpg
  photos/IMG_4203.raw
```

## Directed comparison (`--other`)

`--other` names locations instead of discovering them:

```bash
canon survey /mnt/old-drive/photos \
    --other /mnt/backup/vacation_italy/ \
    --other /mnt/partner-laptop/DCIM/
```
