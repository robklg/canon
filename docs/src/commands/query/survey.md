# canon survey

Survey a location to understand what's here, where it connects, and what's unique. The default output is an orientation map — what's archived, which other locations share content, and how much exists only here. Use it as a starting point when arriving at a new folder, an old drive, or any scope you want to understand.

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

# Filter archive section to a specific archive
canon survey /mnt/old-drive/photos --archive path:/archive/photos

# Include excluded sources in the selection
canon survey /mnt/old-drive/photos --include excluded

# Survey all roots globally (when inside a root but want the full picture)
canon survey --global
```

## Options

| Flag | Description |
|------|-------------|
| `--where <EXPR>` | Filter expression (repeatable). Narrows the selection. |
| `--affinity` | Enable affinity columns (+N more, unique count, classification). Requires `--where`. |
| `--detail <MODE>` | `complement`, `unique`, `overlap`, or `residual`. Replaces the summary view. |
| `--archive <SPEC>` | Filter archive section to a specific archive root (`id:N` or `path:/...`). |
| `--include <VALUE>` | Include additional sources: `excluded`. |
| `--global` | Survey all roots, ignoring current directory scope. |
| `--other <PATH>` | Compare against specific locations (repeatable). Bypasses scope discovery. |
| `--brief` | Skip per-location affinity computation when `--affinity` is active. |
| `--verbose` | Show all locations (summary) or all paths per location (detail views). |
| `-0` | Null-delimited output for `--detail unique`, `--detail overlap`, or `--detail residual`. |

## Reading the output

### Summary view (default)

The default output is an **orientation** view — designed to help you understand the character of a place before deciding what to do next.

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

The output has three sections:

**Survey header**: Shows your scope, any active filters, and source counts. The unhashed/hashed split tells you how many files can participate in content comparison — unhashed files can't be matched. "Unique here" is the count of content that exists nowhere else in Canon's universe.

**Archived**: How many of your files have copies in an archive. The archive paths show *where* in the archive this content lives — the path names often reveal what past-you was thinking when you archived it. Use `--detail overlap --other <archive-path>` to see which specific files are archived at a given location.

**Related locations**: Other places in Canon's universe that share content with your selection. Each line shows:
- **N of M overlap**: How many of your files also exist at this location
- **(T total)**: How many files are at this location overall — this tells you the location's scale relative to the overlap

Use `--detail overlap` to see which of your files appear at each location. Locations are sorted by overlap count, highest first.

### Notes

If you've annotated locations with [`canon note`](../manage/note.md), those notes surface in survey output between the scope header and the statistics. Survey shows notes from the surveyed scope and its descendants (the subtree), capped at the 5 most recent:

```
Survey: /mnt/old-drive/exports
  Notes:
    2026-03-20  .              confirmed: 95% archived, 12 unique remain
    2026-03-18  vacation/      interesting sunset photos, check originals
    2026-03-15  .              phone backup from 2019, mostly photos
  (2 earlier notes across 1 location)
  517 sources here (0 unhashed, 517 hashed)
  ...
```

Each note shows a date, a relative path indicator (`.` for the scope itself, `subfolder/` for descendant locations), and the note text.

Use `--verbose` to show all notes instead of capping at 5.

When no notes exist in the subtree but ancestor scopes have notes, a summary line appears:

```
  (3 ancestral notes)
```

Related locations also show note count indicators when they have notes:

```
Related locations:
  /mnt/backup/pictures/phone/                161 of 517 overlap (18,057 total)  (2 notes)
  /mnt/sandisk-export/camera-roll/2019/dec    82 of 517 overlap (370 total)
```

### Adding filters

The summary works without any `--where` filters — it shows the full character of a location. Filters narrow what you're looking at:

```bash
# What's the story for just the images here?
canon survey /mnt/old-drive/exports --where "@image"

# What about content from a specific period?
canon survey /mnt/old-drive/exports --where "source.mtime|year=2019"
```

The same related locations may appear with different overlap counts, because the overlap is computed against your filtered selection.

### Affinity mode (`--affinity`)

When you have a `--where` filter and want to understand what related locations have *beyond* the overlap, `--affinity` adds classification columns:

```
Related locations:
  /mnt/backup-2022/photos/italy/    ≥  380 of 388 overlap (420 total)  +95 more (31 unique)
  /mnt/partner-laptop/DCIM/vacation >   45 of 388 overlap (225 total) +180 more (42 unique)
  /mnt/backup-2022/photos/misc/     ⊆   30 of 388 overlap (30 total)
```

The additional columns:
- **+N more**: Files at this location that match your filters but have *different* content from your selection — what you'd find if you went there
- **(K unique)**: Of those, how many exist nowhere else
- **Classification symbol**: How this location relates to your selection (see below)

### The four dispositions

With `--affinity`, each related location is classified:

- **Superset** (≥) — Has nearly everything you have, plus more matching content. A more complete version of what you're looking at.
- **Lead** (>) — Has complementary content with partial overlap. A related collection with additional material.
- **Subset** (⊆) — High overlap, no complementary content, and most of the location's own content overlaps with yours. A smaller copy.
- **Mirror** (=) — Overlap but no complementary content, and the location has significant other content outside your filter. A partial copy within a larger collection.

Locations are sorted by classification: supersets first, then leads, then subsets, then mirrors. Within each group, sorted by complementary count descending, then overlap count descending.

## Detail views

Detail views replace the summary with specific file listings. They answer the "show me" questions that arise from reading the summary.

| Summary signal | Question | Detail view |
|---------------|----------|-------------|
| "201 of 517 archived (38.9%)" | Which files are archived, and where? | `--detail archived` |
| "264 unique here" | What content exists only here? | `--detail unique` |
| "161 of 517 overlap" | Which of my files are at that location? | `--detail overlap` |
| "+95 more" (affinity) | What matching content is over there? | `--detail complement` |
| — | What's here that's NOT at a specific location? | `--detail residual` |

### Archived (`--detail archived`)

Shows which of your files are archived, grouped by archive location, with counterpart paths showing where each file lives in the archive:

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

Locations are sorted by file count (most files first). When results are small (20 or fewer per location), all paths are shown; otherwise capped at 5. Use `--verbose` to see all. With `-0`, output is flat, deduplicated selection-side paths only (for piping to `xargs -0`).

Use `--archive` to filter to a specific archive root.

### Unique (`--detail unique`)

Outputs paths of files whose content exists nowhere else:

```
photos/2016-07-14/IMG_4201.jpg
photos/2016-07-14/IMG_4202.jpg
photos/2016-07-18/DSC_0891.jpg
```

Paths are relative when the scope is under the current directory, absolute otherwise. Use `-0` for null-delimited absolute paths (for `xargs -0`).

### Overlap (`--detail overlap`)

Shows which of your files have copies at each related location, along with the counterpart paths at that location:

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

Each `→` line shows where the matching content lives at the other location. Multiple counterparts appear when the same content exists more than once (e.g., OS-generated duplicates like `IMG_0042 2.JPG`). Counterpart paths are relative to the location.

When results are small (20 or fewer), all paths are shown. For larger results, paths are capped at 5 per location; use `--verbose` to see all. With `-0`, output is flat and deduplicated selection-side paths only (no counterpart data) — for piping to `xargs -0`.

### Complement (`--detail complement`)

Requires `--where`. Shows files at related locations that match your filters but have different content from your selection. Implies affinity computation.

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

Paths are relative to the location. When results are small (20 or fewer), all paths are shown; otherwise capped at 5 per location. Use `--verbose` to see all.

### Residual (`--detail residual`)

Requires `--other`. Shows which of your files are NOT shared with the reference location:

```
Not at /mnt/backup/vacation/ (residual):
  photos/IMG_4201.jpg
  photos/IMG_4202.jpg
  photos/IMG_4203.raw
```

Unhashed files are always included in residual output — without a hash, their presence at the reference location can't be confirmed. Use `-0` for flat output. With multiple `--other` locations, each gets a separate listing.

## Directed comparison (`--other`)

By default, survey discovers related locations by searching Canon's full universe for content overlap. `--other` lets you specify locations directly:

```bash
canon survey /mnt/old-drive/photos \
    --other /mnt/backup/vacation_italy/ \
    --other /mnt/partner-laptop/DCIM/
```

Differences from default mode:
- Header reads "Comparing with:" instead of "Related locations:"
- Locations are displayed in user-specified order (not sorted)
- In `--detail complement`, mirrors are shown with a note rather than omitted

Archive status and unique counts are always computed against the full universe regardless of `--other`.

## How exploration typically flows

Survey supports a non-linear exploration style. You might follow any of these paths depending on what the summary reveals:

**Orientation**: Arrive at a location, survey it, read the landscape. The archive paths and location names often tell you what a place *is* — a phone backup, a project folder, a parking dump. From here you might scope down to a subfolder, add `--where` filters, or drill into a detail view.

**Following a thread**: A related location catches your eye. Survey it directly (`canon survey <that-path>`) to understand it. Check `--detail overlap` to see which files connect the two places. Use `canon facts` to understand what metadata is available, then refine with `--where`.

**Assessing coverage**: Use `--affinity` with a `--where` filter to see which locations have more matching content. Drill into `--detail complement` to see what's there. Use `--detail residual --other <location>` to see what's *not* covered.

**Acting on results**: Pipe `--detail unique -0` or `--detail overlap -0` to downstream tools — `xargs -0 open` for inspection, `xargs -0 ls -la` for sizes, or further processing when you're ready.
