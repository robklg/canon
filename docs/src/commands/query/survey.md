# canon survey

Survey a selection for archive status, related locations, and unique content. Use after shaping a selection with `ls` to understand the outward context: what's already resolved, where complementary content lives, and whether to work from here or shift focus.

```bash
# Survey current directory
canon survey

# Survey a specific path
canon survey /mnt/old-drive/photos

# Survey with filters (enables affinity columns: +N more, only here)
canon survey /mnt/old-drive/photos --where "@image AND source.mtime|year=2016"

# Fast first pass — skip affinity computation
canon survey /mnt/old-drive/photos --where "@image" --brief

# Compare against specific locations instead of discovering them
canon survey /mnt/old-drive/photos --where "@image" --other /mnt/backup/vacation/

# See what complementary content exists at related locations
canon survey /mnt/old-drive/photos --where "@image" --detail complement

# List unique-to-scope paths (pipeable)
canon survey /mnt/old-drive/photos --detail unique

# Null-delimited unique paths for xargs
canon survey /mnt/old-drive/photos --detail unique -0 | xargs -0 ls -la

# Filter archive section to a specific archive
canon survey /mnt/old-drive/photos --archive path:/archive/photos

# Include excluded sources in the selection
canon survey /mnt/old-drive/photos --include excluded
```

## Options

| Flag | Description |
|------|-------------|
| `--where <EXPR>` | Filter expression (repeatable). Enables affinity columns (+N more, only here). |
| `--detail <MODE>` | `complement` or `unique`. Replaces the summary view. |
| `--archive <SPEC>` | Filter archive section to a specific archive root (`id:N` or `path:/...`). |
| `--include <VALUE>` | Include additional sources: `excluded`. |
| `--other <PATH>` | Compare against specific locations (repeatable). Bypasses scope discovery. |
| `--brief` | Skip per-location affinity computation. Mutually exclusive with `--detail`. |
| `--verbose` | Show all locations (summary) or all paths per location (complement). |
| `-0` | Null-delimited output for `--detail unique`. |

## Reading the output

### Summary view (default)

```
Selection: /mnt/old-drive/photos
  Filters: @image AND source.mtime|year=2016
  400 sources (12 unhashed, 388 hashed)

Archived: 285 of 388 (73.5%)
  /archive/photos/2016/                      285

Related locations:
  /mnt/backup-2022/photos/italy-2016/     380 of 388 shared   +95 more (31 only here)
  /mnt/partner-laptop/DCIM/vacation/       45 of 388 shared  +180 more (42 only here)
  /mnt/backup-2022/photos/misc/            30 of 388 shared

76 unique to this scope
```

**Selection**: Echoes your query — scope, filters, source counts. The unhashed/hashed split is a data quality signal; unhashed sources can't participate in content comparison.

**Archived**: How many hashed sources have content in an archive. Archive paths show where archived content lives within the archive's directory structure.

**Related locations**: Places that share content with your selection. Each shows:
- **N of M shared**: How many of your selection's hashed sources have identical content at this location
- **+N more** (only with `--where`): Additional sources at this location matching your filters but with different content — the complementary content
- **(K only here)** (only with `--where` and +N more > 0): How many of the complementary sources have content that exists nowhere else

**Unique**: Content that exists only within your scope — nowhere else in the universe.

### The three dispositions

Each related location falls into one of three categories:

- **Superset** — High shared count AND complementary content. The location has nearly everything you have plus more. Consider shifting your focal point there.
- **Lead** — Complementary content with partial overlap. A different but related collection worth investigating for additional material.
- **Mirror** — Overlap but no complementary content. A partial copy, useful for reduction but not a source of new content.

Locations are sorted by classification: supersets first, then leads, then mirrors. Within each group, sorted by complementary count descending, then shared count descending.

Without `--where`, affinity columns are absent and locations are sorted by shared count descending.

### Complement detail (`--detail complement`)

Requires `--where`. Shows the actual files at related locations:

```
Complementary content at related locations:

  /mnt/backup-2022/photos/italy-2016/ (+95, 31 only here):
    week3/IMG_4501.jpg
    week3/IMG_4502.jpg
    week3/IMG_4503.jpg
    week4/IMG_4601.jpg
    week4/IMG_4602.jpg
    ... and 90 more
```

Paths are relative to the location for compactness. Each location shows up to 5 paths; use `--verbose` to see all.

### Unique detail (`--detail unique`)

Outputs bare paths of sources whose content exists nowhere else:

```
/mnt/old-drive/photos/2016-07-14/IMG_4201.jpg
/mnt/old-drive/photos/2016-07-14/IMG_4202.jpg
/mnt/old-drive/photos/2016-07-18/DSC_0891.jpg
```

Suitable for piping. Use `-0` for null-delimited output (for `xargs -0`).

## Directed comparison (`--other`)

By default, Canon discovers related locations by searching the full universe via hash overlap. `--other` lets you specify locations directly:

```bash
canon survey /mnt/old-drive/photos \
    --where "@image" \
    --other /mnt/backup/vacation_italy/ \
    --other /mnt/partner-laptop/DCIM/
```

Differences from default mode:
- Header reads "Comparing with:" instead of "Related locations:"
- Locations are displayed in user-specified order (not sorted by classification)
- In `--detail complement`, mirrors are shown with a note rather than omitted

Archive status and unique counts are always computed against the full universe regardless of `--other`.

## Typical workflow

1. **Explore**: `canon ls /mnt/old-drive/photos --where "@image"` — shape your selection
2. **Assess**: `canon survey /mnt/old-drive/photos --where "@image"` — see the landscape
3. **Investigate**: `canon survey ... --detail complement` — see what's at related locations
4. **Focus**: `canon survey ... --other /mnt/backup/sibling-dir/` — targeted comparison
5. **Cluster**: `canon cluster generate ...` — when ready to assemble
