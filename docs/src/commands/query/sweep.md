# canon sweep

Reduction opportunities across the whole [universe](../../concepts/resolution.md): the ranked places where one dismissal resolves the most.

```bash
# The leaderboard: the ten best reduction opportunities, ranked
canon sweep

# Everything: all entries, all members, findings below the emit floors
canon sweep --all
```

The sweep takes no paths and no filters, and is computed fresh on every run.

## Options

| Flag | Description |
|------|-------------|
| `--limit <N>` | Show up to N leaderboard entries (default: 10). |
| `--all` | No cap: every entry, every place of a multi-place entry, and findings below the emit floors. |

## Reading a finding

```
#1  /Volumes/OldBackup/ARCHIVED/Super8
    96% inside /Volumes/Archive/Media/Super8  (by size · 89% by count)
    counterpart: archived, scanned 5d ago · subject scanned 2d ago
    gain: 1,204 files · 33.7 GB     residual: 1 file · 3.9 GB nowhere else
    → canon survey /Volumes/OldBackup/ARCHIVED/Super8 --other /Volumes/Archive/Media/Super8
```

- **Subject** (the first line): the place the finding is about, the side you might dismiss
- **Counterpart**: where the copies live, with its [standing](../../concepts/resolution.md#standings) and each side's scan age
- **Relation**: `inside` a larger counterpart, or `mirrors` one in both directions; a large gap between "by size" and "by count" means many small files
- **Gain**: what acting on the finding resolves
- **Residual**: content existing nowhere else in the universe; `residual: none` means a clean dismissal
- **`→` line**: the command to judge the finding; an entry with none has no single place on a source root to compare

Excluded content creates no findings and blocks no dismissal.

### Scattered findings

When no single counterpart holds the copies, the finding says `scattered`:

```
#4  /Volumes/Backup/laptop-import/mixed
    94% exists elsewhere, across 7 locations (2 archived · 3 suspended)
    scattered; consolidation candidate · subject scanned 12d ago
```

### Hubs

Many places with copies in one counterpart show as a single entry, with the counterpart on its first line:

```
#2  /Volumes/Archive/Media/iphone-backup
    shared counterpart — 36 places hold copies inside it · archived, scanned 5d ago
    total gain: 7,820 files · 41.2 GB
      /Volumes/OldBackup/iphone-2019  98% inside · 402 files · 2.1 GB
      /Volumes/old-disk/dumps/phone   97% inside · 371 files · 1.9 GB
      … 34 more (--all)
    → canon survey /Volumes/OldBackup/iphone-2019 --other /Volumes/Archive/Media/iphone-backup
```

### Roots close to done

A source root with few [unresolved](../../concepts/resolution.md) sources left shows as one entry, with the root on its first line:

```
#1  /Volumes/oldmac  (whole root)
    3 unresolved sources remain
    6 places, up to 1,204 files · 41.3 GB
      /Volumes/oldmac/Pictures/2019  mirrors · 100% · archived · 612 files · 22.1 GB
      /Volumes/oldmac/Pictures/2020  98% inside · archived · 431 files · 14.8 GB
      … 4 more (--all)
    → canon roots retire path:/Volumes/oldmac --dry-run
```

The count is what the [retirement](../roots/retire.md) review measures.

The `up to` figure is an upper bound: two places can each have copies of the other, and only one of them can be let go.

### Sibling directories under one parent

Several places under one parent, each with its own counterpart, show as one entry, with the parent on its first line:

```
#1  /Volumes/Backup/photos
    10 places under here · 88% of this folder
    up to 4,102 files · 61.4 GB
      /Volumes/Backup/photos/set-01  mirrors · 100% · archived · 402 files · 6.1 GB
      /Volumes/Backup/photos/set-02  98% inside · archived · 388 files · 5.9 GB
      … 8 more (--all)
    → canon survey /Volumes/Backup/photos
```

The percentage is the share of the parent's own sources that lie under those places; the `→` line surveys the whole parent.

## Ranking

Findings rank by these factors, in order, each visible on the finding:

1. **Cleanliness**: ready-to-assess findings above consolidation candidates.
2. **Archive standing**: a place on a source root above an equivalent place in the archive.
3. **Root nearness**: a place on a source root with little left unresolved above a place on a root barely started. Archive roots tie here.
4. **Weight**: gain, by size.
5. **Counterpart standing**: archived above present; scattered content with nothing archived last.
6. **Residual**: less content existing nowhere else ranks higher.

A small place on a root close to done outranks a large one elsewhere, and says so on its own line:

```
#1  /Volumes/oldmac/Pictures/2019
    mirrors /Volumes/Archive/Media/2019  (100% by size · 100% by count)
    counterpart: archived, scanned today · subject scanned today
    20 unresolved sources remain on /Volumes/oldmac
    gain: 30 files · 61.4 KB     residual: none
    → canon survey /Volumes/oldmac/Pictures/2019 --other /Volumes/Archive/Media/2019
```

## Suspended roots

Places on a [suspended](../../concepts/roots.md#suspended-roots) root are not ranked, and neither are places whose counterpart is on one. Each such root gets a footer line:

```
/Volumes/OldBackup suspended — not ranked: 8 places on it (up to 185.3 GB), 4 with copies on it (up to 12.1 GB) · canon roots unsuspend path:/Volumes/OldBackup
```

`--all` does not reveal these places.
