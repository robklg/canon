# canon facts

Discover what [facts](../../concepts/facts.md) you have and check coverage.

```bash
# Overview of all facts (scoped to current directory when inside a root)
canon facts

# Scoped to a specific directory
canon facts /path/to/photos

# Global overview across all roots
canon facts --global

# With filters
canon facts --where 'source.ext|lowercase=jpg'

# Value distribution for a specific fact
canon facts --key content.Make

# With modifiers: group mtime by year-month
canon facts --key 'source.mtime|yearmonth'

# With accessors: distribution by top-level directory
canon facts --key source.rel_path[0]

# Combine accessor and modifier: distribution by filename extension
canon facts --key 'source.rel_path[-1]|ext'

# Show hidden built-in facts
canon facts --all

# Unlimited results (default is 50)
canon facts --key content.hash.sha256 --limit 0

# Include sources from archive roots
canon facts --include archived

# Include excluded sources
canon facts --include excluded

# Include both
canon facts --include all

# Show source count per root (which roots have matching content?)
canon facts --by-root
canon facts --where '@image' --by-root

# Group fact values by root (which roots contribute to each value?)
canon facts --key source.ext --by-root

# Count extensions regardless of letter case
canon facts --key 'source.ext|lowercase'

# Group by any fact key (with modifiers)
canon facts --key source.ext --group-by 'source.mtime|year'

# Compound grouping (root + another fact)
canon facts --key source.ext --by-root --group-by 'content.Make'
```

The output begins with a scope header showing what's being queried (`Facts: /path` or `Facts: all roots`). It is printed whether or not anything matched, so a report of nothing says where Canon looked:

```
Facts: /mnt/old-drive/photos
No sources match the given filters.
```

A path on a [suspended](../roots/roots.md#suspending-roots) root, or the current directory inside one, is set aside and stated instead of counted; when that is the whole scope, nothing is reported and the exit code is 1. `facts delete` is rejected there by name.

Example output:
```
Facts: all roots
Sources matching filters: 34692

Fact                             Type      Count   Coverage
────────────────────────────────────────────────────────────
source.ext                       text      34692     100.0%  (built-in)
source.size                    number      34692     100.0%  (built-in)
source.mtime                     time      34692     100.0%  (built-in)
source.path                      path      34692     100.0%  (derived)
source.rel_path                  path      34692     100.0%  (built-in)
filename                         text      34692     100.0%  (derived)
content.Make                     text       7935      22.9%
content.Model                    text       7935      22.9%
content.hash.sha256              text      34692     100.0%
content.mime                     text      34692     100.0%

(4 built-in/derived facts hidden, use --all to show)
...
```

Type is the key's type, determined from all of its facts: `text`, `number`, `time`, or `path`. Count and Coverage are for the sources in scope. A key whose facts hold more than one type lists as `mixed`, and each such key is listed under the table, with the command to convert it:

```
2 keys are of mixed type, so filters and patterns cannot use their values:
only values of one type can be compared or formatted correctly.
To inspect each and learn how to convert to one type:
  `canon facts convert content.CreateDate`
  `canon facts convert '"content.File Size"'`
```

Example grouped output (`--by-root`):
```
source.ext (by root)

jpg (total: 12,500, 36.0%)
  id:1  ...OldBackup/Pictures             8,000   64.0%
  id:2  ...laptop/photos-backup           4,500   36.0%

png (total: 8,200, 23.6%)
  id:1  ...OldBackup/Pictures             5,000   61.0%
  id:3  ...laptop-import/media            3,200   39.0%
```

A time prints in [your local time](../../concepts/facts.md#how-canon-shows-a-time), at the precision it was written. Times that print the same are one row, however they were written. In the time zone `Europe/Amsterdam`, a capture time written `2023-12-31T20:00:00-00:00` and one written `2023:12:31 21:00:00` share a row, and one written only as `2024` prints as a year:

```
Facts: all roots
Sources matching filters: 4

content.DateTimeOriginal                      Count   Coverage
──────────────────────────────────────────────────────────────
2023-12-31 21:00:00                               2      50.0%
2024                                              1      25.0%
2024-01-01 03:00:00                               1      25.0%
```

A modifier that asks for a finer part than a time carries counts that source under `(no value)`:

```
Facts: all roots
Sources matching filters: 4

DateTimeOriginal|month                        Count   Coverage
──────────────────────────────────────────────────────────────
12                                                2      50.0%
01                                                1      25.0%
(no value)                                        1      25.0%
```

A key or modifier Canon cannot use as written is rejected before anything is counted, every cause in `--key`, `--group-by` and `--where` at once. Each names a rewritten key where one works (`source.size|lowercase` names `source.size|number|lowercase`), and otherwise the modifiers that do apply to the key's type:

```
Error: cannot use the key as written
  source.size|year: |year works on times or text; source.size holds numbers.
    Modifiers for numbers: |bucket, |number
```

A modifier on a key whose facts hold two types is rejected the same way, without `Error:`, since that is an answer about the key; the bare key still reports both types' rows.

`--key source.mtime` groups save times by year in your local time. A save time at the edge of the years 0000–9999 or beyond has no value: it is counted under `(no value)` there, and left out of the `source.mtime` count in the overview ([Save times](../../concepts/facts.md#save-times)).

See also: [`facts delete`](../maintain/facts-delete.md) for removing incorrect metadata, [`prune`](../maintain/prune.md) for cleaning up stale or orphaned data.
