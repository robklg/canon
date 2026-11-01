# canon facts

The [facts](../../concepts/facts.md) you have, and their coverage.

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

# Show hidden source facts
canon facts --all

# Unlimited results (default is 50)
canon facts --key content.hash.sha256 --limit 0

# Show source count per root (which roots have matching content?)
canon facts --by-root

# Group fact values by root (which roots contribute to each value?)
canon facts --key source.ext --by-root

# Group by any fact key (with modifiers)
canon facts --key source.ext --group-by 'source.mtime|year'

# Compound grouping (root + another fact)
canon facts --key source.ext --by-root --group-by 'content.Make'

# Group by several keys, as a list
canon facts --key content.Make --group-by 'content.Model, source.size|bucket(1e6,1e7)'

# A key alias, in either place
canon facts --key @taken --group-by @year
```

```
Facts: all roots
Sources matching filters: 34692

Key                              Type      Count   Coverage
────────────────────────────────────────────────────────────
source.ext                       text      34692     100.0%  (source fact)
source.size                    number      34692     100.0%  (source fact)
source.mtime                     time      34692     100.0%  (source fact)
source.path                      path      34692     100.0%  (source fact)
source.rel_path                  path      34692     100.0%  (source fact)
filename                         text      34692     100.0%  (built-in key)
"content.File Size"            number       7935      22.9%
content.Make                     text       7935      22.9%
content.Model                    text       7935      22.9%
content.hash.sha256              text      34692     100.0%
content.mime                     text      34692     100.0%

(4 source facts hidden, use --all to show)
...
```

A key is shown the way you write it in `--where`. Count is the number of sources in scope that `<key>?` selects. A key with values of [more than one type](../../concepts/facts.md#value-types) is listed as `mixed`:

```
2 keys are of mixed type; filters and patterns support one type only:
  "content.File Size" (text and number)
  content.Model (text and number)
To see their values and convert each to one type:
  `canon facts convert 'content.File Size'`
  `canon facts convert content.Model`
```

## A key's values

`--key` counts the sources in scope by their value for the key:

```
$ canon facts --key content.Model
Facts: all roots
Sources matching filters: 12480

content.Model                                 Count   Coverage   (text)
───────────────────────────────────────────────────────────────────────
"Canon EOS 5D Mark II"                         6102      48.9%
"iPhone 6s"                                    3977      31.9%
DMC-FZ38                                        820       6.6%
""                                               14       0.1%
(no value)                                     1567      12.6%

(no value): NOT content.Model?
```

A value is shown the way you write it in `--where`, so `<header>=<row>` selects exactly the sources the row counted ([quoting](../../reference/filter.md#quoting)).

Each line under a table is `<what is on the row>: <filter>`, with `--where` text after the `: `. A row marked `+` is a coarse time beside finer values in its period. `<header>=<row>` would select those too, so use the row's line.

A time shows in [your local time](../../concepts/facts.md#how-canon-shows-a-time), at the precision it was written, and times that show the same are one row:

```
content.DateTimeOriginal                      Count   Coverage   (time)
───────────────────────────────────────────────────────────────────────
"2023-12-31 21:00:00"                             2      50.0%
2024                                              1      25.0%  +
"2024-01-01 03:00:00"                             1      25.0%

Each year row marked +: content.DateTimeOriginal=<row> AND NOT content.DateTimeOriginal|month?
```

`--limit` never hides `(no value)`; when it hides rows, stderr says how many.

`--key source.mtime` counts by year and `--key source.size` by range (`1K` is 1,000 bytes). Write a modifier to count otherwise: `--key 'source.mtime|datetime'`, `--key 'source.size|number'`.

```
source.size|bucket                            Count   Coverage   (range, source fact)
─────────────────────────────────────────────────────────────────────────────────────
100K-to-1M                                    21004      60.5%
1M-to-10M                                     12950      37.3%
1K-to-10K                                       738       2.1%
```

## Grouped output

`--by-root` and `--group-by` break each value down:

```
source.ext (grouped by root)

jpg (total: 12,500,  36.0%)
  id:1  /Volumes/OldBackup/Pictures           8,000   64.0%
  id:2  /Users/me/laptop/photos-backup        4,500   36.0%

png (total:  8,200,  23.6%)
  id:1  /Volumes/OldBackup/Pictures           5,000   61.0%
  id:3  /Volumes/laptop-import/media          3,200   39.0%

Each indented row: source.ext=<value> AND root_id=<N>
id:<N> (not id=, which names a source): root_id=<N>
```

With several grouping keys, `<1st>`, `<2nd>` … stand for the parts printed on the row, and the root counts as a part:

```
source.ext (grouped by source.mtime|year, content.Make)

jpg (total: 12,500,  36.0%)
  2011 / Canon                                8,000   64.0%
  2012 / Canon                                3,000   24.0%
  2012 / (no value)                           1,500   12.0%

Each indented row: source.ext=<value> AND source.mtime|year=<1st> AND content.Make=<2nd>
Each indented (no value) row: source.ext=<value> AND source.mtime|year=<1st> AND NOT content.Make?
```

`--group-by` takes one key or a comma-separated list, and can be given more than once. `--key` and `--group-by` take a key [alias](../../reference/filter.md#key-aliases).

A key name with a space needs no quotes here: `--key "content.File Size"` reports that key.

See also: [`facts convert`](../maintain/facts-convert.md) for changing a key's type, [`facts delete`](../maintain/facts-delete.md) for removing incorrect metadata, [`prune`](../maintain/prune.md) for cleaning up stale or orphaned data.
