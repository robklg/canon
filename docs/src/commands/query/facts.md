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

# Show hidden source facts
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

# Group by several keys, as a list
canon facts --key content.Make --group-by 'content.Model, source.size|bucket(1e6,1e7)'

# A key alias, in either place
canon facts --key @taken --group-by @year
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

A key prints as `--where` reads it: in Canon's quotes where it needs them. Type is the key's type, determined from all of its facts: `text`, `number`, `time`, or `path`. Count is the number of sources in scope that `<key>?` selects, so `canon ls --where '"content.File Size"?'` lists those 7,935. A key holding a control character prints it as a visible symbol (a line feed as `␊`), marked `!` after Coverage, with one line under the list: `Key marked !: a control character stands where ␊ shows, so the key cannot be pasted as printed`. A report on such a key shows the symbol wherever it prints the key, except in the `canon facts convert` command, which a shell reads as printed, and ends with the same sentence, starting `Key in the header`. A key whose facts hold more than one type lists as `mixed`, and each such key is listed under the table with its types, then the command to convert it, the key quoted for the shell where it needs quotes:

```
2 keys are of mixed type; filters and patterns support one type only:
  "content.File Size" (text and number)
  content.Model (text and number)
To see their values and convert each to one type:
  `canon facts convert 'content.File Size'`
  `canon facts convert content.Model`
```

Without `--where`, the list ends with the status words `--where` takes and the [built-in keys](../../reference/builtins.md#built-in-keys), each with its meaning:

```
Built-in keys (use in --where, --key, --group-by, --emit and patterns):
  filename       last path component
  stem           file name without its extension
  ext            same as source.ext
  hash           content hash, content.hash.sha256
  hash_short     first 8 characters of the content hash
  id             same as source.id
  size           same as source.size
  mtime          same as source.mtime
  root_id        ID of the source's root
```

## A key's values

`--key` counts the sources in scope by their value for the key. The header names the expression counted, and at the end of its line what the rows are (`text`, `number`, `time`, `path`, `time of day`, `range`, or `mixed`) and, for a source fact or a built-in key, its kind:

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

**A row reads back.** A value prints as `--where` reads it, in Canon's quotes where it needs them, so `<header>=<row>` selects exactly the sources the row counted: `canon ls` with the same arguments and `--where 'content.Model="Canon EOS 5D Mark II"'` lists those 6,102. A key the header quotes pastes the same way: `--where '"content.File Size"=4'`. The examples are written for a POSIX shell: `--where` goes in single quotes, and a value holding a single quote writes each `'` as `'\''` (`--where 'content.Genre="Rock '\''n'\'' Roll"'`). Nothing is cut, however long a value or a path.

Each line under a table has one shape, `<what is on the row>: <filter>`, and the text after `: ` is `--where` text. Two kinds of row need their line rather than their value:

- **`+`, a coarse time.** A time printed at a year, month, day or minute, beside finer values in its period, is marked `+`: `<header>=<row>` would select those finer values too. Its line selects the row exactly, with the next finer part written out.
- **`!`, a control character.** A value holding a line feed, a tab or another control character prints it as a visible symbol (`␊`, `␉`) and is marked `!`. Pasted as printed it would select nothing; its line is a glob that selects the row and perhaps a few others that differ only where the symbol stands. Copy that line from after `(may select more): `.

A time prints in [your local time](../../concepts/facts.md#how-canon-shows-a-time), at the precision it was written. Times that print the same are one row, however they were written. In the time zone `Europe/Amsterdam`, a capture time written `2023-12-31T20:00:00-00:00` and one written `2023:12:31 21:00:00` share a row, and one written only as `2024` prints as a year, beside a finer time in 2024:

```
content.DateTimeOriginal                      Count   Coverage   (time)
───────────────────────────────────────────────────────────────────────
"2023-12-31 21:00:00"                             2      50.0%
2024                                              1      25.0%  +
"2024-01-01 03:00:00"                             1      25.0%

Each year row marked +: content.DateTimeOriginal=<row> AND NOT content.DateTimeOriginal|month?
```

```
content.Note                                  Count   Coverage   (text)
───────────────────────────────────────────────────────────────────────
"line1␊line2"                                     1      25.0%  !
(no value)                                        3      75.0%

Row ! "line1␊line2" (may select more): content.Note~"line1"?"line2"
(no value): NOT content.Note?
```

Rows are ordered by count, then by value: numbers smallest first, times earliest first, text in character order, ranges by their lower edge. `(no value)` is always last, and `--limit` never hides it; when `--limit` hides rows, stderr says how many.

A modifier that asks for a finer part than a time carries counts that source under `(no value)`. A part of a time is a number to a filter, so `|month`, `|weekday` and `|quarter` rows are `number` rows, though `|weekday` prints `Monday` and `|quarter` `Q1`:

```
content.DateTimeOriginal|month                Count   Coverage   (number)
─────────────────────────────────────────────────────────────────────────
12                                                2      50.0%
01                                                1      25.0%
(no value)                                        1      25.0%

(no value): NOT content.DateTimeOriginal|month?
```

`--key source.mtime` counts by year and `--key source.size` by `|bucket`'s ranges, and the header says so (`1K` is 1,000 bytes); `mtime`, `size` and an alias for either do the same. Write a modifier to count otherwise: `--key 'source.mtime|datetime'`, `--key 'source.size|number'`.

```
source.size|bucket                            Count   Coverage   (range, source fact)
─────────────────────────────────────────────────────────────────────────────────────
100K-to-1M                                    21004      60.5%
1M-to-10M                                     12950      37.3%
1K-to-10K                                       738       2.1%
```

## Grouped output

`--by-root` and `--group-by` break each value down. The header names each grouping position, `--by-root`'s as `root`; the line under the table says how to ask for one indented row, a placeholder for each printed part:

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

`<value>` is the group's value, `<N>` a root's id and, with one grouping key, `<row>` that key's value on the row. With several grouping positions, `<1st>`, `<2nd>` … stand for the first, second … part printed on the row, the root counted. A `(no value)` part stands for `NOT <key>?`, and its own line writes that out:

```
source.ext (grouped by source.mtime|year, content.Make)

jpg (total: 12,500,  36.0%)
  2011 / Canon                                8,000   64.0%
  2012 / Canon                                3,000   24.0%
  2012 / (no value)                           1,500   12.0%

Each indented row: source.ext=<value> AND source.mtime|year=<1st> AND content.Make=<2nd>
Each indented (no value) row: source.ext=<value> AND source.mtime|year=<1st> AND NOT content.Make?
```

Inside a group, rows with a value come before rows with `(no value)`, the first part where only one of two rows has a value deciding, and then go by count, then by value. The sources with no value for the main key form the `(no value)` group, last, with indented rows of their own and their own line. When `--limit` hides groups, stderr says how many. `canon facts --by-root` with no key counts sources per root, each root printed in full with the same `id:<N>` line.

`--group-by` takes one key or a list of keys separated by commas, with or without spaces around them. A comma inside `|bucket(…)` or inside a quoted name belongs to the key. `--group-by` can be given more than once. `--key` and `--group-by` take a key [alias](../../reference/filter.md#key-aliases), shown as typed; a filter alias is refused there.

A key or modifier Canon cannot use as given is rejected before anything is counted, every cause in `--key`, `--group-by` and `--where` at once, in the [layout a filter's problems use](../../reference/filter.md#when-a-filter-is-rejected). A modifier the key's type cannot take names the modifiers that do apply:

```
Error: cannot use the key as given
  source.size|year
             ^ |year works on times or text; source.size is of type number
    Modifiers for numbers: |bucket, |number
```

A key name with a space needs no quotes here: `--key "content.File Size"` reports that key. A key with no facts is rejected as an unknown key. Where the only refused item of a `--group-by` text reads as known keys with no commas between them, Canon also shows the text with the commas added:

```
Error: cannot use the key as given
  source.ext source.mtime|year
  ^ `source.ext source.mtime` is an unknown key
    `canon facts` lists the keys
    separate grouping keys with a comma: source.ext,source.mtime|year
```

Where the `--where` filter has a problem too, the first line is the filter's: `Error: cannot run the filter as given`.

A modifier on a key of mixed type is rejected the same way, without `Error:`, since that is an answer about the key ("a modifier supports one type only"). The bare key still reports every type's rows, each with a Type column, under the statement that the key is of mixed type and the command to convert it; its value rows have no filter to paste, since a filter supports one type only. In a grouped table each of its values carries its type after it, `2019 (number)`, since values of two types can print alike. A file with no extension has no value for `source.ext`, so `--key source.ext` counts it under `(no value)`.

A save time at the edge of the years 0000–9999 or beyond has no value: it is counted under `(no value)` by `--key source.mtime`, and left out of the `source.mtime` count in the overview ([Save times](../../concepts/facts.md#save-times)).

See also: [`facts convert`](../maintain/facts-convert.md) for changing a key's type, [`facts delete`](../maintain/facts-delete.md) for removing incorrect metadata, [`prune`](../maintain/prune.md) for cleaning up stale or orphaned data.
