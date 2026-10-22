# Facts Reference

Facts are key-value metadata. See [Concepts: Facts](../concepts/facts.md) for an overview.

## Namespaces

| Namespace | Description |
|-----------|-------------|
| `source.*` | Source facts: read from what the scanner records (path, size, mtime) |
| `content.*` | Content facts: imported (EXIF, mime type), and the content hash |

The `content.` prefix is optional when querying. For example, `Make=Apple` is equivalent to `content.Make=Apple`. A name that is a [built-in key](builtins.md#built-in-keys), such as `size` or `filename`, reads as that key.

## Values

Facts can hold three value types, and the path source facts a fourth:

| Type | Examples | Notes |
|------|----------|-------|
| Text | `Apple`, `image/jpeg`, `"2019"` | Compared in character order. A value that reads as a number or a time is quoted to be text ([Filters](filter.md#values-and-types)) |
| Number | `1024`, `3.14`, `-5`, `1e6` | Compared as numbers |
| Time | `2024-07-23 14:30:45`, `2024-07`, `2024` | Printed in your local time at the precision it was written; compared as spans of time. See [How Canon interprets a time](../concepts/facts.md#how-canon-interprets-a-time) |
| Path | `source.rel_path` | Source facts only; compared as text, read in segments with [accessors](#path-accessors) |

## Modifiers

Transform values using `|` syntax:

### Time Modifiers

For time values (like `source.mtime` or EXIF dates), and for text written as a time. Each works in [your local time](../concepts/facts.md#how-canon-shows-a-time), and no output has an offset. A modifier gives a number or a time, which is how a filter compares it; the Prints column is how `facts` and a pattern show it. The examples use the time `2024-07-23 14:30:45`:

| Modifier | Gives | Prints | In a filter | Needs a time known to the |
|----------|-------|--------|-------------|---------------------------|
| `year` | A number | `2024` | `\|year=2024` | year |
| `month` | A number, 1–12 | `07` | `\|month=7` or `07` | month |
| `day` | A number, 1–31 | `23` | `\|day=23` | day |
| `hour` | A number, 0–23 | `14` | `\|hour>=14` | minute |
| `minute` | A number, 0–59 | `30` | `\|minute=30` | minute |
| `second` | A number, 0–59 | `45` | `\|second=45` | second |
| `date` | A day | `2024-07-23` | `\|date=2024-07-23` | day |
| `time` | A time of day, at the time's own precision | `14:30:45` | `\|time>=18:00` | minute |
| `datetime` | The time, at its own precision | `2024-07-23 14:30:45` | `\|datetime="2024-07-23 14:30"` | year |
| `yearmonth` | A month | `2024-07` | `\|yearmonth=2024-07` | month |
| `week` | A number, the ISO week 1–53 | `30` | `\|week=30` | day |
| `weekday` | A number, 1–7 from Monday | `Tuesday` | `\|weekday=2` or `Tuesday` | day |
| `quarter` | A number, 1–4 | `Q3` | `\|quarter=3` or `Q3` | month |

With `=`, `!=` or `IN`, a number outside a modifier's range, such as `|month=13`, gives an error, and so does a name in another case (`|weekday=tuesday`; for that, see [Chaining](expr.md#chaining)). Ordered, any whole number works: `|hour<24`, `|month>0`. A year is written with four digits. A time of day has no date, so after `|time` only `|hour`, `|minute`, `|second` and the text modifiers work on it; those three also read a time of day stored as text (`14:30:00`).

`|week` is the ISO week, whose week 01 can start in the year before: beside `|year`, a pattern `{content.Taken|year}/{content.Taken|week}` puts 2024-12-30 in `2024/01`. Canon has no modifier for the year an ISO week belongs to.

A time written coarser than a modifier needs has no value for it: `|month` on a time known only to `2024` has none, and neither has `|hour` on a time known only to `2024-07-23`. A time is never padded to a precision nobody wrote. Where there is no value, [`canon facts`](../commands/query/facts.md) counts the source under `(no value)`, `--where 'content.DateTimeOriginal|month?'` leaves it out, and `--where 'NOT content.DateTimeOriginal|month?'` selects it ([Filters](filter.md)).

`|datetime` and `|time` give a time at its own precision: `|datetime` prints exactly what Canon prints for the bare time (`2024-07`, `2024-07-23 14:30`), and `|time` prints `14:30` for a time known to the minute and `14:30:45` for one known to the second or finer.

### String Modifiers

These work on text and paths, and on the printed form of another modifier's result ([Chaining](expr.md#chaining)). Directly on a number or a time key they give an error. On a path, `lowercase`, `uppercase` and `capitalize` give a path, and `capitalize` changes each name.

| Modifier | Description | Example |
|----------|-------------|---------|
| `lowercase` | Convert to lowercase | `JPG` → `jpg` |
| `uppercase` | Convert to uppercase | `jpg` → `JPG` |
| `capitalize` | Capitalize the first letter, the rest lower case; on a path, each name | `apple` → `Apple`, `my photos/IMG.JPG` → `My photos/Img.jpg` |
| `stem` | Filename without extension | `photo.jpg` → `photo` |
| `ext` | File extension; a name with none (`README`, `.bashrc`, `trail.`) has no value | `photo.jpg` → `jpg` |
| `short` | First 8 characters | `abc123def456` → `abc123de` |

### Numeric Modifiers

| Modifier | Takes | Gives |
|----------|-------|-------|
| `number` | Text, as a number; a number, as itself | A number. Text that is not a number has no value |
| `bucket` | A number from 0 up | A range label: `0`, then powers of ten, `1-to-10`, `10-to-100`, `1K-to-10K`, `0.00001-to-0.0001`. A number below zero has no value: \|bucket groups numbers from 0 up |
| `bucket(a,b,c)` | Any number, negative ones included | A range label: `under-a`, `a-to-b`, `b-to-c`, `c-and-up`, a number below zero written with `minus-` (`\|bucket(-90,0,90)` gives `under-minus-90`, `minus-90-to-0`, `0-to-90`, `90-and-up`) |

Example: `content.numtext|number>=10` compares text such as `9` and `10` as numbers, and `source.size|bucket` groups file sizes by order of magnitude. A label holds only letters, digits, `-` and `.`. It is compared with `=`, `!=` and `IN`, typed as Canon prints it (`source.size|bucket(1e6)=1000000-and-up`); ordering a label gives an error.

## Path Accessors

Python-style indexing for path values:

| Syntax | Meaning |
|--------|---------|
| `key[-1]` | Last segment (filename) |
| `key[0]` | First segment |
| `key[1:3]` | Slice segments 1 and 2 |
| `key[:-1]` | All but last segment |

Accessors can be combined with modifiers:

```
source.rel_path[-1]        → IMG_001.jpg
source.rel_path[-1]|stem   → IMG_001
source.rel_path[0]         → photos
```

## See Also

- [Source Facts and Built-in Keys](builtins.md) - The keys Canon provides without enrichment
- [Filters](filter.md) - Using facts in queries
- [Pattern Expressions](expr.md) - Using facts in archive patterns
