# Facts Reference

Facts are key-value metadata. See [Concepts: Facts](../concepts/facts.md) for an overview.

## Namespaces

| Namespace | Description |
|-----------|-------------|
| `source.*` | Facts about the file on disk (path, size, mtime) |
| `content.*` | Facts about the content (hash, EXIF, mime type) |

The `content.` prefix is optional when querying. For example, `Make=Apple` is equivalent to `content.Make=Apple`.

## Values

Facts can hold three value types, and the built-in path keys a fourth:

| Type | Examples | Notes |
|------|----------|-------|
| Text | `Apple`, `image/jpeg`, `"2019"` | Compared in character order. A value that looks like a number or a time is quoted to be text ([Filters](filter.md#values-and-types)) |
| Number | `1024`, `3.14`, `-5`, `1e6` | Compared as numbers |
| Time | `2024-07-23 14:30:45`, `2024-07`, `2024` | Printed in your local time at the precision it was written; compared as spans of time. See [How Canon interprets a time](../concepts/facts.md#how-canon-interprets-a-time) |
| Path | `source.rel_path` | Built-in keys only; compared as text, read in segments with [accessors](#path-accessors) |

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

A number outside a modifier's range, such as `|month=13`, gives an error, and so does a name in another case (`|weekday=tuesday`; for that, see [Chaining](expr.md#chaining)). A time of day has no date, so after `|time` only `|hour`, `|minute`, `|second` and the text modifiers work on it.

A time written coarser than a modifier needs has no value for it: `|month` on a time known only to `2024` has none, and neither has `|hour` on a time known only to `2024-07-23`. A time is never padded to a precision nobody wrote. Where there is no value, [`canon facts`](../commands/query/facts.md) counts the source under `(no value)`, `--where 'content.DateTimeOriginal|month?'` leaves it out, and `--where 'NOT content.DateTimeOriginal|month?'` selects it ([Filters](filter.md)).

`|datetime` and `|time` give a time at its own precision: `|datetime` prints exactly what Canon prints for the bare time (`2024-07`, `2024-07-23 14:30`), and `|time` prints `14:30` for a time known to the minute and `14:30:45` for one known to the second or finer.

### String Modifiers

These work on text and paths, and on the printed form of another modifier's result ([Chaining](expr.md#chaining)). Directly on a number or a time key they give an error.

| Modifier | Description | Example |
|----------|-------------|---------|
| `lowercase` | Convert to lowercase | `JPG` → `jpg` |
| `uppercase` | Convert to uppercase | `jpg` → `JPG` |
| `capitalize` | Capitalize first letter | `apple` → `Apple` |
| `stem` | Filename without extension | `photo.jpg` → `photo` |
| `ext` | File extension | `photo.jpg` → `jpg` |
| `short` | First 8 characters | `abc123def456` → `abc123de` |

### Numeric Modifiers

| Modifier | Takes | Gives |
|----------|-------|-------|
| `number` | Text, as a number; a number, as itself | A number. Text that is not a number has no value |
| `bucket` | A number | A range label (1-10, 10-100, etc.) |
| `bucket(a,b,c)` | A number | A range label (<a, a-b, b-c, >c) |

Example: `content.numtext|number>=10` compares text such as `9` and `10` as numbers, and `source.size|bucket` groups file sizes into human-readable ranges. A bucket's label is compared with `=`, `!=` and `IN`, typed as Canon prints it (`source.size|bucket(1e6)=">1000000"`); ordering a label gives an error.

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

- [Built-in Facts](builtins.md) - Complete list of automatic facts
- [Filters](filter.md) - Using facts in queries
- [Pattern Expressions](expr.md) - Using facts in archive patterns
