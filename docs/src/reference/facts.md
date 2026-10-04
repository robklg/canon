# Facts Reference

Facts are key-value metadata. See [Concepts: Facts](../concepts/facts.md) for an overview.

## Namespaces

| Namespace | Description |
|-----------|-------------|
| `source.*` | Facts about the file on disk (path, size, mtime) |
| `content.*` | Facts about the content (hash, EXIF, mime type) |

The `content.` prefix is optional when querying. For example, `Make=Apple` is equivalent to `content.Make=Apple`.

## Values

Facts can hold three value types:

| Type | Examples | Notes |
|------|----------|-------|
| Text | `"Apple"`, `"image/jpeg"` | Strings; quote if contains spaces |
| Number | `1024`, `3.14`, `-5` | Integers or decimals |
| Time | `2024-07-23 14:30:45`, `2024-07`, `2024` | Printed on your clock at the precision it was written; enable time modifiers. See [How Canon reads a time](../concepts/facts.md#how-canon-reads-a-time) |

## Modifiers

Transform values using `|` syntax:

### Time Modifiers

For time values (like `source.mtime` or EXIF dates). Each reads the time on [your clock](../concepts/facts.md#how-canon-shows-a-time), and no output carries an offset. The examples read the time `2024-07-23 14:30:45`:

| Modifier | Output | Example | Needs a time known to the |
|----------|--------|---------|---------------------------|
| `year` | 4-digit year | `2024` | year |
| `month` | 2-digit month | `07` | month |
| `day` | 2-digit day | `23` | day |
| `hour` | 2-digit hour (24h) | `14` | minute |
| `minute` | 2-digit minute | `30` | minute |
| `second` | 2-digit second | `45` | second |
| `date` | Date | `2024-07-23` | day |
| `time` | Time of day, at the time's own precision | `14:30:45` | minute |
| `datetime` | The time as Canon prints it, at its own precision | `2024-07-23 14:30:45` | year |
| `yearmonth` | Year-month | `2024-07` | month |
| `week` | ISO week number, 2 digits | `30` | day |
| `weekday` | Day of the week, in English | `Tuesday` | day |
| `quarter` | Quarter | `Q3` | month |

A time written coarser than a modifier needs has no value for it: `|month` on a time known only to `2024` has none, and neither has `|hour` on a time known only to `2024-07-23`. A time is never padded to a precision nobody wrote. Where there is no value, [`canon facts`](../commands/query/facts.md) counts the source under `(no value)`, `--where 'content.DateTimeOriginal|month?'` leaves it out, and `--where 'NOT content.DateTimeOriginal|month?'` selects it ([Filters](filter.md)).

`|datetime` and `|time` give a time at its own precision: `|datetime` prints exactly what Canon prints for the bare time (`2024-07`, `2024-07-23 14:30`), and `|time` prints `14:30` for a time known to the minute and `14:30:45` for one known to the second or finer.

### String Modifiers

| Modifier | Description | Example |
|----------|-------------|---------|
| `lowercase` | Convert to lowercase | `JPG` → `jpg` |
| `uppercase` | Convert to uppercase | `jpg` → `JPG` |
| `capitalize` | Capitalize first letter | `apple` → `Apple` |
| `stem` | Filename without extension | `photo.jpg` → `photo` |
| `ext` | File extension | `photo.jpg` → `jpg` |
| `short` | First 8 characters | `abc123def456` → `abc123de` |

### Numeric Modifiers

| Modifier | Description |
|----------|-------------|
| `bucket` | Group into ranges (1-10, 10-100, etc.) |
| `bucket(a,b,c)` | Custom ranges (<a, a-b, b-c, >c) |

Example: `source.size|bucket` groups file sizes into human-readable ranges.

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
