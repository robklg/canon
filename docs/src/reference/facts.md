# Facts Reference

Facts are key-value metadata. See [Concepts: Facts](../concepts/facts.md) for an overview.

## Namespaces

| Namespace | Description |
|-----------|-------------|
| `source.*` | Facts about the file on disk (path, size, mtime) |
| `content.*` | Facts about the content (hash, EXIF, mime type) |
| `object.*` | Object-level properties |

The `content.` prefix is optional when querying. For example, `Make=Apple` is equivalent to `content.Make=Apple`.

## Values

Facts can hold three value types:

| Type | Examples | Notes |
|------|----------|-------|
| Text | `"Apple"`, `"image/jpeg"` | Strings; quote if contains spaces |
| Number | `1024`, `3.14`, `-5` | Integers or decimals |
| Timestamp | `1704067200` | Unix timestamps; enable date modifiers |

## Modifiers

Transform values using `|` syntax:

### Time Modifiers

For timestamp values (like `source.mtime` or EXIF dates):

| Modifier | Output | Example |
|----------|--------|---------|
| `year` | 4-digit year | `2024` |
| `month` | 2-digit month | `07` |
| `day` | 2-digit day | `23` |
| `hour` | 2-digit hour (24h) | `14` |
| `minute` | 2-digit minute | `30` |
| `second` | 2-digit second | `45` |
| `date` | ISO date | `2024-07-23` |
| `time` | ISO time | `14:30:45` |
| `datetime` | ISO datetime | `2024-07-23T14:30:45` |
| `yearmonth` | Year-month | `2024-07` |
| `week` | ISO week number | `30` |
| `weekday` | Day of week (Mon=1) | `2` |
| `quarter` | Quarter (1-4) | `3` |

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
