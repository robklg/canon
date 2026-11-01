# Facts Reference

Facts are key-value metadata. See the [overview](../concepts/facts.md) and [key names](../concepts/facts.md#reading-a-keys-name).

## Namespaces

| Namespace | Description |
|-----------|-------------|
| `source.*` | Source facts: taken from what the scanner records (path, size, mtime) |
| `content.*` | Content facts: imported (EXIF, mime type), and the content hash |

## Values

The [value types](../concepts/facts.md#value-types). Only the path source facts have the type path:

| Type | Examples |
|------|----------|
| Text | `Apple`, `image/jpeg`, `"2019"` |
| Number | `1024`, `3.14`, `-5`, `1e6` |
| Time | `2024-07-23 14:30:45`, `2024-07`, `2024` |
| Path | `source.rel_path` |

## Modifiers

Transform values using `|` syntax:

### Time Modifiers

Time modifiers work on time values (like `source.mtime` or EXIF dates) and on text written as a time. Each works in [your local time](../concepts/facts.md#how-canon-shows-a-time). The examples use the time `2024-07-23 14:30:45`:

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

A time coarser than the last column has no value for that modifier ([Precision](../concepts/facts.md#precision)).

### String Modifiers

These work on text and paths, and on the printed form of another modifier's result ([Chaining](expr.md#chaining)).

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
| `bucket` | A number from 0 up | A range label: `0`, then powers of ten, `1-to-10`, `10-to-100`, `1K-to-10K`, `0.00001-to-0.0001`. A number below zero has no value |
| `bucket(a,b,c)` | Any number, negative ones included | A range label: `under-a`, `a-to-b`, `b-to-c`, `c-and-up`, a number below zero written with `minus-` (`\|bucket(-90,0,90)` gives `under-minus-90`, `minus-90-to-0`, `0-to-90`, `90-and-up`) |

A range includes its lower edge and not its upper: `1K-to-10K` holds 1,000 up to 9,999, since `K` is 1,000, `M` 1,000,000 and `G` 1,000,000,000. A label is compared with `=`, `!=` and `IN`, and typed the way Canon shows it (`source.size|bucket(1e6)=1000000-and-up`).

## Path Accessors

Python-style indexing for path values, and for text, which an accessor splits into path segments (`content.SourceFile[-1]`):

| Syntax | Meaning |
|--------|---------|
| `key[-1]` | Last segment (filename) |
| `key[0]` | First segment |
| `key[1:3]` | Slice segments 1 and 2 |
| `key[:-1]` | All but last segment |

An index past the end has no value; a slice past the end is a path with no segments.

```
source.rel_path[-1]        → IMG_001.jpg
source.rel_path[-1]|stem   → IMG_001
source.rel_path[0]         → photos
```
