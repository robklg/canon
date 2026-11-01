# Facts

Facts are key-value metadata attached to [sources or objects](source-object.md).

## Types of Facts

**Source facts** (`source.*`) are about a source itself, recorded by the scanner. Every source has the same fixed set, and they are never imported or deleted:
- `source.ext` - File extension
- `source.size` - File size in bytes
- `source.mtime` - The file's save time
- `source.path`, `source.rel_path` - Where the file is

**Content facts** (`content.*`) are about a source's content. External tools import them via the [enrichment pipeline](../commands/enrich/index.md), and Canon records the content hash when it hashes a file:
- Content hash: `content.hash.sha256`
- EXIF metadata: `content.Make`, `content.Model`, `content.DateTimeOriginal`
- Geolocation: `content.geo.city`, `content.geo.country`
- Media info: `content.mime`, `content.duration`
- Any custom key-value pairs you choose to import

**Built-in keys** (`filename`, `size`, `hash`, ...) are short names for values used often, neither source facts nor content facts. Some have the same value as a source fact: `size` has the same value as `source.size`. See [Source Facts and Built-in Keys](../reference/builtins.md) for the full lists.

## Reading a Key's Name

A plain name means a content fact, in a filter, a report key and a pattern alike:

- **A plain name**: the content fact. `--where 'Make=Apple'` selects the same sources as `--where 'content.Make=Apple'`, and `{Make}` gives `Apple`.
- **The name of a [built-in key](../reference/builtins.md#built-in-keys)**: that key, such as `filename` or `id`. `{size}` gives the file's size, `3`.
- **A full name**: that fact. `{content.size}` gives `big`, for a content fact imported under the name `size`.

A source has one value for each key. A content fact can be stored on the source as well as on its content; filters, `canon facts`, patterns and `canon worklist` all use the fact stored on the content, and the one stored on the source only where the content has none.

## Value Types

A fact has one of four types:
- **Text**: Strings like `Apple` or `image/jpeg`
- **Numbers**: Integers or decimals like `1024` or `3.14`
- **Times**: Points or spans of time on one timeline, like a capture date or a save time. See [Times](#times) below
- **Paths**: `source.path` and `source.rel_path`. [Path accessors](../reference/facts.md#path-accessors) select parts of a path

A key's type is determined from all of its facts, on every root, so it is the same whatever the scope. See [Values and Types](../reference/filter.md#values-and-types) for how a value you type is interpreted as the key's type, and for keys of mixed type.

Import can include type hints. See [Enriching](../commands/enrich/index.md).

## Times

### How Canon shows a time

Canon shows times in your local time: the zone `TZ` names, or else the zone the machine is set to. A manifest uses the [time zone it records](../commands/archive/cluster.md#the-manifests-time-zone) instead. Times show with no offset, at their precision:

| Precision | Shows |
|---|---|
| year | `2024` |
| month | `2024-06` |
| day | `2024-06-15` |
| minute | `2024-06-15 14:30` |
| second | `2024-06-15 14:30:05` |

### How Canon interprets a time

| Written as | Example | Stored as |
|---|---|---|
| A date and time with an offset | `2024-06-15T14:30:00+02:00`, `2024-06-15T12:30:00Z` | an instant, with its offset kept; `Z` and `+00:00` both mean UTC |
| A date and time with no offset | `2024-06-15 14:30:00`, `2024-06-15T14:30:00`, `2024:06:15 14:30:00` | a time with no offset |
| A date, month or year alone | `2024-06-15`, `2024:06:15`, `2024-06`, `2024` | a time with no offset |

See [Times](../reference/filter.md#times) for the spellings in full.

A recorded offset is kept, and is not used when the value is shown or compared. For someone whose time zone is `Europe/Amsterdam`:

| Written as | Shows |
|---|---|
| `2023-12-31T21:00:00-05:00` | `2024-01-01 03:00:00` |
| `2023-12-31T23:30:00Z` | `2024-01-01 00:30:00` |
| `2023:12:31 21:00:00` | `2023-12-31 21:00:00` |
| `2024` | `2024` |
| `2024-06-15T14:30:05.45+02:00` | `2024-06-15 14:30:05` |

### Precision

A time keeps the precision it was written at, and is never padded: `2024` is the year 2024, not its first second.

### Save times

`source.mtime` is a file's save time as the file system reports it: an instant, to the second, with no recorded offset.
