# Facts

Facts are key-value metadata attached to [sources](source.md) or [objects](object.md).

## Types of Facts

**Source facts** (`source.*`) are about a source itself, read from what the scanner records. Every source has the same fixed set, and they are never imported or deleted:
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

When querying, the `content.` prefix is optional: `--where 'Make=Apple'` is equivalent to `--where 'content.Make=Apple'`. A name that is a [built-in key](../reference/builtins.md#built-in-keys), such as `size`, `filename` or `id`, reads as that key instead, so a content fact imported under such a name is reached only by its full name, `content.size`.

A source has one value for each key. A content fact can be stored on the source as well as on its content; filters, `canon facts`, patterns and `canon worklist` all read the fact stored on the content, and the one stored on the source only where the content has none. A fact stored on the content whose value Canon cannot read still counts as the content's: the source then has no value for the key, and the one stored on the source is not used instead.

### An empty value and no value

A stored empty text (`""`) is a value: `content.Make=""` selects it, and a text modifier keeps it (`content.Make|lowercase` of `""` is `""`). A value Canon works out has no value where it would be empty: the extension of a file with none (`README`, `.bashrc`, `trail.`), `|ext` of text with no dot, and a path segment `[n]` past the end. So `NOT source.ext?` selects the files with no extension, and `source.ext=""` selects nothing. A modifier that reads text as a number or a time has no value for `""`. A slice `[a:b]` always has a value, possibly a path with no segments: `source.rel_path[:-1]` of a file at the top of its root is one, and `source.rel_path[:-1]=""` selects such files.

## Value Types

Canon stores facts as:
- **Text**: Strings like `Apple` or `image/jpeg`; compared in character order, so `B` sorts before `a` and `10` before `9`; enables glob matching (`~`) and string modifiers (`|lowercase`, `|stem`)
- **Numbers**: Integers or decimals like `1024` or `3.14`; compared as numbers (`>1000`); enable the `|bucket` modifier
- **Times**: Points or spans of time on one timeline, like a capture date or a save time; compared as spans of time in your local time; enable the time modifiers (`|year`, `|month`). See [Times](#times) below

A key's type is determined from all of its facts, on every root, so it is the same whatever the scope. A value you type in a [filter](../reference/filter.md#values-and-types) is interpreted as the key's type: against a text key, `2019` is rejected until you say whether you mean the text `"2019"` or the year. A key whose facts hold more than one type is of mixed type, and lists as `mixed` in [`canon facts`](../commands/query/facts.md); a filter cannot compare its values until it is converted to one type.

Type hints can be provided during import to ensure correct parsing. See [Enriching](../commands/enrich/index.md) for details.

## Times

### How Canon shows a time

Canon shows times in your local time: the zone `TZ` names, or else the zone the machine is set to, the same zone `date` uses. A manifest works differently: it selects files and names their folders in the [time zone it records](../commands/archive/cluster.md#the-manifests-time-zone), whatever your time zone, so the same manifest does the same thing wherever it runs. `apply`'s check for files changed since the lock was written prints save times in that time zone too, and names it. A printed time has no offset, and prints at its precision:

| Precision | Prints |
|---|---|
| year | `2024` |
| month | `2024-06` |
| day | `2024-06-15` |
| minute | `2024-06-15 14:30` |
| second | `2024-06-15 14:30:05` |

Fractions of a second are kept, and print to the second, cut off rather than rounded: `14:30:05.95` prints `14:30:05`.

Output written for programs states its own form instead: [`worklist`](../commands/enrich/worklist.md) writes times as ISO 8601 text in the form they are stored, and the [book's inventory](../reference/book-format.md) writes UTC.

When Canon cannot determine a time zone at all, it uses UTC and says so once per run, on stderr:

```
this machine's time zone is unknown; times are shown and compared in UTC — set TZ to change it
```

When `TZ` is set to something Canon does not recognize, the line names it:

```
TZ=Europe/Amsterdm is not a time zone Canon can read; times are shown and compared in UTC
```

### How Canon interprets a time

A time is written in one of three forms, told apart by one thing only: whether the text has a UTC offset, as RFC 3339 writes it.

| Written as | Example | Stored as |
|---|---|---|
| A date and time with an offset | `2024-06-15T14:30:00+02:00`, `2024-06-15T12:30:00Z` | an instant, with its offset kept; `Z` and `+00:00` both mean UTC |
| A date and time with `-00:00` | `2024-06-15T12:30:00-00:00` | an instant whose local offset is unknown |
| A date and time with no offset | `2024-06-15 14:30:00`, `2024-06-15T14:30:00`, `2024:06:15 14:30:00` | a time with no offset |
| A date, month or year alone | `2024-06-15`, `2024:06:15`, `2024-06`, `2024` | a time with no offset |

The spellings in full:

- A date is `YYYY-MM-DD`, or `YYYY:MM:DD` as EXIF writes it; a month is `YYYY-MM`; a year is `YYYY`.
- A time of day follows the date after `T` or one space, as `HH:MM` or `HH:MM:SS`, with up to nine digits of a fraction of a second (`14:30:05.45`).
- An offset follows the time directly or after one space, as `Z` or `±HH:MM`. Lower-case `t` and `z` mean the same as upper case.

Anything else is not a time: an offset on a date alone (`2024-06-15+02:00`), a date or time that does not exist (`2024-02-30`), an offset written without its colon (`+0200`), trailing text, and an instant at the edge of the years 0000–9999 or beyond. [`import-facts`](../commands/enrich/import-facts.md) skips such a value and says why; it never cuts text short to make it fit.

How each form is shown in your local time:

- An instant, with or without a recorded offset, is translated to your local time.
- A time with no offset is used as written, in your time zone.
- A recorded offset is kept, and is not used when the value is shown or compared.

For someone whose time zone is `Europe/Amsterdam`:

| Written as | Prints |
|---|---|
| `2023-12-31T21:00:00-05:00` | `2024-01-01 03:00:00` |
| `2023-12-31T23:30:00Z` | `2024-01-01 00:30:00` |
| `2023-12-31T23:30:00-00:00` | `2024-01-01 00:30:00` |
| `2023:12:31 21:00:00` | `2023-12-31 21:00:00` |
| `2024` | `2024` |
| `2024-06-15T14:30:05.45+02:00` | `2024-06-15 14:30:05` |

Under one key, [`canon facts`](../commands/query/facts.md) shows values that print the same as one row. In the hour that repeats when clocks go back, two instants an hour apart print the same and are one row. A time with no offset written in the hour that is skipped when clocks go forward prints as written.

### Precision

Every time keeps the precision it was written at: a year, a month, a day, a minute, a second, or a fraction of a second. A time is never padded to a precision nobody wrote: `2024` is the year 2024, not its first second. A [modifier](../reference/facts.md#time-modifiers) that asks for a finer part than a time carries gives no value for it: `|month` on a time known only to `2024` has none.

### Save times

`source.mtime` is a file's save time as the file system reports it: an instant, to the second, with no recorded offset.

A save time at the edge of the years 0000–9999 or beyond has no value. The edge is within about a day of either end, because Canon accepts a time only where every time zone shows it inside those years. For such a file, `source.mtime?` is false and `NOT source.mtime?` finds it; [`canon facts --key source.mtime`](../commands/query/facts.md) counts it under `(no value)`; [`ls -l`](../commands/query/ls.md) prints `???` for its date; and `apply` stops for a pattern that uses it (see [Missing Values](../reference/expr.md#missing-values)).
