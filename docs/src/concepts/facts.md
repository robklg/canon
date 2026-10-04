# Facts

Facts are key-value metadata attached to [sources](source.md) or [objects](object.md).

## Types of Facts

**Built-in facts** are collected automatically during scanning:
- `source.ext` - File extension
- `source.size` - File size in bytes
- `source.mtime` - The file's save time
- `content.hash.sha256` - Content hash (when computed)

**Imported facts** come from external tools via the [enrichment pipeline](../commands/enrich/index.md):
- EXIF metadata: `content.Make`, `content.Model`, `content.DateTimeOriginal`
- Geolocation: `content.geo.city`, `content.geo.country`
- Media info: `content.mime`, `content.duration`
- Any custom key-value pairs you choose to import

## Namespaces

Facts are namespaced:

- `source.*` - Facts about the file on disk (path, size, timestamps)
- `content.*` - Facts about the content itself (stored on objects when hashed)

When querying, the `content.` prefix is optional: `--where 'Make=Apple'` is equivalent to `--where 'content.Make=Apple'`.

## Value Types

Canon stores facts as:
- **Text**: Strings like `"Apple"` or `"image/jpeg"`; enables string matching (`=`, `~` glob) and string modifiers (`|lowercase`, `|stem`)
- **Numbers**: Integers or decimals like `1024` or `3.14`; enables numeric comparisons (`>1000`) and the `|bucket` modifier
- **Times**: Points or periods on one timeline, like a capture date or a save time; enables the time modifiers (`|year`, `|month`). See [Times](#times) below

Type hints can be provided during import to ensure correct parsing. See [Enriching](../commands/enrich/index.md) for details.

## Times

### How Canon shows a time

Canon reads the times it shows you on your clock: the zone `TZ` names, or else the zone the machine is set to, the same clock `date` reads. Two places read differently. A manifest selects and places files by UTC, whatever your clock, so the same manifest does the same thing wherever it runs. And `apply`'s check for files changed since the lock was written prints a save time as seconds since 1970. A printed time carries no offset, and prints at its precision:

| Precision | Prints |
|---|---|
| year | `2024` |
| month | `2024-06` |
| day | `2024-06-15` |
| minute | `2024-06-15 14:30` |
| second | `2024-06-15 14:30:05` |

Fractions of a second are kept, and print to the second, cut off rather than rounded: `14:30:05.95` prints `14:30:05`.

Output written for programs states its own form instead: [`worklist`](../commands/enrich/worklist.md) writes times as ISO 8601 text in the form they are stored, and the [book's inventory](../reference/book-format.md) writes UTC.

When Canon cannot read a zone at all, it reads times in UTC and says so once per run, on stderr:

```
this machine's time zone is unknown; times are read in UTC — set TZ to change it
```

When `TZ` is set to something Canon cannot read, the line names it:

```
TZ=Europe/Amsterdm is not a time zone Canon can read; times are read in UTC
```

### How Canon reads a time

A time is written in one of three forms, told apart by one thing only: whether the text carries a UTC offset, as RFC 3339 writes it.

| Written as | Example | Stored as |
|---|---|---|
| A date and time with an offset | `2024-06-15T14:30:00+02:00`, `2024-06-15T12:30:00Z` | an instant, with its offset kept; `Z` and `+00:00` both mean UTC |
| A date and time with `-00:00` | `2024-06-15T12:30:00-00:00` | an instant whose local offset is unknown |
| A date and time with no offset | `2024-06-15 14:30:00`, `2024-06-15T14:30:00`, `2024:06:15 14:30:00` | a clock reading |
| A date, month or year alone | `2024-06-15`, `2024:06:15`, `2024-06`, `2024` | a clock reading |

The spellings in full:

- A date is `YYYY-MM-DD`, or `YYYY:MM:DD` as EXIF writes it; a month is `YYYY-MM`; a year is `YYYY`.
- A time of day follows the date after `T` or one space, as `HH:MM` or `HH:MM:SS`, with up to nine digits of a fraction of a second (`14:30:05.45`).
- An offset follows the time directly or after one space, as `Z` or `±HH:MM`. Lower-case `t` and `z` read the same as upper case.

Anything else is not read as a time: an offset on a date alone (`2024-06-15+02:00`), a date or time that does not exist (`2024-02-30`), an offset written without its colon (`+0200`), trailing text, and an instant at the edge of the years 0000–9999 or beyond. [`import-facts`](../commands/enrich/import-facts.md) skips such a value and says why; it never cuts text short to make it fit.

How each form is read on your clock:

- An instant, with or without a recorded offset, is read as your clock showed it at that instant.
- A clock reading is read as written: it is taken to be your clock.
- A recorded offset is kept, and never used to read the value.

For someone whose clock is `Europe/Amsterdam`:

| Written as | Prints |
|---|---|
| `2023-12-31T21:00:00-05:00` | `2024-01-01 03:00:00` |
| `2023-12-31T23:30:00Z` | `2024-01-01 00:30:00` |
| `2023-12-31T23:30:00-00:00` | `2024-01-01 00:30:00` |
| `2023:12:31 21:00:00` | `2023-12-31 21:00:00` |
| `2024` | `2024` |
| `2024-06-15T14:30:05.45+02:00` | `2024-06-15 14:30:05` |

Under one key, [`canon facts`](../commands/query/facts.md) shows values that print the same as one row. In the hour that repeats when clocks go back, two instants an hour apart print the same and are one row. A clock reading written in the hour that is skipped when clocks go forward prints as written.

### Precision

Every time keeps the precision it was written at: a year, a month, a day, a minute, a second, or a fraction of a second. A time is never padded to a precision nobody wrote: `2024` is the year 2024, not its first second. A [modifier](../reference/facts.md#time-modifiers) that reads a finer part than a time carries gives no value for it: `|month` on a time known only to `2024` has none.

### Save times

`source.mtime` is a file's save time as the file system reports it: an instant, to the second, with no recorded offset.

A save time at the edge of the years 0000–9999 or beyond has no value. The edge lies within about a day of either end, because Canon reads a time only where every zone shows it inside those years. For such a file, `source.mtime?` is false and `NOT source.mtime?` finds it; [`canon facts --key source.mtime`](../commands/query/facts.md) counts it under `(no value)`; [`ls -l`](../commands/query/ls.md) prints `???` for its date; and a pattern reading it refuses (see [Missing Values](../reference/expr.md#missing-values)).
