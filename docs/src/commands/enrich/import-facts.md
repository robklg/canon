# canon import-facts

Facts from a processor, as JSONL on stdin.

```bash
canon worklist | some-processor | canon import-facts

# Allow importing facts for sources in archive roots
canon worklist --include archived | some-processor | canon import-facts --allow archived
```

## Input Format

Each line is a JSON object:

```json
{"source_id":123,"basis_rev":0,"facts":{"hash.sha256":"abc123...","mime":"image/jpeg"}}
```

| Field | Description |
|-------|-------------|
| `source_id` | Source ID from the worklist (required) |
| `basis_rev` | Revision from the worklist for staleness check (required) |
| `facts` | Object mapping fact keys to values |

A record is skipped if the file changed since the worklist was generated.

## Fact Namespacing

A name is stored under `content.`: `mime` becomes `content.mime` ([Reading a Key's Name](../../concepts/facts.md#reading-a-keys-name)).

`hash.sha256` creates or links an object.

## Types

Facts are text, numbers, or times ([Value Types](../../concepts/facts.md#value-types)).

A key has one type. Its first import sets it, and a later value that does not fit is skipped and counted. Only `canon facts convert` changes a key's type.

### The Key Decides

| A value written as | Key holds text | Key holds numbers | Key holds times | New key |
|---|---|---|---|---|
| A string | Text, as written | A number, when the text is a valid number and loses nothing; otherwise skipped | A time, when the text is a valid time; otherwise skipped | Text |
| A JSON number | Text, in the spelling written: `1.10` stays `1.10` | A number | Skipped | A number |
| `true` or `false` | Text: `true`, `false` | `1` or `0`, so a filter asks `Flag=1` | Skipped | `1` or `0` |
| A list or an object | Text, as compact JSON | Skipped | Skipped | Text, as compact JSON |
| `null` | Nothing stored | Nothing stored | Nothing stored | Nothing stored |

Text that would lose a leading or trailing zero as a number, such as `01234` or `2.50`, is skipped.

**Dates as strings**: a new key whose first value is a date string with no hint holds text. A time modifier still treats it as a time (`--where 'DateTimeOriginal|year=2024'`).

### Type Hints

A value wrapped in an object with `value` and `type`:

```json
{"source_id":123,"basis_rev":0,"facts":{
  "DateTimeOriginal": {"value": "2024:07:23 11:06:32", "type": "datetime"},
  "duration": {"value": "1:23:45", "type": "duration"},
  "width": {"value": "1920", "type": "number"},
  "rating": 5
}}
```

| Type | Reads | Stored As |
|------|--------|-----------|
| `datetime` | A string in a form Canon accepts as a [time](../../concepts/facts.md#how-canon-interprets-a-time); a whole year from 1900 to 2100 as a JSON number | A time, in the form and at the precision it was written |
| `duration` | `"1:23:45"`, `"5:30"`, or seconds as a number | Seconds (a number) |
| `number` | A JSON number, or a string written as one (`"1920"`) | A number for a number key or a new key; text in the spelling written for a text key |

A hinted value that does not fit its key is skipped.

### What Is Skipped

Every skipped value is counted:

```
content.DateTimeOriginal: 2 values, such as "0000:00:00 00:00:00", do not read as a time (--verbose lists each); have the tool write a time Canon reads (see "How Canon interprets a time" in the facts documentation) or omit values that are no time (an empty EXIF date such as 0000:00:00 00:00:00 is best omitted), or `canon facts convert content.DateTimeOriginal` to see the other ways
content.DateTimeOriginal: 1 value, 1721811600, is a number, and the key holds times; have the tool write the time as text (a year as "2019"), or, for a year, write the number with the datetime hint
Read 4 records: imported 4 facts; skipped 3 values: 2 that do not read as their key's type, 1 that is a number for a time key; 4 null values (nothing stored).
```

`--verbose` lists each value:

```
[/photos] IMG_0042.jpg
  content.DateTimeOriginal: "0000:00:00 00:00:00" skipped (does not read as a time)
  content.ImageWidth: 4032 (on object)
```

A skipped value leaves its file selected by a `NOT <key>?` filter; see [Resuming a Pass](processors.md#resuming-a-pass).

## Replacing a Key's Values

Import replaces a key's existing value, so no delete is needed first. To re-read every file with a key, except on suspended roots:

```bash
canon worklist --global --include all --where 'content.Make?' \
  | some-processor | canon import-facts --allow archived
```

`--include all` brings archived and excluded sources into the worklist, and `--allow archived` lets the import write facts for the archived ones.

## Batches

Records are written in batches, and facts already written stay if the import stops.

## Archive Sources

Facts for sources in archive roots are skipped without `--allow archived`.

## Suspended Roots

A record naming a source on a [suspended root](../../concepts/roots.md#suspended-roots) is set aside and counted:

```
Read 7 records: imported 5 facts; skipped 2 records: 2 on suspended roots.
```
