# canon import-facts

Import facts from JSONL on stdin. Receives output from a processor that consumed a worklist.

```bash
canon worklist | some-processor | canon import-facts

# Allow importing facts for sources in archive roots
canon worklist --include archived | some-processor | canon import-facts --allow archived
```

## Input Format

Each line must be a JSON object with `source_id`, `basis_rev`, and `facts`:

```json
{"source_id":123,"basis_rev":0,"facts":{"hash.sha256":"abc123...","mime":"image/jpeg"}}
```

| Field | Description |
|-------|-------------|
| `source_id` | Source ID from the worklist (required) |
| `basis_rev` | Revision from the worklist for staleness check (required) |
| `facts` | Object mapping fact keys to values |

The processor must pass through `source_id` and `basis_rev` from the worklist entry. If `basis_rev` doesn't match the source's current value, the import is skipped (the file changed since the worklist was generated).

## Fact Namespacing

Facts are automatically namespaced under `content.*`: `mime` becomes `content.mime`. See [Reading a Key's Name](../../concepts/facts.md#reading-a-keys-name). A name starting with `source.` is not imported: a source fact Canon records itself, such as `source.mtime` passed back from [`worklist --emit`](worklist.md#emitting-existing-facts), is counted in the summary; any other such name is skipped, counted and named.

The special key `hash.sha256` creates or links an object, enabling deduplication and archive tracking.

## Types

Canon stores facts as text, numbers, or times; the stored type determines which operations work on a fact (see [Value Types](../../concepts/facts.md#value-types) for the mapping).

A key has one type. Its first import sets it: the type of the first value stored for the key, earlier values in the same run included. Every later value is read as the key's type, on every root, suspended roots included; a value that cannot be is skipped and counted, never stored as another type. Only `canon facts convert` changes a key's type, so a key that took the wrong type from its first value is converted, not deleted and imported again.

### The Key Decides

| A value written as | Key holds text | Key holds numbers | Key holds times | New key |
|---|---|---|---|---|
| A string | Text, as written | A number, when the text reads as one and loses nothing; otherwise skipped | A time, when the text reads as one; otherwise skipped | Text |
| A JSON number | Text, in the spelling written: `1.10` stays `1.10` | A number | Skipped | A number |
| `true` or `false` | Text: `true`, `false` | `1` or `0`, so a filter asks `Flag=1` | Skipped | `1` or `0` |
| A list or an object | Text, as compact JSON | Skipped | Skipped | Text, as compact JSON |
| `null` | Nothing stored | Nothing stored | Nothing stored | Nothing stored |

A key of mixed type takes no value until it is converted to one type.

Two names in one record that read to one key, such as `Model` and `content.Model`, give it neither value: both are skipped and counted, and the key keeps the value it had. A name whose value is `null` gives no value, so the other is decided as if it were alone. Have the tool write each key under one name; a key passed back by [`worklist --emit`](worklist.md#emitting-existing-facts), under the name it was emitted as.

**Text read as a number** loses nothing when its number is the same text in another spelling, such as `1e5` or `-0`. Text that would lose what it carried is skipped: a leading zero in the whole part (`01234`, a code rather than a quantity), a trailing zero in the fraction (`2.50`), or a number Canon cannot hold exactly as written. Converting the key to text keeps such values as written.

**A number is held to about 15 to 17 significant digits.** A JSON integer Canon cannot hold exactly, such as `9007199254740993`, is skipped and counted, with or without the `number` hint. A fraction with more digits is stored as the nearest number Canon holds. An integer written with `.0` or an exponent, such as `9007199254740993.0` or `9007199254740993e0`, counts as a fraction: it is stored rounded, and not counted.

**A time key reads the text `"2019"` as a year** and skips the JSON number `2019`: a number is a time only with the `datetime` hint.

**Dates as strings.** A key that holds times reads a date string such as `"2024:07:23 11:06:32"` as a time, with no hint. A new key whose first value is a date string with no hint holds text: a filter compares it as text, a time modifier reads it as a time without changing it (`--where 'DateTimeOriginal|year=2024'`), and `canon facts convert` changes the key to times.

### Type Hints

Wrap a value in an object with `value` and `type` to say what it is:

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
| `datetime` | A string in a form Canon accepts as a time; a whole year from 1900 to 2100 as a JSON number | A time, in the form and at the precision it was written |
| `duration` | `"1:23:45"`, `"5:30"`, or seconds as a number | Seconds (a number) |
| `number` | A JSON number, or a string written as one (`"1920"`) | As the same JSON number with no hint: a number for a number key or a new key, text in the spelling written for a text key |

A `number` hint is the tool's statement that the value is a number, so its spelling is not judged for loss: `{"value": "01234", "type": "number"}` is stored as `1234`. A string that is not written as a number, such as `twelve`, `inf` or `+5`, is skipped.

A hinted value must fit its key: a `datetime` value for a key that holds text or numbers, and a `duration` value for a key that holds text or times, are skipped. A hint other than these three is skipped too.

### What Is Skipped

A skipped value is counted under its cause in the summary. Before the summary, one line per key and cause gives how many values were skipped, one of them as the tool wrote it, and the way out:

```
content.DateTimeOriginal: 2 values, such as "0000:00:00 00:00:00", do not read as a time (--verbose lists each); have the tool write a time Canon reads (see "How Canon interprets a time" in the facts documentation) or omit values that are no time (an empty EXIF date such as 0000:00:00 00:00:00 is best omitted), or `canon facts convert content.DateTimeOriginal` to see the other ways
content.DateTimeOriginal: 1 value, 1721811600, is a number, and the key holds times; have the tool write the time as text (a year as "2019"), or, for a year, write the number with the datetime hint
Read 4 records: imported 5 facts; skipped 3 values: 2 that do not read as their key's type, 1 that is a number for a time key; 4 null values (nothing stored).
```

A name whose value is `null` and a source fact passed back are counted outside the skipped values: neither is anything to fix, and `--verbose` gives them no line. `--verbose` lists every other value under its file, stored or skipped:

```
[/photos] IMG_0042.jpg
  content.DateTimeOriginal: "0000:00:00 00:00:00" skipped (does not read as a time)
  content.ImageWidth: 4032 (on object)
```

A skipped value is imported only when the pipeline runs again. A pass gated on `NOT <key>?` does not select a source that still holds an older value for the key, and a value skipped at every run, such as the empty EXIF date for a time key, leaves its file selected by such a gate every time. Gate a pass on a fact the tool writes for every file ([Resuming a Pass](processors.md#resuming-a-pass)).

## Times

### How a time is interpreted

A value hinted `"type": "datetime"` is interpreted as follows; [How Canon interprets a time](../../concepts/facts.md#how-canon-interprets-a-time) lists every form.

- **A string** is interpreted by its UTC offset, as RFC 3339 writes it. An offset is kept as given; `Z` and `+00:00` are UTC; `-00:00` is a UTC instant whose local offset is unknown; a time without an offset is assumed to be in your time zone. A date, month or year written alone is a time with no offset, at that precision.
- **Precision** is what was written: a year, a month, a day, a minute or a second, with fractions of a second kept.
- **A JSON number** is accepted only as a whole year from 1900 to 2100 (`2019`, or `2019.0`), a time with no offset, at year precision. A number never means seconds since 1970.

One key may hold every form at once: instants with various offsets, instants whose local offset is unknown, and times with no offset at any precision are all times.

A time that cannot be read is skipped and counted, as in [What Is Skipped](#what-is-skipped).

Every time is shown in [your local time](../../concepts/facts.md#how-canon-shows-a-time).

### Times imported by an earlier version of Canon

A time fact imported by an earlier version of Canon is a time with no offset, to the second. Two gaps stay until its key is re-imported:

- **A value imported with an offset** (`Z` and `-00:00` included) prints its time in UTC rather than in your local time. Canon cannot tell such a value from a true time with no offset, so it cannot find these values for you.
- **A key imported as bare dates, months or years** shows times to the second: `2024-01-01 00:00:00` where a re-imported value shows `2024`.

The shipped `scripts/exif-worklist.sh` writes four time keys: `content.media.capture_datetime` (from `SubSecDateTimeOriginal`, which usually has an offset, or from a video's QuickTime dates), `content.media.create_datetime`, `content.media.modify_datetime` (including `SubSecModifyDate` and `MediaModifyDate`), and `content.geo.datetime` (from `GPSDateTime`). QuickTime and GPS dates hold only a UTC instant, so the script writes them with `-00:00`; an earlier script wrote a video's dates with the offset of the computer it ran on, and GPS dates with `Z`. A library enriched with the script by an earlier version of Canon holds values imported with an offset under these keys, so the remedy below concerns them.

### Re-importing a key

The remedy is a re-import of the whole key. Import replaces a source's or a content's existing value for a key, so no delete is needed first: every value the enrichment tool can read again is overwritten with its true form.

Re-import the whole key, never part of it: a key partly re-imported holds the same instant in two forms, and can put it on two different days. The cost is a re-read of every file holding the key, the archive included, which may be large or slow.

1. **Re-read every file on an open root that holds the key.** For the shipped script:

   ```bash
   canon worklist --global --include all --where '
     content.media.capture_datetime? OR content.media.create_datetime?
     OR content.media.modify_datetime? OR content.geo.datetime?
   ' | ./scripts/exif-worklist.sh | canon import-facts --allow archived
   ```

   `--global` reaches every open root, wherever you run it from; `--include all` brings archived and excluded sources into the worklist, and `--allow archived` lets the import write facts for the archived ones; both are left out by default. The gate selects the files that hold the keys, unlike the usual `NOT <key>?` gate of an enrichment pass, which selects nothing for a key that already holds values. For a processor of your own, put it in place of the script and its keys in the gate.

2. **Reach what suspended roots alone hold.** A fact about content belongs to the content, so the pass above reaches every content that a present file on an open root holds, and the new value holds for its copies on suspended roots too. What is left is content held only on suspended roots, and files there that are not hashed. `canon roots list --suspended` lists the suspended roots; `canon ls` and `canon facts` set a suspended root aside, so they cannot tell you which of its files hold the key. For each suspended root you enriched with the processor, find the reason it was suspended with, open it, run the pass over it, and suspend it again with the same reason:

   ```bash
   canon trail /path/to/root
   canon roots unsuspend path:/path/to/root
   canon worklist /path/to/root --include all --where '
     content.media.capture_datetime? OR content.media.create_datetime?
     OR content.media.modify_datetime? OR content.geo.datetime?
   ' | ./scripts/exif-worklist.sh | canon import-facts --allow archived
   canon roots suspend path:/path/to/root --reason "<the reason trail shows>"
   ```

Values the tool cannot read again, such as a file on a drive that is not connected or a file no longer present, keep the value they hold, with its gap, until their file is read again. So do values no enrichment tool can regenerate, such as facts you wrote by hand.

## Batches

Records are written in batches: a batch is written when it holds 500 records, or about a second after its first record arrived, so the facts of a slow pipeline are written as it runs. While another Canon command is writing to the database, a batch waits for it, up to 30 seconds. A batch that cannot be written ends the import early with an error: the facts in batches already written stay, and, when Canon records decisions, the import's decision in `canon trail` says how far it got and why it stopped. A record that fails is undone whole, and the rest of its batch is written.

Stopping the import with Ctrl-C keeps every batch already written and prints no summary; when Canon records decisions, the import's decision in the trail keeps the status `started`.

## Archive Sources

By default, importing facts for sources in archive roots is skipped. Use `--allow archived` to enable this (useful for backfilling metadata on already-archived files).

## Suspended Roots

A record naming a source on a [suspended root](../roots/roots.md) is set aside and counted in the summary:

```
Read 7 records: imported 5 facts; skipped 2 records: 2 on suspended roots.
```

Nothing is written for those records, and the import is not stopped: one suspended root never breaks a batch. Unsuspend the root and re-run the import to fill them in.
