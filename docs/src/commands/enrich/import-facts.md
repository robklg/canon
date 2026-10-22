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

Facts are automatically namespaced under `content.*`: `mime` becomes `content.mime`. See [Reading a Key's Name](../../concepts/facts.md#reading-a-keys-name). A key starting with `source.` is skipped with a warning and counted as `skipped (reserved)`.

The special key `hash.sha256` creates or links an object, enabling deduplication and archive tracking.

## Type Hints

Canon stores facts as text, numbers, or times; the stored type determines which operations work on a fact (see [Value Types](../../concepts/facts.md#value-types) for the mapping).

If a date like `"2024:07:23 11:06:32"` is stored as text instead of a time, it compares as text: `--where 'DateTimeOriginal=2024'` gives an error against a text key, and only a time modifier treats it as a time (`--where 'DateTimeOriginal|year=2024'`).

### Providing Type Hints

Wrap values in an object with `value` and `type`:

```json
{"source_id":123,"basis_rev":0,"facts":{
  "DateTimeOriginal": {"value": "2024:07:23 11:06:32", "type": "datetime"},
  "duration": {"value": "1:23:45", "type": "duration"},
  "rating": 5
}}
```

| Type | Parses | Stored As |
|------|--------|-----------|
| `datetime` | A string in a form Canon accepts as a time; a whole year from 1900 to 2100 as a JSON number | A time, in the form and at the precision it was written |
| `duration` | `"1:23:45"`, `"5:30"`, or seconds as number | Seconds (number) |
| *(none)* | Strings as text, numbers and `true`/`false` as numbers | As-is; `true` as `1` and `false` as `0`, so a filter asks `Flag=1` |

### Common Pitfalls

**Dates as strings:** EXIF dates from tools like `exiftool` come as strings (`"2024:07:23 11:06:32"`). Without a type hint, they're stored as text: a filter compares them as text, and only a time modifier treats them as times. Always use `"type": "datetime"` for date fields.

**Mixed types:** A fact key must have a consistent type across all sources. You cannot store `DateTimeOriginal` as text for some files and as a time for others. If you initially imported facts with the wrong type and need to re-import with the correct type, first delete the existing entries:

```bash
# Delete the DateTimeOriginal facts that were stored as text: on content, then on unhashed sources
canon facts delete --on object content.DateTimeOriginal --value-type text
canon facts delete --on source content.DateTimeOriginal --value-type text
```

Each previews by default; add `--yes` to delete. Then re-run your processor with proper type hints.

## Times

### How a time is interpreted

A value hinted `"type": "datetime"` is interpreted as follows; [How Canon interprets a time](../../concepts/facts.md#how-canon-interprets-a-time) lists every form.

- **A string** is interpreted by its UTC offset, as RFC 3339 writes it. An offset is kept as given; `Z` and `+00:00` are UTC; `-00:00` is a UTC instant whose local offset is unknown; a time without an offset is assumed to be in your time zone. A date, month or year written alone is a time with no offset, at that precision.
- **Precision** is what was written: a year, a month, a day, a minute or a second, with fractions of a second kept.
- **A JSON number** is accepted only as a whole year from 1900 to 2100 (`2019`, or `2019.0`), a time with no offset, at year precision. A number never means seconds since 1970.

One key may hold every form at once: instants with various offsets, instants whose local offset is unknown, and times with no offset at any precision are all times.

A time that cannot be read is skipped, counted under `skipped (type mismatch)`, and named:

```
Warning: 1704067200 is not a time for 'content.DateTimeOriginal' in /photos/IMG_0042.jpg: a number is a time only when it is a whole year from 1900 to 2100
Warning: "2024-06-15+02:00" is not a time for 'content.DateTimeOriginal' in /photos/IMG_0043.jpg: an offset needs a time of day to belong to
```

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

## Archive Sources

By default, importing facts for sources in archive roots is skipped. Use `--allow archived` to enable this (useful for backfilling metadata on already-archived files).

## Suspended Roots

A record naming a source on a [suspended root](../roots/roots.md) is set aside and counted in the summary:

```
Processed 240 lines: 238 facts imported, 0 skipped (stale), 0 skipped (reserved), 0 skipped (archived), 2 skipped (on suspended roots), 0 skipped (type mismatch), 0 objects created, 0 facts promoted
```

Nothing is written for those records, and the import is not stopped: one suspended root never breaks a batch. Unsuspend the root and re-run the import to fill them in.
