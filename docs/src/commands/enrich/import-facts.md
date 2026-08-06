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

Facts are automatically namespaced under `content.*`: `mime` becomes `content.mime`. See [Namespaces](../../concepts/facts.md#namespaces).

The special key `hash.sha256` creates or links an object, enabling deduplication and archive tracking.

## Type Hints

Canon stores facts as text, numbers, or timestamps (see [Value Types](../../concepts/facts.md#value-types)). The stored type determines what operations work on a fact:

- **Timestamps** enable date modifiers (`|year`, `|month`, `|date`) and date comparisons (`>=2024-01-01`)
- **Numbers** enable numeric comparisons (`>1000`, `<=5.0`) and the `|bucket` modifier
- **Text** enables string matching (`=`, `~` glob) and string modifiers (`|lowercase`, `|stem`)

If a datetime like `"2024:07:23 11:06:32"` is stored as text instead of a timestamp, queries like `--where 'DateTimeOriginal|year=2024'` won't work: the modifier expects a timestamp.

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
| `datetime` | ISO dates, EXIF format, plain years (`2024`) | Unix timestamp |
| `duration` | `"1:23:45"`, `"5:30"`, or seconds as number | Seconds (number) |
| *(none)* | Strings as text, numbers as numbers | As-is |

### Common Pitfalls

**Dates as strings:** EXIF dates from tools like `exiftool` come as strings (`"2024:07:23 11:06:32"`). Without a type hint, they're stored as text and time modifiers won't work. Always use `"type": "datetime"` for date fields.

**Mixed types:** A fact key must have a consistent type across all sources. You cannot store `DateTimeOriginal` as text for some files and as a timestamp for others. If you initially imported facts with the wrong type and need to re-import with the correct type, first delete the existing entries:

```bash
# Delete all DateTimeOriginal facts that were stored as text
canon facts delete --key content.DateTimeOriginal --type text
```

Then re-run your processor with proper type hints.

## Archive Sources

By default, importing facts for sources in archive roots is skipped. Use `--allow archived` to enable this (useful for backfilling metadata on already-archived files).
