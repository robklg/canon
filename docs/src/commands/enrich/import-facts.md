# canon import-facts

Import facts from JSONL on stdin. Designed to receive output from a processor that consumed a worklist.

```bash
canon worklist | some-processor | canon import-facts

# Allow importing facts for sources in archive roots
canon worklist --include-archived | some-processor | canon import-facts --allow-archived
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

Facts are automatically namespaced under `content.*`. For example, `mime` becomes `content.mime`.

The special key `hash.sha256` creates or links an object, enabling deduplication and archive tracking.

## Type Hints

Facts can include type hints to ensure correct storage and enable modifiers:

```json
{"source_id":123,"basis_rev":0,"facts":{
  "capture_datetime": {"value": "2024:07:23 11:06:32", "type": "datetime"},
  "duration": {"value": 125.5, "type": "duration"}
}}
```

| Type | Description |
|------|-------------|
| `datetime` | Parses date strings (ISO, EXIF format) or plain years (2005) as Unix timestamps |
| `duration` | Parses duration strings ("1:23:45", "5:30") or numbers as seconds |

Without type hints, values are stored as-is (strings as text, numbers as numbers). Type hints enable time modifiers (`|year`, `|month`) to work correctly on datetime facts.

## Archive Sources

By default, importing facts for sources in archive roots is skipped. Use `--allow-archived` to enable this (useful for backfilling metadata on already-archived files).
