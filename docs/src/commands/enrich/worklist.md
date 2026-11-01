# canon worklist

Sources as JSONL, for external tools.

```bash
# Sources in current directory (when inside a root)
canon worklist

# All sources across all roots
canon worklist --global

# Only sources missing a content hash
canon worklist --where 'NOT content.hash.sha256?'

# Only JPG files
canon worklist --where 'source.ext=jpg'

# Scope to a specific directory
canon worklist /path/to/photos

# Include sources from archive roots (for backfilling facts); also takes excluded, or all for both
canon worklist --include archived

# Include keys' values in output (for chained enrichment)
canon worklist --emit content.geo.lat --emit content.geo.lon
```

A `--where` filter for repeated passes: [Resuming a Pass](processors.md#resuming-a-pass).

## Output Format

```json
{"source_id":123,"path":"/full/path/to/file.jpg","root_id":1,"size":1024,"mtime":1703980800,"basis_rev":0}
```

| Field | Description |
|-------|-------------|
| `source_id` | Database ID (pass through to import-facts) |
| `path` | Full absolute path to the file |
| `root_id` | ID of the root containing this source |
| `size` | File size in bytes |
| `mtime` | The file's save time, in seconds since 1970 (UTC) |
| `basis_rev` | Revision counter for staleness detection (pass through to import-facts) |

## Emitting Existing Facts

`--emit` adds each source's value for a key (`null` if it has none):

```bash
canon worklist --emit content.geo.lat --emit content.geo.lon
```

```json
{"source_id":123,"path":"/...","basis_rev":0,"facts":{"content.geo.lat":52.37,"content.geo.lon":4.89}}
{"source_id":124,"path":"/...","basis_rev":0,"facts":{"content.geo.lat":null,"content.geo.lon":null}}
```

A time is written as ISO 8601 text, in the form and at the precision it is stored:

```bash
canon worklist --emit content.DateTimeOriginal
```

```json
{"source_id":125,"path":"/photos/IMG_0125.jpg","root_id":1,"size":2048,"mtime":1718454605,"basis_rev":0,"facts":{"content.DateTimeOriginal":"2023-12-31T21:00:00-05:00"}}
{"source_id":126,"path":"/photos/IMG_0126.mov","root_id":1,"size":2048,"mtime":1718454605,"basis_rev":0,"facts":{"content.DateTimeOriginal":"2023-12-31T20:00:00-00:00"}}
{"source_id":127,"path":"/photos/IMG_0127.jpg","root_id":1,"size":2048,"mtime":1718454605,"basis_rev":0,"facts":{"content.DateTimeOriginal":"2023-12-31T21:00:00"}}
{"source_id":128,"path":"/photos/IMG_0128.jpg","root_id":1,"size":2048,"mtime":1718454605,"basis_rev":0,"facts":{"content.DateTimeOriginal":"2024-06-15T14:30:05.45+02:00"}}
{"source_id":129,"path":"/photos/IMG_0129.jpg","root_id":1,"size":2048,"mtime":1718454605,"basis_rev":0,"facts":{"content.DateTimeOriginal":"2024"}}
```

Key names in `--emit` follow [the same rules as in `--where`](../../concepts/facts.md#reading-a-keys-name), except that a name with a space needs no quotes. `--emit geo.lat` emits `content.geo.lat`.

```bash
canon worklist --emit Make --emit source.mtime --emit source.ext
```

```json
{"source_id":1,"path":"/photos/README","root_id":1,"size":5,"mtime":1792271246,"basis_rev":0,"facts":{"Make":null,"source.mtime":"2026-10-17T21:07:26-00:00","source.ext":null}}
{"source_id":2,"path":"/photos/trip/IMG_0125.jpg","root_id":1,"size":3,"mtime":1792271246,"basis_rev":0,"facts":{"Make":"Canon","source.mtime":"2026-10-17T21:07:26-00:00","source.ext":"jpg"}}
```

Reverse geocoding files that have coordinates but no city name:

```bash
canon worklist --emit content.geo.lat --emit content.geo.lon --where 'geo.lat? AND NOT geo.city?' \
  | ./scripts/reverse-geocode.sh \
  | canon import-facts
```
