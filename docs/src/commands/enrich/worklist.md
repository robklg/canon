# canon worklist

Output sources as JSONL for processing by external tools.

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

# Include sources from archive roots (for backfilling facts)
canon worklist --include archived

# Include excluded sources
canon worklist --include excluded

# Include both
canon worklist --include all

# Include existing facts in output (for chained enrichment)
canon worklist --emit content.geo.lat --emit content.geo.lon
```

Choosing a `--where` gate that a repeated pass converges on is covered in [Writing Processors](processors.md#resuming-a-pass).

A path on a [suspended](../roots/roots.md#suspending-roots) root, or the current directory inside one, emits nothing on stdout: the statement goes to stderr and the exit code is 1, so a consumer never mistakes an empty stream for "nothing here".

## Output Format

Each line is a JSON object with source metadata:

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
| `basis_rev` | Revision counter for staleness detection |

## Emitting Existing Facts

With `--emit`, requested facts are included in the output (`null` if absent):

```bash
canon worklist --emit content.geo.lat --emit content.geo.lon
```

```json
{"source_id":123,"path":"/...","basis_rev":0,"facts":{"content.geo.lat":52.37,"content.geo.lon":4.89}}
{"source_id":124,"path":"/...","basis_rev":0,"facts":{"content.geo.lat":null,"content.geo.lon":null}}
```

A time is written as ISO 8601 text, in the form and at the precision it is stored: with its offset (a zero offset as `+00:00`), with `-00:00` for an instant whose local offset is unknown, or with no offset for a time that has none. A fraction of a second is written where one is stored:

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

The entry's own `mtime` field is a number of seconds, as in every entry.

**`--emit` takes the key as stored — write it in full.** Unlike `--where`, it does not add the
optional `content.` prefix for you: `--emit geo.lat` looks for a fact whose key is literally
`geo.lat` and emits `null`, even where `--where 'geo.lat?'` on the same command line matches.
Check how a key is stored with [`canon facts`](../query/facts.md). Built-in `source.*` keys are
never emittable — no fact is ever stored under that namespace ([`import-facts`](import-facts.md)
rejects it), and what they would say is already in the entry's own fields.

This enables processors to build on previous enrichment:

- **Dependent enrichment**: Use extracted coordinates to look up location names
- **Fact combination**: Merge data from multiple sources into derived facts

Example: reverse geocoding files that have coordinates but no city name:

```bash
canon worklist --emit content.geo.lat --emit content.geo.lon --where 'geo.lat? AND NOT geo.city?' \
  | ./scripts/reverse-geocode.sh \
  | canon import-facts
```

## Staleness Detection

The worklist is a snapshot of sources at a point in time. Each entry includes `basis_rev` which tracks file changes. Processors should pass this through to `import-facts`, which will skip the import if the file changed since the worklist was generated.

The `size` and `mtime` fields allow processors to verify a file hasn't changed before extracting facts.
