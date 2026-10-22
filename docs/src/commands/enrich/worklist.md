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

# Include keys' values in output (for chained enrichment)
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

With `--emit`, each source's value for the requested keys is included in the output (`null` if it has none):

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

`--emit` reads a key's name as `--where` does: a [source fact or built-in key](../../reference/builtins.md) emits its value for that source, and any other name is a content fact, so `--emit geo.lat` emits `content.geo.lat`. `--emit` takes a name, never an [alias](../../reference/filter.md#aliases): `--emit @taken` looks for a key named `@taken` and, where none has facts, emits `null` on every row. Each field is named as given. A source fact's time is an instant with no recorded offset, written with `-00:00`, and a file with no extension emits `null` for `source.ext`, which has no value for it:

```bash
canon worklist --emit Make --emit source.mtime --emit source.ext
```

```json
{"source_id":1,"path":"/photos/README","root_id":1,"size":5,"mtime":1792271246,"basis_rev":0,"facts":{"Make":null,"source.mtime":"2026-10-17T21:07:26-00:00","source.ext":null}}
{"source_id":2,"path":"/photos/trip/IMG_0125.jpg","root_id":1,"size":3,"mtime":1792271246,"basis_rev":0,"facts":{"Make":"Canon","source.mtime":"2026-10-17T21:07:26-00:00","source.ext":"jpg"}}
```

A name starting with `source.` that is no source fact is refused before anything is selected, every such name on its own line, and nothing is written to stdout:

```
$ canon worklist --emit source.extension --emit source.nme
Error: cannot use the key as given
  `source.extension` is an unknown key
    `canon facts` lists the keys
  `source.nme` is an unknown key
    `canon facts` lists the keys
```

A `--where` filter with a problem is refused in the same run, its problems first and under its own first line:

```
$ canon worklist --emit source.extension --where 'source.ext=a%b'
Error: cannot run the filter as given
  source.ext=a%b
              ^ `%` needs quotes around the value: source.ext="a%b"
  `source.extension` is an unknown key
    `canon facts` lists the keys
```

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
