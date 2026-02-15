# canon worklist

Output sources as JSONL for processing by external tools.

```bash
# All sources (from source roots only)
canon worklist

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
| `mtime` | Modification time (Unix timestamp) |
| `basis_rev` | Revision counter for staleness detection |

## Emitting Existing Facts

With `--emit`, requested facts are included in the output (`null` if absent):

```bash
canon worklist --emit geo.lat --emit geo.lon
```

```json
{"source_id":123,"path":"/...","basis_rev":0,"facts":{"geo.lat":52.37,"geo.lon":4.89}}
{"source_id":124,"path":"/...","basis_rev":0,"facts":{"geo.lat":null,"geo.lon":null}}
```

This enables processors to build on previous enrichment:

- **Dependent enrichment**: Use extracted coordinates to look up location names
- **Fact combination**: Merge data from multiple sources into derived facts

Example: reverse geocoding files that have coordinates but no city name:

```bash
canon worklist --emit geo.lat --emit geo.lon --where 'geo.lat? AND NOT geo.city?' \
  | ./scripts/reverse-geocode.sh \
  | canon import-facts
```

## Staleness Detection

The worklist is a snapshot of sources at a point in time. Each entry includes `basis_rev` which tracks file changes. Processors should pass this through to `import-facts`, which will skip the import if the file changed since the worklist was generated.

The `size` and `mtime` fields allow processors to verify a file hasn't changed before extracting facts.
