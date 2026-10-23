# Source Facts and Built-in Keys

These keys have a value for every source without enrichment, except `hash`, `hash_short` and the content hash, which a source has once Canon has hashed it.

## Source Facts

Read from what the scanner records. The set is fixed; a source fact is never imported or deleted.

| Key | Type | Description |
|------|------|-------------|
| `source.ext` | text | File extension (no dot); no value for a file with none (`README`, `.bashrc`, `trail.`) |
| `source.size` | number | File size in bytes |
| `source.mtime` | time | File modification time |
| `source.path` | path | Full absolute path |
| `source.rel_path` | path | Path relative to the root |
| `source.root` | path | Root path (hidden) |
| `source.id` | number | Source database ID (hidden) |
| `source.device` | number | Device ID (hidden) |
| `source.inode` | number | Inode number (hidden) |

Hidden source facts are not shown in [`canon facts`](../commands/query/facts.md) by default. Use `--all` to include them.

## Built-in Keys

Short names for values used often. Some have the same value as a source fact.

| Key | Type | Value |
|------|------|-------|
| `filename` | text | The last path component |
| `stem` | text | The file name without its extension |
| `ext` | text | The same as `source.ext` |
| `size` | number | The same as `source.size` |
| `mtime` | time | The same as `source.mtime` |
| `id` | number | The same as `source.id` |
| `root_id` | number | The ID of the source's root |
| `hash` | text | The content hash, `content.hash.sha256` |
| `hash_short` | text | The first 8 characters of the content hash |

[`canon facts`](../commands/query/facts.md) ends with these keys and their meanings when it is run without `--where`.

A name that is a built-in key reads as that key in `--where`, `--key`, `--group-by`, `worklist --emit` and a [pattern](expr.md). A content fact imported under such a name (`size`, `filename`, `id`) is stored as `content.size` and so on, and a filter reaches it only by that full name. [`canon facts delete`](../commands/maintain/facts-delete.md) reads every name under `content.`, so `canon facts delete size` deletes `content.size`.

## The Content Hash

| Key | Type | Description |
|------|------|-------------|
| `content.hash.sha256` | text | SHA-256 content hash, recorded when Canon hashes a source |

A content fact like any imported one, stored on the content.

## In a Pattern

A [pattern](expr.md) reads these keys as a filter does. `{hash}`, `{hash_short}` and `{content.hash.sha256}` read the content hash the lock file recorded for the source.

Patterns also have `{scope.rel_path}`, a key no filter has: the source's path below the scope the manifest records (see [pattern expressions](expr.md)).
