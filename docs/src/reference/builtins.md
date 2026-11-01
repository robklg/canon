# Source Facts and Built-in Keys

These keys need no enrichment. `hash`, `hash_short` and the content hash have a value once Canon has hashed the source.

## Source Facts

Source facts come from what the scanner records. The set is fixed.

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

[`canon facts`](../commands/query/facts.md) shows the hidden ones with `--all`.

## Built-in Keys

Short names for values used often.

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

A name that is a built-in key refers to that key in `--where`, `--key`, `--group-by`, `worklist --emit` and a [pattern](expr.md). A content fact imported under such a name (`size`, `filename`, `id`) is stored as `content.size` and so on, and a filter reaches it only by that full name.

## The Content Hash

| Key | Type | Description |
|------|------|-------------|
| `content.hash.sha256` | text | SHA-256 content hash, recorded when Canon hashes a source |

## In a Pattern

`{hash}`, `{hash_short}` and `{content.hash.sha256}` use the content hash the lock file recorded for the source. Patterns also have [`{scope.rel_path}`](expr.md#scoperel_path), which no filter has.
