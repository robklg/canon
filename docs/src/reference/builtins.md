# Built-in Facts Reference

These facts are automatically available for all sources without enrichment.

## Source Facts

| Fact | Type | Description |
|------|------|-------------|
| `source.id` | num | Database ID (hidden*) |
| `source.ext` | text | File extension (lowercase, no dot) |
| `source.size` | num | File size in bytes |
| `source.mtime` | time | Modification timestamp |
| `source.path` | path | Full absolute path |
| `source.root` | path | Root directory path (hidden) |
| `source.rel_path` | path | Path relative to root (hidden) |
| `source.device` | num | Device ID (hidden) |
| `source.inode` | num | Inode number (hidden) |

## Content Facts

| Fact | Type | Description |
|------|------|-------------|
| `content.hash.sha256` | text | SHA-256 content hash |

## Policy Facts

| Fact | Type | Description |
|------|------|-------------|
| `policy.exclude` | text | Source exclusion flag |

## Pattern Aliases

These aliases are available in [pattern expressions](expr.md):

| Alias | Expands To |
|-------|------------|
| `filename` | `source.rel_path[-1]` |
| `stem` | `source.rel_path[-1]\|stem` |
| `ext` | `source.rel_path[-1]\|ext` |
| `hash` | `content.hash.sha256` |
| `hash_short` | `content.hash.sha256\|short` |
| `id` | `source.id` |

*Hidden facts are not shown in [`canon facts`](../commands/query/facts.md) by default. Use `--all` to include them.