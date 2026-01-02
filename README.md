# Canon

A CLI tool for organizing large media libraries into a canonical archive.

Canon helps you deduplicate, organize, and archive large collections of files (photos, videos, documents) by tracking content hashes and metadata, then generating organized output structures.

## Installation

```bash
cargo build --release
```

The binary will be at `./target/release/canon`.

## Quick Start

```bash
# 1. Scan your files
canon scan /path/to/photos

# 2. Compute content hashes
canon worklist --where '!content.hash.sha256?' | ./scripts/hash-worklist.sh | canon import-facts

# 3. See what you have
canon facts

# 4. Generate a manifest for organizing files
canon cluster generate --where 'content.hash.sha256?'

# 5. Preview what would be copied
canon apply manifest.toml --dry-run

# 6. Apply the manifest
canon apply manifest.toml
```

## Core Concepts

### Sources

A **source** is a file discovered on disk. Canon tracks:
- Location (root + relative path)
- Physical identity (device + inode)
- Size and modification time
- A `basis_rev` that increments when the file changes

### Objects

An **object** represents unique content identified by its hash. Multiple sources can point to the same object (duplicates). Objects are created when you import a content hash.

### Facts

**Facts** are key-value metadata attached to sources or objects. There are two namespaces:

- `source.*` - Built-in facts derived from the file itself (extension, size, mtime, path)
- `content.*` - Facts about the content, stored on objects (hash, EXIF data, mime type)

When you import facts, like hash.sha256, they're automatically namespaced under `content.*`, so you query them later like `content.hash.256`.
Content facts are stored on objects when a content hash is known; otherwise they are attached to the source.
This is referred to as 'promotion'.

### Roots

A **root** is a top-level directory that Canon tracks. Roots have a role:
- `source` - Where your unorganized files live (default)
- `archive` - Where organized files are stored

By default, Canon will not copy files into an archive if an identical object already exists there

## Commands

### canon scan

Scan directories and index files.

```bash
# Scan one or more directories
canon scan /path/to/photos /path/to/more/photos

# Scan as an archive (for tracking already-organized files)
canon scan /path/to/archive --role archive
```

Output shows what was found:
```
Scanned 1234 files: 100 new, 5 updated, 2 moved, 1127 unchanged, 0 missing
```

### canon worklist

Output sources as JSONL for processing by external tools.

A worklist is a snapshot of sources at a point in time. If files change, fact imports may be skipped.

```bash
# All sources (from source roots only)
canon worklist

# Only sources missing a content hash
canon worklist --where '!content.hash.sha256?'

# Only JPG files
canon worklist --where 'source.ext=jpg'

# Scope to a specific directory
canon worklist /path/to/photos

# Include sources from archive roots (for backfilling facts)
canon worklist --include-archived
```

Output format (one JSON object per line):
```json
{"source_id":123,"path":"/full/path/to/file.jpg","root_id":1,"size":1024,"mtime":1703980800,"basis_rev":0}
```

### canon import-facts

Import facts from JSONL on stdin.

```bash
# Import facts from a processor
some-processor | canon import-facts

# Allow importing facts for sources in archive roots
some-processor | canon import-facts --allow-archived
```

Input format:
```json
{"source_id":123,"basis_rev":0,"facts":{"hash.sha256":"abc123...","mime":"image/jpeg"}}
```

Facts are automatically namespaced under `content.*`. The special key `hash.sha256` creates/links an object.

If `basis_rev` doesn't match the source's current value, the import is skipped (the file changed since the worklist was generated).

By default, importing facts for sources in archive roots is skipped. Use `--allow-archived` to enable this (useful for backfilling metadata on already-archived files).

### canon facts

Discover what metadata you have and check coverage.

```bash
# Overview of all facts (source roots only by default)
canon facts

# Scoped to a directory
canon facts /path/to/photos

# With filters
canon facts --where 'source.ext=jpg'

# Value distribution for a specific fact
canon facts content.Make

# Show hidden built-in facts
canon facts --all

# Unlimited results (default is 50)
canon facts content.hash.sha256 --limit 0

# Include sources from archive roots
canon facts --include-archived
```

Example output:
```
Sources matching filters: 34692

Fact                               Count   Coverage
────────────────────────────────────────────────────
source.ext                         34692     100.0%  (built-in)
source.size                        34692     100.0%  (built-in)
source.mtime                       34692     100.0%  (built-in)
source.path                        34692     100.0%  (built-in)
content.hash.sha256                34692     100.0%
content.mime                       34692     100.0%
content.Model                       7935      22.9%
content.Make                        7935      22.9%
...
```

### canon cluster generate

Generate a manifest of files matching filters.

```bash
# All files with content hashes (source roots only by default)
canon cluster generate --where 'content.hash.sha256?'

# Custom output file
canon cluster generate --where 'content.hash.sha256?' -o my-manifest.toml

# Include sources from archive roots
canon cluster generate --where 'content.hash.sha256?' --include-archived

# Show which files were excluded (already archived)
canon cluster generate --where 'content.hash.sha256?' --show-archived
```

The manifest is a TOML file containing the query, output pattern, and all matching sources with their facts.

### canon apply

Apply a manifest to copy files.

```bash
# Preview what would happen
canon apply manifest.toml --dry-run

# Actually copy files
canon apply manifest.toml

# Skip all pre-flight checks
canon apply manifest.toml --force

# Allow duplicates across archives (but not within destination)
canon apply manifest.toml --allow-cross-archive-duplicates
```

**Pre-flight checks** (skipped with `--force`):

1. **Destination collisions** - If multiple sources would map to the same destination path (e.g., using `{filename}` when sources have duplicate names), apply aborts with an error showing which files conflict. This prevents silent data loss.

2. **Archive conflicts** - Checks if files already exist in the destination archive or other archives.

Edit the manifest's `[output]` section to customize the destination:

```toml
[output]
pattern = "{year}/{month}/{filename}"
base_dir = "/path/to/archive"
```

Available pattern variables:
- `{filename}` - Original filename
- `{stem}` - Filename without extension
- `{ext}` - File extension
- `{hash}` - Full content hash
- `{hash_short}` - First 8 characters of hash
- `{id}` - Source ID
- `{year}`, `{month}`, `{day}`, `{date}` - From EXIF DateTimeOriginal
- Any fact key with dots replaced by underscores (e.g., `{content_Make}`)

## Filter Syntax

Filters select sources based on facts using a boolean expression language.

### Basic Operators

| Syntax | Meaning |
|--------|---------|
| `key?` | Fact exists |
| `key=value` | Fact equals value |
| `key!=value` | Fact doesn't equal value |
| `key>value` | Greater than (numbers/dates) |
| `key>=value` | Greater or equal |
| `key<value` | Less than |
| `key<=value` | Less or equal |
| `key IN (v1, v2, ...)` | Fact matches any value in list |

### Boolean Operators

| Syntax | Meaning |
|--------|---------|
| `expr AND expr` | Both conditions must match |
| `expr OR expr` | Either condition matches |
| `NOT expr` | Negates the condition |
| `(expr)` | Grouping for precedence |

Operator precedence (highest to lowest): NOT, AND, OR. Use parentheses to override.

### Values

- Numbers: `1000000`, `-5`, `3.14`
- Dates: `2024-01-15` or `2024-01-15T12:00:00`
- Strings: `jpg`, `Apple`, or quoted `"value with spaces"`

### Examples

```bash
# Files with a content hash
--where 'content.hash.sha256?'

# Files missing a content hash
--where 'NOT content.hash.sha256?'

# JPG files only
--where 'source.ext=jpg'

# JPG or PNG files
--where 'source.ext=jpg OR source.ext=png'

# Common image formats
--where 'source.ext IN (jpg, png, gif, webp)'

# Not temporary files
--where 'NOT source.ext=tmp'

# iPhone photos
--where 'content.Make=Apple'

# Files larger than 1MB
--where 'source.size>1000000'

# Files modified in 2024 or later
--where 'source.mtime>=2024-01-01'

# Large images (combining with parentheses)
--where '(source.ext=jpg OR source.ext=png) AND source.size>1000000'

# Multiple --where flags combine with AND
--where 'source.ext=jpg' --where 'content.Make=Apple'
```

## Workflows

### Hash all files

```bash
canon worklist --where '!content.hash.sha256?' \
  | ./scripts/hash-worklist.sh \
  | canon import-facts
```

### Extract EXIF metadata

Use exiftool or similar to extract metadata:

```bash
canon worklist --where 'source.ext=jpg' | while read -r line; do
  path=$(echo "$line" | jq -r '.path')
  source_id=$(echo "$line" | jq -r '.source_id')
  basis_rev=$(echo "$line" | jq -r '.basis_rev')

  # Extract EXIF as JSON
  exif=$(exiftool -json "$path" 2>/dev/null | jq '.[0] | {Make, Model, DateTimeOriginal}')

  jq -nc \
    --argjson source_id "$source_id" \
    --argjson basis_rev "$basis_rev" \
    --argjson facts "$exif" \
    '{source_id: $source_id, basis_rev: $basis_rev, facts: $facts}'
done | canon import-facts
```

### Organize photos by date

```bash
# Generate manifest for all hashed photos
canon cluster generate --where 'content.hash.sha256?' --where 'source.ext=jpg'

# Edit manifest.toml to set output pattern
# pattern = "{year}/{month}/{date}_{filename}"
# base_dir = "/Volumes/Archive/Photos"

# Preview
canon apply manifest.toml --dry-run

# Execute
canon apply manifest.toml

# Scan the archive to track what's there
canon scan /Volumes/Archive/Photos --role archive
```

### Find duplicates

```bash
# After hashing, check value distribution
canon facts content.hash.sha256 --limit 0 | grep -v "0.0%"
```

Hashes with count > 1 are duplicates.

## Configuration

### Database Location

By default, Canon stores its database at `~/.canon/canon.db`. Override with:

```bash
canon --db /path/to/my.db scan /photos
```

### Concurrent Access

Canon uses SQLite in WAL mode with busy timeout, so multiple commands can run simultaneously* (e.g., parallel import-facts pipelines).

*Note that commands that modify the filesystem (apply) should not be run concurrently.

## Built-in Facts Reference

| Fact | Description |
|------|-------------|
| `source.ext` | File extension (lowercase) |
| `source.size` | File size in bytes |
| `source.mtime` | Modification time (unix timestamp) |
| `source.path` | Full absolute path |
| `source.root` | Root directory path (--all only) |
| `source.rel_path` | Path relative to root (--all only) |
| `source.device` | Device ID (--all only) |
| `source.inode` | Inode number (--all only) |
| `content.hash.sha256` | SHA-256 content hash |
