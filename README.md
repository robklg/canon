# Canon

Canon helps you understand and take control of digital assets spread across many drives, backups, and years — without requiring you to reorganize or delete anything upfront.

## Introduction

Canon is designed to be used iteratively. It supports two primary ways of working:

- discovering and understanding what you have
- intentionally archiving selected assets into a tracked archive

Discovery already provides value on its own.
Archiving is optional, and typically incremental and revisitable.

---

### 1. Discovery

Canon can be used purely as a discovery and search tool.

After scanning sources, Canon allows you to **enrich assets with metadata ("facts")** and query across them — even when the files themselves live on many drives, backups, or disconnected storage.

Fact enrichment is deliberately open-ended:

- facts may come from Canon itself
- from external tools (e.g. `exiftool`)
- or from any process that can extract information from files

Anything that can be extracted or inferred from a file can become searchable in Canon.

Using a powerful boolean expression language, this allows queries such as:

- Where are all my photos from 2017 shot with my iPhone 7?
- Which assets belong to a specific date range or device?
- What do I actually have across all my backups?
- Discover files you forgot you had

In practice, discovery often surfaces assets you forgot existed —
for example, old imports that were never organized (e.g. `microsd_import`, `copy`, `DCIM_1`).

Because Canon stores metadata independently of storage:

- portable drives can be disconnected
- old backups can be put away
- your data remains fully searchable

Discovery alone already provides value, without any organization or export.

---
### 2. Archiving (Cluster and Apply)

Canon lets you archive assets by selecting and grouping them, and then materializing them into an archive.
Different kinds of assets often require different approaches, so archiving typically happens in multiple passes.

Clustering is how you express intent about *which assets should be archived together in the same location*.
It is based on selection and filtering, not on modifying sources.

Clustering can be defined using:

- path scopes
- expressive `where` expressions over enriched facts
- combinations of both

Once you constructed the right expression for the files to cluster, Canon can generate a manifest with the selected sources.
This manifest is then edited to define how the clustered assets should be placed in the archive, with an output pattern you build from facts and modifiers.

Applying a manifest materializes it within an archive root (by copying or moving files), expanding to the specified destination inside an archive.
Before any files are written, Canon previews what will happen and performs a strict preflight check to protect archive integrity:

- manifest is checked for validity and current state
- output paths are fully resolved
- collisions are detected
- unresolved collisions cause the apply step to abort before making any changes

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Core Concepts](#core-concepts)
- [Commands](#commands)
  - [Scan](#scan) – scan, roots
  - [Enrich](#enrich) – worklist, import-facts
  - [Discover](#discover) – ls, facts, coverage, compare
  - [Organize](#organize) – exclude, cluster, apply
- [Filter Syntax](#filter-syntax)
- [Workflows](#workflows)
- [Built-in Facts Reference](#built-in-facts-reference)

## Installation

```bash
cargo build --release
```

The binary will be at `./target/release/canon`.

## Quick Start

Canon is designed to be used iteratively and incrementally.
Instead of a single destructive run, you gradually build up metadata, apply policies, and converge on a canonical archive over time.

```bash
# Scan – index your source files and existing archive
canon scan --add --role source /path/to/photos
canon scan --add --role source /path/to/backup-drive/photos
canon scan --add --role archive /Volumes/Archive

# Enrich – compute content hashes (necessary for deduplication)
canon worklist --where 'NOT content.hash.sha256?' --where 'source.ext|lowercase IN (jpg, nef, heic)' \
  | ./scripts/hash-worklist.sh \
  | canon import-facts

# Enrich – extract EXIF metadata including GPS-based geolocation (city, region, country)
canon worklist --where 'source.ext|lowercase IN (jpg, jpeg, heic, mov, mp4)' \
  | ./scripts/exif-worklist.sh \
  | canon import-facts

# Discover – explore your collection
canon facts                                    # see all available facts
canon facts --key content.geo.region           # where were photos taken?
canon facts --key "content.media.capture_datetime|year"  # which years?

# Preview photos from your trip to Bletchley Park (macOS)
canon ls -0 --where 'content.geo.city=Bletchley' | xargs -0 open -a Preview

# Organize – archive your 2023 Amsterdam trip
canon cluster generate \
  --where 'content.media.capture_datetime|year = 2023' \
  --where 'content.geo.region = "North Holland"' \
  --dest /Volumes/Archive/Trips/2023-Amsterdam

# Edit manifest.toml: set pattern = "{content.media.capture_datetime|date}/{filename}"
canon apply manifest.toml --dry-run
canon apply manifest.toml
```

## Core Concepts

### Sources

A **source** is a file discovered on disk. Canon tracks:
- Location (root + relative path)
- Device ID and inode (for move detection and disconnected storage protection)
- Size and modification time
- Partial hash for integrity validation during transfers
- A `basis_rev` that increments when the file's size or mtime changes

### Objects

An **object** represents unique content identified by its hash. Multiple sources can point to the same object (duplicates). Objects are created when you import a content hash (`hash.sha256`) via the [Enrich](#enrich) pipeline.

Content hashing is essential: it enables deduplication, archive tracking, and integrity validation. Sources without a content hash cannot be organized into an archive.

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

By default, Canon will not copy files into an archive if an identical object already exists there.

## Commands

Commands follow a typical workflow: **Scan → Enrich → Discover → Organize**

---

## Scan

Index files and manage roots.

### canon scan

Scan directories and index files.

```bash
# Add a new root and scan it (--add and --role required for new roots)
canon scan --add --role source /path/to/photos

# Scan multiple new roots
canon scan --add --role source /path/to/photos /path/to/more/photos

# Add as an archive root (for tracking already-organized files)
canon scan --add --role archive /path/to/archive

# Re-scan an existing root (--role optional, validated against existing)
canon scan /path/to/photos

# Scan just a subtree within an existing root
canon scan /path/to/photos/2024

# Compute content hashes during scan (optional for source roots)
canon scan --compute-hashes /path/to/photos

# Verify archive integrity by recomputing all hashes (good for cron jobs)
canon scan --compute-hashes=all /Volumes/Archive
```

**Hash computation:** Archive roots automatically compute hashes for new/changed files during scan (required for duplicate detection). Source roots skip hashing by default—use `--compute-hashes` to hash during scan, or import hashes via the worklist pipeline for more control.

**Integrity verification:** Use `--compute-hashes=all` to recompute hashes for all files, even unchanged ones. This is especially useful for archives: run periodically (e.g., via cron) to detect file corruption. If a file's hash changes without its mtime changing, Canon warns about possible corruption and exits with an error.

**Discovering untracked directories:** Use `--candidates` to find directories with files that aren't yet under any root. This is useful when exploring a drive or backup to see what could be added:

```bash
# Find candidate roots to add under a path
canon scan --candidates /Volumes/Backup

# Output shows directories with untracked files
Candidate roots to add:
  /Volumes/Backup/photos  (3 directories with files)
  /Volumes/Backup/imports  (1 directory with files)
```

Directories under existing roots are skipped. When multiple subdirectories share a common ancestor that could be added as a single root, they're rolled up (unless that ancestor contains an existing root).

Output shows what was found:
```
Scanned 1234 files: 100 new, 5 updated, 2 moved, 1127 unchanged, 0 missing
Hashed 105 files
```

### canon roots

List and manage registered roots.

```bash
# List all roots with file counts
canon roots

# Remove a root by ID (files on disk are NOT deleted)
canon roots rm id:1

# Remove a root by path
canon roots rm path:/path/to/photos

# Skip confirmation prompt
canon roots rm id:1 --yes
```

Example output:
```
ID   ROLE       FILES  PATH
1    source     16635  /path/to/photos
2    archive   169941  /path/to/archive
```

When removing a root, Canon suggests using `canon ls <path>` to preview which sources will be forgotten. The root and all its sources are removed from the database, but files on disk are not deleted.

---

## Enrich

Add metadata to indexed files using external processors.

**Content hashing is required** before files can be organized into an archive. You should hash both your source files and any existing archive files (to enable deduplication and archive tracking).

Canon uses a pipeline model for enrichment: `worklist` outputs sources, an external processor reads files and extracts metadata, then `import-facts` stores the results.

```
canon worklist → processor → canon import-facts
```

Example pipeline to compute content hashes:
```bash
# Hash source files
canon worklist --where 'NOT content.hash.sha256?' | ./scripts/hash-worklist.sh | canon import-facts

# Hash existing archive files (for deduplication tracking)
canon worklist --include-archived --where 'NOT content.hash.sha256?' | ./scripts/hash-worklist.sh | canon import-facts --allow-archived
```

### Writing processors

The `canonargs` helper (in `canonargs/`) reduces boilerplate when writing processors. It handles the worklist input, runs a command for each file, and emits facts in the correct format.

```bash
# Extract a single fact (command outputs one value)
canon worklist | canonargs --fact mime -- file -b --mime-type {} | canon import-facts

# Extract key=value pairs (one per line)
canon worklist | canonargs --kv -- my-extractor {} | canon import-facts

# Extract JSON object
canon worklist | canonargs --json -- exiftool -json {} | canon import-facts
```

Processors can be chained since `canonargs` passes through the path and merges facts.

### canon worklist

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
canon worklist --include-archived

# Include existing facts in output (for chained enrichment)
canon worklist --emit content.geo.lat --emit content.geo.lon
```

Output format (one JSON object per line):
```json
{"source_id":123,"path":"/full/path/to/file.jpg","root_id":1,"size":1024,"mtime":1703980800,"basis_rev":0}
```

With `--emit`, requested facts are always included (`null` if absent):
```json
{"source_id":123,"path":"/...","...":"...","facts":{"content.geo.lat":52.37,"content.geo.lon":4.89}}
{"source_id":124,"path":"/...","...":"...","facts":{"content.geo.lat":null,"content.geo.lon":null}}
```

The worklist is a snapshot of sources at a point in time. Each entry includes `basis_rev` which tracks file changes. The `size` and `mtime` fields allow processors to verify a file hasn't changed since the scan before extracting facts.

### canon import-facts

Import facts from JSONL on stdin. Designed to receive output from a processor that read the worklist.

```bash
canon worklist | some-processor | canon import-facts

# Allow importing facts for sources in archive roots
canon worklist --include-archived | some-processor | canon import-facts --allow-archived
```

Input format (processor output):
```json
{"source_id":123,"basis_rev":0,"facts":{"hash.sha256":"abc123...","mime":"image/jpeg"}}
```

The processor must pass through `source_id` and `basis_rev` from the worklist entry. If `basis_rev` doesn't match the source's current value, the import is skipped (the file changed since the worklist was generated).

Facts are automatically namespaced under `content.*`. The special key `hash.sha256` creates/links an object.

By default, importing facts for sources in archive roots is skipped. Use `--allow-archived` to enable this (useful for backfilling metadata on already-archived files).

---

## Discover

Explore and analyze your library.

### canon ls

List sources matching filters. Useful for quick inspection and piping to other tools.

```bash
# List all sources in current directory
canon ls .

# List sources matching a filter
canon ls --where 'source.ext=jpg'

# Filter by source ID
canon ls --where 'source.id=12345'

# List only archived sources (content exists in an archive)
canon ls --archived

# List archived sources with their archive location(s)
# Output: source_path<TAB>archive_path (one line per archive location)
canon ls --archived=show

# List only unarchived sources (hashed but not in any archive)
canon ls --unarchived

# List only unhashed sources (no content hash yet)
canon ls --unhashed

# Show duplicate files (same content hash), grouped by hash
canon ls --duplicates

# Include sources from archive roots
canon ls --include-archived

# Include excluded sources
canon ls --include-excluded

# Long format with size and date
canon ls -l

# Null-delimited output for xargs (handles spaces in paths, macOS)
canon ls -0 --where 'source.ext=jpg' | xargs -0 open -a Preview
```

**Path display:**
- Relative path input (`.`, `subdir`) → relative output paths
- Absolute path input (`/path/to/dir`) → absolute output paths

Output is one path per line (stdout), with a count printed to stderr:
```
vacation/img001.jpg
vacation/img002.jpg
work/doc.pdf
3 sources
```

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
canon facts --key content.Make

# With modifiers: group mtime by year-month
canon facts --key source.mtime|yearmonth

# With accessors: distribution by top-level directory
canon facts --key source.rel_path[0]

# Combine accessor and modifier: distribution by filename extension
canon facts --key source.rel_path[-1]|ext

# Show hidden built-in facts
canon facts --all

# Unlimited results (default is 50)
canon facts --key content.hash.sha256 --limit 0

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

#### canon facts delete

Delete facts by key. Useful for removing incorrect or unwanted metadata.

```bash
# Preview deletion (dry-run by default)
canon facts delete content.mime --on object
canon facts delete content.Make --on source /path/to/photos --where 'source.ext=jpg'

# Execute deletion
canon facts delete content.mime --on object --yes
```

- `--on source` or `--on object` is required to specify entity type
- Protected namespaces (`source.*`, `policy.*`) cannot be deleted
- Dry-run by default; use `--yes` to execute

### canon prune

Clean up orphaned or stale data from the database.

```bash
# Preview stale facts (file changed since fact was recorded)
canon prune --stale-facts

# Preview orphaned objects (no present sources reference them)
canon prune --orphaned-objects

# Execute deletion
canon prune --stale-facts --yes
canon prune --orphaned-objects --yes
```

**Stale facts** are those where `observed_basis_rev` no longer matches the source's current `basis_rev` (meaning the file was modified after the fact was imported).

**Orphaned objects** are content entries with no remaining present sources. This can happen when files are deleted. You may want to keep them as a historical record, or delete them to clean up the database.

### canon coverage

Show archive coverage statistics - how many sources are hashed and how many are archived.

```bash
# Overview of all source roots
canon coverage

# Scoped to a specific directory
canon coverage /path/to/photos

# With filters
canon coverage --where 'source.ext=jpg'

# Coverage relative to a specific archive root
canon coverage --archive id:1
canon coverage --archive path:/path/to/archive

# Include archive roots in analysis
canon coverage --include-archived
```

Example output:
```
Archive Coverage Report

Root: /path/to/backup1 (source)
  Total sources:     1,234
  Hashed:            1,100 (89.1%)
  Archived:            850 (77.3% of hashed)
  Unarchived:          250

Root: /path/to/backup2 (source)
  Total sources:       567
  Hashed:              500 (88.2%)
  Archived:            400 (80.0% of hashed)
  Unarchived:          100

────────────────────────────────────────
Overall:
  Total sources:     1,801
  Hashed:            1,600 (88.8%)
  Archived:          1,250 (78.1% of hashed)
  Unarchived:          350
```

- **Hashed**: Sources with a content hash (ready for archiving)
- **Archived**: Sources whose content exists in an archive root
- With `--archive`: Shows "In this archive" vs "Not in archive" for that specific archive

### canon compare

Compare two folders by content hash. Useful for verifying backups or finding differences between directories.

```bash
# Compare two directories
canon compare /path/to/folder_a /path/to/folder_b

# With filters
canon compare /path/to/folder_a /path/to/folder_b --where 'source.ext=jpg'

# Summary only (no file lists)
canon compare /path/to/folder_a /path/to/folder_b --quiet
```

Output shows:
- Files only in A (by content)
- Files only in B (by content)
- Files in both (matching content hash)

Exit code is 0 if identical, 1 if differences found.

---

## Organize

Mark exclusions, generate manifests, and copy files.

### canon exclude

Manage source exclusions. Excluded sources are skipped by most commands.

```bash
# Mark sources as excluded (e.g., small files, temp files)
canon exclude set --where 'source.size<1000'
canon exclude set /path/to/photos --where 'source.ext=tmp'

# Exclude a specific file by path
canon exclude set /path/to/photos/unwanted.jpg

# Exclude by source ID (shown in ls --duplicates output)
canon exclude set --id 12345

# Preview what would be excluded
canon exclude set --where 'source.ext=bak' --dry-run

# List currently excluded sources
canon exclude list
canon exclude list /path/to/photos

# Remove exclusions
canon exclude clear
canon exclude clear --where 'source.ext=tmp'

# Preview what would be cleared
canon exclude clear --where 'source.ext=tmp' --dry-run
```

#### canon exclude duplicates

Automatically exclude duplicate files while keeping copies in a preferred location.

```bash
# Exclude duplicates, keeping files under /preferred/path
canon exclude duplicates /scope/path --prefer /preferred/path

# Preview what would be excluded
canon exclude duplicates /scope/path --prefer /preferred/path --dry-run

# With filters
canon exclude duplicates /scope/path --prefer /preferred/path --where 'source.ext=jpg'
```

This is useful for deduplicating across backup drives while keeping the "canonical" copy in your preferred location.

**How exclusions affect other commands:**

| Command | Default behavior | Override |
|---------|------------------|----------|
| `worklist` | Skips excluded | `--include-excluded` |
| `facts` | Skips excluded, shows count | `--include-excluded` |
| `coverage` | Stats on included only | `--include-excluded` shows excluded dimension |
| `cluster generate` | Always skips excluded | No override (hard gate) |
| `apply` | Blocks if manifest has excluded | No override (hard gate) |

Exclusions are stored as `policy.exclude` facts on sources.

### canon cluster generate

Generate a manifest of files matching filters. The `--dest` flag specifies where files will be copied and must be inside a registered archive root.

```bash
# All photos to an archive (unhashed sources are automatically skipped)
canon cluster generate --where 'source.ext IN (jpg, png, heic)' --dest /Volumes/Archive/Photos

# Destination can be a subdirectory within an archive
canon cluster generate --where 'source.ext IN (jpg, png, heic)' --dest /Volumes/Archive/Photos/2024

# Scope to a specific path
canon cluster generate /path/to/photos --dest /Volumes/Archive

# Custom output file
canon cluster generate --where 'source.ext=jpg' --dest /Volumes/Archive -o my-manifest.toml

# Include sources from archive roots
canon cluster generate --where 'source.ext=jpg' --dest /Volumes/Archive --include-archived

# Show which files were excluded (already archived)
canon cluster generate --where 'source.ext=jpg' --dest /Volumes/Archive --show-archived

# Overwrite existing manifest file
canon cluster generate --where 'source.ext=jpg' --dest /Volumes/Archive --force
```

The command generates two files: a manifest (`.toml`) that you edit, and a lock file (`.lock`) containing the source list.

**Typical workflow:**

```bash
canon cluster generate --where 'source.ext IN (jpg, png, heic)' --dest /Volumes/Archive
# Edit manifest.toml to customize the output pattern
canon apply manifest.toml --dry-run   # Preview
canon apply manifest.toml             # Execute
```

**Manifest structure:**

The generated manifest includes helpful comments listing all available pattern variables, modifiers, and aliases based on the facts present in your sources:

```toml
# Available facts for pattern (100% coverage on 1234 sources):
#
# Built-in:
#   filename           text   - Filename (last path component)
#   source.ext         text   - File extension
#   source.mtime       time   - Modification time
#   ...
#
# Content facts:
#   content.Make       text
#   content.Model      text
#   ...
#
# Modifiers:
#   Time: |year |month |day |date ...
#   String: |stem |ext |lowercase ...

[output]
pattern = "{filename}"           # ← Edit this to customize organization
base_dir = "/Volumes/Archive"
archive_root_id = 2
```

**Common output patterns:**

```toml
# Flat (default) - all files in base_dir
pattern = "{filename}"

# By EXIF date
pattern = "{content.DateTimeOriginal|year}/{content.DateTimeOriginal|month}/{filename}"

# By EXIF date with hash prefix (avoids collisions)
pattern = "{content.DateTimeOriginal|year}/{content.DateTimeOriginal|month}/{hash_short}_{filename}"

# By camera model
pattern = "{content.Make}/{content.Model}/{filename}"

# By file type
pattern = "{source.ext}/{filename}"
```

Use `canon cluster refresh manifest.toml` to update the lock file if sources have changed since generation.

### canon apply

Apply a manifest to copy/move files. Copied files are automatically registered in the database with the same content hash, so they're immediately recognized as archived (no separate `scan` needed).

```bash
# Preview what would happen (fast - skips source existence checks)
canon apply manifest.toml --dry-run

# Copy files (default mode, preserves mtime/permissions on Unix)
canon apply manifest.toml

# Show per-file progress during transfer
canon apply manifest.toml --verbose

# Rename files instead of copying (Unix only, fails on cross-device)
canon apply manifest.toml --rename

# Move files: rename if same device, copy+delete if cross-device
canon apply manifest.toml --move --yes

# Only apply sources from specific roots
canon apply manifest.toml --root id:1 --root id:2
canon apply manifest.toml --root path:/path/to/source

# Allow duplicates across archives (but not within destination)
canon apply manifest.toml --allow-cross-archive-duplicates
```

**Transfer modes:**

| Flag | Behavior |
|------|----------|
| (default) | Copy + preserve mtime/permissions (Unix) |
| `--rename` | Atomic rename; fails if cross-device (Unix only) |
| `--move` | Try rename; fallback to copy+delete on cross-device (Unix only, requires `--yes`) |

All modes use noclobber semantics: if a destination file exists, apply aborts with an error.

**Integrity validation:**

During transfer, Canon validates each source file's partial hash (first 8KB + last 8KB) to detect file corruption or modification since the manifest was generated. If validation fails, the transfer is aborted.

**Root filtering:**

Use `--root` to apply only a subset of sources from the manifest. Useful for staged application when sources are on different drives.

- `--root id:N` - Filter by root ID (shown in manifest as `root_id`)
- `--root path:/path` - Filter by root path (must match exactly)

**Pre-flight checks** (mandatory):

1. **Destination collisions** - If multiple sources would map to the same destination path (e.g., using `{filename}` when sources have duplicate names), apply aborts with an error showing which files conflict.

2. **Archive conflicts** - Checks if files already exist in the destination archive or other archives.

3. **Excluded sources** - Blocks if any sources in the manifest are marked as excluded.

Edit the manifest's `[output]` section to customize the destination:

```toml
[output]
pattern = "{content.DateTimeOriginal|year}/{content.DateTimeOriginal|month}/{filename}"
base_dir = "/path/to/archive"
```

Pattern variables use fact keys with optional modifiers (see [Filter Syntax](#filter-syntax) for the full modifier list):
- `{filename}`, `{stem}`, `{ext}` - Filename aliases
- `{hash}`, `{hash_short}` - Content hash aliases
- `{source.mtime|year}`, `{source.mtime|month}` - File modification date
- `{content.DateTimeOriginal|year}` - EXIF date with modifier
- `{content.Make}`, `{content.Model}` - Any fact key

---

## Filter Syntax

Filters select sources based on facts using a boolean expression language. Most commands accept `--where` to filter which sources they operate on. Multiple `--where` flags are combined with AND.

### Basic Operators

| Syntax | Meaning |
|--------|---------|
| `key?` | Fact exists |
| `key=value` | Fact equals value (case-sensitive) |
| `key!=value` | Fact doesn't equal value (case-sensitive) |
| `key~pattern` | Glob pattern match (case-sensitive) |
| `key!~pattern` | Glob pattern doesn't match |
| `key>value` | Greater than (numbers/dates) |
| `key>=value` | Greater or equal |
| `key<value` | Less than |
| `key<=value` | Less or equal |
| `key IN (v1, v2, ...)` | Fact matches any value in list |
| `key NOT IN (v1, v2, ...)` | Fact doesn't match any value in list |

### Glob Patterns

The `~` operator supports shell-style glob patterns:

| Pattern | Meaning |
|---------|---------|
| `*` | Match zero or more characters |
| `?` | Match exactly one character |
| `[abc]` | Match any character in set |
| `[a-z]` | Match character range |
| `[!abc]` | Match any character NOT in set |
| `\*` | Literal asterisk (escape) |

```bash
# Files starting with IMG_
--where 'filename~"IMG_*"'

# Files with 3-letter extension
--where 'source.ext~"???"'

# Files in a year subdirectory
--where 'source.rel_path~"*/2024/*"'

# Exclude temp files
--where 'filename!~"*.tmp"'
```

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

### Modifiers

Apply transformations to fact values using `|` syntax:

| Modifier | Description |
|----------|-------------|
| `year`, `month`, `day` | Extract date component from timestamp |
| `hour`, `minute`, `second` | Extract time component |
| `date`, `time`, `datetime` | Format as date/time string |
| `yearmonth` | Format as YYYY-MM |
| `week`, `weekday`, `quarter` | Date calculations |
| `stem` | Filename without extension |
| `ext` | File extension |
| `short` | First 8 characters (for hashes) |
| `lowercase` | Convert to lowercase |
| `uppercase` | Convert to uppercase |
| `capitalize` | Capitalize first letter |

```bash
# Files from 2024
--where 'source.mtime|year=2024'

# January photos
--where 'content.DateTimeOriginal|month=1'

# Case-insensitive extension matching
--where 'source.ext|lowercase=jpg'

# Case-insensitive glob
--where 'filename|lowercase~"img_*"'
```

### Path Accessors

Python-style indexing for path segments:

| Syntax | Meaning |
|--------|---------|
| `key[-1]` | Last segment (filename) |
| `key[0]` | First segment |
| `key[1:3]` | Slice segments |
| `key[:-1]` | All but last |

```bash
# Match by filename
--where 'source.rel_path[-1]=photo.jpg'

# Combine with modifiers
--where 'source.rel_path[-1]|stem=photo'
```

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

# Exclude certain extensions
--where 'source.ext NOT IN (tmp, bak, log)'

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

---

## Workflows

### Hash all files

```bash
canon worklist --where 'NOT content.hash.sha256?' \
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
# Generate manifest for all JPG photos (unhashed sources are automatically skipped)
canon cluster generate --where 'source.ext=jpg' --dest /Volumes/Archive/Photos

# Edit manifest.toml to set output pattern
# pattern = "{content.DateTimeOriginal|year}/{content.DateTimeOriginal|month}/{filename}"

# Preview
canon apply manifest.toml --dry-run

# Execute (automatically registers copied files in the database)
canon apply manifest.toml

# Optional: Scan an existing archive to track what's already there
# (not needed after apply, which auto-registers new files)
canon scan --add --role archive /Volumes/Archive/Photos
```

### Find and manage duplicates

```bash
# Show duplicate files grouped by content hash
canon ls --duplicates

# Scope to a specific directory
canon ls --duplicates /path/to/photos

# Exclude duplicates, keeping copies in preferred location
canon exclude duplicates /path/to/photos --prefer /path/to/photos/originals --dry-run
canon exclude duplicates /path/to/photos --prefer /path/to/photos/originals
```

---

## Built-in Facts Reference

| Fact | Description |
|------|-------------|
| `source.id` | Source database ID (--all only) |
| `source.ext` | File extension (lowercase) |
| `source.size` | File size in bytes |
| `source.mtime` | Modification time (unix timestamp) |
| `source.path` | Full absolute path |
| `source.root` | Root directory path (--all only) |
| `source.rel_path` | Path relative to root (--all only) |
| `source.device` | Device ID (--all only) |
| `source.inode` | Inode number (--all only) |
| `content.hash.sha256` | SHA-256 content hash |
| `policy.exclude` | Source is excluded (set via `canon exclude set`) |

Aliases for patterns: `filename`, `stem`, `ext`, `hash`, `hash_short`, `id`
