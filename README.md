# Canon

Canon helps you understand and take control of digital assets spread across many drives, backups, and years.

Scan your devices, enrich with metadata, discover what you have, and archive selected files to a canonical location — at your own pace, in small, revisitable steps.

## Features

| Feature | Description |
|---------|-------------|
| **Multi-root scanning** | Index files across multiple drives and backups |
| **Extensible metadata** | Enrich with facts from any external tool (exiftool, etc.) |
| **Powerful filtering** | Boolean expressions with modifiers, path accessors, and globs |
| **Content deduplication** | Identify duplicates by hash across all sources |
| **Integrity validation** | Detect corruption and verify transfers with partial hashing |
| **Offline queryable** | Explore metadata even when source drives are disconnected |
| **Cluster and apply** | Generate manifests, preview changes, and archive with confidence |

## Installation

```bash
cargo install --path .
```

Or build manually:

```bash
cargo build --release
cp target/release/canon /usr/local/bin/
```

## Documentation

Full documentation is available at **[robklg.github.io/canon](https://robklg.github.io/canon/)**.

The documentation covers:

- Setup and getting started
- Core concepts: roots, sources, objects, and facts
- Command reference: scan, enrich, query, and archive
- Filter syntax and pattern expressions

## Quick Example

```bash
# Scan your sources and archive
canon scan --add --role source /path/to/photos
canon scan --add --role archive /Volumes/Archive

# Enrich with EXIF metadata
canon worklist --where 'source.ext|lowercase IN (jpg, jpeg)' \
  | ./scripts/exif-worklist.sh \
  | canon import-facts

# Explore what you have
canon facts --key content.DateTimeOriginal|year
canon ls --where 'content.geo.city=Amsterdam'

# Archive a selection
canon cluster generate \
  --where 'content.DateTimeOriginal|year=2023' \
  --dest /Volumes/Archive/Photos/2023

# Edit manifest.toml to customize the output pattern, then:
canon apply manifest.toml --dry-run
canon apply manifest.toml
```
