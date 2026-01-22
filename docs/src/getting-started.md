# Getting Started

This guide walks through a typical Canon workflow: scanning files, enriching with metadata, querying, and archiving.

## Scanning

First, index your source files and existing archive:

```bash
# Add source roots (files you want to organize)
canon scan --add --role source /path/to/photos
canon scan --add --role source /path/to/backup-drive/photos
canon scan --add --role source --comment "Old backup, possibly duplicates" /Volumes/OldDrive

# Add an archive root (your organized destination)
canon scan --add --role archive /Volumes/Archive
```

By default, Canon computes content hashes during scanning. This enables deduplication and archive tracking.

## Enriching

Use external tools to extract metadata. The example below uses [exiftool](https://exiftool.org/) to extract EXIF data including GPS-based geolocation:

```bash
canon worklist --where 'source.ext|lowercase IN (jpg, jpeg, heic, mov, mp4)' \
  | ./scripts/exif-worklist.sh \
  | canon import-facts
```

See [Enriching](commands/enrich.md) for details on the worklist/import pipeline.

## Querying

Discover what facts are available and explore your files:

```bash
# See all available facts
canon facts

# Check value distribution for a specific fact
canon facts --key content.geo.region          # Where were photos taken?
canon facts --key "content.DateTimeOriginal|year"  # Which years?

# List files matching filters
canon ls --where 'content.geo.city=Bletchley'

# Preview files (macOS)
canon ls -0 --where 'content.geo.city=Bletchley' | xargs -0 open -a Preview
```

## Archiving

When you find a collection worth archiving, create a manifest:

```bash
canon cluster generate \
  --where 'content.DateTimeOriginal|year=2023' \
  --where 'content.geo.region="North Holland"' \
  --dest /Volumes/Archive/Trips/2023-Amsterdam
```

This creates `manifest.toml` with the query parameters and a `manifest.lock` with matching sources.

Edit `manifest.toml` to customize the output pattern:

```toml
[output]
pattern = "{content.DateTimeOriginal|date}/{filename}"
base_dir = "/Volumes/Archive/Trips/2023-Amsterdam"
```

Preview and apply:

```bash
canon apply manifest.toml --dry-run   # Preview what will happen
canon apply manifest.toml             # Execute the copy
```

Files are copied to the archive with paths like:
```
/Volumes/Archive/Trips/2023-Amsterdam/2023-06-16/IMG_001.jpg
```

## Next Steps

- Learn about [Concepts](concepts/index.md) to understand how Canon models your files
- Explore the full [Commands](commands/index.md) reference
- See [Filters](reference/filter.md) for advanced query syntax
