# Getting Started

This guide walks through a typical Canon workflow: scanning files, enriching with metadata, querying, orienting, archiving, and resolving what remains.

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

See [Enriching](commands/enrich/index.md) for details on the worklist/import pipeline.

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

## Orienting

Survey a location to see how it relates to everything else you've indexed:

```bash
canon survey /Volumes/OldDrive/photos
```

The summary shows what's archived, which other locations share content, and how much exists only here. When you don't know where to work next, sweep ranks the places where one decision resolves the most:

```bash
canon sweep
```

See [survey](commands/query/survey.md) and [sweep](commands/query/sweep.md) for reading the output.

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

## Resolving

Content that isn't worth keeping is [excluded](concepts/exclusion.md) rather than deleted: nothing is destroyed, and the decision is recorded.

```bash
# This folder's content is verified in the archive; dismiss the redundant copies
canon exclude set /Volumes/OldDrive/photos --reason "verified archived, originals redundant"
```

Every action is recorded. Read a place's history back at any time:

```bash
canon trail /Volumes/OldDrive/photos
```

When everything on a root is archived or excluded, [retire it](commands/roots/retire.md): its complete story is bound into a plain-text book at the archive, and the storage is free to go.

## Next Steps

- Learn about [Concepts](concepts/index.md) to understand how Canon models your files
- Explore the full [Commands](commands/index.md) reference
- See [Filters](reference/filter.md) for advanced query syntax
