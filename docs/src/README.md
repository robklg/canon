# Introduction

Canon is a CLI tool for organizing large collections of files (photos, music, documents) scattered across old hard drives, backup folders, cloud downloads, and phone exports. It indexes files across any number of locations, identifies content by hash regardless of name or path, and lets you query and filter everything with metadata. When you're ready, it archives what matters and [excludes](concepts/exclusion.md) what is not worth keeping, both on the record. Storage that holds nothing unresolved can be [retired](concepts/retirement.md): its complete story stays readable, in plain text, after the storage is gone.

## The Problem

Files accumulate over years and across devices. Backup drives pile up. You know there are things worth keeping in there, but the scale makes it hard to even start. Manual approaches are risky: one wrong move and something irreplaceable could be gone. So the drives keep sitting in drawers.

## The Approach

Canon works incrementally:

1. **Scan** directories to index files and compute content hashes
2. **Enrich** with metadata extracted by external tools (EXIF, file types, etc.)
3. **Orient**: explore with filters and queries, [survey](commands/query/survey.md) a location's archive overlap, or [sweep](commands/query/sweep.md) all roots for the places where one decision resolves the most
4. **Archive** selected files to a canonical location, at your own pace
5. **Resolve** the rest: [exclude](concepts/exclusion.md) what is not worth keeping, and [retire](concepts/retirement.md) storage once everything on it is archived or excluded

Each step is revisitable. You can scan new drives, add more metadata, refine your queries, and archive in small batches. Canon tracks what's already archived, so you always know your progress. Every action that changes anything is [recorded](concepts/decisions.md), with your optional reason, so past decisions stay reviewable.

Scanning and querying never modify or move your files. Every operation that changes anything has dry-run, preview, and confirmation.

## Key Features

- **Content-based deduplication**: Files are identified by their content hash, not by name or location; the same photo in three backup folders is recognized as one thing
- **Metadata**: Import any key-value facts from external tools (EXIF data, MIME types, geolocation, or anything you want)
- **Filtering**: Query by any combination of facts using boolean expressions and aliases
- **Archiving**: Preview operations with `--dry-run`, validate integrity during transfer, and track what's been archived
- **Decision record**: Every effectful action is recorded with an optional reason and a durable on-disk receipt; [`canon trail`](commands/query/trail.md) reads the history back
- **Retirement**: A resolved root leaves the index with its complete story bound into a plain-text [book](reference/book-format.md) that outlives both the storage and the database
- **Incremental workflow**: Work at your own pace: scan a drive today, enrich it next week, archive a batch next month

To get started, see [Setup](setup.md) and [Getting Started](getting-started.md).
