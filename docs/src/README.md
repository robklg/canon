# Introduction

Canon is a CLI tool for organizing large collections of files — photos, music, documents — scattered across old hard drives, backup folders, cloud downloads, and phone exports. It indexes files across any number of locations, identifies duplicates by content hash, and lets you query and filter everything with metadata. When you're ready, it safely archives what matters to an organized destination.

## The Problem

Files accumulate over years and across devices. Backup drives pile up. You know there are things worth keeping in there, but the scale makes it hard to even start. Manual approaches are risky — one wrong move and something irreplaceable could be gone. So the drives keep sitting in drawers.

## The Approach

Canon takes a methodical, incremental approach:

1. **Scan** directories to index files and compute content hashes
2. **Enrich** with metadata extracted by external tools (EXIF, file types, etc.)
3. **Explore** what you have using filters and queries
4. **Archive** selected files to a canonical location, at your own pace

Each step is revisitable. You can scan new drives, add more metadata, refine your queries, and archive in small batches. Canon tracks what's already archived, so you always know your progress.

Canon never modifies or moves your source files. Every operation that changes anything has dry-run, preview, and confirmation. You can point it at a drive and explore freely without risk.

## Key Features

- **Content-based deduplication**: Files are identified by their content hash, not by name or location — the same photo in three backup folders is recognized as one thing
- **Flexible metadata**: Import any key-value facts from external tools (EXIF data, MIME types, geolocation, or anything you want)
- **Powerful filtering**: Query by any combination of facts using boolean expressions and aliases
- **Safe archiving**: Preview operations with `--dry-run`, validate integrity during transfer, and track what's been archived
- **Incremental workflow**: Work at your own pace — scan a drive today, enrich it next week, archive a batch next month

Ready to get started? See [Setup](setup.md) and [Getting Started](getting-started.md).
