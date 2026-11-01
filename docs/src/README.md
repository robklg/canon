# Introduction

Canon is a CLI tool for organizing large collections of files (photos, music, documents) scattered across old hard drives, backup directories, cloud downloads, and phone exports. It indexes files across any number of locations, identifies content by hash regardless of name or path, and lets you query and filter everything with metadata. When you're ready, it archives what matters and [excludes](concepts/exclusion.md) what is not worth keeping, both [on the record](concepts/decisions.md). Storage that holds nothing unresolved can be [retired](concepts/retirement.md): its complete story stays readable, in plain text, after the storage is gone.

## The Problem

Files accumulate over years and across devices, and the scale makes it hard to even start. Manual approaches are risky: one wrong move and something irreplaceable could be gone.

## The Approach

Canon works incrementally:

1. **Scan** directories to index files and compute content hashes
2. **Enrich** with metadata extracted by external tools (EXIF, file types, etc.)
3. **Orient**: explore with filters and queries, [survey](commands/query/survey.md) a location (what's archived, where it connects, what's unique), or [sweep](commands/query/sweep.md) all roots for the places where one action resolves the most
4. **Archive** selected files to a canonical location, at your own pace
5. **Resolve** the rest: exclude what is not worth keeping, and retire storage once everything on it is archived or excluded

Each step is revisitable, and Canon tracks what's already archived.

Scanning and querying never modify or move your files.

To get started, see [Setup](setup.md) and [Getting Started](getting-started.md).
