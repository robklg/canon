# Canon Commands

## Common Options

Most commands that operate on sources share these options:

**Path scope** — Limit a command to a specific directory by passing a path:

```bash
canon ls /path/to/photos
canon facts /path/to/photos
canon coverage /path/to/photos
```

**Filters** — Select sources using `--where` with boolean expressions:

```bash
canon ls --where 'source.ext=jpg'
canon facts --where 'source.size > 1000000'
canon cluster generate --where 'geo.country=Netherlands' --dest /archive
```

Multiple `--where` flags are combined with AND. See [Filters](../reference/filter.md) for the full syntax.

## Command Reference

- [Managing Roots](roots/index.md): Add and manage storage locations
  - [scan](roots/scan.md): Scan existing or new roots
  - [roots](roots/roots.md): List, suspend, or remove roots
- [Enriching](enrich/index.md): Import metadata from external tools
  - [worklist](enrich/worklist.md): Output sources for external processing
  - [import-facts](enrich/import-facts.md): Import processor output
  - [Writing Processors](enrich/processors.md): Build custom extractors
- [Querying](query/index.md): Explore your indexed files
  - [ls](query/ls.md): List sources matching filters
  - [facts](query/facts.md): Discover available metadata
  - [compare](query/compare.md): Compare directories by content
- [Managing Sources](manage/index.md): Control which sources are processed
  - [exclude](manage/exclude.md): Mark sources to skip during archiving
- [Archiving](archive/index.md): Organize files into your canonical archive
  - [coverage](archive/coverage.md): Check archive progress
  - [cluster](archive/cluster.md): Generate a manifest for archiving
  - [apply](archive/apply.md): Execute the manifest to copy/move files