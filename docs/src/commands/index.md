# Canon Commands

## Common Options

Most commands that operate on sources share these options:

**Path scope**: Limit a command to a specific directory by passing a path:

```bash
canon ls /path/to/photos
canon facts /path/to/photos
canon coverage /path/to/photos
```

A path that is not under any known root is an error. A path that is under a
known root but holds no known sources depends on how many paths you gave:

| Paths given | Behavior |
|---|---|
| One, no known sources | Error: `no sources known at <path>` |
| Several, at least one with sources | The others are skipped and named; the command runs on the rest and exits 0 |
| Several, none with sources | Error naming every path |

A skipped path is always stated, never silently dropped:

```
no sources known at /path/to/photos/2012 — skipped
```

Where the line appears follows the command's own scope channel: stdout for
report commands (`facts`, `coverage`, `survey`), stderr for list commands
(`ls`, `worklist`), and in the ceremony before any confirmation for commands
that change state (`exclude set/clear/set-object`, `cluster generate`,
`facts delete`) — including under `--yes` and `--dry-run`. Display modes that
render a bare stream and carry no scope header of their own
(`coverage --compact`, `survey --detail unique`) state it on stderr, so what
is on stdout stays exactly what was asked for. A skipped path never appears
in the decision record.

Four commands never skip, because a location they name is load-bearing to the
question rather than one more place to look: both sides of `compare`, the
scope and `--prefer` paths of `exclude duplicates`, and `survey --other`.
These error as a single path does.

Paths match the index whichever Unicode normalization form you type. Canon
stores the form the disk gave it and matches your argument against that.

**Filters**: Select sources using `--where` with boolean expressions:

```bash
canon ls --where 'source.ext=jpg'
canon facts --where 'source.size > 1000000'
canon cluster generate --where 'geo.country=Netherlands' --dest /archive
```

Multiple `--where` flags are combined with AND. See [Filters](../reference/filter.md) for the full syntax.

**`--include`**: By default, query commands (`ls`, `facts`, `coverage`, `worklist`, `compare`) show sources from active source roots, hiding excluded and archived sources. Use `--include` to expand what you see:

```bash
canon ls --include excluded          # Also show excluded sources
canon ls --include archived          # Also show sources from archive roots
canon facts --include all            # Show everything
```

`--include` only changes what's displayed; it never modifies anything.

**`--allow`**: Commands that change state (`cluster generate`, `apply`, `import-facts`) skip certain sources by default (e.g., sources already in an archive). Use `--allow` to acknowledge you want to include them:

```bash
canon cluster generate --allow archived     # Include sources from archive roots
canon cluster generate --allow duplicates   # Include content already archived elsewhere
canon import-facts --allow archived         # Import facts for archive sources
```

The available `--allow` values are specific to each command. See individual command pages for details.

## Command Reference

- [Managing Roots](roots/index.md): Add and manage storage locations
  - [scan](roots/scan.md): Scan existing or new roots
  - [roots](roots/roots.md): List, suspend, or remove roots
  - [roots story](roots/story.md): Read a root's resolution story as a map of places
  - [roots retire](roots/retire.md): Bind a resolved root's story into a book and release the root
- [Enriching](enrich/index.md): Import metadata from external tools
  - [worklist](enrich/worklist.md): Output sources for external processing
  - [import-facts](enrich/import-facts.md): Import processor output
  - [Writing Processors](enrich/processors.md): Build custom extractors
- [Querying](query/index.md): Explore your indexed files
  - [ls](query/ls.md): List sources matching filters
  - [facts](query/facts.md): Discover available metadata
  - [compare](query/compare.md): Compare directories by content
  - [survey](query/survey.md): Survey a selection for archive status and related locations
  - [sweep](query/sweep.md): Rank reduction opportunities across all roots
  - [trail](query/trail.md): Read the decision trail
- [Managing Sources](manage/index.md): Control which sources are processed
  - [exclude](manage/exclude.md): Mark sources to skip during archiving
  - [note](manage/note.md): Annotate locations with timestamped notes
- [Archiving](archive/index.md): Organize files into your canonical archive
  - [coverage](archive/coverage.md): Check archive progress
  - [cluster](archive/cluster.md): Generate a manifest for archiving
  - [apply](archive/apply.md): Execute the manifest to copy/move files
- [Maintenance](maintain/index.md): Clean up and maintain the database
  - [facts delete](maintain/facts-delete.md): Remove incorrect or unwanted metadata
  - [prune](maintain/prune.md): Clean up stale, orphaned, or excluded data
  - [ledger reindex](maintain/ledger-reindex.md): Rebuild the extraction ledger from receipts
