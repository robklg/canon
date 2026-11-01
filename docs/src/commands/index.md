# Canon Commands

## Common Options

Most commands that operate on sources share these options:

**Path scope**: Limit a command to a specific directory by passing a path:

```bash
canon ls /path/to/photos
canon facts /path/to/photos
canon coverage /path/to/photos
```

When no paths are given, a command scopes to the current directory if it is inside a known root, and otherwise operates across all roots. Use `--global` to operate across all roots while inside one.

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

**`--allow`**: Commands that change state (`cluster generate`, `apply`, `import-facts`) skip certain sources by default (e.g., sources already in an archive). Use `--allow` to acknowledge you want to include them:

```bash
canon cluster generate --allow archived --dest /archive     # Include sources from archive roots
canon cluster generate --allow duplicates --dest /archive   # Include content already archived elsewhere
canon import-facts --allow archived                         # Import facts for archive sources
```

The available `--allow` values are specific to each command.
