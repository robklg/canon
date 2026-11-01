# canon exclude

Source [exclusions](../../concepts/exclusion.md): sources most commands skip.

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

# Skip confirmation prompt (for scripting)
canon exclude set --where 'source.ext=bak' --yes

# View excluded sources
canon ls --include excluded --where 'excluded?'

# Remove exclusions
canon exclude clear
canon exclude clear --where 'source.ext=tmp'

# Skip confirmation prompt
canon exclude clear --yes
```

A filter with `!=`, `!~`, `NOT IN` or `NOT` also selects sources with no value; the preview [says how many](../../reference/filter.md#what-an-acting-command-states).

## canon exclude duplicates

Exclude duplicates, keeping the copies in a preferred location.

```bash
# Exclude duplicates, keeping files under /preferred/path
canon exclude duplicates /scope/path --prefer /preferred/path

# Preview what would be excluded
canon exclude duplicates /scope/path --prefer /preferred/path --dry-run

# Skip confirmation prompt
canon exclude duplicates /scope/path --prefer /preferred/path --yes

# With filters
canon exclude duplicates /scope/path --prefer /preferred/path --where 'source.ext=jpg'
```

## canon exclude set-object / clear-object

Exclude content by **hash** rather than by path, affecting every source sharing that content ([two levels](../../concepts/exclusion.md#two-levels)).

```bash
# Exclude by a file's content (looks up its hash)
canon exclude set-object /path/to/junk.bin --yes

# Exclude by content across a scope, with filters
canon exclude set-object /scope --where 'content.mime=application/octet-stream' --yes

# Exclude by hash directly (excluding empty content requires this explicit form)
canon exclude set-object --hash <content-hash> --yes

# Restore a content-level exclusion (the full hash; list-objects prints its first 16 characters)
canon exclude clear-object <content-hash>

# List content-level exclusions
canon exclude list-objects
```

`exclude set-object` defaults to a dry-run; pass `--yes` to execute.

## How exclusions affect other commands

| Command | Default behavior | Override |
|---------|------------------|----------|
| `ls` | Skips excluded | `--include excluded` |
| `worklist` | Skips excluded | `--include excluded` |
| `facts` | Skips excluded, shows count | `--include excluded` |
| `coverage` | Counts non-excluded sources | None |
| `cluster generate` | Always skips excluded | None |
| `apply` | Blocks if manifest has excluded | None |

## Provenance

Every exclusion is recorded as a [decision](../../concepts/decisions.md), with a [receipt](../../concepts/decisions.md#receipts) where an archive root is available. `exclude set`, `clear`, `duplicates`, and `set-object` accept `--reason`.
