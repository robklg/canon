# canon exclude

Manage source exclusions. Excluded sources are skipped by most commands.

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
canon ls --include excluded --where 'excluded?' /path/to/photos

# Remove exclusions
canon exclude clear
canon exclude clear --where 'source.ext=tmp'

# Preview what would be cleared
canon exclude clear --where 'source.ext=tmp' --dry-run

# Skip confirmation prompt
canon exclude clear --yes
```

When excluding or clearing more than one source, a confirmation prompt shows the count, root spread, and (for `exclude set`) archive coverage before proceeding. Use `--yes` to skip the prompt, or `--dry-run` to preview without executing.

## canon exclude duplicates

Automatically exclude duplicate files while keeping copies in a preferred location.

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

This is useful for deduplicating across backup drives while keeping the "canonical" copy in your preferred location.

When excluding more than one source, a confirmation prompt shows the count, number of duplicate groups, and skip statistics before proceeding. Use `--yes` to skip the prompt.

**How exclusions affect other commands:**

| Command | Default behavior | Override |
|---------|------------------|----------|
| `ls` | Skips excluded | `--include excluded` or `--excluded` filter mode |
| `worklist` | Skips excluded | `--include excluded` |
| `facts` | Skips excluded, shows count | `--include excluded` |
| `coverage` | Stats on included only | `--include excluded` shows excluded dimension |
| `cluster generate` | Always skips excluded | No override (hard gate) |
| `apply` | Blocks if manifest has excluded | No override (hard gate) |

Exclusions are stored directly on sources and objects in the database.
