# canon exclude

Manage source [exclusions](../../concepts/exclusion.md). Excluded sources are skipped by most commands.

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

When excluding more than one source, a confirmation prompt shows the count, number of duplicate groups, and skip statistics before proceeding. Use `--yes` to skip the prompt.

## canon exclude set-object / clear-object

Exclude content by **hash** rather than by path. Object-level exclusion is universal: it affects *every* source sharing that content, in source roots and archive roots alike. Use it when content is unwanted wherever it turns up: corrupted files, known junk, the same clip scattered across storage.

```bash
# Exclude by a file's content (looks up its hash)
canon exclude set-object /path/to/junk.bin --yes

# Exclude by content across a scope, with filters
canon exclude set-object /scope --where 'content.mime=application/octet-stream' --yes

# Exclude by hash directly — the only way to exclude empty files
canon exclude set-object --hash <content-hash> --yes

# Restore a content-level exclusion (hash as shown by `exclude list-objects`)
canon exclude clear-object <content-hash>

# List content-level exclusions
canon exclude list-objects
```

`exclude set-object` defaults to a dry-run; pass `--yes` to execute. See [Objects](../../concepts/object.md) for how content-level exclusion differs from path-level source exclusion.

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

## Provenance

Every exclusion is recorded as a [decision](../../concepts/decisions.md). When an archive root is configured, a receipt listing the affected sources lands flat in the archive ledger root's `.canon-ledger/`. `exclude set`, `clear`, `duplicates`, and `set-object` accept `--reason` to annotate why; the global `--no-receipt` flag skips the receipt file for one invocation. See [Receipts](../../concepts/decisions.md#receipts) for placement and per-item detail.
