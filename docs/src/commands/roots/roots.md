# canon roots

List and manage registered [roots](../../concepts/roots.md).

Roots are added via [`scan`](scan.md) and managed with the `roots` command. You can list, suspend/unsuspend, add comments, or remove roots.

**Important notes:**
- Removing a root also removes its sources and attached facts from the database
- Removing a root does **not** delete any files on disk
- If you re-add a removed root, you'll need to re-enrich it

```bash
# List all roots with file counts and last scan time
canon roots

# List roots at or beneath a specific path
canon roots /path/to/photos

# List only suspended roots
canon roots --suspended

# Set a comment on a root (omit text to clear)
canon roots comment id:1 "Old backup, possibly duplicates"
canon roots comment id:1

# Suspend a root (hides from all operations without deleting data)
canon roots suspend id:1
canon roots suspend path:/path/to/photos

# Unsuspend a root (make visible again)
canon roots unsuspend id:1

# Remove a root by ID (files on disk are NOT deleted)
canon roots rm id:1

# Remove a root by path
canon roots rm path:/path/to/photos

# Skip confirmation prompt
canon roots rm id:1 --yes
```

Example output:
```
ID   ROLE       FILES  LAST SCAN         PATH
1    source     16635  2h ago            /path/to/photos
2    archive   169941  5d ago            /path/to/archive
3    source      1234  never             /path/to/backup (Old backup, possibly duplicates)
```

## Suspending Roots

Suspended roots are hidden from listings, excluded from `scan --all`, and their sources are excluded from all queries (`ls`, `facts`, `coverage`, `worklist`, etc.). Suspended roots still prevent overlapping (you cannot add a new root at a suspended root's path). Use `--suspended` to list only suspended roots.

## Removing Roots

When removing a root, Canon shows how many sources are "in archive" (same content exists in an archive) vs "not in archive", and suggests using `canon ls <path>` to preview which sources will be forgotten.

## Root Specs

Several commands accept root specifications in two formats:

| Format | Example | Description |
|--------|---------|-------------|
| `id:N` | `id:1` | By database ID (shown in `canon roots` output) |
| `path:/...` | `path:/path/to/photos` | By exact path |

```bash
canon roots suspend id:1
canon roots suspend path:/path/to/photos
```
