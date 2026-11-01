# Scan

Index directories or single files into a [root](../../concepts/roots.md): a directory is walked, and a named file is scanned on its own. By default new and changed files are hashed.

A root's role is set when it is added; to change it, remove the root and re-add it with the new role.

## When to run scan

Re-scan a root after its contents change.

`--verify` recomputes hashes for all files, even unchanged ones, and exits non-zero if a file's hash changed without its mtime changing, so it suits cron jobs that alert on failure.

## Examples

```bash
# Add a new root and scan it (--add and --role required for new roots)
canon scan --add --role source /path/to/photos

# Scan multiple new roots
canon scan --add --role source /path/to/photos /path/to/more/photos

# Add with a descriptive comment
canon scan --add --role source --comment "Photos from 2020 trip" /path/to/photos

# Add as an archive root (for tracking already-organized files)
canon scan --add --role archive /path/to/archive

# Re-scan an existing root (--role optional, validated against existing)
canon scan /path/to/photos

# Re-scan all roots, or all roots of one role
canon scan --all
canon scan --all --role archive

# Scan just a subtree within an existing root
canon scan /path/to/photos/2024

# Scan a single file within an existing root
canon scan /path/to/photos/2024/img_0042.jpg

# Scan without computing hashes (just index files)
canon scan --no-hash /path/to/photos

# Verify archive integrity by recomputing all hashes (good for cron jobs)
canon scan --verify /Volumes/Archive

# Disable mount protection, when device IDs change between scans (e.g., NAS remounts)
canon scan --ignore-device-id /Volumes/NAS

# Mark sources under a deleted directory as not present
canon scan --missing /path/to/deleted/directory
```

**Hash computation:** `--no-hash` indexes files without hashing, for speed or to hash only certain kinds of files.

**Discovering untracked directories:** `--candidates` finds directories with files that aren't yet under any root:

```bash
# Find candidate roots to add under a path
canon scan --candidates /Volumes/Backup
```

```
Candidate roots to add:
  /Volumes/Backup/photos  (3 directories with files)
  /Volumes/Backup/imports  (1 directory with files)
```

**Single files:** Nothing else under the root is looked at. A named file that is gone is skipped with a warning, never recorded as deleted.

## Reading the summary

```
Scanned 1234 files: 100 new, 5 updated, 2 moved, 1127 unchanged, 0 missing
Hashed 105 files
```

| Clause | Meaning |
|---|---|
| `new` | Paths not in the index before |
| `updated` | Files whose content changed at a path already indexed |
| `moved` | Files found at a new path, with matching content and the old path confirmed gone. Otherwise the file counts as `new` and the old path as `missing` |
| `missing` | Indexed paths gone from disk |
| `27753 new (27751 hardlink companions of already-indexed files)` | Each hardlink path is its own source |
| `2 possible moves could not be verified` | The old path could not be checked. The files count as `new` |
| `3 skipped (read errors)` | Files that could not be read, see [Deletions](#deletions) |
| `skipped 214 symlinks`, `skipped 3 special files` | Only regular files become sources. Symlinks are never followed |

### Hash debt

Coverage, duplicate detection, and cluster selection skip sources with no content hash.

```
Scanned 4820 files: 4820 new, 0 updated, 0 moved, 0 unchanged, 0 missing
4820 sources remain unhashed
```

The next scan that hashes reads them, and counts them as backlog:

```
Scanned 4820 files: 0 new, 0 updated, 0 moved, 4820 unchanged, 0 missing
Hashed 4820 files (4820 from backlog)
```

Hashing can be interrupted: the next scan continues from there.

## Moves

Canon follows files that move within or between roots, provided it sees the destination:

```bash
# Index where the files are now
canon scan /Volumes/Photos

# Reorganize on disk
mv /Volumes/Photos/inbox/trip /Volumes/Photos/2024/trip

# Scan again: the sources keep their history at the new paths
canon scan /Volumes/Photos
```

Scanning the destination is enough, also for a move between roots.

A file edited and moved in the same step is reported as `new` plus `missing`.

## Deletions

Use `--missing` to record that a path is gone without re-scanning its parent:

```bash
# Deleted a backup directory: mark its 140 sources as not present
canon scan --missing /Volumes/Backup/old-phone

# Works with any path under a known root, including the root itself
canon scan --missing /Volumes/Backup
```

The sources are marked as not present but remain in the database with their hashes and metadata intact. If the path reappears later, a scan marks them present again.

**A scan never concludes a file was deleted just because it could not read it.** The source keeps its record, and the summary counts it as `skipped (read errors)`.

A deletion, inferred by a scan or marked with `--missing`, is recorded as a [decision](../../concepts/decisions.md) with a source-local receipt, even when no archive root exists:

```bash
canon scan --missing /Volumes/Backup/old-phone \
  --reason "Phone backed up to archive, originals confirmed"
```
