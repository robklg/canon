# canon apply

Apply a manifest to copy/move files. Copied files are automatically registered in the database with the same content hash, so they're immediately recognized as archived (no separate `scan` needed).

```bash
# Preview what would happen (fast - skips source existence checks)
canon apply manifest.toml --dry-run

# Copy files (default mode, preserves mtime/permissions on Unix)
canon apply manifest.toml

# Show per-file progress during transfer
canon apply manifest.toml --verbose

# Resume a previously interrupted apply
canon apply manifest.toml --resume

# Rename files instead of copying (Unix only, fails on cross-device)
canon apply manifest.toml --rename

# Move files: rename if same device, copy+delete if cross-device
canon apply manifest.toml --move

# Only apply sources from specific roots
canon apply manifest.toml --root id:1 --root id:2
canon apply manifest.toml --root path:/path/to/source

# Allow duplicates within the destination archive
canon apply manifest.toml --allow duplicates

# Allow duplicates across archives (but not within destination)
canon apply manifest.toml --allow cross-archive-duplicates
```

**Transfer modes:**

| Flag | Behavior |
|------|----------|
| (default) | Copy + preserve mtime/permissions (Unix) |
| `--rename` | Atomic rename; fails if cross-device (Unix only) |
| `--move` | Try rename; fallback to copy+delete on cross-device (Unix only) |

All modes use noclobber semantics: if a destination file exists, apply aborts with an error.

For `--rename` and `--move`, the confirmation summary shows which source roots will lose files:

```
Mode: rename (sources will be relocated)
Files: 150
Sources from:
  /Volumes/Drive1  (100 files)
  /Volumes/Drive2  (50 files)
```

**Confirmation summary:**

The summary previews the directory files actually enter, which is the manifest's
`base_dir` plus whatever literal directories the pattern begins with:

```
Destination: /Volumes/Archive
Pattern: 2024/{filename}
...
Destination current contents (/Volumes/Archive/2024):
  (will be created)
```

A pattern whose directories come from content has no single placement directory. The
preview shows the directory they all sit under and says so:

```
Pattern: sorted/{source.rel_path}
...
Destination current contents (/Volumes/Archive/sorted):
  (placements fan out under this directory by pattern)
  2023/
  2024/
```

**A recorded scope that no longer resolves:**

Destinations are measured from the scope the manifest records (see
[`{scope.rel_path}`](../../reference/expr.md)). That scope is text, and it can stop
naming a known root: a path retyped by hand, a folder moved, a root removed. Apply
refuses before it plans, names every path it could not resolve, and transfers nothing:

```
Error: The manifest's scope names 1 path under no known root:
  /Volumes/old-laptop/photos/2016
Destinations are measured from the recorded scope, so nothing was moved.
Edit meta.scope, then `canon cluster refresh manifest.toml` to rewrite the lock.
```

The refusal does not depend on whether the pattern reads `{scope.rel_path}`: the
decision record names the scope either way. No decision row is written.

Edit `meta.scope` to name places that resolve, then refresh.

Resolution is tolerant of Unicode normalization: a scope whose root is typed in the other
form resolves and needs no edit, and the whole path is then read in the form that matched
that root. Where a root's own path matches as typed, the rest of each scope path is taken
as written, so two scope paths in one manifest should agree with each other below the root.

**Progress before anything moves:**

Apply reads every source in the manifest before it transfers the first file: once while
planning, then twice more before the transfer loop. On a network volume each pass is a
round-trip per source, so each one names itself and counts:

```
Running preflight checks...
  100% (1234/1234)
Checking destination write permissions...
Checking 1,234 sources can be read...
  100% (1234/1234)
Verifying 1,234 sources against the lock file (reading file heads)...
  100% (1234/1234)
```

The two passes before the transfer loop stay separate: an unreadable source is refused
before any file's content is read. `--dry-run` returns after planning, so it never runs
them.

**Resume mode (`--resume`):**

Use `--resume` to continue a previously interrupted apply. This is useful when:
- Apply was interrupted (Ctrl+C, system crash, disk full)
- Some files failed to transfer due to errors

Resume mode classifies each destination into one of:
- **Already archived** - Registered in database, skipped
- **Resumed** - File exists on disk but not in database, skipped (needs `scan` to register)
- **To transfer** - Not in database, not on disk, will be copied

```bash
# Resume an interrupted apply
canon apply manifest.toml --resume

# Preview what --resume would do
canon apply manifest.toml --resume --dry-run
```

If `--resume` reports "resumed" files, run `canon scan` on the affected paths to register them:

```bash
# Scan only the destination directory that was being written to
canon scan /path/to/archive/2024
```

If `--resume` detects files with size mismatches (partial copies from interrupted transfers), it will error and ask you to delete those files before continuing.

**Integrity validation:**

During transfer, Canon validates each source file's partial hash (first 8KB + last 8KB) to detect file corruption or modification since the manifest was generated. If validation fails, the transfer is aborted.

**Root filtering:**

Use `--root` to apply only a subset of sources from the manifest. Useful for staged application when sources are on different drives.

- `--root id:N` - Filter by root ID (shown in manifest as `root_id`)
- `--root path:/path` - Filter by root path (must match exactly)

**Pre-flight checks** (mandatory). These run once the manifest's recorded scope has
resolved: a scope naming a path under no known root refuses before any of them.

1. **Blocked destination directories** - If a file stands where a destination directory has to go, apply refuses the whole run before transferring anything, naming the file and the destinations it blocks. This check also runs with `--resume`: a file in the way is not evidence of an earlier run's progress. Move or rename the file, or edit the pattern.

2. **Destination collisions** - If multiple sources would map to the same destination path (e.g., using `{filename}` when sources have duplicate names), apply aborts with an error showing which files conflict.

3. **Destination path conflicts** - In regular mode (without `--resume`), checks if any destination paths are already occupied (registered in the database or existing on disk). If conflicts are found, apply suggests using `--resume` to skip already-copied files.

4. **Stale destination records** - If the database shows files as present in the archive but they're missing from disk, apply aborts. Run `canon scan <archive>` to update the database before retrying.

5. **Archive conflicts** - Checks if files already exist in the destination archive or other archives. Empty files are exempt: they are [contentless](../../concepts/object.md#empty-files-are-contentless), so an empty file being applied never conflicts with empty files already in the archive.

6. **Excluded sources** - Blocks if any sources in the manifest are marked as excluded.

7. **Stale sources** - If a source changed since the manifest was generated, apply refuses before transferring anything and names what changed. When the refusal names the stale files in full, it hands them back as the command to re-observe just those:

   ```
   Error: 2 sources have changed since manifest was generated:
     /Volumes/Photos/2024/img_0042.jpg: size: 4211 → 4230, mtime: 1787482715 → 1787482733
     /Volumes/Photos/2024/img_0043.jpg: mtime: 1787482715 → 1787482733

   Re-observe just these files, then refresh:
     canon scan /Volumes/Photos/2024/img_0042.jpg /Volumes/Photos/2024/img_0043.jpg
     canon cluster refresh trip.toml

   If more than these has changed, run `canon scan` then `cluster refresh` to regenerate the lock file.
   Error: Aborting due to stale sources in manifest
   ```

   Past ten stale files the listing truncates and the whole-root remedy is offered alone. The same two lines answer both staleness conditions: when only the lock is behind, `cluster refresh` does the work and the scan finds nothing to do; when the files changed on disk since the last scan, the scan is what brings the database current for the refresh to read. A stale file that was deleted meanwhile is skipped by `canon scan` with a warning; assert the deletion with [`canon scan --missing`](../roots/scan.md) and refresh, and it leaves the lock.

Edit the manifest's `[output]` section to customize the destination:

```toml
[output]
pattern = "{content.DateTimeOriginal|year}/{content.DateTimeOriginal|month}/{filename}"
base_dir = "/path/to/archive"
```

Pattern variables use fact keys with optional modifiers (see [Pattern Expressions](../../reference/expr.md) for the full syntax):
- `{filename}`, `{stem}`, `{ext}` - Filename aliases
- `{hash}`, `{hash_short}` - Content hash aliases
- `{source.mtime|year}`, `{source.mtime|month}` - File modification date
- `{content.DateTimeOriginal|year}` - EXIF date with modifier
- `{content.Make}`, `{content.Model}` - Any fact key

**Recovering from interrupted apply:**

If apply is interrupted or encounters errors:

1. Fix any reported errors (permissions, disk space, etc.)
2. Delete any partial files in the archive (files with wrong sizes from interrupted copies)
3. Re-run with `--resume`:
   ```bash
   canon apply manifest.toml --resume
   ```

Resume mode's classification and its handling of "resumed" and partial files are described above.

If source files changed during apply, refresh the manifest first:
```bash
canon scan <source-paths>
canon cluster refresh manifest.toml
canon apply manifest.toml
```

## Provenance

Every apply is recorded as a [decision](../../concepts/decisions.md), with a receipt listing every file transferred, written under the archive root's `.canon-ledger/` (placement and per-item contents: [Receipts](../../concepts/decisions.md#receipts)). Add `--reason` to record why; when you don't, the manifest's `# === Notes ===` section becomes the reason automatically. The global `--no-receipt` flag skips the receipt file for one invocation.

An apply is also indexed by what it drew out of each source root: the [extraction ledger](../../concepts/decisions.md#the-extraction-ledger--the-trails-outbound-direction). At a source location afterwards, [`canon trail`](../query/trail.md) shows what was archived out of that place and whether the originals remain; `canon trail show <id>` gives the full per-root breakdown and the receipt's location on disk.
