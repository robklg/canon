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

**Pre-flight checks** (mandatory):

1. **Blocked destination directories** - If a file stands where a destination directory has to go, apply refuses the whole run before transferring anything, naming the file and the destinations it blocks. This check also runs with `--resume`: a file in the way is not evidence of an earlier run's progress. Move or rename the file, or edit the pattern.

2. **Destination collisions** - If multiple sources would map to the same destination path (e.g., using `{filename}` when sources have duplicate names), apply aborts with an error showing which files conflict.

3. **Destination path conflicts** - In regular mode (without `--resume`), checks if any destination paths are already occupied (registered in the database or existing on disk). If conflicts are found, apply suggests using `--resume` to skip already-copied files.

4. **Stale destination records** - If the database shows files as present in the archive but they're missing from disk, apply aborts. Run `canon scan <archive>` to update the database before retrying.

5. **Archive conflicts** - Checks if files already exist in the destination archive or other archives. Empty files are exempt: they are [contentless](../../concepts/object.md#empty-files-are-contentless), so an empty file being applied never conflicts with empty files already in the archive.

6. **Excluded sources** - Blocks if any sources in the manifest are marked as excluded.

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
