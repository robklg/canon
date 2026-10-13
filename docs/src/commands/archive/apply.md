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

**The lock's time zone:**

Apply uses the time zone the lock records for every time, never this machine's: the zone the
manifest recorded when [`cluster refresh`](cluster.md) or `cluster generate` built the lock (see
[The manifest's time zone](cluster.md#the-manifests-time-zone)). `--dry-run` names it, and so does
the confirmation summary when the pattern uses a time that depends on the time zone (a time
with an offset, or a file's save time):

```
Pattern: {source.mtime|year}/{filename}
Time zone: Europe/Amsterdam (from the lock)
```

Two machines put a file in the same folder when their time zone data agree for that zone. `apply
--dry-run` on the machine that applies shows the destinations the run will use.

A lock built before lock files recorded the time zone is rejected with an error before apply plans, and no
decision row is written:

```
Error: This lock file was built before lock files recorded the time zone.
Nothing was moved. Rebuild it: canon cluster refresh manifest.toml
```

A lock naming a zone this machine's time zone data does not know is rejected the same way, with
three ways out:

```
Error: This machine's time zone data does not know Europe/Kyiv, the zone the lock was built in. Nothing was moved.
  update this machine's time zone data, or
  apply on the machine that built the lock, or
  change the manifest's zone line to a name this machine knows for the same zone (for example, an older spelling), then canon cluster refresh manifest.toml
```

**No lock file beside the manifest:**

Apply carries out a lock file, so a manifest without one stops with an error before apply plans. There are two
ways to arrive here and they take opposite remedies, told apart by the `lock_hash` the manifest
records. A lock file that has gone while the manifest still names its hash is rebuilt:

```
Error: No lock file beside this manifest: manifest.lock
Nothing was moved. Rebuild it: canon cluster refresh manifest.toml
```

A refresh whose query matched nothing removes the lock and empties `lock_hash`. Refreshing again
would match nothing again, so the way forward is a different selection:

```
Error: No lock file beside this manifest: manifest.lock
Nothing was moved. The last refresh matched nothing, so there is no lock to carry across.
Edit the query or scope in the manifest, then canon cluster refresh manifest.toml to select again.
```

A lock file that exists but cannot be read — empty, or with a line that does not parse — is a
different answer and says so. [`cluster status`](cluster.md) states either absence and continues,
since its job is to say what state the manifest is in; it rejects an unreadable lock, as
apply does.

**A lock file built before lock files recorded the scopes they were built from:**

Apply carries out a lock file. The scopes a lock was built from are recorded in it when the
manifest is generated or refreshed, with each entry's path measured from them; apply uses
those and never `meta.scope`. A lock written by an earlier version of Canon carries neither,
so apply stops with an error before it plans and transfers nothing:

```
Error: This lock file was built before lock files recorded the scopes they were built from.
Nothing was moved. Rebuild it: canon cluster refresh manifest.toml
```

The error does not depend on whether the pattern uses `{scope.rel_path}`: the decision
record names the scope either way. No decision row is written. Run
[`cluster refresh`](cluster.md) and apply again.

**Editing the manifest after the lock was written:**

- Editing `output.pattern` takes effect on the next apply.
- Editing `meta.zone` takes effect on the next [`cluster refresh`](cluster.md). Apply uses the
  lock's time zone.
- Editing `meta.scope` takes effect on the next [`cluster refresh`](cluster.md), like the
  filters beside it. Apply takes the scope from the lock, so an edited `meta.scope` changes
  neither where files land nor what the decision record names until the lock is rebuilt.

A scope path that no longer names a known root, or that names a place Canon has no sources
for, is stated by `cluster generate` and `cluster refresh` when they build the lock, and
does not reach apply. See [`cluster`](cluster.md).

**A pattern that cannot expand:**

`{scope.rel_path}` has no value over a lock built while the manifest recorded no scope: one
generated with `--global`, or with filters and no path. Apply reports the failure per source,
says what the pattern needs, and transfers nothing:

```
Error: 2 sources failed pattern expansion:
  /Volumes/Photos/2016/IMG_0001.jpg: scope.rel_path is not available: the selected sources were not scoped
  /Volumes/Photos/2016/IMG_0002.jpg: scope.rel_path is not available: the selected sources were not scoped

The pattern places files relative to the scopes declared in the manifest, and this lock records none.
Use {source.rel_path} to keep each file's own folders, or add paths to meta.scope and refresh.
Error: Aborting due to pattern expansion failures
```

Apply prints this whatever `meta.scope` says now, because it uses the lock and not the scope.
While the manifest records no scope, a refresh does not help — there is nothing to measure from
— and `cluster status` ends with the same two lines. Once a path with sources is in `meta.scope`,
the refresh is what measures the lock, and `cluster status` names it instead.

A pattern using a file's save time that Canon cannot read, one at the edge of the years
0000–9999 or beyond ([Save times](../../concepts/facts.md#save-times)), stops with an error for that cause:

```
Error: 1 sources failed pattern expansion:
  /photos/2016/IMG_0003.jpg: Canon cannot read this file's save time: it lies at the edge of the years 0000–9999 or beyond

A save time Canon cannot read gives the pattern no value.
Add "source.mtime?" to the manifest's query to leave such files out, or correct the file's save time and scan again; then refresh.
Error: Aborting due to pattern expansion failures
```

A pattern naming a fact some sources do not carry ends with the fact pair instead, `Adjust the
pattern in your manifest, or import the facts it names.`; a run that meets both prints both pairs,
unless the pattern also needs a scope the lock lacks, where the scope pair prints alone. A modifier that asks for a finer part than a time carries gets a pair naming the key and modifier that failed, as the query entry that leaves such sources out:

```
Error: 1 sources failed pattern expansion:
  /photos/2016/IMG_0004.jpg: content.DateTimeOriginal|month has no value: the time is known only to the year

A time known only to a coarser grain than the pattern uses gives the pattern no value.
Use a coarser modifier in the pattern, which takes effect at the next apply; or add "content.DateTimeOriginal|month?" to the manifest's query to leave such files out, then refresh.
Error: Aborting due to pattern expansion failures
```

A query edit and a corrected save time each change what the lock holds, so each takes effect once `canon cluster refresh` rebuilds it. A pattern edit takes effect at the next apply.

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

The two passes before the transfer loop stay separate: an unreadable source is rejected
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

**A suspended root stops the run first**, at either end and before the summary and the
confirmation, so `--dry-run` and `--yes` stop there too and nothing is
written:

The origin end, counting the sources this run would have carried from there:

```
/path/to/photos suspended — rejected: origin of 12 sources · canon roots unsuspend path:/path/to/photos
```

Or the destination:

```
/path/to/archive suspended — rejected: destination · canon roots unsuspend path:/path/to/archive
```

One or the other, never both: the run stops at the first door it meets, and it
looks at the destination first. Exit code 1, no `Error:` prefix, and no decision
row.
[`canon roots unsuspend`](../roots/roots.md) is one way back. At the origin end
there is a second: [`cluster refresh`](cluster.md) rebuilds the lock from the open
world, leaving the suspended root's files out, and the apply then carries what is
left. At the destination end no rebuild helps — the files are going there either
way.

**Pre-flight checks** (mandatory). These run once the lock file has been read: a lock
built before lock files recorded the scopes they were built from, or the time zone, is rejected
before any of them.

1. **Blocked destination directories** - If a file stands where a destination directory has to go, apply rejects the whole run before transferring anything, naming the file and the destinations it blocks. This check also runs with `--resume`: a file in the way is not evidence of an earlier run's progress. Move or rename the file, or edit the pattern.

2. **Destination collisions** - If multiple sources would map to the same destination path (e.g., using `{filename}` when sources have duplicate names), apply aborts with an error showing which files conflict.

3. **Destination path conflicts** - In regular mode (without `--resume`), checks if any destination paths are already occupied (registered in the database or existing on disk). If conflicts are found, apply suggests using `--resume` to skip already-copied files.

4. **Stale destination records** - If the database shows files as present in the archive but they're missing from disk, apply aborts. Run `canon scan <archive>` to update the database before retrying.

5. **Archive conflicts** - Checks if files already exist in the destination archive or other archives. Empty files are exempt: they are [contentless](../../concepts/object.md#empty-files-are-contentless), so an empty file being applied never conflicts with empty files already in the archive.

6. **Excluded sources** - Blocks if any sources in the manifest are marked as excluded.

7. **Stale sources** - If a source changed since the manifest was generated, apply stops with an error before transferring anything and names what changed. When the error names the stale files in full, it hands them back as the command to re-observe just those:

   ```
   Error: 2 sources have changed since manifest was generated:
     /Volumes/Photos/2024/img_0042.jpg: size: 4211 → 4230, mtime (Europe/Amsterdam): 2026-08-23 12:58:35 → 2026-08-23 12:58:53
     /Volumes/Photos/2024/img_0043.jpg: mtime (Europe/Amsterdam): 2026-08-23 12:58:35 → 2026-08-23 12:58:53

   Re-observe just these files, then refresh:
     canon scan /Volumes/Photos/2024/img_0042.jpg /Volumes/Photos/2024/img_0043.jpg
     canon cluster refresh trip.toml

   If more than these has changed, run `canon scan` then `cluster refresh` to regenerate the lock file.
   Error: Aborting due to stale sources in manifest
   ```

   Save times print in the lock's time zone, named on the line. Where the two print the same, in the repeated hour when clocks go back, the line adds the difference, `(+3,600 s)`, and a save time Canon cannot read prints `(unreadable)`.

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
