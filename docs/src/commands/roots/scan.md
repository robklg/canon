# Scan

Scan directories and index files.

When you scan a [root](../../concepts/roots.md), Canon walks the directory tree starting at the given path(s).
For each file it collects basic metadata, such as last modification time and size, and by default computes the content hash.
After scanning, Canon knows about the existence of all [sources](../../concepts/source.md) in that root; hashed sources are linked to [objects](../../concepts/object.md).

Collections of files that belong together can be scanned as separate roots. Each root can be given a comment, to recall what it contains or to note what you discovered there.

To have Canon treat an already organized location as your canonical archive, scan it with `--role archive` from the start. The role is set when the root is added; to change it, remove the root and re-add it with the new role.
You can add multiple archive roots, for instance one for a music collection and another for eBooks.

## When to run scan

Re-scan a root after its contents change, so Canon detects the changes and no files are missed for archiving. When archiving, Canon always checks the validity of the files to be archived.

Scan also serves periodic integrity verification of your archives: `--verify` recomputes hashes to detect corruption, and Canon exits with a non-zero status if any mismatches are found, making it suitable for cron jobs that alert on failure.

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

# Scan just a subtree within an existing root
canon scan /path/to/photos/2024

# Scan without computing hashes (just index files)
canon scan --no-hash /path/to/photos

# Verify archive integrity by recomputing all hashes (good for cron jobs)
canon scan --verify /Volumes/Archive

# Mark sources under a deleted folder as not present
canon scan --missing /path/to/deleted/folder
```

**Hash computation:** By default, Canon computes content hashes for new and changed files during scan; hashes enable deduplication and archive tracking. Hashing can take long. Use `--no-hash` to index files without hashing, either for speed or when you intend to hash only certain kinds of files. Sources left without a hash are reported at the end of the scan and hashed by the next scan that hashes (see [Hash debt](#hash-debt)).

**Integrity verification:** `--verify` recomputes hashes for all files, even unchanged ones. If a file's hash changes without its mtime changing, Canon warns about possible corruption and exits with an error.

**Discovering untracked directories:** Use `--candidates` to find directories with files that aren't yet under any root, for instance when exploring a drive or backup to see what could be added:

```bash
# Find candidate roots to add under a path
canon scan --candidates /Volumes/Backup
```

```
Candidate roots to add:
  /Volumes/Backup/photos  (3 directories with files)
  /Volumes/Backup/imports  (1 directory with files)
```

Directories under existing roots are skipped. When multiple subdirectories share a common ancestor that could be added as a single root, they're rolled up (unless that ancestor contains an existing root).

**Marking deleted paths as missing:** When you delete a folder that was under a scanned root, Canon still considers those files present. Re-scanning the parent would let Canon discover they're gone, but that can be expensive when the parent holds many other files. Use `--missing` to tell Canon directly that a path no longer exists:

```bash
# Deleted a backup folder: mark its 140 sources as not present
canon scan --missing /Volumes/Backup/old-phone

# Works with any path under a known root, including the root itself
canon scan --missing /Volumes/Backup
```

The sources are marked as not present but remain in the database with their hashes and metadata intact. If the path reappears later (e.g., storage remounted), a normal scan will reconcile them back. Cannot be combined with `--all` or `--add`.

The decision is recorded against the folder it was aimed at, so [`canon trail`](../query/trail.md) shows it at that location rather than as a global decision.

**Deletions are recorded.** Whether Canon infers a deletion by re-scanning a parent (files that were present but weren't seen this time) or you mark one directly with `--missing`, the disappearance is captured as [decision provenance](../../concepts/decisions.md): each vanished source is linked to the scan decision, and a **source-local receipt** listing exactly what was lost is written to `.canon-ledger/` on the affected storage. Add `--reason` to say why; the reason travels into both the record and the receipt:

```bash
canon scan --missing /Volumes/Backup/old-phone \
  --reason "Phone backed up to archive, originals confirmed"
```

Deletion is a recorded fate alongside archiving and exclusion: what the storage held, what you kept, released, or discarded, and why, stays reconstructible from the files alone. A deletion is recorded even when no archive root exists. To suppress the receipt for one run use `--no-receipt`; to disable recording entirely set `recording = "Off"` (see [Decision Provenance](../../concepts/decisions.md)).

**Absolute paths need no current directory.** A shell whose working directory has been deleted (a root retired or unmounted in another window, for example) can still run `canon scan /some/absolute/path`. A relative path does need one, and says so:

```
Error: cannot resolve relative path './photos': the current directory is unavailable
```

Output shows what was found:
```
Scanned 1234 files: 100 new, 5 updated, 2 moved, 1127 unchanged, 0 missing
Hashed 105 files
```

**Reading the counts:** `new` counts paths the index has not held before. `updated` counts files whose content changed at a path already indexed, whichever way the application saved them: written in place, or written to a temporary file and renamed over the path. A file whose content is recreated exactly as it was, by a restore or a deduplication pass, is neither new nor updated: it counts as unchanged, and the scan records where the file now sits.

`moved` counts files found at a new path that Canon can tie to a path it already knew. A move is reported only when the content matches and the old path is confirmed gone. When either test fails, the file counts as `new` and the old path counts as `missing`: two accurate records rather than one guess. Rescanning storage that was remounted, or whose filesystem hands out fresh internal identifiers each session, reports nothing at all.

**Hardlink companions.** A file can occupy several paths at once through hardlinks. Each path is its own source, sharing content with the others. A path that appears alongside an already-indexed file counts as `new`, and the summary states how many of the new paths are companions:

```
Scanned 31892 files: 27753 new (27751 hardlink companions of already-indexed files), 0 updated, 0 moved, 4139 unchanged, 0 missing
```

The first scan after upgrading reports this once, for every companion path in the library, and the counts can be large. The scan is otherwise ordinary: it can be interrupted and re-run, and the next scan reports nothing.

**Unverified moves.** Checking whether a file moved means checking whether its old path is gone from the storage that recorded it. Two things make that check impossible: the root holding the old path cannot be read at all, or its directory is readable but its storage is not currently mounted, so everything under it would read as gone whether it is or not. Either way the summary says so rather than assuming an answer:

```
Scanned 12 files: 3 new, 0 updated, 0 moved, 9 unchanged, 0 missing, 2 possible moves could not be verified
```

The files count as `new`, and stay that way: the old path keeps its own source until the root holding it is scanned again, which reports it missing. Canon does not join the two records afterwards.

The same line appears for one scan after a root is remounted, because the remount renumbers the storage and the recorded identifiers have not caught up. Scanning that root refreshes them.

**Skipped entries.** Only regular files become sources. Symlinks are skipped, and never followed. Named pipes, sockets and devices are skipped too, counted separately as `special files`. Both counts reach the summary, so a path visible on disk and absent from the index is accounted for:

```
Scanned 1043 files: 1043 new, 0 updated, 0 moved, 0 unchanged, 0 missing, skipped 214 symlinks
```

The counts say what the walk saw, so they repeat on every scan of the same directory, where `new` and `moved` say what changed.

Some network clients, SMB shares in particular, present a symlink to the operating system as an ordinary file. Canon indexes what the operating system presents, so such a link becomes a source and is not counted here. Its content is the target's content, so both paths resolve to the same object.

### Hash debt

A source with no content hash is invisible to everything that reads content: coverage, duplicate detection, and cluster selection all pass over it. Canon states how many sources a scan leaves in that state:

```
Scanned 4820 files: 4820 new, 0 updated, 0 moved, 0 unchanged, 0 missing
4820 sources remain unhashed
```

The count covers the paths this scan walked, and appears after any scan that leaves sources unhashed. When some of that debt is content the scan tried to read and could not, the line says how much:

```
Scanned 3 files: 0 new, 0 updated, 0 moved, 3 unchanged, 0 missing
Hashed 2 files (2 from backlog)
1 sources remain unhashed (1 could not be read)
```

The qualifier counts what is still in debt when the scan ends, so it is always part of the number in front of it, and it covers only files that hold no hash at all. Two neighbouring cases are reported elsewhere: a file that cannot be read during the walk never becomes a source, and appears as `skipped (read errors)` on the first line; and a file that `--verify` cannot re-read keeps the hash it already had, so it is not in debt and is reported only as a warning.

Files that could not be read do not change the exit status: the non-zero exit is reserved for hash mismatches, which say something about the content rather than about access to it.

The next scan that hashes reads them, whatever else it finds: a file Canon has never read is hashed even when nothing about it changed. The summary separates that backlog from work this scan caused, so a large pay-down is readable:

```
Scanned 4820 files: 0 new, 0 updated, 0 moved, 4820 unchanged, 0 missing
Hashed 4820 files (4820 from backlog)
```

Clearing a root indexed with `--no-hash` can take as long as hashing it the first time would have. The pass can be interrupted: what remains unhashed is reported again, and the next scan continues from there. A file that could not be read this time is warned about individually, counted in the line above, and stays in debt until a later scan reads it.

`--verify` re-reads every file regardless, so it clears debt as a side effect and reports no backlog count.

### Keeping continuity across a move

Canon follows files that move within or between roots, provided it sees the destination:

```bash
# Index where the files are now
canon scan /Volumes/Photos

# Reorganize on disk
mv /Volumes/Photos/inbox/trip /Volumes/Photos/2024/trip

# Scan again: the sources keep their history at the new paths
canon scan /Volumes/Photos
```

Scanning a subtree is enough, as long as the destination is inside it: Canon checks the old path directly rather than needing to have walked it. Moving files to a different root works the same way, and only the destination root needs scanning for the move to be recognized.

Two cases are not followed. Edit the files and move them in the same step, and Canon reports `new` plus `missing` instead: nothing ties the two paths together once both the location and the content have changed. And a move out of a [suspended](roots.md) root is not followed, because a suspended root's contents keep the standing they had; unsuspend it and scan again.

In both cases the records stay: the new path is indexed, the old path is reported missing when its root is scanned, and the old path's deletion receipt names the decision that preceded it.
