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

**Hash computation:** By default, Canon computes content hashes for new and changed files during scan; hashes enable deduplication and archive tracking. Hashing can take long. Use `--no-hash` to index files without hashing, either for speed or when you intend to hash only certain kinds of files.

**Integrity verification:** `--verify` recomputes hashes for all files, even unchanged ones. If a file's hash changes without its mtime changing, Canon warns about possible corruption and exits with an error.

**Discovering untracked directories:** Use `--candidates` to find directories with files that aren't yet under any root, for instance when exploring a drive or backup to see what could be added:

```bash
# Find candidate roots to add under a path
canon scan --candidates /Volumes/Backup

# Output shows directories with untracked files
Candidate roots to add:
  /Volumes/Backup/photos  (3 directories with files)
  /Volumes/Backup/imports  (1 directory with files)
```

Directories under existing roots are skipped. When multiple subdirectories share a common ancestor that could be added as a single root, they're rolled up (unless that ancestor contains an existing root).

**Marking deleted paths as missing:** When you delete a folder that was under a scanned root, Canon still considers those files present. Re-scanning the parent would let Canon discover they're gone, but that can be expensive when the parent holds many other files. Use `--missing` to tell Canon directly that a path no longer exists:

```bash
# Deleted a backup folder — mark its 140 sources as not present
canon scan --missing /Volumes/Backup/old-phone

# Works with any path under a known root, including the root itself
canon scan --missing /Volumes/Backup
```

The sources are marked as not present but remain in the database with their hashes and metadata intact. If the path reappears later (e.g., storage remounted), a normal scan will reconcile them back. Cannot be combined with `--all` or `--add`.

**Deletions are recorded.** Whether Canon infers a deletion by re-scanning a parent (files that were present but weren't seen this time) or you mark one directly with `--missing`, the disappearance is captured as [decision provenance](../../concepts/decisions.md): each vanished source is linked to the scan decision, and a **source-local receipt** listing exactly what was lost is written to `.canon-ledger/` on the affected storage. Add `--reason` to say why; the reason travels into both the record and the receipt:

```bash
canon scan --missing /Volumes/Backup/old-phone \
  --reason "Phone backed up to archive, originals confirmed"
```

Deletion is a recorded fate alongside archiving and exclusion: what the storage held, what you kept, released, or discarded, and why, stays reconstructible from the files alone. A deletion is recorded even when no archive root exists. To suppress the receipt for one run use `--no-receipt`; to disable recording entirely set `recording = "Off"` (see [Decision Provenance](../../concepts/decisions.md)).

Output shows what was found:
```
Scanned 1234 files: 100 new, 5 updated, 2 moved, 1127 unchanged, 0 missing
Hashed 105 files
```
