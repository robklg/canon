# Scan

Scan directories and index files.

When you scan a particular [root](../../concepts/roots.md), Canon will walk the directory tree starting at the given path(s).
For each file, basic metadata such as last modification time and size is collected, and (by default) the hash is computed.
After scanning, Canon knows about the existence of all sources in that root.
If the files were hashed they will be linked to objects.

The hashing process can take quite long, so it is possible to skip that (`--no-hash`).
Not hashing is an option if your intention is to hash selectively, for instance: you're only interested in certain types of files.

There is no real limit on how many roots you can add.
It may be helpful to scan collections of files that belong together as separate roots.
Each root can be given a comment, so this can help you recall what is contained, but you can also use this to store some notes about what you discovered in these roots.

If you have an already organized location that you want Canon to treat as your canonical archive, scan it with `--role archive` from the start. The role is set when the root is added; to change it, you must remove the root and re-add it with the new role.
You can add multiple archive roots, for instance one for your music collection and another for your eBooks.

## When to run scan

If your filesystem changes regularly, make sure to re-scan your roots with Canon.
That way Canon can detect change, and you will not miss files for archiving.
Note that, when archiving, Canon always checks the validity of the files to be archived.

Another use case is periodic integrity verification of your archives. Use `--verify` to recompute hashes for all files and detect corruption. Canon exits with a non-zero status if any mismatches are found, making it suitable for cron jobs that alert on failure.

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

**Hash computation:** By default, Canon computes content hashes for new and changed files during scan. This enables deduplication and archive tracking. Use `--no-hash` to skip hashing if you just want to index files quickly.

**Integrity verification:** Use `--verify` to recompute hashes for all files, even unchanged ones. Run periodically (e.g., via cron) to detect file corruption. If a file's hash changes without its mtime changing, Canon warns about possible corruption and exits with an error.

**Discovering untracked directories:** Use `--candidates` to find directories with files that aren't yet under any root. This is useful when exploring a drive or backup to see what could be added:

```bash
# Find candidate roots to add under a path
canon scan --candidates /Volumes/Backup

# Output shows directories with untracked files
Candidate roots to add:
  /Volumes/Backup/photos  (3 directories with files)
  /Volumes/Backup/imports  (1 directory with files)
```

Directories under existing roots are skipped. When multiple subdirectories share a common ancestor that could be added as a single root, they're rolled up (unless that ancestor contains an existing root).

**Marking deleted paths as missing:** When you delete a folder that was under a scanned root, Canon still thinks those files are present. Normally you'd re-scan the parent to let Canon discover they're gone, but that can be expensive if the parent contains many other files. Use `--missing` to tell Canon directly that a path no longer exists:

```bash
# Deleted a backup folder — mark its 140 sources as not present
canon scan --missing /Volumes/share/Backup/old-phone

# Works with any path under a known root, including the root itself
canon scan --missing /Volumes/share/Backup
```

The sources are marked as not present but remain in the database with their hashes and metadata intact. If the path reappears later (e.g., storage remounted), a normal scan will reconcile them back. Cannot be combined with `--all` or `--add`.

Output shows what was found:
```
Scanned 1234 files: 100 new, 5 updated, 2 moved, 1127 unchanged, 0 missing
Hashed 105 files
```