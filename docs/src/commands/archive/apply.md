# canon apply

Copies or moves files into the archive as a manifest specifies, and registers them, so no `scan` is needed.

```bash
# Preview what would happen
canon apply manifest.toml --dry-run

# Copy files (default mode, preserves mtime/permissions on Unix)
canon apply manifest.toml

# Show per-file progress during transfer
canon apply manifest.toml --verbose

# Rename files instead of copying
canon apply manifest.toml --rename

# Move files
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

No mode overwrites: if a destination file exists, apply aborts with an error.

A `--move` to another volume confirms each copy before it removes the origin, so it takes longer per file on a network volume.

For `--rename` and `--move`, the confirmation summary shows which source roots will lose files:

```
Mode: rename (sources will be relocated)
Files: 150
Sources from:
  /Volumes/Drive1  (100 files)
  /Volumes/Drive2  (50 files)
```

The [manifest's field table](cluster.md#the-manifest) says which edits apply reads.

**Resume mode (`--resume`):**

Continues an interrupted apply, checking what is at each destination:
- **Recorded**: left alone
- **Right size, not recorded**: compared with the lock's hash. A match is recorded; a difference stops the run
- **Wrong size**: stops the run
- **Absent**: transferred

```bash
# Resume an interrupted apply
canon apply manifest.toml --resume

# Preview what --resume would do
canon apply manifest.toml --resume --dry-run
```

**Stopping a run (Ctrl-C):**

The first Ctrl-C stops the run after the current file, and `--resume` finishes the work. The second ends the process at once, and that run writes no receipt.

**Root filtering:**

`--root` applies only the manifest's sources on the roots you name ([root specs](../roots/roots.md#root-specs)).

`apply` checks every source and destination before it transfers anything, and stops with nothing moved if a check fails. `--dry-run` does not see a file changed since the last scan; a real run does.

## Provenance

Every apply is recorded as a [decision](../../concepts/decisions.md), with a [receipt](../../concepts/decisions.md#receipts) under the archive root's `.canon-ledger/`. Without `--reason`, the manifest's `# === Notes ===` section is the reason.
