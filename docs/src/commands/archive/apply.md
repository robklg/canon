# canon apply

Apply a manifest to copy/move files. Copied files are automatically registered in the database with the same content hash, so they're immediately recognized as archived (no separate `scan` needed).

```bash
# Preview what would happen (fast - skips source existence checks)
canon apply manifest.toml --dry-run

# Copy files (default mode, preserves mtime/permissions on Unix)
canon apply manifest.toml

# Show per-file progress during transfer
canon apply manifest.toml --verbose

# Rename files instead of copying (Unix only, fails on cross-device)
canon apply manifest.toml --rename

# Move files: rename if same device, copy+delete if cross-device
canon apply manifest.toml --move --yes

# Only apply sources from specific roots
canon apply manifest.toml --root id:1 --root id:2
canon apply manifest.toml --root path:/path/to/source

# Allow duplicates across archives (but not within destination)
canon apply manifest.toml --allow-cross-archive-duplicates
```

**Transfer modes:**

| Flag | Behavior |
|------|----------|
| (default) | Copy + preserve mtime/permissions (Unix) |
| `--rename` | Atomic rename; fails if cross-device (Unix only) |
| `--move` | Try rename; fallback to copy+delete on cross-device (Unix only, requires `--yes`) |

All modes use noclobber semantics: if a destination file exists, apply aborts with an error.

**Integrity validation:**

During transfer, Canon validates each source file's partial hash (first 8KB + last 8KB) to detect file corruption or modification since the manifest was generated. If validation fails, the transfer is aborted.

**Root filtering:**

Use `--root` to apply only a subset of sources from the manifest. Useful for staged application when sources are on different drives.

- `--root id:N` - Filter by root ID (shown in manifest as `root_id`)
- `--root path:/path` - Filter by root path (must match exactly)

**Pre-flight checks** (mandatory):

1. **Destination collisions** - If multiple sources would map to the same destination path (e.g., using `{filename}` when sources have duplicate names), apply aborts with an error showing which files conflict.

2. **Archive conflicts** - Checks if files already exist in the destination archive or other archives.

3. **Excluded sources** - Blocks if any sources in the manifest are marked as excluded.

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
