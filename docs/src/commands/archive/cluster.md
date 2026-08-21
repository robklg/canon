# canon cluster generate

Generate a manifest of files matching filters. The `--dest` flag specifies where files will be copied and must be inside a registered archive root.

```bash
# All photos to an archive (unhashed sources are automatically skipped)
canon cluster generate --where 'source.ext IN (jpg, png, heic)' --dest /Volumes/Archive/Photos

# Destination can be a subdirectory within an archive
canon cluster generate --where 'source.ext IN (jpg, png, heic)' --dest /Volumes/Archive/Photos/2024

# Scope to a specific path
canon cluster generate /path/to/photos --dest /Volumes/Archive

# Custom output file
canon cluster generate --where 'source.ext=jpg' --dest /Volumes/Archive -o my-manifest.toml

# Allow sources from archive roots
canon cluster generate --where 'source.ext=jpg' --dest /Volumes/Archive --allow archived

# Allow duplicate content (same hash already in an archive)
canon cluster generate --where 'source.ext=jpg' --dest /Volumes/Archive --allow duplicates

# Show which files were excluded (already archived)
canon cluster generate --where 'source.ext=jpg' --dest /Volumes/Archive --show-archived

# Overwrite existing manifest file
canon cluster generate --where 'source.ext=jpg' --dest /Volumes/Archive --force
```

The command generates two files: a manifest (`.toml`) that you edit, and a lock file (`.lock`) containing the source list.

**Typical workflow:**

```bash
canon cluster generate --where 'source.ext IN (jpg, png, heic)' --dest /Volumes/Archive
# Edit manifest.toml to customize the output pattern
canon apply manifest.toml --dry-run   # Preview
canon apply manifest.toml             # Execute
```

**Output:**

After generating, the command prints a summary showing root breakdown and archive coverage:

```
Generated manifest: manifest.toml (1,234 sources in manifest.lock)
  From 2 roots:
    /Volumes/Drive1  (800)
    /Volumes/Drive2  (434)
  1,234 have no archived copy
```

**Empty files are never skipped as "already archived".** A zero-byte file is
[contentless](../../concepts/object.md#empty-files-are-contentless), so
archive detection ignores it and the manifest carries it with its folder.

**Manifest structure:**

The generated manifest includes a cluster summary, a notes section for your own annotations, and comments listing available pattern variables:

```toml
# === Cluster Summary ===
# 1,234 sources from 2 roots:
#   /Volumes/Drive1  (800)
#   /Volumes/Drive2  (434)
# 1,234 have no archived copy

# === Notes ===
#

[meta]
version = 1
query = ["source.ext IN ('jpg', 'png', 'heic')"]
scope = "/path/to/photos"
generated_at = "2026-02-28T12:00:00Z"
lock_hash = "abc123..."

[options]
allow = []                       # e.g. ["archived", "duplicates"]

[output]
pattern = "{filename}"           # ← Edit this to customize organization
base_dir = "/Volumes/Archive"
archive_root_id = 2

# Available facts for pattern (100% coverage on 1234 sources):
# ...
```

- **Cluster Summary** is regenerated on each `cluster refresh`, showing current source counts, root breakdown, and archive coverage.
- **Notes** section is preserved across refreshes — add your own comments here.
- **`version`** field tracks the manifest format version.
- **`[options]`** records which `--allow` flags were used during generation. [`cluster refresh`](cluster.md) reads them, because it re-selects sources from the same query. [`apply`](apply.md) reads only `duplicates`, which speaks to the content it is about to transfer; `archived` acknowledged a selection that has already happened, and apply selects nothing. What apply needs acknowledged, it asks for on its own flags.

**Common output patterns:**

```toml
# Flat (default) - all files in base_dir
pattern = "{filename}"

# Preserve original folder structure (relocate as-is)
pattern = "{source.rel_path}"

# By EXIF date
pattern = "{content.DateTimeOriginal|year}/{content.DateTimeOriginal|month}/{filename}"

# By EXIF date with hash prefix (avoids collisions)
pattern = "{content.DateTimeOriginal|year}/{content.DateTimeOriginal|month}/{hash_short}_{filename}"

# By camera model
pattern = "{content.Make}/{content.Model}/{filename}"

# By file type
pattern = "{source.ext}/{filename}"
```

See [Pattern Expressions](../../reference/expr.md) for the full syntax reference, including modifiers, path accessors, and aliases.

## Refreshing the Lock File

Use `canon cluster refresh` to update the lock file if sources have changed since the manifest was generated:

```bash
# Re-query and update the lock file
canon cluster refresh manifest.toml
```

This re-runs the manifest's query and updates `manifest.lock` with the current matching sources. The manifest settings (`[options]`, `[output]`) remain unchanged.

On refresh:
- The **Cluster Summary** is regenerated with current counts
- The **Notes** section is preserved verbatim
- The same root breakdown and archive coverage summary is printed to stdout
