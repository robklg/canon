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

# Include sources from archive roots
canon cluster generate --where 'source.ext=jpg' --dest /Volumes/Archive --include-archived

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

**Manifest structure:**

The generated manifest includes helpful comments listing all available pattern variables, modifiers, and aliases based on the facts present in your sources:

```toml
# Available facts for pattern (100% coverage on 1234 sources):
#
# Built-in:
#   filename           text   - Filename (last path component)
#   source.ext         text   - File extension
#   source.mtime       time   - Modification time
#   ...
#
# Content facts:
#   content.Make       text
#   content.Model      text
#   ...
#
# Modifiers:
#   Time: |year |month |day |date ...
#   String: |stem |ext |lowercase ...

[output]
pattern = "{filename}"           # ← Edit this to customize organization
base_dir = "/Volumes/Archive"
archive_root_id = 2
```

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

## Refreshing the Lock File

Use `canon cluster refresh` to update the lock file if sources have changed since the manifest was generated:

```bash
# Re-query and update the lock file
canon cluster refresh manifest.toml
```

This re-runs the manifest's query and updates `manifest.lock` with the current matching sources. The manifest settings remain unchanged.