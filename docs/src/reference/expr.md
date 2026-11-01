# Pattern Expressions

The `pattern` field of a [cluster manifest](../commands/archive/cluster.md): where a file ends up in the archive, built from facts with `{expr}`.

## Basic Syntax

Patterns consist of literal path segments and expressions in curly braces:

```
{content.DateTimeOriginal|year}/{content.DateTimeOriginal|month}/{filename}
```

This produces paths like: `2024/07/IMG_001.jpg`

## Fact Keys

Any fact key can be used in a pattern. Key names follow [the same rules as in `--where`](../concepts/facts.md#reading-a-keys-name), except that a name with a space needs no quotes: `{content.Camera Model}`. An alias cannot be used in a pattern.

## `{scope.rel_path}`

The source's path relative to its scope.

The path starts at a base directory, which depends on the scopes the manifest records in the source's root:

- **One directory**: that directory. Scope `photos`, source `photos/2024/a.jpg`: `2024/a.jpg`.
- **Several scopes**: the deepest directory they share, so each scope's name stays in the path. Scopes `photos` and `videos`, same source: `photos/2024/a.jpg`.
- **One file**: the directory the file is in, so only the filename remains. Scope `photos/2024/a.jpg`: `a.jpg`.

Each root has its own base.

[`cluster generate`](../commands/archive/cluster.md) and `cluster refresh` work out the value and record it in the lock file, and [`apply`](../commands/archive/apply.md) uses what was recorded. With no scope recorded it has no value, and there is no fallback destination.

## Modifiers

Transform values using the `|` syntax. See [Facts](facts.md#modifiers) for the complete list.

```
{source.mtime|year}           → 2024
{source.mtime|yearmonth}      → 2024-07
{content.hash.sha256|short}   → a1b2c3d4
{source.ext|uppercase}        → JPG
```

A time with no modifier:

```
{content.DateTimeOriginal}    → 2024-06-15 14:30:05
{content.DateTimeOriginal}    → 2024        (a time known only to the year)
```

A pattern uses the [time zone](../commands/archive/cluster.md#the-manifests-time-zone) recorded in the lock.

A time with a time of day, bare or through `|datetime` or `|time`, writes `:` into the directory or file name. exFAT, FAT32 and NTFS volumes and SMB shares do not allow `:` in a name; for an archive on such storage, use `|date`, `|yearmonth` or `|year`.

### Chaining

```
{filename|stem|lowercase}             → img_001
{source.mtime|weekday|lowercase}      → monday
```

A text modifier after another modifier works on the value as Canon shows it: `|weekday` shows `Monday`, so `|weekday|lowercase` gives `monday`.

## Path Accessors

[Path accessors](facts.md#path-accessors) work the same way in a pattern and in a filter. With `source.rel_path = "photos/2024/vacation/IMG_001.jpg"`:

```
{source.rel_path[-1]}         → IMG_001.jpg
{source.rel_path[1:-1]}       → 2024/vacation
{source.rel_path[-1]|stem}    → IMG_001
```

## Built-in Keys

[Built-in keys](builtins.md#in-a-pattern) work the same way in a pattern and in a filter:

```
{hash_short}_{filename}       → a1b2c3d4_IMG_001.jpg
{hash|short}                  → a1b2c3d4
{ext|lowercase}               → jpg
```

## Where a Value Ends Up

What a `{…}` makes is decided by the type of its value:

- **A path** (`{source.rel_path}`, `{scope.rel_path}`, an accessor `[a:b]`) makes one directory per segment.
- **A path with no segments** makes no directory, and the `/` joining it to the rest goes with it: `{source.rel_path[:-1]}/{filename}` puts a file at the top of its root directly in the destination.
- **Any other value** makes exactly one directory or file name, in its printed form.

## Common Patterns

```toml
# Preserve structure below the scopes declared in the manifest
pattern = "{scope.rel_path}"

# Preserve structure below each source's root
pattern = "{source.rel_path}"

# Flat (all files in one directory)
pattern = "{filename}"

# By EXIF capture date
pattern = "{content.DateTimeOriginal|year}/{content.DateTimeOriginal|month}/{filename}"

# By date with hash prefix (collision-safe)
pattern = "{content.DateTimeOriginal|date}/{hash_short}_{filename}"

# By camera
pattern = "{content.Make}/{content.Model}/{filename}"

# By file type and year
pattern = "{source.ext|lowercase}/{source.mtime|year}/{filename}"
```
