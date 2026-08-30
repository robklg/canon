# Pattern Expressions

Pattern expressions define how files are organized in archives. They use `{expr}` syntax to insert dynamic values based on facts.

Patterns are used in the `pattern` field of [cluster manifests](../commands/archive/cluster.md). When you run `canon cluster generate`, it creates a manifest with a default `pattern = "{filename}"` that you can customize.

## Basic Syntax

Patterns consist of literal path segments and expressions in curly braces:

```
{content.DateTimeOriginal|year}/{content.DateTimeOriginal|month}/{filename}
```

This produces paths like: `2024/07/IMG_001.jpg`

## Fact Keys

Any fact key can be used in a pattern:

- `{source.ext}` - File extension
- `{source.mtime}` - Modification time
- `{content.Make}` - Camera manufacturer (from EXIF)
- `{content.hash.sha256}` - Content hash

Patterns need the key's full name. `--where` and `canon facts --key` let you leave the `content.` prefix off, but a pattern does not: `{Make}` fails with `Unknown fact 'Make'`, so write `{content.Make}`.

`{scope.rel_path}` is not a fact: it is the source's path below the *vantage* — the deepest directory containing every scope the manifest records that lies in that source's own root. With one scope the vantage is that scope. With several, it is the directory they share, so each scope's own name survives in the result. Scopes in different roots each get their own vantage. [`cluster generate`](../commands/archive/cluster.md) records the paths it was scoped to; where the manifest records no scope, or none in a source's root, the pattern is refused rather than guessed.

The recorded paths are resolved against the known roots once per run, matching each path against the normalization forms of its root's stored path. A path whose root portion is typed in the other Unicode normalization resolves and measures normally; below the root, forms must match what the index stores. A path that names no known root is never dropped: [`apply`](../commands/archive/apply.md) refuses the run and names it, and [`cluster refresh`](../commands/archive/cluster.md) states it and keeps it.

## Modifiers

Transform values using the `|` syntax. See [Facts](facts.md#modifiers) for the complete list.

```
{source.mtime|year}           → 2024
{source.mtime|yearmonth}      → 2024-07
{content.hash.sha256|short}   → a1b2c3d4
{source.ext|uppercase}        → JPG
```

Multiple modifiers can be chained:

```
{filename|stem|lowercase}     → img_001
```

## Path Accessors

Extract segments from path values using Python-style indexing:

| Syntax | Meaning |
|--------|---------|
| `key[-1]` | Last segment (filename) |
| `key[0]` | First segment |
| `key[1:3]` | Slice segments 1 and 2 |
| `key[:-1]` | All but last segment |

Examples with `source.rel_path = "photos/2024/vacation/IMG_001.jpg"`:

```
{source.rel_path[-1]}         → IMG_001.jpg
{source.rel_path[0]}          → photos
{source.rel_path[1:-1]}       → 2024/vacation
{source.rel_path[-1]|stem}    → IMG_001
```

## Aliases

Aliases provide shorthand for common expressions. Use `canon facts --show-aliases` to see all available aliases.

| Alias | Expands To |
|-------|------------|
| `filename` | `source.rel_path[-1]` |
| `stem` | `source.rel_path[-1]\|stem` |
| `ext` | `source.rel_path[-1]\|ext` |
| `hash` | `content.hash.sha256` |
| `hash_short` | `content.hash.sha256\|short` |
| `id` | `source.id` |

Example using aliases:

```
{hash_short}_{filename}       → a1b2c3d4_IMG_001.jpg
```

## Missing Values

Canon requires all facts used in a pattern to have values for every source. If any source is missing a required fact, `canon apply` refuses to proceed and reports which facts are missing.

When you run `canon cluster generate`, the manifest includes comments listing all facts with 100% coverage. These are safe to use in your pattern.

If sources are missing required facts, you can:
- Filter them out during generation: `--where 'DateTimeOriginal?'`
- Import the missing facts via the [enrichment pipeline](../commands/enrich/index.md)

## Common Patterns

```toml
# Preserve structure below the manifest's scope
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
pattern = "{source.ext}/{source.mtime|year}/{filename}"
```
