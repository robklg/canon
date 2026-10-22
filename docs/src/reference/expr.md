# Pattern Expressions

Pattern expressions define how files are organized in archives. They use `{expr}` syntax to insert dynamic values based on facts.

Patterns are used in the `pattern` field of [cluster manifests](../commands/archive/cluster.md). When you run `canon cluster generate`, it creates a manifest with a default pattern that you can customize: `{scope.rel_path}` when the generation is scoped, and `{source.rel_path}` when it is not.

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

`{scope.rel_path}` is not a fact: it is the source's path below the *vantage*: the deepest directory containing every scope the manifest records **that Canon could resolve** and that lies in that source's own root. A recorded path Canon could not resolve is stated and left out of the measurement (see below). With one scope naming a folder, the vantage is that folder. With several, it is the directory they share, so each scope's own name survives in the result. A scope naming a single **file** measures from the folder that file sits in, so it renders as its own filename — which is what makes a shell wildcard mean the same thing whether it matched one file or a dozen. Scopes in different roots each get their own vantage.

The value is measured when the selection is made, by [`cluster generate`](../commands/archive/cluster.md) and `cluster refresh`, and recorded in the lock file beside the source it belongs to. [`apply`](../commands/archive/apply.md) uses what was recorded. Editing `meta.scope` therefore takes effect on the next refresh; editing the pattern takes effect on the next apply.

The recorded paths are resolved against the known roots once per run, tolerantly of Unicode normalization in both halves: the root is matched against the normalization forms of its stored path, and the part below the root against the forms the index holds sources under. A path retyped in the other form resolves and measures normally, and a refresh repairs it in the manifest text.

A path Canon cannot resolve, under no known root or with no known sources under it in any form, is never dropped and never silently obeyed. [`cluster refresh`](../commands/archive/cluster.md) names it, keeps it in the file, and leaves it out of the measurement, so the paths that did resolve measure from themselves.

`{scope.rel_path}` is unanswerable in two cases, and they take different answers:

- **The selected sources were not scoped** — the lock was built with `--global`, or with filters and no path, so there was nothing to measure from. Declare a scope in `meta.scope` and refresh, and the lock is measured from it; or use `{source.rel_path}`.
- **The lock file was built before lock files recorded the scopes they were built from.** Apply rejects it outright, whatever the pattern says. Rebuild the lock with `cluster refresh`.

Neither error names a remedy in its own text: the message says which of the two cases you are in, and what to do about it depends on the manifest's wider state, which this message cannot see. The remedy comes from the command you ran. Where no rebuild would help, the remedy is one pair of sentences that both `apply` and `cluster status` print: use `{source.rel_path}` or scope the manifest — never check fact coverage, since `{scope.rel_path}` is not a fact and no import would supply it. Which act `cluster status` names, and when it names the rebuild instead of the remedy, is decided from the manifest's whole state and described on the [`cluster`](../commands/archive/cluster.md) page; `apply` checks the lock's header before the pattern and resolves no scope of its own, so a lock written before the measurement gets *rebuild the lock* from `apply`, and a refresh `apply` names can still be turned away on a scope question — which the refresh answers itself, naming what to do.

There is no fallback destination in either case.

## Modifiers

Transform values using the `|` syntax. See [Facts](facts.md#modifiers) for the complete list.

```
{source.mtime|year}           → 2024
{source.mtime|yearmonth}      → 2024-07
{content.hash.sha256|short}   → a1b2c3d4
{source.ext|uppercase}        → JPG
```

### Chaining

Multiple modifiers can be chained:

```
{filename|stem|lowercase}             → img_001
{source.mtime|weekday|lowercase}      → monday
```

A text modifier after another modifier works on the value as Canon prints it: `|weekday` prints `Monday`, so `|weekday|lowercase` gives `monday`. This holds after `|number` and `|datetime` too, which print the value itself. A text modifier directly on a number or a time key gives an error: say which printed form you mean, such as `source.mtime|datetime|lowercase` or `source.mtime|weekday|lowercase`.

A time with no modifier writes the time as Canon prints it, at its precision:

```
{content.DateTimeOriginal}    → 2024-06-15 14:30:05
{content.DateTimeOriginal}    → 2024        (a time known only to the year)
```

A pattern uses the manifest's [time zone](../commands/archive/cluster.md#the-manifests-time-zone), as the lock records it: a file saved at 23:30 UTC on 31 December 2023 has `{source.mtime|year}` `2024` in a manifest recording `Europe/Amsterdam` and `2023` in one recording `America/New_York`, on any machine. A time with no offset, as a camera writes it, is used as written.

A time with a time of day, bare or through `|datetime` or `|time`, writes `:` into the folder or file name. exFAT, FAT32 and NTFS volumes and SMB shares do not allow `:` in a name; for an archive on such storage, use `|date`, `|yearmonth` or `|year`.

## Path Accessors

Extract segments from path values using Python-style indexing:

| Syntax | Meaning |
|--------|---------|
| `key[-1]` | Last segment (filename) |
| `key[0]` | First segment |
| `key[1:3]` | Slice segments 1 and 2 |
| `key[:-1]` | All but last segment |

An index past the end has no value. A slice always has one, its bounds cut to the segments there are: past the end it is a path with no segments. A slice of an absolute path that starts at the first segment keeps the leading `/` (`source.path[:3]` of `/photos/2024/trip/a.jpg` → `/photos/2024/trip`).

Examples with `source.rel_path = "photos/2024/vacation/IMG_001.jpg"`:

```
{source.rel_path[-1]}         → IMG_001.jpg
{source.rel_path[0]}          → photos
{source.rel_path[1:-1]}       → 2024/vacation
{source.rel_path[-1]|stem}    → IMG_001
```

## Aliases

Aliases provide shorthand for common expressions in patterns.

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

Every fact a pattern uses needs a value for every source. If any source has none, `canon apply` stops with an error and names the cause for each source; see [`apply`](../commands/archive/apply.md) for the remedy it prints.

When you run `canon cluster generate`, the manifest's comments list the facts with 100% coverage: every selected source has a value for them.

A pattern has no value for a source in four cases:

- **The source does not carry the fact.** Filter such sources out during generation, `--where 'DateTimeOriginal?'`, or import the missing facts via the [enrichment pipeline](../commands/enrich/index.md).
- **A modifier asks for a finer part than the time carries**, such as `{content.DateTimeOriginal|month}` on a time known only to `2024`. The reason given is `content.DateTimeOriginal|month has no value: the time is known only to the year`. `--where 'content.DateTimeOriginal|month?'` leaves such sources out, and `--where 'NOT content.DateTimeOriginal|month?'` lists them. In a manifest, use a coarser modifier in the pattern, which takes effect at the next apply, or add `content.DateTimeOriginal|month?` to the query and refresh. A second manifest can take those sources into [year folders](../commands/archive/cluster.md).
- **Text the modifier cannot use**: `{content.Title|number}` on `Canon`, or a time modifier on text that is not a time. The reason given is `content.Title|number has no value: the text is not a number`, naming the expression as the pattern specifies it. `--where 'content.Title|number?'` and `--where 'content.Title|datetime?'` keep only the sources where the text is a number or a time; add one to the manifest's query and refresh. An index that finds no segment, such as `[5]` on a path with three, has no value the same way; so has `{source.ext}` or `|ext` for a name with no extension (`source.ext has no value`, `filename|ext has no value`), and plain `|bucket` for a number below zero (`content.Offset|bucket has no value: |bucket groups numbers from 0 up`).
- **The file's save time is one Canon cannot read**, at the edge of the years 0000–9999 or beyond ([Save times](../concepts/facts.md#save-times)). This is not a missing fact, and no import supplies it. The reason given is `Canon cannot read this file's save time: it lies at the edge of the years 0000–9999 or beyond`. `--where 'NOT source.mtime?'` finds such files; add `source.mtime?` to the manifest's query to leave them out, or correct the file's save time and scan again; either takes effect at the next `canon cluster refresh`.

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
