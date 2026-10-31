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

A key in a pattern is read as in `--where` and `canon facts --key`: a name that is not a built-in key and does not start with `source.` or `content.` is read under `content.`, so `{Make}` is `{content.Make}`. A name with a space needs no quotes: `{content.Camera Model}`. A name holding `|`, `[` or `}` is quoted, with `'…'` inside a pattern, which the manifest's `pattern = "…"` line holds as it is: `{'content.a|b'}`. A pattern cannot contain an alias: `{@taken}` is refused.

`{scope.rel_path}` is not a fact: it is the source's path below the *vantage*: the deepest directory containing every scope the manifest records **that Canon could resolve** and that lies in that source's own root. A recorded path Canon could not resolve is stated and left out of the measurement (see below). With one scope naming a folder, the vantage is that folder. With several, it is the directory they share, so each scope's own name survives in the result. A scope naming a single **file** measures from the folder that file sits in, so it renders as its own filename — which is what makes a shell wildcard mean the same thing whether it matched one file or a dozen. Scopes in different roots each get their own vantage.

The value is measured when the selection is made, by [`cluster generate`](../commands/archive/cluster.md) and `cluster refresh`, and recorded in the lock file beside the source it belongs to. [`apply`](../commands/archive/apply.md) uses what was recorded. Editing `meta.scope` therefore takes effect on the next refresh; editing the pattern takes effect on the next apply.

The recorded paths are resolved against the known roots once per run, tolerantly of Unicode normalization in both halves: the root is matched against the normalization forms of its stored path, and the part below the root against the forms the index holds sources under. A path retyped in the other form resolves and measures normally, and a refresh repairs it in the manifest text.

A path Canon cannot resolve, under no known root or with no known sources under it in any form, is never dropped and never silently obeyed. [`cluster refresh`](../commands/archive/cluster.md) names it, keeps it in the file, and leaves it out of the measurement, so the paths that did resolve measure from themselves.

`{scope.rel_path}` is unanswerable in two cases, and they take different answers:

- **The selected sources were not scoped** — the lock was built with `--global`, or with filters and no path, so there was nothing to measure from. Declare a scope in `meta.scope` and refresh, and the lock is measured from it; or use `{source.rel_path}`.
- **The lock file was built before lock files recorded the scopes they were built from.** Apply rejects it outright, whatever the pattern says. Rebuild the lock with `cluster refresh`.

Neither error names a remedy in its own text: the message says which of the two cases you are in, and what to do about it depends on the manifest's wider state, which this message cannot see. The remedy comes from the command you ran. Where no rebuild would help, the remedy is one line that both `apply` and `cluster status` print: use `{source.rel_path}` or scope the manifest — never check fact coverage, since `{scope.rel_path}` is not a fact and no import would supply it. Which act `cluster status` names, and when it names the rebuild instead of the remedy, is decided from the manifest's whole state and described on the [`cluster`](../commands/archive/cluster.md) page; `apply` checks the lock's header before the pattern and resolves no scope of its own, so a lock written before the measurement gets *rebuild the lock* from `apply`, and a refresh `apply` names can still be turned away on a scope question — which the refresh answers itself, naming what to do.

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

## Built-in Keys

A pattern reads the [built-in keys](builtins.md) as a filter does, with accessors and modifiers like any other key:

```
{hash_short}_{filename}       → a1b2c3d4_IMG_001.jpg
{hash|short}                  → a1b2c3d4
{ext|lowercase}               → jpg
```

`{hash}`, `{hash_short}` and `{content.hash.sha256}` read the content hash the lock file recorded for the source.

## Where a Value Lands

What a `{…}` makes is decided by the type of its value:

- **A path** (`{source.rel_path}`, `{scope.rel_path}`, an accessor `[a:b]`) makes one directory per segment. A leading `/` makes no directory: `{source.path}` places `photos/2024/a.jpg` for `/photos/2024/a.jpg`.
- **A path with no segments** makes no directory, and the `/` joining it to the rest goes with it where keeping it would leave `//` or a leading `/`: `{source.rel_path[:-1]}/{filename}` places a file at the top of its root directly in the destination, and `x/{source.rel_path[:-1]}y/{filename}` places it in `x/y`.
- **Any other value** makes exactly one directory or file name, in its printed form. `..` inside a name stays as it is: `photo..jpg` is copied to `photo..jpg`.

A destination is read as the operating system reads it, so a `.` name is no directory: `a/./b` is `a/b`.

## Checking a Pattern

[`apply`](../commands/archive/apply.md) reads the pattern before anything is planned and refuses it if any `{…}` or its literal text cannot be used, with every problem in one run, under `Error: cannot use the pattern as specified`:

```
Error: cannot use the pattern as specified
  {Rating}/{source.size|year}//x/
   ^ `Rating` is an unknown key
    `canon facts` lists the keys
  {Rating}/{source.size|year}//x/
                       ^ |year works on times or text; source.size is of type number
    Modifiers for numbers: |bucket, |number
  {Rating}/{source.size|year}//x/
                             ^ an empty name between `//` cannot be a directory name
  {Rating}/{source.size|year}//x/
                                ^ a pattern cannot end with `/`
```

A key is checked against the type of all its facts, as in a filter: a key with no facts is an unknown key, a modifier its value cannot take is refused, and a key of mixed type is refused with `a pattern supports one type only`. The literal text cannot start or end with `/`, hold `//`, or have `.` or `..` as a whole name, and a `{` or `}` must be paired.

[`cluster status`](../commands/archive/cluster.md#cluster-status) states the same problems and exits 0. `cluster generate` and `cluster refresh` do not read the pattern.

## Sources With No Place

A source has no place where a `{…}` gives it no value it can place. `canon apply` stops with every cause of every `{…}` for each source and a line saying what to do about each cause; see [`apply`](../commands/archive/apply.md). When you run `canon cluster generate`, the manifest's comments list the keys every selected source has a value for.

A source has no place in these cases:

- **The source does not carry the fact**: `{content.Make} has no value`. Filter such sources out during generation, `--where 'Make?'`, or import the missing facts via the [enrichment pipeline](../commands/enrich/index.md).
- **A modifier asks for a finer part than the time carries**, such as `{content.DateTimeOriginal|month}` on a time known only to `2024`: `{content.DateTimeOriginal|month} has no value: the time is known only to the year`. `--where 'content.DateTimeOriginal|month?'` leaves such sources out, and `--where 'NOT content.DateTimeOriginal|month?'` lists them. In a manifest, use a coarser modifier in the pattern, which takes effect at the next apply, or add `content.DateTimeOriginal|month?` to the query and refresh. A second manifest can take those sources into [year folders](../commands/archive/cluster.md).
- **Text the modifier cannot use**: `{content.Title|number}` on `Canon`, or a time modifier on text that is not a time: `{content.Title|number} has no value: the text is not a number`, naming the expression as the pattern specifies it. `--where 'content.Title|number?'` and `--where 'content.Title|datetime?'` keep only the sources where the text is a number or a time; add one to the manifest's query and refresh. An index that finds no segment, such as `[5]` on a path with three, has no value the same way; so has `{source.ext}` or `|ext` for a name with no extension (`{source.ext} has no value`, `{filename|ext} has no value`), and plain `|bucket` for a number below zero (`{content.Offset|bucket} has no value: |bucket groups numbers from 0 up`).
- **The file's save time is one Canon cannot read**, at the edge of the years 0000–9999 or beyond ([Save times](../concepts/facts.md#save-times)). This is not a missing fact, and no import supplies it. The reason given is `Canon cannot read this file's save time: it lies at the edge of the years 0000–9999 or beyond`. `--where 'NOT source.mtime?'` finds such files; add `source.mtime?` to the manifest's query to leave them out, or correct the file's save time and scan again; either takes effect at the next `canon cluster refresh`.
- **A value that cannot be a name**: text holding `/` (``{content.Make} is `AC/DC`: a file or directory name cannot contain `/` ``), empty text (`{content.Make} is empty: a file or directory name cannot be empty`), a value that is `.` or `..` (``{content.Make} is `..`: a file or directory name cannot be `..` ``), or one holding a null character (`{content.Make} contains a null character: a file or directory name cannot contain one`).
- **A path with no segments leaves no file name**: it stands in the destination's last name, that name is left empty or `.`, and no other `{…}` in the name has a cause of its own, such as `a/{source.rel_path[1:]}` for a file at the top of its root: `{source.rel_path[1:]} has no segments: a file name cannot be empty`. With text beside it that names the file, as in `a/{filename}{source.rel_path[1:]}`, it leaves nothing and the file is placed.

A finished destination holding a `..` directory name or a null character, which literal text around a path with no segments can make (`{source.rel_path[:-1]}../{filename}` for a file at the top of its root gives `../README`), is refused at `apply`'s preflight.

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
