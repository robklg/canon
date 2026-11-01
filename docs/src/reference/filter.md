# Filter Syntax

The language of `--where`: boolean expressions over facts that select sources. Several `--where` flags combine with AND.

## Operators

### Basic

| Syntax | Meaning |
|--------|---------|
| `key?` | Fact has a value; for a key with modifiers, the modified value (`content.DateTimeOriginal\|month?` is false for a time known only to a year) |
| `key=value` | Fact equals value (case-sensitive); for a time, the value falls within the time you typed (`2024-06` is all of June 2024) |
| `key!=value` | Everything `key=value` does not select ([no value](#no-value) included) |
| `key~glob` | Glob match on text and paths (case-sensitive); see [Globs](#globs) |
| `key!~glob` | Everything `key~glob` does not select ([no value](#no-value) included) |
| `key>value` | Greater than; for a time, starts at or after the end of the time you typed |
| `key>=value` | Greater or equal; for a time, starts at or after the start of the time you typed |
| `key<value` | Less than; for a time, ends by the start of the time you typed |
| `key<=value` | Less or equal; for a time, ends by the end of the time you typed |
| `key IN (v1, v2, ...)` | Fact matches any value in list |
| `key NOT IN (v1, v2, ...)` | Everything `key IN (...)` does not select ([no value](#no-value) included) |

Times compare as spans of time in your local time (see [Times](#times)), and text in character order, so `B` sorts before `a` and `10` before `9`.

### Globs

After `~` and `!~` the value is a glob:

| Glob | Meaning |
|------|---------|
| `*` | Any run of characters, `/` included, or none |
| `?` | One visible character |
| `[abc]` | One character from the set |
| `[a-z]` | One character in the range |
| `[!abc]` or `[^abc]` | One character not in the set |
| `"…"` or `'…'` | The quoted text itself, joined to the text beside it |

A space outside quotes ends a glob, and a set cannot hold one. A glob with no wildcard or set matches the whole value.

```bash
# Files starting with IMG_
--where 'filename~IMG_*'

# Files in a year subdirectory
--where 'source.rel_path~*/2024/*'

# Names holding [2024]
--where 'filename~*"[2024]"*'

# A directory name with a space
--where 'source.rel_path~*/"My Pictures"/*'
```

Without quotes a glob holds what a plain value holds (see [Quoting](#quoting)) and `* ? [ ]`. Any other character needs quotes around its part, with `* ? [ ]` left outside them:

A quote mark inside a glob starts quoted text. To match an apostrophe, quote it with the other mark: `filename~*"'"*`.

## Boolean Operators

| Syntax | Meaning |
|--------|---------|
| `expr AND expr` | Both conditions must match |
| `expr OR expr` | Either condition matches |
| `NOT expr` | Negates the condition |
| `(expr)` | Grouping for precedence |

Operator precedence (highest to lowest): NOT, AND, OR.

`AND`, `OR`, `NOT`, `IN` and `NOT IN` are written in capitals.

## Quoting

A value can be written without quotes when it holds only letters and digits, of any language, and `_ . - / : +`: `filename=照片1.jpg`, `source.size>-5`, `source.mtime>=2024-06-15`. Any other character needs quotes, a space included: `content.Model="EOS 5D"`.

A key name needs quotes for different characters, depending on where it is written:

- **In a filter**: anything but letters, digits and `_ . : / -`. `content.XMP-dc:Subject` needs none; `"content.File Size"` needs them.
- **In `--key` and `--group-by`**: `|` and `[`, and a `,` in `--group-by`. `content.File Size` needs none; `"content.a|b"` needs them.
- **In a pattern's `{…}`**: `|`, `[` and `}`. `{content.Camera Model}` needs none; `{'content.a|b'}` needs them.

Outside a filter, a name that does not start with a letter or `_` needs quotes too: `"5 Stars"`.

A value or a key that needs quotes is written in double or single quotes, and that mark inside is doubled.

In a shell, wrap the whole filter in single quotes and use double quotes inside it:

```bash
--where 'content.Title="say ""hi"""'      # the title: say "hi"
--where 'content.Title="it'\''s"'          # the title: it's
--where '"content.File Size">1000000'    # a key with a space
--where '"content.File Size"|bucket(1e6)=1000000-and-up'
```

## Values and Types

Every key has a [type](../concepts/facts.md#value-types). A value you type is interpreted as the key's type before anything runs.

| Key type | A value you type is interpreted as |
|----------|------------------------------|
| Number | A number: `1024`, `-5`, `3.14`, `1e6`. Quoted or bare, the same number |
| Time | A year, a month, a day, a minute, a second or a point within a second, in your local time. See [Times](#times) |
| Text | Text, exactly as typed |
| Path | Text, compared with the path |

**Text and path keys**: a bare value that looks like a number, a time or a time of day (`2019`, `2024-06-15`, `18:00`) gives an error. Quoted, it is text. To ask a number or a date question of text, say so with a modifier: `content.numtext|number>=10`, `content.textdate|year=2024`. Anything else is text without quotes: `IMG_0001.jpg`, `2024-06-15_beach.jpg`.

**Numbers** are compared exactly: `content.Rating=0.3` selects a stored `0.3` and not a value that only rounds to it.

**Text** is compared exactly as stored: an accented letter stored as one character and the same letter stored as a letter plus an accent mark are two different texts. A path typed as an argument is found in either form.

**A key of mixed type**, whose facts hold more than one type, cannot be compared or modified.

## Times

A time covers a span in [your local time](../concepts/facts.md#how-canon-shows-a-time), as wide as its precision: `2024` is the whole of that year, `2024-06` that month, `"2024-06-15 14:30"` that minute.

A comparison selects a stored time when its whole span satisfies the operator. With `2024-06` as the time you type, a stored time is:

- **Inside that span**: `=` selects it. `2024-06-15` is `=2024-06`.
- **Before or after it**: `<` or `>` selects it. `2024-05-31` is `<2024-06`, and `2024-07-01` is `>2024-06`.
- **Coarser than it**: `!=` and `NOT` select it, like a source with no value. `2024` is in none of `=2024-06`, `<2024-06` and `>2024-06`.

| You type | Selects the same as |
|----------|---------------------|
| `source.mtime=2024` | `source.mtime\|year=2024` |
| `source.mtime=2024-06` | `source.mtime\|yearmonth=2024-06` |
| `source.mtime=2024-06-15` | `source.mtime\|date=2024-06-15` |

**A time with an offset** is [translated to your local time](../concepts/facts.md#how-canon-interprets-a-time).

A time, typed or imported, is written like this:

- A date is `YYYY-MM-DD`, or `YYYY:MM:DD` as EXIF writes it; a month is `YYYY-MM`; a year is `YYYY`.
- A time of day follows the date after `T` or one space, as `HH:MM` or `HH:MM:SS`, with up to nine digits of a fraction of a second (`14:30:05.45`).
- An offset follows the time directly or after one space, as `Z` or `±HH:MM`.

Anything else is not a time.

## No Value

`!=`, `!~`, `NOT IN` and `NOT` all select sources with no value. To leave them out, ask for a value as well:

```bash
--where 'content.DateTimeOriginal!=2024 AND content.DateTimeOriginal?'
```

## What an Acting Command States

A command that acts on a `--where` selection (`exclude set`, `exclude clear`, `exclude duplicates`, `exclude set-object`, `cluster generate`, `cluster refresh`, `facts delete`) states this under the number of sources it acts on:

```
Would exclude 2,140 sources:
  of which 312 have no Make
  12 sources have a time that spans 2024-06 (coarser than a month): 9 not selected, 3 selected
```

The lines show under `--yes` too, and stay in the decision's summary, which [`canon trail show`](../commands/query/trail.md) shows.

## Status Predicates

Status predicates check computed state.

| Predicate | True when |
|-----------|-----------|
| `archived?` | Content exists in at least one archive root |
| `hashed?` | Content hash has been computed |
| `excluded?` | Source or object is excluded |
| `enriched?` | Has any stored metadata beyond the content hash |

Status predicates are written in lower case and take only `?`.

```bash
# What still needs archiving?
canon facts --key mime --where 'NOT archived?'

# Survey unresolved content
canon survey --where 'NOT archived?'

# Unidentified files
canon worklist --where 'NOT hashed?'

# Combine status predicates with fact filters
canon ls --where 'NOT archived? AND hashed? AND mime~image/*'

# View excluded sources (requires --include)
canon ls --include excluded --where 'excluded?'
```

`NOT archived?` includes unhashed sources; `NOT archived? AND hashed?` leaves them out.

## Aliases

An alias is a named expression in `$CANON_HOME/aliases.toml` (by default `~/.canon/aliases.toml`), which you use as `@name`. An alias replaces a whole condition or a key: in `--where`, and in `facts --key` and `--group-by`.

### Filter Aliases

The value is one whole filter:

```toml
image = "source.ext IN (jpg, png, heic)"
recent = "source.mtime|year >= 2020"
large = "source.size > 10000000"
```

A filter alias combines with `AND`, `OR`, `NOT` and parentheses, and nothing else may follow it.

### Key Aliases

The value is one key, followed by nothing else:

```toml
filename = "source.rel_path[-1]"
parent = "source.rel_path[-2]"
year = "source.mtime|year"
taken = "content.DateTimeOriginal"
```

A key alias takes the same operators and modifiers as a key, and an accessor only where its value is a key name alone.

### Using Aliases

```bash
# A filter alias on its own
canon ls --where '@image'

# Combine filter aliases
canon ls --where '@image AND @recent'

# A key alias with a glob, a modifier
canon ls --where '@filename~IMG_*'
canon ls --where '@taken|year=2024'

# Negate a filter alias
canon ls --where 'NOT @large'
```

### The Aliases File

A name starts with a letter, then letters, digits, `_` and `-`, in any language.

A manifest's query takes no alias: `cluster generate` writes what each alias stands for into the query.

## Using Modifiers

See [Facts](facts.md#modifiers) for the complete list and what each gives, and [Expressions](expr.md#chaining) for chaining. In a manifest's query a time modifier works in the manifest's [time zone](../commands/archive/cluster.md#the-manifests-time-zone).

```bash
# Files from 2024
--where 'source.mtime|year=2024'

# Case-insensitive extension matching
--where 'source.ext|lowercase=jpg'

# Case-insensitive glob
--where 'filename|lowercase~img_*'

# Common image formats
--where 'source.ext|lowercase IN (jpg, png, gif, webp)'

# iPhone photos
--where 'Make=Apple'

# Large images (combining with parentheses)
--where '(source.ext|lowercase=jpg OR source.ext|lowercase=png) AND source.size>1000000'
```
