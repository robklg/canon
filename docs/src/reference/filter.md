# Filter Syntax

Filters select sources based on facts using a boolean expression language. Most commands accept `--where` to filter which sources they operate on. Multiple `--where` flags are combined with AND.

## Operators

### Basic

| Syntax | Meaning |
|--------|---------|
| `key?` | Fact has a value; for a key with modifiers, the modified value (`content.DateTimeOriginal\|month?` is false for a time known only to a year) |
| `key=value` | Fact equals value (case-sensitive); for a time, the value falls within the time you typed (`2024-06` is all of June 2024) |
| `key!=value` | Everything `key=value` does not select, sources with no value included |
| `key~glob` | Glob match on text and paths (case-sensitive); see [Globs](#globs) |
| `key!~glob` | Everything `key~glob` does not select, sources with no value included |
| `key>value` | Greater than; for a time, starts at or after the end of the time you typed |
| `key>=value` | Greater or equal; for a time, starts at or after the start of the time you typed |
| `key<value` | Less than; for a time, ends by the start of the time you typed |
| `key<=value` | Less or equal; for a time, ends by the end of the time you typed |
| `key IN (v1, v2, ...)` | Fact matches any value in list |
| `key NOT IN (v1, v2, ...)` | Everything `key IN (...)` does not select, sources with no value included |

Numbers compare as numbers, times as spans of time in your local time (see [Times](#times)), and text in character order, so `B` sorts before `a` and `10` before `9`.

### Globs

After `~` and `!~` the value is a glob, read as a shell reads it:

| Glob | Meaning |
|------|---------|
| `*` | Any run of characters, `/` included, or none |
| `?` | One visible character |
| `[abc]` | One character from the set |
| `[a-z]` | One character in the range |
| `[!abc]` or `[^abc]` | One character not in the set |
| `"…"` or `'…'` | The quoted text itself, joined to the text beside it |

`?` and a set member count one visible character: an accented letter or an emoji is one however it is stored, where bash counts one code point. Inside a set every character is itself, a quote mark included; a `]` first in the set and a `-` first or last are members. A space ends the glob inside a set too, so a set never holds a space, and a set a space reaches before its `]` is refused (`filename~[abc OR filename~x.jpg]` is an unclosed set). This is unlike bash, where `*[" "_]*` matches a space or an underscore: in Canon a quote mark inside a set is a member, so that set is unclosed too; write `filename~*" "* OR filename~*_*`. A glob with no wildcard or set matches the whole value.

Text in a glob compares exactly as stored. `filename~Cafe*` selects a `Café.jpg` whose `é` is stored as `e` and an accent mark, and not one stored as the single character `é`.

```bash
# Files starting with IMG_
--where 'filename~IMG_*'

# Files with 3-letter extension
--where 'source.ext~???'

# Files in a year subdirectory
--where 'source.rel_path~*/2024/*'

# Exclude temp files
--where 'filename!~*.tmp'

# Names starting with a literal *
--where 'filename~"*"*'

# Names holding [2024], and names holding @2x
--where 'filename~*"[2024]"*'
--where 'filename~*"@"2x*'

# A directory name with a space
--where 'source.rel_path~*/"My Pictures"/*'
```

Without quotes a glob holds what a plain value holds (see [Quoting](#quoting)) and `* ? [ ]`. Any other character needs quotes around its part, with `* ? [ ]` left outside them; the refusal names the first such character in each part:

```
Error: cannot run the filter as given
  filename~Rock&Roll*
               ^ `&` needs quotes; quote that part, but not * ? [ ]
```

A quote mark inside a glob starts quoted text, as in a shell, so two apostrophes in one glob close each other; inside an `=` value a quote mark needs quotes instead. `filename~John's*` is an unclosed quote, and `filename=John's` is shown with the quoted fix. To match an apostrophe, quote it with the other mark: `filename~*"'"*` selects every name holding one, and `filename~*"O'Brien"*` the names holding `O'Brien`.

Inside quotes a doubled mark is a quote mark, so two quoted parts in the same mark cannot touch: write one quoted run (`*"[2024]@2x"*`) or switch marks (`*"[2024]"'@2x'*`).

A set never closed is refused (`this set is unclosed`), and so is a glob on a number or a time:

```
Error: cannot run the filter as given
  source.mtime~2024*
               ^ `~` works on text; source.mtime is of type time
```

## Quoting

A value can be written without quotes when it holds only letters and digits, of any language, accent marks written as their own character, and `_ . - / : +`: `filename=照片1.jpg`, `source.size>-5`, `source.mtime>=2024-06-15`. Any other character needs quotes, a space included: `content.Model="EOS 5D"`. A value ends at the first space, so in `content.Model=EOS 5D` the `5D` is a second value and the filter is rejected. After `=` and `!=`, `*`, `?` and `[` are wildcards, and Canon shows both ways to write them (see [When a Filter Is Rejected](#when-a-filter-is-rejected)).

A key name can be written without quotes when it holds only letters, digits and `_ . : / -`: `content.XMP-dc:Subject`. A name with any other character, a space included, is written in quotes: `"content.File Size"`.

A value or a key that needs quotes is written in double quotes. A double quote inside is doubled. Single quotes work the same way, a single quote inside doubled. A backslash is an ordinary character.

In a shell, wrap the whole filter in single quotes and use double quotes inside it. An apostrophe in the filter is then written `'\''`, which ends the shell's quote, adds the apostrophe and starts the quote again:

```bash
--where 'content.Title="say ""hi"""'      # the title: say "hi"
--where 'content.Title="it'\''s"'          # the title: it's
--where '"content.File Size">1000000'    # a key with a space
--where '"content.File Size"|bucket(1e6)=">1000000"'
```

A quoted key is the same key: quoting changes how it is parsed, never which key it names. Modifiers and accessors follow the closing quote.

## Values and Types

Every key has a [type](../concepts/facts.md#value-types): text, number, time, or path. A value you type is interpreted once, as the key's type, before anything runs.

| Key type | A value you type is interpreted as |
|----------|------------------------------|
| Number | A number: `1024`, `-5`, `3.14`, `1e6`. Quoted or bare, the same number |
| Time | A year, a month, a day, a minute, a second or a point within a second, in your local time. See [Times](#times) |
| Text | Text, exactly as typed |
| Path | Text, compared with the path |

**Text is text.** Against a text key, a bare value that looks like a number or a time (`2019`, `0500`, `2024-06-15`, `18:00`) gives an error, because Canon cannot tell which you meant. Quoted, it is text (`content.Model="2019"`). To ask a number or a date question of text, say so with a modifier: `content.numtext|number>=10`, `content.textdate|year=2024`. Words such as `Canon` or `IMG_0001.jpg` are text without quotes.

Canon's message prints each value and key so that, copied back into a filter, it means the same: text that looks like a number or a time is shown there in quotes. A row of [`canon facts`](../commands/query/facts.md) prints text as it is, so text such as `2019` appears bare there; put it in quotes when you copy it into a filter.

**Text is compared exactly as stored.** An accented letter stored as one character and the same letter stored as a letter followed by a separate accent mark are two different texts to `=`, `!=`, `IN` and the ordering operators. A path typed as an argument, such as a scope, is found in either stored form; a value in `--where` is not.

**A key's type belongs to the key.** It is determined from every fact the key holds, on every root, so it does not change with the scope or the filter. A key with no facts has no type: `key?` is false everywhere, and anything else asked of it is rejected as an unknown key.

## Times

A time you type covers a span of time in [your local time](../concepts/facts.md#how-canon-shows-a-time): `2024` is the whole of that year, `2024-06` that month, `2024-06-15` that day, `"2024-06-15 14:30"` that minute, `"2024-06-15 14:30:05"` that second. A stored time also covers a span, at the precision it was written. A comparison selects a value when its whole span satisfies the operator, as the [operator table](#basic) says.

A stored value coarser than the time you typed is in none of `<`, `=` and `>`: a photo dated only `2024` is not `=2024-06`, and not before or after it. It is selected by `!=` and `NOT`, like a source with no value.

| You type | Selects the same as |
|----------|---------------------|
| `source.mtime=2024` | `source.mtime\|year=2024` |
| `source.mtime=2024-06` | `source.mtime\|yearmonth=2024-06` |
| `source.mtime=2024-06-15` | `source.mtime\|date=2024-06-15` |

**A fraction of a second** selects that exact point: `"2024-06-15 14:30:05.45"` selects a value stored as `14:30:05.45` and not one stored as `14:30:05.453`. To select a whole second, type it to the second.

**A time with an offset** (`Z`, `+02:00`, `-05:00`) is translated to your local time: for a user in Amsterdam, `"2023-12-31 21:00 -05:00"` is the minute 03:00 on 1 January 2024. The offset goes straight after the time or after one space; a date alone with an offset (`2023-12-31-05:00`) gives an error.

**A time finer than a modifier's result** gives an error. Where every value in the clause is a time, the error names the modifier that holds them: `|yearmonth=2024-06-15` names `|date=2024-06-15`, and `|date="2024-06-15 14:30"` names `|datetime="2024-06-15 14:30"`.

## Everything Else

`!=`, `!~` and `NOT IN` select everything the positive form does not select, sources with no value included, exactly as `NOT` does. To leave out the sources with no value, ask for a value as well:

```bash
--where 'content.DateTimeOriginal!=2024 AND content.DateTimeOriginal?'
```

For the same reason, `content.DateTimeOriginal<2024` and `NOT content.DateTimeOriginal>=2024` differ: the second also selects every source with no `DateTimeOriginal`.

## When a Filter Is Rejected

When Canon can't process a filter, it points out where the mistake is, and for common cases shows the fix. The filter is rejected before anything runs: nothing is listed, planned or changed. Every problem is named at once, each under the line it is on, with a `^` under its first character, a sentence, and the rule it breaks where there is one:

```
Error: cannot run the filter as given
  filename=a%b.jpg AND Rating=big
            ^ `%` needs quotes around the value: filename="a%b.jpg"
  filename=a%b.jpg AND Rating=big
                              ^ Rating holds numbers; "big" is not a number.
    Type a number: 1024, -5, 3.14, 1e20
```

A fix shows how Canon's rules read what was typed, to copy and judge against what was meant. A fix is shown only where the filter it shows runs, and only in these cases:

| What was typed | The fix |
|----------------|---------|
| A character that needs quotes, in a value after `=`, `!=`, `<`, `>`, `<=` or `>=`, or in an `IN` list | The value in quotes, with `'` where the value holds a `"` and no `'`. A list gets one fix, with every item that needs quotes in quotes |
| `*`, `?` or `[` in a value after `=` or `!=` | Both readings: the glob with `~`, and the exact text in quotes |
| A character that needs quotes in a key name | The name in quotes, for a key that has facts |

```
Error: cannot run the filter as given
  filename=IMG*
              ^ `*` is a wildcard, but `=` does not take wildcards. As a glob: filename~IMG*   As exact text: filename="IMG*"
```

```
Error: cannot run the filter as given
  Lens#2=35mm
      ^ an operator is missing after `Lens`
    After a key: =, !=, <, >, <=, >=, ~, !~, ?, IN or NOT IN
    A key name contains letters, digits, _ . : / and -; quote a name with any other character: "Lens#2"
```

A fix keeps where the rules end each part and adds only quotes. In `Title=O'Brien was here` the value ends at the space, so the fix is `Title="O'Brien"`, and the space is named on its own: a space separates values.

A value that cannot be read as its key's type is named with the ways to write what you meant. A rewrite is offered only where the rewritten clause runs:

```
Error: cannot run the filter as given
  Model=2019
        ^ Model holds text; 2019 is a number or a year.
    As text: Model="2019"     As numbers, if its values are numbers: Model|number=2019     As dates, if its values are dates: Model|year=2019
    `canon facts convert content.Model` to see what it holds and change its type
```

A key whose facts hold two types cannot be compared or modified, since a filter compares one type only. That is an answer about the key, not an error, so it prints without `Error:`, and asking whether the key exists (`Model?`) still works:

```
Model=EOS: Model holds text and numbers; a filter compares one type only.
  text: 1 fact   numbers: 1 fact   (every fact of the key, on every root)
To inspect it and learn how to convert it to one type:
  `canon facts convert content.Model`
```

If the key's type changes while a command runs, or a key with no facts gains its first, the command stops and asks you to run it again.

## Status Predicates

Status predicates check computed state: whether a source is in a particular condition, rather than the value of a stored fact. They use the same syntax as fact-existence checks (`key?`) but evaluate differently.

| Predicate | True when |
|-----------|-----------|
| `archived?` | Content exists in at least one archive root |
| `hashed?` | Content hash has been computed |
| `excluded?` | Source or object is excluded |
| `enriched?` | Has any stored metadata beyond the content hash |

Status predicates are written in lower case and are boolean-only: they work with `?` and `NOT ... ?`; asked with a comparison operator (`archived=1`), the filter gives an error. A name in quotes is never a status predicate: `"archived"?` asks whether the stored fact `content.archived` has a value.

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

**Key distinctions:**

- `archived?` vs `content.hash.sha256?`: `archived?` asks whether the content is in an archive; `content.hash.sha256?` asks whether that fact value exists. `archived?` is the canonical way to check archive status.
- `hashed?` vs `content.hash.sha256?`: Equivalent results, different paths. `hashed?` is the idiomatic form.
- `NOT archived?` includes unhashed sources (they are not archived). Use `NOT archived? AND hashed?` to exclude unhashed sources.
- The set of status predicates is closed: these four, not extensible by users.

## Boolean Operators

| Syntax | Meaning |
|--------|---------|
| `expr AND expr` | Both conditions must match |
| `expr OR expr` | Either condition matches |
| `NOT expr` | Negates the condition |
| `(expr)` | Grouping for precedence |

Operator precedence (highest to lowest): NOT, AND, OR. Use parentheses to override.

`AND`, `OR`, `NOT`, `IN` and `NOT IN` are written in capitals; `and` or `aNd` is a word, so `a=1 and b=2` is rejected. A keyword counts only where one can stand: `AND` and `OR` after a complete condition, `NOT` where a condition starts, `IN` and `NOT IN` after a key. Elsewhere the word is read as what stands there: `filename=AND` compares with the text `AND`, and `source.ext= AND source.size>1`, as an empty shell variable leaves it, reads `AND` as the value of `source.ext`, so Canon points at the space before `source.size`: a space separates values.

## Aliases

You can define named aliases in `$CANON_HOME/aliases.toml` (by default `~/.canon/aliases.toml`). There are two kinds of aliases; Canon classifies each automatically by parsing its value:

### Expression Aliases

Shorthand for complete filter predicates. These are values that contain an operator (like `=`, `>`, `IN`, etc.):

```toml
image = "content.mime IN ('image/jpeg', 'image/png', 'image/gif', 'image/tiff', 'image/webp', 'image/heic')"
video = "content.mime IN ('video/mp4', 'video/quicktime', 'video/x-msvideo', 'video/x-matroska')"
tens = "source.mtime|year >= 2010 AND source.mtime|year < 2020"
large = "source.size > 10000000"
```

Expression aliases are wrapped in parentheses when expanded, so boolean logic inside them composes safely:

```bash
canon ls --where '@image AND @tens'
# Expands to: (content.mime IN (...)) AND (source.mtime|year >= 2010 AND source.mtime|year < 2020)
```

### Key Aliases

Shorthand for verbose key paths: accessors, modifiers, and namespaces. These are values that are just a key (no operator):

```toml
filename = "source.rel_path[-1]"
parent = "source.rel_path[-2]"
ext = "source.ext|lowercase"
year = "source.mtime|year"
taken = "content.DateTimeOriginal"
yearmonth = "content.DateTimeOriginal|yearmonth"
```

Key aliases are substituted literally and used with operators in your filter:

```bash
canon ls --where '@filename = "photo.jpg"'
# Expands to: source.rel_path[-1] = "photo.jpg"

canon ls --where '@yearmonth >= 2023-01'
# Expands to: content.DateTimeOriginal|yearmonth >= 2023-01

canon ls --where '@ext = "jpg" AND @year >= 2020'
# Expands to: source.ext|lowercase = "jpg" AND source.mtime|year >= 2020
```

### Using Aliases

Reference aliases with `@name` in any `--where` expression:

```bash
# Expression alias standalone
canon ls --where '@image'

# Compose expression aliases
canon ls --where '@image OR @video'

# Key alias with operator
canon ls --where '@filename~IMG_*'

# Mix both kinds
canon ls --where '@image AND @year >= 2020'

# Negate an expression alias
canon ls --where 'NOT @large'
```

### How Classification Works

Canon reads each alias's value as a filter. A value that reads as a filter with no problem is an expression alias and gets wrapped in parentheses. Any other value, a key path such as `source.rel_path[-1]` among them, is substituted literally, and a value that does not read where it is used is rejected there, as part of the filter it was used in.

**Rules:**
- Alias names must start with a letter and can contain letters, digits, underscores, and hyphens
- `@` inside quoted strings is treated as a literal character, not an alias reference
- Nested aliases are not supported: an alias's value is substituted without being expanded again, so an `@name` where a key belongs in it gives an error naming it, and an `@` elsewhere in it is a character that needs quotes (`icon = 'filename="icon@2x.png"'`)
- The aliases file is only loaded when `@` appears in a `--where` argument
- If the file doesn't exist and no `@` aliases are used, no error is raised

## Using Modifiers

Modifiers can be applied to fact keys using the `|` syntax. See [Facts](facts.md#modifiers) for the complete list and what each gives. A time modifier works in [your local time](../concepts/facts.md#how-canon-shows-a-time); in a manifest's query it works in the manifest's [time zone](../commands/archive/cluster.md#the-manifests-time-zone). A time modifier also works on text written as a time (`content.textdate|year=2024`).

A modifier's result has its own type, and the value you compare it with is interpreted as that type: `|year=2024` and `|month=6` compare numbers, `|weekday=1` and `|weekday=Monday` the same weekday, `|date=2024-06-15` a day. A modifier that does not apply to the key's type gives an error naming the ones that do. How modifiers chain is in [Expressions](expr.md#chaining).

An accessor splits text into path segments, as it does a path (`content.SourceFile[-1]`). An index past the end is no value.

```bash
# Files from 2024
--where 'source.mtime|year=2024'

# January photos
--where 'content.DateTimeOriginal|month=1'

# Case-insensitive extension matching
--where 'source.ext|lowercase=jpg'

# Case-insensitive glob
--where 'filename|lowercase~img_*'
```

### Examples

```bash
# Files with a content hash
--where 'content.hash.sha256?'

# Files missing a content hash
--where 'NOT content.hash.sha256?'

# JPG files only
--where 'source.ext|lowercase=jpg'

# JPG or PNG files
--where 'source.ext|lowercase=jpg OR source.ext|lowercase=png'

# Common image formats
--where 'source.ext|lowercase IN (jpg, png, gif, webp)'

# Exclude certain extensions
--where 'source.ext|lowercase NOT IN (tmp, bak, log)'

# Not temporary files
--where 'NOT source.ext|lowercase=tmp'

# iPhone photos (content. prefix is optional)
--where 'Make=Apple'

# Files larger than 1MB
--where 'source.size>1000000'

# Files modified in 2024 or later
--where 'source.mtime>=2024-01-01'

# Large images (combining with parentheses)
--where '(source.ext|lowercase=jpg OR source.ext|lowercase=png) AND source.size>1000000'

# Multiple --where flags combine with AND
--where 'source.ext|lowercase=jpg' --where 'content.Make=Apple'
```
