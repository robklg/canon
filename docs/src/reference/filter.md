# Filter Syntax

Filters select sources based on facts using a boolean expression language. Most commands accept `--where` to filter which sources they operate on. Multiple `--where` flags are combined with AND.

## Operators

### Basic

| Syntax | Meaning |
|--------|---------|
| `key?` | Fact exists |
| `key=value` | Fact equals value (case-sensitive) |
| `key!=value` | Fact doesn't equal value (case-sensitive) |
| `key~pattern` | Glob pattern match (case-sensitive) |
| `key!~pattern` | Glob pattern doesn't match |
| `key>value` | Greater than (numbers/dates) |
| `key>=value` | Greater or equal |
| `key<value` | Less than |
| `key<=value` | Less or equal |
| `key IN (v1, v2, ...)` | Fact matches any value in list |
| `key NOT IN (v1, v2, ...)` | Fact doesn't match any value in list |

### Glob Patterns

The `~` operator supports shell-style glob patterns:

| Pattern | Meaning |
|---------|---------|
| `*` | Match zero or more characters |
| `?` | Match exactly one character |
| `[abc]` | Match any character in set |
| `[a-z]` | Match character range |
| `[!abc]` | Match any character NOT in set |
| `\*` | Literal asterisk (escape) |

```bash
# Files starting with IMG_
--where 'filename~"IMG_*"'

# Files with 3-letter extension
--where 'source.ext~"???"'

# Files in a year subdirectory
--where 'source.rel_path~"*/2024/*"'

# Exclude temp files
--where 'filename!~"*.tmp"'
```

## Boolean Operators

| Syntax | Meaning |
|--------|---------|
| `expr AND expr` | Both conditions must match |
| `expr OR expr` | Either condition matches |
| `NOT expr` | Negates the condition |
| `(expr)` | Grouping for precedence |

Operator precedence (highest to lowest): NOT, AND, OR. Use parentheses to override.

## Aliases

You can define named aliases for frequently used filter expressions in `$CANON_HOME/aliases.toml` (by default `~/.canon/aliases.toml`):

```toml
image = "content.mime IN ('image/jpeg', 'image/png', 'image/gif', 'image/tiff', 'image/webp', 'image/heic')"
video = "content.mime IN ('video/mp4', 'video/quicktime', 'video/x-msvideo', 'video/x-matroska')"
tens = "source.mtime|year >= 2010 AND source.mtime|year < 2020"
large = "source.size > 10000000"
screenshot = "source.rel_path[0] ~ 'Screenshots*'"
```

Reference aliases with `@name` in any `--where` expression:

```bash
# Simple alias
canon ls --where '@image'

# Compose with boolean logic
canon ls --where '@image OR @video'

# Mix aliases with regular expressions
canon ls --where '@image AND @tens'

# Negate an alias
canon ls --where 'NOT @screenshot'
```

Each alias is wrapped in parentheses when expanded, so boolean logic inside aliases composes safely. For example, `@image AND @tens` expands to:

```
(content.mime IN (...)) AND (source.mtime|year >= 2010 AND source.mtime|year < 2020)
```

**Rules:**
- Alias names must start with a letter and can contain letters, digits, underscores, and hyphens
- `@` inside quoted strings is treated as a literal character, not an alias reference
- Nested aliases are not supported (`@` in alias values is literal)
- The aliases file is only loaded when `@` appears in a `--where` argument
- If the file doesn't exist and no `@` aliases are used, no error is raised

## Using Modifiers

Modifiers can be applied to fact keys using the `|` syntax. See [Facts](facts.md#modifiers) for the complete list.

```bash
# Files from 2024
--where 'source.mtime|year=2024'

# January photos
--where 'content.DateTimeOriginal|month=1'

# Case-insensitive extension matching
--where 'source.ext|lowercase=jpg'

# Case-insensitive glob
--where 'filename|lowercase~"img_*"'
```

### Examples

```bash
# Files with a content hash
--where 'content.hash.sha256?'

# Files missing a content hash
--where 'NOT content.hash.sha256?'

# JPG files only
--where 'source.ext=jpg'

# JPG or PNG files
--where 'source.ext=jpg OR source.ext=png'

# Common image formats
--where 'source.ext IN (jpg, png, gif, webp)'

# Exclude certain extensions
--where 'source.ext NOT IN (tmp, bak, log)'

# Not temporary files
--where 'NOT source.ext=tmp'

# iPhone photos (content. prefix is optional)
--where 'Make=Apple'

# Files larger than 1MB
--where 'source.size>1000000'

# Files modified in 2024 or later
--where 'source.mtime>=2024-01-01'

# Large images (combining with parentheses)
--where '(source.ext=jpg OR source.ext=png) AND source.size>1000000'

# Multiple --where flags combine with AND
--where 'source.ext=jpg' --where 'content.Make=Apple'
```
