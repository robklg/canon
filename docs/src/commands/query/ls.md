# canon ls

List sources matching filters. Useful for quick inspection and piping to other tools.

```bash
# List sources in current directory (default when inside a root)
canon ls

# List sources matching a filter
canon ls --where 'source.ext=jpg'

# Filter by source ID
canon ls --where 'source.id=12345'

# Filter by archive status using status predicates
canon ls --where 'archived?'
canon ls --where 'NOT archived?'
canon ls --where 'NOT archived? AND hashed?'

# Filter by hash status
canon ls --where 'NOT hashed?'

# Show duplicate files (same content hash), grouped by hash
canon ls --duplicates

# View excluded sources (requires --include for visibility)
canon ls --include excluded --where 'excluded?'

# Include sources from archive roots (automatic when scope is in an archive)
canon ls --include archived

# Include excluded sources in results
canon ls --include excluded

# Include both archived and excluded sources
canon ls --include all

# Query all roots, ignoring current directory scope
canon ls --global --where 'source.ext=jpg'

# Long format with size and date
canon ls -l

# Null-delimited output for xargs (handles spaces in paths, macOS)
canon ls -0 --where 'source.ext=jpg' | xargs -0 open -a Preview

# Combine status predicates with fact filters
canon ls --where 'NOT archived? AND mime~image/*'
```

**Status predicates** (`archived?`, `hashed?`, `excluded?`, `enriched?`) compose freely with other `--where` expressions. See [Filter Syntax](../../reference/filter.md#status-predicates) for details.

**`--duplicates`** is a display mode (changes output format to grouped by hash), not a filter. It can be combined with `--where`.

**Status column in long format:** When `--include` is used, `ls -l` shows a status column indicating source state: `E` (source-level exclusion), `X` (object-level exclusion), `A` (source in an archive root), or blank.

**Scope display:** When scoped (via CWD or explicit path), `ls` prints `scope: /path` to stderr. When global, no scope line is printed.

A path on a [suspended](../roots/roots.md#suspending-roots) root, or the current directory inside one, is set aside and stated on stderr beside the scope line; when that is the whole scope, nothing is listed and the exit code is 1.

**Path display:**
- CWD-scoped (no explicit path, inside a root) → relative output paths
- Explicit absolute path or `--global` → absolute output paths

Output is one path per line (stdout), with a count printed to stderr:
```
scope: /Volumes/old-drive/photos
vacation/img001.jpg
vacation/img002.jpg
work/doc.pdf
3 sources
```
