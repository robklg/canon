# canon ls

List sources matching filters. Useful for quick inspection and piping to other tools.

```bash
# List all sources in current directory
canon ls .

# List sources matching a filter
canon ls --where 'source.ext=jpg'

# Filter by source ID
canon ls --where 'source.id=12345'

# List only archived sources (content exists in an archive)
canon ls --archived

# List archived sources with their archive location(s)
# Output: source_path<TAB>archive_path (one line per archive location)
canon ls --archived=show

# List only unarchived sources (hashed but not in any archive)
canon ls --unarchived

# List only unhashed sources (no content hash yet)
canon ls --unhashed

# Show duplicate files (same content hash), grouped by hash
canon ls --duplicates

# Show only excluded sources (source-level and object-level)
canon ls --excluded

# Include sources from archive roots (automatic when scope is in an archive)
canon ls --include archived

# Include excluded sources in results
canon ls --include excluded

# Include both archived and excluded sources
canon ls --include all

# Long format with size and date
canon ls -l

# Null-delimited output for xargs (handles spaces in paths, macOS)
canon ls -0 --where 'source.ext=jpg' | xargs -0 open -a Preview
```

**Filter modes** (`--archived`, `--unarchived`, `--unhashed`, `--duplicates`, `--excluded`) are mutually exclusive -- only one can be active at a time.

**Status column in long format:** When `--include` is used or `--excluded` mode is active, `ls -l` shows a status column indicating source state: `E` (source-level exclusion), `X` (object-level exclusion), `A` (archived), or blank.

**Path display:**
- Relative path input (`.`, `subdir`) → relative output paths
- Absolute path input (`/path/to/dir`) → absolute output paths

Output is one path per line (stdout), with a count printed to stderr:
```
vacation/img001.jpg
vacation/img002.jpg
work/doc.pdf
3 sources
```
