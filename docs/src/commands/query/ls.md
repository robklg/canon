# canon ls

List sources matching filters.

```bash
# List sources in current directory (default when inside a root)
canon ls

# List sources matching a filter
canon ls --where 'source.ext|lowercase=jpg'

# Filter by status
canon ls --where 'NOT archived? AND hashed?'

# Show duplicate files (same content hash), grouped by hash
canon ls --duplicates

# View excluded sources (requires --include for visibility)
canon ls --include excluded --where 'excluded?'

# Include sources from archive roots (automatic when scope is in an archive)
canon ls --include archived

# Query all roots, ignoring current directory scope
canon ls --global --where 'source.ext|lowercase=jpg'

# Long format with size and date
canon ls -l

# Sort by path, size, mtime or name; -r reverses
canon ls --sort size -r

# Null-delimited output for xargs (handles spaces in paths, macOS)
canon ls -0 --where 'source.ext|lowercase=jpg' | xargs -0 open -a Preview
```

- **Status column**: with `--include`, `ls -l` marks `E` (source-level exclusion), `X` (object-level exclusion) or `A` (source in an archive root)

Paths go to stdout, the scope line and the count to stderr:
```
scope: /Volumes/old-drive/photos
vacation/img001.jpg
vacation/img002.jpg
work/doc.pdf
3 sources
```
