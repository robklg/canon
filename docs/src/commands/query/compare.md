# canon compare

Compare two folders by content hash. Useful for verifying backups or finding differences between directories.

```bash
# Compare current directory against another location
canon compare /path/to/folder_b

# Compare two explicit directories
canon compare /path/to/folder_a /path/to/folder_b

# With filters
canon compare /path/to/folder_a /path/to/folder_b --where 'source.ext=jpg'

# Include excluded sources in comparison
canon compare /path/to/folder_a /path/to/folder_b --include excluded

# Show file paths for differences
canon compare /path/to/folder_a /path/to/folder_b --verbose
```

With one path argument, the current directory is used as side A and the argument as side B. With two paths, they are used as A and B explicitly. The current directory must be inside a known root when used as side A.

Output shows:
- Files only in A (by content)
- Files only in B (by content)
- Files in both (matching content hash)

Unhashed files are skipped and counted on stderr. Empty files are skipped and
counted the same way (`Skipped N empty files (no content to compare)`): they
are [contentless](../../concepts/object.md#empty-files-are-contentless).
Compare reports on content; whether two folders correspond file-by-file,
empty files included, is a question it deliberately does not answer.

Exit code is 0 if identical, 1 if differences found.

Both sides are load-bearing here, so a side on a [suspended](../roots/roots.md#suspending-roots) root refuses the whole comparison by name rather than answering about one side alone. The same holds for the current directory when only one path is given.
