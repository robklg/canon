# canon compare

Compare two folders by content hash. Useful for verifying backups or finding differences between directories.

```bash
# Compare two directories
canon compare /path/to/folder_a /path/to/folder_b

# With filters
canon compare /path/to/folder_a /path/to/folder_b --where 'source.ext=jpg'

# Include excluded sources in comparison
canon compare /path/to/folder_a /path/to/folder_b --include excluded

# Show file paths for differences
canon compare /path/to/folder_a /path/to/folder_b --verbose
```

Output shows:
- Files only in A (by content)
- Files only in B (by content)
- Files in both (matching content hash)

Exit code is 0 if identical, 1 if differences found.
