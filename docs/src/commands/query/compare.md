# canon compare

Two directories compared by content hash.

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

With one path argument, the current directory is side A and the argument is side B.

```
Comparing:
  A: /mnt/old-drive/photos/italy
  B: /mnt/old-drive/photos/misc

Files in both (by content): 21
Only in A: 26
Only in B: 0
```

Exit code is 0 if identical, 1 if differences found.
