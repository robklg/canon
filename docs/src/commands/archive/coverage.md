# canon coverage

Show archive coverage statistics - how many sources are hashed and how many are archived.

```bash
# Coverage for current directory (when inside a root)
canon coverage

# Scoped to a specific directory
canon coverage /path/to/photos

# Global overview of all source roots
canon coverage --global

# With filters
canon coverage --where 'source.ext=jpg'

# Coverage relative to a specific archive root
canon coverage --archive id:1
canon coverage --archive path:/path/to/archive

# Include archive roots in analysis
canon coverage --include archived

# Include excluded sources
canon coverage --include excluded

# Include both
canon coverage --include all
```

The output begins with a scope header (`Coverage: /path` or `Coverage: all roots`).

Example output (global):
```
Coverage: all roots

Root: /path/to/backup1 (source)
  Total sources:     1,234
  Hashed:            1,100 (89.1%)
  Archived:            850 (77.3% of hashed)
  Unarchived:          250

Root: /path/to/backup2 (source)
  Total sources:       567
  Hashed:              500 (88.2%)
  Archived:            400 (80.0% of hashed)
  Unarchived:          100

────────────────────────────────────────
Overall:
  Total sources:     1,801
  Hashed:            1,600 (88.8%)
  Archived:          1,250 (78.1% of hashed)
  Unarchived:          350
```

- **Hashed**: Sources with a content hash (ready for archiving)
- **Archived**: Sources whose content exists in an archive root
- With `--archive`: Shows "In this archive" vs "Not in archive" for that specific archive
