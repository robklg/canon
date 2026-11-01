# canon coverage

Archive coverage statistics: how many sources are hashed and how many are archived.

```bash
# Coverage for current directory (when inside a root)
canon coverage

# Scoped to a specific directory
canon coverage /path/to/photos

# Global overview of all source roots
canon coverage --global

# One line per root
canon coverage --global --compact

# With filters
canon coverage --where 'source.ext=jpg'

# Coverage relative to a specific archive root
canon coverage --archive id:1

# Include archive roots in analysis
canon coverage --include archived
```

Global output:
```
Coverage: all roots

Root 1: /path/to/backup1 (source)
  Total sources:      1,234
  Hashed:             1,100 (89.1%)
  Empty files:           40 (no content to cover)
  Archived:             850 (80.2% of 1,060 with content)
  Unarchived:           210

Root 2: /path/to/backup2 (source)
  Total sources:        567
  Hashed:               500 (88.2%)
  Archived:             400 (80.0% of 500 with content)
  Unarchived:           100

────────────────────────────────────────
Overall:
  Total sources:      1,801
  Hashed:             1,600 (88.8%)
  Empty files:           40 (no content to cover)
  Archived:           1,250 (80.1% of 1,560 with content)
  Unarchived:           310
```

- **Hashed**: sources with a content hash
- **Empty files**: [empty files](../../concepts/source-object.md#empty-files-are-contentless), counted in neither `Archived` nor `Unarchived`
- **Archived**: sources whose content exists in an archive root
