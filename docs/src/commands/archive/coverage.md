# canon coverage

Show archive coverage statistics: how many sources are hashed and how many are archived.

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

A path on a [suspended](../roots/roots.md#suspending-roots) root, or the current directory inside one, is set aside and stated under the header; when that is the whole scope, nothing is reported and the exit code is 1. Under `--compact` the statement goes to stderr and no row is emitted for the suspended path.

Example output (global):
```
Coverage: all roots

Root: /path/to/backup1 (source)
  Total sources:     1,234
  Hashed:            1,100 (89.1%)
  Empty files:          40 (no content to cover)
  Archived:            850 (80.2% of 1,060 with content)
  Unarchived:          210

Root: /path/to/backup2 (source)
  Total sources:       567
  Hashed:              500 (88.2%)
  Archived:            400 (80.0% of 500 with content)
  Unarchived:          100

────────────────────────────────────────
Overall:
  Total sources:     1,801
  Hashed:            1,600 (88.8%)
  Empty files:          40 (no content to cover)
  Archived:          1,250 (80.1% of 1,560 with content)
  Unarchived:          310
```

- **Hashed**: Sources with a content hash (ready for archiving)
- **Empty files**: Zero-byte sources, shown when present. They are
  [contentless](../../concepts/object.md#empty-files-are-contentless), so
  coverage counts them in neither `Archived` nor `Unarchived`; the lines add
  up as hashed = empty files + with-content, and with-content = archived +
  unarchived
- **Archived**: Sources whose content exists in an archive root. The
  percentage names its denominator (`of N with content`), so a fully-covered
  selection reads 100% even when it contains empty files; the remainder is
  always exactly `Unarchived`
- With `--archive`: Shows "In this archive" vs "Not in archive" for that specific archive
