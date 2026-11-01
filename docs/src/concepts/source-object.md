# Sources and Objects

A **source** is a file on disk, at one [root](roots.md) and relative path. An **object** is content, identified by its SHA-256 hash. Two files with identical bytes have the same hash and share one object, wherever they are found.

```
Source A: /backup1/photos/IMG_001.jpg  ─┐
Source B: /backup2/old/IMG_001.jpg     ─┼─► Object (hash: abc123...)
Source C: /downloads/photo.jpg         ─┘
```

For each source Canon tracks:

- **Location**: Root path + relative path within the root
- **Identity**: Device ID and inode for move detection
- **Metadata**: Size and modification time
- **Integrity**: Partial hash (first + last 8KB) for validation during transfers
- **State**: A `basis_rev` counter that increments when size or mtime changes

Hashing, by default part of [scanning](../commands/roots/scan.md), links a source to its object. Objects enable:

- **Deduplication**: Multiple sources can point to the same object
- **Archive tracking**: When content exists in an archive, all sources with that hash count as archived
- **Fact sharing**: A content fact stored on the object is available on all sources with that content

Unhashed sources cannot be linked to objects, so they cannot be deduplicated or tracked for archive coverage.

## Empty files are contentless

A zero-byte file has no content, so Canon treats its source as **contentless**. Every
empty file shares the one empty-content object, so its hash identifies nothing.

- **In counts**: never [covered](resolution.md#standings), archived or unresolved, and
  never a block on [retirement](retirement.md).
- **As a file**: ordinary. `ls` finds it, it can be excluded, and archive operations
  take it along with its directory, so a verbatim directory copy keeps its empty files.
