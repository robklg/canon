# Source

A **source** is a file discovered on disk during scanning. Canon tracks:

- **Location**: Root path + relative path within the root
- **Identity**: Device ID and inode for move detection
- **Metadata**: Size and modification time
- **Integrity**: Partial hash (first + last 8KB) for validation during transfers
- **State**: A `basis_rev` counter that increments when size or mtime changes

Sources represent where files are found. Multiple sources can point to the same content (see [Object](object.md)) when files are duplicated across locations.

When a source is scanned with hashing enabled (the default), Canon computes its SHA-256 hash and links it to an [object](object.md). This enables deduplication and archive tracking.

## Exclusion

Sources can be marked as excluded to skip them during archiving. A source is considered excluded if:

- The source itself is marked excluded, OR
- The source's linked object is marked excluded

This two-level check means that excluding an object effectively excludes all sources with that content. Object-level exclusion is useful when you want to skip content regardless of where it appears.
