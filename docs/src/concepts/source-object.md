# Source vs. Object

Understanding the relationship between sources and objects is key to how Canon handles deduplication and archive tracking.

## Sources Are Locations

When a [root](roots.md) is scanned, Canon indexes every file it finds as a **source**. Each source represents a specific file at a specific path.

## Objects Are Content

When sources are hashed, Canon creates or links them to **objects**. An object represents the underlying content, independent of where it was found.

```
Source A: /backup1/photos/IMG_001.jpg  ─┐
Source B: /backup2/old/IMG_001.jpg     ─┼─► Object (hash: abc123...)
Source C: /downloads/photo.jpg         ─┘
```

All three sources above have identical content, so they reference the same object.

## Fact Sharing

When a source is linked to an object:

- **Content facts** (like EXIF metadata) can be stored on the object and become available to all sources with that hash
- **Source facts** (like file path) remain specific to each source

This allows metadata to flow between different copies of the same content. Import a fact once, and it's available everywhere that content exists.

## Archive Tracking

Canon uses the source-object relationship to track archiving progress:

- When you archive a file, Canon copies it to an archive root and records the object's hash
- Any source with that same hash is now considered "archived"
- The `coverage` command shows how many of your sources exist in an archive

## Hashing

By default, Canon hashes all files during scanning. Since hashing can be time-consuming for large collections, you can:

- Use `--no-hash` during scan to skip hashing initially
- Hash selectively via the [enrichment pipeline](../commands/enrich.md), targeting specific file types

Unhashed sources cannot be linked to objects, so they cannot be deduplicated or tracked for archive coverage.
