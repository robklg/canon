# Object

An **object** represents unique content identified by its SHA-256 hash. Objects are content-addressed: two files with identical bytes will have the same hash and thus reference the same object.

Objects enable:

- **Deduplication**: Multiple [sources](source.md) can point to the same object
- **Archive tracking**: When content exists in an archive, all sources with that hash are marked as archived
- **Fact sharing**: Metadata attached to an object is available on all sources with that content

Objects are created automatically when sources are hashed during scanning or enrichment.
