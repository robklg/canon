# Object

An **object** represents unique content identified by its SHA-256 hash. Objects are content-addressed: two files with identical bytes will have the same hash and thus reference the same object.

Objects enable:

- **Deduplication**: Multiple [sources](source.md) can point to the same object
- **Archive tracking**: When content exists in an archive, all sources with that hash are marked as archived
- **Fact sharing**: Metadata attached to an object is available on all sources with that content

Objects are created automatically when sources are hashed during scanning or enrichment.

## Empty files are contentless

A zero-byte file has shape but no content. Every empty file shares the one
empty-content object, so its hash identifies nothing. Canon treats such
sources as **contentless**: they never count as covered or archived (any
empty file anywhere would otherwise cover them all), never count as
unresolved (there is no content to lose), and never block a
[retirement](retirement.md). They are still ordinary files: `ls` finds them,
they can be excluded, and archive operations carry them with their folders,
so a verbatim folder copy keeps its empty files. Reports that mention them
say *empty files* and state the count; they are never silently omitted.
