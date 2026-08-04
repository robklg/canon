# Object

An **object** represents unique content identified by its SHA-256 hash. Objects are content-addressed: two files with identical bytes will have the same hash and thus reference the same object.

Objects enable:

- **Deduplication**: Multiple [sources](source.md) can point to the same object
- **Archive tracking**: When content exists in an archive, all sources with that hash are marked as archived
- **Fact sharing**: Metadata attached to an object is available on all sources with that content

Objects are created automatically when sources are hashed during scanning or enrichment.

## Empty files are contentless

A zero-byte file is all shape and no content: every empty file in the universe
shares the one empty-content object, so its identity identifies nothing. Canon
treats such sources as **contentless** — they never count as covered or
archived (any empty file anywhere would hollowly "cover" them all), never
count as unresolved (there is no content to lose), and never block a
retirement. They are still real files: `ls` finds them, exclusion can dismiss
them, and archive operations **carry them with their folders** — a verbatim
folder copy keeps its empty files precisely because they are never skipped as
"already archived". Where a report speaks of them, it says *empty files*
plainly and states the count rather than hiding it.
