# Facts

Facts are key-value metadata attached to [sources](source.md) or [objects](object.md).

## Types of Facts

**Built-in facts** are collected automatically during scanning:
- `source.ext` - File extension
- `source.size` - File size in bytes
- `source.mtime` - Modification timestamp
- `content.hash.sha256` - Content hash (when computed)

**Imported facts** come from external tools via the [enrichment pipeline](../commands/enrich.md):
- EXIF metadata: `content.Make`, `content.Model`, `content.DateTimeOriginal`
- Geolocation: `content.geo.city`, `content.geo.country`
- Media info: `content.mime`, `content.duration`
- Any custom key-value pairs you choose to import

## Namespaces

Facts are namespaced:

- `source.*` - Facts about the file on disk (path, size, timestamps)
- `content.*` - Facts about the content itself (stored on objects when hashed)
- `policy.*` - Canon's internal state (exclusions, etc.)

When querying, the `content.` prefix is optional: `--where 'Make=Apple'` is equivalent to `--where 'content.Make=Apple'`.

## Value Types

Canon stores facts as:
- **Text**: Strings like `"Apple"` or `"image/jpeg"`
- **Numbers**: Integers or decimals like `1024` or `3.14`
- **Timestamps**: Unix timestamps, enabling date modifiers like `|year` and `|month`

Type hints can be provided during import to ensure correct parsing. See [Enriching](../commands/enrich.md) for details.
