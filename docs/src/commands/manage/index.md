# Managing Sources

After scanning and enriching, you may want to control which sources are included in archiving operations, or annotate locations with observations.

The [`exclude`](exclude.md) command lets you mark sources to skip during `cluster generate` and `apply`. This is useful for:

- Ignoring temporary or system files
- Skipping known duplicates while keeping a preferred copy
- Filtering out small files below a size threshold
- Removing unwanted files from consideration without deleting them

Exclusions are stored directly on sources and can be cleared at any time.

The [`note`](note.md) command lets you annotate locations with timestamped observations during exploration. Notes surface automatically in [`survey`](../query/survey.md) output, serving as breadcrumbs when revisiting locations.
