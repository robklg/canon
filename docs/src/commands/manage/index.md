# Managing Sources

After scanning and enriching, you may want to control which sources are included in archiving operations.

The [`exclude`](exclude.md) command lets you mark sources to skip during `cluster generate` and `apply`. This is useful for:

- Ignoring temporary or system files
- Skipping known duplicates while keeping a preferred copy
- Filtering out small files below a size threshold
- Removing unwanted files from consideration without deleting them

Exclusions are stored as `policy.exclude` facts and can be cleared at any time.
