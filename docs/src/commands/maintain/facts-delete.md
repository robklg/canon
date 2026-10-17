# canon facts delete

Delete facts by key. Useful for removing incorrect or unwanted metadata.

```bash
# Preview deletion (dry-run by default)
canon facts delete content.mime --on object
canon facts delete content.Make --on source /path/to/photos --where 'source.ext=jpg'

# Execute deletion
canon facts delete content.mime --on object --yes
```

- `--on source` deletes facts stored on the source, `--on object` facts stored on the content; one is required
- The key is read under `content.`: `canon facts delete Make` deletes `content.Make`. A [built-in key](../../reference/builtins.md#built-in-keys)'s name is read the same way, so `canon facts delete size` deletes `content.size`, where `--where size` reads the file size
- A name starting with `source.` is refused: `Cannot delete 'source.size': names starting with 'source.' cannot be deleted.`
- Dry-run by default; use `--yes` to execute. The preview names the key it deletes: `Would delete 2 'content.Make' fact rows across 2 objects`
- The population is the one a matching [`canon ls`](../query/ls.md) would list at the same
  scope: excluded sources and archive copies are not reached, so a deletion never goes past
  what you can preview. Inside an archive root, archive sources are in view and are reached,
  the same way a read there sees them
