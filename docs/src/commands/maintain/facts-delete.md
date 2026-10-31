# canon facts delete

Delete facts by key. Useful for removing incorrect or unwanted metadata. To change a key's type, use [`facts convert`](facts-convert.md).

```bash
# Preview deletion (dry-run by default)
canon facts delete content.mime --on object
canon facts delete content.Make --on source /path/to/photos --where 'source.ext=jpg'

# Execute deletion
canon facts delete content.mime --on object --yes
```

- `--on source` deletes facts stored on the source, `--on object` facts stored on the content; one is required
- The key is read under `content.`: `canon facts delete Make` deletes `content.Make`. A [built-in key](../../reference/builtins.md#built-in-keys)'s name is read the same way, so `canon facts delete size` deletes `content.size`, where `--where size` reads the file size
- A key may be given as Canon prints it, in quotes or not ([Quoting](../../reference/filter.md#quoting))
- A name starting with `source.` is refused: `Cannot delete 'source.size': names starting with 'source.' cannot be deleted.`
- Dry-run by default; use `--yes` to execute. The preview names the key it deletes: `Would delete 2 'content.Make' fact rows across 2 objects`
- Under a negated key in `--where`, the count under that line says how many matching sources have no value for it, and a time compared with a finer date how many values it could not place ([What an Acting Command States](../../reference/filter.md#what-an-acting-command-states)): `from 2,140 matching sources, 312 of which have no Make`. A matching source with no fact of the deleted key loses nothing. Where nothing is deleted, a time line with sources not selected still prints, under the sentence that says nothing matched or no fact was found
- The population is the one a matching [`canon ls`](../query/ls.md) would list at the same
  scope: excluded sources and archive copies are not reached, so a deletion never goes past
  what you can preview. Inside an archive root, archive sources are in view and are reached,
  the same way a read there sees them
