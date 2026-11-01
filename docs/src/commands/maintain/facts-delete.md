# canon facts delete

Delete the facts of a key you do not want. To change a key's type instead, use [`facts convert`](facts-convert.md).

```bash
# Preview deletion (dry-run by default)
canon facts delete content.mime --on object
canon facts delete content.Make --on source /path/to/photos --where 'source.ext=jpg'

# Execute deletion
canon facts delete content.mime --on object --yes
```

```
$ canon facts delete Make --on object
Would delete 2 content.Make facts across 2 objects, held by 5 sources (3 in source roots, 2 in archives; 1 of them on suspended roots)

$ canon facts delete Make --on source
No content.Make facts on matching sources; 2 on objects (--on object)
```

- **`--on`** (required): `source` for facts stored on the matching sources, `object` for facts stored on their content. A fact stored on the content is deleted for every copy of it, archive copies and copies on suspended roots included
- **The key**: a name with no prefix is a `content.` key, so `canon facts delete size` deletes `content.size`
- **`--value-type`**: delete only the facts of that type (`text`, `number` or `time`)
- **Matching sources**: the ones [`canon ls`](../query/ls.md) lists at the same scope. Excluded sources are not among them. Archive copies are among them only when the scope is inside an archive root
