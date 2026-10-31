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

- `--on source` deletes facts stored on the matching sources, `--on object` facts stored on their content; one is required. A fact stored on the content belongs to every copy of it, so `--on object` deletes it for every copy, archive copies and copies on suspended roots included, and the line says how many sources hold that content. Empty files are not counted among them; where the deletion takes the fact on empty files' content, the line ends with the empty files that show it: `; also the fact on empty files' content, shown by 1,188 empty files`
- Facts of the key on the other side are named with the flag that reaches them, after a preview or summary as `; 3 on sources not deleted (--on source)`
- The key is read under `content.`: `canon facts delete Make` deletes `content.Make`. A [built-in key](../../reference/builtins.md#built-in-keys)'s name is read the same way, so `canon facts delete size` deletes `content.size`, where `--where size` reads the file size
- A key may be given as Canon prints it, in quotes or not ([Quoting](../../reference/filter.md#quoting)). A key holding a control character, which the key list marks `!`, is typed in the shell's `$'…'` form: `canon facts delete $'content.Make\n' --on object`
- A name starting with `source.` is refused: `Cannot delete 'source.size': names starting with 'source.' cannot be deleted.`
- The content hash is refused, under any name that reads as it:

  ```
  $ canon facts delete hash.sha256 --on object
  The content hash (content.hash.sha256) is Canon's own record of content identity, and its facts are not deleted
  to correct a hash a tool imported wrongly, import the right one with `canon import-facts`, which moves the source to its content
  ```

- Dry-run by default; with `--yes` the facts are deleted at once and only the summary prints, the preview's line starting `Deleted`. The summary is the decision's record, so [`canon trail`](../query/trail.md) shows it
- `--value-type text`, `number` or `time` deletes only the facts of that type, and the line names it: `No content.Make facts of type number on matching objects`. `--value-type duration` is answered:

  ```
  $ canon facts delete Make --on object --value-type duration
  Canon has no duration type: durations are stored as numbers
  ```

- Under a negated key in `--where`, the count under that line says how many matching sources have no value for it, and a time compared with a finer date how many values it could not place ([What an Acting Command States](../../reference/filter.md#what-an-acting-command-states)): `from 2,140 matching sources, 312 of which have no Make`. A matching source with no fact of the deleted key loses nothing. Where nothing is deleted, a time line with sources not selected still prints, under the sentence that says nothing matched or no fact was found
- The matching sources are the ones [`canon ls`](../query/ls.md) lists at the same scope, which are the ones the key list in [`canon facts`](../query/facts.md) counts when run without `--include`. Excluded sources and archive copies are not among them. Inside an archive root, archive sources are, the same way a read there sees them, so content seen only on archive copies is reached by naming the archive root as the scope. Content held only by excluded sources is not reached while they are excluded
