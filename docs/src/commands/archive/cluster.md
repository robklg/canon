# canon cluster

## cluster generate

A manifest of the sources matching filters. `--dest` must be inside a registered archive root.

```bash
# All photos to an archive (unhashed sources are skipped)
canon cluster generate --where 'source.ext|lowercase IN (jpg, png, heic)' --dest /Volumes/Archive/Photos

# Scope to a specific path
canon cluster generate /path/to/photos --dest /Volumes/Archive

# Custom output file
canon cluster generate --where 'source.ext|lowercase=jpg' --dest /Volumes/Archive -o my-manifest.toml

# Also keep sources in archive roots, and sources whose content is already archived
canon cluster generate --where 'source.ext|lowercase=jpg' --dest /Volumes/Archive --allow archived

# Allow sources with identical content in the selection
canon cluster generate --where 'source.ext|lowercase=jpg' --dest /Volumes/Archive --allow duplicates

# List the sources left out as already archived
canon cluster generate --where 'source.ext|lowercase=jpg' --dest /Volumes/Archive --show-archived
```

Two files: the manifest (`.toml`), which you edit, and the lock file (`.lock`), which [`apply`](apply.md) uses as written. The manifest opens in `$VISUAL`, `$EDITOR` or `vi` unless `--no-edit` is given. `-O` writes it at the destination.

**Typical workflow:**

```bash
canon cluster generate --where 'source.ext|lowercase IN (jpg, png, heic)' --dest /Volumes/Archive
# Edit the pattern in the editor that opens
canon apply manifest.toml --dry-run   # Preview
canon apply manifest.toml             # Execute
```

**Output:**

```
Generated manifest: manifest.toml (6 sources in manifest.lock)
  From 2 roots:
    /Volumes/Drive1  (4)
    /Volumes/Drive2  (2)
  6 have no archived copy
Time zone: Europe/Amsterdam, recorded in the manifest's zone line.

To apply: canon apply manifest.toml
```

[Empty files](../../concepts/source-object.md#empty-files-are-contentless) are never skipped as already archived: the manifest takes them with their directory.

## The manifest

```toml
# Canon manifest — edit pattern and Notes freely.
# To change the query, edit it here then run: canon cluster refresh <manifest>
# zone: the time zone for times in the query and in directory names.
# Edit it, or delete it to take this machine's, then run: canon cluster refresh <manifest>
# Other fields are managed by Canon — do not edit.
#
# === Cluster Summary ===
# 4 sources
# From 1 root:
#   /Volumes/Drive1  (4)
# 4 have no archived copy
# === Notes ===
#
[meta]
version = 2
query = ["source.ext|lowercase IN (jpg, png, heic)"]
scope = ["/Volumes/Drive1/photos"]
zone = "Europe/Amsterdam"
generated_at = "2026-11-01T14:15:19Z"
lock_hash = "f1577a6513197dd510a0d7f113692d9a3f82c1d82196384ca7051ee2606e1405"

[options]
allow = []

[output]
pattern = "{scope.rel_path}"
archive_root_id = 3
base_dir = ""

# Keys the 4 selected sources share, for use in the pattern:
# ...
```

| Field | What it holds | An edit takes effect |
|---|---|---|
| `meta.version` | The manifest format version | Managed by Canon |
| `meta.query` | The filters | At the next refresh |
| `meta.scope` | The paths the selection was scoped to; no such line when unscoped | At the next refresh; `apply` takes the scope from the lock |
| `meta.zone` | The time zone for dates, in the query and in directory names | At the next refresh; `apply` uses the lock's |
| `meta.generated_at` | When the manifest was generated | Managed by Canon |
| `meta.lock_hash` | The lock file's hash; empty when a refresh selected nothing | Managed by Canon |
| `options.allow` | The `--allow` values given. A refresh reads them all; `apply` reads only `duplicates` | At the next command that reads it |
| `output.pattern` | Where files go below `base_dir`. Starts at `{scope.rel_path}` for a scoped generation and `{source.rel_path}` otherwise: files keep the directory structure they were found in | At the next apply |
| `output.base_dir` | The destination's directory inside the archive root | At the next apply |
| `output.archive_root_id` | The destination archive root | Managed by Canon |

The Cluster Summary is rewritten on each refresh; Notes are kept.

To copy a filter into `query`, paste it between `'''` and `'''`: `query = ['''filename="a%b.jpg"''']`.

Pattern syntax: [Pattern Expressions](../../reference/expr.md). Examples: [Common Patterns](../../reference/expr.md#common-patterns).

**Year directories**: a second manifest can take the sources whose time is known only to the year:

```toml
query = ["content.DateTimeOriginal|year? AND NOT content.DateTimeOriginal|month?"]
pattern = "{content.DateTimeOriginal|year}/{filename}"
```

## cluster refresh

```bash
# Re-query and update the lock file
canon cluster refresh manifest.toml

# Edit the manifest first, then re-query from what was saved
canon cluster refresh --edit manifest.toml
```

Re-runs the manifest's query and rewrites the lock file. `[options]` and `[output]` stay as they are.

A refresh leaves out sources on [suspended roots](../../concepts/roots.md#suspended-roots).

## The manifest's time zone

A time with an offset, or a file's save time, can fall on a different date in another zone. The manifest records the time zone in its `zone` line and the lock file records it too, so a manifest puts files in the same directories on any machine.

- **`cluster generate`**: records this machine's zone
- **`cluster refresh`**: selects in the zone line's time zone and writes it into the lock
- **[`apply`](apply.md) and `cluster status`**: use the lock's time zone, never this machine's

A changed zone can change which sources a time query selects, and the refresh says so.

## `cluster status`

The state of a manifest, and its next step.

```
Manifest: manifest.toml
Destination: /Volumes/Archive
Pattern: {content.Make}/{content.Taken|month}/{filename}
Lock: 6 entries
Time zone: Europe/Amsterdam (the lock's)

3 of 6 entries have no destination:
  2 sources: {content.Make} has no value
  1 source: {content.Taken|month} has no value: the time is known only to the year
  /Volumes/Photos/2016/IMG_0001.jpg
  /Volumes/Photos/2016/IMG_0003.jpg
  /Volumes/Photos/2016/IMG_0002.jpg

Summary: 0 at destination, 3 pending, 3 with no destination computable, 0 source files missing.

Adjust the pattern in your manifest, or import the missing facts; or add "content.Make?" to the manifest's query to leave such sources out, then run `canon cluster refresh manifest.toml`.
`canon facts` shows which sources have these facts
Use a coarser modifier in the pattern, which takes effect at the next apply; or add "content.Taken|month?" to the manifest's query to leave such sources out, then run `canon cluster refresh manifest.toml`.
```

`NOT CONFIRMED` is a file of the right size that the index does not record at its destination. `canon apply --resume` compares it with the lock.
