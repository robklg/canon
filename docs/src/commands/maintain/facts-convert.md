# canon facts convert

Shows what a stored key holds, and converts it to text, a number or a time.

```bash
canon facts convert content.DateTimeOriginal                      # what it holds, what it can become (read-only)
canon facts convert content.DateTimeOriginal --to time            # preview (dry run by default)
canon facts convert content.DateTimeOriginal --to time --list     # one line per lost or removed fact and source
canon facts convert content.DateTimeOriginal --to time --allow removal=2 --yes \
    --reason "the tool wrote dates as text"
```

| Flag | Effect |
|---|---|
| `--to <type>` | The type to convert to: `text`, `number` or `time`. Without it, the command shows what the key holds |
| `--list` | One line per lost or removed fact and source, tab-separated |
| `-0`, `--null` | With `--list`: fields separated by a NUL, each record ended by one |
| `--allow loss=N`, `--allow removal=N` | Accept the cost the preview states |
| `--yes` | Convert (default is a dry run) |
| `--reason <why>` | Attach a reason to the [decision record](../../concepts/decisions.md) |

A conversion takes no paths and no `--where`: it converts every fact of the key, on every root, suspended roots included.

## What a Key Holds

```
$ canon facts convert DateTimeOriginal
content.DateTimeOriginal (whole index, read-only)
Type: text
Holds:
  text  5 facts, the value of 5 sources: 4 counted by `canon facts --global`, 1 on a suspended root
        most frequent: "0000:00:00 00:00:00" ×2 facts, "2021:03:02 18:44:10" ×1 fact, "2021:03:02 18:50:00" ×1 fact
`canon facts --key content.DateTimeOriginal --global --limit 0` to see the values `canon facts` counts

Can convert to:
  time    the 5 text facts: 3 unchanged, 2 removed
          `|datetime` asks for these values as times without changing anything stored
  number  the 5 text facts: 5 removed
          most of these are not numbers — text on purpose? `|number` asks for these values as numbers without changing anything stored

`canon facts convert content.DateTimeOriginal --to time` to preview
`canon facts convert content.DateTimeOriginal --to number` to preview
```

## The Preview

```
$ canon facts convert DateTimeOriginal --to time
content.DateTimeOriginal → time (whole index, dry run): 5 text facts, the value of 5 sources — 1 of them not counted by `canon facts --global`: 1 on a suspended root
  unchanged 3   spelling only, e.g. 2021:03:02 18:44:10 → 2021-03-02T18:44:10
  removed   2
      "0000:00:00 00:00:00" is not a time: ×2 facts, the value of 2 sources: 1 counted by `canon facts --global`, 1 on suspended root /path/to/backup (id:2)
  After changing the type, the lost and removed values cannot be reimported as your tool wrote them until this key's type is converted back.
  After: untyped dates imported for this key are stored as times.
  Aliases naming this key: @taken.
  Manifests whose naming pattern uses this key place files by the new values at their next apply.
`canon facts convert content.DateTimeOriginal --to time --list` to see each fact with its source (not kept after)
`canon facts convert content.DateTimeOriginal --to time --allow removal=2 --yes` to convert
  add --reason '<why>' to record why
Dry run: nothing changed.
```

Every fact falls in one class:

| Class | Meaning | Needs |
|---|---|---|
| unchanged | The same value; a change of spelling only | nothing |
| lossy | Converts, but loses something the value carried | `--allow loss=N` |
| removed | Cannot be converted to the target type | `--allow removal=N` |

**Text to time** ([How Canon interprets a time](../../concepts/facts.md#how-canon-interprets-a-time)):

| Stored text | Class |
|---|---|
| `2024:07:23 11:06:32`, `2024-07-23T11:06:32+02:00`, `2024-07-23T09:06:32Z`, `2024:07:23 11:06:32.45` | unchanged |
| `2024-07-23`, `2024-07`, `2024`, `2024-07-23 11:06` | unchanged, at its own precision |
| `0000:00:00 00:00:00`, `2024-02-30`, `misc`, `1690000000` | removed |

**Text to number**:

| Stored text | Class | Becomes |
|---|---|---|
| `12`, `2.5`, `1e5` | unchanged | 12, 2.5, 100000 |
| `2.50` | lossy (a trailing decimal zero) | 2.5 |
| `01234` | lossy (a leading zero) | 1234 |
| `14.2.1`, `misc` | removed | |

**Any type to text** is always unchanged: a number in its shortest spelling, a time in the spelling [`worklist --emit`](../enrich/worklist.md#emitting-existing-facts) writes.

```
$ canon facts convert DateTimeOriginal --to time --list
removed	0000:00:00 00:00:00		/path/to/photos/c.jpg	
removed	0000:00:00 00:00:00		/path/to/backup/d.jpg	suspended
```

## Converting

`--yes` converts, all or nothing. Give each lossy and removed count as the preview shows it:

```
$ canon facts convert DateTimeOriginal --to time --allow removal=2 --yes --reason "the tool wrote dates as text"
content.DateTimeOriginal → time: 3 unchanged, 2 removed
  whole index: 5 text facts, the value of 5 sources — 1 of them not counted by `canon facts --global`: 1 on a suspended root
  removed: "0000:00:00 00:00:00" ×2 facts
content.DateTimeOriginal is of type time.
`canon trail show 5` to read this conversion later
```

## After the Conversion

Converting back restores every unchanged value, though not always its original spelling. Lost and removed values come back by importing them again.
