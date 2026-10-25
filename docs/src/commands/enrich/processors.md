# Writing Processors

Processors are scripts or programs that read worklist entries, extract metadata from files, and output facts for import.

## Input and Output

A processor reads JSONL from `worklist` and writes JSONL for `import-facts`.

**Input** (from worklist):
```json
{"source_id":123,"path":"/photos/IMG_001.jpg","basis_rev":0,"size":1024,"mtime":1703980800}
```

**Output** (for import-facts):
```json
{"source_id":123,"basis_rev":0,"facts":{"Make":"Apple","Model":"iPhone 12"}}
```

The processor must pass through `source_id` and `basis_rev` unchanged.

## Custom Processors

Read JSONL from stdin, extract facts from each file, output JSONL to stdout:

```bash
#!/bin/bash
while IFS= read -r line; do
  source_id=$(echo "$line" | jq -r '.source_id')
  basis_rev=$(echo "$line" | jq -r '.basis_rev')
  path=$(echo "$line" | jq -r '.path')

  # Extract facts (example: EXIF data)
  facts=$(exiftool -json -Make -Model "$path" 2>/dev/null | jq '.[0]')

  jq -nc \
    --argjson source_id "$source_id" \
    --argjson basis_rev "$basis_rev" \
    --argjson facts "$facts" \
    '{source_id: $source_id, basis_rev: $basis_rev, facts: $facts}'
done
```

## The canonargs Helper

`canonargs` handles the JSONL parsing and output formatting; you provide a command that extracts data from a single file.

### Installation

```bash
cargo install canonargs
```

### Single Fact Mode

When your command outputs a single value:

```bash
canon worklist | canonargs --fact mime -- file -b --mime-type {} | canon import-facts
```

The `{}` is replaced with the file path. The command's stdout becomes the fact value.

**Default behavior:** Each value is passed on as a string, and the import reads it as the key's type; for a new key, it is stored as text. To specify a type, add `--type`:

```bash
# Store as datetime (enables |year, |month modifiers)
canon worklist | canonargs --fact DateTimeOriginal --type datetime -- exiftool -DateTimeOriginal -s3 {} | canon import-facts

# Store image width as number (using ImageMagick's identify)
canon worklist | canonargs --fact width --type number -- identify -format '%w' {} | canon import-facts
```

Valid types: `datetime`, `duration`, `number`

With `--type number`, a value written as a JSON number is passed on exactly as written, so the import sees the number the command printed. Any other value is passed on with its `number` hint. A `number` hint is the tool's statement that the value is a number, so its spelling is not judged for loss: for a number key or a new key, `01234` is stored as `1234`, and the first such value makes a new key a number key, while the same text with no hint is skipped for a number key as losing what it carried. Send zip codes and other zero-padded codes without `--type number`.

### Key-Value Mode

When your command outputs `key=value` pairs (one per line):

```bash
canon worklist | canonargs --kv -- my-extractor {} | canon import-facts
```

**Default behavior:** Each value is passed on as a string, and the import reads it as the key's type; for a new key, it is stored as text. To specify types, use `key:type=value` syntax:

```
width:number=1920
height:number=1080
DateTimeOriginal:datetime=2024:07:23 14:30:00
codec=h264
```

### JSON Mode

When your command outputs a JSON object:

```bash
canon worklist | canonargs --json -- exiftool -json {} | canon import-facts
```

Example extractor output:
```json
{"Make": "Apple", "Model": "iPhone 12", "DateTimeOriginal": "2024:07:23 14:30:00"}
```

Each value is passed on exactly as the command wrote it. A JSON number such as `"width": 1920` is a number for a number key or a new key. A quoted string such as `"width": "1920"` is read as the key's type, and is stored as text for a new key.

For a time field on a new key, use the typed hint format; a key that already holds times reads a date string as a time:
```json
{"DateTimeOriginal": {"value": "2024:07:23 14:30:00", "type": "datetime"}}
```

### Chaining

Processors can be chained since `canonargs` passes through the worklist entry and merges facts:

```bash
canon worklist \
  | canonargs --fact mime -- file -b --mime-type {} \
  | canonargs --json -- exiftool -json {} \
  | canon import-facts
```

The facts `canonargs` passes through include every field `worklist --emit` wrote, so they are imported again under their names as given. A fact the command writes replaces a passed-through fact of the same name; under another name for the same key, such as `Model` beside `content.Model`, the import takes neither (see [What Is Skipped](import-facts.md#what-is-skipped)). A source fact such as `source.mtime` is not imported, and is counted in the summary. A [built-in key](../../reference/builtins.md#built-in-keys) such as `filename` or `size` is stored as a content fact, `content.filename`, on the content of a hashed source, so every copy of that content shares it; a filter on `filename` still reads the built-in key.

## Using Existing Facts

Processors can read a key's value, an imported fact or a source fact, via the `--emit` flag on worklist. See [Emitting Existing Facts](worklist.md#emitting-existing-facts) for details.

## Resuming a Pass

A `--where` gate decides which sources a pass still needs. Gate on a fact the processor emits for every file it handles, not on the fact you are after.

A processor that emits a capture time only when the file carries one leaves every other file matching `NOT content.media.capture_datetime?` permanently, so each pass reprocesses them:

```bash
# Reprocesses files that have no capture time, every run
canon worklist --where 'NOT content.media.capture_datetime?'

# Converges: this processor emits media.width for every file it reads
canon worklist --where 'NOT content.media.width?'
```

Gates are per fact vocabulary, so a processor covering several kinds of file needs one branch per kind:

```bash
canon worklist --global --include archived --where '
  ((mime ~ image/* OR mime ~ video/*) AND NOT content.media.width?)
  OR (mime ~ audio/* AND NOT content.audio.duration?)
'
```

When no output is unconditional, emit a marker for every file handled, including files nothing was found in:

```json
{"source_id":123,"basis_rev":0,"facts":{"exif.scanned":"13.55"}}
```

`NOT content.exif.scanned?` then selects exactly the unprocessed sources, and storing the tool version lets a later pass reselect on it.

A value the import skips at every run, such as the empty EXIF date `0000:00:00 00:00:00` for a key that holds times, leaves its file selected by a `NOT <key>?` gate on that key every time. A gate on a fact the processor writes for every file selects it once.

The `enriched?` predicate does not serve as a gate: it is true as soon as a source has any fact beyond the content hash, including facts from a different processor.

## Type Hints

The stored type of a fact determines how a filter compares it: times as spans of time, numbers as numbers, text in character order. A key's type is set by its first import, and every later value is read as that type ([The Key Decides](import-facts.md#the-key-decides)). So hints matter for a key's first values: a date string, or a number in a quoted string, with no hint makes a new key a text key. A filter then gives an error for `--where 'width>1000'` against it, and a date or number question has to be asked through a modifier (`width|number>1000`, `DateTimeOriginal|year=2024`). `canon facts convert` changes the type of a key already imported with the wrong one.

See [import-facts](import-facts.md#type-hints) for the hint format and full details.

### Writing Times

Write each time in the form that says what it is. Canon takes exactly what it is given ([How Canon interprets a time](../../concepts/facts.md#how-canon-interprets-a-time)), so a timestamp's correctness is the processor's job: the right offset, a clock that was set right, local time never labelled as UTC.

```json
{"source_id":123,"basis_rev":0,"facts":{
  "media.capture_datetime": {"value": "2024-06-15T14:30:05+02:00", "type": "datetime"},
  "media.create_datetime": {"value": "2024-06-15T12:30:05-00:00", "type": "datetime"},
  "media.modify_datetime": {"value": "2024:06:15 14:30:05", "type": "datetime"},
  "event.date": {"value": "2024-06-15", "type": "datetime"}
}}
```

| Written as | When |
|---|---|
| `2024-06-15T14:30:05+02:00` | The offset of the clock that took the time is known. Where that offset is UTC, write `Z` or `+00:00` |
| `2024-06-15T12:30:05-00:00` | Only the UTC instant is known, as in a field the format defines as UTC, which much video metadata is |
| `2024:06:15 14:30:05` | A time with no offset, as most EXIF dates are |
| `2024-06-15` | Only the date is known; `2024-06` and `2024` likewise for a month or a year |

In your local time, `2024-06-15T14:30:05+02:00` and `2024:06:15 14:30:05` can print alike, but the true form still matters. A time with no offset is assumed to be in your time zone: a camera that wrote no offset, with its clock set to local time on a trip, sits hours away from phone photos of the same moment that wrote theirs. A processor that knows the place, from GPS or otherwise, puts them on one timeline by writing the offset. And Canon keeps the form it is given: [`worklist --emit`](worklist.md#emitting-existing-facts) hands each time to the next processor in the form it was written.

## Tagging Files with Finder Tags (macOS)

While browsing files during archiving work, you can assign macOS Finder tags to classify them on the spot. Canon can then import those tags as facts, making them queryable and usable for clustering.

### The Workflow

1. **Browse and tag in Finder.** Right-click files (or select multiple) and assign tags such as "vacation", "kids", or "junk".

2. **Import tags into Canon:**
   ```bash
   canon worklist Photos/2011 | ./scripts/tag-worklist.sh | canon import-facts
   ```

3. **Query by tags:**
   ```bash
   canon ls --where 'tag.vacation?'                         # files tagged "vacation"
   canon ls --where 'tag.vacation? AND tag.kids?'            # both tags
   canon ls --where 'tag.vacation? AND NOT tag.kids?'        # vacation without kids
   canon facts                                               # see all tag.* keys with counts
   ```

4. **Cluster and archive by tag:**
   ```bash
   canon cluster generate --where 'tag.vacation?' --dest /Archive/Media/2011/Vacation ...
   ```

### How It Works

The `tag-worklist.sh` script reads macOS extended attributes (`com.apple.metadata:_kMDItemUserTags`) from each file. Each Finder tag becomes a fact key like `tag.vacation` or `tag.kids`. The tag name is normalized to lowercase with special characters replaced by underscores.

Tags are presence-based: query them with the `?` (exists) operator, not by value. `tag.vacation?` matches files tagged "vacation", and composes with AND/OR/NOT like any other filter expression.

### Why This Matters

A folder of mixed content often belongs in different places in the archive. Tags let you classify files while previewing them in Finder; the imported tags then drive `--where` filters and clustering to route each part to its destination.

## Tips

- Always pass through `source_id` and `basis_rev` unchanged
- Use `jq -c` for compact JSON output (one object per line)
- Handle errors gracefully—skip files that can't be processed
- Use type hints for a new key's dates, so its first value makes it a time key
- Write numbers as JSON numbers, not quoted strings, so a new key becomes a number key
