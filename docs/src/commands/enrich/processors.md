# Writing Processors

A processor: a script or program that reads worklist entries, extracts metadata from files, and outputs facts for import.

## Input and Output

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

`canonargs` ([installed](index.md#provided-processors) separately) handles the JSONL parsing and output formatting; you provide a command that extracts data from a single file.

### Single Fact Mode

When your command outputs a single value:

```bash
canon worklist | canonargs --fact mime -- file -b --mime-type {} | canon import-facts
```

The `{}` is replaced with the file path. The command's stdout becomes the fact value.

Each value is passed on as a string, and the import interprets it as the key's type; for a new key, it is stored as text. `--type` specifies a type:

```bash
# Store as datetime (enables |year, |month modifiers)
canon worklist | canonargs --fact DateTimeOriginal --type datetime -- exiftool -DateTimeOriginal -s3 {} | canon import-facts

# Store image width as number (using ImageMagick's identify)
canon worklist | canonargs --fact width --type number -- identify -format '%w' {} | canon import-facts
```

Valid types: `datetime`, `duration`, `number`

Send zip codes and other zero-padded codes without `--type number`.

### Key-Value Mode

When your command outputs `key=value` pairs (one per line):

```bash
canon worklist | canonargs --kv -- my-extractor {} | canon import-facts
```

Values are passed on as strings; `key:type=value` specifies a type:

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

Each value is passed on exactly as the command wrote it, and [the key decides](import-facts.md#the-key-decides) how it is stored.

### Chaining

Processors can be chained since `canonargs` passes through the worklist entry and merges facts:

```bash
canon worklist \
  | canonargs --fact mime -- file -b --mime-type {} \
  | canonargs --json -- exiftool -json {} \
  | canon import-facts
```

## Using Existing Facts

`worklist --emit` gives a processor a key's value: [Emitting Existing Facts](worklist.md#emitting-existing-facts).

## Resuming a Pass

A `--where` filter decides which sources a pass still needs. Filter on a fact the processor emits for every file it handles, not on the fact you are after.

A fact emitted only for some files leaves the others selected on every pass:

```bash
# Reprocesses files that have no capture time, every run
canon worklist --where 'NOT content.media.capture_datetime?'

# Converges: this processor emits media.width for every file it reads
canon worklist --where 'NOT content.media.width?'
```

A processor covering several kinds of file needs one branch per kind:

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

`NOT content.exif.scanned?` then selects the unprocessed sources, and storing the tool version lets a later pass reselect on it.

The `enriched?` predicate does not serve here: it is true as soon as a source has any fact beyond the content hash, including facts from a different processor.

## Type Hints

Hints matter for a key's first values: with no hint, a date string or a quoted number makes a new key a text key. See [import-facts](import-facts.md#type-hints) for the hint format and full details.

### Writing Times

Write each time in the form that says what it is.

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
| `2024-06-15T12:30:05-00:00` | Only the UTC instant is known, as in much video metadata |
| `2024:06:15 14:30:05` | A time with no offset, as most EXIF dates are |
| `2024-06-15` | Only the date is known; `2024-06` and `2024` likewise for a month or a year |

A time with no offset is assumed to be in your time zone, so a processor that knows the location, from GPS or otherwise, writes the offset.

## Tagging Files with Finder Tags (macOS)

Finder tags can be imported as facts, to classify files for queries and clusters.

### The Workflow

1. **Tag in Finder**: tags such as "vacation", "kids", or "junk".

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

Each Finder tag becomes a fact key like `tag.vacation`. Query tags with the `?` (exists) operator.
