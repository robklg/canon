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

**Default behavior:** Values are stored as text. To specify a type, add `--type`:

```bash
# Store as datetime (enables |year, |month modifiers)
canon worklist | canonargs --fact DateTimeOriginal --type datetime -- exiftool -DateTimeOriginal -s3 {} | canon import-facts

# Store image width as number (using ImageMagick's identify)
canon worklist | canonargs --fact width --type number -- identify -format '%w' {} | canon import-facts
```

Valid types: `datetime`, `duration`, `number`

### Key-Value Mode

When your command outputs `key=value` pairs (one per line):

```bash
canon worklist | canonargs --kv -- my-extractor {} | canon import-facts
```

**Default behavior:** All values are stored as text. To specify types, use `key:type=value` syntax:

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

**JSON mode auto-detects numbers.** If your command outputs `"width": 1920` (a JSON number), it's stored as a number. If it outputs `"width": "1920"` (a quoted string), it's stored as text.

For datetime fields, you still need to use the typed hint format:
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

## Using Existing Facts

Processors can access previously imported facts via the `--emit` flag on worklist. See [Emitting Existing Facts](worklist.md#emitting-existing-facts) for details.

## Type Hints

The stored type of a fact determines what operations work on it: timestamps enable date modifiers and comparisons, numbers enable numeric comparisons and `|bucket`, text enables string matching and string modifiers. If your processor outputs dates as strings, or numbers as quoted strings, add type hints; without them, queries like `--where 'DateTimeOriginal|year=2024'` or `--where 'width>1000'` won't work.

See [import-facts](import-facts.md#type-hints) for the hint format and full details.

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
- Use type hints for datetime fields so modifiers work correctly
- Ensure numbers are actual JSON numbers, not quoted strings
