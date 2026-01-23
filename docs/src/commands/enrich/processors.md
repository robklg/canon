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

If you don't want to handle JSONL parsing and output formatting yourself, `canonargs` takes care of that. You only provide a command that extracts data from a single file.

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

**Important:** The type of a fact determines what operations work on it:

- **Timestamps** enable `|year`, `|month` modifiers and date comparisons (`>=2024-01-01`)
- **Numbers** enable numeric comparisons (`>1000`) and `|bucket` modifier
- **Text** enables string matching and `|lowercase`, `|stem` modifiers

If your processor outputs dates as strings or numbers as strings, add type hints:

```json
{"source_id":123,"basis_rev":0,"facts":{
  "DateTimeOriginal": {"value": "2024:07:23 11:06:32", "type": "datetime"},
  "duration": {"value": "1:23:45", "type": "duration"},
  "width": 1920
}}
```

Without `"type": "datetime"`, a date string like `"2024:07:23 11:06:32"` is stored as text and `--where 'DateTimeOriginal|year=2024'` won't work.

Numbers from JSON are automatically stored as numbers. But if your extractor outputs `"width": "1920"` (a string), numeric comparisons like `--where 'width>1000'` won't work as expected.

See [import-facts](import-facts.md#type-hints) for full details.

## Tips

- Always pass through `source_id` and `basis_rev` unchanged
- Use `jq -c` for compact JSON output (one object per line)
- Handle errors gracefully—skip files that can't be processed
- Use type hints for datetime fields so modifiers work correctly
- Ensure numbers are actual JSON numbers, not quoted strings
