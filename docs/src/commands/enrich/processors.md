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
cargo install --path canonargs/
```

### Single Fact Mode

When your command outputs a single value:

```bash
canon worklist | canonargs --fact mime -- file -b --mime-type {} | canon import-facts
```

The `{}` is replaced with the file path. The command's stdout becomes the fact value.

### Key-Value Mode

When your command outputs `key=value` pairs (one per line):

```bash
canon worklist | canonargs --kv -- my-extractor {} | canon import-facts
```

Example extractor output:
```
width=1920
height=1080
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

## Tips

- Always pass through `source_id` and `basis_rev` unchanged
- Use `jq -c` for compact JSON output (one object per line)
- Handle errors gracefully—skip files that can't be processed
