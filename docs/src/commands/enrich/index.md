# Enriching

Add metadata to indexed files using external processors.

Canon uses a pipeline model: [`worklist`](worklist.md) outputs sources as JSONL, an external processor extracts metadata, then [`import-facts`](import-facts.md) stores the results.

```
canon worklist → processor → canon import-facts
```

A processor can be any CLI tool or script that extracts information from files: `exiftool` for EXIF data, `file` for MIME types, `ffprobe` for media info, or custom scripts you write yourself.

## Basic Usage

Extract EXIF metadata from images:

```bash
canon worklist --where 'source.ext|lowercase IN (jpg, jpeg, heic)' \
  | ./scripts/exif-worklist.sh \
  | canon import-facts
```

Note the `--where` filter: it's usually smart to limit the worklist to files the processor can actually handle.

Detect MIME types for all files:

```bash
canon worklist | canonargs --fact mime -- file -b --mime-type {} | canon import-facts
```

After enrichment, the imported facts become available for filtering and querying.

## Provided Processors

Canon includes ready-to-use processors:

| Processor | Purpose | Requires |
|-----------|---------|----------|
| `scripts/exif-worklist.sh` | EXIF, GPS, and media metadata | exiftool, jq |
| `scripts/hash-worklist.sh` | SHA-256 content hashes | jq |
| `canonargs --fact mime -- file -b --mime-type {}` | MIME type detection | canonargs |

Install `canonargs` with: `cargo install canonargs`

## Going Deeper

- [`worklist`](worklist.md) - Full options for generating worklists
- [`import-facts`](import-facts.md) - Input format and type hints
- [Writing Processors](processors.md) - Build your own enrichment scripts

## Tip: Selective Hashing

Content hashing normally happens during `scan`. If you prefer to hash only specific file types, use `--no-hash` during scan and hash selectively via the pipeline:

```bash
canon scan --no-hash --add --role source /path/to/mixed-files
canon worklist --where 'mime~image/* OR mime~video/*' \
  | ./scripts/hash-worklist.sh \
  | canon import-facts
```
