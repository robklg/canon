# Enriching

External processors add metadata to indexed files.

[`worklist`](worklist.md) outputs sources as JSONL, a processor extracts metadata, and [`import-facts`](import-facts.md) stores it.

```
canon worklist → processor → canon import-facts
```

A processor can be any CLI tool or script that extracts information from files: `exiftool` for EXIF data, `file` for MIME types, `ffprobe` for media info, or custom scripts. [Writing Processors](processors.md) shows how to build your own.

## Basic Usage

Extract EXIF metadata from images:

```bash
canon worklist --where 'source.ext|lowercase IN (jpg, jpeg, heic)' \
  | ./scripts/exif-worklist.sh \
  | canon import-facts
```

Detect MIME types for all files:

```bash
canon worklist | canonargs --fact mime -- file -b --mime-type {} | canon import-facts
```

## Provided Processors

| Processor | Purpose | Requires |
|-----------|---------|----------|
| `scripts/exif-worklist.sh` | EXIF, GPS, and media metadata | exiftool, jq |
| `scripts/hash-worklist.sh` | SHA-256 content hashes | jq |
| `scripts/tag-worklist.sh` | macOS Finder tags | jq, python3 |
| `canonargs --fact mime -- file -b --mime-type {}` | MIME type detection | canonargs |

Install `canonargs` with: `cargo install canonargs`

## Tip: Selective Hashing

To hash only specific file types, scan with `--no-hash` and hash through the pipeline:

```bash
canon scan --no-hash --add --role source /path/to/mixed-files
canon worklist /path/to/mixed-files --where 'source.ext|lowercase IN (jpg, jpeg, heic, mov, mp4)' \
  | ./scripts/hash-worklist.sh \
  | canon import-facts
```
