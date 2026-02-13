# Setup

## Installation

Install Canon from crates.io:

```bash
cargo install canon-archive
```

This installs the `canon` binary.

### From Source

Alternatively, build from source:

```bash
git clone https://github.com/robklg/canon.git
cd canon
cargo install --path .
```

## Canon Home Directory

Canon stores all state in a single directory called the **canon home**. The default location is `~/.canon/`.

It contains:

| File | Purpose |
|------|---------|
| `canon.db` | SQLite database (roots, sources, objects, facts) |
| `aliases.toml` | Filter aliases (optional — see [Aliases](reference/filter.md#aliases)) |

The directory is created automatically on first use.

### Overriding the Location

You can relocate canon home with the `CANON_HOME` environment variable or the `--canon-home` flag:

```bash
# Via environment variable
export CANON_HOME=/mnt/archive/.canon
canon scan /photos

# Via flag (takes precedence over environment variable)
canon --canon-home /tmp/test-canon scan /photos
```

Precedence: `--canon-home` flag > `CANON_HOME` env var > `~/.canon/`

## Verify Installation

```bash
canon --help
```

You should see the list of available commands. You're ready to start [scanning your files](getting-started.md).
