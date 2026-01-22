# Setup

## Installation

Install Canon using Cargo:

```bash
cargo install --path .
```

Or build and copy to a directory in your `$PATH`:

```bash
cargo build --release
cp target/release/canon /usr/local/bin/
```

## Database

Canon stores all state in a SQLite database. The default location is `~/.canon/canon.db`.

You can override this with the `--db` flag:

```bash
canon --db /path/to/custom.db scan ...
```

The database is created automatically on first use. It contains:

- Registered roots and their scan state
- All indexed sources with metadata
- Content hashes and object references
- Imported facts from enrichment

## Verify Installation

```bash
canon --help
```

You should see the list of available commands. You're ready to start [scanning your files](getting-started.md).
