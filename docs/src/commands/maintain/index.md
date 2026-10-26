# Maintenance

Commands for cleaning up and maintaining Canon's database.

`facts delete`, `facts convert` and `prune` change data in the database (never on disk)
and are dry-run by default; use `--yes` to execute. `ledger reindex` writes by default: it
rebuilds an index by writing rows back from receipts on disk; use `--dry-run` to
preview instead.

- [`facts delete`](facts-delete.md) - Remove incorrect or unwanted metadata
- [`facts convert`](facts-convert.md) - Change a key's type
- [`prune`](prune.md) - Clean up stale, orphaned, or excluded data
- [`ledger reindex`](ledger-reindex.md) - Rebuild the extraction ledger from receipts on disk
