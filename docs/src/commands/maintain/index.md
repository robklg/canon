# Maintenance

Commands for cleaning up and maintaining Canon's database.

`facts delete`, `facts convert` and `prune` change data in the database (never on disk)
and are dry-run by default; use `--yes` to execute. `ledger reindex` writes by default;
use `--dry-run` to preview instead.

- [`facts delete`](facts-delete.md): remove incorrect or unwanted metadata
- [`facts convert`](facts-convert.md): change a key's type
- [`prune`](prune.md): clean up stale, orphaned, or excluded data
- [`ledger reindex`](ledger-reindex.md): rebuild the extraction ledger from receipts on disk
