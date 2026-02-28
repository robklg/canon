# Maintenance

Commands for cleaning up and maintaining Canon's database.

These operations delete data from the database (never from disk). All are dry-run by default — use `--yes` to execute.

- [`facts delete`](facts-delete.md) - Remove incorrect or unwanted metadata
- [`prune`](prune.md) - Clean up stale, orphaned, or excluded data
