# canon ledger reindex

Rebuild the [extraction ledger](../../concepts/decisions.md#the-extraction-ledger--the-trails-outbound-direction) from `apply` receipts already on disk. The ledger is the index behind the `canon trail` lines showing what went out.

```bash
# Preview what would be indexed
canon ledger reindex --dry-run

# Rebuild
canon ledger reindex
```

## When to run it

Run it after restoring a database from an older backup, because recent `apply` decisions may be missing their extraction rows.

It changes only the extraction ledger, and the report is the only record of the run.

## What it does

`reindex` reads the receipt of every `apply` decision and aggregates its items into extraction rows the way a live `apply` does. A decision it cannot index is reported under one of these labels:

- **no receipt**: the decision records no receipt location (`--no-receipt`, receipts off for that run, or a receipt whose write never completed)
- **unreachable**: a receipt was recorded, but its root is gone or offline or the file is missing; retried on the next run
- **malformed**: the receipt fails an integrity check (bad TOML, a decision id that doesn't match); skipped

```
$ canon ledger reindex
Ledger reindex: extraction index
Scanned 3 apply decisions.

  indexed:  1 decisions (1 rows)
  already current:  0
  no receipt:  1
    #7  --no-receipt
  unreachable:  1
    #8  receipt file missing: /Volumes/Archive/.canon-ledger/2016/000008-apply.toml
  malformed:  0

Unreachable receipts are retried on the next run.
```

## Exit status

Exits nonzero only when nothing at all could be processed.
