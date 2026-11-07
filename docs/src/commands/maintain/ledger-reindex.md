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

The extraction ledger records, for each decision, the roots its files were archived from. Reindex
rebuilds that record only for those roots, and only while they are indexed. A decision whose roots
have all been removed or retired is counted under **skipped (roots removed or retired)**, and its
record is unchanged. A root added later is never recorded as the origin of an earlier decision's
files, whether its path is the removed root's path or a directory above or below it. A decision
recorded before the extraction ledger existed has no record of its roots, so its receipt is matched
by path against the roots indexed now.

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

Exits nonzero only when no decision was indexed, already current or skipped.
