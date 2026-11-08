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

`reindex` reads the receipt of every `apply` decision and aggregates its items into extraction rows the way a live `apply` does. It rebuilds a decision's rows only for the roots the decision archived from that are still indexed, so a root added later is never recorded as the origin of an earlier decision's files, even at the same path. The exception is a decision recorded before the extraction ledger existed: it has no record of its roots, so its receipt is matched by path against the roots indexed now.

A decision it does not index is reported under one of these labels:

- **skipped (roots removed or retired)**: every root the decision archived from has been removed or retired; its rows stay as they were
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

Exits nonzero only when no decision was indexed, already current or skipped.
