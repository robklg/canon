# canon ledger reindex

Rebuild the [extraction ledger](../../concepts/decisions.md#the-extraction-ledger--the-trails-outbound-direction) from `apply` receipts already on disk. The ledger is the aggregate index behind `canon trail`'s "what left from here?" lines.

```bash
# Preview what would be indexed
canon ledger reindex --dry-run

# Rebuild
canon ledger reindex
```

## When to run it

- **After upgrading to a Canon that records placements at directory precision**: applies indexed by an older Canon are known only to a coarse common prefix (visible at that prefix and above, silent in deeper views). One reindex rebuilds them at full precision from their receipts.
- **After restoring a database** from an older backup: recent `apply` decisions may be missing their extraction rows even though their receipts survived on disk.
- **After manually clearing or losing rows** in `decision_extractions`.
- **As a periodic check**: it is idempotent and safe to run anytime; decisions already indexed converge to the same rows rather than duplicating.

It never touches receipts, decision records, or any other table; only `decision_extractions`. It writes no decision row of its own: rebuilding an index is not a content decision, so the printed report is the only record of the run.

## What it does

`reindex` walks every `apply` decision and, for each one, tries to read its receipt:

- The decision records no receipt location (`--no-receipt`, receipts off for that run, or a receipt whose write never completed) → reported as **no receipt**: nothing to recover from the row. The reason states what the row shows, not why — the row cannot tell those cases apart.
- A receipt was recorded but isn't reachable right now (its root is gone, offline, or the file itself is missing) → reported as **unreachable**, distinct from "no receipt": nothing is concluded from not being able to check today, and it is retried on the next run.
- The receipt reads but fails an integrity check (bad TOML, a decision id that doesn't match) → reported as **malformed**, skipped.
- The receipt reads cleanly → its items are aggregated into extraction rows, the same way a live `apply` does. If some item's source root is no longer recognized, that root is reported separately as a partial-index gap rather than silently dropped.

Every decision lands in exactly one bucket; the report is never silent about what it couldn't do.

```
$ canon ledger reindex
Ledger reindex: extraction index
Scanned 214 apply decisions.

  indexed:          182 decisions (317 rows)
  already current:  24
  no receipt:       6
    #12  no receipt location recorded
    #31  --no-receipt
  unreachable:      2
    #87   root path not present (offline?): /Volumes/archive-b
    #103  destination root #9 no longer known
  malformed:        0

Unreachable receipts are retried on the next run.
```

`--dry-run` prints the same report with "would index" phrasing and writes nothing.

## Exit status

Exits nonzero only when nothing at all could be processed: every scanned decision landed in `no receipt`/`unreachable`/`malformed` and nothing was indexed. A run that indexes at least one decision, even alongside gaps, exits 0; gaps are expected and self-explaining, not failure.
