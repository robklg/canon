# Decision Provenance

A decision is Canon's record of an effectful action, kept in two artifacts:

- **Record**: a queryable row in Canon's database: what happened, when, optionally why
- **Receipt**: a durable TOML file on disk with the per-item detail: every file the action touched, with its content hash, size, and modification time

A file [archived, excluded or deleted](#transitions) can be traced back to the action that put it in that state, from the files alone.

## What Gets Recorded

These commands write a decision record:

| Command | What it records |
|---------|----------------|
| `scan` | Directory indexing; files gone missing (deletion) |
| `apply` | File archiving |
| `exclude set/clear/duplicates` | Source triage |
| `exclude set-object/clear-object` | Object-level triage |
| `cluster generate/refresh` | Manifest creation |
| `roots rm/suspend/unsuspend` | Structural changes |
| `roots retire` | Retirement |
| `import-facts` | Enrichment |
| `prune` | Data cleanup |
| `facts delete` | Fact removal |
| `facts convert` | A key's change of type |
| `note clear` | Note deletion |

## What a Record Contains

- **Command**: stable identifier (e.g., `exclude_set`, `apply`)
- **Scope**: paths the command operated on
- **Command line**: the full command as typed
- **Reason**: optional user annotation (via `--reason`)
- **Status**: `started`, `completed`, `partial`, `interrupted`, or `refused`
- **Counts**: attempted, completed, failed, skipped
- **Summary**: the completion message you saw
- **Canon version**: which version produced the record
- **Timestamp**: when the command started
- **Time zone**: the time zone the command used for times

### The time zone a decision uses

A decision records the time zone its command used for times: your time zone when it ran, or, for `cluster generate`, `cluster refresh` and `apply`, the zone the manifest records ([the manifest's time zone](../commands/archive/cluster.md#the-manifests-time-zone)). [`trail show`](../commands/query/trail.md#inspecting-a-decision-trail-show) shows it as `time zone:`, and a receipt has it as `zone` in `[meta]`.

## Transitions

A transition is the change in a source's standing that an action made or observed: `archived`, `excluded`, `restored` (an exclusion undone) or `deleted`. Receipts and `canon trail` use the same words.

## Two-Phase Recording

1. **Start**: A "started" record is written after you confirm (or just before execution for commands without confirmation)
2. **Complete**: The record is updated with the outcome after execution finishes

A run that a pre-flight check stops before anything is touched records `refused`. One cut short after work began records `interrupted` or `partial`.

A row left `started` means its outcome was never written: the run was killed or crashed, or the write failed.

## Receipts

Receipts live in a `.canon-ledger/` directory under a root, each named for its decision's id:

```
.canon-ledger/000042-exclude_set.toml
.canon-ledger/Media/2016/000041-apply.toml
.canon-ledger/000043-scan.toml
```

A receipt sits where the action took effect:

- **Apply**: mirrors the destination path under the archive root's `.canon-ledger/`
- **Exclusion**: in the archive ledger root's `.canon-ledger/`
- **Deletion**: in the `.canon-ledger/` of the source root where the files were lost.

### Anatomy of a receipt

Every receipt's `[meta]` block states what happened, to what, and where:

```toml
[meta]
receipt_version = 1
decision_id = 142
command = "scan"
transition = "deleted"
posture = "observed"
status = "completed"
# ...summary, canon_version, command_line...
zone = "Europe/Amsterdam"

[meta.locus]
path = "/mnt/old-drive/photos"
id = 3
```

- **`transition`**: see [Transitions](#transitions)
- **`posture`**: `observed` for a scan-detected deletion, `performed` for every other receipt
- **`[meta.locus]`**: the root holding the receipt: its `path` when the receipt was written, and its database `id`
- **`zone`**: the [time zone the decision records](#the-time-zone-a-decision-uses)
- **`origin_disposition`**: apply receipts only: `retained` (a copy) or `relocated` (a move)

### The provenance chain

Every source carries a `decision_id`, naming the action that last changed its state. When an action changes a file an earlier one touched, the receipt records the earlier decision as `previous_decision_id`. Because that id is also its receipt's filename, you can walk the chain backwards from the files on disk alone.

## Recording Modes

What Canon writes is controlled by `ledger.recording` in `$CANON_HOME/config.toml`:

| Mode | Database record | Receipt file |
|------|-----------------|--------------|
| `Full` (default) | ✓ | ✓ |
| `Records` | ✓ | — |
| `Off` | — | — |

`ledger.layout` controls where apply receipts sit: `Central` (default) collects them under the archive root's `.canon-ledger/`; `Alongside` places them in a `.canon-ledger/` beside each destination directory.

```toml
[ledger]
recording = "Full"   # Full | Records | Off
layout = "Central"   # Central | Alongside
```

With no archive root, an exclusion is recorded without a receipt.

## Annotating Decisions with `--reason`

A short reason for an action:

```bash
canon exclude set --where 'source.ext=dll' --reason "OS system files, no personal value"
canon apply manifest.toml --reason "Italy 2016 — assembled from three drives"
canon scan /mnt/old-laptop --reason "Deleted duplicate movies, originals confirmed in archive"
```

`--reason` is available on: `exclude set`, `exclude clear`, `exclude duplicates`, `exclude set-object`, `apply`, `scan`, `roots rm`, `roots retire`, `roots suspend`, `roots unsuspend`, `facts convert`.

The reason is written into the decision record and into the receipt's `[meta]` when there is one.

For `apply`, manifest notes (from the `# === Notes ===` section) become the reason when `--reason` is not given.

## Suppressing Receipts with `--no-receipt`

To record a decision in the database but skip the receipt file for a single invocation:

```bash
canon exclude set --where 'source.ext=dll' --no-receipt
```

To turn recording off entirely, set `recording = "Off"` in `config.toml`.

## When Recording Does Not Happen

Nothing is recorded on a `--dry-run`, on a declined confirmation, or under `recording = "Off"`.

## Reading the Trail

[`canon trail`](../commands/query/trail.md) shows the record.

## The Extraction Ledger — the Trail's Outbound Direction

The **extraction ledger** is the index of what each `apply` transferred out of each source root: files, bytes, and whether the originals remain. Per-item detail stays in the apply receipt. A row stays readable after its source root is removed. [`canon trail`](../commands/query/trail.md) reads it to show what went out.

### Disk is truth, the database is a rebuildable index

The database is an index over the receipts. [`canon ledger reindex`](../commands/maintain/ledger-reindex.md) rebuilds the extraction ledger from the apply receipts on disk. A decision with no receipt cannot be recovered.

## The Composition Card — State, Not Events

The composition card appears at the bottom of a `canon trail` of one place (see [Reading the Trail](../commands/query/trail.md#the-composition-card-whats-standing-here)). It says what the place is made of now, based on the surviving sources. Origin, on the card, means the source root an `apply` took the content from.

## Crossing In, Crossing Out, Staying Put

An action has two ends: where content came from, and where it ended up. A place may put a boundary between them:

| Origin | Destination | Seen from this place |
|---|---|---|
| inside | outside | **left** — content was archived out of here |
| outside | inside | **arrived** — content came in |
| inside | inside | **rearranged** — content was transferred, but crossed nothing |

Standing at an archive root, a curation pass shows as a rearrangement; standing in the destination directory, the same pass shows as an arrival. In any one place, no file is counted twice.

A transfer across the boundary of a place is a **crossing**. [`canon trail crossings`](../commands/query/trail-crossings.md) shows them.

A crossing relates two places by what was transferred between them. The sweep's **counterpart** relates them by where matching content stands.
