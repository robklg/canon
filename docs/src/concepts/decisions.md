# Decision Provenance

Canon silently records every effectful action you take — scans, exclusions, applies, and more. Each decision leaves two linked artifacts: a queryable **record** in the database and a durable **receipt** file on disk. Together they build a trail of what happened, when, optionally why, and — crucially — *which files specifically*.

## What Gets Recorded

Every command that changes state writes a decision record:

| Command | What it records |
|---------|----------------|
| `scan` | Directory indexing; files gone missing (deletion) |
| `apply` | File archiving |
| `exclude set/clear/duplicates` | Source triage |
| `exclude set-object/clear-object` | Object-level triage |
| `cluster generate/refresh` | Manifest creation |
| `roots rm/suspend/unsuspend` | Structural changes |
| `import-facts` | Enrichment |
| `prune` | Data cleanup |
| `facts delete` | Fact removal |
| `note clear` | Note deletion |

Read-only commands (`ls`, `facts`, `coverage`, `survey`, `compare`, `worklist`) do not record.

## What a Record Contains

Each decision captures:

- **Command** — stable identifier (e.g., `exclude_set`, `apply`)
- **Scope** — paths the command operated on
- **Command line** — the full command as typed
- **Reason** — optional user annotation (via `--reason`)
- **Status** — `started`, `completed`, `partial`, or `interrupted`
- **Counts** — attempted, completed, failed, skipped
- **Summary** — the completion message you saw
- **Canon version** — which version produced the record
- **Timestamp** — when the command started

## Two-Phase Recording

Recording happens in two phases:

1. **Start**: A "started" record is written after you confirm (or just before execution for commands without confirmation)
2. **Complete**: The record is updated with the outcome after execution finishes

If Canon is interrupted (Ctrl+C, crash, power loss), the "started" record survives — a durable trace that the operation was attempted.

## Records vs. Receipts

A decision has two artifacts:

- The **record** — a row in Canon's database (everything above). It answers *what happened, when, and why*, and it's queryable.
- The **receipt** — a durable TOML file written to a `.canon-ledger/` directory on disk, capturing the *per-item detail* the record only summarizes: every file the decision touched, with its content hash, size, and modification time.

The record is the index; the receipt is the evidence. Together they mean a file that reached one of its three terminal fates — **archived**, **excluded**, or **deleted** — can always be traced back to the decision that put it in that state — even years later, even from the files alone.

## Receipts

Receipts live in a `.canon-ledger/` directory under a root. Each is named for the decision that produced it, so the id in the filename links it straight back to the record:

```
.canon-ledger/000042-exclude_set.toml
.canon-ledger/Media/2016/000041-apply.toml
.canon-ledger/000043-scan.toml
```

A receipt sits at the **locus of the action's effect**:

- **Apply receipts** are *targeted*: they mirror the destination path under the archive root's `.canon-ledger/`, sitting alongside the content they describe.
- **Exclusion receipts** are *flat*: they land directly in the archive ledger root's `.canon-ledger/`, since an exclusion isn't tied to any one destination.
- **Deletion receipts** are *source-local*: they land in the `.canon-ledger/` of the **source root where the files were lost** — physically on that drive, so the record of what a drive lost travels with the drive. A single scan that detects deletions across several roots writes one receipt per affected root, all under the one decision.

Each receipt records, per item: the source root and relative path, content hash, size, and modification time. Variants carry the shape of their decision — `exclude duplicates` groups items by content hash, recording which copy was **kept** versus **excluded**; object-level exclusions list every source sharing the content; a deletion receipt lists exactly the sources that went missing.

### The provenance chain

Every source carries a `decision_id` — the decision that last changed its state. When a decision changes a file a previous decision already touched, the receipt records that predecessor as `previous_decision_id`. Because the predecessor's id is also its receipt's filename, you can walk the chain backwards from the files on disk alone — no database required.

## Recording Modes

What Canon writes is controlled by `ledger.recording` in `$CANON_HOME/config.toml`:

| Mode | Database record | Receipt file |
|------|-----------------|--------------|
| `Full` (default) | ✓ | ✓ |
| `Records` | ✓ | — |
| `Off` | — | — |

`ledger.layout` controls where *targeted* (apply) receipts sit: `Central` (default) collects them under the archive root's `.canon-ledger/`; `Alongside` places them in a `.canon-ledger/` beside each destination directory. Layout does not affect exclusion or deletion receipts — those are always flat at their own `.canon-ledger/` root (the archive ledger root for exclusions, the source root for deletions).

```toml
[ledger]
recording = "Full"   # Full | Records | Off
layout = "Central"   # Central | Alongside
```

If no archive root is configured, exclusion decisions are still recorded, but no receipt can be written — Canon warns you so the gap is visible rather than silent. Deletion receipts have no such dependency: they live on the source root, which always exists, so culling files from a drive that was never archived is still recorded in full.

## Annotating Decisions with `--reason`

Attach a short reason to explain *why* you're taking an action:

```bash
canon exclude set --where 'source.ext=dll' --reason "OS system files, no personal value"
canon apply manifest.toml --reason "Italy 2016 — assembled from three drives"
canon scan /mnt/old-laptop --reason "Deleted duplicate movies, originals confirmed in archive"
```

`--reason` is available on: `exclude set`, `exclude clear`, `exclude duplicates`, `exclude set-object`, `apply`, `scan`, `roots rm`.

When not provided, no reason is stored — no prompting, no friction. When provided, the reason is written into both the decision record and the receipt's `[meta]`, so it travels with the durable artifact.

For `apply`, manifest notes (from the `# === Notes ===` section) automatically become the reason when `--reason` is not explicitly provided.

## Suppressing Receipts with `--no-receipt`

To record a decision in the database but skip the receipt file for a single invocation:

```bash
canon exclude set --where 'source.ext=dll' --no-receipt
```

`--no-receipt` is a global flag, per-invocation only — not a persistent setting. Database recording still happens (per the recording mode above); only the receipt file is suppressed. To turn recording off entirely, set `recording = "Off"` in `config.toml`.

## When Recording Does Not Happen

- **Dry-run** (`--dry-run`): No side effects occurred, so nothing to record
- **Declined confirmation**: User said "n" at the prompt
- **Validation failure**: Command failed before any work began
- **`recording = "Off"`**: Recording disabled in `config.toml`

## Reading the Trail

[`canon trail`](../commands/query/trail.md) reads the record back: what happened at a place (`canon trail`), the day's story (`canon trail --today`), and any single decision in full with its receipt locations (`canon trail show <id>`). Notes interleave as the thinking between the actions.
