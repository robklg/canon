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
- **Exclusion receipts** are *flat*: they land directly in the archive ledger root's `.canon-ledger/`. Excluding doesn't touch the file — it's letting go: a conscious judgment that this content doesn't need archiving (or is redundant beside what already is), made safe by destroying nothing. That judgment must outlive the source drive it helps clear, so its receipt lives on the archive side — the medium that endures. And with no destination path to mirror, it sits flat.
- **Deletion receipts** are *source-local*: they land in the `.canon-ledger/` of the **source root where the files were lost** — physically on that drive, so the record of what a drive lost travels with the drive. A single scan that detects deletions across several roots writes one receipt per affected root, all under the one decision.

Each receipt records, per item: the source root and relative path, content hash, size, and modification time. Variants carry the shape of their decision — `exclude duplicates` groups items by content hash, recording which copy was **kept** versus **excluded**; object-level exclusions list every source sharing the content; a deletion receipt lists exactly the sources that went missing.

### Anatomy of a receipt

Every receipt's `[meta]` block states, in its own text, *what happened, to what, and where* — so a reader without Canon (future‑you, an heir opening a drive from a drawer, an external tool, an older binary) never has to infer semantics from the receipt's body shape or from a command name that may have been renamed since:

```toml
[meta]
receipt_version = 1
decision_id = 142
command = "scan"
transition = "deleted"
posture = "observed"
status = "completed"
# ...summary, canon_version, command_line...

[meta.locus]
path = "/mnt/old-drive/photos"
id = 3
```

- **`transition`** — the *what*, in fixed vocabulary: `archived`, `excluded`, `restored` (an exclusion undone), or `deleted`. This is the same word `canon trail` uses for the same action — the trail and the receipt tell one story in one vocabulary.
- **`posture`** — whether Canon **performed** the change or merely **observed** one the world made. A scan‑detected deletion is `observed`: Canon witnessed a loss, it did not cause one. Every other receipt today is `performed`.
- **`[meta.locus]`** — the identity of the root the receipt is anchored to, making its placement into data. Locus is the *receipt's* where, not the action's: an exclusion run in a source folder has its `scope` there, its per-item `root`s there — but its locus is the archive ledger root, because that is where the receipt itself lives. **`path`** is the root's canonical path captured at write time — authoritative for a human and for rebuilding an index from disk, and still meaningful after a drive is remounted elsewhere or a receipt is copied off its root. **`id`** is the join key against a live database. Both are always present.
- **`origin_disposition`** — apply receipts only: `retained` (a copy — the content now lives in two places) or `relocated` (a move — the origin no longer holds the file).

**The granularity rule.** Subjects that can span roots always carry their own per‑item root identity — apply items keep their `source_root`, exclusion and object entries keep their `root`. The **locus** root is always meta‑level. Receipt‑level‑only identity (no per‑item root, as in a deletion receipt) is valid exactly where single‑root‑ness is guaranteed by construction — a deletion receipt is coalesced to one root, so its items inherit the meta locus.

These fields are additive: receipts written before they existed remain valid, and every reader tolerates their absence.

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

## The Extraction Ledger — the Trail's Outbound Direction

Standing at a source location, the decisions above tell only half the story: deletions and exclusions, not what was *archived out of* the place. The **extraction ledger** is the outbound half — an aggregate index, `decision_extractions`, of what each `apply` drew from each source root: how many files, how many bytes, where they went, and whether the originals remain (copied) or are gone from here (moved).

It is deliberately **aggregate-only** — one row per (decision, source root), never a per-item copy. Per-item detail already lives in the apply receipt on disk; the ledger row exists so `canon trail` can answer "what left from here?" without re-reading every receipt on every scoped view.

The recorded paths are write-time snapshots, not live lookups — a row keeps telling its story after the source root has been removed from Canon. `canon trail show` marks such roots `(root removed)`, so a snapshot path never silently reads as a live, visitable location.

### Disk is truth, the database is a rebuildable index

This is the same principle that governs the rest of provenance: the database is a projection over receipts, not a second source of truth. If `decision_extractions` is ever lost — a fresh database, a restored backup missing recent rows, a manual mistake — it can be rebuilt from the receipts still sitting on disk:

```bash
canon ledger reindex
```

See [`ledger reindex`](../commands/maintain/ledger-reindex.md) for the full command. It walks every `apply` decision, reads its receipt (tolerating older receipts that predate today's self-describing fields), and rebuilds the same aggregate rows the forward `apply` path writes — a backfilled row is indistinguishable from a forward-recorded one, by construction (both go through the same aggregation).

Gaps are reported, never inferred: a decision with no receipt on disk (recording was off, or `--no-receipt` was used) is not something `reindex` can recover, and it says so rather than guessing.

## The Composition Card — State, Not Events

Everything above answers "what happened?" — the trail is an event log. The composition card, shown at the bottom of a scoped `canon trail` view (see [Reading the Trail](../commands/query/trail.md#the-composition-card-whats-standing-here)), answers a different question: "what is this place made of, right now?" It is a present-tense read of the surviving sources' own stamps, not a replay of the events that produced them.

The distinction matters because the two can honestly disagree. A location's "Arrived here" total (an event count) never shrinks — a decision that happened stays happened. Its "Standing here" total (a state count) can be smaller, when some of what arrived was later deleted or moved elsewhere. Origin, here, means where content entered Canon's custody from — the source root an `apply` drew it from — attributed at the same decision-level granularity as the extraction ledger above, since that ledger is the card's only source of origin data. It is not, and cannot be, a full per-item lineage; that is a distinct, still-undesigned surface.

The card is computed by its own operation (`ops::composition::compute_composition`), independent of trail rendering, so it can be reused wherever a present-tense "what's here?" reading is useful — the trail command is its first consumer, not its only intended one.

## Crossing In, Crossing Out, Staying Put

State versus events is one axis. There is a second, cutting across both the trail's rollups and the card: **whether content crossed the boundary of the place you are asking about.**

A decision has two endpoints — where content was drawn from, and where it landed. The scope you are viewing draws a boundary between them, or fails to:

| Origin | Destination | From this view |
|---|---|---|
| inside | outside | **left** — content was archived out of here |
| outside | inside | **arrived** — content came in |
| inside | inside | **rearranged** — content moved, but crossed nothing |

The third row is a real category, not an accounting artifact. Because roots cannot nest, a decision with both endpoints inside one view is always an `apply` **within a single archive root** — a curation pass re-placing content that was already in custody. It is not a filesystem rename; those are observed by `scan` and never write an extraction row at all.

Counting such a decision as both a departure and an arrival would read as double the activity, with both counterparty counts naming the very place you are standing. Silently dropping it would be worse — Canon's rule is that gaps explain themselves, and a user who has just done real curation work should see it accounted for. So it gets its own line, and its own word.

**The boundary moves when you move.** Standing at an archive root, a curation pass reads as a rearrangement; standing in the destination folder, the same pass reads as an arrival, because from there the origin genuinely is elsewhere. Neither reading is more true — they answer different questions. What is guaranteed is that within any one view, the categories are disjoint: no file is counted twice.

The word *rearranged* is chosen to state an observable fact about two paths and one boundary, without committing to what a curation pass *is*. It deliberately avoids `relocated`, which already means something narrower in Canon — whether an apply's originals moved or were copied (see `origin_disposition` above). An intra-archive apply can be either.
