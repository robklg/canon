# Decision Provenance

Canon silently records every effectful action you take — scans, exclusions, applies, and more. These **decision records** build a durable trail of what happened, when, and optionally why.

## What Gets Recorded

Every command that changes state writes a decision record:

| Command | What it records |
|---------|----------------|
| `scan` | Directory indexing |
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

## Annotating Decisions with `--reason`

Attach a short reason to explain *why* you're taking an action:

```bash
canon exclude set --where 'source.ext=dll' --reason "OS system files, no personal value"
canon apply manifest.toml --reason "Italy 2016 — assembled from three drives"
canon scan /mnt/old-laptop --reason "Deleted duplicate movies, originals confirmed in archive"
```

`--reason` is available on: `exclude set`, `exclude clear`, `exclude duplicates`, `exclude set-object`, `apply`, `scan`, `roots rm`.

When not provided, no reason is stored — no prompting, no friction.

For `apply`, manifest notes (from the `# === Notes ===` section) automatically become the reason when `--reason` is not explicitly provided.

## Suppressing Recording with `--no-record`

For bulk mechanical operations where recording would clutter the trail:

```bash
canon exclude set --where 'source.ext=dll' --no-record
canon exclude set --where 'source.ext=sys' --no-record

# This one matters — record it
canon exclude set --where 'source.ext=exe' --reason "Old game executables, keeping saves only"
```

`--no-record` is a global flag available on all commands. Per-invocation only — not a persistent setting.

## When Recording Does Not Happen

- **Dry-run** (`--dry-run`): No side effects occurred, so nothing to record
- **Declined confirmation**: User said "n" at the prompt
- **Validation failure**: Command failed before any work began
- **`--no-record`**: User explicitly suppressed recording
