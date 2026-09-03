# canon roots

List and manage registered [roots](../../concepts/roots.md).

Roots are added via [`scan`](scan.md) and managed with the `roots` command. You can list, suspend/unsuspend, add comments, or remove roots.

**Important notes:**
- Removing a root also removes its sources, facts, and notes from the database
- Removing a root does **not** delete any files on disk
- If you re-add a removed root, you'll need to re-enrich it

```bash
# List all roots with file counts and last scan time
canon roots

# List roots at or beneath a specific path
canon roots /path/to/photos

# List only suspended roots
canon roots --suspended

# Set a comment on a root (omit text to clear)
canon roots comment id:1 "Old backup, possibly duplicates"
canon roots comment id:1

# Suspend a root (hides from all operations without deleting data)
canon roots suspend id:1
canon roots suspend path:/path/to/photos

# Unsuspend a root (make visible again)
canon roots unsuspend id:1

# Remove a root by ID (files on disk are NOT deleted)
canon roots rm id:1

# Remove a root by path
canon roots rm path:/path/to/photos

# Skip confirmation prompt
canon roots rm id:1 --yes
```

Example output:
```
ID   ROLE       FILES  LAST SCAN         PATH
1    source     16635  2h ago            /path/to/photos
2    archive   169941  5d ago            /path/to/archive
3    source      1234  never             /path/to/backup (Old backup, possibly duplicates)
```

## Suspending Roots

Suspending a root sets it aside without changing anything in it. Its content stays indexed and keeps the standing it had, and `canon roots unsuspend` brings it back.

A suspended root is hidden from `canon roots` (use `--suspended` to list only suspended roots) and skipped by `scan --all`; scanning a path inside one is refused, naming the way back. Its sources are left out of what the query commands report on: [`ls`](../query/ls.md), [`facts`](../query/facts.md), [`compare`](../query/compare.md), [`survey`](../query/survey.md), [`coverage`](../archive/coverage.md) and [`worklist`](../enrich/worklist.md). [`sweep`](../query/sweep.md) keeps its places off the board and states the pause in a footer.

Two things a suspended root goes on doing. Its copies still count as evidence about content elsewhere, so content that also sits in a suspended archive root still reads as archived, both under `--where archived?` and in [`coverage`](../archive/coverage.md). And what Canon already recorded still reads: [`trail`](../query/trail.md), [`roots story`](story.md), [`note`](../manage/note.md) and bound books are unaffected.

Suspended roots still prevent overlapping (you cannot add a new root at a suspended root's path).

## Removing Roots

When removing a root, Canon shows how many sources are "in archive" (same content exists in an archive) vs "not in archive", and suggests using `canon ls <path>` to preview which sources will be forgotten.

The confirmation also states what removal means for the root's story. If no retirement artifact exists, Canon states that removal deletes the root's inventory, notes, and recorded fates, leaving the story unreviewable, and points at [`canon roots retire`](retire.md) as the way to bind it first. If the root was already retired, the line instead points at where its story is bound. The line never blocks: removal proceeds through the normal confirmation either way. See [what survives removal](../../concepts/retirement.md#what-survives-what).

Removal is itself recorded as a [decision](../../concepts/decisions.md); add `--reason` to say why the root is going. The root's sources, facts, and notes leave the database, but its recorded history survives: receipts already written to the root's `.canon-ledger/` stay on the storage, and past decisions keep rendering in [`canon trail`](../query/trail.md). An apply that drew content from the root still shows its path in `trail show`, marked `(root removed)`, because those records are write-time snapshots.

## Root Specs

Several commands accept root specifications in two formats:

| Format | Example | Description |
|--------|---------|-------------|
| `id:N` | `id:1` | By database ID (shown in `canon roots` output) |
| `path:/...` | `path:/path/to/photos` | By exact path |

```bash
canon roots suspend id:1
canon roots suspend path:/path/to/photos
```
