# canon roots

List and manage registered [roots](../../concepts/roots.md).

Roots are added via [`scan`](scan.md).

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

# Suspend a root (out of view and not acted on, until you unsuspend)
canon roots suspend id:1
canon roots suspend path:/path/to/photos

# Unsuspend a root
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
ID   ROLE        FILES  LAST SCAN         PATH
1    source      16635  2h ago            /path/to/photos
2    archive    169941  5d ago            /path/to/archive
3    source       1234  never             /path/to/backup (Old backup, possibly duplicates)
```

## Suspending Roots

See [Suspended roots](../../concepts/roots.md#suspended-roots) for what suspension is. A suspended root is hidden from `canon roots` (use `--suspended` to list only suspended roots) and skipped by `scan --all`.

A report on a suspended place says so:

```
/path/to/photos suspended — set aside: here · canon roots unsuspend path:/path/to/photos or --global
```

With nothing else to list, the exit code is 1. An action shows the same sentence with `rejected` in place of `set aside`. A command that reads what Canon recorded says so once in its header:

```
Decision trail: /path/to/photos — /path/to/photos suspended · canon roots unsuspend path:/path/to/photos
```

| Commands | At a suspended place |
|---|---|
| [`ls`](../query/ls.md), [`worklist`](../enrich/worklist.md), [`facts`](../query/facts.md), [`coverage`](../archive/coverage.md), [`survey`](../query/survey.md) | Set aside. A live path named beside it still runs |
| [`sweep`](../query/sweep.md) | Leaves its places out of the ranking and says so in a footer |
| [`exclude`](../manage/exclude.md), [`cluster generate`](../archive/cluster.md) and `refresh`, [`apply`](../archive/apply.md) at either end, [`facts delete`](../maintain/facts-delete.md), [`note -m`](../manage/note.md) and `--clear`, [`scan`](scan.md), `roots rm`, [`roots retire`](retire.md#requirements), [`compare`](../query/compare.md), `survey --other` | Rejected by name before any plan or confirmation, so `--yes` and `--dry-run` state it too. Nothing is written |
| [`trail`](../query/trail.md), [`roots story`](story.md), `note`, `roots retire --dry-run` | Work as usual, and say the root is suspended |
| [`import-facts`](../enrich/import-facts.md) | Sets aside the records naming a suspended source and counts them in its summary |
| `exclude set-object`, [`facts delete --on object`](../maintain/facts-delete.md) | Reach every copy of the content, and state how many are on suspended roots |
| [`prune`](../maintain/prune.md) | Skips suspended roots |

## Removing Roots

Removing a root removes its sources, facts, and notes from the database, and deletes no files on disk. If you re-add a removed root, you'll need to re-enrich it. A root added again after removal is a new root, and its story starts empty. The earlier root's decisions stay in the trail: `canon trail --global --all` or a date view shows them. See [what survives removal](../../concepts/retirement.md#what-survives-what).

Removal is recorded as a [decision](../../concepts/decisions.md); add `--reason` to say why the root is going.

## Root Specs

Several commands accept root specifications in two formats:

| Format | Example | Description |
|--------|---------|-------------|
| `id:N` | `id:1` | By database ID (shown in `canon roots` output) |
| `path:/...` | `path:/path/to/photos` | By exact path |
