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

A suspended root is hidden from `canon roots` (use `--suspended` to list only suspended roots) and skipped by `scan --all`.

**A view at a suspended place states the pause and lists nothing.** Naming a path on a suspended root, or standing in one, sets it aside and says so in one sentence:

```
/path/to/photos suspended — set aside: here · canon roots unsuspend path:/path/to/photos or --global
```

The sentence names the root, the verb, how the command reached it, and the way back. That third part is a **place** — the path, or `here` when you are standing in it — wherever you named one; where a command reached the root through something written down instead, it is the **role** the root plays in that command, as [`apply`](../archive/apply.md) and [`cluster status`](../archive/cluster.md) show (`destination`, `origin of 12 sources`). It goes where the command states its scope: stdout for [`facts`](../query/facts.md), [`coverage`](../archive/coverage.md) and [`survey`](../query/survey.md), stderr for [`ls`](../query/ls.md), [`worklist`](../enrich/worklist.md) and any output that is a machine stream. When the whole subject is suspended nothing is listed and the exit code is 1, with no `Error:` prefix. Naming a suspended path beside a live one sets only the suspended one aside: the rest runs, and the sentence prints before the listing. [`sweep`](../query/sweep.md) keeps its places off the board and states the pause in a footer.

**An act at a suspended place is refused by name**, in the same sentence with `refused` in place of `set aside`, and nothing is written: [`exclude`](../manage/exclude.md), [`cluster generate`](../archive/cluster.md) and [`refresh`](../archive/cluster.md), [`apply`](../archive/apply.md) at either end, [`facts delete`](../query/facts.md), [`note -m`](../manage/note.md) and `--clear`, [`scan`](scan.md), and `roots rm`. The refusal comes before any plan or confirmation, so `--yes` and `--dry-run` state it too.

[`roots retire`](retire.md) refuses too, before the readiness review is printed and before anything is bound. `--dry-run` is the other half: the review is a reading, so it renders and closes by stating the pause, refusing nothing. See [retire](retire.md#requirements). [`compare`](../query/compare.md) and `survey --other` refuse as well: both sides of those questions are load-bearing, and answering about one of them alone would answer a different question.

`roots comment` is permitted while a root is suspended: a comment labels the root, it does not reach inside it.

**A suspended root's copies still count as evidence.** Suspending changes what you are asking about, not what is where: content in a suspended root is still indexed, still archived if it was archived, and still grounds claims about other places. Every surface that cites such a copy marks it, and none of them lead with it:

- [`coverage`](../archive/coverage.md) counts it, and says how much of the archived total rests only on suspended roots
- [`survey`](../query/survey.md) counts it on the outward side, marks the location `(suspended)`, and orders live locations first
- [`roots story`](story.md) and the [`roots retire`](retire.md) review mark the location and count it in their totals
- [`trail`](../query/trail.md)'s composition card marks a suspended origin

A location is marked only when every copy Canon knows there is on a suspended root. One copy on a live root and there is nothing to open.

Two acts state a count rather than refusing, because a refusal would be wrong. [`import-facts`](../enrich/import-facts.md) sets aside the records naming a suspended source and counts them in its summary, so one suspended root never breaks a batch. And an object exclusion reaches every copy of the content, suspended roots included, since the judgment is about the content everywhere; the ceremony says how many of the copies it reaches stand on suspended roots.

Suspending and unsuspending are themselves recorded [decisions](../../concepts/decisions.md), and both take `--reason`:

```bash
canon roots suspend path:/path/to/photos --reason "steering the sweep; revisit after the phone import"
```

The reason renders in [`canon trail`](../query/trail.md) like any other, which is where "why did I park this" is answered.

Two things a suspended root goes on doing. Its copies still count as evidence about content elsewhere, so content that also sits in a suspended archive root still reads as archived, both under `--where archived?` and in [`coverage`](../archive/coverage.md). And what Canon already recorded still reads: [`trail`](../query/trail.md), [`roots story`](story.md), [`note`](../manage/note.md) and bound books are unaffected. A reading at a suspended place states the pause once in its header, without a verb:

```
Decision trail: /path/to/photos — /path/to/photos suspended · canon roots unsuspend path:/path/to/photos
```

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
