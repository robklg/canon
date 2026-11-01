# Exclusion

Excluding dismisses content from consideration: it does not need archiving. Nothing
is deleted. The files stay on disk and in the index. Excluded sources are skipped by
default in queries and archiving operations, and `--include excluded` shows them again.

An exclusion can be cleared at any time, returning the content to consideration.
Clearing is itself recorded, as the `restored` transition. Nothing else lifts a
dismissal: the exclusion holds through edits, moves, replacement, and a file
reappearing after an absence.

## Two levels

- **Source-level**: dismisses a file at its path. Other copies of the same content
  are unaffected.
- **Object-level**: dismisses content by hash, for every source sharing that content,
  in source roots and archive roots alike, and any copy scanned later.

A source counts as excluded when either level applies: it is excluded itself, or its
content is.

## The record

Every exclusion is recorded as a [decision](decisions.md) with your optional
`--reason`, one of the three recorded fates beside archived and deleted.

See [`canon exclude`](../commands/manage/exclude.md) for the command.
