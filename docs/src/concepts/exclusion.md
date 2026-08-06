# Exclusion

Excluding dismisses content from consideration: a conscious decision that this
content does not need archiving. Nothing is deleted. The files stay on disk and in
the index; what changes is attention. Excluded sources are skipped by default in
queries and archiving operations, and `--include excluded` shows them again.

An exclusion can be cleared at any time, returning the content to consideration.
Clearing is itself recorded, as the `restored` transition.

Redundancy beside the archive is a common reason to let go, not the only one:
corrupted files, known junk, and content that simply is not worth keeping are all
exclusion's territory.

## Two levels

- **Source-level** exclusion dismisses a file at its path. Other copies of the same
  content are unaffected.
- **Object-level** exclusion dismisses content by hash. It is universal: it affects
  every source sharing that content, in source roots and archive roots alike, and
  any copy scanned later.

A source counts as excluded when either level applies: it is excluded itself, or its
content is.

## The record

Every exclusion is recorded as a [decision](decisions.md) with your optional
`--reason`, and leaves a durable receipt. Exclusion is one of the three recorded
fates, beside archived and deleted: the dismissal is part of the story, not a
silent disappearance.

See [`canon exclude`](../commands/manage/exclude.md) for the command surface.
