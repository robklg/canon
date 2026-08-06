# Resolution

Everything Canon indexes, across all your roots, is your **universe**. Resolving it
is the goal of the work: deciding, for all of it, that content worth keeping is
archived and content not worth keeping is [excluded](exclusion.md). The universe
shrinks from both directions, and what remains is the work still to do.

## Coverage is evidence

**Covered** content is verified present in the archive by content identity: the same
hash stands at an archive root. Coverage is a precise claim about content and
nothing more. It does not say the archive copy is arranged the way you want, and it
does not say you chose it; content can be covered by a copy nobody deliberately
placed.

Coverage is evidence. The judgment that content is *resolved* stays yours: Canon
states what it can verify and leaves the conclusion to you.

## Standings

A present source is in exactly one standing:

- **excluded**: consciously dismissed (see [Exclusion](exclusion.md))
- **contentless**: empty, nothing for content identity to verify (see
  [empty files are contentless](object.md#empty-files-are-contentless))
- **archived from here**: the still-standing original of content archived out of
  this place by a copy
- **covered**: content verified present in the archive, though not archived from
  here
- **unresolved**: none of the above; no resolution evidence

When several could apply, the earlier in this list wins: exclusion is a judgment
and covers the content whatever else is true of it, and an empty file is contentless
before any identity test. A source that was never hashed counts as unresolved,
because its coverage cannot be verified; empty files are the exception, since there
is no content to lose.

## The verdict is asymmetric

Canon can prove content unresolved: present, no coverage evidence, not excluded. It
never certifies the opposite. The [retirement review](retirement.md) says
**NOT READY** while unresolved sources stand; when nothing blocks, it reports that
no blockers were found and leaves the verdict to you.

## Where this vocabulary appears

[`canon coverage`](../commands/archive/coverage.md) counts covered content,
[`canon survey`](../commands/query/survey.md) reads a location's archive overlap,
[`canon roots story`](../commands/roots/story.md) maps a root's standings by place,
and the [retirement review](../commands/roots/retire.md) states them as counts
before a root is released.
