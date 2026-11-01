# Resolution

Everything Canon indexes, across all your roots, is your **universe**. Resolving it
is the goal of the work: deciding, for all of it, that content worth keeping is
archived and content not worth keeping is [excluded](exclusion.md). The universe
shrinks from both directions, and what remains is the work still to do.

## Coverage is evidence

**Covered** content is verified present in the archive by content identity: the same hash
exists at an archive root. Coverage is a precise claim about content and nothing more.
It does not say the archive copy is arranged the way you want, and it does not say you
chose it; content can be covered by a copy nobody put there on purpose.

The judgment that content is resolved stays yours: Canon states what it can verify.

## Standings

A present source is in exactly one standing:

- **excluded**: consciously dismissed (see [Exclusion](exclusion.md))
- **contentless**: empty, nothing for content identity to verify (see
  [empty files are contentless](source-object.md#empty-files-are-contentless))
- **archived from here**: the original, still here, of content copied to the archive
- **covered**: content verified present in the archive, though not archived from
  here
- **unresolved**: none of the above; no resolution evidence

When several could apply, the earlier in this list wins. A source that was never
hashed counts as unresolved.

## The verdict is asymmetric

Canon can prove content unresolved: present, no coverage evidence, not excluded. It
never certifies the opposite. The [retirement review](retirement.md) says
**NOT READY** while unresolved sources remain; when nothing blocks, it reports that
no blockers were found and leaves the verdict to you.
