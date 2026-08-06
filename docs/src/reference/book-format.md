# The Book Format

When a root is [retired](../commands/roots/retire.md), its complete story is compiled
into **the book**: a directory designed to outlive Canon. Everything in it is plain
text in stable formats, so you can read a book decades later with `ls`, a text
editor, and nothing else. This page is the format contract.

A book directory contains:

```
photos-backup-2026-08-02/
├── README.md         the human entry point — start here
├── story.md          the story as told, written at the retirement
├── inventory.jsonl   every source the root ever had, with fates
├── timeline.md       every decision that touched the root, with reasons
├── notes.md          the notes, bound beside the timeline
├── ledger/           the receipts that lived on the drive (absent when it kept none)
└── meta.toml         identity, account, counts, gaps — machine-readable
```

## README.md

The rendered summary a person can just read: the root's identity (path, role,
comment, scan history, the retirement reason), the resolution account, the
verification posture, a guide to the other files, and the gaps, meaning anything this
book should hold but doesn't.

It names `story.md` as the way in, and carries the mapping from the story's plain
words to Canon's own:

| In the story | In Canon |
|---|---|
| chosen for the archive | `archived` |
| let go | `excluded` |
| preserved by copies in the archive | `covered` |
| no known copy in the archive | `unresolved` |
| empty file | `contentless` |
| returned to consideration | `restored` |

## story.md

The story as told: the same reading [`canon roots
story`](../commands/roots/story.md) renders live, written for someone with no Canon
and no memory of the place. What was on it, what was chosen for the archive and where
it lives now, what was let go and why, in plain words.

The page runs in a fixed order:

1. An opening that orients the reader: what this document is, and its date.
2. A short explanation of how to read the entries.
3. The places, in full. A bound story is never capped.
4. A tally of where everything went.
5. The gaps, stated in prose: what was left open on purpose, seen and weighed and
   accepted.
6. A last page.

The tally's lines can sum past its total, because a file copied to the archive and
later dismissed here belongs to two of them. Where that happens the story states by
how much, so the tally reads as overlapping registers rather than as a partition.

The story names itself **one telling of the record**. Another telling could be drawn
from the facts beside it; this is the one written at the retirement.

The ceremony invites a **foreword**: your own words about the whole place, bound
verbatim above Canon's narration. It also offers the entire page to your editor
before it binds. A hand-refined telling is marked in `meta.toml` (`hand_edited`); the
inventory and counts beside it stay the machine-verified record either way.

## inventory.jsonl

One JSON object per line, one line per source, **sorted by path**. The sort order is
the tree structure, so a future reader or tool can browse the retired root without
any index. Fields:

| Field | Presence | Meaning |
|-------|----------|---------|
| `path` | always | Path relative to the root |
| `size` | always | Bytes |
| `mtime` | usually | Modification time, ISO-8601 UTC (`2015-06-12T09:30:00Z`); absent only on entries recovered from receipts that predate per-item mtimes |
| `hash` | where known | Content hash with algorithm prefix (`sha256:…`) |
| `fate` | always | What happened to this source; see the vocabulary below |
| `decision` | where recorded | The fate-determining decision: for `archived`, the apply; for `excluded`/`deleted`, the decision that stamped it; for the standings, the source's most recent recorded transition. Cross-references the timeline's `#N` and the `NNNNNN-command.toml` receipt filenames, in the gathered `ledger/` for drive-local receipts and in the archive's live ledger for apply and exclusion receipts |
| `verification` | always | `content_verified` (hashed) or `name_only` (listed by name, never content-verified) |
| `disposition` | archived only | `moved` or `copied`; absent when the record predates the vocabulary — omitted, never guessed |
| `destination` | archived only | The recorded destination of the apply, readable without Canon |
| `locations` | where known | Archive paths holding this content at compile time (the live tier; `destination` is the recorded fallback). Zero-byte sources carry no location lists: every empty file shares the one empty-content object, so the list would answer nothing about this file |
| `reason` | where recorded | The user's reason on the excluding or deleting decision |

### Fate vocabulary

- `archived` — archived *from here*: an apply receipt names this path as an origin.
  Carries the recorded `destination` and, where resolvable, current `locations`.
  Sources moved into the archive keep an inventory entry even though the root no
  longer holds a record of them; these entries are recovered from the apply receipts.
- `covered` — content verified present in the archive (by hash), archived from
  elsewhere, or archived from here when no receipt survives to say so (recorded as a
  gap).
- `excluded` — consciously dismissed, with the recorded reason. When the content is
  also archived, the archive locations appear as context, so both truths are carried.
- `deleted` — a scan observed the loss; the recorded reason where present.
- `present` — present at retirement, with none of the above recorded.
- `contentless` — empty at retirement (zero bytes). An empty file has no content to
  identify, so the entry claims neither covered nor unresolved. (Added within
  version 1, 2026-08-04; books bound earlier contain no such entries and recorded
  empty files as `covered`.)
- `missing_unexplained` — absent without a recorded deletion, carried as its own fate
  rather than folded into another.

An entry without a hash never carries `content_verified`. For a root that was indexed
but never hashed, every entry is `name_only`.

## timeline.md

Every decision that touched the root, oldest first: date, decision id, command, the
decision's summary as Canon printed it at the time, and the user's reason beneath.
Global decisions, which touch the whole universe rather than this root specifically,
are counted at the end rather than listed. The retirement's own in-flight decision is
absent, because the book is compiled before the release completes and that decision
has nothing to report yet; the retirement's facts live on the identity page instead.
A *prior* retirement attempt, bound but not released, is history and is listed.

## notes.md

Every note on the root, oldest first, with its location: the thinking between the
actions. Removal deletes notes from the index, and binding them here is what keeps
them.

## ledger/

The receipts that lived on the drive itself: the root's own `.canon-ledger/`, copied
verbatim with filenames and timestamps preserved, so `previous_decision_id` chains
inside the receipts remain walkable from disk into the book, without Canon. By the
receipt placement principle a source root's ledger only ever holds **deletion
receipts**, the record of what was lost there. The receipts behind archiving and
letting-go decisions live in the archive's own ledger (`.canon-ledger/` at the
archive root), beside the content they concern; the book points there rather than
copying them, since the story and timeline already carry every decision in full.

When the drive kept no receipts of its own, no `ledger/` directory is written and the
README states the absence. If the drive was unreachable at compile time, the
directory is likewise absent and the gap is recorded in `meta.toml` and the README.

## meta.toml

The machine-readable half, `version = 1`:

- `gaps` — every self-explaining gap: unreadable receipts (per-item origin degraded
  to `covered`), an ungathered ledger, and so on. An empty list asserts that nothing
  this book should hold is missing from it.
- `[identity]` — path, role, comment, suspension, `first_indexed` (when the earliest
  surviving row was first indexed — row evidence, honest on roots older than
  decision recording), last scan, `compiled_at`, the user's reason, `decision_id`
  (the retirement decision that bound this book — the id the trail and the index
  reference this retirement by, readable from the book alone; absent when the
  ceremony ran with recording off — omitted, never guessed), and the Canon version
  that wrote the book.
- `[account]` — the resolution account in counts: the story so far (archived files
  and bytes with the moved/copied split, deleted, unexplained missing) and the
  standing at binding (`archived_standing` — archived from here with the copy
  still standing, `covered`, `excluded`, `contentless`, `unresolved`; the first
  and the two additions arrived within version 1, 2026-08-04). Bytes and derived
  totals are omitted when the record cannot support them — never guessed.
- `[posture]` — `scan_verified` or `on_faith` (with the reason: suspended,
  unreachable, or never scanned) and the last scan time.
- `[counts]` — entry totals per fate (including `contentless`, added within
  version 1; absent in earlier books, read as zero). These are the verification
  anchor: Canon's structural check recounts the inventory and compares against
  them before any removal proceeds.
- `[ledger]` — whether the drive-local ledger was gathered, and how many files.
- `[story]` — the telling's claim: its file (`story.md`), whether it was
  hand-refined at the binding (`hand_edited`), and the reading settings that
  shaped it (the place-map calibration constants). Verification requires the
  claimed file to exist and hold text; the prose itself is never recounted, and
  the inventory and `[counts]` remain the verification anchor. Absent in books
  bound before the telling (added within version 1, 2026-08-05); such books
  verify unchanged.

## Versioning

`version` in `meta.toml` identifies the format. This page describes version 1. Fields
may be added within a version; existing fields keep their meaning. A future Canon
refuses to *verify* a book of a newer version than it knows. The book itself stays
readable either way.
