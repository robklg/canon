# The Book Format

When a root is [retired](../commands/roots/retire.md), its complete story is compiled
into **the book**: a directory of plain text in stable formats, readable with `ls`
and a text editor. This page is the format contract.

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

The summary for a person: the root's identity (path, role,
comment, scan history, the retirement reason), the resolution account, the
verification posture, a guide to the other files, and the gaps, meaning anything this
book should hold but doesn't.

It names `story.md` as the place to start, and gives the mapping from the story's plain
words to Canon's own:

| In the story | In Canon |
|---|---|
| chosen for the archive | `archived` |
| let go | `excluded` |
| preserved by copies in the archive | `covered` |
| no known copy in the archive | `unresolved` |
| empty file | `contentless` |
| returned to consideration | `restored` |

It also states the time zone the book's dates are in:

```
Dates in this book's text — this file, story.md, timeline.md, notes.md — and
in this folder's name are in this time zone: Europe/Amsterdam. The times in
inventory.jsonl and meta.toml are in UTC.
```

## story.md

The story that [`canon roots
story`](../commands/roots/story.md) tells, written for someone with no Canon
and no memory of the place.

The page runs in a fixed order:

1. An opening that orients the reader: what this document is, its date, and the time
   zone its dates are in (`Its dates are in this time zone: Europe/Amsterdam.`).
2. A short explanation of how to read the entries.
3. The places, in full.
4. A tally of where everything went.
5. The gaps, stated in prose: what was left open on purpose.
6. A last page.

Retirement asks for a **foreword**: your own words about the whole root, written
verbatim above Canon's text. It also offers the entire page to your editor before
the book is written. A page edited by hand is marked in `meta.toml` (`hand_edited`).

## inventory.jsonl

One JSON object per line, one line per source, **sorted by path**. Fields:

| Field | Presence | Meaning |
|-------|----------|---------|
| `path` | always | Path relative to the root |
| `size` | always | Bytes |
| `mtime` | usually | The file's save time, ISO 8601 in UTC with `Z` (`2015-06-12T09:30:00Z`; a year before 0000 in ISO 8601's expanded form), or the stored number of seconds since 1970, as text, for a save time beyond the instants Canon can hold; absent only on entries recovered from receipts that record no mtime |
| `hash` | where known | Content hash with algorithm prefix (`sha256:…`) |
| `fate` | always | What happened to this source; see the vocabulary below |
| `decision` | where recorded | The decision record behind the fate: for `archived`, the `apply`; for `excluded`/`deleted`, the action that set it; otherwise the source's most recent recorded transition. Cross-references the timeline's `#N` and the `NNNNNN-command.toml` receipt filenames, in the gathered `ledger/` for drive-local receipts and in the archive's live ledger for apply and exclusion receipts |
| `verification` | always | `content_verified` (hashed) or `name_only` (listed by name, never content-verified) |
| `disposition` | archived only | `moved` or `copied`; absent when the record does not state which: omitted, never guessed |
| `destination` | archived only | The recorded destination of the apply, readable without Canon |
| `locations` | where known | Archive paths holding this content at compile time (`destination` is what the apply recorded). Zero-byte sources have no `locations` |
| `reason` | where recorded | The user's reason for the exclusion or deletion |

### Fate vocabulary

- `archived` — archived from here: an apply receipt names this path as an origin.
  Has the recorded `destination` and, where known, current `locations`.
  Entries for sources moved into the archive are recovered from the apply receipts.
- `covered` — content verified present in the archive (by hash), archived from
  elsewhere, or archived from here when no receipt survives to say so (recorded as a
  gap).
- `excluded` — consciously dismissed, with the recorded reason. When the content is
  also archived, the archive locations appear too.
- `deleted` — a scan observed the loss; the recorded reason where present.
- `present` — present at retirement, with none of the above recorded.
- `contentless` — empty at retirement (zero bytes).
- `missing_unexplained` — absent without a recorded deletion.

## timeline.md

Every action that touched the root, with its summary as Canon printed it at the time
and the user's reason. Global actions are counted at the end, not listed. The
retirement itself is absent; a prior attempt that wrote a book but did not remove the root is listed.

## notes.md

Every note on the root, oldest first, with its location.

## ledger/

The receipts that lived on the drive itself: the root's own `.canon-ledger/`, copied
verbatim with filenames and timestamps preserved. A source root's ledger only ever
holds **deletion receipts**. The receipts of archiving and exclusion
live in the archive's own ledger (`.canon-ledger/` at the archive root), and the book
points there.

If the drive kept no receipts or was unreachable at compile time, there is no
`ledger/` directory, and the README says so.

## meta.toml

The machine-readable half, `version = 1`:

- `gaps` — every gap: unreadable receipts, a ledger that could not be copied, and so on. An empty list asserts that nothing
  this book should hold is missing from it.
- `zone` — the time zone the book's prose dates and directory name are in, the same
  text the retirement decision records ([the time zone a decision
  uses](../concepts/decisions.md#the-time-zone-a-decision-uses)).
- `[identity]` — path, `root_id` (the root's number in the index that bound the book; a
  retirement of the same root, at the same path and on the same day, after its release
  was declined or stopped, replaces the book at the same name that carries it), role,
  comment, suspension, `first_indexed` (when the earliest
  remaining source was first indexed), last scan, `compiled_at`, the user's reason,
  `decision_id` (the id the trail and the index use for the retirement that wrote this
  book; absent when the retirement ran with recording
  off: omitted, never guessed), and the Canon version that wrote the book.
- `[account]` — the resolution account in counts: the story so far (archived files
  and bytes with the moved/copied split, deleted, unexplained missing) and the
  standing at binding (`archived_standing`, meaning a file chosen from here and still
  present — named by an archiving receipt, or, where a decision left none, by the
  directory it drew from; `covered`, `excluded`, `contentless`, `unresolved`). Bytes and
  derived totals are omitted, never guessed, when the record cannot support them.
- `[posture]` — `scan_verified` or `on_faith` (with the reason: suspended,
  unreachable, or never scanned) and the last scan time.
- `[counts]` — entry totals per fate. Before any removal, Canon recounts the inventory
  and compares it with them.
- `[ledger]` — whether the drive-local ledger was copied (`gathered`), and how many files.
- `[story]` — its file (`story.md`), whether it was edited by hand (`hand_edited`),
  and the settings that shaped it. Verification requires the file to exist and hold
  text.

## Versioning

`version` in `meta.toml` identifies the format. This page describes version 1. Fields
may be added within a version; existing fields keep their meaning. A book bound before
a field was added lacks it and verifies unchanged: `zone`, `root_id`, `[story]` with
`story.md`, and the `contentless` fate, whose count is taken as zero and whose empty files such a book
lists as `covered`. Verifying a book of a newer version than Canon knows fails with an
error. The book itself stays readable either way.
