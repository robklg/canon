# The Book Format

When a root is [retired](../commands/roots/retire.md), its complete story is compiled
into **the book** — a directory designed to outlive Canon. Everything in it is plain
text in stable formats: you can read a book decades later with `ls`, a text editor,
and nothing else. This page is the format contract.

A book directory contains:

```
old-drive-2026-08-02/
├── README.md         the human entry point — start here
├── inventory.jsonl   every source the root ever had, with fates
├── timeline.md       every decision that touched the root, with reasons
├── notes.md          the notes, bound beside the timeline
├── ledger/           the drive-local receipts, gathered verbatim
└── meta.toml         identity, account, counts, gaps — machine-readable
```

## README.md

The rendered summary a person can just read: the root's identity (path, role,
comment, scan history, the retirement reason), the resolution account, the
verification posture, a guide to the other files, and the gaps — anything this book
should hold but doesn't, stated plainly.

## inventory.jsonl

One JSON object per line, one line per source, **sorted by path** — the sort order is
the tree structure, so a future reader (or tool) can browse the retired root without
any index. Fields:

| Field | Presence | Meaning |
|-------|----------|---------|
| `path` | always | Path relative to the root |
| `size` | always | Bytes |
| `mtime` | usually | Modification time, ISO-8601 UTC (`2015-06-12T09:30:00Z`); absent only on entries recovered from receipts that predate per-item mtimes |
| `hash` | where known | Content hash with algorithm prefix (`sha256:…`) |
| `fate` | always | What happened to this source — see the vocabulary below |
| `verification` | always | `content_verified` (hashed) or `name_only` (listed by name; never content-verified) |
| `disposition` | archived only | `moved` or `copied`; absent when the record predates the vocabulary — omitted, never guessed |
| `destination` | archived only | The recorded destination of the apply — readable without Canon |
| `locations` | where known | Archive paths holding this content at compile time (the live tier; `destination` is the recorded fallback) |
| `reason` | where recorded | The user's reason on the excluding/deleting decision |

### Fate vocabulary

- `archived` — archived *from here*: an apply receipt names this path as an origin.
  Carries the recorded `destination` and, where resolvable, current `locations`.
  Sources moved into the archive keep an inventory entry even though the root no
  longer holds a record of them — these entries are recovered from the apply
  receipts.
- `covered` — content verified present in the archive (by hash), archived from
  elsewhere — or from here when no receipt survives to say so (recorded as a gap).
- `excluded` — consciously dismissed, with the recorded reason; when the content is
  also archived, the archive locations appear as context (both truths carried).
- `deleted` — a scan observed the loss; the recorded reason where present.
- `present` — present at retirement, none of the above: listed honestly.
- `missing_unexplained` — absent without a recorded deletion: a record-quality fact,
  never hidden.

An entry without a hash can never claim `content_verified` — for an index-only root
this distinction is the whole book, and the book says so plainly.

## timeline.md

Every decision that touched the root, oldest first: date, decision id, command, the
decision's summary as Canon printed it at the time, and the user's reason beneath.
Global decisions (which touch the whole universe, not this root specifically) are
counted at the end rather than listed.

## notes.md

Every note on the root, oldest first, with its location — the thinking between the
actions. Removal deletes notes from the index; binding them here is what makes that
deletion safe.

## ledger/

The root's drive-local `.canon-ledger/` receipts, copied verbatim with filenames and
timestamps preserved — so `previous_decision_id` chains inside the receipts remain
walkable from disk into the book, without Canon. If the drive was unreachable at
compile time, this directory is absent and the gap is recorded in `meta.toml` and the
README.

## meta.toml

The machine-readable half, `version = 1`:

- `gaps` — every self-explaining gap: unreadable receipts (per-item origin degraded
  to `covered`), an ungathered ledger, and so on. An empty list is a claim: nothing
  this book should hold is missing from it.
- `[identity]` — path, role, comment, suspension, first/last scan, `compiled_at`,
  the user's reason, and the Canon version that wrote the book.
- `[account]` — the resolution account in counts: the story so far (archived files
  and bytes with the moved/copied split, deleted, unexplained missing) and the
  standing at binding (covered, excluded, unresolved). Bytes and derived totals are
  omitted when the record cannot support them — never guessed.
- `[posture]` — `scan_verified` or `on_faith` (with the reason: suspended,
  unreachable, or never scanned) and the last scan time.
- `[counts]` — entry totals per fate. These are the verification anchor: Canon's
  structural check recounts the inventory and compares against them before any
  removal proceeds.
- `[ledger]` — whether the drive-local ledger was gathered, and how many files.

## Versioning

`version` in `meta.toml` identifies the format. This page describes version 1.
Fields may be added within a version; existing fields keep their meaning. A future
Canon refuses to *verify* a book of a newer version than it knows — the book itself
remains readable regardless, which is the point.
