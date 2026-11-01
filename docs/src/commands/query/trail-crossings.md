# canon trail crossings

What was transferred between two places, according to the recorded [crossings](../../concepts/decisions.md#crossing-in-crossing-out-staying-put).

```bash
# The places behind canon trail's totals
canon trail crossings

# Everything one root ever delivered here
canon trail crossings --origin /Volumes/old-backup

# Everything it delivered, wherever it went
canon trail crossings --global --origin /Volumes/old-backup
```

## With no flags

```
$ canon trail crossings

Crossings: /archive

Archived from here: 1,251 files (22.1 GB) → 2 destinations.
  /archive/Media/2016
      904 files (18.2 GB) · 3 decisions · 2026-07-11 · 44 folders
  /archive/Documents/scans
      347 files (3.9 GB) · decision #57 · 2026-08-09

Arrived here: 36,412 files (498.2 GB) from 10 origins.
  /Volumes/old-backup/archived
      (root retired — the book: /archive/books/2026-08-11-backup-archived)
      8,398 files (201.4 GB) · 15 decisions · 2026-08-02 – 2026-08-09
  … and 9 more origins.

`canon trail crossings --origin <path>` or `--destination <path>` to list the folders behind an entry
```

`44 folders` counts the recorded destination directories behind an entry.

## Naming a counterpart

`--origin <path>` narrows to content that came from at or under that place, and `--destination <path>` narrows to content that ended up at or under it. The two compose. Naming one lists its section per action:

```
$ canon trail crossings --origin /Volumes/old-backup/archived

Crossings: /archive

Arrived here: 8,398 files (201.4 GB) from /Volumes/old-backup/archived
  (root retired — the book: /archive/books/2026-08-11-backup-archived).

  #48   2026-08-02   1,204 files (31.2 GB)   moved in
        Photos/2016  → Media/2016
        Photos/2017  → Media/2017
        "italy trip + the 2017 backlog"
  #50   2026-08-02     847 files (12.8 GB)   copied in
        Photos/2018  → Media/2018
  … and 13 more decisions.

Standing here: 8,151 of the 8,398 files delivered — 15 decisions stand behind them; 17 delivered.
```

`Standing here` compares what came from this origin with what is here now, when the origin is a whole root.

## Caps and machine output

`--jsonl` emits the timeline's events for the actions with a crossing here.

| Flag | Meaning |
|------|---------|
| `--origin <path>` | Narrow to content that came from at or under this path |
| `--destination <path>` | Narrow to content that ended up at or under this path |
| `--global` | All roots; requires `--origin` or `--destination` |
| `--limit N` | At most N entries per section (default 20) |
| `--all` | No cap |
| `--jsonl` | Machine output (JSONL on stdout) |
