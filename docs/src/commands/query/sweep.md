# canon sweep

Sweep the whole universe for reduction opportunities — the ranked places where one dismissal decision resolves the most. Every other query command asks you to say *where*; the sweep answers that question. It is survey's counterpart: the sweep finds places, survey judges one, you decide, and the exclusion ceremony records.

```bash
# The leaderboard: the ten best reduction opportunities, ranked
canon sweep

# More entries
canon sweep --limit 25

# Everything: all entries, all hub members, findings below the emit floors
canon sweep --all
```

The sweep takes no paths and no filters — it is inherently universe-wide, computed fresh from current database state on every run. Acting on a finding (excluding, archiving) dissolves it: the next run reflects the new state, and the top slot always holds the current best move.

## Options

| Flag | Description |
|------|-------------|
| `--limit <N>` | Show up to N leaderboard entries (default: 10). |
| `--all` | No cap: every entry, every hub member, and findings below the emit floors. |

## Reading a finding

```
#1  /Volumes/OldBackup/Backup/ARCHIVED/Super8
    96% inside /Volumes/Archive/Media/Super8  (by size · 89% by count)
    keeper: archived, scanned 5d ago · subject scanned 2d ago
    gain: 1,204 files · 33.7 GB     residual: 1 file · 3.9 GB nowhere else
    → canon survey . --other /Volumes/Archive/Media/Super8
```

- **The subject** (the full path on the first line) is the place the finding is about.
- **The relation** states how the subject's content connects elsewhere, in survey's vocabulary: a *subset* sits inside a counterpart that holds more; a *mirror* matches its counterpart in both directions. Both percentages matter — a large gap between "by size" and "by count" means many small files carry little weight.
- **The keeper line** appears for subsets only — the structure decides which side holds more; the wording stays declarative ("inside X", never "keep X"), because the smaller side can still be the better copy. Mirrors name a *counterpart* and imply no preferred side. A keeper on a suspended root reads `reconnect to verify` — nothing is safe to act on until that root is scanned again. Both sides carry their scan age: a claim is only as fresh as its basis.
- **Gain** is what acting on the finding resolves. **Residual** is the verdict line: content existing nowhere else in the universe. `residual: none` means a clean dismissal; a small residual often means one rescue away from one.
- **The `→` handoff** is the ready-to-run judging command, written as if you `cd` into the subject first. The sweep only ever hands off to judgment — never to a ready-made exclusion.

When the subject is not fully hashed, the finding says so (`compared on 92% by size`) — unhashed content is unverified, never silently omitted. Notes you've left on the subject or counterpart (`canon note`) surface beside the finding.

### Scattered findings

When no single counterpart concentrates the match, the finding states coverage honestly:

```
#4  /Volumes/laptop-import/mixed
    94% exists elsewhere, across 7 locations (2 archived)
    scattered; consolidation candidate · subject scanned 12d ago
```

Nothing archived holds this content yet — it typically ranks low, but it is visible: scattered redundancy is a consolidation opportunity, not noise.

### Hubs

Many places pointing into one counterpart render as a single entry — one constellation, not thirty competing findings:

```
#2  /Volumes/Archive/Media/iphone-backup — 36 places point here
    counterpart archived, scanned 5d ago · total gain: 7,820 files · 41.2 GB
      /Volumes/OldBackup/old/iphone-2019  98% inside · 402 files · 2.1 GB
      /Volumes/old-disk/dumps/phone       97% inside · 371 files · 1.9 GB
      … 34 more (--all)
    → canon survey /Volumes/Archive/Media/iphone-backup
```

The hub occupies one leaderboard slot and shares one handoff: surveying the counterpart shows every member as a related location.

## Ranking

There is no composite score — every ranking factor is visible on the finding, in this order:

1. **Cleanliness**: ready-to-assess findings (at or above the lifting tolerance) above consolidation-grade overlap.
2. **Weight**: resolution gain, size-led (counts always shown beside sizes).
3. **Keeper safety**: archived counterpart above merely-present, above suspended; scattered content with nothing archived last.
4. **Residual burden**: content existing nowhere else penalizes — a clean dismissal outranks one that needs a rescue first.

Two runs against an unchanged database produce identical output.

## Honesty rules

- **The header declares every omission**: ubiquitous objects (present in too many places to signal anything) and empty files (zero-byte content is contentless — it never creates overlap, never counts in percentages, never blocks a residual).
- **Excluded content is resolution, not overlap**: it neither creates findings nor blocks dismissal, and surfaces as context where substantial (`3,000 sources here already excluded`).
- **Floors trim output, never existence**: small findings are counted in the footer (`12 more below the emit floors (--all)`) and reachable with `--all` — no threshold cliffs.
- **An empty leaderboard is an answer**: nothing above the floors means no folder-level redundancy worth attention. An unscanned or unhashed universe gets a pointer, not an empty list.

## The journey

The sweep closes the triage loop: run it, read the top finding, `cd` there, run the handoff survey, judge, then record the decision with `canon exclude` and a reason. Declining to act leaves no trace — unless you leave a note, which comes back beside the finding on the next run.
