# canon sweep

Sweep the whole [universe](../../concepts/resolution.md) for reduction opportunities: the ranked places where one dismissal decision resolves the most. Every other query command asks you to say *where*; the sweep answers that question itself. The sweep finds places, survey judges one, you decide, and the decision is recorded with `canon exclude`.

```bash
# The leaderboard: the ten best reduction opportunities, ranked
canon sweep

# More entries
canon sweep --limit 25

# Everything: all entries, all members, findings below the emit floors
canon sweep --all
```

The sweep takes no paths and no filters; it is universe-wide, computed fresh from current database state on every run. Acting on a finding (excluding, archiving) removes it: the next run reflects the new state, and the top slot always holds the current best move.

## Options

| Flag | Description |
|------|-------------|
| `--limit <N>` | Show up to N leaderboard entries (default: 10). |
| `--all` | No cap: every entry, every member of a multi-place entry, and findings below the emit floors. |

## Reading a finding

```
#1  /Volumes/OldBackup/ARCHIVED/Super8
    96% inside /Volumes/Archive/Media/Super8  (by size · 89% by count)
    counterpart: archived, scanned 5d ago · subject scanned 2d ago
    gain: 1,204 files · 33.7 GB     residual: 1 file · 3.9 GB nowhere else
    → canon survey . --other /Volumes/Archive/Media/Super8
```

- **The subject** (the full path on the first line) is the place the finding is about, the side you might dismiss. The other side is the **counterpart**: where the copies live. A single finding's headline is its *subject*; a hub's headline is the shared *counterpart* (the hub's own "shared counterpart" line states this).
- **The relation** states how the subject's content connects elsewhere, in survey's vocabulary: a *subset* sits inside a counterpart that holds more; a *mirror* matches its counterpart in both directions. Both percentages matter: a large gap between "by size" and "by count" means many small files carry little weight.
- **The counterpart line** states the counterpart's [*standing*](../../concepts/resolution.md#standings) (`archived` or `present`), which is what makes acting on the finding safe or not. The wording is declarative ("inside X", never "keep X"): the relation implies no preferred side; even for a subset, the smaller side can still be the better copy. Both sides carry their scan age; the claim rests on the last scan.
- **Gain** is what acting on the finding resolves. **Residual** is content existing nowhere else in the universe. `residual: none` means a clean dismissal; a small residual often means one rescue away from a clean one.
- **The `→` handoff** is the ready-to-run judging command, written as if you `cd` into the subject first. The sweep only ever hands off to judgment, never to a ready-made exclusion.

When the subject is not fully hashed, the finding says so (`compared on 92% by size`): unhashed content is unverified, never silently omitted. Notes you've left on the subject or counterpart (`canon note`) surface beside the finding.

A subject that itself stands on an archive root is marked `(in the archive)` and ranks below an equivalent place on a source root: its content is already resolved, so it does not compete for triage attention, and the real opportunity usually sits on the counterpart side. It is demoted, not removed, and the relation is stated anyway; the sweep compares any location to any other. A hub headlined by an archive counterpart is untouched by this: its members are the subjects, and live source members keep the hub competing at full weight.

A subject that is a whole root, rather than a folder inside one, is marked `(whole root)`. Both markers appear where both apply.

### Scattered findings

When no single counterpart concentrates the match, the finding states the spread:

```
#4  /Volumes/Backup/laptop-import/mixed
    94% exists elsewhere, across 7 locations (2 archived · 3 suspended)
    scattered; consolidation candidate · subject scanned 12d ago
```

Scattered content with nothing archived ranks last, but it stays visible: scattered redundancy is a consolidation candidate.

The parenthetical counts how many of those locations are archive roots, and how many stand on [suspended](../roots/roots.md#suspending-roots) roots. The suspended count is omitted when it is zero. No other number moves: a location behind a closed door is still a location, and the content is still there.

### Hubs

Many places pointing into one counterpart render as a single leaderboard entry:

```
#2  /Volumes/Archive/Media/iphone-backup
    shared counterpart — 36 places hold copies inside it · archived, scanned 5d ago
    total gain: 7,820 files · 41.2 GB
      /Volumes/OldBackup/iphone-2019  98% inside · 402 files · 2.1 GB
      /Volumes/old-disk/dumps/phone   97% inside · 371 files · 1.9 GB
      … 34 more (--all)
    → canon survey /Volumes/Archive/Media/iphone-backup
```

The hub occupies one leaderboard slot and shares one handoff: surveying the counterpart shows every member as a related location.

### Roots close to done

A source root with few [unresolved](../../concepts/resolution.md) sources left takes one leaderboard slot of its own, headlined by the root and carrying its places as members:

```
#1  /Volumes/oldmac  (whole root)
    3 unresolved sources remain
    6 places, up to 1,204 files · 41.3 GB
      /Volumes/oldmac/Pictures/2019  mirrors · 100% · archived · 612 files · 22.1 GB
      /Volumes/oldmac/Pictures/2020  98% inside · archived · 431 files · 14.8 GB
      … 4 more (--all)
    → canon roots retire path:/Volumes/oldmac --dry-run
```

The count is the same remainder the [retirement](../roots/retire.md) readiness review measures: sources on the root that are neither archived, nor covered, nor excluded, nor empty. It is a fact about the root and not a verdict about it; the review the handoff names is what judges whether the root is ready, and `--dry-run` reports that verdict rather than acting on it. A zero remainder reads `no unresolved sources remain`.

The figure is an upper bound (`up to`), not a total. Places on one root can be each other's evidence: two folders holding copies of each other both report those bytes, and only one of them can be let go, so what acting would actually resolve is never more than the figure shown.

Each member states its own counterpart [standing](../../concepts/resolution.md#standings), which is what makes acting on it safe; hub members take theirs from the hub's headline instead. Members are capped like hub members, with the omission counted and `--all` revealing the rest. A qualifying root with only one place forms no such entry: one place is already one slot. Archive roots never form one; they are not retired.

## Ranking

There is no composite score: every ranking factor is visible on the finding, in this order:

1. **Cleanliness**: ready-to-assess findings (at or above the lifting tolerance) above consolidation-grade overlap.
2. **Archive standing**: a place standing on a source root above an equivalent place standing in the archive.
3. **Root nearness**: a place on a source root with little left unresolved above a place on a root barely started. Archive roots carry no nearness and tie here.
4. **Weight**: resolution gain, size-led (counts always shown beside sizes).
5. **Counterpart standing**: archived above merely-present; scattered content with nothing archived last.
6. **Residual burden**: content existing nowhere else penalizes; a clean dismissal outranks one that needs a rescue first.

Nearness sorts ahead of weight, so a small place on a root close to done outranks a large one elsewhere. Where it applies, the finding says so on its own line:

```
#1  /Volumes/oldmac/Pictures/2019
    mirrors /Volumes/Archive/Media/2019  (100% by size · 100% by count)
    counterpart: archived, scanned today · subject scanned today
    20 unresolved sources remain on /Volumes/oldmac
    gain: 30 files · 61.4 KB     residual: none
    → canon survey . --other /Volumes/Archive/Media/2019
```

The line appears only where nearness is in play; its absence means the order rests on the other factors. Nearness separates entries only among roots close enough to done for the board to say so; above that, roots tie on it and weight leads. The remainder is bucketed by order of magnitude, so it takes a tenfold change to move a place on the board.

Two runs against an unchanged database produce identical output.

## Suspended roots

Places on a [suspended](../roots/roots.md#suspending-roots) root are not ranked, and neither are places whose counterpart stands on one. Each suspended root that kept places off the board gets a footer line naming both causes, what each is worth at most, and the way back:

```
/Volumes/OldBackup suspended — not ranked: 8 places on it (up to 185.3 GB), 4 with copies on it (up to 12.1 GB) · canon roots unsuspend path:/Volumes/OldBackup
```

Places **on** the root stand there; places **with copies on** it stand elsewhere and rest their claim on content behind the closed door.  Each cause carries its own figure, and each is an upper bound: parked places can be each other's evidence, so what unsuspending would actually resolve is never more than the figure shown. Whether the suspended root is a source or an archive root makes no difference. Above three suspended roots the lines collapse to one, counting roots rather than naming them, with `canon roots list --suspended` as the way back.

Suspended roots stay in the computation: their copies still count as gain rather than residual, so a folder duplicated entirely inside a suspended root does not read as unique. What changes is position, not existence. `--all` does not reveal these places; `canon roots unsuspend` brings them back, and inspecting one parked place is `canon survey <path>`.

The below-floor footer count includes places that are not ranked, so `--all` reveals fewer entries than that count names.

## Honesty rules

- **The header declares every omission**: ubiquitous objects (present in too many places to signal anything) and empty files (zero-byte content is [contentless](../../concepts/object.md#empty-files-are-contentless): it never creates overlap, never counts in percentages, never blocks a residual).
- **Excluded content is resolution, not overlap**: it neither creates findings nor blocks dismissal, and surfaces as context where substantial (`3,000 sources here already excluded`).
- **Floors trim output, never existence**: small findings are counted in the footer (`12 more below the emit floors (--all)`) and reachable with `--all`.
- **A board that changes without you acting on it explains itself**: suspending a root changes what the sweep ranks, and the suspended-root footer lines state the count, the mass, and the way back on every run where places were not ranked.
- **An empty leaderboard is an answer, never a false one**: the board says there is no folder-level redundancy worth attention only when nothing was withheld from it. Where suspension or `--limit` emptied it, the line says that instead. An unscanned or unhashed universe gets a pointer, not an empty list.

## The journey

Run the sweep, read the top finding, `cd` there, run the handoff survey, judge, then record the decision with `canon exclude` and a reason. Declining to act leaves no trace, unless you leave a note, which comes back beside the finding on the next run.
