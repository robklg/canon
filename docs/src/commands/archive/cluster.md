# canon cluster generate

Generate a manifest of files matching filters. The `--dest` flag specifies where files will be copied and must be inside a registered archive root.

```bash
# All photos to an archive (unhashed sources are automatically skipped)
canon cluster generate --where 'source.ext|lowercase IN (jpg, png, heic)' --dest /Volumes/Archive/Photos

# Destination can be a subdirectory within an archive
canon cluster generate --where 'source.ext|lowercase IN (jpg, png, heic)' --dest /Volumes/Archive/Photos/2024

# Scope to a specific path
canon cluster generate /path/to/photos --dest /Volumes/Archive

# Custom output file
canon cluster generate --where 'source.ext|lowercase=jpg' --dest /Volumes/Archive -o my-manifest.toml

# Allow sources from archive roots
canon cluster generate --where 'source.ext|lowercase=jpg' --dest /Volumes/Archive --allow archived

# Allow duplicate content (same hash already in an archive)
canon cluster generate --where 'source.ext|lowercase=jpg' --dest /Volumes/Archive --allow duplicates

# Show which files were excluded (already archived)
canon cluster generate --where 'source.ext|lowercase=jpg' --dest /Volumes/Archive --show-archived

# Overwrite existing manifest file
canon cluster generate --where 'source.ext|lowercase=jpg' --dest /Volumes/Archive --force
```

The command generates two files: a manifest (`.toml`) that you edit, and a lock file (`.lock`) holding what the run settled: the resolved scope, and one line per source recording its state and where it goes. The lock is not edited by hand: [`apply`](apply.md) uses it as written.

**Typical workflow:**

```bash
canon cluster generate --where 'source.ext|lowercase IN (jpg, png, heic)' --dest /Volumes/Archive
# Edit manifest.toml to customize the output pattern
canon apply manifest.toml --dry-run   # Preview
canon apply manifest.toml             # Execute
```

**Output:**

After generating, the command prints a summary showing root breakdown and archive coverage:

```
Generated manifest: manifest.toml (1,234 sources in manifest.lock)
  From 2 roots:
    /Volumes/Drive1  (800)
    /Volumes/Drive2  (434)
  1,234 have no archived copy
Time zone: Europe/Amsterdam, recorded in the manifest's zone line.
```

**Empty files are never skipped as "already archived".** A zero-byte file is
[contentless](../../concepts/object.md#empty-files-are-contentless), so
archive detection ignores it and the manifest carries it with its folder.

**Manifest structure:**

The generated manifest includes a cluster summary, a notes section for your own annotations, and comments listing available pattern variables:

```toml
# Canon manifest — edit pattern and Notes freely.
# To change the query, edit it here then run: canon cluster refresh <manifest>
# zone: the time zone for times in the query and in folder names.
# Edit it, or delete it to take this machine's, then run: canon cluster refresh <manifest>
# Other fields are managed by Canon — do not edit.
#
# === Cluster Summary ===
# 1,234 sources from 2 roots:
#   /Volumes/Drive1  (800)
#   /Volumes/Drive2  (434)
# 1,234 have no archived copy

# === Notes ===
#

[meta]
version = 2
query = ["source.ext|lowercase IN ('jpg', 'png', 'heic')"]
scope = ["/path/to/photos"]
zone = "Europe/Amsterdam"
generated_at = "2026-02-28T12:00:00Z"
lock_hash = "abc123..."

[options]
allow = []                       # e.g. ["archived", "duplicates"]

[output]
pattern = "{scope.rel_path}"     # ← Edit this to customize organization
base_dir = "/Volumes/Archive"
archive_root_id = 2

# Available facts for pattern (100% coverage on 1234 sources):
# ...
```

- **Cluster Summary** is regenerated on each `cluster refresh`, showing current source counts, root breakdown, and archive coverage.
- **Notes** section is preserved across refreshes — add your own comments here.
- **`pattern`** starts at `{scope.rel_path}` when the generation was scoped to any path at all, and at `{source.rel_path}` when it was not: files keep the folder structure they were found in. Edit it to organize them differently. An existing manifest keeps the pattern it recorded.

  Scoping to files rather than a folder — `canon cluster generate notes-2026-02*`, letting the shell expand the wildcard — places those files by name: directly under the destination when they all sit in one folder, and otherwise below the folder they share, which keeps files of the same name apart. How many files the wildcard happened to match makes no difference.

  Writing the manifest inside the destination is fine, but a manifest whose own path is a directory the pattern needs blocks every file below it. Generate and refresh warn when that is the case, naming both paths; [`apply`](apply.md) rejects the run. This is easy to hit by naming a manifest after a folder whose name contains a dot: `-o`/`-O` append `.toml` only when the name has no extension, so `-O photos.2024` writes exactly that name.
- **`zone`** is the time zone used for dates, in the query and in folder names. See [The manifest's time zone](#the-manifests-time-zone).
- **The facts comment** lists the fact keys every source carries, with their type. If a key's fact type is not uniform, it is listed as `mixed`, followed by a `canon facts convert` line for it; generate and refresh also print such keys as a warning. A time fact not written to the second for every source names its coarsest grain and how many values have it, for example `content.Taken time — 12 known only to the year`: a pattern's `|month` finds no value in those.
- **`version`** field tracks the manifest format version.
- **`[options]`** records which `--allow` flags were used during generation. [`cluster refresh`](cluster.md) reads them, because it re-selects sources from the same query. [`apply`](apply.md) reads only `duplicates`, which speaks to the content it is about to transfer; `archived` acknowledged a selection that has already happened, and apply selects nothing. What apply needs acknowledged, it asks for on its own flags.

**Common output patterns:**

```toml
# Structure below the scoped path (default for a scoped generate)
pattern = "{scope.rel_path}"

# Structure below each source's root (default when unscoped)
pattern = "{source.rel_path}"

# Flat - all files in base_dir
pattern = "{filename}"

# By EXIF date
pattern = "{content.DateTimeOriginal|year}/{content.DateTimeOriginal|month}/{filename}"

# By EXIF date with hash prefix (avoids collisions)
pattern = "{content.DateTimeOriginal|year}/{content.DateTimeOriginal|month}/{hash_short}_{filename}"

# By camera model
pattern = "{content.Make}/{content.Model}/{filename}"

# By file type
pattern = "{source.ext|lowercase}/{filename}"
```

See [Pattern Expressions](../../reference/expr.md) for the full syntax reference, including modifiers, path accessors, and aliases.

**Year folders for files known only to the year.** A pattern using `|month` has no value for a time known only to the year. A second manifest takes those files on their own, into year folders:

```toml
query = ["content.DateTimeOriginal|year? AND NOT content.DateTimeOriginal|month?"]
pattern = "{content.DateTimeOriginal|year}/{filename}"
```

## The manifest's time zone

The folder a file goes to can depend on the time zone: a time with an offset, or a file's save time, can fall on a different date in another zone. The manifest records the time zone in its `zone` line, and the lock file carries it, so a manifest puts files in the same folders on whatever machine refreshes or applies it.

- **`cluster generate`** records this machine's zone: a zone name such as `Europe/Amsterdam`, `UTC`, or a fixed offset such as `+03:00`. A machine zone with no name whose offset never changes is recorded as that offset: `TZ=EST5` records `-05:00`, and `TZ=UTC-3` records `+03:00`, because a `TZ` rule counts hours west of UTC, as `date` interprets it. One whose offset changes, such as a `TZ` rule with summer time, has no name to record, so `cluster generate` stops with an error.
- **`cluster refresh`** takes the zone line, selects in that time zone, and writes it into the lock. With no zone line it records this machine's zone and says so. A name that differs only in upper and lower case is written back in its usual spelling.
- **[`apply`](apply.md)** and `cluster status` use the lock's time zone, never this machine's.

Edit the zone line, or delete it to take this machine's, then refresh. When the manifest's time zone is not this machine's, a refresh whose query or pattern uses a time that depends on the time zone says which:

```
times are in Europe/Amsterdam, as the manifest records (this machine: America/New_York)
```

A refresh recording a zone other than the one the old lock was built in says what changes:

```
Time zone changed from Europe/Amsterdam to America/New_York: times in the query and in folder names are now in America/New_York, which can change which sources a time query selects and which folder a file goes to
```

Where the query and the pattern use no time that depends on the time zone (a camera's time with no offset is the same in any time zone), the change is one line in parentheses, and the `times are in …` line does not print. Two names for the same zone, such as `US/Eastern` and `America/New_York`, are not a change.

A zone line Canon does not recognize stops the refresh, which writes nothing:

```
Error: this machine's time zone data does not know "Europe/Amsterdm", the zone line in the manifest — correct the zone line, or update this machine's time zone data
Refresh aborted — manifest.toml and its lock file are unchanged.
```

A zone line holding a rule such as `EST5EDT,M3.2.0,M11.1.0` instead of a name stops the refresh too, with its own message. `cluster generate` stops with an error on a machine zone it cannot record, before it selects anything:

```
Error: TZ=Europe/Amsterdm is not a time zone Canon can read; run it with TZ set to a zone name, for example TZ=Europe/Amsterdam canon cluster generate …
```

A refresh reports these causes together, in one run: the scope's first, then the zone line, then the query. A query Canon cannot run is shown as a `--where` filter's problems are ([When a Filter Is Rejected](../../reference/filter.md#when-a-filter-is-rejected)), as specified in the manifest, followed by the command to fix it:

```
Options from manifest: allow duplicates
Error: cannot run the filter as specified
  source.ext=jpg AND Model=EOS 5D
                              ^ a space separates values
    After a value: AND, OR, ) or the end
`canon cluster refresh --edit manifest.toml` to fix the query and select again
Refresh aborted — manifest.toml and its lock file are unchanged.
```

The query is used as it is written in the manifest, with no aliases. To copy a fix into the manifest, paste it between `'''` and `'''`, which holds both quote marks and backslashes as written: `query = ['''filename="a%b.jpg"''']`.

**What "the same on any machine" rests on.** Zone rules come from each machine's own time zone data. Two machines put a file in the same folder when their data agree for the manifest's zone. `apply --dry-run` on the machine that applies shows the destinations that run will use.

## Refreshing the Lock File

Use `canon cluster refresh` to update the lock file if sources have changed since the manifest was generated:

```bash
# Re-query and update the lock file
canon cluster refresh manifest.toml

# Edit the manifest first, then re-query from what was saved
canon cluster refresh --edit manifest.toml
```

This re-runs the manifest's query and updates `manifest.lock` with the current matching sources. The manifest settings (`[options]`, `[output]`) remain unchanged.

On refresh:
- The **Cluster Summary** is regenerated with current counts
- The **Notes** section is preserved verbatim
- The same root breakdown and archive coverage summary is printed to stdout
- **`meta.scope` is rewritten in the byte-form the index stores**, which is what `cluster generate` records for the same paths — root and the part below it alike. A path retyped in the other Unicode normalization is repaired in the file by the refresh.
- **The lock file records the scopes it was built from, and each entry's path measured from them.** Refresh settles that from the scope it just resolved; [`apply`](apply.md) uses it and does not re-read `meta.scope`. Editing `meta.scope` therefore takes effect on the next refresh, like the filters beside it.

`--edit` opens the manifest in `$VISUAL`/`$EDITOR` before the re-query, so an edited query is the query that runs. The manifest is edited in place. If the editor exits with a failure status, or the saved manifest does not parse, the refresh stops: neither the manifest nor the lock file is written, and the file holds exactly what was saved. Nothing is parsed before the editor opens, so a manifest that no longer parses can be repaired this way.

A path in `meta.scope` that Canon cannot resolve is stated and kept. Refresh narrows a lock rather than deciding where files go, so it continues; the path is written back exactly as it stands, and it contributes nothing, neither to **which files are gathered** nor to **where they land**. Both halves matter: a manifest naming a since-removed drive alongside a live path now locks fewer entries than it did, because the unresolvable path selects nothing. Two kinds, with two lines:

```
no known root at /Volumes/old-laptop/photos/2016 — kept in the manifest; no files come from it, and it does not shape where other files land
no sources known at /Volumes/Photos/2016 — skipped
```

The first names a place under no known root; the second a place Canon knows no sources for. The remaining paths measure from themselves, so what lands where reflects the paths that resolved. A path that stops resolving changes where its siblings land, and names are lost rather than gained.

The two lines are not interchangeable, and the difference shows when nothing else in the scope resolves:

- **Every path skipped for want of sources** stops the refresh. It names every skipped path and leaves the manifest and the lock unchanged.
- **Every path under no known root** continues, because a refresh is the way back from a manifest naming a root that is gone. Nothing is selected — a path Canon cannot resolve selects nothing, never everything — so the lock file is removed and `lock_hash` is emptied, the same as any refresh whose query matches nothing. Edit `meta.scope` first if you want the lock kept.

A path that *is* rewritten comes back in the byte-form the index stores as far as Canon could confirm it: for a skipped path that means its root's form, with the part below the root left as written.

[`apply`](apply.md) does not read `meta.scope`, so it neither rejects a run over these nor repeats them. `cluster status` states them alongside its counts.

### A manifest that reaches a suspended root

A refresh does not run when the manifest's own `meta.scope` names a suspended root, whole, and touches
neither file:

```
/path/to/photos suspended — rejected · canon roots unsuspend path:/path/to/photos
Refresh aborted — manifest.toml and its lock file are unchanged.
```

Refreshing part of it is not offered, and neither is refreshing a scope with one suspended line
beside live ones: a scope is a declaration of what you are assembling, so a lock rebuilt from half
of one would look whole. Exit code 1, with no `Error:` prefix.
[`canon roots unsuspend`](../roots/roots.md) is the way back.

**Lock entries on a suspended root are not a refresh's business.** The lock is Canon's snapshot of
where your files stood when the selection was last made; a refresh asks again, and what stands
behind a closed door is simply not gathered — the new lock is written without it, silently, like
any other act that ignores suspended content. They are rejected where they would be *carried
across*: at [`apply`](apply.md).

`cluster status` states every door the manifest reaches, naming the act each one would turn away:

```
Lock: 1 entries
/path/to/photos suspended — apply would be rejected: origin of 1 source · canon roots unsuspend path:/path/to/photos
Time zone: Europe/Amsterdam (the lock's)
```

A suspended destination is stated the same way. A root the manifest reaches through its scope
*and* its lock is named once, with both acts in the line
(`cluster refresh and apply would be rejected`).

Each next step is withheld where its own act cannot run — which a door is one cause of, and not the
only one. An apply invitation is withheld by a suspended lock entry or destination; the lock rebuild
by a suspended scope line, and equally by a recorded scope that resolves to nothing. Where a rebuild
is the act that brings the apply back within reach, the report offers it:

```
To rebuild the lock: canon cluster refresh manifest.toml
```

That includes a lock whose hash no longer matches the manifest: the lock is unusable as it stands,
and rebuilding it is what makes the apply possible again.

A suspended *destination* gets no step at all: no rebuild changes where the files are going, and
the way back was stated with the door itself — a suspended root is always named with the command
that unsuspends it.

The rebuild is withheld in two further cases, and each says what it owes instead.

If the only thing a rebuild would change is the *wording* of the message you are already reading,
it is not offered — the report names an act when the act gets you further, not when it merely gives
the same answer differently. In that case the problem is a pattern the lock cannot answer, so what you get in
the rebuild's place is the pattern remedy — the same lines [`apply`](apply.md) prints when it
reaches the pattern, and which pair depends on what the pattern is missing, because each takes
different advice. `apply` checks the lock before the pattern, so on a lock written before the
measurement it says to rebuild first. The report offers a rebuild where it clears what stops the
apply: a lock without a header, a hash mismatch, a lost lock, and a pattern that needs a scope
the lock lacks when `meta.scope` names a place with sources to measure from. A pattern that fails
for a missing fact is cleared by no rebuild, so it gets the fact remedy however old the lock; the
lock's age is then stated on the `Lock:` line, in the words of `apply`'s error:

```
Lock: 2 entries — built before lock files recorded the scopes they were built from
```

A pattern that places files relative to the scope, over a lock that records none:

```
The pattern places files relative to the scopes declared in the manifest, and this lock records none.
Use {source.rel_path} to keep each file's own folders, or add paths to meta.scope and refresh.
```

Declaring a scope and refreshing is the second remedy taken; a manifest that already declares one
is offered the refresh directly.

`{scope.rel_path}` is not a fact, so there is no fact coverage to check here. A pattern naming a
fact your sources do not carry gets the other answer:

```
Adjust the pattern in your manifest, or import the facts it names.
Use 'canon facts' to see which sources carry them.
```

A pattern using a file's save time that Canon cannot read, one at the edge of the years
0000–9999 or beyond, gets a third pair. The file has a save time, so no import supplies it:

```
1 of 240 entries have no destination:
  Canon cannot read this file's save time: it lies at the edge of the years 0000–9999 or beyond
  /photos/2016/IMG_0001.jpg
```

```
A save time Canon cannot read gives the pattern no value.
Add "source.mtime?" to the manifest's query to leave such files out, or correct the file's save time and scan again; then refresh.
```

Where both a missing fact and such a save time stop the pattern, both pairs print, the fact pair
first; where the pattern also needs a scope the lock lacks, the scope pair prints alone. A modifier
that asks for a finer part than a time carries gets a fourth pair, naming the key and modifier that
failed as the query entry that leaves such files out:

```
A time known only to a coarser grain than the pattern uses gives the pattern no value.
Use a coarser modifier in the pattern, which takes effect at the next apply; or add "content.DateTimeOriginal|month?" to the manifest's query to leave such files out, then refresh.
```

The query and a file's save time are both evaluated when the lock is built, so those ways out take
effect at the refresh, and `cluster status` names the apply once the rebuilt lock places every file.
The pattern is evaluated when files are placed, so a coarser modifier needs no refresh. `cluster refresh`
states the same cause with its counts, over the lock it has just written:

```
12 sources hold content.DateTimeOriginal only to the year, so {content.DateTimeOriginal|month} has no value for them
```

If the recorded scope resolves to nothing — every path in it set aside, so the report has just
printed `no sources known at …` — a refresh is rejected outright, and pointing at one would only
earn a second rejection. What you get instead is the pair `cluster refresh` prints in that same
state, under a line saying which of the report's own lines they answer:

```
No sources are known under the scopes declared in this manifest, so the lock cannot be rebuilt:
If the files are there, `canon scan /photos/2019` to index them.
If not, edit meta.scope, then `canon cluster refresh manifest.toml` to rewrite the lock.
```

Canon cannot tell a folder it has never scanned from a folder named by mistake, so it offers both
and leads with the one that costs nothing to be wrong about.

What a rebuild would *find* is a different kind of question, and the report does not guess at it: a
refresh re-queries the open world, so a lock standing entirely on suspended roots may come back
holding content gathered since, or may match nothing and be removed. The report names the command;
the refresh states its own outcome. A lock is what your query, your scope and the index add up to,
so unsuspending and refreshing again rebuilds it from the same query and scope — over the world as
it stands then, which is not always the world it described before.

When the query matches nothing, the lock file is removed and `lock_hash` is emptied. The manifest is rewritten in full, with the Cluster Summary stating the zero match and the Notes section preserved as on any other refresh. `cluster status` reads such a manifest and reports the absence rather than failing on it:

```
Lock: none — the last refresh matched nothing.
```

A manifest that still records a `lock_hash` but whose lock file is gone gets `Lock: none — the manifest names a lock file that is not there.` [`apply`](apply.md) stops with an error in both cases: there is nothing to carry across.

### The zone in `cluster status`

`cluster status` works out each destination in the lock's time zone and names it under the `Lock:` line. Where the manifest's zone line says something a refresh would change, a second line says so:

```
Time zone: Europe/Amsterdam (the lock's)
  the manifest's zone line says America/New_York; canon cluster refresh manifest.toml brings the lock up to date
```

```
Time zone: America/New_York (the lock's)
  the manifest has no zone line; a refresh on this machine would record Europe/Paris: canon cluster refresh manifest.toml
```

A zone line Canon does not recognize, and a query Canon cannot run (taken with no aliases), are named as what stops the refresh (`cluster refresh would be rejected: …`, in the refresh's own words), and the report then names no rebuild. A query's problems are shown under that line as a refresh shows them:

```
cluster refresh would be rejected: the recorded query cannot run — `canon cluster refresh --edit manifest.toml` to fix the query and select again
    source.ext=jpg AND Model=EOS 5D
                                ^ a space separates values
      After a value: AND, OR, ) or the end
```

A fix is copied into the manifest as for a refresh, between `'''` and `'''`. Neither stops an apply, which uses the lock's time zone and re-selects nothing.

A lock that records no zone Canon recognizes gets no destinations and no counts: the summary names its entries once, with their source files unchecked, and the rebuild is the step. A lock built before lock files recorded the zone says so on its `Lock:` line; a lock naming a zone this machine's time zone data does not know names the ways out:

```
Lock: 1 entries
Time zone: Europe/Kyiv (the lock's)
  This machine's time zone data does not know Europe/Kyiv, the zone the lock was built in.
    update this machine's time zone data, or
    apply on the machine that built the lock, or
    change the manifest's zone line to a name this machine knows for the same zone (for example, an older spelling), then canon cluster refresh manifest.toml

Summary: 1 entry with no destination computed, and its source file not checked: the lock records no time zone this machine can read.
```
