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

The command generates two files: a manifest (`.toml`) that you edit, and a lock file (`.lock`) holding what the run settled: the resolved scope, and one line per source recording its state and where it goes. The lock is not edited by hand: [`apply`](apply.md) reads it as written.

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
```

**Empty files are never skipped as "already archived".** A zero-byte file is
[contentless](../../concepts/object.md#empty-files-are-contentless), so
archive detection ignores it and the manifest carries it with its folder.

**Manifest structure:**

The generated manifest includes a cluster summary, a notes section for your own annotations, and comments listing available pattern variables:

```toml
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

  Writing the manifest inside the destination is fine, but a manifest whose own path is a directory the pattern needs blocks every file below it. Generate and refresh warn when that is the case, naming both paths; [`apply`](apply.md) refuses the run. This is easy to hit by naming a manifest after a folder whose name contains a dot: `-o`/`-O` append `.toml` only when the name has no extension, so `-O photos.2024` writes exactly that name.
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

## Refreshing the Lock File

Use `canon cluster refresh` to update the lock file if sources have changed since the manifest was generated:

```bash
# Re-query and update the lock file
canon cluster refresh manifest.toml

# Edit the manifest first, then re-query from what was saved
canon cluster refresh manifest.toml --edit
```

This re-runs the manifest's query and updates `manifest.lock` with the current matching sources. The manifest settings (`[options]`, `[output]`) remain unchanged.

On refresh:
- The **Cluster Summary** is regenerated with current counts
- The **Notes** section is preserved verbatim
- The same root breakdown and archive coverage summary is printed to stdout
- **`meta.scope` is rewritten in the byte-form the index stores**, which is what `cluster generate` records for the same paths — root and the part below it alike. A path retyped in the other Unicode normalization is repaired in the file by the refresh.
- **The lock file records the scopes it was built from, and each entry's path measured from them.** Refresh settles that from the scope it just resolved; [`apply`](apply.md) reads it and does not re-read `meta.scope`. Editing `meta.scope` therefore takes effect on the next refresh, like the filters beside it.

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

[`apply`](apply.md) does not read `meta.scope`, so it neither refuses on these nor repeats them. `cluster status` states them alongside its counts.

### A manifest that reaches a suspended root

A refresh refuses when the manifest's own `meta.scope` names a suspended root, whole, and touches
neither file:

```
/path/to/photos suspended — refused · canon roots unsuspend path:/path/to/photos
Refresh aborted — manifest.toml and its lock file are unchanged.
```

Refreshing part of it is not offered, and neither is refreshing a scope with one suspended line
beside live ones: a scope is a declaration of what you are assembling, so a lock rebuilt from half
of one would read as whole. Exit code 1, with no `Error:` prefix.
[`canon roots unsuspend`](../roots/roots.md) is the way back.

**Lock entries on a suspended root are not a refresh's business.** The lock is Canon's snapshot of
where your files stood when the selection was last made; a refresh asks again, and what stands
behind a closed door is simply not gathered — the new lock is written without it, silently, like
any other act that ignores suspended content. They are refused where they would be *carried
across*: at [`apply`](apply.md).

`cluster status` states every door the manifest reaches, naming the act each one would turn away:

```
Lock: 1 entries
/path/to/photos suspended — apply would refuse: origin of 1 source · canon roots unsuspend path:/path/to/photos
```

A suspended destination is stated the same way. A root the manifest reaches through its scope
*and* its lock is named once, with both acts in the line
(`cluster refresh and apply would refuse`).

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

If the only thing a rebuild would change is the *wording* of the refusal you are already reading,
it is not offered — the report names an act when the act gets you further, not when it merely gives
the same answer differently. That refusal is a pattern the lock cannot answer, so what you get in
the rebuild's place is the pattern remedy — the same two lines [`apply`](apply.md) prints when it
reaches the pattern, and which pair depends on what the pattern is missing, because the two take
opposite advice. `apply` checks the lock before the pattern, so on a lock written before the
measurement it says to rebuild first. The report offers a rebuild where it clears what stops the
apply: a lock without a header, a hash mismatch, a lost lock, and a pattern that needs a scope
the lock lacks when `meta.scope` names a place with sources to measure from. A pattern that fails
for a missing fact is cleared by no rebuild, so it gets the fact remedy however old the lock; the
lock's age is then stated on the `Lock:` line, in the words `apply` will refuse with:

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

If the recorded scope resolves to nothing — every path in it set aside, so the report has just
printed `no sources known at …` — a refresh is refused outright, and pointing at one would only
earn a second refusal. What you get instead is the pair `cluster refresh` prints in that same
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

A manifest that still records a `lock_hash` but whose lock file is gone reads `Lock: none — the manifest names a lock file that is not there.` [`apply`](apply.md) refuses in both cases: there is nothing to carry across.
