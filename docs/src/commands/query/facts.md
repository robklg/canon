# canon facts

Discover what metadata you have and check coverage.

```bash
# Overview of all facts (source roots only by default)
canon facts

# Scoped to a directory
canon facts /path/to/photos

# With filters
canon facts --where 'source.ext=jpg'

# Value distribution for a specific fact
canon facts --key content.Make

# With modifiers: group mtime by year-month
canon facts --key source.mtime|yearmonth

# With accessors: distribution by top-level directory
canon facts --key source.rel_path[0]

# Combine accessor and modifier: distribution by filename extension
canon facts --key source.rel_path[-1]|ext

# Show hidden built-in facts
canon facts --all

# Unlimited results (default is 50)
canon facts --key content.hash.sha256 --limit 0

# Include sources from archive roots
canon facts --include-archived

# Group by root (see which roots contribute to each value)
canon facts --key source.ext --by-root

# Group by any fact key (with modifiers)
canon facts --key source.ext --group-by 'source.mtime|year'

# Compound grouping (root + another fact)
canon facts --key source.ext --by-root --group-by 'content.Make'
```

Example output:
```
Sources matching filters: 34692

Fact                               Count   Coverage
────────────────────────────────────────────────────
source.ext                         34692     100.0%  (built-in)
source.size                        34692     100.0%  (built-in)
source.mtime                       34692     100.0%  (built-in)
source.path                        34692     100.0%  (built-in)
content.hash.sha256                34692     100.0%
content.mime                       34692     100.0%
content.Model                       7935      22.9%
content.Make                        7935      22.9%
...
```

Example grouped output (`--by-root`):
```
source.ext (by root)

jpg (total: 12,500, 36.0%)
  id:1  ...stack/Backup/Pictures          8,000   64.0%
  id:2  ...castor-import/gringo           4,500   36.0%

png (total: 8,200, 23.6%)
  id:1  ...stack/Backup/Pictures          5,000   61.0%
  id:3  ...castor-import/hydra            3,200   39.0%
```

## canon facts delete

Delete facts by key. Useful for removing incorrect or unwanted metadata.

```bash
# Preview deletion (dry-run by default)
canon facts delete content.mime --on object
canon facts delete content.Make --on source /path/to/photos --where 'source.ext=jpg'

# Execute deletion
canon facts delete content.mime --on object --yes
```

- `--on source` or `--on object` is required to specify entity type
- Protected namespaces (`source.*`, `policy.*`) cannot be deleted
- Dry-run by default; use `--yes` to execute

## canon prune

Clean up orphaned or stale data from the database.

```bash
# Preview stale facts (file changed since fact was recorded)
canon prune --stale-facts

# Preview orphaned objects (no present sources reference them)
canon prune --orphaned-objects

# Execute deletion
canon prune --stale-facts --yes
canon prune --orphaned-objects --yes
```

**Stale facts** are those where `observed_basis_rev` no longer matches the source's current `basis_rev` (meaning the file was modified after the fact was imported).

**Orphaned objects** are content entries with no remaining present sources. This can happen when files are deleted. You may want to keep them as a historical record, or delete them to clean up the database.
