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
canon facts --include archived

# Include excluded sources
canon facts --include excluded

# Include both
canon facts --include all

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

See also: [`facts delete`](../maintain/facts-delete.md) for removing incorrect metadata, [`prune`](../maintain/prune.md) for cleaning up stale or orphaned data.
