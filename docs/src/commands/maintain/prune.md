# canon prune

Clean up orphaned or stale data from the database.

```bash
# Preview stale facts (stored on a source whose file changed since the fact was recorded)
canon prune --stale-facts

# Preview orphaned objects (no present sources reference them)
canon prune --orphaned-objects

# Preview facts stored on excluded sources and on excluded content
canon prune --excluded-facts
canon prune --excluded-facts=source   # Only facts stored on excluded sources
canon prune --excluded-facts=object   # Only facts stored on excluded content

# Execute deletion
canon prune --stale-facts --yes
canon prune --orphaned-objects --yes
canon prune --excluded-facts --yes
```

**Orphaned objects** are content entries with no remaining present sources, which can
happen when files are deleted. They can serve as a historical record; pruning deletes
them along with their facts and their non-present sources.
