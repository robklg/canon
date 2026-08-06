# canon prune

Clean up orphaned or stale data from the database.

```bash
# Preview stale facts (file changed since fact was recorded)
canon prune --stale-facts

# Preview orphaned objects (no present sources reference them)
canon prune --orphaned-objects

# Preview facts for excluded sources/objects
canon prune --excluded-facts
canon prune --excluded-facts=source   # Only source facts
canon prune --excluded-facts=object   # Only object facts

# Execute deletion
canon prune --stale-facts --yes
canon prune --orphaned-objects --yes
canon prune --excluded-facts --yes
```

**Stale facts** are facts whose `observed_basis_rev` no longer matches the source's
current `basis_rev` (the file was modified after the fact was imported).

**Orphaned objects** are content entries with no remaining present sources. This can
happen when files are deleted. They can serve as a historical record; pruning them
frees database space.

**Excluded facts** are metadata for sources or objects marked as excluded. Pruning
them frees database space.

All prune operations are dry-run by default. Add `--yes` to execute.
