# Querying

After scanning and enriching, you can explore your indexed files.

- [`ls`](ls.md) - List sources matching [filter expressions](../../reference/filter.md)
- [`facts`](facts.md) - Discover available facts and check coverage
- [`compare`](compare.md) - Compare directories to find overlap
- [`survey`](survey.md) - Survey a selection for archive status, related locations, and unique content
- [`sweep`](sweep.md) - Rank the universe's reduction opportunities — the places worth visiting next

All query commands except `sweep` support path scoping (limit to a subdirectory) and `--where` filters; the sweep is inherently universe-wide.

**Scope defaulting**: When no paths are given, query commands scope to the current directory if it's inside a known root. If the current directory is not inside any root, commands operate globally across all roots. Use `--global` to force global scope while inside a root.