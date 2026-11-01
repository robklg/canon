# Querying

- [`ls`](ls.md): sources matching [filter expressions](../../reference/filter.md)
- [`facts`](facts.md): available facts and their coverage
- [`compare`](compare.md): overlap between two directories
- [`survey`](survey.md): a selection's archive status, related locations and unique content
- [`sweep`](sweep.md): reduction opportunities across all roots, ranked
- [`trail`](trail.md): the decision trail
  - [`trail crossings`](trail-crossings.md): what was transferred between two places

`ls`, `facts`, `compare` and `survey` take `--where` filters. Every command here but `sweep` takes a path. [Common options](../index.md#common-options) covers scope defaulting.
