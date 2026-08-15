//! Facts' operations layer: `report` computes value distributions and key
//! enumerations (`canon facts`); `maintain` holds the delete/prune plan-execute
//! pairs; `import` holds the per-record JSONL import logic (`import-facts`).

pub(super) mod import;
pub(super) mod maintain;
pub(super) mod report;
