//! Facts' operations layer: `report` computes value distributions and key
//! enumerations (`canon facts`); `maintain` holds the delete/prune plan-execute
//! pairs; `import` writes one batch of JSONL records per write transaction (`import-facts`);
//! `convert` reads, plans and performs a key's conversion (`facts convert`).

pub(super) mod convert;
pub(super) mod import;
pub(super) mod maintain;
pub(super) mod report;
