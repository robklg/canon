//! Exclude's receipt bodies and the mappers that build them: the durable
//! document types, plus the functions turning plan and stamp-set data into
//! them. Most are pure; `item_for_source` resolves a source's content hash and
//! so takes a connection. `run_exclusion` (the transaction/commit/write-file
//! orchestration these mappers feed) lives in `runner.rs` — kept separate so
//! the subsystem's one transaction site isn't buried under six small mappers.
//!
//! The document shapes are exclude's own — the shared machinery serializes any
//! body and never inspects one, so the only part of a receipt that is common
//! across commands is its `[meta]` table.

use std::collections::HashMap;

use anyhow::Result;
use serde::Serialize;

use crate::core::domain::decision::DecisionStatus;
use crate::core::domain::source::Source;
use crate::core::ops::decision::{DecisionCounts, DecisionParams};
use crate::core::ops::receipt::{ReceiptKind, ReceiptMeta};
use crate::core::repo::{self, Connection};
use crate::exclude::repo as exclude_repo;

use super::types::{DuplicateGroupData, ExcludeItemData};

/// Source-level exclusion receipt — `exclude set`/`exclude clear` and the
/// single-source `set_by_id`/`set_by_path`. Flat list of affected sources.
#[derive(Serialize)]
pub(super) struct ExcludeReceipt {
    pub meta: ReceiptMeta,
    pub items: Vec<ExcludeReceiptItem>,
}

/// One item in an exclusion receipt — a single source whose state transitioned.
#[derive(Serialize)]
pub(super) struct ExcludeReceiptItem {
    pub root: String,
    pub rel_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
    pub size: i64,
    pub mtime: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_decision_id: Option<i64>,
}

/// Duplicate-exclusion receipt — `exclude duplicates`. Grouped by content hash,
/// recording which copy was kept vs which were excluded.
#[derive(Serialize)]
pub(super) struct DuplicatesReceipt {
    pub meta: ReceiptMeta,
    pub groups: Vec<DuplicateGroup>,
}

/// One duplicate group: the shared content hash, the kept copy/copies (under
/// the prefer prefix), and the excluded duplicates.
#[derive(Serialize)]
pub(super) struct DuplicateGroup {
    pub hash: String,
    pub kept: Vec<DuplicateKeptEntry>,
    pub excluded: Vec<DuplicateExcludedEntry>,
}

/// A kept copy in a duplicate group — no state transition, so no
/// `previous_decision_id`.
#[derive(Serialize)]
pub(super) struct DuplicateKeptEntry {
    pub root: String,
    pub rel_path: String,
    pub size: i64,
    pub mtime: i64,
}

/// An excluded copy in a duplicate group. The content hash lives on the group,
/// so it is not repeated per entry.
#[derive(Serialize)]
pub(super) struct DuplicateExcludedEntry {
    pub root: String,
    pub rel_path: String,
    pub size: i64,
    pub mtime: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_decision_id: Option<i64>,
}

/// Object-level exclusion receipt — `exclude set-object`/`clear-object`.
/// Grouped by content hash; each object lists its full stamp-set.
#[derive(Serialize)]
pub(super) struct ObjectExcludeReceipt {
    pub meta: ReceiptMeta,
    pub objects: Vec<ObjectExcludeEntry>,
}

/// One object in an object-exclusion receipt: the content hash and every
/// source the object-level `decision_id` stamp touched (the stamp-set) —
/// including tombstone rows, so the stamp is reconstructable from disk.
#[derive(Serialize)]
pub(super) struct ObjectExcludeEntry {
    pub hash: String,
    pub sources: Vec<ObjectSourceReceiptEntry>,
}

/// A source sharing an excluded object's content.
#[derive(Serialize)]
pub(super) struct ObjectSourceReceiptEntry {
    pub root: String,
    pub rel_path: String,
    pub size: i64,
    pub mtime: i64,
    /// `false` for a tombstone row (`present = 0`) — the file was already gone
    /// when the stamp touched it. Omitted for present sources.
    #[serde(skip_serializing_if = "is_true")]
    pub present: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_decision_id: Option<i64>,
}

/// Serde helper: skip a `bool` field when `true` (serialize only the
/// exceptional `false` case).
fn is_true(b: &bool) -> bool {
    *b
}

/// Counts for an all-succeeded decision (attempted == completed == n, no
/// failures or skips). Records `Some(0)` for failed/skipped to match apply's
/// convention (explicit zero, not NULL).
pub(super) fn counts_all(n: usize) -> DecisionCounts {
    DecisionCounts {
        attempted: Some(n as i64),
        completed: Some(n as i64),
        failed: Some(0),
        skipped: Some(0),
    }
}

pub(super) fn exclude_receipt_items(items: &[ExcludeItemData]) -> Vec<ExcludeReceiptItem> {
    items
        .iter()
        .map(|i| ExcludeReceiptItem {
            root: i.root.clone(),
            rel_path: i.rel_path.clone(),
            hash: i.hash.clone(),
            size: i.size,
            mtime: i.mtime,
            previous_decision_id: i.previous_decision_id,
        })
        .collect()
}

pub(super) fn duplicate_receipt_groups(groups: &[DuplicateGroupData]) -> Vec<DuplicateGroup> {
    groups
        .iter()
        .map(|g| DuplicateGroup {
            hash: g.hash.clone(),
            kept: g
                .kept
                .iter()
                .map(|k| DuplicateKeptEntry {
                    root: k.root.clone(),
                    rel_path: k.rel_path.clone(),
                    size: k.size,
                    mtime: k.mtime,
                })
                .collect(),
            excluded: g
                .excluded
                .iter()
                .map(|e| DuplicateExcludedEntry {
                    root: e.root.clone(),
                    rel_path: e.rel_path.clone(),
                    size: e.size,
                    mtime: e.mtime,
                    previous_decision_id: e.previous_decision_id,
                })
                .collect(),
        })
        .collect()
}

/// Map an object's stamp-set (from `fetch_object_sharers_for_receipt`) into
/// receipt entries, sorted role DESC then root/rel_path — the same ordering as
/// the ceremony display. The entries mirror exactly the set
/// `set_decision_id_by_object` touches, including tombstone rows (marked
/// `present = false`), so the stamp is reconstructable from disk.
pub(super) fn object_stamp_set_entries(
    mut sharers: Vec<exclude_repo::source::ObjectReceiptSource>,
) -> Vec<ObjectSourceReceiptEntry> {
    sharers.sort_by(|a, b| {
        b.root_role
            .cmp(&a.root_role)
            .then_with(|| a.root_path.cmp(&b.root_path))
            .then_with(|| a.rel_path.cmp(&b.rel_path))
    });
    sharers
        .into_iter()
        .map(|s| ObjectSourceReceiptEntry {
            root: s.root_path,
            rel_path: s.rel_path,
            size: s.size,
            mtime: s.mtime,
            present: s.present,
            previous_decision_id: s.previous_decision_id,
        })
        .collect()
}

/// Build a single-object exclusion receipt (set-object / clear-object). `kind`
/// distinguishes the two (`ExcludeObject` vs `RestoreObject`); `locus` is the
/// receipt's anchoring root, from its placement.
pub(super) fn object_exclude_receipt(
    decision: &DecisionParams,
    decision_id: i64,
    locus: (i64, &str),
    kind: ReceiptKind,
    hash: &str,
    sources: Vec<ObjectSourceReceiptEntry>,
    summary: &str,
) -> ObjectExcludeReceipt {
    ObjectExcludeReceipt {
        meta: decision.receipt_meta(
            decision_id,
            DecisionStatus::Completed,
            summary,
            locus,
            kind,
            None,
        ),
        objects: vec![ObjectExcludeEntry {
            hash: hash.to_string(),
            sources,
        }],
    }
}

/// Build an `ExcludeItemData` for a single source, resolving its content hash.
pub(super) fn item_for_source(conn: &Connection, source: &Source) -> Result<ExcludeItemData> {
    let objects = match source.object_id {
        Some(oid) => repo::object::batch_fetch_by_ids(conn, &[oid])?,
        None => HashMap::new(),
    };
    Ok(ExcludeItemData::from_source(source, &objects))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::ops::receipt::{write_receipt, ReceiptLocus};
    use tempfile::tempdir;

    fn sample_meta(command: &str) -> ReceiptMeta {
        ReceiptMeta {
            receipt_version: 1,
            decision_id: 42,
            command: command.to_string(),
            transition: "excluded".to_string(),
            posture: "performed".to_string(),
            status: "completed".to_string(),
            timestamp: 1700000000,
            scope: None,
            reason: None,
            summary: "test".to_string(),
            canon_version: "0.0.0".to_string(),
            command_line: "canon test".to_string(),
            manifest: None,
            origin_disposition: None,
            locus: ReceiptLocus {
                path: "/vol".to_string(),
                id: 42,
            },
        }
    }

    #[test]
    fn test_exclude_receipt_serializes() {
        let receipt = ExcludeReceipt {
            meta: sample_meta("exclude_set"),
            items: vec![
                ExcludeReceiptItem {
                    root: "/vol".to_string(),
                    rel_path: "a.dll".to_string(),
                    hash: Some("sha256:abc".to_string()),
                    size: 10,
                    mtime: 100,
                    previous_decision_id: Some(12),
                },
                ExcludeReceiptItem {
                    root: "/vol".to_string(),
                    rel_path: "b.tmp".to_string(),
                    hash: None,
                    size: 0,
                    mtime: 200,
                    previous_decision_id: None,
                },
            ],
        };
        let out = toml::to_string_pretty(&receipt).unwrap();
        assert!(out.contains("[[items]]"));
        assert!(out.contains("hash = \"sha256:abc\""));
        // Unhashed item omits hash; only the Some item carries it.
        assert_eq!(out.matches("hash =").count(), 1);
        // Only the item with a prior decision carries previous_decision_id.
        assert_eq!(out.matches("previous_decision_id").count(), 1);
        assert!(out.contains("previous_decision_id = 12"));
    }

    #[test]
    fn test_duplicates_receipt_serializes() {
        let receipt = DuplicatesReceipt {
            meta: sample_meta("exclude_duplicates"),
            groups: vec![DuplicateGroup {
                hash: "sha256:dup".to_string(),
                kept: vec![DuplicateKeptEntry {
                    root: "/vol".to_string(),
                    rel_path: "orig.jpg".to_string(),
                    size: 5,
                    mtime: 1,
                }],
                excluded: vec![DuplicateExcludedEntry {
                    root: "/vol".to_string(),
                    rel_path: "copy.jpg".to_string(),
                    size: 5,
                    mtime: 1,
                    previous_decision_id: Some(9),
                }],
            }],
        };
        let out = toml::to_string_pretty(&receipt).unwrap();
        assert!(out.contains("[[groups]]"));
        assert!(out.contains("hash = \"sha256:dup\""));
        assert!(out.contains("[[groups.kept]]"));
        assert!(out.contains("[[groups.excluded]]"));
        // Kept has no previous_decision_id field; only the excluded entry carries it.
        assert_eq!(out.matches("previous_decision_id").count(), 1);
        assert!(out.contains("previous_decision_id = 9"));
    }

    #[test]
    fn test_object_exclude_receipt_serializes() {
        let receipt = ObjectExcludeReceipt {
            meta: sample_meta("exclude_set_object"),
            objects: vec![ObjectExcludeEntry {
                hash: "sha256:obj".to_string(),
                sources: vec![
                    ObjectSourceReceiptEntry {
                        root: "/vol".to_string(),
                        rel_path: "dup.bin".to_string(),
                        size: 4,
                        mtime: 7,
                        present: true,
                        previous_decision_id: None,
                    },
                    ObjectSourceReceiptEntry {
                        root: "/vol".to_string(),
                        rel_path: "gone.bin".to_string(),
                        size: 4,
                        mtime: 7,
                        present: false,
                        previous_decision_id: Some(3),
                    },
                ],
            }],
        };
        let out = toml::to_string_pretty(&receipt).unwrap();
        assert!(out.contains("[[objects]]"));
        assert!(out.contains("hash = \"sha256:obj\""));
        assert!(out.contains("[[objects.sources]]"));
        // `present` is serialized only for the exceptional tombstone entry;
        // the present entry's None previous_decision_id is omitted entirely.
        assert_eq!(out.matches("present = ").count(), 1);
        assert!(out.contains("present = false"));
        assert_eq!(out.matches("previous_decision_id").count(), 1);
        assert!(out.contains("previous_decision_id = 3"));
    }

    /// Each of exclude's three documents survives the real write path and reads
    /// back as TOML. The tests above serialize the bodies; this one pins the
    /// *files* — the writer prepends a comment header and writes to an
    /// `.incomplete` path, and neither is exercised anywhere else.
    #[test]
    fn exclude_receipts_round_trip_through_the_writer() {
        fn write_back<T: Serialize>(dir: &std::path::Path, name: &str, receipt: &T) -> toml::Table {
            write_receipt(&dir.join(format!("{name}.toml")), receipt, "test").unwrap();
            let written = std::fs::read_to_string(dir.join(format!("{name}.incomplete"))).unwrap();
            toml::from_str::<toml::Table>(&written)
                .unwrap_or_else(|e| panic!("{name} must parse back as TOML: {e}\n{written}"))
        }
        let dir = tempdir().unwrap();

        let flat = ExcludeReceipt {
            meta: sample_meta("exclude_set"),
            items: vec![ExcludeReceiptItem {
                root: "/vol".to_string(),
                rel_path: "a.dll".to_string(),
                hash: Some("sha256:abc".to_string()),
                size: 10,
                mtime: 100,
                previous_decision_id: Some(12),
            }],
        };
        let parsed = write_back(dir.path(), "flat", &flat);
        assert_eq!(parsed["items"].as_array().unwrap().len(), 1);

        let duplicates = DuplicatesReceipt {
            meta: sample_meta("exclude_duplicates"),
            groups: vec![DuplicateGroup {
                hash: "sha256:dup".to_string(),
                kept: vec![DuplicateKeptEntry {
                    root: "/vol".to_string(),
                    rel_path: "orig.jpg".to_string(),
                    size: 5,
                    mtime: 1,
                }],
                excluded: vec![DuplicateExcludedEntry {
                    root: "/vol".to_string(),
                    rel_path: "copy.jpg".to_string(),
                    size: 5,
                    mtime: 1,
                    previous_decision_id: Some(9),
                }],
            }],
        };
        let parsed = write_back(dir.path(), "duplicates", &duplicates);
        assert_eq!(parsed["groups"][0]["kept"].as_array().unwrap().len(), 1);

        let objects = ObjectExcludeReceipt {
            meta: sample_meta("exclude_set_object"),
            objects: vec![ObjectExcludeEntry {
                hash: "sha256:obj".to_string(),
                sources: vec![ObjectSourceReceiptEntry {
                    root: "/vol".to_string(),
                    rel_path: "gone.bin".to_string(),
                    size: 4,
                    mtime: 7,
                    present: false,
                    previous_decision_id: Some(3),
                }],
            }],
        };
        let parsed = write_back(dir.path(), "objects", &objects);
        assert_eq!(parsed["objects"][0]["sources"].as_array().unwrap().len(), 1);
    }
}
