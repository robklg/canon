//! Receipt writing for Canon decisions.
//!
//! Receipts are TOML files written alongside archive content, capturing
//! the per-item detail of each decision. The decision record gives you
//! the *why*; the receipt gives you the *what specifically*.

use std::path::Path;

use anyhow::{Context, Result};
use serde::Serialize;

use crate::domain::config::{LedgerConfig, ReceiptLayout};
use crate::domain::root::Root;
use crate::ops::fs::{finalize_file, write_file_incomplete};

/// Reference to a receipt file on disk, stored in the decision record.
pub struct ReceiptRef {
    pub root_id: i64,
    pub rel_path: String,
}

/// Where a receipt file should be written.
///
/// `Targeted` is for decisions that write to a specific archive location (apply):
/// the receipt mirrors the destination path under the archive's `.canon-ledger/`,
/// per the `layout` setting. `LedgerRoot` is for non-targeted decisions
/// (exclusions, and future scan/roots-rm): the receipt lands flat in the ledger
/// root's `.canon-ledger/`, independent of layout.
pub enum ReceiptPlacement {
    Targeted {
        archive_root_id: i64,
        archive_root_path: String,
        /// Relative base directory within the archive root (from manifest config).
        base_dir_rel: String,
    },
    /// Constructed by the exclusion path in Phase 3 (`src/exclude.rs`); tests construct it now.
    #[allow(dead_code)]
    LedgerRoot { root_id: i64, root_path: String },
}

/// Shared meta section for all receipt types.
/// Serializes as the `[meta]` TOML table.
#[derive(Serialize)]
pub struct ReceiptMeta {
    pub receipt_version: u32,
    pub decision_id: i64,
    pub command: String,
    pub timestamp: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub summary: String,
    pub canon_version: String,
    pub command_line: String,
    /// Manifest path — apply receipts only. Omitted for other receipt types.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manifest: Option<String>,
}

/// Apply-specific receipt.
#[derive(Serialize)]
pub struct ApplyReceipt {
    pub meta: ReceiptMeta,
    pub items: Vec<ApplyReceiptItem>,
}

/// One item in an apply receipt — a single completed file transfer.
#[derive(Serialize)]
pub struct ApplyReceiptItem {
    pub source_root: String,
    pub source_rel_path: String,
    pub destination_rel_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
    pub size: i64,
    pub mtime: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_decision_id: Option<i64>,
}

// ---------------------------------------------------------------------------
// Path computation (pure functions, no I/O)
// ---------------------------------------------------------------------------

/// Format the receipt filename: 6-digit zero-padded decision_id + command.
///
/// Examples: `000043-apply.toml`, `1000000-apply.toml` (no truncation beyond 6 digits)
pub fn receipt_filename(decision_id: i64, command: &str) -> String {
    format!("{:06}-{}.toml", decision_id, command)
}

/// Compute the receipt rel_path within an archive root for targeted receipts (apply).
///
/// - Central: `.canon-ledger/{base_dir_rel}/{filename}`, or `.canon-ledger/{filename}` if empty
/// - Alongside: `{base_dir_rel}/.canon-ledger/{filename}`, or `.canon-ledger/{filename}` if empty
pub fn compute_targeted_receipt_rel_path(
    decision_id: i64,
    command: &str,
    base_dir_rel: &str,
    layout: &ReceiptLayout,
) -> String {
    let filename = receipt_filename(decision_id, command);
    match layout {
        ReceiptLayout::Central => {
            if base_dir_rel.is_empty() {
                format!(".canon-ledger/{filename}")
            } else {
                format!(".canon-ledger/{base_dir_rel}/{filename}")
            }
        }
        ReceiptLayout::Alongside => {
            if base_dir_rel.is_empty() {
                format!(".canon-ledger/{filename}")
            } else {
                format!("{base_dir_rel}/.canon-ledger/{filename}")
            }
        }
    }
}

/// Compute the receipt rel_path for non-targeted receipts (exclusions, scan).
///
/// Flat at the ledger root: `.canon-ledger/{filename}` — no destination
/// subdirectory, independent of layout.
pub fn compute_ledger_root_receipt_rel_path(decision_id: i64, command: &str) -> String {
    format!(".canon-ledger/{}", receipt_filename(decision_id, command))
}

/// Resolve which archive root holds non-targeted receipts.
///
/// Uses `config.root` if it names an active archive root; otherwise the lowest-id
/// active archive root; otherwise `None` (no archive root — the caller warns and
/// skips the receipt). Returns `(root_id, root_path)`.
#[allow(dead_code)] // Wired into src/exclude.rs in Phase 3.
pub fn resolve_ledger_root(roots: &[Root], config: &LedgerConfig) -> Option<(i64, String)> {
    if let Some(configured) = config.root {
        if let Some(r) = roots
            .iter()
            .find(|r| r.id == configured && r.is_active() && r.is_archive())
        {
            return Some((r.id, r.path.clone()));
        }
        // Configured root is invalid (missing, suspended, or not an archive) —
        // fall through to the default rather than failing.
    }
    roots
        .iter()
        .filter(|r| r.is_active() && r.is_archive())
        .min_by_key(|r| r.id)
        .map(|r| (r.id, r.path.clone()))
}

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

/// Write a receipt to disk as an `.incomplete` file.
///
/// Prepends a comment header (`# Canon Decision Receipt` + summary line),
/// serializes to TOML, then writes via `write_file_incomplete`. The
/// `.incomplete` file survives crashes as recoverable evidence.
///
/// Returns `Err` on failure — the caller decides whether to warn or propagate.
/// Receipt writing failure never halts a command.
pub fn write_receipt<T: Serialize>(path: &Path, receipt: &T, comment_summary: &str) -> Result<()> {
    let toml_body = toml::to_string_pretty(receipt)
        .with_context(|| format!("Failed to serialize receipt to TOML: {}", path.display()))?;

    let content = format!("# Canon Decision Receipt\n# {comment_summary}\n\n{toml_body}");

    write_file_incomplete(path, content.as_bytes())
        .with_context(|| format!("Failed to write receipt: {}", path.display()))
}

/// Finalize a receipt: rename `.incomplete` → final path (`.toml`).
///
/// Wraps `ops::fs::finalize_file`. Returns `Err` if the `.incomplete` file
/// does not exist — the caller collects a warning.
pub fn finalize_receipt(path: &Path) -> Result<()> {
    finalize_file(path).with_context(|| format!("Failed to finalize receipt: {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    // =========================================================================
    // receipt_filename
    // =========================================================================

    #[test]
    fn test_receipt_filename_padded() {
        assert_eq!(receipt_filename(43, "apply"), "000043-apply.toml");
    }

    #[test]
    fn test_receipt_filename_at_limit() {
        assert_eq!(receipt_filename(999999, "apply"), "999999-apply.toml");
    }

    #[test]
    fn test_receipt_filename_over_limit_no_truncation() {
        assert_eq!(receipt_filename(1000000, "apply"), "1000000-apply.toml");
    }

    #[test]
    fn test_receipt_filename_command_variety() {
        assert_eq!(
            receipt_filename(1, "exclude-set"),
            "000001-exclude-set.toml"
        );
    }

    // =========================================================================
    // compute_targeted_receipt_rel_path
    // =========================================================================

    #[test]
    fn test_path_central_with_base_dir() {
        let path = compute_targeted_receipt_rel_path(
            43,
            "apply",
            "Media/2016/Italy",
            &ReceiptLayout::Central,
        );
        assert_eq!(path, ".canon-ledger/Media/2016/Italy/000043-apply.toml");
    }

    #[test]
    fn test_path_alongside_with_base_dir() {
        let path = compute_targeted_receipt_rel_path(
            43,
            "apply",
            "Media/2016/Italy",
            &ReceiptLayout::Alongside,
        );
        assert_eq!(path, "Media/2016/Italy/.canon-ledger/000043-apply.toml");
    }

    #[test]
    fn test_path_central_empty_base_dir() {
        let path = compute_targeted_receipt_rel_path(43, "apply", "", &ReceiptLayout::Central);
        assert_eq!(path, ".canon-ledger/000043-apply.toml");
    }

    #[test]
    fn test_path_alongside_empty_base_dir() {
        let path = compute_targeted_receipt_rel_path(43, "apply", "", &ReceiptLayout::Alongside);
        assert_eq!(path, ".canon-ledger/000043-apply.toml");
    }

    // =========================================================================
    // ApplyReceipt serialization
    // =========================================================================

    fn make_apply_receipt() -> ApplyReceipt {
        ApplyReceipt {
            meta: ReceiptMeta {
                receipt_version: 1,
                decision_id: 43,
                command: "apply".to_string(),
                timestamp: 1744300800,
                scope: Some(vec!["/Volumes/old-laptop/Photos".to_string()]),
                reason: Some("Italy 2016".to_string()),
                summary: "Applied 2 files".to_string(),
                canon_version: "0.4.1".to_string(),
                command_line: "canon apply manifest.toml".to_string(),
                manifest: Some("/Volumes/Archive/manifest.toml".to_string()),
            },
            items: vec![ApplyReceiptItem {
                source_root: "/Volumes/old-laptop".to_string(),
                source_rel_path: "Photos/italy/IMG_001.jpg".to_string(),
                destination_rel_path: "Media/2016/Italy/IMG_001.jpg".to_string(),
                hash: Some("sha256:abc123".to_string()),
                size: 3456789,
                mtime: 1700000000,
                previous_decision_id: Some(12),
            }],
        }
    }

    #[test]
    fn test_serialize_meta_fields_present() {
        let receipt = make_apply_receipt();
        let toml_str = toml::to_string_pretty(&receipt).unwrap();
        assert!(toml_str.contains("[meta]"), "missing [meta]\n{toml_str}");
        assert!(toml_str.contains("receipt_version = 1"));
        assert!(toml_str.contains("decision_id = 43"));
        assert!(toml_str.contains("command = \"apply\""));
        assert!(toml_str.contains("manifest = \"/Volumes/Archive/manifest.toml\""));
    }

    #[test]
    fn test_serialize_items_present() {
        let receipt = make_apply_receipt();
        let toml_str = toml::to_string_pretty(&receipt).unwrap();
        assert!(
            toml_str.contains("[[items]]"),
            "missing [[items]]\n{toml_str}"
        );
        assert!(toml_str.contains("source_root = \"/Volumes/old-laptop\""));
        assert!(toml_str.contains("destination_rel_path = \"Media/2016/Italy/IMG_001.jpg\""));
        assert!(toml_str.contains("previous_decision_id = 12"));
    }

    #[test]
    fn test_serialize_optional_fields_omitted_when_none() {
        let receipt = ApplyReceipt {
            meta: ReceiptMeta {
                receipt_version: 1,
                decision_id: 1,
                command: "apply".to_string(),
                timestamp: 0,
                scope: None,
                reason: None,
                summary: "done".to_string(),
                canon_version: "0.4.1".to_string(),
                command_line: "canon apply m.lock".to_string(),
                manifest: None,
            },
            items: vec![],
        };
        let toml_str = toml::to_string_pretty(&receipt).unwrap();
        assert!(
            !toml_str.contains("scope"),
            "scope should be absent\n{toml_str}"
        );
        assert!(
            !toml_str.contains("reason"),
            "reason should be absent\n{toml_str}"
        );
        assert!(
            !toml_str.contains("manifest"),
            "manifest should be absent\n{toml_str}"
        );
    }

    #[test]
    fn test_serialize_previous_decision_id_omitted_when_none() {
        let receipt = ApplyReceipt {
            meta: ReceiptMeta {
                receipt_version: 1,
                decision_id: 1,
                command: "apply".to_string(),
                timestamp: 0,
                scope: None,
                reason: None,
                summary: "done".to_string(),
                canon_version: "0.4.1".to_string(),
                command_line: "canon apply m.lock".to_string(),
                manifest: None,
            },
            items: vec![ApplyReceiptItem {
                source_root: "/src".to_string(),
                source_rel_path: "file.jpg".to_string(),
                destination_rel_path: "Media/file.jpg".to_string(),
                hash: None,
                size: 100,
                mtime: 0,
                previous_decision_id: None,
            }],
        };
        let toml_str = toml::to_string_pretty(&receipt).unwrap();
        assert!(
            !toml_str.contains("previous_decision_id"),
            "previous_decision_id should be absent\n{toml_str}"
        );
        assert!(
            !toml_str.contains("hash"),
            "hash should be absent\n{toml_str}"
        );
    }

    // =========================================================================
    // write_receipt + finalize_receipt
    // =========================================================================

    #[test]
    fn test_write_receipt_creates_incomplete_with_header() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("000043-apply.toml");
        let receipt = make_apply_receipt();

        write_receipt(&path, &receipt, "Applied 2 files").unwrap();

        let incomplete = dir.path().join("000043-apply.incomplete");
        assert!(incomplete.exists(), ".incomplete file should exist");
        assert!(!path.exists(), ".toml should not exist yet");

        let content = std::fs::read_to_string(&incomplete).unwrap();
        assert!(
            content.starts_with("# Canon Decision Receipt\n# Applied 2 files\n"),
            "wrong header:\n{content}"
        );
        assert!(content.contains("[meta]"));
        assert!(content.contains("[[items]]"));
    }

    #[test]
    fn test_finalize_receipt_renames() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("000043-apply.toml");
        let receipt = make_apply_receipt();

        write_receipt(&path, &receipt, "Applied 2 files").unwrap();
        finalize_receipt(&path).unwrap();

        assert!(path.exists(), ".toml should exist after finalize");
        assert!(
            !dir.path().join("000043-apply.incomplete").exists(),
            ".incomplete should be gone"
        );

        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("[meta]"));
    }

    #[test]
    fn test_write_receipt_invalid_path_returns_err() {
        let path = std::path::Path::new("/dev/null/receipt.toml");
        let receipt = make_apply_receipt();
        assert!(write_receipt(path, &receipt, "test").is_err());
    }

    // =========================================================================
    // Non-targeted (ledger-root) placement
    // =========================================================================

    fn mk_root(id: i64, role: &str, suspended: bool) -> Root {
        Root {
            id,
            path: format!("/root{id}"),
            role: role.to_string(),
            comment: None,
            last_scanned_at: None,
            suspended,
        }
    }

    #[test]
    fn test_ledger_root_rel_path_flat() {
        assert_eq!(
            compute_ledger_root_receipt_rel_path(42, "exclude_set"),
            ".canon-ledger/000042-exclude_set.toml"
        );
    }

    #[test]
    fn test_resolve_ledger_root_none_when_no_archive() {
        let roots = vec![mk_root(1, "source", false)];
        assert_eq!(resolve_ledger_root(&roots, &LedgerConfig::default()), None);
    }

    #[test]
    fn test_resolve_ledger_root_lowest_id_archive_default() {
        let roots = vec![
            mk_root(1, "source", false),
            mk_root(3, "archive", false),
            mk_root(2, "archive", false),
        ];
        assert_eq!(
            resolve_ledger_root(&roots, &LedgerConfig::default()),
            Some((2, "/root2".to_string()))
        );
    }

    #[test]
    fn test_resolve_ledger_root_configured_valid() {
        let roots = vec![mk_root(1, "archive", false), mk_root(5, "archive", false)];
        let cfg = LedgerConfig {
            root: Some(5),
            ..LedgerConfig::default()
        };
        assert_eq!(
            resolve_ledger_root(&roots, &cfg),
            Some((5, "/root5".to_string()))
        );
    }

    #[test]
    fn test_resolve_ledger_root_configured_missing_falls_back() {
        let roots = vec![mk_root(1, "archive", false), mk_root(2, "archive", false)];
        let cfg = LedgerConfig {
            root: Some(9),
            ..LedgerConfig::default()
        };
        assert_eq!(
            resolve_ledger_root(&roots, &cfg),
            Some((1, "/root1".to_string()))
        );
    }

    #[test]
    fn test_resolve_ledger_root_configured_source_falls_back() {
        let roots = vec![mk_root(1, "archive", false), mk_root(2, "source", false)];
        let cfg = LedgerConfig {
            root: Some(2),
            ..LedgerConfig::default()
        };
        assert_eq!(
            resolve_ledger_root(&roots, &cfg),
            Some((1, "/root1".to_string()))
        );
    }

    #[test]
    fn test_resolve_ledger_root_skips_suspended() {
        let roots = vec![mk_root(1, "archive", true), mk_root(2, "archive", false)];
        assert_eq!(
            resolve_ledger_root(&roots, &LedgerConfig::default()),
            Some((2, "/root2".to_string()))
        );
    }
}
