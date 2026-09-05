//! Receipt writing for Canon decisions.
//!
//! Receipts are TOML files written alongside archive content, capturing
//! the per-item detail of each decision. The decision record gives you
//! the *why*; the receipt gives you the *what specifically*.

use std::path::Path;

use anyhow::{Context, Result};
use serde::Serialize;

use super::fs::{finalize_file, write_file_incomplete};
use crate::core::domain::config::{LedgerConfig, ReceiptLayout};
use crate::core::domain::extraction::OriginDisposition;
use crate::core::domain::fate::{DecisionFamily, FateAspect};
use crate::core::domain::root::Root;

/// Reference to a receipt file on disk, stored in the decision record.
pub struct ReceiptRef {
    pub root_id: i64,
    pub rel_path: String,
}

/// Where a receipt file should be written.
///
/// **Placement principle:** a receipt lives at the *locus of the action's effect*.
/// An apply writes content to an archive, so its receipt goes to that destination
/// archive (`Targeted`). A deletion loses a file from the root it lived on, so its
/// receipt goes to that source root; an exclusion asserts a source↔archive coverage
/// relationship, so its receipt goes to the archive ledger root (both `LedgerRoot`,
/// differing only in which root the caller selects).
///
/// `Targeted` mirrors the destination path under the archive's `.canon-ledger/`, per
/// the `layout` setting. `LedgerRoot` lands flat in the given root's `.canon-ledger/`,
/// independent of layout — the receipt travels with that drive.
pub enum ReceiptPlacement {
    Targeted {
        archive_root_id: i64,
        archive_root_path: String,
        /// Relative base directory within the archive root (from manifest config).
        base_dir_rel: String,
    },
    /// Non-targeted receipts: flat at the ledger root's `.canon-ledger/`.
    LedgerRoot { root_id: i64, root_path: String },
}

impl ReceiptPlacement {
    /// The identity of the root this receipt is anchored to — placement made
    /// data. `Targeted` resolves to the destination archive root; `LedgerRoot`
    /// to the root the caller selected. Returns `(root_id, root_path)`.
    ///
    /// This is the receipt's *where*: the locus of the action's effect (the
    /// Receipt Placement Principle), read straight off the placement rather than
    /// from the decision's scope.
    pub fn locus_root(&self) -> (i64, &str) {
        match self {
            ReceiptPlacement::Targeted {
                archive_root_id,
                archive_root_path,
                ..
            } => (*archive_root_id, archive_root_path),
            ReceiptPlacement::LedgerRoot { root_id, root_path } => (*root_id, root_path),
        }
    }
}

/// What a receipt records — the single authority mapping a writer to the inputs
/// of the shared what-derivation. Each variant maps to a `(DecisionFamily,
/// FateAspect)` pair (fed to `fate_transition`/`fate_posture`) and, for apply,
/// to an origin disposition. No writer emits a transition/posture/origin literal:
/// they name their `ReceiptKind` and the recorder derives the words.
///
/// The variant→family mapping must agree with `decision_family(command)` for the
/// corresponding command; the integrity test enforces that agreement.
pub enum ReceiptKind {
    Apply(OriginDisposition),
    ExcludeSet,
    ExcludeDuplicates,
    ExcludeObject,
    Restore,
    RestoreObject,
    Deletion,
}

impl ReceiptKind {
    /// The `(family, aspect)` the shared derivation keys on. Wider than the
    /// command: a scan's deletion receipt is `(Observe, Absent)`, the discriminant
    /// the command identifier alone can't supply.
    pub fn family_aspect(&self) -> (DecisionFamily, FateAspect) {
        use DecisionFamily::*;
        use FateAspect::*;
        match self {
            ReceiptKind::Apply(_) => (Archive, Present),
            ReceiptKind::ExcludeSet
            | ReceiptKind::ExcludeDuplicates
            | ReceiptKind::ExcludeObject => (Exclude, Present),
            ReceiptKind::Restore | ReceiptKind::RestoreObject => (Restore, Present),
            ReceiptKind::Deletion => (Observe, Absent),
        }
    }

    /// The origin's disposition — apply only. `retained` (content now in two
    /// places) or `relocated` (the origin no longer holds it). Carried as data
    /// by the caller that performed the transfer, never re-parsed from a
    /// command line. `None` for every non-apply receipt.
    pub fn origin_disposition(&self) -> Option<&'static str> {
        match self {
            ReceiptKind::Apply(disposition) => Some(disposition.as_str()),
            _ => None,
        }
    }
}

/// The root a receipt is anchored to — placement made data. Serializes as the
/// nested `[meta.locus]` table.
///
/// `path` is the roots-table canonical path captured at write time —
/// authoritative for a human and for a rebuild-from-disk reader, and stable
/// evidence even after a root is re-pathed or the DB is reset. `id` is the join
/// key for a live DB. Both are always present.
#[derive(Serialize)]
pub struct ReceiptLocus {
    pub path: String,
    pub id: i64,
}

/// Shared meta section for all receipt types.
/// Serializes as the `[meta]` TOML table.
#[derive(Serialize)]
pub struct ReceiptMeta {
    pub receipt_version: u32,
    pub decision_id: i64,
    pub command: String,
    /// The what, in registered transition vocabulary (`archived` | `excluded` |
    /// `restored` | `deleted`), derived once — never a per-writer literal. A
    /// reader without Canon learns what happened without inferring it from body
    /// shape or a churning command name.
    pub transition: String,
    /// The posture accompanying the transition: `performed` (Canon acted) or
    /// `observed` (Canon witnessed a change the world made — a scan deletion).
    pub posture: String,
    /// Terminal status: completed | partial | interrupted. Lets a disk-only
    /// reader distinguish a complete receipt from an interrupted/partial one.
    pub status: String,
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
    /// Origin disposition — apply only: `retained` (Copy) or `relocated`
    /// (Move|Rename). Omitted for every other receipt type.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin_disposition: Option<String>,
    /// The root this receipt is anchored to (the `[meta.locus]` table). Kept
    /// last so the declaration reads in the order the file is written: a TOML
    /// table has to follow every scalar key of its parent, and the serializer
    /// reorders to satisfy that on its own — so a struct declared out of order
    /// still writes correctly, it just no longer describes its own output.
    pub locus: ReceiptLocus,
}

// ---------------------------------------------------------------------------
// Path computation (pure functions, no I/O)
// ---------------------------------------------------------------------------

/// Format the receipt filename: 6-digit zero-padded decision_id + command.
///
/// Examples: `000043-apply.toml`, `1000000-apply.toml` (no truncation beyond 6 digits)
fn receipt_filename(decision_id: i64, command: &str) -> String {
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

/// Where non-targeted receipts land — or why they land nowhere.
///
/// The two absences are different facts about the world and take different
/// answers, so they are two arms rather than one `None`: with no archive root
/// there is nothing to register, and the way forward is to register one; with
/// every archive root suspended the shelf and its ledger stand exactly where
/// they stood, behind a door the user closed, and the only way back is
/// `canon roots unsuspend`. A reader that could not tell them apart would name
/// a cause that is false and prescribe a remedy that does not work.
#[derive(Debug, PartialEq, Eq)]
pub enum LedgerRootOutcome {
    Found {
        root_id: i64,
        root_path: String,
    },
    /// No archive root is registered at all.
    NoArchiveRoot,
    /// Archive roots are registered and every one of them is suspended.
    /// Their paths, lowest root id first — the same order the resolver
    /// would have picked from.
    AllArchiveRootsSuspended {
        roots: Vec<String>,
    },
}

impl LedgerRootOutcome {
    /// The reason no receipt could be placed, as a **record value**: it is
    /// carried into the decision row's summary, so a row with empty receipt
    /// columns explains its own gap instead of leaving a reader to guess
    /// (the consumption-readiness ADR's self-explaining gaps). `None` when a
    /// root was found and a receipt is owed after all.
    pub fn unplaceable_reason(&self) -> Option<String> {
        match self {
            Self::Found { .. } => None,
            Self::NoArchiveRoot => {
                Some("receipt not written: no archive root is registered".to_string())
            }
            Self::AllArchiveRootsSuspended { roots } => Some(format!(
                "receipt not written: every archive root is suspended ({})",
                roots.join(", ")
            )),
        }
    }

    /// The way back from a closed door, as the command that opens it — named
    /// for the first parked root, which is the one the resolver would take.
    /// `canon roots unsuspend`, and only that: the sweep's footer rule, which
    /// settled this wording for a surface that meets the same door.
    pub fn unsuspend_hint(&self) -> Option<String> {
        match self {
            Self::Found { .. } | Self::NoArchiveRoot => None,
            Self::AllArchiveRootsSuspended { roots } => roots
                .first()
                .map(|p| format!("canon roots unsuspend path:{p}")),
        }
    }
}

/// Resolve which archive root holds non-targeted receipts.
///
/// Uses `config.root` if it names an active archive root; otherwise the lowest-id
/// active archive root. With neither available the outcome carries *which*
/// absence it is — the caller states that cause rather than assuming one.
/// Suspended archive roots are skipped deliberately: a receipt is not written
/// into a root the user has closed.
pub fn resolve_ledger_root(roots: &[Root], config: &LedgerConfig) -> LedgerRootOutcome {
    if let Some(configured) = config.root {
        if let Some(r) = roots
            .iter()
            .find(|r| r.id == configured && r.is_active() && r.is_archive())
        {
            return LedgerRootOutcome::Found {
                root_id: r.id,
                root_path: r.path.clone(),
            };
        }
        // Configured root is invalid (missing, suspended, or not an archive) —
        // fall through to the default rather than failing.
    }
    if let Some(r) = roots
        .iter()
        .filter(|r| r.is_active() && r.is_archive())
        .min_by_key(|r| r.id)
    {
        return LedgerRootOutcome::Found {
            root_id: r.id,
            root_path: r.path.clone(),
        };
    }
    let mut parked: Vec<&Root> = roots.iter().filter(|r| r.is_archive()).collect();
    if parked.is_empty() {
        return LedgerRootOutcome::NoArchiveRoot;
    }
    parked.sort_by_key(|r| r.id);
    LedgerRootOutcome::AllArchiveRootsSuspended {
        roots: parked.into_iter().map(|r| r.path.clone()).collect(),
    }
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
/// Wraps `core::ops::fs::finalize_file`. Returns `Err` if the `.incomplete` file
/// does not exist — the caller collects a warning.
pub fn finalize_receipt(path: &Path) -> Result<()> {
    finalize_file(path).with_context(|| format!("Failed to finalize receipt: {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    /// A minimal receipt body for the tests below.
    ///
    /// This module owns the shared `[meta]` table and the writer that puts a
    /// body on disk; it does not own any body. The writer is generic and never
    /// inspects what it serializes — that is precisely what lets each command
    /// define its own body — so the tests here bring a body of their own
    /// rather than borrowing one, and stay true whatever the real bodies grow
    /// into.
    #[derive(Serialize)]
    struct TestReceipt {
        meta: ReceiptMeta,
        items: Vec<TestReceiptItem>,
    }

    #[derive(Serialize)]
    struct TestReceiptItem {
        rel_path: String,
        size: i64,
    }

    /// A receipt whose meta carries every optional field, so a test can assert
    /// that a field is present without first having to populate it.
    fn make_receipt() -> TestReceipt {
        TestReceipt {
            meta: ReceiptMeta {
                receipt_version: 1,
                decision_id: 43,
                command: "apply".to_string(),
                transition: "archived".to_string(),
                posture: "performed".to_string(),
                status: "completed".to_string(),
                timestamp: 1744300800,
                scope: Some(vec!["/Volumes/old-laptop/Photos".to_string()]),
                reason: Some("Italy 2016".to_string()),
                summary: "Applied 2 files".to_string(),
                canon_version: "0.4.1".to_string(),
                command_line: "canon apply manifest.toml".to_string(),
                manifest: Some("/Volumes/Archive/manifest.toml".to_string()),
                origin_disposition: Some("retained".to_string()),
                locus: ReceiptLocus {
                    path: "/Volumes/Archive".to_string(),
                    id: 7,
                },
            },
            items: vec![TestReceiptItem {
                rel_path: "Media/2016/Italy/IMG_001.jpg".to_string(),
                size: 3456789,
            }],
        }
    }

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
    // ReceiptMeta serialization
    // =========================================================================

    #[test]
    fn test_serialize_meta_fields_present() {
        let receipt = make_receipt();
        let toml_str = toml::to_string_pretty(&receipt).unwrap();
        assert!(toml_str.contains("[meta]"), "missing [meta]\n{toml_str}");
        assert!(toml_str.contains("receipt_version = 1"));
        assert!(toml_str.contains("decision_id = 43"));
        assert!(toml_str.contains("command = \"apply\""));
        assert!(toml_str.contains("status = \"completed\""));
        assert!(toml_str.contains("manifest = \"/Volumes/Archive/manifest.toml\""));
    }

    #[test]
    fn test_meta_status_interrupted_serializes() {
        // A partial/interrupted receipt self-describes its status on disk.
        let mut receipt = make_receipt();
        receipt.meta.status = "interrupted".to_string();
        let toml_str = toml::to_string_pretty(&receipt).unwrap();
        assert!(
            toml_str.contains("status = \"interrupted\""),
            "status should serialize\n{toml_str}"
        );
    }

    #[test]
    fn test_serialize_optional_fields_omitted_when_none() {
        let mut receipt = make_receipt();
        receipt.meta.scope = None;
        receipt.meta.reason = None;
        receipt.meta.manifest = None;
        // The command line is asserted against by substring below, so it must
        // not itself mention a manifest.
        receipt.meta.command_line = "canon apply m.lock".to_string();
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

    // =========================================================================
    // write_receipt + finalize_receipt
    // =========================================================================

    #[test]
    fn test_write_receipt_creates_incomplete_with_header() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("000043-apply.toml");
        let receipt = make_receipt();

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
        let receipt = make_receipt();

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
        let receipt = make_receipt();
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

    fn found(id: i64, path: &str) -> LedgerRootOutcome {
        LedgerRootOutcome::Found {
            root_id: id,
            root_path: path.to_string(),
        }
    }

    #[test]
    fn test_resolve_ledger_root_none_when_no_archive() {
        let roots = vec![mk_root(1, "source", false)];
        assert_eq!(
            resolve_ledger_root(&roots, &LedgerConfig::default()),
            LedgerRootOutcome::NoArchiveRoot
        );
    }

    /// The two absences are two facts. An archive root that exists and is
    /// parked must never read as one that was never registered — the whole
    /// point of the arm.
    #[test]
    fn a_parked_archive_fleet_is_not_an_absent_one() {
        let roots = vec![
            mk_root(1, "source", false),
            mk_root(3, "archive", true),
            mk_root(2, "archive", true),
        ];
        assert_eq!(
            resolve_ledger_root(&roots, &LedgerConfig::default()),
            LedgerRootOutcome::AllArchiveRootsSuspended {
                roots: vec!["/root2".to_string(), "/root3".to_string()],
            }
        );
    }

    /// A configured ledger root that is itself parked falls through to the
    /// default, and with every archive root parked the fall-through lands on
    /// the parked arm — never on "no archive root".
    #[test]
    fn a_parked_configured_root_falls_through_to_the_parked_arm() {
        let roots = vec![mk_root(1, "archive", true), mk_root(2, "archive", true)];
        let cfg = LedgerConfig {
            root: Some(1),
            ..LedgerConfig::default()
        };
        assert_eq!(
            resolve_ledger_root(&roots, &cfg),
            LedgerRootOutcome::AllArchiveRootsSuspended {
                roots: vec!["/root1".to_string(), "/root2".to_string()],
            }
        );
    }

    /// The reason is a record value: it says which absence this is, and the
    /// parked one names the roots so the row is readable years later.
    #[test]
    fn each_absence_states_its_own_cause() {
        assert_eq!(
            LedgerRootOutcome::NoArchiveRoot.unplaceable_reason(),
            Some("receipt not written: no archive root is registered".to_string())
        );
        assert_eq!(
            LedgerRootOutcome::AllArchiveRootsSuspended {
                roots: vec!["/a".to_string(), "/b".to_string()],
            }
            .unplaceable_reason(),
            Some("receipt not written: every archive root is suspended (/a, /b)".to_string())
        );
        assert_eq!(found(1, "/root1").unplaceable_reason(), None);
    }

    /// The way back exists only for the door the user closed, and it is
    /// `canon roots unsuspend` and only that — never the destructive door.
    #[test]
    fn only_the_closed_door_offers_a_way_back() {
        assert_eq!(found(1, "/root1").unsuspend_hint(), None);
        assert_eq!(LedgerRootOutcome::NoArchiveRoot.unsuspend_hint(), None);
        assert_eq!(
            LedgerRootOutcome::AllArchiveRootsSuspended {
                roots: vec!["/a".to_string(), "/b".to_string()],
            }
            .unsuspend_hint(),
            Some("canon roots unsuspend path:/a".to_string())
        );
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
            found(2, "/root2")
        );
    }

    #[test]
    fn test_resolve_ledger_root_configured_valid() {
        let roots = vec![mk_root(1, "archive", false), mk_root(5, "archive", false)];
        let cfg = LedgerConfig {
            root: Some(5),
            ..LedgerConfig::default()
        };
        assert_eq!(resolve_ledger_root(&roots, &cfg), found(5, "/root5"));
    }

    #[test]
    fn test_resolve_ledger_root_configured_missing_falls_back() {
        let roots = vec![mk_root(1, "archive", false), mk_root(2, "archive", false)];
        let cfg = LedgerConfig {
            root: Some(9),
            ..LedgerConfig::default()
        };
        assert_eq!(resolve_ledger_root(&roots, &cfg), found(1, "/root1"));
    }

    #[test]
    fn test_resolve_ledger_root_configured_source_falls_back() {
        let roots = vec![mk_root(1, "archive", false), mk_root(2, "source", false)];
        let cfg = LedgerConfig {
            root: Some(2),
            ..LedgerConfig::default()
        };
        assert_eq!(resolve_ledger_root(&roots, &cfg), found(1, "/root1"));
    }

    #[test]
    fn test_resolve_ledger_root_skips_suspended() {
        let roots = vec![mk_root(1, "archive", true), mk_root(2, "archive", false)];
        assert_eq!(
            resolve_ledger_root(&roots, &LedgerConfig::default()),
            found(2, "/root2")
        );
    }

    // =========================================================================
    // The what: transition/posture derivation (the integrity law)
    // =========================================================================

    /// The integrity law: a receipt's derived what/posture equals
    /// the trail's, command for command. `ReceiptKind → (family, aspect) →
    /// fate_transition` is the *same* function the trail rollup labels through,
    /// so the two stories can never diverge. The match is exhaustive — a new
    /// `ReceiptKind` must declare its expected trail command and words here, or
    /// this fails to compile.
    #[test]
    fn receipt_kind_transition_and_posture_match_the_trail_derivation() {
        use crate::core::domain::fate::{decision_family, fate_posture, fate_transition};

        let all = [
            ReceiptKind::Apply(OriginDisposition::Retained),
            ReceiptKind::ExcludeSet,
            ReceiptKind::ExcludeDuplicates,
            ReceiptKind::ExcludeObject,
            ReceiptKind::Restore,
            ReceiptKind::RestoreObject,
            ReceiptKind::Deletion,
        ];
        for kind in &all {
            // (trail command identifier, expected transition, expected posture)
            let (command, transition, posture) = match kind {
                ReceiptKind::Apply(_) => ("apply", "archived", "performed"),
                ReceiptKind::ExcludeSet => ("exclude_set", "excluded", "performed"),
                ReceiptKind::ExcludeDuplicates => ("exclude_duplicates", "excluded", "performed"),
                ReceiptKind::ExcludeObject => ("exclude_set_object", "excluded", "performed"),
                ReceiptKind::Restore => ("exclude_clear", "restored", "performed"),
                ReceiptKind::RestoreObject => ("exclude_clear_object", "restored", "performed"),
                ReceiptKind::Deletion => ("scan", "deleted", "observed"),
            };
            let (family, aspect) = kind.family_aspect();
            // The receipt's family agrees with the trail's command→family map.
            assert_eq!(
                family,
                decision_family(command),
                "family disagrees with trail for {command}"
            );
            // The stamped what == the trail-derived transition word.
            assert_eq!(
                fate_transition(family, aspect).map(|t| t.as_str()),
                Some(transition),
                "transition disagrees for {command}"
            );
            // The stamped posture.
            assert_eq!(
                fate_posture(family, aspect).as_str(),
                posture,
                "posture disagrees for {command}"
            );
        }
    }

    /// Origin disposition is the carried value spoken in its registered word;
    /// only apply carries one at all. Which transfer produces which disposition
    /// is the caller's knowledge, asserted where that mapping lives.
    #[test]
    fn origin_disposition_speaks_the_carried_value() {
        assert_eq!(
            ReceiptKind::Apply(OriginDisposition::Retained).origin_disposition(),
            Some("retained")
        );
        assert_eq!(
            ReceiptKind::Apply(OriginDisposition::Relocated).origin_disposition(),
            Some("relocated")
        );
        assert_eq!(ReceiptKind::ExcludeSet.origin_disposition(), None);
        assert_eq!(ReceiptKind::Deletion.origin_disposition(), None);
    }

    // =========================================================================
    // The where: nested [meta.locus] presence + ordering
    // =========================================================================

    /// The meta states what/where, and the nested `[meta.locus]` table renders
    /// after all flat `[meta]` scalars and before the body — the layout a
    /// reader of a receipt on disk can count on.
    #[test]
    fn meta_serializes_what_where_and_orders_locus_last() {
        let out = toml::to_string_pretty(&make_receipt()).unwrap();
        assert!(out.contains("transition = \"archived\""));
        assert!(out.contains("posture = \"performed\""));
        assert!(out.contains("origin_disposition = \"retained\""));
        assert!(out.contains("[meta.locus]"));
        let locus = &out[out.find("[meta.locus]").unwrap()..];
        assert!(locus.contains("path = \"/Volumes/Archive\""));
        assert!(locus.contains("id = 7"));

        // Ordering: a flat scalar precedes the sub-table, which precedes the
        // items array. TOML requires it and the serializer arranges it, so this
        // pins the shape of what lands on disk rather than the field order.
        let command_at = out.find("command =").unwrap();
        let locus_at = out.find("[meta.locus]").unwrap();
        let items_at = out.find("[[items]]").unwrap();
        assert!(
            command_at < locus_at,
            "flat scalars precede the locus table"
        );
        assert!(locus_at < items_at, "the locus table precedes the items");
    }

    /// The serializer, not the struct, is what puts a nested table after its
    /// parent's scalar keys — so declaring a scalar below one still writes a
    /// valid file. `locus` is kept last for readability, and this is why that
    /// is a preference rather than a requirement.
    ///
    /// Worth a test of its own because nothing else in the corpus declares a
    /// scalar after a sub-table: an older serializer did reject it, the comments
    /// saying so outlived it by several versions, and without this the same
    /// sentence could quietly go stale again.
    #[test]
    fn a_scalar_declared_after_a_nested_table_still_serializes() {
        #[derive(Serialize)]
        struct Nested {
            inner: Inner,
            after: i64,
        }
        #[derive(Serialize)]
        struct Inner {
            value: i64,
        }

        let out = toml::to_string_pretty(&Nested {
            inner: Inner { value: 1 },
            after: 2,
        })
        .expect("a trailing scalar must not break serialization");

        assert!(
            out.find("after =").unwrap() < out.find("[inner]").unwrap(),
            "the scalar is emitted above the table it was declared below\n{out}"
        );
        toml::from_str::<toml::Table>(&out).expect("and the result must be valid TOML");
    }

    /// A receipt that carries no origin disposition — every kind but apply —
    /// omits the key entirely and still carries its locus.
    #[test]
    fn non_apply_receipt_omits_origin_disposition_but_carries_locus() {
        let mut receipt = make_receipt();
        receipt.meta.command = "exclude_set".to_string();
        receipt.meta.transition = "excluded".to_string();
        receipt.meta.manifest = None;
        receipt.meta.origin_disposition = None;
        let out = toml::to_string_pretty(&receipt).unwrap();
        assert!(!out.contains("origin_disposition"));
        assert!(out.contains("transition = \"excluded\""));
        assert!(out.contains("[meta.locus]"));
    }
}
