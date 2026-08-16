//! The manifest format — the on-disk contract between `cluster generate`
//! and `apply`.
//!
//! A manifest is a TOML file the user edits: what was selected, where it
//! should go, and free-form notes. Beside it sits a JSONL lock file, one
//! entry per source, snapshotting file state at generation time so apply
//! can tell whether the world moved underneath it. Both formats are
//! versioned and read back from files users already hold, so everything
//! here is pure: the types, the version gate, the `[options]` vocabulary,
//! and the notes extraction that keeps a user's own writing across a
//! refresh. Reading and writing the files themselves is the operations
//! layer's job.

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

use crate::domain::source::Source;

// ============================================================================
// Manifest data contract (shared between cluster generate and apply)
// ============================================================================

/// TOML manifest config file structure.
#[derive(Serialize, Deserialize)]
pub struct ManifestConfig {
    pub meta: ManifestMeta,
    #[serde(default)]
    pub options: ManifestOptions,
    pub output: ManifestOutput,
}

#[derive(Serialize, Deserialize, Default)]
pub struct ManifestOptions {
    #[serde(default)]
    pub allow: Vec<String>,
}

#[derive(Serialize, Deserialize)]
pub struct ManifestMeta {
    #[serde(default = "default_version")]
    pub version: u32,
    pub query: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    /// RFC3339 timestamp when manifest was generated/refreshed
    pub generated_at: String,
    /// SHA256 hash of the lock file (for integrity validation)
    pub lock_hash: String,
}

fn default_version() -> u32 {
    1
}

const SUPPORTED_MANIFEST_VERSION: u32 = 1;

pub fn validate_manifest_version(version: u32) -> Result<()> {
    if version > SUPPORTED_MANIFEST_VERSION {
        bail!("Manifest version {version} is not supported by this version of Canon. Please update Canon.");
    }
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct ManifestOutput {
    pub pattern: String,
    pub archive_root_id: i64,
    pub base_dir: String,
}

// ============================================================================
// Lock file
// ============================================================================

/// JSONL lock entry (one per line in .lock file)
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LockEntry {
    pub id: i64,
    pub root_id: i64,
    pub path: String,
    // Device and inode are recorded for move detection, not for staleness validation.
    // Staleness is determined by size+mtime+partial_hash only.
    pub device: i64,
    pub inode: i64,
    // File state for pre-transfer staleness validation
    pub size: i64,
    pub mtime: i64,
    pub partial_hash: String, // SHA256 of first 8KB + last 8KB (for integrity validation)
    // Content info
    pub object_id: Option<i64>,
    pub hash_type: Option<String>,
    pub hash_value: Option<String>,
    // Note: `facts` field was removed. Apply looks up facts at runtime from DB.
    // Old lock files with `facts` field are still readable (serde ignores unknown fields).
}

impl LockEntry {
    /// Build a LockEntry from a Source and object hash info.
    pub fn from_source(
        source: &Source,
        hash_type: Option<String>,
        hash_value: Option<String>,
    ) -> Self {
        Self {
            id: source.id,
            root_id: source.root_id,
            path: source.path(),
            device: source.device,
            inode: source.inode,
            size: source.size,
            mtime: source.mtime,
            partial_hash: source.partial_hash.clone(),
            object_id: source.object_id,
            hash_type,
            hash_value,
        }
    }
}

// ============================================================================
// The `[options]` vocabulary
// ============================================================================

/// Parse `allow` values from a manifest's `[options]` section.
pub fn parse_manifest_allow(allow: &[String]) -> Result<(bool, bool)> {
    let mut archived = false;
    let mut duplicates = false;
    for v in allow {
        match v.as_str() {
            "archived" => archived = true,
            "duplicates" => duplicates = true,
            other => bail!(
                "Invalid allow value '{other}' in manifest [options]. Valid: archived, duplicates"
            ),
        }
    }
    Ok((archived, duplicates))
}

// ============================================================================
// Notes extraction
// ============================================================================

/// Extract notes section from an existing manifest.
/// Extract the raw notes section from a manifest (preserves `#` comment markers).
/// Used by manifest refresh to re-emit the notes block verbatim.
pub fn extract_notes_raw(content: &str) -> Option<String> {
    let marker = "# === Notes ===";
    let start_idx = content.find(marker)?;
    let after_marker = start_idx + marker.len();
    let rest = &content[after_marker..];

    // Walk whole lines with their terminators still attached, so the running
    // offset is exact whatever the file's line endings are. A manifest is
    // meant to be opened in an editor, and an editor may hand it back with
    // CRLF endings; assuming a one-byte terminator would shift the offset by
    // a byte per line and cut the notes short — or land inside a character.
    let mut end = rest.len();
    let mut offset = 0;
    for (i, line) in rest.split_inclusive('\n').enumerate() {
        if i > 0 && (line.starts_with("# === ") || line.starts_with('[')) {
            end = offset;
            break;
        }
        offset += line.len();
    }

    Some(rest[..end].to_string())
}

/// Extract notes from a manifest as clean text (strips `#` comment markers).
/// Used for decision reason when manifest notes flow into apply records.
pub fn extract_notes(content: &str) -> Option<String> {
    let raw = extract_notes_raw(content)?;
    let stripped: String = raw
        .lines()
        .map(|line| {
            line.strip_prefix("# ")
                .or_else(|| line.strip_prefix("#"))
                .unwrap_or(line)
        })
        .collect::<Vec<_>>()
        .join("\n");
    let trimmed = stripped.trim().to_string();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // parse_manifest_allow
    // =========================================================================

    #[test]
    fn test_manifest_options_invalid_allow() {
        let result = parse_manifest_allow(&["bogus".to_string()]);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("bogus"));
        assert!(err.contains("archived"));
    }

    #[test]
    fn test_parse_manifest_allow_valid() {
        let (archived, duplicates) =
            parse_manifest_allow(&["archived".to_string(), "duplicates".to_string()]).unwrap();
        assert!(archived);
        assert!(duplicates);
    }

    #[test]
    fn test_parse_manifest_allow_empty() {
        let (archived, duplicates) = parse_manifest_allow(&[]).unwrap();
        assert!(!archived);
        assert!(!duplicates);
    }

    // =========================================================================
    // Version validation
    // =========================================================================

    #[test]
    fn test_version_1_accepted() {
        assert!(validate_manifest_version(1).is_ok());
    }

    #[test]
    fn test_version_future_rejected() {
        let result = validate_manifest_version(99);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("99"));
        assert!(err.contains("not supported"));
    }

    // =========================================================================
    // Manifest serde
    // =========================================================================

    #[test]
    fn test_manifest_options_round_trip() {
        let config = ManifestConfig {
            meta: ManifestMeta {
                version: 1,
                query: vec!["source.ext=jpg".to_string()],
                scope: Some("/photos".to_string()),
                generated_at: "2026-02-15T12:00:00Z".to_string(),
                lock_hash: "abc123".to_string(),
            },
            options: ManifestOptions {
                allow: vec!["archived".to_string(), "duplicates".to_string()],
            },
            output: ManifestOutput {
                pattern: "{filename}".to_string(),
                archive_root_id: 1,
                base_dir: "photos".to_string(),
            },
        };

        let toml_str = toml::to_string_pretty(&config).unwrap();
        let parsed: ManifestConfig = toml::from_str(&toml_str).unwrap();

        assert_eq!(parsed.options.allow, vec!["archived", "duplicates"]);
        assert_eq!(parsed.meta.query, vec!["source.ext=jpg"]);
        assert_eq!(parsed.output.pattern, "{filename}");
    }

    #[test]
    fn test_manifest_options_backward_compat() {
        let toml_str = r#"
[meta]
query = ["source.ext=jpg"]
scope = "/photos"
generated_at = "2026-02-15T12:00:00Z"
lock_hash = "abc123"

[output]
pattern = "{filename}"
archive_root_id = 1
base_dir = "photos"
"#;
        let config: ManifestConfig = toml::from_str(toml_str).unwrap();
        assert!(config.options.allow.is_empty());
    }

    #[test]
    fn test_manifest_without_version_defaults_to_1() {
        let toml_str = r#"
[meta]
query = ["source.ext=jpg"]
scope = "/photos"
generated_at = "2026-02-15T12:00:00Z"
lock_hash = "abc123"

[output]
pattern = "{filename}"
archive_root_id = 1
base_dir = "photos"
"#;
        let config: ManifestConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.meta.version, 1);
    }

    #[test]
    fn test_manifest_with_version_round_trip() {
        let config = ManifestConfig {
            meta: ManifestMeta {
                version: 1,
                query: vec!["source.ext=jpg".to_string()],
                scope: Some("/photos".to_string()),
                generated_at: "2026-02-15T12:00:00Z".to_string(),
                lock_hash: "abc123".to_string(),
            },
            options: ManifestOptions { allow: vec![] },
            output: ManifestOutput {
                pattern: "{filename}".to_string(),
                archive_root_id: 1,
                base_dir: "photos".to_string(),
            },
        };

        let toml_str = toml::to_string_pretty(&config).unwrap();
        let parsed: ManifestConfig = toml::from_str(&toml_str).unwrap();
        assert_eq!(parsed.meta.version, 1);
    }

    // =========================================================================
    // extract_notes
    // =========================================================================

    // extract_notes_raw — preserves # markers for manifest refresh

    #[test]
    fn test_extract_notes_raw_empty_placeholder() {
        let content = "# === Notes ===\n#\n\n[meta]\nversion = 1\n";
        let notes = extract_notes_raw(content).unwrap();
        assert_eq!(notes, "\n#\n\n");
    }

    #[test]
    fn test_extract_notes_raw_with_content() {
        let content =
            "# === Notes ===\n# This cluster has family photos\n# from 2020-2023\n\n[meta]\n";
        let notes = extract_notes_raw(content).unwrap();
        assert_eq!(
            notes,
            "\n# This cluster has family photos\n# from 2020-2023\n\n"
        );
    }

    #[test]
    fn test_extract_notes_raw_before_meta() {
        let content = "# === Notes ===\n# Some note\n[meta]\nversion = 1\n";
        let notes = extract_notes_raw(content).unwrap();
        assert_eq!(notes, "\n# Some note\n");
    }

    #[test]
    fn test_extract_notes_raw_before_next_section() {
        let content = "# === Notes ===\n# My notes\n# === Cluster Summary ===\n# stuff\n";
        let notes = extract_notes_raw(content).unwrap();
        assert_eq!(notes, "\n# My notes\n");
    }

    #[test]
    fn test_extract_notes_raw_handles_crlf() {
        // An editor may return the manifest with two-byte line endings. Every
        // note line must survive, terminators included.
        let content =
            "# === Notes ===\r\n# First note\r\n# Second note\r\n\r\n[meta]\r\nversion = 1\r\n";
        let notes = extract_notes_raw(content).unwrap();
        assert_eq!(notes, "\r\n# First note\r\n# Second note\r\n\r\n");
    }

    #[test]
    fn test_extract_notes_raw_crlf_with_non_ascii() {
        let content =
            "# === Notes ===\r\n# Fotos de la boda — París\r\n# Añadir más tarde\r\n[meta]\r\n";
        let notes = extract_notes_raw(content).unwrap();
        assert_eq!(
            notes,
            "\r\n# Fotos de la boda — París\r\n# Añadir más tarde\r\n"
        );
    }

    #[test]
    fn test_extract_notes_strips_markers_on_crlf() {
        // The last note is short on purpose: a parser that mismeasures a
        // two-byte terminator loses ground with every line, and a trailing
        // blank line would hide that behind the final trim.
        let content = "# === Notes ===\r\n# alpha\r\n# beta\r\n# g\r\n[meta]\r\n";
        let notes = extract_notes(content).unwrap();
        assert_eq!(notes, "alpha\nbeta\ng");
    }

    // extract_notes — strips # markers for decision reason

    #[test]
    fn test_extract_notes_empty_placeholder() {
        let content = "# === Notes ===\n#\n\n[meta]\nversion = 1\n";
        // Empty placeholder: after stripping # prefix, content is whitespace-only → None
        assert!(extract_notes(content).is_none());
    }

    #[test]
    fn test_extract_notes_with_content() {
        let content =
            "# === Notes ===\n# This cluster has family photos\n# from 2020-2023\n\n[meta]\n";
        let notes = extract_notes(content).unwrap();
        assert_eq!(notes, "This cluster has family photos\nfrom 2020-2023");
    }

    #[test]
    fn test_extract_notes_missing() {
        let content = "[meta]\nversion = 1\nquery = []\n";
        assert!(extract_notes(content).is_none());
    }

    #[test]
    fn test_extract_notes_strips_comment_markers() {
        let content = "# === Notes ===\n# Some note\n[meta]\nversion = 1\n";
        let notes = extract_notes(content).unwrap();
        assert_eq!(notes, "Some note");
    }

    #[test]
    fn test_extract_notes_stops_at_next_section() {
        let content = "# === Notes ===\n# My notes\n# === Cluster Summary ===\n# stuff\n";
        let notes = extract_notes(content).unwrap();
        assert_eq!(notes, "My notes");
    }
}
