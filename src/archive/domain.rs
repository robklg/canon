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

use std::fmt;
use std::path::Path;

use anyhow::{bail, Context, Result};
use serde::{de, Deserialize, Deserializer, Serialize};

use crate::core::domain::source::Source;

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
    /// The paths the generation was scoped to, every one of them. Held as a
    /// list because that is what it is: joining several into one field is
    /// what made "the scope" mean three different things in three readers.
    #[serde(
        default,
        deserialize_with = "de_scope",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub scope: Vec<String>,
    /// RFC3339 timestamp when manifest was generated/refreshed
    pub generated_at: String,
    /// SHA256 hash of the lock file (for integrity validation)
    pub lock_hash: String,
}

/// Read `meta.scope`.
///
/// A list, which is the only form Canon writes or accepts. A **version 1**
/// manifest recorded its scopes joined into a single string with `", "`; that
/// form is **refused, never reconstructed**. Splitting it back apart cannot be
/// more than a guess — a directory name may itself contain the separator, and
/// nothing in the string says which reading was meant — and the guess would
/// decide where files are moved.
///
/// The way back is named in the refusal itself and nowhere else: a remedy
/// spelled in two places goes stale in one of them.
///
/// The refusal is spelled here rather than left to serde because this is where
/// the old form is recognisable. A bare type error would tell the reader that a
/// string is not a sequence, which is true and useless.
fn de_scope<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<String>, D::Error> {
    struct ScopeVisitor;

    impl<'de> de::Visitor<'de> for ScopeVisitor {
        type Value = Vec<String>;

        // Every wrong type that is not a string is answered by serde's own
        // "invalid type: …, expected …" using this line, which is why the
        // visitor is worth its length: an untagged enum answers all of them
        // with one message naming a private type and no offending value.
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a list of scope paths")
        }

        fn visit_str<E: de::Error>(self, _: &str) -> Result<Self::Value, E> {
            Err(E::custom(
                "meta.scope must be a list of paths. A string is the version 1 format, \
                 which joined several scopes into one field; splitting them apart again \
                 would be a guess about where files land, so it is refused rather than \
                 read. Fix it with `canon cluster refresh --edit <manifest>`, which opens \
                 the file before reading it, or write scope as a list.",
            ))
        }

        fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let mut paths = Vec::new();
            while let Some(path) = seq.next_element::<String>()? {
                paths.push(path);
            }
            Ok(paths)
        }
    }

    deserializer.deserialize_any(ScopeVisitor)
}

fn default_version() -> u32 {
    1
}

/// The version a freshly written manifest carries.
///
/// 2 since `meta.scope` became a list. The bump does not make an *already
/// shipped* canon binary emit the friendly refusal on a v2 manifest — those
/// parse before they gate, so they fail on the type first — but it stops
/// `version = 1` from meaning two different formats, which is what the next
/// format change would otherwise inherit. From this version on the ordering is
/// the other way round: see [`parse_manifest_config`].
pub const CURRENT_MANIFEST_VERSION: u32 = 2;

const SUPPORTED_MANIFEST_VERSION: u32 = CURRENT_MANIFEST_VERSION;

pub fn validate_manifest_version(version: u32) -> Result<()> {
    if version > SUPPORTED_MANIFEST_VERSION {
        bail!("Manifest version {version} is not supported by this version of Canon. Please update Canon.");
    }
    Ok(())
}

/// Parse a manifest body: **the version gate first, the body's shape second.**
///
/// The order is the whole of it. A manifest written by a later Canon is
/// exactly the file whose shape this binary may not know, so deserializing
/// first spends the error on a serde type mismatch and the friendly "update
/// Canon" refusal becomes unreachable in the one case it exists for. The
/// number is therefore read off a `toml::Value`, which needs no knowledge of
/// the body at all, and gated before `ManifestConfig` is ever asked for.
///
/// Every production read of a manifest passes through here, which is what
/// keeps the ordering a fact about the tree rather than a habit three callers
/// happen to share: `ops/manifest.rs` adds the file read for `cluster status`,
/// `cli/apply.rs` adds its own read because it needs the text a second time
/// for the notes, and `cli/cluster.rs` adds the `[options] allow` vocabulary
/// for generate and refresh. None of them deserializes a `ManifestConfig`
/// itself.
pub fn parse_manifest_config(content: &str, config_path: &Path) -> Result<ManifestConfig> {
    if let Some(version) = probe_manifest_version(content) {
        validate_manifest_version(version)?;
    }
    toml::from_str(content)
        .with_context(|| format!("Failed to parse manifest config: {}", config_path.display()))
}

/// Read `meta.version` while knowing nothing else about the body.
///
/// `None` means the question could not be asked — not TOML at all, no
/// `meta.version`, or a number no version could be — never that the manifest
/// is current. Those all fall through to the full parse, so serde still speaks
/// for every malformed file that is not a version problem, and an absent
/// version is left to `default_version`: an old manifest is not a future one.
///
/// A value past `u32` saturates rather than falling through. It is a future
/// version under any reading, and the refusal is the useful answer.
fn probe_manifest_version(content: &str) -> Option<u32> {
    let value: toml::Value = toml::from_str(content).ok()?;
    let version = value.get("meta")?.get("version")?.as_integer()?;
    if version < 0 {
        return None;
    }
    Some(u32::try_from(version).unwrap_or(u32::MAX))
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

/// The version a freshly written lock file carries.
///
/// 2 since the lock gained a header and a per-entry measurement. There is no
/// version 1 constant, because a version 1 lock has no header to read one
/// out of: its absence *is* the detection.
pub const CURRENT_LOCK_VERSION: u32 = 2;

/// The lock file's first line: what the run settled, as opposed to what it
/// selected.
///
/// A manifest's `meta.scope` is editable text and stays that way; this is the
/// resolved answer taken at the moment the selection was made, so `apply`
/// records paths that exist without re-reading anything a user could have
/// changed since. The existing `lock_hash` covers the whole file, so the
/// header is tamper-evident at no extra cost.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LockHeader {
    /// Required, and required is the point: an older lock's first line has no
    /// such field, so it fails to parse as a header and is recognised as old.
    ///
    /// **A discriminator, not a gate** — deliberately, and unlike
    /// [`validate_manifest_version`] beside it. Nothing reads the number back,
    /// so a lock from a later Canon is read as this one. A lock is ephemeral —
    /// a temporary artifact rebuilt by `cluster refresh` — so no compatibility
    /// path is owed in either direction. Whoever adds a forward gate is
    /// reversing that, not filling an omission.
    pub lock_version: u32,
    /// The resolved scope, for `apply`'s decision record. Confirmed prefixes
    /// only — a line the index could not confirm never reaches here, so the
    /// record cannot claim a place Canon has no sources for.
    pub scope: Vec<LockScope>,
}

/// One resolved scope, as the lock records it.
///
/// Deliberately **not** a serialized [`DecisionScope`]. That type's whole
/// point is that constructing one requires a matching root, so a rootless
/// string is unrepresentable; deserializing into it would put arbitrary file
/// text through the back door and weaken a guarantee bought at some cost.
/// Apply converts through `DecisionScope::new`, leaving the domain type with
/// one constructor.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LockScope {
    pub root_id: i64,
    pub root_path: String,
    pub rel_prefix: String,
}

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
    /// Where this file goes, measured when the selection was settled.
    ///
    /// The opposite case to `facts`, and the reason the note above is worth
    /// reading beside this field: an attribute that can be looked up fresh
    /// left the lock, and this **cannot** be. What `{scope.rel_path}` means is
    /// a property of the *selection* — the shape the user was looking at,
    /// which may be shallower than where the files are — and it is not
    /// recoverable from the entries' own paths afterwards.
    ///
    /// `None` has two meanings, and only the lock as a whole can tell them
    /// apart: a lock with **no header** predates this field, and a lock whose
    /// header records **no scope** had nowhere to measure from. A run that
    /// confirmed a scope always fills this for every entry, because entries
    /// are selected from the same register the measurement is taken from.
    /// See `archive::ops::manifest::LockFile::unmeasured_reason`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope_rel_path: Option<String>,
}

impl LockEntry {
    /// Build a LockEntry from a Source and object hash info.
    ///
    /// The measurement is not set here: it is a property of the run's whole
    /// scope, not of one source, so it is filled afterwards for the entry set
    /// as a whole (`archive::ops::generate::measure_entries`).
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
            scope_rel_path: None,
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
    // The recorded scope — one representation, two on-disk forms
    // =========================================================================

    fn meta_err(scope_toml: &str) -> String {
        let toml_str = format!(
            r#"
[meta]
version = 1
query = []
{scope_toml}
generated_at = "2026-02-15T12:00:00Z"
lock_hash = "abc123"

[output]
pattern = "{{filename}}"
archive_root_id = 1
base_dir = ""
"#
        );
        match toml::from_str::<ManifestConfig>(&toml_str) {
            Ok(_) => panic!("a string scope must be refused"),
            Err(e) => e.to_string(),
        }
    }

    /// M1 — the version 1 joined form is refused, not reconstructed. Splitting
    /// it decides where files are moved on the strength of a guess, and a
    /// directory name may carry the separator itself.
    #[test]
    fn a_version_one_joined_scope_string_is_refused() {
        let err = meta_err(r#"scope = "/vol/work/proj-v1, /vol/work/proj-v2""#);
        assert!(err.contains("must be a list of paths"), "{err}");
        assert!(err.contains("cluster refresh --edit"), "{err}");
    }

    /// M2 — and so is the single-scope form, which is indistinguishable from
    /// the joined one without guessing. One rule, no special case.
    #[test]
    fn a_version_one_single_scope_string_is_refused_the_same_way() {
        let err = meta_err(r#"scope = "/vol/work/proj-v1""#);
        assert!(err.contains("must be a list of paths"), "{err}");
    }

    /// The refusal says what to do about it, rather than reporting that a
    /// string is not a sequence — which is true and useless.
    #[test]
    fn the_refusal_names_the_remedy_rather_than_the_type() {
        let err = meta_err(r#"scope = "/vol/work/proj-v1, v2/src""#);
        assert!(!err.contains("invalid type"), "{err}");
        assert!(err.contains("cluster refresh --edit"), "{err}");
    }

    /// M3 — what everything writes from now on: an array, round-tripping
    /// unchanged, and written as an array rather than a string.
    #[test]
    fn a_scope_list_round_trips_as_an_array() {
        let scope = vec![
            "/vol/work/proj-v1".to_string(),
            "/vol/work/proj-v2".to_string(),
        ];
        let config = ManifestConfig {
            meta: ManifestMeta {
                version: CURRENT_MANIFEST_VERSION,
                query: vec![],
                scope: scope.clone(),
                generated_at: "2026-02-15T12:00:00Z".to_string(),
                lock_hash: "abc123".to_string(),
            },
            options: ManifestOptions::default(),
            output: ManifestOutput {
                pattern: "{filename}".to_string(),
                archive_root_id: 1,
                base_dir: String::new(),
            },
        };
        let toml_str = toml::to_string_pretty(&config).unwrap();
        // An array, never a joined string — nothing writes the legacy form.
        assert!(toml_str.contains("scope = ["), "{toml_str}");
        assert!(!toml_str.contains(r#"scope = ""#), "{toml_str}");
        let parsed: ManifestConfig = toml::from_str(&toml_str).unwrap();
        assert_eq!(parsed.meta.scope, scope);
    }

    /// M4 — an unscoped manifest omits the field entirely, as it always has.
    #[test]
    fn a_manifest_with_no_scope_omits_the_field() {
        let config = ManifestConfig {
            meta: ManifestMeta {
                version: CURRENT_MANIFEST_VERSION,
                query: vec![],
                scope: vec![],
                generated_at: "2026-02-15T12:00:00Z".to_string(),
                lock_hash: "abc123".to_string(),
            },
            options: ManifestOptions::default(),
            output: ManifestOutput {
                pattern: "{filename}".to_string(),
                archive_root_id: 1,
                base_dir: String::new(),
            },
        };
        let toml_str = toml::to_string_pretty(&config).unwrap();
        assert!(!toml_str.contains("scope"), "{toml_str}");
        let parsed: ManifestConfig = toml::from_str(&toml_str).unwrap();
        assert!(parsed.meta.scope.is_empty());
    }

    /// M5 — the gate still refuses a manifest from a later Canon, now that
    /// the current version is 2 and 2 itself must be accepted.
    #[test]
    fn a_future_manifest_version_is_still_refused() {
        assert!(validate_manifest_version(CURRENT_MANIFEST_VERSION).is_ok());
        assert!(validate_manifest_version(CURRENT_MANIFEST_VERSION + 1).is_err());
    }

    /// A manifest from a later Canon whose **body shape** this binary cannot
    /// deserialize still gets the version refusal.
    ///
    /// This is the case the gate exists for and the only one it used to miss:
    /// a format change is what a version bump announces, so the file that most
    /// needs "update Canon" is the file serde cannot parse. The second
    /// assertion is what makes the first mean anything — it holds the body
    /// genuinely undeserializable, so the refusal can only have come from
    /// asking the version first.
    #[test]
    fn a_future_manifest_is_refused_by_version_before_its_shape() {
        let toml_str = r#"
[meta]
version = 99
query = []
scope = { paths = ["/photos"] }
generated_at = "2026-02-15T12:00:00Z"
lock_hash = "abc123"

[output]
pattern = "{filename}"
archive_root_id = 1
base_dir = ""
"#;
        assert!(
            toml::from_str::<ManifestConfig>(toml_str).is_err(),
            "the fixture must be a body this binary cannot deserialize, \
             or the test proves nothing about ordering"
        );

        let err = parse_manifest_config(toml_str, Path::new("/m.toml"))
            .err()
            .expect("a future manifest must be refused")
            .to_string();
        assert!(err.contains("99"), "{err}");
        assert!(err.contains("Please update Canon"), "{err}");
    }

    /// A body that is malformed for any reason *other* than its version still
    /// gets serde's own message: the probe is a gate, never a filter.
    #[test]
    fn a_malformed_current_manifest_still_gets_the_parse_error() {
        let toml_str = r#"
[meta]
version = 1
query = []
generated_at = "2026-02-15T12:00:00Z"
lock_hash = "abc123"
"#;
        let err = parse_manifest_config(toml_str, Path::new("/m.toml"))
            .err()
            .expect("a future manifest must be refused")
            .to_string();
        assert!(err.contains("Failed to parse manifest config"), "{err}");
        assert!(!err.contains("update Canon"), "{err}");
    }

    /// A manifest with no `meta.version` is an *old* one, not a future one:
    /// the probe leaves it to `default_version` rather than refusing it.
    #[test]
    fn a_manifest_without_a_version_is_not_refused() {
        let toml_str = r#"
[meta]
query = []
scope = ["/photos"]
generated_at = "2026-02-15T12:00:00Z"
lock_hash = "abc123"

[output]
pattern = "{filename}"
archive_root_id = 1
base_dir = ""
"#;
        let config = parse_manifest_config(toml_str, Path::new("/m.toml")).unwrap();
        assert_eq!(config.meta.version, 1);
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
                scope: vec!["/photos".to_string()],
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
scope = ["/photos"]
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
scope = ["/photos"]
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
                scope: vec!["/photos".to_string()],
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
