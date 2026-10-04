//! The manifest format — the on-disk contract between `cluster generate`
//! and `apply` — and the table that says which acts a manifest in a given
//! state can still take.
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
//!
//! The decision table at the foot of the file is the second concept here, and
//! it belongs beside the first: what stops `apply` and what stops `cluster
//! refresh` is a property of the manifest-and-lock pair this module already
//! defines. Its inputs arrive settled — someone else did the reading — so it
//! stays as pure as everything above it.

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
    #[expect(clippy::string_slice, reason = "just past a found ASCII marker")]
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

    #[expect(clippy::string_slice, reason = "a sum of whole-line lengths")]
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

// ============================================================================
// The status decision table
// ============================================================================

/// The lock file beside a manifest, as a reader found it.
///
/// An enum rather than three loose booleans, because two of the three claims
/// are claims about a file that was *read*: "its bytes no longer hash to what
/// the manifest records" and "it was written before the measurement" are both
/// unrepresentable where there is no file, and representing them anyway is how
/// a reader ends up asserting a hash mismatch against a lock that is simply
/// gone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockStanding {
    /// No lock file beside the manifest.
    ///
    /// An ordinary state, not a broken one: a refresh whose query matched
    /// nothing takes the lock away and records the empty hash. The two causes
    /// of an absence are told apart by the manifest's own record, which is why
    /// this carries it — an empty `lock_hash` is that zero-match refresh, a
    /// recorded one is a file that has since gone.
    Absent {
        /// Whether the manifest still names a lock hash.
        manifest_names_a_hash: bool,
    },
    /// The lock was read, with its two claims about itself.
    Read {
        /// Whether its bytes still hash to what the manifest records.
        hash_matches: bool,
        /// Whether it carries the header that records the scopes it was built
        /// from.
        /// `false` is a lock written before that header existed, which `apply`
        /// refuses outright whatever the pattern says.
        carries_measurement: bool,
    },
}

impl LockStanding {
    /// The standing of a manifest whose lock file is not there, read off the
    /// hash the manifest records.
    ///
    /// The one spelling of *which absence this is*. Both readers of that fact
    /// arrive at it from the same place — the manifest's `lock_hash` — and
    /// they are at opposite ends of the subsystem: `cluster status` names the
    /// absence in its report, `apply` chooses which way back to print. A
    /// second `is_empty()` beside this one would be the same question asked
    /// twice, and the day the empty hash stops meaning *matched nothing* only
    /// one of them would learn it.
    pub fn absent(recorded_lock_hash: &str) -> Self {
        LockStanding::Absent {
            manifest_names_a_hash: !recorded_lock_hash.is_empty(),
        }
    }

    /// Whether this is an absence whose manifest still names a lock hash — a
    /// lock that existed and has since gone, rather than a refresh that
    /// matched nothing.
    pub fn lost_a_lock_it_names(self) -> bool {
        matches!(
            self,
            LockStanding::Absent {
                manifest_names_a_hash: true
            }
        )
    }
}

/// Everything about a manifest's own state that decides whether either act
/// reading it can run — the inputs of the decision table below.
///
/// Pure, and built by whoever has already done the reading: the facts arrive
/// settled, and nothing here touches a file, a database or a root.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManifestStanding {
    /// What the lock beside it came to.
    pub lock: LockStanding,
    /// Whether the destination pattern expands for the lock's entries as they
    /// stand. Vacuously true where there are no entries to expand it for.
    pub pattern_expands: bool,
    /// Whether the pattern places files relative to a scope the **lock** cannot
    /// place them from — the lock's own answer
    /// (`ops::manifest::LockFile::pattern_needs_a_scope_it_lacks`), never the
    /// manifest's: a manifest may declare a scope its lock was built without.
    /// This is what a rebuild can supply, so it is what decides whether one
    /// clears an unexpandable pattern; never true where the pattern expands.
    pub pattern_needs_a_scope_it_lacks: bool,
    /// Whether any lock entry's source stands on a root the user closed.
    pub parked_origins: bool,
    /// Whether the destination archive root is one the user closed.
    pub parked_destination: bool,
    /// Whether any recorded scope line stands on a root the user closed.
    pub parked_scope: bool,
    /// Whether the recorded scope kept nothing — the source-existence policy's
    /// terminal rule (`ScopeResolution::kept_nothing`).
    pub scope_kept_nothing: bool,
    /// Whether no recorded prefix confirmed, so a rebuild would supply no
    /// measurement (`ScopeResolution::scopes` empty).
    pub scope_confirms_nothing: bool,
}

/// The two acts that read a manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Act {
    /// `canon apply` — carries the lock's entries into the destination.
    Apply,
    /// `canon cluster refresh` — re-queries from the recorded scope and
    /// rewrites the manifest and its lock.
    Refresh,
}

/// Why a `cluster refresh` would be turned away.
///
/// The two rows of the table below that stop a refresh, as the one question a
/// caller telling the user about a refresh needs answered. Separate from
/// [`Obstacle`] because a caller asking it must not have to say what the six
/// apply-only rows would mean here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshRefusal {
    /// A recorded scope line stands on a root the user closed.
    Door,
    /// The recorded scope kept nothing — the source-existence policy's
    /// terminal rule.
    ScopeKeptNothing,
}

/// What a withheld step still owes the reader.
///
/// **A report that names no act has not finished answering.** Where no act
/// brings an apply within reach, the reader is left with a cause and nothing
/// to do about it — which is only honest where the cause was announced with
/// its own way back. Some are: a closed door is always named with the command
/// that opens it. The rest are not, and this says which sentence they owe.
///
/// It is the table's to answer rather than the report's, for the same reason
/// [`RefreshRefusal`] is: deciding it at the printing end would re-test causes
/// the table already holds, and re-spell the precedence between them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwedWayBack {
    /// Nothing further: the line that named the cause carries its own act, or
    /// a step was named and this question does not arise.
    Stated,
    /// The recorded scope kept nothing, and the line saying so names no act.
    ScanOrEditScope,
    /// The pattern cannot expand and no rebuild would change that, so what
    /// moves this manifest is an edit to the pattern or to what it names.
    AdjustPattern,
}

/// One thing standing between a manifest and an act that reads it.
///
/// **The table, and why it is written out rather than reasoned to.** The
/// question *can `apply` run at all* was a nest of booleans in the report,
/// listing four causes where the command itself refuses on more; a fifth input
/// arriving found nobody able to say what the new combination should do, and
/// two separate reviews reasoned to opposite wrong answers about the same
/// withholding rule. So the rows are named, each stops exactly one act, and
/// the matches below carry no catch-all arm.
///
/// | Row | Stops `apply` | Stops `cluster refresh` |
/// |---|---|---|
/// | [`NoLock`](Self::NoLock) | yes — there is nothing to carry across | no — a refresh *writes* the lock; it never reads one |
/// | [`LockHashMismatch`](Self::LockHashMismatch) | yes | no — same reason |
/// | [`LockPredatesMeasurement`](Self::LockPredatesMeasurement) | yes | no — same reason |
/// | [`PatternUnexpandable`](Self::PatternUnexpandable) | yes | no — a refresh names no destination; only apply expands the pattern to move a file |
/// | [`ParkedOrigins`](Self::ParkedOrigins) | yes | no — a record is not a naming: the rebuild re-selects from the open world and leaves them out |
/// | [`ParkedDestination`](Self::ParkedDestination) | yes | no — a refresh writes only the manifest and its lock, never into the archive root |
/// | [`ParkedScope`](Self::ParkedScope) | no — apply reads no recorded scope, taking its own from the lock header | yes |
/// | [`ScopeKeptNothing`](Self::ScopeKeptNothing) | no — same reason | yes |
///
/// **Two unreachable cells, and one that only looks like them.** With no lock
/// there is no hash to mismatch and no header to be missing, so `NoLock` can
/// never be raised beside `LockHashMismatch` or `LockPredatesMeasurement` —
/// **unrepresentable**, by the shape of [`LockStanding`] rather than by
/// anyone's discipline, since both are claims about a file somebody read.
///
/// `PatternUnexpandable` beside `NoLock` is a third impossible state and is
/// **not** of that kind, which is worth saying because the three read alike.
/// With no lock there are no entries, so nothing can fail to expand — but
/// `pattern_expands` is a field beside the enum, not inside it, so the pair
/// *is* constructible here and only the caller's derivation keeps it from
/// arising. The guarantee is therefore a construction fact of
/// `archive::ops::status::compute_manifest_status`, pinned where it is made
/// (`an_absent_lock_is_a_state_the_read_states`, over a real manifest whose
/// lock was removed) rather than claimed here; what this file pins is the
/// pair the enum really does own
/// (`a_manifest_with_no_lock_raises_exactly_one_obstacle`, which also poses
/// the constructible pair so the difference is visible rather than implied).
///
/// **What this table does *not* enforce**, named here rather than left to be
/// discovered: these are the obstacles a *report* can see. `canon apply`
/// refuses at further sites of its own — a non-archive destination root, a
/// `base_dir` escaping the archive root, unhashed entries, and every preflight
/// finding about the world moving under the lock — and those refusals are not
/// raised from this table. Each row below names the `apply` site that enforces
/// it; a *new* apply refusal with no row here is not build-refused, and that
/// is the standing gap this enumeration narrows rather than closes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Obstacle {
    /// No lock file beside the manifest, so there is nothing to carry across.
    /// Enforced at `apply`'s own lock read.
    ///
    /// **Unreachable in the report today** — a manifest with no lock has
    /// nothing pending, and the interface asks for a step only where
    /// something is. Written out anyway, because a row answering nothing is
    /// how a table starts being guessed at, and answered from the standing
    /// rather than at face value, because nothing observes this row being
    /// wrong.
    ///
    /// Its rebuild answer turns on which absence this is. A manifest still
    /// naming a lock hash lost a lock that existed, and a refresh rebuilds
    /// it: `Clears`. A manifest recording the empty hash has no lock because
    /// its own last refresh matched nothing, and a lock is a pure function of
    /// the query, the scope and the index — so refreshing again with none of
    /// the three touched reproduces that same answer: `Leaves`. That is a
    /// **fixed point, not a forecast**: it says what running the same act
    /// over the same inputs comes to, and claims nothing about what the query
    /// would match after the user changes one of them. The interface reads
    /// the same fact the same way (`cli.rs`’s `no_lock_remedy`), which is the
    /// point of the row existing.
    NoLock,
    /// The lock's bytes no longer hash to what the manifest records.
    /// Enforced by `apply`'s lock-hash comparison, before anything is planned.
    LockHashMismatch,
    /// The lock was built before lock files recorded the scopes they were
    /// built from.
    /// Enforced by `apply`'s absent-header refusal, which fires whatever the
    /// pattern says, because a scoped act recorded as a global one is the
    /// silence the lock header exists to close.
    LockPredatesMeasurement,
    /// The destination pattern does not expand for the entries as they stand.
    /// Enforced by `apply`'s expansion-failure refusal, which collects every
    /// failure and aborts before it transfers anything.
    PatternUnexpandable,
    /// Lock entries stand on a root the user closed.
    /// Enforced by `apply`'s origin door, over the one computation
    /// `cluster status` states in apply's conditional.
    ParkedOrigins,
    /// The destination archive root is one the user closed.
    /// Enforced by `apply`'s destination door, asked before the summary.
    ParkedDestination,
    /// A recorded scope line stands on a root the user closed.
    /// Enforced by `cluster refresh`, which refuses whole rather than
    /// rebuilding from half a declaration.
    ParkedScope,
    /// The recorded scope kept nothing — the source-existence policy's
    /// terminal rule. Enforced by `cluster refresh`, which names every
    /// set-aside line as it refuses.
    ScopeKeptNothing,
}

/// What a `cluster refresh` does about one obstacle.
///
/// Total over every row, so the rebuild question needs no partial function:
/// an obstacle that stops the refresh itself answers
/// [`Refused`](Self::Refused) rather than nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rebuild {
    /// A rebuild clears it, so the apply it stops comes within reach.
    Clears,
    /// A rebuild leaves it exactly where it is.
    Leaves,
    /// It is the rebuild's own obstacle: the refresh is refused, so there is
    /// no rebuild to ask about.
    Refused,
}

/// The step that moves a manifest forward, as far as the two acts go.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NextStep {
    /// The apply can run. Whether it is named plain or as a resume is a
    /// question about counts, which this table does not hold.
    Apply,
    /// The apply cannot, and a rebuild is the act that brings it within reach.
    Rebuild,
    /// No act either command offers brings the apply nearer. The way back —
    /// a door's own, or the reason a pattern will not expand — was stated
    /// where the cause was.
    None,
}

impl Obstacle {
    /// Which act this row stops. Exactly one, by the table above.
    pub fn stops(self) -> Act {
        match self {
            Obstacle::NoLock
            | Obstacle::LockHashMismatch
            | Obstacle::LockPredatesMeasurement
            | Obstacle::PatternUnexpandable
            | Obstacle::ParkedOrigins
            | Obstacle::ParkedDestination => Act::Apply,
            Obstacle::ParkedScope | Obstacle::ScopeKeptNothing => Act::Refresh,
        }
    }

    /// What a rebuild does about this row, given the manifest it stands on.
    ///
    /// Only one row needs the standing to answer, and it is the row a rebuild
    /// can go either way on: a pattern fails to expand for reasons a rebuild
    /// supplies and for reasons it cannot. It supplies exactly one — the
    /// scope-relative path of an entry the lock carries no measurement for —
    /// and only where the recorded scope confirms something to measure from.
    /// Both halves are read from the standing, and neither from the lock's
    /// header: a lock *with* a header can still be measured from nothing (it
    /// was built while the manifest declared no scope), and a manifest can
    /// declare a confirming scope its lock has never seen — that is the
    /// ordinary edit-then-refresh workflow, and the rebuild is exactly the
    /// step it needs. A manifest that records no scope, or none that confirms,
    /// rebuilds into an identically unmeasured lock with only the stated
    /// reason changed, which is not a step; and a pattern that fails for a
    /// fact no source carries is helped by no rebuild at all.
    ///
    /// **The residual, declared rather than hidden**: where a rebuild *would*
    /// supply the measurement, this answers `Clears` without knowing whether
    /// the pattern also names a fact nothing supplies. A pattern failing for
    /// both reasons is pointed at a rebuild that only half helps. Closing it
    /// needs the expansion failure to carry its own cause out of the
    /// expression facility, which no reader asks for today.
    pub fn rebuild(self, standing: &ManifestStanding) -> Rebuild {
        match self {
            Obstacle::LockHashMismatch
            | Obstacle::LockPredatesMeasurement
            | Obstacle::ParkedOrigins => Rebuild::Clears,
            // A lock that went missing is rebuilt; a lock that was never
            // written because the query matched nothing is not, because the
            // rebuild is the same act over the same inputs.
            Obstacle::NoLock => {
                if standing.lock.lost_a_lock_it_names() {
                    Rebuild::Clears
                } else {
                    Rebuild::Leaves
                }
            }
            Obstacle::PatternUnexpandable => {
                if standing.pattern_needs_a_scope_it_lacks && !standing.scope_confirms_nothing {
                    Rebuild::Clears
                } else {
                    Rebuild::Leaves
                }
            }
            // No rebuild moves a destination.
            Obstacle::ParkedDestination => Rebuild::Leaves,
            Obstacle::ParkedScope | Obstacle::ScopeKeptNothing => Rebuild::Refused,
        }
    }
}

impl ManifestStanding {
    /// Every obstacle this manifest raises, in the table's own order.
    pub fn obstacles(&self) -> Vec<Obstacle> {
        let mut raised = Vec::new();
        match self.lock {
            LockStanding::Absent { .. } => raised.push(Obstacle::NoLock),
            LockStanding::Read {
                hash_matches,
                carries_measurement,
            } => {
                if !hash_matches {
                    raised.push(Obstacle::LockHashMismatch);
                }
                if !carries_measurement {
                    raised.push(Obstacle::LockPredatesMeasurement);
                }
            }
        }
        if !self.pattern_expands {
            raised.push(Obstacle::PatternUnexpandable);
        }
        if self.parked_origins {
            raised.push(Obstacle::ParkedOrigins);
        }
        if self.parked_destination {
            raised.push(Obstacle::ParkedDestination);
        }
        if self.parked_scope {
            raised.push(Obstacle::ParkedScope);
        }
        if self.scope_kept_nothing {
            raised.push(Obstacle::ScopeKeptNothing);
        }
        raised
    }

    /// Whether this act can run, as the manifest stands.
    pub fn can_run(&self, act: Act) -> bool {
        !self.obstacles().iter().any(|o| o.stops() == act)
    }

    /// Which of the two refusals turns a `cluster refresh` away, where one
    /// does.
    ///
    /// Asked because the answer changes what a reader is owed, not merely
    /// whether an act is named: a closed door announces itself with the act
    /// that opens it, and a scope that kept nothing announces itself with a
    /// skipped line carrying no act at all. An interface deciding that from
    /// `parked_scope` and `scope_kept_nothing` would be re-testing causes the
    /// table already holds, and would re-spell the precedence between them.
    ///
    /// **The door outranks the terminal rule**, matching the order `cluster
    /// refresh` itself asks in — it refuses on a door before it looks at what
    /// the scope kept — so a manifest with both is told about the door, whose
    /// way back is the one already on screen.
    ///
    /// Its `None` is exactly [`can_run(Act::Refresh)`](Self::can_run) — the
    /// same question, answered with its cause — and the two are held to that
    /// by `the_refresh_refusal_agrees_with_the_predicate`, over every shape
    /// the table can take.
    pub fn refresh_refused_by(&self) -> Option<RefreshRefusal> {
        self.obstacles().into_iter().find_map(|o| match o {
            Obstacle::ParkedScope => Some(RefreshRefusal::Door),
            Obstacle::ScopeKeptNothing => Some(RefreshRefusal::ScopeKeptNothing),
            // Every row that stops `apply` and never a refresh. Spelled out
            // rather than swept into a rest, so a new refresh-stopping row
            // cannot arrive without this answering for it.
            Obstacle::NoLock
            | Obstacle::LockHashMismatch
            | Obstacle::LockPredatesMeasurement
            | Obstacle::PatternUnexpandable
            | Obstacle::ParkedOrigins
            | Obstacle::ParkedDestination => None,
        })
    }

    /// What the reader is owed where [`next_step`](Self::next_step) names no
    /// act.
    ///
    /// **Three causes reach that answer and only one of them speaks for
    /// itself.** A door is announced with `canon roots unsuspend`, so it needs
    /// nothing here. A recorded scope that kept nothing is announced as `no
    /// sources known at <p> — skipped`, which names no act. A pattern that
    /// cannot expand states why it cannot, and where no rebuild clears it on
    /// its own — nothing to measure from, a fact no refresh supplies, a save
    /// time or a grain no refresh changes — what moves it is an edit: to the
    /// pattern, the facts or the query, or the file's save time — the query
    /// then refreshed, the file scanned and then refreshed. The way out the
    /// report prints is composed per cause.
    ///
    /// **The scope outranks the pattern**, because a scope that kept nothing
    /// is also why the pattern has nothing to measure from: fixing the scope
    /// is fixing both, and pointing at the pattern first would send the reader
    /// to edit a line that was never the problem.
    ///
    /// Spelled over every row rather than swept into a rest, like
    /// [`refresh_refused_by`](Self::refresh_refused_by) beside it, so a new
    /// row cannot arrive without this saying what it owes.
    pub fn owed_way_back(&self) -> OwedWayBack {
        if !matches!(self.next_step(), NextStep::None) {
            return OwedWayBack::Stated;
        }
        let mut owed = OwedWayBack::Stated;
        for obstacle in self.obstacles() {
            owed = match obstacle {
                Obstacle::ScopeKeptNothing => return OwedWayBack::ScanOrEditScope,
                Obstacle::PatternUnexpandable => match obstacle.rebuild(self) {
                    // A rebuild that clears it was already named as the step,
                    // and a refused one is the door's or the scope's answer
                    // below, not the pattern's.
                    Rebuild::Clears | Rebuild::Refused => owed,
                    Rebuild::Leaves => OwedWayBack::AdjustPattern,
                },
                // Each of these is either named on a line carrying its own
                // act, or is not what withheld the step.
                Obstacle::ParkedScope
                | Obstacle::ParkedDestination
                | Obstacle::ParkedOrigins
                | Obstacle::NoLock
                | Obstacle::LockHashMismatch
                | Obstacle::LockPredatesMeasurement => owed,
            };
        }
        owed
    }

    /// The step that moves this manifest forward.
    ///
    /// **A step is named where it is the act that brings an apply within
    /// reach** — the narrower of two readings, and the one a reader takes. A
    /// rebuild is named where it would run *and* would clear every obstacle
    /// standing in the apply's way; where one of them survives a rebuild, no
    /// act this report can name gets the reader further, and the cause's own
    /// way back is the whole answer.
    ///
    /// **The narrowing reaches only what a report knows, never what a query
    /// will match.** A parked destination is knowable: no rebuild moves it. A
    /// lock standing wholly behind a door is not — a refresh re-queries the
    /// open world, which holds content this lock has never seen — so the act
    /// that runs is named and the refresh states its own outcome.
    pub fn next_step(&self) -> NextStep {
        let obstacles = self.obstacles();
        let stopping_apply: Vec<Obstacle> = obstacles
            .iter()
            .copied()
            .filter(|o| o.stops() == Act::Apply)
            .collect();
        if stopping_apply.is_empty() {
            return NextStep::Apply;
        }
        let rebuild_runs = self.can_run(Act::Refresh);
        if rebuild_runs
            && stopping_apply
                .iter()
                .all(|o| o.rebuild(self) == Rebuild::Clears)
        {
            NextStep::Rebuild
        } else {
            NextStep::None
        }
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

    // =========================================================================
    // The status decision table
    // =========================================================================

    /// A manifest nothing is wrong with: the lock is there, in order, and
    /// carries its measurement; the pattern expands; no door is closed and the
    /// recorded scope confirmed.
    fn sound() -> ManifestStanding {
        ManifestStanding {
            lock: LockStanding::Read {
                hash_matches: true,
                carries_measurement: true,
            },
            pattern_expands: true,
            pattern_needs_a_scope_it_lacks: false,
            parked_origins: false,
            parked_destination: false,
            parked_scope: false,
            scope_kept_nothing: false,
            scope_confirms_nothing: false,
        }
    }

    /// Every row of the table, listed once.
    fn all_obstacles() -> Vec<Obstacle> {
        vec![
            Obstacle::NoLock,
            Obstacle::LockHashMismatch,
            Obstacle::LockPredatesMeasurement,
            Obstacle::PatternUnexpandable,
            Obstacle::ParkedOrigins,
            Obstacle::ParkedDestination,
            Obstacle::ParkedScope,
            Obstacle::ScopeKeptNothing,
        ]
    }

    /// Where each row sits in the census above — an exhaustive match, so a new
    /// obstacle cannot be minted without being given a place here.
    fn position(obstacle: Obstacle) -> usize {
        match obstacle {
            Obstacle::NoLock => 0,
            Obstacle::LockHashMismatch => 1,
            Obstacle::LockPredatesMeasurement => 2,
            Obstacle::PatternUnexpandable => 3,
            Obstacle::ParkedOrigins => 4,
            Obstacle::ParkedDestination => 5,
            Obstacle::ParkedScope => 6,
            Obstacle::ScopeKeptNothing => 7,
        }
    }

    /// **The census is complete, and matched both directions.** A row minted
    /// without a place fails [`position`]'s match at compile time; a row given
    /// a place but left out of the list fails the run here. Eight, because the
    /// charter's seven conditions split the manifest door in two — the origin
    /// end and the destination end meet different acts — and the recorded
    /// scope's own two reasons are a door and the terminal rule, which are
    /// likewise not one row.
    #[test]
    fn the_table_holds_every_row_once() {
        let all = all_obstacles();
        let mut places: Vec<usize> = all.iter().copied().map(position).collect();
        places.sort_unstable();
        places.dedup();
        assert_eq!(
            places,
            (0..all.len()).collect::<Vec<_>>(),
            "every row listed once, and none listed twice"
        );
        assert_eq!(all.len(), 8, "the table's row count");
    }

    /// Each row stops exactly one act, and which one is the table's own claim
    /// rather than a reading of the code: apply transfers from the lock into
    /// the destination and reads no recorded scope, so the lock's states, the
    /// pattern's and the two ends' doors are its; a refresh re-queries from
    /// the recorded scope and writes the lock, so the scope's door and the
    /// sourceless terminal rule are its and nothing else is.
    #[test]
    fn each_row_stops_the_act_the_table_names() {
        for (obstacle, act) in [
            (Obstacle::NoLock, Act::Apply),
            (Obstacle::LockHashMismatch, Act::Apply),
            (Obstacle::LockPredatesMeasurement, Act::Apply),
            (Obstacle::PatternUnexpandable, Act::Apply),
            (Obstacle::ParkedOrigins, Act::Apply),
            (Obstacle::ParkedDestination, Act::Apply),
            (Obstacle::ParkedScope, Act::Refresh),
            (Obstacle::ScopeKeptNothing, Act::Refresh),
        ] {
            assert_eq!(obstacle.stops(), act, "{obstacle:?}");
        }
        // And the two acts partition the table: neither is left with nothing
        // to be stopped by, which would mean a predicate with no rows.
        for act in [Act::Apply, Act::Refresh] {
            assert!(all_obstacles().iter().any(|o| o.stops() == act));
        }
    }

    /// **The two cells the shape makes unreachable, and the one it does not.**
    /// With no lock there is no hash to mismatch and no header to be missing,
    /// so `NoLock` can never be raised beside either — structural, not a
    /// discipline: `LockStanding` has no representation for a lock that is
    /// absent and hash-mismatched at once, and the arm below would not compile
    /// if it tried to pose one.
    ///
    /// The pattern row is the one that reads like a third and is not. With no
    /// lock there are no entries, so nothing can fail to expand — but
    /// `pattern_expands` sits beside the enum rather than inside it, so the
    /// second half here **poses the pair and asserts what actually happens**:
    /// both rows are raised. It records where the shape stops, which is why
    /// the real guarantee is pinned at the derivation instead.
    ///
    /// **It is a record of a gap, not a rule**, and the distinction matters
    /// to whoever closes the gap: fold `pattern_expands` into `LockStanding`,
    /// or suppress the pattern row where the lock is absent, and this arm is
    /// what changes — delete it and say so, rather than reading it as a claim
    /// that the weaker shape is wanted.
    #[test]
    fn a_manifest_with_no_lock_raises_exactly_one_obstacle() {
        for manifest_names_a_hash in [false, true] {
            let standing = ManifestStanding {
                lock: LockStanding::Absent {
                    manifest_names_a_hash,
                },
                ..sound()
            };
            assert_eq!(standing.obstacles(), vec![Obstacle::NoLock]);
        }

        // Constructible today, and therefore not a claim this type may make.
        // The arm that retires when the shape is strengthened.
        let contradictory = ManifestStanding {
            lock: LockStanding::Absent {
                manifest_names_a_hash: true,
            },
            pattern_expands: false,
            ..sound()
        };
        assert_eq!(
            contradictory.obstacles(),
            vec![Obstacle::NoLock, Obstacle::PatternUnexpandable],
            "the pattern row is a construction fact of the caller, not a shape fact here"
        );
    }

    /// **A lock that went missing is rebuilt; one the query never matched is
    /// not.** The row answers from the manifest's own record rather than at
    /// face value, and the difference is a fixed point, not a forecast: a lock
    /// is a pure function of the query, the scope and the index, so refreshing
    /// with none of the three touched reproduces the same answer.
    ///
    /// Pinned because the row is unreachable in the report today — nothing
    /// observes it being wrong — and because the interface answers the same
    /// question from the same field (`cli.rs`’s `no_lock_remedy`), so the two
    /// must not drift.
    #[test]
    fn a_missing_lock_is_rebuilt_and_a_zero_match_one_is_not() {
        let lost = ManifestStanding {
            lock: LockStanding::Absent {
                manifest_names_a_hash: true,
            },
            ..sound()
        };
        assert_eq!(
            Obstacle::NoLock.rebuild(&lost),
            Rebuild::Clears,
            "a lock that existed and went is what a refresh rebuilds"
        );

        let matched_nothing = ManifestStanding {
            lock: LockStanding::Absent {
                manifest_names_a_hash: false,
            },
            ..sound()
        };
        assert_eq!(
            Obstacle::NoLock.rebuild(&matched_nothing),
            Rebuild::Leaves,
            "the same query over the same index matches nothing again"
        );

        // And the row is the one the standing raises in both cases, so the
        // answers above are the ones a reader of the table actually meets.
        assert_eq!(lost.obstacles(), vec![Obstacle::NoLock]);
        assert_eq!(matched_nothing.obstacles(), vec![Obstacle::NoLock]);
    }
    /// **Every withheld step is either self-explaining or told what to do.**
    ///
    /// Walked over every shape the table can take — every input of
    /// [`ManifestStanding`], the lock over both absences and all four
    /// readings — rather than the shapes that arise today: the claim is that
    /// a reader left without a step is never left without an answer, and a
    /// new row that withholds a step must say here what it owes, or fail.
    /// One pair the construction rules out is skipped by name rather than
    /// asserted about: with no lock there are no entries, so nothing can fail
    /// to expand (the construction fact `an_absent_lock_is_a_state_the_read_states`
    /// pins) — and the skip is load-bearing, since the table answers that
    /// pair inconsistently. A pattern that needs a scope it lacks cannot
    /// expand either, but that pair is walked anyway: the table answers it
    /// consistently, so nothing rests on the skip and it is not made.
    ///
    /// Two of the three answers are pinned by name. A door owes nothing,
    /// because its own line carries `canon roots unsuspend`. A scope that kept
    /// nothing outranks a pattern that cannot expand, because it is why the
    /// pattern has nothing to measure from — sending the reader at the pattern
    /// there would be sending them to edit a line that was never the problem.
    #[test]
    fn a_withheld_step_is_never_left_without_an_answer() {
        for lock in [
            LockStanding::absent(""),
            LockStanding::absent("abc123"),
            LockStanding::Read {
                hash_matches: true,
                carries_measurement: true,
            },
            LockStanding::Read {
                hash_matches: true,
                carries_measurement: false,
            },
            LockStanding::Read {
                hash_matches: false,
                carries_measurement: true,
            },
            LockStanding::Read {
                hash_matches: false,
                carries_measurement: false,
            },
        ] {
            for pattern_expands in [true, false] {
                for pattern_needs_a_scope_it_lacks in [true, false] {
                    for parked_origins in [true, false] {
                        for parked_scope in [true, false] {
                            for parked_destination in [true, false] {
                                for scope_kept_nothing in [true, false] {
                                    for scope_confirms_nothing in [true, false] {
                                        if !pattern_expands
                                            && matches!(lock, LockStanding::Absent { .. })
                                        {
                                            continue;
                                        }
                                        let standing = ManifestStanding {
                                            lock,
                                            pattern_expands,
                                            pattern_needs_a_scope_it_lacks,
                                            parked_origins,
                                            parked_destination,
                                            parked_scope,
                                            scope_kept_nothing,
                                            scope_confirms_nothing,
                                        };
                                        let owed = standing.owed_way_back();
                                        if !matches!(standing.next_step(), NextStep::None) {
                                            assert_eq!(
                                                owed,
                                                OwedWayBack::Stated,
                                                "a named step asks nothing of this: {standing:?}"
                                            );
                                            continue;
                                        }
                                        if scope_kept_nothing {
                                            assert_eq!(
                                                owed,
                                                OwedWayBack::ScanOrEditScope,
                                                "the scope outranks the pattern: {standing:?}"
                                            );
                                        }
                                        // Exhaustive on purpose: a fourth
                                        // answer fails to compile here until
                                        // it says which input earns it.
                                        match owed {
                                            // Parked origins are deliberately
                                            // not in this list: a rebuild
                                            // clears them and never withholds
                                            // the step, so they can never be
                                            // what left the reader without one.
                                            OwedWayBack::Stated => assert!(
                                                parked_scope
                                                    || parked_destination
                                                    || pattern_expands,
                                                "nothing here states its own way back: {standing:?}"
                                            ),
                                            OwedWayBack::ScanOrEditScope => assert!(
                                                scope_kept_nothing,
                                                "a scan offered where the scope kept something: {standing:?}"
                                            ),
                                            OwedWayBack::AdjustPattern => assert!(
                                                !pattern_expands && !scope_kept_nothing,
                                                "a pattern remedy owed by something other than the pattern: {standing:?}"
                                            ),
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// **The refusal and the predicate are one question**, so they may never
    /// disagree — and the refusal must name the door first, matching the
    /// order `cluster refresh` itself refuses in.
    ///
    /// Walked over every combination of the two scope inputs against every
    /// lock shape, rather than the shapes that happen to arise today: this
    /// exists so that a row added to one answer and not the other fails here.
    #[test]
    fn the_refresh_refusal_agrees_with_the_predicate() {
        let locks = [
            LockStanding::Absent {
                manifest_names_a_hash: true,
            },
            LockStanding::Absent {
                manifest_names_a_hash: false,
            },
            LockStanding::Read {
                hash_matches: true,
                carries_measurement: true,
            },
            LockStanding::Read {
                hash_matches: false,
                carries_measurement: true,
            },
            LockStanding::Read {
                hash_matches: true,
                carries_measurement: false,
            },
        ];
        for lock in locks {
            for parked_scope in [false, true] {
                for scope_kept_nothing in [false, true] {
                    let standing = ManifestStanding {
                        lock,
                        parked_scope,
                        scope_kept_nothing,
                        ..sound()
                    };
                    let refused = standing.refresh_refused_by();
                    assert_eq!(
                        refused.is_none(),
                        standing.can_run(Act::Refresh),
                        "the cause and the yes/no must answer alike: {standing:?}"
                    );
                    let expected = match (parked_scope, scope_kept_nothing) {
                        (true, _) => Some(RefreshRefusal::Door),
                        (false, true) => Some(RefreshRefusal::ScopeKeptNothing),
                        (false, false) => None,
                    };
                    assert_eq!(
                        refused, expected,
                        "the door outranks the terminal rule: {standing:?}"
                    );
                }
            }
        }
    }

    /// A sound manifest raises nothing, both acts run, and the step is the
    /// apply — the row every other row is measured against.
    #[test]
    fn a_sound_manifest_raises_nothing_and_points_at_the_apply() {
        let standing = sound();
        assert!(standing.obstacles().is_empty());
        assert!(standing.can_run(Act::Apply));
        assert!(standing.can_run(Act::Refresh));
        assert_eq!(standing.next_step(), NextStep::Apply);
    }

    /// **A lock whose hash no longer matches stops the apply, and the rebuild
    /// is the step.** The report used to read this for a header warning and
    /// never for the decision, so it invited the one command that refuses by
    /// name — pointing, in its refusal, at the very rebuild the report already
    /// knew how to print.
    #[test]
    fn a_lock_hash_mismatch_stops_the_apply_and_the_rebuild_is_the_step() {
        let standing = ManifestStanding {
            lock: LockStanding::Read {
                hash_matches: false,
                carries_measurement: true,
            },
            ..sound()
        };
        assert_eq!(standing.obstacles(), vec![Obstacle::LockHashMismatch]);
        assert!(!standing.can_run(Act::Apply));
        assert!(standing.can_run(Act::Refresh));
        assert_eq!(standing.next_step(), NextStep::Rebuild);
    }

    /// **A rebuild that would supply no measurement is not a step.** An
    /// unmeasured lock has two causes and only one of them is a lock: where
    /// the manifest records no scope that confirms, rebuilding produces an
    /// identically unmeasured lock and changes only the stated reason. The
    /// arm above it — the same old lock over a scope that *does* confirm — is
    /// what makes this a discrimination rather than a blanket silence.
    #[test]
    fn a_rebuild_that_supplies_no_measurement_is_not_a_step() {
        let old_and_unexpandable = ManifestStanding {
            lock: LockStanding::Read {
                hash_matches: true,
                carries_measurement: false,
            },
            pattern_expands: false,
            pattern_needs_a_scope_it_lacks: true,
            ..sound()
        };
        assert_eq!(
            old_and_unexpandable.next_step(),
            NextStep::Rebuild,
            "the scope confirms, so the rebuild measures and the pattern expands"
        );

        let nothing_to_measure_from = ManifestStanding {
            scope_confirms_nothing: true,
            ..old_and_unexpandable
        };
        assert_eq!(
            nothing_to_measure_from.next_step(),
            NextStep::None,
            "only the stated reason would change"
        );
        assert_eq!(
            Obstacle::PatternUnexpandable.rebuild(&nothing_to_measure_from),
            Rebuild::Leaves
        );

        // A pattern failing for a fact no source carries is helped by no
        // rebuild, however old the lock: the header's age is not the fact
        // this row reads.
        let old_and_missing_a_fact = ManifestStanding {
            pattern_needs_a_scope_it_lacks: false,
            ..old_and_unexpandable
        };
        assert_eq!(
            old_and_missing_a_fact.next_step(),
            NextStep::None,
            "no rebuild supplies a fact"
        );

        // And a current lock whose pattern will not expand is untouched by
        // this: no rebuild ever supplied its measurement.
        let current_and_unexpandable = ManifestStanding {
            pattern_expands: false,
            ..sound()
        };
        assert_eq!(current_and_unexpandable.next_step(), NextStep::None);
    }

    /// **The rebuild is decided from what the lock carries, never from
    /// whether it carries a header.** A lock written while the manifest
    /// declared no scope has a header and no measurement; declare a scope
    /// that confirms and refresh, and the entries are measured — so the
    /// rebuild is the step, and withholding it would send the reader back to
    /// an edit they have already made. The table once read the header here
    /// and told such a manifest it declared none.
    #[test]
    fn a_declared_scope_the_lock_has_not_seen_is_rebuilt_into_it() {
        let declared_after_the_lock = ManifestStanding {
            lock: LockStanding::Read {
                hash_matches: true,
                carries_measurement: true,
            },
            pattern_expands: false,
            pattern_needs_a_scope_it_lacks: true,
            scope_confirms_nothing: false,
            ..sound()
        };
        assert_eq!(
            Obstacle::PatternUnexpandable.rebuild(&declared_after_the_lock),
            Rebuild::Clears,
            "the header's presence says nothing about what the entries carry"
        );
        assert_eq!(declared_after_the_lock.next_step(), NextStep::Rebuild);
        assert_eq!(
            declared_after_the_lock.owed_way_back(),
            OwedWayBack::Stated,
            "a named step owes no remedy"
        );
    }

    /// **A recorded scope that kept nothing withholds the rebuild**, though no
    /// door is closed anywhere. The report once asked only about doors, and so
    /// named a refresh the source-existence policy's terminal rule turns away
    /// — a pointer whose only outcome is a second refusal.
    #[test]
    fn a_scope_that_kept_nothing_withholds_the_rebuild() {
        let standing = ManifestStanding {
            lock: LockStanding::Read {
                hash_matches: true,
                carries_measurement: false,
            },
            scope_kept_nothing: true,
            scope_confirms_nothing: true,
            ..sound()
        };
        assert!(!standing.can_run(Act::Refresh), "the terminal rule refuses");
        assert!(!standing.can_run(Act::Apply), "the lock is an old one");
        assert_eq!(standing.next_step(), NextStep::None);
        assert_eq!(
            Obstacle::ScopeKeptNothing.rebuild(&standing),
            Rebuild::Refused
        );
    }

    /// The scope's two rows stop the refresh and **not** the apply, which
    /// reads no recorded scope at all: a manifest scoped across two roots with
    /// entries on only one applies perfectly well when the other is closed.
    #[test]
    fn the_recorded_scope_stops_no_apply() {
        for standing in [
            ManifestStanding {
                parked_scope: true,
                ..sound()
            },
            ManifestStanding {
                scope_kept_nothing: true,
                scope_confirms_nothing: true,
                ..sound()
            },
        ] {
            assert!(standing.can_run(Act::Apply));
            assert!(!standing.can_run(Act::Refresh));
            assert_eq!(standing.next_step(), NextStep::Apply);
        }
    }

    /// A parked destination is the one apply obstacle no rebuild moves, so it
    /// silences the pointer even beside obstacles a rebuild would clear.
    #[test]
    fn a_parked_destination_leaves_every_rebuild_where_it_was() {
        let dest = ManifestStanding {
            parked_destination: true,
            ..sound()
        };
        assert_eq!(Obstacle::ParkedDestination.rebuild(&dest), Rebuild::Leaves);
        assert_eq!(dest.next_step(), NextStep::None);

        let both = ManifestStanding {
            parked_origins: true,
            ..dest
        };
        assert_eq!(Obstacle::ParkedOrigins.rebuild(&both), Rebuild::Clears);
        assert_eq!(
            both.next_step(),
            NextStep::None,
            "a refresh would run, but it addresses nothing the reader is stuck on"
        );
    }
}
