//! Scope resolution for Canon commands.
//!
//! All scope-taking commands follow a unified scope model: CWD is an intentional
//! context switch. When the user is inside a scanned root, Canon scopes to that
//! directory. When outside any root, Canon operates globally. The `--global` flag
//! overrides CWD defaulting.
//!
//! This applies to both discovery commands (ls, survey, facts, coverage, worklist)
//! and effectful commands (cluster generate, exclude set/clear/duplicates).
//!
//! This module also hosts path and root resolution against known roots, with
//! a filesystem fallback for the offline-pure predicates in
//! `core::domain::path`/`core::domain::root` — the soft-match-then-fallback
//! pattern for source-querying
//! commands (file-accessing commands like `scan` use `fs::canonicalize` directly).

use anyhow::{bail, Result};
use std::path::{Path, PathBuf};

use super::fs::canonicalize_maybe_missing;
use crate::core::domain;
use crate::core::domain::path::{clean_path, path_strip_prefix, validate_paths_in_roots};
use crate::core::domain::root::{find_containing_root, ParkedPath, ParkedRoot, Root, RootSpec};
use crate::core::domain::scope::{
    DecisionScope, PrefixOutcome, ScopeGrain, ScopeMatch, ScopeResolution,
};
use crate::core::repo::{self, Connection};

/// Classify a canonicalized path as file or directory scope.
///
/// Asks the filesystem whether the path names a file; anything else — a
/// directory, or a path that no longer exists — is treated as a prefix. The
/// look at the disk is why this composes here rather than on `ScopeMatch`
/// itself, which stays free of I/O.
pub fn classify(path: &str) -> ScopeMatch {
    if Path::new(path).is_file() {
        ScopeMatch::ExactFile(path.to_string())
    } else {
        ScopeMatch::UnderDirectory(path.to_string())
    }
}

/// Classify multiple canonicalized paths.
pub fn classify_all(paths: &[String]) -> Vec<ScopeMatch> {
    paths.iter().map(|p| classify(p)).collect()
}

/// Classify canonicalized paths for a command that acts on what it matches,
/// asking the index whatever the disk cannot answer.
///
/// [`classify`] reads the disk, and a path whose storage is detached answers
/// neither "file" nor "directory" — it falls to the prefix arm. On a reporting
/// command that only widens a view; on an effectful one it widens the act: an
/// exclusion aimed at one file the drive no longer offers would take every row
/// recorded beneath that path with it. Dismissing content needs no mount (the
/// judgment must outlive the drive it clears), so the command must mean the
/// same thing attached or detached. When the disk is silent, the index
/// answers: a source standing at exactly this path makes it a file, and
/// anything else stays a prefix.
pub fn classify_all_indexed(conn: &Connection, paths: &[String]) -> Result<Vec<ScopeMatch>> {
    let roots = repo::root::fetch_all(conn)?;
    paths
        .iter()
        .map(|path| {
            let p = Path::new(path);
            if p.is_file() {
                return Ok(ScopeMatch::ExactFile(path.clone()));
            }
            if !p.is_dir() && indexed_as_file(conn, path, &roots)? {
                return Ok(ScopeMatch::ExactFile(path.clone()));
            }
            Ok(ScopeMatch::UnderDirectory(path.clone()))
        })
        .collect()
}

fn indexed_as_file(conn: &Connection, path: &str, roots: &[Root]) -> Result<bool> {
    match find_containing_root(path, roots) {
        Some((root_id, _, _, rel)) if !rel.is_empty() => {
            repo::source::source_exists_at_path(conn, root_id, &rel)
        }
        _ => Ok(false),
    }
}

/// Result of resolving scope for a discovery command.
#[derive(Debug)]
pub struct ResolvedScope {
    /// Resolved scope path strings (empty = global), in the byte-form the
    /// index stores.
    pub prefixes: Vec<String>,
    /// Paths that were asked for, sit under a known root, and have no known
    /// sources — set aside rather than run. Never empty without
    /// [`prefixes`](Self::prefixes) also being non-empty: a scope that kept
    /// nothing is an error, not a silent narrowing. Every command states
    /// these on the channel it states its scope on; nothing acts on them,
    /// and they never become a recorded decision scope.
    pub set_aside: Vec<String>,
    /// Paths that were asked for and stand on a root the user closed the
    /// door on — the boundary's third set-aside cause, stated in the same
    /// position as the second. Same contract: nothing acts on them, and they
    /// never become a recorded decision scope. Non-empty only beside kept
    /// prefixes; a scope where the door took everything is a
    /// [`Door::Closed`], not a narrowing.
    pub parked: Vec<ParkedPath>,
    /// The closed doors this reading stands behind — set **only** by a
    /// remembering caller that turned a [`Door::Closed`] into a scope it
    /// reads at, and never by the boundary itself. One entry per parked
    /// root, in the order they were asked about: a scope may name places on
    /// several closed roots, and stating the first would leave the rest read
    /// behind a door nobody mentioned. Remembering is a permit:
    /// a pause of attention does not make Canon forget, so the trail, the
    /// notes and the story read at a parked place — with the door stated
    /// once, in the header. Distinct from [`parked`](Self::parked), which is
    /// what a view set *aside*: the two are opposite dispositions of the same
    /// fact and must not share a register.
    pub pause: Vec<ParkedRoot>,
    /// Whether the scope came from CWD defaulting (controls relative path display).
    pub from_cwd: bool,
    /// Whether to auto-include archived sources (scope is inside an archive root).
    pub auto_include_archived: bool,
}

impl ResolvedScope {
    /// True when operating globally (no scope restriction).
    pub fn is_global(&self) -> bool {
        self.prefixes.is_empty()
    }
}

/// What the scope boundary found: something to work on, or a closed door.
///
/// **The fact that a place is parked is derived once, here, as data** — never
/// re-detected from `current_dir()` or from an `is_active()` filter applied to
/// a root a caller was already handed. Checking the door as a filter partway
/// down the pipeline removes the parked place from the answer, and a removed
/// thing cannot be stated: that is what made a closed door read as a false
/// empty, a silent widening, or a global act from a place the user had shut.
///
/// It is an enum rather than a flag on [`ResolvedScope`] because a flag can be
/// ignored while [`ResolvedScope::is_global`] keeps saying "all roots" about a
/// place behind a door. Here `is_global()` is unreachable from a closed door
/// by construction: the type, not the caller's diligence, is what stops
/// "global" from being said about a parked place.
///
/// Every consumer names both arms — the permit class it declares (a view sets
/// aside, a remembering view reads, an act refuses) is a decision, and a `_`
/// arm is that decision going unmade.
#[derive(Debug)]
pub enum Door {
    /// Something was kept. `prefixes` empty means global. The two set-aside
    /// registers beside it — [`set_aside`](ResolvedScope::set_aside), the
    /// sourceless, and [`parked`](ResolvedScope::parked), the closed — are
    /// the caller's to state, in the set-aside position.
    Open(ResolvedScope),
    /// Every asked-for place — or the place being stood in — is behind a
    /// closed door. Nothing was kept: a present-tense view has nothing to
    /// list and says so, a remembering view reads at
    /// [`places`](ClosedDoor::places), an act refuses.
    Closed(ClosedDoor),
}

/// A scope that kept nothing because the door was closed on all of it.
#[derive(Debug)]
pub struct ClosedDoor {
    /// The parked places, in the order they were asked for. Never empty.
    pub places: Vec<ParkedPath>,
    /// Paths in the same ask that kept nothing for the *other* reason — no
    /// sources known there. Carried rather than collapsed: a scope refused
    /// for two causes states both, or one of them is spoken with the wrong
    /// reason.
    pub sourceless: Vec<String>,
    /// The CWD door: [`places`](Self::places) is the one directory being
    /// stood in, and the statement says `here` rather than a path.
    pub here: bool,
    /// Whether a remembering view reading at these places should widen to
    /// archived sources — the archive-CWD auto-enable, which a closed door
    /// does not switch off.
    pub auto_include_archived: bool,
}

// ============================================================================
// Path & Root Resolution (soft-match against known roots, fs fallback)
// ============================================================================

/// Resolve a single path against known roots, falling back to fs::canonicalize().
/// Use for source-querying commands. File-accessing commands (scan) use
/// fs::canonicalize directly.
pub fn resolve_path(path: &Path, roots: &[Root], cwd: &Path) -> Result<String> {
    if let Some(matched) = domain::path::resolve_path(path, roots, cwd) {
        return Ok(matched);
    }

    // Fall back to fs::canonicalize (requires path to exist on disk)
    match std::fs::canonicalize(path) {
        Ok(canonical) => Ok(canonical.to_string_lossy().into_owned()),
        Err(_) => bail!(
            "Failed to resolve path: {}\n\
             Path does not match any known root and is not accessible \
             on disk (is the storage attached?)",
            path.display()
        ),
    }
}

/// The first relative path among the inputs, if any — the only case that
/// needs a current directory at all. Pure.
pub fn needs_cwd<'a>(paths: &[&'a Path]) -> Option<&'a Path> {
    paths.iter().copied().find(|p| p.is_relative())
}

/// A current directory to resolve these paths against.
///
/// A working directory can be deleted out from under a running process, and
/// asking for one that no longer exists fails. That failure is only fatal
/// when it is actually needed: an absolute path is resolved without ever
/// reading the CWD, so `canon scan /some/absolute/path` must not depend on
/// where it was launched from. When every input is absolute the placeholder
/// is never read — [`clean_path`] ignores its `cwd` argument for absolute
/// inputs. When a relative path is present, the error names it.
pub fn cwd_for(paths: &[&Path]) -> Result<PathBuf> {
    match std::env::current_dir() {
        Ok(cwd) => Ok(cwd),
        Err(_) => match needs_cwd(paths) {
            None => Ok(PathBuf::from("/")),
            Some(relative) => bail!(
                "cannot resolve relative path '{}': the current directory is unavailable",
                relative.display()
            ),
        },
    }
}

/// Resolve multiple paths against known roots.
pub fn resolve_paths(paths: &[PathBuf], roots: &[Root]) -> Result<Vec<String>> {
    let refs: Vec<&Path> = paths.iter().map(|p| p.as_path()).collect();
    let cwd = cwd_for(&refs)?;
    domain::path::resolve_paths(paths, roots, &cwd)
        .into_iter()
        .zip(paths.iter())
        .map(|(matched, path)| match matched {
            Some(s) => Ok(s),
            None => resolve_path(path, roots, &cwd),
        })
        .collect()
}

/// What the root-spec door found: the root, or the door closed on it.
///
/// **An absence states what it observes, never a cause** — and the third
/// answer this used to give was neither. Filtering suspended roots out of the
/// candidate list made a root that plainly exists come back as
/// "No root for path", so every caller spoke a false cause about a place the
/// user had merely closed. The three answers are now distinct: found, closed,
/// and genuinely unknown — the last still an `Err`, because a spec naming
/// nothing is a mistake rather than a permission question.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RootLookup {
    /// A root, its door open.
    Found(i64),
    /// The root exists and the user closed its door.
    Parked(ParkedRoot),
}

/// Parse root spec (id:N or path:/path) with optional role validation.
///
/// Callers must fetch roots via `core::repo::root::fetch_all()` first, and
/// name both arms of the answer: what a closed door means is the caller's
/// permit class, not the parser's.
pub fn parse_root_spec(
    roots: &[Root],
    spec: &str,
    required_role: Option<&str>,
) -> Result<RootLookup> {
    let (root, role) = find_root_by_spec(roots, spec)?;

    // Validate role (domain logic). Ahead of the door: naming a source root
    // where an archive was asked for is a mistake about *which* root, which
    // stands whether or not that root's door happens to be closed.
    if let Some(req_role) = required_role {
        if role != req_role {
            bail!("Root {} has role '{role}', expected '{req_role}'", root.id);
        }
    }
    Ok(match root.parked() {
        Some(parked) => RootLookup::Parked(parked),
        None => RootLookup::Found(root.id),
    })
}

/// Parse root spec, taking the id whatever the door's state.
///
/// For the callers that **open** the door (`roots suspend`/`unsuspend`) and
/// for remembering (`roots story`, the retirement review as a reading) —
/// the two permits a closed door grants, where a `RootLookup` would only be
/// unwrapped again at the call site.
///
/// Callers must fetch roots via `core::repo::root::fetch_all()` first.
pub fn parse_root_spec_any(roots: &[Root], spec: &str) -> Result<i64> {
    Ok(find_root_by_spec(roots, spec)?.0.id)
}

/// The root a spec names, whatever its door — the one lookup both doors above
/// run, so "unknown" means the same thing at each.
fn find_root_by_spec<'a>(roots: &'a [Root], spec: &str) -> Result<(&'a Root, String)> {
    let parsed = RootSpec::parse(spec)?;
    let root = match parsed {
        RootSpec::ById(id) => roots
            .iter()
            .find(|r| r.id == id)
            .ok_or_else(|| anyhow::anyhow!("No root with id {id}"))?,
        RootSpec::ByPath(ref path) => {
            let cwd = cwd_for(&[Path::new(path)])?;
            let canonical = resolve_path(Path::new(path), roots, &cwd)?;
            roots
                .iter()
                .find(|r| r.path == canonical)
                .ok_or_else(|| anyhow::anyhow!("No root for path: {path}"))?
        }
    };
    let role = root.role.clone();
    Ok((root, role))
}

/// Resolve a path to its containing root (any role) and relative subdir,
/// **including** roots whose door is closed.
///
/// There is one lookup here, deliberately. The active-only sibling this
/// module used to carry answered "not inside a root" for a path one root
/// plainly contains — and every consumer of that answer then said something
/// false about it: the scope boundary went global, `roots rm` said "no root
/// for path". A door is a fact to be *stated*, so resolution always finds the
/// root and the caller asks [`Root::parked`] what to do about it.
///
/// Callers must fetch roots via `core::repo::root::fetch_all()` first.
pub fn resolve_root_path_any(
    roots: &[Root],
    path: &Path,
) -> Result<Option<(i64, String, String, String)>> {
    let cwd = cwd_for(&[path])?;
    // Resolve against ALL roots for path recognition (soft resolution)
    let path_str = resolve_path(path, roots, &cwd)?;
    // Find containing root (pure domain logic)
    Ok(find_containing_root(&path_str, roots))
}

/// Resolve a path to its containing archive root and relative subdir.
/// Unlike parse_root_spec which requires exact root match, this accepts any path
/// inside an archive root and extracts the relative portion.
/// The path does not need to exist - only an ancestor within an archive root must exist.
///
/// Callers must fetch roots via `core::repo::root::fetch_all()` first.
///
/// Returns (root_id, root_path, relative_subdir) or error if not in an archive.
pub fn resolve_archive_path(roots: &[Root], path: &Path) -> Result<(i64, String, String)> {
    // Soft resolution: try matching against known roots first (works offline),
    // then fall back to canonicalize_maybe_missing (tolerates non-existent subdirs)
    let cwd = cwd_for(&[path])?;
    let cleaned = clean_path(path, &cwd);
    let cleaned_str = cleaned.to_string_lossy();

    let path_str = if find_containing_root(&cleaned_str, roots).is_some() {
        cleaned_str.into_owned()
    } else {
        // Fallback: canonicalize (tolerating non-existent subdirectories)
        canonicalize_maybe_missing(path)?
    };

    // **All roots, not the active ones.** Filtering here answered
    // "not inside any registered archive root" about an archive root that
    // plainly is registered — the same false cause the root-spec door
    // retired, one door over. The door is a fact to be stated, so the root is
    // found and its standing is asked about below.
    for root in roots {
        let rel = if path_str == root.path {
            Some(String::new())
        } else {
            path_strip_prefix(&path_str, &root.path).map(str::to_string)
        };
        let Some(rel) = rel else { continue };
        if !root.is_archive() {
            bail!(
                "Path '{}' is inside a {} root, not an archive",
                path.display(),
                root.role
            );
        }
        // Role first, then the door: naming a source root where an archive
        // was asked for is a mistake about *which* root, and it stands
        // whether or not that root's door happens to be closed. Writing into
        // an archive is an act, so a closed one is refused by name.
        if let Some(parked) = root.parked() {
            return Err(crate::core::domain::root::DoorRefused::at(
                &parked,
                crate::core::domain::root::DoorVerb::Refused,
            )
            .into());
        }
        return Ok((root.id, root.path.clone(), rel));
    }

    bail!(
        "Path '{}' is not inside any registered archive root",
        path.display()
    )
}

/// Resolve scope for a discovery command.
///
/// Resolution order:
/// 1. Explicit paths given → resolve, validate they're under known roots, partition
/// 2. `--global` flag → global (empty prefixes)
/// 3. No paths, no `--global` → try CWD:
///    - CWD inside a known root, open → scope to CWD
///    - CWD inside a known root, closed → [`Door::Closed`] naming that root
///    - CWD not inside any root → global fallback (silent)
///    - `current_dir()` fails → global fallback (silent)
///
/// When CWD or explicit path is inside an archive root, sets `auto_include_archived`.
///
/// Reads the process's current directory; [`resolve_scope_at`] is the same
/// resolution with the directory supplied, which is what makes the CWD arm
/// testable at all.
pub fn resolve_scope(
    conn: &Connection,
    explicit_paths: &[PathBuf],
    global: bool,
    roots: &[Root],
) -> Result<Door> {
    resolve_scope_at(
        conn,
        explicit_paths,
        global,
        roots,
        std::env::current_dir().ok().as_deref(),
    )
}

/// [`resolve_scope`] with the current directory supplied rather than read.
///
/// The CWD arm is where the door was silently widening seven commands to the
/// whole universe, and it went unpinned for exactly one reason: a test cannot
/// change the process's directory without changing every other test's. `None`
/// is the arm where `current_dir()` itself failed — global, silently, as
/// before.
pub fn resolve_scope_at(
    conn: &Connection,
    explicit_paths: &[PathBuf],
    global: bool,
    roots: &[Root],
    cwd: Option<&Path>,
) -> Result<Door> {
    // Case 1: Explicit paths given
    if !explicit_paths.is_empty() {
        let prefixes = resolve_paths(explicit_paths, roots)?;
        validate_paths_in_roots(&prefixes, roots)?;
        let partition = apply_source_existence_policy(conn, prefixes, roots)?;
        let ScopePartition {
            kept,
            set_aside,
            parked,
        } = partition;
        if kept.is_empty() {
            // Terminal, and the door is why: state every parked place, and
            // beside it every path that kept nothing for the other reason.
            return Ok(Door::Closed(ClosedDoor {
                places: parked,
                sourceless: set_aside,
                here: false,
                auto_include_archived: false,
            }));
        }
        // One derivation of "is any of this inside an archive root": the
        // same role partition survey reads for its frame statement.
        let auto_include_archived = !domain::root::partition_prefixes_by_role(&kept, roots)
            .archive_side
            .is_empty();
        return Ok(Door::Open(ResolvedScope {
            prefixes: kept,
            set_aside,
            parked,
            pause: Vec::new(),
            from_cwd: false,
            auto_include_archived,
        }));
    }

    // Case 2: --global flag
    if global {
        return Ok(Door::Open(global_scope()));
    }

    // Case 3: No paths, no --global — try CWD
    let Some(cwd) = cwd else {
        return Ok(Door::Open(global_scope()));
    };

    // Resolve against **all** roots: an active-only lookup here is what made
    // standing inside a closed root read as standing nowhere, and "nowhere"
    // falls through to global. Standing somewhere is naming it.
    match resolve_root_path_any(roots, cwd)? {
        Some((root_id, _, role, _)) => {
            let root = roots
                .iter()
                .find(|r| r.id == root_id)
                .ok_or_else(|| anyhow::anyhow!("root {root_id} vanished during resolution"))?;
            let prefixes = resolve_paths(&[cwd.to_path_buf()], roots)?;
            match root.parked() {
                Some(parked_root) => Ok(Door::Closed(ClosedDoor {
                    places: prefixes
                        .into_iter()
                        .map(|path| ParkedPath {
                            path,
                            root: parked_root.clone(),
                        })
                        .collect(),
                    sourceless: Vec::new(),
                    here: true,
                    auto_include_archived: role == "archive",
                })),
                None => Ok(Door::Open(ResolvedScope {
                    prefixes,
                    set_aside: Vec::new(),
                    parked: Vec::new(),
                    pause: Vec::new(),
                    from_cwd: true,
                    auto_include_archived: role == "archive",
                })),
            }
        }
        None => Ok(Door::Open(global_scope())),
    }
}

impl ClosedDoor {
    /// This closed door as a scope a **remembering** view reads at.
    ///
    /// Remembering is one of the four permits: knowledge Canon already holds
    /// still reads behind a closed door. The places become the scope, the
    /// door becomes the pause the header states once, and the sourceless
    /// companions stay set aside for the caller to state — the same two
    /// dispositions the open arm carries, only with the parked places on the
    /// reading side of the line.
    ///
    /// Spoken here rather than in each remembering caller, so the trail and
    /// the notes cannot come to disagree about what reading behind a door
    /// means.
    pub fn read_here(self) -> ResolvedScope {
        let mut pause: Vec<ParkedRoot> = Vec::new();
        for place in &self.places {
            if !pause.iter().any(|r| r.root_id == place.root.root_id) {
                pause.push(place.root.clone());
            }
        }
        ResolvedScope {
            prefixes: self.places.into_iter().map(|p| p.path).collect(),
            set_aside: self.sourceless,
            parked: Vec::new(),
            pause,
            from_cwd: self.here,
            auto_include_archived: self.auto_include_archived,
        }
    }
}

/// The unbounded scope: everything, asked for on purpose or fallen back to.
fn global_scope() -> ResolvedScope {
    ResolvedScope {
        prefixes: Vec::new(),
        set_aside: Vec::new(),
        parked: Vec::new(),
        pause: Vec::new(),
        from_cwd: false,
        auto_include_archived: false,
    }
}

/// History-tense resolution: an explicit path under a live root resolves
/// even when no sources stand there anymore. The source-existence gate is
/// right for present-tense commands ("no sources known" is `ls`'s honest
/// answer) — but the trail's subject is what *happened*, and a place fully
/// moved into the archive still holds its history (extraction rows, notes).
/// Returns `None` when the paths don't resolve under live roots at all —
/// the caller propagates its original error untouched.
pub fn resolve_history_scope(explicit_paths: &[PathBuf], roots: &[Root]) -> Option<ResolvedScope> {
    if explicit_paths.is_empty() {
        return None;
    }
    let prefixes = resolve_paths(explicit_paths, roots).ok()?;
    validate_paths_in_roots(&prefixes, roots).ok()?;
    let auto_include_archived = !domain::root::partition_prefixes_by_role(&prefixes, roots)
        .archive_side
        .is_empty();
    Some(ResolvedScope {
        prefixes,
        set_aside: Vec::new(),
        parked: Vec::new(),
        pause: Vec::new(),
        from_cwd: false,
        auto_include_archived,
    })
}

/// The index's answer to "which byte-form of this below-root remainder does
/// Canon know?" — the form-tolerance rule's database-asking half, spoken once.
///
/// The path's root has already been matched (root containment is
/// form-tolerant in its own right, at both doors); only the relative remainder
/// is retried here, so a root and the content beneath it may each have been
/// stored in whichever form their disk handed over. Returns the candidate the
/// index knows sources under — the stored bytes, which is what every
/// downstream comparison (Rust prefix matching, the SQL boundary spellings)
/// must see. `None` means no form of this remainder has sources: it is
/// genuinely sourceless, and only then does policy see it.
///
/// Root-level remainders (empty) are always known — a root is valid whether or
/// not anything has been scanned into it yet.
///
/// **The as-given-first ordering is load-bearing and must not be tidied.** On
/// a normalization-*sensitive* filesystem two spellings can be two genuinely
/// different directories, and trying the path as written before any bend is
/// what makes each of them resolve to itself.
///
/// Both doors ask through this: the argument door via
/// [`stored_form_with_sources`], the manifest door via
/// [`resolve_recorded_scope`]. Two spellings of one question is how the two
/// doors drifted apart in the first place.
fn stored_form_of_rel(conn: &Connection, root_id: i64, rel: &str) -> Result<Option<String>> {
    if rel.is_empty() {
        return Ok(Some(String::new()));
    }
    for candidate in domain::path::normalization_candidates(rel) {
        if repo::source::sources_exist_at_scope(conn, root_id, &candidate)? {
            return Ok(Some(candidate));
        }
    }
    Ok(None)
}

/// The index's answer to "which byte-form of this path does Canon know?" at
/// the **argument** door: find the root, ask [`stored_form_of_rel`], rejoin.
///
/// `None` means no form of this path has sources; a path under no known root
/// comes back as given, because root membership is validated separately and
/// this gate has nothing to say about it.
fn stored_form_with_sources(
    conn: &Connection,
    path: &str,
    roots: &[Root],
) -> Result<Option<String>> {
    let Some((root_id, root_path, _role, rel_path)) =
        domain::root::find_containing_root(path, roots)
    else {
        return Ok(Some(path.to_string()));
    };
    if rel_path.is_empty() {
        return Ok(Some(path.to_string()));
    }
    Ok(
        stored_form_of_rel(conn, root_id, &rel_path)?.map(|candidate| {
            Path::new(&root_path)
                .join(&candidate)
                .to_string_lossy()
                .into_owned()
        }),
    )
}

/// Resolve a **recorded** (manifest) scope: attribute each prefix to a root,
/// then heal its remainder to the byte-form the index stores, then partition
/// by the source-existence policy.
///
/// The form-tolerance rule's fourth integration point, and the second stage no
/// pure resolution can run: healing needs an answer only the index has, and
/// `ScopeResolution` has no `Connection`. That is why this is the type's only
/// source of outcomes in production.
///
/// **The order is not interchangeable.** Root attribution comes first, over
/// whole-prefix candidates, because a prefix whose *root* portion is written
/// in the other form must match its root before its remainder can be asked
/// about at all. Only then is the remainder retried.
///
/// **Classification still cannot fail.** The `Result` carries *infrastructure*
/// failure — a SQL error — and nothing else; every failure mode of the scope
/// itself remains a caller's disposition, exactly as the pure resolution
/// promises. A prefix the index cannot confirm comes back set aside, not as an
/// error.
pub fn resolve_recorded_scope(
    conn: &Connection,
    prefixes: &[String],
    roots: &[Root],
) -> Result<ScopeResolution> {
    let mut outcomes = Vec::with_capacity(prefixes.len());
    for prefix in prefixes {
        outcomes.push(match domain::scope::attribute_prefix(prefix, roots) {
            Some(scope) => match stored_form_of_rel(conn, scope.root_id, &scope.rel_prefix)? {
                Some(rel) => {
                    let grain = scope_grain(conn, scope.root_id, &rel)?;
                    PrefixOutcome::Confirmed(
                        DecisionScope::new(scope.root_id, scope.root_path, rel),
                        grain,
                    )
                }
                None => PrefixOutcome::SetAside(scope),
            },
            None => PrefixOutcome::Unrooted(prefix.clone()),
        });
    }
    Ok(ScopeResolution::from_outcomes(outcomes))
}

/// What a confirmed prefix names — asked on the **confirmed byte-form**, never
/// on a losing normalization candidate.
///
/// Asked of every confirmed prefix, uniformly: a conditional here would be a
/// second rule about which prefixes have a grain, and there is only one.
///
/// **One question, and it admits no tie**: does a *present* source stand at
/// this path? That is what makes it an item; anything else is a place. Asking
/// about the path itself rather than about what lies below it is what closes
/// the shape where both are true at once — a path can hold a row at it *and*
/// rows beneath it whenever it has a past, and no rule that consults "below"
/// can answer such a path without choosing.
///
/// **This is the structural guarantee, not merely a better answer**, and the
/// fold is the step that carries it: the vantage is a *common prefix* of every
/// measuring point in a root, not any one of them, so the single-scope reading
/// does not prove it. For an entry at `E`: entries are present sources, so one
/// stands at `E`; the scope `S` that selected it satisfies `E ⊑ S` (selection
/// is at-or-under); `S`'s measuring point `P` is `S` itself when `Directory`
/// and `parent(S)` when `Item`, so `S ⊑ P`; and `P ⊑ V` because a common
/// prefix sits at or above everything it folds. `E = V` would force every link
/// to equality — `S` a `Directory` with a present source standing at it, which
/// is what `Directory` denies. So `V` is strictly above `E` and
/// `path_strip_prefix` cannot return `""`. Several scopes only push `V` up,
/// which is the safe direction.
///
/// **Presence, because the question is about now.** The confirmation gate above
/// is history-inclusive on purpose — a manifest naming a place whose files have
/// moved out is confirmed, not set aside — so history cannot answer this: a
/// file that has become a directory leaves a row standing at the path, and
/// reading it as an item would push every file below it down a level.
///
/// **Index evidence, never the disk.** A presence bit is a scan-time snapshot,
/// not a live `stat`, so a manifest measures the same whether or not the drive
/// happens to be mounted — which a `stat` here would not.
fn scope_grain(conn: &Connection, root_id: i64, rel: &str) -> Result<ScopeGrain> {
    // A root is a directory, and knowing that needs no index — true of a root
    // with nothing scanned into it, which the index could only call an item.
    if rel.is_empty() {
        return Ok(ScopeGrain::Directory);
    }
    Ok(
        if repo::source::present_source_exists_at_path(conn, root_id, rel)? {
            ScopeGrain::Item
        } else {
            ScopeGrain::Directory
        },
    )
}

/// The one spelling of the refusal a scope that kept nothing gets, at either
/// door.
///
/// One sentence, because it is one rule: a scope that kept nothing must never
/// look like a narrowing, so it is refused naming every path it could not
/// keep. The argument door raises it from [`apply_source_existence_policy`];
/// the manifest door raises it from `cluster refresh`, which is where that
/// door's dispositions live and which adds its own way back beneath it.
pub fn no_sources_known(paths: &[String]) -> String {
    format!("no sources known at {}", paths.join(", "))
}

/// How a scope's asked-for paths came out of the boundary's two gates.
#[derive(Debug)]
pub struct ScopePartition {
    /// Paths with known sources, in the byte-form the index stores.
    pub kept: Vec<String>,
    /// Paths under a known root with no known sources, in any form.
    pub set_aside: Vec<String>,
    /// Paths standing on a root whose door the user closed.
    pub parked: Vec<ParkedPath>,
}

/// The source-existence policy at the scope boundary, spoken once.
///
/// What was asked and what runs is never a silent difference. A path under a
/// known root that Canon has no sources for cannot be worked on — but among
/// several paths, one empty folder is a reason to say so, not a reason to
/// refuse the whole invocation. The rules:
///
/// - root-level paths (empty remainder) are always kept — a root is valid
///   whether or not anything has been scanned into it yet;
/// - a single path with no sources is an error, unchanged: with nothing else
///   asked for there is no work left to do, and "scan first" is the honest
///   answer;
/// - several paths with at least one keeper proceed, and the rest come back
///   as set-asides for the caller to state;
/// - several paths with no keeper at all is an error naming every one of
///   them — a scope that kept nothing must never look like a narrowing.
///
/// Paths not under any known root are a separate, harder failure and are
/// rejected before this is reached.
///
/// **The door is asked about first**, and the order is the whole point: a
/// parked path is stated as parked whether or not it has sources, because the
/// closed door is *why* it has nothing to say, and "no sources known" at a
/// place the user shut is a second, false cause. The root-level exemption
/// above belongs to the source gate alone — it says a root with nothing
/// scanned into it is still a valid place to ask about, which is a claim
/// about sources and not about doors, so a root-level path on a closed root
/// is parked like any other.
///
/// A scope that kept nothing is still terminal, and the caller decides which
/// terminal: with any parked place it is a closed door (the caller's
/// [`Door::Closed`]); with none, the sourceless refusal, unchanged.
fn apply_source_existence_policy(
    conn: &Connection,
    paths: Vec<String>,
    roots: &[Root],
) -> Result<ScopePartition> {
    let single = paths.len() == 1;
    let mut kept = Vec::new();
    let mut set_aside = Vec::new();
    let mut parked = Vec::new();
    for path in paths {
        if let Some(parked_root) = parked_root_of(&path, roots) {
            parked.push(ParkedPath {
                path,
                root: parked_root,
            });
            continue;
        }
        match stored_form_with_sources(conn, &path, roots)? {
            Some(stored) => kept.push(stored),
            None if single => bail!("{}", no_sources_known(&[path])),
            None => set_aside.push(path),
        }
    }
    if kept.is_empty() && parked.is_empty() {
        bail!("{}", no_sources_known(&set_aside));
    }
    Ok(ScopePartition {
        kept,
        set_aside,
        parked,
    })
}

/// The closed root this path stands on, if any — the one derivation of "is
/// this place behind a door", asked of the whole root list because an
/// active-only lookup cannot answer it.
pub fn parked_root_of(path: &str, roots: &[Root]) -> Option<ParkedRoot> {
    let (root_id, _, _, _) = find_containing_root(path, roots)?;
    roots.iter().find(|r| r.id == root_id)?.parked()
}

/// Refuse a location the question rests on when it stands behind a closed
/// door.
///
/// **The companion of [`validate_sources_exist`], covering its callers and
/// two more.** Those callers are the carve-outs from the boundary's
/// proceed-and-state policy: a location so load-bearing that setting it aside
/// would change the question rather than narrow it — `compare`'s two sides,
/// `exclude duplicates`' scope and prefer paths, `survey --other`'s
/// reference. A closed door there is exactly as load-bearing as an absent
/// one, so the whole ask is refused, naming the door and the way back rather
/// than answering about a smaller world.
///
/// The two extra callers are `exclude`'s single-file arms (`set_by_path`,
/// `set_object_by_file`), which resolve their own path and never reach the
/// boundary's partition at all. They are **not** carve-outs — nothing about
/// them is a comparand — but they are single-target acts, and a single-target
/// act behind a door is refused for the plainer reason that the closed
/// default refuses every act. Two roads to one verb, which is why the verb is
/// not a parameter.
///
/// Asked **before** the existence gate: a parked path is stated as parked
/// whether or not it has sources, the door being the reason.
pub fn refuse_parked_locations(paths: &[String], roots: &[Root]) -> Result<()> {
    for path in paths {
        if let Some(parked) = parked_root_of(path, roots) {
            return Err(crate::core::domain::root::DoorRefused::new(
                &parked,
                crate::core::domain::root::DoorVerb::Refused,
                path,
            )
            .into());
        }
    }
    Ok(())
}

/// Validate that sources exist at each scope path, returning the paths in
/// the byte-form the index stores them under.
///
/// Errors on the first path with no known sources — the abort spelling of
/// the source-existence gate. Its callers are exactly the carve-outs from
/// the boundary's proceed-and-state policy: locations that are load-bearing
/// to the question being asked, where proceeding without one would change
/// the question rather than narrow it (`compare`'s two sides,
/// `exclude duplicates`' scope and prefer paths, `survey --other`'s
/// reference location). Every other scope-taking command goes through
/// [`resolve_scope`], which sets a sourceless path aside and says so.
///
/// Skips root-level paths (empty rel_path) — roots are always valid.
/// Assumes paths are already validated as under known roots.
pub fn validate_sources_exist(
    conn: &Connection,
    paths: &[String],
    roots: &[Root],
) -> Result<Vec<String>> {
    paths
        .iter()
        .map(|path| match stored_form_with_sources(conn, path, roots)? {
            Some(stored) => Ok(stored),
            None => bail!("no sources known at {path}"),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::testing::{insert_root, insert_source, setup_test_db};

    /// The open arm, for a test whose subject is the resolution and not the
    /// door. A closed door here is a test failure that names itself.
    fn opened(door: Door) -> ResolvedScope {
        match door {
            Door::Open(resolved) => resolved,
            Door::Closed(closed) => panic!("expected an open door, got {closed:?}"),
        }
    }

    /// The closed arm, for a test whose subject *is* the door.
    fn closed(door: Door) -> ClosedDoor {
        match door {
            Door::Closed(closed) => closed,
            Door::Open(resolved) => panic!("expected a closed door, got {resolved:?}"),
        }
    }

    fn make_test_root(id: i64, path: &str, role: &str) -> Root {
        Root {
            id,
            path: path.to_string(),
            role: role.to_string(),
            comment: None,
            last_scanned_at: None,
            suspended: false,
        }
    }

    /// The detached-drive guard: an act means the same thing whether or not
    /// the storage is mounted. Neither path here exists on disk, so the disk
    /// answers nothing and the index has to.
    #[test]
    fn a_path_the_disk_cannot_answer_for_is_classified_by_the_index() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/mnt/drive", "source", false);
        insert_source(&conn, root_id, "photos/IMG_1.jpg", None);
        insert_source(&conn, root_id, "photos/album/IMG_2.jpg", None);

        let scopes = classify_all_indexed(
            &conn,
            &[
                "/mnt/drive/photos/IMG_1.jpg".to_string(),
                "/mnt/drive/photos".to_string(),
            ],
        )
        .unwrap();

        assert_eq!(
            scopes,
            vec![
                ScopeMatch::ExactFile("/mnt/drive/photos/IMG_1.jpg".to_string()),
                ScopeMatch::UnderDirectory("/mnt/drive/photos".to_string()),
            ]
        );
        // What the disk-only classification would have said about the file —
        // a prefix, sweeping in whatever the index records beneath it.
        assert_eq!(
            classify("/mnt/drive/photos/IMG_1.jpg"),
            ScopeMatch::UnderDirectory("/mnt/drive/photos/IMG_1.jpg".to_string())
        );
    }

    /// A path the index has never heard of stays a prefix: the classification
    /// narrows on evidence, it does not invent a file.
    #[test]
    fn an_unknown_path_the_disk_cannot_answer_for_stays_a_prefix() {
        let conn = setup_test_db();
        insert_root(&conn, "/mnt/drive", "source", false);

        let scopes =
            classify_all_indexed(&conn, &["/mnt/drive/photos/gone.jpg".to_string()]).unwrap();
        assert_eq!(
            scopes,
            vec![ScopeMatch::UnderDirectory(
                "/mnt/drive/photos/gone.jpg".to_string()
            )]
        );
    }

    #[test]
    fn resolve_explicit_paths() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/a/b", "source", false);
        insert_source(&conn, root_id, "c/file.txt", None);
        let roots = vec![make_test_root(root_id, "/a/b", "source")];
        let result =
            opened(resolve_scope(&conn, &[PathBuf::from("/a/b/c")], false, &roots).unwrap());
        assert!(!result.is_global());
        assert!(!result.from_cwd);
        assert!(!result.auto_include_archived);
        assert_eq!(result.prefixes, vec!["/a/b/c".to_string()]);
    }

    #[test]
    fn resolve_explicit_path_in_archive_root() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/archive", "archive", false);
        insert_source(&conn, root_id, "photos/file.jpg", None);
        let roots = vec![make_test_root(root_id, "/archive", "archive")];
        let result = opened(
            resolve_scope(&conn, &[PathBuf::from("/archive/photos")], false, &roots).unwrap(),
        );
        assert!(result.auto_include_archived);
        assert!(!result.from_cwd);
    }

    #[test]
    fn resolve_explicit_path_not_in_root_errors() {
        let conn = setup_test_db();
        let roots = vec![make_test_root(1, "/a/b", "source")];
        let result = resolve_scope(&conn, &[PathBuf::from("/x/y/z")], false, &roots);
        assert!(result.is_err());
    }

    #[test]
    fn resolve_global_flag() {
        let conn = setup_test_db();
        let roots = vec![make_test_root(1, "/a/b", "source")];
        let result = opened(resolve_scope(&conn, &[], true, &roots).unwrap());
        assert!(result.is_global());
        assert!(!result.from_cwd);
        assert!(!result.auto_include_archived);
    }

    #[test]
    fn resolve_global_flag_with_explicit_paths() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/a/b", "source", false);
        insert_source(&conn, root_id, "c/file.txt", None);
        let roots = vec![make_test_root(root_id, "/a/b", "source")];
        let result =
            opened(resolve_scope(&conn, &[PathBuf::from("/a/b/c")], true, &roots).unwrap());
        assert!(!result.is_global());
        assert_eq!(result.prefixes, vec!["/a/b/c".to_string()]);
    }

    #[test]
    fn resolve_scope_errors_on_unknown_subpath() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, "2011/file.jpg", None);
        let roots = vec![make_test_root(root_id, "/photos", "source")];

        // Path under root but no sources there
        let result = resolve_scope(&conn, &[PathBuf::from("/photos/typo")], false, &roots);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("no sources known"), "error was: {err}");
    }

    #[test]
    fn resolve_scope_accepts_path_with_sources() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, "2011/file.jpg", None);
        let roots = vec![make_test_root(root_id, "/photos", "source")];

        let result =
            opened(resolve_scope(&conn, &[PathBuf::from("/photos/2011")], false, &roots).unwrap());
        assert_eq!(result.prefixes, vec!["/photos/2011".to_string()]);
    }

    // ========================================================================
    // Lazy CWD: a working directory is read only when one is needed
    // ========================================================================

    #[test]
    fn needs_cwd_names_the_first_relative_path() {
        let a = Path::new("/abs/one");
        let b = Path::new("rel/two");
        let c = Path::new("rel/three");
        assert_eq!(needs_cwd(&[a, b, c]), Some(b));
    }

    #[test]
    fn needs_cwd_is_none_when_every_path_is_absolute() {
        assert_eq!(needs_cwd(&[Path::new("/a"), Path::new("/b")]), None);
        assert_eq!(needs_cwd(&[]), None);
    }

    // ========================================================================
    // The scope-boundary honesty policy
    // ========================================================================

    /// A scope with two folders where one is empty is a scope with one
    /// folder's worth of work in it — and a line saying so.
    #[test]
    fn a_multi_path_scope_proceeds_past_a_sourceless_member() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, "2011/file.jpg", None);
        let roots = vec![make_test_root(root_id, "/photos", "source")];

        let resolved = opened(
            resolve_scope(
                &conn,
                &[PathBuf::from("/photos/2011"), PathBuf::from("/photos/2012")],
                false,
                &roots,
            )
            .unwrap(),
        );
        assert_eq!(resolved.prefixes, vec!["/photos/2011".to_string()]);
        assert_eq!(resolved.set_aside, vec!["/photos/2012".to_string()]);
    }

    /// The "scan first" contract: with nothing else asked for, there is no
    /// work left to narrow to.
    #[test]
    fn a_single_sourceless_path_still_errors() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, "2011/file.jpg", None);
        let roots = vec![make_test_root(root_id, "/photos", "source")];

        let result = resolve_scope(&conn, &[PathBuf::from("/photos/2012")], false, &roots);
        let err = result.unwrap_err().to_string();
        assert!(err.contains("no sources known at /photos/2012"), "{err}");
    }

    #[test]
    fn an_all_sourceless_multi_path_scope_errors_naming_every_path() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, "2011/file.jpg", None);
        let roots = vec![make_test_root(root_id, "/photos", "source")];

        let result = resolve_scope(
            &conn,
            &[PathBuf::from("/photos/2012"), PathBuf::from("/photos/2013")],
            false,
            &roots,
        );
        let err = result.unwrap_err().to_string();
        assert!(err.contains("/photos/2012"), "{err}");
        assert!(err.contains("/photos/2013"), "{err}");
    }

    /// Root membership is the harder failure and keeps its precedence: a path
    /// under no known root is refused whatever else was asked for — never set
    /// aside beside the keepers. The temp directory stands in for "a real
    /// place on disk that Canon has never been told about", so resolution
    /// succeeds and root membership is what does the refusing.
    #[test]
    fn a_non_root_path_errors_even_beside_valid_members() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, "2011/file.jpg", None);
        let roots = vec![make_test_root(root_id, "/photos", "source")];

        let outsider = std::env::temp_dir();
        let result = resolve_scope(
            &conn,
            &[PathBuf::from("/photos/2011"), outsider],
            false,
            &roots,
        );
        let err = result.unwrap_err().to_string();
        assert!(err.contains("not under any known root"), "{err}");
    }

    #[test]
    fn root_level_paths_are_always_kept() {
        let conn = setup_test_db();
        // Nothing has ever been scanned into this root.
        let root_id = insert_root(&conn, "/photos", "source", false);
        let other_id = insert_root(&conn, "/scans", "source", false);
        insert_source(&conn, other_id, "a.jpg", None);
        let roots = vec![
            make_test_root(root_id, "/photos", "source"),
            make_test_root(other_id, "/scans", "source"),
        ];

        let resolved = opened(
            resolve_scope(
                &conn,
                &[PathBuf::from("/photos"), PathBuf::from("/scans")],
                false,
                &roots,
            )
            .unwrap(),
        );
        assert_eq!(
            resolved.prefixes,
            vec!["/photos".to_string(), "/scans".to_string()]
        );
        assert!(resolved.set_aside.is_empty());
    }

    /// The record must never claim a place the invocation did not touch.
    /// Decomposition reads the kept prefixes; a set-aside is not among them.
    #[test]
    fn a_set_aside_never_becomes_a_decision_scope() {
        use crate::core::domain::scope::DecisionScope;

        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, "2011/file.jpg", None);
        let roots = vec![make_test_root(root_id, "/photos", "source")];

        let resolved = opened(
            resolve_scope(
                &conn,
                &[PathBuf::from("/photos/2011"), PathBuf::from("/photos/2012")],
                false,
                &roots,
            )
            .unwrap(),
        );
        assert_eq!(resolved.set_aside, vec!["/photos/2012".to_string()]);

        let scopes = DecisionScope::decompose(&resolved.prefixes, &roots);
        assert_eq!(
            scopes,
            vec![DecisionScope::new(
                root_id,
                "/photos".to_string(),
                "2011".to_string()
            )]
        );
        assert!(scopes.iter().all(|sc| sc.rel_prefix != "2012"));
    }

    /// **The archive door.** Writing into an archive is an act, so a
    /// destination inside a closed one is refused by name — never
    /// "not inside any registered archive root", which was the false cause
    /// this door kept after the root-spec door retired it.
    #[test]
    fn a_destination_inside_a_parked_archive_names_the_door() {
        let mut archive = make_test_root(1, "/archive", "archive");
        archive.suspended = true;
        let roots = vec![archive];

        let err = resolve_archive_path(&roots, Path::new("/archive/2024"))
            .unwrap_err()
            .to_string();
        assert_eq!(
            err,
            "/archive suspended — refused: /archive · canon roots unsuspend path:/archive"
        );
    }

    /// Role precedes the door: naming a source root where an archive was
    /// asked for is a mistake about *which* root, and it stands whether or
    /// not that root's door happens to be closed.
    #[test]
    fn a_parked_source_root_named_as_a_destination_is_still_the_wrong_role() {
        let mut source = make_test_root(1, "/photos", "source");
        source.suspended = true;
        let roots = vec![source];

        let err = resolve_archive_path(&roots, Path::new("/photos/2024"))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("is inside a source root, not an archive"),
            "{err}"
        );
    }

    // ========================================================================
    // The closed door
    //
    // The boundary's third set-aside cause. Every row of the partition's
    // precedence has a pin, and the CWD arm — which had none, because it read
    // the process's directory — is reachable through `resolve_scope_at`.
    // ========================================================================

    fn parked_root(id: i64, path: &str, role: &str) -> Root {
        let mut root = make_test_root(id, path, role);
        root.suspended = true;
        root
    }

    /// **The law's verifier.** Standing inside a root the user closed used to
    /// resolve to "not inside any root", which falls through to the whole
    /// universe: seven commands answered about everything while the user was
    /// standing in one place, and `exclude set --yes` there dismissed content
    /// on roots they had never named. The door is now the answer.
    #[test]
    fn a_parked_cwd_is_never_global() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", true);
        insert_source(&conn, root_id, "2011/file.jpg", None);
        let roots = vec![parked_root(root_id, "/photos", "source")];

        let door =
            resolve_scope_at(&conn, &[], false, &roots, Some(Path::new("/photos/2011"))).unwrap();

        let closed = closed(door);
        assert!(closed.here, "the CWD door states `here`, not a path");
        assert_eq!(closed.places.len(), 1);
        assert_eq!(closed.places[0].root.root_path, "/photos");
        assert_eq!(closed.places[0].path, "/photos/2011");
    }

    /// Standing in a place is naming it. The same directory must answer the
    /// same way through both doors — the trail already pins this for itself,
    /// and it now holds at the shared boundary.
    #[test]
    fn a_parked_cwd_answers_like_the_named_place() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", true);
        insert_source(&conn, root_id, "2011/file.jpg", None);
        let roots = vec![parked_root(root_id, "/photos", "source")];

        let by_standing = closed(
            resolve_scope_at(&conn, &[], false, &roots, Some(Path::new("/photos/2011"))).unwrap(),
        );
        let by_naming = closed(
            resolve_scope_at(&conn, &[PathBuf::from("/photos/2011")], false, &roots, None).unwrap(),
        );

        assert_eq!(
            by_standing.places[0].path, by_naming.places[0].path,
            "the same place, whichever door it was reached by"
        );
        assert_eq!(
            by_standing.places[0].root, by_naming.places[0].root,
            "and the same door"
        );
    }

    /// `--global` from inside a closed root asks a different question, and is
    /// answered as asked. It is the one explicit way to widen, and the door
    /// does not take it away.
    #[test]
    fn global_from_a_parked_cwd_is_still_global() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", true);
        let roots = vec![parked_root(root_id, "/photos", "source")];

        let resolved =
            opened(resolve_scope_at(&conn, &[], true, &roots, Some(Path::new("/photos"))).unwrap());
        assert!(resolved.is_global());
        assert!(resolved.parked.is_empty(), "nothing was narrowed to state");
    }

    /// A directory under no known root is still the silent global fallback —
    /// the door changed the answer for closed roots, not for genuinely
    /// unknown places. A real directory on disk, because the CWD arm resolves
    /// through the filesystem fallback like every other path.
    #[test]
    fn a_cwd_under_no_root_is_still_the_silent_global_fallback() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", true);
        let roots = vec![parked_root(root_id, "/photos", "source")];
        let elsewhere = tempfile::tempdir().unwrap();

        let resolved =
            opened(resolve_scope_at(&conn, &[], false, &roots, Some(elsewhere.path())).unwrap());
        assert!(resolved.is_global());
    }

    /// One named parked path is terminal: there is nothing left to narrow to,
    /// and the door is what to say about it.
    #[test]
    fn a_single_parked_path_closes_the_door() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", true);
        insert_source(&conn, root_id, "2011/file.jpg", None);
        let roots = vec![parked_root(root_id, "/photos", "source")];

        let closed =
            closed(resolve_scope(&conn, &[PathBuf::from("/photos/2011")], false, &roots).unwrap());
        assert!(!closed.here);
        assert_eq!(closed.places[0].path, "/photos/2011");
        assert!(closed.sourceless.is_empty());
    }

    /// **Precedence: the door before the sources.** A parked path with
    /// nothing scanned under it is stated as parked, never as "no sources
    /// known" — the closed door is the reason it has nothing to say, and a
    /// second cause would be a false one.
    #[test]
    fn a_parked_path_is_parked_even_without_sources() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", true);
        let roots = vec![parked_root(root_id, "/photos", "source")];

        let closed =
            closed(resolve_scope(&conn, &[PathBuf::from("/photos/2011")], false, &roots).unwrap());
        assert_eq!(closed.places[0].path, "/photos/2011");
        assert!(
            closed.sourceless.is_empty(),
            "the door answered; the source gate never got to speak"
        );
    }

    /// The root-level exemption belongs to the source gate — "a root with
    /// nothing scanned into it is still a place to ask about" — and says
    /// nothing about doors. A closed root's own top is parked like anything
    /// under it.
    #[test]
    fn a_parked_root_level_path_is_parked() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", true);
        let roots = vec![parked_root(root_id, "/photos", "source")];

        let closed =
            closed(resolve_scope(&conn, &[PathBuf::from("/photos")], false, &roots).unwrap());
        assert_eq!(closed.places[0].path, "/photos");
    }

    /// A scope that kept nothing for two different reasons states both. One
    /// cause standing in for the other is exactly the false-cause family this
    /// partition exists to end.
    #[test]
    fn parked_and_sourceless_together_name_both_causes() {
        let conn = setup_test_db();
        let live = insert_root(&conn, "/live", "source", false);
        let parked = insert_root(&conn, "/photos", "source", true);
        insert_source(&conn, live, "2011/file.jpg", None);
        let roots = vec![
            make_test_root(live, "/live", "source"),
            parked_root(parked, "/photos", "source"),
        ];

        let closed = closed(
            resolve_scope(
                &conn,
                &[PathBuf::from("/photos/2011"), PathBuf::from("/live/2012")],
                false,
                &roots,
            )
            .unwrap(),
        );
        assert_eq!(closed.places[0].path, "/photos/2011");
        assert_eq!(closed.sourceless, vec!["/live/2012".to_string()]);
    }

    /// One closed door among several asked-for places is a set-aside, not a
    /// refusal: the rest runs and the door is stated beside it — the
    /// boundary's own proceed-and-state policy, third cause.
    #[test]
    fn a_multi_path_scope_proceeds_past_a_parked_member() {
        let conn = setup_test_db();
        let live = insert_root(&conn, "/live", "source", false);
        let parked = insert_root(&conn, "/photos", "source", true);
        insert_source(&conn, live, "2011/file.jpg", None);
        insert_source(&conn, parked, "2011/file.jpg", None);
        let roots = vec![
            make_test_root(live, "/live", "source"),
            parked_root(parked, "/photos", "source"),
        ];

        let resolved = opened(
            resolve_scope(
                &conn,
                &[PathBuf::from("/photos/2011"), PathBuf::from("/live/2011")],
                false,
                &roots,
            )
            .unwrap(),
        );
        assert_eq!(resolved.prefixes, vec!["/live/2011".to_string()]);
        assert_eq!(resolved.parked.len(), 1);
        assert_eq!(resolved.parked[0].path, "/photos/2011");
    }

    /// Root membership stays the harder failure: a path under no root at all
    /// is refused whatever else was asked for, door or no door.
    #[test]
    fn root_membership_still_precedes_the_door() {
        let conn = setup_test_db();
        let parked = insert_root(&conn, "/photos", "source", true);
        let roots = vec![parked_root(parked, "/photos", "source")];

        let dir = tempfile::tempdir().unwrap();
        let stranger = dir.path().join("nowhere");
        std::fs::create_dir(&stranger).unwrap();

        let err = resolve_scope(
            &conn,
            &[PathBuf::from("/photos/2011"), stranger.clone()],
            false,
            &roots,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("is not under any known root"), "{err}");
    }

    /// The recorded-scope class, extended to the third cause: a place behind
    /// a closed door never becomes a `DecisionScope`, so no act is ever
    /// written down as having happened there.
    #[test]
    fn a_parked_path_never_becomes_a_decision_scope() {
        use crate::core::domain::scope::DecisionScope;

        let conn = setup_test_db();
        let live = insert_root(&conn, "/live", "source", false);
        let parked = insert_root(&conn, "/photos", "source", true);
        insert_source(&conn, live, "2011/file.jpg", None);
        insert_source(&conn, parked, "2011/file.jpg", None);
        let roots = vec![
            make_test_root(live, "/live", "source"),
            parked_root(parked, "/photos", "source"),
        ];

        let resolved = opened(
            resolve_scope(
                &conn,
                &[PathBuf::from("/photos/2011"), PathBuf::from("/live/2011")],
                false,
                &roots,
            )
            .unwrap(),
        );
        let scopes = DecisionScope::decompose(&resolved.prefixes, &roots);
        assert_eq!(
            scopes,
            vec![DecisionScope::new(
                live,
                "/live".to_string(),
                "2011".to_string()
            )]
        );
        assert!(scopes.iter().all(|sc| sc.root_id != parked));
    }

    /// A closed door a **remembering** view reads at keeps the pause and sets
    /// nothing aside: the two registers are opposite dispositions of one fact
    /// and must not be confused, or a reading would print a set-aside line
    /// about the very places it is showing.
    #[test]
    fn reading_behind_a_door_carries_the_pause_and_sets_nothing_aside() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", true);
        let roots = vec![parked_root(root_id, "/photos", "source")];

        let reading = closed(
            resolve_scope_at(&conn, &[], false, &roots, Some(Path::new("/photos/2011"))).unwrap(),
        )
        .read_here();

        assert_eq!(reading.prefixes, vec!["/photos/2011".to_string()]);
        assert!(reading.parked.is_empty());
        assert!(reading.from_cwd);
        assert_eq!(reading.pause[0].root_path, "/photos");
    }

    // ========================================================================
    // Form tolerance at the source-existence gate
    // ========================================================================

    /// A composed `é` (U+00E9) and its decomposed twin (`e` + U+0301) — one
    /// visible folder name, two byte-forms.
    const NFC_DIR: &str = "caf\u{e9}";
    const NFD_DIR: &str = "cafe\u{301}";

    #[test]
    fn an_nfc_argument_finds_sources_stored_in_nfd() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, &format!("{NFD_DIR}/file.jpg"), None);
        let roots = vec![make_test_root(root_id, "/photos", "source")];

        let resolved = opened(
            resolve_scope(
                &conn,
                &[PathBuf::from(format!("/photos/{NFC_DIR}"))],
                false,
                &roots,
            )
            .unwrap(),
        );
        assert_eq!(resolved.prefixes, vec![format!("/photos/{NFD_DIR}")]);
    }

    #[test]
    fn an_nfd_argument_finds_sources_stored_in_nfc() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, &format!("{NFC_DIR}/file.jpg"), None);
        let roots = vec![make_test_root(root_id, "/photos", "source")];

        let resolved = opened(
            resolve_scope(
                &conn,
                &[PathBuf::from(format!("/photos/{NFD_DIR}"))],
                false,
                &roots,
            )
            .unwrap(),
        );
        assert_eq!(resolved.prefixes, vec![format!("/photos/{NFC_DIR}")]);
    }

    /// The form-tolerance rule's pin: the argument bends to the stored form,
    /// and the kept prefix is the stored bytes — so it actually selects the
    /// rows it names, in Rust and in SQL alike.
    #[test]
    fn the_kept_prefix_is_byte_identical_to_the_stored_form() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let stored_rel = format!("{NFD_DIR}/file.jpg");
        insert_source(&conn, root_id, &stored_rel, None);
        let roots = vec![make_test_root(root_id, "/photos", "source")];

        let resolved = opened(
            resolve_scope(
                &conn,
                &[PathBuf::from(format!("/photos/{NFC_DIR}"))],
                false,
                &roots,
            )
            .unwrap(),
        );

        let kept = &resolved.prefixes[0];
        let (_, _, _, rel) = domain::root::find_containing_root(kept, &roots).unwrap();
        assert_eq!(rel.as_bytes(), NFD_DIR.as_bytes());
        assert!(repo::source::sources_exist_at_scope(&conn, root_id, &rel).unwrap());
    }

    // ========================================================================
    // The manifest door — resolve_recorded_scope
    //
    // The same rule as the argument door above, at the door a manifest's
    // `meta.scope` comes through. Every test here builds a real database,
    // because the whole question is what the index knows.
    // ========================================================================

    /// The anchor: generate, refresh and apply use exactly the same code, so
    /// the same paths must produce the same
    /// recorded scope whether they were typed as arguments (through
    /// `resolve_scope`) or written into a manifest by hand (through
    /// `resolve_recorded_scope`). This is the test that fails the day the two
    /// doors drift apart again.
    #[test]
    fn the_two_doors_agree_on_the_same_paths() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, &format!("{NFD_DIR}/a.jpg"), None);
        insert_source(&conn, root_id, "2011/b.jpg", None);
        let roots = vec![make_test_root(root_id, "/photos", "source")];

        // Typed with the accent in the other form, as a user would retype it.
        let typed = [format!("/photos/{NFC_DIR}"), "/photos/2011".to_string()];

        let by_argument = opened(
            resolve_scope(
                &conn,
                &typed.iter().map(PathBuf::from).collect::<Vec<_>>(),
                false,
                &roots,
            )
            .unwrap(),
        );
        let by_manifest = resolve_recorded_scope(&conn, &typed, &roots).unwrap();

        assert_eq!(
            by_manifest.recorded(),
            by_argument.prefixes,
            "the manifest door must record what the argument door keeps"
        );
        assert_eq!(
            by_manifest
                .scopes()
                .iter()
                .map(DecisionScope::display_path)
                .collect::<Vec<_>>(),
            DecisionScope::decompose(&by_argument.prefixes, &roots)
                .iter()
                .map(DecisionScope::display_path)
                .collect::<Vec<_>>(),
        );
    }

    /// The defect's own shape at the unit: an accented prefix typed in the
    /// other normalization, **below an ASCII root**, resolves to the form the
    /// index stores. Attribution alone never sees this — the root matches as
    /// typed, so no candidate but as-given is tried on the whole prefix — and
    /// the second stage is what closes it.
    #[test]
    fn a_recorded_prefix_below_an_ascii_root_heals_to_the_stored_form() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, &format!("{NFD_DIR}/sub1/a.jpg"), None);
        let roots = vec![make_test_root(root_id, "/photos", "source")];

        let resolution =
            resolve_recorded_scope(&conn, &[format!("/photos/{NFC_DIR}/sub1")], &roots).unwrap();

        assert_eq!(
            resolution.scopes(),
            [DecisionScope::new(
                root_id,
                "/photos".to_string(),
                format!("{NFD_DIR}/sub1")
            )],
            "the confirmed scope must carry the index's bytes"
        );
        assert_eq!(resolution.recorded(), [format!("/photos/{NFD_DIR}/sub1")]);
        assert!(resolution.set_aside().is_empty());
        assert!(resolution.unrooted().is_empty());
    }

    /// The two spellings of one visible name, both really present under one
    /// root — the normalization-*sensitive* filesystem's case. As-given wins,
    /// so each prefix resolves to itself and neither is bent onto the other.
    /// This is what the candidate ordering buys and what tidying it would
    /// destroy.
    #[test]
    fn as_given_wins_when_the_index_knows_it() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, &format!("{NFD_DIR}/a.jpg"), None);
        insert_source(&conn, root_id, &format!("{NFC_DIR}/b.jpg"), None);
        let roots = vec![make_test_root(root_id, "/photos", "source")];

        for dir in [NFD_DIR, NFC_DIR] {
            let resolution =
                resolve_recorded_scope(&conn, &[format!("/photos/{dir}")], &roots).unwrap();
            assert_eq!(
                resolution.scopes()[0].rel_prefix.as_bytes(),
                dir.as_bytes(),
                "{dir} must resolve to itself"
            );
        }
    }

    /// A rooted prefix the index knows nothing under is set aside — neither
    /// silently obeyed nor dropped.
    #[test]
    fn a_sourceless_recorded_prefix_is_set_aside() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, "2011/a.jpg", None);
        let roots = vec![make_test_root(root_id, "/photos", "source")];

        let resolution = resolve_recorded_scope(
            &conn,
            &["/photos/2011".to_string(), "/photos/2012".to_string()],
            &roots,
        )
        .unwrap();

        assert_eq!(resolution.set_aside(), ["/photos/2012"]);
        assert!(resolution.unrooted().is_empty());
    }

    /// The behavioural core of the fix: a set-aside line is absent from
    /// `scopes()`, so it never reaches the vantage, the lock header or the
    /// decision record. What that buys downstream — the surviving line
    /// measuring from itself rather than from a common prefix dragged above
    /// it — is the vantage's own claim and is pinned in
    /// `expr::domain::vantage` (V8), because a core test may not name a
    /// subsystem.
    #[test]
    fn a_set_aside_line_never_reaches_the_measurement() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, "2011/a.jpg", None);
        let roots = vec![make_test_root(root_id, "/photos", "source")];

        let resolution = resolve_recorded_scope(
            &conn,
            &["/photos/2011".to_string(), "/photos/2012".to_string()],
            &roots,
        )
        .unwrap();

        assert_eq!(
            resolution.scopes(),
            [DecisionScope::new(
                root_id,
                "/photos".to_string(),
                "2011".to_string()
            )]
        );
        assert!(
            !resolution.scopes().iter().any(|s| s.rel_prefix == "2012"),
            "the set-aside prefix must not be a scope"
        );
    }

    /// The write-back does not destroy the user's own line: a set-aside prefix
    /// is still in `recorded()`, and a refresh writes it back rather than
    /// silently narrowing the manifest.
    #[test]
    fn a_set_aside_line_survives_in_the_write_back() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, "2011/a.jpg", None);
        let roots = vec![make_test_root(root_id, "/photos", "source")];

        let resolution = resolve_recorded_scope(
            &conn,
            &["/photos/2011".to_string(), "/photos/2012".to_string()],
            &roots,
        )
        .unwrap();

        assert_eq!(resolution.recorded(), ["/photos/2011", "/photos/2012"]);
    }

    /// A root's own top is always confirmed, whatever has been scanned into
    /// it — the honesty policy's root-level rule, at the second door.
    #[test]
    fn a_root_level_recorded_prefix_is_always_confirmed() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let roots = vec![make_test_root(root_id, "/photos", "source")];

        let resolution = resolve_recorded_scope(&conn, &["/photos".to_string()], &roots).unwrap();

        assert_eq!(
            resolution.scopes(),
            [DecisionScope::new(
                root_id,
                "/photos".to_string(),
                String::new()
            )]
        );
        assert!(resolution.set_aside().is_empty());
    }

    /// The grain the door supplies, over every case it has to tell apart.
    ///
    /// A prefix with a present source standing at it is an `Item`; one with
    /// none is a `Directory`, whatever the index may still remember beneath it.
    /// A root is a `Directory` without the index being consulted at all.
    ///
    /// **The two rows that carry the whole reason presence is read**, and the
    /// shapes a path with a past takes on a *current* index:
    ///
    /// - `was_dir_now_file` — a directory that has become a file. Tombstones
    ///   lie beneath it and a live row stands at it, so a history-reading
    ///   grain calls it a place, measures from it, and hands its own entry
    ///   `""` — the blank destination this mechanism exists to remove.
    /// - `was_file_now_dir` — the mirror. A tombstone stands at it and live
    ///   content lies beneath, and it is a place, which is what presence says.
    ///
    /// Both are reachable with no staleness whatever: scan, change the disk,
    /// scan again.
    #[test]
    fn the_door_supplies_the_grain_of_every_confirmed_prefix() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let roots = vec![make_test_root(root_id, "/photos", "source")];

        let bury = |rel: &str| {
            conn.execute(
                "UPDATE sources SET present = 0 WHERE root_id = ? AND rel_path = ?",
                rusqlite::params![root_id, rel],
            )
            .unwrap();
        };

        insert_source(&conn, root_id, "dir/a.jpg", None);
        insert_source(&conn, root_id, "solo.jpg", None);

        // A directory that has become a file.
        insert_source(&conn, root_id, "was_dir_now_file/old.jpg", None);
        bury("was_dir_now_file/old.jpg");
        insert_source(&conn, root_id, "was_dir_now_file", None);

        // A file that has become a directory.
        insert_source(&conn, root_id, "was_file_now_dir", None);
        bury("was_file_now_dir");
        insert_source(&conn, root_id, "was_file_now_dir/new.jpg", None);

        // Both live at once: the file-grain scan infers no absence, so a
        // folder replaced by a file of the same name and rescanned *by name*
        // leaves the old contents standing beside the new file.
        insert_source(&conn, root_id, "both_live/old.jpg", None);
        insert_source(&conn, root_id, "both_live", None);

        // The grain is observable only through the register it feeds — which
        // is the layer split working, not a gap in the test.
        let measures_from = |path: &str| {
            let resolution = resolve_recorded_scope(&conn, &[path.to_string()], &roots).unwrap();
            assert_eq!(resolution.scopes().len(), 1, "{path} confirmed");
            resolution.measured_from()[0].location()
        };

        assert_eq!(measures_from("/photos/dir"), "/photos/dir", "a directory");
        assert_eq!(
            measures_from("/photos/dir/a.jpg"),
            "/photos/dir",
            "an item measures from the directory containing it"
        );
        assert_eq!(
            measures_from("/photos/solo.jpg"),
            "/photos",
            "a root-level item measures from its root"
        );
        assert_eq!(measures_from("/photos"), "/photos", "a root is a directory");

        assert_eq!(
            measures_from("/photos/was_dir_now_file"),
            "/photos",
            "a directory that became a file is an item now — measuring from \
             itself would hand its own entry a blank destination"
        );
        assert_eq!(
            measures_from("/photos/was_file_now_dir"),
            "/photos/was_file_now_dir",
            "and a file that became a directory is a place, tombstone at it \
             notwithstanding"
        );
        assert_eq!(
            measures_from("/photos/both_live"),
            "/photos",
            "a live row at the path settles it however much stands beneath — \
             no rule that consults `below` can answer this one without choosing"
        );
    }

    /// A root is a directory, and knowing that needs no index — true of a root
    /// with nothing scanned into it, which the index could only call an item.
    ///
    /// **Asserted on the grain itself, and against the only thing that can
    /// make the branch bite.** Reading `measured_from()` here asserts nothing —
    /// both grains measure an empty remainder to the root — and on an ordinary
    /// index the fall-through happens to agree, since no ordinary source
    /// carries an empty `rel_path`. So the case that pins the branch is the one
    /// where a row *does* stand at the root's own remainder: `rel_path` is
    /// `NOT NULL` with no non-empty check, so nothing in the schema forbids it,
    /// and without the short-circuit such a row would make a root an `Item`.
    ///
    /// That edge is also the unreachability proof's: the argument needs
    /// `parent(S) ≠ S`, which holds for every non-empty remainder and fails
    /// only at the root — where this branch answers before
    /// `containing_location` is ever reached. A root that measured from its own
    /// parent would measure from outside itself.
    #[test]
    fn a_root_is_a_directory_even_with_a_row_standing_at_its_own_remainder() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);

        assert_eq!(
            scope_grain(&conn, root_id, "").unwrap(),
            ScopeGrain::Directory,
            "a root with nothing scanned into it is still a directory"
        );

        insert_source(&conn, root_id, "", None);
        assert_eq!(
            scope_grain(&conn, root_id, "").unwrap(),
            ScopeGrain::Directory,
            "and stays one even with a row standing at its own remainder — \
             the index must not be able to make a root an item"
        );
    }

    /// Core's half of the unreachability guarantee, over every shape a path
    /// with a past can take.
    ///
    /// The guarantee runs `E ⊑ S ⊑ P ⊑ V`. The `S ⊑ P` link is this layer's —
    /// a scope's measuring point is at or above the scope, and **strictly**
    /// above it whenever a present source stands there, because that is what
    /// makes it an `Item`. The `P ⊑ V` link is a property of
    /// `common_path_prefix` — argued, and exercised by value in
    /// `expr::ScopeVantage`'s own battery rather than pinned as a property.
    /// The whole chain is pinned end to end at `archive::ops::generate`,
    /// against the real vantage and a real index.
    ///
    /// Shapes: a plain directory, a plain file, a directory that became a
    /// file, a file that became a directory, both-live (the file-grain scan's
    /// shape), an all-tombstone path, and the root.
    #[test]
    fn a_measuring_point_is_strictly_above_anything_standing_at_its_scope() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let roots = vec![make_test_root(root_id, "/photos", "source")];

        let bury = |rel: &str| {
            conn.execute(
                "UPDATE sources SET present = 0 WHERE root_id = ? AND rel_path = ?",
                rusqlite::params![root_id, rel],
            )
            .unwrap();
        };

        insert_source(&conn, root_id, "plain_dir/a.jpg", None);
        insert_source(&conn, root_id, "plain_file", None);
        insert_source(&conn, root_id, "d2f/old.jpg", None);
        bury("d2f/old.jpg");
        insert_source(&conn, root_id, "d2f", None);
        insert_source(&conn, root_id, "f2d", None);
        bury("f2d");
        insert_source(&conn, root_id, "f2d/new.jpg", None);
        insert_source(&conn, root_id, "both/old.jpg", None);
        insert_source(&conn, root_id, "both", None);
        insert_source(&conn, root_id, "ghost", None);
        insert_source(&conn, root_id, "ghost/g.jpg", None);
        bury("ghost");
        bury("ghost/g.jpg");

        let (mut standing, mut empty_handed) = (0usize, 0usize);
        for rel in ["plain_dir", "plain_file", "d2f", "f2d", "both", "ghost", ""] {
            let path = if rel.is_empty() {
                "/photos".to_string()
            } else {
                format!("/photos/{rel}")
            };
            let resolution =
                resolve_recorded_scope(&conn, std::slice::from_ref(&path), &roots).unwrap();
            let point = resolution.measured_from()[0].location();

            if repo::source::present_source_exists_at_path(&conn, root_id, rel).unwrap() {
                standing += 1;
                assert_ne!(
                    point, path,
                    "{path} has a present source standing on it, so its measuring \
                     point must be strictly above it"
                );
                assert_eq!(
                    domain::path::path_strip_prefix(&path, &point),
                    Some(rel.rsplit('/').next().unwrap()),
                    "and it must render as its own name from there"
                );
            } else {
                empty_handed += 1;
                assert_eq!(
                    point, path,
                    "{path} holds nothing standing, so it measures from itself"
                );
            }
        }
        // Both branches must actually have run: a predicate stuck on one
        // answer would otherwise sweep seven shapes and assert one rule.
        assert_eq!(
            (standing, empty_handed),
            (3, 4),
            "plain_file/d2f/both stand; plain_dir/f2d/ghost/root do not"
        );
    }

    /// The grain is asked on the **confirmed** byte-form, never on the form
    /// the manifest happened to be typed in. A file recorded in the other
    /// normalization must still come back as an item: asking the losing
    /// candidate finds nothing beneath it and nothing at it either, and the
    /// answer would be right by accident.
    #[test]
    fn the_grain_is_asked_on_the_confirmed_byte_form() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, &format!("{NFD_DIR}/a.jpg"), None);
        let roots = vec![make_test_root(root_id, "/photos", "source")];

        // The directory typed the other way: still a directory.
        let dir = resolve_recorded_scope(&conn, &[format!("/photos/{NFC_DIR}")], &roots).unwrap();
        assert_eq!(
            dir.measured_from()[0].location(),
            format!("/photos/{NFD_DIR}")
        );

        // The file inside it, typed the other way: an item, measuring from the
        // directory in the bytes the index stores.
        let item =
            resolve_recorded_scope(&conn, &[format!("/photos/{NFC_DIR}/a.jpg")], &roots).unwrap();
        assert_eq!(
            item.measured_from()[0].location(),
            format!("/photos/{NFD_DIR}"),
        );
    }

    /// The roster law, unchanged by the second stage: a prefix under no known
    /// root is carried as unrooted, never dropped and never set aside — the
    /// two are different answers and the reader says different things about
    /// them.
    #[test]
    fn a_recorded_prefix_under_no_root_is_still_carried() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, "2011/a.jpg", None);
        let roots = vec![make_test_root(root_id, "/photos", "source")];

        let resolution = resolve_recorded_scope(
            &conn,
            &["/gone/proj".to_string(), "/photos/2011".to_string()],
            &roots,
        )
        .unwrap();

        assert_eq!(resolution.unrooted(), ["/gone/proj"]);
        assert!(resolution.set_aside().is_empty());
        assert_eq!(resolution.recorded(), ["/gone/proj", "/photos/2011"]);
    }

    /// Parity for the extraction: the argument door's behaviour is unchanged
    /// by `stored_form_of_rel` being pulled out from under it. Root-level
    /// paths, healed remainders, sourceless paths and paths under no known
    /// root all answer exactly as they did.
    #[test]
    fn the_argument_door_still_answers_as_it_did() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, &format!("{NFD_DIR}/a.jpg"), None);
        let roots = vec![make_test_root(root_id, "/photos", "source")];

        for (path, expected) in [
            ("/photos", Some("/photos".to_string())),
            (
                &format!("/photos/{NFC_DIR}"),
                Some(format!("/photos/{NFD_DIR}")),
            ),
            ("/photos/2012", None),
            ("/elsewhere/x", Some("/elsewhere/x".to_string())),
        ] {
            assert_eq!(
                stored_form_with_sources(&conn, path, &roots).unwrap(),
                expected,
                "for {path}"
            );
        }
    }

    #[test]
    fn a_path_sourceless_under_every_form_reaches_the_policy_as_sourceless() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, "2011/file.jpg", None);
        let roots = vec![make_test_root(root_id, "/photos", "source")];

        // Neither form of this folder is known — the gate says so rather
        // than silently keeping one of the candidates.
        let result = resolve_scope(
            &conn,
            &[PathBuf::from(format!("/photos/{NFC_DIR}"))],
            false,
            &roots,
        );
        let err = result.unwrap_err().to_string();
        assert!(err.contains("no sources known"), "error was: {err}");
    }

    #[test]
    fn history_scope_resolves_an_emptied_place_on_a_live_root() {
        // No sources stand at /photos/moved-away — a move-mode apply took
        // them all — but the path is under a live root, so the history
        // question still resolves.
        let roots = vec![make_test_root(1, "/photos", "source")];
        let resolved =
            resolve_history_scope(&[PathBuf::from("/photos/moved-away")], &roots).unwrap();
        assert_eq!(resolved.prefixes, vec!["/photos/moved-away".to_string()]);
        assert!(!resolved.from_cwd);
        assert!(!resolved.auto_include_archived);
    }

    #[test]
    fn history_scope_declines_paths_under_no_root() {
        let roots = vec![make_test_root(1, "/photos", "source")];
        assert!(resolve_history_scope(&[PathBuf::from("/elsewhere/x")], &roots).is_none());
        assert!(resolve_history_scope(&[], &roots).is_none());
    }

    #[test]
    fn history_scope_carries_the_archive_include_rule() {
        let roots = vec![make_test_root(2, "/archive", "archive")];
        let resolved =
            resolve_history_scope(&[PathBuf::from("/archive/media/old")], &roots).unwrap();
        assert!(resolved.auto_include_archived);
    }

    #[test]
    fn validate_sources_exist_errors_on_unknown() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let roots = vec![make_test_root(root_id, "/photos", "source")];

        let result = validate_sources_exist(&conn, &["/photos/typo".to_string()], &roots);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("no sources known"), "error was: {err}");
    }

    #[test]
    fn validate_sources_exist_accepts_root_level() {
        let conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        let roots = vec![make_test_root(root_id, "/photos", "source")];

        // Root-level path (empty rel_path) should always pass, even with no sources
        let result = validate_sources_exist(&conn, &["/photos".to_string()], &roots);
        assert!(result.is_ok());
    }

    // ========================================================================
    // parse_root_spec() tests (with &[Root] input)
    // ========================================================================

    #[test]
    fn parse_root_spec_impl_by_id_found() {
        let roots = vec![
            make_test_root(1, "/a", "source"),
            make_test_root(2, "/b", "archive"),
        ];
        let result = parse_root_spec(&roots, "id:2", None);
        assert_eq!(result.unwrap(), RootLookup::Found(2));
    }

    #[test]
    fn parse_root_spec_impl_by_id_not_found() {
        let roots = vec![make_test_root(1, "/a", "source")];
        let result = parse_root_spec(&roots, "id:999", None);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("No root with id 999"));
    }

    #[test]
    fn parse_root_spec_impl_role_filter_source_accepts_source() {
        let roots = vec![make_test_root(1, "/a", "source")];
        let result = parse_root_spec(&roots, "id:1", Some("source"));
        assert_eq!(result.unwrap(), RootLookup::Found(1));
    }

    #[test]
    fn parse_root_spec_impl_role_filter_source_rejects_archive() {
        let roots = vec![make_test_root(1, "/a", "archive")];
        let result = parse_root_spec(&roots, "id:1", Some("source"));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("role 'archive', expected 'source'"));
    }

    #[test]
    fn parse_root_spec_impl_role_filter_archive_accepts_archive() {
        let roots = vec![make_test_root(1, "/a", "archive")];
        let result = parse_root_spec(&roots, "id:1", Some("archive"));
        assert_eq!(result.unwrap(), RootLookup::Found(1));
    }

    #[test]
    fn parse_root_spec_impl_role_filter_archive_rejects_source() {
        let roots = vec![make_test_root(1, "/a", "source")];
        let result = parse_root_spec(&roots, "id:1", Some("archive"));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("role 'source', expected 'archive'"));
    }

    #[test]
    fn parse_root_spec_impl_role_filter_none_accepts_any() {
        let roots = vec![
            make_test_root(1, "/a", "source"),
            make_test_root(2, "/b", "archive"),
        ];
        // None means accept any role
        assert_eq!(
            parse_root_spec(&roots, "id:1", None).unwrap(),
            RootLookup::Found(1)
        );
        assert_eq!(
            parse_root_spec(&roots, "id:2", None).unwrap(),
            RootLookup::Found(2)
        );
    }

    /// **The overturn**: a closed root used to come back as no root at all,
    /// and every caller then spoke a false cause about it. It is now named as
    /// what it is, and what to do about it is the caller's permit class.
    #[test]
    fn parse_root_spec_names_a_parked_root_not_an_absence() {
        let mut suspended_root = make_test_root(1, "/a", "source");
        suspended_root.suspended = true;
        let roots = vec![suspended_root];

        let found = parse_root_spec(&roots, "id:1", None).unwrap();
        assert_eq!(
            found,
            RootLookup::Parked(ParkedRoot {
                root_id: 1,
                root_path: "/a".to_string(),
            })
        );
    }

    #[test]
    fn parse_root_spec_any_includes_suspended() {
        let mut suspended_root = make_test_root(1, "/a", "source");
        suspended_root.suspended = true;
        let roots = vec![suspended_root];

        // parse_root_spec_any should include suspended roots
        let result = parse_root_spec_any(&roots, "id:1");
        assert_eq!(result.unwrap(), 1);
    }

    // ========================================================================
    // parse_root_spec ByPath tests (soft resolution)
    // ========================================================================

    #[test]
    fn parse_root_spec_by_path_matches_root() {
        let roots = vec![make_test_root(1, "/a/b", "source")];
        let result = parse_root_spec(&roots, "path:/a/b", None);
        assert_eq!(result.unwrap(), RootLookup::Found(1));
    }

    #[test]
    fn parse_root_spec_by_path_no_match() {
        let roots = vec![make_test_root(1, "/a/b", "source")];
        let result = parse_root_spec(&roots, "path:/nonexistent", None);
        assert!(result.is_err());
    }

    #[test]
    fn parse_root_spec_by_path_role_filter() {
        let roots = vec![make_test_root(1, "/a/b", "source")];
        // Root exists but has wrong role
        let result = parse_root_spec(&roots, "path:/a/b", Some("archive"));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("role 'source', expected 'archive'"));
    }

    /// The same overturn at the path door — which is where the false cause
    /// was actually read: `canon roots rm path:<parked>` answered
    /// "No root for path" about a root the user had merely closed. That
    /// sentence is now reserved for a root that genuinely is not there.
    #[test]
    fn parse_root_spec_by_path_names_a_parked_root_not_an_absence() {
        let mut root = make_test_root(1, "/a/b", "source");
        root.suspended = true;
        let roots = vec![root];
        let found = parse_root_spec(&roots, "path:/a/b", None).unwrap();
        assert_eq!(
            found,
            RootLookup::Parked(ParkedRoot {
                root_id: 1,
                root_path: "/a/b".to_string(),
            })
        );
    }

    #[test]
    fn parse_root_spec_any_by_path_suspended_included() {
        let mut root = make_test_root(1, "/a/b", "source");
        root.suspended = true;
        let roots = vec![root];
        // parse_root_spec_any includes suspended roots
        let result = parse_root_spec_any(&roots, "path:/a/b");
        assert_eq!(result.unwrap(), 1);
    }

    // ========================================================================
    // resolve_archive_path tests
    // ========================================================================

    #[test]
    fn resolve_archive_path_exact_root_match_offline() {
        let roots = vec![make_test_root(1, "/archive", "archive")];
        let result = resolve_archive_path(&roots, Path::new("/archive")).unwrap();
        assert_eq!(result, (1, "/archive".to_string(), String::new()));
    }

    #[test]
    fn resolve_archive_path_under_root_offline() {
        let roots = vec![make_test_root(1, "/archive", "archive")];
        let result = resolve_archive_path(&roots, Path::new("/archive/photos/2020")).unwrap();
        assert_eq!(
            result,
            (1, "/archive".to_string(), "photos/2020".to_string())
        );
    }

    #[test]
    fn resolve_archive_path_rejects_source_role() {
        let roots = vec![make_test_root(1, "/archive", "source")];
        let result = resolve_archive_path(&roots, Path::new("/archive/photos"));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not an archive"));
    }

    #[test]
    fn resolve_archive_path_not_under_any_root() {
        // No known root matches offline; canonicalize_maybe_missing resolves
        // the made-up absolute path against the always-existing "/" ancestor,
        // then the containing-root search comes up empty.
        let roots = vec![make_test_root(1, "/archive", "archive")];
        let result = resolve_archive_path(&roots, Path::new("/totally/unregistered/path"));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("not inside any registered archive root"));
    }

    #[cfg(unix)]
    #[test]
    fn resolve_archive_path_falls_back_to_real_canonicalize_via_symlink() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let real_archive = dir.path().join("real-archive");
        std::fs::create_dir(&real_archive).unwrap();
        let canonical_archive = std::fs::canonicalize(&real_archive)
            .unwrap()
            .to_string_lossy()
            .to_string();

        // A symlink alias whose own path does not lexically start with the
        // root's canonical path — only fs::canonicalize (the fallback) can
        // recognize it, never the offline string match.
        let alias = dir.path().join("alias-to-archive");
        symlink(&real_archive, &alias).unwrap();

        let roots = vec![make_test_root(1, &canonical_archive, "archive")];
        let result = resolve_archive_path(&roots, &alias.join("photos")).unwrap();
        assert_eq!(result, (1, canonical_archive, "photos".to_string()));
    }

    // ========================================================================
    // resolve_path fallback tests (real disk, no known-root match offline)
    // ========================================================================

    #[cfg(unix)]
    #[test]
    fn resolve_path_matches_known_root_via_symlink_alias() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let real_root = dir.path().join("real-root");
        std::fs::create_dir(&real_root).unwrap();
        let canonical_root = std::fs::canonicalize(&real_root)
            .unwrap()
            .to_string_lossy()
            .to_string();

        // The alias's own path does not lexically start with the root's
        // canonical path — only fs::canonicalize (the fallback) resolves it.
        let alias = dir.path().join("alias-to-root");
        symlink(&real_root, &alias).unwrap();

        let roots = vec![make_test_root(1, &canonical_root, "source")];
        let result = resolve_path(&alias, &roots, dir.path()).unwrap();
        assert_eq!(result, canonical_root);
    }

    #[test]
    fn resolve_path_arbitrary_real_disk_path_resolves_via_plain_canonicalize() {
        // A real path that matches no known root still resolves — the
        // compare/exclude case of an arbitrary disk path outside any root.
        let dir = tempfile::tempdir().unwrap();
        let roots = vec![make_test_root(1, "/nowhere/near/here", "source")];
        let result = resolve_path(dir.path(), &roots, dir.path()).unwrap();
        let expected = std::fs::canonicalize(dir.path())
            .unwrap()
            .to_string_lossy()
            .to_string();
        assert_eq!(result, expected);
    }

    #[test]
    fn resolve_path_neither_root_nor_disk_matches_errors() {
        let roots = vec![make_test_root(1, "/a/b", "source")];
        let result = resolve_path(Path::new("/nonexistent/path"), &roots, Path::new("/any"));
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("not accessible on disk"));
        assert!(err.contains("is the storage attached?"));
    }

    #[cfg(unix)]
    #[test]
    fn parse_root_spec_by_path_matches_via_symlink_alias() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let real_root = dir.path().join("real-root");
        std::fs::create_dir(&real_root).unwrap();
        let canonical_root = std::fs::canonicalize(&real_root).unwrap();

        let alias = dir.path().join("alias-to-root");
        symlink(&real_root, &alias).unwrap();

        let roots = vec![make_test_root(
            1,
            &canonical_root.to_string_lossy(),
            "source",
        )];
        let result = parse_root_spec(&roots, &format!("path:{}", alias.display()), None);
        assert_eq!(result.unwrap(), RootLookup::Found(1));
    }

    #[cfg(unix)]
    #[test]
    fn resolve_root_path_matches_via_symlink_alias() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let real_root = dir.path().join("real-root");
        std::fs::create_dir(&real_root).unwrap();
        let canonical_root = std::fs::canonicalize(&real_root)
            .unwrap()
            .to_string_lossy()
            .to_string();

        let alias = dir.path().join("alias-to-root");
        symlink(&real_root, &alias).unwrap();

        let roots = vec![make_test_root(1, &canonical_root, "source")];
        let result = resolve_root_path_any(&roots, &alias).unwrap();
        assert_eq!(
            result,
            Some((1, canonical_root, "source".to_string(), String::new()))
        );
    }
}
