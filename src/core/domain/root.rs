//! Root domain concepts for canon.
//!
//! This module defines how roots are identified and resolved:
//! - Domain types: Root struct, RootSpec enum
//! - Pure domain functions: find_containing_root(), predicates
//!
//! All functions in this module are pure (no I/O). Resolution against known
//! roots with a filesystem fallback (parse_root_spec, resolve_root_path,
//! resolve_archive_path) lives in `core::ops::scope`, which composes these pure
//! predicates. Callers must fetch roots via `core::repo::root::fetch_all()` before
//! calling resolution functions.

use anyhow::{bail, Context, Result};

use super::path::{path_is_under, path_strip_prefix};

// ============================================================================
// Domain Concepts (pure, no I/O)
// ============================================================================

/// Parsed root specification - how a user identifies a root.
///
/// This enum represents the domain concept of "how do users specify a root?"
/// independent of how roots are stored or how paths are canonicalized.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RootSpec {
    /// Root identified by database ID
    ById(i64),
    /// Root identified by path (not yet canonicalized)
    ByPath(String),
}

impl RootSpec {
    /// Parse a root spec string ("id:N" or "path:/foo").
    ///
    /// This is a pure parsing function - it does NOT canonicalize paths.
    /// The caller must canonicalize ByPath variants before database lookup.
    pub fn parse(spec: &str) -> Result<Self> {
        if let Some(id_str) = spec.strip_prefix("id:") {
            let id: i64 = id_str.parse().context("Invalid root ID")?;
            Ok(RootSpec::ById(id))
        } else if let Some(path) = spec.strip_prefix("path:") {
            Ok(RootSpec::ByPath(path.to_string()))
        } else {
            bail!("Invalid root spec '{spec}'. Use id:<N> or path:<path>")
        }
    }
}

/// Find which root contains the given canonical path.
///
/// Pure function - no I/O, no database access. Takes pre-fetched Root objects.
///
/// # Arguments
/// * `canonical_path` - The canonicalized path to look up
/// * `roots` - Slice of Root domain objects (typically from `core::repo::root::fetch_all()`)
///
/// # Returns
/// Some((root_id, root_path, role, relative_path)) if the path is under a root,
/// None otherwise.
/// How a set of resolved prefixes divides by the role of the root that
/// contains each one.
///
/// Source-side and archive-side are different questions, not different
/// amounts of the same one, so a command whose frame reads only one side
/// can say which of the asked-for places it cannot answer for.
#[derive(Debug, Default)]
pub struct RolePartition {
    /// Prefixes under a source root, or under no known root at all.
    pub source_side: Vec<String>,
    /// `(prefix, containing archive-root path)` — the root path is carried so
    /// a statement can name each archive root once rather than repeating it
    /// per prefix.
    pub archive_side: Vec<(String, String)>,
}

/// Split resolved prefixes by the role of their containing root.
///
/// Pure: role attribution is derivable from `(prefixes, roots)`, so it is
/// computed where it is needed rather than carried as state. Suspension is
/// not consulted — a suspended root's role is still its role, and what a
/// closed door permits is the suspension law's own question.
pub fn partition_prefixes_by_role(prefixes: &[String], roots: &[Root]) -> RolePartition {
    let mut partition = RolePartition::default();
    for prefix in prefixes {
        match find_containing_root(prefix, roots) {
            Some((_, root_path, role, _)) if role == "archive" => {
                partition.archive_side.push((prefix.clone(), root_path));
            }
            _ => partition.source_side.push(prefix.clone()),
        }
    }
    partition
}

pub fn find_containing_root(
    canonical_path: &str,
    roots: &[Root],
) -> Option<(i64, String, String, String)> {
    for root in roots {
        if canonical_path == root.path {
            return Some((root.id, root.path.clone(), root.role.clone(), String::new()));
        }
        if let Some(rel) = path_strip_prefix(canonical_path, &root.path) {
            return Some((
                root.id,
                root.path.clone(),
                root.role.clone(),
                rel.to_string(),
            ));
        }
    }
    None
}

/// A root the user closed the door on.
///
/// Suspension is the user's own act — everything about the root closed by
/// default until the same hand opens it again — so a place behind that door
/// is never spoken of as absent, empty or global. This is the payload every
/// door carries when it answers "closed": which root, and where it stands.
///
/// Constructed only from a [`Root`] that is actually suspended
/// ([`Root::parked`]), so a live root cannot be spoken of as parked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParkedRoot {
    /// Database id of the closed root.
    pub root_id: i64,
    /// The root's own path — what the statement names and what the way back
    /// takes.
    pub root_path: String,
}

impl ParkedRoot {
    /// The way back through this particular door.
    pub fn way_back(&self) -> WayBack {
        WayBack::unsuspend(&self.root_path)
    }

    /// The closed door's one sentence:
    /// `<root> suspended — <verb>: <what> · <way back>`.
    ///
    /// The sweep's footer grammar, generalised — the verb is the surface's
    /// permit class and `what` is the place as it was asked about (`here` at
    /// the CWD door, the path otherwise, a count for a lock or a batch).
    /// Every door speaks it: the scope boundary, the root-spec door, the acts
    /// that refuse from inside operations. **Composed here, in the domain,
    /// precisely because its consumers are not all in one layer** — a second
    /// spelling is how three refusals came to say the same thing three ways.
    /// Terminal shaping (grouping several doors, capping the list, choosing a
    /// channel) stays in the interface, where a screen is.
    pub fn door_line(&self, verb: DoorVerb, what: &str) -> String {
        format!(
            "{} suspended — {}: {what} · {}",
            self.root_path,
            verb.label(),
            self.way_back().display(),
        )
    }

    /// What a **remembering** view states about the door it is reading
    /// behind: the pause and the way back, and no verb — it neither set
    /// aside nor refused. It read.
    pub fn pause_line(&self) -> String {
        format!(
            "{} suspended · {}",
            self.root_path,
            self.way_back().display()
        )
    }
}

/// A place that was asked about — by naming it or by standing in it — which
/// turns out to stand on a closed root.
///
/// The path is the one the boundary resolved, not a narrowing of it: what is
/// stated is what was asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParkedPath {
    /// The asked-for place, resolved.
    pub path: String,
    /// The door it stands behind.
    pub root: ParkedRoot,
}

/// What a surface's permit class does about a closed door — the only thing
/// the door's sentence varies by.
///
/// The registry's four permits are the vocabulary: a **view** sets the root
/// aside and says so; an **act** is refused by name with the way back stated.
/// Remembering states a pause and no verb at all (it read), and the sweep's
/// board speaks a third verb from its own file — it partitions a universe
/// rather than a scope, and has no scope to set aside.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DoorVerb {
    /// A view: the parked places are set aside and stated.
    SetAside,
    /// An act: refused by name, nothing written.
    Refused,
}

impl DoorVerb {
    /// The word the sentence uses.
    pub fn label(self) -> &'static str {
        match self {
            DoorVerb::SetAside => "set aside",
            DoorVerb::Refused => "refused",
        }
    }
}

/// A refusal that is itself a legitimate answer: the door was closed.
///
/// **Why an error type and not a printed line.** The exit convention — a
/// refusal that is an answer exits non-zero with no `Error:` prefix, because
/// nothing went wrong — is the interface's to carry out, and an operation may
/// not print. Every door reachable only from inside an operation would
/// otherwise have to choose between saying the wrong thing (`Error:` on a
/// door the user closed themselves) and threading a typed outcome through
/// every result struct between it and the screen. This carries the sentence
/// out through the ordinary error channel and is recognised at the front
/// door, which states it as it stands.
///
/// It cannot hold a free string: the sentence comes from
/// [`ParkedRoot::door_line`], so a refusal raised from an operation and one
/// printed by a command are the same sentence by construction.
#[derive(Debug)]
pub struct DoorRefused {
    line: String,
}

impl DoorRefused {
    pub fn new(root: &ParkedRoot, verb: DoorVerb, what: &str) -> Self {
        Self {
            line: root.door_line(verb, what),
        }
    }

    /// The whole place, named as itself — the commonest `what` there is.
    pub fn at(root: &ParkedRoot, verb: DoorVerb) -> Self {
        Self::new(root, verb, &root.root_path)
    }
}

impl std::fmt::Display for DoorRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.line)
    }
}

impl std::error::Error for DoorRefused {}

/// A command Canon prints as the way out of a situation it is refusing or
/// setting aside — carried as the argv it would run, never as loose text.
///
/// **A way back that does not run is worse than none.** The invariant this
/// type exists to carry is that the printed form and the runnable form cannot
/// disagree: there is one construction and two projections of it, so a
/// round-trip pin over [`argv`](Self::argv) proves what
/// [`display`](Self::display) put on the screen. Two functions returning a
/// string and a vector would be two spellings of one command, free to drift.
///
/// The sweep's footer, the closed door's sentence and the unplaceable-receipt
/// hint all speak through this — the third consumer is what made it a type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WayBack {
    argv: Vec<String>,
}

impl WayBack {
    /// Open this root's door again — the way back from every closed door, and
    /// only this: never the destructive one.
    pub fn unsuspend(root_path: &str) -> Self {
        Self {
            argv: vec![
                "canon".to_string(),
                "roots".to_string(),
                "unsuspend".to_string(),
                format!("path:{root_path}"),
            ],
        }
    }

    /// See the closed doors, when there are too many to name one by one.
    pub fn list_suspended() -> Self {
        Self {
            argv: vec![
                "canon".to_string(),
                "roots".to_string(),
                "list".to_string(),
                "--suspended".to_string(),
            ],
        }
    }

    /// What the user would type: shell-quoted, so a path with a space runs as
    /// printed.
    pub fn display(&self) -> String {
        self.argv
            .iter()
            .map(|a| super::format::shell_quote(a))
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// The argv this line claims will run — what the round-trip pin parses.
    pub fn argv(&self) -> &[String] {
        &self.argv
    }
}

/// A root directory registered in canon.
///
/// Roots are the top-level directories that canon manages. Each root has a role
/// (source or archive) that determines how its contents are treated.
///
/// This struct represents the domain model for roots — it contains all stored
/// root data and provides pure predicates for filtering and classification.
#[derive(Debug, Clone)]
pub struct Root {
    /// Database ID
    pub id: i64,
    /// Canonical absolute path
    pub path: String,
    /// Role: "source" or "archive"
    pub role: String,
    /// Optional user comment
    pub comment: Option<String>,
    /// Unix timestamp of last scan (None if never scanned)
    pub last_scanned_at: Option<i64>,
    /// Whether this root is suspended (hidden from most operations)
    pub suspended: bool,
}

impl Root {
    /// Check if this root is suspended.
    pub fn is_suspended(&self) -> bool {
        self.suspended
    }

    /// Check if this root is active (not suspended).
    pub fn is_active(&self) -> bool {
        !self.suspended
    }

    /// This root as a closed door, or `None` if its door is open.
    ///
    /// The only constructor of [`ParkedRoot`]: speaking of a place as parked
    /// requires a root that actually is.
    pub fn parked(&self) -> Option<ParkedRoot> {
        self.suspended.then(|| ParkedRoot {
            root_id: self.id,
            root_path: self.path.clone(),
        })
    }

    /// Check if this root has the "source" role.
    pub fn is_source(&self) -> bool {
        self.role == "source"
    }

    /// Check if this root has the "archive" role.
    // Part of the domain model API but not currently used. Kept for API completeness.
    #[allow(dead_code)]
    pub fn is_archive(&self) -> bool {
        self.role == "archive"
    }

    /// Check if this root matches a scope path.
    ///
    /// Matching is bidirectional:
    /// - Root is at or under the scope (root path starts with scope)
    /// - Scope is at or under the root (scope starts with root path)
    ///
    /// This is used by `canon roots <scope>` to find related roots.
    ///
    /// Containment is a boundary claim in both directions: `/a/bc` is not
    /// under `/a/b`, so a listing under one scope never offers a root the
    /// scope did not name — which is what the user then removes, suspends or
    /// retires.
    pub fn matches_scope(&self, scope: &str) -> bool {
        path_is_under(&self.path, scope) || path_is_under(scope, &self.path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // RootSpec::parse() tests
    // ========================================================================

    #[test]
    fn parse_root_spec_by_id() {
        assert_eq!(RootSpec::parse("id:123").unwrap(), RootSpec::ById(123));
    }

    #[test]
    fn parse_root_spec_by_id_zero() {
        assert_eq!(RootSpec::parse("id:0").unwrap(), RootSpec::ById(0));
    }

    #[test]
    fn parse_root_spec_by_path() {
        assert_eq!(
            RootSpec::parse("path:/foo/bar").unwrap(),
            RootSpec::ByPath("/foo/bar".to_string())
        );
    }

    #[test]
    fn parse_root_spec_by_path_relative() {
        // Relative paths are accepted (canonicalization is caller's job)
        assert_eq!(
            RootSpec::parse("path:./relative").unwrap(),
            RootSpec::ByPath("./relative".to_string())
        );
    }

    #[test]
    fn parse_root_spec_invalid_id() {
        assert!(RootSpec::parse("id:abc").is_err());
    }

    #[test]
    fn parse_root_spec_invalid_format() {
        assert!(RootSpec::parse("garbage").is_err());
        assert!(RootSpec::parse("").is_err());
    }

    // ========================================================================
    // find_containing_root() tests
    // ========================================================================

    /// Helper to create a Root with a specific id, path, and role.
    fn make_root_with(id: i64, path: &str, role: &str) -> Root {
        Root {
            id,
            path: path.to_string(),
            role: role.to_string(),
            comment: None,
            last_scanned_at: None,
            suspended: false,
        }
    }

    #[test]
    fn find_containing_root_exact_match() {
        let roots = vec![make_root_with(1, "/a/b", "source")];
        let result = find_containing_root("/a/b", &roots);
        assert_eq!(
            result,
            Some((1, "/a/b".to_string(), "source".to_string(), String::new()))
        );
    }

    #[test]
    fn find_containing_root_under_root() {
        let roots = vec![make_root_with(1, "/a/b", "source")];
        let result = find_containing_root("/a/b/c/d", &roots);
        assert_eq!(
            result,
            Some((
                1,
                "/a/b".to_string(),
                "source".to_string(),
                "c/d".to_string()
            ))
        );
    }

    #[test]
    fn find_containing_root_not_found() {
        let roots = vec![make_root_with(1, "/a/b", "source")];
        let result = find_containing_root("/x/y/z", &roots);
        assert_eq!(result, None);
    }

    #[test]
    fn find_containing_root_not_under_similar_prefix() {
        // /a/bc is NOT under /a/b (different directory)
        let roots = vec![make_root_with(1, "/a/b", "source")];
        let result = find_containing_root("/a/bc/d", &roots);
        assert_eq!(result, None);
    }

    #[test]
    fn find_containing_root_multiple_roots_first_match() {
        let roots = vec![
            make_root_with(1, "/a", "source"),
            make_root_with(2, "/a/b", "archive"),
        ];
        // First matching root wins
        let result = find_containing_root("/a/b/c", &roots);
        assert_eq!(
            result,
            Some((1, "/a".to_string(), "source".to_string(), "b/c".to_string()))
        );
    }

    #[test]
    fn find_containing_root_empty_roots() {
        let roots: Vec<Root> = vec![];
        let result = find_containing_root("/a/b", &roots);
        assert_eq!(result, None);
    }

    // ========================================================================
    // Root struct and predicates tests
    // ========================================================================

    /// Helper to create a Root with sensible defaults for testing.
    fn make_root() -> Root {
        Root {
            id: 1,
            path: "/test/path".to_string(),
            role: "source".to_string(),
            comment: None,
            last_scanned_at: None,
            suspended: false,
        }
    }

    #[test]
    fn is_suspended_true() {
        let root = Root {
            suspended: true,
            ..make_root()
        };
        assert!(root.is_suspended());
    }

    #[test]
    fn is_suspended_false() {
        let root = Root {
            suspended: false,
            ..make_root()
        };
        assert!(!root.is_suspended());
    }

    #[test]
    fn is_active_when_not_suspended() {
        let root = Root {
            suspended: false,
            ..make_root()
        };
        assert!(root.is_active());
    }

    #[test]
    fn is_active_when_suspended() {
        let root = Root {
            suspended: true,
            ..make_root()
        };
        assert!(!root.is_active());
    }

    #[test]
    fn is_source_true() {
        let root = Root {
            role: "source".to_string(),
            ..make_root()
        };
        assert!(root.is_source());
        assert!(!root.is_archive());
    }

    #[test]
    fn is_source_false() {
        let root = Root {
            role: "archive".to_string(),
            ..make_root()
        };
        assert!(!root.is_source());
    }

    #[test]
    fn is_archive_true() {
        let root = Root {
            role: "archive".to_string(),
            ..make_root()
        };
        assert!(root.is_archive());
        assert!(!root.is_source());
    }

    #[test]
    fn is_archive_false() {
        let root = Root {
            role: "source".to_string(),
            ..make_root()
        };
        assert!(!root.is_archive());
    }

    #[test]
    fn matches_scope_root_under_scope() {
        // Root /a/b/c is under scope /a/b
        let root = Root {
            path: "/a/b/c".to_string(),
            ..make_root()
        };
        assert!(root.matches_scope("/a/b"));
    }

    #[test]
    fn matches_scope_scope_under_root() {
        // Scope /a/b/c/d is under root /a/b
        let root = Root {
            path: "/a/b".to_string(),
            ..make_root()
        };
        assert!(root.matches_scope("/a/b/c/d"));
    }

    #[test]
    fn matches_scope_exact_match() {
        let root = Root {
            path: "/a/b".to_string(),
            ..make_root()
        };
        assert!(root.matches_scope("/a/b"));
    }

    #[test]
    fn matches_scope_no_match() {
        let root = Root {
            path: "/a/b".to_string(),
            ..make_root()
        };
        assert!(!root.matches_scope("/x/y"));
    }

    /// The documented containment edge, in both directions: a listing must
    /// never offer a root whose path merely begins with the same characters
    /// as the scope, because acting on what a scope listed is the next step.
    #[test]
    fn matches_scope_similar_prefix_no_match() {
        let sibling = Root {
            path: "/a/bc".to_string(),
            ..make_root()
        };
        assert!(!sibling.matches_scope("/a/b"));

        let root = Root {
            path: "/a/b".to_string(),
            ..make_root()
        };
        assert!(!root.matches_scope("/a/bc"));
        // The real shape of the mistake: a backup drive beside the one named.
        assert!(!root.matches_scope("/a/b-backup"));
    }
}
