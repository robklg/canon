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

/// Partition a fleet of roots by whether its door is open.
///
/// **The one spelling of "which roots are open."** Every site that used to
/// filter a `roots` vector it was already holding with `is_active()` or
/// `is_suspended()` partitions here instead: the parked half comes back as
/// data — one [`ParkedRoot`] per closed door, [`Root::parked`]'s sole
/// constructor — rather than being discarded the way a bare filter throws
/// it away. A caller that only ever wanted the open half still gets it, at
/// `.0`; what changes is that the other half stops being silently lost.
///
/// Pure: openness is a property of the `Root` the caller already holds, so
/// nothing here re-derives it from disk or re-fetches from the database.
/// Role is not consulted — a caller wanting only source or archive roots
/// filters before or after, same as it always did.
pub fn open_roots(roots: &[Root]) -> (Vec<&Root>, Vec<ParkedRoot>) {
    let mut open = Vec::new();
    let mut parked = Vec::new();
    for root in roots {
        match root.parked() {
            Some(p) => parked.push(p),
            None => open.push(root),
        }
    }
    (open, parked)
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
        // The place asked about is the root itself: the sentence already
        // names it at the door and again in the way back, so a third naming
        // in the `<what>` slot degenerates into `/r suspended — rejected: /r ·
        // canon roots unsuspend path:/r`. The slot is dropped, here in the
        // one spelling, so every door that reaches a root top reads the same.
        if what == self.root_path {
            return format!(
                "{} suspended — {} · {}",
                self.root_path,
                verb.label(),
                self.way_back().display(),
            );
        }
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

/// The established marker for a root the user has closed the door on. Not a
/// door *statement* — no verb, no way back: the marker is testimony, and the
/// registry's testifying clause is stated, never headlined. One spelling for
/// every surface that tags a cited place with the door behind it.
pub const ROOT_SUSPENDED_MARKER: &str = " (suspended)";

/// `ROOT_SUSPENDED_MARKER` when `suspended`, the empty string otherwise.
pub fn suspended_marker(suspended: bool) -> &'static str {
    if suspended {
        ROOT_SUSPENDED_MARKER
    } else {
        ""
    }
}

/// Above this many named roots a testimony segment collapses to a count.
/// The door statement's cap, reused for the same reason: a qualifier that
/// names twenty roots stops qualifying and starts shouting.
const TESTIMONY_ROOTS_NAMED_CAP: usize = 3;

/// A list of closed-door root paths, named up to the testimony cap or
/// counted above it — the one spelling behind every door-adjacent sentence
/// that lists more than one root, so a sentence naming several closed roots
/// never disagrees with another about when to stop naming and start
/// counting. Sorted and deduped first.
pub enum RootNaming {
    /// At or under the cap: every path, sorted, comma-joined.
    Named(String),
    /// Over the cap: how many, not which.
    Counted(usize),
}

impl RootNaming {
    /// As a bare list: `/a, /b` or `4 suspended roots`.
    pub fn as_list(&self) -> String {
        match self {
            RootNaming::Named(names) => names.clone(),
            RootNaming::Counted(n) => format!("{n} suspended roots"),
        }
    }

    /// As the qualifier's own phrase: `suspended /a, /b` or `4 suspended
    /// roots` — the `Counted` arm already carries the word, so it is not
    /// repeated.
    pub fn as_suspended_phrase(&self) -> String {
        match self {
            RootNaming::Named(names) => format!("suspended {names}"),
            RootNaming::Counted(n) => format!("{n} suspended roots"),
        }
    }
}

/// Cap, sort and dedupe a set of closed-door root paths for a sentence that
/// lists them.
pub fn capped_root_names(root_paths: &[String]) -> RootNaming {
    let mut paths: Vec<&str> = root_paths.iter().map(String::as_str).collect();
    paths.sort_unstable();
    paths.dedup();
    if paths.len() > TESTIMONY_ROOTS_NAMED_CAP {
        RootNaming::Counted(paths.len())
    } else {
        RootNaming::Named(paths.join(", "))
    }
}

/// What a claim says about resting **solely** on copies behind closed doors:
/// `<count> only on suspended <roots>`.
///
/// Three surfaces state it — coverage's archived line, the readiness
/// review's covered line, the story's footer — over one derivation
/// (`only_on_suspended`). Composed in the domain for the reason
/// [`ParkedRoot::door_line`] is: the story's footer is written from
/// operations, being the book's text as well as the terminal's, so an
/// interface-only spelling could not serve it.
///
/// **No way back is offered.** The registry's testifying clause is *stated,
/// never headlined*: this is a qualifier on a claim the surface is otherwise
/// making, not a refusal the user must get past, and a runnable command
/// beside it would turn a footnote into an instruction. The way back to any
/// of these roots is one `canon roots list --suspended` away, and every
/// surface that actually blocks on a door prints it.
///
/// `None` when nothing rests only behind a door — the segment is suppressed
/// at zero rather than printed as `0`, because "0 only on suspended roots"
/// is a sentence about a situation that is not happening.
pub fn only_on_suspended_segment(count: i64, root_paths: &[String]) -> Option<String> {
    if count <= 0 || root_paths.is_empty() {
        return None;
    }
    let where_ = capped_root_names(root_paths).as_suspended_phrase();
    Some(format!("{count} only on {where_}"))
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
    /// A **report about acts it does not perform**: the door is stated in the
    /// conditional, naming the act it would turn away. `cluster status` is
    /// its one surface — it neither refuses nor narrows anything of its own,
    /// and a reader whose next step has just been withheld is owed the act
    /// that is blocked rather than the report's own disposition.
    WouldRefuse(RefusedAct),
}

impl DoorVerb {
    /// The word the sentence uses.
    pub fn label(self) -> &'static str {
        match self {
            DoorVerb::SetAside => "set aside",
            DoorVerb::Refused => "rejected",
            DoorVerb::WouldRefuse(act) => act.label(),
        }
    }
}

/// Which act a report says a door would turn away — the payload of
/// [`DoorVerb::WouldRefuse`].
///
/// A closed set, and deliberately: the acts a *record* can reach behind a door
/// through are exactly the two that read a manifest, and a report naming a
/// third would be naming one nobody derived. The union arm exists because one
/// root can be reached by both — the manifest's scope names it and its lock
/// draws from it — and one door still gets one line.
///
/// The command names are spelled here beside the way back's, which is the one
/// place canon commands belong in the spine: a printed line and the command it
/// names must not drift.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefusedAct {
    /// Only `apply` — the lock's origin end, or the destination.
    Apply,
    /// Only `cluster refresh` — the manifest's recorded scope.
    Refresh,
    /// Both, for a root the manifest reaches through its scope *and* its lock.
    RefreshAndApply,
}

impl RefusedAct {
    /// The conditional phrase that fills the verb slot.
    pub fn label(self) -> &'static str {
        match self {
            RefusedAct::Apply => "apply would be rejected",
            RefusedAct::Refresh => "cluster refresh would be rejected",
            RefusedAct::RefreshAndApply => "cluster refresh and apply would be rejected",
        }
    }

    /// The act, or acts, a caller has found at one door. `None` where neither
    /// is turned away, so a caller cannot state a door with nothing behind it.
    ///
    /// Takes [`RefusedActs`], not a bare `(bool, bool)`: two same-typed
    /// positional booleans are silently transposable at a call site — swap
    /// which is which and the compiler cannot tell — and one of this
    /// function's own callers falls back `.unwrap_or(RefusedAct::Apply)` on
    /// `None`, which would make a transposed pair read as a *different*
    /// wrong act rather than fail loudly. Named fields close that.
    pub fn of(acts: RefusedActs) -> Option<Self> {
        match (acts.refresh, acts.apply) {
            (true, true) => Some(RefusedAct::RefreshAndApply),
            (true, false) => Some(RefusedAct::Refresh),
            (false, true) => Some(RefusedAct::Apply),
            (false, false) => None,
        }
    }
}

/// The two doors [`RefusedAct::of`] asks about, named so a caller cannot
/// transpose which bool answers which question.
#[derive(Debug, Clone, Copy)]
pub struct RefusedActs {
    /// Would `cluster refresh` be refused here?
    pub refresh: bool,
    /// Would `apply` be refused here?
    pub apply: bool,
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
/// It cannot hold a caller-typed string: the sentence comes from
/// [`ParkedRoot::door_line`] (a place refused by name), or — for a refusal
/// about the fleet rather than one place, such as nowhere at all to bind
/// a book — from an established whole-fleet composer built on
/// [`capped_root_names`]/[`RootNaming`], never hand-assembled at the call
/// site. Either way a refusal raised from an operation and one printed by a
/// command are the same sentence by construction.
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

    /// Several doors at once, already composed — an act (apply's origin end)
    /// that can draw from more than one closed root in the same run. Takes
    /// lines already built by `closed_door_reaches`/`closed_door_lines`
    /// rather than a second composition of its own, so a multi-door refusal
    /// reads exactly as the interface would have printed it by hand.
    pub fn from_lines(lines: Vec<String>) -> Self {
        Self {
            line: lines.join("\n"),
        }
    }

    /// A refusal about the fleet rather than one named place — nowhere at
    /// all left to act, every candidate behind a closed door. Takes the
    /// sentence already composed by the caller's own established
    /// whole-fleet formatter (the `capped_root_names`/`RootNaming` family),
    /// never a string assembled at the call site: the door is still closed
    /// and the answer is still legitimate, so it carries out the same way a
    /// per-place refusal does, undecorated by `Error:`.
    pub fn fleetwide(sentence: String) -> Self {
        Self { line: sentence }
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

    // ========================================================================
    // open_roots() — the fleet partition
    // ========================================================================

    #[test]
    fn open_roots_partitions_open_from_parked() {
        let open_root = make_root_with(1, "/open", "source");
        let mut parked_root = make_root_with(2, "/parked", "source");
        parked_root.suspended = true;

        let roots = vec![open_root, parked_root];
        let (open, parked) = open_roots(&roots);

        assert_eq!(open.len(), 1);
        assert_eq!(open[0].path, "/open");
        assert_eq!(parked.len(), 1);
        assert_eq!(
            parked[0],
            ParkedRoot {
                root_id: 2,
                root_path: "/parked".to_string(),
            }
        );
    }

    #[test]
    fn open_roots_of_an_all_open_fleet_leaves_parked_empty() {
        let roots = vec![make_root_with(1, "/a", "source")];
        let (open, parked) = open_roots(&roots);
        assert_eq!(open.len(), 1);
        assert!(parked.is_empty());
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

    // ========================================================================
    // The testimony segment
    // ========================================================================

    fn paths(n: usize) -> Vec<String> {
        (1..=n).map(|i| format!("/arc{i}")).collect()
    }

    #[test]
    fn the_testimony_segment_names_its_roots_up_to_the_cap() {
        assert_eq!(
            only_on_suspended_segment(1, &paths(1)).unwrap(),
            "1 only on suspended /arc1"
        );
        assert_eq!(
            only_on_suspended_segment(37, &paths(3)).unwrap(),
            "37 only on suspended /arc1, /arc2, /arc3"
        );
    }

    #[test]
    fn above_the_cap_the_testimony_segment_counts_its_roots() {
        assert_eq!(
            only_on_suspended_segment(37, &paths(4)).unwrap(),
            "37 only on 4 suspended roots"
        );
    }

    #[test]
    fn the_testimony_segment_offers_no_way_back() {
        // It qualifies a claim the surface is otherwise making; a runnable
        // command beside it would turn a footnote into an instruction.
        let segment = only_on_suspended_segment(2, &paths(1)).unwrap();
        assert!(!segment.contains("canon roots"), "{segment}");
    }

    #[test]
    fn a_door_at_the_root_itself_names_the_root_once_beside_the_way_back() {
        let root = ParkedRoot {
            root_id: 7,
            root_path: "/vol/photos".to_string(),
        };
        let at_root = root.door_line(DoorVerb::Refused, "/vol/photos");
        assert_eq!(
            at_root, "/vol/photos suspended — rejected · canon roots unsuspend path:/vol/photos",
            "the `<what>` slot is dropped when the place is the root itself"
        );
        assert_eq!(
            at_root.matches("/vol/photos").count(),
            2,
            "the root is named at the door and in the way back, never a third time"
        );
        let below = root.door_line(DoorVerb::Refused, "/vol/photos/2019");
        assert!(
            below.contains("— rejected: /vol/photos/2019 ·"),
            "a place below the root keeps the slot: {below}"
        );
    }

    #[test]
    fn nothing_behind_a_door_says_nothing() {
        // Suppressed at zero on either axis: "0 only on suspended roots" is
        // a sentence about a situation that is not happening, and a count
        // with no roots to name has nothing to say.
        assert!(only_on_suspended_segment(0, &paths(2)).is_none());
        assert!(only_on_suspended_segment(5, &[]).is_none());
    }

    #[test]
    fn the_testimony_segment_sorts_and_dedupes_its_roots() {
        let noisy = vec![
            "/arc2".to_string(),
            "/arc1".to_string(),
            "/arc2".to_string(),
        ];
        assert_eq!(
            only_on_suspended_segment(4, &noisy).unwrap(),
            "4 only on suspended /arc1, /arc2"
        );
    }

    // ========================================================================
    // RefusedAct::of() tests — the named-field mapping a transposed bool pair
    // could otherwise silently read as a different, wrong act.
    // ========================================================================

    #[test]
    fn refused_act_of_reads_refresh_and_apply_by_name_not_position() {
        assert_eq!(
            RefusedAct::of(RefusedActs {
                refresh: true,
                apply: false,
            }),
            Some(RefusedAct::Refresh)
        );
        assert_eq!(
            RefusedAct::of(RefusedActs {
                refresh: false,
                apply: true,
            }),
            Some(RefusedAct::Apply)
        );
        assert_eq!(
            RefusedAct::of(RefusedActs {
                refresh: true,
                apply: true,
            }),
            Some(RefusedAct::RefreshAndApply)
        );
        assert_eq!(
            RefusedAct::of(RefusedActs {
                refresh: false,
                apply: false,
            }),
            None
        );
    }
}
