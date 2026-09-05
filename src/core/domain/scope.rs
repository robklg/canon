//! Scope domain concepts for canon.
//!
//! This module defines how path scopes work — the domain knowledge of
//! "what kind of match do we want?" separated from the SQL implementation
//! of "how do we express this in a query?".

use std::path::Path;

use super::path::normalization_candidates;
use super::root::{find_containing_root, Root};

/// Domain concept: what kind of scope match do we want?
///
/// This enum represents the domain decision of whether to match
/// a specific file exactly or all files under a directory.
///
/// Deciding which variant a given path is takes a look at the disk, so that
/// classification lives in the operations layer (`core::ops::scope::classify_all`);
/// the distinction itself is domain knowledge and lives here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopeMatch {
    /// Match a specific file exactly
    ExactFile(String),
    /// Match all files under a directory
    UnderDirectory(String),
}

/// A decision's scope, decomposed to a known root.
///
/// This is the recorder's *where* contract made into a type. Constructing one
/// requires a matching root, so a bare `"."`, a root-relative display path, or
/// any rootless string is unrepresentable — the invariant "a scope is a
/// canonical path under a known root" is a type here, not a convention that a
/// caller can violate (as a raw `"."` once did).
///
/// The recorder derives everything it stores from these: the `decision_scopes`
/// index rows from [`index_pair`](DecisionScope::index_pair), and the
/// `decisions.scope` / `meta.scope` display strings from
/// [`display_path`](DecisionScope::display_path). An empty `Vec<DecisionScope>`
/// means a global (unscoped) decision.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DecisionScope {
    pub root_id: i64,
    /// The root's stored canonical path (from the roots table).
    pub root_path: String,
    /// Path relative to the root; empty = the whole root.
    pub rel_prefix: String,
}

impl DecisionScope {
    /// Construct directly from a known root. For callers that already hold their
    /// root identity (roots rm/suspend/unsuspend, note clear) — no re-lookup.
    pub fn new(root_id: i64, root_path: String, rel_prefix: String) -> Self {
        Self {
            root_id,
            root_path,
            rel_prefix,
        }
    }

    /// The one funnel from resolved canonical prefix strings to typed scopes.
    ///
    /// Each prefix is matched to its containing root; a prefix under no known
    /// root is dropped — exactly as the recorder dropped it before (e.g. a
    /// `scan --add` root that does not exist yet). Results are sorted and
    /// deduplicated so repeated runs record identically.
    pub fn decompose(prefixes: &[String], roots: &[Root]) -> Vec<DecisionScope> {
        let mut scopes: Vec<DecisionScope> = prefixes
            .iter()
            .filter_map(|p| {
                find_containing_root(p, roots).map(|(root_id, root_path, _role, rel)| {
                    DecisionScope::new(root_id, root_path, rel)
                })
            })
            .collect();
        scopes.sort();
        scopes.dedup();
        scopes
    }

    /// Canonical absolute display path: the root path joined with the relative
    /// prefix (the root path alone when the prefix is empty). Never `"."` or a
    /// relative string — by construction.
    pub fn display_path(&self) -> String {
        if self.rel_prefix.is_empty() {
            self.root_path.clone()
        } else {
            format!("{}/{}", self.root_path, self.rel_prefix)
        }
    }

    /// The `(root_id, root_path, rel_prefix)` row for the `decision_scopes`
    /// index. `root_path` is a write-time snapshot (the same precedent as
    /// `decision_extractions`): it keeps the row renderable after the root is
    /// removed, when a live join can no longer resolve the path.
    pub fn index_row(&self) -> (i64, String, String) {
        (
            self.root_id,
            self.root_path.clone(),
            self.rel_prefix.clone(),
        )
    }
}

/// Attribute one recorded prefix to a known root, form-tolerantly.
///
/// The composition path resolution performs at the argument door —
/// [`normalization_candidates`] over [`find_containing_root`] — so a root
/// stored in the disk's form is reachable from a prefix typed in the other
/// one. Candidates are of the **whole** prefix and the first that finds a root
/// wins, which is what makes the form that matched the root the form the
/// prefix carries from here on.
///
/// It does not lexically clean the prefix (`resolve_path` calls `clean_path`
/// first), so a hand-written `..` is matched literally. That is a limit, not a
/// feature.
///
/// This answers *which root*, and only that. What byte-form of the remainder
/// the index knows is a question only the index can answer, and it is asked
/// afterwards, one layer up (`core::ops::scope::resolve_recorded_scope`).
pub fn attribute_prefix(prefix: &str, roots: &[Root]) -> Option<DecisionScope> {
    normalization_candidates(prefix).into_iter().find_map(|c| {
        find_containing_root(&c, roots)
            .map(|(root_id, root_path, _role, rel)| DecisionScope::new(root_id, root_path, rel))
    })
}

/// What a confirmed prefix names in the index: a place, or one item standing
/// at it.
///
/// The vantage's own precondition — *the deepest **directory** containing
/// every scope* — as a fact rather than an assumption. Only the index can
/// answer it (the disk cannot: a manifest door that stats would settle
/// placement differently depending on whether a drive happened to be
/// mounted), so it is supplied by `core::ops::scope::resolve_recorded_scope`
/// and never derived here. Which question it asks, and why presence rather
/// than history, belongs to that door and is written there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopeGrain {
    /// No present source stands at this path, so it is a place. Says nothing
    /// about what lies below it — a path whose content has all been deleted is
    /// still a place, and so is a root by construction.
    Directory,
    /// A present source stands at this path. One item — whatever else the
    /// index may still remember beneath it, and a path with a past can hold
    /// both at once.
    Item,
}

/// A location a measurement may be taken from: a directory, by construction.
///
/// Minted only in [`ScopeResolution::from_outcomes`], from a confirmed scope
/// and its [`ScopeGrain`] — an item scope contributes its containing
/// directory, which is what *the deepest directory containing every scope*
/// already says about a file. [`common_path_prefix`](super::path::common_path_prefix)
/// states the same thing as a prose precondition; at the vantage's boundary it
/// is the signature, so an item path cannot be folded even by mistake.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectoryLocation(DecisionScope);

impl DirectoryLocation {
    /// The root this location lies in — how the vantage groups per root.
    pub fn root_path(&self) -> &str {
        &self.0.root_path
    }

    /// The canonical absolute path measured from.
    pub fn location(&self) -> String {
        self.0.display_path()
    }
}

/// The directory containing a scope's relative path — a scope's own place when
/// it names an item.
///
/// A root is its own containing location, and both ways of arriving there give
/// it: a root-level name's parent is `Some("")`, and a root's own empty prefix
/// has no parent at all — `Path::new("").parent()` is `None`, so the default is
/// written out rather than left to a panic.
fn containing_location(scope: &DecisionScope) -> DecisionScope {
    let parent = Path::new(&scope.rel_prefix)
        .parent()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_default();
    DecisionScope::new(scope.root_id, scope.root_path.clone(), parent)
}

/// What became of one recorded prefix, in the manifest's own order.
///
/// The three answers a recorded prefix can get, and the only input
/// [`ScopeResolution::from_outcomes`] takes — so every register the type
/// exposes is derived from one list rather than accumulated in parallel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrefixOutcome {
    /// Rooted, and the index knows sources at or under it. The scope carries
    /// the stored byte-form throughout: the root's from attribution, the
    /// remainder's from whoever confirmed it. The grain says which of the two
    /// the confirmation was, and only the measurement register reads it.
    Confirmed(DecisionScope, ScopeGrain),
    /// Rooted, but no byte-form of it has any sources. Nothing to measure from
    /// and nothing to select, so it is stated and set aside — never obeyed
    /// silently, never dropped.
    SetAside(DecisionScope),
    /// Matches no known root at all, verbatim as the manifest wrote it.
    Unrooted(String),
}

/// A recorded scope resolved against the known roots — once, for the whole run.
///
/// A manifest's scope is user-editable text, so it arrives in whatever form
/// and whatever state the user left it: a path retyped in another
/// normalization, a folder since moved, a root since removed. That is one
/// question with one answer, and it used to be answered separately by every
/// reader — the vantage matched roots its own way, the recorder matched them
/// another, and each lost a prefix in its own silence.
///
/// Form-tolerant on the way in, in **two stages**, and both are the same
/// form-tolerance rule reaching a second half of one path rather than two
/// rules. Stage one is [`attribute_prefix`], which bends the whole prefix
/// against the known roots. Stage two bends the below-root remainder against
/// the index and needs a database to do it, so it lives one layer up in
/// `core::ops::scope::resolve_recorded_scope`, which is what fills the
/// `Confirmed`/`SetAside` split. Nothing in production assembles a resolution
/// without it: attribution alone answers *which root*, which is not enough to
/// confirm a prefix.
///
/// Honest on the way out: a prefix matching no root is carried, never dropped,
/// and neither is one the index cannot confirm. Dropping either silently
/// narrows whatever the reader does next — the vantage measures from somewhere
/// deeper, the recorder writes a scoped act down as a global one — and a
/// narrowing nobody stated is the class this closes. What to do about a
/// carried prefix is the consumer's own answer; this type classifies and never
/// decides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeResolution {
    /// Sorted and deduplicated: the recorder's projection, so repeated runs
    /// record identically.
    resolved: Vec<DecisionScope>,
    /// In the manifest's own order, first occurrence only — this is read back
    /// to a user against their own file.
    set_aside: Vec<String>,
    /// In the manifest's own order, first occurrence only — likewise read back
    /// to a user against their own file.
    unrooted: Vec<String>,
    /// Every recorded prefix in the manifest's own order: healed as far as it
    /// could be, verbatim past that. What a rewrite writes back.
    recorded: Vec<String>,
    /// The confirmed scopes as places to measure from — a directory scope as
    /// itself, an item scope as its containing directory. Neither sorted nor
    /// deduplicated: it feeds a fold where order and repetition are immaterial.
    measured_from: Vec<DirectoryLocation>,
}

impl ScopeResolution {
    /// Assemble the registers from the per-prefix outcomes, in the manifest's
    /// own order.
    ///
    /// The one assembly site, and the only constructor: what each register
    /// means is decided here rather than agreed on by two loops. The ops-layer
    /// resolution supplies the outcomes; a test that wants the attribution
    /// half alone composes [`attribute_prefix`] into this the same way.
    pub fn from_outcomes(outcomes: Vec<PrefixOutcome>) -> Self {
        let mut resolved: Vec<DecisionScope> = Vec::new();
        let mut set_aside: Vec<String> = Vec::new();
        let mut unrooted: Vec<String> = Vec::new();
        let mut recorded: Vec<String> = Vec::with_capacity(outcomes.len());
        let mut measured_from: Vec<DirectoryLocation> = Vec::new();

        for outcome in outcomes {
            match outcome {
                PrefixOutcome::Confirmed(scope, grain) => {
                    recorded.push(scope.display_path());
                    // The one place a DirectoryLocation is minted, and the
                    // only place the grain is read: an item scope contributes
                    // the directory that contains it, which is what "the
                    // deepest directory containing every scope" already says
                    // about a file. `resolved` is unmoved by this — selection,
                    // the lock header and the decision record see the scope
                    // the user named.
                    measured_from.push(DirectoryLocation(match grain {
                        ScopeGrain::Directory => scope.clone(),
                        ScopeGrain::Item => containing_location(&scope),
                    }));
                    resolved.push(scope);
                }
                PrefixOutcome::SetAside(scope) => {
                    // The root portion matched, so writing back its stored
                    // bytes is healing rather than inventing — it is the same
                    // display path this prefix got before the second stage
                    // existed. Only the remainder is unconfirmed, and nothing
                    // rewrites that.
                    let display = scope.display_path();
                    recorded.push(display.clone());
                    if !set_aside.contains(&display) {
                        set_aside.push(display);
                    }
                }
                PrefixOutcome::Unrooted(prefix) => {
                    if !unrooted.contains(&prefix) {
                        unrooted.push(prefix.clone());
                    }
                    recorded.push(prefix);
                }
            }
        }

        resolved.sort();
        resolved.dedup();

        ScopeResolution {
            resolved,
            set_aside,
            unrooted,
            recorded,
            measured_from,
        }
    }

    /// The places the run measures `{scope.rel_path}` from: every confirmed
    /// scope as a directory, by construction.
    ///
    /// The same register as [`scopes`](Self::scopes) — a set-aside or unrooted
    /// prefix contributes to neither — read through the grain the index
    /// supplied. It exists apart from `scopes()` because measurement and
    /// selection ask different questions of one confirmed prefix: selection
    /// asks *what did the user name*, and a file scope must select that file;
    /// measurement asks *what is there to name below*, and a file has only its
    /// own name to give, measured from the directory it sits in.
    pub fn measured_from(&self) -> &[DirectoryLocation] {
        &self.measured_from
    }

    /// The confirmed prefixes as typed scopes — what the recorder stores,
    /// what the lock header carries, and what a run selects from.
    ///
    /// The vantage reads [`measured_from`](Self::measured_from) rather than
    /// this: the same confirmed set, read as places to measure from. Both are
    /// derived from one outcome list at one site, so they are non-empty
    /// together and neither can carry a prefix the other does not.
    ///
    /// A set-aside prefix is **absent** from here, and that absence is the
    /// behavioural point: a line naming a place Canon cannot confirm stops
    /// dragging the measurement, so the surviving lines place correctly.
    pub fn scopes(&self) -> &[DecisionScope] {
        &self.resolved
    }

    /// The rooted prefixes the index knows no sources under, in the manifest's
    /// own order.
    pub fn set_aside(&self) -> &[String] {
        &self.set_aside
    }

    /// The prefixes that match no known root, in the manifest's own order.
    pub fn unrooted(&self) -> &[String] {
        &self.unrooted
    }

    /// Every recorded prefix, healed as far as it could be and verbatim past
    /// that — what a rewrite writes back.
    ///
    /// **Never what a run selects from.** A prefix in here that is not in
    /// [`scopes`](Self::scopes) contributes nothing to the measurement, and
    /// selecting content it names would gather files the run has told the user
    /// it cannot place. Selection and measurement read the same register or
    /// they disagree; see [`selection`](Self::selection).
    pub fn recorded(&self) -> &[String] {
        &self.recorded
    }

    /// The paths a run selects content from: the confirmed scopes, as absolute
    /// display paths.
    ///
    /// The same confirmed register the lock header is built from and the
    /// vantage measures from, which is the whole point. A set-aside prefix names nothing the index knows,
    /// and an unrooted one may still be an *ancestor* of a known root —
    /// `path_is_under` matches it where `find_containing_root` does not — so
    /// selecting from the recorded list gathers sources no vantage can measure
    /// and the run refuses them at apply, one by one, for a line it already
    /// said measures nothing.
    ///
    /// `None` where the manifest recorded a scope and none of it confirmed:
    /// the selection is **empty**, and the distinction matters because an
    /// empty *scope list* means global. A scope that resolved to nothing must
    /// select nothing, never everything.
    pub fn selection(&self) -> Option<Vec<String>> {
        if self.resolved.is_empty() && !self.recorded.is_empty() {
            return None;
        }
        Some(
            self.resolved
                .iter()
                .map(DecisionScope::display_path)
                .collect(),
        )
    }
}

/// Recover a scope row's root path from its decision's display paths.
///
/// A `decision_scopes` row written before root-path snapshots existed only has
/// `rel_prefix`; the decision's `scope` column holds display paths of the form
/// `root_path` or `root_path/rel_prefix` (see [`DecisionScope::display_path`]).
/// Given the row's known `rel_prefix`, the root path is the candidate with that
/// suffix stripped at a path boundary. NULL-over-guess: exactly one candidate
/// must match — ambiguity or no match returns `None` and the row stays
/// unrecovered rather than wrong.
pub fn recover_root_path(candidates: &[String], rel_prefix: &str) -> Option<String> {
    let matches: Vec<String> = if rel_prefix.is_empty() {
        // A whole-root scope's display IS the root path; only an unambiguous
        // single-candidate decision can attribute it.
        candidates.to_vec()
    } else {
        let suffix = format!("/{rel_prefix}");
        candidates
            .iter()
            .filter_map(|c| {
                let stripped = c.strip_suffix(&suffix)?;
                (!stripped.is_empty()).then(|| stripped.to_string())
            })
            .collect()
    };
    match matches.as_slice() {
        [only] => Some(only.clone()),
        _ => None,
    }
}

#[cfg(test)]
mod scope_resolution_tests {
    use super::*;

    fn root(id: i64, path: &str) -> Root {
        Root {
            id,
            path: path.to_string(),
            role: "source".to_string(),
            comment: None,
            last_scanned_at: None,
            suspended: false,
        }
    }

    fn suspended_root(id: i64, path: &str) -> Root {
        Root {
            suspended: true,
            ..root(id, path)
        }
    }

    fn owned(prefixes: &[&str]) -> Vec<String> {
        prefixes.iter().map(|p| p.to_string()).collect()
    }

    /// The attribution half alone: every prefix that finds a root, confirmed
    /// on its root match and nothing else, **every one of them a directory**.
    ///
    /// The grain is stated in the name because it is a premise, not a default:
    /// only the index can tell a directory from an item, and a helper that
    /// picked one silently would let a case about items pass for the wrong
    /// reason. Cases that need items build their outcomes directly.
    ///
    /// Production never assembles a resolution this way — confirming a prefix
    /// takes an answer only the index has, so `core::ops::scope` always runs
    /// both stages. This is spelled here rather than kept as a production
    /// function so the tree carries no constructor no command uses; what it
    /// pins is the pure half's own claims, which are this module's.
    fn attributed_dirs(prefixes: &[String], roots: &[Root]) -> ScopeResolution {
        ScopeResolution::from_outcomes(
            prefixes
                .iter()
                .map(|p| match attribute_prefix(p, roots) {
                    Some(scope) => PrefixOutcome::Confirmed(scope, ScopeGrain::Directory),
                    None => PrefixOutcome::Unrooted(p.clone()),
                })
                .collect(),
        )
    }

    /// A root path whose last component carries a combining accent, stored in
    /// the decomposed form a disk hands back. Written from code points rather
    /// than as a literal so the two forms cannot be confused by an editor
    /// that normalizes what it saves.
    const ROOT_NFD: &str = "/vol/work/cafe\u{301}";
    /// The same place typed the other way — one precomposed code point.
    const ROOT_NFC: &str = "/vol/work/caf\u{e9}";

    /// A1 — the form half. A prefix typed in the other normalization resolves,
    /// and the prefix that comes back is byte-identical to the root's stored
    /// bytes: the argument bends, the index is never rewritten.
    #[test]
    fn a_prefix_typed_in_another_normalization_resolves_to_the_stored_form() {
        let roots = vec![root(1, ROOT_NFD)];
        let resolution = attributed_dirs(&owned(&[&format!("{ROOT_NFC}/2016")]), &roots);

        assert!(
            resolution.unrooted().is_empty(),
            "the prefix names a known root: {:?}",
            resolution.unrooted()
        );
        assert_eq!(
            resolution.recorded(),
            [format!("{ROOT_NFD}/2016")],
            "the kept prefix must carry the stored root's bytes"
        );
        assert_eq!(
            resolution.scopes(),
            [DecisionScope::new(
                1,
                ROOT_NFD.to_string(),
                "2016".to_string()
            )]
        );
    }

    /// A2 — the unrooted half, at the funnel. The prefix is carried out where
    /// a caller can see it, never quietly discarded.
    #[test]
    fn a_prefix_under_no_root_is_carried_never_dropped() {
        let roots = vec![root(1, "/vol/work")];
        let resolution = attributed_dirs(&owned(&["/vol/gone/proj-v2"]), &roots);

        assert_eq!(resolution.unrooted(), ["/vol/gone/proj-v2"]);
        assert!(resolution.scopes().is_empty());
        assert_eq!(
            resolution.recorded(),
            ["/vol/gone/proj-v2"],
            "a scope was recorded"
        );
    }

    /// A3 — the compound pin, and the configuration the friction is made of:
    /// two prefixes the user wrote as siblings, one of which no longer names a
    /// known root. Neither hides the other — the survivor does not stand in
    /// for the whole recorded scope, and the casualty does not suppress the
    /// survivor.
    #[test]
    fn a_carried_prefix_does_not_displace_its_rooted_siblings() {
        let roots = vec![root(1, "/vol/work")];
        let resolution = attributed_dirs(
            &owned(&["/vol/work/proj-v1", "/vol/work-archive/proj-v2"]),
            &roots,
        );

        assert_eq!(
            resolution.scopes(),
            [DecisionScope::new(
                1,
                "/vol/work".to_string(),
                "proj-v1".to_string()
            )]
        );
        assert_eq!(resolution.unrooted(), ["/vol/work-archive/proj-v2"]);
    }

    /// A4 — the three-projection identity: nothing recorded goes missing, and
    /// every recorded prefix is accounted for by exactly one of the two
    /// answers.
    #[test]
    fn every_recorded_prefix_survives_the_resolution() {
        let roots = vec![root(1, "/vol/work"), root(2, "/media/backup")];
        let prefixes = owned(&[
            "/vol/work/proj-v1",
            "/vol/gone/proj-v2",
            "/media/backup",
            "/elsewhere",
        ]);
        let resolution = attributed_dirs(&prefixes, &roots);

        assert_eq!(resolution.recorded().len(), prefixes.len());

        let mut accounted: Vec<String> = resolution
            .scopes()
            .iter()
            .map(DecisionScope::display_path)
            .chain(resolution.unrooted().iter().cloned())
            .collect();
        accounted.sort();
        let mut recorded = resolution.recorded().to_vec();
        recorded.sort();
        recorded.dedup();
        assert_eq!(accounted, recorded);
    }

    /// A5 — a parked drive never makes a manifest refuse. Suspension closes a
    /// door on attention; it does not unmake the root a recorded prefix names.
    #[test]
    fn a_prefix_on_a_suspended_root_stays_rooted() {
        let roots = vec![suspended_root(1, "/vol/work")];
        let resolution = attributed_dirs(&owned(&["/vol/work/proj-v1"]), &roots);

        assert!(resolution.unrooted().is_empty());
        assert_eq!(
            resolution.scopes(),
            [DecisionScope::new(
                1,
                "/vol/work".to_string(),
                "proj-v1".to_string()
            )]
        );
    }

    /// A6 — the recorder's projection is sorted and deduplicated, so the same
    /// recorded scope written in a different order, or written twice, records
    /// identically. `decompose`'s existing contract, kept.
    #[test]
    fn repeated_resolution_records_identically() {
        let roots = vec![root(1, "/vol/work"), root(2, "/media/backup")];
        let one = attributed_dirs(
            &owned(&["/media/backup/b", "/vol/work/a", "/vol/work/a"]),
            &roots,
        );
        let other = attributed_dirs(&owned(&["/vol/work/a", "/media/backup/b"]), &roots);

        assert_eq!(one.scopes(), other.scopes());
        assert_eq!(one.scopes().len(), 2, "the repeat collapsed");
    }

    /// A8 — the selection register, in all three of its states.
    ///
    /// The middle one is the whole reason this returns an `Option`: a run that
    /// recorded no scope is **global**, and a run whose recorded scope
    /// confirmed nothing must select **nothing**. Both are "empty" to a
    /// careless reader, and downstream an empty scope list means global — so a
    /// manifest naming a drive that is not plugged in would archive the whole
    /// universe if the two collapsed into one value.
    #[test]
    fn the_selection_register_tells_global_apart_from_confirmed_nothing() {
        let roots = vec![root(1, "/vol/work")];

        // Nothing recorded: global, and an empty list is the right shape.
        assert_eq!(
            attributed_dirs(&owned(&[]), &roots).selection(),
            Some(vec![])
        );

        // Recorded, and confirmed: the confirmed scopes as absolute paths.
        assert_eq!(
            attributed_dirs(&owned(&["/vol/work/proj-v1"]), &roots).selection(),
            Some(vec!["/vol/work/proj-v1".to_string()])
        );

        // Recorded, and nothing confirmed: no selection at all, which is a
        // different answer from an empty one.
        assert_eq!(
            attributed_dirs(&owned(&["/vol/gone"]), &roots).selection(),
            None
        );
    }

    /// A9 — the selection is the confirmed register and nothing else, so a
    /// line that contributes nothing to the measurement contributes nothing to
    /// what the run gathers either.
    ///
    /// The case that matters is an unrooted prefix which is an **ancestor** of
    /// a known root: `path_is_under` matches it where `find_containing_root`
    /// does not, so selecting through the recorded list would gather sources
    /// no vantage can measure.
    #[test]
    fn the_selection_never_carries_a_line_that_measures_nothing() {
        let roots = vec![root(1, "/vol/work")];
        let resolution = attributed_dirs(&owned(&["/vol/work/proj-v1", "/vol"]), &roots);

        assert_eq!(resolution.unrooted(), ["/vol"]);
        assert_eq!(
            resolution.recorded(),
            ["/vol/work/proj-v1", "/vol"],
            "the user's own line is still written back"
        );
        assert_eq!(
            resolution.selection(),
            Some(vec!["/vol/work/proj-v1".to_string()]),
            "the ancestor must not reach the selection"
        );
    }

    /// The measurement register in one place: what each grain contributes, and
    /// what nothing contributes.
    ///
    /// A directory scope measures from itself. An item scope measures from the
    /// directory containing it — which is what *the deepest directory
    /// containing every scope* already says about a file, not a second rule.
    /// A root-level item lands on the root, and an item at an empty
    /// remainder — unreachable from the door, which calls a root a directory
    /// without asking, but constructible here — lands on the root too rather
    /// than panicking, because `Path::new("").parent()` is `None` and the
    /// default is written out.
    #[test]
    fn the_measurement_register_reads_each_grain() {
        let cases = [
            ("dir", ScopeGrain::Directory, "/R/dir"),
            ("dir/a.jpg", ScopeGrain::Item, "/R/dir"),
            ("a/b/c/photo.jpg", ScopeGrain::Item, "/R/a/b/c"),
            ("a.jpg", ScopeGrain::Item, "/R"),
            ("", ScopeGrain::Directory, "/R"),
            ("", ScopeGrain::Item, "/R"),
        ];
        for (rel, grain, expected) in cases {
            let resolution = ScopeResolution::from_outcomes(vec![PrefixOutcome::Confirmed(
                DecisionScope::new(1, "/R".to_string(), rel.to_string()),
                grain,
            )]);
            let measured: Vec<String> = resolution
                .measured_from()
                .iter()
                .map(|m| m.location())
                .collect();
            assert_eq!(measured, [expected], "for ({rel:?}, {grain:?})");
            assert_eq!(
                resolution.measured_from()[0].root_path(),
                "/R",
                "for ({rel:?}, {grain:?})"
            );
        }
    }

    /// The constraint-2 pin: **the grain moves the measurement and nothing
    /// else.**
    ///
    /// An item scope appears in `scopes()`, `recorded()` and `selection()` as
    /// the path the user named, byte for byte — so selection, the lock header
    /// and the decision record see today exactly what they saw before. Only
    /// `measured_from()` reads the grain, and it is the only register that
    /// differs between these two resolutions.
    #[test]
    fn the_grain_moves_the_measurement_and_no_other_register() {
        let scope = DecisionScope::new(1, "/R".to_string(), "dir/a.jpg".to_string());
        let as_dir = ScopeResolution::from_outcomes(vec![PrefixOutcome::Confirmed(
            scope.clone(),
            ScopeGrain::Directory,
        )]);
        let as_item = ScopeResolution::from_outcomes(vec![PrefixOutcome::Confirmed(
            scope.clone(),
            ScopeGrain::Item,
        )]);

        assert_eq!(as_item.scopes(), [scope], "selection's register is unmoved");
        assert_eq!(as_dir.scopes(), as_item.scopes());
        assert_eq!(as_dir.recorded(), as_item.recorded());
        assert_eq!(as_dir.selection(), as_item.selection());
        assert_eq!(as_item.selection(), Some(vec!["/R/dir/a.jpg".to_string()]));

        assert_ne!(
            as_dir.measured_from(),
            as_item.measured_from(),
            "and the measurement is the one register that does move"
        );
    }

    /// A prefix that reaches no confirmation contributes nothing to measure
    /// from — the same absence that keeps it out of `scopes()`, seen at the
    /// fifth register. The mirror of `a_set_aside_scope_contributes_no_vantage`
    /// on this side of the boundary.
    #[test]
    fn a_set_aside_or_unrooted_prefix_contributes_nothing_to_measure_from() {
        let resolution = ScopeResolution::from_outcomes(vec![
            PrefixOutcome::SetAside(DecisionScope::new(1, "/R".to_string(), "gone".to_string())),
            PrefixOutcome::Unrooted("/elsewhere".to_string()),
        ]);
        assert!(resolution.measured_from().is_empty());
        assert!(resolution.scopes().is_empty());
    }

    /// A7 — an unrooted prefix is read back to a user against their own file,
    /// so it keeps the order that file wrote it in.
    #[test]
    fn an_unrooted_prefix_keeps_the_manifest_order() {
        let roots = vec![root(1, "/vol/work")];
        let resolution = attributed_dirs(
            &owned(&["/zeta/last", "/vol/work/kept", "/alpha/first"]),
            &roots,
        );

        assert_eq!(resolution.unrooted(), ["/zeta/last", "/alpha/first"]);
    }
}

#[cfg(test)]
mod decision_scope_tests {
    use super::*;

    fn root(id: i64, path: &str) -> Root {
        Root {
            id,
            path: path.to_string(),
            role: "source".to_string(),
            comment: None,
            last_scanned_at: None,
            suspended: false,
        }
    }

    #[test]
    fn display_path_whole_root_is_root_path() {
        let s = DecisionScope::new(1, "/vol/photos".to_string(), String::new());
        assert_eq!(s.display_path(), "/vol/photos");
    }

    #[test]
    fn display_path_sub_prefix_is_joined() {
        let s = DecisionScope::new(1, "/vol/photos".to_string(), "2016/italy".to_string());
        assert_eq!(s.display_path(), "/vol/photos/2016/italy");
    }

    #[test]
    fn index_row_round_trips() {
        let s = DecisionScope::new(7, "/r".to_string(), "sub".to_string());
        assert_eq!(s.index_row(), (7, "/r".to_string(), "sub".to_string()));
    }

    #[test]
    fn decompose_roots_a_whole_root_and_a_sub_prefix() {
        let roots = vec![root(1, "/vol/photos")];
        let scopes = DecisionScope::decompose(
            &["/vol/photos".to_string(), "/vol/photos/2016".to_string()],
            &roots,
        );
        assert_eq!(
            scopes,
            vec![
                DecisionScope::new(1, "/vol/photos".to_string(), String::new()),
                DecisionScope::new(1, "/vol/photos".to_string(), "2016".to_string()),
            ]
        );
    }

    #[test]
    fn decompose_is_segment_safe() {
        // /a/bc is not under /a/b — must not match the wrong root.
        let roots = vec![root(1, "/a/b")];
        let scopes = DecisionScope::decompose(&["/a/bc".to_string()], &roots);
        assert!(scopes.is_empty(), "got: {scopes:?}");
    }

    #[test]
    fn decompose_drops_prefix_under_no_root() {
        let roots = vec![root(1, "/vol/photos")];
        let scopes = DecisionScope::decompose(&["/elsewhere".to_string()], &roots);
        assert!(scopes.is_empty());
    }

    #[test]
    fn decompose_dedups_prefixes_resolving_to_same_pair() {
        let roots = vec![root(1, "/vol/photos")];
        let scopes = DecisionScope::decompose(
            &[
                "/vol/photos/2016".to_string(),
                "/vol/photos/2016".to_string(),
            ],
            &roots,
        );
        assert_eq!(scopes.len(), 1);
        assert_eq!(
            scopes[0].index_row(),
            (1, "/vol/photos".to_string(), "2016".to_string())
        );
    }

    // recover_root_path — NULL-over-guess recovery for pre-snapshot rows

    #[test]
    fn recover_is_the_inverse_of_display_path() {
        // The law that keeps the pair honest: for any scope, recovering from its
        // own display path with its own rel_prefix yields its root path.
        for (root_path, rel_prefix) in [
            ("/vol/photos", ""),
            ("/vol/photos", "2016"),
            ("/vol/photos", "2016/italy"),
            ("/r", "a/b/c"),
        ] {
            let s = DecisionScope::new(1, root_path.to_string(), rel_prefix.to_string());
            assert_eq!(
                recover_root_path(&[s.display_path()], &s.rel_prefix),
                Some(s.root_path.clone()),
                "failed for ({root_path}, {rel_prefix})"
            );
        }
    }

    #[test]
    fn recover_strips_known_suffix_at_path_boundary() {
        let candidates = vec!["/vol/photos/2016/italy".to_string()];
        assert_eq!(
            recover_root_path(&candidates, "2016/italy"),
            Some("/vol/photos".to_string())
        );
    }

    #[test]
    fn recover_empty_prefix_needs_single_candidate() {
        let one = vec!["/vol/photos".to_string()];
        assert_eq!(recover_root_path(&one, ""), Some("/vol/photos".to_string()));
        let two = vec!["/vol/photos".to_string(), "/vol/music".to_string()];
        assert_eq!(recover_root_path(&two, ""), None);
    }

    #[test]
    fn recover_ambiguous_suffix_is_none() {
        let candidates = vec!["/a/sub".to_string(), "/b/sub".to_string()];
        assert_eq!(recover_root_path(&candidates, "sub"), None);
    }

    #[test]
    fn recover_no_match_is_none() {
        let candidates = vec!["/vol/photos/2016".to_string()];
        assert_eq!(recover_root_path(&candidates, "2017"), None);
    }

    #[test]
    fn recover_requires_path_boundary_not_substring() {
        // "/a/bc" must not recover for prefix "c" — suffix match is
        // boundary-safe by construction ("/c" is not a suffix of "/a/bc"),
        // and a candidate equal to "/" + prefix leaves an empty root.
        let candidates = vec!["/a/bc".to_string()];
        assert_eq!(recover_root_path(&candidates, "c"), None);
        let root_is_empty = vec!["/sub".to_string()];
        assert_eq!(recover_root_path(&root_is_empty, "sub"), None);
    }

    #[test]
    fn decompose_empty_input_is_empty() {
        let roots = vec![root(1, "/vol/photos")];
        assert!(DecisionScope::decompose(&[], &roots).is_empty());
    }

    #[test]
    fn no_input_string_yields_a_relative_display() {
        // Intent guard: whatever a caller passes, a resulting DecisionScope's
        // display is always the root's canonical path (or a canonical join) —
        // never "." or a relative fragment. Rootless strings are simply dropped.
        let roots = vec![root(1, "/vol/photos")];
        for bad in [".", "..", "../x", "photos", "2016/italy"] {
            let scopes = DecisionScope::decompose(&[bad.to_string()], &roots);
            for s in &scopes {
                assert!(
                    s.display_path().starts_with('/'),
                    "display_path {:?} is not absolute",
                    s.display_path()
                );
            }
        }
    }
}
