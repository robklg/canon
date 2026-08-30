//! Scope domain concepts for canon.
//!
//! This module defines how path scopes work — the domain knowledge of
//! "what kind of match do we want?" separated from the SQL implementation
//! of "how do we express this in a query?".

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

/// A recorded scope resolved against the known roots — once, for the whole run.
///
/// A manifest's scope is user-editable text, so it arrives in whatever form
/// and whatever state the user left it: a path retyped in another
/// normalization, a folder since moved, a root since removed. That is one
/// question with one answer, and it used to be answered separately by every
/// reader — the vantage matched roots its own way, the recorder matched them
/// another, and each lost a prefix in its own silence.
///
/// Form-tolerant on the way in: each prefix is matched through its
/// normalization candidates, so a root stored in the disk's form is reachable
/// from a prefix typed in the other one — the same bend path resolution
/// performs at the argument door, not a second rule.
///
/// Two things the argument door does that this does not, both deliberate and
/// both limits rather than features. It does not lexically clean the prefix
/// (`resolve_path` calls `clean_path` first), so a hand-written `..` is
/// matched literally. And it has no second-stage retry over the below-root
/// remainder: candidates are of the **whole** prefix, and the first that finds
/// a root wins, so the form that matched the root is the form the whole prefix
/// is recorded in, remainder included.
///
/// The consequence worth knowing: where a root's own path matches as typed —
/// an ASCII root always does — no candidate but as-given is ever tried, and
/// each prefix keeps whatever form its remainder was typed in. Two prefixes
/// under such a root can then disagree below it, and where they do the
/// vantage's common prefix climbs above the differing component and files land
/// a level out. Pre-existing, out of this type's reach, and routed.
///
/// Honest on the way out: a prefix matching no root is carried, never dropped.
/// Dropping one silently narrows whatever the reader does next — the vantage
/// measures from somewhere deeper, the recorder writes a scoped act down as a
/// global one — and a narrowing nobody stated is the class this closes. What
/// to do about a carried prefix is the consumer's own answer; this type
/// classifies and never decides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeResolution {
    /// Sorted and deduplicated: the recorder's projection, so repeated runs
    /// record identically.
    resolved: Vec<DecisionScope>,
    /// In the manifest's own order, first occurrence only — this is read back
    /// to a user against their own file.
    unrooted: Vec<String>,
    /// Every recorded prefix in the manifest's own order: resolved ones in the
    /// byte-form the index stores, unresolved ones verbatim. What a rewrite
    /// writes back.
    recorded: Vec<String>,
}

impl ScopeResolution {
    /// Resolve a recorded scope against the known roots.
    ///
    /// Infallible by design: it classifies, it never fails. Every failure mode
    /// is a caller's disposition — a resolution that could itself error would
    /// hand callers a fourth thing to answer inconsistently, which is the
    /// defect wearing a fix.
    ///
    /// Suspension is not consulted. A parked root is still a known root, so a
    /// manifest naming one resolves exactly as it did before the door closed.
    pub fn resolve(prefixes: &[String], roots: &[Root]) -> Self {
        let mut resolved: Vec<DecisionScope> = Vec::new();
        let mut unrooted: Vec<String> = Vec::new();
        let mut recorded: Vec<String> = Vec::with_capacity(prefixes.len());

        for prefix in prefixes {
            let hit = normalization_candidates(prefix).into_iter().find_map(|c| {
                find_containing_root(&c, roots).map(|(root_id, root_path, _role, rel)| {
                    DecisionScope::new(root_id, root_path, rel)
                })
            });
            match hit {
                Some(scope) => {
                    recorded.push(scope.display_path());
                    resolved.push(scope);
                }
                None => {
                    recorded.push(prefix.clone());
                    if !unrooted.contains(prefix) {
                        unrooted.push(prefix.clone());
                    }
                }
            }
        }

        resolved.sort();
        resolved.dedup();

        ScopeResolution {
            resolved,
            unrooted,
            recorded,
        }
    }

    /// The rooted prefixes as typed scopes — what the recorder stores.
    pub fn scopes(&self) -> &[DecisionScope] {
        &self.resolved
    }

    /// The prefixes that match no known root, in the manifest's own order.
    pub fn unrooted(&self) -> &[String] {
        &self.unrooted
    }

    /// Every recorded prefix, resolved ones healed to the stored byte-form and
    /// unresolved ones verbatim — what a rewrite writes back.
    pub fn recorded(&self) -> &[String] {
        &self.recorded
    }

    /// Whether the manifest recorded any scope at all — a different question
    /// from whether any of it resolved.
    pub fn is_empty(&self) -> bool {
        self.recorded.is_empty()
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
        let resolution = ScopeResolution::resolve(&owned(&[&format!("{ROOT_NFC}/2016")]), &roots);

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
        let resolution = ScopeResolution::resolve(&owned(&["/vol/gone/proj-v2"]), &roots);

        assert_eq!(resolution.unrooted(), ["/vol/gone/proj-v2"]);
        assert!(resolution.scopes().is_empty());
        assert!(!resolution.is_empty(), "a scope was recorded");
    }

    /// A3 — the compound pin, and the configuration the friction is made of:
    /// two prefixes the user wrote as siblings, one of which no longer names a
    /// known root. Neither hides the other — the survivor does not stand in
    /// for the whole recorded scope, and the casualty does not suppress the
    /// survivor.
    #[test]
    fn a_carried_prefix_does_not_displace_its_rooted_siblings() {
        let roots = vec![root(1, "/vol/work")];
        let resolution = ScopeResolution::resolve(
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
        let resolution = ScopeResolution::resolve(&prefixes, &roots);

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
        let resolution = ScopeResolution::resolve(&owned(&["/vol/work/proj-v1"]), &roots);

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
        let one = ScopeResolution::resolve(
            &owned(&["/media/backup/b", "/vol/work/a", "/vol/work/a"]),
            &roots,
        );
        let other = ScopeResolution::resolve(&owned(&["/vol/work/a", "/media/backup/b"]), &roots);

        assert_eq!(one.scopes(), other.scopes());
        assert_eq!(one.scopes().len(), 2, "the repeat collapsed");
    }

    /// A7 — an unrooted prefix is read back to a user against their own file,
    /// so it keeps the order that file wrote it in.
    #[test]
    fn an_unrooted_prefix_keeps_the_manifest_order() {
        let roots = vec![root(1, "/vol/work")];
        let resolution = ScopeResolution::resolve(
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
