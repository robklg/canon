//! Scope domain concepts for canon.
//!
//! This module defines how path scopes work — the domain knowledge of
//! "what kind of match do we want?" separated from the SQL implementation
//! of "how do we express this in a query?".

use std::path::Path;

use super::root::{find_containing_root, Root};

/// Domain concept: what kind of scope match do we want?
///
/// This enum represents the domain decision of whether to match
/// a specific file exactly or all files under a directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopeMatch {
    /// Match a specific file exactly
    ExactFile(String),
    /// Match all files under a directory
    UnderDirectory(String),
}

impl ScopeMatch {
    /// Classify a canonicalized path as file or directory scope.
    ///
    /// This performs filesystem I/O to determine if the path is a file.
    pub fn classify(path: &str) -> Self {
        if Path::new(path).is_file() {
            ScopeMatch::ExactFile(path.to_string())
        } else {
            ScopeMatch::UnderDirectory(path.to_string())
        }
    }

    /// Classify multiple canonicalized paths.
    pub fn classify_all(paths: &[String]) -> Vec<Self> {
        paths.iter().map(|p| Self::classify(p)).collect()
    }
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

    /// The `(root_id, rel_prefix)` pair for the `decision_scopes` index.
    pub fn index_pair(&self) -> (i64, String) {
        (self.root_id, self.rel_prefix.clone())
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
    fn index_pair_round_trips() {
        let s = DecisionScope::new(7, "/r".to_string(), "sub".to_string());
        assert_eq!(s.index_pair(), (7, "sub".to_string()));
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
        assert_eq!(scopes[0].index_pair(), (1, "2016".to_string()));
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
