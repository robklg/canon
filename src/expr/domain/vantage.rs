//! The place a `{scope.rel_path}` measures from.

use std::collections::HashMap;

use crate::core::domain::path::common_path_prefix;
use crate::core::domain::scope::ScopeResolution;

/// The place a `{scope.rel_path}` measures from, one per root.
///
/// One scope: the scope itself. Several: the deepest directory containing
/// every scope that lies in the source's own root — so each scope's own name
/// survives at the destination and siblings cannot collide. Grouping per root
/// is what keeps the vantage from climbing above a root when a manifest spans
/// several; it needs no clamp, because every scope in a root is under it, so
/// the group's common prefix is always at or below the root.
///
/// Derived once, here. Every consumer takes the derived value: what "the
/// scope" means when there is more than one is not re-derived per reader,
/// which is exactly what this replaces.
///
/// Pure and immutable — built once per run from the prefixes and the
/// root-path cache the caller already holds, then borrowed per source.
pub struct ScopeVantage {
    /// Root path → the vantage for sources in that root.
    by_root: HashMap<String, String>,
}

impl ScopeVantage {
    /// Derive the vantage for each root the recorded scope names.
    ///
    /// Takes an already-attributed scope. Which root owns a prefix is the path
    /// law's question and is answered once, in `core`, before this is called —
    /// so a prefix that named no root cannot reach here to be silently skipped,
    /// and passing raw manifest text is a compile error rather than a guess.
    /// What is left is this type's own law and nothing else: the deepest
    /// directory containing every scope that lies in one root.
    pub fn new(scope: &ScopeResolution) -> Self {
        let mut grouped: HashMap<&str, Vec<String>> = HashMap::new();
        for s in scope.scopes() {
            grouped
                .entry(s.root_path.as_str())
                .or_default()
                .push(s.display_path());
        }

        let by_root = grouped
            .into_iter()
            .map(|(root, paths)| {
                (
                    root.to_string(),
                    common_path_prefix(paths.iter().map(String::as_str)),
                )
            })
            .collect();

        ScopeVantage { by_root }
    }

    /// The vantage for a source in this root, or `None` when the recorded
    /// scope names no path in it.
    pub fn for_root(&self, root_path: &str) -> Option<&str> {
        self.by_root.get(root_path).map(|v| v.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::domain::root::Root;
    use crate::core::domain::scope::{attribute_prefix, PrefixOutcome};

    /// The vantage a manifest recording `prefixes` yields against `roots` —
    /// built the way production builds it, through the one resolution, so the
    /// fixture cannot drift from what a run actually hands this type.
    fn v(prefixes: &[&str], roots: &[&str]) -> ScopeVantage {
        let owned: Vec<String> = prefixes.iter().map(|p| p.to_string()).collect();
        let roots: Vec<Root> = roots
            .iter()
            .enumerate()
            .map(|(i, path)| Root {
                id: i as i64 + 1,
                path: path.to_string(),
                role: "source".to_string(),
                comment: None,
                last_scanned_at: None,
                suspended: false,
            })
            .collect();
        ScopeVantage::new(&ScopeResolution::from_outcomes(
            owned
                .iter()
                .map(|p| match attribute_prefix(p, &roots) {
                    Some(scope) => PrefixOutcome::Confirmed(scope),
                    None => PrefixOutcome::Unrooted(p.clone()),
                })
                .collect(),
        ))
    }

    /// V1 — the no-regression guard at the law's own level: with one scope the
    /// vantage is that scope, so every single-scope manifest is unchanged.
    #[test]
    fn a_single_scope_is_its_own_vantage() {
        let vantage = v(&["/vol/work/proj-v1"], &["/vol/work"]);
        assert_eq!(vantage.for_root("/vol/work"), Some("/vol/work/proj-v1"));
    }

    /// V2 — the friction's own shape: naming siblings measures from their
    /// shared parent, so each scope's own name survives at the destination.
    #[test]
    fn sibling_scopes_measure_from_their_shared_parent() {
        let vantage = v(&["/vol/work/proj-v1", "/vol/work/proj-v2"], &["/vol/work"]);
        assert_eq!(vantage.for_root("/vol/work"), Some("/vol/work"));
    }

    /// V3 — no pooling across roots: each root's vantage is derived from the
    /// scopes that lie in it and from nothing else.
    #[test]
    fn scopes_in_different_roots_each_get_their_own_vantage() {
        let vantage = v(
            &["/vol/work/proj-v1", "/media/backup/proj-v1"],
            &["/vol/work", "/media/backup"],
        );
        assert_eq!(vantage.for_root("/vol/work"), Some("/vol/work/proj-v1"));
        assert_eq!(
            vantage.for_root("/media/backup"),
            Some("/media/backup/proj-v1")
        );
    }

    /// V4 — the vantage cannot climb above its root, however far apart the
    /// scopes inside it lie. It falls out of the per-root grouping; there is
    /// no clamp to remove.
    #[test]
    fn the_vantage_never_rises_above_its_root() {
        let vantage = v(&["/vol/work", "/vol/work/deep/inside/here"], &["/vol/work"]);
        assert_eq!(vantage.for_root("/vol/work"), Some("/vol/work"));
    }

    /// V5 — a scope that is the root itself yields the root, so a source
    /// renders its whole root-relative path.
    #[test]
    fn a_scope_that_is_the_root_yields_the_root() {
        let vantage = v(&["/vol/work"], &["/vol/work"]);
        assert_eq!(vantage.for_root("/vol/work"), Some("/vol/work"));
    }

    /// V6 — a root the scope never names has no vantage. This is what the
    /// named refusal at evaluation time is built on.
    #[test]
    fn a_root_the_scope_never_names_has_no_vantage() {
        let vantage = v(&["/vol/work/proj-v1"], &["/vol/work", "/media/backup"]);
        assert_eq!(vantage.for_root("/media/backup"), None);
    }

    /// The vantage side's twin of the byte-prefix pin at evaluation, and the
    /// only test here that separates componentwise containment from byte-wise.
    ///
    /// `/vol/work/proj` is a byte prefix of `/vol/work/proj-v2` but contains
    /// nothing of it, so any grouping that decides containment by bytes reads
    /// the first scope as the outer one and answers `/vol/work/proj` — a
    /// plausible path that neither scope names. Sibling scopes whose names
    /// merely *share* a prefix cannot catch that: byte-wise containment finds
    /// no container among them and falls through to the right answer, which is
    /// why they pass against it and this does not.
    #[test]
    fn a_scope_whose_name_prefixes_another_does_not_merge_with_it() {
        let vantage = v(&["/vol/work/proj", "/vol/work/proj-v2"], &["/vol/work"]);
        assert_eq!(vantage.for_root("/vol/work"), Some("/vol/work"));
    }

    /// V8 — the behavioural half of the manifest door's fourth partition, and
    /// the shape the below-root form mismatch used to take: a recorded prefix
    /// the index cannot confirm contributes nothing to the measurement, so the
    /// sibling that *is* confirmed measures from itself rather than from a
    /// common prefix dragged above the two of them.
    ///
    /// Built through `from_outcomes` rather than through a database: which
    /// prefixes were confirmed is `core::ops::scope`'s answer, and what this
    /// type does with the answer is the only thing under test here.
    #[test]
    fn a_set_aside_scope_contributes_no_vantage() {
        use crate::core::domain::scope::{DecisionScope, PrefixOutcome};

        let confirmed = DecisionScope::new(1, "/vol/work".to_string(), "proj-v1".to_string());
        let set_aside = DecisionScope::new(1, "/vol/work".to_string(), "proj-v2".to_string());

        let both = ScopeVantage::new(&ScopeResolution::from_outcomes(vec![
            PrefixOutcome::Confirmed(confirmed.clone()),
            PrefixOutcome::Confirmed(set_aside.clone()),
        ]));
        assert_eq!(
            both.for_root("/vol/work"),
            Some("/vol/work"),
            "two confirmed siblings measure from their shared parent"
        );

        let one_aside = ScopeVantage::new(&ScopeResolution::from_outcomes(vec![
            PrefixOutcome::Confirmed(confirmed),
            PrefixOutcome::SetAside(set_aside),
        ]));
        assert_eq!(
            one_aside.for_root("/vol/work"),
            Some("/vol/work/proj-v1"),
            "a set-aside sibling must not drag the vantage above the survivor"
        );
    }

    #[test]
    fn no_scope_at_all_yields_no_vantage() {
        let vantage = v(&[], &["/vol/work"]);
        assert_eq!(vantage.for_root("/vol/work"), None);
    }
}
