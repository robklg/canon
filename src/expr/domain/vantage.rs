//! The place a `{scope.rel_path}` measures from.

use std::collections::HashMap;

use crate::core::domain::path::common_path_prefix;
use crate::core::domain::scope::DirectoryLocation;

/// The place a `{scope.rel_path}` measures from, one per root.
///
/// The deepest directory containing every scope that lies in the source's own
/// root — so each scope's own name survives at the destination and siblings
/// cannot collide. One directory scope is therefore its own vantage, and one
/// file scope is the directory it sits in. Grouping per root is what keeps the
/// vantage from climbing above a root when a manifest spans several; it needs
/// no clamp, because every scope in a root is under it, so the group's common
/// prefix is always at or below the root.
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
    /// Derive the vantage for each root the measured-from register names.
    ///
    /// Takes places to measure from, already resolved. Two questions this type
    /// must not answer for itself are shut at the signature rather than left
    /// to a test: which root owns a prefix is the path law's, answered once in
    /// `core` — so raw manifest text cannot be folded — and whether a scope
    /// names a directory or one item is the index's, answered once at the
    /// manifest door, so an item path cannot be folded either. Fold an item
    /// path and it becomes its own vantage, `path_strip_prefix` yields `""`,
    /// and every entry aims at the destination directory itself.
    ///
    /// What is left is this type's own law and nothing else: the deepest
    /// directory containing every scope that lies in one root.
    pub fn new(points: &[DirectoryLocation]) -> Self {
        let mut grouped: HashMap<&str, Vec<String>> = HashMap::new();
        for point in points {
            grouped
                .entry(point.root_path())
                .or_default()
                .push(point.location());
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
    use crate::core::domain::scope::{
        attribute_prefix, PrefixOutcome, ScopeGrain, ScopeResolution,
    };

    fn roots_at(paths: &[&str]) -> Vec<Root> {
        paths
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
            .collect()
    }

    /// The vantage a manifest recording `prefixes` yields against `roots`,
    /// **every prefix a directory** — built the way production builds it,
    /// through the one resolution, so the fixture cannot drift from what a run
    /// actually hands this type.
    ///
    /// The grain is in the name because it is a premise the index supplies and
    /// this helper cannot: a helper that defaulted it silently would let the
    /// item cases below pass without ever exercising an item.
    fn v(prefixes: &[&str], roots: &[&str]) -> ScopeVantage {
        v_grained(
            &prefixes
                .iter()
                .map(|p| (*p, ScopeGrain::Directory))
                .collect::<Vec<_>>(),
            roots,
        )
    }

    /// The same, with each prefix's grain stated.
    fn v_grained(prefixes: &[(&str, ScopeGrain)], roots: &[&str]) -> ScopeVantage {
        let roots = roots_at(roots);
        let resolution = ScopeResolution::from_outcomes(
            prefixes
                .iter()
                .map(|(p, grain)| match attribute_prefix(p, &roots) {
                    Some(scope) => PrefixOutcome::Confirmed(scope, *grain),
                    None => PrefixOutcome::Unrooted(p.to_string()),
                })
                .collect(),
        );
        ScopeVantage::new(resolution.measured_from())
    }

    /// V1 — the no-regression guard at the law's own level: with one scope
    /// naming a directory the vantage is that scope, so every single-scope
    /// manifest over a folder is unchanged. (A single *file* scope is the
    /// table's own last row, and measures from its parent.)
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
        use crate::core::domain::scope::DecisionScope;

        let confirmed = DecisionScope::new(1, "/vol/work".to_string(), "proj-v1".to_string());
        let set_aside = DecisionScope::new(1, "/vol/work".to_string(), "proj-v2".to_string());

        let both = ScopeResolution::from_outcomes(vec![
            PrefixOutcome::Confirmed(confirmed.clone(), ScopeGrain::Directory),
            PrefixOutcome::Confirmed(set_aside.clone(), ScopeGrain::Directory),
        ]);
        assert_eq!(
            ScopeVantage::new(both.measured_from()).for_root("/vol/work"),
            Some("/vol/work"),
            "two confirmed siblings measure from their shared parent"
        );

        let one_aside = ScopeResolution::from_outcomes(vec![
            PrefixOutcome::Confirmed(confirmed, ScopeGrain::Directory),
            PrefixOutcome::SetAside(set_aside),
        ]);
        assert_eq!(
            ScopeVantage::new(one_aside.measured_from()).for_root("/vol/work"),
            Some("/vol/work/proj-v1"),
            "a set-aside sibling must not drag the vantage above the survivor"
        );
    }

    #[test]
    fn no_scope_at_all_yields_no_vantage() {
        let vantage = v(&[], &["/vol/work"]);
        assert_eq!(vantage.for_root("/vol/work"), None);
    }

    /// One row of the table: what the manifest named, and where it measures
    /// from.
    struct Row<'a> {
        case: &'a str,
        scopes: Vec<(&'a str, ScopeGrain)>,
        vantage: &'a str,
    }

    /// The neighbour table — one scope's grain read beside the neighbours that
    /// make a wrong answer obvious.
    ///
    /// The degenerate row is the last one, and it is the friction: a wildcard
    /// that matched a dozen files places them by name, and the same wildcard on
    /// a sparse month matched one file and placed it nowhere. It is in a table
    /// rather than alone because that is the shape of the defect — the single
    /// case is only visibly wrong *next to* the case one argument wider, which
    /// is what the fix-time neighbour walk does by hand and this makes
    /// permanent. Every row is answered by one rule: the deepest **directory**
    /// containing every scope.
    #[test]
    fn the_grain_table_answers_every_neighbour_by_one_rule() {
        use ScopeGrain::{Directory, Item};

        let dozen: Vec<(&str, ScopeGrain)> = DOZEN.iter().map(|p| (*p, Item)).collect();

        let table = [
            Row {
                case: "a directory scope",
                scopes: vec![("/R/dir", Directory)],
                vantage: "/R/dir",
            },
            Row {
                case: "two item scopes in one directory",
                scopes: vec![("/R/dir/a.jpg", Item), ("/R/dir/b.jpg", Item)],
                vantage: "/R/dir",
            },
            Row {
                case: "a dozen items in one directory — the glob that worked",
                scopes: dozen,
                vantage: "/R/dir",
            },
            Row {
                case: "items in different directories",
                scopes: vec![("/R/a/x.jpg", Item), ("/R/b/y.jpg", Item)],
                vantage: "/R",
            },
            Row {
                case: "a directory beside an item elsewhere",
                scopes: vec![("/R/dir", Directory), ("/R/other/x.jpg", Item)],
                vantage: "/R",
            },
            Row {
                case: "one item scope alone — the friction",
                scopes: vec![("/R/dir/a.jpg", Item)],
                vantage: "/R/dir",
            },
        ];

        for row in &table {
            assert_eq!(
                v_grained(&row.scopes, &["/R"]).for_root("/R"),
                Some(row.vantage),
                "{}",
                row.case
            );
        }
    }

    /// Twelve files in one directory — the month the wildcard matched a dozen
    /// of, spelled out rather than generated so the table above reads as a
    /// table.
    const DOZEN: [&str; 12] = [
        "/R/dir/00.jpg",
        "/R/dir/01.jpg",
        "/R/dir/02.jpg",
        "/R/dir/03.jpg",
        "/R/dir/04.jpg",
        "/R/dir/05.jpg",
        "/R/dir/06.jpg",
        "/R/dir/07.jpg",
        "/R/dir/08.jpg",
        "/R/dir/09.jpg",
        "/R/dir/10.jpg",
        "/R/dir/11.jpg",
    ];

    /// An item scope directly under its root measures from the root, so the
    /// file renders as its own name. The row above the table's first: there is
    /// no directory between it and the root to lose.
    #[test]
    fn a_root_level_item_measures_from_its_root() {
        let vantage = v_grained(&[("/R/a.jpg", ScopeGrain::Item)], &["/R"]);
        assert_eq!(vantage.for_root("/R"), Some("/R"));
    }
}
