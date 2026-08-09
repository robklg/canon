//! Location aggregation — the one "where" derivation, shared by act
//! destinations and covered-copy locations.
//!
//! No I/O anywhere here; callers supply everything fetched.

use crate::core::domain::folder_tree::FolderTree;
use crate::domain::path::path_is_under;

/// One location entry on a "where" line: a directory prefix and how many of
/// the line's files it accounts for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocationCount {
    pub path: String,
    pub files: i64,
}

/// A set of locations aggregated to the fewest legible prefixes, capped with
/// a counted remainder — never a silent truncation.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LocationAggregate {
    pub locations: Vec<LocationCount>,
    /// Location groups beyond the cap ("… and N more locations").
    pub omitted_locations: usize,
}

impl LocationAggregate {
    pub fn is_empty(&self) -> bool {
        self.locations.is_empty()
    }

    /// The location paths alone — the merge identity for act grouping.
    pub(super) fn paths(&self) -> Vec<String> {
        self.locations.iter().map(|l| l.path.clone()).collect()
    }
}

/// Aggregate a set of directories (with per-directory file counts) to the
/// fewest legible prefixes.
///
/// `bases` are the path prefixes below which an answer starts to say
/// something (archive root paths for covered locations, known root paths for
/// act destinations): a prefix at or above every base is vacuous — "in the
/// archive" answers nothing — so the walk splits below it instead.
///
/// The rule: collapse single-branch chains from the top to the deepest
/// common node. If that node is strictly below a base, it alone is the
/// answer (the widest honest one-line where — fan-out beneath it is shape,
/// not information). Otherwise emit its branch groups, each chain-collapsed
/// to where it first branches or holds files, ordered by weight then path,
/// capped with a counted remainder.
pub fn aggregate_locations(dirs: &[(&str, i64)], bases: &[&str], cap: usize) -> LocationAggregate {
    aggregate_impl(dirs, bases, cap, false)
}

/// The observed-scatter variant ("copies stand in"): a legible one-line
/// answer descends one level when its branch groups fit the cap — a huge
/// hub node ("Archive/Media") can hide a short, far more informative list.
/// One step, never recursion. Chosen destinations keep the one-line answer
/// (the arrow states a choice; the scatter is what nobody chose).
pub fn aggregate_locations_expanded(
    dirs: &[(&str, i64)],
    bases: &[&str],
    cap: usize,
) -> LocationAggregate {
    aggregate_impl(dirs, bases, cap, true)
}

fn aggregate_impl(
    dirs: &[(&str, i64)],
    bases: &[&str],
    cap: usize,
    expand: bool,
) -> LocationAggregate {
    if dirs.is_empty() {
        return LocationAggregate::default();
    }

    let mut tree = FolderTree::new();
    let mut direct: Vec<i64> = Vec::new();
    for (dir, files) in dirs {
        let fid = tree.intern(dir) as usize;
        if direct.len() < tree.len() {
            direct.resize(tree.len(), 0);
        }
        direct[fid] += files;
    }
    direct.resize(tree.len(), 0);

    // Subtree sums: ids are parents-first, so one reverse pass suffices.
    let mut sub = direct.clone();
    for fid in (0..tree.len() as u32).rev() {
        if let Some(parent) = tree.parent(fid) {
            sub[parent as usize] += sub[fid as usize];
        }
    }

    let collapse = |mut fid: u32| -> u32 {
        while direct[fid as usize] == 0 && tree.children(fid).len() == 1 {
            fid = tree.children(fid)[0];
        }
        fid
    };

    let top = collapse(0);
    let top_path = tree.path(top);
    let branch_groups = |top: u32| -> Vec<LocationCount> {
        let mut groups: Vec<LocationCount> = Vec::new();
        if direct[top as usize] > 0 {
            groups.push(LocationCount {
                path: tree.path(top).to_string(),
                files: direct[top as usize],
            });
        }
        for &child in tree.children(top) {
            let group = collapse(child);
            groups.push(LocationCount {
                path: tree.path(group).to_string(),
                files: sub[child as usize],
            });
        }
        groups.sort_by(|a, b| b.files.cmp(&a.files).then_with(|| a.path.cmp(&b.path)));
        groups
    };

    let legible = bases
        .iter()
        .any(|base| top_path != *base && path_is_under(top_path, base));
    if legible {
        // The widest honest one-line where — unless the caller asked for
        // the expanded form and the hub's branch groups fit the cap whole,
        // in which case the short list is the more informative answer.
        if expand {
            let groups = branch_groups(top);
            if groups.len() > 1 && groups.len() <= cap {
                return LocationAggregate {
                    locations: groups,
                    omitted_locations: 0,
                };
            }
        }
        return LocationAggregate {
            locations: vec![LocationCount {
                path: top_path.to_string(),
                files: sub[top as usize],
            }],
            omitted_locations: 0,
        };
    }

    // Vacuous common node: emit its branch groups instead.
    let mut groups = branch_groups(top);
    let omitted_locations = groups.len().saturating_sub(cap);
    groups.truncate(cap);

    LocationAggregate {
        locations: groups,
        omitted_locations,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn agg(dirs: &[(&str, i64)], bases: &[&str]) -> LocationAggregate {
        aggregate_locations(dirs, bases, 3)
    }

    #[test]
    fn empty_dirs_aggregate_to_nothing() {
        let out = agg(&[], &["/archive"]);
        assert!(out.is_empty());
        assert_eq!(out.omitted_locations, 0);
    }

    #[test]
    fn single_directory_is_its_own_answer() {
        let out = agg(&[("/archive/media/2016", 640)], &["/archive"]);
        assert_eq!(
            out.locations,
            vec![LocationCount {
                path: "/archive/media/2016".into(),
                files: 640
            }]
        );
    }

    #[test]
    fn coherent_fanout_collapses_to_the_common_prefix() {
        // Twelve month dirs under one year: the year is the answer; the
        // fan-out beneath it is shape, not information.
        let dirs: Vec<(String, i64)> = (1..=12)
            .map(|m| (format!("/archive/media/2016/{m:02}"), 10))
            .collect();
        let refs: Vec<(&str, i64)> = dirs.iter().map(|(d, f)| (d.as_str(), *f)).collect();
        let out = agg(&refs, &["/archive"]);
        assert_eq!(
            out.locations,
            vec![LocationCount {
                path: "/archive/media/2016".into(),
                files: 120
            }]
        );
        assert_eq!(out.omitted_locations, 0);
    }

    #[test]
    fn divergence_at_the_base_lists_the_groups() {
        // The common node is the archive root itself — vacuous ("in the
        // archive" answers nothing) — so the branch groups are the answer.
        let out = agg(
            &[
                ("/archive/staging-2019/worlds", 3401),
                ("/archive/games", 11),
            ],
            &["/archive"],
        );
        assert_eq!(
            out.locations,
            vec![
                LocationCount {
                    path: "/archive/staging-2019/worlds".into(),
                    files: 3401
                },
                LocationCount {
                    path: "/archive/games".into(),
                    files: 11
                },
            ]
        );
    }

    #[test]
    fn groups_beyond_the_cap_are_counted_never_dropped_silently() {
        let dirs: Vec<(String, i64)> = (0..6)
            .map(|i| (format!("/archive/spot-{i}"), 10 + i as i64))
            .collect();
        let refs: Vec<(&str, i64)> = dirs.iter().map(|(d, f)| (d.as_str(), *f)).collect();
        let out = agg(&refs, &["/archive"]);
        assert_eq!(out.locations.len(), 3);
        assert_eq!(out.omitted_locations, 3);
        // Heaviest first.
        assert_eq!(out.locations[0].path, "/archive/spot-5");
    }

    #[test]
    fn ordering_is_weight_then_path() {
        let out = agg(
            &[("/archive/b", 10), ("/archive/a", 10), ("/archive/c", 20)],
            &["/archive"],
        );
        let paths: Vec<&str> = out.locations.iter().map(|l| l.path.as_str()).collect();
        assert_eq!(paths, vec!["/archive/c", "/archive/a", "/archive/b"]);
    }

    #[test]
    fn no_matching_base_splits_at_the_first_branching() {
        // A destination whose root is gone from the fleet: nothing is
        // legible, so the top-level branch groups are the answer.
        let out = agg(
            &[("/old-archive/media", 5), ("/old-archive/docs", 3)],
            &["/archive"],
        );
        let paths: Vec<&str> = out.locations.iter().map(|l| l.path.as_str()).collect();
        assert_eq!(paths, vec!["/old-archive/media", "/old-archive/docs"]);
    }

    #[test]
    fn expanded_form_descends_into_a_hub_when_the_list_is_short() {
        // "copies stand in Archive/Media" hides a two-entry answer; the
        // expanded form lists the branches when they fit the cap whole.
        let dirs = [
            ("/archive/media/2010/june", 280),
            ("/archive/media/2011/spain", 300),
            ("/archive/media/2011/home", 60),
        ];
        let plain = aggregate_locations(&dirs, &["/archive"], 3);
        assert_eq!(plain.locations[0].path, "/archive/media");
        assert_eq!(plain.locations.len(), 1);

        let expanded = aggregate_locations_expanded(&dirs, &["/archive"], 3);
        assert_eq!(
            expanded.locations,
            vec![
                LocationCount {
                    path: "/archive/media/2011".into(),
                    files: 360
                },
                LocationCount {
                    path: "/archive/media/2010/june".into(),
                    files: 280
                },
            ]
        );
        assert_eq!(expanded.omitted_locations, 0);
    }

    #[test]
    fn expanded_form_keeps_the_hub_when_branches_exceed_the_cap() {
        let dirs: Vec<(String, i64)> = (0..5)
            .map(|i| (format!("/archive/media/{}", 2010 + i), 10))
            .collect();
        let refs: Vec<(&str, i64)> = dirs.iter().map(|(d, f)| (d.as_str(), *f)).collect();
        let expanded = aggregate_locations_expanded(&refs, &["/archive"], 3);
        assert_eq!(
            expanded.locations,
            vec![LocationCount {
                path: "/archive/media".into(),
                files: 50
            }],
            "five branches never truncate to three — the hub stays the one honest line"
        );
    }

    #[test]
    fn base_itself_carrying_files_is_its_own_group() {
        let out = agg(&[("/archive", 7), ("/archive/media", 5)], &["/archive"]);
        let paths: Vec<&str> = out.locations.iter().map(|l| l.path.as_str()).collect();
        assert_eq!(paths, vec!["/archive", "/archive/media"]);
    }
}
