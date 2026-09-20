//! Location aggregation — the one "where" derivation, shared by act
//! destinations and covered-copy locations.
//!
//! No I/O anywhere here; callers supply everything fetched.

use crate::core::domain::folder_tree::FolderTree;
use crate::core::domain::path::path_is_under;

/// One location entry on a "where" line: a directory prefix and how many of
/// the line's files it accounts for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocationCount {
    pub path: String,
    pub files: i64,
    /// Every copy this entry accounts for stands behind a closed door.
    ///
    /// The all-or-nothing shape is `only_on_suspended`'s, at location grain,
    /// and it is what makes the marker mean something a reader can act on:
    /// *reaching content here means opening a door*. An entry with one live
    /// copy needs no door opened, so marking it would raise a question the
    /// place does not pose. Aggregation merges directories, so the flag is
    /// carried up the walk rather than read off any one of them.
    pub suspended: bool,
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
/// to where it first branches or holds files, **selected** for the cap by
/// weight then path and returned in that same order — standing never
/// decides which locations earn a slot, nor how the returned list is
/// ordered, since this same aggregate feeds the book's Reference voicing,
/// which is never rewritten and so can never be allowed to depend, in
/// membership *or* in order, on which doors happened to be closed the
/// afternoon it was bound. The **stated-never-headlined** law (a parked
/// entry never leads) is applied only at render time, in
/// `story::ops::render::fmt_locations`, gated on `Voicing::Judgment` exactly
/// like the door marker beside it — this function stays voicing-blind by
/// construction, so there is nowhere here a reorder could leak in. The
/// plain form's locations carry no standing (all unmarked, see below), so
/// this key never applies to it regardless.
/// The plain form, for callers whose locations carry no standing: an act's
/// recorded destinations, which are a past-tense line about where content
/// went, and the comparison keys built from them. Every entry comes back
/// unmarked.
pub fn aggregate_locations(dirs: &[(&str, i64)], bases: &[&str], cap: usize) -> LocationAggregate {
    let with_standing: Vec<(&str, i64, bool)> = dirs.iter().map(|(d, f)| (*d, *f, false)).collect();
    aggregate_impl(&with_standing, bases, cap, false)
}

/// The observed-scatter variant ("copies stand in"): a legible one-line
/// answer descends one level when its branch groups fit the cap — a huge
/// hub node ("Archive/Media") can hide a short, far more informative list.
/// One step, never recursion. Chosen destinations keep the one-line answer
/// (the arrow states a choice; the scatter is what nobody chose).
///
/// Each directory carries whether the archive root it stands on is parked;
/// an entry is marked when every copy under it is.
pub fn aggregate_locations_expanded(
    dirs: &[(&str, i64, bool)],
    bases: &[&str],
    cap: usize,
) -> LocationAggregate {
    aggregate_impl(dirs, bases, cap, true)
}

fn aggregate_impl(
    dirs: &[(&str, i64, bool)],
    bases: &[&str],
    cap: usize,
    expand: bool,
) -> LocationAggregate {
    if dirs.is_empty() {
        return LocationAggregate::default();
    }

    let mut tree = FolderTree::new();
    let mut direct: Vec<i64> = Vec::new();
    let mut direct_parked: Vec<i64> = Vec::new();
    for (dir, files, parked) in dirs {
        let fid = tree.intern(dir) as usize;
        if direct.len() < tree.len() {
            direct.resize(tree.len(), 0);
            direct_parked.resize(tree.len(), 0);
        }
        direct[fid] += files;
        if *parked {
            direct_parked[fid] += files;
        }
    }
    direct.resize(tree.len(), 0);
    direct_parked.resize(tree.len(), 0);

    // Subtree sums: ids are parents-first, so one reverse pass suffices.
    // The parked sum rides along so an entry can say whether *all* of what
    // it accounts for stands behind a door — a question no single directory
    // can answer once directories have been merged.
    let mut sub = direct.clone();
    let mut sub_parked = direct_parked.clone();
    for fid in (0..tree.len() as u32).rev() {
        if let Some(parent) = tree.parent(fid) {
            sub[parent as usize] += sub[fid as usize];
            sub_parked[parent as usize] += sub_parked[fid as usize];
        }
    }
    let all_parked = |files: i64, parked: i64| files > 0 && files == parked;

    let collapse = |mut fid: u32| -> u32 {
        while direct[fid as usize] == 0 && tree.children(fid).len() == 1 {
            fid = tree.children(fid)[0];
        }
        fid
    };

    let top = collapse(0);
    let top_path = tree.path(top);
    // Selection order only — weight then path, never standing, and this
    // order is what the aggregate returns: no reorder happens anywhere in
    // this function. Which locations earn a cap slot, and what order they
    // come back in, must never depend on bind-time door status, because
    // this same aggregate feeds the book's Reference voicing, which is
    // never rewritten — a stray reorder here would be as much a bind-time
    // claim as a missing location, even with no marker printed beside it.
    // `story::ops::render::fmt_locations` is where voicing exists, and it is
    // the only place standing may reorder anything, gated on `Voicing::Judgment`.
    let branch_groups = |top: u32| -> Vec<LocationCount> {
        let mut groups: Vec<LocationCount> = Vec::new();
        if direct[top as usize] > 0 {
            groups.push(LocationCount {
                path: tree.path(top).to_string(),
                files: direct[top as usize],
                suspended: all_parked(direct[top as usize], direct_parked[top as usize]),
            });
        }
        for &child in tree.children(top) {
            let group = collapse(child);
            groups.push(LocationCount {
                path: tree.path(group).to_string(),
                files: sub[child as usize],
                suspended: all_parked(sub[child as usize], sub_parked[child as usize]),
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
                suspended: all_parked(sub[top as usize], sub_parked[top as usize]),
            }],
            omitted_locations: 0,
        };
    }

    // Vacuous common node: emit its branch groups instead, weight-selected
    // and left in weight order — never reordered by standing here.
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

    /// Every directory on a root whose door stands open — the shape almost
    /// every case here is about.
    fn live<'a>(dirs: &[(&'a str, i64)]) -> Vec<(&'a str, i64, bool)> {
        dirs.iter().map(|(d, f)| (*d, *f, false)).collect()
    }

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
                files: 640,
                suspended: false,
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
                files: 120,
                suspended: false,
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
                    files: 3401,
                    suspended: false,
                },
                LocationCount {
                    path: "/archive/games".into(),
                    files: 11,
                    suspended: false,
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
    fn cap_membership_never_depends_on_standing_only_weight() {
        // 4 entries, cap 3. The parked entry outweighs the lightest live
        // one, so a standing-aware *selection* would have pushed it out of
        // a cap slot and into the counted remainder for no reason but its
        // root's suspension — exactly the leak the book-facing aggregate
        // must never carry, since the same `LocationAggregate` also feeds
        // the Reference voicing, which is never rewritten and so can never
        // be allowed to depend on which doors were closed at bind time.
        // Selection is weight-only: the parked entry still earns its slot,
        // and standing only reorders the survivors for display.
        let out = aggregate_locations_expanded(
            &[
                ("/arc/a", 40, false),
                ("/arc/b", 30, true),
                ("/arc/c", 20, false),
                ("/arc/d", 5, false),
            ],
            &["/arc"],
            3,
        );
        let paths: Vec<&str> = out.locations.iter().map(|l| l.path.as_str()).collect();
        assert_eq!(out.omitted_locations, 1);
        assert!(
            paths.contains(&"/arc/b"),
            "the parked entry must still earn its cap slot by weight: {paths:?}"
        );
        assert!(
            !paths.contains(&"/arc/d"),
            "the lightest entry, not the parked one, is what the cap should drop: {paths:?}"
        );
        // The survivors come back in weight order, exactly as selected —
        // display order (standing-first) is render-time work, tested at
        // `story::ops::render::fmt_locations` instead, not here.
        assert_eq!(paths, vec!["/arc/a", "/arc/b", "/arc/c"]);
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

        let expanded = aggregate_locations_expanded(&live(&dirs), &["/archive"], 3);
        assert_eq!(
            expanded.locations,
            vec![
                LocationCount {
                    path: "/archive/media/2011".into(),
                    files: 360,
                    suspended: false,
                },
                LocationCount {
                    path: "/archive/media/2010/june".into(),
                    files: 280,
                    suspended: false,
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
        let expanded = aggregate_locations_expanded(&live(&refs), &["/archive"], 3);
        assert_eq!(
            expanded.locations,
            vec![LocationCount {
                path: "/archive/media".into(),
                files: 50,
                suspended: false,
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

    #[test]
    fn a_merged_entry_spanning_two_standings_is_unmarked() {
        // The walk splits at the first branching node, which can sit *above*
        // two archive roots — so one entry accounts for copies on a live root
        // and a parked one at once. Reaching that content needs no door
        // opened, so the entry says nothing about one.
        let out = aggregate_locations_expanded(
            &[
                ("/arc/live1/media", 2, false),
                ("/arc/parked1/media", 3, true),
                ("/other/media", 1, false),
            ],
            &["/arc", "/other"],
            3,
        );
        let merged = out
            .locations
            .iter()
            .find(|l| l.path == "/arc")
            .expect("the two archive roots merge into one entry");
        assert_eq!(merged.files, 5);
        assert!(
            !merged.suspended,
            "a live copy under the entry means nothing needs opening"
        );
    }

    #[test]
    fn the_aggregate_itself_stays_weight_ordered_regardless_of_standing() {
        // This function is voicing-blind by construction (see its own doc):
        // the stated-never-headlined law's *ordering* half is render-time
        // work, in `story::ops::render::fmt_locations`, gated on
        // `Voicing::Judgment`. A heavier parked entry ("/arc/b", 20 files)
        // outranks a lighter live one here, on weight alone — reordering it
        // is exactly what this layer must never do, since the book's
        // Reference voicing reads this same, unreordered aggregate.
        let out = aggregate_locations_expanded(
            &[("/arc/a", 5, false), ("/arc/b", 20, true)],
            &["/arc"],
            3,
        );
        let paths: Vec<&str> = out.locations.iter().map(|l| l.path.as_str()).collect();
        assert_eq!(paths, vec!["/arc/b", "/arc/a"]);
        assert!(out.locations[0].suspended);
        assert!(!out.locations[1].suspended);
    }

    #[test]
    fn a_merged_entry_wholly_behind_doors_is_marked() {
        // The same shape with both roots parked: now every copy the entry
        // accounts for is behind a door, and it says so.
        let out = aggregate_locations_expanded(
            &[
                ("/arc/parked1/media", 2, true),
                ("/arc/parked2/media", 3, true),
                ("/other/media", 1, false),
            ],
            &["/arc", "/other"],
            3,
        );
        let merged = out
            .locations
            .iter()
            .find(|l| l.path == "/arc")
            .expect("the two archive roots merge into one entry");
        assert_eq!(merged.files, 5);
        assert!(merged.suspended);
    }
}
