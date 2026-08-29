//! The outbound counterpart key: folding the ledger's destination leaves into
//! a handful of places a person would name.
//!
//! The ledger records a placement at `(origin dir, destination dir)`, and a
//! manifest pattern can fan one apply across a directory per day. Read back at
//! that precision the outbound view answers *which places?* with a list of
//! generated date folders — correct rows, and not an answer to the question
//! asked. So the bare view groups its destinations at a key derived from the
//! rows in view: coarser than the leaf, deeper than the root.
//!
//! Two properties hold by construction rather than by care, because both are
//! read off the rendered line:
//!
//! * the keys form an **antichain** — no key is at or above another, so a
//!   rendered path never reads as containing rows its own entry excludes;
//! * the groups **partition** the leaves — every leaf lands in exactly one
//!   group, so the sum over entries is the section total.
//!
//! No I/O: the live root paths a key must stay below are passed in as data.

use crate::core::domain::path::{common_path_prefix, path_is_under};

/// How many places is a handful: the refinement budget.
///
/// Part of the answer's grain, not its presentation. `--limit`/`--all` size
/// the *listing* — how much of the answer is shown — and tying the two
/// together would give `--all` a second meaning, re-deriving the grain from a
/// display flag.
const DESTINATION_GROUP_BUDGET: usize = 8;

/// One entry of the outbound listing: a place, and the ledger leaves it
/// stands for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DestinationGroup {
    /// Chain-collapsed deepest common ancestor of the members — never an
    /// ancestor of a sibling group's key.
    pub key: String,
    /// The distinct ledger-leaf destinations this group covers, sorted;
    /// `len()` is the folder count the entry states.
    pub leaves: Vec<String>,
}

/// Partition `leaves` into an antichain of groups.
///
/// Leaves are **absolute, `/`-separated paths** — what the ledger records for
/// a destination, which is always joined onto a root path. The component
/// model assumes it: a relative or single-component leaf degrades to one
/// group keyed at the empty string rather than erroring.
///
/// `floors` are live root paths. A key at or above one is split apart
/// whatever the budget says, so the listing does not answer "where did this
/// go?" with a whole archive. Roots never nest, so at most one floor contains
/// any leaf.
///
/// Two ways a key can still end up at a floor, both deliberate and both
/// pinned: a **single leaf** is already at ledger grain and is never refined,
/// so a delivery to a root's own top keys there; and a group **holding its
/// own key as a member** cannot be split into disjoint sibling subtrees, so
/// it stays whole and states its folder count (see [`refine`]).
///
/// Leaves under no live floor (a removed archive root) grow floor-free: with
/// nothing to stay below, grouping may reach their own deepest common
/// ancestor. A recorded degradation — the alternative is inventing a floor
/// for a place the index no longer knows.
///
/// The budget bounds the **greedy** pass only. Splitting past a floor runs
/// first and is unbounded in width, so a layout whose destinations all sit one
/// component below the root — where there is no intermediate directory to key
/// on — yields one group per destination however many there are. That is the
/// honest answer for that shape, not a cap being missed: any coarser key would
/// be the root itself.
pub fn group_destinations(leaves: &[&str], floors: &[&str]) -> Vec<DestinationGroup> {
    let mut distinct: Vec<String> = leaves.iter().map(|leaf| leaf.to_string()).collect();
    distinct.sort();
    distinct.dedup();
    if distinct.is_empty() {
        return Vec::new();
    }

    // One group over everything, then split down to the floors regardless of
    // budget, then refine on the budget for as long as it buys legibility.
    let mut groups = Vec::new();
    force_split(group_of(distinct), floors, &mut groups);

    loop {
        // Largest first: refinement pays in proportion to how many leaves a
        // key is standing for. Ties by key path, so repeated runs render
        // identically.
        let mut order: Vec<usize> = (0..groups.len()).collect();
        order.sort_by(|&a, &b| {
            groups[b]
                .leaves
                .len()
                .cmp(&groups[a].leaves.len())
                .then_with(|| groups[a].key.cmp(&groups[b].key))
        });

        let mut refined = false;
        for index in order {
            let Some(children) = refine(&groups[index]) else {
                continue;
            };
            if groups.len() - 1 + children.len() > DESTINATION_GROUP_BUDGET {
                // The ordering is a priority, not just a determinism device:
                // if the refinement that would reveal the most cannot be
                // afforded, spending what is left on a smaller one inverts
                // that priority and leaves the listing at mixed grain — one
                // line standing for forty folders beside three standing for
                // one each, which is harder to scan than either grain alone.
                break;
            }
            groups.splice(index..index + 1, children);
            refined = true;
            break;
        }
        if !refined {
            break;
        }
    }

    groups.sort_by(|a, b| a.key.cmp(&b.key));
    groups
}

/// A group keyed at its own members' deepest common ancestor.
///
/// This is where **chain collapse** happens, and why it needs no separate
/// pass: a run of single-child directories is common to every member, so the
/// common prefix walks straight through it and no link in the chain can
/// surface as a key. A single member keys at itself — already ledger grain.
fn group_of(leaves: Vec<String>) -> DestinationGroup {
    let key = common_path_prefix(leaves.iter().map(String::as_str));
    DestinationGroup { key, leaves }
}

/// A path's `/`-separated components, the same decomposition
/// [`common_path_prefix`] works in — so a key derived there and a split
/// computed here can never disagree about where a component ends. `/a/bc` is
/// not under `/a/b` in either.
fn components(path: &str) -> Vec<&str> {
    path.split('/').collect()
}

/// Replace a group by the chain-collapsed groups one component below its key,
/// or `None` where it cannot be split.
///
/// Two refusals, and the second is the load-bearing one. A group **holding
/// its own key as a member** — a delivery to a place and a delivery below it
/// — cannot be split into disjoint sibling subtrees: the member that *is* the
/// key would have to sit beside a key beneath it, which is precisely the
/// ancestor shape the antichain forbids. Such a group stays whole and states
/// its folder count, which is what makes its line honest about what it covers.
fn refine(group: &DestinationGroup) -> Option<Vec<DestinationGroup>> {
    if group.leaves.len() < 2 {
        return None;
    }
    let depth = components(&group.key).len();
    if group
        .leaves
        .iter()
        .any(|leaf| components(leaf).len() <= depth)
    {
        return None;
    }

    let mut buckets: Vec<(String, Vec<String>)> = Vec::new();
    for leaf in &group.leaves {
        let head = components(leaf)[..=depth].join("/");
        match buckets.iter_mut().find(|(bucket, _)| *bucket == head) {
            Some((_, members)) => members.push(leaf.clone()),
            None => buckets.push((head, vec![leaf.clone()])),
        }
    }
    // The key is the members' deepest common ancestor and no member is the
    // key, so two heads always differ. Defensive rather than expected.
    if buckets.len() < 2 {
        return None;
    }
    Some(
        buckets
            .into_iter()
            .map(|(_, members)| group_of(members))
            .collect(),
    )
}

/// Whether a group must be split whatever the budget says.
///
/// A key that is **at or above a live root** stands for a whole archive, and
/// answering "where did this go?" with the archive is true and useless — the
/// same objection that ruled out keying the outbound side by root in the first
/// place. An empty key is the degenerate form of it: leaves in unrelated
/// absolute trees share no ancestor at all, and a group keyed at nothing
/// claims everything.
///
/// A group under no floor is left alone: with no root in view there is no
/// whole-archive claim to prevent, and inventing a floor would be a guess
/// about a place the index no longer knows.
fn needs_force(group: &DestinationGroup, floors: &[&str]) -> bool {
    if group.leaves.len() < 2 {
        return false;
    }
    if group.key.is_empty() || group.key == "/" {
        return true;
    }
    // `path_is_under(floor, key)` reads "the floor lies at or under the key",
    // which is the key lying at or above the floor.
    floors.iter().any(|floor| path_is_under(floor, &group.key))
}

/// Split down past every floor, then emit. Recursive because a forced split's
/// children can sit at a floor themselves.
fn force_split(group: DestinationGroup, floors: &[&str], out: &mut Vec<DestinationGroup>) {
    if needs_force(&group, floors) {
        if let Some(children) = refine(&group) {
            for child in children {
                force_split(child, floors, out);
            }
            return;
        }
    }
    out.push(group);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The corpus is written as owned paths and borrowed at the call, which
    /// is the shape the ops layer hands in (leaf strings it already holds).
    fn group(leaves: &[&str], floors: &[&str]) -> Vec<DestinationGroup> {
        group_destinations(leaves, floors)
    }

    fn keys(groups: &[DestinationGroup]) -> Vec<&str> {
        groups.iter().map(|g| g.key.as_str()).collect()
    }

    fn days(month: &str, days: &[&str]) -> Vec<String> {
        days.iter().map(|d| format!("{month}/{d}")).collect()
    }

    /// The shape this derivation exists for: an archive built by a pattern
    /// that produced a directory per day, plus a couple of folders a person
    /// actually named. The days fold to their months; the named folders
    /// survive as themselves.
    #[test]
    fn the_requester_shape_folds_days_and_keeps_events() {
        let mut leaves = days(
            "/archive/Media/2016/03",
            &["01", "02", "04", "07", "08", "10", "11", "12"],
        );
        leaves.extend(days(
            "/archive/Media/2016/04",
            &["03", "05", "09", "14", "20", "28"],
        ));
        leaves.extend([
            "/archive/Media/2016/an-event/clips".to_string(),
            "/archive/Media/2016/an-event/stills".to_string(),
            "/archive/Media/2016/an-event/raw".to_string(),
            "/archive/Media/2016/another-event".to_string(),
        ]);
        let refs: Vec<&str> = leaves.iter().map(String::as_str).collect();

        let groups = group(&refs, &["/archive"]);
        assert_eq!(
            keys(&groups),
            vec![
                "/archive/Media/2016/03",
                "/archive/Media/2016/04",
                "/archive/Media/2016/an-event",
                "/archive/Media/2016/another-event",
            ]
        );
        // The coverage counts are what make the folding legible.
        let counts: Vec<usize> = groups.iter().map(|g| g.leaves.len()).collect();
        assert_eq!(counts, vec![8, 6, 3, 1]);
    }

    /// The plain case underneath that one: a month whose days cannot all be
    /// listed within the budget stays folded at the month.
    #[test]
    fn days_under_one_month_collapse_to_the_month() {
        let leaves = days(
            "/archive/Media/2016/03",
            &[
                "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12",
            ],
        );
        let refs: Vec<&str> = leaves.iter().map(String::as_str).collect();

        let groups = group(&refs, &["/archive"]);
        assert_eq!(keys(&groups), vec!["/archive/Media/2016/03"]);
        assert_eq!(groups[0].leaves.len(), 12);
    }

    /// Deliveries into genuinely unrelated trees each stand on their own. The
    /// degradation is to today's behaviour — never a manufactured parent that
    /// no delivery was ever made to.
    #[test]
    fn unrelated_areas_never_share_an_invented_parent() {
        let groups = group(
            &["/archive/Media/2016/03/08", "/photos/Backup/incoming"],
            &["/archive", "/photos"],
        );
        assert_eq!(
            keys(&groups),
            vec!["/archive/Media/2016/03/08", "/photos/Backup/incoming"]
        );

        // And with no floors at all, so the force rule is not what saves it:
        // an empty common ancestor claims everything and is split regardless.
        let unfloored = group(
            &["/archive/Media/2016/03/08", "/photos/Backup/incoming"],
            &[],
        );
        assert_eq!(keys(&unfloored), keys(&groups));
    }

    /// **Red-smoke**: without the force rule this corpus keys at `/archive`
    /// itself — twelve leaves is over budget, so the greedy pass alone would
    /// never refine it, and the view would answer "where did it go?" with the
    /// whole archive.
    #[test]
    fn a_key_is_never_at_or_above_a_root_floor() {
        let leaves: Vec<String> = (0..12).map(|i| format!("/archive/d{i:02}")).collect();
        let refs: Vec<&str> = leaves.iter().map(String::as_str).collect();

        let groups = group(&refs, &["/archive"]);
        assert_eq!(groups.len(), 12, "the floor forced the split past budget");
        for g in &groups {
            assert!(
                !path_is_under("/archive", &g.key),
                "{} is at or above the floor",
                g.key
            );
        }

        // A key *above* a floor is the same claim one level out, and is
        // forced the same way: two archives under one parent must not collapse
        // into a line naming the parent.
        let across = group(
            &["/vol/one/Media/a", "/vol/two/Media/b"],
            &["/vol/one", "/vol/two"],
        );
        assert_eq!(keys(&across), vec!["/vol/one/Media/a", "/vol/two/Media/b"]);

        // The **direction** of the floor test, which the two cases above
        // cannot see: both survive `path_is_under`'s arguments being swapped,
        // one because `starts_with` is reflexive and the other because the
        // budget permits the refinement anyway. A key strictly *below* a
        // floor must be left alone — swap the arguments and this corpus force-
        // splits to twelve ledger leaves instead of folding to its month.
        let nested = days(
            "/archive/Media/2016/03",
            &[
                "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12",
            ],
        );
        let refs: Vec<&str> = nested.iter().map(String::as_str).collect();
        assert_eq!(
            keys(&group(&refs, &["/archive"])),
            vec!["/archive/Media/2016/03"],
            "a key below the floor must not be forced apart"
        );
    }

    /// One leaf is already at ledger grain: it keys at itself and is never
    /// refined — including a delivery to a root's own top, which stays exactly
    /// as it renders today.
    #[test]
    fn a_single_leaf_group_keys_at_the_leaf() {
        let groups = group(&["/archive/Media/2016/03/08"], &["/archive"]);
        assert_eq!(keys(&groups), vec!["/archive/Media/2016/03/08"]);
        assert_eq!(groups[0].leaves.len(), 1);

        let root_top = group(&["/archive"], &["/archive"]);
        assert_eq!(keys(&root_top), vec!["/archive"]);
        assert_eq!(root_top[0].leaves, vec!["/archive".to_string()]);
    }

    /// The collision the two "by construction" rules make when a delivery to
    /// a root's own top shares a view with a delivery below it: the group
    /// cannot be split without putting `/archive` beside a key beneath it, so
    /// the antichain wins and the group stays whole.
    ///
    /// Accepted residue rather than a defect. The force rule exists against a
    /// *coarse key swallowing a wide spread*; here the key is a recorded
    /// destination in its own right, and the entry states the two folders it
    /// covers. Splitting would buy a shorter line at the cost of a path that
    /// reads as containing rows its own entry excludes — the exact ambiguity
    /// the antichain was adopted to remove.
    #[test]
    fn a_group_holding_its_own_key_stays_whole() {
        let groups = group(&["/archive", "/archive/Media/2016"], &["/archive"]);
        assert_eq!(keys(&groups), vec!["/archive"]);
        assert_eq!(groups[0].leaves.len(), 2, "the entry states both folders");
    }

    /// The two properties every rendered entry rests on, asserted over the
    /// corpora above rather than argued.
    #[test]
    fn keys_are_an_antichain_and_groups_partition_the_leaves() {
        let mut wide = days(
            "/archive/Media/2016/03",
            &["01", "02", "04", "07", "08", "10"],
        );
        wide.extend(days("/archive/Media/2016/04", &["03", "05"]));
        wide.extend([
            "/archive/Media/2016/an-event/clips".to_string(),
            "/archive/Docs/scans".to_string(),
            "/photos/Backup/incoming".to_string(),
        ]);
        let corpora: Vec<Vec<String>> = vec![
            wide,
            (0..12).map(|i| format!("/archive/d{i:02}")).collect(),
            vec!["/archive".to_string(), "/archive/Media/2016".to_string()],
            vec!["/gone/a/x".to_string(), "/gone/b/y".to_string()],
        ];

        for corpus in &corpora {
            let refs: Vec<&str> = corpus.iter().map(String::as_str).collect();
            let groups = group(&refs, &["/archive", "/photos"]);

            for (i, a) in groups.iter().enumerate() {
                for (j, b) in groups.iter().enumerate() {
                    if i == j {
                        continue;
                    }
                    assert!(
                        !path_is_under(&b.key, &a.key),
                        "{} is at or above {}",
                        a.key,
                        b.key
                    );
                }
            }

            let mut covered: Vec<&str> = groups
                .iter()
                .flat_map(|g| g.leaves.iter().map(String::as_str))
                .collect();
            covered.sort_unstable();
            let mut expected: Vec<&str> = refs.clone();
            expected.sort_unstable();
            expected.dedup();
            assert_eq!(covered, expected, "groups must partition the leaves");

            // Every group's key really is its own members' ancestor, so the
            // rendered path never over- or under-reaches its own entry.
            for g in &groups {
                for leaf in &g.leaves {
                    assert!(path_is_under(leaf, &g.key), "{leaf} is not under {}", g.key);
                }
            }
        }
    }

    /// A run of single-child directories is common to every member, so no link
    /// in it surfaces as a key: the group keys at its own deepest common
    /// ancestor, not at the first component below its parent.
    ///
    /// The corpus is wide enough that the group survives the budget — a
    /// handful of leaves would refine all the way to ledger grain, where
    /// there is no key left to collapse and the property is unobservable.
    #[test]
    fn chain_collapse_keys_at_the_groups_own_dca() {
        let mut leaves: Vec<String> = (0..12).map(|i| format!("/archive/a/b/c/x{i:02}")).collect();
        leaves.push("/archive/z/deep/only".to_string());
        let refs: Vec<&str> = leaves.iter().map(String::as_str).collect();

        let groups = group(&refs, &["/archive"]);
        assert_eq!(
            keys(&groups),
            vec!["/archive/a/b/c", "/archive/z/deep/only"]
        );
        // Not `/archive/a`, and not `/archive/a/b`: neither is a place any
        // delivery distinguishes from the one below it.
        assert_eq!(groups[0].leaves.len(), 12);
    }

    /// A view small enough to list at ledger grain is listed at ledger grain:
    /// the budget is a ceiling, so with nothing to fold the derivation
    /// degrades to today's output rather than inventing a coarser key.
    #[test]
    fn a_handful_of_leaves_stays_at_ledger_grain() {
        let groups = group(
            &[
                "/archive/Media/2016/03/08",
                "/archive/Media/2016/03/04",
                "/archive/Docs/scans",
            ],
            &["/archive"],
        );
        assert_eq!(
            keys(&groups),
            vec![
                "/archive/Docs/scans",
                "/archive/Media/2016/03/04",
                "/archive/Media/2016/03/08",
            ]
        );
    }

    /// The corpus has to be one where the **budget binds**: a handful of
    /// leaves refines all the way to ledger grain whatever order they arrive
    /// in, so ordering, the priority comparator and the stop rule are all
    /// unobservable there and the assertion would hold with every one of them
    /// removed. This one leaves competing refinements on the table, so the
    /// answer genuinely depends on which is taken first.
    #[test]
    fn grouping_is_deterministic() {
        let mut owned = days(
            "/archive/Media/2016/03",
            &["01", "02", "04", "07", "08", "10", "11", "12"],
        );
        owned.extend(days("/archive/Media/2016/04", &["03", "05", "09"]));
        owned.push("/archive/Media/2016/an-event".to_string());
        let leaves: Vec<&str> = owned.iter().map(String::as_str).collect();

        let forward = group(&leaves, &["/archive"]);
        assert!(
            forward.iter().any(|g| g.leaves.len() > 1),
            "the budget must bind, or ordering cannot matter: {:?}",
            keys(&forward)
        );

        let mut reversed: Vec<&str> = leaves.clone();
        reversed.reverse();
        assert_eq!(group(&reversed, &["/archive"]), forward);

        // A rotation, so the comparator is exercised from a third starting
        // point rather than only its own mirror.
        let mut rotated: Vec<&str> = leaves.clone();
        rotated.rotate_left(5);
        assert_eq!(group(&rotated, &["/archive"]), forward);

        // And a repeated leaf changes nothing: the input is deduped first.
        let mut duplicated: Vec<&str> = leaves.clone();
        duplicated.push(leaves[0]);
        assert_eq!(group(&duplicated, &["/archive"]), forward);
    }

    /// Components, never bytes: `/a/bc` is not under `/a/b`, so a sibling
    /// whose name merely starts with another's is never swallowed by it.
    #[test]
    fn component_boundaries_are_respected() {
        let mut leaves: Vec<String> = (0..12).map(|i| format!("/archive/Media/a{i:02}")).collect();
        leaves.push("/archive/MediaExtra/c".to_string());
        let refs: Vec<&str> = leaves.iter().map(String::as_str).collect();

        let groups = group(&refs, &["/archive"]);
        assert_eq!(
            keys(&groups),
            vec!["/archive/Media", "/archive/MediaExtra/c"]
        );
        // The sibling whose name merely starts with the group's own is a
        // separate entry, never a thirteenth leaf folded into it.
        assert_eq!(groups[0].leaves.len(), 12);
    }

    /// A flat archive — every destination one component below the root — has
    /// no intermediate directory to key on, so every leaf is its own entry
    /// however many there are, and the budget does not apply. One directory of
    /// nesting is the whole difference, which is why this is worth a test
    /// rather than a remark: the shape is invisible to any corpus built from
    /// a nested archive.
    #[test]
    fn a_flat_archive_layout_has_no_key_between_root_and_leaf() {
        let flat: Vec<String> = (1..=30)
            .map(|d| format!("/archive/2016-03-{d:02}"))
            .collect();
        let refs: Vec<&str> = flat.iter().map(String::as_str).collect();
        let groups = group(&refs, &["/archive"]);
        assert_eq!(groups.len(), 30, "no coarser key exists below the floor");
        assert!(groups.iter().all(|g| g.leaves.len() == 1));

        // One directory deeper and the same thirty fold into one.
        let nested: Vec<String> = (1..=30)
            .map(|d| format!("/archive/Media/2016/03/{d:02}"))
            .collect();
        let refs: Vec<&str> = nested.iter().map(String::as_str).collect();
        assert_eq!(
            keys(&group(&refs, &["/archive"])),
            vec!["/archive/Media/2016/03"]
        );
    }

    /// The recorded degradation: leaves under no live root grow floor-free,
    /// so grouping may reach a key that — were the root still known — the
    /// force rule would never have allowed. Same corpus with that root live,
    /// and it is taken apart. The difference is exactly what the floors buy.
    #[test]
    fn a_leaf_under_no_live_root_may_group_to_its_dca() {
        let leaves: Vec<String> = (0..12).map(|i| format!("/gone/d{i:02}")).collect();
        let refs: Vec<&str> = leaves.iter().map(String::as_str).collect();

        let floor_free = group(&refs, &["/archive"]);
        assert_eq!(keys(&floor_free), vec!["/gone"]);
        assert_eq!(floor_free[0].leaves.len(), 12);

        let floored = group(&refs, &["/archive", "/gone"]);
        assert_eq!(floored.len(), 12, "a live floor is never a key");
    }
}
