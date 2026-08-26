//! The two-claims law: matching declared scopes and observed placements
//! against a view boundary, and deriving what each matched row means
//! relative to it.
//!
//! A **declared scope** ("I acted on this subtree") matches bidirectionally
//! via [`scopes_touch`]; an **observed placement** (an extraction row's
//! origin or destination location — "my files lie under here") matches
//! descendant-or-equal only via [`placement_in_view`]. Never match a
//! placement with `scopes_touch` — its ancestor branch is how a
//! common-prefix destination once manufactured arrivals at sibling folders.
//!
//! No I/O.

use std::path::Path;

use crate::core::domain::extraction::DecisionExtraction;
use crate::core::domain::path::common_path_prefix;

/// Bidirectional touches-scope over rel prefixes within one root: a decision
/// touches the viewed scope if its prefix is under the view *or* an ancestor
/// of it. "" is the root itself and touches everything. Segment-aware:
/// "a/bc" does not touch "a/b".
///
/// This matches **declared scopes** — `decision_scopes` rows and note paths,
/// where the recorded value means "I acted on this subtree", so acting on an
/// ancestor genuinely touches the view. An extraction row's locations are a
/// different kind of claim; match those with [`placement_in_view`], never
/// with this.
pub fn scopes_touch(view_prefix: &str, other_prefix: &str) -> bool {
    Path::new(view_prefix).starts_with(other_prefix)
        || Path::new(other_prefix).starts_with(view_prefix)
}

/// Which recorded scope brought a decision into the view, and how many other
/// places the decision also names.
///
/// Derived once in the operations layer and carried on the result; **no
/// surface re-derives the match**. The join is already computed there — the
/// filter that decides which decisions surface at all — and discarding it
/// only to have the interface guess again is how a 31-prefix scan came to be
/// labelled by its *first* recorded prefix, a place with nothing to do with
/// the view. The same carried-match discipline governs extraction-row
/// classification (`ops::compute::classify_extraction_rows`) and
/// `trail show`'s scope markers (`ops::show::ShowScope`).
///
/// Matching is [`scopes_touch`] — a `decision_scopes` row is a **declared
/// scope** ("I acted on this subtree"), so an ancestor of the view genuinely
/// matches it. Never `placement_in_view` here (the two-claims law).
pub struct ScopeMatch {
    /// Display path of the matching scope. Where several match, the
    /// **deepest** wins — a scope inside the view is a more precise statement
    /// of where the act was than an ancestor of it. Ties break
    /// lexicographically, so repeated runs render identically.
    pub matched: String,
    /// The decision's other recorded places, for the `+N` remainder. Counted
    /// from the decision's own display column so `+N` keeps meaning what it
    /// means today and stays consistent with `show` and `--jsonl`: one less
    /// than its length when `matched` appears there, its full length when it
    /// does not (a scope row whose display entry was never backfilled).
    pub other_count: usize,
}

/// Whether an **observed placement** lies within the viewed scope:
/// descendant-or-equal only, segment-aware, never bidirectional.
///
/// A placement is where files demonstrably are — an extraction row's origin
/// or destination location. The row's claim is "all my files lie under this
/// location", so it surfaces in every view that contains the location and in
/// no other; wherever it surfaces, its count is exact. An *ancestor* of the
/// view implies nothing about the view (a common prefix of `2016/01` and
/// `2016/02` says nothing about `2016/03`) — matching that direction is how
/// the trail once manufactured history. Declared scopes ("I acted on this
/// subtree") are the other kind of claim and keep [`scopes_touch`].
pub fn placement_in_view(view_prefix: &str, placement: &str) -> bool {
    Path::new(placement).starts_with(view_prefix)
}

/// Which direction a single extraction row reads from inside a view.
///
/// A rollup counts boundary crossings, and the view defines the boundary: the
/// same decision is an arrival seen from a narrow scope and a rearrangement
/// seen from the root that contains both its endpoints. That is the rule
/// working, not an inconsistency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowAspect {
    /// Origin inside, destination outside — content left here.
    Extraction,
    /// Both endpoints inside — content moved within, crossing nothing.
    Rearrangement,
    /// Destination inside, origin outside — content entered here.
    Arrival,
    /// Neither endpoint touches the view.
    Outside,
}

/// The one derivation of what a row means relative to a boundary.
///
/// Consumers supply membership evidence in whatever form they already hold —
/// the rollups establish origin membership by root id and destination
/// membership by snapshot path, the card has only prefixes. Centralizing the
/// *rule* keeps them in agreement; centralizing the *evidence* would silently
/// change how each one matches.
pub fn row_aspect(origin_in_view: bool, destination_in_view: bool) -> RowAspect {
    match (origin_in_view, destination_in_view) {
        (true, true) => RowAspect::Rearrangement,
        (true, false) => RowAspect::Extraction,
        (false, true) => RowAspect::Arrival,
        (false, false) => RowAspect::Outside,
    }
}

/// [`row_aspect`] with membership established from absolute path prefixes —
/// for consumers holding no membership maps. An empty prefix list is a global
/// view, which has no boundary to cross: every row reads as a rearrangement.
/// The composition card is its consumer.
///
/// Membership is [`placement_in_view`] — the row's locations are observed
/// placements, so only a location the view contains is inside it.
pub fn classify_row(row: &DecisionExtraction, prefixes: &[String]) -> RowAspect {
    let within = |path: &str| {
        prefixes.is_empty()
            || prefixes
                .iter()
                .any(|prefix| placement_in_view(prefix, path))
    };
    row_aspect(within(&row.drawn_from()), within(&row.destination_path))
}

/// One rendered timeline line: the display aggregate of a decision's
/// same-aspect placement rows within one origin root.
///
/// `row` is a synthetic display row — files summed, bytes all-or-omitted,
/// each location collapsed to the common prefix of the member rows'
/// locations — so it keeps the row invariant ("all counted files lie under
/// these locations") and every narration helper renders it like any stored
/// row. A one-row group aggregates to that row unchanged.
pub struct PlacementLine {
    pub row: DecisionExtraction,
    pub aspect: RowAspect,
}

/// Collapse a decision's view-matched rows into its rendered lines: one per
/// (origin root, aspect), in first-seen row order (rows arrive in stable
/// `(decision_id, root_id)` fetch order, so repeated runs render
/// identically).
///
/// Because only rows the view matched are aggregated, a line's counts are
/// the view's counts and its locations are common prefixes of in-view
/// placements — the line can never name a location outside the view, nor
/// one the decision didn't place into.
pub fn aggregate_placement_lines(rows: &[(DecisionExtraction, RowAspect)]) -> Vec<PlacementLine> {
    let mut groups: Vec<(&str, RowAspect, Vec<&DecisionExtraction>)> = Vec::new();
    for (row, aspect) in rows {
        match groups
            .iter_mut()
            .find(|(root, a, _)| *root == row.root_path && a == aspect)
        {
            Some((_, _, members)) => members.push(row),
            None => groups.push((&row.root_path, *aspect, vec![row])),
        }
    }
    groups
        .into_iter()
        .map(|(_, aspect, members)| {
            let first = members[0];
            PlacementLine {
                row: DecisionExtraction {
                    decision_id: first.decision_id,
                    root_id: first.root_id,
                    root_path: first.root_path.clone(),
                    rel_prefix: common_path_prefix(members.iter().map(|r| r.rel_prefix.as_str())),
                    files: members.iter().map(|r| r.files).sum(),
                    bytes: if members.iter().all(|r| r.bytes.is_some()) {
                        Some(members.iter().filter_map(|r| r.bytes).sum())
                    } else {
                        None
                    },
                    destination_root_id: first.destination_root_id,
                    destination_path: common_path_prefix(
                        members.iter().map(|r| r.destination_path.as_str()),
                    ),
                    // Decision-wide by construction: one apply, one mode.
                    disposition: first.disposition,
                },
                aspect,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    // scopes_touch

    #[test]
    fn scopes_touch_relations() {
        assert!(scopes_touch("a/b", "a/b")); // equal
        assert!(scopes_touch("a/b/c", "a/b")); // decision on ancestor
        assert!(scopes_touch("a/b", "a/b/c")); // decision on descendant
        assert!(!scopes_touch("a/b", "a/x")); // sibling
        assert!(scopes_touch("", "a/b")); // root view touches everything
        assert!(scopes_touch("a/b", "")); // root-level decision touches every view
        assert!(scopes_touch("", ""));
        assert!(!scopes_touch("a/bc", "a/b")); // segment boundary
        assert!(!scopes_touch("a/b", "a/bc"));
    }

    #[test]
    fn scopes_touch_absolute_paths() {
        // Arrival matching runs on absolute snapshot paths (view prefix vs. a
        // decision's recorded destination_path), not rel-prefixes within one
        // root — the same predicate must hold there too.
        assert!(scopes_touch("/archive/x", "/archive/x")); // equal
        assert!(scopes_touch("/archive/x/y", "/archive/x")); // view deeper than destination
        assert!(scopes_touch("/archive/x", "/archive/x/y")); // destination deeper than view
        assert!(!scopes_touch("/archive/x", "/archive/y")); // sibling
        assert!(!scopes_touch("/archive/x", "/archive/xc")); // segment boundary
        assert!(!scopes_touch("/archive/xc", "/archive/x"));
    }

    // placement_in_view

    #[test]
    fn placement_in_view_is_descendant_or_equal_only() {
        // The two-claims law: unlike a declared scope, an observed placement
        // matches only views that contain it. The ancestor direction — where
        // scopes_touch says yes — is exactly the manufactured-history case.
        assert!(placement_in_view("/archive/x", "/archive/x")); // equal
        assert!(placement_in_view("/archive/x", "/archive/x/y")); // placement deeper: contained
        assert!(!placement_in_view("/archive/x/y", "/archive/x")); // placement above: claims nothing
        assert!(!placement_in_view("/archive/x", "/archive/y")); // sibling
        assert!(!placement_in_view("/archive/x", "/archive/xc")); // segment boundary
        assert!(!placement_in_view("/archive/xc", "/archive/x"));
    }

    #[test]
    fn placement_in_view_handles_rel_prefixes_and_the_root_itself() {
        // Rel-prefix form within one root: "" is the root itself.
        assert!(placement_in_view("", "m/01")); // root view contains every placement
        assert!(placement_in_view("m", "m/01"));
        assert!(!placement_in_view("m/03", "m")); // a common prefix of 01+02 says nothing about 03
        assert!(!placement_in_view("m/01", "")); // a root-level placement is not inside a subfolder
        assert!(placement_in_view("", "")); // root placement, root view
    }

    #[test]
    fn classify_row_never_reads_an_ancestor_location_as_inside() {
        // Both directions of the law, through the card's consumer. A row
        // whose recorded locations sit *above* the view is Outside — under
        // the old bidirectional rule both of these read as inside.
        let view = vec!["/archive/2016/03".to_string()];
        assert_eq!(
            classify_row(
                &mk_extraction("/Volumes/sd", "dcim", "/archive/2016"),
                &view
            ),
            RowAspect::Outside
        );
        assert_eq!(
            // Origin prefix above the view within the same tree.
            classify_row(&mk_extraction("/archive", "2016", "/elsewhere"), &view),
            RowAspect::Outside
        );
    }

    // row_aspect / classify_row — the boundary-crossing rule

    fn mk_extraction(root_path: &str, rel_prefix: &str, destination: &str) -> DecisionExtraction {
        DecisionExtraction {
            decision_id: 42,
            root_id: 1,
            root_path: root_path.to_string(),
            rel_prefix: rel_prefix.to_string(),
            files: 47,
            bytes: Some(3_900),
            destination_root_id: Some(2),
            destination_path: destination.to_string(),
            disposition: None,
        }
    }

    // aggregate_placement_lines

    fn placement(
        root_path: &str,
        rel_prefix: &str,
        destination: &str,
        files: i64,
        bytes: Option<i64>,
    ) -> DecisionExtraction {
        let mut row = mk_extraction(root_path, rel_prefix, destination);
        row.files = files;
        row.bytes = bytes;
        row
    }

    #[test]
    fn aggregate_merges_same_root_same_aspect_rows_into_one_line() {
        let rows = vec![
            (
                placement("/src", "m/01", "/arch/m/01", 105, Some(1_050)),
                RowAspect::Extraction,
            ),
            (
                placement("/src", "m/02", "/arch/m/02", 140, Some(1_400)),
                RowAspect::Extraction,
            ),
        ];
        let lines = aggregate_placement_lines(&rows);
        assert_eq!(lines.len(), 1);
        let line = &lines[0];
        assert_eq!(line.aspect, RowAspect::Extraction);
        assert_eq!(line.row.files, 245);
        assert_eq!(line.row.bytes, Some(2_450));
        // Locations collapse to the members' common prefix — of the matched
        // rows only, so a line never names a place the group didn't touch.
        assert_eq!(line.row.rel_prefix, "m");
        assert_eq!(line.row.drawn_from(), "/src/m");
        assert_eq!(line.row.destination_path, "/arch/m");
    }

    #[test]
    fn aggregate_keeps_aspects_apart_within_one_root() {
        // One decision can draw a row out of the view and rearrange another
        // within it, both from the same root — two lines, first-seen order.
        let rows = vec![
            (
                placement("/arch", "2016", "/arch/2020", 3, Some(30)),
                RowAspect::Rearrangement,
            ),
            (
                placement("/arch", "raw", "/elsewhere", 5, Some(50)),
                RowAspect::Extraction,
            ),
        ];
        let lines = aggregate_placement_lines(&rows);
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0].aspect, RowAspect::Rearrangement);
        assert_eq!(lines[0].row.files, 3);
        assert_eq!(lines[1].aspect, RowAspect::Extraction);
        assert_eq!(lines[1].row.files, 5);
    }

    #[test]
    fn aggregate_bytes_are_all_or_omitted_per_line() {
        let rows = vec![
            (
                placement("/src", "m/01", "/arch/m", 1, Some(10)),
                RowAspect::Extraction,
            ),
            (
                placement("/src", "m/02", "/arch/m", 2, None),
                RowAspect::Extraction,
            ),
            (
                placement("/other", "x", "/arch/m", 4, Some(40)),
                RowAspect::Extraction,
            ),
        ];
        let lines = aggregate_placement_lines(&rows);
        assert_eq!(lines.len(), 2);
        // One unknown-size member omits the line's size, never a partial sum —
        // and never suppresses a sibling line's fully known total.
        assert_eq!(lines[0].row.bytes, None);
        assert_eq!(lines[1].row.bytes, Some(40));
    }

    #[test]
    fn aggregate_one_row_group_is_the_row_unchanged() {
        let row = placement("/src", "m/03", "/arch/m/03", 1_005, Some(10_050));
        let rows = vec![(row.clone(), RowAspect::Arrival)];
        let lines = aggregate_placement_lines(&rows);
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0].row.rel_prefix, row.rel_prefix);
        assert_eq!(lines[0].row.destination_path, row.destination_path);
        assert_eq!(lines[0].row.files, row.files);
        assert_eq!(lines[0].row.bytes, row.bytes);
    }

    #[test]
    fn aggregate_groups_by_root_in_first_seen_order() {
        let rows = vec![
            (
                placement("/a", "x", "/arch", 1, Some(1)),
                RowAspect::Extraction,
            ),
            (
                placement("/b", "y", "/arch", 2, Some(2)),
                RowAspect::Extraction,
            ),
            (
                placement("/a", "z", "/arch", 4, Some(4)),
                RowAspect::Extraction,
            ),
        ];
        let lines = aggregate_placement_lines(&rows);
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0].row.root_path, "/a");
        assert_eq!(lines[0].row.files, 5);
        assert_eq!(lines[1].row.root_path, "/b");
        assert_eq!(lines[1].row.files, 2);
    }

    #[test]
    fn row_aspect_covers_the_boundary_matrix() {
        use RowAspect::*;
        let expected = [
            ((true, true), Rearrangement), // crossed nothing
            ((true, false), Extraction),   // left here
            ((false, true), Arrival),      // entered here
            ((false, false), Outside),     // neither endpoint is ours
        ];
        for ((origin, destination), want) in expected {
            assert_eq!(
                row_aspect(origin, destination),
                want,
                "origin_in_view={origin} destination_in_view={destination}"
            );
        }
    }

    #[test]
    fn classify_row_reads_the_boundary_from_prefixes() {
        let view = vec!["/archive".to_string()];
        // Both endpoints under the view — a curation pass within the archive.
        assert_eq!(
            classify_row(&mk_extraction("/archive", "2016", "/archive/2020"), &view),
            RowAspect::Rearrangement
        );
        // Origin only: content left this place.
        assert_eq!(
            classify_row(&mk_extraction("/archive", "2016", "/elsewhere"), &view),
            RowAspect::Extraction
        );
        // Destination only: content entered it.
        assert_eq!(
            classify_row(
                &mk_extraction("/Volumes/sd", "dcim", "/archive/2020"),
                &view
            ),
            RowAspect::Arrival
        );
        // Neither — the card sees rows fetched without a view filter.
        assert_eq!(
            classify_row(&mk_extraction("/Volumes/sd", "dcim", "/elsewhere"), &view),
            RowAspect::Outside
        );
    }

    #[test]
    fn classify_row_respects_segment_boundaries() {
        // "/archive/2016b" is not under "/archive/2016" — the same segment
        // rule scopes_touch enforces, reached through the row's joined path.
        let view = vec!["/archive/2016".to_string()];
        assert_eq!(
            classify_row(&mk_extraction("/archive", "2016b", "/elsewhere"), &view),
            RowAspect::Outside
        );
        assert_eq!(
            classify_row(&mk_extraction("/archive", "2016", "/elsewhere"), &view),
            RowAspect::Extraction
        );
    }

    #[test]
    fn classify_row_global_view_has_no_boundary() {
        // No prefixes means no boundary to cross: everything is inside.
        assert_eq!(
            classify_row(&mk_extraction("/Volumes/sd", "dcim", "/archive"), &[]),
            RowAspect::Rearrangement
        );
    }

    #[test]
    fn classify_row_matches_any_of_several_prefixes() {
        let view = vec!["/archive".to_string(), "/Volumes/sd".to_string()];
        assert_eq!(
            classify_row(
                &mk_extraction("/Volumes/sd", "dcim", "/archive/2020"),
                &view
            ),
            RowAspect::Rearrangement
        );
    }
}
