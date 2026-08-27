//! Selecting the recorded movements that cross a place's boundary.
//!
//! The trail can be asked about a place, a time, or one decision. This is the
//! fourth axis: a **relation between two places** — what moved from here to
//! there, when, and why. The rule it rests on is one sentence:
//!
//! > A crossing is a row that crosses the view's boundary. A global view has
//! > no boundary of its own, so the named counterpart becomes the boundary.
//!
//! The second half is not a special case bolted onto the first — it is what
//! makes the two doors one computation. Standing at a drive and asking what
//! left, and standing nowhere while naming that drive, must return the same
//! rows for the same reason. Inferring membership from "is it in the view?"
//! cannot do that: globally every endpoint is in the view, so every row would
//! read as a rearrangement and every answer would be empty.
//!
//! No I/O.

use crate::core::domain::extraction::DecisionExtraction;
use crate::core::domain::path::path_is_under;

use super::placement::{classify_row, placement_in_view, RowAspect};

/// The boundary a crossings view measures against, and what narrows it.
pub struct CrossingFilter<'a> {
    /// The viewed scope, absolute. Empty = global: no boundary of its own.
    pub view: &'a [String],
    /// Narrow to rows drawn from at-or-under this location.
    pub origin: Option<&'a String>,
    /// Narrow to rows placed at-or-under this location.
    pub destination: Option<&'a String>,
}

impl CrossingFilter<'_> {
    /// The boundary this view measures against.
    ///
    /// A scoped view is its own boundary. A **global** view has none — every
    /// endpoint is inside it, so inference from view membership classifies
    /// everything as a rearrangement and returns nothing. So a global view
    /// borrows the named counterpart as its boundary, which is what makes
    /// asking globally about a drive mean "everything that drive ever gave,
    /// wherever it went" — and makes it the *same* computation as standing at
    /// the drive, rather than a second one that has to be kept in agreement.
    ///
    /// **When a global view names both counterparts, the deeper one takes the
    /// boundary where they nest.** Elsewhere the flags are pure narrowings
    /// that never touch the framing, which is what lets them compose with no
    /// special case; globally there is no view, so exactly one of them must be
    /// promoted from narrowing to framing, and which one is not free.
    ///
    /// Where the two do **not** nest, both survive the same rows and the
    /// choice only decides which header they report under, so the origin
    /// serves as a stable tie-break. Where one contains the other the row sets
    /// genuinely differ: asking about a drive and a folder inside it, with the
    /// *outer* path framing, puts both endpoints inside the boundary and the
    /// delivery reads as a rearrangement — the door answers "nothing crossed"
    /// about a delivery it holds a record of. Framing on the inner path keeps
    /// it, because from there the rest of the drive really is outside.
    ///
    /// Both readings are correct under this subsystem's own scope-dependence
    /// rule; the choice picks which of the two the reader is handed, and the
    /// deeper path is the more precise statement of the place being asked
    /// about — the same reasoning `ops::compute::build_scope_matches` already
    /// argues when several recorded scopes match one view.
    ///
    /// A global view naming no counterpart has no boundary at all, and every
    /// row would read as a rearrangement — which is not an empty answer but a
    /// false one, since content that crossed would be reported as having
    /// stayed put. The interface refuses that ask (`cli::is_boundless`),
    /// keyed on the **resolved scope** rather than on `--global`, because
    /// standing outside every known root reaches this state silently. This
    /// arm is the floor beneath that refusal, not a behaviour anyone meets.
    pub fn boundary(&self) -> &[String] {
        if !self.view.is_empty() {
            return self.view;
        }
        match (self.origin, self.destination) {
            (Some(origin), Some(destination)) => {
                // Strictly under: equal paths nest both ways and neither is
                // deeper, so they fall to the tie-break like any other pair.
                if destination != origin && path_is_under(destination, origin) {
                    std::slice::from_ref(destination)
                } else {
                    std::slice::from_ref(origin)
                }
            }
            (Some(origin), None) => std::slice::from_ref(origin),
            (None, Some(destination)) => std::slice::from_ref(destination),
            (None, None) => &[],
        }
    }
}

/// What one row is to a crossings view — the total answer, of which
/// the tests' `aspect` helper reads the yes/no half.
///
/// Three outcomes rather than two because "no crossing" has two meanings a
/// reader must be able to tell apart. A view whose every row stayed inside it
/// has to *say* "nothing crossed; this much was rearranged", or the silence
/// reads as "nothing ever happened here" — which is the opposite of the
/// truth about a heavily curated archive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrossingVerdict {
    /// It crossed the boundary, in this direction.
    Crossed(RowAspect),
    /// Selected by every named filter, but both endpoints are inside the
    /// boundary: it crossed nothing.
    Rearranged,
    /// Not this view's row at all — it failed a named filter, or neither end
    /// is ours.
    NotOurs,
}

/// Whether a row crosses this view's boundary, which way it reads, and — when
/// it does not — which of the two reasons applies.
///
/// Membership is [`placement_in_view`] on both ends: via [`classify_row`] for
/// the boundary test, and directly for the two narrowing filters. An
/// extraction row's locations are **observed placements**, so only a location
/// the asked-about place *contains* is inside it. Never `scopes_touch`: its
/// ancestor branch is how a common-prefix destination once manufactured
/// arrivals at sibling folders, and a counterpart named at a deep path is
/// exactly that shape — asking about `/archive/2016` must not borrow a row
/// whose recorded destination is the common prefix `/archive`.
pub fn crossing_verdict(row: &DecisionExtraction, filter: &CrossingFilter) -> CrossingVerdict {
    if let Some(origin) = filter.origin {
        if !placement_in_view(origin, &row.drawn_from()) {
            return CrossingVerdict::NotOurs;
        }
    }
    if let Some(destination) = filter.destination {
        if !placement_in_view(destination, &row.destination_path) {
            return CrossingVerdict::NotOurs;
        }
    }
    // Exhaustive with no `_` arm: a fifth aspect must be a compile error
    // here, not a row silently dropped from a historical claim.
    match classify_row(row, filter.boundary()) {
        RowAspect::Extraction => CrossingVerdict::Crossed(RowAspect::Extraction),
        RowAspect::Arrival => CrossingVerdict::Crossed(RowAspect::Arrival),
        // Crossed nothing. Counted where it belongs — as a rearrangement —
        // and never as a crossing in either direction.
        RowAspect::Rearrangement => CrossingVerdict::Rearranged,
        // Neither end is ours.
        RowAspect::Outside => CrossingVerdict::NotOurs,
    }
}

/// Which end of a crossing is the *counterpart*: the side outside the
/// boundary, which is the side the reader is asking about.
///
/// The rollups' own counterparty rule, spoken once more so the bare listing's
/// keys are the keys those lines already counted — which is what makes the
/// line count equal the counterparty count and the sums equal the rollup
/// sums, by construction rather than by care.
///
/// Total over the two aspects a crossing can have; the other two never reach
/// here, because [`crossing_verdict`] never reports them as crossings.
pub fn counterpart_of(row: &DecisionExtraction, aspect: RowAspect) -> &str {
    match aspect {
        // It left; where it went is the counterpart.
        RowAspect::Extraction => &row.destination_path,
        // It arrived; where it came from is the counterpart. Root-grained,
        // matching the card: an origin's sub-root detail is the delivery's,
        // not the counterpart key's.
        RowAspect::Arrival => &row.root_path,
        // Unreachable by construction — kept as arms rather than a panic so
        // a fifth aspect is a compile error and never a crash in a read path.
        RowAspect::Rearrangement | RowAspect::Outside => &row.destination_path,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::domain::extraction::OriginDisposition;

    fn row(origin_root: &str, rel: &str, destination: &str) -> DecisionExtraction {
        DecisionExtraction {
            decision_id: 1,
            root_id: 1,
            root_path: origin_root.to_string(),
            rel_prefix: rel.to_string(),
            files: 1,
            bytes: Some(10),
            destination_root_id: Some(2),
            destination_path: destination.to_string(),
            disposition: Some(OriginDisposition::Retained),
        }
    }

    fn view(prefixes: &[&str]) -> Vec<String> {
        prefixes.iter().map(|p| p.to_string()).collect()
    }

    /// The yes/no half of the verdict — what most of these cases are about.
    /// Not on the type itself: production reads the whole verdict, and a
    /// convenience with no production caller is dead weight in a law's file.
    fn aspect(row: &DecisionExtraction, filter: &CrossingFilter) -> Option<RowAspect> {
        match crossing_verdict(row, filter) {
            CrossingVerdict::Crossed(aspect) => Some(aspect),
            CrossingVerdict::Rearranged | CrossingVerdict::NotOurs => None,
        }
    }

    fn filter<'a>(
        view: &'a [String],
        origin: Option<&'a String>,
        destination: Option<&'a String>,
    ) -> CrossingFilter<'a> {
        CrossingFilter {
            view,
            origin,
            destination,
        }
    }

    // ------------------------------------------------------------------
    // What is and is not a crossing
    // ------------------------------------------------------------------

    #[test]
    fn an_arrival_crosses_inward() {
        let v = view(&["/archive"]);
        let r = row("/Volumes/sd", "2016", "/archive/Media/2016");
        assert_eq!(
            aspect(&r, &filter(&v, None, None)),
            Some(RowAspect::Arrival)
        );
    }

    #[test]
    fn an_extraction_crosses_outward() {
        let v = view(&["/Volumes/sd"]);
        let r = row("/Volumes/sd", "2016", "/archive/Media/2016");
        assert_eq!(
            aspect(&r, &filter(&v, None, None)),
            Some(RowAspect::Extraction)
        );
    }

    /// Both endpoints inside: content moved *within* the view, so it crossed
    /// nothing. Counting it as a crossing would read as activity that never
    /// left, in whichever direction happened to be asked about.
    #[test]
    fn a_rearrangement_is_never_a_crossing() {
        let v = view(&["/archive"]);
        let r = row("/archive", "2016", "/archive/Media/2016");
        assert_eq!(aspect(&r, &filter(&v, None, None)), None);
    }

    #[test]
    fn a_row_touching_neither_end_is_never_a_crossing() {
        let v = view(&["/archive"]);
        let r = row("/Volumes/sd", "2016", "/elsewhere/Media");
        assert_eq!(aspect(&r, &filter(&v, None, None)), None);
    }

    // ------------------------------------------------------------------
    // The named counterpart: descendant-or-equal, never ancestor
    // ------------------------------------------------------------------

    /// The phantom-arrivals class, refused through the new door for **both**
    /// flags.
    ///
    /// Read the name from the *row's* side: a row whose recorded location
    /// sits above the place being asked about never matches it. A row's
    /// claim is "my files lie under this location", which says nothing about
    /// any particular folder beneath it — `/archive` being the recorded
    /// destination of files that went to `2016/01` and `2016/02` implies
    /// nothing about `2016/03`. Matching that direction is how this subsystem
    /// once manufactured history, and it is why membership is
    /// `placement_in_view` and never `scopes_touch`.
    ///
    /// The mirror — a counterpart above a row's location, which genuinely
    /// does contain it and so genuinely does match — is
    /// [`a_named_counterpart_matches_at_any_depth`].
    #[test]
    fn a_counterpart_above_the_view_is_never_matched() {
        let v = view(&["/archive"]);
        let r = row("/Volumes/sd/photos/2016", "", "/archive/Media/2016");

        // Asking about a place deeper than the row records must not match the
        // row's shallower recorded location.
        let deep_origin = "/Volumes/sd/photos/2016/italy".to_string();
        assert_eq!(aspect(&r, &filter(&v, Some(&deep_origin), None)), None);

        let deep_destination = "/archive/Media/2016/italy".to_string();
        assert_eq!(aspect(&r, &filter(&v, None, Some(&deep_destination))), None);

        // The historical shape itself: a legacy row recorded at a lossy
        // common prefix must stay silent below that prefix rather than
        // manufacturing an arrival at a sibling that received nothing.
        let lossy = row("/Volumes/sd", "", "/archive");
        let sibling = view(&["/archive/Media/2016"]);
        assert_eq!(aspect(&lossy, &filter(&sibling, None, None)), None);
        // And it still answers at and above its own recorded location.
        assert_eq!(
            aspect(&lossy, &filter(&v, None, None)),
            Some(RowAspect::Arrival)
        );
    }

    #[test]
    fn a_named_counterpart_matches_at_any_depth() {
        let v = view(&["/archive"]);
        let r = row("/Volumes/sd", "photos/2016", "/archive/Media/2016");

        for origin in [
            "/Volumes/sd",
            "/Volumes/sd/photos",
            "/Volumes/sd/photos/2016",
        ] {
            let o = origin.to_string();
            assert_eq!(
                aspect(&r, &filter(&v, Some(&o), None)),
                Some(RowAspect::Arrival),
                "{origin}"
            );
        }
    }

    #[test]
    fn both_flags_compose_to_select_exactly_that_pair() {
        let v = view(&["/archive"]);
        let wanted = row("/Volumes/sd", "2016", "/archive/Media/2016");
        let other_origin = row("/Volumes/cf", "2016", "/archive/Media/2016");
        let other_destination = row("/Volumes/sd", "2016", "/archive/Documents");

        let o = "/Volumes/sd".to_string();
        let d = "/archive/Media".to_string();
        let f = filter(&v, Some(&o), Some(&d));

        assert_eq!(aspect(&wanted, &f), Some(RowAspect::Arrival));
        assert_eq!(aspect(&other_origin, &f), None);
        assert_eq!(aspect(&other_destination, &f), None);
    }

    /// A path is bytes, not a pattern. `_` and `%` are ordinary characters in
    /// a real folder name, and a counterpart at `alphaXbeta` must never be
    /// borrowed by an ask for `alpha_beta` — in either direction.
    #[test]
    fn crossing_matching_treats_wildcard_bytes_as_literal() {
        let v = view(&["/archive"]);
        let r = row("/Volumes/alphaXbeta", "", "/archive/pctYdir");

        let literal_origin = "/Volumes/alpha_beta".to_string();
        assert_eq!(aspect(&r, &filter(&v, Some(&literal_origin), None)), None);

        let literal_destination = "/archive/pct%dir".to_string();
        assert_eq!(
            aspect(&r, &filter(&v, None, Some(&literal_destination))),
            None
        );

        // And the reverse direction: a real `_` folder is not matched by an
        // ask that happens to sit where a wildcard would have expanded.
        let underscore = row("/Volumes/alpha_beta", "", "/archive/Media");
        let x_form = "/Volumes/alphaXbeta".to_string();
        assert_eq!(aspect(&underscore, &filter(&v, Some(&x_form), None)), None);
    }

    // ------------------------------------------------------------------
    // The boundary rule itself
    // ------------------------------------------------------------------

    /// Standing at a drive and naming that drive from nowhere are the same
    /// question, and must be the same computation. The second half of the
    /// assertion is why the borrow exists at all: without it, a global view
    /// contains every endpoint, so every row reads as a rearrangement and the
    /// door answers nothing.
    #[test]
    fn a_global_view_borrows_the_named_counterpart_as_its_boundary() {
        let rows = [
            row("/Volumes/sd", "2016", "/archive/Media/2016"),
            row("/Volumes/sd", "2017", "/elsewhere/backup"),
            row("/Volumes/cf", "2016", "/archive/Media/2016"),
        ];

        let standing = view(&["/Volumes/sd"]);
        let global: Vec<String> = Vec::new();
        let drive = "/Volumes/sd".to_string();

        let from_standing: Vec<Option<RowAspect>> = rows
            .iter()
            .map(|r| aspect(r, &filter(&standing, None, None)))
            .collect();
        let from_global: Vec<Option<RowAspect>> = rows
            .iter()
            .map(|r| aspect(r, &filter(&global, Some(&drive), None)))
            .collect();
        assert_eq!(from_standing, from_global);
        assert_eq!(
            from_global,
            vec![
                Some(RowAspect::Extraction),
                Some(RowAspect::Extraction),
                None
            ]
        );

        // Without the borrow — a global view judged on view membership alone
        // — every endpoint is inside and nothing crosses.
        for r in &rows {
            assert_eq!(classify_row(r, &global), RowAspect::Rearrangement);
        }
    }

    /// A destination named alone takes the boundary, so the door answers
    /// "everything that ever landed here, from wherever" without a scope.
    #[test]
    fn a_global_view_borrows_a_named_destination_too() {
        let global: Vec<String> = Vec::new();
        let d = "/archive/Media".to_string();
        let r = row("/Volumes/sd", "2016", "/archive/Media/2016");
        assert_eq!(
            aspect(&r, &filter(&global, None, Some(&d))),
            Some(RowAspect::Arrival)
        );
    }

    /// Where the two named counterparts **nest**, the choice of boundary is
    /// not a matter of which header the same rows report under: the row sets
    /// differ. Framing on the outer path puts both endpoints inside it, so a
    /// delivery the ledger holds a record of reads as a rearrangement and the
    /// door answers "nothing crossed". The deeper path keeps it, because from
    /// there the rest of the outer place really is outside.
    #[test]
    fn where_named_counterparts_nest_the_deeper_one_takes_the_boundary() {
        let global: Vec<String> = Vec::new();
        // An intra-archive curation pass: /arch/Media -> /arch/Curated.
        let r = row("/arch", "Media", "/arch/Curated");

        let outer = "/arch".to_string();
        let inner = "/arch/Curated".to_string();

        // Asked as "what did /arch give to /arch/Curated?", the inner path
        // frames and the delivery survives.
        let f = filter(&global, Some(&outer), Some(&inner));
        assert_eq!(f.boundary(), std::slice::from_ref(&inner));
        assert_eq!(aspect(&r, &f), Some(RowAspect::Arrival));

        // The mirror: origin deeper than destination keeps the origin, and
        // the same delivery reads outbound.
        let deep_origin = "/arch/Media".to_string();
        let shallow_destination = "/arch".to_string();
        let g = filter(&global, Some(&deep_origin), Some(&shallow_destination));
        assert_eq!(g.boundary(), std::slice::from_ref(&deep_origin));
        assert_eq!(aspect(&r, &g), Some(RowAspect::Extraction));

        // Equal paths nest both ways and neither is deeper: they fall to the
        // tie-break, and asking what a place gave itself crosses nothing.
        let same = "/arch".to_string();
        let h = filter(&global, Some(&same), Some(&same));
        assert_eq!(h.boundary(), std::slice::from_ref(&same));
        assert_eq!(aspect(&r, &h), None);
    }

    /// Where the two named counterparts do **not** nest, both framings keep
    /// the same rows and the choice is header-only, so the origin serves as a
    /// stable tie-break: the row reports under the same aspect it did with
    /// the origin alone.
    #[test]
    fn a_second_named_counterpart_narrows_without_swapping_the_boundary() {
        let global: Vec<String> = Vec::new();
        let o = "/Volumes/sd".to_string();
        let d = "/archive/Media".to_string();
        let r = row("/Volumes/sd", "2016", "/archive/Media/2016");

        let origin_only = aspect(&r, &filter(&global, Some(&o), None));
        let both = aspect(&r, &filter(&global, Some(&o), Some(&d)));
        assert_eq!(origin_only, Some(RowAspect::Extraction));
        assert_eq!(both, origin_only);
    }

    /// A scoped view is its own boundary and never borrows: naming a
    /// counterpart narrows what is asked, not where the reader stands.
    #[test]
    fn a_scoped_view_keeps_its_own_boundary_when_a_counterpart_is_named() {
        let v = view(&["/archive"]);
        let o = "/Volumes/sd".to_string();
        let f = filter(&v, Some(&o), None);
        assert_eq!(f.boundary(), v.as_slice());

        let r = row("/Volumes/sd", "2016", "/archive/Media/2016");
        assert_eq!(aspect(&r, &f), Some(RowAspect::Arrival));
    }

    /// The exhaustiveness, asserted rather than assumed: every aspect the
    /// four-cell table can produce has a stated answer here, and the two that
    /// are not crossings say so by name.
    #[test]
    fn the_law_match_covers_every_aspect() {
        let v = view(&["/archive"]);
        let f = filter(&v, None, None);
        let cases = [
            (
                row("/Volumes/sd", "", "/archive/a"),
                Some(RowAspect::Arrival),
            ),
            (
                row("/archive", "a", "/elsewhere"),
                Some(RowAspect::Extraction),
            ),
            (row("/archive", "a", "/archive/b"), None),
            (row("/Volumes/sd", "", "/elsewhere"), None),
        ];
        for (r, expected) in cases {
            assert_eq!(aspect(&r, &f), expected, "{r:?}");
        }
    }

    // ------------------------------------------------------------------
    // Which end is the counterpart
    // ------------------------------------------------------------------

    /// The two ways a row can fail to be a crossing stay distinguishable:
    /// a view that only rearranged must be able to say so, or its silence
    /// reads as "nothing ever happened here".
    #[test]
    fn a_non_crossing_says_which_kind_it_is() {
        let v = view(&["/archive"]);
        let f = filter(&v, None, None);
        assert_eq!(
            crossing_verdict(&row("/archive", "a", "/archive/b"), &f),
            CrossingVerdict::Rearranged
        );
        assert_eq!(
            crossing_verdict(&row("/Volumes/sd", "", "/elsewhere"), &f),
            CrossingVerdict::NotOurs
        );

        // A row excluded by a named filter is `NotOurs`, never `Rearranged`:
        // it was not judged against the boundary at all, and counting it as
        // rearranged would invent activity inside the view.
        let other = "/Volumes/cf".to_string();
        assert_eq!(
            crossing_verdict(
                &row("/archive", "a", "/archive/b"),
                &filter(&v, Some(&other), None)
            ),
            CrossingVerdict::NotOurs
        );
    }

    #[test]
    fn the_counterpart_is_the_end_outside_the_boundary() {
        let r = row("/Volumes/sd", "2016", "/archive/Media/2016");
        assert_eq!(counterpart_of(&r, RowAspect::Arrival), "/Volumes/sd");
        assert_eq!(
            counterpart_of(&r, RowAspect::Extraction),
            "/archive/Media/2016"
        );
    }
}
