//! The place: one node on the story's map, its standing, and the once-rule
//! that sites each reasoned decision's full text at its first emitted slice.
//!
//! No I/O anywhere here; callers supply everything fetched.

use std::collections::HashMap;
use std::collections::HashSet;

use super::acts::ActGroup;
use super::locations::LocationAggregate;
use crate::core::domain::extraction::DecisionExtraction;
use crate::core::domain::fate::{fate_transition, DecisionFamily, FateAspect};
use crate::domain::note::Note;
use crate::domain::source::Source;

/// Named calibratable constants (the sweep discipline). Defaults were
/// calibrated against the real archive 2026-08-04 across three root shapes;
/// recalibration changes constants, never code paths.
#[derive(Debug, Clone, Copy)]
pub struct StoryParams {
    /// Children whose standing proportions differ by no more than this tell
    /// the same line and merge into their parent place.
    pub signature_tolerance: f64,
    /// A child below both dust floors lifts into its parent rather than
    /// earning a place line of its own.
    pub dust_floor_files: i64,
    pub dust_floor_bytes: i64,
    /// Location entries per "where" line before the counted remainder.
    pub where_cap: usize,
}

impl Default for StoryParams {
    fn default() -> Self {
        Self {
            signature_tolerance: 0.15,
            dust_floor_files: 20,
            dust_floor_bytes: 5_000_000,
            where_cap: 3,
        }
    }
}

/// What the splitter needs to know about one decision.
#[derive(Debug, Clone)]
pub struct DecisionInfo {
    pub family: DecisionFamily,
    pub created_at: i64,
    pub reason: Option<String>,
}

/// Everything the place walk consumes, decomposed from the fetched story.
/// The ops layer supplies borrowed slices; no repo types cross this boundary.
pub struct StoryInputs<'a> {
    pub present: &'a [Source],
    pub absent: &'a [Source],
    /// Object ids among the present rows verified present in the archive.
    pub archived: &'a HashSet<i64>,
    /// Subset archived *from this root* (extraction-linked, object-grain) —
    /// the covered/archived standing split's evidence.
    pub archived_from_here: &'a HashSet<i64>,
    /// Extraction rows whose origin is this root — the archived acts.
    pub extractions: &'a [DecisionExtraction],
    pub decisions: &'a HashMap<i64, DecisionInfo>,
    pub notes: &'a [Note],
    /// Object id → full paths of the archive copies. Zero-byte objects are
    /// excluded upstream (the book's contentless gate, reused), so a covered
    /// empty file counts in the standing but claims no locations.
    pub archive_locations: &'a HashMap<i64, Vec<String>>,
    /// Known root paths — the legibility bases for every "where" answer.
    pub bases: &'a [String],
}

/// Present standings and record-quality facts attributed to one place.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PlaceStanding {
    /// Archived from here, the copy standing in the archive — the
    /// deliberate side of the covered/archived split (extraction-linked,
    /// object-grain).
    pub archived: i64,
    pub covered: i64,
    /// Empty files — contentless: all shape, no content; they claim no
    /// coverage and no locations, and they never block (the contentless
    /// law).
    pub contentless: i64,
    pub excluded: i64,
    /// Subset of `excluded`: rows carrying no decision stamp at all —
    /// exclusion is always a deliberate act, so a stampless row evidences
    /// a decision whose record is absent (pre-provenance, or recording
    /// off). Drives the no-record honesty marker at row grain: a place-level
    /// gate was wrong in both directions (a mixed place's stampless rows
    /// earned no marker; an undecided place's stamped rows earned a false
    /// one).
    pub excluded_stampless: i64,
    pub unresolved: i64,
    /// Subset of `unresolved`: never hashed — cannot be content-verified.
    pub unhashed_unresolved: i64,
    /// Absent without a recorded deletion — a record-quality fact.
    pub missing_unexplained: i64,
}

impl PlaceStanding {
    pub fn is_empty(&self) -> bool {
        self.archived == 0
            && self.covered == 0
            && self.contentless == 0
            && self.excluded == 0
            && self.unresolved == 0
            && self.missing_unexplained == 0
    }
}

/// One place on the map: a node the walk emitted, with everything under it
/// attributed by deepest match.
#[derive(Debug)]
pub struct StoryPlace {
    /// Path relative to the root; `""` is the root itself.
    pub rel_path: String,
    /// The acts, grouped for the what/why register; empty = no decision here.
    pub acts: Vec<ActGroup>,
    pub standing: PlaceStanding,
    /// Where this place's covered copies stand — observed, nobody chose it.
    pub covered_where: LocationAggregate,
    /// The user's own testimony at this place, oldest first.
    pub notes: Vec<Note>,
    /// Distinct folders holding content merged into this line.
    pub folder_breadth: u32,
    pub children: Vec<StoryPlace>,
}

impl StoryPlace {
    /// No decision here: nothing was ever acted on at or under this place
    /// (its standing is evidence without an act).
    pub fn undecided(&self) -> bool {
        self.acts.is_empty()
    }

    /// The excluded standing line says nothing the act register hasn't:
    /// the excluded standing is exactly what the excluded-transition
    /// performed acts narrate — same count, all still standing. Only the
    /// excluded line is ever coincidence-omittable — covered/unresolved/
    /// missing are spec-protected and render regardless — and the match is
    /// exact both ways: unaccounted standing (a stampless exclusion) fails
    /// the sum, and a tombstone-carrying slice fails it too (the act's
    /// whole-history count exceeds what stands, so omitting the standing
    /// line would misread as all of it still standing). Other buckets
    /// don't guard the omission (amended 2026-08-04, the excluded-twice
    /// friction): their own lines still render, and a bare restatement of
    /// the act count beside them adds nothing.
    pub fn standing_coincides(&self) -> bool {
        let excluded_word = fate_transition(DecisionFamily::Exclude, FateAspect::Present)
            .expect("exclude/present is a registered transition")
            .as_str();
        let mut acted_files = 0i64;
        let mut acted_present = 0i64;
        for group in &self.acts {
            if group.transition == excluded_word && !group.observed {
                acted_files += group.files;
                acted_present += group.present_files;
            }
        }
        self.standing.excluded == acted_present && acted_present == acted_files
    }
}

/// Site each reasoned decision's full reason at its first emitted slice in
/// pre-order (= render order; children are `rel_path`-sorted) and mark
/// every other slice as a bare-id citation — the reader meets the full
/// reason the first time they meet the id, and every later cite is a
/// backward reference (a widest-slice site was tried and read wrong: the
/// reason landed deep in the tree while the top-of-story slices cited
/// forward to it). A post-pass over the built tree, deliberately: the fold
/// composes first, so the site is an *emitted* slice, and the once-rule is
/// precomputed — never render-order-coupled.
pub fn assign_reason_sites(root: &mut StoryPlace) {
    // Pass 1: per reasoned decision, the pre-order index of its first slice.
    fn collect(place: &StoryPlace, index: &mut usize, sites: &mut HashMap<i64, usize>) {
        let my_index = *index;
        *index += 1;
        for group in &place.acts {
            for decision in &group.decisions {
                if decision.reason.is_some() {
                    sites.entry(decision.id).or_insert(my_index);
                }
            }
        }
        for child in &place.children {
            collect(child, index, sites);
        }
    }
    // Pass 2: mark each slice — the site renders the reason, the rest cite.
    fn apply(place: &mut StoryPlace, index: &mut usize, sites: &HashMap<i64, usize>) {
        let my_index = *index;
        *index += 1;
        for group in &mut place.acts {
            for decision in &mut group.decisions {
                if let Some(site) = sites.get(&decision.id) {
                    decision.reason_here = *site == my_index;
                }
            }
        }
        for child in &mut place.children {
            apply(child, index, sites);
        }
    }
    let mut sites = HashMap::new();
    collect(root, &mut 0, &mut sites);
    apply(root, &mut 0, &sites);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standing_coincidence_is_exact_both_ways() {
        let excluded_group = |files: i64, present_files: i64| ActGroup {
            transition: fate_transition(DecisionFamily::Exclude, FateAspect::Present)
                .expect("registered")
                .as_str(),
            observed: false,
            destination: LocationAggregate::default(),
            files,
            present_files,
            bytes: None,
            moved: None,
            copied: None,
            decisions: vec![],
        };
        let place = |standing: PlaceStanding, acts: Vec<ActGroup>| StoryPlace {
            rel_path: "old".to_string(),
            acts,
            standing,
            covered_where: LocationAggregate::default(),
            notes: vec![],
            folder_breadth: 1,
            children: vec![],
        };
        let excluded_only = |n: i64| PlaceStanding {
            excluded: n,
            ..PlaceStanding::default()
        };

        // The stutter: standing exactly what the act narrates, all standing.
        assert!(place(excluded_only(2), vec![excluded_group(2, 2)]).standing_coincides());
        // Unaccounted standing (a stampless exclusion) fails the sum.
        assert!(!place(excluded_only(3), vec![excluded_group(2, 2)]).standing_coincides());
        // A tombstone-carrying slice fails: the act's whole-history count
        // exceeds what stands — omitting the standing line would misread.
        assert!(!place(excluded_only(2), vec![excluded_group(3, 2)]).standing_coincides());
        // Other buckets don't guard the omission (amended 2026-08-04, the
        // excluded-twice friction): covered renders its own line; the
        // exact excluded coincidence still omits the restatement.
        assert!(place(
            PlaceStanding {
                covered: 1,
                excluded: 2,
                ..PlaceStanding::default()
            },
            vec![excluded_group(2, 2)]
        )
        .standing_coincides());
        // A non-exclusion act never accounts for excluded standing.
        let mut archived = excluded_group(2, 2);
        archived.transition = fate_transition(DecisionFamily::Archive, FateAspect::Present)
            .expect("registered")
            .as_str();
        assert!(!place(excluded_only(2), vec![archived]).standing_coincides());
    }
}
