//! Fixtures shared across the splitter's test corpus and the once-rules
//! tests — genuinely cross-cutting builders only.

use std::collections::{HashMap, HashSet};

use crate::core::domain::extraction::{DecisionExtraction, OriginDisposition};
use crate::core::domain::fate::DecisionFamily;
use crate::core::domain::object::{ArchiveCopy, ArchiveLocation};
use crate::core::domain::resolution::{FromHereDirectory, FromHereEvidence};
use crate::core::domain::source::Source;
use crate::notes::Note;
use crate::story::domain::place::{DecisionInfo, StoryInputs, StoryParams, StoryPlace};
use crate::story::domain::splitter::build_places;

/// Archive locations all standing on one root, open or closed. The root id
/// is a constant: nothing in the story lens reads it — the standing beside
/// it is the whole point.
pub(super) fn to_locations(paths: &[&str], suspended: bool) -> Vec<ArchiveLocation> {
    paths
        .iter()
        .map(|p| ArchiveLocation {
            path: p.to_string(),
            root: ArchiveCopy {
                root_id: 9,
                root_suspended: suspended,
            },
        })
        .collect()
}

pub(super) fn src(id: i64, rel: &str, object_id: Option<i64>) -> Source {
    Source {
        id,
        root_id: 1,
        root_path: "/root".to_string(),
        rel_path: rel.to_string(),
        object_id,
        size: 100,
        mtime: 0,
        excluded: false,
        object_excluded: None,
        device: 0,
        inode: 0,
        partial_hash: String::new(),
        basis_rev: 0,
        root_role: "source".to_string(),
        root_suspended: false,
        decision_id: None,
    }
}

pub(super) fn stamped(id: i64, rel: &str, object_id: Option<i64>, decision: i64) -> Source {
    Source {
        decision_id: Some(decision),
        ..src(id, rel, object_id)
    }
}

pub(super) fn excluded_src(id: i64, rel: &str, object_id: Option<i64>, decision: i64) -> Source {
    Source {
        excluded: true,
        ..stamped(id, rel, object_id, decision)
    }
}

pub(super) fn note_at(id: i64, rel: &str, text: &str) -> Note {
    Note {
        id,
        root_id: 1,
        rel_path: rel.to_string(),
        text: text.to_string(),
        created_at: id * 100,
    }
}

pub(super) fn extraction(
    decision_id: i64,
    rel_prefix: &str,
    files: i64,
    destination: &str,
) -> DecisionExtraction {
    DecisionExtraction {
        decision_id,
        root_id: 1,
        root_path: "/root".to_string(),
        rel_prefix: rel_prefix.to_string(),
        files,
        bytes: Some(files * 100),
        destination_root_id: Some(2),
        destination_path: destination.to_string(),
        disposition: Some(OriginDisposition::Relocated),
    }
}

pub(super) fn dinfo(family: DecisionFamily, created_at: i64, reason: Option<&str>) -> DecisionInfo {
    DecisionInfo {
        family,
        created_at,
        reason: reason.map(str::to_string),
    }
}

pub(super) fn no_dust() -> StoryParams {
    StoryParams {
        dust_floor_files: 0,
        dust_floor_bytes: 0,
        ..StoryParams::default()
    }
}

pub(super) struct Fixture {
    pub(super) present: Vec<Source>,
    pub(super) absent: Vec<Source>,
    pub(super) archived: HashSet<i64>,
    pub(super) archive_copies: HashMap<i64, Vec<ArchiveCopy>>,
    pub(super) from_here: HashMap<i64, Vec<FromHereDirectory>>,
    pub(super) extractions: Vec<DecisionExtraction>,
    pub(super) decisions: HashMap<i64, DecisionInfo>,
    pub(super) notes: Vec<Note>,
    pub(super) archive_locations: HashMap<i64, Vec<ArchiveLocation>>,
    pub(super) bases: Vec<String>,
}

impl Fixture {
    pub(super) fn new() -> Self {
        Self {
            present: vec![],
            absent: vec![],
            archived: HashSet::new(),
            archive_copies: HashMap::new(),
            from_here: HashMap::new(),
            extractions: vec![],
            decisions: HashMap::new(),
            notes: vec![],
            archive_locations: HashMap::new(),
            bases: vec!["/root".to_string(), "/archive".to_string()],
        }
    }

    /// A covered present source: hashed, archived, standing at one or
    /// more archive locations, every one of them on an open root.
    pub(super) fn covered(&mut self, id: i64, rel: &str, locations: &[&str]) {
        self.covered_at(id, rel, &to_locations(locations, false));
    }

    /// The same, with every copy standing behind a closed door.
    pub(super) fn covered_behind_a_door(&mut self, id: i64, rel: &str, locations: &[&str]) {
        self.covered_at(id, rel, &to_locations(locations, true));
    }

    pub(super) fn covered_at(&mut self, id: i64, rel: &str, locations: &[ArchiveLocation]) {
        let obj = 1000 + id;
        self.present.push(src(id, rel, Some(obj)));
        self.archived.insert(obj);
        self.archive_copies
            .insert(obj, locations.iter().map(|l| l.root).collect());
        self.archive_locations.insert(obj, locations.to_vec());
    }

    pub(super) fn build(&self, params: &StoryParams) -> StoryPlace {
        build_places(
            &StoryInputs {
                present: &self.present,
                absent: &self.absent,
                archived: &self.archived,
                from_here: FromHereEvidence::by_directory(&self.from_here),
                extractions: &self.extractions,
                decisions: &self.decisions,
                notes: &self.notes,
                archive_locations: &self.archive_locations,
                bases: &self.bases,
            },
            params,
        )
    }
}

pub(super) fn child_paths(place: &StoryPlace) -> Vec<&str> {
    place.children.iter().map(|c| c.rel_path.as_str()).collect()
}

pub(super) fn child<'a>(root: &'a StoryPlace, rel: &str) -> &'a StoryPlace {
    root.children
        .iter()
        .find(|p| p.rel_path == rel)
        .unwrap_or_else(|| panic!("expected place {rel:?}"))
}
