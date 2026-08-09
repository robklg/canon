//! The story review's compute op (`canon roots story`) — fetch, enrichments,
//! and the place lens behind one typed result.
//!
//! The third lens over `RootStory`: the readiness gate, the book compile,
//! and this view read the same fetched world by construction — never a
//! second fetch. This is a query path, so it renders from DB projections
//! only: the compile's receipt recovery (`collect_origins`) is deliberately
//! out of bounds here (consumption-readiness ADR).

use anyhow::{bail, Result};
use rusqlite::Connection;
use std::collections::HashMap;

use crate::core::domain::resolution::{build_account, ResolutionAccount};
use crate::core::ops::root_story::{fetch_root_story, RootStory};
use crate::domain::root::Root;
use crate::domain::story::{
    assign_reason_sites, build_places, DecisionInfo, StoryInputs, StoryParams, StoryPlace,
};
use crate::repo;

/// Everything the interface renders — it classifies nothing.
#[derive(Debug)]
pub struct StoryReport {
    pub root: Root,
    /// When the earliest surviving row was first indexed (row evidence).
    pub first_indexed: Option<i64>,
    /// Fetch-time observation: an unreachable root's story renders as last
    /// observed — stated, never refused.
    pub reachable: bool,
    /// The place map: the root place with nested children.
    pub places: StoryPlace,
    /// The readiness account over the same fetch — the closing totals. The
    /// place sums fold to it (the agreement law).
    pub account: ResolutionAccount,
}

/// Compute a source root's story: one `RootStory` fetch, the compile's two
/// enrichments (notes, covered-copy locations), then the pure place lens.
///
/// Entry policy: an archive root is refused — the instrument serves the
/// letting-go judgment on source roots; the archive side is `canon trail`'s
/// and the composition card's. A suspended or unreachable root reads fine.
pub fn compute_story(conn: &Connection, root_id: i64, params: &StoryParams) -> Result<StoryReport> {
    let story = fetch_root_story(conn, root_id)?;
    if story.root.role == "archive" {
        bail!(
            "Cannot read the story of {}: an archive root is where stories end up — its places are served by canon trail. The story review reads a source root toward letting go.",
            story.root.path
        );
    }
    report_over(conn, &story, params)
}

/// The lens over an already-fetched `RootStory`: enrichments + place map +
/// account, no fetch. The retirement ceremony composes the bound telling
/// through this over its *own* story, so the gate, the book, and the telling
/// read one world by construction (the one-fetch law). Entry policy (the
/// archive-role refusal) stays with `compute_story` — the ceremony's target
/// is already validated by `validate_retire_target`.
pub fn report_over(
    conn: &Connection,
    story: &RootStory,
    params: &StoryParams,
) -> Result<StoryReport> {
    let notes = repo::note::fetch_by_roots(conn, &[story.root.id])?;

    // Covered-copy locations for the "copies stand in" lines. Empty objects
    // never come back — the archived-ness SQL carries the contentless law
    // (`batch_find_archive_paths` requires size > 0 on the archive side).
    let mut object_ids: Vec<i64> = story.present.iter().filter_map(|s| s.object_id).collect();
    object_ids.sort_unstable();
    object_ids.dedup();
    let archive_locations = repo::object::batch_find_archive_paths(conn, &object_ids)?;

    let decisions: HashMap<i64, DecisionInfo> = story
        .decisions
        .iter()
        .map(|d| {
            (
                d.id,
                DecisionInfo {
                    family: story.stamp_families[&d.id],
                    created_at: d.created_at,
                    reason: d.reason.clone(),
                },
            )
        })
        .collect();
    let bases: Vec<String> = story.roots.iter().map(|r| r.path.clone()).collect();

    let mut places = build_places(
        &StoryInputs {
            present: &story.present,
            absent: &story.absent,
            archived: &story.archived,
            archived_from_here: &story.archived_from_here,
            extractions: &story.extractions,
            decisions: &decisions,
            notes: &notes,
            archive_locations: &archive_locations,
            bases: &bases,
        },
        params,
    );
    // The once-rule: each reasoned decision's full reason renders at its
    // first emitted slice in reading order; other slices cite the bare id.
    // Precomputed here so the tree carries the answer — rendering never
    // decides.
    assign_reason_sites(&mut places);
    let account = build_account(
        &story.present,
        &story.absent,
        &story.archived,
        &story.archived_from_here,
        &story.extractions,
        &story.stamp_families,
    );

    Ok(StoryReport {
        root: story.root.clone(),
        first_indexed: story.first_indexed,
        reachable: story.reachable,
        places,
        account,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::story::StoryPlace;
    use crate::repo::{insert_test_root, open_in_memory_for_test};
    use rusqlite::Connection as SqlConnection;

    #[allow(clippy::too_many_arguments)]
    fn insert_source(
        conn: &SqlConnection,
        root_id: i64,
        rel_path: &str,
        object_id: Option<i64>,
        size: i64,
        present: bool,
        excluded: bool,
        decision_id: Option<i64>,
    ) -> i64 {
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, object_id, device, inode, size, mtime,
                                  partial_hash, scanned_at, last_seen_at, present, excluded, decision_id)
             VALUES (?, ?, ?, 0, 0, ?, 0, 'hash', 10, 10, ?, ?, ?)",
            rusqlite::params![
                root_id,
                rel_path,
                object_id,
                size,
                present as i64,
                excluded as i64,
                decision_id
            ],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn insert_object(conn: &SqlConnection, hash: &str) -> i64 {
        conn.execute(
            "INSERT INTO objects (hash_type, hash_value) VALUES ('sha256', ?)",
            [hash],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn insert_decision(
        conn: &SqlConnection,
        command: &str,
        created_at: i64,
        reason: Option<&str>,
    ) -> i64 {
        conn.execute(
            "INSERT INTO decisions (command, command_line, status, canon_version, created_at, reason)
             VALUES (?1, 'test', 'completed', '0', ?2, ?3)",
            rusqlite::params![command, created_at, reason],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn insert_scope(
        conn: &SqlConnection,
        decision_id: i64,
        root_id: i64,
        root_path: &str,
        rel_prefix: &str,
    ) {
        conn.execute(
            "INSERT INTO decision_scopes (decision_id, root_id, root_path, rel_prefix)
             VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![decision_id, root_id, root_path, rel_prefix],
        )
        .unwrap();
    }

    fn insert_extraction(
        conn: &SqlConnection,
        decision_id: i64,
        root_id: i64,
        rel_prefix: &str,
        files: i64,
        destination: &str,
    ) {
        crate::repo::decision::replace_extractions(
            conn,
            &[crate::domain::extraction::DecisionExtraction {
                decision_id,
                root_id,
                root_path: "/r".to_string(),
                rel_prefix: rel_prefix.to_string(),
                files,
                bytes: Some(files * 100),
                destination_root_id: Some(999),
                destination_path: destination.to_string(),
                disposition: Some(crate::domain::extraction::OriginDisposition::Relocated),
            }],
        )
        .unwrap();
    }

    fn find_child<'a>(place: &'a StoryPlace, rel: &str) -> &'a StoryPlace {
        place
            .children
            .iter()
            .find(|c| c.rel_path == rel)
            .unwrap_or_else(|| {
                panic!(
                    "no child {rel}; children: {:?}",
                    place
                        .children
                        .iter()
                        .map(|c| &c.rel_path)
                        .collect::<Vec<_>>()
                )
            })
    }

    fn no_dust() -> StoryParams {
        StoryParams {
            dust_floor_files: 0,
            dust_floor_bytes: 0,
            ..StoryParams::default()
        }
    }

    #[test]
    fn end_to_end_story_over_a_seeded_root() {
        let conn = open_in_memory_for_test();
        let root = insert_test_root(&conn, "/r", "source", false);
        let archive = insert_test_root(&conn, "/archive", "archive", false);

        // Covered: an object standing in the archive.
        let obj = insert_object(&conn, "aaa");
        insert_source(
            &conn,
            root,
            "photos/a.jpg",
            Some(obj),
            100,
            true,
            false,
            None,
        );
        insert_source(
            &conn,
            archive,
            "media/a.jpg",
            Some(obj),
            100,
            true,
            false,
            None,
        );

        // An exclusion with scope and reason.
        let d_exclude = insert_decision(&conn, "exclude_set", 100, Some("installer junk"));
        insert_source(
            &conn,
            root,
            "old/setup.exe",
            None,
            100,
            true,
            true,
            Some(d_exclude),
        );
        insert_scope(&conn, d_exclude, root, "/r", "old");

        // An apply recorded in the extraction ledger.
        let d_apply = insert_decision(&conn, "apply", 200, Some("the Italy trip"));
        insert_scope(&conn, d_apply, root, "/r", "photos");
        insert_extraction(&conn, d_apply, root, "photos", 5, "/archive/media");

        // A note, and an unexplained absence.
        repo::note::insert(&conn, root, "keep", "still deciding").unwrap();
        insert_source(&conn, root, "gone/z.jpg", None, 100, false, false, None);

        let report = compute_story(&conn, root, &no_dust()).unwrap();

        assert_eq!(report.account.covered, 1);
        assert_eq!(report.account.excluded, 1);
        assert_eq!(report.account.unexplained_missing, 1);
        assert_eq!(report.account.archived_files, 5);

        let old = find_child(&report.places, "old");
        assert_eq!(old.acts[0].transition, "excluded");
        assert_eq!(old.acts[0].reason_summary().reasons[0].0, "installer junk");
        assert_eq!(old.standing.excluded, 1);

        let photos = find_child(&report.places, "photos");
        assert_eq!(photos.acts[0].transition, "archived");
        assert_eq!(photos.acts[0].files, 5);
        assert_eq!(
            photos.acts[0].destination.locations[0].path,
            "/archive/media"
        );
        assert_eq!(photos.standing.covered, 1);
        assert_eq!(photos.covered_where.locations[0].path, "/archive/media");

        let keep = find_child(&report.places, "keep");
        assert_eq!(keep.notes.len(), 1);
        assert_eq!(keep.notes[0].text, "still deciding");
    }

    #[test]
    fn report_over_is_compute_story_minus_the_fetch() {
        // The lens-split law: the ceremony composes the bound telling over
        // its own already-fetched story; the result must be exactly what
        // the live command computes. Compared through the rendering so any
        // divergence is visible in the terms a reader would meet.
        let conn = open_in_memory_for_test();
        let root = insert_test_root(&conn, "/r", "source", false);
        let archive = insert_test_root(&conn, "/archive", "archive", false);
        let obj = insert_object(&conn, "aaa");
        insert_source(
            &conn,
            root,
            "photos/a.jpg",
            Some(obj),
            100,
            true,
            false,
            None,
        );
        insert_source(
            &conn,
            archive,
            "media/a.jpg",
            Some(obj),
            100,
            true,
            false,
            None,
        );
        let d = insert_decision(&conn, "exclude_set", 100, Some("junk"));
        insert_source(&conn, root, "old/setup.exe", None, 100, true, true, Some(d));
        insert_scope(&conn, d, root, "/r", "old");

        let fetched = fetch_root_story(&conn, root).unwrap();
        let via_lens = report_over(&conn, &fetched, &no_dust()).unwrap();
        let via_command = compute_story(&conn, root, &no_dust()).unwrap();
        assert_eq!(
            crate::retire::story_lines(&via_lens, usize::MAX, 0),
            crate::retire::story_lines(&via_command, usize::MAX, 0),
        );
    }

    #[test]
    fn an_archive_root_is_refused_with_a_pointer() {
        let conn = open_in_memory_for_test();
        let archive = insert_test_root(&conn, "/archive", "archive", false);
        let err = compute_story(&conn, archive, &no_dust()).unwrap_err();
        assert!(err.to_string().contains("canon trail"), "{err}");
    }

    #[test]
    fn an_unknown_root_errors_plainly() {
        let conn = open_in_memory_for_test();
        let err = compute_story(&conn, 999, &no_dust()).unwrap_err();
        assert!(err.to_string().contains("not found"), "{err}");
    }

    #[test]
    fn an_empty_root_is_a_bare_story() {
        let conn = open_in_memory_for_test();
        let root = insert_test_root(&conn, "/r", "source", false);
        let report = compute_story(&conn, root, &no_dust()).unwrap();
        assert_eq!(report.places.rel_path, "");
        assert!(report.places.children.is_empty());
        assert!(report.places.undecided());
        assert_eq!(report.account.standing(), 0);
    }

    #[test]
    fn a_root_with_no_decisions_is_all_undecided() {
        let conn = open_in_memory_for_test();
        let root = insert_test_root(&conn, "/r", "source", false);
        let archive = insert_test_root(&conn, "/archive", "archive", false);
        let obj = insert_object(&conn, "bbb");
        insert_source(
            &conn,
            root,
            "stuff/f.jpg",
            Some(obj),
            100,
            true,
            false,
            None,
        );
        insert_source(&conn, archive, "m/f.jpg", Some(obj), 100, true, false, None);

        let report = compute_story(&conn, root, &no_dust()).unwrap();
        assert!(report.places.undecided());
        assert_eq!(report.places.standing.covered, 1);
        assert_eq!(report.places.covered_where.locations[0].path, "/archive/m");
    }

    #[test]
    fn a_suspended_root_still_reads() {
        let conn = open_in_memory_for_test();
        let root = insert_test_root(&conn, "/r", "source", true);
        insert_source(&conn, root, "x.jpg", None, 100, true, false, None);
        let report = compute_story(&conn, root, &no_dust()).unwrap();
        assert!(report.root.suspended);
        assert_eq!(report.places.standing.unresolved, 1);
    }

    #[test]
    fn zero_byte_covered_sources_claim_no_locations() {
        let conn = open_in_memory_for_test();
        let root = insert_test_root(&conn, "/r", "source", false);
        let archive = insert_test_root(&conn, "/archive", "archive", false);
        let obj = insert_object(&conn, "empty");
        insert_source(
            &conn,
            root,
            "world/level.lock",
            Some(obj),
            0,
            true,
            false,
            None,
        );
        insert_source(
            &conn,
            archive,
            "w/level.lock",
            Some(obj),
            0,
            true,
            false,
            None,
        );

        let report = compute_story(&conn, root, &no_dust()).unwrap();
        assert_eq!(
            report.places.standing.contentless, 1,
            "an empty source is contentless — never covered (the contentless law)"
        );
        assert_eq!(
            report.places.standing.covered, 0,
            "the universal empty object creates no coverage"
        );
        assert!(
            report.places.covered_where.is_empty(),
            "and it claims no locations"
        );
    }

    #[test]
    fn covered_locations_survive_the_sql_chunking_boundary() {
        let conn = open_in_memory_for_test();
        let root = insert_test_root(&conn, "/r", "source", false);
        let archive = insert_test_root(&conn, "/archive", "archive", false);
        // Past the ~999 SQL variable limit in one batch of object ids.
        for i in 0..1100 {
            let obj = insert_object(&conn, &format!("h{i}"));
            insert_source(
                &conn,
                root,
                &format!("bulk/f{i}.jpg"),
                Some(obj),
                100,
                true,
                false,
                None,
            );
            insert_source(
                &conn,
                archive,
                &format!("media/f{i}.jpg"),
                Some(obj),
                100,
                true,
                false,
                None,
            );
        }
        let report = compute_story(&conn, root, &no_dust()).unwrap();
        assert_eq!(report.account.covered, 1100);
        assert_eq!(
            report.places.covered_where.locations,
            vec![crate::domain::story::LocationCount {
                path: "/archive/media".into(),
                files: 1100
            }]
        );
    }
}
