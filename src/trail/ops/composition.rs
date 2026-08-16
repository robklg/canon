//! Composition operations — the trail's present-tense complement.
//!
//! `compute_composition` answers "what does this place consist of, right
//! now?", a different question from the trail's "what happened here?"
//! (`ops::trail`). Deliberately a sibling op, not computed inside trail
//! rendering, so later surfaces (a subcommand, survey, the TUI) can reuse it
//! without untangling from trail's rendering concerns.
//!
//! Read-only: no transaction, `&Connection` like `compute_trail`.

use std::collections::{HashMap, HashSet};

use anyhow::Result;

use crate::core::domain::path::path_is_under;
use crate::core::domain::root::find_containing_root;
use crate::repo::{self, Connection};
use crate::trail::domain::composition::{build_card, BucketCount, CompositionCard};

/// Compute the composition card for a scope.
///
/// `None` for a global scope (`prefixes` empty — the card is a scoped-view
/// feature only), a scope with no present sources, or a scope with no origin
/// story to tell (`CompositionCard::has_origin_story()` — the one omission
/// rule every future caller shares).
pub fn compute_composition(
    conn: &Connection,
    prefixes: &[String],
) -> Result<Option<CompositionCard>> {
    if prefixes.is_empty() {
        return Ok(None);
    }

    let roots = repo::root::fetch_all(conn)?;
    // The same decomposition the trail's scoped branch uses.
    let mut root_ids: Vec<i64> = prefixes
        .iter()
        .filter_map(|p| find_containing_root(p, &roots).map(|(root_id, ..)| root_id))
        .collect();
    root_ids.sort_unstable();
    root_ids.dedup();
    if root_ids.is_empty() {
        return Ok(None);
    }

    // present=1 is baked into batch_fetch_by_roots; the domain filter below
    // is path-scope only — SQL never compares paths.
    let sources = repo::source::batch_fetch_by_roots(conn, &root_ids)?;

    let mut groups: HashMap<Option<i64>, BucketCount> = HashMap::new();
    for source in &sources {
        let path = source.path();
        if !prefixes.iter().any(|p| path_is_under(&path, p)) {
            continue;
        }
        let entry = groups.entry(source.decision_id).or_default();
        entry.files += 1;
        entry.bytes += source.size;
    }
    if groups.is_empty() {
        return Ok(None);
    }

    let decision_ids: Vec<i64> = groups.keys().filter_map(|id| *id).collect();
    let decisions: HashMap<i64, crate::core::domain::decision::Decision> =
        repo::decision::fetch_by_ids(conn, &decision_ids)?
            .into_iter()
            .map(|d| (d.id, d))
            .collect();

    let mut extractions_by_decision: HashMap<
        i64,
        Vec<crate::core::domain::extraction::DecisionExtraction>,
    > = HashMap::new();
    for row in repo::decision::fetch_extractions_by_decisions(conn, &decision_ids)? {
        extractions_by_decision
            .entry(row.decision_id)
            .or_default()
            .push(row);
    }

    // Keyed by path, not id: a root removed and re-added carries a new id but
    // the same location, and a location that exists today is visitable today.
    let live_root_paths: HashSet<String> = roots.iter().map(|r| r.path.clone()).collect();

    let card = build_card(
        &groups,
        &decisions,
        &extractions_by_decision,
        &live_root_paths,
        prefixes,
    );
    if !card.has_origin_story() {
        return Ok(None);
    }
    Ok(Some(card))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::db::open_in_memory_for_test;
    use crate::repo::insert_test_root;
    use crate::repo::source::insert_test_source;
    use crate::trail::domain::composition::OriginLine;

    fn insert_decision_at(conn: &Connection, command: &str, created_at: i64) -> i64 {
        conn.execute(
            "INSERT INTO decisions (command, command_line, status, canon_version, created_at)
             VALUES (?1, ?2, 'completed', 'test', ?3)",
            rusqlite::params![command, format!("canon {command}"), created_at],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn stamp(conn: &Connection, source_id: i64, decision_id: i64, present: bool) {
        conn.execute(
            "UPDATE sources SET decision_id = ?, present = ? WHERE id = ?",
            rusqlite::params![decision_id, present as i64, source_id],
        )
        .unwrap();
    }

    #[test]
    fn end_to_end_card_over_a_scoped_tree() {
        let conn = open_in_memory_for_test();
        let source_root = insert_test_root(&conn, "/a", "source", false);
        let archive_root = insert_test_root(&conn, "/archive", "archive", false);
        let apply = insert_decision_at(&conn, "apply", 100);
        repo::decision::replace_extractions(
            &conn,
            &[crate::core::domain::extraction::DecisionExtraction {
                decision_id: apply,
                root_id: source_root,
                root_path: "/a".to_string(),
                rel_prefix: "".to_string(),
                files: 2,
                bytes: Some(20),
                destination_root_id: Some(archive_root),
                destination_path: "/archive".to_string(),
                disposition: None,
            }],
        )
        .unwrap();

        let s1 = insert_test_source(&conn, archive_root, "x.jpg", 1, 1, 10, 0);
        let s2 = insert_test_source(&conn, archive_root, "y.jpg", 1, 2, 10, 0);
        stamp(&conn, s1, apply, true);
        stamp(&conn, s2, apply, true);

        let card = compute_composition(&conn, &["/archive".to_string()])
            .unwrap()
            .unwrap();
        assert_eq!(card.files, 2);
        assert_eq!(card.bytes, 20);
        assert_eq!(card.origins.len(), 1);
        match &card.origins[0] {
            OriginLine::FromRoot {
                root_path, files, ..
            } => {
                assert_eq!(root_path, "/a");
                assert_eq!(*files, 2);
            }
            OriginLine::MultiOrigin { .. } => panic!("expected FromRoot"),
        }
    }

    #[test]
    fn present_only_deleted_source_absent_standing_less_than_arrived() {
        let conn = open_in_memory_for_test();
        let source_root = insert_test_root(&conn, "/a", "source", false);
        let archive_root = insert_test_root(&conn, "/archive", "archive", false);
        let apply = insert_decision_at(&conn, "apply", 100);
        repo::decision::replace_extractions(
            &conn,
            &[crate::core::domain::extraction::DecisionExtraction {
                decision_id: apply,
                root_id: source_root,
                root_path: "/a".to_string(),
                rel_prefix: "".to_string(),
                files: 2,
                bytes: Some(20),
                destination_root_id: Some(archive_root),
                destination_path: "/archive".to_string(),
                disposition: None,
            }],
        )
        .unwrap();

        let s1 = insert_test_source(&conn, archive_root, "x.jpg", 1, 1, 10, 0);
        let s2 = insert_test_source(&conn, archive_root, "y.jpg", 1, 2, 10, 0);
        stamp(&conn, s1, apply, true);
        stamp(&conn, s2, apply, false); // later deleted from the archive

        let card = compute_composition(&conn, &["/archive".to_string()])
            .unwrap()
            .unwrap();
        // Standing (1 file) is less than what the decision drew in (2) —
        // honest divergence, not a bug.
        assert_eq!(card.files, 1);
        assert_eq!(card.origins[0].files(), 1);
    }

    #[test]
    fn excluded_but_present_still_counted() {
        let conn = open_in_memory_for_test();
        let archive_root = insert_test_root(&conn, "/archive", "archive", false);
        let scan = insert_decision_at(&conn, "scan", 100);
        let s1 = insert_test_source(&conn, archive_root, "x.jpg", 1, 1, 10, 0);
        stamp(&conn, s1, scan, true);
        conn.execute("UPDATE sources SET excluded = 1 WHERE id = ?", [s1])
            .unwrap();

        let card = compute_composition(&conn, &["/archive".to_string()]).unwrap();
        // Excluded is a no-story scope here (indexed_here only), but the
        // source itself must still be present in the sum — confirmed via a
        // non-omitted scope below with an exclusion decision instead.
        assert!(card.is_none()); // indexed-here-only omits, as expected

        let exclude = insert_decision_at(&conn, "exclude_set", 200);
        stamp(&conn, s1, exclude, true);
        let card = compute_composition(&conn, &["/archive".to_string()])
            .unwrap()
            .unwrap();
        assert_eq!(card.files, 1); // exclusion does not remove standing
        assert_eq!(card.transitioned[0].label, "excluded");
    }

    #[test]
    fn scope_boundary_similar_prefix_excluded() {
        let conn = open_in_memory_for_test();
        let root = insert_test_root(&conn, "/a/bc", "archive", false);
        let scan = insert_decision_at(&conn, "scan", 100);
        let s1 = insert_test_source(&conn, root, "x.jpg", 1, 1, 10, 0);
        stamp(&conn, s1, scan, true);
        let exclude = insert_decision_at(&conn, "exclude_set", 200);
        stamp(&conn, s1, exclude, true);

        // /a/bc is not under /a/b.
        let card = compute_composition(&conn, &["/a/b".to_string()]).unwrap();
        assert!(card.is_none());

        let card = compute_composition(&conn, &["/a/bc".to_string()])
            .unwrap()
            .unwrap();
        assert_eq!(card.files, 1);
    }

    #[test]
    fn apply_stamped_survivor_renamed_by_scan_keeps_attribution() {
        let conn = open_in_memory_for_test();
        let source_root = insert_test_root(&conn, "/a", "source", false);
        let archive_root = insert_test_root(&conn, "/archive", "archive", false);
        let apply = insert_decision_at(&conn, "apply", 100);
        repo::decision::replace_extractions(
            &conn,
            &[crate::core::domain::extraction::DecisionExtraction {
                decision_id: apply,
                root_id: source_root,
                root_path: "/a".to_string(),
                rel_prefix: "".to_string(),
                files: 1,
                bytes: Some(10),
                destination_root_id: Some(archive_root),
                destination_path: "/archive".to_string(),
                disposition: None,
            }],
        )
        .unwrap();
        // Landed at "x.jpg" under apply's stamp, then renamed by a later
        // scan (Moved preserves decision_id — the set/preserve rule).
        let s1 = insert_test_source(&conn, archive_root, "renamed.jpg", 1, 1, 10, 0);
        stamp(&conn, s1, apply, true);

        let card = compute_composition(&conn, &["/archive".to_string()])
            .unwrap()
            .unwrap();
        assert_eq!(card.origins.len(), 1);
        match &card.origins[0] {
            OriginLine::FromRoot { root_path, .. } => assert_eq!(root_path, "/a"),
            OriginLine::MultiOrigin { .. } => panic!("expected FromRoot"),
        }
    }

    #[test]
    fn intra_archive_curation_pass_reads_as_rearranged_at_the_archive_root() {
        // End to end: an apply drawing from /archive/2016 into /archive/2020.
        // Standing at the archive root, the card must not answer "from
        // /archive" — this place is not its own origin.
        let conn = open_in_memory_for_test();
        let archive_root = insert_test_root(&conn, "/archive", "archive", false);
        let apply = insert_decision_at(&conn, "apply", 100);
        repo::decision::replace_extractions(
            &conn,
            &[crate::core::domain::extraction::DecisionExtraction {
                decision_id: apply,
                root_id: archive_root,
                root_path: "/archive".to_string(),
                rel_prefix: "2016".to_string(),
                files: 1,
                bytes: Some(10),
                destination_root_id: Some(archive_root),
                destination_path: "/archive/2020".to_string(),
                disposition: None,
            }],
        )
        .unwrap();
        let s1 = insert_test_source(&conn, archive_root, "2020/x.jpg", 1, 1, 10, 0);
        stamp(&conn, s1, apply, true);

        let card = compute_composition(&conn, &["/archive".to_string()])
            .unwrap()
            .unwrap();
        assert!(card.origins.is_empty());
        assert_eq!(card.transitioned.len(), 1);
        assert_eq!(card.transitioned[0].label, "rearranged");
        assert_eq!(card.files, 1);

        // The narrower view sees the same content arrive from outside it.
        let narrower = compute_composition(&conn, &["/archive/2020".to_string()])
            .unwrap()
            .unwrap();
        assert!(narrower.transitioned.is_empty());
        assert_eq!(narrower.origins.len(), 1);
    }

    #[test]
    fn empty_scope_is_none() {
        let conn = open_in_memory_for_test();
        assert!(compute_composition(&conn, &[]).unwrap().is_none());
    }

    #[test]
    fn no_story_scope_is_none() {
        let conn = open_in_memory_for_test();
        let root = insert_test_root(&conn, "/a", "source", false);
        let scan = insert_decision_at(&conn, "scan", 100);
        let s1 = insert_test_source(&conn, root, "x.jpg", 1, 1, 10, 0);
        stamp(&conn, s1, scan, true);

        assert!(compute_composition(&conn, &["/a".to_string()])
            .unwrap()
            .is_none());
    }

    #[test]
    fn removed_origin_root_sets_root_removed_from_live_roots_check() {
        let conn = open_in_memory_for_test();
        let archive_root = insert_test_root(&conn, "/archive", "archive", false);
        let apply = insert_decision_at(&conn, "apply", 100);
        // The source root that drew this content is never registered here —
        // as if it was removed after the apply completed.
        repo::decision::replace_extractions(
            &conn,
            &[crate::core::domain::extraction::DecisionExtraction {
                decision_id: apply,
                root_id: 999,
                root_path: "/Volumes/gone".to_string(),
                rel_prefix: "".to_string(),
                files: 1,
                bytes: Some(10),
                destination_root_id: Some(archive_root),
                destination_path: "/archive".to_string(),
                disposition: None,
            }],
        )
        .unwrap();
        let s1 = insert_test_source(&conn, archive_root, "x.jpg", 1, 1, 10, 0);
        stamp(&conn, s1, apply, true);

        let card = compute_composition(&conn, &["/archive".to_string()])
            .unwrap()
            .unwrap();
        match &card.origins[0] {
            OriginLine::FromRoot { root_removed, .. } => assert!(root_removed),
            OriginLine::MultiOrigin { .. } => panic!("expected FromRoot"),
        }
    }
}
