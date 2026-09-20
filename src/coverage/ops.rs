//! Coverage statistics computation.
//!
//! Computes archive coverage statistics from a set of sources.
//! Used by the `coverage` command.

use anyhow::Result;

use crate::core::domain::root::Root;
use crate::core::domain::source::Source;
use crate::core::domain::IncludeSet;
use crate::core::repo::{self, Connection};
use crate::expr::{select_sources, RolePolicy, SelectionParams};
use crate::expr::{Filter, UsedStatus};

use crate::core::domain::resolution::ParkedEvidence;
use crate::core::domain::root::ParkedRoot;
use crate::core::domain::scope::ScopeMatch;
use crate::core::ops::scope::{parse_root_spec, RootLookup};

/// Statistics for a set of sources.
pub struct CoverageStats {
    pub root_id: Option<i64>,
    pub root_path: Option<String>,
    pub root_role: Option<String>,
    pub total_sources: i64,
    pub excluded_sources: i64,
    pub hashed_sources: i64,
    pub archived_sources: i64,
    /// Hashed, non-excluded, empty — contentless: coverage can say nothing
    /// about them (the contentless law), so they leave both the archivable
    /// denominator and the unarchived remainder.
    pub contentless_sources: i64,
    /// What the archived count rests on, where it rests solely on copies
    /// behind doors the user closed — and **zero whenever an `--archive`
    /// frame is in force**, that frame being unable to support an
    /// exclusivity claim (see `compute_stats`). A reader must not take 0
    /// here as "nothing rests behind a door" without knowing which frame
    /// produced it.
    ///
    /// A qualifier on the archived count, never a deduction from it: parking
    /// never un-archives, and these sources are covered. Coverage is the view
    /// a user reads before deciding a drive can go, so what the number rests
    /// on is worth knowing — a wide view may nudge about content hidden
    /// behind a door: marked, never leading.
    pub archived_only_parked: ParkedEvidence,
    /// The fleet the roots are named against — kept beside the evidence so
    /// the display layer resolves nothing. Public like every other field on
    /// this result type (`CoverageStats` itself already escapes the
    /// subsystem as `compute_per_root`'s return type, so a narrower
    /// `pub(super)` here would not change what can actually reach it): the
    /// interface composes the printed suffix itself
    /// (`ParkedEvidence::bulleted_clause`), a one-consumer, screen-only
    /// composition that belongs there rather than behind an ops method —
    /// that claim is about where the *composition* lives, not about this
    /// field's own visibility.
    pub archive_roots: Vec<Root>,
}

impl CoverageStats {
    pub fn new() -> Self {
        CoverageStats {
            root_id: None,
            root_path: None,
            root_role: None,
            total_sources: 0,
            excluded_sources: 0,
            hashed_sources: 0,
            archived_sources: 0,
            contentless_sources: 0,
            archived_only_parked: ParkedEvidence::default(),
            archive_roots: Vec::new(),
        }
    }

    pub fn included_sources(&self) -> i64 {
        self.total_sources - self.excluded_sources
    }

    /// Hashed sources coverage can speak about at all — the archivable
    /// denominator (empty files can never be archived-by-identity, so
    /// counting them would cap the percentage below 100 forever).
    pub fn coverable_sources(&self) -> i64 {
        self.hashed_sources - self.contentless_sources
    }

    pub fn hashed_pct(&self) -> f64 {
        let included = self.included_sources();
        if included == 0 {
            0.0
        } else {
            (self.hashed_sources as f64 / included as f64) * 100.0
        }
    }

    pub fn archived_pct(&self) -> f64 {
        let coverable = self.coverable_sources();
        if coverable == 0 {
            0.0
        } else {
            (self.archived_sources as f64 / coverable as f64) * 100.0
        }
    }

    /// The honest remainder: what is hashed, coverable, and not archived.
    /// Contentless sources are neither archived nor waiting to be — they
    /// no longer hide inside this number.
    pub fn unarchived(&self) -> i64 {
        self.coverable_sources() - self.archived_sources
    }
}

/// Resolve an `--archive` spec to a root id, refusing a root that is not an
/// archive.
///
/// The fetch belongs here rather than at the command: which root a spec names
/// is a question about the world, and the interface has no business asking the
/// database it. Survey resolves its own the same way.
/// The archive a run was told to measure against, and how its door stands.
#[derive(Debug)]
pub struct ArchiveTarget {
    pub root_id: i64,
    /// `Some` when the user has closed the door on it — the citation carries
    /// the standing, and the view marks it.
    pub parked: Option<ParkedRoot>,
}

/// Resolve an `--archive` spec to the archive it names, refusing a root that
/// is not an archive.
///
/// A **parked** archive is answered, not refused. Its copies still ground
/// coverage claims — testifying is what a closed door permits — and the
/// projection behind this view never filtered them out; what was missing was
/// a way to say so, which the standing beside the id now provides. Until it
/// did, an answer here would have been a zero where content stands, and the
/// door was named instead.
pub fn resolve_archive_root(
    conn: &Connection,
    spec: Option<&str>,
) -> Result<Option<ArchiveTarget>> {
    match spec {
        Some(spec) => {
            let roots = repo::root::fetch_all(conn)?;
            match parse_root_spec(&roots, spec, Some("archive"))? {
                RootLookup::Found(root_id) => Ok(Some(ArchiveTarget {
                    root_id,
                    parked: None,
                })),
                RootLookup::Parked(parked) => Ok(Some(ArchiveTarget {
                    root_id: parked.root_id,
                    parked: Some(parked),
                })),
            }
        }
        None => Ok(None),
    }
}

/// Compute coverage statistics from source references.
/// Uses `is_excluded()` which checks BOTH source-level and object-level exclusion.
pub fn compute_stats(
    conn: &mut Connection,
    sources: &[&Source],
    archive_root_id: Option<i64>,
    roots: &[Root],
) -> Result<CoverageStats> {
    let mut stats = CoverageStats::new();

    stats.total_sources = sources.len() as i64;

    // Excluded sources — uses is_excluded() which checks BOTH source and object level
    stats.excluded_sources = sources.iter().filter(|s| s.is_excluded()).count() as i64;

    // Hashed sources (have object_id AND not excluded)
    let hashed_sources: Vec<&&Source> = sources
        .iter()
        .filter(|s| s.object_id.is_some() && !s.is_excluded())
        .collect();
    stats.hashed_sources = hashed_sources.len() as i64;
    stats.contentless_sources = hashed_sources.iter().filter(|s| s.is_contentless()).count() as i64;

    // Archived sources — use batch archive detection
    if stats.hashed_sources > 0 {
        let object_ids: Vec<i64> = hashed_sources.iter().filter_map(|s| s.object_id).collect();
        let copies = repo::object::batch_archive_copies(conn, &object_ids, archive_root_id)?;

        // Count sources whose object has an archive copy (not unique objects
        // — multiple sources can have the same object), and, of those, the
        // ones whose every copy stands behind a closed door.
        //
        // **Only when the frame is the whole archive world.** Under
        // `--archive`, the copies fetched are that one archive's, so if its
        // door is closed then *every* copy in hand is parked and the test is
        // true by construction — it would say "N only on suspended X" of
        // content that also stands in a live archive the frame cannot see.
        // An exclusivity claim needs to have looked everywhere, and this
        // frame has not. The header already marks the archive as suspended,
        // so nothing about the door goes unsaid.
        let frame_can_answer_exclusivity = archive_root_id.is_none();
        for source in &hashed_sources {
            let Some(object_copies) = source.object_id.and_then(|oid| copies.get(&oid)) else {
                continue;
            };
            stats.archived_sources += 1;
            if frame_can_answer_exclusivity {
                stats.archived_only_parked.observe(object_copies);
            }
        }
        stats.archived_only_parked.settle();
        stats.archive_roots = roots.to_vec();
    }

    Ok(stats)
}

/// Compute coverage for a scoped selection.
pub fn compute_scoped(
    conn: &mut Connection,
    scopes: &[ScopeMatch],
    filters: &[Filter],
    archive_root_id: Option<i64>,
    include: &IncludeSet,
) -> Result<(CoverageStats, UsedStatus, usize)> {
    let params = SelectionParams {
        scopes: scopes.to_vec(),
        include: include.clone(),
        filters: filters.to_vec(),
        role_policy: RolePolicy::SourceUnlessIncluded,
    };
    let sel = select_sources(conn, &params)?;
    let used_status = sel.used_status.clone();
    let excluded_count = sel.excluded_count;
    let refs: Vec<&Source> = sel.sources.iter().collect();
    let all_roots = repo::root::fetch_all(conn)?;
    let stats = compute_stats(conn, &refs, archive_root_id, &all_roots)?;
    Ok((stats, used_status, excluded_count))
}

/// Compute per-root breakdown plus overall totals.
pub fn compute_per_root(
    conn: &mut Connection,
    filters: &[Filter],
    archive_root_id: Option<i64>,
    include: &IncludeSet,
) -> Result<(Vec<CoverageStats>, CoverageStats, UsedStatus, usize)> {
    let all_roots = repo::root::fetch_all(conn)?;
    // Routed through the fleet partition rather than a bare `!is_suspended()`
    // filter: behaviour is unchanged — the ratified wide-view silence still
    // applies — but the parked half is now typed `ParkedRoot` data rather
    // than discarded outright, so a future statement has it on hand instead
    // of re-deriving it.
    let (open, _parked) = crate::core::domain::root::open_roots(&all_roots);
    let roots: Vec<&Root> = open
        .into_iter()
        .filter(|r| include.includes_archived() || r.is_source())
        .collect();

    let params = SelectionParams {
        scopes: vec![],
        include: include.clone(),
        filters: filters.to_vec(),
        role_policy: RolePolicy::SourceUnlessIncluded,
    };
    let all_sel = select_sources(conn, &params)?;
    let all_sources = all_sel.sources;

    let mut per_root_stats = Vec::new();
    let mut overall = CoverageStats::new();

    for root in &roots {
        let root_sources: Vec<&Source> = all_sources
            .iter()
            .filter(|s| s.root_id == root.id)
            .collect();

        let mut stats = compute_stats(conn, &root_sources, archive_root_id, &all_roots)?;
        stats.root_id = Some(root.id);
        stats.root_path = Some(root.path.clone());
        stats.root_role = Some(root.role.clone());

        overall.total_sources += stats.total_sources;
        overall.excluded_sources += stats.excluded_sources;
        overall.hashed_sources += stats.hashed_sources;
        overall.archived_sources += stats.archived_sources;
        overall.contentless_sources += stats.contentless_sources;
        overall.archived_only_parked.count += stats.archived_only_parked.count;
        overall
            .archived_only_parked
            .roots
            .extend(stats.archived_only_parked.roots.iter().copied());

        per_root_stats.push(stats);
    }

    overall.archived_only_parked.settle();
    overall.archive_roots = all_roots.clone();

    Ok((
        per_root_stats,
        overall,
        all_sel.used_status,
        all_sel.excluded_count,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::testing::{insert_object, insert_root, insert_source, setup_test_db};

    /// Test that archived counts sources, not unique objects.
    /// Guards against the Object Infrastructure bug pattern where
    /// archived_set.len() was used instead of counting sources.
    #[test]
    fn coverage_archived_counts_sources_not_objects() {
        let mut conn = setup_test_db();

        let source_root = insert_root(&conn, "/photos", "source", false);
        let archive_root = insert_root(&conn, "/archive", "archive", false);

        // One object archived, shared by 3 sources
        let archived_obj = insert_object(&conn, "abc123archived", false);
        insert_source(&conn, source_root, "photo1.jpg", Some(archived_obj));
        insert_source(&conn, source_root, "photo2.jpg", Some(archived_obj));
        insert_source(&conn, source_root, "photo3.jpg", Some(archived_obj));

        // One unarchived object
        let unarchived_obj = insert_object(&conn, "def456unarchived", false);
        insert_source(&conn, source_root, "photo4.jpg", Some(unarchived_obj));

        // Put archived object in archive
        insert_source(&conn, archive_root, "backup.jpg", Some(archived_obj));

        let sources = repo::source::batch_fetch_by_roots(&conn, &[source_root]).unwrap();
        let refs: Vec<&Source> = sources.iter().collect();
        let roots = repo::root::fetch_all(&conn).unwrap();
        let stats = compute_stats(&mut conn, &refs, None, &roots).unwrap();

        assert_eq!(
            stats.archived_sources, 3,
            "Should count 3 sources, not 1 object"
        );
        assert_eq!(stats.total_sources, 4);
        assert_eq!(stats.hashed_sources, 4);
    }

    #[test]
    fn contentless_leaves_the_denominator_and_the_remainder() {
        // The contentless law at the coverage surface: an empty file is
        // neither archived nor waiting to be — a fully-covered root with
        // empties reads 100%, and the unarchived remainder never hides
        // them.
        use crate::core::testing::insert_source_with_size;
        let mut conn = setup_test_db();
        let source_root = insert_root(&conn, "/photos", "source", false);
        let archive_root = insert_root(&conn, "/archive", "archive", false);

        let data_obj = insert_object(&conn, "datahash", false);
        let empty_obj = insert_object(&conn, "emptyhash", false);
        insert_source_with_size(&conn, source_root, "photo.jpg", Some(data_obj), 100);
        insert_source_with_size(&conn, source_root, "empty.log", Some(empty_obj), 0);
        insert_source_with_size(&conn, archive_root, "photo.jpg", Some(data_obj), 100);

        let sources = repo::source::batch_fetch_by_roots(&conn, &[source_root]).unwrap();
        let refs: Vec<&Source> = sources.iter().collect();
        let roots = repo::root::fetch_all(&conn).unwrap();
        let stats = compute_stats(&mut conn, &refs, None, &roots).unwrap();

        assert_eq!(stats.hashed_sources, 2);
        assert_eq!(stats.contentless_sources, 1);
        assert_eq!(stats.coverable_sources(), 1);
        assert_eq!(stats.archived_sources, 1);
        assert_eq!(stats.unarchived(), 0, "the empty file is not 'unarchived'");
        assert_eq!(stats.archived_pct(), 100.0, "covered means covered");
    }

    #[test]
    fn archive_spec_resolves_an_archive_root_and_refuses_a_source_root() {
        let conn = setup_test_db();
        let source = insert_root(&conn, "/photos", "source", false);
        let archive = insert_root(&conn, "/archive", "archive", false);

        assert!(resolve_archive_root(&conn, None).unwrap().is_none());
        let target = resolve_archive_root(&conn, Some(&format!("id:{archive}")))
            .unwrap()
            .unwrap();
        assert_eq!(target.root_id, archive);
        assert!(target.parked.is_none());

        let err = resolve_archive_root(&conn, Some(&format!("id:{source}"))).unwrap_err();
        assert!(err.to_string().contains("expected 'archive'"), "{err}");
    }

    #[test]
    fn overall_rollup_obeys_the_contentless_law() {
        // The rollup must carry every field the law touches, or the totals
        // line contradicts the per-root lines above it: a universe whose
        // content is fully covered reads 100% per root and less than that
        // overall. Two roots, so an assignment in place of an accumulation
        // fails here too.
        use crate::core::testing::insert_source_with_size;
        let mut conn = setup_test_db();
        let r1 = insert_root(&conn, "/r1", "source", false);
        let r2 = insert_root(&conn, "/r2", "source", false);
        let archive = insert_root(&conn, "/archive", "archive", false);

        let empty_obj = insert_object(&conn, "emptyhash", false);
        let data_a = insert_object(&conn, "datahash_a", false);
        let data_b = insert_object(&conn, "datahash_b", false);

        insert_source_with_size(&conn, r1, "data.bin", Some(data_a), 100);
        insert_source_with_size(&conn, r1, "empty.log", Some(empty_obj), 0);
        insert_source_with_size(&conn, r2, "data.bin", Some(data_b), 100);
        insert_source_with_size(&conn, r2, "empty.log", Some(empty_obj), 0);
        insert_source_with_size(&conn, archive, "a.bin", Some(data_a), 100);
        insert_source_with_size(&conn, archive, "b.bin", Some(data_b), 100);

        let (per_root, overall, _, _) =
            compute_per_root(&mut conn, &[], None, &IncludeSet::default()).unwrap();

        assert_eq!(per_root.len(), 2, "archive roots stay out of the breakdown");
        for stats in &per_root {
            assert_eq!(stats.contentless_sources, 1);
            assert_eq!(stats.archived_pct(), 100.0);
        }

        assert_eq!(overall.hashed_sources, 4);
        assert_eq!(overall.contentless_sources, 2, "both empties roll up");
        assert_eq!(overall.coverable_sources(), 2);
        assert_eq!(overall.archived_sources, 2);
        assert_eq!(overall.unarchived(), 0);
        assert_eq!(
            overall.archived_pct(),
            100.0,
            "the totals line must agree with the per-root lines"
        );
    }

    #[test]
    fn coverage_stats_empty() {
        let mut conn = setup_test_db();
        let stats = compute_stats(&mut conn, &[], None, &[]).unwrap();
        assert_eq!(stats.total_sources, 0);
        assert_eq!(stats.hashed_sources, 0);
        assert_eq!(stats.archived_sources, 0);
    }

    #[test]
    fn coverage_stats_excludes_excluded() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos", "source", false);
        let obj = insert_object(&conn, "hash1", false);

        insert_source(&conn, root, "included.jpg", Some(obj));

        use crate::core::testing::insert_source_excluded;
        let excl_obj = insert_object(&conn, "hash2", false);
        insert_source_excluded(&conn, root, "excluded.jpg", Some(excl_obj));

        let sources = repo::source::batch_fetch_by_roots(&conn, &[root]).unwrap();
        let refs: Vec<&Source> = sources.iter().collect();
        let roots = repo::root::fetch_all(&conn).unwrap();
        let stats = compute_stats(&mut conn, &refs, None, &roots).unwrap();

        assert_eq!(stats.total_sources, 2);
        assert_eq!(stats.excluded_sources, 1);
        assert_eq!(stats.hashed_sources, 1); // excluded sources don't count as hashed
    }

    // ========================================================================
    // Testimony: what the archived count rests on
    // ========================================================================

    #[test]
    fn coverage_counts_only_parked_beside_archived() {
        let mut conn = setup_test_db();
        let src_root = insert_root(&conn, "/photos", "source", false);
        let live = insert_root(&conn, "/arc-live", "archive", false);
        let parked = insert_root(&conn, "/arc-parked", "archive", true);

        // One object covered from both archives, one only from the parked
        // one, one covered nowhere.
        let both = insert_object(&conn, "both", false);
        let only_parked = insert_object(&conn, "onlyparked", false);
        let uncovered = insert_object(&conn, "uncovered", false);
        insert_source(&conn, src_root, "a.jpg", Some(both));
        insert_source(&conn, src_root, "b.jpg", Some(only_parked));
        insert_source(&conn, src_root, "c.jpg", Some(uncovered));
        insert_source(&conn, live, "a.jpg", Some(both));
        insert_source(&conn, parked, "a.jpg", Some(both));
        insert_source(&conn, parked, "b.jpg", Some(only_parked));

        let all = crate::core::repo::source::batch_fetch_by_roots(&conn, &[src_root]).unwrap();
        let refs: Vec<&Source> = all.iter().collect();
        let roots = repo::root::fetch_all(&conn).unwrap();
        let stats = compute_stats(&mut conn, &refs, None, &roots).unwrap();

        assert_eq!(
            stats.archived_sources, 2,
            "a parked copy is still coverage — the count does not shrink"
        );
        assert_eq!(stats.archived_only_parked.count, 1);
        assert_eq!(
            stats
                .archived_only_parked
                .segment(&stats.archive_roots)
                .unwrap(),
            "1 only on suspended /arc-parked"
        );
    }

    #[test]
    fn a_wholly_open_archive_says_nothing_about_doors() {
        // The segment is suppressed at zero: "0 only on suspended roots" is
        // a sentence about a situation that is not happening.
        let mut conn = setup_test_db();
        let src_root = insert_root(&conn, "/photos", "source", false);
        let live = insert_root(&conn, "/arc-live", "archive", false);
        let obj = insert_object(&conn, "abc", false);
        insert_source(&conn, src_root, "a.jpg", Some(obj));
        insert_source(&conn, live, "a.jpg", Some(obj));

        let all = crate::core::repo::source::batch_fetch_by_roots(&conn, &[src_root]).unwrap();
        let refs: Vec<&Source> = all.iter().collect();
        let roots = repo::root::fetch_all(&conn).unwrap();
        let stats = compute_stats(&mut conn, &refs, None, &roots).unwrap();

        assert_eq!(stats.archived_sources, 1);
        assert_eq!(stats.archived_only_parked.count, 0);
        assert!(stats
            .archived_only_parked
            .segment(&stats.archive_roots)
            .is_none());
    }

    #[test]
    fn a_parked_archive_spec_is_answered_and_marked_not_refused() {
        // The interim refusal is retired: the projection never filtered
        // these copies out, and the standing beside the id is what lets the
        // view say so instead of putting a zero where content stands.
        let mut conn = setup_test_db();
        let src_root = insert_root(&conn, "/photos", "source", false);
        let parked = insert_root(&conn, "/arc-parked", "archive", true);
        let obj = insert_object(&conn, "abc", false);
        insert_source(&conn, src_root, "a.jpg", Some(obj));
        insert_source(&conn, parked, "a.jpg", Some(obj));

        let target = resolve_archive_root(&conn, Some(&format!("id:{parked}")))
            .unwrap()
            .expect("a parked archive resolves");
        assert_eq!(target.root_id, parked);
        assert_eq!(
            target.parked.as_ref().map(|p| p.root_path.as_str()),
            Some("/arc-parked")
        );

        let all = crate::core::repo::source::batch_fetch_by_roots(&conn, &[src_root]).unwrap();
        let refs: Vec<&Source> = all.iter().collect();
        let roots = repo::root::fetch_all(&conn).unwrap();
        let stats = compute_stats(&mut conn, &refs, Some(target.root_id), &roots).unwrap();
        assert_eq!(stats.archived_sources, 1, "the parked archive testifies");
        assert_eq!(
            stats.archived_only_parked.count, 0,
            "and the frame makes no exclusivity claim it cannot support"
        );
    }

    #[test]
    fn a_single_archive_frame_makes_no_exclusivity_claim() {
        // Under `--archive`, only that archive's copies are in hand, so
        // "every copy is parked" is true by construction whenever its door
        // is closed — and false of the world, whenever the content also
        // stands in a live archive the frame cannot see.
        let mut conn = setup_test_db();
        let src_root = insert_root(&conn, "/photos", "source", false);
        let live = insert_root(&conn, "/arc-live", "archive", false);
        let parked = insert_root(&conn, "/arc-parked", "archive", true);
        let obj = insert_object(&conn, "abc", false);
        insert_source(&conn, src_root, "a.jpg", Some(obj));
        insert_source(&conn, live, "a.jpg", Some(obj));
        insert_source(&conn, parked, "a.jpg", Some(obj));

        let all = crate::core::repo::source::batch_fetch_by_roots(&conn, &[src_root]).unwrap();
        let refs: Vec<&Source> = all.iter().collect();
        let roots = repo::root::fetch_all(&conn).unwrap();

        let framed = compute_stats(&mut conn, &refs, Some(parked), &roots).unwrap();
        assert_eq!(framed.archived_sources, 1);
        assert_eq!(
            framed.archived_only_parked.count, 0,
            "the frame has not looked everywhere, so it claims nothing"
        );
        assert!(framed
            .archived_only_parked
            .segment(&framed.archive_roots)
            .is_none());

        // Unframed, the same world answers — truthfully, with a live copy.
        let whole = compute_stats(&mut conn, &refs, None, &roots).unwrap();
        assert_eq!(whole.archived_only_parked.count, 0);
    }
}
