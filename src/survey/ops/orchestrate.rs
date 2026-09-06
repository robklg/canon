// =============================================================================
// Orchestration — the fetch, resolution, and note-context wiring around
// compute_survey(). Ops owns the fetch: the interface passes through
// pre-resolved scope prefixes and raw filter/orchestration inputs, and
// gets back a fully computed run.
// =============================================================================

use anyhow::{bail, Result};
use rusqlite::Connection;
use std::collections::HashMap;
use std::path::PathBuf;

use crate::core::domain;
use crate::expr::Filter;
use crate::notes::SurveyNoteContext;
use crate::survey::ops::compute::{
    compute_survey, group_by_archive_root, ArchiveScopeStatement, SurveyOutcome, SurveyParams,
};

/// Raw, unresolved orchestration inputs — everything `run_survey` needs
/// beyond `SurveyParams` (which stays `compute_survey`'s pure-computation,
/// pre-resolved input).
pub struct SurveyOrchestration {
    /// Compare against specific locations instead of discovering them.
    pub other_paths: Vec<PathBuf>,
    /// `--archive` root spec (must resolve to an archive-role root).
    pub archive: Option<String>,
    /// Whether to compute per-location note counts — only the default
    /// detail view displays them; other views skip the query entirely.
    pub want_location_note_counts: bool,
}

/// Outcome of `run_survey`: the computed survey plus orchestration-provided
/// extras that don't belong on `compute_survey`'s pure-computation output.
///
/// Note context travels outside `SurveyOutcome` because it applies to all
/// three variants (`Empty`, `AllUnhashed`, `Result`) — the interface prints
/// it regardless of which one comes back. Location note counts stay off
/// `SurveyResult` too — they're default-view-only display data, not
/// something `compute_survey` (or any other caller of it, like the
/// contentless-law canary) needs to carry.
pub struct SurveyRun {
    pub outcome: SurveyOutcome,
    pub note_context: Option<(SurveyNoteContext, String)>,
    /// Asked-for prefixes that fell inside archive roots and were set aside,
    /// grouped so each archive root is named once. Populated only on a mixed
    /// scope — an all-archive scope answers with
    /// [`SurveyOutcome::ArchiveScope`] instead.
    pub archive_side_set_aside: Vec<(String, Vec<String>)>,
    /// The prefixes actually surveyed — the asked-for scope minus anything
    /// on the archive side. Empty when the scope was global.
    pub source_side_prefixes: Vec<String>,
    /// Note counts per related location (absolute path). Empty unless
    /// `orchestration.want_location_note_counts` was set and the outcome
    /// was `Result`.
    pub location_note_counts: HashMap<String, usize>,
}

/// Fetch inputs, resolve `--other`/`--archive`, and compute a survey.
///
/// Takes raw scope prefixes and filter strings (scope prefixes are already
/// resolved by the caller via `core::ops::scope::resolve_scope()`; filter strings
/// are parsed here). Reproduces the same order of fallible operations the
/// interface used to run, in order: roots → note context → `--other`
/// resolve/validate → `--archive` resolve → sources → compute.
pub fn run_survey(
    conn: &mut Connection,
    scope_prefixes: &[String],
    filter_strs: &[String],
    orchestration: &SurveyOrchestration,
    params: &SurveyParams,
) -> Result<SurveyRun> {
    let filters: Vec<Filter> = filter_strs
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    let all_roots = crate::core::repo::root::fetch_all(conn)?;

    // Survey reads source-side selections, so the roles of the containing
    // roots decide what it can be asked here — before any selection exists,
    // and never from a count. A scope entirely inside archives has no
    // question to answer; a mixed scope narrows and says what it set aside.
    let role_partition = domain::root::partition_prefixes_by_role(scope_prefixes, &all_roots);
    if !scope_prefixes.is_empty() && role_partition.source_side.is_empty() {
        return Ok(SurveyRun {
            outcome: SurveyOutcome::ArchiveScope(ArchiveScopeStatement {
                roots: group_by_archive_root(&role_partition.archive_side),
            }),
            note_context: None,
            archive_side_set_aside: Vec::new(),
            source_side_prefixes: Vec::new(),
            location_note_counts: HashMap::new(),
        });
    }
    let archive_side_set_aside = group_by_archive_root(&role_partition.archive_side);
    let scope_prefixes: &[String] = &role_partition.source_side;

    let note_context = if scope_prefixes.len() == 1 {
        if let Some((root_id, _, _, rel_path)) =
            domain::root::find_containing_root(&scope_prefixes[0], &all_roots)
        {
            Some((
                crate::notes::survey_note_context(conn, root_id, &rel_path)?,
                rel_path,
            ))
        } else {
            None
        }
    } else {
        None
    };

    let other_resolved = if !orchestration.other_paths.is_empty() {
        let resolved =
            crate::core::ops::scope::resolve_paths(&orchestration.other_paths, &all_roots)?;
        // The reference is load-bearing: a parked one set aside would turn
        // "how much of this stands there too" into a different question and
        // answer it with a false zero.
        crate::core::ops::scope::refuse_parked_locations(&resolved, &all_roots)?;
        crate::core::ops::scope::validate_sources_exist(conn, &resolved, &all_roots)?
    } else {
        Vec::new()
    };

    for other_path in &other_resolved {
        if scope_prefixes.contains(other_path) {
            bail!("Error: --other location is identical to the surveyed scope. Comparing a location to itself is not meaningful.");
        }
    }

    let archive_root_id = match orchestration.archive {
        Some(ref spec) => {
            match crate::core::ops::scope::parse_root_spec(&all_roots, spec, Some("archive"))? {
                crate::core::ops::scope::RootLookup::Found(id) => Some(id),
                // The archive this survey was told to measure against is behind a
                // closed door. Its copies still testify — but survey's outward
                // side cannot see them yet, so answering would report zero
                // overlap with content that is standing there. The door is named
                // instead of a false count.
                crate::core::ops::scope::RootLookup::Parked(parked) => {
                    return Err(crate::core::domain::root::DoorRefused::at(
                        &parked,
                        crate::core::domain::root::DoorVerb::Refused,
                    )
                    .into())
                }
            }
        }
        None => None,
    };
    let archive_label = archive_root_id.map(|id| {
        let root = all_roots.iter().find(|r| r.id == id).unwrap();
        format!("in {}", root.path)
    });

    let root_ids: Vec<i64> = all_roots.iter().map(|r| r.id).collect();
    let all_sources = crate::core::repo::source::batch_fetch_by_roots(conn, &root_ids)?;

    let mut outcome = compute_survey(
        conn,
        scope_prefixes,
        &filters,
        params,
        &all_sources,
        &other_resolved,
        archive_root_id,
    )?;

    let mut location_note_counts = HashMap::new();
    if let SurveyOutcome::Result(ref mut result) = outcome {
        result.archive_label = archive_label;

        if orchestration.want_location_note_counts {
            let location_scopes: Vec<(i64, String)> = result
                .location_results
                .iter()
                .filter_map(|loc| {
                    domain::root::find_containing_root(&loc.path, &all_roots)
                        .map(|(root_id, _, _, rel_path)| (root_id, rel_path))
                })
                .collect();
            let counts = crate::notes::batch_count_subtree(conn, &location_scopes)?;
            location_note_counts = location_scopes
                .iter()
                .filter_map(|(root_id, rel_path)| {
                    let key = (*root_id, rel_path.clone());
                    counts.get(&key).map(|count| {
                        let root = all_roots.iter().find(|r| r.id == *root_id).unwrap();
                        let abs_path = if rel_path.is_empty() {
                            root.path.clone()
                        } else {
                            format!("{}/{}", root.path, rel_path)
                        };
                        (abs_path, *count)
                    })
                })
                .collect();
        }
    }

    Ok(SurveyRun {
        outcome,
        note_context,
        archive_side_set_aside,
        source_side_prefixes: scope_prefixes.to_vec(),
        location_note_counts,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::domain::include::IncludeSet;
    use crate::core::testing::{insert_root, insert_source, insert_source_excluded, setup_test_db};

    fn orchestration() -> SurveyOrchestration {
        SurveyOrchestration {
            other_paths: Vec::new(),
            archive: None,
            want_location_note_counts: false,
        }
    }

    fn params() -> SurveyParams {
        SurveyParams {
            include: IncludeSet::default(),
            compute_affinity: false,
            compute_overlap_pairs: false,
            compute_residual: false,
            compute_archived_pairs: false,
        }
    }

    /// The statement names each archive root once, however many prefixes
    /// under it were asked about.
    #[test]
    fn an_archive_scope_returns_the_statement_with_each_root_named_once() {
        let mut conn = setup_test_db();
        let archive_id = insert_root(&conn, "/archive", "archive", false);
        insert_source(&conn, archive_id, "media/x/a.jpg", None);
        insert_source(&conn, archive_id, "media/y/b.jpg", None);

        let run = run_survey(
            &mut conn,
            &[
                "/archive/media/x".to_string(),
                "/archive/media/y".to_string(),
            ],
            &[],
            &orchestration(),
            &params(),
        )
        .unwrap();

        match run.outcome {
            SurveyOutcome::ArchiveScope(statement) => {
                assert_eq!(
                    statement.roots,
                    vec![(
                        "/archive".to_string(),
                        vec![
                            "/archive/media/x".to_string(),
                            "/archive/media/y".to_string()
                        ]
                    )]
                );
            }
            _ => panic!("an all-archive scope must answer with the statement"),
        }
    }

    #[test]
    fn a_mixed_scope_narrows_to_the_source_side_and_carries_the_set_asides() {
        let mut conn = setup_test_db();
        let source_id = insert_root(&conn, "/photos", "source", false);
        let archive_id = insert_root(&conn, "/archive", "archive", false);
        insert_source(&conn, source_id, "2011/a.jpg", None);
        insert_source(&conn, archive_id, "media/b.jpg", None);

        let run = run_survey(
            &mut conn,
            &["/photos/2011".to_string(), "/archive/media".to_string()],
            &[],
            &orchestration(),
            &params(),
        )
        .unwrap();

        assert!(
            !matches!(run.outcome, SurveyOutcome::ArchiveScope(_)),
            "a mixed scope proceeds source-side"
        );
        assert_eq!(
            run.archive_side_set_aside,
            vec![("/archive".to_string(), vec!["/archive/media".to_string()])]
        );
        // And what the header will name: the source side alone, never the
        // whole asked-for scope.
        assert_eq!(run.source_side_prefixes, vec!["/photos/2011".to_string()]);
    }

    /// The invariant, both halves: the statement comes from the roles of the
    /// containing roots, and a source-side scope that genuinely selects
    /// nothing still gets the ordinary empty result.
    #[test]
    fn the_archive_statement_comes_from_roles_never_from_a_zero_count() {
        let mut conn = setup_test_db();
        let source_id = insert_root(&conn, "/photos", "source", false);
        let archive_id = insert_root(&conn, "/archive", "archive", false);
        insert_source_excluded(&conn, source_id, "2011/a.jpg", None);
        // An archive place that is far from empty.
        for n in 0..3 {
            insert_source(&conn, archive_id, &format!("media/{n}.jpg"), None);
        }

        // A full archive place: the statement, decided before any selection.
        let archive_run = run_survey(
            &mut conn,
            &["/archive/media".to_string()],
            &[],
            &orchestration(),
            &params(),
        )
        .unwrap();
        assert!(matches!(
            archive_run.outcome,
            SurveyOutcome::ArchiveScope(_)
        ));

        // An empty source-side selection: the ordinary empty result, not a
        // statement — nothing about survey's frame explains this emptiness.
        let source_run = run_survey(
            &mut conn,
            &["/photos/2011".to_string()],
            &[],
            &orchestration(),
            &params(),
        )
        .unwrap();
        assert!(matches!(source_run.outcome, SurveyOutcome::Empty));
    }

    /// Suspension's own law governs what a closed door permits; this seam
    /// reads role, and role alone — a suspended archive root gets the same
    /// statement any archive root gets.
    #[test]
    fn a_suspended_archive_root_is_read_by_role_like_any_other() {
        let mut conn = setup_test_db();
        let archive_id = insert_root(&conn, "/archive", "archive", true);
        insert_source(&conn, archive_id, "media/a.jpg", None);

        let run = run_survey(
            &mut conn,
            &["/archive/media".to_string()],
            &[],
            &orchestration(),
            &params(),
        )
        .unwrap();
        assert!(matches!(run.outcome, SurveyOutcome::ArchiveScope(_)));
    }

    /// `--other` names the reference location the survey compares against —
    /// load-bearing the same way compare's two sides are. Setting it aside
    /// would silently turn a comparison into an unanchored survey, so the
    /// abort stays: the carve-out from the scope boundary's proceed-and-state
    /// policy.
    #[test]
    fn survey_other_still_aborts_on_a_sourceless_reference() {
        let mut conn = setup_test_db();
        let root_id = insert_root(&conn, "/photos", "source", false);
        insert_source(&conn, root_id, "2011/a.jpg", None);

        let orchestration = SurveyOrchestration {
            other_paths: vec![PathBuf::from("/photos/2012")],
            archive: None,
            want_location_note_counts: false,
        };
        let result = run_survey(
            &mut conn,
            &["/photos/2011".to_string()],
            &[],
            &orchestration,
            &params(),
        );
        let err = match result {
            Err(e) => e.to_string(),
            Ok(_) => panic!("a sourceless --other reference must abort"),
        };
        assert!(err.contains("no sources known at /photos/2012"), "{err}");
    }
}
