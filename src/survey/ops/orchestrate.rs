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
use crate::expr::filter::Filter;
use crate::notes::SurveyNoteContext;
use crate::survey::ops::compute::{compute_survey, SurveyOutcome, SurveyParams};

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
    /// Note counts per related location (absolute path). Empty unless
    /// `orchestration.want_location_note_counts` was set and the outcome
    /// was `Result`.
    pub location_note_counts: HashMap<String, usize>,
}

/// Fetch inputs, resolve `--other`/`--archive`, and compute a survey.
///
/// Takes raw scope prefixes and filter strings (scope prefixes are already
/// resolved by the caller via `ops::scope::resolve_scope()`; filter strings
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

    let all_roots = crate::repo::root::fetch_all(conn)?;

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
        let resolved = crate::ops::scope::resolve_paths(&orchestration.other_paths, &all_roots)?;
        crate::ops::scope::validate_sources_exist(conn, &resolved, &all_roots)?;
        resolved
    } else {
        Vec::new()
    };

    for other_path in &other_resolved {
        if scope_prefixes.contains(other_path) {
            bail!("Error: --other location is identical to the surveyed scope. Comparing a location to itself is not meaningful.");
        }
    }

    let archive_root_id = if let Some(ref spec) = orchestration.archive {
        Some(crate::ops::scope::parse_root_spec(
            &all_roots,
            spec,
            Some("archive"),
        )?)
    } else {
        None
    };
    let archive_label = archive_root_id.map(|id| {
        let root = all_roots.iter().find(|r| r.id == id).unwrap();
        format!("in {}", root.path)
    });

    let root_ids: Vec<i64> = all_roots.iter().map(|r| r.id).collect();
    let all_sources = crate::repo::source::batch_fetch_by_roots(conn, &root_ids)?;

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
        location_note_counts,
    })
}
