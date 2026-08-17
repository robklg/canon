//! Survey's interface layer: CLI-shape validation, wiring `ops::orchestrate`
//! into a typed result, and composing the render half's presentation calls.

use std::path::PathBuf;

use anyhow::{bail, Result};

use crate::core::domain::IncludeSet;
use crate::core::ops::scope::ResolvedScope;
use crate::core::repo;
use crate::survey::ops::compute::{SurveyOutcome, SurveyParams};
use crate::survey::ops::orchestrate::{run_survey, SurveyOrchestration, SurveyRun};
use crate::survey::render;

/// Detail output mode for `--detail`.
#[derive(Clone, Copy, PartialEq, clap::ValueEnum)]
pub enum DetailMode {
    /// Show which selection files are archived and where
    Archived,
    /// Show complementary content at related locations
    Complement,
    /// Output bare paths of unique-to-selection content
    Unique,
    /// Show which selection files overlap with each related location
    Overlap,
    /// Show selection files NOT shared with reference location(s)
    Residual,
}

impl DetailMode {
    /// Whether this mode renders a machine stream under `-0` (its detail
    /// printer takes the null flag and emits null-delimited paths). The
    /// match is exhaustive on purpose: a new mode must answer this here,
    /// or the early-exit arms below would write the human header into its
    /// machine stream.
    fn machine_rendering(self) -> bool {
        match self {
            DetailMode::Archived
            | DetailMode::Unique
            | DetailMode::Overlap
            | DetailMode::Residual => true,
            DetailMode::Complement => false,
        }
    }
}

/// Whether an early-exit outcome (empty or all-unhashed selection) must
/// suppress its human header: machine mode requested for a detail mode
/// that renders a machine stream.
fn suppress_early_exit_header(null_delim: bool, detail: Option<DetailMode>) -> bool {
    null_delim && detail.is_some_and(DetailMode::machine_rendering)
}

/// Options controlling survey behavior.
pub struct SurveyOptions {
    /// Original (pre-expansion) filter strings — for display in selection header.
    pub original_filters: Vec<String>,
    /// Visibility control (--include excluded).
    pub include: IncludeSet,
    /// Compare against specific locations instead of discovering them.
    pub other_paths: Vec<PathBuf>,
    /// Opt into affinity enrichment (requires --where).
    pub affinity: bool,
    /// Skip per-location affinity computation.
    pub brief: bool,
    /// Detail output mode (replaces summary).
    pub detail: Option<DetailMode>,
    /// Null-delimited output for --detail unique/overlap.
    pub null_delim: bool,
    /// Filter archive section to a specific archive root.
    pub archive: Option<String>,
    /// Show all paths per location (complement view) / all locations (summary).
    pub verbose: bool,
    /// Resolved scope information for display.
    pub scope: ResolvedScope,
}

pub fn run(
    db: &mut repo::Db,
    scope_prefixes: &[String],
    filter_strs: &[String],
    options: &SurveyOptions,
) -> Result<()> {
    // Validate --detail complement requires --where
    if options.detail == Some(DetailMode::Complement) && filter_strs.is_empty() {
        bail!("`--detail complement` requires `--where` filters to define matching content.");
    }

    // Validate --detail residual requires --other
    if options.detail == Some(DetailMode::Residual) && options.other_paths.is_empty() {
        bail!("`--detail residual` requires `--other` to specify a reference location.");
    }

    // Validate --affinity requires --where
    if options.affinity && filter_strs.is_empty() {
        bail!("`--affinity` requires `--where` filters.");
    }

    let conn = db.conn_mut();

    // Scope is already resolved by the caller via scope::resolve_scope().

    // Build computation params from options
    let compute_affinity =
        (options.affinity || options.detail == Some(DetailMode::Complement)) && !options.brief;
    let params = SurveyParams {
        include: options.include.clone(),
        compute_affinity,
        compute_overlap_pairs: options.detail == Some(DetailMode::Overlap),
        compute_residual: options.detail == Some(DetailMode::Residual),
        compute_archived_pairs: options.detail == Some(DetailMode::Archived),
    };
    let orchestration = SurveyOrchestration {
        other_paths: options.other_paths.clone(),
        archive: options.archive.clone(),
        want_location_note_counts: options.detail.is_none(),
    };

    let SurveyRun {
        outcome,
        note_context,
        location_note_counts,
    } = run_survey(conn, scope_prefixes, filter_strs, &orchestration, &params)?;

    match outcome {
        SurveyOutcome::Empty => {
            let suppress = suppress_early_exit_header(options.null_delim, options.detail);
            if !suppress {
                render::print_survey_header(
                    &options.scope,
                    &options.original_filters,
                    0,
                    0,
                    0,
                    0,
                    None,
                );
                if let Some((ref ctx, ref scope_rel)) = note_context {
                    render::print_notes_section(ctx, scope_rel, options.verbose);
                }
            }
        }
        SurveyOutcome::AllUnhashed { total_count } => {
            let suppress = suppress_early_exit_header(options.null_delim, options.detail);
            if !suppress {
                render::print_survey_header(
                    &options.scope,
                    &options.original_filters,
                    total_count,
                    total_count,
                    0,
                    0,
                    None,
                );
                if let Some((ref ctx, ref scope_rel)) = note_context {
                    render::print_notes_section(ctx, scope_rel, options.verbose);
                }
                println!();
                println!("No hashed sources in selection. Content comparison requires hashing.");
                println!("Run `canon scan` to hash these sources.");
            }
        }
        SurveyOutcome::Result(result) => {
            let display_cwd = std::env::current_dir()
                .ok()
                .and_then(|p| p.to_str().map(|s| s.to_string()));
            match options.detail {
                Some(DetailMode::Archived) => {
                    if !options.null_delim {
                        render::print_survey_header(
                            &options.scope,
                            &options.original_filters,
                            result.total_count,
                            result.unhashed_count,
                            result.total_hashed,
                            result.contentless_count,
                            Some(result.unique_count),
                        );
                        if let Some((ref ctx, ref scope_rel)) = note_context {
                            render::print_notes_section(ctx, scope_rel, options.verbose);
                        }
                        println!();
                    }
                    let cwd = if options.null_delim {
                        None
                    } else {
                        display_cwd.as_deref()
                    };
                    render::print_archived_detail(
                        &result.archived_details,
                        result.archived_source_count,
                        cwd,
                        options.verbose,
                        options.null_delim,
                    );
                }
                Some(DetailMode::Complement) => {
                    render::print_survey_header(
                        &options.scope,
                        &options.original_filters,
                        result.total_count,
                        result.unhashed_count,
                        result.total_hashed,
                        result.contentless_count,
                        Some(result.unique_count),
                    );
                    if let Some((ref ctx, ref scope_rel)) = note_context {
                        render::print_notes_section(ctx, scope_rel, options.verbose);
                    }
                    println!();
                    render::print_complement_detail(
                        &result.location_results,
                        result.total_hashed,
                        result.is_other_mode,
                        options.verbose,
                    );
                }
                Some(DetailMode::Overlap) => {
                    if !options.null_delim {
                        render::print_survey_header(
                            &options.scope,
                            &options.original_filters,
                            result.total_count,
                            result.unhashed_count,
                            result.total_hashed,
                            result.contentless_count,
                            Some(result.unique_count),
                        );
                        if let Some((ref ctx, ref scope_rel)) = note_context {
                            render::print_notes_section(ctx, scope_rel, options.verbose);
                        }
                        println!();
                    }
                    let cwd = if options.null_delim {
                        None
                    } else {
                        display_cwd.as_deref()
                    };
                    render::print_overlap_detail(
                        &result.location_results,
                        result.total_hashed,
                        result.is_other_mode,
                        options.verbose,
                        cwd,
                        options.null_delim,
                    );
                }
                Some(DetailMode::Residual) => {
                    if !options.null_delim {
                        render::print_survey_header(
                            &options.scope,
                            &options.original_filters,
                            result.total_count,
                            result.unhashed_count,
                            result.total_hashed,
                            result.contentless_count,
                            Some(result.unique_count),
                        );
                        if let Some((ref ctx, ref scope_rel)) = note_context {
                            render::print_notes_section(ctx, scope_rel, options.verbose);
                        }
                        println!();
                    }
                    let cwd = if options.null_delim {
                        None
                    } else {
                        display_cwd.as_deref()
                    };
                    render::print_residual_detail(
                        &result.location_results,
                        cwd,
                        options.verbose,
                        options.null_delim,
                    );
                }
                Some(DetailMode::Unique) => {
                    let cwd = if options.null_delim {
                        None
                    } else {
                        display_cwd.as_deref()
                    };
                    render::print_unique_detail(&result.unique_paths, options.null_delim, cwd);
                }
                None => {
                    render::print_survey_header(
                        &options.scope,
                        &options.original_filters,
                        result.total_count,
                        result.unhashed_count,
                        result.total_hashed,
                        result.contentless_count,
                        Some(result.unique_count),
                    );
                    if let Some((ref ctx, ref scope_rel)) = note_context {
                        render::print_notes_section(ctx, scope_rel, options.verbose);
                    }
                    println!();
                    if !result.is_other_mode {
                        render::print_archive_section(
                            result.archived_source_count,
                            result.total_hashed,
                            &result.archive_scopes,
                            result.archive_label.as_deref(),
                            options.verbose,
                        );
                        println!();
                    }

                    render::print_related_locations(
                        &result.location_results,
                        result.total_hashed,
                        result.is_other_mode,
                        options.verbose,
                        &location_note_counts,
                    );
                }
            }

            // Visibility hint for status predicates
            if result.used_status.excluded
                && !options.include.includes_excluded()
                && result.excluded_count > 0
            {
                eprintln!(
                    "({} excluded sources hidden, use --include excluded to show)",
                    result.excluded_count
                );
            }
        }
    }

    Ok(())
}

// =============================================================================
// Tests — validation logic only (computation tests are in survey/ops/tests/)
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::repo::open_in_memory_for_test;
    use crate::ops::test_helpers::{insert_object, insert_root, insert_source};

    fn test_options() -> SurveyOptions {
        SurveyOptions {
            original_filters: Vec::new(),
            include: IncludeSet::default(),
            other_paths: Vec::new(),
            affinity: false,
            brief: false,
            detail: None,
            null_delim: false,
            archive: None,
            verbose: false,
            scope: ResolvedScope {
                prefixes: vec!["/mnt/drive".to_string()],
                from_cwd: false,
                auto_include_archived: false,
            },
        }
    }

    #[test]
    fn machine_mode_suppresses_the_header_for_every_machine_rendering() {
        // The header suppression is derived per mode, not hand-listed:
        // every mode whose detail printer emits a null-delimited stream
        // must stay silent on an empty selection — archived included,
        // the entry the old hand-maintained list was missing.
        for mode in [
            DetailMode::Archived,
            DetailMode::Unique,
            DetailMode::Overlap,
            DetailMode::Residual,
        ] {
            assert!(
                suppress_early_exit_header(true, Some(mode)),
                "machine rendering must suppress the human header"
            );
        }
        // Complement has no -0 rendering; its output is always for humans.
        assert!(!suppress_early_exit_header(
            true,
            Some(DetailMode::Complement)
        ));
        // Without -0 the header always prints.
        assert!(!suppress_early_exit_header(
            false,
            Some(DetailMode::Archived)
        ));
        assert!(!suppress_early_exit_header(true, None));
    }

    #[test]
    fn test_affinity_requires_where() {
        let conn = open_in_memory_for_test();

        let root = insert_root(&conn, "/mnt/drive", "source", false);
        let obj1 = insert_object(&conn, "hash_001", false);
        insert_source(&conn, root, "a.jpg", Some(obj1));

        let options = SurveyOptions {
            affinity: true,
            ..test_options()
        };
        let paths = vec!["/mnt/drive".to_string()];
        let filter_strs: Vec<String> = vec![];

        let mut db = repo::Db::from_connection(conn);
        let result = run(&mut db, &paths, &filter_strs, &options);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("--affinity"));
        assert!(err.contains("--where"));
    }

    #[test]
    fn test_residual_requires_other() {
        let conn = open_in_memory_for_test();

        let root = insert_root(&conn, "/mnt/drive", "source", false);
        let obj1 = insert_object(&conn, "hash_001", false);
        insert_source(&conn, root, "a.jpg", Some(obj1));

        let options = SurveyOptions {
            detail: Some(DetailMode::Residual),
            ..test_options()
        };
        let paths = vec!["/mnt/drive".to_string()];
        let filter_strs: Vec<String> = vec![];

        let mut db = repo::Db::from_connection(conn);
        let result = run(&mut db, &paths, &filter_strs, &options);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("--detail residual"));
        assert!(err.contains("--other"));
    }
}
