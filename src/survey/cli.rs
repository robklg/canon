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

/// The scope the header should name when part of what was asked lay on the
/// archive side: the source side alone, since naming the rest would claim a
/// view survey did not take.
///
/// Everything else on the resolved scope travels across untouched — above
/// all its **set-asides**. Narrowing *which places were surveyed* must never
/// drop *what was skipped*: the two are separate answers to separate
/// questions, and a scope that happened to name an archive place would
/// otherwise swallow the sourceless one beside it. Returns `None` when there
/// is nothing to narrow, so the caller keeps the resolved scope as it stands.
fn narrowed_header_scope(
    resolved: &ResolvedScope,
    source_side_prefixes: Vec<String>,
    has_archive_side: bool,
) -> Option<ResolvedScope> {
    if !has_archive_side {
        return None;
    }
    Some(ResolvedScope {
        prefixes: source_side_prefixes,
        set_aside: resolved.set_aside.clone(),
        from_cwd: resolved.from_cwd,
        auto_include_archived: resolved.auto_include_archived,
    })
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

/// How a survey invocation ended, for the interface to turn into an exit
/// code. `FrameRefused` is not an error: the question was well-formed, and
/// survey answered it by saying it is the wrong instrument for this place —
/// so it carries a non-zero exit without an `Error:` prefix, the shape
/// `compare` uses for a non-identical result.
#[must_use]
#[derive(Debug, PartialEq, Eq)]
pub enum SurveyExit {
    Reported,
    FrameRefused,
}

pub fn run(
    db: &mut repo::Db,
    scope_prefixes: &[String],
    filter_strs: &[String],
    options: &SurveyOptions,
) -> Result<SurveyExit> {
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
        archive_side_set_aside,
        source_side_prefixes,
        location_note_counts,
    } = run_survey(conn, scope_prefixes, filter_strs, &orchestration, &params)?;

    let scope = narrowed_header_scope(
        &options.scope,
        source_side_prefixes,
        !archive_side_set_aside.is_empty(),
    );
    let header = render::HeaderScope {
        scope: scope.as_ref().unwrap_or(&options.scope),
        archive_set_aside: &archive_side_set_aside,
    };

    match outcome {
        SurveyOutcome::ArchiveScope(statement) => {
            // A refusal is a human statement, and stdout belongs to the
            // machine stream whenever one was asked for: the exit code
            // carries the refusal there, and the words go to stderr rather
            // than arriving as one large bogus argument in an `xargs -0`.
            render::print_archive_scope_statement(
                &header,
                &statement.roots,
                suppress_early_exit_header(options.null_delim, options.detail),
            );
            return Ok(SurveyExit::FrameRefused);
        }
        SurveyOutcome::Empty => {
            let suppress = suppress_early_exit_header(options.null_delim, options.detail);
            if !suppress {
                render::print_survey_header(&header, &options.original_filters, 0, 0, 0, 0, None);
                if let Some((ref ctx, ref scope_rel)) = note_context {
                    render::print_notes_section(ctx, scope_rel, options.verbose);
                }
            }
        }
        SurveyOutcome::AllUnhashed { total_count } => {
            let suppress = suppress_early_exit_header(options.null_delim, options.detail);
            if !suppress {
                render::print_survey_header(
                    &header,
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
                            &header,
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
                        &header,
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
                            &header,
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
                            &header,
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
                    // The one detail view with no header of its own: its
                    // stdout is a bare path stream, so the set-asides are
                    // said on stderr rather than not at all.
                    render::eprint_set_asides(&header);
                    let cwd = if options.null_delim {
                        None
                    } else {
                        display_cwd.as_deref()
                    };
                    render::print_unique_detail(&result.unique_paths, options.null_delim, cwd);
                }
                None => {
                    render::print_survey_header(
                        &header,
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

    Ok(SurveyExit::Reported)
}

// =============================================================================
// Tests — validation logic only (computation tests are in survey/ops/tests/)
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::repo::open_in_memory_for_test;
    use crate::core::testing::{insert_object, insert_root, insert_source};

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
                set_aside: Vec::new(),
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

    /// Narrowing the header to the surveyed side must not lose the boundary's
    /// own set-asides — a mixed scope names both, or it under-reports what it
    /// was asked to do.
    #[test]
    fn narrowing_the_header_scope_keeps_the_boundary_set_asides() {
        let resolved = ResolvedScope {
            prefixes: vec!["/photos/2011".to_string(), "/archive/media".to_string()],
            set_aside: vec!["/photos/empty".to_string()],
            from_cwd: false,
            auto_include_archived: true,
        };

        let narrowed =
            narrowed_header_scope(&resolved, vec!["/photos/2011".to_string()], true).unwrap();
        assert_eq!(narrowed.prefixes, vec!["/photos/2011".to_string()]);
        assert_eq!(narrowed.set_aside, vec!["/photos/empty".to_string()]);
        assert!(narrowed.auto_include_archived);

        // Nothing on the archive side: nothing to narrow, and the caller
        // keeps the resolved scope untouched.
        assert!(
            narrowed_header_scope(&resolved, vec!["/photos/2011".to_string()], false).is_none()
        );
    }

    /// The frame refusal reaches the interface as a value, not an error —
    /// so `main.rs` can give it a non-zero exit with no `Error:` prefix.
    #[test]
    fn cli_run_returns_frame_refused_for_an_archive_scope() {
        let conn = open_in_memory_for_test();
        let archive = insert_root(&conn, "/archive", "archive", false);
        let obj = insert_object(&conn, "hash_001", false);
        insert_source(&conn, archive, "media/a.jpg", Some(obj));

        let options = SurveyOptions {
            scope: ResolvedScope {
                prefixes: vec!["/archive/media".to_string()],
                set_aside: Vec::new(),
                from_cwd: false,
                auto_include_archived: true,
            },
            ..test_options()
        };
        let mut db = repo::Db::from_connection(conn);
        let exit = run(&mut db, &["/archive/media".to_string()], &[], &options).unwrap();
        assert_eq!(exit, SurveyExit::FrameRefused);
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
