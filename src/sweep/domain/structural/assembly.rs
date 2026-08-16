//! Stage E — the orchestrator and finding assembly. Runs the full pipeline
//! (build universe → LCA-subtract weights → discover subjects → localize
//! subjects → assemble findings), plus the floor-released second walk that
//! counts what the emit floors keep off the board.

use crate::core::domain::root::Root;
use crate::core::domain::source::Source;

use super::discovery::{discover_subjects, FindingTier};
use super::localization::{localize_subjects, LocalizedSubject, Location, RelationShape};
use super::universe::{build_universe, SweepParams, SweepStats, Universe};
use super::weights::compute_matched;

/// The finding's natural next move, derived from structural facts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FindingNature {
    /// The counterpart lives on a suspended root: nothing reads as safe
    /// until that root is reconnected and re-verified.
    Verify,
    /// The subject's content is essentially archive-covered.
    Dismiss,
    /// Copies exist, but the archive doesn't hold them yet.
    Consolidate,
}

/// One reduction opportunity, as lens-free data. Ranking and presentation
/// derive from these fields; nothing here presumes the triage lens.
#[derive(Debug, PartialEq)]
pub struct StructuralFinding {
    pub subject: Location,
    pub subject_suspended: bool,
    /// The subject stands on an archive root — under the triage lens its
    /// content is already resolved; surfaces honestly marked.
    pub subject_is_archive: bool,
    pub subject_last_scanned_at: Option<i64>,
    pub tier: FindingTier,
    /// True when this finding sits below the emit floors — discovered by the
    /// floor-released walk, shown only when the caller asks for everything.
    pub below_floors: bool,
    pub shape: RelationShape,
    pub context: Vec<super::localization::ContextRelation>,
    /// Fraction of the subject's comparison-participating weight existing
    /// outside the subject (the union containment).
    pub containment_size_pct: f64,
    pub containment_count_pct: f64,
    /// Resolution gain: what acting on this finding resolves.
    pub gain_bytes: u64,
    pub gain_files: u32,
    /// Content existing nowhere else (over comparison-participating weight).
    pub residual_bytes: u64,
    pub residual_files: u32,
    /// Fraction of the subject's comparison-participating weight with an
    /// outside copy on an archive root.
    pub archive_cover_pct: f64,
    /// Comparison-participating weight over all content in the subject —
    /// the honesty qualifier ("compared on N% by size").
    pub hash_coverage_pct: f64,
    pub nature: FindingNature,
}

/// The structural computation's result: every finding, plus the honesty
/// stats for the header.
#[derive(Debug, PartialEq)]
pub struct StructuralSweep {
    pub findings: Vec<StructuralFinding>,
    pub stats: SweepStats,
}

/// Compute every reduction-opportunity finding in the universe.
///
/// `sources` is the policy-filtered slice (presence, exclusion, zero-byte —
/// the caller's rules); `roots` must contain every root the sources belong
/// to. Output order is deterministic: subject root path, then subject path.
pub fn compute_structural(
    sources: &[&Source],
    roots: &[Root],
    params: &SweepParams,
) -> StructuralSweep {
    let mut universe = build_universe(sources, roots, params);
    let weights = compute_matched(&universe);
    let subjects = discover_subjects(&universe, &weights, params);
    // The floors gate discovery (they lift below-floor fragments into an
    // aggregated parent subject), but they must trim output, never
    // existence: a second discovery with the floors released finds the
    // subjects the floors kept off the board. An extra counts only where it
    // is disjoint from every floored subject on its root — anything at,
    // under, or over a floored subject is already that finding's territory.
    let released = SweepParams {
        emit_floor_bytes: 1,
        emit_floor_files: 1,
        ..params.clone()
    };
    let mut extras = discover_subjects(&universe, &weights, &released);
    extras.retain(|e| {
        let tree = &universe.roots_data[e.root_idx].tree;
        !subjects.iter().any(|s| {
            s.root_idx == e.root_idx
                && (tree.is_ancestor_or_equal(s.fid, e.fid)
                    || tree.is_ancestor_or_equal(e.fid, s.fid))
        })
    });
    for e in &mut extras {
        e.below_floors = true;
    }
    universe.stats.below_floor_subjects = extras.len();
    // Reciprocal-mirror dedup runs within each set, so a below-floors
    // finding never displaces an above-floors one. A reciprocal mirror pair
    // spanning the floor boundary can therefore state itself from both ends
    // when everything is assembled — honest, and constructible only in a
    // thin band near the floors.
    let mut localized = localize_subjects(&universe, roots, subjects, params);
    if params.assemble_below_floors {
        localized.extend(localize_subjects(&universe, roots, extras, params));
    }
    let mut findings: Vec<StructuralFinding> = localized
        .into_iter()
        .map(|ls| assemble_finding(&universe, roots, ls, params))
        .collect();
    findings.sort_by(|a, b| {
        (&a.subject.root_path, &a.subject.rel_prefix)
            .cmp(&(&b.subject.root_path, &b.subject.rel_prefix))
    });
    StructuralSweep {
        findings,
        stats: universe.stats,
    }
}

fn assemble_finding(
    universe: &Universe,
    roots: &[Root],
    ls: LocalizedSubject,
    params: &SweepParams,
) -> StructuralFinding {
    let raw = &ls.raw;
    let rd = &universe.roots_data[raw.root_idx];
    let root = &roots[raw.root_idx];
    let (all_bytes, _) = rd.sub_all[raw.fid as usize];
    let archive_cover_pct = if raw.total_bytes > 0 {
        ls.archive_matched_bytes as f64 / raw.total_bytes as f64
    } else {
        0.0
    };
    let counterpart_suspended = matches!(
        ls.shape,
        RelationShape::Pair {
            counterpart_suspended: true,
            ..
        }
    );
    // Verify outranks Dismiss: an archive-covered claim whose counterpart
    // sits on a disconnected drive is not actionable until re-verified.
    let nature = if counterpart_suspended {
        FindingNature::Verify
    } else if archive_cover_pct >= params.lifting_tolerance {
        FindingNature::Dismiss
    } else {
        FindingNature::Consolidate
    };
    StructuralFinding {
        subject: Location {
            root_id: root.id,
            root_path: root.path.clone(),
            rel_prefix: rd.tree.path(raw.fid).to_string(),
        },
        subject_suspended: root.suspended,
        subject_is_archive: root.role == "archive",
        subject_last_scanned_at: root.last_scanned_at,
        tier: raw.tier,
        below_floors: raw.below_floors,
        containment_size_pct: if raw.total_bytes > 0 {
            raw.matched_bytes as f64 / raw.total_bytes as f64
        } else {
            0.0
        },
        containment_count_pct: if raw.total_files > 0 {
            f64::from(raw.matched_files) / f64::from(raw.total_files)
        } else {
            0.0
        },
        gain_bytes: raw.matched_bytes,
        gain_files: raw.matched_files,
        residual_bytes: raw.total_bytes - raw.matched_bytes,
        residual_files: raw.total_files - raw.matched_files,
        archive_cover_pct,
        hash_coverage_pct: if all_bytes > 0 {
            raw.total_bytes as f64 / all_bytes as f64
        } else {
            1.0
        },
        nature,
        shape: ls.shape,
        context: ls.context,
    }
}
