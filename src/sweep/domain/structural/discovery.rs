//! Stage C — the descent that emits maximal subjects. Emission at or above
//! the lifting tolerance stops the descent, so a clean parent dominates its
//! children by construction; a mixed folder whose children emit nothing
//! still emits candidate-tier at or above the candidate threshold.

use super::universe::{RootData, SweepParams, Universe};
use super::weights::MatchedWeights;

/// How settled a finding is: at or above the lifting tolerance, or the
/// consolidation-candidate tier below it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FindingTier {
    Clean,
    Candidate,
}

/// A maximal subject emitted by the descent, before localization.
pub struct RawSubject {
    pub root_idx: usize,
    pub fid: u32,
    pub tier: FindingTier,
    /// Discovered only by the floor-released walk (below the emit floors).
    pub below_floors: bool,
    /// Weight of the subject's content that exists outside the subject.
    pub matched_bytes: u64,
    pub matched_files: u32,
    /// The subject's comparison-participating totals.
    pub total_bytes: u64,
    pub total_files: u32,
}

/// Walk each root's tree and emit maximal subjects.
///
/// Emission at or above the lifting tolerance stops the descent, so a clean
/// parent dominates its children by construction; a mixed folder whose
/// children emit nothing still emits candidate-tier at or above the
/// candidate threshold.
pub fn discover_subjects(
    universe: &Universe,
    weights: &MatchedWeights,
    params: &SweepParams,
) -> Vec<RawSubject> {
    let mut subjects: Vec<RawSubject> = Vec::new();
    for rd in &universe.roots_data {
        if rd.tree.is_empty() {
            continue;
        }
        walk(
            rd,
            &weights.matched[rd.root_idx],
            &weights.upper[rd.root_idx],
            params,
            0,
            &mut subjects,
        );
    }
    subjects
}

fn walk(
    rd: &RootData,
    matched: &[(u64, u32)],
    upper: &[(u64, u32)],
    params: &SweepParams,
    fid: u32,
    subjects: &mut Vec<RawSubject>,
) -> bool {
    // Trim on the monotonic upper bound: if even the pre-subtraction weight
    // is below the floors, no descendant can clear them. The true matched
    // weight cannot gate the descent — a parent can hold less than its
    // children after the intra-root subtraction.
    let (u_bytes, u_files) = upper[fid as usize];
    if u_bytes < params.emit_floor_bytes && u_files < params.emit_floor_files {
        return false;
    }
    let (m_bytes, m_files) = matched[fid as usize];
    let floors_met = m_bytes >= params.emit_floor_bytes || m_files >= params.emit_floor_files;
    let (t_bytes, t_files) = rd.sub_hashed[fid as usize];
    let containment = if t_bytes > 0 {
        m_bytes as f64 / t_bytes as f64
    } else {
        0.0
    };
    let emit = |tier: FindingTier, subjects: &mut Vec<RawSubject>| {
        subjects.push(RawSubject {
            root_idx: rd.root_idx,
            fid,
            tier,
            below_floors: false,
            matched_bytes: m_bytes,
            matched_files: m_files,
            total_bytes: t_bytes,
            total_files: t_files,
        });
    };
    if floors_met && containment >= params.lifting_tolerance {
        emit(FindingTier::Clean, subjects);
        return true;
    }
    let mut emitted = false;
    for &child in rd.tree.children(fid) {
        if walk(rd, matched, upper, params, child, subjects) {
            emitted = true;
        }
    }
    if !emitted && floors_met && containment >= params.candidate_threshold {
        emit(FindingTier::Candidate, subjects);
        return true;
    }
    emitted
}
