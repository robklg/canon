//! The fate vocabulary — the *what* a decision records, and its two
//! derivations, shared substrate between the trail rollup and receipt
//! writing.
//!
//! No I/O. Neither the trail nor the receipt writer owns this vocabulary;
//! each independently derives from it, which is why it lives here rather
//! than inside either.

/// Rollup family of a decision command — the minimal *what* classification the
/// time lens needs. Deliberately small: the full operation taxonomy belongs to
/// the self-describing-receipts slice and its /vision vocabulary work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecisionFamily {
    /// Content given its canonical home (apply).
    Archive,
    /// Sources dismissed from consideration.
    Exclude,
    /// Exclusions lifted — a change of mind.
    Restore,
    /// Canon observing the world (scan): deletions surface here via the
    /// absent side of the decision stamp, never as Canon's own act.
    Observe,
    /// Known command with no fate contribution (intent, knowledge, fleet,
    /// housekeeping).
    Other,
    /// Command identifier from a newer Canon — rendered raw, never dropped.
    Unrecognized,
}

/// Total mapping from decision command identifiers to rollup families.
/// Identifiers are append-only and never reused (see `DecisionCommand`), so
/// each arm here is permanent history: never move an identifier between
/// families.
pub fn decision_family(command: &str) -> DecisionFamily {
    match command {
        "apply" => DecisionFamily::Archive,
        "exclude_set" | "exclude_duplicates" | "exclude_set_object" => DecisionFamily::Exclude,
        "exclude_clear" | "exclude_clear_object" => DecisionFamily::Restore,
        "scan" => DecisionFamily::Observe,
        "cluster_generate" | "cluster_refresh" | "roots_rm" | "roots_retire" | "roots_suspend"
        | "roots_unsuspend" | "import_facts" | "prune" | "facts_delete" | "note_clear" => {
            DecisionFamily::Other
        }
        _ => DecisionFamily::Unrecognized,
    }
}

/// The *what* a decision records — a content transition in registered
/// vocabulary. The trail rollup renders the terminal fates; receipts stamp the
/// terminal subset plus `restored`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transition {
    Archived,
    Excluded,
    Restored,
    Deleted,
}

impl Transition {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Archived => "archived",
            Self::Excluded => "excluded",
            Self::Restored => "restored",
            Self::Deleted => "deleted",
        }
    }
}

/// The presence axis of a decision's stamp: whether the stamped sources are
/// present (indexed / archived) or absent (deleted / tombstoned). The
/// discriminant command identity alone cannot supply — one scan stamps both.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FateAspect {
    Present,
    Absent,
}

/// The single what-derivation, keyed on `(family, aspect)` rather than command
/// so the trail (which has the presence axis) and receipts (which supply the
/// aspect by receipt kind) share one source of truth. Wider than the command:
/// `scan` (Observe) yields `Deleted` only on its `Absent` bucket; its `Present`
/// bucket is indexing, which no receipt stamps (`None`).
pub fn fate_transition(family: DecisionFamily, aspect: FateAspect) -> Option<Transition> {
    use DecisionFamily::*;
    use FateAspect::*;
    match (family, aspect) {
        (Archive, Present) => Some(Transition::Archived),
        (Exclude, _) => Some(Transition::Excluded),
        (Restore, _) => Some(Transition::Restored),
        (Observe, Absent) => Some(Transition::Deleted),
        _ => None,
    }
}

/// The posture of a transition: Canon performing a change vs. observing one the
/// world made (registered vocabulary, orthogonal to the transition word).
///
/// The rollup renders only the transition word; posture is the derivation's
/// other half, stamped by the receipt writer wherever a receipt records a
/// transition. It lives here beside `fate_transition` so the what-vocabulary
/// has one home.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Posture {
    Performed,
    Observed,
}

impl Posture {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Performed => "performed",
            Self::Observed => "observed",
        }
    }
}

/// The posture accompanying a transition. `Observed` exactly when Canon
/// witnessed the change rather than performing it — a scan-observed deletion;
/// every other receipt-stamped transition is `Performed`.
pub fn fate_posture(family: DecisionFamily, aspect: FateAspect) -> Posture {
    match (family, aspect) {
        (DecisionFamily::Observe, FateAspect::Absent) => Posture::Observed,
        _ => Posture::Performed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::decision::DecisionCommand;

    // decision_family — total over every registered identifier

    #[test]
    fn decision_family_covers_all_registered_commands() {
        let expected = [
            (DecisionCommand::Scan, DecisionFamily::Observe),
            (DecisionCommand::Apply, DecisionFamily::Archive),
            (DecisionCommand::ExcludeSet, DecisionFamily::Exclude),
            (DecisionCommand::ExcludeClear, DecisionFamily::Restore),
            (DecisionCommand::ExcludeDuplicates, DecisionFamily::Exclude),
            (DecisionCommand::ExcludeSetObject, DecisionFamily::Exclude),
            (DecisionCommand::ExcludeClearObject, DecisionFamily::Restore),
            (DecisionCommand::ClusterGenerate, DecisionFamily::Other),
            (DecisionCommand::ClusterRefresh, DecisionFamily::Other),
            (DecisionCommand::RootsRm, DecisionFamily::Other),
            (DecisionCommand::RootsRetire, DecisionFamily::Other),
            (DecisionCommand::RootsSuspend, DecisionFamily::Other),
            (DecisionCommand::RootsUnsuspend, DecisionFamily::Other),
            (DecisionCommand::ImportFacts, DecisionFamily::Other),
            (DecisionCommand::Prune, DecisionFamily::Other),
            (DecisionCommand::FactsDelete, DecisionFamily::Other),
            (DecisionCommand::NoteClear, DecisionFamily::Other),
        ];
        for (cmd, family) in expected {
            assert_eq!(decision_family(cmd.as_str()), family, "{}", cmd.as_str());
        }
    }

    #[test]
    fn decision_family_unknown_is_unrecognized() {
        assert_eq!(
            decision_family("reduce_sweep"),
            DecisionFamily::Unrecognized
        );
        assert_eq!(decision_family(""), DecisionFamily::Unrecognized);
    }

    // fate_transition / fate_posture — the shared what-derivation

    #[test]
    fn fate_transition_covers_family_aspect_matrix() {
        use DecisionFamily::*;
        use FateAspect::*;
        use Transition::*;
        let expected = [
            ((Archive, Present), Some(Archived)),
            ((Archive, Absent), None), // apply never stamps absent
            ((Exclude, Present), Some(Excluded)),
            ((Exclude, Absent), Some(Excluded)), // object-exclusion tombstones
            ((Restore, Present), Some(Restored)),
            ((Restore, Absent), Some(Restored)),
            ((Observe, Present), None),         // indexing — no fate
            ((Observe, Absent), Some(Deleted)), // scan-observed deletion
            ((Other, Present), None),
            ((Other, Absent), None),
            ((Unrecognized, Present), None),
            ((Unrecognized, Absent), None),
        ];
        for ((family, aspect), want) in expected {
            assert_eq!(
                fate_transition(family, aspect),
                want,
                "{family:?} / {aspect:?}"
            );
        }
    }

    #[test]
    fn transition_as_str_registered_words() {
        assert_eq!(Transition::Archived.as_str(), "archived");
        assert_eq!(Transition::Excluded.as_str(), "excluded");
        assert_eq!(Transition::Restored.as_str(), "restored");
        assert_eq!(Transition::Deleted.as_str(), "deleted");
    }

    #[test]
    fn fate_posture_observed_only_for_scan_deletion() {
        use DecisionFamily::*;
        use FateAspect::*;
        // The one observed transition: a scan witnessing a loss.
        assert_eq!(fate_posture(Observe, Absent), Posture::Observed);
        // Everything else Canon performs.
        for (family, aspect) in [
            (Observe, Present),
            (Archive, Present),
            (Exclude, Present),
            (Exclude, Absent),
            (Restore, Present),
            (Other, Absent),
            (Unrecognized, Present),
        ] {
            assert_eq!(
                fate_posture(family, aspect),
                Posture::Performed,
                "{family:?} / {aspect:?}"
            );
        }
    }

    #[test]
    fn posture_as_str_registered_words() {
        assert_eq!(Posture::Performed.as_str(), "performed");
        assert_eq!(Posture::Observed.as_str(), "observed");
    }
}
