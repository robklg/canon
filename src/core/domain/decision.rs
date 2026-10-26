/// Stable command identifiers for decision records.
///
/// These strings are written to the decisions table and become permanent history.
/// Values are append-only: never rename, never reuse for different semantics.
/// When removing a command from Canon, keep its variant here so historical
/// records remain interpretable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::EnumIter)]
pub enum DecisionCommand {
    Scan,
    Apply,
    ExcludeSet,
    ExcludeClear,
    ExcludeDuplicates,
    ExcludeSetObject,
    ExcludeClearObject,
    ClusterGenerate,
    ClusterRefresh,
    RootsRm,
    RootsRetire,
    RootsSuspend,
    RootsUnsuspend,
    ImportFacts,
    Prune,
    FactsDelete,
    NoteClear,
    FactsConvert,
}

/// What a command puts in a decision row's receipt columns: a receipt it wrote, the pointer to
/// an artifact that is no receipt (retirement's book), or never anything — so a reader of empty
/// columns can say the command writes none rather than that one went missing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiptColumns {
    Receipt,
    ArtifactPointer,
    Never,
}

impl DecisionCommand {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Scan => "scan",
            Self::Apply => "apply",
            Self::ExcludeSet => "exclude_set",
            Self::ExcludeClear => "exclude_clear",
            Self::ExcludeDuplicates => "exclude_duplicates",
            Self::ExcludeSetObject => "exclude_set_object",
            Self::ExcludeClearObject => "exclude_clear_object",
            Self::ClusterGenerate => "cluster_generate",
            Self::ClusterRefresh => "cluster_refresh",
            Self::RootsRm => "roots_rm",
            Self::RootsRetire => "roots_retire",
            Self::RootsSuspend => "roots_suspend",
            Self::RootsUnsuspend => "roots_unsuspend",
            Self::ImportFacts => "import_facts",
            Self::Prune => "prune",
            Self::FactsDelete => "facts_delete",
            Self::NoteClear => "note_clear",
            Self::FactsConvert => "facts_convert",
        }
    }

    /// The command a decision row records, read back from its stored text — the inverse of
    /// [`DecisionCommand::as_str`]; `None` for text no command of this version writes.
    pub fn from_recorded(text: &str) -> Option<Self> {
        use strum::IntoEnumIterator;
        Self::iter().find(|command| command.as_str() == text)
    }

    /// What this command puts in a decision row's receipt columns.
    pub fn receipt_columns(self) -> ReceiptColumns {
        match self {
            Self::Scan
            | Self::Apply
            | Self::ExcludeSet
            | Self::ExcludeClear
            | Self::ExcludeDuplicates
            | Self::ExcludeSetObject
            | Self::ExcludeClearObject => ReceiptColumns::Receipt,
            Self::RootsRetire => ReceiptColumns::ArtifactPointer,
            Self::ClusterGenerate
            | Self::ClusterRefresh
            | Self::RootsRm
            | Self::RootsSuspend
            | Self::RootsUnsuspend
            | Self::ImportFacts
            | Self::Prune
            | Self::FactsDelete
            | Self::NoteClear
            | Self::FactsConvert => ReceiptColumns::Never,
        }
    }
}

/// Status of a decision record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecisionStatus {
    /// The initial row state; written as a raw SQL literal by
    /// `core::repo::decision::insert_started` rather than through this variant,
    /// so it's never constructed outside tests. Kept for round-trip
    /// completeness against the DB's status values.
    #[allow(dead_code)]
    Started,
    Completed,
    Partial,
    Interrupted,
    /// The run declined to do anything: a pre-flight check said no and it
    /// bailed before touching content. Distinct from `Interrupted`, which
    /// means a run was cut short after starting, and from `Started`, which
    /// means the run's outcome was never written down.
    Refused,
}

impl DecisionStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Completed => "completed",
            Self::Partial => "partial",
            Self::Interrupted => "interrupted",
            Self::Refused => "refused",
        }
    }
}

/// The latest decision that acted on a fact key: which one, by which command (its stored
/// text), and how many facts the key held after it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyDecision {
    pub id: i64,
    pub command: String,
    /// `None` where the row records no count — never read as zero.
    pub remaining: Option<u64>,
}

/// A decision record — what happened, when, why.
/// Uses String for command/status (not enums) because DB reads may return
/// values from future Canon versions with unknown command identifiers.
#[allow(dead_code)]
pub struct Decision {
    pub id: i64,
    pub command: String,
    pub scope: Option<Vec<String>>,
    pub command_line: String,
    pub reason: Option<String>,
    pub status: String,
    pub count_attempted: Option<i64>,
    pub count_completed: Option<i64>,
    pub count_failed: Option<i64>,
    pub count_skipped: Option<i64>,
    pub summary: Option<String>,
    pub canon_version: String,
    pub created_at: i64,
    pub receipt_root_id: Option<i64>,
    pub receipt_rel_path: Option<String>,
    /// The clock the decision read times on, as its receipt states it; `None` for a decision
    /// recorded before Canon kept the zone.
    pub zone: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn decision_command_as_str_all_variants() {
        assert_eq!(DecisionCommand::Scan.as_str(), "scan");
        assert_eq!(DecisionCommand::Apply.as_str(), "apply");
        assert_eq!(DecisionCommand::ExcludeSet.as_str(), "exclude_set");
        assert_eq!(DecisionCommand::ExcludeClear.as_str(), "exclude_clear");
        assert_eq!(
            DecisionCommand::ExcludeDuplicates.as_str(),
            "exclude_duplicates"
        );
        assert_eq!(
            DecisionCommand::ExcludeSetObject.as_str(),
            "exclude_set_object"
        );
        assert_eq!(
            DecisionCommand::ExcludeClearObject.as_str(),
            "exclude_clear_object"
        );
        assert_eq!(
            DecisionCommand::ClusterGenerate.as_str(),
            "cluster_generate"
        );
        assert_eq!(DecisionCommand::ClusterRefresh.as_str(), "cluster_refresh");
        assert_eq!(DecisionCommand::RootsRm.as_str(), "roots_rm");
        assert_eq!(DecisionCommand::RootsRetire.as_str(), "roots_retire");
        assert_eq!(DecisionCommand::RootsSuspend.as_str(), "roots_suspend");
        assert_eq!(DecisionCommand::RootsUnsuspend.as_str(), "roots_unsuspend");
        assert_eq!(DecisionCommand::ImportFacts.as_str(), "import_facts");
        assert_eq!(DecisionCommand::Prune.as_str(), "prune");
        assert_eq!(DecisionCommand::FactsDelete.as_str(), "facts_delete");
        assert_eq!(DecisionCommand::NoteClear.as_str(), "note_clear");
        assert_eq!(DecisionCommand::FactsConvert.as_str(), "facts_convert");
    }

    #[test]
    fn from_recorded_is_the_inverse_of_as_str() {
        use strum::IntoEnumIterator;
        for command in DecisionCommand::iter() {
            assert_eq!(
                DecisionCommand::from_recorded(command.as_str()),
                Some(command)
            );
        }
        assert_eq!(DecisionCommand::from_recorded("not_a_command"), None);
    }

    #[test]
    fn decision_command_strings_are_unique() {
        let all = [
            DecisionCommand::Scan,
            DecisionCommand::Apply,
            DecisionCommand::ExcludeSet,
            DecisionCommand::ExcludeClear,
            DecisionCommand::ExcludeDuplicates,
            DecisionCommand::ExcludeSetObject,
            DecisionCommand::ExcludeClearObject,
            DecisionCommand::ClusterGenerate,
            DecisionCommand::ClusterRefresh,
            DecisionCommand::RootsRm,
            DecisionCommand::RootsRetire,
            DecisionCommand::RootsSuspend,
            DecisionCommand::RootsUnsuspend,
            DecisionCommand::ImportFacts,
            DecisionCommand::Prune,
            DecisionCommand::FactsDelete,
            DecisionCommand::NoteClear,
            DecisionCommand::FactsConvert,
        ];
        let strings: HashSet<&str> = all.iter().map(|c| c.as_str()).collect();
        assert_eq!(strings.len(), all.len());
    }

    #[test]
    fn decision_status_as_str() {
        assert_eq!(DecisionStatus::Started.as_str(), "started");
        assert_eq!(DecisionStatus::Completed.as_str(), "completed");
        assert_eq!(DecisionStatus::Partial.as_str(), "partial");
        assert_eq!(DecisionStatus::Interrupted.as_str(), "interrupted");
        assert_eq!(DecisionStatus::Refused.as_str(), "refused");
    }
}
