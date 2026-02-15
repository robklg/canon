/// What additional sources to include in the working set.
/// Default (empty): active, non-excluded sources from source roots.
#[derive(Debug, Clone, Default)]
pub struct IncludeSet {
    pub excluded: bool,
    pub archived: bool,
}

impl IncludeSet {
    pub fn includes_excluded(&self) -> bool {
        self.excluded
    }
    pub fn includes_archived(&self) -> bool {
        self.archived
    }
    /// True when any non-default visibility is active.
    pub fn is_expanded(&self) -> bool {
        self.excluded || self.archived
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_include_set_default_not_expanded() {
        let set = IncludeSet::default();
        assert!(!set.is_expanded());
        assert!(!set.includes_excluded());
        assert!(!set.includes_archived());
    }

    #[test]
    fn test_include_set_excluded_is_expanded() {
        let set = IncludeSet {
            excluded: true,
            archived: false,
        };
        assert!(set.is_expanded());
        assert!(set.includes_excluded());
        assert!(!set.includes_archived());
    }

    #[test]
    fn test_include_set_archived_is_expanded() {
        let set = IncludeSet {
            excluded: false,
            archived: true,
        };
        assert!(set.is_expanded());
        assert!(!set.includes_excluded());
        assert!(set.includes_archived());
    }
}
