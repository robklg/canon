//! Pure predicates about what a trail argument looks like.

/// Whether an argument looks like a decision id rather than a path.
///
/// `canon trail 191` and `canon trail show 191` are one keystroke apart, and
/// the first is a scope question about a folder named `191`. This only ever
/// decides whether to *offer* the other reading — it is asked after Canon has
/// already found no history at the path, so a real folder named `191` has
/// sources or a story and never reaches it.
pub(crate) fn looks_like_decision_id(arg: &str) -> bool {
    !arg.is_empty() && arg.bytes().all(|b| b.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn looks_like_decision_id_accepts_only_all_digit_args() {
        assert!(looks_like_decision_id("191"));
        assert!(looks_like_decision_id("0"));
        assert!(!looks_like_decision_id(""));
        assert!(!looks_like_decision_id("191a"));
        assert!(!looks_like_decision_id("19/1"));
        assert!(!looks_like_decision_id("./191"));
        // Not a digit anywhere but ASCII: an Arabic-Indic numeral is not an
        // id Canon would parse, so it stays a path.
        assert!(!looks_like_decision_id("١٩١"));
    }
}
