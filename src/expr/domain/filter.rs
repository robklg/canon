//! The filter language: its syntax tree and its comparisons.
//!
//! `--where` is the asking half of the expression language — the half that
//! picks content out of the universe. The reader (`read.rs`) reads a typed
//! filter into this tree of boolean and comparison nodes; binding reads each
//! value as its key's type, and evaluation walks the bound tree against
//! sources. Everything here is pure: the tree is built and compared without
//! ever touching a database.

use super::key::InputKey;
use super::problem::Place;
use crate::core::domain::time::value::PeriodOp;

// ============================================================================
// Expression AST
// ============================================================================

/// The comparison operators. `!=`, `!~` and `NOT IN` have no operator of their own: each
/// reads as `NOT` of its positive form, so "everything else" means one thing everywhere.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CompareOp {
    Eq,
    Gt,
    Ge,
    Lt,
    Le,
    Glob, // ~
}

/// Status predicates — computed boolean state, not stored facts.
///
/// Adding a variant is deliberately noisy: the prefetch walks every variant
/// and matches exhaustively, so a new one cannot be evaluated before someone
/// has said what it needs loaded.
#[derive(Debug, Clone, Copy, PartialEq, strum::EnumIter)]
pub enum StatusPredicate {
    /// Content exists in at least one archive root (including suspended).
    Archived,
    /// Content hash has been computed (source has object_id).
    Hashed,
    /// Source or object is excluded.
    Excluded,
    /// Source has any stored fact, excluding content.hash.sha256.
    Enriched,
}

/// Keywords recognized as status predicates before normalization.
pub const STATUS_KEYWORDS: &[(&str, StatusPredicate)] = &[
    ("archived", StatusPredicate::Archived),
    ("hashed", StatusPredicate::Hashed),
    ("excluded", StatusPredicate::Excluded),
    ("enriched", StatusPredicate::Enriched),
];

/// A value as typed: its text (inside the quotes, an inner quote undoubled), whether it was
/// quoted, the value exactly as typed, where it sits, and — for an unquoted value — the first
/// character that keeps it from standing bare. Which sentence that character takes depends on
/// the key's type, so binding writes it.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::expr) struct InputValue {
    pub text: String,
    pub quoted: bool,
    pub input: String,
    pub place: Place,
    pub needs_quotes: Option<NeedsQuotes>,
}

/// Why an unquoted value cannot stand bare, at the byte of the first character that says so. A
/// wildcard (`*`, `?`, `[` after `=`, `!=` or in an `IN` list) outranks any other character.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(in crate::expr) enum NeedsQuotes {
    Character(usize),
    Wildcard(usize),
}

/// A filter as the reader reads it: each condition keeps its own text as typed (`clause`) and
/// where it sits (`place`). Only conditions read without a problem in their key are in it.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::expr) enum Expr {
    And(Vec<Expr>),
    Or(Vec<Expr>),
    Not(Box<Expr>),
    Exists {
        key: InputKey,
        clause: String,
        place: Place,
    },
    Compare {
        key: InputKey,
        op: CompareOp,
        value: InputValue,
        clause: String,
        place: Place,
    },
    In {
        key: InputKey,
        values: Vec<InputValue>,
        clause: String,
        place: Place,
    },
    Status {
        predicate: StatusPredicate,
        place: Place,
    },
}

/// Result of applying filters, including metadata about which status predicates were used.
pub struct FilterResult {
    pub source_ids: Vec<i64>,
    pub used_status: UsedStatus,
    /// Whether any comparison read an instant on the clock
    /// (`domain::transform::reads_an_instant`).
    pub read_an_instant: bool,
}

/// Flags indicating which status predicates appeared in the filter expression.
#[derive(Debug, Default, Clone)]
pub struct UsedStatus {
    pub archived: bool,
    pub hashed: bool,
    pub excluded: bool,
    pub enriched: bool,
}

impl UsedStatus {
    /// Whether the expression asked for this predicate.
    ///
    /// Reading the flags through here rather than field by field is what
    /// keeps the prefetch honest: a predicate with no flag stops the build
    /// instead of reaching evaluation with nothing loaded.
    ///
    /// The flags themselves are public; this reading of them is not. The
    /// facility re-exports the type, so a bare `pub` here would put the
    /// method on the crate's surface — and what it exists to serve is one
    /// caller, inside.
    pub(in crate::expr) fn uses(&self, predicate: StatusPredicate) -> bool {
        match predicate {
            StatusPredicate::Archived => self.archived,
            StatusPredicate::Hashed => self.hashed,
            StatusPredicate::Excluded => self.excluded,
            StatusPredicate::Enriched => self.enriched,
        }
    }
}

/// Whether a character may stand unquoted in a glob after `~` or `!~`: letters and digits plus
/// `-`, `/`, `?`, `*`, `.`, `_`, `:`, `|`, `[`, `]`, `+`, `@`, `#`, `%`.
pub(in crate::expr) fn is_value_char(c: char) -> bool {
    c.is_alphanumeric()
        || matches!(
            c,
            '_' | '.' | '-' | '/' | ':' | '*' | '?' | '[' | ']' | '|' | '+' | '@' | '#' | '%'
        )
}

// ============================================================================
// Value Comparison
// ============================================================================

/// Compare text by `op`: equality as written, ordering in character order (by Unicode code
/// point, case-sensitive), `~` a glob.
pub(in crate::expr) fn compare_text(stored: &str, op: CompareOp, value: &str) -> bool {
    match op {
        CompareOp::Eq => stored == value,
        CompareOp::Gt => stored > value,
        CompareOp::Ge => stored >= value,
        CompareOp::Lt => stored < value,
        CompareOp::Le => stored <= value,
        CompareOp::Glob => glob_match(stored, value),
    }
}

/// Compare numbers by `op`; equality keeps a tolerance for numbers that differ only by
/// rounding. A glob never reaches here: binding refuses it against a number.
pub(in crate::expr) fn compare_numbers(stored: f64, op: CompareOp, value: f64) -> bool {
    match op {
        CompareOp::Eq => (stored - value).abs() < f64::EPSILON,
        CompareOp::Gt => stored > value,
        CompareOp::Ge => stored >= value,
        CompareOp::Lt => stored < value,
        CompareOp::Le => stored <= value,
        CompareOp::Glob => false,
    }
}

/// The row of the whole-period table an operator reads; `None` for a glob, which binding
/// refuses against a time.
pub(in crate::expr) fn period_op(op: CompareOp) -> Option<PeriodOp> {
    match op {
        CompareOp::Eq => Some(PeriodOp::Inside),
        CompareOp::Ge => Some(PeriodOp::StartsAtOrAfterStart),
        CompareOp::Gt => Some(PeriodOp::StartsAtOrAfterEnd),
        CompareOp::Le => Some(PeriodOp::EndsByEnd),
        CompareOp::Lt => Some(PeriodOp::EndsByStart),
        CompareOp::Glob => None,
    }
}

/// Match a string against a glob pattern (case-sensitive).
///
/// Supports:
/// - `*` matches zero or more characters
/// - `?` matches exactly one character
/// - `[abc]` matches any character in the set
/// - `[!abc]` or `[^abc]` matches any character NOT in the set
/// - `[a-z]` matches character ranges
/// - `\*`, `\?`, `\[` for literal matching
pub(in crate::expr) fn glob_match(text: &str, pattern: &str) -> bool {
    let text_chars: Vec<char> = text.chars().collect();
    let pattern_chars: Vec<char> = pattern.chars().collect();
    glob_match_recursive(&text_chars, 0, &pattern_chars, 0)
}

fn glob_match_recursive(text: &[char], ti: usize, pattern: &[char], pi: usize) -> bool {
    // Both exhausted - match
    if pi >= pattern.len() && ti >= text.len() {
        return true;
    }

    // Pattern exhausted but text remaining - no match
    if pi >= pattern.len() {
        return false;
    }

    // Handle escape sequences
    if pattern[pi] == '\\' && pi + 1 < pattern.len() {
        if ti < text.len() && text[ti] == pattern[pi + 1] {
            return glob_match_recursive(text, ti + 1, pattern, pi + 2);
        }
        return false;
    }

    // Handle wildcards
    match pattern[pi] {
        '*' => {
            // Try matching zero or more characters
            // First try matching zero characters (skip the *)
            if glob_match_recursive(text, ti, pattern, pi + 1) {
                return true;
            }
            // Then try matching one character and continue with same *
            if ti < text.len() && glob_match_recursive(text, ti + 1, pattern, pi) {
                return true;
            }
            false
        }
        '?' => {
            // Match exactly one character
            if ti < text.len() {
                glob_match_recursive(text, ti + 1, pattern, pi + 1)
            } else {
                false
            }
        }
        '[' => {
            // Character class
            if ti >= text.len() {
                return false;
            }
            match parse_char_class(pattern, pi) {
                Some((matches_set, end_pi)) => {
                    let c = text[ti];
                    let in_set = matches_set.contains(&c);
                    // Check if it's a negated class
                    let negated = pi + 1 < pattern.len()
                        && (pattern[pi + 1] == '!' || pattern[pi + 1] == '^');
                    let matches = if negated { !in_set } else { in_set };
                    if matches {
                        glob_match_recursive(text, ti + 1, pattern, end_pi)
                    } else {
                        false
                    }
                }
                None => {
                    // Invalid char class, treat '[' as literal
                    if ti < text.len() && text[ti] == '[' {
                        glob_match_recursive(text, ti + 1, pattern, pi + 1)
                    } else {
                        false
                    }
                }
            }
        }
        c => {
            // Literal character
            if ti < text.len() && text[ti] == c {
                glob_match_recursive(text, ti + 1, pattern, pi + 1)
            } else {
                false
            }
        }
    }
}

/// Parse a character class like [abc], [!abc], [a-z], [!a-z0-9]
/// Returns the set of matching characters and the index after the closing ']'
fn parse_char_class(pattern: &[char], start: usize) -> Option<(Vec<char>, usize)> {
    if pattern[start] != '[' {
        return None;
    }

    let mut i = start + 1;
    let mut chars = Vec::new();

    // Check for negation
    let negated = i < pattern.len() && (pattern[i] == '!' || pattern[i] == '^');
    if negated {
        i += 1;
    }

    // Handle ] as first char (literal)
    if i < pattern.len() && pattern[i] == ']' {
        chars.push(']');
        i += 1;
    }

    while i < pattern.len() && pattern[i] != ']' {
        // Check for range like a-z
        if i + 2 < pattern.len() && pattern[i + 1] == '-' && pattern[i + 2] != ']' {
            let start_c = pattern[i];
            let end_c = pattern[i + 2];
            for c in start_c..=end_c {
                chars.push(c);
            }
            i += 3;
        } else {
            chars.push(pattern[i]);
            i += 1;
        }
    }

    if i >= pattern.len() {
        // No closing ']'
        return None;
    }

    Some((chars, i + 1)) // +1 to skip the closing ']'
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob_matches_wildcards_and_classes() {
        assert!(glob_match("photo.jpg", "*.jpg"));
        assert!(!glob_match("photo.png", "*.jpg"));
        assert!(glob_match("anything", "*"));
        assert!(glob_match("", "*"));
        assert!(glob_match("photo.jpg", "photo.???"));
        assert!(!glob_match("photo.jpeg", "photo.???"));
        assert!(glob_match("a", "[abc]"));
        assert!(!glob_match("d", "[abc]"));
        assert!(glob_match("m", "[a-z]"));
        assert!(!glob_match("M", "[a-z]"));
    }

    #[test]
    fn glob_negated_and_literal_classes() {
        assert!(glob_match("d", "[!abc]"));
        assert!(!glob_match("a", "[!abc]"));
        assert!(glob_match("d", "[^abc]"));
        assert!(!glob_match("a", "[^abc]"));
        // A ']' first in the class is the literal bracket, not the terminator.
        assert!(glob_match("]", "[]abc]"));
        assert!(glob_match("b", "[]abc]"));
        // A class that is never closed is a literal '[', not an error.
        assert!(glob_match("[abc", "[abc"));
        assert!(!glob_match("a", "[abc"));
    }

    #[test]
    fn glob_escapes_metacharacters() {
        assert!(glob_match("*", r"\*"));
        assert!(!glob_match("x", r"\*"));
        assert!(glob_match("?", r"\?"));
        assert!(!glob_match("x", r"\?"));
        assert!(glob_match("[", r"\["));
        assert!(glob_match("a*b", r"a\*b"));
        assert!(!glob_match("axb", r"a\*b"));
    }
}
