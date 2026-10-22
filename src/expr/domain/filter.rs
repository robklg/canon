//! The filter language: its syntax tree and its comparisons.
//!
//! `--where` is the asking half of the expression language — the half that
//! picks content out of the universe. The reader (`read.rs`) reads a typed
//! filter into this tree of boolean and comparison nodes; binding reads each
//! value as its key's type, and evaluation walks the bound tree against
//! sources. Everything here is pure: the tree is built and compared without
//! ever touching a database.

use super::glob::InputGlob;
use super::key::InputKey;
use super::problem::Place;
use crate::core::domain::time::value::PeriodOp;

// ============================================================================
// Expression AST
// ============================================================================

/// The comparison operators. `~` is no comparison: a glob is its own condition
/// ([`Expr::Glob`]). `!=`, `!~` and `NOT IN` have no operator of their own: each reads as `NOT`
/// of its positive form, so "everything else" means one thing everywhere.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CompareOp {
    Eq,
    Gt,
    Ge,
    Lt,
    Le,
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
    /// `key~glob`; `!~` reads as `NOT` of it.
    Glob {
        key: InputKey,
        glob: InputGlob,
        clause: String,
        place: Place,
    },
    Status {
        predicate: StatusPredicate,
        place: Place,
    },
}

impl Expr {
    /// Every place the tree holds, for a copy that stands elsewhere: a filter alias's reading
    /// at its use.
    pub(in crate::expr) fn each_place(&mut self, f: &mut impl FnMut(&mut Place)) {
        match self {
            Expr::And(all) | Expr::Or(all) => all.iter_mut().for_each(|e| e.each_place(f)),
            Expr::Not(inner) => inner.each_place(f),
            Expr::Exists { key, place, .. } => {
                key.each_place(f);
                f(place);
            }
            Expr::Compare {
                key, value, place, ..
            } => {
                key.each_place(f);
                f(&mut value.place);
                f(place);
            }
            Expr::In {
                key, values, place, ..
            } => {
                key.each_place(f);
                values.iter_mut().for_each(|v| f(&mut v.place));
                f(place);
            }
            Expr::Glob {
                key, glob, place, ..
            } => {
                key.each_place(f);
                f(&mut glob.place);
                f(place);
            }
            Expr::Status { place, .. } => f(place),
        }
    }
}

/// Result of applying filters, including metadata about which status predicates were used.
pub struct FilterResult {
    pub source_ids: Vec<i64>,
    pub used_status: UsedStatus,
    /// Whether any comparison read an instant on the clock
    /// (each chain's own answer, `domain::transform::Chained`).
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

// ============================================================================
// Value Comparison
// ============================================================================

/// Compare text by `op`: equality as written, ordering in character order (by Unicode code
/// point, case-sensitive).
pub(in crate::expr) fn compare_text(stored: &str, op: CompareOp, value: &str) -> bool {
    match op {
        CompareOp::Eq => stored == value,
        CompareOp::Gt => stored > value,
        CompareOp::Ge => stored >= value,
        CompareOp::Lt => stored < value,
        CompareOp::Le => stored <= value,
    }
}

/// Compare numbers by `op`, exactly: a typed number is refused where Canon cannot hold it as
/// written, so equality needs no tolerance.
pub(in crate::expr) fn compare_numbers(stored: f64, op: CompareOp, value: f64) -> bool {
    match op {
        CompareOp::Eq => stored == value,
        CompareOp::Gt => stored > value,
        CompareOp::Ge => stored >= value,
        CompareOp::Lt => stored < value,
        CompareOp::Le => stored <= value,
    }
}

/// The row of the whole-period table an operator reads.
pub(in crate::expr) fn period_op(op: CompareOp) -> PeriodOp {
    match op {
        CompareOp::Eq => PeriodOp::Inside,
        CompareOp::Ge => PeriodOp::StartsAtOrAfterStart,
        CompareOp::Gt => PeriodOp::StartsAtOrAfterEnd,
        CompareOp::Le => PeriodOp::EndsByEnd,
        CompareOp::Lt => PeriodOp::EndsByStart,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Numbers compare exactly: no source is both `= 0` and `> 0`, and two numbers that differ
    /// only past what a tolerance would see are two numbers.
    #[test]
    fn equal_numbers_compare_exactly() {
        assert!(!compare_numbers(1e-300, CompareOp::Eq, 0.0));
        assert!(compare_numbers(1e-300, CompareOp::Gt, 0.0));
        assert!(!compare_numbers(0.1 + 0.2, CompareOp::Eq, 0.3));
        assert!(compare_numbers(0.1, CompareOp::Eq, 0.1));
        assert!(compare_numbers(-0.0, CompareOp::Eq, 0.0));
    }
}
