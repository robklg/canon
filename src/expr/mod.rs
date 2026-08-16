//! Expression system for canon.
//!
//! Pattern evaluation, filter parsing, and fact value handling:
//! - Pattern parsing and evaluation for manifest output
//! - Filter expression parsing for `--where` clauses
//! - Fact value types and transformations

// Re-exports are intentional for public API convenience
#![allow(unused_imports)]

pub mod alias;
pub mod eval;
pub mod filter;
pub mod value;

// Re-export primary types from eval.rs
pub use eval::{
    apply_accessor, apply_modifier, evaluate, extract_fact_keys, is_builtin_key,
    normalize_fact_key, normalize_key_string, parse_key_and_accessor, parse_key_with_modifiers,
    parse_modifier, parse_pattern, BuiltinKey, BuiltinKeyCategory, BuiltinKeyVisibility,
    EvalContext, Expr, Modifier, ModifierCall, ModifierCategory, ParsedFactKey, PathAccessor,
    Pattern, PatternSegment,
};

// Re-export from filter.rs
pub use filter::{apply_filters, CompareOp, Filter};

// Re-export from value.rs
pub use value::{fact_value_to_display, get_builtin_value, resolve_fact_value};
