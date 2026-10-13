//! The expression facility: the language for speaking about content in terms
//! of facts, and everything that applies it.
//!
//! One vocabulary, two halves — filters ask, patterns shape — plus the pieces
//! that turn either half into an answer: resolving a key to a value, expanding
//! the `@name` shorthands a filter may be written with, and selection, the
//! language applied within a scope. A language rule is spoken once, here; the
//! rest of the engine consumes finished results rather than re-deriving them.
//!
//! Everything below this line is the whole of the facility from outside. The
//! strata are private: how the language is divided into grammar, application
//! and storage is the facility's own business, and rearranging it must never
//! be a change any caller can see.

mod domain;
mod ops;
mod repo;

// ---------------------------------------------------------------------------
// Named externally
//
// What the rest of the engine actually writes down. Each of these appears by
// name in at least one consumer outside the facility.
// ---------------------------------------------------------------------------

// The asking half: `--where` strings read and bound — or refused, with every reason —
// before anything runs; a bound filter, run, and what the run reports about itself. The key
// `facts --key` and `--group-by` name is read the same way.
pub use domain::bind::FilterRefused;
pub use domain::filter::UsedStatus;
pub use ops::bind::{read_filters, read_report, BoundFilters};
pub use ops::filter::apply_filters;

// The language applied within a scope — the selector every command resolves
// through, so that what is acted on is what was seen.
pub use ops::selection::{select_sources, RolePolicy, SelectionParams};

// Alias expansion, which runs before anything else parses a filter.
pub use ops::alias::expand_filter_strings;

// The shaping half: a pattern, parsed, inspected for the keys it needs, read
// for the placement it commits to, and evaluated against a source — together
// with the scope-relative path the run that selected the source settled for
// it, and, where there is none, the run's own reason there is none.
pub use domain::pattern::{
    evaluate, extract_fact_keys, parse_pattern, placement_shape, EvalContext, Pattern,
    PatternNoValue, Unmeasured,
};
pub use domain::vantage::ScopeVantage;

// The facts a pattern needs, read out of the database with the
// context-supplied law already applied. The law itself stays inside: a
// predicate three callers must remember to call is the failure mode being
// repaired, so what leaves the facility is the fetch, not the rule.
pub use domain::pattern::PatternFacts;
pub use ops::pattern::prefetch_pattern_facts;

// Resolving a key to a value, and rendering one for display — together with
// the attributes a built-in is computed from, which both halves of the
// language hand the resolver.
pub use domain::value::{get_builtin_value, resolve_fact_value, SourceAttributes};

// A source's one value for a stored key: the rule applied to a source's facts
// already in hand, and the fetch that applies it to a batch.
pub use domain::value::source_values;
pub use ops::value::fetch_source_values;

// The key vocabulary: what a key may name, and what a written one parsed to —
// plus the two spellings of the pattern-only keys, which a manifest writes
// down.
pub use domain::key::{
    BuiltinKey, BuiltinKeyCategory, BuiltinKeyVisibility, ParsedFactKey, OBJECT_HASH,
    SCOPE_REL_PATH,
};

// What a modifier gives — a value, or none — and the one printed form of what it gave.
pub use domain::transform::{rendered, Modified, Modifier, ModifierCategory};

// The statement that keys are of mixed type and its way out, each key printed by the
// printing rule — read by `facts`' listing, `cluster`'s summary and a manifest's comments.
pub use domain::print::{mixed_type_statement, mixed_type_way_out};
