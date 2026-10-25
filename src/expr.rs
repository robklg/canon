//! The expression facility: the language for speaking about content in terms
//! of facts, and everything that applies it.
//!
//! One vocabulary, two halves — filters ask, patterns shape — plus the pieces
//! that turn either half into an answer: resolving a key to a value, reading
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

// The asking half: `--where` strings, as given or as a manifest specified them, read and
// bound — or refused, with every reason — before anything runs; a bound filter, run, and what
// the run reports about itself. The key `facts --key` and `--group-by` name is read the same
// way, its aliases with the filters'.
pub use domain::bind::ExpressionRefused;
pub use domain::filter::UsedStatus;
pub use ops::bind::{read_filters, read_report, BoundFilters, ExpressionText};
pub use ops::filter::{apply_filters, apply_filters_noting};

// The language applied within a scope — the selector every command resolves
// through, so that what is acted on is what was seen.
pub use ops::selection::{select_sources, RolePolicy, SelectionParams};

// What a selecting run recorded beside its evaluation, and the lines an acting command's
// plan states from it: how many of what it acts on have no value for a negated key, and which
// stored times a comparison could not place. `NoteMarks` is carried as a parameter type
// alone: `SelectionNotes::of` takes it, and a caller factoring a helper must name it.
#[allow(unused_imports)]
pub use ops::notes::NoteMarks;
pub use ops::notes::{NoteUnit, SelectionNotes};

// The shaping half: a manifest's pattern, read and checked against the stored keys' types —
// or refused, with every reason — inspected for the keys it needs and the placement it commits
// to, and evaluated against a source: where it lands, or every cause it has no place —
// together with the scope-relative path the run that selected the source settled for it, and,
// where there is none, the run's own reason there is none.
pub use domain::pattern::{
    evaluate, BoundPattern, EvalContext, NoPlace, NoPlaceCause, NotPlaced, Placement, Unmeasured,
};
pub use domain::vantage::ScopeVantage;
pub use ops::pattern::read_pattern;

// The facts a pattern needs, read out of the database with the
// context-supplied law already applied. The law itself stays inside: a
// predicate three callers must remember to call is the failure mode being
// repaired, so what leaves the facility is the fetch, not the rule.
pub use domain::pattern::PatternFacts;
pub use ops::pattern::prefetch_pattern_facts;

// Resolving a key to a value, and rendering one for display — together with
// the attributes a source's own row answers a key from, which both halves of
// the language hand the resolver.
pub use domain::value::{resolve_fact_value, source_row_value, SourceAttributes};

// A source's one value for a stored key: the rule applied to a source's facts
// already in hand, and the fetch that applies it to a batch — and a key of any
// kind read for a batch as an expression reads it.
pub use domain::value::source_values;
pub use ops::value::{fetch_named_values, fetch_source_values};

// The key vocabulary: the one reader of a key's name, the three kinds of key,
// and what a written one parsed to — plus the content hash's key and the
// pattern-only key, which a manifest writes down.
pub use domain::key::{
    read_key_name, BuiltinKey, Key, KeyKind, Listing, ParsedFactKey, SourceFact, CONTENT_HASH,
    SCOPE_REL_PATH,
};

// What a modifier gives — a value, or none — and the one printed form of what it gave.
pub use domain::transform::{rendered, Modified, Modifier, ModifierCategory};

// The statement that keys are of mixed type and its way out, each key printed by the
// printing rule — read by `facts`' listing, `cluster`'s summary and a manifest's comments —
// and the printing rule's key and convert command, which an import's skip lines print.
pub use domain::print::{convert_command, mixed_type_statement, mixed_type_way_out, printed_key};
