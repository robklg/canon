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

// The asking half: a filter, run, and what the run reports about itself.
pub use domain::filter::{Filter, UsedStatus};
pub use ops::filter::apply_filters;

// The language applied within a scope — the selector every command resolves
// through, so that what is acted on is what was seen.
pub use ops::selection::{select_sources, RolePolicy, SelectionParams};

// Alias expansion, which runs before anything else parses a filter.
pub use ops::alias::expand_filter_strings;

// The shaping half: a pattern, parsed, inspected for the keys it needs, and
// evaluated against a source.
pub use domain::pattern::{evaluate, extract_fact_keys, parse_pattern, EvalContext, Pattern};

// Resolving a key to a value, and rendering one for display.
pub use domain::value::{fact_value_to_display, get_builtin_value, resolve_fact_value};

// The key vocabulary: what a key may name, and what a written one parsed to.
pub use domain::key::{BuiltinKey, BuiltinKeyCategory, BuiltinKeyVisibility, ParsedFactKey};

// The transforms a value can be put through.
pub use domain::transform::{apply_accessor, apply_modifier, Modifier, ModifierCategory};

// ---------------------------------------------------------------------------
// Completing the surface
//
// Parameter types of items above. Nothing outside names them today, but the
// claim is structural rather than observed: `PathAccessor` and `ModifierCall`
// are parameter types of `apply_accessor` and `apply_modifier`, so a caller
// that wants to factor out a helper taking one must be able to name it.
//
// Return types are deliberately absent, per the parameter-type rule: inference
// always lets a caller leave one unnamed, so carrying it would record demand
// that does not exist. A consumer that genuinely needs to write one down —
// storing it in a field, passing it onward — earns the item a place here by
// a one-line pin edit, with that consumer as the evidence.
//
// The allow sits on the re-export statement rather than on the file, so that
// a re-export which becomes dead for any other reason still says so.
// ---------------------------------------------------------------------------

#[allow(unused_imports)]
pub use domain::transform::{ModifierCall, PathAccessor};

// ---------------------------------------------------------------------------
// Reaching past the front door
//
// One point read that answers a single question about a single key, exposed
// because a caller outside still asks it directly rather than through the
// language. It is the facility's one leak of its own storage, and it closes
// when that caller is rewritten to ask the language instead.
//
// Fully qualified on purpose: a front door reads as interface code, and a
// bare `repo::` there would name the shared repository layer rather than this
// facility's own stratum.
// ---------------------------------------------------------------------------

pub use crate::expr::repo::get_fact_value;
