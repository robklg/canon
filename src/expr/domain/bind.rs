//! **Binding**: between reading a filter and looking at any source, each typed value is read
//! once, as the type its key gives — a year against a time key, a number against a number
//! key, text against a text key — and a value that cannot mean anything for its key is
//! refused by name, with what to type instead. Pure: the key types arrive as data
//! (`expr::ops::bind` reads them), so every rule here is tested with no database.
//!
//! Three things are spoken here once. **The modifier table** ([`result_of`]): what each
//! modifier accepts and gives, read by binding for every key a filter or a report names.
//! **The bare-value classes** ([`bare_class`]): a bare value is number-shaped, time-shaped or
//! a word, and against text only a word is text. **The refusal** ([`FilterRefused`]): one
//! entry per cause, each starting with the clause as written, its ways out rewriting only
//! what they change, so following one binds.

use std::collections::HashMap;
use std::fmt;

use strum::IntoEnumIterator;

use crate::core::domain::fact::{FactType, FactValue, KeyTypes};
use crate::core::domain::format::format_count;
use crate::core::domain::number::read_number;
use crate::core::domain::time::reading::{read_time, read_time_of_day, EXAMPLE_FORMS};
use crate::core::domain::time::value::{Precision, TimeValue};

use super::filter::{CompareOp, Expr, Literal, StatusPredicate, UsedStatus, WrittenKey};
use super::key::{Key, CONTENT_HASH};
use super::print::{convert_command, mixed_type_way_out, printed_key, printed_value, printed_word};
use super::transform::{
    is_magnitude_bucket_label, threshold_bucket_labels, Modifier, ModifierCall, ModifierCategory,
    PathAccessor,
};

// ============================================================================
// What a key gives, and what a typed value reads as
// ============================================================================

/// What a key's chain gives: its base type through its accessor and modifiers.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::expr) enum Gives {
    Text,
    Path,
    Number,
    /// A time at the value's own grain: a time key, `|datetime`.
    Time,
    /// A time at a fixed grain: `|date` (day), `|yearmonth` (month).
    TimeAt(Precision),
    /// A time of day: `|time`.
    TimeOfDay,
    /// A number part: `|year` … `|quarter`.
    Part(Modifier),
    /// A bucket's label, from `|bucket` (no thresholds) or `|bucket(a,b,…)`.
    Bucket(Vec<f64>),
}

impl Gives {
    fn of(t: FactType) -> Gives {
        match t {
            FactType::Text => Gives::Text,
            FactType::Path => Gives::Path,
            FactType::Num => Gives::Number,
            FactType::Time => Gives::Time,
        }
    }

    /// What a value of this kind is, in the plural a statement uses.
    fn words(&self) -> String {
        match self {
            Gives::Text => "text".into(),
            Gives::Path => "paths".into(),
            Gives::Number => "numbers".into(),
            Gives::Time => "times".into(),
            Gives::TimeAt(Precision::Month) => "months".into(),
            Gives::TimeAt(_) => "days".into(),
            Gives::TimeOfDay => "times of day".into(),
            Gives::Part(m) => part_rule(*m).plural.into(),
            Gives::Bucket(_) => "bucket ranges".into(),
        }
    }

    /// Whether text modifiers read this result as text without a modifier before them.
    fn is_text(&self) -> bool {
        matches!(self, Gives::Text | Gives::Path | Gives::Bucket(_))
    }
}

/// A typed value, read once as the type its key gives.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::expr) enum Test {
    Text(String),
    Number(f64),
    /// A typed period, translated to the user's local time when the filter runs.
    Period(TimeValue),
    /// A time of day: a clock reading on the reference day.
    TimeOfDay(TimeValue),
    Part(i64),
    Bucket(String),
    Glob(String),
}

// ============================================================================
// The bound tree
// ============================================================================

/// A key, read and checked: the key as written, and what a value read for it may be while the
/// run reads it ([`ReadAs`]).
#[derive(Debug, Clone)]
pub(in crate::expr) struct BoundKey {
    pub(in crate::expr) written: WrittenKey,
    pub(in crate::expr) read_as: ReadAs,
}

/// What a run may meet for a key, as the key stood when it was read.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(in crate::expr) enum ReadAs {
    /// No value is read through a chain or compared — a bare `?`, a bare report row — so any
    /// type will do.
    Any,
    /// The key had no facts: any value the run meets is new.
    Absent,
    /// The key's one type.
    One(FactType),
}

/// **A key's type changing while a run reads it**, checked one way for a filter and a report:
/// a value of another type than the key was read as, or a value where the key had none, stops
/// the run as an answer (`TypeChanged`) — never a modifier failing, never no value.
pub(in crate::expr) fn check_read_as(
    read_as: ReadAs,
    key: &str,
    value: &FactValue,
    refused: Refused,
) -> anyhow::Result<()> {
    let changed = match read_as {
        ReadAs::Any => false,
        ReadAs::Absent => true,
        ReadAs::One(t) => value.kind() != t,
    };
    if changed {
        return Err(FilterRefused {
            refused,
            causes: vec![BindCause::TypeChanged {
                key: key.to_string(),
                had_no_facts: read_as == ReadAs::Absent,
            }],
        }
        .into());
    }
    Ok(())
}

/// The parsed tree with every key checked and every value read.
#[derive(Debug, Clone)]
pub(in crate::expr) enum Bound {
    And(Vec<Bound>),
    Or(Vec<Bound>),
    Not(Box<Bound>),
    Exists {
        key: BoundKey,
    },
    Compare {
        key: BoundKey,
        op: CompareOp,
        test: Test,
    },
    In {
        key: BoundKey,
        tests: Vec<Test>,
    },
    Status(StatusPredicate),
}

impl Bound {
    /// Which status predicates the tree asks for.
    pub(in crate::expr) fn note_status(&self, used: &mut UsedStatus) {
        match self {
            Bound::And(all) | Bound::Or(all) => all.iter().for_each(|b| b.note_status(used)),
            Bound::Not(inner) => inner.note_status(used),
            Bound::Status(predicate) => match predicate {
                StatusPredicate::Archived => used.archived = true,
                StatusPredicate::Hashed => used.hashed = true,
                StatusPredicate::Excluded => used.excluded = true,
                StatusPredicate::Enriched => used.enriched = true,
            },
            Bound::Exists { .. } | Bound::Compare { .. } | Bound::In { .. } => {}
        }
    }

    /// The base keys the tree reads, for prefetching.
    pub(in crate::expr) fn keys(&self, out: &mut Vec<String>) {
        match self {
            Bound::And(all) | Bound::Or(all) => all.iter().for_each(|b| b.keys(out)),
            Bound::Not(inner) => inner.keys(out),
            Bound::Exists { key } | Bound::Compare { key, .. } | Bound::In { key, .. } => {
                out.push(key.written.base.clone())
            }
            Bound::Status(_) => {}
        }
    }

    /// The typed values the tree holds.
    pub(in crate::expr) fn tests(&self, out: &mut Vec<Test>) {
        match self {
            Bound::And(all) | Bound::Or(all) => all.iter().for_each(|b| b.tests(out)),
            Bound::Not(inner) => inner.tests(out),
            Bound::Compare { test, .. } => out.push(test.clone()),
            Bound::In { tests, .. } => out.extend(tests.iter().cloned()),
            Bound::Exists { .. } | Bound::Status(_) => {}
        }
    }
}

/// The type a base key has before any of its facts is read (`Key::known_type`): a source
/// fact's, a built-in key's, the content hash's. `None`: read it from the key's facts.
pub(in crate::expr) fn known_type(base: &str) -> Option<FactType> {
    Key::read(base).and_then(|key| key.known_type())
}

/// The stored keys an expression names — every base key whose type is read from its facts —
/// whose types binding reads.
pub(in crate::expr) fn stored_keys(expr: &Expr, out: &mut Vec<String>) {
    match expr {
        Expr::And(all) | Expr::Or(all) => all.iter().for_each(|e| stored_keys(e, out)),
        Expr::Not(inner) => stored_keys(inner, out),
        Expr::Exists { key, .. } | Expr::Compare { key, .. } | Expr::In { key, .. } => {
            if known_type(&key.base).is_none() && !out.contains(&key.base) {
                out.push(key.base.clone())
            }
        }
        Expr::Status(_) | Expr::Unread { .. } => {}
    }
}

/// The types of a key: its known type, or what its stored facts hold (`types`, read from
/// every fact of the key); `None` for a key with no facts.
fn types_of(base: &str, types: &HashMap<String, KeyTypes>) -> Option<KeyTypes> {
    match known_type(base) {
        Some(known) => Some(KeyTypes::of(known)),
        None => types.get(base).cloned(),
    }
}

/// **Bind a parsed expression**: every key checked against the modifier table, every value
/// read as the type its key gives. Every cause is collected; where there is one, the
/// returned tree is not to be run.
///
/// Each cause's ways out are bound against the same key types before the refusal is shown,
/// and one that would be refused again is dropped ([`keep_ways_that_bind`]).
pub(in crate::expr) fn bind_expr(
    expr: &Expr,
    types: &HashMap<String, KeyTypes>,
    causes: &mut Vec<BindCause>,
) -> Bound {
    let before = causes.len();
    let bound = bind_clauses(expr, types, causes);
    let binds = |rewrite: &str| {
        Expr::parse(rewrite).is_ok_and(|expr| {
            let mut again = Vec::new();
            bind_clauses(&expr, types, &mut again);
            again.is_empty()
        })
    };
    for cause in &mut causes[before..] {
        keep_ways_that_bind(cause, &binds);
    }
    bound
}

/// **A way out makes what it rewrites bind**: drop every way out of `cause` that `binds`
/// refuses — a filter's clause bound as a clause, a report's key as a key. A wrong modifier
/// left with no way out names the modifiers that apply. It holds for a way out offered through
/// [`Way::to`] at a door that calls this — today both doors that bind a key (`bind_expr`,
/// `bind_report_key`). Both halves are discipline, not the type: a hint is free text, and a
/// third caller of `bind_key` must call this too.
pub(in crate::expr) fn keep_ways_that_bind(cause: &mut BindCause, binds: &dyn Fn(&str) -> bool) {
    if let BindCause::WrongModifier { entry, fallback } = cause {
        entry.keep_ways_that_bind(binds);
        if entry.lines.is_empty() {
            entry.lines.push(vec![Way::hint(std::mem::take(fallback))]);
        }
    } else if let Some(entry) = cause.entry_mut() {
        entry.keep_ways_that_bind(binds);
    }
}

/// Bind every clause of `expr`, collecting causes with their ways out as built.
fn bind_clauses(
    expr: &Expr,
    types: &HashMap<String, KeyTypes>,
    causes: &mut Vec<BindCause>,
) -> Bound {
    let refused = || Bound::And(Vec::new());
    match expr {
        Expr::And(all) => Bound::And(all.iter().map(|e| bind_clauses(e, types, causes)).collect()),
        Expr::Or(all) => Bound::Or(all.iter().map(|e| bind_clauses(e, types, causes)).collect()),
        Expr::Not(inner) => Bound::Not(Box::new(bind_clauses(inner, types, causes))),
        Expr::Status(predicate) => Bound::Status(*predicate),
        Expr::Unread { clause, error } => {
            causes.push(BindCause::Unparsed(Entry::new(
                clause,
                error.clone(),
                Vec::new(),
            )));
            refused()
        }
        Expr::Exists { key, clause } => match bind_key(key, clause, Reads::Existence, types) {
            Ok((bound, _)) => Bound::Exists { key: bound },
            Err(cause) => {
                causes.push(cause);
                refused()
            }
        },
        Expr::Compare {
            key,
            op,
            value,
            clause,
        } => match bind_key(key, clause, Reads::Value, types) {
            Ok((bound, chain)) => {
                let site = Site {
                    key,
                    clause,
                    chain: &chain,
                    list: None,
                };
                match site.read(*op, value) {
                    Ok(test) => Bound::Compare {
                        key: bound,
                        op: *op,
                        test,
                    },
                    Err(cause) => {
                        causes.push(cause);
                        refused()
                    }
                }
            }
            Err(cause) => {
                causes.push(cause);
                refused()
            }
        },
        Expr::In {
            key,
            values,
            clause,
        } => match bind_key(key, clause, Reads::Value, types) {
            Ok((bound, chain)) => {
                let site = Site {
                    key,
                    clause,
                    chain: &chain,
                    list: Some(values),
                };
                let before = causes.len();
                let tests: Vec<Test> = values
                    .iter()
                    .filter_map(|value| match site.read(CompareOp::Eq, value) {
                        Ok(test) => Some(test),
                        Err(cause) => {
                            causes.push(cause);
                            None
                        }
                    })
                    .collect();
                if causes.len() > before {
                    refused()
                } else {
                    Bound::In { key: bound, tests }
                }
            }
            Err(cause) => {
                causes.push(cause);
                refused()
            }
        },
    }
}

/// What a clause reads of its key: only whether a value exists (`k?`), or the value.
#[derive(Clone, Copy, PartialEq)]
pub(in crate::expr) enum Reads {
    Existence,
    Value,
    /// A report's key (`facts --key`, `--group-by`): every value it holds, of whatever type;
    /// only a modifier works on one type.
    Report,
}

/// What a key's chain gives, and whether a modifier gave it.
#[derive(Debug, Clone)]
pub(in crate::expr) struct Chain {
    gives: Gives,
    from_modifier: bool,
    /// The full stored key, where a conversion pointer can be owed: a stored key read with no
    /// accessor and no modifier. Whether it is owed is the pointer's own gate.
    convertible: Option<String>,
}

/// Check a key: its type from all its facts, then its accessor and modifiers through the
/// table. A key with no facts binds only for `?` (it has no value anywhere); a key of two
/// types binds only for a bare `?`, which reads no value. A name with no reading in an
/// expression — a `source.` name that is no source fact — names no key, and is refused
/// wherever it stands.
pub(in crate::expr) fn bind_key(
    key: &WrittenKey,
    clause: &str,
    reads: Reads,
    types: &HashMap<String, KeyTypes>,
) -> Result<(BoundKey, Chain), BindCause> {
    let unknown = || {
        BindCause::UnknownKey(Entry::new(
            clause,
            format!(
                "Unknown fact key: '{}'. Use 'canon facts' to see available keys.",
                key.base
            ),
            Vec::new(),
        ))
    };
    if Key::read(&key.base).is_none() {
        return Err(unknown());
    }
    let bare_existence = reads != Reads::Value && !key.has_transforms();
    let unbound = |gives, read_as| {
        Ok((
            BoundKey {
                written: key.clone(),
                read_as,
            },
            Chain {
                gives,
                from_modifier: false,
                convertible: None,
            },
        ))
    };
    let Some(key_types) = types_of(&key.base, types) else {
        return match reads {
            Reads::Existence | Reads::Report => unbound(
                Gives::Text,
                if key.has_transforms() {
                    ReadAs::Absent
                } else {
                    ReadAs::Any
                },
            ),
            Reads::Value => Err(unknown()),
        };
    };
    let Some(base_type) = key_types.single() else {
        if bare_existence {
            return unbound(Gives::Text, ReadAs::Any);
        }
        return Err(mixed_type(key, clause, &key_types, reads));
    };
    let chain = walk(key, base_type, clause)?;
    Ok((
        BoundKey {
            written: key.clone(),
            read_as: if bare_existence {
                ReadAs::Any
            } else {
                ReadAs::One(base_type)
            },
        },
        chain,
    ))
}

/// The two-type answer: the key as typed, its types and their counts on every root.
fn mixed_type(key: &WrittenKey, clause: &str, key_types: &KeyTypes, reads: Reads) -> BindCause {
    let reader = match reads {
        Reads::Report => "a modifier works on one type only",
        Reads::Existence | Reads::Value => "a filter compares one type only",
    };
    let mut counts = key_types.counts().to_vec();
    counts.sort_by_key(|&(_, n)| std::cmp::Reverse(n));
    let plural = |t: FactType| Gives::of(t).words();
    let types = counts
        .iter()
        .map(|&(t, _)| plural(t))
        .collect::<Vec<_>>()
        .join(" and ");
    let counted = counts
        .iter()
        .map(|&(t, n)| {
            let facts = if n == 1 { "fact" } else { "facts" };
            format!("{}: {} {facts}", plural(t), format_count(n))
        })
        .collect::<Vec<_>>()
        .join("   ");
    BindCause::MixedType {
        entry: Entry::new(
            clause,
            format!("{} holds {types}; {reader}.", key.written_name),
            vec![format!(
                "{counted}   (every fact of the key, on every root)"
            )],
        ),
        key: key.base.clone(),
    }
}

/// Walk a key's accessor and modifiers through the table.
fn walk(key: &WrittenKey, base_type: FactType, clause: &str) -> Result<Chain, BindCause> {
    let mut gives = Gives::of(base_type);
    let stored = known_type(&key.base).is_none();
    if let Some(accessor) = &key.accessor {
        if !matches!(gives, Gives::Text | Gives::Path) {
            let applicable = applicable_line(&gives, false);
            return Err(BindCause::WrongModifier {
                entry: Entry::new(
                    clause,
                    format!(
                        "an accessor ([…]) works on paths or text; {} holds {}.",
                        key.written_name,
                        gives.words()
                    ),
                    vec![applicable.clone()],
                ),
                fallback: applicable,
            });
        }
        gives = match accessor {
            PathAccessor::Index(_) => Gives::Text,
            PathAccessor::Slice { .. } => Gives::Path,
        };
    }
    let mut from_modifier = false;
    let mut previous: Option<Modifier> = None;
    for call in &key.modifiers {
        match result_of(&gives, from_modifier, call) {
            Some(next) => {
                gives = next;
                from_modifier = true;
                previous = Some(call.modifier);
            }
            None => return Err(wrong_modifier(key, clause, &gives, previous, call.modifier)),
        }
    }
    Ok(Chain {
        gives,
        from_modifier,
        convertible: (stored && !key.has_transforms()).then(|| key.base.clone()),
    })
}

/// **The modifier table**: what `call` gives, applied to `input` — `None` where it cannot
/// apply. One exhaustive match, read by binding for filters and reports alike. A text
/// modifier reads text, a path, a bucket's label, or any modifier's result in its printed
/// form; never a stored number or time directly — the user says which printed form they mean
/// (`|number`, `|datetime`).
pub(in crate::expr) fn result_of(
    input: &Gives,
    from_modifier: bool,
    call: &ModifierCall,
) -> Option<Gives> {
    // A time of day carries an hour, a minute and a second, and no date: what it is read on
    // is a reference day, which no modifier may read back as a date.
    let reads_as_time = matches!(
        input,
        Gives::Time | Gives::TimeAt(_) | Gives::Text | Gives::Bucket(_)
    ) || (*input == Gives::TimeOfDay && call.modifier.reads_a_time_of_day());
    match call.modifier {
        Modifier::Year
        | Modifier::Month
        | Modifier::Day
        | Modifier::Hour
        | Modifier::Minute
        | Modifier::Second
        | Modifier::Week
        | Modifier::Weekday
        | Modifier::Quarter => reads_as_time.then_some(Gives::Part(call.modifier)),
        Modifier::Date => reads_as_time.then_some(Gives::TimeAt(Precision::Day)),
        Modifier::YearMonth => reads_as_time.then_some(Gives::TimeAt(Precision::Month)),
        Modifier::DateTime => reads_as_time.then_some(Gives::Time),
        Modifier::Time => reads_as_time.then_some(Gives::TimeOfDay),
        Modifier::Number => matches!(
            input,
            Gives::Text | Gives::Bucket(_) | Gives::Number | Gives::Part(_)
        )
        .then_some(Gives::Number),
        Modifier::Bucket => matches!(input, Gives::Number | Gives::Part(_))
            .then(|| Gives::Bucket(call.args.clone())),
        Modifier::Stem
        | Modifier::Ext
        | Modifier::Short
        | Modifier::Lowercase
        | Modifier::Uppercase
        | Modifier::Capitalize => (input.is_text() || from_modifier).then_some(Gives::Text),
    }
}

/// What a modifier reads, as a statement says it.
fn reads_words(modifier: Modifier) -> &'static str {
    match modifier.category() {
        ModifierCategory::Time => "times or text",
        ModifierCategory::String => "text, paths or another modifier's result",
        ModifierCategory::Numeric => match modifier {
            Modifier::Number => "text or numbers",
            _ => "numbers",
        },
    }
}

fn wrong_modifier(
    key: &WrittenKey,
    clause: &str,
    gives: &Gives,
    previous: Option<Modifier>,
    modifier: Modifier,
) -> BindCause {
    let name: &'static str = modifier.into();
    let subject = match previous {
        Some(p) => format!("|{} gives", <&'static str>::from(p)),
        None => format!("{} holds", key.written_name),
    };
    // A time of day is a time, so "works on times" would not say why: say what is missing.
    let statement = if *gives == Gives::TimeOfDay && modifier.category() == ModifierCategory::Time {
        format!("|{name} needs a date; {subject} a time of day, which has none.")
    } else {
        format!(
            "|{name} works on {}; {subject} {}.",
            reads_words(modifier),
            gives.words()
        )
    };
    let tail = key
        .written
        .get(key.written_name.len()..)
        .unwrap_or_default();
    let way = match (modifier.category(), gives, previous) {
        // A text modifier directly on a stored number or time: say which printed form.
        (ModifierCategory::String, Gives::Number | Gives::Time, None) => {
            let reader = if *gives == Gives::Number {
                "number"
            } else {
                "datetime"
            };
            let new_key = format!("{}|{reader}{tail}", key.written_name);
            Way::to(
                "To use its printed form as text",
                rewrite_key(clause, key, &new_key),
            )
        }
        // `|number` reads one segment of a path, never the whole path.
        (ModifierCategory::Numeric, Gives::Path, _) if modifier == Modifier::Number => {
            let after_accessor = match tail.strip_prefix('[') {
                Some(rest) => rest.split_once(']').map_or("", |(_, r)| r),
                None => tail,
            };
            let new_key = format!("{}[0]{after_accessor}", key.written_name);
            Way::to(
                "|number works on one segment of a path",
                rewrite_key(clause, key, &new_key),
            )
        }
        _ => Way::hint(applicable_line(gives, previous.is_some())),
    };
    BindCause::WrongModifier {
        entry: Entry::with_lines(clause, statement, vec![vec![way]]),
        fallback: applicable_line(gives, previous.is_some()),
    }
}

/// The number `true` or `false` is stored as.
fn stored_boolean(text: &str) -> Option<&'static str> {
    match text {
        "true" => Some("1"),
        "false" => Some("0"),
        _ => None,
    }
}

/// The modifiers that apply to a result, read off the table itself.
fn applicable_line(gives: &Gives, from_modifier: bool) -> String {
    let applies = |m: Modifier| {
        result_of(
            gives,
            from_modifier,
            &ModifierCall {
                modifier: m,
                args: Vec::new(),
            },
        )
        .is_some()
    };
    let named = |m: Modifier| format!("|{}", <&'static str>::from(m));
    let of_category = |c: ModifierCategory| Modifier::iter().filter(move |m| m.category() == c);
    let mut parts = Vec::new();
    for category in ModifierCategory::iter() {
        let kind = match category {
            ModifierCategory::Time => "time modifiers",
            ModifierCategory::Numeric => "",
            ModifierCategory::String => "text modifiers",
        };
        let fit: Vec<Modifier> = of_category(category).filter(|&m| applies(m)).collect();
        let all = of_category(category).count();
        if kind.is_empty() || fit.len() < all {
            parts.extend(fit.into_iter().map(named));
        } else {
            let examples: Vec<String> = fit.into_iter().take(3).map(named).collect();
            parts.push(format!("{kind} ({}, …)", examples.join(", ")));
        }
    }
    if parts.is_empty() {
        format!("No modifier works on {}.", gives.words())
    } else {
        format!("Modifiers for {}: {}", gives.words(), parts.join(", "))
    }
}

// ============================================================================
// Reading a typed value
// ============================================================================

/// The bare-value classes: what a bare value looks like, before anything reads it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(in crate::expr) enum BareClass {
    /// Only digits, `+`, `-`, `.`, `e`, `E`, starting with a digit, `+`, `-` or `.` —
    /// `2024-06` among them, which reads as a month: the class catches, the reading decides.
    NumberShaped,
    /// Begins with a date (`YYYY-MM-DD`, `YYYY:MM:DD`), is digit groups joined by `:`, or is
    /// any spelling the one time reading reads.
    TimeShaped,
    /// Anything else.
    Word,
}

/// **The bare-value class** of a value typed bare.
pub(in crate::expr) fn bare_class(text: &str) -> BareClass {
    let b = text.as_bytes();
    let digits = |r: std::ops::Range<usize>| {
        b.get(r)
            .is_some_and(|s| !s.is_empty() && s.iter().all(u8::is_ascii_digit))
    };
    let begins_with_date = digits(0..4)
        && digits(5..7)
        && digits(8..10)
        && matches!(
            (b.get(4), b.get(7)),
            (Some(b'-'), Some(b'-')) | (Some(b':'), Some(b':'))
        );
    let colon_groups = text.contains(':')
        && text
            .split(':')
            .all(|g| !g.is_empty() && g.bytes().all(|c| c.is_ascii_digit()));
    let number_chars = b
        .iter()
        .all(|c| c.is_ascii_digit() || matches!(c, b'+' | b'-' | b'.' | b'e' | b'E'));
    let starts = b
        .first()
        .is_some_and(|c| c.is_ascii_digit() || matches!(c, b'+' | b'-' | b'.'));
    if number_chars && starts {
        BareClass::NumberShaped
    } else if begins_with_date || colon_groups || read_time(text).is_ok() {
        BareClass::TimeShaped
    } else {
        BareClass::Word
    }
}

/// What a shaped value reads as: a number, a time, a time of day — any that apply.
struct Readings {
    number: Option<f64>,
    time: Option<TimeValue>,
    time_of_day: bool,
}

impl Readings {
    fn of(text: &str) -> Readings {
        Readings {
            number: read_number(text).ok(),
            time: read_time(text).ok(),
            time_of_day: read_time_of_day(text).is_ok(),
        }
    }

    /// Four digits: a number, and a year.
    fn is_year_number(&self) -> bool {
        self.number.is_some() && self.time.is_some_and(|t| t.precision() == Precision::Year)
    }

    fn any(&self) -> bool {
        self.number.is_some() || self.time.is_some() || self.time_of_day
    }

    fn what(&self) -> &'static str {
        if self.is_year_number() {
            return "a number or a year";
        }
        if self.number.is_some() {
            return "a number";
        }
        match self.time.map(|t| t.precision()) {
            Some(Precision::Year) => "a year",
            Some(Precision::Month) => "a month",
            Some(Precision::Day) => "a date",
            Some(_) => "a date and time",
            None => "a time of day",
        }
    }
}

/// One clause being bound: its key, its text as written, what its chain gives.
struct Site<'a> {
    key: &'a WrittenKey,
    clause: &'a str,
    chain: &'a Chain,
    /// The values of an `IN` list, for a way out that rewrites one of them.
    list: Option<&'a [Literal]>,
}

impl Site<'_> {
    /// Read one typed value as the type the key gives, under `op`.
    fn read(&self, op: CompareOp, value: &Literal) -> Result<Test, BindCause> {
        match &self.chain.gives {
            Gives::Text | Gives::Path => self.read_text(op, value),
            Gives::Number => self.read_number(op, value),
            Gives::Time | Gives::TimeAt(_) | Gives::TimeOfDay => self.read_time(op, value),
            Gives::Part(m) => self.read_part(*m, op, value),
            Gives::Bucket(args) => self.read_bucket(args, op, value),
        }
    }

    fn entry(&self, statement: String, hints: Vec<String>) -> Entry {
        Entry::new(self.clause, statement, hints)
    }

    fn entry_with(&self, statement: String, lines: Vec<Vec<Way>>) -> Entry {
        Entry::with_lines(self.clause, statement, lines)
    }

    /// The statement's subject: the key as typed, and whether it holds or gives.
    fn subject(&self) -> String {
        let verb = if self.key.has_transforms() {
            "gives"
        } else {
            "holds"
        };
        format!("{} {verb} {}", self.key.written, self.chain.gives.words())
    }

    fn read_text(&self, op: CompareOp, value: &Literal) -> Result<Test, BindCause> {
        if op == CompareOp::Glob {
            return Ok(Test::Glob(value.text.clone()));
        }
        if value.quoted || bare_class(&value.text) == BareClass::Word {
            return Ok(Test::Text(value.text.clone()));
        }
        let readings = Readings::of(&value.text);
        let as_text = Way::to("As text", self.with_shaped_values_quoted(value));
        if !readings.any() {
            return Err(BindCause::Unreadable(self.entry_with(
                format!(
                    "{}; {} looks like a number or a time but is neither.",
                    self.subject(),
                    value.text
                ),
                vec![vec![as_text]],
            )));
        }
        let mut ways = vec![as_text];
        let all_read = |reads: &dyn Fn(&Readings) -> bool| {
            self.list
                .unwrap_or(std::slice::from_ref(value))
                .iter()
                .all(|v| reads(&Readings::of(&v.text)))
        };
        // A content hash holds hex text, never a number or a date: quoting is its one way out.
        let hash = self.key.base == CONTENT_HASH;
        // A way out rewrites the whole clause, so each is decided over every value of it, never
        // the one being read: every cause of one clause offers the same ways out.
        if !hash && all_read(&|r| r.number.is_some()) {
            if let Some(key) = self.key_with(Modifier::Number) {
                ways.push(Way::to("As numbers, if its values are numbers", key));
            }
        }
        let date_modifier = if hash {
            None
        } else if all_read(&|r| r.is_year_number()) {
            Some(Modifier::Year)
        } else if all_read(&|r| r.time.is_some()) {
            let finest = self
                .list
                .unwrap_or(std::slice::from_ref(value))
                .iter()
                .filter_map(|v| read_time(&v.text).ok())
                .map(|t| t.precision())
                .max()
                .unwrap_or(Precision::Year);
            Some(match finest {
                Precision::Year => Modifier::Year,
                Precision::Month => Modifier::YearMonth,
                Precision::Day => Modifier::Date,
                _ => Modifier::DateTime,
            })
        } else {
            None
        };
        if let Some(modifier) = date_modifier.filter(|m| self.applies(*m)) {
            // A date the user typed in another of Canon's spellings (`2024:07:23`) is shown in
            // the one form Canon shows a time (`2024-07-23`); one with an offset stays as
            // typed, since only the user's clock can print it.
            let new_key = format!("{}|{}", self.key.written, <&'static str>::from(modifier));
            let clause = self.with_key_and_values(&new_key, value, value_as_asked);
            ways.push(Way::to("As dates, if its values are dates", clause));
        }
        let mut lines = vec![ways];
        lines.extend(self.pointer(value).map(|p| vec![Way::hint(p)]));
        Err(BindCause::TypeMismatch(self.entry_with(
            format!("{}; {} is {}.", self.subject(), value.text, readings.what()),
            lines,
        )))
    }

    fn read_number(&self, op: CompareOp, value: &Literal) -> Result<Test, BindCause> {
        if op == CompareOp::Glob {
            return Err(self.glob_on_numbers());
        }
        if let Ok(n) = read_number(&value.text) {
            return Ok(Test::Number(n));
        }
        let mut lines = vec![vec![Way::hint("Type a number: 1024, -5, 3.14, 1e20")]];
        if let Some(n) = stored_boolean(&value.text) {
            // Every `true` and `false` of the clause becomes its number, so the way out
            // rewrites the whole clause, as every way out does.
            lines.push(vec![Way::to(
                format!(
                    "{} is stored as {n}",
                    printed_value(&value.text, FactType::Num)
                ),
                self.with_booleans_as_numbers(value),
            )]);
        }
        Err(BindCause::Unreadable(self.entry_with(
            format!(
                "{}; {} is not a number.",
                self.subject(),
                printed_value(&value.text, FactType::Num)
            ),
            lines,
        )))
    }

    fn read_time(&self, op: CompareOp, value: &Literal) -> Result<Test, BindCause> {
        if op == CompareOp::Glob {
            return Err(self.glob_on_times(value));
        }
        let printed = printed_value(&value.text, FactType::Time);
        if self.chain.gives == Gives::TimeOfDay {
            return read_time_of_day(&value.text)
                .map(Test::TimeOfDay)
                .map_err(|_| {
                    BindCause::Unreadable(self.entry(
                        format!("{}; {printed} is not a time of day.", self.subject()),
                        vec!["Type a time of day: 14:30, 14:30:05".to_string()],
                    ))
                });
        }
        match read_time(&value.text) {
            Ok(tv) => match self.chain.gives {
                Gives::TimeAt(grain) if tv.precision() > grain => Err(self.too_fine(grain, value)),
                _ => Ok(Test::Period(tv)),
            },
            Err(error) => {
                let reason = if read_number(&value.text).is_ok() {
                    ": a year is four digits, and a number never means seconds since 1970"
                        .to_string()
                } else {
                    match error {
                        crate::core::domain::time::reading::TimeReadError::NotInList => {
                            String::new()
                        }
                        other => format!(": {other}"),
                    }
                };
                let forms = EXAMPLE_FORMS
                    .iter()
                    .map(|f| printed_value(f, FactType::Time))
                    .collect::<Vec<_>>()
                    .join(", ");
                let mut lines = vec![vec![Way::hint(format!(
                    "Type a year, month, day, minute or second: {forms}"
                ))]];
                // `|time` rewrites the whole clause, so every value must be a time of day.
                let all_times_of_day = self
                    .list
                    .unwrap_or(std::slice::from_ref(value))
                    .iter()
                    .all(|v| read_time_of_day(&v.text).is_ok());
                if all_times_of_day && self.chain.gives == Gives::Time {
                    if let Some(key) = self.key_with(Modifier::Time) {
                        lines.push(vec![Way::to(
                            "A time of day is compared through |time",
                            key,
                        )]);
                    }
                }
                Err(BindCause::Unreadable(self.entry_with(
                    format!("{}; {printed} is not a time{reason}.", self.subject()),
                    lines,
                )))
            }
        }
    }

    fn read_part(
        &self,
        modifier: Modifier,
        op: CompareOp,
        value: &Literal,
    ) -> Result<Test, BindCause> {
        if op == CompareOp::Glob {
            return Err(self.glob_on_numbers());
        }
        let rule = part_rule(modifier);
        let text = value.text.as_str();
        if let Some(at) = rule.words.iter().position(|w| *w == text) {
            return Ok(Test::Part(at as i64 + 1));
        }
        let digits =
            !text.is_empty() && text.len() <= 9 && text.bytes().all(|b| b.is_ascii_digit());
        let four = modifier != Modifier::Year || text.len() == 4;
        if digits && four {
            let n: i64 = text.parse().unwrap_or(-1);
            if (rule.low..=rule.high).contains(&n) {
                return Ok(Test::Part(n));
            }
            return Err(BindCause::OutOfRange(self.entry(
                format!(
                    "|{} gives {}; {text} is outside it.",
                    <&'static str>::from(modifier),
                    rule.range
                ),
                Vec::new(),
            )));
        }
        Err(BindCause::Unreadable(self.entry(
            format!(
                "|{} gives {}; {} is not one.",
                <&'static str>::from(modifier),
                rule.range,
                printed_value(text, FactType::Num)
            ),
            vec![format!("Type {}", rule.forms)],
        )))
    }

    fn read_bucket(&self, args: &[f64], op: CompareOp, value: &Literal) -> Result<Test, BindCause> {
        if op != CompareOp::Eq {
            let itself = self.key_without_last_modifier();
            return Err(BindCause::BucketOrdered(self.entry(
                format!(
                    "a bucket is compared with =, !=, IN or NOT IN; {} gives bucket ranges.",
                    self.key.written
                ),
                vec![format!(
                    "To order the values, compare {itself} itself with >, >=, < or <="
                )],
            )));
        }
        let text = value.text.as_str();
        if args.is_empty() {
            if is_magnitude_bucket_label(text) {
                return Ok(Test::Bucket(text.to_string()));
            }
            return Err(BindCause::NotABucketLabel(self.entry(
                format!(
                    "{} is not a range |bucket gives.",
                    printed_word(text)
                ),
                vec!["Its ranges are powers of ten, as facts prints them: 0, 1-10, 10-100, 1K-10K, 1M-10M".to_string()],
            )));
        }
        let labels = threshold_bucket_labels(args);
        if labels.iter().any(|l| l == text) {
            return Ok(Test::Bucket(text.to_string()));
        }
        let printed: Vec<String> = labels.iter().map(|l| printed_word(l)).collect();
        Err(BindCause::NotABucketLabel(self.entry(
            format!("{} is not one of the bucket's ranges.", printed_word(text)),
            vec![format!("Its ranges: {}", printed.join(", "))],
        )))
    }

    fn glob_on_numbers(&self) -> BindCause {
        BindCause::GlobNotText(self.entry(
            format!("a pattern (~) matches text; {}.", self.subject()),
            vec![format!(
                "To select a range, compare {} with >= and <",
                self.key.written
            )],
        ))
    }

    fn glob_on_times(&self, value: &Literal) -> BindCause {
        let pattern = value.text.as_str();
        let prefix = pattern
            .strip_suffix('*')
            .filter(|p| !p.contains(['*', '?', '[']))
            .map(|p| p.trim_end_matches(['-', ':', 'T', ' ']));
        let grain = prefix.and_then(|p| read_time(p).ok().map(|t| (p, t.precision())));
        let negated = self
            .clause
            .get(self.key.written.len()..)
            .is_some_and(|rest| rest.trim_start().starts_with("!~"));
        let eq = if negated { "!=" } else { "=" };
        let way = match grain {
            Some((p, precision)) if precision <= Precision::Day => {
                let modifier = match precision {
                    Precision::Year => Modifier::Year,
                    Precision::Month => Modifier::YearMonth,
                    _ => Modifier::Date,
                };
                // The date the pattern reached for, reprinted as every way out reprints one.
                let asked = as_asked(p).unwrap_or_else(|| printed_value(p, FactType::Time));
                Way::to(
                    format!("To select by {}", precision.word()),
                    format!("{}{eq}{asked}", self.key_at_grain(modifier)),
                )
            }
            _ if self.chain.gives == Gives::TimeOfDay => {
                Way::hint("Compare a time of day with =, <, >, <= or >=: 14:30, 14:30:05")
            }
            _ => Way::hint(format!(
                "Compare times with =, <, >= at the grain you mean: {}|year, |yearmonth, |date",
                self.key.written
            )),
        };
        BindCause::GlobNotText(self.entry_with(
            format!("a pattern (~) matches text; {}.", self.subject()),
            vec![vec![way]],
        ))
    }

    fn too_fine(&self, grain: Precision, value: &Literal) -> BindCause {
        // The rewrite replaces the modifier for the whole clause, so it is offered only where
        // every value reads as a time; the finest decides which modifier holds them all.
        let values = self.list.unwrap_or(std::slice::from_ref(value));
        let all_times = values.iter().all(|v| read_time(&v.text).is_ok());
        let typed = values
            .iter()
            .filter_map(|v| read_time(&v.text).ok())
            .map(|t| t.precision())
            .max();
        let (modifier, how) = match (grain, typed) {
            (Precision::Month, Some(Precision::Day)) => (Modifier::Date, "To compare by day"),
            _ => (Modifier::DateTime, "To compare at the precision typed"),
        };
        let last: &'static str = self.key.modifiers.last().map_or("", |c| c.modifier.into());
        let rewrite =
            self.with_key_and_values(&self.key_replacing_last(modifier), value, value_as_asked);
        let lines = if all_times {
            vec![vec![Way::to(how, rewrite)]]
        } else {
            Vec::new()
        };
        BindCause::TooFine(self.entry_with(
            format!(
                "|{last} gives {}; {} is finer than a {}.",
                self.chain.gives.words(),
                printed_value(&value.text, FactType::Time),
                grain.word()
            ),
            lines,
        ))
    }

    /// The conversion pointer: only for a stored text key with no accessor and no modifier,
    /// and only where one other type reads every value of the clause, so that after the
    /// conversion the same clause binds. A number reads through the number reading, a time
    /// through the list of spellings; a time of day alone reads as neither.
    ///
    /// Never for a number or time key: a value such a key cannot read points at the value,
    /// never at the key. The conversion leads it nowhere — number to time and time to number
    /// are refused, and as text a number- or time-shaped value is refused again, while a
    /// clause that binds can match no value the key holds. Never for a path: the conversion
    /// has no path type.
    fn pointer(&self, value: &Literal) -> Option<String> {
        let values = self.list.unwrap_or(std::slice::from_ref(value));
        let as_number = values.iter().all(|v| read_number(&v.text).is_ok());
        let as_time = values.iter().all(|v| read_time(&v.text).is_ok());
        let another_type_reads_them = match self.chain.gives {
            Gives::Text => as_number || as_time,
            Gives::Path => false,
            Gives::Number | Gives::Time | Gives::TimeAt(_) | Gives::TimeOfDay => false,
            Gives::Part(_) | Gives::Bucket(_) => false,
        };
        if !another_type_reads_them {
            return None;
        }
        self.chain.convertible.as_ref().map(|full| {
            format!(
                "{} to see what it holds and change its type",
                convert_command(full)
            )
        })
    }

    /// The clause with the key's chain extended by `modifier` — where the table lets it
    /// apply, so the way out binds.
    fn key_with(&self, modifier: Modifier) -> Option<String> {
        self.applies(modifier).then(|| {
            let new_key = format!("{}|{}", self.key.written, <&'static str>::from(modifier));
            rewrite_key(self.clause, self.key, &new_key)
        })
    }

    /// Whether the table lets `modifier` follow the key's chain.
    fn applies(&self, modifier: Modifier) -> bool {
        let call = ModifierCall {
            modifier,
            args: Vec::new(),
        };
        result_of(&self.chain.gives, self.chain.from_modifier, &call).is_some()
    }

    /// The clause with its key replaced by `new_key` and each value written by `value`.
    fn with_key_and_values(
        &self,
        new_key: &str,
        current: &Literal,
        value: impl Fn(&Literal) -> String,
    ) -> String {
        let values_rewritten = match self.list {
            Some(values) => self.with_items(values.iter().map(&value).collect()),
            None => self.with_value(current, &value(current)),
        };
        rewrite_key(&values_rewritten, self.key, new_key)
    }

    /// The key with its last written modifier replaced by `modifier`.
    fn key_replacing_last(&self, modifier: Modifier) -> String {
        format!(
            "{}|{}",
            self.key_without_last_modifier(),
            <&'static str>::from(modifier)
        )
    }

    /// The key for a time at a grain: the last modifier replaced where it reads a time at a
    /// grain (`|date`, `|yearmonth`, `|datetime`), otherwise the modifier added.
    fn key_at_grain(&self, modifier: Modifier) -> String {
        let replace = self.key.modifiers.last().is_some_and(|c| {
            matches!(
                c.modifier,
                Modifier::Date | Modifier::YearMonth | Modifier::DateTime
            )
        });
        if replace {
            self.key_replacing_last(modifier)
        } else {
            format!("{}|{}", self.key.written, <&'static str>::from(modifier))
        }
    }

    /// The key as written without its last written modifier.
    fn key_without_last_modifier(&self) -> String {
        let name_len = self.key.written_name.len();
        let tail = self.key.written.get(name_len..).unwrap_or_default();
        match tail.rfind('|') {
            Some(at) => format!(
                "{}{}",
                self.key.written_name,
                tail.get(..at).unwrap_or_default()
            ),
            None => self.key.written.clone(),
        }
    }

    /// The clause with every bare number- or time-shaped value quoted, so it reads as text —
    /// in an `IN` list every such item, so the way out meets no second mismatch.
    fn with_shaped_values_quoted(&self, value: &Literal) -> String {
        let as_text = |v: &Literal| {
            if v.quoted || bare_class(&v.text) == BareClass::Word {
                v.written.clone()
            } else {
                printed_value(&v.text, FactType::Text)
            }
        };
        match self.list {
            Some(values) => self.with_items(values.iter().map(as_text).collect()),
            None => self.with_value(value, &as_text(value)),
        }
    }

    /// The clause with every `true` and `false` written as the number it is stored as, the
    /// other values as typed.
    fn with_booleans_as_numbers(&self, value: &Literal) -> String {
        let as_number =
            |v: &Literal| stored_boolean(&v.text).map_or_else(|| v.written.clone(), str::to_string);
        match self.list {
            Some(values) => self.with_items(values.iter().map(as_number).collect()),
            None => self.with_value(value, &as_number(value)),
        }
    }

    /// An `IN` clause with its items replaced, the key and operator as typed.
    fn with_items(&self, items: Vec<String>) -> String {
        let after_key = self
            .clause
            .get(self.key.written.len()..)
            .unwrap_or_default();
        let head = after_key
            .find('(')
            .map_or(after_key, |at| after_key.get(..at).unwrap_or_default());
        format!("{}{head}({})", self.key.written, items.join(", "))
    }

    /// The clause with one value replaced (in an `IN` list, that item), the rest as typed.
    fn with_value(&self, value: &Literal, new: &str) -> String {
        match self.list {
            Some(values) => self.with_items(
                values
                    .iter()
                    .map(|v| {
                        if std::ptr::eq(v, value) {
                            new.to_string()
                        } else {
                            v.written.clone()
                        }
                    })
                    .collect(),
            ),
            None => match self.clause.strip_suffix(value.written.as_str()) {
                Some(prefix) => format!("{prefix}{new}"),
                None => format!("{}={new}", self.key.written),
            },
        }
    }
}

/// **How a way out reprints a typed date**: a date or time the user typed in another of
/// Canon's spellings (`2024:07:23`) in the one form Canon shows a time, named back as asked (a
/// typed fraction kept); `None` for text that is no time and for a time with an offset,
/// which only the user's clock can print — each caller keeps those as it has them.
fn as_asked(text: &str) -> Option<String> {
    read_time(text)
        .ok()
        .and_then(|tv| tv.printed_clock_reading_as_asked())
        .map(|printed| printed_value(&printed, FactType::Time))
}

/// A typed value as a way out writes it: reprinted by [`as_asked`], otherwise as typed.
fn value_as_asked(value: &Literal) -> String {
    as_asked(&value.text).unwrap_or_else(|| value.written.clone())
}

/// The clause with its key replaced, the rest as typed.
fn rewrite_key(clause: &str, key: &WrittenKey, new_key: &str) -> String {
    format!(
        "{new_key}{}",
        clause.get(key.written.len()..).unwrap_or_default()
    )
}

/// A number part's range and words.
struct PartRule {
    low: i64,
    high: i64,
    words: &'static [&'static str],
    plural: &'static str,
    range: &'static str,
    forms: &'static str,
}

fn part_rule(modifier: Modifier) -> PartRule {
    let rule = |low, high, plural, range, forms| PartRule {
        low,
        high,
        words: &[],
        plural,
        range,
        forms,
    };
    match modifier {
        Modifier::Year => rule(
            0,
            9999,
            "years",
            "a year, four digits",
            "a year as four digits: 2024",
        ),
        Modifier::Month => rule(
            1,
            12,
            "months",
            "a month from 1 to 12",
            "a month from 1 to 12: 3 or 03",
        ),
        Modifier::Day => rule(
            1,
            31,
            "days of the month",
            "a day from 1 to 31",
            "a day from 1 to 31: 5 or 05",
        ),
        Modifier::Hour => rule(
            0,
            23,
            "hours",
            "an hour from 0 to 23",
            "an hour from 0 to 23: 9 or 09",
        ),
        Modifier::Minute => rule(
            0,
            59,
            "minutes",
            "a minute from 0 to 59",
            "a minute from 0 to 59: 30",
        ),
        Modifier::Second => rule(
            0,
            59,
            "seconds",
            "a second from 0 to 59",
            "a second from 0 to 59: 45",
        ),
        Modifier::Week => rule(
            1,
            53,
            "weeks",
            "a week from 1 to 53",
            "a week from 1 to 53: 30",
        ),
        Modifier::Weekday => PartRule {
            words: &[
                "Monday",
                "Tuesday",
                "Wednesday",
                "Thursday",
                "Friday",
                "Saturday",
                "Sunday",
            ],
            ..rule(
                1,
                7,
                "weekdays",
                "a weekday from 1 to 7, Monday 1",
                "a weekday from 1 to 7, or its name: Monday … Sunday",
            )
        },
        Modifier::Quarter => PartRule {
            words: &["Q1", "Q2", "Q3", "Q4"],
            ..rule(
                1,
                4,
                "quarters",
                "a quarter from 1 to 4",
                "a quarter from 1 to 4, or Q1 … Q4",
            )
        },
        Modifier::Date
        | Modifier::Time
        | Modifier::DateTime
        | Modifier::YearMonth
        | Modifier::Stem
        | Modifier::Ext
        | Modifier::Short
        | Modifier::Lowercase
        | Modifier::Uppercase
        | Modifier::Capitalize
        | Modifier::Bucket
        | Modifier::Number => unreachable!("not a number part"),
    }
}

// ============================================================================
// The refusal
// ============================================================================

/// What sets two ways out apart on one line.
const WAYS_APART: &str = "     ";

/// One way out: what it does, and — where it rewrites — the clause or key it rewrites to.
/// One value, so what is shown and what is checked cannot part.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::expr) struct Way {
    label: String,
    rewrite: Option<String>,
}

impl Way {
    /// A line that names no rewrite: the forms to type, the modifiers that apply.
    fn hint(text: impl Into<String>) -> Way {
        Way {
            label: text.into(),
            rewrite: None,
        }
    }

    /// A rewrite, shown as `<label>: <rewrite>`.
    fn to(label: impl Into<String>, rewrite: String) -> Way {
        Way {
            label: label.into(),
            rewrite: Some(rewrite),
        }
    }

    fn shown(&self) -> String {
        match &self.rewrite {
            Some(rewrite) => format!("{}: {rewrite}", self.label),
            None => self.label.clone(),
        }
    }
}

/// One entry of a refusal: the clause as written, what is wrong, and the lines under it —
/// each line one or more ways, set apart by [`WAYS_APART`].
#[derive(Debug, Clone, PartialEq)]
pub(in crate::expr) struct Entry {
    clause: String,
    statement: String,
    lines: Vec<Vec<Way>>,
}

impl Entry {
    fn new(clause: &str, statement: String, hints: Vec<String>) -> Entry {
        Entry::with_lines(
            clause,
            statement,
            hints.into_iter().map(|h| vec![Way::hint(h)]).collect(),
        )
    }

    fn with_lines(clause: &str, statement: String, lines: Vec<Vec<Way>>) -> Entry {
        Entry {
            clause: clause.to_string(),
            statement,
            lines,
        }
    }

    /// Drop every way out whose rewrite `binds` refuses; a line left with no way goes.
    fn keep_ways_that_bind(&mut self, binds: &dyn Fn(&str) -> bool) {
        for line in &mut self.lines {
            line.retain(|way| way.rewrite.as_deref().is_none_or(binds));
        }
        self.lines.retain(|line| !line.is_empty());
    }

    /// The clauses or keys this entry's ways out rewrite to, as shown.
    #[cfg(test)]
    pub(in crate::expr) fn rewrites(&self) -> Vec<String> {
        self.lines
            .iter()
            .flatten()
            .filter_map(|way| way.rewrite.clone())
            .collect()
    }

    pub(in crate::expr) fn unparsed(written: &str, error: &anyhow::Error) -> Entry {
        Entry::new(written, error.to_string(), Vec::new())
    }

    fn render(&self, indent: &str, out: &mut Vec<String>) {
        out.push(format!("{indent}{}: {}", self.clause, self.statement));
        out.extend(self.lines.iter().map(|line| {
            let shown: Vec<String> = line.iter().map(Way::shown).collect();
            format!("{indent}  {}", shown.join(WAYS_APART))
        }));
    }
}

/// Why a clause cannot run. Every cause but the last two is input Canon cannot read; those
/// two are answers about what a key is.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::expr) enum BindCause {
    Unparsed(Entry),
    UnknownKey(Entry),
    Unreadable(Entry),
    TypeMismatch(Entry),
    OutOfRange(Entry),
    /// A modifier or accessor the key's type cannot take; `fallback` names the ones that
    /// apply, shown where the entry's own way out would be refused again.
    WrongModifier {
        entry: Entry,
        fallback: String,
    },
    GlobNotText(Entry),
    NotABucketLabel(Entry),
    BucketOrdered(Entry),
    TooFine(Entry),
    /// A key holding two types, read by a clause; `key` is the full stored key.
    MixedType {
        entry: Entry,
        key: String,
    },
    /// A value of a type binding did not bind the key for, met while the filter ran.
    /// `had_no_facts`: the key had none when it was bound, so it gained its first.
    TypeChanged {
        key: String,
        had_no_facts: bool,
    },
}

impl BindCause {
    /// Whether the cause is an answer about what a key is, rather than input Canon cannot
    /// read.
    pub(in crate::expr) fn is_answer(&self) -> bool {
        match self {
            BindCause::MixedType { .. } | BindCause::TypeChanged { .. } => true,
            BindCause::Unparsed(_)
            | BindCause::UnknownKey(_)
            | BindCause::Unreadable(_)
            | BindCause::TypeMismatch(_)
            | BindCause::OutOfRange(_)
            | BindCause::WrongModifier { .. }
            | BindCause::GlobNotText(_)
            | BindCause::NotABucketLabel(_)
            | BindCause::BucketOrdered(_)
            | BindCause::TooFine(_) => false,
        }
    }

    fn entry(&self) -> Option<&Entry> {
        match self {
            BindCause::Unparsed(entry)
            | BindCause::UnknownKey(entry)
            | BindCause::Unreadable(entry)
            | BindCause::TypeMismatch(entry)
            | BindCause::OutOfRange(entry)
            | BindCause::WrongModifier { entry, .. }
            | BindCause::GlobNotText(entry)
            | BindCause::NotABucketLabel(entry)
            | BindCause::BucketOrdered(entry)
            | BindCause::TooFine(entry)
            | BindCause::MixedType { entry, .. } => Some(entry),
            BindCause::TypeChanged { .. } => None,
        }
    }

    fn entry_mut(&mut self) -> Option<&mut Entry> {
        match self {
            BindCause::Unparsed(entry)
            | BindCause::UnknownKey(entry)
            | BindCause::Unreadable(entry)
            | BindCause::TypeMismatch(entry)
            | BindCause::OutOfRange(entry)
            | BindCause::WrongModifier { entry, .. }
            | BindCause::GlobNotText(entry)
            | BindCause::NotABucketLabel(entry)
            | BindCause::BucketOrdered(entry)
            | BindCause::TooFine(entry)
            | BindCause::MixedType { entry, .. } => Some(entry),
            BindCause::TypeChanged { .. } => None,
        }
    }

    fn render(&self, indent: &str, out: &mut Vec<String>) {
        match self {
            BindCause::Unparsed(entry)
            | BindCause::UnknownKey(entry)
            | BindCause::Unreadable(entry)
            | BindCause::TypeMismatch(entry)
            | BindCause::OutOfRange(entry)
            | BindCause::WrongModifier { entry, .. }
            | BindCause::GlobNotText(entry)
            | BindCause::NotABucketLabel(entry)
            | BindCause::BucketOrdered(entry)
            | BindCause::TooFine(entry)
            | BindCause::MixedType { entry, .. } => entry.render(indent, out),
            BindCause::TypeChanged { key, had_no_facts } => out.push(if *had_no_facts {
                format!(
                    "{indent}{} gained facts while this ran; run it again.",
                    printed_key(key)
                )
            } else {
                format!(
                    "{indent}{}'s type changed while this ran; run it again.",
                    printed_key(key)
                )
            }),
        }
    }
}

/// What a refusal refuses: a filter (`--where`), or a key (`facts --key`, `--group-by`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub(in crate::expr) enum Refused {
    Filter,
    Key,
}

/// **A filter Canon will not run**, and every reason, before anything is selected. Pure data
/// with one `Display`: with any input Canon cannot read, a header line then one entry per
/// cause (the caller prefixes `Error: `); with answers only, the entries as they stand. Keys
/// of two types alone share one way-out block at the end; beside other causes, each key's way
/// out sits under its own entry.
#[derive(Debug, Clone)]
pub struct FilterRefused {
    pub(in crate::expr) refused: Refused,
    pub(in crate::expr) causes: Vec<BindCause>,
}

impl FilterRefused {
    /// Whether every cause is an answer — what a key is — so the refusal prints without
    /// `Error:`.
    pub fn is_answer(&self) -> bool {
        self.causes.iter().all(BindCause::is_answer)
    }

    /// Each cause as the clause it names and what is wrong with it, in the order they stand —
    /// for a surface that lists causes in its own words (a manifest's recorded query).
    pub fn entries(&self) -> Vec<(String, String)> {
        self.causes
            .iter()
            .map(|cause| {
                let mut lines = Vec::new();
                cause.render("", &mut lines);
                let first = lines.first().cloned().unwrap_or_default();
                match cause.entry() {
                    Some(entry) => (entry.clause.clone(), entry.statement.clone()),
                    None => (String::new(), first),
                }
            })
            .collect()
    }
}

impl fmt::Display for FilterRefused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut lines = Vec::new();
        let indent = if self.is_answer() {
            ""
        } else {
            lines.push(match self.refused {
                Refused::Filter => "cannot run the filter as written".to_string(),
                Refused::Key => "cannot use the key as written".to_string(),
            });
            "  "
        };
        // Keys of two types alone share one way-out block at the end; beside any other cause,
        // each one's way out sits under its own entry, so none reads as the entry's above it.
        let only_two_types = self
            .causes
            .iter()
            .all(|cause| matches!(cause, BindCause::MixedType { .. }));
        let mut keys: Vec<&str> = Vec::new();
        for cause in &self.causes {
            cause.render(indent, &mut lines);
            if let BindCause::MixedType { key, .. } = cause {
                if only_two_types {
                    if !keys.contains(&key.as_str()) {
                        keys.push(key);
                    }
                } else {
                    lines.extend(
                        mixed_type_way_out(&[key])
                            .into_iter()
                            .map(|l| format!("{indent}  {l}")),
                    );
                }
            }
        }
        lines.extend(
            mixed_type_way_out(&keys)
                .into_iter()
                .map(|l| format!("{indent}{l}")),
        );
        f.write_str(&lines.join("\n"))
    }
}

impl std::error::Error for FilterRefused {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::domain::time::value::TimeForm;

    /// The key types the tests bind against: one key per type a refusal names, and one of two
    /// types.
    fn types() -> HashMap<String, KeyTypes> {
        let one = |t| KeyTypes::from_counts([(t, 3)]).unwrap();
        HashMap::from([
            ("content.taken".to_string(), one(FactType::Time)),
            ("content.numtext".to_string(), one(FactType::Text)),
            ("content.textdate".to_string(), one(FactType::Text)),
            ("content.yeartext".to_string(), one(FactType::Text)),
            ("content.Title".to_string(), one(FactType::Text)),
            ("content.SourceFile".to_string(), one(FactType::Text)),
            ("content.FNumber".to_string(), one(FactType::Num)),
            (
                "content.File Size".to_string(),
                KeyTypes::from_counts([(FactType::Num, 41_220), (FactType::Text, 380)]).unwrap(),
            ),
            (
                "content.Width".to_string(),
                KeyTypes::from_counts([(FactType::Num, 2), (FactType::Time, 1)]).unwrap(),
            ),
        ])
    }

    fn bind(text: &str) -> Result<Bound, Vec<BindCause>> {
        let expr = match Expr::parse(text) {
            Ok(expr) => expr,
            Err(e) => return Err(vec![BindCause::Unparsed(Entry::unparsed(text, &e))]),
        };
        let mut causes = Vec::new();
        let bound = bind_expr(&expr, &types(), &mut causes);
        if causes.is_empty() {
            Ok(bound)
        } else {
            Err(causes)
        }
    }

    fn causes(text: &str) -> Vec<BindCause> {
        bind(text).err().unwrap_or_else(|| panic!("{text} bound"))
    }

    fn test_of(text: &str) -> Test {
        match bind(text).unwrap_or_else(|c| panic!("{text}: {c:?}")) {
            Bound::Compare { test, .. } => test,
            other => panic!("{text}: {other:?}"),
        }
    }

    fn refusal(text: &str) -> String {
        FilterRefused {
            refused: Refused::Filter,
            causes: causes(text),
        }
        .to_string()
    }

    // ------------------------------------------------------------------
    // The bare-value classes
    // ------------------------------------------------------------------

    #[test]
    fn a_bare_value_is_number_shaped_time_shaped_or_a_word() {
        use BareClass::*;
        let cases = [
            ("1024", NumberShaped),
            ("-5", NumberShaped),
            ("3.14", NumberShaped),
            ("1e20", NumberShaped),
            (".5", NumberShaped),
            ("5.", NumberShaped),
            ("+5", NumberShaped),
            ("1e", NumberShaped),
            ("1-2", NumberShaped),
            ("18:00", TimeShaped),
            ("14:30:05", TimeShaped),
            ("2024:07:23", TimeShaped),
            ("25:99", TimeShaped),
            ("2024-06", NumberShaped),
            ("2024", NumberShaped),
            ("2024-06-15", NumberShaped),
            ("2024-06-15T14:30", TimeShaped),
            ("2024-06-15T14:30:05.45+02:00", TimeShaped),
            ("2023-12-31-05:00", TimeShaped),
            ("2024-06-15T14:30+0200", TimeShaped),
            ("E", Word),
            ("E-5", Word),
            ("e5", Word),
            ("3gp", Word),
            ("7z", Word),
            ("nan", Word),
            ("20240615_x.jpg", Word),
            ("jpg", Word),
        ];
        for (text, class) in cases {
            assert_eq!(bare_class(text), class, "{text}");
        }
    }

    #[test]
    fn against_text_a_shaped_value_is_the_type_mismatch_or_unreadable_and_a_word_is_text() {
        for text in [
            "numtext<10",
            "source.rel_path[0]=2019",
            "stem=18:00",
            "textdate>=2024-06",
            "textdate=2024:07:23",
            "source.rel_path[0]=2024-06-15T14:30",
            "content.hash.sha256|short=12345e67",
            "Title=2024-06-15T14:30:05.45+02:00",
        ] {
            assert!(
                matches!(causes(text).as_slice(), [BindCause::TypeMismatch(_)]),
                "{text}: {:?}",
                causes(text)
            );
        }
        for text in [
            "numtext=.5",
            "numtext=1-2",
            "stem=5.",
            "Title=2023-12-31-05:00",
            "Title=25:99",
        ] {
            assert!(
                matches!(causes(text).as_slice(), [BindCause::Unreadable(_)]),
                "{text}"
            );
        }
        for (text, word) in [("ext=E-5", "E-5"), ("ext=3gp", "3gp"), ("Title=nan", "nan")] {
            assert_eq!(test_of(text), Test::Text(word.into()), "{text}");
        }
        // Quoted, every value is text; the operand of `~` is a pattern whatever its spelling.
        assert_eq!(test_of(r#"numtext="10""#), Test::Text("10".into()));
        assert_eq!(test_of("numtext~1*"), Test::Glob("1*".into()));
    }

    // ------------------------------------------------------------------
    // Number and time keys: the key decides, quotes change nothing
    // ------------------------------------------------------------------

    #[test]
    fn against_a_number_or_time_key_quotes_change_nothing() {
        assert_eq!(
            test_of("source.size=1024"),
            test_of(r#"source.size="1024""#)
        );
        assert_eq!(test_of("source.size>=1e6"), Test::Number(1e6));
        assert_eq!(test_of("taken=2024"), test_of(r#"taken="2024""#));
        let Test::Period(tv) = test_of(r#"taken="2024-06-15 14:30""#) else {
            panic!()
        };
        assert_eq!(tv.precision(), Precision::Minute);
    }

    #[test]
    fn a_typed_fraction_binds_as_that_point() {
        let Test::Period(tv) = test_of(r#"taken="2024-06-15 14:30:05.45""#) else {
            panic!()
        };
        assert_eq!(tv.precision(), Precision::Fraction);
        let Test::Period(tv) = test_of("taken=2024-06-15T14:30:05.45+02:00") else {
            panic!()
        };
        assert_eq!(tv.form(), TimeForm::InstantWithOffset);
    }

    #[test]
    fn a_part_reads_its_range_and_its_own_words() {
        assert_eq!(
            test_of("source.mtime|month=3"),
            test_of("source.mtime|month=03")
        );
        assert_eq!(
            test_of("source.mtime|weekday=1"),
            test_of("source.mtime|weekday=Monday")
        );
        assert_eq!(test_of("source.mtime|weekday=Sunday"), Test::Part(7));
        assert_eq!(test_of("source.mtime|quarter=Q3"), Test::Part(3));
        assert_eq!(test_of("source.mtime|year=2024"), Test::Part(2024));
        // Words are read as printed, case included, as `=` reads text.
        assert!(matches!(
            causes("source.mtime|weekday=monday").as_slice(),
            [BindCause::Unreadable(_)]
        ));
        for text in [
            "source.mtime|month=13",
            "source.mtime|weekday=0",
            "source.mtime|hour=24",
        ] {
            assert!(
                matches!(causes(text).as_slice(), [BindCause::OutOfRange(_)]),
                "{text}"
            );
        }
        // A year is four digits.
        for text in ["source.mtime|year=24", "source.mtime|year=20240"] {
            assert!(
                matches!(causes(text).as_slice(), [BindCause::Unreadable(_)]),
                "{text}"
            );
        }
        // A part orders as a whole number.
        assert_eq!(test_of("source.mtime|year>=2020"), Test::Part(2020));
    }

    #[test]
    fn a_time_of_day_reads_through_time() {
        let Test::TimeOfDay(tv) = test_of("source.mtime|time>=18:00") else {
            panic!()
        };
        assert_eq!(tv.precision(), Precision::Minute);
        assert!(matches!(
            causes("source.mtime|time>=6pm").as_slice(),
            [BindCause::Unreadable(_)]
        ));
    }

    #[test]
    fn a_bucket_reads_its_own_labels_and_only_equality() {
        assert_eq!(
            test_of("source.size|bucket(60,300)=60-300"),
            Test::Bucket("60-300".into())
        );
        assert_eq!(
            test_of(r#"source.size|bucket(60,300)="<60""#),
            Test::Bucket("<60".into())
        );
        assert_eq!(
            test_of("source.size|bucket=1K-10K"),
            Test::Bucket("1K-10K".into())
        );
        assert!(matches!(
            causes("source.size|bucket(60,300)=61-299").as_slice(),
            [BindCause::NotABucketLabel(_)]
        ));
        assert!(matches!(
            causes("source.size|bucket=2K-20K").as_slice(),
            [BindCause::NotABucketLabel(_)]
        ));
        assert!(matches!(
            causes("source.size|bucket(60,300)>60-300").as_slice(),
            [BindCause::BucketOrdered(_)]
        ));
    }

    // ------------------------------------------------------------------
    // The modifier table
    // ------------------------------------------------------------------

    /// Every modifier against every input, written out from the rule as stated rather
    /// than read off `result_of`: `T` time, `X` text, `N` number, `P` path, `R` a modifier's
    /// result that is a number (`|month`), `O` a time of day (`|time`).
    #[test]
    fn the_modifier_table_says_what_each_modifier_reads_and_gives() {
        use Modifier::*;
        let time_parts = [
            Year, Month, Day, Hour, Minute, Second, Week, Weekday, Quarter,
        ];
        let text_modifiers = [Stem, Ext, Short, Lowercase, Uppercase, Capitalize];
        let call = |m| ModifierCall {
            modifier: m,
            args: Vec::new(),
        };
        let inputs = [
            ("T", Gives::Time, false),
            ("X", Gives::Text, false),
            ("N", Gives::Number, false),
            ("P", Gives::Path, false),
            ("R", Gives::Part(Month), true),
            ("O", Gives::TimeOfDay, true),
        ];
        for m in Modifier::iter() {
            for (name, input, from_modifier) in &inputs {
                // A time of day (`O`) carries an hour, a minute and a second, never a date.
                let expected = if [Hour, Minute, Second].contains(&m) {
                    matches!(*name, "T" | "X" | "O").then_some(Gives::Part(m))
                } else if time_parts.contains(&m) {
                    matches!(*name, "T" | "X").then_some(Gives::Part(m))
                } else if text_modifiers.contains(&m) {
                    matches!(*name, "X" | "P" | "R" | "O").then_some(Gives::Text)
                } else {
                    match (m, *name) {
                        (Date, "T" | "X") => Some(Gives::TimeAt(Precision::Day)),
                        (YearMonth, "T" | "X") => Some(Gives::TimeAt(Precision::Month)),
                        (DateTime, "T" | "X") => Some(Gives::Time),
                        (Time, "T" | "X" | "O") => Some(Gives::TimeOfDay),
                        (Number, "X" | "N" | "R") => Some(Gives::Number),
                        (Bucket, "N" | "R") => Some(Gives::Bucket(Vec::new())),
                        _ => None,
                    }
                };
                assert_eq!(
                    result_of(input, *from_modifier, &call(m)),
                    expected,
                    "|{} on {name}",
                    <&'static str>::from(m)
                );
            }
        }
    }

    /// The binding table and the chain read one rule for a time of day: after `|time`, every
    /// modifier the table refuses is an error in the chain, and every one it accepts runs —
    /// so a pattern, which is not bound, can never place a date read off the reference day.
    #[test]
    fn the_table_and_the_chain_agree_on_a_time_of_day() {
        use crate::core::domain::fact::FactValue;
        use crate::core::domain::time::zone::UserZone;
        use crate::expr::domain::transform::apply_chain;
        let saved = FactValue::Time(TimeValue::from_epoch_secs(1_718_461_845).unwrap());
        for m in Modifier::iter() {
            let calls = [
                ModifierCall {
                    modifier: Modifier::Time,
                    args: Vec::new(),
                },
                ModifierCall {
                    modifier: m,
                    args: Vec::new(),
                },
            ];
            let table = result_of(&Gives::TimeOfDay, true, &calls[1]).is_some();
            let chain =
                apply_chain(saved.clone(), None, &calls, "k", true, &UserZone::utc()).is_ok();
            assert_eq!(table, chain, "|time|{}", <&'static str>::from(m));
        }
    }

    #[test]
    fn a_wrong_type_modifier_is_refused_naming_what_applies() {
        let text = refusal("source.size|year=2024");
        assert!(
            text.contains("Modifiers for numbers: |bucket, |number"),
            "{text}"
        );
        // A text modifier directly on a stored number or time names the printed form.
        let text = refusal("FNumber|lowercase=x");
        assert!(text.contains("FNumber|number|lowercase=x"), "{text}");
        let text = refusal("source.mtime|lowercase=x");
        assert!(text.contains("source.mtime|datetime|lowercase=x"), "{text}");
        // `|number` on a whole path names a segment.
        let text = refusal("source.rel_path|number>=2019");
        assert!(text.contains("source.rel_path[0]|number>=2019"), "{text}");
        // A text modifier after a modifier reads its printed form, and accessors read
        // text as path segments.
        assert_eq!(
            test_of("source.mtime|weekday|lowercase=monday"),
            Test::Text("monday".into())
        );
        assert_eq!(test_of("SourceFile[-1]=x.jpg"), Test::Text("x.jpg".into()));
        assert!(bind("FNumber|number|lowercase=x").is_ok());
        assert!(bind(r#"source.mtime|datetime|lowercase~"2024-06*""#).is_ok());
        // A time of day has no date: a date-reading modifier after `|time` is refused, an hour
        // is read.
        for text in [
            "source.mtime|time|year=2000",
            "source.mtime|time|weekday=Saturday",
            "source.mtime|time|date=2000-01-01",
            "source.mtime|time|datetime>=2000",
        ] {
            assert!(
                matches!(causes(text).as_slice(), [BindCause::WrongModifier { .. }]),
                "{text}"
            );
            // The statement says what is missing, never "works on times": a time of day is one.
            let name = text
                .split('|')
                .nth(2)
                .unwrap()
                .split(['=', '>'])
                .next()
                .unwrap();
            let said = refusal(text);
            assert!(
                said.contains(&format!(
                    "|{name} needs a date; |time gives a time of day, which has none."
                )),
                "{said}"
            );
        }
        assert_eq!(test_of("source.mtime|time|hour=14"), Test::Part(14));
        // An accessor on a number is refused.
        assert!(matches!(
            causes("source.size[0]=1").as_slice(),
            [BindCause::WrongModifier { .. }]
        ));
    }

    // ------------------------------------------------------------------
    // A typed period finer than a fixed-grain modifier
    // ------------------------------------------------------------------

    #[test]
    fn a_period_finer_than_a_fixed_grain_is_refused_under_every_operator() {
        for (text, way_out) in [
            ("taken|yearmonth=2024-06-15", "taken|date=2024-06-15"),
            ("taken|yearmonth>=2024-06-15", "taken|date>=2024-06-15"),
            (
                r#"taken|date="2024-06-15 14:30""#,
                r#"taken|datetime="2024-06-15 14:30""#,
            ),
            (
                r#"textdate|date="2024-06-15 14:30""#,
                r#"textdate|datetime="2024-06-15 14:30""#,
            ),
            (
                r#"source.rel_path[0]|date>="2024-06-15 14:30""#,
                r#"source.rel_path[0]|datetime>="2024-06-15 14:30""#,
            ),
            (
                "taken|yearmonth IN (2024-06, 2024-07-15)",
                "taken|date IN (2024-06, 2024-07-15)",
            ),
            // A date typed in another of Canon's spellings is shown in Canon's, as asked.
            ("taken|yearmonth=2024:06:15", "taken|date=2024-06-15"),
            (
                "taken|date=2024:07:23T14:30:05.25",
                r#"taken|datetime="2024-07-23 14:30:05.25""#,
            ),
        ] {
            match causes(text).as_slice() {
                [BindCause::TooFine(entry)] => {
                    assert_eq!(entry.rewrites(), [way_out.to_string()], "{text}")
                }
                other => panic!("{text}: {other:?}"),
            }
        }
        // Coarser than the grain is no refusal: `|date=2024` is every day of 2024.
        assert!(bind("taken|date=2024").is_ok());
    }

    // ------------------------------------------------------------------
    // Globs against numbers and times
    // ------------------------------------------------------------------

    #[test]
    fn a_glob_against_a_number_or_time_names_the_question_it_asked() {
        let entry = |text| match causes(text).as_slice() {
            [BindCause::GlobNotText(entry)] => entry.clone(),
            other => panic!("{text}: {other:?}"),
        };
        assert_eq!(
            entry("source.mtime|date~2024-06*").rewrites(),
            ["source.mtime|yearmonth=2024-06".to_string()]
        );
        assert_eq!(
            entry("source.mtime~2026:10:11*").rewrites(),
            ["source.mtime|date=2026-10-11".to_string()]
        );
        assert_eq!(
            entry("source.mtime!~2024*").rewrites(),
            ["source.mtime|year!=2024".to_string()]
        );
        assert!(refusal("source.size~1*").contains("compare source.size with >= and <"));
    }

    // ------------------------------------------------------------------
    // The pin: every way out binds
    // ------------------------------------------------------------------

    /// Following a way out never meets a second refusal: every clause a refusal rewrites
    /// to binds against the same keys.
    #[test]
    fn every_way_out_binds() {
        let refused = [
            "numtext<10",
            "numtext>=10",
            "yeartext=2020",
            "source.rel_path[0]=2019",
            "stem=18:00",
            "textdate>=2024-06",
            "textdate=2024:07:23",
            "textdate!=2024",
            "numtext=.5",
            "numtext IN (9, jpg, 10)",
            "numtext IN (2024, 2025)",
            "textdate IN (2024-06, 2024-07-15)",
            "Title=2024-06-15T14:30:05.45+02:00",
            "source.rel_path[0]=2024-06-15T14:30",
            "content.hash.sha256|short=12345e67",
            "source.mtime|weekday|lowercase=1",
            "source.size=true",
            "source.size=false",
            "source.mtime=18:00",
            "source.mtime IN (18:00, 19:30)",
            "taken IN (2024, 12:30)",
            "taken|yearmonth=2024-06-15",
            r#"taken|date="2024-06-15 14:30""#,
            r#"textdate|date="2024-06-15 14:30""#,
            "taken|yearmonth IN (2024-06, 2024-07-15)",
            r#"taken|date IN (2024-06-15, "2024-06-16 10:00")"#,
            "source.mtime|date~2024-06*",
            "source.mtime~2024-06-15*",
            "source.mtime!~2024*",
            "FNumber|lowercase=x",
            "source.mtime|lowercase=x",
            "source.rel_path|number>=2019",
            "source.rel_path[1:3]|number>=2019",
        ];
        let mut rewrites = 0;
        for text in refused {
            let found = causes(text);
            assert!(!found.is_empty(), "{text} bound");
            for cause in &found {
                for way in cause.entry().map(Entry::rewrites).unwrap_or_default() {
                    rewrites += 1;
                    if let Err(again) = bind(&way) {
                        panic!("{text} → way out {way} is refused again: {again:?}");
                    }
                }
            }
        }
        assert!(
            rewrites >= refused.len(),
            "only {rewrites} ways out were checked"
        );
    }

    // ------------------------------------------------------------------
    // Keys: unknown, two types
    // ------------------------------------------------------------------

    #[test]
    fn a_key_with_no_facts_binds_only_for_existence() {
        assert!(matches!(
            causes("content.nothing=1").as_slice(),
            [BindCause::UnknownKey(_)]
        ));
        assert!(bind("content.nothing?").is_ok());
        assert!(bind("NOT content.nothing?").is_ok());
    }

    #[test]
    fn a_two_type_key_refuses_every_clause_that_reads_its_value_as_an_answer() {
        for text in [
            r#""content.File Size">1000"#,
            r#""content.File Size"~1*"#,
            r#""content.File Size"|number>=1"#,
            r#""content.File Size"|bucket?"#,
        ] {
            match causes(text).as_slice() {
                [cause @ BindCause::MixedType { .. }] => assert!(cause.is_answer(), "{text}"),
                other => panic!("{text}: {other:?}"),
            }
        }
        assert!(bind(r#""content.File Size"?"#).is_ok());
        assert!(bind(r#"NOT "content.File Size"?"#).is_ok());
    }

    // ------------------------------------------------------------------
    // The refusal's shape
    // ------------------------------------------------------------------

    #[test]
    fn the_refusal_carries_its_header_only_with_input_canon_cannot_read() {
        let answer = refusal(r#""content.File Size">1000"#);
        assert_eq!(
            answer,
            [
                r#""content.File Size">1000: "content.File Size" holds numbers and text; a filter compares one type only."#,
                "  numbers: 41,220 facts   text: 380 facts   (every fact of the key, on every root)",
                "To inspect it and learn how to convert it to one type:",
                r#"  `canon facts convert '"content.File Size"'`"#,
            ]
            .join("\n")
        );
        let mismatch = refusal("numtext<10");
        assert_eq!(
            mismatch,
            [
                "cannot run the filter as written",
                "  numtext<10: numtext holds text; 10 is a number.",
                r#"    As text: numtext<"10"     As numbers, if its values are numbers: numtext|number<10"#,
                "    `canon facts convert content.numtext` to see what it holds and change its type",
            ]
            .join("\n")
        );
    }

    #[test]
    fn several_two_type_keys_share_one_way_out_block() {
        let refused = FilterRefused {
            refused: Refused::Filter,
            causes: [
                causes(r#""content.File Size">1"#),
                causes("Width>1"),
                causes(r#""content.File Size"<9"#),
            ]
            .concat(),
        };
        assert!(refused.is_answer());
        let text = refused.to_string();
        assert_eq!(text.matches("To inspect each").count(), 1, "{text}");
        assert_eq!(text.matches("canon facts convert").count(), 2, "{text}");
        assert!(
            text.contains("`canon facts convert content.Width`"),
            "{text}"
        );
        // An answer beside input Canon cannot read carries the header.
        let mixed = FilterRefused {
            refused: Refused::Filter,
            causes: [causes("Width>1"), causes("source.size=x")].concat(),
        };
        assert!(!mixed.is_answer());
        assert!(mixed
            .to_string()
            .starts_with("cannot run the filter as written\n"));
    }

    /// Beside other causes, a two-type key's way out sits under its own entry, at the indent
    /// every way out uses — never at a new entry's, where it would read as the entry's above it.
    /// It fails if the block is shared at the end, as when the key stands alone.
    #[test]
    fn a_two_type_way_out_sits_under_its_own_entry_beside_other_causes() {
        assert_eq!(
            refusal(r#"FNumber=garbage AND "content.File Size"=2 AND numtext<10"#),
            [
                "cannot run the filter as written",
                r#"  FNumber=garbage: FNumber holds numbers; "garbage" is not a number."#,
                "    Type a number: 1024, -5, 3.14, 1e20",
                r#"  "content.File Size"=2: "content.File Size" holds numbers and text; a filter compares one type only."#,
                "    numbers: 41,220 facts   text: 380 facts   (every fact of the key, on every root)",
                "    To inspect it and learn how to convert it to one type:",
                r#"      `canon facts convert '"content.File Size"'`"#,
                "  numtext<10: numtext holds text; 10 is a number.",
                r#"    As text: numtext<"10"     As numbers, if its values are numbers: numtext|number<10"#,
                "    `canon facts convert content.numtext` to see what it holds and change its type",
            ]
            .join("\n")
        );
        // Two two-type keys beside another cause: each way out under its own entry, none at
        // the end.
        let text = refusal(r#""content.File Size">1 AND source.size=x AND Width>1"#);
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(
            lines.last(),
            Some(&"      `canon facts convert content.Width`"),
            "{text}"
        );
        assert_eq!(text.matches("    To inspect it").count(), 2, "{text}");
        assert!(!text.contains("To inspect each"), "{text}");
        let size = lines
            .iter()
            .position(|l| l.starts_with("  source.size=x"))
            .unwrap();
        assert_eq!(
            lines[size - 1],
            r#"      `canon facts convert '"content.File Size"'`"#,
            "{text}"
        );
        // One key read by two clauses beside another cause: each clause's entry carries its
        // own way out, as each conversion pointer does; alone, the shared block names it once.
        let twice = refusal("Width=1 AND Width=2 AND source.size=x");
        assert_eq!(twice.matches("    To inspect it").count(), 2, "{twice}");
        let alone = refusal("Width=1 AND Width=2");
        assert_eq!(alone.matches("canon facts convert").count(), 1, "{alone}");
    }

    /// A way out keeps the user's question and their value: a date typed in another of
    /// Canon's spellings is shown as Canon writes it; a hash is offered only quoting; a word
    /// the user typed is printed one way inside one entry.
    #[test]
    fn a_way_out_reprints_only_in_canons_own_spellings() {
        let rewrites = |text: &str| -> Vec<String> {
            causes(text)
                .iter()
                .flat_map(|c| c.entry().map(Entry::rewrites).unwrap_or_default().to_vec())
                .collect()
        };
        assert_eq!(
            rewrites("textdate=2024:07:23"),
            [r#"textdate="2024:07:23""#, "textdate|date=2024-07-23"]
        );
        assert_eq!(
            rewrites("textdate=2024:07:23T14:30:05")[1],
            r#"textdate|datetime="2024-07-23 14:30:05""#
        );
        assert_eq!(
            rewrites("textdate=2024:07:23T14:30:05.25")[1],
            r#"textdate|datetime="2024-07-23 14:30:05.25""#,
            "a typed fraction is kept: the way out asks the same question"
        );
        assert_eq!(
            rewrites("textdate IN (2024:07:23T14:30:05.25, 2024:07:24)")[1],
            r#"textdate|datetime IN ("2024-07-23 14:30:05.25", 2024-07-24)"#
        );
        assert_eq!(
            rewrites("textdate=2024-07-23T14:30+02:00")[1],
            "textdate|datetime=2024-07-23T14:30+02:00",
            "a time with an offset stays as typed"
        );
        assert_eq!(
            rewrites("content.hash.sha256|short=12345e67"),
            [r#"content.hash.sha256|short="12345e67""#]
        );
        let text = refusal("source.size=true");
        assert!(text.contains(r#""true" is not a number"#), "{text}");
        assert!(
            text.contains(r#""true" is stored as 1: source.size=1"#),
            "{text}"
        );
    }

    #[test]
    fn the_conversion_pointer_is_only_for_a_stored_key_with_no_transforms() {
        // Offered on a text key where one other type reads every value: after the conversion
        // the clause binds.
        for text in [
            "numtext<10",
            "numtext=2024-06-15",
            "numtext IN (2019, 2024-06)",
            "textdate>=2024-06",
            "yeartext=2020",
        ] {
            assert!(refusal(text).contains("canon facts convert"), "{text}");
        }
        // Never on a number or time key: a value it cannot read points at the value.
        for text in [
            "FNumber=big",
            "FNumber>=garbage",
            r#"FNumber="12:30""#,
            "FNumber=2024-06-15",
            "FNumber=2024-06",
            "FNumber IN (2.8, garbage)",
            "FNumber=true",
            "taken=big",
            "taken=yesterday",
            "taken=2024-13",
            "taken=10",
            "taken=24",
            "taken=1704067200",
        ] {
            assert!(!refusal(text).contains("canon facts convert"), "{text}");
        }
        // Not where no one type reads them all: a time of day alone reads as no type, and a
        // number beside a month is neither a number nor a time throughout.
        for text in [
            "numtext=12:30",
            "numtext IN (2024, 12:30)",
            "numtext IN (10, 2024-06)",
            "FNumber=12:30",
            "taken=12:30",
            "taken IN (big, 10)",
            "numtext|lowercase<10",
            "source.rel_path[0]=2019",
            "stem=18:00",
            "content.hash.sha256|short=12345e67",
            "content.hash.sha256=123",
        ] {
            assert!(!refusal(text).contains("canon facts convert"), "{text}");
        }
    }

    /// The pointer's rule, asked of binding itself: on a stored key of each type, the conversion
    /// is offered exactly where converting the key to a type it leads to makes the same clause
    /// bind — so following it never meets this refusal again. From text it leads to numbers or
    /// times. From numbers or times it leads nowhere useful: number to time and time to number
    /// are refused by the conversion, and as text a clause that binds can match no stored value.
    /// It fails if the pointer is offered wherever text reads every value.
    #[test]
    fn the_conversion_pointer_is_offered_exactly_where_a_converted_key_binds() {
        let leads_to = |t: FactType| -> &'static [FactType] {
            match t {
                FactType::Text => &[FactType::Num, FactType::Time],
                FactType::Num | FactType::Time | FactType::Path => &[],
            }
        };
        let binds_as = |clause: &str, t: FactType| {
            let types = HashMap::from([(
                "content.k".to_string(),
                KeyTypes::from_counts([(t, 1)]).unwrap(),
            )]);
            let expr = Expr::parse(clause).unwrap();
            let mut causes = Vec::new();
            bind_expr(&expr, &types, &mut causes);
            causes.is_empty()
        };
        let values = [
            "10",
            "2024",
            "2024-06",
            "2024-06-15",
            "12:30",
            "big",
            r#""12:30""#,
            "1e",
            "2024-06-15T14:30",
            "true",
        ];
        let mut clauses: Vec<String> = values.iter().map(|v| format!("k={v}")).collect();
        for a in values {
            for b in values {
                clauses.push(format!("k IN ({a}, {b})"));
            }
        }
        let all = [
            FactType::Text,
            FactType::Num,
            FactType::Time,
            FactType::Path,
        ];
        for t in all {
            for clause in &clauses {
                if binds_as(clause, t) {
                    continue;
                }
                let types = HashMap::from([(
                    "content.k".to_string(),
                    KeyTypes::from_counts([(t, 1)]).unwrap(),
                )]);
                let mut causes = Vec::new();
                bind_expr(&Expr::parse(clause).unwrap(), &types, &mut causes);
                let offered = FilterRefused {
                    refused: Refused::Filter,
                    causes,
                }
                .to_string()
                .contains("canon facts convert");
                let helps = leads_to(t).iter().any(|&other| binds_as(clause, other));
                assert_eq!(offered, helps, "{clause} on a {t} key");
            }
        }
    }

    /// A way out on the text side rewrites the whole clause, so every type-mismatch cause of
    /// one clause offers the same rewrites, whichever value it was read for.
    #[test]
    fn every_cause_of_one_clause_offers_the_same_ways_out() {
        let values = [
            "10",
            "2019",
            "2024-06",
            "2024-06-15",
            "12:30",
            "2024:07:23",
            "1e20",
            "0500",
            "2024-06-15T14:30",
        ];
        for a in values {
            for b in values {
                let clause = format!("numtext IN ({a}, {b})");
                let offered: Vec<Vec<String>> = causes(&clause)
                    .into_iter()
                    .filter_map(|cause| match cause {
                        BindCause::TypeMismatch(entry) => Some(entry.rewrites()),
                        _ => None,
                    })
                    .collect();
                if let Some(first) = offered.first() {
                    for other in &offered {
                        assert_eq!(other, first, "{clause}");
                    }
                }
            }
        }
        // A quoted sibling reads the same against a date chain, so the date way out stands.
        assert!(refusal(r#"numtext IN (2019, "2024-06")"#)
            .contains("numtext|yearmonth IN (2019, 2024-06)"));
    }

    /// **A way out makes its whole clause bind** (`keep_ways_that_bind`), over every pair of
    /// a wide set of values — `true`, `false` and a value holding five spaces included — on
    /// keys and chains that reach every builder of a way out, under `?` and every comparison
    /// and list operator; each way out is read in the text shown. The corpus must reach each
    /// kind of refusal that offers one, so a narrower corpus cannot pass in silence.
    #[test]
    fn every_way_out_binds_the_whole_clause() {
        let values = [
            "10",
            "2019",
            "0500",
            "-5",
            "1e20",
            "1.2.3",
            "2024-06",
            "2024-06-15",
            "2024-06-15T14:30",
            "2024:07:23",
            "12:30",
            "false",
            r#""a     b""#,
            "2024*",
            "2024-06*",
            "x",
            "true",
            r#""x""#,
            r#""2024-06""#,
            r#""12:30""#,
        ];
        let keys = [
            "numtext",
            "numtext|yearmonth",
            "numtext|number",
            "textdate|date",
            "taken",
            "taken|date",
            "taken|yearmonth",
            "taken|year",
            "taken|time",
            "taken|lowercase",
            "FNumber",
            "FNumber|lowercase",
            "source.mtime",
            "source.mtime|lowercase",
            "source.size",
            "source.rel_path|number",
            "source.rel_path[1:3]|number",
            "source.rel_path[0]",
            "content.hash.sha256|short",
        ];
        let operators = ["=", "!=", "<", "<=", ">", ">=", "~", "!~"];
        let mut clauses = Vec::new();
        for key in keys {
            clauses.push(format!("{key}?"));
            for a in values {
                for op in operators {
                    clauses.push(format!("{key}{op}{a}"));
                }
                for b in values {
                    clauses.push(format!("{key} IN ({a}, {b})"));
                    clauses.push(format!("{key} NOT IN ({a}, {b})"));
                }
            }
        }
        let mut reached: Vec<&'static str> = Vec::new();
        let mut checked = 0;
        for clause in &clauses {
            for cause in bind(clause).err().unwrap_or_default() {
                let ways = cause.entry().map(Entry::rewrites).unwrap_or_default();
                if !ways.is_empty() {
                    let kind = match &cause {
                        BindCause::Unparsed(_) => "unparsed",
                        BindCause::UnknownKey(_) => "unknown key",
                        BindCause::Unreadable(_) => "unreadable",
                        BindCause::TypeMismatch(_) => "type mismatch",
                        BindCause::OutOfRange(_) => "out of range",
                        BindCause::WrongModifier { .. } => "wrong modifier",
                        BindCause::GlobNotText(_) => "glob",
                        BindCause::NotABucketLabel(_) => "bucket label",
                        BindCause::BucketOrdered(_) => "bucket ordered",
                        BindCause::TooFine(_) => "too fine",
                        BindCause::MixedType { .. } => "two types",
                        BindCause::TypeChanged { .. } => "type changed",
                    };
                    if !reached.contains(&kind) {
                        reached.push(kind);
                    }
                }
                let shown = FilterRefused {
                    refused: Refused::Filter,
                    causes: vec![cause.clone()],
                }
                .to_string();
                for way in ways {
                    checked += 1;
                    assert!(
                        shown.contains(&format!(": {way}")),
                        "{way} not shown:\n{shown}"
                    );
                    if let Err(again) = bind(&way) {
                        panic!("{clause} → way out {way} is refused again: {again:?}");
                    }
                }
            }
        }
        for kind in [
            "unreadable",
            "type mismatch",
            "wrong modifier",
            "glob",
            "too fine",
        ] {
            assert!(reached.contains(&kind), "no {kind} way out was reached");
        }
        assert!(checked > 1000, "only {checked} ways out were checked");
    }

    /// A way out that would be refused again is not shown, its text with it; a wrong modifier
    /// left with no way out names the modifiers that apply instead.
    #[test]
    fn a_way_out_that_would_be_refused_again_is_not_offered() {
        let text = refusal("source.size|lowercase=10");
        assert!(!text.contains("|number|lowercase"), "{text}");
        assert!(text.contains("Modifiers for numbers"), "{text}");
        assert!(refusal("source.size|lowercase=x").contains("source.size|number|lowercase=x"));
        let list = refusal("source.size IN (true, big)");
        assert!(!list.contains("is stored as 1"), "{list}");
        assert!(refusal("source.size IN (true, 5)").contains("source.size IN (1, 5)"));
        // Each builder's way out is shown where it binds.
        for (clause, shown) in [
            ("numtext=10", r#"As text: numtext="10""#),
            (
                "numtext=10",
                "As numbers, if its values are numbers: numtext|number=10",
            ),
            (
                "numtext=2024-06",
                "As dates, if its values are dates: numtext|yearmonth=2024-06",
            ),
            ("numtext=1.2.3", r#"As text: numtext="1.2.3""#),
            (
                "source.size IN (true, false)",
                "is stored as 1: source.size IN (1, 0)",
            ),
            (
                "source.size IN (true, false)",
                "is stored as 0: source.size IN (1, 0)",
            ),
            ("source.mtime=12:30", "|time: source.mtime|time=12:30"),
            (
                "source.mtime~2024-06*",
                "To select by month: source.mtime|yearmonth=2024-06",
            ),
            (
                "taken|yearmonth=2024-06-15",
                "To compare by day: taken|date=2024-06-15",
            ),
            ("source.size|lowercase=x", "source.size|number|lowercase=x"),
            (
                "source.rel_path|number>=2019",
                "source.rel_path[0]|number>=2019",
            ),
        ] {
            let text = refusal(clause);
            assert!(text.contains(shown), "{clause}: {shown} not in\n{text}");
        }
    }

    #[test]
    fn every_cause_is_an_answer_or_input_by_its_kind() {
        let entry = || Entry::new("k", String::new(), Vec::new());
        for cause in [
            BindCause::Unparsed(entry()),
            BindCause::UnknownKey(entry()),
            BindCause::Unreadable(entry()),
            BindCause::TypeMismatch(entry()),
            BindCause::OutOfRange(entry()),
            BindCause::WrongModifier {
                entry: entry(),
                fallback: String::new(),
            },
            BindCause::GlobNotText(entry()),
            BindCause::NotABucketLabel(entry()),
            BindCause::BucketOrdered(entry()),
            BindCause::TooFine(entry()),
        ] {
            assert!(!cause.is_answer(), "{cause:?}");
        }
        assert!(BindCause::MixedType {
            entry: entry(),
            key: "content.k".into()
        }
        .is_answer());
        assert!(BindCause::TypeChanged {
            key: "content.k".into(),
            had_no_facts: false,
        }
        .is_answer());
    }
}
