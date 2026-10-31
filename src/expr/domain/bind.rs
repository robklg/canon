//! **Binding**: between reading a filter and looking at any source, each typed value is read
//! once, as the type its key gives — a year against a time key, a number against a number
//! key, text against a text key — and a value that cannot mean anything for its key is
//! refused by name, with what to type instead. Pure: the key types arrive as data
//! (`expr::ops::bind` reads them), so every rule here is tested with no database.
//!
//! Three things are spoken here once. **The modifier table** ([`result_of`]): what each
//! modifier accepts and gives, read by binding for every key a filter or a report names.
//! **The input types** ([`input_type`]): a typed value is a number, a time or a time of day
//! where it reads as one, and text otherwise; against text, only text is text. **The refusal** ([`ExpressionRefused`]): every
//! problem the reader met and every cause binding finds, each drawn under the typed line it
//! sits in: one sentence, the line that states the rule, and a fix only from the closed list,
//! read back and bound before it is shown.

use std::collections::{BTreeMap, HashMap};
use std::fmt;

use strum::IntoEnumIterator;

use crate::core::domain::fact::{FactType, FactValue, KeyTypes};
use crate::core::domain::format::format_count;
use crate::core::domain::number::number_form;
use crate::core::domain::time::reading::{
    read_time, read_time_of_day, TimeReadError, EXAMPLE_FORMS,
};
use crate::core::domain::time::value::{Precision, TimeValue};

use super::filter::{CompareOp, Expr, InputValue, NeedsQuotes, StatusPredicate, UsedStatus};
use super::glob::{needs_quotes_problem, Glob, InputGlob};
use super::key::{InputKey, Key};
use super::print::{
    convert_command, is_plain_bucket_label, mixed_type_way_out, mixed_types, printed_key,
    printed_value, printed_word, quoted, threshold_bucket_labels,
};
use super::problem::{cut, Fix, Place, Problem, TextOrPath};
use super::read::{is_space, read_filter, read_keys, AliasMode, AliasUse, Grammar};
use super::transform::{Modifier, ModifierCall, ModifierCategory, PathAccessor};

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
    pub(in crate::expr) fn of(t: FactType) -> Gives {
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

    /// **The type statement**: what a value of `key`, as typed, is — "is of type number", "is a
    /// day", "is a year, four digits", "is grouped into ranges". One writer, total over what a
    /// key can give.
    fn statement(&self, key: &str) -> String {
        let what = match self {
            Gives::Text => "is of type text".to_string(),
            Gives::Path => "is of type path".to_string(),
            Gives::Number => "is of type number".to_string(),
            Gives::Time => "is of type time".to_string(),
            Gives::TimeAt(precision) => format!("is a {}", precision.word()),
            Gives::TimeOfDay => "is a time of day".to_string(),
            Gives::Part(m) => format!("is {}", part_rule(*m).range),
            Gives::Bucket(_) => "is grouped into ranges".to_string(),
        };
        format!("{key} {what}")
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
}

// ============================================================================
// The bound tree
// ============================================================================

/// A key, read and checked: the key as typed, the base key a value is read from, the accessor
/// and modifiers it is read through, and what a value read for it may be while the run reads
/// it ([`ReadAs`]).
#[derive(Debug, Clone)]
pub(in crate::expr) struct BoundKey {
    pub(in crate::expr) input: String,
    pub(in crate::expr) base: String,
    pub(in crate::expr) accessor: Option<PathAccessor>,
    pub(in crate::expr) modifiers: Vec<ModifierCall>,
    pub(in crate::expr) read_as: ReadAs,
}

impl BoundKey {
    fn of(key: &InputKey, read_as: ReadAs) -> BoundKey {
        BoundKey {
            input: key.input.clone(),
            base: key.base.clone(),
            accessor: key.accessor.as_ref().map(|(accessor, _)| accessor.clone()),
            modifiers: key.modifiers.iter().map(|(call, _)| call.clone()).collect(),
            read_as,
        }
    }

    /// Whether the key reads its base through an accessor or a modifier.
    pub(in crate::expr) fn has_transforms(&self) -> bool {
        self.accessor.is_some() || !self.modifiers.is_empty()
    }
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
    match read_as_changed(read_as, key, value, refused) {
        Some(refusal) => Err(refusal.into()),
        None => Ok(()),
    }
}

/// [`check_read_as`]'s answer as the refusal itself, where the value is not what the key was
/// read as: for a caller that carries it as a typed outcome rather than an error.
pub(in crate::expr) fn read_as_changed(
    read_as: ReadAs,
    key: &str,
    value: &FactValue,
    refused: Refused,
) -> Option<ExpressionRefused> {
    let changed = match read_as {
        ReadAs::Any => false,
        ReadAs::Absent => true,
        ReadAs::One(t) => value.kind() != t,
    };
    changed.then(|| ExpressionRefused {
        refused,
        texts: Vec::new(),
        typed_at: BTreeMap::new(),
        causes: vec![BindCause::TypeChanged {
            key: key.to_string(),
            had_no_facts: read_as == ReadAs::Absent,
        }],
    })
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
    /// A glob, bound only for a key whose result is text or a path.
    Glob {
        key: BoundKey,
        glob: Glob,
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
            Bound::Exists { .. }
            | Bound::Compare { .. }
            | Bound::In { .. }
            | Bound::Glob { .. } => {}
        }
    }

    /// The base keys the tree reads, for prefetching.
    pub(in crate::expr) fn keys(&self, out: &mut Vec<String>) {
        match self {
            Bound::And(all) | Bound::Or(all) => all.iter().for_each(|b| b.keys(out)),
            Bound::Not(inner) => inner.keys(out),
            Bound::Exists { key }
            | Bound::Compare { key, .. }
            | Bound::In { key, .. }
            | Bound::Glob { key, .. } => out.push(key.base.clone()),
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
            Bound::Exists { .. } | Bound::Glob { .. } | Bound::Status(_) => {}
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
        Expr::Exists { key, .. }
        | Expr::Compare { key, .. }
        | Expr::In { key, .. }
        | Expr::Glob { key, .. } => {
            if known_type(&key.base).is_none() && !out.contains(&key.base) {
                out.push(key.base.clone())
            }
        }
        Expr::Status { .. } => {}
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
/// Each cause's fix is bound against the same key types, read with the same `aliases`, before
/// the refusal is shown, and one that would be refused again is dropped
/// ([`keep_fix_that_binds`]).
pub(in crate::expr) fn bind_expr(
    expr: &Expr,
    types: &HashMap<String, KeyTypes>,
    aliases: AliasMode,
    causes: &mut Vec<BindCause>,
) -> Bound {
    let before = causes.len();
    let bound = bind_clauses(expr, types, causes);
    let binds = |rewrite: &str| {
        let reading = read_filter(rewrite, 0, aliases);
        reading.problems.is_empty()
            && reading.value.is_some_and(|expr| {
                let mut again = Vec::new();
                bind_clauses(&expr, types, &mut again);
                again.is_empty()
            })
    };
    for cause in &mut causes[before..] {
        keep_fix_that_binds(cause, &binds);
    }
    bound
}

/// **A fix shown reads back and binds**: drop `cause`'s fix where `binds` refuses what it rewrites
/// to — a filter's clause bound as a clause, a report's key as a key. Every fix is a [`Fix`], from
/// the closed list, so no rewrite can be shown unchecked at a door that calls this or the check
/// named below for its fix — today the three doors that bind a key (`bind_expr`, `bind_report_key`,
/// `read_pattern`); a fourth caller of `bind_key` must call it too. Two fixes are not a clause or
/// one report key, so they pass through here and are checked at their own door instead: a filter's
/// quoted name, a key kept where it names one with facts (`keep_quoted_name_with_facts`), and a
/// `--group-by` text's grouping keys, a key list kept where it reads back and every key binds
/// (`keep_grouping_keys_that_bind`, in `ops/bind.rs`), which a new reader of a key list must call
/// too.
pub(in crate::expr) fn keep_fix_that_binds(cause: &mut BindCause, binds: &dyn Fn(&str) -> bool) {
    if let Some(problem) = cause.problem_mut() {
        problem.keep_fix_that_binds(binds);
    }
}

/// The key a problem's quoted name reads back as, alone and with no problem; `None` where the
/// problem offers no quoted name or the name does not read back as one key.
pub(in crate::expr) fn quoted_name_key(problem: &Problem) -> Option<InputKey> {
    let Fix::KeyName { rewrite } = &problem.fix else {
        return None;
    };
    let reading = read_keys(rewrite, 0, Grammar::Key, AliasMode::NotLoaded);
    match reading.value {
        Some(mut keys) if reading.problems.is_empty() && keys.len() == 1 => keys.pop(),
        Some(_) | None => None,
    }
}

/// **A quoted name is offered only for a key with facts**, so a refusal never offers an unknown
/// key: drop the quoted name where it does not read back as one key, or names a key `types`
/// holds nothing for.
pub(in crate::expr) fn keep_quoted_name_with_facts(
    problem: &mut Problem,
    types: &HashMap<String, KeyTypes>,
) {
    if matches!(problem.fix, Fix::KeyName { .. })
        && quoted_name_key(problem).is_none_or(|key| types_of(&key.base, types).is_none())
    {
        problem.fix = Fix::None;
    }
}

/// Bind every clause of `expr`, collecting causes with their fixes as built.
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
        Expr::Status { predicate, .. } => Bound::Status(*predicate),
        Expr::Exists { key, clause, .. } => match bind_key(key, clause, Reads::Existence, types) {
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
            ..
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
            ..
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
        Expr::Glob {
            key, glob, clause, ..
        } => match bind_key(key, clause, Reads::Value, types) {
            Ok((bound, chain)) => match bind_glob(key, glob, clause, &chain.gives) {
                Ok(glob) => Bound::Glob { key: bound, glob },
                Err(glob_causes) => {
                    causes.extend(glob_causes);
                    refused()
                }
            },
            Err(cause) => {
                causes.push(cause);
                refused()
            }
        },
    }
}

/// The operator of `clause` as typed, with the spaces around it: what stands between the key
/// and the value starting at byte `value_start` of the typed text.
fn operator_as_typed<'c>(clause: &'c str, key: &InputKey, value_start: usize) -> &'c str {
    cut(clause, key.input.len(), value_start - key.place.start)
}

/// **A glob binds where the key's result is text or a path**, and holds nothing that needs
/// quotes: each part that does is one problem, its caret under the part's first such character.
/// For any other result the one cause is the type statement, the operator named as typed, with
/// no fix — and a character that needs quotes is not reported, since quoting it would only meet
/// the type refusal.
fn bind_glob(
    key: &InputKey,
    glob: &InputGlob,
    clause: &str,
    gives: &Gives,
) -> Result<Glob, Vec<BindCause>> {
    match gives {
        Gives::Text | Gives::Path if glob.needs_quotes.is_empty() => Ok(glob.glob.clone()),
        Gives::Text | Gives::Path => Err(glob
            .needs_quotes
            .iter()
            .map(|&at| {
                let rest = cut(&glob.input, at - glob.place.start, glob.input.len());
                BindCause::Read(needs_quotes_problem(rest, at, glob.place))
            })
            .collect()),
        Gives::Number
        | Gives::Time
        | Gives::TimeAt(_)
        | Gives::TimeOfDay
        | Gives::Part(_)
        | Gives::Bucket(_) => {
            let op = operator_as_typed(clause, key, glob.place.start).trim_matches(is_space);
            Err(vec![BindCause::GlobNotText(Problem::new(
                glob.place,
                format!("`{op}` works on text; {}", gives.statement(&key.input)),
                None,
            ))])
        }
    }
}

/// What a clause reads of its key: only whether a value exists (`k?`), or the value.
#[derive(Clone, Copy, PartialEq)]
pub(in crate::expr) enum Reads {
    Existence,
    Value,
    /// A report's key (`facts --key`, `--group-by`): every value it holds, of whatever type;
    /// only a modifier works on one type. A key with no facts is unknown, as for a value
    /// compared.
    Report,
    /// A pattern's key, whose value is placed: a key with no facts is unknown, and a key of
    /// mixed type is one of the pattern's problems, as for a value compared.
    Pattern,
}

/// What a key's chain gives.
#[derive(Debug, Clone)]
pub(in crate::expr) struct Chain {
    gives: Gives,
    /// The full stored key, where the key is one read with no accessor and no modifier, whose
    /// stored type a conversion could change ([`Conversion::WithStoredType`]).
    convertible: Option<String>,
    /// A bare key of mixed type, read where no value is compared: its types, which a report's
    /// rows are of.
    mixed: Option<KeyTypes>,
}

impl Chain {
    /// What a report's rows are, read through this chain.
    pub(in crate::expr) fn rows(self) -> Rows {
        match self.mixed {
            Some(types) => Rows::Mixed(types),
            None => Rows::One(self.gives),
        }
    }
}

/// **What a report key's rows are**: what its chain gives, or — for a bare key of mixed type —
/// its types, each row then of its own value's type.
#[derive(Debug, Clone)]
pub(in crate::expr) enum Rows {
    One(Gives),
    Mixed(KeyTypes),
}

/// Check a key: its type from all its facts, then its accessor and modifiers through the table. A
/// key with no facts binds only where nothing is read but whether a value exists (`?`), its chain
/// checked from any type ([`walk`]); a key of mixed type binds only for a bare `?` or a bare report
/// key, which compare no value. A name with no reading in an expression — a `source.` name that is
/// no source fact — names no key, and is refused wherever it stands.
pub(in crate::expr) fn bind_key(
    key: &InputKey,
    clause: &str,
    reads: Reads,
    types: &HashMap<String, KeyTypes>,
) -> Result<(BoundKey, Chain), BindCause> {
    let unknown = || {
        BindCause::UnknownKey(Problem::new(
            key.name_place,
            format!("`{}` is an unknown key", key.name_for(key.name_place)),
            Some(UNKNOWN_KEY_RULE.to_string()),
        ))
    };
    if Key::read(&key.base).is_none() {
        return Err(unknown());
    }
    let bare_existence = matches!(reads, Reads::Existence | Reads::Report) && !key.has_transforms();
    let unbound = |read_as, mixed| {
        Ok((
            BoundKey::of(key, read_as),
            Chain {
                gives: Gives::Text,
                convertible: None,
                mixed,
            },
        ))
    };
    let Some(key_types) = types_of(&key.base, types) else {
        return match reads {
            Reads::Existence if key.has_transforms() => {
                let chain = walk(key, None)?;
                Ok((BoundKey::of(key, ReadAs::Absent), chain))
            }
            Reads::Existence => unbound(ReadAs::Any, None),
            Reads::Value | Reads::Pattern | Reads::Report => Err(unknown()),
        };
    };
    let Some(base_type) = key_types.single() else {
        if bare_existence {
            return unbound(ReadAs::Any, Some(key_types));
        }
        return Err(mixed_type(key, clause, key_types, reads));
    };
    let chain = walk(key, Some(base_type))?;
    let read_as = if bare_existence {
        ReadAs::Any
    } else {
        ReadAs::One(base_type)
    };
    Ok((BoundKey::of(key, read_as), chain))
}

/// **A pattern's key, bound**: what its value is read as and what its chain gives — a path makes
/// directories, any other value one name. `scope.rel_path` names no key and is answered by the
/// lock entry, so it binds here as a path, its chain walked from the path type; every other key
/// through [`bind_key`], read as a value is. `clause` is the `{…}` as specified.
pub(in crate::expr) fn bind_pattern_key(
    key: &InputKey,
    scope: bool,
    clause: &str,
    types: &HashMap<String, KeyTypes>,
) -> Result<(BoundKey, Gives), BindCause> {
    if scope {
        let chain = walk(key, Some(FactType::Path))?;
        return Ok((BoundKey::of(key, ReadAs::One(FactType::Path)), chain.gives));
    }
    bind_key(key, clause, Reads::Pattern, types).map(|(bound, chain)| (bound, chain.gives))
}

/// The mixed-type answer: the key as typed, its types and their counts on every root.
fn mixed_type(key: &InputKey, clause: &str, key_types: KeyTypes, reads: Reads) -> BindCause {
    let reader = match reads {
        Reads::Report => "a modifier supports one type only",
        Reads::Pattern => "a pattern supports one type only",
        Reads::Existence | Reads::Value => "a filter supports one type only",
    };
    let mut counts = key_types.counts().to_vec();
    counts.sort_by_key(|&(_, n)| std::cmp::Reverse(n));
    let counted = counts
        .iter()
        .map(|&(t, n)| {
            let facts = if n == 1 { "fact" } else { "facts" };
            format!("{}: {} {facts}", Gives::of(t).words(), format_count(n))
        })
        .collect::<Vec<_>>()
        .join("   ");
    BindCause::MixedType(Box::new(MixedType {
        clause: clause.to_string(),
        place: key.place,
        sentence: format!(
            "{} is of mixed type ({}); {reader}",
            key.name_for(key.place),
            mixed_types(&key_types)
        ),
        counts: format!("{counted}   (every fact of the key, on every root)"),
        key: key.base.clone(),
        types: key_types,
    }))
}

/// Walk a key's accessor and modifiers through the table, from its base type — or, for a key
/// with no facts (`None`), from any type: its first step gives what it gives on whichever input
/// takes it (an index text, a slice a path, a modifier its own result), the most permissive
/// reading, so a chain is refused only where no key could pass a value through it.
fn walk(key: &InputKey, base_type: Option<FactType>) -> Result<Chain, BindCause> {
    let mut gives = base_type.map(Gives::of);
    if let Some((accessor, place)) = &key.accessor {
        if let Some(input) = gives
            .as_ref()
            .filter(|g| !matches!(g, Gives::Text | Gives::Path))
        {
            return Err(BindCause::WrongModifier(Problem::new(
                *place,
                format!(
                    "an accessor `[…]` works on paths or text; {}",
                    input.statement(key.up_to(*place))
                ),
                Some(applicable_line(input, false)),
            )));
        }
        gives = Some(match accessor {
            PathAccessor::Index(_) => Gives::Text,
            PathAccessor::Slice { .. } => Gives::Path,
        });
    }
    let mut from_modifier = false;
    for (call, place) in &key.modifiers {
        let next = match &gives {
            Some(input) => result_of(input, from_modifier, call),
            None => [Gives::Text, Gives::Number]
                .iter()
                .find_map(|input| result_of(input, true, call)),
        };
        match (next, &gives) {
            (Some(next), _) => {
                gives = Some(next);
                from_modifier = true;
            }
            (None, Some(input)) => {
                return Err(wrong_modifier(
                    key,
                    *place,
                    input,
                    from_modifier,
                    call.modifier,
                ))
            }
            (None, None) => unreachable!("every modifier takes text or a number"),
        }
    }
    Ok(Chain {
        gives: gives.unwrap_or(Gives::Text),
        convertible: (base_type.is_some()
            && known_type(&key.base).is_none()
            && !key.has_transforms())
        .then(|| key.base.clone()),
        mixed: None,
    })
}

/// **The modifier table**: what `call` gives, applied to `input` — `None` where it cannot
/// apply. One exhaustive match, read by binding for filters and reports alike. A text
/// modifier reads text, a path, a bucket's label, or any modifier's result in its printed
/// form; never a stored number or time directly — the user says which printed form they mean
/// (`|number`, `|datetime`). A time modifier reads a time or text, never a bucket's range.
pub(in crate::expr) fn result_of(
    input: &Gives,
    from_modifier: bool,
    call: &ModifierCall,
) -> Option<Gives> {
    // A time of day carries an hour, a minute and a second, and no date: what it is read on
    // is a reference day, which no modifier may read back as a date. A bucket's range is never
    // a time.
    let reads_as_time = matches!(input, Gives::Time | Gives::TimeAt(_) | Gives::Text)
        || (*input == Gives::TimeOfDay && call.modifier.reads_a_time_of_day());
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
        Modifier::Stem | Modifier::Ext | Modifier::Short => {
            (input.is_text() || from_modifier).then_some(Gives::Text)
        }
        // A case modifier keeps a path a path.
        Modifier::Lowercase | Modifier::Uppercase | Modifier::Capitalize => match input {
            Gives::Path => Some(Gives::Path),
            _ => (input.is_text() || from_modifier).then_some(Gives::Text),
        },
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

/// A modifier the chain's result cannot take: what the modifier works on, and the key as typed
/// up to it; for a time modifier on a time of day, the date it has none of. The rule line names
/// the modifiers that apply.
fn wrong_modifier(
    key: &InputKey,
    place: Place,
    gives: &Gives,
    from_modifier: bool,
    modifier: Modifier,
) -> BindCause {
    let name: &'static str = modifier.into();
    let subject = gives.statement(key.up_to(place));
    // A time of day is a time, so "works on times" would not say why: say what is missing.
    let sentence = if *gives == Gives::TimeOfDay && modifier.category() == ModifierCategory::Time {
        format!("|{name} needs a date; {subject}, which has none")
    } else {
        format!("|{name} works on {}; {subject}", reads_words(modifier))
    };
    BindCause::WrongModifier(Problem::new(
        place,
        sentence,
        Some(applicable_line(gives, from_modifier)),
    ))
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

/// **The input type** of a typed value: what it reads as, never how it looks.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(in crate::expr) enum InputType {
    /// Written as a number, whether or not Canon can hold it exactly (`1e400` included).
    Number,
    /// Read by the one time reading: `2024`, `2024-06-15`, `2024-06-15T14:30+02:00`.
    Time,
    /// A time of day: `14:30`, `14:30:05`.
    TimeOfDay,
    /// Anything else, however it looks (`2024e`, `12:3`, `2024-06-15_beach.jpg`).
    Text,
}

/// **The input type** of `text`: a number by its written form, else a time, else a time of
/// day, else text.
pub(in crate::expr) fn input_type(text: &str) -> InputType {
    if number_form(text).is_some() {
        InputType::Number
    } else if read_time(text).is_ok() {
        InputType::Time
    } else if read_time_of_day(text).is_ok() {
        InputType::TimeOfDay
    } else {
        InputType::Text
    }
}

/// One clause being bound: its key, its text as written, what its chain gives.
struct Site<'a> {
    key: &'a InputKey,
    clause: &'a str,
    chain: &'a Chain,
    /// The values of an `IN` list, for a fix that rewrites the list.
    list: Option<&'a [InputValue]>,
}

impl Site<'_> {
    /// Read one typed value as the type the key gives, under `op`.
    fn read(&self, op: CompareOp, value: &InputValue) -> Result<Test, BindCause> {
        if let Some(needs) = value.needs_quotes {
            if self.chain.gives.is_text() {
                return Err(self.needs_quotes(value, needs));
            }
        }
        match &self.chain.gives {
            Gives::Text | Gives::Path => self.read_text(op, value),
            Gives::Number => self.read_number(value),
            Gives::Time | Gives::TimeAt(_) | Gives::TimeOfDay => self.read_time(value),
            Gives::Part(m) => self.read_part(*m, op, value),
            Gives::Bucket(args) => self.read_bucket(args, op, value),
        }
    }

    /// **The type statement for this clause**, the key as typed: "filename is of type text".
    fn statement(&self) -> String {
        self.chain.gives.statement(&self.key.input)
    }

    /// A problem about `value`, its caret under the value, with no fix.
    fn problem(&self, value: &InputValue, sentence: String, rule_line: Option<String>) -> Problem {
        Problem::new(value.place, sentence, rule_line)
    }

    /// An unquoted value with a character that keeps it from standing bare, where the key gives
    /// text: the character needs quotes, or — after `=`, `!=` or in a list — it is a wildcard
    /// the comparison does not take. Its caret sits under the character. The fix, from the
    /// closed list, adds only quotes or reads the operator as a glob, everything else as typed:
    /// the value quoted; in a list one fix for the clause, every item that needs quotes quoted,
    /// shown with the first; a wildcard after `=` or `!=` the glob and the exact text; a
    /// wildcard in a list, none. Whether it is shown is decided when it is read back
    /// ([`keep_fix_that_binds`]).
    fn needs_quotes(&self, value: &InputValue, needs: NeedsQuotes) -> BindCause {
        let (at, wildcard) = match needs {
            NeedsQuotes::Character(at) => (at, false),
            NeedsQuotes::Wildcard(at) => (at, true),
        };
        let c = cut(&value.input, at - value.place.start, value.input.len())
            .chars()
            .next()
            .unwrap_or_default();
        let sentence = if !wildcard {
            format!("`{c}` needs quotes around the value")
        } else if self.list.is_some() {
            format!("`{c}` is a wildcard, but IN does not take wildcards")
        } else {
            let op =
                operator_as_typed(self.clause, self.key, value.place.start).trim_matches(is_space);
            format!("`{c}` is a wildcard, but `{op}` does not take wildcards")
        };
        let quote = |v: &InputValue| self.needs_quoting(v);
        let fix = match (wildcard, self.list) {
            (false, None) => Fix::Quoted {
                rewrite: self.with_value(value, &quoted_as_typed(&value.text)),
            },
            (false, Some(values)) if first_of(values, value, quote) => Fix::Quoted {
                rewrite: self.with_items_quoted(values, quote),
            },
            (true, None) => match self.as_glob(value) {
                Some(glob) => Fix::GlobOrExact {
                    glob,
                    exact: self.with_value(value, &quoted_as_typed(&value.text)),
                },
                None => Fix::None,
            },
            (false, _) | (true, Some(_)) => Fix::None,
        };
        BindCause::Read(Problem {
            place: Place {
                start: at,
                end: at + c.len_utf8(),
                ..value.place
            },
            sentence,
            rule_line: None,
            fix,
            also: None,
        })
    }

    /// **Whether a list item needs quotes**, for either reason: a character that keeps it from
    /// standing bare, or — for a key of type text or path — reading as a number, a time or a
    /// time of day. A list's one quoting fix quotes every such item, so a list needing both
    /// still gets a fix that binds.
    fn needs_quoting(&self, v: &InputValue) -> bool {
        let character = matches!(v.needs_quotes, Some(NeedsQuotes::Character(_)));
        let another_type = matches!(self.chain.gives, Gives::Text | Gives::Path)
            && !v.quoted
            && input_type(&v.text) != InputType::Text;
        character || another_type
    }

    /// The clause as typed, with every list item `quote` picks quoted in its place, so whatever
    /// else was typed in the list stays where it was.
    fn with_items_quoted(
        &self,
        values: &[InputValue],
        quote: impl Fn(&InputValue) -> bool,
    ) -> String {
        let start = self.key.place.start;
        let mut out = String::new();
        let mut from = 0;
        for v in values {
            if quote(v) {
                out.push_str(cut(self.clause, from, v.place.start - start));
                out.push_str(&quoted_as_typed(&v.text));
                from = v.place.end - start;
            }
        }
        out.push_str(cut(self.clause, from, self.clause.len()));
        out
    }

    /// The clause with its `=` read as `~` and `!=` as `!~`, the value as typed; `None` after
    /// any other operator.
    fn as_glob(&self, value: &InputValue) -> Option<String> {
        let between = operator_as_typed(self.clause, self.key, value.place.start);
        let glob = match between.trim_matches(is_space) {
            "=" => between.replacen('=', "~", 1),
            "!=" => between.replacen("!=", "!~", 1),
            _ => return None,
        };
        Some(format!("{}{glob}{}", self.key.input, value.input))
    }

    /// A value for a key of type text or path: quoted, or reading as no number, time or time of
    /// day, it is text. Otherwise it could mean that other type, and is refused naming both;
    /// after `=`, `!=` or `IN` the fix quotes it for the key's type (in a list, the one fix of
    /// `needs_quoting`, when this item is the first needing quotes), and a key of type text
    /// carries the bottom line.
    fn read_text(&self, op: CompareOp, value: &InputValue) -> Result<Test, BindCause> {
        let reads_as = |v: &InputValue| (!v.quoted).then(|| input_type(&v.text));
        let what = match reads_as(value) {
            None | Some(InputType::Text) => return Ok(Test::Text(value.text.clone())),
            Some(InputType::Number) => "a number",
            Some(InputType::Time) => "a time",
            Some(InputType::TimeOfDay) => "a time of day",
        };
        let quote = |v: &InputValue| self.needs_quoting(v);
        let key_type = match self.chain.gives {
            Gives::Path => TextOrPath::Path,
            _ => TextOrPath::Text,
        };
        let fix = match self.list {
            _ if op != CompareOp::Eq => Fix::None,
            None => Fix::ForKeyType {
                key_type,
                rewrite: self.with_value(value, &quoted_as_typed(&value.text)),
            },
            Some(values) if first_of(values, value, quote) => Fix::ForKeyType {
                key_type,
                rewrite: self.with_items_quoted(values, quote),
            },
            Some(_) => Fix::None,
        };
        let conversion = match (key_type, &self.chain.convertible) {
            (TextOrPath::Path, _) => None,
            (TextOrPath::Text, Some(key)) => Some(Conversion::WithStoredType(key.clone())),
            (TextOrPath::Text, None) => Some(Conversion::InFilterOnly),
        };
        Err(BindCause::TypeMismatch {
            problem: Box::new(Problem {
                fix,
                ..self.problem(
                    value,
                    format!("{}, and `{}` is {what}", self.statement(), value.input),
                    None,
                )
            }),
            conversion,
        })
    }

    /// A number as written, held exactly or refused: what is not written as a number is not
    /// one, and what is written as one but cannot be held as written cannot be represented.
    fn read_number(&self, value: &InputValue) -> Result<Test, BindCause> {
        let Some(form) = number_form(&value.text) else {
            return Err(BindCause::Unreadable(self.problem(
                value,
                format!("{}; `{}` is not a number", self.statement(), value.input),
                Some("Type a number: 1024, -5, 3.14, 1e20".to_string()),
            )));
        };
        form.exact().map(Test::Number).map_err(|_| {
            BindCause::Unreadable(self.problem(
                value,
                format!(
                    "{}; `{}` cannot be represented exactly",
                    self.statement(),
                    value.input
                ),
                None,
            ))
        })
    }

    fn read_time(&self, value: &InputValue) -> Result<Test, BindCause> {
        if self.chain.gives == Gives::TimeOfDay {
            return read_time_of_day(&value.text)
                .map(Test::TimeOfDay)
                .map_err(|_| {
                    BindCause::Unreadable(self.problem(
                        value,
                        format!(
                            "{}; `{}` is not a time of day",
                            self.statement(),
                            value.input
                        ),
                        Some("Type a time of day: 14:30, 14:30:05".to_string()),
                    ))
                });
        }
        let error = match read_time(&value.text) {
            Ok(tv) => {
                return match self.chain.gives {
                    Gives::TimeAt(grain) if tv.precision() > grain => {
                        Err(self.too_fine(grain, value))
                    }
                    _ => Ok(Test::Period(tv)),
                }
            }
            Err(error) => error,
        };
        let what = if read_time_of_day(&value.text).is_ok() {
            "has no date".to_string()
        } else if number_form(&value.text).is_some() {
            "is not a time: a year is four digits, and a number never means seconds since 1970"
                .to_string()
        } else {
            match error {
                TimeReadError::NotInList => "is not a time".to_string(),
                other => format!("is not a time: {other}"),
            }
        };
        let forms = EXAMPLE_FORMS
            .iter()
            .map(|f| printed_value(f, FactType::Time))
            .collect::<Vec<_>>()
            .join(", ");
        Err(BindCause::Unreadable(self.problem(
            value,
            format!("{}; `{}` {what}", self.statement(), value.input),
            Some(format!(
                "Type a year, month, day, minute or second: {forms}"
            )),
        )))
    }

    /// A part's value: one of its words, or a whole number written as digits of any length (a
    /// year four), read without overflow. Only `=`, `!=` and `IN` refuse a number outside the
    /// part's range, which could never match; ordered, any whole number means something.
    fn read_part(
        &self,
        modifier: Modifier,
        op: CompareOp,
        value: &InputValue,
    ) -> Result<Test, BindCause> {
        let rule = part_rule(modifier);
        let text = value.text.as_str();
        if let Some(at) = rule.words.iter().position(|w| *w == text) {
            return Ok(Test::Part(at as i64 + 1));
        }
        let digits = !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit());
        let four = modifier != Modifier::Year || text.len() == 4;
        if digits && four {
            // A number past what an `i64` holds is past every part's range, and orders above
            // every value, so it stands as the largest.
            let n = text.bytes().fold(0i64, |acc, d| {
                acc.saturating_mul(10).saturating_add(i64::from(d - b'0'))
            });
            if op != CompareOp::Eq || (rule.low..=rule.high).contains(&n) {
                return Ok(Test::Part(n));
            }
            return Err(BindCause::OutOfRange(self.problem(
                value,
                format!("{}; `{}` is outside it", self.statement(), value.input),
                None,
            )));
        }
        Err(BindCause::Unreadable(self.problem(
            value,
            format!("{}; `{}` is not one", self.statement(), value.input),
            Some(format!("Type {}", rule.forms)),
        )))
    }

    fn read_bucket(
        &self,
        args: &[f64],
        op: CompareOp,
        value: &InputValue,
    ) -> Result<Test, BindCause> {
        if op != CompareOp::Eq {
            return Err(BindCause::BucketOrdered(self.problem(
                value,
                format!(
                    "a bucket is compared with =, !=, IN or NOT IN; {}",
                    self.statement()
                ),
                None,
            )));
        }
        let text = value.text.as_str();
        let (is_a_range, ranges) = if args.is_empty() {
            (
                is_plain_bucket_label(text),
                "Its ranges are powers of ten, as facts prints them: 0, 1-to-10, 10-to-100, \
                 1K-to-10K, …"
                    .to_string(),
            )
        } else {
            let labels = threshold_bucket_labels(args);
            let printed: Vec<String> = labels.iter().map(|l| printed_word(l)).collect();
            (
                labels.iter().any(|l| l == text),
                format!("Its ranges: {}", printed.join(", ")),
            )
        };
        if is_a_range {
            return Ok(Test::Bucket(text.to_string()));
        }
        Err(BindCause::NotABucketLabel(self.problem(
            value,
            format!("{}; `{}` is not one of them", self.statement(), value.input),
            Some(ranges),
        )))
    }

    fn too_fine(&self, grain: Precision, value: &InputValue) -> BindCause {
        BindCause::TooFine(self.problem(
            value,
            format!(
                "{}; `{}` is finer than a {}",
                self.statement(),
                value.input,
                grain.word()
            ),
            None,
        ))
    }

    /// The clause, not a list, with its value replaced by `new`, the rest as typed.
    fn with_value(&self, value: &InputValue, new: &str) -> String {
        match self.clause.strip_suffix(value.input.as_str()) {
            Some(prefix) => format!("{prefix}{new}"),
            None => format!("{}={new}", self.key.input),
        }
    }
}

/// Whether `value` is the first item of `values` that `picks`, which carries the list's one
/// fix.
fn first_of(
    values: &[InputValue],
    value: &InputValue,
    picks: impl Fn(&InputValue) -> bool,
) -> bool {
    values
        .iter()
        .find(|v| picks(v))
        .is_some_and(|first| std::ptr::eq(first, value))
}

/// **A value quoted for a fix**: in `"…"`, or in `'…'` where the text holds a `"` and no `'`,
/// so no mark is doubled where one need not be.
fn quoted_as_typed(text: &str) -> String {
    if text.contains('"') && !text.contains('\'') {
        format!("'{text}'")
    } else {
        quoted(text)
    }
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

/// The rule line under an unknown key.
const UNKNOWN_KEY_RULE: &str = "`canon facts` lists the keys";

/// **The bottom line**: where the other reading of a value lives, for a refusal whose type
/// mismatches meet a key of type text. It names no clause and rewrites nothing, so it prints
/// once, at the bottom: the modifier always, and the conversion command for each stored key a
/// mismatch read as it is stored, so the command runs as printed. A path key has none, and
/// neither has a bucket, whose range is its own cause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::expr) enum Conversion {
    /// Any key of type text but a stored key read as it is stored: a modifier reads it.
    InFilterOnly,
    /// A stored key read with no accessor and no modifier, its full name: its stored type can
    /// change too.
    WithStoredType(String),
}

/// The bottom line for the stored keys a refusal's mismatches can convert, none or several.
fn conversion_line(stored_keys: &[&str]) -> String {
    let in_filter = "Type conversion: in the filter with a modifier such as |number or |year";
    if stored_keys.is_empty() {
        return format!("{in_filter}.");
    }
    let commands: Vec<String> = stored_keys
        .iter()
        .map(|key| convert_command(key, None))
        .collect();
    format!(
        "{in_filter}; for the stored type, {}.",
        commands.join(" or ")
    )
}

/// A key of mixed type, read by a clause: the clause as typed, where the key sits, the
/// statement, the counts line, the full stored key and its types.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::expr) struct MixedType {
    clause: String,
    place: Place,
    sentence: String,
    counts: String,
    key: String,
    types: KeyTypes,
}

/// Why a filter or key cannot run. `Read` is a problem in what was typed; `UnknownName` a name
/// given alone, outside any expression, that names no key; every other cause but the last two is
/// a typed value or key Canon cannot read for its key, one [`Problem`] each; those two are
/// answers about what a key is.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::expr) enum BindCause {
    Read(Problem),
    UnknownName(String),
    UnknownKey(Problem),
    Unreadable(Problem),
    /// A value that reads as another type for a key of type text or path, and the bottom line
    /// it owes. The problem is boxed so the key the bottom line names keeps every cause small.
    TypeMismatch {
        problem: Box<Problem>,
        conversion: Option<Conversion>,
    },
    OutOfRange(Problem),
    /// A modifier or accessor the key's chain cannot take.
    WrongModifier(Problem),
    GlobNotText(Problem),
    NotABucketLabel(Problem),
    BucketOrdered(Problem),
    TooFine(Problem),
    /// A key of mixed type, read by a clause.
    MixedType(Box<MixedType>),
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
            BindCause::MixedType(_) | BindCause::TypeChanged { .. } => true,
            BindCause::Read(_)
            | BindCause::UnknownName(_)
            | BindCause::UnknownKey(_)
            | BindCause::Unreadable(_)
            | BindCause::TypeMismatch { .. }
            | BindCause::OutOfRange(_)
            | BindCause::WrongModifier(_)
            | BindCause::GlobNotText(_)
            | BindCause::NotABucketLabel(_)
            | BindCause::BucketOrdered(_)
            | BindCause::TooFine(_) => false,
        }
    }

    /// The cause's problem, where it is one.
    pub(in crate::expr) fn problem(&self) -> Option<&Problem> {
        match self {
            BindCause::TypeMismatch { problem, .. } => Some(problem),
            BindCause::Read(problem)
            | BindCause::UnknownKey(problem)
            | BindCause::Unreadable(problem)
            | BindCause::OutOfRange(problem)
            | BindCause::WrongModifier(problem)
            | BindCause::GlobNotText(problem)
            | BindCause::NotABucketLabel(problem)
            | BindCause::BucketOrdered(problem)
            | BindCause::TooFine(problem) => Some(problem),
            BindCause::UnknownName(_) | BindCause::MixedType(_) | BindCause::TypeChanged { .. } => {
                None
            }
        }
    }

    fn problem_mut(&mut self) -> Option<&mut Problem> {
        match self {
            BindCause::TypeMismatch { problem, .. } => Some(problem),
            BindCause::Read(problem)
            | BindCause::UnknownKey(problem)
            | BindCause::Unreadable(problem)
            | BindCause::OutOfRange(problem)
            | BindCause::WrongModifier(problem)
            | BindCause::GlobNotText(problem)
            | BindCause::NotABucketLabel(problem)
            | BindCause::BucketOrdered(problem)
            | BindCause::TooFine(problem) => Some(problem),
            BindCause::UnknownName(_) | BindCause::MixedType(_) | BindCause::TypeChanged { .. } => {
                None
            }
        }
    }

    /// Where the cause points: which text, and where in it. A name given alone and a type
    /// changing while a run reads it point into no text.
    fn place(&self) -> Option<Place> {
        match self {
            BindCause::MixedType(mixed) => Some(mixed.place),
            _ => self.problem().map(|problem| problem.place),
        }
    }

    /// **The cause drawn at an alias's use**, its place being inside the alias's value: the
    /// caret under the alias where it was typed (`name`), the sentence after "in alias
    /// `name`: ", and no fix, since a fix would rewrite the alias's text — the rule line and the
    /// bottom line, which rewrite nothing, kept. A key of mixed type is told as the alias typed,
    /// its clause the alias's name.
    fn at_use(&self, place: Place, name: &str) -> BindCause {
        let mut drawn = self.clone();
        if let BindCause::MixedType(mixed) = &mut drawn {
            mixed.clause = name.to_string();
            mixed.place = place;
        } else if let Some(problem) = drawn.problem_mut() {
            problem.place = place;
            problem.sentence = format!("in alias `{name}`: {}", problem.sentence);
            problem.fix = Fix::None;
        }
        drawn
    }

    /// The cause as it stands in a refusal whose texts begin `by` texts further on.
    fn moved_by(mut self, by: usize) -> BindCause {
        if let BindCause::MixedType(mixed) = &mut self {
            mixed.place.text += by;
        } else if let Some(problem) = self.problem_mut() {
            problem.place.text += by;
        }
        self
    }

    fn render(&self, texts: &[String], indent: &str, out: &mut Vec<String>) {
        let text = |place: Place| texts.get(place.text).map_or("", String::as_str);
        match self {
            BindCause::TypeMismatch { problem, .. } => {
                out.extend(problem.render(text(problem.place), indent))
            }
            BindCause::Read(problem)
            | BindCause::UnknownKey(problem)
            | BindCause::Unreadable(problem)
            | BindCause::OutOfRange(problem)
            | BindCause::WrongModifier(problem)
            | BindCause::GlobNotText(problem)
            | BindCause::NotABucketLabel(problem)
            | BindCause::BucketOrdered(problem)
            | BindCause::TooFine(problem) => {
                out.extend(problem.render(text(problem.place), indent))
            }
            BindCause::UnknownName(name) => {
                out.push(format!("{indent}`{name}` is an unknown key"));
                out.push(format!("{indent}  {UNKNOWN_KEY_RULE}"));
            }
            BindCause::MixedType(mixed) => {
                out.push(format!("{indent}{}: {}", mixed.clause, mixed.sentence));
                out.push(format!("{indent}  {}", mixed.counts));
            }
            BindCause::TypeChanged { key, had_no_facts } => out.push(if *had_no_facts {
                format!(
                    "{indent}{} gained facts while this ran; run it again",
                    printed_key(key)
                )
            } else {
                format!(
                    "{indent}{}'s type changed while this ran; run it again",
                    printed_key(key)
                )
            }),
        }
    }
}

/// What a refusal refuses: a filter given on the command line (`--where`), a key (`facts
/// --key`, `--group-by`), or the filter a manifest specified (its recorded query).
#[derive(Debug, Clone, Copy, PartialEq)]
pub(in crate::expr) enum Refused {
    Filter,
    Key,
    RecordedQuery,
    /// A manifest's pattern, as specified.
    Pattern,
}

/// **An expression Canon will not run**, and every reason, before anything is selected: the
/// texts read, in the order given, and every cause. Pure data with one `Display`: with any
/// input Canon cannot read, the first line naming what was refused, then each cause drawn under
/// the typed line it sits in, in order of text and position (the caller prefixes `Error: `);
/// with answers only, the answers as they stand. Keys of mixed type alone share one way-out
/// block at the end; beside other causes, each key's way out sits under its own entry.
#[derive(Debug, Clone)]
pub struct ExpressionRefused {
    pub(in crate::expr) refused: Refused,
    pub(in crate::expr) texts: Vec<String>,
    /// Where each alias's value among the texts was typed, by the text's number: the place of
    /// `@name`. A text typed as it stands has no entry.
    pub(in crate::expr) typed_at: BTreeMap<usize, Place>,
    pub(in crate::expr) causes: Vec<BindCause>,
}

impl ExpressionRefused {
    /// Whether every cause is an answer — what a key is — so the refusal prints without
    /// `Error:`.
    pub fn is_answer(&self) -> bool {
        self.causes.iter().all(|cause| self.answers(cause))
    }

    /// Whether some cause is an answer about what a key is — beside which a refused manifest's
    /// way back names the conversion.
    pub fn has_answer(&self) -> bool {
        self.causes.iter().any(|cause| self.answers(cause))
    }

    /// Whether one cause of this refusal is an answer.
    fn answers(&self, cause: &BindCause) -> bool {
        match cause {
            // A pattern places by the key's value, so a key of mixed type stops `apply` and is
            // one of the pattern's problems, printed under its first line.
            BindCause::MixedType(_) if self.refused == Refused::Pattern => false,
            other => other.is_answer(),
        }
    }

    /// The refusal of names given alone, outside any expression (`worklist --emit`), that name no
    /// key: one line each, with no typed line to point into.
    pub fn unknown_keys(names: Vec<String>) -> Self {
        ExpressionRefused {
            refused: Refused::Key,
            texts: Vec::new(),
            typed_at: BTreeMap::new(),
            causes: names.into_iter().map(BindCause::UnknownName).collect(),
        }
    }

    /// Two refusals of one run as one, this one's causes first: a filter's first line leads
    /// where either refuses a filter.
    pub fn join(mut self, other: ExpressionRefused) -> Self {
        if self.refused == Refused::Key {
            self.refused = other.refused;
        }
        let by = self.texts.len();
        self.texts.extend(other.texts);
        self.typed_at
            .extend(other.typed_at.into_iter().map(|(text, at)| {
                (
                    text + by,
                    Place {
                        text: at.text + by,
                        ..at
                    },
                )
            }));
        self.causes
            .extend(other.causes.into_iter().map(|cause| cause.moved_by(by)));
        self
    }

    /// One more text for each use of an alias, in the order the uses were met — the alias's
    /// value, typed where the use was. The uses were read with their first text numbered as
    /// the refusal's next ([`AliasMode::Loaded`]).
    pub(in crate::expr) fn push_alias_texts(&mut self, uses: &[AliasUse]) {
        for used in uses {
            self.typed_at.insert(self.texts.len(), used.place);
            self.texts.push(used.value.clone());
        }
    }

    /// Where the text a place sits in was typed, where it is an alias's value.
    fn typed_at(&self, place: Place) -> Option<Place> {
        self.typed_at.get(&place.text).copied()
    }

    /// Every cause at `indent`, without the first line: in order of text and position, each
    /// drawn under the typed line it sits in — a cause inside an alias's value under the alias
    /// where it was typed ([`BindCause::at_use`]), in order of the use and then of its place
    /// in the value. A key of mixed type is told once per clause; the bottom line
    /// ([`Conversion`]) last, once.
    pub fn render(&self, indent: &str) -> Vec<String> {
        let mut causes: Vec<((usize, usize, usize), BindCause)> = self
            .causes
            .iter()
            .map(|cause| match cause.place() {
                None => ((usize::MAX, usize::MAX, 0), cause.clone()),
                Some(place) => match self.typed_at(place) {
                    None => ((place.text, place.start, 0), cause.clone()),
                    Some(at) => {
                        let typed = self.texts.get(at.text).map_or("", String::as_str);
                        let drawn = cause.at_use(at, cut(typed, at.start, at.end));
                        ((at.text, at.start, place.start), drawn)
                    }
                },
            })
            .collect();
        causes.sort_by_key(|(order, _)| *order);
        let mut told: Vec<(&str, &str)> = Vec::new();
        let causes: Vec<&BindCause> = causes
            .iter()
            .map(|(_, cause)| cause)
            .filter(|cause| {
                let BindCause::MixedType(mixed) = cause else {
                    return true;
                };
                let told_as = (mixed.clause.as_str(), mixed.sentence.as_str());
                let new = !told.contains(&told_as);
                told.push(told_as);
                new
            })
            .collect();
        // Keys of mixed type alone share one way-out block at the end; beside any other cause,
        // each one's way out sits under its own entry, so none reads as the entry's above it.
        let only_mixed = causes
            .iter()
            .all(|cause| matches!(cause, BindCause::MixedType(_)));
        let mut lines = Vec::new();
        let mut keys: Vec<(&str, &KeyTypes)> = Vec::new();
        let mut conversion = false;
        let mut stored_keys: Vec<&str> = Vec::new();
        for cause in causes {
            cause.render(&self.texts, indent, &mut lines);
            match cause {
                BindCause::MixedType(mixed) if only_mixed => {
                    if !keys.iter().any(|(k, _)| *k == mixed.key) {
                        keys.push((&mixed.key, &mixed.types));
                    }
                }
                BindCause::MixedType(mixed) => lines.extend(
                    mixed_type_way_out(&[(&mixed.key, &mixed.types)])
                        .into_iter()
                        .map(|l| format!("{indent}  {l}")),
                ),
                BindCause::TypeMismatch {
                    conversion: Some(c),
                    ..
                } => {
                    conversion = true;
                    if let Conversion::WithStoredType(key) = c {
                        if !stored_keys.contains(&key.as_str()) {
                            stored_keys.push(key);
                        }
                    }
                }
                _ => {}
            }
        }
        lines.extend(
            mixed_type_way_out(&keys)
                .into_iter()
                .map(|l| format!("{indent}{l}")),
        );
        if conversion {
            lines.push(format!("{indent}{}", conversion_line(&stored_keys)));
        }
        lines
    }

    /// The first line, naming what was refused.
    fn first_line(&self) -> &'static str {
        match self.refused {
            Refused::Filter => "cannot run the filter as given",
            Refused::Key => "cannot use the key as given",
            Refused::RecordedQuery => "cannot run the filter as specified",
            Refused::Pattern => "cannot use the pattern as specified",
        }
    }
}

impl fmt::Display for ExpressionRefused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let lines = if self.is_answer() {
            self.render("")
        } else {
            let mut lines = vec![self.first_line().to_string()];
            lines.extend(self.render("  "));
            lines
        };
        f.write_str(&lines.join("\n"))
    }
}

impl std::error::Error for ExpressionRefused {}

#[cfg(test)]
mod tests {
    use super::super::alias::CheckedAliases;
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
            ("content.Rating".to_string(), one(FactType::Num)),
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

    /// One filter read and bound against [`types`]: every problem the reader met and every
    /// cause binding found, or the bound tree.
    fn bind(text: &str) -> Result<Bound, Vec<BindCause>> {
        let reading = read_filter(text, 0, AliasMode::NotLoaded);
        let mut causes: Vec<BindCause> =
            reading.problems.into_iter().map(BindCause::Read).collect();
        let Some(expr) = reading.value else {
            return Err(causes);
        };
        let bound = bind_expr(&expr, &types(), AliasMode::NotLoaded, &mut causes);
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
        ExpressionRefused {
            refused: Refused::Filter,
            texts: vec![text.to_string()],
            typed_at: BTreeMap::new(),
            causes: causes(text),
        }
        .to_string()
    }

    // ------------------------------------------------------------------
    // The input types
    // ------------------------------------------------------------------

    /// A value's input type is what it reads as: a number by its written form, whether or not
    /// Canon can hold it; a time; a time of day; and text however it looks.
    #[test]
    fn a_value_that_reads_as_nothing_else_is_text() {
        use InputType::*;
        let cases = [
            ("1024", Number),
            ("-5", Number),
            ("3.14", Number),
            ("1e20", Number),
            ("1e400", Number),
            ("1e-400", Number),
            ("007", Number),
            ("2024", Number),
            ("12345e67", Number),
            ("2024-06", Time),
            ("2024-06-15", Time),
            ("2024:07:23", Time),
            ("2024-06-15T14:30", Time),
            ("2024-06-15T14:30:05.45+02:00", Time),
            ("18:00", TimeOfDay),
            ("14:30:05", TimeOfDay),
            ("23:00:29.000", TimeOfDay),
            ("2024e", Text),
            ("12:3", Text),
            ("2024-06-15_beach.jpg", Text),
            ("1.2.3", Text),
            ("25:99", Text),
            (".5", Text),
            ("5.", Text),
            ("+5", Text),
            ("1e", Text),
            ("1-2", Text),
            ("2023-12-31-05:00", Text),
            ("E-5", Text),
            ("nan", Text),
            ("jpg", Text),
        ];
        for (text, class) in cases {
            assert_eq!(input_type(text), class, "{text}");
        }
    }

    #[test]
    fn against_text_a_value_that_reads_as_another_type_is_the_type_mismatch() {
        for text in [
            "numtext<10",
            "source.rel_path[0]=2019",
            "stem=18:00",
            "textdate>=2024-06",
            "textdate=2024:07:23",
            "source.rel_path[0]=2024-06-15T14:30",
            "content.hash.sha256|short=12345e67",
            "Title=2024-06-15T14:30:05.45+02:00",
            "Title=1e400",
            "Title=1e-400",
        ] {
            assert!(
                matches!(causes(text).as_slice(), [BindCause::TypeMismatch { .. }]),
                "{text}: {:?}",
                causes(text)
            );
        }
        for (text, word) in [
            ("ext=E-5", "E-5"),
            ("ext=3gp", "3gp"),
            ("Title=nan", "nan"),
            ("numtext=.5", ".5"),
            ("numtext=1-2", "1-2"),
            ("stem=5.", "5."),
            ("Title=2023-12-31-05:00", "2023-12-31-05:00"),
            ("Title=25:99", "25:99"),
            ("Title=2024e", "2024e"),
            ("Title=12:3", "12:3"),
            ("Title=2024-06-15_beach.jpg", "2024-06-15_beach.jpg"),
        ] {
            assert_eq!(test_of(text), Test::Text(word.into()), "{text}");
        }
        // Quoted, every value is text; after `~` a glob, whatever its spelling.
        assert_eq!(test_of(r#"numtext="10""#), Test::Text("10".into()));
        assert!(matches!(bind("numtext~1*"), Ok(Bound::Glob { .. })));
    }

    /// **Every type sentence as the table writes it**: each row of the language's table for a
    /// value read as its key's type — the key, the operator and the value typed, and the whole
    /// block the refusal prints: the caret, the sentence, the rule line, the fix, the bottom
    /// line. Expected blocks are the table's words; nothing else is printed.
    #[test]
    fn each_value_refused_for_its_key_prints_the_tables_block() {
        let forms = r#"Type a year, month, day, minute or second: 2024, 2024-06, 2024-06-15, "2024-06-15 14:30""#;
        let stored =
            "Type conversion: in the filter with a modifier such as |number or |year; for \
                      the stored type, `canon facts convert content.Title`.";
        let in_filter = "Type conversion: in the filter with a modifier such as |number or |year.";
        let forms = format!("  {forms}");
        let numbers = format!("  {}", applicable_line(&Gives::Number, false));
        let of_day = format!("  {}", applicable_line(&Gives::TimeOfDay, true));
        let rows: Vec<(&str, usize, String, Vec<&str>)> = vec![
            (
                "filename=2024",
                9,
                r#"filename is of type text, and `2024` is a number. For a text key: filename="2024""#
                    .into(),
                vec![in_filter],
            ),
            (
                "source.rel_path=2024",
                16,
                r#"source.rel_path is of type path, and `2024` is a number. For a path key: source.rel_path="2024""#
                    .into(),
                vec![],
            ),
            (
                "source.rel_path|lowercase=2024",
                26,
                r#"source.rel_path|lowercase is of type path, and `2024` is a number. For a path key: source.rel_path|lowercase="2024""#
                    .into(),
                vec![],
            ),
            (
                "Title<2019",
                6,
                "Title is of type text, and `2019` is a number".into(),
                vec![stored],
            ),
            (
                "Title=2024-06",
                6,
                r#"Title is of type text, and `2024-06` is a time. For a text key: Title="2024-06""#
                    .into(),
                vec![stored],
            ),
            (
                "Title!=14:30",
                7,
                r#"Title is of type text, and `14:30` is a time of day. For a text key: Title!="14:30""#
                    .into(),
                vec![stored],
            ),
            (
                "Rating=big",
                7,
                "Rating is of type number; `big` is not a number".into(),
                vec!["  Type a number: 1024, -5, 3.14, 1e20"],
            ),
            (
                "Rating=1e-400",
                7,
                "Rating is of type number; `1e-400` cannot be represented exactly".into(),
                vec![],
            ),
            (
                "source.mtime=abc",
                13,
                "source.mtime is of type time; `abc` is not a time".into(),
                vec![&forms],
            ),
            (
                "source.mtime=1704067200",
                13,
                "source.mtime is of type time; `1704067200` is not a time: a year is four digits, \
                 and a number never means seconds since 1970"
                    .into(),
                vec![&forms],
            ),
            (
                "source.mtime=14:30",
                13,
                "source.mtime is of type time; `14:30` has no date".into(),
                vec![&forms],
            ),
            (
                "source.mtime|time=x",
                18,
                "source.mtime|time is a time of day; `x` is not a time of day".into(),
                vec!["  Type a time of day: 14:30, 14:30:05"],
            ),
            (
                "source.mtime|year=99",
                18,
                "source.mtime|year is a year, four digits; `99` is not one".into(),
                vec!["  Type a year as four digits: 2024"],
            ),
            (
                "source.mtime|month=13",
                19,
                "source.mtime|month is a month from 1 to 12; `13` is outside it".into(),
                vec![],
            ),
            (
                "source.size|bucket=5",
                19,
                "source.size|bucket is grouped into ranges; `5` is not one of them".into(),
                vec!["  Its ranges are powers of ten, as facts prints them: 0, 1-to-10, 10-to-100, 1K-to-10K, …"],
            ),
            (
                "source.size|bucket(10,100)=5",
                27,
                "source.size|bucket(10,100) is grouped into ranges; `5` is not one of them".into(),
                vec!["  Its ranges: under-10, 10-to-100, 100-and-up"],
            ),
            (
                "source.size|bucket<5",
                19,
                "a bucket is compared with =, !=, IN or NOT IN; source.size|bucket is grouped into \
                 ranges"
                    .into(),
                vec![],
            ),
            (
                "source.mtime~2024*",
                13,
                "`~` works on text; source.mtime is of type time".into(),
                vec![],
            ),
            (
                "source.mtime|yearmonth=2024-06-15",
                23,
                "source.mtime|yearmonth is a month; `2024-06-15` is finer than a month".into(),
                vec![],
            ),
            (
                "source.size|lowercase=x",
                11,
                "|lowercase works on text, paths or another modifier's result; source.size is of \
                 type number"
                    .into(),
                vec![&numbers],
            ),
            (
                "source.mtime|time|year=2024",
                17,
                "|year needs a date; source.mtime|time is a time of day, which has none".into(),
                vec![&of_day],
            ),
            (
                "source.size[0]=1",
                11,
                "an accessor `[…]` works on paths or text; source.size is of type number".into(),
                vec![&numbers],
            ),
            (
                "Rating?=1",
                7,
                "AND or OR is missing before `=1`".into(),
                vec!["  After a condition: AND, OR, ) or the end"],
            ),
            (
                "Nope=1",
                0,
                "`Nope` is an unknown key".into(),
                vec!["  `canon facts` lists the keys"],
            ),
        ];
        for (text, at, sentence, under) in rows {
            let mut expected = vec![
                "cannot run the filter as given".to_string(),
                format!("  {text}"),
                format!("  {}^ {sentence}", " ".repeat(at)),
            ];
            expected.extend(under.iter().map(|line| format!("  {line}")));
            assert_eq!(refusal(text), expected.join("\n"), "{text}");
        }
    }

    /// **The bottom line prints once, in its strongest form**: however many type mismatches a
    /// refusal holds, one line at the bottom — with `canon facts convert` where any of them is a
    /// stored key read as stored, in the filter only otherwise — and none for a path or a
    /// bucket. It fails if the line prints per clause, or the weaker form wins.
    #[test]
    fn the_conversion_line_is_printed_once_naming_each_stored_key() {
        let line = |text: &str| -> Vec<String> {
            refusal(text)
                .lines()
                .filter(|l| l.contains("Type conversion"))
                .map(|l| l.trim_start().to_string())
                .collect()
        };
        let stored =
            "Type conversion: in the filter with a modifier such as |number or |year; for \
                      the stored type, `canon facts convert content.Title`.";
        let both = "Type conversion: in the filter with a modifier such as |number or |year; for \
                      the stored type, `canon facts convert content.Title` or `canon facts \
                      convert content.numtext`.";
        let in_filter = "Type conversion: in the filter with a modifier such as |number or |year.";
        for (text, expected) in [
            ("Title=2025 AND numtext<10 AND Title=2026", vec![both]),
            ("filename=2024 AND Title=2025", vec![stored]),
            ("Title=2025 AND filename=2024", vec![stored]),
            (
                "filename=2024 OR stem=2025 OR filename IN (1, 2)",
                vec![in_filter],
            ),
            ("Title|lowercase=2024", vec![in_filter]),
            ("source.rel_path[0]=2024", vec![in_filter]),
            ("Title IN (2024, 2025)", vec![stored]),
            ("Title<2019 AND Rating=big", vec![stored]),
            ("source.rel_path=2024", vec![]),
            ("source.size|bucket=5", vec![]),
            ("Rating=big", vec![]),
        ] {
            assert_eq!(line(text), expected, "{text}");
        }
        // It sits last, under the blocks, at the refusal's own indent.
        let text = refusal("filename=2024 AND Title=2025");
        assert_eq!(text.lines().last(), Some(&*format!("  {stored}")));
    }

    /// In a list every item that reads as another type has its own caret; the one fix, every
    /// such item quoted, sits on the first.
    #[test]
    fn a_list_has_one_fix_for_the_keys_type_on_its_first_item() {
        assert_eq!(
            refusal("Title IN (2024, x, 2025-06)"),
            [
                "cannot run the filter as given",
                "  Title IN (2024, x, 2025-06)",
                r#"            ^ Title is of type text, and `2024` is a number. For a text key: Title IN ("2024", x, "2025-06")"#,
                "  Title IN (2024, x, 2025-06)",
                "                     ^ Title is of type text, and `2025-06` is a time",
                "  Type conversion: in the filter with a modifier such as |number or |year; for \
                 the stored type, `canon facts convert content.Title`.",
            ]
            .join("\n")
        );
    }

    /// **A list needing quotes for both reasons gets one fix**: every item with a character that
    /// needs quotes and every item that reads as another type is quoted in the one fix, under
    /// the first such item's own sentence and label, so the fix binds where a fix quoting one
    /// reason alone would be refused again.
    #[test]
    fn a_list_needing_quotes_for_both_reasons_gets_one_fix() {
        // A built-in key has no stored type, so the bottom line names the filter only.
        let conversion =
            "  Type conversion: in the filter with a modifier such as |number or |year.";
        assert_eq!(
            refusal("filename IN (2024, a%b)"),
            [
                "cannot run the filter as given",
                "  filename IN (2024, a%b)",
                r#"               ^ filename is of type text, and `2024` is a number. For a text key: filename IN ("2024", "a%b")"#,
                "  filename IN (2024, a%b)",
                "                      ^ `%` needs quotes around the value",
                conversion,
            ]
            .join("\n")
        );
        assert_eq!(
            refusal("filename IN (a%b, 2024, x)"),
            [
                "cannot run the filter as given",
                "  filename IN (a%b, 2024, x)",
                r#"                ^ `%` needs quotes around the value: filename IN ("a%b", "2024", x)"#,
                "  filename IN (a%b, 2024, x)",
                "                    ^ filename is of type text, and `2024` is a number",
                conversion,
            ]
            .join("\n")
        );
    }

    /// Form first, fit second, for a text key too: a value written as a number is a number,
    /// whether or not Canon can hold it, so it gets the fix for a text key.
    #[test]
    fn a_number_form_is_a_number_for_a_text_key_whether_or_not_it_fits() {
        for value in ["1e400", "1e-400", "1.0000000000000001", "2024"] {
            match causes(&format!("filename={value}")).as_slice() {
                [BindCause::TypeMismatch { problem, .. }] => {
                    assert_eq!(
                        problem.fix,
                        Fix::ForKeyType {
                            key_type: TextOrPath::Text,
                            rewrite: format!("filename=\"{value}\""),
                        },
                        "{value}"
                    );
                    assert!(
                        problem
                            .sentence
                            .ends_with(&format!("`{value}` is a number")),
                        "{value}"
                    );
                }
                other => panic!("{value}: {other:?}"),
            }
        }
    }

    /// **A chain is checked on a key with no facts**: its first step takes any type, every later
    /// one the table's, so a chain no key could pass a value through is refused — under `?` and
    /// `NOT`, the one place a key with no facts binds — and one some key could is not.
    #[test]
    fn a_chain_is_checked_on_a_key_with_no_facts() {
        for (text, sentence) in [
            (
                "nothing|year|time?",
                "|time works on times or text; nothing|year is a year, four digits",
            ),
            (
                "NOT nothing|bucket|year?",
                "|year works on times or text; nothing|bucket is grouped into ranges",
            ),
            (
                "nothing[0]|bucket?",
                "|bucket works on numbers; nothing[0] is of type text",
            ),
        ] {
            match causes(text).as_slice() {
                [BindCause::WrongModifier(problem)] => {
                    assert_eq!(problem.sentence, sentence, "{text}")
                }
                other => panic!("{text}: {other:?}"),
            }
        }
        for text in [
            "nothing?",
            "nothing|year?",
            "nothing|bucket?",
            "nothing|lowercase|year?",
            "textdate|lowercase|year?",
            "nothing[0]|number|bucket?",
            "nothing[1:3]|lowercase?",
            "nothing|time|hour?",
        ] {
            assert!(bind(text).is_ok(), "{text}: {:?}", bind(text));
        }
        // A range is never a time, on a key with facts as on one without.
        assert!(matches!(
            causes("source.size|bucket|year?").as_slice(),
            [BindCause::WrongModifier(_)]
        ));
        assert!(bind("source.size|bucket|number?").is_ok());
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

    /// Ordered, a part reads any whole number in its own form, so a bound past its range
    /// means something; `=`, `!=` and `IN` refuse one it can never match. Digits of any length
    /// read without overflow; a year is four digits with every operator.
    #[test]
    fn a_part_reads_any_whole_number_when_ordered() {
        assert_eq!(test_of("source.mtime|hour<24"), Test::Part(24));
        assert_eq!(test_of("source.mtime|month>0"), Test::Part(0));
        assert_eq!(test_of("source.mtime|month=0000000010"), Test::Part(10));
        assert_eq!(
            test_of("source.mtime|hour<99999999999999999999"),
            Test::Part(i64::MAX)
        );
        for text in [
            "source.mtime|month=13",
            "NOT source.mtime|month=13",
            "source.mtime|month IN (1, 13)",
            "source.mtime|hour=99999999999999999999",
        ] {
            assert!(
                matches!(causes(text).as_slice(), [BindCause::OutOfRange(_)]),
                "{text}: {:?}",
                causes(text)
            );
        }
        for text in ["source.mtime|year>999", "source.mtime|year<20240"] {
            assert!(
                matches!(causes(text).as_slice(), [BindCause::Unreadable(_)]),
                "{text}"
            );
        }
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
        for (text, label) in [
            ("source.size|bucket(60,300)=60-to-300", "60-to-300"),
            ("source.size|bucket(60,300)=under-60", "under-60"),
            ("source.size|bucket(60,300)=300-and-up", "300-and-up"),
            ("source.size|bucket(-90,0)=under-minus-90", "under-minus-90"),
            ("source.size|bucket=1K-to-10K", "1K-to-10K"),
            ("source.size|bucket=0", "0"),
            ("source.size|bucket=0.00001-to-0.0001", "0.00001-to-0.0001"),
        ] {
            assert_eq!(test_of(text), Test::Bucket(label.into()), "{text}");
        }
        for text in [
            "source.size|bucket(60,300)=61-to-299",
            "source.size|bucket=2K-to-20K",
        ] {
            assert!(
                matches!(causes(text).as_slice(), [BindCause::NotABucketLabel(_)]),
                "{text}"
            );
        }
        assert!(matches!(
            causes("source.size|bucket(60,300)>60-to-300").as_slice(),
            [BindCause::BucketOrdered(_)]
        ));
    }

    /// A bucket's old labels name no range now: each is refused as not one of its ranges.
    #[test]
    fn an_old_label_is_refused() {
        for text in [
            r#"source.size|bucket(10,100)="<10""#,
            r#"source.size|bucket(10,100)=">100""#,
            "source.size|bucket(10,100)=10-100",
            "source.size|bucket(10,100)=-Inf-10",
            "source.size|bucket(10,100)=100-Inf",
            "source.size|bucket=1-10",
            "source.size|bucket=1K-10K",
            "source.size|bucket=0-0",
        ] {
            assert!(
                matches!(causes(text).as_slice(), [BindCause::NotABucketLabel(_)]),
                "{text}: {:?}",
                causes(text)
            );
        }
        // The rule line lists the ranges in the one printed form.
        assert!(
            refusal(r#"source.size|bucket(10,100)="<10""#)
                .contains("Its ranges: under-10, 10-to-100, 100-and-up"),
            "{}",
            refusal(r#"source.size|bucket(10,100)="<10""#)
        );
    }

    // ------------------------------------------------------------------
    // The modifier table
    // ------------------------------------------------------------------

    /// Every modifier against every input, written out from the rule as stated rather
    /// than read off `result_of`: `T` time, `X` text, `N` number, `P` path, `R` a modifier's
    /// result that is a number (`|month`), `O` a time of day (`|time`), `B` a bucket's range,
    /// which is never a time. A case modifier keeps a path a path.
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
            ("B", Gives::Bucket(Vec::new()), true),
        ];
        for m in Modifier::iter() {
            for (name, input, from_modifier) in &inputs {
                // A time of day (`O`) carries an hour, a minute and a second, never a date.
                let expected = if [Hour, Minute, Second].contains(&m) {
                    matches!(*name, "T" | "X" | "O").then_some(Gives::Part(m))
                } else if time_parts.contains(&m) {
                    matches!(*name, "T" | "X").then_some(Gives::Part(m))
                } else if [Lowercase, Uppercase, Capitalize].contains(&m) && *name == "P" {
                    Some(Gives::Path)
                } else if text_modifiers.contains(&m) {
                    matches!(*name, "X" | "P" | "R" | "O" | "B").then_some(Gives::Text)
                } else {
                    match (m, *name) {
                        (Date, "T" | "X") => Some(Gives::TimeAt(Precision::Day)),
                        (YearMonth, "T" | "X") => Some(Gives::TimeAt(Precision::Month)),
                        (DateTime, "T" | "X") => Some(Gives::Time),
                        (Time, "T" | "X" | "O") => Some(Gives::TimeOfDay),
                        (Number, "X" | "N" | "R" | "B") => Some(Gives::Number),
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
            let chain = apply_chain(saved.clone(), None, &calls, "k", &UserZone::utc()).is_ok();
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
        // No other filter is offered: the sentence and the modifiers that apply, nothing else.
        assert_eq!(
            refusal("FNumber|lowercase=x"),
            [
                "cannot run the filter as given",
                "  FNumber|lowercase=x",
                "         ^ |lowercase works on text, paths or another modifier's result; FNumber \
                 is of type number",
                &format!("    {}", applicable_line(&Gives::Number, false)),
            ]
            .join("\n")
        );
        assert_eq!(
            refusal("source.rel_path|number>=2019").lines().nth(2),
            Some(
                "                 ^ |number works on text or numbers; source.rel_path is of type \
                 path"
            )
        );
        // A text modifier after a modifier reads its printed form, and accessors read
        // text as path segments.
        assert_eq!(
            test_of("source.mtime|weekday|lowercase=monday"),
            Test::Text("monday".into())
        );
        assert_eq!(test_of("SourceFile[-1]=x.jpg"), Test::Text("x.jpg".into()));
        assert!(bind("FNumber|number|lowercase=x").is_ok());
        assert!(bind("source.mtime|datetime|lowercase~2024-06*").is_ok());
        // A time of day has no date: a date-reading modifier after `|time` is refused, an hour
        // is read.
        for text in [
            "source.mtime|time|year=2000",
            "source.mtime|time|weekday=Saturday",
            "source.mtime|time|date=2000-01-01",
            "source.mtime|time|datetime>=2000",
        ] {
            assert!(
                matches!(causes(text).as_slice(), [BindCause::WrongModifier(_)]),
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
                    "|{name} needs a date; source.mtime|time is a time of day, which has none\n"
                )),
                "{said}"
            );
        }
        assert_eq!(test_of("source.mtime|time|hour=14"), Test::Part(14));
        // An accessor on a number is refused.
        assert!(matches!(
            causes("source.size[0]=1").as_slice(),
            [BindCause::WrongModifier(_)]
        ));
    }

    // ------------------------------------------------------------------
    // A typed period finer than a fixed-grain modifier
    // ------------------------------------------------------------------

    /// A time finer than a fixed grain is refused under every operator, saying what the key is
    /// and what the value is finer than; no other modifier is offered.
    #[test]
    fn a_period_finer_than_a_fixed_grain_is_refused_under_every_operator() {
        for (text, sentence) in [
            (
                "taken|yearmonth=2024-06-15",
                "taken|yearmonth is a month; `2024-06-15` is finer than a month",
            ),
            (
                "taken|yearmonth>=2024-06-15",
                "taken|yearmonth is a month; `2024-06-15` is finer than a month",
            ),
            (
                r#"taken|date="2024-06-15 14:30""#,
                r#"taken|date is a day; `"2024-06-15 14:30"` is finer than a day"#,
            ),
            (
                r#"source.rel_path[0]|date>="2024-06-15 14:30""#,
                r#"source.rel_path[0]|date is a day; `"2024-06-15 14:30"` is finer than a day"#,
            ),
            (
                "taken|yearmonth IN (2024-06, 2024-07-15)",
                "taken|yearmonth is a month; `2024-07-15` is finer than a month",
            ),
            (
                "taken|yearmonth=2024:06:15",
                "taken|yearmonth is a month; `2024:06:15` is finer than a month",
            ),
        ] {
            match causes(text).as_slice() {
                [BindCause::TooFine(problem)] => {
                    assert_eq!(problem.sentence, sentence, "{text}");
                    assert_eq!(problem.fix, Fix::None, "{text}");
                    assert_eq!(problem.rule_line, None, "{text}");
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

    /// The refusal of one glob clause: the typed line and a caret under the glob with
    /// `sentence`, and nothing under it.
    fn glob_refusal(text: &str, sentence: &str) -> String {
        let at = text.find('~').map(|at| at + 1).unwrap();
        let rest = cut(text, at, text.len());
        let at = at + rest.len() - rest.trim_start().len();
        [
            "cannot run the filter as given".to_string(),
            format!("  {text}"),
            format!("  {}^ {sentence}", " ".repeat(at)),
        ]
        .join("\n")
    }

    #[test]
    fn a_glob_on_another_type_names_its_type_with_no_fix() {
        for (text, sentence) in [
            (
                "source.size~1*",
                "`~` works on text; source.size is of type number",
            ),
            ("FNumber~2*", "`~` works on text; FNumber is of type number"),
            (
                "source.mtime~2024*",
                "`~` works on text; source.mtime is of type time",
            ),
            (
                "taken|time~14*",
                "`~` works on text; taken|time is a time of day",
            ),
            (
                "taken|year~20*",
                "`~` works on text; taken|year is a year, four digits",
            ),
            (
                "taken|yearmonth~2024*",
                "`~` works on text; taken|yearmonth is a month",
            ),
            (
                "taken|date~2024-06*",
                "`~` works on text; taken|date is a day",
            ),
            (
                "source.size|bucket~1*",
                "`~` works on text; source.size|bucket is grouped into ranges",
            ),
            (
                "source.mtime!~2024*",
                "`!~` works on text; source.mtime is of type time",
            ),
            (
                "source.size !~ 1*",
                "`!~` works on text; source.size is of type number",
            ),
        ] {
            assert!(
                matches!(causes(text).as_slice(), [BindCause::GlobNotText(_)]),
                "{text}"
            );
            assert_eq!(refusal(text), glob_refusal(text, sentence), "{text}");
        }
    }

    /// The type statement, one row per kind of result a key can give. The words for a year, a
    /// month, a day, a month part, a time of day and a range group are the language's sentence
    /// tables'; the other parts read "is" and the range Canon already prints for that part in
    /// its out-of-range sentence ("|hour gives an hour from 0 to 23").
    #[test]
    fn the_type_statement_follows_what_the_key_gives() {
        let rows = [
            (Gives::Text, "k is of type text"),
            (Gives::Path, "k is of type path"),
            (Gives::Number, "k is of type number"),
            (Gives::Time, "k is of type time"),
            (Gives::TimeAt(Precision::Day), "k is a day"),
            (Gives::TimeAt(Precision::Month), "k is a month"),
            (Gives::TimeOfDay, "k is a time of day"),
            (Gives::Part(Modifier::Year), "k is a year, four digits"),
            (Gives::Part(Modifier::Month), "k is a month from 1 to 12"),
            (Gives::Part(Modifier::Day), "k is a day from 1 to 31"),
            (Gives::Part(Modifier::Hour), "k is an hour from 0 to 23"),
            (Gives::Part(Modifier::Minute), "k is a minute from 0 to 59"),
            (Gives::Part(Modifier::Second), "k is a second from 0 to 59"),
            (Gives::Part(Modifier::Week), "k is a week from 1 to 53"),
            (
                Gives::Part(Modifier::Weekday),
                "k is a weekday from 1 to 7, Monday 1",
            ),
            (Gives::Part(Modifier::Quarter), "k is a quarter from 1 to 4"),
            (Gives::Bucket(Vec::new()), "k is grouped into ranges"),
            (Gives::Bucket(vec![10.0]), "k is grouped into ranges"),
        ];
        for (gives, statement) in rows {
            assert_eq!(gives.statement("k"), statement, "{gives:?}");
        }
    }

    #[test]
    fn a_glob_that_needs_quotes_on_another_type_says_only_the_type() {
        assert_eq!(
            refusal("source.mtime~2024@*"),
            glob_refusal(
                "source.mtime~2024@*",
                "`~` works on text; source.mtime is of type time"
            )
        );
        // On a text key the part is named instead, with no fix.
        assert_eq!(
            refusal("Title~2024@*"),
            [
                "cannot run the filter as given",
                "  Title~2024@*",
                "            ^ `@` needs quotes; quote that part, but not * ? [ ]",
            ]
            .join("\n")
        );
        // Where reading stops in the glob, what the reader found is all there is to say, on
        // any key: the condition is never bound.
        for text in ["source.mtime~a%b[c", "Nosuch~a%b[c", "Title~a%b[c"] {
            let at = text.find('%').unwrap();
            assert_eq!(
                refusal(text),
                [
                    "cannot run the filter as given".to_string(),
                    format!("  {text}"),
                    format!(
                        "  {}^ `%` needs quotes; quote that part, but not * ? [ ]",
                        " ".repeat(at)
                    ),
                    format!("  {text}"),
                    format!("  {}^ this set is unclosed", " ".repeat(at + 2)),
                ]
                .join("\n"),
                "{text}"
            );
        }
    }

    // ------------------------------------------------------------------
    // Keys: unknown, mixed type
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
    fn a_mixed_type_key_refuses_every_clause_that_reads_its_value_as_an_answer() {
        for text in [
            r#""content.File Size">1000"#,
            r#""content.File Size"~1*"#,
            r#""content.File Size"|number>=1"#,
            r#""content.File Size"|bucket?"#,
        ] {
            match causes(text).as_slice() {
                [cause @ BindCause::MixedType(_)] => assert!(cause.is_answer(), "{text}"),
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
                r#""content.File Size">1000: "content.File Size" is of mixed type (number and text); a filter supports one type only"#,
                "  numbers: 41,220 facts   text: 380 facts   (every fact of the key, on every root)",
                "To see its values and convert it to one type:",
                "  `canon facts convert 'content.File Size'`",
            ]
            .join("\n")
        );
        let mismatch = refusal("numtext<10");
        assert_eq!(
            mismatch,
            [
                "cannot run the filter as given",
                "  numtext<10",
                "          ^ numtext is of type text, and `10` is a number",
                "  Type conversion: in the filter with a modifier such as |number or |year; for \
                 the stored type, `canon facts convert content.numtext`.",
            ]
            .join("\n")
        );
    }

    #[test]
    fn several_mixed_type_keys_share_one_way_out_block() {
        let refused = ExpressionRefused {
            refused: Refused::Filter,
            texts: Vec::new(),
            typed_at: BTreeMap::new(),
            causes: [
                causes(r#""content.File Size">1"#),
                causes("Width>1"),
                causes(r#""content.File Size"<9"#),
            ]
            .concat(),
        };
        assert!(refused.is_answer());
        let text = refused.to_string();
        assert_eq!(text.matches("To see their values").count(), 1, "{text}");
        assert_eq!(text.matches("canon facts convert").count(), 2, "{text}");
        assert!(
            text.contains("`canon facts convert content.Width`"),
            "{text}"
        );
        // An answer beside input Canon cannot read carries the header.
        let mixed = ExpressionRefused {
            refused: Refused::Filter,
            texts: Vec::new(),
            typed_at: BTreeMap::new(),
            causes: [causes("Width>1"), causes("source.size=x")].concat(),
        };
        assert!(!mixed.is_answer());
        assert!(mixed
            .to_string()
            .starts_with("cannot run the filter as given\n"));
    }

    /// Beside other causes, a mixed-type key's way out sits under its own entry, at the indent
    /// every rule line uses — never at a new entry's, where it would read as the entry's above
    /// it. It fails if the block is shared at the end, as when the key stands alone.
    #[test]
    fn a_mixed_type_way_out_sits_under_its_own_entry_beside_other_causes() {
        assert_eq!(
            refusal(r#"FNumber=garbage AND "content.File Size"=2 AND numtext<10"#),
            [
                "cannot run the filter as given",
                r#"  FNumber=garbage AND "content.File Size"=2 AND numtext<10"#,
                "          ^ FNumber is of type number; `garbage` is not a number",
                "    Type a number: 1024, -5, 3.14, 1e20",
                r#"  "content.File Size"=2: "content.File Size" is of mixed type (number and text); a filter supports one type only"#,
                "    numbers: 41,220 facts   text: 380 facts   (every fact of the key, on every root)",
                "    To see its values and convert it to one type:",
                "      `canon facts convert 'content.File Size'`",
                r#"  FNumber=garbage AND "content.File Size"=2 AND numtext<10"#,
                "                                                        ^ numtext is of type text, and `10` is a number",
                "  Type conversion: in the filter with a modifier such as |number or |year; for \
                 the stored type, `canon facts convert content.numtext`.",
            ]
            .join("\n")
        );
        // Two mixed-type keys beside another cause: each way out under its own entry, none at
        // the end.
        let text = refusal(r#""content.File Size">1 AND source.size=x AND Width>1"#);
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(
            lines.last(),
            Some(&"      `canon facts convert content.Width`"),
            "{text}"
        );
        assert_eq!(text.matches("    To see its values").count(), 2, "{text}");
        assert!(!text.contains("To see their values"), "{text}");
        // The `source.size=x` entry's typed line comes straight after the first key's way out.
        let size = lines
            .iter()
            .position(|l| l.contains("^ source.size is of type number"))
            .unwrap();
        assert_eq!(
            lines[size - 2],
            "      `canon facts convert 'content.File Size'`",
            "{text}"
        );
        // One key read by two clauses beside another cause: each clause's entry carries its
        // own way out; alone, the shared block names it once.
        let twice = refusal("Width=1 AND Width=2 AND source.size=x");
        assert_eq!(twice.matches("    To see its values").count(), 2, "{twice}");
        let alone = refusal("Width=1 AND Width=2");
        assert_eq!(alone.matches("canon facts convert").count(), 1, "{alone}");
    }

    #[test]
    fn every_cause_is_an_answer_or_input_by_its_kind() {
        let place = Place {
            text: 0,
            start: 0,
            end: 1,
        };
        let problem = || Problem::new(place, String::new(), None);
        for cause in [
            BindCause::Read(problem()),
            BindCause::UnknownKey(problem()),
            BindCause::Unreadable(problem()),
            BindCause::TypeMismatch {
                problem: Box::new(problem()),
                conversion: None,
            },
            BindCause::OutOfRange(problem()),
            BindCause::WrongModifier(problem()),
            BindCause::GlobNotText(problem()),
            BindCause::NotABucketLabel(problem()),
            BindCause::BucketOrdered(problem()),
            BindCause::TooFine(problem()),
        ] {
            assert!(!cause.is_answer(), "{cause:?}");
        }
        assert!(BindCause::MixedType(Box::new(MixedType {
            clause: "k=1".into(),
            place,
            sentence: String::new(),
            counts: String::new(),
            key: "content.k".into(),
            types: KeyTypes::from_counts([(FactType::Num, 1), (FactType::Text, 1)]).unwrap(),
        }))
        .is_answer());
        assert!(BindCause::TypeChanged {
            key: "content.k".into(),
            had_no_facts: false,
        }
        .is_answer());
    }

    // ------------------------------------------------------------------
    // Inside an alias
    // ------------------------------------------------------------------

    /// A filter read with `aliases` loaded and bound against [`types`], drawn whole: each use's
    /// value one more text of the refusal, typed where the use was.
    fn alias_refusal(aliases: &[(&str, &str)], text: &str) -> String {
        alias_refused(aliases, text).to_string()
    }

    /// The refusal of `text` read with `aliases`, one text typed.
    fn alias_refused(aliases: &[(&str, &str)], text: &str) -> ExpressionRefused {
        let aliases = CheckedAliases::of(aliases);
        let mode = AliasMode::Loaded {
            aliases: &aliases,
            first_text: 1,
        };
        let reading = read_filter(text, 0, mode);
        let mut causes: Vec<BindCause> =
            reading.problems.into_iter().map(BindCause::Read).collect();
        if let Some(expr) = &reading.value {
            bind_expr(expr, &types(), mode, &mut causes);
        }
        assert!(!causes.is_empty(), "{text} bound");
        let mut refused = ExpressionRefused {
            refused: Refused::Filter,
            texts: vec![text.to_string()],
            typed_at: BTreeMap::new(),
            causes,
        };
        refused.push_alias_texts(&reading.alias_uses);
        refused
    }

    /// Two refusals joined keep each alias's value under the place it was typed: the second's
    /// texts and where they were typed move past the first's together.
    #[test]
    fn a_joined_refusal_keeps_each_alias_under_its_use() {
        let aliases = [("x", "numtext<10")];
        let joined = alias_refused(&aliases, "@x").join(alias_refused(&aliases, "Title=a OR @x"));
        let lines = joined.render("");
        assert_eq!(
            lines,
            [
                "@x",
                "^ in alias `@x`: numtext is of type text, and `10` is a number",
                "Title=a OR @x",
                "           ^ in alias `@x`: numtext is of type text, and `10` is a number",
                "Type conversion: in the filter with a modifier such as |number or |year; for the \
                 stored type, `canon facts convert content.numtext`.",
            ],
            "{joined}"
        );
    }

    fn drawn(lines: &[&str]) -> String {
        lines.join("\n")
    }

    #[test]
    fn binding_names_what_it_found_inside_an_alias() {
        let one = |value: &str| alias_refusal(&[("x", value)], "Title=a AND @x");
        let under = |sentence: &str| {
            drawn(&[
                "cannot run the filter as given",
                "  Title=a AND @x",
                &format!("              ^ in alias `@x`: {sentence}"),
            ])
        };
        assert_eq!(
            one("filename=a%b"),
            under("`%` needs quotes around the value")
        );
        assert_eq!(
            one("filename=IMG*"),
            under("`*` is a wildcard, but `=` does not take wildcards")
        );
        assert_eq!(
            one("size~1*"),
            under("`~` works on text; size is of type number")
        );
        assert_eq!(
            one("filename~a%*"),
            under("`%` needs quotes; quote that part, but not * ? [ ]")
        );
    }

    #[test]
    fn every_place_inside_an_alias_points_at_its_use() {
        // Each use draws its own problem under itself, in the order of the typed text.
        assert_eq!(
            alias_refusal(&[("x", "filename=a%b")], "@x AND FNumber=abc AND @x"),
            drawn(&[
                "cannot run the filter as given",
                "  @x AND FNumber=abc AND @x",
                "  ^ in alias `@x`: `%` needs quotes around the value",
                "  @x AND FNumber=abc AND @x",
                "                 ^ FNumber is of type number; `abc` is not a number",
                "    Type a number: 1024, -5, 3.14, 1e20",
                "  @x AND FNumber=abc AND @x",
                "                         ^ in alias `@x`: `%` needs quotes around the value",
            ])
        );
        // A key alias's own accessor sits in the alias; the line naming what applies stays.
        let text = alias_refusal(&[("t", "taken[0]")], "Title=a OR @t?");
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(
            lines,
            [
                "cannot run the filter as given",
                "  Title=a OR @t?",
                "             ^ in alias `@t`: an accessor `[…]` works on paths or text; taken is of type time",
                &format!("    {}", applicable_line(&Gives::Time, false)),
            ]
        );
    }

    #[test]
    fn inside_an_alias_nothing_that_rewrites_text_is_shown_and_pointers_stay() {
        // A type mismatch's fix rewrites the clause and is left out; the bottom line stays.
        assert_eq!(
            alias_refusal(&[("x", "numtext=10")], "@x"),
            drawn(&[
                "cannot run the filter as given",
                "  @x",
                "  ^ in alias `@x`: numtext is of type text, and `10` is a number",
                "  Type conversion: in the filter with a modifier such as |number or |year; for \
                 the stored type, `canon facts convert content.numtext`.",
            ])
        );
        // A rule line stays.
        assert_eq!(
            alias_refusal(&[("x", "Nope=1")], "@x"),
            drawn(&[
                "cannot run the filter as given",
                "  @x",
                "  ^ in alias `@x`: `Nope` is an unknown key",
                "    `canon facts` lists the keys",
            ])
        );
        // A wrong modifier names the modifiers that apply.
        assert_eq!(
            alias_refusal(&[("x", "taken|lowercase=a")], "@x"),
            drawn(&[
                "cannot run the filter as given",
                "  @x",
                "  ^ in alias `@x`: |lowercase works on text, paths or another modifier's result; \
                 taken is of type time",
                &format!("    {}", applicable_line(&Gives::Time, false)),
            ])
        );
        // A problem's fix is left out, its rule line kept.
        let refused = ExpressionRefused {
            refused: Refused::Filter,
            texts: vec!["@x".to_string(), "content.Foo%Bar=1".to_string()],
            typed_at: BTreeMap::from([(
                1,
                Place {
                    text: 0,
                    start: 0,
                    end: 2,
                },
            )]),
            causes: vec![BindCause::Read(Problem {
                place: Place {
                    text: 1,
                    start: 11,
                    end: 12,
                },
                sentence: "an operator is missing after `content.Foo`".to_string(),
                rule_line: Some("After a key: =, !=, <, >, <=, >=, ~, !~, ?, IN or NOT IN".into()),
                fix: Fix::KeyName {
                    rewrite: "\"content.Foo%Bar\"".to_string(),
                },
                also: None,
            })],
        };
        assert_eq!(
            refused.to_string(),
            drawn(&[
                "cannot run the filter as given",
                "  @x",
                "  ^ in alias `@x`: an operator is missing after `content.Foo`",
                "    After a key: =, !=, <, >, <=, >=, ~, !~, ?, IN or NOT IN",
            ])
        );
        // A key alias's own modifier is named by the alias as typed.
        assert_eq!(
            alias_refusal(&[("d", "taken|yearmonth")], "@d=2024-07-23"),
            drawn(&[
                "cannot run the filter as given",
                "  @d=2024-07-23",
                "     ^ @d is a month; `2024-07-23` is finer than a month",
            ])
        );
        assert_eq!(
            alias_refusal(&[("b", "FNumber|bucket")], "@b>5"),
            drawn(&[
                "cannot run the filter as given",
                "  @b>5",
                "     ^ a bucket is compared with =, !=, IN or NOT IN; @b is grouped into ranges",
            ])
        );
    }

    #[test]
    fn a_sentence_names_the_key_by_where_its_cause_sits() {
        // The key alias's own key, unknown, by its name as the alias writes it.
        assert_eq!(
            alias_refusal(&[("t", "content.DateTimeOriginal")], "@t=2024"),
            drawn(&[
                "cannot run the filter as given",
                "  @t=2024",
                "  ^ in alias `@t`: `content.DateTimeOriginal` is an unknown key",
                "    `canon facts` lists the keys",
            ])
        );
        // Its wrong value by the alias as typed.
        let text = alias_refusal(&[("taken", "taken")], "@taken=garbage");
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(
            lines[..3],
            [
                "cannot run the filter as given",
                "  @taken=garbage",
                "         ^ @taken is of type time; `garbage` is not a time",
            ]
        );
        assert!(
            lines[3].starts_with("    Type a year, month, day"),
            "{text}"
        );
        assert_eq!(
            alias_refusal(&[("n", "numtext")], "@n<10"),
            drawn(&[
                "cannot run the filter as given",
                "  @n<10",
                "     ^ @n is of type text, and `10` is a number",
                "  Type conversion: in the filter with a modifier such as |number or |year; for \
                 the stored type, `canon facts convert content.numtext`.",
            ])
        );
        // A modifier inside the alias is named with the key as the alias writes it, up to the
        // modifier; one typed after the alias, with the alias as typed.
        assert_eq!(
            alias_refusal(&[("t", "taken|time|year")], "@t?").lines().nth(2),
            Some("  ^ in alias `@t`: |year needs a date; taken|time is a time of day, which has none")
        );
        assert_eq!(
            alias_refusal(&[("t", "taken|time")], "@t|year?")
                .lines()
                .nth(2),
            Some("    ^ |year needs a date; @t is a time of day, which has none")
        );
        // Its mixed type by the alias as typed; a filter alias's by its own key.
        assert!(alias_refusal(&[("w", "Width")], "@w=2024").starts_with(
            "@w=2024: @w is of mixed type (number and time); a filter supports one type only"
        ));
        assert!(alias_refusal(&[("old", "Width=5")], "@old").starts_with(
            "@old: Width is of mixed type (number and time); a filter supports one type only"
        ));
    }

    #[test]
    fn the_mixed_type_answer_starts_with_the_clause_as_typed() {
        let answer = drawn(&[
            "@old: Width is of mixed type (number and time); a filter supports one type only",
            "  numbers: 2 facts   times: 1 fact   (every fact of the key, on every root)",
            "To see its values and convert it to one type:",
            "  `canon facts convert content.Width`",
        ]);
        assert_eq!(alias_refusal(&[("old", "Width=5")], "@old"), answer);
        // The same clause twice is told once.
        assert_eq!(
            alias_refusal(&[("old", "Width=5")], "@old OR (@old AND Title=a)"),
            answer
        );
        // A key alias's clause is the typed clause.
        assert_eq!(
            alias_refusal(&[("w", "Width")], "@w=2024 OR @w=2025"),
            drawn(&[
                "@w=2024: @w is of mixed type (number and time); a filter supports one type only",
                "  numbers: 2 facts   times: 1 fact   (every fact of the key, on every root)",
                "@w=2025: @w is of mixed type (number and time); a filter supports one type only",
                "  numbers: 2 facts   times: 1 fact   (every fact of the key, on every root)",
                "To see its values and convert it to one type:",
                "  `canon facts convert content.Width`",
            ])
        );
    }

    /// **A key with no facts** is unknown wherever a value is read — compared, placed or
    /// reported, with a modifier or without; only `?` binds it, as asking whether a value exists.
    #[test]
    fn a_key_with_no_facts_is_unknown_except_to_ask_whether_it_exists() {
        use super::super::read::{read_keys, AliasMode, Grammar};
        let none = HashMap::new();
        for text in ["content.Nope", "content.Nope|year"] {
            let key = read_keys(text, 0, Grammar::Key, AliasMode::NotLoaded)
                .value
                .unwrap()
                .remove(0);
            for reads in [Reads::Report, Reads::Value, Reads::Pattern] {
                assert!(
                    matches!(
                        bind_key(&key, text, reads, &none),
                        Err(BindCause::UnknownKey(_))
                    ),
                    "{text}"
                );
            }
            let (bound, _) = bind_key(&key, text, Reads::Existence, &none).unwrap();
            let want = if key.has_transforms() {
                ReadAs::Absent
            } else {
                ReadAs::Any
            };
            assert_eq!(bound.read_as, want, "{text}");
        }
    }
}
