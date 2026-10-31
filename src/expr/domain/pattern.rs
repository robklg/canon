//! **The shaping half**: a manifest's pattern, read, checked and evaluated — literal text and
//! `{…}`, each holding one key read by the one key grammar (`{source.rel_path[-1]}`,
//! `{content.DateTimeOriginal|year}`), placed by the type of its value.

use std::collections::HashMap;

use super::alias::alias_name_at;
use super::bind::{read_as_changed, BoundKey, ExpressionRefused, Gives, ReadAs, Refused};
use super::key::{
    read_key, single_quoted, InputKey, Key, SourceFact, CONTENT_HASH, SCOPE_REL_PATH,
};
use super::print::printed_key;
use super::problem::{cut, Fix, Place, Problem};
use super::read::{is_space, key_position, read_quoted, Cursor, Grammar, Reading};
use super::transform::{
    apply_chain, no_value_sentence, path_segments, rendered, InstantRecord, Modified, Modifier,
    ModifierCall, NoValueCause, PathAccessor,
};
use super::value::{source_row_value, SourceAttributes};
use crate::core::domain::fact::{FactEntry, FactValue};
use crate::core::domain::format::format_count;
use crate::core::domain::format::with_visible_nulls;
use crate::core::domain::path::{ContainedPath, NotContained};
use crate::core::domain::time::value::Precision;
use crate::core::domain::time::zone::UserZone;

// ============================================================================
// Types
// ============================================================================

/// The facts fetched for a run of pattern evaluation, keyed by source.
///
/// The invariant, and the reason this is a noun rather than a `HashMap`: **no
/// key in here is one the evaluation context supplies itself.** The prefetch
/// that builds it drops those before asking the database, so a stored fact
/// cannot reach evaluation wearing a computed key's name — which is what
/// would silently move where files land.
///
/// Before this type existed the guarantee was a copy of one `if` at four
/// sites, each carrying a comment saying all four had to agree and nothing
/// holding them to it. Wrapping the map is what turns "they agree" from a
/// thing to check into a thing that cannot be otherwise: the only way to fill
/// one in production is the prefetch that enforces it.
pub struct PatternFacts(HashMap<i64, Vec<FactEntry>>);

impl PatternFacts {
    /// Wrap a map the prefetch has already filtered. Facility-private: the
    /// invariant belongs to the prefetch, and this is how it hands its result
    /// over.
    pub(in crate::expr) fn new(facts: HashMap<i64, Vec<FactEntry>>) -> Self {
        PatternFacts(facts)
    }

    /// The stored facts fetched for one source, or nothing if it had none.
    pub fn for_source(&self, id: i64) -> &[FactEntry] {
        self.0.get(&id).map_or(&[], Vec::as_slice)
    }

    /// The one named escape: a test that wants a specific map, including the
    /// hostile ones that plant a context-supplied key deliberately.
    /// `cfg(test)` is what makes the invariant build-refused rather than
    /// convention-held — production has no route to this constructor at all.
    #[cfg(test)]
    pub fn from_entries(facts: HashMap<i64, Vec<FactEntry>>) -> Self {
        PatternFacts(facts)
    }
}

/// Whether the pattern evaluation context answers this key itself.
///
/// **The context-supplied law**: a key this returns true for is never read
/// from the facts table. Spelled once, here, and unreachable from outside the
/// facility on purpose — exporting it would mean three callers each having to
/// remember to call it, which is the failure mode being repaired rather than
/// a repair of it. What consumers get instead is `prefetch_pattern_facts`,
/// which applies it for them.
///
/// Two halves, and only one of them is derived. The derived half reads
/// `Key::is_computed`, which reads the resolver. The other two are named
/// literally because the context answers them without the source row:
/// `scope.rel_path`, which names no key, comes off the lock entry's
/// measurement, and the content hash — what `{hash}`, `{hash_short}` and
/// `{content.hash.sha256}` read — off the hash the lock entry recorded. That
/// half is pinned rather than derived, and the pins say so.
pub(in crate::expr) fn is_context_supplied(key: &str) -> bool {
    key == SCOPE_REL_PATH || key == CONTENT_HASH || Key::read(key).is_some_and(|k| k.is_computed())
}

/// Why a source carries no scope-relative measurement.
///
/// An absent measurement has two possible causes and they take different
/// answers, so an evaluation that cannot tell them apart must assert neither.
/// It is a property of the **lock as a whole**, not of the entry, which is why
/// the caller supplies it rather than the entry carrying it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Unmeasured {
    /// The lock's header records no scope, so there was nothing to measure
    /// from when it was built. A fact about the lock, never the manifest: the
    /// manifest may declare a scope since, and a refresh then measures the
    /// lock from it — whether to name that refresh is for the one reader that
    /// resolves the scope, the report, to say or withhold. While the manifest
    /// declares none, `{scope.rel_path}` has no meaning for this run.
    NoScopeRecorded,
    /// The lock was written before the measurement was recorded in it. A
    /// refresh rebuilds it and the key resolves.
    LockPredatesMeasurement,
}

/// Context for pattern evaluation - provides fact values and source info
pub struct EvalContext<'a> {
    /// Fact values by key, stored rather than computed. Under the
    /// context-supplied law nothing in here is a key this context answers
    /// itself, so a lookup falling through to this map cannot shadow a
    /// computed key.
    facts: HashMap<String, FactValue>,
    /// What the source itself answers, which is every computed key. The
    /// shaping half reaches the same resolver the asking half does; before it
    /// did, it held two path strings and could derive three keys from them.
    source: Option<SourceAttributes>,
    /// This source's scope-relative path, as the run that selected it settled
    /// it. Borrowed from the caller's own record rather than derived here: the
    /// measurement is a property of the *selection*, and by the time a pattern
    /// is expanded the selection is long settled.
    scope_rel: Option<&'a str>,
    /// Why [`scope_rel`](Self::scope_rel) is absent, when it is. Carried
    /// beside it rather than inferred from it, because the two causes are
    /// indistinguishable from the entry alone and a refusal that guesses
    /// between them prescribes a remedy that may not work.
    unmeasured: Unmeasured,
    /// The clock every time is read on, through the frame rule.
    clock: &'a UserZone,
    /// Whether an evaluation read an instant on that clock.
    instant_read: InstantRecord,
    /// The content hash the lock entry recorded, which the hash keys read.
    lock_hash: Option<String>,
}

impl<'a> EvalContext<'a> {
    pub fn new(clock: &'a UserZone) -> Self {
        EvalContext {
            facts: HashMap::new(),
            source: None,
            scope_rel: None,
            // A bare context has no lock behind it and no scope in front of
            // it, which is exactly this reading.
            unmeasured: Unmeasured::NoScopeRecorded,
            clock,
            instant_read: InstantRecord::default(),
            lock_hash: None,
        }
    }

    /// Give the context the content hash its lock entry recorded: what `{hash}`, `{hash_short}`
    /// and `{content.hash.sha256}` read, under the context-supplied law.
    pub fn set_lock_hash(&mut self, hash: Option<String>) {
        self.lock_hash = hash;
    }

    /// Set a fact value (should be properly typed from database lookup)
    pub fn set_fact(&mut self, key: &str, value: FactValue) {
        self.facts.insert(key.to_string(), value);
    }

    /// Point the context at the source it is evaluating for.
    ///
    /// One setter rather than one per attribute: the attributes arrive
    /// together and half a source is not a state any caller means to be in.
    pub fn set_source(&mut self, attrs: SourceAttributes) {
        self.source = Some(attrs);
    }

    /// Give the context this source's settled scope-relative path, and — for
    /// the case where there is none — the run's own reason there is none.
    ///
    /// One setter for both because they are one fact in two parts: half of it
    /// is a state no caller means to be in.
    pub fn set_scope_rel(&mut self, scope_rel: Option<&'a str>, unmeasured: Unmeasured) {
        self.scope_rel = scope_rel;
        self.unmeasured = unmeasured;
    }
}

// ============================================================================
// The pattern grammar
// ============================================================================

/// A pattern as read: its literal text, and each `{…}`'s key.
#[derive(Debug, Clone)]
pub(in crate::expr) enum InputPiece {
    Literal(String),
    Key(Box<PatternInput>),
}

/// One `{…}` as read: its key, whether it names `scope.rel_path` (which names no key and is
/// answered by the lock entry), and the `{…}` as specified, which a sentence about its value
/// names.
#[derive(Debug, Clone)]
pub(in crate::expr) struct PatternInput {
    pub key: InputKey,
    pub scope: bool,
    pub clause: String,
}

/// What a pattern's text divides into before any key is read.
enum Span {
    Literal {
        start: usize,
        end: usize,
    },
    /// `{…}`: the bytes of the braces.
    Braces {
        open: usize,
        close: usize,
    },
}

/// Where a pattern's text stops being readable: a `{` or a quote that never closes holds the
/// rest of the text.
enum Unclosed {
    Brace(usize),
    Quote(usize),
}

/// **The pattern grammar**: literal text, written as it is, and `{…}`, each holding one key read
/// by the key grammar ([`read_key`]) as `--key` reads its argument: a bare name runs to its `[`
/// or `|`, a space inside it the name's, and spaces at either end of the `{…}` are not the key's;
/// a quoted name may hold a `}`. Literal text may not start or end with `/`, hold `//`, or have
/// `.` or `..` as a whole name; a `}` with no `{` is refused. `@name` is refused. Every problem is
/// recorded; reading stops only at a `{` or a quote that never closes.
pub(in crate::expr) fn read_pattern_text(text: &str) -> Reading<Vec<InputPiece>> {
    let mut cursor = Cursor::new(text, 0);
    let (spans, unclosed) = divide(text);
    check_literal_text(&mut cursor, text, &spans, unclosed.is_none());
    let mut pieces = Vec::new();
    for span in &spans {
        match span {
            Span::Literal { start, end } => {
                pieces.push(InputPiece::Literal(cut(text, *start, *end).to_string()))
            }
            Span::Braces { open, close } => {
                if let Some(input) = read_braces(&mut cursor, text, *open, *close) {
                    pieces.push(InputPiece::Key(Box::new(input)));
                }
            }
        }
    }
    match unclosed {
        Some(Unclosed::Brace(at)) => {
            cursor.problem(at, at + 1, "`{` has no matching `}`", None, Fix::None);
            cursor.stop();
        }
        Some(Unclosed::Quote(at)) => {
            cursor.problem(at, at + 1, "this quote is unclosed", None, Fix::None);
            cursor.stop();
        }
        None => {}
    }
    cursor.into_reading(Some(pieces))
}

/// Divide a pattern's text into literal text and `{…}`, finding each `}` past a quoted name that
/// may hold one.
fn divide(text: &str) -> (Vec<Span>, Option<Unclosed>) {
    let mut spans = Vec::new();
    let mut literal_start = 0;
    let mut at = 0;
    while let Some(c) = cut(text, at, text.len()).chars().next() {
        if c != '{' {
            at += c.len_utf8();
            continue;
        }
        if literal_start < at {
            spans.push(Span::Literal {
                start: literal_start,
                end: at,
            });
        }
        let open = at;
        let char_at = |i: usize| cut(text, i, text.len()).chars().next();
        let mut i = open + 1;
        while let Some(c) = char_at(i).filter(|&c| is_space(c)) {
            i += c.len_utf8();
        }
        if let Some(quote @ ('"' | '\'')) = char_at(i) {
            let mut scan = Cursor::new(text, 0);
            scan.set_at(i);
            if read_quoted(&mut scan).is_none() {
                return (spans, Some(Unclosed::Quote(i)));
            }
            debug_assert_eq!(char_at(i), Some(quote));
            i = scan.at();
        }
        let close = loop {
            match char_at(i) {
                None => return (spans, Some(Unclosed::Brace(open))),
                Some('}') => break i,
                Some(c) => i += c.len_utf8(),
            }
        };
        spans.push(Span::Braces { open, close });
        at = close + 1;
        literal_start = at;
    }
    if literal_start < text.len() {
        spans.push(Span::Literal {
            start: literal_start,
            end: text.len(),
        });
    }
    (spans, None)
}

/// The literal text's own problems: a `}` with no `{`, a leading or trailing `/`, `//`, and a
/// whole name `.` or `..`. `ends` is whether the last span ends the pattern, which a `{` that
/// never closes does not.
fn check_literal_text(cursor: &mut Cursor, text: &str, spans: &[Span], ends: bool) {
    let count = spans.len();
    for (index, span) in spans.iter().enumerate() {
        let Span::Literal { start, end } = *span else {
            continue;
        };
        let first = index == 0;
        let last = ends && index + 1 == count;
        let literal = cut(text, start, end);
        for (offset, c) in literal.char_indices() {
            let at = start + offset;
            if c == '}' {
                cursor.problem(at, at + 1, "`}` has no matching `{`", None, Fix::None);
            }
            if c == '/' && cut(literal, offset + 1, literal.len()).starts_with('/') {
                let sentence = "an empty name between `//` cannot be a directory name";
                cursor.problem(at, at + 2, sentence, None, Fix::None);
            }
        }
        if first && literal.starts_with('/') {
            let sentence = "a pattern cannot start with `/`";
            cursor.problem(start, start + 1, sentence, None, Fix::None);
        }
        if last && literal.ends_with('/') {
            let sentence = "a pattern cannot end with `/`";
            cursor.problem(end - 1, end, sentence, None, Fix::None);
        }
        let names: Vec<&str> = literal.split('/').collect();
        let mut name_start = start;
        for (k, name) in names.iter().enumerate() {
            let whole = (k > 0 || first) && (k + 1 < names.len() || last);
            if whole && is_dot_name(name) {
                // The pattern's own last name is where the file's name goes.
                let what = if last && k + 1 == names.len() {
                    "a file or directory name"
                } else {
                    "a directory name"
                };
                let sentence = format!("`{name}` cannot be {what}");
                cursor.problem(
                    name_start,
                    name_start + name.len(),
                    sentence,
                    None,
                    Fix::None,
                );
            }
            name_start += name.len() + 1;
        }
    }
}

/// Read the key inside one `{…}`, recording its problems at the places they were typed. The key
/// is read as `--key` reads its argument: from the text between the braces, spaces at both ends
/// left out, every place put back by where that text starts. `None` where no key is read, or text
/// follows it.
fn read_braces(cursor: &mut Cursor, text: &str, open: usize, close: usize) -> Option<PatternInput> {
    let inside = cut(text, open + 1, close);
    let read = inside.trim_matches(is_space);
    if read.is_empty() {
        let sentence = "a key is missing between `{` and `}`";
        cursor.problem(open, close + 1, sentence, None, Fix::None);
        return None;
    }
    // Where the text read starts in the pattern: a place read from it moves by this much.
    let from = open + 1 + (inside.len() - inside.trim_start_matches(is_space).len());
    let put_back = |cursor: &mut Cursor, problems: Vec<Problem>| {
        for mut problem in problems {
            moved(&mut problem.place, from);
            for at in problem.also.iter_mut().flat_map(|also| also.iter_mut()) {
                *at += from;
            }
            cursor.push_problem(problem);
        }
    };
    let mut sub = Cursor::new(read, 0);
    if sub.peek() == Some('@') {
        if let Some(alias) = alias_name_at(cut(read, 1, read.len())) {
            let end = 1 + alias.len();
            let sentence = format!("`@{alias}` is an alias; a pattern cannot contain aliases");
            sub.problem(0, end, sentence, None, Fix::None);
            put_back(cursor, sub.into_reading::<()>(None).problems);
            return None;
        }
    }
    let key = read_key(&mut sub, Grammar::Pattern);
    let mut follows = false;
    if let Some(key) = &key {
        key_position(&mut sub, key, Grammar::Pattern);
        sub.skip_spaces();
        let rest_start = sub.at();
        if rest_start < read.len() {
            follows = true;
            let sentence = format!(
                "`{}` cannot follow `{}`",
                cut(read, rest_start, read.len()),
                key.input
            );
            sub.problem(rest_start, read.len(), sentence, None, Fix::None);
        }
    }
    let scope = key.as_ref().is_some_and(|key| key.names(SCOPE_REL_PATH));
    // A key the reader refused — a status word, text after it — is not bound as well.
    let read_whole = !follows && sub.problem_count() == 0;
    put_back(cursor, sub.into_reading::<()>(None).problems);
    let mut key = key.filter(|_| read_whole)?;
    key.each_place(&mut |place| moved(place, from));
    if scope {
        key.base = SCOPE_REL_PATH.to_string();
    }
    Some(PatternInput {
        key,
        scope,
        clause: cut(text, open, close + 1).to_string(),
    })
}

/// A place read from a text that starts at byte `from` of the pattern, put where it was typed.
fn moved(place: &mut Place, from: usize) {
    place.start += from;
    place.end += from;
}

// ============================================================================
// The bound pattern
// ============================================================================

/// **A pattern read and checked**: every `{…}`'s key bound against the stored keys' types, so
/// what it places by is known before any source is looked at. Made only by
/// `expr::read_pattern`, and only where nothing was refused; [`evaluate`] takes nothing
/// else, so an unchecked pattern cannot place a file.
#[derive(Debug, Clone)]
pub struct BoundPattern {
    pieces: Vec<Piece>,
}

#[derive(Debug, Clone)]
enum Piece {
    Literal(String),
    Key(PatternKey),
}

/// One `{…}`, bound: the `{…}` as specified, the base key a value is read from, its accessor and
/// modifiers, what a value met may be ([`ReadAs`]), what its chain gives, and whether it is
/// `scope.rel_path`.
#[derive(Debug, Clone)]
struct PatternKey {
    input: String,
    base: String,
    accessor: Option<PathAccessor>,
    modifiers: Vec<ModifierCall>,
    read_as: ReadAs,
    gives: Gives,
    scope: bool,
}

impl PatternKey {
    /// Whether its value may make more than one directory: a path.
    fn may_nest(&self) -> bool {
        self.gives == Gives::Path
    }
}

impl BoundPattern {
    /// Bind a pattern read without a problem, every key with its binding.
    pub(in crate::expr) fn new(pieces: Vec<(InputPiece, Option<(BoundKey, Gives)>)>) -> Self {
        let pieces = pieces
            .into_iter()
            .map(|(piece, bound)| match (piece, bound) {
                (InputPiece::Literal(text), _) => Piece::Literal(text),
                (InputPiece::Key(input), Some((bound, gives))) => Piece::Key(PatternKey {
                    input: input.clause,
                    base: bound.base,
                    accessor: bound.accessor,
                    modifiers: bound.modifiers,
                    read_as: bound.read_as,
                    gives,
                    scope: input.scope,
                }),
                (InputPiece::Key(_), None) => unreachable!("every key of a bound pattern is bound"),
            })
            .collect();
        BoundPattern { pieces }
    }

    fn keys(&self) -> impl Iterator<Item = &PatternKey> {
        self.pieces.iter().filter_map(|piece| match piece {
            Piece::Key(key) => Some(key),
            Piece::Literal(_) => None,
        })
    }

    /// The keys a value is read from, one per `{…}` naming one, the scope key left out — what
    /// the prefetch is given, which drops the rest the evaluation context answers itself.
    pub fn stored_keys(&self) -> Vec<String> {
        self.keys()
            .filter(|key| !key.scope)
            .map(|key| key.base.clone())
            .collect()
    }

    /// Whether this pattern places files relative to the manifest's scope.
    ///
    /// Asked by a caller that has to tell two failures apart: a pattern the scope cannot answer,
    /// and a pattern naming a fact no source carries. They take opposite advice, so a caller
    /// that cannot tell them apart must give advice that is wrong for half its readers. **The
    /// facility answers it, not the caller**: which key means scope-relative placement is a fact
    /// about the language.
    pub fn names_scope_rel_path(&self) -> bool {
        self.keys().any(|key| key.scope)
    }

    /// The directory every placement shares: the literal text before the first `{…}`, up to its
    /// last `/` — `photos/2024/{filename}` places everything in `photos/2024`. `None` where the
    /// pattern commits to none. Nothing is cleaned: a pattern read without a problem has no
    /// leading `/`, no `//` and no `.` or `..` name in its literal text.
    pub fn leading_directory(&self) -> Option<&str> {
        let Some(Piece::Literal(head)) = self.pieces.first() else {
            return None;
        };
        let cut_at = head.rfind('/')?;
        #[expect(clippy::string_slice, reason = "an `rfind` position")]
        Some(&head[..cut_at])
    }

    /// Whether placements may open directories below [`Self::leading_directory`]: a `/` in the
    /// literal text after the first `{…}`, or a `{…}` whose value is a path (`{source.rel_path}`
    /// is `trip/day1/x.jpg`), which the pattern text alone does not show. It answers *can*, not
    /// *will*: reading a fan-out as flat would name one directory where files land all over the
    /// tree, while the other way round only costs a hedge.
    pub fn may_nest(&self) -> bool {
        let mut seen_key = false;
        self.pieces.iter().any(|piece| match piece {
            Piece::Key(key) => {
                seen_key = true;
                key.may_nest()
            }
            Piece::Literal(text) => seen_key && text.contains('/'),
        })
    }
}

// ============================================================================
// Where a value lands
// ============================================================================

/// Where a source lands, and whether placing it read an instant on the clock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placement {
    pub path: ContainedPath,
    pub used_an_instant: bool,
}

/// Why a source has no place.
#[derive(Debug, Clone)]
pub enum NotPlaced {
    /// Every cause of every `{…}` for this source.
    NoPlace(NoPlace),
    /// The finished destination would leave the directory it is placed in.
    NotContained {
        destination: String,
        why: NotContained,
    },
    /// A key's type changed while the run read it: an answer, never a source with no place.
    TypeChanged(ExpressionRefused),
}

/// Every cause one source's `{…}` met.
#[derive(Debug, Clone, PartialEq)]
pub struct NoPlace {
    pub causes: Vec<NoPlaceCause>,
}

/// **Why a `{…}` gives a source no place**, each with its sentence and the query entry its
/// remedy names. `subject` is the `{…}` as specified.
#[derive(Debug, Clone, PartialEq)]
pub enum NoPlaceCause {
    /// A stored key the source has no value for.
    NoFact { subject: String, key: String },
    /// A value Canon works out that is absent: no extension, an index past the end, text a
    /// modifier cannot read, a number below zero under plain `|bucket`.
    NoValue {
        subject: String,
        reason: Option<String>,
    },
    /// The file's save time lies where Canon reads no time.
    UnreadableSaveTime,
    /// `scope.rel_path`, and the lock records no scope to measure from.
    NoScopeRecorded,
    /// `scope.rel_path`, and the lock was written before the measurement was recorded in it.
    LockPredatesMeasurement,
    /// A time written coarser than its modifier reads.
    TooCoarse {
        subject: String,
        modifier: Modifier,
        precision: Precision,
    },
    /// A text value holding `/`.
    ContainsSlash { subject: String, value: String },
    /// An empty text value.
    Empty { subject: String },
    /// A text value that is `.` or `..`, or a path value with such a name (`in_path`).
    DotName {
        subject: String,
        value: String,
        in_path: bool,
    },
    /// A path with no segments stands in the destination's last name and leaves it empty or
    /// `.`, every other `{…}` in that name having given a usable value: no file name is left.
    NoSegments { subject: String },
    /// A value holding a null character.
    NullChar { subject: String },
}

/// What a value that stands in the destination as a name is: never told which, since a name
/// that cannot be one cannot be either.
const A_NAME: &str = "a file or directory name";

impl NoPlaceCause {
    /// The cause as one source's sentence.
    pub fn sentence(&self) -> String {
        match self {
            NoPlaceCause::NoFact { subject, .. } => format!("{subject} has no value"),
            NoPlaceCause::NoValue { subject, reason } => match reason {
                Some(reason) => format!("{subject} has no value: {reason}"),
                None => format!("{subject} has no value"),
            },
            NoPlaceCause::UnreadableSaveTime => {
                "Canon cannot read this file's save time: it lies at \
                                                   the edge of the years 0000–9999 or beyond"
                    .to_string()
            }
            NoPlaceCause::NoScopeRecorded => {
                format!("{SCOPE_REL_PATH} is not available: the selected sources were not scoped")
            }
            NoPlaceCause::LockPredatesMeasurement => format!(
                "{SCOPE_REL_PATH} is not available: this lock file was written before the \
                 scope-relative path was recorded in it."
            ),
            NoPlaceCause::TooCoarse {
                subject,
                modifier,
                precision,
            } => no_value_sentence(
                subject,
                NoValueCause::TooCoarse {
                    modifier: *modifier,
                    precision: *precision,
                },
            ),
            NoPlaceCause::ContainsSlash { subject, value } => format!(
                "{subject} is `{}`: {A_NAME} cannot contain `/`",
                with_visible_nulls(value)
            ),
            NoPlaceCause::Empty { subject } => {
                format!("{subject} is empty: {A_NAME} cannot be empty")
            }
            NoPlaceCause::DotName {
                subject,
                value,
                in_path,
            } => {
                let is = if *in_path { "contains" } else { "is" };
                format!("{subject} {is} `{value}`: {A_NAME} cannot be `{value}`")
            }
            NoPlaceCause::NoSegments { subject } => {
                format!("{subject} has no segments: a file name cannot be empty")
            }
            NoPlaceCause::NullChar { subject } => {
                format!("{subject} contains a null character: {A_NAME} cannot contain one")
            }
        }
    }

    /// The cause as a count line: the sentence with the count in front ("12 sources: …", "1
    /// source: …"). Where the sentence quotes a value, which differs per source, the line names
    /// what the sources share instead: the character `/`.
    pub fn count_line(&self, sources: usize) -> String {
        let count = if sources == 1 {
            "1 source".to_string()
        } else {
            format!("{} sources", format_count(sources as u64))
        };
        let sentence = match self {
            NoPlaceCause::ContainsSlash { subject, .. } => {
                format!("{subject} contains `/`: {A_NAME} cannot contain `/`")
            }
            other => other.sentence(),
        };
        format!("{count}: {sentence}")
    }

    /// The query entry a remedy names to leave such sources out (`content.Make?`,
    /// `source.rel_path[0]|month?`), where the remedy names one: a key with no value, bare where
    /// the printing rule prints it bare and otherwise in `'…'`, the quote a manifest string
    /// keeps; or the `{…}` as specified without its braces.
    pub fn query_entry(&self) -> Option<String> {
        let inner = |subject: &str| {
            let inner = subject
                .strip_prefix('{')
                .and_then(|s| s.strip_suffix('}'))
                .unwrap_or(subject);
            format!("{inner}?")
        };
        match self {
            NoPlaceCause::NoFact { key, .. } => {
                let printed = printed_key(key);
                let name = if printed == *key {
                    printed
                } else {
                    single_quoted(key)
                };
                Some(format!("{name}?"))
            }
            NoPlaceCause::NoValue { subject, .. } | NoPlaceCause::TooCoarse { subject, .. } => {
                Some(inner(subject))
            }
            NoPlaceCause::UnreadableSaveTime => {
                Some(format!("{}?", Key::SourceFact(SourceFact::Mtime).name()))
            }
            NoPlaceCause::NoScopeRecorded
            | NoPlaceCause::LockPredatesMeasurement
            | NoPlaceCause::ContainsSlash { .. }
            | NoPlaceCause::Empty { .. }
            | NoPlaceCause::DotName { .. }
            | NoPlaceCause::NoSegments { .. }
            | NoPlaceCause::NullChar { .. } => None,
        }
    }
}

/// Whether a name is `.` or `..`, which a path reads as a directory rather than as a name: the
/// one test the pattern's literal text, a text value and a path's segments are each held to.
fn is_dot_name(name: &str) -> bool {
    matches!(name, "." | "..")
}

/// One `{…}`'s value for a source, as it joins the destination.
#[derive(Clone)]
enum Value {
    /// One name, in its one printed form.
    Text(String),
    /// A directory per segment.
    Path(Vec<String>),
}

/// A name standing in for a `{…}` that gave no value or a refused one, where the question is
/// what the rest of the pattern would leave: any ordinary name answers it.
const ORDINARY_NAME: &str = "x";

/// A pattern's pieces joined with their values: the finished text, which values stand in it, and
/// where each path with no segments stood.
struct Joined {
    out: String,
    stands: Vec<usize>,
    empty_paths: Vec<(usize, usize)>,
}

/// Join `pieces` with `values`: a path with no segments adds nothing, and the `/` after it goes
/// with it where keeping it would leave `//` or a leading `/`; a `{…}` with no value adds
/// nothing.
fn join(pieces: &[Piece], values: &[Option<Value>]) -> Joined {
    let mut out = String::new();
    let mut drop_slash = false;
    let mut stands = Vec::new();
    let mut empty_paths = Vec::new();
    for (index, (piece, value)) in pieces.iter().zip(values).enumerate() {
        match (piece, value) {
            (Piece::Literal(text), _) => {
                let text = match text.strip_prefix('/') {
                    Some(rest) if drop_slash => rest,
                    _ => text.as_str(),
                };
                out.push_str(text);
            }
            (Piece::Key(_), Some(Value::Path(segments))) if segments.is_empty() => {
                empty_paths.push((index, out.len()));
                drop_slash = out.is_empty() || out.ends_with('/');
                continue;
            }
            (Piece::Key(_), Some(Value::Path(segments))) => {
                out.push_str(&segments.join("/"));
                stands.push(index);
            }
            (Piece::Key(_), Some(Value::Text(text))) => {
                out.push_str(text);
                stands.push(index);
            }
            (Piece::Key(_), None) => {}
        }
        drop_slash = false;
    }
    Joined {
        out,
        stands,
        empty_paths,
    }
}

/// **Evaluate a bound pattern for one source**: each `{…}`'s value, then where it lands. A path
/// value makes one directory per segment and a path with no segments none, the `/` joining it to
/// the rest going with it only where keeping it would leave `//` or a leading `/`; any other
/// value makes exactly one name, checked whole before it joins. Every cause of every `{…}` is
/// collected; the finished destination must be a [`ContainedPath`].
pub fn evaluate(pattern: &BoundPattern, ctx: &EvalContext) -> Result<Placement, NotPlaced> {
    let mut causes = Vec::new();
    let mut values: Vec<Option<Value>> = Vec::new();
    for piece in &pattern.pieces {
        let Piece::Key(key) = piece else {
            values.push(None);
            continue;
        };
        match key_value(key, ctx) {
            Ok(value) => values.push(Some(value)),
            Err(KeyOutcome::Cause(cause)) => {
                causes.push(cause);
                values.push(None);
            }
            Err(KeyOutcome::TypeChanged(refusal)) => return Err(NotPlaced::TypeChanged(refusal)),
        }
    }

    // Join, recording where each value stands in the finished text.
    let Joined {
        out,
        stands,
        empty_paths: _,
    } = join(&pattern.pieces, &values);

    let mut refused: Vec<usize> = Vec::new();
    for index in stands {
        let Piece::Key(key) = &pattern.pieces[index] else {
            continue;
        };
        let subject = || key.input.clone();
        match &values[index] {
            Some(Value::Text(text)) => {
                let cause = if text.contains('/') {
                    Some(NoPlaceCause::ContainsSlash {
                        subject: subject(),
                        value: text.clone(),
                    })
                } else if text.is_empty() {
                    Some(NoPlaceCause::Empty { subject: subject() })
                } else if is_dot_name(text) {
                    Some(NoPlaceCause::DotName {
                        subject: subject(),
                        value: text.clone(),
                        in_path: false,
                    })
                } else if text.contains('\0') {
                    Some(NoPlaceCause::NullChar { subject: subject() })
                } else {
                    None
                };
                if cause.is_some() {
                    refused.push(index);
                }
                causes.extend(cause);
            }
            Some(Value::Path(segments)) => {
                let found = segments.iter().find_map(|segment| {
                    if is_dot_name(segment) {
                        Some(NoPlaceCause::DotName {
                            subject: subject(),
                            value: segment.clone(),
                            in_path: true,
                        })
                    } else if segment.contains('\0') {
                        Some(NoPlaceCause::NullChar { subject: subject() })
                    } else {
                        None
                    }
                });
                if found.is_some() {
                    refused.push(index);
                }
                causes.extend(found);
            }
            None => {}
        }
    }

    // A path with no segments leaves no file name where, had every other `{…}` given an
    // ordinary name, it would stand in the last name and that name would be empty or `.`,
    // which a path reads as its directory. Asked of that join and never of the real one: a
    // `{…}` that gave no value, or a refused one, already says its own cause, and the name
    // must not read short because of it.
    let ordinary: Vec<Option<Value>> = pattern
        .pieces
        .iter()
        .zip(&values)
        .enumerate()
        .map(|(index, (piece, value))| match (piece, value) {
            (Piece::Key(_), None) => Some(Value::Text(ORDINARY_NAME.to_string())),
            (Piece::Key(_), Some(_)) if refused.contains(&index) => {
                Some(Value::Text(ORDINARY_NAME.to_string()))
            }
            _ => value.clone(),
        })
        .collect();
    let would_join = join(&pattern.pieces, &ordinary);
    let file_name_start = would_join.out.rfind('/').map_or(0, |slash| slash + 1);
    if matches!(
        cut(&would_join.out, file_name_start, would_join.out.len()),
        "" | "."
    ) {
        let in_file_name = would_join
            .empty_paths
            .iter()
            .rev()
            .find(|(_, at)| *at >= file_name_start);
        if let Some((index, _)) = in_file_name {
            if let Piece::Key(key) = &pattern.pieces[*index] {
                causes.push(NoPlaceCause::NoSegments {
                    subject: key.input.clone(),
                });
            }
        }
    }

    if !causes.is_empty() {
        return Err(NotPlaced::NoPlace(NoPlace { causes }));
    }
    let path = ContainedPath::new(&out).map_err(|why| NotPlaced::NotContained {
        destination: out.clone(),
        why,
    })?;
    Ok(Placement {
        path,
        used_an_instant: ctx.instant_read.read(),
    })
}

/// Why a `{…}` has no value to join.
enum KeyOutcome {
    Cause(NoPlaceCause),
    TypeChanged(ExpressionRefused),
}

/// One `{…}`'s value for the context's source: read, then through its accessor and modifiers.
fn key_value(key: &PatternKey, ctx: &EvalContext) -> Result<Value, KeyOutcome> {
    let subject = || key.input.clone();
    let cause = |cause| Err(KeyOutcome::Cause(cause));
    let value = if key.scope {
        match ctx.scope_rel {
            Some(scope_rel) => FactValue::Path(scope_rel.to_string()),
            None => {
                return cause(match ctx.unmeasured {
                    Unmeasured::NoScopeRecorded => NoPlaceCause::NoScopeRecorded,
                    Unmeasured::LockPredatesMeasurement => NoPlaceCause::LockPredatesMeasurement,
                })
            }
        }
    } else if key.base == CONTENT_HASH {
        // The lock is the settled record `apply` carries out: its hash, never the facts
        // table's (the context-supplied law).
        match &ctx.lock_hash {
            Some(hash) => FactValue::Text(hash.clone()),
            None => {
                return cause(NoPlaceCause::NoFact {
                    subject: subject(),
                    key: key.base.clone(),
                })
            }
        }
    } else if let Some(computed) = Key::read(&key.base).filter(Key::is_computed) {
        let found = ctx
            .source
            .as_ref()
            .and_then(|source| source_row_value(source, &computed));
        match found {
            Some(value) => value,
            None => {
                return cause(match computed.as_source_fact() {
                    Some(SourceFact::Mtime) => NoPlaceCause::UnreadableSaveTime,
                    _ => NoPlaceCause::NoValue {
                        subject: subject(),
                        reason: None,
                    },
                })
            }
        }
    } else {
        match ctx.facts.get(&key.base) {
            Some(value) => {
                if let Some(refusal) =
                    read_as_changed(key.read_as, &key.base, value, Refused::Pattern)
                {
                    return Err(KeyOutcome::TypeChanged(refusal));
                }
                value.clone()
            }
            None => {
                return cause(NoPlaceCause::NoFact {
                    subject: subject(),
                    key: key.base.clone(),
                })
            }
        }
    };
    let chained = match apply_chain(
        value,
        key.accessor.as_ref(),
        &key.modifiers,
        &key.input,
        ctx.clock,
    ) {
        Ok(chained) => chained,
        // Binding checked the chain against the key's type, and a value of another type stopped
        // the run above, so the chain cannot refuse; were it to, the source has no place.
        Err(error) => {
            return cause(NoPlaceCause::NoValue {
                subject: subject(),
                reason: Some(error.to_string()),
            })
        }
    };
    ctx.instant_read.note(chained.read_an_instant);
    match chained.value {
        Modified::Value(FactValue::Path(path)) => Ok(Value::Path(
            path_segments(&path)
                .into_iter()
                .map(str::to_string)
                .collect(),
        )),
        Modified::Value(value) => Ok(Value::Text(rendered(
            key.modifiers.last().map(|call| call.modifier),
            &value,
            ctx.clock,
        ))),
        Modified::NoValue(NoValueCause::TooCoarse {
            modifier,
            precision,
        }) => cause(NoPlaceCause::TooCoarse {
            subject: subject(),
            modifier,
            precision,
        }),
        Modified::NoValue(other) => cause(NoPlaceCause::NoValue {
            subject: subject(),
            reason: other.reason(),
        }),
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::super::bind::bind_pattern_key;
    use super::super::transform::Modifier;
    use super::*;
    use crate::core::domain::fact::{FactType, KeyTypes};

    /// The stored keys these tests name, with the type each one's facts hold.
    fn types() -> HashMap<String, KeyTypes> {
        [
            ("content.Make", FactType::Text),
            ("content.Title", FactType::Text),
            ("content.Neg", FactType::Num),
            ("content.DateTimeOriginal", FactType::Time),
            ("content.CreateDate", FactType::Time),
        ]
        .into_iter()
        .map(|(key, t)| (key.to_string(), KeyTypes::from_counts([(t, 1)]).unwrap()))
        .collect()
    }

    /// A pattern read and bound against [`types`], with no database: it must bind.
    fn bound(text: &str) -> BoundPattern {
        let reading = read_pattern_text(text);
        assert!(
            reading.problems.is_empty(),
            "{text}: {:?}",
            reading.problems
        );
        let types = types();
        let pieces = reading
            .value
            .unwrap()
            .into_iter()
            .map(|piece| {
                let binding = match &piece {
                    InputPiece::Key(input) => Some(
                        bind_pattern_key(&input.key, input.scope, &input.clause, &types)
                            .unwrap_or_else(|cause| panic!("{text}: {cause:?}")),
                    ),
                    InputPiece::Literal(_) => None,
                };
                (piece, binding)
            })
            .collect();
        BoundPattern::new(pieces)
    }

    fn placed(text: &str, ctx: &EvalContext) -> String {
        match evaluate(&bound(text), ctx) {
            Ok(placement) => placement.path.as_str().to_string(),
            Err(not) => panic!("{text}: {not:?}"),
        }
    }

    fn causes(text: &str, ctx: &EvalContext) -> Vec<NoPlaceCause> {
        match evaluate(&bound(text), ctx) {
            Err(NotPlaced::NoPlace(no_place)) => no_place.causes,
            other => panic!("{text}: {other:?}"),
        }
    }

    fn said(text: &str, ctx: &EvalContext) -> Vec<String> {
        causes(text, ctx)
            .iter()
            .map(NoPlaceCause::sentence)
            .collect()
    }

    /// The predicate a consumer leans on to choose between two opposite
    /// remedies, so it is walked over the shapes that could fool a spelling
    /// match: the key named plainly, named with an accessor, named beside
    /// other keys, and a literal that merely contains its text.
    #[test]
    fn a_pattern_naming_the_scope_key_is_recognised_by_the_facility() {
        for pattern in [
            "{scope.rel_path}",
            "{scope.rel_path}/{filename}",
            "{content.Make}/{scope.rel_path}",
            "{scope.rel_path[0]}",
        ] {
            assert!(
                bound(pattern).names_scope_rel_path(),
                "names the key: {pattern}"
            );
        }
        for pattern in [
            "{source.rel_path}",
            "{content.Make}/{filename}",
            "scope.rel_path/{filename}",
            "{filename}",
        ] {
            assert!(
                !bound(pattern).names_scope_rel_path(),
                "does not name the key: {pattern}"
            );
        }
    }

    /// A source's settled scope-relative path, as the run that selected it
    /// measured it and wrote it into the lock. Evaluation reads this; it does
    /// not compute it.
    fn measured(scope_rel: &str) -> Option<&str> {
        Some(scope_rel)
    }

    /// A source at `root` + `rel`, with the remaining attributes fixed. The
    /// mtime is 2024-06-15 12:00:00 UTC — the same instant `value.rs`'s
    /// display pin already asserts renders as `2024-06-15`.
    fn source(root: &str, rel: &str) -> SourceAttributes {
        SourceAttributes {
            id: 42,
            root_id: 1,
            root_path: root.to_string(),
            rel_path: rel.to_string(),
            size: 1024000,
            mtime: 1718452800,
            device: 16777220,
            inode: 12345678,
        }
    }

    /// The common case: a source under a root nothing else in the test cares
    /// about.
    fn at(rel: &str) -> SourceAttributes {
        source("/root", rel)
    }

    fn context<'a>(zone: &'a UserZone, file: SourceAttributes) -> EvalContext<'a> {
        let mut ctx = EvalContext::new(zone);
        ctx.set_source(file);
        ctx
    }

    #[test]
    fn test_evaluate_simple() {
        let zone = UserZone::utc();
        let ctx = context(&zone, at("photos/2024/image.jpg"));
        assert_eq!(placed("{filename}", &ctx), "image.jpg");
    }

    #[test]
    fn test_evaluate_path_index() {
        let zone = UserZone::utc();
        let ctx = context(&zone, at("photos/2024/vacation/image.jpg"));
        assert_eq!(placed("{source.rel_path[-2]}", &ctx), "vacation");
    }

    #[test]
    fn test_evaluate_path_slice() {
        let zone = UserZone::utc();
        let ctx = context(&zone, at("photos/2024/vacation/image.jpg"));
        assert_eq!(placed("{source.rel_path[0:2]}", &ctx), "photos/2024");
    }

    #[test]
    fn test_evaluate_time_modifier() {
        // The mtime comes from the source, not from a planted fact: the production path
        // plants none.
        let zone = UserZone::utc();
        let ctx = context(&zone, at("photos/image.jpg"));
        assert_eq!(placed("{source.mtime|year}", &ctx), "2024");
    }

    /// **One printed form for a bucket**: the label a pattern places as a directory is the
    /// label a `facts` report counts and the label a rejection lists as one of the ranges.
    #[test]
    fn a_bucket_label_is_the_same_in_a_report_a_rejection_and_a_directory() {
        use crate::expr::domain::key::ParsedFactKey;
        use crate::expr::domain::print::threshold_bucket_labels;
        let zone = UserZone::utc();
        let ctx = context(&zone, at("photos/image.jpg")); // 1,024,000 bytes
        for (key, label) in [
            ("source.size|bucket", "1M-to-10M"),
            ("source.size|bucket(1000,1000000)", "1000000-and-up"),
            ("source.size|bucket(2000000)", "under-2000000"),
        ] {
            assert_eq!(
                placed(&format!("{{{key}}}/{{filename}}"), &ctx),
                format!("{label}/image.jpg"),
                "{key}"
            );
            let reported = ParsedFactKey::parse(key)
                .unwrap()
                .report_value(FactValue::Num(1_024_000.0), &zone)
                .unwrap();
            let Modified::Value(reported) = reported else {
                panic!("{key}: no value")
            };
            assert_eq!(rendered(Some(Modifier::Bucket), &reported, &zone), label);
        }
        // A rejection lists the same labels as its ranges.
        assert!(
            threshold_bucket_labels(&[1000.0, 1_000_000.0]).contains(&"1000000-and-up".to_string())
        );
    }

    /// A text modifier after a part reads the part's printed form, so a pattern places the
    /// directories it always placed: `monday/`, `q2/`, `06/`.
    #[test]
    fn a_text_modifier_after_a_part_places_its_printed_form() {
        let zone = UserZone::utc();
        let mut file = at("photos/image.jpg");
        file.mtime = 1718582400; // 2024-06-17 00:00:00 UTC, a Monday
        let ctx = context(&zone, file);
        for (text, landed) in [
            (
                "{source.mtime|weekday|lowercase}/{filename}",
                "monday/image.jpg",
            ),
            (
                "{source.mtime|quarter|lowercase}/{filename}",
                "q2/image.jpg",
            ),
            ("{source.mtime|month|short}/{filename}", "06/image.jpg"),
            ("{source.mtime|weekday}/{source.mtime|month}", "Monday/06"),
        ] {
            assert_eq!(placed(text, &ctx), landed, "{text}");
        }
    }

    #[test]
    fn test_evaluate_stem_modifier() {
        let zone = UserZone::utc();
        let ctx = context(&zone, at("photos/image.jpg"));
        assert_eq!(placed("{stem}", &ctx), "image");
    }

    /// `{ext}` is the built-in key, `source.ext`'s value, as in a filter.
    #[test]
    fn test_evaluate_ext_modifier() {
        let zone = UserZone::utc();
        let ctx = context(&zone, at("photos/image.JPG"));
        assert_eq!(placed("{ext}", &ctx), "JPG");
        assert_eq!(placed("{ext|lowercase}", &ctx), "jpg");
    }

    #[test]
    fn test_out_of_bounds_error() {
        let zone = UserZone::utc();
        let ctx = context(&zone, at("photos/image.jpg"));
        assert_eq!(
            said("{source.rel_path[10]}", &ctx),
            ["{source.rel_path[10]} has no value: the path has no such segment"]
        );
    }

    #[test]
    fn test_scope_rel_path() {
        let zone = UserZone::utc();
        let mut ctx = context(&zone, source("/Photos", "Home/2024/vacation/image.jpg"));
        ctx.set_scope_rel(
            measured("2024/vacation/image.jpg"),
            Unmeasured::NoScopeRecorded,
        );
        assert_eq!(placed("{scope.rel_path}", &ctx), "2024/vacation/image.jpg");
    }

    /// Sibling scopes still measure from their shared parent, so each scope's own name
    /// survives and the ancestors above it do not come along — but that is settled when the
    /// selection is, and evaluation reads the settled value rather than re-deriving it from a
    /// scope that may have been edited since. The rule itself is `ScopeVantage`'s and is
    /// pinned there; what this pins is that placement is a **lookup**.
    #[test]
    fn scope_rel_path_is_the_measurement_the_lock_recorded() {
        for rel in ["work/proj-v1/src/main.c", "work/proj-v2/src/main.c"] {
            let recorded = rel.trim_start_matches("work/");
            let zone = UserZone::utc();
            let mut ctx = context(&zone, source("/vol", rel));
            ctx.set_scope_rel(measured(recorded), Unmeasured::NoScopeRecorded);
            assert_eq!(placed("{scope.rel_path}", &ctx), recorded);
        }
    }

    /// The path-law pin, moved with the strip it guards. A byte-prefix test would strip
    /// `/vol/photos2` with `/vol/photos` and hand back `2/x.jpg`; containment through its
    /// owner refuses instead, so a source the vantage does not contain is recorded with **no**
    /// measurement, and placement then has none rather than inventing one.
    #[test]
    fn a_sibling_named_like_the_scope_is_not_under_it() {
        use crate::core::domain::path::path_strip_prefix;
        assert_eq!(path_strip_prefix("/vol/photos2/x.jpg", "/vol/photos"), None);

        let zone = UserZone::utc();
        let mut ctx = context(&zone, source("/vol", "photos2/x.jpg"));
        ctx.set_scope_rel(None, Unmeasured::LockPredatesMeasurement);
        assert_eq!(
            causes("{scope.rel_path}", &ctx),
            [NoPlaceCause::LockPredatesMeasurement]
        );
    }

    /// The two causes an absent measurement can mean, each saying its own thing. The
    /// distinction is load-bearing: the two take different answers, and the answer is not
    /// this facility's to give — so each states its own reason and **neither names a remedy**.
    #[test]
    fn an_unmeasured_entry_is_refused_by_the_reason_it_is_unmeasured() {
        let zone = UserZone::utc();

        let mut old_lock = context(&zone, source("/media/backup", "proj-v1/src/main.c"));
        old_lock.set_scope_rel(None, Unmeasured::LockPredatesMeasurement);
        let [err] = said("{scope.rel_path}", &old_lock).try_into().unwrap();
        assert!(err.contains("written before"), "{err}");
        assert!(!err.contains("were not scoped"), "{err}");
        assert!(
            !err.contains("cluster refresh"),
            "a refresh rebuilds this lock but need not make the manifest \
             applicable, and this facility cannot see which: {err}"
        );

        let mut unscoped = context(&zone, source("/media/backup", "proj-v1/src/main.c"));
        unscoped.set_scope_rel(None, Unmeasured::NoScopeRecorded);
        let [err] = said("{scope.rel_path}", &unscoped).try_into().unwrap();
        assert!(
            err.contains("the selected sources were not scoped"),
            "the fact is the lock's, never the manifest's, which may declare one since: {err}"
        );
        assert!(
            !err.contains("cluster refresh"),
            "a refresh cannot give an unscoped manifest a scope: {err}"
        );
    }

    /// And there is no fallback behind the refusal. A destination is the one decision a user
    /// cannot un-decide after a move, so an entry with no measurement must never quietly
    /// become the source's root-relative path, its file name, or the empty string.
    #[test]
    fn an_unmeasured_entry_never_falls_back_to_a_destination() {
        let zone = UserZone::utc();
        let mut ctx = context(&zone, source("/vol", "work/proj-v1/src/main.c"));
        ctx.set_scope_rel(None, Unmeasured::NoScopeRecorded);
        assert!(evaluate(&bound("{scope.rel_path}"), &ctx).is_err());
        assert!(evaluate(&bound("{scope.rel_path}/{filename}"), &ctx).is_err());
    }

    /// A name with no extension has no value for `{source.ext}`, so the pattern places the
    /// file nowhere, where it once placed it with the directory level left out.
    #[test]
    fn a_name_with_no_extension_has_no_place_under_its_extension() {
        let zone = UserZone::utc();
        for rel in ["README", ".bashrc", "trail."] {
            let ctx = context(&zone, at(rel));
            assert_eq!(
                said("photos/{source.ext}/{filename}", &ctx),
                ["{source.ext} has no value"],
                "{rel:?}"
            );
        }
        let ctx = context(&zone, at("a/photo.JPG"));
        assert_eq!(
            placed("photos/{source.ext}/{filename}", &ctx),
            "photos/JPG/photo.JPG"
        );
    }

    /// **A pattern's no-value sentence names the expression as specified**, braces included,
    /// and never the key alone, which may well have a value: `{filename|ext}` and `{ext}` of
    /// `README`, `{content.Neg|bucket}` of -50, `{content.Title|number}` of `Canon`, each with
    /// its reason where it has one.
    #[test]
    fn a_no_value_sentence_names_each_expression_as_specified() {
        let zone = UserZone::utc();
        let mut ctx = context(&zone, at("a/README"));
        ctx.set_fact("content.Neg", FactValue::Num(-50.0));
        ctx.set_fact("content.Title", FactValue::Text("Canon".into()));
        for (pattern, sentence) in [
            ("{filename|ext}/{filename}", "{filename|ext} has no value"),
            ("{ext}/{filename}", "{ext} has no value"),
            ("{ext|lowercase}/{filename}", "{ext|lowercase} has no value"),
            ("{source.ext}/{filename}", "{source.ext} has no value"),
            (
                "{content.Neg|bucket}/{filename}",
                "{content.Neg|bucket} has no value: |bucket groups numbers from 0 up",
            ),
            (
                "{content.Title|number}/{filename}",
                "{content.Title|number} has no value: the text is not a number",
            ),
            (
                "{source.rel_path[5]}/{filename}",
                "{source.rel_path[5]} has no value: the path has no such segment",
            ),
        ] {
            assert_eq!(said(pattern, &ctx), [sentence], "{pattern}");
        }
    }

    #[test]
    fn test_complex_pattern() {
        let zone = UserZone::utc();
        let mut ctx = context(&zone, at("vacation/photos/IMG_001.jpg"));
        ctx.set_lock_hash(Some("abcdef1234567890".to_string()));
        assert_eq!(
            placed(
                "{source.rel_path[0]}/{source.mtime|year}/{stem}_{hash_short}.{ext}",
                &ctx
            ),
            "vacation/2024/IMG_001_abcdef12.jpg"
        );
    }

    /// A pattern reading a save time Canon cannot read has that cause — never the
    /// missing-fact wording — and its sentence states the cause alone. The second save time
    /// is the sentinel 9999-12-31 23:59:59 UTC, past the last instant Canon reads.
    #[test]
    fn a_save_time_canon_cannot_read_refuses_with_its_own_cause() {
        let zone = UserZone::utc();
        for mtime in [300_000_000_000, 253402300799] {
            for written in [
                "{source.mtime|year}/{filename}",
                "{source.mtime}",
                "{mtime}",
            ] {
                let mut file = at("photos/a.jpg");
                file.mtime = mtime;
                let ctx = context(&zone, file);
                assert_eq!(
                    causes(written, &ctx),
                    [NoPlaceCause::UnreadableSaveTime],
                    "{written} at {mtime}"
                );
                assert_eq!(
                    said(written, &ctx),
                    [
                        "Canon cannot read this file's save time: it lies at the edge of the \
                      years 0000–9999 or beyond"
                    ]
                );
            }
        }
    }

    /// A modifier finer than the time was written has that cause, naming the `{…}` and the
    /// grain the time is known to.
    #[test]
    fn a_value_too_coarse_for_its_modifier_refuses_with_its_cause() {
        use crate::core::domain::time::reading::read_time;
        let zone = UserZone::utc();
        let mut ctx = context(&zone, at("photos/a.jpg"));
        ctx.set_fact(
            "content.DateTimeOriginal",
            FactValue::Time(read_time("2024").unwrap()),
        );
        let pattern = "{content.DateTimeOriginal|month}/{filename}";
        assert!(matches!(
            causes(pattern, &ctx).as_slice(),
            [NoPlaceCause::TooCoarse { .. }]
        ));
        assert_eq!(
            said(pattern, &ctx),
            ["{content.DateTimeOriginal|month} has no value: the time is known only to the year"]
        );
        assert_eq!(
            placed("{content.DateTimeOriginal|year}/{filename}", &ctx),
            "2024/a.jpg"
        );
    }

    /// A save time is an instant: using it — modified or printed bare — reads the clock.
    #[test]
    fn a_pattern_using_a_save_time_reads_an_instant() {
        let zone = UserZone::utc();
        for pattern in [
            "{source.mtime|year}/{filename}",
            "{source.mtime}/{filename}",
        ] {
            let ctx = context(&zone, at("photos/a.jpg"));
            assert!(
                evaluate(&bound(pattern), &ctx).unwrap().used_an_instant,
                "{pattern}"
            );
        }
        let mut ctx = context(&zone, at("photos/a.jpg"));
        ctx.set_fact(
            "content.CreateDate",
            FactValue::Time(
                crate::core::domain::time::reading::read_time("2024-06-15T14:30:05+02:00").unwrap(),
            ),
        );
        assert!(
            evaluate(&bound("{content.CreateDate|date}"), &ctx)
                .unwrap()
                .used_an_instant,
            "an instant with its offset"
        );
    }

    /// A clock reading reads as written on any clock, and a pattern naming no time reads
    /// none: neither is a clock that mattered.
    #[test]
    fn a_pattern_using_a_clock_reading_reads_none() {
        let zone = UserZone::utc();
        for pattern in [
            "{content.DateTimeOriginal|year}/{filename}",
            "{content.DateTimeOriginal}/{filename}",
            "{source.ext}/{source.size}/{filename}",
        ] {
            let mut ctx = context(&zone, at("photos/a.jpg"));
            ctx.set_fact(
                "content.DateTimeOriginal",
                FactValue::Time(
                    crate::core::domain::time::reading::read_time("2024-06-15 14:30:05").unwrap(),
                ),
            );
            assert!(
                !evaluate(&bound(pattern), &ctx).unwrap().used_an_instant,
                "{pattern}"
            );
        }
    }
}
