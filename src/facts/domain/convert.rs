//! **The conversion, as pure logic**: what `canon facts convert` reads, plans and checks, from a
//! key's census (`core::repo::fact::key_census`) and nothing else — no database, no screen.
//!
//! A source's value is decided by `expr::source_value` over the source's own fact and its
//! content's, never paired here a second way. Each source sits in one place, the first that
//! applies ([`place_of`]); classes and acknowledgments count facts, places count sources. Every
//! fact of the key is classed — the write set is the census's row set, parked facts included.

use std::collections::{BTreeMap, HashMap};
use std::fmt;

use crate::core::domain::conversion::{convert, Converted, Loss, NotAConversion, NotRead};
use crate::core::domain::fact::{
    CensusSource, FactOn, FactType, FactValue, KeyCensus, KeyTypes, Stored,
};
use crate::core::domain::format::shell_quote;
use crate::core::domain::format_count;
use crate::core::domain::number::spelled;
use crate::core::domain::time::zone::UserZone;
use crate::expr::{
    convert_command, printed_key, printed_value, read_key_name, rendered, source_value, Arriving,
    BuiltinKey, Complement, Key, Noun, Statement, Subject, CONTENT_HASH,
};

use super::{counted, decide, plural, try_parse_duration, Disposition, Hint, JsonFact, KnownHint};

// ============================================================================
// The key
// ============================================================================

/// A name the conversion answers by what it is, never with "unknown key".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyRefusal {
    /// A name starting with `source.`: a source fact's type is fixed, and no other such name
    /// is stored. `t` is the source fact's type, where the name is one.
    SourceName { name: String, t: Option<FactType> },
    /// The content hash, Canon's own record of content identity.
    ContentHash,
    /// A built-in key whose content reading holds no facts: its type is fixed.
    Builtin { name: String, t: FactType },
}

impl KeyRefusal {
    /// The modifiers that ask about this name's values as numbers or times without changing
    /// anything stored — offered only where its values are text, since a modifier named for a
    /// number, a time or a path would be refused where it is typed.
    pub fn offers_modifiers(&self) -> bool {
        match self {
            KeyRefusal::SourceName { t, .. } => *t == Some(FactType::Text),
            KeyRefusal::Builtin { t, .. } => *t == FactType::Text,
            KeyRefusal::ContentHash => false,
        }
    }
}

/// The stored key a conversion acts on: the name's content reading (`Make` → `content.Make`),
/// or a refusal for a `source.` name and for the content hash. A name with no facts is asked
/// about after the census ([`fixed_type_answer`]).
pub fn convert_key(name: &str) -> Result<String, KeyRefusal> {
    let reading = read_key_name(name);
    match reading.content_key {
        None => Err(KeyRefusal::SourceName {
            name: name.to_string(),
            t: reading
                .source_fact
                .and_then(|fact| Key::SourceFact(fact).known_type()),
        }),
        Some(key) if key == CONTENT_HASH => Err(KeyRefusal::ContentHash),
        Some(key) => Ok(key),
    }
}

/// The answer for a name whose content reading holds no facts but which is a built-in key:
/// its type is fixed — the content hash's reason for `hash` and `hash_short`. `None` for any
/// other name, which is then an unknown key.
pub fn fixed_type_answer(name: &str) -> Option<KeyRefusal> {
    let builtin: BuiltinKey = read_key_name(name).builtin?;
    let key = Key::BuiltinKey(builtin);
    if key.reads_the_content_hash() {
        return Some(KeyRefusal::ContentHash);
    }
    key.known_type().map(|t| KeyRefusal::Builtin {
        name: name.to_string(),
        t,
    })
}

/// What `--to` names: a type, or `duration`, which the conversion answers with its routes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Target {
    Type(FactType),
    Duration,
}

impl Target {
    /// The words `--to` takes: the type words (and quietly `num`, `datetime`), and `duration`.
    pub fn parse_word(word: &str) -> Option<Target> {
        match word {
            "duration" => Some(Target::Duration),
            other => FactType::parse_word(other).map(Target::Type),
        }
    }
}

// ============================================================================
// Places
// ============================================================================

/// Where a source sits, for the read and the plan: the first that applies, in this order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Place {
    NoLongerPresent,
    Suspended {
        root_id: i64,
    },
    ArchiveCopy,
    Excluded,
    /// Counted by `canon facts --global`.
    Counted,
}

/// A source's place: no longer present, on a suspended root, an archive copy, excluded, else
/// counted by `canon facts --global` — the first that applies.
pub fn place_of(s: &CensusSource) -> Place {
    if !s.present {
        Place::NoLongerPresent
    } else if !s.source.is_active() {
        Place::Suspended {
            root_id: s.source.root_id,
        }
    } else if s.source.is_from_role("archive") {
        Place::ArchiveCopy
    } else if s.source.is_excluded() {
        Place::Excluded
    } else {
        Place::Counted
    }
}

/// Sources counted by place.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Places {
    pub counted: u64,
    pub archive_copy: u64,
    pub excluded: u64,
    /// Per suspended root, by id: its path and how many sources there.
    pub suspended: BTreeMap<i64, (String, u64)>,
    pub no_longer_present: u64,
}

impl Places {
    fn add(&mut self, s: &CensusSource) {
        match place_of(s) {
            Place::NoLongerPresent => self.no_longer_present += 1,
            Place::Suspended { root_id } => {
                let entry = self
                    .suspended
                    .entry(root_id)
                    .or_insert_with(|| (s.source.root_path.clone(), 0));
                entry.1 += 1;
            }
            Place::ArchiveCopy => self.archive_copy += 1,
            Place::Excluded => self.excluded += 1,
            Place::Counted => self.counted += 1,
        }
    }

    pub fn on_suspended_roots(&self) -> u64 {
        self.suspended.values().map(|(_, n)| n).sum()
    }

    /// Every source counted, wherever it sits.
    pub fn total(&self) -> u64 {
        self.counted + self.not_counted()
    }

    /// The sources `canon facts --global` does not count.
    pub fn not_counted(&self) -> u64 {
        self.archive_copy + self.excluded + self.on_suspended_roots() + self.no_longer_present
    }
}

// ============================================================================
// The census read as values
// ============================================================================

/// The census with each source's value decided and each fact's holders known.
struct Reading<'a> {
    census: &'a KeyCensus,
    /// Per fact: the sources whose value it is.
    value_of: Vec<Vec<usize>>,
    /// Per fact: the sources that hold it — the source it is stored on, or every source of the
    /// content it is stored on.
    holders: Vec<Vec<usize>>,
    /// Per source: its own fact, where it has one.
    own: Vec<Option<usize>>,
    /// Per fact: whether it is stored on empty files' content.
    on_empty_content: Vec<bool>,
}

impl<'a> Reading<'a> {
    fn of(census: &'a KeyCensus) -> Self {
        let mut on_source: HashMap<i64, usize> = HashMap::new();
        let mut on_content: HashMap<i64, usize> = HashMap::new();
        for (i, fact) in census.facts.iter().enumerate() {
            match fact.on {
                FactOn::Source(id) => on_source.insert(id, i),
                FactOn::Content(id) => on_content.insert(id, i),
            };
        }
        let n = census.facts.len();
        let mut value_of = vec![Vec::new(); n];
        let mut holders = vec![Vec::new(); n];
        let mut own = vec![None; census.sources.len()];
        let mut content_sources: HashMap<i64, Vec<usize>> = HashMap::new();
        for (si, s) in census.sources.iter().enumerate() {
            let own_fact = on_source.get(&s.source.id).copied();
            let content_fact = s
                .source
                .object_id
                .and_then(|object| on_content.get(&object).copied());
            if let Some(fi) = own_fact {
                holders[fi].push(si);
                own[si] = Some(fi);
            }
            if let Some(object) = s.source.object_id {
                content_sources.entry(object).or_default().push(si);
            }
            if let Some(fi) = content_fact {
                holders[fi].push(si);
            }
            // A source's own row Canon cannot read is no value, as every reader drops it.
            let readable_own = own_fact.filter(|&fi| is_readable(&census.facts[fi].value));
            let content = content_fact.map(|fi| match census.facts[fi].value {
                Stored::Readable(_) => Stored::Readable(fi),
                Stored::Unreadable => Stored::Unreadable,
            });
            if let Some(fi) = source_value(readable_own, content) {
                value_of[fi].push(si);
            }
        }
        let on_empty_content = census
            .facts
            .iter()
            .map(|fact| match fact.on {
                FactOn::Content(object) => content_sources.get(&object).is_some_and(|ss| {
                    ss.iter()
                        .all(|&si| census.sources[si].source.is_contentless())
                }),
                FactOn::Source(_) => false,
            })
            .collect();
        Reading {
            census,
            value_of,
            holders,
            own,
            on_empty_content,
        }
    }

    fn fact_type(&self, fi: usize) -> FactType {
        match &self.census.facts[fi].value {
            Stored::Readable(v) => v.kind(),
            // A stored time Canon cannot read is still a time row: the type lookup counts it so.
            Stored::Unreadable => FactType::Time,
        }
    }

    /// What a fact is to the sources around it.
    fn standing(&self, fi: usize) -> Standing {
        if !is_readable(&self.census.facts[fi].value) {
            Standing::Unreadable
        } else if self.on_empty_content[fi] {
            Standing::EmptyContent
        } else if !self.value_of[fi].is_empty() {
            Standing::Value
        } else if !self.holders[fi].is_empty() {
            Standing::Hidden
        } else {
            Standing::NoSource
        }
    }

    fn places_of(&self, sources: &[usize]) -> Places {
        let mut places = Places::default();
        for &si in sources {
            places.add(&self.census.sources[si]);
        }
        places
    }
}

/// Every source whose value is a fact of the key, with its place — what the parity pin against
/// `select_sources` reads.
#[cfg(test)]
pub(in crate::facts) fn valued_places(census: &KeyCensus) -> Vec<(i64, Place)> {
    let reading = Reading::of(census);
    let mut valued: Vec<usize> = reading.value_of.iter().flatten().copied().collect();
    valued.sort_unstable();
    valued.dedup();
    valued
        .into_iter()
        .map(|si| {
            let s = &census.sources[si];
            (s.source.id, place_of(s))
        })
        .collect()
}

fn is_readable(value: &Stored<FactValue>) -> bool {
    matches!(value, Stored::Readable(_))
}

/// What a fact is to the sources around it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Standing {
    /// The value of one source or more.
    Value,
    /// Stored on empty files' content: its own line.
    EmptyContent,
    /// Held, and the value of no source: behind another fact of the same source.
    Hidden,
    /// On content no source holds.
    NoSource,
    /// A stored time Canon cannot read: the value of no source.
    Unreadable,
}

/// A value's stored spelling, as `worklist --emit` writes it: text as stored, a number in its
/// one spelling, a time as its ISO 8601 text. What groups values and what `--list` prints.
pub fn stored_spelling(value: &FactValue) -> String {
    match value {
        FactValue::Text(t) | FactValue::Path(t) => t.clone(),
        FactValue::Num(n) => spelled(*n),
        FactValue::Time(t) => t.iso_text(),
    }
}

/// The key a value is grouped by: its type and its stored spelling.
fn group_key(value: &Stored<FactValue>) -> (u8, String) {
    match value {
        Stored::Readable(v) => (
            match v.kind() {
                FactType::Text => 0,
                FactType::Num => 1,
                FactType::Time => 2,
                FactType::Path => 3,
            },
            stored_spelling(v),
        ),
        Stored::Unreadable => (4, String::new()),
    }
}

// ============================================================================
// The read
// ============================================================================

/// The fact on empty files' content, and the empty files that show it, by place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmptyFiles {
    pub t: FactType,
    pub shown_by: Places,
}

/// What a key holds, per type.
#[derive(Debug, Clone)]
pub struct TypeHolding {
    pub t: FactType,
    pub facts: u64,
    /// The sources whose value is a fact of this type, by place — empty files on their own line.
    pub sources: Places,
    pub hidden: u64,
    pub no_source: u64,
    /// Stored times Canon cannot read: the value of no source.
    pub unreadable: u64,
    /// The most frequent values, counted in facts, at most three: each as `canon facts` prints
    /// it (a time on the user's clock), through the printing rule, and grouped by that print, so
    /// values that print the same count as one.
    pub top: Vec<(String, u64)>,
}

/// The read: what a key holds and the empty-files line.
#[derive(Debug, Clone)]
pub struct Holdings {
    pub types: KeyTypes,
    pub per_type: Vec<TypeHolding>,
    pub empty_files: Option<EmptyFiles>,
    /// Whether some fact is the value of more than one source.
    pub shares_content: bool,
}

/// How many of the most frequent values the read shows per type.
pub const READ_VALUES: usize = 3;

/// The key's types, from every fact of its census. `None` for a key with no facts.
pub fn census_types(census: &KeyCensus) -> Option<KeyTypes> {
    types_of(census, &Reading::of(census))
}

fn types_of(census: &KeyCensus, reading: &Reading) -> Option<KeyTypes> {
    KeyTypes::from_counts((0..census.facts.len()).map(|fi| (reading.fact_type(fi), 1)))
}

/// What a key holds, from its census, its values printed on `zone`. `None` for a key with no
/// facts.
pub fn holdings(census: &KeyCensus, zone: &UserZone) -> Option<Holdings> {
    let reading = Reading::of(census);
    let types = types_of(census, &reading)?;
    let mut empty_files = None;
    let per_type = types
        .counts()
        .iter()
        .map(|&(t, facts)| {
            let mut holding = TypeHolding {
                t,
                facts,
                sources: Places::default(),
                hidden: 0,
                no_source: 0,
                unreadable: 0,
                top: Vec::new(),
            };
            let mut values: HashMap<String, u64> = HashMap::new();
            for fi in (0..census.facts.len()).filter(|&fi| reading.fact_type(fi) == t) {
                match reading.standing(fi) {
                    Standing::Value => {
                        for &si in &reading.value_of[fi] {
                            holding.sources.add(&census.sources[si]);
                        }
                    }
                    Standing::EmptyContent => {
                        empty_files = Some(EmptyFiles {
                            t,
                            shown_by: reading.places_of(&reading.value_of[fi]),
                        })
                    }
                    Standing::Hidden => holding.hidden += 1,
                    Standing::NoSource => holding.no_source += 1,
                    Standing::Unreadable => holding.unreadable += 1,
                }
                if let Stored::Readable(v) = &census.facts[fi].value {
                    *values
                        .entry(printed_value(&rendered(None, v, zone), v.kind()))
                        .or_insert(0) += 1;
                }
            }
            let mut ranked: Vec<(String, u64)> = values.into_iter().collect();
            ranked.sort_by(|(va, na), (vb, nb)| nb.cmp(na).then(va.cmp(vb)));
            ranked.truncate(READ_VALUES);
            holding.top = ranked;
            holding
        })
        .collect();
    let shares_content = reading.value_of.iter().any(|sources| sources.len() > 1);
    Some(Holdings {
        types,
        per_type,
        empty_files,
        shares_content,
    })
}

// ============================================================================
// The plan
// ============================================================================

/// Why a fact is removed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Why {
    /// It is not a value of the target type.
    NotRead(NotRead),
    /// It is a stored time Canon cannot read, which no other type can hold.
    Unreadable,
}

/// What converting does to a fact that is not already of the target type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    Unchanged,
    Lossy(Loss),
    Removed(Why),
}

/// One fact that changes, and what it becomes (`None` for a removed fact).
#[derive(Debug, Clone)]
pub struct FactChange {
    pub fact_id: i64,
    pub class: Class,
    pub becomes: Option<FactValue>,
}

/// Facts by class.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ClassCounts {
    pub unchanged: u64,
    pub lossy: u64,
    pub removed: u64,
}

impl ClassCounts {
    pub fn total(&self) -> u64 {
        self.unchanged + self.lossy + self.removed
    }
}

/// What the sources a removed fact covered show after the act, by their own fact's class.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Uncovered {
    /// Their own fact, already of the target type.
    pub own: u64,
    /// Their own fact, converted.
    pub own_converted: u64,
    /// No value: their own fact is removed too.
    pub no_value: u64,
}

/// One distinct value of a lossy or removed class.
#[derive(Debug, Clone)]
pub struct ValueGroup {
    pub value: Stored<FactValue>,
    pub becomes: Option<FactValue>,
    /// The loss, for a lossy group.
    pub loss: Option<Loss>,
    pub facts: u64,
    /// The sources it is the value of, by place.
    pub sources: Places,
    pub hidden: u64,
    pub no_source: u64,
    pub uncovered: Uncovered,
}

/// A class's distinct values: the most frequent shown, the rest counted.
#[derive(Debug, Clone, Default)]
pub struct ClassGroups {
    pub shown: Vec<ValueGroup>,
    pub more: u64,
}

/// How many distinct values the plan shows per class.
pub const PLAN_VALUES: usize = 10;

/// The facts the plan converts, and where they sit — the bridge to what `canon facts` showed.
#[derive(Debug, Clone, Default)]
pub struct Bridge {
    /// The facts that change, by their type now.
    pub facts: Vec<(FactType, u64)>,
    /// The sources whose value they are, by place — empty files on their own line.
    pub sources: Places,
    pub hidden: u64,
    pub no_source: u64,
    pub unreadable: u64,
}

/// The plan: every fact of the key classed against the target type.
#[derive(Debug, Clone)]
pub struct ConversionPlan {
    pub to: FactType,
    /// One per fact that is not already of the target type.
    pub changes: Vec<FactChange>,
    pub counts: ClassCounts,
    /// Facts already of the target type, left as they are.
    pub already: u64,
    pub bridge: Bridge,
    /// One unchanged fact whose spelling changes: before and after.
    pub spelling: Option<(FactValue, FactValue)>,
    pub lossy: ClassGroups,
    pub removed: ClassGroups,
    /// The fact on empty files' content, where it changes, and its class.
    pub empty_files: Option<(EmptyFiles, Class)>,
    /// Removed facts written like durations, where every other non-empty value is one.
    pub durations: Option<u64>,
}

impl ConversionPlan {
    /// Every fact the key holds.
    pub fn facts(&self) -> u64 {
        self.counts.total() + self.already
    }

    /// The facts the key holds after the act.
    pub fn remaining(&self) -> u64 {
        self.counts.unchanged + self.counts.lossy + self.already
    }

    /// Whether the act removes every fact, so the key is forgotten.
    pub fn removes_every_fact(&self) -> bool {
        self.remaining() == 0 && self.counts.removed > 0
    }

    /// Whether most of the facts that change are removed.
    pub fn removes_most(&self) -> bool {
        self.counts.removed * 2 > self.counts.total()
    }

    /// The removal count the duration routes name for removing here and importing again with
    /// the hint: only where every value this plan removes is written like a duration, so the
    /// hint brings each one back, and nothing is lossy, so removing is all the act does and the
    /// command carrying the removal alone runs as printed.
    pub fn duration_removal(&self) -> Option<u64> {
        (self.durations.is_some() && self.counts.lossy == 0)
            .then_some(self.counts.removed)
            .filter(|&removed| removed > 0)
    }
}

/// A fact's class under `to`, or `None` where it is already of that type.
fn class_of(
    value: &Stored<FactValue>,
    to: FactType,
) -> Result<Option<(Class, Option<FactValue>)>, NotAConversion> {
    let value = match value {
        Stored::Unreadable => {
            return match to {
                FactType::Time => Ok(None),
                FactType::Text => Ok(Some((Class::Removed(Why::Unreadable), None))),
                FactType::Num => Err(NotAConversion::TimeToNumber),
                FactType::Path => Err(NotAConversion::PathIsDerived),
            }
        }
        Stored::Readable(v) => v,
    };
    if value.kind() == to {
        return Ok(None);
    }
    Ok(Some(match convert(value, to)? {
        Converted::Unchanged(v) => (Class::Unchanged, Some(v)),
        Converted::Lossy(v, loss) => (Class::Lossy(loss), Some(v)),
        Converted::DoesNotRead(why) => (Class::Removed(Why::NotRead(why)), None),
    }))
}

/// The plan for converting every fact of the key to `to`, or the pair of types the table
/// holds no conversion between.
pub fn plan(census: &KeyCensus, to: FactType) -> Result<ConversionPlan, NotAConversion> {
    let reading = Reading::of(census);
    let mut classes: Vec<Option<(Class, Option<FactValue>)>> =
        Vec::with_capacity(census.facts.len());
    for fact in &census.facts {
        classes.push(class_of(&fact.value, to)?);
    }

    let mut changes = Vec::new();
    let mut counts = ClassCounts::default();
    let mut already = 0;
    let mut bridge = Bridge::default();
    let mut bridge_types: Vec<(FactType, u64)> = Vec::new();
    let mut spelling = None;
    let mut empty_files = None;
    let mut lossy: HashMap<(u8, String), ValueGroup> = HashMap::new();
    let mut removed: HashMap<(u8, String), ValueGroup> = HashMap::new();
    let mut removed_texts: Vec<&str> = Vec::new();

    for (fi, fact) in census.facts.iter().enumerate() {
        let Some((class, becomes)) = &classes[fi] else {
            already += 1;
            continue;
        };
        match class {
            Class::Unchanged => counts.unchanged += 1,
            Class::Lossy(_) => counts.lossy += 1,
            Class::Removed(_) => counts.removed += 1,
        }
        bridge_types.push((reading.fact_type(fi), 1));
        let standing = reading.standing(fi);
        match standing {
            Standing::Value => {
                for &si in &reading.value_of[fi] {
                    bridge.sources.add(&census.sources[si]);
                }
            }
            Standing::EmptyContent => {
                empty_files = Some((
                    EmptyFiles {
                        t: reading.fact_type(fi),
                        shown_by: reading.places_of(&reading.value_of[fi]),
                    },
                    *class,
                ))
            }
            Standing::Hidden => bridge.hidden += 1,
            Standing::NoSource => bridge.no_source += 1,
            Standing::Unreadable => bridge.unreadable += 1,
        }

        if let (Class::Unchanged, Stored::Readable(before), Some(after)) =
            (class, &fact.value, becomes)
        {
            if spelling.is_none() && stored_spelling(before) != stored_spelling(after) {
                spelling = Some((before.clone(), after.clone()));
            }
        }

        let groups = match class {
            Class::Unchanged => None,
            Class::Lossy(_) => Some(&mut lossy),
            Class::Removed(_) => {
                if let Stored::Readable(FactValue::Text(t)) = &fact.value {
                    removed_texts.push(t);
                }
                Some(&mut removed)
            }
        };
        if let Some(groups) = groups {
            let group = groups
                .entry(group_key(&fact.value))
                .or_insert_with(|| ValueGroup {
                    value: fact.value.clone(),
                    becomes: becomes.clone(),
                    loss: match class {
                        Class::Lossy(loss) => Some(*loss),
                        Class::Unchanged | Class::Removed(_) => None,
                    },
                    facts: 0,
                    sources: Places::default(),
                    hidden: 0,
                    no_source: 0,
                    uncovered: Uncovered::default(),
                });
            group.facts += 1;
            match standing {
                Standing::Value | Standing::EmptyContent => {
                    for &si in &reading.value_of[fi] {
                        group.sources.add(&census.sources[si]);
                    }
                }
                Standing::Hidden => group.hidden += 1,
                Standing::NoSource => group.no_source += 1,
                Standing::Unreadable => {}
            }
            // A removed content fact uncovers the own facts it stood in front of.
            if matches!(class, Class::Removed(_)) && matches!(fact.on, FactOn::Content(_)) {
                for &si in &reading.holders[fi] {
                    let Some(own) = reading.own[si] else { continue };
                    if !is_readable(&census.facts[own].value) {
                        continue;
                    }
                    match &classes[own] {
                        None => group.uncovered.own += 1,
                        Some((Class::Unchanged | Class::Lossy(_), _)) => {
                            group.uncovered.own_converted += 1
                        }
                        Some((Class::Removed(_), _)) => group.uncovered.no_value += 1,
                    }
                }
            }
        }

        changes.push(FactChange {
            fact_id: fact.id,
            class: *class,
            becomes: becomes.clone(),
        });
    }

    bridge.facts = KeyTypes::from_counts(bridge_types)
        .map(|types| types.counts().to_vec())
        .unwrap_or_default();
    let durations = (to == FactType::Num)
        .then(|| {
            let written: Vec<&&str> = removed_texts
                .iter()
                .filter(|t| !t.trim().is_empty())
                .collect();
            (!written.is_empty() && written.iter().all(|t| try_parse_duration(t).is_some()))
                .then_some(written.len() as u64)
        })
        .flatten();

    Ok(ConversionPlan {
        to,
        changes,
        counts,
        already,
        bridge,
        spelling,
        lossy: ranked(lossy),
        removed: ranked(removed),
        empty_files,
        durations,
    })
}

/// A class's groups, the most frequent first (ties by value), the first [`PLAN_VALUES`] shown.
fn ranked(groups: HashMap<(u8, String), ValueGroup>) -> ClassGroups {
    let mut all: Vec<((u8, String), ValueGroup)> = groups.into_iter().collect();
    all.sort_by(|(ka, a), (kb, b)| b.facts.cmp(&a.facts).then(ka.cmp(kb)));
    let more = all.len().saturating_sub(PLAN_VALUES) as u64;
    ClassGroups {
        shown: all.into_iter().take(PLAN_VALUES).map(|(_, g)| g).collect(),
        more,
    }
}

/// One target the read names: reachable with its plan, or not a conversion.
#[derive(Debug, Clone)]
pub struct TargetLine {
    pub to: FactType,
    pub reach: Result<ConversionPlan, NotAConversion>,
}

/// Every type the read names, in a fixed order: time, number, text.
pub fn targets(census: &KeyCensus) -> Vec<TargetLine> {
    [FactType::Time, FactType::Num, FactType::Text]
        .into_iter()
        .map(|to| TargetLine {
            to,
            reach: plan(census, to),
        })
        .collect()
}

// ============================================================================
// --list
// ============================================================================

/// What `--list` marks a line with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListMark {
    /// The source's place; `Counted` marks nothing.
    Place(Place),
    Hidden,
    /// The fact on empty files' content, shown by this many empty files.
    EmptyFiles(u64),
    /// Content no source holds: the line names the content hash instead of a path.
    NoSource,
}

/// One line of `--list`: a lossy or removed fact and a source holding it.
#[derive(Debug, Clone)]
pub struct ListLine {
    pub class: Class,
    pub original: Stored<FactValue>,
    pub becomes: Option<FactValue>,
    /// The source's path, or the content hash where no source holds the content.
    pub path: String,
    pub mark: ListMark,
}

/// Every lossy and removed fact of the plan, one line per source holding it.
pub fn list_lines(census: &KeyCensus, plan: &ConversionPlan) -> Vec<ListLine> {
    let reading = Reading::of(census);
    let index: HashMap<i64, usize> = census
        .facts
        .iter()
        .enumerate()
        .map(|(fi, fact)| (fact.id, fi))
        .collect();
    let mut lines = Vec::new();
    for change in &plan.changes {
        if change.class == Class::Unchanged {
            continue;
        }
        let Some(&fi) = index.get(&change.fact_id) else {
            continue;
        };
        let fact = &census.facts[fi];
        let line = |path: String, mark: ListMark| ListLine {
            class: change.class,
            original: fact.value.clone(),
            becomes: change.becomes.clone(),
            path,
            mark,
        };
        match reading.standing(fi) {
            Standing::EmptyContent => lines.push(line(
                fact.object_hash.clone().unwrap_or_default(),
                ListMark::EmptyFiles(reading.value_of[fi].len() as u64),
            )),
            Standing::Value => {
                for &si in &reading.value_of[fi] {
                    let s = &census.sources[si];
                    lines.push(line(s.source.path(), ListMark::Place(place_of(s))));
                }
            }
            Standing::Hidden => {
                for &si in &reading.holders[fi] {
                    lines.push(line(census.sources[si].source.path(), ListMark::Hidden));
                }
            }
            Standing::Unreadable if !reading.holders[fi].is_empty() => {
                for &si in &reading.holders[fi] {
                    let s = &census.sources[si];
                    lines.push(line(s.source.path(), ListMark::Place(place_of(s))));
                }
            }
            Standing::NoSource | Standing::Unreadable => lines.push(line(
                fact.object_hash.clone().unwrap_or_default(),
                ListMark::NoSource,
            )),
        }
    }
    lines
}

// ============================================================================
// Acknowledgments
// ============================================================================

/// An acknowledgment as given: bare, or with the count the user saw.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ack {
    Bare,
    Count(u64),
}

/// The two acknowledgments `--allow` carries.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Acknowledgments {
    pub loss: Option<Ack>,
    pub removal: Option<Ack>,
}

/// The class an acknowledgment is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckClass {
    Loss,
    Removal,
}

impl AckClass {
    /// The word `--allow` takes for it.
    pub fn word(self) -> &'static str {
        match self {
            AckClass::Loss => "loss",
            AckClass::Removal => "removal",
        }
    }

    /// What the conversion does to `n` facts of this class: `removes 6 facts`, `makes no fact
    /// lossy`.
    pub fn what_it_does(self, n: u64) -> String {
        let facts = if n == 0 {
            "no fact".to_string()
        } else {
            counted(n, "fact", "facts")
        };
        match self {
            AckClass::Loss => format!("makes {facts} lossy"),
            AckClass::Removal => format!("removes {facts}"),
        }
    }
}

/// The `--allow` values, `loss=N` and `removal=N` (bare `loss`/`removal` accepted here and
/// refused by the check); anything else is input Canon cannot read.
pub fn parse_allow(values: &[String]) -> Result<Acknowledgments, String> {
    let mut given = Acknowledgments::default();
    for value in values {
        let (word, count) = match value.split_once('=') {
            Some((word, count)) => (word, Some(count)),
            None => (value.as_str(), None),
        };
        let ack = match count {
            None => Ack::Bare,
            Some(count) => Ack::Count(count.parse().map_err(|_| {
                format!("--allow {word}= takes the count the plan states, such as {word}=4")
            })?),
        };
        match word {
            "loss" => given.loss = Some(ack),
            "removal" => given.removal = Some(ack),
            other => {
                return Err(format!(
                    "--allow takes loss=N and removal=N here, not '{other}'"
                ))
            }
        }
    }
    Ok(given)
}

/// An acknowledgment that does not match the plan: the class, what was given, and the count now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AckProblem {
    pub class: AckClass,
    pub given: Option<Ack>,
    pub now: u64,
}

/// Whether the acknowledgments match the plan: every non-empty class carries its count, and
/// every count given equals the class's count now (`0` for an empty class). A bare flag on an
/// empty class acknowledges nothing and is no problem.
pub fn check_acknowledgments(
    plan: &ConversionPlan,
    given: &Acknowledgments,
) -> Result<(), Vec<AckProblem>> {
    let problems: Vec<AckProblem> = [
        (AckClass::Loss, given.loss, plan.counts.lossy),
        (AckClass::Removal, given.removal, plan.counts.removed),
    ]
    .into_iter()
    .filter(|&(_, given, now)| match given {
        None | Some(Ack::Bare) => now > 0,
        Some(Ack::Count(n)) => n != now,
    })
    .map(|(class, given, now)| AckProblem { class, given, now })
    .collect();
    if problems.is_empty() {
        Ok(())
    } else {
        Err(problems)
    }
}

// ============================================================================
// The next import
// ============================================================================

/// What a later import does with one kind of value for a key of the target type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NextImport {
    pub arriving: Vec<Arriving>,
    pub outcome: Outcome,
}

/// Stored as a type, or skipped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Stored(FactType),
    Skipped,
}

/// The kinds of value the plan states for each target, grouped as they are stated together.
fn stated(to: FactType) -> Vec<Vec<Arriving>> {
    match to {
        FactType::Time => vec![vec![Arriving::UntypedDates]],
        FactType::Num => vec![
            vec![Arriving::UntypedNumbers],
            vec![Arriving::UntypedNumbersWithAZero],
        ],
        FactType::Text => vec![
            vec![Arriving::UntypedValues, Arriving::JsonNumbers],
            vec![Arriving::DatetimeOrDurationHinted],
        ],
        FactType::Path => Vec::new(),
    }
}

/// Values of each kind, as an import record carries them.
fn examples(kind: Arriving) -> Vec<JsonFact> {
    let text = |t: &str| JsonFact::Text(t.to_string());
    let hinted = |hint, t: &str| JsonFact::Hinted {
        hint: Hint::Known(hint),
        value: Box::new(text(t)),
        written: format!("\"{t}\""),
    };
    match kind {
        Arriving::UntypedDates => vec![text("2021:03:02 18:44:10"), text("2021-03-02")],
        Arriving::UntypedNumbers => vec![text("12"), text("2.5")],
        Arriving::UntypedNumbersWithAZero => vec![text("01234"), text("2.50")],
        Arriving::UntypedValues => vec![text("misc"), text("2021:03:02 18:44:10"), text("12")],
        Arriving::JsonNumbers => vec![JsonFact::Number("12".into())],
        Arriving::DatetimeOrDurationHinted => vec![
            hinted(KnownHint::Datetime, "2021:03:02 18:44:10"),
            hinted(KnownHint::Duration, "1:23:45"),
        ],
        Arriving::DurationHinted => vec![hinted(KnownHint::Duration, "1:23:45")],
    }
}

/// What import's own decision does with a value, for a key of type `to`.
fn outcome_of(to: FactType, fact: &JsonFact) -> Outcome {
    match decide(Some(&KeyTypes::of(to)), fact) {
        Disposition::Store(value) => Outcome::Stored(value.kind()),
        Disposition::Skip(_) => Outcome::Skipped,
    }
}

/// What the next import does for a key of type `to`, stated per kind of value — read from
/// what `decide` does with each kind's first example, never written by hand.
pub fn next_import(to: FactType) -> Vec<NextImport> {
    stated(to)
        .into_iter()
        .filter_map(|arriving| {
            let first = arriving
                .first()
                .and_then(|&kind| examples(kind).into_iter().next())?;
            Some(NextImport {
                outcome: outcome_of(to, &first),
                arriving,
            })
        })
        .collect()
}

// ============================================================================
// What the read, the plan and the refusals state
// ============================================================================

/// The label a stored time Canon cannot read stands under, wherever its value would print.
pub const UNREADABLE_TIME: &str = "(a time Canon cannot read)";

/// A stored value as printed for a reader of its own type, in its stored spelling.
pub fn printed(value: &FactValue) -> String {
    printed_value(&stored_spelling(value), value.kind())
}

/// A stored value as printed, or the label of a time Canon cannot read.
pub fn printed_stored(value: &Stored<FactValue>) -> String {
    match value {
        Stored::Readable(value) => printed(value),
        Stored::Unreadable => UNREADABLE_TIME.to_string(),
    }
}

/// The sources at each place, the counted first; each suspended root named, at most three,
/// where `name_roots`, else counted together.
pub fn place_parts(places: &Places, name_roots: bool) -> Vec<String> {
    let mut parts = Vec::new();
    if places.counted > 0 {
        parts.push(format!(
            "{} counted by `canon facts --global`",
            format_count(places.counted)
        ));
    }
    if places.archive_copy > 0 {
        parts.push(counted(
            places.archive_copy,
            "on an archive copy",
            "on archive copies",
        ));
    }
    if places.excluded > 0 {
        parts.push(format!("{} excluded", format_count(places.excluded)));
    }
    if name_roots {
        const NAMED: usize = 3;
        for (id, (path, n)) in places.suspended.iter().take(NAMED) {
            parts.push(format!(
                "{} on suspended root {path} (id:{id})",
                format_count(*n)
            ));
        }
        let rest: Vec<u64> = places
            .suspended
            .values()
            .skip(NAMED)
            .map(|(_, n)| *n)
            .collect();
        if !rest.is_empty() {
            parts.push(format!(
                "and {} on {}",
                format_count(rest.iter().sum::<u64>()),
                counted(
                    rest.len() as u64,
                    "more suspended root",
                    "more suspended roots"
                )
            ));
        }
    } else if places.on_suspended_roots() > 0 {
        parts.push(counted(
            places.on_suspended_roots(),
            "on a suspended root",
            "on suspended roots",
        ));
    }
    if places.no_longer_present > 0 {
        parts.push(format!(
            "{} no longer present",
            format_count(places.no_longer_present)
        ));
    }
    parts
}

/// The facts that are no source's value: hidden behind another fact of the same source, and on
/// content no source holds.
pub fn valueless_parts(hidden: u64, no_source: u64) -> Vec<String> {
    let mut parts = Vec::new();
    if hidden > 0 {
        parts.push(format!(
            "{} hidden behind another fact of the same source",
            format_count(hidden)
        ));
    }
    if no_source > 0 {
        parts.push(format!(
            "{} on content no source holds",
            format_count(no_source)
        ));
    }
    parts
}

/// The stored times Canon cannot read, which are the value of no source.
pub fn unreadable_line(n: u64) -> String {
    format!(
        "{}, the value of no source",
        Statement {
            subject: Subject::Counted(n, Noun::Fact),
            complement: Complement::NotReadable(FactType::Time),
        }
    )
}

/// The facts a target changes, by their type now: `the 380 text facts`, `the 41,220 time
/// facts and the 50 number facts`.
pub fn facts_by_type(facts: &[(FactType, u64)]) -> String {
    let each: Vec<String> = facts
        .iter()
        .map(|(t, n)| {
            format!(
                "the {} {t} {}",
                format_count(*n),
                plural(*n, "fact", "facts")
            )
        })
        .collect();
    match each.split_last() {
        Some((last, [])) => last.clone(),
        Some((last, rest)) => format!("{} and {last}", rest.join(", ")),
        None => String::new(),
    }
}

/// `371 unchanged, 9 removed` — the classes holding facts.
pub fn class_summary(counts: &ClassCounts) -> String {
    [
        (counts.unchanged, "unchanged"),
        (counts.lossy, "lossy"),
        (counts.removed, "removed"),
    ]
    .into_iter()
    .filter(|(n, _)| *n > 0)
    .map(|(n, class)| format!("{} {class}", format_count(n)))
    .collect::<Vec<_>>()
    .join(", ")
}

/// **The bridge clause**: the facts a conversion changes and where they sit, against what
/// `canon facts --global` counted — `380 text facts, the value of 300 sources — 50 of them not
/// counted by `canon facts --global`: 50 on suspended roots; 3 hidden behind …`. The plan's
/// first line and the act's record state it in these words.
pub fn bridge_clause(bridge: &Bridge) -> String {
    let mut clause = facts_by_type(&bridge.facts)
        .strip_prefix("the ")
        .unwrap_or_default()
        .to_string();
    if bridge.sources.total() > 0 {
        clause.push_str(&format!(
            ", the value of {}",
            counted(bridge.sources.total(), "source", "sources")
        ));
        let not_counted = bridge.sources.not_counted();
        if not_counted > 0 {
            let places = Places {
                counted: 0,
                ..bridge.sources.clone()
            };
            clause.push_str(&format!(
                " — {} of them not counted by `canon facts --global`: {}",
                format_count(not_counted),
                place_parts(&places, false).join(", ")
            ));
        }
    }
    let mut valueless = valueless_parts(bridge.hidden, bridge.no_source);
    if bridge.unreadable > 0 {
        valueless.push(unreadable_line(bridge.unreadable));
    }
    if !valueless.is_empty() {
        clause.push_str(&format!("; {}", valueless.join("; ")));
    }
    clause
}

/// The reason a target is not reached, and its route where there is one — one line each.
pub fn not_a_conversion_lines(why: NotAConversion, to: FactType) -> Vec<String> {
    let cannot = |from: FactType| {
        Statement {
            subject: Subject::Type(from),
            complement: Complement::CannotBeConvertedTo(to),
        }
        .to_string()
    };
    match why {
        NotAConversion::TimeToNumber => vec![format!(
            "{}; a time known only to a year can go through text",
            cannot(FactType::Time)
        )],
        NotAConversion::NumberToTime => vec![
            format!(
                "{}; numbers that are years can go through text",
                cannot(FactType::Num)
            ),
            "if these are seconds since 1970, have your tool write the times as date-time text \
             with the datetime hint, under a new key"
                .to_string(),
        ],
        NotAConversion::PathIsDerived => vec![cannot(FactType::Path)],
    }
}

/// The command that converts `key` to `to`, as printed: backticked, the key quoted for the
/// shell, each non-empty class's acknowledgment with its count, `--yes`, and the reason where
/// one was given — so it runs as pasted.
pub fn acting_command(
    key: &str,
    to: FactType,
    counts: &ClassCounts,
    reason: Option<&str>,
) -> String {
    let preview = convert_command(key, Some(to));
    let command = preview.strip_suffix('`').unwrap_or(&preview);
    let mut flags = String::new();
    if counts.lossy > 0 {
        flags.push_str(&format!(" --allow loss={}", counts.lossy));
    }
    if counts.removed > 0 {
        flags.push_str(&format!(" --allow removal={}", counts.removed));
    }
    flags.push_str(" --yes");
    if let Some(reason) = reason {
        flags.push_str(&format!(" --reason {}", shell_quote(reason)));
    }
    format!("{command}{flags}`")
}

/// The ways for values written like durations, which no conversion makes: import them under a
/// new key with the duration hint; or, where `removal` counts the facts `--to number` removes,
/// remove them here and import them again with the hint.
pub fn duration_routes(key: &str, removal: Option<u64>) -> Vec<String> {
    let mut routes =
        vec!["import the durations with the duration hint under a new key".to_string()];
    if let Some(removed) = removal {
        let counts = ClassCounts {
            removed,
            ..ClassCounts::default()
        };
        routes.push(format!(
            "or remove these facts here with {}, then import them with the hint",
            acting_command(key, FactType::Num, &counts, None)
        ));
    }
    routes
}

/// What a later import does with values of one kind for a key of type `to`: import's own
/// decision on the kind's first example.
pub fn import_outcome(to: FactType, kind: Arriving) -> Option<Outcome> {
    examples(kind)
        .into_iter()
        .next()
        .map(|first| outcome_of(to, &first))
}

/// **A conversion refused**, carried out to `main`: the key's own answer, a target not reached,
/// `--to duration`'s routes, a key a conversion emptied — answers, printed without `Error:` —
/// or a name Canon knows no facts under, which is input it cannot read.
#[derive(Debug, Clone)]
pub enum ConvertRefused {
    Key(KeyRefusal),
    /// No facts are stored under the name, and no conversion explains why.
    UnknownKey {
        name: String,
    },
    /// The key's last facts were removed by conversion `decision_id`.
    Emptied {
        key: String,
        decision_id: i64,
    },
    NotAConversion {
        key: String,
        types: KeyTypes,
        to: FactType,
        why: NotAConversion,
    },
    /// `--to duration`: the key holds numbers only, or the routes, with the removal count of
    /// `--to number` where it is reachable and removes something.
    Duration {
        key: String,
        numbers_only: bool,
        removal: Option<u64>,
    },
    /// `--yes` with acknowledgments that do not match the plan computed inside the act: the
    /// classes that do not match, the counts now, and the reason given, for the command to run.
    Acknowledgment {
        key: String,
        to: FactType,
        problems: Vec<AckProblem>,
        counts: ClassCounts,
        reason: Option<String>,
    },
}

impl ConvertRefused {
    /// Whether the refusal is an answer: everything but a name Canon knows nothing under.
    pub fn is_answer(&self) -> bool {
        match self {
            ConvertRefused::Key(KeyRefusal::SourceName { t: None, .. })
            | ConvertRefused::UnknownKey { .. } => false,
            ConvertRefused::Key(
                KeyRefusal::SourceName { t: Some(_), .. }
                | KeyRefusal::ContentHash
                | KeyRefusal::Builtin { .. },
            )
            | ConvertRefused::Emptied { .. }
            | ConvertRefused::NotAConversion { .. }
            | ConvertRefused::Duration { .. }
            | ConvertRefused::Acknowledgment { .. } => true,
        }
    }

    fn lines(&self) -> Vec<String> {
        let key_is = |key: &str, complement| {
            Statement {
                subject: Subject::Key(key),
                complement,
            }
            .to_string()
        };
        let modifiers = |name: &str| {
            [("number", FactType::Num), ("datetime", FactType::Time)].map(|(modifier, t)| {
                format!(
                    "`{}|{modifier}` asks for its values as {} without changing anything stored",
                    printed_key(name),
                    t.noun(crate::core::domain::fact::Count::Many)
                )
            })
        };
        let unknown = |name: &str| {
            vec![
                format!("`{name}` is an unknown key"),
                "`canon facts --global` lists the keys".to_string(),
            ]
        };
        match self {
            ConvertRefused::Key(refusal) => {
                let mut lines = match refusal {
                    KeyRefusal::SourceName { name, t: None } => return unknown(name),
                    KeyRefusal::SourceName { name, t: Some(t) } => vec![format!(
                        "{}, and a source fact's type is fixed",
                        key_is(name, Complement::OfType(*t))
                    )],
                    KeyRefusal::Builtin { name, t } => vec![format!(
                        "{}, and a built-in key's type is fixed",
                        key_is(name, Complement::OfType(*t))
                    )],
                    KeyRefusal::ContentHash => vec![
                        format!(
                            "The content hash ({CONTENT_HASH}) is Canon's own record of content \
                             identity, and its type is fixed"
                        ),
                        "to correct a hash a tool imported wrongly, import the right one with \
                         `canon import-facts`, which moves the source to its content"
                            .to_string(),
                    ],
                };
                if refusal.offers_modifiers() {
                    let name = match refusal {
                        KeyRefusal::SourceName { name, .. } | KeyRefusal::Builtin { name, .. } => {
                            name.as_str()
                        }
                        KeyRefusal::ContentHash => CONTENT_HASH,
                    };
                    lines.extend(modifiers(name));
                }
                lines
            }
            ConvertRefused::UnknownKey { name } => unknown(name),
            ConvertRefused::Emptied { key, decision_id } => vec![
                format!(
                    "{} has no facts: conversion {decision_id} removed every fact the key then held",
                    printed_key(key)
                ),
                format!("`canon trail show {decision_id}` to read it"),
            ],
            ConvertRefused::NotAConversion {
                key,
                types,
                to,
                why,
            } => {
                let is = match types.single() {
                    Some(t) => key_is(key, Complement::OfType(t)),
                    None => key_is(key, Complement::OfMixedType(types)),
                };
                let mut reasons = not_a_conversion_lines(*why, *to).into_iter();
                let first = reasons.next().unwrap_or_default();
                std::iter::once(format!("{is}: {first}"))
                    .chain(reasons)
                    .collect()
            }
            ConvertRefused::Duration {
                key,
                numbers_only: true,
                ..
            } => vec![format!(
                "{}: {}",
                key_is(key, Complement::OfType(FactType::Num)),
                Statement {
                    subject: Subject::Arriving(&[Arriving::DurationHinted]),
                    complement: match import_outcome(FactType::Num, Arriving::DurationHinted) {
                        Some(Outcome::Stored(t)) => Complement::StoredAs(t),
                        Some(Outcome::Skipped) | None => Complement::Skipped,
                    },
                }
            )],
            ConvertRefused::Duration {
                key,
                numbers_only: false,
                removal,
            } => std::iter::once(format!(
                "{}",
                Statement {
                    subject: Subject::Value {
                        text: "1:23:45",
                        as_type: FactType::Text,
                    },
                    complement: Complement::Not(FactType::Num),
                }
            ))
            .chain(duration_routes(key, *removal))
            .collect(),
            ConvertRefused::Acknowledgment {
                key,
                to,
                problems,
                counts,
                reason,
            } => {
                let mut lines = vec![format!(
                    "{} → {to}: not converted; --allow does not match the conversion now",
                    printed_key(key)
                )];
                for problem in problems {
                    let word = problem.class.word();
                    let given = match problem.given {
                        Some(Ack::Count(n)) => format!("--allow {word}={n} given"),
                        Some(Ack::Bare) => format!("--allow {word} given with no count"),
                        None => format!("no --allow {word} given"),
                    };
                    lines.push(format!(
                        "  {given}; the conversion {}",
                        problem.class.what_it_does(problem.now)
                    ));
                }
                lines.push(format!(
                    "{} to see what changed",
                    convert_command(key, Some(*to))
                ));
                lines.push(format!(
                    "{} to convert",
                    acting_command(key, *to, counts, reason.as_deref())
                ));
                lines
            }
        }
    }
}

impl fmt::Display for ConvertRefused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.lines().join("\n"))
    }
}

impl std::error::Error for ConvertRefused {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::domain::source::Source;
    use crate::core::domain::time::reading::read_time;

    /// A source in the census: its id, root, content, presence, root state and role.
    struct At {
        root: i64,
        object: Option<i64>,
        present: bool,
        suspended: bool,
        archive: bool,
        excluded: bool,
        size: i64,
    }

    const COUNTED: At = At {
        root: 1,
        object: None,
        present: true,
        suspended: false,
        archive: false,
        excluded: false,
        size: 10,
    };

    fn source(id: i64, at: At) -> CensusSource {
        CensusSource {
            source: Source {
                id,
                root_id: at.root,
                root_path: format!("/root{}", at.root),
                rel_path: format!("f{id}"),
                object_id: at.object,
                size: at.size,
                mtime: 0,
                excluded: at.excluded,
                object_excluded: at.object.map(|_| false),
                device: 0,
                inode: 0,
                partial_hash: String::new(),
                basis_rev: 0,
                root_role: if at.archive { "archive" } else { "source" }.into(),
                root_suspended: at.suspended,
                decision_id: None,
            },
            present: at.present,
        }
    }

    fn text(t: &str) -> Stored<FactValue> {
        Stored::Readable(FactValue::Text(t.into()))
    }

    fn time(t: &str) -> Stored<FactValue> {
        Stored::Readable(FactValue::Time(read_time(t).unwrap()))
    }

    fn fact(id: i64, on: FactOn, value: Stored<FactValue>) -> CensusFact {
        CensusFact {
            id,
            on,
            value,
            object_hash: match on {
                FactOn::Content(o) => Some(format!("hash{o}")),
                FactOn::Source(_) => None,
            },
        }
    }

    use crate::core::domain::fact::CensusFact;

    /// Each text value on its own source, counted by `canon facts --global`.
    fn texts(values: &[&str]) -> KeyCensus {
        KeyCensus {
            facts: values
                .iter()
                .enumerate()
                .map(|(i, v)| fact(i as i64 + 1, FactOn::Source(i as i64 + 1), text(v)))
                .collect(),
            sources: (1..=values.len() as i64)
                .map(|id| source(id, COUNTED))
                .collect(),
        }
    }

    fn class_of_text(value: &str, to: FactType) -> (Class, Option<FactValue>) {
        let plan = plan(&texts(&[value]), to).unwrap();
        let change = &plan.changes[0];
        (change.class, change.becomes.clone())
    }

    #[test]
    fn convert_key_reads_the_content_name_and_refuses_source_names_and_the_hash() {
        assert_eq!(convert_key("Make"), Ok("content.Make".into()));
        assert_eq!(convert_key("content.Make"), Ok("content.Make".into()));
        assert_eq!(convert_key("mtime"), Ok("content.mtime".into()));
        assert_eq!(
            convert_key("source.size"),
            Err(KeyRefusal::SourceName {
                name: "source.size".into(),
                t: Some(FactType::Num)
            })
        );
        assert_eq!(
            convert_key("source.nothing"),
            Err(KeyRefusal::SourceName {
                name: "source.nothing".into(),
                t: None
            })
        );
        assert_eq!(
            convert_key("content.hash.sha256"),
            Err(KeyRefusal::ContentHash)
        );
    }

    /// A built-in name with no facts under its content reading: the content hash's reason
    /// for the two names that read the hash, the fixed type for the rest; modifiers offered
    /// only where its values are text.
    #[test]
    fn a_built_in_name_with_no_facts_is_answered_by_its_fixed_type() {
        assert_eq!(fixed_type_answer("hash"), Some(KeyRefusal::ContentHash));
        assert_eq!(
            fixed_type_answer("hash_short"),
            Some(KeyRefusal::ContentHash)
        );
        let mtime = fixed_type_answer("mtime").unwrap();
        assert_eq!(
            mtime,
            KeyRefusal::Builtin {
                name: "mtime".into(),
                t: FactType::Time
            }
        );
        assert!(!mtime.offers_modifiers());
        assert!(fixed_type_answer("filename").unwrap().offers_modifiers());
        assert!(!fixed_type_answer("size").unwrap().offers_modifiers());
        assert!(!fixed_type_answer("id").unwrap().offers_modifiers());
        assert_eq!(fixed_type_answer("Make"), None);
        assert!(convert_key("source.ext").unwrap_err().offers_modifiers());
        assert!(!convert_key("source.path").unwrap_err().offers_modifiers());
    }

    #[test]
    fn to_takes_the_type_words_and_duration() {
        assert_eq!(
            Target::parse_word("time"),
            Some(Target::Type(FactType::Time))
        );
        assert_eq!(
            Target::parse_word("datetime"),
            Some(Target::Type(FactType::Time))
        );
        assert_eq!(Target::parse_word("num"), Some(Target::Type(FactType::Num)));
        assert_eq!(Target::parse_word("duration"), Some(Target::Duration));
        assert_eq!(Target::parse_word("path"), None);
    }

    /// **Each source sits in the first place that applies**: no longer present, then a
    /// suspended root, then an archive copy, then excluded, else counted — over every
    /// combination of the four.
    #[test]
    fn place_of_takes_the_first_place_that_applies() {
        for bits in 0..16u8 {
            let (gone, parked, archive, excluded) =
                (bits & 1 != 0, bits & 2 != 0, bits & 4 != 0, bits & 8 != 0);
            let s = source(
                1,
                At {
                    root: 7,
                    present: !gone,
                    suspended: parked,
                    archive,
                    excluded,
                    ..COUNTED
                },
            );
            let expected = if gone {
                Place::NoLongerPresent
            } else if parked {
                Place::Suspended { root_id: 7 }
            } else if archive {
                Place::ArchiveCopy
            } else if excluded {
                Place::Excluded
            } else {
                Place::Counted
            };
            assert_eq!(place_of(&s), expected, "{bits:04b}");
        }
    }

    /// **Text → time**: every spelling the one reading reads is unchanged at
    /// its own form and precision; the rest is removed.
    #[test]
    fn text_to_time_check_table() {
        for unchanged in [
            "2024:07:23 11:06:32",
            "2024-07-23T11:06:32",
            "2024-07-23 11:06:32",
            "2024-07-23T11:06:32+02:00",
            "2024-07-23T09:06:32Z",
            "2024-07-23T09:06:32-00:00",
            "2024:07:23 11:06:32.45",
            "2024-07-23",
            "2024:07:23",
            "2024-07",
            "2024",
            "2024-07-23 11:06",
        ] {
            let (class, becomes) = class_of_text(unchanged, FactType::Time);
            assert_eq!(class, Class::Unchanged, "{unchanged}");
            let Some(FactValue::Time(t)) = becomes else {
                panic!("{unchanged}")
            };
            assert_eq!(t, read_time(unchanged).unwrap(), "{unchanged}");
        }
        for removed in ["0000:00:00 00:00:00", "2024-02-30", "misc", "1690000000"] {
            let (class, becomes) = class_of_text(removed, FactType::Time);
            assert!(
                matches!(class, Class::Removed(Why::NotRead(_))),
                "{removed}"
            );
            assert!(becomes.is_none());
        }
    }

    /// **Text → number**: unchanged where the number is the one written, lossy where the reading drops what the text carried, removed where it is no number.
    #[test]
    fn text_to_number_check_table() {
        let num = |v: &str| match class_of_text(v, FactType::Num) {
            (class, Some(FactValue::Num(n))) => (class, Some(n)),
            (class, None) => (class, None),
            other => panic!("{v}: {other:?}"),
        };
        assert_eq!(num("12"), (Class::Unchanged, Some(12.0)));
        assert_eq!(num("2.5"), (Class::Unchanged, Some(2.5)));
        assert_eq!(num("2.50"), (Class::Lossy(Loss::TrailingZero), Some(2.5)));
        assert_eq!(
            num("01234"),
            (Class::Lossy(Loss::LeadingZero), Some(1234.0))
        );
        let (class, nearest) = num("123456789012345678901");
        assert_eq!(class, Class::Lossy(Loss::NotExact));
        assert_eq!(nearest, Some(123456789012345678901.0));
        for removed in ["14.2.1", "misc", "+5"] {
            assert!(
                matches!(num(removed).0, Class::Removed(Why::NotRead(_))),
                "{removed}"
            );
        }
    }

    /// **Any type → text**: always unchanged, a number in its shortest
    /// plain spelling and a time as `worklist --emit` writes it — each reading back through
    /// the same reading as the same value.
    #[test]
    fn any_type_to_text_check_table() {
        let numbers = KeyCensus {
            facts: [4000.0, 2.5, 1e20]
                .iter()
                .enumerate()
                .map(|(i, n)| {
                    fact(
                        i as i64 + 1,
                        FactOn::Source(1),
                        Stored::Readable(FactValue::Num(*n)),
                    )
                })
                .collect(),
            sources: vec![source(1, COUNTED)],
        };
        let plan = plan(&numbers, FactType::Text).unwrap();
        let spelled: Vec<String> = plan
            .changes
            .iter()
            .map(|c| {
                assert_eq!(c.class, Class::Unchanged);
                match c.becomes.as_ref().unwrap() {
                    FactValue::Text(t) => t.clone(),
                    other => panic!("{other:?}"),
                }
            })
            .collect();
        assert_eq!(spelled, ["4000", "2.5", "100000000000000000000"]);
        for (n, t) in [4000.0, 2.5, 1e20].iter().zip(&spelled) {
            assert!(matches!(
                convert(&FactValue::Text(t.clone()), FactType::Num),
                Ok(Converted::Unchanged(FactValue::Num(back))) if back == *n
            ));
        }
        for written in [
            "2014",
            "2014-07-23",
            "2014-07-23T11:06:32",
            "2014-07-23T11:06:32.45+02:00",
            "2014-07-23T09:06:32-00:00",
        ] {
            let census = KeyCensus {
                facts: vec![fact(1, FactOn::Source(1), time(written))],
                sources: vec![source(1, COUNTED)],
            };
            let change = &plan_for(&census, FactType::Text).changes[0];
            assert_eq!(change.class, Class::Unchanged);
            let Some(FactValue::Text(t)) = &change.becomes else {
                panic!()
            };
            assert_eq!(t, written);
            assert_eq!(read_time(t).unwrap(), read_time(written).unwrap());
        }
    }

    fn plan_for(census: &KeyCensus, to: FactType) -> ConversionPlan {
        plan(census, to).unwrap()
    }

    #[test]
    fn a_number_and_a_time_are_not_conversions_of_each_other() {
        let census = KeyCensus {
            facts: vec![
                fact(1, FactOn::Source(1), Stored::Readable(FactValue::Num(3.0))),
                fact(2, FactOn::Source(2), time("2020")),
            ],
            sources: vec![source(1, COUNTED), source(2, COUNTED)],
        };
        assert_eq!(
            plan(&census, FactType::Time).unwrap_err(),
            NotAConversion::NumberToTime
        );
        assert_eq!(
            plan(&census, FactType::Num).unwrap_err(),
            NotAConversion::TimeToNumber
        );
        assert!(plan(&census, FactType::Text).is_ok());
    }

    /// **A key holding two types, converted to one of them, plans the other's facts only**;
    /// the facts already of the target type stay as they are, counted once.
    #[test]
    fn a_mixed_key_plans_the_other_types_facts_only() {
        let census = KeyCensus {
            facts: vec![
                fact(1, FactOn::Source(1), time("2020-01-02")),
                fact(2, FactOn::Source(2), text("2021:03:02 18:44:10")),
                fact(3, FactOn::Source(3), text("0000:00:00 00:00:00")),
            ],
            sources: (1..=3).map(|id| source(id, COUNTED)).collect(),
        };
        let plan = plan_for(&census, FactType::Time);
        assert_eq!(plan.already, 1);
        assert_eq!(
            plan.counts,
            ClassCounts {
                unchanged: 1,
                lossy: 0,
                removed: 1
            }
        );
        assert_eq!(plan.bridge.facts, [(FactType::Text, 2)]);
        assert_eq!(plan.facts(), 3);
        assert_eq!(plan.remaining(), 2);
        let (before, after) = plan.spelling.clone().unwrap();
        assert_eq!(stored_spelling(&before), "2021:03:02 18:44:10");
        assert_eq!(stored_spelling(&after), "2021-03-02T18:44:10");
    }

    /// **Hidden facts convert and are counted as hidden**: a source's own fact behind its
    /// content's is the value of no source, and is classed like every other fact.
    #[test]
    fn hidden_facts_convert_and_are_counted_as_hidden() {
        let census = KeyCensus {
            facts: vec![
                fact(1, FactOn::Content(9), text("2021")),
                fact(2, FactOn::Source(1), text("2019")),
            ],
            sources: vec![source(
                1,
                At {
                    object: Some(9),
                    ..COUNTED
                },
            )],
        };
        let read = holdings(&census, &UserZone::utc()).unwrap();
        assert_eq!(read.per_type[0].hidden, 1);
        assert_eq!(read.per_type[0].sources.counted, 1);
        let plan = plan_for(&census, FactType::Time);
        assert_eq!(plan.counts.unchanged, 2);
        assert_eq!(plan.bridge.hidden, 1);
        assert_eq!(plan.bridge.sources.total(), 1);
    }

    /// **A removed fact says what the sources it covered show after**, by their own fact's
    /// class in the same act: their own fact, their own fact converted, or no value.
    #[test]
    fn an_uncovered_fact_is_reported_by_its_own_class() {
        let census = KeyCensus {
            facts: vec![
                fact(1, FactOn::Content(9), text("misc")),
                fact(2, FactOn::Source(1), text("2019")),
                fact(3, FactOn::Source(2), text("other")),
            ],
            sources: vec![
                source(
                    1,
                    At {
                        object: Some(9),
                        ..COUNTED
                    },
                ),
                source(
                    2,
                    At {
                        object: Some(9),
                        ..COUNTED
                    },
                ),
                source(
                    3,
                    At {
                        object: Some(9),
                        ..COUNTED
                    },
                ),
            ],
        };
        let plan = plan_for(&census, FactType::Time);
        let misc = plan
            .removed
            .shown
            .iter()
            .find(|g| matches!(&g.value, Stored::Readable(FactValue::Text(t)) if t == "misc"))
            .unwrap();
        assert_eq!(
            misc.uncovered,
            Uncovered {
                own: 0,
                own_converted: 1,
                no_value: 1
            }
        );
        assert_eq!(misc.sources.counted, 3);
        // Under --to text nothing is removed, so nothing is uncovered; a time already there
        // is shown as it is.
        let census = KeyCensus {
            facts: vec![
                fact(1, FactOn::Content(9), Stored::Unreadable),
                fact(2, FactOn::Source(1), text("own")),
            ],
            sources: vec![source(
                1,
                At {
                    object: Some(9),
                    ..COUNTED
                },
            )],
        };
        let plan = plan_for(&census, FactType::Text);
        assert_eq!(plan.removed.shown[0].uncovered.own, 1);
    }

    /// **The empty-files line counts what each empty file shows**: the fact on empty files'
    /// content, shown by the empty files whose value it is, by place — outside the place counts
    /// and inside the class counts. An empty file's own fact is hidden behind it like any
    /// file's.
    #[test]
    fn the_empty_files_line_counts_what_each_file_shows() {
        let empty = |id, at: At| {
            source(
                id,
                At {
                    object: Some(5),
                    size: 0,
                    ..at
                },
            )
        };
        let census = KeyCensus {
            facts: vec![
                fact(1, FactOn::Content(5), text("0000:00:00 00:00:00")),
                fact(2, FactOn::Source(1), text("2019")),
                fact(3, FactOn::Source(4), text("2020")),
                fact(4, FactOn::Source(5), text("2021")),
            ],
            sources: vec![
                // An empty file with no content known shows its own fact: an ordinary source.
                source(5, At { size: 0, ..COUNTED }),
                empty(1, COUNTED),
                empty(
                    2,
                    At {
                        suspended: true,
                        root: 3,
                        ..COUNTED
                    },
                ),
                empty(
                    3,
                    At {
                        present: false,
                        ..COUNTED
                    },
                ),
                source(4, COUNTED),
            ],
        };
        let read = holdings(&census, &UserZone::utc()).unwrap();
        let line = read.empty_files.clone().unwrap();
        assert_eq!(line.shown_by.counted, 1);
        assert_eq!(line.shown_by.on_suspended_roots(), 1);
        assert_eq!(line.shown_by.no_longer_present, 1);
        assert_eq!(
            read.per_type[0].sources.total(),
            2,
            "empty files showing the content's fact stand on their own line"
        );
        assert_eq!(read.per_type[0].hidden, 1);
        let plan = plan_for(&census, FactType::Time);
        let (line, class) = plan.empty_files.clone().unwrap();
        assert_eq!(line.shown_by.total(), 3);
        assert!(matches!(class, Class::Removed(_)));
        assert_eq!(plan.counts.removed, 1);
        assert_eq!(plan.bridge.sources.total(), 2);
    }

    /// **Durations are offered only when every other value is one**: under `--to number`, where
    /// every non-empty value removed is written like a duration.
    #[test]
    fn durations_are_offered_only_when_every_other_value_is_one() {
        let plan = plan_for(&texts(&["1:23:45", "12:05", "12", ""]), FactType::Num);
        assert_eq!(plan.durations, Some(2));
        let plan = plan_for(&texts(&["1:23:45", "misc", "12"]), FactType::Num);
        assert_eq!(plan.durations, None);
        let plan = plan_for(&texts(&["12"]), FactType::Num);
        assert_eq!(plan.durations, None);
        let plan = plan_for(&texts(&["1:23:45"]), FactType::Time);
        assert_eq!(plan.durations, None);
    }

    #[test]
    fn ten_values_per_class_then_and_n_more() {
        let values: Vec<String> = (0..13).map(|i| format!("misc{i}")).collect();
        let mut all: Vec<&str> = values.iter().map(String::as_str).collect();
        all.extend(["misc0", "misc0", "misc5"]);
        let plan = plan_for(&texts(&all), FactType::Num);
        assert_eq!(plan.removed.shown.len(), PLAN_VALUES);
        assert_eq!(plan.removed.more, 3);
        assert_eq!(plan.removed.shown[0].facts, 3);
        assert_eq!(plan.removed.shown[1].facts, 2);
        assert!(plan.removes_most());
    }

    #[test]
    fn every_fact_removed_forgets_the_key() {
        let plan = plan_for(&texts(&["misc", "other"]), FactType::Num);
        assert!(plan.removes_every_fact());
        assert_eq!(plan.remaining(), 0);
        let plan = plan_for(&texts(&["misc", "12"]), FactType::Num);
        assert!(!plan.removes_every_fact());
    }

    /// **A stored time Canon cannot read** is already of type time, left as it is under
    /// `--to time`, removed under `--to text`; no number reaches it. The read gives it a line of
    /// its own and never counts it among its values.
    #[test]
    fn an_unreadable_time_is_left_under_time_and_removed_under_text() {
        let census = KeyCensus {
            facts: vec![
                fact(1, FactOn::Content(9), Stored::Unreadable),
                fact(2, FactOn::Source(2), text("2020")),
            ],
            sources: vec![
                source(
                    1,
                    At {
                        object: Some(9),
                        ..COUNTED
                    },
                ),
                source(2, COUNTED),
            ],
        };
        let time = plan_for(&census, FactType::Time);
        assert_eq!(time.already, 1);
        assert_eq!(time.counts.unchanged, 1);
        let text = plan_for(&census, FactType::Text);
        assert_eq!(text.counts.removed, 1);
        assert!(matches!(text.removed.shown[0].value, Stored::Unreadable));
        assert_eq!(text.bridge.unreadable, 1);
        assert_eq!(
            plan(&census, FactType::Num).unwrap_err(),
            NotAConversion::TimeToNumber
        );
        let read = holdings(&census, &UserZone::utc()).unwrap();
        let times = read
            .per_type
            .iter()
            .find(|h| h.t == FactType::Time)
            .unwrap();
        assert_eq!(times.unreadable, 1);
        assert!(times.top.is_empty());
        let lines = list_lines(&census, &text);
        assert!(matches!(lines[0].original, Stored::Unreadable));
    }

    /// **The acknowledgment check**: a missing acknowledgment for a class non-empty now, a bare
    /// flag on one, a grown or shrunk count, and a non-zero count for an empty class are each
    /// refused with the class and its count now; an exact count, and `0` or nothing for an
    /// empty class, are accepted.
    #[test]
    fn the_acknowledgment_check() {
        let plan = plan_for(&texts(&["2.50", "misc", "other"]), FactType::Num);
        assert_eq!(plan.counts.lossy, 1);
        assert_eq!(plan.counts.removed, 2);
        let check = |loss, removal| {
            check_acknowledgments(&plan, &Acknowledgments { loss, removal })
                .err()
                .map(|problems| {
                    problems
                        .iter()
                        .map(|p| (p.class, p.given, p.now))
                        .collect::<Vec<_>>()
                })
        };
        use AckClass::*;
        assert_eq!(check(Some(Ack::Count(1)), Some(Ack::Count(2))), None);
        assert_eq!(
            check(None, Some(Ack::Count(2))),
            Some(vec![(Loss, None, 1)])
        );
        assert_eq!(
            check(Some(Ack::Bare), Some(Ack::Count(2))),
            Some(vec![(Loss, Some(Ack::Bare), 1)])
        );
        assert_eq!(
            check(Some(Ack::Count(1)), Some(Ack::Count(3))),
            Some(vec![(Removal, Some(Ack::Count(3)), 2)])
        );
        assert_eq!(
            check(Some(Ack::Count(0)), Some(Ack::Count(1))),
            Some(vec![
                (Loss, Some(Ack::Count(0)), 1),
                (Removal, Some(Ack::Count(1)), 2)
            ])
        );
        let clean = plan_for(&texts(&["12"]), FactType::Num);
        let ok = |loss| {
            check_acknowledgments(
                &clean,
                &Acknowledgments {
                    loss,
                    removal: None,
                },
            )
        };
        assert!(ok(Some(Ack::Count(0))).is_ok());
        assert!(ok(None).is_ok());
        assert!(ok(Some(Ack::Bare)).is_ok());
        assert_eq!(
            ok(Some(Ack::Count(3))).unwrap_err()[0],
            AckProblem {
                class: Loss,
                given: Some(Ack::Count(3)),
                now: 0
            }
        );
    }

    /// **Every acknowledgment refusal leads with the dry run**: a missing acknowledgment for a
    /// class non-empty now, a bare flag, a grown count, a shrunk count and a count for an empty
    /// class each state the problem, then the dry run, and give the flag with the count now only
    /// after it — so no refusal is a way past the preview.
    #[test]
    fn every_acknowledgment_refusal_leads_with_the_dry_run() {
        let counts = ClassCounts {
            unchanged: 3,
            lossy: 0,
            removed: 6,
        };
        let removal = |given| AckProblem {
            class: AckClass::Removal,
            given,
            now: 6,
        };
        let cases = [
            removal(None),
            removal(Some(Ack::Bare)),
            removal(Some(Ack::Count(4))),
            removal(Some(Ack::Count(9))),
            AckProblem {
                class: AckClass::Loss,
                given: Some(Ack::Count(2)),
                now: 0,
            },
        ];
        for problem in cases {
            let refusal = ConvertRefused::Acknowledgment {
                key: "content.N".into(),
                to: FactType::Num,
                problems: vec![problem],
                counts,
                reason: None,
            };
            assert!(refusal.is_answer());
            let text = refusal.to_string();
            let lines: Vec<&str> = text.lines().collect();
            let dry_run = lines
                .iter()
                .position(|l| {
                    *l == "`canon facts convert content.N --to number` to see what changed"
                })
                .unwrap_or_else(|| panic!("no dry run:\n{text}"));
            let flag = lines
                .iter()
                .position(|l| l.contains("--allow removal=6 --yes"))
                .unwrap_or_else(|| panic!("no flag:\n{text}"));
            assert!(dry_run < flag, "{problem:?}:\n{text}");
            assert!(
                lines[..dry_run].iter().all(|l| !l.contains('`')),
                "nothing runnable before the dry run:\n{text}"
            );
        }
        let grown = ConvertRefused::Acknowledgment {
            key: "content.N".into(),
            to: FactType::Num,
            problems: vec![removal(Some(Ack::Count(4)))],
            counts,
            reason: Some("it's".into()),
        };
        assert_eq!(
            grown.to_string(),
            "content.N → number: not converted; --allow does not match the conversion now\n  \
             --allow removal=4 given; the conversion removes 6 facts\n\
             `canon facts convert content.N --to number` to see what changed\n\
             `canon facts convert content.N --to number --allow removal=6 --yes --reason \
             'it'\\''s'` to convert"
        );
    }

    #[test]
    fn allow_reads_loss_and_removal_with_their_counts() {
        let given = parse_allow(&["loss=31".into(), "removal=4".into()]).unwrap();
        assert_eq!(given.loss, Some(Ack::Count(31)));
        assert_eq!(given.removal, Some(Ack::Count(4)));
        assert_eq!(parse_allow(&["loss".into()]).unwrap().loss, Some(Ack::Bare));
        assert!(parse_allow(&["loss=x".into()]).is_err());
        assert!(parse_allow(&["archived".into()]).is_err());
    }

    /// **The next-import lines are what `decide` does**: every example of every kind each line
    /// states agrees with the line's outcome for a key of the target type.
    #[test]
    fn next_import_is_what_decide_does() {
        for to in [FactType::Time, FactType::Num, FactType::Text] {
            let lines = next_import(to);
            assert!(!lines.is_empty(), "{to}");
            for line in lines {
                for kind in &line.arriving {
                    for example in examples(*kind) {
                        assert_eq!(
                            outcome_of(to, &example),
                            line.outcome,
                            "{to}: {kind:?} {example:?}"
                        );
                    }
                }
            }
        }
        assert_eq!(
            next_import(FactType::Time),
            [NextImport {
                arriving: vec![Arriving::UntypedDates],
                outcome: Outcome::Stored(FactType::Time)
            }]
        );
        assert_eq!(next_import(FactType::Text)[1].outcome, Outcome::Skipped);
        assert_eq!(next_import(FactType::Num)[1].outcome, Outcome::Skipped);
    }

    #[test]
    fn list_names_every_holder_of_a_lossy_or_removed_fact() {
        let census = KeyCensus {
            facts: vec![
                fact(1, FactOn::Content(9), text("misc")),
                fact(2, FactOn::Content(8), text("gone")),
                fact(3, FactOn::Source(3), text("hidden")),
                fact(4, FactOn::Source(4), text("12")),
            ],
            sources: vec![
                source(
                    1,
                    At {
                        object: Some(9),
                        ..COUNTED
                    },
                ),
                source(
                    2,
                    At {
                        object: Some(9),
                        archive: true,
                        ..COUNTED
                    },
                ),
                source(
                    3,
                    At {
                        object: Some(9),
                        ..COUNTED
                    },
                ),
                source(4, COUNTED),
            ],
        };
        let plan = plan_for(&census, FactType::Num);
        let lines = list_lines(&census, &plan);
        let marks: Vec<(String, ListMark)> = lines
            .iter()
            .map(|l| (l.path.clone(), l.mark.clone()))
            .collect();
        assert_eq!(
            marks,
            [
                ("/root1/f1".to_string(), ListMark::Place(Place::Counted)),
                ("/root1/f2".to_string(), ListMark::Place(Place::ArchiveCopy)),
                ("/root1/f3".to_string(), ListMark::Place(Place::Counted)),
                ("hash8".to_string(), ListMark::NoSource),
                ("/root1/f3".to_string(), ListMark::Hidden),
            ]
        );
    }
}
