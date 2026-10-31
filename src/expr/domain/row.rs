//! **A report's rows**: one source's value for a report key, typed and ordered ([`RowValue`]),
//! and the filter a printed row stands for ([`RowFilter`]) — the one place a row's `--where` text
//! is spelled. A row's cell and the value inside its filter come from one constructor
//! ([`ParsedFactKey::row`]), so the two cannot be spelled apart.
//!
//! **The read-back round trip**: a row a report prints, its filter pasted onto the same
//! selection in the same zone, selects exactly the sources the row counted; a marked row's line
//! selects exactly them (a coarse time, `+`) or more (a value holding a control character, `!`).

use std::cmp::Ordering;

use anyhow::{bail, Result};
use unicode_segmentation::UnicodeSegmentation;

use super::bind::{Gives, Rows};
use super::key::{BuiltinKey, ParsedFactKey};
use super::print::{holds_control, pictured, printed_as, quoted};
use super::transform::{apply_chain, bucket_range, rendered, Modified, Modifier, ModifierCall};
use crate::core::domain::fact::{FactType, FactValue};
use crate::core::domain::time::value::{ClockSpan, PeriodOp, Precision};
use crate::core::domain::time::zone::UserZone;

// ============================================================================
// A row's value
// ============================================================================

/// **One source's value for one report key**, after the key's accessor and modifiers: what it
/// was read as, its one printed form, and where it stands in a report's order. Two values are
/// one row when their type and printed form agree ([`RowValue::identity`]).
#[derive(Debug, Clone)]
pub struct RowValue {
    gives: Gives,
    rendered: String,
    order: Order,
}

/// A row's identity ([`RowValue::identity`]): what two values counted under one row share.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RowIdentity {
    of: &'static str,
    rendered: String,
}

/// What a value is ordered by within its type.
#[derive(Debug, Clone)]
enum Order {
    /// A number, a number part.
    Number(f64),
    /// A bucket's label, by its range's lower edge.
    Bucket(f64),
    /// A time, by its period on the user's clock.
    Time(ClockSpan),
    /// Text and paths, by character.
    Text,
}

impl Order {
    /// A key of mixed type orders its rows by type first: number, time, text.
    fn rank(&self) -> u8 {
        match self {
            Order::Number(_) | Order::Bucket(_) => 0,
            Order::Time(_) => 1,
            Order::Text => 2,
        }
    }
}

impl RowValue {
    /// **A stored or computed value read as a report key reads it**: through the key's accessor
    /// and modifiers on the user's clock, `None` where a step gives no value. A bucket's label
    /// keeps the range it labels, which orders it.
    pub(in crate::expr) fn read(
        value: FactValue,
        key: &ParsedFactKey,
        zone: &UserZone,
    ) -> Result<Option<RowValue>> {
        let all = &key.modifiers;
        let bucket = all.last().filter(|call| call.modifier == Modifier::Bucket);
        let before = &all[..all.len() - usize::from(bucket.is_some())];
        let chained = |value, accessor, calls: &[ModifierCall]| {
            apply_chain(value, accessor, calls, &key.raw, zone).map(|chained| chained.value)
        };
        let value = match chained(value, key.accessor.as_ref(), before)? {
            Modified::Value(value) => value,
            Modified::NoValue(_) => return Ok(None),
        };
        let (value, order) = match bucket {
            Some(call) => {
                let FactValue::Num(n) = value else {
                    bail!("{} gave |bucket a value that is not a number", key.raw);
                };
                let Some(range) = bucket_range(n, &call.args) else {
                    return Ok(None);
                };
                match chained(value, None, std::slice::from_ref(call))? {
                    Modified::Value(label) => (label, Order::Bucket(range.lower_edge())),
                    Modified::NoValue(_) => return Ok(None),
                }
            }
            None => {
                let order = match &value {
                    FactValue::Num(n) => Order::Number(*n),
                    FactValue::Time(tv) => Order::Time(tv.on_clock(zone)),
                    FactValue::Text(_) | FactValue::Path(_) => Order::Text,
                };
                (value, order)
            }
        };
        let gives = match &key.rows {
            Rows::One(gives) => gives.clone(),
            Rows::Mixed(_) => Gives::of(value.kind()),
        };
        Ok(Some(RowValue {
            gives,
            rendered: rendered(all.last().map(|call| call.modifier), &value, zone),
            order,
        }))
    }

    /// The value's one printed form, before the printing rule — for a test reading what a report
    /// counted.
    #[cfg(test)]
    pub(in crate::expr) fn rendered(&self) -> &str {
        &self.rendered
    }

    /// **Which row the value is counted under**: its type and its one printed form. Two values are
    /// one row exactly when both agree, so on a key of mixed type the text `"2019"` and the number
    /// `2019` are two rows, as they print.
    pub fn identity(&self) -> RowIdentity {
        let (rendered, of) = self.row_key();
        RowIdentity {
            of,
            rendered: rendered.to_string(),
        }
    }

    /// The pair a row's identity is: the printed form and the type. Read by [`Self::identity`],
    /// [`Self::selects`] and the order's last step, so the three cannot disagree.
    fn row_key(&self) -> (&str, &'static str) {
        (&self.rendered, self.type_word())
    }

    /// The type of the value, in the words a key of mixed type's Type column uses.
    pub fn type_word(&self) -> &'static str {
        let of = match self.gives {
            Gives::Text => FactType::Text,
            Gives::Path => FactType::Path,
            Gives::Number | Gives::Part(_) | Gives::Bucket(_) => FactType::Num,
            Gives::Time | Gives::TimeAt(_) | Gives::TimeOfDay => FactType::Time,
        };
        of.as_str()
    }

    /// **How coarse the value prints**, where a coarser printed value can stand beside finer
    /// ones in its own period: a time printed at its own grain (a bare time key, `|datetime`) or
    /// a time of day (`|time`), printed coarser than a second. A value at a second or finer
    /// prints at the second, so a finer one prints the same and is the same row.
    pub fn grain(&self) -> Option<Grain> {
        let (Gives::Time | Gives::TimeOfDay, Order::Time(span)) = (&self.gives, &self.order) else {
            return None;
        };
        match span.precision() {
            Precision::Year => Some(Grain::Year),
            Precision::Month => Some(Grain::Month),
            Precision::Day => Some(Grain::Day),
            Precision::Minute => Some(Grain::Minute),
            Precision::Second | Precision::Fraction => None,
        }
    }

    /// **Whether `<key>=<this value>` selects a source whose value is `other`**: the same printed
    /// value, or — for a coarse time — a value whose whole period lies inside this one's on the
    /// user's clock, read by the time noun's own table, as the filter reads it.
    pub fn selects(&self, other: &RowValue) -> bool {
        if self.row_key() == other.row_key() {
            return true;
        }
        match (self.grain(), &self.order, &other.order) {
            (Some(_), Order::Time(this), Order::Time(that)) => {
                that.satisfies(PeriodOp::Inside, this)
            }
            _ => false,
        }
    }
}

/// **For each value, the others it selects** ([`RowValue::selects`]): the indices of the
/// distinct values in `values` that `<key>=<value>` would select besides itself — empty for a
/// value that is not coarse. One sort by period start, coarser first at one start; each coarse
/// value then scans forward only while starts lie inside its period.
pub fn contained(values: &[&RowValue]) -> Vec<Vec<usize>> {
    let mut timed: Vec<(usize, &ClockSpan)> = values
        .iter()
        .enumerate()
        .filter_map(|(i, value)| match &value.order {
            Order::Time(span) => Some((i, span)),
            _ => None,
        })
        .collect();
    timed.sort_by(|(i, a), (j, b)| {
        (a.start(), a.precision(), values[*i].rendered.as_str()).cmp(&(
            b.start(),
            b.precision(),
            values[*j].rendered.as_str(),
        ))
    });
    let mut out = vec![Vec::new(); values.len()];
    for (at, &(i, span)) in timed.iter().enumerate() {
        if values[i].grain().is_none() {
            continue;
        }
        for &(j, other) in &timed[at + 1..] {
            if other.start() >= span.end() {
                break;
            }
            if values[i].selects(values[j]) {
                out[i].push(j);
            }
        }
    }
    out
}

impl Ord for RowValue {
    /// **A report's order among values of one count**: by type on a key of mixed type (number,
    /// time, text), then the type's own order — a number by value, a bucket by its range, a time
    /// by its start on the user's clock, text and paths by character — then by printed text and
    /// type, so two values are equal only where they are one row, and every run prints the same.
    fn cmp(&self, other: &Self) -> Ordering {
        let own = match (&self.order, &other.order) {
            (Order::Number(a), Order::Number(b)) | (Order::Bucket(a), Order::Bucket(b)) => {
                a.total_cmp(b)
            }
            (Order::Time(a), Order::Time(b)) => a.start().cmp(&b.start()),
            _ => Ordering::Equal,
        };
        self.order
            .rank()
            .cmp(&other.order.rank())
            .then(own)
            .then_with(|| self.row_key().cmp(&other.row_key()))
    }
}

impl PartialOrd for RowValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for RowValue {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for RowValue {}

// ============================================================================
// Coarse times
// ============================================================================

/// **The grain a coarse time prints at**, which the `+` line names: a time at a year, a month or a
/// day, and a time or a time of day at the minute.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Grain {
    Year,
    Month,
    Day,
    Minute,
}

impl Grain {
    /// The grain in the words a line names it by.
    pub fn word(self) -> &'static str {
        match self {
            Grain::Year => "year",
            Grain::Month => "month",
            Grain::Day => "day",
            Grain::Minute => "minute",
        }
    }

    /// The next finer part, which a value at this grain has none of.
    fn finer(self) -> Modifier {
        match self {
            Grain::Year => Modifier::Month,
            Grain::Month => Modifier::Day,
            Grain::Day => Modifier::Hour,
            Grain::Minute => Modifier::Second,
        }
    }
}

// ============================================================================
// A row's filter
// ============================================================================

/// **The filter a printed row stands for**, as `--where` text: exact, selecting exactly the
/// sources the row counted, or wider, selecting them and perhaps a few more.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowFilter {
    Exact(String),
    Wider(String),
}

/// What stands in a filter's value position: a row's value, or a placeholder a line names.
#[derive(Debug, Clone, Copy)]
pub enum Slot<'a> {
    Value(&'a RowValue),
    Placeholder(&'a str),
}

/// A row as a report prints it: its cell, control pictures in place, and its filter — none for a
/// key of mixed type, whose values a filter cannot read.
#[derive(Debug, Clone)]
pub struct PrintedRow {
    pub cell: String,
    pub filter: Option<RowFilter>,
}

impl RowFilter {
    /// The `--where` text.
    pub fn text(&self) -> &str {
        match self {
            RowFilter::Exact(text) | RowFilter::Wider(text) => text,
        }
    }

    pub fn is_exact(&self) -> bool {
        matches!(self, RowFilter::Exact(_))
    }

    /// **A root's filter**, by the built-in key that names a source's root: `root_id=<id>`, or
    /// `root_id=<N>` where a line names any root.
    pub fn root(id: Option<i64>) -> RowFilter {
        let key: &'static str = BuiltinKey::RootId.into();
        match id {
            Some(id) => RowFilter::Exact(format!("{key}={id}")),
            None => RowFilter::Exact(format!("{key}=<N>")),
        }
    }

    /// **Every part at once**: the parts joined by ` AND `, wider where any part is.
    pub fn all(parts: Vec<RowFilter>) -> RowFilter {
        let exact = parts.iter().all(RowFilter::is_exact);
        let text: Vec<&str> = parts.iter().map(RowFilter::text).collect();
        let text = text.join(" AND ");
        if exact {
            RowFilter::Exact(text)
        } else {
            RowFilter::Wider(text)
        }
    }
}

impl ParsedFactKey {
    /// **A value's row**: its cell — the value through the printing rule against what this key
    /// gives, control pictures in place — and its filter, [`Self::equals`] the value; none on a
    /// key of mixed type.
    pub fn row(&self, value: &RowValue) -> PrintedRow {
        let filter = match self.rows {
            Rows::One(_) => Some(self.equals(Slot::Value(value))),
            Rows::Mixed(_) => None,
        };
        PrintedRow {
            cell: pictured(&printed_as(&value.gives, &value.rendered)),
            filter,
        }
    }

    /// **`<key>=<value>`**: the header's expression, `=`, and the value as the printing rule
    /// prints it — or, for text or a path holding a control character, which no pasted value can
    /// hold, the wider glob: the text in double quotes, cut at each visible character that holds
    /// a control character, a bare `?` in its place (a carriage return and line feed are one).
    pub fn equals(&self, slot: Slot) -> RowFilter {
        let key = self.printed();
        match slot {
            Slot::Placeholder(text) => RowFilter::Exact(format!("{key}={text}")),
            Slot::Value(value) => match value.gives {
                Gives::Text | Gives::Path if holds_control(&value.rendered) => {
                    RowFilter::Wider(format!("{key}~{}", glob_around_controls(&value.rendered)))
                }
                _ => RowFilter::Exact(format!(
                    "{key}={}",
                    printed_as(&value.gives, &value.rendered)
                )),
            },
        }
    }

    /// **`NOT <key>?`**: the sources with no value for the header's expression.
    pub fn no_value(&self) -> RowFilter {
        RowFilter::Exact(format!("NOT {}?", self.printed()))
    }

    /// **A coarse time, exactly**: `<key>=<value> AND NOT <key>|<next finer part>?` — the value,
    /// and none of the finer values in its period.
    pub fn exactly(&self, slot: Slot, grain: Grain) -> RowFilter {
        let key = self.printed();
        let value = match slot {
            Slot::Value(value) => printed_as(&value.gives, &value.rendered),
            Slot::Placeholder(text) => text.to_string(),
        };
        let finer = ModifierCall {
            modifier: grain.finer(),
            args: Vec::new(),
        };
        RowFilter::Exact(format!("{key}={value} AND NOT {key}{}?", finer.spelled()))
    }
}

/// A glob matching `text` where every visible character holding a control character may be any
/// one: each run of the others in double quotes, an inner `"` doubled, each cut a bare `?`.
fn glob_around_controls(text: &str) -> String {
    let mut glob = String::new();
    let mut run = String::new();
    for grapheme in text.graphemes(true) {
        if holds_control(grapheme) {
            if !run.is_empty() {
                glob.push_str(&quoted(&run));
                run.clear();
            }
            glob.push('?');
        } else {
            run.push_str(grapheme);
        }
    }
    if !run.is_empty() {
        glob.push_str(&quoted(&run));
    }
    glob
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::core::domain::fact::KeyTypes;
    use crate::core::domain::time::reading::read_time;
    use crate::expr::domain::bind::bind_expr;
    use crate::expr::domain::read::{read_filter, AliasMode};

    fn key(text: &str, base: FactType) -> ParsedFactKey {
        ParsedFactKey::parse_as(text, base).unwrap()
    }

    /// A value of `key` as a report reads it, on UTC.
    fn read(key: &ParsedFactKey, value: FactValue) -> RowValue {
        RowValue::read(value, key, &UserZone::utc())
            .unwrap()
            .expect("a value")
    }

    fn time(key: &ParsedFactKey, written: &str) -> RowValue {
        read(key, FactValue::Time(read_time(written).unwrap()))
    }

    fn text(key: &ParsedFactKey, value: &str) -> RowValue {
        read(key, FactValue::Text(value.to_string()))
    }

    fn sorted(mut values: Vec<RowValue>) -> Vec<String> {
        values.sort();
        values.iter().map(|v| v.rendered().to_string()).collect()
    }

    /// **A report's order among values of one count**: numbers by value (`9` before `10`), times
    /// by start then text (`2024` before `2024-01-01`), buckets by range, weekdays by number,
    /// text by code point, and on a key of mixed type number, then time, then text.
    #[test]
    fn values_order_by_their_type_then_their_text() {
        let n = key("content.N", FactType::Num);
        let numbers = [10.0, 9.0, 100.0, -1.5].map(|x| read(&n, FactValue::Num(x)));
        assert_eq!(sorted(numbers.to_vec()), ["-1.5", "9", "10", "100"]);

        let t = key("content.T", FactType::Time);
        let times = ["2024-01-01", "2024", "2023-12-31T23:59"].map(|w| time(&t, w));
        assert_eq!(
            sorted(times.to_vec()),
            ["2023-12-31 23:59", "2024", "2024-01-01"]
        );

        let b = key("content.N|bucket", FactType::Num);
        let buckets = [20.0, 0.0, 5.0, 0.000125].map(|x| read(&b, FactValue::Num(x)));
        assert_eq!(
            sorted(buckets.to_vec()),
            ["0", "0.0001-to-0.001", "1-to-10", "10-to-100"]
        );
        let thresholds = key("content.N|bucket(60,300)", FactType::Num);
        let ranges = [400.0, -5.0, 100.0].map(|x| read(&thresholds, FactValue::Num(x)));
        assert_eq!(
            sorted(ranges.to_vec()),
            ["under-60", "60-to-300", "300-and-up"]
        );

        let weekday = key("content.T|weekday", FactType::Time);
        // 2024-06-16 is a Sunday, 2024-06-17 a Monday, 2024-06-19 a Wednesday.
        let days = ["2024-06-16", "2024-06-19", "2024-06-17"].map(|w| time(&weekday, w));
        assert_eq!(sorted(days.to_vec()), ["Monday", "Wednesday", "Sunday"]);

        let s = key("content.S", FactType::Text);
        let texts = ["b", "B", "a", "é"].map(|v| text(&s, v));
        assert_eq!(sorted(texts.to_vec()), ["B", "a", "b", "é"]);

        let mut mixed = key("content.M", FactType::Text);
        mixed.rows = Rows::Mixed(KeyTypes::of(FactType::Text));
        let values = vec![
            text(&mixed, "a"),
            read(&mixed, FactValue::Time(read_time("2024").unwrap())),
            read(&mixed, FactValue::Num(5.0)),
        ];
        assert_eq!(sorted(values), ["5", "2024", "a"]);
    }

    /// **Two values are equal in order exactly when they are one row**: a text and a path that
    /// print alike share a place in the order and are still two rows, and neither selects the
    /// other.
    #[test]
    fn values_equal_in_order_are_one_row() {
        let s = text(&key("content.S", FactType::Text), "a/b");
        let p = read(
            &key("content.P", FactType::Path),
            FactValue::Text("a/b".to_string()),
        );
        assert_ne!(s.identity(), p.identity());
        assert_ne!(s.cmp(&p), Ordering::Equal);
        assert!(!s.selects(&p) && !p.selects(&s));
        assert_eq!(s.cmp(&s.clone()), Ordering::Equal);
    }

    /// **How coarse a value prints**: a time at a year, a month, a day or a minute, and a time of
    /// day at the minute; never a time at a second or finer, a time at a fixed grain (`|date`),
    /// or text that reads like a time.
    #[test]
    fn a_value_is_coarse_only_where_it_prints_at_its_own_coarser_grain() {
        let t = key("content.T", FactType::Time);
        for (written, grain) in [
            ("2024", Some(Grain::Year)),
            ("2024-06", Some(Grain::Month)),
            ("2024-06-15", Some(Grain::Day)),
            ("2024-06-15T14:30", Some(Grain::Minute)),
            ("2024-06-15T14:30:05", None),
            ("2024-06-15T14:30:05.25", None),
        ] {
            assert_eq!(time(&t, written).grain(), grain, "{written}");
        }
        let tod = key("content.T|time", FactType::Time);
        assert_eq!(time(&tod, "2024-06-15T18:30").grain(), Some(Grain::Minute));
        assert_eq!(time(&tod, "2024-06-15T18:30:45").grain(), None);
        let date = key("content.T|date", FactType::Time);
        assert_eq!(time(&date, "2024-06-15T18:30").grain(), None);
        let s = key("content.S", FactType::Text);
        assert_eq!(text(&s, "2024").grain(), None);
    }

    /// **What a coarse value selects**: a year the days and minutes inside it, a day its minutes,
    /// a minute its seconds, a time of day at the minute its seconds — and nothing outside its
    /// period, nothing coarser, and nothing for bare years alone. A second and a fraction in it
    /// print the same, so they are one row and neither is coarse.
    #[test]
    fn contained_finds_each_coarse_value_s_finer_values_in_its_period() {
        let t = key("content.T", FactType::Time);
        let values: Vec<RowValue> = [
            "2024",
            "2024-06-15",
            "2024-06-15T14:30",
            "2024-06-15T14:30:05",
            "2025-01-01",
            "2023",
        ]
        .iter()
        .map(|w| time(&t, w))
        .collect();
        let refs: Vec<&RowValue> = values.iter().collect();
        let mut inside = contained(&refs);
        inside.iter_mut().for_each(|found| found.sort());
        assert_eq!(
            inside,
            [vec![1, 2, 3], vec![2, 3], vec![3], vec![], vec![], vec![]]
        );

        let years: Vec<RowValue> = ["2023", "2024", "2025"]
            .iter()
            .map(|w| time(&t, w))
            .collect();
        let refs: Vec<&RowValue> = years.iter().collect();
        assert!(contained(&refs).iter().all(Vec::is_empty));

        let tod = key("content.T|time", FactType::Time);
        let times: Vec<RowValue> = [
            "2024-06-15T18:30",
            "2024-06-16T18:30:45",
            "2024-06-16T18:31:00",
        ]
        .iter()
        .map(|w| time(&tod, w))
        .collect();
        let refs: Vec<&RowValue> = times.iter().collect();
        assert_eq!(contained(&refs), [vec![1], vec![], vec![]]);
    }

    /// The two instants of the repeated autumn hour in Amsterdam read the same on the clock, so
    /// they are one value, and a day in that night holds them both.
    #[test]
    fn the_repeated_autumn_hour_is_one_period() {
        let zone = UserZone::for_test("Europe/Amsterdam");
        let t = key("content.T", FactType::Time);
        let read_on = |written: &str| {
            RowValue::read(FactValue::Time(read_time(written).unwrap()), &t, &zone)
                .unwrap()
                .unwrap()
        };
        let first = read_on("2024-10-27T00:30:00Z");
        let second = read_on("2024-10-27T01:30:00Z");
        assert_eq!(first.rendered(), second.rendered());
        let day = read_on("2024-10-27");
        assert!(day.selects(&first) && day.selects(&second));
    }

    /// **The scan stays near linear**: 10,000 minutes across a week beside its seven days, each
    /// day finding exactly its own minutes.
    #[test]
    fn contained_stays_near_linear_on_a_large_set() {
        let t = key("content.T", FactType::Time);
        let mut values: Vec<RowValue> = (0..10_000)
            .map(|m| {
                let day = 10 + m / 1440;
                let (h, min) = ((m % 1440) / 60, m % 60);
                time(&t, &format!("2024-06-{day:02}T{h:02}:{min:02}"))
            })
            .collect();
        values.extend((10..17).map(|day| time(&t, &format!("2024-06-{day:02}"))));
        let refs: Vec<&RowValue> = values.iter().collect();
        let started = std::time::Instant::now();
        let inside = contained(&refs);
        assert!(started.elapsed() < std::time::Duration::from_secs(2));
        let per_day: Vec<usize> = inside[10_000..].iter().map(Vec::len).collect();
        assert_eq!(per_day, [1440, 1440, 1440, 1440, 1440, 1440, 1360]);
    }

    /// `filter` read and bound against `key` holding facts of `base`: no problem, no cause.
    fn binds(filter: &str, base_key: &str, base: FactType) {
        let reading = read_filter(filter, 0, AliasMode::NotLoaded);
        assert!(
            reading.problems.is_empty(),
            "{filter}: {:?}",
            reading.problems
        );
        let types = HashMap::from([(base_key.to_string(), KeyTypes::of(base))]);
        let mut causes = Vec::new();
        bind_expr(
            &reading.value.unwrap(),
            &types,
            AliasMode::NotLoaded,
            &mut causes,
        );
        assert!(causes.is_empty(), "{filter}: {causes:?}");
    }

    /// **A value holding a control character** prints with its pictures, and its filter is the
    /// glob of quoted parts around a bare `?` per visible character holding one — a carriage
    /// return and line feed one `?`, a control character at either end too — wider, and read and
    /// bound as a filter however the text around it is written.
    #[test]
    fn a_control_character_makes_the_glob_around_it() {
        let note = key("content.Note", FactType::Text);
        let c1 = char::from_u32(0x85).unwrap();
        for (value, cell, glob) in [
            (
                "line1\nline2",
                "\"line1\u{240A}line2\"",
                "content.Note~\"line1\"?\"line2\"",
            ),
            (
                "a*\nb?[c]\"d",
                "\"a*\u{240A}b?[c]\"\"d\"",
                "content.Note~\"a*\"?\"b?[c]\"\"d\"",
            ),
            (
                "crlf\r\nname",
                "\"crlf\u{240D}\u{240A}name\"",
                "content.Note~\"crlf\"?\"name\"",
            ),
            ("\tlead", "\"\u{2409}lead\"", "content.Note~?\"lead\""),
            ("trail\n", "\"trail\u{240A}\"", "content.Note~\"trail\"?"),
            ("\n\n", "\"\u{240A}\u{240A}\"", "content.Note~??"),
        ] {
            let row = note.row(&text(&note, value));
            assert_eq!(row.cell, cell, "{value:?}");
            assert_eq!(
                row.filter,
                Some(RowFilter::Wider(glob.to_string())),
                "{value:?}"
            );
            binds(glob, "content.Note", FactType::Text);
        }
        let with_c1 = format!("a{c1}b");
        let row = note.row(&text(&note, &with_c1));
        assert_eq!(row.cell, "\"a\u{2426}b\"");
        assert_eq!(
            row.filter,
            Some(RowFilter::Wider("content.Note~\"a\"?\"b\"".to_string()))
        );

        let path = key("source.rel_path", FactType::Path);
        let row = path.row(&read(&path, FactValue::Path("dir/a\nb.jpg".into())));
        assert_eq!(
            row.filter,
            Some(RowFilter::Wider(
                "source.rel_path~\"dir/a\"?\"b.jpg\"".to_string()
            ))
        );
        binds(
            "source.rel_path~\"dir/a\"?\"b.jpg\"",
            "source.rel_path",
            FactType::Path,
        );

        // A stored picture is a visible character: printed as it is, exact.
        let pictured = note.row(&text(&note, "a\u{240A}b"));
        assert_eq!(
            pictured.filter,
            Some(RowFilter::Exact("content.Note=\"a\u{240A}b\"".to_string()))
        );
    }

    /// **Every row filter reads back and binds**: a value row, `(no value)`, a coarse time
    /// exactly, a root, a conjunction — the constructors' every shape, values and placeholders.
    #[test]
    fn every_row_filter_shape_reads_and_binds() {
        let t = key("content.T", FactType::Time);
        let year = time(&t, "2024");
        let exactly = t.exactly(Slot::Value(&year), Grain::Year);
        assert_eq!(
            exactly,
            RowFilter::Exact("content.T=2024 AND NOT content.T|month?".to_string())
        );
        binds(exactly.text(), "content.T", FactType::Time);
        let minute = time(&t, "2024-06-15T14:30");
        let exactly = t.exactly(Slot::Value(&minute), Grain::Minute);
        assert_eq!(
            exactly.text(),
            "content.T=\"2024-06-15 14:30\" AND NOT content.T|second?"
        );
        binds(exactly.text(), "content.T", FactType::Time);
        assert_eq!(
            t.exactly(Slot::Placeholder("<row>"), Grain::Day).text(),
            "content.T=<row> AND NOT content.T|hour?"
        );
        assert_eq!(t.no_value().text(), "NOT content.T?");
        binds(t.no_value().text(), "content.T", FactType::Time);
        assert_eq!(RowFilter::root(Some(3)).text(), "root_id=3");
        assert_eq!(RowFilter::root(None).text(), "root_id=<N>");

        let s = key("content.S", FactType::Text);
        let parts = vec![
            t.equals(Slot::Value(&year)),
            s.equals(Slot::Value(&text(&s, "2019"))),
            RowFilter::root(Some(1)),
        ];
        let all = RowFilter::all(parts);
        assert_eq!(
            all,
            RowFilter::Exact("content.T=2024 AND content.S=\"2019\" AND root_id=1".to_string())
        );
        let wider = RowFilter::all(vec![
            RowFilter::root(Some(1)),
            s.equals(Slot::Value(&text(&s, "a\nb"))),
        ]);
        assert!(!wider.is_exact());
    }
}
