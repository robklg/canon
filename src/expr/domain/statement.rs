//! **The statement grammar**: one way to state a fact about a value and its type, built from
//! typed parts, so a new sentence is a composition of approved parts rather than another
//! wording to check.
//!
//! The one rule: every statement is a subject, `is` or `are`, and a complement — or a subject,
//! `cannot be` and a participle ([`Complement::CannotBeConvertedTo`]). The verb comes from the
//! subject's count alone, and no other verb can be written. A value subject prints through the
//! printing rule ([`printed_value`]) and a key subject through [`printed_key`], so no caller can
//! print either another way; a value the rule leaves bare is set in backticks, one it quotes
//! stands in its own quotes. The type nouns and the loss names are the fact noun's vocabulary
//! (`FactType::noun`, `Loss::name`).
//!
//! What a statement leaves out is the caller's: where the problem sits and who fixes it — a
//! compiler keeps its message apart from its help the same way. What can be done about a type
//! (that it cannot change, a route through another type, a modifier that asks without changing
//! anything stored) is that help. Sentences about acts and their history are outside it.

use std::fmt;

use crate::core::domain::conversion::Loss;
use crate::core::domain::fact::{Count, FactType, KeyTypes};
use crate::core::domain::format::format_count;

use super::print::{mixed_types, printed_key, printed_value};

/// What a statement is about.
#[derive(Debug, Clone, Copy)]
pub enum Subject<'a> {
    /// A stored value, printed for a reader of its own type.
    Value { text: &'a str, as_type: FactType },
    /// A full stored key.
    Key(&'a str),
    /// Any value of a type: `a time`.
    Type(FactType),
    /// A count of things: `4 facts`.
    Counted(u64, Noun),
    /// Most of the values just counted: `most of these`.
    MostOfThese,
    /// Values a later import brings for the key, by what they are: `untyped dates imported for
    /// this key`.
    Arriving(&'a [Arriving]),
}

/// A thing a statement counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Noun {
    Fact,
}

impl Noun {
    fn word(self, n: Count) -> &'static str {
        match (self, n) {
            (Noun::Fact, Count::One) => "fact",
            (Noun::Fact, Count::Many) => "facts",
        }
    }
}

/// A kind of value an import can bring for a key — each spoken in the plural.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Arriving {
    /// Strings with no type hint that are dates.
    UntypedDates,
    /// Strings with no type hint that are numbers.
    UntypedNumbers,
    /// Strings with no type hint that are numbers with a leading or trailing zero.
    UntypedNumbersWithAZero,
    /// Any string with no type hint.
    UntypedValues,
    /// Numbers written as JSON numbers.
    JsonNumbers,
    /// Values given the `datetime` or the `duration` hint.
    DatetimeOrDurationHinted,
    /// Values given the `duration` hint.
    DurationHinted,
}

impl Arriving {
    fn words(self) -> &'static str {
        match self {
            Arriving::UntypedDates => "untyped dates",
            Arriving::UntypedNumbers => "numbers written as text",
            Arriving::UntypedNumbersWithAZero => {
                "numbers written as text with a leading zero or a trailing decimal zero"
            }
            Arriving::UntypedValues => "untyped values",
            Arriving::JsonNumbers => "JSON numbers",
            Arriving::DatetimeOrDurationHinted => "values with the datetime or duration hint",
            Arriving::DurationHinted => "values with the duration hint",
        }
    }
}

/// What a statement says of its subject.
#[derive(Debug, Clone, Copy)]
pub enum Complement<'a> {
    /// `of type time`.
    OfType(FactType),
    /// `of mixed type (time and text)`.
    OfMixedType(&'a KeyTypes),
    /// `not a time` / `not times`.
    Not(FactType),
    /// What text read as a number loses: `a number with a leading zero`, or `a number Canon
    /// cannot represent exactly`.
    WithLoss(Loss),
    /// `not a time Canon can read`: a stored time whose value Canon cannot read.
    NotReadable(FactType),
    /// `written like durations` — the form only, never the meaning.
    WrittenLikeDurations,
    /// `cannot be converted to a number`, the one participle.
    CannotBeConvertedTo(FactType),
    /// `stored as times`.
    StoredAs(FactType),
    /// `skipped`.
    Skipped,
}

/// A subject and what is said of it.
#[derive(Debug, Clone, Copy)]
pub struct Statement<'a> {
    pub subject: Subject<'a>,
    pub complement: Complement<'a>,
}

impl Subject<'_> {
    /// How many things the subject speaks of — the one thing the verb follows.
    fn count(&self) -> Count {
        match self {
            Subject::Value { .. } | Subject::Key(_) | Subject::Type(_) => Count::One,
            Subject::Counted(n, _) => Count::of(*n),
            Subject::MostOfThese | Subject::Arriving(_) => Count::Many,
        }
    }

    fn write(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Subject::Value { text, as_type } => {
                let printed = printed_value(text, *as_type);
                if printed.starts_with('"') {
                    f.write_str(&printed)
                } else {
                    write!(f, "`{printed}`")
                }
            }
            Subject::Key(key) => f.write_str(&printed_key(key)),
            Subject::Type(t) => f.write_str(t.noun(Count::One)),
            Subject::Counted(n, noun) => {
                write!(f, "{} {}", format_count(n), noun.word(Count::of(*n)))
            }
            Subject::MostOfThese => f.write_str("most of these"),
            Subject::Arriving(kinds) => {
                let words: Vec<&str> = kinds.iter().map(|k| k.words()).collect();
                match words.split_last() {
                    Some((last, [])) => f.write_str(last)?,
                    Some((last, rest)) => write!(f, "{} and {last}", rest.join(", "))?,
                    None => f.write_str("values")?,
                }
                f.write_str(" imported for this key")
            }
        }
    }
}

impl Complement<'_> {
    fn write(&self, n: Count, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Complement::OfType(t) => write!(f, "of type {t}"),
            Complement::OfMixedType(types) => write!(f, "of mixed type ({})", mixed_types(types)),
            Complement::Not(t) => write!(f, "not {}", t.noun(n)),
            Complement::WithLoss(loss) => {
                let number = FactType::Num.noun(n);
                match loss.name() {
                    Some(lost) => write!(f, "{number} with {lost}"),
                    None => write!(f, "{number} Canon cannot represent exactly"),
                }
            }
            Complement::NotReadable(t) => write!(f, "not {} Canon can read", t.noun(n)),
            Complement::WrittenLikeDurations => match n {
                Count::One => f.write_str("written like a duration"),
                Count::Many => f.write_str("written like durations"),
            },
            Complement::CannotBeConvertedTo(t) => {
                write!(f, "converted to {}", t.noun(Count::One))
            }
            Complement::StoredAs(t) => write!(f, "stored as {}", t.noun(n)),
            Complement::Skipped => f.write_str("skipped"),
        }
    }

    /// The verb the complement takes after a subject of count `n`.
    fn verb(&self, n: Count) -> &'static str {
        match (self, n) {
            (Complement::CannotBeConvertedTo(_), Count::One | Count::Many) => "cannot be",
            (
                Complement::OfType(_)
                | Complement::OfMixedType(_)
                | Complement::Not(_)
                | Complement::WithLoss(_)
                | Complement::NotReadable(_)
                | Complement::WrittenLikeDurations
                | Complement::StoredAs(_)
                | Complement::Skipped,
                Count::One,
            ) => "is",
            (
                Complement::OfType(_)
                | Complement::OfMixedType(_)
                | Complement::Not(_)
                | Complement::WithLoss(_)
                | Complement::NotReadable(_)
                | Complement::WrittenLikeDurations
                | Complement::StoredAs(_)
                | Complement::Skipped,
                Count::Many,
            ) => "are",
        }
    }
}

impl fmt::Display for Statement<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let n = self.subject.count();
        self.subject.write(f)?;
        write!(f, " {} ", self.complement.verb(n))?;
        self.complement.write(n, f)
    }
}

/// A key's types in the words the Type column and the refusals use: its one type, or `mixed`
/// with the types it holds — `mixed (time and text)`.
pub fn type_words(types: &KeyTypes) -> String {
    match types.single() {
        Some(t) => t.as_str().to_string(),
        None => format!("mixed ({})", mixed_types(types)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn said(subject: Subject, complement: Complement) -> String {
        Statement {
            subject,
            complement,
        }
        .to_string()
    }

    /// The arm each complement is, so the table below can be checked to hold every one: a new
    /// arm fails to build here until it has an index, then fails the table until it has a row.
    fn arm(c: &Complement) -> usize {
        match c {
            Complement::OfType(_) => 0,
            Complement::OfMixedType(_) => 1,
            Complement::Not(_) => 2,
            Complement::WithLoss(_) => 3,
            Complement::NotReadable(_) => 4,
            Complement::WrittenLikeDurations => 5,
            Complement::CannotBeConvertedTo(_) => 6,
            Complement::StoredAs(_) => 7,
            Complement::Skipped => 8,
        }
    }
    const ARMS: usize = 9;

    /// **Every complement reads in both forms**: under a subject of one and of many, each
    /// written out from the rule — `is` with one, `are` with many, `cannot be` with either.
    #[test]
    fn every_complement_reads_in_both_forms() {
        let mixed = KeyTypes::from_counts([(FactType::Time, 3), (FactType::Text, 1)]).unwrap();
        let table: Vec<(Complement, &str, &str)> = vec![
            (
                Complement::OfType(FactType::Time),
                "1 fact is of type time",
                "3 facts are of type time",
            ),
            (
                Complement::OfMixedType(&mixed),
                "1 fact is of mixed type (time and text)",
                "3 facts are of mixed type (time and text)",
            ),
            (
                Complement::Not(FactType::Time),
                "1 fact is not a time",
                "3 facts are not times",
            ),
            (
                Complement::Not(FactType::Text),
                "1 fact is not text",
                "3 facts are not text",
            ),
            (
                Complement::WithLoss(Loss::LeadingZero),
                "1 fact is a number with a leading zero",
                "3 facts are numbers with a leading zero",
            ),
            (
                Complement::WithLoss(Loss::TrailingZero),
                "1 fact is a number with a trailing decimal zero",
                "3 facts are numbers with a trailing decimal zero",
            ),
            (
                Complement::WithLoss(Loss::NotExact),
                "1 fact is a number Canon cannot represent exactly",
                "3 facts are numbers Canon cannot represent exactly",
            ),
            (
                Complement::NotReadable(FactType::Time),
                "1 fact is not a time Canon can read",
                "3 facts are not times Canon can read",
            ),
            (
                Complement::WrittenLikeDurations,
                "1 fact is written like a duration",
                "3 facts are written like durations",
            ),
            (
                Complement::CannotBeConvertedTo(FactType::Num),
                "1 fact cannot be converted to a number",
                "3 facts cannot be converted to a number",
            ),
            (
                Complement::StoredAs(FactType::Time),
                "1 fact is stored as a time",
                "3 facts are stored as times",
            ),
            (
                Complement::StoredAs(FactType::Text),
                "1 fact is stored as text",
                "3 facts are stored as text",
            ),
            (
                Complement::Skipped,
                "1 fact is skipped",
                "3 facts are skipped",
            ),
        ];
        let mut seen = vec![false; ARMS];
        for (complement, one, many) in &table {
            seen[arm(complement)] = true;
            assert_eq!(&said(Subject::Counted(1, Noun::Fact), *complement), one);
            assert_eq!(&said(Subject::Counted(3, Noun::Fact), *complement), many);
        }
        assert!(seen.iter().all(|&s| s), "a complement has no row: {seen:?}");
    }

    /// **A value subject prints through the printing rule**: bare text the reader reads back
    /// as itself stands in backticks; text it would not stands in its own quotes, an inner `"`
    /// doubled — never raw.
    #[test]
    fn a_value_subject_prints_through_the_printing_rule() {
        let not_a_number = Complement::Not(FactType::Num);
        let value = |text| Subject::Value {
            text,
            as_type: FactType::Text,
        };
        assert_eq!(said(value("misc"), not_a_number), "`misc` is not a number");
        assert_eq!(
            said(value("1:23:45"), not_a_number),
            "`1:23:45` is not a number"
        );
        assert_eq!(
            said(
                value("0000:00:00 00:00:00"),
                Complement::Not(FactType::Time)
            ),
            r#""0000:00:00 00:00:00" is not a time"#
        );
        assert_eq!(
            said(value("a \"b\""), not_a_number),
            r#""a ""b""" is not a number"#
        );
        assert_eq!(
            said(value("01234"), Complement::WithLoss(Loss::LeadingZero)),
            r#""01234" is a number with a leading zero"#
        );
        assert_eq!(
            said(
                Subject::Key("content.File Size"),
                Complement::OfType(FactType::Num)
            ),
            r#""content.File Size" is of type number"#
        );
    }

    /// **Only the subject decides `is` or `are`**: one value, key, type or counted thing takes
    /// `is`; none, several, most of these and what an import brings take `are`.
    #[test]
    fn only_the_subject_decides_is_or_are() {
        let complement = Complement::Not(FactType::Time);
        let arriving = [Arriving::UntypedDates];
        let singular = [
            Subject::Value {
                text: "x",
                as_type: FactType::Text,
            },
            Subject::Key("content.Make"),
            Subject::Type(FactType::Num),
            Subject::Counted(1, Noun::Fact),
        ];
        let plural = [
            Subject::Counted(0, Noun::Fact),
            Subject::Counted(2, Noun::Fact),
            Subject::MostOfThese,
            Subject::Arriving(&arriving),
        ];
        for subject in singular {
            assert!(
                said(subject, complement).contains(" is not "),
                "{subject:?}"
            );
        }
        for subject in plural {
            assert!(
                said(subject, complement).contains(" are not "),
                "{subject:?}"
            );
        }
    }

    #[test]
    fn what_an_import_brings_is_listed_in_one_subject() {
        let one = [Arriving::UntypedDates];
        let two = [Arriving::UntypedValues, Arriving::JsonNumbers];
        assert_eq!(
            said(
                Subject::Arriving(&one),
                Complement::StoredAs(FactType::Time)
            ),
            "untyped dates imported for this key are stored as times"
        );
        assert_eq!(
            said(
                Subject::Arriving(&two),
                Complement::StoredAs(FactType::Text)
            ),
            "untyped values and JSON numbers imported for this key are stored as text"
        );
        assert_eq!(
            said(
                Subject::Arriving(&[Arriving::DurationHinted]),
                Complement::StoredAs(FactType::Num)
            ),
            "values with the duration hint imported for this key are stored as numbers"
        );
        assert_eq!(
            said(
                Subject::Type(FactType::Time),
                Complement::CannotBeConvertedTo(FactType::Num)
            ),
            "a time cannot be converted to a number"
        );
        assert_eq!(
            said(Subject::MostOfThese, Complement::Not(FactType::Num)),
            "most of these are not numbers"
        );
    }

    #[test]
    fn type_words_name_one_type_or_mixed_with_its_types() {
        let one = KeyTypes::from_counts([(FactType::Time, 2)]).unwrap();
        let mixed = KeyTypes::from_counts([(FactType::Time, 3), (FactType::Text, 1)]).unwrap();
        assert_eq!(type_words(&one), "time");
        assert_eq!(type_words(&mixed), "mixed (time and text)");
    }
}
