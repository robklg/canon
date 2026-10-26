//! The conversion table: what a stored value becomes as another type, and **the loss
//! judgment** — whether reading text as a number drops what the text carried.
//!
//! **The loss judgment** reads what is lost, never characters. Text read as a number loses
//! what it carried where the number Canon holds is not exactly the number written
//! ([`Loss::NotExact`]), where the whole part has a leading zero (`01234`, [`Loss::LeadingZero`]),
//! or where the fraction ends in a zero (`2.50`, [`Loss::TrailingZero`]) — a trailing zero states
//! a precision, a leading zero a code's width. A change of spelling alone loses nothing: an
//! exponent's form (`1e5`, `1E5`, `1e05`) and the sign of zero (`-0`) are never a loss, though the
//! number is spelled again as `100000` and `0`. Judged in that order; the first found is named.
//!
//! **Where it does not apply**: text read as a time keeps its form, offset and precision as the
//! one reading decides them (`time::reading::read_time`), so it is unchanged or does not read; and
//! every type read as text is unchanged, written in its one spelling (`number::spelled`,
//! `TimeValue::iso_text`), which reads back as the same value.
//!
//! A number and a time are not conversions of each other, and a path is derived, never stored.

use super::fact::{FactType, FactValue};
use super::number::{number_form, read_number, NumberReadError};
use super::time::reading::{read_time, TimeReadError};

/// What a value becomes as another type.
#[derive(Debug, Clone)]
pub enum Converted {
    /// The same value, in the target type.
    Unchanged(FactValue),
    /// The nearest value the target type holds, and what the reading dropped.
    Lossy(FactValue, Loss),
    /// The value has no reading as the target type.
    DoesNotRead(NotRead),
}

/// What reading text as a number drops — the loss judgment's answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Loss {
    /// The number Canon holds is not the number written.
    NotExact,
    /// The whole part starts with a zero (`01234`).
    LeadingZero,
    /// The fraction ends in a zero (`2.50`).
    TrailingZero,
}

impl Loss {
    /// What is lost, as a noun phrase — `a leading zero`, `a trailing decimal zero` — beside
    /// the noun as `FactType::as_str` is. `None` for [`Loss::NotExact`]: what it loses is the
    /// number itself, so a statement says that whole rather than naming a part.
    pub fn name(self) -> Option<&'static str> {
        match self {
            Loss::NotExact => None,
            Loss::LeadingZero => Some("a leading zero"),
            Loss::TrailingZero => Some("a trailing decimal zero"),
        }
    }
}

/// Why a value has no reading as the target type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotRead {
    Number(NumberReadError),
    Time(TimeReadError),
}

/// A pair of types the table holds no conversion between.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotAConversion {
    NumberToTime,
    TimeToNumber,
    PathIsDerived,
}

/// `value` as type `to`: the conversion table.
pub fn convert(value: &FactValue, to: FactType) -> Result<Converted, NotAConversion> {
    use FactType as T;
    use FactValue as V;
    match (value, to) {
        (V::Text(text), T::Num) => Ok(text_to_number(text)),
        (V::Text(text), T::Time) => Ok(match read_time(text) {
            Ok(time) => Converted::Unchanged(V::Time(time)),
            Err(why) => Converted::DoesNotRead(NotRead::Time(why)),
        }),
        (V::Text(_), T::Text) | (V::Num(_), T::Num) | (V::Time(_), T::Time) => {
            Ok(Converted::Unchanged(value.clone()))
        }
        (V::Num(n), T::Text) => Ok(Converted::Unchanged(V::Text(super::number::spelled(*n)))),
        (V::Time(time), T::Text) => Ok(Converted::Unchanged(V::Text(time.iso_text()))),
        (V::Num(_), T::Time) => Err(NotAConversion::NumberToTime),
        (V::Time(_), T::Num) => Err(NotAConversion::TimeToNumber),
        (V::Text(_) | V::Num(_) | V::Time(_), T::Path)
        | (V::Path(_), T::Text | T::Num | T::Time | T::Path) => Err(NotAConversion::PathIsDerived),
    }
}

/// Text read as a number, judged for what the reading drops.
fn text_to_number(text: &str) -> Converted {
    let (number, form) = match (read_number(text), number_form(text)) {
        (Ok(number), Some(form)) => (number, form),
        (Err(why), _) => return Converted::DoesNotRead(NotRead::Number(why)),
        (Ok(_), None) => {
            return Converted::DoesNotRead(NotRead::Number(NumberReadError::NotANumber))
        }
    };
    let whole = form.whole();
    let loss = if form.exact().is_err() {
        Some(Loss::NotExact)
    } else if whole.len() > 1 && whole.starts_with('0') {
        Some(Loss::LeadingZero)
    } else if form.fraction().is_some_and(|f| f.ends_with('0')) {
        Some(Loss::TrailingZero)
    } else {
        None
    };
    match loss {
        Some(loss) => Converted::Lossy(FactValue::Num(number), loss),
        None => Converted::Unchanged(FactValue::Num(number)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::domain::time::value::{Precision, TimeForm};

    /// The class a conversion lands in, and the number it gives where it gives one.
    #[derive(Debug, PartialEq)]
    enum Class {
        Unchanged(f64),
        Lossy(f64, Loss),
        DoesNotRead,
    }

    fn as_number(text: &str) -> Class {
        match convert(&FactValue::Text(text.to_string()), FactType::Num) {
            Ok(Converted::Unchanged(FactValue::Num(n))) => Class::Unchanged(n),
            Ok(Converted::Lossy(FactValue::Num(n), loss)) => Class::Lossy(n, loss),
            Ok(Converted::DoesNotRead(NotRead::Number(_))) => Class::DoesNotRead,
            other => panic!("{text}: {other:?}"),
        }
    }

    /// **The loss judgment's verifier**: what is lost decides, never which characters differ.
    /// Each row's class is the rule's claim: an exponent's spelling and the sign of zero lose
    /// nothing; a trailing fraction zero, a leading whole-part zero and a number Canon cannot
    /// hold exactly lose what the text carried.
    #[test]
    fn the_loss_judgment_reads_what_is_lost_never_characters() {
        for (text, n) in [
            ("1e5", 1e5),
            ("1E5", 1e5),
            ("1e05", 1e5),
            ("0.5", 0.5),
            ("-0", 0.0),
        ] {
            assert_eq!(as_number(text), Class::Unchanged(n), "{text}");
        }
        for (text, n, loss) in [
            ("2.50", 2.5, Loss::TrailingZero),
            ("1.0", 1.0, Loss::TrailingZero),
            ("1.50e2", 150.0, Loss::TrailingZero),
            ("01234", 1234.0, Loss::LeadingZero),
            ("-01", -1.0, Loss::LeadingZero),
            ("00.5", 0.5, Loss::LeadingZero),
            ("9007199254740993", 9007199254740992.0, Loss::NotExact),
            ("1.0000000000000001", 1.0, Loss::NotExact),
            // Judged in order: a number not held exactly is named before its leading zero.
            ("09007199254740993", 9007199254740992.0, Loss::NotExact),
            // A leading zero before a trailing one.
            ("01.50", 1.5, Loss::LeadingZero),
        ] {
            assert_eq!(as_number(text), Class::Lossy(n, loss), "{text}");
        }
    }

    /// Text → number: unchanged, lossy with what is lost, or no reading.
    #[test]
    fn the_text_to_number_check_table() {
        assert_eq!(as_number("12"), Class::Unchanged(12.0));
        assert_eq!(as_number("2.5"), Class::Unchanged(2.5));
        assert_eq!(as_number("2.50"), Class::Lossy(2.5, Loss::TrailingZero));
        assert_eq!(as_number("01234"), Class::Lossy(1234.0, Loss::LeadingZero));
        // Digits beyond what a number holds: the nearest number held.
        assert_eq!(
            as_number("123456789012345678901"),
            Class::Lossy(123456789012345680000.0, Loss::NotExact)
        );
        for text in ["14.2.1", "misc", "+5", "1e400", ""] {
            assert_eq!(as_number(text), Class::DoesNotRead, "{text:?}");
        }
    }

    fn as_time(text: &str) -> Result<(TimeForm, Precision), TimeReadError> {
        match convert(&FactValue::Text(text.to_string()), FactType::Time) {
            Ok(Converted::Unchanged(FactValue::Time(t))) => Ok((t.form(), t.precision())),
            Ok(Converted::DoesNotRead(NotRead::Time(why))) => Err(why),
            other => panic!("{text}: {other:?}"),
        }
    }

    /// Text → time: the one reading of a time, form and precision kept, or no reading.
    #[test]
    fn the_text_to_time_check_table() {
        use Precision as P;
        use TimeForm as F;
        for (text, form, precision) in [
            ("2024:07:23 11:06:32", F::Clock, P::Second),
            ("2024-07-23T11:06:32", F::Clock, P::Second),
            ("2024-07-23 11:06:32", F::Clock, P::Second),
            ("2024-07-23T11:06:32+02:00", F::InstantWithOffset, P::Second),
            ("2024-07-23T09:06:32Z", F::InstantWithOffset, P::Second),
            ("2024-07-23T09:06:32-00:00", F::Instant, P::Second),
            ("2024:07:23 11:06:32.45", F::Clock, P::Fraction),
            ("2024-07-23", F::Clock, P::Day),
            ("2024:07:23", F::Clock, P::Day),
            ("2024-07", F::Clock, P::Month),
            ("2024", F::Clock, P::Year),
            ("2024-07-23 11:06", F::Clock, P::Minute),
        ] {
            assert_eq!(as_time(text), Ok((form, precision)), "{text}");
        }
        for text in ["0000:00:00 00:00:00", "2024-02-30", "misc", "1690000000"] {
            assert!(as_time(text).is_err(), "{text}");
        }
    }

    /// Any type → text is unchanged, and the text reads back as the same value.
    #[test]
    fn any_type_to_text_is_unchanged_and_reads_back() {
        for (n, text) in [
            (4000.0, "4000"),
            (2.5, "2.5"),
            (1e20, "100000000000000000000"),
            (-0.0, "0"),
        ] {
            match convert(&FactValue::Num(n), FactType::Text) {
                Ok(Converted::Unchanged(FactValue::Text(t))) => {
                    assert_eq!(t, text);
                    assert!(matches!(as_number(&t), Class::Unchanged(back) if back == n));
                }
                other => panic!("{n}: {other:?}"),
            }
        }
        for written in [
            "2014",
            "2014-07-23",
            "2014-07-23T11:06:32",
            "2014-07-23T11:06:32.45+02:00",
            "2014-07-23T09:06:32-00:00",
            "2014-07-23T09:06:32+00:00",
        ] {
            let time = read_time(written).unwrap();
            match convert(&FactValue::Time(time), FactType::Text) {
                Ok(Converted::Unchanged(FactValue::Text(t))) => {
                    assert_eq!(t, written);
                    let back = read_time(&t).unwrap();
                    assert_eq!(
                        (back.form(), back.precision(), back.recorded_offset()),
                        (time.form(), time.precision(), time.recorded_offset()),
                        "{written}"
                    );
                }
                other => panic!("{written}: {other:?}"),
            }
        }
        assert!(matches!(
            convert(&FactValue::Text("x".into()), FactType::Text),
            Ok(Converted::Unchanged(FactValue::Text(t))) if t == "x"
        ));
    }

    #[test]
    fn number_to_time_time_to_number_and_paths_are_not_conversions() {
        let time = FactValue::Time(read_time("2019").unwrap());
        assert!(matches!(
            convert(&FactValue::Num(2019.0), FactType::Time),
            Err(NotAConversion::NumberToTime)
        ));
        assert!(matches!(
            convert(&time, FactType::Num),
            Err(NotAConversion::TimeToNumber)
        ));
        for value in [
            FactValue::Text("a".into()),
            FactValue::Num(1.0),
            time,
            FactValue::Path("a/b".into()),
        ] {
            assert!(matches!(
                convert(&value, FactType::Path),
                Err(NotAConversion::PathIsDerived)
            ));
        }
        for to in [FactType::Text, FactType::Num, FactType::Time] {
            assert!(matches!(
                convert(&FactValue::Path("a/b".into()), to),
                Err(NotAConversion::PathIsDerived)
            ));
        }
    }

    #[test]
    fn every_loss_has_its_name_or_is_its_own_statement() {
        assert_eq!(Loss::LeadingZero.name(), Some("a leading zero"));
        assert_eq!(Loss::TrailingZero.name(), Some("a trailing decimal zero"));
        assert_eq!(Loss::NotExact.name(), None);
    }
}
