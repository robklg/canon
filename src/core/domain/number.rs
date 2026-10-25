//! The number noun's two spellings: how Canon reads text as a number ([`read_number`]) and how
//! it writes a number back ([`spelled`]). Every number a user types and every number Canon
//! prints passes through one of them, so a printed number typed back reads as the same number.
//!
//! **The grammar** is `-?digits(.digits)?([eE][+-]?digits)?` — ASCII digits, an optional
//! leading minus, an optional fraction, an optional exponent — and nothing else: no `inf` or
//! `nan`, no leading `+`, no hexadecimal, no `_` separators, no spaces.
//!
//! **Form first, fit second.** Whether text is written as a number is the grammar's answer
//! alone ([`number_form`]); whether Canon can hold it is asked after. [`read_number`] holds the
//! nearest `f64`, exactly as Rust reads the accepted text, and refuses only a number too large
//! to hold (`1e400`). [`NumberForm::exact`] asks more: that the number, held and spelled again,
//! is the same decimal number as written — so `1e400`, `1e-400` and `1.0000000000000001` fail
//! it alike, while `0.1`, `0.10` and `1e20` hold.
//!
//! **The spelling** is the shortest plain decimal that reads back as the same `f64`: never an
//! exponent, never a trailing `.0`, and `-0` written `0`.

/// Why text did not read as a number. `Display` is the user-facing sentence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumberReadError {
    NotANumber,
}

impl std::fmt::Display for NumberReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NumberReadError::NotANumber => f.write_str(
                "not written as a number (digits, optionally a leading minus, a decimal point \
                 and an exponent: 1024, -5, 3.14, 1e20)",
            ),
        }
    }
}

impl std::error::Error for NumberReadError {}

/// Read text as a number through Canon's grammar, as the nearest `f64` (see the module doc).
pub fn read_number(text: &str) -> Result<f64, NumberReadError> {
    number_form(text)
        .and_then(|form| form.nearest())
        .ok_or(NumberReadError::NotANumber)
}

/// Text written as a number in Canon's grammar, before anything asks whether Canon can hold it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NumberForm<'a> {
    text: &'a str,
}

/// A number written in the grammar that Canon cannot hold exactly as written: too large to
/// hold, too small, or with more digits than a held number keeps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotExact;

/// **The form**: whether `text` is written as a number, by the grammar alone.
pub fn number_form(text: &str) -> Option<NumberForm<'_>> {
    in_grammar(text.as_bytes()).then_some(NumberForm { text })
}

impl NumberForm<'_> {
    /// The nearest `f64`, where it is finite.
    fn nearest(&self) -> Option<f64> {
        self.text.parse::<f64>().ok().filter(|n| n.is_finite())
    }

    /// The digits before the decimal point, as written, without the sign (`01234` → `01234`,
    /// `-2.50` → `2`, `1.5e3` → `1`).
    pub fn whole(&self) -> &str {
        self.mantissa().split('.').next().unwrap_or_default()
    }

    /// The digits after the decimal point, as written, where there is one (`2.50` → `50`,
    /// `1.50e2` → `50`, `12` → none).
    pub fn fraction(&self) -> Option<&str> {
        self.mantissa()
            .split_once('.')
            .map(|(_, fraction)| fraction)
    }

    /// Whether it is written as an integer: no decimal point and no exponent (`12`, `-0`; not
    /// `12.0` or `12e0`).
    pub fn written_as_integer(&self) -> bool {
        !self.text.contains(['.', 'e', 'E'])
    }

    /// The number as written without its sign and exponent.
    fn mantissa(&self) -> &str {
        let unsigned = self.text.strip_prefix('-').unwrap_or(self.text);
        unsigned.split(['e', 'E']).next().unwrap_or_default()
    }

    /// **The fit**: the number, where Canon holds it exactly as written — held, then spelled
    /// again ([`spelled`]), it is the same decimal number.
    pub fn exact(&self) -> Result<f64, NotExact> {
        match self.nearest() {
            Some(n) if same_decimal(self.text, &spelled(n)) => Ok(n),
            Some(_) | None => Err(NotExact),
        }
    }
}

/// Whether two texts in the grammar are the same decimal number, whatever their spelling
/// (`1e20` and `100000000000000000000`, `0.10` and `0.1`, `-0` and `0`).
fn same_decimal(a: &str, b: &str) -> bool {
    decimal(a) == decimal(b)
}

/// A number in the grammar as its sign, its significant digits and the power of ten of the
/// last one; zero as no digits and no sign.
fn decimal(text: &str) -> (bool, String, i128) {
    let (negative, rest) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (mantissa, exponent) = match rest.find(['e', 'E']) {
        Some(at) => (
            rest.get(..at).unwrap_or_default(),
            rest.get(at + 1..).unwrap_or_default(),
        ),
        None => (rest, ""),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let exponent: i128 = {
        let (sign, digits) = match exponent.strip_prefix('-') {
            Some(d) => (-1, d),
            None => (1, exponent.strip_prefix('+').unwrap_or(exponent)),
        };
        // An exponent past what `i128` holds saturates; no `f64` reaches one, so the fit has
        // already refused such a number before it is compared.
        let magnitude = digits.bytes().fold(0i128, |acc, d| {
            acc.saturating_mul(10).saturating_add(i128::from(d - b'0'))
        });
        sign * magnitude
    };
    let digits = format!("{whole}{fraction}");
    let significant = digits.trim_start_matches('0');
    let trimmed = significant.trim_end_matches('0');
    if trimmed.is_empty() {
        return (false, String::new(), 0);
    }
    let trailing = (significant.len() - trimmed.len()) as i128;
    let power = exponent
        .saturating_sub(fraction.len() as i128)
        .saturating_add(trailing);
    (negative, trimmed.to_string(), power)
}

/// The one spelling of a number: the shortest plain decimal that reads back as the same
/// value — `500`, `0.1`, `100000000000000000000` — with `-0` written `0`.
pub fn spelled(n: f64) -> String {
    if n == 0.0 {
        return "0".to_string();
    }
    // `f64`'s `Display` is already the shortest text that reads back the same, and never
    // writes an exponent.
    format!("{n}")
}

/// `-?digits(.digits)?([eE][+-]?digits)?`, byte by byte.
fn in_grammar(bytes: &[u8]) -> bool {
    let mut at = 0;
    let digits = |at: &mut usize| {
        let start = *at;
        while bytes.get(*at).is_some_and(u8::is_ascii_digit) {
            *at += 1;
        }
        *at > start
    };
    if bytes.first() == Some(&b'-') {
        at += 1;
    }
    if !digits(&mut at) {
        return false;
    }
    if bytes.get(at) == Some(&b'.') {
        at += 1;
        if !digits(&mut at) {
            return false;
        }
    }
    if matches!(bytes.get(at), Some(b'e' | b'E')) {
        at += 1;
        if matches!(bytes.get(at), Some(b'+' | b'-')) {
            at += 1;
        }
        if !digits(&mut at) {
            return false;
        }
    }
    at == bytes.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_grammar_reads_these_and_nothing_else() {
        let accepted = [
            ("1024", 1024.0),
            ("-5", -5.0),
            ("2.75", 2.75),
            ("1e20", 1e20),
            ("007", 7.0),
            ("1E-3", 0.001),
            ("2.5e+3", 2500.0),
            ("0", 0.0),
        ];
        for (text, n) in accepted {
            assert_eq!(read_number(text), Ok(n), "{text}");
        }
        let refused = [
            "inf", "Infinity", "NaN", "nan", "+5", "0x1F", "1_000", " 5", "5 ", "1e", ".", ".5",
            "5.", "-", "", "1e400", "1,5", "١٢", "--5", "1e+",
        ];
        for text in refused {
            assert_eq!(
                read_number(text),
                Err(NumberReadError::NotANumber),
                "{text:?}"
            );
        }
    }

    /// **Form first, fit second**: every spelling in the grammar has the form, whatever its
    /// size; only one that holds as written has the fit.
    #[test]
    fn a_number_that_does_not_read_back_is_refused() {
        for text in ["1e400", "1e-400", "1.0000000000000001", "-1e400"] {
            let form = number_form(text).unwrap_or_else(|| panic!("{text}: no form"));
            assert_eq!(form.exact(), Err(NotExact), "{text}");
        }
        for (text, n) in [
            ("0.1", 0.1),
            ("2.75", 2.75),
            ("1e20", 1e20),
            ("0.10", 0.1),
            ("-0", 0.0),
            ("007", 7.0),
        ] {
            assert_eq!(number_form(text).map(|f| f.exact()), Some(Ok(n)), "{text}");
        }
        assert!(number_form("3.14").is_some_and(|f| f.exact().is_ok()));
        for text in ["big", "+5", "1e", ""] {
            assert_eq!(number_form(text), None, "{text:?}");
        }
        // Import's reading is the nearest number: only a number too large to hold is refused.
        assert_eq!(read_number("1e-400"), Ok(0.0));
        assert_eq!(read_number("1.0000000000000001"), Ok(1.0));
    }

    #[test]
    fn a_form_gives_its_whole_part_and_fraction_as_written() {
        for (text, whole, fraction) in [
            ("01234", "01234", None),
            ("-2.50", "2", Some("50")),
            ("1.50e2", "1", Some("50")),
            ("1E5", "1", None),
            ("-0", "0", None),
            ("00.5", "00", Some("5")),
        ] {
            let form = number_form(text).unwrap_or_else(|| panic!("{text}: no form"));
            assert_eq!((form.whole(), form.fraction()), (whole, fraction), "{text}");
        }
    }

    #[test]
    fn a_form_says_whether_it_is_written_as_an_integer() {
        for (text, integer) in [
            ("12", true),
            ("-0", true),
            ("9007199254740993", true),
            ("12.0", false),
            ("9007199254740993e0", false),
            ("1E5", false),
            ("-0.00", false),
        ] {
            let form = number_form(text).unwrap_or_else(|| panic!("{text}: no form"));
            assert_eq!(form.written_as_integer(), integer, "{text}");
        }
    }

    #[test]
    fn same_decimal_compares_values_not_spellings() {
        for (a, b) in [
            ("1e20", "100000000000000000000"),
            ("0.10", "0.1"),
            ("-0", "0"),
            ("0.0e5", "0"),
            ("1.5E+3", "1500"),
            ("150e-1", "15"),
            ("007.50", "7.5"),
        ] {
            assert!(same_decimal(a, b), "{a} {b}");
        }
        for (a, b) in [
            ("1", "-1"),
            ("1e-400", "0"),
            ("1.0000000000000001", "1"),
            ("15", "1.5"),
        ] {
            assert!(!same_decimal(a, b), "{a} {b}");
        }
    }

    #[test]
    fn a_number_is_spelled_as_the_shortest_plain_decimal() {
        let cases = [
            (1e20, "100000000000000000000"),
            (2f64.powi(60), "1152921504606847000"),
            (-0.0, "0"),
            (0.0, "0"),
            (500.0, "500"),
            (0.1, "0.1"),
            (-5.25, "-5.25"),
            (1e-7, "0.0000001"),
        ];
        for (n, text) in cases {
            assert_eq!(spelled(n), text, "{n}");
        }
    }

    /// The spelling and the reading are one pair: every spelled number reads back as itself.
    #[test]
    fn every_spelled_number_reads_back_as_itself() {
        let awkward = [
            0.1 + 0.2,
            1.0 / 3.0,
            f64::MAX,
            f64::MIN_POSITIVE,
            5e-324,
            -1e-300,
            2f64.powi(63),
            2f64.powi(53) + 1.0,
            123_456_789.012_345_68,
            -0.0,
            1e17,
            1e16 + 1.0,
        ];
        for n in awkward {
            let text = spelled(n);
            let back = read_number(&text).unwrap_or_else(|e| panic!("{text}: {e}"));
            assert_eq!(back.to_bits(), (n + 0.0).to_bits(), "{n} spelled {text}");
            // And it is held exactly as spelled, so a printed number typed back is never refused.
            assert_eq!(
                number_form(&text).map(|f| f.exact()),
                Some(Ok(back)),
                "{text}"
            );
        }
    }
}
