//! The number noun's two spellings: how Canon reads text as a number ([`read_number`]) and how
//! it writes a number back ([`spelled`]). Every number a user types and every number Canon
//! prints passes through one of them, so a printed number typed back reads as the same number.
//!
//! **The grammar** is `-?digits(.digits)?([eE][+-]?digits)?` — ASCII digits, an optional
//! leading minus, an optional fraction, an optional exponent — and nothing else: no `inf` or
//! `nan`, no leading `+`, no hexadecimal, no `_` separators, no spaces. A number too large to
//! hold (`1e400`) does not read either. The grammar is checked first; the standard library
//! then reads the accepted text, so the value is the nearest `f64`, exactly as Rust reads it.
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

/// Read text as a number through Canon's grammar (see the module doc).
pub fn read_number(text: &str) -> Result<f64, NumberReadError> {
    if !in_grammar(text.as_bytes()) {
        return Err(NumberReadError::NotANumber);
    }
    match text.parse::<f64>() {
        Ok(n) if n.is_finite() => Ok(n),
        _ => Err(NumberReadError::NotANumber),
    }
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
        }
    }
}
