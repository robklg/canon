//! **The printing rule**: how Canon writes a value or a key so that, typed back into
//! `--where`, it reads as the same value or key. The writer asks the reader: a value prints
//! bare exactly when the reader reads it back after `=` as one bare value with the same
//! text **and** the binder reads that bare text as the same value for its type; a key prints
//! bare exactly when the reader reads it back as one key, in a filter and as a report key, with
//! no accessor or modifier and the same base key. Otherwise it prints in double quotes, an inner `"` doubled — which the
//! grammar reads back as the text inside. No character list lives here, so the writer cannot
//! drift from the reader.

use crate::core::domain::fact::{FactType, KeyTypes};
use crate::core::domain::format::{format_count, shell_quote};
use crate::core::domain::number::{number_form, spelled};
use crate::core::domain::time::reading::read_time;

use super::bind::{input_type, InputType};
use super::read::{read_keys, reads_as_one_bare_key, reads_as_one_bare_value, AliasMode, Grammar};
use super::transform::BucketRange;

/// A value as Canon prints it for the reader of `read_as`: bare where the bare text reads
/// back as the same value of that type, otherwise double-quoted.
pub fn printed_value(text: &str, read_as: FactType) -> String {
    let same_value = match read_as {
        FactType::Text | FactType::Path => input_type(text) == InputType::Text,
        FactType::Num => number_form(text).is_some_and(|form| form.exact().is_ok()),
        FactType::Time => read_time(text).is_ok(),
    };
    if same_value && reads_back_bare(text) {
        text.to_string()
    } else {
        quoted(text)
    }
}

/// A word that names itself whatever it is compared with — a bucket's range, a part's
/// name: bare where the reader reads it back as one bare value, otherwise quoted.
pub(in crate::expr) fn printed_word(text: &str) -> String {
    if reads_back_bare(text) {
        text.to_string()
    } else {
        quoted(text)
    }
}

/// A full stored key as Canon prints it: bare where the bare text reads back as that same
/// key, otherwise double-quoted (`"content.File Size"`).
pub fn printed_key(key: &str) -> String {
    let same_key = reads_as_one_bare_key(key) && is_its_own_base_key(key);
    if same_key {
        key.to_string()
    } else {
        quoted(key)
    }
}

/// Whether `key`, read as a report reads a key, is one key with no accessor and no modifier
/// whose base key is `key` itself.
fn is_its_own_base_key(key: &str) -> bool {
    let reading = read_keys(key, 0, Grammar::Key, AliasMode::NotLoaded);
    reading.problems.is_empty()
        && matches!(reading.value.as_deref(),
            Some([read]) if !read.has_transforms() && read.base == key)
}

/// The command that shows a key's values and converts it to one type, as printed: backticked,
/// the full stored key quoted for the shell only, so it runs as pasted
/// (`` `canon facts convert 'content.File Size'` ``), and the target type where one is named
/// (`` `canon facts convert content.Width --to text` ``).
pub fn convert_command(key: &str, to: Option<FactType>) -> String {
    match to {
        Some(to) => format!(
            "`canon facts convert {} --to {}`",
            shell_quote(key),
            to.as_str()
        ),
        None => format!("`canon facts convert {}`", shell_quote(key)),
    }
}

/// The types a key of mixed type holds, in the words the type column uses, the most common
/// first: `text and number`; `text, number and time`.
pub(in crate::expr) fn mixed_types(types: &KeyTypes) -> String {
    let mut counts = types.counts().to_vec();
    counts.sort_by_key(|&(_, n)| std::cmp::Reverse(n));
    let words: Vec<&str> = counts.iter().map(|(t, _)| t.as_str()).collect();
    match words.split_last() {
        Some((last, [])) => last.to_string(),
        Some((last, rest)) => format!("{} and {last}", rest.join(", ")),
        None => String::new(),
    }
}

/// The statement that keys are of mixed type, with their types. One key is named as Canon
/// prints it; several are counted, then named one per line. Spoken here once, beside the
/// printing rule its keys print through, for the surfaces that list such keys: `facts`'
/// listing and `cluster`'s summary. A refused filter states its own line per clause and reads
/// only the way out. Empty for no keys.
pub fn mixed_type_statement(keys: &[(&str, &KeyTypes)]) -> Vec<String> {
    match keys {
        [] => Vec::new(),
        [(key, types)] => vec![format!(
            "{} is of mixed type ({}); filters and patterns support one type only.",
            printed_key(key),
            mixed_types(types)
        )],
        keys => std::iter::once(format!(
            "{} keys are of mixed type; filters and patterns support one type only:",
            format_count(keys.len())
        ))
        .chain(
            keys.iter()
                .map(|(key, types)| format!("  {} ({})", printed_key(key), mixed_types(types))),
        )
        .collect(),
    }
}

/// The way out of a mixed type: a header, then one command per key ([`convert_command`]),
/// indented under the header; empty for no keys. A manifest writes these lines into its
/// comments, and the terminal prints them.
pub fn mixed_type_way_out(keys: &[(&str, &KeyTypes)]) -> Vec<String> {
    let header = match keys {
        [] => return Vec::new(),
        [_] => "To see its values and convert it to one type:",
        _ => "To see their values and convert each to one type:",
    };
    std::iter::once(header.to_string())
        .chain(
            keys.iter()
                .map(|(key, _)| format!("  {}", convert_command(key, None))),
        )
        .collect()
}

/// **A bucket's label**, the one printed form of its range in a report, a rejection and a
/// directory: letters, digits, `-` and `.` only, so every drive holds it as a name and a filter
/// reads it bare. Plain `|bucket`: `0`, then powers of ten joined by `-to-` (`1-to-10`,
/// `1K-to-10K`, `0.00001-to-0.0001`); `|bucket(a,b,…)`: `under-a`, `a-to-b`, `b-and-up`, each
/// threshold spelled as a number is and a number below zero written with `minus-`, so no label
/// starts with `-` and `-` never means both.
pub(in crate::expr) fn bucket_label(range: &BucketRange) -> String {
    match range {
        BucketRange::Zero => "0".to_string(),
        BucketRange::PowerOfTen(e) => {
            format!(
                "{}-to-{}",
                power_of_ten_bound(*e),
                power_of_ten_bound(e + 1)
            )
        }
        BucketRange::Below(t) => format!("under-{}", threshold(*t)),
        BucketRange::Between(a, b) => format!("{}-to-{}", threshold(*a), threshold(*b)),
        BucketRange::From(t) => format!("{}-and-up", threshold(*t)),
    }
}

/// Every label `|bucket(a,b,…)` gives, in order.
pub(in crate::expr) fn threshold_bucket_labels(thresholds: &[f64]) -> Vec<String> {
    let (Some(&first), Some(&last)) = (thresholds.first(), thresholds.last()) else {
        return Vec::new();
    };
    std::iter::once(BucketRange::Below(first))
        .chain(
            thresholds
                .windows(2)
                .map(|pair| BucketRange::Between(pair[0], pair[1])),
        )
        .chain(std::iter::once(BucketRange::From(last)))
        .map(|range| bucket_label(&range))
        .collect()
}

/// Whether `label` is one plain `|bucket` gives: its lower bound read, its range spelled, and
/// the two the same.
pub(in crate::expr) fn is_plain_bucket_label(label: &str) -> bool {
    if label == "0" {
        return true;
    }
    let Some((lower, _)) = label.split_once("-to-") else {
        return false;
    };
    let exponent = match lower.strip_prefix("0.") {
        Some(fraction) => match fraction.strip_suffix('1') {
            Some(zeros) if zeros.bytes().all(|b| b == b'0') => -(fraction.len() as i64),
            Some(_) | None => return false,
        },
        None => {
            let (digits, suffix) = match lower.char_indices().last() {
                Some((at, 'K')) => (lower.get(..at), 3),
                Some((at, 'M')) => (lower.get(..at), 6),
                Some((at, 'G')) => (lower.get(..at), 9),
                _ => (Some(lower), 0),
            };
            match digits.and_then(|d| d.strip_prefix('1')) {
                Some(zeros) if zeros.bytes().all(|b| b == b'0') => zeros.len() as i64 + suffix,
                Some(_) | None => return false,
            }
        }
    };
    i32::try_from(exponent).is_ok_and(|e| bucket_label(&BucketRange::PowerOfTen(e)) == label)
}

/// A power of ten as a bucket bound: below 1 as a decimal (`0.0001`), 1 to 999 as digits, then
/// `K`, `M` and `G`, `G` growing past a thousand (`1000G`).
fn power_of_ten_bound(e: i32) -> String {
    let ones = |n: i32| format!("1{}", "0".repeat(n.max(0) as usize));
    match e {
        e if e < 0 => format!("0.{}1", "0".repeat((-e - 1) as usize)),
        0..=2 => ones(e),
        3..=5 => format!("{}K", ones(e - 3)),
        6..=8 => format!("{}M", ones(e - 6)),
        e => format!("{}G", ones(e - 9)),
    }
}

/// A threshold as a label writes it: the number's own spelling, `minus-` for a leading `-`.
fn threshold(t: f64) -> String {
    let spelled = spelled(t);
    match spelled.strip_prefix('-') {
        Some(magnitude) => format!("minus-{magnitude}"),
        None => spelled,
    }
}

/// The reader's half: one bare value with the same text.
fn reads_back_bare(text: &str) -> bool {
    !text.is_empty() && reads_as_one_bare_value(text)
}

/// Double quotes, an inner `"` doubled.
pub(in crate::expr) fn quoted(text: &str) -> String {
    format!("\"{}\"", text.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::core::domain::fact::KeyTypes;
    use crate::core::domain::number::read_number;
    use crate::expr::domain::bind::{bind_expr, Bound, Test};
    use crate::expr::domain::read::read_filter;

    /// The value the binder reads from `k=<printed>` against a key of type `t`.
    fn read_back(printed: &str, t: FactType) -> Test {
        let types = HashMap::from([(
            "content.k".to_string(),
            KeyTypes::from_counts([(t, 1)]).unwrap(),
        )]);
        let text = format!("k={printed}");
        let reading = read_filter(&text, 0, AliasMode::NotLoaded);
        assert!(
            reading.problems.is_empty(),
            "{text}: {:?}",
            reading.problems
        );
        let expr = reading
            .value
            .unwrap_or_else(|| panic!("{text}: nothing read"));
        let mut causes = Vec::new();
        match bind_expr(&expr, &types, AliasMode::NotLoaded, &mut causes) {
            Bound::Compare { test, .. } if causes.is_empty() => test,
            other => panic!("{text}: {other:?} {causes:?}"),
        }
    }

    /// **The printing rule's pin**: every value prints so that, typed back after `=` against
    /// its type, it reads as itself; every key prints so that it reads back as that key. The
    /// expected spellings are the printing rule's own examples.
    #[test]
    fn every_printed_value_reads_back_as_itself() {
        let quoted_text = [
            ("Canon EOS 5D Mark II", r#""Canon EOS 5D Mark II""#),
            ("<60", r#""<60""#),
            ("", r#""""#),
            (" a ", r#"" a ""#),
            (r#"say "hi""#, r#""say ""hi""""#),
            ("it's", r#""it's""#),
            ("icon@2x.png", r#""icon@2x.png""#),
            ("a,b", r#""a,b""#),
            // After `=` a wildcard character is read as a wildcard, so text holding one prints
            // quoted.
            ("a?b", r#""a?b""#),
            ("[draft]", r#""[draft]""#),
            ("2019", r#""2019""#),
            ("2024-06-15", r#""2024-06-15""#),
            ("0500", r#""0500""#),
            ("18:00", r#""18:00""#),
            ("1e400", r#""1e400""#),
            ("2019:05:04 10:12:00", r#""2019:05:04 10:12:00""#),
        ];
        // A value that reads as no number, time or time of day prints bare, however it looks.
        let bare_text = [
            "DMC-FZ38",
            "IMG_0001.jpg",
            "AND",
            "2024e",
            "1.2.3",
            "12:3",
            "2024-06-15_beach.jpg",
        ];
        for (value, printed) in quoted_text {
            assert_eq!(printed_value(value, FactType::Text), printed, "{value:?}");
        }
        for value in bare_text {
            assert_eq!(printed_value(value, FactType::Text), value, "{value:?}");
        }
        for (value, _) in quoted_text {
            let printed = printed_value(value, FactType::Text);
            assert_eq!(
                read_back(&printed, FactType::Text),
                Test::Text(value.into())
            );
        }
        for value in bare_text {
            assert_eq!(read_back(value, FactType::Text), Test::Text(value.into()));
        }

        // Under a time or a number header, values print bare where they read so.
        for (value, printed) in [
            ("2024", "2024"),
            ("2024-06", "2024-06"),
            ("2024-06-15", "2024-06-15"),
            ("2024-06-15 14:30", r#""2024-06-15 14:30""#),
            ("2024-06-15 14:30:05", r#""2024-06-15 14:30:05""#),
        ] {
            assert_eq!(printed_value(value, FactType::Time), printed, "{value}");
            assert_eq!(
                read_back(&printed_value(value, FactType::Time), FactType::Time),
                Test::Period(read_time(value).unwrap())
            );
        }
        for value in ["1024", "-5", "0.1", "100000000000000000000"] {
            assert_eq!(printed_value(value, FactType::Num), value);
            assert_eq!(
                read_back(value, FactType::Num),
                Test::Number(read_number(value).unwrap())
            );
        }

        // Keys: bare where the bare text reads back as the same key.
        for (key, printed) in [
            ("content.Make", "content.Make"),
            ("source.mtime", "source.mtime"),
            ("content.File Size", r#""content.File Size""#),
            // A name takes `-`, so it reads back bare.
            ("content.XMP-dc:Subject", "content.XMP-dc:Subject"),
            (r#"content.a"b"#, r#""content.a""b""#),
            ("content.a|b", r#""content.a|b""#),
            ("content.icon@2x", r#""content.icon@2x""#),
        ] {
            assert_eq!(printed_key(key), printed, "{key}");
            let reading = read_keys(&printed_key(key), 0, Grammar::Key, AliasMode::NotLoaded);
            assert!(reading.problems.is_empty(), "{key}");
            let read = &reading.value.unwrap()[0];
            assert_eq!((read.base.as_str(), read.has_transforms()), (key, false));
        }
    }

    /// Plain `|bucket`'s labels, each bound spelled exactly: below 1 as a decimal, then digits,
    /// then `K`, `M`, `G`, `G` growing past a thousand; and every label is read back as one.
    #[test]
    fn every_plain_bucket_label_spells_its_bounds_exactly() {
        use crate::expr::domain::transform::bucket_range;
        let label = |n: f64| bucket_label(&bucket_range(n, &[]).unwrap());
        let tiny = format!("0.{}1-to-0.{}1", "0".repeat(299), "0".repeat(298));
        for (n, expected) in [
            (0.0, "0"),
            (0.00005, "0.00001-to-0.0001"),
            (0.5, "0.1-to-1"),
            (1.0, "1-to-10"),
            (1000.0, "1K-to-10K"),
            (2e8, "100M-to-1G"),
            (5e12, "1000G-to-10000G"),
            (1e-300, tiny.as_str()),
        ] {
            assert_eq!(label(n), expected, "{n}");
            assert!(is_plain_bucket_label(expected), "{expected}");
        }
        for not_a_label in [
            "1-10",
            "1K-10K",
            "0-0",
            "2-to-20",
            "1-to-100",
            "-10-to-100",
            "0.0-to-0.1",
            "01-to-10",
            "1K-to-10M",
            "minus-1-to-0",
            "",
        ] {
            assert!(!is_plain_bucket_label(not_a_label), "{not_a_label}");
        }
    }

    /// Thresholds read `under-`, `-to-` and `-and-up`, a number below zero written `minus-`, so
    /// no label starts with `-`.
    #[test]
    fn threshold_labels_say_under_to_and_up() {
        assert_eq!(
            threshold_bucket_labels(&[-90.0, 0.0, 90.0]),
            ["under-minus-90", "minus-90-to-0", "0-to-90", "90-and-up"]
        );
        assert_eq!(
            threshold_bucket_labels(&[10.0, 100.0]),
            ["under-10", "10-to-100", "100-and-up"]
        );
        assert_eq!(
            threshold_bucket_labels(&[0.5, 1e6]),
            ["under-0.5", "0.5-to-1000000", "1000000-and-up"]
        );
        for label in threshold_bucket_labels(&[-90.0, -0.5, 0.0, 90.0]) {
            assert_eq!(printed_word(&label), label, "{label} prints bare");
            assert!(
                label
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.'),
                "{label}"
            );
            assert!(!label.starts_with('-'), "{label}");
        }
    }

    /// Whether the reader itself — the filter reader and the binder — reads `text` as
    /// written, against a text key: the oracle the writer must agree with.
    fn reader_reads_bare(text: &str) -> bool {
        let filter = format!("k={text}");
        let types = HashMap::from([(
            "content.k".to_string(),
            KeyTypes::from_counts([(FactType::Text, 1)]).unwrap(),
        )]);
        let reading = read_filter(&filter, 0, AliasMode::NotLoaded);
        let (true, Some(expr)) = (reading.problems.is_empty(), reading.value) else {
            return false;
        };
        let mut causes = Vec::new();
        matches!(
            bind_expr(&expr, &types, AliasMode::NotLoaded, &mut causes),
            Bound::Compare { test: Test::Text(t), .. } if causes.is_empty() && t == text
        )
    }

    /// The writer asks the reader, on every printable one- and two-character value: bare
    /// exactly where the reader reads the bare text back as itself, quoted everywhere else —
    /// and the quoted form always reads back. A writer keeping its own character list would
    /// disagree with the reader somewhere in this space.
    #[test]
    fn the_printing_rule_agrees_with_the_reader_on_every_short_value() {
        let printable: Vec<char> = (' '..='~').collect();
        let mut values: Vec<String> = printable.iter().map(|c| c.to_string()).collect();
        for a in &printable {
            for b in &printable {
                values.push(format!("{a}{b}"));
            }
        }
        for value in values {
            let printed = printed_value(&value, FactType::Text);
            let expected = if reader_reads_bare(&value) {
                value.clone()
            } else {
                quoted(&value)
            };
            assert_eq!(printed, expected, "{value:?}");
            assert_eq!(
                read_back(&printed, FactType::Text),
                Test::Text(value.clone()),
                "{value:?}"
            );
        }
    }

    /// The statement and the way out, word for word, for one key (named, with its types) and
    /// for several (counted, then each named with its types); a key the language reads only
    /// quoted prints quoted, and its command is quoted for the shell alone.
    #[test]
    fn the_mixed_type_statement_reads_as_decided() {
        use FactType::{Num, Text, Time};
        let create_date = KeyTypes::from_counts([(Text, 1), (Time, 9)]).unwrap();
        let file_size = KeyTypes::from_counts([(Text, 2), (Num, 5)]).unwrap();
        let one = [("content.CreateDate", &create_date)];
        assert_eq!(
            mixed_type_statement(&one),
            ["content.CreateDate is of mixed type (time and text); filters and patterns support \
              one type only."]
        );
        assert_eq!(
            mixed_type_way_out(&one),
            [
                "To see its values and convert it to one type:",
                "  `canon facts convert content.CreateDate`",
            ]
        );

        let two = [
            ("content.CreateDate", &create_date),
            ("content.File Size", &file_size),
        ];
        assert_eq!(
            mixed_type_statement(&two),
            [
                "2 keys are of mixed type; filters and patterns support one type only:",
                "  content.CreateDate (time and text)",
                r#"  "content.File Size" (number and text)"#,
            ]
        );
        assert_eq!(
            mixed_type_way_out(&two),
            [
                "To see their values and convert each to one type:",
                "  `canon facts convert content.CreateDate`",
                "  `canon facts convert 'content.File Size'`",
            ]
        );

        assert_eq!(
            mixed_type_statement(&[("content.File Size", &file_size)])[0],
            r#""content.File Size" is of mixed type (number and text); filters and patterns support one type only."#
        );

        assert!(mixed_type_statement(&[]).is_empty());
        assert!(mixed_type_way_out(&[]).is_empty());
    }

    /// The types are named in the type column's words, the most common first, a tie in the
    /// column's own order; three or more read as a list.
    #[test]
    fn a_mixed_key_names_its_types_most_common_first() {
        use FactType::{Num, Path, Text, Time};
        for (counts, said) in [
            (vec![(Text, 1), (Num, 5)], "number and text"),
            (vec![(Text, 3), (Num, 3)], "text and number"),
            (
                vec![(Time, 1), (Text, 2), (Num, 3)],
                "number, text and time",
            ),
            (
                vec![(Path, 4), (Time, 1), (Text, 2), (Num, 3)],
                "path, number, text and time",
            ),
        ] {
            let types = KeyTypes::from_counts(counts).unwrap();
            assert_eq!(mixed_types(&types), said);
        }
    }

    /// The convert command names the full stored key and quotes it for the shell only: never
    /// in the double quotes a filter reads, which the command would take as part of the name.
    #[test]
    fn the_convert_command_is_quoted_for_the_shell_only() {
        for (key, command) in [
            ("content.Model", "`canon facts convert content.Model`"),
            (
                "content.File Size",
                "`canon facts convert 'content.File Size'`",
            ),
            (
                "content.O'Brien",
                r"`canon facts convert 'content.O'\''Brien'`",
            ),
            ("content.Make#2", "`canon facts convert 'content.Make#2'`"),
        ] {
            assert_eq!(convert_command(key, None), command, "{key}");
            assert!(!convert_command(key, None).contains('"'), "{key}");
        }
        assert_eq!(
            convert_command("content.File Size", Some(FactType::Text)),
            "`canon facts convert 'content.File Size' --to text`"
        );
        assert_eq!(
            convert_command("content.Width", Some(FactType::Time)),
            "`canon facts convert content.Width --to time`"
        );
    }
}
