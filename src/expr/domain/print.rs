//! **The printing rule**: how Canon writes a value or a key so that, typed back into
//! `--where`, it reads as the same value or key. The writer asks the reader: a value prints
//! bare exactly when the reader reads it back after `=` as one bare value with the same
//! text **and** the binder reads that bare text as the same value for its type; a key prints
//! bare exactly when the reader reads it back as one key, in a filter and as a report key, with
//! no accessor or modifier and the same base key. Otherwise it prints in double quotes, an inner `"` doubled — which the
//! grammar reads back as the text inside. No character list lives here, so the writer cannot
//! drift from the reader.

use crate::core::domain::fact::FactType;
use crate::core::domain::format::{format_count, shell_quote};
use crate::core::domain::number::read_number;
use crate::core::domain::time::reading::read_time;

use super::alias::has_alias_references;
use super::bind::{bare_class, BareClass};
use super::read::{read_keys, reads_as_one_bare_key, reads_as_one_bare_value, Grammar};

/// A value as Canon prints it for the reader of `read_as`: bare where the bare text reads
/// back as the same value of that type, otherwise double-quoted.
pub fn printed_value(text: &str, read_as: FactType) -> String {
    let same_value = match read_as {
        FactType::Text | FactType::Path => bare_class(text) == BareClass::Word,
        FactType::Num => read_number(text).is_ok(),
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
    let same_key =
        reads_as_one_bare_key(key) && !has_alias_references(key) && is_its_own_base_key(key);
    if same_key {
        key.to_string()
    } else {
        quoted(key)
    }
}

/// Whether `key`, read as a report reads a key, is one key with no accessor and no modifier
/// whose base key is `key` itself.
fn is_its_own_base_key(key: &str) -> bool {
    let reading = read_keys(key, 0, Grammar::Key);
    reading.problems.is_empty()
        && matches!(reading.value.as_deref(),
            Some([read]) if !read.has_transforms() && read.base == key)
}

/// The command that inspects a key and converts it to one type, as printed: backticked, the
/// key as Canon prints it and quoted again for the shell, so it runs as pasted
/// (`` `canon facts convert '"content.File Size"'` ``).
pub(in crate::expr) fn convert_command(key: &str) -> String {
    format!("`canon facts convert {}`", shell_quote(&printed_key(key)))
}

/// The statement that keys are of mixed type — what is wrong and what it costs. One key is
/// named as Canon prints it; several are counted. Spoken here once, beside the printing rule
/// its keys print through, for the surfaces that list such keys: `facts`' listing and
/// `cluster`'s summary. A refused filter states its own line per clause and reads only the
/// way out. Empty for no keys.
pub fn mixed_type_statement(keys: &[&str]) -> Vec<String> {
    let first = match keys {
        [] => return Vec::new(),
        [key] => format!(
            "{} is of mixed type, so filters and patterns cannot use its values:",
            printed_key(key)
        ),
        keys => format!(
            "{} keys are of mixed type, so filters and patterns cannot use their values:",
            format_count(keys.len())
        ),
    };
    vec![
        first,
        "only values of one type can be compared or formatted correctly.".to_string(),
    ]
}

/// The way out of a mixed type: a header, then one command per key ([`convert_command`]),
/// indented under the header; empty for no keys. A manifest writes these lines into its
/// comments, and the terminal prints them.
pub fn mixed_type_way_out(keys: &[&str]) -> Vec<String> {
    let header = match keys {
        [] => return Vec::new(),
        [_] => "To inspect it and learn how to convert it to one type:",
        _ => "To inspect each and learn how to convert to one type:",
    };
    std::iter::once(header.to_string())
        .chain(keys.iter().map(|key| format!("  {}", convert_command(key))))
        .collect()
}

/// The reader's half: one bare value with the same text, and nothing alias expansion
/// would rewrite.
fn reads_back_bare(text: &str) -> bool {
    !text.is_empty() && !has_alias_references(text) && reads_as_one_bare_value(text)
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
    use crate::expr::domain::bind::{bind_expr, Bound, Test};
    use crate::expr::domain::read::read_filter;

    /// The value the binder reads from `k=<printed>` against a key of type `t`.
    fn read_back(printed: &str, t: FactType) -> Test {
        let types = HashMap::from([(
            "content.k".to_string(),
            KeyTypes::from_counts([(t, 1)]).unwrap(),
        )]);
        let text = format!("k={printed}");
        let reading = read_filter(&text, 0);
        assert!(
            reading.problems.is_empty(),
            "{text}: {:?}",
            reading.problems
        );
        let expr = reading
            .value
            .unwrap_or_else(|| panic!("{text}: nothing read"));
        let mut causes = Vec::new();
        match bind_expr(&expr, &types, &mut causes) {
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
            ("2019:05:04 10:12:00", r#""2019:05:04 10:12:00""#),
        ];
        let bare_text = ["DMC-FZ38", "IMG_0001.jpg", "AND"];
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
            let reading = read_keys(&printed_key(key), 0, Grammar::Key);
            assert!(reading.problems.is_empty(), "{key}");
            let read = &reading.value.unwrap()[0];
            assert_eq!((read.base.as_str(), read.has_transforms()), (key, false));
        }
    }

    /// Whether the reader itself — alias expansion, the filter reader and the binder —
    /// reads `text` as written, against a text key: the oracle the writer must agree with.
    fn reader_reads_bare(text: &str) -> bool {
        let filter = format!("k={text}");
        let no_aliases = std::collections::HashMap::new();
        let expanded = crate::expr::domain::alias::expand_aliases(
            &filter,
            &no_aliases,
            std::path::Path::new("aliases.toml"),
        );
        if !expanded.is_ok_and(|e| e == filter) {
            return false;
        }
        let types = HashMap::from([(
            "content.k".to_string(),
            KeyTypes::from_counts([(FactType::Text, 1)]).unwrap(),
        )]);
        let reading = read_filter(&filter, 0);
        let (true, Some(expr)) = (reading.problems.is_empty(), reading.value) else {
            return false;
        };
        let mut causes = Vec::new();
        matches!(
            bind_expr(&expr, &types, &mut causes),
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

    /// The statement and the way out, word for word, for one key (named) and for several
    /// (counted); a key the language reads only quoted prints quoted, and its command is
    /// quoted again for the shell, so it runs as pasted.
    #[test]
    fn the_two_type_statement_reads_as_decided() {
        let one = ["content.CreateDate"];
        assert_eq!(
            mixed_type_statement(&one),
            [
                "content.CreateDate is of mixed type, so filters and patterns cannot use its values:",
                "only values of one type can be compared or formatted correctly.",
            ]
        );
        assert_eq!(
            mixed_type_way_out(&one),
            [
                "To inspect it and learn how to convert it to one type:",
                "  `canon facts convert content.CreateDate`",
            ]
        );

        let two = ["content.CreateDate", "content.File Size"];
        assert_eq!(
            mixed_type_statement(&two),
            [
                "2 keys are of mixed type, so filters and patterns cannot use their values:",
                "only values of one type can be compared or formatted correctly.",
            ]
        );
        assert_eq!(
            mixed_type_way_out(&two),
            [
                "To inspect each and learn how to convert to one type:",
                "  `canon facts convert content.CreateDate`",
                r#"  `canon facts convert '"content.File Size"'`"#,
            ]
        );

        assert_eq!(
            mixed_type_statement(&["content.File Size"])[0],
            r#""content.File Size" is of mixed type, so filters and patterns cannot use its values:"#
        );

        assert!(mixed_type_statement(&[]).is_empty());
        assert!(mixed_type_way_out(&[]).is_empty());
    }
}
