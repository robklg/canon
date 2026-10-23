//! **Aliases**: the user's own shorthands, the entries of `aliases.toml`, each used as `@name`
//! where a condition starts or a key stands. An alias's value is read where a condition starts,
//! with no alias in it: one key is a key alias, one whole filter a filter alias, anything else a
//! broken entry. The file is checked for what the reader finds and nothing else — no key's type,
//! nothing from the library — and a broken entry refuses every use. Pure: the file arrives as its
//! entries, read by the operations layer.

use std::fmt;
use std::path::{Path, PathBuf};

use unicode_width::UnicodeWidthStr;

use super::filter::Expr;
use super::key::{is_name_char, InputKey};
use super::problem::{cut, Fix, Place, Problem};
use super::read::{is_space, read_alias_value, AliasUse};

/// **An alias name**: a letter, then the key name's characters without `.`, `:` and `/` —
/// letters and digits of any language, `_` and `-`.
fn is_alias_name_char(c: char) -> bool {
    is_name_char(c) && !matches!(c, '.' | ':' | '/')
}

/// The alias name `text` starts with, just after an `@`; `None` where no name starts there. The
/// one spelling of an alias name: the reader reads a use through it, the file check a name.
pub(in crate::expr) fn alias_name_at(text: &str) -> Option<&str> {
    if !text.chars().next().is_some_and(char::is_alphabetic) {
        return None;
    }
    let end = text.find(|c| !is_alias_name_char(c)).unwrap_or(text.len());
    text.get(..end)
}

/// Where in `@name` the name rule refuses a name: the `@` where it is empty, otherwise the first
/// character that cannot stand where it stands; `None` for a name the rule takes.
fn refused_at(name: &str) -> Option<usize> {
    match alias_name_at(name) {
        Some(read) if read.len() == name.len() => None,
        Some(read) => Some(1 + read.len()),
        None if name.is_empty() => Some(0),
        None => Some(1),
    }
}

/// One entry of the aliases file, as TOML gave it: its name, the line it starts on, its value
/// with TOML's escapes read.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::expr) struct RawEntry {
    pub name: String,
    pub line: usize,
    pub value: String,
}

/// What an alias stands for, by how its value reads where a condition starts.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::expr) enum AliasValue {
    Key(InputKey),
    Filter(Expr),
}

/// One entry that reads: its name, its value as written, what the value reads as.
#[derive(Debug, Clone)]
struct CheckedEntry {
    name: String,
    value: String,
    reading: AliasValue,
}

/// **The aliases file, checked**: where it is, whether it exists, and every entry, each one that
/// reads. Built only by [`check_aliases`], so an entry that does not read cannot reach a use.
#[derive(Debug, Clone)]
pub(in crate::expr) struct CheckedAliases {
    path: PathBuf,
    exists: bool,
    entries: Vec<CheckedEntry>,
}

impl CheckedAliases {
    pub(in crate::expr) fn path(&self) -> &Path {
        &self.path
    }

    pub(in crate::expr) fn exists(&self) -> bool {
        self.exists
    }

    /// The alias named `name`, exactly as typed: its value as written, and what it stands for.
    pub(in crate::expr) fn get(&self, name: &str) -> Option<(&str, &AliasValue)> {
        self.entry(name)
            .map(|entry| (entry.value.as_str(), &entry.reading))
    }

    fn entry(&self, name: &str) -> Option<&CheckedEntry> {
        self.entries.iter().find(|entry| entry.name == name)
    }

    /// Aliases that must check, for a test: `(name, value)` each, one per line.
    #[cfg(test)]
    pub(in crate::expr) fn of(entries: &[(&str, &str)]) -> CheckedAliases {
        let raw = entries
            .iter()
            .enumerate()
            .map(|(i, (name, value))| RawEntry {
                name: name.to_string(),
                line: i + 1,
                value: value.to_string(),
            })
            .collect();
        check_aliases(Path::new("/home/.canon/aliases.toml"), true, raw)
            .unwrap_or_else(|refused| panic!("{refused}"))
    }
}

/// **The file check**: each entry, in the order of the lines it starts on — its name against the
/// name rule, its value read at the alias-value position (one key or one filter, no alias in it).
/// Nothing is read from the library, so whether the file loads never follows what is imported.
/// Every entry that does not read is in the refusal.
pub(in crate::expr) fn check_aliases(
    path: &Path,
    exists: bool,
    mut raw: Vec<RawEntry>,
) -> Result<CheckedAliases, AliasesFileRefused> {
    raw.sort_by_key(|entry| entry.line);
    let mut entries = Vec::new();
    let mut blocks = Vec::new();
    for entry in raw {
        let drawn = format!("@{}: {}", entry.name, entry.value);
        let value_at = drawn.len() - entry.value.len();
        let mut problems = Vec::new();
        if let Some(at) = refused_at(&entry.name) {
            let sentence = format!(
                "`@{}` is not an alias name: alias names must start with a letter and contain \
                 only letters, digits, _ and -",
                entry.name
            );
            problems.push(drawn_problem(at, sentence));
        }
        let reading = read_alias_value(&entry.value);
        problems.extend(reading.problems.into_iter().map(|problem| Problem {
            place: Place {
                start: value_at + problem.place.start,
                end: value_at + problem.place.end,
                ..problem.place
            },
            ..problem
        }));
        match reading.value {
            Some(value) if problems.is_empty() => entries.push(CheckedEntry {
                name: entry.name,
                value: entry.value,
                reading: value,
            }),
            _ => blocks.extend(
                problems
                    .iter()
                    .map(|problem| block(entry.line, &drawn, problem)),
            ),
        }
    }
    if blocks.is_empty() {
        Ok(CheckedAliases {
            path: path.to_path_buf(),
            exists,
            entries,
        })
    } else {
        Err(AliasesFileRefused {
            path: path.to_path_buf(),
            blocks,
        })
    }
}

/// A problem in an entry's name, at byte `at` of `@name: value`.
fn drawn_problem(at: usize, sentence: String) -> Problem {
    Problem {
        place: Place {
            text: 0,
            start: at,
            end: at + 1,
        },
        sentence,
        rule_line: None,
        fix: Fix::None,
        also: None,
    }
}

/// One problem's block in the file refusal: the caret block on `@name: value`, its first line
/// labelled `line N: ` and every line under it indented as far. A fix is for what was typed,
/// never for the file, so none is drawn.
fn block(line: usize, drawn: &str, problem: &Problem) -> Vec<String> {
    let problem = Problem {
        fix: Fix::None,
        ..problem.clone()
    };
    let label = format!("line {line}: ");
    let pad = " ".repeat(label.width());
    problem
        .render(drawn, "")
        .into_iter()
        .enumerate()
        .map(|(i, l)| {
            if i == 0 {
                format!("  {label}{l}")
            } else {
                format!("  {pad}{l}")
            }
        })
        .collect()
}

/// **An aliases file with entries that are not valid**, and each problem in them, in file order.
/// Every use of an alias is refused while one stands.
#[derive(Debug)]
pub(in crate::expr) struct AliasesFileRefused {
    path: PathBuf,
    blocks: Vec<Vec<String>>,
}

impl fmt::Display for AliasesFileRefused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "the aliases file {} has entries that are not valid",
            self.path.display()
        )?;
        for line in self.blocks.iter().flatten() {
            write!(f, "\n{line}")?;
        }
        Ok(())
    }
}

impl std::error::Error for AliasesFileRefused {}

/// **The recorded text**: a filter as typed, each alias use replaced by what it stands for — a
/// filter alias's value in parentheses, a key alias's key as written — without the spaces around
/// it. What a manifest records: it selects what the typed text selected, and holds no alias.
/// `uses` are the text's own, in the order they were met, each carrying its alias.
pub(in crate::expr) fn recorded_text(typed: &str, uses: &[AliasUse]) -> String {
    let mut recorded = String::new();
    let mut at = 0;
    for used in uses {
        recorded.push_str(cut(typed, at, used.place.start));
        let value = used.value.trim_matches(is_space);
        if used.is_filter {
            recorded.push_str(&format!("({value})"));
        } else {
            recorded.push_str(value);
        }
        at = used.place.end;
    }
    recorded.push_str(cut(typed, at, typed.len()));
    recorded
}

#[cfg(test)]
mod file_tests {
    use super::*;

    fn raw(name: &str, line: usize, value: &str) -> RawEntry {
        RawEntry {
            name: name.to_string(),
            line,
            value: value.to_string(),
        }
    }

    /// The file's refusal for `entries`, drawn.
    fn refusal(entries: Vec<RawEntry>) -> String {
        check_aliases(Path::new("/Users/me/.canon/aliases.toml"), true, entries)
            .expect_err("refused")
            .to_string()
    }

    /// What a value reads as: a key alias's key as written, a filter alias, or the first
    /// sentence of a broken entry.
    #[derive(Debug, PartialEq)]
    enum Read {
        Key(&'static str),
        Filter,
        Broken(&'static str),
    }

    /// Rule 2: one key followed by the end is a key alias, one whole filter a filter alias, any
    /// other value broken with the problems the reading meets where a condition starts.
    #[test]
    fn an_alias_value_is_one_key_or_one_filter() {
        use Read::{Broken, Filter, Key};
        for (value, want) in [
            ("Taken", Key("Taken")),
            ("  source.rel_path[0]  ", Key("source.rel_path[0]")),
            ("source.mtime|year", Key("source.mtime|year")),
            (r#""NOT""#, Key(r#""NOT""#)),
            ("size", Key("size")),
            ("content.mime IN (image/jpeg, image/png)", Filter),
            ("hashed?", Filter),
            ("Model=EOS", Filter),
            ("NOT Taken?", Filter),
            // A problem binding finds is met where the alias is used, never here.
            ("filename=a%b", Filter),
            ("NOT", Broken("a condition is missing after `NOT`")),
            ("hashed", Broken("`hashed` can only be used with `?`")),
            ("jpg, png", Broken("an operator is missing after `jpg`")),
            ("source.ext IN jpg", Broken("`(` is missing after IN")),
            ("(Taken)", Broken("an operator is missing after `Taken`")),
            ("NOT Taken", Broken("an operator is missing after `Taken`")),
            ("@image", Broken("an alias cannot contain another alias")),
            ("Taken|nosuch", Broken("`|nosuch` is not a modifier")),
            ("", Broken("the filter is empty")),
            ("   ", Broken("the filter is empty")),
        ] {
            let reading = read_alias_value(value);
            let got = match (&reading.value, reading.problems.first()) {
                (Some(AliasValue::Key(key)), None) => Key(key.input.clone().leak()),
                (Some(AliasValue::Filter(_)), None) => Filter,
                (None, Some(problem)) => {
                    let first = problem.sentence.split(" — ").next().unwrap_or_default();
                    Broken(first.to_string().leak())
                }
                other => panic!("{value:?}: {other:?}"),
            };
            assert_eq!(got, want, "{value:?}");
        }
    }

    /// An alias name is a letter, then the key name's characters without `.`, `:` and `/`.
    #[test]
    fn alias_names_take_the_key_names_characters_without_dot_colon_slash() {
        for (text, name) in [
            ("image AND x", Some("image")),
            ("my-alias", Some("my-alias")),
            ("my_alias", Some("my_alias")),
            ("a1=2", Some("a1")),
            ("foto_één", Some("foto_één")),
            ("照片", Some("照片")),
            ("geo.lat", Some("geo")),
            ("xmp:Subject", Some("xmp")),
            ("a/b", Some("a")),
            ("1abc", None),
            ("_x", None),
            ("-x", None),
            (".x", None),
            ("", None),
        ] {
            assert_eq!(alias_name_at(text), name, "{text:?}");
        }
    }

    /// A broken name: the caret under the first character the rule refuses, or under the `@`
    /// where the name is empty, beside the line the entry starts on.
    #[test]
    fn a_broken_name_is_refused_at_the_first_character_the_rule_refuses() {
        let rule = "is not an alias name: alias names must start with a letter and contain \
                    only letters, digits, _ and -";
        for (name, column) in [("my.alias", 3), ("1x", 1), ("a b", 2), ("", 0)] {
            let drawn = refusal(vec![raw(name, 3, "x?")]);
            let lines: Vec<&str> = drawn.lines().collect();
            assert_eq!(
                lines[1..],
                [
                    format!("  line 3: @{name}: x?"),
                    format!("{}^ `@{name}` {rule}", " ".repeat(10 + column)),
                ],
                "{name:?}"
            );
        }
    }

    /// A fix is for what was typed, never for the file: a reader's quoted name is not drawn,
    /// and the rule line stays.
    #[test]
    fn the_file_refusal_shows_no_fix() {
        let drawn = refusal(vec![raw("bad", 1, "Lens#2=35mm")]);
        assert_eq!(
            drawn.lines().skip(1).collect::<Vec<_>>(),
            [
                "  line 1: @bad: Lens#2=35mm",
                "                    ^ an operator is missing after `Lens`",
                "            After a key: =, !=, <, >, <=, >=, ~, !~, ?, IN or NOT IN",
            ]
        );
    }
}
