//! **A problem** in what was typed, and how a refusal draws it: the typed line, a caret under
//! the first character of the problem, one sentence, the line that states the rule where there
//! is one, and a fix only from the closed list. Pure: positions are byte offsets into the text
//! the problem was found in, turned into a terminal column only when the caret is drawn.

use unicode_width::UnicodeWidthStr;

use super::read::is_space;

/// Where a problem sits: which of the refusal's texts, and a byte range in it. A place inside an
/// alias's value sits in that use's own text, which the refusal records where it was typed.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(in crate::expr) struct Place {
    pub text: usize,
    pub start: usize,
    pub end: usize,
}

/// The closed list of fixes a refusal shows. Each is a composition the rules allow, built from
/// what was typed and read back before it is shown — never a claim about what was meant.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::expr) enum Fix {
    None,
    /// The value quoted, after the sentence and a colon.
    Quoted {
        rewrite: String,
    },
    /// A wildcard after `=` or `!=`: the glob and the exact text, after the sentence.
    GlobOrExact {
        glob: String,
        exact: String,
    },
    /// The key's name quoted, on the line that states the key-name rule.
    KeyName {
        rewrite: String,
    },
    /// A value that reads as a number or a time, for a key of type text or path: the clause with
    /// every such value quoted, after the sentence and the key's type.
    ForKeyType {
        key_type: TextOrPath,
        rewrite: String,
    },
}

/// The type a [`Fix::ForKeyType`] names: the key's.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(in crate::expr) enum TextOrPath {
    Text,
    Path,
}

/// The rule a bare name breaks where a quoted name is the fix; the fix follows it after a colon.
pub(in crate::expr) const KEY_NAME_RULE: &str =
    "A key name contains letters, digits, _ . : / and -; quote a name with any other character";

/// One problem: where it sits, its sentence, the line stating the rule it broke, its fix.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::expr) struct Problem {
    pub place: Place,
    pub sentence: String,
    /// The line that states the rule, where there is one.
    pub rule_line: Option<String>,
    pub fix: Fix,
}

impl Problem {
    /// A problem with no fix.
    pub(in crate::expr) fn new(
        place: Place,
        sentence: String,
        rule_line: Option<String>,
    ) -> Problem {
        Problem {
            place,
            sentence,
            rule_line,
            fix: Fix::None,
        }
    }

    /// **A fix shown reads back and binds**: drop the quoted value, the glob and exact pair, or
    /// the value quoted for the key's type, unless every filter it shows `binds` — both of a
    /// pair, or neither is shown. A quoted name is a key, not a filter, and is kept or dropped by
    /// whether it names a key with facts.
    pub(in crate::expr) fn keep_fix_that_binds(&mut self, binds: &dyn Fn(&str) -> bool) {
        let keep = match &self.fix {
            Fix::None | Fix::KeyName { .. } => true,
            Fix::Quoted { rewrite } | Fix::ForKeyType { rewrite, .. } => binds(rewrite),
            Fix::GlobOrExact { glob, exact } => binds(glob) && binds(exact),
        };
        if !keep {
            self.fix = Fix::None;
        }
    }

    /// The problem's block, `text` being the text it was found in: the typed line and the
    /// caret line with the sentence (a quoted value after `: `, the glob and exact text or the
    /// value for the key's type after `. `),
    /// then the rule line and the key-name line, two spaces further in. A text with nothing but spaces in it has no line
    /// to point into, so the sentence stands alone ("the filter is empty").
    pub(in crate::expr) fn render(&self, text: &str, indent: &str) -> Vec<String> {
        let message = match &self.fix {
            Fix::Quoted { rewrite } => format!("{}: {rewrite}", self.sentence),
            Fix::GlobOrExact { glob, exact } => format!(
                "{}. As a glob: {glob}   As exact text: {exact}",
                self.sentence
            ),
            Fix::ForKeyType { key_type, rewrite } => {
                let key_type = match key_type {
                    TextOrPath::Text => "text",
                    TextOrPath::Path => "path",
                };
                format!("{}. For a {key_type} key: {rewrite}", self.sentence)
            }
            Fix::None | Fix::KeyName { .. } => self.sentence.clone(),
        };
        let mut lines = if text.trim_matches(is_space).is_empty() {
            vec![format!("{indent}{message}")]
        } else {
            caret_block(text, self.place.start, &message, indent).to_vec()
        };
        if let Some(rule) = &self.rule_line {
            lines.push(format!("{indent}  {rule}"));
        }
        if let Fix::KeyName { rewrite } = &self.fix {
            lines.push(format!("{indent}  {KEY_NAME_RULE}: {rewrite}"));
        }
        lines
    }
}

/// The text between two byte positions. Every position the reader records sits on a character
/// boundary, since it moves by characters; a range that does not is read as empty.
pub(in crate::expr) fn cut(text: &str, start: usize, end: usize) -> &str {
    text.get(start..end).unwrap_or("")
}

/// The echoed line and the caret line: only the typed line that contains byte `at` (a problem at the
/// end placed after the last character that is not a space), each tab shown as
/// one space and a `\r` at the line's end dropped; the caret's column is the display width of
/// the line before `at` (a wide character two columns, an accent written as its own character
/// none), a tab counted as one.
pub(in crate::expr) fn caret_block(
    text: &str,
    at: usize,
    message: &str,
    indent: &str,
) -> [String; 2] {
    // A problem at the end sits after the last character that is not a space, so a text that
    // ends in a line break shows the line with the mistake.
    let at = at.min(text.trim_end_matches(is_space).len());
    let line_start = cut(text, 0, at).rfind('\n').map_or(0, |i| i + 1);
    let line_end = cut(text, at, text.len())
        .find('\n')
        .map_or(text.len(), |i| at + i);
    let line = cut(text, line_start, line_end);
    let line = line.strip_suffix('\r').unwrap_or(line).replace('\t', " ");
    let before = cut(text, line_start, at).replace('\t', " ");
    let column = before.width();
    [
        format!("{indent}{line}"),
        format!("{indent}{}^ {message}", " ".repeat(column)),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block(text: &str, at: usize) -> [String; 2] {
        caret_block(text, at, "msg", "  ")
    }

    #[test]
    fn the_caret_sits_under_the_first_character_of_the_problem() {
        // `Model=EOS 5D`: the space problem at `5`, byte 10, column 10.
        assert_eq!(
            block("Model=EOS 5D", 10),
            [
                "  Model=EOS 5D".to_string(),
                "            ^ msg".to_string()
            ]
        );
        assert_eq!(
            block("a=1", 0),
            ["  a=1".to_string(), "  ^ msg".to_string()]
        );
    }

    #[test]
    fn a_wide_character_counts_two_columns_and_an_accent_mark_none() {
        // `照片` is two wide characters (four columns), six bytes; the caret under `%` at byte 15.
        let text = "filename=照片%";
        assert_eq!(text.find('%'), Some(15));
        assert_eq!(block(text, 15)[1], format!("  {}^ msg", " ".repeat(13)));
        // `e` + U+0301 is one column, three bytes; the caret under `%` at byte 12.
        let text = "filename=e\u{301}%";
        assert_eq!(text.find('%'), Some(12));
        assert_eq!(block(text, 12)[1], format!("  {}^ msg", " ".repeat(10)));
    }

    #[test]
    fn a_tab_shows_as_one_space_in_the_line_and_in_the_column() {
        assert_eq!(
            block("a=1\tAND\tb%", 9),
            ["  a=1 AND b%".to_string(), "           ^ msg".to_string()]
        );
    }

    #[test]
    fn a_filter_over_several_lines_echoes_only_the_line_with_the_problem() {
        let text = "a=1 AND\r\nb=x%y AND\nc?";
        let at = text.find('%').unwrap();
        assert_eq!(
            block(text, at),
            ["  b=x%y AND".to_string(), "     ^ msg".to_string()]
        );
        let at = text.find('c').unwrap();
        assert_eq!(block(text, at), ["  c?".to_string(), "  ^ msg".to_string()]);
        // On the first line the `\r` before its line break is dropped.
        assert_eq!(block(text, 0)[0], "  a=1 AND");
    }

    #[test]
    fn a_problem_at_the_end_puts_the_caret_one_past_the_last_character() {
        // `source.ext =`: the missing value at byte 12, the text's end.
        assert_eq!(
            block("source.ext =", 12),
            [
                "  source.ext =".to_string(),
                "              ^ msg".to_string()
            ]
        );
        // After a line break at the end, the caret sits after the last character that is not
        // a space, on the line that has it.
        assert_eq!(
            block("a=1 AND\n", 8),
            ["  a=1 AND".to_string(), "         ^ msg".to_string()]
        );
        assert_eq!(
            block("a=1 AND\r\n  ", 11),
            ["  a=1 AND".to_string(), "         ^ msg".to_string()]
        );
    }

    #[test]
    fn a_block_carries_its_fix_its_rule_line_and_its_key_name_line_where_the_layout_puts_them() {
        let problem = |start: usize, rule_line: Option<&str>, fix: Fix| Problem {
            place: Place {
                text: 0,
                start,
                end: start + 1,
            },
            sentence: "`%` needs quotes around the value".to_string(),
            rule_line: rule_line.map(str::to_string),
            fix,
        };
        let f1 = problem(
            10,
            None,
            Fix::Quoted {
                rewrite: "filename=\"a%b\"".to_string(),
            },
        );
        assert_eq!(
            f1.render("filename=a%b", "  "),
            [
                "  filename=a%b",
                "            ^ `%` needs quotes around the value: filename=\"a%b\"",
            ]
        );
        let f3 = Problem {
            sentence: "`*` is a wildcard, but `=` does not take wildcards".to_string(),
            ..problem(
                12,
                None,
                Fix::GlobOrExact {
                    glob: "filename~IMG*".to_string(),
                    exact: "filename=\"IMG*\"".to_string(),
                },
            )
        };
        assert_eq!(
            f3.render("filename=IMG*", "  ")[1],
            "              ^ `*` is a wildcard, but `=` does not take wildcards. As a glob: \
             filename~IMG*   As exact text: filename=\"IMG*\""
        );
        let quoted_name = Problem {
            sentence: "an operator is missing after `content.Foo`".to_string(),
            ..problem(
                11,
                Some("After a key: =, !=, <, >, <=, >=, ~, !~, ?, IN or NOT IN"),
                Fix::KeyName {
                    rewrite: "\"content.Foo%Bar\"".to_string(),
                },
            )
        };
        assert_eq!(
            quoted_name.render("content.Foo%Bar=1", "  "),
            [
                "  content.Foo%Bar=1",
                "             ^ an operator is missing after `content.Foo`",
                "    After a key: =, !=, <, >, <=, >=, ~, !~, ?, IN or NOT IN",
                "    A key name contains letters, digits, _ . : / and -; quote a name with any \
                 other character: \"content.Foo%Bar\"",
            ]
        );
        // A text with nothing in it has no line to point into.
        let empty = Problem {
            sentence: "the filter is empty".to_string(),
            ..problem(0, None, Fix::None)
        };
        assert_eq!(empty.render("  ", "  "), ["  the filter is empty"]);
    }
}
