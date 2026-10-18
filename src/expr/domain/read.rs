//! **The reader**: one text read once, front to back, by one rule per position. What may stand
//! at a position decides what is read there, and where it cannot be read the position's own
//! sentence follows. A problem is recorded where it is met and reading goes on, so every
//! problem in a text is found in one run; reading stops only where nothing after a point can be
//! read for what it is — an unclosed quote, `[` or `|bucket(`.

use std::collections::HashMap;

use unicode_normalization::char::is_combining_mark;

use super::alias::alias_name_at;
use super::filter::{CompareOp, Expr, InputValue, NeedsQuotes, STATUS_KEYWORDS};
use super::glob::read_glob;
use super::key::{follows_a_key_in_a_filter, is_name_char, read_key, InputKey, SCOPE_REL_PATH};
use super::problem::{cut, Fix, Place, Problem};

/// **A space**, as the language reads one: a space, a tab or a line break.
pub(in crate::expr) fn is_space(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\r')
}

/// What a text is read as: a filter (`--where`, a recorded query), one key (`facts --key`),
/// or keys separated by commas (`--group-by`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub(in crate::expr) enum Grammar {
    Filter,
    Key,
    KeyList,
}

/// One text read once, front to back, by characters, so a position never falls inside one.
pub(in crate::expr) struct Cursor<'t> {
    text: &'t str,
    index: usize,
    at: usize,
    problems: Vec<Problem>,
    stopped: bool,
    /// Each `(` met so far, and the byte of the `)` that closes it, or `None` where none does.
    parens: HashMap<usize, Option<usize>>,
}

/// What a reading yields: what could be read, and every problem met on the way.
#[derive(Debug)]
pub(in crate::expr) struct Reading<T> {
    pub value: Option<T>,
    pub problems: Vec<Problem>,
}

impl<'t> Cursor<'t> {
    /// A cursor at the start of `text`, the refusal's text number `index`.
    pub(in crate::expr) fn new(text: &'t str, index: usize) -> Cursor<'t> {
        Cursor {
            text,
            index,
            at: 0,
            problems: Vec::new(),
            stopped: false,
            parens: HashMap::new(),
        }
    }

    pub(in crate::expr) fn at(&self) -> usize {
        self.at
    }

    /// The whole text being read.
    pub(in crate::expr) fn text(&self) -> &'t str {
        self.text
    }

    /// Move to a position found by a forward scan over the same text.
    pub(in crate::expr) fn set_at(&mut self, at: usize) {
        self.at = at.min(self.text.len());
    }

    pub(in crate::expr) fn peek(&self) -> Option<char> {
        self.rest().chars().next()
    }

    /// The text not yet read.
    pub(in crate::expr) fn rest(&self) -> &'t str {
        cut(self.text, self.at, self.text.len())
    }

    pub(in crate::expr) fn at_end(&self) -> bool {
        self.at >= self.text.len()
    }

    pub(in crate::expr) fn bump(&mut self) -> Option<char> {
        let c = self.peek()?;
        self.at += c.len_utf8();
        Some(c)
    }

    /// Read the next character if it is `c`.
    pub(in crate::expr) fn eat(&mut self, c: char) -> bool {
        if self.peek() == Some(c) {
            self.bump();
            true
        } else {
            false
        }
    }

    /// Read characters while `keep` holds.
    pub(in crate::expr) fn bump_while(&mut self, keep: impl Fn(char) -> bool) {
        while self.peek().is_some_and(&keep) {
            self.bump();
        }
    }

    pub(in crate::expr) fn skip_spaces(&mut self) {
        self.bump_while(is_space);
    }

    pub(in crate::expr) fn slice(&self, start: usize, end: usize) -> &'t str {
        cut(self.text, start, end)
    }

    pub(in crate::expr) fn place(&self, start: usize, end: usize) -> Place {
        Place {
            text: self.index,
            start,
            end,
        }
    }

    /// Record a problem between two positions of this text. Once reading has stopped nothing more
    /// is recorded: what follows the stop cannot be read for what it is.
    pub(in crate::expr) fn problem(
        &mut self,
        start: usize,
        end: usize,
        sentence: impl Into<String>,
        rule_line: Option<&str>,
        fix: Fix,
    ) {
        if self.stopped {
            return;
        }
        let place = self.place(start, end);
        self.problems.push(Problem {
            place,
            sentence: sentence.into(),
            rule_line: rule_line.map(str::to_string),
            fix,
        });
    }

    pub(in crate::expr) fn problem_count(&self) -> usize {
        self.problems.len()
    }

    /// Stop reading: nothing after this point can be read for what it is.
    pub(in crate::expr) fn stop(&mut self) {
        self.at = self.text.len();
        self.stopped = true;
    }

    pub(in crate::expr) fn stopped(&self) -> bool {
        self.stopped
    }

    pub(in crate::expr) fn into_problems(self) -> Vec<Problem> {
        self.problems
    }
}

/// **Quoted text**: in `"…"` or `'…'`, the closing mark doubled to stand for itself, every
/// other character itself — a backslash included. Read from the opening mark; `None`, and
/// reading stopped, where the quote never closes.
pub(in crate::expr) fn read_quoted(cursor: &mut Cursor) -> Option<String> {
    let open = cursor.at();
    let quote = cursor.bump()?;
    let mut text = String::new();
    loop {
        match cursor.bump() {
            None => {
                cursor.problem(open, open + 1, "this quote is unclosed", None, Fix::None);
                cursor.stop();
                return None;
            }
            Some(c) if c == quote => {
                if !cursor.eat(quote) {
                    return Some(text);
                }
                text.push(quote);
            }
            Some(c) => text.push(c),
        }
    }
}

/// The rule line under a status word where only a key may stand, naming the stored fact a
/// quoted name reads.
pub(in crate::expr) fn stored_fact_line(word: &str) -> String {
    format!("a stored fact of that name is `content.{word}`")
}

/// **Report keys**: `--key` (`Grammar::Key`: one key, then the end) and `--group-by`
/// (`Grammar::KeyList`: keys separated by `,`, spaces allowed around it, a comma inside
/// `|bucket( … )` or a quoted name belonging to the key). A space at the start or end of the
/// text or of an item is allowed, as in a filter. The keys read without a problem are the
/// value.
pub(in crate::expr) fn read_keys(
    text: &str,
    index: usize,
    grammar: Grammar,
) -> Reading<Vec<InputKey>> {
    let mut cursor = Cursor::new(text, index);
    let mut keys = Vec::new();
    cursor.skip_spaces();
    if cursor.at_end() {
        cursor.problem(0, 0, "the key is empty", None, Fix::None);
        return Reading {
            value: Some(keys),
            problems: cursor.into_problems(),
        };
    }
    let list = grammar == Grammar::KeyList;
    let mut after_comma = false;
    loop {
        cursor.skip_spaces();
        let at = cursor.at();
        if list && cursor.peek() == Some(',') {
            let sentence = if after_comma {
                "a key is missing after `,`"
            } else {
                "a key is missing before `,`"
            };
            cursor.problem(at, at + 1, sentence, None, Fix::None);
            cursor.bump();
            after_comma = true;
            continue;
        }
        if cursor.at_end() {
            if after_comma {
                cursor.problem(at, at, "a key is missing after `,`", None, Fix::None);
            }
            break;
        }
        let before = cursor.problem_count();
        let key = read_key(&mut cursor, grammar);
        if cursor.stopped() {
            break;
        }
        if let Some(key) = &key {
            report_key_position(&mut cursor, key);
        }
        let key_end = cursor.at();
        cursor.skip_spaces();
        match cursor.peek() {
            None => {}
            Some(',') if list => {}
            Some(_) => {
                // What cannot follow runs to the end, or in a list to the next `,`. It is named
                // before it is read through, so a quote in it that never closes is named after it.
                let rest_start = cursor.at();
                let item_end = skip_end(text, rest_start, |_, c| list && c == ',');
                let rest = cursor.slice(rest_start, item_end);
                let rest_end = rest_start + rest.trim_end_matches(is_space).len();
                if let Some(key) = &key {
                    let fix = key
                        .quoted_name_fix(text, key_end, grammar)
                        .map_or(Fix::None, |rewrite| Fix::KeyName { rewrite });
                    cursor.problem(
                        rest_start,
                        rest_end,
                        format!(
                            "`{}` cannot follow `{}`",
                            cursor.slice(rest_start, rest_end),
                            key.input
                        ),
                        None,
                        fix,
                    );
                }
                skip_to(&mut cursor, |_, c| list && c == ',');
                cursor.skip_spaces();
            }
        }
        if let Some(key) = key {
            if cursor.problem_count() == before {
                keys.push(key);
            }
        }
        after_comma = cursor.eat(',');
        if !after_comma && cursor.at_end() {
            break;
        }
    }
    Reading {
        value: Some(keys),
        problems: cursor.into_problems(),
    }
}

/// What a report key may not be, though the key grammar reads it: a status word, bare (a status
/// word is not a key), and a key only patterns have.
fn report_key_position(cursor: &mut Cursor, key: &InputKey) {
    let (start, end) = (key.name_place.start, key.name_place.end);
    if let Some((word, _)) = STATUS_KEYWORDS
        .iter()
        .find(|(word, _)| key.is_bare_name_spelled(word))
    {
        cursor.problem(
            start,
            end,
            format!("`{word}` is not a key"),
            Some(&stored_fact_line(word)),
            Fix::None,
        );
    }
    if key.names(SCOPE_REL_PATH) {
        cursor.problem(
            start,
            end,
            format!("`{SCOPE_REL_PATH}` can only be used in a pattern"),
            None,
            Fix::None,
        );
    }
}

/// Whether the cursor stands where a part may start: the text's start, or after a space, a
/// parenthesis, a comma or an operator (`!` alone is none). Only there does a quote open, so an
/// apostrophe inside a word is text.
fn at_part_start(cursor: &Cursor) -> bool {
    starts_a_part(cursor.text(), cursor.at())
}

fn starts_a_part(text: &str, at: usize) -> bool {
    cut(text, 0, at)
        .chars()
        .next_back()
        .is_none_or(|c| is_space(c) || matches!(c, '(' | ')' | ',' | '=' | '<' | '>' | '~'))
}

/// Step forward to where `stop` holds, or to the end. A `(` is stepped over whole, to the `)`
/// that closes it, where one does; one that never closes is a character like any other. Quoted
/// text is read by the quoting rule where a part starts, so a quote that never closes is named
/// and stops reading here as anywhere else.
pub(in crate::expr) fn skip_to(cursor: &mut Cursor, stop: impl Fn(&Cursor, char) -> bool) {
    while let Some(c) = cursor.peek() {
        if stop(cursor, c) {
            return;
        }
        match c {
            '"' | '\'' if at_part_start(cursor) => {
                if read_quoted(cursor).is_none() {
                    return;
                }
                continue;
            }
            '(' => {
                if let Some(close) = closing_paren(cursor) {
                    cursor.set_at(close + 1);
                    continue;
                }
            }
            _ => {}
        }
        cursor.bump();
    }
}

/// The byte of the `)` that closes the `(` at the cursor, or `None` where none does: the first
/// `)` after it that no `(` between them closes, quoted text where a part starts stepped over.
/// One pass, remembered for every `(` it meets. A `(` still open when the text ends closes
/// nothing, and neither does every `(` it was inside, since each `)` after them closed a later
/// `(`. A quote that never closes holds the rest of the text, so no `)` follows any `(` still
/// open there.
fn closing_paren(cursor: &mut Cursor) -> Option<usize> {
    let text = cursor.text;
    let open = cursor.at;
    if let Some(&known) = cursor.parens.get(&open) {
        return known;
    }
    let mut stack = vec![open];
    let mut at = open + 1;
    while let Some(c) = cut(text, at, text.len()).chars().next() {
        match c {
            '"' | '\'' if starts_a_part(text, at) => {
                let mut quoted = Cursor::new(text, cursor.index);
                quoted.set_at(at);
                if read_quoted(&mut quoted).is_none() {
                    break;
                }
                at = quoted.at();
                continue;
            }
            '(' => match cursor.parens.get(&at) {
                Some(&Some(close)) => {
                    at = close + 1;
                    continue;
                }
                Some(None) => {}
                None => stack.push(at),
            },
            ')' => {
                let inner = stack.pop().expect("the opening `(` is on the stack");
                cursor.parens.insert(inner, Some(at));
                if stack.is_empty() {
                    return Some(at);
                }
            }
            _ => {}
        }
        at += c.len_utf8();
    }
    for unclosed in stack {
        cursor.parens.insert(unclosed, None);
    }
    None
}

/// Where `skip_to` from byte `at` would stop, found on a scratch cursor over the same text, so
/// nothing is recorded: for naming a text before it is read through.
pub(in crate::expr) fn skip_end(
    text: &str,
    at: usize,
    stop: impl Fn(&Cursor, char) -> bool,
) -> usize {
    let mut scan = Cursor::new(text, 0);
    scan.set_at(at);
    skip_to(&mut scan, stop);
    scan.at()
}

/// Whether `word` stands at the cursor as a word of its own: the run of name characters there
/// is exactly `word`.
fn at_keyword(cursor: &Cursor, word: &str) -> bool {
    let rest = cursor.rest();
    let run = rest.find(|c| !is_name_char(c)).unwrap_or(rest.len());
    cut(rest, 0, run) == word
}

/// The rule line under "an operator is missing after `x`".
const AFTER_KEY: &str = "After a key: =, !=, <, >, <=, >=, ~, !~, ?, IN or NOT IN";
/// The rule line under "a space separates values".
const AFTER_VALUE: &str = "After a value: AND, OR, ) or the end";
/// The rule line under "AND or OR is missing before `x`".
const AFTER_CONDITION: &str = "After a condition: AND, OR, ) or the end";

/// **A filter**: conditions joined by `AND` and `OR`, `NOT` binding tightest, then `AND`, then
/// `OR`, grouped by `( … )`. A condition is `key?`, `key op value`, `key IN (…)` or
/// `key NOT IN (…)`, or a status word with `?`. A keyword is one only in exact capitals and only
/// where one may stand: `AND` and `OR` after a complete condition, `NOT` where a condition may
/// start, `IN` and `NOT IN` after a key; anywhere else the word is what that position reads, a
/// value or a key's name. After a problem in a value, reading resumes where the value ends;
/// after any other problem, at the next `AND` or `OR` of the same group, or the `)` closing it.
/// The tree contains every condition whose key reads without a problem.
pub(in crate::expr) fn read_filter(text: &str, index: usize) -> Reading<Expr> {
    let mut cursor = Cursor::new(text, index);
    cursor.skip_spaces();
    if cursor.at_end() {
        cursor.problem(0, 0, "the filter is empty", None, Fix::None);
        return Reading {
            value: None,
            problems: cursor.into_problems(),
        };
    }
    let mut reader = FilterReader { cursor };
    let value = reader.or(0, None);
    Reading {
        value,
        problems: reader.cursor.into_problems(),
    }
}

/// Whether `text`, typed bare after `=`, is read back as one unquoted value with that same text
/// and nothing about it to quote — the reader's half of the printing rule, asked of the reader.
pub(in crate::expr) fn reads_as_one_bare_value(text: &str) -> bool {
    let reading = read_filter(&format!("k={text}"), 0);
    reading.problems.is_empty()
        && matches!(
            reading.value,
            Some(Expr::Compare { value, .. })
                if !value.quoted && value.text == text && value.needs_quotes.is_none()
        )
}

/// Whether `key`, typed bare before `?`, is read back as one key with that same text.
pub(in crate::expr) fn reads_as_one_bare_key(key: &str) -> bool {
    let reading = read_filter(&format!("{key}?"), 0);
    reading.problems.is_empty()
        && matches!(
            reading.value,
            Some(Expr::Exists { key: read, .. })
                if read.input == key && !read.input_name.starts_with(['"', '\''])
        )
}

/// Whether `c` may stand in an unquoted value: letters and digits of any language, accents and
/// vowel signs written as their own character, and `_ . - / : +`.
pub(in crate::expr) fn is_bare_value_char(c: char) -> bool {
    c.is_alphanumeric() || is_combining_mark(c) || matches!(c, '_' | '.' | '-' | '/' | ':' | '+')
}

/// What a value is read against, for the characters that may stand in it unquoted: after `=`,
/// `!=` and in an `IN` list, `*`, `?` and `[` are wildcards. A glob after `~` and `!~` is read
/// by its own grammar (`domain/glob.rs`).
#[derive(Clone, Copy)]
struct ValueRead {
    wildcards: bool,
    in_list: bool,
}

/// One filter text read once, front to back.
struct FilterReader<'t> {
    cursor: Cursor<'t>,
}

impl<'t> FilterReader<'t> {
    /// `and (OR and)*`. `after` is what was typed before a condition belongs, for "a condition is
    /// missing after `x`"; `None` at the start of the text.
    fn or(&mut self, depth: usize, after: Option<&'static str>) -> Option<Expr> {
        let mut parts: Vec<Expr> = self.and(depth, after).into_iter().collect();
        loop {
            self.cursor.skip_spaces();
            if !self.eat_keyword("OR") {
                break;
            }
            parts.extend(self.and(depth, Some("OR")));
        }
        join(parts, Expr::Or)
    }

    /// `unary (AND unary)*`, and what may follow a complete condition: `AND`, `OR`, a `)` closing
    /// this group, or the end. A `)` closing nothing is a problem read past.
    fn and(&mut self, depth: usize, after: Option<&'static str>) -> Option<Expr> {
        let mut parts: Vec<Expr> = self.unary(after).into_iter().collect();
        loop {
            self.cursor.skip_spaces();
            if self.cursor.at_end() || self.at_keyword("OR") {
                break;
            }
            if self.eat_keyword("AND") {
                parts.extend(self.unary(Some("AND")));
                continue;
            }
            let at = self.cursor.at();
            if self.cursor.peek() == Some(')') {
                if depth > 0 {
                    break;
                }
                let sentence = "`)` has no matching `(`";
                self.cursor.problem(at, at + 1, sentence, None, Fix::None);
                self.cursor.bump();
                continue;
            }
            let word = self.next_word();
            self.cursor.problem(
                at,
                at + word.len(),
                format!("AND or OR is missing before `{word}`"),
                Some(AFTER_CONDITION),
                Fix::None,
            );
            self.resume();
        }
        join(parts, Expr::And)
    }

    /// `NOT unary | primary`.
    fn unary(&mut self, after: Option<&'static str>) -> Option<Expr> {
        self.cursor.skip_spaces();
        if self.eat_keyword("NOT") {
            return self
                .unary(Some("NOT"))
                .map(|inner| Expr::Not(Box::new(inner)));
        }
        self.primary(after)
    }

    /// Where a condition belongs: `( … )`, or a condition.
    fn primary(&mut self, after: Option<&'static str>) -> Option<Expr> {
        self.cursor.skip_spaces();
        let at = self.cursor.at();
        match self.cursor.peek() {
            // At the start of the text nothing is open: a `)` there closes nothing, and the
            // condition is read after it.
            Some(')') if after.is_none() => {
                let sentence = "`)` has no matching `(`";
                self.cursor.problem(at, at + 1, sentence, None, Fix::None);
                self.cursor.bump();
                self.primary(None)
            }
            None | Some(')') => {
                if let Some(after) = after {
                    let end = if self.cursor.at_end() { at } else { at + 1 };
                    let sentence = format!("a condition is missing after `{after}`");
                    self.cursor.problem(at, end, sentence, None, Fix::None);
                }
                None
            }
            Some('(') => self.group(),
            Some('@') => self.alias(),
            Some(c) => {
                let typed = match operator_at(self.cursor.rest()) {
                    Some(op) => op.typed,
                    None if c == '?' => "?",
                    None => return self.condition(),
                };
                let sentence = format!("a key is missing before `{typed}`");
                self.cursor
                    .problem(at, at + typed.len(), sentence, None, Fix::None);
                self.resume();
                None
            }
        }
    }

    /// `( or )`, from the `(`.
    fn group(&mut self) -> Option<Expr> {
        let open = self.cursor.at();
        self.cursor.bump();
        let inner = self.or(1, Some("("));
        self.cursor.skip_spaces();
        if !self.cursor.eat(')') {
            let sentence = "this `(` is unclosed";
            self.cursor
                .problem(open, open + 1, sentence, None, Fix::None);
        }
        inner
    }

    /// An `@name` where a condition belongs. The filter reaches the reader with its aliases
    /// expanded, so one left here was never expanded: it comes from a manifest's recorded query,
    /// or from inside an alias's own value. An `@` not followed by an alias name is read as a key
    /// would be.
    fn alias(&mut self) -> Option<Expr> {
        let at = self.cursor.at();
        let after_at: Vec<char> = cut(self.cursor.rest(), 1, self.cursor.rest().len())
            .chars()
            .collect();
        let Some(name) = alias_name_at(&after_at, 0) else {
            return self.condition();
        };
        let end = at + 1 + name.len();
        self.cursor.problem(
            at,
            end,
            format!(
                "'@{name}' is an alias, and aliases are not expanded here; write the filter it \
                 stands for"
            ),
            None,
            Fix::None,
        );
        self.cursor.set_at(end);
        self.resume();
        None
    }

    /// A condition, from its key. A status word, bare, is one only with `?` and nothing else.
    fn condition(&mut self) -> Option<Expr> {
        let start = self.cursor.at();
        let before = self.cursor.problem_count();
        let Some(key) = read_key(&mut self.cursor, Grammar::Filter) else {
            self.resume();
            return None;
        };
        let key_end = self.cursor.at();
        let name = (key.name_place.start, key.name_place.end);
        if key.names(SCOPE_REL_PATH) {
            let sentence = format!("`{SCOPE_REL_PATH}` can only be used in a pattern");
            self.cursor
                .problem(name.0, name.1, sentence, None, Fix::None);
        }
        let status = STATUS_KEYWORDS
            .iter()
            .find(|(word, _)| key.is_bare_name_spelled(word))
            .copied();
        self.cursor.skip_spaces();
        let asks_exists = self.cursor.peek() == Some('?');
        if let Some((word, _)) = status {
            if key.has_transforms() || !asks_exists {
                self.cursor.problem(
                    name.0,
                    name.1,
                    format!("`{word}` can only be used with `?`"),
                    Some(&stored_fact_line(word)),
                    Fix::None,
                );
            }
        }
        let key_read = self.cursor.problem_count() == before;

        if asks_exists {
            self.cursor.bump();
            let place = self.cursor.place(start, self.cursor.at());
            return match status {
                _ if !key_read => None,
                Some((_, predicate)) => Some(Expr::Status { predicate, place }),
                None => Some(Expr::Exists {
                    clause: self.cursor.slice(start, place.end).to_string(),
                    key,
                    place,
                }),
            };
        }
        if self.eat_keyword("IN") {
            return self.list(key, start, key_read, false);
        }
        if self.at_keyword("NOT") {
            let not = self.cursor.at();
            self.eat_keyword("NOT");
            self.cursor.skip_spaces();
            if self.eat_keyword("IN") {
                return self.list(key, start, key_read, true);
            }
            self.cursor.set_at(not);
        }
        let Some(op) = operator_at(self.cursor.rest()) else {
            if status.is_none() {
                let at = self.cursor.at();
                let fix = key
                    .quoted_name_fix(self.cursor.text(), key_end, Grammar::Filter)
                    .map_or(Fix::None, |rewrite| Fix::KeyName { rewrite });
                self.cursor.problem(
                    at,
                    at + self.next_word().len(),
                    format!("an operator is missing after `{}`", key.input),
                    Some(AFTER_KEY),
                    fix,
                );
            }
            self.resume();
            return None;
        };
        self.cursor.set_at(self.cursor.at() + op.typed.len());
        self.cursor.skip_spaces();
        let missing = |cursor: &mut Cursor| {
            let at = cursor.at();
            let end = if cursor.at_end() { at } else { at + 1 };
            let sentence = format!("a value is missing after `{}`", op.typed);
            cursor.problem(at, end, sentence, None, Fix::None);
        };
        let condition = match op.reads {
            Operation::Glob => {
                let Some(glob) = read_glob(&mut self.cursor) else {
                    // Where the glob stopped reading, the cursor records nothing more, so only
                    // a glob that never started is said to be missing.
                    missing(&mut self.cursor);
                    return None;
                };
                let place = self.cursor.place(start, glob.place.end);
                let clause = self.cursor.slice(start, place.end).to_string();
                self.after_value(true);
                Expr::Glob {
                    key,
                    glob,
                    clause,
                    place,
                }
            }
            Operation::Compare(compare) => {
                let read = ValueRead {
                    wildcards: compare == CompareOp::Eq,
                    in_list: false,
                };
                let Some(value) = self.value(read) else {
                    missing(&mut self.cursor);
                    return None;
                };
                let place = self.cursor.place(start, value.place.end);
                let clause = self.cursor.slice(start, place.end).to_string();
                self.after_value(false);
                Expr::Compare {
                    key,
                    op: compare,
                    value,
                    clause,
                    place,
                }
            }
        };
        if !key_read || self.cursor.stopped() {
            return None;
        }
        Some(if op.negated {
            Expr::Not(Box::new(condition))
        } else {
            condition
        })
    }

    /// `( value, … )`, from after `IN`: values separated by commas, spaces allowed around them.
    fn list(&mut self, key: InputKey, start: usize, key_read: bool, negated: bool) -> Option<Expr> {
        self.cursor.skip_spaces();
        let open = self.cursor.at();
        if !self.cursor.eat('(') {
            let end = open + self.next_word().len();
            let sentence = "`(` is missing after IN";
            self.cursor.problem(open, end, sentence, None, Fix::None);
            self.resume();
            return None;
        }
        let read = ValueRead {
            wildcards: true,
            in_list: true,
        };
        let mut values = Vec::new();
        let mut after_comma = false;
        loop {
            self.cursor.skip_spaces();
            let at = self.cursor.at();
            match self.cursor.peek() {
                None => {
                    let sentence = "this `(` is unclosed";
                    self.cursor
                        .problem(open, open + 1, sentence, None, Fix::None);
                    return None;
                }
                Some(')') => {
                    if after_comma {
                        let sentence = "a value is missing after `,`";
                        self.cursor.problem(at, at + 1, sentence, None, Fix::None);
                    } else if values.is_empty() {
                        let sentence = "the list is empty";
                        self.cursor
                            .problem(open, open + 1, sentence, None, Fix::None);
                    }
                    self.cursor.bump();
                    break;
                }
                Some(',') => {
                    let sentence = if after_comma {
                        "a value is missing after `,`"
                    } else {
                        "a value is missing before `,`"
                    };
                    self.cursor.problem(at, at + 1, sentence, None, Fix::None);
                    self.cursor.bump();
                    after_comma = true;
                    continue;
                }
                Some(_) => {}
            }
            values.push(self.value(read)?);
            let value_end = self.cursor.at();
            self.cursor.skip_spaces();
            if !matches!(self.cursor.peek(), None | Some(',' | ')')) {
                // The comma belongs where the value ended: under the first space, or, where the
                // next word follows the value straight on, under that word.
                let at = self.cursor.at();
                let end = if at == value_end {
                    at + self.next_word().len()
                } else {
                    value_end + 1
                };
                self.cursor.problem(
                    value_end,
                    end,
                    "values in a list are separated by commas",
                    None,
                    Fix::None,
                );
                self.skip_list_item();
            }
            after_comma = self.cursor.eat(',');
        }
        if !key_read {
            return None;
        }
        let place = self.cursor.place(start, self.cursor.at());
        let membership = Expr::In {
            key,
            values,
            clause: self.cursor.slice(start, place.end).to_string(),
            place,
        };
        Some(if negated {
            Expr::Not(Box::new(membership))
        } else {
            membership
        })
    }

    /// One value at the cursor: quoted text, or a bare value running to the next space, to a `)`
    /// that closes no `(` opened inside it, or in a list to such a `,`. `None` where no value
    /// stands — the end, a `)`, a list's `,` — or reading stopped in an unclosed quote.
    fn value(&mut self, read: ValueRead) -> Option<InputValue> {
        let start = self.cursor.at();
        let first = self.cursor.peek()?;
        if first == ')' || (read.in_list && first == ',') {
            return None;
        }
        if matches!(first, '"' | '\'') {
            let text = read_quoted(&mut self.cursor)?;
            let end = self.cursor.at();
            return Some(InputValue {
                text,
                quoted: true,
                input: self.cursor.slice(start, end).to_string(),
                place: self.cursor.place(start, end),
                needs_quotes: None,
            });
        }
        let mut depth = 0usize;
        let (mut wildcard, mut other) = (None, None);
        while let Some(c) = self.cursor.peek() {
            match c {
                c if is_space(c) => break,
                ')' if depth == 0 => break,
                ',' if read.in_list && depth == 0 => break,
                '(' => depth += 1,
                ')' => depth -= 1,
                _ => {}
            }
            let at = self.cursor.at();
            if read.wildcards && matches!(c, '*' | '?' | '[') {
                wildcard.get_or_insert(at);
            } else if !is_bare_value_char(c) {
                other.get_or_insert(at);
            }
            self.cursor.bump();
        }
        let end = self.cursor.at();
        let text = self.cursor.slice(start, end).to_string();
        Some(InputValue {
            input: text.clone(),
            text,
            quoted: false,
            place: self.cursor.place(start, end),
            needs_quotes: wildcard
                .map(NeedsQuotes::Wildcard)
                .or(other.map(NeedsQuotes::Character)),
        })
    }

    /// After a value, a space and more than `AND`, `OR`, a `)` or the end: the value ended at the
    /// space, the problem placed under that space, and reading resumes past what follows it.
    /// After a glob the sentence says which parts to quote.
    fn after_value(&mut self, glob: bool) {
        let value_end = self.cursor.at();
        self.cursor.skip_spaces();
        if self.cursor.at() == value_end
            || matches!(self.cursor.peek(), None | Some(')'))
            || self.at_keyword("AND")
            || self.at_keyword("OR")
        {
            return;
        }
        // The caret goes under the space that ended the value.
        let sentence = if glob {
            "a space separates values; quote the part with the space, but not * ? [ ]"
        } else {
            "a space separates values"
        };
        self.cursor.problem(
            value_end,
            value_end + 1,
            sentence,
            Some(AFTER_VALUE),
            Fix::None,
        );
        self.resume();
    }

    /// Resume after a problem: at the next `AND` or `OR` of this group, at the `)` closing it,
    /// or at the end, quoted text and closed parentheses stepped over whole.
    fn resume(&mut self) {
        skip_to(&mut self.cursor, |cursor, c| {
            c == ')'
                || (at_part_start(cursor)
                    && (at_keyword(cursor, "AND") || at_keyword(cursor, "OR")))
        });
    }

    /// Past the rest of a list item: to its `,` or the list's `)`.
    fn skip_list_item(&mut self) {
        skip_to(&mut self.cursor, |_, c| matches!(c, ',' | ')'));
    }

    fn at_keyword(&self, word: &str) -> bool {
        at_keyword(&self.cursor, word)
    }

    /// Read `word` if it stands at the cursor.
    fn eat_keyword(&mut self, word: &str) -> bool {
        if !self.at_keyword(word) {
            return false;
        }
        self.cursor.set_at(self.cursor.at() + word.len());
        true
    }

    /// The next word as typed, for a sentence naming it: quoted text whole where a part starts;
    /// one of `(`, `)` or `,`; or what the reader steps over up to where something may follow a
    /// key (a space, an operator, `?`, `[`, `|`, a `)` or a comma).
    fn next_word(&self) -> &'t str {
        let rest = self.cursor.rest();
        let Some(first) = rest.chars().next() else {
            return "";
        };
        let end = match first {
            '"' | '\'' if at_part_start(&self.cursor) => {
                let mut quoted = Cursor::new(rest, 0);
                read_quoted(&mut quoted);
                quoted.at()
            }
            '(' | ')' | ',' => 1,
            // A word is what the reader steps over to where something may follow a key.
            _ => {
                let after_first = self.cursor.at() + first.len_utf8();
                let end = skip_end(self.cursor.text(), after_first, |at, _| {
                    follows_a_key_in_a_filter(at.rest())
                });
                end - self.cursor.at()
            }
        };
        cut(rest, 0, end)
    }
}

/// An operator as typed, what it reads as, and whether it is `NOT` of that.
struct Operator {
    typed: &'static str,
    reads: Operation,
    negated: bool,
}

/// What an operator reads its value as: a comparison's value, or a glob.
#[derive(Clone, Copy)]
enum Operation {
    Compare(CompareOp),
    Glob,
}

/// The operator `rest` starts with, the longest that fits: `!=`, `!~`, `<=`, `>=`, `=`, `<`,
/// `>`, `~`.
fn operator_at(rest: &str) -> Option<Operator> {
    use Operation::{Compare, Glob};
    const OPERATORS: [(&str, Operation, bool); 8] = [
        ("!=", Compare(CompareOp::Eq), true),
        ("!~", Glob, true),
        ("<=", Compare(CompareOp::Le), false),
        (">=", Compare(CompareOp::Ge), false),
        ("=", Compare(CompareOp::Eq), false),
        ("<", Compare(CompareOp::Lt), false),
        (">", Compare(CompareOp::Gt), false),
        ("~", Glob, false),
    ];
    OPERATORS
        .iter()
        .find(|(typed, _, _)| rest.starts_with(typed))
        .map(|&(typed, reads, negated)| Operator {
            typed,
            reads,
            negated,
        })
}

/// The parts read, joined: none is nothing, one is itself.
fn join(mut parts: Vec<Expr>, joined: fn(Vec<Expr>) -> Expr) -> Option<Expr> {
    match parts.len() {
        0 => None,
        1 => parts.pop(),
        _ => Some(joined(parts)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every problem as (caret byte, sentence, rule line, fix).
    pub(super) fn problems(reading: &[Problem]) -> Vec<(usize, String, Option<String>, Fix)> {
        reading
            .iter()
            .map(|p| {
                (
                    p.place.start,
                    p.sentence.clone(),
                    p.rule_line.clone(),
                    p.fix.clone(),
                )
            })
            .collect()
    }

    #[test]
    fn a_quote_reads_its_closing_mark_doubled_and_every_other_character_as_itself() {
        for (text, want) in [
            (r#""say ""hi""""#, r#"say "hi""#),
            ("'it''s'", "it's"),
            (r#""it's""#, "it's"),
            (r#""C:\photos\""#, r"C:\photos\"),
            (r#""""#, ""),
        ] {
            let mut cursor = Cursor::new(text, 0);
            assert_eq!(read_quoted(&mut cursor).as_deref(), Some(want), "{text}");
            assert!(cursor.at_end(), "{text}");
        }
        let mut cursor = Cursor::new(r#""a""b"#, 0);
        assert_eq!(read_quoted(&mut cursor), None);
        assert!(cursor.stopped());
        assert_eq!(
            problems(&cursor.into_problems()),
            [(0, "this quote is unclosed".to_string(), None, Fix::None)]
        );
    }

    // =========================================================================
    // The filter grammar
    // =========================================================================

    use super::super::filter::StatusPredicate;
    use super::super::glob::{needs_quotes_sentence, InputGlob};
    use super::super::key::Key;
    use super::super::transform::PathAccessor;

    type Said = (usize, String, Option<String>, Fix);

    /// What a filter reads as, and every problem met reading it.
    fn read(text: &str) -> (Option<Expr>, Vec<Said>) {
        let reading = read_filter(text, 0);
        (reading.value, problems(&reading.problems))
    }

    /// Every problem's caret byte and sentence.
    fn said(text: &str) -> Vec<(usize, String)> {
        read(text)
            .1
            .into_iter()
            .map(|(at, sentence, _, _)| (at, sentence))
            .collect()
    }

    fn at(at: usize, sentence: &str) -> Vec<(usize, String)> {
        vec![(at, sentence.to_string())]
    }

    /// A filter that reads with no problem.
    fn runs(text: &str) -> Expr {
        let (value, problems) = read(text);
        assert!(problems.is_empty(), "{text}: {problems:?}");
        value.unwrap_or_else(|| panic!("{text}: nothing read"))
    }

    fn compare_of(text: &str) -> (InputKey, CompareOp, InputValue, String) {
        match runs(text) {
            Expr::Compare {
                key,
                op,
                value,
                clause,
                ..
            } => (key, op, value, clause),
            other => panic!("{text}: expected a comparison, got {other:?}"),
        }
    }

    fn glob_of(text: &str) -> (InputKey, InputGlob, String) {
        match runs(text) {
            Expr::Glob {
                key, glob, clause, ..
            } => (key, glob, clause),
            other => panic!("{text}: expected a glob, got {other:?}"),
        }
    }

    const MODIFIERS: &str = "year, month, day, hour, minute, second, date, time, datetime, \
                             yearmonth, week, weekday, quarter, stem, ext, short, lowercase, \
                             uppercase, capitalize, bucket, number";

    /// One row per position of the grammar, each with its sentence and rule line as the
    /// language states them, the caret byte counted by hand.
    #[test]
    fn every_sentence_of_the_filter_grammar() {
        let bucket = Some("|bucket takes numbers: |bucket(10,100)");
        let after_key = Some(AFTER_KEY);
        let after_condition = Some(AFTER_CONDITION);
        let modifier = format!("`|yeer` is not a modifier — Modifiers: {MODIFIERS}");
        let position = "`x` is not a position — positions are whole numbers: [0], [-1], [1:3]";
        let status = "a stored fact of that name is `content.archived`";
        let rows: Vec<(&str, usize, &str, Option<&str>)> = vec![
            (r#"title="abc"#, 6, "this quote is unclosed", None),
            ("(a? AND b?", 0, "this `(` is unclosed", None),
            ("source.rel_path[0=1", 15, "this `[` is unclosed", None),
            ("source.size>1000)", 16, "`)` has no matching `(`", None),
            (
                "Model=EOS 5D",
                9,
                "a space separates values",
                Some(AFTER_VALUE),
            ),
            (
                "archived? hashed?",
                10,
                "AND or OR is missing before `hashed`",
                after_condition,
            ),
            (
                "filename jpg",
                9,
                "an operator is missing after `filename`",
                after_key,
            ),
            (
                "source.size|bucket()=1",
                19,
                "a number is missing after `(`",
                bucket,
            ),
            ("a? AND", 6, "a condition is missing after `AND`", None),
            ("=jpg", 0, "a key is missing before `=`", None),
            ("source.ext IN jpg", 14, "`(` is missing after IN", None),
            ("source.ext =", 12, "a value is missing after `=`", None),
            (
                "source.ext IN (jpg,)",
                19,
                "a value is missing after `,`",
                None,
            ),
            ("source.ext IN ()", 14, "the list is empty", None),
            ("", 0, "the filter is empty", None),
            ("%x?", 0, "`%x` is not a key", None),
            ("source.rel_path[x]=1", 16, position, None),
            ("source.mtime|yeer=1", 12, &modifier, None),
            (
                "source.mtime|=1",
                13,
                "a modifier is missing after `|`",
                None,
            ),
            ("source.size|bucket(a)=1", 19, "`a` is not a number", bucket),
            (
                "source.size|bucket(300,60)=1",
                23,
                "each number of |bucket must be larger than the one before",
                bucket,
            ),
            (
                "archived=1",
                0,
                "`archived` can only be used with `?`",
                Some(status),
            ),
            (
                "scope.rel_path=a",
                0,
                "`scope.rel_path` can only be used in a pattern",
                None,
            ),
            (
                "archived? 1",
                10,
                "AND or OR is missing before `1`",
                after_condition,
            ),
        ];
        for (text, caret, sentence, rule) in rows {
            assert_eq!(
                read(text).1,
                vec![(
                    caret,
                    sentence.to_string(),
                    rule.map(str::to_string),
                    Fix::None
                )],
                "{text}"
            );
        }
        // Where a quoted name is the fix, the position's own sentence carries it.
        assert_eq!(
            read("content.Foo@Bar=1").1,
            vec![(
                11,
                "an operator is missing after `content.Foo`".to_string(),
                Some(AFTER_KEY.to_string()),
                Fix::KeyName {
                    rewrite: r#""content.Foo@Bar""#.to_string()
                }
            )]
        );
        // The sentences name what was typed, never a part of the reader.
        assert_eq!(
            said("source.ext jpg"),
            at(11, "an operator is missing after `source.ext`")
        );
        assert_eq!(said("= jpg"), at(0, "a key is missing before `=`"));
        assert_eq!(said("!= jpg"), at(0, "a key is missing before `!=`"));
        assert_eq!(said("? AND a?"), at(0, "a key is missing before `?`"));
        // A value may start with `(`: it is read as a value that needs quotes.
        let (_, _, value, _) = compare_of("source.ext = (");
        assert_eq!(value.needs_quotes, Some(NeedsQuotes::Character(13)));
    }

    /// Each condition of a tree, as (clause, place start, place end), in order.
    fn conditions(expr: &Expr) -> Vec<(String, usize, usize)> {
        let mut out = Vec::new();
        fn walk(expr: &Expr, out: &mut Vec<(String, usize, usize)>) {
            match expr {
                Expr::And(parts) | Expr::Or(parts) => parts.iter().for_each(|part| walk(part, out)),
                Expr::Not(inner) => walk(inner, out),
                Expr::Exists { clause, place, .. }
                | Expr::Compare { clause, place, .. }
                | Expr::In { clause, place, .. }
                | Expr::Glob { clause, place, .. } => {
                    out.push((clause.clone(), place.start, place.end))
                }
                Expr::Status { place, .. } => out.push((String::new(), place.start, place.end)),
            }
        }
        walk(expr, &mut out);
        out
    }

    fn clauses(expr: &Expr) -> Vec<String> {
        conditions(expr)
            .into_iter()
            .map(|(clause, _, _)| clause)
            .collect()
    }

    #[test]
    fn reading_resumes_where_the_value_ends() {
        let text = "Model=EOS 5D AND filename=a%b c AND x?";
        let (value, _) = read(text);
        assert_eq!(
            said(text),
            [
                (9, "a space separates values".to_string()),
                (29, "a space separates values".to_string())
            ]
        );
        assert_eq!(
            clauses(&value.unwrap()),
            ["Model=EOS", "filename=a%b", "x?"]
        );
    }

    #[test]
    fn a_grammar_problem_resumes_at_the_next_and_or_or_closing_paren() {
        let text = "a b AND c d OR (e f) AND g?";
        let (value, _) = read(text);
        assert_eq!(
            said(text),
            [
                (2, "an operator is missing after `a`".to_string()),
                (10, "an operator is missing after `c`".to_string()),
                (18, "an operator is missing after `e`".to_string()),
            ]
        );
        assert_eq!(clauses(&value.unwrap()), ["g?"]);
        // Quotes and parentheses on the way are stepped over whole.
        assert_eq!(
            said(r#"a b="x AND y" (c AND d) AND %e?"#),
            [
                (2, "an operator is missing after `a`".to_string()),
                (28, "`%e` is not a key".to_string()),
            ]
        );
        // An apostrophe inside a word opens no quote.
        assert_eq!(
            said("a O'Brien AND %e?"),
            [
                (2, "an operator is missing after `a`".to_string()),
                (14, "`%e` is not a key".to_string()),
            ]
        );
    }

    #[test]
    fn reading_stops_at_an_unclosed_quote() {
        for (text, caret) in [
            (r#"title="abc AND %x?"#, 6),
            (r#""abc=1 AND %"#, 0),
            ("x IN ('a, b) AND %", 6),
            ("a=1 AND b='x OR (", 10),
        ] {
            assert_eq!(said(text), at(caret, "this quote is unclosed"), "{text}");
        }
        // What was found before the stop is kept.
        assert_eq!(
            said(r#"a b AND c="x"#),
            [
                (2, "an operator is missing after `a`".to_string()),
                (10, "this quote is unclosed".to_string()),
            ]
        );
        // A quote met while reading resumes past a problem is read by the same rule.
        assert_eq!(
            said(r#"a b "c AND d?"#),
            [
                (2, "an operator is missing after `a`".to_string()),
                (4, "this quote is unclosed".to_string()),
            ]
        );
        assert_eq!(
            said(r#"a? "b"#),
            [
                (3, r#"AND or OR is missing before `"b`"#.to_string()),
                (3, "this quote is unclosed".to_string()),
            ]
        );
        // In a report key, what cannot follow is named first, as in a filter.
        let said_keys = |text: &str| -> Vec<(usize, String)> {
            read_keys(text, 0, Grammar::Key)
                .problems
                .into_iter()
                .map(|p| (p.place.start, p.sentence))
                .collect()
        };
        assert_eq!(
            said_keys("Make 'x"),
            [
                (5, "`'x` cannot follow `Make`".to_string()),
                (5, "this quote is unclosed".to_string()),
            ]
        );
        assert_eq!(
            said_keys("Make x 'y"),
            [
                (5, "`x 'y` cannot follow `Make`".to_string()),
                (7, "this quote is unclosed".to_string()),
            ]
        );
        // A quote mark where no part starts opens no quote, in a sentence's word as in a skip.
        assert_eq!(
            said("a?'b AND c?"),
            at(2, "AND or OR is missing before `'b`")
        );
    }

    /// A missing separator is placed under the space that ended the value: the first of
    /// several, a tab or a line break as well as a space.
    #[test]
    fn a_missing_separator_is_placed_under_the_space() {
        let places = |text: &str| -> Vec<(usize, usize, String)> {
            read_filter(text, 0)
                .problems
                .into_iter()
                .map(|p| (p.place.start, p.place.end, p.sentence))
                .collect()
        };
        let space = "a space separates values".to_string();
        let comma = "values in a list are separated by commas".to_string();
        assert_eq!(places("Model=EOS 5D"), [(9, 10, space.clone())]);
        assert_eq!(places("Model=EOS   5D"), [(9, 10, space.clone())]);
        assert_eq!(places("Model=EOS\t5D"), [(9, 10, space.clone())]);
        assert_eq!(places("Model=EOS\n5D"), [(9, 10, space)]);
        assert_eq!(places("x IN (a b(c d))"), [(7, 8, comma.clone())]);
        assert_eq!(places("x IN (a   b)"), [(7, 8, comma)]);
    }

    /// A problem's word ends by the rule reading resumes by, so the skip past it never stops
    /// inside it.
    #[test]
    fn a_word_named_and_a_skip_end_at_the_same_place() {
        let places = |text: &str| -> Vec<(usize, usize, String)> {
            read_filter(text, 0)
                .problems
                .into_iter()
                .map(|p| (p.place.start, p.place.end, p.sentence))
                .collect()
        };
        // A quote mark inside a word opens no quote, in the word or after it.
        assert_eq!(
            places("Make's=1 AND b?"),
            [(4, 6, "an operator is missing after `Make`".to_string())]
        );
        let (value, problems) = read("a? Wow!'x AND b?");
        assert_eq!(
            problems.into_iter().map(|p| (p.0, p.1)).collect::<Vec<_>>(),
            at(3, "AND or OR is missing before `Wow!'x`")
        );
        assert_eq!(clauses(&value.unwrap()), ["a?", "b?"]);
        // A word's own parentheses are stepped over whole, quoted text inside them too.
        assert_eq!(
            said("a? content.Foo(a b)?"),
            at(3, "AND or OR is missing before `content.Foo(a b)`")
        );
        assert_eq!(
            said("a? content.Foo(')')?"),
            at(3, "AND or OR is missing before `content.Foo(')')`")
        );
        // With no space before it, the word after a list item is the problem's place.
        assert_eq!(
            places(r#"x IN ("a"b(c d))"#),
            [(
                9,
                15,
                "values in a list are separated by commas".to_string()
            )]
        );
        // A `(` that never closes is a character like any other: reading resumes past it, and
        // the quoted name ends where the name does.
        let (value, problems) = read("Make(x AND b?");
        assert_eq!(
            problems,
            [(
                4,
                "an operator is missing after `Make`".to_string(),
                Some(AFTER_KEY.to_string()),
                Fix::KeyName {
                    rewrite: r#""Make(x""#.to_string()
                }
            )]
        );
        assert_eq!(clauses(&value.unwrap()), ["b?"]);
        // A quote that opens in a word where a part starts is the word's, and is named after it.
        assert_eq!(
            said("a? f('x AND b?"),
            [
                (3, "AND or OR is missing before `f('x AND b?`".to_string()),
                (5, "this quote is unclosed".to_string()),
            ]
        );
    }

    /// Each `(` is paired with its `)` once, so a long run of them, closed or not, is read in
    /// time that grows with the text, never with the number of ways to pair them.
    #[test]
    fn many_parentheses_are_read_in_one_pass() {
        // Read on a thread with a time limit, so a pairing that grows with the ways to pair
        // fails here instead of never finishing.
        let (done, finished) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let open = "(".repeat(5000);
            let unclosed = said(&format!("a? f{open} AND b?"));
            let report = read_keys(&format!("Make x{open}, Model"), 0, Grammar::KeyList);
            let nested = said(&format!("a? f{open}x{} AND b?", ")".repeat(5000)));
            let _ = done.send((open, unclosed, report.problems.len(), nested.len()));
        });
        let (open, unclosed, report, nested) = finished
            .recv_timeout(std::time::Duration::from_secs(20))
            .expect("5,000 parentheses are read within 20 seconds");
        assert_eq!(
            unclosed,
            at(3, &format!("AND or OR is missing before `f{open}`"))
        );
        assert_eq!((report, nested), (1, 1));
    }

    #[test]
    fn a_text_that_is_not_a_key_ends_by_the_same_rule() {
        // A `(` in it pairs with the first `)` that closes it, a group's included: one rule
        // for every scan, which then names the group as unclosed.
        assert_eq!(
            said("(%(a OR c?)"),
            [
                (1, "`%(a OR c?)` is not a key".to_string()),
                (0, "this `(` is unclosed".to_string()),
            ]
        );
        assert_eq!(said("%(a b)=1 AND c?"), at(0, "`%(a b)` is not a key"));
        assert_eq!(clauses(&read("%(a b)=1 AND c?").0.unwrap()), ["c?"]);
        assert_eq!(said("%('a b')?"), at(0, "`%('a b')` is not a key"));
        let reading = read_keys("% 'x", 0, Grammar::Key);
        assert_eq!(
            problems(&reading.problems)
                .into_iter()
                .map(|p| (p.0, p.1))
                .collect::<Vec<_>>(),
            [
                (0, "`% 'x` is not a key".to_string()),
                (2, "this quote is unclosed".to_string()),
            ]
        );
    }

    /// A space ends a value even inside a `(` the value opened: the `(` is value text, not a
    /// group's, so a `)` after the space closes no group, and the grouping sentence says so.
    #[test]
    fn a_space_ends_a_value_even_inside_its_own_parenthesis() {
        let paren = "`)` has no matching `(`".to_string();
        let space = "a space separates values".to_string();
        assert_eq!(
            said("x=a(b c) AND y?"),
            [(5, space.clone()), (7, paren.clone())]
        );
        assert_eq!(
            clauses(&read("x=a(b c) AND y?").0.unwrap()),
            ["x=a(b", "y?"]
        );
        assert_eq!(said("x=(a b)"), [(4, space.clone()), (6, paren.clone())]);
        // Reading never goes back into a value it has read: `OR` in `a(OR` stays value text.
        assert_eq!(said("x=a(OR b"), [(6, space)]);
    }

    #[test]
    fn reading_stops_at_an_unclosed_bracket() {
        assert_eq!(
            said("source.rel_path[0 AND %x? AND ("),
            at(15, "this `[` is unclosed")
        );
    }

    #[test]
    fn a_value_with_spaces_is_one_report() {
        let text = "Model=Canon EOS 5D Mark II";
        assert_eq!(said(text), at(11, "a space separates values"));
        let (value, _) = read(text);
        assert_eq!(clauses(&value.unwrap()), ["Model=Canon"]);
    }

    #[test]
    fn a_value_followed_by_more_is_two_problems() {
        let text = "Title=O'Brien was here";
        assert_eq!(said(text), at(13, "a space separates values"));
        let Some(Expr::Compare { value, .. }) = read(text).0 else {
            panic!("the comparison is read up to the space");
        };
        assert_eq!(value.text, "O'Brien");
        assert_eq!(value.needs_quotes, Some(NeedsQuotes::Character(7)));
    }

    #[test]
    fn a_space_separates_values_always_carries_its_rule_line() {
        for text in [
            "Model=EOS 5D",
            "a=1 and b=2",
            r#"x="a b" c"#,
            "source.ext= AND source.size>1",
            "(a=1 b)",
            "a=1 b AND c=2 d",
        ] {
            let problems = read(text).1;
            assert!(!problems.is_empty(), "{text}");
            for (_, sentence, rule, _) in problems {
                assert_eq!(sentence, "a space separates values", "{text}");
                assert_eq!(rule.as_deref(), Some(AFTER_VALUE), "{text}");
            }
        }
    }

    #[test]
    fn a_missing_and_or_always_carries_its_rule_line() {
        for (text, word) in [
            ("archived? hashed?", "hashed"),
            ("a? and b?", "and"),
            ("x IN (a) y?", "y"),
            ("(a?) b?", "b"),
            (r#"Title="a"b"#, "b"),
            ("a? (b?)", "("),
            ("a? %x", "%x"),
            ("a? O'Brien", "O'Brien"),
            ("a? cafe\u{301}", "cafe\u{301}"),
            ("a? hashed? b?", "hashed"),
            ("a? Wow! x", "Wow!"),
            ("a? content.Foo(5)?", "content.Foo(5)"),
        ] {
            let problems = read(text).1;
            assert_eq!(problems.len(), 1, "{text}");
            let (_, sentence, rule, _) = &problems[0];
            assert_eq!(
                sentence,
                &format!("AND or OR is missing before `{word}`"),
                "{text}"
            );
            assert_eq!(rule.as_deref(), Some(AFTER_CONDITION), "{text}");
        }
    }

    #[test]
    fn a_keyword_is_a_keyword_only_where_one_may_stand() {
        // Where a value stands, a capital keyword is a value.
        assert_eq!(compare_of("Title=AND").2.text, "AND");
        assert_eq!(compare_of("Title=NOT").2.text, "NOT");
        let Expr::In { values, .. } = runs("x IN (a, AND)") else {
            panic!("a list");
        };
        assert_eq!(
            values.iter().map(|v| v.text.as_str()).collect::<Vec<_>>(),
            ["a", "AND"]
        );
        assert_eq!(clauses(&runs("Title=AND AND x?")), ["Title=AND", "x?"]);
        // An empty shell variable: one report, showing how the value was read.
        assert_eq!(
            said("source.ext= AND source.size>1"),
            at(15, "a space separates values")
        );
        // Where a condition must start, `AND`, `OR` and `IN` are names; `NOT` is a keyword.
        assert_eq!(
            said("a? AND OR b?"),
            at(10, "an operator is missing after `OR`")
        );
        assert_eq!(
            said("a? AND AND b?"),
            at(11, "an operator is missing after `AND`")
        );
        assert_eq!(said("AND a?"), at(4, "an operator is missing after `AND`"));
        for word in ["AND", "OR", "IN"] {
            let Expr::Exists { key, .. } = runs(&format!("{word}?")) else {
                panic!("{word}? asks whether a stored fact has a value");
            };
            assert_eq!(key.key, Some(Key::ContentFact(format!("content.{word}"))));
        }
        assert!(matches!(runs("NOT a?"), Expr::Not(_)));
        // Where nothing that can be a key follows, the condition is missing.
        assert_eq!(said("a? AND"), at(6, "a condition is missing after `AND`"));
        assert_eq!(
            said("a? AND )"),
            [
                (7, "a condition is missing after `AND`".to_string()),
                (7, "`)` has no matching `(`".to_string()),
            ]
        );
        assert_eq!(
            said("(a? AND )"),
            at(8, "a condition is missing after `AND`")
        );
        assert_eq!(said("NOT"), at(3, "a condition is missing after `NOT`"));
    }

    #[test]
    fn a_space_before_a_comma_or_paren_in_a_list_is_allowed() {
        let Expr::In { values, clause, .. } = runs("x IN ( a , b )") else {
            panic!("a list");
        };
        assert_eq!(
            values.iter().map(|v| v.text.as_str()).collect::<Vec<_>>(),
            ["a", "b"]
        );
        assert_eq!(clause, "x IN ( a , b )");
    }

    /// The parts of a glob that need quotes, as the caret byte and the sentence binding words
    /// each with for a text or path key.
    fn glob_needs(text: &str) -> Vec<(usize, String)> {
        let (_, glob, _) = glob_of(text);
        glob.needs_quotes
            .iter()
            .map(|&at| {
                let c = cut(text, at, text.len()).chars().next().unwrap();
                (at, needs_quotes_sentence(c))
            })
            .collect()
    }

    #[test]
    fn a_glob_ends_where_a_value_ends() {
        // A `(` opened inside the glob closes inside it: one glob, one part needing quotes.
        let (_, glob, clause) = glob_of("filename~*(1)*");
        assert_eq!(
            (glob.input.as_str(), clause.as_str()),
            ("*(1)*", "filename~*(1)*")
        );
        assert_eq!(glob.needs_quotes, [10]);
        // A `)` closing a `(` opened before the glob ends it.
        assert_eq!(
            clauses(&runs("(filename~*(1)* OR x=1)")),
            ["filename~*(1)*", "x=1"]
        );
        assert_eq!(
            clauses(&runs("(filename~*.jpg) AND x?")),
            ["filename~*.jpg", "x?"]
        );
        // A space inside quotes is part of the glob; one reached inside a set leaves the set
        // unclosed, so a forgotten `]` never takes in the clauses after it.
        assert_eq!(said("filename~*[ _]*"), at(10, "this set is unclosed"));
        assert_eq!(
            said("filename~[abc OR filename~x.jpg]"),
            at(9, "this set is unclosed")
        );
        assert_eq!(
            glob_of(r#"source.rel_path~*/"My Pictures"/*"#).1.input,
            r#"*/"My Pictures"/*"#
        );
        // Quoted, the glob is its text.
        let (_, glob, _) = glob_of(r#"filename~"*.jpg""#);
        assert!(glob.glob.matches("*.jpg"));
        assert!(!glob.glob.matches("a.jpg"));
        assert_eq!(said("filename~"), at(9, "a value is missing after `~`"));
    }

    /// One row per place a glob cannot be read, with its sentence and rule line as the record
    /// states them, the caret byte counted by hand.
    #[test]
    fn every_sentence_of_the_glob_grammar() {
        const SPACE: &str =
            "a space separates values; quote the part with the space, but not * ? [ ]";
        for (text, caret, sentence, rule_line) in [
            ("filename~[abc", 9, "this set is unclosed", None),
            // Reading stops at the set: nothing after it is recorded.
            ("filename~*[2024* AND x=", 10, "this set is unclosed", None),
            ("filename~O'Brien*", 10, "this quote is unclosed", None),
            (
                "source.rel_path~*/My Pictures/*",
                20,
                SPACE,
                Some(AFTER_VALUE),
            ),
            ("filename~IMG* and x", 13, SPACE, Some(AFTER_VALUE)),
            (
                "filename=IMG x",
                12,
                "a space separates values",
                Some(AFTER_VALUE),
            ),
        ] {
            assert_eq!(
                read(text).1,
                [(
                    caret,
                    sentence.to_string(),
                    rule_line.map(str::to_string),
                    Fix::None
                )],
                "{text}"
            );
        }
        for (text, c) in [
            ("filename~a@b", '@'),
            ("filename~a#b", '#'),
            ("filename~a%b", '%'),
            ("filename~a|b", '|'),
            (r"filename~a\b", '\\'),
            ("filename~a(b", '('),
            ("filename~a=b", '='),
        ] {
            assert_eq!(
                glob_needs(text),
                [(
                    10,
                    format!("`{c}` needs quotes; quote that part, but not * ? [ ]")
                )],
                "{text}"
            );
        }
    }

    #[test]
    fn a_negated_glob_reads_as_not_of_the_glob() {
        for text in ["ext!~*.jpg", "ext !~ *.bak"] {
            let Expr::Not(inner) = runs(text) else {
                panic!("{text}: NOT");
            };
            let Expr::Glob { clause, .. } = *inner else {
                panic!("{text}: a glob");
            };
            assert_eq!(clause, text);
        }
    }

    #[test]
    fn each_part_that_needs_quotes_is_one_problem() {
        assert_eq!(glob_of("filename~*R&B#1*").1.needs_quotes, [11]);
        assert_eq!(glob_of("filename~*a@b*c#d*").1.needs_quotes, [11, 15]);
        assert_eq!(glob_of(r#"filename~a@"x"b#"#).1.needs_quotes, [10, 15]);
        // A set's members are never reported.
        assert_eq!(glob_of("filename~*[@#]x@*").1.needs_quotes, [15]);
        // Where reading stops inside the glob, the parts before the stop are worded by the
        // reader, then the stop; on any key, since the condition is never bound.
        let part = |c: char| format!("`{c}` needs quotes; quote that part, but not * ? [ ]");
        for (text, before, stop, sentence) in [
            ("filename~a%b[c", (10, '%'), 12, "this set is unclosed"),
            ("filename~*R&B's*", (11, '&'), 13, "this quote is unclosed"),
            ("source.mtime~a%b[c", (14, '%'), 16, "this set is unclosed"),
        ] {
            let (expr, problems) = read(text);
            assert!(expr.is_none(), "{text}");
            assert_eq!(
                problems,
                [
                    (before.0, part(before.1), None, Fix::None),
                    (stop, sentence.to_string(), None, Fix::None)
                ],
                "{text}"
            );
        }
    }

    #[test]
    fn lower_case_connectives_and_a_lone_bang_are_read_by_position() {
        assert_eq!(
            read("archived? and hashed?").1,
            [(
                10,
                "AND or OR is missing before `and`".to_string(),
                Some(AFTER_CONDITION.to_string()),
                Fix::None
            )]
        );
        assert_eq!(
            read("not hashed?").1,
            [(
                4,
                "an operator is missing after `not`".to_string(),
                Some(AFTER_KEY.to_string()),
                Fix::None
            )]
        );
        assert_eq!(said("!hashed?"), at(0, "`!hashed` is not a key"));
    }

    #[test]
    fn keywords_count_only_in_exact_capitals() {
        assert_eq!(
            said("a? aNd b?"),
            at(3, "AND or OR is missing before `aNd`")
        );
        assert_eq!(said("a? or b?"), at(3, "AND or OR is missing before `or`"));
        assert_eq!(said("x in (a)"), at(2, "an operator is missing after `x`"));
        // The Turkish dotless `ı` makes `IN` when upper-cased, and is no keyword for it.
        assert_eq!(said("x ın (a)"), at(2, "an operator is missing after `x`"));
        assert_eq!(
            said("Title=Cats or Dogs?"),
            at(10, "a space separates values")
        );
        let Expr::Or(parts) = runs("a? AND b? OR c?") else {
            panic!("OR joins last");
        };
        assert!(matches!(
            parts.as_slice(),
            [Expr::And(_), Expr::Exists { .. }]
        ));
    }

    #[test]
    fn status_words_count_only_in_lower_case_and_only_with_a_question_mark() {
        for (text, predicate) in [
            ("archived?", StatusPredicate::Archived),
            ("hashed?", StatusPredicate::Hashed),
            ("excluded?", StatusPredicate::Excluded),
            ("enriched?", StatusPredicate::Enriched),
            ("archived ?", StatusPredicate::Archived),
        ] {
            assert!(
                matches!(runs(text), Expr::Status { predicate: p, .. } if p == predicate),
                "{text}"
            );
        }
        let Expr::Not(inner) = runs("NOT archived?") else {
            panic!("NOT of the status word");
        };
        assert!(matches!(*inner, Expr::Status { .. }));
        let Expr::And(parts) = runs("archived? AND mime~image/*") else {
            panic!("AND");
        };
        assert!(matches!(
            parts.as_slice(),
            [Expr::Status { .. }, Expr::Glob { .. }]
        ));
        // In any other case, or quoted, or a near word, it is a key.
        for (text, base) in [
            ("Archived?", "content.Archived"),
            (r#""archived"?"#, "content.archived"),
            ("'hashed'?", "content.hashed"),
            ("archival?", "content.archival"),
        ] {
            let Expr::Exists { key, .. } = runs(text) else {
                panic!("{text} is a key");
            };
            assert_eq!(key.base, base, "{text}");
        }
        // With anything but `?`, the status word's own sentence.
        let line = |word: &str| Some(format!("a stored fact of that name is `content.{word}`"));
        for text in [
            "archived[0]?",
            "archived|year?",
            "archived = true",
            "archived AND x?",
            "archived",
            "archived IN (a)",
        ] {
            assert_eq!(
                read(text).1,
                [(
                    0,
                    "`archived` can only be used with `?`".to_string(),
                    line("archived"),
                    Fix::None
                )],
                "{text}"
            );
        }
        assert_eq!(
            read("hashed ~ something").1,
            [(
                0,
                "`hashed` can only be used with `?`".to_string(),
                line("hashed"),
                Fix::None
            )]
        );
        // The clause is read through, so a problem after it is found too.
        assert_eq!(
            said("archived = true AND %x?"),
            [
                (0, "`archived` can only be used with `?`".to_string()),
                (20, "`%x` is not a key".to_string()),
            ]
        );
    }

    #[test]
    fn plain_values_of_any_language_read_unquoted() {
        for value in [
            "照片1.jpg",
            "Москва.jpg",
            "राम.jpg",
            "cafe\u{301}.jpg",
            "a+b:c/d-e_f.g",
        ] {
            let (_, _, read, _) = compare_of(&format!("filename={value}"));
            assert_eq!((read.text.as_str(), read.needs_quotes), (value, None));
        }
    }

    #[test]
    fn a_wildcard_anywhere_outranks_needs_quotes() {
        let needs = |text: &str| compare_of(text).2.needs_quotes;
        assert_eq!(needs("filename=a%b*"), Some(NeedsQuotes::Wildcard(12)));
        assert_eq!(needs("filename=IMG*"), Some(NeedsQuotes::Wildcard(12)));
        assert_eq!(needs("filename=[x]"), Some(NeedsQuotes::Wildcard(9)));
        assert_eq!(needs("filename=a%b"), Some(NeedsQuotes::Character(10)));
        // A lone `]` is no wildcard.
        assert_eq!(needs("filename=a]b"), Some(NeedsQuotes::Character(10)));
        // After an ordering operator a wildcard character is one that needs quotes.
        assert_eq!(needs("source.size<a*"), Some(NeedsQuotes::Character(13)));
        let Expr::Not(inner) = runs("filename!=a?b") else {
            panic!("!= is NOT of =");
        };
        let Expr::Compare { value, .. } = *inner else {
            panic!("a comparison");
        };
        assert_eq!(value.needs_quotes, Some(NeedsQuotes::Wildcard(11)));
        let Expr::In { values, .. } = runs("x IN (a, b*)") else {
            panic!("a list");
        };
        assert_eq!(values[1].needs_quotes, Some(NeedsQuotes::Wildcard(10)));
    }

    #[test]
    fn an_empty_group_or_list_item_and_text_touching_a_quote() {
        assert_eq!(said("()"), at(1, "a condition is missing after `(`"));
        assert_eq!(
            said("a? AND ( )"),
            at(9, "a condition is missing after `(`")
        );
        assert_eq!(said(") a?"), at(0, "`)` has no matching `(`"));
        assert_eq!(said(")"), at(0, "`)` has no matching `(`"));
        assert_eq!(said("x IN (,a)"), at(6, "a value is missing before `,`"));
        assert_eq!(said("x IN (a,,b)"), at(8, "a value is missing after `,`"));
        assert_eq!(
            said("x IN (a b)"),
            at(7, "values in a list are separated by commas")
        );
        assert_eq!(
            said(r#"x IN ("a"b)"#),
            at(9, "values in a list are separated by commas")
        );
        assert_eq!(
            said(r#"Title="a"b"#),
            at(9, "AND or OR is missing before `b`")
        );
        assert_eq!(clauses(&runs(r#"Title="a"AND b?"#)), [r#"Title="a""#, "b?"]);
    }

    #[test]
    fn an_alias_where_a_condition_belongs_is_named_and_an_at_in_a_value_needs_quotes() {
        let (value, problems) = read("@image AND x?");
        assert_eq!(
            problems,
            [(
                0,
                "'@image' is an alias, and aliases are not expanded here; write the filter it \
                 stands for"
                    .to_string(),
                None,
                Fix::None
            )]
        );
        assert_eq!(clauses(&value.unwrap()), ["x?"]);
        assert_eq!(said("@1"), at(0, "`@1` is not a key"));
        let (_, _, value, _) = compare_of("filename=icon@2x.png");
        assert_eq!(value.needs_quotes, Some(NeedsQuotes::Character(13)));
    }

    #[test]
    fn the_printing_rules_reader_half_reads_bare_text_back_as_itself() {
        for bare in ["jpg", "AND", "照片1.jpg", "2024-06-15", "a+b"] {
            assert!(reads_as_one_bare_value(bare), "{bare}");
        }
        for quoted in ["", "a b", "a%b", "IMG*", "a)", r#""x""#, "O'Brien"] {
            assert!(!reads_as_one_bare_value(quoted), "{quoted:?}");
        }
        for bare in ["Make", "content.File", "AND", "content.XMP-dc:Subject"] {
            assert!(reads_as_one_bare_key(bare), "{bare}");
        }
        for quoted in ["archived", "NOT", "a b", r#""Make""#, "%x", "3D"] {
            assert!(!reads_as_one_bare_key(quoted), "{quoted:?}");
        }
    }

    // The tree the reader builds, by each shape a filter takes.

    #[test]
    fn a_key_in_a_filter_carries_the_key_its_name_reads_as() {
        use super::super::key::{BuiltinKey, SourceFact};

        for (spellings, key) in [
            (
                ["size?", r#""size"?"#, "'size'|bucket?"],
                Key::BuiltinKey(BuiltinKey::Size),
            ),
            (
                ["hash?", r#""hash"?"#, "'hash'|lowercase?"],
                Key::BuiltinKey(BuiltinKey::Hash),
            ),
            (
                ["source.root?", r#""source.root"?"#, "'source.root'?"],
                Key::SourceFact(SourceFact::Root),
            ),
            (
                ["Make?", r#""Make"?"#, "content.Make?"],
                Key::ContentFact("content.Make".to_string()),
            ),
        ] {
            for text in spellings {
                let Expr::Exists { key: read, .. } = runs(text) else {
                    panic!("{text}");
                };
                assert_eq!(read.key, Some(key.clone()), "{text}");
            }
        }
        let Expr::Exists { key, .. } = runs("source.extension?") else {
            panic!("a key");
        };
        assert_eq!(key.key, None);
    }

    #[test]
    fn a_doubled_quote_is_one_literal_quote_of_either_kind() {
        for (text, expected) in [
            (r#"title="say ""hi""""#, r#"say "hi""#),
            (r#"title='it''s'"#, "it's"),
            (r#"title="it's""#, "it's"),
            (r#"title='say "hi"'"#, r#"say "hi""#),
            (r#"title="""""#, r#"""#),
            (r#"title=''''"#, "'"),
        ] {
            let (_, _, value, _) = compare_of(text);
            assert_eq!(
                (value.text.as_str(), value.quoted),
                (expected, true),
                "{text}"
            );
        }
    }

    #[test]
    fn a_backslash_is_an_ordinary_character() {
        assert_eq!(compare_of(r#"path="C:\photos\""#).2.text, r"C:\photos\");
        assert_eq!(compare_of(r#"title="a\""#).2.text, r"a\");
    }

    #[test]
    fn a_value_says_whether_it_was_quoted_and_the_empty_text_is_writable() {
        for (text, value_text, quoted) in [
            ("size=10", "10", false),
            (r#"size="10""#, "10", true),
            (r#"title="""#, "", true),
            ("title=''", "", true),
        ] {
            let (_, _, value, _) = compare_of(text);
            assert_eq!(
                (value.text.as_str(), value.quoted),
                (value_text, quoted),
                "{text}"
            );
        }
        let (_, _, value, _) = compare_of(r#"size="10""#);
        assert_eq!(value.input, r#""10""#);
    }

    #[test]
    fn a_quoted_key_keeps_every_character_and_takes_accessor_and_modifiers_after_it() {
        let (key, op, value, _) = compare_of(r#""content.File Size">1000"#);
        assert_eq!(key.base, "content.File Size");
        assert_eq!(key.input, r#""content.File Size""#);
        assert_eq!(key.input_name, r#""content.File Size""#);
        assert_eq!((op, value.text.as_str()), (CompareOp::Gt, "1000"));

        // Read under `content.` as the bare name is; a source fact kept.
        assert_eq!(compare_of(r#""File Size"=1"#).0.base, "content.File Size");
        assert_eq!(compare_of("'source.mtime'=2024").0.base, "source.mtime");
        assert_eq!(
            compare_of(r#""Make"=Canon"#).0.base,
            compare_of("Make=Canon").0.base
        );

        // An inner quote doubled; `|` and `[` inside the quotes are part of the name.
        assert_eq!(compare_of(r#""a""b"=1"#).0.base, r#"content.a"b"#);
        let (key, _, _, _) = compare_of(r#""a|b[0]"=1"#);
        assert_eq!(key.base, "content.a|b[0]");
        assert!(key.modifiers.is_empty());

        let (key, _, _, _) = compare_of(r#""content.Source File"[-1]|stem=img"#);
        assert_eq!(key.base, "content.Source File");
        assert_eq!(key.accessor.map(|(a, _)| a), Some(PathAccessor::Index(-1)));
        assert_eq!(key.modifiers.len(), 1);
        assert_eq!(key.input_name, r#""content.Source File""#);
    }

    #[test]
    fn quoting_a_keyword_makes_it_a_key() {
        assert_eq!(compare_of(r#""AND"=1"#).0.base, "content.AND");
    }

    #[test]
    fn a_bucket_with_thresholds_is_a_key() {
        let (key, op, value, clause) = compare_of("source.size|bucket(60,300)=60-300");
        assert_eq!(key.base, "source.size");
        assert_eq!(key.modifiers.len(), 1);
        assert_eq!(key.modifiers[0].0.args, vec![60.0, 300.0]);
        assert_eq!((op, value.text.as_str()), (CompareOp::Eq, "60-300"));
        assert_eq!(clause, "source.size|bucket(60,300)=60-300");
        let (key, _, _, _) = compare_of("source.size|bucket(60, 300) = x");
        assert_eq!(key.modifiers[0].0.args, vec![60.0, 300.0]);
        assert_eq!(
            said("source.size|bucket(60,300=1"),
            at(18, "this `(` is unclosed")
        );
    }

    #[test]
    fn the_negated_operators_read_as_not_of_their_positive_form() {
        let text = "ext!=jpg";
        let Expr::Not(inner) = runs(text) else {
            panic!("{text}: NOT");
        };
        let Expr::Compare { op, clause, .. } = *inner else {
            panic!("{text}: a comparison");
        };
        assert_eq!((op, clause.as_str()), (CompareOp::Eq, text));
    }

    #[test]
    fn every_condition_keeps_its_own_text_and_place() {
        let expr = runs("NOT  taken|year = 2024 AND (Make?  OR ext IN ( jpg ,png ))");
        assert_eq!(
            conditions(&expr),
            [
                ("taken|year = 2024".to_string(), 5, 22),
                ("Make?".to_string(), 28, 33),
                ("ext IN ( jpg ,png )".to_string(), 38, 57),
            ]
        );
        let (key, _, value, _) = compare_of("taken|year=2024");
        assert_eq!(
            (key.input.as_str(), key.input_name.as_str()),
            ("taken|year", "taken")
        );
        assert_eq!((value.place.start, value.place.end), (11, 15));
        assert_eq!(conditions(&runs("archived?")), [(String::new(), 0, 9)]);
    }

    #[test]
    fn an_accessor_comes_once_straight_after_the_name() {
        assert_eq!(
            said("rel_path|stem[0]=x"),
            at(13, "an operator is missing after `rel_path|stem`")
        );
        assert_eq!(
            said("rel_path[0][1]=x"),
            at(11, "an operator is missing after `rel_path[0]`")
        );
        let (key, _, _, _) = compare_of("source.rel_path[1:3]|lowercase=x");
        assert_eq!(
            key.accessor.map(|(a, _)| a),
            Some(PathAccessor::Slice {
                start: Some(1),
                end: Some(3)
            })
        );
    }

    #[test]
    fn each_shape_of_condition_reads_into_the_tree() {
        for (text, base, op, value) in [
            (
                "mime=application/octet-stream",
                "content.mime",
                CompareOp::Eq,
                "application/octet-stream",
            ),
            (
                "tag=my-custom-tag",
                "content.tag",
                CompareOp::Eq,
                "my-custom-tag",
            ),
            (
                r#"mime="application/octet-stream""#,
                "content.mime",
                CompareOp::Eq,
                "application/octet-stream",
            ),
            ("source.size>-1", "source.size", CompareOp::Gt, "-1"),
            (
                "source.mtime>2024-01-15",
                "source.mtime",
                CompareOp::Gt,
                "2024-01-15",
            ),
            ("source.ext=jpg", "source.ext", CompareOp::Eq, "jpg"),
        ] {
            let (key, read_op, read_value, _) = compare_of(text);
            assert_eq!(
                (key.base.as_str(), read_op, read_value.text.as_str()),
                (base, op, value),
                "{text}"
            );
        }
        for (text, base, glob) in [
            ("ext~jp?g", "ext", "jp?g"),
            ("ext~*.tmp", "ext", "*.tmp"),
            ("ext~[jJ][pP][gG]", "ext", "[jJ][pP][gG]"),
        ] {
            let (key, read_glob, _) = glob_of(text);
            assert_eq!(
                (key.base.as_str(), read_glob.input.as_str()),
                (base, glob),
                "{text}"
            );
        }
        assert_eq!(
            clauses(&runs("mime=image/jpeg AND source.size>1000")),
            ["mime=image/jpeg", "source.size>1000"]
        );
        let Expr::In { values, .. } = runs("mime IN (application/octet-stream, image/jpeg)") else {
            panic!("a list");
        };
        assert_eq!(
            values.iter().map(|v| v.text.as_str()).collect::<Vec<_>>(),
            ["application/octet-stream", "image/jpeg"]
        );
        let Expr::Exists { key, .. } = runs("content.hash.sha256?") else {
            panic!("exists");
        };
        assert_eq!(key.base, "content.hash.sha256");
        let Expr::Not(inner) = runs("NOT content.hash.sha256?") else {
            panic!("NOT of exists");
        };
        assert!(matches!(*inner, Expr::Exists { .. }));
        let Expr::And(parts) = runs("(mime=image/jpeg OR mime=image/png) AND source.size>0") else {
            panic!("AND");
        };
        assert!(matches!(parts.as_slice(), [Expr::Or(or), Expr::Compare { .. }] if or.len() == 2));
        assert_eq!(
            clauses(&runs("(source.ext=jpg) AND mime=image/jpeg")),
            ["source.ext=jpg", "mime=image/jpeg"]
        );
    }

    #[test]
    fn a_second_condition_without_and_or_is_refused_not_ignored() {
        assert_eq!(
            said("source.size=1 source.size=2"),
            at(13, "a space separates values")
        );
    }

    #[test]
    fn not_in_reads_as_negated_membership() {
        let Expr::Not(inner) = runs("source.ext NOT IN (jpg, png)") else {
            panic!("NOT");
        };
        let Expr::In {
            key,
            values,
            clause,
            ..
        } = *inner
        else {
            panic!("a list");
        };
        assert_eq!(key.base, "source.ext");
        assert_eq!(
            values.iter().map(|v| v.text.as_str()).collect::<Vec<_>>(),
            ["jpg", "png"]
        );
        assert_eq!(clause, "source.ext NOT IN (jpg, png)");
    }

    #[test]
    fn not_after_a_key_without_in_is_no_operator() {
        assert_eq!(
            said("source.ext NOT = jpg"),
            at(11, "an operator is missing after `source.ext`")
        );
    }
}
