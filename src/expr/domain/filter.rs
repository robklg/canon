//! The filter language: its syntax tree, its reader, and its comparisons.
//!
//! `--where` is the asking half of the expression language — the half that
//! picks content out of the universe. A written filter is tokenized, parsed
//! into a tree of boolean and comparison nodes, and later walked against
//! sources. Everything here is pure: the tree is built and compared without
//! ever touching a database.

use anyhow::{bail, Result};

use super::key::{parse_accessor, read_base_key};
use super::transform::{parse_modifier, ModifierCall, PathAccessor};
use crate::core::domain::time::value::PeriodOp;

// ============================================================================
// Expression AST
// ============================================================================

/// The comparison operators. `!=`, `!~` and `NOT IN` have no operator of their own: each
/// parses as `NOT` of its positive form, so "everything else" means one thing everywhere.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CompareOp {
    Eq,
    Gt,
    Ge,
    Lt,
    Le,
    Glob, // ~
}

/// Status predicates — computed boolean state, not stored facts.
///
/// Adding a variant is deliberately noisy: the prefetch walks every variant
/// and matches exhaustively, so a new one cannot be evaluated before someone
/// has said what it needs loaded.
#[derive(Debug, Clone, Copy, PartialEq, strum::EnumIter)]
pub enum StatusPredicate {
    /// Content exists in at least one archive root (including suspended).
    Archived,
    /// Content hash has been computed (source has object_id).
    Hashed,
    /// Source or object is excluded.
    Excluded,
    /// Source has any stored fact, excluding content.hash.sha256.
    Enriched,
}

/// Keywords recognized as status predicates before normalization.
pub const STATUS_KEYWORDS: &[(&str, StatusPredicate)] = &[
    ("archived", StatusPredicate::Archived),
    ("hashed", StatusPredicate::Hashed),
    ("excluded", StatusPredicate::Excluded),
    ("enriched", StatusPredicate::Enriched),
];

/// A value as written in a filter: its text, and whether it was quoted. Quoting is what
/// says a value is text where the bare word could also read as a number or a time.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::expr) struct Literal {
    pub(in crate::expr) text: String,
    pub(in crate::expr) quoted: bool,
    /// The value exactly as typed, quotes included — what a way out keeps of it.
    pub(in crate::expr) written: String,
}

/// A key as written: the text typed (`written`), its name part as typed (`written_name`,
/// quotes included), and what it reads as — the base key (normalized as a bare key is, an
/// alias read as its stored key), its accessor and its modifiers. The one key reader:
/// `--where`, `--key` and `--group-by` all read a key through [`WrittenKey::read`].
#[derive(Debug, Clone, PartialEq)]
pub(in crate::expr) struct WrittenKey {
    pub(in crate::expr) written: String,
    pub(in crate::expr) written_name: String,
    pub(in crate::expr) base: String,
    pub(in crate::expr) accessor: Option<PathAccessor>,
    pub(in crate::expr) modifiers: Vec<ModifierCall>,
}

/// Filter expression AST - supports boolean logic. Each clause keeps its own text as written
/// (`clause`), so a refusal can show the broken part of the filter the user typed.
#[derive(Debug, Clone)]
pub enum Expr {
    And(Vec<Expr>),
    Or(Vec<Expr>),
    Not(Box<Expr>),
    Exists {
        key: WrittenKey,
        clause: String,
    },
    Compare {
        key: WrittenKey,
        op: CompareOp,
        value: Literal,
        clause: String,
    },
    In {
        key: WrittenKey,
        values: Vec<Literal>,
        clause: String,
    },
    Status(StatusPredicate),
    /// A clause whose key cannot be read (an unknown modifier, a misplaced accessor): parsed
    /// past, so the clauses around it are still read, and refused by binding as its own cause.
    Unread {
        clause: String,
        error: String,
    },
}

/// Result of applying filters, including metadata about which status predicates were used.
pub struct FilterResult {
    pub source_ids: Vec<i64>,
    pub used_status: UsedStatus,
    /// Whether any comparison read an instant on the clock
    /// (`domain::transform::reads_an_instant`).
    pub read_an_instant: bool,
}

/// Flags indicating which status predicates appeared in the filter expression.
#[derive(Debug, Default, Clone)]
pub struct UsedStatus {
    pub archived: bool,
    pub hashed: bool,
    pub excluded: bool,
    pub enriched: bool,
}

impl UsedStatus {
    /// Whether the expression asked for this predicate.
    ///
    /// Reading the flags through here rather than field by field is what
    /// keeps the prefetch honest: a predicate with no flag stops the build
    /// instead of reaching evaluation with nothing loaded.
    ///
    /// The flags themselves are public; this reading of them is not. The
    /// facility re-exports the type, so a bare `pub` here would put the
    /// method on the crate's surface — and what it exists to serve is one
    /// caller, inside.
    pub(in crate::expr) fn uses(&self, predicate: StatusPredicate) -> bool {
        match predicate {
            StatusPredicate::Archived => self.archived,
            StatusPredicate::Hashed => self.hashed,
            StatusPredicate::Excluded => self.excluded,
            StatusPredicate::Enriched => self.enriched,
        }
    }
}

impl Expr {
    /// Parse a filter expression string into an AST
    pub fn parse(s: &str) -> Result<Self> {
        let tokens = tokenize(s)?;
        if tokens.is_empty() {
            bail!("Empty filter expression");
        }
        let chars: Vec<char> = s.chars().collect();
        let mut parser = Parser::new(&tokens, &chars);
        let expr = parser.parse_expr()?;
        if parser.pos < tokens.len() {
            let token = &tokens[parser.pos];
            let text: String = chars[token.start..token.end].iter().collect();
            bail!("Unexpected '{text}' after a complete clause");
        }
        Ok(expr)
    }
}

// ============================================================================
// Tokenizer
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    LParen,
    RParen,
    And,
    Or,
    Not,
    In,
    Comma,
    Op(CompareOp),
    /// `!=` or `!~`: `NOT` of the positive operator.
    NegatedOp(CompareOp),
    Exists, // The '?' suffix
    /// A key, as the text it was written as (quotes, accessor and modifiers included).
    Key(String),
    Value(Literal),
}

/// A token and where it sits in the filter, in characters: `start..end`.
#[derive(Debug, Clone)]
struct Token {
    tok: Tok,
    start: usize,
    end: usize,
}

/// Check if a character is valid in a bare value (after a comparison operator).
/// Values accept a liberal character set: alphanumeric plus common value characters
/// like `-`, `/`, `?`, `*`, `.`, `_`, `:`, `|`, `[`, `]`, `+`, `@`, `#`, `%`.
fn is_value_char(c: char) -> bool {
    c.is_alphanumeric()
        || matches!(
            c,
            '_' | '.' | '-' | '/' | ':' | '*' | '?' | '[' | ']' | '|' | '+' | '@' | '#' | '%'
        )
}

/// Whether a character continues a bare key's name.
fn is_key_name_char(c: char) -> bool {
    c.is_alphanumeric() || matches!(c, '_' | '.' | ':' | '/')
}

/// Check if the token context signals that the next token should be parsed as a value.
/// This is true after comparison operators, after commas (for IN lists),
/// and after LParen that follows IN (first value in an IN list).
fn expects_value(tokens: &[Token]) -> bool {
    match tokens.last().map(|t| &t.tok) {
        Some(Tok::Op(_)) | Some(Tok::NegatedOp(_)) | Some(Tok::Comma) => true,
        // First value in IN list: IN (value, ...)
        Some(Tok::LParen) => tokens.len() >= 2 && matches!(tokens[tokens.len() - 2].tok, Tok::In),
        _ => false,
    }
}

/// Read quoted text starting at the opening quote `chars[start]`: inside, a doubled quote of
/// the same kind is one literal quote, and every other character — a backslash included — is
/// itself. Returns the text and the index just past the closing quote.
fn read_quoted(chars: &[char], start: usize) -> Result<(String, usize)> {
    let quote = chars[start];
    let mut text = String::new();
    let mut i = start + 1;
    loop {
        match chars.get(i) {
            None => bail!("Unterminated string"),
            Some(&c) if c == quote => {
                if chars.get(i + 1) == Some(&quote) {
                    text.push(quote);
                    i += 2;
                } else {
                    return Ok((text, i + 1));
                }
            }
            Some(&c) => {
                text.push(c);
                i += 1;
            }
        }
    }
}

/// Where a key that starts at `chars[start]` ends: a quoted name (either quote mark, an inner
/// quote doubled) or a bare one, then any accessor (`[…]`) and modifiers (`|name`, and after
/// `|bucket(` everything to the matching `)`).
fn key_extent(chars: &[char], start: usize) -> Result<usize> {
    let mut i = if matches!(chars.get(start), Some('"' | '\'')) {
        read_quoted(chars, start)?.1
    } else {
        let mut i = start;
        while chars.get(i).is_some_and(|&c| is_key_name_char(c)) {
            i += 1;
        }
        i
    };
    loop {
        match chars.get(i) {
            Some('[') => {
                while chars.get(i).is_some_and(|&c| c != ']') {
                    i += 1;
                }
                if i >= chars.len() {
                    bail!("Unclosed '[' in accessor");
                }
                i += 1;
            }
            Some('|') => {
                i += 1;
                while chars
                    .get(i)
                    .is_some_and(|&c| c.is_alphanumeric() || c == '_')
                {
                    i += 1;
                }
                if chars.get(i) == Some(&'(') {
                    while chars.get(i).is_some_and(|&c| c != ')') {
                        i += 1;
                    }
                    if i >= chars.len() {
                        bail!("Unclosed '(' in modifier");
                    }
                    i += 1;
                }
            }
            _ => return Ok(i),
        }
    }
}

impl WrittenKey {
    /// Read a key as written: `source.mtime|year`, `content.SourceFile[-1]`,
    /// `"content.File Size"|bucket(60,300)`. A quoted name keeps every character literal (an
    /// inner quote doubled); accessor and modifiers follow the closing quote. Quoting changes
    /// how a key is read, never which key: the name is normalized as a bare one is.
    pub(in crate::expr) fn read(text: &str) -> Result<WrittenKey> {
        let chars: Vec<char> = text.chars().collect();
        let end = key_extent(&chars, 0)?;
        if end != chars.len() || chars.is_empty() {
            bail!("Not a key: '{text}'");
        }
        let (name, name_end) = if matches!(chars.first(), Some('"' | '\'')) {
            read_quoted(&chars, 0)?
        } else {
            let mut i = 0;
            while chars.get(i).is_some_and(|&c| is_key_name_char(c)) {
                i += 1;
            }
            (chars[..i].iter().collect(), i)
        };
        if name.is_empty() {
            bail!("Not a key: '{text}'");
        }
        let written_name: String = chars[..name_end].iter().collect();
        let tail: String = chars[name_end..].iter().collect();

        let (accessor_part, modifier_part) = match tail.find('|') {
            #[expect(clippy::string_slice, reason = "a `find` position")]
            Some(at) => (&tail[..at], &tail[at..]),
            None => (tail.as_str(), ""),
        };
        let accessor = if accessor_part.is_empty() {
            None
        } else {
            match accessor_part
                .strip_prefix('[')
                .and_then(|a| a.strip_suffix(']'))
            {
                Some(inner) if !inner.contains('[') && !inner.contains(']') => {
                    Some(parse_accessor(inner)?)
                }
                _ => bail!("An accessor ('[…]') comes once, before any modifier: '{text}'"),
            }
        };

        let (base, leading_modifier) = read_base_key(&name);
        let mut modifiers = Vec::new();
        if let Some(modifier) = leading_modifier {
            modifiers.push(parse_modifier(modifier)?);
        }
        for written in modifier_part.split('|').skip(1) {
            if written.contains('[') {
                bail!("An accessor ('[…]') comes once, before any modifier: '{text}'");
            }
            modifiers.push(parse_modifier(written.trim())?);
        }
        Ok(WrittenKey {
            written: text.to_string(),
            written_name,
            base,
            accessor,
            modifiers,
        })
    }

    /// Whether the key reads its base through an accessor or a modifier.
    pub(in crate::expr) fn has_transforms(&self) -> bool {
        self.accessor.is_some() || !self.modifiers.is_empty()
    }
}

/// Whether `text`, typed bare after `=`, is read back by the tokenizer as one unquoted value
/// with that same text — the reader's half of the printing rule, asked of the reader itself.
pub(in crate::expr) fn reads_as_one_bare_value(text: &str) -> bool {
    match tokenize(&format!("k={text}")).as_deref() {
        Ok(
            [_, _, Token {
                tok: Tok::Value(value),
                ..
            }],
        ) => !value.quoted && value.text == text,
        _ => false,
    }
}

/// Whether `key`, typed bare before `?`, is read back by the tokenizer as one key token with
/// that same text.
pub(in crate::expr) fn reads_as_one_bare_key(key: &str) -> bool {
    match tokenize(&format!("{key}?")).as_deref() {
        Ok(
            [Token {
                tok: Tok::Key(written),
                ..
            }, Token {
                tok: Tok::Exists, ..
            }],
        ) => written == key && !key.starts_with(['"', '\'']),
        _ => false,
    }
}

fn tokenize(s: &str) -> Result<Vec<Token>> {
    let mut tokens: Vec<Token> = Vec::new();
    let chars: Vec<char> = s.chars().collect();
    let mut i = 0;
    let push = |tokens: &mut Vec<Token>, tok, start, end| tokens.push(Token { tok, start, end });

    while i < chars.len() {
        // Skip whitespace
        if chars[i].is_whitespace() {
            i += 1;
            continue;
        }
        let start = i;

        // Context-aware value parsing: after a comparison operator, comma,
        // or opening paren of an IN list, parse as a value with liberal characters.
        if expects_value(&tokens) {
            if chars[i] == '"' || chars[i] == '\'' {
                let (text, end) = read_quoted(&chars, i)?;
                let written = chars[start..end].iter().collect();
                push(
                    &mut tokens,
                    Tok::Value(Literal {
                        text,
                        quoted: true,
                        written,
                    }),
                    start,
                    end,
                );
                i = end;
                continue;
            }

            // LParen in value position starts an IN list — not a value
            if chars[i] == '(' {
                push(&mut tokens, Tok::LParen, start, i + 1);
                i += 1;
                continue;
            }

            // Bare value: read until whitespace or structural character
            if is_value_char(chars[i]) {
                while i < chars.len() && is_value_char(chars[i]) {
                    i += 1;
                }
                let text: String = chars[start..i].iter().collect();
                push(
                    &mut tokens,
                    Tok::Value(Literal {
                        written: text.clone(),
                        text,
                        quoted: false,
                    }),
                    start,
                    i,
                );
                continue;
            }
        }

        // Single-char tokens
        let single = match chars[i] {
            '(' => Some(Tok::LParen),
            ')' => Some(Tok::RParen),
            ',' => Some(Tok::Comma),
            '?' => Some(Tok::Exists),
            _ => None,
        };
        if let Some(tok) = single {
            push(&mut tokens, tok, start, i + 1);
            i += 1;
            continue;
        }

        // Two-char operators
        let two = match (chars[i], chars.get(i + 1)) {
            ('>', Some('=')) => Some(Tok::Op(CompareOp::Ge)),
            ('<', Some('=')) => Some(Tok::Op(CompareOp::Le)),
            ('!', Some('=')) => Some(Tok::NegatedOp(CompareOp::Eq)),
            ('!', Some('~')) => Some(Tok::NegatedOp(CompareOp::Glob)),
            _ => None,
        };
        if let Some(tok) = two {
            push(&mut tokens, tok, start, i + 2);
            i += 2;
            continue;
        }

        // Single-char operators
        let one = match chars[i] {
            '>' => Some(Tok::Op(CompareOp::Gt)),
            '<' => Some(Tok::Op(CompareOp::Lt)),
            '=' => Some(Tok::Op(CompareOp::Eq)),
            '~' => Some(Tok::Op(CompareOp::Glob)),
            '!' => Some(Tok::Not),
            _ => None,
        };
        if let Some(tok) = one {
            push(&mut tokens, tok, start, i + 1);
            i += 1;
            continue;
        }

        // A key: a bare name, or a quoted one, with any accessor and modifiers; a bare word
        // with none of them may be a keyword instead.
        if chars[i].is_alphabetic() || chars[i] == '_' || chars[i] == '"' || chars[i] == '\'' {
            let end = key_extent(&chars, i)?;
            let word: String = chars[start..end].iter().collect();
            i = end;
            let keyword = if chars[start] == '"' || chars[start] == '\'' {
                None
            } else {
                match word.to_uppercase().as_str() {
                    "AND" => Some(Tok::And),
                    "OR" => Some(Tok::Or),
                    "NOT" => Some(Tok::Not),
                    "IN" => Some(Tok::In),
                    _ => None,
                }
            };
            push(&mut tokens, keyword.unwrap_or(Tok::Key(word)), start, end);
            continue;
        }

        // Numbers (including negative, decimals, and date formats like 2024-01-15)
        if chars[i].is_ascii_digit()
            || (chars[i] == '-' && i + 1 < chars.len() && chars[i + 1].is_ascii_digit())
        {
            if chars[i] == '-' {
                i += 1;
            }
            while i < chars.len()
                && (chars[i].is_ascii_digit()
                    || chars[i] == '.'
                    || chars[i] == '-'
                    || chars[i] == ':'
                    || chars[i] == 'T')
            {
                i += 1;
            }
            let text: String = chars[start..i].iter().collect();
            push(
                &mut tokens,
                Tok::Value(Literal {
                    written: text.clone(),
                    text,
                    quoted: false,
                }),
                start,
                i,
            );
            continue;
        }

        // An `@name` where a key belongs was never expanded: it comes from a manifest's recorded
        // query, which holds the query an alias stood for, or from inside an alias's own value,
        // which expansion's one pass leaves as it is. Inside a value an `@` is text
        // (`is_value_char`), as it is after expansion.
        if chars[i] == '@' {
            if let Some(name) = super::alias::alias_name_at(&chars, i + 1) {
                bail!(
                    "'@{name}' is an alias, and aliases are not expanded here; write the filter \
                     it stands for"
                );
            }
        }
        bail!("Unexpected character: {}", chars[i]);
    }

    Ok(tokens)
}

// ============================================================================
// Parser
// ============================================================================

struct Parser<'a> {
    tokens: &'a [Token],
    chars: &'a [char],
    pos: usize,
}

/// A fixed token as it is written in a filter, for a message naming what was expected.
fn written_form(tok: &Tok) -> &'static str {
    match tok {
        Tok::LParen => "'('",
        Tok::RParen => "')'",
        Tok::Comma => "','",
        Tok::And => "'AND'",
        Tok::Or => "'OR'",
        Tok::Not => "'NOT'",
        Tok::In => "'IN'",
        Tok::Exists => "'?'",
        Tok::Op(_) | Tok::NegatedOp(_) => "an operator",
        Tok::Key(_) => "a key",
        Tok::Value(_) => "a value",
    }
}

impl<'a> Parser<'a> {
    fn new(tokens: &'a [Token], chars: &'a [char]) -> Self {
        Parser {
            tokens,
            chars,
            pos: 0,
        }
    }

    fn peek(&self) -> Option<&Tok> {
        self.tokens.get(self.pos).map(|t| &t.tok)
    }

    fn advance(&mut self) -> Option<&Tok> {
        let tok = self.tokens.get(self.pos).map(|t| &t.tok);
        if tok.is_some() {
            self.pos += 1;
        }
        tok
    }

    fn expect(&mut self, expected: &Tok) -> Result<()> {
        match self.advance() {
            Some(t) if t == expected => Ok(()),
            Some(_) => bail!(
                "Expected {}, got '{}'",
                written_form(expected),
                self.just_read()
            ),
            None => bail!("Expected {}, got end of input", written_form(expected)),
        }
    }

    /// The token just consumed, as it was written in the filter.
    fn just_read(&self) -> String {
        let token = &self.tokens[self.pos - 1];
        self.chars[token.start..token.end].iter().collect()
    }

    /// The filter's own text from token `first` through the last token consumed.
    fn clause_from(&self, first: usize) -> String {
        let start = self.tokens[first].start;
        let end = self.tokens[self.pos - 1].end;
        self.chars[start..end].iter().collect()
    }

    /// expr := or_expr
    fn parse_expr(&mut self) -> Result<Expr> {
        self.parse_or_expr()
    }

    /// or_expr := and_expr ('OR' and_expr)*
    fn parse_or_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_and_expr()?;

        while matches!(self.peek(), Some(Tok::Or)) {
            self.advance(); // consume OR
            let right = self.parse_and_expr()?;
            left = match left {
                Expr::Or(mut v) => {
                    v.push(right);
                    Expr::Or(v)
                }
                _ => Expr::Or(vec![left, right]),
            };
        }

        Ok(left)
    }

    /// and_expr := unary_expr ('AND' unary_expr)*
    fn parse_and_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_unary_expr()?;

        while matches!(self.peek(), Some(Tok::And)) {
            self.advance(); // consume AND
            let right = self.parse_unary_expr()?;
            left = match left {
                Expr::And(mut v) => {
                    v.push(right);
                    Expr::And(v)
                }
                _ => Expr::And(vec![left, right]),
            };
        }

        Ok(left)
    }

    /// unary_expr := 'NOT' unary_expr | primary
    fn parse_unary_expr(&mut self) -> Result<Expr> {
        if matches!(self.peek(), Some(Tok::Not)) {
            self.advance(); // consume NOT
            let expr = self.parse_unary_expr()?;
            return Ok(Expr::Not(Box::new(expr)));
        }
        self.parse_primary()
    }

    /// primary := '(' expr ')' | atom
    fn parse_primary(&mut self) -> Result<Expr> {
        if matches!(self.peek(), Some(Tok::LParen)) {
            self.advance(); // consume '('
            let expr = self.parse_expr()?;
            self.expect(&Tok::RParen)?;
            return Ok(expr);
        }
        self.parse_atom()
    }

    /// atom := status_keyword '?' | key '?' | key ['NOT'] 'IN' '(' value_list ')' | key op value
    fn parse_atom(&mut self) -> Result<Expr> {
        let first = self.pos;
        let written = match self.advance() {
            Some(Tok::Key(k)) => k.clone(),
            Some(_) => bail!("Expected a key, got '{}'", self.just_read()),
            None => bail!("Expected a key, got end of input"),
        };
        let mut key = WrittenKey::read(&written);

        // A status predicate is named by its bare word; quoting a name never changes which.
        // Asked anything but `?`, it is that clause's cause, read past like any key that cannot
        // be read.
        let status = key
            .as_ref()
            .ok()
            .filter(|k| !k.has_transforms())
            .and_then(|k| {
                let name = k.written_name.trim_matches(|c| c == '"' || c == '\'');
                STATUS_KEYWORDS
                    .iter()
                    .find(|(kw, _)| *kw == name)
                    .map(|(kw, predicate)| (*kw, *predicate))
            });
        if let Some((name, predicate)) = status {
            if matches!(self.peek(), Some(Tok::Exists)) {
                self.advance();
                if !self.a_value_follows() {
                    return Ok(Expr::Status(predicate));
                }
                key = Err(anyhow::anyhow!(
                    "'{name}?' is complete on its own; a status word takes no value"
                ));
            } else {
                key = Err(anyhow::anyhow!(
                    "'{name}' is a status predicate and only supports the '?' operator"
                ));
            }
        }
        // A key that cannot be read leaves its clause unread, never the filter: the clause is
        // parsed through to its end, and binding reports it beside every other cause.
        let read = |key: Result<WrittenKey>,
                    clause: String,
                    build: &dyn Fn(WrittenKey, String) -> Expr| {
            match key {
                Ok(key) => build(key, clause),
                Err(error) => Expr::Unread {
                    clause,
                    error: error.to_string(),
                },
            }
        };

        // Check for existence test: key?
        // `key?` followed by a value is one clause asked two ways: the clause is read through
        // to its end and reported, never left to stop the parse at the operator.
        if matches!(self.peek(), Some(Tok::Exists)) {
            self.advance();
            if !self.a_value_follows() {
                let clause = self.clause_from(first);
                return Ok(read(key, clause, &|key, clause| Expr::Exists {
                    key,
                    clause,
                }));
            }
            if key.is_ok() {
                key = Err(anyhow::anyhow!(
                    "'{written}?' is complete on its own; '?' takes no value"
                ));
            }
        }
        // A bare value straight after `?` (`archived? 1`, `source.ext? jpg`) has no operator to
        // read past: the value ends the clause, and the clause carries the cause set above.
        if self.a_bare_value_follows() && matches!(self.tokens[self.pos - 1].tok, Tok::Exists) {
            self.advance();
            let clause = self.clause_from(first);
            return Ok(read(key, clause, &|key, clause| Expr::Exists {
                key,
                clause,
            }));
        }

        // Check for IN: key IN (v1, v2, ...)
        if matches!(self.peek(), Some(Tok::In)) {
            self.advance(); // consume IN
            self.expect(&Tok::LParen)?;
            let values = self.parse_value_list()?;
            self.expect(&Tok::RParen)?;
            let clause = self.clause_from(first);
            return Ok(read(key, clause, &|key, clause| Expr::In {
                key,
                values: values.clone(),
                clause,
            }));
        }

        // Check for NOT IN: key NOT IN (v1, v2, ...) — NOT of the IN.
        if matches!(self.peek(), Some(Tok::Not)) {
            let saved_pos = self.pos;
            self.advance(); // consume NOT
            if matches!(self.peek(), Some(Tok::In)) {
                self.advance(); // consume IN
                self.expect(&Tok::LParen)?;
                let values = self.parse_value_list()?;
                self.expect(&Tok::RParen)?;
                let clause = self.clause_from(first);
                return Ok(read(key, clause, &|key, clause| {
                    Expr::Not(Box::new(Expr::In {
                        key,
                        values: values.clone(),
                        clause,
                    }))
                }));
            }
            // Not followed by IN, restore position
            self.pos = saved_pos;
        }

        // Comparison: key op value; `!=` and `!~` are NOT of the positive comparison.
        let (op, negated) = match self.advance() {
            Some(Tok::Op(op)) => (*op, false),
            Some(Tok::NegatedOp(op)) => (*op, true),
            Some(_) => bail!(
                "Expected an operator after '{written}', got '{}'",
                self.just_read()
            ),
            None => bail!("Expected an operator after '{written}', got end of input"),
        };

        let value = self.parse_value()?;
        let clause = self.clause_from(first);
        Ok(read(key, clause, &|key, clause| {
            let compare = Expr::Compare {
                key,
                op,
                value: value.clone(),
                clause,
            };
            if negated {
                Expr::Not(Box::new(compare))
            } else {
                compare
            }
        }))
    }

    /// Whether a comparison follows where the clause should already be complete: an operator,
    /// `IN`, `NOT IN`, or a bare value.
    fn a_value_follows(&self) -> bool {
        match self.peek() {
            Some(Tok::Op(_) | Tok::NegatedOp(_) | Tok::In) => true,
            Some(Tok::Not) => {
                matches!(self.tokens.get(self.pos + 1).map(|t| &t.tok), Some(Tok::In))
            }
            _ => self.a_bare_value_follows(),
        }
    }

    /// Whether one bare value follows and ends the clause: number- or date-shaped text, or a word
    /// or quoted text (read as a key outside value position) with nothing after it but the end, a
    /// connective or a closing parenthesis. A word followed by more (`archived? hashed?`,
    /// `archived? source.ext = jpg`) is a second clause with its connective missing.
    fn a_bare_value_follows(&self) -> bool {
        match self.peek() {
            Some(Tok::Value(_)) => true,
            Some(Tok::Key(_)) => matches!(
                self.tokens.get(self.pos + 1).map(|t| &t.tok),
                None | Some(Tok::And | Tok::Or | Tok::RParen)
            ),
            _ => false,
        }
    }

    fn parse_value(&mut self) -> Result<Literal> {
        match self.advance() {
            Some(Tok::Value(v)) => Ok(v.clone()),
            Some(_) => bail!("Expected a value, got '{}'", self.just_read()),
            None => bail!("Expected a value, got end of input"),
        }
    }

    fn parse_value_list(&mut self) -> Result<Vec<Literal>> {
        let mut values = vec![self.parse_value()?];
        while matches!(self.peek(), Some(Tok::Comma)) {
            self.advance(); // consume comma
            values.push(self.parse_value()?);
        }
        Ok(values)
    }
}

// ============================================================================
// Value Comparison
// ============================================================================

/// Compare text by `op`: equality as written, ordering in character order (by Unicode code
/// point, case-sensitive), `~` a glob.
pub(in crate::expr) fn compare_text(stored: &str, op: CompareOp, value: &str) -> bool {
    match op {
        CompareOp::Eq => stored == value,
        CompareOp::Gt => stored > value,
        CompareOp::Ge => stored >= value,
        CompareOp::Lt => stored < value,
        CompareOp::Le => stored <= value,
        CompareOp::Glob => glob_match(stored, value),
    }
}

/// Compare numbers by `op`; equality keeps a tolerance for numbers that differ only by
/// rounding. A glob never reaches here: binding refuses it against a number.
pub(in crate::expr) fn compare_numbers(stored: f64, op: CompareOp, value: f64) -> bool {
    match op {
        CompareOp::Eq => (stored - value).abs() < f64::EPSILON,
        CompareOp::Gt => stored > value,
        CompareOp::Ge => stored >= value,
        CompareOp::Lt => stored < value,
        CompareOp::Le => stored <= value,
        CompareOp::Glob => false,
    }
}

/// The row of the whole-period table an operator reads; `None` for a glob, which binding
/// refuses against a time.
pub(in crate::expr) fn period_op(op: CompareOp) -> Option<PeriodOp> {
    match op {
        CompareOp::Eq => Some(PeriodOp::Inside),
        CompareOp::Ge => Some(PeriodOp::StartsAtOrAfterStart),
        CompareOp::Gt => Some(PeriodOp::StartsAtOrAfterEnd),
        CompareOp::Le => Some(PeriodOp::EndsByEnd),
        CompareOp::Lt => Some(PeriodOp::EndsByStart),
        CompareOp::Glob => None,
    }
}

/// Match a string against a glob pattern (case-sensitive).
///
/// Supports:
/// - `*` matches zero or more characters
/// - `?` matches exactly one character
/// - `[abc]` matches any character in the set
/// - `[!abc]` or `[^abc]` matches any character NOT in the set
/// - `[a-z]` matches character ranges
/// - `\*`, `\?`, `\[` for literal matching
pub(in crate::expr) fn glob_match(text: &str, pattern: &str) -> bool {
    let text_chars: Vec<char> = text.chars().collect();
    let pattern_chars: Vec<char> = pattern.chars().collect();
    glob_match_recursive(&text_chars, 0, &pattern_chars, 0)
}

fn glob_match_recursive(text: &[char], ti: usize, pattern: &[char], pi: usize) -> bool {
    // Both exhausted - match
    if pi >= pattern.len() && ti >= text.len() {
        return true;
    }

    // Pattern exhausted but text remaining - no match
    if pi >= pattern.len() {
        return false;
    }

    // Handle escape sequences
    if pattern[pi] == '\\' && pi + 1 < pattern.len() {
        if ti < text.len() && text[ti] == pattern[pi + 1] {
            return glob_match_recursive(text, ti + 1, pattern, pi + 2);
        }
        return false;
    }

    // Handle wildcards
    match pattern[pi] {
        '*' => {
            // Try matching zero or more characters
            // First try matching zero characters (skip the *)
            if glob_match_recursive(text, ti, pattern, pi + 1) {
                return true;
            }
            // Then try matching one character and continue with same *
            if ti < text.len() && glob_match_recursive(text, ti + 1, pattern, pi) {
                return true;
            }
            false
        }
        '?' => {
            // Match exactly one character
            if ti < text.len() {
                glob_match_recursive(text, ti + 1, pattern, pi + 1)
            } else {
                false
            }
        }
        '[' => {
            // Character class
            if ti >= text.len() {
                return false;
            }
            match parse_char_class(pattern, pi) {
                Some((matches_set, end_pi)) => {
                    let c = text[ti];
                    let in_set = matches_set.contains(&c);
                    // Check if it's a negated class
                    let negated = pi + 1 < pattern.len()
                        && (pattern[pi + 1] == '!' || pattern[pi + 1] == '^');
                    let matches = if negated { !in_set } else { in_set };
                    if matches {
                        glob_match_recursive(text, ti + 1, pattern, end_pi)
                    } else {
                        false
                    }
                }
                None => {
                    // Invalid char class, treat '[' as literal
                    if ti < text.len() && text[ti] == '[' {
                        glob_match_recursive(text, ti + 1, pattern, pi + 1)
                    } else {
                        false
                    }
                }
            }
        }
        c => {
            // Literal character
            if ti < text.len() && text[ti] == c {
                glob_match_recursive(text, ti + 1, pattern, pi + 1)
            } else {
                false
            }
        }
    }
}

/// Parse a character class like [abc], [!abc], [a-z], [!a-z0-9]
/// Returns the set of matching characters and the index after the closing ']'
fn parse_char_class(pattern: &[char], start: usize) -> Option<(Vec<char>, usize)> {
    if pattern[start] != '[' {
        return None;
    }

    let mut i = start + 1;
    let mut chars = Vec::new();

    // Check for negation
    let negated = i < pattern.len() && (pattern[i] == '!' || pattern[i] == '^');
    if negated {
        i += 1;
    }

    // Handle ] as first char (literal)
    if i < pattern.len() && pattern[i] == ']' {
        chars.push(']');
        i += 1;
    }

    while i < pattern.len() && pattern[i] != ']' {
        // Check for range like a-z
        if i + 2 < pattern.len() && pattern[i + 1] == '-' && pattern[i + 2] != ']' {
            let start_c = pattern[i];
            let end_c = pattern[i + 2];
            for c in start_c..=end_c {
                chars.push(c);
            }
            i += 3;
        } else {
            chars.push(pattern[i]);
            i += 1;
        }
    }

    if i >= pattern.len() {
        // No closing ']'
        return None;
    }

    Some((chars, i + 1)) // +1 to skip the closing ']'
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    // ========================================================================
    // The grammar: quoting, quoted keys, negation, clause text
    // ========================================================================

    fn compare_of(text: &str) -> (WrittenKey, CompareOp, Literal, String) {
        match Expr::parse(text).unwrap() {
            Expr::Compare {
                key,
                op,
                value,
                clause,
            } => (key, op, value, clause),
            other => panic!("{text}: expected a comparison, got {other:?}"),
        }
    }

    #[test]
    fn a_doubled_quote_is_one_literal_quote_of_either_kind() {
        let cases = [
            (r#"title="say ""hi""""#, r#"say "hi""#),
            (r#"title='it''s'"#, "it's"),
            (r#"title="it's""#, "it's"),
            (r#"title='say "hi"'"#, r#"say "hi""#),
            (r#"title="""""#, r#"""#),
            (r#"title=''''"#, "'"),
        ];
        for (text, expected) in cases {
            let (_, _, value, _) = compare_of(text);
            assert_eq!(value.text, expected, "{text}");
            assert!(value.quoted, "{text}");
        }
    }

    #[test]
    fn a_backslash_is_an_ordinary_character() {
        let (_, _, value, _) = compare_of(r#"path="C:\photos\""#);
        assert_eq!(value.text, r"C:\photos\");
        let (_, _, value, _) = compare_of(r#"title="a\""#);
        assert_eq!(value.text, r"a\");
    }

    #[test]
    fn a_value_says_whether_it_was_quoted_and_the_empty_text_is_writable() {
        let (_, _, value, _) = compare_of("size=10");
        assert_eq!((value.text.as_str(), value.quoted), ("10", false));
        let (_, _, value, _) = compare_of(r#"size="10""#);
        assert_eq!((value.text.as_str(), value.quoted), ("10", true));
        let (_, _, value, _) = compare_of(r#"title="""#);
        assert_eq!((value.text.as_str(), value.quoted), ("", true));
        let (_, _, value, _) = compare_of("title=''");
        assert_eq!((value.text.as_str(), value.quoted), ("", true));
    }

    #[test]
    fn an_unterminated_quote_is_a_parse_error() {
        for text in [
            r#"title="abc"#,
            "title='abc",
            r#""content.File Size>10"#,
            r#"title="a""b"#,
        ] {
            let err = Expr::parse(text).unwrap_err();
            assert!(err.to_string().contains("Unterminated"), "{text}: {err}");
        }
    }

    #[test]
    fn a_quoted_key_keeps_every_character_and_takes_accessor_and_modifiers_after_it() {
        let (key, op, value, _) = compare_of(r#""content.File Size">1000"#);
        assert_eq!(key.base, "content.File Size");
        assert_eq!(key.written, r#""content.File Size""#);
        assert_eq!(key.written_name, r#""content.File Size""#);
        assert_eq!((op, value.text.as_str()), (CompareOp::Gt, "1000"));

        // Normalized as the bare key is: `content.` added, a built-in kept.
        assert_eq!(compare_of(r#""File Size"=1"#).0.base, "content.File Size");
        assert_eq!(compare_of("'source.mtime'=2024").0.base, "source.mtime");
        assert_eq!(
            compare_of(r#""Make"=Canon"#).0.base,
            compare_of("Make=Canon").0.base
        );

        // An inner quote doubled; `|` and `[` inside the quotes are part of the name.
        assert_eq!(compare_of(r#""a""b"=1"#).0.base, r#"content.a"b"#);
        assert_eq!(compare_of(r#""a|b[0]"=1"#).0.base, "content.a|b[0]");
        assert!(compare_of(r#""a|b[0]"=1"#).0.modifiers.is_empty());

        let (key, _, _, _) = compare_of(r#""content.Source File"[-1]|stem=img"#);
        assert_eq!(key.base, "content.Source File");
        assert_eq!(key.accessor, Some(PathAccessor::Index(-1)));
        assert_eq!(key.modifiers.len(), 1);
        assert_eq!(key.written_name, r#""content.Source File""#);
    }

    #[test]
    fn quoting_a_keyword_makes_it_a_key() {
        let (key, _, _, _) = compare_of(r#""AND"=1"#);
        assert_eq!(key.base, "content.AND");
    }

    #[test]
    fn a_bucket_with_thresholds_is_a_key() {
        let (key, op, value, clause) = compare_of("source.size|bucket(60,300)=60-300");
        assert_eq!(key.base, "source.size");
        assert_eq!(key.modifiers.len(), 1);
        assert_eq!(key.modifiers[0].args, vec![60.0, 300.0]);
        assert_eq!((op, value.text.as_str()), (CompareOp::Eq, "60-300"));
        assert_eq!(clause, "source.size|bucket(60,300)=60-300");
        let (key, _, _, _) = compare_of("source.size|bucket(60, 300) = x");
        assert_eq!(key.modifiers[0].args, vec![60.0, 300.0]);
        assert!(Expr::parse("source.size|bucket(60,300=1").is_err());
    }

    #[test]
    fn the_negated_operators_parse_as_not_of_their_positive_form() {
        let cases = [("ext!=jpg", CompareOp::Eq), ("ext!~*.jpg", CompareOp::Glob)];
        for (text, positive) in cases {
            match Expr::parse(text).unwrap() {
                Expr::Not(inner) => match *inner {
                    Expr::Compare { op, clause, .. } => {
                        assert_eq!(op, positive, "{text}");
                        assert_eq!(clause, text);
                    }
                    other => panic!("{text}: {other:?}"),
                },
                other => panic!("{text}: expected NOT, got {other:?}"),
            }
        }
    }

    #[test]
    fn every_clause_keeps_its_own_text_as_written() {
        fn walk(e: &Expr, out: &mut Vec<String>) {
            match e {
                Expr::And(v) | Expr::Or(v) => v.iter().for_each(|e| walk(e, out)),
                Expr::Not(e) => walk(e, out),
                Expr::Exists { clause, .. }
                | Expr::Compare { clause, .. }
                | Expr::In { clause, .. } => out.push(clause.clone()),
                Expr::Status(_) | Expr::Unread { .. } => {}
            }
        }
        let expr =
            Expr::parse("NOT  taken|year = 2024 AND (Make?  OR ext IN ( jpg ,png ))").unwrap();
        let mut clauses = Vec::new();
        walk(&expr, &mut clauses);
        assert_eq!(
            clauses,
            vec!["taken|year = 2024", "Make?", "ext IN ( jpg ,png )"]
        );
        let (key, _, _, _) = compare_of("taken|year=2024");
        assert_eq!(key.written, "taken|year");
        assert_eq!(key.written_name, "taken");
    }

    #[test]
    fn an_accessor_comes_once_before_the_modifiers() {
        for text in ["rel_path|stem[0]=x", "rel_path[0][1]=x"] {
            match Expr::parse(text).unwrap() {
                Expr::Unread { clause, error } => {
                    assert_eq!(clause, text);
                    assert!(error.contains("comes once"), "{error}");
                }
                other => panic!("{text}: {other:?}"),
            }
        }
        let (key, _, _, _) = compare_of("source.rel_path[1:3]|lowercase=x");
        assert_eq!(
            key.accessor,
            Some(PathAccessor::Slice {
                start: Some(1),
                end: Some(3)
            })
        );
    }

    #[test]
    fn the_key_reader_reads_a_key_alone() {
        let key = WrittenKey::read(r#""content.File Size"|bucket(60,300)"#).unwrap();
        assert_eq!(key.base, "content.File Size");
        assert_eq!(key.modifiers[0].args, vec![60.0, 300.0]);
        for not_one_key in ["", "a b", "a=1", r#""a"b"#, "a|"] {
            assert!(
                WrittenKey::read(not_one_key).is_err(),
                "{not_one_key:?} is not one key"
            );
        }
    }

    // ========================================================================
    // Tokenizer: context-aware value parsing
    // ========================================================================

    #[test]
    fn tokenize_mime_type_with_slash_and_hyphen() {
        let expr = Expr::parse("mime=application/octet-stream").unwrap();
        match expr {
            Expr::Compare { key, op, value, .. } => {
                assert_eq!(key.base, "content.mime"); // normalized by parser
                assert_eq!(op, CompareOp::Eq);
                assert_eq!(value.text, "application/octet-stream");
            }
            _ => panic!("Expected Compare"),
        }
    }

    #[test]
    fn tokenize_glob_with_question_mark() {
        let expr = Expr::parse("ext~jp?g").unwrap();
        match expr {
            Expr::Compare { key, op, value, .. } => {
                assert_eq!(key.base, "ext");
                assert_eq!(op, CompareOp::Glob);
                assert_eq!(value.text, "jp?g");
            }
            _ => panic!("Expected Compare"),
        }
    }

    #[test]
    fn tokenize_glob_with_star() {
        let expr = Expr::parse("ext~*.tmp").unwrap();
        match expr {
            Expr::Compare { op, value, .. } => {
                assert_eq!(op, CompareOp::Glob);
                assert_eq!(value.text, "*.tmp");
            }
            _ => panic!("Expected Compare"),
        }
    }

    #[test]
    fn tokenize_not_glob_with_metacharacters() {
        let expr = Expr::parse("ext!~*.bak").unwrap();
        match expr {
            Expr::Not(inner) => match *inner {
                Expr::Compare { op, value, .. } => {
                    assert_eq!(op, CompareOp::Glob);
                    assert_eq!(value.text, "*.bak");
                }
                _ => panic!("Expected NOT of a glob"),
            },
            _ => panic!("Expected Compare"),
        }
    }

    #[test]
    fn tokenize_hyphenated_value() {
        let expr = Expr::parse("tag=my-custom-tag").unwrap();
        match expr {
            Expr::Compare { op, value, .. } => {
                assert_eq!(op, CompareOp::Eq);
                assert_eq!(value.text, "my-custom-tag");
            }
            _ => panic!("Expected Compare"),
        }
    }

    #[test]
    fn tokenize_value_with_boolean_after() {
        let expr = Expr::parse("mime=image/jpeg AND source.size>1000").unwrap();
        match expr {
            Expr::And(parts) => {
                assert_eq!(parts.len(), 2);
                match &parts[0] {
                    Expr::Compare { value, .. } => assert_eq!(value.text, "image/jpeg"),
                    _ => panic!("Expected Compare"),
                }
            }
            _ => panic!("Expected And"),
        }
    }

    #[test]
    fn tokenize_in_list_with_unquoted_values() {
        let expr = Expr::parse("mime IN (application/octet-stream, image/jpeg)").unwrap();
        match expr {
            Expr::In { values, .. } => {
                assert_eq!(
                    values.iter().map(|v| v.text.as_str()).collect::<Vec<_>>(),
                    vec!["application/octet-stream", "image/jpeg"]
                );
            }
            _ => panic!("Expected In"),
        }
    }

    #[test]
    fn tokenize_exists_still_works() {
        let expr = Expr::parse("content.hash.sha256?").unwrap();
        match expr {
            Expr::Exists { key, .. } => {
                assert_eq!(key.base, "content.hash.sha256");
            }
            _ => panic!("Expected Exists"),
        }
    }

    #[test]
    fn tokenize_quoted_value_still_works() {
        let expr = Expr::parse("mime=\"application/octet-stream\"").unwrap();
        match expr {
            Expr::Compare { op, value, .. } => {
                assert_eq!(op, CompareOp::Eq);
                assert_eq!(value.text, "application/octet-stream");
            }
            _ => panic!("Expected Compare"),
        }
    }

    #[test]
    fn tokenize_parenthesized_expression_with_values() {
        let expr = Expr::parse("(mime=image/jpeg OR mime=image/png) AND source.size>0").unwrap();
        match expr {
            Expr::And(parts) => {
                assert_eq!(parts.len(), 2);
                match &parts[0] {
                    Expr::Or(or_parts) => assert_eq!(or_parts.len(), 2),
                    _ => panic!("Expected Or"),
                }
            }
            _ => panic!("Expected And"),
        }
    }

    #[test]
    fn tokenize_glob_with_bracket_range() {
        let expr = Expr::parse("ext~[jJ][pP][gG]").unwrap();
        match expr {
            Expr::Compare { op, value, .. } => {
                assert_eq!(op, CompareOp::Glob);
                assert_eq!(value.text, "[jJ][pP][gG]");
            }
            _ => panic!("Expected Compare"),
        }
    }

    #[test]
    fn tokenize_negative_number_value() {
        let expr = Expr::parse("source.size>-1").unwrap();
        match expr {
            Expr::Compare { op, value, .. } => {
                assert_eq!(op, CompareOp::Gt);
                assert_eq!(value.text, "-1");
            }
            _ => panic!("Expected Compare"),
        }
    }

    #[test]
    fn tokenize_date_value_with_hyphens() {
        let expr = Expr::parse("source.mtime>2024-01-15").unwrap();
        match expr {
            Expr::Compare { op, value, .. } => {
                assert_eq!(op, CompareOp::Gt);
                assert_eq!(value.text, "2024-01-15");
            }
            _ => panic!("Expected Compare"),
        }
    }

    #[test]
    fn tokenize_not_before_exists_no_value_mode() {
        let expr = Expr::parse("NOT content.hash.sha256?").unwrap();
        match expr {
            Expr::Not(inner) => match *inner {
                Expr::Exists { key, .. } => assert_eq!(key.base, "content.hash.sha256"),
                _ => panic!("Expected Exists inside Not"),
            },
            _ => panic!("Expected Not"),
        }
    }

    #[test]
    fn tokenize_simple_value() {
        let expr = Expr::parse("source.ext=jpg").unwrap();
        match expr {
            Expr::Compare { op, value, .. } => {
                assert_eq!(op, CompareOp::Eq);
                assert_eq!(value.text, "jpg");
            }
            _ => panic!("Expected Compare"),
        }
    }

    #[test]
    fn tokenize_value_inside_parens() {
        let expr = Expr::parse("(source.ext=jpg) AND mime=image/jpeg").unwrap();
        match expr {
            Expr::And(parts) => {
                assert_eq!(parts.len(), 2);
                match &parts[0] {
                    Expr::Compare { value, .. } => assert_eq!(value.text, "jpg"),
                    _ => panic!("Expected Compare"),
                }
                match &parts[1] {
                    Expr::Compare { value, .. } => assert_eq!(value.text, "image/jpeg"),
                    _ => panic!("Expected Compare"),
                }
            }
            _ => panic!("Expected And"),
        }
    }

    // ========================================================================
    // Status predicate parsing
    // ========================================================================

    #[test]
    fn parse_status_predicate_archived() {
        let expr = Expr::parse("archived?").unwrap();
        assert!(matches!(expr, Expr::Status(StatusPredicate::Archived)));
    }

    #[test]
    fn parse_status_predicate_hashed() {
        let expr = Expr::parse("hashed?").unwrap();
        assert!(matches!(expr, Expr::Status(StatusPredicate::Hashed)));
    }

    #[test]
    fn parse_status_predicate_excluded() {
        let expr = Expr::parse("excluded?").unwrap();
        assert!(matches!(expr, Expr::Status(StatusPredicate::Excluded)));
    }

    #[test]
    fn parse_status_predicate_enriched() {
        let expr = Expr::parse("enriched?").unwrap();
        assert!(matches!(expr, Expr::Status(StatusPredicate::Enriched)));
    }

    #[test]
    fn parse_status_predicate_in_not() {
        let expr = Expr::parse("NOT archived?").unwrap();
        match expr {
            Expr::Not(inner) => {
                assert!(matches!(*inner, Expr::Status(StatusPredicate::Archived)));
            }
            _ => panic!("Expected Not"),
        }
    }

    #[test]
    fn parse_status_predicate_composed() {
        let expr = Expr::parse("archived? AND mime~image/*").unwrap();
        match expr {
            Expr::And(parts) => {
                assert_eq!(parts.len(), 2);
                assert!(matches!(parts[0], Expr::Status(StatusPredicate::Archived)));
                assert!(matches!(parts[1], Expr::Compare { .. }));
            }
            _ => panic!("Expected And"),
        }
    }

    #[test]
    fn parse_status_predicate_error_on_compare() {
        // Its own clause's cause, read past so the clauses beside it are still read.
        let Expr::Unread { clause, error: msg } = Expr::parse("archived = true").unwrap() else {
            panic!("a status word asked with `=` is its clause's cause");
        };
        assert_eq!(clause, "archived = true");
        assert!(
            msg.contains("status predicate") && msg.contains("'?'"),
            "Error should mention status predicate and '?': {msg}"
        );
    }

    #[test]
    fn parse_status_predicate_error_on_glob() {
        let Expr::Unread { error: msg, .. } = Expr::parse("hashed ~ something").unwrap() else {
            panic!("a status word asked with `~` is its clause's cause");
        };
        assert!(
            msg.contains("status predicate"),
            "Error should mention status predicate: {msg}"
        );
    }

    #[test]
    fn parse_non_status_keyword_normalizes() {
        // "archival" is NOT a status keyword — gets content. prefix
        let expr = Expr::parse("archival?").unwrap();
        match expr {
            Expr::Exists { key, .. } => {
                assert_eq!(key.base, "content.archival");
            }
            _ => panic!("Expected Exists"),
        }
    }
    #[test]
    fn trailing_tokens_are_rejected_not_ignored() {
        // Two clauses with no operator between them is a mistake, not an
        // implicit AND. Accepting the input would filter on the first clause
        // and drop the rest without saying so — the user gets a plausible
        // answer to a question they did not ask.
        let err = Expr::parse("source.size=1 source.size=2").unwrap_err();
        assert_eq!(
            err.to_string(),
            "Unexpected 'source.size' after a complete clause"
        );
    }

    /// A grammar error names what was written, never the parser's own token names.
    #[test]
    fn a_grammar_error_quotes_what_was_written() {
        for (text, said) in [
            ("source.ext = (", "Expected a value, got '('"),
            (
                "source.ext jpg",
                "Expected an operator after 'source.ext', got 'jpg'",
            ),
            ("= jpg", "Expected a key, got '='"),
            ("source.ext IN jpg", "Expected '(', got 'jpg'"),
            (
                "archived? hashed?",
                "Unexpected 'hashed' after a complete clause",
            ),
        ] {
            let err = Expr::parse(text).unwrap_err().to_string();
            assert_eq!(err, said, "{text}");
        }
    }

    #[test]
    fn not_in_parses_as_negated_membership() {
        let expr = Expr::parse("source.ext NOT IN (jpg, png)").unwrap();
        match expr {
            Expr::Not(inner) => match *inner {
                Expr::In {
                    key,
                    values,
                    clause,
                } => {
                    assert_eq!(key.base, "source.ext");
                    let texts: Vec<&str> = values.iter().map(|v| v.text.as_str()).collect();
                    assert_eq!(texts, vec!["jpg", "png"]);
                    assert_eq!(clause, "source.ext NOT IN (jpg, png)");
                }
                other => panic!("expected a membership test inside the negation: {other:?}"),
            },
            other => panic!("expected a negation: {other:?}"),
        }
    }

    #[test]
    fn not_before_non_in_restores_parser_position() {
        // Deciding whether NOT begins a membership test is the parser's only
        // backtrack. If the position is not restored when it does not, the
        // parser resumes past the NOT and reads what follows as though the
        // NOT had never been written — turning a malformed expression into a
        // valid one that means the opposite of what it says.
        let err = Expr::parse("source.ext NOT = jpg").unwrap_err();
        assert_eq!(
            err.to_string(),
            "Expected an operator after 'source.ext', got 'NOT'"
        );
    }

    #[test]
    fn glob_matches_wildcards_and_classes() {
        assert!(glob_match("photo.jpg", "*.jpg"));
        assert!(!glob_match("photo.png", "*.jpg"));
        assert!(glob_match("anything", "*"));
        assert!(glob_match("", "*"));
        assert!(glob_match("photo.jpg", "photo.???"));
        assert!(!glob_match("photo.jpeg", "photo.???"));
        assert!(glob_match("a", "[abc]"));
        assert!(!glob_match("d", "[abc]"));
        assert!(glob_match("m", "[a-z]"));
        assert!(!glob_match("M", "[a-z]"));
    }

    #[test]
    fn glob_negated_and_literal_classes() {
        assert!(glob_match("d", "[!abc]"));
        assert!(!glob_match("a", "[!abc]"));
        assert!(glob_match("d", "[^abc]"));
        assert!(!glob_match("a", "[^abc]"));
        // A ']' first in the class is the literal bracket, not the terminator.
        assert!(glob_match("]", "[]abc]"));
        assert!(glob_match("b", "[]abc]"));
        // A class that is never closed is a literal '[', not an error.
        assert!(glob_match("[abc", "[abc"));
        assert!(!glob_match("a", "[abc"));
    }

    #[test]
    fn glob_escapes_metacharacters() {
        assert!(glob_match("*", r"\*"));
        assert!(!glob_match("x", r"\*"));
        assert!(glob_match("?", r"\?"));
        assert!(!glob_match("x", r"\?"));
        assert!(glob_match("[", r"\["));
        assert!(glob_match("a*b", r"a\*b"));
        assert!(!glob_match("axb", r"a\*b"));
    }
}
