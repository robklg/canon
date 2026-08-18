//! The filter language: its syntax tree, its reader, and its comparisons.
//!
//! `--where` is the asking half of the expression language — the half that
//! picks content out of the universe. A written filter is tokenized, parsed
//! into a tree of boolean and comparison nodes, and later walked against
//! sources. Everything here is pure: the tree is built and compared without
//! ever touching a database.

use anyhow::{bail, Result};

use super::cache::FactValue;
use super::key::normalize_key_string;

// ============================================================================
// Expression AST
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CompareOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
    Glob,    // ~
    NotGlob, // !~
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

/// Filter expression AST - supports boolean logic
#[derive(Debug, Clone)]
pub enum Expr {
    And(Vec<Expr>),
    Or(Vec<Expr>),
    Not(Box<Expr>),
    Exists {
        key: String,
    },
    Compare {
        key: String,
        op: CompareOp,
        value: String,
    },
    In {
        key: String,
        values: Vec<String>,
    },
    Status(StatusPredicate),
}

/// Result of applying filters, including metadata about which status predicates were used.
pub struct FilterResult {
    pub source_ids: Vec<i64>,
    pub used_status: UsedStatus,
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

/// A parsed `--where` expression.
///
/// Opaque on purpose. Every consumer parses a string and hands the result
/// onward; nothing outside the expression facility reads the tree inside —
/// which is what the field's visibility says in the type. Keeping the
/// syntax tree behind the newtype means the language can grow — a filter
/// that remembers the text it was written as, or the name it was saved
/// under — without touching a caller.
#[derive(Debug, Clone)]
pub struct Filter(pub(in crate::expr) Expr);

impl Filter {
    /// Parse a `--where` expression string.
    pub fn parse(s: &str) -> Result<Self> {
        Ok(Filter(Expr::parse(s)?))
    }
}

impl Expr {
    /// Parse a filter expression string into an AST
    pub fn parse(s: &str) -> Result<Self> {
        let tokens = tokenize(s)?;
        if tokens.is_empty() {
            bail!("Empty filter expression");
        }
        let mut parser = Parser::new(&tokens);
        let expr = parser.parse_expr()?;
        if parser.pos < tokens.len() {
            bail!(
                "Unexpected token after expression: {:?}",
                tokens[parser.pos]
            );
        }
        Ok(expr)
    }
}

// ============================================================================
// Tokenizer
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
enum Token {
    LParen,
    RParen,
    And,
    Or,
    Not,
    In,
    Comma,
    Op(CompareOp),
    Exists, // The '?' suffix
    Ident(String),
    Value(String),
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

/// Check if the token context signals that the next token should be parsed as a value.
/// This is true after comparison operators, after commas (for IN lists),
/// and after LParen that follows IN (first value in an IN list).
fn expects_value(tokens: &[Token]) -> bool {
    match tokens.last() {
        Some(Token::Op(_)) | Some(Token::Comma) => true,
        // First value in IN list: IN (value, ...)
        Some(Token::LParen) => tokens.len() >= 2 && matches!(tokens[tokens.len() - 2], Token::In),
        _ => false,
    }
}

fn tokenize(s: &str) -> Result<Vec<Token>> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = s.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        // Skip whitespace
        if chars[i].is_whitespace() {
            i += 1;
            continue;
        }

        // Context-aware value parsing: after a comparison operator, comma,
        // or opening paren of an IN list, parse as a value with liberal characters.
        if expects_value(&tokens) {
            // Quoted strings in value position
            if chars[i] == '"' || chars[i] == '\'' {
                let quote = chars[i];
                i += 1;
                let start = i;
                while i < chars.len() && chars[i] != quote {
                    i += 1;
                }
                if i >= chars.len() {
                    bail!("Unterminated string");
                }
                let val: String = chars[start..i].iter().collect();
                tokens.push(Token::Value(val));
                i += 1; // skip closing quote
                continue;
            }

            // LParen in value position starts an IN list — not a value
            if chars[i] == '(' {
                tokens.push(Token::LParen);
                i += 1;
                continue;
            }

            // Bare value: read until whitespace or structural character
            if is_value_char(chars[i]) {
                let start = i;
                while i < chars.len() && is_value_char(chars[i]) {
                    i += 1;
                }
                let val: String = chars[start..i].iter().collect();
                tokens.push(Token::Value(val));
                continue;
            }
        }

        // Single-char tokens
        match chars[i] {
            '(' => {
                tokens.push(Token::LParen);
                i += 1;
                continue;
            }
            ')' => {
                tokens.push(Token::RParen);
                i += 1;
                continue;
            }
            ',' => {
                tokens.push(Token::Comma);
                i += 1;
                continue;
            }
            '?' => {
                tokens.push(Token::Exists);
                i += 1;
                continue;
            }
            _ => {}
        }

        // Multi-char operators
        if i + 1 < chars.len() {
            let two: String = chars[i..i + 2].iter().collect();
            match two.as_str() {
                ">=" => {
                    tokens.push(Token::Op(CompareOp::Ge));
                    i += 2;
                    continue;
                }
                "<=" => {
                    tokens.push(Token::Op(CompareOp::Le));
                    i += 2;
                    continue;
                }
                "!=" => {
                    tokens.push(Token::Op(CompareOp::Ne));
                    i += 2;
                    continue;
                }
                "!~" => {
                    tokens.push(Token::Op(CompareOp::NotGlob));
                    i += 2;
                    continue;
                }
                _ => {}
            }
        }

        // Single-char operators
        match chars[i] {
            '>' => {
                tokens.push(Token::Op(CompareOp::Gt));
                i += 1;
                continue;
            }
            '<' => {
                tokens.push(Token::Op(CompareOp::Lt));
                i += 1;
                continue;
            }
            '=' => {
                tokens.push(Token::Op(CompareOp::Eq));
                i += 1;
                continue;
            }
            '~' => {
                tokens.push(Token::Op(CompareOp::Glob));
                i += 1;
                continue;
            }
            '!' => {
                tokens.push(Token::Not);
                i += 1;
                continue;
            }
            _ => {}
        }

        // Keywords and identifiers
        // Allow alphanumeric, underscore, dot, pipe, brackets, slash, colon
        // (for accessors like key[-1], modifiers like key|year)
        if chars[i].is_alphabetic() || chars[i] == '_' {
            let start = i;
            while i < chars.len()
                && (chars[i].is_alphanumeric()
                    || chars[i] == '_'
                    || chars[i] == '.'
                    || chars[i] == '|'
                    || chars[i] == '['
                    || chars[i] == ']'
                    || chars[i] == ':'
                    || chars[i] == '/'
                    || (chars[i] == '-' && i > 0 && chars[i - 1] == '['))
            {
                i += 1;
            }
            let word: String = chars[start..i].iter().collect();
            // Only check for keywords if word doesn't contain pipe (modifier syntax) or brackets (accessor syntax)
            if !word.contains('|') && !word.contains('[') {
                match word.to_uppercase().as_str() {
                    "AND" => {
                        tokens.push(Token::And);
                        continue;
                    }
                    "OR" => {
                        tokens.push(Token::Or);
                        continue;
                    }
                    "NOT" => {
                        tokens.push(Token::Not);
                        continue;
                    }
                    "IN" => {
                        tokens.push(Token::In);
                        continue;
                    }
                    _ => {}
                }
            }
            tokens.push(Token::Ident(word));
            continue;
        }

        // Numbers (including negative, decimals, and date formats like 2024-01-15)
        if chars[i].is_ascii_digit()
            || (chars[i] == '-' && i + 1 < chars.len() && chars[i + 1].is_ascii_digit())
        {
            let start = i;
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
            let val: String = chars[start..i].iter().collect();
            tokens.push(Token::Value(val));
            continue;
        }

        // Quoted strings (outside value position — e.g., in IN lists with explicit quoting)
        if chars[i] == '"' || chars[i] == '\'' {
            let quote = chars[i];
            i += 1;
            let start = i;
            while i < chars.len() && chars[i] != quote {
                i += 1;
            }
            if i >= chars.len() {
                bail!("Unterminated string");
            }
            let val: String = chars[start..i].iter().collect();
            tokens.push(Token::Value(val));
            i += 1; // skip closing quote
            continue;
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
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(tokens: &'a [Token]) -> Self {
        Parser { tokens, pos: 0 }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn advance(&mut self) -> Option<&Token> {
        let tok = self.tokens.get(self.pos);
        if tok.is_some() {
            self.pos += 1;
        }
        tok
    }

    fn expect(&mut self, expected: &Token) -> Result<()> {
        match self.advance() {
            Some(t) if t == expected => Ok(()),
            Some(t) => bail!("Expected {expected:?}, got {t:?}"),
            None => bail!("Expected {expected:?}, got end of input"),
        }
    }

    /// expr := or_expr
    fn parse_expr(&mut self) -> Result<Expr> {
        self.parse_or_expr()
    }

    /// or_expr := and_expr ('OR' and_expr)*
    fn parse_or_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_and_expr()?;

        while matches!(self.peek(), Some(Token::Or)) {
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

        while matches!(self.peek(), Some(Token::And)) {
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
        if matches!(self.peek(), Some(Token::Not)) {
            self.advance(); // consume NOT
            let expr = self.parse_unary_expr()?;
            return Ok(Expr::Not(Box::new(expr)));
        }
        self.parse_primary()
    }

    /// primary := '(' expr ')' | atom
    fn parse_primary(&mut self) -> Result<Expr> {
        if matches!(self.peek(), Some(Token::LParen)) {
            self.advance(); // consume '('
            let expr = self.parse_expr()?;
            self.expect(&Token::RParen)?;
            return Ok(expr);
        }
        self.parse_atom()
    }

    /// atom := status_keyword '?' | ident '?' | ident 'IN' '(' value_list ')' | ident op value
    fn parse_atom(&mut self) -> Result<Expr> {
        let raw_key = match self.advance() {
            Some(Token::Ident(k)) => k.clone(),
            Some(t) => bail!("Expected identifier, got {t:?}"),
            None => bail!("Expected identifier, got end of input"),
        };

        // Check for status predicate keywords before normalization
        if let Some((_, predicate)) = STATUS_KEYWORDS.iter().find(|(kw, _)| *kw == raw_key) {
            if matches!(self.peek(), Some(Token::Exists)) {
                self.advance();
                return Ok(Expr::Status(*predicate));
            } else {
                bail!(
                    "'{}' is a status predicate and only supports the '?' operator",
                    raw_key
                );
            }
        }

        // Normalize key (adds content. prefix for non-builtin keys)
        let key = normalize_key_string(&raw_key);

        // Check for existence test: key?
        if matches!(self.peek(), Some(Token::Exists)) {
            self.advance();
            return Ok(Expr::Exists { key });
        }

        // Check for IN: key IN (v1, v2, ...)
        if matches!(self.peek(), Some(Token::In)) {
            self.advance(); // consume IN
            self.expect(&Token::LParen)?;
            let values = self.parse_value_list()?;
            self.expect(&Token::RParen)?;
            return Ok(Expr::In { key, values });
        }

        // Check for NOT IN: key NOT IN (v1, v2, ...)
        if matches!(self.peek(), Some(Token::Not)) {
            let saved_pos = self.pos;
            self.advance(); // consume NOT
            if matches!(self.peek(), Some(Token::In)) {
                self.advance(); // consume IN
                self.expect(&Token::LParen)?;
                let values = self.parse_value_list()?;
                self.expect(&Token::RParen)?;
                return Ok(Expr::Not(Box::new(Expr::In { key, values })));
            }
            // Not followed by IN, restore position
            self.pos = saved_pos;
        }

        // Comparison: key op value
        let op = match self.advance() {
            Some(Token::Op(op)) => *op,
            Some(t) => bail!("Expected operator after '{key}', got {t:?}"),
            None => bail!("Expected operator after '{key}', got end of input"),
        };

        let value = self.parse_value()?;

        Ok(Expr::Compare { key, op, value })
    }

    fn parse_value(&mut self) -> Result<String> {
        match self.advance() {
            Some(Token::Value(v)) => Ok(v.clone()),
            Some(Token::Ident(v)) => Ok(v.clone()), // Allow unquoted values
            Some(t) => bail!("Expected value, got {t:?}"),
            None => bail!("Expected value, got end of input"),
        }
    }

    fn parse_value_list(&mut self) -> Result<Vec<String>> {
        let mut values = vec![self.parse_value()?];
        while matches!(self.peek(), Some(Token::Comma)) {
            self.advance(); // consume comma
            values.push(self.parse_value()?);
        }
        Ok(values)
    }
}

// ============================================================================
// Expression Walks
// ============================================================================

/// Extract all keys used in an expression (for prefetching).
pub fn extract_keys(expr: &Expr, keys: &mut Vec<String>) {
    match expr {
        Expr::And(exprs) | Expr::Or(exprs) => {
            for e in exprs {
                extract_keys(e, keys);
            }
        }
        Expr::Not(e) => extract_keys(e, keys),
        Expr::Exists { key } => keys.push(key.clone()),
        Expr::Compare { key, .. } => keys.push(key.clone()),
        Expr::In { key, .. } => keys.push(key.clone()),
        Expr::Status(_) => {}
    }
}

/// Extract keys used in comparison/IN expressions (not Exists).
///
/// Exists expressions are intentionally excluded — they test for presence,
/// so unknown keys are a valid "not present" result, not an error.
pub fn extract_comparable_keys(expr: &Expr, keys: &mut Vec<String>) {
    match expr {
        Expr::And(exprs) | Expr::Or(exprs) => {
            for e in exprs {
                extract_comparable_keys(e, keys);
            }
        }
        Expr::Not(e) => extract_comparable_keys(e, keys),
        Expr::Exists { .. } => {} // Existence checks don't need key validation
        Expr::Compare { key, .. } => keys.push(key.clone()),
        Expr::In { key, .. } => keys.push(key.clone()),
        Expr::Status(_) => {}
    }
}

/// Detect which status predicates appear in a filter expression tree.
pub fn detect_status_predicates(exprs: &[Filter]) -> UsedStatus {
    let mut used = UsedStatus::default();
    for expr in exprs {
        detect_status_in_expr(&expr.0, &mut used);
    }
    used
}

fn detect_status_in_expr(expr: &Expr, used: &mut UsedStatus) {
    match expr {
        Expr::And(exprs) | Expr::Or(exprs) => {
            for e in exprs {
                detect_status_in_expr(e, used);
            }
        }
        Expr::Not(e) => detect_status_in_expr(e, used),
        Expr::Status(pred) => match pred {
            StatusPredicate::Archived => used.archived = true,
            StatusPredicate::Hashed => used.hashed = true,
            StatusPredicate::Excluded => used.excluded = true,
            StatusPredicate::Enriched => used.enriched = true,
        },
        Expr::Exists { .. } | Expr::Compare { .. } | Expr::In { .. } => {}
    }
}

// ============================================================================
// Value Comparison
// ============================================================================

pub fn compare_fact_value(fact: &FactValue, op: CompareOp, filter_value: &str) -> bool {
    match fact {
        FactValue::Text(t) => compare_text(t, op, filter_value),
        FactValue::Num(n) => compare_numeric(*n, op, filter_value),
        FactValue::Time(ts) => compare_numeric(*ts as f64, op, filter_value),
    }
}

fn compare_text(stored: &str, op: CompareOp, filter_value: &str) -> bool {
    match op {
        CompareOp::Eq => stored == filter_value,
        CompareOp::Ne => stored != filter_value,
        CompareOp::Gt => stored > filter_value,
        CompareOp::Ge => stored >= filter_value,
        CompareOp::Lt => stored < filter_value,
        CompareOp::Le => stored <= filter_value,
        CompareOp::Glob => glob_match(stored, filter_value),
        CompareOp::NotGlob => !glob_match(stored, filter_value),
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
fn glob_match(text: &str, pattern: &str) -> bool {
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

fn compare_numeric(stored: f64, op: CompareOp, filter_value: &str) -> bool {
    // Glob operators don't make sense for numeric values
    if matches!(op, CompareOp::Glob | CompareOp::NotGlob) {
        return false;
    }

    let filter_num = match parse_filter_value(filter_value) {
        Some(n) => n,
        None => return false,
    };

    match op {
        CompareOp::Eq => (stored - filter_num).abs() < f64::EPSILON,
        CompareOp::Ne => (stored - filter_num).abs() >= f64::EPSILON,
        CompareOp::Gt => stored > filter_num,
        CompareOp::Ge => stored >= filter_num,
        CompareOp::Lt => stored < filter_num,
        CompareOp::Le => stored <= filter_num,
        CompareOp::Glob | CompareOp::NotGlob => unreachable!(),
    }
}

/// Parse a filter value string into a numeric value for comparison.
fn parse_filter_value(value: &str) -> Option<f64> {
    // Try as number first
    if let Ok(n) = value.parse::<f64>() {
        return Some(n);
    }

    // Try date formats - convert to Unix timestamp
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(value) {
        return Some(dt.timestamp() as f64);
    }
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S") {
        return Some(dt.and_utc().timestamp() as f64);
    }
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(value, "%Y:%m:%d %H:%M:%S") {
        return Some(dt.and_utc().timestamp() as f64);
    }
    if let Ok(date) = chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d") {
        return Some(date.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp() as f64);
    }

    None
}
// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // Tokenizer: context-aware value parsing
    // ========================================================================

    #[test]
    fn tokenize_mime_type_with_slash_and_hyphen() {
        let expr = Expr::parse("mime=application/octet-stream").unwrap();
        match expr {
            Expr::Compare { key, op, value } => {
                assert_eq!(key, "content.mime"); // normalized by parser
                assert_eq!(op, CompareOp::Eq);
                assert_eq!(value, "application/octet-stream");
            }
            _ => panic!("Expected Compare"),
        }
    }

    #[test]
    fn tokenize_glob_with_question_mark() {
        let expr = Expr::parse("ext~jp?g").unwrap();
        match expr {
            Expr::Compare { key, op, value } => {
                assert_eq!(key, "ext");
                assert_eq!(op, CompareOp::Glob);
                assert_eq!(value, "jp?g");
            }
            _ => panic!("Expected Compare"),
        }
    }

    #[test]
    fn tokenize_glob_with_star() {
        let expr = Expr::parse("ext~*.tmp").unwrap();
        match expr {
            Expr::Compare { key: _, op, value } => {
                assert_eq!(op, CompareOp::Glob);
                assert_eq!(value, "*.tmp");
            }
            _ => panic!("Expected Compare"),
        }
    }

    #[test]
    fn tokenize_not_glob_with_metacharacters() {
        let expr = Expr::parse("ext!~*.bak").unwrap();
        match expr {
            Expr::Compare { key: _, op, value } => {
                assert_eq!(op, CompareOp::NotGlob);
                assert_eq!(value, "*.bak");
            }
            _ => panic!("Expected Compare"),
        }
    }

    #[test]
    fn tokenize_hyphenated_value() {
        let expr = Expr::parse("tag=my-custom-tag").unwrap();
        match expr {
            Expr::Compare { key: _, op, value } => {
                assert_eq!(op, CompareOp::Eq);
                assert_eq!(value, "my-custom-tag");
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
                    Expr::Compare { value, .. } => assert_eq!(value, "image/jpeg"),
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
            Expr::In { key: _, values } => {
                assert_eq!(values, vec!["application/octet-stream", "image/jpeg"]);
            }
            _ => panic!("Expected In"),
        }
    }

    #[test]
    fn tokenize_exists_still_works() {
        let expr = Expr::parse("content.hash.sha256?").unwrap();
        match expr {
            Expr::Exists { key } => {
                assert_eq!(key, "content.hash.sha256");
            }
            _ => panic!("Expected Exists"),
        }
    }

    #[test]
    fn tokenize_quoted_value_still_works() {
        let expr = Expr::parse("mime=\"application/octet-stream\"").unwrap();
        match expr {
            Expr::Compare { key: _, op, value } => {
                assert_eq!(op, CompareOp::Eq);
                assert_eq!(value, "application/octet-stream");
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
            Expr::Compare { key: _, op, value } => {
                assert_eq!(op, CompareOp::Glob);
                assert_eq!(value, "[jJ][pP][gG]");
            }
            _ => panic!("Expected Compare"),
        }
    }

    #[test]
    fn tokenize_negative_number_value() {
        let expr = Expr::parse("source.size>-1").unwrap();
        match expr {
            Expr::Compare { key: _, op, value } => {
                assert_eq!(op, CompareOp::Gt);
                assert_eq!(value, "-1");
            }
            _ => panic!("Expected Compare"),
        }
    }

    #[test]
    fn tokenize_date_value_with_hyphens() {
        let expr = Expr::parse("source.mtime>2024-01-15").unwrap();
        match expr {
            Expr::Compare { key: _, op, value } => {
                assert_eq!(op, CompareOp::Gt);
                assert_eq!(value, "2024-01-15");
            }
            _ => panic!("Expected Compare"),
        }
    }

    #[test]
    fn tokenize_not_before_exists_no_value_mode() {
        let expr = Expr::parse("NOT content.hash.sha256?").unwrap();
        match expr {
            Expr::Not(inner) => match *inner {
                Expr::Exists { key } => assert_eq!(key, "content.hash.sha256"),
                _ => panic!("Expected Exists inside Not"),
            },
            _ => panic!("Expected Not"),
        }
    }

    #[test]
    fn tokenize_simple_value() {
        let expr = Expr::parse("source.ext=jpg").unwrap();
        match expr {
            Expr::Compare { key: _, op, value } => {
                assert_eq!(op, CompareOp::Eq);
                assert_eq!(value, "jpg");
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
                    Expr::Compare { value, .. } => assert_eq!(value, "jpg"),
                    _ => panic!("Expected Compare"),
                }
                match &parts[1] {
                    Expr::Compare { value, .. } => assert_eq!(value, "image/jpeg"),
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
        let err = Expr::parse("archived = true").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("status predicate") && msg.contains("'?'"),
            "Error should mention status predicate and '?': {msg}"
        );
    }

    #[test]
    fn parse_status_predicate_error_on_glob() {
        let err = Expr::parse("hashed ~ something").unwrap_err();
        let msg = err.to_string();
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
            Expr::Exists { key } => {
                assert_eq!(key, "content.archival");
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
        let err = Filter::parse("source.size=1 source.size=2").unwrap_err();
        assert!(err.to_string().contains("Unexpected token"), "{err}");
    }

    #[test]
    fn not_in_parses_as_negated_membership() {
        let expr = Expr::parse("source.ext NOT IN (jpg, png)").unwrap();
        match expr {
            Expr::Not(inner) => match *inner {
                Expr::In { key, values } => {
                    assert_eq!(key, "source.ext");
                    assert_eq!(values, vec!["jpg", "png"]);
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
        assert!(err.to_string().contains("Expected operator"), "{err}");
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
