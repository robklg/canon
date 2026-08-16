use anyhow::{bail, Result};
use rusqlite::{params, Connection};
use std::collections::{HashMap, HashSet};

use super::eval as expr;
use crate::domain::fact;
use crate::repo::db::populate_temp_sources;

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
#[derive(Debug, Clone, Copy, PartialEq)]
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
const STATUS_KEYWORDS: &[(&str, StatusPredicate)] = &[
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

// Keep Filter as alias for backwards compatibility
pub type Filter = Expr;

// ============================================================================
// Fact Cache for Bulk Prefetching
// ============================================================================

/// Cache of prefetched fact values to avoid N+1 queries
struct FactCache {
    /// Source facts: (source_id, key) -> FactValue
    source_facts: HashMap<(i64, String), fact::FactValue>,
    /// Object facts: (object_id, key) -> FactValue
    object_facts: HashMap<(i64, String), fact::FactValue>,
    /// Source to object mapping
    source_objects: HashMap<i64, i64>,
    /// Keys that were prefetched (for existence checks)
    prefetched_keys: HashSet<String>,
    // Lazily populated status predicate data
    /// Object IDs that exist in archive roots (for `archived?`).
    archived_objects: Option<HashSet<i64>>,
    /// Source IDs that are excluded at source or object level (for `excluded?`).
    excluded_sources: Option<HashSet<i64>>,
    /// Source IDs with facts beyond content.hash.sha256 (for `enriched?`).
    enriched_sources: Option<HashSet<i64>>,
}

impl FactCache {
    fn new() -> Self {
        FactCache {
            source_facts: HashMap::new(),
            object_facts: HashMap::new(),
            source_objects: HashMap::new(),
            prefetched_keys: HashSet::new(),
            archived_objects: None,
            excluded_sources: None,
            enriched_sources: None,
        }
    }

    fn get_source_fact(&self, source_id: i64, key: &str) -> Option<&fact::FactValue> {
        self.source_facts.get(&(source_id, key.to_string()))
    }

    fn get_object_fact(&self, source_id: i64, key: &str) -> Option<&fact::FactValue> {
        self.source_objects
            .get(&source_id)
            .and_then(|obj_id| self.object_facts.get(&(*obj_id, key.to_string())))
    }

    fn get_object_id(&self, source_id: i64) -> Option<i64> {
        self.source_objects.get(&source_id).copied()
    }

    fn has_key(&self, key: &str) -> bool {
        self.prefetched_keys.contains(key)
    }
}

/// A fact row: (entity_id, value_text, value_num, value_time).
type FactRow = (i64, Option<String>, Option<f64>, Option<i64>);

/// Prefetch facts for a batch of sources and keys
fn prefetch_facts(conn: &mut Connection, source_ids: &[i64], keys: &[String]) -> Result<FactCache> {
    let mut cache = FactCache::new();

    if source_ids.is_empty() || keys.is_empty() {
        return Ok(cache);
    }

    // Parse keys to get base keys (without accessors/modifiers)
    let base_keys: Vec<String> = keys
        .iter()
        .filter_map(|k| parse_key_with_modifiers(k).ok().map(|(base, _, _)| base))
        .collect();

    // Skip built-in keys (they don't need DB lookups)
    let stored_keys: Vec<&String> = base_keys
        .iter()
        .filter(|k| !expr::is_builtin_key(k))
        .collect();

    for key in &base_keys {
        cache.prefetched_keys.insert(key.clone());
    }

    if stored_keys.is_empty() {
        // content.hash.sha256? needs source_objects even with no stored keys.
        if base_keys.iter().any(|k| k == "content.hash.sha256") {
            populate_temp_sources(conn, source_ids)?;
            let mappings: Vec<(i64, i64)> = conn
                .prepare(
                    "SELECT ts.id, s.object_id
                     FROM temp_sources ts
                     JOIN sources s ON s.id = ts.id
                     WHERE s.object_id IS NOT NULL",
                )?
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
                .collect::<Result<Vec<_>, _>>()?;
            for (source_id, object_id) in mappings {
                cache.source_objects.insert(source_id, object_id);
            }
            conn.execute("DROP TABLE IF EXISTS temp_sources", [])?;
        }
        return Ok(cache);
    }

    // Create temp table for source IDs
    populate_temp_sources(conn, source_ids)?;

    // Fetch source->object mappings
    let mappings: Vec<(i64, i64)> = conn
        .prepare(
            "SELECT ts.id, s.object_id
             FROM temp_sources ts
             JOIN sources s ON s.id = ts.id
             WHERE s.object_id IS NOT NULL",
        )?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<Vec<_>, _>>()?;

    for (source_id, object_id) in mappings {
        cache.source_objects.insert(source_id, object_id);
    }

    // Fetch source facts for all keys
    for key in &stored_keys {
        let facts: Vec<FactRow> = conn
            .prepare(
                "SELECT ts.id, f.value_text, f.value_num, f.value_time
                 FROM temp_sources ts
                 LEFT JOIN facts f ON f.entity_type = 'source' AND f.entity_id = ts.id AND f.key = ?"
            )?
            .query_map([*key], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)))?
            .collect::<Result<Vec<_>, _>>()?;

        for (source_id, text_val, num_val, time_val) in facts {
            if let Some(fv) = to_fact_value(text_val, num_val, time_val) {
                cache.source_facts.insert((source_id, (*key).clone()), fv);
            }
        }
    }

    // Fetch object facts for all keys
    // Get unique object IDs (multiple sources may share the same object)
    let object_ids: Vec<i64> = cache
        .source_objects
        .values()
        .copied()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    if !object_ids.is_empty() {
        // Create temp table for object IDs
        conn.execute("DROP TABLE IF EXISTS temp_objects", [])?;
        conn.execute(
            "CREATE TEMP TABLE temp_objects (id INTEGER PRIMARY KEY)",
            [],
        )?;
        {
            let mut stmt = conn.prepare("INSERT INTO temp_objects (id) VALUES (?)")?;
            for oid in &object_ids {
                stmt.execute([oid])?;
            }
        }

        for key in &stored_keys {
            let facts: Vec<FactRow> = conn
                .prepare(
                    "SELECT t.id, f.value_text, f.value_num, f.value_time
                     FROM temp_objects t
                     LEFT JOIN facts f ON f.entity_type = 'object' AND f.entity_id = t.id AND f.key = ?"
                )?
                .query_map([*key], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)))?
                .collect::<Result<Vec<_>, _>>()?;

            for (object_id, text_val, num_val, time_val) in facts {
                if let Some(fv) = to_fact_value(text_val, num_val, time_val) {
                    cache.object_facts.insert((object_id, (*key).clone()), fv);
                }
            }
        }

        conn.execute("DROP TABLE IF EXISTS temp_objects", [])?;
    }

    conn.execute("DROP TABLE IF EXISTS temp_sources", [])?;

    Ok(cache)
}

/// Convert DB values to FactValue
fn to_fact_value(
    text: Option<String>,
    num: Option<f64>,
    time: Option<i64>,
) -> Option<fact::FactValue> {
    if let Some(t) = text {
        Some(fact::FactValue::Text(t))
    } else if let Some(n) = num {
        Some(fact::FactValue::Num(n))
    } else {
        time.map(fact::FactValue::Time)
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
        let key = expr::normalize_key_string(&raw_key);

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
// Filter Evaluation
// ============================================================================

/// Apply a list of filters to a set of source IDs (AND logic between filters)
pub fn apply_filters(
    conn: &mut Connection,
    source_ids: &[i64],
    filters: &[Filter],
) -> Result<FilterResult> {
    let used_status = detect_status_predicates(filters);

    if filters.is_empty() {
        return Ok(FilterResult {
            source_ids: source_ids.to_vec(),
            used_status,
        });
    }

    // Validate that all keys in filters are known
    validate_filter_keys(conn, filters)?;

    // Extract all keys used in filters and prefetch their values
    let mut all_keys = Vec::new();
    for filter in filters {
        extract_keys(filter, &mut all_keys);
    }
    let mut cache = prefetch_facts(conn, source_ids, &all_keys)?;

    // Prefetch status predicate data (only what's needed)
    prefetch_status_data(conn, source_ids, &used_status, &mut cache)?;

    // Combine all filters with AND
    let combined = if filters.len() == 1 {
        filters[0].clone()
    } else {
        Expr::And(filters.to_vec())
    };

    let mut result = Vec::new();
    for &source_id in source_ids {
        if eval_expr_cached(conn, source_id, &combined, &cache)? {
            result.push(source_id);
        }
    }
    Ok(FilterResult {
        source_ids: result,
        used_status,
    })
}

/// Prefetch status predicate data into the cache based on which predicates are used.
fn prefetch_status_data(
    conn: &mut Connection,
    source_ids: &[i64],
    used: &UsedStatus,
    cache: &mut FactCache,
) -> Result<()> {
    if source_ids.is_empty() {
        return Ok(());
    }

    // Ensure source_objects is populated when hashed? or archived? need it.
    // prefetch_facts only populates this when there are stored fact keys to fetch.
    if (used.hashed || used.archived) && cache.source_objects.is_empty() {
        populate_temp_sources(conn, source_ids)?;
        let mappings: Vec<(i64, i64)> = conn
            .prepare(
                "SELECT ts.id, s.object_id
                 FROM temp_sources ts
                 JOIN sources s ON s.id = ts.id
                 WHERE s.object_id IS NOT NULL",
            )?
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<Result<Vec<_>, _>>()?;
        for (source_id, object_id) in mappings {
            cache.source_objects.insert(source_id, object_id);
        }
        conn.execute("DROP TABLE IF EXISTS temp_sources", [])?;
    }

    if used.archived {
        // Collect object_ids from the cache's source_objects mapping
        let object_ids: Vec<i64> = cache
            .source_objects
            .values()
            .copied()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let archived = crate::repo::object::batch_check_archived(conn, &object_ids, None)?;
        cache.archived_objects = Some(archived);
    }

    if used.excluded {
        populate_temp_sources(conn, source_ids)?;
        let excluded: HashSet<i64> = conn
            .prepare(
                "SELECT DISTINCT s.id FROM temp_sources ts
                 JOIN sources s ON s.id = ts.id
                 WHERE s.excluded = 1
                 UNION
                 SELECT DISTINCT s.id FROM temp_sources ts
                 JOIN sources s ON s.id = ts.id
                 JOIN objects o ON o.id = s.object_id
                 WHERE o.excluded = 1",
            )?
            .query_map([], |row| row.get(0))?
            .collect::<Result<HashSet<_>, _>>()?;
        conn.execute("DROP TABLE IF EXISTS temp_sources", [])?;
        cache.excluded_sources = Some(excluded);
    }

    if used.enriched {
        populate_temp_sources(conn, source_ids)?;
        let enriched: HashSet<i64> = conn
            .prepare(
                "SELECT DISTINCT ts.id FROM temp_sources ts
                 JOIN facts f ON f.entity_type = 'source' AND f.entity_id = ts.id
                     AND f.key != 'content.hash.sha256'
                 UNION
                 SELECT DISTINCT s.id FROM temp_sources ts
                 JOIN sources s ON s.id = ts.id
                 JOIN facts f ON f.entity_type = 'object' AND f.entity_id = s.object_id
                     AND f.key != 'content.hash.sha256'",
            )?
            .query_map([], |row| row.get(0))?
            .collect::<Result<HashSet<_>, _>>()?;
        conn.execute("DROP TABLE IF EXISTS temp_sources", [])?;
        cache.enriched_sources = Some(enriched);
    }

    // hashed? uses existing source_objects map — no extra prefetch needed
    Ok(())
}

/// Validate that all keys used in filters are known (built-in or exist in facts table).
///
/// Exists (`?`) expressions are excluded from validation because their purpose
/// is to test whether a fact is present — an unknown key simply means "not present"
/// for every source, which is a valid (false) result, not an error.
fn validate_filter_keys(conn: &Connection, filters: &[Filter]) -> Result<()> {
    let mut all_keys = Vec::new();
    for filter in filters {
        extract_comparable_keys(filter, &mut all_keys);
    }

    for key in all_keys {
        let (base_key, _, _) = parse_key_with_modifiers(&key)?;
        if !is_known_key(conn, &base_key)? {
            bail!("Unknown fact key: '{base_key}'. Use 'canon facts' to see available keys.");
        }
    }
    Ok(())
}

/// Extract all keys used in an expression (for prefetching).
fn extract_keys(expr: &Expr, keys: &mut Vec<String>) {
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
fn extract_comparable_keys(expr: &Expr, keys: &mut Vec<String>) {
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
fn detect_status_predicates(exprs: &[Filter]) -> UsedStatus {
    let mut used = UsedStatus::default();
    for expr in exprs {
        detect_status_in_expr(expr, &mut used);
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

/// Check if a key is known (either a built-in or exists in the facts table)
fn is_known_key(conn: &Connection, base_key: &str) -> Result<bool> {
    // Check built-in keys first
    if expr::is_builtin_key(base_key) {
        return Ok(true);
    }

    // Check if key exists in facts table (for any entity)
    let exists: bool = conn
        .query_row(
            "SELECT 1 FROM facts WHERE key = ? LIMIT 1",
            [base_key],
            |_| Ok(true),
        )
        .unwrap_or(false);

    Ok(exists)
}

// ============================================================================
// Fact Checking Functions
// ============================================================================

/// Check fact comparison for built-in keys (derived from source columns)
/// This is used by the cached version for built-in key fallback
fn check_fact_compare(
    conn: &Connection,
    source_id: i64,
    key: &str,
    op: CompareOp,
    value: &str,
) -> Result<bool> {
    use expr::BuiltinKey;

    // Parse key, accessor, and modifiers
    let (base_key, accessor, modifiers) = parse_key_with_modifiers(key)?;

    // Handle built-in keys via enum
    if let Some(builtin) = BuiltinKey::from_str(&base_key) {
        match builtin {
            // Text fields
            BuiltinKey::SourceExt | BuiltinKey::Ext => {
                let rel_path: String = conn.query_row(
                    "SELECT rel_path FROM sources WHERE id = ?",
                    [source_id],
                    |row| row.get(0),
                )?;
                let ext = std::path::Path::new(&rel_path)
                    .extension()
                    .and_then(|e| e.to_str())
                    .unwrap_or("");
                let fact_value = FactValue::Text(ext.to_string());
                if let Ok(modified) =
                    apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key)
                {
                    return Ok(compare_fact_value(&modified, op, value));
                }
                return Ok(false);
            }
            BuiltinKey::Filename => {
                let rel_path: String = conn.query_row(
                    "SELECT rel_path FROM sources WHERE id = ?",
                    [source_id],
                    |row| row.get(0),
                )?;
                let filename = std::path::Path::new(&rel_path)
                    .file_name()
                    .and_then(|f| f.to_str())
                    .unwrap_or(&rel_path);
                let fact_value = FactValue::Text(filename.to_string());
                if let Ok(modified) =
                    apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key)
                {
                    return Ok(compare_fact_value(&modified, op, value));
                }
                return Ok(false);
            }
            BuiltinKey::SourceRoot => {
                let root_path: String = conn.query_row(
                    "SELECT r.path FROM sources s JOIN roots r ON s.root_id = r.id WHERE s.id = ?",
                    [source_id],
                    |row| row.get(0),
                )?;
                let fact_value = FactValue::Text(root_path);
                if let Ok(modified) =
                    apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key)
                {
                    return Ok(compare_fact_value(&modified, op, value));
                }
                return Ok(false);
            }
            BuiltinKey::SourcePath => {
                let (root_path, rel_path): (String, String) = conn.query_row(
                    "SELECT r.path, s.rel_path FROM sources s JOIN roots r ON s.root_id = r.id WHERE s.id = ?",
                    [source_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?;
                let full_path = if rel_path.is_empty() {
                    root_path
                } else {
                    format!("{root_path}/{rel_path}")
                };
                let fact_value = FactValue::Text(full_path);
                if let Ok(modified) =
                    apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key)
                {
                    return Ok(compare_fact_value(&modified, op, value));
                }
                return Ok(false);
            }
            BuiltinKey::SourceRelPath => {
                let rel_path: String = conn.query_row(
                    "SELECT rel_path FROM sources WHERE id = ?",
                    [source_id],
                    |row| row.get(0),
                )?;
                let fact_value = FactValue::Text(rel_path);
                if let Ok(modified) =
                    apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key)
                {
                    return Ok(compare_fact_value(&modified, op, value));
                }
                return Ok(false);
            }

            // Numeric fields
            BuiltinKey::SourceSize | BuiltinKey::Size => {
                let v: i64 = conn.query_row(
                    "SELECT size FROM sources WHERE id = ?",
                    [source_id],
                    |row| row.get(0),
                )?;
                let fact_value = FactValue::Num(v as f64);
                if let Ok(modified) =
                    apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key)
                {
                    return Ok(compare_fact_value(&modified, op, value));
                }
                return Ok(false);
            }
            BuiltinKey::SourceMtime | BuiltinKey::Mtime => {
                let v: i64 = conn.query_row(
                    "SELECT mtime FROM sources WHERE id = ?",
                    [source_id],
                    |row| row.get(0),
                )?;
                // mtime is a time value, so use Time type for proper modifier support
                let fact_value = FactValue::Time(v);
                if let Ok(modified) =
                    apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key)
                {
                    return Ok(compare_fact_value(&modified, op, value));
                }
                return Ok(false);
            }
            BuiltinKey::SourceDevice => {
                let device: Option<i64> = conn.query_row(
                    "SELECT device FROM sources WHERE id = ?",
                    [source_id],
                    |row| row.get(0),
                )?;
                if let Some(d) = device {
                    let fact_value = FactValue::Num(d as f64);
                    if let Ok(modified) =
                        apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key)
                    {
                        return Ok(compare_fact_value(&modified, op, value));
                    }
                }
                return Ok(false);
            }
            BuiltinKey::SourceInode => {
                let inode: Option<i64> = conn.query_row(
                    "SELECT inode FROM sources WHERE id = ?",
                    [source_id],
                    |row| row.get(0),
                )?;
                if let Some(i) = inode {
                    let fact_value = FactValue::Num(i as f64);
                    if let Ok(modified) =
                        apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key)
                    {
                        return Ok(compare_fact_value(&modified, op, value));
                    }
                }
                return Ok(false);
            }
            BuiltinKey::RootId => {
                let v: i64 = conn.query_row(
                    "SELECT root_id FROM sources WHERE id = ?",
                    [source_id],
                    |row| row.get(0),
                )?;
                let fact_value = FactValue::Num(v as f64);
                if let Ok(modified) =
                    apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key)
                {
                    return Ok(compare_fact_value(&modified, op, value));
                }
                return Ok(false);
            }
            BuiltinKey::SourceId | BuiltinKey::Id => {
                // The source ID is the source_id parameter itself
                let fact_value = FactValue::Num(source_id as f64);
                if let Ok(modified) =
                    apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key)
                {
                    return Ok(compare_fact_value(&modified, op, value));
                }
                return Ok(false);
            }

            // Other builtin keys (aliases, etc.) fall through to fact lookup
            _ => {}
        }
    }

    // Get object_id for checking object facts
    let object_id: Option<i64> = conn
        .query_row(
            "SELECT object_id FROM sources WHERE id = ?",
            [source_id],
            |row| row.get(0),
        )
        .unwrap_or(None);

    // Check source facts then object facts
    // Note: if modifier fails (e.g., time modifier on text value due to bad data),
    // treat as "no match" rather than error
    if let Some(fact_value) = get_fact_value(conn, "source", source_id, &base_key)? {
        if let Ok(modified) = apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key) {
            if compare_fact_value(&modified, op, value) {
                return Ok(true);
            }
        }
    }

    if let Some(obj_id) = object_id {
        if let Some(fact_value) = get_fact_value(conn, "object", obj_id, &base_key)? {
            if let Ok(modified) =
                apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key)
            {
                if compare_fact_value(&modified, op, value) {
                    return Ok(true);
                }
            }
        }
    }

    Ok(false)
}

// ============================================================================
// Cached Evaluation Functions (for bulk filtering)
// ============================================================================

/// Evaluate an expression using prefetched fact cache
fn eval_expr_cached(
    conn: &Connection,
    source_id: i64,
    expr: &Expr,
    cache: &FactCache,
) -> Result<bool> {
    match expr {
        Expr::And(exprs) => {
            for e in exprs {
                if !eval_expr_cached(conn, source_id, e, cache)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        Expr::Or(exprs) => {
            for e in exprs {
                if eval_expr_cached(conn, source_id, e, cache)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Expr::Not(e) => Ok(!eval_expr_cached(conn, source_id, e, cache)?),
        Expr::Exists { key } => check_fact_exists_cached(conn, source_id, key, cache),
        Expr::Compare { key, op, value } => {
            check_fact_compare_cached(conn, source_id, key, *op, value, cache)
        }
        Expr::In { key, values } => check_fact_in_cached(conn, source_id, key, values, cache),
        Expr::Status(predicate) => match predicate {
            StatusPredicate::Hashed => Ok(cache.get_object_id(source_id).is_some()),
            StatusPredicate::Archived => {
                let archived_set = cache.archived_objects.as_ref().unwrap();
                Ok(cache
                    .get_object_id(source_id)
                    .is_some_and(|oid| archived_set.contains(&oid)))
            }
            StatusPredicate::Excluded => {
                let excluded_set = cache.excluded_sources.as_ref().unwrap();
                Ok(excluded_set.contains(&source_id))
            }
            StatusPredicate::Enriched => {
                let enriched_set = cache.enriched_sources.as_ref().unwrap();
                Ok(enriched_set.contains(&source_id))
            }
        },
    }
}

fn check_fact_exists_cached(
    _conn: &Connection,
    source_id: i64,
    key: &str,
    cache: &FactCache,
) -> Result<bool> {
    let (base_key, _accessor, _modifiers) = parse_key_with_modifiers(key)?;

    // Check cache for stored facts
    if cache.has_key(&base_key) {
        if cache.get_source_fact(source_id, &base_key).is_some() {
            return Ok(true);
        }
        if cache.get_object_fact(source_id, &base_key).is_some() {
            return Ok(true);
        }
    }

    // Check for built-in keys
    if base_key == "content.hash.sha256" {
        return Ok(cache.get_object_id(source_id).is_some());
    }
    Ok(expr::is_builtin_key(&base_key))
}

fn check_fact_compare_cached(
    conn: &Connection,
    source_id: i64,
    key: &str,
    op: CompareOp,
    value: &str,
    cache: &FactCache,
) -> Result<bool> {
    use expr::BuiltinKey;

    let (base_key, accessor, modifiers) = parse_key_with_modifiers(key)?;

    // Handle built-in keys (still need DB for source columns)
    if BuiltinKey::from_str(&base_key).is_some() {
        // For built-ins, fall back to uncached version (they query source table, not facts)
        return check_fact_compare(conn, source_id, key, op, value);
    }

    // Use cache for stored facts
    if let Some(fact_value) = cache.get_source_fact(source_id, &base_key) {
        let local_value = to_local_fact_value(fact_value);
        if let Ok(modified) = apply_accessor_and_modifiers(local_value, &accessor, &modifiers, key)
        {
            if compare_fact_value(&modified, op, value) {
                return Ok(true);
            }
        }
    }

    if let Some(fact_value) = cache.get_object_fact(source_id, &base_key) {
        let local_value = to_local_fact_value(fact_value);
        if let Ok(modified) = apply_accessor_and_modifiers(local_value, &accessor, &modifiers, key)
        {
            if compare_fact_value(&modified, op, value) {
                return Ok(true);
            }
        }
    }

    Ok(false)
}

fn check_fact_in_cached(
    conn: &Connection,
    source_id: i64,
    key: &str,
    values: &[String],
    cache: &FactCache,
) -> Result<bool> {
    for value in values {
        if check_fact_compare_cached(conn, source_id, key, CompareOp::Eq, value, cache)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Convert fact::FactValue to local FactValue
fn to_local_fact_value(fv: &fact::FactValue) -> FactValue {
    match fv {
        fact::FactValue::Text(t) => FactValue::Text(t.clone()),
        fact::FactValue::Num(n) => FactValue::Num(*n),
        fact::FactValue::Time(ts) => FactValue::Time(*ts),
        fact::FactValue::Path(p) => FactValue::Text(p.clone()),
    }
}

// ============================================================================
// Modifier and Accessor Parsing
// ============================================================================

// Use expr::parse_key_with_modifiers for parsing - just re-export for local use
fn parse_key_with_modifiers(
    key: &str,
) -> Result<(String, Option<expr::PathAccessor>, Vec<expr::ModifierCall>)> {
    expr::parse_key_with_modifiers(key)
}

/// Apply accessor and modifiers to a FactValue using the expr module
fn apply_accessor_and_modifiers(
    value: FactValue,
    accessor: &Option<expr::PathAccessor>,
    modifiers: &[expr::ModifierCall],
    key: &str,
) -> Result<FactValue> {
    // Convert to fact::FactValue
    let mut expr_value = match value {
        FactValue::Text(t) => fact::FactValue::Text(t),
        FactValue::Num(n) => fact::FactValue::Num(n),
        FactValue::Time(ts) => fact::FactValue::Time(ts),
    };

    // Apply accessor if present
    if let Some(acc) = accessor {
        expr_value = expr::apply_accessor(&expr_value, acc, key)?;
    }

    // Apply modifiers (for_display: true since filters are typically for display/comparison)
    for modifier_call in modifiers {
        expr_value = expr::apply_modifier(&expr_value, modifier_call, key, true)?;
    }

    // Convert back to FactValue
    Ok(match expr_value {
        fact::FactValue::Text(t) => FactValue::Text(t),
        fact::FactValue::Num(n) => FactValue::Num(n),
        fact::FactValue::Time(ts) => FactValue::Time(ts),
        fact::FactValue::Path(p) => FactValue::Text(p),
    })
}

// ============================================================================
// Value Handling
// ============================================================================

/// Stored fact value - can be text, number, or timestamp
/// A fact value from the database
#[derive(Clone)]
pub enum FactValue {
    Text(String),
    Num(f64),
    Time(i64),
}

impl From<FactValue> for serde_json::Value {
    fn from(fv: FactValue) -> Self {
        match fv {
            FactValue::Text(s) => serde_json::Value::String(s),
            FactValue::Num(n) => serde_json::Number::from_f64(n)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            FactValue::Time(ts) => serde_json::Value::Number(ts.into()),
        }
    }
}

pub fn get_fact_value(
    conn: &Connection,
    entity_type: &str,
    entity_id: i64,
    key: &str,
) -> Result<Option<FactValue>> {
    let result: Option<(Option<String>, Option<f64>, Option<i64>)> = conn
        .query_row(
            "SELECT value_text, value_num, value_time FROM facts
             WHERE entity_type = ? AND entity_id = ? AND key = ?",
            params![entity_type, entity_id, key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .ok();

    Ok(result.and_then(|(text, num, time)| {
        if let Some(t) = text {
            Some(FactValue::Text(t))
        } else if let Some(n) = num {
            Some(FactValue::Num(n))
        } else {
            time.map(FactValue::Time)
        }
    }))
}

fn compare_fact_value(fact: &FactValue, op: CompareOp, filter_value: &str) -> bool {
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
    use crate::repo::open_in_memory_for_test;
    use rusqlite::Connection as RawConnection;

    fn setup_test_db() -> RawConnection {
        open_in_memory_for_test()
    }

    fn insert_root(conn: &RawConnection, path: &str) -> i64 {
        conn.execute("INSERT INTO roots (path) VALUES (?)", [path])
            .unwrap();
        conn.last_insert_rowid()
    }

    fn insert_source(conn: &RawConnection, root_id: i64, rel_path: &str) -> i64 {
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, size, mtime, partial_hash, scanned_at, last_seen_at, device, inode)
             VALUES (?, ?, 1000, 1704067200, '', 0, 0, 0, 0)",
            rusqlite::params![root_id, rel_path],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    #[test]
    fn filter_out_of_bounds_index_is_non_match() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos");

        // 1-segment path: "image.jpg" (index [1] is out of bounds)
        let s1 = insert_source(&conn, root, "image.jpg");
        // 3-segment path: "2024/vacation/photo.jpg" (index [1] = "vacation")
        let s2 = insert_source(&conn, root, "2024/vacation/photo.jpg");
        // 2-segment path: "2024/doc.txt" (index [1] = "doc.txt")
        let s3 = insert_source(&conn, root, "2024/doc.txt");

        let filter = Expr::parse("source.rel_path[1]~'*tion*'").unwrap();
        let result = apply_filters(&mut conn, &[s1, s2, s3], &[filter]).unwrap();

        // s1 silently skipped (out of bounds), s2 matches, s3 doesn't match glob
        assert_eq!(result.source_ids, vec![s2]);
    }

    #[test]
    fn filter_out_of_bounds_negative_index_is_non_match() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos");

        // 1-segment path: [-3] is out of bounds
        let s1 = insert_source(&conn, root, "image.jpg");
        // 3-segment path: [-3] = "2024"
        let s2 = insert_source(&conn, root, "2024/vacation/photo.jpg");

        let filter = Expr::parse("source.rel_path[-3]=2024").unwrap();
        let result = apply_filters(&mut conn, &[s1, s2], &[filter]).unwrap();

        assert_eq!(result.source_ids, vec![s2]);
    }

    #[test]
    fn filter_out_of_bounds_slice_is_non_match() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos");

        // 1-segment path: [2:4] is out of bounds
        let s1 = insert_source(&conn, root, "image.jpg");
        // 4-segment path: [2:4] = "c/d"
        let s2 = insert_source(&conn, root, "a/b/c/d");

        let filter = Expr::parse("source.rel_path[2:4]~'c*'").unwrap();
        let result = apply_filters(&mut conn, &[s1, s2], &[filter]).unwrap();

        assert_eq!(result.source_ids, vec![s2]);
    }

    #[test]
    fn filter_modifier_failure_on_builtin_is_non_match() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos");

        // source.ext is a text value; |year modifier should fail on it
        let s1 = insert_source(&conn, root, "photo.jpg");

        let filter = Expr::parse("source.ext|year=2024").unwrap();
        let result = apply_filters(&mut conn, &[s1], &[filter]).unwrap();

        // Modifier failure treated as non-match, not error
        assert!(result.source_ids.is_empty());
    }

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

    // ========================================================================
    // Status predicate evaluation
    // ========================================================================

    fn insert_source_with_object(
        conn: &RawConnection,
        root_id: i64,
        rel_path: &str,
        object_id: Option<i64>,
    ) -> i64 {
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, object_id, size, mtime, partial_hash, scanned_at, last_seen_at, device, inode)
             VALUES (?, ?, ?, 1000, 1704067200, '', 0, 0, 0, 0)",
            rusqlite::params![root_id, rel_path, object_id],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn insert_object(conn: &RawConnection, hash: &str) -> i64 {
        conn.execute(
            "INSERT INTO objects (hash_type, hash_value) VALUES ('sha256', ?)",
            [hash],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn insert_object_excluded(conn: &RawConnection, hash: &str) -> i64 {
        conn.execute(
            "INSERT INTO objects (hash_type, hash_value, excluded) VALUES ('sha256', ?, 1)",
            [hash],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn insert_archive_root(conn: &RawConnection, path: &str) -> i64 {
        conn.execute(
            "INSERT INTO roots (path, role) VALUES (?, 'archive')",
            [path],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn insert_fact_entry(
        conn: &RawConnection,
        entity_type: &str,
        entity_id: i64,
        key: &str,
        value: &str,
    ) {
        // observed_basis_rev must be NULL for object-type entities (CHECK constraint)
        let basis_rev: Option<i64> = if entity_type == "source" {
            Some(0)
        } else {
            None
        };
        conn.execute(
            "INSERT INTO facts (entity_type, entity_id, key, value_text, observed_at, observed_basis_rev) VALUES (?, ?, ?, ?, 0, ?)",
            rusqlite::params![entity_type, entity_id, key, value, basis_rev],
        )
        .unwrap();
    }

    #[test]
    fn filter_archived_matches() {
        let mut conn = setup_test_db();
        let src_root = insert_root(&conn, "/src");
        let arc_root = insert_archive_root(&conn, "/archive");
        let obj = insert_object(&conn, "hash_a");

        // Source in source root with content also in archive
        let s1 = insert_source_with_object(&conn, src_root, "a.jpg", Some(obj));
        // Same content in archive
        insert_source_with_object(&conn, arc_root, "a.jpg", Some(obj));

        let filter = Expr::parse("archived?").unwrap();
        let result = apply_filters(&mut conn, &[s1], &[filter]).unwrap();
        assert_eq!(result.source_ids, vec![s1]);
    }

    #[test]
    fn filter_archived_excludes_unhashed() {
        let mut conn = setup_test_db();
        let src_root = insert_root(&conn, "/src");
        // Unhashed source (no object_id)
        let s1 = insert_source_with_object(&conn, src_root, "a.jpg", None);

        let filter = Expr::parse("archived?").unwrap();
        let result = apply_filters(&mut conn, &[s1], &[filter]).unwrap();
        assert!(result.source_ids.is_empty());
    }

    #[test]
    fn filter_archived_excludes_unarchived_hashed() {
        let mut conn = setup_test_db();
        let src_root = insert_root(&conn, "/src");
        let obj = insert_object(&conn, "hash_b");
        // Hashed but not in any archive
        let s1 = insert_source_with_object(&conn, src_root, "a.jpg", Some(obj));

        let filter = Expr::parse("archived?").unwrap();
        let result = apply_filters(&mut conn, &[s1], &[filter]).unwrap();
        assert!(result.source_ids.is_empty());
    }

    #[test]
    fn filter_not_archived_includes_unhashed() {
        let mut conn = setup_test_db();
        let src_root = insert_root(&conn, "/src");
        let s1 = insert_source_with_object(&conn, src_root, "a.jpg", None);

        let filter = Expr::parse("NOT archived?").unwrap();
        let result = apply_filters(&mut conn, &[s1], &[filter]).unwrap();
        assert_eq!(result.source_ids, vec![s1]);
    }

    #[test]
    fn filter_hashed_matches() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/src");
        let obj = insert_object(&conn, "hash_c");
        let s1 = insert_source_with_object(&conn, root, "a.jpg", Some(obj));
        let s2 = insert_source_with_object(&conn, root, "b.jpg", None);

        let filter = Expr::parse("hashed?").unwrap();
        let result = apply_filters(&mut conn, &[s1, s2], &[filter]).unwrap();
        assert_eq!(result.source_ids, vec![s1]);
    }

    #[test]
    fn filter_hashed_equivalence() {
        // hashed? and content.hash.sha256? should produce identical results
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/src");
        let obj = insert_object(&conn, "hash_d");
        let s1 = insert_source_with_object(&conn, root, "a.jpg", Some(obj));
        let s2 = insert_source_with_object(&conn, root, "b.jpg", None);
        let ids = [s1, s2];

        let f1 = Expr::parse("hashed?").unwrap();
        let f2 = Expr::parse("content.hash.sha256?").unwrap();
        let r1 = apply_filters(&mut conn, &ids, &[f1]).unwrap();
        let r2 = apply_filters(&mut conn, &ids, &[f2]).unwrap();
        assert_eq!(r1.source_ids, r2.source_ids);
    }

    #[test]
    fn filter_excluded_source_level() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/src");
        // Source-level excluded
        conn.execute(
            "INSERT INTO sources (root_id, rel_path, size, mtime, partial_hash, scanned_at, last_seen_at, device, inode, excluded)
             VALUES (?, 'excl.jpg', 1000, 1704067200, '', 0, 0, 0, 0, 1)",
            [root],
        )
        .unwrap();
        let s1 = conn.last_insert_rowid();

        let filter = Expr::parse("excluded?").unwrap();
        let result = apply_filters(&mut conn, &[s1], &[filter]).unwrap();
        assert_eq!(result.source_ids, vec![s1]);
    }

    #[test]
    fn filter_excluded_object_level() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/src");
        let obj = insert_object_excluded(&conn, "hash_excl");
        let s1 = insert_source_with_object(&conn, root, "a.jpg", Some(obj));

        let filter = Expr::parse("excluded?").unwrap();
        let result = apply_filters(&mut conn, &[s1], &[filter]).unwrap();
        assert_eq!(result.source_ids, vec![s1]);
    }

    #[test]
    fn filter_excluded_non_excluded_fails() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/src");
        let s1 = insert_source_with_object(&conn, root, "a.jpg", None);

        let filter = Expr::parse("excluded?").unwrap();
        let result = apply_filters(&mut conn, &[s1], &[filter]).unwrap();
        assert!(result.source_ids.is_empty());
    }

    #[test]
    fn filter_enriched_with_object_facts() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/src");
        let obj = insert_object(&conn, "hash_e");
        let s1 = insert_source_with_object(&conn, root, "a.jpg", Some(obj));
        // Object-level fact (not hash)
        insert_fact_entry(&conn, "object", obj, "content.mime", "image/jpeg");

        let filter = Expr::parse("enriched?").unwrap();
        let result = apply_filters(&mut conn, &[s1], &[filter]).unwrap();
        assert_eq!(result.source_ids, vec![s1]);
    }

    #[test]
    fn filter_enriched_with_source_facts() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/src");
        // Unhashed source with source-level fact
        let s1 = insert_source_with_object(&conn, root, "a.jpg", None);
        insert_fact_entry(&conn, "source", s1, "policy.tag", "keep");

        let filter = Expr::parse("enriched?").unwrap();
        let result = apply_filters(&mut conn, &[s1], &[filter]).unwrap();
        assert_eq!(result.source_ids, vec![s1]);
    }

    #[test]
    fn filter_enriched_hash_only_fails() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/src");
        let obj = insert_object(&conn, "hash_f");
        let s1 = insert_source_with_object(&conn, root, "a.jpg", Some(obj));
        // Only content.hash.sha256 fact — should NOT count as enriched
        insert_fact_entry(&conn, "object", obj, "content.hash.sha256", "hash_f");

        let filter = Expr::parse("enriched?").unwrap();
        let result = apply_filters(&mut conn, &[s1], &[filter]).unwrap();
        assert!(result.source_ids.is_empty());
    }

    #[test]
    fn filter_enriched_no_facts_fails() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/src");
        let s1 = insert_source_with_object(&conn, root, "a.jpg", None);

        let filter = Expr::parse("enriched?").unwrap();
        let result = apply_filters(&mut conn, &[s1], &[filter]).unwrap();
        assert!(result.source_ids.is_empty());
    }

    #[test]
    fn filter_composed_not_archived_and_hashed() {
        let mut conn = setup_test_db();
        let src_root = insert_root(&conn, "/src");
        let arc_root = insert_archive_root(&conn, "/archive");
        let obj_a = insert_object(&conn, "hash_archived");
        let obj_b = insert_object(&conn, "hash_not_archived");

        // s1: hashed + archived
        let s1 = insert_source_with_object(&conn, src_root, "a.jpg", Some(obj_a));
        insert_source_with_object(&conn, arc_root, "a.jpg", Some(obj_a));
        // s2: hashed + not archived
        let s2 = insert_source_with_object(&conn, src_root, "b.jpg", Some(obj_b));
        // s3: unhashed
        let s3 = insert_source_with_object(&conn, src_root, "c.jpg", None);

        let filter = Expr::parse("NOT archived? AND hashed?").unwrap();
        let result = apply_filters(&mut conn, &[s1, s2, s3], &[filter]).unwrap();
        assert_eq!(result.source_ids, vec![s2]);
    }

    // ========================================================================
    // Status predicate metadata
    // ========================================================================

    #[test]
    fn filter_result_flags_archived_used() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/src");
        let s1 = insert_source(&conn, root, "a.jpg");

        let filter = Expr::parse("archived?").unwrap();
        let result = apply_filters(&mut conn, &[s1], &[filter]).unwrap();
        assert!(result.used_status.archived);
        assert!(!result.used_status.hashed);
        assert!(!result.used_status.excluded);
        assert!(!result.used_status.enriched);
    }

    #[test]
    fn filter_result_flags_not_set_when_unused() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/src");
        let s1 = insert_source(&conn, root, "a.jpg");

        let filter = Expr::parse("source.ext=jpg").unwrap();
        let result = apply_filters(&mut conn, &[s1], &[filter]).unwrap();
        assert!(!result.used_status.archived);
        assert!(!result.used_status.hashed);
        assert!(!result.used_status.excluded);
        assert!(!result.used_status.enriched);
    }

    #[test]
    fn filter_result_flags_nested_detection() {
        let filter = Expr::parse("NOT (archived? AND excluded?)").unwrap();
        let used = detect_status_predicates(&[filter]);
        assert!(used.archived);
        assert!(used.excluded);
        assert!(!used.hashed);
        assert!(!used.enriched);
    }

    /// Regression: `NOT content.mime?` must not error when no facts are ingested.
    /// Existence checks should return false (not error) for unknown keys.
    #[test]
    fn exists_unknown_key_returns_false_not_error() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos");
        let s1 = insert_source(&conn, root, "a.jpg");
        let s2 = insert_source(&conn, root, "b.jpg");

        // No facts ingested — content.mime doesn't exist in facts table
        let filter = Expr::parse("NOT content.mime?").unwrap();
        let result = apply_filters(&mut conn, &[s1, s2], &[filter]).unwrap();

        // All sources should match (content.mime doesn't exist for any)
        assert_eq!(result.source_ids.len(), 2);
    }

    /// Existence check with unknown key (positive) should match nothing.
    #[test]
    fn exists_unknown_key_positive_matches_nothing() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos");
        let s1 = insert_source(&conn, root, "a.jpg");

        let filter = Expr::parse("content.mime?").unwrap();
        let result = apply_filters(&mut conn, &[s1], &[filter]).unwrap();

        assert!(result.source_ids.is_empty());
    }

    /// Compare with unknown key should still error (typo protection).
    #[test]
    fn compare_unknown_key_still_errors() {
        let mut conn = setup_test_db();
        let root = insert_root(&conn, "/photos");
        let s1 = insert_source(&conn, root, "a.jpg");

        let filter = Expr::parse("content.mime=image/jpeg").unwrap();
        let result = apply_filters(&mut conn, &[s1], &[filter]);

        let err = result
            .err()
            .expect("should error on unknown key in comparison");
        assert!(err.to_string().contains("Unknown fact key"));
    }
}
