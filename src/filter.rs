use anyhow::{bail, Result};
use rusqlite::{params, Connection};

use crate::expr;

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

/// Filter expression AST - supports boolean logic
#[derive(Debug, Clone)]
pub enum Expr {
    And(Vec<Expr>),
    Or(Vec<Expr>),
    Not(Box<Expr>),
    Exists { key: String },
    Compare { key: String, op: CompareOp, value: String },
    In { key: String, values: Vec<String> },
}

// Keep Filter as alias for backwards compatibility
pub type Filter = Expr;

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
            bail!("Unexpected token after expression: {:?}", tokens[parser.pos]);
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
    Exists,      // The '?' suffix
    Ident(String),
    Value(String),
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

        // Single-char tokens
        match chars[i] {
            '(' => { tokens.push(Token::LParen); i += 1; continue; }
            ')' => { tokens.push(Token::RParen); i += 1; continue; }
            ',' => { tokens.push(Token::Comma); i += 1; continue; }
            '?' => { tokens.push(Token::Exists); i += 1; continue; }
            _ => {}
        }

        // Multi-char operators
        if i + 1 < chars.len() {
            let two: String = chars[i..i+2].iter().collect();
            match two.as_str() {
                ">=" => { tokens.push(Token::Op(CompareOp::Ge)); i += 2; continue; }
                "<=" => { tokens.push(Token::Op(CompareOp::Le)); i += 2; continue; }
                "!=" => { tokens.push(Token::Op(CompareOp::Ne)); i += 2; continue; }
                "!~" => { tokens.push(Token::Op(CompareOp::NotGlob)); i += 2; continue; }
                _ => {}
            }
        }

        // Single-char operators
        match chars[i] {
            '>' => { tokens.push(Token::Op(CompareOp::Gt)); i += 1; continue; }
            '<' => { tokens.push(Token::Op(CompareOp::Lt)); i += 1; continue; }
            '=' => { tokens.push(Token::Op(CompareOp::Eq)); i += 1; continue; }
            '~' => { tokens.push(Token::Op(CompareOp::Glob)); i += 1; continue; }
            '!' => { tokens.push(Token::Not); i += 1; continue; }
            _ => {}
        }

        // Keywords and identifiers
        // Allow alphanumeric, underscore, dot, pipe, and brackets (for accessors like key[-1] and modifiers like key|year)
        if chars[i].is_alphabetic() || chars[i] == '_' {
            let start = i;
            while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_' || chars[i] == '.' || chars[i] == '|' || chars[i] == '[' || chars[i] == ']' || chars[i] == ':' || (chars[i] == '-' && i > 0 && chars[i-1] == '[')) {
                i += 1;
            }
            let word: String = chars[start..i].iter().collect();
            // Only check for keywords if word doesn't contain pipe (modifier syntax) or brackets (accessor syntax)
            if !word.contains('|') && !word.contains('[') {
                match word.to_uppercase().as_str() {
                    "AND" => { tokens.push(Token::And); continue; }
                    "OR" => { tokens.push(Token::Or); continue; }
                    "NOT" => { tokens.push(Token::Not); continue; }
                    "IN" => { tokens.push(Token::In); continue; }
                    _ => {}
                }
            }
            tokens.push(Token::Ident(word));
            continue;
        }

        // Numbers (including negative, decimals, and date formats like 2024-01-15)
        if chars[i].is_ascii_digit() || (chars[i] == '-' && i + 1 < chars.len() && chars[i+1].is_ascii_digit()) {
            let start = i;
            if chars[i] == '-' { i += 1; }
            while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.' || chars[i] == '-' || chars[i] == ':' || chars[i] == 'T') {
                i += 1;
            }
            let val: String = chars[start..i].iter().collect();
            tokens.push(Token::Value(val));
            continue;
        }

        // Quoted strings
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
        if tok.is_some() { self.pos += 1; }
        tok
    }

    fn expect(&mut self, expected: &Token) -> Result<()> {
        match self.advance() {
            Some(t) if t == expected => Ok(()),
            Some(t) => bail!("Expected {:?}, got {:?}", expected, t),
            None => bail!("Expected {:?}, got end of input", expected),
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
                Expr::Or(mut v) => { v.push(right); Expr::Or(v) }
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
                Expr::And(mut v) => { v.push(right); Expr::And(v) }
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

    /// atom := ident '?' | ident 'IN' '(' value_list ')' | ident op value
    fn parse_atom(&mut self) -> Result<Expr> {
        let key = match self.advance() {
            Some(Token::Ident(k)) => k.clone(),
            Some(t) => bail!("Expected identifier, got {:?}", t),
            None => bail!("Expected identifier, got end of input"),
        };

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
            Some(t) => bail!("Expected operator after '{}', got {:?}", key, t),
            None => bail!("Expected operator after '{}', got end of input", key),
        };

        let value = self.parse_value()?;

        Ok(Expr::Compare { key, op, value })
    }

    fn parse_value(&mut self) -> Result<String> {
        match self.advance() {
            Some(Token::Value(v)) => Ok(v.clone()),
            Some(Token::Ident(v)) => Ok(v.clone()), // Allow unquoted values
            Some(t) => bail!("Expected value, got {:?}", t),
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
pub fn apply_filters(conn: &Connection, source_ids: &[i64], filters: &[Filter]) -> Result<Vec<i64>> {
    if filters.is_empty() {
        return Ok(source_ids.to_vec());
    }

    // Validate that all keys in filters are known
    validate_filter_keys(conn, filters)?;

    // Combine all filters with AND
    let combined = if filters.len() == 1 {
        filters[0].clone()
    } else {
        Expr::And(filters.to_vec())
    };

    let mut result = Vec::new();
    for &source_id in source_ids {
        if eval_expr(conn, source_id, &combined)? {
            result.push(source_id);
        }
    }
    Ok(result)
}

/// Validate that all keys used in filters are known (built-in or exist in facts table)
fn validate_filter_keys(conn: &Connection, filters: &[Filter]) -> Result<()> {
    let mut all_keys = Vec::new();
    for filter in filters {
        extract_keys(filter, &mut all_keys);
    }

    for key in all_keys {
        let (base_key, _, _) = parse_key_with_modifiers(&key)?;
        if !is_known_key(conn, &base_key)? {
            bail!("Unknown fact key: '{}'. Use 'canon facts' to see available keys.", base_key);
        }
    }
    Ok(())
}

/// Extract all keys used in an expression
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

/// Evaluate an expression against a single source
fn eval_expr(conn: &Connection, source_id: i64, expr: &Expr) -> Result<bool> {
    match expr {
        Expr::And(exprs) => {
            for e in exprs {
                if !eval_expr(conn, source_id, e)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        Expr::Or(exprs) => {
            for e in exprs {
                if eval_expr(conn, source_id, e)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Expr::Not(e) => Ok(!eval_expr(conn, source_id, e)?),
        Expr::Exists { key } => check_fact_exists(conn, source_id, key),
        Expr::Compare { key, op, value } => check_fact_compare(conn, source_id, key, *op, value),
        Expr::In { key, values } => check_fact_in(conn, source_id, key, values),
    }
}

// ============================================================================
// Fact Checking Functions
// ============================================================================

fn check_fact_exists(conn: &Connection, source_id: i64, key: &str) -> Result<bool> {
    // Parse key to get base key (ignore accessor and modifiers for existence check)
    let (base_key, _accessor, _modifiers) = parse_key_with_modifiers(key)?;

    // Check source facts
    let source_exists: bool = conn
        .query_row(
            "SELECT 1 FROM facts WHERE entity_type = 'source' AND entity_id = ? AND key = ?",
            params![source_id, base_key],
            |_| Ok(true),
        )
        .unwrap_or(false);

    if source_exists {
        return Ok(true);
    }

    // Check object facts if source has an object
    let object_id: Option<i64> = conn
        .query_row(
            "SELECT object_id FROM sources WHERE id = ?",
            [source_id],
            |row| row.get(0),
        )
        .unwrap_or(None);

    if let Some(obj_id) = object_id {
        let object_exists: bool = conn
            .query_row(
                "SELECT 1 FROM facts WHERE entity_type = 'object' AND entity_id = ? AND key = ?",
                params![obj_id, base_key],
                |_| Ok(true),
            )
            .unwrap_or(false);

        if object_exists {
            return Ok(true);
        }
    }

    // Check for built-in keys (derived from source columns)
    // Note: content.hash.sha256 is in BUILTIN_KEYS but "exists" only if object_id is set
    if base_key == "content.hash.sha256" {
        return Ok(object_id.is_some());
    }
    Ok(expr::is_builtin_key(&base_key))
}

fn check_fact_compare(conn: &Connection, source_id: i64, key: &str, op: CompareOp, value: &str) -> Result<bool> {
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
                let modified = apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key)?;
                return Ok(compare_fact_value(&modified, op, value));
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
                let modified = apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key)?;
                return Ok(compare_fact_value(&modified, op, value));
            }
            BuiltinKey::SourceRoot => {
                let root_path: String = conn.query_row(
                    "SELECT r.path FROM sources s JOIN roots r ON s.root_id = r.id WHERE s.id = ?",
                    [source_id],
                    |row| row.get(0),
                )?;
                let fact_value = FactValue::Text(root_path);
                let modified = apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key)?;
                return Ok(compare_fact_value(&modified, op, value));
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
                    format!("{}/{}", root_path, rel_path)
                };
                let fact_value = FactValue::Text(full_path);
                let modified = apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key)?;
                return Ok(compare_fact_value(&modified, op, value));
            }
            BuiltinKey::SourceRelPath => {
                let rel_path: String = conn.query_row(
                    "SELECT rel_path FROM sources WHERE id = ?",
                    [source_id],
                    |row| row.get(0),
                )?;
                let fact_value = FactValue::Text(rel_path);
                let modified = apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key)?;
                return Ok(compare_fact_value(&modified, op, value));
            }

            // Numeric fields
            BuiltinKey::SourceSize | BuiltinKey::Size => {
                let v: i64 = conn.query_row(
                    "SELECT size FROM sources WHERE id = ?",
                    [source_id],
                    |row| row.get(0),
                )?;
                let fact_value = FactValue::Num(v as f64);
                let modified = apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key)?;
                return Ok(compare_fact_value(&modified, op, value));
            }
            BuiltinKey::SourceMtime | BuiltinKey::Mtime => {
                let v: i64 = conn.query_row(
                    "SELECT mtime FROM sources WHERE id = ?",
                    [source_id],
                    |row| row.get(0),
                )?;
                // mtime is a time value, so use Time type for proper modifier support
                let fact_value = FactValue::Time(v);
                let modified = apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key)?;
                return Ok(compare_fact_value(&modified, op, value));
            }
            BuiltinKey::SourceDevice => {
                let device: Option<i64> = conn.query_row(
                    "SELECT device FROM sources WHERE id = ?",
                    [source_id],
                    |row| row.get(0),
                )?;
                if let Some(d) = device {
                    let fact_value = FactValue::Num(d as f64);
                    let modified = apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key)?;
                    return Ok(compare_fact_value(&modified, op, value));
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
                    let modified = apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key)?;
                    return Ok(compare_fact_value(&modified, op, value));
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
                let modified = apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key)?;
                return Ok(compare_fact_value(&modified, op, value));
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
    if let Some(fact_value) = get_fact_value(conn, "source", source_id, &base_key)? {
        let modified = apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key)?;
        if compare_fact_value(&modified, op, value) {
            return Ok(true);
        }
    }

    if let Some(obj_id) = object_id {
        if let Some(fact_value) = get_fact_value(conn, "object", obj_id, &base_key)? {
            let modified = apply_accessor_and_modifiers(fact_value, &accessor, &modifiers, key)?;
            if compare_fact_value(&modified, op, value) {
                return Ok(true);
            }
        }
    }

    Ok(false)
}

fn check_fact_in(conn: &Connection, source_id: i64, key: &str, values: &[String]) -> Result<bool> {
    // Check if fact value matches any of the provided values
    for value in values {
        if check_fact_compare(conn, source_id, key, CompareOp::Eq, value)? {
            return Ok(true);
        }
    }
    Ok(false)
}

// ============================================================================
// Modifier and Accessor Parsing
// ============================================================================

// Use expr::parse_key_with_modifiers for parsing - just re-export for local use
fn parse_key_with_modifiers(key: &str) -> Result<(String, Option<expr::PathAccessor>, Vec<expr::Modifier>)> {
    expr::parse_key_with_modifiers(key)
}

/// Apply accessor and modifiers to a FactValue using the expr module
fn apply_accessor_and_modifiers(
    value: FactValue,
    accessor: &Option<expr::PathAccessor>,
    modifiers: &[expr::Modifier],
    key: &str,
) -> Result<FactValue> {
    // Convert to expr::FactValue
    let mut expr_value = match value {
        FactValue::Text(t) => expr::FactValue::Text(t),
        FactValue::Num(n) => expr::FactValue::Num(n),
        FactValue::Time(ts) => expr::FactValue::Time(ts),
    };

    // Apply accessor if present
    if let Some(acc) = accessor {
        expr_value = expr::apply_accessor(&expr_value, acc, key)?;
    }

    // Apply modifiers
    for modifier in modifiers {
        expr_value = expr::apply_modifier(&expr_value, *modifier, key)?;
    }

    // Convert back to FactValue
    Ok(match expr_value {
        expr::FactValue::Text(t) => FactValue::Text(t),
        expr::FactValue::Num(n) => FactValue::Num(n),
        expr::FactValue::Time(ts) => FactValue::Time(ts),
        expr::FactValue::Path(p) => FactValue::Text(p),
    })
}

// ============================================================================
// Value Handling
// ============================================================================

/// Stored fact value - can be text, number, or timestamp
enum FactValue {
    Text(String),
    Num(f64),
    Time(i64),
}

fn get_fact_value(conn: &Connection, entity_type: &str, entity_id: i64, key: &str) -> Result<Option<FactValue>> {
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
        } else if let Some(ts) = time {
            Some(FactValue::Time(ts))
        } else {
            None
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
