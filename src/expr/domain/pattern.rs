//! Expression parsing and evaluation for output patterns.
//!
//! This module handles pattern expansion for output paths in manifests.
//! Patterns consist of literal text and placeholders like `{fact.key}`,
//! `{source.rel_path[-1]}`, or `{content.DateTimeOriginal|year}`.

use anyhow::{bail, Result};
use std::collections::HashMap;

use super::key::{expand_alias, parse_key_and_accessor, BuiltinKey};
use super::transform::{
    apply_accessor, apply_modifier, fact_value_to_string, parse_modifier, ModifierCall,
    PathAccessor,
};
use crate::core::domain::fact::FactValue;

// ============================================================================
// Types
// ============================================================================

/// A parsed pattern consisting of segments
#[derive(Debug, Clone)]
pub struct Pattern {
    pub segments: Vec<PatternSegment>,
}

/// A segment in a pattern - either literal text or an expression
#[derive(Debug, Clone)]
pub enum PatternSegment {
    Literal(String),
    Expr(PatternExpr),
}

/// A parsed expression from within `{...}`
#[derive(Debug, Clone)]
pub struct PatternExpr {
    pub key: String,
    pub accessor: Option<PathAccessor>,
    pub modifiers: Vec<ModifierCall>,
}

/// Context for pattern evaluation - provides fact values and source info
pub struct EvalContext {
    /// Fact values by key (properly typed from database)
    facts: HashMap<String, FactValue>,
    /// Source root path (for path derivation)
    source_root: Option<String>,
    /// Source relative path (for path derivation)
    source_rel_path: Option<String>,
    /// Scope prefix for scope.rel_path derivation
    scope_prefix: Option<String>,
}

impl EvalContext {
    pub fn new() -> Self {
        EvalContext {
            facts: HashMap::new(),
            source_root: None,
            source_rel_path: None,
            scope_prefix: None,
        }
    }

    /// Set a fact value (should be properly typed from database lookup)
    pub fn set_fact(&mut self, key: &str, value: FactValue) {
        self.facts.insert(key.to_string(), value);
    }

    /// Set source root path (for deriving source.root, source.path)
    pub fn set_source_root(&mut self, root: String) {
        self.source_root = Some(root);
    }

    /// Set source relative path (for deriving source.rel_path, filename, etc.)
    pub fn set_source_rel_path(&mut self, rel_path: String) {
        self.source_rel_path = Some(rel_path);
    }

    /// Set scope prefix for deriving scope.rel_path
    pub fn set_scope_prefix(&mut self, prefix: Option<String>) {
        self.scope_prefix = prefix;
    }
}

impl Default for EvalContext {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Pattern Parsing
// ============================================================================

/// Parse a pattern string into a Pattern struct
pub fn parse_pattern(pattern: &str) -> Result<Pattern> {
    let mut segments = Vec::new();
    let mut chars = pattern.chars().peekable();
    let mut literal = String::new();

    while let Some(c) = chars.next() {
        if c == '{' {
            // Save any accumulated literal
            if !literal.is_empty() {
                segments.push(PatternSegment::Literal(std::mem::take(&mut literal)));
            }

            // Parse expression until closing brace
            let mut expr_str = String::new();
            let mut depth = 1;
            for c in chars.by_ref() {
                if c == '{' {
                    depth += 1;
                    expr_str.push(c);
                } else if c == '}' {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                    expr_str.push(c);
                } else {
                    expr_str.push(c);
                }
            }
            if depth != 0 {
                bail!("Unclosed '{{' in pattern");
            }

            // Check for alias expansion
            let expr_str = if let Some(expanded) = expand_alias(&expr_str) {
                expanded.to_string()
            } else {
                expr_str
            };

            let expr = parse_expr(&expr_str)?;
            segments.push(PatternSegment::Expr(expr));
        } else {
            literal.push(c);
        }
    }

    // Save any remaining literal
    if !literal.is_empty() {
        segments.push(PatternSegment::Literal(literal));
    }

    Ok(Pattern { segments })
}

/// Parse an expression string (the content within `{...}`)
fn parse_expr(s: &str) -> Result<PatternExpr> {
    let s = s.trim();
    if s.is_empty() {
        bail!("Empty expression");
    }

    // Split by pipe to get modifiers
    let parts: Vec<&str> = s.split('|').collect();
    let key_part = parts[0].trim();
    let modifier_parts = &parts[1..];

    // Parse key and accessor from key_part
    let (key, accessor) = parse_key_and_accessor(key_part)?;

    // Parse modifiers
    let mut modifiers = Vec::new();
    for mod_str in modifier_parts {
        let modifier = parse_modifier(mod_str.trim())?;
        modifiers.push(modifier);
    }

    Ok(PatternExpr {
        key,
        accessor,
        modifiers,
    })
}

/// Extract all fact keys referenced by a pattern (for prefetching from DB)
pub fn extract_fact_keys(pattern: &Pattern) -> Vec<String> {
    let mut keys = Vec::new();
    for segment in &pattern.segments {
        if let PatternSegment::Expr(expr) = segment {
            keys.push(expr.key.clone());
        }
    }
    keys
}

// ============================================================================
// Pattern Evaluation
// ============================================================================

/// Evaluate a parsed pattern with the given context
pub fn evaluate(pattern: &Pattern, ctx: &EvalContext) -> Result<String> {
    let mut result = String::new();

    for segment in &pattern.segments {
        match segment {
            PatternSegment::Literal(s) => result.push_str(s),
            PatternSegment::Expr(expr) => {
                let value = evaluate_expr(expr, ctx)?;
                result.push_str(&value);
            }
        }
    }

    // Sanitize path (remove potentially dangerous characters)
    let result = result.replace("..", "_").replace('\0', "_");

    // Normalize to a clean relative path (strip leading '/', collapse '//', remove '.' segments)
    let result = normalize_pattern_result(&result);

    Ok(result)
}

/// Normalize a pattern result to a clean relative path.
/// Strips leading '/', collapses '//' to '/', removes '.' components.
/// The existing '..' sanitization (replaced with '_') runs before this.
fn normalize_pattern_result(path: &str) -> String {
    path.split('/')
        .filter(|s| !s.is_empty() && *s != ".")
        .collect::<Vec<_>>()
        .join("/")
}

/// Evaluate a single expression
fn evaluate_expr(expr: &PatternExpr, ctx: &EvalContext) -> Result<String> {
    // Get the base value
    let value = get_value(&expr.key, ctx)?;

    // Apply accessor if present
    let value = if let Some(ref accessor) = expr.accessor {
        apply_accessor(&value, accessor, &expr.key)?
    } else {
        value
    };

    // Apply modifiers in order (for_display: false since patterns are used for paths)
    let mut result = value;
    for modifier_call in &expr.modifiers {
        result = apply_modifier(&result, modifier_call, &expr.key, false)?;
    }

    // Convert to string
    Ok(fact_value_to_string(&result))
}

/// Get a fact value by key, handling derived facts
fn get_value(key: &str, ctx: &EvalContext) -> Result<FactValue> {
    // Handle scope.rel_path specially (not a BuiltinKey)
    if key == "scope.rel_path" {
        // Derived: strip scope prefix from full path
        match (&ctx.scope_prefix, &ctx.source_root, &ctx.source_rel_path) {
            (Some(scope), Some(root), Some(rel_path)) => {
                let full_path = if rel_path.is_empty() {
                    root.clone()
                } else {
                    format!("{root}/{rel_path}")
                };
                // Strip scope prefix
                let scope_rel = if full_path.starts_with(scope) {
                    let stripped = &full_path[scope.len()..];
                    stripped.trim_start_matches('/').to_string()
                } else {
                    // If scope doesn't match, return full rel_path
                    rel_path.clone()
                };
                return Ok(FactValue::Path(scope_rel));
            }
            (None, _, _) => bail!(
                "scope.rel_path not available (no scope was specified during manifest generation)"
            ),
            _ => bail!("scope.rel_path not available"),
        }
    }

    // Handle built-in keys via enum
    if let Some(builtin) = BuiltinKey::from_str(key) {
        match builtin {
            BuiltinKey::SourceRelPath => {
                if let Some(ref rel_path) = ctx.source_rel_path {
                    return Ok(FactValue::Path(rel_path.clone()));
                }
                bail!("source.rel_path not available");
            }
            BuiltinKey::SourceRoot => {
                if let Some(ref root) = ctx.source_root {
                    return Ok(FactValue::Path(root.clone()));
                }
                bail!("source.root not available");
            }
            BuiltinKey::SourcePath => {
                // Derived: root + "/" + rel_path
                match (&ctx.source_root, &ctx.source_rel_path) {
                    (Some(root), Some(rel_path)) => {
                        let full = if rel_path.is_empty() {
                            root.clone()
                        } else {
                            format!("{root}/{rel_path}")
                        };
                        return Ok(FactValue::Path(full));
                    }
                    _ => bail!("source.path not available (requires root and rel_path)"),
                }
            }
            // Other builtin keys are looked up in facts or not available in patterns
            _ => {}
        }
    }

    // Look up in facts
    if let Some(value) = ctx.facts.get(key) {
        return Ok(value.clone());
    }

    // Build list of available facts for error message
    let mut available: Vec<&str> = ctx.facts.keys().map(|s| s.as_str()).collect();
    // Add derived facts that are available
    if ctx.source_rel_path.is_some() {
        available.push("source.rel_path");
    }
    if ctx.source_root.is_some() {
        available.push("source.root");
    }
    if ctx.source_root.is_some() && ctx.source_rel_path.is_some() {
        available.push("source.path");
    }
    if ctx.scope_prefix.is_some() && ctx.source_root.is_some() && ctx.source_rel_path.is_some() {
        available.push("scope.rel_path");
    }
    available.sort();

    bail!(
        "Unknown fact '{}'. Available facts: {}",
        key,
        available.join(", ")
    );
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::super::transform::Modifier;
    use super::*;

    #[test]
    fn test_parse_simple_literal() {
        let pattern = parse_pattern("foo/bar").unwrap();
        assert_eq!(pattern.segments.len(), 1);
        assert!(matches!(&pattern.segments[0], PatternSegment::Literal(s) if s == "foo/bar"));
    }

    #[test]
    fn test_parse_simple_expr() {
        let pattern = parse_pattern("{filename}").unwrap();
        assert_eq!(pattern.segments.len(), 1);
        // filename is an alias, so it should expand
        if let PatternSegment::Expr(e) = &pattern.segments[0] {
            assert_eq!(e.key, "source.rel_path");
            assert!(matches!(e.accessor, Some(PathAccessor::Index(-1))));
        } else {
            panic!("Expected Expr");
        }
    }

    #[test]
    fn test_parse_mixed() {
        let pattern = parse_pattern("photos/{filename}").unwrap();
        assert_eq!(pattern.segments.len(), 2);
        assert!(matches!(&pattern.segments[0], PatternSegment::Literal(s) if s == "photos/"));
    }

    #[test]
    fn test_parse_with_modifier() {
        let pattern = parse_pattern("{content.DateTimeOriginal|year}").unwrap();
        if let PatternSegment::Expr(e) = &pattern.segments[0] {
            assert_eq!(e.key, "content.DateTimeOriginal");
            assert_eq!(e.modifiers.len(), 1);
            assert_eq!(e.modifiers[0].modifier, Modifier::Year);
            assert!(e.modifiers[0].args.is_empty());
        } else {
            panic!("Expected Expr");
        }
    }

    #[test]
    fn test_parse_with_accessor() {
        let pattern = parse_pattern("{source.rel_path[-2]}").unwrap();
        if let PatternSegment::Expr(e) = &pattern.segments[0] {
            assert_eq!(e.key, "source.rel_path");
            assert!(matches!(e.accessor, Some(PathAccessor::Index(-2))));
        } else {
            panic!("Expected Expr");
        }
    }

    #[test]
    fn test_parse_slice_accessor() {
        let pattern = parse_pattern("{source.rel_path[1:3]}").unwrap();
        if let PatternSegment::Expr(e) = &pattern.segments[0] {
            assert!(matches!(
                e.accessor,
                Some(PathAccessor::Slice {
                    start: Some(1),
                    end: Some(3)
                })
            ));
        } else {
            panic!("Expected Expr");
        }
    }

    #[test]
    fn test_evaluate_simple() {
        let pattern = parse_pattern("{filename}").unwrap();
        let mut ctx = EvalContext::new();
        ctx.set_source_rel_path("photos/2024/image.jpg".to_string());
        let result = evaluate(&pattern, &ctx).unwrap();
        assert_eq!(result, "image.jpg");
    }

    #[test]
    fn test_evaluate_path_index() {
        let pattern = parse_pattern("{source.rel_path[-2]}").unwrap();
        let mut ctx = EvalContext::new();
        ctx.set_source_rel_path("photos/2024/vacation/image.jpg".to_string());
        let result = evaluate(&pattern, &ctx).unwrap();
        assert_eq!(result, "vacation");
    }

    #[test]
    fn test_evaluate_path_slice() {
        let pattern = parse_pattern("{source.rel_path[0:2]}").unwrap();
        let mut ctx = EvalContext::new();
        ctx.set_source_rel_path("photos/2024/vacation/image.jpg".to_string());
        let result = evaluate(&pattern, &ctx).unwrap();
        assert_eq!(result, "photos/2024");
    }

    #[test]
    fn test_evaluate_time_modifier() {
        let pattern = parse_pattern("{source.mtime|year}").unwrap();
        let mut ctx = EvalContext::new();
        // 2024-06-15 12:00:00 UTC
        ctx.set_fact("source.mtime", FactValue::Time(1718452800));
        let result = evaluate(&pattern, &ctx).unwrap();
        assert_eq!(result, "2024");
    }

    #[test]
    fn test_evaluate_stem_modifier() {
        let pattern = parse_pattern("{stem}").unwrap();
        let mut ctx = EvalContext::new();
        ctx.set_source_rel_path("photos/image.jpg".to_string());
        let result = evaluate(&pattern, &ctx).unwrap();
        assert_eq!(result, "image");
    }

    #[test]
    fn test_evaluate_ext_modifier() {
        let pattern = parse_pattern("{ext}").unwrap();
        let mut ctx = EvalContext::new();
        ctx.set_source_rel_path("photos/image.jpg".to_string());
        let result = evaluate(&pattern, &ctx).unwrap();
        assert_eq!(result, "jpg");
    }

    #[test]
    fn test_out_of_bounds_error() {
        let pattern = parse_pattern("{source.rel_path[10]}").unwrap();
        let mut ctx = EvalContext::new();
        ctx.set_source_rel_path("photos/image.jpg".to_string());
        let result = evaluate(&pattern, &ctx);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("out of bounds"));
    }

    #[test]
    fn test_scope_rel_path() {
        let pattern = parse_pattern("{scope.rel_path}").unwrap();
        let mut ctx = EvalContext::new();
        ctx.set_source_root("/Photos".to_string());
        ctx.set_source_rel_path("Home/2024/vacation/image.jpg".to_string());
        ctx.set_scope_prefix(Some("/Photos/Home".to_string()));
        let result = evaluate(&pattern, &ctx).unwrap();
        assert_eq!(result, "2024/vacation/image.jpg");
    }

    #[test]
    fn test_extract_fact_keys() {
        let pattern = parse_pattern("{content.DateTimeOriginal|year}/{filename}").unwrap();
        let keys = extract_fact_keys(&pattern);
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"content.DateTimeOriginal".to_string()));
        assert!(keys.contains(&"source.rel_path".to_string()));
    }

    #[test]
    fn test_hash_short_alias() {
        let pattern = parse_pattern("{hash_short}").unwrap();
        let mut ctx = EvalContext::new();
        ctx.set_fact(
            "object.hash",
            FactValue::Text("abcdef1234567890".to_string()),
        );
        let result = evaluate(&pattern, &ctx).unwrap();
        assert_eq!(result, "abcdef12");
    }

    #[test]
    fn test_complex_pattern() {
        let pattern =
            parse_pattern("{source.rel_path[0]}/{source.mtime|year}/{stem}_{hash_short}.{ext}")
                .unwrap();
        let mut ctx = EvalContext::new();
        ctx.set_source_rel_path("vacation/photos/IMG_001.jpg".to_string());
        ctx.set_fact("source.mtime", FactValue::Time(1718452800));
        ctx.set_fact(
            "object.hash",
            FactValue::Text("abcdef1234567890".to_string()),
        );
        let result = evaluate(&pattern, &ctx).unwrap();
        assert_eq!(result, "vacation/2024/IMG_001_abcdef12.jpg");
    }

    // =========================================================================
    // normalize_pattern_result tests
    // =========================================================================

    #[test]
    fn normalize_pattern_result_normal_filename() {
        assert_eq!(normalize_pattern_result("5.avi"), "5.avi");
    }

    #[test]
    fn normalize_pattern_result_normal_subdir() {
        assert_eq!(
            normalize_pattern_result("subdir/file.jpg"),
            "subdir/file.jpg"
        );
    }

    #[test]
    fn normalize_pattern_result_leading_slash() {
        assert_eq!(normalize_pattern_result("/5.avi"), "5.avi");
    }

    #[test]
    fn normalize_pattern_result_double_leading_slash() {
        assert_eq!(normalize_pattern_result("//5.avi"), "5.avi");
    }

    #[test]
    fn normalize_pattern_result_interior_double_slash() {
        assert_eq!(
            normalize_pattern_result("subdir//file.jpg"),
            "subdir/file.jpg"
        );
    }

    #[test]
    fn normalize_pattern_result_leading_dot_slash() {
        assert_eq!(
            normalize_pattern_result("./subdir/file.jpg"),
            "subdir/file.jpg"
        );
    }

    #[test]
    fn normalize_pattern_result_interior_dot() {
        assert_eq!(
            normalize_pattern_result("subdir/./file.jpg"),
            "subdir/file.jpg"
        );
    }

    #[test]
    fn normalize_pattern_result_empty() {
        assert_eq!(normalize_pattern_result(""), "");
    }

    #[test]
    fn normalize_pattern_result_lone_slash() {
        assert_eq!(normalize_pattern_result("/"), "");
    }

    #[test]
    fn normalize_pattern_result_triple_slash() {
        assert_eq!(normalize_pattern_result("///"), "");
    }

    #[test]
    fn normalize_pattern_result_multi_level() {
        assert_eq!(normalize_pattern_result("a/b/c"), "a/b/c");
    }

    // =========================================================================
    // Pattern normalization integration tests
    // =========================================================================

    #[test]
    fn test_pattern_flat_file_no_absolute_path() {
        // Source with single-component rel_path (flat file, no subdirectory)
        // Pattern: {source.rel_path[:-1]}/{filename}
        // rel_path = "5.avi", rel_path[:-1] = "" (empty)
        // Raw concatenation: "/5.avi" -> Normalized: "5.avi"
        let pattern = parse_pattern("{source.rel_path[:-1]}/{filename}").unwrap();
        let mut ctx = EvalContext::new();
        ctx.set_source_rel_path("5.avi".to_string());
        let result = evaluate(&pattern, &ctx).unwrap();
        assert_eq!(result, "5.avi");
    }

    #[test]
    fn test_pattern_subdirectory_file_unchanged() {
        // Source with multi-component rel_path — normalization should not change valid paths
        // Pattern: {source.rel_path[:-1]}/{filename}
        // rel_path = "subdir/file.jpg", rel_path[:-1] = "subdir"
        // Result: "subdir/file.jpg" — unchanged by normalization
        let pattern = parse_pattern("{source.rel_path[:-1]}/{filename}").unwrap();
        let mut ctx = EvalContext::new();
        ctx.set_source_rel_path("subdir/file.jpg".to_string());
        let result = evaluate(&pattern, &ctx).unwrap();
        assert_eq!(result, "subdir/file.jpg");
    }
}
