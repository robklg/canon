//! Expression parsing and evaluation for output patterns.
//!
//! This module handles pattern expansion for output paths in manifests.
//! Patterns consist of literal text and placeholders like `{fact.key}`,
//! `{source.rel_path[-1]}`, or `{content.DateTimeOriginal|year}`.

use anyhow::{anyhow, bail, Result};
use std::collections::HashMap;

use super::key::{expand_alias, parse_key_and_accessor, BuiltinKey, SCOPE_REL_PATH};
use super::transform::{
    apply_accessor, apply_modifier, fact_value_to_string, parse_modifier, ModifierCall,
    PathAccessor,
};
use super::vantage::ScopeVantage;
use crate::core::domain::fact::FactValue;
use crate::core::domain::path::path_strip_prefix;

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
pub struct EvalContext<'a> {
    /// Fact values by key (properly typed from database)
    facts: HashMap<String, FactValue>,
    /// Source root path (for path derivation)
    source_root: Option<String>,
    /// Source relative path (for path derivation)
    source_rel_path: Option<String>,
    /// Where a `scope.rel_path` measures from, derived once per run. Borrowed
    /// rather than owned: a context is built per source, and the vantage is
    /// built once for all of them.
    vantage: Option<&'a ScopeVantage>,
}

impl<'a> EvalContext<'a> {
    pub fn new() -> Self {
        EvalContext {
            facts: HashMap::new(),
            source_root: None,
            source_rel_path: None,
            vantage: None,
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

    /// Point the context at the run's derived scope vantage.
    pub fn set_vantage(&mut self, vantage: &'a ScopeVantage) {
        self.vantage = Some(vantage);
    }
}

impl Default for EvalContext<'_> {
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

/// What a pattern can say about where files land before a single value is
/// resolved: the directory prefix every placement shares, and whether
/// expansion can open further directories below it.
///
/// The prefix is the literal directory path standing before the first
/// expression — `photos/2024/{filename}` places everything in `photos/2024`,
/// while `{content.Make}/{filename}` promises no shared directory at all.
/// `None` means the pattern commits to none.
///
/// The second half is deliberately conservative: it answers *can* this fan
/// out, not *will* it. A separator in a later literal fans out, and so does an
/// expression that may expand into more than one component — a path-valued key
/// carries whole directory chains (`{source.rel_path}` is `trip/day1/x.jpg`),
/// which the pattern text alone does not show. Reading a fan-out as flat would
/// name one directory as the destination when files land all over the tree; the
/// other way round only costs a hedge.
pub fn placement_shape(pattern: &Pattern) -> (Option<String>, bool) {
    // Everything literal before the first expression is fixed text; the
    // directory part of it is what every placement shares.
    let mut head = String::new();
    let mut rest = pattern.segments.iter();
    let mut first_expr = None;
    for segment in rest.by_ref() {
        match segment {
            PatternSegment::Literal(text) => head.push_str(text),
            PatternSegment::Expr(expr) => {
                first_expr = Some(expr);
                break;
            }
        }
    }

    // Cut at the last separator: what follows it is the start of a filename,
    // not a directory of its own.
    //
    // The head goes through the same sanitizing and normalizing that
    // evaluation puts the whole expansion through, and for the same reason:
    // this must name the directory files *land* in, not the one the pattern
    // text says. A leading slash is stripped at evaluation, so a prefix that
    // kept it would be an absolute path — which silently replaces the archive
    // root when joined onto it — and `..` becomes `_` there, so a prefix
    // holding `..` would name a directory that never exists.
    let prefix = match head.rfind('/') {
        Some(cut) => {
            let dir = normalize_pattern_result(&head[..cut].replace("..", "_"));
            (!dir.is_empty()).then_some(dir)
        }
        None => None,
    };

    let fans_out = first_expr.is_some_and(expression_may_nest)
        || rest.any(|segment| match segment {
            PatternSegment::Literal(text) => text.contains('/'),
            PatternSegment::Expr(expr) => expression_may_nest(expr),
        });

    (prefix, fans_out)
}

/// Whether one expression may expand into more than one path component.
fn expression_may_nest(expr: &PatternExpr) -> bool {
    // An index accessor picks a single component out of a path, whatever the
    // path held.
    if matches!(expr.accessor, Some(PathAccessor::Index(_))) {
        return false;
    }
    // The scope-relative path is derived at evaluation time and carries no
    // built-in key, but it is a path like any other.
    if expr.key == SCOPE_REL_PATH {
        return true;
    }
    BuiltinKey::from_str(&expr.key)
        .is_some_and(|key| matches!(key.fact_type(), crate::core::domain::fact::FactType::Path))
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
    // The scope-relative path is derived here rather than looked up: it is
    // not a fact and carries no built-in key. Every arm that cannot answer
    // refuses by name — a destination is the one decision a user cannot
    // un-decide after a move, so the alternative to refusing is inventing one.
    if key == SCOPE_REL_PATH {
        let vantage = ctx.vantage.filter(|v| !v.is_empty()).ok_or_else(|| {
            anyhow!("{SCOPE_REL_PATH} is not available: the manifest records no scope")
        })?;
        let (root, rel_path) = match (&ctx.source_root, &ctx.source_rel_path) {
            (Some(root), Some(rel_path)) => (root, rel_path),
            _ => bail!("{SCOPE_REL_PATH} is not available"),
        };
        let measured_from = vantage.for_root(root).ok_or_else(|| {
            anyhow!(
                "{SCOPE_REL_PATH} cannot be measured for a source in {root}: \
                 the manifest's scope names no path in that root"
            )
        })?;
        let full_path = if rel_path.is_empty() {
            root.clone()
        } else {
            format!("{root}/{rel_path}")
        };
        // Containment through its owner, never a byte prefix: `/R/photos`
        // must not swallow `/R/photos2/x.jpg` and strip it to `2/x.jpg`.
        let scope_rel = path_strip_prefix(&full_path, measured_from).ok_or_else(|| {
            anyhow!("{SCOPE_REL_PATH}: {full_path} is not under the scope vantage {measured_from}")
        })?;
        return Ok(FactValue::Path(scope_rel.to_string()));
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
    if ctx.vantage.is_some_and(|v| !v.is_empty())
        && ctx.source_root.is_some()
        && ctx.source_rel_path.is_some()
    {
        available.push(SCOPE_REL_PATH);
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

    fn vantage(prefixes: &[&str], roots: &[&str]) -> ScopeVantage {
        let owned: Vec<String> = prefixes.iter().map(|p| p.to_string()).collect();
        ScopeVantage::new(&owned, roots.iter().copied())
    }

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
        let vantage = vantage(&["/Photos/Home"], &["/Photos"]);
        let mut ctx = EvalContext::new();
        ctx.set_source_root("/Photos".to_string());
        ctx.set_source_rel_path("Home/2024/vacation/image.jpg".to_string());
        ctx.set_vantage(&vantage);
        let result = evaluate(&pattern, &ctx).unwrap();
        assert_eq!(result, "2024/vacation/image.jpg");
    }

    /// P7 — the friction end to end: with sibling scopes each source measures
    /// from their shared parent, so each scope's own name survives and the
    /// ancestors above it do not come along.
    #[test]
    fn scope_rel_path_measures_from_the_vantage() {
        let pattern = parse_pattern("{scope.rel_path}").unwrap();
        let vantage = vantage(&["/vol/work/proj-v1", "/vol/work/proj-v2"], &["/vol"]);
        for rel in ["work/proj-v1/src/main.c", "work/proj-v2/src/main.c"] {
            let mut ctx = EvalContext::new();
            ctx.set_source_root("/vol".to_string());
            ctx.set_source_rel_path(rel.to_string());
            ctx.set_vantage(&vantage);
            let result = evaluate(&pattern, &ctx).unwrap();
            // Measured from `/vol/work`, not from the root: the scope name
            // survives, the ancestor above it does not.
            assert_eq!(result, rel.trim_start_matches("work/"));
        }
    }

    /// P8 — the path-law pin. A byte-prefix test would strip `/vol/photos2`
    /// with `/vol/photos` and hand back `2/x.jpg`; containment through its
    /// owner refuses instead. Asserting on the absence of the wrong answer
    /// matters as much as the error: a bare `is_err()` would pass against a
    /// different bug.
    #[test]
    fn a_sibling_named_like_the_scope_is_not_under_it() {
        let pattern = parse_pattern("{scope.rel_path}").unwrap();
        let vantage = vantage(&["/vol/photos"], &["/vol"]);
        let mut ctx = EvalContext::new();
        ctx.set_source_root("/vol".to_string());
        ctx.set_source_rel_path("photos2/x.jpg".to_string());
        ctx.set_vantage(&vantage);
        let result = evaluate(&pattern, &ctx);
        assert!(result.is_err(), "got {result:?}");
        assert_ne!(result.ok(), Some("2/x.jpg".to_string()));
    }

    /// P9 — the silent fallback is gone. A source in a root the scope never
    /// names is refused, and the message names the root.
    #[test]
    fn a_source_whose_root_carries_no_scope_is_refused_by_name() {
        let pattern = parse_pattern("{scope.rel_path}").unwrap();
        let vantage = vantage(&["/vol/work/proj-v1"], &["/vol", "/media/backup"]);
        let mut ctx = EvalContext::new();
        ctx.set_source_root("/media/backup".to_string());
        ctx.set_source_rel_path("proj-v1/src/main.c".to_string());
        ctx.set_vantage(&vantage);
        let err = evaluate(&pattern, &ctx).unwrap_err().to_string();
        assert!(err.contains("/media/backup"), "{err}");
        assert!(err.contains("names no path in that root"), "{err}");
        // The other half of the distinction: a per-root refusal must not also
        // read as "this manifest records no scope". It records one.
        assert!(!err.contains("records no scope"), "{err}");
    }

    /// P10 — a manifest recording no scope at all says so, and says something
    /// different from P9: nowhere to measure from is not the same answer as
    /// nowhere to measure from *here*.
    #[test]
    fn no_scope_at_all_still_says_so() {
        let pattern = parse_pattern("{scope.rel_path}").unwrap();
        let vantage = vantage(&[], &["/vol"]);
        let mut ctx = EvalContext::new();
        ctx.set_source_root("/vol".to_string());
        ctx.set_source_rel_path("work/proj-v1/src/main.c".to_string());
        ctx.set_vantage(&vantage);
        let err = evaluate(&pattern, &ctx).unwrap_err().to_string();
        assert!(err.contains("records no scope"), "{err}");
        assert!(!err.contains("names no path in that root"), "{err}");
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

    // =========================================================================
    // placement_shape — what a pattern promises about where files land
    // =========================================================================

    fn shape(pattern: &str) -> (Option<String>, bool) {
        placement_shape(&parse_pattern(pattern).unwrap())
    }

    #[test]
    fn placement_shape_reads_a_literal_directory_prefix() {
        assert_eq!(
            shape("photos/2024/{filename}"),
            (Some("photos/2024".to_string()), false)
        );
    }

    #[test]
    fn placement_shape_finds_no_prefix_when_an_expression_comes_first() {
        assert_eq!(shape("{content.Make}/{filename}"), (None, true));
        assert_eq!(shape("{filename}"), (None, false));
    }

    #[test]
    fn placement_shape_cuts_a_partial_component_off_the_prefix() {
        // "img_" starts a filename, not a directory.
        assert_eq!(
            shape("photos/img_{filename}"),
            (Some("photos".to_string()), false)
        );
    }

    #[test]
    fn placement_shape_reports_fan_out_from_a_later_separator() {
        assert_eq!(
            shape("photos/{content.DateTimeOriginal|year}/{filename}"),
            (Some("photos".to_string()), true)
        );
    }

    #[test]
    fn placement_shape_reports_fan_out_for_a_path_valued_key() {
        // The separators live in the value, not in the pattern text: reading
        // this as flat would name one directory while files land all over it.
        assert_eq!(shape("{source.rel_path}"), (None, true));
        assert_eq!(shape("{scope.rel_path}"), (None, true));
        assert_eq!(
            shape("archive/{source.rel_path}"),
            (Some("archive".to_string()), true)
        );
    }

    #[test]
    fn placement_shape_reads_a_single_component_accessor_as_flat() {
        // One component out of a path is one component.
        assert_eq!(shape("{source.rel_path[-1]}"), (None, false));
        assert_eq!(shape("{source.rel_path[0]}/{filename}"), (None, true));
    }

    #[test]
    fn placement_shape_normalizes_the_prefix_the_way_evaluation_does() {
        // A leading slash is stripped when the pattern is evaluated, so a
        // prefix that kept it would name an absolute path — and joining one
        // onto the archive root discards the root entirely.
        assert_eq!(shape("/2024/{filename}"), (Some("2024".to_string()), false));
        // `..` becomes `_` at evaluation; the prefix must name the directory
        // files actually land in.
        assert_eq!(
            shape("../2024/{filename}"),
            (Some("_/2024".to_string()), false)
        );
        // A prefix that normalizes to nothing is no prefix.
        assert_eq!(shape("/{filename}"), (None, false));
        assert_eq!(shape("./{filename}"), (None, false));
    }

    #[test]
    fn placement_shape_reads_a_fully_literal_pattern() {
        assert_eq!(
            shape("archive/one.jpg"),
            (Some("archive".to_string()), false)
        );
        assert_eq!(shape("one.jpg"), (None, false));
    }
}
