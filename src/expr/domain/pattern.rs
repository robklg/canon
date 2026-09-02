//! Expression parsing and evaluation for output patterns.
//!
//! This module handles pattern expansion for output paths in manifests.
//! Patterns consist of literal text and placeholders like `{fact.key}`,
//! `{source.rel_path[-1]}`, or `{content.DateTimeOriginal|year}`.

use anyhow::{anyhow, bail, Result};
use std::collections::HashMap;

use super::key::{expand_alias, parse_key_and_accessor, BuiltinKey, OBJECT_HASH, SCOPE_REL_PATH};
use super::transform::{
    apply_accessor, apply_modifier, fact_value_to_string, parse_modifier, ModifierCall,
    PathAccessor,
};
use super::value::{get_builtin_value, SourceAttributes};
use crate::core::domain::fact::{FactEntry, FactValue};

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

/// The facts fetched for a run of pattern evaluation, keyed by source.
///
/// The invariant, and the reason this is a noun rather than a `HashMap`: **no
/// key in here is one the evaluation context supplies itself.** The prefetch
/// that builds it drops those before asking the database, so a stored fact
/// cannot reach evaluation wearing a computed key's name — which is what
/// would silently move where files land.
///
/// Before this type existed the guarantee was a copy of one `if` at four
/// sites, each carrying a comment saying all four had to agree and nothing
/// holding them to it. Wrapping the map is what turns "they agree" from a
/// thing to check into a thing that cannot be otherwise: the only way to fill
/// one in production is the prefetch that enforces it.
pub struct PatternFacts(HashMap<i64, Vec<FactEntry>>);

impl PatternFacts {
    /// Wrap a map the prefetch has already filtered. Facility-private: the
    /// invariant belongs to the prefetch, and this is how it hands its result
    /// over.
    pub(in crate::expr) fn new(facts: HashMap<i64, Vec<FactEntry>>) -> Self {
        PatternFacts(facts)
    }

    /// The stored facts fetched for one source, or nothing if it had none.
    pub fn for_source(&self, id: i64) -> &[FactEntry] {
        self.0.get(&id).map_or(&[], Vec::as_slice)
    }

    /// The one named escape: a test that wants a specific map, including the
    /// hostile ones that plant a context-supplied key deliberately.
    /// `cfg(test)` is what makes the invariant build-refused rather than
    /// convention-held — production has no route to this constructor at all.
    #[cfg(test)]
    pub fn from_entries(facts: HashMap<i64, Vec<FactEntry>>) -> Self {
        PatternFacts(facts)
    }
}

/// Whether the pattern evaluation context answers this key itself.
///
/// **The context-supplied law**: a key this returns true for is never read
/// from the facts table. Spelled once, here, and unreachable from outside the
/// facility on purpose — exporting it would mean three callers each having to
/// remember to call it, which is the failure mode being repaired rather than
/// a repair of it. What consumers get instead is `prefetch_pattern_facts`,
/// which applies it for them.
///
/// Two halves, and only one of them is derived. The built-in half reads
/// `is_computed`, which reads the resolver — a hand-written list would get
/// `content.hash.sha256` wrong, since it is a built-in *and* a genuinely
/// stored fact. The other two are named literally because neither is a
/// `BuiltinKey`: `scope.rel_path` is derived from the manifest's scope at
/// evaluation time, and `object.hash` comes off the lock entry. That half is
/// pinned rather than derived, and the pins say so.
pub(in crate::expr) fn is_context_supplied(key: &str) -> bool {
    key == SCOPE_REL_PATH
        || key == OBJECT_HASH
        || BuiltinKey::from_str(key).is_some_and(|k| k.is_computed())
}

/// Why a source carries no scope-relative measurement.
///
/// An absent measurement has two possible causes and they take different
/// answers, so an evaluation that cannot tell them apart must assert neither.
/// It is a property of the **lock as a whole**, not of the entry, which is why
/// the caller supplies it rather than the entry carrying it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Unmeasured {
    /// The manifest records no scope, so there was nothing to measure from.
    /// Not a failure of anything: `{scope.rel_path}` has no meaning for this
    /// run, and no amount of refreshing will give it one.
    NoScopeRecorded,
    /// The lock was written before the measurement was recorded in it. A
    /// refresh rebuilds it and the key resolves.
    LockPredatesMeasurement,
}

/// Context for pattern evaluation - provides fact values and source info
pub struct EvalContext<'a> {
    /// Fact values by key, stored rather than computed. Under the
    /// context-supplied law nothing in here is a key this context answers
    /// itself, so a lookup falling through to this map cannot shadow a
    /// built-in.
    facts: HashMap<String, FactValue>,
    /// What the source itself answers, which is every computed built-in. The
    /// shaping half reaches the same resolver the asking half does; before it
    /// did, it held two path strings and could derive three keys from them.
    source: Option<SourceAttributes>,
    /// This source's scope-relative path, as the run that selected it settled
    /// it. Borrowed from the caller's own record rather than derived here: the
    /// measurement is a property of the *selection*, and by the time a pattern
    /// is expanded the selection is long settled.
    scope_rel: Option<&'a str>,
    /// Why [`scope_rel`](Self::scope_rel) is absent, when it is. Carried
    /// beside it rather than inferred from it, because the two causes are
    /// indistinguishable from the entry alone and a refusal that guesses
    /// between them prescribes a remedy that may not work.
    unmeasured: Unmeasured,
}

impl<'a> EvalContext<'a> {
    pub fn new() -> Self {
        EvalContext {
            facts: HashMap::new(),
            source: None,
            scope_rel: None,
            // A bare context has no lock behind it and no scope in front of
            // it, which is exactly this reading.
            unmeasured: Unmeasured::NoScopeRecorded,
        }
    }

    /// Set a fact value (should be properly typed from database lookup)
    pub fn set_fact(&mut self, key: &str, value: FactValue) {
        self.facts.insert(key.to_string(), value);
    }

    /// Point the context at the source it is evaluating for.
    ///
    /// One setter rather than one per attribute: the attributes arrive
    /// together and half a source is not a state any caller means to be in.
    pub fn set_source(&mut self, attrs: SourceAttributes) {
        self.source = Some(attrs);
    }

    /// Give the context this source's settled scope-relative path, and — for
    /// the case where there is none — the run's own reason there is none.
    ///
    /// One setter for both because they are one fact in two parts: half of it
    /// is a state no caller means to be in.
    pub fn set_scope_rel(&mut self, scope_rel: Option<&'a str>, unmeasured: Unmeasured) {
        self.scope_rel = scope_rel;
        self.unmeasured = unmeasured;
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

/// Get a fact value by key.
///
/// Three sources of an answer, in this order: the pattern-only
/// `scope.rel_path`, which the context carries; the built-in vocabulary, which the
/// source answers through the one resolver both halves of the language share;
/// and the facts the caller prefetched. The order is what makes the law hold
/// from this side — but it is not what the law rests on. The prefetch never
/// asks for a key this function answers, so the facts map cannot hold one
/// (`PatternFacts`), and the precedence here is a second lock on a door that
/// is already shut.
fn get_value(key: &str, ctx: &EvalContext) -> Result<FactValue> {
    // The scope-relative path is answered from the context rather than looked
    // up: it is not a fact and carries no built-in key. It is also not
    // *derived* here any more — it was measured when the selection was
    // settled, and this reads what was settled.
    //
    // That collapses the two refusals a *resolution* used to fail with, which
    // cannot happen at evaluation time any more. It does not collapse the
    // third: "the manifest records no scope" was never a resolution failure —
    // it is a property of the manifest, it survives the move to write time
    // unchanged, and it takes a different answer, because no refresh can give
    // an unscoped manifest a scope. A destination is the one decision a user
    // cannot un-decide after a move, so the alternative to refusing is
    // inventing one — and a refusal naming a remedy that cannot work is one
    // step short of that.
    if key == SCOPE_REL_PATH {
        let scope_rel = ctx.scope_rel.ok_or_else(|| match ctx.unmeasured {
            Unmeasured::NoScopeRecorded => {
                anyhow!("{SCOPE_REL_PATH} is not available: the manifest records no scope")
            }
            Unmeasured::LockPredatesMeasurement => anyhow!(
                "{SCOPE_REL_PATH} is not available: this lock file was written before \
                 the scope-relative path was recorded in it. Run `canon cluster refresh` \
                 to rebuild the lock."
            ),
        })?;
        return Ok(FactValue::Path(scope_rel.to_string()));
    }

    // Every built-in the source itself answers goes to the resolver the
    // asking half already uses. Three of these arms used to be written out
    // here by hand, which is why the other six were missing.
    if let Some(builtin) = BuiltinKey::from_str(key) {
        if builtin.is_computed() {
            let source = ctx
                .source
                .as_ref()
                .ok_or_else(|| anyhow!("{key} is not available: no source in context"))?;
            return get_builtin_value(source, builtin)
                .ok_or_else(|| anyhow!("{key} is not available"));
        }
    }

    if let Some(value) = ctx.facts.get(key) {
        return Ok(value.clone());
    }

    bail!(
        "Unknown fact '{key}'. Available facts: {}",
        available(ctx).join(", ")
    );
}

/// What this context can answer, for the error message that says a key was not
/// among them.
///
/// Derived from the same conjugation the resolution above reads, never listed.
/// A hand-written list here would be a fourth spelling of the set — living
/// inside the very message whose job is to describe it accurately — and the
/// hole this story repairs was found precisely because this message happened
/// to be honest about the three keys it knew.
fn available<'k>(ctx: &'k EvalContext<'_>) -> Vec<&'k str> {
    use strum::IntoEnumIterator;

    let mut names: Vec<&str> = ctx.facts.keys().map(String::as_str).collect();
    if ctx.source.is_some() {
        for key in BuiltinKey::iter().filter(BuiltinKey::is_computed) {
            let name: &'static str = key.into();
            names.push(name);
        }
        if ctx.scope_rel.is_some() {
            names.push(SCOPE_REL_PATH);
        }
    }
    names.sort();
    names
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::super::transform::Modifier;
    use super::*;

    /// A source's settled scope-relative path, as the run that selected it
    /// measured it and wrote it into the lock. Evaluation reads this; it does
    /// not compute it, which is what the five tests below now say.
    fn measured(scope_rel: &str) -> Option<&str> {
        Some(scope_rel)
    }

    /// A source at `root` + `rel`, with the remaining attributes fixed. The
    /// mtime is 2024-06-15 12:00:00 UTC — the same instant `value.rs`'s
    /// display pin already asserts renders as `2024-06-15`.
    fn source(root: &str, rel: &str) -> SourceAttributes {
        SourceAttributes {
            id: 42,
            root_id: 1,
            root_path: root.to_string(),
            rel_path: rel.to_string(),
            size: 1024000,
            mtime: 1718452800,
            device: 16777220,
            inode: 12345678,
        }
    }

    /// The common case: a source under a root nothing else in the test cares
    /// about.
    fn at(rel: &str) -> SourceAttributes {
        source("/root", rel)
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
        ctx.set_source(at("photos/2024/image.jpg"));
        let result = evaluate(&pattern, &ctx).unwrap();
        assert_eq!(result, "image.jpg");
    }

    #[test]
    fn test_evaluate_path_index() {
        let pattern = parse_pattern("{source.rel_path[-2]}").unwrap();
        let mut ctx = EvalContext::new();
        ctx.set_source(at("photos/2024/vacation/image.jpg"));
        let result = evaluate(&pattern, &ctx).unwrap();
        assert_eq!(result, "vacation");
    }

    #[test]
    fn test_evaluate_path_slice() {
        let pattern = parse_pattern("{source.rel_path[0:2]}").unwrap();
        let mut ctx = EvalContext::new();
        ctx.set_source(at("photos/2024/vacation/image.jpg"));
        let result = evaluate(&pattern, &ctx).unwrap();
        assert_eq!(result, "photos/2024");
    }

    #[test]
    fn test_evaluate_time_modifier() {
        let pattern = parse_pattern("{source.mtime|year}").unwrap();
        let mut ctx = EvalContext::new();
        // The mtime comes from the source, not from a planted fact. It used
        // to come from a fact here, which is why this test passed while the
        // production path — where nothing plants one — could not expand this
        // pattern at all.
        let mut source = at("photos/image.jpg");
        source.mtime = 1718452800; // 2024-06-15 12:00:00 UTC
        ctx.set_source(source);
        let result = evaluate(&pattern, &ctx).unwrap();
        assert_eq!(result, "2024");
    }

    #[test]
    fn test_evaluate_stem_modifier() {
        let pattern = parse_pattern("{stem}").unwrap();
        let mut ctx = EvalContext::new();
        ctx.set_source(at("photos/image.jpg"));
        let result = evaluate(&pattern, &ctx).unwrap();
        assert_eq!(result, "image");
    }

    #[test]
    fn test_evaluate_ext_modifier() {
        let pattern = parse_pattern("{ext}").unwrap();
        let mut ctx = EvalContext::new();
        ctx.set_source(at("photos/image.jpg"));
        let result = evaluate(&pattern, &ctx).unwrap();
        assert_eq!(result, "jpg");
    }

    #[test]
    fn test_out_of_bounds_error() {
        let pattern = parse_pattern("{source.rel_path[10]}").unwrap();
        let mut ctx = EvalContext::new();
        ctx.set_source(at("photos/image.jpg"));
        let result = evaluate(&pattern, &ctx);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("out of bounds"));
    }

    #[test]
    fn test_scope_rel_path() {
        let pattern = parse_pattern("{scope.rel_path}").unwrap();
        let mut ctx = EvalContext::new();
        ctx.set_source(source("/Photos", "Home/2024/vacation/image.jpg"));
        ctx.set_scope_rel(
            measured("2024/vacation/image.jpg"),
            Unmeasured::NoScopeRecorded,
        );
        let result = evaluate(&pattern, &ctx).unwrap();
        assert_eq!(result, "2024/vacation/image.jpg");
    }

    /// P7 — the claim these five tests used to make, restated where it now
    /// lives. Sibling scopes still measure from their shared parent, so each
    /// scope's own name survives and the ancestors above it do not come
    /// along — but that is settled when the selection is, and evaluation
    /// reads the settled value rather than re-deriving it from a scope that
    /// may have been edited since.
    ///
    /// The rule itself is `ScopeVantage`'s and is pinned there; what this pins
    /// is that expansion is a **lookup**, so the two cannot disagree.
    #[test]
    fn scope_rel_path_is_the_measurement_the_lock_recorded() {
        let pattern = parse_pattern("{scope.rel_path}").unwrap();
        for rel in ["work/proj-v1/src/main.c", "work/proj-v2/src/main.c"] {
            // What `measure_entries` writes for a manifest naming
            // `/vol/work/proj-v1` and `/vol/work/proj-v2`: measured from
            // `/vol/work`, so the scope name survives and the ancestor above
            // it does not.
            let recorded = rel.trim_start_matches("work/");
            let mut ctx = EvalContext::new();
            ctx.set_source(source("/vol", rel));
            ctx.set_scope_rel(measured(recorded), Unmeasured::NoScopeRecorded);
            assert_eq!(evaluate(&pattern, &ctx).unwrap(), recorded);
        }
    }

    /// P8 — the path-law pin, moved with the strip it guards. A byte-prefix
    /// test would strip `/vol/photos2` with `/vol/photos` and hand back
    /// `2/x.jpg`; containment through its owner refuses instead. The strip now
    /// happens at write time, so this asserts the refusal reaches the lock —
    /// a source the vantage does not contain is recorded with **no**
    /// measurement, and expansion then refuses rather than inventing one.
    #[test]
    fn a_sibling_named_like_the_scope_is_not_under_it() {
        use crate::core::domain::path::path_strip_prefix;
        assert_eq!(path_strip_prefix("/vol/photos2/x.jpg", "/vol/photos"), None);

        let pattern = parse_pattern("{scope.rel_path}").unwrap();
        let mut ctx = EvalContext::new();
        ctx.set_source(source("/vol", "photos2/x.jpg"));
        ctx.set_scope_rel(None, Unmeasured::LockPredatesMeasurement);
        let result = evaluate(&pattern, &ctx);
        assert!(result.is_err(), "got {result:?}");
        assert_ne!(result.ok(), Some("2/x.jpg".to_string()));
    }

    /// P9 — the two refusals a *resolution* used to fail with are gone,
    /// because resolution no longer happens here; the two that remain are the
    /// two an absent measurement can actually mean, and each says its own
    /// thing.
    ///
    /// The distinction is load-bearing rather than cosmetic: only one of them
    /// has a remedy. A lock that predates the measurement is rebuilt by a
    /// refresh; a manifest that records no scope has nothing to measure from,
    /// and telling that user to refresh sends them round a loop.
    #[test]
    fn an_unmeasured_entry_is_refused_by_the_reason_it_is_unmeasured() {
        let pattern = parse_pattern("{scope.rel_path}").unwrap();

        let mut old_lock = EvalContext::new();
        old_lock.set_source(source("/media/backup", "proj-v1/src/main.c"));
        old_lock.set_scope_rel(None, Unmeasured::LockPredatesMeasurement);
        let err = evaluate(&pattern, &old_lock).unwrap_err().to_string();
        assert!(err.contains("cluster refresh"), "{err}");
        assert!(!err.contains("records no scope"), "{err}");

        let mut unscoped = EvalContext::new();
        unscoped.set_source(source("/media/backup", "proj-v1/src/main.c"));
        unscoped.set_scope_rel(None, Unmeasured::NoScopeRecorded);
        let err = evaluate(&pattern, &unscoped).unwrap_err().to_string();
        assert!(err.contains("records no scope"), "{err}");
        assert!(
            !err.contains("cluster refresh"),
            "a refresh cannot give an unscoped manifest a scope: {err}"
        );

        // And neither of the old resolution-time refusals may reappear:
        // neither can be true any more.
        for ctx in [&old_lock, &unscoped] {
            let err = evaluate(&pattern, ctx).unwrap_err().to_string();
            assert!(!err.contains("names no path in that root"), "{err}");
        }
    }

    /// P10 — and there is no fallback behind the refusal. A destination is the
    /// one decision a user cannot un-decide after a move, so an entry with no
    /// measurement must never quietly become the source's root-relative path,
    /// its filename, or the empty string.
    #[test]
    fn an_unmeasured_entry_never_falls_back_to_a_destination() {
        let pattern = parse_pattern("{scope.rel_path}").unwrap();
        let mut ctx = EvalContext::new();
        ctx.set_source(source("/vol", "work/proj-v1/src/main.c"));
        ctx.set_scope_rel(None, Unmeasured::NoScopeRecorded);
        assert!(evaluate(&pattern, &ctx).is_err());

        // And the key is not offered as available when it cannot be answered.
        assert!(!available(&ctx).contains(&SCOPE_REL_PATH));
    }

    /// The message that says a key was not found must describe the set the
    /// lookup actually consulted. It is derived from the same conjugation the
    /// lookup reads, so it cannot drift from it — and this asserts on the
    /// keys a hand-written list would have left out, not on the three it
    /// used to name.
    #[test]
    fn an_unknown_key_lists_the_computed_builtins_as_available() {
        let pattern = parse_pattern("{content.NotHere}").unwrap();
        let mut ctx = EvalContext::new();
        ctx.set_source(at("photos/image.jpg"));
        ctx.set_fact("content.Make", FactValue::Text("Canon".to_string()));

        let err = evaluate(&pattern, &ctx).unwrap_err().to_string();
        for expected in [
            "source.ext",
            "source.mtime",
            "source.size",
            "source.id",
            "source.device",
            "source.inode",
            "source.path",
            "source.root",
            "source.rel_path",
            "content.Make",
        ] {
            assert!(err.contains(expected), "'{expected}' missing from: {err}");
        }
        // Keys the source cannot answer are not claimed as available.
        assert!(!err.contains("hash_short"), "{err}");
    }

    /// A source at its own root's top has no filename and so no extension.
    /// The resolver answers with an empty string rather than refusing —
    /// consistent with the asking half, where `resolve_source_ext_no_extension`
    /// pins the same behaviour.
    #[test]
    fn a_source_at_its_root_yields_an_empty_extension() {
        let pattern = parse_pattern("photos/{source.ext}").unwrap();
        let mut ctx = EvalContext::new();
        ctx.set_source(at(""));
        assert_eq!(evaluate(&pattern, &ctx).unwrap(), "photos");
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
        ctx.set_source(at("vacation/photos/IMG_001.jpg"));
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
        ctx.set_source(at("5.avi"));
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
        ctx.set_source(at("subdir/file.jpg"));
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
