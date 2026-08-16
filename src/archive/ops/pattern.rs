//! Turning a manifest's destination pattern into a concrete path for one source.
//!
//! Expansion needs the source's facts on hand, so the two halves stay together:
//! building the evaluation context for a lock entry, and evaluating a pattern
//! against it. Planning and the apply command both expand patterns, so the pair
//! is shared rather than owned by either.

use std::collections::HashMap;

use anyhow::Result;

use crate::archive::domain::LockEntry;
use crate::core::domain::fact::{FactEntry, FactValue};
use crate::core::domain::path::path_strip_prefix;
use crate::expr::{self, EvalContext, Pattern};

/// Build an EvalContext for a source using pre-fetched facts and cached root paths.
fn build_eval_context(
    source: &LockEntry,
    needed_keys: &[String],
    scope_prefix: Option<&str>,
    root_paths: &HashMap<i64, String>,
    all_facts: &HashMap<i64, Vec<FactEntry>>,
) -> Result<EvalContext> {
    let mut ctx = EvalContext::new();

    let root_path = root_paths
        .get(&source.root_id)
        .ok_or_else(|| anyhow::anyhow!("Root {} not found in cache", source.root_id))?;

    let rel_path = if source.path == *root_path {
        String::new()
    } else if let Some(rel) = path_strip_prefix(&source.path, root_path) {
        rel.to_string()
    } else {
        source.path.clone()
    };

    ctx.set_source_root(root_path.clone());
    ctx.set_source_rel_path(rel_path);
    ctx.set_scope_prefix(scope_prefix.map(|s| s.to_string()));

    if let Some(source_facts) = all_facts.get(&source.id) {
        for key in needed_keys {
            // The same three namespaces are skipped where the facts are
            // fetched, both here and for the status read and the confirmation
            // samples. All four lists must agree: a namespace skipped when
            // fetching but not here lets a stored fact shadow the built-in of
            // the same name, which silently changes where files land.
            if key.starts_with("source.") || key.starts_with("scope.") || key == "object.hash" {
                continue;
            }
            if let Some(entry) = source_facts.iter().find(|f| f.key == *key) {
                ctx.set_fact(key, entry.value.clone());
            }
        }
    }

    if let Some(ref hash) = source.hash_value {
        ctx.set_fact("object.hash", FactValue::Text(hash.clone()));
    }

    Ok(ctx)
}

/// Evaluate a pattern for a source, returning the destination relative path.
pub fn evaluate_pattern(
    pattern: &Pattern,
    source: &LockEntry,
    needed_keys: &[String],
    scope_prefix: Option<&str>,
    root_paths: &HashMap<i64, String>,
    all_facts: &HashMap<i64, Vec<FactEntry>>,
) -> Result<String> {
    let ctx = build_eval_context(source, needed_keys, scope_prefix, root_paths, all_facts)?;
    expr::evaluate(pattern, &ctx)
}
