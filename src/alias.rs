use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::Path;

use crate::expr::alias as expr_alias;

/// Load aliases from `$canon_home/aliases.toml`.
/// Returns `Ok(None)` if the file doesn't exist.
/// Errors on TOML parse failure.
fn load_aliases(canon_home: &Path) -> Result<Option<HashMap<String, String>>> {
    let path = canon_home.join("aliases.toml");
    if !path.exists() {
        return Ok(None);
    }
    let content = std::fs::read_to_string(&path)
        .with_context(|| format!("Failed to read {}", path.display()))?;
    let table: HashMap<String, String> =
        toml::from_str(&content).with_context(|| format!("Failed to parse {}", path.display()))?;
    Ok(Some(table))
}

/// Expand alias references in filter strings.
/// Fast path: if no filter contains `@` outside quotes, returns the input unchanged.
/// Otherwise loads aliases from `$canon_home/aliases.toml` and expands all references.
pub fn expand_filter_strings(filters: &[String], canon_home: &Path) -> Result<Vec<String>> {
    if !filters.iter().any(|f| expr_alias::has_alias_references(f)) {
        return Ok(filters.to_vec());
    }

    let aliases = expr_alias::classify_aliases(load_aliases(canon_home)?.unwrap_or_default());
    let aliases_path = canon_home.join("aliases.toml");

    filters
        .iter()
        .map(|f| expr_alias::expand_aliases(f, &aliases, &aliases_path))
        .collect()
}
