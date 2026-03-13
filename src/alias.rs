use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::Path;

use crate::expr::alias as expr_alias;
use crate::expr::filter::Expr;

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

/// Classify alias values: expression aliases get wrapped in parentheses,
/// key aliases pass through unchanged. Classification uses Expr::parse() —
/// if the value parses as a valid filter expression, it's an expression alias.
fn classify_aliases(raw: HashMap<String, String>) -> HashMap<String, String> {
    raw.into_iter()
        .map(|(name, value)| {
            let processed = if Expr::parse(&value).is_ok() {
                format!("({value})")
            } else {
                value
            };
            (name, processed)
        })
        .collect()
}

/// Expand alias references in filter strings.
/// Fast path: if no filter contains `@` outside quotes, returns the input unchanged.
/// Otherwise loads aliases from `$canon_home/aliases.toml` and expands all references.
pub fn expand_filter_strings(filters: &[String], canon_home: &Path) -> Result<Vec<String>> {
    if !filters.iter().any(|f| expr_alias::has_alias_references(f)) {
        return Ok(filters.to_vec());
    }

    let aliases = classify_aliases(load_aliases(canon_home)?.unwrap_or_default());
    let aliases_path = canon_home.join("aliases.toml");

    filters
        .iter()
        .map(|f| expr_alias::expand_aliases(f, &aliases, &aliases_path))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_expression_alias() {
        let mut raw = HashMap::new();
        raw.insert("image".to_string(), "source.ext=jpg".to_string());
        let result = classify_aliases(raw);
        assert_eq!(result["image"], "(source.ext=jpg)");
    }

    #[test]
    fn test_classify_key_alias() {
        let mut raw = HashMap::new();
        raw.insert("filename".to_string(), "source.rel_path[-1]".to_string());
        let result = classify_aliases(raw);
        assert_eq!(result["filename"], "source.rel_path[-1]");
    }

    #[test]
    fn test_classify_compound_expression() {
        let mut raw = HashMap::new();
        raw.insert(
            "tens".to_string(),
            "source.mtime|year >= 2010 AND source.mtime|year < 2020".to_string(),
        );
        let result = classify_aliases(raw);
        assert_eq!(
            result["tens"],
            "(source.mtime|year >= 2010 AND source.mtime|year < 2020)"
        );
    }

    #[test]
    fn test_classify_key_with_modifiers() {
        let mut raw = HashMap::new();
        raw.insert("year".to_string(), "source.mtime|year".to_string());
        let result = classify_aliases(raw);
        assert_eq!(result["year"], "source.mtime|year");
    }

    #[test]
    fn test_classify_key_with_accessors() {
        let mut raw = HashMap::new();
        raw.insert("parent".to_string(), "source.rel_path[-2]".to_string());
        let result = classify_aliases(raw);
        assert_eq!(result["parent"], "source.rel_path[-2]");
    }

    #[test]
    fn test_classify_existence_check() {
        let mut raw = HashMap::new();
        raw.insert("hashed".to_string(), "content.hash.sha256?".to_string());
        let result = classify_aliases(raw);
        assert_eq!(result["hashed"], "(content.hash.sha256?)");
    }

    #[test]
    fn test_classify_empty_map() {
        let raw = HashMap::new();
        let result = classify_aliases(raw);
        assert!(result.is_empty());
    }

    #[test]
    fn test_classify_mixed() {
        let mut raw = HashMap::new();
        raw.insert("image".to_string(), "source.ext=jpg".to_string());
        raw.insert("filename".to_string(), "source.rel_path[-1]".to_string());
        raw.insert("large".to_string(), "source.size > 10000000".to_string());
        raw.insert("year".to_string(), "source.mtime|year".to_string());
        let result = classify_aliases(raw);
        assert_eq!(result["image"], "(source.ext=jpg)");
        assert_eq!(result["filename"], "source.rel_path[-1]");
        assert_eq!(result["large"], "(source.size > 10000000)");
        assert_eq!(result["year"], "source.mtime|year");
    }
}
