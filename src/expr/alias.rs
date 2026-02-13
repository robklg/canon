use anyhow::{bail, Result};
use std::collections::HashMap;
use std::path::Path;

/// Check if a filter string contains any `@` alias references outside of quoted strings.
/// This is the fast-path check to avoid loading the aliases file when not needed.
pub fn has_alias_references(input: &str) -> bool {
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '\'' || chars[i] == '"' {
            let quote = chars[i];
            i += 1;
            while i < chars.len() && chars[i] != quote {
                i += 1;
            }
            if i < chars.len() {
                i += 1;
            }
            continue;
        }
        if chars[i] == '@' {
            return true;
        }
        i += 1;
    }
    false
}

/// Expand `@name` alias references in a filter expression string.
///
/// - `@name` outside quotes is replaced with the alias value from the aliases map
/// - `@` inside single or double quoted strings is treated as literal
/// - Values are substituted literally (caller is responsible for any wrapping)
/// - Unknown aliases produce an error referencing the aliases file path
pub fn expand_aliases(
    input: &str,
    aliases: &HashMap<String, String>,
    aliases_path: &Path,
) -> Result<String> {
    let chars: Vec<char> = input.chars().collect();
    let mut result = String::with_capacity(input.len());
    let mut i = 0;

    while i < chars.len() {
        // Track quoted strings — @ is literal inside quotes
        if chars[i] == '\'' || chars[i] == '"' {
            let quote = chars[i];
            result.push(chars[i]);
            i += 1;
            while i < chars.len() && chars[i] != quote {
                result.push(chars[i]);
                i += 1;
            }
            if i < chars.len() {
                result.push(chars[i]); // closing quote
                i += 1;
            }
            continue;
        }

        // Alias reference
        if chars[i] == '@' {
            i += 1; // skip @

            // Validate: name must start with a letter
            if i >= chars.len() || !chars[i].is_ascii_alphabetic() {
                let bad: String = if i < chars.len() {
                    format!("@{}", chars[i])
                } else {
                    "@".to_string()
                };
                bail!("Invalid alias syntax: '{bad}'. Alias names must start with a letter.");
            }

            // Read alias name: [a-zA-Z][a-zA-Z0-9_-]*
            let name_start = i;
            while i < chars.len()
                && (chars[i].is_ascii_alphanumeric() || chars[i] == '_' || chars[i] == '-')
            {
                i += 1;
            }
            let name: String = chars[name_start..i].iter().collect();

            match aliases.get(&name) {
                Some(value) => {
                    result.push_str(value);
                }
                None => {
                    if aliases.is_empty() {
                        bail!(
                            "Unknown alias '@{name}'. No aliases file found at {}",
                            aliases_path.display()
                        );
                    } else {
                        bail!(
                            "Unknown alias '@{name}'. Check your aliases file at {}",
                            aliases_path.display()
                        );
                    }
                }
            }
            continue;
        }

        // Regular character
        result.push(chars[i]);
        i += 1;
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn test_path() -> PathBuf {
        PathBuf::from("/home/.canon/aliases.toml")
    }

    fn test_aliases() -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert("image".to_string(), "source.ext=jpg".to_string());
        m.insert("video".to_string(), "source.ext=mp4".to_string());
        m.insert("my-alias".to_string(), "source.ext=png".to_string());
        m.insert("my_alias".to_string(), "source.ext=gif".to_string());
        m
    }

    // ======================================================================
    // Basic expansion
    // ======================================================================

    #[test]
    fn test_expand_single_alias() {
        let aliases = test_aliases();
        let result = expand_aliases("@image", &aliases, &test_path()).unwrap();
        assert_eq!(result, "source.ext=jpg");
    }

    #[test]
    fn test_expand_multiple_aliases() {
        let aliases = test_aliases();
        let result = expand_aliases("@image OR @video", &aliases, &test_path()).unwrap();
        assert_eq!(result, "source.ext=jpg OR source.ext=mp4");
    }

    #[test]
    fn test_expand_mixed_with_regular() {
        let aliases = test_aliases();
        let result =
            expand_aliases("@image AND source.size>1000", &aliases, &test_path()).unwrap();
        assert_eq!(result, "source.ext=jpg AND source.size>1000");
    }

    #[test]
    fn test_expand_no_aliases_passthrough() {
        let aliases = test_aliases();
        let result =
            expand_aliases("source.ext=jpg", &aliases, &test_path()).unwrap();
        assert_eq!(result, "source.ext=jpg");
    }

    #[test]
    fn test_expand_empty_string() {
        let aliases = test_aliases();
        let result = expand_aliases("", &aliases, &test_path()).unwrap();
        assert_eq!(result, "");
    }

    // ======================================================================
    // Quoting behavior
    // ======================================================================

    #[test]
    fn test_at_in_single_quotes_literal() {
        let aliases = test_aliases();
        let result =
            expand_aliases("source.path ~ '*@2x*'", &aliases, &test_path()).unwrap();
        assert_eq!(result, "source.path ~ '*@2x*'");
    }

    #[test]
    fn test_at_in_double_quotes_literal() {
        let aliases = test_aliases();
        let result =
            expand_aliases("source.path = \"user@host\"", &aliases, &test_path()).unwrap();
        assert_eq!(result, "source.path = \"user@host\"");
    }

    #[test]
    fn test_mixed_quoted_and_unquoted_at() {
        let aliases = test_aliases();
        let result = expand_aliases(
            "@image AND source.path ~ '*@2x*'",
            &aliases,
            &test_path(),
        )
        .unwrap();
        assert_eq!(result, "source.ext=jpg AND source.path ~ '*@2x*'");
    }

    // ======================================================================
    // Error cases
    // ======================================================================

    #[test]
    fn test_unknown_alias_with_file() {
        let aliases = test_aliases();
        let err = expand_aliases("@nonexistent", &aliases, &test_path()).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Unknown alias '@nonexistent'"), "{msg}");
        assert!(msg.contains("Check your aliases file"), "{msg}");
    }

    #[test]
    fn test_unknown_alias_no_file() {
        let aliases = HashMap::new();
        let err = expand_aliases("@nonexistent", &aliases, &test_path()).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Unknown alias '@nonexistent'"), "{msg}");
        assert!(msg.contains("No aliases file found"), "{msg}");
    }

    #[test]
    fn test_invalid_alias_name_digit() {
        let aliases = test_aliases();
        let err = expand_aliases("@123abc", &aliases, &test_path()).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Alias names must start with a letter"), "{msg}");
    }

    #[test]
    fn test_bare_at_end_of_input() {
        let aliases = test_aliases();
        let err = expand_aliases("something @", &aliases, &test_path()).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Alias names must start with a letter"), "{msg}");
    }

    // ======================================================================
    // has_alias_references
    // ======================================================================

    #[test]
    fn test_has_refs_simple() {
        assert!(has_alias_references("@image"));
    }

    #[test]
    fn test_has_refs_none() {
        assert!(!has_alias_references("source.ext=jpg"));
    }

    #[test]
    fn test_has_refs_at_in_quotes_ignored() {
        assert!(!has_alias_references("source.path ~ '*@2x*'"));
    }

    #[test]
    fn test_has_refs_mixed() {
        assert!(has_alias_references("source.path ~ '*@2x*' AND @image"));
    }

    // ======================================================================
    // Name validation edge cases
    // ======================================================================

    #[test]
    fn test_alias_name_with_hyphens() {
        let aliases = test_aliases();
        let result = expand_aliases("@my-alias", &aliases, &test_path()).unwrap();
        assert_eq!(result, "source.ext=png");
    }

    #[test]
    fn test_alias_name_with_underscores() {
        let aliases = test_aliases();
        let result = expand_aliases("@my_alias", &aliases, &test_path()).unwrap();
        assert_eq!(result, "source.ext=gif");
    }

    #[test]
    fn test_expand_pre_wrapped_value() {
        // Simulates what the command layer provides for expression aliases
        let mut aliases = HashMap::new();
        aliases.insert("image".to_string(), "(source.ext=jpg)".to_string());
        let result = expand_aliases("@image", &aliases, &test_path()).unwrap();
        assert_eq!(result, "(source.ext=jpg)");
    }

    #[test]
    fn test_alias_name_stops_at_boundary() {
        let aliases = test_aliases();
        // @image) should read "image" and leave the ")" in output
        let result = expand_aliases("(@image)", &aliases, &test_path()).unwrap();
        assert_eq!(result, "(source.ext=jpg)");
    }
}
