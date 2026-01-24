//! Scope domain concepts for canon.
//!
//! This module defines how path scopes work — the domain knowledge of
//! "what kind of match do we want?" separated from the SQL implementation
//! of "how do we express this in a query?".

use std::path::Path;

/// Domain concept: what kind of scope match do we want?
///
/// This enum represents the domain decision of whether to match
/// a specific file exactly or all files under a directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopeMatch {
    /// Match a specific file exactly
    ExactFile(String),
    /// Match all files under a directory
    UnderDirectory(String),
}

impl ScopeMatch {
    /// Classify a canonicalized path as file or directory scope.
    ///
    /// This performs filesystem I/O to determine if the path is a file.
    pub fn classify(path: &str) -> Self {
        if Path::new(path).is_file() {
            ScopeMatch::ExactFile(path.to_string())
        } else {
            ScopeMatch::UnderDirectory(path.to_string())
        }
    }

    /// Classify multiple canonicalized paths.
    pub fn classify_all(paths: &[String]) -> Vec<Self> {
        paths.iter().map(|p| Self::classify(p)).collect()
    }
}

// ============================================================================
// SQL Generation (no filesystem I/O)
// ============================================================================

/// Build SQL clause for multiple scope matches.
///
/// Returns the clause string and a vector of parameters to bind.
/// If no scopes, returns ("1=1", vec![]) to match everything.
///
/// This function is pure — no filesystem I/O. The classification
/// of paths into file vs directory happens via `ScopeMatch::classify`.
pub fn build_scope_clause(scopes: &[ScopeMatch]) -> (String, Vec<String>) {
    if scopes.is_empty() {
        return ("1=1".to_string(), vec![]);
    }

    let mut conditions: Vec<String> = Vec::new();
    let mut params: Vec<String> = Vec::new();

    for scope in scopes {
        match scope {
            ScopeMatch::ExactFile(path) => {
                // Exact file match
                conditions.push("(r.path || '/' || s.rel_path) = ?".to_string());
                params.push(path.clone());
            }
            ScopeMatch::UnderDirectory(path) => {
                // Directory: match files under it
                conditions.push("(r.path || '/' || s.rel_path) LIKE ? || '/%'".to_string());
                params.push(path.clone());
            }
        }
    }

    let clause = format!("({})", conditions.join(" OR "));
    (clause, params)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_scope_clause_empty() {
        let (clause, params) = build_scope_clause(&[]);
        assert_eq!(clause, "1=1");
        assert!(params.is_empty());
    }

    #[test]
    fn build_scope_clause_single_file() {
        let scopes = vec![ScopeMatch::ExactFile("/a/b/file.txt".to_string())];
        let (clause, params) = build_scope_clause(&scopes);
        assert!(clause.contains("= ?"));
        assert!(!clause.contains("LIKE"));
        assert_eq!(params, vec!["/a/b/file.txt"]);
    }

    #[test]
    fn build_scope_clause_single_directory() {
        let scopes = vec![ScopeMatch::UnderDirectory("/a/b".to_string())];
        let (clause, params) = build_scope_clause(&scopes);
        assert!(clause.contains("LIKE ? || '/%'"));
        assert!(!clause.contains("= ?"));
        assert_eq!(params, vec!["/a/b"]);
    }

    #[test]
    fn build_scope_clause_mixed() {
        let scopes = vec![
            ScopeMatch::ExactFile("/a/file.txt".to_string()),
            ScopeMatch::UnderDirectory("/b/dir".to_string()),
        ];
        let (clause, params) = build_scope_clause(&scopes);
        assert!(clause.contains(" OR "));
        assert!(clause.contains("= ?"));
        assert!(clause.contains("LIKE ? || '/%'"));
        assert_eq!(params, vec!["/a/file.txt", "/b/dir"]);
    }

    #[test]
    fn build_scope_clause_multiple_directories() {
        let scopes = vec![
            ScopeMatch::UnderDirectory("/a".to_string()),
            ScopeMatch::UnderDirectory("/b".to_string()),
            ScopeMatch::UnderDirectory("/c".to_string()),
        ];
        let (clause, params) = build_scope_clause(&scopes);
        assert!(clause.starts_with("("));
        assert!(clause.ends_with(")"));
        assert_eq!(clause.matches(" OR ").count(), 2);
        assert_eq!(params, vec!["/a", "/b", "/c"]);
    }

}
