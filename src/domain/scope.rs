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
