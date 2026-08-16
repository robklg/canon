/// A timestamped note on a location within a root.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Note {
    pub id: i64,
    pub root_id: i64,
    pub rel_path: String,
    pub text: String,
    pub created_at: i64,
}

/// Summary of a location's notes — most recent note + count.
/// Used by spatial listing mode.
#[derive(Debug, Clone)]
pub struct LocationEntry {
    pub root_id: i64,
    pub rel_path: String,
    pub note_count: usize,
    pub latest_text: String,
    pub latest_created_at: i64,
}

/// Compute all ancestor rel_paths for a given rel_path.
/// "a/b/c" → ["a/b", "a", ""]
/// "a" → [""]
/// "" → [] (root has no ancestors)
pub fn ancestor_paths(rel_path: &str) -> Vec<String> {
    if rel_path.is_empty() {
        return Vec::new();
    }

    let mut result = Vec::new();
    let mut path = rel_path;
    while let Some(pos) = path.rfind('/') {
        path = &path[..pos];
        result.push(path.to_string());
    }
    // The root ancestor (empty string)
    result.push(String::new());
    result
}

/// Compute the display path of a note relative to a scope.
/// Same scope → "."
/// Descendant → first divergent path segment with trailing "/"
///
/// A note outside the scope is a caller error, not an input: the panic below
/// is the contract. Callers get the guarantee from the query that produced the
/// notes — the subtree fetch matches path boundaries literally, so everything
/// it returns is at or under the scope it was asked for. A subtree query that
/// over-matched (a prefix compared as a pattern, say) would land here.
pub fn relative_to_scope(note_rel_path: &str, scope_rel_path: &str) -> String {
    if note_rel_path == scope_rel_path {
        return ".".to_string();
    }

    let remainder = if scope_rel_path.is_empty() {
        note_rel_path
    } else {
        // Strip the scope prefix plus the separator
        let prefix = format!("{}/", scope_rel_path);
        note_rel_path
            .strip_prefix(&prefix)
            .expect("note_rel_path must be a descendant of scope_rel_path")
    };

    // Take the first segment
    let first_segment = match remainder.find('/') {
        Some(pos) => &remainder[..pos],
        None => remainder,
    };

    format!("{}/", first_segment)
}

/// Display path for a note in a timeline listing. Shared by `note list` and
/// the book's notes page — one note-identity rendering, two hosts.
pub fn note_display_path(
    note: &Note,
    roots: &std::collections::HashMap<i64, crate::core::domain::root::Root>,
    use_full_path: bool,
) -> String {
    if use_full_path {
        match roots.get(&note.root_id) {
            Some(root) => {
                if note.rel_path.is_empty() {
                    root.path.clone()
                } else {
                    format!("{}/{}", root.path, note.rel_path)
                }
            }
            None => note.rel_path.clone(),
        }
    } else if note.rel_path.is_empty() {
        "(root)".to_string()
    } else {
        note.rel_path.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ancestor_paths_deep() {
        assert_eq!(ancestor_paths("a/b/c"), vec!["a/b", "a", ""]);
    }

    #[test]
    fn ancestor_paths_single_segment() {
        assert_eq!(ancestor_paths("a"), vec![""]);
    }

    #[test]
    fn ancestor_paths_root() {
        let result: Vec<String> = ancestor_paths("");
        assert!(result.is_empty());
    }

    #[test]
    fn ancestor_paths_two_segments() {
        assert_eq!(ancestor_paths("a/b"), vec!["a", ""]);
    }

    #[test]
    fn relative_same_scope() {
        assert_eq!(relative_to_scope("a/b", "a/b"), ".");
    }

    #[test]
    fn relative_direct_child() {
        assert_eq!(relative_to_scope("a/b/c", "a/b"), "c/");
    }

    #[test]
    fn relative_deeper_descendant() {
        assert_eq!(relative_to_scope("a/b/c/d/e", "a/b"), "c/");
    }

    #[test]
    fn relative_from_root() {
        assert_eq!(relative_to_scope("a/b", ""), "a/");
    }

    #[test]
    fn relative_root_level_note() {
        assert_eq!(relative_to_scope("", ""), ".");
    }

    /// The non-descendant case is a broken precondition, not a display case:
    /// only a scope query that over-matched could produce it.
    #[test]
    #[should_panic(expected = "must be a descendant")]
    fn relative_to_scope_requires_a_descendant() {
        relative_to_scope("elsewhere/note", "scope");
    }
}
