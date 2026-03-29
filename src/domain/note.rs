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
}
