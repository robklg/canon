//! Scope display for the interface layer.
//!
//! Scope resolution lives in `core::ops::scope` (behavioral policy).
//! This module provides the display functions that format scope
//! information for terminal output — stdout for reports, stderr for lists.

use std::io::Write;

use crate::core::ops::scope::ResolvedScope;

/// Print scope header for report commands (stdout).
///
/// Always prints — shows "Label: /path" when scoped, "Label: all roots" when global.
/// For multiple paths (≤2): "Label: /path1, /path2"
/// For multiple paths (>2): one per line indented.
pub fn print_report_scope(handle: &mut impl Write, label: &str, scope: &ResolvedScope) {
    if scope.is_global() {
        let _ = writeln!(handle, "{label}: all roots");
    } else if scope.prefixes.len() == 1 {
        let _ = writeln!(handle, "{label}: {}", scope.prefixes[0]);
    } else if scope.prefixes.len() <= 2 {
        let _ = writeln!(handle, "{label}: {}", scope.prefixes.join(", "));
    } else {
        let _ = writeln!(handle, "{label}:");
        for p in &scope.prefixes {
            let _ = writeln!(handle, "  {p}");
        }
    }
}

/// Print scope header for list/data commands (stderr).
///
/// Only prints when scoped — "scope: /path". Silent when global.
pub fn print_list_scope(scope: &ResolvedScope) {
    if scope.is_global() {
        return;
    }
    if scope.prefixes.len() == 1 {
        eprintln!("scope: {}", scope.prefixes[0]);
    } else {
        eprintln!("scope: {}", scope.prefixes.join(", "));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn print_report_scope_scoped() {
        let scope = ResolvedScope {
            prefixes: vec!["/a/b".to_string()],
            from_cwd: true,
            auto_include_archived: false,
        };
        let mut buf = Vec::new();
        print_report_scope(&mut buf, "Facts", &scope);
        assert_eq!(String::from_utf8(buf).unwrap(), "Facts: /a/b\n");
    }

    #[test]
    fn print_report_scope_global() {
        let scope = ResolvedScope {
            prefixes: Vec::new(),
            from_cwd: false,
            auto_include_archived: false,
        };
        let mut buf = Vec::new();
        print_report_scope(&mut buf, "Facts", &scope);
        assert_eq!(String::from_utf8(buf).unwrap(), "Facts: all roots\n");
    }

    #[test]
    fn print_report_scope_two_paths() {
        let scope = ResolvedScope {
            prefixes: vec!["/a/b".to_string(), "/c/d".to_string()],
            from_cwd: false,
            auto_include_archived: false,
        };
        let mut buf = Vec::new();
        print_report_scope(&mut buf, "Survey", &scope);
        assert_eq!(String::from_utf8(buf).unwrap(), "Survey: /a/b, /c/d\n");
    }

    #[test]
    fn print_report_scope_many_paths() {
        let scope = ResolvedScope {
            prefixes: vec!["/a".to_string(), "/b".to_string(), "/c".to_string()],
            from_cwd: false,
            auto_include_archived: false,
        };
        let mut buf = Vec::new();
        print_report_scope(&mut buf, "Facts", &scope);
        assert_eq!(
            String::from_utf8(buf).unwrap(),
            "Facts:\n  /a\n  /b\n  /c\n"
        );
    }
}
