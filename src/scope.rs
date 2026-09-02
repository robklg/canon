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
/// Set-aside paths follow the scope itself.
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
    write_set_asides(handle, &scope.set_aside);
}

/// Print scope header for list/data commands (stderr handle).
///
/// Only prints when scoped — "scope: /path". Silent when global, except that
/// set-aside paths are stated whenever there are any.
pub fn print_list_scope(handle: &mut impl Write, scope: &ResolvedScope) {
    if !scope.is_global() {
        if scope.prefixes.len() == 1 {
            let _ = writeln!(handle, "scope: {}", scope.prefixes[0]);
        } else {
            let _ = writeln!(handle, "scope: {}", scope.prefixes.join(", "));
        }
    }
    write_set_asides(handle, &scope.set_aside);
}

/// State what the scope boundary set aside, ahead of any ceremony display —
/// for effectful commands, whose scope channel is the ceremony itself. Runs
/// before the plan and before any confirmation, so `--yes` and `--dry-run`
/// see the same statement an interactive run does.
pub fn print_scope_set_asides(scope: &ResolvedScope) {
    let stdout = std::io::stdout();
    let mut handle = stdout.lock();
    write_set_asides(&mut handle, &scope.set_aside);
}

/// State what the scope boundary set aside on stderr, for display paths
/// that carry no scope line of their own — a compact or machine-shaped
/// stdout stays exactly what it is, and the skip is still said.
pub fn eprint_scope_set_asides(scope: &ResolvedScope) {
    write_set_asides(&mut std::io::stderr().lock(), &scope.set_aside);
}

/// The one spelling of a set-aside line. A path Canon knows no sources for
/// is named and marked skipped — never dropped, never counted as run.
///
/// Takes the paths rather than a [`ResolvedScope`] because the same statement
/// is owed at two doors: the argument door, where a scope comes back as a
/// `ResolvedScope`, and the manifest door, where `cluster refresh` holds a
/// `ScopeResolution` instead. One situation, one sentence — the partition it
/// came out of is not the sentence's business.
pub fn write_set_asides(handle: &mut impl Write, set_aside: &[String]) {
    for path in set_aside {
        let _ = writeln!(handle, "no sources known at {path} — skipped");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn print_report_scope_scoped() {
        let scope = ResolvedScope {
            prefixes: vec!["/a/b".to_string()],
            set_aside: Vec::new(),
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
            set_aside: Vec::new(),
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
            set_aside: Vec::new(),
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
            set_aside: Vec::new(),
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

    #[test]
    fn print_report_scope_states_set_asides_after_the_scope_line() {
        let scope = ResolvedScope {
            prefixes: vec!["/a/b".to_string()],
            set_aside: vec!["/a/empty".to_string(), "/a/also-empty".to_string()],
            from_cwd: false,
            auto_include_archived: false,
        };
        let mut buf = Vec::new();
        print_report_scope(&mut buf, "Facts", &scope);
        assert_eq!(
            String::from_utf8(buf).unwrap(),
            "Facts: /a/b\n\
             no sources known at /a/empty — skipped\n\
             no sources known at /a/also-empty — skipped\n"
        );
    }

    #[test]
    fn print_list_scope_states_set_asides_on_its_handle() {
        let scope = ResolvedScope {
            prefixes: vec!["/a/b".to_string()],
            set_aside: vec!["/a/empty".to_string()],
            from_cwd: false,
            auto_include_archived: false,
        };
        let mut buf = Vec::new();
        print_list_scope(&mut buf, &scope);
        assert_eq!(
            String::from_utf8(buf).unwrap(),
            "scope: /a/b\nno sources known at /a/empty — skipped\n"
        );
    }

    /// A global scope says nothing about itself, but a set-aside is still a
    /// difference between what was asked and what ran.
    #[test]
    fn print_list_scope_is_silent_when_global_and_nothing_was_set_aside() {
        let scope = ResolvedScope {
            prefixes: Vec::new(),
            set_aside: Vec::new(),
            from_cwd: false,
            auto_include_archived: false,
        };
        let mut buf = Vec::new();
        print_list_scope(&mut buf, &scope);
        assert!(buf.is_empty());
    }
}
