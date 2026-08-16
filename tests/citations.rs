//! The comment citation scan.
//!
//! Comments that cite code locations go stale when code moves — and the stale
//! prose is never part of the diff that moved it, so no diff review sees it.
//! This check makes the mechanically checkable citation forms fail the build:
//!
//! - **File citations** — a comment names a `foo.rs`; the file must exist
//!   somewhere under `src/` or `tests/`.
//! - **Module-path citations** — a comment names an `a::b::c` path; the path's
//!   module prefix must resolve against the tree (filesystem modules plus
//!   inline `mod` declarations), allowing a trailing item segment and a
//!   `Type::method` tail.
//! - **Inventory bullets** — a `Modules:` line in a CLAUDE.md enumerates
//!   module names; each must exist in the directory the bullet describes.
//!   CLAUDE.md files are git-ignored, so an absent file is skipped, never
//!   failed.
//!
//! Deliberately out of scope: prose that asserts a *state* without naming a
//! path ("no old-tree file reaches..."). That form is not mechanically
//! checkable; it is covered by the move-story review question that asks
//! whether any prose near a diff now states something false.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

// ---------------------------------------------------------------------------
// The module universe: everything a citation may legitimately resolve to.
// ---------------------------------------------------------------------------

struct Universe {
    /// Full module paths, `::`-joined, plus every prefix ("archive::ops::plan",
    /// "archive::ops", "archive", ...).
    modules: HashSet<String>,
    /// Basenames of every `.rs` file in the tree ("plan.rs", "eval.rs", ...).
    file_names: HashSet<String>,
    /// Path roots that are not this tree's to resolve: std and friends,
    /// Cargo.toml dependencies, workspace members, primitives.
    external_roots: HashSet<String>,
}

impl Universe {
    fn build(root: &Path) -> Self {
        let mut modules = HashSet::new();
        let mut file_names = HashSet::new();

        for file in collect_rs_files(&root.join("src")) {
            if let Some(name) = file.file_name().and_then(|n| n.to_str()) {
                file_names.insert(name.to_string());
            }
            let module = module_path_of(&root.join("src"), &file);
            // Register the module and every prefix.
            let mut prefix = String::new();
            for seg in module.split("::").filter(|s| !s.is_empty()) {
                if !prefix.is_empty() {
                    prefix.push_str("::");
                }
                prefix.push_str(seg);
                modules.insert(prefix.clone());
            }
            // Inline `mod` declarations (test modules and nested submodules).
            let text = fs::read_to_string(&file)
                .unwrap_or_else(|e| panic!("failed to read {}: {}", file.display(), e));
            for inline in scan_inline_mods(&module, &text) {
                modules.insert(inline);
            }
        }
        for file in collect_rs_files(&root.join("tests")) {
            if let Some(name) = file.file_name().and_then(|n| n.to_str()) {
                file_names.insert(name.to_string());
            }
        }

        // Roots this tree does not own. `core` is deliberately absent even
        // though the standard library has a crate by that name: this tree has
        // its own `core` module, it is the most-cited module in the source,
        // and exempting the name would leave every citation of it unchecked.
        // The standard library is spelled `std` here throughout.
        let mut external_roots: HashSet<String> =
            ["std", "alloc", "str", "io", "proc_macro", "crate"]
                .iter()
                .map(|s| s.to_string())
                .collect();
        external_roots.extend(cargo_dependency_roots(&root.join("Cargo.toml")));

        Universe {
            modules,
            file_names,
            external_roots,
        }
    }

    #[cfg(test)]
    fn from_parts(modules: &[&str], file_names: &[&str], external_roots: &[&str]) -> Self {
        Universe {
            modules: modules.iter().map(|s| s.to_string()).collect(),
            file_names: file_names.iter().map(|s| s.to_string()).collect(),
            external_roots: external_roots.iter().map(|s| s.to_string()).collect(),
        }
    }

    /// A cited module path resolves when its segments are suffix-aligned with a
    /// known module — the citation may start below the crate root ("format" for
    /// `domain::format`) — with at most one trailing item segment, or two when
    /// the first of them is a type name (`Type::method`).
    fn resolves_module_path(&self, segments: &[String]) -> bool {
        for prefix_len in (1..=segments.len()).rev() {
            let prefix = &segments[..prefix_len];
            if !self.known_module_suffix(prefix) {
                continue;
            }
            let tail = &segments[prefix_len..];
            match tail.len() {
                0 | 1 => return true,
                2 if starts_uppercase(&tail[0]) => return true,
                _ => continue,
            }
        }
        false
    }

    fn known_module_suffix(&self, segments: &[String]) -> bool {
        let joined = segments.join("::");
        if self.modules.contains(&joined) {
            return true;
        }
        let suffix = format!("::{}", joined);
        self.modules.iter().any(|m| m.ends_with(&suffix))
    }
}

fn module_path_of(src: &Path, file: &Path) -> String {
    let rel = file.strip_prefix(src).expect("file under src");
    let mut segments: Vec<String> = rel
        .components()
        .map(|c| c.as_os_str().to_string_lossy().to_string())
        .collect();
    if let Some(last) = segments.last_mut() {
        *last = last.trim_end_matches(".rs").to_string();
    }
    if segments.last().map(String::as_str) == Some("mod") {
        segments.pop();
    }
    if segments.last().map(String::as_str) == Some("main") {
        segments.pop();
    }
    segments.join("::")
}

/// Find `mod name;` / `mod name {` declarations and return them as full paths
/// under `containing`. Nested inline modules register one level deep, which
/// covers the test-module citations that occur in practice.
fn scan_inline_mods(containing: &str, text: &str) -> Vec<String> {
    let mut out = Vec::new();
    for line in text.lines() {
        let mut rest = line.trim_start();
        if let Some(after) = rest.strip_prefix("pub") {
            rest = after.trim_start();
            if let Some(after) = rest.strip_prefix('(') {
                match after.find(')') {
                    Some(close) => rest = after[close + 1..].trim_start(),
                    None => continue,
                }
            }
        }
        let Some(after) = rest.strip_prefix("mod ") else {
            continue;
        };
        let name: String = after
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        if name.is_empty() {
            continue;
        }
        let tail = after[name.len()..].trim_start();
        if tail.starts_with(';') || tail.starts_with('{') {
            if containing.is_empty() {
                out.push(name);
            } else {
                out.push(format!("{}::{}", containing, name));
            }
        }
    }
    out
}

fn cargo_dependency_roots(cargo_toml: &Path) -> Vec<String> {
    let text = fs::read_to_string(cargo_toml)
        .unwrap_or_else(|e| panic!("failed to read {}: {}", cargo_toml.display(), e));
    let mut roots = Vec::new();
    let mut in_deps = false;
    for line in text.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            in_deps = line.contains("dependencies") || line == "[workspace]";
            continue;
        }
        if !in_deps {
            continue;
        }
        if let Some(eq) = line.find('=') {
            let name = line[..eq].trim();
            if !name.is_empty()
                && name
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
            {
                roots.push(name.replace('-', "_"));
            }
        }
        // Workspace members are quoted list entries, not `name = ...` lines.
        if line.contains('"') {
            for piece in line.split('"').skip(1).step_by(2) {
                let member = piece.rsplit('/').next().unwrap_or(piece);
                if member
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
                {
                    roots.push(member.replace('-', "_"));
                }
            }
        }
    }
    roots
}

// ---------------------------------------------------------------------------
// Citation extraction from comment text.
// ---------------------------------------------------------------------------

/// The comment portion of a line: everything after the first `//` that is not
/// part of a `://` URL scheme. Returns `None` for lines without a comment.
fn comment_text(line: &str) -> Option<&str> {
    let bytes = line.as_bytes();
    let mut i = 0;
    while i + 1 < bytes.len() {
        if bytes[i] == b'/' && bytes[i + 1] == b'/' && (i == 0 || bytes[i - 1] != b':') {
            return Some(&line[i + 2..]);
        }
        i += 1;
    }
    None
}

/// Module-path citations: maximal runs of identifier/`::` characters that
/// contain a `::`. Type paths (uppercase first segment), relative paths
/// (`self`/`super`), and external-crate paths are the caller's to filter.
fn module_citations(text: &str) -> Vec<Vec<String>> {
    let mut out = Vec::new();
    for run in split_runs(text, |c: char| {
        c.is_ascii_alphanumeric() || c == '_' || c == ':'
    }) {
        if !run.contains("::") {
            continue;
        }
        if run.starts_with("::") {
            // A fragment of a broken larger expression (`crate::<sibling>::inner`
            // splits at the placeholder) — not a citation.
            continue;
        }
        let segments: Vec<String> = run
            .split("::")
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();
        if segments.len() < 2 {
            continue;
        }
        if segments
            .iter()
            .any(|s| s.chars().next().is_some_and(|c| c.is_ascii_digit()))
        {
            continue;
        }
        out.push(segments);
    }
    out
}

/// File citations: tokens ending in `.rs`. URL fragments (containing `//`) are
/// skipped; `mod.rs` and `lib.rs` are structural names, not citations of a
/// particular file.
fn file_citations(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    for run in split_runs(text, |c: char| {
        c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '/' || c == '-'
    }) {
        if run.contains("//") || !run.ends_with(".rs") {
            continue;
        }
        let base = run.rsplit('/').next().unwrap_or(&run);
        let Some(stem) = base.strip_suffix(".rs") else {
            continue;
        };
        if stem.is_empty() {
            continue; // a bare `.rs` (as in "every `.rs` file") names no file.
        }
        if base == "mod.rs" || base == "lib.rs" {
            continue;
        }
        if !base
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        {
            continue;
        }
        out.push(base.to_string());
    }
    out
}

fn split_runs(text: &str, is_run_char: impl Fn(char) -> bool) -> Vec<String> {
    let mut runs = Vec::new();
    let mut current = String::new();
    for c in text.chars() {
        if is_run_char(c) {
            current.push(c);
        } else if !current.is_empty() {
            runs.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        runs.push(current);
    }
    runs
}

fn starts_uppercase(s: &str) -> bool {
    s.chars().next().is_some_and(|c| c.is_ascii_uppercase())
}

/// All citation violations in one file's text, as human-readable lines.
fn check_comment_citations(display_path: &str, text: &str, universe: &Universe) -> Vec<String> {
    let mut violations = Vec::new();
    for (idx, line) in text.lines().enumerate() {
        let Some(comment) = comment_text(line) else {
            continue;
        };
        for segments in module_citations(comment) {
            if starts_uppercase(&segments[0]) {
                continue; // Type::method / Type::VARIANT — not a module path.
            }
            if segments[0] == "self" || segments[0] == "super" {
                continue; // relative; not resolvable from here.
            }
            if universe.external_roots.contains(&segments[0]) {
                continue;
            }
            if !universe.resolves_module_path(&segments) {
                violations.push(format!(
                    "{}:{}: comment cites `{}`, which does not resolve",
                    display_path,
                    idx + 1,
                    segments.join("::"),
                ));
            }
        }
        for file in file_citations(comment) {
            if !universe.file_names.contains(&file) {
                violations.push(format!(
                    "{}:{}: comment cites `{}`, but no such file exists",
                    display_path,
                    idx + 1,
                    file,
                ));
            }
        }
    }
    violations
}

// ---------------------------------------------------------------------------
// CLAUDE.md inventory bullets.
// ---------------------------------------------------------------------------

/// Check one `Modules:` inventory line against the directory it enumerates.
/// Only plain lowercase names (optionally `.rs`-suffixed or slash-qualified)
/// are checked; backticked type names and flags are not module citations.
fn check_inventory_line(
    display_path: &str,
    line_no: usize,
    after_modules: &str,
    dir: &Path,
) -> Vec<String> {
    let mut violations = Vec::new();
    for token in after_modules.split('`').skip(1).step_by(2) {
        let plain = token.trim_end_matches(".rs");
        let is_name = !plain.is_empty()
            && plain
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '/');
        if !is_name {
            continue;
        }
        let exists = dir.join(format!("{}.rs", plain)).exists()
            || dir.join(plain).is_dir()
            || dir.join(token).exists();
        if !exists {
            violations.push(format!(
                "{}:{}: inventory names `{}`, absent from {}",
                display_path,
                line_no,
                token,
                dir.display(),
            ));
        }
    }
    violations
}

fn check_claude_md(root: &Path, path: &Path) -> Vec<String> {
    let Ok(text) = fs::read_to_string(path) else {
        return Vec::new(); // git-ignored file, absent in this checkout — skip.
    };
    let display = path
        .strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .to_string();
    let own_dir = path.parent().expect("CLAUDE.md has a parent").to_path_buf();
    let mut violations = Vec::new();
    for (idx, line) in text.lines().enumerate() {
        let Some(pos) = line.find("Modules: ") else {
            continue;
        };
        let after = &line[pos + "Modules: ".len()..];
        // A top-level bullet names its directory as **`X/`**; a nested
        // CLAUDE.md enumerates its own directory.
        let dir = bullet_directory(line).map(|d| root.join("src").join(d));
        let dir = dir.unwrap_or_else(|| own_dir.clone());
        if !dir.starts_with(root.join("src")) {
            continue; // top-level file with no directory marker on the line.
        }
        violations.extend(check_inventory_line(&display, idx + 1, after, &dir));
    }
    violations
}

fn bullet_directory(line: &str) -> Option<String> {
    let start = line.find("**`")? + 3;
    let rest = &line[start..];
    let end = rest.find("`**")?;
    let name = rest[..end].trim_end_matches('/');
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '/')
    {
        return None;
    }
    Some(name.to_string())
}

fn collect_rs_files(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    if root.is_dir() {
        collect_rs_files_inner(root, &mut out);
    }
    out.sort();
    out
}

fn collect_rs_files_inner(dir: &Path, out: &mut Vec<PathBuf>) {
    let mut entries: Vec<PathBuf> = fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("failed to read dir {}: {}", dir.display(), e))
        .map(|e| e.expect("dir entry read failed").path())
        .collect();
    entries.sort();
    for path in entries {
        if path.is_dir() {
            collect_rs_files_inner(&path, out);
        } else if path.extension().and_then(|s| s.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

// ---------------------------------------------------------------------------
// The checks over the real tree.
// ---------------------------------------------------------------------------

#[test]
fn comment_citations_resolve() {
    let root = repo_root();
    let universe = Universe::build(&root);
    let mut violations = Vec::new();
    for dir in ["src", "tests"] {
        for file in collect_rs_files(&root.join(dir)) {
            if file.file_name().and_then(|n| n.to_str()) == Some("citations.rs") {
                continue; // this file's own corpus is deliberately stale.
            }
            let text = fs::read_to_string(&file)
                .unwrap_or_else(|e| panic!("failed to read {}: {}", file.display(), e));
            let display = file
                .strip_prefix(&root)
                .unwrap_or(&file)
                .to_string_lossy()
                .to_string();
            violations.extend(check_comment_citations(&display, &text, &universe));
        }
    }
    assert!(
        violations.is_empty(),
        "comments cite code locations that no longer exist — update the prose \
         (or fix the scan's tolerance if a citation is genuinely legitimate):\n{}",
        violations.join("\n"),
    );
}

#[test]
fn claude_md_inventories_resolve() {
    let root = repo_root();
    let mut candidates = vec![root.join("CLAUDE.md")];
    if let Ok(entries) = fs::read_dir(root.join("src")) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                candidates.push(path.join("CLAUDE.md"));
            }
        }
    }
    let mut violations = Vec::new();
    for path in candidates {
        violations.extend(check_claude_md(&root, &path));
    }
    assert!(
        violations.is_empty(),
        "CLAUDE.md inventory bullets name modules that do not exist:\n{}",
        violations.join("\n"),
    );
}

// ---------------------------------------------------------------------------
// Self-tests over synthetic corpora — the scan's own red smoke: each asserts
// the checker fires (or stays quiet) on content constructed to exercise one
// rule, independent of the real tree's state.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod self_tests {
    use super::*;

    fn universe() -> Universe {
        Universe::from_parts(
            &[
                "expr",
                "expr::eval",
                "repo",
                "repo::source",
                "archive",
                "archive::ops",
                "archive::ops::plan",
                "facts",
                "facts::ops",
                "facts::ops::maintain",
                "facts::ops::maintain::tests",
                "domain",
                "domain::format",
            ],
            &["eval.rs", "source.rs", "plan.rs"],
            &["std", "serde", "rusqlite"],
        )
    }

    #[test]
    fn flags_stale_file_citation() {
        // `expr` exists as a module in this universe, but no `expr.rs` file
        // does — the file form of the citation is stale and must flag.
        let v = check_comment_citations("x.rs", "// types come from expr.rs\n", &universe());
        assert_eq!(
            v.len(),
            1,
            "a citation of a nonexistent file must flag: {:?}",
            v
        );
        assert!(v[0].contains("expr.rs"));
        let v = check_comment_citations("x.rs", "// types come from ghost.rs\n", &universe());
        assert_eq!(v.len(), 1, "a citation of a nonexistent file must flag");
        assert!(v[0].contains("ghost.rs"));
    }

    #[test]
    fn accepts_existing_file_citation() {
        let v = check_comment_citations("x.rs", "// see eval.rs for the parser\n", &universe());
        assert!(v.is_empty(), "{:?}", v);
    }

    #[test]
    fn skips_structural_and_url_file_names() {
        let text = "// a future lib.rs controls the surface; see https://host/x.rs\n";
        let v = check_comment_citations("x.rs", text, &universe());
        assert!(v.is_empty(), "{:?}", v);
    }

    #[test]
    fn flags_unresolvable_module_path() {
        let v = check_comment_citations("x.rs", "// from canon::fact_repo\n", &universe());
        assert_eq!(v.len(), 1, "an unknown root must flag: {:?}", v);
        let v = check_comment_citations("x.rs", "// via ops::cluster::plan_apply\n", &universe());
        assert_eq!(v.len(), 1, "a dead middle segment must flag: {:?}", v);
    }

    #[test]
    fn accepts_module_item_and_type_method_tails() {
        let text = "// repo::source::insert_destination, then archive::ops::plan\n\
                    // domain::format::FormatThing::render is a Type::method tail\n";
        let v = check_comment_citations("x.rs", text, &universe());
        assert!(v.is_empty(), "{:?}", v);
    }

    #[test]
    fn accepts_suffix_aligned_citation_below_crate_root() {
        let v = check_comment_citations("x.rs", "// see format::format_count()\n", &universe());
        assert!(v.is_empty(), "domain::format suffix must resolve: {:?}", v);
    }

    #[test]
    fn accepts_inline_test_module_paths() {
        let v = check_comment_citations(
            "x.rs",
            "// facts::ops::maintain::tests holds it\n",
            &universe(),
        );
        assert!(v.is_empty(), "{:?}", v);
    }

    #[test]
    fn skips_external_type_and_relative_paths() {
        let text = "// std::fs::read, serde::Deserialize, Self::new, self::helper\n";
        let v = check_comment_citations("x.rs", text, &universe());
        assert!(v.is_empty(), "{:?}", v);
    }

    #[test]
    fn only_comment_text_is_scanned() {
        let text = "let x = ghost::path::to_thing();\n// but comments are: ghost::path\n";
        let v = check_comment_citations("x.rs", text, &universe());
        assert_eq!(v.len(), 1, "code is not scanned, comments are: {:?}", v);
    }

    #[test]
    fn inline_mod_scan_finds_declarations() {
        let text = "pub mod alpha;\nmod tests {\n    fn f() {}\n}\npub(super) mod beta;\n// mod not_this in prose\n";
        let found = scan_inline_mods("root", text);
        assert_eq!(found, vec!["root::alpha", "root::tests", "root::beta"]);
    }

    #[test]
    fn inventory_flags_missing_and_accepts_present() {
        let dir = tempfile::tempdir().expect("tempdir");
        fs::write(dir.path().join("alpha.rs"), "").expect("write");
        fs::create_dir(dir.path().join("beta")).expect("mkdir");
        let v = check_inventory_line(
            "CLAUDE.md",
            1,
            "`alpha`, `beta`, `NotAModule`, `--flag`",
            dir.path(),
        );
        assert!(
            v.is_empty(),
            "present names and non-name tokens must pass: {:?}",
            v
        );
        let v = check_inventory_line("CLAUDE.md", 1, "`alpha`, `gamma`", dir.path());
        assert_eq!(v.len(), 1, "a missing module must flag: {:?}", v);
        assert!(v[0].contains("gamma"));
    }

    #[test]
    fn bullet_directory_extraction() {
        assert_eq!(
            bullet_directory("- **`repo/`** — ALL database access. Modules: `db`"),
            Some("repo".to_string()),
        );
        assert_eq!(bullet_directory("plain prose with Modules: `x`"), None);
    }
}
