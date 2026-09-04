//! Shared machinery for the tree-scanning guards.
//!
//! Rust integration tests are separate binaries and cannot `use` each other, so
//! a subdirectory module — compiled into each binary that declares it, never a
//! test binary of its own — is the sanctioned way to spell a walk once. Both
//! `citations.rs` and `laws.rs` read the same tree and the same set of
//! CLAUDE.md files; two copies of that walk would be the spoken-once doctrine
//! violated inside the instruments built to enforce it.
//!
//! What lives here is shared substrate: the file walk, the module `Universe` a
//! citation resolves against, and the CLAUDE.md enumeration. A guard's own
//! rule stays in the guard — the inventory-bullet check is citations' alone,
//! and the law matcher is the roster's.

#![allow(dead_code)] // each binary declaring this module uses a different part.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

pub fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

// ---------------------------------------------------------------------------
// The module universe: everything a citation may legitimately resolve to.
// ---------------------------------------------------------------------------

/// Path roots this tree does not own, so a citation starting at one of them
/// is nobody's to resolve here.
///
/// `core` is deliberately absent even though the standard library has a crate
/// by that name: this tree has its own `core` module, it is the most-cited
/// module in the source, and exempting the name would leave every citation of
/// it unchecked. The standard library is spelled `std` here throughout.
///
/// The primitive types are here for the same reason as `std`, with one extra:
/// a citation like `u8::MAX` names an associated constant of a type this tree
/// does not own and **cannot go stale** — there is no rename that could
/// falsify it. Flagging it taught the reader that the guard fires on correct
/// prose, which is how a guard becomes something people write around.
pub const EXTERNAL_ROOTS: &[&str] = &[
    "std",
    "alloc",
    "io",
    "proc_macro",
    "crate",
    // primitive types
    "u8",
    "u16",
    "u32",
    "u64",
    "u128",
    "usize",
    "i8",
    "i16",
    "i32",
    "i64",
    "i128",
    "isize",
    "f32",
    "f64",
    "bool",
    "char",
    "str",
];

pub struct Universe {
    /// Full module paths, `::`-joined, plus every prefix ("archive::ops::plan",
    /// "archive::ops", "archive", ...).
    pub modules: HashSet<String>,
    /// Basenames of every `.rs` file in the tree ("plan.rs", "filter.rs", ...).
    pub file_names: HashSet<String>,
    /// Repo-relative paths of the same files, slash-joined
    /// ("src/archive/ops/plan.rs", ...).
    pub file_paths: HashSet<String>,
    /// Path roots that are not this tree's to resolve: std and friends,
    /// Cargo.toml dependencies, workspace members, primitives.
    pub external_roots: HashSet<String>,
}

impl Universe {
    pub fn build(root: &Path) -> Self {
        let mut modules = HashSet::new();
        let mut file_names = HashSet::new();
        let mut file_paths = HashSet::new();

        for file in collect_rs_files(&root.join("src")) {
            if let Some(name) = file.file_name().and_then(|n| n.to_str()) {
                file_names.insert(name.to_string());
            }
            file_paths.insert(relative_slash_path(root, &file));
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
            file_paths.insert(relative_slash_path(root, &file));
        }

        let mut external_roots: HashSet<String> =
            EXTERNAL_ROOTS.iter().map(|s| s.to_string()).collect();
        external_roots.extend(cargo_dependency_roots(&root.join("Cargo.toml")));

        Universe {
            modules,
            file_names,
            file_paths,
            external_roots,
        }
    }

    pub fn from_parts(modules: &[&str], file_paths: &[&str], external_roots: &[&str]) -> Self {
        Universe {
            modules: modules.iter().map(|s| s.to_string()).collect(),
            file_names: file_paths
                .iter()
                .map(|p| p.rsplit('/').next().unwrap_or(p).to_string())
                .collect(),
            file_paths: file_paths.iter().map(|s| s.to_string()).collect(),
            external_roots: external_roots.iter().map(|s| s.to_string()).collect(),
        }
    }

    /// A cited file resolves when a bare basename is a file somewhere in the
    /// tree, or — for a citation that carries directories — when the whole
    /// path is suffix-aligned with a real one at a component boundary, so
    /// `retire/ops/frame.rs` and `src/retire/ops/frame.rs` both resolve while
    /// the same basename under another subsystem does not. (The failing form
    /// is spelled concretely in citations' `flags_full_path_citation_with_wrong_directory`,
    /// where it sits in a string literal the scan does not read — a comment
    /// here could not name it without failing the very check it describes.)
    pub fn resolves_file_citation(&self, cite: &str) -> bool {
        if !cite.contains('/') {
            return self.file_names.contains(cite);
        }
        let suffix = format!("/{}", cite);
        self.file_paths
            .iter()
            .any(|p| p == cite || p.ends_with(&suffix))
    }

    /// A cited module path resolves when its segments are suffix-aligned with a
    /// known module — the citation may start below the crate root ("format" for
    /// `domain::format`) — with at most one trailing item segment, or two when
    /// the first of them is a type name (`Type::method`).
    pub fn resolves_module_path(&self, segments: &[String]) -> bool {
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

    pub fn known_module_suffix(&self, segments: &[String]) -> bool {
        let joined = segments.join("::");
        if self.modules.contains(&joined) {
            return true;
        }
        let suffix = format!("::{}", joined);
        self.modules.iter().any(|m| m.ends_with(&suffix))
    }
}

pub fn starts_uppercase(s: &str) -> bool {
    s.chars().next().is_some_and(|c| c.is_ascii_uppercase())
}

pub fn relative_slash_path(root: &Path, file: &Path) -> String {
    file.strip_prefix(root)
        .unwrap_or(file)
        .components()
        .map(|c| c.as_os_str().to_string_lossy().to_string())
        .collect::<Vec<_>>()
        .join("/")
}

pub fn module_path_of(src: &Path, file: &Path) -> String {
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
pub fn scan_inline_mods(containing: &str, text: &str) -> Vec<String> {
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

pub fn cargo_dependency_roots(cargo_toml: &Path) -> Vec<String> {
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

pub fn collect_rs_files(root: &Path) -> Vec<PathBuf> {
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

/// Every CLAUDE.md the guards read: the root file, then one per `src/`
/// subdirectory. These files are tracked, and every `src/` subdirectory owes
/// one — a directory that documents nothing is a subsystem no reader can find
/// their way into — so a caller reads each of these and fails on an absent
/// one. Which register files must exist is asserted in one place, the ceiling
/// guard's both-directions match; the callers here only need to not paper over
/// an absence when they meet it.
pub fn claude_md_paths(root: &Path) -> Vec<PathBuf> {
    let mut candidates = vec![root.join("CLAUDE.md")];
    if let Ok(entries) = fs::read_dir(root.join("src")) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                candidates.push(path.join("CLAUDE.md"));
            }
        }
    }
    candidates
}
