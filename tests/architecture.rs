//! Deny-by-default architecture test.
//!
//! Enforces the layering rules in CLAUDE.md's "Layered Architecture" section by
//! statically scanning every `.rs` file under `src/`. Governing specs:
//! `.claude/specs/2026-08-07-architecture-test.md` (ADR: feature-first-structure,
//! Step-Zero Rule Tiers amendment) and `.claude/specs/2026-08-08-syn-scanner-migration.md`
//! (scanner rewritten on `syn`'s AST — see the visitor spec there for how use
//! trees, inline paths, and macro/attribute token streams are walked).

use std::fs;
use std::path::{Path, PathBuf};

use proc_macro2::{Spacing, TokenStream, TokenTree};
use syn::spanned::Spanned;
use syn::visit::{self, Visit};
use syn::{Attribute, ItemUse, Macro, Meta, Path as SynPath, UseTree};

// ============================================================================
// Layers
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Layer {
    Domain,
    Repo,
    Ops,
    Expr,
    Interface,
}

fn classify_layer(rel_path: &str) -> Layer {
    if rel_path.starts_with("domain/") {
        Layer::Domain
    } else if rel_path.starts_with("repo/") {
        Layer::Repo
    } else if rel_path.starts_with("ops/") {
        Layer::Ops
    } else if rel_path.starts_with("expr/") {
        Layer::Expr
    } else {
        Layer::Interface
    }
}

// ============================================================================
// Rules
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Rule {
    DomainNoOps,
    DomainNoRepo,
    DomainNoRusqlite,
    DomainNoStdFs,
    DomainNoStdIo,
    DomainNoStdioMacro,
    RepoNoOps,
    OpsNoStdioMacro,
    InterfaceRepoDataMovement,
    UpwardDependency,
}

impl Rule {
    fn label(self) -> &'static str {
        match self {
            Rule::DomainNoOps => "domain must not reference ops",
            Rule::DomainNoRepo => "domain must not reference repo",
            Rule::DomainNoRusqlite => "domain must not reference rusqlite",
            Rule::DomainNoStdFs => "domain must not reference std::fs",
            Rule::DomainNoStdIo => "domain must not reference std::io",
            Rule::DomainNoStdioMacro => "domain must not use stdio macros",
            Rule::RepoNoOps => "repo must not reference ops",
            Rule::OpsNoStdioMacro => "ops must not use stdio macros",
            Rule::InterfaceRepoDataMovement => {
                "interface must not move data through repo (plumbing only)"
            }
            Rule::UpwardDependency => "lower layer must not import an interface module",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Severity {
    Read,
    Write,
    TestOnly,
}

impl Severity {
    fn label(self) -> &'static str {
        match self {
            Severity::Read => "read",
            Severity::Write => "write",
            Severity::TestOnly => "test-only",
        }
    }
}

#[derive(Debug, Clone)]
struct Violation {
    file: String,
    line: usize,
    rule: Rule,
    reference: String,
}

// ============================================================================
// Tiers (ADR amendment: Step-Zero Rule Tiers, 2026-08-07)
// ============================================================================

/// Tier 1 — sanctioned plumbing: the interface may hold/open the database.
/// Normalized (crate:: stripped) form. Matches only `Rule::InterfaceRepoDataMovement`.
const TIER1_PLUMBING: &[&str] = &[
    "repo",
    "repo::Db",
    "repo::Connection",
    "repo::DbOptions",
    "repo::open_with_options",
    "repo::open_in_memory_for_test",
    "repo::db::Db",
    "repo::db::Connection",
    "repo::db::DbOptions",
    "repo::db::open_with_options",
    "repo::db::open_in_memory_for_test",
];

struct Tier2Entry {
    file: &'static str,
    rule: Rule,
}

/// Tier 2 — documented-exception allowlist, currently empty. Each entry's
/// file must have at least one matching violation, or it's stale (delete
/// it) — kept non-empty-capable for any future documented exception.
const TIER2: &[Tier2Entry] = &[];

struct Tier3Entry {
    file: &'static str,
    reference: &'static str,
    severity: Severity,
}

/// Tier 3 — R1 drift baseline, seeded empirically against the real tree (see the
/// spec's Seeding procedure). Matched both directions: an observed violation not
/// in this list fails (new drift refused); an entry with no observation fails
/// (repairing a site requires deleting its entry in the same commit).
const TIER3: &[Tier3Entry] = &[
    Tier3Entry {
        file: "apply.rs",
        reference: "repo::root::fetch_all",
        severity: Severity::Read,
    },
    Tier3Entry {
        file: "apply.rs",
        reference: "repo::fact::batch_fetch_key_for_sources",
        severity: Severity::Read,
    },
    Tier3Entry {
        file: "cluster.rs",
        reference: "repo::root::fetch_all",
        severity: Severity::Read,
    },
    Tier3Entry {
        file: "coverage.rs",
        reference: "repo::root::fetch_all",
        severity: Severity::Read,
    },
    Tier3Entry {
        file: "exclude.rs",
        reference: "repo::root::fetch_all",
        severity: Severity::Read,
    },
    Tier3Entry {
        file: "facts.rs",
        reference: "repo::root::fetch_all",
        severity: Severity::Read,
    },
    Tier3Entry {
        file: "import_facts.rs",
        reference: "repo::root::fetch_all",
        severity: Severity::Read,
    },
    Tier3Entry {
        file: "note.rs",
        reference: "repo::root::fetch_all",
        severity: Severity::Read,
    },
    Tier3Entry {
        file: "note.rs",
        reference: "repo::note::insert",
        severity: Severity::Write,
    },
    Tier3Entry {
        file: "roots.rs",
        reference: "repo::root::fetch_all",
        severity: Severity::Read,
    },
    Tier3Entry {
        file: "roots.rs",
        reference: "repo::root::fetch_file_counts",
        severity: Severity::Read,
    },
    Tier3Entry {
        file: "roots.rs",
        reference: "repo::root::set_comment",
        severity: Severity::Write,
    },
    Tier3Entry {
        file: "scan.rs",
        reference: "repo::root::fetch_all",
        severity: Severity::Read,
    },
    Tier3Entry {
        file: "scan.rs",
        reference: "repo::root::create",
        severity: Severity::Write,
    },
    Tier3Entry {
        file: "scan.rs",
        reference: "repo::root::update_last_scanned_at",
        severity: Severity::Write,
    },
    Tier3Entry {
        file: "trail.rs",
        reference: "repo::root::fetch_all",
        severity: Severity::Read,
    },
    Tier3Entry {
        file: "main.rs",
        reference: "repo::root::fetch_all",
        severity: Severity::Read,
    },
    Tier3Entry {
        file: "main.rs",
        reference: "repo::print_profile_summary",
        severity: Severity::Read,
    },
    Tier3Entry {
        file: "exclude.rs",
        reference: "repo::Db::from_connection",
        severity: Severity::TestOnly,
    },
    Tier3Entry {
        file: "survey.rs",
        reference: "repo::Db::from_connection",
        severity: Severity::TestOnly,
    },
    Tier3Entry {
        file: "scan.rs",
        reference: "repo::insert_test_root",
        severity: Severity::TestOnly,
    },
    Tier3Entry {
        file: "repo/note.rs",
        reference: "crate::ops::test_helpers::insert_note",
        severity: Severity::TestOnly,
    },
    Tier3Entry {
        file: "repo/note.rs",
        reference: "crate::ops::test_helpers::insert_root",
        severity: Severity::TestOnly,
    },
    Tier3Entry {
        file: "repo/note.rs",
        reference: "crate::ops::test_helpers::setup_test_db",
        severity: Severity::TestOnly,
    },
];

// ============================================================================
// Classification
// ============================================================================

fn is_or_under(path: &str, root: &str) -> bool {
    path == root || path.starts_with(&format!("{root}::"))
}

fn classify_reference(
    layer: Layer,
    raw_path: &str,
    interface_modules: &[String],
) -> Option<(Rule, String)> {
    let has_crate = raw_path.starts_with("crate::");
    let no_crate = raw_path.strip_prefix("crate::").unwrap_or(raw_path);

    match layer {
        Layer::Domain => {
            if is_or_under(no_crate, "ops") {
                return Some((Rule::DomainNoOps, raw_path.to_string()));
            }
            if is_or_under(no_crate, "repo") {
                return Some((Rule::DomainNoRepo, no_crate.to_string()));
            }
            if is_or_under(raw_path, "rusqlite") {
                return Some((Rule::DomainNoRusqlite, raw_path.to_string()));
            }
            if is_or_under(raw_path, "std::fs") {
                return Some((Rule::DomainNoStdFs, raw_path.to_string()));
            }
            if is_or_under(raw_path, "std::io") {
                return Some((Rule::DomainNoStdIo, raw_path.to_string()));
            }
        }
        Layer::Repo => {
            if is_or_under(no_crate, "ops") {
                return Some((Rule::RepoNoOps, raw_path.to_string()));
            }
        }
        Layer::Ops | Layer::Expr => {}
        Layer::Interface => {
            if is_or_under(no_crate, "repo") {
                return Some((Rule::InterfaceRepoDataMovement, no_crate.to_string()));
            }
        }
    }

    if layer != Layer::Interface && has_crate {
        for name in interface_modules {
            if is_or_under(no_crate, name) {
                return Some((Rule::UpwardDependency, raw_path.to_string()));
            }
        }
    }

    None
}

// ============================================================================
// syn-based scanning: use-tree expansion, inline path references, and
// macro/attribute token-stream walks. See the migration spec's "Visitor
// specification" and "Contract subtleties" sections for the rationale behind
// each of these.
// ============================================================================

const STDIO_MACROS: &[&str] = &["println", "eprintln", "print", "dbg"];

/// Expands a `UseTree` into `(path, renamed, glob)` leaves. `self` inside a
/// group maps to the parent path — a bare `Name { ident: "self" }` contributes
/// the prefix itself.
fn expand_use_tree(tree: &UseTree, prefix: &[String], out: &mut Vec<(String, bool, bool)>) {
    match tree {
        UseTree::Path(p) => {
            let mut next = prefix.to_vec();
            next.push(p.ident.to_string());
            expand_use_tree(&p.tree, &next, out);
        }
        UseTree::Name(n) => {
            if n.ident == "self" {
                out.push((prefix.join("::"), false, false));
            } else {
                let mut full = prefix.to_vec();
                full.push(n.ident.to_string());
                out.push((full.join("::"), false, false));
            }
        }
        UseTree::Rename(r) => {
            if r.ident == "self" {
                out.push((prefix.join("::"), true, false));
            } else {
                let mut full = prefix.to_vec();
                full.push(r.ident.to_string());
                out.push((full.join("::"), true, false));
            }
        }
        UseTree::Glob(_) => {
            out.push((prefix.join("::"), false, true));
        }
        UseTree::Group(g) => {
            for item in &g.items {
                expand_use_tree(item, prefix, out);
            }
        }
    }
}

fn path_to_string(path: &SynPath) -> String {
    path.segments
        .iter()
        .map(|seg| seg.ident.to_string())
        .collect::<Vec<_>>()
        .join("::")
}

struct ArchVisitor<'a> {
    layer: Layer,
    file_label: &'a str,
    interface_modules: &'a [String],
    violations: Vec<Violation>,
    error: Option<String>,
}

impl<'a> ArchVisitor<'a> {
    fn macro_rule(&self) -> Option<Rule> {
        match self.layer {
            Layer::Domain => Some(Rule::DomainNoStdioMacro),
            Layer::Ops => Some(Rule::OpsNoStdioMacro),
            _ => None,
        }
    }

    fn record(&mut self, line: usize, path: &str) {
        if let Some((rule, reference)) =
            classify_reference(self.layer, path, self.interface_modules)
        {
            self.violations.push(Violation {
                file: self.file_label.to_string(),
                line,
                rule,
                reference,
            });
        }
    }

    /// Walks a raw token stream (a macro body or an attribute's list tokens),
    /// reconstructing `ident (:: ident)*` runs and flagging nested stdio-macro
    /// invocations — the parity gap left by `syn` treating these as opaque
    /// tokens rather than parsed AST nodes.
    fn walk_tokens(&mut self, tokens: TokenStream) {
        if self.error.is_some() {
            return;
        }
        let trees: Vec<TokenTree> = tokens.into_iter().collect();
        let mut i = 0;
        while i < trees.len() {
            match &trees[i] {
                TokenTree::Ident(ident) => {
                    let line = ident.span().start().line;
                    let name = ident.to_string();

                    if STDIO_MACROS.contains(&name.as_str()) {
                        if let Some(TokenTree::Punct(p)) = trees.get(i + 1) {
                            if p.as_char() == '!' {
                                if let Some(rule) = self.macro_rule() {
                                    self.violations.push(Violation {
                                        file: self.file_label.to_string(),
                                        line,
                                        rule,
                                        reference: format!("{name}!"),
                                    });
                                }
                            }
                        }
                    }

                    let mut run = vec![name];
                    let mut j = i + 1;
                    loop {
                        let joint_coloncolon = matches!(
                            (trees.get(j), trees.get(j + 1)),
                            (Some(TokenTree::Punct(p1)), Some(TokenTree::Punct(p2)))
                                if p1.as_char() == ':'
                                    && p1.spacing() == Spacing::Joint
                                    && p2.as_char() == ':'
                        );
                        if !joint_coloncolon {
                            break;
                        }
                        if let Some(TokenTree::Ident(next)) = trees.get(j + 2) {
                            run.push(next.to_string());
                            j += 3;
                        } else {
                            break;
                        }
                    }
                    self.record(line, &run.join("::"));
                    i = j;
                }
                TokenTree::Group(g) => {
                    self.walk_tokens(g.stream());
                    i += 1;
                }
                _ => i += 1,
            }
        }
    }
}

impl<'a, 'ast> Visit<'ast> for ArchVisitor<'a> {
    fn visit_item_use(&mut self, item: &'ast ItemUse) {
        if self.error.is_some() {
            return;
        }
        let line = item.span().start().line;
        let mut leaves = Vec::new();
        expand_use_tree(&item.tree, &[], &mut leaves);
        for (path, renamed, glob) in &leaves {
            if let Some((rule, reference)) =
                classify_reference(self.layer, path, self.interface_modules)
            {
                if *renamed || *glob {
                    self.error = Some(format!(
                        "{}:{}: {} import of restricted path `{}` defeats scanning",
                        self.file_label,
                        line,
                        if *glob { "glob" } else { "aliased" },
                        path,
                    ));
                    return;
                }
                self.violations.push(Violation {
                    file: self.file_label.to_string(),
                    line,
                    rule,
                    reference,
                });
            }
        }
        // No default descent: use trees hold no syn::Path nodes to double-count,
        // and this replaces the old span-masking pass.
    }

    fn visit_path(&mut self, path: &'ast SynPath) {
        if self.error.is_some() {
            return;
        }
        let line = path.span().start().line;
        self.record(line, &path_to_string(path));
        visit::visit_path(self, path);
    }

    fn visit_macro(&mut self, mac: &'ast Macro) {
        if self.error.is_some() {
            return;
        }
        let line = mac.span().start().line;
        if let Some(last) = mac.path.segments.last() {
            let name = last.ident.to_string();
            if STDIO_MACROS.contains(&name.as_str()) {
                if let Some(rule) = self.macro_rule() {
                    self.violations.push(Violation {
                        file: self.file_label.to_string(),
                        line,
                        rule,
                        reference: format!("{name}!"),
                    });
                }
            }
        }
        self.walk_tokens(mac.tokens.clone());
        visit::visit_macro(self, mac);
    }

    fn visit_attribute(&mut self, attr: &'ast Attribute) {
        if self.error.is_some() {
            return;
        }
        if let Meta::List(list) = &attr.meta {
            self.walk_tokens(list.tokens.clone());
        }
        visit::visit_attribute(self, attr);
    }
}

// ============================================================================
// Per-file scan
// ============================================================================

fn scan_file(
    layer: Layer,
    file_label: &str,
    raw_text: &str,
    interface_modules: &[String],
) -> Result<Vec<Violation>, String> {
    let file = syn::parse_file(raw_text).map_err(|e| format!("{file_label}: {e}"))?;

    let mut visitor = ArchVisitor {
        layer,
        file_label,
        interface_modules,
        violations: Vec::new(),
        error: None,
    };
    visitor.visit_file(&file);

    match visitor.error {
        Some(e) => Err(e),
        None => Ok(visitor.violations),
    }
}

// ============================================================================
// File collection
// ============================================================================

fn collect_rs_files(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    collect_rs_files_inner(root, &mut out);
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

fn interface_module_names(src_root: &Path) -> Vec<String> {
    let mut names = Vec::new();
    let mut entries: Vec<PathBuf> = fs::read_dir(src_root)
        .expect("failed to read src/")
        .map(|e| e.expect("dir entry read failed").path())
        .collect();
    entries.sort();
    for path in entries {
        if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("rs") {
            if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                if stem != "main" {
                    names.push(stem.to_string());
                }
            }
        }
    }
    names
}

// ============================================================================
// Tier evaluation
// ============================================================================

struct ScanOutcome {
    new_drift: Vec<Violation>,
    unused_tier2: Vec<usize>,
    unused_tier3: Vec<usize>,
}

fn evaluate_violations(
    all: &[Violation],
    tier2: &[Tier2Entry],
    tier3: &[Tier3Entry],
) -> ScanOutcome {
    let mut tier2_hit = vec![false; tier2.len()];
    let mut tier3_hit = vec![false; tier3.len()];
    let mut new_drift = Vec::new();

    for v in all {
        if v.rule == Rule::InterfaceRepoDataMovement
            && TIER1_PLUMBING.contains(&v.reference.as_str())
        {
            continue;
        }

        let mut allowed = false;
        for (idx, t2) in tier2.iter().enumerate() {
            if t2.file == v.file && t2.rule == v.rule {
                tier2_hit[idx] = true;
                allowed = true;
            }
        }
        if !allowed {
            for (idx, t3) in tier3.iter().enumerate() {
                if t3.file == v.file && t3.reference == v.reference.as_str() {
                    tier3_hit[idx] = true;
                    allowed = true;
                }
            }
        }
        if !allowed {
            new_drift.push(v.clone());
        }
    }

    let unused_tier2 = tier2_hit
        .iter()
        .enumerate()
        .filter(|(_, hit)| !**hit)
        .map(|(i, _)| i)
        .collect();
    let unused_tier3 = tier3_hit
        .iter()
        .enumerate()
        .filter(|(_, hit)| !**hit)
        .map(|(i, _)| i)
        .collect();

    ScanOutcome {
        new_drift,
        unused_tier2,
        unused_tier3,
    }
}

// ============================================================================
// Main assertion test
// ============================================================================

#[test]
fn architecture_rules_hold() {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    let src_root = Path::new(&manifest_dir).join("src");
    let interface_modules = interface_module_names(&src_root);

    let files = collect_rs_files(&src_root);
    let mut all_violations = Vec::new();
    let mut audit_marker_count = 0usize;

    for path in &files {
        let raw_text = fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("failed to read {}: {}", path.display(), e));
        audit_marker_count += raw_text.matches("// AUDIT:").count();

        let rel_path = path.strip_prefix(&src_root).expect("file under src_root");
        let rel_str = rel_path.to_str().expect("utf8 path").replace('\\', "/");
        let layer = classify_layer(&rel_str);

        match scan_file(layer, &rel_str, &raw_text, &interface_modules) {
            Ok(violations) => all_violations.extend(violations),
            Err(e) => panic!("architecture scanner failure: {e}"),
        }
    }

    println!("// AUDIT: marker count = {audit_marker_count}");

    let outcome = evaluate_violations(&all_violations, TIER2, TIER3);

    if !outcome.new_drift.is_empty()
        || !outcome.unused_tier2.is_empty()
        || !outcome.unused_tier3.is_empty()
    {
        let mut msg = String::new();

        if !outcome.new_drift.is_empty() {
            msg.push_str("New drift (violations not covered by any tier):\n");
            let mut sorted = outcome.new_drift.clone();
            sorted.sort_by(|a, b| (a.file.as_str(), a.line).cmp(&(b.file.as_str(), b.line)));
            for v in &sorted {
                msg.push_str(&format!(
                    "  {}:{}  {}  \u{2014} {}\n",
                    v.file,
                    v.line,
                    v.reference,
                    v.rule.label()
                ));
            }
            msg.push_str(
                "  Fix: new code routes through ops (see CLAUDE.md Layered Architecture); \
                 an intentional repair deletes the matching baseline/allowlist entry in the \
                 same commit; a genuinely new sanctioned exception requires the architecture \
                 board.\n",
            );
        }

        if !outcome.unused_tier2.is_empty() {
            msg.push_str(
                "Stale Tier 2 (documented exception) entries with no matching violation:\n",
            );
            for idx in &outcome.unused_tier2 {
                let t2 = &TIER2[*idx];
                msg.push_str(&format!("  {} \u{2014} {}\n", t2.file, t2.rule.label()));
            }
            msg.push_str("  Fix: delete the entry \u{2014} the exception was repaired.\n");
        }

        if !outcome.unused_tier3.is_empty() {
            msg.push_str("Stale Tier 3 (drift baseline) entries with no matching violation:\n");
            for idx in &outcome.unused_tier3 {
                let t3 = &TIER3[*idx];
                msg.push_str(&format!(
                    "  {}  {}  ({})\n",
                    t3.file,
                    t3.reference,
                    t3.severity.label()
                ));
            }
            msg.push_str("  Fix: delete the entry in the same commit that repaired the site.\n");
        }

        panic!("{msg}");
    }
}

// ============================================================================
// Self-tests (August's spec)
// ============================================================================

#[cfg(test)]
mod self_tests {
    use super::*;

    #[test]
    fn strip_doc_comment_code_example_no_stdio_violation() {
        let text = "/// println!(\"x\");\nfn f() {}\n";
        let violations = scan_file(Layer::Ops, "synthetic.rs", text, &[]).unwrap();
        assert!(violations.is_empty(), "{violations:?}");
    }

    #[test]
    fn strip_string_literal_no_reference() {
        let text = "fn f() { let s = \"repo::root::fetch_all\"; }\n";
        let violations = scan_file(Layer::Interface, "synthetic.rs", text, &[]).unwrap();
        assert!(violations.is_empty(), "{violations:?}");
    }

    #[test]
    fn strip_nested_block_comment_parses() {
        let text = "/* outer /* inner */ still outer */\nfn f() {}\n";
        let result = scan_file(Layer::Domain, "synthetic.rs", text, &[]);
        assert!(result.is_ok(), "{result:?}");
    }

    #[test]
    fn strip_unterminated_block_comment_fails() {
        let text = "/* never closed\nfn f() {}\n";
        let result = scan_file(Layer::Domain, "synthetic.rs", text, &[]);
        assert!(result.is_err());
    }

    #[test]
    fn strip_unterminated_raw_string_fails() {
        let text = "fn f() { let s = r#\"never closed; }\n";
        let result = scan_file(Layer::Domain, "synthetic.rs", text, &[]);
        assert!(result.is_err());
    }

    #[test]
    fn strip_preserves_line_numbers() {
        let text = "/* line1\nline2\nline3 */\nfn f() { repo::root::fetch_all(); }\n";
        let violations = scan_file(Layer::Domain, "synthetic.rs", text, &[]).unwrap();
        let v = violations
            .iter()
            .find(|v| v.rule == Rule::DomainNoRepo)
            .expect("expected a violation");
        assert_eq!(v.line, 4);
    }

    #[test]
    fn use_expansion_group_and_self() {
        let text = "use crate::repo::{root, source::batch_fetch_by_roots};\nuse crate::repo::{self, Db};\n";
        let violations = scan_file(Layer::Interface, "synthetic.rs", text, &[]).unwrap();
        let refs: std::collections::BTreeSet<_> =
            violations.iter().map(|v| v.reference.clone()).collect();
        assert!(refs.contains("repo::root"), "{refs:?}");
        assert!(
            refs.contains("repo::source::batch_fetch_by_roots"),
            "{refs:?}"
        );
        assert!(refs.contains("repo::Db"), "{refs:?}");
        assert!(refs.contains("repo"), "{refs:?}");
    }

    #[test]
    fn evasion_refusal_alias() {
        let text = "use crate::repo as r;\n";
        let result = scan_file(Layer::Interface, "synthetic.rs", text, &[]);
        assert!(result.is_err());
    }

    #[test]
    fn evasion_refusal_glob() {
        let text = "use crate::repo::root::*;\n";
        let result = scan_file(Layer::Interface, "synthetic.rs", text, &[]);
        assert!(result.is_err());
    }

    #[test]
    fn tier_matching_both_directions() {
        let v = Violation {
            file: "roots.rs".to_string(),
            line: 1,
            rule: Rule::InterfaceRepoDataMovement,
            reference: "repo::root::fetch_all".to_string(),
        };

        let outcome = evaluate_violations(std::slice::from_ref(&v), &[], &[]);
        assert_eq!(outcome.new_drift.len(), 1);

        let tier3 = [Tier3Entry {
            file: "roots.rs",
            reference: "repo::root::fetch_all",
            severity: Severity::Read,
        }];
        let outcome = evaluate_violations(std::slice::from_ref(&v), &[], &tier3);
        assert!(outcome.new_drift.is_empty());
        assert!(outcome.unused_tier3.is_empty());

        let outcome = evaluate_violations(&[], &[], &tier3);
        assert_eq!(outcome.unused_tier3.len(), 1);
    }

    #[test]
    fn plumbing_list_needs_no_tier_entry() {
        let v = Violation {
            file: "roots.rs".to_string(),
            line: 1,
            rule: Rule::InterfaceRepoDataMovement,
            reference: "repo::Db".to_string(),
        };
        let outcome = evaluate_violations(&[v], &[], &[]);
        assert!(outcome.new_drift.is_empty());

        let v2 = Violation {
            file: "roots.rs".to_string(),
            line: 1,
            rule: Rule::InterfaceRepoDataMovement,
            reference: "repo::root::fetch_all".to_string(),
        };
        let outcome2 = evaluate_violations(&[v2], &[], &[]);
        assert_eq!(outcome2.new_drift.len(), 1);
    }

    #[test]
    fn word_boundary_excludes_prefixed_identifiers() {
        let text = "fn f() { let my_repo = 1; my_repo::thing(); }\n";
        let violations = scan_file(Layer::Interface, "synthetic.rs", text, &[]).unwrap();
        assert!(violations.is_empty(), "{violations:?}");
    }

    #[test]
    fn macro_body_token_walk_detects_nested_path_and_stdio_macro() {
        let text = "fn f() { my_macro!(repo::root::fetch_all(), println!(\"x\")); }\n";
        let violations = scan_file(Layer::Domain, "synthetic.rs", text, &[]).unwrap();
        assert!(
            violations
                .iter()
                .any(|v| v.rule == Rule::DomainNoRepo && v.reference == "repo::root::fetch_all"),
            "{violations:?}"
        );
        assert!(
            violations
                .iter()
                .any(|v| v.rule == Rule::DomainNoStdioMacro && v.reference == "println!"),
            "{violations:?}"
        );
    }
}
