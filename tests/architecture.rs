//! Deny-by-default architecture test.
//!
//! Statically scans every `.rs` file under `src/` and asserts the project's
//! layering rules: domain stays pure, the interface never moves data through
//! repo, and references between feature subsystems stay on declared surfaces.
//! Three rule tiers (sanctioned plumbing, documented exceptions, a drift
//! baseline matched both directions) keep the net deny-by-default. The
//! scanner is built on `syn`'s AST; use trees, inline paths, and
//! macro/attribute token streams are all walked.

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

/// Old-tree stratum directory names — excluded when discovering subsystem
/// directories (a subsystem is any top-level `src/` directory that isn't one
/// of these and isn't `core`).
const OLD_LAYER_DIRS: &[&str] = &["domain", "repo", "ops", "expr"];

/// Stratum front-door module names inside a subsystem. For the
/// sibling-boundary rule these are never "declared surface": a sibling
/// reference to `crate::<sibling>::<stratum>` hands out the internals'
/// front door, so it is refused like any deeper reach — a sibling's
/// surface is its barrel's *item* re-exports, never a stratum module
/// (ADR amendment: Sibling-Boundary Rule, 2026-08-08).
const STRATUM_FRONT_DOORS: &[&str] = &["domain", "repo", "ops", "cli"];

fn classify_layer(rel_path: &str) -> Layer {
    if let Some(rest) = rel_path.strip_prefix("core/") {
        return classify_subsystem_stratum(rest);
    }
    if let Some(rest) = rel_path.strip_prefix("retire/") {
        return classify_subsystem_stratum(rest);
    }
    if let Some(rest) = rel_path.strip_prefix("story/") {
        return classify_subsystem_stratum(rest);
    }
    if let Some(rest) = rel_path.strip_prefix("trail/") {
        return classify_subsystem_stratum(rest);
    }
    if let Some(rest) = rel_path.strip_prefix("sweep/") {
        return classify_subsystem_stratum(rest);
    }
    if let Some(rest) = rel_path.strip_prefix("survey/") {
        return classify_subsystem_stratum(rest);
    }
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

/// Classifies a path already stripped of its `core/`/`<subsystem>/` prefix
/// into a stratum. `mod.rs`/`cli.rs`-style files with no substratum prefix
/// (barrels, command entry points) default to `Interface` — the same
/// fallback the old flat tree already used for anything unrecognized, and
/// correct here too: a barrel has no `use crate::` logic of its own to
/// misclassify, and a subsystem's `cli.rs` genuinely is its interface layer.
fn classify_subsystem_stratum(rest: &str) -> Layer {
    if rest.starts_with("domain/") || rest == "domain.rs" {
        Layer::Domain
    } else if rest.starts_with("repo/") || rest == "repo.rs" {
        Layer::Repo
    } else if rest.starts_with("ops/") || rest == "ops.rs" {
        Layer::Ops
    } else {
        Layer::Interface
    }
}

// ============================================================================
// Subsystem home (feature-first tree) — the sibling-boundary rule's basis.
// ============================================================================

/// Where a file lives relative to the feature-first migration: the shared
/// hub (`core/`), a leaf subsystem (`retire/`, and more as the streak adds
/// them), or the pre-migration flat tree (everything not yet moved —
/// referencing it is legal staging, never drift, per the migration's
/// membership-is-criterial/movement-is-staged principle).
#[derive(Debug, Clone, PartialEq, Eq)]
enum Home {
    Core,
    Subsystem(String),
    OldTree,
}

fn top_level_dir(rel_path: &str) -> Option<&str> {
    rel_path.split_once('/').map(|(first, _)| first)
}

fn classify_home(rel_path: &str) -> Home {
    match top_level_dir(rel_path) {
        Some("core") => Home::Core,
        Some(dir) if !OLD_LAYER_DIRS.contains(&dir) => Home::Subsystem(dir.to_string()),
        _ => Home::OldTree,
    }
}

/// Discovers subsystem directory names by listing `src/`'s top-level
/// directories and excluding `core` and the old stratum dirs — mirrors
/// `interface_module_names`'s dynamic-enumeration style rather than a
/// hardcoded list, so a new subsystem needs no scanner edit to be
/// recognized.
fn subsystem_dir_names(src_root: &Path) -> Vec<String> {
    let mut names = Vec::new();
    let mut entries: Vec<PathBuf> = fs::read_dir(src_root)
        .expect("failed to read src/")
        .map(|e| e.expect("dir entry read failed").path())
        .collect();
    entries.sort();
    for path in entries {
        if path.is_dir() {
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                if name != "core" && !OLD_LAYER_DIRS.contains(&name) {
                    names.push(name.to_string());
                }
            }
        }
    }
    names
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
    CoreReferencesSubsystem,
    SubsystemSiblingInternalReach,
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
            Rule::CoreReferencesSubsystem => {
                "core must not reference a subsystem (hub must not depend on a spoke)"
            }
            Rule::SubsystemSiblingInternalReach => {
                "a subsystem must not reach past a sibling subsystem's one-segment public \
                 surface into its internals"
            }
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
        file: "retire/cli.rs",
        reference: "repo::root::fetch_all",
        severity: Severity::Read,
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
        file: "story/cli.rs",
        reference: "repo::root::fetch_all",
        severity: Severity::Read,
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
        file: "trail/cli.rs",
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
        file: "survey/cli.rs",
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
    home: &Home,
    raw_path: &str,
    interface_modules: &[String],
    subsystem_names: &[String],
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

    // ------------------------------------------------------------------
    // Subsystem-boundary rule (feature-first migration): orthogonal to
    // stratum (Layer) — applies to a reference regardless of whether the
    // referencing file is domain/repo/ops within its home. Old-tree
    // (`Home::OldTree`) files are unrestricted here — reaching into a
    // subsystem's public surface from the pre-migration tree (e.g. `main.rs`
    // dispatching into `retire::`) is legal staging, and no old-tree file
    // currently reaches a subsystem's internals (nothing to guard yet; add
    // a rule when a real case exists, per record-on-sight/extract-on-
    // second-use — not speculatively).
    // ------------------------------------------------------------------
    if has_crate {
        if let Some(ref_root) = no_crate.split("::").next() {
            if subsystem_names.iter().any(|s| s == ref_root) {
                match home {
                    Home::Core => {
                        return Some((Rule::CoreReferencesSubsystem, raw_path.to_string()));
                    }
                    Home::Subsystem(own) if own != ref_root => {
                        // A reference exactly one segment past the sibling's
                        // root (`crate::<sibling>::item`) is the sibling's
                        // deliberate public surface (its `mod.rs` re-export
                        // list) — legal, unless the segment names a stratum
                        // front door (`domain`/`repo`/`ops`/`cli`): those are
                        // internals by construction, whatever the barrel's
                        // visibility says. Two or more segments past the root
                        // (`crate::<sibling>::inner::item`) reaches past the
                        // surface into internals — refused.
                        let mut segs = no_crate.split("::");
                        segs.next();
                        let second = segs.next();
                        let deeper = segs.next().is_some();
                        let names_front_door =
                            second.is_some_and(|s| STRATUM_FRONT_DOORS.contains(&s));
                        if deeper || names_front_door {
                            return Some((
                                Rule::SubsystemSiblingInternalReach,
                                raw_path.to_string(),
                            ));
                        }
                    }
                    Home::Subsystem(_) | Home::OldTree => {}
                }
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
    home: &'a Home,
    file_label: &'a str,
    interface_modules: &'a [String],
    subsystem_names: &'a [String],
    violations: Vec<Violation>,
    error: Option<String>,
}

impl<'a> ArchVisitor<'a> {
    /// Sibling-boundary import refusals (deny-by-default): a `use` that
    /// takes a *module handle* on a sibling subsystem — its bare root
    /// (any form: plain, `{self, ..}`, aliased, glob), or an aliased/glob
    /// import at surface depth — lets every later unprefixed path escape
    /// the depth scan that enforces the sibling-boundary rule. Refused
    /// rather than skipped, exactly like the alias/glob refusal on the
    /// layer rules (ADR amendment: Sibling-Boundary Rule, 2026-08-08).
    /// Plain item imports at surface depth (`use crate::<sibling>::item;`)
    /// stay legal; stratum front doors are handled by `classify_reference`.
    fn sibling_import_refusal(&self, path: &str, renamed: bool, glob: bool) -> Option<String> {
        let Home::Subsystem(own) = self.home else {
            return None;
        };
        let no_crate = path.strip_prefix("crate::")?;
        let mut segs = no_crate.split("::");
        let root = segs.next()?;
        if root == own || !self.subsystem_names.iter().any(|s| s == root) {
            return None;
        }
        let depth_past_root = segs.count();
        if depth_past_root == 0 {
            return Some(format!(
                "import at sibling subsystem root `{no_crate}` defeats sibling-boundary \
                 scanning — name items through the sibling's one-segment surface with \
                 full `crate::` paths",
            ));
        }
        if (renamed || glob) && depth_past_root == 1 {
            return Some(format!(
                "{} import of sibling surface path `{no_crate}` defeats sibling-boundary \
                 scanning",
                if glob { "glob" } else { "aliased" },
            ));
        }
        None
    }

    fn macro_rule(&self) -> Option<Rule> {
        match self.layer {
            Layer::Domain => Some(Rule::DomainNoStdioMacro),
            Layer::Ops => Some(Rule::OpsNoStdioMacro),
            _ => None,
        }
    }

    fn record(&mut self, line: usize, path: &str) {
        if let Some((rule, reference)) = classify_reference(
            self.layer,
            self.home,
            path,
            self.interface_modules,
            self.subsystem_names,
        ) {
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
            if let Some(err) = self.sibling_import_refusal(path, *renamed, *glob) {
                self.error = Some(format!("{}:{}: {}", self.file_label, line, err));
                return;
            }
            if let Some((rule, reference)) = classify_reference(
                self.layer,
                self.home,
                path,
                self.interface_modules,
                self.subsystem_names,
            ) {
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
    home: &Home,
    file_label: &str,
    raw_text: &str,
    interface_modules: &[String],
    subsystem_names: &[String],
) -> Result<Vec<Violation>, String> {
    let file = syn::parse_file(raw_text).map_err(|e| format!("{file_label}: {e}"))?;

    let mut visitor = ArchVisitor {
        layer,
        home,
        file_label,
        interface_modules,
        subsystem_names,
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
    let subsystem_names = subsystem_dir_names(&src_root);
    // A name can be both a flat interface file and a subsystem directory
    // during the coexistence window before the interface stratum itself
    // moves (`trail.rs` alongside `trail/`, until the CLI split retires the
    // flat file) — the subsystem-boundary rule already governs references
    // into the directory, so the name must not also trip the flat-file
    // upward-dependency check below.
    let interface_modules: Vec<String> = interface_module_names(&src_root)
        .into_iter()
        .filter(|n| !subsystem_names.contains(n))
        .collect();

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
        let home = classify_home(&rel_str);

        match scan_file(
            layer,
            &home,
            &rel_str,
            &raw_text,
            &interface_modules,
            &subsystem_names,
        ) {
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
        let violations =
            scan_file(Layer::Ops, &Home::OldTree, "synthetic.rs", text, &[], &[]).unwrap();
        assert!(violations.is_empty(), "{violations:?}");
    }

    #[test]
    fn strip_string_literal_no_reference() {
        let text = "fn f() { let s = \"repo::root::fetch_all\"; }\n";
        let violations = scan_file(
            Layer::Interface,
            &Home::OldTree,
            "synthetic.rs",
            text,
            &[],
            &[],
        )
        .unwrap();
        assert!(violations.is_empty(), "{violations:?}");
    }

    #[test]
    fn strip_nested_block_comment_parses() {
        let text = "/* outer /* inner */ still outer */\nfn f() {}\n";
        let result = scan_file(
            Layer::Domain,
            &Home::OldTree,
            "synthetic.rs",
            text,
            &[],
            &[],
        );
        assert!(result.is_ok(), "{result:?}");
    }

    #[test]
    fn strip_unterminated_block_comment_fails() {
        let text = "/* never closed\nfn f() {}\n";
        let result = scan_file(
            Layer::Domain,
            &Home::OldTree,
            "synthetic.rs",
            text,
            &[],
            &[],
        );
        assert!(result.is_err());
    }

    #[test]
    fn strip_unterminated_raw_string_fails() {
        let text = "fn f() { let s = r#\"never closed; }\n";
        let result = scan_file(
            Layer::Domain,
            &Home::OldTree,
            "synthetic.rs",
            text,
            &[],
            &[],
        );
        assert!(result.is_err());
    }

    #[test]
    fn strip_preserves_line_numbers() {
        let text = "/* line1\nline2\nline3 */\nfn f() { repo::root::fetch_all(); }\n";
        let violations = scan_file(
            Layer::Domain,
            &Home::OldTree,
            "synthetic.rs",
            text,
            &[],
            &[],
        )
        .unwrap();
        let v = violations
            .iter()
            .find(|v| v.rule == Rule::DomainNoRepo)
            .expect("expected a violation");
        assert_eq!(v.line, 4);
    }

    #[test]
    fn use_expansion_group_and_self() {
        let text = "use crate::repo::{root, source::batch_fetch_by_roots};\nuse crate::repo::{self, Db};\n";
        let violations = scan_file(
            Layer::Interface,
            &Home::OldTree,
            "synthetic.rs",
            text,
            &[],
            &[],
        )
        .unwrap();
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
        let result = scan_file(
            Layer::Interface,
            &Home::OldTree,
            "synthetic.rs",
            text,
            &[],
            &[],
        );
        assert!(result.is_err());
    }

    #[test]
    fn evasion_refusal_glob() {
        let text = "use crate::repo::root::*;\n";
        let result = scan_file(
            Layer::Interface,
            &Home::OldTree,
            "synthetic.rs",
            text,
            &[],
            &[],
        );
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
        let violations = scan_file(
            Layer::Interface,
            &Home::OldTree,
            "synthetic.rs",
            text,
            &[],
            &[],
        )
        .unwrap();
        assert!(violations.is_empty(), "{violations:?}");
    }

    #[test]
    fn macro_body_token_walk_detects_nested_path_and_stdio_macro() {
        let text = "fn f() { my_macro!(repo::root::fetch_all(), println!(\"x\")); }\n";
        let violations = scan_file(
            Layer::Domain,
            &Home::OldTree,
            "synthetic.rs",
            text,
            &[],
            &[],
        )
        .unwrap();
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

    // ========================================================================
    // Subsystem-boundary rule (feature-first migration). Synthetic fixtures
    // pin the rule shape itself, independent of which directories are
    // physically populated. `retire/` exists for real; `story/` has not yet
    // moved — so the real `retire -> story::report_over` reference is still
    // old-tree access (unrestricted), not yet the sibling-boundary case
    // these fixtures pin. That becomes the non-synthetic case once `story/`
    // physically moves.
    // ========================================================================

    #[test]
    fn core_must_not_reference_a_subsystem() {
        let text = "use crate::retire::something;\n";
        let subsystems = vec!["retire".to_string()];
        let violations = scan_file(
            Layer::Domain,
            &Home::Core,
            "core/domain/resolution.rs",
            text,
            &[],
            &subsystems,
        )
        .unwrap();
        assert!(
            violations
                .iter()
                .any(|v| v.rule == Rule::CoreReferencesSubsystem),
            "{violations:?}"
        );
    }

    #[test]
    fn subsystem_may_reference_sibling_public_surface_one_segment_deep() {
        // Stands in for the real `ops/retire/ceremony.rs` -> `ops::story::report_over`
        // dependency (traced 2026-08-08, epic Working register) — retire depends on
        // story's *finished result* through its declared one-segment surface, never
        // on story's internals.
        let text = "use crate::story::report_over;\n";
        let subsystems = vec!["retire".to_string(), "story".to_string()];
        let violations = scan_file(
            Layer::Ops,
            &Home::Subsystem("retire".to_string()),
            "retire/ops/ceremony.rs",
            text,
            &[],
            &subsystems,
        )
        .unwrap();
        assert!(violations.is_empty(), "{violations:?}");
    }

    #[test]
    fn retire_may_reference_trail_public_surface_one_segment_deep() {
        // Stands in for the real `retire/ops/compile.rs` -> `trail::{compute_trail,
        // TrailParams, TrailResult, TrailView, TimelineEvent}` dependency — the
        // book's timeline page composes over trail's finished report through its
        // declared one-segment barrel, never trail's internals.
        let text =
            "use crate::trail::{compute_trail, TrailParams, TrailResult, TrailView, TimelineEvent};\n";
        let subsystems = vec!["retire".to_string(), "trail".to_string()];
        let violations = scan_file(
            Layer::Ops,
            &Home::Subsystem("retire".to_string()),
            "retire/ops/compile.rs",
            text,
            &[],
            &subsystems,
        )
        .unwrap();
        assert!(violations.is_empty(), "{violations:?}");
    }

    #[test]
    fn subsystem_must_not_reference_sibling_internals_two_segments_deep() {
        let text = "use crate::story::domain::internal_helper;\n";
        let subsystems = vec!["retire".to_string(), "story".to_string()];
        let violations = scan_file(
            Layer::Ops,
            &Home::Subsystem("retire".to_string()),
            "retire/ops/ceremony.rs",
            text,
            &[],
            &subsystems,
        )
        .unwrap();
        assert!(
            violations
                .iter()
                .any(|v| v.rule == Rule::SubsystemSiblingInternalReach),
            "{violations:?}"
        );
    }

    #[test]
    fn subsystem_may_reference_core_at_any_depth() {
        let text = "use crate::core::domain::resolution::classify_present;\n";
        let subsystems = vec!["retire".to_string()];
        let violations = scan_file(
            Layer::Domain,
            &Home::Subsystem("retire".to_string()),
            "retire/domain.rs",
            text,
            &[],
            &subsystems,
        )
        .unwrap();
        assert!(violations.is_empty(), "{violations:?}");
    }

    #[test]
    fn subsystem_may_reference_its_own_internals_at_any_depth() {
        let text = "use crate::retire::domain::Readiness;\n";
        let subsystems = vec!["retire".to_string()];
        let violations = scan_file(
            Layer::Ops,
            &Home::Subsystem("retire".to_string()),
            "retire/ops/ceremony.rs",
            text,
            &[],
            &subsystems,
        )
        .unwrap();
        assert!(violations.is_empty(), "{violations:?}");
    }

    #[test]
    fn sibling_root_module_import_is_refused() {
        // A bare module handle on the sibling makes every later unprefixed
        // path (`story::domain::x`) invisible to the depth scan — refused.
        let text = "use crate::story;\n";
        let subsystems = vec!["retire".to_string(), "story".to_string()];
        let result = scan_file(
            Layer::Ops,
            &Home::Subsystem("retire".to_string()),
            "retire/ops/ceremony.rs",
            text,
            &[],
            &subsystems,
        );
        assert!(result.is_err(), "{result:?}");
    }

    #[test]
    fn sibling_root_glob_import_is_refused() {
        let text = "use crate::story::*;\n";
        let subsystems = vec!["retire".to_string(), "story".to_string()];
        let result = scan_file(
            Layer::Ops,
            &Home::Subsystem("retire".to_string()),
            "retire/ops/ceremony.rs",
            text,
            &[],
            &subsystems,
        );
        assert!(result.is_err(), "{result:?}");
    }

    #[test]
    fn sibling_surface_alias_import_is_refused() {
        // Aliasing at surface depth can't be told apart from a module
        // alias, so it's refused whole — a legal consumer imports the
        // item unaliased.
        let text = "use crate::story::report_over as r;\n";
        let subsystems = vec!["retire".to_string(), "story".to_string()];
        let result = scan_file(
            Layer::Ops,
            &Home::Subsystem("retire".to_string()),
            "retire/ops/ceremony.rs",
            text,
            &[],
            &subsystems,
        );
        assert!(result.is_err(), "{result:?}");
    }

    #[test]
    fn sibling_surface_glob_import_is_refused() {
        let text = "use crate::story::helpers::*;\n";
        let subsystems = vec!["retire".to_string(), "story".to_string()];
        let result = scan_file(
            Layer::Ops,
            &Home::Subsystem("retire".to_string()),
            "retire/ops/ceremony.rs",
            text,
            &[],
            &subsystems,
        );
        assert!(result.is_err(), "{result:?}");
    }

    #[test]
    fn sibling_stratum_front_door_is_refused_at_one_segment() {
        // `crate::story::domain` is one segment past the root, but a
        // stratum front door is internals by construction — a violation,
        // not surface.
        let text = "use crate::story::domain;\n";
        let subsystems = vec!["retire".to_string(), "story".to_string()];
        let violations = scan_file(
            Layer::Ops,
            &Home::Subsystem("retire".to_string()),
            "retire/ops/ceremony.rs",
            text,
            &[],
            &subsystems,
        )
        .unwrap();
        assert!(
            violations
                .iter()
                .any(|v| v.rule == Rule::SubsystemSiblingInternalReach),
            "{violations:?}"
        );
    }

    #[test]
    fn old_tree_may_reference_a_subsystems_public_surface() {
        // main.rs dispatching into `retire::` command entry points — legal staging,
        // unrestricted, same as any other Interface-layer reference today.
        let text = "use crate::retire::retire;\n";
        let subsystems = vec!["retire".to_string()];
        let violations = scan_file(
            Layer::Interface,
            &Home::OldTree,
            "main.rs",
            text,
            &[],
            &subsystems,
        )
        .unwrap();
        assert!(violations.is_empty(), "{violations:?}");
    }

    #[test]
    fn classify_layer_recognizes_core_retire_and_story_strata() {
        assert_eq!(classify_layer("core/domain/resolution.rs"), Layer::Domain);
        assert_eq!(classify_layer("core/repo/decision.rs"), Layer::Repo);
        assert_eq!(classify_layer("core/ops/root_story.rs"), Layer::Ops);
        assert_eq!(classify_layer("core/mod.rs"), Layer::Interface);
        assert_eq!(classify_layer("retire/domain.rs"), Layer::Domain);
        assert_eq!(classify_layer("retire/ops/ceremony.rs"), Layer::Ops);
        assert_eq!(classify_layer("retire/cli.rs"), Layer::Interface);
        assert_eq!(classify_layer("story/domain/place.rs"), Layer::Domain);
        assert_eq!(classify_layer("story/ops/report.rs"), Layer::Ops);
        assert_eq!(classify_layer("story/mod.rs"), Layer::Interface);
        // A bare `repo.rs` (no `repo/` directory) classifies as Repo, same as
        // the single-file `domain.rs`/`ops.rs` pattern above.
        assert_eq!(classify_layer("trail/repo.rs"), Layer::Repo);
    }

    #[test]
    fn classify_home_recognizes_core_subsystem_and_old_tree() {
        assert_eq!(classify_home("core/domain/resolution.rs"), Home::Core);
        assert_eq!(
            classify_home("retire/ops/ceremony.rs"),
            Home::Subsystem("retire".to_string())
        );
        assert_eq!(classify_home("domain/retire.rs"), Home::OldTree);
        assert_eq!(classify_home("main.rs"), Home::OldTree);
    }

    // ========================================================================
    // Barrel-surface sealing — every subsystem's public surface is pinned
    // ========================================================================

    /// Each subsystem's complete public surface: the `pub use` items its
    /// front door re-exports — CLI entry points, the finished-result items
    /// siblings consume, and pub-field/variant types riding for
    /// crate-readiness (a future crate boundary can't expose a public field
    /// of a private type). Changing a barrel means editing its pin here in
    /// the same commit: a surface change is a deliberate, reviewable act.
    const SUBSYSTEM_BARREL_ITEMS: &[(&str, &[&str])] = &[
        (
            "retire",
            &[
                "retire",
                "retired",
                "find_retirement_covering_path",
                "RetiredScope",
            ],
        ),
        (
            "story",
            &[
                "story",
                "ActDecision",
                "ActGroup",
                "ReasonSummary",
                "aggregate_locations",
                "LocationAggregate",
                "LocationCount",
                "PlaceStanding",
                "StoryParams",
                "StoryPlace",
                "file_noun",
                "fmt_locations",
                "reference_place_lines",
                "report_over",
                "StoryReport",
            ],
        ),
        (
            "trail",
            &[
                "run",
                "run_show",
                "TrailArgs",
                "RowAspect",
                "DayGroup",
                "DayRollup",
                "FateLine",
                "TimelineEvent",
                "WhenValue",
                "compute_trail",
                "TrailParams",
                "TrailResult",
                "TrailView",
                "ArrivalRollup",
                "ExtractionRollup",
                "RearrangementRollup",
            ],
        ),
        ("sweep", &["run"]),
        // main.rs reaches DetailMode/SurveyOptions/run for the survey
        // command; the contentless-law canary reaches the rest.
        (
            "survey",
            &[
                "DetailMode",
                "ObjectIndex",
                "SurveyOptions",
                "SurveyOutcome",
                "SurveyParams",
                "compute_survey",
                "run",
            ],
        ),
    ];

    #[test]
    fn subsystem_barrels_seal_to_their_pinned_surfaces() {
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
        let src_root = Path::new(&manifest_dir).join("src");

        // Deny-by-default on the table itself: every subsystem directory
        // needs a pin row (a new subsystem cannot arrive unsealed), and
        // every row must name a live subsystem (a stale pin is deleted with
        // its subsystem, never left dangling).
        let dirs = subsystem_dir_names(&src_root);
        let pinned: Vec<&str> = SUBSYSTEM_BARREL_ITEMS
            .iter()
            .map(|(name, _)| *name)
            .collect();
        for dir in &dirs {
            assert!(
                pinned.contains(&dir.as_str()),
                "subsystem `{dir}` has no pinned barrel surface — add its row to \
                 SUBSYSTEM_BARREL_ITEMS"
            );
        }
        for name in &pinned {
            assert!(
                dirs.iter().any(|d| d == name),
                "pinned subsystem `{name}` has no directory under src/ — delete its stale row"
            );
        }

        for (name, items) in SUBSYSTEM_BARREL_ITEMS {
            // The front door is `src/<name>/mod.rs`, or the flat
            // `src/<name>.rs` while a subsystem coexists with its
            // same-named interface file.
            let mod_rs = src_root.join(name).join("mod.rs");
            let front_door = if mod_rs.exists() {
                mod_rs
            } else {
                src_root.join(format!("{name}.rs"))
            };
            let text = fs::read_to_string(&front_door)
                .unwrap_or_else(|e| panic!("failed to read {}: {}", front_door.display(), e));
            let file = syn::parse_file(&text)
                .unwrap_or_else(|e| panic!("failed to parse {}: {}", front_door.display(), e));

            let mut exported = Vec::new();
            for item in &file.items {
                if let syn::Item::Use(item_use) = item {
                    if matches!(item_use.vis, syn::Visibility::Public(_)) {
                        let mut leaves = Vec::new();
                        expand_use_tree(&item_use.tree, &[], &mut leaves);
                        for (path, _, _) in leaves {
                            let leaf = path.rsplit("::").next().unwrap_or(&path).to_string();
                            exported.push(leaf);
                        }
                    }
                }
            }
            exported.sort();
            exported.dedup();
            let mut expected: Vec<String> = items.iter().map(|s| s.to_string()).collect();
            expected.sort();
            assert_eq!(
                exported, expected,
                "{name}'s `pub use` surface no longer matches its pinned barrel"
            );

            // The front door's mod declarations must never be pub — the
            // barrel's `pub use` list is the only public surface.
            for item in &file.items {
                if let syn::Item::Mod(item_mod) = item {
                    assert!(
                        !matches!(item_mod.vis, syn::Visibility::Public(_)),
                        "{name}'s front door: `mod {}` must not be pub",
                        item_mod.ident
                    );
                }
            }

            // Every mod declaration anywhere inside the stratum must stay
            // sealed too: never bare `pub`, never `pub(in ...)` — private or
            // `pub(super)` only (item-level `pub` below a sealed mod is
            // still needed for multi-hop re-exports to compile, so this
            // checks module front doors, not every item).
            for path in collect_rs_files(&src_root.join(name)) {
                let raw = fs::read_to_string(&path)
                    .unwrap_or_else(|e| panic!("failed to read {}: {}", path.display(), e));
                let file = syn::parse_file(&raw)
                    .unwrap_or_else(|e| panic!("failed to parse {}: {}", path.display(), e));
                let rel = path
                    .strip_prefix(&src_root)
                    .expect("file under src_root")
                    .display()
                    .to_string();
                for item in &file.items {
                    if let syn::Item::Mod(item_mod) = item {
                        match &item_mod.vis {
                            syn::Visibility::Public(_) => panic!(
                                "{rel}: `mod {}` is bare pub — stratum mods must stay sealed \
                                 (pub(super) at most)",
                                item_mod.ident
                            ),
                            syn::Visibility::Restricted(r) => assert!(
                                r.in_token.is_none(),
                                "{rel}: `mod {}` uses `pub(in ...)` — not permitted, use \
                                 `pub(super)` instead",
                                item_mod.ident
                            ),
                            syn::Visibility::Inherited => {}
                        }
                    }
                }
            }
        }
    }
}
