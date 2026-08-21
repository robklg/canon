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
    Interface,
    /// Test scaffolding and the cross-surface canaries. Exempt from the four
    /// stratum rules: a fixture builds a database by writing rows, and a
    /// canary reads across every surface it guards, so those rules would only
    /// ever mistake test setup for production data movement.
    ///
    /// The exemption stops there. Layer is orthogonal to home, so a file
    /// here still answers to the boundary rules — a fixture in `core/` may
    /// no more reach a subsystem than any other core file, which is the one
    /// rule core exists to prove. Membership is spelled out one path at a
    /// time in `classify_layer`: a new testing home is a deliberate edit
    /// here, never something a directory name grants itself.
    Testing,
}

/// Paths that classify as `Layer::Testing`: the shared fixtures, and the
/// contentless law's canary at the crate root.
const TESTING_PATHS: &[&str] = &["core/testing/", "contentless_law_tests.rs"];

/// Stratum front-door module names inside a subsystem. For the
/// sibling-boundary rule these are never "declared surface": a sibling
/// reference to `crate::<sibling>::<stratum>` hands out the internals'
/// front door, so it is refused like any deeper reach — a sibling's
/// surface is its barrel's *item* re-exports, never a stratum module
/// (ADR amendment: Sibling-Boundary Rule, 2026-08-08).
const STRATUM_FRONT_DOORS: &[&str] = &["domain", "repo", "ops", "cli"];

fn classify_layer(rel_path: &str) -> Layer {
    if TESTING_PATHS
        .iter()
        .any(|p| rel_path == *p || rel_path.starts_with(p))
    {
        return Layer::Testing;
    }
    // Any directory under `src/` is `core` or a subsystem — the barrel pin
    // refuses a subsystem directory it does not list, so no third kind of
    // directory can exist — and both classify their strata the same way.
    // Splitting on the first separator therefore covers every directory by
    // construction: a newly added subsystem classifies correctly before
    // anyone remembers this function exists. The per-directory arm chain
    // this replaces could silently drop a whole subsystem out of the layer
    // rules when its arm was forgotten; making that unrepresentable retired
    // both the chain and the guard test that watched over it.
    if let Some((_, rest)) = rel_path.split_once('/') {
        return classify_subsystem_stratum(rest);
    }
    // Everything left is a flat file at the crate root: `main.rs`, the
    // utilities beside it, and the front-door barrels of the subsystems that
    // have one there rather than in their own `mod.rs`.
    Layer::Interface
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

/// Where a file lives in the feature-first tree: the shared hub (`core/`),
/// one of the feature modules beside it, or the crate root — the flat files
/// directly in `src/`. Two unlike things sit at that root and share the
/// variant: `main.rs` with the utilities it dispatches through, which belong
/// to no subsystem, and the flat front-door barrels, each belonging entirely
/// to the subsystem it opens. (Not every front door is here: `retire` and
/// `story` put theirs in their own `mod.rs`, which classifies as the
/// subsystem.) Both kinds are exempted by the boundary rule below, for the
/// reasons given there.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Home {
    Core,
    Subsystem(String),
    CrateRoot,
}

fn top_level_dir(rel_path: &str) -> Option<&str> {
    rel_path.split_once('/').map(|(first, _)| first)
}

fn classify_home(rel_path: &str) -> Home {
    match top_level_dir(rel_path) {
        Some("core") => Home::Core,
        Some(dir) => Home::Subsystem(dir.to_string()),
        None => Home::CrateRoot,
    }
}

/// Discovers subsystem directory names by listing `src/`'s top-level
/// directories and excluding `core` — mirrors `interface_module_names`'s
/// dynamic-enumeration style rather than a hardcoded list, so a new
/// subsystem needs no scanner edit to be recognized.
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
                if name != "core" {
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
    /// The entry covers at least one reach that writes. An entry naming a
    /// module import rather than a call takes the worst severity under it:
    /// the reaches it stands for are spelled through a bound name the scan
    /// cannot follow, so the row must not read milder than what it admits.
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
    "core::repo",
    "core::repo::Db",
    "core::repo::Connection",
    "core::repo::DbOptions",
    "core::repo::open_with_options",
    "core::repo::open_in_memory_for_test",
    "core::repo::db::Db",
    "core::repo::db::Connection",
    "core::repo::db::DbOptions",
    "core::repo::db::open_with_options",
    "core::repo::db::open_in_memory_for_test",
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
        file: "archive/cli/apply.rs",
        reference: "core::repo::root::fetch_all",
        severity: Severity::Read,
    },
    Tier3Entry {
        file: "archive/cli/apply.rs",
        reference: "core::repo::fact::batch_fetch_key_for_sources",
        severity: Severity::Read,
    },
    Tier3Entry {
        file: "archive/cli/cluster.rs",
        reference: "core::repo::root::fetch_all",
        severity: Severity::Read,
    },
    Tier3Entry {
        file: "exclude/cli.rs",
        reference: "core::repo::root::fetch_all",
        severity: Severity::Read,
    },
    Tier3Entry {
        file: "facts/cli.rs",
        reference: "core::repo::root::fetch_all",
        severity: Severity::Read,
    },
    Tier3Entry {
        file: "facts/cli/import.rs",
        reference: "core::repo::root::fetch_all",
        severity: Severity::Read,
    },
    Tier3Entry {
        file: "notes/cli.rs",
        reference: "core::repo::root::fetch_all",
        severity: Severity::Read,
    },
    Tier3Entry {
        file: "retire/cli.rs",
        reference: "core::repo::root::fetch_all",
        severity: Severity::Read,
    },
    Tier3Entry {
        file: "roots/cli.rs",
        reference: "core::repo::root::fetch_all",
        severity: Severity::Read,
    },
    Tier3Entry {
        file: "story/cli.rs",
        reference: "core::repo::root::fetch_all",
        severity: Severity::Read,
    },
    Tier3Entry {
        file: "scan/cli.rs",
        reference: "core::repo::root::fetch_all",
        severity: Severity::Read,
    },
    Tier3Entry {
        file: "trail/cli.rs",
        reference: "core::repo::root::fetch_all",
        severity: Severity::Read,
    },
    Tier3Entry {
        file: "main.rs",
        reference: "core::repo::root::fetch_all",
        severity: Severity::Read,
    },
    Tier3Entry {
        file: "main.rs",
        reference: "core::repo::print_profile_summary",
        severity: Severity::Read,
    },
    Tier3Entry {
        file: "exclude/cli.rs",
        reference: "core::repo::Db::from_connection",
        severity: Severity::TestOnly,
    },
    Tier3Entry {
        file: "survey/cli.rs",
        reference: "core::repo::Db::from_connection",
        severity: Severity::TestOnly,
    },
    Tier3Entry {
        file: "scan/cli.rs",
        reference: "core::repo::insert_test_root",
        severity: Severity::TestOnly,
    },
    // Own-repo reaches from an interface file. Invisible until the interface
    // arm gained the own-repo matcher, so these are drift that was always
    // there, not drift that arrived. The two module-import rows each stand
    // for the calls made through their bound name — notes::repo's `insert`,
    // roots::repo::root's `fetch_file_counts` and `set_comment` — which is
    // why they name a module and carry the write severity.
    Tier3Entry {
        file: "notes/cli.rs",
        reference: "crate::notes::repo",
        severity: Severity::Write,
    },
    Tier3Entry {
        file: "roots/cli.rs",
        reference: "crate::roots::repo",
        severity: Severity::Write,
    },
    Tier3Entry {
        file: "scan/cli.rs",
        reference: "crate::scan::repo::root::update_last_scanned_at",
        severity: Severity::Write,
    },
    Tier3Entry {
        file: "scan/cli.rs",
        reference: "crate::scan::repo::root::create",
        severity: Severity::Write,
    },
];

// ============================================================================
// Classification
// ============================================================================

fn is_or_under(path: &str, root: &str) -> bool {
    path == root || path.starts_with(&format!("{root}::"))
}

/// Matches a reference against the repository layer, whose canonical path is
/// `core::repo`.
///
/// A file that imports the module itself — `use crate::core::repo::{self, Db}`,
/// the prevailing idiom — then spells its calls `repo::root::fetch_all(..)`.
/// That bare form denotes the same layer and must be caught the same way, or
/// the most common call shape in the tree would be invisible here. A match is
/// reported in the canonical spelling, so a reference carries one name in the
/// baselines below however the source spelled it.
///
/// What makes the bare form unambiguous is not that the crate root has no
/// `repo` module — that alone would leave a subsystem free to bind the name to
/// its own `repo` stratum — but that `bare_repo_binding_refusal` refuses any
/// import that would. The two belong together: loosen one and the other starts
/// reporting references against a layer they do not belong to.
fn match_repo(path: &str) -> Option<String> {
    if is_or_under(path, "core::repo") {
        return Some(path.to_string());
    }
    if is_or_under(path, "repo") {
        return Some(format!("core::repo{}", &path["repo".len()..]));
    }
    None
}

/// Matches a reference against the operations layer.
///
/// This matches a path that begins at `core::ops` or at `ops` — which, given
/// the `crate::` prefix its callers strip, means the two top-level spellings
/// `crate::core::ops` and `crate::ops`. It does not see a subsystem's own
/// operations stratum
/// (`crate::ls::ops`, `super::ops`); that is `match_own_ops`'s job, and the two
/// are kept apart because reaching your own stratum and reaching the spine's
/// are different edges with different rules.
///
/// `core::ops` is the shared spine's operations stratum. The bare `crate::ops`
/// spelling no longer resolves to anything — the pre-migration operations tree
/// is gone — and is matched anyway, deliberately: a path that names no module
/// must still be refused rather than pass unrecognized, and the spelling is the
/// one a habit or a stale example would reach for. Neither form is rewritten
/// into the other; a reference is reported as written.
fn is_ops_path(path: &str) -> bool {
    is_or_under(path, "core::ops") || is_or_under(path, "ops")
}

/// Matches a reference to a subsystem's own operations stratum — the edge
/// both the repository rule and the domain rule refuse, spelled from inside
/// the subsystem.
///
/// `is_ops_path` sees only the two top-level spellings — the spine's
/// `core::ops` and a bare `crate::ops`, which now names nothing — and a
/// subsystem's own `ops` is neither.
/// It is reached by one of two spellings, and a matcher keyed on either alone
/// leaves the other free: `crate::<own>::ops` names the subsystem it belongs
/// to, while `super::ops` climbs to the same place without naming it. Both
/// are the one edge the rules exist to refuse, so both are refused.
///
/// The climbing form is matched however far it climbs, and without asking
/// which subsystem the file belongs to. That is deliberately blunt rather
/// than exact: a climb whose remainder is `ops` can only mean the stratum —
/// the one path shape it would over-match is a *nested* `ops` module inside
/// another stratum, which does not exist, and whose arrival would surface as
/// a refusal to compile the test, never a silently missed reach. Nothing
/// outside a `super::` chain is affected, which is what keeps `std::ops` out
/// of it.
fn match_own_ops(home: &Home, raw_path: &str, no_crate: &str) -> Option<String> {
    if let Home::Subsystem(own) = home {
        if is_or_under(no_crate, &format!("{own}::ops")) {
            return Some(raw_path.to_string());
        }
    }

    let mut climbed = raw_path;
    let mut levels = 0;
    while let Some(rest) = climbed.strip_prefix("super::") {
        climbed = rest;
        levels += 1;
    }
    if levels > 0 && is_or_under(climbed, "ops") {
        return Some(raw_path.to_string());
    }

    None
}

/// Matches a reference to a subsystem's own repository stratum —
/// `match_own_ops`'s repository twin, serving the domain rule: `match_repo`
/// sees the spine's `core::repo` and the bare form, and a subsystem's own
/// `repo` is neither. Same two spellings, same deliberately blunt climb, for
/// the reasons given there.
fn match_own_repo(home: &Home, raw_path: &str, no_crate: &str) -> Option<String> {
    if let Home::Subsystem(own) = home {
        if is_or_under(no_crate, &format!("{own}::repo")) {
            return Some(raw_path.to_string());
        }
    }

    let mut climbed = raw_path;
    let mut levels = 0;
    while let Some(rest) = climbed.strip_prefix("super::") {
        climbed = rest;
        levels += 1;
    }
    if levels > 0 && is_or_under(climbed, "repo") {
        return Some(raw_path.to_string());
    }

    None
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
            if is_ops_path(no_crate) {
                return Some((Rule::DomainNoOps, raw_path.to_string()));
            }
            if let Some(reference) = match_own_ops(home, raw_path, no_crate) {
                return Some((Rule::DomainNoOps, reference));
            }
            if let Some(reference) = match_repo(no_crate) {
                return Some((Rule::DomainNoRepo, reference));
            }
            if let Some(reference) = match_own_repo(home, raw_path, no_crate) {
                return Some((Rule::DomainNoRepo, reference));
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
            if is_ops_path(no_crate) {
                return Some((Rule::RepoNoOps, raw_path.to_string()));
            }
            if let Some(reference) = match_own_ops(home, raw_path, no_crate) {
                return Some((Rule::RepoNoOps, reference));
            }
        }
        Layer::Ops | Layer::Testing => {}
        Layer::Interface => {
            if let Some(reference) = match_repo(no_crate) {
                return Some((Rule::InterfaceRepoDataMovement, reference));
            }
            // The own-repo twin, for the same reason the domain arm carries
            // it: a subsystem reaching its own `repo` names neither the
            // spine's `core::repo` nor the bare form, so `match_repo` alone
            // left every own-repo reach from an interface file invisible —
            // and an interface file is where the rule matters most.
            if let Some(reference) = match_own_repo(home, raw_path, no_crate) {
                return Some((Rule::InterfaceRepoDataMovement, reference));
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
    // referencing file is domain/repo/ops within its home. Crate-root
    // (`Home::CrateRoot`) files are unrestricted here, because two of the
    // tree's load-bearing references live there: `main.rs` dispatching into
    // `retire::`, which is what the tree is for, and a front-door barrel
    // naming its own subsystem's strata, which is how a barrel is written at
    // all. The cost, stated rather than implied: no rule in this file refuses
    // a crate-root reach past a subsystem's barrel. What refuses it is the
    // module system — every *subsystem* front door declares its strata with
    // bare-private `mod`, so `crate::<sub>::repo::…` resolves from nowhere.
    // (`core` is the deliberate opposite and not a subsystem: its strata are
    // `pub mod`, because the spine's job is to be reachable.)
    //
    // That is a property of how the front doors are written, and the seal
    // below pins it whole: a stratum `mod` at a front door must carry
    // inherited visibility — `pub(super)` and `pub(crate)` are refused along
    // with bare `pub`, because at that one depth `super` *is* the crate
    // root, and either would open the stratum to every flat file.
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
                    Home::Subsystem(_) | Home::CrateRoot => {}
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

    /// Refuses a `use` that binds the bare name `repo` to anything but the
    /// shared repository layer.
    ///
    /// `match_repo` reads `repo::root::fetch_all(..)` as the shared layer
    /// because the only import that binds that name is
    /// `use crate::core::repo::{self, ..}`. A subsystem importing its *own*
    /// `repo` stratum the same way would bind the name to a different module,
    /// and every later bare path in that file would be reported against
    /// `core::repo` — the wrong layer, under a path that appears nowhere in
    /// the source. Refusing the binding keeps the reading true by
    /// construction instead of by convention.
    ///
    /// An aliased import binds a different name and is unaffected; that is
    /// the form the tree already uses everywhere (`scan_repo`, `notes_repo`,
    /// and the rest). A glob binds the module's contents, not the module.
    fn bare_repo_binding_refusal(&self, path: &str, renamed: bool, glob: bool) -> Option<String> {
        if renamed || glob || path == "crate::core::repo" {
            return None;
        }
        if path.rsplit("::").next()? != "repo" {
            return None;
        }
        Some(format!(
            "import `{path}` binds the bare name `repo` to a module other than the shared \
             repository layer, so later `repo::..` paths in this file would be read as \
             `core::repo` — import it under a distinct name or spell its items in full",
        ))
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
            if let Some(err) = self.bare_repo_binding_refusal(path, *renamed, *glob) {
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
                // A rename or a glob normally aborts: the bound name carries
                // the reach through the rest of the file, where no path walk
                // can see it. One rename is exempt — exactly the import
                // `bare_repo_binding_refusal` *requires* to be renamed, so
                // that later bare `repo::..` paths keep meaning the shared
                // layer. Aborting there would leave the two guards demanding
                // opposite things, so the exemption is asked of that guard
                // rather than restated here. The import
                // line is a complete observation of the reach on its own, so
                // it is recorded as a violation instead, at the grain a
                // baseline row names. A glob still aborts: it binds contents,
                // not the module, and no rule forces that spelling.
                let forced_rename = *renamed
                    && !*glob
                    && self.bare_repo_binding_refusal(path, false, false).is_some();
                if (*renamed || *glob) && !forced_rename {
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
// Pinned exceptions
// ============================================================================

/// Calls that reach the database directly, rather than through a repository.
const SQL_CALLS: &[&str] = &[
    ".query_row(",
    ".query_map(",
    ".query(",
    ".prepare(",
    ".execute(",
    ".execute_batch(",
];

/// The expression facility's operations stratum leaves SQL to its repository
/// stratum — with exactly one accepted exception, pinned here.
///
/// `check_fact_compare` is the per-source fallback for built-in keys, the
/// values derived from source columns rather than stored as facts, and it
/// queries the database itself. It is accepted rather than repaired: moving it
/// whole into `repo` would put comparison logic there instead, which is the
/// same law broken from the other side. The repair that does work is deriving
/// built-in values in one place rather than two, which deletes the function
/// outright — so the exception ends by being removed, not by being relocated.
///
/// The exception predates the stratum. What changed when the code moved is
/// that the file it sits in now claims a layer, so the claim needs a stated
/// exception rather than silence.
///
/// This pin fails three ways, each of them wanted:
///
/// - a second SQL-speaking function appears in the stratum — the exception was
///   being treated as a precedent;
/// - the marker on the function is dropped while the SQL stays — the site
///   would vanish from the audit count while still owing the repair;
/// - the function goes away, which is the repair landing. The pin is then
///   stale and fails, so the marker, this test, and the reason for both are
///   removed by one change. That is the property that keeps it from being
///   overlooked: the repair cannot land quietly.
#[test]
fn the_facility_leaves_sql_to_its_repo_but_for_one_pinned_exception() {
    const PINNED: &str = "check_fact_compare";

    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    let ops_root = Path::new(&manifest_dir).join("src/expr/ops");
    let mut files = collect_rs_files(&ops_root);
    files.push(Path::new(&manifest_dir).join("src/expr/ops.rs"));

    let mut speaks_sql: Vec<(String, String, bool)> = Vec::new();

    for path in &files {
        let text = fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("failed to read {}: {}", path.display(), e));
        let rel = path
            .strip_prefix(&manifest_dir)
            .unwrap_or(path)
            .to_string_lossy()
            .replace('\\', "/");

        // Test modules build databases by writing rows; that is fixture work,
        // not the stratum reaching past its layer.
        // Everything from the test module on is out of scope. Cutting at the
        // first `#[cfg(test)]` is only sound while there is at most one: a
        // test-only helper placed early would otherwise hide every production
        // function below it from this scan, and the pin would go quiet without
        // saying so. So the assumption is asserted rather than relied on.
        assert!(
            text.matches("#[cfg(test)]").count() <= 1,
            "{rel}: more than one #[cfg(test)] — the SQL scan cuts at the first \
             one and would stop seeing production code below it"
        );
        let production = match text.find("#[cfg(test)]") {
            Some(i) => &text[..i],
            None => &text[..],
        };

        let mut current: Option<String> = None;
        let mut marked = false;
        let mut pending_marker = false;

        for line in production.lines() {
            if line.contains("// AUDIT:") {
                pending_marker = true;
            }
            // Indentation is stripped before matching, so a method inside an
            // `impl` block is attributed to itself rather than silently
            // inheriting the last top-level function's name — which would let
            // a second SQL speaker hide behind the first one's identity and
            // then be dropped as a duplicate. Visibility is matched by taking
            // whatever precedes `fn`, so no spelling of `pub(in ...)` slips by
            // through not being listed.
            let head = line.trim_start();
            if let Some(rest) = head.strip_prefix("fn ").or_else(|| {
                head.split_once(" fn ").and_then(|(qualifiers, rest)| {
                    let qualifiers = qualifiers.trim_end_matches(" async");
                    (qualifiers == "pub"
                        || qualifiers == "async"
                        || (qualifiers.starts_with("pub(") && qualifiers.ends_with(')')))
                    .then_some(rest)
                })
            }) {
                let name: String = rest
                    .chars()
                    .take_while(|c| c.is_alphanumeric() || *c == '_')
                    .collect();
                current = Some(name);
                marked = pending_marker;
                pending_marker = false;
            } else if line.trim().is_empty() {
                // A blank line separates a marker from anything it could mark.
                pending_marker = false;
            }

            if SQL_CALLS.iter().any(|c| line.contains(c)) {
                let owner = current
                    .clone()
                    .unwrap_or_else(|| "<file scope>".to_string());
                if !speaks_sql.iter().any(|(f, n, _)| f == &rel && n == &owner) {
                    speaks_sql.push((rel.clone(), owner, marked));
                }
            }
        }
    }

    let names: Vec<&str> = speaks_sql.iter().map(|(_, n, _)| n.as_str()).collect();
    assert_eq!(
        names,
        vec![PINNED],
        "the facility's operations stratum speaks SQL in {} place(s): {:?}.\n  \
         Exactly one is pinned ({PINNED}). If a new one appeared, it is drift — route it \
         through `expr/repo.rs`.\n  \
         If {PINNED} is gone, the repair landed: delete this test and its `// AUDIT:` marker \
         in the same commit.",
        speaks_sql.len(),
        speaks_sql
    );

    assert!(
        speaks_sql[0].2,
        "{PINNED} still queries the database but has lost its `// AUDIT:` marker, so it no \
         longer appears in the audit count that tracks work still owed."
    );
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
            scan_file(Layer::Ops, &Home::CrateRoot, "synthetic.rs", text, &[], &[]).unwrap();
        assert!(violations.is_empty(), "{violations:?}");
    }

    #[test]
    fn strip_string_literal_no_reference() {
        let text = "fn f() { let s = \"core::repo::root::fetch_all\"; }\n";
        let violations = scan_file(
            Layer::Interface,
            &Home::CrateRoot,
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
            &Home::CrateRoot,
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
            &Home::CrateRoot,
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
            &Home::CrateRoot,
            "synthetic.rs",
            text,
            &[],
            &[],
        );
        assert!(result.is_err());
    }

    #[test]
    fn strip_preserves_line_numbers() {
        let text = "/* line1\nline2\nline3 */\nfn f() { core::repo::root::fetch_all(); }\n";
        let violations = scan_file(
            Layer::Domain,
            &Home::CrateRoot,
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
        let text = "use crate::core::repo::{root, source::batch_fetch_by_roots};\nuse crate::core::repo::{self, Db};\n";
        let violations = scan_file(
            Layer::Interface,
            &Home::CrateRoot,
            "synthetic.rs",
            text,
            &[],
            &[],
        )
        .unwrap();
        let refs: std::collections::BTreeSet<_> =
            violations.iter().map(|v| v.reference.clone()).collect();
        assert!(refs.contains("core::repo::root"), "{refs:?}");
        assert!(
            refs.contains("core::repo::source::batch_fetch_by_roots"),
            "{refs:?}"
        );
        assert!(refs.contains("core::repo::Db"), "{refs:?}");
        assert!(refs.contains("core::repo"), "{refs:?}");
    }

    #[test]
    fn evasion_refusal_alias() {
        let text = "use crate::core::repo as r;\n";
        let result = scan_file(
            Layer::Interface,
            &Home::CrateRoot,
            "synthetic.rs",
            text,
            &[],
            &[],
        );
        assert!(result.is_err());
    }

    #[test]
    fn evasion_refusal_glob() {
        let text = "use crate::core::repo::root::*;\n";
        let result = scan_file(
            Layer::Interface,
            &Home::CrateRoot,
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
            reference: "core::repo::root::fetch_all".to_string(),
        };

        let outcome = evaluate_violations(std::slice::from_ref(&v), &[], &[]);
        assert_eq!(outcome.new_drift.len(), 1);

        let tier3 = [Tier3Entry {
            file: "roots.rs",
            reference: "core::repo::root::fetch_all",
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
            reference: "core::repo::Db".to_string(),
        };
        let outcome = evaluate_violations(&[v], &[], &[]);
        assert!(outcome.new_drift.is_empty());

        let v2 = Violation {
            file: "roots.rs".to_string(),
            line: 1,
            rule: Rule::InterfaceRepoDataMovement,
            reference: "core::repo::root::fetch_all".to_string(),
        };
        let outcome2 = evaluate_violations(&[v2], &[], &[]);
        assert_eq!(outcome2.new_drift.len(), 1);
    }

    /// Binding the bare name `repo` to a subsystem's own stratum would make
    /// every later `repo::..` path in that file read as the shared layer.
    #[test]
    fn binding_bare_repo_to_a_subsystem_stratum_is_refused() {
        let subsystems = vec!["scan".to_string()];
        let result = scan_file(
            Layer::Interface,
            &Home::Subsystem("scan".to_string()),
            "scan/cli.rs",
            "use crate::scan::repo::{self, root};\n",
            &[],
            &subsystems,
        );
        assert!(result.is_err(), "{result:?}");

        // Neither of the forms that do not bind the bare name aborts the scan:
        // the alias the tree actually uses, and the shared layer's own import.
        for text in [
            "use crate::scan::repo as scan_repo;\n",
            "use crate::core::repo::{self, Connection};\n",
        ] {
            let ok = scan_file(
                Layer::Interface,
                &Home::Subsystem("scan".to_string()),
                "scan/cli.rs",
                text,
                &[],
                &subsystems,
            );
            assert!(ok.is_ok(), "{text:?} -> {ok:?}");
        }
    }

    /// The rename this rule forces is not read as evasion — and no other one
    /// is spared. Without the pairing the two guards contradict: the bare
    /// form is refused for binding `repo`, and the renamed form would abort
    /// the scan for defeating it, leaving a subsystem no legal spelling.
    #[test]
    fn the_rename_the_bare_repo_rule_forces_is_recorded_not_refused() {
        let subsystems = vec!["scan".to_string()];
        let forced = scan_file(
            Layer::Interface,
            &Home::Subsystem("scan".to_string()),
            "scan/cli.rs",
            "use crate::scan::repo as scan_repo;\n",
            &[],
            &subsystems,
        )
        .expect("a forced rename is recorded, not refused");
        assert_eq!(forced.len(), 1, "{forced:?}");
        assert_eq!(forced[0].reference, "crate::scan::repo");

        // `crate::core::repo` may be bound bare, so nothing forces a rename
        // of it — aliasing it is evasion and still aborts.
        let evasion = scan_file(
            Layer::Interface,
            &Home::Subsystem("scan".to_string()),
            "scan/cli.rs",
            "use crate::core::repo as r;\n",
            &[],
            &subsystems,
        );
        assert!(evasion.is_err(), "{evasion:?}");

        // A glob binds contents, not the module: no rule forces it, and the
        // exemption must not widen to cover it.
        let globbed = scan_file(
            Layer::Interface,
            &Home::Subsystem("scan".to_string()),
            "scan/cli.rs",
            "use crate::scan::repo::*;\n",
            &[],
            &subsystems,
        );
        assert!(globbed.is_err(), "{globbed:?}");
    }

    /// The operations layer is matched at both of the spellings the matcher
    /// accepts. `core::ops` is the one that names a live module, and it has no
    /// live observation anywhere in the tree, so without this it could be
    /// misspelled unnoticed.
    ///
    /// The bare-`ops::` sample resolves to nothing at all — the pre-migration
    /// operations tree it once named is gone. It stays here because the
    /// spelling must be refused whether or not a module answers to it: what
    /// would reach for it now is a habit or a stale example, and either should
    /// be told no rather than pass unrecognized.
    #[test]
    fn domain_reaching_ops_is_caught_at_both_of_its_spellings() {
        for text in [
            "use crate::ops::anything::at_all;\n",
            "use crate::core::ops::root_story::fetch_root_story;\n",
        ] {
            let violations = scan_file(
                Layer::Domain,
                &Home::Core,
                "core/domain/source.rs",
                text,
                &[],
                &[],
            )
            .unwrap();
            assert!(
                violations.iter().any(|v| v.rule == Rule::DomainNoOps),
                "{text:?} -> {violations:?}"
            );
        }
    }

    /// A subsystem's repository stratum must not reach the operations stratum
    /// beside it. The absolute spelling names the subsystem the file already
    /// belongs to, which is the form a file written from the outside in tends
    /// to take.
    #[test]
    fn subsystem_repo_reaching_its_own_ops_is_refused_when_spelled_absolutely() {
        let subsystems = vec!["expr".to_string()];
        let violations = scan_file(
            Layer::Repo,
            &Home::Subsystem("expr".to_string()),
            "expr/repo.rs",
            "use crate::expr::ops::filter::apply_filters;\n",
            &[],
            &subsystems,
        )
        .unwrap();
        assert!(
            violations.iter().any(|v| v.rule == Rule::RepoNoOps),
            "{violations:?}"
        );

        // The sibling stratum a repository file may legitimately read is the
        // domain one, and naming the subsystem is not itself the problem.
        let ok = scan_file(
            Layer::Repo,
            &Home::Subsystem("expr".to_string()),
            "expr/repo.rs",
            "use crate::expr::domain::cache::FactCache;\n",
            &[],
            &subsystems,
        )
        .unwrap();
        assert!(
            !ok.iter().any(|v| v.rule == Rule::RepoNoOps),
            "reading its own domain must stay legal, got {ok:?}"
        );
    }

    /// The same edge, climbed rather than named. Written from inside the
    /// subsystem this is the shorter spelling, so a guard that only knew the
    /// absolute form would be refusing the phrasing nobody reaches for.
    #[test]
    fn subsystem_repo_reaching_its_own_ops_is_refused_when_spelled_relatively() {
        for text in [
            "use super::ops::filter::apply_filters;\n",
            "use super::super::ops::filter::apply_filters;\n",
        ] {
            let violations = scan_file(
                Layer::Repo,
                &Home::Subsystem("expr".to_string()),
                "expr/repo.rs",
                text,
                &[],
                &["expr".to_string()],
            )
            .unwrap();
            assert!(
                violations.iter().any(|v| v.rule == Rule::RepoNoOps),
                "{text:?} -> {violations:?}"
            );
        }

        // `std::ops` is not a climb, and the repository layer imports it.
        let ok = scan_file(
            Layer::Repo,
            &Home::Core,
            "core/repo/db.rs",
            "use std::ops::Deref;\n",
            &[],
            &[],
        )
        .unwrap();
        assert!(
            !ok.iter().any(|v| v.rule == Rule::RepoNoOps),
            "std::ops is unrelated to the operations layer, got {ok:?}"
        );
    }

    /// A subsystem's domain stratum must not reach the operations or the
    /// repository stratum beside it. The spine spellings (`core::ops`,
    /// `core::repo`) were always refused; these are the same edges spelled at
    /// the subsystem's own strata — absolute and climbed — which the spine
    /// matchers cannot see.
    #[test]
    fn subsystem_domain_reaching_its_own_ops_or_repo_is_refused() {
        for (text, rule) in [
            (
                "use crate::expr::ops::filter::apply_filters;\n",
                Rule::DomainNoOps,
            ),
            (
                "use super::super::ops::filter::apply_filters;\n",
                Rule::DomainNoOps,
            ),
            (
                "use crate::expr::repo::get_fact_value;\n",
                Rule::DomainNoRepo,
            ),
            (
                "use super::super::repo::get_fact_value;\n",
                Rule::DomainNoRepo,
            ),
        ] {
            let violations = scan_file(
                Layer::Domain,
                &Home::Subsystem("expr".to_string()),
                "expr/domain/filter.rs",
                text,
                &[],
                &["expr".to_string()],
            )
            .unwrap();
            assert!(
                violations.iter().any(|v| v.rule == rule),
                "{text:?} -> {violations:?}"
            );
        }

        // A domain file reading the domain module beside it stays legal.
        let ok = scan_file(
            Layer::Domain,
            &Home::Subsystem("expr".to_string()),
            "expr/domain/filter.rs",
            "use super::key::ParsedFactKey;\n",
            &[],
            &["expr".to_string()],
        )
        .unwrap();
        assert!(
            ok.is_empty(),
            "a sibling domain module is legal, got {ok:?}"
        );
    }

    /// The repository layer is at `core::repo`, and an interface file that
    /// reaches a data function there is drift the rule must see — spelled
    /// either way the tree really spells it.
    #[test]
    fn interface_reaching_repo_data_is_caught_at_its_real_path() {
        for text in [
            "use crate::core::repo::root::fetch_all;\n",
            "use crate::core::repo::{self, Db};\nfn f() { repo::root::fetch_all(); }\n",
        ] {
            let violations = scan_file(
                Layer::Interface,
                &Home::CrateRoot,
                "synthetic.rs",
                text,
                &[],
                &[],
            )
            .unwrap();
            assert!(
                violations
                    .iter()
                    .any(|v| v.rule == Rule::InterfaceRepoDataMovement
                        && v.reference == "core::repo::root::fetch_all"),
                "{text:?} -> {violations:?}"
            );
        }
    }

    /// Holding the database is plumbing; moving data through it is not. Both
    /// halves are asserted together so a rule that stopped firing altogether
    /// could not pass as a widened plumbing list.
    #[test]
    fn holding_the_database_is_plumbing_but_fetching_is_not() {
        let text = "use crate::core::repo::Db;\nuse crate::core::repo::root::fetch_all;\n";
        let violations = scan_file(
            Layer::Interface,
            &Home::CrateRoot,
            "synthetic.rs",
            text,
            &[],
            &[],
        )
        .unwrap();
        let outcome = evaluate_violations(&violations, &[], &[]);
        let drift: Vec<&str> = outcome
            .new_drift
            .iter()
            .map(|v| v.reference.as_str())
            .collect();
        assert_eq!(drift, vec!["core::repo::root::fetch_all"], "{violations:?}");
    }

    #[test]
    fn word_boundary_excludes_prefixed_identifiers() {
        let text = "fn f() { let my_repo = 1; my_repo::thing(); }\n";
        let violations = scan_file(
            Layer::Interface,
            &Home::CrateRoot,
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
        let text = "fn f() { my_macro!(core::repo::root::fetch_all(), println!(\"x\")); }\n";
        let violations = scan_file(
            Layer::Domain,
            &Home::CrateRoot,
            "synthetic.rs",
            text,
            &[],
            &[],
        )
        .unwrap();
        assert!(
            violations
                .iter()
                .any(|v| v.rule == Rule::DomainNoRepo
                    && v.reference == "core::repo::root::fetch_all"),
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
    // physically populated — so a fixture stays meaningful whether or not
    // the subsystem it names has been carved out yet. Several of them now
    // stand for live edges rather than hypothetical ones: `retire` really
    // does reach `story` and `trail` through their barrels.
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
        // Stands in for the real `retire/ops/ceremony.rs` -> `story::report_over`
        // dependency — retire depends on
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
    fn the_crate_root_may_reference_a_subsystems_public_surface() {
        // main.rs dispatching into `retire::` command entry points — what the
        // tree exists to allow, and unrestricted by the boundary rule.
        let text = "use crate::retire::retire;\n";
        let subsystems = vec!["retire".to_string()];
        let violations = scan_file(
            Layer::Interface,
            &Home::CrateRoot,
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
    fn the_expression_facility_answers_to_the_stratum_rules() {
        // The facility was once a layer of its own, exempt from every stratum
        // rule. The failure that exemption invites is silence, not noise: a
        // repo reach from a domain file inside it would have been allowed
        // rather than misclassified, so nothing would have said a word.
        assert_eq!(classify_layer("expr/domain/filter.rs"), Layer::Domain);
        assert_eq!(classify_layer("expr/domain.rs"), Layer::Domain);
        assert_eq!(classify_layer("expr/repo.rs"), Layer::Repo);
        assert_eq!(classify_layer("expr/ops/selection.rs"), Layer::Ops);
        assert_eq!(classify_layer("expr/ops.rs"), Layer::Ops);
        // The front door itself, which is where the barrel lives.
        assert_eq!(classify_layer("expr.rs"), Layer::Interface);
    }

    #[test]
    fn the_fixtures_and_the_canary_classify_as_testing() {
        assert_eq!(classify_layer("core/testing/mod.rs"), Layer::Testing);
        assert_eq!(classify_layer("core/testing/helpers.rs"), Layer::Testing);
        assert_eq!(classify_layer("contentless_law_tests.rs"), Layer::Testing);
    }

    #[test]
    fn testing_files_raise_no_rule_for_the_reaches_they_make() {
        // What the canary actually does: batch reads through repo (data
        // movement anywhere else in the interface layer) and calls into
        // the subsystems whose surfaces it guards.
        let text = "\
use crate::core::repo;
use crate::survey::ObjectIndex;
fn c() {
    let _ = crate::core::repo::source::batch_fetch_by_roots(&conn, &ids);
    let _ = crate::exclude::plan_set_objects(&conn);
}
";
        let subsystems = vec!["survey".to_string(), "exclude".to_string()];
        let violations = scan_file(
            Layer::Testing,
            &Home::CrateRoot,
            "contentless_law_tests.rs",
            text,
            &[],
            &subsystems,
        )
        .unwrap();
        assert!(violations.is_empty(), "{violations:?}");

        // And what the fixtures do: open a database and write rows.
        let fixture = "\
use crate::core::repo::Connection;
fn s() -> Connection {
    crate::core::repo::db::open_in_memory_for_test()
}
";
        let violations = scan_file(
            Layer::Testing,
            &Home::Core,
            "core/testing/helpers.rs",
            fixture,
            &[],
            &subsystems,
        )
        .unwrap();
        assert!(violations.is_empty(), "{violations:?}");
    }

    /// Control for the two above: the testing layer is spelled path by path,
    /// so nothing else may drift into it. Were `classify_layer` to hand out
    /// `Testing` broadly, the four stratum rules would go quiet across the
    /// whole tree. Two other guards would catch that too — the stratum
    /// control below, and the drift baselines, which match both directions
    /// and so redden on entries left unmatched — but this is the one that
    /// names the cause.
    #[test]
    fn the_testing_layer_claims_nothing_beyond_its_own_paths() {
        assert_eq!(classify_layer("core/ops/receipt.rs"), Layer::Ops);
        assert_eq!(classify_layer("core/repo/source.rs"), Layer::Repo);
        assert_eq!(classify_layer("core/domain/fate.rs"), Layer::Domain);
        assert_eq!(classify_layer("archive/ops/receipt.rs"), Layer::Ops);
        assert_eq!(classify_layer("main.rs"), Layer::Interface);
        // Not a prefix match on a bare name: a subsystem file whose path
        // merely starts the same way stays where it was.
        assert_eq!(classify_layer("core/testing.rs"), Layer::Interface);
    }

    /// The exemption is from the stratum rules only. A fixture living in
    /// core is still core, and the hub may not depend on a spoke through
    /// its test scaffolding any more than through its production code —
    /// the one rule core exists to prove, which would otherwise hold here
    /// only by reading the classifier.
    #[test]
    fn a_testing_file_in_core_still_may_not_reach_a_subsystem() {
        let text = "use crate::survey::ObjectIndex;\n";
        let subsystems = vec!["survey".to_string()];
        let violations = scan_file(
            Layer::Testing,
            &Home::Core,
            "core/testing/helpers.rs",
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
    fn classify_home_recognizes_core_subsystem_and_crate_root() {
        assert_eq!(classify_home("core/domain/resolution.rs"), Home::Core);
        assert_eq!(
            classify_home("retire/ops/ceremony.rs"),
            Home::Subsystem("retire".to_string())
        );
        assert_eq!(classify_home("main.rs"), Home::CrateRoot);
        // Most subsystems open through a flat front-door file, which therefore
        // lives at the crate root — beside `main.rs`, not inside the directory
        // it opens. `retire` and `story` use their own `mod.rs` instead, so
        // their front doors classify as the subsystem, not the crate root.
        // (`core` also opens through a `mod.rs`, but it is not a subsystem at
        // all: the arm above catches it first.)
        assert_eq!(classify_home("ls.rs"), Home::CrateRoot);
        assert_eq!(
            classify_home("retire/mod.rs"),
            Home::Subsystem("retire".to_string())
        );
    }

    // ========================================================================
    // Barrel-surface sealing — every subsystem's public surface is pinned
    // ========================================================================

    /// Each subsystem's complete public surface: the `pub use` items its
    /// front door re-exports — CLI entry points, the finished-result items
    /// siblings consume, and the parameter types of those, which a caller
    /// must be able to name to factor a helper around a call.
    ///
    /// Nothing structural forces an unnamed type onto a barrel: this is one
    /// binary, so no crate boundary — and no lint, which was checked rather
    /// than assumed — requires a type to be re-exported for a value of it to
    /// cross a module boundary. The line is drawn by rule instead — the
    /// parameter-type rule: a parameter type of an exported item is carried
    /// even while nothing names it, because the constraint binds the moment a
    /// caller writes a helper; a return type is not, because inference always
    /// lets a caller leave it unnamed, and carrying it would record demand
    /// that does not exist. Real demand for a return type earns it a place by
    /// a one-line pin edit, with the consumer as the evidence.
    ///
    /// Changing a barrel means editing its pin here in the same commit: a
    /// surface change is a deliberate, reviewable act.
    const SUBSYSTEM_BARREL_ITEMS: &[(&str, &[&str])] = &[
        (
            "expr",
            &[
                // Named externally: what the rest of the engine writes down.
                "Filter",
                "UsedStatus",
                "apply_filters",
                "select_sources",
                "RolePolicy",
                "SelectionParams",
                "expand_filter_strings",
                "parse_pattern",
                "extract_fact_keys",
                "evaluate",
                "Pattern",
                "EvalContext",
                "resolve_fact_value",
                "get_builtin_value",
                "fact_value_to_display",
                "ParsedFactKey",
                "BuiltinKey",
                "BuiltinKeyCategory",
                "BuiltinKeyVisibility",
                "Modifier",
                "ModifierCategory",
                "apply_accessor",
                "apply_modifier",
                // Completing the surface: parameter types of the above,
                // named by nothing today.
                "PathAccessor",
                "ModifierCall",
                // The one point read still reached past the language.
                "get_fact_value",
            ],
        ),
        ("ls", &["run", "show_duplicates"]),
        ("worklist", &["run"]),
        ("coverage", &["run", "compute_per_root"]),
        ("compare", &["run", "CompareOptions", "run_compare"]),
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
        (
            "exclude",
            &[
                "set",
                "set_by_id",
                "set_by_path",
                "clear",
                "exclude_duplicates",
                "set_object_by_hash",
                "set_object_by_file",
                "set_objects_by_filter",
                "clear_object",
                "list_objects",
                "SetOptions",
                "ClearOptions",
                "plan_set_objects",
                "ExcludeSetObjectsParams",
                "check_set_object_by_file",
            ],
        ),
        ("scan", &["run", "find_candidates"]),
        (
            "roots",
            &[
                "list",
                "remove",
                "set_comment",
                "suspend",
                "unsuspend",
                "remove_root_data",
                "plan_remove",
            ],
        ),
        (
            "notes",
            &[
                "run",
                "Note",
                "fetch_by_roots",
                "fetch_all",
                "insert",
                "count_subtree_notes",
                "batch_count_subtree",
                "note_display_path",
                "relative_to_scope",
                "survey_note_context",
                "SurveyNoteContext",
                "format_note_date",
            ],
        ),
        (
            "facts",
            &[
                "run",
                "import_run",
                "delete_facts",
                "DeleteOptions",
                "prune_stale",
                "prune_orphaned_objects",
                "prune_excluded_facts",
                "show_aliases",
            ],
        ),
        // The command surface, the transfer mode main's dispatch selects, and
        // the two types the contentless-law canary constructs directly.
        (
            "archive",
            &[
                "run",
                "ApplyOptions",
                "generate",
                "refresh",
                "status",
                "GenerateOptions",
                "plan_generate",
                "ClusterGenerateParams",
                "TransferMode",
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
            // A subsystem's front door is either `src/<name>/mod.rs` or the
            // flat `src/<name>.rs` beside the directory — a settled choice of
            // file placement, not a transitional state. Most take the flat
            // form; `retire` and `story` take the other.
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

            // The front door's mod declarations must be bare-private — the
            // barrel's `pub use` list is the only public surface. Restricted
            // visibility is refused along with bare `pub`: at this depth
            // `super` *is* the crate root, so `pub(super)` and `pub(crate)`
            // would each open the stratum to every flat file.
            for item in &file.items {
                if let syn::Item::Mod(item_mod) = item {
                    assert!(
                        matches!(item_mod.vis, syn::Visibility::Inherited),
                        "{name}'s front door: `mod {}` must be bare-private — \
                         any visibility at this depth opens the stratum to the \
                         crate root",
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
