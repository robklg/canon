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

/// The context-supplied set is spelled inside the expression facility and
/// nowhere else.
///
/// One line used to decide which facts reach pattern evaluation, and it was
/// written out four times — three fetch sites plus the set site — each
/// carrying a comment saying all four had to agree and nothing holding them
/// to it. Its own words for the consequence: *a stored fact shadows a
/// built-in and destinations move.* Three of those sites now call one
/// function and the fourth is gone, so what remains possible is a **fifth**
/// site appearing somewhere new. This is the pin against that.
///
/// **The exemptions are not oversights, and unifying them would be a false
/// equality.** Three sites outside the facility match the same bytes and each
/// serves a different verb:
///
/// - `facts/domain.rs` — *reserving an import namespace*: it refuses
///   `source.*` on the way **in**. Same bytes, opposite direction.
/// - `facts/ops/maintain.rs` — *protecting facts from deletion*: a different
///   set (`source.*` **and** `policy.*`) answering a different question.
/// - `worklist/ops.rs` — *routing an entity lookup*: source table vs object
///   table. A dispatch switch, not a claim about what supplies a key.
///
/// Convergent spelling without convergent meaning. A matcher that flagged
/// these would be asserting the very equality the design rejects, so they are
/// listed by name with their verbs, and the count is exact: a second spelling
/// added inside an exempt file fails this too.
///
/// The scan covers test code as well as production. A test spelling the set
/// out is still a spelling of it, and admitting one would need the same
/// deliberate act as admitting a production one.
#[test]
fn the_context_supplied_set_is_spelled_only_inside_expr() {
    // The two prefix tests the old skiplist was built from. `object.hash` is
    // deliberately absent: it is an exact-match on an ordinary string and
    // matching it would flag every unrelated mention of the key.
    const SPELLINGS: &[&str] = &["starts_with(\"source.\")", "starts_with(\"scope.\")"];

    /// Each exemption, with the verb it serves and how many times it spells
    /// one of the above.
    const EXEMPT: &[(&str, usize)] = &[
        // reserving an import namespace
        ("src/facts/domain.rs", 1),
        // protecting facts from deletion
        ("src/facts/ops/maintain.rs", 1),
        // routing an entity lookup
        ("src/worklist/ops.rs", 1),
    ];

    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    let src_root = Path::new(&manifest_dir).join("src");

    let mut found: Vec<(String, usize)> = Vec::new();
    for path in collect_rs_files(&src_root) {
        let rel = path
            .strip_prefix(&manifest_dir)
            .unwrap_or(&path)
            .to_string_lossy()
            .replace('\\', "/");
        // The facility is where the set is allowed to be spelled.
        if rel.starts_with("src/expr/") || rel == "src/expr.rs" {
            continue;
        }
        let text = fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("failed to read {}: {}", path.display(), e));
        let count: usize = SPELLINGS.iter().map(|s| text.matches(s).count()).sum();
        if count > 0 {
            found.push((rel, count));
        }
    }
    found.sort();

    let expected: Vec<(String, usize)> =
        EXEMPT.iter().map(|(f, n)| ((*f).to_string(), *n)).collect();

    assert_eq!(
        found, expected,
        "\n  The context-supplied set is spelled outside `src/expr/`.\n  \
         Found: {found:?}\n  Exempt: {expected:?}\n  \
         A new site means a fifth spelling of the rule that decides which facts reach \
         pattern evaluation — route it through `expr::prefetch_pattern_facts` instead, \
         which applies the rule for its callers.\n  \
         If an exemption is genuinely gone, delete its row here in the same commit; if a new \
         one is genuinely a different verb, add it with that verb named."
    );
}

/// Where a positional probe may be spelled: the law's own owner, and this
/// file, which declares the spellings and plants them in its self-tests.
///
/// `path.rs` spells none today. It is named prospectively: if the owner ever
/// needs a probe, that is where one belongs.
const PROBE_HOMES: &[&str] = &["src/core/domain/path.rs", "tests/architecture.rs"];

/// The fingerprint: taking a byte or character prefix and then reading the
/// single element sitting at the prefix's length to see whether it is a
/// separator.
const PROBE_SPELLINGS: &[&str] = &[
    "as_bytes().get(",
    "as_bytes()[",
    "bytes().nth(",
    "chars().nth(",
];

/// Each exemption, with the verb it serves and how many times it spells a
/// probe.
const PROBE_EXEMPT: &[(&str, usize)] = &[
    // reading fixed-offset fields out of a timestamp — not a path
    ("src/facts/ops/import.rs", 2),
];

/// How many probe spellings a file's text contains.
fn positional_probes(text: &str) -> usize {
    PROBE_SPELLINGS
        .iter()
        .map(|s| text.matches(s).count())
        .sum()
}

/// Containment is decided in one place, and a hand-rolled boundary test
/// somewhere else fails the build.
///
/// `core::domain::path::path_is_under` asks `Path::starts_with`, which is
/// component-aware by construction. Re-deriving that rule looks different:
/// take a byte prefix, then read the one element sitting at the prefix's
/// length and check it against a separator. That positional read is the
/// fingerprint of a second spelling of a rule that already has one — and a
/// second spelling drifts silently. It is correct on the day it is written
/// and diverges the day the owner is repaired.
///
/// This is not tidiness. The path law's failure mode is that files land
/// somewhere the user did not ask for, at exit 0, with the record claiming
/// otherwise. A law whose only defense is that everyone remembers to call
/// the right function has no defense.
///
/// **What this does not catch, stated rather than left to be discovered.**
/// The aperture is one *idiom*, not the whole class of second spellings, and
/// it is deliberately narrow — a wide one would be dishonest rather than
/// merely noisy, because `Path::starts_with` and `str::starts_with` are
/// indistinguishable to a text scan. Four things go unseen:
///
/// - **A bare `starts_with` alone** as a containment claim, with no
///   positional read after it. That is a *wrong* containment test rather
///   than a second spelling of the right one, and behaviour tests catch it.
///   Widening to every path-ish prefix test was weighed and declined: around
///   twenty-six sites, nearly all either already correct or test assertions
///   sharing one verb, so the exemption block would assert the very equality
///   this design rejects.
/// - **The same idiom split across two statements.** The match is on the
///   adjacency of receiver and accessor, so binding the receiver to a local
///   first and reading the local at the prefix's length scores zero. A
///   two-line refactor of the exact defect walks through.
/// - **The slice form** — taking the remainder from the prefix's length and
///   asking whether *it* begins with a separator. Same rule, no positional
///   read.
/// - **The separator-concatenation family** — appending a separator to a
///   prefix and stripping that, which is `path_strip_prefix` re-derived.
///   `notes::domain::relative_to_scope` does this in production today; it
///   agrees with the owner, so it is duplication rather than a live defect,
///   and it is recorded rather than silent.
///
/// **The scan covers `tests/` as well as `src/`.** A test hand-rolling the
/// boundary is still a second spelling of it, and admitting one should need
/// the same deliberate act as admitting a production one. The one blind spot
/// inside that coverage is this file, exempt wholesale so the self-tests can
/// plant probes — an unbounded exemption, and the reason the self-tests
/// carry the weight of the matcher's correctness rather than a count does.
/// The door types: a permit class is a decision, and a `_` arm is that
/// decision going unmade.
///
/// `Door` and `RootLookup` are the two answers the boundary and the root-spec
/// door give about a place the user closed. Each consumer's arm *is* its
/// declared permit — a view sets aside, a remembering view reads, an act
/// refuses — so a wildcard is not a shorthand here: it is a surface silently
/// picking whatever the other arm happened to be, which is how a closed door
/// came to read as "all roots" at seven commands at once.
const DOOR_TYPES: &[&str] = &["Door", "RootLookup"];

/// How many `match` blocks in `text` name a door type in one arm and carry a
/// wildcard arm beside it.
///
/// Deliberately lexical, and its blind spots are enumerated below so that it
/// is not *secretly* dumb — the path law's guard sets that precedent, and a
/// guard whose aperture is undeclared is one a reader over-trusts.
///
/// A `match` block is read as the lines from the one holding `match` down to
/// the first line at or left of its own indentation that closes a brace; only
/// arms at exactly one indent step in are read, so a nested match on some
/// other type cannot lend its catch-all to this one. An arm "names a door
/// type" when the pattern left of its `=>` spells `Door::` or `RootLookup::`,
/// however it is qualified. A **catch-all** is `_`, `_ if …`, or a bare
/// lowercase binding (`other => …`) — the two spellings of the same silent
/// decision.
///
/// **What it cannot see**, each stated with why it is tolerable today:
///
/// - a match whose arms never spell the type at all — bare
///   `Open(..)`/`Closed(..)` through a glob import. Nothing in this tree
///   imports them that way, and every site is written qualified so this stays
///   true;
/// - `let Door::Open(x) = door else { … }` and `if let … else` — real, and
///   idiomatic Rust, but not the same defect: both *force* the other branch to
///   be written, so nothing falls through silently. What they cost is the
///   arm's name, not the decision;
/// - `parse_root_spec_any`, which routes around `RootLookup` entirely. It
///   exists for the two permits a closed door grants (opening, remembering)
///   and a new caller reaching for it to skip the door is a review finding by
///   name, not something a text scan can tell from a legitimate use;
/// - a match written across lines this indentation rule mis-reads. The scan
///   covers `src/` and `tests/`, and `tests/architecture.rs` is exempt
///   wholesale so the checks here can plant probes.
fn wildcard_arms_on_door_types(text: &str) -> usize {
    let indent_of = |line: &str| line.len() - line.trim_start().len();
    let mut count = 0;
    let lines: Vec<&str> = text.lines().collect();
    for (i, line) in lines.iter().enumerate() {
        if !line.contains("match ") {
            continue;
        }
        let base = indent_of(line);
        let arm = base + 4;
        let mut names_door = false;
        let mut wildcard = false;
        for probe in lines.iter().skip(i + 1) {
            let trimmed = probe.trim_start();
            if trimmed.is_empty() {
                continue;
            }
            let indent = indent_of(probe);
            if indent <= base && trimmed.starts_with('}') {
                break;
            }
            if indent != arm {
                continue;
            }
            let pattern = trimmed.split("=>").next().unwrap_or_default();
            if DOOR_TYPES
                .iter()
                .any(|ty| pattern.contains(&format!("{ty}::")))
            {
                names_door = true;
            }
            if is_catch_all(pattern) {
                wildcard = true;
            }
        }
        if names_door && wildcard {
            count += 1;
        }
    }
    count
}

/// Whether an arm's pattern matches everything: `_`, `_ if …`, or a bare
/// lowercase binding. A binding is the same silent decision as an underscore
/// — it just gives the thing it ignored a name.
fn is_catch_all(pattern: &str) -> bool {
    let p = pattern.split(" if ").next().unwrap_or_default().trim();
    // `_` satisfies the run below on its own; there is no separate case.
    !p.is_empty()
        && p.starts_with(|c: char| c.is_ascii_lowercase() || c == '_')
        && p.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// **No consumer of the closed door reads it with a wildcard.**
#[test]
fn the_door_types_are_consumed_by_name() {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    let base = Path::new(&manifest_dir);

    let mut found: Vec<(String, usize)> = Vec::new();
    for dir in ["src", "tests"] {
        for path in collect_rs_files(&base.join(dir)) {
            let rel = path
                .strip_prefix(base)
                .unwrap_or(&path)
                .to_string_lossy()
                .replace('\\', "/");
            if rel == "tests/architecture.rs" {
                continue;
            }
            let text = fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("failed to read {}: {}", path.display(), e));
            let count = wildcard_arms_on_door_types(&text);
            if count > 0 {
                found.push((rel, count));
            }
        }
    }
    found.sort();

    assert!(
        found.is_empty(),
        "\n  A closed door is read with a `_` arm: {found:?}\n  \
         Each arm of `Door`/`RootLookup` is a surface's declared permit class — \
         a view sets aside, a remembering view reads, an act refuses. Name the arm \
         you mean; a wildcard picks one silently, which is the defect the type exists \
         to make unwritable."
    );
}

#[test]
fn the_containment_probe_is_spelled_only_inside_the_path_law() {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    let base = Path::new(&manifest_dir);

    let mut found: Vec<(String, usize)> = Vec::new();
    for dir in ["src", "tests"] {
        for path in collect_rs_files(&base.join(dir)) {
            let rel = path
                .strip_prefix(base)
                .unwrap_or(&path)
                .to_string_lossy()
                .replace('\\', "/");
            if PROBE_HOMES.contains(&rel.as_str()) {
                continue;
            }
            let text = fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("failed to read {}: {}", path.display(), e));
            let count = positional_probes(&text);
            if count > 0 {
                found.push((rel, count));
            }
        }
    }
    found.sort();

    let expected: Vec<(String, usize)> = PROBE_EXEMPT
        .iter()
        .map(|(f, n)| ((*f).to_string(), *n))
        .collect();

    assert_eq!(
        found, expected,
        "\n  A positional containment probe is spelled outside the path law.\n  \
         Found: {found:?}\n  Exempt: {expected:?}\n  \
         Containment has one owner: ask `core::domain::path::path_is_under` (or, inside a \
         query, the registered `core::repo::db::path_at_or_under_sql` / \
         `path_strictly_under_sql`) instead of working the boundary out again.\n  \
         If a new site genuinely reads a fixed offset out of something that is not a path, \
         add it here with that verb named and its exact count; if an exemption is genuinely \
         gone, delete its row in the same commit."
    );
}

// ============================================================================
// Two censuses over the production tree
// ============================================================================

/// One file's production text: every line outside a `#[cfg(test)]` module.
///
/// The test modules are **blanked, not deleted**, so a line's number stays its
/// number and a census can say where it read something. Modules nest — a repo
/// stratum with `source`/`root`/`fact` submodules carries a test module inside
/// each — so the walk recurses rather than cutting at the first attribute the
/// way the SQL scan above can afford to within one stratum.
///
/// Externalised test files — a stratum's own `tests/` directory, per the
/// fixture-sharing criterion — carry no `#[cfg(test)]` at all and are excluded
/// by path instead, at each census's walk.
fn production_text(file_label: &str, raw: &str) -> String {
    let file = syn::parse_file(raw).unwrap_or_else(|e| panic!("failed to parse {file_label}: {e}"));
    let mut ranges = Vec::new();
    test_mod_line_ranges(&file.items, &mut ranges);
    raw.lines()
        .enumerate()
        .map(|(idx, line)| {
            let line_no = idx + 1;
            if ranges
                .iter()
                .any(|(from, to)| line_no >= *from && line_no <= *to)
            {
                ""
            } else {
                line
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// The line ranges every `#[cfg(test)]` module occupies, at any depth.
fn test_mod_line_ranges(items: &[syn::Item], out: &mut Vec<(usize, usize)>) {
    for item in items {
        let syn::Item::Mod(item_mod) = item else {
            continue;
        };
        if item_mod.attrs.iter().any(is_cfg_test) {
            let span = item.span();
            out.push((span.start().line, span.end().line));
            continue;
        }
        if let Some((_, inner)) = &item_mod.content {
            test_mod_line_ranges(inner, out);
        }
    }
}

/// A `#[cfg(test)]` attribute, however the predicate inside it is spelled.
fn is_cfg_test(attr: &Attribute) -> bool {
    match &attr.meta {
        Meta::List(list) => list.path.is_ident("cfg") && list.tokens.to_string().contains("test"),
        _ => false,
    }
}

/// True for a file that is test scaffolding by placement: a `tests/` directory
/// inside a stratum, where the fixture-sharing criterion externalises a corpus
/// several files share. The directory name is the whole signal, so a
/// production module may not be called `tests`.
fn is_externalised_test_file(rel: &str) -> bool {
    let mut components: Vec<&str> = rel.split('/').collect();
    components.pop();
    components.contains(&"tests")
}

// ----------------------------------------------------------------------------
// The `decompose` call-site census
// ----------------------------------------------------------------------------

/// Why one production caller of `DecisionScope::decompose` may hold the
/// funnel's drop licence.
///
/// `decompose` drops a prefix lying under no known root, and the drop is
/// silent. On a claim-bearing path that silence is how a scoped act comes to
/// be recorded as a global one — twice observed, which is why the licence is
/// no longer general. Every caller redeems it one of exactly two ways, and a
/// new caller passing text owes one of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DecomposeRedeemer {
    /// The prefixes came from `core::ops::scope::resolve_scope`, which has
    /// already matched each one to a root — form-tolerantly — and carried
    /// what it could not as a set-aside. Nothing reaching `decompose` from
    /// there is droppable, so the licence is unexercised.
    RootValidated,
    /// The caller records its scopes a second time at completion, through
    /// `record_scopes`, so a prefix under a root that did not exist at
    /// `start()` — a `scan --add` root — lands then rather than never. The
    /// drop happens and is reconciled.
    ReconciledAtCompletion,
}

impl DecomposeRedeemer {
    fn as_str(self) -> &'static str {
        match self {
            DecomposeRedeemer::RootValidated => "root-validated prefixes (the drop is unreachable)",
            DecomposeRedeemer::ReconciledAtCompletion => {
                "reconciled at completion by `record_scopes`"
            }
        }
    }
}

/// The census: every production call site of the funnel, with the count of
/// calls in each file, and the redeemer that answers for them.
///
/// The count is part of the row on purpose. Without it a second call added to
/// a file that already has a row would inherit that row's reason silently —
/// and a reason is about a specific thing a specific caller passes, not about
/// a file. With it, every new call site fails the build until someone states
/// which redeemer it holds.
const DECOMPOSE_CALLERS: &[(&str, usize, DecomposeRedeemer)] = &[
    ("exclude/cli.rs", 1, DecomposeRedeemer::RootValidated),
    ("facts/cli.rs", 1, DecomposeRedeemer::RootValidated),
    ("scan/cli.rs", 1, DecomposeRedeemer::ReconciledAtCompletion),
];

/// Every production call of the funnel, by file. Matching `::decompose(`
/// catches both the `DecisionScope::` spelling every caller uses and the
/// `Self::` one the owner could reach for, while leaving the definition —
/// `pub fn decompose(` — unmatched.
fn decompose_call_sites(src_root: &Path) -> Vec<(String, usize)> {
    let mut found = Vec::new();
    for path in collect_rs_files(src_root) {
        let rel = path
            .strip_prefix(src_root)
            .expect("file under src")
            .to_string_lossy()
            .replace('\\', "/");
        if is_externalised_test_file(&rel) {
            continue;
        }
        let text = fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("failed to read {}: {}", path.display(), e));
        let calls = production_text(&rel, &text).matches("::decompose(").count();
        if calls > 0 {
            found.push((rel, calls));
        }
    }
    found
}

/// Both directions over supplied evidence: a call site the census does not
/// carry, a row the tree no longer produces, a count that has moved.
fn decompose_census_violations(
    found: &[(String, usize)],
    rows: &[(&str, usize, DecomposeRedeemer)],
) -> Vec<String> {
    let mut violations = Vec::new();
    for (file, calls) in found {
        match rows.iter().find(|(f, _, _)| f == file) {
            None => violations.push(format!(
                "{file} calls the funnel {calls}× and holds no row — add one naming \
                 the redeemer that answers for the drop",
            )),
            Some((_, pinned, redeemer)) if pinned != calls => violations.push(format!(
                "{file} calls the funnel {calls}× and its row pins {pinned} — the new \
                 call needs its own redeemer, not `{}`",
                redeemer.as_str(),
            )),
            Some(_) => {}
        }
    }
    for (file, _, _) in rows {
        if !found.iter().any(|(f, _)| f == file) {
            violations.push(format!(
                "the census carries `{file}`, which no longer calls the funnel — \
                 delete the row with the call",
            ));
        }
    }
    violations
}

#[test]
fn every_decompose_caller_names_its_redeemer() {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    let src_root = Path::new(&manifest_dir).join("src");
    let violations =
        decompose_census_violations(&decompose_call_sites(&src_root), DECOMPOSE_CALLERS);
    assert!(
        violations.is_empty(),
        "\n  `DecisionScope::decompose` drops a prefix under no known root, silently. \
         Every production caller states why that is safe for what it passes:\n{}\n",
        violations
            .iter()
            .map(|v| format!("  {v}"))
            .collect::<Vec<_>>()
            .join("\n"),
    );
}

// ----------------------------------------------------------------------------
// The reachability-claim census
// ----------------------------------------------------------------------------

/// One file's standing reachability claims: `.expect(` and `.unwrap()` in the
/// production region of a `cli` or `ops` stratum file.
///
/// Each is a claim that a case cannot happen, made at discipline grade on a
/// path that runs. One of them shipped a panic on `cluster generate`'s
/// commonest invocation, from an `expect` asserting a gate the comment eight
/// lines above it refuted — which is what this baseline is here to stop
/// growing while the standing ones are sorted.
///
/// **The row carries counts and no class.** Sorting each site — unrepresentable
/// via a `match`, or a genuine invariant the type holds and keeps with its
/// reason — is a judgment per site, and a class column filled in here would be
/// this scan making those judgments by seeding itself. So a number here is a
/// debt, not a permission: it may fall in any commit, and it may rise only in
/// one that says why.
struct ReachabilityClaims {
    file: &'static str,
    expects: usize,
    unwraps: usize,
}

/// The baseline, seeded against the real tree. Matched both directions: a
/// count that grows fails (new drift refused), and a count that falls fails
/// until the row is lowered in the same commit (a repair cannot land quietly).
const REACHABILITY_CLAIMS: &[ReachabilityClaims] = &[
    ReachabilityClaims {
        file: "archive/ops/execute.rs",
        expects: 2,
        unwraps: 0,
    },
    ReachabilityClaims {
        file: "compare/ops.rs",
        expects: 1,
        unwraps: 0,
    },
    ReachabilityClaims {
        file: "core/ops/decision.rs",
        expects: 1,
        unwraps: 0,
    },
    ReachabilityClaims {
        file: "exclude/cli.rs",
        expects: 1,
        unwraps: 0,
    },
    ReachabilityClaims {
        file: "expr/ops/filter.rs",
        expects: 0,
        unwraps: 3,
    },
    ReachabilityClaims {
        file: "facts/ops/import.rs",
        expects: 1,
        unwraps: 0,
    },
    ReachabilityClaims {
        file: "retire/ops/verify.rs",
        expects: 3,
        unwraps: 0,
    },
    ReachabilityClaims {
        file: "roots/cli.rs",
        expects: 0,
        unwraps: 2,
    },
    ReachabilityClaims {
        file: "scan/cli.rs",
        expects: 1,
        unwraps: 0,
    },
    ReachabilityClaims {
        file: "scan/ops/receipt.rs",
        expects: 1,
        unwraps: 0,
    },
    ReachabilityClaims {
        file: "scan/ops/types.rs",
        expects: 1,
        unwraps: 0,
    },
    ReachabilityClaims {
        file: "survey/ops/compute.rs",
        expects: 0,
        unwraps: 8,
    },
    ReachabilityClaims {
        file: "survey/ops/orchestrate.rs",
        expects: 0,
        unwraps: 2,
    },
    ReachabilityClaims {
        file: "trail/render.rs",
        expects: 1,
        unwraps: 0,
    },
    ReachabilityClaims {
        file: "trail/ops/compute.rs",
        expects: 0,
        unwraps: 2,
    },
    ReachabilityClaims {
        file: "trail/ops/crossings.rs",
        expects: 3,
        unwraps: 0,
    },
    ReachabilityClaims {
        file: "worklist/ops.rs",
        expects: 0,
        unwraps: 1,
    },
];

/// Count the claims standing in every `cli`/`ops` file's production region.
/// Files with none are absent from the result, which is what makes a row for
/// one of them stale rather than merely zero.
fn reachability_claim_counts(src_root: &Path) -> Vec<(String, usize, usize)> {
    let mut counts = Vec::new();
    for path in collect_rs_files(src_root) {
        let rel = path
            .strip_prefix(src_root)
            .expect("file under src")
            .to_string_lossy()
            .replace('\\', "/");
        if is_externalised_test_file(&rel) {
            continue;
        }
        // The two strata whose code runs against a user, read whole. `cli` is
        // a stratum name, not a filename: the layer model puts `render.rs`,
        // `jsonl.rs` and the crate-root flat files in the same interface layer
        // as `cli.rs`, so a line drawn inside it would be a blind spot rather
        // than a boundary. Domain and repo stay out — a claim there is pure,
        // and its blast radius is a unit test.
        if !matches!(classify_layer(&rel), Layer::Ops | Layer::Interface) {
            continue;
        }
        let text = fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("failed to read {}: {}", path.display(), e));
        let production = production_text(&rel, &text);
        let expects = production.matches(".expect(").count();
        let unwraps = production.matches(".unwrap()").count();
        if expects + unwraps > 0 {
            counts.push((rel, expects, unwraps));
        }
    }
    counts
}

/// Both directions over supplied evidence.
fn reachability_census_violations(
    found: &[(String, usize, usize)],
    rows: &[ReachabilityClaims],
) -> Vec<String> {
    let mut violations = Vec::new();
    for (file, expects, unwraps) in found {
        match rows.iter().find(|r| r.file == file) {
            None => violations.push(format!(
                "{file} makes {expects} `expect` and {unwraps} `unwrap` claims and has \
                 no row — a new claim is drift until someone looks at it",
            )),
            Some(row) if row.expects != *expects || row.unwraps != *unwraps => {
                violations.push(format!(
                    "{file} makes {expects}/{unwraps} (expect/unwrap) claims, its row \
                     pins {}/{} — raise the row with the reason, or lower it with the repair",
                    row.expects, row.unwraps,
                ));
            }
            Some(_) => {}
        }
    }
    for row in rows {
        if !found.iter().any(|(f, _, _)| f == row.file) {
            violations.push(format!(
                "the baseline carries `{}`, which now makes no claims at all — delete \
                 the row with the repair",
                row.file,
            ));
        }
    }
    violations
}

#[test]
fn the_reachability_claims_in_cli_and_ops_are_the_ones_on_the_baseline() {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    let src_root = Path::new(&manifest_dir).join("src");
    let violations =
        reachability_census_violations(&reachability_claim_counts(&src_root), REACHABILITY_CLAIMS);
    assert!(
        violations.is_empty(),
        "\n  An `expect` or an `unwrap` on a path that runs is a reachability claim \
         at discipline grade. The standing ones are counted here so they can be \
         sorted, and so no new one arrives unseen:\n{}\n",
        violations
            .iter()
            .map(|v| format!("  {v}"))
            .collect::<Vec<_>>()
            .join("\n"),
    );
}

// ----------------------------------------------------------------------------
// Two law verifiers that only a spelling check can make
// ----------------------------------------------------------------------------

/// Every identifier in a token stream, at any nesting depth.
///
/// Lexing rather than reading text is what makes this see code and nothing
/// else: comments are gone, and a word inside a string literal stays a
/// literal. A doc comment survives as a `#[doc = "…"]` attribute, whose text
/// is likewise a literal.
fn identifiers_in(stream: TokenStream, out: &mut Vec<(String, proc_macro2::Span)>) {
    for tree in stream {
        match tree {
            TokenTree::Ident(ident) => out.push((ident.to_string(), ident.span())),
            TokenTree::Group(group) => identifiers_in(group.stream(), out),
            _ => {}
        }
    }
}

/// The lens separation law: the sweep's structural engine produces lens-free
/// findings, and the lens is a separate derivation over them.
///
/// The half a spelling check can settle is the **direction of the edge**: the
/// lens names the engine, and the engine never names the lens. That is what
/// leaves a future lens a second function over the same `StructuralSweep`
/// rather than a rewrite — and a single `use super::lens` inside the engine
/// would undo it while every behavioural test stayed green, because ranking
/// computed in the wrong place still ranks correctly.
///
/// Identifiers only, so the prose in the engine's own files may go on
/// explaining what the lens does with what it is handed. A name is refused on
/// containing `lens` in any case, which covers the module, `LensParams`, and
/// anything later derived from either.
#[test]
fn the_structural_engine_never_names_the_lens() {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    let engine = Path::new(&manifest_dir).join("src/sweep/domain/structural");
    let mut files = collect_rs_files(&engine);
    files.push(Path::new(&manifest_dir).join("src/sweep/domain/structural.rs"));
    assert!(
        files.len() > 1,
        "the engine's files moved — this guard is now watching nothing"
    );

    let mut violations = Vec::new();
    for path in &files {
        let text = fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("failed to read {}: {}", path.display(), e));
        let stream: TokenStream = text
            .parse()
            .unwrap_or_else(|e| panic!("failed to lex {}: {}", path.display(), e));
        let mut idents = Vec::new();
        identifiers_in(stream, &mut idents);
        for (ident, span) in idents {
            if ident.to_ascii_lowercase().contains("lens") {
                violations.push(format!(
                    "{}:{}: names `{ident}` — the engine must not know the lens exists",
                    path.file_name().unwrap_or_default().to_string_lossy(),
                    span.start().line,
                ));
            }
        }
    }
    assert!(
        violations.is_empty(),
        "\n  The lens separation law is one-way: the lens reads the engine's findings, \
         the engine knows nothing of the lens.\n{}\n",
        violations
            .iter()
            .map(|v| format!("  {v}"))
            .collect::<Vec<_>>()
            .join("\n"),
    );
}

/// The fate vocabulary's words, read out of the file that owns them.
///
/// Read rather than spelled, because a guard against literals that carries the
/// literals it refuses would be the defect wearing the defence's clothes — and
/// would go quiet the day a word is added. The arm shape is
/// `Self::Variant => "word",` and nothing else in the vocabulary's file writes
/// a string on the right of a match arm.
fn fate_vocabulary_words(src_root: &Path) -> Vec<String> {
    let owner = src_root.join("core/domain/fate.rs");
    let text = fs::read_to_string(&owner)
        .unwrap_or_else(|e| panic!("failed to read {}: {}", owner.display(), e));
    let mut words = Vec::new();
    for (_, after) in text
        .match_indices("=> \"")
        .map(|(i, _)| (i, &text[i + 4..]))
    {
        if let Some(end) = after.find('"') {
            words.push(after[..end].to_string());
        }
    }
    words.sort();
    words.dedup();
    assert!(
        words.len() >= 4,
        "the fate vocabulary reads as {words:?} — the arm shape changed and this \
         guard is now refusing almost nothing"
    );
    words
}

/// The never-literal law: a transition or posture word is derived, never
/// written down.
///
/// The book is where it bites hardest — a retired root's story is read forever
/// and cannot be rewritten — and it is the reason the law's correct scope is
/// *transitions*: the standings beside them (`covered`, `present`,
/// `missing_unexplained`, `contentless`) are present-tense facts with no
/// derivation to come from, so they are named constants in one place instead.
///
/// Only a spelling check settles this. A test comparing a derived word to the
/// vocabulary's own answer passes just as happily when the word was typed in,
/// because a literal that is currently correct is indistinguishable from a
/// derivation at run time — which is exactly the defect: it stops being correct
/// the day the vocabulary moves, silently, in a document nobody may rewrite.
///
/// Comments are not exempt, deliberately: prose spells these words in
/// backticks throughout, so a **quoted** one is a spelling of the wire word and
/// carries the same drift.
#[test]
fn the_book_never_spells_a_fate_word_as_a_literal() {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    let src_root = Path::new(&manifest_dir).join("src");
    let words = fate_vocabulary_words(&src_root);

    let mut violations = Vec::new();
    for path in collect_rs_files(&src_root.join("retire")) {
        let rel = path
            .strip_prefix(&src_root)
            .expect("file under src")
            .to_string_lossy()
            .replace('\\', "/");
        if is_externalised_test_file(&rel) {
            continue;
        }
        let text = fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("failed to read {}: {}", path.display(), e));
        let production = production_text(&rel, &text);
        for (line_no, line) in production.lines().enumerate() {
            for word in &words {
                if line.contains(&format!("\"{word}\"")) {
                    violations.push(format!("{rel}:{}: spells `\"{word}\"`", line_no + 1));
                }
            }
        }
    }
    assert!(
        violations.is_empty(),
        "\n  A fate word written down is a fate word that stops agreeing with the \
         vocabulary the day the vocabulary moves — in a book that is never \
         rewritten. Derive it through `fate_transition`:\n{}\n",
        violations
            .iter()
            .map(|v| format!("  {v}"))
            .collect::<Vec<_>>()
            .join("\n"),
    );
}

// ============================================================================
// Self-tests (August's spec)
// ============================================================================

#[cfg(test)]
mod self_tests {
    use super::*;

    // ------------------------------------------------------------------
    // The door-type matcher.
    //
    // A matcher exercised only by a tree scan that returns zero cannot be
    // told apart from a disarmed one. These plant the defect it names, in
    // both of its spellings, and one legitimate shape it must not flag.
    // ------------------------------------------------------------------

    /// The underscore, which is what the guard was written for.
    #[test]
    fn a_planted_underscore_arm_on_a_door_type_is_seen() {
        let text = "\
fn f(door: Door) -> usize {
    match door {
        Door::Open(scope) => scope.len(),
        _ => 0,
    }
}
";
        assert_eq!(wildcard_arms_on_door_types(text), 1);
    }

    /// **The same silent decision, with a name on it.** A bare binding is
    /// irrefutable exactly as `_` is; the only difference is that it reads
    /// like a considered arm. This is the shape the first version of the
    /// guard let through.
    #[test]
    fn a_planted_named_catch_all_on_a_door_type_is_seen() {
        let text = "\
fn f(lookup: RootLookup) -> i64 {
    match lookup {
        RootLookup::Found(id) => id,
        other => fallback(other),
    }
}
";
        assert_eq!(wildcard_arms_on_door_types(text), 1);
    }

    /// A match that names every arm is not flagged, and neither is a nested
    /// match on some other type that carries its own wildcard — the arm
    /// indentation is what keeps one from lending its catch-all to the other.
    #[test]
    fn a_fully_named_door_match_is_not_flagged() {
        let text = "\
fn f(door: Door) -> usize {
    match door {
        Door::Open(scope) => match scope.kind {
            Kind::A => 1,
            _ => 2,
        },
        Door::Closed(closed) => closed.places.len(),
    }
}
";
        assert_eq!(wildcard_arms_on_door_types(text), 0);
    }

    /// The catch-all predicate itself, over the shapes an arm can take.
    ///
    /// The negatives include three that are **lowercase** — `x @ Door::Open`,
    /// `ref other`, `d @ _` — because without them every negative is rejected
    /// by the first character alone and the rest of the predicate is never
    /// exercised at all.
    #[test]
    fn a_catch_all_is_a_pattern_that_refuses_nothing() {
        for pattern in ["_ ", "_ if x.is_empty() ", "other ", "anything_else ", "d "] {
            assert!(is_catch_all(pattern), "{pattern:?} matches everything");
        }
        for pattern in [
            "Door::Open(scope) ",
            "RootLookup::Found(id) ",
            "Some(x) ",
            "A | B ",
            "Door::Open(_) ",
            "x @ Door::Open(_) ",
            "ref other ",
            "d @ _ ",
        ] {
            assert!(!is_catch_all(pattern), "{pattern:?} refuses something");
        }
    }

    /// The guard reads its own tree, so the corpus it runs against must not
    /// be empty — a scan over nothing also returns zero.
    ///
    /// **Asked per type**, over the same two directories the guard scans: a
    /// stale entry in `DOOR_TYPES` whose type has been renamed away is half a
    /// disarmed guard, and a total across types hides it behind the other's
    /// hits.
    #[test]
    fn every_door_type_is_actually_present_in_the_tree() {
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
        let base = Path::new(&manifest_dir);
        let corpus: Vec<String> = ["src", "tests"]
            .iter()
            .flat_map(|dir| collect_rs_files(&base.join(dir)))
            .map(|path| fs::read_to_string(&path).unwrap())
            .collect();

        for ty in DOOR_TYPES {
            let files = corpus
                .iter()
                .filter(|text| text.contains(&format!("{ty}::")))
                .count();
            assert!(
                files > 0,
                "`{ty}` is on the guard's table and named nowhere in the tree it scans — \
                 either the type was renamed and the row is stale, or the guard is \
                 half disarmed"
            );
        }
    }

    // ------------------------------------------------------------------
    // The containment-probe matcher.
    //
    // These plant probes deliberately. This file is one of `PROBE_HOMES`
    // for exactly that reason: a matcher that cannot be exercised against
    // the defect it names is the class specimen, not a defense.
    // ------------------------------------------------------------------

    /// Red smoke against the defect the guard names: the hand-rolled
    /// boundary test, in the shape it actually took in the tree.
    #[test]
    fn a_planted_positional_probe_is_seen() {
        let text = "fn under(p: &str, d: &str) -> bool {\n    \
                    p == d || (p.starts_with(d) && p.as_bytes().get(d.len()) == Some(&b'/'))\n}\n";
        assert_eq!(positional_probes(text), 1);
    }

    /// The guard must not fire on the correct spelling. A guard that flags
    /// right answers is how a guard becomes something people write around.
    #[test]
    fn a_component_wise_containment_test_is_not_seen() {
        let text = "pub fn path_is_under(path: &str, prefix: &str) -> bool {\n    \
                    Path::new(path).starts_with(prefix)\n}\n";
        assert_eq!(positional_probes(text), 0);
    }

    /// Lines copied from the tree, **written independently of
    /// `PROBE_SPELLINGS`** — which is the whole point of them.
    ///
    /// An earlier version of the test below generated its haystack from the
    /// constant, so every entry matched itself and a typo in the constant
    /// passed while disarming the guard. That is the defect this file exists
    /// to refuse, committed inside the instrument that refuses it. The corpus
    /// breaks the circle: a mistyped spelling matches nothing here.
    const PROBE_CORPUS: &[&str] = &[
        // src/core/domain/source.rs, as it stood before the repair
        "full_path.starts_with(dir) && full_path.as_bytes().get(dir.len()) == Some(&b'/')",
        // the same idiom reached by indexing rather than by `get`
        "prefix_len < s.len() && s.as_bytes()[prefix_len] == b'/'",
        // src/facts/ops/import.rs — a fixed offset into a timestamp
        "s.len() >= 19 && s.chars().nth(4) == Some(':')",
        // the byte-iterator form
        "p.bytes().nth(d.len()) == Some(b'/')",
    ];

    /// A typo in `PROBE_SPELLINGS` is a matcher that cannot fire, and a
    /// matcher that cannot fire is the class specimen rather than a defense.
    ///
    /// Two directions, and the corpus is what makes either of them mean
    /// something: every spelling must be found in a line written without
    /// reference to it, and every one of those lines must be seen by the
    /// matcher as a whole.
    #[test]
    fn every_probe_spelling_is_seen() {
        for spelling in PROBE_SPELLINGS {
            let hits = PROBE_CORPUS.iter().filter(|l| l.contains(spelling)).count();
            assert_eq!(
                hits, 1,
                "the spelling {spelling:?} matches no line in the corpus — it is mistyped, \
                 or the corpus lost the line it was copied from. Either way the guard is \
                 disarmed for that spelling."
            );
        }

        for line in PROBE_CORPUS {
            assert_eq!(
                positional_probes(line),
                1,
                "the matcher does not see this probe: {line:?}"
            );
        }
    }

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
    // The two censuses — red smoke over synthetic evidence
    // ========================================================================

    #[test]
    fn the_production_text_blanks_every_test_module_at_any_depth() {
        // A test module at the top level, and one nested inside a production
        // module — the repo-stratum shape, where the first is not the only one.
        let text = "\
fn f() { g().unwrap() }
#[cfg(test)]
mod t {
    fn a() { h().unwrap() }
}
mod source {
    fn s() { i().unwrap() }
    #[cfg(test)]
    mod tests {
        fn b() { j().unwrap() }
    }
}
";
        let production = production_text("synthetic.rs", text);
        assert_eq!(
            production.matches(".unwrap()").count(),
            2,
            "the two production calls survive and the two test ones do not: {production}",
        );
        assert!(production.contains("fn f()") && production.contains("fn s()"));
        assert!(!production.contains("fn a()") && !production.contains("fn b()"));
        // Blanked, not deleted: a line keeps its number.
        assert_eq!(production.lines().count(), text.lines().count());
    }

    #[test]
    fn a_cfg_attribute_that_is_not_about_test_leaves_its_module_alone() {
        let text = "#[cfg(unix)]\nmod u {\n    fn f() { g().unwrap() }\n}\n";
        assert!(production_text("synthetic.rs", text).contains(".unwrap()"));
    }

    #[test]
    fn an_externalised_test_file_is_known_by_its_directory() {
        assert!(is_externalised_test_file("trail/ops/tests/compute.rs"));
        assert!(is_externalised_test_file("retire/ops/tests/fixtures.rs"));
        // The name only counts as a directory: a file called `tests.rs` is
        // production code as far as placement can tell.
        assert!(!is_externalised_test_file("trail/ops/tests.rs"));
        assert!(!is_externalised_test_file("trail/ops/compute.rs"));
    }

    #[test]
    fn the_decompose_census_refuses_a_new_caller_and_a_stale_row() {
        let rows = &[("exclude/cli.rs", 1, DecomposeRedeemer::RootValidated)];

        // Whole: the tree produces exactly what the census carries.
        assert!(decompose_census_violations(&[("exclude/cli.rs".into(), 1)], rows).is_empty());

        // A caller nobody vouched for.
        let v = decompose_census_violations(
            &[("exclude/cli.rs".into(), 1), ("survey/cli.rs".into(), 1)],
            rows,
        );
        assert_eq!(v.len(), 1, "{v:?}");
        assert!(
            v[0].contains("survey/cli.rs") && v[0].contains("holds no row"),
            "{v:?}"
        );

        // A second call in a file that already had one: the existing reason
        // answers for the first call, never for a call it has not seen.
        let v = decompose_census_violations(&[("exclude/cli.rs".into(), 2)], rows);
        assert_eq!(v.len(), 1, "{v:?}");
        assert!(v[0].contains("pins 1"), "{v:?}");

        // The other direction: the caller is gone and the row outlived it.
        let v = decompose_census_violations(&[], rows);
        assert_eq!(v.len(), 1, "{v:?}");
        assert!(v[0].contains("no longer calls the funnel"), "{v:?}");
    }

    #[test]
    fn the_reachability_census_refuses_growth_and_a_silent_repair() {
        let rows = &[ReachabilityClaims {
            file: "survey/ops/compute.rs",
            expects: 0,
            unwraps: 8,
        }];

        assert!(
            reachability_census_violations(&[("survey/ops/compute.rs".into(), 0, 8)], rows)
                .is_empty()
        );

        // One more claim than the baseline carries.
        let v = reachability_census_violations(&[("survey/ops/compute.rs".into(), 0, 9)], rows);
        assert_eq!(v.len(), 1, "{v:?}");
        assert!(v[0].contains("pins 0/8"), "{v:?}");

        // One fewer: a repair that did not lower its row is as invisible as
        // drift, so it fails the same way.
        let v = reachability_census_violations(&[("survey/ops/compute.rs".into(), 0, 7)], rows);
        assert_eq!(v.len(), 1, "{v:?}");
        assert!(v[0].contains("pins 0/8"), "{v:?}");

        // A file that had none and now makes one.
        let v = reachability_census_violations(
            &[
                ("survey/ops/compute.rs".into(), 0, 8),
                ("ls/ops.rs".into(), 1, 0),
            ],
            rows,
        );
        assert_eq!(v.len(), 1, "{v:?}");
        assert!(
            v[0].contains("ls/ops.rs") && v[0].contains("no row"),
            "{v:?}"
        );

        // And the repair that empties a file entirely.
        let v = reachability_census_violations(&[], rows);
        assert_eq!(v.len(), 1, "{v:?}");
        assert!(v[0].contains("no claims at all"), "{v:?}");
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
                "placement_shape",
                "evaluate",
                "Pattern",
                "EvalContext",
                "Unmeasured",
                "ScopeVantage",
                "prefetch_pattern_facts",
                "PatternFacts",
                "resolve_fact_value",
                "get_builtin_value",
                "fact_value_to_display",
                "SourceAttributes",
                "ParsedFactKey",
                "BuiltinKey",
                "BuiltinKeyCategory",
                "BuiltinKeyVisibility",
                "SCOPE_REL_PATH",
                "OBJECT_HASH",
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
                "run_crossings",
                "run_show",
                "CrossingsArgs",
                "TrailArgs",
                "RowAspect",
                "ScopeMatch",
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
                "TrailExit",
            ],
        ),
        ("sweep", &["run"]),
        // main.rs reaches DetailMode/SurveyOptions/run for the survey
        // command, and SurveyExit to turn a refused frame into an exit code;
        // the contentless-law canary reaches the rest.
        (
            "survey",
            &[
                "DetailMode",
                "ObjectIndex",
                "SurveyExit",
                "SurveyOptions",
                "SurveyOutcome",
                "SurveyParams",
                "compute_survey",
                // Which channel survey's scope statement — and the closed
                // door's — belongs on is survey's own knowledge; the front
                // door asks rather than re-deriving it.
                "machine_shaped_stdout",
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
        // The two command entry points main.rs dispatches, plus the
        // physical-identity law and the types naming its subject, which the
        // contentless-law canary asks directly.
        (
            "scan",
            &[
                "run",
                "find_candidates",
                "same_physical_file",
                "FileObservation",
                "IdentityClaim",
            ],
        ),
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
