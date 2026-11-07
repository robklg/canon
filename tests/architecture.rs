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

mod common;

use common::{collect_rs_files, is_externalised_test_file, production_text};

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

/// Paths that classify as `Layer::Testing`: the shared fixtures, and the two
/// law tests at the crate root that read across every surface of their law —
/// the contentless law's canary and the source-value law's verifier.
const TESTING_PATHS: &[&str] = &[
    "core/testing/",
    "contentless_law_tests.rs",
    "source_value_law_tests.rs",
];

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
        #[expect(clippy::string_slice, reason = "just past a matched ASCII prefix")]
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

/// The crate-root modules the upward-dependency rule guards: every flat file
/// `classify_layer` reads as interface. A flat file it reads as testing (the
/// law tests) is not an interface module, so one law test may read another's
/// helpers; `main` is the entry, not a module anything imports.
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
                if stem != "main" && classify_layer(&format!("{stem}.rs")) == Layer::Interface {
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
/// **No site outside the facility is exempt.** `facts delete` and import
/// refuse a `source.` name without spelling it: both read the facility's own
/// name reader, where such a name has no content reading. A site that matched
/// the same bytes for a different verb would be listed by name with its verb,
/// and the count is exact, so a second spelling inside an exempt file fails
/// this too.
///
/// The scan covers test code as well as production. A test spelling the set
/// out is still a spelling of it, and admitting one would need the same
/// deliberate act as admitting a production one.
#[test]
fn the_context_supplied_set_is_spelled_only_inside_expr() {
    // The two prefix tests the old skiplist was built from. The content-hash
    // key, the listed half's other name, is deliberately absent: it is an
    // exact-match on an ordinary string and matching it would flag every
    // unrelated mention of the key.
    const SPELLINGS: &[&str] = &["starts_with(\"source.\")", "starts_with(\"scope.\")"];

    /// Each exemption, with the verb it serves and how many times it spells
    /// one of the above.
    const EXEMPT: &[(&str, usize)] = &[];

    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    let base = Path::new(&manifest_dir);

    // The facility is where the set is allowed to be spelled.
    let found = spelled_only_here(base, &["src/expr/", "src/expr.rs"], |text| {
        SPELLINGS.iter().map(|s| text.matches(s).count()).sum()
    });

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
const PROBE_EXEMPT: &[(&str, usize)] = &[];

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
/// `Door`, `RootLookup` and `PrefixOutcome` are the three answers the
/// boundary, the root-spec door and the manifest door give about a place the
/// user closed. Each consumer's arm *is* its declared permit — a view sets
/// aside, a remembering view reads, an act refuses — so a wildcard is not a
/// shorthand here: it is a surface silently picking whatever the other arm
/// happened to be, which is how a closed door came to read as "all roots" at
/// seven commands at once.
///
/// `PrefixOutcome` joins them because its own catch-all costs a lock file: a
/// `_` arm folding `Parked` in with `Confirmed` selects from behind the door,
/// and one folding it in with `SetAside` refuses a manifest with the wrong
/// cause and then deletes the lock it was refusing to touch.
const DOOR_TYPES: &[&str] = &["Door", "RootLookup", "PrefixOutcome"];

/// A "spelled only here" census, the shared shape behind three guards that
/// each pin a rule to one owner: every file outside `homes` where `count_in`
/// finds the watched pattern, path paired with its count, sorted.
///
/// `homes` are path prefixes exempt from the count entirely — matched by
/// `starts_with`, so a whole file (`"tests/architecture.rs"`) and a directory
/// (`"src/expr/"`) are the same kind of entry: the rule's own owner, and —
/// for a guard whose self-tests plant the defect it names — the guard's own
/// file. Anything a caller still expects to find *outside* the homes (a
/// different verb spelling the same bytes for its own reason) is not this
/// function's business: it reports every hit past the homes and leaves
/// matching that against an exemption table to the caller.
///
/// The walk covers `src/` and `tests/` alike, at every call site: a
/// hand-rolled second spelling of a rule inside a test is still a second
/// spelling of it, and admitting one should need the same deliberate act as
/// admitting a production one.
fn spelled_only_here(
    manifest_dir: &Path,
    homes: &[&str],
    count_in: impl Fn(&str) -> usize,
) -> Vec<(String, usize)> {
    let mut found: Vec<(String, usize)> = Vec::new();
    for dir in ["src", "tests"] {
        for path in collect_rs_files(&manifest_dir.join(dir)) {
            let rel = path
                .strip_prefix(manifest_dir)
                .unwrap_or(&path)
                .to_string_lossy()
                .replace('\\', "/");
            if homes.iter().any(|home| rel.starts_with(home)) {
                continue;
            }
            let text = fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("failed to read {}: {}", path.display(), e));
            let count = count_in(&text);
            if count > 0 {
                found.push((rel, count));
            }
        }
    }
    found.sort();
    found
}

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
/// type" when the pattern left of its `=>` spells one of [`DOOR_TYPES`]
/// followed by `::`, however it is qualified. A **catch-all** is `_`, `_ if …`, or a bare
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

    // This file is exempt wholesale so the self-tests below can plant probes.
    let found = spelled_only_here(
        base,
        &["tests/architecture.rs"],
        wildcard_arms_on_door_types,
    );

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

    let found = spelled_only_here(base, PROBE_HOMES, positional_probes);

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

/// Where a read of the machine's time zone may be spelled: its owner, and this file, which
/// declares the spellings and plants them in its self-test.
const ZONE_HOMES: &[&str] = &["src/core/domain/time/zone.rs", "tests/architecture.rs"];

/// The ways a program reads the machine's zone: chrono's local clock, jiff's system zone
/// and its zone-carrying "now", and the `TZ` variable read directly.
const ZONE_SPELLINGS: &[&str] = &[
    "chrono::Local",
    "Local::now(",
    "Local.timestamp",
    "Local.from_local",
    "TimeZone::system(",
    "TimeZone::try_system(",
    "Zoned::now(",
    "var(\"TZ\")",
    "var_os(\"TZ\")",
];

/// Each exemption, with the verb it serves and how many times it spells a read.
const ZONE_EXEMPT: &[(&str, usize)] = &[];

/// How many reads the owner itself spells, outside comments and its tests: `TZ` once and
/// the system zone once.
const ZONE_OWNER_READS: usize = 2;

/// How many machine-zone spellings a text contains.
fn machine_zone_reads(text: &str) -> usize {
    ZONE_SPELLINGS.iter().map(|s| text.matches(s).count()).sum()
}

/// **The user's-zone owner**: the machine's zone is read in one place,
/// `core::domain::time::zone::UserZone::machine`, and a read anywhere else fails the build.
///
/// Every time Canon shows a person reads on the user's clock through one rule, with the
/// zone handed to it as a parameter. A second read of the machine's zone is a second answer
/// to "which clock?" — correct on the machine it was written on and wrong on the next — and
/// a manifest's run must never read it at all. So the read has one owner, and this guard
/// holds the rest of the tree to it.
///
/// Matched both directions: a read outside the owner fails unless it is a named exemption
/// with its exact count, an exemption whose read has gone fails, and the owner must still
/// spell exactly its own reads — so the owner moving, or growing a second read, fails too.
///
/// **What this does not catch.** The scan is textual. An aliased import
/// (`use jiff::tz::TimeZone as Tz;` then `Tz::system()`), a read of `/etc/localtime` by
/// path, `std::env::vars()` searched for `TZ`, and a compile-time `env!("TZ")` all go
/// unseen, as would a dependency that reads the zone itself.
#[test]
fn the_machine_zone_is_read_only_by_its_owner() {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    let base = Path::new(&manifest_dir);

    let found = spelled_only_here(base, ZONE_HOMES, machine_zone_reads);
    let expected: Vec<(String, usize)> = ZONE_EXEMPT
        .iter()
        .map(|(f, n)| ((*f).to_string(), *n))
        .collect();
    assert_eq!(
        found, expected,
        "\n  The machine's time zone is read outside its owner.\n  \
         Found: {found:?}\n  Exempt: {expected:?}\n  \
         Take the user's zone as a parameter, handed down from the interface \
         (`crate::user_zone()` in `main.rs`), and read times through the frame rule.\n  \
         If an exemption is genuinely gone, delete its row in the same commit."
    );

    let owner = ZONE_HOMES[0];
    let raw = fs::read_to_string(base.join(owner)).expect("the owner's file");
    let code: String = production_text(owner, &raw)
        .lines()
        .filter(|line| !line.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n");
    assert_eq!(
        machine_zone_reads(&code),
        ZONE_OWNER_READS,
        "\n  The owner `{owner}` no longer spells exactly its own reads of the machine's zone.\n  \
         If the owner moved, move `ZONE_HOMES` with it; if it reads the zone a new way, \
         say so in `ZONE_OWNER_READS`."
    );
}

/// The ways zone rules are looked up by a name or a rule: jiff's zone database and its rule
/// parser. The machine's own zone is the guard above's; these are any zone a person names.
const ZONE_RULE_SPELLINGS: &[&str] = &["TimeZone::get(", "TimeZone::posix("];

/// How many lookups the owner itself spells, outside comments and its test module: the rule
/// parser once in the machine classifier, the database and the rule parser once each in the
/// zone-name parse, and the database once in the test-only `UserZone::for_test`, which sits
/// outside the test module.
const ZONE_RULE_OWNER_LOOKUPS: usize = 4;

/// How many zone-rule lookups a text contains.
fn zone_rule_lookups(text: &str) -> usize {
    ZONE_RULE_SPELLINGS
        .iter()
        .map(|s| text.matches(s).count())
        .sum()
}

/// **The zone-name parse**: a zone a person names — a manifest's `zone` line — is read in
/// one place, `core::domain::time::zone::parse_zone_name`, beside the user's-zone owner, and
/// a lookup of zone rules anywhere else fails the build.
///
/// A second reading of a zone name is a second answer to "which rules does this name
/// mean?": a parser in `archive` that accepted a rule string, or flattened one to today's
/// offset, would place files on a clock the manifest never recorded. So the lookup has one
/// owner. Matched both directions, like the guard above: a lookup outside the owner fails
/// (there are no exemptions), and the owner must still spell exactly its own lookups.
///
/// **What this does not catch**: the same blind spots as the guard above — an aliased
/// import, and jiff's other doors to a named zone (its zone-database handle, `"…[Europe/Amsterdam]"`
/// parsed as a `Zoned`), which nothing in the tree uses.
#[test]
fn zone_rules_are_looked_up_only_by_their_owner() {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    let base = Path::new(&manifest_dir);

    let found = spelled_only_here(base, ZONE_HOMES, zone_rule_lookups);
    assert!(
        found.is_empty(),
        "\n  Zone rules are looked up by name outside the zone-name parse.\n  \
         Found: {found:?}\n  \
         Read a zone a person names through `parse_zone_name`, and the machine's through \
         `UserZone::machine`; hand the zone on as a `RecordedZone` or a `UserZone`."
    );

    let owner = ZONE_HOMES[0];
    let raw = fs::read_to_string(base.join(owner)).expect("the owner's file");
    let code: String = production_text(owner, &raw)
        .lines()
        .filter(|line| !line.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n");
    assert_eq!(
        zone_rule_lookups(&code),
        ZONE_RULE_OWNER_LOOKUPS,
        "\n  The owner `{owner}` no longer spells exactly its own zone-rule lookups.\n  \
         If the owner moved, move `ZONE_HOMES` with it; if it looks rules up a new way, \
         say so in `ZONE_RULE_OWNER_LOOKUPS`."
    );
}

/// The zone-rule matcher counts each spelling it declares, red-smoked on its own fixtures.
#[test]
fn the_zone_rule_matcher_counts_each_spelling() {
    for spelling in ZONE_RULE_SPELLINGS {
        assert_eq!(zone_rule_lookups(spelling), 1, "{spelling}");
    }
    assert_eq!(
        zone_rule_lookups("TimeZone::get(\"UTC\"); TimeZone::posix(rule); TimeZone::get(n)"),
        3
    );
    assert_eq!(
        zone_rule_lookups("TimeZone::UTC; TimeZone::fixed(o); TimeZone::try_system()"),
        0
    );
}

/// The zone matcher counts each spelling it declares — the matcher red-smoked on its own
/// fixtures, so a spelling dropped from the list fails here rather than going quiet.
#[test]
fn the_zone_matcher_counts_each_spelling() {
    for spelling in ZONE_SPELLINGS {
        assert_eq!(machine_zone_reads(spelling), 1, "{spelling}");
    }
    assert_eq!(
        machine_zone_reads("let now = chrono::Local::now(); std::env::var(\"TZ\")"),
        3
    );
    assert_eq!(
        machine_zone_reads("Timestamp::now(); TimeZone::get(\"UTC\"); env(\"TZ\", z)"),
        0
    );
}

// ============================================================================
// Two censuses over the production tree
//
// The production/test split (`production_text`, `is_externalised_test_file`)
// moved to `common` when the closed-door conformance pass's suspension-read
// census became its second consumer.
// ============================================================================

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
// The `DecisionRecorder::start` settlement census
// ----------------------------------------------------------------------------

/// How one production caller of `DecisionRecorder::start` settles the status
/// claim that `start()` registers.
///
/// `start()` writes `started` before anything happens — a claim about a run
/// that has not finished. A row that still reads `started` afterwards means
/// the run never reached a last act at all, which is the recovery signal the
/// provenance model publishes. That is only true while *every* caller settles
/// some way, and the ways are not interchangeable, so each caller declares
/// which one answers for it.
///
/// Exhaustive with no `_` arm: a fifth shape cannot arrive without a name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Settlement {
    /// Wrapped in `ArmedRecorder`: the guard's cleanup writes the command's
    /// terminal on every non-panicking exit. The recorder's own battery holds
    /// the mechanism.
    Armed,
    /// Started on `&tx`. A `?` rolls the row back with the transaction, so
    /// there is no row left to mis-describe.
    RollsBackWithTransaction,
    /// A closure or wrapper whose single `Err` arm settles, so every `?` in
    /// the body reaches it by construction. The file carries the test
    /// declaring it.
    OwnWrapper,
    /// Zero `?` after `start`. It settles on named paths, held by its own
    /// refusal pins.
    Positional,
}

impl Settlement {
    fn as_str(self) -> &'static str {
        match self {
            Settlement::Armed => "armed (`ArmedRecorder` settles on drop)",
            Settlement::RollsBackWithTransaction => "started on `&tx` (the row rolls back)",
            Settlement::OwnWrapper => "its own wrapper's single `Err` arm",
            Settlement::Positional => "positional (no `?` after `start`)",
        }
    }
}

/// The census: every production file that starts a decision, how many times,
/// and which shape settles those starts.
///
/// The count is part of the row for the reason the `decompose` census gives:
/// a new start added to a file that already has a row would otherwise inherit
/// that row's declaration silently, and a declaration is about a specific call
/// site's shape, not about a file.
///
/// **A file may hold several rows**, one per shape, and its counts sum —
/// `facts/ops/maintain.rs` arms three of its four and settles the fourth
/// through its own wrapper, because `execute_delete`'s window needs a mutable
/// connection that an armed guard's shared borrow forbids.
///
/// What this pins is that each caller *declares* its shape; it cannot verify
/// the shape. Each tag is backed by the thing it names: the guard's battery,
/// the `&tx` start, the declaring tests scan, retirement and facts carry, and
/// apply's two refusal pins.
const RECORDER_START_SITES: &[(&str, usize, Settlement)] = &[
    ("archive/cli/cluster.rs", 2, Settlement::Armed),
    ("archive/ops/execute.rs", 1, Settlement::Positional),
    (
        "exclude/ops/runner.rs",
        1,
        Settlement::RollsBackWithTransaction,
    ),
    ("facts/cli/import.rs", 1, Settlement::Armed),
    (
        "facts/ops/convert.rs",
        1,
        Settlement::RollsBackWithTransaction,
    ),
    ("facts/ops/maintain.rs", 3, Settlement::Armed),
    ("facts/ops/maintain.rs", 1, Settlement::OwnWrapper),
    ("notes/ops.rs", 1, Settlement::RollsBackWithTransaction),
    ("retire/ops/ceremony.rs", 1, Settlement::OwnWrapper),
    ("roots/ops.rs", 3, Settlement::Armed),
    ("scan/cli.rs", 1, Settlement::OwnWrapper),
];

/// Every production start of a decision, by file.
///
/// Matching `DecisionRecorder::start(` with the paren leaves the definition —
/// `pub fn start(` — unmatched, and leaves a comment naming the function
/// without calling it unmatched too.
///
/// **Blind spot, named**: `production_text` blanks `cfg(test)` modules but not
/// comments, so a commented-out call counts. The same aperture the `decompose`
/// census has, and for the same reason — a text scan cannot parse.
fn recorder_start_sites(src_root: &Path) -> Vec<(String, usize)> {
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
        let calls = production_text(&rel, &text)
            .matches("DecisionRecorder::start(")
            .count();
        if calls > 0 {
            found.push((rel, calls));
        }
    }
    found
}

/// Both directions over supplied evidence: a file that starts a decision and
/// declares nothing, a row whose file no longer starts one, a count that has
/// moved. A file's rows are summed before comparing, so several shapes in one
/// file are one arithmetic statement.
fn recorder_census_violations(
    found: &[(String, usize)],
    rows: &[(&str, usize, Settlement)],
) -> Vec<String> {
    let mut violations = Vec::new();
    for (file, calls) in found {
        let declared: usize = rows
            .iter()
            .filter(|(f, _, _)| f == file)
            .map(|(_, n, _)| n)
            .sum();
        if declared == 0 {
            violations.push(format!(
                "{file} starts a decision {calls}× and declares no settlement — add a row \
                 naming the shape that settles the row `start()` registers",
            ));
        } else if declared != *calls {
            let shapes: Vec<&str> = rows
                .iter()
                .filter(|(f, _, _)| f == file)
                .map(|(_, _, s)| s.as_str())
                .collect();
            violations.push(format!(
                "{file} starts a decision {calls}× and its rows declare {declared} ({}) — \
                 the new start needs its own declaration, not one already spoken for",
                shapes.join(", "),
            ));
        }
    }
    for (file, _, _) in rows {
        if !found.iter().any(|(f, _)| f == file) {
            violations.push(format!(
                "the census carries `{file}`, which no longer starts a decision — \
                 delete the row with the call",
            ));
        }
    }
    violations
}

#[test]
fn every_recorder_start_site_declares_its_settlement() {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    let src_root = Path::new(&manifest_dir).join("src");
    let violations =
        recorder_census_violations(&recorder_start_sites(&src_root), RECORDER_START_SITES);
    assert!(
        violations.is_empty(),
        "\n  `DecisionRecorder::start` registers a claim that the row's run has not \
         finished. A row left `started` is supposed to mean the run never reached a last \
         act — which holds only while every caller settles:\n{}\n",
        violations
            .iter()
            .map(|v| format!("  {v}"))
            .collect::<Vec<_>>()
            .join("\n"),
    );
}

// ----------------------------------------------------------------------------
// The empty-remainder caller census
// ----------------------------------------------------------------------------

/// Why one production caller of `path_at_or_under_sql`/`path_strictly_under_sql`
/// may pass an argument the helpers themselves answer wrong on.
///
/// Both helpers are spelled for a non-empty prefix. Their SQL never matches a
/// real `rel_path`/`rel_prefix` column on an empty one: a real remainder
/// never starts with `/`, so `path_strictly_under_sql`'s clause is false for
/// every row, and `path_at_or_under_sql`'s exact-match arm only ever catches
/// a column that is itself stored empty. Conjugating the empty case inside
/// the projection — so a caller cannot forget the branch — is the pair's own
/// repair and does not live here; until it does, every production caller
/// conjugates the case in its own place, and a new caller passing text owes
/// one of the two reasons below.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EmptyRemainderGuard {
    /// The call site itself branches on `rel_path`/`rel_prefix`.is_empty()
    /// and only ever reaches the helper on the non-empty arm; the empty case
    /// is answered by a hand-written whole-scope query instead (no boundary
    /// predicate at all, since none is needed to mean "everything").
    BranchesAtTheCallSite,
    /// Reached only through `trail::ops::place::history_evidence_at`'s
    /// ordered evidence chain — presence, then notes, both correct on an
    /// empty remainder, asked first — so these two SQL calls go unreached
    /// whenever the root carries any source or note history at all. One of
    /// the chain's two callers, `trail::cli::run`'s scope-evidence gate,
    /// excludes every root's own top before reaching the chain, closing the
    /// gap for it entirely. The other, `trail::ops::crossings::counterpart_is_known`,
    /// does not: a root named as a counterpart that has never held a source
    /// or a note, yet carries a decision or an extraction scoped at its own
    /// top, can still reach here empty and read as an unknown counterpart.
    /// Named, not closed — the same interim standing as the helpers' own gap
    /// above. The third call this file makes binds `destination_path`,
    /// always an absolute filesystem path and never empty by construction,
    /// so it carries no gap of its own.
    EvidenceOrderedWithOneNamedGap,
}

impl EmptyRemainderGuard {
    fn as_str(self) -> &'static str {
        match self {
            EmptyRemainderGuard::BranchesAtTheCallSite => {
                "branches on the empty remainder at the call site"
            }
            EmptyRemainderGuard::EvidenceOrderedWithOneNamedGap => {
                "guarded by history_evidence_at's evidence order, with one named gap \
                 through counterpart_is_known"
            }
        }
    }
}

/// The census: every production call site of either spelling, with the count
/// of calls in each file, and the guard that answers for them.
///
/// The count is part of the row for the same reason the `decompose` census
/// above keeps one: a second call added to a file that already has a row
/// would otherwise inherit its reason silently, and a reason is about a
/// specific argument a specific caller passes, not about a file.
const EMPTY_REMAINDER_CALLERS: &[(&str, usize, EmptyRemainderGuard)] = &[
    (
        "core/repo/source.rs",
        1,
        EmptyRemainderGuard::BranchesAtTheCallSite,
    ),
    (
        "notes/repo.rs",
        6,
        EmptyRemainderGuard::BranchesAtTheCallSite,
    ),
    (
        "scan/repo.rs",
        3,
        EmptyRemainderGuard::BranchesAtTheCallSite,
    ),
    (
        "trail/repo.rs",
        3,
        EmptyRemainderGuard::EvidenceOrderedWithOneNamedGap,
    ),
];

/// Every production call of either spelling, by file. Matching on the
/// argument-opening quote (`path_at_or_under_sql("`/`path_strictly_under_sql("`)
/// catches every caller, which always passes a string-literal column name,
/// while leaving the two definitions — `pub fn path_at_or_under_sql(column:
/// &str)` — unmatched.
fn path_boundary_call_sites(src_root: &Path) -> Vec<(String, usize)> {
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
        let production = production_text(&rel, &text);
        let calls = production.matches("path_at_or_under_sql(\"").count()
            + production.matches("path_strictly_under_sql(\"").count();
        if calls > 0 {
            found.push((rel, calls));
        }
    }
    found
}

/// Both directions over supplied evidence: a call site the census does not
/// carry, a row the tree no longer produces, a count that has moved.
fn empty_remainder_census_violations(
    found: &[(String, usize)],
    rows: &[(&str, usize, EmptyRemainderGuard)],
) -> Vec<String> {
    let mut violations = Vec::new();
    for (file, calls) in found {
        match rows.iter().find(|(f, _, _)| f == file) {
            None => violations.push(format!(
                "{file} calls the boundary helpers {calls}× and holds no row — add one \
                 naming how it guards the empty remainder",
            )),
            Some((_, pinned, guard)) if pinned != calls => violations.push(format!(
                "{file} calls the boundary helpers {calls}× and its row pins {pinned} — \
                 the new call needs its own guard named, not `{}`",
                guard.as_str(),
            )),
            Some(_) => {}
        }
    }
    for (file, _, _) in rows {
        if !found.iter().any(|(f, _)| f == file) {
            violations.push(format!(
                "the census carries `{file}`, which no longer calls the boundary helpers \
                 — delete the row with the call",
            ));
        }
    }
    violations
}

#[test]
fn every_path_boundary_caller_names_its_empty_remainder_guard() {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    let src_root = Path::new(&manifest_dir).join("src");
    let violations = empty_remainder_census_violations(
        &path_boundary_call_sites(&src_root),
        EMPTY_REMAINDER_CALLERS,
    );
    assert!(
        violations.is_empty(),
        "\n  `path_at_or_under_sql`/`path_strictly_under_sql` answer wrong on an empty \
         prefix. Every production caller states how it keeps that prefix from reaching \
         them:\n{}\n",
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
        file: "compare/ops.rs",
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
        file: "roots/cli.rs",
        expects: 0,
        unwraps: 2,
    },
    ReachabilityClaims {
        file: "scan/ops/receipt.rs",
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
    #[expect(clippy::string_slice, reason = "just past a found ASCII `=> \"`")]
    let arms: Vec<&str> = text
        .match_indices("=> \"")
        .map(|(i, _)| &text[i + 4..])
        .collect();
    for after in arms {
        if let Some(end) = after.find('"') {
            #[expect(clippy::string_slice, reason = "a `find` position")]
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

/// The vocabulary's own home: the `as_str` arms this test reads the six
/// words out of. Spelling them there is the definition, not a violation of
/// the rule the definition makes possible.
const FATE_VOCABULARY_OWNER: &str = "core/domain/fate.rs";

/// Where a fate-vocabulary word is legitimately spelled outside its owner —
/// a different, unrelated vocabulary that happens to share one of the six
/// words. `(file, word, count)`, exact-match both directions like
/// `PROBE_EXEMPT`: a new coincidence needs the same deliberate row a widened
/// census needs anywhere else, and a resolved one is caught if its row
/// outlives the fix.
const FATE_WORD_EXEMPT: &[(&str, &str, usize)] = &[
    // the `--allow` manifest vocabulary (`archived`/`duplicates`)
    ("archive/cli/cluster.rs", "archived", 1),
    ("archive/domain.rs", "archived", 1),
    // the archived-ness *standing* a coverage query answers, never a
    // decision's *transition* — the carve-out the retire prose already
    // names for `covered`/`present`/`contentless` applies to this word too
    ("core/repo/object.rs", "archived", 1),
    // the `--include` vocabulary (`excluded`/`archived`/`all`)
    ("coverage/cli.rs", "archived", 1),
    ("coverage/cli.rs", "excluded", 1),
    // the `--where` status-predicate vocabulary (`archived?`/`excluded?`)
    ("expr/domain/filter.rs", "archived", 1),
    ("expr/domain/filter.rs", "excluded", 1),
    // `facts convert --list`'s place mark — where a source stands (no longer
    // present, suspended, archive copy, excluded), never a decision's transition
    ("facts/cli/convert.rs", "excluded", 1),
    // the standing word (`archived`/`present` — a counterpart's or a
    // member's standing), never a decision's transition
    ("sweep/cli.rs", "archived", 2),
];

/// The never-literal law: a transition or posture word is derived, never
/// written down.
///
/// The book bites hardest — a retired root's story is read forever and
/// cannot be rewritten — but the reach is canon-wide: the trail states the
/// same rule for receipt words over the same owner, and any reader deriving
/// a transition or posture is bound by it. The law's correct scope is
/// *transitions*: the standings beside them (`covered`, `present`,
/// `missing_unexplained`, `contentless`) are present-tense facts with no
/// derivation to come from, so they are named constants in one place instead
/// — which is also why a word spelling a *standing* rather than a
/// *transition* (`FATE_WORD_EXEMPT`'s `core/repo/object.rs` and `sweep/cli.rs`
/// rows) is not this law's business even though it shares the six words'
/// spelling.
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

    let mut hits: Vec<(String, usize, String)> = Vec::new();
    for path in collect_rs_files(&src_root) {
        let rel = path
            .strip_prefix(&src_root)
            .expect("file under src")
            .to_string_lossy()
            .replace('\\', "/");
        if is_externalised_test_file(&rel) || rel == FATE_VOCABULARY_OWNER {
            continue;
        }
        let text = fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("failed to read {}: {}", path.display(), e));
        let production = production_text(&rel, &text);
        for (line_no, line) in production.lines().enumerate() {
            for word in &words {
                if line.contains(&format!("\"{word}\"")) {
                    hits.push((rel.clone(), line_no + 1, word.clone()));
                }
            }
        }
    }

    let mut by_file_word: Vec<(String, String)> = hits
        .iter()
        .map(|(f, _, w)| (f.clone(), w.clone()))
        .collect();
    by_file_word.sort();
    let mut found: Vec<(String, String, usize)> = Vec::new();
    for (f, w) in by_file_word {
        match found.last_mut() {
            Some(last) if last.0 == f && last.1 == w => last.2 += 1,
            _ => found.push((f, w, 1)),
        }
    }

    let expected: Vec<(String, String, usize)> = FATE_WORD_EXEMPT
        .iter()
        .map(|(f, w, n)| ((*f).to_string(), (*w).to_string(), *n))
        .collect();

    assert_eq!(
        found,
        expected,
        "\n  A fate word written down is a fate word that stops agreeing with the \
         vocabulary the day the vocabulary moves — in a book that is never \
         rewritten, and the law now reaches every file. Derive it through \
         `fate_transition`/`fate_posture` (or `Transition::as_str`/`Posture::as_str`):\n{}\n\n  \
         Found: {found:?}\n  Exempt: {expected:?}\n  \
         If a new site genuinely spells a *different* vocabulary that happens to share \
         one of these six words, add it to `FATE_WORD_EXEMPT` with its exact count; if \
         an exemption is genuinely gone, delete its row in the same commit.",
        hits.iter()
            .map(|(f, l, w)| format!("  {f}:{l}: spells `\"{w}\"`"))
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

    /// The manifest door's own type, planted the same way — the row that
    /// fails if `PrefixOutcome` is dropped from the table, since a type named
    /// nowhere on it is simply not scanned for.
    #[test]
    fn a_planted_wildcard_on_the_manifest_door_is_seen() {
        let text = "\
fn f(outcome: PrefixOutcome) -> usize {
    match outcome {
        PrefixOutcome::Confirmed(scope, _) => scope.len(),
        _ => 0,
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
        // src/facts/ops/import.rs, as it stood — a fixed offset into a timestamp
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
        assert_eq!(classify_layer("source_value_law_tests.rs"), Layer::Testing);
    }

    #[test]
    fn the_upward_rule_guards_the_interface_files_and_not_the_law_tests() {
        let src_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let names = interface_module_names(&src_root);
        for interface in ["ceremony", "scope", "progress", "ledger", "expr"] {
            assert!(names.iter().any(|n| n == interface), "{interface}");
        }
        for testing in ["contentless_law_tests", "source_value_law_tests", "main"] {
            assert!(!names.iter().any(|n| n == testing), "{testing}");
        }
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
    fn the_recorder_census_refuses_a_new_caller_a_stale_row_and_a_moved_count() {
        let rows = &[
            ("roots/ops.rs", 3, Settlement::Armed),
            ("facts/ops/maintain.rs", 3, Settlement::Armed),
            ("facts/ops/maintain.rs", 1, Settlement::OwnWrapper),
        ];

        // Whole, including the file whose two rows sum to its four starts.
        assert!(recorder_census_violations(
            &[
                ("roots/ops.rs".into(), 3),
                ("facts/ops/maintain.rs".into(), 4)
            ],
            rows
        )
        .is_empty());

        // A caller nobody declared.
        let v = recorder_census_violations(
            &[
                ("roots/ops.rs".into(), 3),
                ("facts/ops/maintain.rs".into(), 4),
                ("survey/cli.rs".into(), 1),
            ],
            rows,
        );
        assert_eq!(v.len(), 1, "{v:?}");
        assert!(
            v[0].contains("survey/cli.rs") && v[0].contains("declares no settlement"),
            "{v:?}"
        );

        // A fifth start in the multi-row file: the shapes already declared
        // answer for four calls, never for one they have not seen.
        let v = recorder_census_violations(
            &[
                ("roots/ops.rs".into(), 3),
                ("facts/ops/maintain.rs".into(), 5),
            ],
            rows,
        );
        assert_eq!(v.len(), 1, "{v:?}");
        assert!(v[0].contains("declare 4"), "{v:?}");

        // The other direction: the file stopped starting decisions and the
        // rows outlived it. Both of its rows are named.
        let v = recorder_census_violations(&[("roots/ops.rs".into(), 3)], rows);
        assert_eq!(v.len(), 2, "{v:?}");
        assert!(
            v.iter().all(|m| m.contains("no longer starts a decision")),
            "{v:?}"
        );
    }

    #[test]
    fn the_empty_remainder_census_refuses_a_new_caller_and_a_stale_row() {
        let rows = &[(
            "core/repo/source.rs",
            1,
            EmptyRemainderGuard::BranchesAtTheCallSite,
        )];

        // Whole: the tree produces exactly what the census carries.
        assert!(
            empty_remainder_census_violations(&[("core/repo/source.rs".into(), 1)], rows)
                .is_empty()
        );

        // A caller nobody vouched for.
        let v = empty_remainder_census_violations(
            &[
                ("core/repo/source.rs".into(), 1),
                ("survey/ops/compute.rs".into(), 1),
            ],
            rows,
        );
        assert_eq!(v.len(), 1, "{v:?}");
        assert!(
            v[0].contains("survey/ops/compute.rs") && v[0].contains("holds no row"),
            "{v:?}"
        );

        // A second call in a file that already had one: the existing reason
        // answers for the first call, never for a call it has not seen.
        let v = empty_remainder_census_violations(&[("core/repo/source.rs".into(), 2)], rows);
        assert_eq!(v.len(), 1, "{v:?}");
        assert!(v[0].contains("pins 1"), "{v:?}");

        // The other direction: the caller is gone and the row outlived it.
        let v = empty_remainder_census_violations(&[], rows);
        assert_eq!(v.len(), 1, "{v:?}");
        assert!(
            v[0].contains("no longer calls the boundary helpers"),
            "{v:?}"
        );
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
                "read_filters",
                "read_report",
                "BoundFilters",
                "ExpressionText",
                "ExpressionRefused",
                "UsedStatus",
                "apply_filters",
                "apply_filters_noting",
                "select_sources",
                "RolePolicy",
                "SelectionParams",
                "NoteMarks",
                "SelectionNotes",
                "NoteUnit",
                "read_pattern",
                "BoundPattern",
                "evaluate",
                "Placement",
                "NotPlaced",
                "NoPlace",
                "NoPlaceCause",
                "EvalContext",
                "Unmeasured",
                "ScopeVantage",
                "prefetch_pattern_facts",
                "PatternFacts",
                "source_row_value",
                "SourceAttributes",
                "holds_control",
                "pictured",
                "contained",
                "Grain",
                "RowFilter",
                "RowIdentity",
                "RowValue",
                "Slot",
                "count_values_by_key",
                "fetch_row_values",
                "source_value",
                "source_values",
                "fetch_source_values",
                "fetch_named_values",
                "read_key_name",
                "NameReading",
                "BuiltinKey",
                "Key",
                "KeyKind",
                "Listing",
                "ParsedFactKey",
                "SourceFact",
                "CONTENT_HASH",
                "SCOPE_REL_PATH",
                "Modifier",
                "ModifierCategory",
                "rendered",
                "mixed_type_statement",
                "mixed_type_way_out",
                "convert_command",
                "printed_key",
                "printed_value",
                "Arriving",
                "Statement",
                "Subject",
                "Complement",
                "Noun",
                "type_words",
                "aliases_naming_key",
                "AliasesNaming",
            ],
        ),
        ("ls", &["run", "show_duplicates"]),
        ("worklist", &["run", "build_entries"]),
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
                "Voicing",
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
                "plan_duplicates",
                "ExcludeDuplicatesParams",
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
            ],
        ),
        (
            "facts",
            &[
                "run",
                "import_run",
                "convert",
                "ConvertOptions",
                "ConvertRefused",
                "TypeWord",
                "DeleteRefused",
                "plan_convert",
                "delete_facts",
                "DeleteOptions",
                "prune_stale",
                "prune_orphaned_objects",
                "prune_excluded_facts",
                // Read by the source-value law's verifier.
                "compute_distribution",
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
                "RefreshRefused",
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
