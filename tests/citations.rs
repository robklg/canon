//! The comment citation scan.
//!
//! Comments that cite code locations go stale when code moves — and the stale
//! prose is never part of the diff that moved it, so no diff review sees it.
//! This check makes the mechanically checkable citation forms fail the build:
//!
//! - **File citations** — a comment names a `foo.rs`; the file must exist
//!   somewhere under `src/` or `tests/`. A citation that carries directories
//!   (`retire/ops/frame.rs`) is held to the whole path, suffix-aligned like a
//!   module path: naming a location and being checked only on the basename
//!   would let a move between subsystems pass unseen, which is the very thing
//!   this check exists to catch.
//! - **Module-path citations** — a comment names an `a::b::c` path; the path's
//!   module prefix must resolve against the tree (filesystem modules plus
//!   inline `mod` declarations), allowing a trailing item segment and a
//!   `Type::method` tail.
//! - **Inventory bullets** — a `Modules:` line in a CLAUDE.md enumerates
//!   module names; each must exist in the directory the bullet describes.
//!   These files are tracked and every `src/` subdirectory owes one, so an
//!   absent file is a failure rather than something to skip past.
//!
//! Deliberately out of scope: prose that asserts a *state* without naming a
//! path ("no old-tree file reaches..."). That form is not mechanically
//! checkable; it is covered by the move-story review question that asks
//! whether any prose near a diff now states something false.

use std::fs;
use std::path::Path;

mod common;

use common::{claude_md_paths, collect_rs_files, repo_root, starts_uppercase, Universe};

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

/// File citations: tokens ending in `.rs`, as written. URL fragments
/// (containing `//`) are skipped; a bare `mod.rs` or `lib.rs` is a structural
/// name, not a citation of a particular file — but `retire/mod.rs` names one,
/// so a path-qualified citation of either is checked.
///
/// A citation keeps its directories only when they read as directories. Prose
/// pairs two files with a slash (`trail.rs/survey.rs`), and the run splitter
/// cannot tell that from a path — so a non-final component ending in `.rs`
/// demotes the citation to its basename rather than inventing a directory.
fn file_citations(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    for run in split_runs(text, |c: char| {
        c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '/' || c == '-'
    }) {
        if run.contains("//") || !run.ends_with(".rs") {
            continue;
        }
        let run = run.trim_start_matches("./");
        let base = run.rsplit('/').next().unwrap_or(run);
        let Some(stem) = base.strip_suffix(".rs") else {
            continue;
        };
        if stem.is_empty() {
            continue; // a bare `.rs` (as in "every `.rs` file") names no file.
        }
        if !base
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        {
            continue;
        }
        let mut components: Vec<&str> = run.split('/').collect();
        let is_path = components.len() > 1
            && components[..components.len() - 1]
                .iter()
                .all(|c| !c.is_empty() && !c.ends_with(".rs"));
        if !is_path {
            components = vec![base];
        }
        if components.len() == 1 && (base == "mod.rs" || base == "lib.rs") {
            continue;
        }
        out.push(components.join("/"));
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
            if !universe.resolves_file_citation(&file) {
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
    let text = fs::read_to_string(path).unwrap_or_else(|e| {
        panic!(
            "failed to read {} — every src/ subdirectory owes a CLAUDE.md: {}",
            path.display(),
            e,
        )
    });
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

// ---------------------------------------------------------------------------
// The identifier rider: a backticked name in a register that names nothing.
// ---------------------------------------------------------------------------

/// One backticked identifier read from a register file, and where.
#[derive(Debug, Clone, PartialEq, Eq)]
struct IdentifierCitation {
    name: String,
    file: String,
    line: usize,
}

/// Prospective names: an identifier a register spells before the code that
/// answers to it exists. Exact-match, and matched both directions — an entry
/// whose name has since landed fails until it is deleted, so this cannot
/// become a place stale citations go to be forgotten.
///
/// Empty is the wanted state, and it is the state today.
const PROSPECTIVE_NAMES: &[(&str, &str)] = &[];

/// The backticked identifiers a register file spells: spans whose whole
/// content is a bare `snake_case` name of **three or more** segments.
///
/// Three, because that is where the shape stops being ordinary English written
/// in code's clothes. A two-segment name (`rel_path`, `root_id`, `dry_run`) is
/// a field, a column or a flag, and a register spells those constantly without
/// claiming any particular item exists. Three segments is the shape of the
/// thing that goes stale invisibly: a test name, a long function name — the
/// pin citations prose makes and nothing checks.
///
/// Three classes stay silent by construction rather than by exception, and
/// each is a fixture below. **CLI flags** (`--dry-run`) are not bare
/// identifiers — the dashes end the match. **Column names**
/// (`previous_decision_id`) are spelled in the SQL that reads them, so they
/// resolve like any other name. **Prospective names** are the exception, and
/// they are listed above with their reason.
///
/// Fenced blocks are skipped whole: a fence holds example text — a config
/// file, a shell transcript — and a name inside one is an illustration, not a
/// claim about this tree. The fence marker is read at the start of a line, so
/// an inline triple-backtick in prose (there is none) would not toggle it.
fn identifier_citations(display_path: &str, text: &str) -> Vec<IdentifierCitation> {
    let mut out = Vec::new();
    let mut fenced = false;
    for (idx, line) in text.lines().enumerate() {
        if line.trim_start().starts_with("```") {
            fenced = !fenced;
            continue;
        }
        if fenced {
            continue;
        }
        for span in line.split('`').skip(1).step_by(2) {
            if is_multi_segment_snake_case(span) {
                out.push(IdentifierCitation {
                    name: span.to_string(),
                    file: display_path.to_string(),
                    line: idx + 1,
                });
            }
        }
    }
    out
}

/// A bare lowercase identifier of three or more `_`-joined segments, each
/// non-empty. Anything else — a path, a flag, a call with parentheses, a type
/// name, a two-segment field — is not this shape.
fn is_multi_segment_snake_case(span: &str) -> bool {
    let segments: Vec<&str> = span.split('_').collect();
    segments.len() >= 3
        && segments.iter().all(|s| {
            !s.is_empty()
                && s.chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        })
}

/// Every identifier token spelled anywhere under `src/` or `tests/` — maximal
/// runs of identifier characters, comments and string literals included. A
/// register may legitimately cite a name that appears only in a comment, and
/// the question here is whether the name exists in the tree at all, not
/// whether it is a declaration.
fn identifier_tokens(root: &Path) -> std::collections::HashSet<String> {
    let mut tokens = std::collections::HashSet::new();
    for dir in ["src", "tests"] {
        for file in collect_rs_files(&root.join(dir)) {
            let text = fs::read_to_string(&file)
                .unwrap_or_else(|e| panic!("failed to read {}: {}", file.display(), e));
            let mut current = String::new();
            for c in text.chars() {
                if c.is_ascii_alphanumeric() || c == '_' {
                    current.push(c);
                } else if !current.is_empty() {
                    tokens.insert(std::mem::take(&mut current));
                }
            }
            if !current.is_empty() {
                tokens.insert(current);
            }
        }
    }
    tokens
}

/// Both directions over supplied evidence: a citation naming nothing and not
/// listed, and a listed name the tree has since grown.
fn identifier_rider_violations(
    citations: &[IdentifierCitation],
    tokens: &std::collections::HashSet<String>,
    prospective: &[(&str, &str)],
) -> Vec<String> {
    let listed: Vec<&str> = prospective.iter().map(|(n, _)| *n).collect();
    let mut violations = Vec::new();
    for cite in citations {
        if tokens.contains(&cite.name) || listed.contains(&cite.name.as_str()) {
            continue;
        }
        violations.push(format!(
            "{}:{}: `{}` names nothing under src/ or tests/",
            cite.file, cite.line, cite.name,
        ));
    }
    for (name, _) in prospective {
        if tokens.contains(*name) {
            violations.push(format!(
                "`{name}` is listed as prospective and now exists — delete the entry",
            ));
        }
    }
    violations
}

#[test]
fn identifiers_cited_in_the_registers_resolve() {
    let root = repo_root();
    let tokens = identifier_tokens(&root);
    let mut citations = Vec::new();
    for path in claude_md_paths(&root) {
        let text = fs::read_to_string(&path).unwrap_or_else(|e| {
            panic!(
                "failed to read {} — every src/ subdirectory owes a CLAUDE.md: {}",
                path.display(),
                e,
            )
        });
        let display = path
            .strip_prefix(&root)
            .unwrap_or(&path)
            .to_string_lossy()
            .to_string();
        citations.extend(identifier_citations(&display, &text));
    }
    let violations = identifier_rider_violations(&citations, &tokens, PROSPECTIVE_NAMES);
    assert!(
        violations.is_empty(),
        "\n  A register names an identifier the tree does not have. A pin cited in \
         prose goes stale invisibly — the rename that broke it is never in the \
         same diff — so the citation is repaired here, or listed as prospective \
         with its reason:\n{}\n",
        violations
            .iter()
            .map(|v| format!("  {v}"))
            .collect::<Vec<_>>()
            .join("\n"),
    );
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
    let mut violations = Vec::new();
    for path in claude_md_paths(&root) {
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
    use common::scan_inline_mods;

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
            &[
                "src/expr/eval.rs",
                "src/repo/source.rs",
                "src/archive/ops/plan.rs",
                "src/retire/mod.rs",
            ],
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
    fn accepts_full_path_citation() {
        // Suffix-aligned like a module path: with or without the `src/` head.
        let text = "// see archive/ops/plan.rs and src/repo/source.rs\n";
        let v = check_comment_citations("x.rs", text, &universe());
        assert!(v.is_empty(), "{:?}", v);
    }

    #[test]
    fn flags_full_path_citation_with_wrong_directory() {
        // `plan.rs` exists — under `archive/ops`, not `exclude/ops`. The
        // basename check alone would pass this, which is the gap the path
        // form closes: a file moved between subsystems leaves prose behind.
        let v = check_comment_citations("x.rs", "// see exclude/ops/plan.rs\n", &universe());
        assert_eq!(v.len(), 1, "a wrong directory must flag: {:?}", v);
        assert!(v[0].contains("exclude/ops/plan.rs"));
    }

    #[test]
    fn path_qualified_structural_name_is_checked() {
        // A bare `mod.rs` names no particular file and is skipped; a
        // path-qualified one names exactly one, so it resolves or flags.
        let v = check_comment_citations("x.rs", "// see retire/mod.rs\n", &universe());
        assert!(v.is_empty(), "an existing path-qualified mod.rs: {:?}", v);
        let v = check_comment_citations("x.rs", "// see ghost/mod.rs\n", &universe());
        assert_eq!(v.len(), 1, "a stale one must flag: {:?}", v);
    }

    #[test]
    fn prose_pairing_two_files_is_not_a_path() {
        // "trail.rs/survey.rs" is prose joining two names, not a directory
        // and a file — it is read as the basename it ends with.
        let v = check_comment_citations("x.rs", "// like plan.rs/source.rs do\n", &universe());
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
    fn accepts_a_primitive_associated_constant() {
        // The real external-roots list, not a synthetic one: the claim is
        // about what the guard actually carries.
        let universe = Universe::from_parts(&[], &[], common::EXTERNAL_ROOTS);
        let text = "// capped at u8::MAX, indexed to usize::MAX, within f64::EPSILON\n";
        let v = check_comment_citations("x.rs", text, &universe);
        assert!(
            v.is_empty(),
            "a primitive's associated constant is a citation nothing can \
             falsify — it must not flag: {:?}",
            v,
        );
    }

    #[test]
    fn still_flags_an_unknown_root() {
        // The guard against widening into a hole: admitting the primitives
        // must not admit anything that merely looks like one.
        let universe = Universe::from_parts(&[], &[], common::EXTERNAL_ROOTS);
        let v = check_comment_citations("x.rs", "// see nonexistent::MAX\n", &universe);
        assert_eq!(v.len(), 1, "an unknown root must still flag: {:?}", v);
        assert!(v[0].contains("nonexistent::MAX"), "{:?}", v);
    }

    #[test]
    fn bullet_directory_extraction() {
        assert_eq!(
            bullet_directory("- **`repo/`** — ALL database access. Modules: `db`"),
            Some("repo".to_string()),
        );
        assert_eq!(bullet_directory("plain prose with Modules: `x`"), None);
    }
    // ----------------------------------------------------------------
    // The identifier rider.
    // ----------------------------------------------------------------

    fn tokens(names: &[&str]) -> std::collections::HashSet<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    fn names(text: &str) -> Vec<String> {
        identifier_citations("CLAUDE.md", text)
            .into_iter()
            .map(|c| c.name)
            .collect()
    }

    #[test]
    fn reads_a_backticked_identifier_with_its_place() {
        assert_eq!(
            identifier_citations(
                "src/trail/CLAUDE.md",
                "intro\nheld by `a_group_is_never_empty`\n"
            ),
            vec![IdentifierCitation {
                name: "a_group_is_never_empty".to_string(),
                file: "src/trail/CLAUDE.md".to_string(),
                line: 2,
            }],
        );
    }

    #[test]
    fn the_three_english_classes_stay_silent() {
        // A CLI flag is not a bare identifier — the dashes end the match.
        assert!(names("pass `--dry-run` or `--no-receipt`\n").is_empty());
        // A two-segment name is a field, a column or a flag, and a register
        // spells those without claiming any item exists.
        assert!(names("the `rel_path` column and the `root_id` beside it\n").is_empty());
        // Neither is anything carrying punctuation: a path, a call, a type.
        assert!(names("see `core/domain/path.rs`, `resolve_path()`, `ScopeMatch`\n").is_empty());
        assert!(names("`core::ops::scope::stored_form_of_rel`\n").is_empty());
        // A column name of three segments *is* read — and resolves, because
        // the SQL that reads it spells it too. That is the class staying
        // silent by resolving, not by being skipped.
        assert_eq!(names("`previous_decision_id`\n"), ["previous_decision_id"]);
        assert!(identifier_rider_violations(
            &identifier_citations("CLAUDE.md", "`previous_decision_id`\n"),
            &tokens(&["previous_decision_id"]),
            &[],
        )
        .is_empty());
    }

    #[test]
    fn a_fenced_block_names_nothing() {
        // Example text — a config file, a shell transcript — is an
        // illustration, not a claim about this tree.
        assert!(names("```toml\nrecording = \"Full\"\n`a_name_in_a_fence`\n```\n").is_empty());
        // And the fence closes: prose after it is read again.
        assert_eq!(
            names("```\nx\n```\nheld by `a_group_is_never_empty`\n"),
            ["a_group_is_never_empty"],
        );
    }

    #[test]
    fn flags_a_planted_name_the_tree_does_not_have() {
        let cites = identifier_citations(
            "src/expr/CLAUDE.md",
            "pinned by `a_source_key_the_context_does_not_supply_is_fetched`\n",
        );
        let v = identifier_rider_violations(&cites, &tokens(&["something_else_entirely"]), &[]);
        assert_eq!(v.len(), 1, "{v:?}");
        assert!(v[0].contains("src/expr/CLAUDE.md:1"), "{v:?}");
        assert!(v[0].contains("names nothing"), "{v:?}");

        // Listing it as prospective silences it, and only it.
        assert!(identifier_rider_violations(
            &cites,
            &tokens(&[]),
            &[(
                "a_source_key_the_context_does_not_supply_is_fetched",
                "the pin this names is not written yet",
            )],
        )
        .is_empty());
    }

    #[test]
    fn a_prospective_name_that_has_landed_fails_until_its_entry_goes() {
        // The other direction: the list cannot become a place stale citations
        // go to be forgotten.
        let v = identifier_rider_violations(
            &[],
            &tokens(&["a_name_that_has_since_landed"]),
            &[("a_name_that_has_since_landed", "written any day now")],
        );
        assert_eq!(v.len(), 1, "{v:?}");
        assert!(v[0].contains("now exists"), "{v:?}");
    }

    #[test]
    fn a_name_is_matched_whole_never_as_a_stem() {
        // `foo_bar_baz` is not answered for by `foo_bar_baz_qux`: a citation
        // that resolves to a longer name resolves to a different thing.
        let cites = identifier_citations("CLAUDE.md", "`a_stem_of_something`\n");
        assert_eq!(
            identifier_rider_violations(&cites, &tokens(&["a_stem_of_something_longer"]), &[])
                .len(),
            1,
        );
    }
}
