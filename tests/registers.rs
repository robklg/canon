//! The register ceiling.
//!
//! A tracked CLAUDE.md is prose about code, and prose about code accretes:
//! every change has a reason to add a sentence and none has a reason to
//! remove one. Left alone the file drifts from a map into an inventory, and
//! the inventory goes stale invisibly, because nothing in the diff that
//! falsifies a sentence touches the sentence.
//!
//! This holds two numbers per register file. **Words** is the bulk. **Verifier
//! names** is the shape that ages worst — a distinct backticked identifier
//! that resolves to a `#[test] fn` somewhere in the tree — because a renamed
//! test silently falsifies every prose citation of it, and a rule is entitled
//! to one verifier, never a battery.
//!
//! Both are ceilings, not targets. Shrinking is free and needs no edit here.
//! Growing means raising a number in this table, in the same commit, where a
//! reviewer sees the raise beside the prose that needed it — every CLAUDE.md
//! is tracked, so the raise is in the diff a chartered review reads. Each
//! ceiling is the file's **measured** size: exact, no headroom, so every added
//! sentence is a raise that says what the prose bought. The root file's row is
//! the requester's to raise, under the root-file rule.
//!
//! Matched **both directions**: a register the tree owes with no row fails,
//! and a row naming a register the tree does not owe fails. Both directions
//! read the *directories*, so neither notices a file that has been deleted
//! while its directory stands — that is the third check's alone, and it is why
//! there is a third check: every `src/` subdirectory owes a CLAUDE.md, and a
//! subsystem that documents nothing is one no reader can find their way into.

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

mod common;

use common::LANGUAGE_REGISTER;

struct Ceiling {
    path: &'static str,
    words: usize,
    verifiers: usize,
}

/// The table. Raise a number and say why in the same commit's message.
const CEILINGS: &[Ceiling] = &[
    Ceiling {
        path: "CLAUDE.md",
        words: 12885,
        verifiers: 9,
    },
    Ceiling {
        path: "src/archive/CLAUDE.md",
        words: 8251,
        verifiers: 43,
    },
    Ceiling {
        path: "src/compare/CLAUDE.md",
        words: 475,
        verifiers: 2,
    },
    Ceiling {
        path: "src/core/CLAUDE.md",
        words: 9351,
        verifiers: 35,
    },
    Ceiling {
        path: "src/coverage/CLAUDE.md",
        words: 682,
        verifiers: 3,
    },
    Ceiling {
        path: "src/exclude/CLAUDE.md",
        words: 1669,
        verifiers: 16,
    },
    Ceiling {
        path: "src/expr/CLAUDE.md",
        words: 5855,
        verifiers: 46,
    },
    Ceiling {
        path: "src/facts/CLAUDE.md",
        words: 1442,
        verifiers: 5,
    },
    Ceiling {
        path: "src/ls/CLAUDE.md",
        words: 324,
        verifiers: 0,
    },
    Ceiling {
        path: "src/notes/CLAUDE.md",
        words: 513,
        verifiers: 3,
    },
    Ceiling {
        path: "src/retire/CLAUDE.md",
        words: 3625,
        verifiers: 13,
    },
    Ceiling {
        path: "src/roots/CLAUDE.md",
        words: 801,
        verifiers: 2,
    },
    Ceiling {
        path: "src/scan/CLAUDE.md",
        words: 4444,
        verifiers: 1,
    },
    Ceiling {
        path: "src/story/CLAUDE.md",
        words: 1926,
        verifiers: 12,
    },
    Ceiling {
        path: "src/survey/CLAUDE.md",
        words: 1516,
        verifiers: 7,
    },
    Ceiling {
        path: "src/sweep/CLAUDE.md",
        words: 4545,
        verifiers: 27,
    },
    Ceiling {
        path: "src/trail/CLAUDE.md",
        words: 7772,
        verifiers: 31,
    },
    Ceiling {
        path: "src/worklist/CLAUDE.md",
        words: 406,
        verifiers: 0,
    },
    Ceiling {
        path: "docs/LANGUAGE.md",
        words: 2614,
        verifiers: 0,
    },
];

// ---------------------------------------------------------------------------
// Measurement.
// ---------------------------------------------------------------------------

struct Measured {
    path: String,
    words: usize,
    verifiers: usize,
}

/// Words, counted the way `wc -w` counts them: runs separated by whitespace.
/// Deliberately crude — this is a bulk ceiling, and a cleverer measure would
/// only give a writer something to argue with.
fn word_count(text: &str) -> usize {
    text.split_whitespace().count()
}

/// The distinct backticked identifiers in `text` that name a test in the tree.
///
/// Only a span that is *exactly* one identifier counts. A span carrying a
/// module path (`core::domain::path`) or a call is not a verifier citation,
/// and a rule naming its owner or a type is not spending from this budget.
fn verifier_names(text: &str, test_fns: &BTreeSet<String>) -> BTreeSet<String> {
    let mut found = BTreeSet::new();
    for span in text.split('`').skip(1).step_by(2) {
        if span.is_empty() {
            continue;
        }
        let is_identifier = span.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            && !span.starts_with(|c: char| c.is_ascii_digit());
        if is_identifier && test_fns.contains(span) {
            found.insert(span.to_string());
        }
    }
    found
}

/// Every register this table must carry a row for: the CLAUDE.md files the
/// prose guards read, plus the language register.
fn tracked_registers(root: &Path) -> Vec<String> {
    let mut paths: Vec<String> = common::claude_md_paths(root)
        .iter()
        .map(|p| common::relative_slash_path(root, p))
        .collect();
    paths.push(LANGUAGE_REGISTER.to_string());
    paths.sort();
    paths
}

// ---------------------------------------------------------------------------
// The rules, as pure functions over measurements — this is the seam the red
// smoke feeds, so neither check needs a tree to be exercised against a defect.
// ---------------------------------------------------------------------------

fn over_ceiling(measured: &[Measured], table: &[Ceiling]) -> Vec<String> {
    let mut over = Vec::new();
    for m in measured {
        let Some(row) = table.iter().find(|c| c.path == m.path) else {
            continue; // a missing row is the other check's finding, not this one.
        };
        if m.words > row.words {
            over.push(format!(
                "  {}: {} words, ceiling {}",
                m.path, m.words, row.words,
            ));
        }
        if m.verifiers > row.verifiers {
            over.push(format!(
                "  {}: {} verifier names, cap {}",
                m.path, m.verifiers, row.verifiers,
            ));
        }
    }
    over
}

/// The registers the tree owes that are not on disk.
///
/// Split from its caller for one reason: it is the **only** check that catches
/// a deleted register whose directory still stands. The both-directions match
/// reads directories, so the path stays in `expected` and stays in the table,
/// and the ceiling walk skips a file it cannot read — a deletion passes both
/// in silence. A check carrying a defect alone must be able to fail against
/// it, so existence is a supplied predicate here and the tree is the caller's.
fn absent(expected: &[String], exists: impl Fn(&str) -> bool) -> Vec<String> {
    expected
        .iter()
        .filter(|p| !exists(p))
        .map(|p| p.to_string())
        .collect()
}

fn table_mismatches(expected: &[String], table: &[Ceiling]) -> Vec<String> {
    let on_disk: BTreeSet<&str> = expected.iter().map(String::as_str).collect();
    let in_table: BTreeSet<&str> = table.iter().map(|c| c.path).collect();
    let mut problems: Vec<String> = in_table
        .difference(&on_disk)
        .map(|p| format!("  {p}: a row for a register the tree does not owe"))
        .collect();
    problems.extend(
        on_disk
            .difference(&in_table)
            .map(|p| format!("  {p}: a tracked register with no row")),
    );
    problems
}

// ---------------------------------------------------------------------------
// The checks over the real tree.
// ---------------------------------------------------------------------------

fn measure_all(root: &Path) -> Vec<Measured> {
    let test_fns = common::test_fn_names(root);
    tracked_registers(root)
        .into_iter()
        .filter_map(|path| {
            let text = fs::read_to_string(root.join(&path)).ok()?;
            let verifiers = verifier_names(&text, &test_fns).len();
            Some(Measured {
                words: word_count(&text),
                verifiers,
                path,
            })
        })
        .collect()
}

#[test]
fn every_tracked_register_stays_under_its_ceiling() {
    let root = common::repo_root();
    let over = over_ceiling(&measure_all(&root), CEILINGS);
    assert!(
        over.is_empty(),
        "a register grew past its ceiling:\n{}\n  \
         Cut, or raise the number in tests/registers.rs in this same commit \
         and say in the message what the prose bought.",
        over.join("\n"),
    );
}

#[test]
fn the_ceiling_table_names_exactly_the_tracked_registers() {
    let root = common::repo_root();
    let problems = table_mismatches(&tracked_registers(&root), CEILINGS);
    assert!(
        problems.is_empty(),
        "the ceiling table and the tracked registers disagree:\n{}",
        problems.join("\n"),
    );
}

#[test]
fn every_src_subdirectory_carries_its_register() {
    let root = common::repo_root();
    let missing = absent(&tracked_registers(&root), |p| root.join(p).is_file());
    assert!(
        missing.is_empty(),
        "a tracked register is absent — every src/ subdirectory owes a \
         CLAUDE.md, because a subsystem that documents nothing is one no \
         reader can find their way into:\n  {}",
        missing.join("\n  "),
    );
}

/// Every `CLAUDE.md` that exists strictly below the ratchet's own reach:
/// not the root file, and not one directly inside a `src/*` subsystem
/// directory (`claude_md_paths`'s own depth), but nested deeper still. That
/// walk stops at depth one, so a register any deeper is invisible to every
/// ceiling and citation check — a register no guard can see must not exist.
fn claude_md_paths_below_depth_one(root: &Path) -> Vec<String> {
    let mut found = Vec::new();
    if let Ok(entries) = fs::read_dir(root.join("src")) {
        for entry in entries.flatten() {
            let subsystem = entry.path();
            if !subsystem.is_dir() {
                continue;
            }
            // The subsystem's own `CLAUDE.md` here, at depth one, is exactly
            // what the ratchet expects — only its subdirectories are walked.
            let Ok(nested) = fs::read_dir(&subsystem) else {
                continue;
            };
            for entry in nested.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    walk_for_claude_md(&path, root, &mut found);
                }
            }
        }
    }
    found.sort();
    found
}

fn walk_for_claude_md(dir: &Path, root: &Path, out: &mut Vec<String>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk_for_claude_md(&path, root, out);
        } else if path.file_name().and_then(|n| n.to_str()) == Some("CLAUDE.md") {
            out.push(common::relative_slash_path(root, &path));
        }
    }
}

#[test]
fn no_claude_md_exists_below_the_ratchets_reach() {
    let root = common::repo_root();
    let deep = claude_md_paths_below_depth_one(&root);
    assert!(
        deep.is_empty(),
        "a CLAUDE.md exists where the ratchet's walk never looks — the guards \
         read the root file and one per src/*/, so a register here is \
         invisible to every ceiling and citation check; move it up to its \
         subsystem's own file, or fold it into one the walk already reads:\n  {}",
        deep.join("\n  "),
    );
}

// ---------------------------------------------------------------------------
// Red smoke: each check, run against the defect it names.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod self_tests {
    use super::*;

    fn row(path: &'static str, words: usize, verifiers: usize) -> Ceiling {
        Ceiling {
            path,
            words,
            verifiers,
        }
    }

    fn measured(path: &str, words: usize, verifiers: usize) -> Measured {
        Measured {
            path: path.to_string(),
            words,
            verifiers,
        }
    }

    #[test]
    fn a_register_that_grew_trips_its_word_ceiling() {
        let over = over_ceiling(
            &[measured("a/CLAUDE.md", 101, 0)],
            &[row("a/CLAUDE.md", 100, 0)],
        );
        assert_eq!(over.len(), 1, "{over:?}");
        assert!(over[0].contains("101 words, ceiling 100"), "{over:?}");
        // One word under is silent: the ceiling is a ratchet, not a target.
        assert!(over_ceiling(
            &[measured("a/CLAUDE.md", 100, 0)],
            &[row("a/CLAUDE.md", 100, 0)]
        )
        .is_empty());
    }

    #[test]
    fn a_register_that_grew_a_battery_trips_the_name_cap() {
        let over = over_ceiling(
            &[measured("a/CLAUDE.md", 10, 4)],
            &[row("a/CLAUDE.md", 100, 3)],
        );
        assert_eq!(over.len(), 1, "{over:?}");
        assert!(over[0].contains("4 verifier names, cap 3"), "{over:?}");
    }

    #[test]
    fn a_tracked_register_with_no_row_is_named() {
        let problems = table_mismatches(
            &["a/CLAUDE.md".to_string(), "b/CLAUDE.md".to_string()],
            &[row("a/CLAUDE.md", 1, 0)],
        );
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(problems[0].contains("b/CLAUDE.md"), "{problems:?}");
        assert!(problems[0].contains("no row"), "{problems:?}");
    }

    #[test]
    fn a_row_naming_no_register_is_named() {
        let problems = table_mismatches(
            &["a/CLAUDE.md".to_string()],
            &[row("a/CLAUDE.md", 1, 0), row("gone/CLAUDE.md", 1, 0)],
        );
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(problems[0].contains("gone/CLAUDE.md"), "{problems:?}");
        assert!(problems[0].contains("does not owe"), "{problems:?}");
    }

    #[test]
    fn only_a_bare_identifier_that_names_a_test_is_a_verifier() {
        let tests: BTreeSet<String> = ["a_real_test", "another_test"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let text = "Held by `a_real_test`, and again by `a_real_test`. \
                    `a::path::a_real_test` is a module path, `run(x)` is a call, \
                    `NotATest` names no test, and `another_test` does.";
        let found = verifier_names(text, &tests);
        assert_eq!(found.len(), 2, "{found:?}");
        assert!(found.contains("a_real_test") && found.contains("another_test"));
    }

    #[test]
    fn a_deleted_register_whose_directory_stands_is_named() {
        let expected = ["a/CLAUDE.md".to_string(), "b/CLAUDE.md".to_string()];
        // `b`'s directory is still there — which is why the other two checks
        // see nothing — but its file is gone.
        let missing = absent(&expected, |p| p == "a/CLAUDE.md");
        assert_eq!(missing, vec!["b/CLAUDE.md".to_string()]);
        // The defect this check exists for is invisible to its neighbours:
        // the row is still owed, so the match is clean...
        let table = [row("a/CLAUDE.md", 1, 0), row("b/CLAUDE.md", 1, 0)];
        assert!(table_mismatches(&expected, &table).is_empty());
        // ...and an unreadable file contributes no measurement to compare.
        assert!(over_ceiling(&[measured("a/CLAUDE.md", 1, 0)], &table).is_empty());
    }

    #[test]
    fn a_test_fn_is_read_from_its_attribute_not_its_name() {
        let root = common::repo_root();
        let names = common::test_fn_names(&root);
        // This very function, found by its own attribute.
        assert!(names.contains("a_test_fn_is_read_from_its_attribute_not_its_name"));
        // A `#[test]`-less helper in this same file is not a verifier.
        assert!(!names.contains("measured"));
    }

    #[test]
    fn a_claude_md_below_depth_one_is_named_and_a_depth_one_one_is_not() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path();
        fs::create_dir_all(root.join("src/widget")).expect("mkdir");
        fs::write(root.join("src/widget/CLAUDE.md"), "# depth one, expected").expect("write");
        // The ratchet's own depth is silent.
        assert!(claude_md_paths_below_depth_one(root).is_empty());

        fs::create_dir_all(root.join("src/widget/ops")).expect("mkdir");
        fs::write(root.join("src/widget/ops/CLAUDE.md"), "# too deep").expect("write");
        let found = claude_md_paths_below_depth_one(root);
        assert_eq!(
            found,
            vec!["src/widget/ops/CLAUDE.md".to_string()],
            "{found:?}"
        );
    }
}
