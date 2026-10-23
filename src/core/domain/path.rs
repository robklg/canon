//! Path utilities for canon.
//!
//! Pure path manipulation only — no I/O. Soft-match-then-fallback resolution
//! against known roots (whose fallback branch touches the filesystem) lives
//! in `core::ops::scope`.
//!
//! No database dependencies.

use anyhow::{bail, Result};
use std::path::{Component, Path, PathBuf};
use unicode_normalization::UnicodeNormalization;

use super::root::{find_containing_root, Root};

/// Check if a path is equal to or under a directory prefix.
/// Uses Path::starts_with which correctly handles directory boundaries.
/// Example: path_is_under("/a/bc/d", "/a/b") → false
///          path_is_under("/a/b/d", "/a/b") → true
pub fn path_is_under(path: &str, prefix: &str) -> bool {
    Path::new(path).starts_with(prefix)
}

/// Strip a directory prefix from a path, returning the relative portion.
/// Uses Path::strip_prefix which correctly handles directory boundaries.
/// Returns None if path is not under prefix.
/// Example: path_strip_prefix("/a/b/c", "/a/b") → Some("c")
///          path_strip_prefix("/a/bc", "/a/b") → None
pub fn path_strip_prefix<'a>(path: &'a str, prefix: &str) -> Option<&'a str> {
    Path::new(path)
        .strip_prefix(prefix)
        .ok()
        .and_then(|p| p.to_str())
}

/// **Destination containment**: a relative path made only of ordinary names —
/// no root, no Windows prefix, no `..`, no null character — so that joined
/// under any directory its names cannot leave that directory. Built only by
/// [`ContainedPath::new`], or as the default, the directory itself. `new`
/// parses with [`Path::components`] and keeps the names as the platform reads
/// them (`./a`, `a/./b` and `a//b` are `a`, `a/b`, `a/b`): a `.` never leaves
/// a directory, so it is read, not refused.
/// Purely lexical — no normalization, no filesystem access — so a path that
/// merely *contains* `..` as a name is refused rather than resolved, and a
/// link on disk below the directory is never read: it can still lead outside.
///
/// Every destination a manifest places a file at, and the manifest's
/// `base_dir`, is one. The path copied to and recorded is [`as_str`]'s, the
/// one rebuilt from the parsed names, so the index records what a scan finds.
///
/// [`as_str`]: ContainedPath::as_str
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct ContainedPath(String);

/// Why a path is no [`ContainedPath`]: the first name met that could leave the
/// directory it is joined under. Ordered as a report lists them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum NotContained {
    /// A `..` directory name.
    ParentDirectory,
    /// A name holding a null character.
    NullChar,
    /// An absolute path, or one with a Windows prefix.
    Rooted,
}

impl ContainedPath {
    /// Parse `path` into its names, refusing the first that could leave the
    /// directory the path is joined under. `""` and `"."` are the directory
    /// itself.
    pub fn new(path: &str) -> Result<ContainedPath, NotContained> {
        let mut names: Vec<&str> = Vec::new();
        for component in Path::new(path).components() {
            match component {
                Component::RootDir | Component::Prefix(_) => return Err(NotContained::Rooted),
                Component::ParentDir => return Err(NotContained::ParentDirectory),
                Component::CurDir => {}
                Component::Normal(name) => {
                    let name = name.to_str().expect("a name parsed from a str is a str");
                    if name.contains('\0') {
                        return Err(NotContained::NullChar);
                    }
                    names.push(name);
                }
            }
        }
        Ok(ContainedPath(names.join("/")))
    }

    /// This path with `below` under it: contained under contained stays
    /// contained, so no check is owed. An empty remainder gives this path back
    /// through [`join_under_root`], the rule's owner; an empty base is no root
    /// to join under, and gives `below` back.
    pub fn join(&self, below: &ContainedPath) -> ContainedPath {
        if self.0.is_empty() {
            below.clone()
        } else {
            ContainedPath(join_under_root(&self.0, &below.0))
        }
    }

    /// The one join onto a directory on disk, through [`join_under_root`]: the
    /// directory itself gives `root` back, with no separator after it. Every
    /// directory this is joined under starts at a root Canon holds as text, so
    /// reading `root` as text loses nothing. Its limit: `root` is joined as
    /// written, so a `root` ending in a separator — only `/` itself — gives a
    /// doubled one (`//a`).
    pub fn under(&self, root: &Path) -> PathBuf {
        PathBuf::from(join_under_root(&root.to_string_lossy(), &self.0))
    }

    /// The names joined by `/`, as the index and a receipt store them; `""`
    /// for the directory itself.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ContainedPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

// ============================================================================
// Soft Path Resolution (offline-capable)
// ============================================================================

/// Clean a path lexically: make absolute (relative to cwd), resolve `.` and `..`
/// without filesystem access. Does NOT resolve symlinks or normalize case.
pub fn clean_path(path: &Path, cwd: &Path) -> PathBuf {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    };
    let mut components = Vec::new();
    for component in absolute.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                // Don't pop past the root
                if components
                    .last()
                    .is_some_and(|c| matches!(c, Component::Normal(_)))
                {
                    components.pop();
                }
            }
            other => components.push(other),
        }
    }
    components.iter().collect()
}

/// Candidate byte-forms of a path for matching against stored data:
/// as given, then NFC, then NFD — deduplicated, order preserved.
///
/// The same visible path can reach Canon in more than one byte-form: a
/// filesystem hands out one Unicode normalization form, a shell or a
/// clipboard may hand over another, and the index stores whichever form the
/// disk gave when the path was scanned. Matching therefore bends the
/// argument to the stored form; stored bytes are never rewritten, so
/// receipts and rows keep the disk's own spelling. ASCII paths — the common
/// case — have a single candidate and cost nothing.
pub fn normalization_candidates(path: &str) -> Vec<String> {
    if path.is_ascii() {
        return vec![path.to_string()];
    }
    let mut candidates = Vec::with_capacity(3);
    candidates.push(path.to_string());
    for form in [
        path.nfc().collect::<String>(),
        path.nfd().collect::<String>(),
    ] {
        if !candidates.contains(&form) {
            candidates.push(form);
        }
    }
    candidates
}

/// Which known root contains a path, and the byte-form that found it.
///
/// The extra field over [`find_containing_root`]'s tuple is the point: a
/// caller must carry on with the form that matched, not the one it was
/// handed, or the next comparison undoes the match.
pub struct RootedPath {
    /// The path as the caller must carry it on — [`root_path`](Self::root_path)
    /// and [`rel_path`](Self::rel_path) joined, never the text as handed in.
    pub path: String,
    pub root_id: i64,
    /// The root's own stored path.
    pub root_path: String,
    /// The remainder below the root. Which byte-form it carries is the
    /// reading's, not the struct's: [`resolve_containing_root`] hands back the
    /// form that matched, [`resolve_containing_root_as_written`] the form the
    /// caller typed.
    pub rel_path: String,
}

// The root's *role* is deliberately absent. `find_containing_root` returns it
// and no caller reached for here wants it: carrying it would record a demand
// that does not exist, and adding it later is one edit with its consumer as
// the evidence.

/// The **root half of the form-tolerance law, spoken once**: which known root
/// contains this path, asked of every byte-form in the owner's order.
///
/// Pure and database-free, which is why it runs *first* at every door. The
/// law's other half — which byte-form of the below-root remainder the index
/// knows — can only be answered by the index and is asked afterwards, one
/// layer up (`core::ops::scope::known_form_of_rel` and its sources reading
/// `stored_form_of_rel`). **The order is not interchangeable**: a path whose
/// root portion is written in the other form must match its root before its
/// remainder can be asked about at all.
///
/// Candidates are of the **whole** path and the first that finds a root wins,
/// which is what makes the form that matched the root the form the path
/// carries from here on. `None` means no byte-form of this path lies under
/// any known root — genuinely rootless, and the caller's own disposition.
///
/// It does not lexically clean the path; callers taking user arguments run
/// [`clean_path`] first ([`resolve_path`] does).
pub fn resolve_containing_root(path: &str, roots: &[Root]) -> Option<RootedPath> {
    normalization_candidates(path)
        .into_iter()
        .find_map(|candidate| {
            find_containing_root(&candidate, roots).map(|(root_id, root_path, _role, rel_path)| {
                RootedPath {
                    path: candidate,
                    root_id,
                    root_path,
                    rel_path,
                }
            })
        })
}

/// [`resolve_containing_root`] for a path naming somewhere content is
/// **going**: the root portion bends to be found, the remainder never does.
///
/// The two readings differ in what the below-root text *is*. Everywhere else
/// it is an argument about something Canon already recorded, so bending it is
/// how the record is reached. Here it names a folder that need not exist yet,
/// and there is nothing to reach: the only bytes in play are the ones the
/// caller typed, so rewriting them would invent a name nobody asked for —
/// which is the law's own rule about recorded bytes, read from the other end.
///
/// Recovering the remainder as written is a **component** count and not a byte
/// slice: normalizing changes a component's length but never adds, removes or
/// moves a separator, so the text as handed in has exactly the components the
/// matched candidate has, and the remainder is the tail past the root's own.
/// Counting through [`Path`] rather than splitting on `/` is what makes the
/// count survive a doubled or trailing separator, in the path or in the root.
///
/// **Like [`resolve_containing_root`], it does not lexically clean the path**,
/// and here the precondition bites harder than there: [`Path::components`]
/// keeps `..` verbatim, so an uncleaned `/root/../root/sub` comes back as
/// `../root/sub` — a remainder [`ContainedPath::new`] refuses, against a field
/// that promises a below-root one. Callers taking user arguments run
/// [`clean_path`] first, which resolves `..` away; the one caller does.
pub fn resolve_containing_root_as_written(path: &str, roots: &[Root]) -> Option<RootedPath> {
    let matched = resolve_containing_root(path, roots)?;
    let rel_path = Path::new(path)
        .components()
        .skip(Path::new(&matched.root_path).components().count())
        .collect::<PathBuf>()
        .to_string_lossy()
        .into_owned();
    Some(RootedPath {
        path: join_under_root(&matched.root_path, &rel_path),
        rel_path,
        ..matched
    })
}

/// A root path and a below-root remainder as one absolute path; the root
/// alone when the remainder is empty.
///
/// The law's last step, and it had three spellings: rejoining is what carries
/// a healed remainder back out to a caller who asked about a whole path.
/// `Path::join` is not one of them — it appends a separator to an empty
/// remainder, so a root would come back with a trailing slash.
pub fn join_under_root(root_path: &str, rel: &str) -> String {
    if rel.is_empty() {
        root_path.to_string()
    } else {
        format!("{root_path}/{rel}")
    }
}

/// Whether any byte-form of `path` is `prefix` or lies under it.
///
/// [`path_is_under`] read form-tolerantly, for the comparisons that have no
/// root to look up: a retirement's scope snapshot is recorded text, not a live
/// root, so [`resolve_containing_root`] has nothing to find there.
///
/// **The text side bends and the recorded side never does** — the law's own
/// asymmetry. This answers a yes/no, so the as-given-first ordering that
/// governs *healing* is moot: any candidate matching is the honest question
/// when the recorded side is snapshot text whose form was fixed whenever it
/// was written.
pub fn path_is_under_any_form(path: &str, prefix: &str) -> bool {
    normalization_candidates(path)
        .iter()
        .any(|candidate| path_is_under(candidate, prefix))
}

/// Whether any byte-form of `path` is exactly `other`.
///
/// [`path_is_under_any_form`]'s equality sibling, for the two places that
/// compare a whole path Canon wrote down earlier against a whole path it holds
/// now — a receipt's origin root, a book's recorded root. Same asymmetry: the
/// text read back bends, the stored path never does.
pub fn same_path_any_form(path: &str, other: &str) -> bool {
    normalization_candidates(path)
        .iter()
        .any(|candidate| candidate == other)
}

/// Resolve a single path against known roots (pure, offline — no filesystem
/// access). Returns `None` when the (lexically cleaned) path doesn't match
/// any known root; a filesystem fallback for unmatched paths lives in
/// `core::ops::scope::resolve_path`.
///
/// The root half's whole-path reading: [`resolve_containing_root`] keeping
/// only the form that matched, so a root stored in the disk's form is
/// reachable from an argument typed in the other one.
pub fn resolve_path(path: &Path, roots: &[Root], cwd: &Path) -> Option<String> {
    let cleaned = clean_path(path, cwd);
    resolve_containing_root(&cleaned.to_string_lossy(), roots).map(|rooted| rooted.path)
}

/// Resolve multiple paths against known roots (pure, offline).
pub fn resolve_paths(paths: &[PathBuf], roots: &[Root], cwd: &Path) -> Vec<Option<String>> {
    paths.iter().map(|p| resolve_path(p, roots, cwd)).collect()
}

/// Format a path for display: relative when under cwd, absolute otherwise.
/// Returns "." when path equals cwd exactly.
pub fn format_path(full_path: &str, cwd: Option<&str>) -> String {
    if let Some(cwd) = cwd {
        if full_path == cwd {
            ".".to_string()
        } else if let Some(rel) = path_strip_prefix(full_path, cwd) {
            rel.to_string()
        } else {
            full_path.to_string()
        }
    } else {
        full_path.to_string()
    }
}

/// Component-wise common prefix of a set of paths taken whole — no trailing
/// component is dropped, because the inputs are already directory locations
/// rather than item paths. The display collapse for a group of placement
/// locations: one location returns itself, divergent tops return `""`.
/// Pure, no I/O.
pub fn common_path_prefix<'a>(paths: impl Iterator<Item = &'a str>) -> String {
    let mut common: Option<Vec<&str>> = None;
    for path in paths {
        let components: Vec<&str> = path.split('/').collect();
        common = Some(match common {
            None => components,
            Some(prev) => prev
                .into_iter()
                .zip(components)
                .take_while(|(a, b)| a == b)
                .map(|(a, _)| a)
                .collect(),
        });
    }
    common.unwrap_or_default().join("/")
}

/// Verify that all resolved paths are under a known root.
/// Returns error on the first path not under any root.
/// Uses find_containing_root() which checks all roots (including suspended),
/// preserving offline root support — paths matching known roots pass
/// regardless of whether the root's disk is mounted.
pub fn validate_paths_in_roots(paths: &[String], roots: &[Root]) -> Result<()> {
    for path in paths {
        if find_containing_root(path, roots).is_none() {
            bail!("{path} is not under any known root");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // path_is_under tests

    #[test]
    fn path_is_under_exact_match() {
        assert!(path_is_under("/a/b", "/a/b"));
    }

    #[test]
    fn path_is_under_child() {
        assert!(path_is_under("/a/b/c", "/a/b"));
    }

    #[test]
    fn path_is_under_deep_child() {
        assert!(path_is_under("/a/b/c/d/e", "/a/b"));
    }

    #[test]
    fn path_is_under_false_positive_prevention() {
        // Critical: /a/bc is NOT under /a/b (different directory)
        assert!(!path_is_under("/a/bc", "/a/b"));
        assert!(!path_is_under("/a/bc/d", "/a/b"));
    }

    #[test]
    fn path_is_under_unrelated() {
        assert!(!path_is_under("/x/y/z", "/a/b"));
    }

    #[test]
    fn path_is_under_root() {
        assert!(path_is_under("/a/b/c", "/"));
    }

    // ContainedPath's base_dir cases, as the manifest's base_dir gate met them

    #[test]
    fn a_contained_path_refuses_parent_components() {
        for path in ["..", "../../tmp", "photos/../../../tmp"] {
            assert_eq!(
                ContainedPath::new(path),
                Err(NotContained::ParentDirectory),
                "{path}"
            );
        }
    }

    #[test]
    fn a_contained_path_refuses_absolute() {
        assert_eq!(ContainedPath::new("/tmp/evil"), Err(NotContained::Rooted));
    }

    #[test]
    fn a_contained_path_accepts_nested_relative() {
        for path in ["", "photos", "photos/2020/summer", "photos/..hidden"] {
            // A name merely containing dots is not a traversal.
            assert_eq!(
                ContainedPath::new(path).map(|p| p.as_str().to_string()),
                Ok(path.to_string())
            );
        }
    }

    // ContainedPath tests

    /// **The destination-containment law's verifier**: a fixed table, never
    /// generated input. Each refused row is a path that, joined under a
    /// directory, would leave it or name a file no file system holds; each
    /// accepted row is rebuilt from its parsed names, which is the path copied
    /// to and recorded.
    #[test]
    fn no_destination_escapes_the_destination_directory() {
        use NotContained::*;
        let refused = [
            ("..", ParentDirectory),
            ("a/../b", ParentDirectory),
            ("../../x", ParentDirectory),
            // `{source.rel_path[:-1]}../{filename}` for a file at the top of its root,
            // and its repeated form.
            ("../README", ParentDirectory),
            ("../../README", ParentDirectory),
            ("/abs", Rooted),
            ("a/b\0c", NullChar),
            ("\0", NullChar),
        ];
        for (path, why) in refused {
            assert_eq!(ContainedPath::new(path), Err(why), "{path:?}");
        }
        let accepted = [
            ("x", "x"),
            ("./x", "x"),
            (".", ""),
            ("", ""),
            ("out/./x", "out/x"),
            ("a/./x", "a/x"),
            ("a//b", "a/b"),
            ("a/b/", "a/b"),
            ("photo..jpg", "photo..jpg"),
            ("..hidden/x", "..hidden/x"),
        ];
        for (path, rebuilt) in accepted {
            assert_eq!(
                ContainedPath::new(path).map(|p| p.as_str().to_string()),
                Ok(rebuilt.to_string()),
                "{path:?}"
            );
        }
    }

    #[test]
    fn contained_joined_to_contained_stays_contained() {
        let p = |s: &str| ContainedPath::new(s).unwrap();
        let rows = [
            ("", "x", "x"),
            ("", "", ""),
            ("photos", "", "photos"),
            ("photos", "2024/x.jpg", "photos/2024/x.jpg"),
            ("./out/./a", "b//c", "out/a/b/c"),
        ];
        for (base, below, joined) in rows {
            let got = p(base).join(&p(below));
            assert_eq!(got.as_str(), joined, "{base:?} + {below:?}");
            assert_eq!(ContainedPath::new(got.as_str()), Ok(got.clone()));
        }
        // Compared as text: a path compares by its names, so a trailing separator
        // would compare equal.
        let under = |path: &str| p(path).under(Path::new("/arch")).display().to_string();
        assert_eq!(under(""), "/arch");
        assert_eq!(under("a/./b"), "/arch/a/b");
    }

    // path_strip_prefix tests

    #[test]
    fn path_strip_prefix_basic() {
        assert_eq!(path_strip_prefix("/a/b/c", "/a/b"), Some("c"));
    }

    #[test]
    fn path_strip_prefix_deep() {
        assert_eq!(path_strip_prefix("/a/b/c/d", "/a/b"), Some("c/d"));
    }

    #[test]
    fn path_strip_prefix_exact_match() {
        // When path equals prefix, result is empty string
        assert_eq!(path_strip_prefix("/a/b", "/a/b"), Some(""));
    }

    #[test]
    fn path_strip_prefix_not_under() {
        // /a/bc is not under /a/b
        assert_eq!(path_strip_prefix("/a/bc", "/a/b"), None);
    }

    #[test]
    fn path_strip_prefix_unrelated() {
        assert_eq!(path_strip_prefix("/x/y", "/a/b"), None);
    }

    // ========================================================================
    // clean_path tests (pure, no filesystem)
    // ========================================================================

    #[test]
    fn clean_absolute_no_dots() {
        let result = clean_path(Path::new("/a/b/c"), Path::new("/any"));
        assert_eq!(result, PathBuf::from("/a/b/c"));
    }

    #[test]
    fn clean_relative_joins_cwd() {
        let result = clean_path(Path::new("b/c"), Path::new("/a"));
        assert_eq!(result, PathBuf::from("/a/b/c"));
    }

    #[test]
    fn clean_dotdot() {
        let result = clean_path(Path::new("/a/b/../c"), Path::new("/any"));
        assert_eq!(result, PathBuf::from("/a/c"));
    }

    #[test]
    fn clean_dot() {
        let result = clean_path(Path::new("/a/./b/c"), Path::new("/any"));
        assert_eq!(result, PathBuf::from("/a/b/c"));
    }

    #[test]
    fn clean_multiple_dotdot() {
        let result = clean_path(Path::new("/a/b/c/../../d"), Path::new("/any"));
        assert_eq!(result, PathBuf::from("/a/d"));
    }

    #[test]
    fn clean_dotdot_past_root() {
        let result = clean_path(Path::new("/a/../../b"), Path::new("/any"));
        assert_eq!(result, PathBuf::from("/b"));
    }

    #[test]
    fn clean_relative_with_dotdot() {
        let result = clean_path(Path::new("../b"), Path::new("/a/c"));
        assert_eq!(result, PathBuf::from("/a/b"));
    }

    #[test]
    fn clean_trailing_slash() {
        let result = clean_path(Path::new("/a/b/"), Path::new("/any"));
        assert_eq!(result, PathBuf::from("/a/b"));
    }

    #[test]
    fn clean_just_root() {
        let result = clean_path(Path::new("/"), Path::new("/any"));
        assert_eq!(result, PathBuf::from("/"));
    }

    #[test]
    fn clean_empty_relative() {
        let result = clean_path(Path::new(""), Path::new("/a/b"));
        assert_eq!(result, PathBuf::from("/a/b"));
    }

    // ========================================================================
    // resolve_path tests (using fake Root structs, no DB)
    // ========================================================================

    fn make_test_root(id: i64, path: &str) -> Root {
        Root {
            id,
            path: path.to_string(),
            role: "source".to_string(),
            comment: None,
            last_scanned_at: None,
            suspended: false,
        }
    }

    #[test]
    fn resolve_matches_known_root_exact() {
        let roots = vec![make_test_root(1, "/a/b")];
        let result = resolve_path(Path::new("/a/b"), &roots, Path::new("/any"));
        assert_eq!(result.unwrap(), "/a/b");
    }

    #[test]
    fn resolve_matches_under_root() {
        let roots = vec![make_test_root(1, "/a/b")];
        let result = resolve_path(Path::new("/a/b/c/d"), &roots, Path::new("/any"));
        assert_eq!(result.unwrap(), "/a/b/c/d");
    }

    #[test]
    fn resolve_relative_matches_root() {
        let roots = vec![make_test_root(1, "/a/b")];
        let result = resolve_path(Path::new("b/c"), &roots, Path::new("/a"));
        assert_eq!(result.unwrap(), "/a/b/c");
    }

    #[test]
    fn resolve_dotdot_matches_root() {
        let roots = vec![make_test_root(1, "/a/b")];
        let result = resolve_path(Path::new("/a/x/../b/c"), &roots, Path::new("/any"));
        assert_eq!(result.unwrap(), "/a/b/c");
    }

    #[test]
    fn resolve_no_match_returns_none() {
        let roots = vec![make_test_root(1, "/a/b")];
        let result = resolve_path(Path::new("/nonexistent/path"), &roots, Path::new("/any"));
        assert!(result.is_none());
    }

    // ========================================================================
    // normalization_candidates tests
    // ========================================================================

    /// A composed `ë` (U+00EB) and its decomposed twin (`e` + U+0308) —
    /// the two byte-forms of one visible character.
    const NFC_DIR: &str = "caf\u{e9}";
    const NFD_DIR: &str = "cafe\u{301}";

    #[test]
    fn normalization_candidates_ascii_is_a_single_identity_candidate() {
        assert_eq!(normalization_candidates("/a/b/c"), vec!["/a/b/c"]);
        assert_eq!(normalization_candidates(""), vec![""]);
    }

    #[test]
    fn normalization_candidates_are_deduped_as_given_first() {
        // An NFC argument: itself, then its NFD twin — two candidates, the
        // NFC form appearing once even though it is also the NFC candidate.
        let from_nfc = normalization_candidates(NFC_DIR);
        assert_eq!(from_nfc, vec![NFC_DIR.to_string(), NFD_DIR.to_string()]);

        // An NFD argument: itself first, then its NFC twin.
        let from_nfd = normalization_candidates(NFD_DIR);
        assert_eq!(from_nfd, vec![NFD_DIR.to_string(), NFC_DIR.to_string()]);
    }

    #[test]
    fn resolve_path_matches_a_root_stored_in_the_other_form() {
        let roots = vec![make_test_root(1, &format!("/mnt/{NFD_DIR}"))];
        // Argument typed in NFC; the root is stored NFD.
        let resolved = resolve_path(
            Path::new(&format!("/mnt/{NFC_DIR}/photos")),
            &roots,
            Path::new("/any"),
        );
        assert_eq!(resolved.unwrap(), format!("/mnt/{NFD_DIR}/photos"));

        // And the other direction.
        let roots = vec![make_test_root(1, &format!("/mnt/{NFC_DIR}"))];
        let resolved = resolve_path(
            Path::new(&format!("/mnt/{NFD_DIR}/photos")),
            &roots,
            Path::new("/any"),
        );
        assert_eq!(resolved.unwrap(), format!("/mnt/{NFC_DIR}/photos"));
    }

    #[test]
    fn resolve_suspended_root_still_matches() {
        let roots = vec![Root {
            suspended: true,
            ..make_test_root(1, "/a/b")
        }];
        let result = resolve_path(Path::new("/a/b/c"), &roots, Path::new("/any"));
        assert_eq!(result.unwrap(), "/a/b/c");
    }

    // ========================================================================
    // format_path tests
    // ========================================================================

    #[test]
    fn test_format_path_strips_cwd() {
        assert_eq!(format_path("/a/b/c/file.jpg", Some("/a/b")), "c/file.jpg");
    }

    #[test]
    fn test_format_path_absolute_fallback() {
        assert_eq!(format_path("/x/y/z.jpg", Some("/a/b")), "/x/y/z.jpg");
    }

    #[test]
    fn test_format_path_cwd_itself() {
        assert_eq!(format_path("/a/b", Some("/a/b")), ".");
    }

    #[test]
    fn test_format_path_no_cwd() {
        assert_eq!(format_path("/a/b/c.jpg", None), "/a/b/c.jpg");
    }

    // ========================================================================
    // validate_paths_in_roots tests
    // ========================================================================

    #[test]
    fn validate_path_under_active_root() {
        let roots = vec![make_test_root(1, "/a/b")];
        assert!(validate_paths_in_roots(&["/a/b/c".to_string()], &roots).is_ok());
    }

    #[test]
    fn validate_path_under_suspended_root() {
        let roots = vec![Root {
            suspended: true,
            ..make_test_root(1, "/a/b")
        }];
        // Suspended root is still "known"
        assert!(validate_paths_in_roots(&["/a/b/c".to_string()], &roots).is_ok());
    }

    #[test]
    fn validate_path_not_under_any_root() {
        let roots = vec![make_test_root(1, "/a/b")];
        let result = validate_paths_in_roots(&["/x/y/z".to_string()], &roots);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("not under any known root"));
    }

    #[test]
    fn validate_multiple_valid_paths() {
        let roots = vec![make_test_root(1, "/a/b"), make_test_root(2, "/c/d")];
        assert!(
            validate_paths_in_roots(&["/a/b/c".to_string(), "/c/d/e".to_string()], &roots).is_ok()
        );
    }

    #[test]
    fn validate_multiple_paths_second_invalid() {
        let roots = vec![make_test_root(1, "/a/b")];
        let result = validate_paths_in_roots(&["/a/b/c".to_string(), "/x/y".to_string()], &roots);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("/x/y"));
    }

    #[test]
    fn validate_empty_paths() {
        let roots = vec![make_test_root(1, "/a/b")];
        assert!(validate_paths_in_roots(&[], &roots).is_ok());
    }

    // common_path_prefix tests

    #[test]
    fn common_path_prefix_single_path_returns_itself() {
        // No trailing component is dropped — the input is already a
        // directory location, not an item path.
        assert_eq!(common_path_prefix(["m/03"].into_iter()), "m/03");
        assert_eq!(common_path_prefix(["/arch/m/03"].into_iter()), "/arch/m/03");
    }

    #[test]
    fn common_path_prefix_collapses_to_shared_ancestor() {
        assert_eq!(common_path_prefix(["m/01", "m/02"].into_iter()), "m");
        assert_eq!(
            common_path_prefix(["/arch/m/01", "/arch/m/02"].into_iter()),
            "/arch/m"
        );
        assert_eq!(common_path_prefix(["m", "m/01"].into_iter()), "m");
        assert_eq!(
            common_path_prefix(["фото/2016", "фото/2017"].into_iter()),
            "фото"
        );
    }

    #[test]
    fn common_path_prefix_root_level_and_divergent() {
        assert_eq!(common_path_prefix(["", "m/01"].into_iter()), "");
        assert_eq!(common_path_prefix(["a/x", "b/y"].into_iter()), "");
        assert_eq!(common_path_prefix(std::iter::empty()), "");
    }

    // ========================================================================
    // The form-tolerance law's root half, and its two comparison readings
    // ========================================================================

    /// The root half's own spelling: the candidate that found the root is
    /// what comes back, and the remainder comes back in that same form.
    #[test]
    fn resolve_containing_root_carries_the_form_that_matched() {
        let roots = vec![make_test_root(1, &format!("/mnt/{NFD_DIR}"))];
        let rooted =
            resolve_containing_root(&format!("/mnt/{NFC_DIR}/2016/a.jpg"), &roots).unwrap();
        assert_eq!(rooted.root_id, 1);
        assert_eq!(rooted.root_path, format!("/mnt/{NFD_DIR}"));
        assert_eq!(rooted.rel_path, "2016/a.jpg");
        assert_eq!(rooted.path, format!("/mnt/{NFD_DIR}/2016/a.jpg"));
    }

    /// **As-given first, and it must not be tidied.** On a
    /// normalization-sensitive filesystem the two spellings are two genuinely
    /// different directories, so each has to resolve to itself.
    #[test]
    fn resolve_containing_root_tries_the_path_as_written_before_bending_it() {
        let roots = vec![
            make_test_root(1, &format!("/mnt/{NFD_DIR}")),
            make_test_root(2, &format!("/mnt/{NFC_DIR}")),
        ];
        assert_eq!(
            resolve_containing_root(&format!("/mnt/{NFC_DIR}/x"), &roots)
                .unwrap()
                .root_id,
            2
        );
        assert_eq!(
            resolve_containing_root(&format!("/mnt/{NFD_DIR}/x"), &roots)
                .unwrap()
                .root_id,
            1
        );
    }

    #[test]
    fn resolve_containing_root_answers_none_for_a_genuinely_rootless_path() {
        let roots = vec![make_test_root(1, "/mnt/photos")];
        assert!(resolve_containing_root("/elsewhere/x", &roots).is_none());
    }

    /// The as-written reading's whole claim: the root bends, the remainder
    /// does not — so a folder about to be created keeps the bytes the caller
    /// typed even where the root above it had to be found in the other form.
    #[test]
    fn resolve_containing_root_as_written_bends_the_root_and_not_the_name_below_it() {
        let roots = vec![make_test_root(1, &format!("/mnt/{NFD_DIR}"))];

        let rooted =
            resolve_containing_root_as_written(&format!("/mnt/{NFC_DIR}/{NFC_DIR}"), &roots)
                .unwrap();
        assert_eq!(rooted.root_path, format!("/mnt/{NFD_DIR}"), "root found");
        assert_eq!(rooted.rel_path, NFC_DIR, "name below it as typed");
        assert_eq!(rooted.path, format!("/mnt/{NFD_DIR}/{NFC_DIR}"));

        // The matched reading beside it, on the same input: it heals the
        // whole path, which is right where the remainder names a record and
        // wrong where it names something not yet on disk.
        assert_eq!(
            resolve_containing_root(&format!("/mnt/{NFC_DIR}/{NFC_DIR}"), &roots)
                .unwrap()
                .rel_path,
            NFD_DIR,
        );

        // A remainder several deep, and none at all.
        assert_eq!(
            resolve_containing_root_as_written(&format!("/mnt/{NFC_DIR}/{NFC_DIR}/b"), &roots)
                .unwrap()
                .rel_path,
            format!("{NFC_DIR}/b"),
        );
        let root_itself =
            resolve_containing_root_as_written(&format!("/mnt/{NFC_DIR}"), &roots).unwrap();
        assert_eq!(root_itself.rel_path, "");
        assert_eq!(root_itself.path, format!("/mnt/{NFD_DIR}"));
    }

    /// Redundant separators — doubled or trailing, in the path or in the
    /// root — do not reach the remainder. **Not every uncleaned shape**: `..`
    /// survives a component count, and that is the doc's stated precondition
    /// rather than a claim made here. The one caller cleans its argument, so
    /// none of these arrives there; the promise under test is the struct's
    /// field, not that caller's habit, and a text split keeps none of them.
    #[test]
    fn resolve_containing_root_as_written_hands_back_a_below_root_remainder() {
        let roots = vec![make_test_root(1, "/mnt/root")];
        let rel = |p: &str| {
            resolve_containing_root_as_written(p, &roots)
                .unwrap()
                .rel_path
        };

        assert_eq!(rel("/mnt/root//sub"), "sub", "a doubled separator");
        assert_eq!(rel("/mnt/root/sub/"), "sub", "a trailing separator");
        assert_eq!(rel("/mnt/root/a//b/"), "a/b", "both, several deep");

        // Green under a text split too, and kept deliberately: it guards the
        // neighbouring off-by-one, not the shape above it.
        assert_eq!(rel("/mnt/root"), "", "the root itself");

        // An untidy *root*, which no caller cleans — it comes from the index.
        let untidy = vec![make_test_root(3, "/mnt/root/")];
        assert_eq!(
            resolve_containing_root_as_written("/mnt/root/sub", &untidy)
                .unwrap()
                .rel_path,
            "sub",
        );
    }

    /// The rejoin's one behaviour worth pinning: an empty remainder gives
    /// the root back unchanged. `Path::join` appends a separator here, which
    /// is why the owner is a plain function and not that.
    #[test]
    fn join_under_root_returns_the_root_itself_for_an_empty_remainder() {
        assert_eq!(join_under_root("/mnt/photos", ""), "/mnt/photos");
        assert_eq!(
            join_under_root("/mnt/photos", "2016/a.jpg"),
            "/mnt/photos/2016/a.jpg"
        );
    }

    /// The containment reading: the text side bends, the recorded side never
    /// does — and the path law's own boundary rule survives the bending.
    #[test]
    fn path_is_under_any_form_bends_the_text_side_only() {
        let recorded = format!("/mnt/{NFD_DIR}");
        assert!(path_is_under_any_form(
            &format!("/mnt/{NFC_DIR}/2016"),
            &recorded
        ));
        assert!(path_is_under_any_form(&recorded, &recorded));
        // Not a licence to match a sibling: the boundary is still segment-aware.
        assert!(!path_is_under_any_form(
            &format!("/mnt/{NFC_DIR}x/2016"),
            &recorded
        ));
        assert!(!path_is_under_any_form("/elsewhere", &recorded));
    }

    /// The equality reading. A prefix is not a match — only the whole path is.
    #[test]
    fn same_path_any_form_is_whole_path_equality() {
        let recorded = format!("/mnt/{NFD_DIR}");
        assert!(same_path_any_form(&format!("/mnt/{NFC_DIR}"), &recorded));
        assert!(same_path_any_form(&recorded, &recorded));
        assert!(!same_path_any_form(
            &format!("/mnt/{NFC_DIR}/2016"),
            &recorded
        ));
        assert!(!same_path_any_form("/mnt/other", &recorded));
    }
}
