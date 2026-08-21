//! Root candidate discovery: scanning a scope for untracked files not under
//! any known root, then collapsing the results into candidate root
//! directories. Pure filesystem traversal — no database access.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Result;

/// A candidate root directory discovered by scanning for untracked files.
pub struct RootCandidate {
    /// Absolute path to the candidate directory.
    pub path: PathBuf,
    /// Number of directories with files under this candidate.
    pub dir_count: usize,
}

/// Result of scanning for untracked root candidates.
pub struct CandidateResult {
    /// Candidate root directories, sorted by path.
    pub candidates: Vec<RootCandidate>,
    /// Warnings encountered during filesystem walk (e.g., permission errors).
    pub warnings: Vec<String>,
}

/// Scan a scope directory for untracked files not under any known root,
/// then collapse the results into candidate root directories.
///
/// `root_paths` should contain only active (non-suspended) root paths.
pub fn find_root_candidates(scope: &Path, root_paths: &[PathBuf]) -> Result<CandidateResult> {
    let mut dirs_with_files: HashSet<PathBuf> = HashSet::new();
    let mut warnings: Vec<String> = Vec::new();

    scan_for_untracked(scope, root_paths, &mut dirs_with_files, &mut warnings)?;

    let candidates = find_common_ancestors(&dirs_with_files, root_paths, scope)
        .into_iter()
        .map(|(path, dir_count)| RootCandidate { path, dir_count })
        .collect();

    Ok(CandidateResult {
        candidates,
        warnings,
    })
}

/// Recursively scan for directories with files not under any root.
fn scan_for_untracked(
    dir: &Path,
    roots: &[PathBuf],
    result: &mut HashSet<PathBuf>,
    warnings: &mut Vec<String>,
) -> Result<()> {
    // Skip if this directory is under an existing root
    if roots
        .iter()
        .any(|root| dir == root || dir.starts_with(root))
    {
        return Ok(());
    }

    let entries: Vec<_> = match fs::read_dir(dir) {
        Ok(rd) => rd.filter_map(|e| e.ok()).collect(),
        Err(e) => {
            warnings.push(format!("cannot read {}: {e}", dir.display()));
            return Ok(());
        }
    };

    // Check if this directory has any files (stop at first one found)
    let has_file = entries
        .iter()
        .any(|e| e.file_type().map(|ft| ft.is_file()).unwrap_or(false));

    // Check if this directory contains any root (can't be added as a root — invariant)
    let contains_root = roots
        .iter()
        .any(|root| root.starts_with(dir) && root != dir);

    if has_file && !contains_root {
        result.insert(dir.to_path_buf());
    } else {
        for entry in entries {
            if entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
                scan_for_untracked(&entry.path(), roots, result, warnings)?;
            }
        }
    }

    Ok(())
}

/// Find shortest common ancestors for a set of directories,
/// bounded by scope and not crossing root boundaries.
fn find_common_ancestors(
    dirs_with_files: &HashSet<PathBuf>,
    roots: &[PathBuf],
    scope: &Path,
) -> Vec<(PathBuf, usize)> {
    let mut ancestors: HashMap<PathBuf, usize> = HashMap::new();

    for dir in dirs_with_files {
        let mut current = dir.clone();
        let mut highest_untracked = dir.clone();

        while let Some(parent) = current.parent() {
            if parent == scope || !parent.starts_with(scope) {
                break;
            }
            if roots
                .iter()
                .any(|root| parent == root || parent.starts_with(root))
            {
                break;
            }
            if roots.iter().any(|root| root.starts_with(parent)) {
                break;
            }

            highest_untracked = parent.to_path_buf();
            current = parent.to_path_buf();
        }

        *ancestors.entry(highest_untracked).or_insert(0) += 1;
    }

    let mut result: Vec<_> = ancestors.into_iter().collect();
    result.sort_by(|a, b| a.0.cmp(&b.0));
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Create `dir` under `base` and drop a file in it. Returns the directory.
    fn dir_with_file(base: &Path, dir: &str) -> PathBuf {
        let path = base.join(dir);
        fs::create_dir_all(&path).unwrap();
        fs::write(path.join("file.txt"), b"x").unwrap();
        path
    }

    /// The candidate paths, in the order the discovery returns them.
    fn paths(result: &CandidateResult) -> Vec<PathBuf> {
        result.candidates.iter().map(|c| c.path.clone()).collect()
    }

    #[test]
    fn find_root_candidates_collapses_to_common_ancestor() {
        // Two untracked directories under one parent offer that parent as the
        // candidate — one root to add, not one per leaf.
        let temp = TempDir::new().unwrap();
        let scope = temp.path();
        dir_with_file(scope, "photos/2020");
        dir_with_file(scope, "photos/2021");

        let result = find_root_candidates(scope, &[]).unwrap();

        assert_eq!(paths(&result), vec![scope.join("photos")]);
        // Both leaves are counted under the one candidate.
        assert_eq!(result.candidates[0].dir_count, 2);
    }

    #[test]
    fn find_root_candidates_skips_dirs_under_known_root() {
        // Already-indexed content is not a candidate: the walk turns back at a
        // known root instead of re-offering what is already tracked.
        let temp = TempDir::new().unwrap();
        let scope = temp.path();
        let known = dir_with_file(scope, "tracked");
        dir_with_file(scope, "tracked/deeper");
        dir_with_file(scope, "untracked");

        let result = find_root_candidates(scope, &[known]).unwrap();

        assert_eq!(paths(&result), vec![scope.join("untracked")]);
    }

    #[test]
    fn find_root_candidates_stops_at_dir_containing_a_root() {
        // Roots never nest, so a directory holding a root can never itself be
        // offered — the walk descends past it, and the collapse stops below it
        // rather than proposing an unaddable parent.
        let temp = TempDir::new().unwrap();
        let scope = temp.path();
        let holder = dir_with_file(scope, "holder");
        let inner_root = dir_with_file(scope, "holder/indexed");
        dir_with_file(scope, "holder/loose/deep");

        let result = find_root_candidates(scope, &[inner_root]).unwrap();

        // Not `holder` (it contains a root) and not `holder/loose/deep` — the
        // collapse rises to the highest directory that is still addable.
        assert_eq!(paths(&result), vec![holder.join("loose")]);
    }

    #[test]
    fn find_root_candidates_bounded_by_scope() {
        // The collapse never rises above the scope it was asked about, even
        // when every file below it shares a higher ancestor.
        let temp = TempDir::new().unwrap();
        let scope = temp.path().join("scope");
        fs::create_dir_all(&scope).unwrap();
        dir_with_file(temp.path(), "sibling");
        dir_with_file(&scope, "a/b");
        dir_with_file(&scope, "a/c");

        let result = find_root_candidates(&scope, &[]).unwrap();

        assert_eq!(paths(&result), vec![scope.join("a")]);
    }

    #[test]
    fn find_root_candidates_empty_scope_no_candidates() {
        // Directories alone are not content: a scope holding no files offers
        // nothing, rather than proposing its own empty skeleton.
        let temp = TempDir::new().unwrap();
        let scope = temp.path();
        fs::create_dir_all(scope.join("empty/deeper")).unwrap();

        let result = find_root_candidates(scope, &[]).unwrap();

        assert!(result.candidates.is_empty());
        assert!(result.warnings.is_empty());
    }
}
