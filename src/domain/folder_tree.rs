//! A folder tree with interned folder ids — shared tree infrastructure for
//! boundary-finding walks (the sweep's containment walk, the story review's
//! signature walk). Topology only: consumers hold their payloads in
//! side-vectors indexed by folder id.

use std::collections::HashMap;

/// Folder tree with interned folder ids. Folder `""` is the root folder
/// itself. Ids are created parents-first, so `parent(fid) < fid` always
/// holds — bottom-up aggregation is a reverse walk over ids.
pub struct FolderTree {
    ids: HashMap<String, u32>,
    paths: Vec<String>,
    parent: Vec<Option<u32>>,
    depth: Vec<u32>,
    children: Vec<Vec<u32>>,
}

impl FolderTree {
    pub fn new() -> Self {
        Self {
            ids: HashMap::new(),
            paths: Vec::new(),
            parent: Vec::new(),
            depth: Vec::new(),
            children: Vec::new(),
        }
    }

    /// Intern a folder path (and any missing ancestors), returning its id.
    pub fn intern(&mut self, folder: &str) -> u32 {
        if let Some(&fid) = self.ids.get(folder) {
            return fid;
        }
        // Walk up to the nearest interned ancestor, collecting the missing
        // chain, then create it shallowest-first so parents precede children.
        let mut chain: Vec<String> = Vec::new();
        let mut cur = folder.to_string();
        let mut anchor: Option<u32> = None;
        loop {
            chain.push(cur.clone());
            if cur.is_empty() {
                break;
            }
            let head = match cur.rsplit_once('/') {
                Some((head, _)) => head.to_string(),
                None => String::new(),
            };
            if let Some(&fid) = self.ids.get(&head) {
                anchor = Some(fid);
                break;
            }
            cur = head;
        }
        let mut parent = anchor;
        for path in chain.into_iter().rev() {
            let fid = self.paths.len() as u32;
            self.ids.insert(path.clone(), fid);
            self.paths.push(path);
            self.parent.push(parent);
            self.depth.push(match parent {
                Some(p) => self.depth[p as usize] + 1,
                None => 0,
            });
            self.children.push(Vec::new());
            if let Some(p) = parent {
                self.children[p as usize].push(fid);
            }
            parent = Some(fid);
        }
        parent.expect("intern creates at least one folder")
    }

    /// Look up an already-interned folder's id.
    pub fn id(&self, folder: &str) -> Option<u32> {
        self.ids.get(folder).copied()
    }

    pub fn len(&self) -> usize {
        self.paths.len()
    }

    pub fn is_empty(&self) -> bool {
        self.paths.is_empty()
    }

    pub fn path(&self, fid: u32) -> &str {
        &self.paths[fid as usize]
    }

    pub fn parent(&self, fid: u32) -> Option<u32> {
        self.parent[fid as usize]
    }

    pub fn children(&self, fid: u32) -> &[u32] {
        &self.children[fid as usize]
    }

    /// Whether `anc` is `desc` or one of its ancestors.
    pub fn is_ancestor_or_equal(&self, anc: u32, desc: u32) -> bool {
        let mut cur = Some(desc);
        while let Some(f) = cur {
            if f == anc {
                return true;
            }
            cur = self.parent[f as usize];
        }
        false
    }

    /// Lowest common ancestor of two folders.
    pub fn lca(&self, a: u32, b: u32) -> u32 {
        let (mut a, mut b) = (a, b);
        while self.depth[a as usize] > self.depth[b as usize] {
            a = self.parent[a as usize].expect("deeper folder has a parent");
        }
        while self.depth[b as usize] > self.depth[a as usize] {
            b = self.parent[b as usize].expect("deeper folder has a parent");
        }
        while a != b {
            a = self.parent[a as usize].expect("distinct folders share a root ancestor");
            b = self.parent[b as usize].expect("distinct folders share a root ancestor");
        }
        a
    }
}

impl Default for FolderTree {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intern_is_idempotent_with_correct_parent_chain() {
        let mut tree = FolderTree::new();
        let c = tree.intern("a/b/c");
        assert_eq!(tree.intern("a/b/c"), c);
        assert_eq!(tree.path(c), "a/b/c");
        let b = tree.parent(c).unwrap();
        assert_eq!(tree.path(b), "a/b");
        let a = tree.parent(b).unwrap();
        assert_eq!(tree.path(a), "a");
        let root = tree.parent(a).unwrap();
        assert_eq!(tree.path(root), "");
        assert_eq!(tree.parent(root), None);
        assert_eq!(tree.len(), 4);
    }

    #[test]
    fn parent_id_always_precedes_child_id() {
        let mut tree = FolderTree::new();
        tree.intern("x/y/z");
        tree.intern("a");
        tree.intern("x/w");
        for fid in 0..tree.len() as u32 {
            if let Some(p) = tree.parent(fid) {
                assert!(p < fid, "parent {p} must precede child {fid}");
            }
        }
    }

    #[test]
    fn deep_path_interns_iteratively() {
        let deep = (0..60)
            .map(|i| format!("d{i}"))
            .collect::<Vec<_>>()
            .join("/");
        let mut tree = FolderTree::new();
        let fid = tree.intern(&deep);
        assert_eq!(tree.len(), 61); // 60 segments + the root folder
        assert_eq!(tree.path(fid), deep);
    }

    #[test]
    fn id_looks_up_interned_folders_only() {
        let mut tree = FolderTree::new();
        let ab = tree.intern("a/b");
        assert_eq!(tree.id("a/b"), Some(ab));
        assert_eq!(tree.id("a"), Some(tree.parent(ab).unwrap()));
        assert_eq!(tree.id("a/b/c"), None);
    }

    #[test]
    fn lca_covers_all_relations() {
        let mut tree = FolderTree::new();
        let root = tree.intern("");
        let ab = tree.intern("a/b");
        let ac = tree.intern("a/c");
        let a = tree.intern("a");
        let x = tree.intern("x");
        assert_eq!(tree.lca(ab, ab), ab);
        assert_eq!(tree.lca(a, ab), a);
        assert_eq!(tree.lca(ab, ac), a);
        assert_eq!(tree.lca(ab, x), root);
    }
}
