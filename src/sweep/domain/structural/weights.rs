//! Stage B — LCA-subtraction matched weights. The correctness heart of the
//! sweep: an object duplicated only within one root is matched everywhere
//! except on the path from its copies' common ancestor up to the root.

use super::universe::{subtree_sums, Universe};

/// Per-root, per-folder matched weight, in two forms.
///
/// `pub` (not `pub(super)`): the domain test tree sits as a sibling of
/// `structural/`, not nested inside it, so cross-module reach for the test
/// helpers needs the same visibility as any other stage-to-stage boundary —
/// the private `mod` chain up to `sweep.rs`'s sealed barrel is what actually
/// keeps this out of reach from other subsystems, not this item's own marking.
pub struct MatchedWeights {
    /// Subtree weight of content that exists outside that folder — anywhere:
    /// another root, or the same root outside the subtree. NOT monotonic down
    /// the tree: the intra-root subtraction can leave a parent with less
    /// matched weight than its children.
    pub matched: Vec<Vec<(u64, u32)>>,
    /// Pre-subtraction subtree weight: a monotonic upper bound on `matched`,
    /// safe for trimming whole subtrees during the descent.
    pub upper: Vec<Vec<(u64, u32)>>,
}

/// Compute matched weights per root.
///
/// An object duplicated only within one root is matched everywhere except on
/// the path from its copies' common ancestor up to the root: those are
/// exactly the subtrees containing every copy, where nothing exists outside.
/// The same subtraction makes an object with all copies in one folder match
/// nowhere. Cross-root objects match at every local node unconditionally.
pub fn compute_matched(universe: &Universe) -> MatchedWeights {
    let mut leaf: Vec<Vec<(u64, u32)>> = universe
        .roots_data
        .iter()
        .map(|rd| vec![(0, 0); rd.tree.len()])
        .collect();
    // (lca fid, bytes, files) to remove along the lca→root path, per root.
    let mut subtractions: Vec<Vec<(u32, u64, u32)>> = vec![Vec::new(); universe.roots_data.len()];

    for (oid, locs) in &universe.obj_locs {
        let size = universe.obj_size[oid];
        let cross_root = locs.iter().any(|&(ri, _)| ri != locs[0].0);
        for &(ri, fid) in locs {
            let cell = &mut leaf[ri][fid as usize];
            cell.0 += size;
            cell.1 += 1;
        }
        if !cross_root {
            let ri = locs[0].0;
            let tree = &universe.roots_data[ri].tree;
            let lca = locs
                .iter()
                .map(|&(_, fid)| fid)
                .reduce(|a, b| tree.lca(a, b))
                .expect("object has at least one copy");
            let copies = locs.len() as u32;
            subtractions[ri].push((lca, size * u64::from(copies), copies));
        }
    }

    let upper: Vec<Vec<(u64, u32)>> = universe
        .roots_data
        .iter()
        .zip(&leaf)
        .map(|(rd, l)| subtree_sums(&rd.tree, l))
        .collect();
    let mut matched = upper.clone();
    for (ri, subs) in subtractions.iter().enumerate() {
        let tree = &universe.roots_data[ri].tree;
        for &(lca, bytes, files) in subs {
            let mut cur = Some(lca);
            while let Some(fid) = cur {
                let cell = &mut matched[ri][fid as usize];
                cell.0 -= bytes;
                cell.1 -= files;
                cur = tree.parent(fid);
            }
        }
    }
    MatchedWeights { matched, upper }
}
