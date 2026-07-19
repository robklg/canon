//! Domain types for the extraction ledger — the trail's outbound direction.
//!
//! An extraction row records the aggregate shape of one decision drawing
//! content out of one source root into an archive: how much, from where
//! within the root, to where in the destination, and how (copied vs moved).
//! Aggregate only — per-item detail stays in the apply receipt on disk.

use std::collections::HashMap;

use super::path::common_dir_prefix;
use super::root::Root;

/// The origin's fate after an apply transfer — registered vocabulary (receipt
/// `origin_disposition`). `None` on backfilled rows from pre-vocabulary
/// receipts, rendered neutrally rather than guessed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OriginDisposition {
    Retained,
    Relocated,
}

impl OriginDisposition {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Retained => "retained",
            Self::Relocated => "relocated",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "retained" => Some(Self::Retained),
            "relocated" => Some(Self::Relocated),
            _ => None,
        }
    }
}

/// One row of the extraction ledger: a decision drawing content from one
/// source root. Aggregate-level by construction — never per-item; per-item
/// detail stays in the apply receipt on disk.
#[derive(Debug, Clone, PartialEq)]
pub struct DecisionExtraction {
    pub decision_id: i64,
    pub root_id: i64,
    /// Snapshot of the source root's canonical path at record time — outlives
    /// root removal, so `show` and the aspect line can render forever.
    pub root_path: String,
    /// Common directory prefix of the drawn items' rel paths within the root.
    pub rel_prefix: String,
    pub files: i64,
    /// `None` only when the record can't say — never guessed.
    pub bytes: Option<i64>,
    pub destination_root_id: Option<i64>,
    /// Destination root path + common directory prefix of the drawn items'
    /// destination rel paths — decision-wide (shared across every row of one
    /// decision), not the manifest's configured base dir.
    pub destination_path: String,
    /// `None` for pre-vocabulary backfilled receipts — rendered neutrally.
    pub disposition: Option<OriginDisposition>,
}

impl DecisionExtraction {
    /// The location this row was drawn from: the source root's path plus its
    /// common rel prefix (an empty prefix means the root itself). The one
    /// derivation of an extraction's "here" — the trail's scope cell and
    /// `trail show`'s `drew from:` section both render it, and neither
    /// re-joins the parts itself.
    pub fn drawn_from(&self) -> String {
        join_prefix(&self.root_path, &self.rel_prefix)
    }

    /// Whether this row's origin root is gone from the live index — the one
    /// derivation behind every `(root removed)` marker.
    ///
    /// Matched on the **snapshot path, not `root_id`**, for the same reason
    /// arrival matching is: a root that was removed and re-added carries a
    /// new id, so an id comparison would call a perfectly visitable location
    /// removed. A root that exists today is visitable today, whatever a stale
    /// snapshot id says.
    ///
    /// Takes live paths rather than a prepared set so each caller can pass
    /// what it already holds; the root count is small and this runs per
    /// rendered row.
    pub fn origin_root_removed<'a>(
        &self,
        live_root_paths: impl IntoIterator<Item = &'a str>,
    ) -> bool {
        !live_root_paths
            .into_iter()
            .any(|path| path == self.root_path)
    }
}

/// One completed transfer, item-shaped for [`build_extraction_rows`] — the
/// shape both the forward (apply) and backfill (`ledger reindex`) recording
/// paths share, so a backfilled row is structurally indistinguishable from a
/// forward-recorded one.
pub struct ExtractionItem<'a> {
    pub source_root: &'a str,
    pub source_rel_path: &'a str,
    pub destination_rel_path: &'a str,
    pub size: i64,
}

/// Aggregate completed transfers into one [`DecisionExtraction`] row per
/// source root. `destination_path` is derived from the *items'* common
/// destination prefix (never a caller-supplied base dir) so forward
/// recording and backfill derive it identically — the round-trip law.
///
/// Source roots that match no known root's stored path are reported in the
/// second return value instead of producing a row — forward recording warns
/// on this (transfers come from indexed sources, so it shouldn't happen);
/// reindex reports it as a partial-index gap (`unknown_source_roots`).
pub fn build_extraction_rows<'a>(
    items: &[ExtractionItem<'a>],
    known_roots: &[Root],
    destination_root: (Option<i64>, &str),
    disposition: Option<OriginDisposition>,
    decision_id: i64,
) -> (Vec<DecisionExtraction>, Vec<String>) {
    if items.is_empty() {
        return (Vec::new(), Vec::new());
    }

    let dest_prefix = common_dir_prefix(items.iter().map(|i| i.destination_rel_path));
    let destination_path = join_prefix(destination_root.1, &dest_prefix);

    // Group in one pass, then sort by root path so the row order is a
    // function of the input rather than of hashing — forward recording and
    // backfill must produce identical row sequences for the round-trip law.
    let mut groups: HashMap<&str, Vec<&ExtractionItem>> = HashMap::new();
    for item in items {
        groups.entry(item.source_root).or_default().push(item);
    }
    let mut source_roots: Vec<&str> = groups.keys().copied().collect();
    source_roots.sort_unstable();

    let mut rows = Vec::new();
    let mut unknown_roots = Vec::new();
    for source_root in source_roots {
        let group = &groups[source_root];
        match known_roots.iter().find(|r| r.path == source_root) {
            Some(root) => rows.push(DecisionExtraction {
                decision_id,
                root_id: root.id,
                root_path: root.path.clone(),
                rel_prefix: common_dir_prefix(group.iter().map(|i| i.source_rel_path)),
                files: group.len() as i64,
                bytes: Some(group.iter().map(|i| i.size).sum()),
                destination_root_id: destination_root.0,
                destination_path: destination_path.clone(),
                disposition,
            }),
            None => unknown_roots.push(source_root.to_string()),
        }
    }

    (rows, unknown_roots)
}

fn join_prefix(root_path: &str, prefix: &str) -> String {
    if prefix.is_empty() {
        root_path.to_string()
    } else {
        format!("{root_path}/{prefix}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root(id: i64, path: &str) -> Root {
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
    fn origin_disposition_round_trips() {
        assert_eq!(OriginDisposition::Retained.as_str(), "retained");
        assert_eq!(OriginDisposition::Relocated.as_str(), "relocated");
        assert_eq!(
            OriginDisposition::from_str("retained"),
            Some(OriginDisposition::Retained)
        );
        assert_eq!(
            OriginDisposition::from_str("relocated"),
            Some(OriginDisposition::Relocated)
        );
        assert_eq!(OriginDisposition::from_str("unknown"), None);
    }

    #[test]
    fn drawn_from_joins_prefix_and_bare_root() {
        let mut row = DecisionExtraction {
            decision_id: 1,
            root_id: 1,
            root_path: "/vol/photos".to_string(),
            rel_prefix: "2016/italy".to_string(),
            files: 1,
            bytes: None,
            destination_root_id: None,
            destination_path: "/archive".to_string(),
            disposition: None,
        };
        assert_eq!(row.drawn_from(), "/vol/photos/2016/italy");
        row.rel_prefix = String::new();
        assert_eq!(row.drawn_from(), "/vol/photos");
    }

    #[test]
    fn origin_root_removed_reads_paths_not_ids() {
        let row = DecisionExtraction {
            decision_id: 1,
            // A stale id: this root was removed and re-added, so the live
            // index knows the same location under a different id now.
            root_id: 7,
            root_path: "/vol/photos".to_string(),
            rel_prefix: String::new(),
            files: 1,
            bytes: None,
            destination_root_id: None,
            destination_path: "/archive".to_string(),
            disposition: None,
        };
        // Live at the same path under id 99 — visitable today, so not removed.
        assert!(!row.origin_root_removed(["/vol/photos"]));
        // Gone entirely.
        assert!(row.origin_root_removed(["/archive", "/vol/other"]));
        assert!(row.origin_root_removed([]));
        // Segment boundary: /vol/photos-old is a different location.
        assert!(row.origin_root_removed(["/vol/photos-old"]));
    }

    #[test]
    fn build_extraction_rows_orders_rows_by_source_root() {
        // Row order must be a function of the input, not of hash iteration —
        // the round-trip law compares forward and backfilled rows in order.
        let items = vec![
            ExtractionItem {
                source_root: "/vol/c",
                source_rel_path: "1.jpg",
                destination_rel_path: "1.jpg",
                size: 1,
            },
            ExtractionItem {
                source_root: "/vol/a",
                source_rel_path: "2.jpg",
                destination_rel_path: "2.jpg",
                size: 1,
            },
            ExtractionItem {
                source_root: "/vol/b",
                source_rel_path: "3.jpg",
                destination_rel_path: "3.jpg",
                size: 1,
            },
        ];
        let roots = vec![root(1, "/vol/a"), root(2, "/vol/b"), root(3, "/vol/c")];
        let (rows, _) = build_extraction_rows(&items, &roots, (Some(1), "/archive"), None, 1);
        let paths: Vec<&str> = rows.iter().map(|r| r.root_path.as_str()).collect();
        assert_eq!(paths, vec!["/vol/a", "/vol/b", "/vol/c"]);
    }

    #[test]
    fn build_extraction_rows_empty_items_is_empty() {
        let (rows, unknown) = build_extraction_rows(&[], &[], (Some(1), "/archive"), None, 1);
        assert!(rows.is_empty());
        assert!(unknown.is_empty());
    }

    #[test]
    fn build_extraction_rows_single_root() {
        let items = vec![
            ExtractionItem {
                source_root: "/vol/photos",
                source_rel_path: "2016/italy/a.jpg",
                destination_rel_path: "2016/Italy/a.jpg",
                size: 100,
            },
            ExtractionItem {
                source_root: "/vol/photos",
                source_rel_path: "2016/italy/b.jpg",
                destination_rel_path: "2016/Italy/b.jpg",
                size: 200,
            },
        ];
        let roots = vec![root(1, "/vol/photos")];
        let (rows, unknown) = build_extraction_rows(
            &items,
            &roots,
            (Some(7), "/archive"),
            Some(OriginDisposition::Retained),
            42,
        );
        assert!(unknown.is_empty());
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.decision_id, 42);
        assert_eq!(row.root_id, 1);
        assert_eq!(row.root_path, "/vol/photos");
        assert_eq!(row.rel_prefix, "2016/italy");
        assert_eq!(row.files, 2);
        assert_eq!(row.bytes, Some(300));
        assert_eq!(row.destination_root_id, Some(7));
        assert_eq!(row.destination_path, "/archive/2016/Italy");
        assert_eq!(row.disposition, Some(OriginDisposition::Retained));
    }

    #[test]
    fn build_extraction_rows_multi_root_one_row_each() {
        let items = vec![
            ExtractionItem {
                source_root: "/vol/a",
                source_rel_path: "x/1.jpg",
                destination_rel_path: "out/1.jpg",
                size: 10,
            },
            ExtractionItem {
                source_root: "/vol/b",
                source_rel_path: "y/2.jpg",
                destination_rel_path: "out/2.jpg",
                size: 20,
            },
        ];
        let roots = vec![root(1, "/vol/a"), root(2, "/vol/b")];
        let (rows, unknown) = build_extraction_rows(&items, &roots, (Some(9), "/archive"), None, 1);
        assert!(unknown.is_empty());
        assert_eq!(rows.len(), 2);
        let a = rows.iter().find(|r| r.root_id == 1).unwrap();
        let b = rows.iter().find(|r| r.root_id == 2).unwrap();
        assert_eq!(a.rel_prefix, "x");
        assert_eq!(a.files, 1);
        assert_eq!(b.rel_prefix, "y");
        assert_eq!(b.files, 1);
        // destination_path is decision-wide, shared across both rows.
        assert_eq!(a.destination_path, b.destination_path);
    }

    #[test]
    fn build_extraction_rows_unknown_source_root_reported() {
        let items = vec![ExtractionItem {
            source_root: "/vol/gone",
            source_rel_path: "a.jpg",
            destination_rel_path: "a.jpg",
            size: 1,
        }];
        let (rows, unknown) = build_extraction_rows(&items, &[], (Some(1), "/archive"), None, 1);
        assert!(rows.is_empty());
        assert_eq!(unknown, vec!["/vol/gone".to_string()]);
    }

    #[test]
    fn build_extraction_rows_partial_unknown_roots_still_indexes_matched() {
        let items = vec![
            ExtractionItem {
                source_root: "/vol/known",
                source_rel_path: "a.jpg",
                destination_rel_path: "a.jpg",
                size: 1,
            },
            ExtractionItem {
                source_root: "/vol/unknown",
                source_rel_path: "b.jpg",
                destination_rel_path: "b.jpg",
                size: 2,
            },
        ];
        let roots = vec![root(1, "/vol/known")];
        let (rows, unknown) = build_extraction_rows(&items, &roots, (Some(1), "/archive"), None, 1);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].root_id, 1);
        assert_eq!(unknown, vec!["/vol/unknown".to_string()]);
    }

    #[test]
    fn build_extraction_rows_root_level_file_has_empty_prefix() {
        let items = vec![ExtractionItem {
            source_root: "/vol/photos",
            source_rel_path: "a.jpg",
            destination_rel_path: "a.jpg",
            size: 1,
        }];
        let roots = vec![root(1, "/vol/photos")];
        let (rows, _) = build_extraction_rows(&items, &roots, (Some(1), "/archive"), None, 1);
        assert_eq!(rows[0].rel_prefix, "");
        assert_eq!(rows[0].destination_path, "/archive");
    }
}
