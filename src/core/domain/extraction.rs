//! Domain types for the extraction ledger — the trail's outbound direction.
//!
//! An extraction row records the aggregate shape of one decision drawing
//! content out of one source root into an archive: how much, from where
//! within the root, to where in the destination, and how (copied vs moved).
//! Aggregate only — per-item detail stays in the apply receipt on disk.
//!
//! **The uniform row claim**: every row asserts that its files lie *under*
//! its recorded origin location and were placed *under* its recorded
//! destination location. Rows written at directory precision (one row per
//! origin-directory × destination-directory pair) make that claim tightly —
//! the locations are the items' containing directories; legacy rows written
//! before precision hold lossy common prefixes and make the same claim
//! slackly. Matching and counting treat both identically
//! (`placement_in_view`): a row surfaces exactly where the view contains its
//! location, and wherever it surfaces its count is exact.

use std::collections::BTreeMap;

use crate::domain::root::Root;

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
    /// Where within the root this row's files were drawn from — their
    /// containing directory at directory precision; on legacy rows, the
    /// lossy common prefix of every drawn item. Either way, all counted
    /// files lie under it (the uniform row claim).
    pub rel_prefix: String,
    pub files: i64,
    /// `None` only when the record can't say — never guessed.
    pub bytes: Option<i64>,
    pub destination_root_id: Option<i64>,
    /// Destination root path + where under it this row's files landed —
    /// their containing directory at directory precision; on legacy rows,
    /// the decision-wide common prefix. Never the manifest's configured
    /// base dir.
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

/// Aggregate completed transfers into [`DecisionExtraction`] rows at
/// directory precision: one row per (source root, origin directory,
/// destination directory) present among the items, so every recorded
/// location is a place files demonstrably are — matchable at recorded
/// precision, never a lossy summary. `destination_path` is derived from the
/// *items'* destination dirs (never a caller-supplied base dir) so forward
/// recording and backfill derive it identically — the round-trip law.
///
/// Source roots that match no known root's stored path are reported in the
/// second return value instead of producing rows — forward recording warns
/// on this (transfers come from indexed sources, so it shouldn't happen);
/// reindex reports it as a partial-index gap (`unknown_source_roots`).
pub fn build_extraction_rows<'a>(
    items: &[ExtractionItem<'a>],
    known_roots: &[Root],
    destination_root: (Option<i64>, &str),
    disposition: Option<OriginDisposition>,
    decision_id: i64,
) -> (Vec<DecisionExtraction>, Vec<String>) {
    // A BTreeMap keys the grouping *and* the output order: rows come out
    // sorted by (source root, origin dir, destination dir), a function of
    // the input rather than of hashing — forward recording and backfill
    // must produce identical row sequences for the round-trip law.
    let mut groups: BTreeMap<(&str, &str, &str), (i64, i64)> = BTreeMap::new();
    for item in items {
        let key = (
            item.source_root,
            parent_dir(item.source_rel_path),
            parent_dir(item.destination_rel_path),
        );
        let entry = groups.entry(key).or_insert((0, 0));
        entry.0 += 1;
        entry.1 += item.size;
    }

    let mut rows = Vec::new();
    let mut unknown_roots: Vec<String> = Vec::new();
    for ((source_root, origin_dir, dest_dir), (files, bytes)) in groups {
        match known_roots.iter().find(|r| r.path == source_root) {
            Some(root) => rows.push(DecisionExtraction {
                decision_id,
                root_id: root.id,
                root_path: root.path.clone(),
                rel_prefix: origin_dir.to_string(),
                files,
                bytes: Some(bytes),
                destination_root_id: destination_root.0,
                destination_path: join_prefix(destination_root.1, dest_dir),
                disposition,
            }),
            None => {
                if unknown_roots.last().map(String::as_str) != Some(source_root) {
                    unknown_roots.push(source_root.to_string());
                }
            }
        }
    }

    (rows, unknown_roots)
}

/// The containing directory of a relative item path — `""` for a top-level
/// file. Unlike a common-prefix collapse, this is exact: the file is *in*
/// this directory.
fn parent_dir(rel_path: &str) -> &str {
    rel_path.rsplit_once('/').map(|(dir, _)| dir).unwrap_or("")
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
        // Both groups landed in the same destination directory here — equal
        // by observation, not decision-wide by construction.
        assert_eq!(a.destination_path, "/archive/out");
        assert_eq!(b.destination_path, "/archive/out");
    }

    #[test]
    fn build_extraction_rows_one_row_per_directory_pair() {
        // The precision this ledger exists for: items fanning out to two
        // destination directories produce two rows, each claiming exactly
        // the directory its files are in — never one row under a lossy
        // common prefix that a sibling view would falsely match.
        let items = vec![
            ExtractionItem {
                source_root: "/vol/sd",
                source_rel_path: "dcim/a.jpg",
                destination_rel_path: "m/01/a.jpg",
                size: 100,
            },
            ExtractionItem {
                source_root: "/vol/sd",
                source_rel_path: "dcim/b.jpg",
                destination_rel_path: "m/01/b.jpg",
                size: 5,
            },
            ExtractionItem {
                source_root: "/vol/sd",
                source_rel_path: "dcim/c.jpg",
                destination_rel_path: "m/02/c.jpg",
                size: 140,
            },
        ];
        let roots = vec![root(1, "/vol/sd")];
        let (rows, unknown) =
            build_extraction_rows(&items, &roots, (Some(9), "/archive"), None, 42);
        assert!(unknown.is_empty());
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].rel_prefix, "dcim");
        assert_eq!(rows[0].destination_path, "/archive/m/01");
        assert_eq!(rows[0].files, 2);
        assert_eq!(rows[0].bytes, Some(105));
        assert_eq!(rows[1].destination_path, "/archive/m/02");
        assert_eq!(rows[1].files, 1);
        assert_eq!(rows[1].bytes, Some(140));
    }

    #[test]
    fn build_extraction_rows_sum_to_the_items() {
        // The rows are a partition of the items: files and bytes across all
        // rows total the input exactly, whatever the directory fan-out.
        let items = vec![
            ExtractionItem {
                source_root: "/vol/sd",
                source_rel_path: "a/1.jpg",
                destination_rel_path: "x/1.jpg",
                size: 1,
            },
            ExtractionItem {
                source_root: "/vol/sd",
                source_rel_path: "b/2.jpg",
                destination_rel_path: "y/2.jpg",
                size: 2,
            },
            ExtractionItem {
                source_root: "/vol/sd",
                source_rel_path: "a/3.jpg",
                destination_rel_path: "y/3.jpg",
                size: 4,
            },
            ExtractionItem {
                source_root: "/vol/other",
                source_rel_path: "4.jpg",
                destination_rel_path: "x/4.jpg",
                size: 8,
            },
        ];
        let roots = vec![root(1, "/vol/sd"), root(2, "/vol/other")];
        let (rows, _) = build_extraction_rows(&items, &roots, (Some(9), "/archive"), None, 1);
        assert_eq!(rows.iter().map(|r| r.files).sum::<i64>(), 4);
        assert_eq!(rows.iter().filter_map(|r| r.bytes).sum::<i64>(), 15);
    }

    #[test]
    fn build_extraction_rows_order_is_a_function_of_the_input() {
        // Same items, shuffled — identical row sequence (the round-trip law
        // compares forward and backfilled rows in order).
        let mk = |src: &'static str, dst: &'static str| ExtractionItem {
            source_root: "/vol/sd",
            source_rel_path: src,
            destination_rel_path: dst,
            size: 1,
        };
        let roots = vec![root(1, "/vol/sd")];
        let forward = vec![mk("b/1.jpg", "y/1.jpg"), mk("a/2.jpg", "x/2.jpg")];
        let shuffled = vec![mk("a/2.jpg", "x/2.jpg"), mk("b/1.jpg", "y/1.jpg")];
        let (rows_a, _) = build_extraction_rows(&forward, &roots, (Some(9), "/archive"), None, 1);
        let (rows_b, _) = build_extraction_rows(&shuffled, &roots, (Some(9), "/archive"), None, 1);
        assert_eq!(rows_a, rows_b);
        assert_eq!(rows_a[0].rel_prefix, "a");
        assert_eq!(rows_a[1].rel_prefix, "b");
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
