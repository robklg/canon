//! The shelf listing: the retired fleet, disk-primary.

use std::collections::HashSet;
use std::path::Path;

use anyhow::Result;
use rusqlite::Connection;
use serde::Deserialize;

use crate::core::domain::config::LedgerConfig;
use crate::core::domain::decision::DecisionCommand;
use crate::core::repo;
use crate::ops;

use super::{iso_date, SHELF_DIR};

pub enum ShelfLine {
    /// A book standing on the shelf, enriched from its decision row when one
    /// matches (`indexed`); an Off-mode book renders from its meta alone.
    Book {
        root_path: String,
        /// The retirement decision's date, or the book's own compile date
        /// when no row matches. `None` only if the meta carries no date.
        retired_on: Option<String>,
        entries: Option<i64>,
        /// The book's directory name on the shelf.
        book_dir: String,
        reason: Option<String>,
        indexed: bool,
    },
    /// A recorded retirement with no standing book on the shelf.
    RecordedOnly {
        root_path: String,
        retired_on: String,
        /// The recorded book location (marked fallback if the shelf's root
        /// left the index).
        book_path: String,
        reason: Option<String>,
    },
    /// A directory on the shelf that could not be identified as a book —
    /// counted, never silently skipped.
    Unidentified { dir_name: String },
}

/// The retired fleet: a union, disk-primary. Books on the shelf are the
/// primary lines (the concept's "browsing the shelf reads the retired
/// fleet"); decision rows enrich them and contribute marked lines for
/// retirements whose book is not standing. When the shelf is unreachable
/// the rows still answer, hedged (`shelf_reachable`).
pub struct ShelfListing {
    /// The shelf's path; `None` when no archive root is registered.
    pub shelf: Option<String>,
    pub shelf_reachable: bool,
    /// Chronological; unidentified directories last.
    pub lines: Vec<ShelfLine>,
    /// `.replaced-<name>` directories — a replaced book set aside by a swap
    /// that never finished (crash between old-aside and old-removed). Each
    /// is a full standing book copy the fleet must not silently hide;
    /// fix-forward removes it only when a same-name bind completes.
    pub aside_dirs: Vec<String>,
}

/// The listing probe: identify a book for its fleet line. Identification,
/// not verification — deliberately no version gate (a future-version book
/// still lists; only `verify_book` refuses what it cannot check).
#[derive(Deserialize)]
struct ListingProbe {
    identity: ListingIdentity,
    counts: Option<ListingCounts>,
}

#[derive(Deserialize)]
struct ListingIdentity {
    path: String,
    compiled_at: Option<String>,
}

#[derive(Deserialize)]
struct ListingCounts {
    entries: Option<i64>,
}

fn read_listing_probe(dir: &Path) -> Option<ListingProbe> {
    let raw = std::fs::read_to_string(dir.join("meta.toml")).ok()?;
    toml::from_str(&raw).ok()
}

/// Compute the retired fleet. Rows join books on one key: the decision's
/// recorded artifact reference naming the book's directory — the key every
/// bound book carries, past and future, so no book needs a special case.
pub fn compute_shelf_listing(conn: &Connection, config: &LedgerConfig) -> Result<ShelfListing> {
    let roots = repo::root::fetch_all(conn)?;
    let rows =
        crate::retire::repo::fetch_bound_retirements(conn, DecisionCommand::RootsRetire.as_str())?;
    let shelf = ops::receipt::resolve_ledger_root(&roots, config)
        .map(|(_, path)| format!("{path}/{SHELF_DIR}"));

    let basename = |rel: &str| rel.rsplit('/').next().unwrap_or(rel).to_string();

    let mut lines: Vec<ShelfLine> = Vec::new();
    let mut book_dirs: HashSet<String> = HashSet::new();
    let mut aside_dirs: Vec<String> = Vec::new();
    let mut shelf_reachable = false;
    if let Some(shelf_path) = &shelf {
        if let Ok(entries) = std::fs::read_dir(shelf_path) {
            shelf_reachable = true;
            let mut dirs: Vec<String> = entries
                .filter_map(|e| e.ok())
                .filter(|e| e.path().is_dir())
                .filter_map(|e| e.file_name().to_str().map(String::from))
                // A `.compiling-<name>` temp was never placed — not fleet.
                // A `.replaced-<name>` aside is a stranded full book copy
                // (interrupted swap) — not fleet either, but never silent:
                // counted and named so absence is explained.
                .filter(|name| {
                    if name.starts_with(".replaced-") {
                        aside_dirs.push(name.clone());
                        return false;
                    }
                    !name.starts_with('.')
                })
                .collect();
            aside_dirs.sort();
            dirs.sort();
            for dir_name in dirs {
                let dir = Path::new(shelf_path).join(&dir_name);
                match read_listing_probe(&dir) {
                    Some(probe) => {
                        // Newest first, so the first match is the newest
                        // retirement referencing this book.
                        let row = rows
                            .iter()
                            .find(|r| basename(&r.receipt_rel_path) == dir_name);
                        lines.push(ShelfLine::Book {
                            root_path: probe.identity.path,
                            retired_on: row.map(|r| iso_date(r.created_at)).or_else(|| {
                                probe
                                    .identity
                                    .compiled_at
                                    .map(|at| at.chars().take(10).collect())
                            }),
                            entries: probe.counts.and_then(|c| c.entries),
                            book_dir: dir_name.clone(),
                            reason: row.and_then(|r| r.reason.clone()),
                            indexed: row.is_some(),
                        });
                        book_dirs.insert(dir_name);
                    }
                    None => lines.push(ShelfLine::Unidentified { dir_name }),
                }
            }
        }
    }

    // Rows whose book is not standing — deduped to the newest per recorded
    // location (a replaced re-run's older decision references the same book).
    let mut seen_paths: HashSet<(i64, String)> = HashSet::new();
    for row in &rows {
        if shelf_reachable && book_dirs.contains(&basename(&row.receipt_rel_path)) {
            continue;
        }
        if !seen_paths.insert((row.receipt_root_id, row.receipt_rel_path.clone())) {
            continue;
        }
        let book_path = match roots.iter().find(|r| r.id == row.receipt_root_id) {
            Some(root) => format!("{}/{}", root.path, row.receipt_rel_path),
            None => format!(
                "root #{} (removed)/{}",
                row.receipt_root_id, row.receipt_rel_path
            ),
        };
        lines.push(ShelfLine::RecordedOnly {
            root_path: row.root_path.clone(),
            retired_on: iso_date(row.created_at),
            book_path,
            reason: row.reason.clone(),
        });
    }

    // Chronological (the trail's convention: the fleet reads as the story of
    // finishing); unidentified directories last, in name order.
    lines.sort_by(|a, b| {
        let key = |l: &ShelfLine| match l {
            ShelfLine::Book { retired_on, .. } => (0, retired_on.clone().unwrap_or_default()),
            ShelfLine::RecordedOnly { retired_on, .. } => (0, retired_on.clone()),
            ShelfLine::Unidentified { dir_name } => (1, dir_name.clone()),
        };
        key(a).cmp(&key(b))
    });

    Ok(ShelfListing {
        shelf,
        shelf_reachable,
        lines,
        aside_dirs,
    })
}
