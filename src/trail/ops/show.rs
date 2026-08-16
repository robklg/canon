//! The trail's show path: `compute_show`, a single decision's full detail —
//! receipt pointers (with retirement relocation), and its extraction-ledger
//! `drew from:` lines.
//!
//! Everything here is served from DB projections — receipt files are never
//! read; pointers return their locations only. The one carve-out: pointer
//! relocation may *stat* the book (existence only, never contents) — all
//! data still comes from the index; only the redirect target's existence is
//! observed, and unreachability degrades to an honest hedge.
//!
//! Read-only: no transactions, no stdio.

use std::collections::{HashMap, HashSet};

use anyhow::Result;

use crate::core::domain::decision::{Decision, DecisionCommand};
use crate::core::domain::extraction::DecisionExtraction;
use crate::core::domain::root::Root;
use crate::core::repo::{self, Connection};
use crate::trail::domain::placement::{aggregate_placement_lines, RowAspect};

/// A receipt's on-disk location, as a pointer (contents are never read here).
pub struct ReceiptPointer {
    pub root_display: String,
    pub rel_path: String,
    /// When the locus root is retired: where the pointer now leads.
    pub relocation: Option<PointerRelocation>,
}

/// Where a retired root's receipt leads now. The book's location is a DB
/// projection (the retirement decision's artifact reference); the filesystem
/// is only consulted for existence — the book is stat'ed, never read.
pub enum PointerRelocation {
    /// The gathered copy stands in the book — a redirect you can follow.
    Gathered { book_ledger_path: String },
    /// The book stands but holds no gathered copy — the book records why
    /// (retired on faith, or the copy is gone). A stat cannot tell which,
    /// so this claims neither; the book's own gap record can.
    NotGathered { book_path: String },
    /// The book's location isn't reachable right now — no claim either way.
    Unreachable { book_path: String },
}

/// One `drew from:` line: an origin root's aggregate over the decision's
/// placement rows, with the distinct origin directories carried alongside
/// when the draw fanned out. Liveness is derived at read time from the live
/// roots list — never stored (the snapshot records what happened; the
/// marker says what the index knows now).
pub struct ShowExtraction {
    /// The collapsed drawn-from location: root path + the common prefix of
    /// the group's origin directories.
    pub location: String,
    pub root_removed: bool,
    /// Where a removed origin root's story lives now, when it was retired:
    /// the bound book's display path. `None` for a live root or a plain
    /// `roots rm` (no bound story to point at).
    pub retired_book: Option<String>,
    pub files: i64,
    /// `None` if any member row lacks a size — never a partial sum.
    pub bytes: Option<i64>,
    /// Distinct origin directories (root-relative; `""` is the root
    /// itself), each with its own counts, in recorded order. Empty when the
    /// group drew from a single directory — the location already says it.
    pub directories: Vec<ShowDrewDir>,
}

/// One origin directory's share of a `drew from:` group.
pub struct ShowDrewDir {
    pub dir: String,
    pub files: i64,
    pub bytes: Option<i64>,
}

pub struct ShowResult {
    pub decision: Decision,
    pub receipts: Vec<ReceiptPointer>,
    /// Why there is no receipt, when there is none — absence is never mute.
    pub receipt_absence: Option<String>,
    /// What this decision drew from each source root, if any (the extraction
    /// ledger's per-decision view — the source side of an apply).
    pub extractions: Vec<ShowExtraction>,
}

pub fn compute_show(conn: &Connection, id: i64) -> Result<Option<ShowResult>> {
    let Some(decision) = repo::decision::fetch_by_id(conn, id)? else {
        return Ok(None);
    };
    let roots = repo::root::fetch_all(conn)?;
    let root_display = |root_id: i64| {
        roots
            .iter()
            .find(|r| r.id == root_id)
            .map(|r| r.path.clone())
            .unwrap_or_else(|| format!("root #{root_id} (removed)"))
    };

    // Pointer rows carry their locus root id — it drives retirement
    // detection below; the display stays snapshot-first.
    let mut pointer_rows: Vec<(i64, String, String)> = Vec::new();
    if let (Some(root_id), Some(rel)) = (decision.receipt_root_id, &decision.receipt_rel_path) {
        pointer_rows.push((root_id, root_display(root_id), rel.clone()));
    }
    // Per-root receipts (e.g. one deletion receipt per source root).
    // Snapshot-first: the row's write-time root path renders even after the
    // root is removed; the live join covers pre-migration rows the hook
    // couldn't recover, and only then the marked fallback — the pointer
    // line is never silently absent.
    for row in crate::trail::repo::fetch_scope_rows(conn, id)? {
        if let Some(rel) = row.receipt_rel_path {
            let display = row
                .root_path
                .clone()
                .unwrap_or_else(|| root_display(row.root_id));
            let dup = pointer_rows
                .iter()
                .any(|(_, d, r)| *r == rel && *d == display);
            if !dup {
                pointer_rows.push((row.root_id, display, rel));
            }
        }
    }

    // Relocation: a pointer whose locus root is retired leads to the book
    // now. Detection is a DB projection — the newest bound `roots retire`
    // decision touching the root (the rm guard's lookup, reused). A removed
    // root with no such decision (plain rm, Off-mode retirement) keeps the
    // plain pointer.
    let live_ids: HashSet<i64> = roots.iter().map(|r| r.id).collect();
    let mut bindings: HashMap<i64, Option<(i64, String)>> = HashMap::new();
    for (root_id, _, _) in &pointer_rows {
        if !live_ids.contains(root_id) && !bindings.contains_key(root_id) {
            let binding = repo::decision::fetch_latest_receipt_for_root(
                conn,
                DecisionCommand::RootsRetire.as_str(),
                *root_id,
            )?;
            bindings.insert(*root_id, binding);
        }
    }
    let receipts: Vec<ReceiptPointer> =
        pointer_rows
            .into_iter()
            .map(|(root_id, root_display, rel_path)| {
                let relocation = bindings.get(&root_id).and_then(|b| b.as_ref()).map(
                    |(book_root_id, book_rel)| {
                        relocate_pointer(&roots, *book_root_id, book_rel, &rel_path)
                    },
                );
                ReceiptPointer {
                    root_display,
                    rel_path,
                    relocation,
                }
            })
            .collect();

    let receipt_absence = if receipts.is_empty() {
        // The opt-out is recorded in the command line itself; beyond that the
        // row can't say whether recording mode or placement suppressed it.
        Some(if decision.command_line.contains("--no-receipt") {
            "no receipt (--no-receipt)".to_string()
        } else {
            "no receipt recorded".to_string()
        })
    } else {
        None
    };

    // Group the decision's placement rows per origin root through the one
    // aggregation helper (a constant aspect — `show` is view-independent, so
    // every row reads the same way), then fold the per-directory shares.
    let raw = repo::decision::fetch_extractions_by_decisions(conn, &[id])?;
    let tagged: Vec<(DecisionExtraction, RowAspect)> = raw
        .iter()
        .cloned()
        .map(|row| (row, RowAspect::Extraction))
        .collect();
    let mut extractions = Vec::new();
    for line in aggregate_placement_lines(&tagged) {
        let mut dirs: Vec<ShowDrewDir> = Vec::new();
        for row in raw.iter().filter(|r| r.root_path == line.row.root_path) {
            match dirs.iter_mut().find(|d| d.dir == row.rel_prefix) {
                Some(d) => {
                    d.files += row.files;
                    d.bytes = match (d.bytes, row.bytes) {
                        (Some(a), Some(b)) => Some(a + b),
                        _ => None,
                    };
                }
                None => dirs.push(ShowDrewDir {
                    dir: row.rel_prefix.clone(),
                    files: row.files,
                    bytes: row.bytes,
                }),
            }
        }
        let root_removed = line
            .row
            .origin_root_removed(roots.iter().map(|r| r.path.as_str()));
        // A removed origin root that was retired points at its book —
        // the origin reads as bound history, not a dead end. DB
        // projections only, same as every query path.
        let retired_book = if root_removed {
            crate::retire::find_retirement_covering_path(conn, &line.row.root_path)?
                .map(|r| r.book_display)
        } else {
            None
        };
        extractions.push(ShowExtraction {
            root_removed,
            retired_book,
            location: line.row.drawn_from(),
            files: line.row.files,
            bytes: line.row.bytes,
            directories: if dirs.len() > 1 { dirs } else { Vec::new() },
        });
    }

    Ok(Some(ShowResult {
        decision,
        receipts,
        receipt_absence,
        extractions,
    }))
}

/// Where a retired root's receipt leads now. The book's location comes from
/// the retirement decision's artifact reference; the filesystem is only
/// consulted for existence — the narrow carve-out to the no-receipt-reads
/// law (stat the book, never read it). The gather copied `.canon-ledger/`
/// verbatim into the book's `ledger/`, subpaths and filenames preserved, so
/// the receipt→gathered-copy mapping is mechanical.
fn relocate_pointer(
    roots: &[Root],
    book_root_id: i64,
    book_rel: &str,
    receipt_rel: &str,
) -> PointerRelocation {
    let Some(book_root) = roots.iter().find(|r| r.id == book_root_id) else {
        // The shelf's own root left the index — no absolute path to observe.
        return PointerRelocation::Unreachable {
            book_path: format!("root #{book_root_id} (removed)/{book_rel}"),
        };
    };
    let book_path = format!("{}/{}", book_root.path, book_rel);
    // This mirrors, by hand, the layout the retirement gather writes
    // (ledger files moved from ".canon-ledger/" into the book's "ledger/");
    // if the gather's destination directory ever changes, this redirect
    // silently stops finding the copy and misreports it as not gathered.
    let gathered_rel = receipt_rel
        .strip_prefix(".canon-ledger/")
        .unwrap_or(receipt_rel);
    let book_ledger_path = format!("{book_path}/ledger/{gathered_rel}");
    if std::path::Path::new(&book_ledger_path).exists() {
        PointerRelocation::Gathered { book_ledger_path }
    } else if std::path::Path::new(&book_path).exists() {
        PointerRelocation::NotGathered { book_path }
    } else {
        PointerRelocation::Unreachable { book_path }
    }
}
