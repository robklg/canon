//! Sweep operations — the finder seat's compute op.
//!
//! `compute_sweep` owns the fetch (new commands compose repo from ops;
//! survey's interface-side fetch is the documented historical exception),
//! applies the sweep's inclusion policy, runs the structural computation,
//! ranks it under the reduction lens, and decorates the shown entries with
//! notes and excluded-context counts. Read-only: one connection, one fetch,
//! snapshot semantics — no transaction.

use std::collections::HashMap;

use anyhow::Result;

use crate::domain::note::Note;
use crate::domain::path::path_is_under;
use crate::domain::source::Source;
use crate::domain::sweep::{
    compute_structural, reduction_lens, LeaderboardEntry, Location, RelationShape, SweepParams,
    SweepStats,
};
use crate::repo::{self, Connection};

/// Default number of leaderboard entries shown; a hub occupies one.
pub const DEFAULT_ENTRY_CAP: usize = 10;

pub struct SweepOptions {
    /// Entry cap override; `None` = the default cap.
    pub limit: Option<usize>,
    /// Reveal everything: no entry cap, below-floor findings included,
    /// hub members uncapped.
    pub all: bool,
}

/// Outcome of compute_sweep: a report, or a degenerate universe with its
/// own honest message — never an error.
pub enum SweepOutcome {
    /// No roots exist — point at scanning.
    NoRoots,
    /// Roots exist but nothing comparison-participating is hashed — point
    /// at the enrichment workflow.
    NoHashedContent,
    Report(SweepReport),
}

pub struct SweepReport {
    /// Ranked leaderboard entries, capped unless `--all`.
    pub entries: Vec<LeaderboardEntry>,
    /// Ranked entries trimmed by the cap (0 under `--all`).
    pub beyond_cap: usize,
    /// Computation honesty counts, including the below-floor subject count
    /// (those findings are inside `entries` only under `--all`).
    pub stats: SweepStats,
    /// Present, non-excluded sources set aside as contentless.
    pub empty_files_ignored: usize,
    /// Excluded present sources under each subject, where substantial
    /// (at least the file emit floor).
    pub excluded_context: HashMap<Location, usize>,
    /// Notes at or under each shown finding's subject or counterpart,
    /// oldest first.
    pub notes: HashMap<Location, Vec<Note>>,
}

/// Compute the universe-wide sweep.
pub fn compute_sweep(conn: &Connection, options: &SweepOptions) -> Result<SweepOutcome> {
    let roots = repo::root::fetch_all(conn)?;
    if roots.is_empty() {
        return Ok(SweepOutcome::NoRoots);
    }
    let root_ids: Vec<i64> = roots.iter().map(|r| r.id).collect();
    let all_sources = repo::source::batch_fetch_by_roots(conn, &root_ids)?;

    // Inclusion policy: present (baked into the fetch), non-excluded,
    // carrying content. Suspended roots stay in — their findings are shown
    // and flagged, never filtered. Exclusion resolves rather than overlaps:
    // excluded sources leave the comparison entirely and return only as
    // per-subject context counts.
    let mut kept: Vec<&Source> = Vec::new();
    let mut excluded: Vec<&Source> = Vec::new();
    let mut empty_files_ignored = 0usize;
    for source in &all_sources {
        if source.is_excluded() {
            excluded.push(source);
        } else if source.is_contentless() {
            // The contentless law: no identity claims about empty content —
            // counted as context, deliberately never filtered silently.
            empty_files_ignored += 1;
        } else {
            kept.push(source);
        }
    }
    if !kept.iter().any(|s| s.object_id.is_some()) {
        return Ok(SweepOutcome::NoHashedContent);
    }

    let params = SweepParams {
        assemble_below_floors: options.all,
        ..SweepParams::default()
    };
    let ranked = reduction_lens(compute_structural(&kept, &roots, &params));

    let mut entries = ranked.entries;
    let beyond_cap = if options.all {
        0
    } else {
        let cap = options.limit.unwrap_or(DEFAULT_ENTRY_CAP);
        let over = entries.len().saturating_sub(cap);
        entries.truncate(cap);
        over
    };

    let all_notes = repo::note::fetch_by_roots(conn, &root_ids)?;
    let mut notes: HashMap<Location, Vec<Note>> = HashMap::new();
    let mut excluded_context: HashMap<Location, usize> = HashMap::new();
    for entry in &entries {
        for loc in note_locations(entry) {
            if notes.contains_key(loc) {
                continue;
            }
            let mut here: Vec<Note> = all_notes
                .iter()
                .filter(|n| n.root_id == loc.root_id && path_is_under(&n.rel_path, &loc.rel_prefix))
                .cloned()
                .collect();
            if here.is_empty() {
                continue;
            }
            here.sort_by_key(|n| (n.created_at, n.id));
            notes.insert(loc.clone(), here);
        }
        for loc in subject_locations(entry) {
            if excluded_context.contains_key(loc) {
                continue;
            }
            let count = excluded
                .iter()
                .filter(|s| s.root_id == loc.root_id && path_is_under(&s.rel_path, &loc.rel_prefix))
                .count();
            if count >= params.emit_floor_files as usize {
                excluded_context.insert(loc.clone(), count);
            }
        }
    }

    Ok(SweepOutcome::Report(SweepReport {
        entries,
        beyond_cap,
        stats: ranked.stats,
        empty_files_ignored,
        excluded_context,
        notes,
    }))
}

/// The locations a note surfaces at: the subject and, for pair relations,
/// the counterpart (hubs: the shared counterpart plus each member subject).
fn note_locations(entry: &LeaderboardEntry) -> Vec<&Location> {
    match entry {
        LeaderboardEntry::Single(f) => {
            let mut locs = vec![&f.subject];
            if let RelationShape::Pair { counterpart, .. } = &f.shape {
                locs.push(counterpart);
            }
            locs
        }
        LeaderboardEntry::Hub(h) => {
            let mut locs = vec![&h.counterpart];
            locs.extend(h.members.iter().map(|m| &m.subject));
            locs
        }
    }
}

/// The subject locations of an entry — where "here" points in the excluded
/// context line.
fn subject_locations(entry: &LeaderboardEntry) -> Vec<&Location> {
    match entry {
        LeaderboardEntry::Single(f) => vec![&f.subject],
        LeaderboardEntry::Hub(h) => h.members.iter().map(|m| &m.subject).collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::test_helpers::{
        insert_object, insert_root, insert_source, insert_source_excluded, insert_source_with_size,
        setup_test_db,
    };

    /// Two roots with one 20 MB duplicated folder (`big` ↔ `q`) and unique
    /// noise keeping the subjects from lifting to the whole root.
    fn seed_basic(conn: &Connection) -> (i64, i64) {
        let r1 = insert_root(conn, "/r1", "source", false);
        let r2 = insert_root(conn, "/r2", "source", false);
        let obj = insert_object(conn, "dup", false);
        insert_source_with_size(conn, r1, "big/f", Some(obj), 20_000_000);
        insert_source_with_size(conn, r2, "q/f", Some(obj), 20_000_000);
        let noise = insert_object(conn, "noise", false);
        insert_source_with_size(conn, r1, "noise/u", Some(noise), 5_000_000);
        (r1, r2)
    }

    fn report(conn: &Connection, options: &SweepOptions) -> SweepReport {
        match compute_sweep(conn, options).unwrap() {
            SweepOutcome::Report(r) => r,
            SweepOutcome::NoRoots => panic!("unexpected NoRoots"),
            SweepOutcome::NoHashedContent => panic!("unexpected NoHashedContent"),
        }
    }

    fn default_options() -> SweepOptions {
        SweepOptions {
            limit: None,
            all: false,
        }
    }

    fn subject_prefixes(report: &SweepReport) -> Vec<(String, String)> {
        report
            .entries
            .iter()
            .flat_map(subject_locations)
            .map(|l| (l.root_path.clone(), l.rel_prefix.clone()))
            .collect()
    }

    #[test]
    fn empty_db_is_no_roots() {
        let conn = setup_test_db();
        assert!(matches!(
            compute_sweep(&conn, &default_options()).unwrap(),
            SweepOutcome::NoRoots
        ));
    }

    #[test]
    fn unhashed_only_is_no_hashed_content() {
        let conn = setup_test_db();
        let r1 = insert_root(&conn, "/r1", "source", false);
        insert_source(&conn, r1, "a/f", None);
        assert!(matches!(
            compute_sweep(&conn, &default_options()).unwrap(),
            SweepOutcome::NoHashedContent
        ));
    }

    #[test]
    fn excluded_hashed_only_is_no_hashed_content() {
        // Excluded content is resolution, not comparison material: a universe
        // where everything hashed is excluded has nothing to sweep.
        let conn = setup_test_db();
        let r1 = insert_root(&conn, "/r1", "source", false);
        let obj = insert_object(&conn, "h1", false);
        insert_source_excluded(&conn, r1, "a/f", Some(obj));
        assert!(matches!(
            compute_sweep(&conn, &default_options()).unwrap(),
            SweepOutcome::NoHashedContent
        ));
    }

    #[test]
    fn basic_duplication_reports_the_subject() {
        let conn = setup_test_db();
        seed_basic(&conn);
        let report = report(&conn, &default_options());
        assert!(subject_prefixes(&report).contains(&("/r1".to_string(), "big".to_string())));
        assert_eq!(report.beyond_cap, 0);
        assert_eq!(report.empty_files_ignored, 0);
    }

    #[test]
    fn zero_byte_sources_are_counted_and_create_no_findings() {
        let conn = setup_test_db();
        let (r1, r2) = seed_basic(&conn);
        let empty = insert_object(&conn, "empty", false);
        insert_source_with_size(&conn, r1, "voids/e", Some(empty), 0);
        insert_source_with_size(&conn, r2, "voids/e", Some(empty), 0);
        let report = report(&conn, &default_options());
        assert_eq!(report.empty_files_ignored, 2);
        assert!(!subject_prefixes(&report)
            .iter()
            .any(|(_, rel)| rel == "voids"));
    }

    #[test]
    fn cap_trims_entries_and_counts_the_trim() {
        let conn = setup_test_db();
        let (r1, r2) = seed_basic(&conn);
        // A second disjoint above-floor finding with its own counterpart.
        let obj = insert_object(&conn, "dup2", false);
        insert_source_with_size(&conn, r1, "big2/f", Some(obj), 15_000_000);
        insert_source_with_size(&conn, r2, "q2/f", Some(obj), 15_000_000);

        let full = report(
            &conn,
            &SweepOptions {
                limit: Some(100),
                all: false,
            },
        );
        let total = full.entries.len();
        assert!(total >= 2);

        let capped = report(
            &conn,
            &SweepOptions {
                limit: Some(1),
                all: false,
            },
        );
        assert_eq!(capped.entries.len(), 1);
        assert_eq!(capped.beyond_cap, total - 1);
    }

    #[test]
    fn all_reveals_below_floor_findings() {
        let conn = setup_test_db();
        let (r1, r2) = seed_basic(&conn);
        let small = insert_object(&conn, "small", false);
        insert_source_with_size(&conn, r1, "small/f", Some(small), 2_000_000);
        insert_source_with_size(&conn, r2, "q2/f", Some(small), 2_000_000);

        let trimmed = report(&conn, &default_options());
        assert_eq!(trimmed.stats.below_floor_subjects, 1);
        assert!(!subject_prefixes(&trimmed).contains(&("/r1".to_string(), "small".to_string())));

        let all = report(
            &conn,
            &SweepOptions {
                limit: None,
                all: true,
            },
        );
        assert!(subject_prefixes(&all).contains(&("/r1".to_string(), "small".to_string())));
        assert_eq!(all.beyond_cap, 0);
    }

    #[test]
    fn excluded_context_surfaces_at_the_file_floor() {
        let conn = setup_test_db();
        let (r1, _) = seed_basic(&conn);
        // 24 excluded sources under the subject: below the bar, no context.
        for i in 0..24 {
            insert_source_excluded(&conn, r1, &format!("big/x{i}"), None);
        }
        let below = report(&conn, &default_options());
        assert!(below.excluded_context.is_empty());

        insert_source_excluded(&conn, r1, "big/x24", None);
        let at_bar = report(&conn, &default_options());
        let big = Location {
            root_id: r1,
            root_path: "/r1".to_string(),
            rel_prefix: "big".to_string(),
        };
        assert_eq!(at_bar.excluded_context.get(&big), Some(&25));
    }

    #[test]
    fn notes_attach_to_subject_and_counterpart() {
        let conn = setup_test_db();
        let (r1, r2) = seed_basic(&conn);
        repo::note::insert(&conn, r1, "big", "check me").unwrap();
        repo::note::insert(&conn, r1, "big/sub", "deeper").unwrap();
        repo::note::insert(&conn, r1, "noise", "elsewhere").unwrap();
        repo::note::insert(&conn, r2, "q", "counterpart side").unwrap();

        let report = report(&conn, &default_options());
        let by_prefix = |root_id: i64, root_path: &str, rel: &str| {
            report.notes.get(&Location {
                root_id,
                root_path: root_path.to_string(),
                rel_prefix: rel.to_string(),
            })
        };
        let subject_notes = by_prefix(r1, "/r1", "big").expect("subject notes");
        assert_eq!(subject_notes.len(), 2);
        assert!(subject_notes.iter().any(|n| n.text == "deeper"));
        // The note on `noise` belongs to no finding location.
        assert!(report
            .notes
            .keys()
            .all(|loc| !(loc.root_id == r1 && loc.rel_prefix == "noise")));
    }

    #[test]
    fn identical_state_yields_identical_reports() {
        let conn = setup_test_db();
        seed_basic(&conn);
        let a = report(&conn, &default_options());
        let b = report(&conn, &default_options());
        assert_eq!(a.entries, b.entries);
        assert_eq!(a.stats, b.stats);
    }
}
