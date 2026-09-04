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

use crate::core::domain::path::path_is_under;
use crate::core::domain::source::Source;
use crate::core::repo::{self, Connection};
use crate::notes::{fetch_by_roots, Note};
use crate::sweep::domain::{
    compute_structural, reduction_lens, LeaderboardEntry, LensParams, Location, PlaceCensus,
    RelationShape, RootNearness, SuspendedRootTally, SweepParams, SweepStats,
};

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
    /// Boxed: the report is the only variant carrying data, and it grew past
    /// the point where every `SweepOutcome` value would pay for it. One
    /// allocation per run, on a command that runs once.
    Report(Box<SweepReport>),
}

pub struct SweepReport {
    /// Ranked leaderboard entries, capped unless `--all`.
    pub entries: Vec<LeaderboardEntry>,
    /// Ranked entries trimmed by the cap (0 under `--all`).
    pub beyond_cap: usize,
    /// Places the lens set aside behind a closed door, per suspended root,
    /// ordered by root path. Carried under `--all` too: `--all` reveals what
    /// the floors and the cap hid, never a door the user closed.
    pub suspended: Vec<SuspendedRootTally>,
    /// Computation honesty counts, including the below-floor subject count
    /// (those findings are inside `entries` only under `--all`).
    pub stats: SweepStats,
    /// Roots whose remainder the board states beside an entry, by root id.
    /// The lens decides where the fact appears; the interface only renders it.
    pub stated_remainders: HashMap<i64, i64>,
    /// Places that mirror each other, by the surviving place's subject: where
    /// two entries stated one overlap from opposite ends the board shows one,
    /// and says on it that the other place mirrors back.
    pub reciprocal_places: HashMap<Location, Location>,
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
    // carrying content. Suspended roots stay in — computed always, ranked
    // never: dropping them from the universe would falsify claims about live
    // places, because a live folder duplicated entirely inside a parked root
    // would then read as unique. Which places earn a slot is a *board*
    // question and belongs to the lens, which sets parked places aside there.
    // Exclusion resolves rather than overlaps: excluded sources leave the
    // comparison entirely and return only as per-subject context counts.
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

    // Root-nearness: the retirement readiness review's own remainder measure,
    // per source root. One extra batch read over object ids already in hand —
    // the sweep must never make a second full pass for a ranking term, and the
    // naive per-root shape (`fetch_root_story` or `build_account` once per
    // root) would issue two queries per root.
    let mut object_ids: Vec<i64> = all_sources.iter().filter_map(|s| s.object_id).collect();
    object_ids.sort_unstable();
    object_ids.dedup();
    let archived = repo::object::batch_check_archived(conn, &object_ids, None)?;
    let nearness = RootNearness::project(&roots, &all_sources, &archived);

    // The sibling-parent axis's coverage figure, projected from the same slice
    // the structural computation is given — numerator and denominator over one
    // population, and no second query: the sweep already holds every present
    // source in the universe.
    let census = PlaceCensus::project(kept.iter().copied());

    let lens_params = LensParams::default();
    let ranked = reduction_lens(
        compute_structural(&kept, &roots, &params),
        &nearness,
        &census,
        &lens_params,
    );

    // The cap runs after the lens, so it trims a board the set-aside has
    // already left: the board refills from below rather than keeping holes.
    let mut entries = ranked.entries;
    let beyond_cap = if options.all {
        0
    } else {
        let cap = options.limit.unwrap_or(DEFAULT_ENTRY_CAP);
        let over = entries.len().saturating_sub(cap);
        entries.truncate(cap);
        over
    };

    let all_notes = fetch_by_roots(conn, &root_ids)?;
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

    Ok(SweepOutcome::Report(Box::new(SweepReport {
        entries,
        beyond_cap,
        suspended: ranked.suspended,
        stated_remainders: ranked.stated_remainders,
        reciprocal_places: ranked.reciprocal_places,
        stats: ranked.stats,
        empty_files_ignored,
        excluded_context,
        notes,
    })))
}

/// The locations a note surfaces at: the subject and, for pair relations,
/// the counterpart (hubs: the shared counterpart plus each member subject).
///
/// A **root entry** names only its root, and a **parent entry** only its
/// parent. The headline is the place the whole entry is about, and the note
/// lookup matches a location's whole subtree, so every note under it already
/// surfaces there — listing the members too would print each of them a second
/// time. One entry about one place speaks once about it.
fn note_locations(entry: &LeaderboardEntry) -> Vec<&Location> {
    match entry {
        LeaderboardEntry::Single(f) => {
            let mut locs = vec![&f.subject];
            if let RelationShape::Pair { counterpart, .. } = &f.shape {
                locs.push(counterpart);
            }
            locs
        }
        LeaderboardEntry::Root(r) => vec![&r.root],
        LeaderboardEntry::Parent(p) => vec![&p.parent],
        LeaderboardEntry::Hub(h) => {
            let mut locs = vec![&h.counterpart];
            locs.extend(h.members.iter().map(|m| &m.subject));
            locs
        }
    }
}

/// The subject locations of an entry — where "here" points in the excluded
/// context line. A root entry points at its root and a parent entry at its
/// parent, for the same reason their notes do: the count under the headline is
/// that place's own, and a per-member count beside it would partition the same
/// number twice over.
fn subject_locations(entry: &LeaderboardEntry) -> Vec<&Location> {
    match entry {
        LeaderboardEntry::Single(f) => vec![&f.subject],
        LeaderboardEntry::Root(r) => vec![&r.root],
        LeaderboardEntry::Parent(p) => vec![&p.parent],
        LeaderboardEntry::Hub(h) => h.members.iter().map(|m| &m.subject).collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::testing::{
        insert_object, insert_root, insert_source, insert_source_excluded, insert_source_with_size,
        setup_test_db,
    };
    use crate::notes::insert;
    use crate::sweep::domain::structural::{compute_structural, FindingNature};
    use crate::sweep::domain::LensParams;

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
            SweepOutcome::Report(r) => *r,
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

    /// Every place a report puts on the board, root-entry members included.
    /// Deliberately not `subject_locations`, which answers a narrower
    /// production question ("where does `here` point?") and names a root or
    /// parent entry by its headline rather than by the places inside it. These fixtures are
    /// small enough that most of their roots read as near-done, so a helper
    /// that could not see inside a root entry would quietly stop seeing most
    /// of the board.
    fn subject_prefixes(report: &SweepReport) -> Vec<(String, String)> {
        report
            .entries
            .iter()
            .flat_map(|entry| match entry {
                LeaderboardEntry::Root(r) => r.members.iter().map(|m| &m.subject).collect(),
                LeaderboardEntry::Parent(p) => p.members.iter().map(|m| &m.subject).collect(),
                other => subject_locations(other),
            })
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
    fn a_suspended_root_stays_in_the_universe_but_leaves_the_board() {
        // The two halves of "computed always, ranked never", driven through
        // compute_sweep's own fetch — which the domain-level suspension tests
        // bypass. Dropping a suspended root from the universe would falsify
        // claims about live places: `/r1/big` exists entirely inside `/r2`,
        // and if `/r2` left the computation it would read as unique. So the
        // residual must still count the parked root's copies — and the place
        // whose evidence sits behind that door must still leave the board.
        //
        // The residual half is the reason this test is here and must not be
        // simplified away: it is the one place the inclusion half of
        // "computed always, ranked never" becomes checkable, and unifying the
        // sweep's inclusion policy
        // with the query surfaces' active-only convention would break it
        // silently everywhere else.
        let conn = setup_test_db();
        let r1 = insert_root(&conn, "/r1", "source", false);
        let r2 = insert_root(&conn, "/r2", "source", true);
        let obj = insert_object(&conn, "dup", false);
        insert_source_with_size(&conn, r1, "big/f", Some(obj), 20_000_000);
        insert_source_with_size(&conn, r2, "q/f", Some(obj), 20_000_000);
        let noise = insert_object(&conn, "noise", false);
        insert_source_with_size(&conn, r1, "noise/u", Some(noise), 5_000_000);

        // Computed: the finding exists, with the parked copy counted as gain
        // rather than residual.
        let structural = {
            let roots = repo::root::fetch_all(&conn).unwrap();
            let sources = repo::source::batch_fetch_by_roots(
                &conn,
                &roots.iter().map(|r| r.id).collect::<Vec<_>>(),
            )
            .unwrap();
            let kept: Vec<&Source> = sources.iter().collect();
            compute_structural(&kept, &roots, &SweepParams::default())
        };
        let big = structural
            .findings
            .iter()
            .find(|f| f.subject.root_path == "/r1" && f.subject.rel_prefix == "big")
            .expect("the place is computed, parked counterpart and all");
        assert_eq!(big.gain_bytes, 20_000_000);
        assert_eq!(big.residual_bytes, 0, "a parked copy is still a copy");
        assert_eq!(big.nature, FindingNature::Verify);

        // Ranked never: it holds no slot, and the board says why.
        let report = report(&conn, &default_options());
        assert!(!subject_prefixes(&report).contains(&("/r1".to_string(), "big".to_string())));
        assert_eq!(report.suspended.len(), 1);
        let tally = &report.suspended[0];
        assert_eq!(tally.root_path, "/r2");
        // One line, both causes: `/r2` is entirely matched, so it stands as a
        // place on the parked root itself, and `/r1/big` is a place whose
        // copies are on it. Each counted once, on the one root.
        assert_eq!(tally.places_on_it, 1);
        assert_eq!(tally.places_with_copies_on_it, 1);
        // And the two masses stay apart. This universe holds 25 MB in total,
        // 20 MB of it the duplicated content at stake — which the two causes
        // see from opposite sides, `/r2` holding the copies that make
        // `/r1/big` redundant. Adding them would publish 40 MB for a 25 MB
        // universe on a board whose currency is recoverable gain.
        assert_eq!(tally.gain_bytes_on_it, 20_000_000);
        assert_eq!(tally.gain_bytes_with_copies_on_it, 20_000_000);
    }

    /// `seed_basic` plus a heavier duplicated folder standing on a suspended
    /// root — the shape that used to fill the board. The extra noise on `/r2`
    /// keeps that root from lifting whole, so the live board is exactly the
    /// `big` finding.
    fn seed_with_a_parked_root(conn: &Connection) -> i64 {
        let (_, r2) = seed_basic(conn);
        let rs = insert_root(conn, "/rs", "source", true);
        let obj = insert_object(conn, "parked-dup", false);
        insert_source_with_size(conn, rs, "heavy/f", Some(obj), 40_000_000);
        insert_source_with_size(conn, r2, "heavy-copy/f", Some(obj), 40_000_000);
        let noise = insert_object(conn, "r2-noise", false);
        insert_source_with_size(conn, r2, "r2-noise/u", Some(noise), 45_000_000);
        rs
    }

    #[test]
    fn the_cap_refills_the_board_from_below_after_set_aside() {
        // The parked place outweighs the live one, so under the old order it
        // took the only slot. The cap runs after the set-aside: the slot goes
        // to the live place, and `beyond_cap` counts only what the cap trimmed.
        let conn = setup_test_db();
        seed_with_a_parked_root(&conn);
        let capped = report(
            &conn,
            &SweepOptions {
                limit: Some(1),
                all: false,
            },
        );
        assert_eq!(capped.entries.len(), 1);
        // The 40 MB parked place outweighs `big` and would have held this
        // slot; the set-aside runs first, so the slot refills from below.
        assert!(subject_prefixes(&capped).contains(&("/r1".to_string(), "big".to_string())));
        assert_eq!(
            capped.beyond_cap, 0,
            "the cap trims a board the set-aside has already left"
        );
        assert_eq!(capped.suspended.len(), 1);
        assert_eq!(capped.suspended[0].root_path, "/rs");
        assert_eq!(capped.suspended[0].places_on_it, 1);
        assert_eq!(capped.suspended[0].places_with_copies_on_it, 1);
    }

    #[test]
    fn all_does_not_reveal_set_aside_or_sunk_places() {
        // `--all` reveals what the floors and the cap hid; suspension is a
        // door the user closed, and the way back is `roots unsuspend` alone.
        let conn = setup_test_db();
        seed_with_a_parked_root(&conn);
        let all = report(
            &conn,
            &SweepOptions {
                limit: None,
                all: true,
            },
        );
        // Neither the place standing on the parked root...
        assert!(!subject_prefixes(&all).iter().any(|(root, _)| root == "/rs"));
        // ...nor the live place whose evidence stands on it.
        assert!(!subject_prefixes(&all).contains(&("/r2".to_string(), "heavy-copy".to_string())));
        assert_eq!(all.suspended.len(), 1);
        assert_eq!(all.suspended[0].root_path, "/rs");
    }

    #[test]
    fn an_all_suspended_universe_states_the_count_rather_than_nothing_found() {
        // The empty-board path already prints the footers, so "every root
        // suspended" states the count and the mass rather than a bare
        // nothing-found — which would be a false "empty".
        let conn = setup_test_db();
        let r1 = insert_root(&conn, "/r1", "source", true);
        let r2 = insert_root(&conn, "/r2", "source", true);
        let obj = insert_object(&conn, "dup", false);
        insert_source_with_size(&conn, r1, "big/f", Some(obj), 20_000_000);
        insert_source_with_size(&conn, r2, "q/f", Some(obj), 20_000_000);
        let noise = insert_object(&conn, "noise", false);
        insert_source_with_size(&conn, r1, "noise/u", Some(noise), 5_000_000);

        let report = report(&conn, &default_options());
        assert!(report.entries.is_empty());
        assert!(!report.suspended.is_empty());
        let places: usize = report
            .suspended
            .iter()
            .map(|t| t.places_on_it + t.places_with_copies_on_it)
            .sum();
        assert!(places > 0);
    }

    #[test]
    fn below_floor_minus_set_aside_equals_what_rendered_under_all() {
        // The arithmetic the reader is asked to do on one screen: the
        // below-floor count includes places behind a closed door, so `--all`
        // reveals fewer than that count offers, and the suspended lines are
        // what explain the difference. Every finding here is below the floors.
        let conn = setup_test_db();
        let live = insert_root(&conn, "/r1", "source", false);
        let other = insert_root(&conn, "/r2", "source", false);
        let parked = insert_root(&conn, "/rs", "source", true);
        let a = insert_object(&conn, "small-a", false);
        insert_source_with_size(&conn, live, "small/f", Some(a), 2_000_000);
        insert_source_with_size(&conn, other, "copy-a/f", Some(a), 2_000_000);
        let b = insert_object(&conn, "small-b", false);
        insert_source_with_size(&conn, parked, "small/f", Some(b), 2_000_000);
        insert_source_with_size(&conn, other, "copy-b/f", Some(b), 2_000_000);

        let all = report(
            &conn,
            &SweepOptions {
                limit: None,
                all: true,
            },
        );
        let set_aside: usize = all
            .suspended
            .iter()
            .map(|t| t.places_on_it + t.places_with_copies_on_it)
            .sum();
        assert!(
            set_aside > 0,
            "the fixture must set at least one place aside"
        );
        assert!(
            !all.entries.is_empty(),
            "the fixture must also render something, or the arithmetic is vacuous"
        );
        assert_eq!(
            all.stats.below_floor_subjects - set_aside,
            all.entries.len()
        );
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
        insert(&conn, r1, "big", "check me").unwrap();
        insert(&conn, r1, "big/sub", "deeper").unwrap();
        insert(&conn, r1, "noise", "elsewhere").unwrap();
        insert(&conn, r2, "q", "counterpart side").unwrap();

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
    fn the_coverage_denominator_counts_only_comparison_participating_rows() {
        // The judgment this pins end to end: a parent entry's coverage is
        // measured over the **kept** slice — present, non-excluded,
        // non-contentless — and not over every present source.
        //
        // It is a flip, not a rounding difference. Under `photos` sit 70 kept
        // rows, of which 60 lie under the two members, plus 100 excluded rows
        // and 50 empty ones. Over the kept slice the run accounts for 60/70 =
        // 86% and the entry forms; over every present row it would read
        // 60/220 = 27%, fall under the gate, and no entry would exist at all.
        // So swapping the population at the one projection site in this file
        // fails here rather than silently changing what the board shows.
        //
        // Excluded content is resolution rather than overlap and puts nothing
        // further at stake under the parent; an empty source travels with its
        // place and is resolved with the place's own fate. Neither belongs in
        // a figure about how much of the parent this run accounts for.
        let conn = setup_test_db();
        let r1 = insert_root(&conn, "/r1", "source", false);
        let r2 = insert_root(&conn, "/r2", "source", false);
        for i in 0..30 {
            let a = insert_object(&conn, &format!("a{i}"), false);
            insert_source_with_size(&conn, r1, &format!("photos/a/f{i}"), Some(a), 1_000_000);
            insert_source_with_size(&conn, r2, &format!("one/f{i}"), Some(a), 1_000_000);
            let b = insert_object(&conn, &format!("b{i}"), false);
            insert_source_with_size(&conn, r1, &format!("photos/b/f{i}"), Some(b), 1_000_000);
            insert_source_with_size(&conn, r2, &format!("two/f{i}"), Some(b), 1_000_000);
        }
        // Unmatched content directly under `photos`, so the descent reaches
        // the two children rather than stopping at the parent.
        for i in 0..10 {
            let u = insert_object(&conn, &format!("u{i}"), false);
            insert_source_with_size(&conn, r1, &format!("photos/misc/u{i}"), Some(u), 1_000_000);
        }
        // The two populations part company here: excluded and empty rows sit
        // under the parent and under no member.
        for i in 0..100 {
            insert_source_excluded(&conn, r1, &format!("photos/junk/e{i}"), None);
        }
        let empty = insert_object(&conn, "empty", false);
        for i in 0..50 {
            insert_source_with_size(&conn, r1, &format!("photos/void/z{i}"), Some(empty), 0);
        }
        // Noise keeping `/r2` from lifting whole.
        for i in 0..10 {
            let n = insert_object(&conn, &format!("n{i}"), false);
            insert_source_with_size(&conn, r2, &format!("noise/n{i}"), Some(n), 5_000_000);
        }

        let report = report(&conn, &default_options());
        let entry = report
            .entries
            .iter()
            .find_map(|e| match e {
                LeaderboardEntry::Parent(p) if p.parent.rel_prefix == "photos" => Some(p),
                _ => None,
            })
            .expect("the run forms — over the kept slice it clears the gate");
        assert_eq!(entry.members.len(), 2);
        assert!(
            (entry.coverage - 60.0 / 70.0).abs() < 1e-9,
            "{}",
            entry.coverage
        );
        assert!(
            entry.coverage > LensParams::default().sibling_parent_coverage,
            "and the other population would put it under the gate"
        );
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
