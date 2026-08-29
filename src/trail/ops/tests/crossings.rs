use crate::core::domain::extraction::{DecisionExtraction, OriginDisposition};
use crate::core::repo;
use crate::core::repo::db::open_in_memory_for_test;
use crate::core::repo::insert_test_root;
use crate::core::repo::source::insert_test_source;
use crate::core::repo::Connection;
use crate::trail::domain::placement::RowAspect;
use crate::trail::ops::compute::compute_trail;
use crate::trail::ops::crossings::{
    compute_crossings, CrossingBody, Crossings, CrossingsParams, CrossingsResult, NothingCrossed,
};

use super::fixtures::{extraction_row, insert_decision_at, params};

fn crossings_params(prefixes: Vec<&str>) -> CrossingsParams {
    CrossingsParams {
        prefixes: prefixes.into_iter().map(String::from).collect(),
        origin: None,
        destination: None,
        limit: None,
        machine_output: false,
    }
}

fn reported(conn: &Connection, params: &CrossingsParams) -> CrossingsResult {
    match compute_crossings(conn, params).unwrap() {
        Crossings::Reported(result) => *result,
        Crossings::UnknownCounterpart(paths) => {
            panic!("expected a report, got unknown counterpart {paths:?}")
        }
    }
}

fn section(
    result: &CrossingsResult,
    aspect: RowAspect,
) -> &crate::trail::ops::crossings::CrossingSection {
    result
        .sections
        .iter()
        .find(|s| s.aspect == aspect)
        .unwrap_or_else(|| panic!("no {aspect:?} section"))
}

fn write_rows(conn: &Connection, rows: &[DecisionExtraction]) {
    // Grouped per decision: the writer replaces a decision's rows wholesale,
    // so one call per decision is the production shape.
    let mut ids: Vec<i64> = rows.iter().map(|r| r.decision_id).collect();
    ids.sort_unstable();
    ids.dedup();
    for id in ids {
        let own: Vec<DecisionExtraction> = rows
            .iter()
            .filter(|r| r.decision_id == id)
            .cloned()
            .collect();
        repo::decision::replace_extractions(conn, &own).unwrap();
    }
}

/// Two source drives feeding one archive, plus one intra-archive
/// rearrangement — the shape the bare view is read against.
fn archive_with_two_origins(conn: &Connection) -> (i64, i64, i64) {
    let sd = insert_test_root(conn, "/Volumes/sd", "source", false);
    let cf = insert_test_root(conn, "/Volumes/cf", "source", false);
    let archive = insert_test_root(conn, "/archive", "archive", false);

    let first = insert_decision_at(conn, "apply", 100);
    let second = insert_decision_at(conn, "apply", 200);
    let third = insert_decision_at(conn, "apply", 300);
    let internal = insert_decision_at(conn, "apply", 400);

    write_rows(
        conn,
        &[
            extraction_row(
                first,
                sd,
                "/Volumes/sd",
                "2016",
                10,
                Some(100),
                "/archive/Media/2016",
            ),
            extraction_row(
                second,
                sd,
                "/Volumes/sd",
                "2017",
                5,
                Some(50),
                "/archive/Media/2016",
            ),
            extraction_row(
                third,
                cf,
                "/Volumes/cf",
                "raw",
                3,
                Some(30),
                "/archive/Documents",
            ),
            extraction_row(
                internal,
                archive,
                "/archive",
                "Media/2016",
                2,
                Some(20),
                "/archive/Media/2016-italy",
            ),
        ],
    );
    (sd, cf, archive)
}

// ----------------------------------------------------------------------
// The coherence property: the bare view is the rollups, itemized
// ----------------------------------------------------------------------

/// The door is those rollup lines made expandable, so this is a structural
/// property rather than a claim: same rows, same counterparty keys, therefore
/// equal totals and equal cardinality.
///
/// Pinned rather than assumed because the two computations establish origin
/// membership by **different evidence** — the rollups by root id, this door by
/// the write-time snapshot path — which `row_aspect`'s own contract sanctions
/// and which is exactly why it is checked.
#[test]
fn the_bare_view_itemizes_exactly_what_the_rollups_count() {
    let conn = open_in_memory_for_test();
    archive_with_two_origins(&conn);

    let trail = compute_trail(&conn, &params(vec!["/archive".to_string()])).unwrap();
    let result = reported(&conn, &crossings_params(vec!["/archive"]));

    let arrivals = section(&result, RowAspect::Arrival);
    let rollup = trail.arrival_rollup.as_ref().unwrap();
    assert_eq!(arrivals.files, rollup.files);
    assert_eq!(arrivals.bytes, rollup.bytes);
    assert_eq!(arrivals.counterparty_count, rollup.origins);
    match &arrivals.body {
        CrossingBody::Counterparts { lines, more } => {
            assert_eq!(lines.len(), rollup.origins);
            assert_eq!(*more, 0);
            assert_eq!(lines.iter().map(|l| l.files).sum::<i64>(), rollup.files);
        }
        CrossingBody::Deliveries { .. } => panic!("a bare view lists counterparts"),
    }

    // The outbound direction, from the drive's side, against the same run's
    // extraction rollup.
    let from_drive = compute_trail(&conn, &params(vec!["/Volumes/sd".to_string()])).unwrap();
    let out = reported(&conn, &crossings_params(vec!["/Volumes/sd"]));
    let out_section = section(&out, RowAspect::Extraction);
    let out_rollup = from_drive.extraction_rollup.as_ref().unwrap();
    assert_eq!(out_section.files, out_rollup.files);
    assert_eq!(out_section.bytes, out_rollup.bytes);
    assert_eq!(out_section.counterparty_count, out_rollup.destinations);
}

/// The same property where the outbound side actually coarsens — the case
/// that would break if the rollup and the door ever derived the grain
/// separately instead of sharing one derivation.
///
/// The failure this pins is not arithmetic drift but a reading one: a rollup
/// saying `→ 19 destinations` standing one line above a door listing three of
/// them, with the teaching hint between them inviting the comparison.
#[test]
fn the_grouped_outbound_view_itemizes_exactly_what_the_rollup_counts() {
    let conn = open_in_memory_for_test();
    a_drive_delivering_into_generated_folders(&conn);

    let trail = compute_trail(&conn, &params(vec!["/Volumes/sd".to_string()])).unwrap();
    let result = reported(&conn, &crossings_params(vec!["/Volumes/sd"]));
    let outbound = section(&result, RowAspect::Extraction);
    let rollup = trail.extraction_rollup.as_ref().unwrap();

    assert_eq!(rollup.destinations, 3, "the rollup counts grouped places");
    assert_eq!(outbound.counterparty_count, rollup.destinations);
    assert_eq!(outbound.files, rollup.files);
    assert_eq!(outbound.bytes, rollup.bytes);

    let entries = counterpart_entries(&result, RowAspect::Extraction);
    assert_eq!(entries.len(), rollup.destinations);
    // Partition: the entries sum to the section total, and the folders they
    // state sum to the ledger leaves underneath.
    assert_eq!(entries.iter().map(|l| l.files).sum::<i64>(), rollup.files);
    assert_eq!(entries.iter().map(|l| l.folders).sum::<usize>(), 19);
}

/// One drive delivering into a pattern-generated archive: twelve day folders
/// under one month, six under another, and one folder a person named. At
/// ledger precision that is nineteen destinations, none of them a place
/// anyone would name.
fn a_drive_delivering_into_generated_folders(conn: &Connection) -> (i64, Vec<i64>) {
    let sd = insert_test_root(conn, "/Volumes/sd", "source", false);
    insert_test_root(conn, "/archive", "archive", false);

    let mut destinations: Vec<String> = (1..=12)
        .map(|d| format!("/archive/Media/2016/03/{d:02}"))
        .collect();
    destinations.extend((1..=6).map(|d| format!("/archive/Media/2016/04/{d:02}")));
    destinations.push("/archive/Media/2016/a-named-folder".to_string());

    // Two applies, so a grouped entry can carry a plural decision count and
    // the lone named folder can carry a singleton one.
    let first = insert_decision_at(conn, "apply", 100);
    let second = insert_decision_at(conn, "apply", 200);
    let rows: Vec<DecisionExtraction> = destinations
        .iter()
        .enumerate()
        .map(|(i, destination)| {
            let decision = if destination.contains("a-named-folder") || i % 2 == 0 {
                first
            } else {
                second
            };
            extraction_row(
                decision,
                sd,
                "/Volumes/sd",
                &format!("dir{i:02}"),
                (i as i64) + 1,
                Some(((i as i64) + 1) * 10),
                destination,
            )
        })
        .collect();
    write_rows(conn, &rows);
    (sd, vec![first, second])
}

fn counterpart_paths(result: &CrossingsResult, aspect: RowAspect) -> Vec<String> {
    match &section(result, aspect).body {
        CrossingBody::Counterparts { lines, .. } => {
            lines.iter().map(|l| l.counterpart.path.clone()).collect()
        }
        CrossingBody::Deliveries { .. } => panic!("expected counterparts"),
    }
}

fn counterpart_entries(
    result: &CrossingsResult,
    aspect: RowAspect,
) -> &[crate::trail::ops::crossings::CounterpartLine] {
    match &section(result, aspect).body {
        CrossingBody::Counterparts { lines, .. } => lines,
        CrossingBody::Deliveries { .. } => panic!("expected counterparts"),
    }
}

/// The outbound listing names months and the folder a person named — never
/// the nineteen generated day folders underneath them.
#[test]
fn the_outbound_view_groups_generated_folders_into_places_a_person_would_name() {
    let conn = open_in_memory_for_test();
    a_drive_delivering_into_generated_folders(&conn);

    let result = reported(&conn, &crossings_params(vec!["/Volumes/sd"]));
    let mut paths = counterpart_paths(&result, RowAspect::Extraction);
    paths.sort();
    assert_eq!(
        paths,
        vec![
            "/archive/Media/2016/03",
            "/archive/Media/2016/04",
            "/archive/Media/2016/a-named-folder",
        ]
    );

    // The coarsening is visible: a grouped entry says how many finer places
    // it stands for, and a leaf entry says nothing because it is one.
    let mut covered: Vec<(String, usize)> = counterpart_entries(&result, RowAspect::Extraction)
        .iter()
        .map(|l| (l.counterpart.path.clone(), l.folders))
        .collect();
    covered.sort();
    assert_eq!(
        covered,
        vec![
            ("/archive/Media/2016/03".to_string(), 12),
            ("/archive/Media/2016/04".to_string(), 6),
            ("/archive/Media/2016/a-named-folder".to_string(), 1),
        ]
    );
}

/// The reach the coarsening must not cost: naming a grouped entry drops into
/// it at the ledger's own grain, with the decisions behind it.
#[test]
fn a_destination_flag_narrows_into_a_grouped_entry() {
    let conn = open_in_memory_for_test();
    a_drive_delivering_into_generated_folders(&conn);

    let mut p = crossings_params(vec!["/Volumes/sd"]);
    p.destination = Some("/archive/Media/2016/03".to_string());
    let result = reported(&conn, &p);
    let narrowed = section(&result, RowAspect::Extraction);
    assert!(narrowed.named.is_some(), "the entry was named, so it opens");

    match &narrowed.body {
        CrossingBody::Deliveries { lines, .. } => {
            let places: Vec<&str> = lines
                .iter()
                .flat_map(|l| l.places.iter().map(|p| p.destination.as_str()))
                .collect();
            assert_eq!(places.len(), 12, "all twelve leaves are reachable");
            assert!(places.contains(&"/archive/Media/2016/03/07"));
        }
        CrossingBody::Counterparts { .. } => panic!("a named destination lists deliveries"),
    }
}

/// The boundary-borrowing rule's own pin, extended past the row set to the
/// display grain: standing at the drive and naming it from nowhere are one
/// computation, so they must not group two ways.
#[test]
fn global_origin_groups_identically_to_standing_at_the_drive() {
    let conn = open_in_memory_for_test();
    a_drive_delivering_into_generated_folders(&conn);

    let standing = reported(&conn, &crossings_params(vec!["/Volumes/sd"]));
    let mut global = crossings_params(vec![]);
    global.origin = Some("/Volumes/sd".to_string());
    let borrowed = reported(&conn, &global);

    assert_eq!(
        counterpart_paths(&standing, RowAspect::Extraction),
        counterpart_paths(&borrowed, RowAspect::Extraction)
    );
    assert_eq!(
        section(&standing, RowAspect::Extraction).counterparty_count,
        section(&borrowed, RowAspect::Extraction).counterparty_count
    );
}

/// The cap sizes the listing of **entries**, which after grouping are the
/// places — so a remainder now names a quantity the reader can act on.
#[test]
fn the_cap_applies_to_grouped_entries() {
    let conn = open_in_memory_for_test();
    a_drive_delivering_into_generated_folders(&conn);

    let mut p = crossings_params(vec!["/Volumes/sd"]);
    p.limit = Some(2);
    let result = reported(&conn, &p);
    match &section(&result, RowAspect::Extraction).body {
        CrossingBody::Counterparts { lines, more } => {
            assert_eq!(lines.len(), 2);
            // Two of three grouped entries, not two of nineteen leaves.
            assert_eq!(*more, 1);
        }
        CrossingBody::Deliveries { .. } => panic!("expected counterparts"),
    }
}

/// A fully-determined answer is a handle, not a statistic: the entry drawn by
/// one decision carries that decision's real id, and it is the id `trail
/// show` takes.
#[test]
fn a_singleton_decision_count_names_its_id() {
    let conn = open_in_memory_for_test();
    let (_, applies) = a_drive_delivering_into_generated_folders(&conn);

    let result = reported(&conn, &crossings_params(vec!["/Volumes/sd"]));
    let named_folder = counterpart_entries(&result, RowAspect::Extraction)
        .iter()
        .find(|l| l.counterpart.path.ends_with("a-named-folder"))
        .expect("the named folder is its own entry");
    assert_eq!(named_folder.decision_ids, vec![applies[0]]);
    // The date of a single decision is one date, never a degenerate range.
    assert_eq!(named_folder.first_at, named_folder.last_at);
}

#[test]
fn a_plural_count_stays_anonymous() {
    let conn = open_in_memory_for_test();
    let (_, applies) = a_drive_delivering_into_generated_folders(&conn);

    let result = reported(&conn, &crossings_params(vec!["/Volumes/sd"]));
    let month = counterpart_entries(&result, RowAspect::Extraction)
        .iter()
        .find(|l| l.counterpart.path.ends_with("/03"))
        .expect("the month is a grouped entry");
    let mut ids = month.decision_ids.clone();
    ids.sort_unstable();
    assert_eq!(ids, applies, "both applies fed this month");
    assert!(month.first_at < month.last_at, "a real span, not one date");
}

/// A rearrangement is in neither crossing section — and at an archive root
/// that is the section the intra-archive apply lands in.
#[test]
fn an_intra_view_apply_appears_in_no_crossing_section() {
    let conn = open_in_memory_for_test();
    archive_with_two_origins(&conn);
    let result = reported(&conn, &crossings_params(vec!["/archive"]));

    // The internal apply drew from /archive/Media/2016 into
    // /archive/Media/2016-italy: both ends inside, so it appears nowhere.
    for s in &result.sections {
        if let CrossingBody::Counterparts { lines, .. } = &s.body {
            for line in lines {
                assert_ne!(line.counterpart.path, "/archive/Media/2016-italy");
            }
        }
    }
}

// ----------------------------------------------------------------------
// The counterpart evidence gate
// ----------------------------------------------------------------------

/// The arm the scope gate does not have. `history_evidence_at` opens on
/// `find_containing_root` and returns false for a path under no live root, so
/// without the ledger arm a plain-`rm`'d origin root would be reported
/// unknown — and this door would refuse to open on a line the composition
/// card printed a second earlier, which is this feature's own motivating
/// defect reproduced inside the fix.
#[test]
fn a_plain_removed_origin_root_still_opens_its_door() {
    let conn = open_in_memory_for_test();
    let archive = insert_test_root(&conn, "/archive", "archive", false);
    let apply = insert_decision_at(&conn, "apply", 100);
    write_rows(
        &conn,
        &[extraction_row(
            apply,
            999,
            "/Volumes/gone",
            "",
            7,
            Some(70),
            "/archive/Media",
        )],
    );
    let _ = archive;

    let mut p = crossings_params(vec!["/archive"]);
    p.origin = Some("/Volumes/gone".to_string());
    let result = reported(&conn, &p);
    let arrivals = section(&result, RowAspect::Arrival);
    assert_eq!(arrivals.files, 7);
    assert!(arrivals.named.as_ref().unwrap().root_removed);
}

#[test]
fn a_retired_counterpart_is_known_and_carries_its_book() {
    let conn = open_in_memory_for_test();
    let archive = insert_test_root(&conn, "/archive", "archive", false);
    let apply = insert_decision_at(&conn, "apply", 100);
    write_rows(
        &conn,
        &[extraction_row(
            apply,
            999,
            "/Volumes/gone",
            "",
            7,
            Some(70),
            "/archive/Media",
        )],
    );
    let retire = insert_decision_at(&conn, "roots_retire", 200);
    conn.execute(
        "UPDATE decisions SET receipt_root_id = ?2, receipt_rel_path = 'books/gone' WHERE id = ?1",
        rusqlite::params![retire, archive],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO decision_scopes (decision_id, root_id, root_path, rel_prefix)
         VALUES (?1, 999, '/Volumes/gone', '')",
        rusqlite::params![retire],
    )
    .unwrap();

    let mut p = crossings_params(vec!["/archive"]);
    p.origin = Some("/Volumes/gone".to_string());
    let named = section(&reported(&conn, &p), RowAspect::Arrival)
        .named
        .as_ref()
        .map(|c| c.retired_book.clone())
        .unwrap();
    assert_eq!(named.as_deref(), Some("/archive/books/gone"));
}

/// A place Canon has no record of is stated as unknown, never rendered as a
/// plausible empty view of somewhere it has never been.
#[test]
fn an_unknown_counterpart_is_not_a_silently_empty_view() {
    let conn = open_in_memory_for_test();
    archive_with_two_origins(&conn);

    let mut p = crossings_params(vec!["/archive"]);
    p.origin = Some("/Volumes/never-existed".to_string());
    match compute_crossings(&conn, &p).unwrap() {
        Crossings::UnknownCounterpart(paths) => {
            assert_eq!(paths, vec!["/Volumes/never-existed".to_string()])
        }
        Crossings::Reported(_) => panic!("an unknown counterpart must be stated, not rendered"),
    }
}

// ----------------------------------------------------------------------
// The named view: row grain, never a collapsed prefix
// ----------------------------------------------------------------------

/// The timeline collapses a decision's rows to their common prefix. Here that
/// would be fatal: an apply drawing from two distinct directories would
/// collapse to the root, which is the exact aboutness the reader reached for
/// when they named the counterpart.
#[test]
fn a_named_counterpart_renders_row_grain_not_a_collapsed_prefix() {
    let conn = open_in_memory_for_test();
    let sd = insert_test_root(&conn, "/Volumes/sd", "source", false);
    insert_test_root(&conn, "/archive", "archive", false);
    let apply = insert_decision_at(&conn, "apply", 100);
    write_rows(
        &conn,
        &[
            extraction_row(
                apply,
                sd,
                "/Volumes/sd",
                "Photos/2016",
                4,
                Some(40),
                "/archive/Media/2016",
            ),
            extraction_row(
                apply,
                sd,
                "/Volumes/sd",
                "Video/raw",
                2,
                Some(20),
                "/archive/Media/video",
            ),
        ],
    );

    let mut p = crossings_params(vec!["/archive"]);
    p.origin = Some("/Volumes/sd".to_string());
    let result = reported(&conn, &p);
    match &section(&result, RowAspect::Arrival).body {
        CrossingBody::Deliveries { lines, .. } => {
            assert_eq!(lines.len(), 1, "one decision, one delivery");
            let places: Vec<&str> = lines[0].places.iter().map(|p| p.origin.as_str()).collect();
            assert_eq!(
                places,
                vec!["/Volumes/sd/Photos/2016", "/Volumes/sd/Video/raw"]
            );
            // Never the common prefix, which is the drive itself.
            assert!(!places.contains(&"/Volumes/sd"));
        }
        CrossingBody::Counterparts { .. } => panic!("a named counterpart lists deliveries"),
    }
}

#[test]
fn a_decision_with_no_recorded_reason_carries_none() {
    let conn = open_in_memory_for_test();
    let sd = insert_test_root(&conn, "/Volumes/sd", "source", false);
    insert_test_root(&conn, "/archive", "archive", false);
    let with_reason = insert_decision_at(&conn, "apply", 100);
    let without = insert_decision_at(&conn, "apply", 200);
    conn.execute(
        "UPDATE decisions SET reason = 'italy trip' WHERE id = ?1",
        [with_reason],
    )
    .unwrap();
    write_rows(
        &conn,
        &[
            extraction_row(
                with_reason,
                sd,
                "/Volumes/sd",
                "a",
                1,
                Some(10),
                "/archive/a",
            ),
            extraction_row(without, sd, "/Volumes/sd", "b", 1, Some(10), "/archive/b"),
        ],
    );

    let mut p = crossings_params(vec!["/archive"]);
    p.origin = Some("/Volumes/sd".to_string());
    let result = reported(&conn, &p);
    match &section(&result, RowAspect::Arrival).body {
        CrossingBody::Deliveries { lines, .. } => {
            assert_eq!(lines[0].reason.as_deref(), Some("italy trip"));
            assert_eq!(lines[1].reason, None);
        }
        CrossingBody::Counterparts { .. } => panic!("expected deliveries"),
    }
}

/// One unknown-size row omits its own section's total and never suppresses
/// the sibling section's known one — the all-or-omitted rule evaluated per
/// section over its own rows, exactly as the three rollups evaluate it.
#[test]
fn bytes_are_all_or_omitted_per_section() {
    let conn = open_in_memory_for_test();
    let sd = insert_test_root(&conn, "/Volumes/sd", "source", false);
    let archive = insert_test_root(&conn, "/archive", "archive", false);
    let inbound = insert_decision_at(&conn, "apply", 100);
    let outbound = insert_decision_at(&conn, "apply", 200);
    write_rows(
        &conn,
        &[
            // Arrives at /archive with no recorded size.
            extraction_row(inbound, sd, "/Volumes/sd", "a", 1, None, "/archive/a"),
            // Leaves /archive with one.
            extraction_row(
                outbound,
                archive,
                "/archive",
                "b",
                2,
                Some(20),
                "/elsewhere",
            ),
        ],
    );

    let result = reported(&conn, &crossings_params(vec!["/archive"]));
    assert_eq!(section(&result, RowAspect::Arrival).bytes, None);
    assert_eq!(section(&result, RowAspect::Extraction).bytes, Some(20));
}

// ----------------------------------------------------------------------
// The reconciliation line
// ----------------------------------------------------------------------

/// An archive holding one drive's delivery, of which one file has since been
/// deleted — the shape the reconciliation line reads.
/// `rel_prefix` is the directory within the drive the row was drawn from.
/// It is a parameter because the sub-root gate can only be reached when the
/// row sits *below* the root: with an empty prefix, naming a sub-path matches
/// no row at all and the evidence gate answers first, so the gate under test
/// is never consulted.
fn archive_with_a_partly_surviving_delivery(conn: &Connection, rel_prefix: &str) -> i64 {
    let sd = insert_test_root(conn, "/Volumes/sd", "source", false);
    let archive = insert_test_root(conn, "/archive", "archive", false);
    let apply = insert_decision_at(conn, "apply", 100);
    write_rows(
        conn,
        &[extraction_row(
            apply,
            sd,
            "/Volumes/sd",
            rel_prefix,
            2,
            Some(20),
            "/archive",
        )],
    );
    let kept = insert_test_source(conn, archive, "x.jpg", 1, 1, 10, 0);
    let gone = insert_test_source(conn, archive, "y.jpg", 1, 2, 10, 0);
    for (id, present) in [(kept, 1), (gone, 0)] {
        conn.execute(
            "UPDATE sources SET decision_id = ?, present = ? WHERE id = ?",
            rusqlite::params![apply, present, id],
        )
        .unwrap();
    }
    apply
}

/// The standing count is the card's own, not a second count of the same
/// thing: one meaning, one spelling.
#[test]
fn the_reconciliation_line_comes_from_the_card_not_a_re_derivation() {
    let conn = open_in_memory_for_test();
    archive_with_a_partly_surviving_delivery(&conn, "");

    let mut p = crossings_params(vec!["/archive"]);
    p.origin = Some("/Volumes/sd".to_string());
    let result = reported(&conn, &p);
    let line = result.reconciliation.as_ref().unwrap();
    assert_eq!(line.delivered, 2);
    assert_eq!(line.standing, 1);

    let card =
        crate::trail::ops::composition::compute_composition(&conn, &["/archive".to_string()])
            .unwrap()
            .unwrap();
    let card_files = match &card.origins[0] {
        crate::trail::domain::composition::OriginLine::FromRoot { files, .. } => *files,
        crate::trail::domain::composition::OriginLine::MultiOrigin { .. } => panic!("FromRoot"),
    };
    assert_eq!(line.standing, card_files);
}

/// The card attributes at root level by design, so a sub-root origin has no
/// card number at that grain — and inventing one is what this line exists to
/// avoid.
#[test]
fn the_reconciliation_line_is_absent_at_a_sub_root_origin() {
    let conn = open_in_memory_for_test();
    // The row sits *below* the drive, so naming the sub-path matches it and
    // the gate under test is actually reached. With the row at the root, the
    // sub-path matches nothing, the evidence gate answers "unknown" first,
    // and `reconcile` is never consulted — the test would then pass without
    // exercising the rule it is named for.
    archive_with_a_partly_surviving_delivery(&conn, "photos");

    let mut p = crossings_params(vec!["/archive"]);
    p.origin = Some("/Volumes/sd".to_string());
    assert!(reported(&conn, &p).reconciliation.is_some());

    // The same drive, named one level deeper. The rows still match (the door
    // is descendant-or-equal), so the section exists and `delivered` is
    // known — but the card attributes at root level, so nothing is keyed at
    // this grain and the line is absent rather than invented.
    let mut deeper = crossings_params(vec!["/archive"]);
    deeper.origin = Some("/Volumes/sd/photos".to_string());
    let result = reported(&conn, &deeper);
    assert_eq!(
        section(&result, RowAspect::Arrival).files,
        2,
        "the section must exist, or reconcile was never reached and this \
         test proves nothing about the card grain"
    );
    assert!(result.reconciliation.is_none());
}

#[test]
fn the_reconciliation_line_is_absent_for_a_multi_origin_card_line() {
    let conn = open_in_memory_for_test();
    let sd = insert_test_root(&conn, "/Volumes/sd", "source", false);
    let cf = insert_test_root(&conn, "/Volumes/cf", "source", false);
    let archive = insert_test_root(&conn, "/archive", "archive", false);
    // One apply drawing from two roots: the card cannot attribute which
    // surviving files came from which side, so it renders a MultiOrigin line.
    let apply = insert_decision_at(&conn, "apply", 100);
    write_rows(
        &conn,
        &[
            extraction_row(apply, sd, "/Volumes/sd", "", 1, Some(10), "/archive"),
            extraction_row(apply, cf, "/Volumes/cf", "", 1, Some(10), "/archive"),
        ],
    );
    let s = insert_test_source(&conn, archive, "x.jpg", 1, 1, 10, 0);
    conn.execute(
        "UPDATE sources SET decision_id = ?, present = 1 WHERE id = ?",
        rusqlite::params![apply, s],
    )
    .unwrap();

    let mut p = crossings_params(vec!["/archive"]);
    p.origin = Some("/Volumes/sd".to_string());
    assert!(reported(&conn, &p).reconciliation.is_none());
}

#[test]
fn the_reconciliation_line_is_absent_for_a_global_view() {
    let conn = open_in_memory_for_test();
    archive_with_a_partly_surviving_delivery(&conn, "");

    let mut p = crossings_params(vec![]);
    p.origin = Some("/Volumes/sd".to_string());
    assert!(reported(&conn, &p).reconciliation.is_none());
}

/// `--destination` narrows the delivered count and narrows nothing about the
/// card, which answers for the whole view. Comparing them can state that more
/// files stand here than were ever delivered — impossible, and the one thing
/// this line must never do.
#[test]
fn the_reconciliation_line_is_absent_when_a_destination_narrows_the_delivery() {
    let conn = open_in_memory_for_test();
    let apply = archive_with_a_partly_surviving_delivery(&conn, "");
    // A second delivery from the same origin into a different folder, so a
    // destination-narrowed section really is smaller than the card's count.
    let sd = repo::root::fetch_all(&conn)
        .unwrap()
        .into_iter()
        .find(|r| r.path == "/Volumes/sd")
        .unwrap()
        .id;
    write_rows(
        &conn,
        &[
            extraction_row(apply, sd, "/Volumes/sd", "", 2, Some(20), "/archive"),
            extraction_row(apply, sd, "/Volumes/sd", "b", 1, Some(10), "/archive/Other"),
        ],
    );

    let mut p = crossings_params(vec!["/archive"]);
    p.origin = Some("/Volumes/sd".to_string());
    // Unnarrowed, the two counts range over the same content and compare.
    assert!(reported(&conn, &p).reconciliation.is_some());

    // Narrowed, they do not — so the line is absent rather than impossible.
    p.destination = Some("/archive/Other".to_string());
    let narrowed = reported(&conn, &p);
    let delivered = section(&narrowed, RowAspect::Arrival).files;
    assert_eq!(delivered, 1, "the section really is narrowed");
    assert!(
        narrowed.reconciliation.is_none(),
        "a narrowed delivery must not be compared against an unnarrowed standing"
    );
}

/// One drive, two applies into the archive, only one of which stamped a
/// source that still stands — so the card's decision count and the door's
/// genuinely differ, which is the shape the decisions clause exists for.
fn an_origin_whose_two_counts_both_diverge(conn: &Connection) -> (i64, i64) {
    let sd = insert_test_root(conn, "/Volumes/sd", "source", false);
    let archive = insert_test_root(conn, "/archive", "archive", false);
    let first = insert_decision_at(conn, "apply", 100);
    let second = insert_decision_at(conn, "apply", 200);
    write_rows(
        conn,
        &[
            extraction_row(first, sd, "/Volumes/sd", "photos", 2, Some(20), "/archive"),
            extraction_row(second, sd, "/Volumes/sd", "photos", 3, Some(30), "/archive"),
        ],
    );
    // One survivor, stamped by the first apply only: the second delivered
    // and nothing it delivered still stands.
    let kept = insert_test_source(conn, archive, "x.jpg", 1, 1, 10, 0);
    conn.execute(
        "UPDATE sources SET decision_id = ?, present = 1 WHERE id = ?",
        rusqlite::params![first, kept],
    )
    .unwrap();
    (first, second)
}

/// **The decisions clause acquires no gate of its own.** Both numbers are
/// read off the same card line at the same match, so every condition that
/// silences the file clause silences this one — asserted by walking the six
/// conditions rather than by argument.
///
/// Each case is preceded by a **positive control**: the ungated run must
/// produce a line carrying both axes, or the case proves nothing about the
/// gate it names. A fixture that never reaches `reconcile` at all yields an
/// absent line for a reason of its own, and a gate test resting on that
/// passes through an entirely different branch than the one it is named for.
#[test]
fn the_decisions_clause_inherits_every_file_clause_gate() {
    let conn = open_in_memory_for_test();
    an_origin_whose_two_counts_both_diverge(&conn);

    let ungated = || {
        let mut p = crossings_params(vec!["/archive"]);
        p.origin = Some("/Volumes/sd".to_string());
        p
    };

    // Positive control: the computation is reached and both axes diverge, so
    // every `is_none()` below is the gate firing rather than a fixture that
    // never had a line to suppress.
    let line = reported(&conn, &ungated()).reconciliation.unwrap();
    assert_eq!((line.standing, line.delivered), (1, 5));
    assert_eq!(
        (line.standing_decisions, line.delivered_decisions),
        (1, 2),
        "the fixture must diverge on the decisions axis too"
    );

    // 1. Machine output — a present-tense number has no place in a stream
    //    whose contract is view-independent.
    let mut machine = ungated();
    machine.machine_output = true;
    assert!(reported(&conn, &machine).reconciliation.is_none());

    // 2. A global view: the card answers for no place in particular.
    //
    //    Held in two places, and this case cannot tell them apart —
    //    red-smoking `reconcile`'s own disjunct leaves every test passing,
    //    because `compute_composition` refuses a global scope first and there
    //    is no card to read either count off. Asserted as the pair it is,
    //    rather than claiming a branch this fixture never reaches.
    let mut global = ungated();
    global.prefixes = Vec::new();
    assert!(reported(&conn, &global).reconciliation.is_none());
    assert!(
        crate::trail::ops::composition::compute_composition(&conn, &[])
            .unwrap()
            .is_none(),
        "the card's own global refusal is the outer half of this gate"
    );

    // 3. A named destination narrows the delivered count and narrows nothing
    //    about the card.
    let mut narrowed = ungated();
    narrowed.destination = Some("/archive".to_string());
    assert!(reported(&conn, &narrowed).reconciliation.is_none());

    // 4. No origin named: nothing to attribute the card's line to.
    let mut unnamed = ungated();
    unnamed.origin = None;
    assert!(reported(&conn, &unnamed).reconciliation.is_none());

    // 5. A sub-root origin: the card attributes at root level by design, so
    //    there is no card number at this grain to compare against.
    let mut deeper = ungated();
    deeper.origin = Some("/Volumes/sd/photos".to_string());
    let sub_root = reported(&conn, &deeper);
    assert_eq!(
        section(&sub_root, RowAspect::Arrival).files,
        5,
        "the section must exist, or the gate under test was never reached"
    );
    assert!(sub_root.reconciliation.is_none());

    // 6. No arrival section at all: an origin Canon knows, that delivered
    //    nothing here. There is no delivered count to set anything beside.
    let other = insert_test_root(&conn, "/Volumes/other-drive", "source", false);
    insert_test_source(&conn, other, "y.jpg", 1, 3, 10, 0);
    let mut silent = ungated();
    silent.origin = Some("/Volumes/other-drive".to_string());
    let quiet = reported(&conn, &silent);
    assert!(
        !quiet
            .sections
            .iter()
            .any(|s| s.aspect == RowAspect::Arrival),
        "the fixture must genuinely have no arrival section"
    );
    assert!(quiet.reconciliation.is_none());
}

/// The card's decision count is the card's own, at the exact match site —
/// never a second count of the same thing, on either axis.
#[test]
fn both_reconciliation_axes_come_from_the_card_not_a_re_derivation() {
    let conn = open_in_memory_for_test();
    let (first, _) = an_origin_whose_two_counts_both_diverge(&conn);

    let mut p = crossings_params(vec!["/archive"]);
    p.origin = Some("/Volumes/sd".to_string());
    let line = reported(&conn, &p).reconciliation.unwrap();

    let card =
        crate::trail::ops::composition::compute_composition(&conn, &["/archive".to_string()])
            .unwrap()
            .unwrap();
    match &card.origins[0] {
        crate::trail::domain::composition::OriginLine::FromRoot {
            files,
            decision_ids,
            ..
        } => {
            assert_eq!(line.standing, *files);
            assert_eq!(line.standing_decisions, decision_ids.len());
            assert_eq!(decision_ids, &vec![first]);
        }
        crate::trail::domain::composition::OriginLine::MultiOrigin { .. } => panic!("FromRoot"),
    }

    // And the delivered half is the door's own section count, not a third.
    assert_eq!(
        line.delivered_decisions,
        section(&reported(&conn, &p), RowAspect::Arrival).decision_count
    );
}

#[test]
fn the_reconciliation_line_is_absent_under_machine_output() {
    let conn = open_in_memory_for_test();
    archive_with_a_partly_surviving_delivery(&conn, "");

    let mut p = crossings_params(vec!["/archive"]);
    p.origin = Some("/Volumes/sd".to_string());
    p.machine_output = true;
    assert!(reported(&conn, &p).reconciliation.is_none());
}

// ----------------------------------------------------------------------
// Nothing crossed
// ----------------------------------------------------------------------

/// An archive whose applies were all intra-archive: both sections are empty
/// while rows genuinely exist. Saying "nothing crossed" without naming the
/// rearrangement reads as "nothing ever happened here".
#[test]
fn nothing_crossed_names_the_rearrangement_when_there_is_one() {
    let conn = open_in_memory_for_test();
    let archive = insert_test_root(&conn, "/archive", "archive", false);
    let apply = insert_decision_at(&conn, "apply", 100);
    write_rows(
        &conn,
        &[extraction_row(
            apply,
            archive,
            "/archive",
            "2016",
            9,
            Some(90),
            "/archive/Media/2016",
        )],
    );

    let result = reported(&conn, &crossings_params(vec!["/archive"]));
    assert!(result.sections.is_empty());
    match result.nothing_crossed {
        Some(NothingCrossed::Rearranged { files, bytes }) => {
            assert_eq!(files, 9);
            assert_eq!(bytes, Some(90));
        }
        _ => panic!("a view that only rearranged must say so"),
    }
}

#[test]
fn nothing_crossed_says_nothing_when_there_are_no_rows_at_all() {
    let conn = open_in_memory_for_test();
    insert_test_root(&conn, "/archive", "archive", false);
    let result = reported(&conn, &crossings_params(vec!["/archive"]));
    assert!(matches!(
        result.nothing_crossed,
        Some(NothingCrossed::Nothing)
    ));
}

// ----------------------------------------------------------------------
// Snapshot-path matching, and the query-path law
// ----------------------------------------------------------------------

/// Matching runs on write-time snapshot paths, so a destination root removed
/// and re-added under the same location keeps its link. A root-id join would
/// break exactly here — which is why `fetch_extractions_by_origin_root`, which
/// keys on `root_id`, is not what this door reads.
#[test]
fn a_removed_and_re_added_destination_root_keeps_its_link() {
    let conn = open_in_memory_for_test();
    let sd = insert_test_root(&conn, "/Volumes/sd", "source", false);
    let apply = insert_decision_at(&conn, "apply", 100);
    // The row's destination_root_id points at an archive root id that no
    // longer exists; the path is what survives.
    write_rows(
        &conn,
        &[DecisionExtraction {
            decision_id: apply,
            root_id: sd,
            root_path: "/Volumes/sd".to_string(),
            rel_prefix: "".to_string(),
            files: 4,
            bytes: Some(40),
            destination_root_id: Some(4242),
            destination_path: "/archive/Media".to_string(),
            disposition: Some(OriginDisposition::Relocated),
        }],
    );
    // Re-added under the same path, with a fresh id.
    insert_test_root(&conn, "/archive", "archive", false);

    let result = reported(&conn, &crossings_params(vec!["/archive"]));
    let arrivals = section(&result, RowAspect::Arrival);
    assert_eq!(arrivals.files, 4);
    match &arrivals.body {
        CrossingBody::Counterparts { lines, .. } => {
            assert_eq!(lines[0].counterpart.path, "/Volumes/sd");
            assert!(!lines[0].counterpart.root_removed);
        }
        CrossingBody::Deliveries { .. } => panic!("expected counterparts"),
    }
}

/// Read-only, and fresh per run: the door reports on decisions, it never
/// becomes one. Asserted over the whole surface rather than trusted to the
/// `&Connection` signature, since a recorder could be reached through it.
#[test]
fn crossings_records_no_decision_row() {
    let conn = open_in_memory_for_test();
    archive_with_two_origins(&conn);
    let before: i64 = conn
        .query_row("SELECT COUNT(*) FROM decisions", [], |r| r.get(0))
        .unwrap();

    // Every shape of the ask: bare, named, global-with-counterpart, capped.
    let mut named = crossings_params(vec!["/archive"]);
    named.origin = Some("/Volumes/sd".to_string());
    let mut global = crossings_params(vec![]);
    global.origin = Some("/Volumes/sd".to_string());
    let mut capped = crossings_params(vec!["/archive"]);
    capped.limit = Some(1);
    for p in [crossings_params(vec!["/archive"]), named, global, capped] {
        compute_crossings(&conn, &p).unwrap();
    }

    let after: i64 = conn
        .query_row("SELECT COUNT(*) FROM decisions", [], |r| r.get(0))
        .unwrap();
    assert_eq!(before, after, "a crossings run recorded a decision");
}

/// The query-path law, asserted at the new surface: every answer here comes
/// from DB projections. With no canon home and no ledger directory anywhere,
/// a run that opened a receipt file would fail rather than answer.
#[test]
fn crossings_reads_no_receipt_file() {
    let conn = open_in_memory_for_test();
    archive_with_two_origins(&conn);
    // The rows name `/archive/...` locations that exist in no filesystem
    // under test; a complete answer proves nothing on disk was consulted.
    let result = reported(&conn, &crossings_params(vec!["/archive"]));
    assert!(!result.sections.is_empty());
    assert!(!std::path::Path::new("/archive").exists());
}

// ----------------------------------------------------------------------
// Caps
// ----------------------------------------------------------------------

#[test]
fn the_section_cap_carries_an_explicit_remainder() {
    let conn = open_in_memory_for_test();
    let sd = insert_test_root(&conn, "/Volumes/sd", "source", false);
    insert_test_root(&conn, "/archive", "archive", false);
    let mut rows = Vec::new();
    for i in 0..7 {
        let apply = insert_decision_at(&conn, "apply", 100 + i);
        rows.push(extraction_row(
            apply,
            sd,
            "/Volumes/sd",
            &format!("dir{i}"),
            1,
            Some(10),
            &format!("/archive/d{i}"),
        ));
    }
    write_rows(&conn, &rows);

    let mut p = crossings_params(vec!["/archive"]);
    p.limit = Some(3);
    let result = reported(&conn, &p);
    match &section(&result, RowAspect::Arrival).body {
        // All seven share one origin root, so the *arrival* listing is one
        // line; the cap is exercised from the drive's side instead.
        CrossingBody::Counterparts { lines, .. } => assert_eq!(lines.len(), 1),
        CrossingBody::Deliveries { .. } => panic!("expected counterparts"),
    }

    let mut out = crossings_params(vec!["/Volumes/sd"]);
    out.limit = Some(3);
    let outbound = reported(&conn, &out);
    match &section(&outbound, RowAspect::Extraction).body {
        CrossingBody::Counterparts { lines, more } => {
            assert_eq!(lines.len(), 3);
            assert_eq!(*more, 4);
        }
        CrossingBody::Deliveries { .. } => panic!("expected counterparts"),
    }

    // `--all` uncaps.
    let uncapped = reported(&conn, &crossings_params(vec!["/Volumes/sd"]));
    match &section(&uncapped, RowAspect::Extraction).body {
        CrossingBody::Counterparts { lines, more } => {
            assert_eq!(lines.len(), 7);
            assert_eq!(*more, 0);
        }
        CrossingBody::Deliveries { .. } => panic!("expected counterparts"),
    }
}

/// Machine output carries each decision's **full** row set, not the
/// crossing-matched subset — the view-independence contract, which this door
/// must not become the one view to break.
#[test]
fn the_listed_decisions_carry_their_full_row_sets() {
    let conn = open_in_memory_for_test();
    let sd = insert_test_root(&conn, "/Volumes/sd", "source", false);
    let cf = insert_test_root(&conn, "/Volumes/cf", "source", false);
    insert_test_root(&conn, "/archive", "archive", false);
    let apply = insert_decision_at(&conn, "apply", 100);
    write_rows(
        &conn,
        &[
            extraction_row(apply, sd, "/Volumes/sd", "", 1, Some(10), "/archive/a"),
            // Same decision, a row that lands nowhere near the view.
            extraction_row(apply, cf, "/Volumes/cf", "", 1, Some(10), "/elsewhere"),
        ],
    );

    let mut p = crossings_params(vec!["/archive"]);
    p.origin = Some("/Volumes/sd".to_string());
    let result = reported(&conn, &p);
    assert_eq!(result.decisions.len(), 1);
    assert_eq!(result.extractions_all.get(&apply).unwrap().len(), 2);
}
