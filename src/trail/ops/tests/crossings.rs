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
