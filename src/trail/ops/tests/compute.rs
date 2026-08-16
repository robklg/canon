use chrono::NaiveDate;

use crate::core::repo;
use crate::core::repo::db::open_in_memory_for_test;
use crate::core::repo::insert_test_root;
use crate::trail::domain::placement::RowAspect;
use crate::trail::domain::timeline::{TimelineEvent, WhenValue};
use crate::trail::ops::compute::{
    classify_extraction_rows, compute_trail, local_midnight, TrailView,
};

use super::fixtures::{
    aspects_of, decision_ids, extraction_row, insert_decision_at, insert_note_at, params, scope,
};

#[test]
fn scoped_visibility_is_bidirectional() {
    let conn = open_in_memory_for_test();
    let root = insert_test_root(&conn, "/a", "source", false);
    let on_ancestor = insert_decision_at(&conn, "exclude_set", 100);
    scope(&conn, on_ancestor, root, "x");
    let on_descendant = insert_decision_at(&conn, "exclude_set", 200);
    scope(&conn, on_descendant, root, "x/y/z");
    let sibling = insert_decision_at(&conn, "exclude_set", 300);
    scope(&conn, sibling, root, "w");

    let result = compute_trail(&conn, &params(vec!["/a/x/y".to_string()])).unwrap();
    let ids = decision_ids(&result.view);
    assert!(ids.contains(&on_ancestor)); // decision on ancestor touched here
    assert!(ids.contains(&on_descendant)); // decision below is activity here
    assert!(!ids.contains(&sibling));
}

#[test]
fn global_decisions_counted_not_listed_in_scoped_view() {
    let conn = open_in_memory_for_test();
    let root = insert_test_root(&conn, "/a", "source", false);
    let scoped = insert_decision_at(&conn, "scan", 100);
    scope(&conn, scoped, root, "");
    insert_decision_at(&conn, "import_facts", 200); // no scope rows

    let result = compute_trail(&conn, &params(vec!["/a".to_string()])).unwrap();
    assert_eq!(decision_ids(&result.view), vec![scoped]);
    assert_eq!(result.unscoped_decisions, 1);

    // The global view lists it and reports no unscoped footer.
    let global = compute_trail(&conn, &params(Vec::new())).unwrap();
    assert_eq!(decision_ids(&global.view).len(), 2);
    assert_eq!(global.unscoped_decisions, 0);
}

// ------------------------------------------------------------------
// Extraction ledger visibility — the outbound direction
// ------------------------------------------------------------------

#[test]
fn classify_extraction_rows_judges_each_row_by_its_own_endpoints() {
    // Per row, not per decision. One apply reaches three ways at once, and
    // each row is judged by its own two endpoints:
    //   - `inside`  drawn from the view, landing in it  -> rearrangement
    //   - `left`    drawn from the view, landing outside -> extraction
    //   - `outside` drawn from elsewhere, landing here   -> arrival
    // Any classification that asks the question of the decision instead
    // of each row gets `left` wrong and drops `outside` entirely.
    let rows = vec![
        extraction_row(42, 1, "/archive", "2016", 47, Some(10), "/archive/2020"),
        extraction_row(42, 1, "/archive", "raw", 5, Some(1), "/elsewhere"),
        extraction_row(42, 2, "/Volumes/sd", "dcim", 8, Some(2), "/archive/2020"),
    ];
    let pairs = vec![(1, String::new())]; // the view's own root: /archive
    let prefixes = vec!["/archive".to_string()];

    let placements = classify_extraction_rows(rows, &pairs, &prefixes);
    assert_eq!(
        aspects_of(&placements, 42),
        vec![
            RowAspect::Rearrangement,
            RowAspect::Extraction,
            RowAspect::Arrival
        ]
    );
}

#[test]
fn classify_extraction_rows_classifies_one_directional_decisions() {
    let row = extraction_row(42, 1, "/src", "photos", 3, Some(9), "/archive/2020");

    // Viewed from the drawn-from root: an extraction.
    let out = classify_extraction_rows(
        vec![row.clone()],
        &[(1, String::new())],
        &["/src".to_string()],
    );
    assert_eq!(aspects_of(&out, 42), vec![RowAspect::Extraction]);

    // Viewed from the destination: an arrival (the view's own roots don't
    // include the source root, whatever ids they carry).
    let inbound = classify_extraction_rows(
        vec![row.clone()],
        &[(9, String::new())],
        &["/archive/2020".to_string()],
    );
    assert_eq!(aspects_of(&inbound, 42), vec![RowAspect::Arrival]);

    // A row touching neither endpoint is dropped, not tagged `Outside` —
    // the caller falls back to the selection-scope headline.
    let neither = classify_extraction_rows(
        vec![row.clone()],
        &[(9, String::new())],
        &["/elsewhere".to_string()],
    );
    assert!(neither.is_empty());
}

#[test]
fn classify_extraction_rows_never_matches_a_placement_above_the_view() {
    // The manufactured-history guard, both directions. A recorded
    // location that is an *ancestor* of the view claims nothing about
    // the view: a common prefix of `m/01` and `m/02` says nothing about
    // `m/03`. Bidirectional matching here is how #25 once rendered 245
    // files at a folder it never touched.
    let row = extraction_row(7, 1, "/src", "m", 245, Some(10), "/archive/m");

    // Destination `/archive/m` must not surface at the deeper sibling
    // view `/archive/m/03`...
    let deep_dest = classify_extraction_rows(
        vec![row.clone()],
        &[(9, String::new())],
        &["/archive/m/03".to_string()],
    );
    assert!(deep_dest.is_empty());

    // ...and origin prefix `m` must not surface at the deeper view
    // `m/03` of the same root.
    let deep_origin = classify_extraction_rows(
        vec![row.clone()],
        &[(1, "m/03".to_string())],
        &["/src/m/03".to_string()],
    );
    assert!(deep_origin.is_empty());

    // At a view that *contains* the placements, both match as before.
    let containing =
        classify_extraction_rows(vec![row], &[(1, String::new())], &["/src".to_string()]);
    assert_eq!(aspects_of(&containing, 7), vec![RowAspect::Extraction]);
}

#[test]
fn extraction_row_surfaces_scoped_view_despite_global_selection_scope() {
    let conn = open_in_memory_for_test();
    let root_a = insert_test_root(&conn, "/a", "source", false);
    insert_test_root(&conn, "/b", "source", false);
    // The apply's own selection scope is global (no decision_scopes row).
    let decision_id = insert_decision_at(&conn, "apply", 100);
    repo::decision::replace_extractions(
        &conn,
        &[extraction_row(
            decision_id,
            root_a,
            "/a",
            "",
            47,
            Some(3_900_000),
            "/archive/x",
        )],
    )
    .unwrap();

    // Surfaces in a view of the drawn-from root...
    let view_a = compute_trail(&conn, &params(vec!["/a".to_string()])).unwrap();
    assert_eq!(decision_ids(&view_a.view), vec![decision_id]);
    assert_eq!(
        aspects_of(&view_a.placements, decision_id),
        vec![RowAspect::Extraction]
    );
    // ...and being shown here means it must not double as "not shown".
    assert_eq!(view_a.unscoped_decisions, 0);

    // A sibling root never touched by the extraction doesn't see it...
    let view_b = compute_trail(&conn, &params(vec!["/b".to_string()])).unwrap();
    assert!(decision_ids(&view_b.view).is_empty());
    // ...and its footer still counts the untouched global decision.
    assert_eq!(view_b.unscoped_decisions, 1);
}

#[test]
fn a_story_handoff_at_an_emptied_place_gets_a_real_answer() {
    // Handoffs must answer, not just parse: the story review points at
    // emptied places by design (the best-resolved places are the
    // emptiest). End to end — the history-scope resolution accepts the
    // sourceless path, and the trail it feeds renders the extraction
    // that emptied the place, not an empty view.
    let conn = open_in_memory_for_test();
    let root_id = insert_test_root(&conn, "/a", "source", false);
    let decision_id = insert_decision_at(&conn, "apply", 100);
    repo::decision::replace_extractions(
        &conn,
        &[extraction_row(
            decision_id,
            root_id,
            "/a",
            "old/photos",
            12,
            Some(1_000),
            "/archive/2016",
        )],
    )
    .unwrap();

    let roots = repo::root::fetch_all(&conn).unwrap();
    let resolved = crate::ops::scope::resolve_history_scope(
        &[std::path::PathBuf::from("/a/old/photos")],
        &roots,
    )
    .expect("an emptied place on a live root resolves in history tense");

    let result = compute_trail(&conn, &params(resolved.prefixes)).unwrap();
    assert_eq!(decision_ids(&result.view), vec![decision_id]);
    assert_eq!(
        aspects_of(&result.placements, decision_id),
        vec![RowAspect::Extraction]
    );
}

#[test]
fn decision_with_scope_row_and_extraction_row_appears_exactly_once() {
    let conn = open_in_memory_for_test();
    let root = insert_test_root(&conn, "/a", "source", false);
    let decision_id = insert_decision_at(&conn, "apply", 100);
    scope(&conn, decision_id, root, "");
    repo::decision::replace_extractions(
        &conn,
        &[extraction_row(
            decision_id,
            root,
            "/a",
            "",
            5,
            Some(500),
            "/archive",
        )],
    )
    .unwrap();

    let result = compute_trail(&conn, &params(vec!["/a".to_string()])).unwrap();
    // Union+dedup: one id, not two — never both a selection line and an
    // extraction line (the id-set union collapses to one appearance).
    assert_eq!(decision_ids(&result.view), vec![decision_id]);
    assert!(result.placements.contains_key(&decision_id));
}

#[test]
fn extraction_rollup_reports_whole_history_even_when_capped() {
    let conn = open_in_memory_for_test();
    let root = insert_test_root(&conn, "/a", "source", false);
    let d1 = insert_decision_at(&conn, "apply", 100);
    let d2 = insert_decision_at(&conn, "apply", 200);
    repo::decision::replace_extractions(
        &conn,
        &[extraction_row(
            d1,
            root,
            "/a",
            "",
            10,
            Some(1_000),
            "/archive/x",
        )],
    )
    .unwrap();
    repo::decision::replace_extractions(
        &conn,
        &[extraction_row(
            d2,
            root,
            "/a",
            "",
            20,
            Some(2_000),
            "/archive/y",
        )],
    )
    .unwrap();

    let mut p = params(vec!["/a".to_string()]);
    p.limit = Some(1);
    let result = compute_trail(&conn, &p).unwrap();
    assert_eq!(result.earlier_decisions, 1); // the window is capped...
    let rollup = result.extraction_rollup.unwrap();
    assert_eq!(rollup.files, 30); // ...but the rollup is whole-history
    assert_eq!(rollup.bytes, Some(3_000));
    assert_eq!(rollup.destinations, 2);
}

#[test]
fn extraction_rollup_none_when_no_touching_rows() {
    let conn = open_in_memory_for_test();
    insert_test_root(&conn, "/a", "source", false);
    let result = compute_trail(&conn, &params(vec!["/a".to_string()])).unwrap();
    assert!(result.extraction_rollup.is_none());
}

#[test]
fn extraction_rollup_bytes_omitted_when_any_row_lacks_them() {
    let conn = open_in_memory_for_test();
    let root = insert_test_root(&conn, "/a", "source", false);
    let d1 = insert_decision_at(&conn, "apply", 100);
    let d2 = insert_decision_at(&conn, "apply", 200);
    repo::decision::replace_extractions(
        &conn,
        &[extraction_row(
            d1,
            root,
            "/a",
            "",
            10,
            Some(1_000),
            "/archive/x",
        )],
    )
    .unwrap();
    repo::decision::replace_extractions(
        &conn,
        &[extraction_row(d2, root, "/a", "", 20, None, "/archive/y")],
    )
    .unwrap();

    let result = compute_trail(&conn, &params(vec!["/a".to_string()])).unwrap();
    let rollup = result.extraction_rollup.unwrap();
    assert_eq!(rollup.files, 30);
    assert_eq!(rollup.bytes, None);
}

#[test]
fn extraction_rollup_none_for_global_view() {
    let conn = open_in_memory_for_test();
    let root = insert_test_root(&conn, "/a", "source", false);
    let d = insert_decision_at(&conn, "apply", 100);
    repo::decision::replace_extractions(
        &conn,
        &[extraction_row(d, root, "/a", "", 1, Some(10), "/archive")],
    )
    .unwrap();

    let result = compute_trail(&conn, &params(Vec::new())).unwrap();
    assert!(result.extraction_rollup.is_none());
    assert!(result.placements.is_empty());
}

#[test]
fn extraction_rollup_none_for_time_lens_view() {
    let conn = open_in_memory_for_test();
    let root = insert_test_root(&conn, "/a", "source", false);
    let d = insert_decision_at(&conn, "apply", 100);
    scope(&conn, d, root, "");
    repo::decision::replace_extractions(
        &conn,
        &[extraction_row(d, root, "/a", "", 1, Some(10), "/archive")],
    )
    .unwrap();

    let mut p = params(vec!["/a".to_string()]);
    p.timeframe = Some(WhenValue::Since(
        NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(),
    ));
    let result = compute_trail(&conn, &p).unwrap();
    assert!(result.extraction_rollup.is_none());
}

// ------------------------------------------------------------------
// Arrival ledger visibility — the extraction ledger's inbound direction
// ------------------------------------------------------------------

#[test]
fn arrival_row_surfaces_scoped_view_despite_unrelated_source_root() {
    let conn = open_in_memory_for_test();
    let source_root = insert_test_root(&conn, "/a", "source", false);
    insert_test_root(&conn, "/archive", "archive", false);
    // The apply's own selection scope is global (no decision_scopes row).
    let decision_id = insert_decision_at(&conn, "apply", 100);
    repo::decision::replace_extractions(
        &conn,
        &[extraction_row(
            decision_id,
            source_root,
            "/a",
            "",
            47,
            Some(3_900_000),
            "/archive/x",
        )],
    )
    .unwrap();

    // Surfaces in a view of the destination it landed in...
    let view = compute_trail(&conn, &params(vec!["/archive".to_string()])).unwrap();
    assert_eq!(decision_ids(&view.view), vec![decision_id]);
    assert_eq!(
        aspects_of(&view.placements, decision_id),
        vec![RowAspect::Arrival]
    );
    // ...and being shown here means it must not double as "not shown".
    assert_eq!(view.unscoped_decisions, 0);

    // A view of the source root sees the outbound extraction aspect, not
    // an arrival — the two directions are distinct per-row tags.
    let source_view = compute_trail(&conn, &params(vec!["/a".to_string()])).unwrap();
    assert_eq!(
        aspects_of(&source_view.placements, decision_id),
        vec![RowAspect::Extraction]
    );
}

#[test]
fn arrival_matching_is_descendant_or_equal_and_segment_aware() {
    // Re-pointed from `arrival_matching_is_bidirectional_and_segment_aware`
    // when the placement law landed: an ancestor destination used to match
    // (`shallower` surfaced at `/archive/x`), which is exactly how a
    // common-prefix destination manufactured arrivals at sibling folders.
    // The recorded location must now be *contained by* the view.
    let conn = open_in_memory_for_test();
    let source_root = insert_test_root(&conn, "/a", "source", false);
    insert_test_root(&conn, "/archive", "archive", false);
    let deeper = insert_decision_at(&conn, "apply", 100); // destination deeper than the view
    repo::decision::replace_extractions(
        &conn,
        &[extraction_row(
            deeper,
            source_root,
            "/a",
            "",
            1,
            Some(10),
            "/archive/x/y",
        )],
    )
    .unwrap();
    let shallower = insert_decision_at(&conn, "apply", 200); // destination is an ancestor of the view
    repo::decision::replace_extractions(
        &conn,
        &[extraction_row(
            shallower,
            source_root,
            "/a",
            "",
            1,
            Some(10),
            "/archive",
        )],
    )
    .unwrap();
    let sibling = insert_decision_at(&conn, "apply", 300); // similar prefix, not a real ancestor/descendant
    repo::decision::replace_extractions(
        &conn,
        &[extraction_row(
            sibling,
            source_root,
            "/a",
            "",
            1,
            Some(10),
            "/archive/xc",
        )],
    )
    .unwrap();

    let result = compute_trail(&conn, &params(vec!["/archive/x".to_string()])).unwrap();
    let ids = decision_ids(&result.view);
    assert!(ids.contains(&deeper)); // /archive/x/y is inside the view
    assert!(!ids.contains(&shallower)); // /archive claims nothing about /archive/x
    assert!(!ids.contains(&sibling)); // /archive/xc is not under /archive/x

    // At the ancestor view every placement is contained — all three match.
    let wide = compute_trail(&conn, &params(vec!["/archive".to_string()])).unwrap();
    let wide_ids = decision_ids(&wide.view);
    assert!(wide_ids.contains(&deeper));
    assert!(wide_ids.contains(&shallower));
    assert!(wide_ids.contains(&sibling));
}

#[test]
fn a_sibling_view_never_lists_a_delivery_that_missed_it() {
    // The acceptance-review finding, as a guard. Decision `elsewhere`
    // delivered 245 files to `m/01` and `m/02`; its recorded destination
    // collapsed to the common prefix `/archive/m`. Decision `here`
    // delivered 1,005 files into `m/03` itself. Standing at `m/03`, the
    // trail once listed both, identically — the header's promise broken.
    let conn = open_in_memory_for_test();
    let source_root = insert_test_root(&conn, "/a", "source", false);
    insert_test_root(&conn, "/archive", "archive", false);
    let elsewhere = insert_decision_at(&conn, "apply", 100);
    repo::decision::replace_extractions(
        &conn,
        &[extraction_row(
            elsewhere,
            source_root,
            "/a",
            "",
            245,
            Some(2_450),
            "/archive/m",
        )],
    )
    .unwrap();
    let here = insert_decision_at(&conn, "apply", 200);
    repo::decision::replace_extractions(
        &conn,
        &[extraction_row(
            here,
            source_root,
            "/a",
            "",
            1_005,
            Some(10_050),
            "/archive/m/03",
        )],
    )
    .unwrap();

    // At m/03: only the decision that actually delivered there — and the
    // rollup counts its files alone, not the 1,250-file sum.
    let month = compute_trail(&conn, &params(vec!["/archive/m/03".to_string()])).unwrap();
    assert_eq!(decision_ids(&month.view), vec![here]);
    assert!(!month.placements.contains_key(&elsewhere));
    let rollup = month.arrival_rollup.expect("m/03 received content");
    assert_eq!(rollup.files, 1_005);
    assert_eq!(rollup.bytes, Some(10_050));

    // At the year the coarse row's claim is true, so both surface and
    // the rollup honestly sums them.
    let year = compute_trail(&conn, &params(vec!["/archive/m".to_string()])).unwrap();
    let ids = decision_ids(&year.view);
    assert!(ids.contains(&elsewhere) && ids.contains(&here));
    assert_eq!(
        year.arrival_rollup.expect("m received content").files,
        1_250
    );
}

#[test]
fn partial_view_counts_only_files_placed_within_it() {
    // With directory-precision rows, one apply fanning out to two
    // folders reads exactly at each: standing at m/01 the rollup says
    // 105, not the apply-wide 245 — and at the ancestor it says 245.
    let conn = open_in_memory_for_test();
    let source_root = insert_test_root(&conn, "/a", "source", false);
    insert_test_root(&conn, "/archive", "archive", false);
    let apply = insert_decision_at(&conn, "apply", 100);
    let mut to_01 = extraction_row(
        apply,
        source_root,
        "/a",
        "dcim",
        105,
        Some(1_050),
        "/archive/m/01",
    );
    to_01.destination_root_id = Some(2);
    let mut to_02 = extraction_row(
        apply,
        source_root,
        "/a",
        "dcim",
        140,
        Some(1_400),
        "/archive/m/02",
    );
    to_02.destination_root_id = Some(2);
    repo::decision::replace_extractions(&conn, &[to_01, to_02]).unwrap();

    let one = compute_trail(&conn, &params(vec!["/archive/m/01".to_string()])).unwrap();
    assert_eq!(aspects_of(&one.placements, apply), vec![RowAspect::Arrival]);
    let rollup = one.arrival_rollup.expect("m/01 received content");
    assert_eq!(rollup.files, 105);
    assert_eq!(rollup.bytes, Some(1_050));

    let both = compute_trail(&conn, &params(vec!["/archive/m".to_string()])).unwrap();
    assert_eq!(
        aspects_of(&both.placements, apply),
        vec![RowAspect::Arrival, RowAspect::Arrival]
    );
    assert_eq!(both.arrival_rollup.expect("m received content").files, 245);
}

#[test]
fn time_lens_applies_the_same_placement_law() {
    // Arrivals join --since/--on views through the same id extension, so
    // a placement above the view must stay invisible there too.
    let conn = open_in_memory_for_test();
    let source_root = insert_test_root(&conn, "/a", "source", false);
    insert_test_root(&conn, "/archive", "archive", false);
    let day = NaiveDate::from_ymd_opt(2026, 7, 10).unwrap();
    let apply = insert_decision_at(&conn, "apply", local_midnight(day) + 3_600);
    repo::decision::replace_extractions(
        &conn,
        &[extraction_row(
            apply,
            source_root,
            "/a",
            "",
            245,
            Some(2_450),
            "/archive/m",
        )],
    )
    .unwrap();

    let mut p = params(vec!["/archive/m/03".to_string()]);
    p.timeframe = Some(WhenValue::Since(day));
    let result = compute_trail(&conn, &p).unwrap();
    assert!(decision_ids(&result.view).is_empty());

    let mut wide = params(vec!["/archive/m".to_string()]);
    wide.timeframe = Some(WhenValue::Since(day));
    let seen = compute_trail(&conn, &wide).unwrap();
    assert_eq!(decision_ids(&seen.view), vec![apply]);
}

#[test]
fn arrival_surfaces_when_the_destination_root_was_removed() {
    // The archive root that received this apply is gone; the folder is
    // still viewable because a surviving ancestor root covers it. The
    // extraction row's `destination_root_id` now points at nothing, so
    // only snapshot-path matching can still surface the arrival.
    let conn = open_in_memory_for_test();
    let source_root = insert_test_root(&conn, "/a", "source", false);
    insert_test_root(&conn, "/archive", "archive", false);
    let removed_root = insert_test_root(&conn, "/archive/media", "archive", false);
    let decision_id = insert_decision_at(&conn, "apply", 100);
    let mut row = extraction_row(
        decision_id,
        source_root,
        "/a",
        "",
        5,
        Some(500),
        "/archive/media",
    );
    row.destination_root_id = Some(removed_root);
    repo::decision::replace_extractions(&conn, &[row]).unwrap();

    conn.execute("DELETE FROM roots WHERE id = ?", [removed_root])
        .unwrap();

    let result = compute_trail(&conn, &params(vec!["/archive/media".to_string()])).unwrap();
    assert_eq!(
        aspects_of(&result.placements, decision_id),
        vec![RowAspect::Arrival]
    );
    // Shown here means it must not also count as "not shown".
    assert_eq!(result.unscoped_decisions, 0);
}

#[test]
fn arrival_surfaces_when_the_destination_root_was_removed_and_re_added() {
    // The harder half: the destination path is registered again, but as a
    // *new* root with a new id. The recorded `destination_root_id` is
    // stale, so a root-id-keyed join would silently lose this arrival
    // while the snapshot path still matches exactly.
    let conn = open_in_memory_for_test();
    let old_destination = insert_test_root(&conn, "/archive/media", "archive", false);
    // Inserted after, so re-adding below can't reuse the freed rowid.
    let source_root = insert_test_root(&conn, "/a", "source", false);
    let decision_id = insert_decision_at(&conn, "apply", 100);
    let mut row = extraction_row(
        decision_id,
        source_root,
        "/a",
        "",
        5,
        Some(500),
        "/archive/media",
    );
    row.destination_root_id = Some(old_destination);
    repo::decision::replace_extractions(&conn, &[row]).unwrap();

    conn.execute("DELETE FROM roots WHERE id = ?", [old_destination])
        .unwrap();
    let re_added = insert_test_root(&conn, "/archive/media", "archive", false);
    assert_ne!(
        re_added, old_destination,
        "the point of this test is a changed id"
    );

    let result = compute_trail(&conn, &params(vec!["/archive/media".to_string()])).unwrap();
    assert_eq!(
        aspects_of(&result.placements, decision_id),
        vec![RowAspect::Arrival]
    );
    assert_eq!(result.unscoped_decisions, 0);
}

#[test]
fn decision_with_extraction_and_arrival_row_touching_same_view_appears_once() {
    // Intra-view relocation: a decision whose source and destination are
    // both inside the viewed scope. It must list once, its row tagged as
    // a rearrangement — one row, one aspect, one rendered line.
    let conn = open_in_memory_for_test();
    let root = insert_test_root(&conn, "/a", "source", false);
    let decision_id = insert_decision_at(&conn, "apply", 100);
    repo::decision::replace_extractions(
        &conn,
        &[extraction_row(
            decision_id,
            root,
            "/a",
            "photos",
            5,
            Some(500),
            "/a/archive",
        )],
    )
    .unwrap();

    let result = compute_trail(&conn, &params(vec!["/a".to_string()])).unwrap();
    assert_eq!(decision_ids(&result.view), vec![decision_id]);
    assert_eq!(
        aspects_of(&result.placements, decision_id),
        vec![RowAspect::Rearrangement]
    );
}

#[test]
fn arrival_rollup_reports_whole_history_even_when_capped() {
    let conn = open_in_memory_for_test();
    let source_root = insert_test_root(&conn, "/a", "source", false);
    insert_test_root(&conn, "/archive", "archive", false);
    let d1 = insert_decision_at(&conn, "apply", 100);
    let d2 = insert_decision_at(&conn, "apply", 200);
    repo::decision::replace_extractions(
        &conn,
        &[extraction_row(
            d1,
            source_root,
            "/a",
            "",
            10,
            Some(1_000),
            "/archive/x",
        )],
    )
    .unwrap();
    repo::decision::replace_extractions(
        &conn,
        &[extraction_row(
            d2,
            source_root,
            "/a",
            "",
            20,
            Some(2_000),
            "/archive/y",
        )],
    )
    .unwrap();

    let mut p = params(vec!["/archive".to_string()]);
    p.limit = Some(1);
    let result = compute_trail(&conn, &p).unwrap();
    assert_eq!(result.earlier_decisions, 1); // the window is capped...
    let rollup = result.arrival_rollup.unwrap();
    assert_eq!(rollup.files, 30); // ...but the rollup is whole-history
    assert_eq!(rollup.bytes, Some(3_000));
    assert_eq!(rollup.origins, 1); // both rows drawn from the same root
}

#[test]
fn arrival_rollup_none_when_no_touching_rows() {
    let conn = open_in_memory_for_test();
    insert_test_root(&conn, "/archive", "archive", false);
    let result = compute_trail(&conn, &params(vec!["/archive".to_string()])).unwrap();
    assert!(result.arrival_rollup.is_none());
}

#[test]
fn arrival_rollup_bytes_omitted_when_any_row_lacks_them() {
    let conn = open_in_memory_for_test();
    let source_root = insert_test_root(&conn, "/a", "source", false);
    insert_test_root(&conn, "/archive", "archive", false);
    let d1 = insert_decision_at(&conn, "apply", 100);
    let d2 = insert_decision_at(&conn, "apply", 200);
    repo::decision::replace_extractions(
        &conn,
        &[extraction_row(
            d1,
            source_root,
            "/a",
            "",
            10,
            Some(1_000),
            "/archive/x",
        )],
    )
    .unwrap();
    repo::decision::replace_extractions(
        &conn,
        &[extraction_row(
            d2,
            source_root,
            "/a",
            "",
            20,
            None,
            "/archive/y",
        )],
    )
    .unwrap();

    let result = compute_trail(&conn, &params(vec!["/archive".to_string()])).unwrap();
    let rollup = result.arrival_rollup.unwrap();
    assert_eq!(rollup.files, 30);
    assert_eq!(rollup.bytes, None);
}

#[test]
fn arrival_rollup_distinguishes_origins_by_root_path() {
    let conn = open_in_memory_for_test();
    let root_a = insert_test_root(&conn, "/a", "source", false);
    let root_b = insert_test_root(&conn, "/b", "source", false);
    insert_test_root(&conn, "/archive", "archive", false);
    let d = insert_decision_at(&conn, "apply", 100);
    repo::decision::replace_extractions(
        &conn,
        &[
            extraction_row(d, root_a, "/a", "", 1, Some(10), "/archive/x"),
            extraction_row(d, root_b, "/b", "", 1, Some(10), "/archive/x"),
        ],
    )
    .unwrap();

    let result = compute_trail(&conn, &params(vec!["/archive".to_string()])).unwrap();
    let rollup = result.arrival_rollup.unwrap();
    assert_eq!(rollup.files, 2);
    assert_eq!(rollup.origins, 2);
}

#[test]
fn arrival_rollup_and_arrivals_empty_for_global_view() {
    let conn = open_in_memory_for_test();
    let source_root = insert_test_root(&conn, "/a", "source", false);
    insert_test_root(&conn, "/archive", "archive", false);
    let d = insert_decision_at(&conn, "apply", 100);
    repo::decision::replace_extractions(
        &conn,
        &[extraction_row(
            d,
            source_root,
            "/a",
            "",
            1,
            Some(10),
            "/archive/x",
        )],
    )
    .unwrap();

    let result = compute_trail(&conn, &params(Vec::new())).unwrap();
    assert!(result.placements.is_empty());
    assert!(result.arrival_rollup.is_none());
}

#[test]
fn arrival_rollup_none_for_time_lens_view_but_decision_still_listed() {
    let conn = open_in_memory_for_test();
    let source_root = insert_test_root(&conn, "/a", "source", false);
    insert_test_root(&conn, "/archive", "archive", false);
    let day1 = NaiveDate::from_ymd_opt(2026, 7, 10).unwrap();
    let ts1 = local_midnight(day1) + 3600;
    let apply = insert_decision_at(&conn, "apply", ts1);
    repo::decision::replace_extractions(
        &conn,
        &[extraction_row(
            apply,
            source_root,
            "/a",
            "",
            5,
            Some(500),
            "/archive/x",
        )],
    )
    .unwrap();

    let mut p = params(vec!["/archive".to_string()]);
    p.timeframe = Some(WhenValue::Since(day1));
    let result = compute_trail(&conn, &p).unwrap();
    assert!(result.arrival_rollup.is_none());
    match &result.view {
        TrailView::Days(days) => {
            assert_eq!(days.len(), 1);
            let ids: Vec<i64> = days[0]
                .events
                .iter()
                .filter_map(|e| match e {
                    TimelineEvent::Decision(d) => Some(d.id),
                    TimelineEvent::Note(_) => None,
                })
                .collect();
            assert_eq!(ids, vec![apply]);
        }
        TrailView::Recent(_) => panic!("time lens must be Days"),
    }
}

// ------------------------------------------------------------------
// Rearrangement: rows that crossed no boundary
//
// An intra-archive apply cannot currently be produced through the CLI
// (`apply` aborts with "files already in destination archive" whatever
// --allow is given) — that is the one-way-relocation machinery the vision
// open question names. These fixtures write the extraction rows such an
// apply *would* record, which is what the read layer sees either way.
// ------------------------------------------------------------------

#[test]
fn rearrangement_row_leaves_both_crossing_rollups() {
    let conn = open_in_memory_for_test();
    let archive = insert_test_root(&conn, "/archive", "archive", false);
    let d = insert_decision_at(&conn, "apply", 100);
    // Drawn from /archive/2016, landing in /archive/2020: viewed at the
    // archive root, both endpoints are inside, so nothing crossed.
    repo::decision::replace_extractions(
        &conn,
        &[extraction_row(
            d,
            archive,
            "/archive",
            "2016",
            47,
            Some(3_900),
            "/archive/2020",
        )],
    )
    .unwrap();

    let result = compute_trail(&conn, &params(vec!["/archive".to_string()])).unwrap();
    assert!(
        result.extraction_rollup.is_none(),
        "nothing left this place"
    );
    assert!(result.arrival_rollup.is_none(), "nothing entered it");
    let rollup = result.rearrangement_rollup.unwrap();
    assert_eq!(rollup.files, 47);
    assert_eq!(rollup.bytes, Some(3_900));
}

#[test]
fn narrower_view_reads_the_same_decision_as_an_arrival() {
    // The scope-dependence is the rule working: the boundary moved, so
    // the same row now crosses it.
    let conn = open_in_memory_for_test();
    let archive = insert_test_root(&conn, "/archive", "archive", false);
    let d = insert_decision_at(&conn, "apply", 100);
    repo::decision::replace_extractions(
        &conn,
        &[extraction_row(
            d,
            archive,
            "/archive",
            "2016",
            47,
            Some(3_900),
            "/archive/2020",
        )],
    )
    .unwrap();

    let result = compute_trail(&conn, &params(vec!["/archive/2020".to_string()])).unwrap();
    assert!(result.rearrangement_rollup.is_none());
    let rollup = result.arrival_rollup.unwrap();
    assert_eq!(rollup.files, 47);
}

#[test]
fn one_decision_can_feed_two_rollups_at_once() {
    // The footer-level form of the mixed-origin bug: an apply drawing
    // from inside the view and from outside it rearranged some content
    // and received the rest. Filtering decisions rather than rows would
    // put all 55 files in one rollup.
    let conn = open_in_memory_for_test();
    let archive = insert_test_root(&conn, "/archive", "archive", false);
    let sd = insert_test_root(&conn, "/Volumes/sd", "source", false);
    let d = insert_decision_at(&conn, "apply", 100);
    repo::decision::replace_extractions(
        &conn,
        &[
            extraction_row(
                d,
                archive,
                "/archive",
                "2016",
                47,
                Some(3_900),
                "/archive/2020",
            ),
            extraction_row(d, sd, "/Volumes/sd", "dcim", 8, Some(800), "/archive/2020"),
        ],
    )
    .unwrap();

    let result = compute_trail(&conn, &params(vec!["/archive".to_string()])).unwrap();
    let rearranged = result.rearrangement_rollup.unwrap();
    assert_eq!(rearranged.files, 47);
    assert_eq!(rearranged.bytes, Some(3_900));
    let arrived = result.arrival_rollup.unwrap();
    assert_eq!(arrived.files, 8);
    assert_eq!(arrived.bytes, Some(800));
    assert_eq!(arrived.origins, 1, "only the outside root is an origin");
    assert!(result.extraction_rollup.is_none());
}

#[test]
fn bytes_are_all_or_omitted_per_rollup_not_across_them() {
    // An unknown-size crossing must not suppress a fully known
    // rearrangement total — each rollup judges its own rows.
    let conn = open_in_memory_for_test();
    let archive = insert_test_root(&conn, "/archive", "archive", false);
    let sd = insert_test_root(&conn, "/Volumes/sd", "source", false);
    let d = insert_decision_at(&conn, "apply", 100);
    repo::decision::replace_extractions(
        &conn,
        &[
            extraction_row(
                d,
                archive,
                "/archive",
                "2016",
                47,
                Some(3_900),
                "/archive/2020",
            ),
            extraction_row(d, sd, "/Volumes/sd", "dcim", 8, None, "/archive/2020"),
        ],
    )
    .unwrap();

    let result = compute_trail(&conn, &params(vec!["/archive".to_string()])).unwrap();
    assert_eq!(result.arrival_rollup.unwrap().bytes, None);
    assert_eq!(result.rearrangement_rollup.unwrap().bytes, Some(3_900));
}

#[test]
fn rearrangement_rollup_is_whole_history_despite_the_cap() {
    let conn = open_in_memory_for_test();
    let archive = insert_test_root(&conn, "/archive", "archive", false);
    let d1 = insert_decision_at(&conn, "apply", 100);
    let d2 = insert_decision_at(&conn, "apply", 200);
    for (d, files, bytes) in [(d1, 10, 1_000), (d2, 20, 2_000)] {
        repo::decision::replace_extractions(
            &conn,
            &[extraction_row(
                d,
                archive,
                "/archive",
                "2016",
                files,
                Some(bytes),
                "/archive/2020",
            )],
        )
        .unwrap();
    }

    let mut p = params(vec!["/archive".to_string()]);
    p.limit = Some(1);
    let result = compute_trail(&conn, &p).unwrap();
    assert_eq!(result.earlier_decisions, 1);
    let rollup = result.rearrangement_rollup.unwrap();
    assert_eq!(rollup.files, 30);
    assert_eq!(rollup.bytes, Some(3_000));
}

#[test]
fn rearrangement_rollup_none_for_global_and_time_lens_views() {
    let conn = open_in_memory_for_test();
    let archive = insert_test_root(&conn, "/archive", "archive", false);
    let day = NaiveDate::from_ymd_opt(2026, 7, 10).unwrap();
    let d = insert_decision_at(&conn, "apply", local_midnight(day) + 3600);
    repo::decision::replace_extractions(
        &conn,
        &[extraction_row(
            d,
            archive,
            "/archive",
            "2016",
            47,
            Some(3_900),
            "/archive/2020",
        )],
    )
    .unwrap();

    let global = compute_trail(&conn, &params(Vec::new())).unwrap();
    assert!(global.rearrangement_rollup.is_none());

    let mut p = params(vec!["/archive".to_string()]);
    p.timeframe = Some(WhenValue::Since(day));
    let timed = compute_trail(&conn, &p).unwrap();
    assert!(timed.rearrangement_rollup.is_none());
}

#[test]
fn extractions_all_populated_for_global_view_jsonl_completeness() {
    let conn = open_in_memory_for_test();
    let root = insert_test_root(&conn, "/a", "source", false);
    let d = insert_decision_at(&conn, "apply", 100);
    repo::decision::replace_extractions(
        &conn,
        &[extraction_row(d, root, "/a", "", 1, Some(10), "/archive")],
    )
    .unwrap();

    let result = compute_trail(&conn, &params(Vec::new())).unwrap();
    // The touching map is empty at global scope (nothing to touch)...
    assert!(result.placements.is_empty());
    // ...but the full-per-decision map used for JSONL still has it.
    assert!(result.extractions_all.contains_key(&d));
    assert_eq!(result.extractions_all[&d].len(), 1);
}

#[test]
fn cap_keeps_most_recent_and_counts_earlier() {
    let conn = open_in_memory_for_test();
    for ts in [100, 200, 300] {
        insert_decision_at(&conn, "scan", ts);
    }
    let mut p = params(Vec::new());
    p.limit = Some(2);
    let result = compute_trail(&conn, &p).unwrap();
    let ids = decision_ids(&result.view);
    assert_eq!(ids.len(), 2);
    assert_eq!(result.earlier_decisions, 1);
    assert_eq!(result.total_decisions, 3);
    // A timeline reads forward: oldest of the kept window first.
    match &result.view {
        TrailView::Recent(events) => {
            assert!(events[0].created_at() < events[1].created_at());
        }
        TrailView::Days(_) => panic!("scope lens must be Recent"),
    }
}

#[test]
fn notes_interleave_and_can_be_suppressed() {
    let conn = open_in_memory_for_test();
    let root = insert_test_root(&conn, "/a", "source", false);
    let d = insert_decision_at(&conn, "scan", 100);
    scope(&conn, d, root, "");
    insert_note_at(&conn, root, "x", 150);

    let with_notes = compute_trail(&conn, &params(vec!["/a".to_string()])).unwrap();
    match &with_notes.view {
        TrailView::Recent(events) => {
            assert_eq!(events.len(), 2);
            // Chronological: the decision (100) precedes the note (150).
            assert!(matches!(events[0], TimelineEvent::Decision(_)));
            assert!(matches!(events[1], TimelineEvent::Note(_)));
        }
        TrailView::Days(_) => panic!(),
    }

    let mut p = params(vec!["/a".to_string()]);
    p.include_notes = false;
    let without = compute_trail(&conn, &p).unwrap();
    match &without.view {
        TrailView::Recent(events) => assert_eq!(events.len(), 1),
        TrailView::Days(_) => panic!(),
    }
}

#[test]
fn scope_with_only_notes_still_shows_them() {
    let conn = open_in_memory_for_test();
    let root = insert_test_root(&conn, "/a", "source", false);
    insert_note_at(&conn, root, "x", 150);

    let result = compute_trail(&conn, &params(vec!["/a".to_string()])).unwrap();
    match &result.view {
        TrailView::Recent(events) => {
            assert_eq!(events.len(), 1);
            assert!(matches!(events[0], TimelineEvent::Note(_)));
        }
        TrailView::Days(_) => panic!(),
    }
    assert_eq!(result.total_decisions, 0);
}

#[test]
fn time_lens_groups_days_and_rolls_up() {
    let conn = open_in_memory_for_test();
    let root = insert_test_root(&conn, "/a", "source", false);
    // Two dates, epochs built through the same local mapping the
    // implementation uses, so the test is timezone-independent.
    let day1 = NaiveDate::from_ymd_opt(2026, 7, 10).unwrap();
    let day2 = NaiveDate::from_ymd_opt(2026, 7, 11).unwrap();
    let ts1 = local_midnight(day1) + 3600;
    let ts2 = local_midnight(day2) + 3600;

    let scan = insert_decision_at(&conn, "scan", ts1);
    scope(&conn, scan, root, "");
    let apply = insert_decision_at(&conn, "apply", ts2);
    scope(&conn, apply, root, "");

    // Stamp: the scan observed two deletions and one new file.
    let new = crate::core::repo::source::insert_test_source(&conn, root, "n.jpg", 1, 1, 10, 0);
    let g1 = crate::core::repo::source::insert_test_source(&conn, root, "g1.jpg", 1, 2, 100, 0);
    let g2 = crate::core::repo::source::insert_test_source(&conn, root, "g2.jpg", 1, 3, 200, 0);
    for (id, present) in [(new, 1), (g1, 0), (g2, 0)] {
        conn.execute(
            "UPDATE sources SET decision_id = ?, present = ? WHERE id = ?",
            rusqlite::params![scan, present, id],
        )
        .unwrap();
    }

    let mut p = params(vec!["/a".to_string()]);
    p.timeframe = Some(WhenValue::Since(day1));
    let result = compute_trail(&conn, &p).unwrap();
    match &result.view {
        TrailView::Days(days) => {
            assert_eq!(days.len(), 2);
            assert_eq!(days[0].date, day1); // oldest first
            assert_eq!(days[0].rollup.deleted.files, 2);
            assert_eq!(days[0].rollup.deleted.bytes, Some(300));
            assert_eq!(days[1].date, day2);
        }
        TrailView::Recent(_) => panic!("time lens must be Days"),
    }

    // --on day2 excludes day1.
    p.timeframe = Some(WhenValue::On(day2));
    let result = compute_trail(&conn, &p).unwrap();
    assert_eq!(decision_ids(&result.view), vec![apply]);
}

// ------------------------------------------------------------------
// Time lens pinning: extraction-touching decisions join --since/--on
// views through the same shared scoped id-union as the scope lens; day
// rollups need no new mechanics.
// ------------------------------------------------------------------

#[test]
fn time_lens_includes_extraction_touching_decision_in_right_day() {
    let conn = open_in_memory_for_test();
    let root = insert_test_root(&conn, "/a", "source", false);
    let day1 = NaiveDate::from_ymd_opt(2026, 7, 10).unwrap();
    let ts1 = local_midnight(day1) + 3600;

    // Global selection scope (no decision_scopes row) — only the
    // extraction row ties it to this view.
    let apply = insert_decision_at(&conn, "apply", ts1);
    repo::decision::replace_extractions(
        &conn,
        &[extraction_row(
            apply,
            root,
            "/a",
            "",
            5,
            Some(500),
            "/archive",
        )],
    )
    .unwrap();

    let mut p = params(vec!["/a".to_string()]);
    p.timeframe = Some(WhenValue::Since(day1));
    let result = compute_trail(&conn, &p).unwrap();
    match &result.view {
        TrailView::Days(days) => {
            assert_eq!(days.len(), 1);
            assert_eq!(days[0].date, day1);
            let ids: Vec<i64> = days[0]
                .events
                .iter()
                .filter_map(|e| match e {
                    TimelineEvent::Decision(d) => Some(d.id),
                    TimelineEvent::Note(_) => None,
                })
                .collect();
            assert_eq!(ids, vec![apply]);
        }
        TrailView::Recent(_) => panic!("time lens must be Days"),
    }
}

#[test]
fn time_lens_day_archived_rollup_reflects_apply_stamps_regardless_of_extraction_rows() {
    // Day rollups already aggregate apply's destination-row stamps
    // (present bucket => archived); extraction rows are a separate
    // projection and need no new rollup mechanics.
    let conn = open_in_memory_for_test();
    let root = insert_test_root(&conn, "/a", "source", false);
    let archive_root = insert_test_root(&conn, "/archive", "archive", false);
    let day1 = NaiveDate::from_ymd_opt(2026, 7, 10).unwrap();
    let ts1 = local_midnight(day1) + 3600;

    let apply = insert_decision_at(&conn, "apply", ts1);
    scope(&conn, apply, root, "");
    repo::decision::replace_extractions(
        &conn,
        &[extraction_row(
            apply,
            root,
            "/a",
            "",
            3,
            Some(300),
            "/archive",
        )],
    )
    .unwrap();
    // Three destination sources stamped by this decision — the rollup's
    // "archived" line comes from *these* DB stamps, an independent
    // mechanism from the extraction row's own `files` count above (which
    // happens to agree here, but is not where the rollup reads from).
    for (i, name) in ["a.jpg", "b.jpg", "c.jpg"].iter().enumerate() {
        let dest = crate::core::repo::source::insert_test_source(
            &conn,
            archive_root,
            name,
            1,
            i as i64 + 1,
            100,
            0,
        );
        conn.execute(
            "UPDATE sources SET decision_id = ?, present = 1 WHERE id = ?",
            rusqlite::params![apply, dest],
        )
        .unwrap();
    }

    let mut p = params(vec!["/a".to_string()]);
    p.timeframe = Some(WhenValue::Since(day1));
    let result = compute_trail(&conn, &p).unwrap();
    match &result.view {
        TrailView::Days(days) => {
            assert_eq!(days[0].rollup.archived.files, 3);
        }
        TrailView::Recent(_) => panic!("time lens must be Days"),
    }
}

#[test]
fn time_lens_global_view_unchanged_by_extraction_rows() {
    let conn = open_in_memory_for_test();
    let root = insert_test_root(&conn, "/a", "source", false);
    let day1 = NaiveDate::from_ymd_opt(2026, 7, 10).unwrap();
    let ts1 = local_midnight(day1) + 3600;
    let apply = insert_decision_at(&conn, "apply", ts1);
    repo::decision::replace_extractions(
        &conn,
        &[extraction_row(
            apply,
            root,
            "/a",
            "",
            5,
            Some(500),
            "/archive",
        )],
    )
    .unwrap();

    let mut p = params(Vec::new());
    p.timeframe = Some(WhenValue::Since(day1));
    let result = compute_trail(&conn, &p).unwrap();
    assert_eq!(result.unscoped_decisions, 0); // global view: never counted
    match &result.view {
        TrailView::Days(days) => assert_eq!(days.len(), 1),
        TrailView::Recent(_) => panic!("time lens must be Days"),
    }
}

#[test]
fn started_decision_appears_in_timeline() {
    let conn = open_in_memory_for_test();
    conn.execute(
        "INSERT INTO decisions (command, command_line, status, canon_version, created_at)
         VALUES ('apply', 'canon apply m.lock', 'started', 'test', 100)",
        [],
    )
    .unwrap();
    let result = compute_trail(&conn, &params(Vec::new())).unwrap();
    match &result.view {
        TrailView::Recent(events) => match &events[0] {
            TimelineEvent::Decision(d) => assert_eq!(d.status, "started"),
            TimelineEvent::Note(_) => panic!(),
        },
        TrailView::Days(_) => panic!(),
    }
}
