//! Reason-site tests (the once-rule, through the real splitter) and the
//! standing-coincidence law confirmed end-to-end.

use crate::core::domain::fate::DecisionFamily;
use crate::story::domain::place::assign_reason_sites;

use super::fixtures::{child, dinfo, excluded_src, no_dust, note_at, Fixture};

#[test]
fn the_first_slice_in_reading_order_carries_the_reason() {
    // #50's slices weigh 2 at `a` and 3 at `b` — the reason still
    // sites at `a`, the first place in pre-order (= render order): the
    // reader meets the full reason the first time they meet the id,
    // and the wider later slice cites backward.
    let mut fx = Fixture::new();
    for i in 0..2 {
        fx.present
            .push(excluded_src(1 + i, &format!("a/f{i}"), None, 50));
    }
    for i in 0..3 {
        fx.present
            .push(excluded_src(10 + i, &format!("b/f{i}"), None, 50));
    }
    fx.decisions
        .insert(50, dinfo(DecisionFamily::Exclude, 100, Some("junk")));
    fx.notes.push(note_at(1, "a", "watching"));
    fx.notes.push(note_at(2, "b", "watching"));
    let mut root = fx.build(&no_dust());
    assign_reason_sites(&mut root);

    let a = child(&root, "a");
    let b = child(&root, "b");
    assert!(a.acts[0].decisions[0].reason_here, "first in reading order");
    assert!(!b.acts[0].decisions[0].reason_here, "wider, but later");
    let summary = b.acts[0].reason_summary();
    assert!(summary.reasons.is_empty());
    assert_eq!(summary.cited, vec![50]);
    assert!(
        summary.without_reason.is_empty(),
        "cited is never without-reason"
    );
}

#[test]
fn reason_site_is_an_emitted_slice() {
    // #70's raw dirs a/x and a/y fold into `a`: the site is the first
    // EMITTED slice in reading order — the post-pass runs over the
    // built tree, never over raw atoms.
    let mut fx = Fixture::new();
    for i in 0..3 {
        fx.present
            .push(excluded_src(1 + i, &format!("a/x/f{i}"), None, 70));
    }
    for i in 0..2 {
        fx.present
            .push(excluded_src(10 + i, &format!("a/y/f{i}"), None, 70));
    }
    for i in 0..4 {
        fx.present
            .push(excluded_src(20 + i, &format!("b/f{i}"), None, 70));
    }
    fx.decisions
        .insert(70, dinfo(DecisionFamily::Exclude, 100, Some("old builds")));
    fx.notes.push(note_at(1, "a", "watching"));
    fx.notes.push(note_at(2, "b", "watching"));
    let mut root = fx.build(&no_dust());
    assign_reason_sites(&mut root);

    let a = child(&root, "a");
    let b = child(&root, "b");
    assert_eq!(a.acts[0].files, 5, "a/x and a/y folded into one slice");
    assert!(a.acts[0].decisions[0].reason_here);
    assert!(!b.acts[0].decisions[0].reason_here);
}

#[test]
fn coincidence_holds_through_the_real_splitter() {
    // Two stamped exclusions and nothing else: the built place's
    // excluded standing is exactly the act's present share.
    let mut fx = Fixture::new();
    fx.present.push(excluded_src(1, "old/a.exe", None, 57));
    fx.present.push(excluded_src(2, "old/b.exe", None, 57));
    fx.decisions
        .insert(57, dinfo(DecisionFamily::Exclude, 100, None));
    let root = fx.build(&no_dust());
    assert_eq!(root.standing.excluded, 2);
    assert_eq!(root.acts[0].present_files, 2);
    assert!(root.standing_coincides());
}
