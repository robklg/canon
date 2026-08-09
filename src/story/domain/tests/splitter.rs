//! The emission-rule corpus: forcing, divergence, dust, pockets, the
//! slice-sum law, the agreement law.

use std::collections::HashMap;

use crate::core::domain::resolution::build_account;
use crate::domain::extraction::{DecisionExtraction, OriginDisposition};
use crate::domain::trail::DecisionFamily;
use crate::story::domain::place::{PlaceStanding, StoryParams, StoryPlace};

use super::fixtures::{
    child_paths, dinfo, excluded_src, extraction, no_dust, src, stamped, Fixture,
};

#[test]
fn empty_root_is_a_bare_root_place() {
    let fx = Fixture::new();
    let root = fx.build(&no_dust());
    assert_eq!(root.rel_path, "");
    assert!(root.undecided());
    assert!(root.standing.is_empty());
    assert!(root.children.is_empty());
    assert_eq!(root.folder_breadth, 0);
}

#[test]
fn stampless_excluded_rows_are_counted_beside_the_stamped() {
    // The no-record marker's substrate: `excluded_stampless` counts
    // exactly the excluded rows with no decision stamp (pre-provenance,
    // or recording off), row grain, folded like every standing count.
    let mut fx = Fixture::new();
    fx.present.push(excluded_src(1, "a/x.bin", None, 57));
    fx.present.push(excluded_src(2, "a/y.bin", None, 57));
    fx.present.push(crate::domain::source::Source {
        excluded: true,
        ..src(3, "a/z.bin", None)
    });
    fx.decisions
        .insert(57, dinfo(DecisionFamily::Exclude, 100, None));

    let root = fx.build(&no_dust());
    assert_eq!(root.standing.excluded, 3);
    assert_eq!(root.standing.excluded_stampless, 1);
}

#[test]
fn uniform_undecided_tree_merges_to_one_line() {
    let mut fx = Fixture::new();
    fx.covered(1, "a/x.jpg", &["/archive/media/x.jpg"]);
    fx.covered(2, "a/y.jpg", &["/archive/media/y.jpg"]);
    fx.covered(3, "b/z.jpg", &["/archive/media/z.jpg"]);
    let root = fx.build(&no_dust());
    assert!(root.children.is_empty());
    assert_eq!(root.standing.covered, 3);
    assert!(root.undecided());
    assert_eq!(root.folder_breadth, 2, "a and b merged into the root line");
    assert_eq!(
        root.covered_where.locations,
        vec![crate::story::domain::locations::LocationCount {
            path: "/archive/media".into(),
            files: 3
        }]
    );
}

#[test]
fn covered_where_divergence_splits_the_minecraft_case() {
    let mut fx = Fixture::new();
    fx.covered(
        1,
        "home/minecraft/world1/level.dat",
        &["/archive/staging-2019/worlds/level.dat"],
    );
    fx.covered(
        2,
        "home/minecraft/world1/region.mca",
        &["/archive/staging-2019/worlds/region.mca"],
    );
    fx.covered(3, "home/photos/a.jpg", &["/archive/media/a.jpg"]);
    fx.covered(4, "home/photos/b.jpg", &["/archive/media/b.jpg"]);
    let root = fx.build(&no_dust());
    // `home` blends both stories; its children diverge and surface as
    // pockets — children of the root place, not of an unemitted `home`.
    assert_eq!(child_paths(&root), vec!["home/minecraft", "home/photos"]);
    let minecraft = &root.children[0];
    assert!(minecraft.undecided());
    assert_eq!(minecraft.standing.covered, 2);
    assert_eq!(
        minecraft.covered_where.locations[0].path,
        "/archive/staging-2019/worlds"
    );
    assert_eq!(root.standing.covered, 0, "everything attributed deeper");
}

#[test]
fn standing_divergence_splits() {
    let mut fx = Fixture::new();
    fx.covered(1, "a/x.jpg", &["/archive/media/x.jpg"]);
    fx.covered(2, "a/y.jpg", &["/archive/media/y.jpg"]);
    fx.covered(3, "a/z.jpg", &["/archive/media/z.jpg"]);
    // Hashed but not archived: unresolved.
    for (i, rel) in ["b/p.raw", "b/q.raw", "b/r.raw"].iter().enumerate() {
        fx.present
            .push(src(10 + i as i64, rel, Some(500 + i as i64)));
    }
    let root = fx.build(&no_dust());
    assert_eq!(child_paths(&root), vec!["a", "b"]);
    assert_eq!(root.children[0].standing.covered, 3);
    assert_eq!(root.children[1].standing.unresolved, 3);
    assert!(root.children[1].covered_where.is_empty());
}

#[test]
fn within_tolerance_children_merge() {
    let mut fx = Fixture::new();
    fx.covered(1, "a/x.jpg", &["/archive/media/x.jpg"]);
    fx.present.push(src(2, "a/y.raw", Some(501)));
    fx.covered(3, "b/z.jpg", &["/archive/media/z.jpg"]);
    fx.present.push(src(4, "b/w.raw", Some(502)));
    let root = fx.build(&no_dust());
    assert!(root.children.is_empty());
    assert_eq!(root.standing.covered, 2);
    assert_eq!(root.standing.unresolved, 2);
}

#[test]
fn dust_floor_lifts_fragments() {
    let mut fx = Fixture::new();
    for i in 0..100 {
        fx.covered(i, &format!("big/f{i}.jpg"), &["/archive/media/f.jpg"]);
    }
    // Strongly divergent but tiny: five unresolved files under the
    // default floors lift into the root line instead of splitting.
    for i in 0..5 {
        fx.present
            .push(src(200 + i, &format!("tiny/t{i}.raw"), Some(600 + i)));
    }
    let root = fx.build(&StoryParams::default());
    assert!(root.children.is_empty());
    assert_eq!(root.standing.covered, 100);
    assert_eq!(root.standing.unresolved, 5);
}

#[test]
fn a_note_forces_a_place() {
    let mut fx = Fixture::new();
    fx.covered(1, "a/x.jpg", &["/archive/media/x.jpg"]);
    fx.covered(2, "b/y.jpg", &["/archive/media/y.jpg"]);
    fx.notes.push(super::fixtures::note_at(
        7,
        "b",
        "beautiful pictures, still need a home",
    ));
    let root = fx.build(&no_dust());
    assert_eq!(child_paths(&root), vec!["b"]);
    let b = &root.children[0];
    assert_eq!(b.notes.len(), 1);
    assert_eq!(b.notes[0].text, "beautiful pictures, still need a home");
    assert_eq!(b.standing.covered, 1);
    assert!(b.undecided(), "a note is testimony, not a decision stamp");
}

#[test]
fn a_reasoned_exclusion_forces_its_place() {
    let mut fx = Fixture::new();
    fx.present.push(excluded_src(1, "old/setup1.exe", None, 57));
    fx.present.push(excluded_src(2, "old/setup2.exe", None, 57));
    fx.decisions.insert(
        57,
        dinfo(DecisionFamily::Exclude, 100, Some("installer junk")),
    );
    let root = fx.build(&no_dust());
    // The care anchor: the reasoned decision's dirs LCA is `old`.
    assert_eq!(child_paths(&root), vec!["old"]);
    let old = &root.children[0];
    assert!(!old.undecided());
    assert_eq!(old.acts.len(), 1);
    assert_eq!(old.acts[0].transition, "excluded");
    assert_eq!(old.acts[0].files, 2);
    assert_eq!(old.standing.excluded, 2);
    let reasons = old.acts[0].reason_summary();
    assert_eq!(reasons.reasons[0].0, "installer junk");
}

#[test]
fn a_reasonless_uniform_exclusion_folds_to_the_root() {
    let mut fx = Fixture::new();
    fx.present.push(excluded_src(1, "old/setup1.exe", None, 57));
    fx.present.push(excluded_src(2, "old/setup2.exe", None, 57));
    fx.decisions
        .insert(57, dinfo(DecisionFamily::Exclude, 100, None));
    let root = fx.build(&no_dust());
    // No care, no divergence: the slice folds to the root line.
    assert!(root.children.is_empty());
    assert_eq!(root.acts.len(), 1);
    assert_eq!(root.acts[0].transition, "excluded");
    assert_eq!(root.acts[0].files, 2);
    assert_eq!(root.standing.excluded, 2);
}

#[test]
fn nested_reasoned_acts_render_as_containment() {
    let mut fx = Fixture::new();
    fx.extractions.push(extraction(
        42,
        "pictures/italy",
        640,
        "/archive/media/2016-italy",
    ));
    fx.extractions
        .push(extraction(51, "pictures", 4102, "/archive/media/rest"));
    fx.decisions.insert(
        42,
        dinfo(DecisionFamily::Archive, 100, Some("the Italy trip")),
    );
    fx.decisions.insert(
        51,
        dinfo(
            DecisionFamily::Archive,
            200,
            Some("rest of the pictures, mechanical"),
        ),
    );
    fx.covered(
        9,
        "pictures/italy/leftover.jpg",
        &["/archive/media/2016-italy/leftover.jpg"],
    );
    let root = fx.build(&no_dust());
    // The onion: both reasoned acts force their care anchors, which are
    // their origin dirs — the layered passes read as layers.
    assert_eq!(child_paths(&root), vec!["pictures"]);
    let pictures = &root.children[0];
    assert_eq!(pictures.acts.len(), 1);
    assert_eq!(pictures.acts[0].files, 4102);
    assert_eq!(
        pictures.acts[0].destination.locations[0].path,
        "/archive/media/rest"
    );
    assert_eq!(child_paths(pictures), vec!["pictures/italy"]);
    let italy = &pictures.children[0];
    assert_eq!(italy.acts[0].files, 640);
    assert_eq!(italy.acts[0].moved, Some(640));
    assert_eq!(
        italy.standing.covered, 1,
        "deepest-match: the leftover belongs to italy, not pictures"
    );
    let reasons = italy.acts[0].reason_summary();
    assert_eq!(reasons.reasons[0].0, "the Italy trip");
}

#[test]
fn a_reasoned_decision_forces_the_lca_of_its_dirs() {
    let mut fx = Fixture::new();
    fx.present.push(excluded_src(1, "a/b/x.tmp", None, 60));
    fx.present.push(excluded_src(2, "a/c/y.tmp", None, 60));
    fx.decisions
        .insert(60, dinfo(DecisionFamily::Exclude, 100, Some("temp litter")));
    let root = fx.build(&no_dust());
    // Care was expressed at the decision's grain: one forced place at
    // the dirs' LCA, the per-dir slices merged into one line there.
    assert_eq!(child_paths(&root), vec!["a"]);
    let a = &root.children[0];
    assert!(a.children.is_empty());
    assert_eq!(a.acts.len(), 1);
    assert_eq!(a.acts[0].files, 2);
    assert_eq!(
        a.acts[0].decisions.len(),
        1,
        "slices merged, not duplicated"
    );
}

#[test]
fn an_observed_deletion_lands_where_it_happened() {
    let mut fx = Fixture::new();
    fx.absent.push(stamped(1, "gone/a.jpg", None, 70));
    fx.absent.push(stamped(2, "gone/b.jpg", None, 70));
    fx.decisions
        .insert(70, dinfo(DecisionFamily::Observe, 100, None));
    // Divergent surroundings: the loss reads as `gone`'s story, not the
    // root's (deleted rows join the story population).
    fx.covered(10, "kept/a.jpg", &["/archive/media/a.jpg"]);
    fx.covered(11, "kept/b.jpg", &["/archive/media/b.jpg"]);
    let root = fx.build(&no_dust());
    assert_eq!(child_paths(&root), vec!["gone", "kept"]);
    let gone = &root.children[0];
    assert_eq!(gone.acts.len(), 1);
    assert_eq!(gone.acts[0].transition, "deleted");
    assert!(gone.acts[0].observed);
    assert_eq!(gone.acts[0].files, 2);
    assert_eq!(gone.standing.missing_unexplained, 0);
}

#[test]
fn slice_sum_law_reconciles_through_any_fold() {
    // One apply drew from three dirs; the emission shape must never
    // change the totals. Distinct reasons keep groups one-decision so
    // per-decision files are readable from the built tree.
    let mut fx = Fixture::new();
    fx.extractions
        .push(extraction(42, "a", 5, "/archive/media/set"));
    fx.extractions
        .push(extraction(42, "a/b", 3, "/archive/media/set"));
    fx.extractions
        .push(extraction(42, "c", 2, "/archive/media/set"));
    fx.decisions.insert(
        42,
        dinfo(DecisionFamily::Archive, 100, Some("the whole set")),
    );
    // Divergent standings force `a` out while `c` folds to the root.
    for i in 0..20 {
        fx.covered(100 + i, &format!("a/k{i}.jpg"), &["/archive/media/k.jpg"]);
    }
    for i in 0..20 {
        fx.present
            .push(src(200 + i, &format!("z/u{i}.raw"), Some(900 + i)));
    }
    let root = fx.build(&no_dust());

    fn slice_files(place: &StoryPlace, id: i64, sum: &mut i64) {
        for group in &place.acts {
            if group.decisions.iter().any(|d| d.id == id) {
                *sum += group.files;
            }
        }
        for child in &place.children {
            slice_files(child, id, sum);
        }
    }
    let mut total = 0;
    slice_files(&root, 42, &mut total);
    assert_eq!(total, 10, "slices reconcile exactly to the row totals");

    // And the shape: `a` split out (care anchor at `a` — the LCA of the
    // decision's dirs — plus covered divergence), holding its merged
    // slice; the `c` slice folded to the root line.
    let a = root
        .children
        .iter()
        .find(|p| p.rel_path == "a")
        .expect("a splits");
    let a_slice: i64 = a.acts.iter().map(|g| g.files).sum();
    assert_eq!(a_slice, 8, "a's dirs merged into one slice");
    let root_slice: i64 = root.acts.iter().map(|g| g.files).sum();
    assert_eq!(root_slice, 2, "c's slice folded to the root");
}

#[test]
fn no_decision_here_never_lies() {
    // The `.cache` contradiction: a child uniformly excluded by a
    // decision stamped in its own dirs must either fold into a register
    // that shows the act, or show its slice — never render undecided
    // beside excluded standing.
    let mut fx = Fixture::new();
    for i in 0..20 {
        fx.present
            .push(excluded_src(i, &format!("home/.cache/c{i}.tmp"), None, 114));
    }
    fx.decisions
        .insert(114, dinfo(DecisionFamily::Exclude, 100, None));
    for i in 0..20 {
        fx.covered(
            100 + i,
            &format!("home/pics/p{i}.jpg"),
            &["/archive/m/p.jpg"],
        );
    }
    let root = fx.build(&no_dust());

    fn assert_honest(place: &StoryPlace) {
        if place.undecided() {
            assert_eq!(
                place.standing.excluded, 0,
                "{}: no decision here beside excluded standing",
                place.rel_path
            );
        }
        for child in &place.children {
            assert_honest(child);
        }
    }
    assert_honest(&root);
}

#[test]
fn reasonless_mechanical_exclusions_fold_ids_enumerated() {
    // The dotfolder wall: three sibling folders excluded by three
    // different reasonless decisions tell the same story — one register,
    // every id enumerated.
    let mut fx = Fixture::new();
    for i in 0..4 {
        fx.present
            .push(excluded_src(i, &format!("home/.cache/c{i}.tmp"), None, 114));
    }
    for i in 0..3 {
        fx.present.push(excluded_src(
            10 + i,
            &format!("home/.opera/o{i}.dat"),
            None,
            115,
        ));
    }
    for i in 0..2 {
        fx.present.push(excluded_src(
            20 + i,
            &format!("home/.purple/p{i}.xml"),
            None,
            116,
        ));
    }
    for id in [114, 115, 116] {
        fx.decisions
            .insert(id, dinfo(DecisionFamily::Exclude, id * 10, None));
    }
    // Divergent context so `home` earns its own line.
    for i in 0..10 {
        fx.covered(100 + i, &format!("pics/p{i}.jpg"), &["/archive/m/p.jpg"]);
    }
    let root = fx.build(&no_dust());
    // Nothing about `home` is second-guessable (uniformly excluded, one
    // shared signature), so the wall folds all the way into the root's
    // register; only the covered context splits out.
    assert_eq!(child_paths(&root), vec!["pics"]);
    assert_eq!(root.acts.len(), 1, "one shared register");
    let group = &root.acts[0];
    assert_eq!(group.files, 9);
    let ids: Vec<i64> = group.decisions.iter().map(|d| d.id).collect();
    assert_eq!(ids, vec![114, 115, 116], "every id enumerated");
    assert_eq!(group.reason_summary().without_reason, vec![114, 115, 116]);
}

#[test]
fn same_story_children_fold_against_the_residual_context() {
    // A reasoned sibling forced out of the fold (.tvtime) must not
    // dilute the context: the dotfolders' story is compared against
    // what they would fold into — the parent's residual register —
    // not the whole subtree (the home-dir over-emission).
    let mut fx = Fixture::new();
    for i in 0..5 {
        fx.present
            .push(excluded_src(i, &format!("old-home/k{i}.dat"), None, 98));
    }
    for i in 0..20 {
        fx.present.push(excluded_src(
            10 + i,
            &format!("old-home/.cache/c{i}"),
            None,
            98,
        ));
    }
    for i in 0..15 {
        fx.present.push(excluded_src(
            40 + i,
            &format!("old-home/.compiz/z{i}"),
            None,
            98,
        ));
    }
    fx.decisions.insert(
        98,
        dinfo(
            DecisionFamily::Exclude,
            100,
            Some("nothing important remains here"),
        ),
    );
    for i in 0..6 {
        fx.present.push(excluded_src(
            60 + i,
            &format!("old-home/.tvtime/t{i}"),
            None,
            97,
        ));
    }
    fx.decisions.insert(
        97,
        dinfo(
            DecisionFamily::Exclude,
            90,
            Some("channels worth remembering"),
        ),
    );
    let root = fx.build(&no_dust());
    let home = root
        .children
        .iter()
        .find(|p| p.rel_path == "old-home")
        .expect("the care anchor forces old-home");
    assert_eq!(
        child_paths(home),
        vec!["old-home/.tvtime"],
        "only the differently-reasoned act splits; same-story dotfolders fold"
    );
    let register: i64 = home.acts.iter().map(|g| g.files).sum();
    assert_eq!(
        register, 40,
        "the folded slices land in old-home's register"
    );
}

#[test]
fn a_note_on_a_file_gathers_that_files_fate() {
    // The noted-script finding: a noted file is its own place, and
    // that place carries the file's standing and act slice — intent and
    // fate side by side, discrepancies visible without the trail.
    let mut fx = Fixture::new();
    fx.present
        .push(excluded_src(1, "usr/local/bin/keepme.sh", None, 153));
    for i in 0..10 {
        fx.present.push(excluded_src(
            10 + i,
            &format!("usr/local/bin/b{i}"),
            None,
            153,
        ));
    }
    fx.decisions.insert(
        153,
        dinfo(
            DecisionFamily::Exclude,
            100,
            Some("non-package files are not important"),
        ),
    );
    fx.notes.push(super::fixtures::note_at(
        7,
        "usr/local/bin/keepme.sh",
        "important script",
    ));
    let root = fx.build(&no_dust());

    fn find<'a>(place: &'a StoryPlace, rel: &str) -> Option<&'a StoryPlace> {
        if place.rel_path == rel {
            return Some(place);
        }
        place.children.iter().find_map(|c| find(c, rel))
    }
    let script = find(&root, "usr/local/bin/keepme.sh").expect("the noted file is its own place");
    assert_eq!(script.standing.excluded, 1, "the file's standing sits here");
    assert_eq!(script.acts.len(), 1, "and its act slice");
    assert_eq!(script.acts[0].transition, "excluded");
    assert_eq!(script.acts[0].files, 1);
    assert_eq!(script.notes[0].text, "important script");
}

#[test]
fn a_note_on_a_dir_gathers_the_subtree_it_alone_claims() {
    // With no deeper place forced, a noted dir is the deepest emitted
    // ancestor of everything beneath it — standings and act slices from
    // the whole subtree render at the noted place.
    let mut fx = Fixture::new();
    fx.present
        .push(excluded_src(1, "keep/sub/tool.sh", None, 153));
    fx.decisions
        .insert(153, dinfo(DecisionFamily::Exclude, 100, None));
    fx.notes
        .push(super::fixtures::note_at(7, "keep", "the tool lives here"));
    // Divergent surroundings so the root context differs.
    for i in 0..10 {
        fx.covered(100 + i, &format!("pics/p{i}.jpg"), &["/archive/m/p.jpg"]);
    }
    let root = fx.build(&no_dust());
    let keep = root
        .children
        .iter()
        .find(|p| p.rel_path == "keep")
        .expect("the note forces keep");
    assert!(keep.children.is_empty());
    assert_eq!(
        keep.standing.excluded, 1,
        "the subtree's standing gathers here"
    );
    assert_eq!(keep.acts.len(), 1, "and its act slice");
    assert_eq!(keep.acts[0].files, 1);
}

#[test]
fn disjoint_reasoned_footprints_surface_as_crossing_boundaries() {
    // Two reasoned exclusions with coherent, disjoint footprints: the
    // care anchors land at each decision's own dirs-LCA — the map shows
    // exactly the boundary where the decisions cross, each place with
    // its reason.
    let mut fx = Fixture::new();
    for i in 0..20 {
        fx.present
            .push(excluded_src(i, &format!("usr/sgml/4.0/f{i}"), None, 131));
    }
    for i in 0..20 {
        fx.present.push(excluded_src(
            30 + i,
            &format!("usr/sgml/4.1/g{i}"),
            None,
            153,
        ));
    }
    fx.decisions.insert(
        131,
        dinfo(
            DecisionFamily::Exclude,
            100,
            Some("only the script matters"),
        ),
    );
    fx.decisions.insert(
        153,
        dinfo(
            DecisionFamily::Exclude,
            200,
            Some("non-package files checked"),
        ),
    );
    let root = fx.build(&no_dust());
    assert_eq!(
        child_paths(&root),
        vec!["usr/sgml/4.0", "usr/sgml/4.1"],
        "each decision's footprint is its own boundary"
    );
    let r40 = root.children[0].acts[0].reason_summary();
    assert_eq!(r40.reasons[0].0, "only the script matters");
    let r41 = root.children[1].acts[0].reason_summary();
    assert_eq!(r41.reasons[0].0, "non-package files checked");
}

#[test]
fn interleaved_scattered_acts_share_one_register() {
    // The same two decisions with scattered, interleaved footprints
    // (the data/usr sgml wall): there is no clean crossing line — both
    // care anchors land on the shared region, dirs holding a slice of
    // only one decision tell the same what/where story and fold, and
    // both reasons render in the one register. The why is deliberately
    // not a divergence axis; it surfaces at each decision's own grain
    // via the care anchor.
    let mut fx2 = Fixture::new();
    for (i, dir) in ["usr/a", "usr/b", "usr/c"].iter().enumerate() {
        for j in 0..10 {
            fx2.present.push(excluded_src(
                (i * 10 + j) as i64,
                &format!("{dir}/f{j}"),
                None,
                131,
            ));
        }
        for j in 0..10 {
            fx2.present.push(excluded_src(
                (100 + i * 10 + j) as i64,
                &format!("{dir}/sub/g{j}"),
                None,
                153,
            ));
        }
    }
    fx2.decisions.insert(
        131,
        dinfo(
            DecisionFamily::Exclude,
            100,
            Some("only the script matters"),
        ),
    );
    fx2.decisions.insert(
        153,
        dinfo(
            DecisionFamily::Exclude,
            200,
            Some("non-package files checked"),
        ),
    );
    let root2 = fx2.build(&no_dust());
    // Both decisions' dirs LCA to `usr` — one forced place, no
    // fragmentation beneath, both reasons in its register.
    assert_eq!(child_paths(&root2), vec!["usr"]);
    let usr = &root2.children[0];
    assert!(usr.children.is_empty(), "interleaved slices fold");
    assert_eq!(usr.acts.len(), 1, "one shared what/where register");
    let summary = usr.acts[0].reason_summary();
    assert_eq!(summary.reasons.len(), 2, "both whys enumerated");
}

#[test]
fn reasoned_act_forces_below_dust() {
    // `data/.deb`: one reasoned file far under the floors still
    // surfaces — recorded care earns a line, floors notwithstanding.
    let mut fx = Fixture::new();
    fx.present
        .push(excluded_src(1, "data/.deb/stray.deb", None, 161));
    fx.decisions
        .insert(161, dinfo(DecisionFamily::Exclude, 100, Some("stray file")));
    for i in 0..100 {
        fx.present
            .push(excluded_src(10 + i, &format!("data/bin/b{i}"), None, 157));
    }
    fx.decisions
        .insert(157, dinfo(DecisionFamily::Exclude, 90, None));
    let root = fx.build(&StoryParams::default());
    let deb = root
        .children
        .iter()
        .find(|p| p.rel_path == "data/.deb")
        .expect("the reasoned act surfaces despite the dust floors");
    assert_eq!(deb.acts[0].files, 1);
    assert_eq!(deb.acts[0].reason_summary().reasons[0].0, "stray file");
}

#[test]
fn mirror_destination_apply_does_not_fragment() {
    // A reasonless export whose per-dir destinations mirror the origins:
    // the signature's destination answer is decision-level, so the
    // slices share one signature and fold to one line.
    let mut fx = Fixture::new();
    fx.extractions
        .push(extraction(174, "home/a", 2, "/archive/export/home/a"));
    fx.extractions
        .push(extraction(174, "home/b", 3, "/archive/export/home/b"));
    fx.extractions
        .push(extraction(174, "etc/x", 1, "/archive/export/etc/x"));
    fx.decisions
        .insert(174, dinfo(DecisionFamily::Archive, 100, None));
    let root = fx.build(&no_dust());
    assert!(root.children.is_empty(), "uniform story, no fragmentation");
    assert_eq!(root.acts.len(), 1);
    assert_eq!(root.acts[0].files, 6);
    assert_eq!(
        root.acts[0].destination.locations[0].path, "/archive/export",
        "the pooled destination collapses to the common answer"
    );
}

#[test]
fn tombstone_dirs_carry_slices() {
    // An object exclusion stamps an absent sharer in another dir: the
    // act register is whole-history, so that dir narrates the act while
    // its standing stays a missing fact.
    let mut fx = Fixture::new();
    fx.present.push(excluded_src(1, "keep/x.jpg", None, 120));
    fx.absent.push(stamped(2, "gone2/x.jpg", None, 120));
    fx.decisions
        .insert(120, dinfo(DecisionFamily::Exclude, 100, None));
    let root = fx.build(&no_dust());
    let gone2 = root
        .children
        .iter()
        .find(|p| p.rel_path == "gone2")
        .expect("the tombstone dir diverges");
    assert_eq!(gone2.acts.len(), 1, "the tombstone carries the slice");
    assert_eq!(gone2.acts[0].transition, "excluded");
    assert_eq!(gone2.acts[0].files, 1);
    assert_eq!(gone2.standing.excluded, 0, "standing stays present-tense");
    assert_eq!(gone2.standing.missing_unexplained, 1);
}

#[test]
fn emptied_place_dust_uses_act_weight() {
    // The emptied-place shape: a move-mode apply left zero present
    // files, but the act weight carries the place past the floors; a
    // sibling one-file slice under both floors folds instead.
    let mut fx = Fixture::new();
    fx.extractions.push(DecisionExtraction {
        decision_id: 174,
        root_id: 1,
        root_path: "/root".to_string(),
        rel_prefix: "some/important/dir".to_string(),
        files: 2,
        bytes: Some(3_100_000_000),
        destination_root_id: Some(2),
        destination_path: "/archive/export/some/important/dir".to_string(),
        disposition: Some(OriginDisposition::Relocated),
    });
    fx.extractions.push(DecisionExtraction {
        decision_id: 174,
        root_id: 1,
        root_path: "/root".to_string(),
        rel_prefix: "tiny".to_string(),
        files: 1,
        bytes: Some(100),
        destination_root_id: Some(2),
        destination_path: "/archive/export/tiny".to_string(),
        disposition: Some(OriginDisposition::Relocated),
    });
    fx.decisions
        .insert(174, dinfo(DecisionFamily::Archive, 100, None));
    for i in 0..50 {
        fx.present
            .push(excluded_src(10 + i, &format!("data/d{i}"), None, 100));
    }
    fx.decisions
        .insert(100, dinfo(DecisionFamily::Exclude, 50, None));
    let root = fx.build(&StoryParams::default());
    // The boundary settles at the widest honest node of the emptied
    // chain (`some`); in real data a note is what would force the deep
    // dir itself.
    let widest = root
        .children
        .iter()
        .find(|p| p.rel_path == "some")
        .expect("act weight carries the emptied place past the floors");
    assert_eq!(widest.acts[0].files, 2);
    assert!(
        !root.children.iter().any(|p| p.rel_path == "tiny"),
        "a dust-sized slice folds"
    );
    let root_archived: i64 = root
        .acts
        .iter()
        .filter(|g| g.transition == "archived")
        .map(|g| g.files)
        .sum();
    assert_eq!(
        root_archived, 1,
        "the folded slice lands in the root register"
    );
}

#[test]
fn missing_without_a_stamp_is_standing_not_an_act() {
    let mut fx = Fixture::new();
    fx.absent.push(src(1, "lost/x.jpg", None));
    let root = fx.build(&no_dust());
    assert!(root.children.is_empty());
    assert!(root.undecided());
    assert_eq!(root.standing.missing_unexplained, 1);
}

#[test]
fn copies_count_per_location_never_per_item() {
    let mut fx = Fixture::new();
    // Two copies in one archive dir count that dir once for this file.
    fx.covered(
        1,
        "m/x.jpg",
        &[
            "/archive/a/x.jpg",
            "/archive/a/x-copy.jpg",
            "/archive/b/x.jpg",
        ],
    );
    let root = fx.build(&no_dust());
    let paths: Vec<(&str, i64)> = root
        .covered_where
        .locations
        .iter()
        .map(|l| (l.path.as_str(), l.files))
        .collect();
    assert_eq!(paths, vec![("/archive/a", 1), ("/archive/b", 1)]);
}

#[test]
fn agreement_law_place_sums_fold_to_the_account() {
    let mut fx = Fixture::new();
    fx.covered(1, "a/x.jpg", &["/archive/media/x.jpg"]);
    fx.covered(2, "a/y.jpg", &["/archive/media/y.jpg"]);
    fx.covered(3, "a/z.jpg", &["/archive/media/z.jpg"]);
    fx.present.push(excluded_src(10, "b/i.exe", None, 57));
    fx.present.push(excluded_src(11, "b/j.exe", None, 57));
    fx.present.push(src(12, "c/u.raw", Some(700)));
    fx.present.push(src(13, "c/v.raw", None));
    fx.absent.push(stamped(20, "gone/a.jpg", None, 70));
    fx.absent.push(stamped(21, "gone/b.jpg", None, 70));
    fx.absent.push(src(22, "lost/w.jpg", None));
    fx.extractions
        .push(extraction(42, "a", 5, "/archive/media"));
    fx.decisions
        .insert(57, dinfo(DecisionFamily::Exclude, 100, None));
    fx.decisions
        .insert(70, dinfo(DecisionFamily::Observe, 200, None));
    fx.decisions
        .insert(42, dinfo(DecisionFamily::Archive, 300, None));
    let root = fx.build(&no_dust());

    fn fold(place: &StoryPlace, sum: &mut PlaceStanding) {
        sum.archived += place.standing.archived;
        sum.covered += place.standing.covered;
        sum.contentless += place.standing.contentless;
        sum.excluded += place.standing.excluded;
        sum.unresolved += place.standing.unresolved;
        sum.unhashed_unresolved += place.standing.unhashed_unresolved;
        sum.missing_unexplained += place.standing.missing_unexplained;
        for child in &place.children {
            fold(child, sum);
        }
    }
    let mut sum = PlaceStanding::default();
    fold(&root, &mut sum);

    let stamp_families: HashMap<i64, DecisionFamily> = fx
        .decisions
        .iter()
        .map(|(id, info)| (*id, info.family))
        .collect();
    let account = build_account(
        &fx.present,
        &fx.absent,
        &fx.archived,
        &fx.archived_from_here,
        &fx.extractions,
        &stamp_families,
    );
    assert_eq!(sum.archived, account.archived_standing);
    assert_eq!(sum.covered, account.covered);
    assert_eq!(sum.contentless, account.contentless);
    assert_eq!(sum.excluded, account.excluded);
    assert_eq!(sum.unresolved, account.unresolved);
    assert_eq!(sum.unhashed_unresolved, account.unhashed_unresolved);
    assert_eq!(sum.missing_unexplained, account.unexplained_missing);
}
