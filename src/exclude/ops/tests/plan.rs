use crate::core::ops::scope::classify_all;
use crate::core::testing::{
    insert_object, insert_root, insert_source, insert_source_excluded, insert_source_with_size,
    setup_test_db,
};
use crate::exclude::ops::plan::{plan_clear, plan_duplicates, plan_set, plan_set_objects};

use super::fixtures::{
    make_clear_params, make_duplicates_params, make_set_objects_params, make_set_params,
};

// =========================================================================
// plan_set() tests
// =========================================================================

#[test]
fn test_plan_set_empty_when_no_sources() {
    let mut conn = setup_test_db();
    let _root = insert_root(&conn, "/photos", "source", false);

    let plan = plan_set(&mut conn, &make_set_params(vec![])).unwrap();

    assert!(plan.source_ids().is_empty());
    assert_eq!(plan.root_count, 0);
    assert_eq!(plan.not_archived_count, 0);
}

#[test]
fn test_plan_set_excludes_already_excluded() {
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    let id1 = insert_source(&conn, root, "a.jpg", None);
    let _id2 = insert_source_excluded(&conn, root, "b.jpg", None);

    let plan = plan_set(&mut conn, &make_set_params(vec![])).unwrap();

    assert_eq!(plan.source_ids(), vec![id1]);
}

#[test]
fn test_plan_set_skips_object_level_excluded() {
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    let excluded_obj = insert_object(&conn, "abc123", true);
    let _id = insert_source(&conn, root, "a.jpg", Some(excluded_obj));

    let plan = plan_set(&mut conn, &make_set_params(vec![])).unwrap();

    // Object-level excluded sources are filtered out by select_sources()
    assert!(plan.source_ids().is_empty());
}

#[test]
fn test_plan_set_counts_roots() {
    let mut conn = setup_test_db();
    let root1 = insert_root(&conn, "/root1", "source", false);
    let root2 = insert_root(&conn, "/root2", "source", false);
    insert_source(&conn, root1, "a.jpg", None);
    insert_source(&conn, root2, "b.jpg", None);

    let plan = plan_set(&mut conn, &make_set_params(vec![])).unwrap();

    assert_eq!(plan.root_count, 2);
}

#[test]
fn test_plan_set_archive_coverage() {
    let mut conn = setup_test_db();
    let source_root = insert_root(&conn, "/source", "source", false);
    let archive_root = insert_root(&conn, "/archive", "archive", false);

    // Object that IS archived
    let archived_obj = insert_object(&conn, "archived_hash", false);
    insert_source(&conn, archive_root, "copy.jpg", Some(archived_obj));
    insert_source(&conn, source_root, "file1.jpg", Some(archived_obj));

    // Object that is NOT archived
    let unarchived_obj = insert_object(&conn, "unarchived_hash", false);
    insert_source(&conn, source_root, "file2.jpg", Some(unarchived_obj));

    let plan = plan_set(&mut conn, &make_set_params(vec![])).unwrap();

    assert_eq!(plan.source_ids().len(), 2);
    assert_eq!(plan.not_archived_count, 1, "Only the unarchived source");
}

#[test]
fn test_plan_set_unhashed_not_archived() {
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/source", "source", false);
    insert_source(&conn, root, "unhashed.jpg", None);

    let plan = plan_set(&mut conn, &make_set_params(vec![])).unwrap();

    assert_eq!(
        plan.not_archived_count, 1,
        "Unhashed counts as not archived"
    );
}

#[test]
fn test_plan_set_includes_paths() {
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    insert_source(&conn, root, "subdir/a.jpg", None);

    let plan = plan_set(&mut conn, &make_set_params(vec![])).unwrap();

    assert_eq!(plan.paths(), vec!["/photos/subdir/a.jpg"]);
}

#[test]
fn test_plan_set_respects_scope() {
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    let in_scope = insert_source(&conn, root, "2024/a.jpg", None);
    let _out_scope = insert_source(&conn, root, "2023/b.jpg", None);

    let scopes = classify_all(&["/photos/2024".to_string()]);
    let plan = plan_set(&mut conn, &make_set_params(scopes)).unwrap();

    assert_eq!(plan.source_ids(), vec![in_scope]);
}

// =========================================================================
// plan items carry receipt data + accessor regression
// =========================================================================

#[test]
fn test_plan_set_items_carry_receipt_data() {
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    let obj = insert_object(&conn, "abc123hash", false);
    let id = insert_source(&conn, root, "a.jpg", Some(obj));
    // A prior decision on the source becomes previous_decision_id.
    conn.execute(
        "UPDATE sources SET decision_id = 7 WHERE id = ?",
        rusqlite::params![id],
    )
    .unwrap();

    let plan = plan_set(&mut conn, &make_set_params(vec![])).unwrap();

    assert_eq!(plan.items.len(), 1);
    let it = &plan.items[0];
    assert_eq!(it.source_id, id);
    assert_eq!(it.hash.as_deref(), Some("sha256:abc123hash"));
    assert_eq!(it.size, 1000);
    assert_eq!(it.mtime, 1704067200);
    assert_eq!(it.previous_decision_id, Some(7));
}

#[test]
fn test_plan_set_item_unhashed_has_no_hash() {
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    insert_source(&conn, root, "a.jpg", None);

    let plan = plan_set(&mut conn, &make_set_params(vec![])).unwrap();

    assert_eq!(plan.items[0].hash, None);
    assert_eq!(plan.items[0].previous_decision_id, None);
}

#[test]
fn test_plan_clear_items_carry_receipt_data() {
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    let obj = insert_object(&conn, "clearhashval", false);
    let id = insert_source_excluded(&conn, root, "sub/a.jpg", Some(obj));
    conn.execute(
        "UPDATE sources SET decision_id = 3 WHERE id = ?",
        rusqlite::params![id],
    )
    .unwrap();

    let plan = plan_clear(&mut conn, &make_clear_params(vec![])).unwrap();

    assert_eq!(plan.items.len(), 1);
    let it = &plan.items[0];
    assert_eq!(it.hash.as_deref(), Some("sha256:clearhashval"));
    assert_eq!(it.previous_decision_id, Some(3));
    assert_eq!(it.path(), "/photos/sub/a.jpg");
}

#[test]
fn test_plan_accessors_derive_from_items() {
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    let id1 = insert_source(&conn, root, "a.jpg", None);
    let id2 = insert_source(&conn, root, "sub/b.jpg", None);

    let plan = plan_set(&mut conn, &make_set_params(vec![])).unwrap();

    // Accessors derive, in order, from items (the regression guard for the
    // old parallel source_ids/paths vectors).
    let expected_ids: Vec<i64> = plan.items.iter().map(|i| i.source_id).collect();
    let expected_paths: Vec<String> = plan.items.iter().map(|i| i.path()).collect();
    assert_eq!(plan.source_ids(), expected_ids);
    assert_eq!(plan.paths(), expected_paths);
    assert!(plan.source_ids().contains(&id1));
    assert!(plan.source_ids().contains(&id2));
}

// =========================================================================
// plan_clear() tests
// =========================================================================

#[test]
fn test_plan_clear_returns_source_level_only() {
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    let excluded_id = insert_source_excluded(&conn, root, "excluded.jpg", None);
    let _normal_id = insert_source(&conn, root, "normal.jpg", None);

    let plan = plan_clear(&mut conn, &make_clear_params(vec![])).unwrap();

    assert_eq!(plan.source_ids(), vec![excluded_id]);
}

#[test]
fn test_plan_clear_ignores_object_level() {
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);

    // Source NOT excluded, but object IS excluded
    let excluded_obj = insert_object(&conn, "abc123excluded", true);
    insert_source(&conn, root, "obj_excluded.jpg", Some(excluded_obj));

    let plan = plan_clear(&mut conn, &make_clear_params(vec![])).unwrap();

    assert!(
        plan.source_ids().is_empty(),
        "Object-level excluded sources should NOT appear"
    );
}

#[test]
fn test_plan_clear_respects_scope() {
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    let in_scope = insert_source_excluded(&conn, root, "2024/excluded.jpg", None);
    let _out_scope = insert_source_excluded(&conn, root, "2023/excluded.jpg", None);

    let scopes = classify_all(&["/photos/2024".to_string()]);
    let plan = plan_clear(&mut conn, &make_clear_params(scopes)).unwrap();

    assert_eq!(plan.source_ids(), vec![in_scope]);
}

#[test]
fn test_plan_clear_returns_paths() {
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    insert_source_excluded(&conn, root, "subdir/excluded.jpg", None);

    let plan = plan_clear(&mut conn, &make_clear_params(vec![])).unwrap();

    assert_eq!(plan.paths(), vec!["/photos/subdir/excluded.jpg"]);
}

#[test]
fn test_plan_clear_counts_roots() {
    let mut conn = setup_test_db();
    let root1 = insert_root(&conn, "/root1", "source", false);
    let root2 = insert_root(&conn, "/root2", "source", false);
    insert_source_excluded(&conn, root1, "a.jpg", None);
    insert_source_excluded(&conn, root2, "b.jpg", None);

    let plan = plan_clear(&mut conn, &make_clear_params(vec![])).unwrap();

    assert_eq!(plan.root_count, 2);
}

#[test]
fn test_plan_clear_empty_when_none_excluded() {
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    insert_source(&conn, root, "normal.jpg", None);

    let plan = plan_clear(&mut conn, &make_clear_params(vec![])).unwrap();

    assert!(plan.source_ids().is_empty());
    assert_eq!(plan.root_count, 0);
}

#[test]
fn test_plan_clear_ignores_suspended_roots() {
    let mut conn = setup_test_db();
    let _suspended = insert_root(&conn, "/suspended", "source", true);
    conn.execute(
        "INSERT INTO sources (root_id, rel_path, size, mtime, partial_hash, scanned_at, last_seen_at, device, inode, excluded)
         VALUES (?, 'excluded.jpg', 1000, 1704067200, '', 0, 0, 0, 0, 1)",
        rusqlite::params![_suspended],
    )
    .unwrap();

    let plan = plan_clear(&mut conn, &make_clear_params(vec![])).unwrap();

    assert!(plan.source_ids().is_empty());
}

#[test]
fn test_plan_clear_reaches_archive_role_sources() {
    // Single-target set accepts an archive-role source, so an exclusion can
    // stand there — and whatever set can reach, clear must be able to undo.
    // A role filter in plan_clear would strand such an exclusion permanently.
    let mut conn = setup_test_db();
    let archive = insert_root(&conn, "/archive", "archive", false);
    insert_source_excluded(&conn, archive, "archived.jpg", None);

    let plan = plan_clear(&mut conn, &make_clear_params(vec![])).unwrap();

    assert_eq!(plan.items.len(), 1);
    assert_eq!(plan.items[0].rel_path, "archived.jpg");
}

// =========================================================================
// plan_duplicates() tests
// =========================================================================

#[test]
fn test_plan_duplicates_empty_when_no_sources() {
    let mut conn = setup_test_db();
    let _root = insert_root(&conn, "/source", "source", false);

    let plan = plan_duplicates(&mut conn, &make_duplicates_params(vec![], "/archive")).unwrap();

    assert!(plan.source_ids().is_empty());
    assert_eq!(plan.scope_count, 0);
    assert_eq!(plan.group_count, 0);
}

#[test]
fn test_plan_duplicates_excludes_with_prefer_copy() {
    let mut conn = setup_test_db();
    let source_root = insert_root(&conn, "/source", "source", false);
    let archive_root = insert_root(&conn, "/archive", "archive", false);

    let obj = insert_object(&conn, "same_hash", false);
    let source_id = insert_source(&conn, source_root, "photo.jpg", Some(obj));
    insert_source(&conn, archive_root, "photo.jpg", Some(obj));

    let plan = plan_duplicates(&mut conn, &make_duplicates_params(vec![], "/archive")).unwrap();

    assert_eq!(plan.source_ids(), vec![source_id]);
    assert_eq!(plan.scope_count, 1);
}

#[test]
fn test_plan_duplicates_skips_no_copy() {
    let mut conn = setup_test_db();
    let source_root = insert_root(&conn, "/source", "source", false);
    let _archive_root = insert_root(&conn, "/archive", "archive", false);

    let obj = insert_object(&conn, "unique_hash", false);
    insert_source(&conn, source_root, "unique.jpg", Some(obj));

    let plan = plan_duplicates(&mut conn, &make_duplicates_params(vec![], "/archive")).unwrap();

    assert!(plan.source_ids().is_empty());
    assert_eq!(plan.skipped_not_covered, 1);
}

#[test]
fn test_plan_duplicates_skips_multiple_copies() {
    let mut conn = setup_test_db();
    let source_root = insert_root(&conn, "/source", "source", false);
    let archive_root = insert_root(&conn, "/archive", "archive", false);

    let obj = insert_object(&conn, "multi_hash", false);
    insert_source(&conn, source_root, "photo.jpg", Some(obj));
    insert_source(&conn, archive_root, "copy1.jpg", Some(obj));
    insert_source(&conn, archive_root, "copy2.jpg", Some(obj));

    let plan = plan_duplicates(&mut conn, &make_duplicates_params(vec![], "/archive")).unwrap();

    assert!(plan.source_ids().is_empty());
    assert_eq!(plan.skipped_multiple, 1);
}

#[test]
fn test_plan_duplicates_skips_unhashed() {
    let mut conn = setup_test_db();
    let source_root = insert_root(&conn, "/source", "source", false);

    insert_source(&conn, source_root, "unhashed.jpg", None);

    let plan = plan_duplicates(&mut conn, &make_duplicates_params(vec![], "/archive")).unwrap();

    assert!(plan.source_ids().is_empty());
    assert_eq!(plan.skipped_no_hash, 1);
    assert_eq!(plan.scope_count, 1);
}

#[test]
fn test_plan_duplicates_skips_in_prefer() {
    let mut conn = setup_test_db();
    let archive_root = insert_root(&conn, "/archive", "source", false);

    let obj = insert_object(&conn, "prefer_hash", false);
    insert_source(&conn, archive_root, "photo.jpg", Some(obj));

    // Source is in the prefer path itself — should be skipped
    let plan = plan_duplicates(&mut conn, &make_duplicates_params(vec![], "/archive")).unwrap();

    assert!(plan.source_ids().is_empty());
    assert_eq!(plan.skipped_in_prefer, 1);
}

#[test]
fn test_plan_duplicates_computes_group_count() {
    let mut conn = setup_test_db();
    let source_root = insert_root(&conn, "/source", "source", false);
    let archive_root = insert_root(&conn, "/archive", "archive", false);

    let obj1 = insert_object(&conn, "group1_hash", false);
    let obj2 = insert_object(&conn, "group2_hash", false);

    // 2 sources for obj1, 2 sources for obj2
    insert_source(&conn, source_root, "a/photo1.jpg", Some(obj1));
    insert_source(&conn, source_root, "b/photo1.jpg", Some(obj1));
    insert_source(&conn, source_root, "a/photo2.jpg", Some(obj2));
    insert_source(&conn, source_root, "b/photo2.jpg", Some(obj2));

    // 1 copy each in archive
    insert_source(&conn, archive_root, "photo1.jpg", Some(obj1));
    insert_source(&conn, archive_root, "photo2.jpg", Some(obj2));

    let plan = plan_duplicates(&mut conn, &make_duplicates_params(vec![], "/archive")).unwrap();

    assert_eq!(plan.source_ids().len(), 4);
    assert_eq!(plan.group_count, 2, "2 distinct object groups");
}

#[test]
fn test_plan_duplicates_includes_paths() {
    let mut conn = setup_test_db();
    let source_root = insert_root(&conn, "/source", "source", false);
    let archive_root = insert_root(&conn, "/archive", "archive", false);

    let obj = insert_object(&conn, "path_hash", false);
    insert_source(&conn, source_root, "subdir/photo.jpg", Some(obj));
    insert_source(&conn, archive_root, "photo.jpg", Some(obj));

    let plan = plan_duplicates(&mut conn, &make_duplicates_params(vec![], "/archive")).unwrap();

    assert_eq!(plan.paths(), vec!["/source/subdir/photo.jpg"]);
}

#[test]
fn test_plan_duplicates_scope_count() {
    let mut conn = setup_test_db();
    let source_root = insert_root(&conn, "/source", "source", false);
    let archive_root = insert_root(&conn, "/archive", "archive", false);

    let obj = insert_object(&conn, "scope_hash", false);
    insert_source(&conn, source_root, "a.jpg", Some(obj));
    insert_source(&conn, source_root, "b.jpg", None); // unhashed
    insert_source(&conn, archive_root, "a.jpg", Some(obj));

    let plan = plan_duplicates(&mut conn, &make_duplicates_params(vec![], "/archive")).unwrap();

    assert_eq!(
        plan.scope_count, 2,
        "Both sources in scope (before analysis)"
    );
    assert_eq!(
        plan.source_ids().len(),
        1,
        "Only hashed with prefer copy excluded"
    );
}

#[test]
fn test_plan_duplicates_respects_scope() {
    let mut conn = setup_test_db();
    let source_root = insert_root(&conn, "/source", "source", false);
    let archive_root = insert_root(&conn, "/archive", "archive", false);

    let obj1 = insert_object(&conn, "in_scope_hash", false);
    let obj2 = insert_object(&conn, "out_scope_hash", false);

    let in_scope = insert_source(&conn, source_root, "2024/photo.jpg", Some(obj1));
    insert_source(&conn, source_root, "2023/photo.jpg", Some(obj2));
    insert_source(&conn, archive_root, "photo1.jpg", Some(obj1));
    insert_source(&conn, archive_root, "photo2.jpg", Some(obj2));

    let scopes = classify_all(&["/source/2024".to_string()]);
    let plan = plan_duplicates(&mut conn, &make_duplicates_params(scopes, "/archive")).unwrap();

    assert_eq!(plan.source_ids(), vec![in_scope]);
    assert_eq!(plan.scope_count, 1);
}

// =========================================================================
// plan_duplicates group reconstruction
// =========================================================================

#[test]
fn test_plan_duplicates_reconstructs_group() {
    let mut conn = setup_test_db();
    let source_root = insert_root(&conn, "/source", "source", false);
    let archive_root = insert_root(&conn, "/archive", "archive", false);
    let obj = insert_object(&conn, "dup_hash_value", false);
    let src_id = insert_source(&conn, source_root, "photo.jpg", Some(obj));
    let arch_id = insert_source(&conn, archive_root, "kept.jpg", Some(obj));

    let plan = plan_duplicates(&mut conn, &make_duplicates_params(vec![], "/archive")).unwrap();

    assert_eq!(plan.groups.len(), 1);
    assert_eq!(plan.group_count, 1);
    let g = &plan.groups[0];
    assert_eq!(g.hash, "sha256:dup_hash_value");
    // excluded = the source-root copy (not under the prefer prefix)
    assert_eq!(
        g.excluded.iter().map(|i| i.source_id).collect::<Vec<_>>(),
        vec![src_id]
    );
    // kept = the archive copy under the prefer prefix (no transition)
    assert_eq!(
        g.kept.iter().map(|i| i.source_id).collect::<Vec<_>>(),
        vec![arch_id]
    );
    assert_eq!(g.kept[0].path(), "/archive/kept.jpg");
    // accessors derive from groups[*].excluded
    assert_eq!(plan.source_ids(), vec![src_id]);
    assert_eq!(plan.paths(), vec!["/source/photo.jpg"]);
}

#[test]
fn test_plan_duplicates_groups_by_object() {
    let mut conn = setup_test_db();
    let source_root = insert_root(&conn, "/source", "source", false);
    let archive_root = insert_root(&conn, "/archive", "archive", false);
    let obj1 = insert_object(&conn, "group1_hash", false);
    let obj2 = insert_object(&conn, "group2_hash", false);
    // Two source-root copies per object, one archive copy each.
    insert_source(&conn, source_root, "a/photo1.jpg", Some(obj1));
    insert_source(&conn, source_root, "b/photo1.jpg", Some(obj1));
    insert_source(&conn, source_root, "a/photo2.jpg", Some(obj2));
    insert_source(&conn, source_root, "b/photo2.jpg", Some(obj2));
    insert_source(&conn, archive_root, "photo1.jpg", Some(obj1));
    insert_source(&conn, archive_root, "photo2.jpg", Some(obj2));

    let plan = plan_duplicates(&mut conn, &make_duplicates_params(vec![], "/archive")).unwrap();

    assert_eq!(plan.groups.len(), 2, "one group per object");
    for g in &plan.groups {
        assert_eq!(g.excluded.len(), 2, "both source-root copies excluded");
        assert_eq!(g.kept.len(), 1, "single archive copy kept");
    }
    // 4 excluded across both groups
    assert_eq!(plan.source_ids().len(), 4);
}

// =========================================================================
// plan_set_objects() tests
// =========================================================================

#[test]
fn test_plan_set_objects_empty_when_no_sources() {
    let mut conn = setup_test_db();
    let _root = insert_root(&conn, "/photos", "source", false);

    let plan = plan_set_objects(&mut conn, &make_set_objects_params(vec![])).unwrap();

    assert!(plan.objects.is_empty());
    assert_eq!(plan.total_source_count, 0);
    assert_eq!(plan.total_archive_count, 0);
}

#[test]
fn test_plan_set_objects_includes_non_excluded() {
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    let obj = insert_object(&conn, "abc123hash_value_x", false);
    insert_source(&conn, root, "photo.jpg", Some(obj));

    let plan = plan_set_objects(&mut conn, &make_set_objects_params(vec![])).unwrap();

    assert_eq!(plan.objects.len(), 1);
    assert_eq!(plan.objects[0].object_id, obj);
}

#[test]
fn test_plans_never_offer_archive_role_sources() {
    // An exclusion landing on an archive-role source could never be undone:
    // plan_clear only ever looks at source-role roots. So the intake side
    // holds the same line — even when archive copies are the only in-scope
    // rows, neither plan_set nor plan_duplicates offers them.
    let mut conn = setup_test_db();
    let archive1 = insert_root(&conn, "/archive", "archive", false);
    let archive2 = insert_root(&conn, "/archive2", "archive", false);
    let obj = insert_object(&conn, "dup_hash_value", false);
    insert_source(&conn, archive1, "kept.jpg", Some(obj));
    insert_source(&conn, archive2, "stray.jpg", Some(obj));

    let plan = plan_set(&mut conn, &make_set_params(vec![])).unwrap();
    assert!(plan.source_ids().is_empty());

    let dup = plan_duplicates(&mut conn, &make_duplicates_params(vec![], "/archive")).unwrap();
    assert!(dup.groups.is_empty());
}

#[test]
fn test_plan_set_objects_reaches_objects_behind_excluded_sources() {
    // Excluding an object must reach objects whose only in-scope copies are
    // already source-excluded — the common flow: a folder dismissed source
    // by source, then its content dismissed everywhere. The object plan
    // deliberately widens its selection to excluded sources; harmonizing it
    // with the sibling plans' default visibility makes these objects
    // invisible.
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    let obj = insert_object(&conn, "behind_excluded_hash", false);
    insert_source_excluded(&conn, root, "a.jpg", Some(obj));

    let plan = plan_set_objects(&mut conn, &make_set_objects_params(vec![])).unwrap();
    assert_eq!(plan.objects.len(), 1);
}

#[test]
fn test_plan_set_objects_skips_already_excluded() {
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    let obj = insert_object(&conn, "already_excl_hash", true);
    insert_source(&conn, root, "photo.jpg", Some(obj));

    let plan = plan_set_objects(&mut conn, &make_set_objects_params(vec![])).unwrap();

    assert!(plan.objects.is_empty());
    assert_eq!(plan.skipped_already_excluded, 1);
}

#[test]
fn test_plan_set_objects_skips_unhashed() {
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    insert_source(&conn, root, "unhashed.jpg", None);

    let plan = plan_set_objects(&mut conn, &make_set_objects_params(vec![])).unwrap();

    assert!(plan.objects.is_empty());
    assert_eq!(plan.skipped_no_hash, 1);
}

#[test]
fn test_plan_set_objects_skips_empty() {
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    let obj = insert_object(&conn, "empty_hash_value_x", false);
    insert_source_with_size(&conn, root, "empty.txt", Some(obj), 0);

    let plan = plan_set_objects(&mut conn, &make_set_objects_params(vec![])).unwrap();

    assert!(plan.objects.is_empty());
    assert_eq!(plan.skipped_empty, 1);
}

#[test]
fn test_plan_set_objects_counts_each_skipped_empty_file() {
    // The interface prints "{n} empty files skipped", so the count must be
    // per set-aside source — all empty files share the one empty-content
    // object, and an object-grain count would report any number of them as 1.
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    let obj = insert_object(&conn, "empty_hash_value_x", false);
    insert_source_with_size(&conn, root, "a/empty1.txt", Some(obj), 0);
    insert_source_with_size(&conn, root, "a/empty2.txt", Some(obj), 0);
    insert_source_with_size(&conn, root, "b/empty3.txt", Some(obj), 0);

    let plan = plan_set_objects(&mut conn, &make_set_objects_params(vec![])).unwrap();

    assert!(plan.objects.is_empty());
    assert_eq!(plan.skipped_empty, 3);
}

#[test]
fn test_plan_set_objects_computes_source_counts() {
    let mut conn = setup_test_db();
    let source_root = insert_root(&conn, "/source", "source", false);
    let archive_root = insert_root(&conn, "/archive", "archive", false);
    let obj = insert_object(&conn, "counts_hash_value", false);
    insert_source(&conn, source_root, "photo.jpg", Some(obj));
    insert_source(&conn, archive_root, "photo.jpg", Some(obj));

    let plan = plan_set_objects(&mut conn, &make_set_objects_params(vec![])).unwrap();

    assert_eq!(plan.objects.len(), 1);
    assert_eq!(plan.total_source_count, 2);
    assert_eq!(plan.total_archive_count, 1);
}

#[test]
fn test_plan_set_objects_hash_prefix() {
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    let obj = insert_object(&conn, "abcdef1234567890extra", false);
    insert_source(&conn, root, "photo.jpg", Some(obj));

    let plan = plan_set_objects(&mut conn, &make_set_objects_params(vec![])).unwrap();

    assert_eq!(plan.objects[0].hash_prefix, "abcdef1234567890");
}

#[test]
fn test_plan_set_objects_respects_scope() {
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    let obj1 = insert_object(&conn, "in_scope_obj_hash", false);
    let obj2 = insert_object(&conn, "out_scope_obj_hsh", false);
    insert_source(&conn, root, "2024/photo.jpg", Some(obj1));
    insert_source(&conn, root, "2023/photo.jpg", Some(obj2));

    let scopes = classify_all(&["/photos/2024".to_string()]);
    let plan = plan_set_objects(&mut conn, &make_set_objects_params(scopes)).unwrap();

    assert_eq!(plan.objects.len(), 1);
    assert_eq!(plan.objects[0].object_id, obj1);
}

#[test]
fn test_plan_set_objects_deduplicates_objects() {
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    let obj = insert_object(&conn, "shared_obj_hash_xx", false);
    // Two sources sharing the same object
    insert_source(&conn, root, "copy1.jpg", Some(obj));
    insert_source(&conn, root, "copy2.jpg", Some(obj));

    let plan = plan_set_objects(&mut conn, &make_set_objects_params(vec![])).unwrap();

    assert_eq!(plan.objects.len(), 1, "Same object should appear once");
}

#[test]
fn test_plan_set_objects_source_sort_order() {
    let mut conn = setup_test_db();
    let source_root = insert_root(&conn, "/source", "source", false);
    let archive_root = insert_root(&conn, "/archive", "archive", false);
    let obj = insert_object(&conn, "sort_order_hash_xx", false);
    insert_source(&conn, source_root, "photo.jpg", Some(obj));
    insert_source(&conn, archive_root, "photo.jpg", Some(obj));

    let plan = plan_set_objects(&mut conn, &make_set_objects_params(vec![])).unwrap();

    let sources = &plan.objects[0].sources;
    assert_eq!(sources.len(), 2);
    // Source roots come first (role DESC: 'source' > 'archive')
    assert!(!sources[0].is_archive, "Source root should come first");
    assert!(sources[1].is_archive, "Archive root should come second");
}
