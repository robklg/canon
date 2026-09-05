use crate::core::testing::{
    insert_object, insert_root, insert_source, insert_source_excluded, is_object_excluded,
    is_source_excluded, setup_test_db,
};
use crate::exclude::ops::execute::{
    execute_clear, execute_duplicates, execute_set, execute_set_objects,
};
use crate::exclude::ops::types::{
    DuplicateGroupData, ExcludeClearPlan, ExcludeDuplicatesPlan, ExcludeSetObjectsPlan,
    ExcludeSetPlan, ObjectPlanEntry, ReceiptDestination,
};

use super::fixtures::item;

// =========================================================================
// execute tests
// =========================================================================

#[test]
fn test_execute_set_marks_excluded() {
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    let id1 = insert_source(&conn, root, "a.jpg", None);
    let id2 = insert_source(&conn, root, "b.jpg", None);

    let plan = ExcludeSetPlan {
        items: vec![item(id1, "/photos", "a.jpg"), item(id2, "/photos", "b.jpg")],
        root_count: 1,
        not_archived_count: 2,
    };

    execute_set(&mut conn, &plan, &ReceiptDestination::none(), None).unwrap();

    assert!(is_source_excluded(&conn, id1));
    assert!(is_source_excluded(&conn, id2));
}

#[test]
fn test_execute_clear_clears_excluded() {
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    let id1 = insert_source_excluded(&conn, root, "a.jpg", None);
    let id2 = insert_source_excluded(&conn, root, "b.jpg", None);

    let plan = ExcludeClearPlan {
        items: vec![item(id1, "/photos", "a.jpg"), item(id2, "/photos", "b.jpg")],
        root_count: 1,
    };

    execute_clear(&mut conn, &plan, &ReceiptDestination::none(), None).unwrap();

    assert!(!is_source_excluded(&conn, id1));
    assert!(!is_source_excluded(&conn, id2));
}

#[test]
fn test_execute_set_returns_count() {
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    let id1 = insert_source(&conn, root, "a.jpg", None);

    let plan = ExcludeSetPlan {
        items: vec![item(id1, "/photos", "a.jpg")],
        root_count: 1,
        not_archived_count: 1,
    };

    let result = execute_set(&mut conn, &plan, &ReceiptDestination::none(), None).unwrap();
    assert_eq!(result.count, 1);
}

#[test]
fn test_execute_clear_returns_count() {
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    let id1 = insert_source_excluded(&conn, root, "a.jpg", None);
    let id2 = insert_source_excluded(&conn, root, "b.jpg", None);

    let plan = ExcludeClearPlan {
        items: vec![item(id1, "/photos", "a.jpg"), item(id2, "/photos", "b.jpg")],
        root_count: 1,
    };

    let result = execute_clear(&mut conn, &plan, &ReceiptDestination::none(), None).unwrap();
    assert_eq!(result.count, 2);
}

// =========================================================================
// execute_duplicates(, None) tests
// =========================================================================

#[test]
fn test_execute_duplicates_marks_excluded() {
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/source", "source", false);
    let id1 = insert_source(&conn, root, "a.jpg", None);
    let id2 = insert_source(&conn, root, "b.jpg", None);

    let plan = ExcludeDuplicatesPlan {
        groups: vec![DuplicateGroupData {
            hash: "sha256:dup".to_string(),
            kept: vec![],
            excluded: vec![item(id1, "/source", "a.jpg"), item(id2, "/source", "b.jpg")],
        }],
        group_count: 1,
        prefer_prefix: "/archive".to_string(),
        scope_count: 2,
        skipped_no_hash: 0,
        skipped_in_prefer: 0,
        skipped_not_covered: 0,
        skipped_multiple: 0,
    };

    execute_duplicates(&mut conn, &plan, &ReceiptDestination::none(), None).unwrap();

    assert!(is_source_excluded(&conn, id1));
    assert!(is_source_excluded(&conn, id2));
}

#[test]
fn test_execute_duplicates_returns_count() {
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/source", "source", false);
    let id1 = insert_source(&conn, root, "a.jpg", None);

    let plan = ExcludeDuplicatesPlan {
        groups: vec![DuplicateGroupData {
            hash: "sha256:dup".to_string(),
            kept: vec![],
            excluded: vec![item(id1, "/source", "a.jpg")],
        }],
        group_count: 1,
        prefer_prefix: "/archive".to_string(),
        scope_count: 1,
        skipped_no_hash: 0,
        skipped_in_prefer: 0,
        skipped_not_covered: 0,
        skipped_multiple: 0,
    };

    let result = execute_duplicates(&mut conn, &plan, &ReceiptDestination::none(), None).unwrap();
    assert_eq!(result.count, 1);
}

// =========================================================================
// execute_set_objects(, None) tests
// =========================================================================

#[test]
fn test_execute_set_objects_marks_excluded() {
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    let obj1 = insert_object(&conn, "exec_obj_hash1_xx", false);
    let obj2 = insert_object(&conn, "exec_obj_hash2_xx", false);
    insert_source(&conn, root, "a.jpg", Some(obj1));
    insert_source(&conn, root, "b.jpg", Some(obj2));

    let plan = ExcludeSetObjectsPlan {
        objects: vec![
            ObjectPlanEntry {
                object_id: obj1,
                hash_prefix: "exec_obj_hash1_x".to_string(),
                hash: "sha256:exec_obj_hash1_xx".to_string(),
                sources: vec![],
            },
            ObjectPlanEntry {
                object_id: obj2,
                hash_prefix: "exec_obj_hash2_x".to_string(),
                hash: "sha256:exec_obj_hash2_xx".to_string(),
                sources: vec![],
            },
        ],
        total_source_count: 2,
        total_archive_count: 0,
        skipped_no_hash: 0,
        skipped_empty: 0,
        skipped_already_excluded: 0,
    };

    execute_set_objects(&mut conn, &plan, &ReceiptDestination::none(), None).unwrap();

    assert!(is_object_excluded(&conn, obj1));
    assert!(is_object_excluded(&conn, obj2));
}

#[test]
fn test_execute_set_objects_returns_count() {
    let mut conn = setup_test_db();
    let _root = insert_root(&conn, "/photos", "source", false);
    let obj = insert_object(&conn, "count_obj_hash_xxx", false);

    let plan = ExcludeSetObjectsPlan {
        objects: vec![ObjectPlanEntry {
            object_id: obj,
            hash_prefix: "count_obj_hash_x".to_string(),
            hash: "sha256:count_obj_hash_xxx".to_string(),
            sources: vec![],
        }],
        total_source_count: 1,
        total_archive_count: 0,
        skipped_no_hash: 0,
        skipped_empty: 0,
        skipped_already_excluded: 0,
    };

    let result = execute_set_objects(&mut conn, &plan, &ReceiptDestination::none(), None).unwrap();
    assert_eq!(result.count, 1);
}
