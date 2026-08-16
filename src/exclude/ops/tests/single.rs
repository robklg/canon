use crate::exclude::ops::single::{
    check_clear_object, check_set_object_by_file, check_set_object_by_hash, check_set_source_by_id,
    check_set_source_by_path, execute_clear_object, execute_set_object, fetch_object_sources,
    list_excluded_objects, ObjectClearCheck, ObjectExclusionCheck, SourceExclusionCheck,
};
use crate::ops::test_helpers::{
    insert_object, insert_root, insert_source, insert_source_excluded, insert_source_with_size,
    setup_test_db,
};

// =========================================================================
// check_set_source_by_id() tests
// =========================================================================

#[test]
fn test_check_source_by_id_ready() {
    let conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    let id = insert_source(&conn, root, "photo.jpg", None);

    let result = check_set_source_by_id(&conn, id).unwrap();
    match result {
        SourceExclusionCheck::Ready { item } => {
            assert_eq!(item.source_id, id);
            assert_eq!(item.path(), "/photos/photo.jpg");
        }
        SourceExclusionCheck::AlreadyExcluded { .. } => {
            panic!("Expected Ready, got AlreadyExcluded");
        }
    }
}

#[test]
fn test_check_source_by_id_already_excluded() {
    let conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    let id = insert_source_excluded(&conn, root, "photo.jpg", None);

    let result = check_set_source_by_id(&conn, id).unwrap();
    match result {
        SourceExclusionCheck::AlreadyExcluded { path } => {
            assert_eq!(path, "/photos/photo.jpg");
        }
        SourceExclusionCheck::Ready { .. } => {
            panic!("Expected AlreadyExcluded, got Ready");
        }
    }
}

#[test]
fn test_check_source_by_id_not_found() {
    let conn = setup_test_db();

    let result = check_set_source_by_id(&conn, 99999);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("not found"),
        "Error should mention 'not found', got: {err}"
    );
}

#[test]
fn test_check_source_by_id_not_present() {
    let conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    // Not present (batch_fetch_by_ids filters these out)
    conn.execute(
        "INSERT INTO sources (root_id, rel_path, size, mtime, partial_hash, scanned_at, last_seen_at, device, inode, present)
         VALUES (?, 'deleted.jpg', 1000, 1704067200, '', 0, 0, 0, 0, 0)",
        rusqlite::params![root],
    )
    .unwrap();
    let source_id = conn.last_insert_rowid();

    let result = check_set_source_by_id(&conn, source_id);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("not found") || err.contains("not present"),
        "Error should mention not found/present, got: {err}"
    );
}

// =========================================================================
// check_set_source_by_path() tests
// =========================================================================

#[test]
fn test_check_source_by_path_ready() {
    let conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    let id = insert_source(&conn, root, "photo.jpg", None);

    let result = check_set_source_by_path(&conn, root, "photo.jpg", "/photos/photo.jpg").unwrap();
    match result {
        SourceExclusionCheck::Ready { item } => {
            assert_eq!(item.source_id, id);
            assert_eq!(item.path(), "/photos/photo.jpg");
        }
        SourceExclusionCheck::AlreadyExcluded { .. } => {
            panic!("Expected Ready, got AlreadyExcluded");
        }
    }
}

#[test]
fn test_check_source_by_path_not_found() {
    let conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);

    let result =
        check_set_source_by_path(&conn, root, "nonexistent.jpg", "/photos/nonexistent.jpg");
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("No source found"),
        "Error should mention no source found, got: {err}"
    );
}

#[test]
fn test_check_source_by_path_already_excluded() {
    let conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    insert_source_excluded(&conn, root, "photo.jpg", None);

    let result = check_set_source_by_path(&conn, root, "photo.jpg", "/photos/photo.jpg").unwrap();
    match result {
        SourceExclusionCheck::AlreadyExcluded { path } => {
            assert_eq!(path, "/photos/photo.jpg");
        }
        SourceExclusionCheck::Ready { .. } => {
            panic!("Expected AlreadyExcluded, got Ready");
        }
    }
}

// =========================================================================
// check_set_object_by_hash() tests
// =========================================================================

#[test]
fn test_check_object_by_hash_ready() {
    let conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    let obj = insert_object(&conn, "abc123hash_value_x", false);
    insert_source(&conn, root, "photo.jpg", Some(obj));

    let result = check_set_object_by_hash(&conn, "abc123hash_value_x").unwrap();
    match result {
        ObjectExclusionCheck::Ready {
            object_id,
            hash_prefix,
            hash: _,
            sources,
        } => {
            assert_eq!(object_id, obj);
            assert_eq!(hash_prefix, "abc123hash_value");
            assert_eq!(sources.len(), 1);
            assert_eq!(sources[0].path, "/photos/photo.jpg");
        }
        ObjectExclusionCheck::AlreadyExcluded { .. } => {
            panic!("Expected Ready, got AlreadyExcluded");
        }
    }
}

#[test]
fn test_check_object_by_hash_not_found() {
    let conn = setup_test_db();

    let result = check_set_object_by_hash(&conn, "nonexistent_hash");
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("No object found"),
        "Error should mention no object found, got: {err}"
    );
}

#[test]
fn test_check_object_by_hash_already_excluded() {
    let conn = setup_test_db();
    let _root = insert_root(&conn, "/photos", "source", false);
    let _obj = insert_object(&conn, "excluded_hash_val_", true);

    let result = check_set_object_by_hash(&conn, "excluded_hash_val_").unwrap();
    match result {
        ObjectExclusionCheck::AlreadyExcluded { hash_prefix } => {
            assert_eq!(hash_prefix, "excluded_hash_va");
        }
        ObjectExclusionCheck::Ready { .. } => {
            panic!("Expected AlreadyExcluded, got Ready");
        }
    }
}

// =========================================================================
// check_set_object_by_file() tests
// =========================================================================

#[test]
fn test_check_object_by_file_ready() {
    let conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    let obj = insert_object(&conn, "file_obj_hash_val_", false);
    insert_source(&conn, root, "photo.jpg", Some(obj));

    let result = check_set_object_by_file(&conn, root, "photo.jpg", "/photos/photo.jpg").unwrap();
    match result {
        ObjectExclusionCheck::Ready {
            object_id,
            hash_prefix,
            hash: _,
            sources,
        } => {
            assert_eq!(object_id, obj);
            assert_eq!(hash_prefix, "file_obj_hash_va");
            assert_eq!(sources.len(), 1);
        }
        ObjectExclusionCheck::AlreadyExcluded { .. } => {
            panic!("Expected Ready, got AlreadyExcluded");
        }
    }
}

#[test]
fn test_check_object_by_file_not_found() {
    let conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);

    let result = check_set_object_by_file(&conn, root, "missing.jpg", "/photos/missing.jpg");
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("No hashed source found"),
        "Error should mention no hashed source, got: {err}"
    );
}

#[test]
fn test_check_object_by_file_unhashed() {
    let conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    insert_source(&conn, root, "unhashed.jpg", None);

    let result = check_set_object_by_file(&conn, root, "unhashed.jpg", "/photos/unhashed.jpg");
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("No hashed source found"),
        "Error should mention no hashed source, got: {err}"
    );
}

#[test]
fn test_check_object_by_file_empty() {
    let conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    let obj = insert_object(&conn, "empty_file_hash_v_", false);
    insert_source_with_size(&conn, root, "empty.txt", Some(obj), 0);

    let result = check_set_object_by_file(&conn, root, "empty.txt", "/photos/empty.txt");
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("Cannot exclude empty file"),
        "Error should mention empty file, got: {err}"
    );
}

// =========================================================================
// check_clear_object() tests
// =========================================================================

#[test]
fn test_check_clear_object_ready() {
    let conn = setup_test_db();
    let obj = insert_object(&conn, "clear_ready_hash_v", true);

    let result = check_clear_object(&conn, "clear_ready_hash_v").unwrap();
    match result {
        ObjectClearCheck::Ready {
            object_id,
            hash_prefix,
            hash: _,
        } => {
            assert_eq!(object_id, obj);
            assert_eq!(hash_prefix, "clear_ready_hash");
        }
        ObjectClearCheck::NotExcluded { .. } => {
            panic!("Expected Ready, got NotExcluded");
        }
    }
}

#[test]
fn test_check_clear_object_not_found() {
    let conn = setup_test_db();

    let result = check_clear_object(&conn, "nonexistent_hash");
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("No object found"),
        "Error should mention no object found, got: {err}"
    );
}

#[test]
fn test_check_clear_object_not_excluded() {
    let conn = setup_test_db();
    let _obj = insert_object(&conn, "not_excluded_hash_", false);

    let result = check_clear_object(&conn, "not_excluded_hash_").unwrap();
    match result {
        ObjectClearCheck::NotExcluded { hash_prefix } => {
            assert_eq!(hash_prefix, "not_excluded_has");
        }
        ObjectClearCheck::Ready { .. } => {
            panic!("Expected NotExcluded, got Ready");
        }
    }
}

// =========================================================================
// fetch_object_sources() tests
// =========================================================================

#[test]
fn test_fetch_object_sources_returns_paths() {
    let conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    let obj = insert_object(&conn, "fetch_src_hash_val", false);
    insert_source(&conn, root, "2024/photo.jpg", Some(obj));

    let sources = fetch_object_sources(&conn, obj).unwrap();

    assert_eq!(sources.len(), 1);
    assert_eq!(sources[0].path, "/photos/2024/photo.jpg");
}

#[test]
fn test_fetch_object_sources_includes_role() {
    let conn = setup_test_db();
    let source_root = insert_root(&conn, "/source", "source", false);
    let archive_root = insert_root(&conn, "/archive", "archive", false);
    let obj = insert_object(&conn, "role_src_hash_val_", false);
    insert_source(&conn, source_root, "photo.jpg", Some(obj));
    insert_source(&conn, archive_root, "photo.jpg", Some(obj));

    let sources = fetch_object_sources(&conn, obj).unwrap();

    assert_eq!(sources.len(), 2);
    // Source roots come first (role DESC: 'source' > 'archive')
    assert!(!sources[0].is_archive, "Source root should come first");
    assert!(sources[1].is_archive, "Archive root should come second");
}

#[test]
fn test_fetch_object_sources_empty_rel_path() {
    let conn = setup_test_db();
    let root = insert_root(&conn, "/archive/photo.jpg", "archive", false);
    let obj = insert_object(&conn, "empty_rel_hash_val", false);
    insert_source(&conn, root, "", Some(obj));

    let sources = fetch_object_sources(&conn, obj).unwrap();

    assert_eq!(sources.len(), 1);
    assert_eq!(sources[0].path, "/archive/photo.jpg");
}

#[test]
fn test_fetch_object_sources_excludes_not_present() {
    let conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    let obj = insert_object(&conn, "present_hash_val_x", false);
    insert_source(&conn, root, "present.jpg", Some(obj));
    // Not present source
    conn.execute(
        "INSERT INTO sources (root_id, rel_path, object_id, size, mtime, partial_hash, scanned_at, last_seen_at, device, inode, present)
         VALUES (?, 'deleted.jpg', ?, 1000, 1704067200, '', 0, 0, 0, 0, 0)",
        rusqlite::params![root, obj],
    )
    .unwrap();

    let sources = fetch_object_sources(&conn, obj).unwrap();

    assert_eq!(sources.len(), 1);
    assert_eq!(sources[0].path, "/photos/present.jpg");
}

// =========================================================================
// list_excluded_objects() tests
// =========================================================================

#[test]
fn test_list_excluded_objects_returns_entries() {
    let conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    let obj = insert_object(&conn, "list_excl_hash_val", true);
    insert_source(&conn, root, "photo.jpg", Some(obj));

    let entries = list_excluded_objects(&conn).unwrap();

    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].object_id, obj);
    assert_eq!(entries[0].hash_prefix, "list_excl_hash_v");
    assert_eq!(entries[0].source_count, 1);
}

#[test]
fn test_list_excluded_objects_source_counts() {
    let conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    let obj = insert_object(&conn, "multi_src_hash_val", true);
    insert_source(&conn, root, "photo1.jpg", Some(obj));
    insert_source(&conn, root, "photo2.jpg", Some(obj));
    // Not present — still counted by fetch_sources_by_object_ids
    conn.execute(
        "INSERT INTO sources (root_id, rel_path, object_id, size, mtime, partial_hash, scanned_at, last_seen_at, device, inode, present)
         VALUES (?, 'deleted.jpg', ?, 1000, 1704067200, '', 0, 0, 0, 0, 0)",
        rusqlite::params![root, obj],
    )
    .unwrap();

    let entries = list_excluded_objects(&conn).unwrap();

    assert_eq!(entries.len(), 1);
    // fetch_sources_by_object_ids returns present sources only
    // (the repo function filters for present=1)
    assert!(entries[0].source_count >= 2);
}

#[test]
fn test_list_excluded_objects_empty() {
    let conn = setup_test_db();
    let _root = insert_root(&conn, "/photos", "source", false);
    let _obj = insert_object(&conn, "not_excl_hash_val_", false);

    let entries = list_excluded_objects(&conn).unwrap();

    assert!(entries.is_empty());
}

// =========================================================================
// execute_set_object tests
// =========================================================================

#[test]
fn test_execute_set_object_excludes_and_returns_summary() {
    let mut conn = setup_test_db();
    let root = insert_root(&conn, "/photos", "source", false);
    let obj_id = insert_object(&conn, "abcdef1234567890", false);
    let _src = insert_source(&conn, root, "a.jpg", Some(obj_id));

    let sources = fetch_object_sources(&conn, obj_id).unwrap();
    let result = execute_set_object(
        &mut conn,
        obj_id,
        "abcdef1234567890",
        "sha256:abcdef1234567890",
        &sources,
        None,
        None,
    )
    .unwrap();

    assert_eq!(result.object_id, obj_id);
    assert_eq!(result.summary, "Excluded object: abcdef1234567890...");
    assert_eq!(result.source_count, 1);

    // Verify actually excluded in DB
    let objects = crate::core::repo::object::batch_fetch_by_ids(&conn, &[obj_id]).unwrap();
    assert!(objects.get(&obj_id).unwrap().is_excluded());
}

#[test]
fn test_execute_set_object_summary_includes_hash_prefix() {
    let mut conn = setup_test_db();
    let obj_id = insert_object(&conn, "deadbeef12345678", false);

    let result = execute_set_object(
        &mut conn,
        obj_id,
        "deadbeef12345678",
        "sha256:deadbeef12345678",
        &[],
        None,
        None,
    )
    .unwrap();

    assert!(result.summary.contains("deadbeef12345678"));
}

// =========================================================================
// execute_clear_object tests
// =========================================================================

#[test]
fn test_execute_clear_object_clears_and_returns_summary() {
    let mut conn = setup_test_db();
    let obj_id = insert_object(&conn, "abcdef1234567890", true); // already excluded

    let result = execute_clear_object(
        &mut conn,
        obj_id,
        "abcdef1234567890",
        "sha256:abcdef1234567890",
        None,
        None,
    )
    .unwrap();

    assert_eq!(result.object_id, obj_id);
    assert_eq!(
        result.summary,
        "Cleared exclusion from object: abcdef1234567890..."
    );

    // Verify no longer excluded in DB
    let objects = crate::core::repo::object::batch_fetch_by_ids(&conn, &[obj_id]).unwrap();
    assert!(!objects.get(&obj_id).unwrap().is_excluded());
}
