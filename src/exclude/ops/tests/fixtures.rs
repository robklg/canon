//! Fixtures shared across the exclude ops test modules — genuinely
//! cross-cutting helpers only; `receipts.rs`'s own file-local helpers
//! (`full_decision`, `ledger_root`, `fetch_source_decision_id`) stay there
//! since they're used only by receipt tests, not shared with plan/execute.

use crate::domain::scope::ScopeMatch;
use crate::exclude::ops::types::{
    ExcludeClearParams, ExcludeDuplicatesParams, ExcludeItemData, ExcludeSetObjectsParams,
    ExcludeSetParams,
};

pub(super) fn make_set_params(scopes: Vec<ScopeMatch>) -> ExcludeSetParams {
    ExcludeSetParams {
        scopes,
        filters: vec![],
    }
}

pub(super) fn make_clear_params(scopes: Vec<ScopeMatch>) -> ExcludeClearParams {
    ExcludeClearParams {
        scopes,
        filters: vec![],
    }
}

/// Minimal ExcludeItemData for execute tests (only source_id is consumed).
pub(super) fn item(source_id: i64, root: &str, rel_path: &str) -> ExcludeItemData {
    ExcludeItemData {
        source_id,
        root: root.to_string(),
        rel_path: rel_path.to_string(),
        hash: None,
        size: 1000,
        mtime: 1704067200,
        previous_decision_id: None,
    }
}

pub(super) fn make_duplicates_params(
    scopes: Vec<ScopeMatch>,
    prefer_prefix: &str,
) -> ExcludeDuplicatesParams {
    ExcludeDuplicatesParams {
        scopes,
        filters: vec![],
        prefer_prefix: prefer_prefix.to_string(),
    }
}

pub(super) fn make_set_objects_params(scopes: Vec<ScopeMatch>) -> ExcludeSetObjectsParams {
    ExcludeSetObjectsParams {
        scopes,
        filters: vec![],
    }
}
