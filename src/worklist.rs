use anyhow::Result;
use std::io::{self, Write};
use std::path::PathBuf;

use crate::domain::path::resolve_paths;
use crate::domain::scope::ScopeMatch;
use crate::domain::IncludeSet;
use crate::expr::filter::Filter;
use crate::ops;
use crate::ops::selection::{self, RolePolicy, SelectionParams};
use crate::repo::{self, Db};

pub fn run(
    db: &mut Db,
    scope_paths: &[PathBuf],
    filter_strs: &[String],
    include: &IncludeSet,
    unique_content: bool,
    emit_keys: &[String],
) -> Result<()> {
    // Parse filters upfront
    let filters: Vec<Filter> = filter_strs
        .iter()
        .map(|f| Filter::parse(f))
        .collect::<Result<Vec<_>>>()?;

    let conn = db.conn_mut();

    // Resolve scope paths
    let all_roots = repo::root::fetch_all(conn)?;
    let scope_prefixes = resolve_paths(scope_paths, &all_roots)?;
    let scopes = ScopeMatch::classify_all(&scope_prefixes);

    let params = SelectionParams {
        scopes,
        include: include.clone(),
        filters,
        role_policy: RolePolicy::SourceUnlessIncluded,
    };
    let sel = selection::select_sources(conn, &params)?;

    let result = ops::worklist::build_entries(conn, &sel.sources, unique_content, emit_keys)?;

    let stdout = io::stdout();
    let mut handle = stdout.lock();
    for entry in &result.entries {
        let json = serde_json::to_string(entry)?;
        writeln!(handle, "{json}")?;
    }

    // Report stats to stderr
    if include.includes_excluded() && sel.included_excluded_count > 0 {
        eprintln!("Included {} excluded sources", sel.included_excluded_count);
    } else if sel.excluded_count > 0 {
        eprintln!("Skipped {0} excluded sources", sel.excluded_count);
    }
    if unique_content && (result.skipped_unhashed > 0 || result.skipped_duplicate > 0) {
        eprintln!(
            "Skipped {} unhashed, {} duplicate sources",
            result.skipped_unhashed, result.skipped_duplicate
        );
    }

    Ok(())
}
