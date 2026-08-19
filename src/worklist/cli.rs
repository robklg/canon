use anyhow::Result;
use std::io::{self, Write};

use crate::core::domain::IncludeSet;
use crate::core::ops::scope::classify_all;
use crate::core::repo::Db;
use crate::expr::Filter;
use crate::expr::{select_sources, RolePolicy, SelectionParams};

pub fn run(
    db: &mut Db,
    scope_prefixes: &[String],
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

    let scopes = classify_all(scope_prefixes);

    let params = SelectionParams {
        scopes,
        include: include.clone(),
        filters,
        role_policy: RolePolicy::SourceUnlessIncluded,
    };
    let sel = select_sources(conn, &params)?;

    let result =
        crate::worklist::ops::build_entries(conn, &sel.sources, unique_content, emit_keys)?;

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
