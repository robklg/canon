use anyhow::Result;
use std::io::{self, Write};

use crate::core::domain::IncludeSet;
use crate::core::ops::scope::classify_all;
use crate::core::repo::Db;
use crate::expr::read_filters;
use crate::expr::{select_sources, RolePolicy, SelectionParams};

pub fn run(
    db: &mut Db,
    scope_prefixes: &[String],
    filter_strs: &[String],
    include: &IncludeSet,
    unique_content: bool,
    emit_keys: &[String],
) -> Result<()> {
    let conn = db.conn_mut();

    // Read the `--emit` names, then the filters, before anything is selected.
    let emit_keys = crate::worklist::ops::read_emit_keys(emit_keys)?;
    let filters = read_filters(conn, filter_strs)?;

    let scopes = classify_all(scope_prefixes);

    let params = SelectionParams {
        scopes,
        include: include.clone(),
        filters,
        role_policy: RolePolicy::SourceUnlessIncluded,
        clock: crate::user_zone().clone(),
    };
    let sel = select_sources(conn, &params)?;

    let result = crate::worklist::ops::build_entries(
        conn,
        &sel.sources,
        unique_content,
        &emit_keys,
        crate::user_zone(),
    )?;

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

#[cfg(test)]
mod tests {
    use super::*;

    /// The `--emit` names are read first: a refused name is the run's one answer even beside
    /// a filter that cannot be read, so nothing is selected and nothing reaches stdout.
    #[test]
    fn an_unknown_emit_name_is_refused_before_the_filters_and_the_selection() {
        crate::core::domain::time::zone::with_test_zone(
            crate::core::domain::time::zone::UserZone::utc(),
            || {
                use crate::core::repo::open_in_memory_for_test;
                use crate::core::testing::{insert_root, insert_source};

                let conn = open_in_memory_for_test();
                let root = insert_root(&conn, "/photos", "source", false);
                insert_source(&conn, root, "a.jpg", None);
                let mut db = Db::from_connection(conn);
                let refused = run(
                    &mut db,
                    &[],
                    &["source.size >".to_string()],
                    &IncludeSet::default(),
                    false,
                    &["source.extension".to_string()],
                )
                .expect_err("refused");
                assert_eq!(
                    refused.to_string(),
                    "cannot use the key as given\n  `source.extension` is an unknown key"
                );
            },
        );
    }
}
