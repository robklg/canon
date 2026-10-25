use anyhow::Result;
use std::io::{self, Write};

use crate::core::domain::IncludeSet;
use crate::core::ops::scope::classify_all;
use crate::core::repo::Db;
use crate::expr::{read_filters, ExpressionRefused, ExpressionText};
use crate::expr::{select_sources, RolePolicy, SelectionParams};

pub fn run(
    db: &mut Db,
    scope_prefixes: &[String],
    filter_text: &ExpressionText,
    include: &IncludeSet,
    unique_content: bool,
    emit_keys: &[String],
) -> Result<()> {
    let conn = db.conn_mut();

    // Read the `--emit` names and the filters before anything is selected; when both are
    // refused, one refusal names every problem, the filter's first.
    let (emit_keys, filters) = match (
        crate::worklist::ops::read_emit_keys(emit_keys),
        read_filters(conn, filter_text),
    ) {
        (Ok(keys), Ok(filters)) => (keys, filters),
        (Err(names), Ok(_)) => return Err(names.into()),
        (Ok(_), Err(error)) => return Err(error),
        (Err(names), Err(error)) => {
            return Err(match error.downcast::<ExpressionRefused>() {
                Ok(filters) => filters.join(names).into(),
                Err(error) => error,
            })
        }
    };

    let scopes = classify_all(scope_prefixes);

    let params = SelectionParams {
        scopes,
        include: include.clone(),
        filters,
        role_policy: RolePolicy::SourceUnlessIncluded,
        clock: crate::user_zone().clone(),
        notes: false,
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

    /// An `--emit` name and a filter refused in one run are named together, the filter's first
    /// line leading and the name after the filter's blocks, so nothing is selected and nothing
    /// reaches stdout; a name refused alone keeps the key's first line.
    #[test]
    fn an_emit_name_and_a_filter_are_refused_together() {
        crate::core::domain::time::zone::with_test_zone(
            crate::core::domain::time::zone::UserZone::utc(),
            || {
                use crate::core::repo::open_in_memory_for_test;
                use crate::core::testing::{insert_number_fact, insert_root, insert_source};

                let conn = open_in_memory_for_test();
                let root = insert_root(&conn, "/photos", "source", false);
                let a = insert_source(&conn, root, "a.jpg", None);
                insert_number_fact(&conn, a, "content.Rating", 5.0);
                let mut db = Db::from_connection(conn);
                let mut refused = |filters: &[&str]| {
                    run(
                        &mut db,
                        &[],
                        &ExpressionText::plain(filters),
                        &IncludeSet::default(),
                        false,
                        &["source.extension".to_string()],
                    )
                    .expect_err("refused")
                    .to_string()
                };
                assert_eq!(
                    refused(&["Rating=big"]),
                    "cannot run the filter as given\n  Rating=big\n         ^ Rating is of \
                     type number; `big` is not a number\n    Type a number: 1024, -5, 3.14, \
                     1e20\n  `source.extension` is an unknown key\n    `canon facts` lists the keys"
                );
                assert_eq!(
                    refused(&[]),
                    "cannot use the key as given\n  `source.extension` is an unknown key\n    \
                     `canon facts` lists the keys"
                );
            },
        );
    }
}
